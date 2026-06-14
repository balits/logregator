//! ## Framing
//! Each message is length delimited: [[4-byte frame_len BE]][[frame_bytes]].
//! Frame themselves are: [[1-byte message_type]][[payload_bytes]].
//!
//! | type (u8) | payload              | layout |
//! |-----------|----------------------|--------|
//! | 0x01      | Insert (client)      | [[source_id: i64 BE]][[ts: i64 BE]][[key_len: u32 BE]][[key: key_len bytes]][[value_len: u32 BE]][[value: value_len bytes]] |
//! | 0x02      | Range (client)       | [[source_id: i64 BE]][[key_len: u32 BE]][[key: key_len bytes]][[start_ts: i64 BE]][[end_ts: i64 BE]][[filter_len: u32 BE]][[filter: filter_len bytes]] |
//! | 0x03      | BatchInsert (client) | [[num_records: u32 BE]] then per record: [[source_id: i64 BE]][[ts: i64 BE]][[key_len: u32 BE]][[key: key_len bytes]][[value_len: u32 BE]][[value: value_len bytes]] |
//! | 0x81      | InsertOk (server)    | *no payload* |
//! | 0x82      | RangeRecord (server) | [[source_id: i64 BE]][[ts: i64 BE]][[seq: u64 BE]][[key_len: u32 BE]][[key: key_len bytes]][[value_len: u32 BE]][[value: value_len bytes]] |
//! | 0x83      | RangeEnd (server)    | *no payload* |
//! | 0x04      | Metrics (client)     | *no payload* |
//! | 0x84      | Metrics (server)     | [[json_len: u32 BE]][[json: json_len bytes]] |
//! | 0x05      | Ping (client)        | *no payload* |
//! | 0x85      | Pong (server)        | *no payload* |
//! | 0xFF      | Error (server)       | [[err_len: u32 BE]][[err: err_len bytes]] |

use std::{fmt::Display, io};

use anyhow::Context;
use bytes::{Buf, BufMut, BytesMut};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Decoder, Encoder};

use crate::storage;

const LENGTH_SZ: usize = 4;

pub type SourceId = i64;
pub type Timestamp = i64;
pub type SeqNum = u64;

#[derive(Debug)]
pub struct Insert {
    pub source_id: SourceId,
    pub ts: Timestamp,
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct Range {
    pub source_id: SourceId,
    pub key: String,
    pub start_ts: Timestamp,
    pub end_ts: Timestamp,
    pub filter: String,
}

#[derive(Debug)]
pub struct BatchInsert {
    pub records: Vec<Insert>,
}

#[derive(Debug)]
pub enum ClientMessage {
    Insert(Insert),
    Range(Range),
    BatchInsert(BatchInsert),
    Metrics,
    Ping,
}

#[derive(Debug)]
pub struct Record {
    pub source_id: SourceId,
    pub ts: Timestamp,
    pub seq: SeqNum,
    pub key: String,
    pub value: String,
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "source={}, ts={}, seq={}: key={} value={}",
            self.source_id, self.ts, self.seq, self.key, self.value,
        )
    }
}

impl TryFrom<storage::Record> for Record {
    type Error = anyhow::Error;
    fn try_from(value: storage::Record) -> Result<Self, Self::Error> {
        let (source_id, ts, seq, key, value) = value.extract_all_fields_owned()?;
        Ok(Self {
            source_id,
            ts,
            seq,
            key,
            value,
        })
    }
}

#[derive(Debug)]
pub enum ServerMessage {
    InsertOk,
    RangeRecord(Record),
    RangeEnd,
    Metrics(String),
    Pong,
    Error(String),
}

/// Internal engine commands.
///
/// Each variant carries the channels needed to send results back
/// to the connection handler (based on the envelope method
/// seen in tokios mini-redis):
///   - Insert: ACK via oneshot
///   - Range: error | ACK via oneshot, batch-streaming records through mpsc
pub enum Command {
    Insert(Insert, oneshot::Sender<anyhow::Result<()>>),
    BatchInsert(BatchInsert, oneshot::Sender<anyhow::Result<()>>),
    Range(
        Range,
        oneshot::Sender<anyhow::Result<()>>,
        mpsc::Sender<Vec<Record>>,
    ),
    Metrics(oneshot::Sender<anyhow::Result<String>>),
}

// Server codec is used to decode client messages and encode server messages
pub struct ServerCodec;

impl Decoder for ServerCodec {
    type Item = ClientMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < LENGTH_SZ {
            return Ok(None);
        }
        let frame_len = u32::from_be_bytes(src[..LENGTH_SZ].try_into().unwrap()) as usize;
        let total_len = LENGTH_SZ + frame_len;
        if src.len() < total_len {
            src.reserve(total_len - src.len());
            return Ok(None);
        }
        let mut frame = src.split_to(total_len);
        frame.advance(LENGTH_SZ);

        let msg_type = frame[0];
        frame.advance(1);

        match msg_type {
            0x01 => decode_insert(frame.as_ref())
                .map(|i| Some(ClientMessage::Insert(i)))
                .map_err(io::Error::other),
            0x02 => decode_range(frame.as_ref())
                .map(|r| Some(ClientMessage::Range(r)))
                .map_err(io::Error::other),
            0x03 => decode_batch_insert(frame.as_ref())
                .map(|b| Some(ClientMessage::BatchInsert(b)))
                .map_err(io::Error::other),
             0x04 => Ok(Some(ClientMessage::Metrics)),
             0x05 => Ok(Some(ClientMessage::Ping)),
            t => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("server_codec.decode: unknown message type: {t:#04x}"),
            )),
        }
    }
}

impl Encoder<ServerMessage> for ServerCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: ServerMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let start = dst.len();
        dst.put_u32(0);
        match msg {
            ServerMessage::InsertOk => {
                dst.put_u8(0x81);
            }
            ServerMessage::RangeRecord(rec) => {
                dst.put_u8(0x82);
                encode_record(&rec, dst);
            }
            ServerMessage::RangeEnd => {
                dst.put_u8(0x83);
            }
            ServerMessage::Metrics(json) => {
                dst.put_u8(0x84);
                dst.put_u32(json.len() as u32);
                dst.put_slice(json.as_bytes());
            }
            ServerMessage::Pong => {
                dst.put_u8(0x85);
            }
            ServerMessage::Error(err) => {
                dst.put_u8(0xFF);
                dst.put_u32(err.len() as u32);
                dst.put_slice(err.as_bytes());
            }
        }
        let frame_len = dst.len() - start - LENGTH_SZ;
        dst[start..start + LENGTH_SZ].copy_from_slice(&(frame_len as u32).to_be_bytes());
        Ok(())
    }
}

fn decode_insert(buf: &[u8]) -> anyhow::Result<Insert> {
    let source_id =
        i64::from_be_bytes(array8(buf, 0).context("decode_insert: failed to read source_id")?);
    let ts = i64::from_be_bytes(array8(buf, 8).context("decode_insert: failed to read timestamp")?);
    let key_len =
        u32::from_be_bytes(array4(buf, 16).context("decode_insert: failed to read key length")?)
            as usize;
    let key = from_utf8(&buf[20..20 + key_len]).context("decode_insert: failed to decode key")?;
    let value_len = u32::from_be_bytes(
        array4(buf, 20 + key_len).context("decode_insert: failed to read value length")?,
    ) as usize;
    let value = from_utf8(&buf[24 + key_len..24 + key_len + value_len])
        .context("decode_insert: failed to decode value")?;
    Ok(Insert {
        source_id,
        ts,
        key,
        value,
    })
}

fn decode_range(buf: &[u8]) -> anyhow::Result<Range> {
    let source_id =
        i64::from_be_bytes(array8(buf, 0).context("decode_range: failed to read source_id")?);
    let key_len =
        u32::from_be_bytes(array4(buf, 8).context("decode_range: failed to read key length")?)
            as usize;
    let key = from_utf8(&buf[12..12 + key_len]).context("decode_range: failed to decode key")?;
    let start_ts = i64::from_be_bytes(
        array8(buf, 12 + key_len).context("decode_range: failed to read start_ts")?,
    );
    let end_ts = i64::from_be_bytes(
        array8(buf, 20 + key_len).context("decode_range: failed to read end_ts")?,
    );
    let filter_len = u32::from_be_bytes(
        array4(buf, 28 + key_len).context("decode_range: failed to read filter length")?,
    ) as usize;
    let filter = from_utf8(&buf[32 + key_len..32 + key_len + filter_len])
        .context("decode_range: failed to decode filter")?;
    Ok(Range {
        source_id,
        key,
        start_ts,
        end_ts,
        filter,
    })
}

fn decode_batch_insert(buf: &[u8]) -> anyhow::Result<BatchInsert> {
    let num_records = u32::from_be_bytes(
        array4(buf, 0).context("decode_batch_insert: failed to read record count")?,
    ) as usize;
    let mut records = Vec::with_capacity(num_records);
    let mut offset = 4;
    for _ in 0..num_records {
        let source_id = i64::from_be_bytes(
            array8(buf, offset).context("decode_batch_insert: failed to read source_id")?,
        );
        offset += 8;
        let ts = i64::from_be_bytes(
            array8(buf, offset).context("decode_batch_insert: failed to read timestamp")?,
        );
        offset += 8;
        let key_len = u32::from_be_bytes(
            array4(buf, offset).context("decode_batch_insert: failed to read key length")?,
        ) as usize;
        offset += 4;
        let key = from_utf8(&buf[offset..offset + key_len])
            .context("decode_batch_insert: failed to decode key")?;
        offset += key_len;
        let value_len = u32::from_be_bytes(
            array4(buf, offset).context("decode_batch_insert: failed to read value length")?,
        ) as usize;
        offset += 4;
        let value = from_utf8(&buf[offset..offset + value_len])
            .context("decode_batch_insert: failed to decode value")?;
        offset += value_len;
        records.push(Insert {
            source_id,
            ts,
            key,
            value,
        });
    }
    Ok(BatchInsert { records })
}

fn encode_record(rec: &Record, dst: &mut BytesMut) {
    dst.put_i64(rec.source_id);
    dst.put_i64(rec.ts);
    dst.put_u64(rec.seq);
    dst.put_u32(rec.key.len() as u32);
    dst.put_slice(rec.key.as_bytes());
    dst.put_u32(rec.value.len() as u32);
    dst.put_slice(rec.value.as_bytes());
}

fn array4(buf: &[u8], off: usize) -> io::Result<[u8; 4]> {
    buf.get(off..off + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated frame"))
}

fn array8(buf: &[u8], off: usize) -> io::Result<[u8; 8]> {
    buf.get(off..off + 8)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated frame"))
}

fn from_utf8(buf: &[u8]) -> io::Result<String> {
    String::from_utf8(buf.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

// ClientCodec is used to decode server messages and encode client messages
pub struct ClientCodec;

impl Encoder<ClientMessage> for ClientCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: ClientMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let start = dst.len();
        dst.put_u32(0);
        match msg {
            ClientMessage::Insert(i) => {
                dst.put_u8(0x01);
                dst.put_i64(i.source_id);
                dst.put_i64(i.ts);
                dst.put_u32(i.key.len() as u32);
                dst.put_slice(i.key.as_bytes());
                dst.put_u32(i.value.len() as u32);
                dst.put_slice(i.value.as_bytes());
            }
            ClientMessage::Range(r) => {
                dst.put_u8(0x02);
                dst.put_i64(r.source_id);
                dst.put_u32(r.key.len() as u32);
                dst.put_slice(r.key.as_bytes());
                dst.put_i64(r.start_ts);
                dst.put_i64(r.end_ts);
                dst.put_u32(r.filter.len() as u32);
                dst.put_slice(r.filter.as_bytes());
            }
            ClientMessage::BatchInsert(b) => {
                dst.put_u8(0x03);
                dst.put_u32(b.records.len() as u32);
                for i in &b.records {
                    dst.put_i64(i.source_id);
                    dst.put_i64(i.ts);
                    dst.put_u32(i.key.len() as u32);
                    dst.put_slice(i.key.as_bytes());
                    dst.put_u32(i.value.len() as u32);
                    dst.put_slice(i.value.as_bytes());
                }
            }
            ClientMessage::Metrics => {
                dst.put_u8(0x04);
            }
            ClientMessage::Ping => {
                dst.put_u8(0x05);
            }
        }
        let frame_len = dst.len() - start - LENGTH_SZ;
        dst[start..start + LENGTH_SZ].copy_from_slice(&(frame_len as u32).to_be_bytes());
        Ok(())
    }
}

impl Decoder for ClientCodec {
    type Item = ServerMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < LENGTH_SZ {
            return Ok(None);
        }
        let frame_len = u32::from_be_bytes(src[..LENGTH_SZ].try_into().unwrap()) as usize;
        let total_len = LENGTH_SZ + frame_len;
        if src.len() < total_len {
            src.reserve(total_len - src.len());
            return Ok(None);
        }
        let mut frame = src.split_to(total_len);
        frame.advance(LENGTH_SZ);

        let msg_type = frame[0];
        frame.advance(1);

        match msg_type {
            0x81 => Ok(Some(ServerMessage::InsertOk)),
            0x82 => decode_server_range_record(frame.as_ref())
                .map(Some)
                .map_err(io::Error::other),
            0x83 => Ok(Some(ServerMessage::RangeEnd)),
             0x84 => decode_metrics(frame.as_ref())
                .map(Some)
                .map_err(io::Error::other),
             0x85 => Ok(Some(ServerMessage::Pong)),
             0xFF => decode_server_error(frame.as_ref())
                .map(|e| Some(ServerMessage::Error(e)))
                .map_err(io::Error::other),
            t => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("client_codec.decode: unknown message type: {t:#04x}"),
            )),
        }
    }
}

fn decode_server_range_record(buf: &[u8]) -> anyhow::Result<ServerMessage> {
    let source_id = i64::from_be_bytes(
        array8(buf, 0).context("decode_server_range_record: failed to read source_id")?,
    );
    let ts = i64::from_be_bytes(
        array8(buf, 8).context("decode_server_range_record: failed to read timestamp")?,
    );
    let seq = u64::from_be_bytes(
        array8(buf, 16).context("decode_server_range_record: failed to read seq_num")?,
    );
    let key_len = u32::from_be_bytes(
        array4(buf, 24).context("decode_server_range_record: failed to read key length")?,
    ) as usize;
    let key = from_utf8(&buf[28..28 + key_len])
        .context("decode_server_range_record: failed to decode key")?;
    let value_len = u32::from_be_bytes(
        array4(buf, 28 + key_len)
            .context("decode_server_range_record: failed to read value length")?,
    ) as usize;
    let value = from_utf8(&buf[32 + key_len..32 + key_len + value_len])
        .context("decode_server_range_record: failed to decode value")?;
    Ok(ServerMessage::RangeRecord(Record {
        source_id,
        ts,
        seq,
        key,
        value,
    }))
}

fn decode_server_error(buf: &[u8]) -> anyhow::Result<String> {
    let msg_len = u32::from_be_bytes(
        array4(buf, 0).context("decode_server_error: failed to read error message length")?,
    ) as usize;
    from_utf8(&buf[4..4 + msg_len]).context("decode_server_error: failed to decode error message")
}

fn decode_metrics(buf: &[u8]) -> anyhow::Result<ServerMessage> {
    let json_len = u32::from_be_bytes(
        array4(buf, 0).context("decode_metrics: failed to read json length")?,
    ) as usize;
    let json = from_utf8(&buf[4..4 + json_len])
        .context("decode_metrics: failed to decode metrics json")?;
    Ok(ServerMessage::Metrics(json))
}
