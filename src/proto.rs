//! ## Framing
//! Each message is length delimited: [4-byte frame_len BE][frame_bytes].
//! Frame themselves are: [1-byte message_type][payload_bytes].
//!
//! | type (u8) | payload              |
//! |-----------|----------------------|
//! | 0x01      | Insert (client)      |
//! | 0x02      | Range (client)       |
//! | 0x03      | BatchInsert (client) |
//! | 0x81      | InsertOk (server)    |
//! | 0x82      | RangeRecord (server) |
//! | 0x83      | RangeEnd (server)    |
//! | 0xFF      | Error (server)       |
//!
//! - Insert (0x01):
//!     <code> [source_id: 8 bytes BE][ts: 8 bytes BE][key_len: 4 bytes BE][key bytes][value_len: 4 bytes BE][value bytes]
//!
//! - Range (0x02)
//!     <code> [source_id: 8 bytes BE][key_len: 4 bytes BE][key bytes][start_ts: 8 bytes BE][end_ts: 8 bytes BE] </code>
//!
//! - BatchInsert (0x03)
//!     <code> [num_records: 4 bytes BE][record_1][record_2]...  </code>
//!     each record has same layout as Insert payload (no type byte per record).
//!
//! - InsertOk (0x81)
//!     Empty payload.
//!
//! - RangeRecord (0x82) //!     <code> [source_id: 8][ts: 8][seq: 8][key_len: 4][key][value_len: 4][value] </code>
//!
//! - RangeEnd (0x83)
//!     Empty payload.
//!
//! - Error (0xFF)
//!     <code> [msg_len: 4 bytes BE][msg bytes] </code>

use std::io;

use bytes::{Buf, BufMut, BytesMut};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Decoder, Encoder};

use crate::storage;

const LENGTH_SZ: usize = 4;

pub(crate) type SourceId = i64;
pub(crate) type Timestamp = i64;
pub(crate) type SeqNum = u64;

#[derive(Debug)]
pub(crate) struct Insert {
    pub(crate) source_id: SourceId,
    pub(crate) ts: Timestamp,
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Debug)]
pub(crate) struct Range {
    pub(crate) source_id: SourceId,
    pub(crate) key: String,
    pub(crate) start_ts: Timestamp,
    pub(crate) end_ts: Timestamp,
}

#[derive(Debug)]
pub(crate) struct BatchInsert {
    pub(crate) records: Vec<Insert>,
}

#[derive(Debug)]
pub(crate) enum ClientMessage {
    Insert(Insert),
    Range(Range),
    BatchInsert(BatchInsert),
}

#[derive(Debug)]
pub(crate) struct Record {
    pub(crate) source_id: SourceId,
    pub(crate) ts: Timestamp,
    pub(crate) seq: SeqNum,
    pub(crate) key: String,
    pub(crate) value: String,
}

impl TryFrom<storage::Record> for Record {
    type Error = anyhow::Error;
    fn try_from(value: storage::Record) -> Result<Self, Self::Error> {
        let (source_id, ts, seq, key, value) = value.extract_all_fields()?;
        Ok(Self { source_id, ts, seq, key, value })
    }
}

#[derive(Debug)]
pub(crate) enum ServerMessage {
    InsertOk,
    RangeRecord(Record),
    RangeEnd,
    Error(String),
}

/// Internal engine commands.
///
/// Each variant carries a channels needed to send results back
/// to the connection handler:
///   - Insert / BatchInsert: ACK via oneshot
///   - Range: error | ACK via oneshot, streaming records from disk through mpsc
/// following the tokios envelope method seen in mini-redis.
pub enum Command {
    Insert(Insert, oneshot::Sender<anyhow::Result<()>>),
    BatchInsert(BatchInsert, oneshot::Sender<anyhow::Result<()>>),
    Range(Range, oneshot::Sender<anyhow::Result<()>>, mpsc::Sender<Record>),
}

/// Wire format per frame: <code>[4-byte frame_len BE][1-byte type][payload]</code>
pub(crate) struct Codec;

impl Decoder for Codec {
    type Item = ClientMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<ClientMessage>, Self::Error> {
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
            0x01 => decode_insert(frame.as_ref()).map(|i| Some(ClientMessage::Insert(i))),
            0x02 => decode_range(frame.as_ref()).map(|r| Some(ClientMessage::Range(r))),
            0x03 => decode_batch_insert(frame.as_ref()).map(|bi| Some(ClientMessage::BatchInsert(bi))),
            t => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("codec.decode: unknown message type: {t:#04x}"),
            )),
        }
    }
}

impl Encoder<ServerMessage> for Codec {
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
            ServerMessage::Error(err) => {
                dst.put_u8(0xFF);
                dst.put_u32(err.len() as u32);
                dst.put_slice(err.as_bytes());
            }
        }
        let frame_len = dst.len() - start - LENGTH_SZ;
        dst[start..start + LENGTH_SZ]
            .copy_from_slice(&(frame_len as u32).to_be_bytes());
        Ok(())
    }
}

fn decode_insert(buf: &[u8]) -> io::Result<Insert> {
    let source_id = i64::from_be_bytes(array8(buf, 0)?);
    let ts = i64::from_be_bytes(array8(buf, 8)?);
    let key_len = u32::from_be_bytes(array4(buf, 16)?) as usize;
    let key = from_utf8(&buf[20..20 + key_len])?;
    let value_len = u32::from_be_bytes(array4(buf, 20 + key_len)?) as usize;
    let value = from_utf8(&buf[24 + key_len..24 + key_len + value_len])?;
    Ok(Insert { source_id, ts, key, value })
}

fn decode_range(buf: &[u8]) -> io::Result<Range> {
    let source_id = i64::from_be_bytes(array8(buf, 0)?);
    let key_len = u32::from_be_bytes(array4(buf, 8)?) as usize;
    let key = from_utf8(&buf[12..12 + key_len])?;
    let start_ts = i64::from_be_bytes(array8(buf, 12 + key_len)?);
    let end_ts = i64::from_be_bytes(array8(buf, 20 + key_len)?);
    Ok(Range { source_id, key, start_ts, end_ts })
}

fn decode_batch_insert(buf: &[u8]) -> io::Result<BatchInsert> {
    let num = u32::from_be_bytes(array4(buf, 0)?) as usize;
    let mut offset = 4;
    let mut records = Vec::with_capacity(num);
    for _ in 0..num {
        let source_id = i64::from_be_bytes(array8(buf, offset)?);
        let ts = i64::from_be_bytes(array8(buf, offset + 8)?);
        let key_len = u32::from_be_bytes(array4(buf, offset + 16)?) as usize;
        let key = from_utf8(&buf[offset + 20..offset + 20 + key_len])?;
        let value_len = u32::from_be_bytes(array4(buf, offset + 20 + key_len)?) as usize;
        let value = from_utf8(&buf[offset + 24 + key_len..offset + 24 + key_len + value_len])?;
        records.push(Insert { source_id, ts, key, value });
        offset += 24 + key_len + value_len;
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
    String::from_utf8(buf.to_vec())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
