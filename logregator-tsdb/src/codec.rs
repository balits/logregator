use std::{
    error::Error, fmt::Debug, io::{self, BufReader, BufWriter, Read, Write},
};

use tracing::trace;

use crate::record::{KEY_SIZE, Key, MAX_PAYLOAD_LENGTH, PAYLOAD_LEN_SIZE, Record};

#[derive(Debug, thiserror::Error)]
pub enum BytesCodecError {
    #[error("bytes_codec: io error: {0}")]
    Io(#[from] io::Error),

    #[error("bytes_codec: maximum payload size exceeded (limit: {limit}, got: {got})")]
    MaxPayloadExceeded { limit: usize, got: usize },

    #[error("bytes_codec: unexpected error: {msg}")]
    Unexpected { msg: String },

    #[error("bytes_codec: output buffer size is insufficient (need: {need}, got: {got}")]
    NeedMoreBuf { need: usize, got: usize },
}

// fn torn(msg: &str) -> BytesCodecError {
//     BytesCodecError::Torn { msg: msg.into() }
// }

fn unexpected(msg: &str) -> BytesCodecError {
    BytesCodecError::Unexpected { msg: msg.into() }
}

fn need_more_buf(need: usize, got: usize) -> BytesCodecError {
    BytesCodecError::NeedMoreBuf { need, got }
}

pub trait Codec: Clone + Copy + Debug {
    type Error: From<io::Error> + Error + Send + Sync + 'static;

    fn encode(&self, rec: &Record, dst: &mut [u8]) -> Result<usize, Self::Error>;
    fn decode(&self, dst: &[u8]) -> Result<Option<(Record, usize)>, Self::Error>;
}

#[derive(Debug, Clone, Copy)]
pub struct BytesCodec;

impl Codec for BytesCodec {
    type Error = BytesCodecError;

    fn encode(&self, rec: &Record, dst: &mut [u8]) -> Result<usize, Self::Error> {
        if dst.len() < rec.wire_len() {
            trace!("not enough bytes to encode into (has: {}, need: {})", dst.len(), rec.wire_len());
            return Err(need_more_buf(rec.wire_len(), dst.len()));
        }

        let mut key_bytes = [0u8; KEY_SIZE];
        rec.key.to_be_bytes(&mut key_bytes);
        dst[0..KEY_SIZE].copy_from_slice(&key_bytes);

        let payload_len = (rec.payload.len() as u32).to_be_bytes();
        dst[KEY_SIZE..KEY_SIZE+PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len);
        dst[KEY_SIZE + PAYLOAD_LEN_SIZE..KEY_SIZE + PAYLOAD_LEN_SIZE + rec.payload.len()]
            .copy_from_slice(&rec.payload);

        Ok(rec.wire_len())
    }

    fn decode(&self, src: &[u8]) -> Result<Option<(Record, usize)>, Self::Error> {
        if src.len() < KEY_SIZE + PAYLOAD_LEN_SIZE {
            trace!("not enough bytes to decode from (key + payload len)");
            return Ok(None);
        }

        let key = Key::from_be_bytes(&src[..KEY_SIZE]).ok_or_else(|| unexpected("failed to decode key"))?;

        let payload_len = u32::from_be_bytes([
            src[KEY_SIZE],
            src[KEY_SIZE + 1],
            src[KEY_SIZE + 2],
            src[KEY_SIZE + 3],
        ]) as usize;

        if payload_len > MAX_PAYLOAD_LENGTH {
            trace!("max payload size exceeded");
            return Err(BytesCodecError::MaxPayloadExceeded {
                limit: MAX_PAYLOAD_LENGTH,
                got: payload_len,
            });
        }
        let total = KEY_SIZE + PAYLOAD_LEN_SIZE + payload_len ;
        if src.len() < total {
            trace!("not enoguh bytes to decode from (payload)");
            return Ok(None);
        }

        let payload =
            src[KEY_SIZE + PAYLOAD_LEN_SIZE..total].to_vec();

        Ok(Some((Record { key, payload }, total)))
    }
}

const BUFSIZE: usize = 2 * 1024;

pub struct FramedWriter<W: Write, C: Codec> {
    inner: BufWriter<W>,
    buf: Vec<u8>,
    codec: C,
}

impl<W: Write, C: Codec> FramedWriter<W, C> {
    pub fn new(w: W, c: C) -> Self {
        Self {
            inner: BufWriter::new(w),
            buf: vec![0; BUFSIZE],
            codec: c,
        }
    }

    pub fn write(&mut self, rec: &Record) -> Result<(), C::Error> {
        self.buf.clear();
        self.buf.resize(rec.wire_len(), 0);

        self.codec.encode(rec, &mut self.buf)?;
        self.inner
            .write_all(&mut self.buf)
            .map_err(C::Error::from)?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()?;
        Ok(())
    }
}

pub struct FramedReader<R: Read, C: Codec> {
    inner: BufReader<R>,
    buf: Vec<u8>,
    start: usize,
    end: usize,
    codec: C,
}

impl<R, C> Debug for FramedReader<R, C>
where
    R: Read + Debug,
    C: Codec + Debug
 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
       f.debug_struct("FramedReader<R, C>") 
        .field("inner", &format_args!("{:?}", &self.inner))
        .field("buf", &format_args!("[0..{}]", &self.buf.len()))
        .field("start", &self.start)
        .field("end", &self.end)
        .field("codec", &self.codec)
        .finish()
    }
}

impl<R: Read, C: Codec> FramedReader<R, C> {
    pub fn new(r: R, codec: C) -> Self {
        Self {
            inner: BufReader::new(r),
            buf: vec![0; BUFSIZE],
            start: 0,
            end: 0,
            codec,
        }
    }

    fn fill(&mut self) -> io::Result<bool> {
        if self.end == self.buf.len() {
            if self.start > 0 {
                self.buf.copy_within(self.start..self.end, 0);
                self.end -= self.start;
                self.start = 0;
            }

            if self.end == self.buf.len() {
                self.buf.resize(self.buf.len() * 2, 0);
            }
        }

        match self.inner.read(&mut self.buf[self.end..]) {
            Ok(0) => Ok(true),
            Ok(n) => {
                self.end += n;
                Ok(false)
            }
            Err(e) => Err(e)
        }
    }
}

impl<R: Read, C: Codec> Iterator for FramedReader<R, C> {
    type Item = Result<Record, C::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.codec.decode(&mut self.buf[self.start..self.end]) {
                Ok(Some((rec, n))) => {
                    self.start += n;
                    return Some(Ok(rec));
                },
                Ok(None) => match self.fill() {
                    Ok(false) => continue,
                    Ok(true) => return None,
                    Err(e) => {
                        trace!("FramedReader: failed to fill internal buffer: {}", e);
                        return None;
                    }
                },
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod testing {
    use proptest::prelude::*;
    use crate::record::{Key, Record};

    pub fn arb_record() -> impl Strategy<Value = Record> {
        (
            any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>(),
            proptest::collection::vec(any::<u8>(), 0..4096),
        )
            .prop_map(|(source_id, timestamp, sequence_num, stream_id, payload)| Record {
                key: Key::new(source_id, timestamp, sequence_num, stream_id),
                payload,
            })
    }
}

#[cfg(test)]
mod test {
    use super::testing::arb_record;
    use super::*;
    use crate::record::{KEY_SIZE, PAYLOAD_LEN_SIZE, MAX_PAYLOAD_LENGTH};
    use proptest::prelude::*;
    use std::io::Cursor;

    proptest! {
        /// encode -> decode reproduces the original record, and reports
        /// exactly rec.wire_len() bytes consumed.
        #[test]
        fn prop_encode_decode_roundtrip(rec in arb_record()) {
            let codec = BytesCodec;
            let mut buf = vec![0u8; rec.wire_len()];
            let written = codec.encode(&rec, &mut buf).unwrap();
            prop_assert_eq!(written, rec.wire_len());

            let (decoded, consumed) = codec.decode(&mut buf).unwrap().expect("should decode");
            prop_assert_eq!(consumed, rec.wire_len());
            prop_assert_eq!(decoded, rec);
        }

        /// Trailing garbage after a valid frame is ignored, and the
        /// reported consumed length still points exactly at the frame end
        /// (this is what lets FramedReader slice buf[start..] correctly).
        #[test]
        fn prop_decode_ignores_trailing_bytes(
            rec in arb_record(),
            trailing in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let codec = BytesCodec;
            let mut buf = vec![0u8; rec.wire_len()];
            codec.encode(&rec, &mut buf).unwrap();
            buf.extend_from_slice(&trailing);

            let (decoded, consumed) = codec.decode(&mut buf).unwrap().expect("should decode");
            prop_assert_eq!(consumed, rec.wire_len());
            prop_assert_eq!(decoded, rec);
        }

        /// Fewer than KEY_SIZE + PAYLOAD_LEN_SIZE bytes -> Ok(None), never panics.
        #[test]
        fn prop_partial_header_returns_none(
            bytes in proptest::collection::vec(any::<u8>(), 0..(KEY_SIZE + PAYLOAD_LEN_SIZE)),
        ) {
            let codec = BytesCodec;
            let mut buf = bytes;
            let result = codec.decode(&mut buf).unwrap();
            prop_assert!(result.is_none());
        }

        /// Full header but a truncated payload -> Ok(None), never panics.
        #[test]
        fn prop_partial_payload_returns_none(
            rec in arb_record().prop_filter("need a non-empty payload", |r| !r.payload.is_empty()),
            missing in 1usize..=1000,
        ) {
            let codec = BytesCodec;
            let mut full = vec![0u8; rec.wire_len()];
            codec.encode(&rec, &mut full).unwrap();
            let cut = full.len().saturating_sub(missing.min(rec.payload.len()));
            let mut truncated = full[..cut.max(KEY_SIZE + PAYLOAD_LEN_SIZE)].to_vec();

            let result = codec.decode(&mut truncated).unwrap();
            prop_assert!(result.is_none());
        }

        /// A length prefix over MAX_PAYLOAD_LENGTH is rejected immediately,
        /// without requiring the payload bytes to actually be buffered.
        #[test]
        fn prop_max_payload_exceeded(over_by in 1u32..1_000_000) {
            let codec = BytesCodec;
            let bogus_len = MAX_PAYLOAD_LENGTH as u32 + over_by;
            let mut buf = vec![0u8; KEY_SIZE + PAYLOAD_LEN_SIZE];
            buf[KEY_SIZE..KEY_SIZE + PAYLOAD_LEN_SIZE]
                .copy_from_slice(&bogus_len.to_be_bytes());

            let err = codec.decode(&mut buf).unwrap_err();
            let m = matches!(err, BytesCodecError::MaxPayloadExceeded { .. }); 
            prop_assert!(m);
        }

        /// decode() never panics on arbitrary bytes of any length.
        #[test]
        fn prop_decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let codec = BytesCodec;
            let mut buf = bytes;
            let _ = codec.decode(&mut buf); // Ok or Err both fine, panic is not
        }

        /// Writing N records then reading them back through
        /// FramedWriter/FramedReader reproduces the same sequence.
        /// Payload sizes deliberately range past BUFSIZE so this exercises
        /// fill()'s compaction/resize path, not just a single read().
        #[test]
        fn prop_framed_writer_reader_roundtrip(
            records in proptest::collection::vec(arb_record(), 1..40),
        ) {
            let mut writer = FramedWriter::new(Cursor::new(Vec::new()), BytesCodec);
            for rec in &records {
                writer.write(rec).unwrap();
            }
            writer.flush().unwrap();
            let bytes = writer.inner.into_inner().unwrap().into_inner();

            let reader = FramedReader::new(Cursor::new(bytes), BytesCodec);
            let decoded: Vec<Record> = reader.map(|r| r.expect("decode failed")).collect();

            prop_assert_eq!(records, decoded);
        }

    }

    #[test]
    fn framed_reader_grows_buffer_even_after_prior_compaction() {
    let small = Record {
        key: crate::record::Key::new(1, 1, 1, 1),
        payload: vec![0xAA; 4],
    };
    let big_payload_len = MAX_PAYLOAD_LENGTH - KEY_SIZE - PAYLOAD_LEN_SIZE; // several buffer-doublings' worth
    let big = Record {
        key: crate::record::Key::new(2, 2, 2, 2),
        payload: vec![0xBB; big_payload_len],
    };

    let mut writer = FramedWriter::new(Cursor::new(Vec::new()), BytesCodec);
    writer.write(&small).unwrap();
    writer.write(&big).unwrap();
    writer.flush().unwrap();
    let bytes = writer.inner.into_inner().unwrap().into_inner();

    let mut reader = FramedReader::new(Cursor::new(bytes), BytesCodec);
    let mut decoded = vec![];
    while let Some(r) = reader.next() {
        let r = r.unwrap();
        decoded.push(r);
    }

    assert_eq!(decoded, vec![small, big]);
}

    /// Deterministic (non-property) test: forces a record to straddle the
    /// BUFSIZE fill boundary exactly, to reliably catch the fill()
    /// overwrite-on-refill bug regardless of proptest shrinking luck.
    #[test]
    fn framed_reader_handles_record_split_across_fill_boundary() {
        let leading_payload_len = BUFSIZE - KEY_SIZE - PAYLOAD_LEN_SIZE - 10;
        let straddling = Record {
            key: crate::record::Key::new(1, 2, 3, 4),
            payload: vec![0xAB; leading_payload_len],
        };
        let second = Record {
            key: crate::record::Key::new(5, 6, 7, 8),
            payload: vec![0xCD; 500],
        };

        let mut writer = FramedWriter::new(Cursor::new(Vec::new()), BytesCodec);
        writer.write(&straddling).unwrap();
        writer.write(&second).unwrap();
        writer.flush().unwrap();
        let bytes = writer.inner.into_inner().unwrap().into_inner();

        let reader = FramedReader::new(Cursor::new(bytes), BytesCodec);
        let decoded: Vec<Record> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(decoded, vec![straddling, second]);
    }

    /// Documents current WAL-style recovery semantics: a torn trailing
    /// record at EOF ends the stream cleanly (no error) rather than
    /// failing. If SSTable reads need strict behavior instead, this is
    /// the test to change once that's added.
    #[test]
    fn framed_reader_stops_cleanly_on_torn_trailing_record() {
        let good = Record {
            key: crate::record::Key::new(1, 1, 1, 1),
            payload: vec![1, 2, 3],
        };
        let torn = Record {
            key: crate::record::Key::new(2, 2, 2, 2),
            payload: vec![9; 100],
        };

        let mut writer = FramedWriter::new(Cursor::new(Vec::new()), BytesCodec);
        writer.write(&good).unwrap();
        writer.write(&torn).unwrap();
        writer.flush().unwrap();
        let mut bytes = writer.inner.into_inner().unwrap().into_inner();
        bytes.truncate(bytes.len() - 40); // chop the tail of the second record

        let reader = FramedReader::new(Cursor::new(bytes), BytesCodec);
        let decoded: Vec<Record> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(decoded, vec![good]);
    }
}
