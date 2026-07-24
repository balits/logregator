/// More concretely, it implements both [SpecCodec<Record>] and [SpecCodec<Key>]
#[derive(Debug, Clone, Copy)]
pub struct DefaultCodec;

#[derive(thiserror::Error, Debug, Clone)]
#[error("unexpected_size: not enough bytes: got {got}, want: {want}")]
pub struct UnexpectedSize {
    pub got: usize,
    pub want: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("codec error: io error: {0}")]
    Io(#[from] io::Error),

    #[error("codec error: unexpected error: {msg}")]
    Unexpected { msg: String },

    #[error("codec error: {0}")]
    UnexpectedSize(UnexpectedSize),

    #[error("codec error: {0}")]
    InvalidPayloadSize(InvalidPayloadSize),
}

#[derive(thiserror::Error, Debug, Clone)]
#[error("invalid payload size: min: {min}, max: {max}, got: {got}")]
pub struct InvalidPayloadSize {
    pub min: usize,
    pub max: usize,
    pub got: usize,
}

impl InvalidPayloadSize {
    pub fn new(got: usize) -> Self {
        let min = crate::record::MIN_PAYLOAD_LENGTH;
        let max = crate::record::MAX_PAYLOAD_LENGTH;
        Self { min, max, got }
    }
}

pub fn not_enough_bytes(got: usize, want: usize) -> CodecError {
    CodecError::UnexpectedSize(UnexpectedSize { got, want })
}

pub fn unexpected(msg: impl Into<String>) -> CodecError {
    CodecError::Unexpected { msg: msg.into() }
}

pub fn invalid_payload_sz(got: usize) -> CodecError {
    CodecError::InvalidPayloadSize(InvalidPayloadSize::new(got))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::record::{
        KEY_SIZE, Key, MAX_PAYLOAD_LENGTH, MIN_PAYLOAD_LENGTH, PAYLOAD_LEN_SIZE, Record,
    };
    use proptest::prelude::*;
    use std::io::Cursor;

    pub fn arb_record() -> impl Strategy<Value = Record> {
        (
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            proptest::collection::vec(any::<u8>(), MIN_PAYLOAD_LENGTH..MAX_PAYLOAD_LENGTH),
        )
            .prop_map(
                |(source_id, timestamp, sequence_num, stream_id, payload)| Record {
                    key: Key::new(source_id, timestamp, sequence_num, stream_id),
                    payload: payload.into_boxed_slice(),
                },
            )
    }

    proptest! {
        /// encode -> decode reproduces the original record, and reports
        /// exactly rec.wire_len() bytes consumed.
        #[test]
        fn prop_encode_decode_roundtrip(rec in arb_record()) {
            let codec = DefaultCodec;
            let mut buf = vec![0u8; rec.wire_len()];
            let written = codec.encode(&rec, &mut buf).unwrap();
            prop_assert_eq!(written, rec.wire_len());

            let (decoded, consumed): (Record, usize) = codec.decode(&buf).unwrap().expect("should decode");
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
            let codec = DefaultCodec;
            let mut buf = vec![0u8; rec.wire_len()];
            codec.encode(&rec, &mut buf).unwrap();
            buf.extend_from_slice(&trailing);

            let (decoded, consumed): (Record, usize) = codec.decode(&buf).unwrap().expect("should decode");
            prop_assert_eq!(consumed, rec.wire_len());
            prop_assert_eq!(decoded, rec);
        }

        /// Fewer than KEY_SIZE + PAYLOAD_LEN_SIZE bytes -> Ok(None), never panics.
        #[test]
        fn prop_partial_header_returns_none(
            bytes in proptest::collection::vec(any::<u8>(), 0..(KEY_SIZE + PAYLOAD_LEN_SIZE)),
        ) {
            let codec = DefaultCodec;
            let buf = bytes;
            let result: Option<(Record, usize)> = codec.decode(&buf).unwrap();
            prop_assert!(result.is_none());
        }

        /// Full header but a truncated payload -> Ok(None), never panics.
        #[test]
        fn prop_partial_payload_returns_none(
            rec in arb_record().prop_filter("need a non-empty payload", |r| !r.payload.is_empty()),
            missing in 1usize..=1000,
        ) {
            let codec = DefaultCodec;
            let mut full = vec![0u8; rec.wire_len()];
            codec.encode(&rec, &mut full).unwrap();
            let cut = full.len().saturating_sub(missing.min(rec.payload.len()));
            let truncated = full[..cut.max(KEY_SIZE + PAYLOAD_LEN_SIZE)].to_vec();

            let result: Option<(Record, usize)> = codec.decode(&truncated).unwrap();
            prop_assert!(result.is_none());
        }

        /// A length prefix over MAX_PAYLOAD_LENGTH is rejected immediately,
        /// without requiring the payload bytes to actually be buffered.
        #[test]
        fn prop_max_payload_exceeded(over_by in 1u32..1_000_000) {
            let codec = DefaultCodec;
            let bogus_len = MAX_PAYLOAD_LENGTH as u32 + over_by;
            let mut buf = vec![0u8; KEY_SIZE + PAYLOAD_LEN_SIZE];
            buf[KEY_SIZE..KEY_SIZE + PAYLOAD_LEN_SIZE]
                .copy_from_slice(&bogus_len.to_be_bytes());

            let err= SpecCodec::<Record>::decode(&codec, &buf).unwrap_err();
            let m = matches!(err, CodecError::InvalidPayloadSize { .. });
            prop_assert!(m);
        }

        /// decode() never panics on arbitrary bytes of any length.
        #[test]
        fn prop_decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let codec = DefaultCodec;
            let buf = bytes;

            let _ = SpecCodec::<Record>::decode(&codec, &buf);
        }

        /// Writing N records then reading them back through
        /// FramedWriter/FramedReader reproduces the same sequence.
        /// Payload sizes deliberately range past BUFSIZE so this exercises
        /// fill()'s compaction/resize path, not just a single read().
        #[test]
        fn prop_framed_writer_reader_roundtrip(
            records in proptest::collection::vec(arb_record(), 1..40),
        ) {
            let mut writer = FramedWriter::new(Cursor::new(Vec::new()), DefaultCodec);
            for rec in &records {
                writer.write(rec).unwrap();
            }
            writer.flush().unwrap();
            let bytes = writer.into_inner().unwrap().into_inner();

            let reader = FramedReader::new(Cursor::new(bytes), DefaultCodec);
            let decoded: Vec<Record> = reader.map(|r| r.expect("decode failed")).collect();

            prop_assert_eq!(records, decoded);
        }

    }

    #[test]
    fn framed_reader_grows_buffer_even_after_prior_compaction() {
        let small = Record {
            key: crate::record::Key::new(1, 1, 1, 1),
            payload: vec![0xAA; 4].into_boxed_slice(),
        };
        let big_payload_len = MAX_PAYLOAD_LENGTH - KEY_SIZE - PAYLOAD_LEN_SIZE; // several buffer-doublings' worth
        let big = Record {
            key: crate::record::Key::new(2, 2, 2, 2),
            payload: vec![0xBB; big_payload_len].into_boxed_slice(),
        };

        let mut writer = FramedWriter::new(Cursor::new(Vec::new()), DefaultCodec);
        writer.write(&small).unwrap();
        writer.write(&big).unwrap();
        writer.flush().unwrap();
        let bytes = writer.into_inner().unwrap().into_inner();

        let reader: FramedReader<Cursor<Vec<u8>>, DefaultCodec, Record> =
            FramedReader::new(Cursor::new(bytes), DefaultCodec);
        let mut decoded = vec![];
        for r in reader {
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
            payload: vec![0xAB; leading_payload_len].into_boxed_slice(),
        };
        let second = Record {
            key: crate::record::Key::new(5, 6, 7, 8),
            payload: vec![0xCD; 500].into_boxed_slice(),
        };

        let mut writer = FramedWriter::new(Cursor::new(Vec::new()), DefaultCodec);
        writer.write(&straddling).unwrap();
        writer.write(&second).unwrap();
        writer.flush().unwrap();
        let bytes = writer.into_inner().unwrap().into_inner();

        let reader = FramedReader::new(Cursor::new(bytes), DefaultCodec);
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
            payload: vec![1, 2, 3].into_boxed_slice(),
        };
        let torn = Record {
            key: crate::record::Key::new(2, 2, 2, 2),
            payload: vec![9; 100].into_boxed_slice(),
        };

        let mut writer = FramedWriter::new(Cursor::new(Vec::new()), DefaultCodec);
        writer.write(&good).unwrap();
        writer.write(&torn).unwrap();
        writer.flush().unwrap();
        let mut bytes = writer.into_inner().unwrap().into_inner();
        bytes.truncate(bytes.len() - 40); // chop the tail of the second record

        let reader = FramedReader::new(Cursor::new(bytes), DefaultCodec);
        let decoded: Vec<Record> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(decoded, vec![good]);
    }
}

use std::{
    fmt::Debug,
    io::{self, BufWriter, IntoInnerError, Read, Write},
    marker::PhantomData,
};

use tracing::{info, instrument, trace, trace_span};

use crate::record::{
    KEY_SIZE, Key, MAX_PAYLOAD_LENGTH, MIN_PAYLOAD_LENGTH, PAYLOAD_LEN_SIZE, Record,
};

pub trait WireLen: Sized {
    fn wire_len(&self) -> usize;
}

pub trait SpecCodec<I: WireLen>: Clone + Debug {
    fn encode(&self, item: &I, dst: &mut [u8]) -> Result<usize, CodecError>;
    fn decode(&self, src: &[u8]) -> Result<Option<(I, usize)>, CodecError>;
}

impl SpecCodec<Key> for DefaultCodec {
    // #[instrument(ret)]
    fn encode(&self, key: &Key, dst: &mut [u8]) -> Result<usize, CodecError> {
        if dst.len() < key.wire_len() {
            return Err(CodecError::UnexpectedSize(UnexpectedSize {
                got: dst.len(),
                want: key.wire_len(),
            }));
        }
        key.to_be_bytes(dst.try_into().map_err(io::Error::other)?);
        // info!(dst);
        Ok(KEY_SIZE)
    }

    fn decode(&self, src: &[u8]) -> Result<Option<(Key, usize)>, CodecError> {
        if src.len() < KEY_SIZE {
            trace!(
                "not enough bytes to decode from (got = {}, want = {})",
                src.len(),
                KEY_SIZE
            );
            return Ok(None);
        }
        let k = Key::from_be_bytes(&src[..KEY_SIZE])?;
        Ok(Some((k, KEY_SIZE)))
    }
}

impl SpecCodec<Record> for DefaultCodec {
    fn encode(&self, rec: &Record, dst: &mut [u8]) -> Result<usize, CodecError> {
        if dst.len() < rec.wire_len() {
            trace!(
                "not enough bytes to encode into `dst` (got: {}, want: {})",
                dst.len(),
                rec.wire_len()
            );
            return Err(not_enough_bytes(dst.len(), rec.wire_len()));
        }

        let _ = <Self as SpecCodec<Key>>::encode(self, &rec.key, &mut dst[0..KEY_SIZE])?;

        if !(MIN_PAYLOAD_LENGTH..=MAX_PAYLOAD_LENGTH).contains(&rec.payload.len()) {
            return Err(invalid_payload_sz(rec.payload.len()));
        }

        let payload_len = (rec.payload.len() as u32).to_be_bytes();
        dst[KEY_SIZE..KEY_SIZE + PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len);
        dst[KEY_SIZE + PAYLOAD_LEN_SIZE..KEY_SIZE + PAYLOAD_LEN_SIZE + rec.payload.len()]
            .copy_from_slice(&rec.payload);

        Ok(rec.wire_len())
    }

    fn decode(&self, src: &[u8]) -> Result<Option<(Record, usize)>, CodecError> {
        if src.len() < KEY_SIZE + PAYLOAD_LEN_SIZE {
            trace!(
                "not enough bytes to decode from (src.len = {}, KEY_SIZE + PAYLOAD_LEN_SIZE = {})",
                src.len(),
                KEY_SIZE + PAYLOAD_LEN_SIZE
            );
            return Ok(None);
        }

        let (key, _) = match <Self as SpecCodec<Key>>::decode(self, src)? {
            None => return Ok(None),
            Some(t) => t,
        };

        let payload_len = u32::from_be_bytes([
            src[KEY_SIZE],
            src[KEY_SIZE + 1],
            src[KEY_SIZE + 2],
            src[KEY_SIZE + 3],
        ]) as usize;

        if !(MIN_PAYLOAD_LENGTH..=MAX_PAYLOAD_LENGTH).contains(&payload_len) {
            return Err(invalid_payload_sz(payload_len));
        }
        let total = KEY_SIZE + PAYLOAD_LEN_SIZE + payload_len;
        if src.len() < total {
            trace!("not enoguh bytes to decode from (payload)");
            return Ok(None);
        }

        let payload = src[KEY_SIZE + PAYLOAD_LEN_SIZE..total]
            .to_vec()
            .into_boxed_slice();

        Ok(Some((Record { key, payload }, total)))
    }
}

pub(super) const BUFSIZE: usize = 4 * 1024;

pub struct FramedReader<R: io::Read, C: SpecCodec<I>, I: WireLen> {
    inner: io::BufReader<R>,
    buf: Vec<u8>,
    start: usize,
    end: usize,
    codec: C,
    _phantom: PhantomData<I>,
}

impl<R, C, I> FramedReader<R, C, I>
where
    R: io::Read,
    C: SpecCodec<I>,
    I: WireLen,
{
    pub fn new(r: R, codec: C) -> Self {
        Self {
            inner: io::BufReader::new(r),
            buf: vec![0; BUFSIZE],
            start: 0,
            end: 0,
            codec,
            _phantom: PhantomData,
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
            Err(e) => Err(e),
        }
    }

    pub fn buf_reader(&mut self) -> &mut io::BufReader<R> {
        &mut self.inner
    }
}

impl<R, C, I> Iterator for FramedReader<R, C, I>
where
    R: io::Read,
    C: SpecCodec<I>,
    I: WireLen,
{
    type Item = Result<I, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.codec.decode(&self.buf[self.start..self.end]) {
                Ok(Some((item, n))) => {
                    self.start += n;
                    return Some(Ok(item));
                }
                Ok(None) => match self.fill() {
                    Ok(false) => continue,
                    Ok(true) => return None,
                    Err(e) => {
                        trace!(
                            "FramedReader::fill (Iterator::next): failed to fill internal buffer: {}",
                            e
                        );
                        return None;
                    }
                },
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl<R, C, I> Debug for FramedReader<R, C, I>
where
    R: io::Read + Debug,
    C: SpecCodec<I> + Debug,
    I: WireLen,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedReader<R, C>")
            .field("inner", &format_args!("{:?}", self.inner))
            .field("buf", &format_args!("[0..{}]", self.buf.len()))
            .field("start", &self.start)
            .field("end", &self.end)
            .field("codec", &self.codec)
            .finish()
    }
}

pub struct FramedWriter<W: Write, C: SpecCodec<I>, I: WireLen> {
    inner: BufWriter<W>,
    buf: Vec<u8>,
    codec: C,
    _phantom: PhantomData<I>,
}

impl<W, C, I> FramedWriter<W, C, I>
where
    W: io::Write,
    C: SpecCodec<I>,
    I: WireLen,
{
    pub fn new(w: W, c: C) -> Self {
        Self {
            inner: BufWriter::new(w),
            buf: vec![0; BUFSIZE],
            codec: c,
            _phantom: PhantomData,
        }
    }

    pub fn write(&mut self, item: &I) -> Result<(), CodecError> {
        self.buf.clear();
        self.buf.resize(item.wire_len(), 0);

        self.codec.encode(item, &mut self.buf)?;
        self.inner.write_all(&self.buf).map_err(CodecError::from)?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()?;
        Ok(())
    }

    pub fn buf_writer(&mut self) -> &mut BufWriter<W> {
        &mut self.inner
    }

    pub fn into_inner(self) -> Result<W, IntoInnerError<BufWriter<W>>> {
        self.inner.into_inner()
    }
}

impl<W, C, I> Debug for FramedWriter<W, C, I>
where
    W: io::Write + Debug,
    C: SpecCodec<I> + Debug,
    I: WireLen,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedWriter")
            .field(
                "inner",
                &format_args!("{:?}", std::any::type_name_of_val(&self.inner)),
            )
            .field("buf", &format_args!("[0..{}]", self.buf.len()))
            .field("codec", &self.codec)
            .finish()
    }
}
