use tracing::{Level, instrument, span, trace};

use crate::{
    codec::{self, Codec, FramedWriter, InvalidPayloadSize},
    record::{KEY_SIZE, Key, MAX_RECORD_WIRE_LENGTH, MIN_RECORD_WIRE_LENGTH, Record},
};
use std::{fmt::Debug, io, sync::Arc};

const SZ_U16: usize = size_of::<u16>();
/// couple times larger than 4KB page cache
const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;
/// blocks cannot be larger than 64KB (u16::MAX) as we use u16s for record offsets inside the block
const MAX_BLOCK_SIZE: usize = u16::MAX as usize;

// /// Metadata about a block, such as where the number of records in the block or
// /// or where offset segment starts and ends
// #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
// pub struct Metadata {
//     pub offset_segment_start: usize,
//     pub offset_segment_end: usize,
//     pub num_of_records: usize,
// }

/// Block represents an inmemory, encoded form of a list of records, followed by their offsets in
/// the block, then suffixed by the number of records in the block.
///
/// Each record is encoded into the [data] vector by a [BlockWriter]. In the following offset segment, each
/// records starting position is recorded as an u16, then the whole block will be suffixed by the number of records
#[derive(PartialEq, Eq)]
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn num_of_records(&self) -> usize {
        self.offsets.len()
    }

    pub fn offset_segment_start(&self) -> usize {
        self.data.len()
    }

    pub fn offset_segment_len(&self) -> usize {
        self.offsets.len() * SZ_U16
    }

    pub fn offset_segment_end(&self) -> usize {
        self.offset_segment_start() + self.offset_segment_len()
    }
}

impl Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("data", &format_args!("[0..{}]", self.data.len()))
            .field("offsets", &self.offsets)
            .finish()
    }
}

impl Block {
    #[instrument(err, skip(self))]
    pub fn encode(&self) -> Result<Vec<u8>, BlockCodecError> {
        if self.offsets.len() > (u16::MAX as usize) {
            // invalid_block_size
            return Err(too_many_records(self.offsets.len()));
        }
        let offset_segment_len = self.offset_segment_len();

        if offset_segment_len == 0 {
            return Err(corrupted_meta("offset segment length is 0".into()));
        }

        let data_len = self.data.len();
        let num_of_records_size = SZ_U16;
        let total_size = data_len + self.offset_segment_len() + num_of_records_size;
        trace!(
            total_size,
            data_len, offset_segment_len, num_of_records_size
        );

        if total_size > MAX_BLOCK_SIZE {
            return Err(max_size_exceeded(total_size));
        }

        // is this still a valid check?
        // ..64KB-37 is still a safe size for creating offsets
        if self.data.len() > (u16::MAX as usize) - MIN_RECORD_WIRE_LENGTH {
            // otherwise the last offset could possibly overflow
            todo!()
        }

        let mut data = self.data.clone();

        data.reserve_exact(offset_segment_len + SZ_U16);

        let offset_len = self.offsets.len();
        for i in 0..offset_len {
            let start = self.offsets[i] as usize;
            let end = if i < self.offsets.len() - 1 {
                self.offsets[i + 1] as usize
            } else {
                self.offset_segment_start()
            };

            if end <= start {
                trace!(
                    "offsets where not strictly monotonic at index {i}/{offset_len}: end = {end} <= start {start}"
                );

                return Err(invalid_offsets(start, end));
            }

            // converting back and forth usize <-> u16 its safe
            data.extend((start as u16).to_be_bytes());
        }

        data.extend((self.offsets.len() as u16).to_be_bytes());
        Ok(data)
    }

    /// # Errors
    ///
    /// this method indexed heavily into [src]
    /// so the caller should make sure [src] is long enough and well formed
    #[instrument(skip(src), err)]
    pub fn decode(src: &[u8]) -> Result<Self, BlockCodecError> {
        let num_of_records = {
            if src.len() < SZ_U16 {
                return Err(unexpected_size(src.len(), SZ_U16));
            }

            let buf = src[src.len() - SZ_U16..].try_into().map_err(|e| {
                trace!("Block::decode: failed to convert last 2 bytes into [u8; 2]: {e}");
                num_of_records_missing()
            })?;

            u16::from_be_bytes(buf) as usize
        };

        let offset_segment_len = num_of_records * SZ_U16;
        // no overflow due to the num_of_record block's check
        let offset_segment_end = src.len() - SZ_U16;

        if offset_segment_end < offset_segment_len {
            return Err(BlockCodecError::InvalidOffsetSegment {
                start: None,
                end: Some(offset_segment_end),
                len: Some(offset_segment_len),
            });
        }

        let offset_segment_start = offset_segment_end - offset_segment_len;

        trace!(
            src_len = src.len(),
            offset_start = offset_segment_start,
            offset_end = offset_segment_end,
            offset_len = offset_segment_len,
        );

        let data = src[..offset_segment_start].to_vec();

        let offsets = {
            let offset_span = span!(Level::TRACE, "offset_checking");
            let _guard = offset_span.enter();

            let iter = src[offset_segment_start..offset_segment_end]
                .as_chunks::<SZ_U16>()
                .0
                .iter()
                .inspect(|ch| {
                    trace!("decode: chunk: {:?}", ch);
                })
                .map(|[a, b]| u16::from_be_bytes([*a, *b]));

            let mut offsets = Vec::with_capacity(num_of_records);
            offsets.extend(iter);

            for i in 0..offsets.len() {
                let start = offsets[i] as usize;
                let end = if i < offsets.len() - 1 {
                    offsets[i + 1] as usize
                } else {
                    offset_segment_start
                };

                if end <= start {
                    return Err(invalid_offsets(start, end));
                }

                let record_wire_len = end - start;

                if !(MIN_RECORD_WIRE_LENGTH..=MAX_RECORD_WIRE_LENGTH).contains(&record_wire_len) {
                    return Err(BlockCodecError::InvalidPayloadSize(
                        InvalidPayloadSize::new(record_wire_len),
                    ));
                }

                trace!(
                    "offset chunk: start = {start}, end = {end}, wire_length = {record_wire_len}"
                );
            }

            offsets
        };

        Ok(Self { data, offsets })
    }
}

/// A cursor-like iterator over a Block
/// To use the iterator, first it needs to be set to the first element,
/// with [seek_to_first()]. From this point on, one can use next() or seek()
/// to update the cursors internal item, [in_valid()] will return false if this
// fails. It also fails after [next()] results in going past the offset array.
pub struct BlockCursor<C: Codec> {
    block: Arc<Block>,
    curr_offset_idx: usize,
    record: Result<Option<Record>, C::Error>,
    codec: C,
}

impl<C: Codec> BlockCursor<C> {
    pub fn new(block: Arc<Block>, c: C) -> Self {
        Self {
            block,
            curr_offset_idx: 0,
            record: Ok(None),
            codec: c,
        }
    }

    pub fn is_ok(&self) -> bool {
        self.record.is_ok()
    }

    pub fn is_record(&self) -> bool {
        self.current().is_some()
    }

    pub fn unwrap_err(&self) -> Option<&C::Error> {
        self.record.as_ref().err()
    }

    pub fn current(&self) -> Option<&Record> {
        match self.record.as_ref() {
            Ok(o) => o.as_ref(),
            _ => None,
        }
    }

    pub fn next(&mut self) {
        self.curr_offset_idx += 1;
        self.update_current();
    }

    pub fn seek_to_first(&mut self) {
        self.curr_offset_idx = 0;
        self.update_current();
    }

    pub fn seek(&mut self, seek_key: Key) {
        let mut seek_key_bs = [0u8; KEY_SIZE];
        seek_key.to_be_bytes(&mut seek_key_bs);

        // TODO: instead of turning the bytes into Key and then comparing
        // we could just compare the bytes themselves (disregarding Key::stream_id: u64, the last 8 bytes)
        let res = self.block.offsets.binary_search_by(|o| {
            let offset = *o as usize;
            let key_bs = &self.block.data[offset..offset + KEY_SIZE];
            let key = Key::from_be_bytes(key_bs).expect("BlockIter::seek: failed to decode key");
            key.cmp(&seek_key)
        });

        self.curr_offset_idx = match res {
            Ok(i) => i,  // exact match
            Err(i) => i, // first elem > target
        };
        self.update_current();
    }

    pub fn peek_key(&mut self) -> Option<&Key> {
        match self.record.as_ref() {
            Ok(s) => s.as_ref().map(|r| &r.key),
            Err(_) => None,
        }
    }

    fn update_current(&mut self) {
        if self.curr_offset_idx >= self.block.offsets.len() {
            trace!("update_current: offset idx walked the length of the array");
            self.record = Ok(None);
            return;
        }

        let start_offset = self.block.offsets[self.curr_offset_idx] as usize;
        let end_offset = if self.curr_offset_idx + 1 < self.block.offsets.len() {
            self.block.offsets[self.curr_offset_idx + 1] as usize
        } else {
            self.block.offset_segment_start()
        };

        let record_bs = &self.block.data[start_offset..end_offset];
        match self.codec.decode(record_bs) {
            Ok(Some((rec, _))) => {
                trace!("update_current: decode succesfull");
                self.record = Ok(Some(rec));
            }
            Ok(None) => {
                trace!("update_current: decode returned None");
                self.record = Ok(None);
            }
            Err(e) => {
                trace!("update_current: failed to decode Record: {:?}", e);
                self.record = Err(e);
            }
        };
    }
}

/// A struct used to encode records into SST blocks.
/// It keeps a list of offsets where each records starts,
/// and its bound by [limit] in size, usually 4KB.
/// Internally it looks like [record_1, record_2, ...] [offset_1, offset_2, ...] [num_of_records]
///
/// # NOTE:
///
/// Calculating the blocks size will always use [Record::wire_len]
/// instead of its in-memory counterpart [Record::size_of]
#[derive(Debug)]
pub struct BlockWriter<C: Codec> {
    framed: FramedWriter<Vec<u8>, C>,
    offsets: Vec<u16>,
    size: usize,
    limit: usize,
}

pub enum WriteResult {
    Written,
    Full,
}

#[derive(Debug, thiserror::Error, Clone, Copy)]
#[error("invalid block limit: got = {0}, max = {max}", max = MAX_BLOCK_SIZE)]
pub struct InvalidBlockWriterLimit(usize);

impl<C: Codec> BlockWriter<C> {
    #[instrument(err)]
    pub fn new(c: C, limit: Option<usize>) -> Result<Self, InvalidBlockWriterLimit> {
        let limit = match limit {
            Some(l) => {
                if l > MAX_BLOCK_SIZE {
                    return Err(InvalidBlockWriterLimit(l));
                }
                l
            }
            None => DEFAULT_BLOCK_SIZE,
        };

        let size = 0;
        let w = Vec::with_capacity(limit);
        let framed = FramedWriter::new(w, c);
        let offsets =
            Vec::with_capacity(limit / (MAX_RECORD_WIRE_LENGTH + MIN_RECORD_WIRE_LENGTH / 2));
        let this = Self {
            size,
            framed,
            limit,
            offsets,
        };
        Ok(this)
    }

    /// Encodes record into the block, returning Ok(true) if successful.
    /// If the block is full, Ok(false) is returned,
    /// if any other error is encounder Err(..) is returned
    pub fn write(&mut self, rec: &Record) -> Result<WriteResult, C::Error> {
        let est_total_size = self.size
            + rec.wire_len()
            + (self.offsets.len() + 1) * size_of::<u16>()
            + size_of::<u16>();

        if !self.offsets.is_empty() && est_total_size > self.limit {
            return Ok(WriteResult::Full);
        }

        if est_total_size > MAX_BLOCK_SIZE {
            return Ok(WriteResult::Full);
        }

        self.framed.write(rec)?;
        self.offsets.push(self.size as u16);
        self.size += rec.wire_len();
        Ok(WriteResult::Written)
    }

    /// flush the internal buffer, appends the offset footer and
    /// the [num_of_records] to W.
    /// The offsets are written and [num_of_records] as u16.
    pub fn finish(self) -> io::Result<Block> {
        let data = self.framed.into_inner().inspect_err(|e| {
            trace!(
                "failed to unwrap FramedWriter<W, C>'s inner W (out from its BufWriter<W>): {:?}",
                e
            )
        })?;

        let block = Block {
            data,
            offsets: self.offsets,
        };
        Ok(block)
    }

    pub fn size(&self) -> usize {
        self.size
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum InvalidBlockSize {
    /// This is derived from the block.offsets.len()
    #[error("invalid_block_size: too many records in block (got = {0}, max = {max}), this would result in an u16 overflow at the [num_of_records] suffix", max = u16::MAX)]
    TooManyRecords(usize),

    #[error("invalid_block_size: max size exceeded (got = {0}, max = {max})", max = MAX_BLOCK_SIZE)]
    MaxSizeExceeded(usize),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum DecodeBlockError {
    #[error(
        "decode_block_error: failed to decode num_of_records: u16 suffix from the end of the raw bytes"
    )]
    NumOfRecordsMissing,

    #[error("decode_block_error: failed to decode raw bytes: {0}")]
    UnexpectedSize(codec::UnexpectedSize),
}

/// BlockCodecError encapsulates any error that could occurr during encoding/decoding
/// blocks
#[derive(Debug, Clone, thiserror::Error)]
pub enum BlockCodecError {
    #[error("block_error: {0}")]
    InvalidBlockSize(InvalidBlockSize),

    /// returned when offsets are malformed such as when they are not strictly monotonic
    #[error("block_error: invalid offsets, start = {start}, end = {end}")]
    InvalidOffsets { start: usize, end: usize },

    #[error("block_error: invalid offset segment, start = {start:?}, end = {end:?}, len = {len:?}")]
    InvalidOffsetSegment {
        start: Option<usize>,
        end: Option<usize>,
        len: Option<usize>,
    },

    #[error("block_error: {0}")]
    InvalidPayloadSize(InvalidPayloadSize),

    #[error("block_error: corrupted metadata {0}")]
    CorruptedMetadata(String),

    #[error("block_error: {0}")]
    DecodeError(DecodeBlockError),
}

fn too_many_records(record_count: usize) -> BlockCodecError {
    BlockCodecError::InvalidBlockSize(InvalidBlockSize::TooManyRecords(record_count))
}

fn max_size_exceeded(size: usize) -> BlockCodecError {
    BlockCodecError::InvalidBlockSize(InvalidBlockSize::MaxSizeExceeded(size))
}

fn corrupted_meta(context: String) -> BlockCodecError {
    BlockCodecError::CorruptedMetadata(context)
}

fn invalid_offsets(start: usize, end: usize) -> BlockCodecError {
    BlockCodecError::InvalidOffsets { start, end }
}

fn num_of_records_missing() -> BlockCodecError {
    BlockCodecError::DecodeError(DecodeBlockError::NumOfRecordsMissing)
}

fn unexpected_size(got: usize, want: usize) -> BlockCodecError {
    BlockCodecError::DecodeError(DecodeBlockError::UnexpectedSize(codec::UnexpectedSize {
        got,
        want,
    }))
}

#[cfg(test)]
mod test {
    use crate::codec::BytesCodec;

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn block_writer() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .try_init();

        let c = BytesCodec;
        let payload_byte = 67u8;
        let num_kb = 5;
        let limit = num_kb * 1024;
        let payload_len = (limit / num_kb) - size_of::<Key>() - size_of::<u16>();
        //                                                                  ^^^ subtract num_of_records encoded as u16
        let mut bw = BlockWriter::new(c, Some(limit)).expect("failed to create BlockWriter");

        let source_id = 6;
        let records: Vec<Record> = (0..10)
            .map(|i| {
                #[allow(unused_parens)]
                Record {
                    key: Key {
                        source_id,
                        timestamp: i,
                        sequence_num: i,
                        stream_id: 7,
                    },
                    payload: vec![payload_byte; payload_len].into_boxed_slice(),
                }
            })
            .collect();

        let taken = 4;
        for r in records.iter().take(taken) {
            match bw.write(r) {
                Ok(WriteResult::Written) => {
                    println!("record (ts: {}) appended", r.key.sequence_num);
                }
                Ok(WriteResult::Full) => {
                    println!(
                        "blcok was full: failed to write record (ts: {}, wire_len: {}) (block.size = {}, block.limit = {})",
                        r.key.sequence_num,
                        r.wire_len(),
                        bw.size,
                        bw.limit
                    );
                }
                Err(e) => {
                    println!(
                        "failed to append record (ts: {}): {:?}",
                        r.key.sequence_num, e
                    )
                }
            }
        }

        dbg!(&bw);
        let block = Arc::new(bw.finish().expect("failed to finish writing block"));
        dbg!(&block);
        assert_eq!(taken, block.num_of_records());
        assert_eq!(block.data.len(), block.offset_segment_start());

        let mut bi = BlockCursor::new(block.clone(), c);

        assert!(!bi.is_ok());
        bi.seek_to_first();
        assert!(bi.is_ok());

        while bi.is_ok() {
            let rec = bi.current();
            assert!(rec.is_some());
            match rec {
                Some(rec) => {
                    println!("BlockIter, current rec: {:?}", rec)
                }
                None => {
                    println!("BlockIter, current was None");
                }
            }
            bi.next();
        }

        assert_eq!(None, bi.current());

        bi.seek_to_first();
        assert!(bi.is_ok());

        for i in 0..taken {
            let search_key = Key {
                source_id,
                timestamp: i as u64,
                sequence_num: i as u64,
                stream_id: Default::default(),
            };

            bi.seek(search_key);
            match bi.current() {
                Some(rec) => {
                    println!("BlockIter, current rec: {:?}", rec)
                }
                None => {
                    println!("BlockIter, current was None");
                }
            }
        }

        let raw_block = block.encode().expect("failed to encode block");

        let block2 = Block::decode(&raw_block).expect("failed to decode `encoded` block ");
        //println!("{:?}", raw_block);

        println!("{:?}", block);
        println!("{:?}", block2);
        assert_eq!(*block, block2);

        let invalid_block_too_little_malformed = block.data.clone();
        let res = Block::decode(&invalid_block_too_little_malformed);
        dbg!(&res);
        assert!(
            res.is_err(),
            "decoding an invalid block should return io::Error"
        );

        let mut invalid_block_too_big_malformed = raw_block.clone();
        invalid_block_too_big_malformed.extend(raw_block.clone());
        let res = Block::decode(&invalid_block_too_big_malformed);
        dbg!(&res);
        assert!(
            res.is_err(),
            "decoding an invalid block should return io::Error"
        );
    }
}
