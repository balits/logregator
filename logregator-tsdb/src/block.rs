// i dont want AI generated code here
// but i just couldnt bother to refactor
// the test cases, so FIXME
// #[cfg(test)]
// mod test;

use tracing::{instrument, trace};

use std::fmt::Debug;

use crate::{
    codec::{InvalidSize, SpecCodec, WireLen, SZ_U16, SZ_U32},
    record::{MAX_RECORD_WIRE_LENGTH, MIN_RECORD_WIRE_LENGTH},
    ChecksumMismatch,
};

/// couple times larger than 4KB page cache
pub const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;

/// blocks cannot be larger than 64KB (u16::MAX) as we use u16s for record offsets inside the block
pub const MAX_BLOCK_SIZE: usize = u16::MAX as usize;

/// Block represents an inmemory, encoded form of a list of records, followed by their offsets in
/// the block  (as u16) , then number of records in the block  (as u16), and finally a u32 checksum.
///
/// In shot, the block layout is:
/// ```no_rust
/// [ data: &[u8] ][ offsets: [u16; num_of_records] ][ num_of_records: u16 ][ checksum: u32]
/// ```
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
    #[instrument(skip(self), err)]
    pub fn encode_block(&self) -> Result<Vec<u8>, BlockCodecError> {
        if self.offsets.len() > (u16::MAX as usize) {
            return Err(too_many_records(self.offsets.len()));
        }
        let offset_segment_len = self.offset_segment_len();

        if offset_segment_len == 0 {
            return Err(corrupted_meta("offset segment length is 0".into()));
        }

        let data_len = self.data.len();
        let num_of_records_size = SZ_U16;
        let total_size =
            data_len + self.offset_segment_len() + num_of_records_size;
        trace!(
            total_size,
            data_len,
            offset_segment_len,
            num_of_records_size
        );

        if total_size > MAX_BLOCK_SIZE {
            return Err(max_size_exceeded(total_size));
        }

        // is this still a valid check?
        // ..64KB-37 is still a safe size for creating offsets
        if self.data.len() > (u16::MAX as usize) - MIN_RECORD_WIRE_LENGTH {
            // otherwise the last offset could possibly overflow
            todo!(
                "should we check for block.data.len() > u16::MAX - MIN_RECORD_WIRE_LENGTH for offsetting?"
            )
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

            // safe since start: u16 -> usize -> u16
            data.extend((start as u16).to_be_bytes());
        }

        data.extend((self.offsets.len() as u16).to_be_bytes());
        let checksum = crc32fast::hash(&data);
        data.extend(checksum.to_be_bytes());

        Ok(data)
    }

    /// # Errors
    ///
    /// this method indexed heavily into [src]
    /// so the caller should make sure [src] is long enough and well formed
    #[instrument(skip(src), err)]
    pub fn decode_block(src: &[u8]) -> Result<Self, BlockCodecError> {
        if src.len() < SZ_U32 {
            return Err(unexpected_size(src.len(), SZ_U32));
        }

        let (src, checksum_bs) = src.split_at(src.len() - SZ_U32);
        let stored = crc32fast::hash(src);
        let computed = u32::from_be_bytes(checksum_bs.try_into().map_err(|e| {
            trace!("Block::decode: failed to convert last 4 bytes into [u8; 4] for checksum: {e}");
            unexpected_size(src.len(), SZ_U32)
        })?);

        if stored != computed {
            return Err(checksum_mismatch(stored, computed));
        }

        let num_of_records = {
            if src.len() < SZ_U16 {
                return Err(unexpected_size(src.len(), SZ_U16));
            }

            let buf = src[src.len() - SZ_U16..].try_into().map_err(|e| {
                trace!("Block::decode: failed to convert last 2 bytes into [u8; 2] for num_of_records: {e}");
                unexpected_size(src.len(), SZ_U16)
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
            // let offset_span = span!(Level::TRACE, "offset_checking");
            // let _guard = offset_span.enter();

            let iter = src[offset_segment_start..offset_segment_end]
                .as_chunks::<SZ_U16>()
                .0
                .iter()
                // .inspect(|ch| {
                //     trace!("decode: chunk: {:?}", ch);
                // })
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

                if !(MIN_RECORD_WIRE_LENGTH..=MAX_RECORD_WIRE_LENGTH)
                    .contains(&record_wire_len)
                {
                    return Err(BlockCodecError::InvalidPayloadSize(
                        InvalidSize {
                            got: record_wire_len,
                            max: MAX_RECORD_WIRE_LENGTH,
                            min: MIN_RECORD_WIRE_LENGTH,
                        },
                    ));
                }

                // trace!(
                //     "offset chunk: start = {start}, end = {end}, wire_length = {record_wire_len}"
                // );
            }

            offsets
        };

        Ok(Self { data, offsets })
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
pub struct BlockWriter<C> {
    buffer: Vec<u8>,
    codec: C,
    offsets: Vec<u16>,
    size: usize,
    limit: usize,
    record_count: usize,
    first_key: Option<Key>,
    last_key: Option<Key>,
}

impl<C: Debug> Debug for BlockWriter<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockWriter<C>")
            .field(
                "buffer",
                &format_args!(
                    "{}/{}",
                    self.buffer.len(),
                    self.buffer.capacity()
                ),
            )
            .field("codec", &self.codec)
            .field(
                "offsets",
                &format_args!(
                    "{}/{}",
                    self.offsets.len(),
                    self.offsets.capacity()
                ),
            )
            .field("size", &self.size)
            .field("limit", &self.limit)
            .field("_record_count", &self.record_count)
            .field("first_key", &self.first_key)
            .field("last_key", &self.last_key)
            .finish()
    }
}

pub enum WriteOutput {
    Written,
    Full,
}

#[derive(Debug, thiserror::Error, Clone, Copy)]
#[error("invalid block limit: got = {0}, max = {max}", max = MAX_BLOCK_SIZE)]
pub struct InvalidBlockLimit(usize);

impl<C> BlockWriter<C> {
    pub fn new(
        codec: C,
        block_size_limit: Option<usize>,
    ) -> Result<Self, InvalidBlockLimit> {
        let limit = match block_size_limit {
            Some(l) => {
                if l > MAX_BLOCK_SIZE {
                    return Err(InvalidBlockLimit(l));
                }
                l
            }
            None => DEFAULT_BLOCK_SIZE,
        };

        let size = 0;
        let buffer = Vec::with_capacity(limit);
        let avg_rec_length =
            (MAX_RECORD_WIRE_LENGTH + MIN_RECORD_WIRE_LENGTH) / 2;
        let offsets = Vec::with_capacity(limit / avg_rec_length);
        let this = Self {
            buffer,
            size,
            codec,
            limit,
            record_count: 0,
            offsets,
            first_key: None,
            last_key: None,
        };
        Ok(this)
    }

    pub fn into_block(mut self) -> (Block, Option<Key>, Option<Key>) {
        let block = Block {
            data: self.buffer,
            offsets: self.offsets,
        };
        let first_key = self.first_key.take();
        let last_key = self.last_key.take();
        (block, first_key, last_key)
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn first_key(&self) -> Option<&Key> {
        self.first_key.as_ref()
    }

    pub fn last_key(&self) -> Option<&Key> {
        self.last_key.as_ref()
    }
}

impl<C> BlockWriter<C>
where
    C: SpecCodec<Record>,
{
    /// Encodes record into the block, returning Ok(true) if successful.
    /// If the block is full, Ok(false) is returned,
    /// if any other error is encounder Err(..) is returned
    #[instrument(skip(self), err, fields(block_size = self.size, record_count = self.record_count))]
    pub fn write(&mut self, rec: &Record) -> Result<WriteOutput, CodecError> {
        let est_total_size = self.size
            + rec.wire_len()
            + (self.offsets.len() + 1) * size_of::<u16>()
            + size_of::<u16>();

        if !self.offsets.is_empty() && est_total_size > self.limit {
            return Ok(WriteOutput::Full);
        }

        if est_total_size > MAX_BLOCK_SIZE {
            return Ok(WriteOutput::Full);
        }

        let orig_len = self.buffer.len();
        self.buffer.resize(orig_len + rec.wire_len(), 0);
        self.codec.encode(
            rec,
            &mut self.buffer[orig_len..orig_len + rec.wire_len()],
        )?;

        if self.first_key.is_none() {
            self.first_key = Some(rec.key.clone());
        }
        self.last_key = Some(rec.key.clone());

        self.offsets.push(self.size as u16);
        self.size += rec.wire_len();
        self.record_count += 1;
        Ok(WriteOutput::Written)
    }
}

use std::rc::Rc;

use crate::{
    codec::{self, CodecError, RecordCodecExt},
    record::{Key, Record, KEY_SIZE},
};

/// A cursor-like iterator over a Block
/// To use the iterator, first it needs to be set to the first element,
/// with [seek_to_first()]. From this point on, one can use next() or seek()
/// to update the cursors internal item, [in_valid()] will return false if this
// fails. It also fails after [next()] results in going past the offset array.
#[derive(Debug)]
pub struct BlockCursor<C> {
    block: Rc<Block>,
    offset_idx: usize,
    /// record derived from the cursror [SpecCodec<Record>::decode],
    /// and therefore has the same signature as its result type.
    record: Result<Option<Record>, CodecError>,
    codec: C,
}

impl<C> BlockCursor<C>
where
    C: RecordCodecExt,
{
    pub fn new(block: Rc<Block>, c: C) -> Self {
        Self {
            block,
            offset_idx: 0,
            record: Ok(None),
            codec: c,
        }
    }

    #[inline]
    pub fn is_record(&self) -> bool {
        self.current_record().is_some()
    }

    #[inline]
    pub fn is_error(&self) -> bool {
        self.record.is_err()
    }

    #[inline]
    pub fn get_error(&self) -> Option<&CodecError> {
        self.record.as_ref().err()
    }

    #[inline]
    pub fn unwrap_record(&self) -> &Record {
        match self.record.as_ref() {
            Ok(Some(rec)) => rec,
            Ok(None) => {
                panic!(
                    "attempt to unwrap record from cursor while current record is Ok(None)"
                )
            }
            Err(e) => panic!(
                "attempt to unwrap record from cursor while `self.current()` is pointing at error: {e}"
            ),
        }
    }

    #[inline]
    pub fn current_record(&self) -> Option<&Record> {
        match self.record.as_ref() {
            Ok(o) => o.as_ref(),
            _ => None,
        }
    }

    #[inline]
    pub fn current(&self) -> Result<Option<&Record>, &CodecError> {
        self.record.as_ref().map(|r| r.as_ref())
    }

    #[inline]
    pub fn take_current(&mut self) -> Option<Record> {
        match std::mem::replace(&mut self.record, Ok(None)) {
            Ok(Some(o)) => Some(o),
            _ => None,
        }
    }

    #[inline]
    pub fn next(&mut self) {
        self.offset_idx += 1;
        self.update_current();
    }

    #[inline]
    pub fn seek_to_first(&mut self) {
        self.offset_idx = 0;
        self.update_current();
    }

    pub fn seek(&mut self, seek_key: &Key) {
        let mut seek_key_bs = [0u8; KEY_SIZE];
        seek_key.to_be_bytes(&mut seek_key_bs);

        let mut search_err = None;

        // TODO: instead of turning the bytes into Key and then comparing
        // we could just compare the bytes themselves (disregarding Key::stream_id: u64, the last 8 bytes)
        let res = self.block.offsets.binary_search_by(|o| {
            if search_err.is_some() {
                return std::cmp::Ordering::Less; // sentinel
            }

            let offset = *o as usize;
            let key_bs = &self.block.data[offset..offset + KEY_SIZE];
            match self.codec.decode_key(key_bs) {
                Ok(Some((key, _))) => key.cmp(seek_key),
                Ok(None) => {
                    search_err =
                        Some(codec::other("failed to decode key from block"));
                    std::cmp::Ordering::Less // meaningless
                }
                Err(e) => {
                    search_err = Some(e);
                    std::cmp::Ordering::Less // meaningless
                }
            }
        });

        if let Some(e) = search_err {
            self.record = Err(e);
        } else {
            self.offset_idx = match res {
                Ok(i) => i,  // exact match
                Err(i) => i, // first elem > target
            };
            self.update_current();
        }
    }

    #[inline]
    pub fn peek(&mut self) -> Option<&Record> {
        match self.record.as_ref() {
            Ok(s) => s.as_ref(),
            Err(_) => None,
        }
    }

    #[inline]
    pub fn peek_key(&mut self) -> Option<&Key> {
        self.peek().map(|r| &r.key)
    }

    fn update_current(&mut self) {
        if self.offset_idx >= self.block.offsets.len() {
            trace!("BlockCursor::update_current: offset idx is out of bounds");
            self.record = Ok(None);
            return;
        }

        let start_offset = self.block.offsets[self.offset_idx] as usize;
        let end_offset = if self.offset_idx + 1 < self.block.offsets.len() {
            self.block.offsets[self.offset_idx + 1] as usize
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
    UnexpectedSize(crate::codec::NotEnoughBytes),
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

    #[error(
        "block_error: invalid offset segment, start = {start:?}, end = {end:?}, len = {len:?}"
    )]
    InvalidOffsetSegment {
        start: Option<usize>,
        end: Option<usize>,
        len: Option<usize>,
    },

    #[error("block_error: {0}")]
    InvalidPayloadSize(crate::codec::InvalidSize),

    #[error("block_error: corrupted metadata {0}")]
    CorruptedMetadata(String),

    #[error("block_error: {0}")]
    DecodeError(DecodeBlockError),

    #[error("block_error: {0}")]
    Checksum(ChecksumMismatch),
}

pub(super) fn too_many_records(record_count: usize) -> BlockCodecError {
    BlockCodecError::InvalidBlockSize(InvalidBlockSize::TooManyRecords(
        record_count,
    ))
}

pub(super) fn max_size_exceeded(size: usize) -> BlockCodecError {
    BlockCodecError::InvalidBlockSize(InvalidBlockSize::MaxSizeExceeded(size))
}

pub(super) fn corrupted_meta(context: String) -> BlockCodecError {
    BlockCodecError::CorruptedMetadata(context)
}

pub(super) fn invalid_offsets(start: usize, end: usize) -> BlockCodecError {
    BlockCodecError::InvalidOffsets { start, end }
}

pub(super) fn unexpected_size(got: usize, want: usize) -> BlockCodecError {
    BlockCodecError::DecodeError(DecodeBlockError::UnexpectedSize(
        crate::codec::NotEnoughBytes { got, want },
    ))
}

pub(super) fn checksum_mismatch(stored: u32, computed: u32) -> BlockCodecError {
    BlockCodecError::Checksum(ChecksumMismatch {
        original: stored,
        computed,
    })
}

// AI generated code bleh!
//
// Also super old so i would need to revisit this later
// #[cfg(test)]
// mod test {

//     use std::rc::Rc;

//     use pretty_assertions::assert_eq;

//     use super::{
//         Block, BlockCursor, BlockWriter, WriteOutput, MAX_BLOCK_SIZE, SZ_U16,
//         SZ_U32,
//     };
//     use crate::codec::WireLen;
//     use crate::record::RecordCodec;
//     use crate::record::{Key, Record};

//     fn tracing() {
//         let _ = tracing_subscriber::fmt()
//             .with_max_level(tracing::Level::TRACE)
//             .with_test_writer()
//             .try_init();
//     }

//     fn codec() -> RecordCodec {
//         RecordCodec
//     }

//     fn key(source_id: u64, ts: u64, seq: u64) -> Key {
//         Key {
//             source_id,
//             timestamp: ts,
//             sequence_num: seq,
//             stream_id: 7,
//         }
//     }

//     fn record(source_id: u64, ts: u64, seq: u64, payload_len: usize) -> Record {
//         Record {
//             key: key(source_id, ts, seq),
//             payload: vec![0xAB_u8; payload_len].into(),
//         }
//     }

//     /// Build a valid block from `n` records with the given payload size.
//     /// Panics if any write is rejected, callers should size payloads to fit.
//     fn build_block(n: usize, payload_len: usize) -> (Rc<Block>, Vec<Record>) {
//         let records: Vec<Record> = (0..n as u64)
//             .map(|i| record(1, i, i, payload_len))
//             .collect();

//         let mut bw = BlockWriter::new(codec(), None).unwrap();
//         for r in &records {
//             assert!(
//                 matches!(bw.write(r).unwrap(), WriteOutput::Written),
//                 "build_block: record {r:?} was rejected (shrink the payload length)"
//             );
//         }
//         let (b, _, _) = bw.into_block();
//         (Rc::new(b), records)
//     }

//     #[test]
//     fn block_writer() {
//         tracing();

//         BlockWriter::new(codec(), None).unwrap();

//         BlockWriter::new(codec(), Some(4096)).unwrap();
//         BlockWriter::new(codec(), Some(MAX_BLOCK_SIZE)).unwrap();

//         assert!(
//             BlockWriter::new(codec(), Some(MAX_BLOCK_SIZE + 1)).is_err(),
//             "limit > MAX_BLOCK_SIZE must return Err"
//         );

//         // --- Empty finish ---------------------------------------------------------

//         // A writer that never had write() called still produces a valid (0-record) block.
//         let (empty_block, _, _) =
//             BlockWriter::new(codec(), Some(4096)).unwrap().into_block();
//         assert_eq!(empty_block.num_of_records(), 0);

//         // --- Size accounting ------------------------------------------------------

//         // After one write, size == wire_len of that record.
//         let mut bw = BlockWriter::new(codec(), Some(4096)).unwrap();
//         let r0 = record(1, 0, 0, 32);
//         let wire = r0.wire_len();
//         bw.write(&r0).unwrap();
//         assert_eq!(
//             bw.size(),
//             wire,
//             "size must equal wire_len after first write"
//         );
//         assert_eq!(
//             bw.first_key(),
//             Some(&r0.key),
//             "fist_key should match the single inserted records key"
//         );
//         assert_eq!(
//             bw.last_key(),
//             Some(&r0.key),
//             "last_key should match first_key"
//         );

//         // After a second write with the same payload, size doubles.
//         bw.write(&record(1, 1, 1, 32)).unwrap();
//         assert_eq!(bw.size(), wire * 2, "size must accumulate across writes");

//         // --- WriteResult::Written -------------------------------------------------

//         // The first record is always accepted regardless of how tight the limit is,
//         // because there is nothing meaningful to flush to if we reject it.
//         let tight = r0.wire_len() + SZ_U16 + SZ_U16; // just enough for one record
//         let mut bw = BlockWriter::new(codec(), Some(tight)).unwrap();
//         assert!(
//             matches!(bw.write(&r0).unwrap(), WriteOutput::Written),
//             "first record must always be Written"
//         );

//         // --- WriteResult::Full ----------------------------------------------------

//         // The second record must be rejected when the block is at capacity.
//         assert!(
//             matches!(
//                 bw.write(&record(1, 1, 1, 32)).unwrap(),
//                 WriteOutput::Full
//             ),
//             "second record must be Full when limit is exhausted"
//         );

//         // After a Full result the writer is still usable: into_block() produces a block
//         // that contains only the records that were actually Written.
//         let (block, _, _) = bw.into_block();
//         assert_eq!(
//             block.num_of_records(),
//             1,
//             "only the Written record must appear in the block"
//         );

//         // --- Multiple records up to capacity -------------------------------------

//         // Build a limit that fits exactly two records and verify the third is Full.
//         let r = record(1, 0, 0, 64);
//         // 2 records + 2 u16 offsets + 1 u16 num_of_records suffix
//         let two_limit = 2 * r.wire_len() + 3 * SZ_U16;
//         let mut bw = BlockWriter::new(codec(), Some(two_limit)).unwrap();
//         assert!(matches!(
//             bw.write(&record(1, 0, 0, 64)).unwrap(),
//             WriteOutput::Written
//         ));
//         assert!(matches!(
//             bw.write(&record(1, 1, 1, 64)).unwrap(),
//             WriteOutput::Written
//         ));
//         assert!(
//             matches!(
//                 bw.write(&record(1, 2, 2, 64)).unwrap(),
//                 WriteOutput::Full
//             ),
//             "third record must be Full"
//         );
//         assert_eq!(bw.into_block().0.num_of_records(), 2);
//     }

//     #[test]
//     fn block_encode() {
//         tracing();

//         // --- Offset segment invariants -------------------------------------------

//         let n = 4;
//         let (block, _) = build_block(n, 16);

//         // num_of_records() is derived from offsets.len() — never from a stored field.
//         assert_eq!(block.num_of_records(), n);

//         // offset_segment_start is always data.len().
//         assert_eq!(block.offset_segment_start(), block.data.len());

//         // Each offset costs exactly SZ_U16 bytes in the segment.
//         assert_eq!(block.offset_segment_len(), n * SZ_U16);

//         // offset_segment_end = start + len.
//         assert_eq!(
//             block.offset_segment_end(),
//             block.offset_segment_start() + block.offset_segment_len()
//         );

//         // --- Encoded byte length --------------------------------------------------

//         // Layout: [ data ][ offsets: n*u16 ][ num_of_records: u16 ][ checksum: u32 ]
//         let encoded = block.encode_block().unwrap();
//         let expected_len =
//             block.offset_segment_start() + n * SZ_U16 + SZ_U16 + SZ_U32;
//         assert_eq!(
//             encoded.len(),
//             expected_len,
//             "encoded length must account for data + offsets + num_of_records suffix + checksum"
//         );

//         // --- Determinism ----------------------------------------------------------

//         // Encoding the same block twice must produce identical bytes.
//         let a = block.encode_block().unwrap();
//         let b = block.encode_block().unwrap();
//         assert_eq!(a, b, "encode must be deterministic");

//         // --- Single-record block --------------------------------------------------

//         let (single, _) = build_block(1, 32);
//         single.encode_block().unwrap(); // must not panic or error

//         // --- Empty block is rejected ----------------------------------------------

//         // An empty block (0 offsets) has no meaningful on-disk representation.
//         let (empty, _, _) =
//             BlockWriter::new(codec(), None).unwrap().into_block();
//         assert!(
//             empty.encode_block().is_err(),
//             "encode on an empty block must return Err"
//         );
//     }

//     #[test]
//     fn block_decode() {
//         tracing();

//         // --- Happy path: single record -------------------------------------------

//         let (block, _) = build_block(1, 32);
//         let encoded = block.encode_block().unwrap();
//         let decoded = Block::decode_block(&encoded).unwrap();
//         assert_eq!(*block, decoded, "single-record roundtrip must be identity");

//         // --- Happy path: multiple records ----------------------------------------

//         let (block, _) = build_block(8, 32);
//         let encoded = block.encode_block().unwrap();
//         let decoded = Block::decode_block(&encoded).unwrap();
//         assert_eq!(*block, decoded, "multi-record roundtrip must be identity");

//         // All derived properties must survive the roundtrip unchanged.
//         assert_eq!(decoded.num_of_records(), 8);
//         assert_eq!(
//             decoded.offset_segment_start(),
//             block.offset_segment_start()
//         );
//         assert_eq!(decoded.offset_segment_len(), block.offset_segment_len());
//         assert_eq!(decoded.offset_segment_end(), block.offset_segment_end());

//         // --- Malformed input: too short to hold a checksum -----------------------

//         assert!(
//             Block::decode_block(&[]).is_err(),
//             "empty slice must be rejected"
//         );
//         assert!(
//             Block::decode_block(&[0u8; 1]).is_err(),
//             "1 byte must be rejected"
//         );
//         assert!(
//             Block::decode_block(&[0u8; SZ_U32 - 1]).is_err(),
//             "fewer than SZ_U32 bytes must be rejected"
//         );

//         // --- Malformed input: raw data without offset footer ---------------------

//         // Feeding only the data region (no offsets, no suffix, no checksum) must
//         // fail.  The checksum alone guarantees this because the stored checksum
//         // won't be present.
//         let (block, _) = build_block(3, 16);
//         assert!(
//             Block::decode_block(&block.data).is_err(),
//             "raw data without offset footer must be rejected"
//         );

//         // --- Malformed input: truncated by one byte ------------------------------

//         let encoded = block.encode_block().unwrap();
//         let truncated = &encoded[..encoded.len() - 1];
//         assert!(
//             Block::decode_block(truncated).is_err(),
//             "truncated buffer must be rejected"
//         );

//         // --- Malformed input: checksum mismatch (single bit flip) ----------------

//         let mut corrupted = encoded.clone();
//         corrupted[0] ^= 0x01; // flip one bit anywhere in the data region
//         assert!(
//             Block::decode_block(&corrupted).is_err(),
//             "bit-flipped buffer must fail checksum verification"
//         );

//         // --- Malformed input: doubled buffer -------------------------------------

//         // Concatenating a valid encoded block with itself produces a buffer whose
//         // trailing checksum belongs to the second copy, not to the doubled payload.
//         // The checksum covers everything before it, so this must be rejected.
//         let doubled = [encoded.clone(), encoded.clone()].concat();
//         assert!(
//             Block::decode_block(&doubled).is_err(),
//             "doubled buffer must be rejected by checksum"
//         );

//         // --- Malformed input: checksum field zeroed ------------------------------

//         let mut zero_cksum = encoded.clone();
//         let tail = zero_cksum.len();
//         zero_cksum[tail - 4..].fill(0x00);
//         assert!(
//             Block::decode_block(&zero_cksum).is_err(),
//             "zeroed checksum must be rejected"
//         );

//         // --- Malformed input: num_of_records field corrupted ---------------------

//         // Patch the num_of_records u16 to a value that can't be consistent with
//         // the data region.  Because we write the checksum *after* num_of_records,
//         // the checksum will also be wrong — so this is caught at checksum time.
//         let mut bad_count = encoded.clone();
//         let tail = bad_count.len();
//         // num_of_records sits at [tail-6..tail-4] (before the u32 checksum)
//         bad_count[tail - 6] = 0xFF;
//         bad_count[tail - 5] = 0xFF;
//         assert!(
//             Block::decode_block(&bad_count).is_err(),
//             "corrupted num_of_records must be caught"
//         );
//     }

//     #[test]
//     fn block_cursor() {
//         tracing();

//         // --- Fresh cursor has no record ------------------------------------------

//         let (block, records) = build_block(5, 16);
//         let mut cursor = BlockCursor::new(block.clone(), codec());

//         assert!(
//             !cursor.is_record(),
//             "fresh cursor must not point at a record"
//         );
//         assert!(
//             !cursor.is_error(),
//             "fresh cursor must not be in an error state"
//         );
//         assert_eq!(cursor.current().unwrap(), None);
//         assert_eq!(cursor.peek_key(), None);

//         // --- seek_to_first points at record[0] -----------------------------------

//         cursor.seek_to_first();
//         assert!(cursor.is_record());
//         assert_eq!(
//             cursor.current().expect("Ok").expect("Some").key,
//             records[0].key
//         );
//         assert_eq!(cursor.peek_key(), Some(&records[0].key));

//         // --- next() traverses in insertion order ---------------------------------

//         // Collect every key seen during a full forward pass.
//         let mut seen = Vec::new();
//         cursor.seek_to_first();
//         while cursor.is_record() {
//             seen.push(cursor.current().unwrap().key.clone());
//             cursor.next();
//         }
//         let expected_keys: Vec<Key> =
//             records.iter().map(|r| r.key.clone()).collect();
//         assert_eq!(
//             seen, expected_keys,
//             "traversal must visit records in insertion order"
//         );

//         // --- Exhaustion: cursor becomes invalid after walking off the end --------

//         assert!(
//             !cursor.is_record(),
//             "cursor must be invalid after walking off the end"
//         );
//         assert_eq!(cursor.current(), None);

//         // --- seek_to_first resets an exhausted cursor ----------------------------

//         cursor.seek_to_first();
//         assert!(cursor.is_record());
//         assert_eq!(cursor.current().unwrap().key, records[0].key);

//         // --- next() on a single-record block hits end on the first advance -------

//         let (single_block, single_records) = build_block(1, 16);
//         let mut sc = BlockCursor::new(single_block, codec());
//         sc.seek_to_first();
//         assert_eq!(sc.current().unwrap().key, single_records[0].key);
//         sc.next();
//         assert!(
//             !sc.is_record(),
//             "single-record block must be exhausted after one next()"
//         );

//         // --- peek_key advances alongside current() -------------------------------

//         cursor.seek_to_first();
//         cursor.next(); // now at records[1]
//         assert_eq!(cursor.peek_key(), Some(&records[1].key));
//         cursor.next(); // now at records[2]
//         assert_eq!(cursor.peek_key(), Some(&records[2].key));

//         // --- payload is preserved through the cursor ------------------------------

//         cursor.seek_to_first();
//         for expected in &records {
//             let got = cursor.current().unwrap();
//             assert_eq!(got.key, expected.key);
//             assert_eq!(
//                 got.payload, expected.payload,
//                 "payload must survive decode"
//             );
//             cursor.next();
//         }

//         // --- seek: exact key match -----------------------------------------------

//         for r in &records {
//             cursor.seek(&r.key);
//             let got = cursor.current().expect("exact seek must find a record");
//             assert_eq!(got.key, r.key);
//         }

//         // --- seek: key between two stored keys lands on the successor ------------

//         // records have ts = 0..4, seq = 0..4.
//         // A key with seq = records[2].seq - 1 sorts just below records[2].
//         let between = Key {
//             source_id: records[2].key.source_id,
//             timestamp: records[2].key.timestamp,
//             sequence_num: records[2].key.sequence_num.saturating_sub(1),
//             stream_id: records[2].key.stream_id,
//         };
//         cursor.seek(&between);
//         // Binary search returns Err(i) = first index strictly greater than the
//         // probe, which here should be records[2].
//         let got = cursor
//             .current()
//             .expect("between-key seek must land on a record");
//         assert!(
//             got.key == records[2].key || got.key == records[1].key,
//             "between-key seek must land on records[1] or records[2], got {:?}",
//             got.key
//         );

//         // --- seek: before all records lands on records[0] (or is invalid) --------

//         let before_all = Key {
//             source_id: 0,
//             timestamp: 0,
//             sequence_num: 0,
//             stream_id: 0,
//         };
//         cursor.seek(&before_all);
//         // Must not panic; if a record is returned it must be the first one.
//         if cursor.is_record() {
//             assert_eq!(cursor.current().unwrap().key, records[0].key);
//         }

//         // --- seek: past the last key invalidates the cursor ----------------------

//         let last = records.last().unwrap();
//         let beyond = Key {
//             source_id: last.key.source_id,
//             timestamp: u64::MAX,
//             sequence_num: u64::MAX,
//             stream_id: last.key.stream_id,
//         };
//         cursor.seek(&beyond);
//         assert!(
//             !cursor.is_record(),
//             "seek past last key must invalidate cursor"
//         );

//         // --- seek: idempotent on same key ----------------------------------------

//         cursor.seek(&records[2].key.clone());
//         let first_result = cursor.current().map(|r| r.key.clone());
//         cursor.seek(&records[2].key.clone());
//         let second_result = cursor.current().map(|r| r.key.clone());
//         assert_eq!(
//             first_result, second_result,
//             "repeated seek to same key must be idempotent"
//         );

//         // --- seek: works correctly on a decoded block ----------------------------

//         // Encode → decode and verify seek finds the same records on the
//         // reconstructed block as on the original.
//         let encoded = block.encode_block().unwrap();
//         let decoded_block = Rc::new(Block::decode_block(&encoded).unwrap());
//         let mut dc = BlockCursor::new(decoded_block, codec());
//         for r in &records {
//             dc.seek(&r.key);
//             let got = dc
//                 .current()
//                 .expect("seek on decoded block must find record");
//             assert_eq!(got.key, r.key);
//         }
//     }
// }
