use tracing::instrument;

use crate::{
    block::{Block, DEFAULT_BLOCK_SIZE, MAX_BLOCK_SIZE},
    codec::{Codec, CodecError},
    record::{Key, MAX_RECORD_WIRE_LENGTH, MIN_RECORD_WIRE_LENGTH, Record},
};

/// A struct used to encode records into SST blocks.
/// It keeps a list of offsets where each records starts,
/// and its bound by [limit] in size, usually 4KB.
/// Internally it looks like [record_1, record_2, ...] [offset_1, offset_2, ...] [num_of_records]
///
/// # NOTE:
///
/// Calculating the blocks size will always use [Record::wire_len]
/// instead of its in-memory counterpart [Record::size_of]
pub struct BlockWriter<C: Codec> {
    buffer: Vec<u8>,
    codec: C,
    offsets: Vec<u16>,
    size: usize,
    limit: usize,
    _record_count: usize,
    first_key: Option<Key>,
    last_key: Option<Key>,
}

impl<C: Codec> std::fmt::Debug for BlockWriter<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockWriter<C>")
            .field(
                "buffer",
                &format_args!("{}/{}", self.buffer.len(), self.buffer.capacity()),
            )
            .field("codec", &self.codec)
            .field(
                "offsets",
                &format_args!("{}/{}", self.offsets.len(), self.offsets.capacity()),
            )
            .field("size", &self.size)
            .field("limit", &self.limit)
            .field("_record_count", &self._record_count)
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

impl<C: Codec> BlockWriter<C> {
    #[instrument(err)]
    pub fn new(codec: C, limit: Option<usize>) -> Result<Self, InvalidBlockLimit> {
        let limit = match limit {
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
        let offsets =
            Vec::with_capacity(limit / (MAX_RECORD_WIRE_LENGTH + MIN_RECORD_WIRE_LENGTH / 2));
        let this = Self {
            buffer,
            size,
            codec,
            limit,
            _record_count: 0,
            offsets,
            first_key: None,
            last_key: None,
        };
        Ok(this)
    }

    /// Encodes record into the block, returning Ok(true) if successful.
    /// If the block is full, Ok(false) is returned,
    /// if any other error is encounder Err(..) is returned
    #[instrument(skip(self), err, fields(block_size = self.size, record_count = self._record_count))]
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
        self.codec
            .encode(rec, &mut self.buffer[orig_len..orig_len + rec.wire_len()])?;

        if self.first_key.is_none() {
            self.first_key = Some(rec.key.clone());
        }
        self.last_key = Some(rec.key.clone());

        self.offsets.push(self.size as u16);
        self.size += rec.wire_len();
        self._record_count += 1;
        Ok(WriteOutput::Written)
    }

    #[instrument(skip(self), fields(first_key = ?self.first_key, last_key = ?self.last_key))]
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
