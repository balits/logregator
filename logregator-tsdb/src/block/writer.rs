use std::io;

use tracing::{instrument, trace};

use crate::{
    block::{Block, DEFAULT_BLOCK_SIZE, MAX_BLOCK_SIZE},
    codec::{Codec, FramedWriter},
    record::{MAX_RECORD_WIRE_LENGTH, MIN_RECORD_WIRE_LENGTH, Record},
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
