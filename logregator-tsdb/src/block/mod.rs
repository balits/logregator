mod cursor;
mod error;
mod writer;

// i dont want AI generated code here
// but i just couldnt bother to refactor
// the test cases, so FIXME
// #[cfg(test)]
// mod test;

pub use cursor::BlockCursor;
pub use error::*;
pub use writer::*;

use crc32c::crc32c;
use tracing::{instrument, trace};

use std::fmt::Debug;

use crate::{
    codec::{InvalidPayloadSize, SZ_U16, SZ_U32},
    record::{MAX_RECORD_WIRE_LENGTH, MIN_RECORD_WIRE_LENGTH},
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
        let checksum = crc32c(&data);
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
        let stored = crc32c(src);
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

                if !(MIN_RECORD_WIRE_LENGTH..=MAX_RECORD_WIRE_LENGTH).contains(&record_wire_len) {
                    return Err(BlockCodecError::InvalidPayloadSize(
                        InvalidPayloadSize::new(record_wire_len),
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
