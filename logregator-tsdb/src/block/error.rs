use crate::block::MAX_BLOCK_SIZE;

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
    UnexpectedSize(crate::codec::UnexpectedSize),
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
    InvalidPayloadSize(crate::codec::InvalidPayloadSize),

    #[error("block_error: corrupted metadata {0}")]
    CorruptedMetadata(String),

    #[error("block_error: {0}")]
    DecodeError(DecodeBlockError),

    #[error("block_error: checksum mismatch, {stored:#x} (stored) != {computed:#x} (computed)")]
    Checksum { stored: u32, computed: u32 },
}

pub(super) fn too_many_records(record_count: usize) -> BlockCodecError {
    BlockCodecError::InvalidBlockSize(InvalidBlockSize::TooManyRecords(record_count))
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

pub(super) fn num_of_records_missing() -> BlockCodecError {
    BlockCodecError::DecodeError(DecodeBlockError::NumOfRecordsMissing)
}

pub(super) fn unexpected_size(got: usize, want: usize) -> BlockCodecError {
    BlockCodecError::DecodeError(DecodeBlockError::UnexpectedSize(
        crate::codec::UnexpectedSize { got, want },
    ))
}

pub(super) fn checksum_mismatch(stored: u32, computed: u32) -> BlockCodecError {
    BlockCodecError::Checksum { stored, computed }
}
