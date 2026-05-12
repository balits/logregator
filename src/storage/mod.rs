//! # Storage
//!
//! LSM-tree based storage engine for the aggregator.
//!
//! ## Records
//! Records contain the source_id, timestamp, key and value in one contigous byte buffer.
//! The MemTable holds a BTreeSet of records, where the records are ordered and compared by source_id,
//! timestamp, seq_num and key, in this order.
//! The SSTable files contain such records, and thanks to source_id and timestamp being encoded as
//! big-endian bytes, they remain perfectly sorted after flushing from the BTreeSet (which itself is
//! already sorted).
//! 
//! ## TODOs
//! - [x] 1. Compaction 
//! - [ ] 2. SSTable index blocks
//!     SSTableIter scans from byte 0 every time. With an index block footer ([key_offset_pairs...][index_offset: u64]), you'd binary-search to the nearest offset, reducing scan cost from O(N) to O(log N). For a bloom miss, you skip the file entirely (done). For a bloom hit, the index avoids re-reading all preceding records.
//! - [ ] 3. Block compression?
//!     zstd-compressed 64KB blocks within SSTables. Big win for disk/IO, small CPU cost.
//! - [ ] 4. memmap reads?

mod wal;
mod memtable;
mod engine;
mod iter;
mod record;
mod sstable;
pub mod compaction;

pub(crate) use wal::Wal;
pub(crate) use memtable::MemTable;
pub(crate) use sstable::*;
pub use record::Record;
#[allow(unused)]
pub use engine::Engine;