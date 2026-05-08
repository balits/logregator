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
//! - [ ] 1. Compaction 
//!     Currently SSTables accumulate with every flush. Overlapping key ranges across files cause linear read amplification. Compaction merges N small SSTables into one, dropping duplicates along the way. Given your append-only model (no deletes), the dedup logic in MergeIter already handles byte-identical records, so compaction is just: merge sorted streams → write new file → delete old files → update cache.
//! - [ ] 2. SSTable index blocks
//!     SSTableIter scans from byte 0 every time. With an index block footer ([key_offset_pairs...][index_offset: u64]), you'd binary-search to the nearest offset, reducing scan cost from O(N) to O(log N). For a bloom miss, you skip the file entirely (done). For a bloom hit, the index avoids re-reading all preceding records.
//! - [ ] 3. Block compression?
//!     zstd-compressed 64KB blocks within SSTables. Big win for disk/IO, small CPU cost.

mod wal;
mod memtable;
mod engine;
mod iter;
mod compactor;
mod record;
mod bloom_filter;

pub(crate) use wal::Wal;
pub(crate) use memtable::MemTable;
pub(crate) use engine::Engine;
pub(crate) use compactor::Compactor;
pub(crate) use bloom_filter::BloomFilter;
