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
//! - [x] 2. SSTable index blocks
//!      `SSTableIterFiltered::new` calls `meta.index.binary_seek(&start)` before reading.
//! - [ ] 3. Block compression?
//!      zstd-compressed 64KB blocks within SSTables. Big win for disk/IO, small CPU cost.
//! - [ ] 4. memmap reads?

mod backend;
mod engine;
pub(crate) mod io_worker;
mod iter;
mod memtable;
mod record;
mod sstable;
mod wal;

pub use backend::{Backend, BackendConfig};
#[allow(unused)]
pub use engine::Engine;
pub use memtable::MemTable;
pub use record::Record;
pub use sstable::*;
pub use wal::*;
