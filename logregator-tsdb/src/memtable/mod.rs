mod map;
#[cfg(test)]
mod test;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as MemOrdering;
use crate::buffer::Buffer;
use crate::memtable::map::MapError;
use crate::record::{RecordValue, RecordKey};
use self::map::{Map, MapFrozen};

pub struct Memtable {
    inner: Map,

    /// soft cap on the size of the memtable in bytes
    limit: AtomicUsize,

    /// approximate size of the memtable in bytes
    sz: AtomicUsize,
}


impl Memtable {
    fn with_limit(limit: usize) -> Self {
        Self {
            inner: Map::default(),
            limit: AtomicUsize::new(limit),
            sz: AtomicUsize::default(),
        }
    }

    fn limit(&self) -> usize {
        self.limit.load(MemOrdering::Relaxed)
    }

    fn size_hint(&self) -> usize {
        self.sz.load(MemOrdering::Relaxed)
    }

    // TODO: move atomic sz + limit to MapImpl behind RwLock?
    // but that would contend the lock every time we would call accessor methods...
    fn append(&self, k: RecordKey, v: RecordValue) -> Result<bool, MapError> {
        let rec_sz = std::mem::size_of::<RecordKey>()
            + std::mem::size_of_val(&v.meta)
            + v.payload.len();
        self.inner.append(k, v.payload)?;
        let sz = self.sz.fetch_add(rec_sz, MemOrdering::Relaxed);
        Ok(sz > self.limit.load(MemOrdering::Relaxed))
    }

    pub fn range_cloned(&self, source_id: u32, start_ts: u64, end_ts: u64) -> Result<Vec<(RecordKey, Buffer)>, MapError> {
        self.inner.range_cloned(source_id, start_ts, end_ts)
    }

    pub fn flush(&self) {
        todo!()
    }

    pub fn freeze(&self) -> Result<MemtableFrozen, MapError> {
        let inner = self.inner.freeze()?;
        let limit = self.limit.load(MemOrdering::Relaxed);
        let sz = self.sz.load(MemOrdering::Relaxed);
        Ok(MemtableFrozen { inner, limit, sz })
    }
}

pub struct MemtableFrozen {
    inner: MapFrozen,

    /// soft cap on the size of the memtable in bytes
    limit: usize,

    /// approximate size of the memtable in bytes
    sz: usize,
}

impl MemtableFrozen {
    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn count(&self) -> usize {
        self.inner.count()
    }

    pub fn size_hint(&self) -> usize {
        self.sz
    }

    pub fn range_cloned(&self, source_id: u32, start_ts: u64, end_ts: u64) -> Vec<(RecordKey, Buffer)> {
        self.inner.range_cloned(source_id, start_ts, end_ts)
    }

}