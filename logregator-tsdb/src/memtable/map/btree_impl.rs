use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;

use crate::buffer::Buffer;
use crate::memtable::RecordKey;
use crate::memtable::map::MapError;

/// A Map implemented by a `std::collections::BTreeMap`, wrapped around an `Arc<RwLock<_>>`,
/// which is safe to both read and write concurrently.
#[derive(Debug, Default)]
pub(crate) struct MapOwned(Arc<RwLock<BTreeMap<RecordKey, Buffer>>>);

impl MapOwned {
    /// Appends the key value pair to the underlying storage.
    /// Returns `MapError::RwlockPoisoned` if we cant acquire the lock
    pub(crate) fn append(&self, k: RecordKey, v: Buffer) -> Result<(), MapError> {
        let mut g = self.0.write().map_err(|_| MapError::RwlockPoisoned)?;
        g.insert(k, v);
        Ok(())
    }

    // TODO: is this needed?
    pub(crate) fn clear(&self) -> Result<(), MapError> {
        let mut g = self.0.write().map_err(|_| MapError::RwlockPoisoned)?;
        g.clear();
        Ok(())
    }

    pub(crate) fn freeze(&self) -> Result<MapFrozen, MapError> {
        let mut g = self.0.write().map_err(|_| MapError::RwlockPoisoned)?;
        let inner = std::mem::take(&mut *g);
        Ok(MapFrozen(Arc::new(inner)))
    }

    pub(crate) fn range_cloned(
        &self,
        source_id: u32,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<(RecordKey, Buffer)>, MapError> {
        let start = RecordKey {
            ts: start_ts,
            source_id,
            seq: 0,
        };
        let end = RecordKey {
            ts: end_ts,
            source_id,
            seq: 0,
        };

        let g = self.0.write().map_err(|_| MapError::RwlockPoisoned)?;
        let v = g
            .range(start..end)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(v)
    }
}

/// A read-only Map implemeneted by `std::collections::BTreeMap`, which is safe to read concurrently.
pub(crate) struct MapFrozen(Arc<BTreeMap<RecordKey, Buffer>>);

impl MapFrozen {
    pub(crate) fn count(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn range_cloned(
        &self,
        source_id: u32,
        start_ts: u64,
        end_ts: u64,
    ) -> Vec<(RecordKey, Buffer)> {
        let start = RecordKey {
            ts: start_ts,
            source_id,
            seq: 0,
        };
        let end = RecordKey {
            ts: end_ts,
            source_id,
            seq: 0,
        };

        self.0
            .range(start..end)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}
