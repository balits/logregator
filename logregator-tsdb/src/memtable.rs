use std::{
    collections::{BTreeSet, btree_set::Range},
    ops::{Bound, Deref, DerefMut},
    sync::{Arc, atomic::AtomicBool},
};

use tracing::instrument;

use crate::record::{Key, Record};

/// 16 Mb, is this sane?
pub const DEFAULT_MEMTABLE_SIZE: usize = 16 * 10 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum AppendOutput {
    /// Signals that the record was inserted
    /// successfuly.
    Ok,

    /// Signals that the memtable is full,
    /// and if the record wasnt inserted before the
    /// memtable filled up, returns the original record
    Full(Option<Record>),
}

impl AppendOutput {
    /// returns true if the record was inserted into
    /// the memtable, even if it filled up afterwards
    pub fn was_appended(&self) -> bool {
        match &self {
            AppendOutput::Ok => true,
            AppendOutput::Full(None) => true,
            AppendOutput::Full(Some(_)) => false,
        }
    }
}

/// A wrapper around a memtable giving both read
/// and wrtie access to it by the [Deref] and [DerefMut] traits.
#[derive(Debug, Clone)]
pub struct MutMemtable(MemtableInner);

impl Deref for MutMemtable {
    type Target = MemtableInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MutMemtable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl MutMemtable {
    #[deprecated(
        note = "use new() with an optional size limit, which defaults to `DEFAULT_MEMTABLE_SIZE`"
    )]
    pub fn with_size_limit(id: u64, limit: usize) -> Self {
        Self(MemtableInner::with_size_limit(id, limit))
    }

    pub fn new(id: u64, limit: Option<usize>) -> Self {
        Self(MemtableInner::with_size_limit(
            id,
            limit.unwrap_or(DEFAULT_MEMTABLE_SIZE),
        ))
    }
}

/// A wrapper around a memtable, giving only read-only
/// access to the underlying memtable.
#[derive(Debug, Clone)]
pub struct FrozenMemtable(Arc<MemtableInner>);

impl Deref for FrozenMemtable {
    type Target = MemtableInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A wrapper around a memtable, giving only read-only
/// access to the underlying memtable.
#[derive(Debug)]
pub struct FlushableMemtable {
    inner: Arc<MemtableInner>,
    flushed: AtomicBool,
}

impl Deref for FlushableMemtable {
    type Target = MemtableInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<FrozenMemtable> for FlushableMemtable {
    fn from(f: FrozenMemtable) -> Self {
        Self {
            inner: f.0,
            flushed: AtomicBool::new(false),
        }
    }
}

impl FlushableMemtable {
    pub fn mark_flushed(&mut self) {
        self.flushed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .expect("TODO: mark_flushed panicked on CAS");
    }

    pub fn is_flushed(&mut self) -> bool {
        self.flushed.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// The actual implementation of the Memtable,
/// wrapper structs merely derefernece to this one.
#[derive(Debug, Clone)]
pub struct MemtableInner {
    /// Unique ID of assigned to each memtable.
    id: u64,

    /// Set of records ordered by [record.key](crate::record::Key).
    set: BTreeSet<Record>,

    /// Soft limit on the inmemory size of the memtable in bytes.
    ///
    /// Given an empty memtable, the first record always gets inserted,
    /// no matter if it makes the memtable exceed its limit.
    ///
    /// However, a memtable with already existing records (meaning a non-zero
    /// `limit`) refuses to insert records that would make `size_bytes` exceed
    /// `limit`
    limit: usize,

    /// Current size of the memtable in bytes
    size_bytes: usize,

    /// Optional first key found in this memtable.
    first_key: Option<Key>,

    /// Optional last key found in this memtable.
    last_key: Option<Key>,
}

impl MemtableInner {
    pub fn with_size_limit(id: u64, limit: usize) -> Self {
        let set = BTreeSet::new();
        Self {
            id,
            set,
            limit,
            size_bytes: 0,
            first_key: None,
            last_key: None,
        }
    }

    #[instrument(skip(self, rec),fields(rec_size = rec.size_of(), memtable_size = self.size_bytes, memtable_limit = self.limit), ret)]
    pub fn append(&mut self, rec: Record) -> AppendOutput {
        // ensure first records is always inserted,
        // even if it would overflow the memtable (which is unlikely)
        if self.size_bytes + rec.size_of() > self.limit && self.size_bytes != 0
        {
            return AppendOutput::Full(Some(rec));
        }

        if self.first_key.is_none() {
            self.first_key = Some(rec.key.clone())
        }
        self.last_key = Some(rec.key.clone());
        self.size_bytes += rec.size_of();
        let _insert_success = self.set.insert(rec);
        if self.size_bytes >= self.limit {
            AppendOutput::Full(None)
        } else {
            AppendOutput::Ok
        }
    }

    pub fn last(&self) -> Option<&Record> {
        self.set.last()
    }

    pub fn range<K>(&self, start: Bound<K>, end: Bound<K>) -> Range<'_, Record>
    where
        K: AsRef<Key>,
    {
        let start = start.as_ref().map(|k| k.as_ref());
        let end = end.as_ref().map(|k| k.as_ref());
        self.set
            .range::<Key, (Bound<&Key>, Bound<&Key>)>((start, end))
    }

    pub fn range_cloned(
        &self,
        start: Bound<&Key>,
        end: Bound<&Key>,
    ) -> BTreeSet<Record> {
        self.set
            .range::<Key, (Bound<&Key>, Bound<&Key>)>((start, end))
            .cloned()
            .collect()
    }

    pub fn full_range(&self) -> Range<'_, Record> {
        let start = Key::default();
        let end = Key::new(u64::MAX, u64::MAX, u64::MAX, u64::MAX);
        self.set.range::<Key, (Bound<&Key>, Bound<&Key>)>((
            Bound::Included(&start),
            Bound::Included(&end),
        ))
    }

    pub fn freeze(&mut self, new_id: u64) -> FrozenMemtable {
        let inner =
            std::mem::replace(self, Self::with_size_limit(new_id, self.limit));
        FrozenMemtable(Arc::new(inner))
    }

    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns how many records this memtable holds.
    #[inline]
    pub fn count(&self) -> usize {
        self.set.len()
    }

    /// Returns how many raw bytes this memtable holds.
    #[inline]
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    #[inline]
    pub fn limit(&self) -> usize {
        self.limit
    }

    #[inline]
    pub fn key_range(&self) -> (Option<&Key>, Option<&Key>) {
        (self.first_key.as_ref(), self.last_key.as_ref())
    }

    #[inline]
    pub fn key_bounds_full(&self) -> crate::Result<(Bound<&Key>, Bound<&Key>)> {
        let b = match self.key_range() {
            (Some(k1), Some(k2)) => (Bound::Included(k1), Bound::Included(k2)),
            (None, _) | (_, None) => {
                return Err(crate::Error::from(
                    "`first_key` or `last_key` not set after calling `active_memtable.append(record)`",
                ));
            }
        };

        Ok(b)
    }
}

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use crate::{
        memtable::{AppendOutput, MutMemtable},
        record::{Key, Record},
    };
    use pretty_assertions::assert_eq;

    fn tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            // .with_target(true)
            // .with_span_events(FmtSpan::NEW)
            .with_test_writer()
            .try_init();
    }

    #[test]
    fn lifecylce() {
        tracing();
        let base = Record {
            key: Key::new(1, 2, 3, 4),
            payload: vec![0x01, 0x02, 0x03, 0x04].into(),
        };
        let max_count = 4;
        let max_size = max_count * base.size_of();

        let mut m = MutMemtable::with_size_limit(1, max_size);
        for i in 0..(max_count - 1) {
            let k = Key::dummy(i as u64);
            let mut rec = Record {
                key: k,
                payload: base.payload.clone(),
            };
            rec.key.source_id = i as u64;
            let rec_sz = rec.size_of();

            assert_eq!(AppendOutput::Ok, m.append(rec));
            assert_eq!(m.size_bytes(), rec_sz * (i + 1))
        }

        // memtable just filled up
        let res = m.append(Record {
            key: Key::dummy(max_count as u64 - 1),
            payload: base.payload.clone(),
        });

        dbg!(&m);
        assert!(res.was_appended(), "record shouldve fit into memtable");
        pretty_assertions::assert_eq!(AppendOutput::Full(None), res);
        pretty_assertions::assert_eq!(max_count, m.count());
        pretty_assertions::assert_eq!(max_size, m.size_bytes());

        // memtable cant hold more records
        let filled = m.append(base.clone());
        dbg!(&filled);
        assert!(!filled.was_appended());

        dbg!(max_count, m.count());
        dbg!(max_size, m.size_bytes());

        for (i, rec) in m.full_range().enumerate() {
            dbg!(&rec.key);
            pretty_assertions::assert_eq!(i as u64, rec.key.stream_id);
        }

        let f = m.freeze(2);
        pretty_assertions::assert_eq!(max_size, f.size_bytes());
        pretty_assertions::assert_eq!(max_count, f.count());
        // .freeze leaves behind an empty memtable
        pretty_assertions::assert_eq!(0, m.size_bytes());
        pretty_assertions::assert_eq!(0, m.count());

        for (i, rec) in f
            .range(
                Bound::Included(&Key::default()),
                Bound::Excluded(&Key {
                    source_id: u64::MAX,
                    ..Default::default()
                }),
            )
            .enumerate()
        {
            pretty_assertions::assert_eq!(i as u64, rec.key.source_id);
        }

        dbg!(m);
        dbg!(f);
    }
}
