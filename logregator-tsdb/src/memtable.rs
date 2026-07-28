use std::{
    collections::{BTreeSet, btree_set::Range},
    ops::Bound,
};

use tracing::{instrument, trace};

use crate::record::{Key, Record};

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

    /// Returns the leftover record
    /// that couldnt be inserted into the memtable.
    pub fn leftover(self) -> Option<Record> {
        match self {
            AppendOutput::Full(Some(r)) => Some(r),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Memtable {
    set: BTreeSet<Record>,
    limit: usize,
    size_bytes: usize,
}

impl Memtable {
    pub fn new(limit: usize) -> Self {
        let set = BTreeSet::new();
        Self {
            set,
            limit,
            size_bytes: 0,
        }
    }

    #[instrument(skip(self, rec),fields(rec_size = rec.size_of(), memtable_size = self.size_bytes, memtable_limit = self.limit), ret)]
    pub fn append(&mut self, rec: Record) -> AppendOutput {
        // ensure first records is always inserted,
        // even if it would overflow the memtable (which is unlikely)
        if self.size_bytes + rec.size_of() > self.limit && self.size_bytes != 0 {
            return AppendOutput::Full(Some(rec));
        }
        self.size_bytes += rec.size_of();
        self.set.insert(rec);
        if self.size_bytes >= self.limit {
            AppendOutput::Full(None)
        } else {
            AppendOutput::Ok
        }
    }

    pub fn last(&self) -> Option<&Record> {
        self.set.last()
    }

    pub fn range(&self, start: Bound<&Key>, end: Bound<&Key>) -> Range<'_, Record> {
        self.set
            .range::<Key, (Bound<&Key>, Bound<&Key>)>((start, end))
    }

    pub fn full_range(&self) -> Range<'_, Record> {
        let start = Key::default();
        let end = Key::new(u64::MAX, u64::MAX, u64::MAX, u64::MAX);
        self.set.range::<Key, (Bound<&Key>, Bound<&Key>)>((
            Bound::Included(&start),
            Bound::Included(&end),
        ))
    }

    #[instrument(skip(self))]
    pub fn freeze(&mut self) -> FrozenMemtable {
        let frozen = std::mem::replace(self, Self::new(self.limit));
        trace!(
            "memtable frozen, size = {} limit = {}",
            self.size_bytes, self.limit
        );
        FrozenMemtable(frozen)
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.set.len()
    }

    #[inline]
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    #[inline]
    pub fn limit(&self) -> usize {
        self.limit
    }
}

#[derive(Debug)]
pub struct FrozenMemtable(Memtable);

impl FrozenMemtable {
    pub fn range(&self, start: Bound<&Key>, end: Bound<&Key>) -> Range<'_, Record> {
        self.0.range(start, end)
    }
    pub fn full_range(&self) -> Range<'_, Record> {
        self.0.full_range()
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.0.count()
    }

    #[inline]
    pub fn size_bytes(&self) -> usize {
        self.0.size_bytes()
    }

    #[inline]
    pub fn limit(&self) -> usize {
        self.0.limit()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use crate::{
        memtable::{AppendOutput, Memtable},
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
            payload: vec![0x01, 0x02, 0x03, 0x04].into_boxed_slice(),
        };
        let max_count = 4;
        let max_size = max_count * base.size_of();

        let mut m = Memtable::new(max_size);
        for i in 0..(max_count - 1) {
            let k = Key::dummy(i as u64);
            let mut rec = Record {
                key: k,
                payload: base.payload.clone(),
            };
            rec.key.source_id = i as u64;
            let rec_sz = rec.size_of();

            assert_eq!(AppendOutput::Ok, m.append(rec));
            assert_eq!(m.size_bytes, rec_sz * (i + 1))
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

        let f = m.freeze();
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
