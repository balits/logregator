use std::{
    collections::{BTreeSet, btree_set::Range},
    ops::Bound,
};

use tracing::instrument;

use crate::record::{Key, Record};

#[derive(Debug, PartialEq, Eq)]
pub enum AppendOutput {
    Ok,
    Full,
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

    #[instrument(skip(self, r),fields(key = ?r.key, rec_size = r.size_of(), memtable_size = self.size_bytes, memtable_limit = self.limit), ret)]
    pub fn append(&mut self, r: Record) -> AppendOutput {
        if self.size_bytes + r.size_of() > self.limit {
            return AppendOutput::Full;
        }
        self.size_bytes += r.size_of();
        self.set.insert(r);
        if self.size_bytes >= self.limit {
            AppendOutput::Full
        } else {
            AppendOutput::Ok
        }
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

    pub fn freeze(&mut self) -> FrozenMemtable {
        let frozen = std::mem::replace(self, Self::new(self.limit));
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
        assert_eq!(AppendOutput::Full, res, "memtable shouldve filled up");
        assert_eq!(max_count, m.count());
        assert_eq!(max_size, m.size_bytes());

        // memtable cant hold more records
        assert_eq!(AppendOutput::Full, m.append(base.clone()));
        dbg!(max_count, m.count());
        dbg!(max_size, m.size_bytes());

        for (i, rec) in m.full_range().enumerate() {
            dbg!(&rec.key);
            assert_eq!(i as u64, rec.key.stream_id);
        }

        let f = m.freeze();
        assert_eq!(max_size, f.size_bytes());
        assert_eq!(max_count, f.count());
        // .freeze leaves behind an empty memtable
        assert_eq!(0, m.size_bytes());
        assert_eq!(0, m.count());

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
            assert_eq!(i as u64, rec.key.source_id);
        }

        dbg!(m);
        dbg!(f);
    }
}
