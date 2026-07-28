use std::{
    collections::{HashSet, VecDeque},
    ops::Bound,
    path::Path,
    rc::Rc,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
    vec::IntoIter as VecIntoIter,
};

use tracing::instrument;

use crate::codec;
use crate::codec::RecordCodecExt;
use crate::{
    codec::SpecCodec,
    label::{LabeledIter, StreamRegistry},
    memtable::{AppendOutput, FrozenMemtable, Memtable},
    merge_iter::MergeIter,
    record::{self, Key, Record},
    sst::{self, SstFileWriter, SstHandle, SstReadError},
};
use crate::{label::LabelMap, wal::Wal};

#[derive(Debug)]
pub struct LsmState<R: RecordCodecExt, L> {
    active_memtable: Memtable,
    frozen_memtables: VecDeque<Arc<FrozenMemtable>>,
    sst_handles: Vec<Rc<SstHandle<R>>>,
    wal: Wal<R>,

    stream_reg: Arc<RwLock<Arc<StreamRegistry>>>,
    label_codec: L,

    next_stream_id: u64,
    next_seq_num: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error("failed to append to lsm-tree: {0}")]
    CodecError(#[from] codec::CodecError),

    #[error("failed to append to lsm-tree: label error: {0}")]
    LabelError(String),

    #[error("failed to append to lsm-tree: locking failed: {0}")]
    LockError(String),
}

#[derive(Debug, thiserror::Error)]
pub enum RangeError {
    #[error("faild to range over lsm-tree: {0}")]
    SstReadError(#[from] sst::SstReadError),

    #[error("failed to append to lsm-tree: locking failed: {0}")]
    LockError(String),
}

impl<R, L> LsmState<R, L>
where
    R: RecordCodecExt,
    L: SpecCodec<LabelMap>,
{
    pub fn new_uninit(memtable_limit: usize, wal: Wal<R>, label_codec: L) -> Self {
        let active_memtable = Memtable::new(memtable_limit);
        let frozen_memtables = VecDeque::new();
        let sst_handles = Vec::new();
        let stream_reg = Arc::new(RwLock::new(Arc::new(StreamRegistry::default())));

        Self {
            active_memtable,
            frozen_memtables,
            sst_handles,
            wal,
            stream_reg,
            label_codec,
            next_stream_id: 0,
            next_seq_num: 0,
        }
    }

    pub fn append_batch<'a>(
        &mut self,
        payloads: impl Iterator<Item = &'a [u8]>,
        source_id: u64,
    ) -> Result<bool, (usize, AppendError)> {
        let mut needs_flush = false;
        for (i, p) in payloads.enumerate() {
            needs_flush = self.append_single(p, source_id).map_err(|e| (i, e))?;
        }
        Ok(needs_flush)
    }

    // returns true if flush is needed
    #[instrument(skip(self, payload), fields(raw_payload_len = payload.len()), err)]
    fn append_single(&mut self, payload: &[u8], source_id: u64) -> Result<bool, AppendError> {
        record::check_payload_size(payload.len())?;

        let (labelmap, label_bytes_read) = match self.label_codec.decode(payload) {
            Ok(Some((m, r))) => (Arc::new(m), r),
            Ok(None) => {
                return Err(
                    codec::CodecError::Other("label map codec returned Ok(None)".into()).into(),
                );
            }
            Err(e) => return Err(e.into()),
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("failed to make UNIX timestemp")
            .as_nanos() as u64;

        // Arc RwLock Arc StreamReg
        // ^^^ :      :
        // |   ^^^^^^ :
        // |   |      ^^^
        // |   |      |
        // |   |      cheap clones for snapshots
        // |   |
        // |   modifying/swapping
        // |
        // share between threads

        let stream_id = {
            let mut write_guard = self
                .stream_reg
                .write()
                .map_err(|e| AppendError::LockError(e.to_string()))?;

            match write_guard.get_stream_id_by_labelmap(&labelmap) {
                Some(stream_id) => {
                    let existing_map= write_guard.get_labelmap_by_stream(stream_id).ok_or_else(|| {
                        AppendError::LabelError("inconsisent label state: stream registry contains stream_id but stream registry doesnt".into())})?;

                    if existing_map.as_ref() != labelmap.as_ref() {
                        return Err(AppendError::LabelError(
                            "hash collision occured (likelyhood was 2^16/2^65 ~ 1,7×10^-15) and im lazy to resolve it".into(),
                        ));
                    }

                    *stream_id
                }
                None => {
                    self.next_stream_id += 1;
                    let stream_id = self.next_stream_id;

                    let mut reg = (**write_guard).clone();
                    reg.insert(stream_id, &labelmap);
                    *write_guard = Arc::new(reg);

                    // write entry to manifest
                    stream_id
                }
            }
        };

        self.next_seq_num += 1;
        let key = record::Key {
            source_id,
            timestamp,
            sequence_num: self.next_seq_num,
            stream_id,
        };

        let rec = Record {
            key,
            payload: payload[label_bytes_read..].to_vec().into_boxed_slice(),
        };

        // trace!(
        //     record_size_of = rec.size_of(),
        //     record_wire_len = rec.wire_len(),
        //     payload = String::from_utf8_lossy_owned(rec.payload.to_vec())
        // );

        self.wal.append(&rec)?;
        match self.active_memtable.append(rec) {
            AppendOutput::Ok => Ok(false),
            AppendOutput::Full(None) => {
                let frozen = self.active_memtable.freeze();
                self.frozen_memtables.push_back(Arc::new(frozen));
                Ok(true)
            }
            AppendOutput::Full(Some(rec)) => {
                let frozen = self.active_memtable.freeze();
                self.frozen_memtables.push_back(Arc::new(frozen));
                let res = self.active_memtable.append(rec);
                debug_assert_eq!(
                    AppendOutput::Ok,
                    res,
                    "empty memtable should insert the first record, even if it overflows the memory limit"
                );
                Ok(true)
            }
        }
    }

    pub fn range<'a, I>(
        &mut self,
        labels: I,
        start_t: Option<u64>,
        end_t: Option<u64>,
    ) -> Result<LabeledIter<'_, R, VecIntoIter<MergeIter<'_, R>>>, RangeError>
    where
        I: Iterator<Item = (&'a Arc<str>, &'a Arc<str>)>,
    {
        let start_t = start_t.unwrap_or(0);
        let end_t = end_t.unwrap_or(u64::MAX);

        let snapshot = {
            let read_guard = self
                .stream_reg
                .read()
                .map_err(|e| RangeError::LockError(e.to_string()))?;
            (*read_guard).clone()
        };

        let stream_ids = Self::label_intersection(&snapshot, labels);

        let merge_iters: Result<Vec<MergeIter<'_, R>>, SstReadError> = stream_ids
            .iter()
            .map(|id| {
                let start = Bound::Included(&Key {
                    stream_id: *id,
                    timestamp: start_t,
                    sequence_num: 0,
                    source_id: 0,
                });

                let end = Bound::Included(&Key {
                    stream_id: *id,
                    timestamp: end_t,
                    sequence_num: u64::MAX,
                    source_id: u64::MAX,
                });

                MergeIter::new(
                    &self.active_memtable,
                    &self.frozen_memtables,
                    &self.sst_handles,
                    start,
                    end,
                )
            })
            .collect();

        let merge_iters = merge_iters.map_err(RangeError::SstReadError)?;

        Ok(LabeledIter::new(merge_iters, snapshot))
    }

    pub fn label_intersection<'a, I>(stream_reg: &StreamRegistry, labels: I) -> HashSet<u64>
    where
        I: Iterator<Item = (&'a Arc<str>, &'a Arc<str>)>,
    {
        let mut sets: Vec<&HashSet<u64>> = labels
            .flat_map(|(k, v)| stream_reg.get_streams_by_kv(k.clone(), v.clone()))
            .collect();

        if sets.is_empty() {
            return HashSet::new();
        }

        sets.sort_unstable_by_key(|s| s.len());

        let mut acc = sets[0].clone();
        for s in &sets[1..] {
            acc.retain(|id| s.contains(id));
            if s.is_empty() {
                break;
            }
        }

        acc
    }

    pub fn active_memtable_size_bytes(&self) -> usize {
        self.active_memtable.size_bytes()
    }

    pub fn all_memtable_size_bytes(&self) -> usize {
        let mut sum = self.active_memtable.size_bytes();
        for f in &self.frozen_memtables {
            sum += f.size_bytes()
        }
        sum
    }

    pub fn pop_frozen(&mut self) -> Option<Arc<FrozenMemtable>> {
        self.frozen_memtables.pop_front()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemtableFlushError {
    #[error("failed to flush memtable to disk: {0}")]
    SstWriterCreateError(#[from] sst::SstWriterCreateError),

    #[error("failed to flush memtable to disk: {0}")]
    SstWriteError(#[from] sst::SstWriteError),

    #[error("failed to flush memtable to disk: {0}")]
    SstFinalizeError(#[from] sst::SstFinalizeError),
}

#[allow(unused)]
pub fn flush<C: RecordCodecExt>(
    memtable: Arc<FrozenMemtable>,
    sst_id: u32,
    block_limit: Option<usize>,
    dir: &Path,
    codec: C,
) -> Result<SstHandle<C>, MemtableFlushError> {
    let mut sw = SstFileWriter::new(sst_id, codec, block_limit, Some(dir))?;
    for rec in memtable.full_range() {
        sw.write(rec)?;
    }
    Ok(sw.finalize_file()?)
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use pretty_assertions::assert_eq;

    use crate::{
        codec::{SpecCodec, WireLen},
        label::{LabelMap, LabelMapCodec},
        lsm::LsmState,
        record::{KEY_SIZE, RecordCodec},
        wal::Wal,
    };

    fn tracing() {
        let try_init = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            // .with_target(true)
            // .with_span_events(FmtSpan::NEW)
            .with_test_writer()
            .try_init();
        let _ = try_init;
    }

    fn data(codec: LabelMapCodec) -> (Box<[u8]>, Box<[u8]>, LabelMap) {
        let mut map = BTreeMap::new();
        map.insert("foo".into(), "bar".into());
        map.insert("bar".into(), "baz".into());
        map.insert("baz".into(), "foo".into());
        let lm = LabelMap {
            inner: map,
            fingerprint: 0,
        };

        let mut payload_with_labelmap = vec![0; lm.wire_len()];
        let n = codec.encode(&lm, &mut payload_with_labelmap).expect("data");
        assert_eq!(
            n,
            lm.wire_len(),
            "expected to write full labelmaps wire length into destination buffer"
        );

        let payload_without_labelmap = b"asd".to_vec();
        payload_with_labelmap.extend_from_slice(&payload_without_labelmap);

        (
            payload_with_labelmap.into_boxed_slice(),
            payload_without_labelmap.into_boxed_slice(),
            lm,
        )
    }

    #[test]
    fn append() {
        tracing();
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let label_codec = LabelMapCodec;
        let (payload_with_labelmap, payload_no_labelmap, _labelmap) = data(label_codec);

        let max_count = 4;
        let record_size_of = KEY_SIZE + size_of::<Box<[u8]>>() + payload_no_labelmap.len();
        // let record_wire_len = KEY_SIZE + SZ_U32 + payload_no_labelmap.len();
        let memtable_limit = max_count * record_size_of;
        let record_codce = RecordCodec;
        let wal = Wal::new(tmp.path(), record_codce).expect("wal::new");
        let mut lsm = LsmState::new_uninit(memtable_limit, wal, label_codec);

        let mut computed_sz = 0;
        for i in 0..max_count {
            lsm.append_single(&payload_with_labelmap, i as u64)
                .expect("lsm::append");
            computed_sz += record_size_of;
        }

        let records_per_memtable = memtable_limit.div_ceil(record_size_of);
        dbg!(
            record_size_of,
            memtable_limit,
            records_per_memtable,
            max_count,
            computed_sz,
            lsm.active_memtable_size_bytes(),
            lsm.all_memtable_size_bytes(),
            // &lsm.active_memtable,
            // &lsm.frozen_memtables,
        );
        assert_eq!(lsm.active_memtable_size_bytes(), 0);
        assert_eq!(lsm.frozen_memtables[0].size_bytes(), computed_sz);

        // dbg!(&lsm);

        for i in 0..max_count * 2 {
            lsm.append_single(&payload_with_labelmap, i as u64)
                .expect("lsm::append");
            computed_sz += record_size_of;
        }
        // dbg!(&lsm);
        assert_eq!(lsm.all_memtable_size_bytes(), computed_sz);
    }

    /// LsmState only freezes the memtables, flushing will
    /// be the role of a background Io worker,
    /// so this range only goes over memtables, not sstables
    #[test]
    fn range() {
        tracing();
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let label_codec = LabelMapCodec;
        let (payload_with_labelmap, payload_no_labelmap, labelmap) = data(label_codec);

        let max_count = 4;
        let record_size_of = KEY_SIZE + size_of::<Box<[u8]>>() + payload_no_labelmap.len();
        // let record_wire_len = KEY_SIZE + SZ_U32 + payload_no_labelmap.len();
        let memtable_limit = max_count * record_size_of;
        let record_codce = RecordCodec;
        let wal = Wal::new(tmp.path(), record_codce).expect("wal::new");
        let mut lsm = LsmState::new_uninit(memtable_limit, wal, label_codec);

        // let mut computed_sz = 0;
        let rec_count = max_count * 4;
        for i in 0..rec_count {
            let mut changed = payload_with_labelmap.to_vec();
            changed.extend(format!("{i}").as_bytes());
            let payload = changed.into_boxed_slice();
            lsm.append_single(&payload, i as u64).expect("lsm::append");
            // computed_sz += record_size_of;
        }

        // assert_eq!(lsm.all_memtable_size_bytes(), computed_sz);

        let labels = labelmap.iter();
        let iter = lsm.range(labels, None, None).expect("lsm::range");

        let mut n = 0;
        for (_, rec) in iter {
            println!("{rec}");
            n += 1;
        }
        assert_eq!(
            n, rec_count,
            "expected iterator to have all records inserted to lsm"
        )
    }
}
