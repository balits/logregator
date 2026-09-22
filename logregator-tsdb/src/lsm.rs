use std::{
    cell::RefCell,
    collections::VecDeque,
    ops::Bound,
    path::PathBuf,
    rc::Rc,
    sync::mpsc,
    time::{SystemTime, UNIX_EPOCH},
    vec::IntoIter as VecIntoIter,
};

use tracing::{error, instrument};

use crate::{
    codec::{self, RecordCodecExt, SpecCodec},
    counter::Counter,
    io,
    label::{LabelMap, LabeledIter, StreamRegistry},
    manifest::ManifestEntry,
    memtable::{AppendOutput, FrozenMemtable, MutMemtable},
    merge_iter::MergeIter,
    record::{self, Key, Record},
    sst::{self, SstHandle},
};

#[derive(Debug)]
pub struct LsmConfig<R, L> {
    basepath: PathBuf,
    block_size_limit: Option<usize>,
    _retention_days: Option<u32>,
    record_codec: R,
    label_codec: L,
}

#[derive(Debug)]
pub struct LsmTree<R: RecordCodecExt, L: SpecCodec<LabelMap>> {
    config: LsmConfig<R, L>,
    next_memtable_id: Counter,
    next_stream_id: Counter,
    next_seq_num: Counter,

    memtable: MutMemtable,
    frozen_memtables: VecDeque<FrozenMemtable>,
    sst_handles: Vec<SstHandle<R>>,
    stream_buffer: Vec<ManifestEntry>,
    stream_reg: Rc<RefCell<StreamRegistry>>,

    io_tx: mpsc::Sender<io::IoEvent<R>>,
}

impl<R, L> LsmTree<R, L>
where
    R: RecordCodecExt,
    L: SpecCodec<LabelMap>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new_uninit(
        basepath: PathBuf,
        record_codec: R,
        label_codec: L,
        block_size_limit: Option<usize>,
        memtable: MutMemtable,
        sst_handles: Vec<SstHandle<R>>,
        stream_reg: StreamRegistry,
        next_stream_id: u64,
        next_seq_num: u64,
        retention_days: Option<u32>,
        io_tx: mpsc::Sender<io::IoEvent<R>>,
    ) -> Self {
        let next_memtable_id = memtable.id() + 1;
        let frozen_memtables = VecDeque::new();
        let stream_buffer = Vec::new();
        let config = LsmConfig {
            basepath,
            record_codec,
            block_size_limit,
            label_codec,
            _retention_days: retention_days,
        };
        let stream_reg = Rc::new(RefCell::new(stream_reg));

        Self {
            memtable,
            frozen_memtables,
            sst_handles,
            stream_reg,
            next_memtable_id: next_memtable_id.into(),
            next_stream_id: next_stream_id.into(),
            next_seq_num: next_seq_num.into(),
            config,
            io_tx,
            stream_buffer,
        }
    }
}

// #[derive(Debug, thiserror::Error)]
// pub enum AppendError {
//     #[error("failed to append to lsm-tree: {0}")]
//     CodecError(#[from] codec::CodecError),

//     #[error("failed to append to lsm-tree: label error: {0}")]
//     LabelError(String),

//     #[error("failed to append to lsm-tree: locking failed: {0}")]
//     LockError(String),

//     #[error(
//         "failed to append to lsm-tree: failed to send IO job to the IO worker: {0}"
//     )]
//     IoError(#[from] crate::io::IoError),
// }

#[derive(Debug, thiserror::Error)]
pub enum RangeError {
    #[error("faild to range over lsm-tree: {0}")]
    SstReadError(#[from] sst::SstError),

    #[error("failed to append to lsm-tree: locking failed: {0}")]
    LockError(String),
}

impl<R, L> LsmTree<R, L>
where
    R: RecordCodecExt,
    L: SpecCodec<LabelMap> + 'static,
{
    #[cfg(test)]
    pub fn append_single(
        &mut self,
        payload: &[u8],
        source_id: u64,
    ) -> crate::Result<()> {
        let iter = [payload].into_iter();
        self.append_batch(iter, source_id)
    }

    pub fn append_batch<'a>(
        &mut self,
        payloads: impl Iterator<Item = &'a [u8]>,
        source_id: u64,
    ) -> crate::Result<()> {
        for payload in payloads {
            record::check_payload_size(payload.len())?;

            let (labelmap, label_bytes_read) =
                match self.config.label_codec.decode(payload) {
                    Ok(Some((m, r))) => (Rc::new(m), r),
                    Ok(None) => {
                        return Err(codec::CodecError::Other(
                            "label map codec returned Ok(None)".into(),
                        )
                        .into());
                    }
                    Err(e) => return Err(e.into()),
                };

            let stream_id = self
                .stream_reg
                .borrow()
                .get_stream_id_by_labelmap(&labelmap)
                .copied()
                .unwrap_or_else(|| self.next_stream_id.inc_and_get());

            // drops the need for the borrow() to live while the later None
            // branch calls borrow_mut()
            let prev_stream_id = self
                .stream_reg
                .borrow()
                .get_stream_id_by_labelmap(&labelmap)
                .copied();

            match prev_stream_id {
                Some(_) => {
                    let reg = self.stream_reg.borrow();
                    let existing_map = reg.get_labelmap_by_stream(&stream_id).ok_or_else(|| {
                        crate::Error::from("label state error: StreamRegistry.stream_by_labelmap contains stream_id but StreamRegistry.labelmap_by_stream doesnt")
                    })?;

                    if existing_map.as_ref() != labelmap.as_ref() {
                        return Err(crate::Error::from(
                            "label state error: hash collision occured (likelyhood was 2^16/2^65 ~ 1,7×10^-15) and I'm lazy to resolve it",
                        ));
                    }
                }
                None => {
                    self.stream_reg
                        .borrow_mut()
                        .insert(stream_id, labelmap.clone());
                    let stream_update = ManifestEntry::stream_update(stream_id, labelmap.clone())
                        .expect("labelmap was invalid based on ManifestEntry::stream_update(_, _), but it came from LabelMapCodec::decode(_)");
                    self.stream_buffer.push(stream_update);
                }
            }

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("failed to make UNIX timestemp")
                .as_nanos() as u64;

            let key = record::Key {
                source_id,
                timestamp,
                sequence_num: self.next_seq_num.inc_and_get(),
                stream_id,
            };

            let rec = Record {
                key,
                payload: payload[label_bytes_read..].into(),
            };

            self.io_tx
                .send(io::IoEvent::AppendWal(rec.clone()))
                .map_err(|e| crate::Error::from(format!("{e}")))?;

            if let AppendOutput::Full(partial_rec) = self.memtable.append(rec) {
                self.issue_flush()?;

                if let Some(r) = partial_rec {
                    error!("REJECTED KEY: {:?}", r.key);
                    // debug_assert!(
                    assert!(
                        self.memtable.append(r).was_appended(),
                        "append_batch: empty (frozen-then-replaced) memtable should insert the first record, even if it overflows the memory limit"
                    );
                }
            }
        }

        self.io_tx
            .send(io::IoEvent::FsyncWal)
            .map_err(|e| crate::Error::from(format!("{e}")))?;

        Ok(())
    }

    #[instrument(skip(self, labels), fields(labels_size_hint = ?labels.size_hint()), err)]
    pub fn range<'lsm, I, S>(
        &'lsm self,
        labels: I,
        start_t: Option<u64>,
        end_t: Option<u64>,
    ) -> Result<
        LabeledIter<'lsm, R, VecIntoIter<MergeIter<'lsm, 'lsm, R>>>,
        RangeError,
    >
    where
        S: AsRef<str>,
        I: Iterator<Item = (S, S)>,
    {
        let start_t = start_t.unwrap_or(0);
        let end_t = end_t.unwrap_or(u64::MAX);

        let stream_ids = self.stream_reg.borrow().label_intersection(labels);

        let merge_iters: Result<Vec<_>, sst::SstError> = stream_ids
            .into_iter()
            .map(|id| {
                let start = Bound::Included(Key {
                    stream_id: id,
                    timestamp: start_t,
                    sequence_num: 0,
                    source_id: 0,
                });

                let end = Bound::Included(Key {
                    stream_id: id,
                    timestamp: end_t,
                    sequence_num: u64::MAX,
                    source_id: u64::MAX,
                });

                MergeIter::new(
                    &self.memtable,
                    &self.frozen_memtables,
                    &self.sst_handles,
                    start,
                    end,
                )
            })
            .collect();

        let merge_iters = merge_iters.map_err(RangeError::SstReadError)?;

        Ok(LabeledIter::new(merge_iters, self.stream_reg.clone()))
    }

    pub fn sst_handles(&self) -> &[sst::SstHandle<R>] {
        &self.sst_handles
    }

    /// NOTE: one call to to the io task's flush_memtables
    /// should correspond to exactly one call to drain_sst_queue]
    pub fn drain_sst_queue(
        &mut self,
        flush_results: &mpsc::Receiver<sst::SstHandle<R>>,
    ) {
        error!("\t=> lsm.drain_sst_queue() called");
        for sst in flush_results.try_iter() {
            error!("draining sst {}", sst.id());
            if let Some(frozen) = self.frozen_memtables.front()
                && frozen.id() == sst.id()
            {
                error!("\t=> popping sst {}", sst.id());
                self.frozen_memtables.pop_front();
            }

            self.sst_handles.push(sst);
        }

        error!(still_frozen = ?self.frozen_memtables);
    }

    pub fn active_memtable(&self) -> &MutMemtable {
        &self.memtable
    }

    pub fn frozen_memtables(&self) -> &VecDeque<FrozenMemtable> {
        &self.frozen_memtables
    }

    pub fn all_memtable_size_bytes(&self) -> usize {
        self.frozen_memtables
            .iter()
            .fold(self.memtable.size_bytes(), |acc, f| acc + f.size_bytes())
    }

    fn issue_flush(&mut self) -> crate::Result<()> {
        let frozen = self.memtable.freeze(self.next_memtable_id.inc_and_get());
        self.frozen_memtables.push_back(frozen.clone());

        let flush_payload = io::FlushMemtablePayload {
            memtable: frozen.clone().into(),
            manifest_entries: std::mem::take(&mut self.stream_buffer),
            block_size_limit: self.config.block_size_limit,
            dir: self.config.basepath.clone(),
            record_codec: self.config.record_codec.clone(),
        };

        self.io_tx
            .send(io::IoEvent::FlushMemtable(flush_payload))
            .map_err(|e| crate::Error::from(format!("{e}")))?;

        Ok(())
    }
}

#[cfg(test)]
impl<R, L> LsmTree<R, L>
where
    R: RecordCodecExt,
    L: SpecCodec<LabelMap> + 'static,
{
    pub fn new_test(
        basepath: PathBuf,
        memtable_limit: usize,
        record_codec: R,
        label_codec: L,
        block_size_limit: Option<usize>,
        retention_days: Option<u32>,
        io_tx: mpsc::Sender<io::IoEvent<R>>,
    ) -> std::io::Result<Self> {
        let next_memtable_id = crate::counter::Counter::default();
        let memtable =
            MutMemtable::new(next_memtable_id.current(), memtable_limit);
        let stream_buffer = Vec::new();
        let frozen_memtables = VecDeque::new();
        let sst_handles = Vec::new();
        let config = LsmConfig {
            basepath,
            record_codec,
            block_size_limit,
            label_codec,
            _retention_days: retention_days,
        };
        let stream_reg = Rc::new(RefCell::new(StreamRegistry::default()));

        Ok(Self {
            memtable,
            frozen_memtables,
            sst_handles,
            stream_reg,
            next_memtable_id,
            next_stream_id: 0.into(),
            next_seq_num: 0.into(),
            config,
            io_tx,
            stream_buffer,
        })
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, sync::mpsc};

    use pretty_assertions::assert_eq;

    use crate::{
        codec::{SpecCodec, WireLen},
        label::{LabelMap, LabelMapCodec},
        lsm::LsmTree,
        record::{KEY_SIZE, RecordCodec},
    };

    fn tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            // .with_target(true)
            // .with_span_events(FmtSpan::NEW)
            .with_test_writer()
            .try_init();
    }

    fn data(codec: LabelMapCodec) -> (Box<[u8]>, Box<[u8]>, LabelMap) {
        let mut map = BTreeMap::new();
        map.insert("foo".into(), "bar".into());
        map.insert("bar".into(), "baz".into());
        map.insert("baz".into(), "foo".into());
        let lm = LabelMap::new(map);
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
        let tmpdir = tempfile::TempDir::new().expect("tempdir");
        let label_codec = LabelMapCodec;
        let (payload_with_labelmap, payload_no_labelmap, _labelmap) =
            data(label_codec);

        let max_count = 4;
        let record_size_of =
            KEY_SIZE + size_of::<Box<[u8]>>() + payload_no_labelmap.len();
        // let record_wire_len = KEY_SIZE + SZ_U32 + payload_no_labelmap.len();
        let memtable_limit = max_count * record_size_of;
        let record_codec = RecordCodec;

        let (io_tx, _io_rx) = mpsc::channel();
        let mut lsm = LsmTree::new_test(
            tmpdir.path().to_path_buf(),
            memtable_limit,
            record_codec,
            label_codec,
            None,
            None,
            io_tx,
        )
        .expect("lsmstate::new");

        let mut computed_sz = 0;
        let record_count = max_count;
        for i in 0..record_count {
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
            lsm.active_memtable().size_bytes(),
            lsm.all_memtable_size_bytes(),
            // &lsm.active_memtable,
            // &lsm.frozen_memtables,
        );
        assert_eq!(lsm.active_memtable().size_bytes(), 0);
        assert_eq!(lsm.all_memtable_size_bytes(), computed_sz);

        // dbg!(&lsm);

        for i in 0..max_count * 2 {
            lsm.append_single(&payload_with_labelmap, i as u64)
                .expect("lsm::append");
            computed_sz += record_size_of;
        }
        // dbg!(&lsm);
        assert_eq!(lsm.all_memtable_size_bytes(), computed_sz);

        // println!("=> Checking frozen state <=");
        // let mut j = 0;
        // while let Some((m, w)) = lsm.pop_frozen() {
        //     let mut frozen_memtable_cnt = 0;
        //     dbg!(&m);
        //     for rec in m.full_range() {
        //         println!("CHECK_FROZEN: {j}-{frozen_memtable_cnt}\tmemtable\t{rec}");
        //         frozen_memtable_cnt += 1;
        //     }

        //     w.flush().expect("CHECK_FROZEN: flush frozen_wal (since theyre not flushed when theyre flushed to save IO)");
        //     let wr = w.as_reader().expect("CHECK_FROZEN: frozen_wal.as_reader()");
        //     dbg!(&wr);

        //     let mut frozen_wal_cnt = 0;
        //     for e in wr {
        //         let rec = e.expect("CHECK_FROZEN: frozen_wal::framed_reader::next().unwrap()");
        //         println!("CHECK_FROZEN: {j}-{frozen_wal_cnt}\twal\t{rec}");
        //         frozen_wal_cnt += 1;
        //     }
        //     assert_eq!(
        //         frozen_memtable_cnt, frozen_wal_cnt,
        //         "CHECK_FROZEN: expected frozen_memtable and frozen_wal to have the same amount of records"
        //     );

        //     j += 1;
        // }

        // this is not actually valid,
        // i just wanna see the results (:
        // println!("visited frozen records: {j} out of all {record_count} records");
    }

    /// LsmState only freezes the memtables, flushing will
    /// be the role of a background Io worker,
    /// so this range only goes over memtables, not sstables
    #[test]
    fn range() {
        tracing();
        let tmpdir = tempfile::TempDir::new().expect("tempdir");
        let label_codec = LabelMapCodec;
        let (payload_with_labelmap, payload_no_labelmap, labelmap) =
            data(label_codec);

        let max_count = 4;
        let record_size_of =
            KEY_SIZE + size_of::<Box<[u8]>>() + payload_no_labelmap.len();
        let memtable_limit = max_count * record_size_of;
        let record_codec = RecordCodec;
        let (io_tx, _io_rx) = mpsc::channel();
        let mut lsm = LsmTree::new_test(
            tmpdir.path().to_path_buf(),
            memtable_limit,
            record_codec,
            label_codec,
            None,
            None,
            io_tx,
        )
        .expect("lsmstate::new");

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
            "expected merge iterator (only memtables) to have all records inserted to lsm"
        )
    }
}
