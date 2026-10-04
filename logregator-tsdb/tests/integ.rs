#[cfg(test)]
#[ignore = "compilation"]
#[allow(unused)]
mod test {
    use std::{collections::BTreeMap, path::PathBuf, sync::mpsc};

    use logregator_tsdb::{
        codec::{RecordCodecExt, SpecCodec, WireLen},
        io,
        label::{LabelMap, LabelMapCodec, StreamRegistry},
        lsm::LsmTree,
        manifest::{Manifest, ManifestCodec, ManifestCodecExt},
        memtable::MutMemtable,
        record::{Key, Record, RecordCodec},
        sst,
        wal::Wal,
    };

    use tracing::{info, instrument, trace, warn};

    struct TmpPath(PathBuf);

    impl TmpPath {
        fn new() -> Self {
            let p = tempfile::TempDir::new()
                .expect("failed to create tmp dir")
                .keep();
            TmpPath(p)
        }
    }

    impl Drop for TmpPath {
        fn drop(&mut self) {
            trace!("CLEANUP tempdir");
            if let Err(e) = std::fs::remove_dir_all(&self.0) {
                warn!("failed to remove tempdir: {e}")
            }
        }
    }

    fn tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init();
    }

    fn key(source_id: u64, ts: u64, seq: u64) -> Key {
        Key {
            source_id,
            timestamp: ts,
            sequence_num: seq,
            stream_id: 7,
        }
    }

    fn record(source_id: u64, ts: u64, seq: u64, payload_len: usize) -> Record {
        Record {
            key: key(source_id, ts, seq),
            payload: vec![0xAB_u8; payload_len].into(),
        }
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

        let payload_without_labelmap: Vec<u8> =
            std::iter::repeat_n("_SixSeven_", 32)
                .flat_map(|s| s.chars())
                .map(|c: char| c as u8)
                .collect();
        payload_with_labelmap.extend_from_slice(&payload_without_labelmap);

        (
            payload_with_labelmap.into_boxed_slice(),
            payload_without_labelmap.into_boxed_slice(),
            lm,
        )
    }

    #[test]
    fn new_blank() {
        tracing();

        let queue_size = Some(32);
        let record_codec = RecordCodec;
        let label_codec = LabelMapCodec;
        let manifest_codec = ManifestCodec::new(label_codec);
        let memtable_size_limit = 1024 * 4;
        let manifest_size_limit = Some(1024 * 2);
        let block_size_limit = Some(8 * 1024);

        let basepath = TmpPath::new();

        info!("1) RECOVER wal + memtable");
        let mut wal =
            Wal::new(basepath.0.as_ref(), record_codec).expect("wal open");
        let next_seq_num = 0;

        // TODO: we could get this from the MANIFEST, by finding the sstable with the highest id and incrementing it
        // as no other memtable has been persisted to disk at that point
        let next_memtable_id = 0;
        let memtable =
            MutMemtable::with_size_limit(next_memtable_id, memtable_size_limit);

        info!("2) RECOVER manifest");
        let mut manifest = Manifest::open(
            basepath.0.as_ref(),
            manifest_codec,
            manifest_size_limit,
        )
        .expect("memtable new");

        let stream_reg = StreamRegistry::default();
        let next_stream_id = 0;
        let sst_handles = vec![];

        let (io_tx, io_rx) = mpsc::channel();

        let (sst_tx, sst_rx) = mpsc::channel();

        let mut lsm = LsmTree::new_uninit(
            basepath.0.clone(),
            record_codec,
            label_codec,
            block_size_limit,
            memtable,
            sst_handles,
            stream_reg,
            next_stream_id,
            next_seq_num,
            None,
            io_tx,
        );

        info!("3) GENERATE/INSERT input batches");
        let rounds = 5;
        let batch_size = 10;
        let mut input_batches: Vec<Vec<Box<[u8]>>> = vec![];
        let mut expected_payloads: Vec<Vec<u8>> = vec![];

        let (_, _, sample_lm) = data(label_codec);
        let label_header_len = sample_lm.wire_len();

        for r in 0..rounds {
            let mut raw_batch = vec![];
            for _ in 0..batch_size {
                let (p_with_labels, p_without_labels, _) = data(label_codec);
                expected_payloads.push(p_without_labels.to_vec());
                raw_batch.push(p_with_labels);
            }
            input_batches.push(raw_batch);
        }

        for (i, b) in input_batches.iter().enumerate() {
            lsm.append_batch(b.iter().map(|p| p.as_ref()), i as u64)
                .expect("append_batch");
        }

        info!("4) PROCESS io events");
        test_process_io_events(&io_rx, &mut wal, &mut manifest, sst_tx)
            .expect("processing io events");

        lsm.drain_sst_queue(&sst_rx);

        info!("5) EXTRACT RECORDS from sstables and memtables");
        let mut found_sst_batches: Vec<Vec<Record>> = vec![];

        for (i, handle) in lsm.sst_handles().iter().enumerate() {
            let mut cursor = handle
                .cursor(None)
                .expect("cursor()")
                .expect("cursor(): returned Ok(None) for bounds = None");

            let mut sst_records = vec![];
            while let Some(rec) = cursor.current_record() {
                sst_records.push(rec.clone());
                cursor.next();
            }
            found_sst_batches.push(sst_records);
        }

        println!("{:>3}: {}", "Input Batches Sent", input_batches.len());
        println!("{:>3}: {}", "Total Input Records", expected_payloads.len());
        println!("{:>3}: {}", "SST Files Flushed", lsm.sst_handles().len());
        println!(
            "{:<3}: {} bytes",
            "Active Memtable Size",
            lsm.active_memtable().size_bytes()
        );
        println!("--------------------------------------------------");

        println!("\n[INPUT BATCHES SUMMARY]");
        for (i, batch) in input_batches.iter().enumerate() {
            println!(
                "  Batch [{i}]: {} raw payloads (Source ID: {i})",
                batch.len()
            );
        }

        println!("\n[SSTABLE RECORDS FOUND]");
        for (i, sst_batch) in found_sst_batches.iter().enumerate() {
            println!("  SST File [{i}]: {} records", sst_batch.len());
            for (j, rec) in sst_batch.iter().take(3).enumerate() {
                println!(
                    "    Record {j} => StreamID: {}, SeqNum: {}, Timestamp: {}, Payload Size: {} len",
                    rec.key.stream_id,
                    rec.key.sequence_num,
                    rec.key.timestamp,
                    rec.payload.len()
                );
            }
            if sst_batch.len() > 3 {
                println!("    ... and {} more records", sst_batch.len() - 3);
            }
        }

        info!("6) QUERY RECORDS using lsm.range()");
        let range_iter =
            lsm.range(sample_lm.iter(), None, None).expect("lsm.range");
        let mut range_records: Vec<Record> = vec![];

        for (_labelmap, rec) in range_iter {
            range_records.push(rec.as_ref().clone());
        }

        println!("\n[ LSM TREE RANGE QUERY RESULTS ]");
        println!("  Total Range Records Returned: {}", range_records.len());
        println!("==================================================\n");

        for (i, r) in range_records.iter().enumerate() {
            println!("{i} -> {:?}", r.key.sequence_num);
        }

        // --- ASSERTIONS ---

        // 1. Ensure range query retrieved every single record inserted
        assert_eq!(
            range_records.len(),
            expected_payloads.len(),
            "range() count must match total records inserted"
        );

        // 2. Compare decoded actual payloads against expected stripped payloads
        let actual_payloads: Vec<Vec<u8>> =
            range_records.iter().map(|r| r.payload.to_vec()).collect();

        pretty_assertions::assert_eq!(expected_payloads, actual_payloads);
    }

    #[instrument(skip(evs, wal, manifest, sst_tx))]
    pub fn test_process_io_events<R, L, M>(
        evs: &mpsc::Receiver<io::IoEvent<R>>,
        wal: &mut Wal<R>,
        manifest: &mut Manifest<M, L>,
        sst_tx: mpsc::Sender<sst::SstHandle<R>>,
    ) -> logregator_tsdb::Result<()>
    where
        R: RecordCodecExt,
        L: SpecCodec<LabelMap>,
        M: ManifestCodecExt<L>,
    {
        while let Ok(e) = evs.try_recv() {
            trace!("processing io event {e:?}");
            match e {
                io::IoEvent::AppendWal(rec) => wal.append(&rec)?,
                io::IoEvent::FsyncWal => {
                    wal.fsyncdata()?;
                }
                io::IoEvent::FlushMemtable(payload) => {
                    let handle = io::flush_memtable(manifest, payload)?;
                    sst_tx.send(handle).map_err(|e| {
                        logregator_tsdb::Error::from(format!("{e}"))
                    })?;
                }
            }
        }

        Ok(())
    }
}
