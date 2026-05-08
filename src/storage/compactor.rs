use std::collections::HashMap;
use std::io::{Seek, Write};
use std::sync::atomic::Ordering;
use std::{fs, io};
use std::path::PathBuf;

use anyhow::Context;

use crate::storage::{BloomFilter, Engine};
use crate::storage::engine::SSTableMeta;
use crate::storage::iter::{MergeIter, SSTableScanner};

/// A background task that:
/// - Picks a set of SSTables to merge
/// - Reads all records from them in sorted order (multi-way merge of BTreeSet<Record>-ordered streams)
/// - For each (source_id, ts, key), keeps only the latest record (last one in merge order)
/// - Drops records whose latest value is a tombstone
/// - Writes a new merged SSTable, deletes the originals
pub(crate) struct Compactor {
    file_num_limit: u64,
    file_sz_limit: u64,
}

impl Compactor {
    /// collect_tables looks at the current list of sstables
    /// in the engine, if <code>file_num_limit</code> many sstables has exceeded
    /// the <code>file_sz_limit</code> limit, they are returned to be merged later on.
    pub(crate) fn collect_tables<'a>(&'a self, map: &'a mut HashMap<PathBuf, SSTableMeta>) -> Option<Vec<(&'a PathBuf, &'a SSTableMeta)>> {
        let mut v= vec![];
        for (path, meta) in map.iter() {
            if meta.file_size >= self.file_sz_limit {
                v.push((path, meta));
            }

            if v.len() >= self.file_num_limit as usize {
                return Some(v)
            }
        }
        None
    }

    pub(crate) fn merge<'a>(&'a self, engine: &'a mut Engine, tables: Vec<(&'a PathBuf, &'a SSTableMeta)>) -> anyhow::Result<()>{
        let num_records_hint: usize = tables.iter().map(|(_, m)| m.num_records).sum();

        let sstable_scanners: Vec<SSTableScanner<'_>> = tables
            .iter()
            .filter_map(|(p, m)| {
                SSTableScanner::new(p, m)
                    .inspect_err(|e| {
                        tracing::error!(error = %e, "compactor.merge: failed to turn sstable path to iterator");
                    })
                    .ok()
            })
            .collect();

        let merge_iter = MergeIter::new(None, sstable_scanners);
        let mut path = engine.dir.clone();
        path.push(format!( "{:010}.sst", engine.sst_counter.load(Ordering::Relaxed)));
        
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .context("compactor.merge: failed to open sstable file")?;
        let mut w = io::BufWriter::new(f);
        let mut bloom = BloomFilter::new(num_records_hint, 0.01);
        let mut actual_records = 0usize;

        for rec in merge_iter {
            let len_buf = (rec.len() as u64).to_le_bytes();
            w.write_all(&len_buf)
                .context("compactor.merge: failed to write record length")?;
            w.write_all(rec.as_bytes())
                .context("compactor.merge: failed to write record")?;
            bloom.insert(rec.extract_source_id()?, rec.extract_key()?);
            actual_records += 1
        }

        let bloom_offset = w.stream_position()
            .context("compactor.merge: failed to seek to current position")?;
        bloom.encode(&mut w)
            .context("compactor.merge: failed to write bloom filter footer")?;
        w.write_all(&bloom_offset.to_le_bytes())
            .context("compactor.merge: failed to write bloom filter offset")?;

        w.flush().context("compactor.merge: failed to flush write buffer")?;
        w.get_ref()
            .sync_all()
            .context("compactor.merge: failed to sync_all file")?;

        engine.sstable_map.insert(path.clone(), SSTableMeta {
            bloom,
            bloom_offset,
            num_records: actual_records,
            file_size: fs::metadata(&path)
                .context("compactor.merge: failed to read sstable file metadata")?
                .len(),
        });

        for (p, _) in tables {
            engine.sstable_map.remove(p);
        }

        engine.sst_counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}