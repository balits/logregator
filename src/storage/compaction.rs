use std::io::{Seek, Write};
use std::path::PathBuf;
use std::{fs, io};

use anyhow::Context;
use tokio::sync::mpsc;

use crate::storage::BloomFilter;
use crate::storage::engine::SSTableMeta;
use crate::storage::iter::{MergeIter, SSTableScanner};

#[derive(Debug)]
pub(crate) struct CompactionCommand {
    pub(crate) tables: Vec<SSTableMeta>,
    pub(crate) new_file_path: PathBuf,
    pub(crate) new_file_id: usize,
}

#[derive(Debug)]
pub(crate) struct CompactionResult {
    pub(crate) new_meta: SSTableMeta,
    pub(crate) ids_to_remove: Vec<usize>,
}

pub(crate) async fn compactor_loop(
    mut cmd_rx: mpsc::Receiver<CompactionCommand>,
    result_tx: mpsc::Sender<CompactionResult>,
) -> anyhow::Result<()> {
    loop {
        if let Some(cmd) = cmd_rx.recv().await {
            let res = compact(cmd)
                .context("compactor: failed to compact")?;

            result_tx.send(res).await.context("compactor: failed to send compaction result")?;
        }
    }
}

fn compact(cmd: CompactionCommand) -> anyhow::Result<CompactionResult> {
    let num_records_hint: usize = cmd.tables.iter().map(|m| m.num_records).sum();
    let ids_to_remove: Vec<usize> = cmd.tables.iter().map(|m| m.id).collect();

    let sstable_scanners: Vec<SSTableScanner<'_>> = cmd
        .tables
        .iter()
        .filter_map(|meta| {
            SSTableScanner::new(&meta.path, meta)
                .inspect_err(|e| {
                    tracing::error!(error = %e, "compact: failed to turn sstable path to iterator");
                })
                .ok()
        })
        .collect();

    let merge_iter = MergeIter::new(None, sstable_scanners);

    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&cmd.new_file_path)
        .context("compact: failed to open sstable file")?;
    let mut w = io::BufWriter::new(f);
    let mut bloom = BloomFilter::new(num_records_hint, 0.01);
    let mut new_num_records = 0usize;

    for rec in merge_iter {
        let len_buf = (rec.len() as u64).to_le_bytes();
        w.write_all(&len_buf)
            .context("compact: failed to write record length")?;
        w.write_all(rec.as_bytes())
            .context("compact: failed to write record")?;
        bloom.insert(rec.extract_source_id()?, rec.extract_key()?);
        new_num_records += 1
    }

    let bloom_offset = w
        .stream_position()
        .context("compact: failed to seek to current position")?;
    bloom
        .encode(&mut w)
        .context("compact: failed to write bloom filter footer")?;
    w.write_all(&bloom_offset.to_le_bytes())
        .context("compact: failed to write bloom filter offset")?;

    w.flush().context("compact: failed to flush write buffer")?;
    w.get_ref()
        .sync_all()
        .context("compact: failed to sync_all file")?;

    let new_file_size = fs::metadata(&cmd.new_file_path)
        .context("compactor.merge: failed to read sstable file metadata")?
        .len();

    let new_meta = SSTableMeta {
        id: cmd.new_file_id,
        path: cmd.new_file_path,
        bloom,
        bloom_offset,
        num_records: new_num_records,
        file_size: new_file_size,
    };

    Ok(CompactionResult {
        new_meta,
        ids_to_remove,
    })
}
