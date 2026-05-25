use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use tokio::sync::mpsc;

use crate::metrics::Metrics;
use crate::storage::SSTableMeta;
use crate::storage::iter::{MergeIter, RecordIter, SSTableScaner};

#[derive(Debug)]
pub struct CompactionCommand {
    pub(crate) tables: Vec<SSTableMeta>,
    pub(crate) new_file_path: PathBuf,
    pub(crate) new_file_id: u64,
}

#[derive(Debug)]
pub struct CompactionResult {
    pub(crate) new_meta: SSTableMeta,
    pub(crate) ids_to_remove: Vec<u64>,
}

pub async fn compaction_loop(
    mut cmd_rx: mpsc::Receiver<CompactionCommand>,
    result_tx: mpsc::Sender<CompactionResult>,
    metrics: Option<Arc<Metrics>>,
) -> anyhow::Result<()> {
    loop {
        if let Some(cmd) = cmd_rx.recv().await {
            let _t0 = Instant::now();
            let res = compact(cmd).context("compactor: failed to compact")?;
            if let Some(ref m) = metrics {
                m.engine.compaction_count.inc(1);
                m.engine.compaction_duration.record_instant(_t0);
            }

            result_tx
                .send(res)
                .await
                .context("compactor: failed to send compaction result")?;
        }
    }
}

fn compact(cmd: CompactionCommand) -> anyhow::Result<CompactionResult> {
    let num_records_hint: usize = cmd.tables.iter().map(|m| m.num_records).sum();
    let ids_to_remove: Vec<u64> = cmd.tables.iter().map(|m| m.id).collect();

    let sstable_scanners: Vec<RecordIter> = cmd
        .tables
        .into_iter()
        .filter_map(|meta| {
            SSTableScaner::new(meta)
                .inspect_err(|e| {
                    tracing::error!(error = %e, "compact: failed to turn sstable path to iterator");
                })
                .ok()
                .map(RecordIter::Scan)
        })
        .collect();

    let merge_iter = MergeIter::new(None, sstable_scanners);

    let new_meta = SSTableMeta::write_to_file(
        cmd.new_file_path,
        cmd.new_file_id,
        merge_iter,
        num_records_hint,
    )
    .context("engine.flush: failed to write sstable file")?;

    Ok(CompactionResult {
        new_meta,
        ids_to_remove,
    })
}
