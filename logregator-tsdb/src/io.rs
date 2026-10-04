use std::{fmt::Debug, path::PathBuf};

use tracing::instrument;

use crate::{
    codec::{self, RecordCodecExt, SpecCodec},
    label::LabelMap,
    manifest::{Manifest, ManifestCodecExt, ManifestEntry},
    memtable::FlushableMemtable,
    sst::{self, SstFileWriter, SstHandle},
};

#[instrument(ret, err)]
pub fn flush_memtable<R, M, L>(
    manifest: &mut Manifest<M, L>,
    payload: FlushMemtablePayload<R>,
) -> crate::Result<SstHandle<R>>
where
    R: RecordCodecExt,
    M: ManifestCodecExt<L>,
    L: SpecCodec<LabelMap>,
{
    let FlushMemtablePayload {
        memtable,

        manifest_entries,
        block_size_limit,
        dir,
        record_codec,
    } = payload;

    let (range_start, range_end) = memtable.key_bounds_full()?;

    let id = memtable.id();
    let record_count = memtable.count();
    let mut sw = SstFileWriter::new(
        id,
        record_codec,
        block_size_limit,
        Some(&dir),
        record_count,
    )
    .map_err(|e| IoError::MemtableFlushError(e.into()))?;

    for rec in memtable.range(range_start, range_end) {
        sw.write(rec)
            .map_err(|e| IoError::MemtableFlushError(e.into()))?;
    }

    let handle = sw
        .finalize_file()
        .map_err(|e| IoError::MemtableFlushError(e.into()))?;

    manifest.append(&ManifestEntry::MemtableFlush(id))?;
    manifest.append_many(manifest_entries.iter())?;
    Ok(handle)
}

#[derive(Debug)]
pub enum IoEvent<R> {
    AppendWal(crate::record::Record),
    FsyncWal,
    FlushMemtable(FlushMemtablePayload<R>),
}

unsafe impl<R: Send> Send for IoEvent<R> {}

#[derive(Debug)]
pub struct FlushMemtablePayload<R> {
    pub memtable: FlushableMemtable,
    pub manifest_entries: Vec<ManifestEntry>,
    pub block_size_limit: Option<usize>,
    pub dir: PathBuf,
    pub record_codec: R,
}

#[derive(thiserror::Error, Debug)]
pub enum IoError {
    #[error("Backgroud-IO error: failed to send value over channel: {0}")]
    ChannelSendErr(String),

    #[error("Background-IO error: a `std::io::Error` occured: {0}")]
    StdIoError(String),

    #[error("Background-IO error: {0}")]
    MemtableFlushError(#[from] MemtableFlushError),
}

#[derive(Debug, thiserror::Error)]
pub enum MemtableFlushError {
    #[error("failed to flush memtable to disk: {0}")]
    SstError(#[from] sst::SstError),

    #[error(
        "failed to flush memtable to disk: failed to append entry to manifest: {0}"
    )]
    ManifestError(#[from] codec::CodecError),
}
