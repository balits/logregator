use std::{collections::VecDeque, ops::Bound, path::Path, rc::Rc, sync::Arc};

use crate::{
    codec::RecordCodec,
    memtable::{AppendOutput, FrozenMemtable, Memtable},
    merge_iter::MergeIter,
    record::{Key, Record},
    sst::{self, SstFileWriter, SstHandle, SstReadError},
};

pub struct Lsm<C> {
    _state: LsmState<C>,
}

pub struct LsmState<C> {
    active_memtable: Memtable,
    frozen_memtables: VecDeque<Arc<FrozenMemtable>>,
    sst_handles: Vec<Rc<SstHandle<C>>>,
}

impl<C> LsmState<C>
where
    C: RecordCodec,
{
    pub fn append(&mut self, rec: Record) {
        if AppendOutput::Full == self.active_memtable.append(rec) {
            let f = Arc::new(self.active_memtable.freeze());
            self.frozen_memtables.push_back(f);
        }
    }

    pub fn range(
        &self,
        start: Bound<&Key>,
        end: Bound<&Key>,
    ) -> Result<MergeIter<'_, C>, SstReadError> {
        MergeIter::new(
            &self.active_memtable,
            &self.frozen_memtables,
            &self.sst_handles,
            start,
            end,
        )
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
pub fn flush<C: RecordCodec>(
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
