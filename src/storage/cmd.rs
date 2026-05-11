use crate::storage::{compaction, engine, iter::{SSTableIter, MergeIter}};


#[derive(Debug)]
pub enum Command {
    Compaction(compaction::CompactionCommand),
    Insert(engine::Insert),
    Range(engine::Range)
}

pub enum Result {
    Compaction(compaction::CompactionResult),
    Insert(anyhow::Result<()>),
    Range(anyhow::Result<MergeIter<SSTableIter>>),
}