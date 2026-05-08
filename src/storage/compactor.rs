
/// A background task that:
/// - Picks a set of SSTables to merge
/// - Reads all records from them in sorted order (multi-way merge of BTreeSet<Record>-ordered streams)
/// - For each (source_id, ts, key), keeps only the latest record (last one in merge order)
/// - Drops records whose latest value is a tombstone
/// - Writes a new merged SSTable, deletes the originals
pub(crate) struct Compactor {

}