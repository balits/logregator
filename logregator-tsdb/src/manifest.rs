use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Seek},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    codec::{CodecError, FramedReader, FramedWriter, SZ_U32, SZ_U64, SpecCodec, WireLen},
    label::{self, LabelMap, LabelMapCodec},
};
use serde::{Deserialize, Serialize};
use tracing::{instrument, trace};

pub const MANIFEST_FILE_NAME: &str = "MANIFEST";
pub const MANIFEST_SNAPSHOT_FILE_NAME: &str = "MANIFEST.snapshot";
pub const MAINFEST_DEFAULT_MAX_FILE_SIZE: usize = 20 * 1028;

/// TODO: should the methods return io::Error or CodecError::Io(io::Error)
pub struct Manifest {
    basepath: PathBuf,
    framed: FramedWriter<File, ManifestCodec, ManifestEntry>,
    f: File,
    codec: ManifestCodec,
    max_file_size: usize,
}

impl Manifest {
    pub fn open(
        path: impl AsRef<Path>,
        codec: ManifestCodec,
        max_file_size: Option<usize>,
    ) -> io::Result<Self> {
        let basepath = path.as_ref().to_path_buf();
        let f = File::options()
            .read(true)
            .create(true)
            .append(true)
            .open(basepath.join(MANIFEST_FILE_NAME))?;

        let framed = FramedWriter::new(f.try_clone()?, codec);

        Ok(Self {
            basepath,
            framed,
            f,
            codec,
            max_file_size: max_file_size.unwrap_or(MAINFEST_DEFAULT_MAX_FILE_SIZE),
        })
    }

    /// returns true if the manifest file filled up and a new snapshot is needed
    pub fn append(&mut self, e: &ManifestEntry) -> Result<bool, CodecError> {
        self.framed.write(e)?;
        Ok(self.framed.size_hint() > self.max_file_size)
    }

    /// creates a snapshot out of the current state of the
    /// manifest file, merges it with the pre-existing snapshot
    /// file if it exists, then serializes it back to the file as
    /// json
    pub fn snapshot(&mut self) -> Result<(), CodecError> {
        self.framed.flush()?;
        Snapshot::from_manifest(self)?.write_to(&self.basepath)?;
        self.f.set_len(0)?;
        self.f.sync_all()?;
        Ok(())
    }

    fn as_reader(&self) -> io::Result<FramedReader<File, ManifestCodec, ManifestEntry>> {
        let mut f = self.f.try_clone()?;
        f.seek(io::SeekFrom::Start(0))?;
        Ok(FramedReader::new(f, self.codec))
    }

    #[cfg(test)]
    fn sync(&mut self) -> io::Result<()> {
        self.framed.flush()?;
        self.f.sync_all()?;
        Ok(())
    }

    #[cfg(test)]
    fn new_reader(
        p: impl AsRef<Path>,
        codec: ManifestCodec,
    ) -> io::Result<FramedReader<File, ManifestCodec, ManifestEntry>> {
        let basepath = p.as_ref().to_path_buf();
        let f = OpenOptions::new()
            .read(true)
            .open(basepath.join(MANIFEST_FILE_NAME))?;
        Ok(FramedReader::new(f, codec))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum SstState {
    Live,
    Removed,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Snapshot {
    pub(crate) sstable_state: HashMap<u64, SstState>,
    pub(crate) labelmaps: HashMap<u64, Arc<LabelMap>>,
}

impl Snapshot {
    pub fn from_manifest(manifest: &Manifest) -> Result<Snapshot, CodecError> {
        let r = manifest.as_reader()?;
        let mut ssts = HashMap::new();
        let mut labelmaps = HashMap::new();

        for e in r {
            let entry = e?;
            match entry {
                ManifestEntry::SstFlush(id) => {
                    ssts.insert(id, SstState::Live);
                }
                ManifestEntry::Compaction(ids) => {
                    for id in ids {
                        ssts.insert(id, SstState::Removed);
                    }
                }
                ManifestEntry::StreamUpdate {
                    stream_id,
                    labelmap,
                } => {
                    labelmaps.insert(stream_id, labelmap);
                }
            };
        }

        Ok(Self {
            sstable_state: ssts,
            labelmaps,
        })
    }

    pub fn write_to(self, basepath: &Path) -> io::Result<()> {
        let snap_path = basepath.join(MANIFEST_SNAPSHOT_FILE_NAME);

        let snap = if std::fs::exists(&snap_path)? {
            let f = OpenOptions::new().read(true).open(&snap_path)?;
            let old: Self = serde_json::from_reader(f)?;
            Self::merge(old, self)
        } else {
            self
        };

        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(snap_path)?;
        serde_json::to_writer_pretty(f, &snap).map_err(io::Error::other)?;
        Ok(())
    }

    fn merge(mut older: Self, delta: Self) -> Self {
        let mut ssts = older.sstable_state;
        ssts.retain(|_, v| matches!(v, SstState::Live));

        for (id, state) in delta.sstable_state {
            match state {
                SstState::Live => ssts.insert(id, state),
                SstState::Removed => ssts.remove(&id),
            };
        }

        for (stream_id, labelmap) in delta.labelmaps {
            older.labelmaps.insert(stream_id, labelmap);
        }

        Self {
            sstable_state: ssts,
            labelmaps: older.labelmaps,
        }
    }

    #[cfg(test)]
    fn open_for_test(basepath: &Path) -> io::Result<Self> {
        let snap_path = basepath.join(MANIFEST_SNAPSHOT_FILE_NAME);
        let f = OpenOptions::new().read(true).open(&snap_path)?;
        let s = serde_json::from_reader(f)?;
        Ok(s)
    }
}

const MANIFEST_ENTRY_TAG_SZ: usize = 1;

/// Represents what can reside in the Manifest file.
/// Users should call the respective constructors functions
/// of each enum variant so that their respective
/// invariants are upheld and panics/UB happens during
/// encoding / decoding.  
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestEntry {
    SstFlush(u64),
    /// Currently the compaction mechanism (delete all ssts older than N days)  
    /// is not implemented, but in the future this should hold multiple sst ids
    /// that is a Vec<u64> instead of one u64
    Compaction(Vec<u64>),
    StreamUpdate {
        stream_id: u64,
        labelmap: Arc<LabelMap>,
    },
}

impl WireLen for ManifestEntry {
    #[inline]
    fn wire_len(&self) -> usize {
        let mut sz = 1; // tag
        match self {
            Self::SstFlush(_) => {
                sz += SZ_U64;
            }
            Self::Compaction(v) => {
                sz += SZ_U32; // len prefix
                sz += v.len() * SZ_U64;
            }
            Self::StreamUpdate {
                stream_id: _,
                labelmap,
            } => {
                sz += SZ_U64;
                sz += labelmap.wire_len();
            }
        };
        sz
    }
}

impl ManifestEntry {
    pub fn tag(&self) -> u8 {
        match self {
            Self::SstFlush(_) => 1,
            Self::Compaction(_) => 2,
            Self::StreamUpdate { .. } => 3,
        }
    }

    pub fn sst_flush(id: u64) -> Self {
        Self::SstFlush(id)
    }

    pub fn compaction(ids: Vec<u64>) -> Option<Self> {
        if ids.len() > u32::MAX as usize {
            None
        } else {
            Some(Self::Compaction(ids))
        }
    }

    pub fn stream_update(stream_id: u64, labelmap: Arc<LabelMap>) -> Option<Self> {
        if labelmap.len() > label::MAX_LABEL_COUNT {
            return None;
        }

        for (k, v) in labelmap.iter() {
            if !(label::MIN_STR_SIZE..label::MAX_STR_SIZE).contains(&k.len()) {
                return None;
            }
            if !(label::MIN_STR_SIZE..label::MAX_STR_SIZE).contains(&v.len()) {
                return None;
            }
        }

        Some(Self::StreamUpdate {
            stream_id,
            labelmap,
        })
    }

    pub fn variant_str(&self) -> &str {
        match self {
            Self::SstFlush(_) => "ManifestEntry::SstFlush",
            Self::Compaction(_) => "ManifestEntry::Compaction",
            Self::StreamUpdate { .. } => "ManifestEntry::StreamUpdate",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ManifestCodec {
    label_codec: LabelMapCodec,
}

impl SpecCodec<ManifestEntry> for ManifestCodec {
    #[instrument(ret, err)]
    fn encode(&self, item: &ManifestEntry, dst: &mut [u8]) -> Result<usize, CodecError> {
        if dst.len() < item.wire_len() {
            trace!(
                "not enough bytes to encode {item:?} into `dst` (got: {}, want: {}, entry variant {})",
                dst.len(),
                item.wire_len(),
                item.variant_str(),
            );

            return Err(crate::codec::not_enough_bytes(dst.len(), item.wire_len()));
        }
        dst[0] = item.tag();

        trace!(manifest_entry_wire_len = item.wire_len());
        match item {
            ManifestEntry::SstFlush(id) => {
                dst[1..1 + SZ_U64].copy_from_slice(&id.to_be_bytes());
            }
            ManifestEntry::Compaction(ids) => {
                dst[1..1 + SZ_U32].copy_from_slice(&(ids.len() as u32).to_be_bytes());
                let mut n = 1 + SZ_U32;
                for id in ids {
                    dst[n..n + SZ_U64].copy_from_slice(&id.to_be_bytes());
                    n += SZ_U64;
                }

                if n != item.wire_len() {
                    return Err(CodecError::Other(format!(
                        "failed to encode Sstable ids of {}, wrote {} bytes out of {}",
                        item.variant_str(),
                        n,
                        item.wire_len()
                    )));
                }
            }
            ManifestEntry::StreamUpdate {
                stream_id,
                labelmap,
            } => {
                dst[1..1 + SZ_U64].copy_from_slice(&stream_id.to_be_bytes());
                let n = self.label_codec.encode(
                    labelmap,
                    &mut dst[1 + SZ_U64..1 + SZ_U64 + labelmap.wire_len()],
                )?;

                if n != labelmap.wire_len() {
                    return Err(CodecError::Other(format!(
                        "failed to encode labelmap field of ManifestEntry, wrote {} bytes out of {}",
                        n,
                        labelmap.wire_len()
                    )));
                }
            }
        };

        Ok(item.wire_len())
    }

    #[instrument(skip(self, src), ret, err)]
    fn decode(&self, src: &[u8]) -> Result<Option<(ManifestEntry, usize)>, CodecError> {
        if src.len() < MANIFEST_ENTRY_TAG_SZ {
            trace!(
                "not enough bytes to decode from (got: {}, want: {})",
                src.len(),
                MANIFEST_ENTRY_TAG_SZ
            );

            return Ok(None);
        }

        let tag = src[0];

        let me = match tag {
            1 => {
                if src.len() < MANIFEST_ENTRY_TAG_SZ + SZ_U64 {
                    trace!("not enough bytes to decode ManifestEntry::SstFlush(id)'s id");
                    return Err(CodecError::NotEnoughBytes(crate::codec::NotEnoughBytes {
                        got: src.len(),
                        want: MANIFEST_ENTRY_TAG_SZ + SZ_U64,
                    }));
                }
                let id = u64::from_be_bytes([
                    src[1], src[2], src[3], src[4], src[5], src[6], src[7], src[8],
                ]);
                ManifestEntry::SstFlush(id)
            }
            2 => {
                if src.len() < MANIFEST_ENTRY_TAG_SZ + SZ_U32 {
                    trace!("not enough bytes to decode ManifestEntry::Compaction(ids)'s len");
                    return Err(CodecError::NotEnoughBytes(crate::codec::NotEnoughBytes {
                        got: src.len(),
                        want: MANIFEST_ENTRY_TAG_SZ + SZ_U32,
                    }));
                }
                let len = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;
                if src.len() < MANIFEST_ENTRY_TAG_SZ + SZ_U32 + len * SZ_U64 {
                    trace!(
                        "not enough bytes to decode ManifestEntry::Compaction(ids)'s ids (parsed length * SZ_U64 > src.len())"
                    );
                    return Err(CodecError::NotEnoughBytes(crate::codec::NotEnoughBytes {
                        got: src.len(),
                        want: MANIFEST_ENTRY_TAG_SZ + SZ_U32 + len * SZ_U64,
                    }));
                }

                let n = MANIFEST_ENTRY_TAG_SZ + SZ_U32;
                let mut v = Vec::with_capacity(len);
                for _ in 0..len {
                    let sst_id = u64::from_be_bytes([
                        src[n],
                        src[n + 1],
                        src[n + 2],
                        src[n + 3],
                        src[n + 4],
                        src[n + 5],
                        src[n + 6],
                        src[n + 7],
                    ]);
                    v.push(sst_id);
                }
                ManifestEntry::Compaction(v)
            }
            3 => {
                if src.len() < MANIFEST_ENTRY_TAG_SZ + SZ_U64 {
                    trace!(
                        "not enough bytes to decode ManifestEntry::StreamUpdate{{stream_id: _, labelmap: _ }}'s stream_id"
                    );
                    return Err(CodecError::NotEnoughBytes(crate::codec::NotEnoughBytes {
                        got: src.len(),
                        want: MANIFEST_ENTRY_TAG_SZ + SZ_U64,
                    }));
                }

                let stream_id = u64::from_be_bytes([
                    src[1], src[2], src[3], src[4], src[5], src[6], src[7], src[8],
                ]);

                if let Some((labelmap, _)) = self
                    .label_codec
                    .decode(&src[MANIFEST_ENTRY_TAG_SZ + SZ_U64..])?
                {
                    // decode() already parses only valid labelmaps
                    ManifestEntry::StreamUpdate {
                        stream_id,
                        labelmap: Arc::new(labelmap),
                    }
                } else {
                    return Ok(None);
                }
            }
            n => {
                return Err(CodecError::Other(format!(
                    "unknown ManifestEntry variant {n}"
                )));
            }
        };

        let wrote = me.wire_len();
        Ok(Some((me, wrote)))
    }
}

#[cfg(test)]
mod test {

    use std::collections::BTreeMap;

    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::*;

    fn tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .try_init();
    }

    #[test]
    fn lifecycle() {
        tracing();

        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let d = TempDir::new().unwrap();
        let codec = ManifestCodec {
            label_codec: LabelMapCodec,
        };
        let mut m = Manifest::open(d.path(), codec, None).unwrap();

        for i in 0..100u64 {
            let op = match i % 3 {
                0 => ManifestEntry::SstFlush(i),
                1 => ManifestEntry::Compaction(vec![i]),
                2 => {
                    let mut map = BTreeMap::new();
                    map.insert("foo".into(), "barbar".into());
                    map.insert("bar".into(), "bazbaz".into());
                    map.insert("baz".into(), "foofoo".into());
                    let labelmap = Arc::new(LabelMap::new(map));
                    ManifestEntry::StreamUpdate {
                        stream_id: 67,
                        labelmap,
                    }
                }
                _ => unreachable!(),
            };
            m.append(&op).unwrap();
        }
        m.sync().unwrap();
    }

    #[test]
    fn recover() {
        tracing();

        let d = TempDir::new().expect("tempfile");
        let codec = ManifestCodec {
            label_codec: LabelMapCodec,
        };
        let mut m = Manifest::open(d.path(), codec, None).unwrap();

        let record_num = 100u64;
        let written: Vec<ManifestEntry> = (0..record_num)
            .map(|i| match i % 3 {
                0 => ManifestEntry::SstFlush(i),
                1 => ManifestEntry::Compaction(vec![i]),
                2 => {
                    let mut map = BTreeMap::new();
                    map.insert("foo".into(), "barbar".into());
                    map.insert("bar".into(), "bazbaz".into());
                    map.insert("baz".into(), "foofoo".into());
                    let labelmap = Arc::new(LabelMap::new(map));
                    ManifestEntry::StreamUpdate {
                        stream_id: 67,
                        labelmap,
                    }
                }
                _ => unreachable!(),
            })
            .collect();

        for rec in &written {
            m.append(rec).expect("wal append failed");
        }
        m.sync().unwrap();
        // dbg!(&written);

        let recovered: Vec<ManifestEntry> = Manifest::new_reader(d.path(), codec)
            .unwrap()
            .map(|res| {
                let op = res.expect("recover: failed to decode op");
                // dbg!(&op);
                // assert_eq!(i as u32, op.as_u32());
                op
            })
            .collect();

        // dbg!(&recovered);

        assert_eq!(
            record_num as usize,
            recovered.len(),
            "expected `record_num` amount of recovered ops"
        );
        assert_eq!(
            written, recovered,
            "recovered records must match written ops"
        );
    }

    /// Pins down the shared-fd offset bug: recovering must not disturb the
    /// writer's position. Append, recover, append again, recover again —
    /// the second recovery should see all records from both rounds, in
    /// order, not have the first round overwritten.
    #[test]
    fn wal_append_after_recovery_does_not_corrupt_prior_records() {
        tracing();

        let d = TempDir::new().unwrap();
        let codec = ManifestCodec {
            label_codec: LabelMapCodec,
        };
        let mut m = Manifest::open(d.path(), codec, None).unwrap();

        let first_batch: Vec<ManifestEntry> = (0..10u64)
            .map(|i| match i % 3 {
                0 => ManifestEntry::sst_flush(i),
                1 => ManifestEntry::compaction(vec![i]).unwrap(),
                2 => {
                    let mut map = BTreeMap::new();
                    map.insert("foo".into(), "barbar".into());
                    map.insert("bar".into(), "bazbaz".into());
                    map.insert("baz".into(), "foofoo".into());
                    let labelmap = Arc::new(LabelMap::new(map));
                    ManifestEntry::stream_update(67, labelmap).unwrap()
                }
                _ => unreachable!(),
            })
            .collect();
        for rec in &first_batch {
            m.append(rec).expect("append failed");
        }
        m.sync().expect("flush failed");

        // Recover once, this seeks a shared fd back to 0 in the current
        // implementation, which is exactly the bug this test targets.
        let _ = Manifest::new_reader(d.path(), codec)
            .expect("recovery failed")
            .collect::<Vec<_>>();

        let second_batch: Vec<ManifestEntry> = (10..20u64)
            .map(|i| match i % 3 {
                0 => ManifestEntry::sst_flush(i),
                1 => ManifestEntry::compaction(vec![i]).unwrap(),
                2 => {
                    let mut map = BTreeMap::new();
                    map.insert("foo".into(), "barbar".into());
                    map.insert("bar".into(), "bazbaz".into());
                    map.insert("baz".into(), "foofoo".into());
                    let labelmap = Arc::new(LabelMap::new(map));
                    ManifestEntry::stream_update(67, labelmap).unwrap()
                }
                _ => unreachable!(),
            })
            .collect();

        for rec in &second_batch {
            m.append(rec).expect("append failed");
        }
        m.sync().expect("sync failed");

        let recovered: Vec<ManifestEntry> = Manifest::new_reader(d.path(), codec)
            .expect("recovery failed")
            .map(|r| r.expect("decode failed"))
            .collect();

        let expected: Vec<ManifestEntry> = first_batch.into_iter().chain(second_batch).collect();
        assert_eq!(
            expected, recovered,
            "appending after a recovery pass must not corrupt or overwrite prior ops"
        );
    }

    #[test]
    fn snapshot() {
        tracing();

        let d = TempDir::new().unwrap();
        let codec = ManifestCodec {
            label_codec: LabelMapCodec,
        };
        let mut m = Manifest::open(d.path(), codec, Some(100)).unwrap();

        let first_batch: Vec<ManifestEntry> = (0..10u64)
            .map(|i| match i % 3 {
                0 => ManifestEntry::sst_flush(i),
                1 => ManifestEntry::compaction(vec![i, i + 1, i + 2]).unwrap(),
                2 => {
                    let mut map = BTreeMap::new();
                    map.insert("foo".into(), format!("{i}").into());
                    map.insert("bar".into(), format!("{i}").into());
                    map.insert("baz".into(), format!("{i}").into());
                    let labelmap = Arc::new(LabelMap::new(map));
                    ManifestEntry::stream_update(i, labelmap).unwrap()
                }
                _ => unreachable!(),
            })
            .collect();
        let mut needs_snapshot = false;
        for rec in &first_batch {
            needs_snapshot = m.append(rec).expect("append failed");
        }
        m.sync().expect("flush failed");
        assert!(needs_snapshot, "manifest shouldve filled up");
        m.snapshot().expect("manifest::snapshot");
        let s1 = Snapshot::open_for_test(d.path()).expect("Snapshot::open_for_test");
        // dbg!(&s1);
        assert_eq!(
            m.f.metadata().expect("fs::metadata").len(),
            0,
            "manifest shouldve been truncated"
        );

        let second_batch: Vec<ManifestEntry> = (10..20u64)
            .map(|i| match i % 3 {
                0 => ManifestEntry::sst_flush(i),
                1 => ManifestEntry::compaction(vec![i, i + 1, i + 2]).unwrap(),
                2 => {
                    let mut map = BTreeMap::new();
                    map.insert("foo".into(), format!("{}", i + 1).into());
                    map.insert("bar".into(), format!("{}", i + 1).into());
                    map.insert("baz".into(), format!("{}", i + 1).into());
                    let labelmap = Arc::new(LabelMap::new(map));
                    ManifestEntry::stream_update(i, labelmap).unwrap()
                }
                _ => unreachable!(),
            })
            .collect();

        for rec in &second_batch {
            needs_snapshot = m.append(rec).expect("append failed");
        }
        m.sync().expect("flush failed");
        assert!(needs_snapshot, "manifest shouldve filled up");
        m.snapshot().expect("manifest::snapshot");
        let s2 = Snapshot::open_for_test(d.path()).expect("Snapshot::open_for_test");
        // dbg!(&s2);

        pretty_assertions::assert_ne!(s1, s2);
        assert!(
            s1.sstable_state
                .iter()
                .filter(|(_, st)| matches!(st, SstState::Live))
                .count()
                < s2.sstable_state.len()
        );
        assert!(s1.labelmaps.len() < s2.labelmaps.len());

        // ensure manifest is still functioning after merging snapshot
        let final_batch: Vec<ManifestEntry> = (20..30u64)
            .map(|i| match i % 3 {
                0 => ManifestEntry::sst_flush(i),
                1 => ManifestEntry::compaction(vec![i, i + 1, i + 2]).unwrap(),
                2 => {
                    let mut map = BTreeMap::new();
                    map.insert("foo".into(), format!("{}", i + 1).into());
                    map.insert("bar".into(), format!("{}", i + 1).into());
                    map.insert("baz".into(), format!("{}", i + 1).into());
                    let labelmap = Arc::new(LabelMap::new(map));
                    ManifestEntry::stream_update(i, labelmap).unwrap()
                }
                _ => unreachable!(),
            })
            .collect();

        for rec in &final_batch {
            needs_snapshot = m.append(rec).expect("append failed");
        }
        m.sync().expect("flush failed");
        assert!(needs_snapshot, "manifest shouldve filled up");
        m.snapshot().expect("manifest::snapshot");
        let s2 = Snapshot::open_for_test(d.path()).expect("Snapshot::open_for_test");
        // dbg!(&s2);
        pretty_assertions::assert_ne!(s1, s2);
    }
}
