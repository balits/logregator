use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use tracing::{instrument, trace};

use crate::{
    codec::{self, CodecError, FramedReader, FramedWriter, SZ_U64, SpecCodec, WireLen},
    label::{self, LabelMap, LabelMapCodec},
};

/// TODO: should the methods return io::Error or CodecError (which an Io(io::Error) variant)
pub struct Manifest {
    _pathbuf: PathBuf,
    framed: FramedWriter<File, ManifestCodec, ManifestEntry>,
    f: File,
}

impl Manifest {
    pub fn new(path: &std::path::Path, codec: ManifestCodec) -> std::io::Result<Self> {
        let f = std::fs::File::options()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;

        let framed = FramedWriter::new(f.try_clone()?, codec);

        Ok(Self {
            _pathbuf: path.into(),
            framed,
            f,
        })
    }

    pub fn append(&mut self, op: &ManifestEntry) -> Result<(), CodecError> {
        self.framed.write(op)
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.framed.flush()?;
        self.f.sync_data()?;
        Ok(())
    }

    pub fn try_recover(
        p: impl AsRef<Path>,
        codec: ManifestCodec,
    ) -> io::Result<FramedReader<File, ManifestCodec, ManifestEntry>> {
        let f = OpenOptions::new().read(true).append(true).open(p)?;
        Ok(FramedReader::new(f, codec))
    }
}

const MANIFEST_ENTRY_TAG_SZ: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestEntry {
    SstFlush(u64),
    Compaction(u64),
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
            Self::SstFlush(_) | Self::Compaction(_) => {
                sz += SZ_U64;
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

    pub fn compactoin(id: u64) -> Self {
        Self::Compaction(id)
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
            Self::Compaction(_) => "ManifestEntry::SstFlush",
            Self::StreamUpdate { .. } => "ManifestEntry::StreamUpdate",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ManifestCodec {
    label_codec: LabelMapCodec,
}

impl SpecCodec<ManifestEntry> for ManifestCodec {
    #[instrument(err)]
    fn encode(&self, item: &ManifestEntry, dst: &mut [u8]) -> Result<usize, codec::CodecError> {
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

        match item {
            ManifestEntry::SstFlush(id) => {
                dst[1..1 + SZ_U64].copy_from_slice(&(*id).to_be_bytes());
            }
            ManifestEntry::Compaction(id) => {
                dst[1..1 + SZ_U64].copy_from_slice(&(*id).to_be_bytes());
            }
            ManifestEntry::StreamUpdate {
                stream_id,
                labelmap,
            } => {
                dst[1..1 + SZ_U64].copy_from_slice(&(*stream_id).to_be_bytes());
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

    // #[instrument(err)]
    fn decode(&self, src: &[u8]) -> Result<Option<(ManifestEntry, usize)>, codec::CodecError> {
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
            n @ 1..=2 => {
                if src.len() < MANIFEST_ENTRY_TAG_SZ + SZ_U64 {
                    trace!(
                        "not enough bytes to decode from (got: {}, want: {})",
                        src.len(),
                        MANIFEST_ENTRY_TAG_SZ + SZ_U64
                    );
                }

                let id = u64::from_be_bytes([
                    src[1], src[2], src[3], src[4], src[5], src[6], src[7], src[8],
                ]);
                match n {
                    1 => ManifestEntry::SstFlush(id),
                    2 => ManifestEntry::Compaction(id),
                    _n => unreachable!("match: n was fixed to 1..=2, but got the value {_n}"),
                }
            }
            3 => {
                if src.len() < MANIFEST_ENTRY_TAG_SZ + SZ_U64 {
                    trace!(
                        "not enough bytes to decode from (got: {}, want: {})",
                        src.len(),
                        MANIFEST_ENTRY_TAG_SZ + SZ_U64
                    );
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
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn lifecycle() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let f = NamedTempFile::new().unwrap();
        let codec = ManifestCodec {
            label_codec: LabelMapCodec,
        };
        let mut m = Manifest::new(f.path(), codec).unwrap();

        for i in 0..100u64 {
            let op = match i % 3 {
                0 => ManifestEntry::SstFlush(i),
                1 => ManifestEntry::Compaction(i),
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
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            // .with_target(true)
            // .with_span_events(FmtSpan::NEW)
            .with_test_writer()
            .try_init();

        let tempf = NamedTempFile::new().expect("tempfile");
        let codec = ManifestCodec {
            label_codec: LabelMapCodec,
        };
        let mut m = Manifest::new(tempf.path(), codec.clone()).unwrap();

        let record_num = 100u64;
        let written: Vec<ManifestEntry> = (0..record_num)
            .map(|i| match i % 3 {
                0 => ManifestEntry::SstFlush(i),
                1 => ManifestEntry::Compaction(i),
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

        let recovered: Vec<ManifestEntry> = Manifest::try_recover(tempf.path(), codec)
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
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let f = NamedTempFile::new().unwrap();
        let codec = ManifestCodec {
            label_codec: LabelMapCodec,
        };
        let mut w = Manifest::new(f.path(), codec.clone()).unwrap();

        let first_batch: Vec<ManifestEntry> = (0..10u64)
            .map(|i| match i % 3 {
                0 => ManifestEntry::SstFlush(i),
                1 => ManifestEntry::Compaction(i),
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
        for rec in &first_batch {
            w.append(rec).expect("append failed");
        }
        w.sync().expect("flush failed");

        // Recover once, this seeks a shared fd back to 0 in the current
        // implementation, which is exactly the bug this test targets.
        let _ = Manifest::try_recover(f.path(), codec.clone())
            .expect("recovery failed")
            .collect::<Vec<_>>();

        let second_batch: Vec<ManifestEntry> = (10..20u64)
            .map(|i| match i % 3 {
                0 => ManifestEntry::SstFlush(i),
                1 => ManifestEntry::Compaction(i),
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

        for rec in &second_batch {
            w.append(rec).expect("append failed");
        }
        w.sync().expect("sync failed");

        let recovered: Vec<ManifestEntry> = Manifest::try_recover(f.path(), codec)
            .expect("recovery failed")
            .map(|r| r.expect("decode failed"))
            .collect();

        let expected: Vec<ManifestEntry> = first_batch.into_iter().chain(second_batch).collect();
        assert_eq!(
            expected, recovered,
            "appending after a recovery pass must not corrupt or overwrite prior ops"
        );
    }
}
