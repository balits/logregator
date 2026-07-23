use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use tracing::{instrument, trace};

use crate::codec::{self, CodecError, FramedReader, FramedWriter, SpecCodec, WireLen};

/// TODO: should the methods return io::Error or CodecError (which an Io(io::Error) variant)
pub struct Manifest {
    pathbuf: PathBuf,
    framed: FramedWriter<File, ManifestCodec, ManifestOp>,
    f: File,
}

impl Manifest {
    pub fn new(path: &std::path::Path) -> std::io::Result<Self> {
        let f = std::fs::File::options()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;

        let framed = FramedWriter::new(f.try_clone()?, ManifestCodec);

        Ok(Self {
            pathbuf: path.into(),
            framed,
            f,
        })
    }

    pub fn append(&mut self, op: &ManifestOp) -> Result<(), CodecError> {
        self.framed.write(op)
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.framed.flush()?;
        self.f.sync_data()?;
        Ok(())
    }
    fn try_recover(
        p: impl AsRef<Path>,
    ) -> io::Result<FramedReader<File, ManifestCodec, ManifestOp>> {
        let f = OpenOptions::new().read(true).append(true).open(p)?;
        Ok(FramedReader::new(f, ManifestCodec))
    }
}

const MANIFEST_OP_ID_SIZE: usize = size_of::<u32>();
const MANIFEST_OP_WIRE_LEN: usize = 1usize + MANIFEST_OP_ID_SIZE;

/// [0 | 1 | ... Op enum tag: u8] [id_of_op_object: u32] => 40 byte each
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestOp {
    MemtableFlush(u32),
    Compaction(u32),
}

impl WireLen for ManifestOp {
    #[inline]
    fn wire_len(&self) -> usize {
        MANIFEST_OP_WIRE_LEN
    }
}

impl ManifestOp {
    pub fn tag(&self) -> u8 {
        match self {
            Self::MemtableFlush(_) => 1,
            Self::Compaction(_) => 2,
        }
    }

    pub fn id(&self) -> u32 {
        match self {
            Self::MemtableFlush(i) => *i,
            Self::Compaction(i) => *i,
        }
    }

    pub fn try_from_parts(tag: u8, id: u32) -> Option<Self> {
        let op = match tag {
            1 => Self::MemtableFlush(id),
            2 => Self::Compaction(id),
            _ => return None,
        };
        Some(op)
    }
}

#[derive(Debug, Clone)]
pub struct ManifestCodec;

impl SpecCodec<ManifestOp> for ManifestCodec {
    #[instrument(err)]
    fn encode(&self, op: &ManifestOp, dst: &mut [u8]) -> Result<usize, codec::CodecError> {
        if dst.len() < op.wire_len() {
            trace!(
                "not enough bytes to encode {op:?} into `dst` (got: {}, want: {})",
                dst.len(),
                op.wire_len()
            );

            return Err(crate::codec::not_enough_bytes(dst.len(), op.wire_len()));
        }
        dst[0] = op.tag();
        dst[1..1 + MANIFEST_OP_ID_SIZE].copy_from_slice(&op.id().to_be_bytes());
        Ok(op.wire_len())
    }

    // #[instrument(err)]
    fn decode(&self, src: &[u8]) -> Result<Option<(ManifestOp, usize)>, codec::CodecError> {
        if src.len() < MANIFEST_OP_WIRE_LEN {
            trace!(
                "not enough bytes to decode from (got: {}, want: {})",
                src.len(),
                MANIFEST_OP_WIRE_LEN
            );

            return Ok(None);
        }

        let tag = src[0];
        let id = u32::from_be_bytes(
            src[1..1 + MANIFEST_OP_ID_SIZE]
                .try_into()
                .map_err(io::Error::other)?,
        );
        let op = ManifestOp::try_from_parts(tag, id).ok_or(io::Error::other(
            "failed to convert (tag: u8, id: u32) to Operation",
        ))?;

        Ok(Some((op, op.wire_len())))
    }
}

#[cfg(test)]
mod test {

    use pretty_assertions::assert_eq;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::codec::DefaultCodec;

    const PAYLOAD: &[u8] = b"du bist gut genug";

    #[test]
    fn lifecycle() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let f = NamedTempFile::new().unwrap();
        let mut m = Manifest::new(f.path()).unwrap();

        for i in 0..100u32 {
            let op = match i % 2 {
                0 => ManifestOp::MemtableFlush(i),
                1 => ManifestOp::Compaction(i),
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
        let mut m = Manifest::new(tempf.path()).unwrap();

        let record_num = 100u32;
        let written: Vec<ManifestOp> = (0..record_num)
            .map(|i| match i % 2 {
                0 => ManifestOp::MemtableFlush(i),
                1 => ManifestOp::Compaction(i),
                _ => unreachable!(),
            })
            .collect();

        for rec in &written {
            m.append(rec).expect("wal append failed");
        }
        m.sync().unwrap();
        // dbg!(&written);

        let recovered: Vec<ManifestOp> = Manifest::try_recover(tempf.path())
            .unwrap()
            .enumerate()
            .map(|(i, res)| {
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

        let codec = DefaultCodec;
        let f = NamedTempFile::new().unwrap();
        let mut w = Manifest::new(f.path()).unwrap();

        let first_batch: Vec<ManifestOp> = (0..10u32)
            .map(|i| match i % 2 {
                0 => ManifestOp::MemtableFlush(i),
                1 => ManifestOp::Compaction(i),
                _ => unreachable!(),
            })
            .collect();
        for rec in &first_batch {
            w.append(rec).expect("append failed");
        }
        w.sync().expect("flush failed");

        // Recover once, this seeks a shared fd back to 0 in the current
        // implementation, which is exactly the bug this test targets.
        let _ = Manifest::try_recover(f.path())
            .expect("recovery failed")
            .collect::<Vec<_>>();

        let second_batch: Vec<ManifestOp> = (10..20u32)
            .map(|i| match i % 2 {
                0 => ManifestOp::MemtableFlush(i),
                1 => ManifestOp::Compaction(i),
                _ => unreachable!(),
            })
            .collect();

        for rec in &second_batch {
            w.append(rec).expect("append failed");
        }
        w.sync().expect("sync failed");

        let recovered: Vec<ManifestOp> = Manifest::try_recover(f.path())
            .expect("recovery failed")
            .map(|r| r.expect("decode failed"))
            .collect();

        let expected: Vec<ManifestOp> = first_batch.into_iter().chain(second_batch).collect();
        assert_eq!(
            expected, recovered,
            "appending after a recovery pass must not corrupt or overwrite prior ops"
        );
    }
}
