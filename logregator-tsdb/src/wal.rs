use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::{fs::File, io};

use tracing::instrument;

use crate::codec::{CodecError, FramedReader, FramedWriter, SpecCodec};
use crate::record::Record;

pub struct Wal<C: SpecCodec<Record>> {
    path: PathBuf,
    framed: FramedWriter<File, C, Record>,
    f: File,
}

impl<C> Wal<C>
where
    C: SpecCodec<Record>,
{
    pub fn new(path: &Path, codec: C) -> io::Result<Self> {
        let f = open_read_append(path)?;
        let path = path.to_path_buf();
        let framed = FramedWriter::new(f.try_clone()?, codec.clone());
        Ok(Self { path, framed, f })
    }

    #[instrument(level = "trace", skip(self))]
    pub fn append(&mut self, rec: &Record) -> Result<(), CodecError> {
        self.framed.write(rec)?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub fn flush(&mut self) -> io::Result<()> {
        self.framed.flush()?;
        Ok(())
    }

    fn try_recover(path: &Path, codec: C) -> io::Result<FramedReader<File, C, Record>> {
        let f = open_read_append(path)?;
        Ok(FramedReader::new(f, codec))
    }
}

fn open_read_append(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).append(true).open(path)
}

#[cfg(test)]
mod test {

    use pretty_assertions::assert_eq;
    use tempfile::NamedTempFile;

    use crate::{
        codec::RecordCodec,
        record::{Key, Record},
        wal::Wal,
    };

    const PAYLOAD: &[u8] = b"du bist gut genug";

    #[test]
    fn wal_lifecycle() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let f = NamedTempFile::new().expect("tempfile");
        let mut w = Wal::new(f.path(), RecordCodec).expect("wal::new");

        for i in 0..100u64 {
            let rec = Record {
                key: Key::new(1, 2, i, 4),
                payload: PAYLOAD.to_vec().into_boxed_slice(),
            };
            w.append(&rec).expect("wal append failed");
        }
        w.flush().expect("flush failed");
    }

    #[test]
    fn wal_recover() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let codec = RecordCodec;
        let tempf = NamedTempFile::new().expect("tempfile");
        let mut w = Wal::new(tempf.path(), codec.clone()).expect("wal::new");

        let record_num = 100u64;
        let written: Vec<Record> = (0..record_num)
            .map(|seq| Record {
                key: Key::new(1, 2, seq, 67),
                payload: PAYLOAD.to_vec().into_boxed_slice(),
            })
            .collect();

        for rec in &written {
            w.append(rec).expect("wal append failed");
        }
        w.flush().expect("flush failed");

        let recovered: Vec<Record> = Wal::try_recover(tempf.path(), codec.clone())
            .expect("failed to open wal for recovery")
            .enumerate()
            .map(|(i, res)| {
                let rec = res.expect("recover: failed to decode record");
                assert_eq!(
                    i as u64, rec.key.sequence_num,
                    "recover: sequence_num mismatch"
                );
                assert_eq!(67, rec.key.stream_id, "recover: stream_id mismatch");
                assert_eq!(PAYLOAD, &*rec.payload, "recover: payload mismatch");
                rec
            })
            .collect();

        assert_eq!(
            record_num as usize,
            recovered.len(),
            "expected `record_num` amount of recovered records"
        );
        assert_eq!(
            written, recovered,
            "recovered records must match written records"
        );
    }

    /// Pins down the shared-fd offset bug: recovering must not disturb the
    /// writer's position. Append, recover, append again, recover again —
    /// the second recovery should see all records from both rounds, in
    /// order, not have the first round overwritten.
    #[test]
    fn wal_append_after_recovery_does_not_corrupt_prior_records() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let codec = RecordCodec;
        let f = NamedTempFile::new().expect("tempfile");
        let mut w = Wal::new(f.path(), codec.clone()).expect("wal::new");

        let first_batch: Vec<Record> = (0..10u64)
            .map(|seq| Record {
                key: Key::new(1, 1, seq, 1),
                payload: PAYLOAD.to_vec().into_boxed_slice(),
            })
            .collect();
        for rec in &first_batch {
            w.append(rec).expect("append failed");
        }
        w.flush().expect("flush failed");

        // Recover once — this seeks a shared fd back to 0 in the current
        // implementation, which is exactly the bug this test targets.
        let _ = Wal::try_recover(f.path(), codec.clone())
            .expect("recovery failed")
            .collect::<Vec<_>>();

        let second_batch: Vec<Record> = (10..20u64)
            .map(|seq| Record {
                key: Key::new(1, 1, seq, 1),
                payload: PAYLOAD.to_vec().into_boxed_slice(),
            })
            .collect();
        for rec in &second_batch {
            w.append(rec).expect("append failed");
        }
        w.flush().expect("flush failed");

        let recovered: Vec<Record> = Wal::try_recover(f.path(), codec.clone())
            .expect("recovery failed")
            .map(|r| r.expect("decode failed"))
            .collect();

        let expected: Vec<Record> = first_batch.into_iter().chain(second_batch).collect();
        assert_eq!(
            expected, recovered,
            "appending after a recovery pass must not corrupt or overwrite prior records"
        );
    }
}
