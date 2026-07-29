use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::{fs::File, io};

use tracing::instrument;

use crate::codec::{CodecError, FramedReader, FramedWriter, SpecCodec};
use crate::record::Record;

pub const WAL_FILE_EXT: &str = ".wal";

#[derive(Debug)]
pub struct Wal<C: SpecCodec<Record>> {
    id: u64,
    basepath: PathBuf,
    framed: FramedWriter<File, C, Record>,
    // _f: File,
    codec: C,
}

pub fn format_wal_filename(id: u64) -> String {
    format!("{id:020}{WAL_FILE_EXT}")
}

pub fn format_frozen_wal_filename(id: u64) -> String {
    format!("{id:020}.frozen{WAL_FILE_EXT}")
}

impl<C> Wal<C>
where
    C: SpecCodec<Record>,
{
    pub fn new(id: u64, basepath: &Path, codec: C) -> io::Result<Self> {
        let wal_path = basepath.join(format_wal_filename(id));
        let f = open_read_append(&wal_path)?;

        let framed = FramedWriter::new(f, codec.clone());
        Ok(Self {
            id,
            basepath: basepath.to_path_buf(),
            framed,
            codec,
        })
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

    pub fn freeze(&mut self, new_id: u64) -> Result<Self, CodecError> {
        let old_name = self.basepath.join(format_wal_filename(self.id));
        let frozen_name = self.basepath.join(format_frozen_wal_filename(self.id));
        std::fs::rename(old_name, frozen_name)?;

        let new = Self::new(new_id, &self.basepath, self.codec.clone())?;
        Ok(std::mem::replace(self, new))
    }

    fn _try_recover(path: &Path, codec: C) -> io::Result<FramedReader<File, C, Record>> {
        let f = open_read_append(path)?;
        Ok(FramedReader::new(f, codec))
    }
}

fn open_read_append(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
}

#[cfg(test)]
mod test {

    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use crate::{
        record::{Key, Record, RecordCodec},
        wal::{Wal, format_wal_filename},
    };

    const PAYLOAD: &[u8] = b"du bist gut genug";

    #[test]
    fn wal_lifecycle() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let d = TempDir::new().expect("tempdir");
        let mut w = Wal::new(0, d.path(), RecordCodec).expect("wal::new");

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
        let d = TempDir::new().expect("tempdir");
        let mut w = Wal::new(0, d.path(), codec).expect("wal::new");

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

        let recovered: Vec<Record> =
            Wal::_try_recover(&w.basepath.join(format_wal_filename(w.id)), codec)
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
        let d = TempDir::new().expect("tempdir");
        let mut w = Wal::new(0, d.path(), codec).expect("wal::new");

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

        let wal_file_path = d.path().join(format_wal_filename(w.id));
        // Recover once: this seeks a shared fd back to 0 in the current
        // implementation, which is exactly the bug this test targets.
        let _ = Wal::_try_recover(&wal_file_path, codec)
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

        let recovered: Vec<Record> = Wal::_try_recover(&wal_file_path, codec)
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
