use std::fs::OpenOptions;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::{fs::File, io};

use tracing::instrument;

use crate::{
    codec::{CodecError, FramedReader, FramedWriter, SpecCodec},
    record::Record,
};

pub const WAL_FILE_NAME: &str = "WAL";

#[derive(Debug)]
pub struct Wal<C: SpecCodec<Record>> {
    basepath: PathBuf,
    framed: FramedWriter<File, C, Record>,
    f: File,
    codec: C,
}

pub fn format_wal_filename(basepath: &Path) -> PathBuf {
    basepath.join(WAL_FILE_NAME)
}

impl<C> Wal<C>
where
    C: SpecCodec<Record>,
{
    #[instrument(ret, err)]
    pub fn open_latest(basepath: &Path, codec: C) -> crate::Result<Self> {
        // let wal_ext = OsString::from(WAL_FILE_EXT);
        // let mut active_wal: Option<(PathBuf, u64)> = None;

        // for e in std::fs::read_dir(basepath)? {
        //     let e = e?;
        //     let meta = e.metadata()?;
        //     let path = e.path();
        //     if meta.is_dir() {
        //         trace!("{} is not a file, skipping", path.display());
        //         continue;
        //     }

        //     let mut comps = path.components();

        //     if let Some(last_ext) = comps.next_back()
        //         && last_ext.as_os_str() != wal_ext
        //     {
        //         trace!(
        //             "{} last file extension is not {}, skipping",
        //             path.display(),
        //             WAL_FILE_EXT
        //         );
        //         continue;
        //     }

        //     if let Some(second_last_ext) = comps.next_back()
        //         && second_last_ext.as_os_str() == frozen_ext
        //     {
        //         trace!("{} wal file is frozen, skipping", path.display(),);
        //         continue;
        //     }

        //     match active_wal {
        //         Some((w, _)) => {
        //             let e = format!(
        //                 "{} is another active WAL, while there was a previous one at {}",
        //                 path.display(),
        //                 w.display()
        //             );
        //             error!(e);
        //             return Err(e.as_str().into());
        //         }
        //         None => {
        //             if let Some(raw_id) = comps.next_back()
        //                 && let Some(str_id) = raw_id.as_os_str().to_str()
        //                 && let Ok(id) = str_id.parse()
        //             {
        //                 active_wal = Some((path, id));
        //             }
        //         }
        //     }
        // }

        let f = open_read_append(format_wal_filename(basepath))?;

        let fw = FramedWriter::new(f.try_clone()?, codec.clone());
        let w = Self {
            basepath: basepath.to_path_buf(),
            framed: fw,
            f,
            codec,
        };

        Ok(w)
    }

    pub fn new(basepath: &Path, codec: C) -> io::Result<Self> {
        let f = open_read_append(format_wal_filename(basepath))?;
        let framed = FramedWriter::new(f.try_clone()?, codec.clone());
        Ok(Self {
            basepath: basepath.to_path_buf(),
            framed,
            f,
            codec,
        })
    }

    #[instrument(level = "trace", skip(self), ret, err)]
    pub fn append(&mut self, rec: &Record) -> Result<(), CodecError> {
        self.framed.write(rec)?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub fn flush_inner(&mut self) -> io::Result<()> {
        self.framed.flush()?;
        Ok(())
    }

    #[instrument(ret, err)]
    pub fn fsyncdata(&mut self) -> io::Result<()> {
        self.framed.flush()?;
        self.f.sync_data()?;
        Ok(())
    }

    // // Freezes the WAL and swaps it with a new once in-place with the given id.
    // // Notably it does not flush the contents of the WAL, as that would introduce unnecessary syscalls and disk-IO to the write path harming performance
    // #[deprecated]
    // pub fn freeze_no_flush(&mut self, new_id: u64) -> Result<Self, CodecError> {
    //     // self.framed.flush()?;
    //     // self.f.sync_data()?;
    //     let old_name = self.basepath.join(format_wal_filename(self.id));
    //     let frozen_name =
    //         self.basepath.join(format_frozen_wal_filename(self.id));
    //     std::fs::rename(old_name, frozen_name)?;

    //     let new = Self::new(new_id, &self.basepath, self.codec.clone())?;
    //     Ok(std::mem::replace(self, new))
    // }

    pub fn get_file_name(&self) -> PathBuf {
        format_wal_filename(&self.basepath)
    }

    pub fn as_reader(&self) -> io::Result<FramedReader<File, C, Record>> {
        let mut f = self.f.try_clone()?;
        f.seek(io::SeekFrom::Start(0))?;
        Ok(FramedReader::new(f, self.codec.clone()))
    }

    #[cfg(test)]
    fn test_recover(
        path: &Path,
        codec: C,
    ) -> io::Result<FramedReader<File, C, Record>> {
        let f = open_read_append(path)?;
        Ok(FramedReader::new(f, codec))
    }
}

fn open_read_append(path: impl AsRef<Path>) -> io::Result<File> {
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
        let mut w = Wal::new(d.path(), RecordCodec).expect("wal::new");

        for i in 0..100u64 {
            let rec = Record {
                key: Key::new(1, 2, i, 4),
                payload: PAYLOAD.into(),
            };
            w.append(&rec).expect("wal append failed");
        }
        w.flush_inner().expect("flush failed");
    }

    #[test]
    fn wal_recover() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let codec = RecordCodec;
        let d = TempDir::new().expect("tempdir");
        let mut w = Wal::new(d.path(), codec).expect("wal::new");

        let record_num = 100u64;
        let written: Vec<Record> = (0..record_num)
            .map(|seq| Record {
                key: Key::new(1, 2, seq, 67),
                payload: PAYLOAD.into(),
            })
            .collect();

        for rec in &written {
            w.append(rec).expect("wal append failed");
        }
        w.flush_inner().expect("fsync failed");
        let wal_path = format_wal_filename(&w.basepath);

        let recovered: Vec<Record> = Wal::test_recover(&wal_path, codec)
            .expect("failed to open wal for recovery")
            .enumerate()
            .map(|(i, res)| {
                let rec = res.expect("recover: failed to decode record");
                assert_eq!(
                    i as u64, rec.key.sequence_num,
                    "recover: sequence_num mismatch"
                );
                assert_eq!(
                    67, rec.key.stream_id,
                    "recover: stream_id mismatch"
                );
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
        let mut w = Wal::new(d.path(), codec).expect("wal::new");

        let first_batch: Vec<Record> = (0..10u64)
            .map(|seq| Record {
                key: Key::new(1, 1, seq, 1),
                payload: PAYLOAD.into(),
            })
            .collect();
        for rec in &first_batch {
            w.append(rec).expect("append failed");
        }
        w.flush_inner().expect("flush failed");

        let wal_file_path = format_wal_filename(&w.basepath);
        // Recover once: this seeks a shared fd back to 0 in the current
        // implementation, which is exactly the bug this test targets.
        let _ = Wal::test_recover(&wal_file_path, codec)
            .expect("recovery failed")
            .collect::<Vec<_>>();

        let second_batch: Vec<Record> = (10..20u64)
            .map(|seq| Record {
                key: Key::new(1, 1, seq, 1),
                payload: PAYLOAD.into(),
            })
            .collect();
        for rec in &second_batch {
            w.append(rec).expect("append failed");
        }
        w.flush_inner().expect("flush failed");

        let recovered: Vec<Record> = Wal::test_recover(&wal_file_path, codec)
            .expect("recovery failed")
            .map(|r| r.expect("decode failed"))
            .collect();

        let expected: Vec<Record> =
            first_batch.into_iter().chain(second_batch).collect();
        assert_eq!(
            expected, recovered,
            "appending after a recovery pass must not corrupt or overwrite prior records"
        );
    }
}
