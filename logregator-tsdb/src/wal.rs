use std::fs::OpenOptions;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::{fs::File, io};

use tracing::instrument;

use crate::codec;
use crate::{
    codec::{
        CodecError, FramedReader, FramedWriter, RecordCodecExt, SpecCodec,
    },
    record::Record,
};

pub const WAL_FILENAME_FORMAT: &str = "{:010}.active.wal";
pub const FROZEN_WAL_FILENAME_FORMAT: &str = "{:010}.frozen.wal";

#[derive(Debug)]
enum Crc32State {
    Checksum(u32),
    Hasher(crc32fast::Hasher),
}

impl Crc32State {
    // ???
    pub fn new_hasher() -> Self {
        Self::Hasher(crc32fast::Hasher::new())
    }
}

/// An append-only log file which corresponds to exactly one [Memtable].
#[derive(Debug)]
pub struct Wal<C: RecordCodecExt> {
    /// The WAL data directory under which all active and frozen and WALs go.
    basepath: PathBuf,

    /// Framed writer of records into the WAL
    /// TODO: add a small wrapper `enum WalEntry { Record(Record), Checksum(..) }`
    framed: FramedWriter<File, C, Record>,

    /// File handle to the WAL
    f: File,

    /// ID which comes from the [Memtable] that produced
    /// this WAL to be flushed. One [Memtable] always corresponds to one WAL
    ///
    /// - [Memtable](crate::memtable::MemtableInner)
    id: u64,

    /// codec that works on both [Record], [Key] and [WalEntry]
    codec: C,

    /// The crc32 hasher state, either being an actual Hasher, or the finished
    /// checksum. Reading the checksum from the [Wal] file is as easy as
    /// reading the last 4 bytes
    crc32_state: Crc32State,
    // /// Optional hasher used to calculate the integrety of this Wal,
    // /// through the [FramedWriter]'s `on_write` hook.
    // ///
    // /// # Invariant
    // ///
    // /// At any given time, only one of `running_checksum` and `finished_checksum`
    // /// is set to `Some(_)`. The lifecyle of an acitve wal is that it starts with
    // /// a `Some(_)` hasher, then once its filled up, hashers is finalized,
    // /// and the result is set to `finished_checksum`, leaving `None` behind in the
    // /// hasher.
    // ///
    // ///
    // /// - [FramedWriter](crate::codec::FramedWriter)
    // checksum_hasher: Option<crc32fast::Hasher>,

    // /// finished checksum calculated with `checksum_hasher`.
    // ///
    // finished_checksum: Option<u32>,
}

pub fn format_wal_filename(basepath: &Path) -> PathBuf {
    basepath.join(WAL_FILENAME_FORMAT)
}

impl<C: RecordCodecExt> Wal<C> {
    #[instrument(ret, err)]
    pub fn open_active(basepath: &Path, codec: C) -> crate::Result<Self> {
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

        todo!(
            "uncomment the previous lines and use the following strategy: \
            use the `.active.wal` and `.frozen.wal` extensions to find
            the only active wal (error on multiple active wals), then
            the following code should be fine, and the ID could be parsed from
            the active filename.
        "
        );

        let f = open_read_append(format_wal_filename(basepath))?;
        let id: u64 = todo!("parse active wal filename for ID");
        let integrity: Crc32State =
            todo!("replay records to rebuild the crc32 hashers state");

        let fw = FramedWriter::new(f.try_clone()?, codec.clone());
        let w = Self {
            basepath: basepath.to_path_buf(),
            framed: fw,
            f,
            id,
            crc32_state,
            codec,
        };

        Ok(w)
    }

    #[deprecated(note = "use open_empty() or open_active()")]
    pub fn new(basepath: &Path, codec: C) -> io::Result<Self> {
        panic!(
            "new() is deprecated, as it has no idea about a memtable_id parameter"
        );

        let f = open_read_append(format_wal_filename(basepath))?;
        let framed = FramedWriter::new(f.try_clone()?, codec.clone());

        Ok(Self {
            basepath: basepath.to_path_buf(),
            framed,
            f,
            id: 0, // already panics, doesnt matter
            crc32_state: Crc32State::Checksum(0), // already panics, doesnt matter
            codec,
        })
    }

    /// Opens a new empty but active wal file from the given `memtable_id`.
    pub fn open_empty(
        basepath: &Path,
        memtable_id: u64,
        codec: C,
    ) -> io::Result<Self> {
        let f = open_read_append(format_wal_filename(basepath))?;

        let crc32_state = Crc32State::new_hasher();

        let framed = FramedWriter::new(f.try_clone()?, codec.clone());

        Ok(Self {
            basepath: basepath.to_path_buf(),
            framed,
            f,
            id: memtable_id,
            crc32_state,
            codec,
        })
    }

    #[instrument(level = "trace", skip(self), ret, err)]
    pub fn append(&mut self, rec: &Record) -> Result<(), CodecError> {
        self.framed.write_with(rec, |_, record_bytes| {
            if let Crc32State::Hasher(hasher) = &mut self.crc32_state {
                hasher.update(record_bytes);
            }
        })?;

        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub fn flush(&mut self) -> io::Result<()> {
        self.framed.flush()?;

        todo!(
            "move out of self.crc32_state somehow. \
                (1) if its a hasher, finalize it and replace it with a Crc32State::Checksum \
                (2) if its a checksum, just copy it back"
        );

        // let crc = &mut self.crc32_state;
        // if let Crc32State::Hasher(hasher) = crc {
        //     let checksum = hasher.finalize();
        //     std::mem::replace(crc, Crc32State::Checksum(checksum));
        // }

        Ok(())
    }

    #[instrument(ret, err)]
    pub fn fsyncdata(&mut self) -> io::Result<()> {
        self.flush()?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalEntry {
    Record(Record),
    Checksum(u32),
}

impl codec::WireLen for WalEntry {
    fn wire_len(&self) -> usize {
        1 + match self {
            WalEntry::Record(r) => r.wire_len(),
            WalEntry::Checksum(c) => size_of_val(c),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntryCodec<R: RecordCodecExt> {
    record_codec: R,
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
        w.flush().expect("flush failed");
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
        w.flush().expect("fsync failed");
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
        w.flush().expect("flush failed");

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
        w.flush().expect("flush failed");

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
