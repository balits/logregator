//! - **TODO**: module docs
//!
//! - But even bigger **TODO** is file name formatting.
//!   I cant use constants for file extensions, then build the whole
//!   filename format string by combining in a const settingby using `format!` for example.
//!   But also `format!` cant take the format argument as a variable, only as a literal.
//!
//!
//! - slighty smaller but important **TODO** is tracking when we clone files and
//!   pass it around to [FramedWriter]s or [FramedReader]s, since with readers we
//!   need to seek to start, but since cloned [File](std::fs::File) share the
//!   same handle, seeking on the cloned file used by [FramedReader] also
//!   seeks the original file held by the Wal, making subsequent writes result
//!   in a corrupted state on disk.
//!
//! ```rust
//! # fn main() {
//! // => this works <=
//!
//! let foo_ext = "foo";
//! let bar_ext = "bar";
//! let id = 10;
//!
//! let formatted_path_good = format!("{:10}.{}.{}", id, foo_ext, bar_ext);
//!
//! // => this doesnt work <=
//! // regardless how we consturct the format_str
//!
//! let format_str = format!("{{:10}}.{}.{}", , foo_ext, bar_ext);
//!
//! let formatted_path_bad = format!(format_str, id, foo_ext, bar_ext);
//!
//! //! #}
//! ```

use std::fmt::Debug;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{error, instrument, span, trace, warn};

use crate::codec::{SpecCodec, WireLen};
use crate::positional_file::OffsetFile;
use crate::{
    codec::{self, FramedReader, FramedWriter, RecordCodecExt},
    record::Record,
};

/// File extension to be used for active WAL file names.
const ACTIVE_EXT: &str = "active";

/// File extension to be used for frozen the WAL file names.
const FROZEN_EXT: &str = "frozen";

/// The final file extension to be used for all WAL file names.
const WAL_EXT: &str = "wal";

// As far as I know there isnt yet a `const` way of creating
// a format &'static str which itself is also formatted to begin with; this
// of course could be done in a `non-const` setting
// with format!("{{<some_fmt_here>}}.{ACTIVE_EXT}.{WAL_EXT}")
//
// So until then i guess i just gotta be extra carefull when reworking these

/// Filename format for the active WAL file, which internally uses
/// the [WAL_EXT] extension in principle. In practive `format!` doesnt work
/// at compilation time...
#[deprecated(note = "cant use format! in const context anyways...")]
#[allow(unused)]
const ACTIVE_WAL_FILENAME_FMT: &str = "{:010}.active.wal";

/// Filename format for a frozen WAL file, which internally uses
/// the [FROZEN_EXT] and [WAL_EXT] extension in principle. In practive `format!`
/// doesnt work at compilation time...
#[deprecated(note = "cant use format! in const context anyways...")]
#[allow(unused)]
const FROZEN_WAL_FILENAME_FMT: &str = "{:010}.frozen.wal";

/// Returns the formatted wal file in the format of
/// `<basepath>/<id>.<active | frozen>.wal`
pub fn format_wal_filename(
    basepath: &Path,
    id: u64,
    is_active: bool,
) -> PathBuf {
    // OMG why cant i use dynamic format argumet in format! ://

    // anyways i think this is 1 clone i think,
    // compared to pathbuf.join(format!(...))

    let basepath_str = basepath.display();

    if is_active {
        format!("{basepath_str}/{:010}.{ACTIVE_EXT}.{WAL_EXT}", id).into()
    } else {
        format!("{basepath_str}/{:010}.{FROZEN_EXT}.{WAL_EXT}", id).into()
    }
}

fn opts_create_read_append() -> OpenOptions {
    let mut o = OpenOptions::new();
    o.create(true).read(true).append(true);
    o
}

fn opts_read() -> OpenOptions {
    let mut o = OpenOptions::new();
    o.read(true);
    o
}

/// An active, mutable Wal. Note that there can only be one active Wal at
/// one time. An active wal could be appended to, and finally could be
/// moved into a [FrozenWal] by [flush]-ing it to disk.
pub struct ActiveWal<R: RecordCodecExt> {
    inner: Inner<R>,
    fw: FramedWriter<File, WalEntryCodec<R>, WalEntry>,
}

impl<R: RecordCodecExt> Debug for ActiveWal<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveWal")
            .field("inner", &self.inner)
            .field("active", &true)
            .finish()
    }
}

impl<R: RecordCodecExt> ActiveWal<R> {
    #[instrument]
    pub fn create_active(
        basepath: &Path,
        next_id: u64,
        record_codec: R,
    ) -> crate::Result<Option<Self>> {
        let w = Inner::create_active(basepath, next_id, record_codec.clone())?;

        if let Some(active) = w {
            trace!(
                "wal: create active found an already existing active wal at {}",
                active.filename().display()
            );
            return Ok(Some(active));
        };

        trace!(
            "wal: create active found no acttive wals, creating a new one..."
        );

        let wal_path = format_wal_filename(basepath, next_id, true);

        let arcfile = Arc::new(opts_read().open(&wal_path)?);
        let mutfile = opts_create_read_append().open(&wal_path)?;

        // SAFETY
        //
        // new_empty doenst check relations between the wal paths parts and
        // the actual files path, but file was opened with `format_wal_filename()`
        let inner = unsafe {
            Inner::new_empty(basepath, next_id, arcfile, record_codec)
        };

        let fw = FramedWriter::new(mutfile, inner.codec.clone());

        Ok(Some(ActiveWal { inner, fw }))
    }

    #[instrument]
    pub fn open_active(
        basepath: &Path,
        record_codec: R,
    ) -> crate::Result<Option<Self>> {
        Inner::open_active(basepath, record_codec)
    }

    /// Appends a new record to the Wal
    #[instrument(skip(record))]
    pub fn append(&mut self, record: &Record) -> crate::Result<()> {
        self.fw.write_with(
            &WalEntry::Record(record.clone()),
            |_, record_bytes| {
                if let ChecksumState::Hasher(hasher) =
                    &mut self.inner.checksum_state
                {
                    hasher.update(record_bytes);
                }
            },
        )?;

        Ok(())
    }

    /// Flushes the Wal to disk and appends the finalized checksum,
    /// returning a [FrozenWal], if the Wal wasnt flushed already (indicated
    /// by the checksum state still not being `Finalized`).
    ///
    /// If by some secret scary way an active states inner state is altered,
    /// for example with unsafe raw ptr magic, then calling flush might not
    /// actually result in the file flushed and `fsyncdata`-d.
    pub fn flush(self) -> crate::Result<Option<FrozenWal<R>>> {
        let Self { mut inner, mut fw } = self;

        // # SAFETY
        //
        // ChecksumState::Hasher is a crc32fast::Hasher,
        // whose `finalize()` method does not panic
        let checksum = unsafe {
            let old_state = std::ptr::read(&inner.checksum_state as *const _);
            match old_state {
                ChecksumState::Hasher(hasher) => {
                    let checksum = hasher.finalize();
                    std::ptr::write(
                        &mut inner.checksum_state as *mut _,
                        ChecksumState::Finalized(checksum),
                    );
                    Some(checksum)
                }
                ChecksumState::Finalized(c) => {
                    warn!("Wal::flush: checksum already finalized: {c}");
                    None
                }
            }
        };

        match checksum {
            Some(checksum) => {
                fw.write(&WalEntry::Checksum(checksum))?;
                fw.flush()?;

                let old_active_name =
                    format_wal_filename(&inner.basepath, inner.id, true);
                let new_frozen_name =
                    format_wal_filename(&inner.basepath, inner.id, false);

                std::fs::rename(old_active_name, new_frozen_name)?;
                inner.file.sync_data()?;

                Ok(Some(FrozenWal { inner }))
            }
            None => Ok(None),
        }
    }

    /// Used to create readers over the wal, which can safely
    /// read the shared file concurrently.
    pub fn new_reader(
        &self,
    ) -> FramedReader<OffsetFile, WalEntryCodec<R>, WalEntry> {
        self.inner.new_reader()
    }

    /// Returns the formatted wal file in the format of
    /// `<basepath>/<id>.<active | frozen>.wal`,
    /// see [format_wal_filename] for more.
    pub fn filename(&self) -> PathBuf {
        format_wal_filename(&self.inner.basepath, self.inner.id, true)
    }
}

/// A frozen Wal, which has already been flushed to disk with a valid
/// checksum at the end of the file.
pub struct FrozenWal<R: RecordCodecExt> {
    inner: Inner<R>,
}

impl<R: RecordCodecExt> Debug for FrozenWal<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrozenWal")
            .field("inner", &self.inner)
            .field("active", &false)
            .finish()
    }
}

impl<R: RecordCodecExt> FrozenWal<R> {
    #[instrument]
    pub fn open_frozen(
        basepath: &Path,
        id: u64,
        record_codec: R,
    ) -> crate::Result<Self> {
        Inner::open_frozen(basepath, record_codec, id)
    }

    /// Returns the checksum at the end of the [FrozenWal]
    ///
    /// # Panics
    ///
    /// This function panics if the frozen wals [ChecksumState] was not
    /// already `Finalized`.
    pub fn checksum(&self) -> u32 {
        match self.inner.checksum_state {
            ChecksumState::Finalized(c) => c,
            ChecksumState::Hasher(_) => {
                panic!("FrozenWal's checksum state wasnt `Finalized`")
            }
        }
    }

    /// Used to create readers over the wal, which can safely
    /// read the shared file concurrently.
    pub fn new_reader(
        &self,
    ) -> FramedReader<OffsetFile, WalEntryCodec<R>, WalEntry> {
        self.inner.new_reader()
    }

    /// Returns the formatted wal file in the format of
    /// `<basepath>/<id>.<active | frozen>.wal`,
    /// see [format_wal_filename] for more info.
    pub fn filename(&self) -> PathBuf {
        format_wal_filename(&self.inner.basepath, self.inner.id, false)
    }
}

/// The inner representation of a write-ahead log.
/// It encapsulates the file handle, the codec and the integrity checksum.
///
struct Inner<R: RecordCodecExt> {
    /// The WAL data directory under which all active and frozen and WALs go.
    basepath: PathBuf,

    /// File handle to the WAL
    ///
    /// Note that mixing FramedReaders and FramedWrites with a [File],
    /// seeking anywhere in the file modifies every other [File] cloned from it.
    ///
    /// Currenlty i havent figured out a way to make this 100% safe, maybe
    /// some state around ACTIVE | FLUSHED or RAII wrapper around
    /// WalFramedWriter / WalFramedReader that remembers the original `seek_position`
    /// then on drop seeks there.
    file: Arc<File>,

    /// Framed writer of records into the WAL, set to `Some(_)`` if this is an active
    /// Wal, set to `None` if its a frozen one.
    // fw: Option<FramedWriter<File, WalEntryCodec<R>, WalEntry>>,

    /// ID which comes from the [Memtable] that produced
    /// this WAL to be flushed. One [Memtable] always corresponds to one WAL
    id: u64,
    /// codec that works on both [Record], [Key] and [WalEntry]
    codec: WalEntryCodec<R>,

    /// The checksum hasher state, either being an actual Hasher, or the finished
    /// checksum. Reading the checksum from the [Wal] file is as easy as
    /// reading the last 4 bytes
    checksum_state: ChecksumState,
}

impl<R: RecordCodecExt> std::fmt::Debug for Inner<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            // .field("basepath", &self.basepath)
            // .field("id", &self.id)
            // .field("file", &self.file)
            // .field("checksum_state", &self.checksum_state)
            .finish_non_exhaustive()
    }
}

impl<R: RecordCodecExt> Inner<R> {
    /// Attempts to create a new active Wal in "/basepath" using the
    /// active wal filename format `/basepath/<id>.ACTIVE_EXT.WAL_EXT`.
    ///
    /// During any io or wal errors, Err(_) is returned
    ///
    /// If an active Wal already exists Ok(None) is returned.
    #[instrument(name = "inner_create_active")]
    pub fn create_active(
        basepath: &Path,
        next_id: u64,
        record_codec: R,
    ) -> crate::Result<Option<ActiveWal<R>>> {
        if Self::find_active_wal(basepath)?.is_some() {
            trace!("no active wal found");
            return Ok(None);
        };

        let wal_path = format_wal_filename(basepath, next_id, true);

        // if no such file is present, create it first, then reopen it as readable
        let mutfile = opts_create_read_append().open(&wal_path)?;
        let arcfile = Arc::new(opts_read().open(&wal_path)?);

        // # SAFETY
        //
        // new_empty() doesnt check the relations between the basepath, id,
        // file path and extensions, but `wal_path` came from `format_wal_filename()``
        let inner = unsafe {
            Inner::new_empty(basepath, next_id, arcfile, record_codec)
        };

        let fw = FramedWriter::new(mutfile, inner.codec.clone());

        Ok(Some(ActiveWal { inner, fw }))
    }

    /// Finds and opens the current active Wal file `/basepath`.
    ///
    /// It looks up the active wal path using the active wal filename format
    /// `/basepath/<id>.ACTIVE_EXT.WAL_EXT`, returning None if it wasnt found.
    ///
    /// # TODO
    /// - errors
    /// - find_active_wal()...
    #[instrument(ret, err)]
    pub fn open_active(
        basepath: &Path,
        record_codec: R,
    ) -> crate::Result<Option<ActiveWal<R>>> {
        let Some((wal_path_found, wal_id)) = Self::find_active_wal(basepath)?
        else {
            return Ok(None);
        };

        // little sanity check for myself
        let wal_path_rebuilt = format_wal_filename(basepath, wal_id, true);
        debug_assert_eq!(
            wal_path_found, wal_path_rebuilt,
            "rebuilt active wal path, and discovered wal path DO NOT match"
        );

        // shared read only file for OffsetFiles
        let arcfile = Arc::new(opts_read().open(&wal_path_rebuilt)?);
        // single mutable file handle for FramedWrite
        let mutfile = opts_create_read_append().open(&wal_path_rebuilt)?;

        let mut checksum_state = ChecksumState::new_hasher();
        let entry_codec = WalEntryCodec {
            record_codec: record_codec.clone(),
        };

        // check file integrity by iterating over each entry in the file
        // building a checksum of our own, then checking if the last entry
        // was *the* checksum. If so, compare the two checksums,
        // signaling if the wal is corrupted or not.
        // If the last entry was indeed the checksum (and no more checksums
        // where found that would validate the only-one-at-the-end checksum
        // invariant), this wall needs to be flushed and should not be
        // appended to, otherwise its considered still active wal.
        //
        // also walk resets seek position to SeekFrom::Start(0)
        Self::check_raw_wal_integrity(
            OffsetFile::new(arcfile.clone()),
            entry_codec.clone(),
            &mut checksum_state,
        )?;

        let fw = FramedWriter::new(mutfile, entry_codec.clone());

        let inner = Self {
            basepath: basepath.to_path_buf(),
            file: arcfile,
            id: wal_id,
            checksum_state,
            codec: entry_codec,
        };

        Ok(Some(ActiveWal { inner, fw }))
    }

    pub fn open_frozen(
        basepath: &Path,
        record_codec: R,
        id: u64,
    ) -> crate::Result<FrozenWal<R>> {
        let target_path = format_wal_filename(basepath, id, false);
        let file = Arc::new(opts_read().open(&target_path)?);
        let entry_codec = WalEntryCodec {
            record_codec: record_codec.clone(),
        };

        let mut checksum_state = ChecksumState::new_hasher();
        let output = Self::check_raw_wal_integrity(
            OffsetFile::new(file.clone()),
            entry_codec.clone(),
            &mut checksum_state,
        )?;

        match output {
            CheckRawWalOutput::ValidChecksum => {
                let inner = Self {
                    basepath: basepath.to_path_buf(),
                    file,
                    id,
                    checksum_state,
                    codec: entry_codec,
                };

                Ok(FrozenWal { inner })
            }
            CheckRawWalOutput::NoTrailingChecksum => {
                let err = WalError::InvalidFrozenState {
                    cause: Some(
                        "frozen wal has no trailing checksum".to_string(),
                    ),
                };
                Err(err.into())
            }
            CheckRawWalOutput::EmptyWal => {
                let err = WalError::InvalidFrozenState {
                    cause: Some("frozen wal has no entries".to_string()),
                };
                Err(err.into())
            }
        }
    }

    /// Returns a new [FramedReader] over the Wal. It is safe to
    /// create as many concurrent readers as possible, since the
    /// each [OffsetFile] uses its own offset + [FileExt]'s '`read_at(self.offset)`.
    ///
    /// - [FileExt](std::os::unix::fs::FileExt)
    pub fn new_reader(
        &self,
    ) -> FramedReader<OffsetFile, WalEntryCodec<R>, WalEntry> {
        let f = OffsetFile::new(self.file.clone());
        FramedReader::new(f, self.codec.clone())
    }

    /// Checks the wal files integrity:
    /// (1) For a frozen Wal, only one checksum exists and it only exists
    ///     as the last entry. The checksum entry needs to match the
    ///     reconstructed checksum, otherwise the file is considered corrupted
    ///
    /// (2) For an active Wal, there should not be any checksums; if a valid
    ///     (and only) one exists at the end, and the checksum match,
    ///     the Wal should be considered a valid frozen and should be flushed
    ///
    /// If the integrity was upheld, `Ok(_)` is returned, see [CheckRawWalOutput] for more information.
    ///
    /// **Note** that if any I/O or checksum is encountered, the function
    /// short circuts, meaning the callback might not be invoked on all
    /// entires. To make sure it is, check for `Ok(())` return value.
    ///
    /// Iteration happens using `walk`, which resets the seek position to
    /// to the start of the file.
    fn check_raw_wal_integrity(
        file: OffsetFile,
        entry_codec: WalEntryCodec<R>,
        checksum_state: &mut ChecksumState,
    ) -> crate::Result<CheckRawWalOutput> {
        // last entry encountered during a walk
        let mut last: Option<WalEntry> = None;
        // has there been *any* checksums during the walk?
        let mut checksum_found = false;

        let check_integrity = |entry: &WalEntry, entry_bs: &[u8]| {
            match &entry {
                WalEntry::Record(_) => {
                    // found a checksum before a record?
                    if checksum_found {
                        let ie = IntegrityError::InvalidChecksumPosition;
                        return Err(WalError::Integrity(ie).into());
                    }

                    // currenlty this is could never fail,
                    // but for my future self reading the code in a couple
                    // months it could save some headache
                    debug_assert!(
                        checksum_state.update(entry_bs),
                        "hasher could not be updated during checksum replay \
                            (somehow turned from ::Hasher to ::Finalized?)"
                    );
                }
                WalEntry::Checksum(_) => {
                    if checksum_found {
                        let ie = IntegrityError::MultipleChecksums;
                        return Err(WalError::Integrity(ie).into());
                    } else {
                        checksum_found = true
                    }
                }
            }

            // clone should be okay, since Record { key: /* 4 * u64*/, payload: Arc<_> }
            last = Some(entry.clone());
            Ok(())
        };

        Self::walk_with(file, entry_codec, check_integrity)?;

        match last {
            Some(WalEntry::Record(_)) => {
                // checksum found but records followed
                //
                // technically this branch cannot be taken because
                // of the check in the loop
                if checksum_found {
                    let ie = IntegrityError::InvalidChecksumPosition;
                    Err(WalError::Integrity(ie).into())
                } else {
                    Ok(CheckRawWalOutput::NoTrailingChecksum)
                }
            }
            Some(WalEntry::Checksum(original)) => {
                let computed = checksum_state.finalize();
                if original != computed {
                    let ie = IntegrityError::ChecksumMismatch(
                        crate::ChecksumMismatch { original, computed },
                    );
                    Err(WalError::Integrity(ie).into())
                } else {
                    Ok(CheckRawWalOutput::ValidChecksum)
                }
            }
            None => Ok(CheckRawWalOutput::EmptyWal),
        }
    }

    /// Walks over the contents of the whole file from start to end,
    /// returning early if the provided closure returns `Err(_)`. Before
    /// returning, the file is seeked back to the start.
    fn walk_with<F>(
        file: OffsetFile,
        entry_codec: WalEntryCodec<R>,
        mut walk_fn: F,
    ) -> crate::Result<()>
    where
        F: FnMut(&WalEntry, &[u8]) -> crate::Result<()>,
    {
        // file.seek(io::SeekFrom::Start(0))?;
        let reader = FramedReader::new(file.clone_reset(), entry_codec.clone());
        let mut entry_buf = vec![];

        let mut ret = Ok(());
        for e in reader {
            let entry = e?;

            entry_buf.resize(entry.wire_len(), 0);
            entry_codec.encode(&entry, &mut entry_buf)?;

            let err = {
                let walk_fn_span = span!(tracing::Level::INFO, "__wal_walk_fn");
                let _g = walk_fn_span.enter();
                walk_fn(&entry, &entry_buf)
            };

            if let Err(e) = err {
                ret = Err(e);
                break;
            }
        }

        ret
    }

    #[instrument(ret, err)]
    fn find_active_wal(
        basepath: &Path,
    ) -> crate::Result<Option<(PathBuf, u64)>> {
        use std::ffi::OsString;

        #[allow(non_snake_case)]
        let WAL_EXT_LITERAL = OsString::from(WAL_EXT);
        #[allow(non_snake_case)]
        let FROZEN_EXT_LITERAL = OsString::from(FROZEN_EXT);
        let mut active_wal: Option<(PathBuf, u64)> = None;

        let dir =
            std::fs::read_dir(basepath).inspect_err(|_| eprintln!("baj"))?;

        for e in dir {
            let entry = e?;
            if entry.metadata()?.is_dir() {
                trace!(
                    "{}: is not a directory, skipping",
                    entry.path().display()
                );
                continue;
            }

            let fname = entry.file_name();
            let fname = match fname.into_string() {
                Ok(s) => s,
                Err(o) => {
                    trace!(
                        "{}: could not be turned into a String (invalid unicode)",
                        o.display()
                    );
                    continue;
                }
            };

            let mut ext_dot_parts = fname.split('.');

            let Some(wal_ext) = ext_dot_parts.next_back() else {
                trace!("{}: no last extension found (wanted .wal)", &fname);
                continue;
            };

            let Some(state_ext) = ext_dot_parts.next_back() else {
                trace!(
                    "{}: no 2nd-to-last extension found (wanted .frozen or .active)",
                    &fname
                );
                continue;
            };

            let Some(raw_wal_id) = ext_dot_parts.next_back() else {
                trace!(
                    "{}: no 3rd-to-last extension found (wanted ID)",
                    &fname
                );
                continue;
            };

            if wal_ext != WAL_EXT_LITERAL {
                trace!(
                    "{}: last file extension {} is not {}, skipping",
                    fname, wal_ext, WAL_EXT,
                );
                continue;
            }

            if state_ext == FROZEN_EXT_LITERAL {
                trace!("{} has a frozen extension, skipping", fname);
                continue;
            }

            match active_wal {
                Some((duplicate, _)) => {
                    let e = format!(
                        "duplicate active Wal files found: first found = {}, duplicate = {}",
                        fname,
                        duplicate.display()
                    );
                    error!(e);
                    return Err(e.into());
                }
                None => match raw_wal_id.parse() {
                    Ok(id) => {
                        active_wal = Some((entry.path(), id));
                    }
                    Err(e) => {
                        let e = format!(
                            "{}: failed to parse ID from raw id {}, error: {}",
                            fname, raw_wal_id, e
                        );
                        error!("{e}");
                        return Err(e.into());
                    }
                },
            }
        }

        Ok(active_wal)
    }

    /// TODO: should use this new func
    ///
    /// Returns an "emtpy" Inner, given the basepath,
    /// id, file and record codec.
    ///
    /// # SAFETY
    ///
    /// This function does not check if the Files path
    /// is a correctly formatteed active or frozen wal path,
    /// nor any other relations of the parameters.
    unsafe fn new_empty(
        basepath: &Path,
        id: u64,
        file: Arc<File>,
        record_codec: R,
    ) -> Self {
        Inner {
            basepath: basepath.to_path_buf(),
            file,
            id,
            codec: WalEntryCodec { record_codec },
            checksum_state: ChecksumState::new_hasher(),
        }
    }
}

/// Return type of [Inner::check_raw_wal_integrity].
enum CheckRawWalOutput {
    /// The raw Wal has a valid (and only) checksum entry at the end of
    /// the file. The Wal should be considered *Frozen*, no further appends are
    /// permitted and should be flushed immedietly.
    ValidChecksum,

    /// The raw Wal has no checksum entry, meaning its still *Active* and
    /// can be appended to later.
    NoTrailingChecksum,

    /// No entries where found in the raw Wal
    EmptyWal,
}

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal error: {0}")]
    Integrity(#[from] IntegrityError),

    #[error("wal error: invalid frozen wal state, cause = {cause:?}")]
    InvalidFrozenState { cause: Option<String> },
}

/// Error for checking the integrity of a Wal, currently it has 3 variants,
/// one for mismatched checksums (see [ChecksumMismatch](crate::ChecksumMismatch)),
/// one for multiple checksums in a Wal, and one for frozen Wals specifically,
/// where the last entry in isnt (the only) [WalEntry::Checksum]
#[derive(Debug, thiserror::Error)]
pub enum IntegrityError {
    /// Stored checksum did not match replayed checksum
    #[error("wal integrity error: {0}")]
    ChecksumMismatch(#[from] crate::ChecksumMismatch),

    /// Multiple checksums where found, but a valid
    /// Wal only has one checksum entry (at the end of the file)
    #[error(
        "wal integrity error: multiple checksums found \
        (only 1 could be set at the end of a frozen wal, and none in an active wal)"
    )]
    MultipleChecksums,

    /// Checksum was found at an invalid (non-trailing) position
    #[error(
        "wal integrity error: checksum found somewhere other then the files end"
    )]
    InvalidChecksumPosition,

    /// Checksum not found entirely, this is only an error for a frozen Wal,
    /// as active Wals can continue appends, up until it needs to be flushed,
    /// when the checksum should be appended last
    #[error("wal integrity error: checksum not found")]
    ChecksumNotFound,
}

/// An entry in the wal, which is thin a wrapper around a
/// [Record](crate::record::Record), and a checksum.
///
/// # Note
///
/// Only one checksum entry should exist in a WAL, and it should be the last
/// entry in the whole file. This way its easy to determine if a WAL if "finished" and should be flushed, or if it can keep accepting records.
///
/// With the checksum presetn, checking the integrity of the file is done by
/// iterating over the Wal with a [FramedReader](crate::codec::FramedReader)
/// while building up a running checksum, then comparing them.
#[derive(Debug, Clone)]
pub enum WalEntry {
    /// Normal record inserted into the wal, on the wire its prefix tag is "0"
    Record(Record),

    /// Checksum to prove the integrity of the whole WAL, only one should
    /// exist in a WAL, and it should be placed last; on the wire its prefix
    /// tag is "1"
    Checksum(u32),
}

impl codec::WireLen for WalEntry {
    /// tag + (record.wire_len() OR size_of::<u32>())
    fn wire_len(&self) -> usize {
        1 + match self {
            WalEntry::Record(r) => r.wire_len(),
            WalEntry::Checksum(c) => size_of_val(c),
        }
    }
}

/// Codec for [WalEntry], and just like how [WalEntry] is a wrapper
/// around [Record](crate::record::Record), [WalEntryCodec] also wraps and
/// delegates to [RecordCodec](crate::record::RecordCodec)
#[derive(Debug, Copy, Clone)]
pub struct WalEntryCodec<R> {
    record_codec: R,
}

impl<R: RecordCodecExt> codec::SpecCodec<WalEntry> for WalEntryCodec<R> {
    #[instrument]
    fn encode(
        &self,
        item: &WalEntry,
        dst: &mut [u8],
    ) -> Result<usize, codec::CodecError> {
        if dst.len() < item.wire_len() {
            trace!(
                "not enough bytes to encode into `dst` (got: {}, want: {})",
                dst.len(),
                item.wire_len()
            );
            return Err(codec::not_enough_bytes(dst.len(), item.wire_len()));
        }

        match item {
            WalEntry::Record(rec) => {
                dst[0] = 0;
                self.record_codec
                    .encode_record(rec, &mut dst[1..])
                    .map(|n| n + 1) // tag that i ofc forgot
            }
            WalEntry::Checksum(cs) => {
                dst[0] = 1;
                dst[1..5].copy_from_slice(&cs.to_be_bytes());
                Ok(5)
            }
        }
    }

    #[instrument]
    fn decode(
        &self,
        src: &[u8],
    ) -> Result<Option<(WalEntry, usize)>, codec::CodecError> {
        if src.is_empty() {
            trace!("not enough bytes for WalEntry tag prefix (1 byte long)");
            return Ok(None);
        }

        match src[0] {
            0 => {
                let rec_n = self.record_codec.decode_record(&src[1..])?;
                Ok(rec_n.map(|(rec, n)| (WalEntry::Record(rec), n + 1)))
            }
            1 => {
                if src.len() < 5 {
                    trace!(
                        "not enough bytes for WalEntry::Checksum (1 + 5 byte long)"
                    );
                    return Ok(None);
                }

                let cs = u32::from_be_bytes([src[1], src[2], src[3], src[4]]);
                Ok(Some((WalEntry::Checksum(cs), 5)))
            }
            other => {
                let err =
                    format!("invalid tag prefix {other} found for WalEntry");
                error!("{err}");
                Err(codec::CodecError::Other(err))
            }
        }
    }
}

/// A wrapper around a crc hasher OR its finalized checksum value.
#[derive(Debug)]
enum ChecksumState {
    Finalized(u32),
    Hasher(crc32fast::Hasher),
}

impl ChecksumState {
    /// Creates a new hasher variant with empty state.
    fn new_hasher() -> Self {
        Self::Hasher(crc32fast::Hasher::new())
    }

    /// Updates the internal state of the `Hasher(_)` variant,
    /// returning true if succesfull, and false if the current variant is `Finalized(_)`
    fn update(&mut self, buf: &[u8]) -> bool {
        match self {
            ChecksumState::Finalized(_) => {
                trace!(
                    "tried calling ChecksumState::update() on a ChecksumState::Finalized(_) variant"
                );
                false
            }
            ChecksumState::Hasher(hasher) => {
                hasher.update(buf);
                true
            }
        }
    }

    /// Return the finalized checksum if th `self` is `Finalized(_)`,
    /// or compute with the `Hasher(hasher)` variant, and swapping it with
    /// `Finalize(_)` before returning it
    fn finalize(&mut self) -> u32 {
        match self {
            ChecksumState::Finalized(cs) => *cs,
            _ => {
                // SAFETY
                //
                // Read self to later consume it, since crc32fast::Hasher takes
                // self by value in finalize().
                //
                // Later we write a valied variant into self, and before that
                // crc32fast::Hasher::finalize() is guaranteed to not panic,
                // meaning the pointer left behind wont cause UB
                let cs = unsafe { std::ptr::read(self as *const _) };

                match cs {
                    ChecksumState::Hasher(hasher) => {
                        let fin = hasher.finalize();

                        unsafe {
                            std::ptr::write(
                                self as *mut _,
                                ChecksumState::Finalized(fin),
                            );
                        };

                        fin
                    }
                    ChecksumState::Finalized(_) => unreachable!(),
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    use tempfile::TempDir;

    use crate::{
        record::{Key, Record, RecordCodec},
        wal::{ActiveWal, FrozenWal, WalEntry},
    };

    const PAYLOAD: &[u8] = b"du bist gut genug";

    fn tracing() -> tracing::subscriber::DefaultGuard {
        let s = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .finish();
        tracing::subscriber::set_default(s)
    }

    #[test]
    fn wal_append_flush_read_frozen() {
        let _g = tracing();

        let record_codec = RecordCodec;
        let d = TempDir::with_prefix("WAL_TEST_DIR_").expect("tempdir");

        let mut w = ActiveWal::create_active(d.path(), 0, record_codec)
            .expect("create_active -> Err")
            .expect("create_active -> None");

        let records: Vec<_> = (0..4u64)
            .map(|i| Record {
                key: Key::new(1, 2, i, 4),
                payload: PAYLOAD.into(),
            })
            .collect();

        for r in &records {
            w.append(r).expect("wal append failed");
        }
        let frozen = w.flush().expect("flush -> Err").expect("flush -> None");

        let mut frozen_entries = vec![];
        let mut checksum_found = false;
        for e in frozen.new_reader().enumerate() {
            let (i, e) = e;
            let e = match e {
                Ok(we) => we,
                Err(e) => {
                    panic!("frozen.new_reader() next entry errored: {e}")
                }
            };

            match &e {
                WalEntry::Record(_) => {
                    if checksum_found {
                        panic!(
                            "checksum found, but another record followed (pos = {i})"
                        )
                    }
                }
                WalEntry::Checksum(_) => {
                    if checksum_found {
                        panic!("multiple checksums found (pos = {i})")
                    } else {
                        checksum_found = true
                    }
                }
            }

            // println!("{e:?}");
            frozen_entries.push(e);
        }

        assert!(
            matches!(frozen_entries.last(), Some(WalEntry::Checksum(_))),
            "last entry in forzen wal wasnt checksum!"
        );

        frozen_entries
            .iter()
            .take(frozen_entries.len() - 1)
            .zip(records)
            .enumerate()
            .for_each(|(i, (w, r))| {
                let WalEntry::Record(w) = w else {
                    panic!("checksum found in the record section of wal")
                };
                println!(
                    "[test]: ({i}) =>\n\twal_entry_key:\t{:?}\n\trecord_key:\t{:?}",
                    w.key, r.key
                );

                pretty_assertions::assert_eq!(w.key, r.key);
                pretty_assertions::assert_eq!(w.payload, r.payload);
            });
    }

    #[test]
    fn wal_recover() {
        let _g = tracing();

        let record_codec = RecordCodec;
        let d = TempDir::with_prefix("WAL_TEST_DIR_").expect("tempdir");
        let basepath = d.path();

        let mut w = ActiveWal::create_active(basepath, 0, record_codec)
            .expect("create_active -> Err")
            .expect("create_active -> None");

        let records: Vec<_> = (0..4u64)
            .map(|i| Record {
                key: Key::new(1, 2, i, 4),
                payload: PAYLOAD.into(),
            })
            .collect();

        for r in &records {
            w.append(r).expect("wal append failed");
        }
        let frozen = w.flush().expect("flush -> Err").expect("flush -> None");

        let mut frozen_entries = vec![];
        for e in frozen.new_reader().enumerate() {
            frozen_entries.push(e);
        }

        // let wal_filename = frozen.filename();

        let reopen_active = ActiveWal::open_active(basepath, record_codec);
        assert!(reopen_active.is_ok(), "ActiveWal::open_active -> error");
        assert!(
            matches!(reopen_active, Ok(None)),
            "active file found after active.flush()"
        );

        let reopen_frozen =
            FrozenWal::open_frozen(basepath, frozen.inner.id, record_codec)
                .expect("failed to open frozen wal");

        let mut checksum_found_at = None;
        let mut n_found = 0;
        reopen_frozen.new_reader().enumerate().for_each(|(i, res)| {
            n_found += 1;
            let e = res.expect("recover: failed to decode record");
            let WalEntry::Record(rec) = e else {
                checksum_found_at = Some(i);
                return;
            };

            assert_eq!(
                i as u64, rec.key.sequence_num,
                "recover: sequence_num mismatch"
            );
            assert_eq!(PAYLOAD, &*rec.payload, "recover: payload mismatch");
        });

        assert_eq!(
            n_found,
            records.len() + 1,
            "[test]: expected to found records.len() + 1 many items in reopened frozen wal"
        );

        assert_eq!(
            checksum_found_at,
            Some(n_found - 1),
            "[test]: expected checksum entry to be the last"
        );
    }

    // /// Pins down the shared-fd offset bug: recovering must not disturb the
    // /// writer's position. Append, recover, append again, recover again —
    // /// the second recovery should see all records from both rounds, in
    // /// order, not have the first round overwritten.
    // #[test]
    // fn wal_append_after_recovery_does_not_corrupt_prior_records() {
    //     let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    //     let codec = RecordCodec;
    //     let d = TempDir::new().expect("tempdir");
    //     let mut w = __Wal::open_new(d.path(), 0, codec).expect("wal::new");

    //     let first_batch: Vec<Record> = (0..10u64)
    //         .map(|seq| Record {
    //             key: Key::new(1, 1, seq, 1),
    //             payload: PAYLOAD.into(),
    //         })
    //         .collect();
    //     for rec in &first_batch {
    //         w.append(rec).expect("append failed");
    //     }
    //     w.flush().expect("flush failed");

    //     let wal_file_path = format_wal_filename(&w.basepath);
    //     // Recover once: this seeks a shared fd back to 0 in the current
    //     // implementation, which is exactly the bug this test targets.
    //     let _ = __Wal::test_recover(&wal_file_path, codec)
    //         .expect("recovery failed")
    //         .collect::<Vec<_>>();

    //     let second_batch: Vec<Record> = (10..20u64)
    //         .map(|seq| Record {
    //             key: Key::new(1, 1, seq, 1),
    //             payload: PAYLOAD.into(),
    //         })
    //         .collect();
    //     for rec in &second_batch {
    //         w.append(rec).expect("append failed");
    //     }
    //     w.flush().expect("flush failed");

    //     let recovered: Vec<Record> = __Wal::test_recover(&wal_file_path, codec)
    //         .expect("recovery failed")
    //         .map(|r| r.expect("decode failed"))
    //         .collect();

    //     let expected: Vec<Record> =
    //         first_batch.into_iter().chain(second_batch).collect();
    //     assert_eq!(
    //         expected, recovered,
    //         "appending after a recovery pass must not corrupt or overwrite prior records"
    //     );
    // }
}
