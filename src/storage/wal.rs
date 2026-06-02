use std::{
    collections::BTreeSet, fs, io::{self, Read, Write}, path::{Path, PathBuf}, sync::Arc
};

use anyhow::Context;
use tracing::instrument;

use crate::{metrics::Metrics, storage::record::Record};

pub struct Wal {
    path: PathBuf,
    w: io::BufWriter<std::fs::File>,
    metrics: Option<Arc<Metrics>>,
}

impl Wal {
    pub const WAL_DIR: &'static str = "wals";
    pub const ACTIVE_WAL_NAME: &'static str = "__active_wal.log";

    pub fn format_inactive_wal_path(base_path: &Path, id: u64) -> PathBuf {
        base_path.join(format!("{}/wal_{:010}.log", Self::WAL_DIR, id))
    }
    
    pub fn format_active_wal_path(base_path: &Path) -> PathBuf {
        base_path.join(Self::WAL_DIR).join(Self::ACTIVE_WAL_NAME)
    }

    pub(super) fn open_active(base_path: &Path) -> anyhow::Result<Self> {
        let path = base_path.join(Self::WAL_DIR).join(Self::ACTIVE_WAL_NAME);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .with_context(|| format!("wal::new: failed to open file {:?}", path))?;

        let w = io::BufWriter::new(
            file.try_clone()
                .context("wal::new: failed to clone file descriptor")?,
        );

        Ok(Wal {
            path: path,
            w,
            metrics: None,
        })
    }

    pub(super) fn set_metrics(&mut self, metrics: &Arc<Metrics>) {
        self.metrics = Some(metrics.clone());
    }

    pub(super) fn write_one(&mut self, rec: &Record) -> anyhow::Result<()> {
        let len = rec.len() as u64;
        self.w
            .write_all(&len.to_le_bytes())
            .context("wal.write_one: failed to write prefix len")?;
        self.w
            .write_all(rec.as_bytes())
            .context("wal.write_one: failed to write record")?;
        if let Some(ref m) = self.metrics {
            m.wal.write_count.inc(1);
            m.wal.write_bytes.inc(len);
        }
        Ok(())
    }

    pub(super) fn write_many(&mut self, recs: &[Record]) -> anyhow::Result<()> {
        let mut total_bytes = 0u64;
        for rec in recs {
            let len = rec.len() as u64;
            self.w
                .write_all(&len.to_le_bytes())
                .context("wal.write_many: failed to write prefix len")?;
            self.w
                .write_all(rec.as_bytes())
                .context("wal.write_many: failed to write record")?;
            total_bytes += 8 + len; // 8 byte length prefix + record bytes
        }
        if let Some(ref m) = self.metrics {
            m.wal.write_count.inc(recs.len() as u64);
            m.wal.write_bytes.inc(total_bytes);
        }
        Ok(())
    }

    #[instrument(skip_all, err)]
    pub(super) fn recover_all(base_path: &Path) -> anyhow::Result<Vec<Record>>{
        let mut records = BTreeSet::new();
        let files =  fs::read_dir(base_path.join(Self::WAL_DIR))
            .context("wal.recover_all: failed to read wal dir")?;
        for f in files {
            let path = f?.path();
            if path.extension().and_then(|n| n.to_str()) == Some("log") {
                if let Ok(rec) = Self::recover_file(path.as_path()) {
                    records.extend(rec.into_iter());
                }
            }
        }

        Ok(records.into_iter().collect())
    }

    pub(super) fn recover_file(path: &Path) -> anyhow::Result<Vec<Record>> {
        let f = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("wal.recover_file: failed to open {}", path.display()))?;
        let mut r = io::BufReader::new(f);
        let mut records = Vec::new();

        loop {
            let mut len_buf = [0u8; 8];
            match r.read_exact(&mut len_buf) {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e).context("wal.recover: failed to read record length"),
                _ => {}
            };
            let len = u64::from_le_bytes(len_buf) as usize;

            let mut buf = vec![0u8; len];
            r.read_exact(&mut buf)
                .context("wal.recover: failed to read record buffer")?;

            records.push(Record::from_vec(buf));
        }

        Ok(records)
    }

    #[instrument(skip_all, err)]
    pub(super) fn flush_buffer(&mut self) -> anyhow::Result<()> {
        self.w.flush()?;
        Ok(())
    }

    /// creates a new active WAL file, returning the old files new, rotated path
    #[instrument(skip_all, err)]
    pub(super) fn rotate(&mut self, base_path: &Path, sst_id: u64) -> anyhow::Result<PathBuf> {
        self.flush_buffer()?;

        let rotated_path = Self::format_inactive_wal_path(base_path, sst_id);
        fs::rename(self.path.as_path(), rotated_path.as_path())
            .context("wal.rotate: failed to rename old WAL")?;

        let new_active_path = base_path.join(Self::WAL_DIR).join(Self::ACTIVE_WAL_NAME);
        let f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&new_active_path)
            .with_context(|| format!("wal.rotate: failed to open file {:?}", &new_active_path))?;

        let w = io::BufWriter::new(
            f.try_clone()
                .context("wal.rotate: failed to clone file descriptor")?,
        );
        self.path = new_active_path;
        self.w = w;

        Ok(rotated_path)
    }
}

/// Simple wrapper around a file that only exposes an fsync() call
pub struct WalSyncer(std::fs::File);

impl WalSyncer {
    pub(super) fn fsync(&mut self) -> anyhow::Result<()> {
        self.0.sync_all().context("io_worker: fsync() failed")?;
        Ok(())
    }

    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("wal_syncer.open: failed to create directory {}", parent.display()))?;
        }
        let f = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(WalSyncer(f))
    }
}

impl From<fs::File> for WalSyncer {
    fn from(f: fs::File) -> Self {
        Self(f)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_persistence() {
        let dir = tempdir().unwrap();
        let wals_dir = dir.path().join(Wal::WAL_DIR);
        std::fs::create_dir_all(&wals_dir).unwrap();

        let entry = Record::from_raw_parts(1, 12345, 0, "user_1", "login_event");

        {
            let mut wal = Wal::open_active(dir.path()).expect("failed to create WAL");
            wal.write_one(&entry).expect("failed to write");
        }

        let recovered = Wal::recover_all(dir.path()).expect("failed to recover");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].cmp(&entry), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_wal_ordering() {
        let dir = tempdir().unwrap();
        let wals_dir = dir.path().join(Wal::WAL_DIR);
        std::fs::create_dir_all(&wals_dir).unwrap();
        let mut wal = Wal::open_active(dir.path()).unwrap();

        let entries = vec![
            Record::from_raw_parts(1, 1, 0, "a", "v1"),
            Record::from_raw_parts(1, 2, 0, "b", "v2"),
            Record::from_raw_parts(1, 3, 0, "c", "v3"),
        ];

        for e in &entries {
            wal.write_one(e).unwrap();
        }
        wal.flush_buffer().unwrap();

        let recovered = Wal::recover_all(dir.path()).unwrap();

        assert_eq!(recovered.len(), entries.len());
        for (r, e) in recovered.iter().zip(entries.iter()) {
            assert_eq!(r.cmp(e), std::cmp::Ordering::Equal);
        }
    }
}
