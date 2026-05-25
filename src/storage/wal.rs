use std::io::{self, Read, Seek, Write};
use std::sync::Arc;

use anyhow::{Context, Ok};

use crate::metrics::Metrics;
use crate::storage::record::Record;

pub struct Wal {
    w: io::BufWriter<std::fs::File>,
    metrics: Option<Arc<Metrics>>,
}

impl Wal {
    pub const WAL_PATH_FMT: &'static str = "wal.log";

    pub fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let file: std::fs::File = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .with_context(|| format!("wal: failed to open file {:?}", path))?;

        Ok(Wal {
            w: io::BufWriter::new(
                file.try_clone()
                    .context("wal::new: failed to clone file descriptor")?,
            ),
            metrics: None,
        })
    }

    pub fn set_metrics(&mut self, metrics: &Arc<Metrics>) {
        self.metrics = Some(metrics.clone());
    }

    pub fn write_one(&mut self, rec: &Record) -> anyhow::Result<()> {
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

    pub fn write_many(&mut self, recs: &[Record]) -> anyhow::Result<()> {
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

    pub fn recover(&mut self) -> anyhow::Result<Vec<Record>> {
        let mut f = self
            .w
            .get_ref()
            .try_clone()
            .context("wal.recover: failed to rewind to beginning of file")?;
        f.rewind()
            .context("wal.recover: failed to rewind to start of file")?;
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

    pub fn sync(&mut self) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        self.w.flush().context("wal.sync: failed to flush")?;
        self.w
            .get_ref()
            .sync_all()
            .context("wal.sync: failed to sync_all")?;
        if let Some(ref m) = self.metrics {
            m.wal.sync_count.inc(1);
            m.wal.sync_latency.record_instant(start);
        }
        Ok(())
    }

    pub fn clear(&mut self) -> anyhow::Result<()> {
        let f = self.w.get_mut();
        f.set_len(0).context("wal.clear: failed to truncate")?;
        f.rewind()
            .context("wal.clear: failed to rewind to beginning of file")?;
        let f_cloned = self
            .w
            .get_ref()
            .try_clone()
            .context("wal.clear: failed to clone file")?;
        self.w = io::BufWriter::new(f_cloned);

        Ok(())
    }

    // pub fn clear_path(path: &std::path::Path) -> anyhow::Result<()> {
    //     let f = std::fs::OpenOptions::new()
    //         .write(true)
    //         .open(path)
    //         .with_context(|| format!("wal.clear_path: failed to open {:?}", path))?;
    //     f.set_len(0).context("wal.clear_path: failed to truncate")?;
    //     drop(f);
    //     Ok(())
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_persistence() {
        let dir = tempdir().unwrap();
        let wal_path_buf = dir.path().join("test.wal");
        let wal_path = &wal_path_buf;

        let entry = Record::from_raw_parts(1, 12345, 0, "user_1", "login_event");

        {
            let mut wal = Wal::new(wal_path).expect("failed to create WAL");
            wal.write_one(&entry).expect("failed to write");
            wal.sync().expect("failed to sync");
        }

        let mut wal = Wal::new(wal_path).expect("failed to re-open WAL");
        let recovered = wal.recover().expect("failed to recover");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].cmp(&entry), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_wal_ordering() {
        let dir = tempdir().unwrap();
        let wal_path_buf = dir.path().join("ordering.wal");
        let wal_path = &wal_path_buf;
        let mut wal = Wal::new(wal_path).unwrap();

        let entries = vec![
            Record::from_raw_parts(1, 1, 0, "a", "v1"),
            Record::from_raw_parts(1, 2, 0, "b", "v2"),
            Record::from_raw_parts(1, 3, 0, "c", "v3"),
        ];

        for e in &entries {
            wal.write_one(e).unwrap();
        }
        wal.sync().unwrap();

        let recovered = wal.recover().unwrap();

        assert_eq!(recovered.len(), entries.len());
        for (r, e) in recovered.iter().zip(entries.iter()) {
            assert_eq!(r.cmp(e), std::cmp::Ordering::Equal);
        }
    }

    #[test]
    fn test_wal_clear() {
        let dir = tempdir().unwrap();
        let wal_path_buf = dir.path().join("clear.wal");
        let wal_path = &wal_path_buf;
        let mut wal = Wal::new(wal_path).unwrap();

        wal.write_one(&Record::from_raw_parts(1, 1, 0, "k", "v"))
            .unwrap();
        wal.sync().unwrap();
        wal.clear().expect("Failed to clear WAL");

        let recovered = wal.recover().unwrap();
        assert_eq!(recovered.len(), 0);
    }
}
