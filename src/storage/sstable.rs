//! This module defines the SSTable file handles (and metadata) through which we can interract with the
//! actual records in the DB in sorted order. It contains each tables metadata, a file path
//! and helper objects like the bloom filter and index block.

use std::{
    fs,
    hash::{BuildHasher, Hasher, RandomState},
    io::{self, Seek, Write},
    path::{Path, PathBuf},
};

use anyhow::Context;

use crate::storage::Record;

#[derive(Debug, Clone)]
pub struct SSTableMeta {
    pub id: u64,
    pub path: PathBuf,
    pub bloom: BloomFilter,
    pub bloom_offset: u64,
    pub index: IndexBlock,
    pub index_offset: u64,
    pub file_size: u64,
    pub num_records: usize,
}

impl SSTableMeta {
    pub fn format_file_path(base_path: &Path, id: u64) -> PathBuf {
        base_path.join(format!("{:010}.sst", id))
    }

    pub fn write_to_file<I: Iterator<Item = Record>>(
        path: PathBuf,
        id: u64,
        source: I,
        source_len: usize,
    ) -> anyhow::Result<Self> {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .context("engine.flush: failed to open sstable file")?;
        let mut w = io::BufWriter::new(f);
        let mut bloom = BloomFilter::new(source_len, 0.01);
        let mut index = IndexBlock::with_capacity(source_len);

        let mut record_offset = 0u64;
        let mut num_records = 0usize;
        for (idx, rec) in source.enumerate() {
            let len_buf = (rec.len() as u64).to_le_bytes();
            w.write_all(&len_buf)
                .context("engine.flush: failed to write record length")?;
            w.write_all(rec.as_bytes())
                .context("engine.flush: failed to write record")?;

            bloom.insert(
                rec.extract_source_id()
                    .context("write_to_file: failed to extract source_id for bloom")?,
                rec.extract_key()
                    .context("write_to_file: failed to extract key for bloom")?,
            );

            index
                .try_insert(idx, &rec, record_offset)
                .context("engine.flush: failed to insert record into index block")?;

            record_offset += 8 + rec.len() as u64;
            num_records += 1
        }

        let index_offset = w
            .stream_position()
            .context("engine.flush: failed to get index block offset")?;
        index
            .encode(&mut w)
            .context("engine.flush: failed to write index block to file")?;

        let bloom_offset = w
            .stream_position()
            .context("engine.flush: failed to get bloom filter offset")?;
        bloom
            .encode(&mut w)
            .context("engine.flush: failed to write bloom filter to file")?;

        w.write_all(&index_offset.to_le_bytes())
            .context("engine.flush: failed to write index offset")?;
        w.write_all(&bloom_offset.to_le_bytes())
            .context("engine.flush: failed to write bloom offset")?;

        w.flush()
            .context("engine.flush: failed to flush write buffer")?;
        w.get_ref()
            .sync_all()
            .context("engine.flush: failed to sync_all file")?;

        let file_size = fs::metadata(&path)
            .context("engine.flush: failed to read sstable file metadata")?
            .len();

        let meta = SSTableMeta {
            id,
            path,
            bloom,
            bloom_offset,
            index,
            index_offset,
            num_records,
            file_size,
        };

        Ok(meta)
    }
}

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bitmap: Vec<u8>,
    n_bits: usize,
    n_hashes: usize,
    rs: RandomState,
}

impl BloomFilter {
    const SALT1: &[u8] = b"bloom_salt_1";
    const SALT2: &[u8] = b"bloom_salt_2";

    pub fn new(n: usize, fpr: f64) -> Self {
        let m = (-(n as f64 * fpr.ln()) / 2f64.ln().powi(2)).ceil() as usize;
        let k = ((m as f64 / (n as f64)) * 2f64.ln()) as usize;
        Self {
            bitmap: vec![0; m.div_ceil(8)],
            n_bits: m,
            n_hashes: k.max(1),
            rs: RandomState::new(),
        }
    }

    /// read_from_unchecked parses a reader (and seaker) and returns the filter and its file offset.
    pub unsafe fn read_from_unchecked(
        mut r: impl io::Read + io::Seek,
    ) -> anyhow::Result<(Self, u64)> {
        let mut bloom_offset_buf = [0u8; 8];
        r.read_exact(&mut bloom_offset_buf)
            .context("bloom_filter.load_from: failed to read bloom filter offset")?;
        let bloom_offset = u64::from_le_bytes(bloom_offset_buf);

        r.seek(io::SeekFrom::Start(bloom_offset))
            .context("bloom_filter.load_from: failed to seek to bloom filter start offset")?;
        let bloom = Self::decode(&mut r)
            .context("bloom_filter.read_from_unchecked: failed to decode bloom filter")?;
        Ok((bloom, bloom_offset))
    }

    pub fn positions(&self, source_id: i64, key: &[u8]) -> Vec<usize> {
        let mut hbuf = Vec::with_capacity(8 + key.len());
        hbuf.extend_from_slice(&source_id.to_le_bytes());
        hbuf.extend_from_slice(key);
        let h1 = self.hash_with_salt(&hbuf, Self::SALT1);
        let h2 = self.hash_with_salt(&hbuf, Self::SALT2);
        (0..(self.n_hashes as u64))
            .map(|i| h1.wrapping_add(i.wrapping_mul(h2)) as usize % self.n_bits)
            .collect()
    }

    pub fn insert(&mut self, source_id: i64, key: &[u8]) {
        for pos in self.positions(source_id, key) {
            self.set_bit(pos);
        }
    }

    pub fn contains(&self, source_id: i64, key: &[u8]) -> bool {
        self.positions(source_id, key)
            .into_iter()
            .all(|pos| self.get_bit(pos))
    }

    /// bloom filter layout:
    ///
    /// [[n_bits u64]] [[n_hashes u64]] [[bf_len u64]] [bf_content [[u8; bf_len]]]
    ///
    /// NOTE: this does not append the bloom_filter start offset
    pub fn encode(&self, mut w: impl io::Write) -> anyhow::Result<()> {
        w.write_all(&(self.n_bits as u64).to_le_bytes())
            .context("bloom_filter.encode: failed to write n_bits")?;
        w.write_all(&(self.n_hashes as u64).to_le_bytes())
            .context("bloom_filter.encode: failed to write n_hashes")?;
        w.write_all(&(self.bitmap.len() as u64).to_le_bytes())
            .context("bloom_filter.encode: failed to write bitmap len")?;
        w.write_all(&self.bitmap)
            .context("bloom_filter.encode: failed to write bitmap")?;
        Ok(())
    }

    /// <code>[n_bits u64][n_hashes u64][bf_len u64][bf_content [u8; bf_len]]</code>
    ///
    /// NOTE: decode assumes the reader points to the beginning of the bloom filter footer.
    pub fn decode(mut r: impl io::Read) -> anyhow::Result<Self> {
        let mut u64_buf = [0u8; 8];
        r.read_exact(&mut u64_buf)
            .context("bloom_filter.decode: failed to read n_bits")?;
        let n_bits = u64::from_le_bytes(u64_buf) as usize;

        r.read_exact(&mut u64_buf)
            .context("bloom_filter.decode: failed to read n_hashes")?;
        let n_hashes = u64::from_le_bytes(u64_buf) as usize;

        r.read_exact(&mut u64_buf)
            .context("bloom_filter.decode: failed to read filter len")?;
        let len = u64::from_le_bytes(u64_buf) as usize;

        let mut bitmap_buf = vec![0u8; len];
        r.read_exact(&mut bitmap_buf)
            .context("bloom_filter.decode: failed to read filter contents")?;

        Ok(Self {
            n_bits,
            n_hashes,
            bitmap: bitmap_buf,
            rs: RandomState::new(),
        })
    }

    fn set_bit(&mut self, pos: usize) {
        self.bitmap[pos >> 3] |= 1 << (pos & 7)
    }
    fn get_bit(&self, pos: usize) -> bool {
        (self.bitmap[pos >> 3] >> (pos & 7)) & 1 == 1
    }

    fn hash_with_salt(&self, data: &[u8], salt: &[u8]) -> u64 {
        let mut hasher = self.rs.build_hasher();
        hasher.write(data);
        hasher.write(salt);
        hasher.finish()
    }
}

/// RecordIndex stores (record_key, file_offset) pairs for
/// every N records in an sstable file.
///
/// # Layout
///
/// <code>[[source_id: i64 LE]][[timestamp: i64 LE]][[seq_num: u64 LE]][[key_len: u32 LE]][[key: key_len bytes]][[offset: u64 LE]]</code>
#[derive(Debug, Clone, Default)]
pub struct IndexBlock {
    /// inner is expected to be sorted, since we only insert
    /// elements produced by memtable or merge iterator
    /// which are both sorted.
    ///
    /// Sadly i dont know how to make this a type level constrain
    /// I could use a BinaryHeap here too, but ugh..?
    pub inner: Vec<RecordIndex>,
}

impl IndexBlock {
    pub const PERIOD_SZ: usize = 16;

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Vec::with_capacity(cap),
        }
    }

    /// try_insert converts the key bytes to utf8 Strings, and inserts the (key, offset) pair if <code>index % PERIOD_SZ == 0</code>
    pub fn try_insert(&mut self, idx: usize, rec: &Record, offset: u64) -> anyhow::Result<bool> {
        if !idx.is_multiple_of(Self::PERIOD_SZ) {
            return Ok(false);
        }

        let (source_id, timestamp, seq_num, key) = rec
            .extract_all_fields_no_value_ref()
            .context("record_index.try_insert: failed to extract fields from record bytes")?;

        let key = String::from_utf8(key.to_vec())
            .context("record_index.try_insert: failed to turn key into utf8 string")?;

        let i = RecordIndex {
            source_id,
            timestamp,
            seq_num,
            key,
            offset,
        };

        self.inner.push(i);
        Ok(true)
    }

    /// read_from_unchecked parses a reader (and seaker) and returns the index and its file offset.
    pub fn read_from_unchecked(mut r: impl io::Read + io::Seek) -> anyhow::Result<(Self, u64)> {
        let mut offset_buf = [0u8; 8];
        r.read_exact(&mut offset_buf)
            .context("record_index.load_from: failed to read index offset")?;
        let offset = u64::from_le_bytes(offset_buf);

        r.seek(io::SeekFrom::Start(offset))
            .context("record_index.load_from: failed to seek to index start offset")?;
        let index =
            Self::decode(&mut r).context("record_index.load_from: failed to decode index")?;
        Ok((index, offset))
    }

    fn decode(mut r: impl io::Read) -> anyhow::Result<Self> {
        let mut u64_buf = [0u8; 8];
        let mut u32_buf = [0u8; 4];
        r.read_exact(&mut u32_buf)
            .context("record_index.decode: failed to read index len")?;
        let len = u32::from_le_bytes(u32_buf) as usize;
        let mut vec = Vec::with_capacity(len);

        for _ in 0..len {
            r.read_exact(&mut u64_buf)
                .context("record_index.decode: failed to read source_id")?;
            let source_id = i64::from_le_bytes(u64_buf);

            r.read_exact(&mut u64_buf)
                .context("record_index.decode: failed to read timestamp")?;
            let timestamp = i64::from_le_bytes(u64_buf);

            r.read_exact(&mut u64_buf)
                .context("record_index.decode: failed to read seq_num")?;
            let seq_num = u64::from_le_bytes(u64_buf);

            r.read_exact(&mut u32_buf)
                .context("record_index.decode: failed to read key len")?;
            let key_len = u32::from_le_bytes(u32_buf) as usize;
            let mut key_buf = vec![0u8; key_len];
            r.read_exact(&mut key_buf)
                .context("record_index.decode: failed to read key")?;
            let key = String::from_utf8(key_buf)
                .context("record_index.decode: failed to decode key as utf8")?;

            r.read_exact(&mut u64_buf)
                .context("record_index.decode: failed to read offset")?;
            let offset = u64::from_le_bytes(u64_buf);

            vec.push(RecordIndex {
                source_id,
                timestamp,
                seq_num,
                key,
                offset,
            });
        }

        Ok(Self { inner: vec })
    }

    pub fn encode(&self, mut w: impl io::Write) -> anyhow::Result<()> {
        w.write_all(&(self.inner.len() as u32).to_le_bytes())
            .context("record_index.encode: failed to write index len")?;
        for entry in self.inner.iter() {
            w.write_all(&entry.source_id.to_le_bytes())
                .context("record_index.encode: failed to write source_id")?;
            w.write_all(&entry.timestamp.to_le_bytes())
                .context("record_index.encode: failed to write timestamp")?;
            w.write_all(&entry.seq_num.to_le_bytes())
                .context("record_index.encode: failed to write seq_num")?;
            w.write_all(&(entry.key.len() as u32).to_le_bytes())
                .context("record_index.encode: failed to write key len")?;
            w.write_all(entry.key.as_bytes())
                .context("record_index.encode: failed to write key")?;
            w.write_all(&entry.offset.to_le_bytes())
                .context("record_index.encode: failed to write offset")?;
        }
        Ok(())
    }

    /// binary_seek find the nearest byte offset that is <= target by searching the index.
    /// Compares by (source_id, timestamp, key) — intentionally excludes seq_num since
    /// range-start sentinels always carry seq_num=0 and can never match index entries
    /// which carry real seq_nums.
    /// Returns 0 if the index is empty or all entries are > target.
    pub fn binary_seek(&self, target: &Record) -> u64 {
        let Ok((target_sid, target_ts, _target_seq, target_key)) =
            target.extract_all_fields_no_value_ref()
        else {
            return 0;
        };
        let idx = self.inner.binary_search_by(|entry| {
            entry
                .source_id
                .cmp(&target_sid)
                .then(entry.timestamp.cmp(&target_ts))
                .then(entry.key.as_bytes().cmp(target_key))
        });
        match idx {
            Ok(i) => self.inner[i].offset,
            Err(0) => 0,
            Err(i) => self.inner[i - 1].offset,
        }
    }
}

// RecordIndex the actual pointer to a record in an SSTable file.
// Aside from its offset it tracks other metadata so that even
// filtered queries can be executed in O(log N).
#[derive(Debug, Clone)]
pub struct RecordIndex {
    source_id: i64,
    timestamp: i64,
    seq_num: u64,
    key: String,
    offset: u64,
}

impl RecordIndex {
    pub fn cmp_record(&self, other: &Record) -> Option<std::cmp::Ordering> {
        let (sid, ts, seq, key) = other.extract_all_fields_no_value_ref().ok()?;
        Some(
            self.source_id
                .cmp(&sid)
                .then(self.timestamp.cmp(&ts))
                .then(self.seq_num.cmp(&seq))
                .then(self.key.as_bytes().cmp(key)),
        )
    }
}
