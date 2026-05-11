use std::{
    hash::{BuildHasher, Hasher, RandomState},
    io,
};

use anyhow::Context;

const SALT1: &[u8] = b"bloom_salt_1";
const SALT2: &[u8] = b"bloom_salt_2";

#[derive(Debug, Clone)]
pub(crate) struct BloomFilter {
    bitmap: Vec<u8>,
    n_bits: usize,
    n_hashes: usize,
    rs: RandomState,
}

impl BloomFilter {
    pub(crate) fn new(n: usize, fpr: f64) -> Self {
        let m = (-(n as f64 * fpr.ln()) / 2f64.ln().powi(2)).ceil() as usize;
        let k = ((m as f64 / (n as f64)) * 2f64.ln()) as usize;
        Self {
            bitmap: vec![0; m.div_ceil(8)],
            n_bits: m,
            n_hashes: k.max(1),
            rs: RandomState::new(),
        }
    }

    pub(crate) fn positions(&self, source_id: i64, key: &[u8]) -> Vec<usize> {
        let mut hbuf = Vec::with_capacity(8 + key.len());
        hbuf.extend_from_slice(&source_id.to_le_bytes());
        hbuf.extend_from_slice(key);
        let h1 = self.hash_with_salt(&hbuf, SALT1);
        let h2 = self.hash_with_salt(&hbuf, SALT2);
        (0..(self.n_hashes as u64))
            .map(|i| h1.wrapping_add(i.wrapping_mul(h2)) as usize % self.n_bits)
            .collect()
    }

    pub(crate) fn insert(&mut self, source_id: i64, key: &[u8]) {
        for pos in self.positions(source_id, key) {
            self.set_bit(pos);
        }
    }

    pub(crate) fn contains(&self, source_id: i64, key: &[u8]) -> bool {
        self.positions(source_id, key).into_iter().all(|pos| self.get_bit(pos))
    }

    /// <code>[n_bits u64][n_hashes u64][bf_len u64][bf_content [u8; bf_len]]</code>
    ///
    /// NOTE: this does not append the bloom_filter start offset
    pub(crate) fn encode<W: io::Write>(&self, mut w: W) -> anyhow::Result<()> {
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
    pub(crate) fn decode<R: io::Read>(mut r: R) -> anyhow::Result<Self> {
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
