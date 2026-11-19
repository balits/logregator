// TODO: No idea how to write this

use std::{fmt::Debug, hash::Hasher};

use fnv::FnvHasher;

use crate::codec::{self, SpecCodec, WireLen};

/// this should not be a problem, as sst::MAX_SST_SIZE is already u32::MAX too
const MAX_BITMAP_SIZE: usize = u32::MAX as usize;
const NUM_CONV_ERR: &str =
    "BloomFilter: failed to convert between u64 and usize";

/// this is used to do double hashing with a fixed polynomial, or something
fn hash_with_salt(data: &[u8], salt: &[u8]) -> u64 {
    let mut hasher = FnvHasher::default();
    hasher.write(data);
    hasher.write(salt);
    hasher.finish()
}

/// A bloom filter that answers the question:
/// "Does this SST contain any records for stream_id X?"
pub struct BloomFilter {
    bitmap: Vec<u8>,
    n_bits: u64,
    n_hashes: u64,
}

impl Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter").finish_non_exhaustive()
    }
}

impl BloomFilter {
    const SALT1: &[u8] = b"bloom_salt_1";
    const SALT2: &[u8] = b"bloom_salt_2";

    /// Returns a new bloom filter given the expected number of
    /// entries `n`, and the false positive probability `fpr`.
    ///
    /// `n` positive non-zero, while `fpr` must be withing [0.0, 1.0),
    /// otherwise `None` is returned.
    pub fn new(n: usize, fpr: f64) -> Option<Self> {
        if n == 0 {
            return None;
        }

        if !(0.0..1.0).contains(&fpr) {
            return None;
        }

        let n_bits = (-(n as f64 * fpr.ln()) / 2f64.ln().powi(2)).ceil() as u64;
        let k = ((n_bits as f64 / (n as f64)) * 2f64.ln()) as u64;
        let bitmap = vec![0; n_bits.div_ceil(8) as usize];
        Some(Self {
            bitmap,
            n_bits,
            n_hashes: k.max(1),
        })
    }

    pub fn may_contain(&self, stream_id: u64) -> bool {
        self.positions(stream_id)
            .into_iter()
            .all(|pos| self.get_bit(pos.try_into().expect(NUM_CONV_ERR)))
    }

    pub fn insert(&mut self, stream_id: u64) {
        for pos in self.positions(stream_id) {
            self.set_bit(pos.try_into().expect(NUM_CONV_ERR));
        }
    }

    #[inline]
    fn set_bit(&mut self, pos: usize) {
        self.bitmap[pos >> 3] |= 1 << (pos & 7)
    }

    #[inline]
    fn get_bit(&self, pos: usize) -> bool {
        (self.bitmap[pos >> 3] >> (pos & 7)) & 1 == 1
    }

    fn positions(&self, stream_id: u64) -> Vec<u64> {
        let bytes = stream_id.to_be_bytes();
        let h1 = hash_with_salt(&bytes, Self::SALT1);
        let h2 = hash_with_salt(&bytes, Self::SALT2);

        (0..self.n_hashes)
            .map(|i| h1.wrapping_add(i.wrapping_mul(h2)) % self.n_bits)
            .collect()
    }
}

impl WireLen for BloomFilter {
    #[inline]
    fn wire_len(&self) -> usize {
        size_of_val(&self.n_bits)
            + size_of_val(&self.n_hashes)
            + size_of::<u32>()
            + self.bitmap.len()
    }
}

/// wire layout:
/// ```not_rust
/// [n_bits: u64][n_hashes: u64][bitmap_len: u32][bitmap: bytes]
/// ```
#[derive(Debug, Clone)]
pub struct BloomFilterCodec;

impl SpecCodec<BloomFilter> for BloomFilterCodec {
    fn encode(
        &self,
        item: &BloomFilter,
        dst: &mut [u8],
    ) -> Result<usize, codec::CodecError> {
        if dst.len() < item.wire_len() {
            return Err(codec::not_enough_bytes(dst.len(), item.wire_len()));
        }
        dst[0..8].copy_from_slice(&(item.n_bits.to_be_bytes()));
        dst[8..16].copy_from_slice(&(item.n_hashes.to_be_bytes()));
        if item.bitmap.len() > MAX_BITMAP_SIZE {
            return Err(codec::invalid_payload_sz(
                item.bitmap.len(),
                MAX_BITMAP_SIZE,
                0,
            ));
        }
        dst[16..20]
            .copy_from_slice(&((item.bitmap.len() as u32).to_be_bytes()));
        dst[20..20 + item.bitmap.len()].copy_from_slice(&item.bitmap);

        Ok(item.wire_len())
    }

    fn decode(
        &self,
        src: &[u8],
    ) -> Result<Option<(BloomFilter, usize)>, crate::codec::CodecError> {
        fn nom_decode(
            input: &[u8],
        ) -> nom::IResult<&[u8], (u64, u64, Vec<u8>)> {
            let (input, n_bits) = nom::number::complete::be_u64(input)?;
            let (input, n_hashes) = nom::number::complete::be_u64(input)?;

            let (input, bitmap_len) = nom::number::complete::be_u32(input)?;
            let (input, bitmap) =
                nom::bytes::complete::take(bitmap_len)(input)?;

            Ok((input, (n_bits, n_hashes, bitmap.to_vec())))
        }

        let decode_res = nom_decode(src).map_err(|e| {
            codec::other(format!(
                "bloom filter: nom parsing error: failed to decode bloom filter: {e}"
            ))
        })?;

        let (remaining, (n_bits, n_hashes, bitmap)) = decode_res;

        if n_bits == 0 {
            return Err(codec::other(
                "bloom filter: n_bits must be greater than 0",
            ));
        }

        if n_hashes == 0 {
            return Err(codec::other(
                "bloom filter: n_hashes must be greater than 0",
            ));
        }

        let expected_len = n_bits.div_ceil(8) as usize;
        let got_len = bitmap.len();
        if got_len != expected_len {
            return Err(codec::other(format!(
                "bloom filter: bitmap size mismatch: expected {expected_len}, got {got_len}"
            )));
        }

        let read = src.len() - remaining.len();
        let bloom = BloomFilter {
            bitmap,
            n_bits,
            n_hashes,
        };
        Ok(Some((bloom, read)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter() -> BloomFilter {
        BloomFilter::new(100, 0.01).unwrap()
    }

    /// Allocates a new intermediary buffer, encodes `f` into it
    /// using `BloomFilterCodec` and writes the buffers content to `w`.
    fn encode_to(
        f: &BloomFilter,
        mut w: impl std::io::Write,
    ) -> Result<(), codec::CodecError> {
        let codec = BloomFilterCodec;
        let mut buf = vec![0; f.wire_len()];
        codec.encode(f, &mut buf)?;
        w.write_all(&buf)?;
        Ok(())
    }

    #[test]
    fn empty_filter_contains_nothing() {
        let f = filter();
        for id in 0..100 {
            assert!(!f.may_contain(id));
        }
    }

    #[test]
    fn inserted_items_are_contained() {
        let mut f = filter();
        for id in 0..20 {
            f.insert(id);
        }
        for id in 0..20 {
            assert!(f.may_contain(id), "missing id {id}");
        }
        for id in 100..200 {
            assert!(
                !f.may_contain(id),
                "false positive for {id} (FPR is ~1e-5, so 100 misses should be clean)"
            );
        }
    }

    #[test]
    fn codec_roundtrip_preserves_filter() {
        let mut f = filter();
        let ids = [1u64, 7, 42, 99];
        for &id in &ids {
            f.insert(id);
        }

        let mut buf = Vec::new();
        encode_to(&f, &mut buf).unwrap();
        assert_eq!(buf.len(), f.wire_len());

        let (decoded, read) = BloomFilterCodec.decode(&buf).unwrap().unwrap();
        assert_eq!(read, buf.len());
        assert_eq!(decoded.n_bits, f.n_bits);
        assert_eq!(decoded.n_hashes, f.n_hashes);
        assert_eq!(decoded.bitmap, f.bitmap);
        for &id in &ids {
            assert!(decoded.may_contain(id));
        }
        assert!(!decoded.may_contain(1234));
    }

    #[test]
    fn encode_accepts_oversized_destination() {
        let f = filter();
        let mut buf = vec![0xAA; f.wire_len() + 4];
        let written = BloomFilterCodec.encode(&f, &mut buf).unwrap();
        assert_eq!(written, f.wire_len());
        assert!(buf[written..].iter().all(|&b| b == 0xAA));

        let (_, read) = BloomFilterCodec.decode(&buf).unwrap().unwrap();
        assert_eq!(read, written);
    }

    #[test]
    fn encode_rejects_short_destination() {
        let f = filter();
        let mut buf = vec![0; f.wire_len() - 1];
        assert!(BloomFilterCodec.encode(&f, &mut buf).is_err());
    }

    #[test]
    fn wire_header_matches_documented_layout() {
        let mut f = filter();
        f.insert(3);
        let mut buf = vec![0; f.wire_len()];
        BloomFilterCodec.encode(&f, &mut buf).unwrap();

        assert_eq!(&buf[0..8], &f.n_bits.to_be_bytes()[..]);
        assert_eq!(&buf[8..16], &f.n_hashes.to_be_bytes()[..]);
        assert_eq!(&buf[16..20], &(f.bitmap.len() as u32).to_be_bytes()[..]);
        assert_eq!(&buf[20..], &f.bitmap[..]);
    }
}
