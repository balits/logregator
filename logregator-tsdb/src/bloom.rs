// TODO: No idea how to write this

use std::{fmt::Debug, hash::Hasher};

use fnv::FnvHasher;

use crate::codec::{self, SpecCodec, WireLen};

/// this should not be a problem, as sst::MAX_SST_SIZE is already u32::MAX too
const MAX_BITMAP_SIZE: usize = u32::MAX as usize;
const NUM_CONV_ERR: &str = "BloomFilter: failed to convert between u64 and usize";

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

    pub fn new(n: usize, fpr: f64) -> Self {
        let m = (-(n as f64 * fpr.ln()) / 2f64.ln().powi(2)).ceil() as u64;
        let k = ((m as f64 / (n as f64)) * 2f64.ln()) as u64;
        let bitmap = vec![0; m.div_ceil(8) as usize];
        Self {
            bitmap,
            n_bits: m,
            n_hashes: k.max(1),
        }
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
            .map(|i| h1.wrapping_add(i.wrapping_mul(h2))  % self.n_bits)
            .collect()
    }


    //-------------------------------------------------
    // So i know i have SpecCodec<I> everywhere
    // but i rly dont feel like introducing
    // <B: SpecCodec<BloomFilter> to lsm, or sst
    // or wherever. Plus realistically a bloom
    // filters wire representation is not gonna change.
    //-------------------------------------------------


    /// Allocates a new intermediary buffer, encodes `self` into it
    /// using `BloomFilterCodec` and writes the buffers content to `w`.
    pub fn encode(&self, mut w: impl std::io::Write) -> Result<(), codec::CodecError> {
        let codec = BloomFilterCodec;
        let mut buf = vec![0; self.wire_len()];
        codec.encode(self, &mut buf)?;
        w.write_all(&buf)?;
        Ok(())
    }
}

impl WireLen for BloomFilter {
    #[inline]
    fn wire_len(&self) -> usize {
        size_of_val(&self.n_bits) +
        size_of_val(&self.n_hashes) + 
        size_of::<u32>() + // len prefix 
        self.bitmap.len()
    }
}

/// wire layout:
/// ```not_rust
/// [n_bits: u64][n_hashes: u64][bitmap_len: u64][bitmap: bytes]
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
        dst.copy_from_slice(&(item.n_bits.to_be_bytes()));
        dst.copy_from_slice(&(item.n_hashes.to_be_bytes()));
        if item.bitmap.len() > MAX_BITMAP_SIZE {
            return Err(codec::invalid_payload_sz(item.bitmap.len(), MAX_BITMAP_SIZE, 0));            
        }
        dst.copy_from_slice(&((item.bitmap.len() as u32).to_be_bytes()));
        dst.copy_from_slice(&item.bitmap);

        Ok(item.wire_len())
    }

    fn decode(
        &self,
        src: &[u8],
    ) -> Result<Option<(BloomFilter, usize)>, crate::codec::CodecError> {
        fn nom_decode(input: &[u8]) -> nom::IResult<&[u8], BloomFilter> {
            let (input, n_bits) = nom::number::complete::be_u64(input)?; 
            let (input, n_hashes) = nom::number::complete::be_u64(input)?; 

            let (input, bitmap_len) = nom::number::complete::be_u32(input)?; 
            let (input, bitmap) = nom::bytes::complete::take(bitmap_len)(input)?;

            let bloom = BloomFilter {
                n_bits,
                n_hashes,
                bitmap: bitmap.to_vec()
            };

            Ok((input, bloom))
        }

        let (remaining, bloom) = nom_decode(src).map_err(|e|
            codec::other(format!("nom parsing error: failed to decode bloom filter: {e}"))
        )?;
        let read = src.len() - remaining.len();
        Ok(Some((bloom, read)))
    }
}
