use std::{borrow::Borrow, cmp::Ordering, fmt::Debug, io, mem::size_of};

use tracing::trace;

use crate::codec::*;

#[repr(C)]
#[derive(Clone)]
pub struct Record {
    pub key: Key,
    pub payload: Box<[u8]>,
}

impl Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("key", &self.key)
            .field("payload", &format_args!("[..{}]", self.payload.len()))
            .finish()
    }
}

pub const KEY_SIZE: usize = std::mem::size_of::<Key>();
pub const PAYLOAD_LEN_SIZE: usize = 4; // vec.len() as u32

pub const MIN_PAYLOAD_LENGTH: usize = 1;
pub const MAX_PAYLOAD_LENGTH: usize = 4096;

/// [KEY_SIZE]+ u32 as the payloads length prefix + [MIN_PAYLOAD_LENGTH]
pub const MIN_RECORD_WIRE_LENGTH: usize = KEY_SIZE + size_of::<u32>() + MIN_PAYLOAD_LENGTH;
/// [KEY_SIZE]+ u32 as the payloads length prefix + [MAX_PAYLOAD_LENGTH]
pub const MAX_RECORD_WIRE_LENGTH: usize = KEY_SIZE + size_of::<u32>() + MAX_PAYLOAD_LENGTH;

impl WireLen for Record {
    #[inline]
    fn wire_len(&self) -> usize {
        self.key.wire_len() + size_of::<u32>() + self.payload.len()
    }
}

impl Record {
    /// Returns the resident memory used by this record in bytes.
    /// This should equal [key: 4 * 8 bytes] [boxed_slice: 8 + 8 bytes] [1 byte * payload_len].
    #[inline]
    pub const fn size_of(&self) -> usize {
        size_of::<Self>() + self.payload.len()
    }

    // /// Returns the size of this record as its serialized to bytes.
    // /// This should equal [key: 4 * 8 bytes] [payload_len: 4 bytes] [1 byte * payload_len].
    // #[inline]
    // pub const fn wire_len(&self) -> usize {
    //     KEY_SIZE + size_of::<u32>() + self.payload.len()
    // }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl Eq for Record {}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl Borrow<Key> for Record {
    fn borrow(&self) -> &Key {
        &self.key
    }
}

#[repr(C)]
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub stream_id: u64,
    pub timestamp: u64,
    pub sequence_num: u64,
    pub source_id: u64,
}

impl WireLen for Key {
    #[inline]
    fn wire_len(&self) -> usize {
        KEY_SIZE
    }
}

// impl PartialEq for Key {
//     fn eq(&self, other: &Self) -> bool {
//         self.source_id == other.source_id
//             && self.timestamp == other.timestamp
//             && self.sequence_num == other.sequence_num
//     }
// }

// impl Eq for Key {}

// impl PartialOrd for Key {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl Ord for Key {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.source_id
//             .cmp(&other.source_id)
//             .then(self.timestamp.cmp(&other.timestamp))
//             .then(self.sequence_num.cmp(&other.sequence_num))
//     }
// }

impl Key {
    pub fn new(source_id: u64, timestamp: u64, sequence_num: u64, stream_id: u64) -> Self {
        Self {
            source_id,
            timestamp,
            sequence_num,
            stream_id,
        }
    }

    #[cfg(test)]
    pub fn dummy(i: impl Into<u64>) -> Self {
        let i = i.into();
        Self::new(i, i, i, i)
    }

    pub fn from_be_bytes(src: &[u8]) -> Result<Self, CodecError> {
        if src.len() < KEY_SIZE {
            return Err(CodecError::UnexpectedSize(crate::codec::UnexpectedSize {
                got: src.len(),
                want: KEY_SIZE,
            }));
        }

        let source_id = u64::from_be_bytes([
            src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
        ]);
        let timestamp = u64::from_be_bytes([
            src[8], src[9], src[10], src[11], src[12], src[13], src[14], src[15],
        ]);
        let sequence_num = u64::from_be_bytes([
            src[16], src[17], src[18], src[19], src[20], src[21], src[22], src[23],
        ]);
        let stream_id = u64::from_be_bytes([
            src[24], src[25], src[26], src[27], src[28], src[29], src[30], src[31],
        ]);

        Ok(Key {
            source_id,
            timestamp,
            sequence_num,
            stream_id,
        })
    }

    pub fn to_be_bytes(&self, dst: &mut [u8; 32]) {
        dst[0..8].copy_from_slice(&self.source_id.to_be_bytes());
        dst[8..16].copy_from_slice(&self.timestamp.to_be_bytes());
        dst[16..24].copy_from_slice(&self.sequence_num.to_be_bytes());
        dst[24..32].copy_from_slice(&self.stream_id.to_be_bytes());
    }
}

/// More concretely, it implements both [SpecCodec<Record>] and [SpecCodec<Key>]
#[derive(Debug, Clone, Copy)]
pub struct RecordCodec;

impl SpecCodec<Key> for RecordCodec {
    // #[instrument(ret)]
    fn encode(&self, key: &Key, dst: &mut [u8]) -> Result<usize, CodecError> {
        if dst.len() < key.wire_len() {
            return Err(CodecError::UnexpectedSize(UnexpectedSize {
                got: dst.len(),
                want: key.wire_len(),
            }));
        }
        key.to_be_bytes(dst.try_into().map_err(io::Error::other)?);
        // info!(dst);
        Ok(KEY_SIZE)
    }

    fn decode(&self, src: &[u8]) -> Result<Option<(Key, usize)>, CodecError> {
        if src.len() < KEY_SIZE {
            trace!(
                "not enough bytes to decode from (got = {}, want = {})",
                src.len(),
                KEY_SIZE
            );
            return Ok(None);
        }
        let k = Key::from_be_bytes(&src[..KEY_SIZE])?;
        Ok(Some((k, KEY_SIZE)))
    }
}

impl SpecCodec<Record> for RecordCodec {
    fn encode(&self, rec: &Record, dst: &mut [u8]) -> Result<usize, CodecError> {
        if dst.len() < rec.wire_len() {
            trace!(
                "not enough bytes to encode into `dst` (got: {}, want: {})",
                dst.len(),
                rec.wire_len()
            );
            return Err(not_enough_bytes(dst.len(), rec.wire_len()));
        }

        let _ = <Self as SpecCodec<Key>>::encode(self, &rec.key, &mut dst[0..KEY_SIZE])?;

        if !(MIN_PAYLOAD_LENGTH..=MAX_PAYLOAD_LENGTH).contains(&rec.payload.len()) {
            return Err(invalid_payload_sz(rec.payload.len()));
        }

        let payload_len = (rec.payload.len() as u32).to_be_bytes();
        dst[KEY_SIZE..KEY_SIZE + PAYLOAD_LEN_SIZE].copy_from_slice(&payload_len);
        dst[KEY_SIZE + PAYLOAD_LEN_SIZE..KEY_SIZE + PAYLOAD_LEN_SIZE + rec.payload.len()]
            .copy_from_slice(&rec.payload);

        Ok(rec.wire_len())
    }

    fn decode(&self, src: &[u8]) -> Result<Option<(Record, usize)>, CodecError> {
        if src.len() < KEY_SIZE + PAYLOAD_LEN_SIZE {
            trace!(
                "not enough bytes to decode from (src.len = {}, KEY_SIZE + PAYLOAD_LEN_SIZE = {})",
                src.len(),
                KEY_SIZE + PAYLOAD_LEN_SIZE
            );
            return Ok(None);
        }

        let (key, _) = match <Self as SpecCodec<Key>>::decode(self, src)? {
            None => return Ok(None),
            Some(t) => t,
        };

        let payload_len = u32::from_be_bytes([
            src[KEY_SIZE],
            src[KEY_SIZE + 1],
            src[KEY_SIZE + 2],
            src[KEY_SIZE + 3],
        ]) as usize;

        if !(MIN_PAYLOAD_LENGTH..=MAX_PAYLOAD_LENGTH).contains(&payload_len) {
            return Err(invalid_payload_sz(payload_len));
        }
        let total = KEY_SIZE + PAYLOAD_LEN_SIZE + payload_len;
        if src.len() < total {
            trace!("not enoguh bytes to decode from (payload)");
            return Ok(None);
        }

        let payload = src[KEY_SIZE + PAYLOAD_LEN_SIZE..total]
            .to_vec()
            .into_boxed_slice();

        Ok(Some((Record { key, payload }, total)))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn key_as_bytes_from_bytes() {
        let key = Key::new(1, 2, 3, 4);
        // should be [0,0,0,0,0,0,0,1, 0,0,0,0,0,0,0,2, 0,0,0,0,0,0,0,3, 0,0,0,0,0,0,0,4]
        let mut bytes = [0u8; 32];
        key.to_be_bytes(&mut bytes);
        assert_eq!(std::mem::size_of_val(&key), bytes.len());
        let key2 = Key::from_be_bytes(&bytes).expect("failed to parse bytes into Key");
        assert_eq!(key, key2);
    }
}
