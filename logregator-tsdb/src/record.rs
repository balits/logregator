use std::{borrow::Borrow, cmp::Ordering, fmt::Debug, mem::size_of};

use crate::codec::CodecError;

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
            .field("payload", &format_args!("[0..{}]", self.payload.len()))
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

impl Record {
    /// Returns the resident memory used by this record in bytes.
    /// This should equal [key: 4 * 8 bytes] [boxed_slice: 8 + 8 bytes] [1 byte * payload_len].
    #[inline]
    pub const fn size_of(&self) -> usize {
        size_of::<Self>() + self.payload.len()
    }

    /// Returns the size of this record as its serialized to bytes.
    /// This should equal [key: 4 * 8 bytes] [payload_len: 4 bytes] [1 byte * payload_len].
    #[inline]
    pub const fn wire_len(&self) -> usize {
        size_of::<Key>() + size_of_val(&(self.payload.len() as u32)) + self.payload.len()
    }
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
#[derive(Debug, Clone, Default)]
pub struct Key {
    // fields for keys, hashing, eq, and ord
    pub source_id: u64,
    pub timestamp: u64,
    pub sequence_num: u64,

    // stream_id is not used for any of the above
    pub stream_id: u64,
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.source_id == other.source_id
            && self.timestamp == other.timestamp
            && self.sequence_num == other.timestamp
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.source_id
            .cmp(&other.source_id)
            .then(self.timestamp.cmp(&other.timestamp))
            .then(self.sequence_num.cmp(&other.sequence_num))
    }
}

impl Key {
    pub fn new(source_id: u64, timestamp: u64, sequence_num: u64, stream_id: u64) -> Self {
        Self {
            source_id,
            timestamp,
            sequence_num,
            stream_id,
        }
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
