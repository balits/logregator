use std::{borrow::Borrow, cmp::Ordering, fmt::Debug};

#[repr(C)]
#[derive(Clone)]
pub struct Record {
    pub key: Key,
    pub payload: Vec<u8>,
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
pub const MAX_PAYLOAD_LENGTH: usize = 4096;

impl Record {
    /// Returns the size of `self` in bytes.
    #[inline]
    pub const fn sizeof(&self) -> usize {
        std::mem::size_of::<Key>() + self.payload.len()
    }

    /// Returns the wire length of `self` in bytes, including
    /// the payloads length prefix used in encoding/decoding.
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
        self.key.partial_cmp(&other.key)
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

#[repr(C)]
#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Eq, Ord)]
pub struct Key {
    // fields for keys, hashing, eq, and ord
    pub source_id: u64,
    pub timestamp: u64,
    pub sequence_num: u64,

    // stream_id is not used for any of the above
    pub stream_id: u64,
}

impl Borrow<Key> for Record {
    fn borrow(&self) -> &Key {
        &self.key
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

    pub fn from_be_bytes(src: &[u8]) -> Option<Self> {
        if src.len() < KEY_SIZE {
            None
        } else {
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

            Some(Key {
                source_id,
                timestamp,
                sequence_num,
                stream_id,
            })
        }
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
