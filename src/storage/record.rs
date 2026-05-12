use std::cmp;

use anyhow::{Context, bail};

/// Record represents each entry in the SSTables as a byte vector.
/// [source_id: i64] | [timestamp: i64] | [seq_num: u64] | [key_len: u32] | [key_content: [u8; key_len]] | [value_len: u32] | [value_content: [u8; value_len]]
///
/// - source_id is the logs producer clients id
/// - timestamp is the unix nano timestamp
/// - seq_num is the global monotonic revision number issued by the storage engine
/// - key and value are the logs key and value
#[derive(Clone, Debug)]
pub struct Record(Vec<u8>);

impl Record {
    pub fn from_raw_parts(source_id: i64, timestamp: i64, seq_num: u64, key: &str, value: &str) -> Self {
        let len = 8 + 8 + 8 + 4 + key.len() + 4 + value.len();
        let mut v = Vec::with_capacity(len);
        v.extend_from_slice(&source_id.to_be_bytes());
        v.extend_from_slice(&timestamp.to_be_bytes());
        v.extend_from_slice(&seq_num.to_be_bytes());
        v.extend_from_slice(&(key.len() as u32).to_be_bytes());
        v.extend_from_slice(key.as_bytes());
        v.extend_from_slice(&(value.len() as u32).to_be_bytes());
        v.extend_from_slice(value.as_bytes());
        Record(v)
    }

    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Record(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn extract_source_id(&self) -> anyhow::Result<i64> {
        if self.0.len() < 8 {
            bail!("record.extract: not enough bytes for source_id")
        }
        let i = i64::from_be_bytes([
            self.0[0], self.0[1], self.0[2], self.0[3],
            self.0[4], self.0[5], self.0[6], self.0[7],
        ]);
        Ok(i)
    }

    pub fn extract_timestamp(&self) -> anyhow::Result<i64> {
        if self.0.len() < 16 {
            bail!("record.extract: not enough bytes for timestamp")
        }
        let i = i64::from_be_bytes([
            self.0[8], self.0[9], self.0[10], self.0[11],
            self.0[12], self.0[13], self.0[14], self.0[15],
        ]);
        Ok(i)
    }

    pub fn extract_seq_num(&self) -> anyhow::Result<u64> {
        if self.0.len() < 24 {
            bail!("record.extract: not enough bytes for seq_num")
        }
        let i = u64::from_be_bytes([
            self.0[16], self.0[17], self.0[18], self.0[19],
            self.0[20], self.0[21], self.0[22], self.0[23],
        ]);
        Ok(i)
    }

    pub fn extract_key(&self) -> anyhow::Result<&[u8]> {
        if self.0.len() < 28 {
            bail!("record.extract: not enough bytes for key length")
        }
        let key_len = u32::from_be_bytes([
            self.0[24], self.0[25], self.0[26], self.0[27],
        ]) as usize;
        if self.0.len() < 28 + key_len {
            bail!("record.extract: not enough bytes for key contents")
        }
        Ok(&self.0[28..28 + key_len])
    }

    pub fn extract_value(&self) -> anyhow::Result<&[u8]> {
        if self.0.len() < 28 {
            bail!("record.extract: not enough bytes for key length")
        }
        let key_len = u32::from_be_bytes([
            self.0[24], self.0[25], self.0[26], self.0[27],
        ]) as usize;
        if self.0.len() < 28 + key_len {
            bail!("record.extract: not enough bytes for key contents")
        }

        if self.0.len() < 32 + key_len {
            bail!("record.extract: not enough bytes for value length")
        }
        let value_len = u32::from_be_bytes([
            self.0[28 + key_len], self.0[29 + key_len], self.0[30 + key_len], self.0[31 + key_len],
        ]) as usize;
        if self.0.len() < 32 + key_len + value_len {
            bail!("record.extract: not enough bytes for value contents")
        }

        Ok(&self.0[32 + key_len..(32 + key_len + value_len)])
    }

    pub fn extract_all_fields_owned(&self) -> anyhow::Result<(i64, i64, u64, String, String)> {
        let (sid, ts, seq, _key, _val) = self.extract_all_fields_ref()
            .context("extract_all_fields_owned: failed to extract fields")?;
        let key = String::from_utf8(_key.to_vec())
            .context("extract_all_fields_owned: failed to decode key as utf-8")?;
        let value = String::from_utf8(_val.to_vec())
            .context("extract_all_fields_owned: failed to decode value as utf-8")?;
        Ok((sid, ts, seq, key, value))
    }

    pub fn extract_all_fields_ref<'a>(&'a self) -> anyhow::Result<(i64, i64, u64, &'a [u8], &'a [u8])> {
        if self.0.len() < 24 {
            bail!("record.extract_all: not enough bytes for integer fields")
        }
        let source_id = i64::from_be_bytes([
            self.0[0], self.0[1], self.0[2], self.0[3],
            self.0[4], self.0[5], self.0[6], self.0[7],
        ]);
        let timestamp = i64::from_be_bytes([
            self.0[8], self.0[9], self.0[10], self.0[11],
            self.0[12], self.0[13], self.0[14], self.0[15],
        ]);
        let seq = u64::from_be_bytes([
            self.0[16], self.0[17], self.0[18], self.0[19],
            self.0[20], self.0[21], self.0[22], self.0[23],
        ]);

        if self.0.len() < 28 {
            bail!("record.extract_all: not enough bytes for key length")
        }
        let key_len = u32::from_be_bytes([
            self.0[24], self.0[25], self.0[26], self.0[27],
        ]) as usize;
        if self.0.len() < 28 + key_len {
            bail!("record.extract_all: not enough bytes for key contents")
        }
        let key = &self.0[28..28 + key_len];

        if self.0.len() < 32 + key_len {
            bail!("record.extract_all: not enough bytes for value length")
        }
        let value_len = u32::from_be_bytes([
            self.0[28 + key_len], self.0[29 + key_len], self.0[30 + key_len], self.0[31 + key_len],
        ]) as usize;
        if self.0.len() < 32 + key_len + value_len {
            bail!("record.extract_all: not enough bytes for value contents")
        }

        let value = &self.0[32 + key_len..(32 + key_len + value_len)];
        Ok((source_id, timestamp, seq, key, value))
    }

    pub fn extract_all_fields_no_value_ref<'a>(&'a self) -> anyhow::Result<(i64, i64, u64, &'a [u8])> {
        if self.0.len() < 24 {
            bail!("record.extract_all: not enough bytes for integer fields")
        }
        let source_id = i64::from_be_bytes([
            self.0[0], self.0[1], self.0[2], self.0[3],
            self.0[4], self.0[5], self.0[6], self.0[7],
        ]);
        let timestamp = i64::from_be_bytes([
            self.0[8], self.0[9], self.0[10], self.0[11],
            self.0[12], self.0[13], self.0[14], self.0[15],
        ]);
        let seq = u64::from_be_bytes([
            self.0[16], self.0[17], self.0[18], self.0[19],
            self.0[20], self.0[21], self.0[22], self.0[23],
        ]);

        if self.0.len() < 28 {
            bail!("record.extract_all: not enough bytes for key length")
        }
        let key_len = u32::from_be_bytes([
            self.0[24], self.0[25], self.0[26], self.0[27],
        ]) as usize;
        if self.0.len() < 28 + key_len {
            bail!("record.extract_all: not enough bytes for key contents")
        }
        let key = &self.0[28..28 + key_len];
        Ok((source_id, timestamp, seq, key))
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl Eq for Record {}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.0[0..8].cmp(&other.0[0..8])
            .then(self.0[8..16].cmp(&other.0[8..16]))
            .then(self.0[16..24].cmp(&other.0[16..24]))
            .then(self.extract_key()
                .expect("record.partial_cmp: failed to extract keys of self")
                .cmp(
                    other.extract_key()
                    .expect("record.partial_cmp: failed to extract keys of other")
                )
            )
        )
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}