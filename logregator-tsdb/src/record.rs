#[repr(C)]
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub source_id: u64,
    pub timestamp: u64,
    pub sequence_num: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Value {
    pub stream_id: u32,
    pub payload: bytes::Bytes,
}

impl Value {
    #[inline]
    pub fn sizeof(&self) -> usize {
        4 + self.payload.len()
    }
}
