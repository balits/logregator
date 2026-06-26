#[repr(C)]
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub source_id: u32,
    pub ts: u64,
    pub seq: u64,
}

impl Key {
    #[inline]
    pub fn sizeof() -> usize {
        // TODO: leave as it or reuse my older WireLen trait + derive macro (overkill)?
        20
    }
}

#[repr(C)]
#[derive(Debug, Default, Clone)]
pub struct Value {
    pub label_id: u32,
    pub payload: bytes::Bytes,
}

// TODO: Add labels

// #[repr(C)]
// #[derive(Debug, Default, zerocopy::FromBytes)]
// pub struct RecordMeta {
//     pub level: u8,
//     pub service_id: u32,
//     // host_id: u32,
//     // tenant_id: u32,
// }

// label_id -> LabelSet -> Label { service_id, ... }


impl Value {
    #[inline]
    pub fn sizeof(&self) -> usize {
        // TODO: leave as it or reuse my older WireLen trait + derive macro (overkill)?
        4+self.payload.len()
    }
}