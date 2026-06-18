use crate::buffer::Buffer;


#[repr(C)]
#[derive(zerocopy::FromBytes)]
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordKey {
    pub ts: u64,
    pub source_id: u32,
    pub seq: u32,
}

#[repr(C)]
#[derive(Debug, Default, zerocopy::FromBytes)]
pub struct RecordMeta {
    pub level: u8,
    pub service_id: u32,
    // host_id: u32,
    // tenant_id: u32,
}

pub struct RecordValue {
    pub meta: RecordMeta,
    pub payload: Buffer,
}
