use bytes::{Buf, BufMut};

use crate::record::{Key, Value};

pub trait Codec: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn decode_key(&self, src: &mut impl Buf) -> Result<Key, Self::Error>;
    fn decode_value(&self, src: &mut impl Buf) -> Result<Value, Self::Error>;

    fn encode_key(&self, key: &Key, dst: &mut impl BufMut) -> Result<(), Self::Error>;
    fn encode_value(&self, value: &Value, dst: &mut impl BufMut) -> Result<(), Self::Error>;

    fn decode_pair(&self, src: &mut impl Buf) -> Result<(Key, Value), Self::Error> {
        let k = self.decode_key(src)?;
        let v = self.decode_value(src)?;
        Ok((k,v))
    }
    
    fn encode_pair(&self, key: &Key, value: &Value, dst: &mut impl BufMut) -> Result<(), Self::Error> {
        self.encode_key(key, dst)?;
        self.encode_value(value, dst)?;
        Ok(())
    }
}

pub struct BytesCodec;

#[derive(Debug, Clone, thiserror::Error)]
pub enum BytesCodecError {
    #[error("not enough bytes found in buffer")]
    NotEnoughBytes
}

impl Codec for BytesCodec {
    type Error = BytesCodecError;

    fn encode_key(&self, key: &Key, dst: &mut impl BufMut) -> Result<(), Self::Error> {
        // Big Endian is the default for put_*
        dst.put_u64(key.source_id);
        dst.put_u64(key.timestamp);
        dst.put_u64(key.sequence_num);
        Ok(())
    }

    fn encode_value(&self, value: &Value, dst: &mut impl BufMut) -> Result<(), Self::Error> {
        dst.put_u32(value.stream_id);
        dst.put_u32(value.payload.len() as u32);
        dst.put_slice(&value.payload);
        Ok(())
    }

    fn decode_key(&self, src: &mut impl Buf) -> Result<Key, Self::Error> {
        if src.remaining() < std::mem::size_of::<Key>() {
            return Err(BytesCodecError::NotEnoughBytes)
        }
        let source_id = src.get_u64();
        let timestamp = src.get_u64();
        let sequence_num = src.get_u64();
        Ok(Key { source_id, timestamp, sequence_num })
    }
    
    fn decode_value(&self, src: &mut impl Buf) -> Result<Value, Self::Error> {
        if src.remaining() < 2 * std::mem::size_of::<u32>() {
            return Err(BytesCodecError::NotEnoughBytes)
        }
        let stream_id = src.get_u32();
        let payload_len = src.get_u32() as usize;
        if src.remaining() < payload_len {
            return Err(BytesCodecError::NotEnoughBytes)
        }
        let payload = src.copy_to_bytes(payload_len);
        Ok(Value { stream_id, payload})
    }
}

#[cfg(test)]
mod test {
    use bytes::{Bytes, BytesMut};

use crate::{codec::{BytesCodec, Codec}, record::{Key, Value}};

    #[test]
    fn lifecylce() {
        let c = BytesCodec;
        let mut sink = BytesMut::with_capacity(4096);
        let key = Key::default();
        let value = Value {
            stream_id: 67,
            payload: Bytes::from_static(b"why was six afraid of seven")
        };

        c.encode_key(&key, &mut sink).expect("encode_key failed");
        let k2 = c.decode_key(&mut sink).expect("decode_key failed");
        assert_eq!(key, k2);

        c.encode_value(&value, &mut sink).expect("encode_value failed");
        let v2 = c.decode_value(&mut sink).expect("decode_value failed");
        assert_eq!(value, v2);

        c.encode_pair(&key, &value, &mut sink).expect("encode_pair failed");
        let (k3, v3) = c.decode_pair(&mut sink).expect("decode_pair failed");

        assert_eq!(key, k3);
        assert_eq!(value, v3);
    }
}