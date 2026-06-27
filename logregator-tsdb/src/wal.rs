use std::{fs::File, io::{self, Write}};

use bytes::BytesMut;
use tracing::trace;

use crate::record::{Key, Value};
use crate::codec::{BytesCodec, Codec};

pub struct Wal<C: Codec = BytesCodec> {
    f: File,
    buf: BytesMut,
    codec: C,
}

impl<C: Codec> Wal<C> {
    const DEFAULT_BUF_SIZE: usize = 2 * 1024;

    pub fn new(f: File, codec: C) -> Self {
        let buf = BytesMut::with_capacity(Self::DEFAULT_BUF_SIZE);
        Self { f, buf, codec }
    }

    pub fn append(&mut self, key: &Key, value: &Value) -> io::Result<()> {
        self.buf.clear();
        self.codec
            .encode_pair(key, value, &mut self.buf)
            .map_err(|e| {
                trace!("wal: failed to encode key-value");
                io::Error::other(e)
            })?;
        self.f.write_all(&self.buf)
            .map_err(|e| {
                trace!("wal: failed to write to file");
                e
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
use tempfile::tempfile;

use crate::{codec::BytesCodec, record::{Key, Value}, wal::Wal};
    #[test]
    fn lifecylce() {
        let f = tempfile().expect("tempfile failed");
        let c = BytesCodec;
        let mut w = Wal::new(f, c);

        let k = Key::default();
        let mut v = Value {
            stream_id: 0,
            payload: Bytes::from_static(b"foobarbaz")
        };
        for _ in 0..100 {
            v.stream_id += 1;
            w.append(&k, &v).expect("append failed");
        }
    }
}