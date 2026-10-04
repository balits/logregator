#![feature(clone_from_ref)]

pub mod block;
pub mod bloom;
pub mod codec;
pub mod counter;
pub mod io;
pub mod label;
pub mod manifest;
pub mod memtable;
pub mod merge_iter;
pub mod record;
pub mod sst;
pub mod wal;

pub mod lsm;

pub type Result<T> = std::result::Result<T, Error>;

/// This is a very simplistic implementation of a custom error type
/// thats a thin pointer (Box<T>) instead of a fat pointer (Box<dyn T>).
/// Fat pointers are two word-size long, so they cant fit
/// neatly into a single CPU registry, but thin pointers perfectly do.
///
/// This is the same optimization (though way dumber)
/// that `anyhow` and `serde_json` uses in their error times.
///
/// For more details, see [ErrorImpl]
///
/// TODO: when we see how frontends actualy consume errors from this crate,
/// maybe it will be time to revisit module level errors of how they wrap
/// std::io::Error into Arcs
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error {
    inner: Box<ErrorImpl>,
}

impl From<ErrorImpl> for Error {
    fn from(value: ErrorImpl) -> Self {
        Error {
            inner: Box::new(value),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorImpl {
    #[error("lsm error: {0}")]
    Message(Box<str>),

    #[error("lsm error (custom): {0}")]
    Other(Box<dyn std::error::Error>),

    #[error("lsm error (codec): {0}")]
    Codec(#[from] codec::CodecError),

    #[error("lsm error (std::io): {0}")]
    StdIo(#[from] std::io::Error),

    #[error("lsm error (sst): {0}")]
    SstError(#[from] sst::SstError),

    #[error("lsm error (io-thread): {0}")]
    IoThreadError(#[from] io::IoError),
}

// sadly i gotta do this manually ):

impl From<codec::CodecError> for Error {
    fn from(e: codec::CodecError) -> Self {
        ErrorImpl::from(e).into()
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        ErrorImpl::from(e).into()
    }
}

impl From<sst::SstError> for Error {
    fn from(e: sst::SstError) -> Self {
        ErrorImpl::from(e).into()
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        let boxed_str = Box::clone_from_ref(s);
        Self {
            inner: Box::new(ErrorImpl::Message(boxed_str)),
        }
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        s.as_str().into()
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Self {
            inner: Box::new(ErrorImpl::Other(e)),
        }
    }
}

impl From<io::IoError> for Error {
    fn from(e: io::IoError) -> Self {
        Self {
            inner: Box::new(ErrorImpl::IoThreadError(e)),
        }
    }
}

// these are some common errors that keep reoccuring,
// so i just moved them the crate root

#[derive(thiserror::Error, Debug, Clone)]
#[error("checksum mismatch {original:#x} (stored) != {computed:#x} (computed)")]
pub struct ChecksumMismatch {
    original: u32,
    computed: u32,
}

/// small module for debugging informations
/// about the codebae, like size / wire length of types
#[cfg(test)]
#[allow(unused)]
mod cli {
    use std::{rc::Rc, sync::Arc};

    use crate::{
        codec::WireLen,
        record::{Key, Record, RecordCodec},
        sst::{SstFileWriter, SstHandle},
    };

    #[test]
    fn layouts() {
        let k = Key::dummy(2u64);
        let rec = record(1, 3);

        println!("SIZEOF key {}", size_of_val(&k));
        println!("SIZEOF record {}", size_of_val(&rec));
        println!("CUSTOM_SIZEOF record {}", rec.size_of());
        println!("WIRE_LEN record {}", rec.wire_len());

        let ssth = sst_handle();
        println!("SIZEOF SstHandle {}", size_of_val(&ssth));
    }

    fn sst_handle() -> SstHandle<RecordCodec> {
        let tempdir = tempfile::TempDir::new().unwrap();

        let codec = RecordCodec;

        let num_records = 512u64;
        let mut sw = SstFileWriter::new(
            1,
            codec,
            None,
            Some(tempdir.path()),
            num_records as usize,
        )
        .unwrap();

        for i in 0..num_records {
            sw.write(&record(i, 512)).unwrap();
        }

        sw.finalize_file().unwrap()
    }

    fn record(i: u64, len: usize) -> Record {
        Record {
            key: crate::record::Key {
                source_id: i,
                timestamp: i,
                sequence_num: i,
                stream_id: i,
            },
            payload: vec![67u8; len].into(),
        }
    }
}
