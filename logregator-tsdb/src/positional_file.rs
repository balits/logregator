use std::ops::Deref;
use std::os::unix::fs::FileExt;
use std::{io, sync::Arc};

use std::fs::File;

#[derive(Debug)]
pub struct OffsetFile {
    file: Arc<File>,
    offset: u64,
}

impl OffsetFile {
    pub fn new(file: Arc<File>) -> Self {
        Self { file, offset: 0 }
    }

    /// Creates a new instace by cloning the inner `Arc<File>`
    /// and settign the offset to 0.
    #[inline]
    pub fn clone_reset(&self) -> Self {
        Self {
            file: self.file.clone(),
            offset: 0,
        }
    }
}

impl io::Read for OffsetFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }
}

impl Deref for OffsetFile {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}
