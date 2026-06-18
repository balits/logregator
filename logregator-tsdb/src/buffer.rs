#[cfg(not(feature = "bytes-buffer"))]
use vec_buffer::BufferImpl;
#[cfg(feature = "bytes-buffer")]
use bytes_buffer::BufferImpl;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct Buffer {
    inner: BufferImpl,
}

impl Buffer {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn copy_from_slice(src: &[u8]) -> Self {
        Self { inner: BufferImpl::copy_from_slice(src)}
    }

    pub fn slice_owned(&self, start: usize, end: usize) -> Self {
        Self {inner: self.inner.slice_owned(start, end) }
    }

    pub fn slice_borrowed(&self, start: usize, end: usize) -> &[u8] {
        self.inner.slice_borrowed(start, end)
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl std::ops::Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    } 
}


#[cfg(not(feature = "bytes-buffer"))]
mod vec_buffer {
    #[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
    pub(super) struct BufferImpl(Vec<u8>);

    impl BufferImpl {
        pub fn len(&self) -> usize {
            self.0.len()
        }

        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }

        pub fn copy_from_slice(src: &[u8]) -> Self {
            BufferImpl(Vec::from(src))
        }

        pub fn slice_owned(&self, start: usize, end: usize) -> Self {
            BufferImpl(self.0[start..end].to_vec())
        }

        pub fn slice_borrowed(&self, start: usize, end: usize) -> &[u8] {
            &self.0[start..end]
        }
    }

    impl AsRef<[u8]> for BufferImpl {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }

    impl std::ops::Deref for BufferImpl {
        type Target = [u8];
        fn deref(&self) -> &Self::Target {
            self.0.deref()
        } 
    }
}

#[cfg(feature = "bytes-buffer")]
mod bytes_buffer {

    #[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
    pub(super) struct BufferImpl(bytes::Bytes);

    impl BufferImpl {
        pub fn len(&self) -> usize {
            self.0.len()
        }

        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }

        pub fn copy_from_slice(src: &[u8]) -> Self {
            BufferImpl(bytes::Bytes::copy_from_slice(src))
        }
        pub fn slice_owned(&self, start: usize, end: usize) -> Self {
            BufferImpl(self.0.slice(start..end))
        }

        pub fn slice_borrowed(&self, start: usize, end: usize) -> &[u8] {
            let bs = self.0.as_slice();
            &bs[start..end]
        }
    }

    impl AsRef<[u8]> for BufferImpl {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }

    impl std::ops::Deref for BufferImpl {
        type Target = [u8];
        fn deref(&self) -> &Self::Target {
            self.0.deref()
        } 
    }
}