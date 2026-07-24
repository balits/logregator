use std::rc::Rc;

use tracing::trace;

use crate::{
    block::Block,
    codec::{self, CodecError, SpecCodec},
    record::{KEY_SIZE, Key, Record},
};

/// A cursor-like iterator over a Block
/// To use the iterator, first it needs to be set to the first element,
/// with [seek_to_first()]. From this point on, one can use next() or seek()
/// to update the cursors internal item, [in_valid()] will return false if this
// fails. It also fails after [next()] results in going past the offset array.
#[derive(Debug)]
pub struct BlockCursor<C> {
    block: Rc<Block>,
    offset_idx: usize,
    record: Result<Option<Record>, CodecError>,
    codec: C,
}

impl<C> BlockCursor<C>
where
    C: SpecCodec<Record> + SpecCodec<Key>,
{
    pub fn new(block: Rc<Block>, c: C) -> Self {
        Self {
            block,
            offset_idx: 0,
            record: Ok(None),
            codec: c,
        }
    }

    #[inline]
    pub fn is_record(&self) -> bool {
        self.current().is_some()
    }

    #[inline]
    pub fn is_error(&self) -> bool {
        self.record.is_err()
    }

    #[inline]
    pub fn get_error(&self) -> Option<&CodecError> {
        self.record.as_ref().err()
    }

    #[inline]
    pub fn unwrap_record(&self) -> &Record {
        match self.record.as_ref() {
            Ok(Some(rec)) => rec,
            Ok(None) => {
                panic!("attempt to unwrap record from cursor while current record is Ok(None)")
            }
            Err(e) => panic!(
                "attempt to unwrap record from cursor while `self.current()` is pointing at error: {e}"
            ),
        }
    }

    #[inline]
    pub fn current(&self) -> Option<&Record> {
        match self.record.as_ref() {
            Ok(o) => o.as_ref(),
            _ => None,
        }
    }

    #[inline]
    pub fn take_current(&mut self) -> Option<Record> {
        match std::mem::replace(&mut self.record, Ok(None)) {
            Ok(Some(o)) => Some(o),
            _ => None,
        }
    }

    #[inline]
    pub fn next(&mut self) {
        self.offset_idx += 1;
        self.update_current();
    }

    #[inline]
    pub fn seek_to_first(&mut self) {
        self.offset_idx = 0;
        self.update_current();
    }

    pub fn seek(&mut self, seek_key: &Key) {
        let mut seek_key_bs = [0u8; KEY_SIZE];
        seek_key.to_be_bytes(&mut seek_key_bs);

        let mut search_err = None;

        // TODO: instead of turning the bytes into Key and then comparing
        // we could just compare the bytes themselves (disregarding Key::stream_id: u64, the last 8 bytes)
        let res = self.block.offsets.binary_search_by(|o| {
            if search_err.is_some() {
                return std::cmp::Ordering::Less; // sentinel
            }

            let offset = *o as usize;
            let key_bs = &self.block.data[offset..offset + KEY_SIZE];
            match <C as SpecCodec<Key>>::decode(&self.codec, key_bs) {
                Ok(Some((key, _))) => key.cmp(seek_key),
                Ok(None) => {
                    search_err = Some(codec::unexpected("failed to decode key from block"));
                    std::cmp::Ordering::Less // meaningless
                }
                Err(e) => {
                    search_err = Some(e);
                    std::cmp::Ordering::Less // meaningless
                }
            }
        });

        if let Some(e) = search_err {
            self.record = Err(e);
        } else {
            self.offset_idx = match res {
                Ok(i) => i,  // exact match
                Err(i) => i, // first elem > target
            };
            self.update_current();
        }
    }

    #[inline]
    pub fn peek(&mut self) -> Option<&Record> {
        match self.record.as_ref() {
            Ok(s) => s.as_ref(),
            Err(_) => None,
        }
    }

    #[inline]
    pub fn peek_key(&mut self) -> Option<&Key> {
        self.peek().map(|r| &r.key)
    }

    fn update_current(&mut self) {
        if self.offset_idx >= self.block.offsets.len() {
            trace!("BlockCursor::update_current: offset idx is out of bounds");
            self.record = Ok(None);
            return;
        }

        let start_offset = self.block.offsets[self.offset_idx] as usize;
        let end_offset = if self.offset_idx + 1 < self.block.offsets.len() {
            self.block.offsets[self.offset_idx + 1] as usize
        } else {
            self.block.offset_segment_start()
        };

        let record_bs = &self.block.data[start_offset..end_offset];
        match self.codec.decode(record_bs) {
            Ok(Some((rec, _))) => {
                trace!("update_current: decode succesfull");
                self.record = Ok(Some(rec));
            }
            Ok(None) => {
                trace!("update_current: decode returned None");
                self.record = Ok(None);
            }
            Err(e) => {
                trace!("update_current: failed to decode Record: {:?}", e);
                self.record = Err(e);
            }
        };
    }
}
