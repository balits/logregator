use std::sync::Arc;

use tracing::trace;

use crate::{
    block::Block,
    codec::Codec,
    record::{KEY_SIZE, Key, Record},
};

/// A cursor-like iterator over a Block
/// To use the iterator, first it needs to be set to the first element,
/// with [seek_to_first()]. From this point on, one can use next() or seek()
/// to update the cursors internal item, [in_valid()] will return false if this
// fails. It also fails after [next()] results in going past the offset array.
pub struct BlockCursor<C: Codec> {
    block: Arc<Block>,
    curr_offset_idx: usize,
    record: Result<Option<Record>, C::Error>,
    codec: C,
}

impl<C: Codec> BlockCursor<C> {
    pub fn new(block: Arc<Block>, c: C) -> Self {
        Self {
            block,
            curr_offset_idx: 0,
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
    pub fn get_err(&self) -> Option<&C::Error> {
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
    pub fn next(&mut self) {
        self.curr_offset_idx += 1;
        self.update_current();
    }

    #[inline]
    pub fn seek_to_first(&mut self) {
        self.curr_offset_idx = 0;
        self.update_current();
    }

    pub fn seek(&mut self, seek_key: Key) {
        let mut seek_key_bs = [0u8; KEY_SIZE];
        seek_key.to_be_bytes(&mut seek_key_bs);

        let mut search_err = None;
        //
        // TODO: instead of turning the bytes into Key and then comparing
        // we could just compare the bytes themselves (disregarding Key::stream_id: u64, the last 8 bytes)
        let res = self.block.offsets.binary_search_by(|o| {
            if search_err.is_some() {
                return std::cmp::Ordering::Less; // sentinel
            }

            let offset = *o as usize;
            let key_bs = &self.block.data[offset..offset + KEY_SIZE];
            match self.codec.decode_key(key_bs) {
                Ok(key) => key.cmp(&seek_key),
                Err(e) => {
                    search_err = Some(e);
                    std::cmp::Ordering::Less // sentinel
                }
            }
        });

        if let Some(e) = search_err {
            self.record = Err(e);
        } else {
            self.curr_offset_idx = match res {
                Ok(i) => i,  // exact match
                Err(i) => i, // first elem > target
            };
            self.update_current();
        }
    }

    pub fn peek_key(&mut self) -> Option<&Key> {
        match self.record.as_ref() {
            Ok(s) => s.as_ref().map(|r| &r.key),
            Err(_) => None,
        }
    }

    fn update_current(&mut self) {
        if self.curr_offset_idx >= self.block.offsets.len() {
            trace!("update_current: offset idx walked the length of the array");
            self.record = Ok(None);
            return;
        }

        let start_offset = self.block.offsets[self.curr_offset_idx] as usize;
        let end_offset = if self.curr_offset_idx + 1 < self.block.offsets.len() {
            self.block.offsets[self.curr_offset_idx + 1] as usize
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
