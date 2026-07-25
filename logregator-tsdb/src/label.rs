use std::{
    collections::{HashMap, HashSet},
    io,
};

use nom::IResult;
use nom::multi::length_data;
use nom::{Parser, number::complete::be_u32};

use crate::codec::{CodecError, SZ_U32, SpecCodec, WireLen};

type Label = String;

pub type LabelSet = HashMap<Label, String>;

impl WireLen for LabelSet {
    /// # Format
    ///
    /// ```not_rust
    /// [length prefix for eachthe labels: u32]
    /// [label_key_str_len_prefix: u32, that many bytes: [u8]]
    /// [label_value_str_len_prefix: u32, that many bytes: [u8]]
    /// ```
    fn wire_len(&self) -> usize {
        let mut sz = SZ_U32;
        for (k, v) in self.iter() {
            sz += SZ_U32 * 2;
            sz += k.len() + v.len()
        }

        sz
    }
}

pub type StreamRegistry = HashMap<usize /* stream_id */, LabelSet>;

pub type LabelRegistry = HashMap<Label, Vec<usize /* stream_id */>>;

#[derive(Debug, Clone, Copy)]
pub struct LabelSetCodec {}

pub const MAX_LABEL_KV_SIZE: usize = 64;
pub const MIN_LABEL_KV_SIZE: usize = 1;

impl SpecCodec<LabelSet> for LabelSetCodec {
    fn decode(&self, src: &[u8]) -> Result<Option<(LabelSet, usize)>, CodecError> {
        let res: IResult<&[u8], &[u8]> = length_data(be_u32).parse(src);

        todo!()

        // if src.len() < SZ_U32 {
        //     trace!(
        //         "not enough bytes to decode LabelSet from (src.len = {}, labelset length prefix = {})",
        //         src.len(),
        //         SZ_U32
        //     );
        //     return Ok(None);
        // }

        // let labelset_len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // let labelset = HashMap::with_capacity(labelset_len);

        // let decode_label_str = || -> Result<()> {

        //     if src.len() < consumed + SZ_U32 {
        //         trace!(
        //             "not enough bytes for labels key length prefix (src.len = {}, consumed bytes + length prefix = {})",
        //             src.len(),
        //             consumed + SZ_U32
        //         );
        //         return Ok(None);
        //     }

        //     let label_key_len = u32::from_be_bytes([
        //         src[consumed],
        //         src[consumed + 1],
        //         src[consumed + 2],
        //         src[consumed + 3],
        //     ]) as usize;
        //     consumed += 4;

        //     if !(MIN_LABEL_KV_SIZE..MAX_LABEL_KV_SIZE).contains(&label_key_len) {
        //         unimplemented!()
        //     }

        //     if src.len() < consumed + label_key_len {
        //         trace!(
        //             "not enough bytes for labels key (src.len = {}, consumed bytes + label key len = {})",
        //             src.len(),
        //             consumed + label_key_len
        //         );
        //         return Ok(None);
        //     }

        //     let label_key = &src[consumed..consumed + label_key_len];
        //  }
        // let mut consumed = 4;
        // for i in 0..labelset_len {
        //     if src.len() < consumed + SZ_U32 {
        //         trace!(
        //             "not enough bytes for labels key length prefix (src.len = {}, consumed bytes + length prefix = {})",
        //             src.len(),
        //             consumed + SZ_U32
        //         );
        //         return Ok(None);
        //     }

        //     let label_key_len = u32::from_be_bytes([
        //         src[consumed],
        //         src[consumed + 1],
        //         src[consumed + 2],
        //         src[consumed + 3],
        //     ]) as usize;
        //     consumed += 4;

        //     if !(MIN_LABEL_KV_SIZE..MAX_LABEL_KV_SIZE).contains(&label_key_len) {
        //         unimplemented!()
        //     }

        //     if src.len() < consumed + label_key_len {
        //         trace!(
        //             "not enough bytes for labels key (src.len = {}, consumed bytes + label key len = {})",
        //             src.len(),
        //             consumed + label_key_len
        //         );
        //         return Ok(None);
        //     }

        //     let label_key = &src[consumed..consumed + label_key_len];
        //     consumed += label_key_len;

        // }

        // Ok(Some((labelset, 0)))
    }

    fn encode(&self, item: &LabelSet, dst: &mut [u8]) -> Result<usize, CodecError> {
        unreachable!("SpecCodec<LabelSet>::encode is never used, as payloads stay untouched")
    }
}
