use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::rc::Rc;

use hashbrown::{Equivalent, HashMap, HashSet};
use nom::IResult;
use nom::{bytes::complete::take, number::complete::be_u8};
use serde::{Deserialize, Serialize};

use crate::codec::{
    self, CodecError, RecordCodecExt, SpecCodec, WireLen, SZ_U8,
};
use crate::merge_iter::MergeIter;
use crate::record::Record;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Label(pub Rc<str>, pub Rc<str>);

impl Equivalent<Label> for (&str, &str) {
    fn equivalent(&self, key: &Label) -> bool {
        self.0 == key.0.as_ref() && self.1 == key.1.as_ref()
    }
}

#[derive(Debug, Default, Clone)]
pub struct StreamRegistry {
    /// Source of truth, stores stream_id -> label_map.
    /// All other label related structs are derived from this one.
    labelmap_by_stream: HashMap<u64, Rc<LabelMap>>,

    /// used for writes, label_map -> stream_id,
    /// for quick hash checking
    stream_by_labelmap: HashMap<Rc<LabelMap>, u64>,

    /// used for reads: given a label, returns all
    /// stream_ids that has any of the given labels
    /// attached to it.
    kv_index: HashMap<Label, HashSet<u64>>,
}

/// Turns a stream -> labelmap mapping into a StreamRegistry
/// by inserting items from that mapping into the secondary
/// maps.
impl From<HashMap<u64, Rc<LabelMap>>> for StreamRegistry {
    fn from(labelmap_by_stream: HashMap<u64, Rc<LabelMap>>) -> Self {
        let mut stream_by_labelmap = HashMap::new();
        let mut kv_index: HashMap<Label, HashSet<u64>> = HashMap::new();

        for (stream_id, labelmap) in labelmap_by_stream.iter() {
            stream_by_labelmap.insert(labelmap.clone(), *stream_id);

            for (k, v) in labelmap.iter() {
                let lref: (&str, &str) = (&k, &v);
                let label = kv_index.get_mut(&lref);
                match label {
                    Some(stream_ids) => {
                        stream_ids.insert(*stream_id);
                    }
                    None => {
                        let mut stream_ids = HashSet::new();
                        stream_ids.insert(*stream_id);
                        kv_index.insert(Label(k, v), stream_ids);
                    }
                }
            }
        }

        Self {
            labelmap_by_stream,
            stream_by_labelmap,
            kv_index,
        }
    }
}

impl StreamRegistry {
    pub fn insert(&mut self, stream_id: u64, labelmap: Rc<LabelMap>) {
        self.labelmap_by_stream.insert(stream_id, labelmap.clone());
        self.stream_by_labelmap.insert(labelmap.clone(), stream_id);

        for (k, v) in labelmap.inner.iter() {
            let lref: (&str, &str) = (k, v);
            let label = self.kv_index.get_mut(&lref);
            match label {
                Some(stream_ids) => {
                    stream_ids.insert(stream_id);
                }
                None => {
                    let mut stream_ids = HashSet::new();
                    stream_ids.insert(stream_id);
                    self.kv_index
                        .insert(Label(k.clone(), v.clone()), stream_ids);
                }
            }
        }
    }

    pub fn get_stream_id_by_labelmap(
        &self,
        labelmap: &LabelMap,
    ) -> Option<&u64> {
        self.stream_by_labelmap.get(labelmap)
    }

    pub fn get_labelmap_by_stream(
        &self,
        stream_id: &u64,
    ) -> Option<&Rc<LabelMap>> {
        self.labelmap_by_stream.get(stream_id)
    }

    pub fn get_labelmap_by_stream_cloned(
        &self,
        stream_id: &u64,
    ) -> Option<Rc<LabelMap>> {
        self.labelmap_by_stream.get(stream_id).cloned()
    }

    pub fn get_streams_by_kv(&self, kv: (&str, &str)) -> Option<&HashSet<u64>> {
        self.kv_index.get(&kv)
    }

    pub fn label_intersection<I, S>(&self, labels: I) -> HashSet<u64>
    where
        I: Iterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        let mut sets: Vec<HashSet<u64>> = labels
            .flat_map(|(k, v)| {
                self.kv_index.get(&(k.as_ref(), v.as_ref())).cloned()
            })
            .collect();

        match sets.len() {
            0 => HashSet::new(),
            1 => sets.swap_remove(0),
            _ => {
                sets.sort_unstable_by_key(|s| s.len());
                let mut iter = sets.into_iter();
                let mut acc = iter.next().unwrap();

                for s in iter {
                    acc.retain(|id| s.contains(id));
                    if acc.is_empty() {
                        break;
                    }
                }

                acc
            }
        }
    }
}

/// stores labels -> stream_id
/// Its a wrapper over HashMap<S, S> where s is currently a String
/// but i might replace it with Rc<str> for cheap copies.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LabelMap {
    pub(crate) inner: BTreeMap<Rc<str>, Rc<str>>,
    pub(crate) fingerprint: u64,
}

impl Hash for LabelMap {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.fingerprint);
    }
}

impl PartialEq for LabelMap {
    fn eq(&self, other: &Self) -> bool {
        if self.fingerprint != other.fingerprint {
            return false;
        }
        self.inner.eq(&other.inner)
    }
}

impl Eq for LabelMap {}

impl WireLen for LabelMap {
    /// # Format
    ///
    /// ```not_rust
    /// [length prefix for each labels: u32]
    /// [label_key_str_len_prefix: u32, that many bytes: [u8]]
    /// [label_value_str_len_prefix: u32, that many bytes: [u8]]
    /// ```
    fn wire_len(&self) -> usize {
        let mut sz = SZ_U8;
        for (k, v) in self.inner.iter() {
            sz += SZ_U8 * 2;
            sz += k.len() + v.len()
        }

        sz
    }
}

impl LabelMap {
    pub fn iter(&self) -> impl Iterator<Item = (Rc<str>, Rc<str>)> {
        self.inner.iter().map(|(k, v)| (k.clone(), v.clone()))
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn new(map: BTreeMap<Rc<str>, Rc<str>>) -> Self {
        let mut hasher = DefaultHasher::new();

        for (k, v) in &map {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }

        LabelMap {
            inner: map,
            fingerprint: hasher.finish(),
        }
    }
}

/// Helps to decode [LabelMap]s.
///
/// A LabelMap can only hold [u8::MAX] key-value pairs at max
/// and each of those key-values can only me [u8::MAX] long
///
/// A log payloads internal representation is
/// [label_map...][raw bytes...], but inside [Record]
/// the payloads label_map bytes are not stripped.
///
/// The label_map wire format is the following:
///
/// [label_map_size: u8]
/// // this repeats label_map_size times:
/// [key_len: u8][key: key_len-many bytes]
/// [val_len: u8][val: val_len-many bytes]
#[derive(Debug, Clone, Copy)]
pub struct LabelMapCodec;

/// equals to 1, no empty key/values are allowed
pub const MIN_STR_SIZE: usize = 1;
pub const MAX_STR_SIZE: usize = u8::MAX as usize;
pub const MAX_LABEL_COUNT: usize = u8::MAX as usize;
pub const LABEL_COUNT_WIRE_LEN: usize = size_of::<u8>();

fn take_lp_str(src: &[u8]) -> IResult<&[u8], &str> {
    let (input, len) = be_u8(src)?;
    let (input, str_bs) = take(len)(input)?;
    let s = std::str::from_utf8(str_bs).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::MapRes,
        ))
    })?;
    Ok((input, s))
}

fn parse_label_map(src: &[u8]) -> IResult<&[u8], LabelMap> {
    let (input, label_count) = be_u8(src)?;
    let mut remainder = input;
    let label_count = label_count as usize;
    // trace!(label_count);

    let mut map: BTreeMap<Rc<str>, Rc<str>> = BTreeMap::new();
    for _ in 0..label_count {
        let (_input, k) = take_lp_str(remainder)?;
        remainder = _input;
        let (_input, v) = take_lp_str(remainder)?;
        remainder = _input;
        // trace!("inserting {k} => {v}");
        map.insert(Rc::from(k), Rc::from(v));
    }

    let labelmap = LabelMap::new(map);

    Ok((remainder, labelmap))
}

impl SpecCodec<LabelMap> for LabelMapCodec {
    fn decode(
        &self,
        src: &[u8],
    ) -> Result<Option<(LabelMap, usize)>, CodecError> {
        let (remaining, map) = parse_label_map(src).map_err(|e| {
            CodecError::Other(format!(
                "nom parsing error: failed to parse key-value pairs for label map: {e}"
            ))
        })?;

        let read = src.len() - remaining.len();
        Ok(Some((map, read)))
    }

    fn encode(
        &self,
        item: &LabelMap,
        dst: &mut [u8],
    ) -> Result<usize, CodecError> {
        if item.inner.len() > MAX_LABEL_COUNT {
            return Err(CodecError::Other(format!(
                "too many items in labelmap, got = {} > max = {}",
                item.inner.len(),
                MAX_LABEL_COUNT
            )));
        }

        for (k, v) in &item.inner {
            if !(MIN_STR_SIZE..MAX_STR_SIZE).contains(&k.len()) {
                return Err(CodecError::Other(format!(
                    "labelmaps key length is invalid, got = {}, min = {}, max = {}",
                    k.len(),
                    MIN_STR_SIZE,
                    MAX_STR_SIZE
                )));
            }

            if !(MIN_STR_SIZE..MAX_STR_SIZE).contains(&v.len()) {
                return Err(CodecError::Other(format!(
                    "labelmaps value length is invalid, got = {}, min = {}, max = {}",
                    v.len(),
                    MIN_STR_SIZE,
                    MAX_STR_SIZE
                )));
            }
        }

        // now wire_len() should the actual, valid size of the labelmap
        if dst.len() < item.wire_len() {
            return Err(codec::not_enough_bytes(dst.len(), item.wire_len()));
        }

        dst[0..1].copy_from_slice(&(item.inner.len() as u8).to_be_bytes());
        let mut start = 1;

        for (k, v) in &item.inner {
            dst[start..start + 1]
                .copy_from_slice(&(k.len() as u8).to_be_bytes());
            start += 1;
            dst[start..start + k.len()].copy_from_slice(k.as_bytes());
            start += k.len();

            dst[start..start + 1]
                .copy_from_slice(&(v.len() as u8).to_be_bytes());
            start += 1;
            dst[start..start + v.len()].copy_from_slice(v.as_bytes());
            start += v.len();
        }

        Ok(start)
    }
}

/// Iterator over a range of keys, possibly spanning
/// multiple Memtables and SSTables. It (roughly) yields
/// `(LabelMap, Record)`, and uses its `stream_registry` to
/// pair records to [LabelMap].
#[derive(Debug)]
pub struct LabeledIter<'l, C, M> {
    // this is not safe and also not Send or Sync...
    stream_registry: Rc<RefCell<StreamRegistry>>,
    iters: M,
    current: Option<MergeIter<'l, 'l, C>>,
}

impl<'l, C, M> LabeledIter<'l, C, M>
where
    M: Iterator<Item = MergeIter<'l, 'l, C>> + Debug,
    C: Debug,
{
    pub fn new<I>(i: I, stream_registry: Rc<RefCell<StreamRegistry>>) -> Self
    where
        I: IntoIterator<IntoIter = M>,
    {
        let me = Self {
            iters: i.into_iter(),
            stream_registry,
            current: None,
        };
        // dbg!(&me);
        me
    }
}

impl<'l, C, M> Iterator for LabeledIter<'l, C, M>
where
    C: RecordCodecExt,
    M: Iterator<Item = MergeIter<'l, 'l, C>>,
{
    type Item = (Option<Rc<LabelMap>>, Cow<'l, Record>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_none() {
            self.current = self.iters.next();
            self.current.as_ref()?;
        }

        match self.current.as_mut() {
            Some(mr) => match mr.next() {
                Some(rec) => {
                    let labelmap: Option<Rc<LabelMap>> = self
                        .stream_registry
                        .borrow()
                        .get_labelmap_by_stream_cloned(
                            &rec.as_ref().key.stream_id,
                        );
                    Some((labelmap, rec))
                }
                None => None,
            },
            None => None,
        }
    }
}

#[cfg(test)]
mod test {

    use pretty_assertions::assert_eq;

    use super::*;
    fn _encode(map: &LabelMap) -> Vec<u8> {
        let mut vec = vec![];

        if map.inner.len() > MAX_LABEL_COUNT {
            panic!("more than {} labels", MAX_LABEL_COUNT);
        }

        vec.extend((map.inner.len() as u8).to_be_bytes());

        for (k, v) in &map.inner {
            if k.len() < MIN_STR_SIZE || k.len() > MAX_STR_SIZE {
                panic!(
                    "invalid bounds for key str, min {}, max {}, got {}",
                    MIN_STR_SIZE,
                    MAX_STR_SIZE,
                    k.len(),
                );
            }

            if v.len() < MIN_STR_SIZE || v.len() > MAX_STR_SIZE {
                panic!(
                    "invalid bounds for value str, min {}, max {}, got {}",
                    MIN_STR_SIZE,
                    MAX_STR_SIZE,
                    v.len(),
                );
            }

            vec.extend((k.len() as u8).to_be_bytes());
            vec.extend(k.as_bytes());
            vec.extend((v.len() as u8).to_be_bytes());
            vec.extend(v.as_bytes());
        }
        vec
    }

    #[test]
    fn it_works() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .try_init();

        let mut map = BTreeMap::new();
        map.insert("foo".into(), "barbar".into());
        map.insert("bar".into(), "bazbaz".into());
        map.insert("baz".into(), "foofoo".into());
        let labelmap = LabelMap {
            inner: map,
            fingerprint: 0,
        };
        let codec = LabelMapCodec;
        let mut buf = vec![0; labelmap.wire_len()];
        let n = codec.encode(&labelmap, &mut buf).unwrap();
        assert_eq!(
            n,
            buf.len(),
            "expected to write the full maps wire length into destintation buffer"
        );

        let (labelmap2, read) =
            codec.decode(&buf).expect("decode").expect("option");
        assert_eq!(n, read, "expected to read the full input bytes lenght");
        dbg!(&labelmap2);
        assert_eq!(labelmap.inner, labelmap2.inner);
    }

    #[ignore = "testing differences between Rc::from and unsafe { Rc::from_raw(*str)}"]
    #[allow(unused)]
    fn test() {
        let b = b"foo";
        let s = std::str::from_utf8(b).unwrap();
        let _arc: Rc<str> = std::rc::Rc::from(s);
    }
}
