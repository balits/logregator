use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    rc::Rc,
};

use tracing::{instrument, trace};

use crate::{
    block::{
        self, Block, BlockCodecError, BlockCursor, BlockWriter, InvalidBlockLimit, WriteOutput,
    },
    codec::{self, Codec, CodecError},
    record::{KEY_SIZE, Key, Record},
};

/// we can tweak this, but it cannot exceed u32::MAX,
/// since [BlockMetadata] carries and encodes each blocks
/// offset as u32
pub const MAX_SST_SIZE: usize = (u32::MAX / 2) as usize; // 2GB

pub fn format_sst_filename(id: u32) -> String {
    format!("{id:010}.sst")
}

#[derive(thiserror::Error, Debug)]
pub enum SstFinalizeError {
    #[error("failed to finalize SSTable: {0}")]
    WriteError(#[from] SstWriteError),

    #[error("failed to finalize SSTable: missing first/last key")]
    MissingKeys,
}

#[derive(thiserror::Error, Debug)]
pub enum SstWriterCreateError {
    #[error("failed to create sst writer: an io error occurred: {0}")]
    Io(#[from] io::Error),

    #[error("failed to create sst writer: {0}")]
    InvalidBlockLimit(#[from] InvalidBlockLimit),
}

#[derive(thiserror::Error, Debug)]
pub enum SstWriteError {
    #[error("sst write error: an io error occurred: {0}")]
    Io(#[from] io::Error),

    #[error("sst write error: {0}")]
    BlockCodecError(#[from] block::BlockCodecError),

    #[error("sst write error: {0}")]
    InvalidBlockLimit(#[from] block::InvalidBlockLimit),

    #[error("sst write error: {0}")]
    CodecError(#[from] codec::CodecError),

    #[error("sst write error: sst full (context: {0})")]
    SstFull(String),
}

#[derive(Debug)]
pub struct SstWriter<W: io::Write, C: Codec> {
    writer: W,
    block_bytes_written: usize,
    block_writer: BlockWriter<C>,
    block_limit: usize,
    block_meta: Vec<BlockMetadata>,
    record_count: usize,
    first_key: Option<Key>,
    last_key: Option<Key>,
    codec: C,
}

impl<W: io::Write, C: Codec> SstWriter<W, C> {
    pub fn new(writer: W, codec: C, block_limit: Option<usize>) -> Result<Self, InvalidBlockLimit> {
        let block_writer = BlockWriter::new(codec.clone(), block_limit)?;
        let block_limit = block_writer.limit();

        Ok(Self {
            writer,
            block_bytes_written: 0,
            block_writer,
            block_limit,
            block_meta: vec![],
            first_key: None,
            last_key: None,
            record_count: 0,
            codec,
        })
    }

    #[instrument(skip(self, rec), err, fields(block_count = self.block_meta.len()))]
    pub fn write(&mut self, rec: &Record) -> Result<(), SstWriteError> {
        if self.block_bytes_written >= MAX_SST_SIZE {
            return Err(SstWriteError::SstFull("SST is already at max size".into()));
        }

        match self.block_writer.write(rec) {
            Ok(WriteOutput::Written) => {}
            Ok(WriteOutput::Full) => {
                self.flush()?;
                // first call to block_writer.write always succeeds
                // so infinite recursion can't happen
                self.write(rec)?;
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        Ok(())
    }

    pub fn finalize(mut self) -> Result<(W, SstMeta), SstFinalizeError> {
        if self.block_writer.size() > 0 {
            self.flush()?;
        }

        if self.first_key.is_none() || self.last_key.is_none() {
            return Err(SstFinalizeError::MissingKeys);
        }

        // block_bytes_written can't be greater than MAX_SST_SIZE (which can't be greater than u32::MAX)
        // so this cast is safe
        let meta_start = self.block_bytes_written as u32;
        let metadata_bytes = BlockMetadata::encode_metas(&self.block_meta, &self.codec)
            .map_err(SstWriteError::from)?;

        self.writer
            .write_all(&metadata_bytes)
            .map_err(SstWriteError::from)?;
        self.writer
            .write_all(&meta_start.to_be_bytes())
            .map_err(SstWriteError::from)?;
        let size = self.block_bytes_written + metadata_bytes.len() + size_of_val(&meta_start);

        let meta = SstMeta {
            block_meta: self.block_meta,
            block_meta_offset: meta_start as usize, // meta_start was usize originally so this cast is safe
            size,
            record_count: self.record_count,
            first_key: self.first_key.unwrap(),
            last_key: self.last_key.unwrap(),
        };

        Ok((self.writer, meta))
    }

    #[instrument(skip(self), err)]
    fn flush(&mut self) -> Result<(), SstWriteError> {
        if self.block_bytes_written >= MAX_SST_SIZE {
            return Err(SstWriteError::SstFull("SST is already at max size".into()));
        }

        if self.block_bytes_written + self.block_writer.size() > MAX_SST_SIZE {
            return Err(SstWriteError::SstFull(format!(
                "cannot flush latest block into the SST, current sst size = {}, latest block size = {}",
                self.block_bytes_written,
                self.block_writer.size(),
            )));
        }

        let old_block_writer = std::mem::replace(
            &mut self.block_writer,
            BlockWriter::new(self.codec.clone(), Some(self.block_limit))?,
        );

        let (block, first_key, last_key) = old_block_writer.into_block();
        let block_offset = self.block_bytes_written;
        let block_bytes = block.encode_block()?;
        self.writer.write_all(&block_bytes)?;

        // SAFETY
        //
        // Firstly, we already made sure block_offset is less than u32::MAX (see MAX_SST_SIZE),
        // so casting offset from usize to u32 is safe.
        //
        // Secondly, this function is only called when block writer is full, meaning it has
        // at least 1 record written into it, therefore first_key and last_key cannot be None
        // and unwrapping is safe.
        let first_key = first_key.unwrap();
        let last_key = last_key.unwrap();
        let meta = BlockMetadata {
            offset: block_offset as u32,
            first_key: first_key.clone(),
            last_key: last_key.clone(),
        };
        self.block_meta.push(meta);
        self.block_bytes_written += block_bytes.len();
        if self.first_key.is_none() {
            self.first_key = Some(first_key);
        }
        self.last_key = Some(last_key);
        self.record_count += block.num_of_records();

        Ok(())
    }
}

#[derive(Debug)]
pub struct SstFileWriter<C: Codec> {
    inner: SstWriter<FileHandle, C>,
}

impl<C: Codec> SstFileWriter<C> {
    pub fn new(
        id: u32,
        codec: C,
        block_limit: Option<usize>,
        path_prefix: Option<&Path>,
    ) -> Result<Self, SstWriterCreateError> {
        let fh = FileHandle::open(
            id,
            path_prefix,
            OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(true)
                .create_new(true),
        )?;

        let inner = SstWriter::new(fh, codec, block_limit)?;

        Ok(Self { inner })
    }

    pub fn write(&mut self, rec: &Record) -> Result<(), SstWriteError> {
        self.inner.write(rec)
    }

    pub fn finalize_file(self) -> Result<SstHandle, SstFinalizeError> {
        let (fh, meta) = self.inner.finalize()?;
        Ok(SstHandle { fh, meta })
    }
}

/// Handle to an immutable SSTable file on the disk.
/// This is created by [SstWriter::finish] and is used
/// in [SstReader].
#[derive(Debug)]
pub struct SstHandle {
    fh: FileHandle,
    meta: SstMeta,
}

#[derive(thiserror::Error, Debug)]
pub enum SstReadError {
    #[error("sst read error: an io error occured: {0}")]
    Io(#[from] io::Error),

    #[error("sst read error: block index out of bounds: the len is {len} but the index is {idx}")]
    BlockIdxOutOfBounds { idx: usize, len: usize },

    #[error("sst read error: {0}")]
    BlockCodecError(#[from] BlockCodecError),

    #[error("sst read error: {0}")]
    CodecError(#[from] CodecError),

    #[error("sst read error: corrupted not-sorted block")]
    CorruptedBlockNotSorted,
}

impl SstHandle {
    #[inline]
    pub fn block_meta(&self) -> &[BlockMetadata] {
        &self.meta.block_meta
    }

    pub fn cursor<C: Codec>(self: &Rc<Self>, codec: C) -> Result<SstCursor<C>, SstReadError> {
        SstCursor::new(self.clone(), codec)
    }

    pub fn read_block(&self, block_idx: usize) -> Result<Rc<Block>, SstReadError> {
        let block_start = self
            .block_meta()
            .get(block_idx)
            .ok_or(SstReadError::BlockIdxOutOfBounds {
                idx: block_idx,
                len: self.meta.block_meta.len(),
            })?
            .offset as usize;

        let block_end = if block_idx == self.block_meta().len() - 1 {
            self.meta.block_meta_offset
        } else {
            self.block_meta()[block_idx + 1].offset as usize
        };

        debug_assert!(
            block_start < block_end,
            "malformed sst blocks: block_start >= block_end"
        );

        let mut buf = vec![0; block_end - block_start];
        self.fh
            .as_file()
            .read_exact_at(&mut buf, block_start as u64)?;

        let b = Block::decode_block(&buf)?;
        Ok(Rc::new(b))
    }
}

#[derive(Debug)]
pub struct SstMeta {
    block_meta: Vec<BlockMetadata>,
    block_meta_offset: usize,
    size: usize, // can be derived with block_meta_offset + size_of block metas wire_length + size_of_val(block_meta_offset)
    record_count: usize, // for debugging purposes
    first_key: Key,
    last_key: Key,
}

#[derive(Debug)]
pub struct SstCursor<C: Codec> {
    sst: Rc<SstHandle>,
    block_cursor: Result<BlockCursor<C>, SstReadError>,
    block_idx: usize,
    codec: C,
}

impl<C: Codec> SstCursor<C> {
    fn new(sst: Rc<SstHandle>, codec: C) -> Result<Self, SstReadError> {
        let block_idx = 0;
        let block = sst.read_block(block_idx)?;
        let mut cursor = BlockCursor::new(block, codec.clone());
        cursor.seek_to_first();
        let block_cursor = Ok(cursor);

        Ok(Self {
            sst,
            block_cursor,
            block_idx: 0,
            codec,
        })
    }

    #[inline]
    pub fn is_error(&self) -> bool {
        match self.block_cursor.as_ref() {
            Err(_) => true,
            Ok(c) => c.is_error(),
        }
    }

    pub fn get_error(&self) -> Option<Result<&SstReadError, &CodecError>> {
        match self.block_cursor.as_ref() {
            Err(e) => Some(Ok(e)),
            Ok(c) => match c.get_error() {
                Some(e) => Some(Err(e)),
                None => None,
            },
        }
    }

    #[inline]
    pub fn get_codec_error(&self) -> Option<&CodecError> {
        self.block_cursor.as_ref().ok().and_then(|c| c.get_error())
    }

    #[inline]
    pub fn get_sst_error(&self) -> Option<&SstReadError> {
        self.block_cursor.as_ref().err()
    }

    #[inline]
    pub fn is_record(&self) -> bool {
        self.block_cursor
            .as_ref()
            .map(|c| c.is_record())
            .unwrap_or(false)
    }

    #[inline]
    pub fn current(&self) -> Option<&Record> {
        self.block_cursor.as_ref().map(|c| c.current()).ok()?
    }

    #[inline]
    #[instrument(skip(self), fields(block_idx = self.block_idx, block_count = self.sst.block_meta().len()))]
    pub fn next(&mut self) {
        if let Ok(c) = self.block_cursor.as_mut() {
            trace!("advancing inner cursor");
            c.next();
            if c.is_record() {
                trace!("after advance: inner cursor no longer yields records");
                return;
            }
        }

        if !self.is_error() {
            trace!("inner cursor is empty, advancing to the next one");
            self.block_idx += 1;
            self.update_current();
        }
    }

    pub fn seek_to_first(&mut self) {
        self.block_idx = 0;
        // internally it calls new_cursor.seek_to_first()
        self.update_current();
    }

    #[instrument(skip(self))]
    pub fn seek(&mut self, seek_key: &Key) {
        use std::cmp::Ordering::*;
        let mut bin_search_error = None;

        // ...copied from BlockCursor::seek():
        // TODO: instead of turning the bytes into Key and then comparing
        // we could just compare the bytes themselves (disregarding Key::stream_id: u64, the last 8 bytes)
        let res = self.sst.block_meta().binary_search_by(|block| {
            match (
                seek_key.cmp(&block.first_key),
                seek_key.cmp(&block.last_key),
            ) {
                (Less, Greater)=> {
                    trace!("binary search: block is not sorted: seek key is both less than block.first_key but also greater than block.last_key");
                    bin_search_error = Some(SstReadError::CorruptedBlockNotSorted);
                    Equal // doesnt matter
                },
                (Equal, _) => Equal,
                (_, Equal) => Equal,
                (Greater, Less) => Equal,
                (Less, _) => Less,
                (_, Greater) => Greater,
            }
        });

        if let Some(e) = bin_search_error {
            self.block_cursor = Err(e);
            return;
        }

        trace!("binary search result = {:?}", res);
        self.block_idx = match res {
            Ok(i) => i,  // exact match
            Err(i) => i, // first elem > target
        };
        self.update_current();
        if let Ok(c) = self.block_cursor.as_mut() {
            c.seek(seek_key);
        }
    }

    #[instrument(skip(self), fields(block_idx = self.block_idx, block_count = self.sst.block_meta().len()))]
    fn update_current(&mut self) {
        if self.block_idx >= self.sst.block_meta().len() {
            trace!("block_idx is out of bounds");
            self.block_cursor = Err(SstReadError::BlockIdxOutOfBounds {
                idx: self.block_idx,
                len: self.sst.block_meta().len(),
            });
            return;
        }

        let block = match self.sst.read_block(self.block_idx) {
            Ok(b) => b,
            Err(e) => {
                trace!("failed to read next block");
                trace!(read_block_error = %e);
                self.block_cursor = Err(e);
                return;
            }
        };
        trace!("read next block, creating next cursor and seeking to its start...");

        let mut cursor = BlockCursor::new(block, self.codec.clone());
        cursor.seek_to_first();
        self.block_cursor = Ok(cursor);
    }
}

/// Wrapper around a file, a unique ID, and the files path
#[derive(Debug)]
pub struct FileHandle {
    id: u32,
    file: File,
    path: PathBuf,
}

impl FileHandle {
    pub fn open(id: u32, prefix: Option<&Path>, opts: &mut OpenOptions) -> io::Result<Self> {
        let fmt = format_sst_filename(id);
        let path = match prefix {
            Some(p) => p.join(fmt),
            None => fmt.into(),
        };
        let file = opts.open(&path)?;
        Ok(Self { id, path, file })
    }

    #[inline]
    pub fn as_file(&self) -> &File {
        &self.file
    }
}

impl io::Write for FileHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.as_file().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.as_file().flush()
    }
}

impl io::Read for FileHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.as_file().read(buf)
    }
}

pub const SZ_U32: usize = size_of::<u32>();

/// Metadata for blocks inside an SSTable. It contains the blocks start in bytes as offset
/// and the first/last keys in this block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMetadata {
    offset: u32,
    first_key: Key,
    last_key: Key,
}

impl BlockMetadata {
    const fn wire_len() -> usize {
        size_of::<u32>() + KEY_SIZE + KEY_SIZE
    }

    pub fn encode_metas<C: Codec>(metas: &[Self], c: &C) -> Result<Vec<u8>, CodecError> {
        // need enough bytes the lenght prefix, for the metas themselves,
        // and for the 32bit checksum
        let capacity = SZ_U32 + metas.len() * Self::wire_len() + SZ_U32;
        let mut data = Vec::with_capacity(capacity);
        data.extend((metas.len() as u32).to_be_bytes());

        for m in metas {
            data.extend(&m.offset.to_be_bytes());
            data.extend(c.encode_key(&m.first_key)?);
            data.extend(c.encode_key(&m.last_key)?);
        }
        let checksum = crc32c::crc32c(&data[..]);
        data.extend(checksum.to_be_bytes());
        Ok(data)
    }

    #[instrument(skip(src), err)]
    pub fn decode_metas<C: Codec>(src: &[u8], codec: &C) -> Result<Vec<Self>, CodecError> {
        if src.len() < SZ_U32 {
            trace!("not enough bytes for length prefix");
            return Err(CodecError::UnexpectedSize(codec::UnexpectedSize {
                got: src.len(),
                want: SZ_U32,
            }));
        }
        let metas_len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let mut consumed = SZ_U32; // we already read length prefix

        let want_total_size = SZ_U32 + metas_len * Self::wire_len() + SZ_U32;
        if src.len() < want_total_size {
            trace!(
                "not enough bytes for length prefix + {} * block metas + checksum (src.len() = {})",
                metas_len,
                src.len()
            );
            return Err(CodecError::UnexpectedSize(codec::UnexpectedSize {
                got: src.len(),
                want: want_total_size,
            }));
        }

        let mut data = Vec::with_capacity(metas_len);
        for _ in 0..metas_len {
            let offset = u32::from_be_bytes([
                src[consumed],
                src[consumed + 1],
                src[consumed + 2],
                src[consumed + 3],
            ]);
            consumed += SZ_U32;
            let first_key = codec.decode_key(&src[consumed..consumed + KEY_SIZE])?;
            consumed += KEY_SIZE;
            let last_key = codec.decode_key(&src[consumed..consumed + KEY_SIZE])?;
            consumed += KEY_SIZE;

            let meta = BlockMetadata {
                offset,
                first_key,
                last_key,
            };
            data.push(meta);
        }

        let computed = crc32c::crc32c(&src[..consumed]);
        let stored = u32::from_be_bytes([
            src[consumed],
            src[consumed + 1],
            src[consumed + 2],
            src[consumed + 3],
        ]);
        if stored != computed {
            return Err(CodecError::Other(format!(
                "checksum mismatch, {stored:#x} (stored) != {computed:#x} (computed)"
            )));
        }

        Ok(data)
    }
}

#[cfg(test)]
mod test {
    use std::rc::Rc;

    use pretty_assertions::{assert_eq, assert_str_eq};
    use tracing_subscriber::fmt::format::FmtSpan;

    use super::BlockMetadata;
    use crate::{
        codec::BytesCodec,
        record::Record,
        sst::{SstFileWriter, SstWriter},
    };

    fn tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            // .with_target(true)
            // .with_span_events(FmtSpan::NEW)
            .with_test_writer()
            .try_init();
    }

    fn record(i: u64, len: usize) -> Record {
        Record {
            key: crate::record::Key {
                source_id: i,
                timestamp: i,
                sequence_num: i,
                stream_id: i,
            },
            payload: vec![67u8; len].into_boxed_slice(),
        }
    }

    #[test]
    fn meta_codec() {
        tracing();
        let codec = BytesCodec;
        let metas: Vec<BlockMetadata> = (0..10)
            .map(|i| BlockMetadata {
                offset: i as u32,
                first_key: crate::record::Key {
                    source_id: i,
                    timestamp: i,
                    sequence_num: i,
                    stream_id: i,
                },
                last_key: crate::record::Key {
                    source_id: i,
                    timestamp: i,
                    sequence_num: i,
                    stream_id: i,
                },
            })
            .collect();

        let bytes = BlockMetadata::encode_metas(&metas, &codec).unwrap();
        let m2 = BlockMetadata::decode_metas(&bytes, &codec).unwrap();
        assert_eq!(metas, m2);
    }

    #[test]
    fn sst_writer_inmem() {
        tracing();
        let codec = BytesCodec;
        let writer = std::io::Cursor::new(Vec::new());
        let mut sw = SstWriter::new(writer, codec, None).unwrap();

        let num_records = 512usize;
        for i in 0..num_records {
            sw.write(&record(i as u64, 512)).unwrap();
        }

        let (_, meta) = sw.finalize().unwrap();
        assert_eq!(meta.record_count, num_records);
        dbg!(meta);
    }

    #[test]
    fn sst_file_writer() {
        let tempdir = tempfile::TempDir::new().unwrap();

        tracing();
        let codec = BytesCodec;

        let mut sw = SstFileWriter::new(1, codec, None, Some(tempdir.path())).unwrap();

        for i in 0..512 {
            sw.write(&record(i, 512)).unwrap();
        }

        let sst = sw.finalize_file().unwrap();
        dbg!(sst);
    }

    #[test]
    fn sst_read_block() {
        let tempdir = tempfile::TempDir::new().unwrap();
        tracing();
        let codec = BytesCodec;
        let mut sw = SstFileWriter::new(1, codec, None, Some(tempdir.path())).unwrap();
        let num_records = 10usize;
        for i in 0..num_records {
            sw.write(&record(i as u64, 512)).unwrap();
        }
        dbg!(&sw);
        let sst = sw.finalize_file().unwrap();
        assert_eq!(sst.meta.record_count, num_records);
        dbg!(&sst);

        for i in 0..sst.meta.block_meta.len() {
            let block = sst.read_block(i).unwrap();
            dbg!(block);
        }

        match sst.read_block(sst.meta.block_meta.len()) {
            Err(e) => {
                dbg!(e);
            }
            Ok(_) => panic!("reading past the end of blocks of sst shouldve errored!"),
        }
    }

    #[test]
    fn sst_cursor() {
        let tempdir = tempfile::TempDir::new().unwrap();
        tracing();
        let codec = BytesCodec;
        let mut sw = SstFileWriter::new(1, codec, None, Some(tempdir.path())).unwrap();
        let num_records = 10;
        let records: Vec<Record> = (0..num_records).map(|i| record(i as u64, 512)).collect();
        for r in &records {
            sw.write(r).unwrap();
        }
        dbg!(&sw);
        let sst = Rc::new(sw.finalize_file().unwrap());
        assert_eq!(sst.meta.record_count, num_records);

        let mut c = sst.cursor(codec).unwrap();
        println!("\n\n=> CURSOR: iterating using .next() <=\n\n");
        println!(
            "Before iterating the cursor: sst has {} block(s) and {} record(s) in total",
            sst.block_meta().len(),
            sst.meta.record_count
        );

        let mut i = 0;
        loop {
            if let Some(joint_err) = c.get_error() {
                match joint_err {
                    Ok(se) => println!("cursor stopped from sst read error: {se}"),
                    Err(ce) => println!("cursor stopped from codec error: {ce}"),
                }
                break;
            }

            match c.current() {
                Some(rec) => {
                    println!("{i}: record is {rec:?}");
                    i += 1;
                }
                None => {
                    println!("{i}: record is None");
                    break;
                }
            }

            println!("calling cursor.next()");
            c.next();
        }

        assert_eq!(
            i, num_records,
            "cursor shouldve seen all elements inside the sstable"
        );

        // c.seek_to_first();
        println!("\n\n=> CURSOR: seeking (in order)<=");
        for (i, r) in records.iter().enumerate() {
            c.seek(&r.key);

            if let Some(joint_err) = c.get_error() {
                match joint_err {
                    Ok(se) => panic!("{i}/{num_records}: cursor stopped from sst read error: {se}"),
                    Err(ce) => panic!("{i}/{num_records}:cursor stopped from codec error: {ce}"),
                }
            }

            println!("{i}/{num_records}: {:?}", c.current());
            assert_eq!(
                Some(r),
                c.current(),
                "{i}/{num_records}: c.current() returned None"
            );
        }

        println!("\n\n=> CURSOR: seeking (in order)<=");
        let mut random_records: Vec<(usize, Record)> =
            records.iter().cloned().enumerate().collect();
        use rand::seq::SliceRandom;
        random_records.shuffle(&mut rand::rng());

        for (i, r) in random_records {
            c.seek(&r.key);

            if let Some(joint_err) = c.get_error() {
                match joint_err {
                    Ok(se) => panic!("{i}/{num_records}: cursor stopped from sst read error: {se}"),
                    Err(ce) => panic!("{i}/{num_records}:cursor stopped from codec error: {ce}"),
                }
            }

            println!("{i}/{num_records}: {:?}", c.current());
            assert_eq!(
                Some(&r),
                c.current(),
                "{i}/{num_records}: c.current() returned None"
            );
        }
    }
}
