use std::{
    fmt::Debug,
    fs::{File, OpenOptions},
    io,
    ops::Bound,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    rc::Rc,
};

use tracing::{instrument, span, trace, Level};

use crate::{
    block::{
        self, Block, BlockCursor, BlockWriter, InvalidBlockLimit, WriteOutput,
    },
    bloom::{self, BloomFilter, BloomFilterCodec},
    codec::{self, CodecError, RecordCodecExt, SpecCodec, WireLen},
    record::{Key, Record, KEY_SIZE},
};

/// we can tweak this, but it cannot exceed u32::MAX,
/// since [BlockMetadata] carries and encodes each blocks
/// offset as u32
pub const MAX_SST_SIZE: usize = (u32::MAX / 2) as usize; // 2GB

/// (block_metadata_start_offset, bloom_filter_start_offset, crc_checksum)
pub const SST_FOOTER_SIZE: usize = size_of::<u32>() * 3;
pub const MIN_SST_SIZE: usize = SST_FOOTER_SIZE;

pub fn format_sst_filename(id: u64) -> String {
    format!("{id:020}.sst")
}

#[derive(thiserror::Error, Debug)]
pub enum SstError {
    #[error("sst error: custom error occured: {0}")]
    Other(String),

    #[error("sst error: a std::io error occurred: {0}")]
    // Io(Arc<io::Error>),
    Io(#[from] io::Error),

    #[error("sst error: {0}")]
    BlockCodecError(#[from] block::BlockCodecError),

    #[error("sst error: {0}")]
    CodecError(#[from] codec::CodecError),

    #[error("sst error: {0}")]
    InvalidBlockLimit(#[from] block::InvalidBlockLimit),

    #[error("sst error: invalid block: block not sorted")]
    InvalidBlockNotSorted,

    #[error(
        "sst error: SSTable filled up, cannot insert more records / blocks"
    )]
    SstFilledUp,

    #[error("sst error: invalid SSTable size: min = {min}, max = {max}, got = {got}")]
    InvalidSstSize { min: usize, max: usize, got: u64 },

    #[error(
        "sst error: block index out of bounds: the len is {len} but the index is {idx}"
    )]
    BlockIdxOutOfBounds { idx: usize, len: usize },

    // #[error(
    //     "sst error: bound {bound:?} does not exist in the current SSTable"
    // )]
    // BoundNotExist { bound: Bound<Key> },
    #[error(
        "sst read error: bound {bound_kind:?} with stream_id {stream_id} not found"
    )]
    StreamIdNotFound {
        stream_id: u64,
        bound_kind: BoundKind,
    },

    #[error("sst error: failed to finalize SSTable: missing first/last key")]
    FinalizeMissingKeys,

    #[error("sst error: {0}")]
    ChecksumMismatch(crate::ChecksumMismatch),

    #[error("sst error: block cursor exhausted")]
    BlockCursorExhausted,
}

#[derive(Debug)]
pub enum BoundKind {
    Start,
    End,
    None,
}

#[derive(Debug)]
pub(crate) struct WriterState<W, C> {
    pub(crate) writer: W,
    pub(crate) block_bytes_written: usize,
    pub(crate) block_writer: BlockWriter<C>,
    pub(crate) block_limit: usize,
    pub(crate) block_meta: Vec<BlockMetadata>,
    pub(crate) record_count: usize,
    pub(crate) first_key: Option<Key>,
    pub(crate) last_key: Option<Key>,
    pub(crate) bloom: BloomFilter,
    pub(crate) codec: C,
}

#[derive(Debug)]
pub(crate) struct FinalizedWriterState<W, C> {
    pub(crate) inner: WriterState<W, C>,
    pub(crate) block_metadata_start_offset: usize,
    #[allow(unused)]
    pub(crate) bloom_filter_start_offset: usize,
}

/// SSTable binary layout:
/// ```not_rust
/// [...block]                    # each block contains records and their offsets
/// [...block_metadata]           # each metadata contains the blocks offset plus first and last keys
/// [bloom_filter]                # answers "does this SST contain any records for stream_id X"
/// [block_metadata_start_offset] # where 'block_metadata' starts
/// [bloom_filter_start_offset] # where 'bloom_filter' starts
/// ```
#[derive(Debug)]
pub struct HashedWriterState<W: io::Write, C> {
    state: WriterState<W, C>,
    integrity_hasher: crc32fast::Hasher,
}

impl<W, C> HashedWriterState<W, C>
where
    W: io::Write + std::fmt::Debug,
    C: SpecCodec<Record> + SpecCodec<Key>,
{
    pub fn new(
        writer: W,
        codec: C,
        block_limit: Option<usize>,
        record_count: usize,
    ) -> Result<Self, InvalidBlockLimit> {
        let block_writer = BlockWriter::new(codec.clone(), block_limit)?;
        let block_limit = block_writer.limit();
        let bloom = BloomFilter::new(record_count, 0.01);
        let state = WriterState {
            writer,
            block_bytes_written: 0,
            block_writer,
            block_limit,
            block_meta: vec![],
            first_key: None,
            last_key: None,
            record_count: 0,
            bloom,
            codec,
        };

        Ok(Self {
            state,
            integrity_hasher: crc32fast::Hasher::new(),
        })
    }

    #[instrument(skip(self), err, fields(block_count = self.state.block_meta.len()))]
    pub fn write(&mut self, rec: &Record) -> Result<(), SstError> {
        if self.state.block_bytes_written >= MAX_SST_SIZE {
            return Err(SstError::SstFilledUp);
        }

        match self.state.block_writer.write(rec) {
            Ok(WriteOutput::Written) => {
                self.state.bloom.insert(rec.key.stream_id);
            }
            Ok(WriteOutput::Full) => {
                self.flush_blocks()?;
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

    /// the layout of the finalized state is roughly like so:
    /// 0: raw_blocks (handled by `self.flush_blocks`)
    ///
    /// 1: block_metadata
    /// 2: bloom_filter
    /// 3: block_metadata_start_offset
    /// 4: bloom_filter_start_offset
    ///
    /// 5: crc hash of the whole sst
    ///
    /// # NOTE
    ///
    /// Before the crc hash is appended to the end of the writer,
    /// the hasher should be updated with the new bytes written to
    /// the writer in this function.
    #[instrument(skip(self), ret, err)]
    pub(crate) fn finalize(
        mut self,
    ) -> Result<FinalizedWriterState<W, C>, SstError> {
        if self.state.block_writer.size() > 0 {
            self.flush_blocks()?;
        }

        if self.state.first_key.is_none() || self.state.last_key.is_none() {
            return Err(SstError::FinalizeMissingKeys);
        }

        // # SAFETY
        //
        // block_bytes_written can't be greater than MAX_SST_SIZE (which can't be greater than u32::MAX)
        // so this cast is safe

        let block_meta_start = self.state.block_bytes_written as u32;
        let block_meta_bytes = BlockMetadata::encode_metas(
            &self.state.block_meta,
            &self.state.codec,
        )
        .map_err(SstError::from)?;
        let bloom_filter_start =
            block_meta_start + block_meta_bytes.len() as u32;

        // 1.
        self.state
            .writer
            .write_all(&block_meta_bytes)
            .map_err(SstError::from)?;
        self.integrity_hasher.update(&block_meta_bytes);

        // 2.
        {
            /*
            -------------------------------------------------
            omg, i know i have SpecCodec<I> everywhere
            but i rly dont feel like introducing
            <B: SpecCodec<BloomFilter> to lsm, or sst
            or wherever. Plus realistically a bloom
            filters wire representation is not gonna change.
            -------------------------------------------------
            */

            let codec = bloom::BloomFilterCodec;
            let mut bloom_buf = vec![0; self.state.bloom.wire_len()];
            codec
                .encode(&self.state.bloom, &mut bloom_buf)
                .map_err(SstError::from)?;
            self.state
                .writer
                .write_all(&bloom_buf)
                .map_err(SstError::from)?;

            self.integrity_hasher.update(&bloom_buf);
        }

        // 3.
        self.state
            .writer
            .write_all(&block_meta_start.to_be_bytes())
            .map_err(SstError::from)?;
        self.integrity_hasher
            .update(&block_meta_start.to_be_bytes());

        // 4.
        self.state
            .writer
            .write_all(&bloom_filter_start.to_be_bytes())
            .map_err(SstError::from)?;
        self.integrity_hasher
            .update(&bloom_filter_start.to_be_bytes());

        // 5.
        self.state
            .writer
            .write_all(&self.integrity_hasher.finalize().to_be_bytes())
            .map_err(SstError::from)?;

        Ok(FinalizedWriterState {
            inner: self.state,
            block_metadata_start_offset: block_meta_start as usize,
            bloom_filter_start_offset: bloom_filter_start as usize,
        })
    }

    #[instrument(skip(self), err)]
    fn flush_blocks(&mut self) -> Result<(), SstError> {
        if self.state.block_bytes_written >= MAX_SST_SIZE {
            return Err(SstError::SstFilledUp);
        }

        let estimated_bytes =
            self.state.block_bytes_written + self.state.block_writer.size();
        if estimated_bytes > MAX_SST_SIZE {
            return Err(SstError::InvalidSstSize {
                got: estimated_bytes as u64,
                min: MIN_SST_SIZE,
                max: MAX_SST_SIZE,
            });
        }

        let old_block_writer = std::mem::replace(
            &mut self.state.block_writer,
            BlockWriter::new(
                self.state.codec.clone(),
                Some(self.state.block_limit),
            )?,
        );

        let (block, first_key, last_key) = old_block_writer.into_block();
        let block_offset = self.state.block_bytes_written;
        let block_bytes = block.encode_block()?;
        self.state.writer.write_all(&block_bytes)?;
        self.integrity_hasher.update(&block_bytes);

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

        self.state.block_meta.push(meta);
        self.state.block_bytes_written += block_bytes.len();
        self.state.record_count += block.num_of_records();

        if self.state.first_key.is_none() {
            self.state.first_key = Some(first_key);
        }
        self.state.last_key = Some(last_key);

        Ok(())
    }
}

#[derive(Debug)]
pub struct SstFileWriter<C> {
    inner: HashedWriterState<SstFileHandle, C>,
}

impl<C> SstFileWriter<C>
where
    C: SpecCodec<Record> + SpecCodec<Key>,
{
    #[instrument(ret, err)]
    pub fn new(
        id: u64,
        codec: C,
        block_limit: Option<usize>,
        path_prefix: Option<&Path>,
        record_count: usize,
    ) -> Result<Self, SstError> {
        let fh = SstFileHandle::open(
            id,
            path_prefix,
            OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(true)
                .create_new(true),
        )?;

        let inner =
            HashedWriterState::new(fh, codec, block_limit, record_count)?;

        Ok(Self { inner })
    }

    pub fn write(&mut self, rec: &Record) -> Result<(), SstError> {
        self.inner.write(rec)
    }

    pub fn finalize_file(self) -> Result<SstHandle<C>, SstError> {
        let state = self.inner.finalize()?;

        Ok(SstHandle {
            fh: state.inner.writer,
            block_metadata: state.inner.block_meta,
            block_metadata_start_offset: state.block_metadata_start_offset,
            codec: state.inner.codec,
            bloom: state.inner.bloom,
        })
    }
}

/// Handle to an immutable SSTable file on the disk.
/// This is created by [SstWriter::finish] and is used
/// by [SstCursor].
#[derive(Debug)]
pub struct SstHandle<C> {
    fh: SstFileHandle,
    block_metadata: Vec<BlockMetadata>,
    block_metadata_start_offset: usize,
    bloom: BloomFilter,
    codec: C,
}

impl<C> SstHandle<C> {
    pub fn id(&self) -> u64 {
        self.fh._id
    }
}

impl<C> SstHandle<C>
where
    C: RecordCodecExt,
{
    #[instrument(err, level = "TRACE")]
    pub fn open_path<P>(path: P, codec: C) -> Result<Self, SstError>
    where
        P: AsRef<Path> + std::fmt::Debug,
    {
        let fh = SstFileHandle::open_from_path(path.as_ref())?;
        Self::open_file_handle(fh, codec)
    }

    #[instrument(err, level = "TRACE")]
    pub fn open_file_handle(
        fh: SstFileHandle,
        codec: C,
    ) -> Result<Self, SstError> {
        let f = fh.file();
        let fsize = f.metadata()?.len();

        // "footer" is (block_metadata_start_offset, bloom_filter_start_offset, crc_checksum)
        if fsize < MIN_SST_SIZE as u64 {
            return Err(SstError::InvalidSstSize {
                got: fsize,
                min: MIN_SST_SIZE,
                max: MAX_SST_SIZE,
            });
        } else if fsize > MAX_SST_SIZE as u64 {
            return Err(SstError::Other(format!(
                "sst file too large (got: {fsize} max: {MAX_SST_SIZE})"
            )));
        }

        let mut footer_buf = [0; SST_FOOTER_SIZE];
        f.read_exact_at(&mut footer_buf, fsize - SST_FOOTER_SIZE as u64)
            .map_err(|e| SstError::Other(format!("{e}")))?;

        // FIXED: what do i do with this checksum?
        // to reconstruct we would have to iterate over
        // the whooole sst file
        //
        // SOLUTION: sst level checksum is made out of
        // the bloom and blockmeta bytes, and is generated
        // by `finalize()`, which we are reading anyways,
        // just pass it to a crc hasher.
        let (blockmeta_offset, bloom_offset, orig_checksum) = (
            u32::from_be_bytes([
                footer_buf[0],
                footer_buf[1],
                footer_buf[2],
                footer_buf[3],
            ]) as u64,
            u32::from_be_bytes([
                footer_buf[4],
                footer_buf[5],
                footer_buf[6],
                footer_buf[7],
            ]) as u64,
            u32::from_be_bytes([
                footer_buf[8],
                footer_buf[9],
                footer_buf[10],
                footer_buf[11],
            ]),
        );

        if blockmeta_offset > fsize {
            return Err(SstError::Other(format!(
                "block metadata start offset points past the size of the file (offset: {blockmeta_offset} file_size: {fsize})"
            )));
        }
        if bloom_offset > fsize {
            return Err(SstError::Other(format!(
                "bloom filter start offset points past the size of the file (offset: {bloom_offset} file_size: {fsize})"
            )));
        }
        if blockmeta_offset >= bloom_offset {
            return Err(SstError::Other(
                "block metadata start offset is greater than bloom filter start offset".to_string()
            ));
        }

        let bloom_segment_len: usize =
            (fsize - SST_FOOTER_SIZE as u64 - bloom_offset) as usize;

        let blockmeta_segment_len: usize = (bloom_offset - blockmeta_offset).try_into().map_err(|e| {
            SstError::Other(format!(
                "faild to cast meta_segment_lenght (= file_size {fsize} - meta_offset {blockmeta_offset}) from u64 to usize: {e}"
            ))
        })?;

        let mut hasher = crc32fast::Hasher::new();
        let mut readbuf = vec![0; blockmeta_segment_len];

        f.read_exact_at(&mut readbuf, blockmeta_offset)?;
        let block_metadata = BlockMetadata::decode_metas(&readbuf[..], &codec)?;
        hasher.update(&readbuf[..]);

        readbuf.resize(bloom_segment_len, 0);
        f.read_exact_at(&mut readbuf, bloom_offset)?;

        let bloom = {
            /*
            -------------------------------------------------
            omg, i know i have SpecCodec<I> everywhere
            but i rly dont feel like introducing
            <B: SpecCodec<BloomFilter> to lsm, or sst
            or wherever. Plus realistically a bloom
            filters wire representation is not gonna change.
            -------------------------------------------------
            */

            let codec = BloomFilterCodec;
            match codec.decode(&readbuf) {
                Ok(Some((bloom, n))) => {
                    hasher.update(&readbuf[..n]);
                    bloom
                }
                Ok(None) => {
                    return Err(SstError::Other(
                        "failed to decode bloom filter: not enough bytes in buffer"
                        .to_string()
                    ));
                }
                Err(e) => return Err(e.into()),
            }
        };

        let computed_checksum = hasher.finalize();
        if orig_checksum != computed_checksum {
            return Err(SstError::ChecksumMismatch(crate::ChecksumMismatch {
                original: orig_checksum,
                computed: computed_checksum,
            }));
        }

        Ok(SstHandle {
            fh,
            block_metadata,
            block_metadata_start_offset: blockmeta_offset as usize,
            codec,
            bloom,
        })
    }

    #[inline]
    pub fn block_meta(&self) -> &[BlockMetadata] {
        &self.block_metadata
    }

    pub fn read_block(&self, block_idx: usize) -> Result<Rc<Block>, SstError> {
        let block_start = self
            .block_meta()
            .get(block_idx)
            .ok_or(SstError::BlockIdxOutOfBounds {
                idx: block_idx,
                len: self.block_metadata.len(),
            })?
            .offset as usize;

        let block_end = if block_idx == self.block_meta().len() - 1 {
            self.block_metadata_start_offset
        } else {
            self.block_meta()[block_idx + 1].offset as usize
        };

        debug_assert!(
            block_start < block_end,
            "malformed sst blocks: block_start >= block_end"
        );

        let mut buf = vec![0; block_end - block_start];
        self.fh.file().read_exact_at(&mut buf, block_start as u64)?;

        let b = Block::decode_block(&buf)?;
        Ok(Rc::new(b))
    }

    pub fn cursor<'a>(
        &'a self,
        bounds: Option<(Bound<Key>, Bound<Key>)>,
    ) -> Result<Option<SstCursor<'a, C>>, SstError> {
        let bounds = bounds.unwrap_or((Bound::Unbounded, Bound::Unbounded));
        SstCursor::new(self, bounds)
    }

    pub fn may_contain_stream_id(&self, stream_id: u64) -> bool {
        self.bloom.may_contain(stream_id)
    }

    pub fn may_contain_stream_id_bounds(&self, bounds: (u64, u64)) -> bool {
        (bounds.0..bounds.1)
            .any(|stream_id| self.may_contain_stream_id(stream_id))
    }

    /// looks at which [Block] contains the given stream_id if any
    pub fn block_idx_for_stream_id(
        &self,
        stream_id: u64,
    ) -> Result<usize, SstError> {
        if !self.may_contain_stream_id(stream_id) {
            return Err(SstError::StreamIdNotFound {
                stream_id,
                bound_kind: BoundKind::None,
            });
        }

        // let mut bin_search_error = Ok(());
        let bin_search_output = self
            .block_metadata
            // .binary_search_by(|blockmeta| {
            //     match (blockmeta.first_key.stream_id.cmp(&stream_id), blockmeta.last_key.stream_id.cmp(&stream_id)) {
            //         (Less, Greater) => {
            //             trace!("SstHandle: binary search for stream_id: stream_id is both less than block.first_key but also greater than block.last_key (Note: this should've been imposible because of checking `bloom_filter.may_contain_stream_id(stream_id)`)");
            //             bin_search_error = Err(SstError::BoundNotExist { bound: Bound::Unbounded });
            //             Equal // doesnt matter
            //         }
            //         (Equal, _) => Equal,
            //         (_,Equal) =>  Equal,
            //         (Less, _) => Less,
            //         (_, Greater) => Greater,
            //         (Greater, Less) => Equal,
            //     };
            //     todo!()
            // });
            .binary_search_by_key(&stream_id, |b| b.last_key.stream_id);

        match bin_search_output {
            Ok(i) => Ok(i),
            Err(i) if i <= self.block_metadata.len() => Ok(i),
            Err(_) => Err(SstError::StreamIdNotFound {
                stream_id,
                bound_kind: BoundKind::None,
            }),
        }
    }

    pub fn file_handle(&self) -> &SstFileHandle {
        &self.fh
    }
}

#[derive(Debug)]
pub struct SstCursor<'s, C> {
    sst: &'s SstHandle<C>,
    block_cursor: Result<BlockCursor<C>, SstError>,
    block_idx: usize,
    _start_bound: Bound<Key>,
    end_bound: Bound<Key>,
    codec: C,
}

impl<'s, C> SstCursor<'s, C>
where
    C: RecordCodecExt,
{
    /// Creates a new cursor from an [SstHandle] in
    /// the range `(bounds.0..bounds.1)`.
    ///
    /// If the start bound is `Bound::Unbounded`,
    /// the cursor will begin from the start of the
    /// first block.
    ///
    /// If its `Bound::Included`, it looks up which block
    /// the starting stream_id belongs to, opens it,
    /// and seeks to the start key, propagating any
    /// errors encountered.
    ///
    /// If its `Bound::Excluded`, the process is similar,
    /// except the after seeking to the start key, we
    /// advance the inner cursor by one.
    ///
    /// # Note
    ///
    /// If the block containing the start keys `stream_id`
    /// is not in this SSTable, `SstError::StreamIdNotFound`.
    #[instrument(skip(sst), fields(sst_id = sst.id()), level = "TRACE")]
    fn new(
        sst: &'s SstHandle<C>,
        bounds: (Bound<Key>, Bound<Key>),
    ) -> Result<Option<Self>, SstError> {
        let start_block_idx = match bounds.0.as_ref() {
            Bound::Included(k) | Bound::Excluded(k) => {
                sst.block_idx_for_stream_id(k.stream_id)?
            }
            Bound::Unbounded => 0,
        };
        trace!("new cursors start_block_idx = {start_block_idx}");

        let block = sst.read_block(start_block_idx)?;
        let mut cursor = BlockCursor::new(block, sst.codec.clone());

        match bounds.0.as_ref() {
            Bound::Included(k) => {
                trace!("block_cursor > seeking to key");
                cursor.seek(&k);
            }
            Bound::Excluded(k) => {
                trace!("block_cursor > seeking to past the key (once)");
                cursor.seek(&k);
                if Some(k) == cursor.current_record().map(|r| &r.key) {
                    cursor.next();
                }
            }
            Bound::Unbounded => {
                trace!("block_cursor > seeking to first");
                cursor.seek_to_first();
            }
        }

        Ok(Some(Self {
            sst,
            block_cursor: Ok(cursor),
            block_idx: start_block_idx,
            codec: sst.codec.clone(),
            _start_bound: bounds.0,
            end_bound: bounds.1,
        }))
    }

    #[inline]
    fn _is_in_bounds(&self, key: &Key) -> bool {
        let after_start = match &self._start_bound {
            Bound::Included(b) => key >= b,
            Bound::Excluded(b) => key > b,
            Bound::Unbounded => true,
        };

        let before_end = match &self.end_bound {
            Bound::Included(b) => key <= b,
            Bound::Excluded(b) => key < b,
            Bound::Unbounded => true,
        };

        after_start && before_end
    }
    #[inline]
    pub fn is_record(&self) -> bool {
        self.block_cursor
            .as_ref()
            .map(|c| c.is_record())
            .unwrap_or(false)
    }

    #[inline]
    pub fn is_error(&self) -> bool {
        match self.block_cursor.as_ref() {
            Err(_) => true,
            Ok(c) => c.is_error(),
        }
    }

    #[inline]
    pub fn get_error(&self) -> Option<Result<&SstError, &CodecError>> {
        match self.block_cursor.as_ref() {
            Err(e) => Some(Ok(e)),
            Ok(c) => match c.get_error() {
                Some(e) => Some(Err(e)),
                None => None,
            },
        }
    }

    // cant clone an Error that wraps an io::Error
    // #[inline]
    // pub fn get_error_cloned(&self) -> Option<Result<SstError, CodecError>> {
    //     match self.block_cursor.as_ref() {
    //         Err(e) => Some(Ok(e.clone())),
    //         Ok(c) => c.get_error().map(|e| Err(e.clone())),
    //     }
    // }

    #[inline]
    pub fn get_codec_error(&self) -> Option<&CodecError> {
        self.block_cursor.as_ref().ok().and_then(|c| c.get_error())
    }

    #[inline]
    pub fn get_sst_error(&self) -> Option<&SstError> {
        self.block_cursor.as_ref().err()
    }

    #[inline]
    pub fn current_record(&self) -> Option<&Record> {
        self.block_cursor
            .as_ref()
            .map(|c| c.current_record())
            .ok()?
    }

    #[inline]
    pub fn current(
        &self,
    ) -> Result<Option<&Record>, Result<&SstError, &CodecError>> {
        match self.block_cursor.as_ref() {
            Ok(c) => match c.current() {
                Ok(opt) => Ok(opt),
                Err(e) => Err(Err(e)),
            },
            Err(e) => Err(Ok(e)),
        }
    }

    #[inline]
    pub fn take_current(&mut self) -> Option<Record> {
        if let Ok(c) = self.block_cursor.as_mut() {
            c.take_current()
        } else {
            None
        }
    }

    #[inline]
    #[instrument(skip(self), fields(block_idx = self.block_idx, block_count = self.sst.block_meta().len()))]
    pub fn next(&mut self) {
        if let Ok(c) = self.block_cursor.as_mut() {
            trace!("advancing inner cursor");
            c.next();

            match c.current() {
                Ok(Some(rec)) => {
                    if !c.is_error() {
                        trace!("after advance: inner cursor no longer yields records");
                        return;
                    }

                    if !self.is_error() {
                        trace!(
                            "inner cursor is empty, advancing to the next one"
                        );
                        self.block_idx += 1;
                        self.update_current();
                    }
                    trace!("next record is {rec:?}")
                }
                Ok(None) => {
                    trace!("after advance: block_cursor.current() returned Ok(None), not enough bytes to decode a Record")
                }
                Err(_) => todo!(),
            };
        };

        if !self.is_error() {
            self.block_idx += 1;

            if !self.next_block_may_be_in_bounds() {
                self.block_cursor = Err(SstError::BlockCursorExhausted);
                return;
            }

            self.update_current();
        }
    }

    fn _next(&mut self) {
        let current = {
            let Ok(bc) = self.block_cursor.as_mut() else {
                return;
            };
            bc.next()
            bc.current()
        };

        let span = span!(Level::TRACE, "block_cursor_next");
        let _guard = span.enter();

        match current {
            Ok(Some(rec)) => {
                trace!("inner cursor no longer yields records");

                if bc.is_error() {
                    trace!("inner cursor is empty, advancing to the next one");
                    // self.block_idx += 1;
                    // self.update_current();
                }
                trace!("next record is {rec:?}")
            }
            Ok(None) => {
                trace!("after advance: block_cursor.current() returned Ok(None), not enough bytes to decode a Record")
            }
            Err(e) => {}
        }
    }

    fn next_block_may_be_in_bounds(&self) -> bool {
        let Some(meta) = self.sst.block_meta().get(self.block_idx) else {
            return false;
        };
        match &self.end_bound {
            Bound::Included(end) => &meta.first_key <= end,
            Bound::Excluded(end) => &meta.first_key < end,
            Bound::Unbounded => true,
        }
    }

    pub fn seek_to_first(&mut self) {
        self.block_idx = 0;
        // internally it calls "new_cursor".seek_to_first()
        self.update_current();
    }

    #[instrument(skip(self))]
    pub fn seek(&mut self, seek_key: &Key) {
        use std::cmp::Ordering::*;
        let mut bin_search_error = None;

        // ...copied from BlockCursor::seek():
        // TODO: instead of turning the bytes into Key and then comparing
        // we could just compare the bytes themselves (disregarding Key::stream_id: u64, the last 8 bytes)
        let bin_search_output = self.sst.block_meta().binary_search_by(|block| {
            match (
                seek_key.cmp(&block.first_key),
                seek_key.cmp(&block.last_key),
            ) {
                (Less, Greater)=> {
                    trace!("binary search: block is not sorted: seek key is both less than block.first_key but also greater than block.last_key");
                    bin_search_error = Some(SstError::InvalidBlockNotSorted);
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
        trace!("binary search result = {:?}", bin_search_output);

        self.block_idx = match bin_search_output {
            Ok(i) => i, // exact match

            // first elem -> target, self.update_current() will
            // signal failure if the index cant be found
            Err(i) => i,
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
            self.block_cursor = Err(SstError::BlockIdxOutOfBounds {
                idx: self.block_idx,
                len: self.sst.block_meta().len(),
            });
            return;
        }

        let block = match self.sst.read_block(self.block_idx) {
            Ok(b) => b,
            Err(e) => {
                trace!("failed to read next block, error = {e:?}");
                self.block_cursor = Err(e);
                return;
            }
        };
        trace!(
            "read next block, creating next cursor and seeking to its start..."
        );

        let mut cursor = BlockCursor::new(block, self.codec.clone());
        cursor.seek_to_first();
        self.block_cursor = Ok(cursor);
    }
}

/// Wrapper around a file, a unique ID, and the files path
#[derive(Debug)]
pub struct SstFileHandle {
    _id: u64,
    file: File,
    path: PathBuf,
}

impl SstFileHandle {
    pub fn open(
        id: u64,
        prefix: Option<&Path>,
        opts: &mut OpenOptions,
    ) -> io::Result<Self> {
        let fmt = format_sst_filename(id);
        let path = match prefix {
            Some(p) => p.join(fmt),
            None => fmt.into(),
        };
        let file = opts.open(&path)?;
        Ok(Self {
            _id: id,
            path,
            file,
        })
    }

    /// checks if the path is a valid sst file path, then opens it
    /// as read-only
    pub fn open_from_path(path: &Path) -> io::Result<Self> {
        let fname =
            path.file_name().and_then(|s| s.to_str()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing filename")
            })?;

        if !fname.ends_with(".sst") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not an .sst file",
            ));
        }

        let digits = &fname[..fname.len() - ".sst".len()];
        let id: u64 = digits.parse().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse sst id: {e}"),
            )
        })?;

        Ok(SstFileHandle {
            _id: id,
            file: File::open(path)?,
            path: path.to_path_buf(),
        })
    }

    #[inline]
    pub fn file(&self) -> &File {
        &self.file
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl io::Write for SstFileHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file().flush()
    }
}

impl io::Read for SstFileHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file().read(buf)
    }
}

impl io::Seek for SstFileHandle {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.file().seek(pos)
    }
}

pub const SZ_U32: usize = size_of::<u32>();

/// Metadata for blocks inside an SSTable. It contains the blocks start in bytes as offset
/// and the first/last keys in this block.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct BlockMetadata {
    offset: u32,
    first_key: Key,
    last_key: Key,
}

impl WireLen for BlockMetadata {
    fn wire_len(&self) -> usize {
        Self::BLOCK_META_SIZE
    }
}

impl BlockMetadata {
    pub const BLOCK_META_SIZE: usize = size_of::<u32>() + 2 * KEY_SIZE;

    /// # Format
    ///
    /// ```not_rust
    /// [len: u32]
    /// // len times many metas:
    /// [meta: offset: u32, first_key: 4 * u64, last_key: 4 * u64]
    /// ...
    /// ```
    #[instrument(skip(metas), fields(metas_count = metas.len(), codec = ?codec), err)]
    pub fn encode_metas<C: SpecCodec<Key>>(
        metas: &[Self],
        codec: &C,
    ) -> Result<Vec<u8>, CodecError> {
        // need enough bytes the lenght prefix, for the metas themselves,
        // and for the 32bit checksum
        let capacity = SZ_U32 + metas.len() * Self::BLOCK_META_SIZE + SZ_U32;
        let mut data = Vec::with_capacity(capacity);
        data.extend((metas.len() as u32).to_be_bytes());
        let mut key_bytes = [0; KEY_SIZE];
        let mut encode_key = |key: &Key| -> Result<[u8; KEY_SIZE], CodecError> {
            codec.encode(key, &mut key_bytes)?;
            Ok(key_bytes)
        };

        for m in metas {
            data.extend(&m.offset.to_be_bytes());
            data.extend(encode_key(&m.first_key)?);
            data.extend(encode_key(&m.last_key)?);
        }
        let checksum = crc32fast::hash(&data[..]);
        data.extend(checksum.to_be_bytes());
        debug_assert_eq!(
            capacity,
            data.len(),
            "before encoding we allocated {} bytes but wrote {}",
            capacity,
            data.len()
        );
        Ok(data)
    }

    #[instrument(skip(src), err)]
    pub fn decode_metas<C: SpecCodec<Key>>(
        src: &[u8],
        codec: &C,
    ) -> Result<Vec<Self>, CodecError> {
        if src.len() < SZ_U32 {
            trace!("not enough bytes for length prefix");
            return Err(CodecError::NotEnoughBytes(codec::NotEnoughBytes {
                got: src.len(),
                want: SZ_U32,
            }));
        }
        let metas_len =
            u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let mut consumed = SZ_U32; // we already read length prefix

        let want_total_size =
            SZ_U32 + metas_len * Self::BLOCK_META_SIZE + SZ_U32;
        if src.len() < want_total_size {
            trace!(
                "not enough bytes for length prefix + {metas_len} * block metas + checksum. src.len() = {}, want_total = {want_total_size} = length prefix {SZ_U32} + {} (= metas_len {metas_len} * BlockMetadata::wire_len() {}) + checksum u32 {SZ_U32}",
                src.len(),
                metas_len * Self::BLOCK_META_SIZE,
                Self::BLOCK_META_SIZE
            );
            return Err(CodecError::NotEnoughBytes(codec::NotEnoughBytes {
                got: src.len(),
                want: want_total_size,
            }));
        }

        let decode_key = |src: &[u8]| -> Result<Key, CodecError> {
            match codec.decode(src) {
                Ok(Some((key, _))) => Ok(key),
                Ok(None) => Err(codec::other(
                    "BlockMetadata::decode_metas(): failed to decode key from raw bytes",
                )),
                Err(e) => Err(e),
            }
        };

        let mut data = Vec::with_capacity(metas_len);
        for _ in 0..metas_len {
            let offset = u32::from_be_bytes([
                src[consumed],
                src[consumed + 1],
                src[consumed + 2],
                src[consumed + 3],
            ]);
            consumed += SZ_U32;
            let first_key = decode_key(&src[consumed..consumed + KEY_SIZE])?;
            consumed += KEY_SIZE;
            let last_key = decode_key(&src[consumed..consumed + KEY_SIZE])?;
            consumed += KEY_SIZE;

            let meta = BlockMetadata {
                offset,
                first_key,
                last_key,
            };
            data.push(meta);
        }

        let computed = crc32fast::hash(&src[..consumed]);
        let stored = u32::from_be_bytes([
            src[consumed],
            src[consumed + 1],
            src[consumed + 2],
            src[consumed + 3],
        ]);
        if stored != computed {
            return Err(codec::other(format!(
                "checksum mismatch, {stored:#x} (stored) != {computed:#x} (computed)"
            )));
        }

        Ok(data)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use super::BlockMetadata;
    use crate::{
        block::DEFAULT_BLOCK_SIZE,
        record::{Record, RecordCodec},
        sst::{HashedWriterState, SstFileWriter, SstHandle},
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
            payload: vec![67u8; len].into(),
        }
    }

    #[test]
    fn block_meta_codec() {
        tracing();
        let codec = RecordCodec;
        let metas: Vec<BlockMetadata> = (0..2)
            .map(|i| BlockMetadata {
                offset: i as u32,
                first_key: crate::record::Key {
                    source_id: i + 1,
                    timestamp: i + 1,
                    sequence_num: i + 1,
                    stream_id: i + 1,
                },
                last_key: crate::record::Key {
                    source_id: i + 2,
                    timestamp: i + 2,
                    sequence_num: i + 2,
                    stream_id: i + 2,
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
        let codec = RecordCodec;
        let writer = std::io::Cursor::new(Vec::new());
        let num_records = 512usize;
        let mut sw =
            HashedWriterState::new(writer, codec, None, num_records).unwrap();

        for i in 0..num_records {
            sw.write(&record(i as u64, 512)).unwrap();
        }

        let state = sw.finalize().unwrap();
        assert_eq!(state.inner.record_count, num_records);
        dbg!(state);
    }

    #[test]
    fn sst_file_writer() {
        let tempdir = tempfile::TempDir::new().unwrap();

        tracing();
        let codec = RecordCodec;
        let num_records = 512;
        let mut sw = SstFileWriter::new(
            1,
            codec,
            None,
            Some(tempdir.path()),
            num_records,
        )
        .unwrap();

        for i in 0..num_records {
            sw.write(&record(i as u64, 512)).unwrap();
        }

        let sst = sw.finalize_file().unwrap();
        dbg!(sst);
    }

    #[test]
    fn sst_read_block() {
        let tempdir = tempfile::TempDir::new().unwrap();
        tracing();
        let codec = RecordCodec;
        let num_records = 10usize;
        let mut sw = SstFileWriter::new(
            1,
            codec,
            None,
            Some(tempdir.path()),
            num_records,
        )
        .unwrap();
        for i in 0..num_records {
            sw.write(&record(i as u64, 512)).unwrap();
        }
        dbg!(&sw);
        let sst = sw.finalize_file().unwrap();
        // assert_eq!(sst.meta._record_count, num_records);
        dbg!(&sst);

        for i in 0..sst.block_meta().len() {
            let block = sst.read_block(i).unwrap();
            dbg!(block);
        }

        match sst.read_block(sst.block_meta().len()) {
            Err(e) => {
                dbg!(e);
            }
            Ok(_) => panic!(
                "reading past the end of blocks of sst shouldve errored!"
            ),
        }
    }

    #[test]
    fn sst_cursor() {
        let tempdir = tempfile::TempDir::new().unwrap();
        tracing();
        let codec = RecordCodec;
        let num_records = 10;
        let mut sw = SstFileWriter::new(
            1,
            codec,
            None,
            Some(tempdir.path()),
            num_records,
        )
        .unwrap();
        let records: Vec<Record> =
            (0..num_records).map(|i| record(i as u64, 512)).collect();
        for r in &records {
            sw.write(r).unwrap();
        }
        dbg!(&sw);
        let sst = Arc::new(sw.finalize_file().unwrap());
        // assert_eq!(sst.meta._record_count, num_records);

        let mut c = sst
            .cursor(None)
            .expect("sst::cursor -> Err")
            .expect("sst::Cursor -> Ok(None)");
        println!("\n\n=> CURSOR: iterating using .next() <=\n\n");
        println!(
            "Before iterating the cursor: sst has {} block(s)",
            sst.block_meta().len(),
            // sst.meta._record_count
        );

        let mut i = 0;
        loop {
            if let Some(joint_err) = c.get_error() {
                match joint_err {
                    Ok(se) => {
                        println!("cursor stopped from sst read error: {se}")
                    }
                    Err(ce) => {
                        println!("cursor stopped from codec error: {ce}")
                    }
                }
                break;
            }

            match c.current_record() {
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
                    Ok(se) => panic!(
                        "{i}/{num_records}: cursor stopped from sst read error: {se}"
                    ),
                    Err(ce) => panic!(
                        "{i}/{num_records}:cursor stopped from codec error: {ce}"
                    ),
                }
            }

            println!("{i}/{num_records}: {:?}", c.current_record());
            assert_eq!(
                Some(r),
                c.current_record(),
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
                    Ok(se) => panic!(
                        "{i}/{num_records}: cursor stopped from sst read error: {se}"
                    ),
                    Err(ce) => panic!(
                        "{i}/{num_records}:cursor stopped from codec error: {ce}"
                    ),
                }
            }

            println!("{i}/{num_records}: {:?}", c.current_record());
            assert_eq!(
                Some(&r),
                c.current_record(),
                "{i}/{num_records}: c.current() returned None"
            );
        }
    }

    #[test]
    fn sst_open() {
        tracing();
        let tempdir = tempfile::TempDir::new().unwrap();
        let codec = RecordCodec;
        let block_limit = DEFAULT_BLOCK_SIZE;
        let num_records = 10;
        let mut sw = SstFileWriter::new(
            1,
            codec,
            Some(block_limit),
            Some(tempdir.path()),
            num_records,
        )
        .unwrap();

        for i in 0..num_records {
            sw.write(&record(i as u64, 512)).unwrap();
        }

        let sst = sw.finalize_file().unwrap();
        dbg!(&sst);
        let fh = sst.file_handle();

        SstHandle::open_path(&fh.path, codec).unwrap();
    }
}
