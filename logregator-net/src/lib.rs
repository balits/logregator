#![feature(iter_map_windows)]
#![feature(mpmc_channel)]
#![allow(unused)]
// use std::sync::mpmc;

use std::{
    marker::PhantomData,
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    rc::Rc,
    sync::{Arc, mpmc},
    thread,
};

use futures::sink::SinkExt;
use logregator_tsdb::{
    codec::{RecordCodecExt, SpecCodec},
    label::{LabelMap, LabeledIter},
    lsm::{LsmTree, RecoverOptions},
};
use tokio::{
    io::{ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, FramedRead, FramedWrite};

use bytes::{Bytes, BytesMut};
use tracing::{error, info, instrument, trace, warn};

#[instrument(skip_all, err)]
async fn task_per_conn(
    localset: tokio::task::LocalSet,
    addr: std::net::SocketAddr,
    req_sender: mpmc::Sender<Request>,
    resp_recv: mpmc::Receiver<Response>,
) -> Result<(), NetError> {
    let socket = TcpListener::bind(addr).await?;

    loop {
        let (conn, _addr) = socket.accept().await?;
        // these are Arc<Mutex<_>>-es, wouldnt a signle Framed<TcpStream, Codec> be more efficient?
        // well yeah but then i couldnt separate my Request and Response enums, as a single
        // Framec<_, Codec> takes only a single codec
        let (readhalf, writehalf) = tokio::io::split(conn);

        let request_codec = RequestCodec {};
        let response_codec = ResponseCodec {};

        let mut fread = FramedRead::new(readhalf, request_codec);
        let mut fwrite = FramedWrite::new(writehalf, response_codec);

        let req_sender = req_sender.clone();
        let resp_recv = resp_recv.clone();

        localset.spawn_local(async move {
            if let Err(e) =
                conn_loop(fread, fwrite, req_sender, resp_recv).await
            {
                error!("connection loop errored: {e}")
            }
        });
    }

    Ok(())
}

#[instrument(skip_all, err)]
async fn conn_loop(
    fread: FramedRead<ReadHalf<TcpStream>, RequestCodec>,
    fwrite: FramedWrite<WriteHalf<TcpStream>, ResponseCodec>,
    req_sender: mpmc::Sender<Request>,
    resp_recv: mpmc::Receiver<Response>,
) -> Result<(), NetError> {
    let resp: Response = todo!();

    tokio::select! {
        req = fread.next() => {
            let req = match req{
                Some(Ok(r)) => r,
                Some(Err(e)) => return Err(e),
                None => {
                    warn!("framed read closed");
                    return Ok(());
                },
            };

            match req_sender.send(req) {
                Ok(_) =>  {
                    trace!("request read + sent through channel");
                },
                Err(e) => {
                    warn!("request could not be sent through channel, error = {:?}", &e);
                    return Err(e.into())
                },
            }
        },

        _ = async {} => {
            match resp_recv.try_recv() {
                Ok(r) => {
                    fwrite.send(resp).await.inspect_err(|e| {
                        warn!("response could not be written to TcpStream")
                    })?;
                },
                Err(mpmc::TryRecvError::Disconnected) => {
                    return Err(NetError::ResponseRecvDisconnected())
                },
                Err(mpmc::TryRecvError::Empty) => {},

           };
        }
    };
    Ok(())
}

async fn lsm_loop<'lsm, R, L>(
    lsm: &'lsm mut logregator_tsdb::lsm::LsmTree<R, L>,
    mut req_recv: tokio::sync::mpsc::Receiver<Request>,
    resp_sender: tokio::sync::mpsc::Sender<Response>,
    recv_buf_size: usize,
    labeled_iter_buf: usize,
) where
    'lsm: 'static,
    R: logregator_tsdb::codec::RecordCodecExt + 'lsm,
    L: logregator_tsdb::codec::SpecCodec<logregator_tsdb::label::LabelMap>
        + 'lsm,
{
    assert_ne!(recv_buf_size, 0, "net: recv_buf_size must be non-zero");
    assert_ne!(
        labeled_iter_buf, 0,
        "net: labeled_iter_buf must be non-zero"
    );

    let mut recv_buf = Vec::with_capacity(recv_buf_size);

    loop {
        if req_recv.recv_many(&mut recv_buf, recv_buf_size).await == 0 {
            info!("request queue closed and empty");
            return;
        }

        for r in recv_buf.drain(..) {
            match r {
                Request::AppendBatch(bytes_offset) => todo!(),
                Request::Range {
                    labels,
                    start_time,
                    end_time,
                } => {
                    let label_iter = labels
                        .iter()
                        .map(|(k, v)| (k.as_utf8_str(), v.as_utf8_str()));

                    match lsm.range(label_iter, start_time, end_time) {
                        Ok(iter) => {
                            let type_erased_iter: ArcedLabeledIter =
                                Arc::new(iter.map(|(l, r)| {
                                    // r has 4 u64 and an Arc<[u8]>, i think its safe to
                                    // clone until further profiling shows it isnt
                                    (l, r.into_owned())
                                }));

                            resp_sender.send(Response::Range(type_erased_iter));
                        }
                        Err(e) => error!("{e}"),
                    }
                }
            };
        }
    }
}

pub struct BytesUtf8(Bytes);

impl BytesUtf8 {
    pub fn from_bytes(src: Bytes) -> Result<Self, std::str::Utf8Error> {
        let _ = std::str::from_utf8(&src)?;
        Ok(Self(src))
    }

    pub fn as_utf8_str(&self) -> &str {
        std::str::from_utf8(self.0.as_ref())
            .expect("net: BytesUtf8 should always be valid utf8, since theres only 1, utf8-proof way to contstruct it")
    }
}

pub enum Request {
    AppendBatch(BytesOffset),
    Range {
        labels: Vec<(BytesUtf8, BytesUtf8)>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    },
}

impl Request {
    pub fn encode_into(&self, dst: &mut [u8]) {}
}

pub struct BytesOffset {
    raw: bytes::Bytes,
    offsets: Vec<NonZeroUsize>,
}

#[repr(u8)]
enum RequestKind {
    AppendBatch,
    /// Range is a kind of request where the bytes
    /// could be decoded roughly as length-prefixed
    /// list of utf-8-encoded key-value pair known as [Label](logregator-tsdb/src/label.rs).
    ///
    /// Currenlty the tsdb crate requires an iterator over
    /// key-value pairs of type `(AsRef<str>, AsRef<str>)`.
    /// This is because the crates [StreamRegistry] keeps track of reference-counted
    /// strings for cheap cloning. Since turning the incoming bytes into [str]s,
    /// then allocating a new [Rc<str>] for each label in the query would be wasteful
    /// [AsRef<str>] is perfectly fine, since its only used for comparisons anyways
    ///
    /// This means the layout is similar to the layout of [LabelMap]
    Range,
}

#[derive(Debug, Clone, Copy)]
struct RequestCodec {}

impl RequestCodec {
    /// Parses the input bytes as a length-prefixed,
    /// utf-8 encoded string, then returns the remaining
    /// bytes and the parsed string, returning an error if
    /// its not valid-utf, or if parsing wasn't succesful otherwise.
    ///
    /// > copied from logregator-tsdb/src/label.rs,
    /// > because exporting that funciton from that deep
    /// > feels icky, but i should merge these into
    /// > logregator-tsdb/src/codec.rs later
    fn parse_lp_str(src: &[u8]) -> nom::IResult<&[u8], &str> {
        let (input, len) = nom::number::complete::be_u8(src)?;

        let len = len as usize;

        if !(logregator_tsdb::label::MIN_STR_SIZE
            ..logregator_tsdb::label::MIN_STR_SIZE)
            .contains(&len)
        {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::LengthValue, // LengthValue?
            )));
        }

        let (input, str_bs) = nom::bytes::complete::take(len)(input)?;
        let s = std::str::from_utf8(str_bs).map_err(|_| {
            nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::MapRes,
            ))
        })?;
        Ok((input, s))
    }

    /// Parses the input bytes for a length-prefixed list
    /// of key-value pairs, where each key-value is itself
    /// a lenght-prefixed utf-8-encoded [str].
    ///
    /// As per the quirks of the [logregator-tsdb] crate,
    /// each [LabelMap] can only hold [MAX_LABEL_COUNT] labels,
    /// and each label key and label values size must be between [MIN_STR_SIZE] and [MAX_STR_SIZE].
    ///
    /// In practice right now these constants are [u8::MAX],
    /// meaning each length prefix fits into 1 single byte.
    fn parse_lp_str_vec(src: &[u8]) -> nom::IResult<&[u8], Vec<(&str, &str)>> {
        let (input, label_count) = nom::number::complete::be_u8(src)?;
        let (input, str_bs) = nom::bytes::complete::take(label_count)(input)?;

        let mut v = Vec::with_capacity(label_count as usize);

        for _ in 0..label_count {
            let (input, key) = Self::parse_lp_str(input)?;
            let (input, val) = Self::parse_lp_str(input)?;
            v.push((key, val));
        }

        Ok((input, v))
    }
}

impl tokio_util::codec::Decoder for RequestCodec {
    type Item = Request;

    type Error = NetError;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            src.reserve(1);
            return Ok(None);
        }
        let tag = src[0];

        match tag {
            0 => {
                todo!("impl decode for Request::AppendBatch")
            }
            1 => {
                todo!("impl decode for Request::Range")
            }
            _ => Err(NetError::Codec(format!(
                "decode: unknown tag {tag} for Request"
            ))),
        }
    }
}

type ArcedLabeledIter = Arc<
    dyn Iterator<
            Item = (
                Option<Arc<logregator_tsdb::label::LabelMap>>,
                logregator_tsdb::record::Record,
            ),
        > + 'static,
>;

enum Response {
    Range(ArcedLabeledIter),
}

// FIXME: if LabelMap and Record is Send,
// and LabeledIter is Send, then Arc<dyn Iterator<Item = (LabelMap, Record)>>
// should be send also, no?
unsafe impl Send for Response {}

#[derive(Debug, Clone, Copy)]
struct ResponseCodec {}

impl tokio_util::codec::Encoder<Response> for ResponseCodec {
    type Error = NetError;

    fn encode(
        &mut self,
        item: Response,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        todo!("implement Encoder for ResponseCodec")
    }
}

#[derive(thiserror::Error, Debug)]
pub enum NetError {
    #[error("net error: some other error occured: 0")]
    Message(String),

    #[error("net error: std::io:error: {0}")]
    Io(Arc<std::io::Error>),

    #[error("net error: failed to send request through std::mpmc channel: {0}")]
    RequestStdSend(#[from] mpmc::SendError<Request>),

    #[error("net error: failed to send request through flume channel: {0}")]
    RequestFlumeSend(#[from] flume::SendError<Request>),

    #[error("net error: response channels reciever side disconnected")]
    ResponseRecvDisconnected(),

    #[error("net error: codec failed: {0}")]
    Codec(String),
}

impl From<std::io::Error> for NetError {
    fn from(e: std::io::Error) -> Self {
        NetError::Io(Arc::new(e))
    }
}

/// this yields raw payloads from the underlying batch
/// in the form of `Iterator<Item=Bytes>>` which
/// could be used in the lsm-tree as `Iterator<Item=AsRef<[u8]>>`
struct AppendBatchIter<'a> {
    inner: &'a BytesOffset,
    offset_idx: Option<usize>,
}

impl<'a> AppendBatchIter<'a> {
    pub(crate) fn new(batch: &'a BytesOffset) -> Self {
        debug_assert!(
            !batch.offsets.is_empty(),
            "logregator-net: AppendBatches offset array is empty"
        );
        Self {
            inner: batch,
            offset_idx: Some(0),
        }
    }
}

impl<'a> Iterator for AppendBatchIter<'a> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        match self.offset_idx {
            Some(start_offset_idx) => {
                let mut next_start_offset = None;
                let end: usize = if start_offset_idx + 1
                    < self.inner.offsets.len()
                {
                    let end_offset = unsafe {
                        *self.inner.offsets.get_unchecked(start_offset_idx + 1)
                    };
                    next_start_offset = Some(end_offset.get());
                    end_offset.get()
                } else {
                    self.inner.offsets.len()
                };

                let start_offset = self.inner.offsets[start_offset_idx].get();
                let slice = self.inner.raw.slice(start_offset..end);
                self.offset_idx = next_start_offset;
                Some(slice)
            }
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetOptions {
    addr: std::net::SocketAddr,
    tcp_backlog_size: u32,
    num_shards: NonZeroU64,
    request_codec: RequestCodec,
    response_codec: ResponseCodec,
    request_channel_bound: Option<usize>,
    response_channel_bound: Option<usize>,
    io_channel_bound: Option<usize>,
}

impl NetOptions {
    #[cfg(test)]
    pub fn new_test_options(num_shards: NonZeroU64) -> Self {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        Self {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 6767),
            tcp_backlog_size: 64,
            num_shards,
            request_codec: RequestCodec {},
            response_codec: ResponseCodec {},
            request_channel_bound: None,
            response_channel_bound: None,
            io_channel_bound: None,
        }
    }

    pub fn new_from_file(
        path: impl AsRef<std::path::Path>,
    ) -> std::io::Result<Self> {
        unimplemented!("open + parse config file for NetOptions (json / toml)")
    }
}

pub fn create_shard<R, L>(
    shard_id: shard::ShardID,
    lb: Arc<shard::LoadBalancer>,
    net_options: Arc<NetOptions>,
    lsm_recover_opts: RecoverOptions<R, L>,
) -> Result<shard::Shard<R, L>, NetError>
where
    R: RecordCodecExt + Sync + Send + 'static,
    L: SpecCodec<LabelMap> + Sync + Send + 'static,
{
    assert!(
        shard_id.0 < lb.num_shards_usize(),
        "spawn shard must be < `num_shards`"
    );

    // let net_options =
    //     NetOptions::new_from_file(net_options_path).map_err(|e| {
    //         NetError::Message(format!(
    //             "failed to open/parse logregator-net options from path {}: {}",
    //             net_options_path.display(),
    //             e
    //         ))
    //     })?;

    let shard_parts = shard::create_shard_parts(&net_options);

    let shard = shard::Shard::new(
        shard_id,
        net_options.clone(),
        &lsm_recover_opts,
        shard_parts,
        lb.clone(),
    )
    .expect("failed to create `Shard` instance for thread: {thread_name}");

    Ok(shard)
}

// other stuff
mod shard {
    use futures::sink::SinkExt;
    use logregator_tsdb::{
        codec::{RecordCodecExt, SpecCodec},
        label::LabelMap,
        lsm::{LsmTree, LsmTreeOptions, RecoverOptions},
    };
    use std::{
        fmt::Debug,
        net::Ipv4Addr,
        num::{NonZeroU64, NonZeroUsize},
        sync::Arc,
    };
    use tokio::{
        io::{ReadHalf, WriteHalf},
        net::{TcpSocket, TcpStream},
        runtime::LocalOptions,
    };
    use tokio_stream::StreamExt;
    use tokio_util::codec::{FramedRead, FramedWrite};
    use tracing::{error, info, instrument, trace, warn};

    use crate::NetError;

    #[derive(Debug, Clone)]
    pub struct LoadBalancer {
        num_shards: NonZeroU64,
    }

    impl LoadBalancer {
        pub fn new(num_shards: NonZeroU64) -> Option<Self> {
            if num_shards.get() > usize::MAX as u64 {
                error!(
                    "net: num_shards: u64 cannot exceed usize::MAX (so it can be cast as usize)"
                );
                return None;
            }
            Some(Self { num_shards })
        }

        pub fn hash_to_shard(&self, source_id: u64) -> ShardID {
            // # SAFETY
            //
            // new() already checked that num_shards is <= usize::MAX, so this cast is fine
            ShardID(source_id.rem_euclid(self.num_shards().get()) as usize)
        }

        #[inline]
        pub fn num_shards(&self) -> NonZeroU64 {
            self.num_shards
        }

        #[inline]
        pub fn num_shards_usize(&self) -> usize {
            // # SAFETY
            //
            // new() already checked that num_shards is <= usize::MAX, so this cast is fine
            self.num_shards.get() as usize
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct ShardID(pub usize);

    pub struct ShardParts<R> {
        request_codec: crate::RequestCodec,
        response_codec: crate::ResponseCodec,

        request_sender: flume::Sender<crate::Request>,
        request_recv: flume::Receiver<crate::Request>,

        response_sender: flume::Sender<crate::Response>,
        response_recv: flume::Receiver<crate::Response>,

        io_sender: flume::Sender<logregator_tsdb::io::IoEvent<R>>,
        io_recv: flume::Receiver<logregator_tsdb::io::IoEvent<R>>,
    }

    pub fn read_shard_options() {}

    #[allow(clippy::too_many_arguments)]
    pub fn create_shard_parts<R>(options: &crate::NetOptions) -> ShardParts<R> {
        let (request_sender, request_recv) = match options.request_channel_bound
        {
            Some(bound) => flume::bounded(bound),
            None => flume::unbounded(),
        };

        let (response_sender, response_recv) =
            match options.response_channel_bound {
                Some(bound) => flume::bounded(bound),
                None => flume::unbounded(),
            };

        let (io_sender, io_recv) = match options.io_channel_bound {
            Some(bound) => flume::bounded(bound),
            None => flume::unbounded(),
        };

        ShardParts {
            request_codec: options.request_codec,
            response_codec: options.response_codec,
            request_sender,
            request_recv,
            response_sender,
            response_recv,
            io_sender,
            io_recv,
        }
    }

    use crate::NetOptions;

    pub struct Shard<R: RecordCodecExt, L: SpecCodec<LabelMap>> {
        id: ShardID,
        net_options: Arc<NetOptions>,
        parts: ShardParts<R>,
        lb: Arc<LoadBalancer>,
        lsm: LsmTree<R, L>,
    }

    impl<R: RecordCodecExt, L: SpecCodec<LabelMap>> Debug for Shard<R, L> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Shard")
                .field("shard_id", &self.id)
                .field("addr", &self.net_options.addr)
                .finish_non_exhaustive()
        }
    }

    impl<R, L> Shard<R, L>
    where
        R: RecordCodecExt + 'static,
        L: SpecCodec<LabelMap> + 'static,
    {
        pub fn new(
            id: ShardID,
            net_options: Arc<NetOptions>,
            lsm_recover_options: &RecoverOptions<R, L>,
            parts: ShardParts<R>,
            lb: Arc<LoadBalancer>,
        ) -> Result<Self, NetError> {
            let lsm =
                LsmTree::recover(lsm_recover_options, parts.io_sender.clone())
                    .expect("failed to recover lsm tree");

            Ok(Self {
                id,
                net_options,
                lsm,
                parts,
                lb,
            })
        }

        #[inline]
        pub fn id(&self) -> ShardID {
            self.id
        }

        #[instrument()]
        pub fn run(mut self) -> std::io::Result<()> {
            let socket = tokio::net::TcpSocket::new_v4()?;
            socket.set_reuseport(true)?;

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .name(format!("tokio-logregator-runtime-shard-{}", self.id.0))
                .build_local(tokio::runtime::LocalOptions::default())?;

            rt.block_on(async move {
                match self.do_run(socket).await {
                    Ok(()) => info!("Shard::run exited successfully"),
                    Err(err) => error!("Shard::run exited with error: {err}"),
                }
            });

            Ok(())
        }

        async fn do_run(self, socket: TcpSocket) -> Result<(), NetError> {
            let listener = socket.listen(self.net_options.tcp_backlog_size)?;

            let request_codec = self.net_options.request_codec;
            let response_codec = self.net_options.response_codec;

            loop {
                let (conn, _addr) = listener.accept().await?;

                let (readhalf, writehalf) = tokio::io::split(conn);

                let mut fread = FramedRead::new(readhalf, request_codec);
                let mut fwrite = FramedWrite::new(writehalf, response_codec);

                let request_sender = self.parts.request_sender.clone();
                let request_recv = self.parts.request_recv.clone();

                let response_sender = self.parts.response_sender.clone();
                let response_recv = self.parts.response_recv.clone();

                let conn_handler = tokio::task::spawn_local(async {
                    if let Err(e) = Self::handle_connection(
                        fread,
                        fwrite,
                        request_sender,
                        request_recv,
                        response_sender,
                        response_recv,
                    )
                    .await
                    {
                        error!("connection loop errored: {e}");
                    }
                })
                .await;
            }
        }

        async fn handle_connection(
            mut fread: FramedRead<ReadHalf<TcpStream>, crate::RequestCodec>,
            mut fwrite: FramedWrite<WriteHalf<TcpStream>, crate::ResponseCodec>,

            request_sender: flume::Sender<crate::Request>,
            request_recv: flume::Receiver<crate::Request>,

            response_sender: flume::Sender<crate::Response>,
            response_recv: flume::Receiver<crate::Response>,
        ) -> Result<(), NetError> {
            let resp: crate::Response = todo!();

            tokio::select! {
                req = fread.next() => {
                    let req = match req{
                        Some(Ok(r)) => r,
                        Some(Err(e)) => return Err(e),
                        None => {
                            warn!("framed read closed");
                            return Ok(());
                        },
                    };

                    match request_sender.send_async(req).await {
                        Ok(_) =>  {
                            trace!("request read + sent through channel");
                        },
                        Err(e) => {
                            warn!("request could not be sent through channel, error = {:?}", &e);
                            return Err(e.into())
                        },
                    }
                },

                response = response_recv.recv_async() => {
                    match response {
                        Ok(r) => {
                            fwrite.send(resp).await.inspect_err(|e| {
                                warn!("response could not be written to TcpStream")
                            })?;
                        },
                        Err(flume::RecvError::Disconnected) => {
                            warn!("flume response_recv channel disconnected");
                            return Err(NetError::ResponseRecvDisconnected())
                        },

                };
                }
            };

            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use std::{env::temp_dir, num::NonZeroU64, sync::Arc};

    use tracing::error;

    use crate::{
        NetOptions,
        shard::{LoadBalancer, Shard, ShardID},
    };

    fn tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            // .with_target(true)
            // .with_span_events(FmtSpan::NEW)
            .with_test_writer()
            .try_init();
    }

    type TestShard = Shard<
        logregator_tsdb::record::RecordCodec,
        logregator_tsdb::label::LabelMapCodec,
    >;

    #[tokio::test]
    async fn rt_spawn_shards() {
        let num_shards = NonZeroU64::new(1).unwrap();
        let lb = Arc::new(
            LoadBalancer::new(num_shards).expect("[test_err]: load balancer"),
        );

        let net_options = Arc::new(NetOptions::new_test_options(num_shards));

        let handles: Vec<_> = (0..num_shards.get())
            .map(|shard_id| {
                let basepath = tempfile::Builder::new()
                    .prefix("test_spawn_shard_single")
                    .tempdir()
                    .expect("[test_err]: tempfile");

                #[cfg(test)]
                let lsm_recover_opts =
                    logregator_tsdb::lsm::recover_options_for_test(
                        basepath.path().to_path_buf(),
                    );

                let shard_id = ShardID(shard_id as usize);

                let shard = super::create_shard(
                    shard_id,
                    lb.clone(),
                    net_options.clone(),
                    lsm_recover_opts,
                )
                .expect("[test_err]: Shard");

                std::thread::spawn(move || {
                    shard.run();
                })
            })
            .collect();

        handles.into_iter().enumerate().for_each(|(i, h)| {
            let err = format!("failed to join handle {}", i);
            h.join()
                .unwrap_or_else(|e| error!("failed to join handle-{i}: {e:?}"));
        });
    }
}
