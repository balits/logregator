#![feature(iter_map_windows)]
#![feature(mpmc_channel)]
#![allow(unused)]
// use std::sync::mpmc;

use std::{marker::PhantomData, num::NonZeroUsize, sync::mpmc};

use futures::sink::SinkExt;
use tokio::{
    io::{ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
};
use tokio_stream::StreamExt;
use tokio_util::{
    codec::{Framed, FramedRead, FramedWrite},
    io::CopyToBytes,
};

use bytes::{Bytes, BytesMut};
use tracing::{error, instrument, trace, warn};

#[instrument(skip_all, err)]
async fn task_per_conn(
    addr: std::net::SocketAddr,
    req_sender: mpmc::Sender<Request>,
    resp_recv: mpmc::Receiver<Response>,
) -> Result<(), NetError> {
    let socket = TcpListener::bind(addr).await?;

    loop {
        let (conn, _addr) = socket.accept().await?;
        let (readhalf, writehalf) = tokio::io::split(conn);

        let request_codec = RequestCodec {};
        let response_codec = ResponseCodec {};

        let mut fread = FramedRead::new(readhalf, request_codec);
        let mut fwrite = FramedWrite::new(writehalf, response_codec);

        let req_sender = req_sender.clone();
        let resp_recv = resp_recv.clone();

        tokio::spawn(async move {
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

struct Request {
    kind: RequestKind,
    payload: BytesOffset,
}

struct BytesOffset {
    raw: bytes::Bytes,
    offsets: Vec<NonZeroUsize>,
}

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
        let x = nom::Error;

        if !(logregator_tsdb::label::MIN_STR_LEN
            ..logregator_tsdb::label::MIN_STR_LEN)
            .contains(len)
        {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::MapRes,
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
                todo!("impl decode for Response::AppendBatch")
            }
            1 => {
                todo!("impl decode for Response::Range")
            }
            _ => Err(todo!("create return type for unknown tag in Response")),
        }
    }
}
enum Response {}

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
enum NetError {
    #[error("net error: std::io:error: {0}")]
    Io(#[from] std::io::Error),

    #[error("net error: failed to send request through std::mpmc channel: {0}")]
    RequestSend(#[from] mpmc::SendError<Request>),

    #[error("net error: response channels reciever side disconnected")]
    ResponseRecvDisconnected(),
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
