use anyhow::{Context, format_err};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::proto::{self, BatchInsert, ClientMessage, ServerMessage};

pub struct Client {
    framed: Framed<TcpStream, proto::ClientCodec>,
}

impl Client {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let conn = TcpStream::connect(addr).await.with_context(|| {
            format!("client.connect: failed to establish connection to addr {addr}")
        })?;
        let _ = conn.set_nodelay(true);
        Ok(Self {
            framed: Framed::new(conn, proto::ClientCodec),
        })
    }

    pub async fn insert(&mut self, insert: proto::Insert) -> anyhow::Result<()> {
        let msg = ClientMessage::Insert(insert);
        self.framed
            .send(msg)
            .await
            .context("client.insert: failed to send client message")?;
        match self.framed.next().await {
            Some(Ok(ServerMessage::InsertOk)) => return Ok(()),
            Some(Ok(ServerMessage::Error(e))) => return Err(format_err!(e)),
            Some(Ok(resp)) => return Err(format_err!("client.insert: unexpected response message: {:?}", resp)),
            Some(Err(e)) => return Err(format_err!(e)),
            None => return Err(format_err!("client.insert: connection closed")),
        }
    }

    pub async fn batch_insert(&mut self, records: Vec<proto::Insert>) -> anyhow::Result<()> {
        let msg = ClientMessage::BatchInsert(BatchInsert { records });
        self.framed
            .send(msg)
            .await
            .context("client.batch_insert: failed to send client message")?;
        match self.framed.next().await {
            Some(Ok(ServerMessage::InsertOk)) => return Ok(()),
            Some(Ok(ServerMessage::Error(e))) => return Err(format_err!(e)),
            Some(Ok(resp)) => return Err(format_err!("client.batch_insert: unexpected response message: {:?}", resp)),
            Some(Err(e)) => return Err(format_err!(e)),
            None => return Err(format_err!("client.batch_insert: connection closed")),
        }
    }

    pub async fn range(&mut self, range: proto::Range) -> anyhow::Result<Vec<proto::Record>> {
        let mut v = Vec::with_capacity(64);
        let msg = ClientMessage::Range(range);
        self.framed
            .send(msg)
            .await
            .context("client.range: failed to send client message")?;
        loop {
            match self.framed.next().await {
                Some(Ok(ServerMessage::RangeRecord(r))) => {
                    v.push(r);
                }
                Some(Ok(ServerMessage::RangeEnd)) => {
                    return Ok(v);
                }
                Some(Ok(ServerMessage::Error(e))) => return Err(format_err!(e)),
                Some(Ok(resp)) => return Err(format_err!("client.batch_insert: response message: {:?}", resp)),
                Some(Err(e)) => return Err(format_err!(e)),
                None => return Err(format_err!("client.batch_insert: connection closed")),
            }
        }
    }
}
