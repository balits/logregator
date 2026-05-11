use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Context;
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed};

use crate::proto::{self, ServerMessage};

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub const ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4321);

    pub async fn new(addr: Option<SocketAddr>) -> std::io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr.unwrap_or(Self::ADDR)).await?,
        })
    }

    pub async fn run_main_loop(&self, cmd_tx: Sender<proto::Command>) -> anyhow::Result<()> {
        tracing::info!("listening on {}", Self::ADDR);
        loop {
            let (conn, addr) = self
                .listener
                .accept()
                .await
                .context("server.main_loop: failed to accept connection")?;

            let cmd_tx = cmd_tx.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::handle_conn(conn, addr, cmd_tx).await {
                    tracing::error!(error = %e, "server.main_loop: client connection failed: {addr}");
                }
            });
        }
    }

    async fn handle_conn(
        conn: TcpStream,
        addr: SocketAddr,
        engine_tx: Sender<proto::Command>,
    ) -> anyhow::Result<()> {
        tracing::info!("server.handle_conn: handling {addr}");
        // let (rhalf, w) = tokio::io::split(conn);
        // let mut reader = FramedRead::new(rhalf, proto::Codec);
        // let mut writer = FramedWrite::new(w, proto::Codec);
        let mut framed = Framed::new(conn, proto::Codec);

        while let Some(msg) = framed.next().await {
            let msg = msg?;

            // TODO: create response channel, await result,
            // encode as ServerMessage, write via _w

            match msg {
                proto::ClientMessage::Insert(i) => {
                    tracing::debug!("INSERT request");
                    let (tx, rx) = oneshot::channel();
                    let cmd = proto::Command::Insert(i, tx);
                    engine_tx
                        .send(cmd)
                        .await
                        .context("server.handle_conn(cmd=INSERT): failed to send insert cmd to engine")?;
                    let res = rx
                        .await
                        .context("server.handle_conn(cmd=INSERT): failed to await cmd result")?;
                    let reply = res
                        .map(|_| ServerMessage::InsertOk)
                        .unwrap_or_else(|e| {
                            tracing::info!("server_handle_conn(cmd=INSERT): command failed, replying with error {:#}", e);
                            ServerMessage::Error(format!("{:#}", e))
                        });
                    framed
                        .send(reply)
                        .await
                        .context("server.handle_conn(cmd=INSERT): failed to reply to connection")?;
                },
                proto::ClientMessage::BatchInsert(b) => {
                    tracing::debug!("BATCH_INSERT request");
                    let (tx, rx) = oneshot::channel();
                    let cmd = proto::Command::BatchInsert(b, tx);
                    engine_tx
                        .send(cmd)
                        .await
                        .context("server.handle_conn(cmd=BATCH_INSERT): failed to send batch cmd to engine")?;
                    let res = rx
                        .await
                        .context("server.handle_conn(cmd=BATCH_INSERT): failed to await cmd result")?;
                    let reply = res
                        .map(|_| ServerMessage::InsertOk)
                        .unwrap_or_else(|e| {
                            tracing::info!("server_handle_conn(cmd=BATCH_INSERT): command failed, replying with error {:#}", e);
                            ServerMessage::Error(format!("{:#}", e))
                        });
                    framed
                        .send(reply)
                        .await
                        .context("server.handle_conn(cmd=BATCH_INSERT): failed to reply to connection")?;
                },
                proto::ClientMessage::Range(r) => {
                    tracing::debug!("RANGE request");
                    let (end_send, mut end_recv) = oneshot::channel();
                    let (record_send, mut record_recv) = mpsc::channel(512);
                    let cmd = proto::Command::Range(r, end_send, record_send);
                    engine_tx
                        .send(cmd)
                        .await
                        .context("server.handle_conn(cmd=RANGE): failed to send range cmd to engine")?;
                    
                    loop {
                        tokio::select! {
                            biased;

                            end = &mut end_recv => {
                                let reply = match end {
                                    Ok(Ok(_)) => ServerMessage::RangeEnd,
                                    Ok(Err(e)) => ServerMessage::Error(format!("{:#}", e)),
                                    Err(_) => ServerMessage::Error("engine closed".into()),
                                };
                                framed
                                    .send(reply)
                                    .await
                                    .context("server.handle_conn(cmd=RANGE): failed to reply to connection with end/error")?;
                                break;
                            },

                            Some(rec) = record_recv.recv() => {
                                let reply = ServerMessage::RangeRecord(rec);
                                framed
                                    .send(reply)
                                    .await
                                    .context("server.handle_conn(cmd=RANGE): failed to reply to connection with record")?;
                            }
                        };
                    }
                }
            };
        }

        tracing::info!("server.handle_conn: {addr} disconnected");
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        tracing::info!("server.main_loop: closing server");
    }
}
