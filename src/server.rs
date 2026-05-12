use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Context;
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed};
use tracing::Instrument;

use crate::proto::{self, ServerMessage};

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub const ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4321);

    pub async fn new(addr: Option<SocketAddr>) -> anyhow::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr.unwrap_or(Self::ADDR))
                .await
                .context("server.new: failed to bind")?,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    pub async fn run_main_loop(&self, cmd_tx: Sender<proto::Command>) -> anyhow::Result<()> {
        let server_addr = self.listener.local_addr().context("server.main_loop: failed to get local addr")?;
        tracing::info!("listening on {server_addr}");
        loop {
            let (conn, addr) = self
                .listener
                .accept()
                .await
                .context("server.main_loop: failed to accept connection")?;

            let cmd_tx = cmd_tx.clone();
            let span = tracing::info_span!("conn", %addr);

            tokio::spawn(
                async move {
                    if let Err(e) = Self::handle_conn(conn, addr, cmd_tx).await {
                        tracing::error!(error = %e, "connection handler failed");
                    }
                }
                .instrument(span),
            );
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
        let mut framed = Framed::new(conn, proto::ServerCodec);

        while let Some(msg) = framed.next().await {
            let msg = msg
                .context("server.handle_conn: failed to read client message")?;

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
                            Some(rec) = record_recv.recv() => {
                                let reply = ServerMessage::RangeRecord(rec);
                                framed
                                    .send(reply)
                                    .await
                                    .context("server.handle_conn(cmd=RANGE): failed to reply to connection with record")?;
                            },
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

#[cfg(test)]
mod integration_tests {
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    use crate::client::Client;
    use crate::proto;
    use crate::storage::compaction::{CompactionCommand, CompactionResult};
    use crate::storage::Engine;

    use super::Server;

    fn make_insert(source_id: i64, ts: i64, key: &str, value: &str) -> proto::Insert {
        proto::Insert { source_id, ts, key: key.into(), value: value.into() }
    }

    async fn setup() -> (crate::client::Client, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let (network_tx, network_rx) = mpsc::channel(64);
        let (compaction_tx, compaction_rx) = mpsc::channel::<CompactionCommand>(1);
        let (result_tx, result_rx) = mpsc::channel::<CompactionResult>(1);

        let mut engine = Engine::open(
            dir.path().join("data"),
            1024,
            compaction_tx,
            result_rx,
        )
        .unwrap();
        tokio::spawn(async move { engine.engine_loop(network_rx).await });
        tokio::spawn(async move {
            crate::storage::compaction::compaction_loop(compaction_rx, result_tx).await
        });

        let server = Server::new(Some("127.0.0.1:0".parse().unwrap())).await.unwrap();
        let addr = server.addr();
        tokio::spawn(async move { server.run_main_loop(network_tx).await });

        let client = Client::connect(&addr.to_string()).await.unwrap();
        (client, dir)
    }

    #[tokio::test]
    async fn test_insert_then_range() {
        let (mut client, _dir) = setup().await;

        client.insert(make_insert(1, 100, "sys", "cpu normal")).await.unwrap();

        let results = client.range(proto::Range {
            source_id: 1,
            key: "sys".into(),
            start_ts: 0,
            end_ts: 200,
            filter: "".into(),
        }).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].ts, 100);
        assert_eq!(results[0].key, "sys");
        assert_eq!(results[0].value, "cpu normal");
    }

    #[tokio::test]
    async fn test_range_empty() {
        let (mut client, _dir) = setup().await;

        let results = client.range(proto::Range {
            source_id: 1,
            key: "nonexistent".into(),
            start_ts: 0,
            end_ts: 100,
            filter: "".into(),
        }).await.unwrap();

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_range_invalid_ts() {
        let (mut client, _dir) = setup().await;

        let err = client.range(proto::Range {
            source_id: 1,
            key: "sys".into(),
            start_ts: 100,
            end_ts: 0,
            filter: "".into(),
        }).await.unwrap_err();

        assert!(err.to_string().contains("end_ts cannot be smaller than start_ts"));
    }

    #[tokio::test]
    async fn test_batch_insert_then_range() {
        let (mut client, _dir) = setup().await;

        client.batch_insert(vec![
            make_insert(1, 10, "sys", "first"),
            make_insert(1, 20, "sys", "second"),
            make_insert(1, 30, "sys", "third"),
        ]).await.unwrap();

        let results = client.range(proto::Range {
            source_id: 1,
            key: "sys".into(),
            start_ts: 15,
            end_ts: 35,
            filter: "".into(),
        }).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].ts, 20);
        assert_eq!(results[0].value, "second");
        assert_eq!(results[1].ts, 30);
        assert_eq!(results[1].value, "third");
    }

    #[tokio::test]
    async fn test_insert_flush_then_range() {
        let dir = tempdir().unwrap();
        let (network_tx, network_rx) = mpsc::channel(64);
        let (compaction_tx, compaction_rx) = mpsc::channel::<CompactionCommand>(1);
        let (result_tx, result_rx) = mpsc::channel::<CompactionResult>(1);

        let mut engine = Engine::open(
            dir.path().join("data"),
            80,
            compaction_tx,
            result_rx,
        )
        .unwrap();
        tokio::spawn(async move { engine.engine_loop(network_rx).await });
        tokio::spawn(async move {
            crate::storage::compaction::compaction_loop(compaction_rx, result_tx).await
        });

        let server = Server::new(Some("127.0.0.1:0".parse().unwrap())).await.unwrap();
        let addr = server.addr();
        tokio::spawn(async move { server.run_main_loop(network_tx).await });

        let mut client = Client::connect(&addr.to_string()).await.unwrap();

        client.insert(make_insert(1, 10, "sys", "small")).await.unwrap();
        client.insert(make_insert(1, 20, "sys", "massive_payload_to_force_flush")).await.unwrap();

        let results = client.range(proto::Range {
            source_id: 1,
            key: "sys".into(),
            start_ts: 0,
            end_ts: 100,
            filter: "".into(),
        }).await.unwrap();

        assert_eq!(results.len(), 2);
        let mut timestamps: Vec<i64> = results.iter().map(|r| r.ts).collect();
        timestamps.sort();
        assert_eq!(timestamps, vec![10, 20]);
    }

    #[tokio::test]
    async fn test_multiple_inserts_range_subset() {
        let (mut client, _dir) = setup().await;

        for i in 0..10 {
            client.insert(make_insert(1, i * 10, "sys", &format!("val_{}", i))).await.unwrap();
        }

        let results = client.range(proto::Range {
            source_id: 1,
            key: "sys".into(),
            start_ts: 20,
            end_ts: 60,
            filter: "".into(),
        }).await.unwrap();

        assert_eq!(results.len(), 4);
    }
}
