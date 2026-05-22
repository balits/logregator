use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::Context;
use bytes::BufMut;
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tracing::Instrument;

use crate::metrics::Metrics;
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

    pub async fn run_main_loop(
        &self,
        insert_tx: Sender<proto::Command>,
        range_tx: Sender<proto::Command>,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<()> {
        let server_addr = self.listener.local_addr().context("server.main_loop: failed to get local addr")?;
        tracing::info!("listening on {server_addr}");
        loop {
            let (conn, addr) = self
                .listener
                .accept()
                .await
                .context("server.main_loop: failed to accept connection")?;

            let batch_size = insert_tx.max_capacity();
            let insert_tx = insert_tx.clone();
            let range_tx = range_tx.clone();
            let metrics = metrics.clone();
            metrics.server.connections_accepted.inc(1);
            metrics.server.connections_active.add(1);
            let span = tracing::info_span!("conn", %addr);
            tokio::spawn(
                async move {
                    if let Err(e) = Self::handle_conn(conn, addr, insert_tx, range_tx, batch_size, metrics).await {
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
        insert_tx: Sender<proto::Command>,
        range_tx: Sender<proto::Command>,
        _batch_size: usize,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<()> {
        metrics.server.connections_active.add(1);
        tracing::info!("server.handle_conn: handling {addr}");
        let _ = conn.set_nodelay(true);
        let mut framed = Framed::new(conn, proto::ServerCodec);

        while let Some(msg) = framed.next().await {
            metrics.server.frames_read.inc(1);
            let msg = msg
                .context("server.handle_conn: failed to read client message")?;

            match msg {
                proto::ClientMessage::Insert(i) => {
                    let (tx, rx) = oneshot::channel();
                    let cmd = proto::Command::Insert(i, tx);
                    insert_tx
                        .send(cmd)
                        .await
                        .context("server.handle_conn(cmd=INSERT): failed to send insert cmd to engine")?;
                    let res = rx
                        .await
                        .context("server.handle_conn(cmd=INSERT): failed to await cmd result")?;
                    let reply = res
                        .map(|_| ServerMessage::InsertOk)
                        .unwrap_or_else(|e| {
                            ServerMessage::Error(format!("{:#}", e))
                        });
                    metrics.server.frames_written.inc(1);
                    framed
                        .send(reply)
                        .await
                        .context("server.handle_conn(cmd=INSERT): failed to reply to connection")?;
                },
                proto::ClientMessage::BatchInsert(b) => {
                    let (tx, rx) = oneshot::channel();
                    let cmd = proto::Command::BatchInsert(b, tx);
                    insert_tx
                        .send(cmd)
                        .await
                        .context("server.handle_conn(cmd=BATCH_INSERT): failed to send batch insert cmd to engine")?;
                    let res = rx
                        .await
                        .context("server.handle_conn(cmd=BATCH_INSERT): failed to await cmd result")?;
                    let reply = res
                        .map(|_| ServerMessage::InsertOk)
                        .unwrap_or_else(|e| {
                            ServerMessage::Error(format!("{:#}", e))
                        });
                    metrics.server.frames_written.inc(1);
                    framed
                        .send(reply)
                        .await
                        .context("server.handle_conn(cmd=BATCH_INSERT): failed to reply to connection")?;
                },
                proto::ClientMessage::Range(r) => {
                    let (end_send,  end_recv) = oneshot::channel();
                    let (record_send,  mut record_recv) = mpsc::channel(512);
                    let cmd = proto::Command::Range(r, end_send, record_send);

                    range_tx
                        .send(cmd)
                        .await
                        .context("server.handle_conn(cmd=RANGE): failed to send range cmd to engine")?;

                    // Receive record batches from engine, write each record directly into
                    // Framed's write buffer. Flush the TCP socket only when the buffer
                    // exceeds 8KB to coalesce many small frames into fewer TCP segments.
                    const FLUSH_THRESHOLD: usize = 8192;
                    fn write_record_frame(buf: &mut bytes::BytesMut, rec: &proto::Record) {
                        let start = buf.len();
                        buf.put_u32(0);
                        buf.put_u8(0x82);
                        buf.put_i64(rec.source_id);
                        buf.put_i64(rec.ts);
                        buf.put_u64(rec.seq);
                        buf.put_u32(rec.key.len() as u32);
                        buf.put_slice(rec.key.as_bytes());
                        buf.put_u32(rec.value.len() as u32);
                        buf.put_slice(rec.value.as_bytes());
                        let frame_len = buf.len() - start - 4;
                        buf[start..start + 4].copy_from_slice(&(frame_len as u32).to_be_bytes());
                    }
                    let mut record_frames = 0u64;
                    loop {
                        match record_recv.recv().await {
                            Some(records) => {
                                record_frames += records.len() as u64;
                                for rec in &records {
                                    write_record_frame(framed.write_buffer_mut(), rec);
                                }
                                if framed.write_buffer().len() > FLUSH_THRESHOLD {
                                    framed.flush().await
                                        .context("server.handle_conn(cmd=RANGE): failed to flush records")?;
                                }
                            }
                            None => break,
                        }
                    }
                    framed.flush().await
                        .context("server.handle_conn(cmd=RANGE): failed final flush")?;
                    metrics.server.frames_written.inc(record_frames);

                    let reply = match end_recv.await {
                        Ok(Ok(_)) => ServerMessage::RangeEnd,
                        Ok(Err(e)) => ServerMessage::Error(format!("{:#}", e)),
                        Err(_) => ServerMessage::Error("engine closed".into()),
                    };
                    metrics.server.frames_written.inc(1);
                    framed
                        .send(reply)
                        .await
                        .context("server.handle_conn(cmd=RANGE): failed to reply to connection with end/error")?;
                }
            };
        }

        tracing::info!("server.handle_conn: {addr} disconnected");
        metrics.server.connections_active.add(-1);
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
        let batch_size = 64;
        let metrics = std::sync::Arc::new(crate::metrics::Metrics::default());
        let (insert_tx, insert_rx) = mpsc::channel::<proto::Command>(batch_size);
        let (range_tx, range_rx) = mpsc::channel::<proto::Command>(batch_size);
        let (compaction_tx, compaction_rx) = mpsc::channel::<CompactionCommand>(1);
        let (result_tx, result_rx) = mpsc::channel::<CompactionResult>(1);

        let mut engine = Engine::open(
            dir.path().join("data"),
            1024,
            compaction_tx,
            result_rx,
        )
        .unwrap();
        engine.set_metrics(&metrics);
        let m1 = metrics.clone();
        tokio::spawn(async move { engine.engine_loop(insert_rx, range_rx).await });
        tokio::spawn(async move {
            crate::storage::compaction::compaction_loop(
                compaction_rx,
                result_tx,
                Some(metrics),
            )
            .await
        });

        let server = Server::new(Some("127.0.0.1:0".parse().unwrap())).await.unwrap();
        let addr = server.addr();
        tokio::spawn(async move { server.run_main_loop(insert_tx, range_tx, m1).await });

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
    async fn test_insert_flush_then_range() {
        let dir = tempdir().unwrap();
        let batch_size = 64;
        let metrics = std::sync::Arc::new(crate::metrics::Metrics::default());
        let (insert_tx, insert_rx) = mpsc::channel::<proto::Command>(batch_size);
        let (range_tx, range_rx) = mpsc::channel::<proto::Command>(batch_size);
        let (compaction_tx, compaction_rx) = mpsc::channel::<CompactionCommand>(1);
        let (result_tx, result_rx) = mpsc::channel::<CompactionResult>(1);

        let mut engine = Engine::open(
            dir.path().join("data"),
            80,
            compaction_tx,
            result_rx,
        )
        .unwrap();
        engine.set_metrics(&metrics);
        let m1 = metrics.clone();
        tokio::spawn(async move { engine.engine_loop(insert_rx, range_rx).await });
        tokio::spawn(async move {
            crate::storage::compaction::compaction_loop(
                compaction_rx,
                result_tx,
                Some(metrics),
            )
            .await
        });

        let server = Server::new(Some("127.0.0.1:0".parse().unwrap())).await.unwrap();
        let addr = server.addr();
        tokio::spawn(async move { server.run_main_loop(insert_tx, range_tx, m1).await });

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
