use bytes::BytesMut;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use logregator_core::proto::{self, ClientMessage, Insert, Record, ServerMessage};
use tokio_util::codec::{Decoder, Encoder};

fn bench_servercodec_encode(c: &mut Criterion) {
    let mut codec = proto::ServerCodec;
    let mut buf = BytesMut::with_capacity(1024);

    c.bench_function("servercodec_encode", |b| {
        b.iter(|| {
            buf.clear();
            let msg = ServerMessage::RangeRecord(Record {
                source_id: 67,
                ts: 1715450067,
                seq: 67,
                key: "sx_svn".into(),
                value: "six_and_seven: 6, 7".into(),
            });
            codec.encode(black_box(msg), &mut buf).unwrap();
        })
    });
}

fn bench_servercodec_decode(c: &mut Criterion) {
    let mut client_c = proto::ClientCodec;
    let mut server_c = proto::ServerCodec;
    let mut buf = BytesMut::with_capacity(1024);

    let msg = ClientMessage::Insert(Insert {
        source_id: 67,
        ts: 171540067,
        key: "sx_svn".into(),
        value: "six_and_seven: 6, 7".into(),
    });
    client_c.encode(msg, &mut buf).unwrap();
    let frame = buf.split().freeze();

    c.bench_function("servercodec_decode", |b| {
        b.iter(|| {
            let mut readbuf = BytesMut::from(frame.as_ref());
            let dec = server_c.decode(&mut readbuf).unwrap();
            black_box(dec);
        });
    });
}

criterion_group!(benches, bench_servercodec_encode, bench_servercodec_decode);
criterion_main!(benches);
