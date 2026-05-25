use criterion::{Criterion, criterion_group, criterion_main};
use logregator::storage::Engine;
use tempfile::tempdir;
use tokio::sync::mpsc;

fn bench_engine_insert(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let (cmd_tx, _) = mpsc::channel(1);
    let (_, res_rx) = mpsc::channel(1);

    let mut engine =
        Engine::open(dir.path().to_path_buf(), 64 * 1024 * 1024, cmd_tx, res_rx).unwrap();
    let mut i = 0;

    c.bench_function("engine_insert", |b| {
        b.iter(|| {
            engine
                .insert(
                    std::hint::black_box(67),
                    std::hint::black_box(i),
                    std::hint::black_box("sx_svn"),
                    std::hint::black_box("six: 6, seven: 7"),
                )
                .unwrap();
            i += 1;
        })
    });
}

fn bench_engine_range_memtable(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let (cmd_tx, _) = mpsc::channel(1);
    let (_, res_rx) = mpsc::channel(1);

    let mut engine = Engine::open(
        dir.path().to_path_buf(),
        64 * 1024 * 1024, // big memtable so we dont flush
        cmd_tx,
        res_rx,
    )
    .unwrap();
    let source_id = 64;
    let key = "key";
    let value = "six: 6, seve: 7";
    let max_ts = 2048;
    for i in 0..max_ts {
        engine
            .insert(source_id, i, key, value)
            .expect("failed to setup engine before bench");
    }

    c.bench_function("engine_range_memtable", |b| {
        b.iter(|| {
            engine
                .range(
                    std::hint::black_box(source_id),
                    std::hint::black_box(key),
                    std::hint::black_box(0),
                    std::hint::black_box(max_ts),
                    std::hint::black_box(""),
                )
                .unwrap();
        })
    });
}

fn bench_engine_range_sstable(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let (cmd_tx, _) = mpsc::channel(1);
    let (_, res_rx) = mpsc::channel(1);

    let mut engine = Engine::open(
        dir.path().to_path_buf(),
        256, // frequent flushes -> many sstables -> MergeIter might be slower
        cmd_tx,
        res_rx,
    )
    .unwrap();
    let source_id = 64;
    let key = "key";
    let value = "six: 6, seve: 7";
    let max_ts = 2048;
    for i in 0..max_ts {
        engine
            .insert(source_id, i, key, value)
            .expect("failed to setup engine before bench");
    }

    c.bench_function("engine_range_sstable", |b| {
        b.iter(|| {
            engine
                .range(
                    std::hint::black_box(source_id),
                    std::hint::black_box(key),
                    std::hint::black_box(0),
                    std::hint::black_box(max_ts),
                    std::hint::black_box(""),
                )
                .unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_engine_insert,
    bench_engine_range_memtable,
    bench_engine_range_sstable
);
criterion_main!(benches);
