use criterion::{Criterion, criterion_group, criterion_main};
use logregator_core::storage::{Engine, SSTableMeta};
use tempfile::tempdir;

fn bench_engine_insert(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let (mut engine, _) = Engine::open(dir.path().to_path_buf(), 64 * 1024 * 1024).unwrap();
    let mut i = 0;

    c.bench_function("engine_insert", |b| {
        b.iter(|| {
            engine
                .insert_record(
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
    let (mut engine, _) = Engine::open(dir.path().to_path_buf(), 64 * 1024 * 1024).unwrap();
    let source_id = 64;
    let key = "key";
    let value = "six: 6, seve: 7";
    let max_ts = 2048;
    for i in 0..max_ts {
        engine
            .insert_record(source_id, i, key, value)
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
    let (mut engine, _) = Engine::open(dir.path().to_path_buf(), 0).unwrap();
    let source_id = 64;
    let key = "key";
    let value = "six: 6, seve: 7";
    let max_ts = 2048;
    for i in 0..max_ts {
        engine
            .insert_record(source_id, i, key, value)
            .expect("failed to setup engine before bench");
        if i > 0 && i % 32 == 0 {
            let (frozen, sst_id) = engine.prepare_flush();
            let meta = SSTableMeta::write_to_file(
                engine.clone_base_dir(),
                sst_id,
                frozen.iter(),
                frozen.len(),
            )
            .unwrap();
            engine.remove_frozen_memtable(sst_id);
            engine.insert_meta(meta);
        }
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
