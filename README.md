# logregator

A time-series log aggregation engine with a custom LSM-tree storage backend and  TCP frontend(s), written in Rust.

## Overview

`logregator` is a learning project to teach me more on building a database and networking stack from scratch in Rust.
The goal is a simple, readable implementation that can be incrementally optimized layer by layer.
For example an implementation using owned `Vec`s could technically be correct and simple, but it also leads to a bunch of memory churn,
so once `logregator-tsdb` crate stabilizes, the first optimization is introducing an arena allocator, and revisiting all the
`Rc`/`Arc` usages, possibly adding `zerocopy` for the TCP stream parsing.

The system accepts log records over TCP in batches, persists them through a write-ahead log and memtable, and flushes immutable memtables to disk as sorted string tables.
Queries merge results across memtables and SSTables using a streaming merge iterator.
The storage engine also includes a label/stream index of sorts, like Grafana Loki.
This naturally makes queries more performant, given log records only use low-cardinality labels, that queries almost always filter on like "env", "namespace", "service". Otherwise since each unique combination of labels results in a new stream, this cardinality explosion would increase memory usage and lower query performance significantly (see more at the [official Grafana Loki docs]("https://grafana.com/docs/loki/latest/")).


## Architecture

```
logregator
├── logregator-tsdb          Storage engine (work in progress)
├── logregator-core          TCP server/client, wire protocol, metrics, db (old, deprecated)
├── logregatord              CLI binaries: server, client
├── logregator-tools         Benchmarker, comparator, baseline manager
└── xtask                    Cargo task runner
```

## Crates

### logregator-tsdb

The standalone LSM-tree storage engine. This is the current focus of development, and is very much work in progress.
Its designed to be a plug-and-play crate for frontend adapters, so both a glommio and tokio adapter could consume it. 

#### Core components

- **`lsm`**: Top-level `LsmTree` struct. Manages the active memtable, frozen memtables, SST handles, stream registry, and IO event dispatch. Provides `append_batch()` and `range()` as the main API.
- **`memtable`**: `BTreeSet<Record>`-backed memtable with size-aware insertion. `MutMemtable` (read/write) and `FrozenMemtable` (read-only, `Arc`-wrapped) wrappers enforce access semantics at compile time.
- **`record`**: `Record { key: Key, payload: Arc<[u8]> }` with a `Key` containing `source_id`, `timestamp`, `sequence_num`, and `stream_id` (4x `u64`).
- **`codec`**: Generic `SpecCodec<I>` trait for encode/decode, `WireLen` trait for serialized size, `FramedReader`/`FramedWriter` for length-delimited I/O.
- **`block`**: Fixed-size blocks with data + u16 offset index + CRC32 checksum. Default block size: 16KB.
- **`sst`**: SSTable writer and reader. File layout: `[blocks][block_metadata][bloom_filter][footer]`. Footer carries bloom offset, block metadata offset, and CRC32.
- **`bloom`**: FNV-based double-hashing bloom filter for SSTable probabilistic lookups.
- **`wal`**: Write-ahead log using the framed writer over a file. Records are durably written before reaching the memtable.
- **`manifest`**: Tracks SST state (live/removed) and stream registry updates. Supports snapshotting for fast recovery.
- **`io`**: IO event enum (`AppendWal`, `FsyncWal`, `FlushMemtable`) sent over an `mpsc` channel to a dedicated IO thread.
- **`merge_iter`**: Binary min-heap merge iterator across frozen memtables and SST cursors, bounded by key ranges.
- **`label`**: `StreamRegistry` maps label key-value pairs to stream IDs. `LabelMap` is the serialized label set carried in each record payload.

#### Design decisions

- Codec layer is decoupled from allocation strategy, the wire format does not change when the allocator changes (WIP).
- Payloads are currently `Arc<[u8]>` (one heap allocation per record). Arena allocation is planned; see [`logregator-tsdb/README.md`](logregator-tsdb/README.md) for more detail.
- Error type uses thin `Box<ErrorImpl>` pointers (same optimization as `anyhow` / `serde_json`).

### logregator-core (old, deprecated)

Contains the TCP server, async client, wire protocol codec, metrics, and the older storage engine.

#### Wire protocol (`proto.rs`)

Length-delimited framing: `[[4-byte frame_len BE]][[frame_bytes]]`, where each frame is `[[1-byte message_type]][[payload]]`.

| Type  | Direction    | Name          | Payload |
|-------|-------------|---------------|---------|
| 0x01  | Client->Srv  | Insert        | source_id (i64) + ts (i64) + key_len (u32) + key + value_len (u32) + value |
| 0x02  | Client->Srv  | Range         | source_id (i64) + key_len (i32) + key + start_ts (i64) + end_ts (i64) + filter_len (u32) + filter |
| 0x03  | Client->Srv  | BatchInsert   | num_records (u32) then N × Insert |
| 0x04  | Client->Srv  | Metrics       | (no payload) |
| 0x05  | Client->Srv  | Ping          | (no payload) |
| 0x81  | Srv->Client  | InsertOk      | (no payload) |
| 0x82  | Srv->Client  | RangeRecord   | same as Insert layout |
| 0x83  | Srv->Client  | RangeEnd      | (no payload) |
| 0x84  | Srv->Client  | Metrics       | json_len (u32) + JSON snapshot |
| 0x85  | Srv->Client  | Pong          | (no payload) |
| 0xFF  | Srv->Client  | Error         | err_len (u32) + error message |

Uses `tokio_util::codec::{Decoder, Encoder}` with `bytes::BytesMut`.

#### Metrics (`metrics.rs`)

HDR histogram latencies (min/p50/p90/p95/p99/p99.9/max/mean/stddev), counters, and gauges for WAL, engine, and server subsystems. Serializable to JSON via `MetricsSnapshot`.

#### Runtime (`runtime.rs`)

`RuntimeBuilder::spawn()` wires together the TCP listener, an `IoWorker` (dedicated thread for fsync/flush/compaction), and a `Backend` (drains command channels in batches).

### logregatord

CLI binaries for running the server and client.

```sh
# starts on 127.0.0.1:4321
cargo run --bin server
# interactive client, but far from a REPL that i play to add lated
cargo run --bin client          
```

### logregator-tools

Benchmarking and comparison tooling.

- **`bench`**: Load generator against a live server. Workloads: Write, Read, Mixed, Tail, Bulk. Sweeps concurrency levels, samples RSS memory, writes `benchmark_result.json`.
- **`cmp`**: Compares baseline vs target benchmark JSONs, computing per-field deltas with regression detection.
- **`desc`** / **`baseline`**: Manage baseline benchmark runs.

### xtask

Cargo task runner for common workflows.

```sh
cargo xtask bench               # run server + benchmark + regression check
cargo xtask smoke               # build + test
cargo xtask prep                # build + clippy -D warnings + fmt
```

## Getting Started

**Requirements:** Rust nightly (edition 2024).

```sh
# Build
cargo build

# Run server
cargo run --bin server

# Run client
cargo run --bin client

# Run benchmarks
cargo xtask bench
```

Criterion microbenchmarks for the storage engine and codec are in `logregator-core/benches/`,
and in `logregator-tsdb/benches` (WIP).
