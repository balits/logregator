# logregator

A high-performance TCP log aggregator written in Rust. Accepts timestamped log records over TCP, stores them in a memtable (BTreeSet), writes them to a WAL for durability, and flushes to SSTables on disk. Supports range queries by (source_id, timestamp) prefix.

## Architecture

```
client → TCP → Server → cmd_channel → Backend → Engine → MemTable / SSTables
                                        │              ↕
                                        → IoWorker (async fsync, flush, compaction)
```

- **Server** — Accepts TCP connections, decodes protocol frames, dispatches commands to Backend via an mpsc channel
- **Backend** — Event loop that drains the command channel in batches (512), processes inserts/ranges, sends WAL sync/flush/compaction jobs to IoWorker
- **Engine** — Owns the MemTable, WAL, and SSTable metadata. Routes reads across memtable → frozen memtables → SSTables via MergeIter
- **IoWorker** — Background async worker that handles fsync (timer-based), memtable flush to SST, and SST compaction via `spawn_blocking`
- **WAL** — Write-ahead log for durability. Rotated after each flush. Synced at configurable interval (default: every batch)
- **MemTable** — BTreeSet-based, configurable capacity. When full, frozen and flushed asynchronously
- **SSTable** — On-disk sorted file with index blocks and bloom filters for efficient range scans

## Usage

```sh
# Start server
cargo run --release --bin server -- --addr 127.0.0.1:4321

# Or with profiling + small memtable + timer fsync
just flamegraph -- --addr 127.0.0.1:4321 --memtable-mb 4 --fsync-interval 1000

# Run benchmark
cargo run --release --bin benchmark -- loadgen \
  --external-server true --addr 127.0.0.1:4321 \
  --duration-secs 30 --workloads write,read,mixed,tail
```

### Justfile commands

| Command | Description |
|---------|-------------|
| `just bench` | Run all workloads (write, read, mixed, tail) |
| `just server-bench` | Start server for benchmarking |
| `just server-bench-flame` | Start server with perf profiling → `flamegraph.svg` |

## Performance Goals

Measured on WSL2 (single core).

| Metric | Good | Great | Current (best) |
|--------|------|-------|:--------------:|
| Write 1c throughput | 100K/s | 500K/s | **122.6K/s** |
| Write 16c throughput | 100K/s | 1M/s | **63.2K/s** |
| Insert P50 latency (1c) | <5ms | <1ms | **3.67ms** |
| Insert P99 latency (1c) | <20ms | <5ms | **8.29ms** |
| Range throughput (16c) | 500/s | 5K/s | **8/s** |
| Mixed throughput (peak) | 500 ops/s | 5K ops/s | **~41 ops/s** |
| Tail throughput (peak) | 1K ops/s | 10K ops/s | **~211 ops/s** |

## Status / TODO

- [x] Async flush via IoWorker (unblocks engine loop)
- [x] Timer-based fsync (configurable interval)
- [x] Size-based compaction trigger (8× memtable)
- [x] WAL: fix rotate ordering (rename before open)
- [x] Metrics: expose SST count via protocol-level `Metrics` command
- [x] Benchmark: per-run metrics snapshot for SST count (local & external servers)
- [ ] WAL: proper stale WAL cleanup after flush
- [ ] Write throughput meets "Good" target (122.6K/s 1c, 63.2K/s 16c)
- [ ] Read performance: range queries still slow (8/s at 16c)

## Roadmap

### Phase 1 — Instant-read filters (small code, big read win)

| Step | Change | Why |
|------|--------|-----|
| 1 | Store `min_ts`/`max_ts` per SST during flush | Skip SSTs whose time range doesn't intersect the query |
| 2 | Store `source_ids: BTreeSet<i64>` per SST | Skip SSTs that don't contain the queried source |
| 3 | Return direct iterator when only 1 SST matches | Eliminates BinaryHeap merge overhead for the common case |

Effect: time-bounded range queries check 1–2 SSTs instead of 13. Estimated 5–10× latency improvement for single-client reads.

### Phase 2 — Leveled compaction + key-range metadata

| Step | Change | Why |
|------|--------|-----|
| 4 | Implement leveled compaction (RocksDB-style: tiers of non-overlapping SSTs) | Write amplification drops from O(N) to O(log N); compaction CPU drops further |
| 5 | Track `first_key`/`last_key` per SST (not just time) | Leveled compaction inherently needs this for level boundaries; read path uses it to binary-search the exact file per level |

Leveled compaction and read optimizations are complementary:
- Leveled compaction organizes SSTs into **non-overlapping levels** + overlapping L0
- Per-SST key-range metadata lets the read path **skip irrelevant files in each level**
- With both in place, a range query merges *(L0 files that intersect the range)* + *(1 file per deeper level)* instead of *all files everywhere*

### Phase 3 — Cache & async read

| Step | Change | Why |
|------|--------|-----|
| 6 | Block cache (LRU for index blocks + data blocks) | Repeated queries don't re-read disk; cuts tail latency |
| 7 | Async SST reads via spawn_blocking | Reduces range latency variance |

### Compaction improvements (under Phase 2)

1. **Skip compaction when key space is small** — with few unique keys, merging SSTs is pure write amplification
2. **Don't rebuild bloom filter during compaction** — reuse from input SSTs (25.2% CPU in bloom filter encoding)
3. **Run compaction at lower thread priority** — dedicated thread with reduced niceness so it doesn't starve the insert path
