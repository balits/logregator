# logregator

High-performance log aggregator in Rust. Ingests log records over TCP, stores them in an LSM-tree backed engine, and supports time-range queries.

## Project Goal

Build a high-performance log aggregator with:
- A tokio-based TCP server accepting concurrent connections
- A custom binary wire protocol (length-delimited frames)
- An LSM-tree storage engine with WAL, memtable, SSTables, bloom filters, index blocks
- A workload benchmark binary (`loadgen`) for measuring throughput/latency
- **Target**: >10k records/sec throughput, <20ms range P50 latency

---

## Architecture Overview

```
Client (TcpStream)  <->  Server (tokio)  <-[mpsc]->  Storage Engine  <->  SSTable files
                                                        |
                                                  Compactor (bg task)
```

Three layers communicate via message passing (no locks, no shared state):

### 1. Wire Protocol (`src/proto.rs`)

Length-delimited framing: `[4-byte frame_len BE][1-byte type][payload]`

| Type | Direction | Name | Payload |
|------|-----------|------|---------|
| `0x01` | client->server | Insert | source_id, ts, key, value |
| `0x02` | client->server | Range | source_id, key, start_ts, end_ts, filter |
| `0x03` | client->server | BatchInsert | num_records + per-record: source_id, ts, key, value |
| `0x81` | server->client | InsertOk | (empty) |
| `0x82` | server->client | RangeRecord | source_id, ts, seq, key, value |
| `0x83` | server->client | RangeEnd | (empty) |
| `0xFF` | server->client | Error | error string |

Codecs:
- `ServerCodec`: decodes `ClientMessage`, encodes `ServerMessage`
- `ClientCodec`: decodes `ServerMessage`, encodes `ClientMessage`

### 2. Server (`src/server.rs`)

- `Server::run_main_loop(cmd_tx)` — accepts TCP connections in a loop
- Each connection gets a spawned handler: `handle_conn(conn, engine_tx)`
- Handler reads `ClientMessage` via `Framed<ServerCodec>`, converts to `Command`, sends to engine via `mpsc::Sender<Command>`
- `Command` enum carries response channels:
  - `Insert(Insert, oneshot::Sender)` — oneshot for ACK
  - `BatchInsert(BatchInsert, oneshot::Sender)` — oneshot for ACK
  - `Range(Range, oneshot::Sender, mpsc::Sender<Record>)` — oneshot for end, mpsc for streaming records
- Range handler uses `ReceiverStream` + `send_all` to stream records to client

### 3. Engine (`src/storage/engine.rs`)

Actor model — single task owns `&mut self`, processes commands from `mpsc::Receiver<Command>`:

```
engine_loop():
  loop {
    select! {
      recv_many(cmds) → {
        for cmd in cmds {
          match cmd {
            Insert → insert into memtable, WAL append
            BatchInsert → batch insert into memtable, WAL batch_append
            Range → handle_range (spawn_blocking merge iterator)
          }
        }
        if needs_flush → flush().await ...spawn_blocking SSTable write...
      }
      CompactionResult → apply compaction, re-trigger if threshold met
    }
  }
```

Key design decisions:
- **No locks inside engine**: the `&mut self` guarantees exclusive access
- **`flush()` is async** via `spawn_blocking` for SSTable I/O — prevents blocking the tokio worker
- **WAL cleared synchronously** before spawn_blocking to prevent racing with new inserts
- **Compaction threshold ≥ 4**: re-trigger logic in both `flush()` and compaction result handler
- **`handle_range` uses `spawn_blocking`** to iterate the merge iterator off the main task

### 4. Storage Layer (`src/storage/`)

#### MemTable (`memtable.rs`)
- `BTreeSet<Record>` with byte-size tracking (`current_sz > limit` triggers flush)
- `range_cloned(source_id, start_ts, end_ts)` — clones only matching records (efficient snapshot)
- Records sorted by `(source_id, timestamp, seq_num, key)` via `Record::cmp`

#### WAL — Write-Ahead Log (`wal.rs`)
- Single file (`wal.log`), length-delimited records (`[8-byte len LE][data]`)
- `append()` — write + flush + fsync (for individual inserts)
- `batch_append()` — write all records, then flush + fsync once
- `clear()` — truncate to 0, rewind, replace BufWriter
- Engine opens WAL on startup, recovers any unflushed records into memtable

#### SSTable (`sstable.rs`)
- Flat directory (no daily subdirs), files named `{id:010}.sst`
- Layout: `[records...][index_block][bloom_filter][8-byte index_offset][8-byte bloom_offset]`
- Each record: `[8-byte len LE][data]`
- `IndexBlock`: list of `(first_record_key, offset)` entries for binary seek
- `BloomFilter`: bloom filter keyed by `(source_id, key)` for fast negative lookups
- `SSTableMeta`: cached metadata (path, bloom, index, offset, file_size)

#### Record (`record.rs`)
- Internal binary format: `[source_id: i8 LE][ts: i64 LE][seq_num: u64 LE][key_len: u32 LE][key][value_len: u32 LE][value]`
- Implements `Ord` by `(source_id, timestamp, seq_num, key)`
- `extract_all_fields_ref()` — zero-copy field access
- `extract_all_fields_owned()` — owned String/Vec extraction

### 5. Iterators (`src/storage/iter.rs`)

- `MergeIter` — N-way merge of memtable + SSTable iterators, deduplicates by seq_num
- `RecordIter` enum: `Mem(Vec<Record>)`, `File(SSTableIter)`, `Scan(SSTableScanner)`
- `SSTableIter` — uses index block for binary seek, scans linearly
- `MemTableIterOwned` — filters cloned records by source_id, key, time range, substring filter

### 6. Compaction (`src/storage/compaction.rs`)

- Background task: receives `CompactionCommand`, merges all SSTables into one, sends `CompactionResult`
- Compactor channel capacity = 1 (prevents race where queued commands reference already-removed files)
- All SSTables merged into one (high write amplification but bounds file count)
- Uses `RecordIter::Scan` for compaction merge

---

## Communication Flow

### Insert
```
Client → framed.send(ClientMessage::Insert)
  → Server decode → Command::Insert(oneshot)
    → engine_tx.send(cmd)
      → Engine::insert() → WAL append, memtable insert
    → oneshot response
  → Server encode → ServerMessage::InsertOk
→ Client receives
```

### BatchInsert
```
Client → framed.send(ClientMessage::BatchInsert)
  → Server decode → Command::BatchInsert(oneshot)
    → engine_tx.send(cmd)
      → Engine::batch_insert() → WAL batch_append, memtable inserts
      → if needs_flush → flush().await
    → oneshot response
  → Server encode → ServerMessage::InsertOk
→ Client receives
```

### Range
```
Client → framed.send(ClientMessage::Range)
  → Server decode → Command::Range(oneshot, mpsc::Sender)
    → engine_tx.send(cmd)
      → Engine::handle_range() → spawn_blocking(move || {
          range() → MergeIter
          for record in iter {
            record_sender.blocking_send(record)
          }
          end_sender.send(Ok(()))
        })
    → record_stream = ReceiverStream::new(record_recv)
    → framed.send_all(record_stream)  // streams records
    → framed.send(RangeEnd)
  → Client receives RangeRecord × N, then RangeEnd
```

---

## Benchmarking (`src/bin/loadgen.rs`)

### Workloads
| Kind | Behavior |
|------|----------|
| `Write` | 100% inserts, batched at `batch_size` (default 512) |
| `Read` | 100% range queries |
| `Mixed` | range_fraction (0.5) probability of range vs insert |
| `Tail` | 10% range, 90% insert |
| `Bulk` | 50% range, 50% insert (default) |

### Batching
- Inserts (non-range iterations) are accumulated in `pending: Vec<Insert>`
- When `pending.len() >= batch_size`, sent as one `batch_insert()` call
- Remaining pending flushed at end of run
- Each `batch_insert` has a 30-second timeout to prevent hanging when engine is overwhelmed
- Channel capacity (`--channel-capacity`, default 8192) separates insert batch size from engine mpsc backpressure

### Measures
- Throughput: records/sec and range queries/sec
- Latency: min, p50, p90, p95, p99, p99.9, max, mean, stddev (microseconds)
- Error count, SSTable count
- Saturation analysis (throughput vs concurrency)

### Flags
| Flag | Default | Description |
|------|---------|-------------|
| `--duration-secs` | 10 | Benchmark duration per run |
| `--workloads` | bulk | Can specify multiple: `--workloads bulk --workloads write` |
| `--batch-size` | 512 | Records per batch insert |
| `--channel-capacity` | 8192 | Engine mpsc channel capacity |
| `--concurrency-min/max/step` | 1/16/2 | Client count sweep |
| `--sources` | 4 | Distinct source IDs |
| `--keys` | 4 | Distinct log keys |
| `--value-size` | 128 | Payload byte size per record |
| `--out` | — | Save results to `prof/{name}_{timestamp}.md` |
| `--profile-mem` | 0 (off) | Memory profiling interval in ms. Samples RSS from `/proc/self/status` + engine state (memtable/SSTables). Adds table at end |

---

## Performance Targets & Current Status

| Target | Current (Final) | Status |
|--------|-----------------|--------|
| >10k records/sec throughput | 81,408 rec/s (write-only) | ✅ Met |
| <20ms range P50 latency | **194µs** (1 client), **3.4ms** (6 clients) | ✅ **Met** |
| Zero errors at peak throughput | 0 errors (bulk), ~512 errors (write final flush) | ⚠️ Minor end-of-run |

### Final Results (with TCP_NODELAY + all optimizations)

**Bulk (50% range, 10s):**
| Clients | INS/S | RNG/S | Range P50 | Range P99 | Insert P99 |
|---------|-------|-------|-----------|-----------|------------|
| 1       | 3,037 | 3,526 | **194µs** | 1,559µs   | 22µs       |
| 2       | 1,210 | 1,217 | **830µs** | 7,534µs   | 20µs       |
| 6       | 814   | 802   | **3.4ms** | 28ms      | 34µs       |

**Write-only (1 client, 5s):** 81,408 rec/s, Insert P99=20µs

### Optimization Progression

| Step | Change | Range P50 (6 cl) | Bulk INS/S (6 cl) |
|------|--------|------------------|-------------------|
| Phase 4 (baseline) | Per-record sends, single channel | ~44ms | ~197 |
| Fix 1+2 | Double-clone fix + batch insert | ~44ms | 245 |
| Step 3+4 | Remove spawn_blocking + dual channels | ~44ms | 266 |
| Step 5 | Buffered TCP writes (8KB threshold) | ~44ms | 252 |
| Step 5b | Engine-side batching (Vec<Record>, size=32) | **1.15ms** | **416** |
| Step 6 | `TCP_NODELAY` on client + server sockets | **194µs** (1 cl) | **3,037** (1 cl) |

### Key Decisions Log

- **Async `flush()` via `spawn_blocking`** — fixes 12-client crash from synchronous `flush_inner()` blocking tokio worker. Engine loop yields during SSTable I/O. WAL cleared synchronously before spawn to avoid racing with new inserts.
- **Compaction threshold ≥ 4** — normative LSM behavior. With 64MB memtable (~4s fill at peak), compactions happen every ~4 flushes.
- **64MB memtable** — matches production LSM sizes. Prevents frequent flushes during short benchmarks.
- **Compactor channel capacity = 1** — prevents race where multiple queued commands reference files already removed by an earlier compaction.
- **Compaction merges ALL SSTables** — bounds file count at the cost of write amplification.
- **SSTables read into memory before merging** — trades upfront I/O (read all matching records from file) for zero file I/O during merge phase.
- **Batch insert (0x03) with 512-record batches** — dramatically reduces engine command count (512 records → 1 command). Enables 81k rec/s write throughput.
- **`flush()` returns `bool`** — `insert()` and `batch_insert()` return `needs_flush` flag. Engine loop uses async `flush()` after all commands in `recv_many` batch are processed.

---
## Completed Optimizations

### Step 3: Removed `spawn_blocking` in range queries ✅

Replaced `handle_range` with inline `process_range` on the engine loop. The merge iterator iterates in-memory data (zero I/O), records are sent async through `record_sender.send(rec).await`. Eliminated spawn_blocking scheduling overhead.

**Impact:** ~0% alone. Was blocked by Nagle's algorithm.

### Step 4: Dual-channel priority ✅

Split engine into `insert_rx` (high priority) and `range_rx` (low priority) channels. Server routes Insert/BatchInsert to `insert_tx`, Range to `range_tx`. Engine drains all inserts first (via `recv_many`), then processes one range (via `try_recv`), then flushes. Prevents ranges from starving inserts.

**Impact:** Combined with Step 5's engine batching, range P50 went from 44ms → 1.15ms at 6 clients.

### Step 5a: Buffered TCP writes ✅

Replaced `Framed::send_all` with direct writes to `Framed::write_buffer_mut()`. Flushes TCP socket only when buffer exceeds 8KB. Coalesces many small frames into fewer TCP segments.

**Impact:** ~0% alone. Bottleneck wasn't flush frequency.

### Step 5b: Engine-side record batching ✅

Changed internal range channel from `mpsc::Sender<proto::Record>` to `mpsc::Sender<Vec<proto::Record>>`. Engine accumulates records into batches of 32 chunks before sending through the channel. Server receives chunks and writes each record to the Framed buffer.

**Impact:** Reduced per-record await overhead from 800 awaits → ~26 awaits per 400-record range at 6 clients. Range P50: 44ms → 1.15ms.

### Step 6: TCP_NODELAY ✅

Set `TCP_NODELAY` on both client (`client.rs:17`) and server (`server.rs:70`) TCP sockets. Disables Nagle's algorithm which was adding 40ms delayed ACK timer per small frame.

**Impact:** The single biggest fix. Range P50: 44ms → 194µs (227x improvement). Bulk INS/S at 1 client: 28 → 3,037 (108x improvement).

### Final Architecture Changes

- `proto::Command::Range` now carries `mpsc::Sender<Vec<proto::Record>>` instead of `mpsc::Sender<proto::Record>`
- `Engine::engine_loop()` takes two receivers: `insert_rx` and `range_rx`
- `Server::run_main_loop()` takes two senders: `insert_tx` and `range_tx`
- Client and server connections use `TcpStream::set_nodelay(true)`
- Range records are batched in the engine (32 records per batch) and written directly to the Framed buffer on the server (flushed every 8KB)

### Relevant Files

| File | Purpose |
|------|---------|
| `src/proto.rs` | Wire protocol: types, codecs, commands |
| `src/client.rs` | TCP client with insert/batch_insert/range |
| `src/server.rs` | TCP server, connection handler |
| `src/storage/engine.rs` | Engine actor, insert/flush/range/engine_loop |
| `src/storage/memtable.rs` | BTreeSet-based memtable |
| `src/storage/wal.rs` | Write-ahead log |
| `src/storage/sstable.rs` | SSTable read/write, index blocks, bloom filters |
| `src/storage/iter.rs` | MergeIter, SSTableIter, MemTableIterOwned |
| `src/storage/record.rs` | Record binary format |
| `src/storage/compaction.rs` | Background compaction task |
| `src/bin/loadgen.rs` | Workload benchmark binary |
| `src/bin/server.rs` | Standalone server binary |
| `src/mem_profile.rs` | Memory profiler module (RSS sampling, engine state, report generation) |
| `prof/` | Benchmark results and memory profiles |
| `prof/mem/` | Memory profile output directory |
## Architecture

```
TCP client → length-prefixed binary protocol
                 ↓
            Network handler (tokio)
                 ↓
          Bounded mpsc channel
                 ↓
         Engine run loop (actor)
         ↙              ↘
   MemTable            SSTable pool
      ↓                    ↓
   flush()            compaction
      ↓                    ↓
   SSTable            merged SSTable
```

### Data flow

- **Client** sends `Insert`, `BatchInsert`, or `Range` frames over TCP
- **Server** (`src/server.rs`) accepts connections, decodes frames, dispatches to engine via `mpsc::Sender<Command>`
- **Engine** (`src/storage/engine.rs`) is a single async task that owns `&mut self` — no locks. Processes inserts into `MemTable` + `Wal`, and range queries via a merge iterator over memtable + SSTables.
- **Compactor** (`src/storage/compaction.rs`) runs as a background task, triggered when ≥4 SSTables exist, merges all into one.

### Performance (write-only, single client)

- **Before WAL batching**: 81,408 inserts/s
- **After WAL batching** (fsync once per recv_many cycle, not per batch): **134,400 inserts/s**
- Multi-client perf is bottlenecked by flush blocking the actor loop (10,445 INS/S at 6 clients — see Next Steps)

## Next steps

These are ordered by impact, highest first.

### 1. Dual memtable (active/frozen) — unlocks multi-client throughput

The engine actor blocks on `flush().await` — during that time zero commands are processed. All 6 clients wait. After the flush (50–100ms), the engine catches up in one batch.

**Fix:** split `MemTable` into active + frozen:

```
engine_loop:
  recv_many(cmds) → insert into active memtable + WAL
  if active > limit:
    freeze active, swap in fresh active
    spawn_blocking(flush frozen memtable to SSTable)
  // no await on flush — engine keeps processing
```

No WAL change needed — records are already buffered. The frozen memtable's records are in the WAL file; flush drains them from the WAL, not from memory.

**Impact:** estimated 10k → **~80k+ INS/S** at 6 clients (6× improvement).

### 2. Leveled compaction — prevents range latency degradation

Currently all SSTables are merged into one (high write amplification, unbounded read cost). Each range query scans every SSTable that passes the bloom filter. As data grows, range latency grows linearly.

**Fix:** leveled compaction (e.g., RocksDB-style size-tiered or leveled). Keeps per-level file counts small, bounds the merge iterator's work per query.

### 3. Server-side metrics — observability for debugging and profiling

See [Metrics](#metrics) section below.

### 4. Graceful shutdown — reliability

On SIGTERM/SIGINT: stop accepting connections, drain in-flight commands, flush memtable, close WAL, signal compactor to finish.

### 5. CLI tool — developer convenience

```bash
logregator insert <source_id> <ts> <key> <value>
logregator batch-insert <json_file>
logregator range <source_id> <key> <start_ts> <end_ts> [--filter <substring>]
```

### 6. DbManager / per-day engine routing — production data organization

Route inserts to per-day LSM directories. Enables retention by deleting day directories. Built by wrapping the engine map behind a shared `mpsc::Receiver`.

## Metrics

### Approach

Use standard Rust metrics dependencies:
- **`metrics`** crate (`metrics = "0.24"`) for the counters/gauges facade — integrates with `tracing` via `metrics-tracing` for automatic span-level metrics.
- **`hdrhistogram`** crate (`hdrhistogram = "7"`) for latency histograms — lock-free concurrent recording, p50/p90/p99/p99.9 out of the box.
- **`metrics-util`** (`metrics-util = "0.20"`) for quantile snapshots and registry utilities.

Metrics are printed periodically to stdout via a background task and/or exposed over a TCP stats endpoint. No Prometheus exporter — keep it lightweight.

### Metric types

| Type | Rust primitive | Semantic |
|------|---------------|----------|
| `Counter` | `AtomicU64` | Monotonically increasing count (ops, bytes, errors) |
| `Gauge` | `AtomicI64` | Point-in-time value (active connections, memtable bytes) |
| `Histogram` | `HdrHistogram` or bucketed `AtomicU64[]` | Latency distribution (insert, range, flush, compaction) — logged as p50/p90/p99/p99.9 |

### Registry design

```
MetricRegistry (Arc)                HistogramStore (Arc<Mutex<HashMap>>)
  ├── server: ServerMetrics           ├── insert_latency_us
  │   ├── connections_accepted C      ├── batch_insert_latency_us
  │   ├── connections_active G        ├── range_latency_us
  │   ├── bytes_read C                ├── flush_duration_us
  │   ├── bytes_written C             ├── compaction_duration_us
  │   ├── frames_decoded C            ├── wal_write_us
  │   └── frames_encoded C            └── wal_sync_us
  ├── engine: EngineMetrics
  │   ├── inserts_received C
  │   ├── insert_failures C
  │   ├── batch_inserts_received C
  │   ├── records_inserted C
  │   ├── ranges_received C
  │   ├── records_scanned C
  │   ├── memtable_bytes G
  │   ├── memtable_limit G
  │   ├── sstable_count G
  │   ├── flush_count C
  │   └── compaction_count C
  └── wal: WalMetrics
      ├── write_count C
      ├── write_bytes C
      └── sync_count C
```

### Where to instrument

| Component | File | What to measure |
|-----------|------|-----------------|
| `Server::handle_conn` | `server.rs` | Connections (accept + close), bytes/frames per direction, per-command latency |
| `Engine::insert` | `engine.rs` | Insert count + latency, WAL write latency |
| `Engine::batch_insert` | `engine.rs` | Batch count, records per batch, latency |
| `Engine::range` | `engine.rs` | Range count, records scanned, latency, bloom miss/hit ratio |
| `Engine::flush_inner` | `engine.rs` | Flush count, duration, bytes written |
| `Engine::engine_loop` | `engine.rs` | Channel depth snapshot, memtable/sstable gauges |
| `Wal::write_one` | `wal.rs` | Write count + bytes + latency |
| `Wal::write_many` | `wal.rs` | Write count + bytes + latency |
| `Wal::sync` | `wal.rs` | Sync count + latency |
| `compaction_loop` | `compaction.rs` | Compaction count, input bytes, output bytes, duration |

### Output strategies

1. **Periodic log** — a background task prints a metrics snapshot every N seconds to stdout (for profiling sessions)
2. **Stats command** — engine processes a `Stats` command variant, returns a JSON/metrics text blob over TCP (for live monitoring)
3. **Histogram flush** — histograms are reset after each snapshot to track recent behavior, not cumulative

### Implementation order

1. Add `metrics`, `hdrhistogram`, `metrics-util` deps to Cargo.toml
2. Create `src/metrics.rs` — wraps `metrics` counters/gauges + `hdrhistogram` histograms behind a `Metrics` struct (`Arc`-safe)
3. Add `metrics: Arc<Metrics>` field to `Engine`, pass through `Server` to connection handlers
4. Instrument engine hot paths: `insert()`, `batch_insert()`, `process_range()`, `flush()`, `engine_loop()` gauges
5. Instrument server hot paths: connection accept/close, bytes/frames per direction
6. Instrument WAL: `write_one`, `write_many`, `sync` — count + `hdrhistogram` latencies
7. Background snapshot task — prints `Metrics::dump()` every 5 seconds
8. (Optional) `proto::Command::Stats(oneshot::Sender)` for live TCP querying

---

## TODO

### OK — minimum to call it working

| Sev | Issue | Why | Fix |
|-----|-------|-----|-----|
| **Critical** | **Dual-memtable** | Single-threaded engine blocks ALL command processing during `flush().await`. At 3+ clients the mpsc fills up, TCP handlers stall, and all inserts time out (0 INS/S). 1-client throughput is 50k INS/S but the architecture can't scale. | When memtable exceeds limit: (1) freeze it into an immutable memtable, (2) swap in a fresh active memtable, (3) flush frozen memtable via `spawn_blocking` in background. WAL: don't clear until frozen memtable is flushed. Engine loop keeps processing inserts against the active memtable during flush I/O. |
| **High** | **Graceful shutdown** | `Ctrl+C` kills tasks mid-write. Engine drops in-flight commands, compactor stops, WAL may hold unflushed data. Tests leak temp dirs because spawned tasks are abandoned. | `tokio::signal::ctrl_c()` → send close on insert/range channels → engine drains remaining commands, flushes memtable to SSTable, syncs + closes WAL → join compactor → exit cleanly. |
| **Medium** | **Metrics: flush duration broken** | Shows 112M µs (~112s) for a 5s run — nonsensical. Probably a scoping issue with the `_t0` variable or WSL2 `Instant` drift. All latency percentiles show the same value (P50=P90=P99), indicating the HDR histogram only recorded one distinct value per metric. | Debug the `_t0` capture in `engine_loop`'s flush block — ensure it measures just the flush call, not accumulated loop time. For the single-valued histograms: verify `hdrhistogram::record()` isn't silently erroring (check `Result`) and that values span a meaningful range. Consider `SyncHistogram` for thread-safe recording. |

### GREAT — engineering excellence

| Sev | Issue | Why | Fix |
|-----|-------|-----|-----|
| **Medium** | **Leveled compaction** | Current compaction merges ALL SSTables into one → O(N) write amplification per compaction cycle. Bounds file count but doesn't bound I/O cost. | Implement leveled compaction with size-tiered levels (L0=raw flushes, L1..LN=merged runs). Each level has a size cap. When exceeded, compact one sorted run into the next level. |
| **Medium** | **SSTable streaming** | `Engine::range()` loads ALL matching SSTable records into `Vec<Record>` before merging. Kills memory on large datasets. | Make `SSTableIter` yield records lazily (read + decode one at a time). `MergeIter` polls each source iterator on demand. The range response already streams via `mpsc::Sender<Vec<Record>>` — just need the read side to be lazy too. |
| **Low** | **Per-day engine isolation** | All data goes into one flat SSTable directory. No data lifecycle management. | Rotate engine per day: `data/2026-05-17/storage/`. Query crosses days via a router that fans out the range request. Old directories can be archived/deleted. |
| **Low** | **Server CLI parity** | `bin/server.rs` only has `--addr` and `--batch-size`. Can't set `--memtable-mb`, `--channel-capacity`, `--data-dir` at runtime. | Add clap args matching loadgen's engine flags. Store data under `--data-dir/<timestamp>/` with a symlink to `latest/`. |
| **Low** | **Async WAL fsync** | `Wal::sync()` calls `sync_all()` synchronously on the engine task, blocking all command processing during the fsync. | Offload `sync_all()` to `spawn_blocking`. The engine can process the next batch during fsync, then wait for completion before issuing another sync. (Risk: multiple pending syncs need careful ordering.) |
| **Low** | **Metrics histogram reset** | Histograms accumulate across benchmark runs. Final dump includes data from ALL runs, not just the last one. | Add `Metrics::reset()` that replaces each histogram with a fresh one. Call it before each benchmark run. Or: implement README's "histogram flush after snapshot" strategy. |
| **Low** | **Compaction error recovery** | `compaction_loop` returns `Err` on failure but the spawned task ignores it — compaction fails silently. | Log the error and restart the loop rather than returning. Add a watch channel for the engine to detect compactor health. |
| **Low** | **Range filter: regex / prefix** | Currently only substring filter (simple `contains`). A log aggregator should support regex and prefix-match filters. | Add `FilterKind` enum to `proto::Range`: `Substring`, `Prefix`, `Regex`. Apply matching in `MemTableIterOwned` and skip SSTable records that don't match. |
| **Low** | **Benchmark regression harness** | No automated way to track performance changes across commits. | Save benchmark results to `prof/baseline.json`. Add `--compare` flag to loadgen that diffs against baseline and flags regressions >5%. |
