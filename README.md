# logregator

High-performance log aggregator in Rust. Ingests log records over TCP, stores them in an LSM-tree backed engine, and supports time-range queries.

## Architecture

```
TCP client → length-prefixed binary protocol
                 ↓
            Network handler (tokio)
                 ↓
          Bounded mpsc channel
                 ↓
       Engine run loop (actor model)
            ↙              ↘
      MemTable            SSTable pool
         ↓                    ↓
      flush()            compaction
         ↓                    ↓
      SSTable            merged SSTable
```

## Roadmap

### 0. Storage engine: index blocks
Add a sparse key→offset index to the SSTable footer. Turns range queries from O(N) full-file scans to O(log N) seeks. Footer grows from 8 bytes to 16 bytes:

`[records...][index_block][bloom_filter][bloom_offset u64][index_offset u64]`

One index entry per ~16 KB of record data. `SSTableIter` uses it to seek directly to the first relevant record instead of scanning from byte 0.

### 1. DbManager: per-day engine routing
New `DbManager` sits between the server and a map of per-day engines:

- **Directory layout**: `<base_dir>/YYYY-MM-DD/{wal.log, *.sst}` — one independent LSM tree per day
- **Routing**: `Insert`/`BatchInsert` → extract date from timestamp, route to/create that day's `Engine`. `Range` → fan out to all engines whose date falls in `[start_ts, end_ts)`, merge results.
- **Per-engine compactor**: Each `Engine::open()` creates its own compaction channels and spawns its own `compactor_loop` internally.
- **Retention**: Background `tokio::time::interval` task deletes day directories older than the retention period.
- **Server integration**: Server sends `Command`s via a shared `mpsc::Sender<Command>`; `DbManager::run()` owns the receiver.

### 2. CLI
Binary at `src/bin/logregator.rs`:

```bash
logregator insert <source_id> <ts> <key> <value>
logregator batch-insert <json_file>
logregator range <source_id> <key> <start_ts> <end_ts> [--filter <substring>]
```

The `--filter` flag filters results by a simple `record.value.contains(substring)`,
as building a fine grained grep or query language is outside of the scope of this project.

### 3. Benchmarks
- Single insert throughput (ops/sec)
- Batch insert throughput (records/sec for N=10, 100, 1000)
- Range query latency (empty, 10 results, 1000 results)
- Bloom filter miss performance gain from index blocks
