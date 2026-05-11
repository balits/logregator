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

## Status

### Done
- [x] WAL (write-ahead log, recovery, clear)
- [x] MemTable with BTreeSet ordering
- [x] SSTable format (sorted records, bloom filter footer)
- [x] SSTableIter (time-range + key filtering)
- [x] SSTableScanner (full-file scan for compaction)
- [x] MergeIter (k-way merge across memtable + SSTables)
- [x] BloomFilter (insert, contains, encode/decode)
- [x] Engine actor loop (insert, range, compaction result handling)
- [x] Compactor (merge N files → 1, dedup, channel-based)

### Next
- [ ] **TCP server** — `tokio::net::TcpListener` with length-prefixed framing
- [ ] **Wire compaction** — spawn `compactor_loop`, delete old files on result
- [ ] **SSTable index blocks** — sparse key→offset map in footer for O(log N) seeks
- [ ] **Graceful shutdown** — signal → drain writes → flush → close
- [ ] **README / architecture docs**

### Future
- [ ] Config TOML file and CLI (with `clap`)
- [ ] Client library
- [ ] Structured metrics (flush latency, compaction duration, read amplification)
- [ ] Block compression (zstd)
- [ ] Leveled compaction
