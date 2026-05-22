## Issues found across all benchmarks:
1. First-batch timeout on every run (high priority)
Every client's first insert batch hits the 30s timeout and fails. Error count = concurrency × 512 (one full batch per client). Root cause: the engine blocks on a synchronous flush() during connection setup, and the client doesn't wait for the TCP handshake + engine readiness before sending its first batch.

2. Engine loop is the single bottleneck (high priority)
Write throughput caps at ~20k INS/S regardless of concurrency (1 to 16 clients all produce the same throughput). The single-threaded engine_loop that handles insert → WAL → memtable → flush is fully CPU-bound. Adding clients just increases latency, not throughput.

3. Synchronous flush blocks all inserts (high priority)
Each flush() freezes the engine loop for ~33ms (32MB threshold) to ~69ms (64MB threshold). During a flush, no inserts or range queries are processed — clients pile up in the mpsc channel. This is the root cause of the 30s timeout on multi-client runs (the channel fills up and clients back up).

4. Range queries share the engine loop with inserts (medium priority)
In mixed workloads, ranges and inserts compete for the same task. At 8 clients: only 215 INS/S + 213 RNG/S (vs 20k INS/S write-only). Range P99 latency degrades from 29ms (4 clients) to 128ms (8 clients) because ranges wait behind insert batches.

5. Memory grows monotonically — no plateau (medium priority)
RSS climbs ~0.8 MB/s across runs, never stabilizing. Rust's allocator doesn't return pages to the OS. The page cache holds WAL + SSTable data even after flushes complete. A 2-minute benchmark grows from 16MB to 128MB with no sign of leveling off.

6. Compaction not working (low priority)
compaction_count: 0 in all runs. The compaction loop runs but never merges SSTables. After 6 flushes, only 3 SSTs exist (some get cleaned up, but compaction isn't contributing). This will become a problem with longer runs.

7. Latency P50=P90=P99 for batch operations (cosmetic)
All percentiles are identical for batch_insert, flush, and range — every operation in a recv_many batch completes at nearly the same wall-clock time. The histogram captures one value per batch, not per record, making the percentile breakpoints meaningless for batch operations.

##Recommended fix order:
1. Dual-memtable (unblock the engine loop during flush → fixes issues 1, 2, 3, and indirectly 4)
2. Proper graceful shutdown (cleaner benchmarking)
3. Compaction wiring (issue 6)
4. Page-cache / memory management (issue 5)
5. Per-record histogram recording (issue 7)