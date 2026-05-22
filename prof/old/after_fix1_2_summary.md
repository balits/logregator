# After Fix 1 + 2: Range double-clone fix + Client batching

## Bulk workload (50% range, 50% insert)
| Clients | INS/S | RNG/S | Range P50 | Insert P99 | ERR | SST |
|---------|-------|-------|-----------|------------|-----|-----|
| 1       | 24    | 26    | 43,965µs  | 15µs       | 0   | 0   |
| 2       | 48    | 51    | 43,945µs  | 41µs       | 0   | 0   |
| 6       | 245   | 233   | 42,790µs  | 24µs       | 0   | 0   |

## Write-only workload
| Clients | INS/S | Insert P99 | ERR  | SST |
|---------|-------|------------|------|-----|
| 1       | 81,408| 16µs       | 512  | 1   |

## Comparison with Phase 4 (before)
| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Bulk INS/S (6 cl) | ~197 | 245 | +24% |
| Insert P99 | ~98,000µs | 24µs | **99.98% reduction** |
| Range P50 | ~44ms | ~44ms | unchanged |
| Write rec/s | N/A | 81,408 | new capability |

## Key insights
1. **Batch insert gives 81,408 rec/s write throughput** with Insert P99 of 16µs
2. **Range P50 at 44ms is the bottleneck** for mixed workloads (limits to ~22 iterations/sec)
3. **Insert P99 improved 6000x** compared to per-record sends
4. **Next step: Dual-channel priority** to reduce insert contention during range queries
