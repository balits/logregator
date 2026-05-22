
==> RESULTS <==

  RUN  CLIENTS  INSERTS    INS/S    RNG/S  REC/QRY      ERR      SST  INS_P99  RNG_P99    BATCH WORKLOAD
    0        1    30372     3037     3018    230.4        0        0       22     1559      512     bulk
    1        2    12099     1210     1217    474.3        0        0       20     7534      512     bulk
    2        6     8145      814      802    604.8        0        0       34    27975      512     bulk

==> INSERT LATENCY (µs) <==

  RUN      MIN      P50      P90      P95      P99    P99.9      MAX     MEAN   STDDEV
------------------------------------------------------------------------------------------
    0        7       12       16       18       22       22       22       12        3
    1        8       11       14       16       20      280      280       11        9
    2        9       21       32       34       34       34       34       20        7

==> RANGE LATENCY (µs) <==

  RUN      MIN      P50      P90      P95      P99    P99.9      MAX     MEAN   STDDEV
------------------------------------------------------------------------------------------
    0       12      194      719      902     1559     3006     4983      306      343
    1       13      830     3902     5124     7534    11810    16607     1559     1757
    2       19     3448    20737    23310    27975    36343    52518     7408     8487

==> SATURATION ANALYSIS <==
Workload: Bulk
Concurrency vs Insert Throughput:
   1 clients: 3037 ops/s
   2 clients: 1210 ops/s
   6 clients: 814 ops/s

Max throughput: 3037 ops/s at 1 clients
SATURATION DETECTED: Throughput dropped from 3037 to 1210 ops/s from concurrency 1 to 2
SATURATION DETECTED: Throughput dropped from 1210 to 814 ops/s from concurrency 2 to 6
