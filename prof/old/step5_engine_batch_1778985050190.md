
==> RESULTS <==

  RUN  CLIENTS  INSERTS    INS/S    RNG/S  REC/QRY      ERR      SST  INS_P99  RNG_P99    BATCH WORKLOAD
    0        1      257       26       28     99.5        0        0       15    48006      512     bulk
    1        2      457       46       49    228.4        0        0       16    48141      512     bulk
    2        6     4161      416      409    468.2        0        0       36    48050      512     bulk

==> INSERT LATENCY (µs) <==

  RUN      MIN      P50      P90      P95      P99    P99.9      MAX     MEAN   STDDEV
------------------------------------------------------------------------------------------
    0       15       15       15       15       15       15       15       15        0
    1       12       16       16       16       16       16       16       14        2
    2       11       15       32       32       36       36       36       17        7

==> RANGE LATENCY (µs) <==

  RUN      MIN      P50      P90      P95      P99    P99.9      MAX     MEAN   STDDEV
------------------------------------------------------------------------------------------
    0       12    43962    44130    44557    48006    48132    48132    35197    17767
    1       18    43966    44601    47861    48141    48835    48835    40586    12251
    2       13     1150    44102    44824    48050    51701    59994    14642    20036

==> SATURATION ANALYSIS <==
Workload: Bulk
Concurrency vs Insert Throughput:
   1 clients: 26 ops/s
   2 clients: 46 ops/s
   6 clients: 416 ops/s

Max throughput: 416 ops/s at 6 clients
No clear saturation point detected within tested concurrency range.
