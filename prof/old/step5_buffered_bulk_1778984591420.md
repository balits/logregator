
==> RESULTS <==

  RUN  CLIENTS  INSERTS    INS/S    RNG/S  REC/QRY      ERR      SST  INS_P99  RNG_P99    BATCH WORKLOAD
    0        1      274       27       30     91.4        0        0       26    48040      512     bulk
    1        2      456       46       49    219.1        0        0       32    48149      512     bulk
    2        6     2524      252      243    370.9        0        0       81    48150      512     bulk

==> INSERT LATENCY (µs) <==

  RUN      MIN      P50      P90      P95      P99    P99.9      MAX     MEAN   STDDEV
------------------------------------------------------------------------------------------
    0       26       26       26       26       26       26       26       26        0
    1       32       32       32       32       32       32       32       32        0
    2       15       61       81       81       81       81       81       56       22

==> RANGE LATENCY (µs) <==

  RUN      MIN      P50      P90      P95      P99    P99.9      MAX     MEAN   STDDEV
------------------------------------------------------------------------------------------
    0       18    43933    44216    44716    48040    51701    51701    33784    18788
    1       12    43956    44308    44895    48149    48763    48763    40897    11523
    2       20    42128    44234    44987    48150    50806    52134    24717    21295

==> SATURATION ANALYSIS <==
Workload: Bulk
Concurrency vs Insert Throughput:
   1 clients: 27 ops/s
   2 clients: 46 ops/s
   6 clients: 252 ops/s

Max throughput: 252 ops/s at 6 clients
No clear saturation point detected within tested concurrency range.
