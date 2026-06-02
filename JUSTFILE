TAG := "latest"
OUT := ""
PROFILE_MEM := "1000"

# === Benchmark ===

bench addr="127.0.0.1:4321" dur="30":
    cargo run --bin benchmark --release \
        -- loadgen \
        --workloads write,read,mixed,tail \
        --concurrency-min   1 \
        --concurrency-max  16 \
        --concurrency-step  2 \
        --duration-secs {{ dur }} \
        --profile-mem {{ PROFILE_MEM }} \
        --tag {{ TAG }} \
        --external-server true \
        --addr {{ addr }} \
        {{ if OUT != "" { "--out " + OUT } else { "" } }} \

# === Server startup ===

server-bench:
    RUSTFLAGS="--cfg tokio_unstable" \
    cargo run --release --bin server -- --addr 127.0.0.1:4321 --profile none --log-level error

server-bench-flame *extra:
    PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH" RUSTFLAGS="-C force-frame-pointers=y" \
    cargo flamegraph --cmd 'record -F 99 --call-graph fp' --bin server -- \
    {{ if extra != "" { extra } else { "--addr 127.0.0.1:4321 --profile none --log-level error --fsync-interval 1000" } }}

# === Profiling (WSL2-limited) ===
#
# WSL2 constraints: kernel.perf_event_paranoid=2, no tracepoints, no eBPF, no perf sched.
# What works:  hardware events (cycles, instructions, cache), DWARF call graphs, on-CPU samples.
# What doesn't: context switch events, scheduler tracepoints, off-CPU time capture.
#
# For true off-CPU flamegraphs (Brendan Gregg method), run on bare Linux with:
#   perf sched record -g -- bench_command   (needs CAP_PERFMON)
# Then process with FlameGraph/stackcollapse-perf-sched.awk + flamegraph.pl

# On-CPU flamegraph (the one we use):
#   just server-bench-flame
#
# Hardware event stats — counts cycles, instructions, cache misses, page faults
server-perf-stat *extra:
    RUSTFLAGS="-C force-frame-pointers=y" cargo build --release --bin server 2>&1 | tail -1
    ARGS={{ if extra != "" { extra } else { "--addr 127.0.0.1:4321 --profile none --log-level error --fsync-interval 1000" } }}
    perf stat -e cycles,instructions,cache-misses,cache-references,branch-misses,page-faults \
        target/release/server $ARGS

# Syscall breakdown — count + cumulative time per syscall type (via strace)
server-offcpu-strace *extra:
    RUSTFLAGS="-C force-frame-pointers=y" cargo build --release --bin server 2>&1 | tail -1
    ARGS={{ if extra != "" { extra } else { "--addr 127.0.0.1:4321 --profile none --log-level error --fsync-interval 1000" } }}
    strace -fc -o strace.syscall_summary target/release/server $ARGS &
    SERVER_PID=$!
    echo "Server PID $SERVER_PID — run benchmark in another terminal, then kill with: kill $SERVER_PID"
    wait $SERVER_PID 2>/dev/null
    echo "=== Syscall summary ==="
    cat strace.syscall_summary

# I/O syscall trace with per-call latency — shows which file operations block
server-offcpu-io *extra:
    RUSTFLAGS="-C force-frame-pointers=y" cargo build --release --bin server 2>&1 | tail -1
    ARGS={{ if extra != "" { extra } else { "--addr 127.0.0.1:4321 --profile none --log-level error --fsync-interval 1000" } }}
    strace -T -e trace=read,write,pread64,pwrite64,fsync,fdatasync,openat,close \
        -o strace.io_trace target/release/server $ARGS &
    SERVER_PID=$!
    echo "Server PID $SERVER_PID — run benchmark, then: kill $SERVER_PID"
    echo "After kill, check strace.io_trace for per-syscall timing"
    wait $SERVER_PID 2>/dev/null

# === CI ===

ci:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test

cpu-bench:
    cargo bench