loadgen tag="" out="" profile-mem="1000":
    cargo run --bin loadgen --release -- \
        --workloads write,read,mixed,tail \
        --concurrency-min   1 \
        --concurrency-max  16 \
        --concurrency-step  2 \
        --duration-secs    10 \
        --profile-mem {{ profile-mem }} \
        {{ if tag != "" { "--tag " + tag } else { "" } }} \
        {{ if out != "" { "--out " + out } else { "" } }}

ci:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test

cpu-bench:
    cargo run --release --bin loadgen