# just file module for tsdb related commands
# after importing this file into another just file,
# all the commands below can be called by prefixing their name with
# the module name, like tsdb::fmt
mod tsdb "logregator-tsdb/justfile"

# NOTE:
#
# Right now some old crates fail to compile, so only `logregator-tsdb` is tested.
# The old crates should be pruned and this recipe should test the whole workspace instead

test_all: tsdb::test
    @echo "JUST RECIPE SHOULD BE REPLACED WITH cargo test --workspace, once other crates are pruned"
    # cargo test --workspace


lint_all: tsdb::lint
    @echo "JUST RECIPE SHOULD BE REPLACED \\
    WITH cargo clippy --workspace, once other crates are pruned"
    # cargo clippy --workspace --all-targets --all-features -- -D warnings

fmt_check_all:
    cargo fmt --check
