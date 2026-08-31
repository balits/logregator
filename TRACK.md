# What?

This document is a collection of todos and issues,
so i dont have to keep everything in my head (:

## Roadmap

- [ ] Complete `logregator-tsdb` flush and compaction pipeline
- [ ] Integrate `logregator-tsdb` as the storage backend in `logregator-core`
- [ ] Tokio frontend adapter
- [ ] Glommio (io_uring) frontend adapter
- [ ] Arena allocation for memtable payloads
- [ ] zstd block compression
- [ ] mmap-based SSTable reads

## Known Issues

The following items need attention before the next release:

- [ ] workspace wont compile, as ive only been working on `logregator-tsdb/`
- [ ] `logregator-tools/src/lib.rs` declares `pub mod bench;` but the module does not exist
- [ ] Typo in `logregator-tsdb/src/manifest.rs`: `MAINFEST_DEFAULT_MAX_FILE_SIZE` should be `MANIFEST_DEFAULT_MAX_FILE_SIZE`
- [ ] Typo in `logregator-tsdb/src/lsm.rs:116`: `"faild to range"` should be `"failed to range"`
- [ ] Typo in `logregator-tsdb/src/record.rs:29`: `AVG_RECORD_WIRE_LENGHT` should be `AVG_RECORD_WIRE_LENGTH`
- [ ] Typo in `logregator-tsdb/src/record.rs:297`: `"not enoguh bytes"` should be `"not enough bytes"`
- [ ] Typo in `logregator-tsdb/src/memtable.rs` doc comment: `"wrtie access"` should be `"write access"`
- [ ] `logregator-tsdb/src/block.rs` has a `FIXME` comment about unrefactored AI-generated test code
- [ ] `logregator-tsdb/src/lsm.rs:295-308` uses `error!()` for debug-level logging in `drain_sst_queue`
- [ ] Uses nightly-only feature gate `#![feature(clone_from_ref)]`
- [ ] No `LICENSE` file
- [ ] No CI configuration
- [ ] And a bunch of TODO comments for smaller things
