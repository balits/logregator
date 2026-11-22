# What?

This document is a collection of todos and issues,
so i dont have to keep everything in my head (:

Each PR-sized group has a GitHub issue. Put `Closes #N` in the PR description so
GitHub links the PR to the issue and closes it on merge. The file:line details
live here so the issues stay readable.

## Current state (2026-11-18)

- `cargo check -p logregator-tsdb --all-targets` passes (with warnings).
- `cargo test -p logregator-tsdb`: 22 passed, 7 failed (6 bloom panic, 1 missing SST dir).
- Default branch is still `lsm`; `main`/`net` are stale; local `lsm` is 2 commits ahead of origin.
- Uncommitted: `bloom.rs` fix (keep it) and `Cargo.lock` sync.

## PR 0 — Repo setup + CI

Issue: **#6** · branch: `ci/tsdb-tests` · base: `main`

- [x] Retarget `main` to the current `lsm` tip (`git branch -f main lsm`, `git push --force-with-lease origin main`) and switch the GitHub default branch to `main`.
- [x] Delete the stale `lsm` branch (remote + local) and tidy tracking refs (`git fetch --prune`, `git remote set-head origin main`).
- [x] Add `.github/workflows/tsdb.yml`: `cargo fmt --all --check`, `cargo test -p logregator-tsdb`, `cargo check -p logregator-tsdb --all-targets`.
- [x] Add `.github/PULL_REQUEST_TEMPLATE.md`.
- [x] Enable branch protection on `main`: require PR + green checks.
- [x] Exit: all later PRs target `main`.

## PR 1 — SST write / reopen roundtrip

Issue: **#7**, also closes **#3** · branch: `fix/tsdb-sst-roundtrip`

- [x] Deny warnings using clippy in CI.
- [x] Bloom encode: slice each field into its own destination range, use `dst[20..20 + item.bitmap.len()]` (safe for oversized `dst`), add a byte-exact test. The fix is already in the working tree — commit it.
- [x] Bloom wire doc says `bitmap_len: u64` but encodes `u32` (`bloom.rs:113-116`) — already fixed in working tree.
- [x] Guard `BloomFilter::new(0, _)` division by zero (`bloom.rs:39-48,78`, assert already in working tree) and validate decoded `n_bits`/`n_hashes` against bitmap length (`bloom.rs:141-165`).
- [x] Align integrity checksum scope: writer hashes blocks + metadata + bloom + footer offsets (`sst.rs:341,257,281,289-298`), reader hashes only metadata + bloom (`sst.rs:532-570`) — every written SST fails to open.
- [x] Fix the stale "FIXED" comment that no longer matches `finalize` (`sst.rs:478-485`).
- [x] Validate offsets before slicing: `bloom_offset` against `fsize - footer` (`sst.rs:507-524`), `read_block` uses only `debug_assert` (`sst.rs:601-613`), `decode_metas` checks no ordering/monotonicity (`sst.rs:1195-1255`).
- [x] Block size accounting omits the 4-byte checksum (`block.rs:77-88,369-380,124-126`), `todo!()` for oversized data (`block.rs:94-96`), a failed encode leaves zero-filled bytes behind a stale offset (`block.rs:382-397`).
- [x] Checked `u32` arithmetic for `bloom_filter_start` (`sst.rs:243-250`); keep the `io::Error` context on footer reads (`sst.rs:474-476`).
- [x] Re-enable or replace the commented-out block tests (`block.rs`) — 4 active tests (`block_writer`, `block_encode`, `block_decode`, `block_cursor`)(AI-generated) 
- [ ] Add write → open → full-read tests with multiple blocks, plus corrupt-CRC and short-destination cases; no test currently passes `Some((Bound, Bound))` to a cursor.
- [ ] Exit: the 6 SST tests go green.

## PR 2 — Bounded cursors + merge iterator

Issue: **#2** · branch: `fix/tsdb-cursor-bounds`

- [ ] Finish the original bounded-cursor task: cursors must start and end inside the key range instead of filling up with irrelevant records.
- [ ] Remove the reachable `todo!()` in `SstCursor::next` (`sst.rs:869,887`) and the dead `_next` with another `todo!()` (`sst.rs:903-934`).
- [ ] Fix first-record loss: `MergeIter` calls `cursor.next()` before `take_current()` (`merge_iter.rs:94-102`) even though `SstCursor::new` already positions on the first in-range record (`sst.rs:730-758`).
- [ ] Propagate cursor errors instead of discarding them (`merge_iter.rs:95,108-110`); make exhaustion distinct from a real error (`sst.rs:785-791,891-896`).
- [ ] Enforce bounds per record: `_is_in_bounds` is never called (`sst.rs:761-776`), end bound is only checked per block (`sst.rs:936-945`), `seek`/`seek_to_first` ignore bounds (`sst.rs:954-996`), `Bound::Excluded` can leak duplicate keys (`sst.rs:738-744`).
- [ ] `SstCursor::new` always returns `Ok(Some(..))`; the `Ok(None)` caller branch is dead (`sst.rs:718-759`, `merge_iter.rs:53-56`).
- [ ] `block_idx_for_stream_id` can return `i == len` (`sst.rs:668-675,722-731`); binary search on `last_key.stream_id` skips earlier blocks when a stream spans blocks (`sst.rs:649-666`); commented-out search code has its own `todo!()` (`sst.rs:648-665`).
- [ ] Bloom pre-pruning uses only the start bound's `stream_id` (`merge_iter.rs:46-50`), duplicating the check in `sst.rs:641-646,722-725` and ignoring the end bound.
- [ ] Remove the unnecessary `'m: 's` lifetime coupling (`merge_iter.rs:19-29,111-116`).
- [ ] Add `MergeIter` unit tests: cross-source ordering, duplicate keys, bound endpoints, error propagation, empty sources; multi-block cursor advance tests.
- [ ] Block cursor: Key ordering mismatch (Key::Ord vs to_be_bytes)
- [ ] Exit: `range()` returns exactly the SST records inside the requested bounds.

## PR 3 — Recovery, layout and IO worker

Issue: **#8** · branch: `fix/tsdb-recovery`

- [ ] Settle the SST path layout: flushes write `<base>/<id>.sst` (`io.rs:36-42`, `sst.rs:1036-1046`) but recovery scans `<base>/sst/<day>/*.sst` (`lsm.rs:225-273`).
- [ ] Implement `valid_daydir_path` (`sst.rs:47-51`) and create the directories (`WAL_DIR_NAME` unused at `wal.rs:51-52`; WALs written to LSM root at `lsm.rs:133-136`). `lsm::test::recover` currently fails with `NotFound` at `lsm.rs:232`/`:951`.
- [ ] Replace `unimplemented!()` and `todo!()` in `lsm.rs:202-209,236,245,249,258` with real errors.
- [ ] Restore IDs from `RecoverOptions` instead of hardcoding 0 (`lsm.rs:131,153,176-177`); decide an overflow policy for `Counter::wrapping_add` (`counter.rs:7-11`).
- [ ] Load manifest snapshot state into the stream registry and SST handles (`manifest.rs:215-219`; `lsm.rs:168,232-273`); `SstState::Removed` is produced but never acted on (`manifest.rs:241-245`).
- [ ] `SstFileWriter` uses `create_new(true)` (`sst.rs:392-397`) but recovery seeds memtable id 0 → flush after restart fails on an existing file.
- [ ] Define the IO worker contract: `IoEvent` is sent (`lsm.rs:52,484-503,632-634`) but never consumed in production (`io.rs:59-64`); `flush_memtable` is never called.
- [ ] Add a real `FsyncWal` handler — no handler exists and `ActiveWal` has no fsync method (`io.rs:62`, `wal.rs:134-262`).
- [ ] Handle flush failures: SST is finalized before manifest append, leaving orphans (`io.rs:50-55`); `FlushableMemtable::mark_flushed` is never called (`memtable.rs:108-122`, `lsm.rs:580-598`).
- [ ] Send failures are flattened into strings with no retry (`lsm.rs:484-486,502-504,632-634`); no shutdown/backpressure event.
- [ ] Change `error!()` to `trace!`/`debug!` in `drain_sst_queue` (`lsm.rs:586`).
- [ ] Rebuild a minimal `tests/integ.rs` (currently 100% commented out, lines 1-293): append → flush → drain IO events → recover → query.
- [ ] Move test-only API out of the production surface (`recover_options_for_test` at `lsm.rs:93`, `new_test`/`append_single` at `lsm.rs:382-390,640-683`).
- [ ] Exit: flush → restart → query test passes.

## PR 4 — WAL and manifest durability

Issue: **#9** · branch: `fix/tsdb-durability`

- [ ] Fix inverted `create_active` semantics: `Inner::create_active` returns `None` when an active WAL exists (`wal.rs:378-381`) but the public wrapper treats `Some` as "found existing" and `None` as "create new" (`wal.rs:141-170`); logs are inverted too (`wal.rs:378-380,669-673`).
- [ ] `open_active` discards the integrity verdict (`wal.rs:447-451,782-785`), so a WAL with a trailing checksum gets appended to; `ChecksumState::update` return value is ignored (`wal.rs:183-193,962-975`).
- [ ] `find_active_wal` accepts any extension except `.frozen.wal` (`wal.rs:689-723`); one malformed id aborts discovery (`wal.rs:735-747`); duplicate-error message is swapped (`wal.rs:725-733`).
- [ ] Torn trailing records are silently dropped (`codec.rs:197-200`, `wal.rs:618-650`) and then appended after; truncate to the last valid frame before reopening. CRC is computed over re-encoded entries, not raw bytes (`wal.rs:634-635`).
- [ ] `flush` fsyncs the read-only `Arc<File>` instead of the write fd and does not fsync the directory after rename (`wal.rs:232-240`); unsafe `ptr::read`/`ptr::write` on enum state (`wal.rs:211-227,980-1011`); `ChecksumNotFound` is never constructed (`wal.rs:833-837`).
- [ ] Manifest `FramedWriter::bytes_written` is not reset after `set_len(0)` (`codec.rs:234,261-263`; `manifest.rs:143,169-175`) → a snapshot is triggered on every append after the first.
- [ ] Snapshot is rewritten in place with no temp file, rename, fsync or generation marker (`manifest.rs:264-282`); a crash mid-write bricks startup.
- [ ] Manifest appends are buffered with no production flush/fsync (`manifest.rs:141-149,188-193`); a torn tail makes `from_manifest` fail (`manifest.rs:231-236`).
- [ ] `append_many` overwrites its `flushed` flag per entry (`manifest.rs:153-162`).
- [ ] `ManifestCodec::decode` ignores the label codec's consumed length (`manifest.rs:589-600,609`); `LabelMapCodec::decode` never returns `Ok(None)` on incomplete input (`label.rs:296-309`).
- [ ] Validation ranges `1..255` reject length-255 strings (`manifest.rs:387-393`, `label.rs:316-342`); `Compaction` length cast to `u32` (`manifest.rs:451-453`); public enum variants bypass validating constructors (`manifest.rs:321-332` vs `:367-400`, `io.rs:54-55`).
- [ ] Un-comment and fix the append-after-recovery regression test (`wal.rs:1185-1237`); add checksum, torn-tail, invalid-tag and manifest-corruption tests.
- [ ] Exit: append → crash → recover preserves all complete records; no repeated snapshots.

## PR 5 — Query correctness

Issue: **#10** · branch: `fix/tsdb-query-correctness`

- [ ] `LabeledIter::next` returns `None` when its current `MergeIter` is exhausted instead of advancing `self.iters` (`label.rs:419-441`); `lsm.rs:534-563` creates one per stream, so multi-stream queries return only the first.
- [ ] `unsafe impl Send for LabeledIter` is unsound (`label.rs:387-391`); `unsafe impl Send for LabelMap` is unnecessary (`label.rs:173-176`).
- [ ] `StreamRegistry::insert` leaves stale `kv_index`/`stream_by_labelmap` entries on labelmap updates and has no remove/update API (`label.rs:46-98`).
- [ ] `label_intersection` skips unknown labels instead of returning an empty result (`label.rs:127-158`); duplicate label keys collapse on parse (`label.rs:282-289`).
- [ ] Collision handling aborts on a 64-bit fingerprint (`lsm.rs:442-447`, `label.rs:178-191,224-235`); deserialized snapshots can carry an arbitrary fingerprint (`label.rs:167-171`).
- [ ] Label decode accepts empty/255-byte strings that `ManifestEntry::stream_update` rejects → `.expect()` panic (`label.rs:263-294`, `manifest.rs:387-393`, `lsm.rs:457-458`); `wire_len` doc claims `u32` prefixes but uses `u8` (`label.rs:196-212`).
- [ ] `Record` Eq/Ord compares only the key (`record.rs:83-101`) → `BTreeSet` dedups by key and codec round-trip tests cannot detect payload corruption.
- [ ] Memtable `first_key` is set once and `last_key` overwritten in insertion order (`memtable.rs:177-180`) but used as flush bounds (`io.rs:32-45`) → out-of-order keys silently lost; duplicate inserts inflate `size_bytes` (`memtable.rs:181-182`); size constant is 160 KiB despite the 16 Mb comment (`memtable.rs:11-12`).
- [ ] Payload minimum inconsistency: codec requires ≥2 bytes (`record.rs:19,250-258`) but `lsm.rs:398` validates the pre-label payload and stores `payload[label_bytes_read..]` (`lsm.rs:481`).
- [ ] Key `Ord` field order (`stream_id, timestamp, sequence_num, source_id`) differs from wire order; stale comments propose raw byte comparison (`record.rs:109-116,186-191`, `block.rs:505-506`, `sst.rs:959-960`).
- [ ] Cross-crate mismatch: `logregator-net` expects `Rc<LabelMap>` while `LabeledIter` yields `Arc<LabelMap>` (`logregator-net/src/lib.rs:298-305`).
- [ ] Tests: multi-stream queries, out-of-order inserts, duplicate keys, registry semantics, payload-aware roundtrips, `Excluded`/`Unbounded` bounds.
- [ ] Exit: multi-stream and out-of-order integration tests pass.

## PR 6 — Tooling cleanup

Issue: **#11** · branch: `chore/tsdb-tooling`

- [ ] Remove `#![feature(clone_from_ref)]` (`lib.rs:1,94`) so the crate builds on stable; update `rust-toolchain.toml` and README.
- [ ] Typos: `MAINFEST_DEFAULT_MAX_FILE_SIZE` (`manifest.rs:22`), `"faild to range"` (`lsm.rs:370`), `AVG_RECORD_WIRE_LENGHT` (`record.rs:28-29`), `"not enoguh bytes"` (`record.rs:297`), `"wrtie access"` (memtable doc).
- [ ] Remove unused dependencies: `bytes`, `arc-swap`, `crossbeam-skiplist`, `const_format` (`Cargo.toml:7,12,13,21`).
- [ ] Remove dead code: commented `AppendError` (`lsm.rs:351-366`), old WAL constants (`wal.rs:73-82`), commented manifest directory scan (`manifest.rs:24-25,54-109`), `CodecError` TODO (`codec.rs:20-21`), `Error` TODO (`lib.rs:31-33`).
- [ ] Exit: stable toolchain builds the crate; clippy clean.

## Existing issues not bound to a PR yet

- [ ] **#1** — Make `Codec::decode` return `Result<ITEM, CodecError>` instead of `Result<Option<ITEM>, CodecError>`. Related: `FramedReader` repeats the same error forever without advancing (`codec.rs:208`) and swallows `fill()` I/O errors (`codec.rs:200-206`). Decide after PR 4 settles error policy.
- [ ] **#4** — Criterion benchmarks for tsdb (`logregator-tsdb/benches` is WIP); compare with RocksDB once PR 3/5 make the engine work end-to-end.
- [ ] **#5** — Tokio network adapter (`net` branch); out of scope for the storage crate.

## Later / outside this crate

- [ ] Complete the flush pipeline (PR 3); compaction is explicitly not implemented (`manifest.rs:324-327`).
- [ ] Arena allocation for memtable payloads — README decision: defer until profiling shows allocation is a bottleneck.
- [ ] zstd block compression; mmap-based SSTable reads.
- [ ] Integrate `logregator-tsdb` into `logregator-core`; glommio adapter later.
- [ ] Workspace does not compile (only tsdb is maintained); `logregator-tools/src/lib.rs` declares a missing `bench` module.

## Workflow reminders

- One issue ↔ one PR; put `Closes #N` in the PR body.
- Fix + regression test in the same commit, so `git bisect` stays useful.
- Run `cargo fmt --all`, `cargo test -p logregator-tsdb`, `cargo check -p logregator-tsdb --all-targets` before each commit.
- Base every PR on `main`, merge sequentially, delete the branch after merge.
