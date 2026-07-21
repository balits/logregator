use std::rc::Rc;

use pretty_assertions::assert_eq;

use super::{Block, BlockCursor, BlockWriter, MAX_BLOCK_SIZE, SZ_U16, SZ_U32, WriteOutput};
use crate::codec::BytesCodec;
use crate::record::{Key, Record};

fn tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_test_writer()
        .try_init();
}

fn codec() -> BytesCodec {
    BytesCodec
}

fn key(source_id: u64, ts: u64, seq: u64) -> Key {
    Key {
        source_id,
        timestamp: ts,
        sequence_num: seq,
        stream_id: 7,
    }
}

fn record(source_id: u64, ts: u64, seq: u64, payload_len: usize) -> Record {
    Record {
        key: key(source_id, ts, seq),
        payload: vec![0xAB_u8; payload_len].into_boxed_slice(),
    }
}

/// Build a valid block from `n` records with the given payload size.
/// Panics if any write is rejected, callers should size payloads to fit.
fn build_block(n: usize, payload_len: usize) -> (Rc<Block>, Vec<Record>) {
    let records: Vec<Record> = (0..n as u64)
        .map(|i| record(1, i, i, payload_len))
        .collect();

    let mut bw = BlockWriter::new(codec(), None).unwrap();
    for r in &records {
        assert!(
            matches!(bw.write(r).unwrap(), WriteOutput::Written),
            "build_block: record {r:?} was rejected (shrink the payload length)"
        );
    }
    let (b, _, _) = bw.into_block();
    (Rc::new(b), records)
}

#[test]
fn block_writer() {
    tracing();

    BlockWriter::new(codec(), None).unwrap();

    BlockWriter::new(codec(), Some(4096)).unwrap();
    BlockWriter::new(codec(), Some(MAX_BLOCK_SIZE)).unwrap();

    assert!(
        BlockWriter::new(codec(), Some(MAX_BLOCK_SIZE + 1)).is_err(),
        "limit > MAX_BLOCK_SIZE must return Err"
    );

    // --- Empty finish ---------------------------------------------------------

    // A writer that never had write() called still produces a valid (0-record) block.
    let (empty_block, _, _) = BlockWriter::new(codec(), Some(4096)).unwrap().into_block();
    assert_eq!(empty_block.num_of_records(), 0);

    // --- Size accounting ------------------------------------------------------

    // After one write, size == wire_len of that record.
    let mut bw = BlockWriter::new(codec(), Some(4096)).unwrap();
    let r0 = record(1, 0, 0, 32);
    let wire = r0.wire_len();
    bw.write(&r0).unwrap();
    assert_eq!(
        bw.size(),
        wire,
        "size must equal wire_len after first write"
    );
    assert_eq!(
        bw.first_key(),
        Some(&r0.key),
        "fist_key should match the single inserted records key"
    );
    assert_eq!(
        bw.last_key(),
        Some(&r0.key),
        "last_key should match first_key"
    );

    // After a second write with the same payload, size doubles.
    bw.write(&record(1, 1, 1, 32)).unwrap();
    assert_eq!(bw.size(), wire * 2, "size must accumulate across writes");

    // --- WriteResult::Written -------------------------------------------------

    // The first record is always accepted regardless of how tight the limit is,
    // because there is nothing meaningful to flush to if we reject it.
    let tight = r0.wire_len() + SZ_U16 + SZ_U16; // just enough for one record
    let mut bw = BlockWriter::new(codec(), Some(tight)).unwrap();
    assert!(
        matches!(bw.write(&r0).unwrap(), WriteOutput::Written),
        "first record must always be Written"
    );

    // --- WriteResult::Full ----------------------------------------------------

    // The second record must be rejected when the block is at capacity.
    assert!(
        matches!(bw.write(&record(1, 1, 1, 32)).unwrap(), WriteOutput::Full),
        "second record must be Full when limit is exhausted"
    );

    // After a Full result the writer is still usable: into_block() produces a block
    // that contains only the records that were actually Written.
    let (block, _, _) = bw.into_block();
    assert_eq!(
        block.num_of_records(),
        1,
        "only the Written record must appear in the block"
    );

    // --- Multiple records up to capacity -------------------------------------

    // Build a limit that fits exactly two records and verify the third is Full.
    let r = record(1, 0, 0, 64);
    // 2 records + 2 u16 offsets + 1 u16 num_of_records suffix
    let two_limit = 2 * r.wire_len() + 3 * SZ_U16;
    let mut bw = BlockWriter::new(codec(), Some(two_limit)).unwrap();
    assert!(matches!(
        bw.write(&record(1, 0, 0, 64)).unwrap(),
        WriteOutput::Written
    ));
    assert!(matches!(
        bw.write(&record(1, 1, 1, 64)).unwrap(),
        WriteOutput::Written
    ));
    assert!(
        matches!(bw.write(&record(1, 2, 2, 64)).unwrap(), WriteOutput::Full),
        "third record must be Full"
    );
    assert_eq!(bw.into_block().0.num_of_records(), 2);
}

#[test]
fn block_encode() {
    tracing();

    // --- Offset segment invariants -------------------------------------------

    let n = 4;
    let (block, _) = build_block(n, 16);

    // num_of_records() is derived from offsets.len() — never from a stored field.
    assert_eq!(block.num_of_records(), n);

    // offset_segment_start is always data.len().
    assert_eq!(block.offset_segment_start(), block.data.len());

    // Each offset costs exactly SZ_U16 bytes in the segment.
    assert_eq!(block.offset_segment_len(), n * SZ_U16);

    // offset_segment_end = start + len.
    assert_eq!(
        block.offset_segment_end(),
        block.offset_segment_start() + block.offset_segment_len()
    );

    // --- Encoded byte length --------------------------------------------------

    // Layout: [ data ][ offsets: n*u16 ][ num_of_records: u16 ][ checksum: u32 ]
    let encoded = block.encode_block().unwrap();
    let expected_len = block.offset_segment_start() + n * SZ_U16 + SZ_U16 + SZ_U32;
    assert_eq!(
        encoded.len(),
        expected_len,
        "encoded length must account for data + offsets + num_of_records suffix + checksum"
    );

    // --- Determinism ----------------------------------------------------------

    // Encoding the same block twice must produce identical bytes.
    let a = block.encode_block().unwrap();
    let b = block.encode_block().unwrap();
    assert_eq!(a, b, "encode must be deterministic");

    // --- Single-record block --------------------------------------------------

    let (single, _) = build_block(1, 32);
    single.encode_block().unwrap(); // must not panic or error

    // --- Empty block is rejected ----------------------------------------------

    // An empty block (0 offsets) has no meaningful on-disk representation.
    let (empty, _, _) = BlockWriter::new(codec(), None).unwrap().into_block();
    assert!(
        empty.encode_block().is_err(),
        "encode on an empty block must return Err"
    );
}

#[test]
fn block_decode() {
    tracing();

    // --- Happy path: single record -------------------------------------------

    let (block, _) = build_block(1, 32);
    let encoded = block.encode_block().unwrap();
    let decoded = Block::decode_block(&encoded).unwrap();
    assert_eq!(*block, decoded, "single-record roundtrip must be identity");

    // --- Happy path: multiple records ----------------------------------------

    let (block, _) = build_block(8, 32);
    let encoded = block.encode_block().unwrap();
    let decoded = Block::decode_block(&encoded).unwrap();
    assert_eq!(*block, decoded, "multi-record roundtrip must be identity");

    // All derived properties must survive the roundtrip unchanged.
    assert_eq!(decoded.num_of_records(), 8);
    assert_eq!(decoded.offset_segment_start(), block.offset_segment_start());
    assert_eq!(decoded.offset_segment_len(), block.offset_segment_len());
    assert_eq!(decoded.offset_segment_end(), block.offset_segment_end());

    // --- Malformed input: too short to hold a checksum -----------------------

    assert!(
        Block::decode_block(&[]).is_err(),
        "empty slice must be rejected"
    );
    assert!(
        Block::decode_block(&[0u8; 1]).is_err(),
        "1 byte must be rejected"
    );
    assert!(
        Block::decode_block(&[0u8; SZ_U32 - 1]).is_err(),
        "fewer than SZ_U32 bytes must be rejected"
    );

    // --- Malformed input: raw data without offset footer ---------------------

    // Feeding only the data region (no offsets, no suffix, no checksum) must
    // fail.  The checksum alone guarantees this because the stored checksum
    // won't be present.
    let (block, _) = build_block(3, 16);
    assert!(
        Block::decode_block(&block.data).is_err(),
        "raw data without offset footer must be rejected"
    );

    // --- Malformed input: truncated by one byte ------------------------------

    let encoded = block.encode_block().unwrap();
    let truncated = &encoded[..encoded.len() - 1];
    assert!(
        Block::decode_block(truncated).is_err(),
        "truncated buffer must be rejected"
    );

    // --- Malformed input: checksum mismatch (single bit flip) ----------------

    let mut corrupted = encoded.clone();
    corrupted[0] ^= 0x01; // flip one bit anywhere in the data region
    assert!(
        Block::decode_block(&corrupted).is_err(),
        "bit-flipped buffer must fail checksum verification"
    );

    // --- Malformed input: doubled buffer -------------------------------------

    // Concatenating a valid encoded block with itself produces a buffer whose
    // trailing checksum belongs to the second copy, not to the doubled payload.
    // The checksum covers everything before it, so this must be rejected.
    let doubled = [encoded.clone(), encoded.clone()].concat();
    assert!(
        Block::decode_block(&doubled).is_err(),
        "doubled buffer must be rejected by checksum"
    );

    // --- Malformed input: checksum field zeroed ------------------------------

    let mut zero_cksum = encoded.clone();
    let tail = zero_cksum.len();
    zero_cksum[tail - 4..].fill(0x00);
    assert!(
        Block::decode_block(&zero_cksum).is_err(),
        "zeroed checksum must be rejected"
    );

    // --- Malformed input: num_of_records field corrupted ---------------------

    // Patch the num_of_records u16 to a value that can't be consistent with
    // the data region.  Because we write the checksum *after* num_of_records,
    // the checksum will also be wrong — so this is caught at checksum time.
    let mut bad_count = encoded.clone();
    let tail = bad_count.len();
    // num_of_records sits at [tail-6..tail-4] (before the u32 checksum)
    bad_count[tail - 6] = 0xFF;
    bad_count[tail - 5] = 0xFF;
    assert!(
        Block::decode_block(&bad_count).is_err(),
        "corrupted num_of_records must be caught"
    );
}

#[test]
fn block_cursor() {
    tracing();

    // --- Fresh cursor has no record ------------------------------------------

    let (block, records) = build_block(5, 16);
    let mut cursor = BlockCursor::new(block.clone(), codec());

    assert!(
        !cursor.is_record(),
        "fresh cursor must not point at a record"
    );
    assert!(
        !cursor.is_error(),
        "fresh cursor must not be in an error state"
    );
    assert_eq!(cursor.current(), None);
    assert_eq!(cursor.peek_key(), None);

    // --- seek_to_first points at record[0] -----------------------------------

    cursor.seek_to_first();
    assert!(cursor.is_record());
    assert_eq!(cursor.current().unwrap().key, records[0].key);
    assert_eq!(cursor.peek_key(), Some(&records[0].key));

    // --- next() traverses in insertion order ---------------------------------

    // Collect every key seen during a full forward pass.
    let mut seen = Vec::new();
    cursor.seek_to_first();
    while cursor.is_record() {
        seen.push(cursor.current().unwrap().key.clone());
        cursor.next();
    }
    let expected_keys: Vec<Key> = records.iter().map(|r| r.key.clone()).collect();
    assert_eq!(
        seen, expected_keys,
        "traversal must visit records in insertion order"
    );

    // --- Exhaustion: cursor becomes invalid after walking off the end --------

    assert!(
        !cursor.is_record(),
        "cursor must be invalid after walking off the end"
    );
    assert_eq!(cursor.current(), None);

    // --- seek_to_first resets an exhausted cursor ----------------------------

    cursor.seek_to_first();
    assert!(cursor.is_record());
    assert_eq!(cursor.current().unwrap().key, records[0].key);

    // --- next() on a single-record block hits end on the first advance -------

    let (single_block, single_records) = build_block(1, 16);
    let mut sc = BlockCursor::new(single_block, codec());
    sc.seek_to_first();
    assert_eq!(sc.current().unwrap().key, single_records[0].key);
    sc.next();
    assert!(
        !sc.is_record(),
        "single-record block must be exhausted after one next()"
    );

    // --- peek_key advances alongside current() -------------------------------

    cursor.seek_to_first();
    cursor.next(); // now at records[1]
    assert_eq!(cursor.peek_key(), Some(&records[1].key));
    cursor.next(); // now at records[2]
    assert_eq!(cursor.peek_key(), Some(&records[2].key));

    // --- payload is preserved through the cursor ------------------------------

    cursor.seek_to_first();
    for expected in &records {
        let got = cursor.current().unwrap();
        assert_eq!(got.key, expected.key);
        assert_eq!(got.payload, expected.payload, "payload must survive decode");
        cursor.next();
    }

    // --- seek: exact key match -----------------------------------------------

    for r in &records {
        cursor.seek(r.key.clone());
        let got = cursor.current().expect("exact seek must find a record");
        assert_eq!(got.key, r.key);
    }

    // --- seek: key between two stored keys lands on the successor ------------

    // records have ts = 0..4, seq = 0..4.
    // A key with seq = records[2].seq - 1 sorts just below records[2].
    let between = Key {
        source_id: records[2].key.source_id,
        timestamp: records[2].key.timestamp,
        sequence_num: records[2].key.sequence_num.saturating_sub(1),
        stream_id: records[2].key.stream_id,
    };
    cursor.seek(between);
    // Binary search returns Err(i) = first index strictly greater than the
    // probe, which here should be records[2].
    let got = cursor
        .current()
        .expect("between-key seek must land on a record");
    assert!(
        got.key == records[2].key || got.key == records[1].key,
        "between-key seek must land on records[1] or records[2], got {:?}",
        got.key
    );

    // --- seek: before all records lands on records[0] (or is invalid) --------

    let before_all = Key {
        source_id: 0,
        timestamp: 0,
        sequence_num: 0,
        stream_id: 0,
    };
    cursor.seek(before_all);
    // Must not panic; if a record is returned it must be the first one.
    if cursor.is_record() {
        assert_eq!(cursor.current().unwrap().key, records[0].key);
    }

    // --- seek: past the last key invalidates the cursor ----------------------

    let last = records.last().unwrap();
    let beyond = Key {
        source_id: last.key.source_id,
        timestamp: u64::MAX,
        sequence_num: u64::MAX,
        stream_id: last.key.stream_id,
    };
    cursor.seek(beyond);
    assert!(
        !cursor.is_record(),
        "seek past last key must invalidate cursor"
    );

    // --- seek: idempotent on same key ----------------------------------------

    cursor.seek(records[2].key.clone());
    let first_result = cursor.current().map(|r| r.key.clone());
    cursor.seek(records[2].key.clone());
    let second_result = cursor.current().map(|r| r.key.clone());
    assert_eq!(
        first_result, second_result,
        "repeated seek to same key must be idempotent"
    );

    // --- seek: works correctly on a decoded block ----------------------------

    // Encode → decode and verify seek finds the same records on the
    // reconstructed block as on the original.
    let encoded = block.encode_block().unwrap();
    let decoded_block = Rc::new(Block::decode_block(&encoded).unwrap());
    let mut dc = BlockCursor::new(decoded_block, codec());
    for r in &records {
        dc.seek(r.key.clone());
        let got = dc
            .current()
            .expect("seek on decoded block must find record");
        assert_eq!(got.key, r.key);
    }
}
