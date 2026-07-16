# Motivation

The storage engine currently represents records as

```rust
pub struct Record {
    pub key: Key,
    pub payload: Box<[u8]>,
}
```

This is an intentionally simple design.

The decoder owns the payload, every record is self-contained, and there are no lifetimes propagating throughout the codebase.

The codec API is equally straightforward:

```rust
pub trait Codec {
    fn encode(&self, rec: &Record, dst: &mut [u8]) -> Result<usize, Error>;
    fn decode(&self, src: &[u8]) -> Result<Option<(Record, usize)>, Error>;
}
```

The decoder receives bytes and returns an owned `Record`.

This is easy to understand and easy to use.

However, as the database grows, this representation becomes increasingly expensive.

---

# The Problem

Every decoded record currently performs its own heap allocation.

```
socket/file
      │
      ▼
 &[u8]
      │
      ▼
decode()
      │
      ▼
Vec<u8>
      │
      ▼
Box<[u8]>
```

Suppose a memtable contains

```
1,000,000 records
```

Then there are also

```
1,000,000 heap allocations
```

Each allocation has several costs.

* allocator bookkeeping
* allocator synchronization
* allocator fragmentation
* pointer chasing
* cache misses
* one deallocation when the memtable is dropped

The payload itself is usually much smaller than the allocator metadata.

For workloads consisting of many small payloads, allocator overhead becomes surprisingly significant.

---

# Why This Matters

An LSM tree spends most of its time doing one of two things:

1. writing into the active memtable
2. reading records from immutable memtables during flushes

Both workloads are append-heavy.

Records are almost never individually deleted.

Instead,

```
allocate
allocate
allocate
allocate
allocate

...

drop ENTIRE memtable
```

This allocation pattern is exactly what arena allocators were designed for.

---

# Arena Allocation

Instead of allocating each payload independently,

```
Record
    └── Box<[u8]>

Record
    └── Box<[u8]>

Record
    └── Box<[u8]>
```

allocate one large region.

```
+------------------------------------------------------+
|                                                      |
| payload A                                            |
| payload B                                            |
| payload C                                            |
| payload D                                            |
| payload E                                            |
|                                                      |
+------------------------------------------------------+
```

Appending a record simply copies its payload into the next available location.

```
arena
    ^
    |
next free byte
```

No allocator is involved after the arena itself has been created.

---

# Why Storage Engines Like Arenas

Arena allocation provides several useful properties.

## Constant-time allocation

Appending a payload becomes

```text
offset = next

copy bytes

next += len
```

No allocator.

No free list.

No searching.

No locking.

---

## Constant-time destruction

Dropping one million `Box<[u8]>` values requires one million deallocations.

Dropping an arena requires exactly one.

```
drop(arena)
```

Everything disappears at once.

This matches the lifetime of a memtable perfectly.

---

## Better locality

Individual heap allocations are scattered throughout memory.

```
payload A

           payload B


payload C


                    payload D
```

An arena stores everything together.

```
AAAAAAAA
BBBBBBBB
CCCCCCCC
DDDDDDDD
```

Sequential scans become much more cache friendly.

---

## Predictable memory accounting

Instead of estimating

```
sizeof(record)
+ payload
+ allocator overhead
```

the arena knows exactly how many bytes have been used.

```
arena.used()
```

This makes memtable accounting simpler.

---

# The Lifetime Problem

A common first idea is to avoid allocation entirely.

```rust
struct Record<'a> {
    key: Key,
    payload: &'a [u8],
}
```

Unfortunately this pushes lifetimes into every component.

```
Memtable<'a>

FrozenMemtable<'a>

MergeIter<'a>

SSTBuilder<'a>

Compactor<'a>
```

Every API becomes lifetime-parameterized.

This quickly spreads throughout the entire storage engine.

While borrowed payloads are excellent for parsers, they are much less suitable for long-lived storage structures.

---

# Why Not bytes::Bytes?

Another idea is

```rust
payload: Bytes
```

This avoids copies in networking code.

However, a memtable is not a networking primitive.

Once a record enters the database, ownership is already transferred.

Reference counting provides relatively little benefit.

It also couples the storage engine to a particular ecosystem.

The database should ideally remain usable from

* std::io
* Tokio
* Glommio
* io_uring
* mmap
* custom transports

without introducing unnecessary dependencies.

---

# Codec Independence

One design goal is that the codec should remain unaware of allocation strategy.

Today,

```text
Codec

bytes
↓

Record
```

Tomorrow,

```text
Codec

bytes
↓

Arena

↓

Record
```

The wire format should not change.

The codec should not know where memory comes from.

Encoding and decoding are logically separate from memory management.

---

# Current Design

The existing codec remains

```rust
fn decode(src: &[u8])
    -> Result<Option<(Record, usize)>, Error>;
```

Internally it performs

```
copy payload

↓

Box<[u8]>
```

This is acceptable.

It is simple.

It is correct.

It is likely sufficient until profiling identifies allocation as a bottleneck.

Premature optimization should be avoided.

---

# Future Direction

Eventually, decoding could become

```
decode_into(...)
```

rather than

```
decode(...)
```

The caller would supply the destination.

Conceptually,

```
input bytes

↓

codec

↓

memory sink
```

rather than

```
input bytes

↓

codec

↓

Box<[u8]>
```

This separates parsing from allocation.

---

# A Possible PayloadSink Abstraction

One possible interface is

```rust
trait PayloadSink {
    fn alloc(&mut self, len: usize) -> &mut [u8];
}
```

The codec then becomes

```rust
decode(src, sink)
```

Internally,

```
read payload length

↓

sink.alloc(len)

↓

copy bytes

↓

return Record
```

The sink decides where bytes are stored.

Possible implementations include

* Vec
* bump arena
* slab allocator
* mmap region
* shared memory

The codec never needs to know.

---

# Arena Layout

One possible arena layout

```
+------------------------------------------------------------+
| payload A | payload B | payload C | payload D | payload E |
+------------------------------------------------------------+
```

The record stores

```
key

offset

length
```

rather than

```
Box<[u8]>
```

Conceptually

```
Record

key

offset = 8128

len = 96
```

Reading the payload becomes

```
arena[offset .. offset + len]
```

---

# Slab Allocation

Another possibility is a slab allocator.

Unlike a bump arena,

```
allocate

allocate

free

allocate
```

is supported.

Memory can be reused.

This is useful for long-lived caches.

However,

memtables are append-only.

Individual records are almost never removed.

Entire memtables disappear together.

Because of this,

a bump arena is generally simpler and faster.

---

# Ownership Model

The arena should be owned by the memtable.

```
Memtable

├── BTreeSet<Record>

└── Arena
```

Records only contain offsets.

The arena owns the bytes.

Freezing a memtable naturally freezes the arena.

Dropping the frozen memtable releases everything simultaneously.

This mirrors the lifecycle of the LSM tree.

---

# Memory Accounting

Current accounting approximates

```
sizeof(Record)

+

payload length
```

Arena accounting becomes

```
arena.used()

+

metadata
```

This is simpler and typically more accurate.

---

# Performance Expectations

Moving from individual `Box<[u8]>` allocations to arena allocation is expected to improve

* append throughput
* cache locality
* memory fragmentation
* memtable destruction time

The wire format does **not** change.

The codec does **not** fundamentally change.

The allocation strategy changes.

---

# Design Philosophy

The storage engine should remain independent of networking frameworks.

The codec should remain independent of allocation strategy.

The allocator should remain independent of the wire format.

Each component should have a single responsibility.

```
Transport
    │
    ▼
Codec
    │
    ▼
Allocator
    │
    ▼
Storage Engine
```

Maintaining these boundaries allows each subsystem to evolve independently.

---

# Decision

For now,

keep the existing codec exactly as it is.

The current implementation is simple, easy to reason about, and entirely adequate until profiling indicates otherwise.

Arena allocation should be viewed as an implementation detail of future memtable storage rather than a required redesign of the serialization layer.

When the time comes to optimize allocation, introduce an allocation abstraction beneath the codec rather than exposing lifetimes or transport-specific types throughout the storage engine.

Until then, prefer simplicity over premature optimization.

