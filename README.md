# CharybdisDB
This project implements a **persistent key-value storage engine** based on the **Log-Structured Merge Tree (LSM-tree)** design.
The system emphasizes **crash consistency, durability, and read efficiency** and is intended as an educational and experimental storage engine rather than a production database.

The implementation explores core storage system concepts such as **write ahead logging, immutable SSTables, compaction, bloom filters, and sparse indexing**, using safe and idiomatic Rust.

## Design Goals

- Correctness under crashes and restarts
- Clear separation of in-memory and on-disk structures
- Efficient point lookups with bounded disk I/O
- Minimal concurrency complexity while remaining thread safe
- Faithful implementation of classic LSM tree ideas

## High-Level Architecture

The system follows a **write-optimized storage model**:

1. **Writes** are first recorded in a **Write-Ahead Log (WAL)** to guarantee durability.
2. Updates are applied to an in-memory **memtable**.
3. When the memtable reaches a size threshold, it is flushed as an immutable **Sorted String Table (SST)**.
4. Periodic **compaction** merges multiple SSTs into new ones, discarding obsolete entries and tombstones.
5. **Reads** consult the memtable first, then search SSTs in descending recency order using bloom filters and sparse indexes.

## Core Components

### Memtable
- Implemented as a `BTreeMap<String, Option<String>>`
- Stores the most recent writes and deletions (tombstones)
- Protected by a `RwLock` to allow concurrent reads

### Write-Ahead Log (WAL)
- Append-only log persisted on disk
- Every `PUT` or `DELETE` is written to the WAL **before** mutating the memtable
- WAL is replayed on startup to restore state after crashes
- WAL is truncated only after SST flushes and manifest updates are durable
- Batched `fsync` is used to amortize disk costs

### Sorted String Tables (SSTs)
- Immutable, sorted files stored on disk
- Written sequentially to minimize random I/O
- Represent a consistent snapshot of the memtable at flush time
- SST entries include tombstones to preserve deletion semantics

### Bloom Filters
- One bloom filter per SST
- Used to probabilistically reject SSTs during reads
- Reduces unnecessary disk reads for negative lookups
- Configurable false positive rate
- Serialized using `rkyv` for compact, zero copy loading

### Sparse Indexes
- Stored separately per SST
- Maps selected keys to byte offsets in the SST file
- Enables efficient seeking into large SST files
- Binary search over sparse index followed by sequential scan

### Manifest
- Tracks the set of live SST files
- Updated atomically using a temporary file + rename protocol
- Used on startup to reconstruct in memory metadata
- Prevents dangling or partially written SSTs from being loaded

### Compaction
- Triggered after a configurable number of updates
- Performs a multi way merge over all existing SSTs
- Resolves duplicate keys by keeping the newest value
- Drops deleted keys (tombstones) where safe
- Produces a new set of compacted SSTs and updates the manifest atomically

## Read Path

For a `GET(key)` operation:

1. Check the memtable
2. Iterate over SSTs from newest to oldest:
   - Check bloom filter
   - Binary search sparse index for starting offset
   - Sequentially scan SST entries until key is found or surpassed
3. Return value, tombstone or not found result

This design bounds disk I/O while preserving correctness.

## Concurrency Model

- Shared state managed using `Arc`
- `RwLock` protects the memtable and manifest
- Atomic counters track SST IDs and update counts
- WAL file access is serialized via a `Mutex`
- Designed for correctness over maximal throughput

## Persistence and Crash Consistency

The system guarantees that:

- Acknowledged writes are durable across crashes
- WAL replay restores all committed operations
- Partially written SSTs are never observed
- Manifest updates are atomic and crash safe

Crash recovery proceeds by:
1. Loading the manifest
2. Reconstructing SST metadata
3. Replaying the WAL
4. Truncating the WAL after successful recovery

## Serialization Strategy

| Component | Format |
|--------|--------|
| WAL entries | JSON |
| SST entries | JSON |
| Sparse index | binary |
| Bloom filter | binary |

The mixed approach favors debuggability for core data and efficiency for metadata.

## Testing

The project currently includes:
- An end to end integration test (`test_server.rs`)
  - Validates PUT / GET / DELETE behavior
  - Exercises durability via server restarts
  - Verifies correctness across multiple operations

While limited in number, these tests cover the full storage pipeline from client request to disk persistence and recovery.

## Limitations

- No range queries or iterators
- No background compaction thread
- No checksums or corruption detection
- JSON based SST format
- Single node, non distributed design

## Design scope and learning goals
This project is intentionally scoped to emphasize the core mechanics of
LSM-tree–based storage engines:

- Write ahead logging and crash recovery
- Immutable SSTables with tombstones
- Read path optimization via bloom filters and sparse indexing
- Atomic metadata management using manifest files
- Deterministic compaction semantics

Certain production oriented features (e.g., background compaction threads,
checksumming, compression and binary SST formats) are intentionally omitted
to keep the implementation compact and auditable and to focus on
correctness and crash safety over raw throughput.

The design mirrors real world storage engines (e.g., LevelDB style LSMs)
while remaining small enough to reason about end to end.

## License
MIT
