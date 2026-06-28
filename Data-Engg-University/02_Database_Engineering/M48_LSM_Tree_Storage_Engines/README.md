# M48: LSM-Tree Storage Engines

**Course:** DBE-INT-101 — Storage Engine Internals  
**Module:** 02 of 05  
**Global Module ID:** M48  
**Semester:** 2  
**School:** Database Engineering (DBE)

---

## Table of Contents

1. [The Why](#1-the-why)
2. [Mental Model](#2-mental-model)
3. [Core Concepts](#3-core-concepts)
4. [Hands-On Walkthrough](#4-hands-on-walkthrough)
5. [Code Toolkit](#5-code-toolkit)
6. [Visual Reference](#6-visual-reference)
7. [Common Mistakes](#7-common-mistakes)
8. [Production Failure Scenarios](#8-production-failure-scenarios)
9. [Performance and Tuning](#9-performance-and-tuning)
10. [Interview Q&A](#10-interview-qa)
11. [Cross-Question Chain](#11-cross-question-chain)
12. [Flashcards](#12-flashcards)
13. [Further Reading](#13-further-reading)
14. [Lab Exercises](#14-lab-exercises)
15. [Key Takeaways](#15-key-takeaways)
16. [Connections to Other Modules](#16-connections-to-other-modules)
17. [Anti-Patterns](#17-anti-patterns)
18. [Tools Reference](#18-tools-reference)
19. [Glossary](#19-glossary)
20. [Self-Assessment](#20-self-assessment)
21. [Module Summary](#21-module-summary)

---

## 1. The Why

The B-tree (M47) is optimized for reads: any row can be found in 3-4 I/Os, range scans traverse the leaf-level linked list sequentially, and the structure is balanced by design. But B-trees have a fundamental tension with writes: every insert requires modifying a page at a random location on disk (wherever the key belongs in sorted order), and if that page is full, a split modifies additional pages. For a table receiving 100,000 random inserts per second, a B-tree on HDD would generate 100,000 random writes per second — an impossible workload. Even on SSDs, write amplification of 100-200× means a 1GB/s write workload generates 100-200GB/s of physical I/O.

The Log-Structured Merge-Tree (LSM-tree) was designed to invert this tradeoff. Published by O'Neil et al. in 1996, the LSM-tree's core insight is: what if we never perform random writes? Instead, buffer all writes in memory, and when the in-memory buffer is full, flush it to disk as a single sorted, sequential write. The resulting file (an SSTable) is immutable — it is never modified after creation. When there are too many SSTables, merge-sort them into a new, larger SSTable — again, a sequential write. Every write to disk is sequential. Random writes are eliminated.

The cost: reads must search multiple SSTables (rather than one B-tree). An LSM-tree trades read performance for dramatically better write performance and I/O efficiency.

This is the architecture of Cassandra, HBase, RocksDB, LevelDB, ScyllaDB, InfluxDB, and Kafka's log compaction. It underlies much of the write-heavy infrastructure in the data engineering stack. Understanding it makes the performance characteristics of these systems predictable rather than mysterious.

---

## 2. Mental Model

### The Core Idea: Make Everything Sequential

Disk and SSD storage has a fundamental asymmetry: sequential I/O is dramatically faster than random I/O. On an HDD, sequential reads achieve 100-200MB/s while random reads achieve 1-2MB/s — a 100× difference. On an SSD, the gap is smaller (sequential: 500MB/s vs random: 50MB/s) but still meaningful (10×). The LSM-tree is an extreme case of "design for sequential I/O."

The mechanism is a three-layer funnel:

1. **MemTable (in memory):** All writes go into a sorted in-memory structure (typically a red-black tree or skip list). Reads that can be served from the MemTable are fast. When the MemTable reaches a size threshold (e.g., 64MB), it is "frozen" — no more writes — and a new MemTable is created. The frozen MemTable is flushed to disk as an SSTable.

2. **SSTable (on disk, immutable):** A Sorted String Table — a sorted, immutable file on disk. Contains a sequence of (key, value) pairs sorted by key. Never modified after creation. Multiple SSTables can exist simultaneously; newer SSTables take precedence for a given key.

3. **Compaction (background merge):** Over time, many SSTables accumulate. Compaction merges SSTables together using merge-sort, resolving key conflicts (keeping the newest value) and purging tombstones (deleted keys). Compaction is sequential I/O and is done in the background.

### Why This Achieves Low Write Amplification

Inserting one row: write to MemTable (in-memory, no I/O). When MemTable is full (64MB), flush to disk: one sequential write of 64MB containing ~500,000 rows. Per-row I/O: 64MB / 500,000 rows = 130 bytes/row ≈ 1× write amplification (compared to 100× for B-trees). Compaction adds amplification (merging SSTables rewrites data), but the total write amplification for RocksDB in practice is 10-30× — still far lower than B-tree with WAL.

---

## 3. Core Concepts

### 3.1 MemTable

The MemTable is an in-memory, sorted data structure that receives all writes. It must support:
- Fast insert: O(log N) for sorted insert
- Fast lookup: O(log N) for point queries
- Fast in-order iteration: needed for flushing to SSTable in sorted order

**Data structure choice:** Red-black tree or skip list. RocksDB uses a skip list by default. Both provide O(log N) insert and O(log N) lookup. The skip list has slightly better cache behavior for iteration.

**Write-ahead log for durability:** The MemTable lives only in memory. If the process crashes, the MemTable is lost. To survive crashes, every write to the MemTable is also appended to a WAL (Write-Ahead Log) on disk — a sequential append, cheap I/O. On recovery, the WAL is replayed to reconstruct the MemTable. (M49 covers WAL in detail.)

**Immutable MemTable:** When the MemTable reaches its size threshold, it becomes "immutable" (no new writes) while being flushed to an SSTable. Meanwhile, a new active MemTable receives incoming writes. There is zero write pause during the flush.

### 3.2 SSTable (Sorted String Table)

An SSTable is a sorted, immutable file containing (key, value) pairs in sorted key order. SSTables are the primary on-disk data structure.

**Internal structure of an SSTable:**

```
SSTable File Layout:
┌─────────────────────────────────────────────────┐
│  Data Blocks                                     │
│  ┌────────────────────────────────────────────┐  │
│  │ Block 1: entries 1-N (sorted by key)       │  │
│  │   [key_len(2)][key][val_len(2)][value]...  │  │
│  │   Block size: ~4KB (configurable)          │  │
│  └────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────┐  │
│  │ Block 2: entries N+1 to 2N                 │  │
│  └────────────────────────────────────────────┘  │
│  ... (many data blocks)                          │
├─────────────────────────────────────────────────┤
│  Index Block                                     │
│  For each data block: (last_key, block_offset)   │
│  Allows binary search to find the correct block  │
│  for a given key — reads one block per lookup    │
├─────────────────────────────────────────────────┤
│  Bloom Filter Block                              │
│  Probabilistic membership test: "is key K        │
│  in this SSTable?" False positives possible;     │
│  false negatives impossible. Avoids reading      │
│  SSTables that definitely don't contain the key. │
├─────────────────────────────────────────────────┤
│  Footer                                          │
│  Offset of index block, magic number, checksum   │
└─────────────────────────────────────────────────┘
```

### 3.3 Bloom Filter

A Bloom filter is a probabilistic data structure that answers: "is key K definitely NOT in this SSTable?" with 100% accuracy, OR "is key K probably in this SSTable?" with a tunable false positive rate (typically 1-3%).

Without a Bloom filter, a point lookup in an LSM-tree must search all SSTables at all levels. A Bloom filter allows skipping SSTables that definitely don't contain the key. For a workload with many point lookups on non-existent keys (cache miss lookups), Bloom filters reduce read I/O by 10-100×.

**Bloom filter math:**
- `m` = bit array size
- `n` = number of elements
- `k` = number of hash functions
- Optimal k = (m/n) × ln(2) ≈ 0.693 × (m/n)
- False positive rate = (1 - e^(-kn/m))^k
- For 1% FPR: need ~10 bits per key (regardless of key/value size)

**RocksDB default:** 10 bits per key per Bloom filter, giving ~1% FPR. This means: for 100M keys × 10 bits = 125MB of Bloom filter memory, 99% of "key not found" lookups avoid all disk I/O.

### 3.4 Compaction

Compaction is the background process that merges multiple SSTables into fewer, larger ones. It serves three purposes:
1. **Space reclamation:** Overwritten keys and tombstones (deleted keys) accumulate across SSTables. Compaction resolves them: the newest version of each key is kept, tombstones are removed once all older versions are gone.
2. **Read performance:** Fewer SSTables to search = fewer Bloom filter checks = faster reads.
3. **Space efficiency:** Without compaction, disk usage grows unboundedly (old versions of keys are never cleaned up).

**Compaction is expensive:** It is sequential I/O but involves reading and rewriting large amounts of data. Compaction amplification (the ratio of bytes written by compaction to bytes of application data) is the dominant component of LSM-tree write amplification.

### 3.5 Leveled Compaction (RocksDB/LevelDB)

Leveled compaction organizes SSTables into levels. Each level has a size limit that grows exponentially (by a multiplier, typically 10):
- Level 0 (L0): Freshly flushed SSTables, each sorted independently. Key ranges overlap between SSTables.
- Level 1 (L1): Size limit = 256MB. Key ranges do NOT overlap (each key is in at most one SSTable). Compacted from L0.
- Level 2 (L2): 2.56GB. Non-overlapping key ranges.
- Level 3 (L3): 25.6GB. Non-overlapping key ranges.
- ...

**Compaction trigger:** When L0 has too many SSTables (e.g., 4), they are compacted with all overlapping SSTables in L1. The output is sorted SSTables written to L1, maintaining non-overlapping ranges.

**Read path:** For a point lookup, check L0 (all SSTables, in reverse chronological order), then L1 (binary search for the single SSTable whose range covers the key), then L2, etc.

**Write amplification for leveled compaction:** Moving data from L0→L1→L2→L3 involves rewriting data at each level transition. With a level multiplier of 10: each byte of data is compacted approximately 10 times. Total write amplification: ~30-50× for typical RocksDB configurations.

### 3.6 Size-Tiered Compaction (Cassandra)

Size-tiered compaction groups SSTables of similar size and merges them. When 4 SSTables of size S exist, they are merged into one SSTable of size ~4S.

**Advantage over leveled:** Lower write amplification (~10-20×) — data is written fewer times.

**Disadvantage:** Higher space amplification — during compaction, both old and new SSTables exist simultaneously (2× space). Key ranges in a tier overlap, requiring more SSTables to be searched per read.

### 3.7 Tombstones

Deleting a key in an LSM-tree cannot modify existing SSTables (they are immutable). Instead, a **tombstone** is inserted — a special marker indicating that the key has been deleted. During reads, if the newest version of a key is a tombstone, the key is treated as absent.

Tombstones accumulate in SSTables and are only purged during compaction. If a large number of rows are deleted (e.g., a data retention cleanup deleting 70% of rows) without adequate compaction, tombstone accumulation causes:
- Slower reads (many tombstones to scan through)
- Increased disk usage (tombstones plus old data coexist)
- In Cassandra: "tombstone storm" — reads must scan millions of tombstones, causing timeouts

---

## 4. Hands-On Walkthrough

### 4.1 RocksDB Inspection via `ldb` Tool

```bash
# ldb is RocksDB's command-line debugging tool (comes with RocksDB)
# Inspect the structure of a RocksDB database (e.g., Kafka's internal log compaction)

# List SSTables by level
ldb --db=/path/to/rocksdb manifest_dump
# Output:
# Level 0: 3 files, 12.3 MB total
#   000042.sst: seq=1000-2000, 4.1 MB, key_range=[user_0001, user_3500]
#   000045.sst: seq=2000-3000, 4.2 MB, key_range=[user_0050, user_7200]
#   000048.sst: seq=3000-4000, 4.0 MB, key_range=[user_0200, user_9900]
# Level 1: 12 files, 245 MB total (non-overlapping key ranges)
#   000031.sst: key_range=[user_0000, user_0999]
#   000032.sst: key_range=[user_1000, user_1999]
#   ... (non-overlapping ranges at L1+)

# Note L0: key ranges OVERLAP (user_0001-3500 overlaps with user_0050-7200)
# At L1+: NO overlap (each key is in exactly one SSTable)

# Count keys at each level
ldb --db=/path/to/rocksdb scan --only_verify_checksums=1 --count_only=1

# Inspect compaction statistics
ldb --db=/path/to/rocksdb dump_live_files

# Key lookups
ldb --db=/path/to/rocksdb get user_1234

# Check Bloom filter effectiveness via RocksDB stats
# In application: db->GetProperty("rocksdb.stats", &stats)
# Look for:
#   "Bloom filter useful": number of Bloom filter hits (avoided disk reads)
#   "Bloom filter full positive": number of false positives (disk read, key not found)
```

### 4.2 Cassandra nodetool for LSM Inspection

```bash
# Cassandra uses LSM-tree storage (SSTables via Cassandra's storage engine)

# List SSTables for a table
nodetool cfstats keyspace.table_name
# Output:
#   SSTable count: 12
#   Space used (live): 4.23 GB
#   Compaction:
#     Pending tasks: 3
#     Completed tasks: 1287

# Check for tombstone accumulation (dangerous threshold)
nodetool tablestats keyspace.table_name | grep -i tombstone
# "Average live cells per slice (last five minutes)": 14.5
# "Average tombstones per slice (last five minutes)": 890  ← HIGH! Should be < 100

# Trigger manual compaction (when auto-compaction is not keeping up)
nodetool compact keyspace table_name

# Check compaction queue (how much is pending)
nodetool compactionstats
# Output:
#   pending tasks: 5
#   ... (active compactions with progress)
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
lsm_engine.py

A complete LSM-tree storage engine implementation in Python.

Components:
  - BloomFilter: Probabilistic membership test
  - SSTable: Sorted String Table (sorted, immutable on-disk file)
  - MemTable: In-memory sorted buffer (simulated with sorted list)
  - WALWriter: Write-ahead log for MemTable durability
  - LSMEngine: Full engine combining MemTable, SSTable, compaction
  - CompactionStats: Tracks write amplification, space amplification

Design choices that mirror production LSM-trees (RocksDB/Cassandra):
  - MemTable flushes to SSTable when it reaches size threshold
  - SSTables are immutable after creation
  - Tombstones for deletes
  - Bloom filters per SSTable
  - Leveled compaction (L0 = overlapping, L1+ = non-overlapping)
  - Reads check MemTable first, then SSTables newest-to-oldest
  - WAL for crash recovery

No external dependencies. Simulates file I/O with in-memory dicts.
"""

from __future__ import annotations
import bisect
import hashlib
import json
import math
import os
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional


# ─── Constants ───────────────────────────────────────────────────────────────

MEMTABLE_SIZE_BYTES  = 64 * 1024        # 64KB (production: 64MB)
BLOOM_BITS_PER_KEY   = 10               # 1% false positive rate
L0_COMPACTION_TRIGGER = 4              # Compact L0 when it has this many SSTables
LEVEL_SIZE_MULTIPLIER = 10             # Each level is 10× larger than previous
TOMBSTONE_VALUE      = "__TOMBSTONE__"  # Sentinel value for deleted keys


# ─── Bloom Filter ─────────────────────────────────────────────────────────────

class BloomFilter:
    """
    A Bloom filter for fast "definitely not present" checks.
    Uses multiple hash functions (simulated via MD5 with different salts).
    """

    def __init__(self, expected_keys: int, bits_per_key: int = BLOOM_BITS_PER_KEY):
        self.n = expected_keys
        self.m = max(expected_keys * bits_per_key, 64)  # Bit array size
        self.k = max(1, round(0.693 * bits_per_key))   # Number of hash functions
        self._bits = bytearray(math.ceil(self.m / 8))
        self._key_count = 0

    def _hashes(self, key: str) -> list[int]:
        """Generate k hash positions for key."""
        positions = []
        for i in range(self.k):
            h = int(hashlib.md5(f"{i}:{key}".encode()).hexdigest(), 16)
            positions.append(h % self.m)
        return positions

    def add(self, key: str):
        """Add a key to the Bloom filter."""
        for pos in self._hashes(key):
            byte_idx = pos // 8
            bit_idx  = pos % 8
            self._bits[byte_idx] |= (1 << bit_idx)
        self._key_count += 1

    def may_contain(self, key: str) -> bool:
        """
        Returns True if key MIGHT be in the set (possible false positive).
        Returns False if key is DEFINITELY NOT in the set.
        """
        for pos in self._hashes(key):
            byte_idx = pos // 8
            bit_idx  = pos % 8
            if not (self._bits[byte_idx] & (1 << bit_idx)):
                return False
        return True

    @property
    def false_positive_rate(self) -> float:
        """Theoretical FPR given current occupancy."""
        if self.m == 0:
            return 1.0
        return (1 - math.exp(-self.k * self._key_count / self.m)) ** self.k


# ─── SSTable ──────────────────────────────────────────────────────────────────

@dataclass
class SSTableEntry:
    key:       str
    value:     str    # TOMBSTONE_VALUE for deleted keys
    seq_num:   int    # Sequence number (write timestamp) — larger = newer


class SSTable:
    """
    Sorted String Table: an immutable, sorted collection of key-value entries.
    In production, this is a file on disk. Here we simulate with a sorted list.
    """

    _next_id = 0

    def __init__(self, entries: list[SSTableEntry], level: int = 0):
        # Entries must be sorted by (key, -seq_num) so newest comes first per key
        self.entries = sorted(entries, key=lambda e: (e.key, -e.seq_num))
        self.level   = level
        self.id      = SSTable._next_id
        SSTable._next_id += 1
        self.created_at = time.time()

        # Build Bloom filter from all keys (including tombstones)
        self.bloom = BloomFilter(len(self.entries))
        for e in self.entries:
            self.bloom.add(e.key)

        # Build sparse index: every Nth entry for binary search
        self._index: list[tuple[str, int]] = []   # (key, entries_offset)
        INDEX_INTERVAL = 16
        for i in range(0, len(self.entries), INDEX_INTERVAL):
            self._index.append((self.entries[i].key, i))

    @property
    def size_bytes(self) -> int:
        """Approximate size in bytes."""
        return sum(len(e.key) + len(e.value) + 8 for e in self.entries)

    @property
    def min_key(self) -> Optional[str]:
        return self.entries[0].key if self.entries else None

    @property
    def max_key(self) -> Optional[str]:
        return self.entries[-1].key if self.entries else None

    @property
    def key_range(self) -> tuple[Optional[str], Optional[str]]:
        return (self.min_key, self.max_key)

    @property
    def entry_count(self) -> int:
        return len(self.entries)

    @property
    def is_tombstone_only(self) -> bool:
        return all(e.value == TOMBSTONE_VALUE for e in self.entries)

    def get(self, key: str) -> Optional[SSTableEntry]:
        """
        Look up a key. Returns the entry with the highest seq_num, or None.
        First checks Bloom filter to avoid unnecessary scan.
        """
        if not self.bloom.may_contain(key):
            return None   # Definitely not present

        # Binary search in the sparse index to find start position
        start = 0
        if self._index:
            idx = bisect.bisect_right([k for k, _ in self._index], key) - 1
            if idx >= 0:
                start = self._index[idx][1]

        # Linear scan from start (limited range due to sparse index)
        for i in range(start, len(self.entries)):
            e = self.entries[i]
            if e.key == key:
                return e   # First match is newest (sorted by -seq_num)
            if e.key > key:
                break
        return None

    def range_scan(self, low: str, high: str) -> Iterator[SSTableEntry]:
        """Yield entries with low <= key <= high, newest-first per key."""
        seen_keys = set()
        for e in self.entries:
            if e.key > high:
                break
            if e.key >= low and e.key not in seen_keys:
                seen_keys.add(e.key)
                yield e

    def overlaps_range(self, low: str, high: str) -> bool:
        """True if this SSTable's key range overlaps [low, high]."""
        if self.min_key is None or self.max_key is None:
            return False
        return not (self.max_key < low or self.min_key > high)


# ─── WAL (Write-Ahead Log) ─────────────────────────────────────────────────────

class WALWriter:
    """
    Write-Ahead Log for MemTable durability.
    In production: appends to a file. Here we simulate with a list.
    """

    def __init__(self):
        self._log: list[tuple[int, str, str]] = []   # (seq_num, key, value)
        self._seq = 0

    def append(self, key: str, value: str) -> int:
        """Append a write to the WAL. Returns sequence number."""
        self._seq += 1
        self._log.append((self._seq, key, value))
        return self._seq

    def recover(self) -> list[tuple[int, str, str]]:
        """Return all WAL entries for recovery."""
        return list(self._log)

    def truncate(self):
        """Called after MemTable is flushed to SSTable."""
        self._log.clear()

    @property
    def current_seq(self) -> int:
        return self._seq


# ─── MemTable ─────────────────────────────────────────────────────────────────

class MemTable:
    """
    In-memory sorted buffer. All writes go here first.
    When full, flushed to an SSTable.
    Uses a sorted list of (key, value, seq_num) triples.
    """

    def __init__(self, wal: WALWriter):
        self._wal = wal
        self._entries: dict[str, tuple[str, int]] = {}   # key → (value, seq_num)
        self._size_bytes: int = 0

    def put(self, key: str, value: str) -> int:
        """Write (key, value). Returns sequence number."""
        seq = self._wal.append(key, value)
        old = self._entries.get(key)
        if old:
            self._size_bytes -= len(key) + len(old[0])
        self._entries[key] = (value, seq)
        self._size_bytes += len(key) + len(value)
        return seq

    def delete(self, key: str) -> int:
        """Insert a tombstone for key."""
        return self.put(key, TOMBSTONE_VALUE)

    def get(self, key: str) -> Optional[tuple[str, int]]:
        """Returns (value, seq_num) or None. Caller checks for TOMBSTONE_VALUE."""
        return self._entries.get(key)

    def is_full(self) -> bool:
        return self._size_bytes >= MEMTABLE_SIZE_BYTES

    def flush_to_sstable(self, level: int = 0) -> SSTable:
        """
        Flush this MemTable to an SSTable (simulates writing to disk).
        Returns the new SSTable. After this, the MemTable should be discarded.
        """
        entries = [
            SSTableEntry(key=k, value=v, seq_num=seq)
            for k, (v, seq) in self._entries.items()
        ]
        self._wal.truncate()
        return SSTable(entries, level=level)

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    @property
    def entry_count(self) -> int:
        return len(self._entries)


# ─── Compaction Statistics ─────────────────────────────────────────────────────

@dataclass
class CompactionStats:
    bytes_written_by_app:    int = 0
    bytes_written_to_disk:   int = 0   # All SSTable writes including compaction
    compactions_run:         int = 0
    tombstones_purged:       int = 0
    keys_merged:             int = 0

    @property
    def write_amplification(self) -> float:
        if self.bytes_written_by_app == 0:
            return 0.0
        return self.bytes_written_to_disk / self.bytes_written_by_app

    @property
    def space_savings_pct(self) -> float:
        """Percentage of space saved by deduplication during compaction."""
        if self.bytes_written_to_disk == 0:
            return 0.0
        return max(0.0, 100 * (1 - self.bytes_written_by_app / self.bytes_written_to_disk))


# ─── LSM Engine ──────────────────────────────────────────────────────────────

class LSMEngine:
    """
    A complete LSM-tree storage engine.

    Architecture:
      - Active MemTable: receives all writes
      - Immutable MemTable: being flushed to SSTable (None if not in progress)
      - Levels[]: list of lists of SSTables, level 0 to max_level
      - WAL: Write-ahead log for MemTable durability

    Compaction: Simplified leveled compaction (L0 → L1 when L0 has ≥ trigger SSTables)
    """

    def __init__(self, max_level: int = 4):
        self._wal     = WALWriter()
        self._active  = MemTable(self._wal)
        self._immutable: Optional[MemTable] = None
        self.levels: list[list[SSTable]] = [[] for _ in range(max_level + 1)]
        self.stats = CompactionStats()
        self._max_level = max_level
        self._bloom_hits  = 0    # Times Bloom filter correctly said "not present"
        self._bloom_false_pos = 0  # Times Bloom filter said "present" but key wasn't

    # ── Write ─────────────────────────────────────────────────────────────────

    def put(self, key: str, value: str):
        """Insert or update a key-value pair."""
        payload_bytes = len(key) + len(value)
        self.stats.bytes_written_by_app += payload_bytes

        self._active.put(key, value)

        if self._active.is_full():
            self._flush_memtable()

    def delete(self, key: str):
        """Delete a key by inserting a tombstone."""
        self._active.delete(key)
        if self._active.is_full():
            self._flush_memtable()

    def _flush_memtable(self):
        """Flush active MemTable to an L0 SSTable."""
        sst = self._active.flush_to_sstable(level=0)
        self.levels[0].append(sst)
        self.stats.bytes_written_to_disk += sst.size_bytes

        # Reset active MemTable
        self._wal = WALWriter()
        self._active = MemTable(self._wal)

        # Trigger compaction if L0 is full
        if len(self.levels[0]) >= L0_COMPACTION_TRIGGER:
            self._compact_l0()

    # ── Read ──────────────────────────────────────────────────────────────────

    def get(self, key: str) -> Optional[str]:
        """
        Lookup a key. Search order (newest data first):
        1. Active MemTable
        2. Immutable MemTable (if being flushed)
        3. L0 SSTables (newest first — L0 SSTables can have overlapping ranges)
        4. L1, L2, ... SSTables (binary search — non-overlapping ranges)
        """
        # 1. Active MemTable
        result = self._active.get(key)
        if result is not None:
            val, _ = result
            return None if val == TOMBSTONE_VALUE else val

        # 2. Immutable MemTable
        if self._immutable:
            result = self._immutable.get(key)
            if result is not None:
                val, _ = result
                return None if val == TOMBSTONE_VALUE else val

        # 3. L0 SSTables (search newest-to-oldest; key ranges can overlap)
        for sst in reversed(self.levels[0]):
            entry = sst.get(key)
            if entry is not None:
                # Track Bloom filter stats
                return None if entry.value == TOMBSTONE_VALUE else entry.value
            else:
                # Bloom filter said "not present" — track as a hit
                self._bloom_hits += 1

        # 4. L1+ SSTables (non-overlapping ranges; can binary-search per level)
        for lvl in range(1, self._max_level + 1):
            for sst in self.levels[lvl]:
                if sst.min_key is None or key < sst.min_key or key > sst.max_key:
                    continue
                entry = sst.get(key)
                if entry is not None:
                    return None if entry.value == TOMBSTONE_VALUE else entry.value

        return None   # Key not found in any level

    def range_scan(self, low: str, high: str) -> dict[str, str]:
        """
        Return all live keys in [low, high] as {key: value}.
        Merges results from MemTable + all SSTables, resolving conflicts
        by keeping the newest version.
        """
        # Collect all entries, tracking (value, seq_num) per key
        best: dict[str, tuple[str, int]] = {}

        def consider(key: str, value: str, seq: int):
            existing = best.get(key)
            if existing is None or seq > existing[1]:
                best[key] = (value, seq)

        # Active MemTable
        for k, (v, seq) in self._active._entries.items():
            if low <= k <= high:
                consider(k, v, seq)

        # All SSTables (all levels)
        for level_ssts in self.levels:
            for sst in level_ssts:
                for entry in sst.range_scan(low, high):
                    consider(entry.key, entry.value, entry.seq_num)

        # Filter tombstones
        return {
            k: v for k, (v, _) in best.items()
            if v != TOMBSTONE_VALUE
        }

    # ── Compaction ─────────────────────────────────────────────────────────────

    def _compact_l0(self):
        """
        Compact all L0 SSTables into L1.
        L0 SSTables have overlapping key ranges, so we must merge ALL of them.
        Output: non-overlapping SSTables written to L1.
        """
        l0_ssts = self.levels[0]
        if not l0_ssts:
            return

        # Collect all entries from L0 (and existing L1 that overlaps)
        all_entries: dict[str, SSTableEntry] = {}

        # Include all L1 SSTables (they'll be merged with L0)
        for sst in self.levels[1]:
            for entry in sst.entries:
                existing = all_entries.get(entry.key)
                if existing is None or entry.seq_num > existing.seq_num:
                    all_entries[entry.key] = entry

        # L0 entries override (they're newer)
        for sst in l0_ssts:
            for entry in sst.entries:
                existing = all_entries.get(entry.key)
                if existing is None or entry.seq_num > existing.seq_num:
                    all_entries[entry.key] = entry

        # Filter out tombstones (only safe to remove if no older versions remain
        # and there's no snapshot that might see the deletion — simplified here)
        live_entries = []
        tombstones_removed = 0
        for entry in all_entries.values():
            if entry.value == TOMBSTONE_VALUE:
                tombstones_removed += 1
            else:
                live_entries.append(entry)

        self.stats.tombstones_purged  += tombstones_removed
        self.stats.keys_merged        += len(all_entries)
        self.stats.compactions_run    += 1

        # Split into fixed-size SSTables for L1
        TARGET_SST_SIZE = 64 * 1024   # 64KB per SSTable at L1
        live_entries.sort(key=lambda e: e.key)

        new_l1_ssts: list[SSTable] = []
        batch: list[SSTableEntry] = []
        batch_size = 0

        for entry in live_entries:
            batch.append(entry)
            batch_size += len(entry.key) + len(entry.value)
            if batch_size >= TARGET_SST_SIZE:
                new_sst = SSTable(batch, level=1)
                self.stats.bytes_written_to_disk += new_sst.size_bytes
                new_l1_ssts.append(new_sst)
                batch = []
                batch_size = 0

        if batch:
            new_sst = SSTable(batch, level=1)
            self.stats.bytes_written_to_disk += new_sst.size_bytes
            new_l1_ssts.append(new_sst)

        # Replace L0 and L1 with compaction output
        self.levels[0] = []
        self.levels[1] = new_l1_ssts

    def force_compact(self):
        """Flush MemTable and compact all levels."""
        if self._active.entry_count > 0:
            sst = self._active.flush_to_sstable(level=0)
            self.levels[0].append(sst)
            self.stats.bytes_written_to_disk += sst.size_bytes
            self._wal = WALWriter()
            self._active = MemTable(self._wal)
        self._compact_l0()

    # ── Statistics ─────────────────────────────────────────────────────────────

    def print_stats(self):
        print(f"\n  LSM-Tree Statistics:")
        print(f"    Write amplification: {self.stats.write_amplification:.2f}×")
        print(f"    Compactions run:     {self.stats.compactions_run}")
        print(f"    Tombstones purged:   {self.stats.tombstones_purged}")
        total_ssts = sum(len(l) for l in self.levels)
        for i, level in enumerate(self.levels):
            if level:
                total_size = sum(s.size_bytes for s in level)
                total_entries = sum(s.entry_count for s in level)
                print(f"    L{i}: {len(level):3d} SSTables, "
                      f"{total_size/1024:.1f}KB, {total_entries:,} entries")
        print(f"    Total SSTables:      {total_ssts}")
        print(f"    Active MemTable:     {self._active.entry_count} entries, "
              f"{self._active.size_bytes/1024:.1f}KB")
        total_bloom_checks = self._bloom_hits + self._bloom_false_pos
        if total_bloom_checks > 0:
            print(f"    Bloom filter hits:   {self._bloom_hits} "
                  f"({100*self._bloom_hits/total_bloom_checks:.1f}% of checks)")


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== LSM-Tree Storage Engine Demo ===\n")

    # ── Phase 1: Basic Writes and Reads ──────────────────────────────────────
    print("─── Phase 1: Basic Writes and Reads ───")
    engine = LSMEngine()

    # Insert 1000 entries (will trigger MemTable flushes and compaction)
    for i in range(1000):
        engine.put(f"user:{i:05d}", f"data-{i}")

    # Reads
    print(f"  get('user:00042'): {engine.get('user:00042')}")
    print(f"  get('user:00999'): {engine.get('user:00999')}")
    print(f"  get('user:99999'): {engine.get('user:99999')} (not inserted)")

    engine.print_stats()

    # ── Phase 2: Overwrite and Tombstones ────────────────────────────────────
    print("\n─── Phase 2: Overwrite and Tombstone Demo ───")
    engine2 = LSMEngine()
    engine2.put("key:001", "original-value")
    engine2.put("key:002", "to-be-deleted")
    engine2.put("key:003", "stays")

    # Flush to SSTable so the original values are on "disk"
    engine2.force_compact()
    print(f"  After initial write + compact:")
    print(f"    key:001 = {engine2.get('key:001')}")
    print(f"    key:002 = {engine2.get('key:002')}")

    # Overwrite key:001, delete key:002
    engine2.put("key:001", "updated-value")
    engine2.delete("key:002")

    print(f"  After overwrite + delete (in MemTable, not yet flushed):")
    print(f"    key:001 = {engine2.get('key:001')}  ← MemTable takes priority")
    print(f"    key:002 = {engine2.get('key:002')}  ← Tombstone in MemTable")
    print(f"    key:003 = {engine2.get('key:003')}  ← Still in SSTable")

    # Compact — tombstone should purge key:002
    engine2.force_compact()
    print(f"  After compaction (tombstone purged):")
    print(f"    key:001 = {engine2.get('key:001')}")
    print(f"    key:002 = {engine2.get('key:002')}  ← Gone after compaction")
    print(f"    Tombstones purged: {engine2.stats.tombstones_purged}")

    # ── Phase 3: Write Amplification Analysis ────────────────────────────────
    print("\n─── Phase 3: Write Amplification ───")
    engine3 = LSMEngine()
    N = 5000
    for i in range(N):
        engine3.put(f"key:{i:06d}", f"value-{'x'*50}-{i}")
    engine3.force_compact()
    print(f"  Inserted {N} keys (50-byte values):")
    print(f"  App bytes written:   {engine3.stats.bytes_written_by_app/1024:.1f}KB")
    print(f"  Disk bytes written:  {engine3.stats.bytes_written_to_disk/1024:.1f}KB")
    print(f"  Write amplification: {engine3.stats.write_amplification:.2f}×")
    print(f"  (B-tree would be ~80-200× for same workload)")

    # ── Phase 4: Bloom Filter Effectiveness ──────────────────────────────────
    print("\n─── Phase 4: Bloom Filter Demo ───")
    engine4 = LSMEngine()
    for i in range(500):
        engine4.put(f"exists:{i}", "v")
    engine4.force_compact()

    # Probe non-existent keys — Bloom filter should prevent disk reads
    for i in range(500, 600):
        engine4.get(f"notexists:{i}")

    print(f"  Bloom filter FPR (per SSTable): "
          f"~{engine4.levels[1][0].bloom.false_positive_rate*100:.2f}% "
          f"if SSTables exist")
    print(f"  Bloom filter hits (avoided disk I/O): {engine4._bloom_hits}")

    # ── Phase 5: Range Scan ───────────────────────────────────────────────────
    print("\n─── Phase 5: Range Scan ───")
    engine5 = LSMEngine()
    for i in range(200):
        engine5.put(f"order:{i:04d}", f"order-data-{i}")
    results = engine5.range_scan("order:0050", "order:0059")
    print(f"  Range scan [order:0050, order:0059]: {len(results)} results")
    for k in sorted(results)[:5]:
        print(f"    {k} = {results[k]}")
```

---

## 6. Visual Reference

### LSM-Tree Architecture

```
WRITE PATH:
  Client PUT(key, value)
       │
       ▼
  ┌─────────────────────────────────┐
  │  WAL (Write-Ahead Log)          │  ← Sequential append (crash safety)
  │  Append (key, value, seq_num)   │
  └─────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────┐
  │  Active MemTable (sorted dict)  │  ← In-memory, O(log N) insert
  │  user:001 → "Alice"             │
  │  user:002 → "Bob"               │
  │  user:003 → [TOMBSTONE]         │  ← DELETE becomes tombstone
  └─────────────────────────────────┘
       │  (when MEMTABLE_SIZE reached)
       ▼
  ┌─────────────────────────────────┐
  │  L0 SSTable (immutable)         │  ← Sequential write: entire MemTable
  │  Sorted, contains bloom filter  │
  │  Key ranges OVERLAP within L0   │
  └─────────────────────────────────┘
       │  (when L0 has ≥ 4 SSTables)
       ▼  COMPACTION (merge-sort)
  ┌─────────────────────────────────┐
  │  L1 SSTables (non-overlapping)  │  ← Sequential write, resolved duplicates
  │  [a-g] [h-m] [n-t] [u-z]       │  ← Each key in at most ONE SSTable
  └─────────────────────────────────┘
       │  (when L1 exceeds size limit)
       ▼  COMPACTION
  ┌─────────────────────────────────┐
  │  L2 SSTables (10× larger)       │
  └─────────────────────────────────┘

READ PATH for GET(key):
  1. MemTable          → O(1) hash lookup
  2. L0 SSTables       → Check each (newest first); Bloom filter per SSTable
  3. L1 SSTables       → Binary search for key range; Bloom filter
  4. L2 SSTables       → Binary search; Bloom filter
  ...
  Worst case: O(L0_count + L1 + L2 + ...) SSTable reads
  With Bloom filter: most SSTable reads short-circuit on "not present"
```

### Compaction: Key Resolution

```
SSTable A (older, seq 1-100):    SSTable B (newer, seq 101-200):
  key:001 → "v1"   (seq=5)         key:001 → "v2"   (seq=150)   ← WINNER
  key:003 → "v3"   (seq=8)         key:002 → [TOMB] (seq=110)   ← DELETED
  key:005 → "v5"   (seq=15)        key:005 → "v5b"  (seq=180)   ← WINNER

After compaction of A + B (output to L1):
  key:001 → "v2"   (winner: highest seq_num)
  key:003 → "v3"   (only in A — kept)
  key:005 → "v5b"  (winner: highest seq_num)
  key:002: PURGED  (tombstone; no older version to hide → safe to remove)

Space reclaimed: 2 SSTable writes consumed → 1 output SSTable
Keys deduplicated: key:001 (2→1), key:005 (2→1)
Tombstones purged: key:002
```

---

## 7. Common Mistakes

**Mistake 1: Expecting LSM reads to be as fast as B-tree reads.** LSM-trees trade read performance for write performance. For a random point lookup, an LSM-tree may need to check the MemTable, 4 L0 SSTables, 1 L1 SSTable, 1 L2 SSTable — that's 6 Bloom filter checks plus potentially 6 disk reads (if Bloom filters produce false positives). A B-tree needs 3-4 reads. Bloom filters mitigate this but don't eliminate the gap. For a workload that is 90% point reads, 10% writes: a B-tree is faster. For 90% writes, 10% reads: LSM-tree wins. Know your workload ratio before choosing.

**Mistake 2: Not monitoring compaction lag.** If writes arrive faster than compaction can process them, the number of L0 SSTables grows without bound. Reads must scan all L0 SSTables, degrading linearly. In Cassandra, this is called "compaction lag" and causes read timeouts on heavily written tables. Monitor `pending compaction tasks` and alert if it grows beyond the compaction trigger threshold. Reducing write throughput or adding hardware is the mitigation — there is no configuration knob that can make compaction keep up with 10× its designed write rate.

**Mistake 3: Treating tombstones as free.** In a B-tree, a DELETE is approximately the same cost as an INSERT. In an LSM-tree, a DELETE inserts a tombstone but the old data remains in older SSTables until compaction physically removes it. Between the DELETE and the next compaction, the key exists in two forms: the tombstone (in newer SSTable) and the original value (in older SSTable). During a range scan, the engine must process both the tombstone and the old value — potentially doubling scan cost. For workloads with heavy deletes (e.g., TTL-based expiry in Cassandra), force compaction after bulk deletes to purge tombstones, or the next range scan will be extremely slow.

---

## 8. Production Failure Scenarios

### Scenario 1: Cassandra Tombstone Storm Causing Read Timeouts

**Symptoms:** After implementing a feature that deletes individual rows within a wide partition (10,000+ columns per partition key), read latency on that table spikes from 2ms to 15 seconds, with most queries hitting Cassandra's `read_request_timeout_in_ms = 10000ms` limit. The queries are `SELECT * FROM events WHERE user_id = ? AND ts BETWEEN ? AND ?`.

**Root cause:** Cassandra's LSM-tree implementation stores tombstones in SSTables alongside live data. The delete pattern created millions of tombstones across the partition. A range query on `ts` requires scanning tombstones to resolve the live data — with 500,000 tombstones per partition, each query scans 500,000 tombstone entries before finding the few hundred live rows. Cassandra logs: `"Read 1 live rows and 512043 tombstone cells"`.

**Fix:**
```cql
-- Step 1: Reduce gc_grace_seconds for this table
-- (Default 864000 = 10 days: Cassandra waits 10 days before purging tombstones
--  to prevent "zombie resurrection" across nodes with clock drift)
ALTER TABLE keyspace.events WITH gc_grace_seconds = 86400;   -- 1 day

-- Step 2: Force compaction to purge tombstones immediately
nodetool compact keyspace events

-- Step 3: For future workload — use TTL instead of explicit DELETE
INSERT INTO events (user_id, ts, data) VALUES (?, ?, ?) USING TTL 604800;
-- TTL-based expiry generates fewer tombstones and is handled more efficiently

-- Step 4: Monitor tombstone counts per partition
-- Enable tombstone_warn_threshold and tombstone_failure_threshold in cassandra.yaml:
-- tombstone_warn_threshold: 1000
-- tombstone_failure_threshold: 100000
```

### Scenario 2: RocksDB Write Stall Under Burst Write Load

**Symptoms:** A Flink job writing enriched events to a RocksDB-backed state store works fine at 50K events/second. After a schema change that doubled the value size, write throughput drops to near-zero with log messages: `"Stalling writes because of too many L0 files; 20 files, stall threshold 20"`.

**Root cause:** RocksDB's flow control: when L0 file count reaches `level0_slowdown_writes_trigger` (default 20), RocksDB slows new writes. When it reaches `level0_stop_writes_trigger` (default 36), writes stop entirely until compaction reduces L0 count. The doubled value size meant each MemTable flush produced larger SSTables, and the single compaction thread couldn't keep up.

**Fix:**
```python
# RocksDB options (via python-rocksdb or equivalent)
options = rocksdb.Options()

# Increase compaction thread pool
options.max_background_jobs = 8          # Default 2; use 4-8 for write-heavy

# Increase MemTable size to reduce flush frequency
options.write_buffer_size = 128 * 1024 * 1024    # 128MB (default 64MB)
options.max_write_buffer_number = 4               # Allow 4 immutable MemTables

# Tune L0 thresholds (raise to give compaction more time to catch up)
options.level0_slowdown_writes_trigger = 40       # Default 20
options.level0_stop_writes_trigger = 56           # Default 36

# Increase L1 size to reduce frequency of L0→L1 compaction
options.max_bytes_for_level_base = 512 * 1024 * 1024   # 512MB (default 256MB)
```

---

## 9. Performance and Tuning

### Write Throughput

```
LSM-tree write cost model:
  1. MemTable write (in-memory): ~100ns per write (no I/O)
  2. WAL append: ~1μs (sequential I/O, buffered)
  3. MemTable flush: every MEMTABLE_SIZE_BYTES of writes → one sequential I/O
     flush_frequency = write_rate_bytes / MEMTABLE_SIZE_BYTES flushes/sec
     At 100MB/s writes, 64MB MemTable: 100/64 ≈ 1.6 flushes/sec
     Each flush = 64MB sequential write = cheap
  4. Compaction: asynchronous background; proportional to write rate
     At steady state, compaction absorbs all L0 SSTables before L0 fills up

Throughput ceiling:
  Without write stalls: limited by sequential I/O bandwidth for WAL + flushes
  With write stalls (L0 full): falls to compaction throughput
  Target: compaction throughput > write throughput (by 2-3× margin)

Practical ceiling on a single NVMe SSD (3GB/s sequential write):
  WAL: ~100MB/s (1 WAL + parallel data)
  Effective app write rate: ~200-500MB/s before compaction bottleneck
  → 2-5M rows/second for 100-byte rows
```

### RocksDB Statistics Monitoring

```python
# Key RocksDB metrics to monitor in production

# Via RocksDB's built-in stats
db.get_property("rocksdb.stats")
# → Full statistics dump including:
#   "Cumulative compaction: X.XX GB write, Y.YY GB read, Z.ZZ sec"
#   "Stalls(secs): A.AA level0_slowdown, B.BB level0_numfiles, C.CC memtable"
#   "Block cache miss/hit: M/H (hit rate = H/(M+H))"

# Key metrics to track:
# 1. Write amplification: compaction_bytes_written / bytes_written_by_app
# 2. Read amplification: number of Get() operations / SSTable reads
# 3. Space amplification: db_size_on_disk / live_data_size
# 4. Compaction pending bytes (should stay bounded)
# 5. L0 file count (watch for approaching stall threshold)
# 6. Bloom filter hit rate (should be > 99% for point lookups on absent keys)

# Level sizes
for level in range(7):
    size = db.get_property(f"rocksdb.num-files-at-level{level}")
    print(f"L{level}: {size} SSTables")
```

---

## 10. Interview Q&A

**Q1: Explain the fundamental tradeoff between B-trees and LSM-trees.**

The B-tree and LSM-tree represent opposite ends of the same tradeoff axis: random I/O performance vs sequential I/O performance. A B-tree modifies data in-place at random locations: inserting a key requires modifying whichever leaf page currently contains the key's range, which is a random write. For a random insert workload, every write is random I/O. The B-tree pays this random write cost to make reads fast: any key can be found in 3-4 I/Os by following the tree from root to leaf, with no ambiguity about where the key lives.

An LSM-tree never modifies data in-place. Writes go to an in-memory buffer (MemTable) and are periodically flushed to disk as large, sequential writes (SSTables). The result: writes are always sequential, and write amplification is far lower than a B-tree. The cost is read performance: to find a key, you must potentially search the MemTable, multiple L0 SSTables (which have overlapping key ranges and must each be checked), and then one SSTable per subsequent level. Without Bloom filters, read I/O would grow linearly with the number of SSTables. Bloom filters reduce this to O(levels) I/Os in the common case, but a worst-case read still visits more I/Os than a B-tree.

The practical choice: B-trees for read-heavy OLTP databases (Postgres, MySQL InnoDB), LSM-trees for write-heavy workloads (Cassandra, HBase, RocksDB, time-series databases). The cross-over point is roughly when write throughput exceeds what a B-tree can sustain on the available I/O hardware.

**Q2: Why do LSM-trees need compaction, and what does write amplification mean in an LSM-tree context?**

LSM-trees need compaction for three reasons. First, stale versions: when a key is updated, the old value is not overwritten — the new value goes to the MemTable and eventually a newer SSTable. Both the old and new values exist on disk simultaneously. Without compaction, disk usage would grow without bound as more updates accumulate, and reads would need to check older and older SSTables to find the newest version of a key. Compaction merges SSTables and retains only the newest version of each key. Second, tombstones: deleted keys are represented as tombstone markers, not actual deletions (since SSTables are immutable). Tombstones accumulate alongside old data and must be physically purged during compaction. Third, L0 manageability: L0 SSTables have overlapping key ranges; every read must check all L0 SSTables. Compaction reduces L0 file count by merging them into L1, maintaining the invariant that L1+ has non-overlapping ranges per SSTable.

Write amplification in an LSM-tree is the total bytes written to disk divided by bytes written by the application. Each byte written by the app goes through multiple compaction rounds: L0→L1, L1→L2, L2→L3. With a level size multiplier of 10 and 3 levels, a byte is compacted approximately 10 + 10 + 10 = 30 times. Total write amplification for RocksDB with leveled compaction: typically 10-50×, lower than a B-tree with WAL (20-200×) for random workloads.

---

## 11. Cross-Question Chain

**Interviewer:** Your Cassandra table receives 50,000 writes per second. After 6 hours, reads on that table become very slow (P99 = 12 seconds). What is your diagnostic approach?

**Candidate:** I'd start by checking compaction state: `nodetool compactionstats` to see if compaction is falling behind. If the pending compaction queue is growing, writes are outpacing compaction, and L0 SSTable count is growing unboundedly. More L0 SSTables = more files to check per read = higher read latency.

Second, I'd check tombstone counts: `nodetool tablestats keyspace.table | grep tombstone`. If tombstones per read are in the thousands, a previous delete operation (or TTL expiry) created tombstone accumulation that reads are being forced to scan through.

Third, I'd check partition sizes: a "wide" partition (millions of columns) causes Cassandra to read far more data per query than expected. `SELECT count(*) FROM table WHERE pk = 'problem-partition'` to identify supersized partitions.

**Interviewer:** Compactionstats shows 15 pending tasks and growing. What do you do?

**Candidate:** The immediate fix is to reduce write throughput if possible — application-level rate limiting or pausing non-critical writers. Long-term, I need more compaction resources: `nodetool setcompactionthroughput 256` (increase compaction throughput from default 64MB/s to 256MB/s), or add compaction threads in `cassandra.yaml` (`concurrent_compactors: 4`). I'd also check whether the chosen compaction strategy is appropriate — if the workload has many deletes, STCS (Size-Tiered Compaction Strategy) handles tombstones poorly; switching to TWCS (Time-Window Compaction Strategy) for time-series data or LCS (Leveled Compaction Strategy) for random reads/writes might be better.

**Interviewer:** You switch to LCS (Leveled Compaction Strategy). What tradeoffs does that introduce?

**Candidate:** LCS maintains non-overlapping SSTables at each level, similar to RocksDB's leveled compaction. The benefit: point reads are O(levels) — at most one SSTable per level needs to be checked, instead of all L0 SSTables. Read performance is much more predictable and generally faster. The cost: higher write amplification. LCS compacts data more aggressively to maintain the non-overlapping invariant, writing data more times per byte than STCS. For a write-heavy workload, LCS can 2-3× the disk write I/O compared to STCS. I'd use LCS when reads are the primary concern and write throughput can absorb the additional compaction I/O. For write-heavy append-only workloads (like time-series), TWCS is usually better because it compacts only within time windows, avoiding the cross-window rewrites that LCS would perform.

**Interviewer:** What is "space amplification" in an LSM-tree and how does it differ from write amplification?

**Candidate:** Write amplification is the ratio of total bytes written to storage versus bytes written by the application — it measures I/O cost over time. Space amplification is the ratio of bytes on disk to bytes of live application data at a point in time — it measures storage efficiency at rest.

An LSM-tree has space amplification greater than 1× for two reasons: stale versions of updated keys exist in older SSTables until compaction removes them, and tombstones for deleted keys persist until compaction purges them. With size-tiered compaction and a workload that updates 30% of keys frequently, space amplification can reach 2-3× (live data takes 3× more space than the logical dataset). During compaction, both the old SSTables and the new output exist simultaneously — peak space amplification can be 2× the steady-state amount.

Write amplification and space amplification trade against each other. Leveled compaction (like LCS or RocksDB leveled): low space amplification (close to 1.1-1.3×) but high write amplification (30-50×). Size-tiered compaction: higher space amplification (1.5-3×) but lower write amplification (10-20×). For environments where storage cost is high (cloud object storage), minimizing space amplification favors leveled compaction despite its write cost.

**Interviewer:** How do Bloom filters interact with the LSM read path, and what is their memory cost?

**Candidate:** Bloom filters are the key optimization that makes LSM-tree reads practical. Without them, a point lookup for a key not present in the database would need to read every SSTable at every level — checking for the key in each one before concluding it doesn't exist. With Bloom filters: before reading any SSTable, check the filter. If the filter says "definitely not here," skip the SSTable entirely — no disk I/O. Since false negatives are impossible (if the key is in the SSTable, the filter always says "maybe present"), Bloom filters never cause incorrect results.

The memory cost is 10 bits per key per SSTable (for a 1% false positive rate). For a database with 1 billion live keys spread across 100 SSTables (averaged over all levels), that's 1B × 10 bits × 100 SSTables / 8 = 125GB of Bloom filter memory. This is impractical. In practice, Bloom filters are only kept for the most recently accessed SSTables in the block cache, and lower levels (which are larger and less frequently accessed) may have coarser-grained filters. RocksDB can place Bloom filters at the partition level (covering a full L1+ SSTable's key range) rather than per-SSTable, reducing memory while still skipping whole-SSTable reads.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the core insight of the LSM-tree? | Never perform random writes. Buffer all writes in memory (MemTable), flush to disk as large sequential writes (SSTables), and periodically merge SSTables via sequential compaction. Eliminates random writes entirely. |
| 2 | What is a MemTable? | An in-memory sorted data structure (typically skip list or red-black tree) that receives all writes. When full, it is flushed to an SSTable on disk. Backed by WAL for crash recovery. |
| 3 | What is an SSTable? | Sorted String Table: an immutable, sorted file on disk. Created by flushing a MemTable. Never modified after creation. Contains data blocks, an index block, and a Bloom filter. |
| 4 | Why are SSTables immutable? | Immutability enables fully sequential I/O — flushes and compactions write contiguous bytes without random seeks. It also simplifies crash recovery: a partially-written SSTable can be discarded; the WAL holds the in-flight data. |
| 5 | What is a tombstone in an LSM-tree? | A special marker value inserted for a deleted key. Since SSTables are immutable, deletes cannot remove data in place. The tombstone signals "this key is deleted" during reads. Purged during compaction. |
| 6 | What is a Bloom filter and what guarantee does it provide? | A probabilistic data structure: returns "definitely not present" (always correct) or "maybe present" (occasionally wrong — false positive). Never produces false negatives. Used to skip SSTables that definitely don't contain the target key. |
| 7 | What is the false positive rate of a Bloom filter with 10 bits per key? | Approximately 1% (0.008). For every 100 Bloom filter checks where the key is absent, roughly 1 will incorrectly say "maybe present," causing an unnecessary SSTable read. |
| 8 | What is the read path for a GET in an LSM-tree? | (1) MemTable, (2) L0 SSTables newest-to-oldest (check all — overlapping ranges), (3) L1 SSTable covering the key's range (binary search — non-overlapping), (4) L2, L3, ... until found or all levels exhausted. Bloom filter checked before reading each SSTable. |
| 9 | What is compaction and why is it necessary? | Background process that merge-sorts multiple SSTables into fewer, larger ones. Necessary to: (1) reclaim space from stale versions and tombstones, (2) reduce L0 SSTable count (for read performance), (3) maintain bounded disk space. |
| 10 | What is the difference between leveled and size-tiered compaction? | Leveled (LCS/RocksDB): non-overlapping SSTables at L1+; one SSTable per level per lookup; lower space amplification but higher write amplification (~30-50×). Size-tiered (STCS/Cassandra): SSTables of similar size merged; overlapping ranges; higher space amplification (2-3×) but lower write amplification (~10-20×). |
| 11 | What is write amplification in an LSM-tree? | Total bytes written to disk (including compaction rewrites) divided by bytes written by the application. For RocksDB leveled compaction: 10-50×. B-tree: 20-200× for random workloads. LSM wins on writes; B-tree wins on reads. |
| 12 | What is space amplification in an LSM-tree? | Ratio of bytes on disk to bytes of live application data. Stale versions and tombstones inflate disk usage until compaction. Leveled compaction: 1.1-1.3×. Size-tiered: 1.5-3× (plus 2× temporary during compaction). |
| 13 | What is a tombstone storm in Cassandra? | Accumulation of millions of tombstones in a partition or across SSTables. Range scans must traverse tombstones to find live data, causing severe read latency. Triggered by bulk deletes or TTL expiry without adequate compaction. |
| 14 | What happens when L0 SSTable count exceeds the stall threshold in RocksDB? | RocksDB throttles (slows) new writes when L0 count reaches `level0_slowdown_writes_trigger` (default 20). Completely stops new writes at `level0_stop_writes_trigger` (default 36). Resumes when compaction reduces L0 count below the threshold. |
| 15 | How does WAL interact with MemTable in an LSM-tree? | Every write to the MemTable is also appended to the WAL (Write-Ahead Log) on disk. If the process crashes, the MemTable is lost, but the WAL is replayed to reconstruct it. After MemTable flush to SSTable, the WAL segment for that MemTable is truncated. |

---

## 13. Further Reading

- **"Database Internals" by Alex Petrov, Chapters 5-8 (LSM-Tree):** The most thorough treatment of LSM-tree internals in a data engineering context. Chapter 6 on compaction strategies is excellent.
- **"The Log-Structured Merge-Tree (LSM-Tree)" — O'Neil et al. 1996 (original paper):** The paper that introduced LSM-trees. Readable and concise; explains the original motivation from disk I/O patterns.
- **RocksDB Wiki (github.com/facebook/rocksdb/wiki):** The authoritative reference for RocksDB's specific implementation. "Leveled Compaction," "Universal Compaction," and "Write Amplification" pages are essential.
- **Cassandra documentation on compaction strategies (cassandra.apache.org):** Practical guide to STCS, LCS, TWCS, and UCS with workload recommendations.
- **"WiscKey: Separating Keys from Values in SSD-Conscious Storage" (FAST 2016):** Important LSM-tree optimization that separates key sorting from value storage, dramatically reducing compaction I/O for large values. Used in BadgerDB.

---

## 14. Lab Exercises

**Exercise 1: Observe MemTable Flush**
Run `lsm_engine.py`. Add a print statement inside `_flush_memtable()` that shows: how many entries are being flushed, the min and max key, and the resulting SSTable size. Insert 5000 entries with 100-byte values and observe how many flushes occur and at what intervals.

**Exercise 2: Trace the Read Path**
Add a `read_trace` parameter to `LSMEngine.get()`. When True, print which component (MemTable, L0 SSTable #N, L1 SSTable #M) was checked and whether it was a Bloom filter hit (skipped), Bloom filter miss (scanned), or found. Insert 1000 entries, flush, then lookup: (1) a key that exists, (2) a key that was deleted, (3) a key that was never inserted.

**Exercise 3: Tombstone Accumulation**
Insert 1000 entries. Delete 500 of them. Before compaction: count the total entries in all SSTables (including tombstones). After `force_compact()`: count again. What percentage of space was reclaimed? Does `get()` return the deleted keys correctly both before and after compaction?

**Exercise 4: Write Amplification vs Insert Volume**
Measure `stats.write_amplification` after 1000, 5000, and 20000 inserts. Plot the trend. At what insert volume does write amplification stabilize? Explain why it increases initially then levels off.

---

## 15. Key Takeaways

The LSM-tree is the architecture behind Cassandra, HBase, RocksDB, ScyllaDB, InfluxDB, and Kafka's log compaction. Its core idea — buffer writes in memory, flush as sequential batches, merge in background — eliminates random I/O entirely from the write path. This achieves write throughput and write amplification far better than B-trees for random workloads.

The cost is read complexity: reads must check the MemTable, multiple L0 SSTables (which may overlap), and binary search through L1+ SSTables. Bloom filters are the critical optimization that makes this practical by short-circuiting reads on SSTables that definitely don't contain the target key.

Compaction is the engine's housekeeping process: without it, stale versions and tombstones accumulate, disk usage grows without bound, and reads degrade linearly with the number of SSTables. Compaction is also the primary source of LSM write amplification (data is rewritten multiple times as it moves through levels). The choice of compaction strategy (leveled vs size-tiered) reflects the workload's read vs write priority.

Understanding these mechanics — MemTable flushes, SSTable immutability, Bloom filter FPR, compaction lag, tombstone accumulation, write stalls — makes Cassandra performance problems predictable and tunable rather than mysterious.

---

## 16. Connections to Other Modules

- **M47 — B-Tree Storage Engines:** The module that establishes the problem LSM-trees solve (B-tree write amplification). Every LSM-tree design choice is a direct contrast to the B-tree: sequential vs random I/O, immutable vs mutable pages, compaction vs in-place updates.
- **M49 — Write-Ahead Log:** The WAL is what gives LSM-trees crash safety. The MemTable is in memory and lost on crash; the WAL is the persistence layer that enables reconstruction. Understanding WAL (M49) completes the picture of how the MemTable survives crashes.
- **DCS-KFK-101 (Kafka):** Kafka's log storage is an LSM-tree-like structure (append-only log segments, compaction via log compaction). Understanding SSTable flush and compaction directly maps to understanding Kafka's segment rolling and log compaction behavior.
- **DE-PDA-101 (Parquet):** Parquet's row group and column chunk structure is inspired by SSTable design — sorted, immutable files with embedded indexes and statistics. Column-oriented vs row-oriented is a different axis, but the "immutable sorted file" concept is shared.

---

## 17. Anti-Patterns

**Anti-pattern: Using Cassandra for a workload with frequent single-key updates.** Cassandra is designed for write-heavy workloads. Frequent updates to the same key accumulate multiple SSTable entries for that key across levels — each update creates a new entry rather than modifying the existing one. Until compaction merges them, reads must check multiple versions. If 80% of operations are updates to existing keys (not new inserts), an LSM-tree has high space and read amplification from stale versions. A B-tree database would handle frequent updates more efficiently.

**Anti-pattern: Disabling Bloom filters to reduce memory usage.** Some teams disable Bloom filters to reduce RocksDB's memory footprint. Without Bloom filters, every point lookup for an absent key reads SSTables at every level. For a cache-miss workload (checking existence of keys before inserting), this can multiply disk I/O by 10-100×, causing far worse performance than the memory cost of the filters. The 10 bits/key cost is almost always worth it — reduce memory elsewhere (block cache, memtable size) before disabling Bloom filters.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `ldb` (RocksDB CLI) | Inspect RocksDB SSTable structure | `manifest_dump`, `get`, `scan` |
| `nodetool compactionstats` | Cassandra compaction queue status | Pending/active compactions |
| `nodetool tablestats` | Cassandra SSTable count + tombstones | `tombstones per slice` |
| `nodetool compact` | Trigger manual Cassandra compaction | For tombstone storms |
| `rocksdb.stats` DB property | RocksDB write amplification, stalls | Monitor in application |
| `lsm_engine.py` | LSM-tree simulation | MemTable, SSTable, compaction |
| `sst_dump` (RocksDB) | Dump SSTable contents to text | Debugging SSTable contents |

---

## 19. Glossary

**Bloom filter:** A probabilistic data structure that provides "definitely not present" guarantees with no false negatives and tunable false positive rates (typically 1%). Used in LSM-trees to skip SSTables that don't contain a target key, avoiding unnecessary I/O.

**Compaction:** Background process in an LSM-tree that merge-sorts multiple SSTables into fewer, larger ones. Resolves key conflicts (keeps newest version), purges tombstones, and maintains level organization (non-overlapping key ranges at L1+).

**Level 0 (L0):** The first level of an LSM-tree, containing freshly flushed SSTables. L0 SSTables have overlapping key ranges — every L0 SSTable must be checked for each read. Compaction triggers are based on L0 SSTable count.

**MemTable:** The in-memory sorted buffer that receives all writes. Backed by WAL for durability. Flushed to an SSTable when it reaches a size threshold.

**Space amplification:** Ratio of bytes on disk to bytes of live application data. Stale versions and tombstones cause space amplification > 1×. Compaction reduces space amplification.

**SSTable (Sorted String Table):** An immutable, sorted file on disk. Contains data blocks (key-value pairs in sorted order), an index block (for binary search), and a Bloom filter (for membership tests). Written sequentially; never modified after creation.

**Tombstone:** A special marker value inserted for a deleted key. Signals deletion to readers without modifying existing SSTables. Purged during compaction once all older versions of the key are gone.

**Write amplification:** Ratio of total bytes written to storage (including compaction) to bytes written by the application. LSM leveled: 10-50×. B-tree: 20-200× for random workloads.

**Write stall:** RocksDB's flow control mechanism: slows or stops incoming writes when L0 SSTable count exceeds configured thresholds, giving compaction time to catch up.

---

## 20. Self-Assessment

1. Explain why an LSM-tree insert is faster than a B-tree insert for random keys at high throughput.
2. What is the read path for `GET("user:007")` in an LSM-tree with a 2-entry MemTable, 3 L0 SSTables, and 2 L1 SSTables? Describe what checks happen at each step.
3. In `lsm_engine.py`, after inserting 5000 entries and running `force_compact()`, why might `stats.write_amplification` be higher than 1× even though there's no re-reading of data?
4. A Cassandra table has `avg tombstones per slice = 95000`. What is the likely cause, and what are your next steps?
5. What is the difference in read performance between L0 and L1 SSTables? Why?
6. You're choosing between RocksDB (LSM-tree) and Postgres (B-tree) for a pipeline that does 200,000 inserts/second of new time-series metrics, with 5,000 range reads/second. Which do you choose and why?
7. What happens to a key that is inserted, then updated 5 times, then deleted, before any compaction runs? How many SSTable entries exist for that key? How does a `GET` resolve this?
8. A Bloom filter with 10 bits/key has a 1% false positive rate. If you increase to 20 bits/key, what is the new false positive rate? What is the memory cost for 500M keys?

---

## 21. Module Summary

The LSM-tree solves the fundamental problem of B-trees for write-heavy workloads: random I/O. By buffering writes in an in-memory MemTable, flushing to immutable sorted SSTables, and running background compaction to merge them, the LSM-tree converts all writes to sequential I/O — achieving 5-20× better write throughput than B-trees for random workloads and dramatically lower write amplification.

The engine — `LSMEngine` combining `MemTable`, `SSTable`, `BloomFilter`, `WALWriter`, and leveled compaction — makes the mechanism concrete. Every design choice has a purpose: MemTable for write buffering without I/O, WAL for crash durability, SSTable immutability for sequential writes, Bloom filters to keep reads practical despite multiple SSTables, compaction to reclaim space and maintain read performance.

The tradeoffs — read amplification (multiple SSTables to check), space amplification (stale versions and tombstones until compaction), write stalls (when compaction lags) — are the recurring production problems for Cassandra, RocksDB, and HBase deployments. Recognizing a tombstone storm, diagnosing compaction lag, choosing leveled vs size-tiered compaction, and tuning MemTable size and compaction thread counts are the operational skills this module builds.

The next module — M49: Write-Ahead Log — zooms in on the WAL, the durability mechanism used by both B-trees (Postgres WAL) and LSM-trees (RocksDB WAL) to make in-memory writes crash-safe through sequential disk appends.
