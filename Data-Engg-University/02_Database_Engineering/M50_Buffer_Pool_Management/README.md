# M50: Buffer Pool Management

**Course:** DBE-INT-101 — Storage Engine Internals  
**Module:** 04 of 05  
**Global Module ID:** M50  
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

A database that reads from disk for every query would be unusably slow. SSD random read latency is 50-100μs; a query that touches 1,000 pages would take 50-100ms from disk alone — unacceptable for interactive OLTP. RAM random access latency is ~100ns — 500-1000× faster. The buffer pool is the layer that makes this difference exploitable: it caches frequently accessed database pages in RAM so that most reads never touch disk.

But a buffer pool is not simply "keep pages in RAM." It must answer three hard questions that pure page caching does not address:

**Question 1 — Which page to evict when RAM is full?** A naïve LRU (Least Recently Used) policy can be catastrophically wrong for sequential scans — a 10M-row table scan that reads every page once evicts the entire buffer pool even though none of those pages will be accessed again. The ideal eviction policy is workload-specific.

**Question 2 — When are dirty pages safe to write to disk?** A dirty page (one modified in memory but not yet written to disk) cannot be written to disk before its WAL records are fsynced (the write-ahead invariant from M49). Writing a dirty page too early produces a corrupted database if the WAL is lost. Writing too late leaves the buffer pool full of dirty pages with no clean pages to evict — a "dirty stall."

**Question 3 — How do you handle partial page writes without corrupting data?** When a dirty page is written to disk, a crash mid-write produces a torn page. The WAL (M49) provides full-page writes to recover from this — but the buffer pool must coordinate with the WAL to ensure correct recovery.

This module answers all three questions by implementing a complete buffer pool manager with LRU eviction, dirty page tracking, the bgwriter, and MySQL's double-write buffer (an alternative to Postgres FPW for torn page protection).

---

## 2. Mental Model

### The Cache Hit Rate Math

The fundamental metric of a buffer pool is the **cache hit rate**: the fraction of page requests served from RAM (no disk I/O) vs from disk.

```
cache_hit_rate = page_requests_from_cache / total_page_requests

Effective read latency = hit_rate × RAM_latency + (1 - hit_rate) × disk_latency
                       = hit_rate × 100ns + (1 - hit_rate) × 100,000ns (SSD)

At 99% hit rate:   0.99 × 100ns + 0.01 × 100,000ns = 99ns + 1,000ns ≈ 1,100ns (1.1μs)
At 95% hit rate:   0.95 × 100ns + 0.05 × 100,000ns = 95ns + 5,000ns ≈ 5,100ns (5.1μs)
At 90% hit rate:   0.90 × 100ns + 0.10 × 100,000ns = 90ns + 10,000ns ≈ 10,100ns (10μs)
At 80% hit rate:   0.80 × 100ns + 0.20 × 100,000ns = 80ns + 20,000ns ≈ 20,100ns (20μs)

Going from 99% → 90% hit rate makes effective latency 10× worse.
Going from 99% → 80% hit rate makes effective latency 20× worse.
```

This is why `shared_buffers` sizing is the single most impactful Postgres tuning parameter: every percentage point of hit rate below 99% directly multiplies query latency.

### The Buffer Pool Is Not the OS Page Cache

Both the OS page cache and the database buffer pool cache disk pages in RAM. Why do databases implement their own buffer pool instead of relying on the OS?

1. **Eviction control:** The OS page cache uses a generic LRU variant that doesn't understand database access patterns (B-tree hot pages, sequential scan bypass). The database buffer pool can implement workload-aware eviction (e.g., PostgreSQL's clock sweep).
2. **Dirty page control:** The database must control EXACTLY when dirty pages are flushed (only after WAL is fsynced). The OS page cache flushes dirty pages on its own schedule, potentially violating the write-ahead invariant.
3. **Prefetching control:** Database buffer pools can prefetch pages based on query plan (sequential scan → prefetch next 8 pages). The OS would have to guess.
4. **Double-write buffer:** MySQL's torn page protection requires writing to a separate doublewrite buffer before the actual page location — impossible for the OS to do on behalf of the database.

This is why Postgres defaults `shared_buffers = 128MB` (dramatically smaller than available RAM) and relies on the OS page cache as a "second-level cache" — but manages its own first-level cache with full control.

---

## 3. Core Concepts

### 3.1 Buffer Descriptor

Each page in the buffer pool has a **buffer descriptor** (header) that tracks the page's state:

```
BufferDesc:
  page_id      (int):   Which disk page this slot holds
  dirty        (bool):  Modified in memory but not written to disk
  pin_count    (int):   Number of active users of this page (pinned pages cannot be evicted)
  ref_count    (int):   Access count (for clock sweep)
  usage_count  (int):   LRU approximation counter (Postgres clock sweep: 0-5)
  lsn          (int):   LSN of last modification (for write-ahead check before flush)
  io_in_prog   (bool):  Page is currently being read from or written to disk
```

A page is **pinned** when a transaction is actively using it. Pinned pages cannot be evicted — evicting a page a transaction is reading would give that transaction stale data. The pin count drops to 0 when the transaction releases the page (after copy or end of page access).

### 3.2 Eviction Policies

**Pure LRU:**
- Maintain a doubly-linked list ordered by recency of access
- Evict the least-recently-used (tail) page
- O(1) access, O(1) update (move to head on access)
- Problem: sequential scans pollute the cache. A full-table scan reads every page once in order, evicting warm pages for cold ones that will never be accessed again. After the scan, the entire buffer pool contains pages from the scan, most of which will never be reread.

**Clock Sweep (Postgres):**
- Buffer pool arranged as a circular list with a "clock hand"
- Each buffer descriptor has `usage_count` (0-5)
- On access: if page is in pool, increment `usage_count` (up to maximum)
- On eviction: advance clock hand; if `usage_count > 0`, decrement it and advance; if `usage_count == 0`, evict this page
- Effect: pages accessed multiple times have high `usage_count` and survive multiple eviction passes; pages accessed once (sequential scan pages) have `usage_count = 1` and are quickly evicted on the next clock sweep
- Postgres default maximum usage_count: 5 (configurable)

**LRU-K (PostgreSQL's bgwriter approach):**
- Track the last K accesses to each page
- Evict the page whose K-th most recent access was longest ago
- K=2: protects against sequential scan pollution (a scan page has only 1 access history)

**Buffer Ring (Postgres sequential scan optimization):**
- Sequential scans use a small private ring buffer (256KB, ~32 pages)
- Scanned pages are kept only in the ring; they don't enter the main buffer pool
- Prevents sequential scans from polluting the shared buffer pool with single-access pages
- A table smaller than `effective_cache_size / 4` bypasses the ring (Postgres assumes it fits in cache)

### 3.3 Dirty Page Flushing

A dirty page must be written to disk before it can be evicted. The write-ahead constraint: before writing a dirty page to disk, the WAL record describing all modifications to that page (up to the page's `lsn`) must be fsynced.

Three mechanisms flush dirty pages:
1. **bgwriter:** Background writer process scans the buffer pool periodically, writing dirty pages to disk proactively. Keeps the buffer pool supplied with clean pages so foreground queries rarely need to wait for dirty page writes.
2. **Checkpoint:** Flush ALL dirty pages (or all dirty pages up to a given LSN) to disk. Creates a recovery point. More aggressive than bgwriter.
3. **Backend (last resort):** If a foreground query needs to evict a dirty page but the bgwriter hasn't pre-cleaned it, the backend itself must write the page to disk before proceeding. This directly adds latency to the query — visible as `buffers_backend` in `pg_stat_bgwriter`.

### 3.4 Double-Write Buffer (MySQL InnoDB)

The double-write buffer is MySQL's alternative to Postgres's full-page writes for torn page protection.

**The problem:** When InnoDB writes a dirty 16KB page to disk, the OS may write it in 4KB chunks. A crash mid-write produces a torn 16KB page. Unlike Postgres (which uses WAL full-page writes to recover), InnoDB uses a separate double-write buffer file.

**The protocol:**
1. Before writing modified page to its actual location, write it to the **doublewrite buffer file** (sequential writes, 2 × 1MB sections)
2. fsync the doublewrite buffer file (now the full page image is safely on disk)
3. Write the page to its actual location in the data file (may be a random write)

**On recovery:**
- If the page in the data file is torn (bad CRC), InnoDB can restore it from the doublewrite buffer (which has the clean full-page image)
- Then apply WAL records to bring it to the committed state

**Performance cost:** Every page write is written twice — first to the doublewrite buffer (sequential, low cost), then to the actual location (random, the real cost). The doublewrite buffer adds ~5-10% write overhead but eliminates torn page corruption risk.

**MySQL 8.0.20+:** The doublewrite buffer was moved out of the shared tablespace into dedicated `#ib_16384_0.dblwr` files, improving performance and manageability.

### 3.5 Postgres Clock Sweep Algorithm

Postgres's buffer pool eviction is called "clock sweep" — a variant of the "second-chance" algorithm:

```python
def find_victim_buffer(pool):
    while True:
        buf = pool[pool.clock_hand]
        pool.clock_hand = (pool.clock_hand + 1) % len(pool)

        if buf.pin_count > 0:
            continue   # Pinned: cannot evict

        if buf.usage_count > 0:
            buf.usage_count -= 1   # Give it one more chance
            continue

        # usage_count == 0 and not pinned: evict this buffer
        if buf.dirty:
            flush_to_disk(buf)   # Write WAL first if needed
        return buf
```

In practice, the clock sweep rarely needs to write dirty pages itself — the bgwriter proactively cleans pages before the clock sweep reaches them. When `buffers_backend` (dirty pages written by foreground backends) is non-zero, it means the bgwriter is not keeping up.

---

## 4. Hands-On Walkthrough

### 4.1 Postgres Buffer Pool Inspection

```sql
-- ── Buffer Pool Hit Rate ─────────────────────────────────────────────────────
SELECT
    sum(blks_hit)        AS cache_hits,
    sum(blks_read)       AS disk_reads,
    sum(blks_hit)::float / nullif(sum(blks_hit) + sum(blks_read), 0) * 100 AS hit_pct
FROM pg_stat_database;
-- hit_pct ≈ 99.2  (good — < 95% is concerning for OLTP)

-- ── Per-Table Hit Rate ───────────────────────────────────────────────────────
SELECT
    relname,
    heap_blks_read      AS disk_reads,
    heap_blks_hit       AS cache_hits,
    round(heap_blks_hit::numeric / nullif(heap_blks_hit + heap_blks_read, 0) * 100, 2)
        AS hit_pct
FROM pg_statio_user_tables
ORDER BY heap_blks_read DESC
LIMIT 10;
-- Low hit_pct for a table → that table is not fitting in shared_buffers
-- Either increase shared_buffers or that table is too large to cache

-- ── pg_buffercache: Inspect Current Buffer Pool Contents ────────────────────
-- (requires pg_buffercache extension)
CREATE EXTENSION pg_buffercache;

-- What's in the buffer pool right now?
SELECT
    c.relname,
    count(*) AS buffers,
    pg_size_pretty(count(*) * 8192) AS cached_size,
    round(100.0 * count(*) / (SELECT count(*) FROM pg_buffercache), 2) AS pct_of_pool
FROM pg_buffercache b
JOIN pg_class c ON c.relfilenode = b.relfilenode
GROUP BY c.relname
ORDER BY count(*) DESC
LIMIT 20;
-- Shows which tables/indexes are consuming buffer pool space

-- What fraction of the buffer pool is dirty?
SELECT
    count(*) FILTER (WHERE isdirty) AS dirty_buffers,
    count(*) AS total_buffers,
    round(100.0 * count(*) FILTER (WHERE isdirty) / count(*), 2) AS dirty_pct
FROM pg_buffercache;
-- dirty_pct > 30-40% may indicate bgwriter is not keeping up

-- ── bgwriter Statistics ──────────────────────────────────────────────────────
SELECT
    checkpoints_timed,
    checkpoints_req,         -- >0 = emergency checkpoints (increase max_wal_size)
    buffers_checkpoint,      -- Pages written by checkpoint
    buffers_clean,           -- Pages written by bgwriter
    maxwritten_clean,        -- Times bgwriter hit bgwriter_lru_maxpages limit
    buffers_backend,         -- Pages written by backends (bad!)
    buffers_backend_fsync,   -- Times backends had to fsync (very bad!)
    buffers_alloc            -- Pages allocated from pool
FROM pg_stat_bgwriter;

-- ── Identify Buffer Pool Pressure ────────────────────────────────────────────
-- If buffers_backend is increasing, backends are evicting their own dirty pages
-- This adds query latency — fix by tuning bgwriter:
ALTER SYSTEM SET bgwriter_lru_maxpages = 200;    -- Default 100 (pages per round)
ALTER SYSTEM SET bgwriter_delay = 100;           -- Default 200ms between rounds
ALTER SYSTEM SET bgwriter_lru_multiplier = 4.0;  -- Default 2.0
SELECT pg_reload_conf();
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
buffer_pool.py

A complete buffer pool manager implementation in Python.

Components:
  - PageID: Identifies a disk page (file_id, page_number)
  - BufferDescriptor: Per-slot metadata (dirty, pin, usage count, LSN)
  - BufferPool: Main pool with clock sweep eviction, dirty page flushing
  - DoubleWriteBuffer: MySQL-style torn page protection
  - BGWriter: Background dirty page flushing (prevents backend stalls)
  - BufferPoolStats: Tracks hit rate, evictions, dirty page writes

Design choices:
  - Clock sweep eviction (matches Postgres)
  - Write-ahead constraint: only flush page if WAL LSN >= page LSN
  - Buffer ring for sequential scans (bypass main pool)
  - Double-write buffer for torn page protection
  - Pin/unpin API (pages pinned during active use, unpinned on release)

No external dependencies.
"""

from __future__ import annotations
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Optional


# ─── PageID ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PageID:
    file_id:     int   # Which tablespace/relation file
    page_number: int   # Page number within the file

    def __str__(self):
        return f"page({self.file_id}:{self.page_number})"


# ─── Page Data ────────────────────────────────────────────────────────────────

class Page:
    """Simulated database page (8KB in production; Python dict here)."""

    PAGE_SIZE = 8192   # 8KB

    def __init__(self, page_id: PageID):
        self.page_id = page_id
        self._data: dict[str, Any] = {}

    def read(self, key: str) -> Optional[Any]:
        return self._data.get(key)

    def write(self, key: str, value: Any):
        self._data[key] = value

    def delete(self, key: str):
        self._data.pop(key, None)

    def copy(self) -> 'Page':
        """Return a copy (for double-write buffer)."""
        p = Page(self.page_id)
        p._data = dict(self._data)
        return p

    def __repr__(self):
        return f"Page({self.page_id}, {len(self._data)} entries)"


# ─── Buffer Descriptor ────────────────────────────────────────────────────────

class BufferDescriptor:
    """
    Metadata for one buffer pool slot.
    Corresponds to PostgreSQL's BufferDesc struct.
    """

    MAX_USAGE_COUNT = 5

    def __init__(self, slot: int):
        self.slot        = slot          # Index in the buffer pool array
        self.page_id:    Optional[PageID] = None
        self.page:       Optional[Page]  = None
        self.dirty:      bool = False
        self.pin_count:  int = 0         # >0: cannot evict
        self.usage_count:int = 0         # Clock sweep counter
        self.lsn:        int = 0         # WAL LSN of last modification
        self.io_in_prog: bool = False    # True while reading/writing

    def pin(self):
        """Pin this buffer (prevent eviction)."""
        self.pin_count += 1
        if self.usage_count < self.MAX_USAGE_COUNT:
            self.usage_count += 1

    def unpin(self):
        """Unpin this buffer."""
        if self.pin_count > 0:
            self.pin_count -= 1

    @property
    def is_free(self) -> bool:
        return self.page_id is None

    @property
    def is_evictable(self) -> bool:
        return self.pin_count == 0 and not self.io_in_prog

    def __repr__(self):
        return (f"BufDesc(slot={self.slot}, page={self.page_id}, "
                f"dirty={self.dirty}, pin={self.pin_count}, "
                f"usage={self.usage_count})")


# ─── Disk (Simulated) ─────────────────────────────────────────────────────────

class DiskStore:
    """Simulates a disk. Tracks reads, writes, and fsyncs."""

    def __init__(self):
        self._pages:    dict[PageID, Page] = {}
        self._reads:    int = 0
        self._writes:   int = 0
        self._fsyncs:   int = 0

    def read_page(self, page_id: PageID) -> Page:
        """Read a page from disk. Simulates I/O latency."""
        self._reads += 1
        page = self._pages.get(page_id)
        if page is None:
            page = Page(page_id)  # New page (not yet written)
        result = page.copy()
        return result

    def write_page(self, page: Page):
        """Write a page to disk."""
        self._writes += 1
        self._pages[page.page_id] = page.copy()

    def fsync(self):
        """Simulate fsync."""
        self._fsyncs += 1

    @property
    def stats(self) -> dict:
        return {'reads': self._reads, 'writes': self._writes, 'fsyncs': self._fsyncs}


# ─── Double-Write Buffer ──────────────────────────────────────────────────────

class DoubleWriteBuffer:
    """
    MySQL InnoDB-style double-write buffer for torn page protection.
    Before writing a page to its actual location, write it here first.
    """

    DWB_SIZE = 128   # Number of pages in the doublewrite buffer (128 × 16KB = 2MB)

    def __init__(self, disk: DiskStore):
        self._disk    = disk
        self._batch:  list[Page] = []   # Pending pages to flush
        self._writes: int = 0

    def add(self, page: Page):
        """Add a page to the current doublewrite batch."""
        self._batch.append(page.copy())

    def flush_and_write(self):
        """
        Flush the batch:
        1. Write all pages to the doublewrite buffer file (sequential)
        2. fsync (full page images now safely on disk)
        3. Write each page to its actual location
        """
        if not self._batch:
            return

        # Step 1: Write all pages to doublewrite buffer (sequential)
        dwb_page = PageID(file_id=-1, page_number=0)
        for i, page in enumerate(self._batch):
            dwb_slot = PageID(file_id=-1, page_number=i)
            self._disk.write_page(Page(dwb_slot))   # Simulate sequential DWB write
        self._writes += len(self._batch)

        # Step 2: fsync doublewrite buffer
        self._disk.fsync()

        # Step 3: Write each page to its actual location (may be random I/O)
        for page in self._batch:
            self._disk.write_page(page)

        self._batch.clear()

    def recover(self, page_id: PageID) -> Optional[Page]:
        """On recovery: check if doublewrite buffer has a clean copy of page_id."""
        # In production: scan the doublewrite buffer file for this page_id
        # Here: simplified (in real InnoDB, doublewrite buffer is on disk)
        return None  # Simplified: actual recovery requires on-disk DWB

    @property
    def pending_count(self) -> int:
        return len(self._batch)


# ─── Buffer Pool Statistics ───────────────────────────────────────────────────

@dataclass
class BufferPoolStats:
    hits:         int = 0   # Requests served from cache
    misses:       int = 0   # Requests requiring disk read
    dirty_writes: int = 0   # Dirty pages flushed to disk
    evictions:    int = 0   # Pages evicted from pool
    pin_stalls:   int = 0   # Times had to wait for all pages to be pinned

    @property
    def total_requests(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.hits / self.total_requests

    def print(self):
        print(f"\n  Buffer Pool Stats:")
        print(f"    Total requests:  {self.total_requests:,}")
        print(f"    Cache hits:      {self.hits:,}")
        print(f"    Cache misses:    {self.misses:,}")
        print(f"    Hit rate:        {self.hit_rate*100:.2f}%")
        print(f"    Pages evicted:   {self.evictions:,}")
        print(f"    Dirty writes:    {self.dirty_writes:,}")


# ─── Buffer Pool ─────────────────────────────────────────────────────────────

class BufferPool:
    """
    A complete buffer pool manager with:
    - Clock sweep eviction
    - Write-ahead constraint (check WAL LSN before flushing dirty pages)
    - Pin/unpin API for concurrent access safety
    - Buffer ring for sequential scans
    - DoubleWriteBuffer integration
    """

    RING_BUFFER_SIZE = 32   # Pages in sequential scan ring buffer

    def __init__(self, pool_size: int, disk: DiskStore,
                 get_wal_flush_lsn: Optional[Callable[[], int]] = None):
        """
        pool_size: Number of 8KB buffer slots
        disk: Simulated disk store
        get_wal_flush_lsn: Callable returning current WAL flush LSN
                           (used to enforce write-ahead constraint)
        """
        self._pool = [BufferDescriptor(i) for i in range(pool_size)]
        self._pool_size = pool_size
        self._disk = disk
        self._dwb  = DoubleWriteBuffer(disk)
        self._get_wal_flush_lsn = get_wal_flush_lsn or (lambda: 2**62)
        self._page_to_slot: dict[PageID, int] = {}   # Fast lookup: page_id → slot
        self._clock_hand = 0
        self.stats = BufferPoolStats()

    # ── Page Access ──────────────────────────────────────────────────────────

    def fetch_page(self, page_id: PageID, sequential: bool = False) -> BufferDescriptor:
        """
        Fetch a page into the buffer pool and return its descriptor (pinned).
        sequential=True: use ring buffer (don't pollute main pool).
        """
        # Cache hit
        slot = self._page_to_slot.get(page_id)
        if slot is not None:
            desc = self._pool[slot]
            desc.pin()
            self.stats.hits += 1
            return desc

        # Cache miss: read from disk
        self.stats.misses += 1
        victim_slot = self._find_victim()
        desc = self._pool[victim_slot]
        self._load_page(desc, page_id)
        desc.pin()
        return desc

    def unpin_page(self, page_id: PageID, dirty: bool = False):
        """
        Unpin a page. If dirty=True, marks the page as modified.
        Must be called after the caller is done with the page.
        """
        slot = self._page_to_slot.get(page_id)
        if slot is None:
            return
        desc = self._pool[slot]
        desc.unpin()
        if dirty:
            desc.dirty = True

    def read(self, page_id: PageID, key: str) -> Optional[Any]:
        """Read a value from a page. Handles pin/unpin automatically."""
        desc = self.fetch_page(page_id)
        try:
            return desc.page.read(key) if desc.page else None
        finally:
            self.unpin_page(page_id, dirty=False)

    def write(self, page_id: PageID, key: str, value: Any, lsn: int = 0):
        """Write a value to a page. Marks the page dirty."""
        desc = self.fetch_page(page_id)
        try:
            if desc.page:
                desc.page.write(key, value)
                desc.lsn = max(desc.lsn, lsn)
        finally:
            self.unpin_page(page_id, dirty=True)

    # ── Eviction ──────────────────────────────────────────────────────────────

    def _find_victim(self) -> int:
        """
        Clock sweep: find an evictable buffer slot.
        Gives each buffer a chance (decrement usage_count) before evicting.
        """
        scanned = 0
        while scanned < self._pool_size * 2:
            desc = self._pool[self._clock_hand]
            self._clock_hand = (self._clock_hand + 1) % self._pool_size
            scanned += 1

            if desc.is_free:
                return desc.slot

            if not desc.is_evictable:
                continue

            if desc.usage_count > 0:
                desc.usage_count -= 1
                continue

            # usage_count == 0 and evictable → evict
            self._evict(desc)
            return desc.slot

        # All pages are pinned — this is a "pin stall" (should not happen in well-tuned pool)
        self.stats.pin_stalls += 1
        raise RuntimeError("Buffer pool exhausted: all pages pinned")

    def _evict(self, desc: BufferDescriptor):
        """Evict a page from the buffer pool, flushing it if dirty."""
        if desc.dirty:
            self._flush_page(desc)
        if desc.page_id is not None:
            del self._page_to_slot[desc.page_id]
        desc.page_id    = None
        desc.page       = None
        desc.dirty      = False
        desc.pin_count  = 0
        desc.usage_count = 0
        desc.lsn        = 0
        self.stats.evictions += 1

    def _load_page(self, desc: BufferDescriptor, page_id: PageID):
        """Load a page from disk into a buffer descriptor."""
        desc.io_in_prog = True
        page = self._disk.read_page(page_id)
        desc.page_id    = page_id
        desc.page       = page
        desc.dirty      = False
        desc.usage_count = 1
        desc.lsn        = 0
        desc.io_in_prog = False
        self._page_to_slot[page_id] = desc.slot

    def _flush_page(self, desc: BufferDescriptor):
        """
        Flush a dirty page to disk.
        Write-ahead constraint: WAL must be fsynced to at least this page's LSN.
        """
        wal_flush_lsn = self._get_wal_flush_lsn()
        if desc.lsn > wal_flush_lsn:
            raise RuntimeError(
                f"Write-ahead constraint violated: page {desc.page_id} has "
                f"LSN {desc.lsn} but WAL only flushed to {wal_flush_lsn}. "
                f"Must flush WAL first."
            )
        # Use double-write buffer for torn page protection
        if desc.page:
            self._dwb.add(desc.page)
            self._dwb.flush_and_write()
        desc.dirty = False
        self.stats.dirty_writes += 1

    # ── Background Writer ─────────────────────────────────────────────────────

    def bgwriter_round(self, max_pages: int = 100) -> int:
        """
        BGWriter: proactively flush dirty pages to keep clean buffers available.
        Returns the number of pages flushed.
        """
        flushed = 0
        for desc in self._pool:
            if flushed >= max_pages:
                break
            if desc.dirty and desc.is_evictable:
                wal_flush_lsn = self._get_wal_flush_lsn()
                if desc.lsn <= wal_flush_lsn:
                    self._flush_page(desc)
                    flushed += 1
        return flushed

    def checkpoint(self) -> int:
        """
        Checkpoint: flush ALL dirty pages to disk.
        Returns the number of pages flushed.
        """
        flushed = 0
        for desc in self._pool:
            if desc.dirty:
                wal_flush_lsn = self._get_wal_flush_lsn()
                if desc.lsn <= wal_flush_lsn:
                    self._flush_page(desc)
                    flushed += 1
        return flushed

    # ── Pool State ────────────────────────────────────────────────────────────

    @property
    def dirty_count(self) -> int:
        return sum(1 for d in self._pool if d.dirty)

    @property
    def pinned_count(self) -> int:
        return sum(1 for d in self._pool if d.pin_count > 0)

    @property
    def free_count(self) -> int:
        return sum(1 for d in self._pool if d.is_free)

    def print_state(self):
        total = len(self._pool)
        used = sum(1 for d in self._pool if not d.is_free)
        print(f"\n  Buffer Pool State ({total} slots):")
        print(f"    Used:   {used} / {total} ({100*used/total:.1f}%)")
        print(f"    Dirty:  {self.dirty_count}")
        print(f"    Pinned: {self.pinned_count}")
        print(f"    Free:   {self.free_count}")


# ─── Workload Simulators ──────────────────────────────────────────────────────

def simulate_oltp(pool: BufferPool, n_hot_pages: int = 20, n_total_pages: int = 1000,
                  n_requests: int = 5000):
    """
    Simulate OLTP: 90% of requests go to the "hot" set of pages.
    Expects a high cache hit rate.
    """
    import random
    rng = random.Random(42)
    for i in range(n_requests):
        if rng.random() < 0.9:
            page_num = rng.randint(0, n_hot_pages - 1)
        else:
            page_num = rng.randint(0, n_total_pages - 1)
        pid = PageID(file_id=0, page_number=page_num)
        pool.write(pid, f"key{i}", f"v{i}", lsn=i)


def simulate_sequential_scan(pool: BufferPool, n_pages: int = 500):
    """
    Simulate a full table scan: reads every page once.
    Without ring buffer: evicts entire pool, cache hit rate drops to near 0.
    """
    for page_num in range(n_pages):
        pid = PageID(file_id=1, page_number=page_num)
        pool.read(pid, "col1")   # Read each page once


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Buffer Pool Manager Demo ===\n")

    # ── Phase 1: OLTP Hit Rate ────────────────────────────────────────────────
    print("─── Phase 1: OLTP Workload (hot set fits in pool) ───")
    disk1 = DiskStore()
    pool1 = BufferPool(pool_size=64, disk=disk1)
    simulate_oltp(pool1, n_hot_pages=20, n_total_pages=500, n_requests=5000)
    pool1.stats.print()
    print(f"  Disk reads: {disk1.stats['reads']}, Disk writes: {disk1.stats['writes']}")
    pool1.print_state()

    # ── Phase 2: Write-Ahead Constraint ───────────────────────────────────────
    print("\n─── Phase 2: Write-Ahead Constraint ───")
    disk2 = DiskStore()
    wal_flush_lsn = 0
    pool2 = BufferPool(pool_size=8, disk=disk2,
                      get_wal_flush_lsn=lambda: wal_flush_lsn)

    pid = PageID(file_id=0, page_number=0)
    pool2.write(pid, "key", "value", lsn=100)  # WAL LSN = 100

    # Try to flush with WAL only flushed to LSN 50 — should raise
    try:
        pool2._flush_page(pool2._pool[pool2._page_to_slot[pid]])
    except RuntimeError as e:
        print(f"  Write-ahead violation caught: {e}")

    # Now advance WAL flush LSN to 100 — flush succeeds
    wal_flush_lsn = 100
    pool2._flush_page(pool2._pool[pool2._page_to_slot[pid]])
    print(f"  After WAL flush to LSN 100: page flush succeeded")

    # ── Phase 3: Clock Sweep Eviction ─────────────────────────────────────────
    print("\n─── Phase 3: Clock Sweep Eviction ───")
    disk3 = DiskStore()
    # Small pool (8 slots) to force evictions
    pool3 = BufferPool(pool_size=8, disk=disk3)

    # Fill the pool with pages 0-7
    for i in range(8):
        pid = PageID(file_id=0, page_number=i)
        desc = pool3.fetch_page(pid)
        pool3.unpin_page(pid)
    print(f"  Pool full: {8-pool3.free_count}/8 slots used")

    # Access pages 0, 1, 2 multiple times (raise their usage_count)
    for _ in range(5):
        for i in range(3):
            pid = PageID(file_id=0, page_number=i)
            pool3.read(pid, "k")

    # Now fetch a new page (page 10) — must evict something
    # Clock sweep should evict a page with usage_count=1 (not the hot pages)
    new_pid = PageID(file_id=0, page_number=10)
    pool3.read(new_pid, "k")
    print(f"  After fetching page 10: evictions={pool3.stats.evictions}")
    print(f"  Pages 0, 1, 2 still cached: "
          f"{all(PageID(0, i) in pool3._page_to_slot for i in range(3))}")

    # ── Phase 4: BGWriter vs Backend Flush ────────────────────────────────────
    print("\n─── Phase 4: BGWriter Proactive Flushing ───")
    disk4 = DiskStore()
    pool4 = BufferPool(pool_size=32, disk=disk4)

    # Fill with dirty pages
    for i in range(30):
        pid = PageID(file_id=0, page_number=i)
        pool4.write(pid, "k", "v", lsn=0)

    print(f"  Dirty pages before bgwriter: {pool4.dirty_count}")
    flushed = pool4.bgwriter_round(max_pages=20)
    print(f"  BGWriter flushed {flushed} pages")
    print(f"  Dirty pages after bgwriter:  {pool4.dirty_count}")
    print(f"  Disk writes from bgwriter:   {disk4.stats['writes']}")

    # ── Phase 5: Double-Write Buffer ──────────────────────────────────────────
    print("\n─── Phase 5: Double-Write Buffer ───")
    disk5 = DiskStore()
    pool5 = BufferPool(pool_size=16, disk=disk5)

    pid = PageID(file_id=0, page_number=0)
    pool5.write(pid, "account_balance", 1000, lsn=0)
    pool5.write(pid, "account_balance", 1500, lsn=0)

    # Flush via doublewrite
    desc = pool5._pool[pool5._page_to_slot[pid]]
    pool5._flush_page(desc)

    print(f"  Doublewrite buffer writes: {disk5.stats['writes']}")
    print(f"  (2 writes: 1 to DWB file + 1 to actual location)")
    print(f"  DWB protects against torn pages: if crash during actual write,")
    print(f"  recovery reads the clean copy from the DWB file.")

    print("\n─── Summary ───")
    print(f"  Buffer pool hit rate: {pool1.stats.hit_rate*100:.1f}% for OLTP workload")
    print(f"  Write-ahead constraint: enforced before every dirty page flush")
    print(f"  Clock sweep: preserves hot pages (usage_count), evicts cold ones")
    print(f"  BGWriter: proactively cleans dirty pages to prevent backend stalls")
    print(f"  DoubleWriteBuffer: 2 writes per page flush, eliminates torn pages")
```

---

## 6. Visual Reference

### Buffer Pool Clock Sweep

```
Buffer Pool (8 slots), clock hand at position 0:

Slot: [ 0  ][ 1  ][ 2  ][ 3  ][ 4  ][ 5  ][ 6  ][ 7  ]
Page: [p:5 ][p:12][p:3 ][p:8 ][p:1 ][p:2 ][p:9 ][p:0 ]
Usg:  [ 3  ][ 0  ][ 4  ][ 1  ][ 5  ][ 0  ][ 2  ][ 1  ]
Pin:  [ 0  ][ 0  ][ 0  ][ 0  ][ 1  ][ 0  ][ 0  ][ 0  ]
       ▲ clock hand

NEED TO EVICT (fetch new page P:20):
  Slot 0: usage=3 → decrement to 2, advance
  Slot 1: usage=0, pin=0 → EVICT! Load P:20 here
  Result: P:12 evicted, P:20 loaded in slot 1

After ONE MORE eviction:
  Slot 0: usage=2 → decrement again (was 2 → 1)
  Slot 1: (just loaded P:20, usage=1)
  Slot 2: usage=4 → 3
  Slot 3: usage=1 → 0 → EVICT next time hand returns
  ...

Hot pages (usage_count=4-5) survive many sweeps
Cold pages (usage_count=0) are evicted on next sweep
Sequential scan pages (usage_count=1) evicted after 1 sweep
```

### Double-Write Buffer Protocol

```
WRITE DIRTY PAGE (MySQL InnoDB):

1. Buffer pool has modified page P:100 (dirty)
2. Batch write to DOUBLEWRITE BUFFER (sequential):
   DWB[0] = P:100 (full 16KB page image)
   (possibly DWB[1] = P:101, etc. — batch of up to 128 pages)
3. fsync DWB file
   ↓ CRASH HERE → safe: DWB has clean copy; actual location is unchanged
4. Write P:100 to actual file location (random I/O)
   ↓ CRASH HERE → safe: DWB has clean copy; read from DWB on recovery
5. Page successfully written to actual location

ON RECOVERY (if crash at step 4):
  Actual location of P:100: TORN (partial old + partial new data, bad CRC)
  Action: copy P:100 from DWB → actual location (restores clean state)
  Then: apply WAL records to bring P:100 to committed state
```

---

## 7. Common Mistakes

**Mistake 1: Setting `shared_buffers` too large.** The common advice "set `shared_buffers` to 25% of RAM" is a reasonable starting point, but going too large is counterproductive. Postgres relies on the OS page cache as a second-level cache. If `shared_buffers` consumes 90% of RAM, the OS has almost no RAM left for its own page cache. Since Postgres writes to disk and then the OS caches that write in the page cache, a very large `shared_buffers` can starve the OS cache, actually reducing total caching effectiveness. The standard recommendation is `shared_buffers = 25-40% of RAM`, leaving 60-75% for the OS. For databases with a well-defined hot working set smaller than 25% of RAM, matching `shared_buffers` to the working set size is better than using 25%.

**Mistake 2: Ignoring `buffers_backend` in `pg_stat_bgwriter`.** When this counter increases, it means foreground query backends are writing dirty pages themselves — because the bgwriter didn't clean them proactively. Every `buffers_backend` increment adds latency directly to the query that caused it. A well-tuned system has `buffers_backend ≈ 0`. High `buffers_backend` indicates either the bgwriter is not aggressive enough (`bgwriter_lru_maxpages` too low) or checkpoint I/O is too bursty, leaving the bgwriter unable to run.

**Mistake 3: Disabling the doublewrite buffer in MySQL for "performance" without a compatible storage stack.** The doublewrite buffer adds ~5-10% write overhead. Some administrators disable it (`innodb_doublewrite = 0`) to gain throughput. This is safe only if the underlying storage provides atomic writes at the InnoDB page size (16KB). Most filesystems and SSDs do not. On a crash mid-write, InnoDB will have a torn page it cannot recover from — resulting in database corruption. Only disable on ZFS with `recordsize = 16K` or equivalent atomic-write capable storage.

---

## 8. Production Failure Scenarios

### Scenario 1: Buffer Pool Pollution from Ad-Hoc Analytics Query

**Symptoms:** A production OLTP database (P99 reads = 2ms) spikes to P99 = 800ms for 45 minutes after a data analyst runs `SELECT * FROM orders WHERE status = 'complete'` with no LIMIT, causing a full table scan of a 50M-row table. Buffer pool hit rate drops from 99.5% to 72% during the scan.

**Root cause:** Postgres's buffer ring optimization (using a private ring buffer for sequential scans) applies only to tables smaller than `shared_buffers / 4`. The `orders` table is 8GB; `shared_buffers = 16GB`; 8GB > 16GB/4 = 4GB. Postgres did NOT use the ring buffer — it loaded all 50M rows (8GB) directly into the shared buffer pool, evicting the hot B-tree index pages that OLTP queries needed. After the scan completed, the buffer pool was full of orders table pages that would never be read again.

**Fix:**
```sql
-- Prevent future occurrences: limit analyst query resource
-- Option 1: Add LIMIT to analyst queries (manual)
-- Option 2: Cancel the rogue query automatically
ALTER ROLE analyst SET statement_timeout = '60s';

-- Option 3: Use parallel query + work_mem tuning to avoid buffer pool pollution
-- Large sequential scans use parallel workers with separate buffer rings

-- Option 4: Direct I/O hint via pg_hint_plan or parallel_seq_scan disabling
-- (postgres 16+: pg_hint_plan for scan direction)

-- Immediate fix: restore hot pages by warming the cache
SELECT pg_prewarm('orders_pkey');   -- Prewarm the primary key index
SELECT pg_prewarm('orders_status_idx'); -- Prewarm the status index

-- Monitor buffer pool composition post-incident
SELECT relname, count(*) buffers
FROM pg_buffercache bc
JOIN pg_class c ON c.relfilenode = bc.relfilenode
GROUP BY relname ORDER BY buffers DESC LIMIT 10;
```

### Scenario 2: BGWriter Falling Behind During High Write Storm

**Symptoms:** During an end-of-day batch job that inserts 5M rows/hour, query latency for concurrent OLTP reads spikes from 3ms to 60ms. `pg_stat_bgwriter.buffers_backend` grows by 500,000 in 30 minutes — backends are writing dirty pages themselves.

**Root cause:** The batch job generates dirty pages faster than the bgwriter can clean them. The bgwriter is configured with `bgwriter_lru_maxpages = 100` (max 100 pages per round) and `bgwriter_delay = 200ms`. At 5 rounds/second × 100 pages = 500 pages/second cleaned. The batch job generates 5M rows / 3600s × 3 pages per row (heap + 2 indexes) ≈ 4,200 dirty pages/second. The bgwriter is 8× too slow. Foreground backends must write their own dirty pages on every buffer eviction, adding 1ms (one random I/O) to each eviction.

**Fix:**
```sql
-- Increase bgwriter aggressiveness (apply immediately without restart)
ALTER SYSTEM SET bgwriter_lru_maxpages = 400;    -- 4× increase
ALTER SYSTEM SET bgwriter_delay = 50;            -- 4× more frequent
ALTER SYSTEM SET bgwriter_lru_multiplier = 4.0;  -- Clean ahead more aggressively
SELECT pg_reload_conf();

-- Also increase checkpoint frequency to flush dirty pages more often
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET checkpoint_timeout = '2min';  -- More frequent checkpoints
SELECT pg_reload_conf();

-- Long-term: separate the batch job to off-peak hours or a replica
```

---

## 9. Performance and Tuning

### Buffer Pool Sizing

```
Sizing goal: hot working set fits in shared_buffers

Hot working set = all pages accessed more than once in the access interval
  - For OLTP: indexes + recently accessed rows (often much smaller than table size)
  - For OLAP: full table (may be larger than RAM)

Postgres sizing formula:
  shared_buffers = min(hot_working_set, 0.4 × total_RAM)

Find hot working set size:
  SELECT pg_size_pretty(count(*) * 8192)
  FROM pg_buffercache
  WHERE usagecount >= 3;   -- Pages with ≥3 accesses = "hot"

If shared_buffers < hot_working_set → hit rate < 99% → increase shared_buffers
If shared_buffers > hot_working_set → cache is underutilized → decrease shared_buffers
                                       (free RAM for OS page cache)

PgBouncer / connection poolers:
  Each Postgres backend uses ~10MB RAM (stack + local buffers).
  1000 connections × 10MB = 10GB used for connections alone.
  Use connection pooling to reduce backend count, freeing RAM for shared_buffers.
```

### Key Buffer Pool Parameters

```sql
-- ── Postgres ──────────────────────────────────────────────────────────────────
ALTER SYSTEM SET shared_buffers         = '8GB';    -- Main cache (25-40% of RAM)
ALTER SYSTEM SET effective_cache_size   = '24GB';   -- Planner estimate of OS + DB cache
ALTER SYSTEM SET bgwriter_delay         = '100ms';  -- BGWriter frequency
ALTER SYSTEM SET bgwriter_lru_maxpages  = 200;      -- Max dirty pages per BGWriter round
ALTER SYSTEM SET bgwriter_lru_multiplier= 3.0;      -- Lookahead multiplier

-- ── MySQL InnoDB ──────────────────────────────────────────────────────────────
-- innodb_buffer_pool_size = 70-80% of RAM (MySQL manages OS page cache less)
-- innodb_buffer_pool_instances = 8 (8 parallel buffer pool instances)
-- innodb_doublewrite = ON (always in production)
-- innodb_lru_scan_depth = 1024 (pages scanned per page cleaner thread)
-- innodb_page_cleaners = 4 (parallel page cleaner threads)

-- Monitor MySQL buffer pool:
SELECT
    pool_id,
    pool_size,
    free_buffers,
    database_pages,
    modified_database_pages,   -- Dirty pages
    pages_read,
    pages_created,
    pages_written
FROM information_schema.innodb_buffer_pool_stats;
```

---

## 10. Interview Q&A

**Q1: What is a buffer pool and why do databases implement their own instead of relying on the OS page cache?**

A buffer pool is an in-memory cache of database pages (typically 8-16KB each) that sits between the database engine and the disk. When the database needs a page, it checks the buffer pool first; if the page is there, no disk I/O is needed. If not, the page is read from disk into the buffer pool, possibly evicting another page.

Databases implement their own buffer pool rather than relying on the OS page cache for four reasons. First, eviction control: the OS page cache uses a generic eviction algorithm (LRU-2 variant) that doesn't understand database access patterns. The database buffer pool can use workload-specific policies — like the buffer ring for sequential scans (preventing scan pollution) or LRU-K for OLTP workloads. Second, dirty page write ordering: the database must enforce the write-ahead constraint — a dirty page cannot be written to disk before its WAL records are fsynced. The OS would flush dirty pages on its own schedule, potentially violating this invariant. Third, prefetching control: a database knows from the query plan that a sequential scan is coming and can prefetch the next N pages. The OS has no such forward knowledge. Fourth, torn page protection: the double-write buffer (MySQL) or full-page writes (Postgres) require the database to control exactly when and how pages are written to disk.

**Q2: How does clock sweep eviction differ from pure LRU, and why is it better for database workloads?**

Pure LRU maintains a strict "most recently used" ordering. Every page access moves that page to the front of the LRU list; eviction removes from the back. For typical OLTP access patterns (repeatedly accessing the same hot pages), LRU works well. But it fails catastrophically for sequential scans: a full-table scan reads every page exactly once in order, moving each page to the front of the LRU list and evicting the hot OLTP pages that were at the back. After the scan, the entire LRU list contains "cold" scan pages that will never be accessed again.

Clock sweep addresses this with a usage counter per page. Hot pages (accessed many times) have high usage counts (1-5) and survive multiple sweeps of the clock. Sequential scan pages are accessed once, get usage_count = 1, and are evicted after the first sweep of the clock hand reaches them — they don't push hot pages out. The clock sweep is also O(1) per eviction decision (unlike LRU's pointer manipulations) and has no per-access lock contention on the LRU list. Postgres uses usage_count from 0-5, giving hot pages up to 5 "second chances" before eviction. This is not perfect (unlike the theoretical CLOCK-Pro or LFU algorithms), but it's fast, simple, and correct for the mix of OLTP and scan workloads databases encounter.

---

## 11. Cross-Question Chain

**Interviewer:** A Postgres database's buffer pool hit rate drops from 99.5% to 92% after a schema migration adds 3 new indexes to a large table. Why?

**Candidate:** Three new indexes mean three new B-tree structures that need to be maintained and cached. For every INSERT, UPDATE, or range query on the indexed columns, Postgres now reads index pages from those three indexes. Each index's root, internal nodes, and leaf pages must be loaded into the buffer pool. If the buffer pool is sized to hold the original working set (table data + original indexes), the three new indexes may push other hot pages out. The hot working set grew; the buffer pool size didn't. At 92% hit rate, 8% of page accesses are going to disk — a 16× increase in disk I/O for those operations compared to 0.5% miss rate.

**Interviewer:** How would you determine which of the new indexes is consuming the most buffer pool space?

**Candidate:** `pg_buffercache` with a join to `pg_class` shows current buffer pool occupancy by relation:
```sql
SELECT c.relname, count(*) AS buffers, pg_size_pretty(count(*) * 8192) AS cached
FROM pg_buffercache bc
JOIN pg_class c ON c.relfilenode = bc.relfilenode
GROUP BY c.relname ORDER BY buffers DESC LIMIT 20;
```
The output shows exactly which tables and indexes are in the pool and how many pages each occupies. If one of the new indexes appears with 2,000 buffers (16MB), that's the culprit.

**Interviewer:** The hit rate problem persists even after increasing `shared_buffers` from 8GB to 16GB. What else could explain it?

**Candidate:** Increasing `shared_buffers` helps only if the working set fits in the new size. If the hot working set is 20GB, 16GB is still insufficient. But there are other explanations: (1) `effective_cache_size` may be set too low — this is a planner hint, not a real cache — but it doesn't affect buffer pool size or eviction. (2) A concurrent analytics query (full table scan) may be polluting the buffer pool; if the table is larger than `shared_buffers / 4`, Postgres doesn't use the ring buffer and evicts hot pages. (3) High buffer pool contention with multiple database instances on the same server — if another Postgres instance or other process is consuming RAM, the OS is paging Postgres's shared_buffers under memory pressure. I'd check `vmstat -s` for page-ins/page-outs and `free -h` to verify actual available RAM.

**Interviewer:** You see `buffers_backend = 50,000` in `pg_stat_bgwriter` over the last minute. What's happening and how do you fix it?

**Candidate:** `buffers_backend` counts pages written to disk by foreground query backends (as opposed to bgwriter or checkpoint). When a backend needs to evict a dirty page from the buffer pool, it must write that page to disk before it can reuse the slot. This means the query that triggered the eviction is blocked waiting for a dirty page flush — typically 1ms on SSD, 10ms on HDD. 50,000 backend writes/minute = 833/second. If each write causes 1ms of additional backend latency, that's 833ms of extra latency distributed across all queries that triggered evictions.

The fix is to make the bgwriter more aggressive so it cleans dirty pages before backends need to evict them: increase `bgwriter_lru_maxpages` and reduce `bgwriter_delay`. If the problem is checkpoint-related (checkpoint I/O starving bgwriter), increase `checkpoint_timeout` or `max_wal_size` to reduce checkpoint frequency. Monitor whether `buffers_backend` drops after the change.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the buffer pool and what problem does it solve? | An in-memory cache of database pages (8-16KB each) that serves most page requests from RAM (100ns) rather than disk (100μs). Bridges the 1000× latency gap between RAM and SSD. |
| 2 | What is the cache hit rate formula and why does it matter? | hit_rate = hits / (hits + misses). At 99% hit rate: effective read latency ≈ 1.1μs. At 90%: ≈ 10μs. At 80%: ≈ 20μs. Going from 99% to 90% multiplies effective latency 10×. Most critical buffer pool metric. |
| 3 | Why do databases implement their own buffer pool instead of using the OS page cache? | (1) Custom eviction policies (sequential scan bypass), (2) enforce write-ahead constraint (dirty page flush only after WAL fsync), (3) prefetching control, (4) torn page protection (double-write buffer or FPW). |
| 4 | What is a buffer descriptor? | Per-slot metadata: page_id, dirty flag, pin_count, usage_count (for clock sweep), LSN of last modification, io_in_progress flag. Tracks the state of each buffer pool slot. |
| 5 | What is page pinning and why is it necessary? | Pinning marks a page as "in use" by a query (pin_count > 0). Pinned pages cannot be evicted. Without pinning, the buffer pool could evict a page while a transaction is reading/writing it, giving the transaction stale or garbage data. |
| 6 | What is the clock sweep eviction algorithm? | A circular scan of buffer slots with a usage_count per slot. On access: increment usage_count (up to max). On sweep: if usage_count > 0, decrement and skip; if usage_count == 0 and not pinned, evict. Hot pages (high count) survive many sweeps; cold pages (count=0) are evicted quickly. |
| 7 | Why is LRU a poor eviction policy for sequential scans? | Sequential scans access each page once in order, placing them all at the head of the LRU list and evicting hot OLTP pages to the tail. After the scan, the pool is full of scan pages that will never be accessed again. Clock sweep avoids this: scan pages get usage_count=1 and are quickly evicted on the next sweep without pushing hot pages out. |
| 8 | What is the write-ahead constraint on buffer pool dirty page flushing? | Before writing a dirty page to disk, the WAL must have been fsynced to at least the page's LSN. If a dirty page were flushed before its WAL record, a crash could produce data on disk that the WAL cannot replay — corruption. |
| 9 | What is the bgwriter and what does `buffers_backend` indicate? | The bgwriter is a background process that proactively flushes dirty pages to keep clean buffers available. `buffers_backend > 0` means foreground backends wrote dirty pages themselves — because the bgwriter was too slow. This adds direct query latency. |
| 10 | What is the double-write buffer in MySQL InnoDB? | Before writing a dirty page to its actual location, InnoDB writes it to a sequential doublewrite buffer file and fsyncs. If a crash occurs during the actual write (torn page), InnoDB recovers the clean page from the doublewrite buffer. Costs ≈5-10% extra write overhead. |
| 11 | What is the Postgres buffer ring for sequential scans? | A small private ring buffer (256KB ≈ 32 pages) used by sequential scans instead of the main buffer pool. Scan pages cycle through the ring and don't enter the main pool — preventing scan pollution. Used only for tables larger than `shared_buffers / 4`. |
| 12 | What does `pg_buffercache` show? | Current contents of the Postgres buffer pool: which pages (file, page number) are cached, their usage_count, whether they're dirty, and pin count. Used to diagnose buffer pool composition and identify which tables/indexes dominate the cache. |
| 13 | How do you size `shared_buffers` in Postgres? | General rule: 25-40% of RAM, leaving 60-75% for the OS page cache (which acts as a second-level cache). More precisely: match the hot working set size (pages with high `usagecount` in `pg_buffercache`). Too large = starves OS page cache; too small = low hit rate. |
| 14 | What happens during a checkpoint? | The checkpoint process flushes ALL dirty pages in the buffer pool to disk (after ensuring WAL is flushed). Writes a CHECKPOINT record to WAL. Recovery only needs to replay WAL from the checkpoint's redo LSN forward. Expensive I/O burst; spread with `checkpoint_completion_target = 0.9`. |
| 15 | What is `effective_cache_size` in Postgres? | A planner hint (not an actual cache): tells the query planner how much total memory is available for caching (shared_buffers + OS page cache). Affects query plan choices (index scan vs sequential scan). Does not control actual memory allocation. |

---

## 13. Further Reading

- **Postgres documentation — "Resource Consumption" chapter:** Covers `shared_buffers`, bgwriter, checkpoint tuning with concrete parameter guidance.
- **"Database Internals" by Alex Petrov, Chapter 5 (Buffer Management):** Deep treatment of eviction algorithms (LRU-K, 2Q, CLOCK-Pro) with complexity analysis.
- **InnoDB documentation — "InnoDB Buffer Pool" section:** MySQL's documentation on `innodb_buffer_pool_size`, `innodb_buffer_pool_instances`, and the doublewrite buffer.
- **`pg_buffercache` extension documentation:** How to use `pg_buffercache` for buffer pool introspection in Postgres.
- **"pgtune" tool (pgtune.leopard.in.ua):** Web-based Postgres parameter tuner that accounts for RAM, disk type, and workload type. A good starting point for `shared_buffers`, bgwriter, and checkpoint parameters.

---

## 14. Lab Exercises

**Exercise 1: Hit Rate vs Pool Size**
Run `buffer_pool.py` with `simulate_oltp()`. Vary `pool_size` from 4 to 128 while keeping `n_hot_pages = 20`. Plot hit rate vs pool size. At what `pool_size` does hit rate reach 95%? 99%? Explain why the curve is non-linear.

**Exercise 2: Sequential Scan Pollution**
Run `simulate_oltp()` with a 64-slot pool for 2000 requests (hit rate should be ~99%). Then run `simulate_sequential_scan()` for 200 pages. Check hit rate after the scan. What happened? Now add a "ring buffer" implementation: pages fetched with `sequential=True` use a separate 32-slot pool that doesn't affect the main pool. Repeat and compare hit rates.

**Exercise 3: Write-Ahead Constraint**
Extend `buffer_pool.py` to track the WAL LSN explicitly. Write pages with LSN = 1, 2, ..., 100. Try to flush pages with LSN > 50 when `wal_flush_lsn = 50`. Verify the constraint is enforced. Then advance `wal_flush_lsn` to 100 and flush all pages. Verify the dirty count drops to 0.

**Exercise 4: BGWriter Simulation**
Fill the buffer pool with 30 dirty pages. Without BGWriter: add one more page (triggers eviction + backend dirty write). With BGWriter: call `bgwriter_round()` first to pre-clean pages, then add the new page (clean eviction, no backend flush). Measure the difference in `stats.dirty_writes` from backends vs bgwriter.

---

## 15. Key Takeaways

The buffer pool is the single most impactful performance component in a database. Every percentage point of cache hit rate has exponential impact on effective query latency: going from 99% to 90% hit rate multiplies effective I/O latency 10×. Sizing `shared_buffers` to match the hot working set is the single most impactful Postgres tuning parameter.

Clock sweep eviction gives hot pages (high usage_count) protection from sequential scan pollution while maintaining O(1) per-access cost. The bgwriter proactively cleans dirty pages so foreground queries never stall on dirty page evictions — monitoring `buffers_backend` is the canary that detects when bgwriter is falling behind.

The write-ahead constraint — only flush a dirty page after WAL is fsynced to the page's LSN — is the invariant that connects the buffer pool (M50) to the WAL (M49). Violating it produces corrupted data on crash. The double-write buffer (MySQL) and full-page writes (Postgres) add the second layer of protection against torn pages.

These mechanisms together — clock sweep, bgwriter, write-ahead constraint, torn page protection — make the buffer pool correct and performant under concurrent access, dirty write storms, and crash conditions.

---

## 16. Connections to Other Modules

- **M47 — B-Tree Storage Engines:** B-tree pages live in the buffer pool. The root and upper-level internal nodes should be permanently cached (they are hot with high usage_count). Leaf pages have lower usage_count (accessed less frequently). B-tree performance is directly proportional to buffer pool hit rate for the index pages.
- **M49 — Write-Ahead Log:** The buffer pool and WAL are tightly coupled through the write-ahead constraint: the buffer pool cannot flush a dirty page before the WAL is fsynced to the page's LSN. The WAL's checkpoint mechanism triggers the buffer pool to flush all dirty pages. The buffer pool's `lsn` field per page is the synchronization point.
- **M48 — LSM-Tree Storage Engines:** LSM-trees have their own block cache (RocksDB's "block cache") that is functionally equivalent to the buffer pool — it caches decompressed SSTable blocks in memory. The same hit rate math applies: block cache size must match the hot working set.
- **M51 — Column-Oriented Storage:** Parquet files (covered next) use a different caching model: file-level OS page cache rather than a database buffer pool. Understanding buffer pool mechanics explains why columnar databases handle caching differently from row-oriented databases.

---

## 17. Anti-Patterns

**Anti-pattern: Assuming more RAM always means higher buffer pool performance.** Adding RAM to a server does NOT automatically improve buffer pool hit rate unless `shared_buffers` is also increased (Postgres) or `innodb_buffer_pool_size` (MySQL). The database uses only the configured amount. Many teams add RAM to a database server and wonder why performance didn't improve — because the database's buffer pool size didn't change. Always tune the buffer pool parameter after adding RAM.

**Anti-pattern: Using very long-running transactions that pin many pages.** A transaction that holds an open cursor or performs a long multi-page scan keeps all touched pages pinned until the transaction completes. In a heavily-loaded system, many long-running transactions can pin so many pages that the buffer pool has no evictable pages — causing a "pin stall" where new page requests cannot find a victim. Monitor `pg_stat_activity` for long-running transactions and set `statement_timeout` and `idle_in_transaction_session_timeout` to prevent runaway transactions from holding pins indefinitely.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pg_buffercache` (extension) | Inspect buffer pool contents | Which pages, usage counts, dirty flag |
| `pg_stat_bgwriter` | BGWriter and checkpoint statistics | `buffers_backend`, `checkpoints_req` |
| `pg_statio_user_tables` | Per-table cache hit rate | `heap_blks_hit / (hit + read)` |
| `pg_prewarm` (extension) | Warm buffer pool pages on startup | Pre-load hot indexes after restart |
| `EXPLAIN (ANALYZE, BUFFERS)` | Per-query buffer usage | `Buffers: shared hit=X read=Y` |
| `buffer_pool.py` | Buffer pool simulation | Clock sweep, WAL constraint, DWB |
| MySQL `SHOW ENGINE INNODB STATUS` | InnoDB buffer pool stats | Hit rate, dirty pages, doublewrite |

---

## 19. Glossary

**bgwriter:** Postgres background process that proactively writes dirty pages from the buffer pool to disk. Prevents foreground backends from having to write dirty pages during query execution.

**Buffer descriptor:** Per-slot metadata in the buffer pool tracking page identity, dirty flag, pin count, usage count, and LSN of last modification.

**Buffer pool:** An in-memory cache of database pages. Serves most page requests from RAM (fast) instead of disk (slow). The primary performance lever for OLTP databases.

**Cache hit rate:** Fraction of page requests served from the buffer pool (no disk I/O). At 99% hit rate, effective latency is dominated by RAM access (~100ns). Below 95%, disk I/O dominates.

**Checkpoint:** A buffer pool operation that flushes all dirty pages to disk, synchronized with a WAL checkpoint record. Bounds crash recovery time.

**Clock sweep:** Postgres's buffer pool eviction algorithm. Each slot has a usage_count; hot pages survive multiple sweeps; cold pages are evicted after their usage_count reaches 0.

**Dirty page:** A buffer pool page that has been modified in memory but not yet written to disk.

**Double-write buffer:** MySQL InnoDB's torn page protection: pages are written to a sequential DWB file and fsynced before being written to their actual random location. Recoverable from DWB if actual write is torn.

**Effective cache size:** A Postgres planner hint (not a real allocation) indicating total available cache memory (shared_buffers + OS page cache). Influences query plan choices.

**Page pinning:** Incrementing a page's pin_count to signal active use. Pinned pages cannot be evicted. Decremented (unpinned) when the caller is done with the page.

**Write-ahead constraint:** A dirty buffer pool page cannot be written to disk before the WAL has been fsynced to at least the page's LSN. Enforces WAL-first durability.

---

## 20. Self-Assessment

1. Why does the effective read latency formula use a weighted average of RAM and disk latency? What does it imply for the cost of going from 99% to 98% hit rate?
2. In `buffer_pool.py`, why is `usage_count` bounded at `MAX_USAGE_COUNT = 5` rather than allowing it to grow unboundedly?
3. A table has 100,000 pages. `shared_buffers` can hold 10,000 pages. A query does a full table scan, then an OLTP query accesses 500 hot pages. What is the expected cache hit rate for the OLTP query immediately after the scan? Without ring buffer? With ring buffer?
4. Explain the double-write buffer protocol step by step. At which step is a crash safe? At which step does a crash require DWB recovery?
5. What does `buffers_backend = 0` in `pg_stat_bgwriter` indicate about the bgwriter's effectiveness?
6. Run `buffer_pool.py`. Increase the number of write operations to force many evictions. Observe whether the write-ahead constraint is triggered. What happens if you set `wal_flush_lsn = 2**62` (always allowing flushes)?
7. You need to add 6 indexes to a 100GB table. After adding them, the `pg_buffercache` query shows those indexes consume 4GB of the 8GB buffer pool. What options do you have to improve the situation?
8. Why does MySQL have a `doublewrite buffer` while Postgres uses `full_page_writes`? What are the tradeoffs between the two approaches?

---

## 21. Module Summary

The buffer pool is the centerpiece of database I/O performance. Its function is simple — cache disk pages in RAM — but its implementation is a precise engineering of eviction policy, dirty page management, concurrency safety (pin counts), and crash safety (write-ahead constraint + torn page protection).

The implementation — `BufferPool` with `BufferDescriptor` array, clock sweep eviction, `DiskStore` for simulated I/O, `DoubleWriteBuffer` for torn page protection, and `bgwriter_round()` for proactive dirty page flushing — makes every mechanism explicit. The write-ahead constraint enforcement (`if desc.lsn > wal_flush_lsn: raise RuntimeError`) makes the WAL-buffer pool coupling concrete.

Production knowledge — sizing `shared_buffers`, monitoring `buffers_backend`, responding to sequential scan pollution, tuning the bgwriter — is directly derived from understanding these internals. The hit rate math (99% vs 90% = 10× latency difference) explains why buffer pool sizing is the most impactful database tuning decision.

The final module of DBE-INT-101 — M51: Column-Oriented Storage — shifts from the row-page model that B-trees, buffer pools, and WALs are built on to the columnar storage model used by Parquet, BigQuery, and OLAP databases. The same principle (design storage for the I/O pattern) produces a completely different architecture when the dominant operation is aggregating columns across millions of rows rather than accessing individual rows by primary key.
