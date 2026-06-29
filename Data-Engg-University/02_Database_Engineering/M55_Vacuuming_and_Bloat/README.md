# M55: Vacuuming and Bloat

**Course:** DBE-INT-102 — Postgres Architecture  
**Module:** 04 of 05  
**Global Module ID:** M55  
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

Every UPDATE in Postgres creates a new tuple version; the old version is not overwritten in place — it is left on disk and marked as dead. Every DELETE similarly leaves a dead tuple on disk. These dead tuples accumulate silently. They waste disk space. They slow sequential scans (the executor must read and skip them). They prevent the planner from using index-only scans (the visibility map page flags are not set until VACUUM clears the dead tuples). Left long enough, they cause a Postgres emergency: transaction ID wraparound.

Transaction ID (xid) wraparound is not a performance problem. It is a correctness problem. Postgres uses 32-bit xids — a finite space of 4 billion transaction IDs. When the database approaches the 2-billion xid mark from the oldest non-frozen xid on any live tuple, Postgres begins refusing all writes with `ERROR: database is not accepting commands to avoid wraparound data loss`. The only recovery from a wraparound emergency is a `VACUUM FREEZE` that can take hours on large databases, during which the database remains read-only.

VACUUM is the process that prevents all of this: it reclaims dead tuples, updates the visibility map (enabling index-only scans), advances the frozen xid horizon, and updates planner statistics. Autovacuum runs VACUUM automatically based on dead tuple thresholds — but it can fall behind on high-write tables, and its defaults are calibrated for small-to-medium tables. Understanding when and why autovacuum falls behind, and how to fix it, is a core data engineering competency.

---

## 2. Mental Model

### Postgres's "Mark and Don't Compact" Approach

Most databases that need to reclaim deleted data either compact in place (move live rows to fill gaps left by deleted rows, rewriting the entire page) or use a separate compaction pass (like LSM compaction from M48). Postgres does neither for individual deletes. It marks deleted rows' xmax field with the deleting transaction's xid and leaves them in place. Dead rows are not reclaimed until VACUUM runs. VACUUM marks dead tuple slots as reusable; subsequent inserts can use those slots. But VACUUM does not compact the file — it does not shrink the physical file size by default. The file can only shrink via `VACUUM FULL` (which rewrites the entire table and requires an exclusive lock) or `pg_repack` (which does the same without the exclusive lock).

### The MVCC Visibility Window

Why can't Postgres immediately reclaim a deleted tuple? Because another concurrent transaction might be reading it. MVCC (Multi-Version Concurrency Control) gives each transaction a consistent snapshot of the database at the moment the transaction started. A row deleted by transaction T1 is still visible to transaction T2 that started before T1 committed. The deleted row must remain on disk until no active transaction's snapshot could possibly see it — specifically, until its xmax is older than the oldest active transaction's xmin across the entire cluster.

This is the `oldest_active_xmin` horizon: the oldest transaction xmin currently held by any active backend process (tracked via the procarray from M52). Dead tuples whose xmax is older than this horizon are invisible to all current and future transactions and can be safely reclaimed by VACUUM.

### Transaction ID Space and Freezing

Postgres's 32-bit xid space is modular: after 4 billion xids, it wraps around to 0. Postgres uses a convention: any xid that is "more than 2 billion in the past" is considered "in the future" — its data is invisible. This prevents xids from wrapping around into the visible range and making old data appear to be from a future transaction.

To prevent old tuples from being interpreted as "from the future" after wraparound, VACUUM replaces their actual xid with a special **frozen xid** marker (`FrozenTransactionId = 2`). Frozen tuples are always visible to all transactions — they are "infinitely old." Once a tuple is frozen, its xid no longer participates in the wraparound calculation.

The **vacuum_freeze_min_age** (default 50 million xids) controls when a tuple becomes eligible for freezing: once its xid is more than 50M xids old, VACUUM can freeze it. The **vacuum_freeze_table_age** (default 150 million xids) triggers an aggressive freeze pass on the entire table. The **autovacuum_freeze_max_age** (default 200 million xids) triggers emergency autovacuum regardless of dead tuple count — purely to prevent wraparound.

---

## 3. Core Concepts

### 3.1 Tuple Header: xmin, xmax, and Visibility

Every Postgres tuple (row version) has a 23-byte header containing two key fields:

- **xmin:** The xid of the transaction that created this tuple version. The tuple is visible only to transactions whose snapshot includes xmin as committed.
- **xmax:** The xid of the transaction that deleted (or updated, creating a new version) this tuple. When xmax = 0, the tuple has not been deleted. When xmax is set, the tuple is visible only to transactions whose snapshot predates xmax's commit.

```
Tuple header (23 bytes):
  t_xmin    (4 bytes): xid that created this version
  t_xmax    (4 bytes): xid that deleted/updated this version (0 if live)
  t_cid     (4 bytes): command ID within the transaction
  t_ctid    (6 bytes): TID of the newer version (for HOT chains)
  t_infomask (2 bytes): flags: XMIN_COMMITTED, XMAX_COMMITTED, IS_FROZEN, etc.
  t_infomask2 (2 bytes): column count, HOT-updated flag
  t_hoff    (1 byte): offset to actual data
```

An **UPDATE** in Postgres:
1. Marks the old tuple's `t_xmax` = current xid (making it "dead" once the transaction commits)
2. Inserts a new tuple with `t_xmin` = current xid and `t_ctid` pointing to the new version

This is why Postgres updates are more expensive than updates in some other databases: every UPDATE writes a full new row version.

### 3.2 What VACUUM Does

VACUUM performs several distinct operations in a single pass:

**1. Dead Tuple Reclamation:**
Scans all pages in the table. For each dead tuple (xmax committed and older than oldest_active_xmin): marks the tuple slot as free in the page's ItemIdArray. Updates `FSM` (Free Space Map) to record available space on this page. Future INSERTs can reuse these slots.

**2. Visibility Map Update:**
If all tuples on a page are visible to all current and future transactions (no live transactions with xmin newer than the page's newest xmin), sets the "all-visible" bit in the visibility map. This enables index-only scans for queries on this page — the executor can return values from the index without checking the heap.

**3. Transaction ID Freezing:**
For tuples older than `vacuum_freeze_min_age` xids, replaces their xmin with the frozen xid marker (2). Updates `pg_class.relfrozenxid` with the new oldest non-frozen xid for the table.

**4. Statistics Update (if `VACUUM ANALYZE`):**
Runs `ANALYZE` to update `pg_statistic` with current row counts and value distributions.

**5. Free Space Map Update:**
Records available free space per page so that INSERTs can find space without scanning from the beginning of the file.

### 3.3 VACUUM vs VACUUM FULL

| Feature | VACUUM | VACUUM FULL |
|---|---|---|
| Reclaims dead tuples | Yes | Yes |
| Returns space to OS | No (space is reusable within table) | Yes (rewrites entire file) |
| Requires exclusive lock | No (concurrent reads/writes allowed) | Yes (blocks ALL access) |
| Duration | Fast (only processes pages with dead tuples) | Slow (full table rewrite) |
| Index handling | Scans indexes to remove dead entries | Rebuilds all indexes |
| Use case | Routine maintenance | Emergency bloat recovery after massive deletes |

For routine production maintenance, only `VACUUM` (or autovacuum) should be used. `VACUUM FULL` is appropriate only in specific cases: after deleting more than 50% of a table, when the table has become so bloated that the OS file size matters (billing, backup cost), or when `pg_repack` (a non-locking alternative) is not available.

### 3.4 Autovacuum

Autovacuum is the Postgres background daemon that runs VACUUM automatically when dead tuple thresholds are exceeded. It consists of:
- **Autovacuum launcher:** Monitors all tables in all databases every `autovacuum_naptime` (1 minute default). When a table's dead tuple count exceeds the threshold, schedules an autovacuum worker.
- **Autovacuum workers:** Perform VACUUM on scheduled tables. Limited to `autovacuum_max_workers` (3 default) simultaneously across all databases.

**Autovacuum trigger threshold:**
```
trigger = autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × n_live_tup
        = 50 + 0.20 × n_live_tup
```

For a 1,000,000-row table: threshold = 50 + 200,000 = 200,000 dead tuples before autovacuum triggers. This is appropriate for medium tables but is too lenient for large tables — on a 100M-row table, autovacuum waits for 20M dead tuples, meaning 20M rows of bloat before cleanup begins.

**Autovacuum cost throttling:** Autovacuum intentionally limits its I/O to avoid impacting foreground queries. Controlled by `autovacuum_vacuum_cost_delay` (2ms default) and `autovacuum_vacuum_cost_limit` (200 default). The worker pauses `cost_delay` milliseconds after accumulating `cost_limit` cost points. This throttling is why autovacuum can fall behind on high-write tables — the table accumulates dead tuples faster than the throttled autovacuum can remove them.

### 3.5 Table Bloat

Table bloat is the accumulation of wasted space within the table file — space occupied by dead tuples or free space from previously dead tuples that has not been reused. Bloat manifests as:

- **Dead tuple bloat:** Unreclaimed dead tuples. Causes: autovacuum falling behind, or `idle in transaction` connections holding old xmin snapshots that prevent VACUUM from reclaiming dead tuples.
- **Free space bloat:** Free space on pages (after VACUUM ran) not yet refilled by new INSERTs. Happens when the table's write rate is lower than the vacuum reclaim rate, or when `fillfactor < 100` was set intentionally.
- **Table file size bloat:** The physical file on disk is larger than the amount of live data. VACUUM marks space as free but does not shrink the file. Only `VACUUM FULL` / `pg_repack` shrink the file.

**Estimating bloat:**
```sql
-- Quick bloat estimate using pg_stat_user_tables
SELECT
    schemaname, relname,
    n_live_tup,
    n_dead_tup,
    round(n_dead_tup::numeric / nullif(n_live_tup + n_dead_tup, 0) * 100, 2) AS dead_pct,
    pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) AS table_size
FROM pg_stat_user_tables
WHERE n_dead_tup > 10000
ORDER BY n_dead_tup DESC;

-- More accurate bloat using pg_relation_size and expected size:
SELECT
    relname,
    pg_size_pretty(pg_relation_size(oid)) AS file_size,
    pg_size_pretty(n_live_tup * 200) AS estimated_live_size,  -- ~200 bytes/row (adjust!)
    pg_size_pretty(pg_relation_size(oid) - n_live_tup * 200) AS estimated_bloat
FROM pg_class
JOIN pg_stat_user_tables ON relid = pg_class.oid
WHERE relkind = 'r'
ORDER BY pg_relation_size(oid) DESC;
```

### 3.6 Transaction ID Wraparound: The Emergency

The xid space is 4 billion (2^32). Postgres reserves xids 0, 1, and 2 for special purposes (invalid, bootstrap, frozen). The usable space is about 4 billion. But Postgres uses a modular arithmetic convention: any xid that is more than 2^31 (2.1 billion) xids in the past is considered "in the future" and invisible. This means there are effectively only 2 billion usable xids at any time — transactions can only be distinguished from each other across a window of 2 billion xids.

When the gap between the oldest non-frozen xid (`pg_database.datfrozenxid`) and the current xid (`txid_current()`) approaches 2 billion, Postgres enters protective mode:

- At `vacuum_freeze_table_age` (150M xids): autovacuum aggressively freezes all tables
- At `autovacuum_freeze_max_age` (200M xids): emergency autovacuum triggers regardless of dead tuple count
- At 1.5 billion xids: Postgres logs `WARNING: database "X" must be vacuumed within Y transactions to avoid wraparound`
- At 2 billion xids: Postgres refuses all writes: `ERROR: database is not accepting commands to avoid wraparound data loss in database "X"`

The only fix when this triggers: shut down applications that generate new transactions, run `VACUUM FREEZE` on all tables (potentially hours for large databases), and restart Postgres. This is a production emergency with downtime.

Prevention: monitor `age(datfrozenxid)` for all databases and alert when it approaches 500M xids. Ensure autovacuum can keep up with the workload.

```sql
-- Monitor wraparound risk
SELECT
    datname,
    age(datfrozenxid)           AS xid_age,
    2000000000 - age(datfrozenxid) AS xids_remaining,
    pg_size_pretty(pg_database_size(datname)) AS db_size
FROM pg_database
WHERE datallowconn
ORDER BY xid_age DESC;

-- Per-table wraparound risk
SELECT
    relname,
    age(relfrozenxid)           AS table_xid_age,
    2000000000 - age(relfrozenxid) AS xids_remaining
FROM pg_class
WHERE relkind = 'r'
ORDER BY age(relfrozenxid) DESC
LIMIT 20;
```

---

## 4. Hands-On Walkthrough

### 4.1 Observing Bloat Accumulation and VACUUM

```sql
-- Create a table with high update rate
CREATE TABLE updates_test (id INT PRIMARY KEY, val TEXT);
INSERT INTO updates_test SELECT i, 'initial' FROM generate_series(1, 100000) i;

-- Check initial state
SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname = 'updates_test';
-- n_live_tup=100000, n_dead_tup=0

-- Simulate updates (each UPDATE creates a dead tuple)
UPDATE updates_test SET val = 'updated-' || id WHERE id <= 50000;
-- Check again (must wait for stats collector or use SELECT count(*))
SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname = 'updates_test';
-- Approximate: n_live_tup≈100000, n_dead_tup≈50000

-- Run VACUUM and check
VACUUM VERBOSE updates_test;
-- Output:
--   INFO:  vacuuming "public.updates_test"
--   INFO:  scanned index "updates_test_pkey" to remove 50000 row versions
--   INFO:  "updates_test": removed 50000 row versions in 277 pages
--   INFO:  index scan bypassed: 0 pages from table (0.0% of total) have 0 dead item identifiers
--   INFO:  "updates_test": found 50000 removable, 100000 nonremovable row versions
--       in 555 out of 555 pages

SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname = 'updates_test';
-- n_live_tup≈100000, n_dead_tup≈0
```

### 4.2 Autovacuum Configuration for High-Write Tables

```sql
-- Check autovacuum thresholds for a specific table
SELECT
    relname,
    current_setting('autovacuum_vacuum_threshold')  AS av_threshold,
    current_setting('autovacuum_vacuum_scale_factor') AS av_scale,
    n_live_tup,
    (current_setting('autovacuum_vacuum_threshold')::int
     + current_setting('autovacuum_vacuum_scale_factor')::float * n_live_tup)::int
        AS autovacuum_trigger_at
FROM pg_stat_user_tables
WHERE relname = 'orders';

-- Tune autovacuum for a specific high-write table
ALTER TABLE orders SET (
    autovacuum_vacuum_scale_factor = 0.01,    -- Trigger at 1% dead tuples (not 20%)
    autovacuum_vacuum_threshold = 100,        -- Plus 100 base tuples
    autovacuum_analyze_scale_factor = 0.005,  -- Analyze at 0.5% changes
    autovacuum_vacuum_cost_delay = 2,         -- 2ms pause between cost units (less throttling)
    autovacuum_vacuum_cost_limit = 400        -- 400 cost units per pause (more aggressive)
);
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
vacuum_simulator.py

Simulate Postgres MVCC, dead tuple accumulation, VACUUM, and XID wraparound.

Components:
  - TupleHeader: Simulates Postgres tuple xmin/xmax fields
  - HeapPage: Simulates an 8KB heap page with ItemIdArray
  - HeapTable: Collection of heap pages
  - MVCCEngine: Transaction snapshot management + tuple visibility
  - VacuumWorker: Simulates VACUUM dead tuple reclamation + xid freezing
  - AutovacuumLauncher: Monitors tables and triggers VacuumWorker
  - BloatTracker: Measures and reports table bloat over time
  - XIDWrapAroundMonitor: Tracks xid age and warns on wraparound risk

No external dependencies.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Optional


# ─── Constants ────────────────────────────────────────────────────────────────

FROZEN_XID           = 2              # Special frozen xid — always visible
INVALID_XID          = 0              # No xid
XID_SPACE            = 2**32         # Total xid space (4 billion)
WRAPAROUND_DANGER    = 1_500_000_000  # Alert threshold (1.5 billion xids behind)
WRAPAROUND_EMERGENCY = 2_000_000_000  # Emergency threshold (refuse writes)
PAGE_SIZE_BYTES      = 8192
TUPLES_PER_PAGE      = 30            # Approximate for demo (real ~30-200 depending on row size)


# ─── Tuple Header ─────────────────────────────────────────────────────────────

@dataclass
class TupleHeader:
    """
    Simulates a Postgres tuple header (23 bytes in production).
    Contains xmin, xmax, and visibility flags.
    """
    xmin:         int   = INVALID_XID  # Transaction that created this tuple
    xmax:         int   = INVALID_XID  # Transaction that deleted/updated (0 = live)
    ctid_page:    int   = 0            # (page, offset) of newer version for HOT chains
    ctid_offset:  int   = 0
    frozen:       bool  = False        # True if xmin has been frozen


@dataclass
class HeapTuple:
    """A row version in the heap."""
    header:   TupleHeader
    data:     dict
    slot_id:  int        # Position in page's ItemIdArray
    dead:     bool = False  # Set by VACUUM after reclamation decision


class HeapPage:
    """
    Simulates an 8KB Postgres heap page.
    Contains a fixed number of tuple slots (ItemIdArray + actual tuple data).
    """

    def __init__(self, page_id: int, capacity: int = TUPLES_PER_PAGE):
        self.page_id   = page_id
        self.capacity  = capacity
        self._slots:   list[Optional[HeapTuple]] = [None] * capacity
        self._free_slots: list[int] = list(range(capacity))
        self.all_visible: bool = False   # Visibility map bit

    def insert(self, header: TupleHeader, data: dict) -> Optional[int]:
        """Insert a new tuple; returns slot_id or None if page is full."""
        if not self._free_slots:
            return None
        slot_id = self._free_slots.pop(0)
        self._slots[slot_id] = HeapTuple(header, data, slot_id)
        self.all_visible = False   # Page modified, visibility map bit cleared
        return slot_id

    def get_tuple(self, slot_id: int) -> Optional[HeapTuple]:
        return self._slots[slot_id]

    def mark_dead(self, slot_id: int):
        """Mark a slot as available for reuse after VACUUM confirms reclaim."""
        if self._slots[slot_id] is not None:
            self._slots[slot_id].dead = True

    def reclaim_slot(self, slot_id: int):
        """VACUUM reclaims a dead slot — marks it as free for reuse."""
        if self._slots[slot_id] and self._slots[slot_id].dead:
            self._slots[slot_id] = None
            self._free_slots.append(slot_id)

    def live_tuples(self) -> list[HeapTuple]:
        return [t for t in self._slots if t is not None and not t.dead]

    def dead_tuples(self) -> list[HeapTuple]:
        return [t for t in self._slots if t is not None and t.dead]

    def free_slot_count(self) -> int:
        return len(self._free_slots)

    def live_count(self) -> int:
        return sum(1 for t in self._slots if t is not None and not t.dead)

    def dead_count(self) -> int:
        return sum(1 for t in self._slots if t is not None and t.dead)


class HeapTable:
    """Collection of heap pages. Grows as needed."""

    def __init__(self, name: str):
        self.name    = name
        self._pages: list[HeapPage] = [HeapPage(0)]
        self.relfrozenxid = 3     # Oldest non-frozen xid (starts just after bootstrap)

    def insert(self, header: TupleHeader, data: dict) -> tuple[int, int]:
        """Insert tuple into first page with free space. Returns (page_id, slot_id)."""
        for page in self._pages:
            slot_id = page.insert(header, data)
            if slot_id is not None:
                return page.page_id, slot_id
        # All pages full — allocate new page
        new_page = HeapPage(len(self._pages))
        self._pages.append(new_page)
        slot_id = new_page.insert(header, data)
        return new_page.page_id, slot_id

    def get_tuple(self, page_id: int, slot_id: int) -> Optional[HeapTuple]:
        if page_id < len(self._pages):
            return self._pages[page_id].get_tuple(slot_id)
        return None

    def total_pages(self) -> int:
        return len(self._pages)

    def total_live(self) -> int:
        return sum(p.live_count() for p in self._pages)

    def total_dead(self) -> int:
        return sum(p.dead_count() for p in self._pages)

    def dead_fraction(self) -> float:
        total = self.total_live() + self.total_dead()
        return self.total_dead() / max(1, total)

    def bloat_pct(self) -> float:
        return self.dead_fraction() * 100


# ─── MVCC Engine ──────────────────────────────────────────────────────────────

class MVCCSnapshot:
    """
    Represents an MVCC snapshot taken at transaction start.
    A row is visible if:
      xmin is committed AND xmin < snapshot_xmax AND xmin NOT IN active_xids
      AND (xmax = 0 OR xmax is in_progress OR xmax > snapshot_xmax OR xmax IN active_xids)
    """
    def __init__(self, snapshot_xmax: int, active_xids: set[int]):
        self.snapshot_xmax = snapshot_xmax
        self.active_xids   = active_xids

    def is_visible(self, tup: HeapTuple, committed_xids: set[int]) -> bool:
        """Return True if this tuple is visible in this snapshot."""
        header = tup.header

        # Frozen tuples are always visible
        if header.frozen or header.xmin == FROZEN_XID:
            created = True
        elif header.xmin in committed_xids:
            created = (header.xmin < self.snapshot_xmax
                       and header.xmin not in self.active_xids)
        else:
            created = False   # xmin not yet committed

        if not created:
            return False

        # Check deletion
        if header.xmax == INVALID_XID:
            return True  # Not deleted

        # Deleted by xmax — check if that deletion is visible to this snapshot
        if header.xmax in committed_xids:
            deleted = (header.xmax < self.snapshot_xmax
                       and header.xmax not in self.active_xids)
        else:
            deleted = False  # Deletion not yet committed

        return not deleted


class MVCCEngine:
    """
    Manages transaction IDs, snapshots, commits, and tuple visibility.
    """

    def __init__(self):
        self._next_xid: int       = 3        # 0=invalid, 1=bootstrap, 2=frozen
        self._committed: set[int] = set()
        self._active:   set[int]  = set()

    def begin(self) -> int:
        xid = self._next_xid
        self._next_xid += 1
        self._active.add(xid)
        return xid

    def commit(self, xid: int):
        self._active.discard(xid)
        self._committed.add(xid)

    def abort(self, xid: int):
        self._active.discard(xid)

    def snapshot(self) -> MVCCSnapshot:
        return MVCCSnapshot(self._next_xid, set(self._active))

    def oldest_active_xmin(self) -> int:
        if self._active:
            return min(self._active)
        return self._next_xid  # No active transactions: all xids are "in the past"

    def current_xid(self) -> int:
        return self._next_xid

    def xid_age(self, frozen_xid: int) -> int:
        """Age of a frozen xid = number of transactions since it was last frozen."""
        return max(0, self._next_xid - frozen_xid)


# ─── VACUUM Worker ────────────────────────────────────────────────────────────

class VacuumWorker:
    """
    Simulates the VACUUM process:
    - Scan all pages
    - Identify dead tuples (xmax committed and xmax < oldest_active_xmin)
    - Reclaim dead tuple slots
    - Update visibility map
    - Freeze old tuples (xmin age > vacuum_freeze_min_age)
    - Update relfrozenxid
    """

    FREEZE_MIN_AGE = 50_000_000   # Freeze tuples older than 50M xids (simulated as 50K)
    # (Using reduced values for demo; production defaults are 50M and 200M)
    FREEZE_MIN_AGE_DEMO = 50      # Use smaller number for demo

    def __init__(self, mvcc: MVCCEngine, verbose: bool = False):
        self.mvcc    = mvcc
        self.verbose = verbose
        self.stats   = {'runs': 0, 'tuples_removed': 0, 'pages_scanned': 0,
                        'tuples_frozen': 0}

    def vacuum(self, table: HeapTable, aggressive: bool = False) -> dict:
        """
        Run VACUUM on the table.
        aggressive=True: freeze all eligible tuples (full freeze pass).
        """
        oldest_xmin = self.mvcc.oldest_active_xmin()
        current_xid = self.mvcc.current_xid()
        removed, frozen, pages_scanned = 0, 0, 0
        new_relfrozenxid = current_xid  # Will be updated to actual oldest non-frozen

        for page in table._pages:
            pages_scanned += 1
            for slot_id, tup in enumerate(page._slots):
                if tup is None:
                    continue

                # ── Dead tuple reclamation ────────────────────────────────────
                if (tup.header.xmax != INVALID_XID
                        and tup.header.xmax in self.mvcc._committed
                        and tup.header.xmax < oldest_xmin):
                    page.mark_dead(slot_id)
                    page.reclaim_slot(slot_id)
                    removed += 1
                    continue

                # ── Tuple freezing ────────────────────────────────────────────
                if not tup.header.frozen:
                    xmin_age = current_xid - tup.header.xmin
                    if xmin_age > self.FREEZE_MIN_AGE_DEMO or aggressive:
                        tup.header.frozen = True
                        tup.header.xmin   = FROZEN_XID
                        frozen += 1
                    else:
                        # Track oldest non-frozen xid for relfrozenxid update
                        new_relfrozenxid = min(new_relfrozenxid, tup.header.xmin)

            # ── Visibility map update ─────────────────────────────────────────
            # Set all-visible if no dead tuples remain and no in-progress xmin
            if page.dead_count() == 0 and all(
                t.header.xmax == INVALID_XID or t.header.frozen
                for t in page._slots if t is not None
            ):
                page.all_visible = True

        table.relfrozenxid = max(2, new_relfrozenxid)
        self.stats['runs'] += 1
        self.stats['tuples_removed'] += removed
        self.stats['tuples_frozen'] += frozen
        self.stats['pages_scanned'] += pages_scanned

        result = {
            'pages_scanned':    pages_scanned,
            'tuples_removed':   removed,
            'tuples_frozen':    frozen,
            'new_relfrozenxid': table.relfrozenxid,
            'xid_age_after':    self.mvcc.xid_age(table.relfrozenxid),
        }

        if self.verbose:
            print(f"  VACUUM {table.name}: "
                  f"removed={removed}, frozen={frozen}, "
                  f"pages={pages_scanned}, "
                  f"relfrozenxid={table.relfrozenxid}")
        return result


# ─── Autovacuum Launcher ──────────────────────────────────────────────────────

class AutovacuumLauncher:
    """
    Monitors tables and triggers VACUUM when dead tuple threshold is exceeded.
    Simulates the autovacuum launcher + worker.
    """

    def __init__(self, mvcc: MVCCEngine,
                 scale_factor: float = 0.20,
                 threshold: int = 50,
                 max_workers: int = 3):
        self.mvcc        = mvcc
        self.scale_factor = scale_factor
        self.threshold   = threshold
        self.max_workers = max_workers
        self._tables:    list[HeapTable] = []
        self._worker     = VacuumWorker(mvcc, verbose=True)
        self.vacuums_run = 0

    def register(self, table: HeapTable):
        self._tables.append(table)

    def check_and_vacuum(self):
        """
        Check each table; trigger VACUUM if dead tuple count exceeds threshold.
        Returns number of vacuums triggered.
        """
        triggered = 0
        for table in self._tables:
            dead       = table.total_dead()
            live       = table.total_live()
            av_trigger = self.threshold + self.scale_factor * live

            # Check for wraparound risk (freeze threshold regardless of dead tuples)
            xid_age = self.mvcc.xid_age(table.relfrozenxid)
            force_aggressive = xid_age > 150  # Simulated autovacuum_freeze_max_age

            if dead >= av_trigger or force_aggressive:
                aggressive = force_aggressive
                self._worker.vacuum(table, aggressive=aggressive)
                self.vacuums_run += 1
                triggered += 1

        return triggered


# ─── XID Wraparound Monitor ───────────────────────────────────────────────────

class XIDWrapAroundMonitor:
    """Tracks xid age and warns on wraparound risk."""

    WARN_AGE    = 1_500_000_000
    DANGER_AGE  = 2_000_000_000

    def check(self, mvcc: MVCCEngine, tables: list[HeapTable]) -> dict:
        current_xid = mvcc.current_xid()
        results = []
        for table in tables:
            age = mvcc.xid_age(table.relfrozenxid)
            status = 'ok'
            if age > self.DANGER_AGE:
                status = 'EMERGENCY: refusing writes'
            elif age > self.WARN_AGE:
                status = 'WARNING: approaching wraparound'
            elif age > self.WARN_AGE // 3:
                status = 'CAUTION: schedule aggressive vacuum'
            results.append({
                'table':         table.name,
                'relfrozenxid':  table.relfrozenxid,
                'xid_age':       age,
                'xids_remaining': max(0, self.DANGER_AGE - age),
                'status':        status,
            })
        return {'current_xid': current_xid, 'tables': results}


# ─── Bloat Tracker ────────────────────────────────────────────────────────────

class BloatTracker:
    """Records bloat statistics over time."""

    def __init__(self):
        self._history: list[dict] = []

    def record(self, table: HeapTable, label: str):
        self._history.append({
            'label':      label,
            'live':       table.total_live(),
            'dead':       table.total_dead(),
            'pages':      table.total_pages(),
            'dead_pct':   table.bloat_pct(),
        })

    def print_report(self):
        print(f"\n  {'Label':<30} {'Live':>8} {'Dead':>8} {'Pages':>6} {'Dead%':>7}")
        print(f"  {'-'*30} {'-'*8} {'-'*8} {'-'*6} {'-'*7}")
        for r in self._history:
            print(f"  {r['label']:<30} {r['live']:>8,} {r['dead']:>8,} "
                  f"{r['pages']:>6} {r['dead_pct']:>6.1f}%")


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== VACUUM and Bloat Simulator ===\n")

    mvcc    = MVCCEngine()
    orders  = HeapTable("orders")
    bloat   = BloatTracker()
    monitor = XIDWrapAroundMonitor()
    av      = AutovacuumLauncher(mvcc, scale_factor=0.20, threshold=5)
    av.register(orders)

    # ── Phase 1: Insert Initial Data ──────────────────────────────────────────
    print("─── Phase 1: Initial 100 Inserts ───")
    xid = mvcc.begin()
    for i in range(100):
        header = TupleHeader(xmin=xid)
        orders.insert(header, {'id': i, 'status': 'pending'})
    mvcc.commit(xid)
    bloat.record(orders, "After 100 inserts")
    print(f"  live={orders.total_live()}, dead={orders.total_dead()}, "
          f"pages={orders.total_pages()}")

    # ── Phase 2: Update 40 Rows (creates 40 dead tuples) ─────────────────────
    print("\n─── Phase 2: Update 40 Rows (MVCC dead tuple creation) ───")
    # Collect page/slot of first 40 tuples to "delete" their old versions
    old_locations = []
    for page in orders._pages:
        for slot_id, tup in enumerate(page._slots):
            if tup and len(old_locations) < 40:
                old_locations.append((page, slot_id, tup))

    xid_upd = mvcc.begin()
    for page, slot_id, old_tup in old_locations:
        # Mark old version as deleted (xmax = update xid)
        old_tup.header.xmax = xid_upd
        # Insert new version
        new_header = TupleHeader(xmin=xid_upd)
        orders.insert(new_header, {'id': old_tup.data['id'], 'status': 'shipped'})
    mvcc.commit(xid_upd)
    bloat.record(orders, "After 40 updates (before VACUUM)")
    print(f"  live={orders.total_live()}, dead={orders.total_dead()}, "
          f"bloat={orders.bloat_pct():.1f}%")
    print(f"  Autovacuum threshold: {av.threshold} + {av.scale_factor} × {orders.total_live()} "
          f"= {int(av.threshold + av.scale_factor * orders.total_live())} dead tuples needed")

    # ── Phase 3: Autovacuum Check (trigger at threshold) ─────────────────────
    print("\n─── Phase 3: Autovacuum Threshold Check ───")
    n_triggered = av.check_and_vacuum()
    bloat.record(orders, "After autovacuum")
    print(f"  Autovacuum triggered: {n_triggered} time(s)")
    print(f"  live={orders.total_live()}, dead={orders.total_dead()}, "
          f"bloat={orders.bloat_pct():.1f}%")

    # ── Phase 4: Simulate High-Write Scenario (Autovacuum Falls Behind) ───────
    print("\n─── Phase 4: High-Write Scenario ───")
    print("  Inserting and updating rapidly (autovacuum cannot keep up)...")
    for batch in range(10):
        # Rapid batch of updates
        xid = mvcc.begin()
        batch_old = []
        count = 0
        for page in orders._pages:
            for slot_id, tup in enumerate(page._slots):
                if tup and not tup.dead and tup.header.xmax == INVALID_XID:
                    if count < 20:
                        batch_old.append((page, slot_id, tup))
                        count += 1
        for page, slot_id, old_tup in batch_old:
            old_tup.header.xmax = xid
            new_header = TupleHeader(xmin=xid)
            orders.insert(new_header, {'id': old_tup.data['id'], 'status': f'batch-{batch}'})
        mvcc.commit(xid)

    bloat.record(orders, "After rapid updates (autovacuum behind)")
    print(f"  live={orders.total_live()}, dead={orders.total_dead()}, "
          f"bloat={orders.bloat_pct():.1f}%")

    # Manual VACUUM to catch up
    print("  Running VACUUM VERBOSE to catch up...")
    worker = VacuumWorker(mvcc, verbose=True)
    worker.vacuum(orders)
    bloat.record(orders, "After manual VACUUM")

    # ── Phase 5: XID Wraparound Monitoring ────────────────────────────────────
    print("\n─── Phase 5: XID Wraparound Monitoring ───")
    # Simulate xid advancing far ahead (as if many transactions ran)
    for _ in range(200):
        xid = mvcc.begin()
        mvcc.commit(xid)

    xid_report = monitor.check(mvcc, [orders])
    print(f"  Current xid:      {xid_report['current_xid']}")
    for t in xid_report['tables']:
        print(f"  Table: {t['table']}")
        print(f"    relfrozenxid: {t['relfrozenxid']}")
        print(f"    xid_age:      {t['xid_age']}")
        print(f"    xids_remaining: {t['xids_remaining']}")
        print(f"    status:       {t['status']}")

    # Force freeze
    print("\n  Running VACUUM with aggressive freeze (autovacuum_freeze_max_age triggered)...")
    worker.vacuum(orders, aggressive=True)
    xid_report2 = monitor.check(mvcc, [orders])
    for t in xid_report2['tables']:
        print(f"  After freeze — xid_age: {t['xid_age']}, status: {t['status']}")

    # ── Phase 6: Bloat Summary ─────────────────────────────────────────────────
    print("\n─── Phase 6: Bloat History ───")
    bloat.print_report()

    print("\n─── Vacuum Worker Stats ───")
    print(f"  Runs:           {worker.stats['runs']}")
    print(f"  Tuples removed: {worker.stats['tuples_removed']:,}")
    print(f"  Tuples frozen:  {worker.stats['tuples_frozen']:,}")
    print(f"  Pages scanned:  {worker.stats['pages_scanned']:,}")
```

---

## 6. Visual Reference

### Tuple Lifecycle: Insert → Update → VACUUM

```
Time →

INSERT (xid=100):           [xmin=100, xmax=0,   data="original"]  ← live

UPDATE (xid=200):           [xmin=100, xmax=200, data="original"]  ← dead (invisible to new txns)
                            [xmin=200, xmax=0,   data="updated" ]  ← live

Transaction T3 (xid=300) reads:
  Snapshot: xmax=300, active={}
  Tuple1 (xmin=100, xmax=200): xmin committed (100 < 300), xmax committed (200 < 300) → DELETED
  Tuple2 (xmin=200, xmax=0):   xmin committed (200 < 300) → VISIBLE

VACUUM runs (oldest_active_xmin = 300):
  Tuple1: xmax=200 < 300 → DEAD → reclaim slot
  Tuple2: xmax=0 → live → keep; freeze if xmin age > freeze_min_age

After VACUUM:               [slot reclaimed: free for next INSERT]
                            [xmin=FROZEN, xmax=0, data="updated"] ← frozen, always visible
```

### Autovacuum Trigger and Wraparound Protection

```
n_dead_tup accumulates ───────────────────────────────────────────────────►
                                                                           │
  0        50M      100M     150M      200M
  │         │         │        │         │
  │    autovacuum trigger    Freeze    Emergency
  │    (scale_factor × n)   passes    autovacuum
  │                          begin     (regardless
  │                                    of dead tup)
  │
  │  Normal path: autovacuum runs, removes dead tuples, advances relfrozenxid
  │
  │  Falling behind:
  │    - autovacuum_max_workers = 3 (all busy on other tables)
  │    - idle_in_transaction holds oldest xmin → VACUUM can't reclaim
  │    - cost throttling limits worker throughput
  │
  ▼
  XID age grows ─────────────────────────────────────────────────────────────►
  0         500M       1B        1.5B       2B
  │          │          │         │          │
  safe     caution    alert     WARNING    EMERGENCY
                               log msg    writes refused
```

---

## 7. Common Mistakes

**Mistake 1: Disabling autovacuum on large tables to "improve performance."** Autovacuum's default cost throttling makes it run gently in the background. If it appears to impact query performance, the right response is to tune its cost parameters or run it off-peak — not disable it. A table with autovacuum disabled accumulates dead tuples indefinitely. Beyond degraded query performance from bloat, the table eventually triggers the wraparound emergency. There is almost never a legitimate reason to disable autovacuum. If the table is read-only (and therefore generates no dead tuples), autovacuum ignores it naturally.

**Mistake 2: Running `VACUUM FULL` routinely.** `VACUUM FULL` requires an `AccessExclusiveLock` that blocks all reads and writes for the entire table rewrite. On a large table, this means an outage measured in hours. It should be used only in specific circumstances: after deleting a very large fraction of the table (> 50%), when disk space is critically short, or as a one-time operation after a catastrophic bloat event. For routine maintenance, regular `VACUUM` (or autovacuum) reclaims space within the file and marks it for reuse. For online table compaction without an exclusive lock, use `pg_repack`.

**Mistake 3: Holding `idle in transaction` connections for extended periods.** The MVCC visibility rule is: dead tuples can only be reclaimed when their xmax is older than the oldest active transaction's xmin. An `idle in transaction` connection holds a snapshot xmin that is as old as when the transaction started. If a connection opened a transaction at 9:00 AM and is still `idle in transaction` at noon, its xmin from 9:00 AM prevents VACUUM from reclaiming any dead tuple created after 9:00 AM. On a high-write table, this means 3 hours of dead tuples accumulate and cannot be cleaned. Set `idle_in_transaction_session_timeout = '5min'` to terminate such connections automatically.

---

## 8. Production Failure Scenarios

### Scenario 1: Autovacuum Falling Behind on a High-Write Table

**Symptoms:** `pg_stat_user_tables` shows `n_dead_tup` growing continuously on the `orders` table. Table size on disk grows from 50GB to 200GB over three weeks, despite no increase in `n_live_tup`. Sequential scan queries on `orders` slow down from 4 seconds to 15 seconds.

**Root cause:** The `orders` table receives 500 updates/second. Autovacuum's default cost throttling (200 cost units, 2ms delay) limits its throughput to approximately 10M dead tuple reclaims per hour. At 500 updates/second = 1.8M updates/hour, autovacuum keeps up. But during the business day's peak (1,500 updates/second = 5.4M updates/hour), autovacuum cannot keep pace. Dead tuples accumulate faster than they are reclaimed.

**Fix:**
```sql
-- Step 1: Run manual VACUUM immediately to reduce immediate bloat
VACUUM ANALYZE orders;

-- Step 2: Reduce autovacuum cost throttling for this specific table
ALTER TABLE orders SET (
    autovacuum_vacuum_scale_factor = 0.02,   -- Trigger at 2% dead tuples (not 20%)
    autovacuum_vacuum_cost_delay = 2,         -- 2ms pause (default is also 2ms)
    autovacuum_vacuum_cost_limit = 800        -- 800 cost points before pause (not 200 — 4× more aggressive)
);

-- Step 3: Globally increase autovacuum workers (if multiple tables are affected)
ALTER SYSTEM SET autovacuum_max_workers = 6;   -- Increase from 3 to 6 workers
SELECT pg_reload_conf();

-- Step 4: Monitor progress
SELECT relname, n_live_tup, n_dead_tup,
       last_autovacuum, last_autoanalyze
FROM pg_stat_user_tables WHERE relname = 'orders';
```

### Scenario 2: Transaction ID Wraparound Warning

**Symptoms:** Postgres logs appear: `WARNING: database "production" must be vacuumed within 400000000 transactions`. A few days later: `ERROR: database is not accepting commands to avoid wraparound data loss in database "production"`. All application writes fail.

**Root cause:** A large table (`events`, 2TB) had autovacuum disabled months ago "to prevent it from affecting query performance." The table's `relfrozenxid` has not been updated in 1.8 billion transactions. The gap between `datfrozenxid` and the current xid exceeded 2 billion.

**Recovery (emergency procedure):**
```sql
-- Step 1 (immediate): Check which tables are the most dangerous
SELECT relname, age(relfrozenxid) AS xid_age
FROM pg_class WHERE relkind = 'r'
ORDER BY age(relfrozenxid) DESC LIMIT 10;

-- Step 2: Force VACUUM FREEZE on the most critical tables
-- This bypasses autovacuum and runs as a regular (non-blocking) VACUUM
-- but with aggressive freezing
VACUUM FREEZE events;
-- This may take hours for a 2TB table

-- Step 3: While VACUUM FREEZE is running, monitor progress
SELECT now(), datname, age(datfrozenxid), txid_current()
FROM pg_database WHERE datname = 'production';

-- Step 4: After recovery, re-enable autovacuum and tune it
ALTER TABLE events RESET (autovacuum_enabled);  -- Re-enable autovacuum
ALTER TABLE events SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_cost_limit = 1000
);

-- Step 5: Set up monitoring to prevent recurrence
-- Alert when age(datfrozenxid) > 500,000,000 (500M xids)
-- SELECT datname, age(datfrozenxid) FROM pg_database
-- WHERE age(datfrozenxid) > 500000000;
```

---

## 9. Performance and Tuning

### Autovacuum Tuning for Data Engineering Workloads

```sql
-- ── Global autovacuum settings ────────────────────────────────────────────────
-- Increase worker count for databases with many tables
ALTER SYSTEM SET autovacuum_max_workers = 6;

-- Make autovacuum more aggressive globally
ALTER SYSTEM SET autovacuum_vacuum_cost_limit = 800;     -- Default: 200
ALTER SYSTEM SET autovacuum_vacuum_cost_delay = '2ms';   -- Default: 2ms (keep or reduce to 1ms)

-- Reduce scale factor globally for more frequent vacuums on large tables
ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 0.05;  -- Default: 0.20 (trigger at 5% not 20%)

-- Shorter naptime: check tables more frequently
ALTER SYSTEM SET autovacuum_naptime = '30s';             -- Default: 1min

SELECT pg_reload_conf();

-- ── Per-table tuning for high-write tables ────────────────────────────────────
ALTER TABLE orders SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 100,
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_cost_limit = 1000,
    autovacuum_vacuum_cost_delay = 2
);

-- ── Wraparound monitoring query (run as a cron job / scheduled check) ─────────
SELECT
    datname,
    age(datfrozenxid)                   AS xid_age,
    2000000000 - age(datfrozenxid)      AS xids_until_emergency,
    CASE
        WHEN age(datfrozenxid) > 1500000000 THEN 'CRITICAL'
        WHEN age(datfrozenxid) > 1000000000 THEN 'WARNING'
        WHEN age(datfrozenxid) >  500000000 THEN 'CAUTION'
        ELSE 'OK'
    END AS status
FROM pg_database
WHERE datallowconn
ORDER BY xid_age DESC;

-- ── Dead tuple accumulation rate (run periodically) ──────────────────────────
SELECT
    relname,
    n_dead_tup,
    n_live_tup,
    round(n_dead_tup::numeric / nullif(n_live_tup, 0) * 100, 2) AS dead_pct,
    last_autovacuum,
    last_vacuum,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables
WHERE n_live_tup > 10000
ORDER BY n_dead_tup DESC
LIMIT 20;

-- ── Visibility map coverage (index-only scan eligibility) ─────────────────────
-- Requires pg_visibility extension
CREATE EXTENSION IF NOT EXISTS pg_visibility;
SELECT
    relname,
    count(*) FILTER (WHERE all_visible) AS visible_pages,
    count(*) AS total_pages,
    round(count(*) FILTER (WHERE all_visible)::numeric / count(*) * 100, 2) AS visibility_pct
FROM pg_class
JOIN pg_visibility(oid) ON TRUE
WHERE relname = 'orders'
GROUP BY relname;
-- Low visibility_pct → autovacuum hasn't run recently →
-- index-only scans must fall back to heap fetches
```

---

## 10. Interview Q&A

**Q1: Explain why Postgres UPDATE creates a dead tuple instead of modifying the row in place, and what consequences this has for production operations.**

Postgres UPDATE creates a new tuple version rather than modifying the row in place because of MVCC — Multi-Version Concurrency Control. When transaction T1 updates a row, transaction T2 that started before T1 must still see the original row value. If Postgres modified the row in place, T2 would see the modified value, violating transaction isolation. By keeping the original tuple on disk with its xmax set to T1's transaction ID, T2's snapshot can still see the original, while T3 (starting after T1 commits) sees only the new version.

The production consequence is dead tuple accumulation. Every UPDATE leaves one dead tuple behind. A table with 1,000 updates per second accumulates 86.4 million dead tuples per day if VACUUM does not run fast enough to keep up. Dead tuples consume disk space (the table file grows even though n_live_tup stays constant), slow sequential scans (the executor reads and skips dead tuples on every page), and prevent index-only scans (the visibility map flag is not set on pages with dead tuples, so the executor must fetch the heap to verify tuple visibility even when the index has all the needed data).

VACUUM is the process that reclaims dead tuple slots — but it marks the slots as reusable without shrinking the file. Only `VACUUM FULL` or `pg_repack` actually shrinks the physical file, and only `VACUUM FULL` is built-in (though it requires an exclusive lock). For routine operations, the goal is to run VACUUM frequently enough that dead tuple accumulation stays bounded and does not affect query performance or disk space.

**Q2: What is transaction ID wraparound and why is it a correctness problem, not just a performance problem?**

Postgres assigns a sequential 32-bit transaction ID (xid) to every transaction that modifies data. These xids are used for MVCC visibility: a tuple's `xmin` tells the database which transaction created it, and the visibility check determines whether that xmin is "in the past" (committed before the current snapshot) or "in the future" (not yet visible).

The problem is that 32-bit xids are finite — there are only about 4 billion of them, and Postgres cycles through the entire space in a busy database in 1-2 years of continuous writes. Postgres uses a modular arithmetic convention to handle this: any xid that is more than 2 billion xids in the past is considered to be "in the future" and its data is treated as invisible. This works as long as the oldest live xid in the database is never more than 2 billion xids behind the current xid.

When the gap approaches 2 billion, Postgres begins refusing all write transactions to prevent corruption. Without this protection, after wraparound, old xids would appear to be "from the future" — rows created by old transactions would become invisible, as if they had never existed. This is data loss, not just slowness.

The prevention mechanism is tuple freezing: VACUUM replaces a tuple's actual xmin with the special frozen xid marker (2), which is always considered "in the past" regardless of current xid. Once a tuple is frozen, it no longer consumes xid space. Autovacuum is responsible for freezing tuples before the wraparound danger threshold is reached. The monitoring query on `age(datfrozenxid)` — the age of the oldest unfrozen xid in the database — is the key metric for wraparound prevention.

---

## 11. Cross-Question Chain

**Interviewer:** How does Postgres determine whether a dead tuple can be safely reclaimed by VACUUM?

**Candidate:** A dead tuple has its `xmax` field set to the xid of the transaction that deleted or updated it. VACUUM can reclaim a dead tuple when two conditions are met: xmax is committed (the deletion transaction actually completed), and xmax is older than the `oldest_active_xmin` — the oldest transaction xmin currently held by any active backend process. The oldest_active_xmin is the floor of visibility: any xmax older than this value is invisible to all current and future transactions, because every active snapshot was taken after that xmax committed.

**Interviewer:** What happens to dead tuple reclamation when a connection is `idle in transaction` for 2 hours?

**Candidate:** The `idle in transaction` connection holds an MVCC snapshot from when its transaction started — 2 hours ago. That snapshot has an xmin equal to the xid that was current 2 hours ago. The oldest_active_xmin for the cluster is now pinned at that 2-hour-old xid. VACUUM cannot reclaim any dead tuple whose xmax is newer than that 2-hour-old xid — which means every dead tuple created in the last 2 hours is immune to VACUUM. On a high-write table receiving 1,000 updates/second, 2 hours = 7.2 million dead tuples that cannot be reclaimed. The table bloats by the size of those tuples while the `idle in transaction` connection sits idle.

The fix: set `idle_in_transaction_session_timeout = '5min'`. This terminates any connection that holds an open transaction with no active query for more than 5 minutes. The xmin is released, the oldest_active_xmin advances, and VACUUM can reclaim the backlog.

**Interviewer:** The xid age of a table reaches 600 million. What happens automatically, and what should you do manually?

**Candidate:** At 600 million xid age, autovacuum enters an elevated freeze mode. The `autovacuum_freeze_max_age` parameter (default 200 million xids — but we're already past that) should have triggered aggressive freeze passes at 200 million. At 600 million, we've either missed multiple autovacuum cycles or autovacuum has been unable to keep up. What should happen automatically: autovacuum workers ignore their normal cost throttling and run aggressive `VACUUM FREEZE` passes. What should you do manually: first, check whether autovacuum workers are active on this table (`SELECT * FROM pg_stat_activity WHERE query LIKE 'autovacuum%'`). If not, run `VACUUM FREEZE table_name` manually from a psql session. This is a blocking operation (single process, not concurrent with application writes in terms of priority) but does not hold an exclusive lock on the table — application queries can continue during a `VACUUM FREEZE`.

**Interviewer:** After VACUUM finishes, the table size on disk doesn't shrink. The file is still 200GB but only 50GB of live data exists. The team asks you to reclaim the 150GB. What are your options?

**Candidate:** Regular VACUUM reclaims dead tuple slots for reuse within the file but cannot shrink the file. To reclaim the 150GB of actual disk space, the options are:

First: `VACUUM FULL table_name`. This rewrites the entire table into a new file, drops the old file, and releases the 150GB back to the OS. It requires an `AccessExclusiveLock` for the entire duration — blocking all reads and writes. On a 200GB table, that could be 1-4 hours of downtime depending on I/O speed. Appropriate for a maintenance window but not acceptable during business hours.

Second: `pg_repack`. An external tool (installable as a Postgres extension) that performs the same full table rewrite but without the exclusive lock. It creates a shadow table, copies live rows into it, applies concurrent changes via a trigger, then swaps the tables with a very brief lock at the end (milliseconds). This is the production-safe option for online bloat reclamation. Install with `CREATE EXTENSION pg_repack` and run `pg_repack -t table_name`.

Third: if the table allows it — partition the table by time and drop old partitions rather than deleting rows. Dropping a partition is an instant metadata operation that returns space to the OS immediately. This is the most scalable approach for time-series or log data, but requires the schema to be restructured.

**Interviewer:** How do you know which tables need the most urgent vacuum attention, without reading logs?

**Candidate:** Three queries in combination. First, for dead tuple bloat: `SELECT relname, n_dead_tup, n_live_tup, round(n_dead_tup/nullif(n_live_tup,0)*100,2) AS dead_pct, last_autovacuum FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 10`. Tables with dead_pct > 20% and large n_dead_tup are the immediate concern. Second, for wraparound risk: `SELECT relname, age(relfrozenxid) AS xid_age FROM pg_class WHERE relkind='r' ORDER BY age(relfrozenxid) DESC LIMIT 10`. Tables with xid_age approaching 200 million need an aggressive vacuum pass. Third, for autovacuum being blocked: `SELECT * FROM pg_stat_activity WHERE wait_event = 'Lock' AND query LIKE 'autovacuum%'` — if autovacuum workers are waiting for locks, they're blocked by long-running transactions that need to be identified and either killed or waited out.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | Why does Postgres UPDATE leave a dead tuple on disk instead of modifying in place? | MVCC: concurrent transactions may still need to see the old value. Modifying in place would violate snapshot isolation. The old tuple stays until no active transaction's snapshot could possibly see it (until its xmax is older than oldest_active_xmin). |
| 2 | What fields in the Postgres tuple header control MVCC visibility? | `xmin`: xid that created this tuple version. `xmax`: xid that deleted/updated this tuple (0 = still live). `frozen` flag: tuple has been frozen (always visible). |
| 3 | What is `oldest_active_xmin` and why does it matter for VACUUM? | The oldest transaction xmin currently held by any active backend (from the procarray). VACUUM cannot reclaim a dead tuple if its xmax is newer than oldest_active_xmin — some active transaction might still need to see the deleted row. |
| 4 | What is the autovacuum trigger threshold formula? | `trigger = autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × n_live_tup`. Defaults: 50 + 0.20 × n_live_tup. For a 1M-row table: trigger at 200,050 dead tuples. For a 100M-row table: trigger at 20,000,050 — too high for most workloads. |
| 5 | What does VACUUM do to a dead tuple slot? | Marks it as free in the page's ItemIdArray; adds the page to the Free Space Map so future INSERTs can reuse the slot. Does NOT shrink the physical file. |
| 6 | What does VACUUM do to the visibility map? | Sets the "all-visible" bit for pages where all tuples are visible to all active and future transactions. This bit enables index-only scans — the executor can skip heap fetches for pages with the bit set. |
| 7 | What is the difference between VACUUM and VACUUM FULL? | VACUUM: no exclusive lock, marks slots as reusable, does not shrink file. Runs concurrently with reads/writes. VACUUM FULL: exclusive lock (blocks all access), rewrites entire table into a new file, returns space to OS. Use VACUUM FULL only for emergency disk reclamation. |
| 8 | What is xid (transaction ID) wraparound and why is it catastrophic? | Postgres uses 32-bit xids. After ~2 billion xids, old xids appear "in the future" — their tuples become invisible, causing data loss. Postgres prevents this by refusing writes when the danger threshold is reached. Recovery requires VACUUM FREEZE (hours of read-only operation). |
| 9 | What is a "frozen" tuple? | A tuple whose xmin has been replaced with the special frozen xid marker (2). Frozen tuples are always visible to all transactions — they are "infinitely old." Freezing removes the tuple from the wraparound risk calculation. |
| 10 | What prevents VACUUM from reclaiming dead tuples on a high-write table? | Three common causes: (1) autovacuum cost throttling limits worker throughput; (2) `idle in transaction` connections hold old xmin snapshots; (3) only 3 autovacuum workers for many tables. Fix: increase cost_limit, set idle_in_transaction_session_timeout, increase autovacuum_max_workers. |
| 11 | How do you monitor transaction ID wraparound risk? | `SELECT datname, age(datfrozenxid), 2000000000 - age(datfrozenxid) AS xids_remaining FROM pg_database`. Alert when xid_age > 500M (caution) or 1B (warning). Per-table: `SELECT relname, age(relfrozenxid) FROM pg_class WHERE relkind='r' ORDER BY age(relfrozenxid) DESC`. |
| 12 | Why does VACUUM not shrink the table file? | VACUUM marks dead tuple slots as reusable within the existing file pages. It cannot move live tuples to consolidate space and does not deallocate pages at the end of the file (except for pages at the very end that become entirely empty). Only VACUUM FULL / pg_repack rewrite the file and return space to the OS. |
| 13 | What is table bloat and how do you estimate it? | Wasted space in the table file from dead tuples and unreused free space. Estimate: compare `pg_relation_size(table)` to `n_live_tup × avg_row_size`. Query: `SELECT n_dead_tup, n_dead_tup*100/nullif(n_live_tup+n_dead_tup,0) AS dead_pct FROM pg_stat_user_tables`. |
| 14 | What is `pg_repack` and when should you use it? | An external Postgres extension that performs the equivalent of VACUUM FULL (rewrites the table file, returns space to OS) without an exclusive lock. Uses a shadow table + triggers to capture concurrent writes. Use when VACUUM FULL's table-locking downtime is not acceptable. |
| 15 | How does an `idle in transaction` connection cause VACUUM to fail? | The connection holds an MVCC snapshot with an xmin from when the transaction started. The oldest_active_xmin is pinned at that old xmin. VACUUM cannot reclaim any dead tuple whose xmax is newer than that xmin — meaning all dead tuples created since the transaction started accumulate unreclaimed. |

---

## 13. Further Reading

- **Postgres documentation — "Routine Vacuuming":** The authoritative reference for autovacuum configuration, cost throttling, and wraparound prevention.
- **"The Internals of PostgreSQL" by Hironobu Suzuki (interdb.jp) — Chapter 5 (Concurrency Control) and Chapter 6 (Vacuum Processing):** Detailed walkthrough of the MVCC tuple header format, visibility rules, and VACUUM's internal algorithm.
- **Postgres source: `src/backend/commands/vacuum.c` and `src/backend/postmaster/autovacuum.c`:** The VACUUM implementation and autovacuum launcher logic.
- **"Avoiding PostgreSQL Wraparound Emergencies" (Citus Data blog):** Practical case studies of wraparound incidents and monitoring strategies.
- **pg_repack documentation (reorg.github.io/pg_repack):** How to use pg_repack for online table compaction without exclusive locks.

---

## 14. Lab Exercises

**Exercise 1: Dead Tuple Observation**
Create a table, insert 10,000 rows, then run `UPDATE` on half of them. Query `pg_stat_user_tables` to see `n_dead_tup`. Run `VACUUM VERBOSE` and observe the output. Query `n_dead_tup` again.

**Exercise 2: Autovacuum Threshold Calculation**
For each of the following tables (n_live_tup values), calculate the autovacuum trigger threshold with default settings (threshold=50, scale_factor=0.20): 1,000 rows, 100,000 rows, 1,000,000 rows, 10,000,000 rows, 100,000,000 rows. Which threshold is too high for a high-write table?

**Exercise 3: Wraparound Monitoring**
In `vacuum_simulator.py`, extend the `XIDWrapAroundMonitor` to simulate the emergency state: advance the MVCC engine's xid counter to within 50 million of the `WRAPAROUND_EMERGENCY` threshold and verify the monitor correctly reports the `EMERGENCY` status.

**Exercise 4: MVCC Visibility Trace**
In `vacuum_simulator.py`, add a method `MVCCEngine.visible_tuples(table, snapshot)` that returns all tuples visible in a given snapshot. Insert 10 rows, update 5 of them, then take snapshots at three points (before update, during update, after update) and call `visible_tuples` for each. Verify the results match MVCC expectations.

---

## 15. Key Takeaways

VACUUM is not optional maintenance — it is fundamental to Postgres's correctness. The MVCC design that provides snapshot isolation without locks requires dead tuples to accumulate, and VACUUM is the only mechanism that reclaims them. Without VACUUM, tables bloat indefinitely and eventually trigger transaction ID wraparound, which causes data loss.

The two separate functions of VACUUM — dead tuple reclamation and xid freezing — address two separate problems. Dead tuple reclamation addresses performance and disk space. Xid freezing addresses correctness: without it, old tuples become invisible after wraparound. Autovacuum performs both, triggered by dead tuple thresholds and xid age thresholds independently.

The most important production tuning levers: reduce `autovacuum_vacuum_scale_factor` from 0.20 to 0.01-0.05 for large tables (reducing dead tuples per autovacuum cycle), increase `autovacuum_vacuum_cost_limit` from 200 to 800 (making each cycle faster), increase `autovacuum_max_workers` from 3 to 6 (more tables can be vacuumed simultaneously), and set `idle_in_transaction_session_timeout` to prevent connections from pinning old xmin snapshots.

---

## 16. Connections to Other Modules

- **M52 — Postgres Architecture:** Autovacuum is the background process described in M52. The procarray (M52) is the source of `oldest_active_xmin` — VACUUM reads the procarray to determine which dead tuples can be reclaimed. `idle in transaction` connections (M52) are the #1 cause of VACUUM falling behind.
- **M53 — Query Planning:** Stale statistics from autovacuum falling behind cause bad query plans (M53). `VACUUM ANALYZE` updates both dead tuple state and planner statistics simultaneously. The visibility map (maintained by VACUUM) is the prerequisite for index-only scans, which the planner chooses when it judges them cheaper than index+heap scans.
- **M54 — Indexes:** Index-only scans require the visibility map to have the all-visible bit set. VACUUM sets this bit. Autovacuum falling behind means the visibility map is not maintained — index-only scans degrade to effective index scans with heap fetches.
- **M49 — Write-Ahead Log:** VACUUM writes WAL records for the visibility map updates and free space map changes. The WAL overhead from VACUUM is non-trivial on large tables with heavy write activity.
- **M47 — B-Tree Storage Engines:** Index bloat (dead index entries) is also addressed by VACUUM — VACUUM scans each index to remove index entries pointing to reclaimed heap tuples. This is the most time-consuming part of VACUUM for tables with many large indexes.

---

## 17. Anti-Patterns

**Anti-pattern: Relying on `VACUUM FULL` as a scheduled maintenance task.** Some teams schedule `VACUUM FULL` nightly to "keep tables clean." This takes an exclusive lock on the table for the entire run — effectively a nightly outage for every table. The right approach: tune autovacuum to reclaim dead tuples as they accumulate, so the table never develops enough bloat to require `VACUUM FULL`. If a table's bloat requires urgent file-size reduction, use `pg_repack` instead.

**Anti-pattern: Setting `autovacuum_vacuum_scale_factor = 0.20` globally and not tuning per-table.** The default 20% scale factor is appropriate for small tables (trigger at 200 dead tuples for a 1,000-row table), mediocre for medium tables (20,000 dead tuples for 100,000 rows), and actively harmful for large tables (2,000,000 dead tuples for 10,000,000 rows before autovacuum triggers). Large high-write tables need per-table tuning: `ALTER TABLE big_table SET (autovacuum_vacuum_scale_factor = 0.01)`.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pg_stat_user_tables` | Dead tuple counts per table | `n_dead_tup`, `n_live_tup`, `last_autovacuum` |
| `pg_database.datfrozenxid` | Database-level wraparound risk | `age(datfrozenxid)` — alert if > 500M |
| `pg_class.relfrozenxid` | Per-table wraparound risk | `age(relfrozenxid)` — identify specific tables |
| `VACUUM VERBOSE` | Manual vacuum with diagnostic output | Shows pages/tuples removed, frozen |
| `VACUUM FREEZE` | Force-freeze all eligible tuples | Emergency wraparound prevention |
| `VACUUM FULL` | Rewrite table to reclaim disk space | Requires exclusive lock — use sparingly |
| `pg_repack` | Online table compaction (no exclusive lock) | Alternative to VACUUM FULL for production |
| `pg_visibility` (extension) | Visibility map per-page analysis | Check all_visible fraction for index-only scan eligibility |
| `vacuum_simulator.py` | MVCC + VACUUM simulation | Visualize dead tuple lifecycle |

---

## 19. Glossary

**Autovacuum:** Background Postgres process (launcher + workers) that automatically triggers VACUUM when dead tuple thresholds are exceeded. Critical for both performance (dead tuple reclamation) and correctness (xid freezing). Should never be disabled.

**Dead tuple:** A tuple version in the heap that is no longer visible to any transaction (xmax committed and older than oldest_active_xmin) but has not yet been reclaimed by VACUUM. Occupies disk space and is processed (and skipped) by sequential scans.

**Frozen tuple:** A tuple whose xmin has been replaced with the frozen xid marker (2). Always visible to all transactions; not counted in the wraparound risk calculation.

**Free Space Map (FSM):** A per-table data structure tracking available free space per page. Updated by VACUUM after reclaiming dead tuple slots. Used by INSERT to find pages with space without scanning from the beginning.

**oldest_active_xmin:** The minimum xmin across all active backend processes (from the procarray). The floor below which VACUUM can safely reclaim dead tuples.

**relfrozenxid:** The OID of the oldest unfrozen xid on any live tuple in this table (stored in pg_class). `age(relfrozenxid) = current_xid - relfrozenxid`. The key metric for per-table wraparound risk.

**Transaction ID (xid) wraparound:** The condition when the age of the oldest non-frozen xid approaches 2 billion. Postgres refuses writes to prevent data corruption. Prevention: regular VACUUM with aggressive freezing.

**VACUUM:** The Postgres process that reclaims dead tuple slots, updates the Free Space Map and Visibility Map, freezes old tuples to prevent wraparound, and optionally updates planner statistics (VACUUM ANALYZE).

**VACUUM FULL:** A VACUUM variant that rewrites the entire table file, returning disk space to the OS. Requires an exclusive lock blocking all reads and writes. Use sparingly; prefer pg_repack for online compaction.

**Visibility map:** A per-page bit array storing whether all tuples on a page are visible to all current and future transactions. Set by VACUUM. Required for index-only scans.

**xmax:** The transaction ID of the transaction that deleted or updated this tuple version. When xmax = 0, the tuple is live. When xmax is set to a committed xid, the tuple is dead (if xmax < oldest_active_xmin).

**xmin:** The transaction ID of the transaction that created this tuple version. A tuple is visible if xmin is committed and in the snapshot's past.

---

## 20. Self-Assessment

1. Explain why Postgres UPDATE creates a dead tuple. How does the `xmax` field determine when VACUUM can reclaim it?
2. What is `oldest_active_xmin` and which shared memory structure provides it? How does an `idle in transaction` connection block VACUUM?
3. For a table with 5,000,000 rows, what is the autovacuum trigger threshold with default settings? Is this appropriate? What per-table setting would you change and to what value?
4. In `vacuum_simulator.py`, trace Phase 2 (Update 40 rows). What changes in the TupleHeader for the old versions? What happens in Phase 3 when autovacuum runs?
5. Describe the three phases of xid wraparound protection: the threshold that triggers aggressive freezing, the threshold that triggers emergency autovacuum, and the threshold at which Postgres refuses writes.
6. A table has `n_dead_tup = 50,000,000` and `n_live_tup = 2,000,000`. The table size is 400GB. After running `VACUUM`, the file is still 400GB. Why? What are your options to reclaim the disk space?
7. What does the visibility map track and why does it matter for query performance?
8. Why is `VACUUM FULL` dangerous to schedule as routine maintenance? What is the production-safe alternative when a table's file size must be reduced?

---

## 21. Module Summary

Postgres's MVCC model is what makes UPDATE expensive: every update leaves a dead tuple behind that must persist until no active transaction could possibly see it. VACUUM is the garbage collector that reclaims these dead tuples, updates the visibility map (enabling index-only scans), and freezes old transaction IDs (preventing wraparound). Autovacuum automates this process, but its defaults are calibrated for small-to-medium tables and must be tuned for high-write production tables.

The simulation — `MVCCEngine`, `HeapPage`, `VacuumWorker`, `AutovacuumLauncher`, `XIDWrapAroundMonitor` — makes the lifecycle concrete: you can watch dead tuples accumulate as updates run, autovacuum trigger when the threshold is crossed, VACUUM reclaim the slots, and the wraparound monitor track xid age in real time.

The production essentials: tune `autovacuum_vacuum_scale_factor` down to 0.01 for large tables; set `idle_in_transaction_session_timeout` to prevent old snapshots from blocking VACUUM; monitor `age(datfrozenxid)` and alert at 500M xids; use `pg_repack` instead of `VACUUM FULL` when disk space must be reclaimed without a write outage; and never disable autovacuum on tables that receive writes.

The next module — M56: Postgres for Data Engineering — covers the two Postgres extensions most critical for data engineering pipelines: PgBouncer for connection pooling (the solution to the process-per-connection overhead from M52) and logical replication for CDC (capturing row-level changes as a stream for downstream analytics and Kafka pipelines).
