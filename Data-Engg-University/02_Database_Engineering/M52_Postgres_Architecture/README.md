# M52: Postgres Architecture

**Course:** DBE-INT-102 — Postgres Architecture  
**Module:** 01 of 05  
**Global Module ID:** M52  
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

Postgres is the most widely deployed database in data engineering stacks. It is the metadata store for Apache Airflow, the default target for dbt, the backend for many ETL orchestration tools, the operational database for countless data-adjacent microservices, and often the source for CDC pipelines that feed Kafka. You will interact with Postgres every day as a data engineer — and in most cases, you will interact with it as a black box.

The black-box engineer reaches for `shared_buffers = 256MB` because they read it somewhere. They create indexes hoping queries will speed up. They restart Postgres when it seems slow. They don't know why autovacuum runs, what the checkpointer does, or why there are dozens of Postgres processes running at once.

The engineer who understands Postgres architecture can answer questions the black-box engineer cannot: Why does this query use a sequential scan despite having an index? Why is `pg_stat_activity` showing many `idle in transaction` connections? Why did adding RAM not speed up queries? Why did `VACUUM ANALYZE` make queries faster? Why does Postgres spawn a new process per connection?

This module maps the entire Postgres architecture: how the postmaster process manages the cluster, what each background process does and why, how shared memory is organized, how the WAL writer and checkpointer interact, and how a client connection flows from TCP socket to query result. Every background process and shared memory region has a purpose that is traceable to a fundamental constraint — and understanding those purposes is what makes Postgres tuning predictable rather than superstitious.

---

## 2. Mental Model

### Postgres Is a Multi-Process, Not Multi-Thread, Server

Most application servers use threads for concurrency: one thread pool, shared memory, OS-scheduled. Postgres uses **processes**: each client connection spawns a dedicated OS process (a "backend process"). Every backend process has its own stack, registers, and virtual address space, but shares the **shared memory segment** (buffer pool, WAL buffers, lock tables) via `shmget`/`mmap`.

This design choice — made in 1995 when threads were unreliable — has profound production implications:
- **Each connection = one OS process** → 1000 connections = 1000 processes → context switching overhead, memory overhead (~10MB per process for stack + local buffers), and OS scheduling pressure.
- **No GIL problem** → True parallelism for independent queries (each process on its own CPU core).
- **Crash isolation** → A backend crash does not crash Postgres; the postmaster detects it and cleans up without affecting other connections.
- **Connection overhead** → Opening a new database connection requires a `fork()` syscall + authentication (~5-10ms). This is why **PgBouncer** (connection pooling) is essentially mandatory for applications with short-lived connections.

### The Shared Memory Segment

All backend processes and background processes share one region of memory: the **shared memory segment**, allocated at Postgres startup via `shmget()`. It contains:
- **Shared buffer pool** (`shared_buffers`): The page cache for database files. All backends read/write pages through this cache.
- **WAL buffers** (`wal_buffers`): In-memory buffer for WAL records before they are fsynced to disk.
- **Lock tables**: Track which transactions hold which locks on which rows/pages/tables.
- **Procarray**: Tracks all active backend PIDs and their current transaction states. Used by MVCC to determine row visibility.
- **Commit log (CLOG)**: Tracks the commit/abort status of all transaction IDs.
- **Background worker registry**: Tracks registered background workers (autovacuum workers, logical replication workers, etc.).

---

## 3. Core Concepts

### 3.1 The Postmaster Process

The **postmaster** is the parent process of all Postgres processes. It:
1. Allocates the shared memory segment at startup
2. Listens on the TCP port (default 5432) for incoming connections
3. Forks a new backend process for each accepted connection
4. Monitors all child processes; if a backend crashes, postmaster detects it (via `SIGCHLD`) and cleans up the shared memory state
5. Restarts background processes if they crash

When you run `pg_ctl start`, you start the postmaster. When you run `pg_ctl stop`, the postmaster sends `SIGTERM` to all backends (graceful shutdown) or `SIGQUIT` (immediate shutdown).

The postmaster itself does not handle queries. Its sole job is process management and shared memory lifecycle.

### 3.2 Backend Processes

A **backend process** is forked by the postmaster for each client connection. It:
1. Authenticates the client (checks `pg_hba.conf`)
2. Receives SQL from the client (via the frontend/backend protocol over TCP)
3. Parses → plans → executes the query
4. Reads/writes pages through the shared buffer pool
5. Writes WAL records (to WAL buffers, flushed by the WAL writer)
6. Returns results to the client

Each backend process runs independently. Two backends executing different queries do not share execution state — only shared memory (buffer pool, lock tables, procarray).

**Memory per backend:**
- Stack: ~8MB default (`ulimit -s`)
- Local buffers (`temp_buffers`): 8MB default (for temporary tables)
- Work memory (`work_mem`): 4MB default (for sorts and hash tables — one per sort/hash node in the plan, can multiply with parallel query)
- Total: 15-50MB typical per backend process

At 100 active connections: 1.5-5GB RAM consumed by backends alone, before the shared buffer pool.

### 3.3 Background Processes

Postgres runs several background processes that are always active (started by postmaster at startup):

**WAL Writer (`walwriter`):**
Flushes WAL buffers to the WAL files on disk at regular intervals (`wal_writer_delay = 200ms` default) or when the WAL buffer is nearly full. This reduces the per-transaction fsync overhead: many transactions can batch their WAL records into one WAL writer flush. When `synchronous_commit = on`, individual backends may also call fsync directly on commit.

**Checkpointer (`checkpointer`):**
Performs checkpoints — flushing all dirty pages from the shared buffer pool to the data files on disk. Triggered by `checkpoint_timeout` (default 5 minutes) or when WAL grows beyond `max_wal_size` (default 1GB). Spreads I/O over `checkpoint_completion_target` fraction of the checkpoint interval (default 90%). Writes a CHECKPOINT WAL record when done, establishing the new redo LSN for crash recovery.

**Background Writer (`bgwriter`):**
Proactively writes dirty pages from the buffer pool to disk, outside of checkpoint cycles. Goal: keep a supply of clean (evictable) buffer pool pages so that backend processes never have to flush a dirty page during query execution. Configured by `bgwriter_delay`, `bgwriter_lru_maxpages`, `bgwriter_lru_multiplier`. If the bgwriter cannot keep up, backends write dirty pages themselves (`buffers_backend` counter in `pg_stat_bgwriter`).

**Autovacuum Launcher + Workers:**
The autovacuum launcher monitors all tables in all databases. When a table's dead tuple count exceeds a threshold (`autovacuum_vacuum_scale_factor × n_live_tup + autovacuum_vacuum_threshold`), the launcher spawns an autovacuum worker process to vacuum that table. Workers are limited by `autovacuum_max_workers` (default 3). Autovacuum reclaims dead tuples (from MVCC), updates planner statistics (like `ANALYZE`), and prevents transaction ID wraparound (see M55).

**Statistics Collector:**
Collects query execution statistics and writes them to `pg_stat_*` views. Backends send UDP messages to the statistics collector after each query or table access. The statistics collector aggregates them and materializes them into the statistics views. In Postgres 15+, the statistics collector was replaced with shared memory statistics (no separate process).

**WAL Receiver (replica only):**
On a streaming replication standby, the WAL receiver connects to the primary, receives WAL records, and writes them to the standby's WAL. The startup process then replays them against the standby's data files.

**Logical Replication Worker:**
For logical replication (CDC source), the walsender process on the primary streams decoded logical changes to subscribers. On the subscriber side, a logical replication worker applies the changes to local tables.

### 3.4 Shared Memory Layout

```
Postgres Shared Memory Segment (allocated at startup):

┌─────────────────────────────────────────────────────────────────────────┐
│  Shared Buffer Pool                                                      │
│  Size: shared_buffers (default 128MB)                                   │
│  Contains: database pages (8KB each)                                    │
│  Access: all backends, bgwriter, checkpointer                           │
├─────────────────────────────────────────────────────────────────────────┤
│  WAL Buffers                                                             │
│  Size: wal_buffers (default = 1/32 of shared_buffers, max 16MB)        │
│  Contains: WAL records pending fsync to WAL files                       │
│  Access: backends write; WAL writer flushes                             │
├─────────────────────────────────────────────────────────────────────────┤
│  Lock Table (LockMethodTable)                                            │
│  Contains: all currently held locks (spinlocks, lightweight locks,       │
│            regular locks, advisory locks)                                │
│  Access: all backends on every lock acquire/release                     │
├─────────────────────────────────────────────────────────────────────────┤
│  Procarray                                                               │
│  Contains: per-backend PGPROC struct                                    │
│    pid, database, username, transaction xid,                            │
│    oldest active xmin (for MVCC visibility)                             │
│  Access: all backends on every MVCC snapshot acquisition                │
├─────────────────────────────────────────────────────────────────────────┤
│  Commit Log (CLOG) / Transaction Status                                  │
│  Contains: commit/abort status of every transaction ID                  │
│  Access: backends on MVCC visibility checks                             │
├─────────────────────────────────────────────────────────────────────────┤
│  Multixact State                                                         │
│  Contains: shared row-level lock state (FOR SHARE multiple holders)     │
├─────────────────────────────────────────────────────────────────────────┤
│  Background Worker Registry                                              │
│  Contains: registered background workers and their state                │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.5 The Connection Lifecycle

A complete Postgres connection from client to query result:

```
1. CLIENT: TCP connect to host:5432
2. POSTMASTER: accept() on listening socket
3. POSTMASTER: fork() → new BACKEND process
4. BACKEND: SSL negotiation (if configured)
5. BACKEND: Authentication (pg_hba.conf: trust, md5, scram-sha-256, cert, ...)
6. BACKEND: Send ReadyForQuery message to client
7. CLIENT: Send query ("SELECT * FROM users WHERE id = 1")
8. BACKEND: Parse (tokenize SQL → parse tree)
9. BACKEND: Analyze (resolve names → query tree)
10. BACKEND: Plan (optimize → physical plan with operators)
11. BACKEND: Execute (fetch pages from buffer pool, evaluate predicates, return rows)
12. BACKEND: Send RowDescription + DataRow + CommandComplete messages
13. BACKEND: Send ReadyForQuery (waiting for next query)
14. CLIENT: Send Terminate message
15. BACKEND: exit()
16. POSTMASTER: SIGCHLD → clean up shared memory state (lock table, procarray)
```

### 3.6 WAL Writer and Checkpointer Interaction

The WAL writer and checkpointer coordinate to provide durability with acceptable I/O overhead:

```
Time →
Backend commits T1 → WAL record in WAL buffer → WAL writer flushes (every 200ms or on full buffer)
Backend commits T2 → WAL record in WAL buffer ↗
Backend commits T3 → WAL record in WAL buffer ↗
                      (group commit: multiple commits share one WAL fsync)

Every checkpoint_timeout (5min):
  checkpointer:
    1. Write CHECKPOINT_START to WAL
    2. Begin flushing dirty pages from buffer pool (spread over 4.5min = 0.9 × 5min)
    3. Write CHECKPOINT_END to WAL with redo_lsn
    4. Update pg_control file with new checkpoint LSN

Recovery after crash:
  Read pg_control → find last checkpoint LSN
  Replay WAL from checkpoint_redo_lsn to end of WAL
  Redo all committed changes; undo all uncommitted
```

---

## 4. Hands-On Walkthrough

### 4.1 Inspecting Postgres Processes

```bash
# List all Postgres processes (Linux)
ps aux | grep postgres

# Typical output for a running cluster with 3 connections:
# postgres  1234  0.0  0.0  ... postgres: postmaster
# postgres  1235  0.0  0.1  ... postgres: checkpointer
# postgres  1236  0.0  0.1  ... postgres: background writer
# postgres  1237  0.0  0.1  ... postgres: walwriter
# postgres  1238  0.0  0.0  ... postgres: autovacuum launcher
# postgres  1239  0.0  0.0  ... postgres: stats collector   (pre-PG15)
# postgres  1240  0.0  0.0  ... postgres: logical replication launcher
# postgres  1301  0.5  0.2  ... postgres: mydb myuser [local] idle
# postgres  1302  0.8  0.3  ... postgres: mydb myuser [local] SELECT
# postgres  1303  0.0  0.2  ... postgres: mydb myuser [local] idle in transaction

# Process tree
pstree -p $(pgrep -o postgres)
# postgres(1234)─┬─postgres(1235)   # checkpointer
#                ├─postgres(1236)   # bgwriter
#                ├─postgres(1237)   # walwriter
#                ├─postgres(1238)   # autovacuum launcher
#                ├─postgres(1301)   # backend: idle
#                ├─postgres(1302)   # backend: SELECT
#                └─postgres(1303)   # backend: idle in transaction

# Shared memory segment
ipcs -m
# ------ Shared Memory Segments --------
# key        shmid      owner      perms      bytes      nattch
# 0x0052e2c1 196608     postgres   600        134217728  8
# 134217728 bytes = 128MB = shared_buffers default size
```

### 4.2 Inspecting Background Process Activity via SQL

```sql
-- ── Active queries and process states ────────────────────────────────────────
SELECT
    pid,
    usename,
    application_name,
    state,
    wait_event_type,
    wait_event,
    left(query, 80) AS query_snippet,
    now() - query_start AS query_age
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY query_start;

-- Common states:
-- 'active'            → currently running a query
-- 'idle'              → waiting for next command (connection is open, no query)
-- 'idle in transaction' → transaction open but no active query (dangerous: holds locks!)
-- 'idle in transaction (aborted)' → aborted transaction not yet rolled back

-- Common wait events:
-- Lock/relation     → waiting for a table lock
-- Lock/tuple        → waiting for a row lock
-- IO/DataFileRead   → reading a page from disk (buffer pool miss)
-- IO/WALWrite       → backend waiting for WAL fsync
-- Client/ClientRead → waiting for the client to send the next command

-- ── Background process health ─────────────────────────────────────────────────
SELECT * FROM pg_stat_bgwriter;
-- Key columns:
--   checkpoints_timed   → scheduled (good)
--   checkpoints_req     → emergency, WAL exceeded max_wal_size (increase max_wal_size)
--   buffers_checkpoint  → dirty pages flushed by checkpointer
--   buffers_clean       → dirty pages flushed by bgwriter (proactive)
--   buffers_backend     → dirty pages flushed by backends (bad! means bgwriter is behind)
--   maxwritten_clean    → times bgwriter was stopped by bgwriter_lru_maxpages limit

-- ── WAL statistics ────────────────────────────────────────────────────────────
SELECT
    wal_records,
    wal_fpi,
    pg_size_pretty(wal_bytes) AS wal_bytes,
    wal_sync,
    round(wal_sync_time::numeric, 2) AS sync_ms
FROM pg_stat_wal;

-- ── Autovacuum activity ───────────────────────────────────────────────────────
SELECT
    schemaname, relname,
    last_vacuum, last_autovacuum,
    last_analyze, last_autoanalyze,
    n_dead_tup, n_live_tup,
    round(n_dead_tup::numeric / nullif(n_live_tup + n_dead_tup, 0) * 100, 2) AS dead_pct
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC
LIMIT 10;

-- ── Lock contention ──────────────────────────────────────────────────────────
SELECT
    blocked.pid         AS blocked_pid,
    blocked.query       AS blocked_query,
    blocking.pid        AS blocking_pid,
    blocking.query      AS blocking_query,
    blocked_locks.locktype
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked
    ON blocked.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking
    ON blocking.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
postgres_architecture.py

Simulate key aspects of Postgres's multi-process architecture in Python.

Components:
  - SharedMemorySegment: Simulates shared_buffers + WAL buffers + lock table + procarray
  - BackendProcess: Simulates a client connection backend
  - WALWriter: Background WAL flusher
  - Checkpointer: Background dirty-page flusher
  - BGWriter: Proactive page cleaner
  - Postmaster: Process manager (spawns and monitors backends)
  - ConnectionPool: Demonstrates the overhead of process-per-connection
  - ArchitectureMonitor: Collects pg_stat_bgwriter-equivalent metrics

Design mirrors Postgres architecture:
  - One shared memory segment for all processes
  - Process-per-connection model (simulated with threads)
  - WAL writer batches WAL flushes across concurrent commits (group commit)
  - Checkpointer spreads dirty page flushes over checkpoint interval
  - Postmaster detects crashed backends and cleans up

No external dependencies.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Optional


# ─── Shared Memory Segment ────────────────────────────────────────────────────

@dataclass
class SharedPage:
    page_id:     int
    data:        dict = field(default_factory=dict)
    dirty:       bool = False
    lsn:         int  = 0
    pin_count:   int  = 0


class SharedMemorySegment:
    """
    Simulates Postgres shared memory: buffer pool + WAL buffers + lock table + procarray.
    In production: allocated via shmget() at postmaster startup.
    All backend processes attach to this segment via shmat().
    """

    def __init__(self, buffer_pool_size: int = 64, wal_buffer_kb: int = 512):
        # Buffer pool
        self._pages: dict[int, SharedPage]   = {}
        self._pool_size = buffer_pool_size
        self._lock = threading.Lock()

        # WAL buffers
        self._wal_buffer: list[dict]         = []
        self._wal_lsn: int                   = 0
        self._wal_flush_lsn: int             = 0
        self._wal_lock = threading.Lock()

        # Lock table
        self._row_locks: dict[str, int]      = {}   # key → holding pid
        self._lock_table_lock = threading.Lock()

        # Procarray: active backends and their xmin
        self._procarray: dict[int, dict]     = {}   # pid → {xid, xmin, state}
        self._procarray_lock = threading.Lock()

        # Commit log: xid → 'committed' | 'aborted' | 'in_progress'
        self._clog: dict[int, str]           = {}
        self._next_xid: int                  = 1
        self._clog_lock = threading.Lock()

        # Stats
        self.stats = {
            'buffers_alloc':      0,
            'buffers_hit':        0,
            'buffers_read':       0,
            'wal_records':        0,
            'wal_flushes':        0,
        }

    # ── Buffer Pool ──────────────────────────────────────────────────────────

    def get_page(self, page_id: int) -> SharedPage:
        """Fetch a page (cache hit or simulated disk read)."""
        with self._lock:
            if page_id in self._pages:
                self.stats['buffers_hit'] += 1
                return self._pages[page_id]
            self.stats['buffers_read'] += 1
            self.stats['buffers_alloc'] += 1
            page = SharedPage(page_id)
            self._pages[page_id] = page
            return page

    def dirty_page_count(self) -> int:
        with self._lock:
            return sum(1 for p in self._pages.values() if p.dirty)

    def flush_dirty_pages(self, max_pages: Optional[int] = None) -> int:
        """Flush dirty pages (simulates checkpointer/bgwriter disk write)."""
        flushed = 0
        with self._lock:
            for page in list(self._pages.values()):
                if max_pages and flushed >= max_pages:
                    break
                if page.dirty and page.lsn <= self._wal_flush_lsn:
                    page.dirty = False
                    flushed += 1
        return flushed

    # ── WAL Buffers ──────────────────────────────────────────────────────────

    def write_wal(self, xid: int, record_type: str, payload: dict) -> int:
        """Write a WAL record to the in-memory WAL buffer."""
        with self._wal_lock:
            self._wal_lsn += len(str(payload)) + 24   # simulate record size
            lsn = self._wal_lsn
            self._wal_buffer.append({
                'lsn':    lsn,
                'xid':    xid,
                'type':   record_type,
                'payload': payload
            })
            self.stats['wal_records'] += 1
            return lsn

    def flush_wal(self) -> int:
        """Flush WAL buffer to disk (simulated fsync). Returns new flush LSN."""
        with self._wal_lock:
            self._wal_flush_lsn = self._wal_lsn
            self._wal_buffer.clear()
            self.stats['wal_flushes'] += 1
            return self._wal_flush_lsn

    # ── Transaction Management ────────────────────────────────────────────────

    def begin_transaction(self, pid: int) -> int:
        with self._clog_lock:
            xid = self._next_xid
            self._next_xid += 1
            self._clog[xid] = 'in_progress'
        with self._procarray_lock:
            self._procarray[pid] = {'xid': xid, 'state': 'active'}
        return xid

    def commit_transaction(self, pid: int, xid: int):
        with self._clog_lock:
            self._clog[xid] = 'committed'
        with self._procarray_lock:
            if pid in self._procarray:
                self._procarray[pid]['state'] = 'idle'

    def abort_transaction(self, pid: int, xid: int):
        with self._clog_lock:
            self._clog[xid] = 'aborted'
        with self._procarray_lock:
            if pid in self._procarray:
                self._procarray[pid]['state'] = 'idle'

    def unregister_backend(self, pid: int):
        with self._procarray_lock:
            self._procarray.pop(pid, None)

    # ── Lock Table ────────────────────────────────────────────────────────────

    def acquire_row_lock(self, key: str, pid: int, timeout_s: float = 2.0) -> bool:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            with self._lock_table_lock:
                if key not in self._row_locks:
                    self._row_locks[key] = pid
                    return True
            time.sleep(0.01)
        return False  # Lock timeout

    def release_row_lock(self, key: str, pid: int):
        with self._lock_table_lock:
            if self._row_locks.get(key) == pid:
                del self._row_locks[key]

    def active_lock_count(self) -> int:
        with self._lock_table_lock:
            return len(self._row_locks)

    # ── Procarray / Visibility ─────────────────────────────────────────────────

    def get_snapshot(self) -> dict:
        """
        Take an MVCC snapshot: capture xmin (oldest active xid) and list of active xids.
        Used to determine which tuple versions are visible to this transaction.
        """
        with self._procarray_lock:
            active_xids = {v['xid'] for v in self._procarray.values()
                          if v.get('state') == 'active'}
        with self._clog_lock:
            # xmin = current next_xid (all xids below this were started before snapshot)
            snapshot_xmax = self._next_xid
        return {
            'snapshot_xmax': snapshot_xmax,
            'active_xids':   active_xids,
        }

    def is_visible(self, xid: int, snapshot: dict) -> bool:
        """MVCC visibility check: is this xid's data visible in the given snapshot?"""
        with self._clog_lock:
            status = self._clog.get(xid, 'in_progress')
        if status == 'committed':
            return xid < snapshot['snapshot_xmax'] and xid not in snapshot['active_xids']
        return False


# ─── Background Processes ─────────────────────────────────────────────────────

class WALWriter(threading.Thread):
    """
    Background WAL writer: flushes WAL buffers to disk periodically.
    Simulates PostgreSQL's walwriter process.
    """

    def __init__(self, shm: SharedMemorySegment, delay_s: float = 0.2):
        super().__init__(daemon=True, name="walwriter")
        self.shm      = shm
        self.delay_s  = delay_s
        self._running = True
        self.flushes  = 0

    def run(self):
        while self._running:
            time.sleep(self.delay_s)
            flushed_lsn = self.shm.flush_wal()
            if flushed_lsn > 0:
                self.flushes += 1

    def stop(self):
        self._running = False


class BGWriter(threading.Thread):
    """
    Background writer: proactively flushes dirty pages from the buffer pool.
    Simulates PostgreSQL's bgwriter process.
    """

    def __init__(self, shm: SharedMemorySegment, delay_s: float = 0.1,
                 max_pages_per_round: int = 100):
        super().__init__(daemon=True, name="bgwriter")
        self.shm = shm
        self.delay_s = delay_s
        self.max_pages = max_pages_per_round
        self._running = True
        self.pages_written = 0

    def run(self):
        while self._running:
            time.sleep(self.delay_s)
            written = self.shm.flush_dirty_pages(max_pages=self.max_pages)
            self.pages_written += written

    def stop(self):
        self._running = False


class Checkpointer(threading.Thread):
    """
    Checkpointer: periodically flushes ALL dirty pages and writes a checkpoint WAL record.
    Simulates PostgreSQL's checkpointer process.
    """

    def __init__(self, shm: SharedMemorySegment, interval_s: float = 5.0,
                 completion_target: float = 0.9):
        super().__init__(daemon=True, name="checkpointer")
        self.shm = shm
        self.interval_s         = interval_s
        self.completion_target  = completion_target
        self._running           = True
        self.checkpoints_timed  = 0
        self.checkpoints_req    = 0
        self.buffers_checkpoint = 0

    def run(self):
        while self._running:
            time.sleep(self.interval_s)
            self._do_checkpoint(trigger='timed')

    def _do_checkpoint(self, trigger: str = 'timed'):
        """
        Checkpoint: flush WAL first, then flush all dirty pages.
        Spreads page flushes over completion_target × interval.
        """
        # 1. Flush WAL (write CHECKPOINT_START equivalent)
        self.shm.flush_wal()

        # 2. Flush dirty pages (spread over completion_target × interval)
        flush_budget_s = self.interval_s * self.completion_target
        flush_start = time.time()
        flushed = 0

        while (time.time() - flush_start) < flush_budget_s:
            n = self.shm.flush_dirty_pages(max_pages=10)
            flushed += n
            if n == 0:
                break
            time.sleep(0.05)

        self.buffers_checkpoint += flushed
        if trigger == 'timed':
            self.checkpoints_timed += 1
        else:
            self.checkpoints_req += 1

    def stop(self):
        self._running = False


# ─── Backend Process ──────────────────────────────────────────────────────────

class BackendProcess(threading.Thread):
    """
    Simulates a Postgres backend process (one per client connection).
    Authenticates, receives queries, executes them via shared memory.
    """

    def __init__(self, pid: int, shm: SharedMemorySegment,
                 n_queries: int = 10, think_time_s: float = 0.05):
        super().__init__(daemon=True, name=f"backend-{pid}")
        self.pid          = pid
        self.shm          = shm
        self.n_queries    = n_queries
        self.think_time_s = think_time_s
        self._rng         = random.Random(pid)
        self.queries_run  = 0
        self.lock_waits   = 0

    def run(self):
        """Simulate a series of transactions."""
        for _ in range(self.n_queries):
            time.sleep(self.think_time_s * (0.5 + self._rng.random()))
            self._execute_transaction()
        self.shm.unregister_backend(self.pid)

    def _execute_transaction(self):
        # Begin
        xid = self.shm.begin_transaction(self.pid)
        page_id = self._rng.randint(0, 99)
        key = f"row:{page_id}:{self._rng.randint(0, 9)}"

        # Acquire row lock
        acquired = self.shm.acquire_row_lock(key, self.pid)
        if not acquired:
            self.lock_waits += 1
            self.shm.abort_transaction(self.pid, xid)
            return

        try:
            # Read page
            page = self.shm.get_page(page_id)

            # Modify page
            page.data[key] = f"value-{xid}"
            page.dirty = True

            # Write WAL
            lsn = self.shm.write_wal(xid, 'WRITE', {'page_id': page_id, 'key': key})
            page.lsn = lsn

            # Commit (WAL record first; backend may fsync if synchronous_commit=on)
            self.shm.write_wal(xid, 'COMMIT', {})
            self.shm.commit_transaction(self.pid, xid)
        finally:
            self.shm.release_row_lock(key, self.pid)

        self.queries_run += 1


# ─── Postmaster ───────────────────────────────────────────────────────────────

class Postmaster:
    """
    Simulates the Postgres postmaster:
    - Allocates shared memory
    - Starts background processes
    - Accepts connections (spawns backends)
    - Monitors child processes
    """

    def __init__(self, buffer_pool_size: int = 64):
        print("Postmaster: allocating shared memory segment...")
        self.shm            = SharedMemorySegment(buffer_pool_size)
        self._backends:  list[BackendProcess]   = []
        self._wal_writer: Optional[WALWriter]   = None
        self._bgwriter:  Optional[BGWriter]     = None
        self._checkpointer: Optional[Checkpointer] = None

    def start(self):
        """Start background processes (equivalent to pg_ctl start)."""
        print("Postmaster: starting background processes...")
        self._wal_writer   = WALWriter(self.shm)
        self._bgwriter     = BGWriter(self.shm)
        self._checkpointer = Checkpointer(self.shm, interval_s=2.0)
        self._wal_writer.start()
        self._bgwriter.start()
        self._checkpointer.start()
        print("Postmaster: ready to accept connections")

    def accept_connection(self, pid: int, n_queries: int = 10) -> BackendProcess:
        """Fork a backend process for a new connection."""
        backend = BackendProcess(pid, self.shm, n_queries)
        self._backends.append(backend)
        backend.start()
        return backend

    def wait_for_backends(self):
        """Wait for all backends to finish (graceful shutdown)."""
        for b in self._backends:
            b.join()

    def stop(self):
        """Shutdown: stop background processes."""
        if self._wal_writer:
            self._wal_writer.stop()
        if self._bgwriter:
            self._bgwriter.stop()
        if self._checkpointer:
            self._checkpointer.stop()

    def print_stats(self):
        print("\n  Postmaster Statistics:")
        print(f"    Active backends:      {sum(1 for b in self._backends if b.is_alive())}")
        print(f"    Total backends run:   {len(self._backends)}")
        total_queries = sum(b.queries_run for b in self._backends)
        total_waits   = sum(b.lock_waits for b in self._backends)
        print(f"    Total queries run:    {total_queries:,}")
        print(f"    Lock waits:           {total_waits}")
        print(f"\n  Shared Memory Stats:")
        print(f"    Buffer pool pages:    {len(self.shm._pages)}")
        print(f"    Dirty pages:          {self.shm.dirty_page_count()}")
        print(f"    Buffer hits:          {self.shm.stats['buffers_hit']:,}")
        print(f"    Buffer reads:         {self.shm.stats['buffers_read']:,}")
        hit_total = self.shm.stats['buffers_hit'] + self.shm.stats['buffers_read']
        hit_rate  = self.shm.stats['buffers_hit'] / max(1, hit_total) * 100
        print(f"    Buffer hit rate:      {hit_rate:.1f}%")
        print(f"    WAL records:          {self.shm.stats['wal_records']:,}")
        print(f"    WAL flushes:          {self.shm.stats['wal_flushes']:,}")
        if self._bgwriter:
            print(f"\n  BGWriter:")
            print(f"    Pages written:        {self._bgwriter.pages_written:,}")
        if self._checkpointer:
            print(f"\n  Checkpointer:")
            print(f"    Checkpoints (timed):  {self._checkpointer.checkpoints_timed}")
            print(f"    Buffers flushed:      {self._checkpointer.buffers_checkpoint:,}")


# ─── Connection Overhead Demo ─────────────────────────────────────────────────

def demo_connection_overhead():
    """
    Demonstrate why process-per-connection is expensive at high concurrency.
    Shows memory overhead per connection and latency from fork().
    """
    print("\n─── Connection Overhead Demo ───")
    print("  Process-per-connection model implications:")
    print()

    # Memory math
    per_backend_mb = 10    # stack + local buffers (typical)
    shared_buffers_mb = 512

    for n_conns in [10, 50, 100, 500, 1000]:
        backend_mem = n_conns * per_backend_mb
        total_mem   = backend_mem + shared_buffers_mb
        print(f"  {n_conns:5d} connections: "
              f"{backend_mem:5d}MB backends + {shared_buffers_mb}MB shared = "
              f"{total_mem:6d}MB total")

    print()
    print("  At 1000 connections: 10GB RAM consumed by backends alone.")
    print("  Solution: PgBouncer connection pool (M56) — multiplex many")
    print("            app connections over few actual Postgres backends.")


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Postgres Architecture Simulation ===\n")

    # ── Phase 1: Start Postmaster and Background Processes ───────────────────
    print("─── Phase 1: Postmaster Startup ───")
    postmaster = Postmaster(buffer_pool_size=64)
    postmaster.start()
    time.sleep(0.2)

    # ── Phase 2: Simulate Client Connections (Backend Processes) ─────────────
    print("\n─── Phase 2: Client Connections ───")
    print("  Spawning 10 backend processes (10 concurrent connections)...")
    for pid in range(1001, 1011):
        postmaster.accept_connection(pid, n_queries=20)

    # Let them run
    postmaster.wait_for_backends()
    print("  All backends finished.")

    # ── Phase 3: Background Process Stats ────────────────────────────────────
    print("\n─── Phase 3: Architecture Statistics ───")
    postmaster.print_stats()

    # ── Phase 4: MVCC Snapshot Demo ──────────────────────────────────────────
    print("\n─── Phase 4: MVCC Snapshot (Procarray) ───")
    shm = postmaster.shm
    # Start two transactions
    xid1 = shm.begin_transaction(pid=9001)
    xid2 = shm.begin_transaction(pid=9002)
    # Take a snapshot before either commits
    snap = shm.get_snapshot()
    print(f"  Snapshot taken: xmax={snap['snapshot_xmax']}, "
          f"active_xids={snap['active_xids']}")
    # Commit xid1
    shm.commit_transaction(9001, xid1)
    # Check visibility:
    #   xid1: committed BUT was active when snapshot was taken → NOT visible
    #   xid2: still in progress → NOT visible
    print(f"  xid1 (committed after snapshot start) visible? "
          f"{shm.is_visible(xid1, snap)}")
    print(f"  xid2 (still in progress) visible? {shm.is_visible(xid2, snap)}")
    # New snapshot after xid1 committed → xid1 now visible
    snap2 = shm.get_snapshot()
    shm.commit_transaction(9002, xid2)
    snap3 = shm.get_snapshot()
    print(f"  xid1 visible in snap2 (after xid1 commit)? "
          f"{shm.is_visible(xid1, snap3)}")

    # ── Phase 5: Connection Overhead ─────────────────────────────────────────
    demo_connection_overhead()

    # ── Phase 6: Lock Contention ──────────────────────────────────────────────
    print("\n─── Phase 6: Lock Table ───")
    shm2 = SharedMemorySegment()
    # Two backends fighting for the same row
    key = "row:42:0"
    acquired1 = shm2.acquire_row_lock(key, pid=2001)
    print(f"  Backend 2001 acquired lock on '{key}': {acquired1}")
    acquired2 = shm2.acquire_row_lock(key, pid=2002, timeout_s=0.1)
    print(f"  Backend 2002 tried to acquire same lock (timeout 0.1s): {acquired2}")
    shm2.release_row_lock(key, pid=2001)
    acquired3 = shm2.acquire_row_lock(key, pid=2002, timeout_s=0.1)
    print(f"  Backend 2002 after 2001 released: {acquired3}")
    shm2.release_row_lock(key, pid=2002)

    postmaster.stop()
    print("\n  Postmaster stopped.")
```

---

## 6. Visual Reference

### Postgres Process Architecture

```
                    ┌──────────────────────────────────┐
                    │  POSTMASTER (PID 1234)            │
                    │  Listens on :5432                  │
                    │  Manages shared memory lifecycle   │
                    └───────────────┬──────────────────┘
                                    │ fork() per connection + start background procs
        ┌───────────────────────────┼─────────────────────────────────────────┐
        │                           │                                         │
   Background Processes             │                              Client Connections
        │                           │
  ┌─────┴──────────────────────┐   │   ┌─────────────────────────────────────┐
  │ walwriter  (200ms flush)   │   │   │ backend (pid=1301): idle             │
  │ bgwriter   (100ms sweep)   │   │   │ backend (pid=1302): SELECT ...       │
  │ checkpointer (5min cycle)  │   │   │ backend (pid=1303): idle in txn ← ⚠ │
  │ autovacuum launcher        │   │   │ backend (pid=1304): INSERT ...       │
  │ autovacuum worker(s)       │   │   └─────────────────────────────────────┘
  │ stats collector (pre-PG15) │   │
  │ logical replication worker │   │
  └────────────────────────────┘   │
                                    │
              ┌─────────────────────┴──────────────────────────────────────────┐
              │                    SHARED MEMORY                                │
              │                                                                 │
              │  ┌─────────────────────────────┐  ┌──────────────────────────┐ │
              │  │  Shared Buffer Pool (8KB pp) │  │  WAL Buffers (16MB)      │ │
              │  │  shared_buffers = 128MB      │  │  WAL records → walwriter │ │
              │  │  All backends R/W pages here │  └──────────────────────────┘ │
              │  └─────────────────────────────┘                                │
              │  ┌─────────────────────────────┐  ┌──────────────────────────┐ │
              │  │  Lock Table                  │  │  Procarray               │ │
              │  │  All held locks              │  │  All backend PIDs + xids │ │
              │  │  Deadlock detection graph    │  │  MVCC snapshots          │ │
              │  └─────────────────────────────┘  └──────────────────────────┘ │
              │  ┌─────────────────────────────┐                                │
              │  │  CLOG (Commit Log)           │                                │
              │  │  xid → committed/aborted     │                                │
              │  └─────────────────────────────┘                                │
              └────────────────────────────────────────────────────────────────┘
```

### WAL Writer + Checkpointer Timeline

```
Time →  0s                    5s                    10s
        │                     │                     │
WAL:    ├──W──W──W──W──W──W──┼──W──W──W──W──W──W──┤  (W = WAL buffer write)
        │    ↓ every 200ms   │                     │
        │    WAL flush        │                     │
Buffer: ├──D──D──D──D──D──D──┼──D──D──D──D──D──D──┤  (D = page dirtied)
        │      ↓ bgwriter     │                     │
        │      cleans some    │                     │
        │                     │ CHECKPOINT (5min)   │
        │                     │ flush ALL dirty     │
        │                     │ pages over 4.5min   │
        │                     │ ←────────────────── │

Recovery point after checkpoint:
  Only need WAL from checkpoint LSN forward
  All pages before checkpoint LSN are on disk
```

---

## 7. Common Mistakes

**Mistake 1: Opening too many direct Postgres connections from the application.** Each connection is an OS process (~10MB RAM, fork() overhead, OS scheduling slot). An application with 200 app servers × 20 thread pool connections = 4,000 Postgres backends. At 10MB each: 40GB RAM consumed by connections alone, before any data is cached. Postgres starts slowing dramatically above ~100-200 active connections due to shared memory contention (lock table, procarray). The fix: PgBouncer in transaction-mode pooling, reducing 4,000 app connections to 20-50 actual Postgres backends.

**Mistake 2: Leaving `idle in transaction` connections open.** A connection in `idle in transaction` state holds an open transaction with an MVCC snapshot. This means: (1) autovacuum cannot remove dead tuples visible to this snapshot (table bloat accumulates), (2) the transaction holds any locks it acquired (blocking other writers), (3) it keeps the oldest active xid from advancing (causing transaction ID pressure). Set `idle_in_transaction_session_timeout = '5min'` to terminate connections that hold transactions idle for more than 5 minutes.

**Mistake 3: Assuming `shared_buffers = 25% of RAM` is always right without checking actual buffer hit rate.** The 25% rule is a starting point, not a formula. For a database where the entire hot working set is 2GB on a 64GB server, `shared_buffers = 16GB` wastes 14GB that the OS page cache would use more efficiently. Check `pg_statio_user_tables` and `pg_buffercache` to measure the actual hot working set. Size `shared_buffers` to match it, not to a fixed percentage.

---

## 8. Production Failure Scenarios

### Scenario 1: Out-of-Memory Crash from Too Many Connections

**Symptoms:** Postgres crashes with `OOM kill` (kernel: `Out of memory: Kill process postgres`). The Postgres logs show `LOG: connection received: host=app.internal` at 3,000+ connections. The application reports `FATAL: sorry, too many clients already` just before the crash.

**Root cause:** 3,000 connections × 10MB per backend = 30GB + 8GB `shared_buffers` = 38GB, exceeding the server's 32GB RAM. The Linux OOM killer killed the Postgres postmaster. All connections were immediately dropped.

**Fix:**
```bash
# Immediate: add PgBouncer in front of Postgres
# pgbouncer.ini:
[databases]
mydb = host=127.0.0.1 port=5432 dbname=mydb

[pgbouncer]
pool_mode = transaction
max_client_conn = 5000       # App can open up to 5000 connections to PgBouncer
default_pool_size = 25       # Only 25 actual Postgres backends per database
server_pool_size = 25
listen_port = 6432

# In Postgres: enforce connection limits as a safety net
ALTER SYSTEM SET max_connections = 100;   # Hard limit (requires restart)
SELECT pg_reload_conf();

# Monitor connections going forward
SELECT count(*), state FROM pg_stat_activity GROUP BY state;
```

### Scenario 2: Checkpointer I/O Causing Query Latency Spikes

**Symptoms:** Every 5 minutes, P99 query latency spikes from 3ms to 150ms for approximately 30 seconds, then returns to normal. `pg_stat_bgwriter.checkpoints_timed` increases on schedule; `checkpoint_write_time` shows 28,000ms per checkpoint.

**Root cause:** The checkpointer flushes 28 seconds worth of dirty pages at the beginning of each checkpoint, creating an I/O burst that saturates the SSD write queue and starves the buffer pool's read path. The default `checkpoint_completion_target = 0.5` means the checkpointer tries to flush all dirty pages in the first 50% of the checkpoint interval (2.5 minutes), then the second half is idle — creating uneven I/O.

**Fix:**
```sql
-- Spread checkpoint I/O over 90% of the interval
ALTER SYSTEM SET checkpoint_completion_target = 0.9;

-- Increase the checkpoint interval to reduce frequency
ALTER SYSTEM SET checkpoint_timeout = '10min';

-- Give the bgwriter more headroom to pre-clean pages
ALTER SYSTEM SET bgwriter_lru_maxpages = 200;
ALTER SYSTEM SET bgwriter_delay = '100ms';

SELECT pg_reload_conf();

-- Verify improvement: watch pg_stat_bgwriter.checkpoint_write_time
-- It should now show smaller write_time values (more spread out)
SELECT checkpoints_timed, checkpoint_write_time, checkpoint_sync_time
FROM pg_stat_bgwriter;
```

---

## 9. Performance and Tuning

### Key Architecture Parameters

```sql
-- ── Connection and Process Settings ──────────────────────────────────────────
-- max_connections: hard limit on client connections (requires restart)
-- Each additional connection = ~10MB RAM + OS process overhead
-- Rule: max_connections should be < 100 for OLTP (use PgBouncer for more)
ALTER SYSTEM SET max_connections = 100;

-- ── Shared Memory ─────────────────────────────────────────────────────────────
-- shared_buffers: buffer pool size
-- Rule: 25-40% of RAM, but match actual hot working set
-- Requires restart to change
ALTER SYSTEM SET shared_buffers = '8GB';    -- For 32GB RAM server

-- wal_buffers: WAL in-memory buffer before walwriter flushes
-- Default: 1/32 of shared_buffers (min 64KB, max 16MB)
-- Increase if pg_stat_wal.wal_buffers_full > 0
ALTER SYSTEM SET wal_buffers = '16MB';

-- ── Background Writer ─────────────────────────────────────────────────────────
ALTER SYSTEM SET bgwriter_delay = '100ms';          -- How often bgwriter runs
ALTER SYSTEM SET bgwriter_lru_maxpages = 200;       -- Max pages per run
ALTER SYSTEM SET bgwriter_lru_multiplier = 4.0;     -- Look-ahead aggressiveness

-- ── Checkpointer ──────────────────────────────────────────────────────────────
ALTER SYSTEM SET checkpoint_timeout = '10min';
ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;

-- ── Autovacuum (tuned for high-write tables) ──────────────────────────────────
ALTER TABLE high_write_table SET (
    autovacuum_vacuum_scale_factor = 0.01,  -- Trigger at 1% dead tuples
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_cost_delay = 2        -- Less I/O throttling
);

-- ── Connection Safety ─────────────────────────────────────────────────────────
ALTER SYSTEM SET idle_in_transaction_session_timeout = '5min';
ALTER SYSTEM SET statement_timeout = '30s';    -- Kill runaway queries
SELECT pg_reload_conf();
```

### Architecture Observability Checklist

```sql
-- 1. Connection count by state
SELECT state, count(*) FROM pg_stat_activity GROUP BY state ORDER BY count(*) DESC;

-- 2. Long-running queries
SELECT pid, now() - query_start AS age, left(query, 80)
FROM pg_stat_activity WHERE state = 'active' ORDER BY age DESC LIMIT 10;

-- 3. Idle-in-transaction connections (holding locks and xmin)
SELECT pid, now() - xact_start AS txn_age, left(query, 50) AS last_query
FROM pg_stat_activity WHERE state = 'idle in transaction' ORDER BY txn_age DESC;

-- 4. Lock waiters
SELECT count(*) FROM pg_locks WHERE NOT granted;

-- 5. Buffer hit rate
SELECT
    sum(blks_hit)::float / nullif(sum(blks_hit) + sum(blks_read), 0) * 100 AS hit_pct
FROM pg_stat_database;

-- 6. Bgwriter health (buffers_backend should be near 0)
SELECT buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_req
FROM pg_stat_bgwriter;

-- 7. WAL generation rate
SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) AS total_wal;
```

---

## 10. Interview Q&A

**Q1: Why does Postgres use a process-per-connection model instead of threads?**

Postgres's process-per-connection model predates reliable POSIX threads (pthreads). When Postgres was architected in the early 1990s, threads were implementation-specific, often buggy, and varied dramatically between Unix variants. Processes provided isolation, crash safety, and portability.

The architectural consequences are significant. Each connection is an independent OS process with its own stack, virtual address space, and OS scheduling slot. All backend processes share a single region of shared memory — the buffer pool, WAL buffers, lock table, and procarray — via POSIX shared memory (`shmget`). A crash in one backend process does not crash others; the postmaster detects the crash via `SIGCHLD` and cleans up the crashed backend's shared memory state without affecting running queries.

The cost is connection overhead: each backend process consumes approximately 10MB of RAM (stack, local buffers, work memory for in-flight queries) plus an OS process scheduling slot. At 100 connections this is manageable; at 1,000 connections the process overhead becomes significant (10GB of RAM for connections alone, plus contention on shared memory structures like the lock table and procarray). This is why PgBouncer and other connection poolers are essential for Postgres at scale — they multiplex thousands of application connections over tens of actual Postgres backend processes.

**Q2: What does the autovacuum process do and why is it critical for Postgres correctness, not just performance?**

Autovacuum has two correctness functions, not just one performance function. The performance function everyone knows: it reclaims storage occupied by dead tuples (old MVCC versions of updated or deleted rows), reducing table bloat and maintaining planner statistics.

The correctness function is transaction ID wraparound prevention. Postgres uses 32-bit transaction IDs (xids) — a 4-billion xid space. Transaction IDs are assigned sequentially; when they reach the maximum, they wrap around. If a live tuple was created by xid 1,000,000 and the database has wrapped around to xid 1,000,001, Postgres cannot determine whether xid 1,000,000 is "in the past" (the tuple is visible) or "in the future" (the tuple is not yet visible). Postgres prevents this by freezing tuples whose xids are old enough — replacing the actual xid with the special "frozen xid" marker that is always visible. Autovacuum is responsible for this freezing. If autovacuum falls badly behind and a table's oldest unfrozen xid approaches the 2-billion-ago wraparound danger zone, Postgres enters "transaction ID wraparound protection mode" — refusing all writes to that table until manual `VACUUM FREEZE` is run.

The practical implication: autovacuum is not optional. Disabling it or letting it fall behind causes table bloat (fixable), degraded planner statistics (fixable), and ultimately transaction ID wraparound (a database emergency that requires downtime and manual intervention).

---

## 11. Cross-Question Chain

**Interviewer:** Your Postgres database is running out of connections at 200. The application reports `FATAL: sorry, too many clients already`. How do you fix this without restarting Postgres?

**Candidate:** Without a restart, I can't change `max_connections` — it requires a restart. My immediate options: first, kill idle connections that are wasting slots — especially `idle in transaction` connections which hold locks and xmin snapshots. `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction' AND now() - xact_start > interval '5 minutes'`. Second, deploy PgBouncer in front of Postgres — it runs as a separate process and doesn't require Postgres to restart. Connections go to PgBouncer (port 6432), which pools them into the existing 200-connection Postgres slots. Third, have the application reduce its connection pool size per server to bring total connections below 200.

**Interviewer:** You deploy PgBouncer in transaction mode. What does `transaction mode` mean and what Postgres features does it break?

**Candidate:** Transaction pooling means PgBouncer assigns a Postgres backend connection to an application connection only for the duration of one transaction. Between transactions, the backend is returned to the pool. This enables 5,000 app connections to multiplex over 25 Postgres backends. The applications can't tell — PgBouncer transparently routes each transaction to any available backend.

The features it breaks are those that depend on connection-level state: `SET` commands (like `SET search_path`) — any `SET` in one transaction may be applied to a different backend on the next transaction; prepared statements created with `PREPARE` — these are session-level and the next transaction may go to a different backend that doesn't have the prepared statement; advisory locks (`pg_advisory_lock`) — session-level locks are lost when the connection is returned to the pool; `LISTEN/NOTIFY` — requires a persistent connection; temporary tables — session-level, lost on pool handoff.

For most OLTP applications, none of these are used and transaction mode works transparently. For applications using prepared statements heavily (e.g., JDBC's `preparedStatementCache`), use statement-level PgBouncer mode or disable prepared statement caching in the application.

**Interviewer:** You set `idle_in_transaction_session_timeout = '5min'`. How does this interact with long-running analytics queries in the same database?

**Candidate:** `idle_in_transaction_session_timeout` terminates connections that are in a transaction but not actively running a query — the state is `idle in transaction`. It does NOT affect connections in `active` state running a query, regardless of duration. A 2-hour analytics query in `active` state is unaffected. The timeout only fires if a transaction is open but the connection is sitting idle (waiting for the application to send the next SQL statement within the transaction).

For long-running analytics queries that run as a single statement (no multi-statement transaction), `idle_in_transaction_session_timeout` has no effect. If the analytics query runs inside a `BEGIN; ... long query ...; COMMIT;` block and there's a pause between the `BEGIN` and the long query (perhaps the app fetches some config first), the timeout could kill it. The right control for analytics is `statement_timeout` — which terminates any query that runs for longer than the specified duration. A separate analytics role with a higher `statement_timeout` (or `statement_timeout = 0` for no limit) can be configured with `ALTER ROLE analytics_user SET statement_timeout = 0`.

**Interviewer:** Explain what the checkpointer does, how it interacts with the bgwriter, and why the two exist separately.

**Candidate:** The checkpointer and bgwriter have distinct goals that justify separate processes.

The checkpointer performs periodic, full flushes: every `checkpoint_timeout` (default 5 minutes), it writes all dirty pages from the buffer pool to the data files on disk, then writes a CHECKPOINT WAL record. This creates a guaranteed recovery point — after a crash, Postgres only needs to replay WAL from the last checkpoint's redo LSN. The checkpointer is inherently bursty: if there are 10,000 dirty pages to flush, they all need to go to disk within the checkpoint window. `checkpoint_completion_target = 0.9` spreads this over 90% of the interval to reduce the I/O burst.

The bgwriter operates continuously between checkpoints. Its goal is not to create a recovery point — it's to keep a supply of clean (evictable) buffer pool pages so that backend processes never have to wait for a dirty page flush during query execution. When a backend needs to evict a page to load a new one, if the victim page is dirty, the backend must write it to disk before evicting — adding disk I/O latency directly to the query. The bgwriter prevents this by proactively cleaning pages in the background.

The two are separate because their I/O patterns differ: the bgwriter makes gentle, continuous progress on whichever dirty pages are least recently used; the checkpointer makes comprehensive sweeps of all dirty pages at fixed intervals. Running them in one process would force a compromise between the two goals — either the checkpoint wouldn't be comprehensive enough, or the bgwriter would be too aggressive.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | Why does Postgres use a process per connection instead of threads? | Historical: threads were unreliable in early 1990s Unix. Effect: each connection = one OS process (~10MB RAM, fork() overhead). Benefits: crash isolation, no GIL. Cost: high connection overhead → requires PgBouncer at scale. |
| 2 | What is the postmaster process? | The parent process of all Postgres processes. Allocates shared memory at startup, listens on the TCP port, forks backend processes for each connection, monitors and restarts crashed background processes. Equivalent to a process supervisor. |
| 3 | What is in the Postgres shared memory segment? | Shared buffer pool (page cache), WAL buffers (pre-fsync WAL records), lock table (all held locks), procarray (all backend PIDs + active xids for MVCC), CLOG (commit/abort status per xid), background worker registry. |
| 4 | What does the WAL writer process do? | Flushes WAL buffers to WAL files on disk every `wal_writer_delay` (200ms default). Enables group commit: multiple concurrent transactions share one fsync. Without WAL writer, each commit would require its own fsync (much more expensive). |
| 5 | What does the checkpointer process do? | Periodically flushes ALL dirty pages from the buffer pool to disk, then writes a CHECKPOINT WAL record. Creates crash recovery points — after a crash, replay only WAL from the last checkpoint forward. Frequency: `checkpoint_timeout` (5min default). |
| 6 | What does the bgwriter process do? | Proactively flushes dirty pages between checkpoints, keeping a supply of clean evictable pages in the buffer pool. Prevents backends from having to flush dirty pages themselves during query execution. `buffers_backend > 0` means bgwriter is falling behind. |
| 7 | What is the autovacuum launcher and why is it critical for correctness? | Monitors tables for dead tuple accumulation; spawns autovacuum workers to reclaim dead tuples and freeze old transaction IDs. Critical for correctness: prevents transaction ID wraparound (XID exhaustion), which causes Postgres to refuse writes to affected tables. |
| 8 | What is `idle in transaction` state and why is it dangerous? | A connection with an open transaction but no active query. Dangerous: (1) holds locks that block other transactions, (2) holds an MVCC snapshot that prevents autovacuum from removing old tuples, (3) keeps xmin from advancing. Fix: `idle_in_transaction_session_timeout`. |
| 9 | What is the procarray and how is it used? | Shared memory structure tracking all active backend PIDs and their current transaction xids. Used for MVCC snapshot acquisition: each new transaction reads the procarray to determine which xids are currently active (and thus whose data should not be visible yet). |
| 10 | What is the CLOG (Commit Log)? | Shared memory (and on-disk) structure mapping each transaction xid to its status: `in_progress`, `committed`, or `aborted`. Used during MVCC visibility checks to determine whether a given tuple version is visible. |
| 11 | Why is `max_connections = 1000` a bad idea without PgBouncer? | At 1,000 connections × 10MB per backend = 10GB RAM consumed by process overhead alone, before any data is cached. Contention on shared memory structures (lock table, procarray) also grows with connection count. 100-200 direct connections is the practical Postgres limit. |
| 12 | What does `checkpoint_completion_target = 0.9` do? | Spreads checkpoint dirty-page I/O over 90% of the checkpoint interval (4.5 minutes for a 5-minute interval). Without it (default was 0.5 in older Postgres), checkpoint I/O is concentrated in 50% of the interval, creating I/O bursts that spike query latency. |
| 13 | What is PgBouncer transaction mode and what does it break? | Transaction mode returns the Postgres backend to the pool after each transaction, allowing thousands of app connections to share tens of backends. Breaks: session-level features — `SET` commands, `PREPARE` statements, advisory locks (`pg_advisory_lock`), `LISTEN/NOTIFY`, temporary tables. |
| 14 | How does the connection lifecycle work in Postgres? | TCP connect → postmaster accepts → forks backend → SSL/auth → ReadyForQuery → client sends SQL → backend parses/plans/executes → sends results → ReadyForQuery → repeat → client Terminates → backend exits → postmaster cleans procarray/locks. |
| 15 | What triggers an emergency checkpoint (`checkpoints_req`)? | WAL size exceeds `max_wal_size` (default 1GB). Postgres must checkpoint to allow old WAL to be recycled. Emergency checkpoints are more I/O-intensive (no gradual spread). Fix: increase `max_wal_size` so scheduled checkpoints run before WAL overflows. |

---

## 13. Further Reading

- **Postgres documentation — "Architecture" and "Server Configuration" chapters:** The official reference for all parameters, process descriptions, and shared memory layout.
- **"The Internals of PostgreSQL" by Hironobu Suzuki (interdb.jp):** The most detailed freely available deep-dive into Postgres internals, covering WAL, MVCC, vacuum, and query planning with diagrams.
- **Postgres source: `src/backend/postmaster/postmaster.c`:** The postmaster main loop and `BackendStartup()` function show exactly what happens on each new connection.
- **"PostgreSQL 14 Internals" by Egor Rogov (postgrespro.com):** Modern book covering all aspects of Postgres internals with clear illustrations.
- **PgBouncer documentation (pgbouncer.org):** Explains pool modes (session, transaction, statement) and their trade-offs.

---

## 14. Lab Exercises

**Exercise 1: Process Observation**
Start a local Postgres instance. Run `ps aux | grep postgres` and identify each process type. Open 5 `psql` connections in separate terminals and run `ps aux | grep postgres` again. Count the new backend processes. Run a query that takes a few seconds (`pg_sleep(10)`) and observe the backend process state in `pg_stat_activity`.

**Exercise 2: Shared Memory Observation**
Run `ipcs -m` to find the Postgres shared memory segment. Note its size in bytes. Compare to your `shared_buffers` setting (`SHOW shared_buffers`). Calculate: how many 8KB pages does the configured `shared_buffers` hold?

**Exercise 3: Simulate BGWriter vs Backend Flush**
In `postgres_architecture.py`, modify `BackendProcess._execute_transaction` to check `shm.dirty_page_count()` before and after each transaction. Run with and without the BGWriter thread. Compare `buffers_backend` (simulated by counting how often backends had to wait for dirty pages to be flushed).

**Exercise 4: MVCC Snapshot Visibility**
In `postgres_architecture.py`, extend the MVCC demo (Phase 4). Start 3 transactions. Take a snapshot. Commit two of them. Verify that: (1) the committed-before-snapshot xid is visible in the snapshot, (2) the committed-after-snapshot xid is NOT visible in the snapshot, (3) the still-in-progress xid is NOT visible.

---

## 15. Key Takeaways

Postgres's architecture is built on two fundamental choices: process-per-connection (isolation and crash safety at the cost of connection overhead) and shared memory for the buffer pool and coordination structures (enabling concurrent access to cached data across all backends). Every production issue with Postgres traces back to one of these two pillars.

The process model makes connection pooling (PgBouncer) non-optional for applications with more than 100 concurrent connections. The shared memory model makes `shared_buffers` the single most important configuration parameter — but also makes buffer hit rate the primary metric for diagnosing slow queries.

The background processes — WAL writer, bgwriter, checkpointer, autovacuum — are the maintenance infrastructure that keeps Postgres healthy under sustained write load. `buffers_backend > 0`, `checkpoints_req > 0`, and `n_dead_tup` accumulation are the early warning signals that one of these processes is falling behind.

The MVCC procarray and CLOG are the foundation of Postgres's concurrency model. `idle in transaction` connections are dangerous because they hold snapshots that freeze the oldest visible xmin, preventing autovacuum from reclaiming dead tuples.

---

## 16. Connections to Other Modules

- **M49 — Write-Ahead Log:** The WAL writer in this module is the process that implements M49's WAL flushing. The checkpointer in this module is the process that implements M49's checkpoint operation. The architecture makes the WAL mechanisms concrete.
- **M50 — Buffer Pool Management:** The bgwriter and checkpointer are the processes that flush dirty pages from the buffer pool described in M50. `buffers_backend` from `pg_stat_bgwriter` is the `dirty_writes` counter from M50's BufferPoolStats.
- **M53 — Query Planning:** Backend processes (this module) are what execute the query plans described in M53. The parse → analyze → plan → execute pipeline runs inside each backend process on every query.
- **M55 — Vacuuming and Bloat:** The autovacuum launcher and workers (this module) are the processes that implement the vacuum operations described in M55. Understanding the autovacuum process architecture explains why autovacuum can fall behind and what to do about it.
- **M56 — Postgres for Data Engineering:** PgBouncer (M56) addresses the connection overhead problem described in this module. Logical replication workers (M56) are a type of background process spawned by the postmaster.

---

## 17. Anti-Patterns

**Anti-pattern: Configuring `max_connections = 500` and assuming applications can open 500 persistent connections.** `max_connections` is the hard upper limit, not a recommended operating point. Sustained operation near `max_connections` creates contention on the procarray lock (which must be scanned on every MVCC snapshot acquisition), the lock table (which scales with connection count), and shared memory. Target: keep active connections below 100. Use PgBouncer for applications that need more.

**Anti-pattern: Increasing `work_mem` globally to speed up sorts.** `work_mem` is allocated per sort node per backend, and a complex query plan can have many sort/hash nodes. With `work_mem = 256MB` and 50 active backends each running a 3-node query: 50 × 3 × 256MB = 38GB of work memory, potentially exhausting RAM and causing OOM kills. Set `work_mem` conservatively globally (4-16MB) and increase it per-session for specific heavy analytical queries: `SET work_mem = '1GB'; SELECT ...`.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pg_stat_activity` | View active connections and states | Filter for `idle in transaction`, lock waits |
| `pg_stat_bgwriter` | Background process statistics | `buffers_backend`, `checkpoints_req` |
| `pg_stat_wal` | WAL write/fsync statistics | `wal_sync_time`, `wal_buffers_full` |
| `ps aux | grep postgres` | OS-level process list | Identify all Postgres processes |
| `ipcs -m` | Shared memory segments | Verify shared memory size |
| `pg_buffercache` (extension) | Buffer pool contents | Diagnose which tables are cached |
| `postgres_architecture.py` | Architecture simulation | Postmaster, backends, WAL writer, checkpointer |
| PgBouncer | Connection pooler | Reduce backend process count |

---

## 19. Glossary

**Autovacuum:** Postgres background process (launcher + workers) that reclaims dead tuples, updates planner statistics, and freezes old transaction IDs to prevent wraparound. Not optional — critical for both performance and correctness.

**Backend process:** A Postgres OS process forked by the postmaster for each client connection. Handles authentication, query parsing, planning, and execution. Has its own stack and local memory; shares the shared memory segment.

**BGWriter (Background Writer):** Postgres background process that proactively writes dirty buffer pool pages to disk between checkpoints. Keeps a supply of clean evictable pages; prevents backends from flushing dirty pages during query execution.

**Checkpointer:** Postgres background process that periodically flushes all dirty buffer pool pages to disk and writes a CHECKPOINT WAL record. Creates crash recovery points.

**CLOG (Commit Log):** Shared memory structure (backed by `pg_xact/` on disk) mapping each transaction xid to its status: in_progress, committed, or aborted. Used in MVCC visibility checks.

**Idle in transaction:** Connection state where a transaction is open but no query is currently executing. Dangerous: holds locks, holds MVCC snapshot (preventing autovacuum), and blocks xmin advancement.

**Postmaster:** The parent Postgres process. Allocates shared memory, listens on the TCP port, forks backend processes per connection, monitors background processes. Equivalent to a process supervisor.

**Procarray:** Shared memory structure tracking all active backend PIDs, their transaction xids, and their current xmin. Used for MVCC snapshot acquisition and visibility checks.

**Shared buffer pool:** The in-memory cache for database pages, stored in the shared memory segment. All backends read/write pages through this cache. Sized by `shared_buffers`.

**WAL writer:** Postgres background process that flushes WAL buffers to WAL files on disk every `wal_writer_delay` (200ms default). Enables group commit.

---

## 20. Self-Assessment

1. Why does Postgres use processes instead of threads for client connections? What are the three main consequences for production operations?
2. Name the five main background processes in a Postgres cluster and state the purpose of each in one sentence.
3. What data structures are in the Postgres shared memory segment? Why must they be in shared memory (not per-process memory)?
4. A monitoring alert fires: `buffers_backend` has been increasing by 5,000/minute for the past hour. What is happening, and what two things can you do to fix it?
5. What is the difference between `idle` and `idle in transaction` connection states? Why is `idle in transaction` more dangerous than `idle`?
6. In `postgres_architecture.py`, what does `get_snapshot()` return and how does `is_visible()` use it? Trace through the MVCC snapshot example (Phase 4).
7. You are asked to increase Postgres `max_connections` from 100 to 500 to handle more app traffic without PgBouncer. What are three specific negative consequences of this change?
8. What does `checkpoint_completion_target = 0.9` do, and what symptom would you see in `pg_stat_bgwriter` if it were set to `0.1`?

---

## 21. Module Summary

Postgres's architecture is a precise response to two constraints: disk is slow (cache pages in a shared buffer pool) and processes must be isolated (fork a process per connection). The postmaster orchestrates the entire cluster — allocating shared memory, forking backends, starting background processes, and cleaning up after crashes. Six background processes keep the cluster healthy: the WAL writer batches and flushes WAL records, the bgwriter proactively cleans dirty pages, the checkpointer creates recovery points, autovacuum reclaims dead tuple space and prevents xid wraparound, and the statistics collector materializes runtime metrics.

The simulation — `Postmaster`, `SharedMemorySegment`, `BackendProcess`, `WALWriter`, `BGWriter`, `Checkpointer` — makes the interaction concrete: multiple backends competing for pages in shared memory, the WAL writer batching their WAL records, the bgwriter pre-cleaning pages so backends never wait for dirty flushes. The MVCC snapshot demo shows how the procarray is the coordination point for transaction visibility.

The production implications are direct: `idle in transaction` connections are silent killers of autovacuum effectiveness and lock availability; `buffers_backend > 0` is the bgwriter alarm; `checkpoints_req > 0` means WAL is growing faster than scheduled checkpoints can absorb it; and `max_connections` is a resource limit, not a capacity target — PgBouncer is the capacity mechanism.

The next module — M53: Query Planning — takes a single SQL statement and traces the complete path from parse tree to physical execution plan, explaining how the Postgres planner chooses between index scans, sequential scans, hash joins, and sort-merge joins.
