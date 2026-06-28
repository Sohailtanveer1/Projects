# M49: Write-Ahead Log

**Course:** DBE-INT-101 — Storage Engine Internals  
**Module:** 03 of 05  
**Global Module ID:** M49  
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

Every database system faces the same fundamental problem: the CPU and RAM operate at nanosecond speed; durability requires writing to disk, which operates at microsecond-to-millisecond speed. If you modify a B-tree page in memory (fast) and then write it to disk (slow), there is a window between the modification and the write where a power failure or crash loses the data permanently. The in-memory modification is gone (RAM is volatile), and the on-disk page still has the old data.

The naive fix — write to disk before returning to the client — is too slow. Writing one B-tree leaf page (8KB) to disk at a random location takes ~1ms on an SSD. At 10,000 writes/second, that requires 10,000 random disk writes per second — far beyond what a single SSD supports.

The Write-Ahead Log (WAL) solves this with a simple but profound insight: sequential writes are orders of magnitude faster than random writes. Instead of writing the modified B-tree page to its random position on disk, write the change to the end of a sequential log first. The log write is sequential, fast (hundreds of thousands per second), and small (one log record per change). The B-tree page is written to its random position lazily, in the background, in a batched way. If the system crashes before the page is written, the WAL is replayed on recovery to redo the change.

The result: writes are acknowledged after the sequential WAL write (fast), not after the random page write (slow). Durability is guaranteed by the WAL. Random I/O is deferred and batched. This is why Postgres, MySQL InnoDB, SQLite, and every major database use a WAL. It is the mechanism that reconciles fast writes with durable storage.

---

## 2. Mental Model

### Two Invariants

The WAL is built on two invariants, enforced strictly:

**Invariant 1 (Write-Ahead):** A WAL record describing a change must be written and fsynced to disk BEFORE the page containing the change is written to disk.

**Invariant 2 (Commit):** A transaction is durable (survived any subsequent crash) if and only if its commit WAL record has been fsynced to disk.

These two invariants together mean: at any point in time, the WAL contains a complete history of all committed changes — even changes whose corresponding pages haven't been flushed to disk yet. On recovery, replay the WAL from the last checkpoint, and all committed transactions are restored.

### Sequential vs Random I/O

```
Without WAL (direct page write):
  INSERT row → find leaf page → modify page in memory → write 8KB page to disk
  Cost: 1 random write per operation (8KB, random position)
  Throughput: ~10,000 random writes/sec on SSD × 8KB = ~80MB/s

With WAL:
  INSERT row → find leaf page → modify page in memory
             → write WAL record to log end → fsync WAL
             (page written to disk lazily, later, in batch)
  Cost: 1 sequential WAL write per operation (~100-500 bytes, sequential)
  Throughput: ~100,000 sequential writes/sec × 300 bytes ≈ 30MB/s app data
  (Random page writes happen but are batched and amortized — not per-operation)
```

The WAL doesn't eliminate disk I/O — it converts random I/O to sequential I/O and batches the random I/O. The net result: much higher write throughput.

---

## 3. Core Concepts

### 3.1 WAL Record Structure

A WAL record (also called a "log record") describes a single atomic change to the database. Every WAL record contains:

```
WAL Record Layout (Postgres-style):

  XLogRecord Header (24 bytes):
    xl_tot_len (4): Total length of this record including header
    xl_xid    (4): Transaction ID (0 for non-transactional)
    xl_prev   (8): LSN of previous WAL record (for backward scan)
    xl_info   (1): Record type flags
    xl_rmid   (1): Resource manager ID (who generated this — heap, btree, etc.)
    xl_crc    (4): CRC32 of entire record

  Record Body (variable):
    Block reference (which page was modified)
    Full page image (optional — on first write after checkpoint)
    Redo data (deltas: what changed)
```

The **LSN (Log Sequence Number)** is a monotonically increasing byte offset in the WAL file that uniquely identifies the position of each record. LSN is used to:
- Determine if a WAL record has already been applied (if page LSN ≥ record LSN, skip)
- Track recovery progress (replay from checkpoint LSN)
- Coordinate replication (follower's last flush_lsn, as covered in M42)

### 3.2 The WAL Write Protocol

A complete write operation with WAL:

```
BEGIN TRANSACTION:
  1. Client sends INSERT/UPDATE/DELETE

PREPARE (WAL logging):
  2. Acquire buffer pool lock on target page
  3. Modify the page in the buffer pool (in memory only)
  4. Write WAL record to WAL buffer (in memory)
  5. Assign LSN to this WAL record
  6. Set page.lsn = this LSN (marks that page as "dirty, pending WAL flush at this LSN")

COMMIT:
  7. Write commit record to WAL buffer
  8. fsync WAL buffer to disk (this makes the transaction durable)
  9. Return success to client

BACKGROUND (checkpoint/bgwriter):
  10. Eventually, flush dirty pages from buffer pool to disk
      (Can only flush a page after its LSN's WAL record is flushed — Invariant 1)
```

Steps 2-8 happen in the client's transaction. Step 10 happens asynchronously. The client's latency is dominated by step 8 (WAL fsync), not step 10 (page flush).

### 3.3 Crash Recovery with the WAL (ARIES)

The standard WAL crash recovery algorithm is ARIES (Algorithms for Recovery and Isolation Exploiting Semantics), used by Postgres, MySQL, and most major databases. Recovery has three phases:

**Phase 1: Analysis**
Read the WAL from the last checkpoint forward. Build a table of active transactions at the time of crash (winners = committed, losers = not committed). Build a dirty page table (which pages were modified since the last checkpoint and may not have been flushed to disk).

**Phase 2: Redo ("Repeat History")**
Starting from the checkpoint LSN, replay ALL WAL records — even for transactions that eventually aborted. This restores the database to exactly the state it was in at the moment of crash (including uncommitted changes). ARIES's key insight: "repeat history" — don't skip records for aborted transactions, because we need to get the page state exactly right before applying undo.

**Phase 3: Undo**
For every transaction that was active (not committed) at crash time, undo its changes in reverse order. WAL records for undo are themselves logged (compensation log records, CLRs) so that if the recovery crashes mid-undo, the undo can be resumed without re-doing already-undone work.

**Result:** After recovery, the database contains exactly the committed state as of the last successful commit before the crash. All uncommitted transactions are rolled back.

### 3.4 Checkpoints

Without checkpoints, recovery would always replay the entire WAL from the beginning — which could be terabytes for a busy database.

A **checkpoint** is a synchronization point where:
1. All dirty pages in the buffer pool are flushed to disk
2. A checkpoint record is written to the WAL recording: which WAL LSN is the "redo point" (earliest WAL position needed for recovery)

After a checkpoint:
- WAL records before the checkpoint's redo LSN are no longer needed for recovery
- Those WAL segments can be archived and eventually deleted
- Recovery after a crash only needs to replay from the checkpoint's redo LSN forward

**Checkpoint cost:** Flushing all dirty pages is expensive I/O (many random writes). Frequent checkpoints = expensive but fast recovery. Infrequent checkpoints = cheap but slow recovery (must replay more WAL). Postgres default: `checkpoint_completion_target = 0.9`, spread checkpoint I/O over 90% of `checkpoint_timeout = 5min`.

### 3.5 Full-Page Writes (FPW)

A subtle crash scenario: a database writes an 8KB page to disk. The OS issues a write to the physical disk. The disk writes the first 4KB (matching the physical sector size), then the power fails. The page on disk is now half-old / half-new — a "torn page." The CRC check on the page will fail.

The WAL record describes deltas: "change byte offset 1234 from 'X' to 'Y'." If the page on disk is torn, applying the delta to the torn page produces a corrupt result.

**Solution: Full-Page Writes (FPW):** The first time a page is modified after a checkpoint, the entire page image is included in the WAL record (not just the delta). On recovery, the full-page image is written before applying subsequent deltas. This guarantees that even a torn page is safely recovered.

FPW doubles WAL volume for the first write to each page after a checkpoint. Postgres enables FPW by default (`full_page_writes = on`). Disabling FPW is dangerous — acceptable only on file systems with atomic 8KB writes (some ZFS/ext4 configurations).

### 3.6 WAL Segmentation and Archiving

The WAL is not a single infinite file. It is divided into fixed-size segments (typically 16MB in Postgres). Older segments are retained until:
- Recovery no longer needs them (all dirty pages from before their creation have been checkpointed)
- Replication slot followers have consumed them (M42)
- Continuous archiving has copied them to object storage

**WAL archiving:** In Postgres, `archive_mode = on` + `archive_command` causes each completed WAL segment to be copied to a configurable destination (S3, NFS, etc.) before the database deletes the local copy. This is the foundation of Point-in-Time Recovery (PITR): restore a base backup, then replay archived WAL to any desired point in time.

---

## 4. Hands-On Walkthrough

### 4.1 Observing the Postgres WAL

```sql
-- ── WAL Position and LSN ─────────────────────────────────────────────────────
-- Current WAL insert position (where new WAL records will be written)
SELECT pg_current_wal_insert_lsn();
-- 0/3A7F028

-- Current WAL write position (last position written to OS buffer)
SELECT pg_current_wal_lsn();
-- 0/3A7F028

-- Current WAL flush position (last position fsynced to disk)
SELECT pg_current_wal_flush_lsn();
-- 0/3A7F028  (same as write/insert if no async writes pending)

-- What WAL file contains this LSN?
SELECT pg_walfile_name(pg_current_wal_lsn());
-- 000000010000000000000003  (segment file name)

-- How much WAL has been generated since server start?
SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0'));
-- 58 MB

-- ── WAL Statistics ───────────────────────────────────────────────────────────
SELECT
    *
FROM pg_stat_wal;
-- wal_records      | 1248392   (total WAL records written)
-- wal_fpi          | 12483     (full-page images written)
-- wal_bytes        | 61943820  (total WAL bytes written)
-- wal_buffers_full | 0         (times WAL buffers filled before commit — bad if > 0)
-- wal_write        | 89234     (WAL writes to OS)
-- wal_sync         | 89234     (WAL fsyncs to disk)
-- wal_write_time   | 1234.5    (ms spent in WAL write)
-- wal_sync_time    | 5678.9    (ms spent in WAL fsync — key latency driver)

-- ── Checkpoint Statistics ─────────────────────────────────────────────────────
SELECT
    checkpoints_timed,   -- Checkpoints triggered by checkpoint_timeout
    checkpoints_req,     -- Checkpoints triggered by max_wal_size (emergency)
    checkpoint_write_time,
    checkpoint_sync_time,
    buffers_checkpoint,  -- Dirty pages flushed during checkpoint
    buffers_clean,       -- Dirty pages flushed by bgwriter
    buffers_backend      -- Dirty pages flushed by backend (bad if high)
FROM pg_stat_bgwriter;
-- buffers_backend high → backends are writing dirty pages directly
-- → increase shared_buffers or bgwriter_lru_maxpages

-- ── WAL Configuration ─────────────────────────────────────────────────────────
SELECT name, setting, unit
FROM pg_settings
WHERE name IN (
    'wal_level',                   -- minimal / replica / logical
    'synchronous_commit',          -- on / off / local / remote_apply
    'wal_sync_method',             -- fsync / fdatasync / open_sync / etc.
    'wal_buffers',                 -- WAL buffer size (default = 1/32 of shared_buffers)
    'checkpoint_timeout',          -- How often to checkpoint (default 5min)
    'max_wal_size',                -- Max WAL size before forced checkpoint (default 1GB)
    'checkpoint_completion_target',-- Spread checkpoint I/O over this fraction of interval
    'full_page_writes'             -- Write full page image on first post-checkpoint modify
);
```

### 4.2 WAL Level Impact on WAL Volume

```sql
-- wal_level controls what information is recorded in WAL
-- minimal: Only what's needed for crash recovery (cannot use for replication)
-- replica: Everything needed for streaming replication (default)
-- logical: Additional information for logical decoding (replication slots, CDC)

-- Measure WAL volume for a bulk INSERT at different wal_levels:
-- (Run in a test environment — do not change wal_level in production without restart)

-- At wal_level = replica (default):
SELECT pg_current_wal_lsn() AS before_insert;
INSERT INTO test_table SELECT i, md5(i::text) FROM generate_series(1, 100000) i;
SELECT pg_size_pretty(
    pg_wal_lsn_diff(pg_current_wal_lsn(), 'X/Y')  -- replace X/Y with before_insert
) AS wal_generated;
-- Typical: ~30-50MB for 100K rows at wal_level=replica

-- For bulk loads, use COPY with wal_level=minimal when possible:
-- SET session_replication_role = replica; (disables triggers/constraints)
-- COPY test_table FROM '/tmp/data.csv' CSV;
-- Much lower WAL volume for bulk unlogged operations
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
wal_engine.py

A complete Write-Ahead Log implementation in Python.

Components:
  - WALRecord: Individual log record with header and payload
  - WALWriter: Writes records sequentially, manages segments, group commit
  - WALReader: Reads records forward or backward from a given LSN
  - CheckpointManager: Tracks dirty pages, checkpoint LSN, recovery point
  - RecoveryEngine: ARIES-style recovery (analysis, redo, undo)
  - StorageEngine: A simple page-based storage engine backed by WAL

Design mirrors Postgres WAL:
  - LSN = byte offset in WAL file (monotonically increasing)
  - Each record has prev_lsn for backward chaining
  - Checkpoint records record the redo LSN
  - Full-page writes on first post-checkpoint modification
  - Group commit: batch multiple WAL records into one fsync

No external dependencies.
"""

from __future__ import annotations
import hashlib
import io
import struct
import time
from dataclasses import dataclass, field
from enum import IntEnum, auto
from typing import Any, Iterator, Optional


# ─── Record Types ─────────────────────────────────────────────────────────────

class RecordType(IntEnum):
    """Types of WAL records."""
    WRITE       = 1   # Page write: (page_id, offset, old_value, new_value)
    COMMIT      = 2   # Transaction committed
    ABORT       = 3   # Transaction aborted
    CHECKPOINT  = 4   # Checkpoint: (redo_lsn, dirty_pages)
    BEGIN       = 5   # Transaction began
    FULL_PAGE   = 6   # Full page image (after checkpoint)
    CLR         = 7   # Compensation Log Record (for undo)


# ─── WAL Record ──────────────────────────────────────────────────────────────

HEADER_FMT = ">IIQHBB"   # big-endian: length(4), xid(4), prev_lsn(8), type(2), rmid(1), flags(1)
HEADER_SIZE = struct.calcsize(HEADER_FMT)

@dataclass
class WALRecord:
    """A single WAL record."""
    lsn:      int           # Byte offset of this record in the WAL
    xid:      int           # Transaction ID (0 = non-transactional)
    prev_lsn: int           # LSN of previous record in this transaction
    rtype:    RecordType
    rmid:     int           # Resource manager (0=heap, 1=btree, etc.)
    payload:  bytes         # Record-specific data

    @property
    def total_length(self) -> int:
        return HEADER_SIZE + len(self.payload) + 4   # +4 for trailing CRC

    def serialize(self) -> bytes:
        """Serialize to bytes for WAL file."""
        header = struct.pack(HEADER_FMT,
            self.total_length,
            self.xid,
            self.prev_lsn,
            self.rtype,
            self.rmid,
            0   # flags
        )
        crc = _crc32(header + self.payload)
        return header + self.payload + struct.pack(">I", crc)

    @staticmethod
    def deserialize(data: bytes, lsn: int) -> 'WALRecord':
        """Parse from bytes at the given LSN."""
        if len(data) < HEADER_SIZE + 4:
            raise ValueError(f"Record too short: {len(data)} bytes")
        length, xid, prev_lsn, rtype, rmid, flags = struct.unpack_from(HEADER_FMT, data, 0)
        payload = data[HEADER_SIZE : length - 4]
        stored_crc = struct.unpack_from(">I", data, length - 4)[0]
        computed_crc = _crc32(data[:HEADER_SIZE] + payload)
        if stored_crc != computed_crc:
            raise ValueError(f"WAL record CRC mismatch at LSN {lsn}: "
                           f"stored={stored_crc:#010x} computed={computed_crc:#010x}")
        return WALRecord(lsn=lsn, xid=xid, prev_lsn=prev_lsn,
                        rtype=RecordType(rtype), rmid=rmid, payload=payload)


def _crc32(data: bytes) -> int:
    """CRC32 checksum."""
    import binascii
    return binascii.crc32(data) & 0xFFFFFFFF


# ─── Payload Builders/Parsers ─────────────────────────────────────────────────

def encode_write(page_id: int, key: str, old_val: Optional[str], new_val: str) -> bytes:
    """Encode a WRITE record payload."""
    old_b = (old_val or "").encode()
    new_b = new_val.encode()
    key_b = key.encode()
    return struct.pack(">IHH", page_id, len(old_b), len(new_b)) + key_b + b"\x00" + old_b + new_b

def decode_write(payload: bytes) -> tuple[int, str, Optional[str], str]:
    """Decode a WRITE record payload → (page_id, key, old_val, new_val)."""
    page_id, old_len, new_len = struct.unpack_from(">IHH", payload, 0)
    rest = payload[8:]
    null_idx = rest.index(b"\x00")
    key = rest[:null_idx].decode()
    rest = rest[null_idx + 1:]
    old_val = rest[:old_len].decode() if old_len else None
    new_val = rest[old_len:old_len + new_len].decode()
    return page_id, key, old_val, new_val

def encode_checkpoint(redo_lsn: int, dirty_page_ids: list[int]) -> bytes:
    """Encode a CHECKPOINT record payload."""
    n = len(dirty_page_ids)
    fmt = f">QI{n}I"
    return struct.pack(fmt, redo_lsn, n, *dirty_page_ids)

def decode_checkpoint(payload: bytes) -> tuple[int, list[int]]:
    """Decode a CHECKPOINT record → (redo_lsn, dirty_page_ids)."""
    redo_lsn, n = struct.unpack_from(">QI", payload, 0)
    if n > 0:
        ids = struct.unpack_from(f">{n}I", payload, 12)
    else:
        ids = ()
    return redo_lsn, list(ids)

def encode_full_page(page_id: int, page_data: bytes) -> bytes:
    """Encode a FULL_PAGE record payload."""
    return struct.pack(">I", page_id) + page_data

def decode_full_page(payload: bytes) -> tuple[int, bytes]:
    page_id = struct.unpack_from(">I", payload, 0)[0]
    return page_id, payload[4:]


# ─── WAL Writer ──────────────────────────────────────────────────────────────

class WALWriter:
    """
    Writes WAL records sequentially.
    Simulates file I/O with an in-memory BytesIO buffer.
    Tracks LSN as a byte offset.
    """

    SEGMENT_SIZE = 16 * 1024 * 1024   # 16MB segments (matches Postgres default)

    def __init__(self):
        self._buf       = io.BytesIO()  # Simulated WAL file
        self._lsn       = 0            # Current LSN (write position)
        self._flush_lsn = 0            # Last fsynced LSN
        self._prev_lsn_by_xid: dict[int, int] = {}   # xid → last LSN
        self._write_count = 0
        self._fsync_count = 0

    @property
    def current_lsn(self) -> int:
        return self._lsn

    @property
    def flush_lsn(self) -> int:
        return self._flush_lsn

    def write(self, xid: int, rtype: RecordType, rmid: int, payload: bytes) -> int:
        """
        Write a WAL record. Returns the LSN of the written record.
        Does NOT fsync — call flush() to make durable.
        """
        prev_lsn = self._prev_lsn_by_xid.get(xid, 0)
        rec = WALRecord(lsn=self._lsn, xid=xid, prev_lsn=prev_lsn,
                       rtype=rtype, rmid=rmid, payload=payload)
        data = rec.serialize()
        self._buf.seek(self._lsn)
        self._buf.write(data)
        lsn = self._lsn
        self._lsn += len(data)
        self._write_count += 1
        if xid != 0:
            self._prev_lsn_by_xid[xid] = lsn
        return lsn

    def flush(self) -> int:
        """
        fsync the WAL buffer to disk (simulated).
        All records written before this call are now durable.
        Returns the new flush_lsn.
        """
        self._flush_lsn = self._lsn
        self._fsync_count += 1
        # (In production: os.fsync(fd) or fdatasync(fd))
        return self._flush_lsn

    def write_and_flush(self, xid: int, rtype: RecordType, rmid: int, payload: bytes) -> int:
        """Write + immediate fsync (synchronous_commit = on mode)."""
        lsn = self.write(xid, rtype, rmid, payload)
        self.flush()
        return lsn

    def print_stats(self):
        print(f"  WAL Stats: writes={self._write_count}, fsyncs={self._fsync_count}, "
              f"lsn={self._lsn}, flush_lsn={self._flush_lsn}, "
              f"size={self._lsn/1024:.1f}KB")


# ─── WAL Reader ──────────────────────────────────────────────────────────────

class WALReader:
    """Reads WAL records sequentially from a WALWriter's buffer."""

    def __init__(self, writer: WALWriter):
        self._writer = writer

    def scan_from(self, start_lsn: int = 0) -> Iterator[WALRecord]:
        """Yield all WAL records from start_lsn forward."""
        buf   = self._writer._buf
        pos   = start_lsn
        end   = self._writer._lsn

        while pos < end:
            buf.seek(pos)
            # Read length prefix first
            length_data = buf.read(4)
            if len(length_data) < 4:
                break
            length = struct.unpack(">I", length_data)[0]
            if length == 0 or pos + length > end:
                break
            buf.seek(pos)
            record_data = buf.read(length)
            if len(record_data) < length:
                break
            try:
                rec = WALRecord.deserialize(record_data, pos)
                yield rec
                pos += length
            except ValueError as e:
                print(f"  WAL corruption detected at LSN {pos}: {e}")
                break

    def scan_transaction(self, xid: int, last_lsn: int) -> list[WALRecord]:
        """
        Walk backward through a transaction's records using prev_lsn chain.
        Used during UNDO phase of recovery.
        """
        records = []
        lsn = last_lsn
        while lsn > 0:
            buf = self._writer._buf
            buf.seek(lsn)
            length_data = buf.read(4)
            if len(length_data) < 4:
                break
            length = struct.unpack(">I", length_data)[0]
            buf.seek(lsn)
            record_data = buf.read(length)
            rec = WALRecord.deserialize(record_data, lsn)
            if rec.xid != xid:
                break
            records.append(rec)
            lsn = rec.prev_lsn
        return records


# ─── Storage Engine (Simple Page Store) ─────────────────────────────────────

@dataclass
class Page:
    """A simple key-value store page."""
    page_id:    int
    data:       dict[str, str] = field(default_factory=dict)
    lsn:        int = 0           # LSN of last WAL record that modified this page
    dirty:      bool = False
    full_page_written: bool = False  # True if FPW has been written since last checkpoint

    def get(self, key: str) -> Optional[str]:
        return self.data.get(key)

    def put(self, key: str, value: str):
        self.data[key] = value
        self.dirty = True

    def delete(self, key: str):
        self.data.pop(key, None)
        self.dirty = True

    def apply_write(self, key: str, new_val: str, lsn: int):
        """Apply a WRITE record during recovery."""
        self.data[key] = new_val
        self.dirty = True
        self.lsn = lsn


class PageStore:
    """
    Simple in-memory page store (simulates the buffer pool + disk).
    Tracks which pages are dirty (need to be flushed to disk).
    """

    def __init__(self):
        self._pages: dict[int, Page] = {}
        self._pages_since_checkpoint: set[int] = set()   # Need full-page write

    def get_page(self, page_id: int) -> Page:
        if page_id not in self._pages:
            self._pages[page_id] = Page(page_id)
        return self._pages[page_id]

    def needs_full_page_write(self, page_id: int) -> bool:
        return page_id in self._pages_since_checkpoint

    def mark_full_page_written(self, page_id: int):
        self._pages_since_checkpoint.discard(page_id)

    def dirty_page_ids(self) -> list[int]:
        return [p.page_id for p in self._pages.values() if p.dirty]

    def flush_page(self, page_id: int):
        """Simulate flushing a page to disk."""
        if page_id in self._pages:
            self._pages[page_id].dirty = False

    def checkpoint_reset(self):
        """After checkpoint: all current pages need FPW if modified again."""
        self._pages_since_checkpoint = set(self._pages.keys())


# ─── Transaction Manager + Storage Engine ────────────────────────────────────

class StorageEngine:
    """
    A storage engine combining:
    - PageStore (buffer pool simulation)
    - WALWriter (write-ahead log)
    - CheckpointManager (checkpoint tracking)
    - Transaction support (BEGIN, COMMIT, ABORT)
    """

    def __init__(self):
        self._wal    = WALWriter()
        self._reader = WALReader(self._wal)
        self._pages  = PageStore()
        self._next_xid = 1
        self._active_txns: dict[int, list[tuple]] = {}   # xid → list of (page_id, key, old_val)
        self._last_checkpoint_lsn = 0

    def begin(self) -> int:
        """Begin a new transaction. Returns xid."""
        xid = self._next_xid
        self._next_xid += 1
        self._active_txns[xid] = []
        self._wal.write(xid, RecordType.BEGIN, 0, b"")
        return xid

    def write(self, xid: int, page_id: int, key: str, value: str):
        """Write a key-value pair as part of transaction xid."""
        page = self._pages.get_page(page_id)
        old_val = page.get(key)

        # Full-page write if this is the first modification since last checkpoint
        if self._pages.needs_full_page_write(page_id) and not page.full_page_written:
            page_bytes = _encode_page(page)
            fpw_lsn = self._wal.write(xid, RecordType.FULL_PAGE, 0,
                                      encode_full_page(page_id, page_bytes))
            page.full_page_written = True
            self._pages.mark_full_page_written(page_id)

        # Write the WAL record for this change
        payload = encode_write(page_id, key, old_val, value)
        lsn = self._wal.write(xid, RecordType.WRITE, 0, payload)

        # Apply to page in memory
        page.apply_write(key, value, lsn)

        # Record for potential undo
        self._active_txns[xid].append((page_id, key, old_val))

    def read(self, page_id: int, key: str) -> Optional[str]:
        """Read a key from a page (no WAL required for reads)."""
        page = self._pages.get_page(page_id)
        return page.get(key)

    def commit(self, xid: int):
        """Commit transaction. WAL commit record is fsynced — now durable."""
        self._wal.write_and_flush(xid, RecordType.COMMIT, 0, b"")
        self._active_txns.pop(xid, None)

    def abort(self, xid: int):
        """Abort transaction — undo all its changes."""
        changes = self._active_txns.pop(xid, [])
        # Undo in reverse order
        for page_id, key, old_val in reversed(changes):
            page = self._pages.get_page(page_id)
            if old_val is None:
                page.delete(key)
            else:
                page.put(key, old_val)
            # Write CLR (Compensation Log Record) for the undo
            undo_val = old_val or ""
            payload = encode_write(page_id, key, None, undo_val)
            self._wal.write(xid, RecordType.CLR, 0, payload)
        # Write abort record and flush
        self._wal.write_and_flush(xid, RecordType.ABORT, 0, b"")

    def checkpoint(self):
        """
        Perform a checkpoint:
        1. Flush all dirty pages to disk
        2. Write checkpoint WAL record with redo_lsn
        3. Reset full-page-write tracking
        """
        dirty_ids = self._pages.dirty_page_ids()
        for pid in dirty_ids:
            self._pages.flush_page(pid)

        # Redo LSN = earliest LSN that could need replay
        # (For simplicity: current LSN = no replay needed for already-flushed pages)
        redo_lsn = self._wal.current_lsn
        payload = encode_checkpoint(redo_lsn, dirty_ids)
        chk_lsn = self._wal.write_and_flush(0, RecordType.CHECKPOINT, 0, payload)
        self._last_checkpoint_lsn = chk_lsn

        # All pages now need FPW on next modification
        self._pages.checkpoint_reset()
        for page in self._pages._pages.values():
            page.full_page_written = False

        print(f"  Checkpoint at LSN {chk_lsn}: flushed {len(dirty_ids)} dirty pages, "
              f"redo_lsn={redo_lsn}")

    def print_wal(self):
        """Print a human-readable WAL trace."""
        print("\n  WAL Trace:")
        for rec in self._reader.scan_from(0):
            if rec.rtype == RecordType.WRITE:
                pid, key, old_val, new_val = decode_write(rec.payload)
                print(f"    LSN={rec.lsn:6d} XID={rec.xid:3d} WRITE  "
                      f"page={pid} key={key} old={old_val!r} new={new_val!r}")
            elif rec.rtype == RecordType.CHECKPOINT:
                redo_lsn, dirty_ids = decode_checkpoint(rec.payload)
                print(f"    LSN={rec.lsn:6d} XID={rec.xid:3d} CHECKPOINT "
                      f"redo_lsn={redo_lsn} dirty_pages={dirty_ids}")
            elif rec.rtype == RecordType.FULL_PAGE:
                pid, _ = decode_full_page(rec.payload)
                print(f"    LSN={rec.lsn:6d} XID={rec.xid:3d} FULL_PAGE page={pid}")
            else:
                print(f"    LSN={rec.lsn:6d} XID={rec.xid:3d} {rec.rtype.name}")


def _encode_page(page: Page) -> bytes:
    """Serialize a page's data dict to bytes (for FPW)."""
    import json
    return json.dumps(page.data).encode()


# ─── Recovery Engine (ARIES-style) ───────────────────────────────────────────

class RecoveryEngine:
    """
    ARIES-style recovery: Analysis → Redo → Undo
    Simulates recovery from a crash by replaying the WAL.
    """

    def __init__(self, wal_writer: WALWriter):
        self._reader = WALReader(wal_writer)

    def recover(self, crash_lsn: Optional[int] = None) -> PageStore:
        """
        Recover the database state by replaying WAL.
        If crash_lsn is provided, only consider records up to that LSN
        (simulates a crash mid-WAL).
        Returns a fresh PageStore with the recovered state.
        """
        print("\n  === ARIES Recovery ===")

        # Find checkpoint
        checkpoint_lsn, redo_lsn = self._find_latest_checkpoint(crash_lsn)
        print(f"  Latest checkpoint: LSN={checkpoint_lsn}, redo_lsn={redo_lsn}")

        # Phase 1: Analysis
        committed_xids, active_xids, last_lsn_per_xid = self._analysis(redo_lsn, crash_lsn)
        print(f"  Analysis: committed={committed_xids}, active(loser)={active_xids}")

        # Phase 2: Redo
        pages = self._redo(redo_lsn, crash_lsn)
        print(f"  Redo: replayed WAL from LSN {redo_lsn} to {crash_lsn or 'end'}")

        # Phase 3: Undo
        self._undo(pages, active_xids, last_lsn_per_xid)
        print(f"  Undo: rolled back {len(active_xids)} uncommitted transactions")
        print(f"  Recovery complete.")
        return pages

    def _find_latest_checkpoint(self, up_to_lsn: Optional[int]) -> tuple[int, int]:
        """Find the most recent CHECKPOINT record."""
        last_chk_lsn = 0
        last_redo_lsn = 0
        for rec in self._reader.scan_from(0):
            if up_to_lsn is not None and rec.lsn >= up_to_lsn:
                break
            if rec.rtype == RecordType.CHECKPOINT:
                redo_lsn, _ = decode_checkpoint(rec.payload)
                last_chk_lsn = rec.lsn
                last_redo_lsn = redo_lsn
        return last_chk_lsn, last_redo_lsn

    def _analysis(self, from_lsn: int, up_to_lsn: Optional[int]
                 ) -> tuple[set[int], set[int], dict[int, int]]:
        """
        Analysis phase: determine which transactions committed,
        which were still active (losers).
        """
        committed = set()
        active    = set()
        last_lsn: dict[int, int] = {}

        for rec in self._reader.scan_from(from_lsn):
            if up_to_lsn is not None and rec.lsn >= up_to_lsn:
                break
            if rec.xid == 0:
                continue
            last_lsn[rec.xid] = rec.lsn
            if rec.rtype == RecordType.BEGIN:
                active.add(rec.xid)
            elif rec.rtype == RecordType.COMMIT:
                active.discard(rec.xid)
                committed.add(rec.xid)
            elif rec.rtype == RecordType.ABORT:
                active.discard(rec.xid)

        return committed, active, last_lsn

    def _redo(self, from_lsn: int, up_to_lsn: Optional[int]) -> PageStore:
        """
        Redo phase: replay ALL WAL records (including losers) to restore
        exact crash-time state.
        """
        pages = PageStore()
        for rec in self._reader.scan_from(from_lsn):
            if up_to_lsn is not None and rec.lsn >= up_to_lsn:
                break
            if rec.rtype == RecordType.WRITE:
                pid, key, _, new_val = decode_write(rec.payload)
                page = pages.get_page(pid)
                # Only apply if page LSN < record LSN (page may already be up to date)
                if page.lsn < rec.lsn:
                    page.apply_write(key, new_val, rec.lsn)
            elif rec.rtype == RecordType.FULL_PAGE:
                pid, page_bytes = decode_full_page(rec.payload)
                import json
                page = pages.get_page(pid)
                page.data = json.loads(page_bytes.decode())
                page.lsn = rec.lsn
        return pages

    def _undo(self, pages: PageStore, active_xids: set[int],
              last_lsn_per_xid: dict[int, int]):
        """
        Undo phase: roll back all active (uncommitted) transactions in reverse order.
        """
        # Collect all write records for active xids, in reverse LSN order
        writes_to_undo: list[tuple[int, int, str, Optional[str]]] = []  # (lsn, pid, key, old_val)
        for rec in self._reader.scan_from(0):
            if rec.xid in active_xids and rec.rtype == RecordType.WRITE:
                pid, key, old_val, _ = decode_write(rec.payload)
                writes_to_undo.append((rec.lsn, pid, key, old_val))

        # Apply undos in reverse order (largest LSN first)
        for lsn, pid, key, old_val in sorted(writes_to_undo, reverse=True):
            page = pages.get_page(pid)
            if old_val is None:
                page.data.pop(key, None)
            else:
                page.data[key] = old_val


# ─── Group Commit Demo ─────────────────────────────────────────────────────────

def demo_group_commit():
    """
    Demonstrate group commit: multiple transactions share a single fsync.
    """
    print("\n─── Group Commit Demo ───")
    wal = WALWriter()
    N = 10

    # Without group commit: each transaction fsyncs immediately
    start = time.time()
    for i in range(N):
        xid = i + 1
        wal.write(xid, RecordType.BEGIN, 0, b"")
        wal.write(xid, RecordType.WRITE, 0,
                  encode_write(0, f"key{i}", None, f"val{i}"))
        wal.write_and_flush(xid, RecordType.COMMIT, 0, b"")   # fsync per commit
    solo_fsyncs = wal._fsync_count

    # With group commit: batch N transactions into one fsync
    wal2 = WALWriter()
    for i in range(N):
        xid = i + 1
        wal2.write(xid, RecordType.BEGIN, 0, b"")
        wal2.write(xid, RecordType.WRITE, 0,
                   encode_write(0, f"key{i}", None, f"val{i}"))
        wal2.write(xid, RecordType.COMMIT, 0, b"")   # Write but don't fsync yet
    wal2.flush()   # ONE fsync for all N commits
    group_fsyncs = wal2._fsync_count

    print(f"  {N} transactions, individual fsync: {solo_fsyncs} fsyncs")
    print(f"  {N} transactions, group commit:     {group_fsyncs} fsync (1 fsync!)")
    print(f"  Group commit reduces fsync count by {N}×")
    print(f"  This is why synchronous_commit=on with high concurrency is fast:")
    print(f"  many concurrent transactions share each fsync.")


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Write-Ahead Log Demo ===\n")

    # ── Phase 1: Basic Transactions ───────────────────────────────────────────
    print("─── Phase 1: Basic Transactions ───")
    engine = StorageEngine()

    # Transaction 1: committed
    xid1 = engine.begin()
    engine.write(xid1, page_id=0, key="user:1", value="Alice")
    engine.write(xid1, page_id=0, key="user:2", value="Bob")
    engine.commit(xid1)
    print(f"  T1 committed: user:1={engine.read(0, 'user:1')}, "
          f"user:2={engine.read(0, 'user:2')}")

    # Transaction 2: aborted (rollback)
    xid2 = engine.begin()
    engine.write(xid2, page_id=0, key="user:1", value="OVERWRITTEN")
    print(f"  T2 before abort: user:1={engine.read(0, 'user:1')}")
    engine.abort(xid2)
    print(f"  T2 aborted: user:1={engine.read(0, 'user:1')} (restored)")

    # ── Phase 2: Checkpoint and WAL Trace ─────────────────────────────────────
    print("\n─── Phase 2: Checkpoint ───")
    engine.checkpoint()
    engine.print_wal()
    engine._wal.print_stats()

    # ── Phase 3: Recovery ─────────────────────────────────────────────────────
    print("\n─── Phase 3: ARIES Recovery ───")
    # Simulate a crash mid-transaction:
    # T3 writes some data but DOES NOT commit before crash

    xid3 = engine.begin()
    engine.write(xid3, page_id=1, key="order:1", value="pending")
    engine.write(xid3, page_id=1, key="order:2", value="pending")
    # << CRASH HERE — T3 not committed >>
    crash_lsn = engine._wal.current_lsn

    xid4 = engine.begin()
    engine.write(xid4, page_id=1, key="order:3", value="complete")
    engine.commit(xid4)   # T4 committed AFTER crash (won't be recovered)

    # Recover to the crash point (T4's data was written after crash, not in crashed DB)
    recovery = RecoveryEngine(engine._wal)
    recovered_pages = recovery.recover(crash_lsn=crash_lsn)
    print(f"  order:1 after recovery: {recovered_pages.get_page(1).get('order:1')}"
          f" (None = T3 rolled back)")
    print(f"  user:1 after recovery:  {recovered_pages.get_page(0).get('user:1')}"
          f" (T1 committed — preserved)")

    # ── Phase 4: Group Commit ─────────────────────────────────────────────────
    demo_group_commit()

    # ── Phase 5: Full-Page Write ──────────────────────────────────────────────
    print("\n─── Phase 5: Full-Page Writes ───")
    engine2 = StorageEngine()
    xid = engine2.begin()
    engine2.write(xid, page_id=5, key="a", value="1")
    engine2.commit(xid)
    engine2.checkpoint()   # All pages reset — next write needs FPW

    # Next write to page 5 after checkpoint: FPW triggered
    print(f"  After checkpoint, page 5 needs FPW: "
          f"{engine2._pages.needs_full_page_write(5)}")
    xid2 = engine2.begin()
    engine2.write(xid2, page_id=5, key="a", value="2")   # Should trigger FPW
    engine2.commit(xid2)
    print(f"  After write to page 5 post-checkpoint: FPW written")

    # Print WAL to show FPW record
    for rec in WALReader(engine2._wal).scan_from(0):
        if rec.rtype == RecordType.FULL_PAGE:
            pid, _ = decode_full_page(rec.payload)
            print(f"  Found FULL_PAGE record at LSN {rec.lsn} for page {pid}")
```

---

## 6. Visual Reference

### WAL Write Protocol Timeline

```
TRANSACTION TIMELINE:

Client          Buffer Pool         WAL Buffer          WAL on Disk
  │                  │                  │                    │
  │── BEGIN T1 ──►  │                  │                    │
  │                  │                  │── write BEGIN ──►  │
  │                  │                  │                    │
  │── UPDATE row ──► │                  │                    │
  │                  │── modify page ─► │                    │
  │                  │  (in memory)     │── write WRITE ──►  │
  │                  │                  │   (key, old, new)   │
  │                  │                  │                    │
  │── COMMIT ────────────────────────► │                    │
  │                  │                  │── write COMMIT ──► │
  │                  │                  │── fsync ─────────► │ ← DURABILITY POINT
  │◄── success ──────────────────────── │                    │
  │                  │                  │                    │
  │                  │ (later, background checkpoint)         │
  │                  │── write page ────────────────────────► │
  │                  │  (to random location on disk)          │

Key insight: Client sees "success" after WAL fsync (step 6), NOT after page write (step 9).
WAL fsync: ~100μs (sequential write)
Page write: ~1ms (random write)
WAL gives 10× lower commit latency.
```

### ARIES Recovery Phases

```
WAL CONTENTS (after crash):
  LSN=0:  BEGIN T1
  LSN=50: WRITE T1 (page=0, key="a", old=nil, new="v1")
  LSN=120:COMMIT T1                    ← WINNER
  LSN=200:CHECKPOINT (redo_lsn=200)    ← All pages flushed as of LSN=200
  LSN=280:BEGIN T2
  LSN=340:WRITE T2 (page=0, key="b", old=nil, new="v2")
  LSN=400:BEGIN T3
  LSN=450:WRITE T3 (page=1, key="c", old=nil, new="v3")
  LSN=500:COMMIT T3                    ← WINNER
  <<< CRASH HERE (LSN=550) >>>
  T2 never committed → LOSER

PHASE 1 — ANALYSIS (scan from checkpoint LSN=200):
  Active at crash: {T2}     (began, never committed)
  Committed:       {T3}     (committed after checkpoint)
  Dirty pages:     {0, 1}   (modified after checkpoint)

PHASE 2 — REDO (replay from redo_lsn=200 to crash):
  Apply LSN=280 BEGIN T2    → (no-op)
  Apply LSN=340 WRITE T2    → page[0]["b"] = "v2"   (even though T2 is a loser!)
  Apply LSN=400 BEGIN T3    → (no-op)
  Apply LSN=450 WRITE T3    → page[1]["c"] = "v3"
  Apply LSN=500 COMMIT T3   → (no-op — already noted in analysis)

PHASE 3 — UNDO (roll back losers — T2):
  Undo LSN=340 WRITE T2    → page[0]["b"] = nil   (restore old value)
  Write CLR for undo        → logged (in case recovery crashes again)

FINAL STATE:
  page[0]: {"a": "v1"}         (T1 committed before checkpoint, page flushed)
  page[0]["b"] ABSENT          (T2 rolled back)
  page[1]: {"c": "v3"}         (T3 committed)
```

---

## 7. Common Mistakes

**Mistake 1: Setting `synchronous_commit = off` without understanding the durability tradeoff.** `synchronous_commit = off` tells Postgres to return success to the client WITHOUT waiting for the WAL record to be fsynced. The WAL is written to the OS buffer and fsynced asynchronously. If the system crashes within the `wal_writer_delay` window (default 200ms), the last ~200ms of committed transactions are lost — they were reported as committed to the client but the WAL record wasn't on disk yet. For workloads where losing a few recent transactions is acceptable (analytics inserts, counters, non-critical event logging), `synchronous_commit = off` can dramatically improve throughput. For financial transactions, order records, or any data where loss is unacceptable, it must be `on`.

**Mistake 2: Not tuning `checkpoint_completion_target` and experiencing "checkpoint storm."** A checkpoint flushes all dirty pages from the buffer pool to disk. If all dirty pages are flushed at once, it creates a burst of I/O that starves regular query I/O. Postgres spreads checkpoint I/O over `checkpoint_completion_target × checkpoint_timeout` seconds (default: 0.9 × 5min = 4.5min). If `checkpoint_completion_target = 0.5` (old default), a 5-minute checkpoint interval concentrates all checkpoint I/O into 2.5 minutes, leaving 2.5 minutes of no checkpoint I/O — causing CPU spikes and I/O spikes. Set `checkpoint_completion_target = 0.9` in virtually all production configurations.

**Mistake 3: Disabling `full_page_writes` without understanding the risk.** Full-page writes protect against torn pages — a page that is partially written by a crash mid-write. Disabling FPW reduces WAL volume (often by 30-50% for write-heavy OLTP) but makes the database vulnerable to torn-page corruption on systems where the physical sector size doesn't match the page size. On modern SSDs and file systems like ZFS (which use 512-byte or 4KB atomic writes), torn pages on 8KB Postgres pages are still possible. Only disable FPW if you have verified your storage stack provides atomic writes at the database page size — which is rare in practice.

---

## 8. Production Failure Scenarios

### Scenario 1: WAL Volume Growth from Forgotten Replication Slot

(Covered in M44's failure analysis — but warrants inclusion here in the WAL context.)

**Symptoms:** A 500GB NFS mount used for WAL archiving fills up over a weekend. The database goes into read-only mode with error `"PANIC: could not write to file 'pg_wal/00000001000000050000005B': No space left on device"`. All write operations fail.

**Root cause:** A replication slot `analytics_slot` was created for a logical replication subscriber (a CDC consumer). The subscriber was decommissioned without dropping the slot. The slot's `restart_lsn` is frozen at the last LSN it confirmed — Postgres must retain all WAL from that LSN forward, even though the subscriber is gone. Over time, WAL accumulated to fill the 500GB mount.

**Diagnosis and fix:**
```sql
-- Identify stuck replication slots
SELECT slot_name, slot_type, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_behind
FROM pg_replication_slots
ORDER BY restart_lsn ASC;
-- analytics_slot | logical | f | 487 GB ← 487GB of WAL being retained!

-- Fix: Drop the inactive slot (DESTRUCTIVE — subscriber cannot resume)
SELECT pg_drop_replication_slot('analytics_slot');

-- Immediate effect: Postgres can now clean up old WAL segments
-- Next checkpoint will allow archiving/deleting old segments

-- Prevention: Set max_slot_wal_keep_size (Postgres 13+)
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
-- Postgres will invalidate a slot that would require retaining > 10GB of WAL
```

### Scenario 2: Group Commit Starvation Under Low Concurrency

**Symptoms:** A Postgres database serving a single-threaded batch pipeline has commit throughput of only 3,000 transactions/second, even though the CPU is mostly idle and the SSD can do 10,000 I/O ops/second. The pipeline does many small, sequential commits.

**Root cause:** With `synchronous_commit = on` and low concurrency, each transaction gets its own WAL fsync. An NVMe SSD might sustain 100,000 fsyncs/second (10μs per fsync). With one transaction in flight at a time, throughput is 1 / (execution_time + fsync_latency) ≈ 1 / (50μs + 100μs) = ~6,700 transactions/second. Group commit works by having multiple concurrent transactions share one fsync. With sequential single-thread commits, there's nothing to group — each gets its own fsync.

**Fix:** Use a connection pool with multiple concurrent connections to the same database — 8-16 connections all committing simultaneously. Postgres groups the concurrent commits into shared fsyncs, reducing per-transaction fsync overhead. Alternatively, use explicit transaction batching (group 100 inserts into one transaction, one fsync per 100 rows).

---

## 9. Performance and Tuning

### WAL Latency and Throughput

```
WAL performance model:
  commit_latency = wal_write_time + wal_sync_time
  wal_write_time  ≈ WAL_bytes / sequential_write_bandwidth
  wal_sync_time   ≈ 1 / fsync_ops_per_second   (for individual commits)
                  ≈ near 0 for group commit with high concurrency

  NVMe SSD: fsync latency ≈ 50-200μs → 5,000-20,000 commits/sec solo
  SSD SATA:  fsync latency ≈ 200-500μs → 2,000-5,000 commits/sec solo
  HDD:       fsync latency ≈ 5-10ms    → 100-200 commits/sec solo
  Group commit: amortizes fsync over N concurrent transactions
    → effective latency stays low until fsync rate is saturated

wal_sync_method comparison:
  fsync (default): writes + syncs entire OS buffer cache page
  fdatasync: like fsync but doesn't sync metadata (slightly faster)
  open_sync: O_SYNC flag — each write call is synchronous (avoids OS buffering)
  open_datasync: O_DSYNC — sync data but not metadata per write

For NVMe SSDs: fdatasync or open_datasync often fastest
For PostgreSQL on Linux: wal_sync_method=fdatasync is generally optimal
Check: SELECT name, setting FROM pg_settings WHERE name = 'wal_sync_method';
```

### Key Postgres WAL Parameters

```sql
-- ── Durability vs Performance ──────────────────────────────────────────────
-- synchronous_commit: on (safe) | off (1-2× faster, may lose last 200ms)
-- synchronous_standby_names: require N standbys to confirm WAL receipt

-- ── WAL Buffer ──────────────────────────────────────────────────────────────
-- wal_buffers: WAL in-memory buffer (default = 1/32 of shared_buffers, min 64KB, max 16MB)
-- Increase if pg_stat_wal.wal_buffers_full > 0
ALTER SYSTEM SET wal_buffers = '16MB';   -- For high-write workloads

-- ── Checkpoint Tuning ────────────────────────────────────────────────────────
ALTER SYSTEM SET checkpoint_timeout = '10min';              -- How often to checkpoint
ALTER SYSTEM SET max_wal_size = '4GB';                      -- Max WAL before forced checkpoint
ALTER SYSTEM SET checkpoint_completion_target = 0.9;        -- Spread I/O over 90% of interval

-- ── WAL Retention for Replication ────────────────────────────────────────────
ALTER SYSTEM SET wal_keep_size = '1GB';         -- Keep at least 1GB of WAL for standbys
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB'; -- Prevent slot WAL accumulation

-- ── Monitor WAL Performance ──────────────────────────────────────────────────
SELECT
    wal_records,
    wal_fpi,      -- Full page images: high if frequent checkpoints
    pg_size_pretty(wal_bytes) AS wal_bytes,
    wal_write,
    wal_sync,
    round(wal_write_time::numeric, 2) AS write_ms,
    round(wal_sync_time::numeric,  2) AS sync_ms,   -- Key latency metric
    stats_reset
FROM pg_stat_wal;
-- wal_sync_time high → I/O bound fsync (upgrade storage or use group commit)
-- wal_fpi high      → frequent checkpoints or large max_wal_size
```

---

## 10. Interview Q&A

**Q1: What is the Write-Ahead Log and why do databases use it?**

The Write-Ahead Log is a sequentially-written append-only file that records every change made to a database before that change is applied to the permanent data files. Databases use it to solve a fundamental tension in storage engineering: modifying a data page (like a B-tree leaf) requires a random write to wherever that page lives on disk — slow and expensive — while sequential I/O to a log file is 10-100× faster. The WAL converts per-operation random I/O into per-operation sequential I/O.

The protocol is simple: before modifying a page, write a WAL record describing the change to the sequential log and fsync it. Now the change is durable — if the system crashes, the WAL record survives on disk. The page modification happens in the buffer pool (in memory) and is eventually written to disk by background processes. On crash, the WAL is replayed to reconstruct any page modifications that hadn't been written to the permanent data files yet. The client receives "commit confirmed" after the WAL fsync, not after the page write, so commit latency is dominated by sequential I/O (~100μs on NVMe) rather than random I/O (~1ms).

**Q2: Explain ARIES crash recovery. Why does it replay even aborted transactions during the redo phase?**

ARIES is the dominant WAL crash recovery algorithm, used by Postgres, MySQL, Db2, and most major databases. It has three phases.

The analysis phase reads the WAL from the last checkpoint forward, reconstructing which transactions were committed (winners) and which were still active at the time of crash (losers). It also builds a "dirty page table" — which pages were modified since the checkpoint and may not have been flushed to disk.

The redo phase — which some find counterintuitive — replays ALL WAL records from the checkpoint's redo LSN, including records from transactions that will ultimately be undone. The reason: the redo phase's goal is to restore the database to exactly the state it was in at the moment of crash, including uncommitted changes. This is necessary because the undo phase needs to apply undos to the correct page state. If the redo phase skipped loser transactions, the page state at the start of undo would be wrong — some pages would be in their pre-modification state even though the modification was successfully applied before the crash. ARIES's key principle is "repeat history": the redo phase is history-preserving, the undo phase is correctness-restoring.

The undo phase then walks backward through each loser transaction's WAL records (using the prev_lsn chain) and applies the inverse operation for each change. Undo operations are themselves logged as Compensation Log Records (CLRs) — this makes undo idempotent if recovery crashes again mid-undo.

---

## 11. Cross-Question Chain

**Interviewer:** Your Postgres instance is writing 50,000 inserts/second. You notice that `pg_stat_wal.wal_sync_time` is 45,000ms per minute — meaning nearly half of all wall-clock time is spent in fsync. How do you diagnose and fix this?

**Candidate:** 45 seconds of fsync time per minute means either: (1) many individual fsyncs (one per commit, low concurrency), (2) each fsync is slow (saturated storage), or (3) both. I'd start by checking `wal_sync` count in `pg_stat_wal` — if there are 50,000 fsyncs/minute (one per transaction), then group commit is not helping because transactions are being committed sequentially rather than concurrently. If there are 5,000 fsyncs/minute, then each fsync is taking 9ms — which suggests storage saturation (9ms is HDD territory, not NVMe).

For sequential commits: the fix is increasing concurrent connections and batching — if this is a pipeline, use multiple parallel workers or batch 10-100 writes per transaction. For slow fsyncs: check `iostat -x` for disk I/O utilization — if the WAL disk is at 100% utilization, I need either faster storage or to move the WAL to a dedicated NVMe device with `ALTER SYSTEM SET data_directory` (or symlink pg_wal to a dedicated disk).

**Interviewer:** You move the WAL to a dedicated NVMe. fsync time drops, but now `checkpoints_req` in `pg_stat_bgwriter` is much larger than `checkpoints_timed`. What does that mean?

**Candidate:** `checkpoints_req` counts checkpoints triggered because WAL grew to `max_wal_size` (emergency checkpoint), not because `checkpoint_timeout` elapsed. This means the database is generating WAL faster than checkpoints can clean it up — WAL size hits `max_wal_size` frequently, triggering emergency checkpoints. Emergency checkpoints are more aggressive (flush dirty pages faster, creating I/O bursts) than scheduled checkpoints.

The fix: increase `max_wal_size` to give the database more room between checkpoints. If WAL is currently 1GB but we're hitting it constantly with 50K inserts/sec, increase to 4-8GB. Also ensure `checkpoint_completion_target = 0.9` to spread checkpoint I/O. After tuning, `checkpoints_req` should be zero or near-zero — all checkpoints triggered by the timeout, not by WAL overflow.

**Interviewer:** You set `synchronous_commit = off` to improve throughput. What exactly does this risk, and when is it safe?

**Candidate:** `synchronous_commit = off` returns "commit confirmed" to the client without waiting for the WAL record to be fsynced to disk. The WAL writer flushes the buffer asynchronously within `wal_writer_delay` (default 200ms). If the server crashes within that 200ms window, committed transactions whose WAL records weren't fsynced are lost — they were confirmed to the client but aren't on disk. Crucially, this is NOT a corruption risk: the database will still be consistent after crash recovery (no partial transactions, no dirty data) — it just won't have those last 200ms of commits.

It's safe when: (1) the data can be re-derived or re-ingested (event logs, metrics, cache-populating writes), (2) the application can tolerate losing the last few hundred milliseconds of data on crash, (3) idempotent reprocessing is possible. It's not safe when: financial records, order state, user account changes, or any data where acknowledgment to the user means "definitely persisted." In practice, I use `synchronous_commit = off` for write-heavy analytics pipelines where Kafka is the system of record and Postgres is a downstream derived store.

**Interviewer:** What is a full-page write and why does Postgres do it after every checkpoint?

**Candidate:** A full-page write (FPW) is when Postgres includes the entire page image in the WAL record — not just the change delta — for the first modification of a page after each checkpoint. The reason is torn pages: when Postgres writes an 8KB page to disk, the physical storage device may not write all 8KB atomically. If the power fails mid-write, the page on disk contains 4KB of old data and 4KB of new data — a "torn page" with an invalid CRC. Applying a WAL delta record to a torn page produces garbage.

FPW prevents this: the WAL contains the full pre-image of the page. On recovery, Postgres first restores the full page image from the FPW record, then applies subsequent delta records — and the full page is always written atomically from the WAL (since WAL records are smaller and written sequentially, they don't have the partial-write problem). After each checkpoint, all pages in the buffer pool are marked as needing FPW — because the checkpoint flushes pages to disk, and those flushed pages become the "base" that delta records will be applied against. If a page is later written and torn, the last checkpoint flush state is on disk, and the FPW record gives the full page state at the start of the post-checkpoint modifications.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the core invariant of the Write-Ahead Log? | A WAL record describing a change MUST be written and fsynced BEFORE the page containing the change is written to disk. A transaction is durable only after its commit record is fsynced. |
| 2 | Why does WAL improve write performance vs direct page writes? | WAL writes are sequential (append to log end → fast, 100-500× throughput of random writes). Direct page writes are random (write to wherever the page lives on disk → slow, few thousand ops/sec). WAL converts random I/O per-operation to sequential I/O per-operation. |
| 3 | What is an LSN? | Log Sequence Number: a monotonically increasing byte offset in the WAL file. Uniquely identifies each WAL record. Used to determine recovery progress, skip already-applied records, and coordinate replication. |
| 4 | What are the three phases of ARIES recovery? | Analysis: find committed (winners) vs active (losers) transactions. Redo: replay ALL records from last checkpoint (including losers) to restore crash-time state. Undo: roll back all loser transactions. |
| 5 | Why does ARIES redo even aborted transactions? | To restore the exact crash-time page state before applying undo. Undo must apply to the correct page state. If redo skipped losers, pages would be in wrong states and undo would produce incorrect results. |
| 6 | What is a Compensation Log Record (CLR)? | A WAL record written during the undo phase to log each undo operation. Makes undo idempotent: if recovery crashes during undo, CLRs prevent re-undoing already-undone changes. |
| 7 | What is a checkpoint? | A WAL synchronization point where all dirty pages are flushed to disk, and a CHECKPOINT record is written with the redo LSN. Recovery after a crash only needs to replay WAL from the checkpoint's redo LSN — older WAL can be archived/deleted. |
| 8 | What is a full-page write (FPW) and why is it done after every checkpoint? | FPW includes the entire page image in the WAL record for the first modification of a page after each checkpoint. Protects against torn pages (partial writes of 8KB pages to disk). After checkpoint, the page on disk is the new "base" — deltas applied to a torn base would corrupt data. FPW ensures recovery can restore a clean full page before applying deltas. |
| 9 | What does `synchronous_commit = off` risk? | Up to `wal_writer_delay` (200ms default) of committed transactions may be lost on crash. Not a corruption risk — database remains consistent after recovery — but acknowledged commits may be missing. Safe for re-derivable data; unsafe for financial or user-facing critical data. |
| 10 | What causes `checkpoints_req` to exceed `checkpoints_timed`? | WAL size reaching `max_wal_size` before the `checkpoint_timeout` expires. Means the database generates WAL faster than scheduled checkpoints can handle. Fix: increase `max_wal_size`. |
| 11 | What is group commit? | Batching multiple concurrent transactions' commit WAL records into a single fsync. Amortizes the fsync cost across N transactions. Effective when many transactions are in-flight simultaneously. Single-threaded sequential commits defeat group commit. |
| 12 | What information does a WAL record contain? | LSN (position), XID (transaction ID), prev_lsn (previous record in this transaction), record type, resource manager ID, and payload (what changed: page ID, key, old value, new value). |
| 13 | What does `wal_level = logical` enable vs `replica`? | `logical` adds additional change data to WAL records (column values before/after change) needed for logical decoding. Required for CDC tools (Debezium, pgoutput), logical replication, and replication slots. `replica` is sufficient for physical streaming replication. `logical` generates 10-30% more WAL. |
| 14 | How does a replication slot affect WAL retention? | Postgres must retain all WAL from the slot's `restart_lsn` (last confirmed position) forward — even if the subscriber is disconnected. Inactive slots accumulate WAL unboundedly. Fix: `max_slot_wal_keep_size` (Postgres 13+) limits WAL retention per slot. |
| 15 | What is the relationship between WAL and MVCC? | MVCC (Multi-Version Concurrency Control) creates new heap tuple versions for each UPDATE rather than modifying in-place. WAL records both the old and new tuple versions. On recovery, MVCC state is rebuilt from WAL — old versions become invisible per visibility rules once their creating transaction is confirmed committed/aborted. |

---

## 13. Further Reading

- **"PostgreSQL: Up and Running" — Chapter on WAL and PITR:** Practical coverage of Postgres-specific WAL configuration, archiving, and Point-in-Time Recovery.
- **ARIES paper: "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging" (Mohan et al., 1992):** The original paper. Dense but authoritative. The abstract and Section 2 (Overview) are accessible.
- **Postgres documentation — "Write Ahead Log" chapter:** Covers `pg_stat_wal`, checkpoint tuning, WAL archiving configuration, and `pg_waldump` usage.
- **`pg_waldump` tool:** Ships with Postgres, decodes WAL records to human-readable form. `pg_waldump -p $PGDATA/pg_wal -n 20` prints the last 20 WAL records.
- **"Database Internals" by Alex Petrov, Chapter 7 (Log-Structured Storage):** Covers WAL mechanics in database-agnostic terms, including group commit, checkpoints, and fuzzy checkpoints.

---

## 14. Lab Exercises

**Exercise 1: Trace WAL Records**
Run `wal_engine.py`. Add print statements in `StorageEngine.write()` and `StorageEngine.commit()` that log: LSN assigned, whether FPW was triggered, and whether this write caused an fsync. Insert 3 transactions (commit 2, abort 1) and trace the complete WAL.

**Exercise 2: Simulate Torn Page Recovery**
In `wal_engine.py`, implement a `torn_page_crash()` method that corrupts a page (sets half its data to garbage bytes) before recovery. Run recovery and verify that FPW records allow correct recovery despite the torn page.

**Exercise 3: Measure Group Commit Benefit**
Extend `demo_group_commit()` to vary N from 1 to 100 transactions. For each N, calculate the simulated fsync count with individual commits vs group commit. Plot fsync_count vs N. At what N does group commit deliver the most benefit?

**Exercise 4: Recovery Completeness**
Write a test that: (1) inserts 1000 key-value pairs across 100 transactions (all committed), (2) runs a checkpoint, (3) inserts 200 more (100 committed, 100 pending at crash), (4) calls `recovery.recover()`, (5) verifies that exactly the 1100 committed pairs are present and the 100 uncommitted pairs are absent.

---

## 15. Key Takeaways

The Write-Ahead Log is the mechanism that makes database writes both fast and durable. It resolves the tension between random I/O (slow, necessary for B-tree page writes) and sequential I/O (fast, suitable for append-only logging) by deferring random I/O and guaranteeing durability through sequential I/O first.

The WAL write protocol — write WAL record → fsync WAL → return to client — means commit latency is dominated by sequential WAL I/O (~100μs on NVMe) not random page I/O (~1ms). Group commit amortizes fsync cost across concurrent transactions, making high-concurrency workloads dramatically more efficient.

ARIES recovery — Analysis, Redo, Undo — is the standard algorithm: it guarantees crash recovery to the exact committed state through a combination of "repeat history" (redo even losers) and selective rollback (undo uncommitted changes). Full-page writes protect against torn pages. Checkpoints bound recovery time by providing a guaranteed redo start point.

The production implications are concrete: replication slots that accumulate WAL kill disk space, `synchronous_commit = off` trades 200ms of durability for throughput, `max_wal_size` controls the balance between checkpoint frequency and WAL growth, and `wal_sync_time` in `pg_stat_wal` is the key metric for identifying WAL I/O bottlenecks.

---

## 16. Connections to Other Modules

- **M47 — B-Tree Storage Engines:** B-tree page modifications are logged to the WAL before the page is written. Full-page writes (FPW) are generated for the first post-checkpoint modification of each B-tree page. WAL is the crash safety layer for the B-tree.
- **M48 — LSM-Tree Storage Engines:** LSM-trees use a simpler WAL (just the MemTable WAL — no full-page writes needed since SSTables are written completely in one sequential flush). The WAL is truncated after each MemTable flush. Understanding both WAL designs illuminates how the two architectures differ in their durability approaches.
- **M42 — Leader-Follower Replication Log:** That module implemented replication as a WAL-based protocol (LSN tracking, flush_lsn, replay_lsn). Postgres streaming replication IS WAL shipping: the WAL is streamed from primary to standbys, which replay it. The LSN concepts from M42 and M49 are the same concept.
- **M50 — Buffer Pool Management:** The buffer pool is the "other side" of the WAL story. The WAL records what's dirty; the buffer pool manages which dirty pages need to be flushed to disk and when. The interaction between WAL (must flush WAL record before flushing page) and buffer pool (when to flush dirty pages) determines checkpoint behavior and recovery time.

---

## 17. Anti-Patterns

**Anti-pattern: Placing WAL and data files on the same disk.** WAL writes are sequential; data page writes (from checkpoints and bgwriter) are random. Mixing them on the same disk causes I/O pattern interference — the disk head must alternate between sequential and random positions, reducing effective throughput of both. In production Postgres, place `pg_wal` on a dedicated SSD or NVMe device. Even within SSDs, separating WAL I/O from data I/O provides better queue depth utilization.

**Anti-pattern: Creating many logical replication slots without monitoring WAL growth.** Each active logical replication slot retains WAL from its `restart_lsn` forward. Inactive or lagging slots silently accumulate WAL until disk is exhausted. Always: (1) set `max_slot_wal_keep_size` (Postgres 13+) as a safety limit, (2) monitor `pg_replication_slots.restart_lsn` and alert if any slot is more than 1GB behind `pg_current_wal_lsn()`.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pg_waldump` | Decode WAL records to human-readable text | `pg_waldump -p pg_wal -n 50` |
| `pg_stat_wal` | WAL write/fsync statistics | Monitor `wal_sync_time`, `wal_buffers_full` |
| `pg_stat_bgwriter` | Checkpoint and bgwriter statistics | `checkpoints_req`, `buffers_backend` |
| `pg_replication_slots` | Replication slot state | Monitor `restart_lsn` for accumulation |
| `wal_engine.py` | WAL simulation | WAL records, ARIES recovery, group commit |
| `iostat -x` | Disk I/O utilization | Identify WAL disk saturation |
| Postgres `archive_mode + archive_command` | Continuous WAL archiving for PITR | Ship WAL to S3/GCS |

---

## 19. Glossary

**ARIES (Algorithms for Recovery and Isolation Exploiting Semantics):** The standard WAL crash recovery algorithm used by most major databases. Three phases: Analysis (identify winners/losers), Redo (replay all records to restore crash-time state), Undo (roll back loser transactions).

**Checkpoint:** A WAL synchronization point where all dirty pages are flushed to disk and a CHECKPOINT record marks the redo LSN. Recovery only needs to replay WAL from the checkpoint's redo LSN forward.

**CLR (Compensation Log Record):** A WAL record written during the undo phase to log undo operations. Enables idempotent recovery if recovery crashes during undo.

**Full-page write (FPW):** Writing the complete page image to WAL (not just the delta) for the first modification of a page after each checkpoint. Protects against torn pages (partial page writes on crash).

**Group commit:** Amortizing WAL fsync cost across multiple concurrent transactions that commit at approximately the same time. Single fsync per "batch" instead of one fsync per transaction.

**LSN (Log Sequence Number):** Monotonically increasing byte offset in the WAL file. Identifies each WAL record's position. Used for recovery, replication, and page-level tracking.

**Synchronous commit:** The Postgres parameter controlling whether commit waits for WAL fsync (`on`) or returns immediately after WAL write (`off`). `off` risks losing last ~200ms of committed transactions on crash but significantly improves throughput.

**Torn page:** A page written only partially to disk due to a crash mid-write. The stored page is a mix of old and new data. Full-page writes in the WAL protect against torn page corruption.

**WAL (Write-Ahead Log):** A sequentially-written append-only file containing records of every change made to a database. Written and fsynced before the page is modified on disk. Used for crash recovery, replication, and PITR.

**WAL segment:** A fixed-size file (16MB in Postgres) containing a portion of the WAL. Segments are archived and eventually deleted once no longer needed for recovery or replication.

---

## 20. Self-Assessment

1. Explain why `commit_latency = wal_sync_time` rather than `page_write_time`. What does this imply for database hardware selection?
2. In ARIES recovery, if a page was already flushed to disk before the crash (its LSN on disk is greater than the WAL record's LSN), what does the redo phase do?
3. A transaction writes to 5 pages. How many WAL records are generated minimum? How many if this is the first write to each page after the last checkpoint?
4. In `wal_engine.py`, trace through what happens when `crash_lsn` is set to immediately after `xid3`'s second WRITE record. Which data survives recovery? Which is rolled back?
5. Why does Postgres not simply write all dirty pages to disk before returning the commit acknowledgment to the client, rather than using a WAL?
6. What is `wal_level = logical` and when would you set it? What is its performance cost?
7. You see `buffers_backend` is high in `pg_stat_bgwriter`. What does this mean and how do you fix it?
8. Explain the relationship between `checkpoint_timeout`, `max_wal_size`, and `checkpoint_completion_target`. How do you tune them for a write-heavy OLTP workload?

---

## 21. Module Summary

The Write-Ahead Log is the foundational durability mechanism of database storage engines. Its design is elegantly simple: write the change description to a sequential log and fsync it before modifying the page; if the system crashes, replay the log to restore the committed state. This converts per-operation random I/O (slow) to per-operation sequential I/O (fast) while maintaining ACID durability guarantees.

The implementation — `WALWriter` with LSN-based sequencing, `WALRecord` with CRC integrity, `StorageEngine` with full-page writes and checkpoint support, and `RecoveryEngine` with ARIES-style three-phase recovery — makes each component concrete. The group commit demonstration explains why high-concurrency workloads efficiently share fsync cost while single-threaded pipelines are bottlenecked by per-commit fsyncs.

Production WAL knowledge — monitoring `wal_sync_time`, understanding replication slot WAL accumulation, tuning `synchronous_commit`, sizing `max_wal_size`, and enabling WAL archiving for PITR — is directly applicable to operating Postgres in production. These are the levers that determine whether a database achieves 1,000 commits/second or 100,000 commits/second.

The next module — M50: Buffer Pool Management — examines the other half of the storage engine picture: the in-memory cache that holds modified pages between WAL write and disk flush, and the eviction, dirty page flushing, and double-write buffer mechanisms that make the buffer pool safe and efficient.
