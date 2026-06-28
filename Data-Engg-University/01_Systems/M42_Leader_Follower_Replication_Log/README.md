# M42: Implementing a Leader-Follower Replication Log

**Course:** SYS-DST-102 — Distributed Systems in Practice  
**Module:** 01 of 05  
**Global Module ID:** M42  
**Semester:** 2  
**School:** Systems (SYS)

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

M39 described leader-follower replication as a concept — one node accepts writes, others replicate changes. That description is essential for understanding the guarantee model. But there is a significant gap between understanding that "followers replay a replication log" and being able to reason about what happens at the byte level when a follower lags, when a leader's WAL segment rotates, or when a follower reconnects after a crash and must find the right point to resume replication.

This module closes that gap by implementing a complete leader-follower replication log in Python. We build every component from scratch: the WAL (write-ahead log) that the leader writes to, the replication protocol that transfers log entries to followers, the follower's state machine that applies entries and tracks its position, and the recovery logic that handles crashes and reconnections. Every abstraction from M39 becomes concrete code you can read, modify, and break.

The implementation follows the same structure used by Postgres physical streaming replication — not as a toy that ignores the hard parts, but as a simplified-but-complete system that makes the right design choices where they matter. By the time this module is complete, questions like "why does Postgres replication track sent_lsn, write_lsn, flush_lsn, and replay_lsn separately?" and "why does Kafka's ISR use a lag threshold rather than a binary connected/disconnected state?" will have obvious answers in the code you have written.

This is not academic exercise. The engineering judgment required to implement replication correctly — choosing durable WAL fsync over speed, designing reconnect-safe position tracking, making idempotent apply operations — is identical to the judgment required to configure and debug production replication systems. Knowing what is hard to implement makes you dramatically better at configuring systems that already implement it.

---

## 2. Mental Model

### The Replication Log as a Data Structure

A replication log is an append-only sequence of entries. Each entry describes a state change to the system. The log has these properties:

1. **Append-only:** entries are never modified or deleted from the middle. Once written, they are immutable. New entries are added only at the end.
2. **Indexed:** each entry has a monotonically increasing position (log sequence number, LSN, or offset). Followers track their current position in the leader's log.
3. **Durable:** entries survive crashes. The leader writes entries to disk (fsync) before acknowledging them to clients. Followers write entries to disk before acknowledging them to the leader.
4. **Ordered:** entries are applied in sequence. Entry N must be applied before entry N+1. The order is the order in which the leader accepted the writes.

This is the same data structure as a Kafka topic partition, a Postgres WAL segment, a Raft log, and a Redis AOF file. Every persistent log-structured system uses this structure because it enables three critical properties: fast sequential writes (append-only avoids seek), crash recovery (replay from last checkpoint), and replication (send entries to followers and let them replay).

### The Two-Level State Machine

A leader-follower replication system has two state machines:

**Leader's state machine:** the source of truth. Accepts writes, appends them to the WAL, and reports the WAL position (LSN) back to the writer. Followers read from the leader's WAL.

**Follower's state machine:** a replica. Reads entries from the leader's WAL starting at its current position, writes them to its local WAL, applies them to its local state (buffer/store), and advances its tracked position. On reconnect after a crash, it sends its last known position and asks the leader to send all entries from that position forward.

The gap between the leader's current LSN and the follower's replayed LSN is the replication lag.

---

## 3. Core Concepts

### 3.1 WAL Entry Format

Every WAL entry contains:
- **LSN (Log Sequence Number):** a monotonically increasing 64-bit integer identifying this entry's position
- **Term/Epoch:** identifies which leader wrote this entry (used for conflict detection on reconnect)
- **Type:** the kind of operation (PUT, DELETE, CHECKPOINT, etc.)
- **Payload:** the data for this operation
- **CRC:** checksum for detecting corruption

```
WAL Entry on disk:
┌─────────────┬──────────┬────────┬──────────────────────┬──────────┐
│ LSN (8 bytes)│ Term (4B)│Type (1B)│ Payload (variable)  │ CRC (4B) │
└─────────────┴──────────┴────────┴──────────────────────┴──────────┘
```

The LSN is the key concept. It serves as:
- A position for followers to track their replication progress
- A cursor for the leader to know where to resume sending after a follower reconnects
- A conflict detector: if a follower's last LSN doesn't match the leader's entry at that position, log divergence is detected

### 3.2 Leader WAL Position Tracking

The leader tracks multiple WAL positions per follower, corresponding to pipeline stages:

```
Leader WAL:  [LSN 1][LSN 2][LSN 3][LSN 4][LSN 5]
                                          ↑ leader_lsn (current end)

Per-follower tracking:
  sent_lsn:   last LSN sent to follower (in flight, may not be written yet)
  write_lsn:  follower has written to its WAL buffer
  flush_lsn:  follower has fsynced to disk
  replay_lsn: follower has applied to its state machine

Commit condition (synchronous):
  An entry is committed on the follower when flush_lsn >= entry.lsn
  (the entry is durable on the follower's disk)
```

This multi-position tracking is exactly what Postgres's `pg_stat_replication` view exposes. Each lag column measures a different pipeline stage:
- `write_lag`: time between primary WAL flush and standby OS buffer write
- `flush_lag`: time between primary WAL flush and standby fsync
- `replay_lag`: time between primary WAL flush and standby apply (user-observable lag)

### 3.3 Follower Reconnect Protocol

When a follower reconnects after being offline, it must:
1. Send its last known position (`last_flush_lsn`) to the leader
2. The leader finds this position in its WAL
3. If the position is valid (entry exists and has the correct term), the leader starts streaming from `last_flush_lsn + 1`
4. If the position is invalid (entry doesn't exist or has wrong term — indicating log divergence), the leader sends a "resync required" signal, and the follower must either accept a full resync or be rejected

This protocol prevents a follower from applying entries from a divergent leader's log (split-brain protection).

### 3.4 Log Segmentation and Retention

A WAL is stored as a series of fixed-size segments (e.g., 16MB each in Postgres). Old segments can be deleted once no follower needs them — the leader tracks the minimum flush_lsn across all followers and retains all segments containing entries >= that LSN.

If a follower is offline for long enough that its required WAL segment is deleted, it must do a full base backup (a snapshot of the leader's current state) and then resume streaming from the backup's LSN. This is the Postgres `pg_basebackup` operation.

---

## 4. Hands-On Walkthrough

### 4.1 Tracing Postgres Replication Positions

```sql
-- On the PRIMARY: understand each replication position
SELECT
    application_name,
    -- sent_lsn: leader has sent up to here (in network buffer)
    -- Follower may not have received/written it yet
    sent_lsn,
    -- write_lsn: follower has written to OS buffer (fast, not durable)
    write_lsn,
    -- flush_lsn: follower has fsynced to disk (durable)
    -- This is what synchronous_commit=on waits for
    flush_lsn,
    -- replay_lsn: follower has applied to data files
    -- This is what hot-standby reads reflect
    replay_lsn,
    -- Lag in bytes at each stage
    (sent_lsn  - write_lsn)  AS write_pending_bytes,
    (write_lsn - flush_lsn)  AS flush_pending_bytes,
    (flush_lsn - replay_lsn) AS replay_pending_bytes,
    -- Time lag at each stage
    write_lag,
    flush_lag,
    replay_lag,
    sync_state   -- sync, async, quorum
FROM pg_stat_replication;

-- Understanding the pipeline:
-- Leader WAL write → sent_lsn → write_lsn → flush_lsn → replay_lsn
--                   (network)   (OS buf)    (disk)      (applied)

-- Current leader LSN
SELECT pg_current_wal_lsn();

-- On STANDBY: check own position
SELECT
    pg_last_wal_receive_lsn() AS received,   -- what the WAL receiver got
    pg_last_wal_replay_lsn()  AS replayed,   -- what the apply process replayed
    pg_last_xact_replay_timestamp() AS lag_as_time;
```

### 4.2 Observing WAL Segment Rotation

```bash
# List WAL segments on the primary
ls -lh $PGDATA/pg_wal/
# 000000010000000000000001  ← segment: timeline=1, segment=1
# 000000010000000000000002
# 000000010000000000000003  ← current (3 segments = 48MB of WAL)

# Force a WAL segment switch (for testing)
psql -c "SELECT pg_switch_wal();"
# Returns the LSN at which the switch happened

# Check WAL segment that contains a specific LSN
psql -c "SELECT pg_walfile_name('0/1A2B3C4D');"
# Returns: 000000010000000000000001

# Check which WAL segments a standby still needs
# If oldest required LSN is 0/1000000, segments before that can be deleted
SELECT pg_walfile_name(restart_lsn) AS oldest_required_segment
FROM pg_replication_slots;
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
replication_log.py

A complete implementation of leader-follower replication in Python.

Components:
  - WALEntry: typed log entry with CRC checksum
  - WALWriter: durable write-ahead log (leader side)
  - WALReader: sequential WAL reader for follower streaming
  - ReplicationLeader: streams WAL entries to connected followers
  - ReplicationFollower: receives and applies WAL entries
  - ReplicationSystem: wires everything together for demonstration

Design choices that mirror production systems:
  - Entries are written to disk (simulated) before being acknowledged
  - Followers track flush_lsn (durable position) not just received position
  - Reconnect protocol validates term to detect log divergence
  - Old segments are retained as long as any follower needs them

No external dependencies.
"""

import hashlib
import json
import os
import struct
import threading
import time
import queue
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Iterator


# ─── WAL Entry Format ────────────────────────────────────────────────────────────

class EntryType(Enum):
    PUT        = 1   # Set key → value
    DELETE     = 2   # Remove key
    CHECKPOINT = 3   # Leader checkpoint marker
    NOOP       = 4   # Heartbeat / keep-alive


@dataclass
class WALEntry:
    lsn:     int         # Log Sequence Number (monotonically increasing)
    term:    int         # Leader term (epoch) — used for split-brain detection
    type:    EntryType
    key:     str = ""
    value:   str = ""

    def serialize(self) -> bytes:
        """Serialize to bytes for disk storage."""
        payload = json.dumps({
            'lsn': self.lsn,
            'term': self.term,
            'type': self.type.value,
            'key': self.key,
            'value': self.value,
        }).encode()
        crc = hashlib.crc32(payload) & 0xFFFFFFFF
        # Format: 4-byte payload length + payload + 4-byte CRC
        return struct.pack('>I', len(payload)) + payload + struct.pack('>I', crc)

    @classmethod
    def deserialize(cls, data: bytes) -> tuple['WALEntry', int]:
        """
        Deserialize from bytes. Returns (entry, bytes_consumed).
        Raises ValueError on CRC mismatch.
        """
        if len(data) < 8:
            raise ValueError("Insufficient data for WAL entry header")
        length = struct.unpack('>I', data[:4])[0]
        if len(data) < 4 + length + 4:
            raise ValueError("Truncated WAL entry")
        payload = data[4:4 + length]
        stored_crc = struct.unpack('>I', data[4 + length:8 + length])[0]
        computed_crc = hashlib.crc32(payload) & 0xFFFFFFFF
        if stored_crc != computed_crc:
            raise ValueError(f"CRC mismatch: stored={stored_crc} computed={computed_crc}")
        d = json.loads(payload)
        entry = cls(
            lsn=d['lsn'], term=d['term'],
            type=EntryType(d['type']),
            key=d.get('key', ''), value=d.get('value', ''),
        )
        return entry, 8 + length


# ─── WAL Writer (Leader Side) ────────────────────────────────────────────────────

class WALWriter:
    """
    Append-only write-ahead log. Provides durable sequential writes.

    Design decisions:
    - Each entry gets a monotonically increasing LSN
    - Entries are written to an in-memory buffer (simulating page cache)
    - flush() simulates fsync — making entries durable on disk
    - All entries are kept in memory (no segment rotation for simplicity)
    """

    def __init__(self, term: int = 1):
        self.term = term
        self._entries: list[WALEntry] = []        # In-memory WAL (simulates disk)
        self._flushed_through: int = 0            # Last LSN fsynced to disk
        self._next_lsn: int = 1
        self._lock = threading.Lock()
        # Subscribers notified when new entries are flushed
        self._subscribers: list[queue.Queue] = []

    @property
    def current_lsn(self) -> int:
        """Current leader LSN (end of log)."""
        return self._next_lsn - 1

    @property
    def flush_lsn(self) -> int:
        """Highest LSN durably written to disk."""
        return self._flushed_through

    def append(self, type: EntryType, key: str = "", value: str = "") -> WALEntry:
        """
        Append an entry to the WAL. Returns the entry with its assigned LSN.
        The entry is in the write buffer but NOT yet flushed (not durable).
        """
        with self._lock:
            entry = WALEntry(
                lsn=self._next_lsn,
                term=self.term,
                type=type,
                key=key,
                value=value,
            )
            self._entries.append(entry)
            self._next_lsn += 1
            return entry

    def flush(self) -> int:
        """
        Simulate fsync: make all buffered entries durable.
        Returns the new flush_lsn.
        In production: this is where fdatasync() is called on the WAL file.
        """
        with self._lock:
            if self._entries:
                self._flushed_through = self._entries[-1].lsn
            # Notify all follower queues of new flushed entries
            new_entries = [e for e in self._entries if e.lsn > self._flushed_through - len(
                [x for x in self._entries if x.lsn <= self._flushed_through]
            )]
            for q in self._subscribers:
                for entry in self._entries:
                    if entry.lsn <= self._flushed_through:
                        try:
                            q.put_nowait(entry)
                        except queue.Full:
                            pass
            return self._flushed_through

    def write_and_flush(self, type: EntryType, key: str = "",
                        value: str = "") -> WALEntry:
        """
        Append + flush atomically. This is the synchronous write path:
        the entry is durable before this function returns.
        """
        entry = self.append(type, key, value)
        with self._lock:
            self._flushed_through = entry.lsn
        # Notify subscribers
        for q in self._subscribers:
            try:
                q.put_nowait(entry)
            except queue.Full:
                pass
        return entry

    def read_from(self, start_lsn: int) -> list[WALEntry]:
        """
        Return all entries with LSN >= start_lsn that are flushed.
        Used by followers to fetch entries they haven't seen yet.
        """
        with self._lock:
            return [e for e in self._entries
                    if e.lsn >= start_lsn and e.lsn <= self._flushed_through]

    def get_entry(self, lsn: int) -> Optional[WALEntry]:
        """Return a specific entry by LSN (for reconnect validation)."""
        with self._lock:
            for e in self._entries:
                if e.lsn == lsn:
                    return e
            return None

    def subscribe(self) -> queue.Queue:
        """
        Subscribe to new WAL entries. Returns a queue that receives
        entries as they are flushed. Used for push-based replication.
        """
        q: queue.Queue = queue.Queue(maxsize=10000)
        with self._lock:
            self._subscribers.append(q)
            # Send all existing flushed entries immediately
            for e in self._entries:
                if e.lsn <= self._flushed_through:
                    try:
                        q.put_nowait(e)
                    except queue.Full:
                        break
        return q

    def min_required_lsn(self, follower_flush_lsns: list[int]) -> int:
        """
        Minimum LSN any follower still needs.
        Entries before this LSN can be safely deleted (WAL segment cleanup).
        """
        if not follower_flush_lsns:
            return self._flushed_through
        return min(follower_flush_lsns)


# ─── Follower Position State ──────────────────────────────────────────────────────

@dataclass
class FollowerPosition:
    """
    Tracks a follower's position in the replication pipeline.
    Mirrors pg_stat_replication columns exactly.
    """
    follower_id: str
    sent_lsn:   int = 0   # Last entry sent (may be in transit)
    write_lsn:  int = 0   # Received and written to OS buffer
    flush_lsn:  int = 0   # Written to disk (durable — commit threshold)
    replay_lsn: int = 0   # Applied to state machine (observable to readers)

    @property
    def lag(self) -> int:
        """Replication lag in log entries (flush position behind leader)."""
        return max(0, self.flush_lsn - self.replay_lsn)


# ─── Replication Leader ──────────────────────────────────────────────────────────

class ReplicationLeader:
    """
    Manages replication to a set of followers.
    Provides:
      - Follower registration and position tracking
      - Synchronous/asynchronous write modes
      - Follower reconnect with position validation
      - ISR (in-sync replica) management
    """

    def __init__(self, wal: WALWriter, min_sync_followers: int = 1,
                 lag_threshold: int = 10):
        self.wal = wal
        self.min_sync_followers = min_sync_followers
        self.lag_threshold = lag_threshold    # Entries behind before leaving ISR
        self._positions: dict[str, FollowerPosition] = {}
        self._lock = threading.Lock()
        self._events: list[str] = []

    def _log(self, msg: str):
        ts = time.monotonic()
        self._events.append(f"[{ts:.3f}] {msg}")
        print(f"  [Leader] {msg}")

    def register_follower(self, follower_id: str,
                          start_lsn: int = 0,
                          last_term: int = 0) -> tuple[bool, int]:
        """
        Register a new or reconnecting follower.

        For a reconnecting follower:
        - Validates that their last_flush_lsn matches the leader's log at that position
        - If valid: resumes streaming from last_flush_lsn + 1
        - If diverged: returns (False, -1) — follower must resync

        Returns (success, resume_from_lsn).
        """
        with self._lock:
            if start_lsn > 0:
                # Reconnecting follower: validate their last known position
                leader_entry = self.wal.get_entry(start_lsn)
                if leader_entry is None:
                    self._log(f"Follower {follower_id} reconnect FAILED: "
                              f"LSN {start_lsn} not in leader log — resync required")
                    return False, -1
                if leader_entry.term != last_term:
                    self._log(f"Follower {follower_id} reconnect FAILED: "
                              f"term mismatch at LSN {start_lsn} "
                              f"(leader={leader_entry.term}, follower={last_term}) "
                              f"— log divergence, resync required")
                    return False, -1
                resume_from = start_lsn + 1
                self._log(f"Follower {follower_id} reconnected, "
                          f"resuming from LSN {resume_from}")
            else:
                resume_from = 1   # New follower: start from the beginning
                self._log(f"Follower {follower_id} registered (new)")

            self._positions[follower_id] = FollowerPosition(
                follower_id=follower_id,
                flush_lsn=start_lsn,
                replay_lsn=start_lsn,
            )
            return True, resume_from

    def update_follower_position(self, follower_id: str,
                                  write_lsn: int = 0,
                                  flush_lsn: int = 0,
                                  replay_lsn: int = 0):
        """
        Process a position update from a follower.
        Followers send position updates after each pipeline stage.
        """
        with self._lock:
            if follower_id not in self._positions:
                return
            pos = self._positions[follower_id]
            if write_lsn:
                pos.write_lsn = max(pos.write_lsn, write_lsn)
            if flush_lsn:
                pos.flush_lsn = max(pos.flush_lsn, flush_lsn)
            if replay_lsn:
                pos.replay_lsn = max(pos.replay_lsn, replay_lsn)

    def get_isr(self) -> list[str]:
        """
        Return the In-Sync Replica set: followers whose flush_lsn is
        within lag_threshold entries of the leader's current LSN.
        """
        with self._lock:
            leader_lsn = self.wal.flush_lsn
            return [
                fid for fid, pos in self._positions.items()
                if (leader_lsn - pos.flush_lsn) <= self.lag_threshold
            ]

    def is_committed_on_quorum(self, lsn: int,
                               required_followers: int = 1) -> bool:
        """
        Check if an entry is durably committed on at least
        required_followers followers (for semi-synchronous replication).
        """
        with self._lock:
            count = sum(
                1 for pos in self._positions.values()
                if pos.flush_lsn >= lsn
            )
            return count >= required_followers

    def wait_for_commit(self, lsn: int, timeout_s: float = 5.0) -> bool:
        """
        Wait until lsn is committed on min_sync_followers followers.
        This implements synchronous replication from the leader side.
        """
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.is_committed_on_quorum(lsn, self.min_sync_followers):
                return True
            time.sleep(0.01)
        return False

    def print_positions(self):
        with self._lock:
            leader_lsn = self.wal.flush_lsn
            isr = self.get_isr()
            print(f"\n  Leader LSN: {leader_lsn}")
            print(f"  ISR: {isr}")
            for fid, pos in self._positions.items():
                lag = leader_lsn - pos.flush_lsn
                in_isr = "✓ ISR" if fid in isr else "✗ out-of-sync"
                print(f"    {fid}: flush={pos.flush_lsn} replay={pos.replay_lsn} "
                      f"lag={lag} [{in_isr}]")


# ─── Replication Follower ────────────────────────────────────────────────────────

class ReplicationFollower:
    """
    Applies WAL entries from the leader and maintains a local state machine.

    Implements:
      - Idempotent apply (safe to re-apply already-seen entries)
      - Position tracking and reporting back to leader
      - Crash recovery: on restart, reconnects with last flush_lsn
      - Simulated disk flush (fsync) before reporting flush_lsn to leader
    """

    def __init__(self, follower_id: str, leader: ReplicationLeader,
                 apply_delay_ms: float = 2.0):
        self.follower_id = follower_id
        self.leader = leader
        self.apply_delay_ms = apply_delay_ms

        # Follower state
        self._state: dict[str, str] = {}         # Applied key-value store
        self._write_lsn: int = 0                 # Written to OS buffer
        self._flush_lsn: int = 0                 # Flushed to disk (durable)
        self._replay_lsn: int = 0                # Applied to state machine
        self._last_term: int = 0                 # Term of last applied entry
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._events: list[str] = []

    def _log(self, msg: str):
        self._events.append(msg)
        print(f"  [{self.follower_id}] {msg}")

    def start(self, start_lsn: int = 0):
        """
        Start the follower. Registers with the leader and begins applying entries.
        start_lsn=0 for new followers, last flush_lsn for reconnecting followers.
        """
        success, resume_from = self.leader.register_follower(
            self.follower_id,
            start_lsn=start_lsn,
            last_term=self._last_term,
        )
        if not success:
            self._log("Reconnect failed — need full resync (not implemented in demo)")
            return False

        self._running = True
        self._thread = threading.Thread(
            target=self._apply_loop,
            args=(resume_from,),
            daemon=True,
        )
        self._thread.start()
        return True

    def stop(self):
        """Simulate a clean shutdown (flush before stopping)."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def crash(self):
        """
        Simulate a crash: stop without flushing OS buffer.
        After crash, flush_lsn (durable) is the safe restart position.
        write_lsn may be ahead of flush_lsn — those entries are lost.
        """
        self._log(f"CRASHED (write_lsn={self._write_lsn}, "
                  f"flush_lsn={self._flush_lsn} — "
                  f"{self._write_lsn - self._flush_lsn} entries lost from OS buffer)")
        self._running = False
        # On crash: write_lsn reverts to flush_lsn (OS buffer lost)
        with self._lock:
            self._write_lsn = self._flush_lsn

    def recover(self):
        """
        Recover from crash: reconnect with last flush_lsn.
        The leader will resend entries from flush_lsn + 1.
        """
        self._log(f"Recovering from crash, reconnecting at flush_lsn={self._flush_lsn}")
        return self.start(start_lsn=self._flush_lsn)

    def _apply_loop(self, start_lsn: int):
        """
        Main replication loop:
        1. Fetch new entries from leader WAL
        2. Write to local "disk" (OS buffer simulation)
        3. Fsync (flush to durable storage)
        4. Apply to state machine
        5. Report positions back to leader
        """
        next_lsn = start_lsn

        while self._running:
            # Fetch new entries from leader
            new_entries = self.leader.wal.read_from(next_lsn)

            if not new_entries:
                time.sleep(0.05)  # No new entries — poll again shortly
                continue

            for entry in new_entries:
                if not self._running:
                    break

                # Stage 1: Write to OS buffer (fast, not durable)
                with self._lock:
                    self._write_lsn = entry.lsn
                    self._last_term = entry.term

                self.leader.update_follower_position(
                    self.follower_id, write_lsn=entry.lsn
                )

                # Stage 2: Simulate fsync (durable write)
                # In production: fdatasync() on the WAL file
                time.sleep(self.apply_delay_ms / 1000)   # Simulate disk latency

                with self._lock:
                    self._flush_lsn = entry.lsn

                self.leader.update_follower_position(
                    self.follower_id, flush_lsn=entry.lsn
                )

                # Stage 3: Apply to state machine (idempotent)
                self._apply_entry(entry)

                with self._lock:
                    self._replay_lsn = entry.lsn
                    next_lsn = entry.lsn + 1

                self.leader.update_follower_position(
                    self.follower_id, replay_lsn=entry.lsn
                )

    def _apply_entry(self, entry: WALEntry):
        """
        Apply a WAL entry to the local state machine.
        Idempotent: re-applying an already-seen entry has the same effect.
        """
        with self._lock:
            if entry.type == EntryType.PUT:
                self._state[entry.key] = entry.value
            elif entry.type == EntryType.DELETE:
                self._state.pop(entry.key, None)
            elif entry.type == EntryType.CHECKPOINT:
                pass   # Checkpoint: no state change, just a position marker
            elif entry.type == EntryType.NOOP:
                pass   # Heartbeat: keeps LSN sequence advancing

    def read(self, key: str) -> Optional[str]:
        """Read from the follower's state machine (may be slightly stale)."""
        with self._lock:
            return self._state.get(key)

    @property
    def flush_lsn(self) -> int:
        return self._flush_lsn

    @property
    def replay_lsn(self) -> int:
        return self._replay_lsn

    def get_status(self) -> dict:
        with self._lock:
            return {
                'follower_id': self.follower_id,
                'write_lsn': self._write_lsn,
                'flush_lsn': self._flush_lsn,
                'replay_lsn': self._replay_lsn,
                'state_size': len(self._state),
                'running': self._running,
            }


# ─── Replication System Demo ──────────────────────────────────────────────────────

class ReplicationSystem:
    """
    Wires together WALWriter + ReplicationLeader + ReplicationFollowers
    for end-to-end demonstration.
    """

    def __init__(self, n_followers: int = 2, sync_followers: int = 1):
        self.wal = WALWriter(term=1)
        self.leader = ReplicationLeader(
            wal=self.wal,
            min_sync_followers=sync_followers,
            lag_threshold=5,
        )
        self.followers = {
            f"follower-{i}": ReplicationFollower(
                follower_id=f"follower-{i}",
                leader=self.leader,
                apply_delay_ms=2.0 + i * 1.5,   # Each follower slightly different speed
            )
            for i in range(n_followers)
        }

    def start(self):
        for follower in self.followers.values():
            follower.start()
        time.sleep(0.1)

    def stop(self):
        for follower in self.followers.values():
            follower.stop()

    def write(self, key: str, value: str, sync: bool = True) -> int:
        """Write a key-value pair. If sync=True, wait for quorum commit."""
        entry = self.wal.write_and_flush(EntryType.PUT, key=key, value=value)
        if sync:
            committed = self.leader.wait_for_commit(entry.lsn, timeout_s=3.0)
            if not committed:
                print(f"  WARNING: write at LSN {entry.lsn} not confirmed by quorum")
        return entry.lsn

    def read_from_follower(self, follower_id: str, key: str) -> Optional[str]:
        """Read from a specific follower (may be stale if lagging)."""
        follower = self.followers.get(follower_id)
        if not follower:
            return None
        return follower.read(key)

    def print_cluster_state(self):
        self.leader.print_positions()

    def simulate_follower_crash(self, follower_id: str):
        """Crash a follower and observe lag accumulation."""
        follower = self.followers.get(follower_id)
        if follower:
            follower.crash()

    def simulate_follower_recovery(self, follower_id: str):
        """Recover a crashed follower and observe catch-up replication."""
        follower = self.followers.get(follower_id)
        if follower:
            follower.recover()


# ─── Demo ─────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Leader-Follower Replication Log Demo ===\n")

    system = ReplicationSystem(n_followers=2, sync_followers=1)
    system.start()

    # ── 1. Normal writes ────────────────────────────────────────────────────────
    print("─── Phase 1: Normal Writes ───")
    for i in range(5):
        lsn = system.write(f"key-{i}", f"value-{i}", sync=True)
        print(f"  Written: key-{i}=value-{i} at LSN {lsn}")

    time.sleep(0.2)   # Allow followers to catch up
    system.print_cluster_state()

    # Verify reads from follower
    val = system.read_from_follower("follower-0", "key-3")
    print(f"\n  Follower-0 read key-3: {val}")

    # ── 2. Crash a follower, write more, observe lag ────────────────────────────
    print("\n─── Phase 2: Follower Crash ───")
    system.simulate_follower_crash("follower-0")

    for i in range(5, 10):
        system.write(f"key-{i}", f"value-{i}", sync=False)
    time.sleep(0.1)

    print("\n  Cluster state after follower-0 crash:")
    system.print_cluster_state()

    # ── 3. Follower recovery ─────────────────────────────────────────────────────
    print("\n─── Phase 3: Follower Recovery ───")
    system.simulate_follower_recovery("follower-0")
    time.sleep(0.5)   # Allow follower-0 to catch up

    print("\n  Cluster state after follower-0 recovery:")
    system.print_cluster_state()

    val = system.read_from_follower("follower-0", "key-8")
    print(f"  Follower-0 read key-8 (written during crash): {val}")

    # ── 4. WAL validation: check what's in the log ──────────────────────────────
    print("\n─── Phase 4: WAL Contents ───")
    all_entries = system.wal.read_from(1)
    print(f"  Total WAL entries: {len(all_entries)}")
    for entry in all_entries[:5]:
        print(f"  LSN={entry.lsn} term={entry.term} "
              f"type={entry.type.name} key={entry.key!r} value={entry.value!r}")
    if len(all_entries) > 5:
        print(f"  ... and {len(all_entries) - 5} more entries")

    system.stop()
    print("\n✓ Replication system demo complete")
```

---

## 6. Visual Reference

### WAL Position Pipeline

```
LEADER:
  WAL: [1][2][3][4][5][6][7][8][9][10]
                                   ↑ flush_lsn=10 (current leader position)

FOLLOWER-0 (in sync):
  write_lsn=10  → received and in OS buffer
  flush_lsn=10  → fsynced to disk (durable) ← commit threshold
  replay_lsn=9  → applied to state machine (1 entry behind flush)

FOLLOWER-1 (lagging):
  write_lsn=7   → received and in OS buffer  
  flush_lsn=6   → fsynced to disk
  replay_lsn=6  → applied to state machine

Lag analysis:
  follower-0: flush lag = 0 (in ISR), replay lag = 1
  follower-1: flush lag = 4 (approaching ISR threshold)

If lag_threshold=5 and follower-1's flush_lsn doesn't advance:
  → follower-1 removed from ISR
  → sync writes no longer wait for follower-1
```

### Reconnect Protocol

```
FOLLOWER STATE:         write_lsn=8    flush_lsn=6
FOLLOWER CRASHES:       OS buffer lost
                        flush_lsn=6 is the safe restart position

FOLLOWER RECONNECTS:    sends (last_flush_lsn=6, last_term=1)
LEADER VALIDATES:       entry[LSN=6].term == 1? → YES, log is consistent
LEADER RESPONDS:        "resume from LSN 7"
FOLLOWER APPLIES:       LSN 7, 8, 9, 10 (the entries missed during crash)

WITHOUT TERM CHECK:
  If a new leader was elected (term=2) and follower-0 has entries from term=1
  at positions 7-10 that the new leader doesn't have...
  → term mismatch at follower's last_flush_lsn
  → RESYNC REQUIRED: follower cannot safely resume
  → Must take a base backup from the new leader
```

### ISR Membership Lifecycle

```
T=0:  ISR = [leader, follower-0, follower-1]
            All within lag_threshold=5 of leader

T=10: follower-1 disk slow (GC pause, disk saturation)
      leader_lsn=50, follower-1 flush_lsn=44 → lag=6 > threshold
      ISR = [leader, follower-0]
      sync writes: wait for follower-0 only

T=20: follower-1 catches up: flush_lsn=50 → lag=0
      Re-added to ISR
      ISR = [leader, follower-0, follower-1]

Key insight: ISR uses a LAG THRESHOLD (not binary up/down)
  Binary: follower-1 is "up" even at lag=6 (passes health check)
  Lag threshold: follower-1 is "out of ISR" because it's too far behind
  This is the key difference between binary health checks and ISR
```

---

## 7. Common Mistakes

**Mistake 1: Confusing write_lsn and flush_lsn as the safe commit position.** The follower's `write_lsn` is the highest entry written to the OS buffer — but if the follower crashes at this point, the OS buffer is lost. Only `flush_lsn` (entries confirmed fsynced to disk) is the safe commit position for synchronous replication. For a write to be considered durably committed on a follower, the leader must receive a `flush_lsn >= write.lsn` from that follower. Waiting for `write_lsn` gives a false sense of durability — the follower's OS buffer can be lost in a crash.

**Mistake 2: Checking ISR membership with a binary health check instead of lag threshold.** A follower that is alive and responding to health checks but 10,000 entries behind the leader is not a useful synchronous replica. If a synchronous write requires this follower's ACK, it will wait for a follower that is minutes behind — unacceptable latency or a timeout. The ISR must track lag (entries or time behind the leader), not just connectivity. The `lag_threshold` in the implementation exists for this reason.

**Mistake 3: Not validating term on reconnect.** When a follower reconnects after being offline, it is possible that a new leader was elected with a higher term during its absence. If the follower's last `flush_lsn` position exists in the new leader's log but with a different term, the logs have diverged — the follower's entry at that position came from a different (now-invalid) leader. Without term validation, the follower would resume applying entries from a divergent history. Always validate term on reconnect; require resync if terms don't match.

**Mistake 4: Re-applying already-applied entries without idempotency.** When a follower recovers and resumes from its `flush_lsn`, it may re-receive entries it already applied (if replay_lsn > flush_lsn at the time of crash — the entry was applied but not re-reported). Without idempotent apply logic, applying the same `PUT key=X value=Y` twice is harmless. But applying the same `DELETE key=X` twice, or the same `INCREMENT counter` twice, would produce incorrect state. Design all WAL entry types to be idempotent from the start.

---

## 8. Production Failure Scenarios

### Scenario 1: Follower Falls Behind During a Large Bulk Load

**Symptoms:** A bulk data load job writes 500,000 rows to the leader in 45 seconds. The Postgres standby, which was in sync before the load, is now 4GB of WAL behind the primary. Monitoring shows `pg_stat_replication.replay_lag = 280s`. New queries on the hot standby return stale data — rows inserted during the bulk load are not yet visible.

**Root cause:** The follower's disk I/O rate (WAL replay throughput) is lower than the leader's write rate. During the bulk load, the leader wrote WAL at 90MB/second. The standby can replay WAL at 40MB/second. The lag accumulated at 50MB/second during the load. After the load ends, the standby gradually catches up at its maximum replay rate.

**Key observation from the implementation:** `flush_lsn` advances but `replay_lsn` falls behind. The follower is durably saving the WAL (flush_lsn is close to the leader) but can't apply entries to its state machine fast enough. In Postgres terms: the WAL receiver process is keeping up, but the startup process (WAL apply) is lagging.

**Fix:** For bulk loads, temporarily increase the standby's `recovery_min_apply_delay = 0` (if it was set for lag protection) and ensure the standby has adequate I/O bandwidth. For persistent lag issues: upgrade the standby's disk to NVMe, or apply WAL in parallel (`recovery_parallelism` in Postgres 14+).

### Scenario 2: ISR Collapse Triggered by Noisy Neighbor VM

**Symptoms:** A Kafka cluster with RF=3 and min.insync.replicas=2 experiences repeated `NOT_ENOUGH_REPLICAS` producer errors every Tuesday morning between 09:00-09:45. The pattern repeats weekly. The affected broker is always broker-2. During these windows, broker-2's follower replicas fall out of ISR.

**Root cause:** Every Tuesday at 09:00, a backup job runs on the same physical host as broker-2's VM (a noisy neighbor). The backup job saturates the shared NVMe storage. Broker-2's disk I/O for WAL writes (fsync latency) spikes to 800ms. The follower replicas that broker-2 is responsible for are partitions where broker-2 is the leader — they can't replicate fast enough because broker-2's own disk is saturated. The followers' fetch requests to broker-2 time out; they fall behind `replica.lag.time.max.ms=30s` and are removed from ISR.

**Key observation from the implementation:** This is the exact ISR lag threshold mechanism in action. The follower's fetch_lsn falls more than `lag_threshold` entries behind the leader's current LSN because the leader itself is slow (saturated disk), making it unable to process follower fetch requests fast enough.

**Fix:** Move broker-2 to a dedicated host (no VM neighbors) or move the backup job to off-peak hours. Long-term: use dedicated storage per broker rather than shared cloud block storage.

---

## 9. Performance and Tuning

### WAL Write Throughput

The leader's WAL throughput is bounded by disk fsync latency:

```
max_throughput = 1 / fsync_latency_per_write

NVMe SSD (P99 fsync = 2ms): ~500 writes/second
SATA SSD (P99 fsync = 10ms): ~100 writes/second
HDD (P99 fsync = 20ms):     ~50 writes/second

With WAL batching (group commit):
  Multiple writes buffered and flushed together in one fsync
  max_throughput = batch_size / fsync_latency
  NVMe + 50-write batches: 50 × 500 = 25,000 writes/second
```

Postgres `wal_writer_delay` (default 200ms) and `commit_delay` (default 0) control group commit behavior. Setting `commit_delay=100` (microseconds) gives Postgres 100μs to accumulate more commits before fsyncing, improving throughput under load.

### Follower Apply Throughput

```python
# In our implementation: apply_delay_ms per entry
# For a follower at 2ms per entry:
#   Max throughput = 1000ms/2ms = 500 entries/second
#
# If the leader produces at 600 entries/second:
#   Lag accumulates at 100 entries/second
#   After 60 seconds: 6,000 entries of lag
#   ISR threshold (lag_threshold=5) exceeded after 0.05 seconds!
#
# Tuning to prevent lag:
#   1. Reduce per-entry apply cost (batch writes, async apply)
#   2. Increase ISR threshold to accommodate burst lag
#   3. Apply in parallel (Postgres parallel recovery in 14+)
#   4. Upgrade follower disk to reduce fsync latency
```

---

## 10. Interview Q&A

**Q1: What is a WAL and why does it exist?**

A Write-Ahead Log is an append-only, sequentially-written log of every change made to a database or data store. It exists for two reasons. First, crash recovery: if the process crashes before changes are applied to the actual data files (the B-tree pages, the heap tuples), the WAL can be replayed on restart to bring the data files back to a consistent state. The WAL is written sequentially and fsynced to disk before the transaction is acknowledged — this is the "write-ahead" guarantee: the log is written ahead of the actual data files.

Second, replication: the same WAL that enables crash recovery also enables followers to replicate the leader's state changes by streaming the WAL and applying it locally. This is why Postgres physical replication is based on shipping the WAL — the WAL already contains every byte-level change, and followers simply replay it.

The key property that makes WALs fast is that they are append-only sequential writes. Sequential writes to a spinning disk or SSD are dramatically faster than random writes (the same data without a WAL would require updating pages in-place at random positions). This is also why Kafka's log-structured storage achieves high throughput — it's the same sequential-write principle.

**Q2: What is the difference between flush_lsn and replay_lsn in Postgres replication? Why do they exist separately?**

`flush_lsn` is the highest WAL position that the standby has confirmed fsynced to its own disk — the entry is durable on the standby. `replay_lsn` is the highest WAL position that the standby has applied to its data files — the change is visible to queries on the standby (hot standby reads).

They are tracked separately because flushing WAL to disk and applying it to data files are two separate pipeline stages with different latency characteristics and different failure semantics. Flushing is fast (just a WAL append + fsync) and is the operation that matters for durability — a primary configured with `synchronous_commit=on` waits for `flush_lsn` on the standby before returning success to the client. Applying is slower (reading WAL records, updating heap pages, updating indexes) and is the operation that matters for read consistency on the standby — a user reading from the hot standby sees data only up to `replay_lsn`.

A standby can be well ahead on `flush_lsn` (durable WAL, good for failover) while behind on `replay_lsn` (slow apply, stale reads). For failover safety, what matters is `flush_lsn`. For read freshness on the standby, what matters is `replay_lsn`. Having both metrics separately lets operators make targeted decisions.

---

## 11. Cross-Question Chain

**Interviewer:** You're running a Postgres primary with one synchronous standby. An application write takes 15ms instead of the expected 3ms. Where do you look first?

**Candidate:** The 15ms write time with a synchronous standby means the primary is waiting for the standby to confirm `flush_lsn >= commit_lsn`. The extra 12ms is most likely standby fsync latency — the standby's disk is taking 12ms to fsync the WAL entry instead of the expected 1ms.

I'd start with `pg_stat_replication.flush_lag` on the primary — this column shows the time between the primary flushing a WAL record and the standby confirming it was flushed. If it shows 12ms, the standby disk is the bottleneck. I'd then run `iostat -x 1` on the standby to check `await` (disk latency) — if it shows high latency, the standby's storage is saturated or slow.

Secondary check: is the latency consistent or occasional? Occasional spikes (P99 issue, not P50) suggest GC pauses on the standby (if it's JVM-based) or disk scheduling contention. Consistent high latency suggests under-provisioned storage.

**Interviewer:** The standby disk shows 2ms fsync latency in iostat, but flush_lag in pg_stat_replication is still 12ms. What else could cause this?

**Candidate:** The discrepancy means something between the primary flushing and the standby fsync is taking the extra 10ms. Three candidates. First: network RTT. The primary writes the WAL, sends it over the network to the standby's WAL receiver, and waits for the standby to write and fsync before responding. If the network RTT between primary and standby is 10ms (cross-AZ), that adds directly to flush_lag. I'd check `ping` between the two hosts and `pg_stat_replication.write_lag` vs `flush_lag` — if `write_lag` is also 10ms, the network is the bottleneck.

Second: WAL sender on the primary has a `wal_sender_timeout` but also a `wal_sender_delay` (internal to the WAL sender process). If the WAL sender is batching entries, it might wait before sending, adding latency.

Third: the standby WAL receiver is writing to OS buffer and then fsyncing — if the WAL file's directory has synchronous I/O issues (shared NFS mount? disk controller queue depth?), iostat might show 2ms for individual I/O operations but the total queue time adds up to 10ms at the application level. Check `iowait` percentage on the standby, and look at `queue depth` in iostat.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is a WAL and why does it exist? | An append-only sequential log of every change to a data store. Exists for (1) crash recovery: replay WAL on restart to restore consistency; (2) replication: followers stream WAL from the leader and replay it locally. Sequential writes make WAL fast. |
| 2 | What is an LSN? | Log Sequence Number: a monotonically increasing 64-bit integer identifying an entry's position in the WAL. Used by followers to track their replication progress and by the leader to know where to resume streaming after a reconnect. |
| 3 | What is the difference between flush_lsn and replay_lsn? | `flush_lsn`: highest WAL position fsynced to disk on the follower (durable). `replay_lsn`: highest WAL position applied to the data files (visible to reads). Sync replication waits for flush_lsn. Hot-standby read freshness depends on replay_lsn. |
| 4 | What is the ISR lag threshold and why is it better than a binary health check? | ISR removes a follower when its flush_lsn falls more than lag_threshold entries behind the leader. Binary health check only detects "is the process running?" A follower can be running but 10 minutes behind — binary check won't catch it, lag threshold will. |
| 5 | Why must the reconnect protocol validate the term of the follower's last flush_lsn? | If a new leader was elected with a higher term during the follower's absence, entries at the same LSN position may have different content (different leader's writes). Term validation detects this divergence. Without it, the follower would apply entries from a stale/invalid history. |
| 6 | Why are WAL entries designed to be idempotent? | On recovery, a follower may re-apply entries already in its state machine (if replay_lsn > flush_lsn at crash time). Idempotent entries (PUT, DELETE, SET) produce the same result whether applied once or twice. Non-idempotent operations (INCREMENT, APPEND) require special handling. |
| 7 | What is group commit in WAL systems? | Batching multiple write transactions so a single fsync covers all of them. Instead of fsync per commit, the WAL writer accumulates N commits and then fsyncs once. Trades per-transaction durability confirmation delay for higher throughput. Postgres uses `wal_writer_delay` and `commit_delay` for this. |
| 8 | What does "WAL segment rotation" mean in Postgres? | Postgres WAL is stored in fixed-size (16MB default) segment files. When a segment is full, a new one is created (rotation). Old segments can be deleted once no follower needs them (all followers' restart_lsn > segment's last entry). In Kafka's equivalent: log segment rolling (roll.ms, segment.bytes). |
| 9 | What is WAL archival and why is it needed? | Copying WAL segments to durable external storage (S3, NFS) so that a follower that falls too far behind can recover from the archive rather than requiring a full base backup. If the leader deletes old WAL segments and a follower is far behind, without archival the follower can never catch up. |
| 10 | Why does the follower track write_lsn separately from flush_lsn? | `write_lsn`: entry is in the OS page cache — fast to write, not crash-safe. `flush_lsn`: entry has been fsynced — crash-safe. The distinction matters for durability: synchronous commit should wait for `flush_lsn`, not `write_lsn`. Postgres's `synchronous_commit=remote_write` uses `write_lsn` (faster but slightly less safe). |

---

## 13. Further Reading

- **Postgres documentation — "High Availability, Load Balancing, and Replication":** The source of truth for Postgres WAL streaming replication. Sections on replication positions (sent/write/flush/replay LSN) and the WAL sender/receiver protocol match the implementation in this module exactly.
- **"Designing Data-Intensive Applications" by Martin Kleppmann, Chapter 5:** The section on "Implementation of Replication Logs" covers statement-based, WAL-based, and logical replication. The discussion of WAL positions and their semantics is the theoretical foundation for this implementation.
- **"Database Internals" by Alex Petrov, Chapter 11 (Replication and Consistency):** More technically rigorous than DDIA. Covers WAL structure, replication log formats, and follower recovery protocols at byte-level detail.
- **Postgres source: `src/backend/replication/walsender.c`:** The actual WAL sender implementation in Postgres. Reading the `WalSndLoop` function shows exactly how the WAL sender tracks positions and handles reconnects.

---

## 14. Lab Exercises

**Exercise 1: Run the Demo and Observe Lag**
Run `replication_log.py`. Observe phase 2 (follower crash) — the cluster state shows follower-0's flush_lsn stopped advancing. Observe phase 3 (recovery) — follower-0 reconnects, requests entries from its last flush_lsn, and catches up. Modify `apply_delay_ms` for follower-1 to 50ms and observe it consistently lagging in the cluster state output.

**Exercise 2: Trigger ISR Removal**
Reduce `lag_threshold` to 2 in the `ReplicationSystem`. Run with `apply_delay_ms=20ms` for one follower. Observe that follower quickly falls out of ISR. Write 10 more entries with `sync=True` — they should complete quickly (not waiting for the out-of-ISR follower). Verify by checking `leader.get_isr()`.

**Exercise 3: Simulate Log Divergence**
Add a method to `WALWriter` to change the term mid-stream. Register a follower, write 5 entries (term=1), increment the term to 2, write 5 more entries, then crash and recover the follower with `start_lsn=7, last_term=1`. Observe the term mismatch at LSN 7 triggering a resync requirement.

**Exercise 4: Implement WAL Checksum Validation**
Modify `WALEntry.deserialize()` to corrupt one byte of a WAL entry. Observe the CRC check failing. This demonstrates how WAL implementations detect disk corruption — every major WAL implementation includes CRC per entry.

---

## 15. Key Takeaways

A replication log is an append-only, sequentially-written data structure indexed by LSN. Its properties — durability (WAL fsync before ACK), idempotency (safe to re-apply), and positional resumption (reconnect from last flush_lsn) — are what make leader-follower replication correct in the face of crashes, network partitions, and follower restarts.

The four-position tracking (sent, write, flush, replay) is not bureaucratic complexity — it reflects real pipeline stages with different latency and failure semantics. `flush_lsn` is the durability threshold; `replay_lsn` is the read-consistency threshold. Production monitoring must track both.

ISR membership based on lag threshold (not binary up/down) is the key mechanism that prevents a slow follower from holding up synchronous writes. The lag threshold is the tuning knob that balances durability (keep slow follower in ISR — more durable but slower) vs availability (remove slow follower from ISR — faster but fewer durability copies).

Term validation on reconnect is the mechanism that prevents a follower from applying entries from a diverged (stale-leader) history. Without it, any reconnecting follower could corrupt its state by applying entries from the wrong leader.

---

## 16. Connections to Other Modules

- **M39 — Replication:** This module implements the leader-follower replication concepts from M39. The four-position tracking maps directly to `pg_stat_replication`. The ISR mechanism maps to Kafka's ISR protocol.
- **M38 — Consensus Algorithms:** The term validation on follower reconnect is the practical application of Raft's term-based step-down mechanism. In Raft, log entries are tagged with their leader's term — this is exactly the term field in `WALEntry`.
- **M40 — Failure Modes:** The follower crash simulation exercises the partial failure and recovery mechanisms. The ISR threshold implements gray failure detection — a lagging follower is a form of gray failure that binary health checks miss.
- **M43 — Raft Follower:** M43 builds directly on this module's `ReplicationFollower`, adding the Raft-specific state machine (FOLLOWER/CANDIDATE/LEADER transitions, election timeout, vote handling).

---

## 17. Anti-Patterns

**Anti-pattern: Treating write_lsn as the commit threshold.** In `synchronous_commit=remote_write` mode, Postgres waits only for `write_lsn` (OS buffer write), not `flush_lsn` (disk fsync). This is faster but means the standby can acknowledge a write that is not yet crash-safe. If the standby's OS crashes before flushing its page cache, that data is lost even though the primary returned success. Only use `remote_write` for data where brief RPO > 0 is acceptable.

**Anti-pattern: Not persisting `flush_lsn` before applying entries to the state machine.** An implementation that updates the state machine and then fsyncs can lose the state machine update if it crashes between the two. The correct order is: (1) fsync the WAL entry, (2) update `flush_lsn`, (3) apply to state machine, (4) update `replay_lsn`. Reversing steps 2 and 3 means the state machine has applied entries that were lost in a crash — the state machine is ahead of the durable WAL position, and re-applying them on recovery produces duplicates.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pg_stat_replication` | Monitor Postgres replication positions | sent/write/flush/replay LSN per standby |
| `pg_last_wal_replay_lsn()` | Standby's current replay position | Check lag from within standby |
| `pg_current_wal_lsn()` | Leader's current WAL position | Compare to follower's replay_lsn for lag |
| `pg_walfile_name(lsn)` | WAL segment containing an LSN | Find which file stores a given log position |
| `pg_switch_wal()` | Force WAL segment rotation | Testing WAL archival and segment cleanup |
| `replication_log.py` | Full leader-follower replication simulation | WALWriter, ReplicationLeader, ReplicationFollower |
| `iostat -x 1` | Disk I/O latency (await column) | Diagnose follower replication lag from disk saturation |

---

## 19. Glossary

**CRC (Cyclic Redundancy Check):** A checksum appended to each WAL entry to detect corruption. If the stored CRC doesn't match the computed CRC on read, the entry is corrupted — the system should stop and alert rather than applying corrupted data.

**Flush LSN:** The highest WAL position confirmed fsynced to the follower's disk. Entries at or below flush_lsn will survive a follower crash. The commit threshold for synchronous replication.

**Group commit:** Batching multiple transactions' WAL writes so a single fsync covers all of them. Improves write throughput at the cost of slightly increased per-transaction latency.

**ISR (In-Sync Replica):** The set of followers whose flush_lsn is within `lag_threshold` entries of the leader's current LSN. Synchronous replication waits only for ISR members to ACK.

**LSN (Log Sequence Number):** A monotonically increasing integer identifying a WAL entry's position. Used by followers to track their replication progress and by the leader to determine where to resume streaming.

**Replay LSN:** The highest WAL position applied to the follower's state machine. Determines what data is visible on the follower (hot-standby read freshness).

**Term:** An epoch number identifying which leader wrote a WAL entry. Used during follower reconnect to detect log divergence — if the follower's last flush_lsn position has a different term than the leader's log at that position, the logs have diverged.

**WAL (Write-Ahead Log):** An append-only sequential log of every change to a data store. Written to disk before the change is applied to data files. Enables both crash recovery and replication.

**WAL segment:** A fixed-size file (16MB in Postgres) containing WAL entries. Segments are rotated when full. Old segments can be deleted once no follower needs them.

**Write LSN:** The highest WAL position written to the follower's OS buffer. Faster to update than flush_lsn (no fsync required) but not crash-safe — OS buffer is lost on crash.

---

## 20. Self-Assessment

1. What is the correct ordering of the four WAL positions? Which is always ≤ which?
2. A follower has flush_lsn=100 and replay_lsn=95. It crashes. When it recovers, what LSN does it send to the leader? What happens to entries 96-100?
3. Why would a follower with flush_lsn=100 be removed from ISR if the leader is at lsn=110 and lag_threshold=5?
4. What is the purpose of the term field in a WAL entry? Give a scenario where term validation prevents a correctness bug.
5. Run `replication_log.py` and modify it so follower-1 has `apply_delay_ms=100`. Observe when it falls out of ISR. What would happen if a synchronous write required follower-1's ACK while it's out of ISR?
6. Why is sequential WAL writing faster than random in-place writes? What hardware property makes this true?
7. Describe the two-phase nature of a follower's apply loop: why is "fsync first, apply second" the correct order?
8. In Postgres, `synchronous_commit=remote_write` waits for `write_lsn` instead of `flush_lsn`. What is the failure scenario this creates?

---

## 21. Module Summary

This module implemented a complete leader-follower replication log in Python — not as a simplified toy that avoids the hard parts, but as a system that makes the correct engineering decisions: WAL entries with CRC checksums, four-position tracking (sent/write/flush/replay), ISR membership based on lag threshold, term-validated reconnect protocol, and idempotent entry application.

Every design choice maps directly to a production system. The four-position tracking is `pg_stat_replication` made explicit. The ISR lag threshold is Kafka's `replica.lag.time.max.ms` made concrete. The term validation on reconnect is the mechanism behind Raft's epoch-based step-down, applied to WAL replication. The idempotent apply is why Postgres's WAL operations are safe to re-apply on recovery.

Building this implementation — rather than just reading about it — makes the configuration knobs of production systems understandable from first principles. When `flush_lag` shows 50ms in `pg_stat_replication`, you now know exactly which pipeline stage is slow, why it matters for commit latency, and where to look for the root cause. When a Kafka follower falls out of ISR, you now understand the lag threshold mechanism that triggers it and what it means for producer durability.

The next module — M43: Implementing a Raft Follower — extends this foundation by implementing the consensus-layer state machine on top of the replication log: leader election, vote handling, and the election restriction that makes Raft safe.
