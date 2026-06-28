# M39: Replication

**Course:** SYS-DST-101 — Distributed Systems Theory  
**Module:** 03 of 05  
**Global Module ID:** M39  
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

Replication — keeping copies of data on multiple nodes — is the single most important mechanism in distributed data systems. Everything else in distributed systems theory either explains the guarantees replication provides, the problems it creates, or the tradeoffs it forces. You cannot reason about Kafka, Postgres, Cassandra, Flink checkpointing, or any other data system without understanding what replication strategy they use and what promises that strategy makes.

The motivation for replication is threefold. First, fault tolerance: if one node fails, another copy of the data keeps the system alive. Second, read scalability: multiple replicas can serve reads in parallel, distributing query load. Third, latency: placing replicas near users reduces the distance data travels, reducing round-trip time for reads.

But replication immediately creates a consistency problem: when you write to one replica, how do you ensure all other replicas see the same data? And what do you do when they can't all be updated simultaneously? This is the replication lag problem, and every replication strategy in existence is a different answer to it — with different consistency guarantees, different latency costs, and different failure behaviors.

M38 explained consensus as the mechanism behind strongly-consistent replication (Raft). This module surveys the entire replication landscape: synchronous vs asynchronous, leader-follower (single-leader), multi-leader, and leaderless. Each appears in production data systems. Knowing which strategy a system uses tells you immediately what failures it can tolerate, what consistency anomalies you might observe, and how to configure it for your specific RPO/RTO requirements.

---

## 2. Mental Model

### The Core Tension

Replication lives at the intersection of two opposing forces. Consistency requires that all replicas agree on the current value before a write is acknowledged — which requires waiting for all replicas to respond, costing latency. Availability and performance want writes to complete as fast as possible — which means not waiting for replicas, accepting that they may diverge temporarily.

Every replication strategy sits somewhere on this spectrum. Synchronous replication is at the consistency extreme: the writer waits for all (or a quorum of) replicas to confirm before returning success. Asynchronous replication is at the performance extreme: the writer returns immediately and replicas catch up later. Semi-synchronous splits the difference: wait for one replica, replicate asynchronously to the rest.

### The Replication Taxonomy

```
Replication
├── By leader architecture
│   ├── Single-leader (leader-follower)
│   │   └── One node accepts writes; others replicate
│   ├── Multi-leader
│   │   └── Multiple nodes accept writes; replicate to each other
│   └── Leaderless (peer-to-peer)
│       └── Any node accepts writes; quorum required
└── By synchrony
    ├── Synchronous
    │   └── Writer waits for replica acknowledgment
    ├── Asynchronous
    │   └── Writer does not wait; replica catches up independently
    └── Semi-synchronous
        └── Writer waits for one replica; rest are async
```

---

## 3. Core Concepts

### 3.1 Single-Leader Replication (Leader-Follower)

Single-leader replication designates one node as the leader (primary, master). All writes go to the leader. The leader records the write in its replication log and forwards changes to follower nodes (replicas, secondaries, standbys).

**How the replication log works:**
The leader maintains a log of every change — called the replication log, write-ahead log (WAL), or binlog depending on the system. Followers replay this log to stay current. The log captures the intent of each write:
- **Statement-based replication:** Log the SQL statement itself (`INSERT INTO events VALUES (...)`). Problem: non-deterministic functions like `NOW()` produce different values on leader and follower.
- **Row-based replication:** Log the actual before/after row values. Deterministic, handles all cases. Used by Postgres (logical replication) and MySQL (binlog in ROW format).
- **WAL shipping:** Postgres physical replication ships the raw WAL bytes — the exact pages changed. Very fast but tightly coupled to storage format. Cannot be used across major Postgres versions.

**Reads:** In a basic single-leader setup, reads can go to any replica. This gives read scalability — one leader handles writes, N followers handle reads. But reads from followers may return stale data if the replication lag is non-zero. This is the classic **read-your-writes** problem: a user writes to the leader and immediately reads from a follower — the follower hasn't caught up yet, so the user doesn't see their own write.

**Failover:** When the leader fails, a follower must be promoted. This is the most dangerous operation in single-leader replication:
- The promoted follower must have caught up to the leader's last committed state, or writes are lost.
- The old leader must not rejoin as leader (split-brain). It must recognize it is no longer the leader before accepting writes.
- If the old leader had partially-applied writes that the new leader doesn't know about, those writes are lost (unless the failover system explicitly handles this).

**Where you see it:** Postgres (primary + standbys), MySQL (source + replicas), Kafka partitions (partition leader + followers), Redis Sentinel/Cluster (primary + replicas).

### 3.2 Synchronous vs Asynchronous Replication

**Synchronous replication:**
The leader waits for a quorum of followers to acknowledge the write before returning success to the client. If a follower is unavailable, the write blocks until it comes back (or a timeout triggers a reconfiguration).

```
Client → Leader: WRITE x=1
Leader → Follower-1: replicate x=1
Follower-1 → Leader: ACK
Leader → Client: SUCCESS   ← client only sees success after Follower-1 ACKs
```

**Guarantees:**
- RPO = 0 on failover to any acknowledged follower (no data loss)
- Linearizability possible (reads from leader see all committed writes)
- **Durability:** the write is on at least 2 nodes before the client sees success

**Costs:**
- Write latency = leader fsync + network RTT to follower + follower fsync
- If the synchronous follower is slow or unavailable, writes stall

**Asynchronous replication:**
The leader writes locally, sends changes to followers asynchronously, and returns success to the client immediately.

```
Client → Leader: WRITE x=1
Leader → Client: SUCCESS   ← returns immediately
Leader → Follower-1: replicate x=1 (in background)
```

**Guarantees:**
- Low write latency (no waiting for followers)
- **No durability guarantee beyond the leader:** if the leader crashes after returning success but before the follower receives the change, the write is lost

**Costs:**
- RPO > 0: data loss on leader failure = lag at time of crash
- Replication lag accumulates under write pressure; followers lag behind

**Semi-synchronous (Kafka acks=all, Postgres synchronous_standby_names):**
Postgres `synchronous_commit = on` with `synchronous_standby_names = 'standby1'`: waits for one nominated synchronous standby to ACK before returning success. Other standbys are async. This gives durability (2 copies) without requiring all replicas to be fast.

Kafka `acks=all` with `min.insync.replicas=2` in a 3-replica topic: waits for the leader + at least one follower to ACK. This is synchronous to a quorum of ISR members, asynchronous to the third replica if it's behind.

### 3.3 Replication Lag and Its Consistency Anomalies

**Replication lag** is the delay between a write being committed on the leader and being visible on a follower. Under normal conditions in a well-provisioned system, this is milliseconds. Under write pressure, follower restart, or network congestion, lag can grow to seconds, minutes, or hours.

Lag causes observable consistency anomalies for applications reading from followers:

**Read-your-own-writes (read-your-writes consistency):** After writing to the leader, reading from a lagged follower returns the old value. The user observes their write has been "lost."

Solution: For a user's own writes, always route reads to the leader for a bounded window after a write. Or track the replication position of each user's last write and only serve reads from followers that have caught up to that position.

**Monotonic reads:** A user reads value V from follower-A (lag = 1s), then reads from follower-B (lag = 10s). They observe a "time reversal" — value V disappears in the second read. They see the world going backward.

Solution: Always route a user's reads to the same replica (session affinity). If that replica fails, be prepared to re-read the latest value from the leader.

**Consistent prefix reads:** Reads observe writes in a different order from how they were written. In a sharded system, if shard A replicates faster than shard B, a read across both shards may see A's writes but not B's — observing a causally inconsistent state.

Solution: Multi-shard reads go through a coordinator that tracks write timestamps. Or use a system with global commit timestamps (Spanner, CockroachDB).

### 3.4 Postgres Replication Deep Dive

Postgres supports three replication modes relevant to data engineers:

**Physical streaming replication (WAL shipping):**
The standby continuously receives the WAL byte stream from the primary. It replays changes at the storage level — byte-for-byte identical to the primary. Supports synchronous and asynchronous modes. Lag is measured as WAL bytes behind the primary. Used for HA failover (Patroni, Stolon).

```
-- On primary:
-- pg_stat_replication shows connected standbys and their lag
SELECT application_name, state, sent_lsn, write_lsn, flush_lsn, replay_lsn,
       (sent_lsn - replay_lsn) AS replication_lag_bytes
FROM pg_stat_replication;

-- On standby:
-- pg_stat_wal_receiver shows connection to primary and lag
SELECT status, receive_start_lsn, received_lsn, last_msg_send_time,
       last_msg_receipt_time, latest_end_lsn
FROM pg_stat_wal_receiver;
```

**Logical replication:**
Replicates changes at the row level, not the WAL byte level. The publisher decodes WAL into a logical change stream (INSERT/UPDATE/DELETE of rows). The subscriber applies these changes. Allows partial replication (specific tables), cross-version replication, and heterogeneous replication (Postgres → Kafka, Postgres → Postgres with different schema).

Logical replication powers: Debezium CDC (logical decoding), logical standbys in different Postgres versions, and read replicas that need only a subset of tables.

**Synchronous commit levels in Postgres:**
Postgres offers five levels of synchronous commit, balancing durability and latency:
- `off`: leader doesn't wait for WAL fsync — fastest, but a crash can lose the last ~600ms of commits
- `local`: leader waits for its own WAL fsync — default for single-node
- `remote_write`: leader waits for standby to write to its OS buffer (not fsync) — fast, 1 RTT
- `on` (default with sync replication): leader waits for standby to fsync WAL — safest async is still preserved
- `remote_apply`: leader waits for standby to apply the change to its data files — ensures hot standby reads are always fresh

### 3.5 Kafka Partition Replication Deep Dive

Each Kafka partition has one leader and N-1 followers. The leader handles all produce and consume operations. Followers replicate by consuming from the leader (they behave like consumers of an internal replication topic).

**ISR (In-Sync Replicas):** A follower is in the ISR if it is within `replica.lag.time.max.ms` (default 30s) of the leader's log end offset. If a follower falls behind (slow disk, GC pause, network issue), it is removed from the ISR. If it catches up, it is re-added.

**acks=all behavior:**
When a producer sends with `acks=all`, the leader only responds success when all members of the current ISR have acknowledged the message. "All ISR" means the follower count can shrink dynamically — if one replica falls behind and is removed from ISR, subsequent `acks=all` writes only need the remaining ISR members.

**min.insync.replicas (min.isr):**
Sets a floor on ISR size. With `min.isr=2` and `acks=all`, the leader refuses to acknowledge writes if the ISR has shrunk to fewer than 2 members. This prevents the "ISR of 1" scenario where `acks=all` is effectively `acks=1`:

```
Topic: events, RF=3 (1 leader + 2 followers)
Normal: ISR=[leader, follower-1, follower-2]

follower-2 crashes:
  ISR=[leader, follower-1]
  acks=all: writes succeed (ISR has 2 members, min.isr=2 satisfied)

follower-1 also crashes:
  ISR=[leader]
  acks=all + min.isr=2: writes FAIL with NOT_ENOUGH_REPLICAS
  acks=all + min.isr=1 (default): writes succeed (effectively acks=1 — dangerous)
```

**Unclean leader election:**
If all ISR members fail and only an out-of-sync replica is available, Kafka can either wait for an ISR member to recover (`unclean.leader.election.enable=false`, the safe default) or elect the out-of-sync replica as leader (`unclean.leader.election.enable=true`). Enabling unclean election restores availability but risks data loss — the out-of-sync replica may be missing the most recent committed messages.

### 3.6 Multi-Leader Replication

Multi-leader replication allows writes to be accepted at multiple nodes simultaneously. Each leader replicates its writes to all other leaders. This enables writing in multiple data centers simultaneously (geo-distributed write performance) or offline-capable applications (mobile apps that sync when online).

**The fundamental problem: write conflicts.** If two clients simultaneously write different values to the same key — one going to leader-A, the other to leader-B — the system must decide which value wins. This is the conflict resolution problem.

**Conflict resolution strategies:**

**Last Write Wins (LWW):** Use a timestamp (wall clock or logical clock). The write with the highest timestamp wins. Problem: clock skew between nodes can cause the "wrong" write to win. A write that arrived later physically (but with an earlier clock) is silently discarded. LWW is Cassandra's default conflict resolution. It works for data where writes are independent and approximate timestamps are acceptable, but it silently discards concurrent writes to the same key.

**Merge / CRDT (Conflict-free Replicated Data Type):** Design the data structure so that concurrent writes can always be merged without conflict. A counter (increment-only) can merge by taking the max of each node's counter. A set (add-only) can merge by taking the union. CRDTs are the correct model for collaborative data structures (shopping carts, counters, distributed document editing). CouchDB uses CRDTs for its multi-master replication.

**Application-level conflict handling:** Surface conflicts to the application layer. The application sees both conflicting values and resolves them using domain logic. CouchDB's revision conflict mechanism works this way.

**Causal consistency / version vectors:** Track causal dependencies between writes using version vectors. A version vector is a vector of (node, sequence_number) pairs. A write is in conflict only if neither version's vector is strictly greater than the other. If A's vector is [node1:5, node2:3] and B's is [node1:4, node2:6], neither supersedes the other — true conflict. If A's vector is [node1:5, node2:3] and B's is [node1:5, node2:4], B happened after A — no conflict, B wins.

**Where multi-leader appears in data engineering:**
- CouchDB / PouchDB: multi-master replication for offline-capable apps
- DynamoDB: global tables (multi-region active-active) with last-write-wins
- Cassandra: multi-datacenter replication with tunable consistency
- Conflict Handling in CDC: Debezium in multi-source mode requires application-level deduplication

### 3.7 Leaderless Replication (Dynamo-style)

Leaderless replication has no designated leader. Any node can accept reads and writes. Consistency is achieved through quorum reads and writes: a write must succeed on W of N nodes; a read must succeed on R of N nodes.

**Quorum consistency condition:** For reads to always see the most recent write, the quorum overlap condition must hold: W + R > N. This ensures that at least one node in the read quorum has seen the latest write.

```
N=3 replicas
W=2 (write quorum)
R=2 (read quorum)
W + R = 4 > N = 3  ✓ — reads always intersect with writes

W=1 (write quorum)
R=1 (read quorum)
W + R = 2 = N = 2  ✗ — may miss the latest write (reads may miss writes)
```

Cassandra's consistency levels implement this:
- `ONE` = quorum of 1 (W=1 or R=1)
- `QUORUM` = majority quorum (W = R = ⌊N/2⌋ + 1)
- `ALL` = full N (W = R = N)
- `LOCAL_QUORUM` = quorum within the local DC only

**Read repair:** When a read quorum returns different versions from different nodes, the coordinator returns the most recent version to the client and repairs the stale node in the background (or synchronously, depending on `read_repair_chance`).

**Anti-entropy / Hinted handoff:** Cassandra uses anti-entropy (continuous background comparison and repair between nodes) to ensure replicas converge. Hinted handoff stores writes for temporarily-unavailable nodes and delivers them when the node recovers.

**Sloppy quorum:** When fewer than W or R nodes in the preferred replica set are available, Cassandra can write to non-preferred nodes (nodes that don't normally own this data range) to meet the quorum count. This is `QUORUM` achieved with different physical nodes. The write is delivered to the correct node via hinted handoff on recovery. Sloppy quorum improves availability at the cost of temporary inconsistency.

**Version vectors in Cassandra:** Cassandra uses timestamps (not vector clocks) as version identifiers. Each cell has a timestamp provided by the client. LWW conflict resolution uses these timestamps. If two clients write to the same row within the same millisecond, the one with the higher timestamp wins — there is no structural way to detect this conflict; one write is silently dropped.

### 3.8 The Read-Write Quorum Tradeoff

The W + R > N condition gives you strong consistency, but the specific values of W and R affect availability and performance:

- **W=N, R=1 (write-heavy, fast reads):** All nodes must accept writes; any single node can serve reads. Writes are slow and unavailable if any node fails. Reads are instant. Good for write-once, read-many data (lookup tables).
- **W=1, R=N (fast writes, read-heavy verification):** Any node can accept a write; reads must query all nodes. Writes are fast but reads are expensive. Good for high-write data where reads can tolerate higher latency.
- **W=R=⌊N/2⌋+1 (balanced quorum):** Majority quorum for both reads and writes. Tolerates ⌊(N-1)/2⌋ node failures for both operations. The default "balanced" configuration.

Even with W + R > N, leaderless systems are NOT linearizable by default. A concurrent write and read can overlap in a way where the read sees the old value even though a write quorum was achieved, due to the lack of atomic operations across nodes. Linearizability in leaderless systems requires additional mechanisms (Cassandra's lightweight transactions use Paxos for this).

---

## 4. Hands-On Walkthrough

### 4.1 Checking Postgres Replication Lag

```sql
-- On the PRIMARY: check replication status and lag
SELECT
    application_name,
    client_addr,
    state,                    -- streaming, catchup, etc.
    sync_state,               -- sync, async, quorum
    (pg_current_wal_lsn() - sent_lsn) AS pending_bytes,   -- not yet sent
    (sent_lsn - write_lsn)   AS write_lag_bytes,          -- sent but not written
    (write_lsn - flush_lsn)  AS flush_lag_bytes,          -- written but not fsynced
    (flush_lsn - replay_lsn) AS replay_lag_bytes,         -- fsynced but not applied
    write_lag,                -- time between primary WAL write and standby write
    flush_lag,                -- time between primary WAL flush and standby flush
    replay_lag                -- time between primary WAL flush and standby apply
FROM pg_stat_replication
ORDER BY replay_lag DESC NULLS LAST;

-- Understanding the lag columns:
-- write_lag  → standby has received but not yet written to disk
-- flush_lag  → standby has written but not fsynced (OS buffer)
-- replay_lag → standby has fsynced but not yet applied to data files
-- replay_lag is the user-observable lag: what a hot-standby read would show

-- On the STANDBY: check recovery status
SELECT
    now() - pg_last_xact_replay_timestamp() AS replication_lag,
    pg_last_wal_receive_lsn() AS received_lsn,
    pg_last_wal_replay_lsn()  AS replayed_lsn,
    pg_is_in_recovery()       AS is_standby
;

-- Find tables with the most WAL traffic (generating most replication load)
SELECT relname, n_tup_ins, n_tup_upd, n_tup_del,
       n_tup_ins + n_tup_upd + n_tup_del AS total_writes
FROM pg_stat_user_tables
ORDER BY total_writes DESC
LIMIT 10;
```

### 4.2 Checking Kafka ISR and Replication Health

```bash
# Describe topic and see ISR status
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic events
# Output:
# Topic: events  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
# Topic: events  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
# Topic: events  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
#
# If a replica falls out of ISR:
# Topic: events  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3
#                                                               ↑ broker 2 is out of ISR

# Check under-replicated partitions (URP) — ISR smaller than RF
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions
# Any output here = health issue. In a healthy cluster, this should return nothing.

# Check offline partitions
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --unavailable-partitions

# Monitor replica lag via JMX/Prometheus metrics
# kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica
# This is the max offset lag across all follower replicas on this broker

# Check consumer group lag (different from replication lag)
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group my-consumer-group
```

### 4.3 Checking Cassandra Replication State

```bash
# Check cluster ring and token distribution
nodetool status
# Shows: node status (U=up, D=down), replication state, load, token ownership

# Check if a specific node is in sync
nodetool netstats
# Shows: streams in progress, messages pending/completed

# Check repair status (anti-entropy progress)
nodetool compactionstats
nodetool describecluster

# Cassandra read repair can be triggered manually
nodetool repair keyspace_name table_name

# Check replication factor and settings for a keyspace
cqlsh> DESCRIBE KEYSPACE analytics;
# CREATE KEYSPACE analytics WITH replication = {
#   'class': 'NetworkTopologyStrategy',
#   'us-east-1': '3',
#   'eu-west-1': '3'
# };

# Check consistency level behavior with tracing
cqlsh> CONSISTENCY QUORUM;
cqlsh> TRACING ON;
cqlsh> SELECT * FROM analytics.events WHERE event_id = '...';
# Shows which nodes were contacted, their response times, and the read repair status
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
replication_analyzer.py

Analyze and simulate replication strategies for distributed data systems.
Covers:
  - Single-leader replication lag simulation
  - Quorum consistency analysis (W, R, N)
  - Multi-leader conflict detection
  - Replication strategy recommender

No external dependencies.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import random
import time


# ─── Enums and Types ────────────────────────────────────────────────────────────

class ReplicationMode(Enum):
    SYNCHRONOUS  = "synchronous"    # Wait for all replicas
    SEMI_SYNC    = "semi_sync"      # Wait for one replica
    ASYNCHRONOUS = "asynchronous"   # Fire and forget


class ConflictResolution(Enum):
    LWW         = "last_write_wins"
    CRDT        = "crdt_merge"
    APPLICATION = "application_defined"
    VERSION_VEC = "version_vectors"


@dataclass
class ReplicationConfig:
    """Configuration for a replication setup."""
    system_name: str
    strategy: str           # single_leader, multi_leader, leaderless
    mode: ReplicationMode
    n_replicas: int
    write_quorum: int       # W (for leaderless) or 1 (for single-leader sync)
    read_quorum: int        # R (for leaderless) or 1 (for async reads from replica)
    conflict_resolution: Optional[ConflictResolution] = None
    min_insync_replicas: Optional[int] = None  # Kafka-specific

    @property
    def quorum_satisfied(self) -> bool:
        """Does W + R > N? (Leaderless quorum overlap condition)"""
        return self.write_quorum + self.read_quorum > self.n_replicas

    @property
    def fault_tolerance(self) -> int:
        """How many replica failures can be tolerated?"""
        if self.strategy == "leaderless":
            # Can tolerate N - W failures for writes, N - R failures for reads
            return min(self.n_replicas - self.write_quorum,
                      self.n_replicas - self.read_quorum)
        elif self.strategy == "single_leader":
            # Single-leader: can tolerate N-1 failures (any follower) for reads
            # but leader failure requires election (handled by consensus layer)
            return self.n_replicas - 1
        else:
            return 0


@dataclass
class WriteResult:
    key: str
    value: str
    timestamp: float
    replicas_acked: list[str]
    success: bool
    latency_ms: float
    data_loss_risk: str   # "none", "low", "high"


@dataclass
class VersionVector:
    """
    Track causal history of a value across nodes.
    {node_id: sequence_number}
    """
    clocks: dict[str, int] = field(default_factory=dict)

    def increment(self, node_id: str):
        self.clocks[node_id] = self.clocks.get(node_id, 0) + 1

    def dominates(self, other: 'VersionVector') -> bool:
        """Return True if self happened-after other."""
        if not other.clocks:
            return True
        for node, seq in other.clocks.items():
            if self.clocks.get(node, 0) < seq:
                return False
        return True

    def concurrent_with(self, other: 'VersionVector') -> bool:
        """Return True if neither vector dominates the other (conflict)."""
        return not self.dominates(other) and not other.dominates(self)

    def merge(self, other: 'VersionVector') -> 'VersionVector':
        """Merge two version vectors (take element-wise max)."""
        merged = {}
        all_nodes = set(self.clocks) | set(other.clocks)
        for node in all_nodes:
            merged[node] = max(self.clocks.get(node, 0), other.clocks.get(node, 0))
        return VersionVector(merged)


# ─── Single-Leader Replication Simulator ────────────────────────────────────────

class Replica:
    """A single replica node in a replication setup."""

    def __init__(self, node_id: str, is_leader: bool = False,
                 base_latency_ms: float = 1.0):
        self.node_id = node_id
        self.is_leader = is_leader
        self.data: dict[str, tuple[str, float]] = {}    # key → (value, timestamp)
        self.log: list[dict] = []
        self.base_latency_ms = base_latency_ms
        self.replication_lag_ms: float = 0.0
        self._offline = False

    def write(self, key: str, value: str, timestamp: float) -> bool:
        """Write a key-value pair. Returns False if replica is offline."""
        if self._offline:
            return False
        # Simulate disk write latency
        time.sleep(self.base_latency_ms / 1000)
        self.data[key] = (value, timestamp)
        self.log.append({'key': key, 'value': value, 'timestamp': timestamp})
        return True

    def read(self, key: str) -> Optional[tuple[str, float]]:
        """Read a value. Returns None if offline or key not found."""
        if self._offline:
            return None
        return self.data.get(key)

    def set_offline(self, offline: bool):
        self._offline = offline

    @property
    def lag_behind_leader(self) -> int:
        """Approximate: how many operations behind the leader."""
        return 0  # Simplified; real lag is tracked per LSN


class SingleLeaderReplication:
    """
    Simulate single-leader replication with configurable sync mode.
    """

    def __init__(self, n_replicas: int = 3,
                 mode: ReplicationMode = ReplicationMode.ASYNCHRONOUS,
                 min_sync_replicas: int = 1):
        self.mode = mode
        self.min_sync_replicas = min_sync_replicas
        self.leader = Replica("leader", is_leader=True, base_latency_ms=2.0)
        self.followers = [
            Replica(f"follower-{i}", base_latency_ms=random.uniform(1.0, 5.0))
            for i in range(n_replicas - 1)
        ]
        self._write_count = 0

    def write(self, key: str, value: str) -> WriteResult:
        ts = time.time()
        start = time.monotonic()

        # Step 1: Write to leader
        success = self.leader.write(key, value, ts)
        if not success:
            return WriteResult(key, value, ts, [], False, 0, "high")

        replicas_acked = ["leader"]
        self._write_count += 1

        if self.mode == ReplicationMode.SYNCHRONOUS:
            # Wait for ALL available followers
            for f in self.followers:
                if f.write(key, value, ts):
                    replicas_acked.append(f.node_id)
            data_loss_risk = "none" if len(replicas_acked) > 1 else "high"

        elif self.mode == ReplicationMode.SEMI_SYNC:
            # Wait for min_sync_replicas followers
            synced = 0
            for f in self.followers:
                if synced >= self.min_sync_replicas:
                    break
                if f.write(key, value, ts):
                    replicas_acked.append(f.node_id)
                    synced += 1
            # Replicate rest asynchronously (fire and forget simulation)
            data_loss_risk = "none" if synced > 0 else "high"

        else:  # ASYNCHRONOUS
            # Don't wait — replicate in background (simulated by just writing after)
            for f in self.followers:
                # In reality, this would be async; here we just note they'll catch up
                f.write(key, value, ts)  # Would be async in production
            data_loss_risk = "low"  # Leader acked, followers may be behind

        latency_ms = (time.monotonic() - start) * 1000

        return WriteResult(
            key=key, value=value, timestamp=ts,
            replicas_acked=replicas_acked,
            success=True,
            latency_ms=round(latency_ms, 2),
            data_loss_risk=data_loss_risk,
        )

    def read_from_follower(self, key: str, follower_index: int = 0) -> Optional[str]:
        """Read from a specific follower (may return stale data)."""
        if follower_index >= len(self.followers):
            return None
        result = self.followers[follower_index].read(key)
        return result[0] if result else None

    def read_from_leader(self, key: str) -> Optional[str]:
        """Read from leader (always fresh)."""
        result = self.leader.read(key)
        return result[0] if result else None

    def simulate_follower_failure(self, follower_index: int):
        self.followers[follower_index].set_offline(True)
        print(f"  ⚡ {self.followers[follower_index].node_id} went offline")

    def get_consistency_anomalies(self) -> list[str]:
        """Check for any consistency anomalies across replicas."""
        anomalies = []
        all_keys = set(self.leader.data.keys())
        for f in self.followers:
            all_keys |= set(f.data.keys())

        for key in all_keys:
            leader_val = self.leader.data.get(key)
            for f in self.followers:
                follower_val = f.data.get(key)
                if leader_val and follower_val and leader_val != follower_val:
                    anomalies.append(
                        f"Key '{key}': leader={leader_val[0]} != "
                        f"{f.node_id}={follower_val[0]}"
                    )
        return anomalies


# ─── Quorum Consistency Analyzer ────────────────────────────────────────────────

def analyze_quorum_config(n: int, w: int, r: int, system_name: str = "") -> dict:
    """
    Analyze a quorum configuration (N, W, R) for leaderless replication.

    Returns: consistency guarantee, fault tolerance, trade-offs
    """
    overlap = w + r - n
    quorum_satisfied = overlap > 0
    write_fault_tolerance = n - w  # failures before writes fail
    read_fault_tolerance = n - r   # failures before reads fail

    if w == n:
        write_mode = "all replicas (highly durable, low availability)"
    elif w > n // 2:
        write_mode = "majority (CP with quorum)"
    else:
        write_mode = "minority (AP, high availability)"

    if r == 1:
        read_mode = "single replica (fast, may be stale)"
    elif r > n // 2:
        read_mode = "majority (always sees latest)"
    else:
        read_mode = "minority (may miss latest write)"

    consistency = "STRONG (W+R>N: reads always see latest write)" if quorum_satisfied \
                  else "EVENTUAL (W+R<=N: reads may miss recent writes)"

    return {
        "system": system_name or f"N={n},W={w},R={r}",
        "n": n, "w": w, "r": r,
        "w_plus_r": w + r,
        "quorum_overlap": overlap,
        "consistency": consistency,
        "write_fault_tolerance": write_fault_tolerance,
        "read_fault_tolerance": read_fault_tolerance,
        "write_mode": write_mode,
        "read_mode": read_mode,
    }


def print_quorum_comparison():
    """Print a comparison of common quorum configurations."""
    configs = [
        (3, 1, 1, "Cassandra ONE/ONE"),
        (3, 2, 2, "Cassandra QUORUM/QUORUM"),
        (3, 3, 1, "Cassandra ALL write / ONE read"),
        (3, 1, 3, "Cassandra ONE write / ALL read"),
        (5, 3, 3, "Cassandra QUORUM/QUORUM (5 nodes)"),
        (3, 2, 1, "DynamoDB eventual consistency"),
        (3, 2, 2, "DynamoDB strong consistency"),
    ]

    print(f"\n{'System':<40} {'W+R>N':>6} {'W-Tol':>6} {'R-Tol':>6} Consistency")
    print("─" * 90)
    for n, w, r, name in configs:
        info = analyze_quorum_config(n, w, r, name)
        quorum_ok = "✓" if info['quorum_overlap'] > 0 else "✗"
        print(f"{name:<40} {quorum_ok:>6} "
              f"{info['write_fault_tolerance']:>6} "
              f"{info['read_fault_tolerance']:>6} "
              f"{info['consistency'][:40]}")


# ─── Multi-Leader Conflict Detector ──────────────────────────────────────────────

@dataclass
class WriteRecord:
    key: str
    value: str
    origin_node: str
    version: VersionVector
    timestamp: float


class MultiLeaderConflictDetector:
    """
    Detect conflicts in a multi-leader replication scenario.
    Uses version vectors to identify true conflicts vs. causal updates.
    """

    def __init__(self, node_ids: list[str]):
        self.node_ids = node_ids
        self.writes: list[WriteRecord] = []
        self.node_sequences: dict[str, int] = {n: 0 for n in node_ids}

    def write(self, key: str, value: str, node_id: str,
              parent_version: Optional[VersionVector] = None) -> WriteRecord:
        """Record a write from a specific node."""
        if node_id not in self.node_ids:
            raise ValueError(f"Unknown node: {node_id}")

        self.node_sequences[node_id] += 1
        version = VersionVector(dict(parent_version.clocks) if parent_version else {})
        version.increment(node_id)

        record = WriteRecord(
            key=key, value=value, origin_node=node_id,
            version=version, timestamp=time.time()
        )
        self.writes.append(record)
        return record

    def detect_conflicts(self) -> list[tuple[WriteRecord, WriteRecord, str]]:
        """
        Find conflicting writes: pairs of writes to the same key
        where neither version vector dominates the other.

        Returns list of (write_a, write_b, conflict_type).
        """
        conflicts = []
        key_writes: dict[str, list[WriteRecord]] = {}

        for w in self.writes:
            key_writes.setdefault(w.key, []).append(w)

        for key, writes in key_writes.items():
            for i in range(len(writes)):
                for j in range(i + 1, len(writes)):
                    a, b = writes[i], writes[j]
                    if a.version.concurrent_with(b.version):
                        conflict_type = "CONCURRENT_WRITE"
                        if a.origin_node == b.origin_node:
                            conflict_type = "SAME_NODE_RACE"  # Shouldn't happen
                        conflicts.append((a, b, conflict_type))

        return conflicts

    def resolve_lww(self, conflicts: list[tuple[WriteRecord, WriteRecord, str]]) \
            -> list[dict]:
        """Resolve conflicts using Last Write Wins (by timestamp)."""
        resolutions = []
        for a, b, conflict_type in conflicts:
            if a.timestamp >= b.timestamp:
                winner, loser = a, b
            else:
                winner, loser = b, a
            resolutions.append({
                'key': a.key,
                'winner': winner.value,
                'winner_node': winner.origin_node,
                'loser': loser.value,
                'loser_node': loser.origin_node,
                'silently_discarded': loser.value,
                'resolution': 'LWW',
            })
        return resolutions


# ─── Replication Strategy Recommender ───────────────────────────────────────────

def recommend_replication_strategy(
    rpo_seconds: float,
    read_heavy: bool,
    geo_distributed: bool,
    write_volume_per_second: int,
    conflict_tolerance: bool,
) -> dict:
    """
    Recommend a replication strategy based on requirements.

    rpo_seconds:       Maximum acceptable data loss in seconds
    read_heavy:        True if reads >> writes
    geo_distributed:   True if writes come from multiple geographic regions
    write_volume_per_second: Peak write rate
    conflict_tolerance: True if application can handle write conflicts
    """
    recommendations = []
    reasoning = []

    if rpo_seconds == 0:
        strategy = "single_leader_synchronous"
        mode = "Synchronous replication (acks=all + min.isr=2 for Kafka; "
               "synchronous_commit=on for Postgres)"
        reasoning.append("RPO=0 requires synchronous replication: "
                         "every write must be on ≥2 nodes before ACK.")
        if write_volume_per_second > 50000:
            reasoning.append(
                "WARNING: High write volume with synchronous replication will "
                "require fast replicas in the same AZ (< 5ms RTT) to avoid "
                "write latency becoming a bottleneck."
            )
    elif rpo_seconds < 60:
        strategy = "single_leader_semi_sync"
        mode = "Semi-synchronous: wait for 1 synchronous replica, rest async."
        reasoning.append(
            f"RPO={rpo_seconds}s is achievable with semi-sync replication. "
            "Write latency is lower than full sync but provides durability "
            "on 2 nodes before ACK."
        )
    else:
        strategy = "single_leader_async"
        mode = "Asynchronous replication (fastest writes, RPO bounded by lag)."
        reasoning.append(
            f"RPO={rpo_seconds}s allows async replication. Monitor replication "
            "lag and alert when lag exceeds RPO threshold."
        )

    if geo_distributed and conflict_tolerance:
        strategy = "multi_leader"
        mode = "Multi-leader (active-active): write to nearest datacenter."
        reasoning.append(
            "Geo-distributed writes with conflict tolerance → multi-leader. "
            "Choose conflict resolution strategy: LWW for independent data, "
            "CRDTs for counters/sets, application logic for complex conflicts."
        )
    elif geo_distributed and not conflict_tolerance:
        reasoning.append(
            "Geo-distributed but no conflict tolerance → single-leader "
            "with a primary region, async replicas in other regions, OR "
            "leaderless with LOCAL_QUORUM consistency per region."
        )

    if read_heavy and not geo_distributed:
        reasoning.append(
            "Read-heavy workload → add read replicas to the single-leader setup. "
            "Route 80-90% of reads to followers. Accept replication lag for "
            "non-critical reads; route critical reads to the leader."
        )

    return {
        'recommended_strategy': strategy,
        'mode': mode,
        'reasoning': reasoning,
        'cassandra_consistency': _cassandra_consistency(rpo_seconds, geo_distributed),
        'kafka_config': _kafka_config(rpo_seconds),
        'postgres_config': _postgres_config(rpo_seconds),
    }


def _cassandra_consistency(rpo_seconds: float, geo_distributed: bool) -> dict:
    if rpo_seconds == 0:
        level = "QUORUM" if not geo_distributed else "EACH_QUORUM"
    elif rpo_seconds < 60:
        level = "LOCAL_QUORUM" if geo_distributed else "QUORUM"
    else:
        level = "ONE" if not geo_distributed else "LOCAL_ONE"
    return {'write_consistency': level, 'read_consistency': level}


def _kafka_config(rpo_seconds: float) -> dict:
    if rpo_seconds == 0:
        return {
            'acks': 'all',
            'min.insync.replicas': 2,
            'replication.factor': 3,
            'unclean.leader.election.enable': False,
        }
    elif rpo_seconds < 60:
        return {
            'acks': 'all',
            'min.insync.replicas': 2,
            'replication.factor': 3,
            'unclean.leader.election.enable': False,
        }
    else:
        return {
            'acks': '1',
            'replication.factor': 3,
            'unclean.leader.election.enable': False,
        }


def _postgres_config(rpo_seconds: float) -> dict:
    if rpo_seconds == 0:
        return {
            'synchronous_commit': 'on',
            'synchronous_standby_names': "'standby1'",
            'wal_level': 'replica',
            'max_wal_senders': 5,
        }
    elif rpo_seconds < 10:
        return {
            'synchronous_commit': 'remote_write',
            'synchronous_standby_names': "'standby1'",
        }
    else:
        return {
            'synchronous_commit': 'local',
            'wal_level': 'replica',
            'max_wal_senders': 5,
        }


# ─── Demo ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Replication Analyzer ===\n")

    # 1. Single-leader comparison
    print("─── Single-Leader Sync vs Async Comparison ───")
    for mode in [ReplicationMode.ASYNCHRONOUS,
                 ReplicationMode.SEMI_SYNC,
                 ReplicationMode.SYNCHRONOUS]:
        sim = SingleLeaderReplication(n_replicas=3, mode=mode, min_sync_replicas=1)
        results = [sim.write(f"key-{i}", f"value-{i}") for i in range(5)]
        avg_latency = sum(r.latency_ms for r in results) / len(results)
        print(f"  {mode.value:15s}: avg_latency={avg_latency:.2f}ms "
              f"data_loss_risk={results[0].data_loss_risk}")

    # 2. Quorum configuration analysis
    print("\n─── Quorum Configuration Analysis ───")
    print_quorum_comparison()

    # 3. Multi-leader conflict detection
    print("\n─── Multi-Leader Conflict Detection ───")
    detector = MultiLeaderConflictDetector(["dc-us", "dc-eu"])

    # User A in US and User B in EU simultaneously update the same record
    v_base = VersionVector({"dc-us": 2, "dc-eu": 2})  # Last common state
    write_us = detector.write("user:123:email", "alice@company.com", "dc-us", v_base)
    write_eu = detector.write("user:123:email", "alice@corp.eu", "dc-eu", v_base)

    # Non-conflicting: sequential update in same DC
    write_us2 = detector.write("user:123:name", "Alice", "dc-us",
                                VersionVector({"dc-us": 1}))
    write_us3 = detector.write("user:123:name", "Alice Smith", "dc-us",
                                write_us2.version)

    conflicts = detector.detect_conflicts()
    print(f"  Found {len(conflicts)} conflict(s):")
    for a, b, ctype in conflicts:
        print(f"    {ctype}: key='{a.key}' "
              f"dc-us='{a.value}' vs dc-eu='{b.value}'")

    resolutions = detector.resolve_lww(conflicts)
    for r in resolutions:
        print(f"    LWW resolution: '{r['winner']}' wins, "
              f"'{r['silently_discarded']}' silently discarded")

    # 4. Strategy recommender
    print("\n─── Replication Strategy Recommender ───")
    scenarios = [
        dict(rpo_seconds=0, read_heavy=False, geo_distributed=False,
             write_volume_per_second=5000, conflict_tolerance=False,
             label="Financial transaction log"),
        dict(rpo_seconds=300, read_heavy=True, geo_distributed=True,
             write_volume_per_second=500, conflict_tolerance=True,
             label="Social media user profiles"),
        dict(rpo_seconds=60, read_heavy=True, geo_distributed=False,
             write_volume_per_second=10000, conflict_tolerance=False,
             label="Analytics event store"),
    ]
    for s in scenarios:
        label = s.pop('label')
        rec = recommend_replication_strategy(**s)
        print(f"\n  Scenario: {label}")
        print(f"    Strategy: {rec['recommended_strategy']}")
        print(f"    Kafka: acks={rec['kafka_config']['acks']} "
              f"min.isr={rec['kafka_config'].get('min.insync.replicas', 'N/A')}")
        print(f"    Postgres: synchronous_commit={rec['postgres_config'].get('synchronous_commit')}")
        print(f"    Cassandra write: {rec['cassandra_consistency']['write_consistency']}")
```

---

## 6. Visual Reference

### Single-Leader Replication

```
SYNCHRONOUS REPLICATION
━━━━━━━━━━━━━━━━━━━━━━
Client ──WRITE──► Leader ──replicate──► Follower-1 ──ACK──►
                    │                                        │
                    └────────── SUCCESS ◄────────────────────┘
 Only returns after follower ACKs. RPO=0.
 Write latency = leader_fsync + RTT + follower_fsync

ASYNCHRONOUS REPLICATION
━━━━━━━━━━━━━━━━━━━━━━━━
Client ──WRITE──► Leader ──► SUCCESS   (returns immediately)
                    │
                    └──async──► Follower-1 (catches up in background)
 Low latency. Data loss risk if leader crashes before follower catches up.

SEMI-SYNCHRONOUS (Postgres synchronous_commit=on, Kafka acks=all+min.isr=2)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Client ──WRITE──► Leader ──replicate──► Follower-1 (SYNC) ──ACK──►
                    │                                                │
                    └────────── SUCCESS ◄─────────────────────────────┘
                    │
                    └──async──► Follower-2 (ASYNC, catches up later)
 1 RTT wait. RPO=0 for data on sync follower. Follower-2 may be behind.
```

### Kafka ISR and acks

```
Topic: events, RF=3 (N=3)
ISR = [broker-1 (leader), broker-2, broker-3]

acks=all + min.insync.replicas=2:
  Producer ──► broker-1 (leader appends to log)
               ├──► broker-2 fetches and ACKs
               └──► broker-3 fetches and ACKs
  broker-1 ──► Producer: SUCCESS (quorum of 2 have confirmed)

If broker-2 goes down:
  ISR = [broker-1, broker-3]   ← still 2 members, min.isr=2 satisfied
  Writes continue normally.

If broker-3 also goes down:
  ISR = [broker-1]              ← 1 member, min.isr=2 NOT satisfied
  acks=all produces: NOT_ENOUGH_REPLICAS error
  (Prevents silent acks=1 behavior)
```

### Leaderless Quorum (Cassandra QUORUM/QUORUM, N=3)

```
N=3 replicas: node-A, node-B, node-C
W=2 (QUORUM), R=2 (QUORUM)

Write x=1:
  Client ──► node-A: x=1 ✓
  Client ──► node-B: x=1 ✓    ← 2 ACKs = quorum reached → SUCCESS
  Client ──► node-C: x=1 ✓ (maybe)

Read x:
  Client ──► node-A: x=1 ✓
  Client ──► node-B: x=1 ✓    ← 2 responses = quorum reached
  Returns: x=1

Overlap: write quorum {A,B} ∩ read quorum {A,B} = {A,B} ≥ 1 node
  The overlapping node (A or B) has the latest write. ✓

If node-C had old value x=0:
  Read quorum {B,C} = {x=1, x=0}
  Coordinator returns max timestamp → x=1 (and repairs C in background)
```

---

## 7. Common Mistakes

**Mistake 1: Using `acks=all` without setting `min.insync.replicas=2`.** The default `min.insync.replicas=1`. With RF=3 and `acks=all`, if two followers fall behind and leave the ISR, the ISR shrinks to just the leader. `acks=all` then means "all 1 ISR members" = effectively `acks=1`. The producer gets success, but the message is on only one broker. If that broker fails, the message is lost. Always pair `acks=all` with `min.insync.replicas=2` on topics where RPO=0 is required.

**Mistake 2: Assuming Postgres streaming replication provides read-your-writes consistency from standbys.** In the default async streaming configuration, `pg_last_xact_replay_timestamp()` on a standby can lag the primary by seconds. A user who writes data and then immediately reads from a standby (e.g., through a load balancer) may see stale data. Fix: use `synchronous_commit = remote_apply` for hot standbys where fresh reads are required, or always route a user's reads to the primary for a bounded window after their write.

**Mistake 3: Enabling `unclean.leader.election.enable=true` on Kafka topics.** This setting allows an out-of-sync replica to be elected leader when all ISR members are unavailable. It restores availability, but the out-of-sync replica may be missing committed messages — those messages are gone. For any topic where data loss is unacceptable (event sourcing, financial transactions, audit logs), this must be `false`. The default in modern Kafka is `false`; verify it hasn't been set to `true` in a misguided attempt to restore availability after an incident.

**Mistake 4: Using CRDT-incompatible data structures with Cassandra and expecting LWW to be safe.** Cassandra LWW silently discards concurrent writes based on timestamps. For counters, this means lost increments: if two clients simultaneously increment a counter (both read 5, both write 6), one write is discarded and the counter shows 6 instead of 7. Cassandra has a dedicated `COUNTER` type that uses CRDT semantics. Always use it for counters. LWW is only safe for data where the last write is genuinely the correct state (user profile updates, configuration values).

**Mistake 5: Treating Cassandra QUORUM as equivalent to strong consistency.** QUORUM (W + R > N) guarantees that reads see the latest write under the quorum model. But Cassandra's QUORUM is not linearizable: a concurrent read and write can see an interleaved state that violates sequential consistency. For true linearizability in Cassandra, use Lightweight Transactions (LWT), which add a Paxos round-trip. LWT is significantly more expensive (2-4× latency) and should only be used where linearizability is genuinely required.

---

## 8. Production Failure Scenarios

### Scenario 1: Postgres Async Replication Lag Causes Data Loss on Failover

**Symptoms:** A production Postgres primary crashes unexpectedly at 02:14 UTC. The monitoring system promotes the warm standby at 02:15. After the promotion, the ops team discovers that 47 transactions committed between 02:13:45 and 02:14 are missing from the new primary. Users who successfully submitted orders during those 75 seconds see their orders "disappear."

**Root cause:** Postgres was configured with `synchronous_commit = off` — the fastest and most dangerous configuration. The primary had not yet shipped the last 75 seconds of WAL to the standby. When the primary crashed, those WAL records were lost. The promotion succeeded, but the standby's state was 75 seconds behind.

**What the monitoring missed:** `pg_stat_replication.replay_lag` showed the standby was 45 seconds behind — above the 30-second alert threshold — but the alert had been silenced due to "known slow periods." The replication lag metric was present but the alert was disabled.

**Fix:** Set `synchronous_commit = remote_write` or `on` for any table with financial or order data. Accept the latency penalty. Ensure replication lag alerts are never silenced without an escalation process. For highest-value tables, consider PostgreSQL synchronous replication to a replica in the same AZ (1ms RTT, negligible latency cost).

### Scenario 2: Cassandra LWW Silently Discards Writes in Multi-DC Setup

**Symptoms:** An e-commerce site uses Cassandra multi-datacenter replication (US + EU) with NetworkTopologyStrategy and LOCAL_QUORUM consistency. A user in Germany updates their shipping address from "Berlin" to "Munich" via the EU datacenter. Simultaneously, a backend job (triggered by a cron in the US datacenter) resets user preferences to defaults — inadvertently writing the old Berlin address. 200ms later, the LWW resolution occurs: the backend job's write has a timestamp of `T+1ms` (server clock); the user's write has `T`. LWW picks the higher timestamp: the backend job's write wins. The user's update is silently discarded.

**Root cause:** LWW is sensitive to clock skew between datacenters. The backend job's server clock was 1ms ahead of the EU server clock. The user's write arrived first causally but had a lower timestamp numerically.

**Impact:** The user's shipping address reverts to Berlin. Their next order ships to the wrong address. No error was ever returned to the user — from their perspective, the update succeeded.

**Fix:** For user-initiated writes that must not be overwritten by background jobs, use application-level ordering (background jobs check a "locked" flag before writing) or use Cassandra Lightweight Transactions (LWT) for the user update to ensure CAS (compare-and-swap) semantics. Alternatively, restructure the data model: user shipping address is a separate table from preference defaults, so the background job writes can never conflict with the shipping address update.

### Scenario 3: Kafka ISR Shrink Under High Load Causes Producer Outage

**Symptoms:** At peak traffic, a Kafka cluster's brokers are under high CPU and disk I/O load. Follower replicas fall behind their leaders by more than `replica.lag.time.max.ms` (30s) and are removed from the ISR. Over 45 minutes, all three topics have ISR = [leader only]. All producers using `acks=all` and `min.insync.replicas=2` begin receiving `NOT_ENOUGH_REPLICAS` exceptions. The application stops ingesting events. The event pipeline stalls.

**Root cause:** Follower replication competes with consumer fetch requests for disk read I/O. Under heavy consumer load (many consumers reading from the beginning of the log — a reprocessing job was triggered), the disk was saturated. Followers could not keep up with replication. ISR shrank progressively. The `min.insync.replicas=2` safety valve correctly prevented silent data loss (the right behavior), but the result was a complete write outage.

**Fix:** Separate the disk I/O paths: use JBOD (multiple disks) on Kafka brokers so replication I/O and consumer I/O are on different physical disks. Throttle large reprocessing consumers using `kafka-consumer-groups.sh --throttle` or `fetch.max.bytes` tuning. Add ISR shrink alerting: alert immediately when any topic has `ISR < RF`, before the `min.insync.replicas` threshold is hit. Address the root cause (disk saturation) rather than changing `min.insync.replicas`.

### Scenario 4: Multi-Leader Split-Brain in DynamoDB Global Tables

**Symptoms:** A company uses DynamoDB Global Tables (multi-region active-active replication) to allow writes from both `us-east-1` and `eu-west-1`. A network issue between regions causes replication to stall for 8 minutes. During those 8 minutes, 1,200 writes to user session tokens occur in both regions simultaneously — some keys updated in both regions concurrently. When the network recovers and replication resumes, DynamoDB's LWW conflict resolution silently picks one write per key based on timestamp. 340 user sessions are invalidated: their session token in the "winner" region doesn't match the application server's cached token in the "loser" region, causing unexpected logouts.

**Root cause:** DynamoDB Global Tables uses LWW. When both regions write to the same item during a replication outage, one write is discarded. Session tokens are not safe for LWW: both writes are "correct" from their region's perspective, but LWW makes one session invalid.

**Fix:** Session tokens should not be stored in a multi-leader system. Use single-region writes for session state with a global token format that doesn't conflict (e.g., include the originating region in the token so each region has a distinct key namespace). Alternatively, use a strongly consistent store for sessions (ElastiCache with single-region leader) and accept higher latency for cross-region auth.

---

## 9. Performance and Tuning

### Replication Lag Budget

In a synchronous replication setup, the write latency budget is:
```
write_latency = leader_disk_fsync + network_RTT + follower_disk_fsync
```

Within a single AZ (same datacenter):
- Leader fsync (NVMe SSD): 2–5ms
- Network RTT: 0.5–1ms
- Follower fsync: 2–5ms
- Total: ~5–11ms per write

Cross-AZ (same region, different AZ):
- Network RTT: 1–5ms
- Total: ~7–15ms per write

Cross-region:
- Network RTT: 50–150ms
- Total: ~55–160ms per write

This is the fundamental reason cross-region synchronous replication is impractical for latency-sensitive systems.

### Postgres Replication Tuning

```
# Reduce replication lag under write pressure:
wal_compression = on              # Compress WAL segments (reduces network I/O)
wal_buffers = 64MB                # Larger WAL buffer reduces fsyncs under burst load
max_wal_senders = 10              # Allow enough sender connections
wal_keep_size = 1GB               # Keep WAL for slow standbys (prevent them disconnecting)

# Sync commit tuning per transaction:
-- For critical writes (don't lose):
SET synchronous_commit = 'on';
INSERT INTO orders ...;

-- For non-critical bulk writes (lose a few seconds is OK):
SET synchronous_commit = 'off';
INSERT INTO analytics_events ...;
-- Can also be set at table level or session level
```

### Kafka Replication Tuning

```
# Broker config for follower replication throughput:
num.replica.fetchers=4        # Parallel fetch threads per broker for replication
replica.fetch.max.bytes=10MB  # Larger fetch size reduces RTT count for high-volume
replica.lag.time.max.ms=30000 # ISR removal threshold — increase if followers are slow
                               # (but too high = stale ISR membership)

# Producer config for balancing durability vs throughput:
acks=all                       # Wait for ISR quorum ACK
min.insync.replicas=2          # Minimum ISR size (set on topic)
linger.ms=5                    # Batch for 5ms to improve throughput
batch.size=65536               # 64KB batches

# Topic-level config:
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name events \
  --alter \
  --add-config min.insync.replicas=2,unclean.leader.election.enable=false
```

### Cassandra Replication Tuning

```
# Keyspace replication factor:
# For 3 nodes, RF=3 (all nodes hold all data) — max durability, no node can be missing
# For 6 nodes, RF=3 — balance of durability and node efficiency

# Read repair chance:
# read_repair_chance = 0 (default in Cassandra 4.x — background repairs via nodetool repair)
# Avoid relying on read repair for consistency; use nodetool repair on a schedule

# Consistency level guidance:
# LOCAL_QUORUM for multi-DC: only requires quorum within local DC
#   → lower latency, tolerates inter-DC network issues
# QUORUM for single-DC: requires quorum across all nodes
# Use LOCAL_QUORUM in production multi-DC setups — QUORUM requires cross-DC round trips

# Hinted handoff:
# hinted_handoff_enabled=true (default)
# max_hint_window=3h (default) — hints stored up to 3 hours
# If a node is down for > 3h, hints are dropped; run nodetool repair after recovery
```

---

## 10. Interview Q&A

**Q1: What is replication lag and what consistency anomalies does it cause?**

Replication lag is the delay between a write being committed on the leader (or accepted by a write quorum in leaderless systems) and that write being visible on a follower or lagging replica. In a healthy system with modern hardware in the same datacenter, lag is typically single-digit milliseconds. Under write pressure, follower restart, or network congestion, lag can grow to seconds or minutes.

Lag causes three observable consistency anomalies for applications that read from replicas. The first is read-your-own-writes: a user writes data through the leader and immediately reads from a lagged follower — their write is invisible. From the user's perspective, the write was lost, even though it was committed. The second is monotonic reads: a user reads version V from one replica, then reads the same data from a different, more-lagged replica and sees an older version — the world appears to go backward in time. The third is consistent prefix reads: in sharded systems, some shards may replicate faster than others. A query that joins across shards may see shard A's writes but not shard B's, observing a causally inconsistent state (an effect without its cause).

Mitigating these anomalies requires either routing reads to the leader (serializable reads, higher latency), session affinity (always route a user's reads to the same replica), or write-to-read lag tracking (only serve a read from a replica that has caught up to the client's last write LSN).

**Q2: Compare Kafka `acks=all` and Postgres synchronous replication. What guarantees do they each provide?**

Both provide synchronous replication to a quorum of replicas, but with different scopes and semantics.

Kafka `acks=all` guarantees that a produce request is not acknowledged until all current ISR (In-Sync Replica) members have written the message to their local log. "All ISR" is dynamic: if a follower falls behind and is removed from ISR, the remaining ISR members are sufficient. The `min.insync.replicas` setting puts a floor on ISR size: if ISR shrinks below this threshold, new writes are rejected rather than accepted with a reduced quorum. With RF=3, `acks=all`, and `min.insync.replicas=2`, the guarantee is: the message is on at least 2 brokers before the producer sees success. If both the leader and one ISR follower fail simultaneously (an unlikely but possible failure), the message may be lost — but this requires two concurrent failures.

Postgres synchronous replication (`synchronous_commit=on` with `synchronous_standby_names`) guarantees that a transaction commit is not acknowledged until the nominated synchronous standby has written the WAL record to its disk. If the synchronous standby is unavailable, writes block — Postgres does not accept writes in a degraded state (unlike Kafka which continues if ISR >= min.isr). With `synchronous_commit=remote_apply`, the guarantee is even stronger: Postgres waits for the standby to apply the change to its data files, ensuring hot-standby reads on the standby reflect the committed state.

The key operational difference: Kafka gracefully degrades when followers fall behind (ISR shrinks, but writes continue if min.isr is satisfied), while Postgres with synchronous replication blocks writes if the designated synchronous standby is unavailable, providing stronger durability guarantees at the cost of availability.

**Q3: What is the difference between single-leader, multi-leader, and leaderless replication? When would you choose each?**

Single-leader replication designates one node to receive all writes and replicates changes to followers. It is the simplest model, provides the clearest consistency semantics (reads from leader are always fresh), and is the right choice for the vast majority of data engineering workloads. Choose single-leader when: writes come from one geographic region, the write rate is manageable by a single node, and you need strong consistency or simple failover. Examples: Postgres primary-standby, Kafka partition leader, Redis primary.

Multi-leader replication allows writes at multiple nodes simultaneously, with each leader replicating to the others. It enables geo-distributed active-active setups (write to the nearest datacenter without cross-region round-trips) and offline-capable applications. The cost is conflict resolution: when two leaders accept concurrent writes to the same key, one must win and the other is discarded or merged. Choose multi-leader when: write latency across regions is unacceptable, the application can tolerate or handle write conflicts, and the data model can be designed around conflict-safe operations. Examples: Cassandra multi-datacenter, DynamoDB Global Tables.

Leaderless replication allows any node to accept reads and writes, using quorum overlap (W + R > N) to ensure reads see the latest write. It provides higher availability (no single leader failure point) and enables tunable consistency. Choose leaderless when: high availability is paramount, the workload tolerates tunable (not strict) consistency, and the system needs to tolerate node failures without leader election. Cassandra is the canonical example in data engineering.

---

## 11. Cross-Question Chain

**Interviewer:** Your team is building a user profile service. Profiles are read 100x more often than they're written. You're using Postgres. How do you design the replication setup?

**Candidate:** High read-to-write ratio screams read replicas. I'd configure one Postgres primary (handles all writes) with three or four async streaming standbys for reads. Since reads vastly outnumber writes, I'd use PgBouncer connection pooling in front of the replicas and route 80% of reads to replicas. The application tier needs to be aware of replication lag: for any read immediately following a write (read-your-own-writes), it routes to the primary. For browsing or search (lag-tolerant reads), it uses replicas.

For replication mode: `synchronous_commit=on` with one synchronous standby. Profiles are user data — losing a few seconds of writes on failover would be bad UX. The synchronous standby gives RPO=0 for the most recent writes. The other two standbys are async — they're there for read scale, not durability.

**Interviewer:** One of your standby replicas falls 2 minutes behind. How do you detect and respond to this?

**Candidate:** I'd alert on `pg_stat_replication.replay_lag` — the time between primary WAL flush and standby apply. Two minutes of lag is alarming. First response: check the standby's resource utilization. High CPU or disk I/O usually means a long-running query on the standby is blocking WAL replay (hot standbys can have this issue). Terminate the blocking query on the standby. If it's a Postgres `max_standby_streaming_delay` configuration issue (where replication pauses while a conflicting query is running), I'd tune that.

If the standby genuinely can't keep up (under-provisioned), I'd temporarily remove it from the read rotation until it catches up, then return it to the pool once `replay_lag` is under 5 seconds. I'd also check if a runaway consumer (a data warehouse ETL job, maybe) is issuing sequential scans that compete with replay.

For long-term: add monitoring on `pending_bytes` (WAL backlog on primary not yet sent) — this catches the problem earlier. And ensure the async standbys are excluded from the connection pool when their lag exceeds the application's tolerance threshold.

**Interviewer:** The primary fails. Walk me through the failover process and what could go wrong.

**Candidate:** With a synchronous standby, the failover path is: Patroni (or Stolon, or a manual operator) detects that the primary is unreachable — it checks from multiple places to avoid false promotion. It promotes the synchronous standby as the new primary. The old primary is fenced (via STONITH, or the cloud instance is terminated) to prevent split-brain. The async standbys are re-pointed to the new primary.

What can go wrong: first, split-brain. If the old primary is partitioned but not dead — still accepting writes — and we promote the standby, we have two nodes that think they're the primary. This is why fencing is critical: kill the old primary or remove its access to the database volume before promoting the standby. Patroni uses DCS (etcd, ZooKeeper, or Consul) to manage leader leases and prevent this.

Second, data loss in the async standbys. If the failed primary had sent some WAL to the synchronous standby but not the async ones, the new primary (former synchronous standby) will be ahead of the async standbys. The async standbys will see their LSN is behind the new primary's timeline and need to resync. Postgres handles this via timeline switching — `restore_command` on standbys, or pg_rewind to fast-forward to the new primary's timeline.

Third, application connection routing. The application is still pointing to the old primary's DNS or IP. Update the DNS record or VIP (virtual IP) to point to the new primary. Patroni can integrate with HAProxy or a cloud load balancer for this. Set `TCP_KEEPALIVE` on database connections to ensure stale connections to the dead primary are detected quickly.

**Interviewer:** The application team says they need "zero recovery time" — they don't want to redeploy or reconnect after a failover. How do you achieve that?

**Candidate:** "Zero recovery time" for the application means transparent failover at the connection layer. Three components: first, connection pooling with health-checking. PgBouncer in front of Postgres, with automatic reconnection. PgBouncer detects the primary is down and starts queuing connections. When the new primary is up and reachable, PgBouncer routes to it — the application's connections are never terminated; they just pause briefly.

Second, DNS-based failover. Point the application to a DNS name (e.g., `postgres-primary.internal`) rather than a specific IP. On failover, Patroni updates the DNS record to the new primary's IP. DNS TTL should be 10 seconds — short enough for fast failover, long enough not to stress the DNS server. The application needs to handle transient connection errors during the TTL propagation window — a retry with backoff of 1–5 seconds is enough.

Third, Patroni's built-in HAProxy integration. Patroni exposes a health endpoint (HTTP): the primary returns 200, standbys return 503. HAProxy front-ends the connection, checks the health endpoint every second, and routes to the healthy primary. Failover is transparent to the application — the HAProxy address never changes, only the backend it routes to. This is the most production-proven pattern for Postgres HA transparent failover.

The honest caveat: "zero" recovery time means no application code changes but there's still a brief (2–10 second) window of write failures during failover — the time for Patroni to detect, fence, and promote. Applications must handle transient database errors gracefully (connection retry with backoff) regardless of the HA topology. There is no truly instantaneous failover in distributed systems — only fast failover with graceful error handling.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What are the three types of replication by leader architecture? | Single-leader (all writes to one node, replicated to followers), Multi-leader (writes to any leader, replicated between leaders), Leaderless (writes to any node, quorum required). |
| 2 | In single-leader replication, what is the primary trade-off between synchronous and asynchronous modes? | Synchronous: RPO=0 (no data loss on failover), higher write latency (one round-trip to follower). Asynchronous: low write latency, non-zero RPO (data on leader only until follower catches up). |
| 3 | What is replication lag and what causes it? | The delay between a write on the leader and its visibility on a follower. Causes: follower disk I/O saturation, network congestion, competing queries on standby blocking WAL replay, follower GC pause (JVM-based systems). |
| 4 | What is the read-your-own-writes consistency problem? | After writing to the leader, reading from a lagged follower returns stale data — the user doesn't see their own write. Fix: route the user's reads to the primary (or the same replica) for a window after each write. |
| 5 | What is monotonic reads? | A user reads value V from replica A, then reads from slower replica B and sees an older value V-1. They observe time going backward. Fix: route a user's reads to the same replica (session affinity). |
| 6 | What does `min.insync.replicas=2` in Kafka prevent? | Prevents `acks=all` from being effectively `acks=1`. If all followers leave the ISR, the ISR shrinks to just the leader. Without `min.insync.replicas=2`, `acks=all` would succeed with ISR=[leader] — a single point of failure. With `min.insync.replicas=2`, writes fail if ISR < 2, preventing silent data loss. |
| 7 | What is the ISR in Kafka? | In-Sync Replicas: the set of follower replicas that are within `replica.lag.time.max.ms` of the leader's log end offset. `acks=all` waits for ACK from all ISR members. A lagged follower is removed from ISR; it rejoins when it catches up. |
| 8 | What is `unclean.leader.election.enable` in Kafka? | When all ISR members are unavailable, controls whether an out-of-sync replica can be elected leader (`true`) to restore availability at the cost of data loss, or whether the partition waits for an ISR member to recover (`false`, the safe default). |
| 9 | What is the quorum overlap condition for leaderless replication? | W + R > N. Ensures that at least one node in the read quorum has seen the latest write. With N=3, W=2, R=2: W+R=4>3. If W+R=N, a read quorum may not intersect the write quorum, returning stale data. |
| 10 | In Cassandra, what is the difference between QUORUM and LOCAL_QUORUM? | QUORUM requires acknowledgment from a majority of ALL replicas across all datacenters. LOCAL_QUORUM requires acknowledgment from a majority of replicas in the LOCAL datacenter only. In multi-DC setups, LOCAL_QUORUM is preferred: lower latency, tolerates inter-DC network issues. |
| 11 | What is Last Write Wins (LWW) conflict resolution? | The write with the highest timestamp wins. Concurrent writes to the same key: the one with the higher clock value survives; the other is silently discarded. Cassandra uses LWW by default. Vulnerable to clock skew — a write that arrived later physically but with an earlier clock value is lost. |
| 12 | What is a version vector and when is it used? | A vector of (node, sequence_number) pairs tracking causal history. If neither vector dominates the other (concurrent writes from different nodes), it's a true conflict. Used in CouchDB, Riak, and distributed shopping carts to detect concurrent writes that LWW would silently discard. |
| 13 | What is the difference between Postgres physical and logical replication? | Physical: streams raw WAL bytes — exact page copies, fast, tightly coupled to Postgres version. Used for HA standbys. Logical: decodes WAL into row-level changes (INSERT/UPDATE/DELETE), allows partial table replication, cross-version replication, and heterogeneous targets (Debezium CDC). |
| 14 | What Postgres config gives the strongest durability guarantee for synchronous replication? | `synchronous_commit = remote_apply`: waits for the standby to apply the change to its data files, not just fsync the WAL. Ensures hot-standby reads on the standby are always fresh. More durable but higher latency than `remote_write` or `on`. |
| 15 | What is read repair in Cassandra? | When a read quorum returns different versions from different replicas, the coordinator returns the most recent value and asynchronously (or synchronously, depending on `read_repair_chance`) updates stale replicas. Ensures eventual consistency. |
| 16 | What is hinted handoff in Cassandra? | When a node is temporarily unavailable, the coordinator stores writes intended for it as "hints." When the node recovers, hints are delivered and applied. Hints are stored for at most `max_hint_window` (default 3h). If the node is down longer, hints are dropped — run `nodetool repair` after a long outage. |
| 17 | What is a sloppy quorum? | In leaderless replication, when fewer than W nodes in the preferred replica set are available, a sloppy quorum writes to other available nodes that don't normally own that data range. Improves availability at the cost of temporary inconsistency. Used by Cassandra and DynamoDB. |
| 18 | What is the key operational advantage of Kafka KRaft over ZooKeeper for replication management? | KRaft eliminates the separate ZooKeeper cluster. ISR changes, leader elections, and metadata updates are all Raft log entries. No cross-system operational complexity. KRaft controller elections are faster (seconds vs. tens of seconds) and the metadata store scales to millions of partitions. |
| 19 | Why is the combination `acks=1` and `unclean.leader.election.enable=false` still not RPO=0? | `acks=1` means only the leader has confirmed the write. If the leader crashes before replicating to any follower, and `unclean.leader.election.enable=false` causes the partition to wait for a proper ISR member (which never received the write), the message may be lost. Only `acks=all` + `min.insync.replicas=2` + `unclean.leader.election.enable=false` gives RPO=0. |
| 20 | What is Cassandra Lightweight Transaction (LWT) and when is it necessary? | LWT uses Paxos for a compare-and-swap (CAS) operation: `INSERT ... IF NOT EXISTS` or `UPDATE ... IF condition`. It provides linearizability — unlike QUORUM reads which are not linearizable. Use LWT when you need atomic check-and-set guarantees (user registration, idempotent writes, counter increments where lost increments are unacceptable). Cost: 2–4× latency of a regular write. |

---

## 13. Further Reading

- **"Designing Data-Intensive Applications" by Martin Kleppmann, Chapter 5 (Replication):** The definitive textbook treatment of replication. Covers single-leader, multi-leader, and leaderless replication, replication lag anomalies, and consistency guarantees with examples from real systems. Read the entire chapter — it maps directly to production questions.
- **Postgres documentation — "High Availability, Load Balancing, and Replication":** Covers streaming replication, WAL shipping, synchronous replication levels, and logical replication in authoritative detail. The section on `synchronous_commit` levels is particularly useful.
- **Kafka documentation — "Replication" and "Producer Config":** The Kafka documentation on replication (ISR, acks, min.insync.replicas) and the producer configuration reference are essential. The "Replication" section of the Kafka documentation explains the ISR protocol and leadership election in detail.
- **Apache Cassandra documentation — "Data Replication":** Covers NetworkTopologyStrategy, consistency levels, read repair, and anti-entropy repair. The consistency level reference table (which levels satisfy W+R>N) is a must-read.
- **"Highly Available Transactions: Virtues and Limitations" by Bailis et al. (2014):** Formal analysis of which consistency levels are achievable without requiring coordination (without consensus). Maps directly to the spectrum from eventual to linearizable consistency in production systems.
- **Patroni documentation:** The canonical open-source Postgres HA solution. Reading its architecture documentation explains how real-world Postgres failover works — DCS-based leader leases, fencing, timeline management.

---

## 14. Lab Exercises

**Exercise 1: Observe Postgres Replication Lag**

Set up a Postgres primary and one async streaming standby (Docker Compose is sufficient). Run `pg_bench` against the primary at increasing load. On the standby, continuously query `now() - pg_last_xact_replay_timestamp()`. Observe lag growing under write pressure. Then set `synchronous_commit = on` on the primary with the standby as synchronous standby. Re-run the benchmark and observe: (a) latency increases, (b) lag drops to near-zero. Document the latency vs. lag trade-off.

**Exercise 2: Simulate Kafka ISR Shrink**

Create a Kafka topic with RF=3, `min.insync.replicas=2`. Pause one of the follower brokers (using Docker: `docker pause kafka-broker-2`). After `replica.lag.time.max.ms` (30s), run `kafka-topics.sh --describe --under-replicated-partitions` and observe the ISR shrink. Attempt to produce with `acks=all` — it should still succeed (ISR=2, min.isr=2). Pause a second broker — ISR shrinks to 1. Produce again — observe `NOT_ENOUGH_REPLICAS` error. Unpause both brokers, wait for ISR to rebuild, verify produces succeed.

**Exercise 3: Cassandra Quorum Trade-offs**

In a 3-node Cassandra cluster, run writes and reads with different consistency levels and measure latency: (a) ONE/ONE — fastest, (b) QUORUM/QUORUM — consistent. Take one node offline. With ONE/ONE, writes and reads still work. With QUORUM/QUORUM, they still work (quorum of 2 is still met). With ALL/ALL, writes and reads fail. Document the availability vs. consistency trade-off at each level.

**Exercise 4: Multi-Leader Conflict Simulation**

Using the `replication_analyzer.py` `MultiLeaderConflictDetector`: create three nodes, write the same key concurrently from two different nodes with a common parent version vector. Run `detect_conflicts()` and observe the conflict. Run `resolve_lww()` and observe that one write is silently discarded. Then write the same key sequentially (child version from parent): observe no conflict is detected (one version dominates the other). This demonstrates the difference between truly concurrent writes (conflict) and causally ordered writes (no conflict).

**Exercise 5: RPO Calculation**

Given a Postgres setup with async streaming replication where the standby is typically 5 seconds behind the primary: (a) Calculate the worst-case RPO if the primary fails immediately after a write. (b) A user places an order. The primary ACKs the insert. The primary crashes 3 seconds later. The standby is 5 seconds behind at that moment. Is the order in the standby? (c) What synchronous_commit level would guarantee the order is never lost? (d) What latency overhead does that add, assuming the standby is in the same AZ with 1ms RTT?

---

## 15. Key Takeaways

Replication serves three purposes: fault tolerance (surviving node failures), read scalability (distributing query load), and latency reduction (placing data near users). Every replication decision is a trade-off between these benefits and the consistency guarantees you're willing to sacrifice.

Single-leader replication is the right default for most data engineering systems. It provides the simplest consistency model, clear failover semantics, and well-understood failure modes. The critical configuration decisions are: synchronous vs asynchronous mode (determines RPO) and how many replicas to wait for (determines durability vs latency). For Kafka: `acks=all` + `min.insync.replicas=2` is the correct default for durable topics. For Postgres: `synchronous_commit=on` with one sync standby for high-value data.

Replication lag is the most common source of subtle consistency bugs in production. The three lag-induced anomalies — read-your-own-writes, monotonic reads, and consistent prefix reads — all have known solutions: route writes to leader, session affinity, or track write LSN per session. The failure to handle these in application code produces bugs that are notoriously hard to reproduce in development environments (no lag) but appear only in production (real lag under load).

Multi-leader and leaderless replication trade simplicity for higher availability and geo-distribution capability. Multi-leader introduces the conflict resolution problem — LWW silently discards writes, CRDTs require careful data modeling, and application-level resolution requires significant engineering investment. Leaderless (Cassandra) provides tunable consistency but is not linearizable by default.

The interview test: given a system's RPO and write latency requirements, specify the exact Kafka producer configuration (acks, min.insync.replicas), Postgres replication configuration (synchronous_commit, synchronous_standby_names), or Cassandra consistency level. This is the operational knowledge that distinguishes engineers who have run these systems in production.

---

## 16. Connections to Other Modules

- **M37 — CAP Theorem:** Synchronous replication implements the C in CP; asynchronous implements the A in AP. The CAP framework classifies each replication strategy: Postgres sync = CP, Cassandra ONE = AP.
- **M38 — Consensus Algorithms:** Raft is single-leader replication where leadership is determined by consensus and log replication is enforced with the Raft safety guarantees. Raft adds the formal safety properties (Leader Completeness, Log Matching) to the leader-follower replication model.
- **M40 — Failure Modes:** Split-brain is the failure mode that replication's failover protocol must prevent. The synchronous standby in Postgres and the ISR protocol in Kafka are both defenses against split-brain. M40 covers the failure landscape broadly.
- **DCS-KFK-101 — Kafka Internals:** ISR management, `acks`, and `min.insync.replicas` are applied Kafka-specific topics that build directly on the leaderless and synchronous replication concepts from this module.
- **DBE-SQL-101 — Postgres Internals:** WAL, streaming replication, and logical replication are the Postgres implementation of the synchronous and semi-synchronous single-leader replication concepts from this module.

---

## 17. Anti-Patterns

**Anti-pattern: Reading from async replicas without lag awareness in the application.** The application doesn't check `pg_last_xact_replay_timestamp()` or the Kafka consumer offset lag before reading. Users see stale data intermittently, producing hard-to-reproduce bugs. Fix: instrument the application to route lag-sensitive reads (post-write reads) to the primary. Use read replicas only for demonstrably lag-tolerant queries (analytics, reporting, full-text search indexing).

**Anti-pattern: Using Cassandra QUORUM without LWT for unique constraint enforcement.** QUORUM reads are not linearizable — two concurrent inserts checking "does this username exist?" can both see no existing record and both insert the same username. This creates duplicate entries, violating application-level uniqueness constraints that Cassandra doesn't enforce natively. For uniqueness, use LWT: `INSERT INTO users (username, ...) VALUES (...) IF NOT EXISTS`. Accept the 2-4× latency cost.

**Anti-pattern: Treating Postgres physical standby as a consistent read replica for OLAP queries.** Long-running analytics queries on a hot standby trigger `QueryConflictedWithRecovery` errors and `max_standby_streaming_delay` pauses. Replication pauses while the conflicting query runs. During these pauses, replication lag accumulates. If the query runs for 10 minutes, lag grows to 10 minutes. Solution: use logical replication to a dedicated read replica configured with `hot_standby_feedback=on` (signals the primary not to vacuum rows needed by the standby query) or use a separate OLAP replica with `max_standby_streaming_delay = -1` (never pause replication for queries — instead, cancel the conflicting query).

**Anti-pattern: Setting `replica.lag.time.max.ms` very high to prevent ISR removal.** Raising this threshold means a follower can be 10+ minutes behind and still be in the ISR. When a failover occurs, the follower is elected leader with 10 minutes of missing messages — a massive RPO violation. The correct fix for follower lag is not to raise the ISR removal threshold but to fix the root cause (disk, network, or competing I/O) and alert when lag exceeds the RPO threshold.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pg_stat_replication` | Postgres replication lag monitoring | Shows per-standby lag in bytes and time (replay_lag) |
| `pg_stat_wal_receiver` | Standby-side replication monitoring | Shows received vs. replayed LSN on the standby |
| `pg_last_xact_replay_timestamp()` | Standby lag (time-based) | `now() - pg_last_xact_replay_timestamp()` = lag |
| `kafka-topics.sh --under-replicated-partitions` | Kafka ISR health | Lists partitions where ISR < RF — key health indicator |
| `kafka-topics.sh --describe --topic X` | Kafka partition detail | Shows Leader, Replicas, ISR per partition |
| `kafka-configs.sh --alter --entity-type topics` | Change topic-level replication config | Set min.insync.replicas, unclean.leader.election.enable |
| `nodetool status` | Cassandra node health and replication | Shows node state, ownership, and sync status |
| `nodetool repair` | Cassandra anti-entropy repair | Force reconciliation of replicas after outage |
| `nodetool netstats` | Cassandra streaming progress | Shows active gossip and streaming operations |
| `cqlsh TRACING ON` | Cassandra per-request trace | Shows which nodes were contacted, repair status, latency |
| Patroni REST API | Postgres HA controller | `/primary` returns 200 on primary, 503 on standby |
| `replication_analyzer.py` | Multi-mode replication simulator | Single-leader sim, quorum analysis, conflict detection |

---

## 19. Glossary

**Anti-entropy:** Background process in leaderless systems (Cassandra) that continuously compares replicas and synchronizes differences. Ensures eventual consistency independent of read repair.

**acks (Kafka):** Producer acknowledgment level. `0` = fire and forget; `1` = leader ACK only; `all` (or `-1`) = wait for all ISR members.

**Conflict-free Replicated Data Type (CRDT):** A data structure designed so that concurrent updates can always be merged without conflicts. Examples: counters (take max per node), sets (take union). CRDTs avoid the conflict resolution problem entirely by constraining the data model.

**Failover:** The process of promoting a follower to leader when the current leader fails. In synchronous replication, the promoted node has all committed data (RPO=0). In async replication, there may be data loss.

**Hinted handoff:** In Cassandra, writing data destined for an unavailable node to a temporary hint store on another node, delivering it when the target recovers.

**Hot standby:** A Postgres standby that actively serves read queries while simultaneously replaying WAL from the primary. The default standby configuration in modern Postgres.

**ISR (In-Sync Replicas):** Kafka's set of follower replicas that are within `replica.lag.time.max.ms` of the leader's log end. `acks=all` requires all ISR members to ACK. ISR membership is dynamic.

**Last Write Wins (LWW):** Conflict resolution strategy that uses timestamps to select the winning write among concurrent writes to the same key. Simple but susceptible to clock skew; silently discards one write.

**Leaderless replication:** Replication where any node can accept writes, using quorum overlap (W + R > N) to ensure consistency. Cassandra and DynamoDB use this model.

**Logical replication (Postgres):** Row-level replication that decodes WAL changes into INSERT/UPDATE/DELETE events. Allows partial table replication, cross-version replication, and change data capture (CDC) via Debezium.

**min.insync.replicas:** Kafka topic-level setting that requires at least this many ISR members before `acks=all` writes succeed. Prevents silent ISR=1 scenarios.

**Monotonic reads:** A consistency guarantee: if a user reads value V at time T, they will never read an older value V-1 at a later time T+1. Not guaranteed by default in async single-leader replication with multiple replicas.

**Physical replication (Postgres):** Streaming replication at the storage block level (raw WAL bytes). Creates byte-for-byte replicas. Used for HA standby promotion.

**Read repair:** In leaderless replication, when a read quorum returns different versions, the coordinator updates stale replicas with the latest version. Ensures eventual convergence.

**Read-your-own-writes:** A consistency guarantee: after a write, the same client always reads the written value. Not guaranteed when reads are routed to async replicas.

**Replication lag:** The delay between a write on the leader and its visibility on a follower. Measured in time (seconds) or log position (bytes behind).

**Semi-synchronous replication:** Waits for a subset of replicas (typically one) to ACK before returning success to the client. Balances durability (RPO near-0) with write latency (one RTT penalty).

**Sloppy quorum:** Writing to available nodes outside the preferred replica set when the preferred set has insufficient members. Improves availability at the cost of temporary inconsistency.

**Synchronous replication:** The leader waits for a quorum of replicas to ACK before returning success. Guarantees RPO=0 at the cost of write latency.

**Unclean leader election (Kafka):** Allowing an out-of-sync replica to become leader when all ISR members are unavailable. Restores availability at the risk of data loss.

**Version vector:** A per-node sequence number vector tracking causal history. Two version vectors that are neither ≥ nor ≤ to each other indicate a true concurrent write (conflict). Used for accurate conflict detection.

**WAL (Write-Ahead Log):** The durability log in Postgres (and other databases) where changes are written before being applied to data files. The source of truth for replication.

---

## 20. Self-Assessment

1. A Postgres cluster uses async streaming replication. The primary crashes. The standby is 45 seconds behind. What is the RPO? What data is lost?
2. Kafka topic has RF=3, `acks=all`, `min.insync.replicas=2`. Two followers crash. What happens to new produce requests? What is the correct operational response?
3. Cassandra cluster: N=3, W=2, R=1. Does W+R > N? Will reads always see the latest write? What consistency level does this correspond to?
4. A user in Germany updates their email in an EU Cassandra datacenter using `LOCAL_QUORUM`. A background job in the US datacenter reads the same row using `LOCAL_QUORUM` 100ms later and writes the old email back. Using LWW, which write wins? How would you prevent this?
5. What is the difference between `synchronous_commit = on` and `synchronous_commit = remote_apply` in Postgres? When would you choose `remote_apply`?
6. A Kafka topic with `acks=1` has the leader crash immediately after ACKing a produce request. Was the message guaranteed to be replicated? Will it be present after leader failover?
7. Describe the read-your-own-writes problem and give two solutions. Which solution would you use for a high-traffic user profile service?
8. Why is `unclean.leader.election.enable=true` dangerous? In what scenario might you temporarily enable it, and what steps would you take before and after?
9. Your Cassandra cluster has three datacenters, each with 3 nodes (total 9 nodes). What is the quorum for `QUORUM` consistency? What is the quorum for `LOCAL_QUORUM` in one datacenter? Which do you use for production writes and why?
10. A Postgres hot standby is falling behind by 3 minutes. You check `pg_stat_activity` on the standby and see a long-running analytics query. What is causing the lag and how do you fix it without affecting the analytics workload?

---

## 21. Module Summary

Replication is the mechanism by which distributed systems achieve fault tolerance, read scalability, and low-latency reads. The core tension — consistency requires synchronous coordination, performance requires asynchronous independence — drives every configuration decision in every data system.

Single-leader replication is the dominant model in data engineering. One node accepts writes; followers replicate changes. The critical variables are synchrony (synchronous waits for follower ACK, guaranteeing RPO=0; asynchronous doesn't wait, accepting non-zero RPO for lower latency) and durability (how many replicas must acknowledge before success). Kafka's `acks=all` + `min.insync.replicas=2` and Postgres's `synchronous_commit=on` are the production-correct implementations of synchronous replication for their respective systems.

Replication lag — the delay between write on leader and visibility on follower — causes three consistency anomalies: read-your-own-writes, monotonic reads, and consistent prefix reads. Each has well-understood solutions, but they require explicit handling in application code. The absence of this handling is one of the most common sources of subtle, production-only bugs in data-intensive applications.

Multi-leader and leaderless replication sacrifice simplicity for availability and geo-distribution capability. Multi-leader enables active-active writes in multiple data centers but introduces conflict resolution: Last Write Wins silently discards concurrent writes, CRDTs avoid conflicts through data structure design, and application-level resolution provides maximum control at maximum complexity. Leaderless (Cassandra) uses quorum overlap (W + R > N) to ensure reads see the latest write, with tunable trade-offs between consistency level and fault tolerance.

The next module — M40: Failure Modes — examines what happens when these replication strategies encounter adversarial conditions: split-brain, network partitions, partial failures, and cascading failures. Understanding replication failure modes requires the foundation built in this module — every failure mode in distributed systems is ultimately a failure of the replication or consensus mechanism.
