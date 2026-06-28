# M44: Reading Real Failure Reports

**Course:** SYS-DST-102 — Distributed Systems in Practice  
**Module:** 03 of 05  
**Global Module ID:** M44  
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

Distributed systems theory teaches what *can* go wrong. Implementation teaches what it *looks like* at the code level. But neither teaches you to diagnose a failure from its observable symptoms — the incomplete timeline, the ambiguous logs, the contradictory metrics — the way it actually appears during an incident.

Reading real failure reports fills that gap. An incident postmortem is a document written by engineers who experienced a failure and reconstructed what happened. The reconstruction is always incomplete (distributed systems leave partial evidence), the timeline is always approximate, and the root cause is always stated in terms of the specific system that failed. Your job as a reader is to translate from "Kafka ISR shrinkage caused consumer lag" into the precise distributed systems concepts — replication lag, ISR threshold, follower disk saturation — that explain *why* the observed symptoms appeared.

This skill is worth developing for three reasons. First, you will be on-call and you will see failures described in incident channels in exactly this form. The faster you can map symptoms to root causes, the faster you can apply the correct mitigation. Second, real postmortems are the only source of empirical evidence about how distributed systems actually fail in production — not in contrived lab scenarios, but under real load, with real bugs, real configurations, and real human errors. Third, interviews increasingly include "read this failure scenario and explain what happened" questions at Staff level. Candidates who have read and analyzed real postmortems answer these questions with specificity that candidates who only studied theory cannot match.

This module analyzes five real or composite failure reports: Kafka ISR collapse, ZooKeeper session expiry cascade, Cassandra network partition with stale quorum reads, Postgres replication slot preventing WAL cleanup, and a distributed transaction failure from the 2PC blocking problem. For each, we extract the precise distributed systems mechanism, the observability signatures, and the mitigation.

---

## 2. Mental Model

### The Failure Analysis Framework

When reading a failure report, apply this five-step framework:

1. **Classify the failure type.** Which category from M40 does this belong to? Network partition? Crash failure? Gray failure? Cascading failure? Multiple simultaneous types?

2. **Identify the violated invariant.** Every distributed systems failure violates a specific invariant. ISR collapse violates the durability invariant (min.insync.replicas). ZooKeeper session expiry triggers a reconfiguration cascade. Postgres replication slot retention violates the WAL cleanup assumption. Name the invariant explicitly.

3. **Trace the causation chain.** A-caused-B-caused-C-caused-D is the structure of most failures. The root cause is A; the observable symptom is D. The postmortem often starts with D and works backward. Read forward (from A to D) to understand; work backward (from D to A) during an incident.

4. **Map to metrics.** What metric would have been anomalous before the failure was user-visible? This is the early warning that should have triggered an alert. Postmortems that don't include "and here is the alert that would have caught this earlier" are incomplete.

5. **State the prevention.** Not just "we fixed the bug" — what configuration change, architectural change, or operational practice would prevent this class of failure?

### The Difference Between Root Cause and Proximate Cause

The proximate cause is the immediate trigger of the failure (a broker's disk filled up). The root cause is the underlying condition that made the system vulnerable (replication slot preventing WAL cleanup). Postmortems that only identify proximate causes produce fixes that prevent the exact same incident from recurring but don't prevent the next variant. Root cause analysis produces fixes that prevent the class of failure.

---

## 3. Core Concepts

### 3.1 Kafka ISR Collapse

**The mechanism (from M39):** Kafka's In-Sync Replica set contains followers whose lag (time since last fetch from leader) is within `replica.lag.time.max.ms`. If a follower's disk or network slows, it falls behind this threshold and is removed from ISR. With `acks=all` (exactly-once, strong durability), the producer must wait for all ISR members to ACK. If ISR shrinks to 1 (only the leader), `acks=all` is effectively `acks=1` — no replication durability.

**The dangerous configuration combination:**
- `acks=all`: strong durability requirement
- `min.insync.replicas=1`: allows ISR=1 without rejecting producers
- `unclean.leader.election.enable=true`: allows an out-of-sync replica to become leader on failover

With this combination: ISR collapses to leader, leader crashes, unclean leader is elected (missing entries), producers already got success ACKs for those entries. Data loss.

**The safe defaults:**
- `acks=all`
- `min.insync.replicas=2` (rejects producers when ISR < 2)
- `unclean.leader.election.enable=false`

### 3.2 ZooKeeper Session Expiry Cascade

**The mechanism:** ZooKeeper clients (Kafka brokers, HDFS NameNode) maintain a session with the ZooKeeper ensemble via periodic heartbeats. If a client doesn't send a heartbeat within its session timeout (typically 6-30 seconds), ZooKeeper expires the session and deletes all ephemeral nodes owned by that client. In Kafka, the broker's registration in ZooKeeper is an ephemeral node — session expiry triggers broker deregistration, partition reassignment, and rebalancing.

**The cascade:** If ZooKeeper itself becomes slow (GC pause, disk I/O) and delays session heartbeat acknowledgment, all clients simultaneously fail to get their heartbeats ACK'd in time. All sessions expire simultaneously. Every broker deregisters. The controller triggers a massive partition reassignment. Brokers reconnect and re-register. The controller processes thousands of partition state changes. Controller itself becomes overloaded and slow. More timeouts. The cluster takes minutes to restabilize.

**Why KRaft helps:** By replacing ZooKeeper with Raft-based metadata management inside Kafka itself, KRaft eliminates the external dependency. A JVM GC pause in ZooKeeper can't cause a cascade of Kafka broker deregistrations because Kafka no longer uses ZooKeeper for liveness.

### 3.3 Cassandra Stale Quorum Read

**The mechanism (from M39):** Cassandra uses leaderless replication with quorum reads (QUORUM consistency: R+W > N). With N=3, W=2, R=2, the write quorum and read quorum overlap by at least 1 node — the overlapping node always has the latest write.

**The failure mode:** A network partition separates Cassandra into two groups. Group A (2 nodes) and Group B (1 node). A write with CL=QUORUM succeeds on Group A (2/3 nodes = quorum). The partition heals. A read with CL=QUORUM executes — it happens to hit Group B's node (1 replica) + one Group A node. The Group B node has an old value (the write went to Group A during the partition). Both are returning values, but the old-value node's timestamp may be newer than expected due to clock skew, causing the old value to "win" the read repair comparison.

**The root cause:** Clock skew combined with network partition. If the Group B node's wall clock is ahead by even a few seconds, its older value has a newer timestamp, and LWW (Last Write Wins — Cassandra's default conflict resolution) selects the wrong value. Without hybrid logical clocks or a coordination mechanism, this is unavoidable in a leaderless system.

### 3.4 Postgres Replication Slot WAL Retention

**The mechanism:** Postgres replication slots guarantee that WAL segments are retained until the slot's consumer has confirmed reading them. This prevents the primary from deleting WAL that a replica still needs, even if the replica is offline for extended periods.

**The failure mode:** A streaming replica goes offline unexpectedly. The primary continues to receive writes. WAL segments accumulate because the slot prevents their deletion. After hours or days, the primary's disk fills with undeleted WAL segments. When the disk reaches 100%, the primary can no longer write new WAL — all writes block, and the Postgres cluster stops accepting transactions entirely. The entire application goes down because of an offline read replica.

**The metrics signature:**
```sql
-- Monitor slot WAL retention on the primary
SELECT
    slot_name,
    restart_lsn,
    confirmed_flush_lsn,
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_size
FROM pg_replication_slots
WHERE active = false;   -- Inactive slots are the dangerous ones
```

**The prevention:** `max_slot_wal_keep_size` (Postgres 13+): the maximum WAL size a slot can retain. When a slot's retained WAL exceeds this size, the slot is invalidated rather than allowing disk exhaustion.

### 3.5 2PC Coordinator Crash (Blocking Problem)

**The mechanism (from M41):** In two-phase commit, if the coordinator crashes after sending PREPARE but before sending COMMIT/ABORT, all participants are in-doubt: they've acquired locks and voted YES, but don't know whether to commit or abort. They cannot abort unilaterally (the coordinator might have logged COMMIT before crashing). They cannot commit unilaterally (other participants might have voted NO). They wait, holding locks.

**The production manifestation:** A distributed transaction coordinator crashes. 15 minutes later, a DBA notices that a database table has rows in PENDING state. The queries against those rows are blocking on lock wait. `pg_prepared_xacts` shows prepared transactions from 15 minutes ago. `pg_locks` shows these transactions holding row locks. Application queries are queuing behind the locks. The application appears hung.

**The recovery procedure:**
```sql
-- Find stuck prepared transactions
SELECT gid, prepared, owner, database
FROM pg_prepared_xacts
WHERE prepared < now() - interval '5 minutes';

-- In an application with known transaction semantics:
-- If you can determine the intended outcome:
COMMIT PREPARED 'transaction-id-from-coordinator-log';
-- or
ROLLBACK PREPARED 'transaction-id-from-coordinator-log';

-- After rollback: release locks, application can proceed
-- Must be done by the DBA — no automated recovery without coordinator restart
```

---

## 4. Hands-On Walkthrough

### 4.1 Reconstructing an ISR Collapse from Metrics

```bash
# ── Simulated Timeline ──────────────────────────────────────────────────────────
# 09:00 - Normal operation: ISR=[broker-0, broker-1, broker-2] for topic "orders"
# 09:03 - broker-2 GC pause starts (JVM full GC, ~8 seconds)
# 09:03 - broker-2 falls behind replica.lag.time.max.ms (default 10s)
# 09:03:10 - broker-2 removed from ISR: ISR=[broker-0, broker-1]
# 09:03:11 - GC pause ends. broker-2 reconnects and catches up
# 09:03:15 - broker-2 re-added to ISR: ISR=[broker-0, broker-1, broker-2]
# 09:04 - broker-1 disk saturation (backup job)
# 09:04:10 - broker-1 removed from ISR: ISR=[broker-0, broker-2]
# 09:07 - broker-1 disk saturation continues
# 09:07:10 - broker-2 GC pause again. ISR=[broker-0] ← ALERT: ISR=1
# 09:07:11 - If min.insync.replicas=1: producers continue, all acks go to broker-0 alone
# 09:07:30 - broker-0 crashes
# 09:07:31 - WITH unclean.leader.election.enable=false: topic goes offline (unavailable)
#            WITH unclean.leader.election.enable=true: broker-1 or broker-2 elected
#            (neither has last ~30s of data → DATA LOSS)

# ── What Kafka metrics showed ────────────────────────────────────────────────────
# kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
#   09:03-09:03:15: value=1 (brief ISR shrink)
#   09:04+: value=1 (persistent, broker-1 lagging)
#   09:07+: value=3 (CRITICAL: multiple partitions now under-replicated)

# kafka.server:type=ReplicaManager,name=IsrShrinksPerSec
#   Spikes at 09:03:10, 09:04:10, 09:07:10

# kafka.server:type=ReplicaManager,name=IsrExpandsPerSec
#   Spike at 09:03:15 (broker-2 rejoins)
#   No spike after 09:04:10 (broker-1 never rejoins)

# ── What the alert should have been ─────────────────────────────────────────────
# Alert: UnderReplicatedPartitions > 0 for > 60 seconds
#   Would have fired at 09:05:10 (broker-1 lagging 60s)
#   Gives 2 minutes before the catastrophic ISR=1 moment

# ── Kafka CLI to check ISR state ──────────────────────────────────────────────
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders
# Topic: orders  PartitionCount: 8  ReplicationFactor: 3
# Topic: orders  Partition: 0  Leader: 0  Replicas: 0,1,2  Isr: 0,2
#                                                            ^ broker-1 missing from ISR!

kafka-topics.sh --bootstrap-server localhost:9092 \
  --under-replicated-partitions
# Topic: orders  Partition: 0  Leader: 0  Replicas: 0,1,2  Isr: 0,2
# Topic: orders  Partition: 3  Leader: 1  Replicas: 1,2,0  Isr: 1,0
# ... lists all partitions with ISR < RF
```

### 4.2 Diagnosing Postgres Replication Slot WAL Accumulation

```sql
-- ── Early warning detection ──────────────────────────────────────────────────────
-- Check replication slot WAL retention (run on PRIMARY)
SELECT
    slot_name,
    slot_type,
    active,
    active_pid,
    restart_lsn,
    confirmed_flush_lsn,
    -- Bytes of WAL retained by this slot
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS wal_retained_bytes,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_retained_pretty
FROM pg_replication_slots
ORDER BY wal_retained_bytes DESC;

-- Expected output when healthy:
-- slot_name  | active | wal_retained_bytes | wal_retained_pretty
-- standby-1  | t      | 2097152            | 2048 kB      ← small, active
-- debezium   | t      | 52428800           | 50 MB        ← small, active CDC slot

-- Expected output when slot is problematic:
-- standby-1  | f      | 32212254720        | 30 GB        ← DANGER: inactive, 30GB retained

-- ── Alert threshold ──────────────────────────────────────────────────────────────
-- Alert: Any slot with active=false and wal_retained_bytes > 5GB
-- This gives enough time to investigate before disk exhaustion

-- ── Protection (Postgres 13+) ────────────────────────────────────────────────────
-- In postgresql.conf:
-- max_slot_wal_keep_size = '10GB'
-- When exceeded, slot is invalidated (not deleted — it errors, preventing further WAL retention)

-- After setting max_slot_wal_keep_size, check for invalidated slots:
SELECT slot_name, invalidation_reason
FROM pg_replication_slots
WHERE invalidation_reason IS NOT NULL;
-- slot_name | invalidation_reason
-- standby-1 | wal_removed
-- (Slot is invalidated — standby must do a full resync)

-- ── Checking disk pressure from WAL retention ─────────────────────────────────
-- Check current WAL directory size
-- (Run in bash on the primary server)
du -sh /var/lib/postgresql/14/main/pg_wal/
# 31G   /var/lib/postgresql/14/main/pg_wal/  ← 31GB of WAL files!

-- Check disk free space
df -h /var/lib/postgresql/
# Filesystem      Size  Used Avail Use%
# /dev/nvme0n1     50G   48G  1.5G  97%   ← 97% full!
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
failure_report_analyzer.py

Tools for reading and analyzing distributed system failure reports.

Components:
  - FailureReport: structured representation of a postmortem
  - FailureClassifier: maps symptoms to failure types (M40 taxonomy)
  - ISRCollapseAnalyzer: reconstructs ISR state from a metric timeline
  - ReplicationSlotChecker: models Postgres WAL retention
  - TwoPCRecoveryAdvisor: identifies stuck prepared transactions
  - PostmortemTemplate: generates a structured postmortem document

No external dependencies.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime


# ─── Failure Classification (from M40 taxonomy) ──────────────────────────────────

class FailureType(Enum):
    CRASH            = "Crash Failure"       # Node stops and doesn't recover
    GRAY             = "Gray Failure"        # Node alive but degraded
    NETWORK_PARTITION = "Network Partition"  # Nodes can't communicate
    CASCADING        = "Cascading Failure"   # Failure propagates
    SPLIT_BRAIN      = "Split Brain"         # Two nodes believe they are authoritative
    BYZANTINE        = "Byzantine Failure"   # Node sends conflicting messages
    OMISSION         = "Omission Failure"    # Messages dropped without error
    TIMING           = "Timing Failure"      # Response outside expected time bounds


@dataclass
class FailureEvent:
    """A single event in a failure timeline."""
    time_offset_s: float   # Seconds from incident start
    description:   str
    metrics:       dict = field(default_factory=dict)
    is_root_cause: bool = False
    is_user_visible: bool = False


@dataclass
class FailureReport:
    """
    Structured representation of a distributed system failure.
    Maps to the sections of a standard postmortem document.
    """
    title:           str
    system:          str                    # e.g., "Kafka", "Postgres", "etcd"
    duration_s:      float                  # Total incident duration
    failure_types:   list[FailureType]
    timeline:        list[FailureEvent]
    root_cause:      str                    # Brief root cause statement
    proximate_cause: str                    # Immediate trigger
    violated_invariant: str                 # Which distributed systems guarantee was broken
    user_impact:     str
    metrics_signatures: dict                # key: metric name, value: expected anomalous value
    prevention:      list[str]              # List of preventive measures
    detection_gap_s: float = 0.0            # How long before the incident was detected

    def format(self) -> str:
        lines = [
            f"=== FAILURE REPORT: {self.title} ===",
            f"System: {self.system}",
            f"Duration: {self.duration_s / 60:.1f} minutes",
            f"Failure types: {', '.join(ft.value for ft in self.failure_types)}",
            "",
            "─── VIOLATED INVARIANT ───",
            f"  {self.violated_invariant}",
            "",
            "─── ROOT CAUSE ───",
            f"  {self.root_cause}",
            "",
            "─── PROXIMATE CAUSE ───",
            f"  {self.proximate_cause}",
            "",
            "─── TIMELINE ───",
        ]
        for event in sorted(self.timeline, key=lambda e: e.time_offset_s):
            prefix = "ROOT→" if event.is_root_cause else "USER→" if event.is_user_visible else "     "
            lines.append(f"  {prefix} T+{event.time_offset_s:5.0f}s: {event.description}")
            for k, v in event.metrics.items():
                lines.append(f"          metric: {k} = {v}")
        lines += [
            "",
            "─── METRICS SIGNATURES (what should have alerted) ───",
        ]
        for metric, value in self.metrics_signatures.items():
            lines.append(f"  {metric}: {value}")
        lines += [
            "",
            f"─── DETECTION GAP: {self.detection_gap_s / 60:.1f} minutes ───",
            "",
            "─── PREVENTION ───",
        ]
        for p in self.prevention:
            lines.append(f"  • {p}")
        lines.append(f"\nUser impact: {self.user_impact}")
        return "\n".join(lines)


# ─── ISR Collapse Analyzer ───────────────────────────────────────────────────────

@dataclass
class BrokerState:
    """Simulated state of a Kafka broker at a point in time."""
    broker_id:   int
    alive:       bool = True
    disk_ok:     bool = True
    gc_pausing:  bool = False
    fetch_lag_s: float = 0.0   # Seconds behind the leader

    @property
    def in_isr_candidate(self) -> bool:
        """Would this broker be in ISR?"""
        return (self.alive and self.disk_ok and
                not self.gc_pausing and
                self.fetch_lag_s < 10.0)   # replica.lag.time.max.ms = 10s


class ISRCollapseAnalyzer:
    """
    Simulates ISR state over time given a sequence of broker events.
    Useful for reconstructing what happened during an ISR collapse.
    """

    def __init__(self, brokers: list[int], replication_factor: int = 3,
                 min_insync_replicas: int = 2,
                 unclean_election_enabled: bool = False):
        self.replication_factor = replication_factor
        self.min_insync_replicas = min_insync_replicas
        self.unclean_election_enabled = unclean_election_enabled
        self.brokers: dict[int, BrokerState] = {b: BrokerState(b) for b in brokers}
        self._history: list[tuple[float, list[int], str]] = []
        self._t = 0.0

    def advance(self, dt_s: float, event_description: str = "",
                broker_mutations: dict[int, dict] = None):
        """
        Advance simulation time by dt_s seconds, applying broker state mutations.
        broker_mutations: {broker_id: {'alive': False, 'gc_pausing': True, ...}}
        """
        self._t += dt_s
        if broker_mutations:
            for bid, changes in broker_mutations.items():
                if bid in self.brokers:
                    for k, v in changes.items():
                        setattr(self.brokers[bid], k, v)

        isr = self._compute_isr()
        self._history.append((self._t, isr, event_description))
        return isr

    def _compute_isr(self) -> list[int]:
        return [bid for bid, b in self.brokers.items() if b.in_isr_candidate]

    def analyze_risk(self, isr: list[int]) -> dict:
        """Analyze the risk level of a given ISR state."""
        isr_size = len(isr)
        risk = {
            'isr_size': isr_size,
            'produces_available': isr_size >= self.min_insync_replicas,
            'durability_risk': 'NONE',
            'unclean_election_risk': 'NONE',
        }
        if isr_size == 0:
            risk['durability_risk'] = 'CATASTROPHIC: No replicas'
        elif isr_size == 1:
            risk['durability_risk'] = 'HIGH: Single point of failure'
            if self.unclean_election_enabled:
                risk['unclean_election_risk'] = 'HIGH: Data loss on broker failure'
        elif isr_size < self.replication_factor:
            risk['durability_risk'] = 'MEDIUM: Under-replicated'

        return risk

    def print_timeline(self):
        print("\n=== ISR Collapse Timeline ===")
        print(f"{'Time':>7}  {'ISR':30}  {'Risk':30}  {'Event'}")
        print("-" * 100)
        for t, isr, event in self._history:
            risk = self.analyze_risk(isr)
            risk_str = risk['durability_risk']
            isr_str = str(isr)
            print(f"T+{t:5.0f}s  {isr_str:30}  {risk_str:30}  {event}")

    def get_dangerous_windows(self) -> list[tuple[float, float, str]]:
        """Return time windows where ISR was at dangerous levels."""
        windows = []
        in_danger = False
        danger_start = 0.0
        danger_reason = ""
        for t, isr, event in self._history:
            risk = self.analyze_risk(isr)
            is_dangerous = risk['durability_risk'] != 'NONE'
            if is_dangerous and not in_danger:
                in_danger = True
                danger_start = t
                danger_reason = risk['durability_risk']
            elif not is_dangerous and in_danger:
                in_danger = False
                windows.append((danger_start, t, danger_reason))
        if in_danger:
            windows.append((danger_start, self._t, danger_reason))
        return windows


# ─── Replication Slot Checker ─────────────────────────────────────────────────────

@dataclass
class ReplicationSlot:
    """Models a Postgres replication slot's WAL retention behavior."""
    name:                 str
    active:               bool
    restart_lsn_bytes:    int   # Simulated restart LSN (as absolute byte offset)
    last_confirmed_bytes: int   # Last LSN confirmed by consumer
    max_slot_wal_keep:    int = -1  # -1 = unlimited (dangerous)

    @property
    def retained_bytes(self) -> int:
        return max(0, self.restart_lsn_bytes)  # In real Postgres: current_lsn - restart_lsn

    def is_dangerous(self, disk_total_bytes: int, current_lsn_bytes: int) -> dict:
        retained = current_lsn_bytes - self.restart_lsn_bytes
        percent_disk = retained / disk_total_bytes * 100
        return {
            'slot_name': self.name,
            'active': self.active,
            'retained_bytes': retained,
            'retained_gb': retained / 1024**3,
            'disk_percent': percent_disk,
            'dangerous': not self.active and percent_disk > 10,
            'will_fill_disk': retained > disk_total_bytes * 0.8,
        }

    def should_invalidate(self, current_lsn_bytes: int) -> bool:
        """
        Returns True if max_slot_wal_keep would trigger slot invalidation.
        """
        if self.max_slot_wal_keep < 0:
            return False   # Unlimited retention (dangerous)
        retained = current_lsn_bytes - self.restart_lsn_bytes
        return retained > self.max_slot_wal_keep


class ReplicationSlotChecker:
    """Simulates Postgres WAL retention across multiple replication slots."""

    def __init__(self, disk_size_gb: float = 100.0):
        self.disk_size_bytes = int(disk_size_gb * 1024**3)
        self.slots: dict[str, ReplicationSlot] = {}
        self.current_lsn_bytes: int = 0
        self.wal_growth_rate_bytes_per_s: float = 1024**2 * 10  # 10MB/s

    def add_slot(self, name: str, active: bool = True,
                 max_slot_wal_keep_gb: float = -1):
        """Add a replication slot at the current LSN position."""
        max_bytes = int(max_slot_wal_keep_gb * 1024**3) if max_slot_wal_keep_gb > 0 else -1
        slot = ReplicationSlot(
            name=name,
            active=active,
            restart_lsn_bytes=self.current_lsn_bytes,
            last_confirmed_bytes=self.current_lsn_bytes,
            max_slot_wal_keep=max_bytes,
        )
        self.slots[name] = slot

    def deactivate_slot(self, name: str):
        """Simulate a replica going offline (slot becomes inactive)."""
        if name in self.slots:
            self.slots[name].active = False
            print(f"  [ReplicationSlot] {name} deactivated "
                  f"(restart_lsn frozen at {self.current_lsn_bytes:,} bytes)")

    def advance_time(self, seconds: float):
        """Advance WAL production by the given number of seconds."""
        self.current_lsn_bytes += int(self.wal_growth_rate_bytes_per_s * seconds)

    def analyze_slots(self) -> list[dict]:
        return [
            slot.is_dangerous(self.disk_size_bytes, self.current_lsn_bytes)
            for slot in self.slots.values()
        ]

    def print_status(self):
        total_retained = self.current_lsn_bytes - min(
            (s.restart_lsn_bytes for s in self.slots.values()),
            default=0,
        )
        disk_used_percent = total_retained / self.disk_size_bytes * 100
        print(f"\n  Current LSN offset: {self.current_lsn_bytes / 1024**3:.1f} GB")
        print(f"  Disk used by WAL: {total_retained / 1024**3:.1f} GB "
              f"({disk_used_percent:.1f}% of {self.disk_size_bytes/1024**3:.0f}GB disk)")
        for analysis in self.analyze_slots():
            status = "⚠ DANGEROUS" if analysis['dangerous'] else "OK"
            print(f"  Slot {analysis['slot_name']}: "
                  f"retained={analysis['retained_gb']:.1f}GB "
                  f"active={analysis['active']} {status}")
            if analysis['will_fill_disk']:
                print(f"    !! WILL FILL DISK: {analysis['retained_bytes']:,} bytes retained")


# ─── Postmortem Template Generator ───────────────────────────────────────────────

class PostmortemTemplate:
    """Generates a structured postmortem document from a FailureReport."""

    def __init__(self, report: FailureReport):
        self.report = report

    def generate(self) -> str:
        r = self.report
        sections = [
            f"# Incident Postmortem: {r.title}",
            f"**Date:** {datetime.now().strftime('%Y-%m-%d')}",
            f"**System:** {r.system}",
            f"**Duration:** {r.duration_s / 60:.0f} minutes",
            f"**Severity:** {'P0' if r.duration_s > 3600 else 'P1' if r.duration_s > 600 else 'P2'}",
            "",
            "## User Impact",
            r.user_impact,
            "",
            "## Timeline",
            "",
            "| Time | Event | Root Cause? | User Visible? |",
            "|------|-------|-------------|--------------|",
        ]
        for event in sorted(r.timeline, key=lambda e: e.time_offset_s):
            t = f"T+{event.time_offset_s:.0f}s"
            rc = "✓" if event.is_root_cause else ""
            uv = "✓" if event.is_user_visible else ""
            sections.append(f"| {t} | {event.description} | {rc} | {uv} |")

        sections += [
            "",
            "## Root Cause Analysis",
            "",
            f"**Root cause:** {r.root_cause}",
            f"**Proximate cause:** {r.proximate_cause}",
            f"**Violated invariant:** {r.violated_invariant}",
            f"**Failure type(s):** {', '.join(ft.value for ft in r.failure_types)}",
            "",
            "## Why This Wasn't Detected Sooner",
            f"Detection gap: {r.detection_gap_s / 60:.1f} minutes. "
            "The following metrics were anomalous but not alerted on:",
            "",
            "| Metric | Anomalous Value | Should Have Alerted? |",
            "|--------|-----------------|---------------------|",
        ]
        for metric, value in r.metrics_signatures.items():
            sections.append(f"| {metric} | {value} | NO (no alert configured) |")

        sections += [
            "",
            "## Prevention",
            "",
        ]
        for i, p in enumerate(r.prevention, 1):
            sections.append(f"{i}. {p}")

        return "\n".join(sections)


# ─── Pre-Built Failure Reports ────────────────────────────────────────────────────

def kafka_isr_collapse_report() -> FailureReport:
    """Composite failure report: Kafka ISR collapse leading to data loss."""
    return FailureReport(
        title="Kafka Orders Topic Data Loss via ISR Collapse",
        system="Kafka 2.8 (ZooKeeper mode), 3 brokers, RF=3",
        duration_s=1800,   # 30 minutes
        failure_types=[FailureType.GRAY, FailureType.CASCADING],
        user_impact="Order events produced between 09:07:30 and 09:37:00 were silently lost. "
                    "~4,200 order events not processed. Downstream inventory service became inconsistent.",
        violated_invariant=(
            "acks=all should guarantee that a write is on all ISR members before ACK. "
            "With min.insync.replicas=1 and unclean.leader.election.enable=true, "
            "ISR=1 → acks=all degrades to acks=1 → data on single broker lost on crash."
        ),
        root_cause=(
            "min.insync.replicas=1 (default) allowed ISR to shrink to 1 without rejecting producers. "
            "Combined with unclean.leader.election.enable=true, this permitted a lagging "
            "replica (broker-1) to become leader after broker-0 crashed, "
            "discarding ~30s of unconfirmed writes."
        ),
        proximate_cause="broker-0 disk failure at 09:07:30, when ISR was already 1 (only broker-0).",
        timeline=[
            FailureEvent(0, "Normal operation. ISR=[0,1,2]"),
            FailureEvent(180, "broker-2 JVM GC pause (8s). ISR=[0,1]",
                         metrics={'IsrShrinks/sec': 1}),
            FailureEvent(195, "broker-2 rejoins ISR. ISR=[0,1,2]"),
            FailureEvent(240, "broker-1 disk saturation (backup job). ISR=[0,2]",
                         metrics={'UnderReplicatedPartitions': 8},
                         is_root_cause=True),
            FailureEvent(420, "broker-2 GC pause again. ISR=[0] ← SINGLE REPLICA",
                         metrics={'UnderReplicatedPartitions': 24, 'IsrShrinksPerSec': 8}),
            FailureEvent(450, "broker-0 disk failure. Leader election triggered.", is_user_visible=True),
            FailureEvent(455, "Unclean election: broker-1 elected (lagging 30s of data)",
                         metrics={'LeaderElections': 8}),
            FailureEvent(457, "Consumers receive orders from 30s ago (duplicated). "
                              "Orders after 09:07:00 missing.", is_user_visible=True),
        ],
        metrics_signatures={
            'kafka.server:UnderReplicatedPartitions': '>0 for 60s → ALERT',
            'kafka.server:IsrShrinksPerSec': '>0 sustained → ALERT',
            'broker-1:disk_write_await_ms': '> 20ms (gray failure, not caught)',
        },
        detection_gap_s=270,  # 4.5 minutes before alert
        prevention=[
            "Set min.insync.replicas=2 for all production topics",
            "Set unclean.leader.election.enable=false (default in Kafka 2.8+)",
            "Alert on UnderReplicatedPartitions > 0 sustained for > 60 seconds",
            "Alert on disk_write_await_ms > 20ms on any broker (gray failure detection)",
        ],
    )


def postgres_slot_retention_report() -> FailureReport:
    """Composite failure report: Postgres replication slot WAL retention → disk full."""
    return FailureReport(
        title="Postgres Primary Outage: Replication Slot WAL Retention",
        system="Postgres 12, primary + 2 standbys + 1 Debezium CDC slot",
        duration_s=3600,
        failure_types=[FailureType.CRASH, FailureType.TIMING],
        user_impact="All database writes blocked for 60 minutes. Application returned 500 errors. "
                    "10,000 failed requests. Downstream CDC pipeline 4 hours behind after recovery.",
        violated_invariant=(
            "Replication slots guarantee WAL retention for offline consumers. "
            "Without max_slot_wal_keep_size, this guarantee has no upper bound — "
            "an offline replica can cause the primary to fill its disk and stop accepting writes entirely."
        ),
        root_cause=(
            "A Debezium CDC connector was deployed with a bug causing it to pause consuming. "
            "Its replication slot (logical) stopped advancing, retaining all WAL produced since the pause. "
            "No alert on slot WAL retention. Disk filled in 6 hours."
        ),
        proximate_cause="Disk 100% full on primary → Postgres WAL write error → all transactions blocked.",
        timeline=[
            FailureEvent(0, "Debezium connector deployed with pagination bug → stops consuming"),
            FailureEvent(60, "Replication slot 'debezium-orders' stops advancing",
                         metrics={'slot_wal_retained_bytes': '0 (normal at start)'},
                         is_root_cause=True),
            FailureEvent(3600, "Slot retains 36GB WAL (10MB/s × 3600s)",
                         metrics={'slot_wal_retained_gb': '36'}),
            FailureEvent(19800, "Disk reaches 95% full",
                         metrics={'disk_used_pct': '95%'}),
            FailureEvent(21600, "Disk reaches 100%. WAL write fails.",
                         metrics={'disk_used_pct': '100%', 'pg_wal_write_errors': 1},
                         is_user_visible=True),
            FailureEvent(21605, "All transactions blocked. Application returns errors.",
                         is_user_visible=True),
            FailureEvent(25200, "On-call engineer identified slot issue, dropped slot",
                         metrics={'disk_freed_gb': '36'}),
            FailureEvent(25260, "Postgres writes resume. Debezium resync required."),
        ],
        metrics_signatures={
            'pg_replication_slot_wal_retained_bytes{active=false}': '> 5GB → ALERT',
            'disk_used_pct': '> 80% → WARNING, > 90% → ALERT',
            'pg_replication_slot_active{slot=debezium-orders}': '= 0 for > 5min → ALERT',
        },
        detection_gap_s=21600,  # 6 hours without detection
        prevention=[
            "Set max_slot_wal_keep_size = '10GB' in postgresql.conf (Postgres 13+)",
            "Alert on inactive replication slot WAL retention > 5GB",
            "Alert on replication slot active=false for > 5 minutes",
            "Alert on disk_used_pct > 80% on Postgres data volume",
            "Monitor Debezium connector lag (kafka.connect.source.task.metrics.offset-lag)",
        ],
    )


# ─── Demo ─────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Failure Report Analysis Demo ===\n")

    # ── 1. ISR Collapse Reconstruction ──────────────────────────────────────────
    print("─── ISR Collapse Reconstruction ───")
    analyzer = ISRCollapseAnalyzer(
        brokers=[0, 1, 2],
        replication_factor=3,
        min_insync_replicas=1,        # Dangerous default
        unclean_election_enabled=True,  # Dangerous default
    )
    analyzer.advance(0, "Normal operation")
    analyzer.advance(180, "Broker-2 GC pause",
                     broker_mutations={2: {'gc_pausing': True}})
    analyzer.advance(10, "Broker-2 GC still pausing")
    analyzer.advance(5, "Broker-2 GC ends",
                     broker_mutations={2: {'gc_pausing': False}})
    analyzer.advance(45, "Broker-1 disk saturation",
                     broker_mutations={1: {'disk_ok': False}})
    analyzer.advance(180, "Broker-1 still degraded")
    analyzer.advance(5, "Broker-2 second GC pause",
                     broker_mutations={2: {'gc_pausing': True}})
    analyzer.advance(0, "ISR=1 CRITICAL")
    analyzer.advance(30, "Broker-0 disk failure",
                     broker_mutations={0: {'alive': False}})

    analyzer.print_timeline()

    windows = analyzer.get_dangerous_windows()
    print(f"\n  Dangerous ISR windows:")
    for start, end, reason in windows:
        print(f"    T+{start:.0f}s to T+{end:.0f}s: {reason}")

    # ── 2. Replication Slot WAL Retention Simulation ─────────────────────────────
    print("\n─── Postgres Replication Slot Simulation ───")
    checker = ReplicationSlotChecker(disk_size_gb=50.0)
    checker.add_slot("standby-1", active=True, max_slot_wal_keep_gb=15.0)
    checker.add_slot("debezium-orders", active=True, max_slot_wal_keep_gb=-1)  # Unlimited!

    checker.advance_time(3600)  # 1 hour of writes
    checker.deactivate_slot("debezium-orders")  # Connector stops

    print("\n  After Debezium connector stops:")
    checker.print_status()

    checker.advance_time(3600 * 4)  # 4 more hours
    print("\n  After 4 more hours with inactive slot:")
    checker.print_status()

    # ── 3. Print Kafka Failure Report ────────────────────────────────────────────
    print("\n─── Kafka ISR Collapse Failure Report ───")
    report = kafka_isr_collapse_report()
    print(report.format())

    # ── 4. Generate Postmortem Document ──────────────────────────────────────────
    print("\n─── Generated Postmortem for Postgres Slot Issue ───")
    slot_report = postgres_slot_retention_report()
    template = PostmortemTemplate(slot_report)
    print(template.generate()[:2000])  # First 2000 chars of the postmortem
    print("  ... [truncated for demo]")

    print("\n✓ Failure report analysis demo complete")
```

---

## 6. Visual Reference

### The ISR Collapse State Machine

```
Normal:  ISR = [broker-0, broker-1, broker-2]
         All 3 replicas current. acks=all waits for 2 (not self).

Step 1:  broker-2 GC pause (8s > replica.lag.time.max.ms=10s? NO, 8s < 10s)
         → ISR unchanged (barely escaped)

Step 2:  broker-1 disk saturation (fetch lag grows)
         09:04:10 → broker-1 lag = 10.1s > 10s
         ISR = [broker-0, broker-2]
         Under-replicated = YES. Durability: 2/3 replicas (degraded but safe)

Step 3:  broker-2 second GC pause (12s > 10s threshold)
         09:07:10 → broker-2 lag = 12s
         ISR = [broker-0]
         ← THIS IS THE CRITICAL MOMENT
         With min.insync.replicas=1: producers STILL ACCEPT WRITES
         (acks=all now effectively = acks=1, only broker-0)

Step 4:  broker-0 disk failure
         09:07:30 → broker-0 crashes
         No ISR left. Leader election required.
         With unclean.leader.election.enable=true:
           broker-1 or broker-2 elected (both lagging)
           broker-0's last 30s of writes → LOST

Key insight:
  min.insync.replicas=1 + acks=all = FALSE DURABILITY GUARANTEE
  The producer gets a success ACK but the data is on only 1 broker.
  = functionally equivalent to acks=1 with extra steps.
```

### Postgres WAL Retention Cascade

```
T=0h:   Debezium connector stops consuming
        restart_lsn (slot) = 0x001A000000  ← frozen here
        primary current_lsn = 0x001A000000
        WAL retained: 0 bytes

T=1h:   primary current_lsn = 0x00D4000000  (840MB of WAL)
        restart_lsn still = 0x001A000000
        WAL retained: 840MB

T=6h:   primary current_lsn = 0x04B0000000 (5GB of WAL)
        restart_lsn still frozen
        WAL retained: 5GB
        Disk used: 15% of 50GB disk

T=20h:  WAL retained: 20GB
        Disk used: 40% of 50GB disk
        → No alert configured on slot WAL retention

T=55h:  WAL retained: 46GB
        Disk used: 92% → 80% ALERT should fire
        → Still no alert

T=60h:  WAL retained: 50GB
        Disk used: 100%
        Postgres WAL write FAILS
        All transactions blocked
        Application DOWN

Without max_slot_wal_keep_size: slot has no upper bound.
An offline connector = entire application availability risk.
```

---

## 7. Common Mistakes

**Mistake 1: Treating the proximate cause as the root cause.** The proximate cause of the Kafka ISR data loss was "broker-0 disk failure." The root cause was `min.insync.replicas=1` combined with `unclean.leader.election.enable=true`. If the team fixes only the proximate cause (replaces the failed disk), the same data loss will occur the next time a broker fails. Fixing the root cause (changing the configuration) prevents the entire class of failure. Always ask: "if this proximate cause happened again, would the failure repeat?" If yes, you haven't found the root cause.

**Mistake 2: Not mapping failure symptoms to specific distributed systems invariants.** "Kafka had high consumer lag" is not a useful postmortem observation. "The ISR for topic X shrunk to 1 because the follower's fetch lag exceeded replica.lag.time.max.ms=10s, caused by broker-2's JVM GC pause of 12s — 2 seconds over threshold" is useful. Every symptom should be expressed in terms of the specific mechanism that produced it. This precision is what enables prevention — you can fix a GC configuration; you can't fix "high consumer lag."

**Mistake 3: Missing the early warning metric that was present but unmonitored.** The Kafka ISR collapse showed `UnderReplicatedPartitions > 0` for 3+ minutes before the critical ISR=1 moment. The Postgres slot issue showed `disk_used_pct` climbing for 55 hours before disk full. Real failure reports almost always have an early warning metric that nobody was monitoring. Part of reading a failure report is explicitly identifying this metric and building the alert that would have caught it earlier.

---

## 8. Production Failure Scenarios

### Scenario: ZooKeeper GC Pause Cascade

**Report summary:** A Kafka cluster (ZooKeeper mode, 20 brokers) experienced a complete cluster restart during a traffic spike. All consumers experienced 8 minutes of total message delivery interruption.

**What happened:** At peak traffic, the Kafka controller noticed ZooKeeper latency spikes. ZooKeeper itself experienced a JVM full GC pause of 4.2 seconds. During this pause, ZooKeeper could not process heartbeat acknowledgments. All 20 Kafka brokers' ZooKeeper sessions timed out simultaneously (session timeout = 6 seconds, ZooKeeper unresponsive for 4.2 seconds pushed multiple brokers past the deadline). All 20 brokers were deregistered simultaneously. The controller attempted to reassign leadership for all partitions (thousands) simultaneously. The ZooKeeper ensemble was overwhelmed by the coordinated re-registration storm. Multiple re-registration rounds required.

**Violated invariant:** ZooKeeper's session timeout mechanism assumes only individual nodes fail, not ZooKeeper itself failing. When ZooKeeper becomes slow, all clients time out simultaneously — the opposite of the gradual individual failure the protocol was designed for.

**Key metrics:**
- `zookeeper_latency_max_ms`: spiked from 5ms to 4200ms (signal was present)
- `kafka_controller_active_controller_count`: dropped to 0 (no controller) for 90 seconds
- `kafka_network_RequestsPerSec{request=LeaderAndIsr}`: spiked to 50,000 (re-election storm)

**Prevention:** Tune ZooKeeper JVM for low GC latency (G1GC with `-XX:MaxGCPauseMillis=50`). Increase `zookeeper.session.timeout.ms` to 30000ms (reduces false timeouts but increases failover time). Migrate to KRaft (eliminates ZooKeeper dependency entirely). Alert on ZooKeeper P99 latency > 50ms.

### Scenario: Cassandra Read Repair Causing Clock-Skew Data Corruption

**Report summary:** A Cassandra 4-node cluster (RF=3, QUORUM reads/writes) returned stale data for user profile updates for a 90-second window after a brief network partition. 230 user profiles showed old phone numbers/emails.

**What happened:** A network switch reboot caused a 45-second partition: [node-0, node-1] vs [node-2, node-3]. A profile update write (CL=QUORUM) landed on [node-0, node-1]. Node-2 and node-3 had the old value. Partition healed. A read request (CL=QUORUM) hit node-1 (new value) and node-3 (old value). Normal behavior: the new value wins. But node-3's clock was 3 seconds ahead (NTP drift — it was the last to sync). The old row's `writetime()` was wall clock time on node-3 = 3 seconds ahead of the new write on node-1. LWW selected node-3's old value as the "latest" write. Read repair propagated the old value to node-1. The profile update was effectively silently discarded.

**Violated invariant:** LWW (Last Write Wins) with wall clock timestamps does not correctly order events in the presence of clock skew. The "last" write is determined by wall clock time, not by causality — a clock 3 seconds ahead can make an older write "win."

**Prevention:** Use NTP with `ntpd -q` and monitor `chronyc tracking` for offset. Enforce `max clock skew < 200ms` across all Cassandra nodes. For critical data, use Cassandra Lightweight Transactions (LWT/PAXOS) for CAS semantics instead of LWW. Alert on `chronyc.offset > 100ms`.

---

## 9. Performance and Tuning

### ISR Configuration for Maximum Durability

```bash
# Kafka topic configuration for production durability
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name orders \
  --alter --add-config \
  "min.insync.replicas=2,\
   replication.factor=3,\
   unclean.leader.election.enable=false"

# Broker configuration (server.properties)
# Protect against ISR thrashing from GC pauses:
replica.lag.time.max.ms=30000  # 30s (default 10s) — reduces false ISR removals
# Allow GC pauses up to 20s before broker falls out of ISR

# Protect against ISR thrashing from network jitter:
replica.fetch.wait.max.ms=500  # 500ms max fetch wait (default 500ms)
```

### Postgres Slot Monitoring Query

```sql
-- Alert query: any inactive slot with > 5GB retained WAL
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots
WHERE NOT active
  AND pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) > 5368709120;  -- 5GB

-- Run as a Prometheus custom metric via postgres_exporter:
-- pg_replication_slots_inactive_wal_bytes > 5e9 → alert
```

---

## 10. Interview Q&A

**Q1: What does "reading a failure report" actually teach you that studying the theory doesn't?**

Theory teaches the invariants of distributed systems — what guarantees are supposed to hold. A failure report teaches the gap between the guarantee as specified and the guarantee as implemented in a real system under real conditions. That gap is where careers are made and lost.

For example, Kafka's `acks=all` guarantee sounds airtight: a write is acknowledged only when all ISR members have confirmed it. Theory says this is strong durability. A real failure report shows that `acks=all` + `min.insync.replicas=1` (the default) is not strong durability — it's `acks=1` with extra steps, because the ISR can shrink to 1 and the guarantee silently degrades. The theory gives you the guarantee. The failure report gives you the configuration trap that voids the guarantee.

More specifically, failure reports teach four things the theory doesn't: the operational fingerprints (which metrics show anomalous values before the failure is user-visible), the configuration traps (which settings, innocuous in isolation, create catastrophic combinations), the timeline compression (how quickly conditions can compound from "degraded" to "down"), and the detection gaps (how long systems were in a dangerous state before anyone noticed).

**Q2: Describe the exact mechanism by which a Postgres replication slot can take down the primary database.**

Postgres replication slots provide a guarantee: the primary will not delete any WAL segment that the slot's consumer has not yet read. This is done by recording the slot's `restart_lsn` — the primary retains all WAL from `restart_lsn` forward. When a streaming standby or logical consumer (like Debezium) is active and consuming, `restart_lsn` advances in lock-step with consumption, so retained WAL stays small.

The failure occurs when the consumer goes offline (crash, network issue, deployment bug) and the slot becomes inactive. `restart_lsn` is frozen at the last consumed position. The primary continues receiving writes at its normal rate — say 10MB/s. Every second, 10MB of WAL accumulates that cannot be deleted because the inactive slot's `restart_lsn` hasn't advanced. After 6 hours, the primary has 216GB of uncleanable WAL. When the primary's disk reaches 100% capacity, Postgres cannot write new WAL entries. The WAL writer process returns an I/O error. This causes all transactions that attempt to commit to fail — the primary can no longer accept any writes. The entire application stops.

The monitoring gap that allows this: most teams alert on disk_used_pct but not on per-slot WAL retention. The disk alert fires only after the damage is done (disk full). The correct alert fires early: "slot X is inactive and has retained > 5GB of WAL." The prevention is `max_slot_wal_keep_size` (Postgres 13+), which caps retention per slot and invalidates the slot if the cap is exceeded — preferring a standby that needs a full resync over a primary that fills its disk and goes offline.

---

## 11. Cross-Question Chain

**Interviewer:** Your Kafka cluster has 3 brokers, RF=3, acks=all, min.insync.replicas=2. A broker goes down. What happens?

**Candidate:** With RF=3 and one broker down, the remaining two brokers form the ISR (assuming the failed broker was in sync before it went down). With min.insync.replicas=2 and ISR=2, the system remains available for producers and consumers. Producers with acks=all wait for both remaining ISR members to confirm each write — quorum is satisfied. The topic is under-replicated (ISR=2 < RF=3), so `UnderReplicatedPartitions > 0`, which should trigger a WARNING alert. But the cluster is still serving traffic without data loss risk.

**Interviewer:** That broker comes back. How does it rejoin?

**Candidate:** The recovering broker restarts, connects to the cluster, and discovers which partitions it needs to catch up on. For partitions where it's a follower, it sends fetch requests to the current leader starting from its last replicated offset. The leader's log segment is still intact (WAL retention ensures segments needed by known replicas are kept). The follower replicates all entries it missed during the downtime. Once its lag falls below `replica.lag.time.max.ms`, the controller adds it back to ISR. From the Kafka metrics perspective: `IsrExpandsPerSec` spikes briefly, then `UnderReplicatedPartitions` returns to 0.

**Interviewer:** What if the broker was down for 3 days and the leader has already rotated and deleted old log segments?

**Candidate:** This is the equivalent of the Postgres "slot too far behind for WAL to still exist" scenario. The recovering broker fetches from its last offset, but the leader responds with an `OffsetOutOfRange` error — the segment containing that offset has been deleted. The follower cannot catch up incrementally; it needs a full log replica from the leader. It will request a full sync (similar to Postgres pg_basebackup). In Kafka terms, the follower enters a `Truncating` state, receives the leader's current log, and re-establishes in-sync status. The time to rejoin depends on the volume of data to replicate.

**Interviewer:** How would you have detected this 3-day outage on the broker before it affected serving?

**Candidate:** Two detection mechanisms. First, `UnderReplicatedPartitions` alert: the moment the broker went down, any partition it led or followed becomes under-replicated. An alert on `UnderReplicatedPartitions > 0 sustained for 60 seconds` fires within 60 seconds of the broker going offline — well within an SLA requirement to re-establish RF=3 within hours. Second, a broker health alert: `kafka.server:BrokerState` or the broker's `/health` endpoint going offline should trigger a direct on-call page. Three days means neither alert was in place. The fix is two-fold: set the `UnderReplicatedPartitions` alert, and ensure broker process monitoring (systemd health check or Kubernetes liveness probe) notifies the team immediately on broker crash.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the difference between root cause and proximate cause in a postmortem? | Proximate cause: the immediate trigger of the failure. Root cause: the underlying condition that made the system vulnerable to that trigger. Fixing only the proximate cause allows the next variant of the same failure to recur. |
| 2 | What dangerous Kafka configuration allows acks=all to provide no durability? | `min.insync.replicas=1` allows ISR to shrink to 1 without blocking producers. With ISR=1, acks=all is equivalent to acks=1 — a single broker failure loses all unconfirmed writes. |
| 3 | What is unclean.leader.election.enable and why is false safer? | Allows an out-of-ISR replica to be elected leader when no ISR member is available. Enables availability after severe ISR collapse but allows data loss (the new leader has missed recent writes). `false` means the partition goes offline instead — safer for durability. |
| 4 | Why does a Postgres replication slot cause disk exhaustion? | Slots prevent WAL segment deletion until the consumer's restart_lsn advances. If the consumer goes offline, restart_lsn freezes, WAL accumulates indefinitely, and disk fills. Fix: max_slot_wal_keep_size (Postgres 13+). |
| 5 | What is the "detection gap" in a postmortem and why is it significant? | The time between when the anomalous metric first appeared and when the incident was detected. Closing the detection gap is the most actionable outcome of a postmortem — requires adding an alert on the early-warning metric. |
| 6 | How does a ZooKeeper GC pause cause a cascade of Kafka broker deregistrations? | ZooKeeper pauses → cannot process session heartbeat ACKs → all Kafka broker sessions timeout simultaneously → all brokers deregister → controller processes massive partition reassignment storm → controller overloaded → more timeouts. |
| 7 | What Cassandra failure mode arises from clock skew combined with LWW? | LWW (Last Write Wins) uses wall clock timestamps. A node with a clock 3 seconds ahead can make an old write appear as "newer" than a later write on a node with a more accurate clock. The old value wins conflict resolution, silently discarding the actual latest write. |
| 8 | What metric would detect a Postgres replication slot problem before disk exhaustion? | `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` per slot. Alert: > 5GB on an inactive slot. This fires hours before disk fills, giving time to investigate and fix. |
| 9 | What is the five-step failure analysis framework? | (1) Classify failure type, (2) identify violated invariant, (3) trace A→B→C causation chain, (4) map to metrics (early warning), (5) state prevention (class, not instance). |
| 10 | What Kafka metric provides the earliest warning of an impending ISR collapse? | `kafka.server:UnderReplicatedPartitions > 0 sustained for > 60 seconds`. This fires when the first ISR shrinkage occurs, giving time to investigate before the ISR shrinks further and becomes critical. |
| 11 | Why is "Kafka had high consumer lag" not a useful postmortem observation? | It doesn't identify the mechanism. Useful version: "Kafka topic X partition 3 had 12-minute consumer lag because ISR shrank to 1 (broker-2 fetch lag exceeded replica.lag.time.max.ms=10s due to broker-2's disk await=150ms from a noisy neighbor VM), causing producer backpressure." |
| 12 | What is the Kafka safe configuration for production durability? | RF ≥ 3, min.insync.replicas=2, acks=all, unclean.leader.election.enable=false. Together: at least 2 ISR replicas must confirm each write; leadership only granted to ISR members. |
| 13 | What happened in the ZooKeeper replacement (KRaft) that addresses the GC cascade failure? | KRaft replaces ZooKeeper with Raft-based metadata management inside Kafka itself. Kafka brokers no longer register in ZooKeeper. A ZooKeeper GC pause cannot cause broker deregistrations. The metadata cluster is Kafka, not an external dependency. |
| 14 | Why do postmortems often identify a metric that was anomalous but not alerted on? | Most teams instrument for known failure modes (disk full, process down) but not for precursor conditions (ISR shrinkage, slot WAL retention, ZooKeeper latency). Real failures routinely expose unmonitored precursors. The postmortem is the prompt to add those alerts. |
| 15 | What is the 2PC stuck prepared transaction production symptom? | Rows in PENDING state, lock contention on those rows, queries queuing behind the locks. `pg_prepared_xacts` shows transactions prepared minutes/hours ago. `pg_locks` shows those transactions holding row locks. Application appears hung or slow. |

---

## 13. Further Reading

- **"Kafka ISR Design" (Confluent documentation):** Explains ISR membership, the lag threshold, min.insync.replicas semantics, and unclean leader election from the Kafka maintainer perspective.
- **"Autopsy of a Data Loss Incident" (Jay Kreps / LinkedIn Engineering blog, 2013):** One of the earliest public postmortems on Kafka data loss due to ISR configuration. The specific combination of settings described in this module traces back to this incident.
- **"Understanding and Solving Postgres Replication Conflicts" (Postgres wiki):** Covers WAL retention for replication slots and the configuration options. max_slot_wal_keep_size documentation included.
- **"Postmortems at Google" (Site Reliability Engineering book, Chapter 15):** The canonical treatment of blameless postmortems, root cause analysis, and actionable prevention. The five-step framework in this module is a simplified version of Google's SRE postmortem process.
- **"Distributed Systems Postmortems" (github.com/danluu/post-mortems):** A curated list of real distributed systems postmortems with links to the original reports. Invaluable reading for pattern recognition.
- **"Aphyr's Jepsen series" (aphyr.com/tags/jepsen):** Kyle Kingsbury's systematic distributed systems correctness testing. Each Jepsen report is a detailed failure analysis of a real system — exactly the kind of analysis this module teaches.

---

## 14. Lab Exercises

**Exercise 1: Analyze the ISR Timeline**
Run `failure_report_analyzer.py`. In the ISR collapse reconstruction, modify `min_insync_replicas=2` and observe that `produces_available` becomes False when ISR drops to 1. This models the correct behavior: with min.insync.replicas=2, producers would have received `NOT_ENOUGH_REPLICAS` errors at T+420s — before the broker failure at T+450s. The system would have been degraded but not silently losing data.

**Exercise 2: Simulate WAL Retention at Different Write Rates**
In the `ReplicationSlotChecker`, try `wal_growth_rate_bytes_per_s = 50 * 1024**2` (50MB/s, high-write database). Observe that disk fills in under 12 hours instead of 60 hours. This illustrates why a "we have 3 days before disk fills" assumption from a low-traffic environment is dangerously wrong for a high-traffic primary.

**Exercise 3: Write Your Own Failure Report**
Choose a real or hypothetical failure scenario involving a Kafka or Postgres system. Use the `FailureReport` dataclass to structure it: identify the violated invariant explicitly, construct a timeline with `is_root_cause` markers, list the metrics signatures, and calculate the detection gap. Generate the postmortem with `PostmortemTemplate`.

**Exercise 4: Add an Alert Threshold**
The ISR collapse simulation doesn't include an "alert fires at T+X" mechanism. Add a method `get_alert_fire_time(threshold_sustained_s)` to `ISRCollapseAnalyzer` that returns when an alert on "ISR < RF for > threshold seconds" would have fired. Verify that with `threshold=60s`, the alert fires at T+300s (broker-1 lag 60s after it started) — well before the critical T+420s ISR=1 moment.

---

## 15. Key Takeaways

Distributed systems failures follow recognizable patterns. Once you have seen an ISR collapse, a ZooKeeper cascade, or a replication slot disk exhaustion, you will recognize the early metrics signatures of the next variant before it escalates to a user-visible incident.

Every real failure exposes a gap between a theoretical guarantee and its practical implementation. `acks=all` is not a durability guarantee in isolation — it's a durability guarantee conditional on `min.insync.replicas > 1` and `unclean.leader.election.enable=false`. The failure report teaches the conditions. The postmortem teaches which conditions were not satisfied.

The most actionable output of a postmortem is the detection gap and the alert that would have closed it. "We saw `UnderReplicatedPartitions > 0` for 3 minutes but had no alert" is fixable in 30 minutes. "We need to redesign our replication topology" is a 3-month project. Good postmortems produce both — and prioritize the 30-minute fix so the next incident doesn't become a 3-month project.

---

## 16. Connections to Other Modules

- **M39 — Replication:** The ISR collapse report is the practical realization of M39's ISR mechanism under adverse conditions. Every configuration recommendation in M39 (acks=all, min.insync.replicas=2, unclean.leader.election=false) traces back to the failure patterns analyzed in this module.
- **M40 — Failure Modes:** The failure taxonomy (gray failure, cascading failure, network partition) from M40 provides the classification framework for step 1 of the failure analysis framework. Every failure report maps to one or more of M40's failure types.
- **M41 — Distributed Transactions:** The 2PC stuck prepared transaction scenario is the production manifestation of the blocking problem described in M41. The recovery procedure (`COMMIT PREPARED` / `ROLLBACK PREPARED`) is the operational mitigation.
- **M45 — Clock Synchronization:** The Cassandra LWW clock-skew failure directly motivates M45's discussion of hybrid logical clocks. HLC eliminates the false LWW winner problem by combining logical ordering with physical time.

---

## 17. Anti-Patterns

**Anti-pattern: Writing postmortems that only describe what happened, not why it was able to happen.** A postmortem that says "broker-0 crashed, causing ISR to shrink to 0, causing data loss" does not prevent recurrence. A postmortem that says "the combination of min.insync.replicas=1 (default) and unclean.leader.election.enable=true allowed ISR=1 to exist while producers still received success ACKs — this combination silently voids the acks=all durability guarantee" enables prevention. The "why it was able to happen" section is the difference between a useful postmortem and a chronicle.

**Anti-pattern: Only monitoring for hard failures (process down, disk full) rather than precursors (ISR shrinkage, slot lag, ZooKeeper latency).** Hard failure alerts are important but they fire too late — by the time the process is down, the incident has already escalated. Precursor monitoring catches the early warning state while there is still time to intervene. Every postmortem should produce at least one new precursor alert that would have fired before the incident became user-visible.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `kafka-topics.sh --under-replicated-partitions` | List under-replicated partitions | Immediate ISR health check |
| `kafka-topics.sh --describe` | Show ISR and replica assignment per partition | Detailed ISR state per topic |
| `pg_replication_slots` view | Postgres replication slot state | Monitor slot WAL retention |
| `pg_prepared_xacts` view | List stuck prepared transactions | 2PC recovery identification |
| `failure_report_analyzer.py` | Structured failure report and simulation | ISRCollapseAnalyzer, ReplicationSlotChecker |
| `danluu/post-mortems` | Real distributed systems postmortems | Pattern recognition reading |
| `chronyc tracking` | NTP clock sync status | Clock skew monitoring (Cassandra LWW) |
| `etcdctl endpoint status` | Raft cluster state | Leader, term, log index per node |

---

## 19. Glossary

**Cascading failure:** A failure that propagates from one component to others through load redistribution or shared dependency. ZooKeeper GC pause → all session expirations → controller storm is a cascading failure.

**Detection gap:** The time between when a failure's early warning metric first became anomalous and when the incident was actually detected. Closing the detection gap is the primary operational outcome of a postmortem.

**Gray failure:** A node or component that is alive and passing health checks but performing below acceptable threshold. The ISR scenario where broker-1's disk await was 150ms (visible as gray failure in P99 metrics) is a gray failure. Binary health checks miss it.

**ISR collapse:** The process by which Kafka's In-Sync Replica set shrinks from RF (replication factor) to a subset. At ISR=1, acks=all provides no replication durability.

**LWW (Last Write Wins):** Cassandra's default conflict resolution strategy. In concurrent writes, the write with the highest timestamp wins. Vulnerable to clock skew: a node with a fast clock can make old writes appear newer.

**max_slot_wal_keep_size:** Postgres 13+ configuration parameter. Caps the WAL retained by a replication slot. When exceeded, the slot is invalidated rather than allowing disk exhaustion. The critical protection against the slot WAL retention failure mode.

**min.insync.replicas:** Kafka broker/topic configuration. Minimum ISR size required for producers to succeed. Setting to 1 (default) allows acks=all with ISR=1 — no replication durability. Setting to 2 ensures at least 2 replicas confirm each write.

**Postmortem:** A blameless incident report that reconstructs what happened, identifies root cause, calculates detection gap, and produces actionable prevention steps.

**Proximate cause:** The immediate trigger of a failure, as opposed to the root cause (the underlying vulnerability). "broker-0 disk failure" vs "min.insync.replicas=1 allowed ISR=1."

**Replication slot:** A Postgres mechanism that guarantees WAL retention for a specific consumer. Prevents WAL segment deletion until the consumer advances past those segments. Dangerous when consumer goes offline and slot becomes inactive.

**unclean.leader.election.enable:** Kafka setting that allows an out-of-ISR replica to become leader when no ISR member is available. Enables availability at the cost of potential data loss. Default changed to `false` in Kafka 2.8+.

---

## 20. Self-Assessment

1. Apply the five-step framework to this scenario: "Our Cassandra cluster returned wrong data for 2 minutes after a rolling restart." What is the failure type? What invariant was likely violated?
2. A Kafka cluster with RF=3 has `min.insync.replicas=2`. The ISR is currently [broker-0, broker-1]. Broker-1 then crashes. What happens to producers using acks=all?
3. Why does `replica.lag.time.max.ms=10s` cause false ISR removals during JVM GC pauses? What would you set it to if your GC pauses are typically 5-8 seconds?
4. You see `pg_replication_slots.active=false` for a slot and `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)=8GB`. The Postgres data volume is 50GB total (currently 60% full). When will the disk fill? What should you do right now?
5. Run `failure_report_analyzer.py` and trace the exact moment in the ISR collapse simulation where a `min.insync.replicas=2` alert would fire vs a `min.insync.replicas=1` system. What is the user experience difference?
6. What is the Kafka KRaft motivation for eliminating ZooKeeper, and which specific failure mode from this module does it prevent?
7. Describe the Cassandra clock-skew LWW failure mode. Under what exact conditions does it produce incorrect data, and what is the correct mitigation?
8. Why is the 2PC blocking problem fundamentally unresolvable by the participants themselves (without external intervention)?

---

## 21. Module Summary

This module developed the skill of reading distributed systems failure reports — translating from operational symptoms (ISR collapse, disk full, stale reads) to the precise distributed systems mechanisms that produced them (ISR threshold violation, replication slot retention, LWW clock-skew conflict).

Five failure archetypes were analyzed in depth: Kafka ISR collapse (min.insync.replicas=1 + unclean election silently voiding acks=all durability), ZooKeeper GC cascade (shared dependency making individual-fault assumptions fail in bulk-fault scenarios), Cassandra LWW clock skew (leaderless conflict resolution vulnerable to NTP drift), Postgres replication slot WAL exhaustion (retention guarantee without upper bound destroying primary availability), and 2PC coordinator crash (blocking problem making in-doubt transactions require human recovery).

Each analysis produced three outputs: the violated invariant stated precisely, the early warning metric that was present but unmonitored, and the prevention that closes the entire class of failure. This pattern — invariant, early warning, prevention — is the template for every postmortem worth writing.

The next module — M45: Clock Synchronization — examines the problem of ordering events in distributed systems in depth. The Cassandra clock-skew LWW failure from this module is the concrete motivation: when physical clocks can disagree, what mechanisms allow distributed systems to maintain consistent ordering of events?
