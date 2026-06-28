# M37: The CAP Theorem and PACELC

**Course:** SYS-DST-101 — Distributed Systems Theory  
**Module:** 01 of 05  
**Global Module ID:** M37  
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

Every architectural decision in distributed data systems involves trade-offs between consistency and availability. These trade-offs are not implementation choices — they are theoretically necessary consequences of physics and mathematics. CAP theorem, proven by Eric Brewer in 2000 and formalized by Gilbert and Lynch in 2002, establishes the hard boundary: a distributed system cannot simultaneously provide Consistency, Availability, and Partition Tolerance. When a network partition occurs, you must choose one.

Data engineers who don't understand CAP make architectural decisions blindly. They choose Cassandra for a use case requiring strong consistency, then discover that users see stale reads. They choose Postgres with synchronous replication for a use case where availability matters more than consistency, then discover 30-second write pauses during network hiccups. They configure Kafka with `acks=all` + `min.insync.replicas=3` for a fire-and-forget logging pipeline and wonder why producers are getting timeout errors during broker maintenance.

CAP tells you what to expect when a network partition happens. PACELC extends the analysis to the normal case: even without partition, there is a trade-off between latency and consistency. This extension is more operationally relevant for data platforms that run in healthy networks 99.9% of the time but still need to choose between low-latency writes (asynchronous replication) and durable consistency (synchronous replication).

The practical payoff of understanding CAP + PACELC: you can classify any distributed system — Kafka, Postgres, Cassandra, DynamoDB, Spanner, ZooKeeper — by its behavior during failures and under normal operation. This classification directly determines which systems are appropriate for which use cases, and what failure modes to design for.

---

## 2. Mental Model

### The Three Properties

**Consistency (C):** Every read receives the most recent write or an error. In a consistent system, there is no such thing as a stale read — every node has the same view of data. This is the C in ACID, but stronger: it applies across multiple nodes, not just within a single transaction. The formal definition is linearizability: operations appear to take effect atomically at some point between their invocation and completion, and in an order consistent with real time.

**Availability (A):** Every request receives a response (not necessarily the most recent write). An available system never returns an error for a client request due to node failures. Note what availability means precisely: every non-failing node must return a response. A system that routes all reads to a single primary is not available in the CAP sense if that primary is partitioned — requests to the isolated secondary would need to fail, violating availability.

**Partition Tolerance (P):** The system continues to operate despite arbitrary message loss or delay between nodes. In any real network — even a private cloud VPC — packets can be dropped, links can fail, and routers can be misconfigured. Partition tolerance is therefore not optional for any real distributed system operating over a network. This is the key insight that makes the CAP theorem operational: you cannot choose between C, A, and P as three independent options. P is mandatory, so the real choice is between C and A during a partition.

### The Real Choice: CP vs AP

Because partition tolerance is required, the theorem reduces to: when a partition occurs, choose between consistency and availability.

**CP (Consistent, Partition-tolerant):** During a partition, the system sacrifices availability. A node that cannot guarantee it has the latest data refuses to serve reads (or returns an error). The system is correct but unavailable. Examples: ZooKeeper, HBase, Postgres with synchronous replication, Kafka with `acks=all` + ISR enforcement.

**AP (Available, Partition-tolerant):** During a partition, the system sacrifices consistency. Every node continues to serve requests with potentially stale data. The system is available but may return outdated reads. Examples: Cassandra (by default), DynamoDB (with eventual consistency), Kafka consumers reading from non-leader replicas (with `isolation.level=read_uncommitted`).

### PACELC: The Normal-Case Extension

CAP only describes behavior during partitions. But most of the time, networks are healthy. Daniel Abadi's PACELC framework (2012) extends the analysis:

- **During a Partition (P):** choose between Availability (A) vs Consistency (C)  
- **Else (E):** even when the system is running normally (no partition), choose between Latency (L) vs Consistency (C)

The PACELC tuple is written as `PA/EL` (available during partition, low latency normally) or `PC/EC` (consistent during partition, consistent normally) or combinations thereof.

The normal-case trade-off is often more practically important than the partition-case trade-off because partitions are rare but latency vs consistency is a constant operational decision. Synchronous replication (every write waits for acknowledgment from all replicas) provides strong consistency but adds cross-replica network RTT to every write. Asynchronous replication provides low latency but risks data loss if the primary fails before replicating.

---

## 3. Core Concepts

### 3.1 Formal CAP Statement

The theorem states: it is impossible for a distributed data store to simultaneously provide all three of the following:

1. **Consistency:** Every read receives the most recent write or an error
2. **Availability:** Every request (to a non-failed node) receives a response
3. **Partition Tolerance:** The system continues operating despite arbitrary network partitions

**The proof sketch:** Imagine a two-node system (N1 and N2) storing a value `v`. A client writes `v=1` to N1. Before N1 can replicate to N2, a network partition occurs. Now a client reads from N2.

- If we require consistency: N2 must return an error or wait for the partition to heal (not available).
- If we require availability: N2 must respond. But the only value it has is the stale `v=0` (not consistent).
- We cannot have both.

### 3.2 Classifying Real Systems

**Kafka:**  
Kafka is designed to be CP. The ISR (In-Sync Replicas) mechanism ensures that writes are only acknowledged when committed to all in-sync replicas. `min.insync.replicas` controls how many replicas must be in-sync for a write to succeed. With `acks=all` and `min.insync.replicas=2` in a 3-broker cluster: if two brokers go down, Kafka refuses new writes (unavailable) rather than accepting writes that might be lost. Consumer reads are consistent by default (reading committed offsets only).

However, Kafka's availability story is nuanced: reads can continue from the leader even during replication lag. The trade-off is between write availability (CP during partition) and read consistency (AP when replicas lag).

**PACELC for Kafka:** `PC/EC` — consistent during partition, consistent normally (writes wait for ISR acknowledgment, adding latency).

**Apache Cassandra:**  
Cassandra is designed to be AP. Every node can accept reads and writes independently. With `consistency_level=ONE`, a write succeeds if any single node acknowledges it, regardless of what other nodes have. During a partition, Cassandra continues serving all nodes, accepting writes that will be reconciled later via anti-entropy. This means reads may return stale data.

**PACELC for Cassandra:** `PA/EL` — available during partition, low latency normally (writes return after one replica acknowledges, without waiting for all).

Cassandra can be configured toward consistency: `consistency_level=QUORUM` (majority of replicas must respond) or `ALL` (all replicas). At `ALL`, Cassandra approximates CP behavior.

**PostgreSQL:**  
Single-node Postgres is trivially consistent (single master, no replication issues). With synchronous replication (`synchronous_commit=on`, `synchronous_standby_names`), Postgres is CP: writes wait for acknowledgment from the standby before returning to the client. During a network partition between primary and standby, Postgres primary waits forever for acknowledgment — effectively pausing all writes (unavailable) rather than accepting writes that might diverge.

With asynchronous replication, Postgres is PA/EL: writes return immediately, data is replicated later. The standby may be behind, and a failover could lose committed transactions.

**PACELC for synchronous Postgres:** `PC/EC` — consistent during partition (pauses writes), high consistency/latency normally (every write waits for replication acknowledgment).

**Apache ZooKeeper:**  
ZooKeeper is explicitly CP. It uses ZAB (ZooKeeper Atomic Broadcast), a consensus protocol. If a leader cannot communicate with a majority of its followers, it stops processing writes. This ensures that all reads to ZooKeeper return consistent data — at the cost of availability during partitions. This is why Kafka's original ZooKeeper dependency was a CP bottleneck: ZooKeeper being unavailable made Kafka controller elections impossible.

**Google Spanner:**  
Spanner is notable for being `PC/EC` (consistent both during partition and normally) while achieving very low latency. It uses GPS and atomic clock hardware (TrueTime) to bound clock uncertainty, enabling globally consistent reads without distributed consensus on every read. This breaks the apparent PACELC constraint — but only because of specialized hardware that provides a bounded global time reference, not available in commodity infrastructure.

**DynamoDB:**  
With eventually consistent reads: `PA/EL`. With strongly consistent reads: `PC/EC` but at higher latency. DynamoDB allows the client to choose per-request, making it a configurable position on the CAP/PACELC spectrum.

### 3.3 The PACELC Latency/Consistency Trade-off

The normal-case (E) trade-off in PACELC is about replication:

**Synchronous replication (EL sacrificed for EC):**
```
Client → writes to Primary
Primary → waits for acknowledgment from replica(s)
Replica writes data
Replica → ACK to Primary
Primary → ACK to Client
```
Write latency = local write time + network RTT to replica + replica write time. In a single-AZ deployment (1ms RTT), this adds ~2ms per write. Cross-region (50ms RTT), this adds ~100ms per write. The client is blocked during replication.

**Asynchronous replication (EC sacrificed for EL):**
```
Client → writes to Primary
Primary → ACK to Client immediately
Primary → replicates to replica (asynchronously, background)
```
Write latency = local write time only. The client is not blocked. But if the primary crashes before replication completes, the committed-to-client data is lost.

For data engineering, the correct choice depends on the pipeline's durability requirements:
- Audit logs, financial transactions, data lake delta writes → synchronous (EC): losing data is unacceptable
- Metrics, telemetry, click-stream → asynchronous (EL): losing a few events is acceptable; low producer latency matters

### 3.4 Network Partitions in Practice

A network partition is not always a total network outage. Common real-world partition scenarios:

**Asymmetric partition:** Node A can send to Node B, but Node B's responses are dropped. From A's perspective, writes appear to succeed (A sends the write). From B's perspective, it never received the write. A reads its own write; B doesn't know about it.

**Partial partition:** Some nodes are partitioned from others. In a 5-node cluster, nodes 1-2 can communicate with each other, nodes 3-5 can communicate with each other, but 1-2 cannot communicate with 3-5. Two "islands" form. If each island elects a leader, split-brain occurs.

**Slow network (gray failure):** Packets are not dropped but arrive with high latency. A timeout-based system may treat slow responses as failures and trigger failover — causing a partition even though the network is technically connected.

**Detection difficulty:** The challenge with partitions is that a node cannot distinguish "the other node is down" from "the other node is alive but I can't reach it." Both scenarios look identical from the outside. This is why the failure detector problem is fundamental to distributed systems.

### 3.5 Practical Implications for Data Platform Design

**Data ingestion (Kafka → data lake):** Kafka as the durable log should be CP (`acks=all`, `min.insync.replicas=2`). Data loss at ingestion is the hardest category of data loss to recover from — you can always reprocess stale data, but you cannot recover data that was never durably written.

**OLAP queries (BigQuery, Redshift, Athena):** These systems sacrifice strong consistency for availability and query performance. BigQuery's storage layer uses eventual consistency for metadata updates; a newly written table may not immediately appear in `INFORMATION_SCHEMA`. Design pipelines that tolerate this: poll for job completion rather than assuming immediate availability.

**Metadata stores (Airflow's Postgres, Hive Metastore):** These are CP stores. If the metadata database is unavailable, pipelines should fail fast rather than proceed with potentially stale catalog information. A Spark job that proceeds with a stale partition list from the metastore may read the wrong data.

**Caching layer (Redis, Memcached):** Typically AP: the cache may hold stale data. Cache misses fall through to the CP source. Design the cache as an optimization, not a consistency guarantee.

---

## 4. Hands-On Walkthrough

### 4.1 Observing CAP in Action with Kafka

```bash
# Setup: 3-broker Kafka cluster, topic with RF=3, min.insync.replicas=2

# Create topic with strong consistency guarantees
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic consistent-events \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2

# Configure producer for strong consistency (CP behavior)
cat > producer-cp.properties << 'EOF'
bootstrap.servers=broker1:9092,broker2:9092,broker3:9092
acks=all
retries=3
enable.idempotence=true
EOF

# Simulate partition: stop broker 2 and broker 3
# With min.insync.replicas=2, only 1 broker remains in ISR
# Producers with acks=all will now get NotEnoughReplicasException

kafka-console-producer.sh \
  --producer.config producer-cp.properties \
  --topic consistent-events \
  --broker-list broker1:9092
# Type a message...
# Expected output: WARN [Producer clientId=...] Got error...
# org.apache.kafka.common.errors.NotEnoughReplicasException:
# Messages are rejected since there are fewer in-sync replicas than required.

# This is CP behavior: Kafka sacrifices availability (refuses writes)
# to maintain consistency (not accepting data that might be lost)

# AP behavior: configure acks=1 (only leader acknowledges)
cat > producer-ap.properties << 'EOF'
bootstrap.servers=broker1:9092
acks=1
EOF

# With acks=1, the producer gets ACK from broker1 even if broker2/3 are down
# The write appears to succeed but is not replicated
# This is AP behavior: available (writes succeed) but not strongly consistent
```

### 4.2 Observing PACELC Trade-offs: Latency vs Consistency

```python
# Measure write latency under synchronous vs asynchronous replication
# This demonstrates the PACELC E-side trade-off

import time
import psycopg2

def measure_write_latency(dsn: str, synchronous_commit: str, iterations: int = 100):
    """
    Measure Postgres write latency under different synchronous_commit settings.
    synchronous_commit='on'   → wait for WAL flush on primary AND standby (EC)
    synchronous_commit='local' → wait for WAL flush on primary only
    synchronous_commit='off'  → return after WAL buffer write (EL)
    """
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    # Set synchronous_commit for this session
    cur.execute(f"SET synchronous_commit = '{synchronous_commit}'")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS latency_test (
            id SERIAL PRIMARY KEY,
            value TEXT,
            written_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.commit()

    latencies = []
    for i in range(iterations):
        start = time.perf_counter()
        cur.execute("INSERT INTO latency_test (value) VALUES (%s)", (f"event-{i}",))
        conn.commit()
        latency_ms = (time.perf_counter() - start) * 1000
        latencies.append(latency_ms)

    avg = sum(latencies) / len(latencies)
    p99 = sorted(latencies)[int(0.99 * len(latencies))]
    cur.close()
    conn.close()

    return {
        'synchronous_commit': synchronous_commit,
        'avg_ms': round(avg, 2),
        'p99_ms': round(p99, 2),
        'min_ms': round(min(latencies), 2),
        'max_ms': round(max(latencies), 2),
    }

# Typical results (single node, no replica):
# synchronous_commit='on'   → avg ~0.8ms  (waits for WAL flush to disk)
# synchronous_commit='local' → avg ~0.8ms  (same as 'on' with no replica)
# synchronous_commit='off'  → avg ~0.05ms (returns after WAL buffer write)
#
# With a synchronous replica in another AZ (50ms RTT):
# synchronous_commit='on'   → avg ~102ms  (waits for remote WAL flush)
# synchronous_commit='off'  → avg ~0.05ms (does not wait for replica)
#
# This is the PACELC E-side trade-off:
# EC (consistent normally): pay 102ms per write for durability
# EL (low latency normally): pay 0.05ms per write, accept potential data loss
```

### 4.3 Cassandra Consistency Level Demonstration

```python
from cassandra.cluster import Cluster
from cassandra.policies import ConsistencyLevel
from cassandra import WriteTimeout, ReadTimeout, Unavailable

def cassandra_consistency_demo():
    """
    Demonstrates Cassandra's configurable CAP position.
    Cassandra is AP by default but can be configured toward CP
    via consistency level settings.
    """
    cluster = Cluster(['cassandra1', 'cassandra2', 'cassandra3'])
    session = cluster.connect('data_platform')

    # AP behavior: consistency_level=ONE
    # Write succeeds as long as one replica responds
    # Read returns data from one replica (may be stale)
    session.default_consistency_level = ConsistencyLevel.ONE

    try:
        session.execute(
            "INSERT INTO events (id, value) VALUES (%s, %s)",
            ('event-1', 'data')
        )
        row = session.execute("SELECT * FROM events WHERE id = 'event-1'").one()
        print(f"ONE consistency: write and read succeeded. value={row.value}")
        # Even if 2 out of 3 nodes are down, this succeeds → AP
    except Unavailable as e:
        print(f"Unavailable: {e}")  # Only if ALL nodes are down

    # CP behavior: consistency_level=QUORUM (majority must respond)
    # RF=3 → QUORUM = 2 replicas must acknowledge
    session.default_consistency_level = ConsistencyLevel.QUORUM

    try:
        session.execute(
            "INSERT INTO events (id, value) VALUES (%s, %s)",
            ('event-2', 'data')
        )
        row = session.execute("SELECT * FROM events WHERE id = 'event-2'").one()
        print(f"QUORUM consistency: write and read succeeded.")
        # Requires 2/3 nodes → if only 1 node available, this fails (CP)
    except Unavailable as e:
        print(f"QUORUM unavailable (CP behavior): {e}")

    # STRONGEST consistency: ALL
    # Every replica must acknowledge — truly CP
    session.default_consistency_level = ConsistencyLevel.ALL

    try:
        session.execute("INSERT INTO events (id, value) VALUES (%s, %s)", ('event-3', 'data'))
        print("ALL consistency: write succeeded (all 3 nodes responded)")
    except (WriteTimeout, Unavailable) as e:
        print(f"ALL consistency failed (expected with any node down): {e}")

    cluster.shutdown()
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
cap_pacelc_analyzer.py

Tools for reasoning about CAP/PACELC trade-offs in a data platform.
Classifies systems, simulates partition behavior, and measures latency/consistency
trade-offs for informed architectural decisions.

No external dependencies required for core analysis.
"""

import time
import random
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from collections import defaultdict


# ─── CAP/PACELC Classification ──────────────────────────────────────────────────

class CAPClass(Enum):
    CP = "CP"   # Consistent + Partition-tolerant (sacrifices Availability)
    AP = "AP"   # Available + Partition-tolerant (sacrifices Consistency)
    # CA is not meaningful for distributed systems (can't sacrifice P)


class PACELCClass(Enum):
    PA_EL = "PA/EL"   # Available during partition, Low latency normally (Cassandra default)
    PA_EC = "PA/EC"   # Available during partition, Consistent normally (rare)
    PC_EL = "PC/EL"   # Consistent during partition, Low latency normally (rare)
    PC_EC = "PC/EC"   # Consistent during partition, Consistent normally (Postgres sync)


@dataclass
class SystemClassification:
    system_name: str
    cap_class: CAPClass
    pacelc_class: PACELCClass
    partition_behavior: str     # What happens during a partition
    normal_behavior: str        # What happens in the normal case
    data_loss_risk: str         # "none", "low", "medium", "high"
    availability_risk: str      # "none", "low", "medium", "high"
    notes: str
    configuration_dependency: Optional[str] = None  # "depends on config"


# Classification database for common data engineering systems
SYSTEM_CLASSIFICATIONS = {
    "kafka_acks_all": SystemClassification(
        system_name="Kafka (acks=all, min.insync.replicas=2)",
        cap_class=CAPClass.CP,
        pacelc_class=PACELCClass.PC_EC,
        partition_behavior=(
            "Producers get NotEnoughReplicasException if fewer than "
            "min.insync.replicas brokers are in ISR. Writes are refused. "
            "Consumers can still read committed offsets from the leader."
        ),
        normal_behavior=(
            "Every write waits for acknowledgment from all ISR replicas before "
            "returning to producer. Adds per-write latency equal to the RTT to "
            "the slowest ISR replica."
        ),
        data_loss_risk="none",
        availability_risk="medium",  # ISR shrinkage can block writes
        notes=(
            "The canonical choice for durable event streaming. Pay latency for "
            "zero data loss. Appropriate for financial events, CDC streams, audit logs."
        ),
    ),
    "kafka_acks_1": SystemClassification(
        system_name="Kafka (acks=1)",
        cap_class=CAPClass.AP,
        pacelc_class=PACELCClass.PA_EL,
        partition_behavior=(
            "Writes succeed as long as the leader responds. Replicas may be lagging. "
            "If the leader crashes before replication, committed data is lost."
        ),
        normal_behavior=(
            "Write returns after leader disk write only. "
            "No cross-replica network wait. Minimum write latency."
        ),
        data_loss_risk="medium",
        availability_risk="none",
        notes=(
            "Appropriate for high-throughput, loss-tolerant streams: metrics, "
            "click events, non-critical logs. NOT appropriate for financial data."
        ),
    ),
    "postgres_sync_replication": SystemClassification(
        system_name="PostgreSQL (synchronous_commit=on, synchronous standby)",
        cap_class=CAPClass.CP,
        pacelc_class=PACELCClass.PC_EC,
        partition_behavior=(
            "If the synchronous standby is unreachable, the primary waits indefinitely "
            "for acknowledgment. All writes block until the partition heals or the "
            "standby is removed from synchronous_standby_names."
        ),
        normal_behavior=(
            "Every write waits for the standby to flush the WAL to disk. "
            "Write latency includes cross-replica network RTT. "
            "Across AZs: adds ~2ms. Cross-region: adds ~100ms+."
        ),
        data_loss_risk="none",
        availability_risk="high",  # Full write pause during partition
        notes=(
            "Use for Airflow metadata DB, dbt target, any store where data loss is "
            "unacceptable. The write pause during partition may require a standby "
            "removal procedure (ALTER SYSTEM) to restore availability."
        ),
    ),
    "postgres_async_replication": SystemClassification(
        system_name="PostgreSQL (synchronous_commit=off or async standby)",
        cap_class=CAPClass.AP,
        pacelc_class=PACELCClass.PA_EL,
        partition_behavior=(
            "Writes succeed immediately. Replication falls behind. "
            "On failover to standby, up to replication_lag seconds of data may be lost."
        ),
        normal_behavior=(
            "Writes return after WAL buffer write (sub-millisecond). "
            "No replication wait. Maximum write throughput."
        ),
        data_loss_risk="low",  # Typically < 1s of lag in healthy systems
        availability_risk="none",
        notes=(
            "Appropriate for read replicas (reporting workloads) where some staleness "
            "is acceptable. Not appropriate as primary for transactional data if RPO > 0."
        ),
    ),
    "cassandra_consistency_one": SystemClassification(
        system_name="Cassandra (consistency_level=ONE)",
        cap_class=CAPClass.AP,
        pacelc_class=PACELCClass.PA_EL,
        partition_behavior=(
            "Reads and writes succeed as long as any single replica is reachable. "
            "Partitioned nodes serve stale reads. Concurrent writes to different "
            "replicas are reconciled via last-write-wins (LWW) on read repair."
        ),
        normal_behavior=(
            "Write acknowledged after one replica responds. "
            "Read served by one replica (possibly stale). Lowest latency. "
            "Most available but weakest consistency."
        ),
        data_loss_risk="medium",
        availability_risk="none",
        notes=(
            "Default Cassandra behavior. Good for time-series data, sensor readings, "
            "access logs where eventual consistency is acceptable."
        ),
    ),
    "cassandra_consistency_quorum": SystemClassification(
        system_name="Cassandra (consistency_level=QUORUM, RF=3)",
        cap_class=CAPClass.CP,
        pacelc_class=PACELCClass.PC_EC,
        partition_behavior=(
            "With RF=3, QUORUM=2. If only 1 replica is available, reads and writes fail. "
            "System sacrifices availability for consistency during partition."
        ),
        normal_behavior=(
            "Write requires 2/3 replicas to acknowledge. Read requires 2/3 replicas. "
            "Write + Read at QUORUM guarantees reading your own writes."
        ),
        data_loss_risk="none",
        availability_risk="medium",
        notes=(
            "QUORUM reads + QUORUM writes provide strong consistency for RF=3. "
            "Required for Cassandra use cases with strict consistency requirements."
        ),
    ),
    "zookeeper": SystemClassification(
        system_name="Apache ZooKeeper",
        cap_class=CAPClass.CP,
        pacelc_class=PACELCClass.PC_EC,
        partition_behavior=(
            "If the leader cannot maintain quorum (majority of followers available), "
            "it stops processing writes and becomes read-only or unavailable. "
            "ZAB consensus requires majority quorum."
        ),
        normal_behavior=(
            "All writes go through the leader and are replicated via ZAB before "
            "acknowledging. Reads can be served by any node but may be slightly stale "
            "unless sync() is called."
        ),
        data_loss_risk="none",
        availability_risk="high",
        notes=(
            "CP by design. Used for distributed coordination, leader election, "
            "configuration management. Small cluster (3-5 nodes), not for large-scale "
            "data storage. Kafka historically depended on ZooKeeper for CP guarantees."
        ),
    ),
    "dynamodb_eventual": SystemClassification(
        system_name="DynamoDB (eventually consistent reads)",
        cap_class=CAPClass.AP,
        pacelc_class=PACELCClass.PA_EL,
        partition_behavior=(
            "Continues serving reads from available nodes. Writes succeed if at least "
            "one replica responds. Partitioned replicas may return stale data."
        ),
        normal_behavior=(
            "Writes replicated asynchronously to 3 availability zones. "
            "Reads served by nearest replica (may be stale by milliseconds to seconds)."
        ),
        data_loss_risk="low",
        availability_risk="none",
        notes="Good for high-throughput read workloads where slight staleness is acceptable.",
    ),
    "dynamodb_strong": SystemClassification(
        system_name="DynamoDB (strongly consistent reads)",
        cap_class=CAPClass.CP,
        pacelc_class=PACELCClass.PC_EC,
        partition_behavior=(
            "Strongly consistent reads fail if the primary replica is unavailable. "
            "Trades availability for consistency during partition."
        ),
        normal_behavior=(
            "Reads always go to the primary replica. Higher latency than eventually "
            "consistent reads. Costs 2× read capacity units."
        ),
        data_loss_risk="none",
        availability_risk="low",
        notes="Use for shopping carts, account balances, any read that must be current.",
    ),
}


def classify_system(system_key: str) -> Optional[SystemClassification]:
    """Look up the CAP/PACELC classification for a known system."""
    return SYSTEM_CLASSIFICATIONS.get(system_key)


def compare_systems(system_keys: list[str]) -> None:
    """Print a comparison table for a set of systems."""
    print("\n" + "=" * 90)
    print(f"{'System':<40} {'CAP':<6} {'PACELC':<10} {'Data Loss':<12} {'Avail Risk'}")
    print("-" * 90)
    for key in system_keys:
        sc = SYSTEM_CLASSIFICATIONS.get(key)
        if sc:
            print(
                f"{sc.system_name[:38]:<40} "
                f"{sc.cap_class.value:<6} "
                f"{sc.pacelc_class.value:<10} "
                f"{sc.data_loss_risk:<12} "
                f"{sc.availability_risk}"
            )
    print("=" * 90)


# ─── Partition Simulation ─────────────────────────────────────────────────────────

@dataclass
class Node:
    node_id: str
    data: dict = field(default_factory=dict)
    is_leader: bool = False
    is_partitioned: bool = False
    write_count: int = 0
    stale_reads: int = 0


class SimpleDistributedStore:
    """
    Simplified simulation of CP vs AP behavior during a network partition.
    Not a real distributed system — illustrates the behavioral difference.
    """

    def __init__(self, mode: str = "CP", node_count: int = 3):
        assert mode in ("CP", "AP"), "mode must be 'CP' or 'AP'"
        self.mode = mode
        self.nodes = {f"node-{i}": Node(f"node-{i}") for i in range(node_count)}
        self.nodes["node-0"].is_leader = True
        self._lock = threading.Lock()

    def _get_leader(self) -> Optional[Node]:
        for node in self.nodes.values():
            if node.is_leader and not node.is_partitioned:
                return node
        return None

    def _get_replicas(self) -> list[Node]:
        return [n for n in self.nodes.values() if not n.is_leader]

    def simulate_partition(self, node_id: str):
        """Partition a node from the rest of the cluster."""
        if node_id in self.nodes:
            self.nodes[node_id].is_partitioned = True
            print(f"[PARTITION] Node {node_id} is now partitioned from cluster")

    def heal_partition(self, node_id: str):
        """Heal a node's partition."""
        if node_id in self.nodes:
            self.nodes[node_id].is_partitioned = False
            # Sync data from leader (simplified)
            leader = self._get_leader()
            if leader:
                self.nodes[node_id].data = dict(leader.data)
            print(f"[HEAL] Node {node_id} reconnected and synced")

    def write(self, key: str, value: str) -> dict:
        """
        Write a key-value pair.
        CP mode: fails if not enough replicas are available.
        AP mode: always succeeds, accepts divergence.
        """
        with self._lock:
            leader = self._get_leader()

            if self.mode == "CP":
                # CP: require majority of nodes to be available
                available = sum(1 for n in self.nodes.values() if not n.is_partitioned)
                quorum = len(self.nodes) // 2 + 1
                if available < quorum:
                    return {
                        'success': False,
                        'error': f'NotEnoughReplicas: {available}/{len(self.nodes)} available, '
                                 f'need {quorum} (quorum)',
                        'cap_note': 'CP: sacrificing availability for consistency',
                    }
                if leader is None:
                    return {'success': False, 'error': 'No leader available', 'cap_note': 'CP'}

                # Write to leader and all available replicas
                leader.data[key] = value
                leader.write_count += 1
                for replica in self._get_replicas():
                    if not replica.is_partitioned:
                        replica.data[key] = value

                return {
                    'success': True,
                    'written_to': leader.node_id,
                    'replicated_to': [r.node_id for r in self._get_replicas()
                                      if not r.is_partitioned],
                    'cap_note': 'CP: write succeeded with quorum',
                }

            else:  # AP mode
                # AP: always accept the write on available nodes
                written_to = []
                for node in self.nodes.values():
                    if not node.is_partitioned:
                        node.data[key] = value
                        node.write_count += 1
                        written_to.append(node.node_id)

                # Partitioned nodes keep their old value → divergence
                diverged = [n.node_id for n in self.nodes.values() if n.is_partitioned]

                return {
                    'success': True,
                    'written_to': written_to,
                    'diverged_nodes': diverged,
                    'cap_note': (
                        'AP: write succeeded but diverged nodes have stale data'
                        if diverged else 'AP: write succeeded, all nodes updated'
                    ),
                }

    def read(self, key: str, node_id: str = None) -> dict:
        """
        Read a key from a specific node (or the leader by default).
        AP systems may return stale data from partitioned nodes.
        """
        with self._lock:
            if node_id is None:
                leader = self._get_leader()
                if leader is None:
                    return {'success': False, 'error': 'No leader available'}
                node = leader
            else:
                node = self.nodes.get(node_id)
                if node is None:
                    return {'success': False, 'error': f'Node {node_id} not found'}

            if node.is_partitioned and self.mode == "CP":
                return {
                    'success': False,
                    'error': f'Node {node_id} is partitioned and CP mode refuses stale reads',
                    'cap_note': 'CP: sacrificing availability (read refused)',
                }

            value = node.data.get(key)
            return {
                'success': True,
                'value': value,
                'from_node': node.node_id,
                'node_partitioned': node.is_partitioned,
                'cap_note': (
                    'AP: reading from partitioned node (may be stale)'
                    if node.is_partitioned else 'Reading from healthy node'
                ),
            }

    def show_state(self):
        """Display the current state of all nodes."""
        print(f"\n{'='*50}")
        print(f"Store mode: {self.mode}")
        for node in self.nodes.values():
            status = "LEADER" if node.is_leader else "REPLICA"
            partitioned = " [PARTITIONED]" if node.is_partitioned else ""
            print(f"  {node.node_id} ({status}){partitioned}: {node.data}")
        print('='*50)


# ─── PACELC Latency Estimator ─────────────────────────────────────────────────────

def estimate_pacelc_latency(
    write_latency_local_ms: float = 1.0,
    replica_rtt_ms: float = 2.0,
    replica_write_ms: float = 0.5,
    replication_mode: str = "sync",
) -> dict:
    """
    Estimate write latency under synchronous vs asynchronous replication.
    Demonstrates the PACELC E-side (Latency vs Consistency) trade-off.

    replication_mode:
      "sync"  → EC: wait for all replicas (high consistency, high latency)
      "async" → EL: return immediately, replicate in background (low latency, some risk)
      "semi"  → EC for majority, async for rest (compromise)
    """
    if replication_mode == "sync":
        # Synchronous: total = local write + replica RTT + replica write
        total_ms = write_latency_local_ms + replica_rtt_ms + replica_write_ms
        return {
            'mode': 'synchronous (EC)',
            'write_latency_ms': round(total_ms, 2),
            'components': {
                'local_write_ms': write_latency_local_ms,
                'replica_rtt_ms': replica_rtt_ms,
                'replica_write_ms': replica_write_ms,
            },
            'consistency': 'STRONG — data guaranteed on replicas before client ACK',
            'data_loss_on_crash': 'NONE — replicas have the data',
            'pacelc': 'EC',
        }
    elif replication_mode == "async":
        # Asynchronous: total = local write only
        total_ms = write_latency_local_ms
        return {
            'mode': 'asynchronous (EL)',
            'write_latency_ms': round(total_ms, 2),
            'components': {
                'local_write_ms': write_latency_local_ms,
                'replica_rtt_ms': 0,
                'replica_write_ms': 0,
            },
            'consistency': 'EVENTUAL — replicas may lag by up to replication delay',
            'data_loss_on_crash': f'UP TO {replica_rtt_ms + replica_write_ms:.1f}ms of data',
            'pacelc': 'EL',
        }
    elif replication_mode == "semi":
        # Semi-sync: wait for one replica, async for rest
        total_ms = write_latency_local_ms + replica_rtt_ms + replica_write_ms
        return {
            'mode': 'semi-synchronous',
            'write_latency_ms': round(total_ms, 2),
            'consistency': 'DURABLE on primary + 1 replica; eventual for rest',
            'data_loss_on_crash': 'Data on primary + 1 replica guaranteed; rest may lag',
            'pacelc': 'EC (for one replica)',
        }


def print_cap_pacelc_guide():
    """Print a decision guide for choosing system consistency level."""
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║          CAP / PACELC Decision Guide for Data Engineers             ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Question 1: What happens if writes are temporarily refused?         ║
║  ├─ Pipeline halts/alerts → CP is OK                                ║
║  └─ Pipeline must continue at all costs → AP required               ║
║                                                                      ║
║  Question 2: What is the cost of reading stale data?                ║
║  ├─ Financial loss / incorrect decisions → need CP (strong reads)   ║
║  └─ Slightly outdated metrics/counts acceptable → AP reads OK       ║
║                                                                      ║
║  Question 3: What is the write latency budget?                      ║
║  ├─ < 5ms required → async replication (EL)                        ║
║  └─ Latency acceptable, durability paramount → sync (EC)           ║
║                                                                      ║
║  Question 4: What is the RPO (Recovery Point Objective)?            ║
║  ├─ RPO = 0 (zero data loss) → synchronous replication (EC)       ║
║  └─ RPO > 0 acceptable → async replication (EL)                   ║
║                                                                      ║
║  Common patterns:                                                    ║
║  • Kafka ingest (financial/CDC) → acks=all, min.isr=2 (CP/EC)     ║
║  • Kafka ingest (metrics/logs)  → acks=1 (AP/EL)                  ║
║  • Metadata store (Airflow DB)  → sync Postgres replica (CP/EC)   ║
║  • Analytics cache (Redis)      → async (AP/EL, cache is lossy)   ║
║  • Cassandra time-series        → ONE consistency (AP/EL)          ║
╚══════════════════════════════════════════════════════════════════════╝
""")


# ─── Demo ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== CAP/PACELC Analysis Demo ===\n")

    # 1. Compare systems
    print("System Classifications:")
    compare_systems([
        "kafka_acks_all", "kafka_acks_1",
        "postgres_sync_replication", "postgres_async_replication",
        "cassandra_consistency_one", "cassandra_consistency_quorum",
        "zookeeper",
    ])

    # 2. Simulate CP behavior
    print("\n--- CP Store Simulation ---")
    cp_store = SimpleDistributedStore(mode="CP", node_count=3)
    print("Write before partition:")
    print(cp_store.write("user:1", "Alice"))

    print("\nSimulating partition of node-1 and node-2 (only node-0 available)...")
    cp_store.simulate_partition("node-1")
    cp_store.simulate_partition("node-2")

    print("\nAttempting write during partition:")
    print(cp_store.write("user:2", "Bob"))
    # Expected: fails — quorum not met (CP behavior)

    # 3. Simulate AP behavior
    print("\n--- AP Store Simulation ---")
    ap_store = SimpleDistributedStore(mode="AP", node_count=3)
    print("Write before partition:")
    print(ap_store.write("user:1", "Alice"))

    print("\nSimulating partition of node-1...")
    ap_store.simulate_partition("node-1")

    print("\nWrite during partition (to node-0 and node-2):")
    print(ap_store.write("user:2", "Bob"))
    # Expected: succeeds but node-1 diverges (AP behavior)

    print("\nRead from partitioned node-1 (stale read):")
    print(ap_store.read("user:2", "node-1"))
    # Returns None or old value — stale read

    ap_store.show_state()
    ap_store.heal_partition("node-1")
    ap_store.show_state()

    # 4. PACELC latency
    print("\n--- PACELC Latency Estimates ---")
    for mode in ["async", "sync", "semi"]:
        result = estimate_pacelc_latency(
            write_latency_local_ms=1.0,
            replica_rtt_ms=50.0,   # Cross-AZ: ~2ms. Cross-region: ~50ms
            replica_write_ms=1.0,
            replication_mode=mode,
        )
        print(f"\n{result['mode']}:")
        print(f"  Write latency: {result['write_latency_ms']}ms")
        print(f"  Consistency:   {result['consistency']}")
        print(f"  Data loss risk: {result['data_loss_on_crash']}")

    print_cap_pacelc_guide()
```

---

## 6. Visual Reference

### CAP Triangle

```
                    Consistency (C)
                        /\
                       /  \
                      /    \
                     /  CA  \
                    / (single \
                   /   node)   \
                  /─────────────\
                 /      │        \
                /   CP  │  AP     \
               /        │          \
              /  (real  │ (real     \
             / systems) │ systems)   \
            ─────────────────────────
         Partition              Availability
         Tolerance (P)               (A)

CA is only possible without partitions (single-node systems).
Real distributed systems must have P. Choice is between C and A.
```

### PACELC Decision Matrix

```
                  DURING PARTITION          ELSE (Normal)
                 ┌───────────────┐         ┌───────────────┐
                 │  PA  vs  PC   │         │  EL  vs  EC   │
                 └───────────────┘         └───────────────┘

System           Partition behavior        Normal behavior
──────────────────────────────────────────────────────────
Kafka acks=all   PC (refuse writes)        EC (wait for ISR ACK)
Kafka acks=1     PA (accept writes)        EL (leader-only ACK)
Cassandra ONE    PA (all nodes accept)     EL (one replica ACK)
Cassandra QUORUM PC (quorum required)      EC (quorum required)
Postgres sync    PC (writes block)         EC (wait for standby)
Postgres async   PA (writes succeed)       EL (no standby wait)
ZooKeeper        PC (quorum required)      EC (ZAB consensus)
DynamoDB strong  PC (strong reads fail)    EC (primary only)
DynamoDB eventual PA (all nodes serve)    EL (async replication)
```

### Write Path: Sync vs Async Replication

```
SYNCHRONOUS (EC — consistent normally):

  Client ──write──► Primary ──replicate──► Replica
                       │                      │
                       │◄─────── ACK ──────────┘
                       │
                    ACK to Client
  
  Total latency = local_write + RTT + replica_write
  Cross-AZ (2ms RTT): ~3-4ms per write
  Cross-region (50ms RTT): ~102ms per write

ASYNCHRONOUS (EL — low latency normally):

  Client ──write──► Primary ──ACK──► Client
                       │
                       └──async──► Replica  (background, no wait)
  
  Total latency = local_write only
  Typical: ~0.5-2ms per write
  Risk: data between last sync and crash is LOST on failover
```

---

## 7. Common Mistakes

**Mistake 1: Treating CAP as a one-time system selection rather than a per-operation choice.** Many systems (Cassandra, DynamoDB, Riak) allow consistency levels to be set per request. This means the same system can be CP for some operations and AP for others. Designing a pipeline that uses `QUORUM` reads/writes for financial aggregations and `ONE` for metrics collection within the same Cassandra cluster is a valid and common pattern. CAP classification describes a system's default behavior, not its only behavior.

**Mistake 2: Assuming "eventual consistency" means "eventually correct."** Eventual consistency means replicas will converge to the same value given no new writes and sufficient time. It does not mean the value will be the "correct" or "intended" value. In Cassandra's Last Write Wins (LWW) reconciliation, if two conflicting writes happen during a partition and both reach different replicas, the winner is determined by timestamp — which means a slightly-later-timestamped stale write can overwrite a more-recent meaningful write. This is a data correctness problem, not just a staleness problem.

**Mistake 3: Configuring `acks=all` without setting `min.insync.replicas`.** `acks=all` means "wait for all in-sync replicas to acknowledge." But "in-sync replicas" is determined by `min.insync.replicas`. If `min.insync.replicas=1` (the default), then `acks=all` only requires one replica (the leader itself) to acknowledge — equivalent to `acks=1`. The correct CP configuration is `acks=all` + `min.insync.replicas=2` (for RF=3 clusters).

**Mistake 4: Believing network partitions only happen in multi-datacenter setups.** Partitions happen within a single AZ due to: misconfigured security groups blocking inter-broker traffic, a switch firmware bug dropping certain packets, a VM migration causing brief network interruption, or a GC pause causing a broker to miss heartbeats and be declared down. CP systems fail closed (refuse writes) during these events; AP systems fail open (accept potentially inconsistent writes). Neither is wrong — it depends on the application's requirements.

**Mistake 5: Treating availability and latency as the same trade-off.** PACELC distinguishes them. A system can be CP (consistent during partitions, sacrificing availability) but still be EL (low latency normally, sacrificing consistency). This is the "PC/EL" quadrant — rare in practice, but possible with lazy replication schemes. Conversely, a system can be PA (available during partitions) but EC (high consistency normally). Understanding that partition behavior and normal-case behavior are independent is the key insight of PACELC over CAP.

---

## 8. Production Failure Scenarios

### Scenario 1: Kafka ISR Shrinkage Blocks Data Pipeline

**Symptoms:** At 3:22 AM, a Kafka producer in the ingestion service starts throwing `org.apache.kafka.common.errors.NotEnoughReplicasException`. The pipeline monitoring page shows zero events being written. The Kafka broker health check is green (all three brokers are responding). The alert fires on the pipeline's SLO for data freshness.

**Root cause:** One of the three Kafka brokers experienced a GC pause lasting 45 seconds — long enough to be removed from the ISR (In-Sync Replicas). The topic was configured with `min.insync.replicas=3` (requiring all three brokers). With only two brokers in ISR, the producer with `acks=all` receives `NotEnoughReplicasException` on every write attempt. The system is behaving correctly per its CP configuration: it refuses writes that cannot be durably replicated to all three brokers.

**This is CAP at work:** Kafka made the CP trade-off — sacrificing availability (refusing writes) to maintain consistency (ensuring no data is written to fewer than the required replicas).

**Resolution:**
```bash
# Check ISR for the affected topic
kafka-topics.sh --describe --topic events \
  --bootstrap-server localhost:9092
# Look for: Isr: 1,3 (missing broker 2)

# Option A: Wait for GC to complete and broker 2 to rejoin ISR (correct fix)
# Option B (emergency): Temporarily lower min.insync.replicas
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name events \
  --alter --add-config min.insync.replicas=2
# Restores writes at the cost of reduced durability until broker 2 rejoins

# Monitor ISR recovery
watch -n 5 kafka-topics.sh --describe --topic events \
  --bootstrap-server localhost:9092
```

### Scenario 2: Postgres Synchronous Replication Pause During Network Hiccup

**Symptoms:** All writes to the Airflow metadata database stall for 90 seconds at 11:15 AM. Airflow's scheduler shows tasks stuck in `queued` state (cannot write task instance status updates). Grafana shows `pg_stat_activity.wait_event_type = 'Lock'` on the primary. The Airflow application logs show write timeouts.

**Root cause:** A 90-second network maintenance window caused brief packet loss between the Postgres primary (us-east-1a) and the synchronous standby (us-east-1b). The primary's `synchronous_commit=on` configuration caused all writes to block, waiting for the standby's WAL acknowledgment. The standby was temporarily unreachable. Writes queued up at the primary, and applications waiting for those writes timed out.

**This is PACELC at work:** The system made the EC trade-off — ensuring consistency (no write is committed without standby acknowledgment) at the cost of elevated latency (all writes blocked until the network recovered).

**Long-term fix:** For Airflow's metadata database (where some data loss on failover is acceptable given Airflow's own recovery mechanisms), switch to asynchronous replication or `synchronous_commit=local` (WAL flushes locally but does not wait for standby):
```sql
-- Allow WAL flush locally without waiting for standby
ALTER SYSTEM SET synchronous_commit = 'local';
SELECT pg_reload_conf();
-- Benefit: eliminates write blocking during network hiccups
-- Cost: up to replication_lag seconds of data may be lost on failover
```

### Scenario 3: Cassandra Stale Reads Cause Double-Counting in Metrics Pipeline

**Symptoms:** The daily active users (DAU) metric for the previous day is reported as 1.23M at 8 AM but increases to 1.27M when re-queried at 10 AM. The metrics are supposed to be immutable (previous-day data should not change). The data engineering team initially suspects a pipeline bug.

**Root cause:** The metrics pipeline writes aggregated counts to Cassandra with `consistency_level=ONE`. A Cassandra node was briefly partitioned during the write. The count `1.23M` was written to two of three nodes. The third node (which served the 8 AM read) had the old value. At 10 AM, the read hit a different node which had received the updated value. This is not a double-count — it is an AP stale read: the eventually consistent system served stale data until anti-entropy repaired the divergence.

**Resolution:** For metrics that are logically immutable (previous-day aggregations), use `consistency_level=QUORUM` for both reads and writes. This ensures that QUORUM reads always return the most recent QUORUM-written value:
```python
from cassandra.policies import ConsistencyLevel

# For metrics writes (immutable, must be consistent)
session.execute(
    "INSERT INTO daily_metrics (date, metric, value) VALUES (%s, %s, %s)",
    (yesterday, 'dau', 1270000),
    timeout=5.0,
)
session.default_consistency_level = ConsistencyLevel.QUORUM
```

### Scenario 4: DynamoDB Strongly Consistent Reads Cause Latency Spike

**Symptoms:** After a "hardening" exercise where a data engineer changed all DynamoDB reads in the pipeline to `ConsistentRead=True` (strongly consistent), pipeline latency increased from P99=8ms to P99=95ms. Throughput dropped by 40%. The engineer was trying to prevent stale reads after a recent incident.

**Root cause:** Strongly consistent reads in DynamoDB are routed to the primary replica in the partition's leader AZ. All reads in the same AZ that was slightly farther from the primary now include a cross-AZ network hop. Additionally, strongly consistent reads consume twice the read capacity units as eventually consistent reads — the pipeline hit throughput throttling.

**This is PACELC EC penalty:** Switching from EL to EC doubled the read cost and added cross-AZ latency. The correct fix was not to switch everything to strong reads but to identify which reads actually required strong consistency (writes followed immediately by reads of the same item) and use `ConsistentRead=True` only for those specific cases.

---

## 9. Performance and Tuning

### Choosing Consistency Levels for Throughput

The performance impact of consistency levels is directly proportional to the number of replicas that must respond before returning to the client.

For Cassandra with RF=3:
- `ONE`: latency of the fastest replica (best case ~1ms)
- `QUORUM` (2 of 3): latency of the second-fastest replica (~2ms)
- `ALL`: latency of the slowest replica (~5ms, includes stragglers)

The tail latency amplification effect (M27's "slowest node sets the pace") means that `ALL` consistency in Cassandra has disproportionately high P99 latency compared to `ONE`.

For Kafka, the ISR size determines write latency:
- `acks=1`: leader disk flush only (~1ms)
- `acks=all`, ISR size=3: median latency of all three replicas (~3ms within a single AZ)
- `acks=all`, cross-AZ replicas: adds the cross-AZ RTT to every write (typically 2–10ms per AZ hop)

### Tuning `min.insync.replicas` for Availability vs Durability

For a Kafka topic with RF=3:

| `min.insync.replicas` | Durability | Availability |
|---|---|---|
| 1 | Survives leader crash only (1 copy guaranteed) | Maximum: writes succeed with any 1 broker |
| 2 | Survives 1 broker crash (2 copies guaranteed) | Medium: tolerates 1 broker failure |
| 3 | Requires all 3 replicas (all copies guaranteed) | Minimum: any single broker failure blocks writes |

The standard production recommendation is `min.insync.replicas=2` with RF=3. This provides strong durability (data on 2 brokers before ACK) while tolerating 1 broker failure before writes are blocked.

---

## 10. Interview Q&A

**Q1: Explain the CAP theorem in your own words and apply it to three specific data systems you have worked with.**

The CAP theorem states that a distributed system can guarantee at most two of three properties: Consistency (every read sees the most recent write), Availability (every non-failed node responds to every request), and Partition Tolerance (the system functions despite network partitions). Since network partitions are an inescapable reality of any system running over a real network — cables fail, switches misconfigure, cloud AZs lose connectivity — partition tolerance is non-negotiable. The real choice is between consistency and availability when a partition occurs.

Kafka with `acks=all` and `min.insync.replicas=2` is a CP system. When the ISR shrinks below `min.insync.replicas` due to broker failures or network issues, Kafka refuses new writes rather than accepting writes that might not be replicated. A producer gets `NotEnoughReplicasException`. This is the CP trade-off: the system refuses requests (unavailable) to maintain the guarantee that every written message is on at least two brokers (consistent).

Apache Cassandra with the default `consistency_level=ONE` is an AP system. Each Cassandra node can independently accept reads and writes. During a partition, both sides of the partition continue serving requests, accepting writes that will be reconciled later via last-write-wins. The system remains available (every request gets a response) but at the cost of consistency (reads may return stale data from a partitioned node that has not received recent writes).

Postgres with synchronous replication is also CP. If the synchronous standby is unreachable, the primary waits indefinitely for write acknowledgment. All writes block — the primary is effectively unavailable for writes — rather than proceeding with writes that would not be on the standby. This is the correct behavior for a system designated as the source of truth for transactional data.

**Q2: Explain PACELC and why it is more useful than CAP for everyday data platform decisions.**

CAP describes what happens during a network partition. But partitions are rare — a well-run data platform might experience a true network partition once a month. The operational decision engineers face daily is not "what happens during partitions?" but "how should we balance latency and consistency under normal network conditions?" PACELC answers this.

PACELC adds a second dimension: During normal operation (Else), systems must choose between Latency (L) and Consistency (C). Synchronous replication makes the EC choice: every write waits for acknowledgment from replicas before returning to the client, guaranteeing that the data is durable on multiple nodes. But it adds the cross-replica network round-trip to every write latency. Asynchronous replication makes the EL choice: writes return immediately after the local write, minimizing latency, but replicas may lag behind. If the primary crashes before replication, data is lost.

For a data platform, the PACELC E-side trade-off comes up in practical decisions constantly: should Kafka producers use `acks=all` (EC: every write waits for ISR acknowledgment) or `acks=1` (EL: leader-only acknowledgment, minimum latency)? Should Postgres standby replication be synchronous (EC: every write waits for standby) or asynchronous (EL: writes return immediately)? These are EL vs EC decisions, not partition decisions. PACELC gives the vocabulary to reason about both the partition case and the common case, making it more actionable than CAP alone.

**Q3: A data engineering team is debating whether to use Cassandra or Postgres for a new pipeline that writes immutable daily aggregation metrics. One engineer argues Cassandra is better because it scales horizontally. Another argues Postgres is better because it's strongly consistent. How do you evaluate this?**

I'd start by asking what consistency properties the pipeline actually requires. Immutable aggregation metrics are written once per day and then read many times. The key question is: can readers tolerate reading stale data? If the metrics are logically immutable once written (no updates), the consistency risk is during the initial write window, not afterward.

The horizontal scaling argument for Cassandra is valid but needs context. How large is the dataset? How many concurrent readers? If the dataset is a few hundred million rows of daily metrics, a single well-tuned Postgres instance handles it without any scaling pressure. Horizontal scaling matters when you exceed single-node capacity — and for daily aggregations, that threshold is very high.

The consistency argument for Postgres is valid: Postgres with synchronous replication gives you strong consistency. But Cassandra at `QUORUM` consistency (with RF=3) also gives you strong consistency for reads and writes — the QUORUM read always returns the QUORUM-written value. Cassandra's consistency is configurable.

The practical recommendation: if this is a new pipeline with uncertain scale requirements and the team already operates Postgres, start with Postgres. Operational complexity of adding a new system (Cassandra) has a real cost. If and when you hit Postgres's scaling limits (and with daily metrics, you probably won't), migrate. If the team already operates Cassandra at scale, using it with `QUORUM` consistency for immutable metrics is entirely appropriate. The decision should be driven by operational expertise, scale requirements, and consistency requirements — not by an abstract CAP classification.

---

## 11. Cross-Question Chain

**Interviewer:** We're designing a system that tracks user account balances and needs to handle payments. Where does it sit on the CAP spectrum and why?

**Candidate:** Account balances are a canonical CP use case. A payment debit reads the current balance and writes the new balance — if two concurrent payments read the same (stale) balance and both proceed, we overdraft. Strong consistency is required. We need the CP position: the system should refuse to process a payment rather than risk processing it with stale balance data. In practice, this means using a CP database like Postgres with synchronous replication or a consensus-based store, and designing the payment write path to use serializable isolation or row-level locks to prevent concurrent writes to the same account.

**Interviewer:** That system is deployed in two AWS regions for redundancy. How does your CAP analysis change?

**Candidate:** Multi-region synchronous replication with CP semantics means every write must cross the inter-region network (50–200ms round-trip) before returning to the client. A payment that takes 150ms just for replication latency is likely too slow for a user-facing application. This is the PACELC EC penalty applied to the multi-region case.

The practical solution is to not try to maintain strict CP semantics across regions in real time. Instead: route all writes for an account to a primary region (single-master per account), accept that the secondary region is asynchronously replicated (AP/EL in the normal case, but only for the secondary — the primary is still CP within its region). On failover, use a recovery period to reconcile any transactions that were in-flight during the failover. This is how most payment systems actually work — they are CP within a region but accept the consistency/latency trade-off across regions.

**Interviewer:** What is PACELC and how does it apply to the cross-region case you just described?

**Candidate:** PACELC extends CAP by describing the trade-off in the normal (non-partition) case. During a Partition, you choose Availability vs Consistency — that's the CAP piece. Else (normally), you choose Latency vs Consistency. The cross-region case hits the EL vs EC trade-off head-on: synchronous cross-region replication gives EC (every write acknowledged by both regions) but adds 150ms+ of latency. Asynchronous replication gives EL (writes return in milliseconds) but means the secondary region may be seconds behind.

For the payment system: the primary region should be EC within itself (synchronous replication within the AZ, strong consistency for balance reads). Cross-region should be EL (asynchronous replication to the secondary, with explicit recovery procedures on failover). This is `PC/EC` in the primary region (consistent during partition, consistent normally within the region) and `PA/EL` for the cross-region leg (available during partition across regions, low latency for the cross-region replication).

**Interviewer:** You said "explicit recovery procedures on failover." What does that mean in practice?

**Candidate:** When the primary region fails and traffic fails over to the secondary, the secondary may have missed the last few seconds of write traffic due to replication lag. Recovery means: first, preventing any new writes from proceeding until you've assessed the state. Second, comparing the secondary's state against any write logs you have (application logs, the pending transaction queue) to identify what was committed at the primary but not yet replicated. Third, either replaying those writes or applying compensating transactions (refunds, reversals) as appropriate.

This is the concept of an RPO (Recovery Point Objective) — how much data loss is acceptable. If RPO = 0 (zero data loss), you must use synchronous replication everywhere, including cross-region, accepting the latency. If RPO = 5 seconds (you can lose up to 5 seconds of data), asynchronous replication with a maximum lag of 5 seconds is acceptable, and the recovery procedure handles anything in that window.

The classic pattern for payment systems is a two-phase approach: write to the primary and a durable queue (Kafka) simultaneously. On failover, replay the queue against the new primary to recover any committed-but-not-replicated writes. The queue is the bridge between the EL (asynchronous replication) cross-region leg and the zero-data-loss requirement.

**Interviewer:** Zoom out: when is an AP system the RIGHT choice for a data engineering use case, even though it sacrifices consistency?

**Candidate:** AP is the right choice when three conditions hold simultaneously: the consequence of a stale read is acceptable (re-querying gives the right answer eventually), write throughput or write latency requirements exceed what CP systems can provide, and the data is naturally idempotent or append-only so convergence produces the correct result.

Concretely: time-series metrics collection is a perfect AP use case. A Prometheus counter stores event counts; if a partition causes two replicas to diverge, the eventual convergence (summing the two partial counts) produces the correct total. Using AP semantics (Cassandra at `ONE`, or a simple append-to-S3 model) allows ingesting millions of metrics per second without the latency overhead of synchronous consensus.

A second example: CDC-based event logs where the source of truth is the upstream database. Kafka with `acks=1` for CDC events is an AP position — acceptable because even if a few events are lost from Kafka, they can be re-read from the source database during a backfill. The source of truth is the database (CP), not Kafka (AP in this configuration). Kafka is used for distribution, not as the system of record.

The decision principle: AP is correct when you have an external CP source of truth that events are derived from, or when convergence semantics naturally produce the right answer (as with commutative/associative operations like sum, max, union).

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | State the CAP theorem. | A distributed data store cannot simultaneously provide Consistency (every read sees the most recent write), Availability (every non-failing node responds), and Partition Tolerance (functions despite network partitions). Must choose 2. Since P is required in real systems, choice is CP or AP. |
| 2 | What is the formal definition of consistency in CAP? | Linearizability: operations appear to take effect atomically at a single point in time, and in an order consistent with real time. Every read returns the most recent write or an error — never stale data. |
| 3 | Why can you never choose "CA" in a real distributed system? | Partition tolerance is non-optional. Any system communicating over a real network can experience packet loss, link failure, or routing issues. A CA system would have to stop functioning entirely during a partition to avoid violating either C or A — equivalent to a single-node system. |
| 4 | What does PACELC stand for? | During Partition: Availability vs Consistency. Else (normal operation): Latency vs Consistency. PACELC extends CAP by characterizing the latency/consistency trade-off in the common case (no partition). |
| 5 | Where does Kafka (acks=all, min.insync.replicas=2) sit in PACELC? | PC/EC — Consistent during Partition (refuses writes when ISR < min.isr), Consistent normally (every write waits for all ISR replicas to acknowledge before returning to producer). |
| 6 | Where does Cassandra (consistency_level=ONE) sit in PACELC? | PA/EL — Available during Partition (all nodes continue serving), Low Latency normally (write returns after single replica acknowledges, no cross-replica wait). |
| 7 | What is the CP behavior of Postgres with synchronous replication during a partition? | If the synchronous standby is unreachable, the primary pauses all writes indefinitely, waiting for standby acknowledgment. Writes block rather than proceeding without replication — sacrificing availability for consistency. |
| 8 | What configuration makes Kafka effectively not CP even with acks=all? | Setting `min.insync.replicas=1`. With min.isr=1, `acks=all` only requires the leader itself to acknowledge — equivalent to `acks=1`. Both min.isr=2 AND acks=all are required for true CP behavior. |
| 9 | What is "last-write-wins" (LWW) and which system uses it? | A conflict resolution strategy where concurrent writes are resolved by comparing timestamps — the write with the higher timestamp wins. Cassandra uses LWW by default. Risk: a slightly-later-timestamped stale write can overwrite a more-recent meaningful write during partition recovery. |
| 10 | What is the latency penalty of synchronous cross-region replication? | Write latency = local write time + cross-region RTT (50–200ms) + remote write time. A write that takes 1ms locally takes 100–200ms with synchronous cross-region replication. This is the PACELC EC penalty. |
| 11 | What does `min.insync.replicas=2` with `replication-factor=3` guarantee? | Every acknowledged write is on at least 2 of 3 brokers. The cluster can tolerate 1 broker failure without losing data. If 2 brokers fail simultaneously, writes are refused (CP behavior). |
| 12 | What is an "asymmetric partition"? | Node A can send to Node B, but B's responses are dropped. From A's perspective, messages are sent but no ACK arrives. From B's perspective, it received the message but its response was dropped. Neither node can determine what the other node knows. |
| 13 | Why is ZooKeeper classified as CP? | ZooKeeper uses ZAB (ZooKeeper Atomic Broadcast), a consensus protocol requiring quorum (majority of nodes). If the leader cannot maintain quorum, it stops processing writes rather than serving potentially inconsistent responses. |
| 14 | What is Google Spanner's PACELC classification and why is it unusual? | PC/EC (consistent during partitions, consistent normally) with relatively low latency. Unusual because it achieves this via GPS + atomic clock hardware (TrueTime), which bounds global clock uncertainty without distributed consensus on each read — hardware not available in commodity infrastructure. |
| 15 | What does "eventual consistency" actually guarantee? | Replicas will converge to the same value given: (1) no new writes, and (2) sufficient time for anti-entropy to run. It does NOT guarantee the converged value is the "correct" or most-recent value — conflict resolution semantics (LWW, CRDT, etc.) determine what "converged" means. |
| 16 | What is the ISR (In-Sync Replicas) in Kafka and how does it enforce CP behavior? | ISR is the set of replicas that are sufficiently caught up to the leader. With `acks=all`, a write is only acknowledged when all ISR replicas have written it. If ISR shrinks below `min.insync.replicas`, writes are refused — CP enforcement. |
| 17 | How can DynamoDB behave as both CP and AP depending on configuration? | DynamoDB allows per-request `ConsistentRead` flag. `ConsistentRead=False` (default): eventually consistent reads from any replica (AP/EL). `ConsistentRead=True`: strongly consistent reads from the primary only (PC/EC). Same system, different position on the CAP spectrum per request. |
| 18 | What is the RPO (Recovery Point Objective) and how does it relate to synchronous replication? | RPO is the maximum acceptable amount of data loss, measured in time. Synchronous replication achieves RPO=0: every committed write is on replicas before ACK. Asynchronous replication has RPO > 0: up to the replication lag can be lost. RPO directly determines whether EC or EL is required. |
| 19 | Why is a "gray failure" (slow network, not complete failure) dangerous for CP systems? | CP systems use timeouts to detect failures. A slow network may trigger the timeout, causing the system to declare a partition and make a CP decision (refuse writes or deny reads) — even though the nodes are actually still connected. This causes unnecessary availability loss. |
| 20 | Give a concrete example where AP is the RIGHT architectural choice. | Time-series metrics ingestion with Cassandra at `consistency_level=ONE`. Event counts are commutative — lost events can be detected and backfilled, and partial counts converge correctly on anti-entropy. The source of truth is the instrumented system; Cassandra is a derived store. High write throughput (millions of events/second) would be impossible with synchronous CP semantics. |

---

## 13. Further Reading

- **"CAP Twelve Years Later: How the 'Rules' Have Changed" by Eric Brewer (2012):** Brewer himself revisiting and nuancing CAP — the "CA is not viable" clarification and the observation that partition tolerance can be managed, not just accepted. Essential context from the theorem's author.
- **"Perspectives on the CAP Theorem" by Gilbert and Lynch (2012):** The formal proof of CAP, written accessibly enough to be read by practitioners. Establishes the precise definitions of C, A, and P that make the theorem exact.
- **"PACELC" by Daniel J. Abadi (2012), IEEE Computer:** The original PACELC paper. Short and direct. Explains why CAP's focus on partitions misses the operational trade-off in the normal case.
- **"Designing Data-Intensive Applications" by Martin Kleppmann, Chapter 9:** The most accessible treatment of consistency and consensus for practitioners. Covers linearizability, causality, and the total order broadcast problem. The figure showing the relationship between consistency models is worth framing.
- **"Cassandra: The Definitive Guide" — Chapter on Consistency:** Practical treatment of Cassandra's tunable consistency model, the QUORUM formula, and the trade-offs at each consistency level.
- **Kafka documentation — "Replication":** The canonical description of ISR, leader election, `min.insync.replicas`, and `acks` semantics. Directly maps to CP/AP positions for different configurations.

---

## 14. Lab Exercises

**Exercise 1: CAP Classification Challenge**

For each of the following systems, determine the CAP classification (CP or AP) and the PACELC classification, given the stated configuration. Write a one-paragraph justification for each: (1) Redis Cluster with default settings, (2) Apache HBase, (3) CockroachDB, (4) Elasticsearch with `index.number_of_replicas=1` and `index.write.wait_for_active_shards=2`, (5) MySQL Group Replication.

**Exercise 2: Kafka CP Demonstration**

Using a local Kafka cluster (3 brokers via Docker Compose), create a topic with `replication-factor=3`, `min.insync.replicas=2`. Write a Python producer with `acks=all`. Stop broker 2 and broker 3. Observe the `NotEnoughReplicasException`. Then change `min.insync.replicas=1` via `kafka-configs.sh` and observe that writes succeed again. Document: what changed, what the trade-off is, and under what production conditions you'd accept `min.insync.replicas=1`.

**Exercise 3: PACELC Latency Benchmark**

Using a local Postgres instance, benchmark write latency under four settings: (1) `synchronous_commit=off`, (2) `synchronous_commit=local`, (3) `synchronous_commit=on` with no standby, (4) `synchronous_commit=on` with a local standby (add a streaming replica). Compare latencies and explain the difference between each setting. Use the `measure_write_latency` function from the code toolkit as a starting point.

**Exercise 4: Simulate Partition in SimpleDistributedStore**

Using the `SimpleDistributedStore` class from the code toolkit: (1) Create a CP store with 3 nodes; write a key; partition node-1 and node-2 (only node-0 available); attempt to write — observe failure; (2) Heal the partition; (3) Repeat with AP mode — observe that writes succeed even during partition; observe the stale read from the partitioned node; observe state divergence; observe convergence after healing. Write a brief analysis of when each behavior is correct.

**Exercise 5: Consistency Level Impact on Cassandra (or Simulated)**

Either using a local Cassandra cluster or the theoretical framework: calculate the expected latency and availability impact of `ONE`, `QUORUM`, and `ALL` consistency levels for a 3-node Cassandra cluster where node latencies are 1ms, 2ms, and 5ms (simulating geographic distribution). At each consistency level, what is the minimum latency? What is the latency if the slowest node is unavailable? When would you choose each?

---

## 15. Key Takeaways

The CAP theorem's operational implication reduces to a single decision: when a network partition occurs, should the system refuse requests (CP) or serve them with potentially stale data (AP)? Neither answer is universally correct — it depends entirely on the application's tolerance for data loss versus its tolerance for unavailability.

PACELC extends this to the everyday case: even without partitions, synchronous replication (EC) adds cross-replica latency to every write, while asynchronous replication (EL) writes at minimum latency but accepts potential data loss. The EC/EL choice directly determines write latency, RPO, and operational complexity.

The practical classification for common data engineering systems: Kafka with `acks=all` + `min.insync.replicas=2` is CP/EC — the right choice for durable event streaming. Cassandra with `ONE` is AP/EL — the right choice for high-throughput time-series with eventual consistency tolerance. Postgres with synchronous replication is CP/EC — the right choice for transactional metadata stores. These aren't rigid categories: both Cassandra and DynamoDB allow per-operation consistency tuning, and Kafka's behavior changes with `acks` and `min.isr` configuration.

The interview implication: being able to classify any distributed system on the CAP/PACELC spectrum, with specific justification tied to that system's replication and consensus mechanisms, is a Staff-level data engineering skill. The question "is X a CP or AP system?" is always followed by "explain why" and "under what configuration."

---

## 16. Connections to Other Modules

- **M38 — Consensus Algorithms:** CAP's CP position requires consensus (agreement among a quorum of nodes). Paxos and Raft are the mechanisms that implement the "C" in CP. Understanding why CP systems use quorum voting (rather than simply waiting for all nodes) requires understanding consensus algorithms.
- **M39 — Replication:** The PACELC E-side trade-off is directly implemented through replication modes — synchronous (EC) or asynchronous (EL). Replication covers the mechanism; PACELC provides the framework for evaluating the trade-off.
- **M40 — Failure Modes:** Split-brain (two nodes both believing they are the leader) is the failure that CAP partitions make possible. Network partitions don't just reduce availability — they create the conditions for data divergence that split-brain represents.
- **M41 — Distributed Transactions:** 2PC is a CP protocol — it blocks indefinitely if a participant is unavailable. Saga is AP-tolerant — it handles failures through compensating transactions rather than blocking. The choice between 2PC and Saga maps directly to the CP/AP decision.
- **DCS-KFK-101 — Kafka Internals (M-series):** Kafka's ISR, `acks`, and `min.insync.replicas` are the production manifestations of CAP in Kafka specifically. M37's theory directly explains Kafka's design choices.

---

## 17. Anti-Patterns

**Anti-pattern: Adding replicas to a CP system without accounting for the latency penalty.** Adding more synchronous replicas increases durability but increases write latency. Adding a cross-region synchronous replica to Kafka (or Postgres) adds the inter-region RTT to every write. Teams that add "disaster recovery" replicas synchronously and then complain about write latency have made an uninformed PACELC trade-off.

**Anti-pattern: Mixing CP and AP assumptions in the same pipeline.** A pipeline that writes with `acks=1` (AP) to Kafka and then reads immediately with the assumption that the data is available (CP assumption) creates a subtle race condition — the message may not yet be replicated to the broker the consumer connects to. Consistency assumptions must be consistent throughout the pipeline.

**Anti-pattern: Using eventual consistency for a use case that requires read-your-own-writes.** A user registration service that writes a new account to Cassandra with `ONE` consistency and then immediately reads it (also with `ONE`) from a different replica will intermittently fail to find the account. Read-your-own-writes requires either: (1) routing reads to the same node as the write, (2) using `QUORUM` for both, or (3) a short wait after write before read.

**Anti-pattern: Treating "the database is CP" as a complete consistency guarantee.** CP guarantees linearizability at the storage level. It does not prevent application-level races. Two concurrent reads followed by two concurrent writes at the application layer can still produce an inconsistent state even in a CP database, unless the application uses transactions, compare-and-swap operations, or other higher-level consistency mechanisms.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `kafka-topics.sh --describe` | Check Kafka ISR status | See current ISR, identify shrinkage |
| `kafka-configs.sh --alter` | Change `min.insync.replicas` at runtime | Emergency: lower min.isr to restore writes |
| `psql: SHOW synchronous_commit` | Check Postgres sync commit mode | Verify EC vs EL configuration |
| `psql: ALTER SYSTEM SET synchronous_commit` | Change sync mode | Tune EC/EL trade-off |
| `cassandra-cli: consistency QUORUM` | Set Cassandra consistency level | Switch AP→CP behavior per session |
| `nodetool status` | Check Cassandra ring status | Identify downed nodes, ISR equivalent |
| `pg_stat_replication` | Monitor Postgres replica lag | `sent_lsn - replay_lsn` for EL lag |
| `cap_pacelc_analyzer.py compare_systems` | Print CAP/PACELC comparison table | Quick reference for system classification |
| `cap_pacelc_analyzer.py SimpleDistributedStore` | Simulate CP vs AP behavior | Lab demonstration of partition behavior |

---

## 19. Glossary

**Availability (CAP):** Every request received by a non-failed node must result in a response. Note: this is stronger than uptime — a system that routes to a single primary is not "available" in CAP terms if the primary is partitioned and the secondary refuses to serve.

**CAP Theorem:** Proved by Brewer (conjecture) and Gilbert/Lynch (formal proof). A distributed system cannot simultaneously guarantee Consistency, Availability, and Partition Tolerance.

**Consistency (CAP):** Linearizability — every read reflects the most recent write, as if operations executed atomically and sequentially on a single node.

**CP System:** A distributed system that chooses Consistency + Partition Tolerance. During a partition, it becomes unavailable (refuses requests) rather than serving stale data. Examples: ZooKeeper, Kafka with acks=all, Postgres with synchronous replication.

**AP System:** A distributed system that chooses Availability + Partition Tolerance. During a partition, it continues serving requests with potentially stale or divergent data. Examples: Cassandra (default), DynamoDB (eventually consistent).

**PACELC:** During Partition: Availability vs Consistency; Else (normal): Latency vs Consistency. Framework by Daniel Abadi extending CAP to describe the normal-case trade-off.

**EC (PACELC):** Else Consistent — the system ensures consistency in the normal case, adding cross-replica wait to every operation. Synchronous replication is EC.

**EL (PACELC):** Else Low-Latency — the system prioritizes low latency in the normal case, allowing some replica lag. Asynchronous replication is EL.

**Eventual Consistency:** A guarantee that replicas will converge to the same value given no new writes and sufficient time. Does not specify how quickly, nor what "same value" means in the presence of concurrent conflicting writes.

**ISR (In-Sync Replicas):** Kafka's mechanism for tracking which replicas are sufficiently up-to-date with the leader. The ISR is the set of replicas that have acknowledged the leader's most recent writes. `acks=all` requires acknowledgment from all ISR members.

**Last Write Wins (LWW):** A conflict resolution strategy that resolves concurrent writes by their wall-clock timestamp — the write with the higher timestamp wins. Used by default in Cassandra. Vulnerable to clock skew and to out-of-order delivery.

**Linearizability:** The formal correctness condition for strong consistency. Operations appear instantaneous and atomic, taking effect at a single point in real time between invocation and return.

**Partition Tolerance:** The system continues operating correctly despite arbitrary message loss or delay between nodes. Required for any system running over a real network.

**RPO (Recovery Point Objective):** The maximum acceptable data loss, measured as time. RPO=0 requires synchronous replication (EC). RPO>0 allows asynchronous replication (EL).

**Split-brain:** A failure mode where two nodes (or sets of nodes) both believe they are the authoritative primary and begin accepting writes independently. Produces divergent state that is difficult to reconcile. Prevented by CP systems using quorum consensus.

---

## 20. Self-Assessment

1. A three-broker Kafka cluster has `min.insync.replicas=2` and `acks=all`. Broker 2 experiences a GC pause and is removed from the ISR. Only brokers 1 and 3 are in the ISR. A producer tries to write. Does the write succeed? Explain why, citing CAP/PACELC.
2. Describe the exact sequence of events that leads to stale reads in Cassandra with `consistency_level=ONE` during a brief partition of one node.
3. What is the PACELC classification for DynamoDB with strongly consistent reads? What about with eventually consistent reads? What configuration change moves it between the two?
4. A data engineer configures Postgres with an asynchronous standby for a pipeline metadata store. The primary crashes during a batch write. What data is lost? How much? What determines the answer?
5. Explain why `acks=all` without `min.insync.replicas=2` does not provide CP guarantees in Kafka.
6. A 5-node Cassandra cluster has RF=3. The team uses `consistency_level=QUORUM`. How many nodes must respond for a write to succeed? What happens if 2 nodes are partitioned?
7. Why is the partition in "partition tolerance" non-negotiable in real distributed systems? What real-world events cause partitions beyond total network outages?
8. A payment processing system uses async replication (AP/EL) for its primary Postgres. The primary crashes and fails over to the standby, which is 8 seconds behind. Describe what recovery looks like. What data is at risk and what mechanisms can recover it?
9. What makes Google Spanner's PC/EC classification unusual compared to commodity distributed databases, and what hardware requirement makes it possible?
10. You are building a user activity event stream. Users generate ~10,000 events/second. Some events may be lost during broker failures. Would you choose CP (`acks=all`) or AP (`acks=1`)? Justify in terms of CAP and PACELC, citing the acceptable RPO and the operational trade-offs.

---

## 21. Module Summary

The CAP theorem establishes the foundational constraint of distributed systems design: Consistency, Availability, and Partition Tolerance cannot all be simultaneously guaranteed. Since network partitions are inescapable in any real distributed system, the practical choice is between Consistency (CP — refuse requests when consistency cannot be guaranteed) and Availability (AP — serve requests even when data may be stale).

PACELC extends this to the common case: even when partitions don't occur, systems must trade off Latency (EL — respond immediately using asynchronous replication) against Consistency (EC — wait for replica acknowledgment using synchronous replication). The everyday architectural decision of synchronous vs asynchronous replication is a PACELC EC/EL choice, not a CAP choice.

The classifications matter because they determine failure behavior. A CP system like Kafka with `acks=all` will refuse writes when ISR shrinks below `min.insync.replicas` — alerting you to a problem but maintaining data integrity. An AP system like Cassandra with `ONE` will continue accepting writes during partition — remaining available but creating divergence that must be reconciled. Neither is universally correct; the choice depends on whether unavailability or data inconsistency is the more acceptable failure mode for a given use case.

The interview answer to "classify X" is never just "CP" or "AP" — it's CP or AP, the PACELC position, the specific configuration that determines the position, and the failure mode that results from that position. For Kafka: CP/EC with `acks=all` and `min.insync.replicas=2`, producing `NotEnoughReplicasException` when ISR shrinks. For Cassandra: AP/EL with `ONE`, producing stale reads during partition. For synchronous Postgres: CP/EC, producing write pauses when the synchronous standby is unreachable. These concrete answers demonstrate the understanding that matters in production systems design.

The next module — M38: Consensus Algorithms — explains the mechanism that makes CP systems work: how distributed nodes agree on a value in the presence of failures, forming the foundation of Raft (used by Kafka KRaft, etcd, CockroachDB) and Paxos (used by Google Chubby, Apache Zookeeper's ZAB).
