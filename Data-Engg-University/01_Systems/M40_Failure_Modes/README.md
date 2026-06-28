# M40: Failure Modes

**Course:** SYS-DST-101 — Distributed Systems Theory  
**Module:** 04 of 05  
**Global Module ID:** M40  
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

Every distributed system eventually fails. The question is not whether your Kafka cluster will experience a partition, whether your Postgres primary will crash, or whether a network switch will drop packets — it's when, and whether your system handles it gracefully or catastrophically. The data engineering systems that cause the most damage in production are not the ones that fail in obvious ways (a process crashes, a disk fills up) but the ones that fail in subtle ways: a system that appears healthy while silently corrupting data, a cluster that splits into two independent islands each believing it is the authoritative truth, a single slow service that inexplicably brings down the entire pipeline.

Understanding failure modes is the difference between being the engineer who diagnoses an incident in 15 minutes and the one who spends three hours chasing symptoms while the root cause hides. More importantly, it is the difference between designing systems that fail gracefully (degraded performance, clear error signals) and systems that fail catastrophically (silent data loss, cascading outages).

This module covers four failure modes that every senior and staff data engineer must understand at depth: split-brain (two nodes that each believe they are the authoritative leader), network partition (the separation of nodes into islands that cannot communicate), partial failure (some components fail while others succeed, creating an inconsistent system state), and cascading failure (a failure in one component that propagates and amplifies through the system, ultimately taking down components that were initially healthy).

These are not academic curiosities. Split-brain caused MongoDB to acknowledge writes to two primaries simultaneously, losing data in a widely-reported 2011 incident. Network partitions caused Kafka ISR collapses. Partial failures cause the most subtle and dangerous class of distributed systems bugs — the ones where the system returns success to the client but the operation was not fully applied. Cascading failures are the mechanism behind most large-scale production outages.

---

## 2. Mental Model

### Why Failure Modes Matter More Than Failure Rates

In a distributed system, the failure rate of individual components is less important than the system's behavior when those components fail. A system with three 99.9% available components running in series has at best 99.7% availability (0.999³). But a system where each component fails gracefully degrades to reduced capacity, while a system where component failures cascade is unavailable until all failing components are recovered.

The mental model for failure analysis is: **for every component, ask "what happens when this is partially available?"** Not just "what happens when this crashes" (clean failure) but "what happens when this is slow?" "what happens when this returns wrong data?" "what happens when this is reachable by some nodes but not others?" Clean failures are easy to handle — you detect them quickly and reroute. Partial failures are insidious because they look like success.

### The Failure Mode Taxonomy

```
Failure Modes in Distributed Systems
├── By cause
│   ├── Node failure (crash, OOM, hardware)
│   ├── Network failure (partition, packet loss, latency)
│   └── Byzantine failure (incorrect/malicious behavior)
├── By observability
│   ├── Clean failure (immediate and detectable)
│   ├── Gray failure (intermittent, hard to detect)
│   └── Silent failure (no error signal; incorrect behavior)
└── By scope
    ├── Local (single node/process)
    ├── Component (single service cluster)
    └── Systemic (cascading across services)
```

Data engineering systems almost exclusively deal with crash failures and network failures — not Byzantine failures (which require cryptographic defenses). The most operationally significant failure types are gray failures and silent failures, precisely because they defeat the simple "is it up?" monitoring that most teams rely on.

---

## 3. Core Concepts

### 3.1 Split-Brain

Split-brain occurs when two (or more) nodes in a cluster each believe they are the authoritative leader, and both are actively processing writes. In a database context, this means two nodes independently accepting writes — diverging data that can never be automatically reconciled without data loss.

**How split-brain happens in practice:**

In a single-leader replication setup with leader election, split-brain occurs when:
1. The leader fails (or appears to fail due to a network partition)
2. A new leader is elected
3. The old leader recovers and does not realize it has been replaced
4. Both leaders accept writes from clients

The gap between step 1 and step 3 is the split-brain window. If the old leader never properly checks whether it has been demoted before accepting writes, both nodes will diverge.

**Why split-brain is dangerous:**
- Two nodes accept different writes for the same key
- When the partition heals, there is no safe automatic reconciliation — both writes claim to be authoritative
- If the system resolves via LWW (timestamp), one write is silently discarded
- If the system resolves via merge, the merged state may be semantically invalid
- If the system takes the later timestamp unconditionally, a write that arrived later physically but with an earlier clock is dropped

**Mechanisms that prevent split-brain:**

**Fencing (STONITH — Shoot The Other Node In The Head):** Before promoting a standby, the HA controller sends a fencing command to the old leader — power-cycling it, removing its network access, or unmounting its storage volume. The old leader is incapacitated before the new leader starts accepting writes. This guarantees that there is at most one active leader at any time.

**Lease-based leadership:** A leader holds a time-bounded lease from a consensus system (etcd, ZooKeeper). The lease is renewed periodically. If the leader cannot renew its lease (cannot reach etcd), it voluntarily stops accepting writes — it doesn't know whether a new leader has been elected. The lease duration sets a bound on split-brain exposure: at most `lease_duration` seconds of divergent writes before the old leader steps down.

**Epoch-based write rejection:** Each leadership term has an epoch number. Writes are tagged with the leader's epoch. Followers reject writes from leaders with a stale epoch (lower than the current epoch). If an old leader recovers and attempts writes, its epoch is lower than the new leader's epoch — followers reject it.

**Kafka and split-brain:** Kafka's partition leadership is managed by the KRaft controller (or ZooKeeper, pre-KRaft). The controller is the single source of truth for partition leadership. If a leader loses connectivity to the controller and a new leader is elected, the old leader will eventually see responses from followers (or the controller) with a higher epoch and voluntarily cease being leader. The key: Kafka followers reject produce/fetch requests from leaders with a stale epoch, effectively fencing the old leader from the follower cluster without a hard hardware-level STONITH operation.

**Postgres and split-brain:** Postgres does not have native split-brain prevention built in. It relies on the HA controller (Patroni, Stolon) to implement fencing. Patroni uses distributed lock-based leadership: only the node holding the DCS lock (in etcd or ZooKeeper) is the primary. If a node loses the DCS lock, it immediately stops accepting writes. If it cannot access the DCS to check its lock status, it demotes itself. Patroni also optionally executes STONITH scripts to power-cycle the old primary before promoting the standby.

### 3.2 Network Partition

A network partition is a failure in which some nodes in a cluster can communicate with each other but not with others, splitting the cluster into isolated islands. This is not a node failure — all nodes are running. The failure is in the network between them.

**Types of network partitions:**

**Complete partition:** Node set A cannot communicate with node set B at all. Symmetric: A cannot reach B, B cannot reach A.

**Asymmetric partition:** Node A can send messages to B, but B's responses are dropped (A cannot hear B). Or A can hear B but B cannot hear A. Asymmetric partitions are particularly dangerous for distributed systems because they defeat simple "can I reach you?" health checks.

**Partial partition:** Some nodes in set A can reach some nodes in set B, but not all. A can reach B1 but not B2. This creates a graph of connectivity rather than clean isolation.

**Gray failure:** The connection between A and B is not broken — it exists but is severely degraded. Packets are delivered but with 5-second delays. From A's perspective, B looks like it's down (heartbeats time out). From B's perspective, A looks like it's down. Both trigger leader elections. Neither partition is total — if timeouts were longer, both would see each other as alive.

**Partition behavior in consensus systems (Raft, M38):** In a 3-node Raft cluster split 2-1, the majority island (2 nodes) retains quorum and can elect a leader. The minority island (1 node) cannot elect a leader (needs 2 votes out of 3). The minority island goes unavailable for writes. When the partition heals, the minority node catches up from the majority node's log. This is the correct CP behavior: sacrifice availability (minority island) to maintain consistency (no split-brain).

**Partition behavior in eventual consistency systems (Cassandra):** With `QUORUM` consistency and a 2-1 partition in a 3-node cluster, the majority island (2 nodes) satisfies quorum and continues operating. The minority island (1 node) can serve reads with `ONE` consistency but fails `QUORUM` reads/writes. If the application uses `LOCAL_QUORUM` in a multi-DC setup, each DC can continue operating independently — enabling "availability" at the cost of cross-DC consistency.

**What partitions look like from the application:**
- Connections time out (network drops packets entirely)
- Connections stall indefinitely (packets delayed, no RST/FIN)
- Partial responses (first part of a response arrives, then hangs)
- RPCs succeed but responses are never received
- The system enters an indefinite "waiting for quorum" state

**Detecting partitions:** TCP connections will not fail immediately during a partition — TCP retransmits until the retransmit timeout (TCP RTO, typically 90 seconds by default on Linux). Without application-level heartbeats with aggressive timeouts, a network partition can go undetected for 90 seconds. Always use application-level heartbeats (or SO_KEEPALIVE with short intervals) rather than relying on TCP to detect broken connections.

### 3.3 Partial Failure

Partial failure is the condition where a distributed operation succeeds on some components and fails on others, leaving the system in an inconsistent intermediate state. The caller may receive success, failure, or no response at all — and any of these can happen while the system is in a partially-applied state.

**Why partial failure is the hardest distributed systems problem:**
In a local function call, a function either succeeds or fails — there is no "partially succeeded." In a distributed call, the function call itself can succeed on some nodes while failing on others. The network can drop the request before it arrives (easy: client retries). The network can drop the response after the server applied the change (hardest: the server applied it, the client doesn't know, the client retries and applies it a second time).

**Partial failure examples in data engineering:**

**Kafka partial produce:** A producer sends a batch of 10 messages. The batch is partially written to the leader before the leader crashes. Some messages may be in the leader's log but not yet replicated to followers. The new leader may not have those messages. The producer receives a timeout (no ACK). Should the producer retry? If yes, there's a risk of duplicate delivery. If no, messages are lost.

Kafka's solution: **idempotent producer** (`enable.idempotence=true`). The broker assigns each producer a `producer_id` and tracks a sequence number per partition. If a retry delivers a duplicate message (same producer_id + sequence_number), the broker deduplicates silently. This converts partial failure (uncertain delivery) into exactly-once semantics within a session.

**Database partial commit:** A transaction updates tables A and B. The write to A succeeds. The write to B fails (disk error, timeout). Without transactions, A is updated but B is not — inconsistent state. With ACID transactions and two-phase commit, either both succeed or neither succeeds — atomicity.

**Multi-step pipeline partial failure:** An ETL job reads from Kafka, transforms, and writes to two output systems: a data warehouse and a notifications table. Step 1 (Kafka consume) succeeds. Step 2 (warehouse write) succeeds. Step 3 (notifications write) fails. The warehouse has new data but no notifications were sent. If the job retries from the last committed Kafka offset, the warehouse gets duplicate writes (unless idempotent). If the job retries from scratch, the warehouse write is duplicated again.

The universal solution to partial failure is **idempotency**: design every operation so that executing it multiple times has the same effect as executing it once. If every write operation is idempotent (upsert with a stable key, rather than append), partial failure is converted from "inconsistent state" to "at-most-once applied state" — retries are safe.

**The network timeout problem:** When a network call to a remote service times out, the caller does not know whether the request was received and applied, was received but not applied, or was never received. This uncertainty is the core of partial failure. The three possible responses to a timeout are: retry (risk of duplicate), abandon (risk of data loss), or check (query the server for the current state before deciding). For idempotent operations, retry is always safe. For non-idempotent operations, always check before retrying.

### 3.4 Cascading Failure

Cascading failure is a failure mode where an initial failure in one component triggers failures in other components that were initially healthy, propagating and amplifying through the system until a large-scale outage occurs. Each component's failure adds load or reduces capacity for neighboring components, driving them into failure.

**The core mechanism: load redistribution.** When component A fails, the traffic it was handling is redirected to healthy components B and C. B and C are now handling more traffic than before. If they were near their capacity limits, the additional load pushes them into failure. Their failure redistributes load to the remaining components, which are now handling three components' worth of traffic. The cascade accelerates.

**Cascading failure in data engineering:**

**Consumer group cascade:** A Kafka consumer group has 10 consumers. Each processes 1,000 events/second. Total throughput: 10,000 events/second. One consumer dies. The Kafka consumer group rebalances, redistributing its partitions to the remaining 9 consumers. Each now processes ~1,111 events/second. If the consumers are processing near CPU capacity, the extra 11% load pushes them to 100% CPU. Processing slows down. Consumers begin timing out. Kafka's session.timeout.ms fires, Kafka thinks more consumers are dead, and triggers another rebalance. The rebalance itself halts processing for 30-60 seconds (stop-the-world rebalance). During the halt, messages pile up. When the rebalance completes, consumers face a massive backlog. The backlog causes memory pressure (buffered messages). Memory pressure causes GC pauses. GC pauses cause consumer heartbeat failures. Kafka triggers another rebalance. The cascade is underway.

**Retry storm:** A downstream service becomes slow (not down — just slow). Clients have a 5-second timeout. Requests pile up waiting for responses. The client's request queue fills. New requests are rejected immediately with "queue full." But the clients are also retrying the timed-out requests — immediately, without backoff. The downstream service is now receiving its normal traffic (new requests) plus all the retried traffic (3-5× normal at peak retry). The additional load makes the downstream service slower, increasing timeouts, increasing retries. The cascade amplifies.

**Connection pool exhaustion:** Service A calls Service B using a connection pool of 100 connections. Service B becomes slow. Service A's 100 connections are all in-use waiting for slow B. New requests to A find no available connection and fail immediately. The failures are returned to A's callers as errors. A's callers retry. The retries require connections that aren't available. The connection pool is exhausted for the duration of B's slowness.

**Mitigation mechanisms:**

**Circuit breaker:** Tracks failure rate to a downstream service. When the failure rate exceeds a threshold (e.g., 50% failure in the last 10 seconds), the circuit "opens" and immediately rejects all calls to that service without attempting them. This prevents retries from amplifying load on the failing service. After a configured timeout, the circuit enters "half-open" state and allows one trial call. If it succeeds, the circuit closes. If not, it opens again. Circuit breakers convert cascading failures into controlled degradation.

**Bulkhead:** Isolate resource pools per downstream dependency. Instead of one shared connection pool of 100 connections for all downstream calls, allocate dedicated pools: 30 connections for Service B, 30 for Service C, 40 for other. If Service B fails and exhausts its 30 connections, Service C's 30 connections are unaffected. The failure is contained within the bulkhead.

**Rate limiting and backpressure:** Producers slow down when consumers are falling behind, rather than continuing to generate load that piles up. In Kafka: if consumer lag grows beyond a threshold, upstream producers should reduce their produce rate or delay writes. In HTTP services: use response codes (429 Too Many Requests) to signal to callers that they should back off.

**Exponential backoff with jitter:** When a request fails, wait before retrying — and wait longer on each successive failure. Add randomization (jitter) to prevent "thundering herd" — all clients retrying at exactly the same time after a retry delay, which would create a synchronized pulse of load on the recovering service.

**Shed load gracefully:** When a service is overloaded, it should reject new requests quickly (fast 503) rather than queuing them and slowing down all responses. A slow 503 after 30 seconds is much worse than an immediate 503 — the slow response ties up the caller's resources for 30 seconds before informing it that the call failed.

### 3.5 Gray Failure

Gray failure deserves special treatment because it is the failure mode that most teams are least prepared for. A gray failure is a partial degradation that is difficult to detect with standard binary health checks ("is it up?") but causes significant system-level disruption.

**Characteristics of gray failures:**
- The node is alive and responding to health checks (HTTP 200 on /health)
- But its performance is severely degraded (P99 latency 10× normal)
- Or its success rate is 80% (20% of requests silently fail or return wrong data)
- Or it is reachable by some nodes but not others (asymmetric partition)

**Why gray failures are dangerous:**
- Load balancers do not remove the node (it passes health checks)
- The node continues receiving traffic (at normal volume)
- Responses are slow or incorrect
- Error rates are elevated but below alarm thresholds
- The gray node becomes a "black hole" that pulls work in and returns degraded results

**Detecting gray failures:**
Binary health checks (`/health` returns 200 or 503) do not detect gray failures. Effective detection requires:
- **Latency percentile monitoring:** Alert on P99 latency, not just availability. A node with 200ms P99 latency when others have 5ms P99 is experiencing gray failure.
- **Per-node error rate:** Track error rates per instance, not just aggregate. A single node with 20% error rate is invisible in a 10-node cluster (aggregate error rate = 2%).
- **Canary probing:** Proactively send test requests with known correct answers and verify the response. A probe that sends `SELECT 1` and expects `1` will detect a database node that is running but returning corrupted data.
- **Cross-node latency comparison:** Compare response times across all instances continuously. A node whose P95 latency is 3× the cluster median is a gray failure candidate.

**Gray failure in practice: the "hot node" in Cassandra.** One Cassandra node is on a VM whose hypervisor is heavily loaded. The node is alive (responds to gossip, passes health checks) but has 4× the disk latency of other nodes. `nodetool status` shows it as `UN` (Up Normal). But 1/3 of all read and write quorum operations route through it. Those operations see 4× higher latency than the others. The application's P99 latency is 3× the P50. The hot node is never removed from the ring because it's never "down" by health-check standards.

---

## 4. Hands-On Walkthrough

### 4.1 Observing Kafka Split-Brain Prevention via Epoch Rejection

```bash
# Check which broker is the current leader for a partition
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic events | grep "Partition: 0"
# Topic: events  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3

# Check the leader epoch — this increments on each leadership change
# If we see: leader epoch changes without a broker restart, it means
# there was a leader election (partition, crash, or manual reassignment)

# Watch for leader epoch changes over time
watch -n 2 'kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic events 2>/dev/null | grep "Partition: 0"'

# Trigger a leader change by bouncing broker 1 (the current leader)
# Stop broker 1
kafka-server-stop.sh  # on broker-1 host

# Within 30 seconds, one of brokers 2 or 3 will be elected leader
# The new leader has a higher epoch
# When broker-1 restarts and attempts to act as leader, 
# brokers 2 and 3 reject its requests (stale epoch)
# Broker-1 becomes a follower

# Verify epoch-based rejection via broker logs
# grep "leader epoch" /var/log/kafka/server.log
# You'll see messages like:
# "Handling fetch from broker 1 with leader epoch 5 while current epoch is 6 — rejecting"
```

### 4.2 Simulating a Network Partition with tc (Traffic Control)

```bash
# Simulate a network partition on a specific IP using Linux tc (traffic control)
# This drops all packets to/from 10.0.1.5 (another Kafka broker)

# Add partition (drop all packets to target IP)
sudo tc qdisc add dev eth0 root handle 1: prio
sudo tc filter add dev eth0 parent 1: protocol ip u32 \
  match ip dst 10.0.1.5/32 action drop

# Verify partition is active
ping 10.0.1.5  # Should show packet loss

# Watch Kafka respond to the partition
# - The partitioned broker's partitions will show ISR shrinkage
watch -n 2 'kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic events | grep -v "^$"'

# Remove the partition
sudo tc filter del dev eth0 parent 1:

# Alternative: use iptables for simpler syntax
sudo iptables -A INPUT  -s 10.0.1.5 -j DROP
sudo iptables -A OUTPUT -d 10.0.1.5 -j DROP

# Remove iptables rules
sudo iptables -D INPUT  -s 10.0.1.5 -j DROP
sudo iptables -D OUTPUT -d 10.0.1.5 -j DROP

# Simulate gray failure (packet delay, not drop)
sudo tc qdisc add dev eth0 root netem delay 500ms 100ms
# This adds 500ms ± 100ms latency to ALL packets on eth0
# Kafka's replication will show ISR lag but nodes remain "up"

# Remove gray failure simulation
sudo tc qdisc del dev eth0 root
```

### 4.3 Detecting Cascading Failure via Kafka Consumer Lag

```bash
# Monitor consumer group lag — the leading indicator of cascade risk
watch -n 5 'kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group pipeline-consumer 2>/dev/null \
  | awk "NR==1 || /events/"'
# Output shows: partition, current offset, log-end offset, LAG
# Lag growing = consumers falling behind = cascade risk

# Alert query: sum of lag across all partitions
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group pipeline-consumer 2>/dev/null \
  | awk 'NR>1 {sum += $5} END {print "Total lag:", sum}'

# Check consumer group member count (rebalances visible as member count changes)
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group pipeline-consumer 2>/dev/null \
  | grep -v "^$" | awk '{print $7}' | sort -u | wc -l
# If this count changes repeatedly, rebalances are occurring — potential cascade

# Key JMX metrics for cascade detection:
# kafka.consumer:type=consumer-fetch-manager-metrics,client-id=X,name=records-lag-max
#   → Max consumer lag across all partitions
# kafka.consumer:type=consumer-coordinator-metrics,client-id=X,name=rebalance-rate-per-hour
#   → Rebalance frequency (should be near 0 in steady state)
# kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
#   → Partitions where ISR < RF (warning: ISR shrinkage)
```

### 4.4 Postgres Split-Brain Prevention: Patroni Fencing Test

```bash
# Check current Patroni leader
patronictl -c /etc/patroni/patroni.yml list
# +--------+----------+---------+---------+----+-----------+
# | Member | Host     | Role    | State   | TL | Lag in MB |
# +--------+----------+---------+---------+----+-----------+
# | node-1 | 10.0.1.1 | Leader  | running |  1 |           |
# | node-2 | 10.0.1.2 | Replica | running |  1 | 0         |
# | node-3 | 10.0.1.3 | Replica | running |  1 | 0         |
# +--------+----------+---------+---------+----+-----------+

# Simulate leader failure by pausing the Patroni process on node-1
# (Not killing Postgres — testing that Patroni handles leadership transfer)
ssh node-1 "sudo systemctl stop patroni"

# Watch Patroni elect a new leader
watch -n 2 'patronictl -c /etc/patroni/patroni.yml list'
# node-2 becomes leader within ~10 seconds (2× ttl + election timeout)

# Key: Patroni has STONITH configured
# Before promoting node-2, Patroni calls the STONITH script to fence node-1
# Only after successful fencing does node-2 begin accepting writes

# Check Patroni logs for fencing evidence
journalctl -u patroni -n 50 | grep -i "demote\|fence\|promote"

# Verify old primary is truly demoted
ssh node-1 psql -c "SELECT pg_is_in_recovery();"
# Should return: t (true — it's now a standby, not a primary)

# If Patroni's STONITH fails:
# Patroni will NOT promote the standby — it will wait or raise an alert
# This is the correct safe behavior: never promote without successful fencing
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
failure_mode_simulator.py

Simulate and analyze distributed system failure modes:
  - Split-brain detection and prevention
  - Network partition simulation
  - Cascading failure analysis
  - Gray failure detection
  - Circuit breaker implementation

No external dependencies.
"""

import time
import random
import threading
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
from typing import Optional, Callable


# ─── Enums ──────────────────────────────────────────────────────────────────────

class NodeStatus(Enum):
    HEALTHY  = "healthy"
    DEGRADED = "degraded"   # Gray failure: alive but slow/partial
    ISOLATED = "isolated"   # Network partition: alive but unreachable
    CRASHED  = "crashed"    # Hard failure: process down


class CircuitState(Enum):
    CLOSED   = "closed"    # Normal operation
    OPEN     = "open"      # Failing — reject all requests
    HALF_OPEN = "half_open" # Testing recovery with limited requests


# ─── Split-Brain Detector ────────────────────────────────────────────────────────

@dataclass
class LeaderRecord:
    node_id: str
    epoch: int
    acquired_at: float
    active: bool = True


class SplitBrainDetector:
    """
    Simulate leader election with epoch-based split-brain prevention.

    The key invariant: at most one node can be leader per epoch.
    Any node that sees a higher epoch immediately steps down.
    """

    def __init__(self, node_ids: list[str]):
        self.node_ids = node_ids
        self.current_epoch = 0
        self.current_leader: Optional[str] = None
        self.leader_history: list[LeaderRecord] = []
        self._lock = threading.Lock()

    def elect_leader(self, candidate_id: str) -> tuple[bool, int]:
        """
        Attempt to elect a candidate as leader.
        Returns (success, epoch). Fails if another node holds higher epoch.
        """
        with self._lock:
            new_epoch = self.current_epoch + 1

            # Check for concurrent election (two candidates in same epoch)
            for record in self.leader_history:
                if record.epoch == new_epoch and record.active:
                    return False, new_epoch  # Split-brain detected: epoch already claimed

            self.current_epoch = new_epoch
            self.current_leader = candidate_id
            record = LeaderRecord(
                node_id=candidate_id,
                epoch=new_epoch,
                acquired_at=time.monotonic(),
            )
            self.leader_history.append(record)
            return True, new_epoch

    def revoke_leadership(self, node_id: str, reason: str = "") -> bool:
        """Revoke leadership from a node (fencing operation)."""
        with self._lock:
            for record in self.leader_history:
                if record.node_id == node_id and record.active:
                    record.active = False
                    if self.current_leader == node_id:
                        self.current_leader = None
                    print(f"  ✓ Fenced {node_id} (epoch {record.epoch})"
                          f"{': ' + reason if reason else ''}")
                    return True
        return False

    def check_epoch(self, node_id: str, claimed_epoch: int) -> bool:
        """
        A node checks if its claimed epoch is still valid.
        Returns False if a higher epoch exists (node must step down).
        This is how stale leaders are prevented from accepting writes.
        """
        with self._lock:
            if claimed_epoch < self.current_epoch:
                print(f"  ✗ {node_id} has stale epoch {claimed_epoch} "
                      f"(current is {self.current_epoch}) — must step down")
                return False
            return True

    def detect_split_brain(self) -> list[tuple[LeaderRecord, LeaderRecord]]:
        """Find any epochs where multiple active leaders exist (split-brain)."""
        split_brains = []
        epoch_leaders: dict[int, list[LeaderRecord]] = {}
        for record in self.leader_history:
            if record.active:
                epoch_leaders.setdefault(record.epoch, []).append(record)

        for epoch, records in epoch_leaders.items():
            if len(records) > 1:
                for i in range(len(records)):
                    for j in range(i + 1, len(records)):
                        split_brains.append((records[i], records[j]))

        return split_brains

    def print_history(self):
        print("\nLeader Election History:")
        for r in self.leader_history:
            status = "ACTIVE" if r.active else "REVOKED"
            print(f"  Epoch {r.epoch}: {r.node_id} [{status}]")


# ─── Partition Simulator ────────────────────────────────────────────────────────

class NetworkPartitionSimulator:
    """
    Simulate network partitions between nodes.
    Models: complete partition, asymmetric partition, gray failure.
    """

    def __init__(self, node_ids: list[str]):
        self.node_ids = set(node_ids)
        # Connectivity matrix: can_communicate[A][B] = True if A can reach B
        self.can_communicate: dict[str, dict[str, bool]] = {
            n: {m: True for m in node_ids if m != n}
            for n in node_ids
        }
        # Gray failure: packet delay (ms) per directed edge
        self.packet_delay_ms: dict[str, dict[str, float]] = {
            n: {m: 0.0 for m in node_ids if m != n}
            for n in node_ids
        }
        self._lock = threading.Lock()

    def partition(self, set_a: list[str], set_b: list[str],
                  asymmetric: bool = False):
        """
        Partition: nodes in set_a cannot communicate with nodes in set_b.
        asymmetric=True: A→B fails but B→A succeeds.
        """
        with self._lock:
            for a in set_a:
                for b in set_b:
                    self.can_communicate[a][b] = False
                    if not asymmetric:
                        self.can_communicate[b][a] = False
        direction = "A→B only" if asymmetric else "bidirectional"
        print(f"  ⚡ Partition ({direction}): {set_a} ↔ {set_b}")

    def heal(self, set_a: list[str], set_b: list[str]):
        """Heal a partition between two node sets."""
        with self._lock:
            for a in set_a:
                for b in set_b:
                    self.can_communicate[a][b] = True
                    self.can_communicate[b][a] = True
        print(f"  ✓ Partition healed: {set_a} ↔ {set_b}")

    def add_gray_failure(self, node_id: str, delay_ms: float):
        """
        Add artificial delay to all connections from a node (gray failure).
        The node stays "up" but is severely degraded.
        """
        with self._lock:
            for other in self.node_ids:
                if other != node_id:
                    self.packet_delay_ms[node_id][other] = delay_ms
                    self.packet_delay_ms[other][node_id] = delay_ms
        print(f"  ⚠ Gray failure: {node_id} has +{delay_ms}ms latency")

    def can_reach(self, source: str, target: str) -> bool:
        return self.can_communicate.get(source, {}).get(target, False)

    def get_reachable_set(self, source: str) -> set[str]:
        """Return all nodes reachable from source."""
        return {n for n in self.node_ids
                if n != source and self.can_reach(source, n)}

    def get_islands(self) -> list[set[str]]:
        """Find connected components (islands) given current partition state."""
        visited: set[str] = set()
        islands: list[set[str]] = []

        def dfs(node: str, island: set[str]):
            island.add(node)
            visited.add(node)
            for other in self.node_ids:
                if other not in visited and self.can_reach(node, other):
                    dfs(other, island)

        for node in self.node_ids:
            if node not in visited:
                island: set[str] = set()
                dfs(node, island)
                islands.append(island)

        return islands

    def analyze_quorum(self, total_nodes: int) -> list[dict]:
        """Determine if each island has quorum."""
        quorum = total_nodes // 2 + 1
        islands = self.get_islands()
        results = []
        for island in islands:
            has_quorum = len(island) >= quorum
            results.append({
                'island': sorted(island),
                'size': len(island),
                'has_quorum': has_quorum,
                'status': 'CAN ELECT LEADER' if has_quorum else 'UNAVAILABLE (no quorum)',
            })
        return results


# ─── Cascading Failure Simulator ────────────────────────────────────────────────

@dataclass
class ServiceNode:
    node_id: str
    capacity_rps: int          # Max requests per second
    current_load_rps: int = 0
    status: NodeStatus = NodeStatus.HEALTHY
    failure_threshold: float = 0.9   # Fails at 90% capacity

    @property
    def load_fraction(self) -> float:
        return self.current_load_rps / self.capacity_rps if self.capacity_rps > 0 else 0

    @property
    def is_failed(self) -> bool:
        return (self.status == NodeStatus.CRASHED or
                self.load_fraction >= self.failure_threshold)


class CascadeSimulator:
    """
    Simulate cascading failure in a service cluster.
    Each time a node fails, its load is redistributed to surviving nodes.
    """

    def __init__(self, nodes: list[ServiceNode]):
        self.nodes = {n.node_id: n for n in nodes}
        self.events: list[str] = []

    def _log(self, msg: str):
        self.events.append(msg)
        print(f"  {msg}")

    def total_capacity(self) -> int:
        return sum(n.capacity_rps for n in self.nodes.values()
                   if not n.is_failed and n.status != NodeStatus.ISOLATED)

    def healthy_nodes(self) -> list[ServiceNode]:
        return [n for n in self.nodes.values()
                if not n.is_failed and n.status != NodeStatus.ISOLATED]

    def redistribute_load(self):
        """
        When a node fails, redistribute its load evenly to healthy nodes.
        If any newly-loaded node exceeds its threshold, it also fails.
        This can trigger a cascade.
        """
        changed = True
        while changed:
            changed = False
            # Collect all load
            total_load = sum(n.current_load_rps for n in self.nodes.values())
            healthy = self.healthy_nodes()

            if not healthy:
                self._log("ALL NODES FAILED — total outage")
                return

            # Distribute load evenly
            per_node = total_load / len(healthy)
            for node in healthy:
                node.current_load_rps = int(per_node)

            # Check for new failures due to overload
            for node in healthy:
                if node.load_fraction >= node.failure_threshold:
                    node.status = NodeStatus.CRASHED
                    self._log(f"⚡ {node.node_id} FAILED (load {node.current_load_rps}"
                             f"/{node.capacity_rps} = {node.load_fraction:.0%})")
                    changed = True  # Trigger another redistribution round

    def fail_node(self, node_id: str, reason: str = "manual"):
        """Fail a specific node and trigger load redistribution."""
        node = self.nodes.get(node_id)
        if node and not node.is_failed:
            node.status = NodeStatus.CRASHED
            node.current_load_rps = 0
            self._log(f"⚡ {node_id} failed ({reason})")
            self.redistribute_load()

    def print_state(self):
        print("\n  Cluster state:")
        for node in sorted(self.nodes.values(), key=lambda n: n.node_id):
            bar = "█" * int(node.load_fraction * 20)
            spaces = "░" * (20 - int(node.load_fraction * 20))
            status_icon = "✓" if not node.is_failed else "✗"
            print(f"    {status_icon} {node.node_id}: [{bar}{spaces}] "
                  f"{node.current_load_rps}/{node.capacity_rps} rps "
                  f"({node.load_fraction:.0%}) [{node.status.value}]")


# ─── Circuit Breaker ─────────────────────────────────────────────────────────────

class CircuitBreaker:
    """
    Circuit breaker pattern to prevent cascading failures.

    States:
      CLOSED   → Normal operation. Track failure rate.
      OPEN     → Too many failures. Reject all requests immediately.
      HALF_OPEN → Test if downstream recovered. Allow one request.
    """

    def __init__(self,
                 failure_threshold: float = 0.5,    # 50% failure rate opens circuit
                 window_seconds: float = 10.0,
                 open_duration_seconds: float = 30.0,
                 half_open_max_calls: int = 3):
        self.failure_threshold = failure_threshold
        self.window_seconds = window_seconds
        self.open_duration = open_duration_seconds
        self.half_open_max_calls = half_open_max_calls

        self.state = CircuitState.CLOSED
        self._requests: deque[tuple[float, bool]] = deque()  # (timestamp, success)
        self._opened_at: Optional[float] = None
        self._half_open_successes = 0
        self._lock = threading.Lock()

    def _clean_window(self):
        """Remove requests outside the sliding window."""
        cutoff = time.monotonic() - self.window_seconds
        while self._requests and self._requests[0][0] < cutoff:
            self._requests.popleft()

    def _failure_rate(self) -> float:
        if not self._requests:
            return 0.0
        failures = sum(1 for _, success in self._requests if not success)
        return failures / len(self._requests)

    def call(self, fn: Callable, *args, **kwargs):
        """
        Execute fn through the circuit breaker.
        Raises CircuitOpenError if circuit is open.
        """
        with self._lock:
            now = time.monotonic()

            if self.state == CircuitState.OPEN:
                if now - self._opened_at >= self.open_duration:
                    self.state = CircuitState.HALF_OPEN
                    self._half_open_successes = 0
                    print(f"  ↕ Circuit HALF_OPEN: testing downstream")
                else:
                    raise CircuitOpenError(
                        f"Circuit OPEN (failure rate was "
                        f"{self._failure_rate():.0%}). "
                        f"Retry after "
                        f"{self.open_duration - (now - self._opened_at):.1f}s"
                    )

            if self.state == CircuitState.HALF_OPEN:
                if self._half_open_successes >= self.half_open_max_calls:
                    # Enough successful probes — close the circuit
                    self.state = CircuitState.CLOSED
                    self._requests.clear()
                    print(f"  ✓ Circuit CLOSED: downstream recovered")

        # Execute the call (outside lock to avoid blocking other threads)
        success = False
        try:
            result = fn(*args, **kwargs)
            success = True
            return result
        except Exception as e:
            raise
        finally:
            with self._lock:
                self._requests.append((time.monotonic(), success))
                self._clean_window()

                if success and self.state == CircuitState.HALF_OPEN:
                    self._half_open_successes += 1
                elif not success:
                    rate = self._failure_rate()
                    if (rate >= self.failure_threshold and
                            self.state == CircuitState.CLOSED and
                            len(self._requests) >= 5):  # Minimum sample size
                        self.state = CircuitState.OPEN
                        self._opened_at = time.monotonic()
                        print(f"  ✗ Circuit OPEN: failure rate {rate:.0%}")

    @property
    def current_state(self) -> CircuitState:
        with self._lock:
            return self.state


class CircuitOpenError(Exception):
    pass


# ─── Gray Failure Detector ───────────────────────────────────────────────────────

class GrayFailureDetector:
    """
    Detect gray failures by comparing per-node latency against cluster baseline.
    A node is flagged as gray-failed if its P99 latency is significantly higher
    than the cluster median.
    """

    def __init__(self, node_ids: list[str],
                 degradation_threshold: float = 3.0):  # 3× median = gray failure
        self.node_ids = node_ids
        self.threshold = degradation_threshold
        self.latency_samples: dict[str, deque[float]] = {
            n: deque(maxlen=100) for n in node_ids
        }

    def record_latency(self, node_id: str, latency_ms: float):
        self.latency_samples[node_id].append(latency_ms)

    def _percentile(self, samples: list[float], p: float) -> float:
        if not samples:
            return 0.0
        sorted_samples = sorted(samples)
        idx = int(len(sorted_samples) * p / 100)
        return sorted_samples[min(idx, len(sorted_samples) - 1)]

    def get_cluster_median_p99(self) -> float:
        """Get the median P99 latency across all healthy nodes."""
        p99s = []
        for node_id, samples in self.latency_samples.items():
            if len(samples) >= 10:
                p99s.append(self._percentile(list(samples), 99))
        if not p99s:
            return 0.0
        p99s.sort()
        return p99s[len(p99s) // 2]

    def detect_gray_failures(self) -> list[dict]:
        """Return nodes whose P99 latency is significantly above the cluster median."""
        cluster_p99 = self.get_cluster_median_p99()
        if cluster_p99 == 0:
            return []

        gray_failures = []
        for node_id, samples in self.latency_samples.items():
            if len(samples) < 10:
                continue
            node_p99 = self._percentile(list(samples), 99)
            ratio = node_p99 / cluster_p99 if cluster_p99 > 0 else 1.0
            if ratio >= self.threshold:
                gray_failures.append({
                    'node_id': node_id,
                    'node_p99_ms': round(node_p99, 2),
                    'cluster_median_p99_ms': round(cluster_p99, 2),
                    'ratio': round(ratio, 2),
                    'severity': 'CRITICAL' if ratio >= 5 else 'WARNING',
                })
        return gray_failures


# ─── Demo ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Distributed System Failure Mode Simulator ===\n")

    # ── 1. Split-brain simulation ──────────────────────────────────────────────
    print("─── 1. Split-Brain Detection ───")
    detector = SplitBrainDetector(["node-A", "node-B", "node-C"])

    # Normal election
    ok, epoch = detector.elect_leader("node-A")
    print(f"  node-A elected: epoch={epoch}")

    # node-A appears to crash (partition): node-B starts election
    detector.revoke_leadership("node-A", "partition detected")
    ok, epoch = detector.elect_leader("node-B")
    print(f"  node-B elected: epoch={epoch}")

    # Verify: if node-A tries to act as leader with stale epoch (1), it's rejected
    print(f"  node-A checks epoch 1: valid? "
          f"{detector.check_epoch('node-A', 1)}")

    # Check for split-brain (should be none — epoch 1 was revoked before epoch 2)
    split = detector.detect_split_brain()
    print(f"  Split-brain detected: {len(split)} instance(s) — "
          f"{'SAFE' if not split else 'DANGER'}")
    detector.print_history()

    # ── 2. Partition analysis ──────────────────────────────────────────────────
    print("\n─── 2. Network Partition Simulation ───")
    partition_sim = NetworkPartitionSimulator(["node-A", "node-B", "node-C"])

    # 2-1 partition (majority island: A+B, minority island: C)
    partition_sim.partition(["node-A", "node-B"], ["node-C"])
    islands = partition_sim.analyze_quorum(total_nodes=3)
    for island in islands:
        print(f"  Island {island['island']}: {island['status']}")

    # Gray failure on node-C
    partition_sim.heal(["node-A", "node-B"], ["node-C"])
    partition_sim.add_gray_failure("node-C", delay_ms=500)

    # ── 3. Cascading failure simulation ───────────────────────────────────────
    print("\n─── 3. Cascading Failure Simulation ───")
    # 5-node cluster, each handling 200 rps, total load = 1000 rps
    nodes = [ServiceNode(f"worker-{i}", capacity_rps=250, current_load_rps=200)
             for i in range(5)]
    cascade = CascadeSimulator(nodes)
    print("  Initial state:")
    cascade.print_state()

    # Fail one node — load redistributes: 1000 rps / 4 nodes = 250 rps each
    # 250/250 = 100% = at threshold → ALL nodes cascade
    print(f"\n  Failing worker-0 (load will cascade if remaining nodes hit threshold):")
    cascade.fail_node("worker-0", "disk failure")
    cascade.print_state()

    # ── 4. Circuit breaker ─────────────────────────────────────────────────────
    print("\n─── 4. Circuit Breaker ───")
    cb = CircuitBreaker(failure_threshold=0.5, window_seconds=5.0,
                        open_duration_seconds=3.0)

    call_count = 0
    def flaky_service():
        global call_count
        call_count += 1
        if call_count <= 8:  # First 8 calls fail
            raise Exception("Service unavailable")
        return "OK"

    for i in range(15):
        try:
            result = cb.call(flaky_service)
            print(f"  Call {i+1}: SUCCESS ({result}) [circuit: {cb.current_state.value}]")
        except CircuitOpenError as e:
            print(f"  Call {i+1}: CIRCUIT OPEN (fast fail)")
        except Exception as e:
            print(f"  Call {i+1}: FAIL ({e}) [circuit: {cb.current_state.value}]")
        time.sleep(0.3)

    # ── 5. Gray failure detection ──────────────────────────────────────────────
    print("\n─── 5. Gray Failure Detection ───")
    gfd = GrayFailureDetector(["node-A", "node-B", "node-C", "node-D"])

    # Normal nodes: 5-15ms latency
    for node_id in ["node-A", "node-B", "node-D"]:
        for _ in range(50):
            gfd.record_latency(node_id, random.uniform(5, 15))

    # node-C: gray failure — 200-400ms latency (but not "down")
    for _ in range(50):
        gfd.record_latency("node-C", random.uniform(200, 400))

    gray = gfd.detect_gray_failures()
    if gray:
        for g in gray:
            print(f"  ⚠ GRAY FAILURE: {g['node_id']} "
                  f"P99={g['node_p99_ms']}ms "
                  f"(cluster median: {g['cluster_median_p99_ms']}ms, "
                  f"ratio: {g['ratio']}×) [{g['severity']}]")
    else:
        print("  No gray failures detected")
```

---

## 6. Visual Reference

### Split-Brain Timeline

```
T=0: 3-node cluster  [A=LEADER][B=follower][C=follower]
     Leader epoch: 1

T=5: Network partition — A isolated from B,C
     A: still thinks it's leader (epoch 1) — continues writing
     B: no heartbeat from A → starts election
     C: no heartbeat from A → starts election

T=6: B wins election (B+C = quorum)
     New leader epoch: 2
     B: LEADER (epoch 2)  C: follower

T=7: WITHOUT FENCING:
     A: still accepts writes (epoch 1 — stale leader)
     B: also accepts writes (epoch 2 — correct leader)
     ← SPLIT-BRAIN: two active leaders

T=7: WITH FENCING:
     Before B becomes leader, HA controller power-cycles A
     A: OFFLINE
     B: LEADER (epoch 2) — safe to accept writes

T=15: Partition heals. A comes back online.
     WITH EPOCH PROTECTION:
     A: attempts writes → followers see epoch 1 < current epoch 2 → REJECTED
     A: receives rejection, sees epoch 2 → steps down to FOLLOWER
     ← No split-brain, no data divergence
```

### Network Partition Island Analysis

```
5-node cluster:  [1][2][3][4][5]
quorum = 3

Partition scenario: {1,2} ↔ {3,4,5}

Island {1,2}: size=2 < quorum=3
  → UNAVAILABLE: cannot elect leader, refuse writes (CP)

Island {3,4,5}: size=3 = quorum=3
  → AVAILABLE: can elect leader, process writes

When partition heals:
  Nodes 1,2 receive log entries from 3,4,5's log
  Any writes accepted by 1,2's minority island during partition...
  → NONE: CP prevented writes in minority island (safe)
  → If AP: 1,2 may have divergent data requiring reconciliation

Gray failure vs hard partition:
  Hard partition: packets dropped, ping fails, immediate TCP errors
  Gray failure:   packets delayed 500ms+, ping responds slowly,
                  TCP connections stall but don't close,
                  election timers fire despite connectivity existing
                  → system behaves as partitioned but harder to diagnose
```

### Cascading Failure Timeline

```
5-node consumer group: each node 200/250 rps (80% capacity)
Total: 1000 rps

T=0: [200/250][200/250][200/250][200/250][200/250]
                                         All healthy (80%)

T=1: Node-0 crashes (disk failure). Load redistributes.
     1000 rps / 4 nodes = 250 rps each
     [  DEAD  ][250/250][250/250][250/250][250/250]
                100%      100%     100%     100%
                ← All at threshold → cascade begins

T=2: Node-1 fails (overloaded). Load redistributes.
     1000 rps / 3 nodes = 333 rps each (exceeds capacity)
     [  DEAD  ][  DEAD ][333/250][333/250][333/250]
                         133%     133%     133%   ← all fail

T=3: TOTAL OUTAGE — all nodes failed

Prevention:
  - Run at 50-60% capacity (leave headroom for N-1 failures)
  - Circuit breaker: stop sending to cluster before cascade
  - Shed load: reject new work before becoming overloaded
```

---

## 7. Common Mistakes

**Mistake 1: Relying on TCP to detect network partitions promptly.** TCP's retransmit timeout (RTO) starts at ~200ms and backs off exponentially, reaching up to 120 seconds. A network partition will not cause a TCP connection to fail for up to 2 minutes by default. In that window, a Raft leader may not know its followers are unreachable; a Kafka consumer may not know the broker is partitioned. Always use application-level heartbeats with aggressive timeouts (500ms–2s) and SO_KEEPALIVE with short intervals. Never assume TCP will promptly signal a broken path.

**Mistake 2: Treating partial failure as either success or failure.** Network timeouts mean "I don't know what happened." They are not success (the request may have been applied) and not failure (the request may not have been applied). Treating a timeout as failure and retrying a non-idempotent operation causes double execution. Treating a timeout as success means missing an operation that didn't go through. The correct response to a timeout on a non-idempotent operation is: (1) retry with the same idempotent key if the operation is idempotent, or (2) query the server for the current state before deciding whether to retry.

**Mistake 3: Not leaving headroom for cascading failure.** Operating a cluster at 90% capacity is dangerous. When one node fails in a 10-node cluster at 90% capacity, the remaining 9 nodes take 100/9 ≈ 111% of their capacity — they immediately fail. A cluster operating at 50% capacity can tolerate losing half its nodes before load redistribution causes any node to exceed 100%. For production clusters, the standard guidance is: run at 60-70% of total capacity to tolerate the failure of any single node without triggering a cascade.

**Mistake 4: Using binary health checks to detect gray failures.** A `/health` endpoint that returns HTTP 200 tells you the process is running. It doesn't tell you whether the process is serving requests at acceptable latency. A Cassandra node with a JVM GC pause every 30 seconds is technically "healthy" by binary health check standards but is causing elevated P99 latency for all queries that route through it. Monitor per-node latency percentiles (P95, P99) and error rates, not just binary up/down status.

**Mistake 5: Implementing retry logic without exponential backoff.** Immediate retries on failure amplify load on a struggling service. If a service is at 100% capacity and returns errors, and 100 clients each immediately retry 3 times, the service receives 4× normal load (original + 3 retries per client). This converts a brief overload event into a sustained cascade. Always implement exponential backoff with jitter: first retry after 100ms, then 200ms, 400ms, 800ms, etc. The jitter (randomization within each window) prevents synchronized retry pulses from all clients.

---

## 8. Production Failure Scenarios

### Scenario 1: MongoDB Split-Brain (2011 — Historical)

**Background:** In November 2011, a widely-reported MongoDB production incident resulted in data loss and a split-brain situation affecting the 10gen-hosted MongoDB service. This incident became a seminal case study in distributed systems failure modes.

**What happened:** A MongoDB replica set (3 nodes) experienced a network partition that isolated the primary from the secondary nodes. The secondaries, following election rules, promoted one of themselves to primary. The original primary, however, did not demote itself — it continued accepting writes. For a period of time, both nodes believed they were the primary and accepted client writes.

**Root cause:** MongoDB's version at the time had an election mechanism that did not guarantee the old primary would detect and honor the new primary's election. The epoch-based step-down mechanism that modern consensus algorithms use was not robust enough in that version. The HA controller (driver-side) would reconnect to whichever node identified itself as primary, which could vary by client depending on connection state.

**Data loss:** When the partition healed, MongoDB had to decide which primary's state to keep. Writes accepted by the old primary (now the minority island) that had not been replicated to the new primary's majority island were rolled back — permanently lost. Clients had received success acknowledgments for those writes, but the data was gone.

**Lesson:** Split-brain prevention requires both epoch-based write rejection (followers reject writes from stale leaders) and explicit fencing (the HA controller actively demotes or incapacitates the old primary before the new one accepts writes). Neither alone is sufficient — epoch-based rejection requires the old leader to hear from someone with a higher epoch, which may not happen during a partition.

### Scenario 2: AWS EC2 Network Partition Causes Kafka ISR Collapse

**Symptoms:** At 03:12 UTC, a segment of the AWS EC2 networking fabric in us-east-1 experiences degraded connectivity between two availability zones. Kafka brokers in AZ-A can communicate with each other and with AZ-C, but connectivity between AZ-A and AZ-B is severely degraded (50% packet loss, 500ms latency for packets that do get through).

**Failure cascade:**
- Kafka broker-4 (AZ-B) has replication connections to broker-1 and broker-2 (AZ-A) as leaders for various partitions. These connections begin experiencing timeouts.
- Broker-4's follower fetch requests time out. `replica.lag.time.max.ms` (30 seconds) is exceeded. Broker-4 is removed from ISR for all partitions where it is a follower.
- Producers with `acks=all` and `min.insync.replicas=2` are unaffected initially — broker-1, broker-2, broker-3 (AZ-A and AZ-C) form ISR of 2 for those partitions.
- But broker-5 (AZ-B) is also the partition leader for several partitions. Broker-5's follower connections to AZ-A brokers (which are its followers for those partitions) also time out. Broker-5's partitions see ISR collapse to ISR=[broker-5].
- `min.insync.replicas=2`: producers to broker-5's partitions begin receiving `NOT_ENOUGH_REPLICAS`.
- The KRaft controller (in AZ-A) detects broker-5's ISR collapse and triggers leader elections for broker-5's partitions, moving leadership to AZ-A or AZ-C brokers.
- New partition leaders in AZ-A/AZ-C restore ISR to ≥2, resolving the producer errors.
- Total impact: 4 minutes of `NOT_ENOUGH_REPLICAS` for partitions led by AZ-B brokers.

**Lessons:** Multi-AZ Kafka deployments should use rack-aware replica assignment (`kafka-topics.sh --replica-assignment`) to spread replicas across AZs. ISR collapse is expected during cross-AZ network events — design producers to handle `NOT_ENOUGH_REPLICAS` with retry, not as a fatal error. Monitor `under_replicated_partitions` and set a PagerDuty alert at the first occurrence.

### Scenario 3: Cascading Consumer Failure in a Flink Pipeline

**Symptoms:** A production Flink streaming pipeline processing 50,000 events/second enters a failure loop that progressively degrades throughput over 20 minutes, eventually producing zero output for 45 minutes before manual intervention.

**Failure sequence:**
1. One Flink TaskManager JVM (of 8 total) suffers an OOM exception due to a sudden spike in event size. TaskManager crashes.
2. Flink's JobManager detects the failure and attempts to restart the failed TaskManager and restore state from the last Flink checkpoint (taken every 60 seconds).
3. The checkpoint restoration runs. During checkpoint restoration (takes 45 seconds), the entire Flink job is paused — no events processed.
4. 50,000 events/second × 45 seconds = 2.25 million events pile up in the Kafka topic.
5. Flink restores and resumes. It now has a 2.25M event backlog. To catch up, it processes at full speed, which uses 100% of each TaskManager's memory.
6. Memory pressure causes frequent GC pauses. GC pauses cause TaskManagers to miss heartbeat deadlines. JobManager interprets missed heartbeats as TaskManager failure.
7. JobManager triggers another restart. During the second checkpoint restoration (another 45 seconds), more events pile up.
8. The cycle repeats: restore → process at full speed → GC → restart → restore.
9. Each restart cycle causes the checkpoint position to be further behind, because checkpoints taken during the high-memory GC cycles are incomplete. Flink falls further and further behind.

**Root cause:** The initial failure (OOM from large events) triggered a cascade that the recovery mechanism (full-speed catchup) made worse. The pipeline lacked backpressure: Flink did not slow down Kafka consumption to match a sustainable processing rate.

**Fix:** Enable Flink's backpressure monitoring. Set `taskmanager.memory.process.size` with adequate headroom for event size spikes. Reduce checkpoint interval to 30 seconds to limit backlog accumulation during recovery. Use `kafka.consumer.commit-offsets-on-checkpoints` to ensure Kafka offset commits match Flink checkpoint commits, preventing re-processing stale events.

### Scenario 4: Gray Failure — GC-Pausing Postgres Node

**Symptoms:** A data engineering team's Postgres query service reports P99 query latency of 850ms over a 48-hour period. P50 is normal at 12ms. The average latency is 85ms (slightly elevated). The monitoring dashboard shows the cluster as "healthy" — all three Postgres nodes respond to `/health` checks with 200. Engineers spend two days investigating the query layer (slow queries, missing indexes) before identifying the root cause.

**Root cause:** One of three Postgres nodes (a PgBouncer-pooled connection pool with a round-robin load balancer) has a JVM-hosted sidecar process (a monitoring agent) that consumes memory and triggers OS-level memory pressure. Under memory pressure, the Linux kernel swaps the Postgres buffer pool to disk, causing page faults on every query. The affected node has 400ms average latency; the other two nodes have 8ms average latency.

Because PgBouncer uses round-robin, one in every three connections routes to the degraded node. That one-in-three is the source of the P99 spike: the top 33% of all queries hit the degraded node and are slow. P50 is fine (only 50% of queries are above median, and the degraded node's queries cluster above P66). The overall average is pulled up but the binary health check ("does port 5432 accept connections?") never fires.

**Detection and fix:** The team finally added per-node P99 monitoring. The degraded node showed P99 = 1,200ms while the healthy nodes showed P99 = 20ms. The monitoring agent was stopped, memory pressure resolved, and the node's performance returned to normal. The lesson: gray failures are invisible to binary health checks. Every component needs P95/P99 latency monitoring per instance, not just aggregate.

---

## 9. Performance and Tuning

### Tuning TCP Keepalive for Fast Partition Detection

```bash
# Linux kernel TCP keepalive parameters
# Default: keepalive_time=7200s (2 hours!) — far too slow for distributed systems
sysctl net.ipv4.tcp_keepalive_time    # Time before first keepalive probe
sysctl net.ipv4.tcp_keepalive_intvl   # Interval between probes
sysctl net.ipv4.tcp_keepalive_probes  # Number of probes before declaring dead

# Production recommendation for distributed systems:
echo "net.ipv4.tcp_keepalive_time=60"    >> /etc/sysctl.d/99-tcp.conf
echo "net.ipv4.tcp_keepalive_intvl=10"   >> /etc/sysctl.d/99-tcp.conf
echo "net.ipv4.tcp_keepalive_probes=5"   >> /etc/sysctl.d/99-tcp.conf
sysctl -p /etc/sysctl.d/99-tcp.conf
# With these settings: dead connection detected in 60 + (10×5) = 110 seconds
# Still slow — always use application-level heartbeats too

# Kafka broker: network.idle.timeout.ms (default 600000ms = 10min!)
# Reduce to detect idle connections faster:
network.idle.timeout.ms=60000   # 1 minute
connections.max.idle.ms=60000

# Postgres: tcp_keepalives_idle, tcp_keepalives_interval, tcp_keepalives_count
# Set in postgresql.conf:
tcp_keepalives_idle = 60
tcp_keepalives_interval = 10
tcp_keepalives_count = 5
```

### Kafka Configuration for Partition Resilience

```bash
# Replication timeouts — balance between fast detection and false positives
replica.lag.time.max.ms=30000    # ISR removal after 30s of lag
# Too low (5s): spurious ISR removals under load spikes
# Too high (120s): slow detection of genuinely failed replicas

# Controller election timeouts (KRaft)
controller.quorum.election.timeout.ms=1000
controller.quorum.fetch.timeout.ms=2000
controller.quorum.retry.backoff.ms=20

# Session timeout for brokers to detect dead peers
# (in ZooKeeper-based Kafka)
zookeeper.session.timeout.ms=18000   # Default: allow for GC pauses
zookeeper.connection.timeout.ms=18000

# Producer retry configuration for partial failure handling
retries=2147483647               # Retry indefinitely (with backoff)
retry.backoff.ms=100             # Initial retry backoff
delivery.timeout.ms=120000       # Total time before giving up (2 minutes)
# With these settings: producer retries with backoff for up to 2 minutes
# Combined with enable.idempotence=true: at-least-once → exactly-once
```

### Flink Checkpointing for Cascading Failure Recovery

```yaml
# Flink config: reduce cascade blast radius
execution.checkpointing.interval: 30000    # Checkpoint every 30s (not 60s)
execution.checkpointing.timeout: 60000     # 1 minute checkpoint timeout
execution.checkpointing.max-concurrent-checkpoints: 1
state.backend: rocksdb                     # Incremental checkpoints (faster recovery)
state.backend.incremental: true
# With incremental checkpoints, recovery time = last_checkpoint_size (not total state)
# Reduces the "catch up" backlog after recovery

# Backpressure and rate limiting:
pipeline.max-parallelism: 128
# Set source rate limits in Kafka source:
# KafkaSource.builder().setStartingOffsets(...)
#   .setProperty("fetch.max.bytes", "1048576")  # 1MB max per fetch
#   .setProperty("max.poll.records", "500")      # Limit batch size
# Prevents consuming faster than the pipeline can process
```

---

## 10. Interview Q&A

**Q1: What is split-brain and how do distributed systems prevent it?**

Split-brain is the condition where two nodes in a distributed cluster each believe they are the authoritative leader and independently accept writes, causing the cluster's state to diverge. It arises from the combination of leader elections and the absence of reliable split-brain prevention mechanisms: an old leader is isolated by a network partition, a new leader is elected, the old leader recovers, and both leaders now accept writes for the same keys.

Distributed systems prevent split-brain through three complementary mechanisms. First, epoch-based write rejection: every leadership term is assigned an epoch number. Followers reject writes from any leader whose epoch is lower than the current epoch. When an old leader reconnects and sees rejections with a higher epoch, it knows it has been replaced and steps down. This is how Kafka's partition leadership model and Raft consensus both handle stale leaders.

Second, fencing (STONITH): before a HA controller promotes a new leader, it sends a fencing command to incapacitate the old leader — power-cycling it, removing its network access, or unmounting its storage volume. The old leader is made incapable of accepting writes before the new leader starts. This is the mechanism Patroni uses for Postgres HA and is required whenever epoch-based rejection is not sufficient (e.g., the old leader is partitioned and cannot receive epoch update messages).

Third, lease-based leadership: a leader holds a time-bounded lease from a consensus system (etcd, ZooKeeper). If the leader cannot renew its lease, it voluntarily stops accepting writes — even if it cannot confirm whether a new leader was elected. The lease duration sets the maximum split-brain exposure: at most `lease_duration` seconds of divergent writes before the old leader self-demotes.

**Q2: Describe the difference between a hard partition and a gray failure. Why are gray failures more dangerous?**

A hard partition is a complete or near-complete network failure between two nodes: packets are dropped, TCP connections fail to establish, heartbeats are never received. The failure is detectable: within the heartbeat timeout period (seconds to minutes), the affected system transitions to a known failure state — leader election occurs, the partitioned node is removed from ISR, or the node is flagged as unavailable.

A gray failure is a partial degradation: the network connection exists, but performance is severely degraded (high latency, packet loss, intermittent drops). The node is alive — it responds to TCP connection attempts, passes HTTP health checks, and participates in gossip protocols. But it is severely slow or partially unreliable. From the system's perspective, the node's binary status is "up," but from the application's perspective, every third request to that node takes 10 seconds.

Gray failures are more dangerous for three reasons. First, they are invisible to standard monitoring: binary health checks ("is port open?", "does HTTP 200?") do not detect latency degradation. The degraded node stays in the load balancer rotation and continues receiving traffic. Second, they degrade user experience without triggering alerts: the system isn't down (no alert fires), but P99 latency is elevated and a fraction of requests are very slow. Third, they can trigger partial cascades: in a consensus system, a gray failure can cause election timeouts (the system can't hear responses in time), leading to unnecessary leader elections and write pauses, even though the node is technically alive.

Detecting gray failures requires per-node latency percentile monitoring (P95, P99) compared against cluster baselines, combined with per-node error rate tracking. A node whose P99 is 5× the cluster median is experiencing gray failure and should be removed from rotation — even if its binary health check passes.

**Q3: Your Kafka consumer group is experiencing a cascading rebalance loop. How do you diagnose and stop it?**

A cascading rebalance loop is a cycle where Kafka consumers fail heartbeat checks, triggering a rebalance, which causes processing to stop, which causes backlog accumulation, which causes processing to be overloaded when it resumes, which causes GC pauses, which causes heartbeat failures, which triggers another rebalance.

Diagnosis starts with three metrics: `kafka.consumer:type=consumer-coordinator-metrics,name=rebalance-rate-per-hour` (rebalances per hour — should be near zero in steady state; anything > 1/hour is a problem), consumer group lag via `kafka-consumer-groups.sh --describe` (growing lag indicates falling behind), and `kafka.consumer:type=consumer-fetch-manager-metrics,name=records-lag-max` (max lag per partition).

In the logs, look for `Rebalance in progress, stopping consumption` messages. Check whether the rebalance is triggered by consumer departure (a consumer actually died) or heartbeat timeout (consumer alive but slow). If it's heartbeat timeout, check: JVM GC pause logs (`-Xlog:gc*`), CPU utilization (near 100% = GC or compute bound), and `max.poll.interval.ms` configuration (default 5 minutes; if a single `poll()` batch takes more than 5 minutes to process, Kafka interprets the consumer as dead).

To stop the cascade: first, reduce the processing batch size (`max.poll.records` from 500 to 50) so each poll interval processes less work. Second, increase `session.timeout.ms` and `heartbeat.interval.ms` temporarily to give consumers more time to process before being marked dead (buy time to diagnose). Third, reduce the consumer group's parallelism by temporarily pausing some consumers, reducing the risk that all consumers are simultaneously overloaded. The root fix is to reduce per-record processing time and right-size `max.poll.interval.ms` to match actual processing latency.

---

## 11. Cross-Question Chain

**Interviewer:** A co-worker proposes disabling Patroni's STONITH for your Postgres cluster because "it's caused two false-positive fencing events in the past month." What's your response?

**Candidate:** I understand the frustration — a false-positive STONITH that unnecessarily powers off the primary during a transient network blip is genuinely painful. But disabling STONITH entirely is the wrong fix because it removes the only reliable protection against split-brain. Without fencing, if the primary is partitioned (but not dead) and a standby is promoted, both can accept writes. When the partition heals, there's no safe automatic reconciliation — data loss is guaranteed for one side.

The correct fix for false-positive fencing is to tune the STONITH trigger conditions, not remove them. First: is STONITH firing because the node is truly partitioned, or because the DCS (etcd/ZooKeeper) is briefly unreachable? If Patroni loses DCS connectivity, it demotes the primary as a precaution. If the DCS itself had a transient blip, this produces a false positive. Fix: ensure the DCS is highly available and on a reliable network path. Second: increase Patroni's `ttl` (time-to-live for the leader lease) from the default 30s to 45-60s. This gives the primary more time to recover connectivity before Patroni decides to demote it.

The core principle: false-positive STONITH is painful but recoverable (the primary restarts). Split-brain is catastrophic and may not be recoverable without data loss. Always err on the side of aggressive fencing.

**Interviewer:** If you were designing the STONITH mechanism for a new Postgres HA system, what would you build?

**Candidate:** I'd use a three-layer approach. First, DCS-based leadership lock: the primary holds a time-bounded lock in etcd (TTL = 30 seconds, renewed every 10 seconds). Any primary that cannot renew its lock voluntarily demotes itself. This handles the case where the primary is alive but isolated from other Postgres nodes — it self-demotes rather than requiring an external fencing action.

Second, network-level fencing: on promotion, the HA controller updates an AWS security group rule (or a GCP firewall rule) to deny all inbound connections to the old primary's port 5432. This prevents any clients or standbys from connecting to the old primary even if it hasn't fully self-demoted. The security group change is near-instant and doesn't require SSH access to the old primary.

Third, storage-level fencing as the final backstop: for the highest-integrity use cases, the old primary's EBS volume is remounted as read-only before the new primary starts. This makes it physically impossible for the old primary to accept writes at the storage level, regardless of network state.

The sequence: (1) old primary loses DCS lock → self-demotes, (2) HA controller updates network security group → external fence, (3) new primary starts accepting writes. Storage-level fence is only needed if 1 and 2 both fail, which is extremely rare. This gives defense in depth without needing actual power-cycling (which causes unnecessary recovery time).

**Interviewer:** Three weeks after implementing your HA system, a postmortem shows the system had 8 minutes of unavailability during a failover. The expectation was 30 seconds. What might have caused the delay?

**Candidate:** Eight minutes versus 30 seconds means something stalled in the failover path. I'd investigate the sequence in order.

First: Patroni DCS check. Did Patroni fail to acquire the leader lock on the new primary? If etcd was slow or had a network blip, the leader lock acquisition could have timed out and retried. Check `patronictl -c patroni.yml list` logs from the incident — was there a period where "no leader" state persisted for minutes?

Second: STONITH execution time. How long did the security group update take? AWS API calls can occasionally take 30-60 seconds. If the STONITH API call timed out and Patroni retried, that's several minutes right there. Check CloudTrail for the security group modification time.

Third: standby WAL replay state. If the promoted standby was significantly behind the primary (high replication lag), it needs to replay the remaining WAL before accepting writes. If the standby was minutes behind (due to a long-running query blocking WAL replay on the standby), the new primary couldn't accept writes until the replay completed. Check `pg_stat_replication.replay_lag` in the days before the incident — was lag trending upward?

Fourth: application reconnection. After Patroni updated the HAProxy endpoint, how long did it take for application connection pools to drain old connections to the dead primary and acquire new ones to the new primary? If the application had a long `connect_timeout` on each pool connection attempt to the dead primary, this could add minutes of latency before all connections migrated.

Each of these is a different fix: ensure etcd is highly available, set STONITH API call timeouts aggressively (5-10s max), monitor replication lag and alert before it grows, and tune PgBouncer's `reconnect_timeout` and `server_check_delay`.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is split-brain in a distributed system? | Two or more nodes simultaneously believe they are the authoritative leader and independently accept writes. Causes state divergence. When the partition heals, one side's data must be lost or manually reconciled. |
| 2 | What is STONITH (Shoot The Other Node In The Head)? | A fencing mechanism that physically incapacitates the old leader before promoting a new one: power-cycling the node, removing its network access, or making its storage read-only. Prevents split-brain by guaranteeing at most one active writer. |
| 3 | How does epoch-based write rejection prevent split-brain? | Each leadership term has an epoch number. Followers reject writes from any leader with an epoch lower than the current epoch. An old leader that reconnects after being replaced sees rejections with a higher epoch and steps down. |
| 4 | What is a network partition? | A failure in which some nodes can communicate with each other but not with others, splitting the cluster into isolated islands. All nodes are running; the failure is in the network between them. |
| 5 | What is a gray failure? | A partial degradation where a node is alive and passes binary health checks, but is severely degraded: high latency, elevated error rate, or reachable only by some nodes. Binary monitoring does not detect it; per-node P99 latency monitoring does. |
| 6 | What is partial failure and why is it harder than clean failure? | An operation succeeds on some nodes and fails on others, leaving the system in an inconsistent intermediate state. The caller may receive success, failure, or timeout — and cannot determine what actually happened. Clean failures are immediately detectable; partial failures require explicit idempotency handling. |
| 7 | What is the correct response to a network timeout on a non-idempotent distributed operation? | Query the server for the current state before deciding to retry. A timeout means "I don't know what happened" — not success and not failure. If the operation was applied, retry would duplicate it. If not applied, abandoning would lose it. Check state, then decide. |
| 8 | What is cascading failure? | A failure in one component that redistributes load to remaining components, driving them into failure, propagating further until a large fraction of the system fails. The mechanism: failed node's traffic redistributes to healthy nodes, pushing them to capacity, causing them to fail. |
| 9 | What is a circuit breaker and what failure mode does it address? | A pattern that tracks failure rate to a downstream service and "opens" (rejects all calls fast) when failure rate exceeds a threshold. Addresses cascading failure by preventing retries from amplifying load on a failing service. States: CLOSED (normal), OPEN (reject all), HALF_OPEN (test recovery). |
| 10 | What is a bulkhead pattern? | Isolates resource pools per downstream dependency. Instead of one shared connection pool for all downstream calls, each dependency gets a dedicated pool. A failing dependency exhausts only its own pool; other dependencies' pools remain unaffected. Contains cascading failure. |
| 11 | Why should production clusters run at 60-70% capacity instead of 90%? | To tolerate node failures without cascading. At 90% capacity, failing one node in a 10-node cluster redistributes to 9 nodes at 100% → immediate cascade. At 60% capacity, one failure redistributes to 9 nodes at 67% — well within safe bounds. |
| 12 | What is exponential backoff with jitter and why is the jitter important? | On failure, wait before retrying: 100ms, then 200ms, 400ms, 800ms (exponential). Jitter adds randomization to each interval. Without jitter, all clients retry at exactly the same time after the backoff delay, creating a synchronized pulse of load on the recovering service ("thundering herd"). |
| 13 | How does Patroni prevent split-brain in Postgres HA? | Patroni uses a DCS (etcd/ZooKeeper) leader lease: only the node holding the lock is the primary. If a node loses the lock, it demotes itself. Before promoting a standby, Patroni executes a STONITH script to incapacitate the old primary. Never promotes without successful fencing. |
| 14 | How does Kafka prevent split-brain after a partition leader change? | A new partition leader is elected with a higher leader epoch. If the old leader attempts to produce or fetch, followers and the new leader reject requests with the stale epoch. The old leader sees the rejection, updates its epoch, and becomes a follower. |
| 15 | What Linux command simulates a network partition for testing? | `tc` (traffic control): `tc qdisc add dev eth0 root handle 1: prio` + `tc filter add ... action drop` drops packets to a specific IP. Or `iptables -A INPUT -s <IP> -j DROP` / `iptables -A OUTPUT -d <IP> -j DROP`. Remove with `tc qdisc del` or `iptables -D`. |
| 16 | What is the default Linux TCP keepalive timeout and why is it a problem for distributed systems? | `net.ipv4.tcp_keepalive_time` defaults to 7200 seconds (2 hours). A network partition will not cause a TCP connection to fail for up to 2 hours. Distributed systems need application-level heartbeats (500ms–2s) and tuned TCP keepalive (60s idle time) for fast partition detection. |
| 17 | What causes a Kafka consumer cascade rebalance loop? | Initial consumer failure → load redistributes → remaining consumers overloaded → GC pauses → heartbeat timeouts → Kafka treats consumers as dead → rebalances → processing stops → backlog grows → overloaded when processing resumes → GC pauses → another rebalance. |
| 18 | How do you detect gray failures in a distributed cluster? | Per-node P99 latency monitoring compared against cluster baseline. A node whose P99 is 3-5× the cluster median is a gray failure. Binary health checks (HTTP 200, TCP connect) do not detect gray failures. Also: per-node error rate tracking and canary probing. |
| 19 | What is the "thundering herd" problem in distributed systems recovery? | When a service recovers, all clients that were waiting (or had backed off) simultaneously retry, creating a traffic spike that exceeds the just-recovered service's capacity, potentially causing it to fail again. Prevented by exponential backoff with jitter. |
| 20 | What is the difference between a "clean" (crash) failure and a partial failure? | A clean failure is immediately detectable: the process crashes, the TCP connection resets, or the node stops responding. Monitoring fires. The system reroutes traffic. A partial failure is ambiguous: the operation may or may not have been applied. The caller receives a timeout, not a clear success or failure signal. |

---

## 13. Further Reading

- **"Designing Data-Intensive Applications" by Martin Kleppmann, Chapter 8 (The Trouble with Distributed Systems):** The most thorough practitioner treatment of distributed failure modes. Covers unreliable clocks, network delays, partial failures, and process pauses. Required reading for any senior data engineer.
- **"Gray Failure: The Achilles' Heel of Cloud-Scale Systems" (Huang et al., Microsoft Research, 2017):** The seminal paper on gray failures in cloud systems. Defines gray failure formally and presents evidence from Azure production incidents. Shows that gray failures are responsible for more availability loss than hard failures in practice.
- **Patroni documentation — "Replication modes" and "Fencing":** The Patroni documentation on STONITH/fencing explains why fencing is necessary and describes multiple fencing implementations (watchdog, AWS EC2, GCP GCE, scripts). Read the "Replica Bootstrap" and "Switchover/Failover" sections.
- **Kafka documentation — "Replication" and "Configuration":** The `replica.lag.time.max.ms` and ISR protocol documentation explains exactly how Kafka detects and handles partition failures. The `min.insync.replicas` reference and the behavior of `acks=all` during ISR shrink are essential operational knowledge.
- **"Chaos Engineering" by Rosenthal et al. (O'Reilly):** Netflix's book on deliberately injecting failures to test system resilience. Chapter 4 on cascading failures and Chapter 6 on partial network failures are directly applicable to data engineering infrastructure.
- **Netflix Hystrix documentation (archived):** The original circuit breaker, bulkhead, and thread isolation patterns. Even though Hystrix itself is now in maintenance mode, its documentation is the clearest explanation of these patterns and the failure modes they address.

---

## 14. Lab Exercises

**Exercise 1: Simulate and Observe a Kafka Partition**

Using a 3-broker Kafka cluster (Docker Compose): (1) Identify the partition leader for a test topic. (2) Use `tc` or `iptables` to drop packets between the leader broker and one follower. (3) Watch ISR shrink: `watch -n 2 'kafka-topics.sh --describe --under-replicated-partitions'`. (4) Note how long it takes for the follower to leave ISR (`replica.lag.time.max.ms = 30s`). (5) Heal the partition and observe ISR recovery. (6) Repeat with `acks=all` + `min.insync.replicas=2`: observe that writes continue as long as ISR ≥ 2, and fail when ISR = 1.

**Exercise 2: Observe Gray Failure via Artificial Latency**

Using `tc netem` on one Kafka broker: add 500ms latency (`tc qdisc add dev eth0 root netem delay 500ms`). (1) Monitor broker-level latency via Kafka metrics: `kafka.server:type=BrokerTopicMetrics,name=ProduceTotalTimeMs`. (2) Check whether the broker is removed from ISR (it shouldn't be, since netem introduces latency but not packet loss). (3) Verify that producers sending to this broker's partitions experience high latency while producers to other brokers are normal. (4) Simulate gray failure detection: check whether a binary health check (`nc -z broker 9092`) returns immediately while the actual produce latency is 500ms. This demonstrates why binary checks miss gray failures.

**Exercise 3: Build and Test a Circuit Breaker**

Using `circuit_breaker` from the code toolkit: (1) Configure a circuit breaker with `failure_threshold=0.5`, `window_seconds=10`. (2) Call a `flaky_service()` function that fails the first 8 calls, succeeds from the 9th. (3) Observe the circuit open after sufficient failures, reject subsequent calls immediately (without calling `flaky_service`), enter `HALF_OPEN` after the open duration, and close after successful probes. (4) Measure the latency difference between a call that goes through to `flaky_service` (slow) vs. a call that's rejected by the open circuit (fast). This demonstrates the "fail fast" property of circuit breakers.

**Exercise 4: Cascading Failure via Consumer Overload**

Using `cascade_simulator.py`: (1) Create 5 nodes at 80% capacity. (2) Fail one node. (3) Observe the cascade: load redistribution pushes remaining nodes to 100%, they fail, redistribution continues, total outage. (4) Redo with 5 nodes at 50% capacity. (5) Fail one node. (6) Observe: redistribution to 62.5% — below threshold, no cascade. Document the relationship between initial capacity and cascade resistance.

**Exercise 5: Simulate Patroni Failover and Verify Fencing**

In a Patroni-managed Postgres cluster (Docker Compose): (1) Confirm the initial primary via `patronictl list`. (2) Pause the Patroni process on the primary (`systemctl stop patroni`), simulating leader isolation. (3) Observe Patroni on the standby detect the leader absence and run the STONITH callback. (4) Verify the standby becomes primary via `patronictl list`. (5) Restart the old primary's Patroni process and verify it rejoins as a replica, not a primary. (6) Write a test row before the failover, confirm it is present on the new primary.

---

## 15. Key Takeaways

Distributed systems fail in four qualitatively different ways: split-brain (divergent leaders), network partition (isolated islands), partial failure (uncertain operation completion), and cascading failure (amplifying propagation). Understanding these failure modes at depth — not just their names but their mechanisms, their detection signals, and their mitigations — is the foundation of reliable production data infrastructure.

Split-brain is prevented by epoch-based write rejection (followers refuse stale leaders) and fencing (HA controllers incapacitate old leaders before promoting new ones). Never operate a consensus-based system without fencing. The pain of false-positive STONITH events (unnecessary node restart) is far less than the pain of split-brain (data loss requiring manual reconciliation).

Network partitions are the normal operating mode for distributed systems at scale — not exceptional events. CP systems (Raft, Zookeeper, Kafka with `acks=all`) sacrifice availability in the minority island to prevent split-brain. AP systems (Cassandra with ONE, DynamoDB eventual) continue serving from partitioned nodes at the cost of potentially stale reads. Design your system knowing which partitions will occur and deciding in advance which CP vs. AP trade-offs are acceptable.

Partial failure is the hardest failure mode to handle correctly. The universal mitigation is idempotency: make every operation safe to retry. For Kafka producers: `enable.idempotence=true`. For database writes: use upsert with a stable business key. For multi-step pipelines: checkpoint with exactly-once semantics and replay from the last checkpoint.

Cascading failure is prevented by capacity headroom (never exceed 60-70% capacity), circuit breakers (fail fast to avoid amplifying load), bulkheads (isolate resource pools per dependency), and backpressure (slow down producers when consumers are falling behind). The diagnostic signal for an impending cascade is consumer lag growth — the leading indicator that the system is falling behind and the cascade mechanism is beginning.

---

## 16. Connections to Other Modules

- **M37 — CAP Theorem:** Network partitions are the P in CAP. The CAP choice (CP vs AP) determines how a system behaves when a partition splits it into islands. M37 explains the theory; this module shows what partition behavior looks like in practice.
- **M38 — Consensus Algorithms:** Raft's epoch (term) system is the mechanism behind split-brain prevention. The step-down rule ("if you see a higher term, become a follower") is exactly the epoch-based write rejection described in this module.
- **M39 — Replication:** Replication lag is a form of partial failure — the write was committed on the leader but not yet visible on the follower. ISR management in Kafka is a response to the partial failure of followers falling behind.
- **M41 — Distributed Transactions:** 2PC is vulnerable to partial failure (the coordinator can crash between Phase 1 and Phase 2, leaving participants in a blocked state). Saga compensating transactions are designed specifically to handle partial failure in distributed transaction workflows.
- **DCS-SPK-101 — Spark Fundamentals:** Spark task failures and speculative execution are applications of partial failure handling — when a task is suspected to be a gray failure (running but very slow), Spark launches a speculative copy.
- **DE-ORC-101 — Airflow:** Airflow task retry logic, SLAs, and dependency management are all partial failure handling mechanisms for workflow orchestration.

---

## 17. Anti-Patterns

**Anti-pattern: Catching and silently swallowing all exceptions in a distributed client.** A producer or consumer that catches all exceptions and logs them without raising them makes partial failures invisible. The caller thinks everything is fine; data is silently lost. Always propagate exceptions from distributed clients to the application layer and let the application decide how to handle them (retry, dead-letter, alert).

**Anti-pattern: Using a single large thread pool for all downstream calls.** If all downstream calls (service A, B, C) share a single thread pool and service A becomes slow, its calls consume all threads. Service B and C calls queue behind them, even if B and C are healthy. Implement bulkheads: separate thread pools (or reactive pipelines with separate queues) per downstream dependency.

**Anti-pattern: Setting `request.timeout.ms` too long in Kafka producers.** A long timeout (default `request.timeout.ms=30000`) means a producer will wait 30 seconds before giving up on a partition that's leader-less. During that 30 seconds, the producer's send buffer fills, other partitions stall, and end-to-end latency spikes. For latency-sensitive producers, set `request.timeout.ms=5000` or lower, and handle `TimeoutException` with retry logic at the application level.

**Anti-pattern: Manual, ad-hoc fencing procedures.** "When the primary fails, SSH to it and stop Postgres before promoting the standby" is not a reliable fencing procedure. SSH access may not be available during a failure (the same network issue that caused the failover may prevent SSH). Fencing must be automated, reliable, and testable. Use hardware fencing (IPMI/iDRAC) or cloud API fencing (security group modification, instance stop via AWS API) with the HA controller executing it automatically.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `tc` (traffic control) | Simulate network partitions and gray failures | `tc qdisc add dev eth0 root netem delay 500ms` for gray failure; `action drop` for partition |
| `iptables` | Firewall-based partition simulation | `iptables -A INPUT -s <IP> -j DROP` to isolate a node |
| `patronictl list` | Postgres HA cluster state | Shows leader, epoch, replication lag, member health |
| `patronictl failover` | Trigger manual failover | Tests STONITH and promotion path |
| `kafka-topics.sh --under-replicated-partitions` | Kafka ISR health | Lists partitions where ISR < RF — key cascade indicator |
| `kafka-consumer-groups.sh --describe` | Consumer lag | Shows per-partition lag — leading indicator of cascade |
| `etcdctl endpoint status` | Raft leader and term | Monitor for leader changes (election events) |
| `nodetool status` | Cassandra cluster health | Node state (UN = Up/Normal), ring token distribution |
| `failure_mode_simulator.py` | Simulate all four failure modes | Split-brain, partition, cascade, gray failure, circuit breaker |
| `vmstat` / `iostat` | Node resource utilization | High iowait = potential gray failure (disk-bound) |
| `ss -s` / `netstat` | TCP connection state | Detect CLOSE_WAIT accumulation (connection pool exhaustion) |
| `jstack` / `jmap` | JVM thread dump / heap analysis | Diagnose GC-induced heartbeat failures |

---

## 19. Glossary

**Backpressure:** A mechanism where a consumer signals to a producer to slow down when it is falling behind. Prevents queue overflow and cascading failure from load accumulation.

**Bulkhead:** A resource isolation pattern that assigns dedicated resource pools (thread pools, connection pools) per downstream dependency, preventing one failing dependency from exhausting all resources.

**Cascading failure:** A failure in one component that redistributes load to neighboring components, driving them into failure, propagating until a large fraction of the system fails.

**Circuit breaker:** A pattern that tracks failure rate to a downstream service and transitions to OPEN state (rejecting all calls fast) when failure rate exceeds a threshold. Prevents retry storms from amplifying load on a failing service.

**Clean failure:** A failure that is immediately and unambiguously detectable: the process crashes, the TCP connection resets, the node stops responding to all probes.

**Epoch:** A monotonically increasing number identifying a leadership term in a consensus system. Writes from leaders with stale epochs are rejected by followers. Equivalent to Raft's "term" and ZAB's "epoch."

**Fencing:** Any mechanism that incapacitates an old leader before a new leader is promoted, preventing split-brain. Examples: STONITH (power cycling), network fencing (firewall rules), storage fencing (read-only mount).

**Gray failure:** A partial degradation where a node is alive and passes binary health checks, but is severely degraded (high latency, elevated error rate, or asymmetric connectivity). Invisible to standard monitoring; detectable only via per-node latency percentile monitoring.

**Idempotency:** A property of an operation where executing it multiple times has the same effect as executing it once. The universal mitigation for partial failure: if every operation is idempotent, retries are safe.

**Network partition:** A failure in which the network is split such that some nodes can communicate with each other but not with others. All nodes are running; the failure is in the network paths between them.

**Partial failure:** A condition where a distributed operation succeeds on some components and fails on others, leaving the system in an inconsistent intermediate state. The caller receives a timeout — not a clear success or failure.

**STONITH (Shoot The Other Node In The Head):** A type of fencing that physically incapacitates the old leader (power off, remove network access) before promoting a new leader. Prevents split-brain even if epoch-based rejection fails.

**Split-brain:** The condition where two or more nodes simultaneously believe they are the authoritative leader and independently accept writes, causing state divergence.

**Thundering herd:** A synchronized pulse of retry traffic from multiple clients after a service recovers, potentially overwhelming the just-recovered service. Prevented by exponential backoff with jitter.

**TCP keepalive:** A TCP mechanism that sends periodic probes to verify that a connection is still alive. Default interval is 2 hours — far too slow for distributed systems. Must be tuned or supplemented with application-level heartbeats.

---

## 20. Self-Assessment

1. What is split-brain? Give one mechanism that prevents it and explain why epoch-based rejection alone is not always sufficient.
2. Describe the difference between a hard partition and a gray failure. What monitoring approach detects each?
3. A network partition splits your 3-node Raft cluster into a 1-node island and a 2-node island. What happens in each island? What happens when the partition heals?
4. You receive a `TimeoutException` from a Kafka produce call. The timeout was 30 seconds. What do you know about the state of the message in Kafka? What do you do next?
5. Describe a cascading failure scenario in a microservices pipeline. What is the triggering event and what is the amplification mechanism?
6. What is exponential backoff with jitter? Why is the jitter component necessary?
7. Your Cassandra cluster has one node with P99 latency of 1,200ms while the other two nodes have P99 of 15ms. `nodetool status` shows all nodes as `UN` (Up/Normal). What is happening and how do you fix it?
8. What is the circuit breaker pattern? Draw the three states and describe the transitions between them.
9. A Flink streaming job keeps restarting in a loop during recovery after a TaskManager failure. What is likely happening and what configuration changes would you make?
10. Your ops team wants to disable Patroni's STONITH because it caused two false-positive fencing events. What alternative do you propose that reduces false positives without removing split-brain protection?

---

## 21. Module Summary

Distributed systems fail in ways that simple monitoring misses. The four failure modes covered in this module — split-brain, network partition, partial failure, and cascading failure — represent the failure patterns that cause the most severe production incidents in data engineering infrastructure.

Split-brain is prevented by two complementary mechanisms: epoch-based write rejection (stale leaders are silenced by followers refusing their writes) and fencing (the HA controller physically incapacitates the old leader before the new one starts). Epoch rejection requires the old leader to hear from someone with a higher epoch — which may not happen during a partition. Fencing provides the backstop. Operating a consensus cluster without fencing is accepting split-brain risk.

Network partitions are not exceptional events — they are normal operational occurrences in multi-node systems. Every data engineering system must have a documented decision for each partition scenario: which nodes maintain quorum, which become unavailable, and how clients handle the transition. The default Linux TCP timeout (2 hours) is wildly inadequate for this; always use application-level heartbeats and tuned keepalives.

Partial failure — where an operation may or may not have been applied — is the hardest distributed systems problem to handle correctly. The correct response to a timeout on a non-idempotent call is to query the server's state before retrying. The systematic mitigation is designing all operations as idempotent from the start, allowing safe retry at every layer.

Cascading failure is the mechanism behind most large-scale outages. It requires three conditions: the system operates near capacity, load redistribution is automatic and unbounded, and there is no circuit breaker or backpressure to interrupt the cascade. Preventing cascades requires operating at 60-70% capacity, implementing circuit breakers and bulkheads, and designing backpressure from producers to consumers.

Gray failure — the silent killer — is invisible to binary monitoring and responsible for the most persistent, hardest-to-diagnose degradations. Every production cluster requires per-node P99 latency monitoring compared against the cluster baseline. A node that is technically "up" but 5× slower than its peers is failing; it needs to be removed from rotation before it becomes the source of a cascade.

The next module — M41: Distributed Transactions — covers the hardest class of distributed systems problems: coordinating atomic operations across multiple nodes, databases, or services. The failure modes from this module (especially partial failure) are the core challenge that distributed transactions must overcome.
