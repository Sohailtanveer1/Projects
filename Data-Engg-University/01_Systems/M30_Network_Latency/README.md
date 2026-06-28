# M30: Network Latency

**Course:** SYS-NET-101 — Networking for Data Engineers  
**Module:** 04 of 05  
**Global Module ID:** M30  
**Semester:** 1  
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

Latency is not a property of a single machine — it is a property of the path between two machines. Every time data moves from one place to another in a distributed system, latency is the tax that physics and protocol design impose on that movement. Understanding this tax is what separates data engineers who write distributed systems from data engineers who merely configure them.

Three concrete examples of where latency understanding directly changes engineering decisions:

**Example 1 — Raft consensus and cross-region Kafka.** KRaft (Kafka's replacement for ZooKeeper) uses the Raft consensus algorithm. Raft requires a quorum of nodes to acknowledge a log entry before it is committed. The minimum commit latency equals the RTT between the leader and the slowest quorum member. With three nodes in the same AZ (RTT ~0.5 ms), Raft commits in under 1 ms. With nodes split across three regions (RTTs of 20–80 ms), every commit costs at least one cross-region RTT — 20–80 ms per write. This is not a configuration problem. It is a physics problem: the speed of light between California and Frankfurt is approximately 40 ms, and no amount of tuning can reduce a commit that requires a Frankfurt acknowledgment to below 40 ms. A data engineer who doesn't understand this will propose multi-region Raft deployments with unrealistic latency expectations.

**Example 2 — Spark shuffle as a latency amplifier.** A Spark shuffle does not just move data — it establishes TCP connections between every pair of executors in the cluster, transfers data blocks, and waits for all blocks to arrive before the next stage can begin. If there are 200 executors and the shuffle requires 200 × 200 = 40,000 data transfers, and each transfer involves at least one RTT for connection establishment, a 1 ms inter-AZ RTT multiplies to 40,000 ms = 40 seconds of pure handshake overhead. With TLS, each handshake is 2 RTTs. The design implication: Spark executors should be co-located in the same AZ for latency-sensitive shuffle-heavy jobs. The performance problem is not code — it is topology.

**Example 3 — Kafka `acks=all` as a latency floor.** A Kafka producer with `acks=all` and three ISR replicas in three different AZs (RTT 2 ms between any two) will have a minimum produce latency of at least 2 ms — one RTT from the leader to a quorum follower. This is the floor set by physics and protocol. No JVM tuning, no kernel tuning, no Kafka configuration changes this. A P99 produce latency of 15 ms might be hiding a P50 of 2.2 ms — the P50 is the RTT floor, and the P99 is the RTT floor plus protocol overhead plus occasional GC. Understanding latency floors prevents engineers from chasing phantom optimization opportunities.

---

## 2. Mental Model

### Latency as a Physical Constraint

The speed of light in fiber is approximately 200,000 km/s (two-thirds of the speed of light in vacuum, due to the refractive index of glass). This sets a hard physical lower bound on latency:

```
Minimum one-way latency ≥ distance_km / 200,000 km/s
```

Practical RTT examples (one-way × 2, plus switching/routing overhead):
- Same rack (< 10 m): ~0.1 ms
- Same data center (< 1 km): ~0.2–0.5 ms
- Same city (~100 km): ~1–2 ms
- Same region, cross-AZ (~30 km): ~0.5–2 ms
- US East ↔ US West (~4,000 km): ~60–70 ms
- US East ↔ Europe (~7,000 km): ~80–100 ms
- US West ↔ Asia (~10,000 km): ~120–150 ms

These are hard lower bounds. Actual RTTs are always higher due to routing overhead, switching delays, queuing, and the fact that fiber does not follow straight lines between cities.

### Latency vs Throughput: Two Different Things

A common confusion is treating latency and throughput as interchangeable. They are not:

**Latency:** Time to complete one unit of work. How long does it take for one message to travel from producer to consumer? Measured in milliseconds. Affected by: RTT, processing time, queuing delay, serialization overhead.

**Throughput:** Units of work per unit of time. How many messages can travel from producer to consumer per second? Measured in messages/second or bytes/second. Affected by: bandwidth, parallelism, batch size, serialization overhead.

A slow highway (high latency) can still have high throughput if it is wide (many lanes = parallelism). A fast single-lane road (low latency) can have low throughput if it is narrow. In distributed systems: a single-threaded system has low latency for each request but low throughput; a highly pipelined system has high throughput but potentially higher per-item latency.

**Little's Law:** The fundamental relationship between latency, throughput, and concurrency:
```
L = λ × W
```
Where:
- L = number of items in the system (queue length + being processed)
- λ = throughput (items per second)
- W = average latency (seconds)

Rearranged: `W = L / λ`. If you want to achieve λ = 10,000 messages/second with 100 concurrent requests in flight, average latency = 100 / 10,000 = 10 ms. This relationship is useful for capacity planning: given a desired throughput and an observed latency, you can compute the required concurrency (number of parallel connections, threads, partitions).

### Percentile Thinking: Don't Optimize the Average

Latency distributions in production systems are almost never Gaussian (bell curve). They are typically right-skewed with a long tail: a large fraction of requests complete quickly, and a small fraction take much longer. The average hides this tail completely.

```
P50 (median): the latency that 50% of requests are under
P95:          the latency that 95% of requests are under
P99:          the latency that 99% of requests are under
P999:         the latency that 99.9% of requests are under (1 in 1000)
```

For a Kafka cluster with P50 = 2 ms and P99 = 150 ms, the average might be 5 ms — but 1% of all produce requests take 150 ms or more. At 1 million messages/second, that's 10,000 requests per second experiencing 150 ms latency. P99 is the production-relevant number, not the average.

---

## 3. Core Concepts

### 3.1 Latency Components

Every network latency measurement decomposes into four components:

**Propagation delay:** The time for a signal to travel from sender to receiver at the speed of light. This is the physics floor — it cannot be reduced without moving servers physically closer together.
```
propagation_delay = distance / speed_of_light_in_medium
                  = distance_km / 200,000 km/s
```

**Transmission delay:** The time to push all bits of a packet onto the wire. For a 1500-byte packet on a 1 Gbps link:
```
transmission_delay = packet_size_bits / bandwidth_bps
                   = (1500 × 8) / 1,000,000,000
                   = 0.012 ms
```
Transmission delay is negligible at high bandwidths for typical packet sizes.

**Processing delay:** Time spent in network devices (switches, routers, NICs) processing the packet — header parsing, routing table lookup, CRC check. Typically < 0.01 ms per hop in modern data center switches.

**Queuing delay:** Time spent waiting in a queue — either at the NIC transmit queue, in switch buffers, or in the OS network stack. This is the most variable component. When a link is congested, queuing delay can dominate and add milliseconds of jitter. This is the component that creates the P99/P50 gap.

**Total latency = propagation + transmission + processing + queuing**

For same-datacenter traffic, propagation is dominant. For cross-region traffic, propagation is overwhelmingly dominant. Queuing is the source of latency variance (jitter).

### 3.2 RTT Measurement and Interpretation

Round-trip time is the total time for a probe to travel to a destination and back. It is double the one-way latency (assuming symmetric routing) plus the processing time at the destination.

**`ping` as an RTT tool:**
```bash
ping -c 20 kafka-broker-1.prod.internal
# PING kafka-broker-1.prod.internal (10.0.1.47): 56 data bytes
# 64 bytes from 10.0.1.47: icmp_seq=0 ttl=64 time=0.428 ms
# 64 bytes from 10.0.1.47: icmp_seq=1 ttl=64 time=0.391 ms
# ...
# round-trip min/avg/max/stddev = 0.372/0.431/0.612/0.048 ms
```

The `time=` is the RTT per packet. The `stddev` tells you jitter — higher stddev means less predictable latency (queuing effects or routing inconsistency).

**`ping` limitations:**
- Uses ICMP, which may be rate-limited or deprioritized by routers (making measured RTT higher than TCP RTT)
- Does not measure TCP connection setup latency (no TLS, no kernel socket overhead)
- Does not measure application-level latency (no serialization, no Kafka protocol overhead)

**TCP RTT via `ss`:** `ss -tipm dst :9092` shows the smoothed RTT of active TCP connections — this is the RTT the TCP stack actually experiences for Kafka traffic, including kernel processing. It is more representative than ICMP ping.

### 3.3 Bandwidth vs Latency: Independent Dimensions

Bandwidth and latency are orthogonal properties of a network path. A link can have:
- High bandwidth AND high latency (satellite: 600 ms RTT, 600 Mbps bandwidth)
- Low bandwidth AND low latency (serial line: < 1 ms RTT, 115 Kbps bandwidth)
- High bandwidth AND low latency (local 100 Gbps NIC: 0.1 ms RTT, 100 Gbps bandwidth)

The confusion arises because in practice, high-capacity long-distance links (e.g., undersea cables) tend to have both high bandwidth AND high latency. But these are separate physical properties.

**When bandwidth matters:** Large data transfers — Kafka topic replication, Spark shuffle, S3 data uploads. Throughput-limited applications.

**When latency matters:** Request-response workloads — Kafka produce/consume acknowledgment, Raft consensus, JDBC queries. Any system where you are waiting for a response before proceeding.

**When both matter:** Streaming pipelines where both freshness (latency) and throughput matter simultaneously. A Kafka consumer with a freshness SLO of 1 second AND a throughput requirement of 1 GB/s must optimize both independently.

### 3.4 Latency's Effect on Raft Consensus

Raft is the consensus algorithm used by KRaft (Kafka's controller layer) and many other distributed systems (etcd, CockroachDB, TiKV). Understanding how RTT affects Raft is essential for anyone designing multi-region Kafka deployments.

**Raft commit latency:**
In Raft, a write is committed when a majority (quorum) of nodes have appended it to their log. The leader sends the log entry to all followers in parallel; it waits for a quorum to acknowledge before committing. The commit latency is therefore:

```
commit_latency ≈ RTT(leader → slowest_quorum_member) + follower_disk_write_time
```

For a 3-node cluster where a quorum is 2 nodes (leader + 1 follower):
```
commit_latency ≈ RTT(leader → faster_follower) + disk_write_time
```

**Single-region (same AZ, all nodes):**
- RTT: 0.5 ms
- Disk write (NVMe): 0.1 ms
- Commit latency: ~0.6 ms

**Multi-AZ (three AZs, one node per AZ):**
- Intra-AZ RTT: 0.5 ms (local follower)
- Cross-AZ RTT: 2 ms (distant follower)
- With 3 nodes, quorum is 2 → leader waits for the FASTER follower (local AZ)
- Commit latency: ~0.6 ms (same-AZ quorum can be reached)
- Note: the third node in the farthest AZ does NOT delay commits — the quorum is satisfied by the first 2

**Multi-region (three regions):**
- Leader in us-east-1, followers in us-west-2 (65 ms RTT) and eu-west-1 (80 ms RTT)
- With 3 nodes, quorum is 2 → leader must wait for the FASTER follower (us-west-2)
- Commit latency: ~65 ms per write
- Every KRaft metadata write costs 65 ms minimum

**The asymmetry of quorum:** In a 3-node cluster, writes commit after 2 acknowledgments (the leader plus 1 follower). The leader does not need to wait for the slowest member. This means that in a 3-node multi-AZ setup where 2 nodes are in the same AZ, commits are fast (same-AZ RTT) because the quorum is satisfied locally. In a strict 1-node-per-region setup, every commit must cross the fastest inter-region link.

**5-node cluster for multi-region Raft:**
With 5 nodes and quorum = 3: you can place 3 nodes in one region and 1 each in two other regions. The quorum can be satisfied within the primary region (3 nodes), making commit latency local even with cross-region replicas for disaster recovery. This is the standard design for low-latency multi-region Raft deployments.

### 3.5 Latency Amplification in Distributed Systems

A single network call with 5 ms latency is manageable. But distributed systems chain network calls together, and each chain link adds latency. Latency amplification occurs when:

**Sequential calls (additive latency):**
```
Total = L1 + L2 + L3 + ... + Ln
```
A pipeline with 10 sequential network calls at 5 ms each takes 50 ms minimum, even if each call is fast.

**Fan-out (latency determined by the slowest):**
```
Total = max(L1, L2, L3, ..., Ln)
```
A Spark stage that reads from 200 partitions simultaneously takes as long as the SLOWEST partition read. One slow node (due to GC, disk throttling, or network congestion) determines the entire stage's duration. This is the "straggler" problem.

**Cascading fan-out:**
```
Stage 1: 200 parallel reads → max(L1...L200)
Stage 2: depends on Stage 1 → 200 parallel reads again
...
```
In a 10-stage Spark job, each stage's straggler latency multiplies the overall job duration.

**The 99th percentile problem:** If each of 100 parallel calls has a 1% chance of taking 100 ms (P99 = 100 ms), the probability that AT LEAST ONE call takes 100 ms is `1 - (0.99)^100 = 63.4%`. In a system with 100 parallel calls, you will observe a > P99 response time more than 63% of the time. This is why tail latency optimization is critical for large-scale distributed systems: P999 at the individual server level becomes P50 at the application level with enough parallelism.

### 3.6 Queuing Theory and Latency Under Load

Queuing delay — the time a request spends waiting before being processed — is the dominant source of latency variance in production systems.

**M/M/1 queue model:** The simplest queuing model assumes Poisson arrivals and exponential service times. The average waiting time in queue is:

```
W_queue = (ρ / μ) × (1 / (1 - ρ))
```

Where:
- ρ = utilization = arrival_rate / service_rate (must be < 1 for stable queue)
- μ = service rate (requests per second)
- 1/μ = average service time

The key insight is the **non-linear relationship between utilization and latency:**

| Utilization (ρ) | W_queue (× service_time) | P99 effect |
|----------------|--------------------------|------------|
| 50% | 1× | Minimal |
| 70% | 2.3× | Noticeable |
| 80% | 4× | Significant |
| 90% | 9× | High |
| 95% | 19× | Very high |
| 99% | 99× | Unacceptable |

A Kafka broker handling 90% of its capacity will have average queuing delay 9× its processing time. A broker at 50% capacity has 1× queuing delay. This is why headroom matters: running at 90% utilization is not "10% from capacity" — it is "90% more latency than running at 50%."

### 3.7 Cross-Region Latency Reference Table

These are approximate RTTs between major cloud regions (AWS, as of publication; verify current values with `ping` or cloud latency maps):

| Route | Approx RTT | Speed-of-Light Floor |
|-------|-----------|---------------------|
| us-east-1 ↔ us-east-2 (Ohio) | 10–15 ms | ~5 ms |
| us-east-1 ↔ us-west-2 (Oregon) | 60–75 ms | ~30 ms |
| us-east-1 ↔ eu-west-1 (Ireland) | 75–95 ms | ~40 ms |
| us-east-1 ↔ eu-central-1 (Frankfurt) | 85–110 ms | ~50 ms |
| us-east-1 ↔ ap-southeast-1 (Singapore) | 170–200 ms | ~100 ms |
| us-east-1 ↔ ap-northeast-1 (Tokyo) | 150–175 ms | ~85 ms |
| eu-west-1 ↔ ap-southeast-1 | 160–185 ms | ~90 ms |
| Same-AZ (within one AZ) | 0.3–1 ms | ~0.1 ms |
| Cross-AZ (same region) | 1–3 ms | ~0.5 ms |

The "speed-of-light floor" is the theoretical minimum RTT for that distance (distance / 200,000 km/s × 2). Actual RTTs are 1.5–2× the floor due to routing overhead.

### 3.8 Latency in Kafka: End-to-End Decomposition

The end-to-end latency from producer `send()` to consumer `poll()` returning the message decomposes as:

```
E2E latency = producer_batching_delay
            + producer_network_send (1 RTT to broker leader)
            + broker_leader_write_to_page_cache
            + replication_to_ISR (1 RTT leader→follower, if acks=all)
            + consumer_fetch_interval (up to fetch.max.wait.ms)
            + consumer_network_receive (1 RTT broker→consumer)
            + consumer_processing
```

**Typical values (same AZ, acks=all, default config):**
- `linger.ms=5` batching delay: 5 ms
- Producer→broker network: 0.5 ms (1 RTT same AZ)
- Broker page cache write: 0.05 ms
- Replication (1 ISR follower, same AZ): 0.5 ms
- Consumer `fetch.max.wait.ms=500`: 0–500 ms (depends on traffic)
- Broker→consumer network: 0.5 ms
- **Total: 6.5–506.5 ms** (dominated by `fetch.max.wait.ms`)

**For low-latency streaming (reduce `fetch.max.wait.ms`):**
- `fetch.max.wait.ms=10`, `linger.ms=0`:
- Producer→broker: 0.5 ms + broker write: 0.05 ms + replication: 0.5 ms + consumer fetch: 10 ms + consumer→broker: 0.5 ms
- **Total: ~11 ms** — dominated by `fetch.max.wait.ms`

The practical conclusion: for low-latency Kafka consumption, `fetch.max.wait.ms` is the dominant tuning lever on the consumer side, not network speed. The network itself contributes ~1–2 ms per hop in the same AZ.

---

## 4. Hands-On Walkthrough

### 4.1 Measuring RTT with ping and hping3

```bash
# Basic RTT measurement (20 samples for statistical stability)
ping -c 20 kafka-broker-1.prod.internal

# High-frequency pings for jitter analysis
ping -c 100 -i 0.1 kafka-broker-1.prod.internal
# Low stddev = consistent latency; high stddev = queuing/routing jitter

# TCP RTT measurement (port 9092 — simulates Kafka traffic better than ICMP)
hping3 -S -p 9092 -c 20 kafka-broker-1.prod.internal
# -S = SYN packet; measures time for SYN-ACK response
# Output: rtt=0.5ms mdev=0.03ms

# If hping3 not available, use nc with timing:
time nc -z -w 1 kafka-broker-1.prod.internal 9092
```

### 4.2 Measuring TCP RTT on Active Connections

```bash
# RTT for active Kafka producer connections
ss -tipm 'dst :9092' | grep -E 'rtt|ESTAB'
# Output: rtt:0.428/0.048  ← smoothed RTT / variance in ms

# Continuous monitoring of RTT on a specific connection
watch -n1 "ss -tipm 'dst 10.0.1.47:9092' | grep rtt"

# All TCP connections to Kafka with RTT
ss -tipm 'dst :9092' | awk '/ESTAB/{estab=$0} /rtt/{print estab; print "  " $0}'
```

### 4.3 Measuring Latency with traceroute

```bash
# Trace the network path and per-hop latency
traceroute -n kafka-broker-1.prod.internal

# TCP traceroute (more accurate; avoids ICMP firewalling)
traceroute -T -p 9092 kafka-broker-1.prod.internal

# MTR: continuous traceroute with running statistics
mtr --tcp --port 9092 --report --report-cycles 50 kafka-broker-1.prod.internal
# Shows per-hop: packet loss %, avg/worst RTT — identifies which hop adds latency

# Paris traceroute: uses flow-consistent probing (better for ECMP paths)
paris-traceroute kafka-broker-1.prod.internal
```

### 4.4 Measuring Kafka End-to-End Latency

```bash
# Kafka's built-in producer performance test with latency stats
kafka-producer-perf-test.sh \
  --topic latency-test \
  --num-records 10000 \
  --record-size 1024 \
  --throughput 1000 \
  --producer-props \
    bootstrap.servers=kafka-broker-1:9092 \
    acks=all \
    linger.ms=0

# Output:
# 10000 records sent, 999.8 records/sec (0.98 MB/sec),
# 1.3 ms avg latency, 15.0 ms max latency,
# 1 ms 50th, 2 ms 95th, 8 ms 99th, 15 ms 99.9th.

# End-to-end latency (producer → consumer):
# Use Kafka consumer with --from-beginning and compare timestamps:
kafka-consumer-perf-test.sh \
  --topic latency-test \
  --messages 10000 \
  --bootstrap-server kafka-broker-1:9092
```

### 4.5 Simulating Latency with tc (Traffic Control)

```bash
# Add 10ms latency to all outgoing traffic on eth0 (useful for testing)
sudo tc qdisc add dev eth0 root netem delay 10ms

# Add latency with jitter (5ms ± 2ms jitter, 25% correlation)
sudo tc qdisc add dev eth0 root netem delay 5ms 2ms 25%

# Add packet loss (1%)
sudo tc qdisc add dev eth0 root netem loss 1%

# Apply to specific destination only (using tc filter)
sudo tc qdisc add dev eth0 root handle 1: prio
sudo tc filter add dev eth0 parent 1: protocol ip u32 \
  match ip dst 10.0.1.47/32 flowid 1:3
sudo tc qdisc add dev eth0 parent 1:3 netem delay 50ms

# Remove all tc rules
sudo tc qdisc del dev eth0 root
```

### 4.6 Measuring Latency Distribution with wrk2 / perf

```bash
# latency distribution for HTTP services (Kafka REST proxy, Schema Registry)
wrk2 -t4 -c100 -d30s -R1000 --latency \
  http://schema-registry:8081/subjects

# Output includes HDR histogram:
# 50.000%    1.23ms
# 75.000%    1.89ms
# 90.000%    3.45ms
# 99.000%   15.23ms
# 99.900%   45.67ms
# 99.990%  123.00ms

# For Kafka specifically, use producer-perf-test with --print-metrics
kafka-producer-perf-test.sh ... --print-metrics 2>&1 | \
  grep -E 'latency|percentile'
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
latency_diagnostics.py

Network latency measurement and analysis toolkit for data engineers.
Measures RTT, models latency under load, and calculates distributed
system latency bounds.

Dependencies: stdlib only (socket, subprocess, time, statistics)
"""

import os
import re
import time
import socket
import struct
import subprocess
import statistics
import math
from dataclasses import dataclass, field
from typing import Optional


# ─── Data Structures ─────────────────────────────────────────────────────────────

@dataclass
class LatencySample:
    """A single latency measurement."""
    timestamp: float
    latency_ms: float
    success: bool = True
    error: Optional[str] = None


@dataclass
class LatencyStats:
    """Statistical summary of latency measurements."""
    count: int = 0
    min_ms: float = 0.0
    max_ms: float = 0.0
    mean_ms: float = 0.0
    median_ms: float = 0.0
    stdev_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    p999_ms: float = 0.0
    loss_pct: float = 0.0


@dataclass
class RaftLatencyModel:
    """
    Models Raft consensus commit latency for a given node topology.
    All times in milliseconds.
    """
    node_rtts_ms: list[float]       # RTT from leader to each follower
    disk_write_ms: float = 0.1      # follower disk write latency (NVMe default)

    @property
    def quorum_size(self) -> int:
        """Quorum = majority = (n+1) / 2"""
        n = len(self.node_rtts_ms) + 1  # +1 for leader
        return n // 2 + 1

    @property
    def followers_needed(self) -> int:
        """Number of follower acks needed for quorum."""
        return self.quorum_size - 1  # leader already counted

    @property
    def commit_latency_ms(self) -> float:
        """
        Minimum commit latency = RTT to the slowest follower needed for quorum.
        The leader sorts follower RTTs and waits for the fastest
        (followers_needed) to respond.
        """
        sorted_rtts = sorted(self.node_rtts_ms)
        if self.followers_needed > len(sorted_rtts):
            return float('inf')  # not enough nodes
        # Wait for the Nth fastest follower
        slowest_needed_rtt = sorted_rtts[self.followers_needed - 1]
        return slowest_needed_rtt + self.disk_write_ms


@dataclass
class KafkaLatencyModel:
    """
    Models end-to-end Kafka produce latency (producer.send → broker ack).
    All times in milliseconds.
    """
    producer_to_leader_rtt_ms: float = 1.0
    linger_ms: float = 5.0
    broker_write_ms: float = 0.1
    acks: str = "all"                    # "1", "all", "0"
    isr_replication_rtts_ms: list[float] = field(default_factory=list)  # leader→each follower
    num_partitions: int = 1

    @property
    def produce_latency_ms(self) -> float:
        """
        Minimum produce latency for one batch, from send() to ack.
        """
        latency = self.linger_ms + self.producer_to_leader_rtt_ms + self.broker_write_ms
        if self.acks == "all" and self.isr_replication_rtts_ms:
            # Leader must get ack from all ISR followers
            # (In Kafka, unlike Raft, ALL ISR must ack for acks=all)
            replication_latency = max(self.isr_replication_rtts_ms) + self.broker_write_ms
            latency += replication_latency
        return latency


# ─── RTT Measurement ──────────────────────────────────────────────────────────────

def measure_tcp_connect_latency(
    host: str,
    port: int,
    count: int = 10,
    interval_s: float = 0.1,
    timeout_s: float = 5.0,
) -> LatencyStats:
    """
    Measure TCP connection establishment latency (SYN-SYN/ACK round trip).
    This is more representative of Kafka connection latency than ICMP ping.
    Uses non-blocking connect to measure SYN-SYN/ACK time.
    """
    samples = []

    for _ in range(count):
        start = time.perf_counter()
        success = True
        error_msg = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout_s)
            s.connect((host, port))
            elapsed_ms = (time.perf_counter() - start) * 1000
            s.close()
        except socket.timeout:
            elapsed_ms = timeout_s * 1000
            success = False
            error_msg = "timeout"
        except (ConnectionRefusedError, OSError) as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            success = False
            error_msg = str(e)
        finally:
            try:
                s.close()
            except Exception:
                pass

        samples.append(LatencySample(
            timestamp=time.time(),
            latency_ms=elapsed_ms,
            success=success,
            error=error_msg,
        ))
        time.sleep(interval_s)

    return compute_latency_stats(samples)


def measure_request_response_latency(
    host: str,
    port: int,
    request_bytes: bytes,
    count: int = 10,
    timeout_s: float = 5.0,
) -> LatencyStats:
    """
    Measure full request-response latency: connect + send + receive first byte.
    Useful for measuring actual Kafka or HTTP service latency.
    """
    samples = []

    for _ in range(count):
        start = time.perf_counter()
        success = True
        error_msg = None
        try:
            with socket.create_connection((host, port), timeout=timeout_s) as s:
                s.sendall(request_bytes)
                # Read first byte of response
                s.recv(1)
                elapsed_ms = (time.perf_counter() - start) * 1000
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            success = False
            error_msg = str(e)

        samples.append(LatencySample(
            timestamp=time.time(),
            latency_ms=elapsed_ms,
            success=success,
            error=error_msg,
        ))

    return compute_latency_stats(samples)


def measure_dns_latency(
    hostname: str,
    count: int = 20,
) -> LatencyStats:
    """
    Measure DNS resolution latency using the OS resolver.
    Includes cache effects (subsequent calls may be faster).
    """
    samples = []
    for _ in range(count):
        start = time.perf_counter()
        success = True
        error_msg = None
        try:
            socket.getaddrinfo(hostname, None)
            elapsed_ms = (time.perf_counter() - start) * 1000
        except socket.gaierror as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            success = False
            error_msg = str(e)
        samples.append(LatencySample(
            timestamp=time.time(),
            latency_ms=elapsed_ms,
            success=success,
            error=error_msg,
        ))
    return compute_latency_stats(samples)


# ─── Statistics ─────────────────────────────────────────────────────────────────

def compute_latency_stats(samples: list[LatencySample]) -> LatencyStats:
    """Compute statistical summary from a list of latency samples."""
    if not samples:
        return LatencyStats()

    successful = [s.latency_ms for s in samples if s.success]
    failed = [s for s in samples if not s.success]
    total = len(samples)

    if not successful:
        return LatencyStats(
            count=total,
            loss_pct=100.0,
        )

    sorted_latencies = sorted(successful)
    n = len(sorted_latencies)

    def percentile(sorted_data: list[float], p: float) -> float:
        """Compute the pth percentile of sorted data."""
        if not sorted_data:
            return 0.0
        idx = p / 100 * (len(sorted_data) - 1)
        lower = int(idx)
        upper = lower + 1
        if upper >= len(sorted_data):
            return sorted_data[-1]
        frac = idx - lower
        return sorted_data[lower] * (1 - frac) + sorted_data[upper] * frac

    return LatencyStats(
        count=total,
        min_ms=sorted_latencies[0],
        max_ms=sorted_latencies[-1],
        mean_ms=statistics.mean(sorted_latencies),
        median_ms=statistics.median(sorted_latencies),
        stdev_ms=statistics.stdev(sorted_latencies) if n > 1 else 0.0,
        p95_ms=percentile(sorted_latencies, 95),
        p99_ms=percentile(sorted_latencies, 99),
        p999_ms=percentile(sorted_latencies, 99.9),
        loss_pct=len(failed) / total * 100,
    )


# ─── Raft / Kafka Latency Modeling ──────────────────────────────────────────────

def model_raft_topologies() -> list[dict]:
    """
    Model commit latency for common Raft deployment topologies.
    Returns a list of scenario dicts.
    """
    topologies = [
        {
            "name": "3-node same-AZ",
            "description": "All nodes in the same availability zone",
            "model": RaftLatencyModel(
                node_rtts_ms=[0.5, 0.5],  # RTT to 2 followers
                disk_write_ms=0.1,
            ),
        },
        {
            "name": "3-node multi-AZ (same region)",
            "description": "One node per AZ; quorum can be same-AZ",
            "model": RaftLatencyModel(
                node_rtts_ms=[0.5, 2.0],  # 1 same-AZ, 1 cross-AZ follower
                disk_write_ms=0.1,
            ),
        },
        {
            "name": "3-node multi-region (1 per region)",
            "description": "Strict geographic distribution; every commit crosses regions",
            "model": RaftLatencyModel(
                node_rtts_ms=[65.0, 85.0],  # us-east→us-west, us-east→eu-west
                disk_write_ms=0.1,
            ),
        },
        {
            "name": "5-node: 3 primary + 2 secondary regions",
            "description": "3 nodes in primary region, 1 each in 2 secondary regions",
            "model": RaftLatencyModel(
                node_rtts_ms=[0.5, 0.5, 65.0, 85.0],  # 2 same-region + 2 remote
                disk_write_ms=0.1,
            ),
        },
    ]

    results = []
    for t in topologies:
        model = t["model"]
        results.append({
            "name": t["name"],
            "description": t["description"],
            "nodes": len(model.node_rtts_ms) + 1,
            "quorum": model.quorum_size,
            "commit_latency_ms": round(model.commit_latency_ms, 2),
            "follower_rtts_ms": model.node_rtts_ms,
        })
    return results


def model_kafka_e2e_latency() -> list[dict]:
    """
    Model Kafka end-to-end latency for common configurations.
    """
    scenarios = [
        {
            "name": "Same-AZ, acks=1, low latency",
            "model": KafkaLatencyModel(
                producer_to_leader_rtt_ms=0.5,
                linger_ms=0,
                broker_write_ms=0.05,
                acks="1",
                isr_replication_rtts_ms=[0.5],
            ),
        },
        {
            "name": "Same-AZ, acks=all, default batching",
            "model": KafkaLatencyModel(
                producer_to_leader_rtt_ms=0.5,
                linger_ms=5,
                broker_write_ms=0.05,
                acks="all",
                isr_replication_rtts_ms=[0.5, 0.5],
            ),
        },
        {
            "name": "Multi-AZ, acks=all, 1 follower cross-AZ",
            "model": KafkaLatencyModel(
                producer_to_leader_rtt_ms=0.5,
                linger_ms=5,
                broker_write_ms=0.05,
                acks="all",
                isr_replication_rtts_ms=[0.5, 2.0],  # 1 same-AZ, 1 cross-AZ
            ),
        },
        {
            "name": "Cross-region, acks=all (disaster recovery setup)",
            "model": KafkaLatencyModel(
                producer_to_leader_rtt_ms=0.5,
                linger_ms=5,
                broker_write_ms=0.05,
                acks="all",
                isr_replication_rtts_ms=[0.5, 0.5, 65.0],  # 2 same-region + 1 remote DR
            ),
        },
    ]

    results = []
    for s in scenarios:
        model = s["model"]
        results.append({
            "name": s["name"],
            "acks": model.acks,
            "linger_ms": model.linger_ms,
            "isr_rtts_ms": model.isr_replication_rtts_ms,
            "produce_latency_ms": round(model.produce_latency_ms, 2),
        })
    return results


# ─── Queuing Theory ───────────────────────────────────────────────────────────────

def queuing_latency_multiplier(utilization: float) -> float:
    """
    M/M/1 queue: compute the waiting time multiplier at a given utilization.
    Returns: queuing_wait / service_time (e.g., 9.0 means 9× service time in queue)
    utilization must be in (0, 1).
    """
    if utilization <= 0 or utilization >= 1:
        return float('inf') if utilization >= 1 else 0.0
    return utilization / (1 - utilization)


def print_queuing_table() -> None:
    """Print utilization vs latency multiplier table."""
    print(f"  {'Utilization':>12}  {'Queue wait':>12}  {'Total latency (× service time)':>32}")
    print(f"  {'-'*12}  {'-'*12}  {'-'*32}")
    for pct in [10, 20, 30, 50, 60, 70, 80, 90, 95, 99]:
        u = pct / 100
        mult = queuing_latency_multiplier(u)
        total = 1 + mult
        warn = "  ⚠️" if pct >= 80 else ""
        print(f"  {pct:>11}%  {mult:>11.1f}×  {total:>31.1f}×{warn}")


def little_law_concurrency(
    throughput_rps: float,
    latency_ms: float,
) -> float:
    """
    Little's Law: L = λ × W
    Returns the required concurrency (in-flight requests) to achieve
    the given throughput at the given latency.
    """
    latency_s = latency_ms / 1000
    return throughput_rps * latency_s


# ─── Report ───────────────────────────────────────────────────────────────────────

def latency_health_report(
    endpoints: Optional[list[tuple[str, int]]] = None,
    sample_count: int = 20,
) -> None:
    """Print a latency health report for given host:port endpoints."""
    print("=" * 70)
    print("NETWORK LATENCY HEALTH REPORT")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    if endpoints:
        print(f"\n[1] TCP Connect Latency ({sample_count} samples each)")
        print(f"  {'Endpoint':<35}  {'P50':>7}  {'P95':>7}  {'P99':>7}  "
              f"{'Jitter':>8}  {'Loss':>6}")
        print(f"  {'-'*35}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*8}  {'-'*6}")

        for host, port in endpoints:
            stats = measure_tcp_connect_latency(host, port, count=sample_count)
            jitter = stats.stdev_ms
            warn = ""
            if stats.p99_ms > 50:
                warn = " ⚠️ HIGH P99"
            elif jitter > 5:
                warn = " ⚠️ HIGH JITTER"
            print(f"  {host+':'+str(port):<35}  "
                  f"{stats.median_ms:>6.1f}ms  {stats.p95_ms:>6.1f}ms  "
                  f"{stats.p99_ms:>6.1f}ms  {jitter:>7.2f}ms  "
                  f"{stats.loss_pct:>5.1f}%{warn}")

    print(f"\n[2] Raft Commit Latency by Topology")
    print(f"  {'Topology':<45}  {'Nodes':>5}  {'Quorum':>6}  "
          f"{'Commit Latency':>15}")
    print(f"  {'-'*45}  {'-'*5}  {'-'*6}  {'-'*15}")
    for t in model_raft_topologies():
        warn = " ⚠️" if t['commit_latency_ms'] > 10 else ""
        print(f"  {t['name']:<45}  {t['nodes']:>5}  {t['quorum']:>6}  "
              f"{t['commit_latency_ms']:>13.1f}ms{warn}")

    print(f"\n[3] Kafka Produce Latency Models")
    print(f"  {'Scenario':<45}  {'acks':>5}  {'Latency':>10}")
    print(f"  {'-'*45}  {'-'*5}  {'-'*10}")
    for s in model_kafka_e2e_latency():
        warn = " ⚠️" if s['produce_latency_ms'] > 20 else ""
        print(f"  {s['name']:<45}  {s['acks']:>5}  "
              f"{s['produce_latency_ms']:>8.1f}ms{warn}")

    print(f"\n[4] Queuing Latency Multiplier (M/M/1 model)")
    print_queuing_table()

    print(f"\n[5] Little's Law: Required Concurrency")
    print(f"  {'Throughput (rps)':>18}  {'Latency (ms)':>14}  "
          f"{'Required concurrency':>22}")
    print(f"  {'-'*18}  {'-'*14}  {'-'*22}")
    for tput, lat in [(1000, 5), (10000, 5), (10000, 50), (100000, 10)]:
        conc = little_law_concurrency(tput, lat)
        print(f"  {tput:>18,}  {lat:>13}ms  {conc:>21.0f}")

    print()
    print("=" * 70)


# ─── Main ─────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Network latency diagnostic toolkit")
    parser.add_argument('--endpoints', nargs='+', metavar='HOST:PORT',
                        help='Endpoints to measure (e.g. kafka-1:9092 postgres:5432)')
    parser.add_argument('--samples', type=int, default=20,
                        help='Number of samples per endpoint (default: 20)')
    parser.add_argument('--model', action='store_true',
                        help='Show latency models only (no measurement)')
    args = parser.parse_args()

    endpoints = []
    if args.endpoints:
        for ep in args.endpoints:
            host, port = ep.rsplit(':', 1)
            endpoints.append((host, int(port)))

    if args.model:
        print("\n=== Raft Latency Models ===")
        for t in model_raft_topologies():
            print(f"\n{t['name']}: {t['commit_latency_ms']}ms commit latency")
            print(f"  Nodes: {t['nodes']}, Quorum: {t['quorum']}")
            print(f"  Follower RTTs: {t['follower_rtts_ms']} ms")

        print("\n=== Kafka Produce Latency Models ===")
        for s in model_kafka_e2e_latency():
            print(f"\n{s['name']}: {s['produce_latency_ms']}ms")
            print(f"  acks={s['acks']}, linger={s['linger_ms']}ms, "
                  f"ISR RTTs={s['isr_rtts_ms']}ms")
    else:
        latency_health_report(endpoints=endpoints, sample_count=args.samples)
```

---

## 6. Visual Reference

### Latency by Distance

```
Distance        RTT              Use case
──────────────────────────────────────────────────────
Same CPU        1–10 ns          L1/L2 cache
Same socket     10–40 ns         L3 cache, DRAM
Same server     50–300 µs        Unix socket, loopback
Same rack       100–500 µs       Local NIC → top-of-rack switch
Same datacenter 200–500 µs       Intra-DC east-west traffic
Same AZ         0.3–1 ms         ██
Cross-AZ        1–3 ms           ████
Same-continent  10–50 ms         ██████████████
Cross-continent 60–120 ms        ████████████████████████████
Intercontinental 150–200 ms      ████████████████████████████████████████
```

### Raft Commit Latency: Topology Comparison

```
3-node same-AZ:
  Leader ──0.5ms──► F1    Wait for F1 (quorum = 2)
         ──0.5ms──► F2    Commit latency: ~0.6ms

3-node multi-region (worst case):
  Leader ──65ms───► F1    Wait for F1 (fastest)
         ──85ms───► F2    Commit latency: ~65ms
                          Every write costs a US-East→US-West RTT

5-node, 3 in primary region (best of both):
  Leader ──0.5ms──► F1    Quorum = 3; satisfied within primary region
         ──0.5ms──► F2    Commit latency: ~0.6ms
         ──65ms──► F3    Remote replicas don't delay commits
         ──85ms──► F4
```

### Queuing Latency Non-Linearity

```
Latency
(× service time)
    │
100 │                                                     ●
 50 │                                              ●
 20 │                                         ●
 10 │                                   ●
  5 │                            ●
  2 │                    ●
  1 │           ●
  0 │     ●
    └──────────────────────────────────────────────────── utilization
      0%  20%  40%  60%  70%  80%  90%  95%  99%  100%

Doubling utilization from 50% → 90% multiplies latency by 9×.
The knee of the curve is at ~70% utilization.
```

### Kafka E2E Latency Decomposition

```
Time →
  │ Producer.send()
  ├─[linger.ms=5]──────────────────────────────────────► batch ready
  │                                                       │
  ├─[1 RTT: producer→leader]──────────────────────────── send batch
  │                                                       │
  │  [broker page cache write: 0.05ms]                    │
  │                                                       │
  ├─[acks=all: 1 RTT per ISR follower, in parallel]──────► last ISR ack
  │                                                       │
  ├─[leader sends ack to producer] ◄─────────────────────┘
  │
  ├─[consumer fetch.max.wait.ms: 0–500ms]──────────────── consumer poll()
  │
  ├─[1 RTT: broker→consumer]─────────────────────────────► consumer receives
  │
  │
  P50 with defaults: ~6ms (linger.ms=5 dominates)
  P50 with linger.ms=0: ~1ms (RTT dominates)
```

---

## 7. Common Mistakes

**Mistake 1: Optimizing average latency instead of P99.** Average latency hides the tail. A system with mean=2ms and P99=500ms (due to occasional GC pauses or network jitter) will appear healthy on average-based dashboards while 1% of users experience catastrophic latency. Always monitor percentiles, especially P99 and P999.

**Mistake 2: Placing Raft/KRaft nodes across three regions without understanding commit latency.** Three nodes, one per region, means every Raft commit requires a cross-region round trip (the fastest inter-region RTT becomes the commit floor). At 65+ ms per commit, this is completely unsuitable for high-throughput metadata operations. The correct design is 5 nodes with 3 in the primary region.

**Mistake 3: Confusing replication lag with latency.** Kafka replication lag is measured in bytes or time offset, not in RTT. A replication lag of 100 ms means the follower is 100 ms behind the leader's committed position — it is a throughput-based metric, not a pure latency metric. Confusing "my follower has 100ms lag" with "my network RTT is 100ms" leads to misdiagnosis.

**Mistake 4: Ignoring queuing latency under load.** Latency benchmarks run at 10% of system capacity look great. The same system at 90% capacity has 9× the queuing delay. Always benchmark at realistic production utilization (70–80%), not peak capacity.

**Mistake 5: Not measuring P99 latency for the slowest component.** In a fan-out system, the tail latency of the slowest component becomes the median latency of the overall operation. If a Spark stage reads from 100 partitions and P99 of each individual partition read is 50 ms, then approximately 1 - (0.99)^100 = 63% of stages take at least 50 ms. The P99 of individual components must be in single-digit milliseconds for the aggregate to be fast.

**Mistake 6: Using `ping` RTT as the TCP application RTT.** ICMP ping is often rate-limited or prioritized differently by routers. The actual TCP RTT measured by `ss -tipm` on a live Kafka connection may be 20–50% lower than the ICMP ping RTT to the same host.

---

## 8. Production Failure Scenarios

### Scenario 1: KRaft Controller Commit Latency Causes Kafka Instability

**Symptoms:** A Kafka cluster using KRaft (no ZooKeeper) shows intermittent broker deregistration and re-registration in logs. Topic metadata operations are slow. `kafka-metadata-quorum.sh --describe` shows `CurrentOffset` advancing slowly. The three KRaft controller nodes are in three separate AWS regions (us-east-1, us-west-2, eu-west-1).

**Root cause:** KRaft metadata commits require quorum agreement. With one node per region, every metadata commit — broker registration, partition reassignment, config change — requires at least one cross-region RTT (65 ms to us-west-2). Each broker heartbeat to the controller requires a metadata update. High cluster activity (many partition reassignments during rebalancing) generates thousands of KRaft commits per minute, each costing 65 ms. The KRaft leader's commit pipeline backs up, causing heartbeat timeouts (controlled by `broker.heartbeat.interval.ms` and `broker.session.timeout.ms`).

**Fix:** Move KRaft controller nodes to the same region: all three controllers in us-east-1 (or whichever region is primary). If geo-redundancy is required for KRaft, use 5 nodes with 3 in the primary region and 1 each in two secondary regions — commits remain within the primary region while cross-region replicas provide disaster recovery.

```properties
# server.properties — for a properly co-located KRaft setup
controller.quorum.voters=1@kafka-controller-1.us-east-1:9093,\
                          2@kafka-controller-2.us-east-1:9093,\
                          3@kafka-controller-3.us-east-1:9093
# All three controllers in the same region = sub-1ms commit latency
```

### Scenario 2: Spark Job Stage Duration Explodes After AZ Migration

**Symptoms:** A Spark job that previously ran in 45 minutes now takes 3.5 hours after the cluster was migrated from a single-AZ to a multi-AZ deployment (executors distributed across 3 AZs for availability). The job is shuffle-heavy with 500 executors and 2000 shuffle partitions.

**Root cause:** Cross-AZ network latency amplification. In a single-AZ deployment, all executor-to-executor shuffle TCP connections had sub-1ms RTT. In a multi-AZ deployment with 500 executors spread across 3 AZs, roughly 2/3 of all shuffle connections are cross-AZ (2 ms RTT). The shuffle involves establishing 500 × 500 / 2 ≈ 125,000 connections. Each cross-AZ connection establishment costs 2 ms (TCP handshake) + 4 ms (TLS handshake) = 6 ms. With 83,000 cross-AZ connections: 83,000 × 6 ms / degree_of_parallelism. With `spark.shuffle.io.numConnectionsPerPeer=4` and 4 threads, effective serialization is much less — but the aggregate still adds minutes.

Additionally, the shuffle data transfer itself is limited by the cross-AZ bandwidth. Each shuffle mapper must transfer data to 500 reducer locations; at 2 ms RTT and with TCP slow start, each transfer starts slowly, and the data volume is proportional to the number of cross-AZ transfers.

**Fix:** For latency-sensitive shuffle-heavy Spark jobs, use a single-AZ deployment. The availability tradeoff (loss of AZ fault tolerance) is acceptable for batch jobs where re-running is cheap. If multi-AZ is required, use `spark.locality.wait` to prefer local-AZ task scheduling: `spark.locality.wait.rack=10s` gives the scheduler 10 seconds to find an executor in the same AZ before giving up.

### Scenario 3: Kafka Consumer Lag Correlates with Time-of-Day

**Symptoms:** Kafka consumer lag for a critical pipeline grows every day between 9 AM and 11 AM UTC, then recovers. The producer throughput is constant. The consumer CPU is at 30%. Network throughput is well below link capacity.

**Root cause:** Queuing latency at 90% broker utilization during business hours. During business hours, a combination of pipelines hitting the same Kafka cluster drives broker CPU utilization from 50% (off-hours) to 88% (peak hours). At 88% utilization, queuing latency is approximately `0.88 / (1 - 0.88) = 7.3×` the baseline processing time. Produce and fetch requests that took 0.5 ms at 50% utilization now average 3.7 ms. The consumer's `poll()` is returning less data per poll because each fetch takes longer, causing the consumer to fall behind the producer.

**Fix:** Either scale the Kafka cluster (add brokers to distribute load and reduce per-broker utilization to < 70%), or add back-pressure on producers during peak hours using Kafka quotas (`kafka-configs.sh --entity-type clients --add-config producer_byte_rate=<rate>`).

### Scenario 4: P99 Latency Spike Correlates with Garbage Collection

**Symptoms:** Kafka producer P99 latency is 2 ms 95% of the time, but spikes to 250 ms every 30–60 seconds. The spike duration is approximately 200 ms. P50 is unaffected.

**Root cause:** JVM garbage collection. The Kafka broker JVM runs a G1GC full collection approximately every 45 seconds. During the GC pause (200 ms), the broker's request handler threads are suspended. Producers that sent requests during the GC pause wait the full 200 ms before receiving an acknowledgment. After GC, all queued requests are processed in burst, appearing as a latency spike followed by rapid recovery.

**Diagnosis:**
```bash
# Check GC logs on the broker
grep "GC pause" /var/log/kafka/kafka-gc.log | tail -20
# Look for "Full GC" or long Young GC pauses

# Correlate with produce latency spikes:
# Kafka metrics: kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce
# Look for bursts of high TotalTimeMs coinciding with GC timestamps
```

**Fix:** Tune G1GC to avoid long pauses:
```
KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=20 \
  -XX:G1HeapRegionSize=16m \
  -XX:InitiatingHeapOccupancyPercent=35 \
  -XX:G1ReservePercent=15"
```

Also check that heap size is appropriate (Kafka brokers typically run well with 6 GB heap; large heaps cause longer GC pauses).

---

## 9. Performance and Tuning

### Reducing Kafka Produce Latency

The dominant tuning levers, in order of impact:

**1. `linger.ms` (default: 0 in most configs)**
The time the producer waits to accumulate a batch before sending. Every millisecond of `linger.ms` adds directly to P50 produce latency but improves throughput (larger batches). For latency-sensitive pipelines: `linger.ms=0`. For throughput-optimized pipelines: `linger.ms=5–20`.

**2. ISR topology for `acks=all`**
With `acks=all`, the produce latency floor is `RTT(producer→leader) + max(ISR replication RTTs) + disk_write_latency`. Ensure follower brokers are in the same AZ as the leader for latency-sensitive topics. Use rack awareness (`broker.rack`) to control how partition leaders and followers are distributed.

**3. `fetch.max.wait.ms` on consumers (default: 500 ms)**
The broker will wait up to `fetch.max.wait.ms` before returning an empty fetch response. For low-latency consumption: `fetch.max.wait.ms=50` or lower. Trade-off: more frequent empty fetch responses increase CPU overhead on both broker and consumer.

**4. `socket.send.buffer.bytes` / `socket.receive.buffer.bytes`**
For cross-region Kafka (MirrorMaker 2), the BDP calculation from M27 applies. Set socket buffers to at least 2× BDP.

### Network Latency Tuning for Data Engineering Hosts

```bash
# /etc/sysctl.d/99-latency.conf

# Disable IRQ coalescing for lowest latency (at cost of CPU)
# (alternative: use ethtool -C to tune interrupt coalescing per NIC)

# Reduce TCP timestamp overhead (minor latency reduction)
net.ipv4.tcp_timestamps=0    # Disable if not needed for PAWS

# CPU governor: performance mode prevents frequency scaling latency
# (from M26 — CPU Diagnostics: frequency scaling adds 0.1–1ms jitter)
# Set in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Minimize kernel scheduling latency for network threads
# (From M26: isolcpus for network-critical processes)
```

### NIC Interrupt Affinity for Low Latency

```bash
# Pin NIC interrupts to the same NUMA node as Kafka's JVM
# Step 1: Find the NIC's IRQs
grep eth0 /proc/interrupts | awk '{print $1}' | tr -d ':'

# Step 2: Pin each IRQ to a CPU on NUMA node 0
echo 1 > /proc/irq/<IRQ>/smp_affinity

# Step 3: Pin Kafka JVM to NUMA node 0 (from M26)
numactl --cpunodebind=0 --membind=0 /opt/kafka/bin/kafka-server-start.sh ...

# Result: NIC interrupts and JVM processing share the same memory bus,
# eliminating cross-NUMA packet processing latency
```

### Understanding Latency Budgets

A latency budget allocates the total acceptable latency across all components in a pipeline. Example for a Kafka pipeline with a 50 ms SLO:

```
50 ms budget decomposed:
  Producer linger.ms:           5 ms
  Producer → broker (same-AZ): 0.5 ms
  Broker write + replication:   1 ms
  fetch.max.wait.ms:           30 ms   ← dominant consumer factor
  Broker → consumer:            0.5 ms
  Consumer processing:         10 ms
  Margin:                       3 ms
  Total:                       50 ms
```

If the consumer processing time grows to 25 ms (due to a new transform), the SLO is violated unless `fetch.max.wait.ms` is reduced to 15 ms (trading empty fetch overhead for latency).

---

## 10. Interview Q&A

**Q1: Explain the relationship between latency and Raft consensus. Why can't you make a 3-node multi-region Kafka (KRaft) cluster have sub-10ms commit latency if the regions are 65 ms apart?**

Raft achieves consensus through a write-ahead log replication protocol. The leader appends an entry to its own log and sends it to all followers in parallel. The entry is considered committed — and the leader can acknowledge it — when a quorum (majority) of nodes have confirmed appending it. For a 3-node cluster, quorum is 2 nodes: the leader plus one follower. The leader's commit latency is therefore bounded below by the RTT to the fastest quorum member, plus the follower's log append time (typically < 0.2 ms on NVMe).

If the three nodes are in three separate regions — say us-east-1, us-west-2 (65 ms RTT), and eu-west-1 (85 ms RTT) — the leader must receive an acknowledgment from at least one follower before committing. The follower in us-west-2 is the faster of the two, so the commit latency is bounded below by the 65 ms RTT to us-west-2. This is a physics constraint: the signal must physically travel 4,000 km to reach us-west-2 and back. Light travels at 200,000 km/s in fiber, giving a one-way propagation delay of 20 ms and a round-trip minimum of 40 ms. Actual RTT is 65 ms due to routing overhead. No protocol optimization, JVM tuning, or kernel parameter can reduce this below ~40 ms.

In KRaft terms, every metadata change — broker registration, partition reassignment, topic creation — requires a Raft commit. At 65 ms per commit, a cluster with frequent metadata operations (many partitions, frequent reassignments, high producer churn triggering metadata refreshes) will have a backlogged KRaft controller. Brokers waiting for metadata operations to complete will appear sluggish, and heartbeat timeouts become more likely. The correct design for geo-distributed Kafka is either to keep all KRaft controllers in one region, or use a 5-node layout where 3 controllers in the primary region can form a quorum without crossing regions.

**Q2: A Kafka producer SLO is P99 produce latency < 20 ms. You're seeing P99 = 180 ms. Walk through your diagnostic process.**

I would decompose the 180 ms into its constituent parts by measuring each hop independently, starting from the component most likely to dominate.

First, measure the producer-to-broker RTT using `ss -tipm dst :9092` on the producer host. This shows the actual TCP-layer RTT to the broker. If this is 0.5 ms (same AZ), the network is not the cause. If it's 2 ms or higher, check whether the producer is cross-AZ.

Second, check broker-side latency. Kafka exposes `RequestMetrics.TotalTimeMs` per request type. For ProduceRequest, TotalTimeMs = queue time + local time + remote time (replication) + response time. If `RequestQueueTimeMs` is high (> 10 ms), the broker's request handler threads are saturated — check broker CPU utilization and `num.network.threads`/`num.io.threads` configuration.

Third, if broker queue time is low, check replication latency. `ReplicaManager.IsrShrinkRate` and `ReplicaFetcherManager.MaxLag` show whether ISR followers are keeping up. If a follower is lagging significantly, it may be causing `acks=all` to wait longer than expected. Check the follower broker's disk I/O and network.

Fourth, check for JVM GC pauses on the broker. A 180 ms P99 that correlates with GC activity (check `jstat -gcutil <broker-pid> 1s` or the broker's GC log) suggests that GC pauses are causing occasional blocking. G1GC pauses > 20 ms should appear directly as P99 latency spikes.

Fifth, if none of the above, look at the producer itself: `linger.ms` > 100 ms, an overloaded `producer.send()` thread, or a slow `Serializer` would cause producer-side latency that has nothing to do with the network.

**Q3: Explain Little's Law and how you would use it to estimate the number of Kafka partitions needed for a given throughput and latency target.**

Little's Law states that in any stable system, the average number of items in the system (L) equals the average arrival rate (λ) multiplied by the average time each item spends in the system (W): L = λ × W.

To apply it to Kafka partition sizing: treat each partition as an independent service unit. A partition can be read by at most one consumer in a consumer group at a time. The "items in the system" are the messages being processed; the arrival rate is the message throughput; the time in system is the processing latency per message.

Suppose the requirement is λ = 50,000 messages/second throughput with W = 10 ms maximum end-to-end latency (P99). Little's Law says L = 50,000 × 0.010 = 500. This means the system needs 500 units of concurrency — 500 messages can be "in flight" simultaneously. If each consumer thread handles one message at a time and has 10 ms processing latency, we need 500 consumer threads. If each consumer instance has 10 threads, we need 50 consumer instances, each handling one partition. That gives us a lower bound of 50 partitions.

In practice, you add headroom (say 2×), so you'd start with 100 partitions. This also bounds the maximum consumer group parallelism — you cannot have more consumers than partitions in a group — so the partition count should be set based on the expected maximum consumer parallelism, not just current needs. Partitions cannot be reduced without recreating the topic, so it's better to overprovision by 2–4× at creation time.

---

## 11. Cross-Question Chain

**Interviewer:** You're designing a multi-region Kafka deployment for a financial services company. Requirements: primary in us-east-1, disaster recovery in eu-west-1, RPO (Recovery Point Objective) < 5 minutes, P99 produce latency < 10 ms for normal operation. How do you design it?

**Candidate:** These two requirements are in tension. A P99 < 10 ms produce latency with `acks=all` across regions is impossible: the us-east-1 to eu-west-1 RTT is 80–95 ms, which would make every produce request wait for eu-west-1 acknowledgment. So I would decouple the replication topology from the primary cluster's durability model. The primary Kafka cluster in us-east-1 has three brokers with `acks=all` guaranteeing durability within the region — P99 produce latency of 1–2 ms with all three brokers in the same region. Separately, MirrorMaker 2 asynchronously replicates to eu-west-1 with typical lag of 1–3 minutes (well within the 5-minute RPO). The eu-west-1 cluster is a replica, not part of the ISR for the primary cluster. For failover, the eu-west-1 cluster is promoted to primary (DNS update, consumer offset import) with RPO bounded by MirrorMaker 2 lag.

**Interviewer:** The risk committee says asynchronous replication is unacceptable — they want RPO of zero (no data loss on failover). How does that change your design?

**Candidate:** Zero RPO means synchronous replication across regions — every produce request must be acknowledged by eu-west-1 before returning success. This means the eu-west-1 broker must be in the ISR for every partition, and `acks=all` must wait for eu-west-1 acknowledgment. At 80–95 ms RTT, P99 produce latency becomes at minimum 80 ms. The 10 ms latency SLO is incompatible with zero RPO and a cross-Atlantic topology. I would present this trade-off to the risk committee explicitly: zero RPO requires 80+ ms produce latency, or the primary site must be relocated to be geographically closer to the DR site (e.g., both on the US East Coast with ~5 ms inter-city RTT, reducing the DR replication overhead to ~10 ms while maintaining near-zero RPO). If 80 ms is truly unacceptable and zero RPO is non-negotiable, a different architecture is needed — perhaps a synchronous write to an append-only transaction log in both regions using a consensus protocol designed for this (like CockroachDB's multi-region tables), with Kafka used for async downstream consumers within each region.

**Interviewer:** Let's say they accept 80 ms. You configure three brokers per region, `acks=all`, with eu-west-1 ISR members. After deployment, you see that P99 produce latency is 250 ms, much higher than the 80–95 ms RTT would suggest. What's happening?

**Candidate:** If the floor is 80 ms but P99 is 250 ms, the 170 ms gap above the floor comes from one of three sources. First, queuing at the eu-west-1 broker: if the eu-west-1 broker is handling high request volume, the replication request from the us-east-1 leader spends time in the request queue before being processed. At 90% broker utilization, queuing delay is 9× the processing time — even if processing is 5 ms, queuing adds 45 ms. Second, the eu-west-1 broker's replication I/O is slow — disk `w_await` is elevated, so appending the replicated data takes 30–50 ms instead of < 1 ms. Third, the TCP connection between the leader and the eu-west-1 follower has high queuing delay from network congestion on the cross-Atlantic link during peak hours, or the socket buffers are undersized for the 80 ms RTT path (BDP = 10 Gbps × 80 ms = 100 MB; check `net.core.rmem_max`). I'd start by measuring the actual RTT between the leader and eu-west-1 follower using `ss -tipm` during a high-latency window, then check eu-west-1 broker request queue depth and disk I/O with `kafka-metrics-reporter` and `iostat`.

**Interviewer:** You find the issue: the cross-Atlantic TCP connections have a receive buffer of 16 MB but the BDP is 100 MB. After fixing the buffer, P99 is 95 ms — right at the RTT floor. The business now wants to add a third region (ap-northeast-1, Tokyo, ~150 ms RTT from us-east-1). They still want zero RPO. What happens to produce latency?

**Candidate:** With Tokyo in the ISR and `acks=all`, every produce request must wait for Tokyo to acknowledge. Tokyo's RTT from us-east-1 is 150–175 ms. The produce latency P50 jumps from 80 ms (eu-west-1 floor) to 150 ms (Tokyo floor) because `acks=all` waits for ALL ISR replicas, and Tokyo is the slowest. This is the key distinction between Kafka's `acks=all` (all ISR must acknowledge) and Raft's quorum (only majority must acknowledge). With Raft, adding Tokyo would not increase commit latency because you still only need a quorum — but Kafka's `acks=all` is stricter. The alternatives for three-region zero-RPO Kafka are: either accept 150 ms produce latency, or use per-topic `min.insync.replicas=2` with the Tokyo replica not contributing to the ISR requirement (use `unclean.leader.election.enable=false` and `min.insync.replicas=2` to require only 2 ISR acknowledgments, allowing the Tokyo replica to be asynchronous). The zero-RPO guarantee is then only valid if both us-east-1 AND eu-west-1 fail simultaneously before Tokyo catches up — a scenario the risk committee may consider acceptable.

**Interviewer:** You recommend `min.insync.replicas=2` with three replicas (one per region). The risk committee approves it. Six months later, the eu-west-1 broker has a network partition from us-east-1. The partition lasts 2 hours. What is the ISR behavior and what are the data loss implications?

**Candidate:** When the eu-west-1 broker loses connectivity to the us-east-1 leader, the leader's ISR tracking detects that eu-west-1 is not fetching within `replica.lag.time.max.ms` (default 30 seconds). After 30 seconds, the leader removes eu-west-1 from the ISR. The ISR is now: [us-east-1 leader, ap-northeast-1 follower]. With `min.insync.replicas=2`, there are exactly 2 ISR replicas, which satisfies the minimum. Producers can continue producing with `acks=all` — each write is acknowledged by both us-east-1 and ap-northeast-1. eu-west-1 falls further behind during the 2-hour partition.

The data loss implication: during the partition, eu-west-1 is not receiving new writes. When the partition heals, eu-west-1 reconnects, truncates its log to the last synchronized offset, and begins fetching missed data from the leader. Once fully caught up, it rejoins the ISR. No data is lost during this process because the leader and ap-northeast-1 both have all the data.

The risk scenario is what happens if us-east-1 suffers a catastrophic failure (data center loss) during the 2-hour partition. If that happens, the cluster must elect a new leader from the remaining ISR: only ap-northeast-1 is in the ISR, so ap-northeast-1 becomes the leader. All data produced during the 2-hour partition is available on ap-northeast-1 and there is zero data loss — because ap-northeast-1 was in the ISR throughout. eu-west-1 is behind and its data is 2 hours stale. The zero-RPO guarantee holds as long as at least one ISR member (other than the failed leader) is up. In this topology, the only scenario for data loss is if us-east-1 AND ap-northeast-1 both fail simultaneously while eu-west-1 is partitioned — an extremely unlikely multi-failure scenario.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is propagation delay? | Time for a signal to travel from sender to receiver. Physics floor: distance / (200,000 km/s for fiber). Cannot be reduced without moving servers. |
| 2 | What is queuing delay? | Time a packet spends waiting in a queue (NIC buffer, switch buffer, OS network stack). The dominant source of latency variance. Grows non-linearly with utilization. |
| 3 | What is the M/M/1 queuing formula for waiting time? | W_queue = (ρ / μ) × (1 / (1 - ρ)). At 90% utilization, queuing wait = 9× service time. At 50%, = 1× service time. |
| 4 | What is Little's Law? | L = λ × W. Average items in system = throughput × average latency. Used to derive required concurrency from throughput and latency targets. |
| 5 | Why is P99 more important than average latency? | Latency distributions are right-skewed. Average hides the tail. P99 tells you what the 1 worst percent experiences — at 1M rps, that's 10,000 requests/second in the tail. |
| 6 | What is the approximate RTT between two AWS AZs in the same region? | 1–3 ms. Same-AZ: 0.3–1 ms. Cross-region US East↔US West: ~65 ms. Cross-Atlantic: ~85 ms. |
| 7 | What is the speed of light in fiber? | ~200,000 km/s (2/3 of vacuum speed). Sets the physics floor for propagation delay. |
| 8 | What is Raft commit latency in a 3-node same-AZ cluster vs. 1-node-per-region? | Same-AZ: ~0.6 ms (0.5 ms RTT + disk write). One per region (US/EU/AP): ~65 ms minimum (US-East→US-West RTT). |
| 9 | How does adding a 3rd region to a 3-node Kafka ISR affect produce latency? | `acks=all` waits for ALL ISR replicas. Adding a 150 ms RTT Tokyo follower makes the floor 150 ms per produce, even if the other two replicas are local. |
| 10 | What does `min.insync.replicas=2` allow with 3 replicas across 3 regions? | Produces succeed if at least 2 ISR replicas acknowledge. A third replica (e.g., Tokyo) can fall out of ISR without blocking producers — but zero-RPO is only guaranteed if 2+ ISR replicas survive any failure. |
| 11 | What is the 99th percentile problem in fan-out systems? | If N parallel calls each have P99 = X ms, the probability that at least one exceeds X is 1-(0.99)^N. At N=100, that's 63%. P99 of individual calls → ~P50 of the aggregate. |
| 12 | What is bandwidth-delay product? | BDP = bandwidth × RTT. The minimum TCP buffer size needed to saturate a link. Cross-region 10 Gbps at 80 ms RTT requires ~100 MB TCP buffers. |
| 13 | What Kafka config dominates consumer-side latency? | `fetch.max.wait.ms` (default 500 ms). The broker waits up to this long before returning an empty fetch. For low-latency consumption, reduce to 50 ms or lower. |
| 14 | What Kafka config dominates producer-side latency? | `linger.ms`. Every millisecond of linger adds directly to P50 produce latency. Set to 0 for lowest latency; higher for better throughput via batching. |
| 15 | What `tc` command adds 50 ms latency to outgoing traffic? | `sudo tc qdisc add dev eth0 root netem delay 50ms` — useful for testing latency tolerance. Remove with `sudo tc qdisc del dev eth0 root`. |
| 16 | Why is running a system at 90% utilization dangerous for latency SLOs? | M/M/1 queuing model: at 90%, queuing delay = 9× service time. A 1 ms request becomes 10 ms on average. P99 is far worse. Headroom below 70% utilization is recommended. |
| 17 | What tool shows per-hop latency and packet loss to a destination? | `mtr --tcp --report --report-cycles 50 <host>`. Better than traceroute for identifying which hop contributes latency or loss. |
| 18 | What `ss` flag shows the RTT of active TCP connections? | `ss -tipm` — shows `rtt:<smoothed_rtt>/<variance>` in milliseconds per socket. |
| 19 | Why does the 5-node Raft layout (3+1+1) allow fast commits? | With 5 nodes and quorum=3, the 3 nodes in the primary region form a quorum. Commits succeed within the primary region without waiting for cross-region followers. |
| 20 | What is jitter, and why does it matter for streaming pipelines? | Jitter = variance in latency (stdev). High jitter causes bursty consumer behavior: idle periods followed by backlog spikes. Monitor stdev alongside mean/P99. |

---

## 13. Further Reading

- **"Designing Data-Intensive Applications" — Martin Kleppmann, Chapter 8 (The Trouble with Distributed Systems):** The best practical treatment of latency, network unreliability, and timing in distributed systems. Explains why latency tail matters, the role of clocks in distributed systems, and the constraints that physics places on consistency.
- **"The Tail at Scale" — Google (2013, CACM):** The seminal paper on tail latency in large-scale distributed systems. Explains why latency percentiles compound in fan-out systems and describes Google's hedged request and tied request techniques. Freely available as a PDF.
- **"Systems Performance" — Brendan Gregg, Chapter 10 (Network):** Performance analysis methodology for network latency, including `ss`, `tcpdump`, and perf-based approaches. Chapter 2 (Methodology) covers USE method application to network.
- **Raft visualization — thesecretlivesofdata.com/raft/:** An interactive animation of the Raft consensus algorithm. Essential for building a visual intuition for leader election, log replication, and how RTT determines commit latency.
- **"In Search of an Understandable Consensus Algorithm" (Raft paper) — Ongaro & Ousterhout:** The original Raft paper. Section 5 (The Raft Consensus Algorithm) is the definitive reference for commit latency analysis.
- **Confluent blog: "Kafka Latency Tuning Guide":** Practical guide to reducing Kafka end-to-end latency, covering `linger.ms`, `fetch.max.wait.ms`, partition count, and network tuning.
- **AWS latency measurements — cloudping.info / cloudharmony.com:** Real-time and historical inter-region latency measurements for all AWS, GCP, and Azure regions. Essential for designing multi-region architectures.

---

## 14. Lab Exercises

**Exercise 1: Measure and Model RTT**

From a machine with access to a Kafka cluster, measure the RTT to each broker with `hping3 -S -p 9092 -c 50`. Also measure the TCP RTT on an active Kafka connection with `ss -tipm dst :9092`. Compare the two — are they the same? Use the measured RTTs in the `RaftLatencyModel` from the code toolkit to compute the theoretical minimum commit latency for your topology.

**Exercise 2: Observe Queuing Latency Under Load**

Set up a local Kafka broker. Run `kafka-producer-perf-test.sh` at 10%, 50%, 80%, and 95% of the broker's capacity (measure capacity with a calibration run at `--throughput -1`). At each utilization level, observe the P99 produce latency. Plot the relationship and compare to the M/M/1 model predictions.

**Exercise 3: tc-Based Latency Testing**

Add 50 ms artificial latency to the loopback interface: `tc qdisc add dev lo root netem delay 50ms`. Run the Kafka producer performance test and observe the effect on P50 and P99 latency. Then add jitter: `tc qdisc change dev lo root netem delay 50ms 20ms 50%`. Observe how jitter affects the P99. Remove with `tc qdisc del dev lo root`.

**Exercise 4: Raft Commit Latency Measurement**

In a KRaft cluster, use `kafka-metadata-quorum.sh --describe` to observe `CurrentOffset` and `LastFetchTimestamp` for each voter. Measure how fast `CurrentOffset` advances (commits per second). Compare commit rate at different broker CPU utilizations. Add artificial latency with `tc` to simulate cross-AZ RTT and observe the effect on commit rate.

**Exercise 5: Fan-Out Latency Simulation**

Write a Python program that makes N parallel TCP connections to the same endpoint and measures how long it takes for ALL connections to complete (the max, not the mean). Vary N from 1 to 1000. Plot max latency vs N. Add P99 latency to the connection function (by randomly sleeping 100 ms for 1% of connections). Observe how the aggregate max latency approaches the P99 as N grows.

---

## 15. Key Takeaways

Latency is a physical constraint before it is an engineering problem. The speed of light in fiber — 200,000 km/s — sets a floor below which no engineering can take you. Same-AZ RTT is measured in fractions of a millisecond; cross-Atlantic RTT is measured in hundreds of milliseconds. Before configuring `acks=all` on a cross-region ISR, before placing Raft nodes in separate continents, before blaming Kafka for 150 ms produce latency — know your topology's physics floor.

Three quantitative tools belong permanently in your toolkit. Little's Law (L = λW) converts between throughput, latency, and concurrency — use it to size partitions, consumer groups, and connection pools. The M/M/1 queuing model predicts how latency explodes near capacity — the knee of the curve is at 70% utilization, not 90%. And percentile decomposition of Raft commit latency (commit = RTT to Nth-fastest quorum member + disk write) explains why cross-region consensus is so expensive and why the 5-node layout (3+1+1) solves multi-region Kafka without paying the cross-region latency penalty on every commit.

On the diagnostic side: `ss -tipm` is the tool for TCP-level RTT on live connections, `mtr` is the tool for identifying which hop adds latency in a path, `tc netem` is the tool for locally simulating network conditions you can't reproduce in production, and percentile monitoring (P99, P999) is the measurement discipline that makes SLOs real.

---

## 16. Connections to Other Modules

- **M27 — TCP/IP Fundamentals:** Bandwidth-delay product (M27) is the connection between RTT (M30) and TCP buffer sizing. The slow start analysis from M27 is a specific latency amplifier at connection establishment time.
- **M29 — TLS and mTLS:** TLS adds 1–2 RTTs to connection setup. In a system with high connection churn (Spark shuffle), TLS connection overhead multiplies by the number of connections, making RTT directly relevant to overall job latency.
- **M31 — Load Balancing and Proxies:** Proxies add latency (typically 0.1–1 ms for L7 proxies). Understanding latency budgets (M30) determines how much proxy overhead is acceptable before an SLO is violated.
- **SYS-DST-101 — Distributed Systems Theory:** CAP theorem and PACELC model the trade-off between consistency and latency. PACELC explicitly states: during normal operation, consistent systems (C) have higher latency (L) than available systems (A). The RTT analysis in M30 quantifies the "L" in PACELC for Kafka and Raft.
- **DCS-KFK-101 — Apache Kafka Internals:** ISR, replication lag, and `acks=all` semantics are directly explained by RTT analysis. The minimum produce latency for `acks=all` is computed from the leader-to-ISR RTT.
- **DCS-STR-101 — Stream Processing Theory:** Watermarks and event-time processing are affected by end-to-end latency. A pipeline with high latency requires more aggressive watermark slack, which increases the delay before a window closes and results are emitted.

---

## 17. Anti-Patterns

**Anti-pattern: Synchronous cross-region calls in the hot path.** Any architecture where a user-facing request (or a Kafka produce request) must wait for a response from a server in another region before completing. At 65+ ms per RTT, this creates latency that no local optimization can compensate. Move cross-region communication to asynchronous paths (replication, MirrorMaker 2, event streaming) and keep the synchronous hot path within a single region.

**Anti-pattern: Using `acks=all` with cross-region ISR for low-latency topics.** `acks=all` requires ALL ISR members to acknowledge. Even one slow cross-region ISR member sets the produce latency floor. For topics with latency SLOs, either keep ISR members within a single region or use `min.insync.replicas=2` with replication factor 3 to avoid blocking on the remote replica.

**Anti-pattern: Monitoring only average latency.** Average latency makes tailed distributions look healthy. A service with mean=1 ms and P99=500 ms (due to GC pauses) will appear fine on average dashboards. Monitor P50, P95, P99, and P999. Alert on P99, not mean.

**Anti-pattern: Not accounting for startup latency.** Spark job benchmarks often measure steady-state throughput without accounting for startup: TCP connection establishment (and slow start), JVM JIT warmup, DNS resolution, and shuffle connection overhead all occur at the beginning of a job. A "10-minute job" that has a 2-minute startup cost is a 10-minute job — but optimizing the 8-minute steady-state phase by 20% saves 1.6 minutes, while fixing the startup cost saves 2 minutes.

**Anti-pattern: Ignoring network topology when placing Spark executors.** Spark data locality (`spark.locality.wait`) prefers to place tasks on the executor that holds the data. But without rack awareness configuration, Spark does not know which AZ executors are in and may schedule shuffle-intensive tasks to cross-AZ executors unnecessarily. Configure `spark.locality.wait.rack` and set up Spark rack awareness to avoid cross-AZ shuffle overhead.

---

## 18. Tools Reference

| Tool | Purpose | Key Flags |
|------|---------|-----------|
| `ping -c 20 <host>` | ICMP RTT measurement | `-i 0.1` for faster sampling |
| `hping3 -S -p <port> -c 20 <host>` | TCP SYN RTT (port-specific) | More accurate for Kafka than ICMP |
| `ss -tipm 'dst :<port>'` | TCP RTT on live connections | `rtt:<ms>/<variance>` in output |
| `traceroute -n <host>` | Per-hop latency | `-T -p <port>` for TCP |
| `mtr --tcp --port <p> --report <host>` | Continuous traceroute with stats | `--report-cycles 50` for stable stats |
| `tc qdisc add dev eth0 root netem delay Xms` | Add artificial latency | `loss X%` for packet loss |
| `tc qdisc del dev eth0 root` | Remove tc rules | — |
| `kafka-producer-perf-test.sh` | Kafka produce latency benchmark | `--throughput -1` for max, use `--print-metrics` |
| `kafka-metadata-quorum.sh --describe` | KRaft quorum status and lag | Shows CurrentOffset, LastFetchTimestamp |
| `nstat -az` | Per-second TCP counters | Includes retransmits (packet loss signal) |
| `netstat -s \| grep -E 'retransmit\|timeout'` | TCP error counters | Cumulative since boot |
| `sar -n DEV 1` | NIC throughput per second | `sar -n TCP 1` for TCP metrics |
| `wrk2 -t4 -c100 --latency <url>` | HTTP latency distribution | HDR histogram output |
| `python3 latency_diagnostics.py --model` | Model Raft/Kafka latency | No network required |

---

## 19. Glossary

**Bandwidth:** The maximum data transfer rate of a network link, measured in bits per second (bps). A property of the link, not the path.

**BDP (Bandwidth-Delay Product):** Bandwidth × RTT. The amount of data "in flight" on a path at full utilization. TCP buffers must be ≥ BDP to saturate high-latency links.

**Commit Latency (Raft):** Time from when a leader appends a log entry to when it receives quorum acknowledgment. Lower-bounded by the RTT to the Nth-fastest follower needed for quorum.

**Fan-out:** A pattern where one request triggers N parallel sub-requests. Total latency = max(sub-request latencies). P99 of sub-requests becomes ~P50 of the aggregate with large N.

**Jitter:** Variance in latency over time. High jitter causes bursty behavior in consumers. Measured as standard deviation of RTT samples.

**Latency:** The time to complete one unit of work. Distinct from throughput (units per second).

**Little's Law:** L = λ × W. Concurrency = throughput × latency. Fundamental relationship in queuing theory.

**M/M/1 Queue:** A queuing model with Poisson arrivals, exponential service times, and one server. Predicts W_queue = (ρ/μ) × 1/(1-ρ). Non-linear explosion at high utilization.

**MTR:** My TraceRoute. Combines ping and traceroute into a continuous per-hop latency and packet loss display.

**P99:** The 99th percentile latency — 99% of requests complete within this time. The standard production SLO metric.

**Propagation Delay:** Time for a signal to traverse a physical medium. Determined by distance and signal speed. The physics floor on latency.

**Queuing Delay:** Time a packet waits in a queue before being processed. The primary source of latency variance and the dominant component of tail latency.

**Raft:** A distributed consensus algorithm that replicates a log across multiple servers. Commit latency is bounded by the RTT to the quorum-forming follower.

**RTT (Round-Trip Time):** Time for a probe to travel to a destination and back. The fundamental latency unit for TCP and distributed systems analysis.

**Straggler:** In a fan-out parallel operation, the slowest sub-task that determines the overall completion time. Common in Spark stages where one slow partition read delays the entire stage.

**Tail Latency:** The high-percentile latency (P99, P999) of a distribution. Often much higher than the mean due to occasional slow events (GC pauses, network jitter, disk seek).

**Throughput:** The rate of work completion — messages per second, bytes per second. Distinct from latency.

**Utilization (ρ):** Fraction of time a resource is busy. At ρ → 1, queuing delay → ∞. The safe operating range is ρ < 0.7–0.8 for latency-sensitive systems.

---

## 20. Self-Assessment

1. A Kafka cluster has three brokers: one in us-east-1a, one in us-east-1b (2 ms RTT from 1a), and one in eu-west-1 (85 ms RTT from us-east-1). `acks=all`, `replication.factor=3`. What is the minimum P50 produce latency? What would it be with `min.insync.replicas=2`?
2. Compute the minimum KRaft commit latency for a 5-node cluster: 3 nodes in us-east-1 (0.5 ms intra-region RTT), 1 node in us-west-2 (65 ms RTT), 1 node in eu-west-1 (85 ms RTT). Show your work.
3. A Spark job with 200 executors has P99 shuffle read latency of 5 ms per partition. What fraction of stages will experience at least one partition read exceeding 5 ms? What would P99 need to be per partition to ensure < 5% of stages are affected?
4. A Kafka broker is at 85% CPU utilization. Using the M/M/1 model, if baseline processing time for a fetch request is 1 ms, what is the expected average fetch latency?
5. A pipeline requires 100,000 messages/second throughput with P99 latency ≤ 20 ms. Using Little's Law, how many concurrent "in-flight" requests are needed?
6. Explain the difference between latency and throughput using a highway analogy. Give a Kafka example where you would optimize for each separately.
7. Why does `acks=all` behave differently from Raft quorum when it comes to cross-region latency? Which is more sensitive to slow replicas, and why?
8. A Kafka consumer group has 10 consumers and 50 partitions. The consumers are in us-east-1; 20 of the 50 partitions are led by brokers in eu-west-1 (85 ms RTT). What is the effect on consumer throughput and latency for those 20 partitions?
9. What is jitter, and how does it affect P99 latency differently from mean latency? Give an example from a production Kafka environment.
10. You observe that `ss -tipm dst :9092` shows RTT of 0.8 ms for in-region connections and 85 ms for cross-region, yet your P99 produce latency is 250 ms. What additional components could explain the 250 ms, and what would you measure first?

---

## 21. Module Summary

Network latency is the tax that physics and protocol design impose on distributed systems. The speed of light in fiber — 200,000 km/s — sets a floor that no engineering can breach: a packet traveling from New York to Frankfurt cannot arrive in less than ~40 ms, regardless of how fast the hardware is on either end. Understanding this floor is what transforms latency from a mysterious variable into a predictable, budgeted resource.

The three quantitative frameworks that give data engineers control over latency: **Little's Law** (L = λW) connects throughput, latency, and concurrency — telling you how many parallel connections or partitions are needed to hit a throughput target at a given latency. The **M/M/1 queuing model** predicts the non-linear explosion of latency near capacity — at 90% utilization you have 9× the queuing delay you had at 50%, which is why maintaining headroom is a latency strategy, not a waste of capacity. And **Raft commit latency analysis** (commit ≈ RTT to Nth-fastest quorum member) quantifies exactly what cross-region Kafka deployments cost and why the 5-node layout with 3 nodes in the primary region is the canonical multi-region KRaft design.

On Kafka specifically: `linger.ms` and `fetch.max.wait.ms` are the dominant latency tuning levers — the network RTT itself is often a fraction of the latency budget. `acks=all` creates a produce latency floor equal to the slowest ISR replica's RTT; adding a cross-region follower to the ISR makes that floor cross-region. For streaming pipelines with latency SLOs, every component of the end-to-end latency must be budgeted explicitly — producer batching, replication, consumer fetch wait, and consumer processing — because all components are additive and must fit within the SLO.

The next and final module of SYS-NET-101 — M31: Load Balancing and Proxies — completes the networking picture by examining how traffic is distributed across multiple endpoints, how L4 and L7 load balancers interact with the TCP and application layers analyzed in M27–M30, and how connection pooling and reverse proxies affect the latency and throughput characteristics of data infrastructure.
