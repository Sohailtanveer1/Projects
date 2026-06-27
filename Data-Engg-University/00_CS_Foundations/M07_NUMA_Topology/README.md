# M02: NUMA Topology and Spark Executor Placement

**School:** CS Foundations (CSF)  
**Course:** CSF-ARC-102 — Memory Architecture  
**Module:** 02 of 05  
**Difficulty:** ★★★★☆  
**Estimated study time:** 5–6 hours  
**Last updated:** 2026-06-27

---

## Table of Contents

1. [Why This Module Exists](#1-why-this-module-exists)
2. [Prerequisites](#2-prerequisites)
3. [Learning Objectives](#3-learning-objectives)
4. [First Principles](#4-first-principles)
5. [Architecture](#5-architecture)
6. [Deep Dive — NUMA Mechanics](#6-deep-dive--numa-mechanics)
7. [Mental Models](#7-mental-models)
8. [Failure Scenarios](#8-failure-scenarios)
9. [Recovery Procedures](#9-recovery-procedures)
10. [Trade-offs](#10-trade-offs)
11. [Comparisons](#11-comparisons)
12. [Production Examples](#12-production-examples)
13. [Code](#13-code)
14. [Labs](#14-labs)
15. [Summary](#15-summary)
16. [Interview Q&A](#16-interview-qa)
17. [Cross-Question Chain](#17-cross-question-chain)
18. [What's Next](#18-whats-next)
19. [Flashcards](#19-flashcards)
20. [References](#20-references)

---

## 1. Why This Module Exists

In M01, we modeled memory as a single hierarchy: L1 → L2 → L3 → DRAM. That model is accurate for single-socket machines. But production data engineering runs on servers — and servers with more than 32 cores typically have **two or more CPU sockets**, each with its own local DRAM and its own L3 cache.

When a program running on socket 0 accesses memory that was allocated on socket 1's DRAM, it crosses an interconnect (Intel QPI / UltraPath Interconnect, or AMD Infinity Fabric). That crossing takes roughly **1.5–2× longer** than accessing local DRAM. This is **NUMA: Non-Uniform Memory Access** — the latency of memory access is not uniform; it depends on where the data lives relative to the core that needs it.

A Spark executor on a 2-socket server that ignores NUMA topology may route 50% of its memory accesses through the inter-socket interconnect. For a memory-bandwidth-limited workload (which most analytical pipelines are), this cuts effective throughput by 30–50% compared to a NUMA-aware placement.

This is not a theoretical concern. The AWS instances used for large Spark jobs — `r6i.32xlarge`, `r6i.48xlarge`, `x2idn.32xlarge`, `p3.16xlarge` — are all 2-socket (or more) NUMA systems. Spark, Kafka, and Flink all have NUMA-related configuration parameters that are off by default or misconfigured in most deployments.

---

## 2. Prerequisites

- CSF-ARC-102 M01: Cache lines, the memory hierarchy, DRAM access patterns, bandwidth limits.
- CSF-ARC-101 M04: `perf stat`, IPC analysis, identifying memory-bound workloads.
- Familiarity with Linux process management basics (CPU affinity, memory mapping concepts).

---

## 3. Learning Objectives

By the end of this module you will be able to:

1. Explain what NUMA is, why it exists, and what the remote memory penalty is in cycles and nanoseconds.
2. Use `numactl --hardware`, `numactl --show`, `lstopo`, and `lscpu` to detect NUMA topology on any Linux machine.
3. Explain Linux's first-touch NUMA memory allocation policy and why it causes remote memory access for multi-threaded workloads.
4. Use `numactl --localalloc`, `--membind`, and `--cpunodebind` to pin a process to a NUMA node.
5. Configure Spark's `spark.executor.cores` and JVM's `-XX:+UseNUMA` flag for NUMA-aware execution.
6. Explain why Java's NUMA support requires specific JVM flags and what the JVM does differently with them.
7. Detect NUMA-induced performance problems using `perf stat` and `numastat`.
8. Explain how Kafka's log partition assignment interacts with NUMA topology on multi-socket brokers.

---

## 4. First Principles

### Why Multiple Sockets Exist

A single CPU socket is limited by:
- **Die size:** Manufacturing yield drops as die area grows (more defects hit the chip). Beyond ~800 mm², defect-free yields collapse. An Intel Xeon Sapphire Rapids die is ~895 mm² — already at the limit.
- **Power:** A single die dissipating > 400W needs exotic cooling. More cores = more power per socket.
- **Memory bandwidth:** A single memory controller can support 8 DDR5 channels = ~307 GB/s. Scaling beyond this requires a second socket with its own controller.

The solution: multiple sockets connected by a high-speed inter-socket interconnect. Intel calls theirs UltraPath Interconnect (UPI). AMD calls theirs Infinity Fabric. These links run at 10–16 GT/s and provide 30–50 GB/s of cross-socket bandwidth — fast, but lower than a single socket's local DRAM bandwidth.

### Why NUMA Latency Is Higher

Local DRAM access path for socket 0:
```
Core 0 (socket 0) → L3 (socket 0) → memory controller (socket 0) → DIMM (socket 0)
Latency: ~70 ns
```

Remote DRAM access path for socket 0 accessing socket 1 memory:
```
Core 0 (socket 0) → L3 (socket 0) → memory controller (socket 0)
  → UPI link (socket 0 → socket 1, ~30 ns transit)
  → memory controller (socket 1) → DIMM (socket 1)
Latency: ~120–160 ns (1.7–2.3× local)
```

The UPI link adds ~30 ns of wire latency (physical distance across the PCB, signal propagation, protocol overhead) plus queuing delay if the link is shared. The remote memory controller adds another scheduling delay.

**Bandwidth impact:** Local DRAM bandwidth: 200 GB/s (8-channel DDR5-4800). UPI bandwidth: ~50 GB/s per link direction. If a core on socket 0 issues memory requests to socket 1 DRAM at maximum rate, the UPI link saturates at 50 GB/s — 4× less than local bandwidth.

### The First-Touch Policy

Linux's default NUMA memory allocation policy is **first-touch**: physical pages are allocated on the NUMA node of the thread that **first writes** to the virtual address. The `malloc()` or `mmap()` call only creates a virtual address range; physical pages are not assigned until the first write (demand paging).

```
Thread on socket 0: malloc(1 GB) → virtual addresses 0x7f0000000000–0x7f003FFFFFFF
No physical pages allocated yet.

Thread on socket 0: writes to 0x7f0000000000 → page fault
  → OS allocates physical page from socket 0's local DRAM
  → Maps virtual address to socket 0 physical address

Thread on socket 1: reads 0x7f0000000000
  → Hits L3 (socket 0's L3) → cache miss → socket 0 DRAM
  → Must traverse UPI to get to socket 0's memory controller
  → ~120–160 ns instead of ~70 ns
```

**The consequence for multi-threaded programs:** If one thread initializes a large array and many threads then process it, all of the array's pages are on the initializing thread's socket. Workers on the other socket pay the remote latency for every access.

**Java/JVM complication:** The JVM allocates heap memory in a large block at startup (or lazily via `mmap`). On a 2-socket system, the OS allocates these pages on whichever socket the JVM's main thread runs on. If the main thread is on socket 0, all JVM heap pages are on socket 0's DRAM. Spark executor threads that happen to be scheduled on socket 1 cores pay remote DRAM latency for all heap accesses — including every Spark task's data buffer, hash table, and intermediate result.

This is the default behavior on every Spark deployment that doesn't explicitly configure NUMA. It is silently degrading performance on every multi-socket AWS instance.

---

## 5. Architecture

### NUMA Topology Diagrams

**2-socket Intel Xeon (e.g., AWS r6i.32xlarge: 2× Intel Ice Lake)**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        r6i.32xlarge — 2-Socket NUMA                          │
│                                                                               │
│  ┌───────────────────────────────────┐  ┌───────────────────────────────────┐│
│  │  NUMA Node 0 (Socket 0)           │  │  NUMA Node 1 (Socket 1)           ││
│  │                                   │  │                                   ││
│  │  vCPUs: 0–31 (32 physical cores)  │  │  vCPUs: 32–63                     ││
│  │  L3: 60 MB (Intel Ice Lake)       │  │  L3: 60 MB                        ││
│  │                                   │  │                                   ││
│  │  Local DRAM: 512 GB               │  │  Local DRAM: 512 GB               ││
│  │  Bandwidth: ~200 GB/s             │  │  Bandwidth: ~200 GB/s             ││
│  │  Latency:    ~70 ns               │  │  Latency:    ~70 ns               ││
│  │                                   │  │                                   ││
│  └──────────────┬────────────────────┘  └───────────────┬───────────────────┘│
│                 │                                        │                    │
│                 └────────────── UPI Link ───────────────┘                    │
│                          Bandwidth: ~50 GB/s                                  │
│                          Latency:   ~120 ns (remote access penalty: 1.7×)    │
│                                                                               │
│  Total: 64 vCPUs, 1024 GB DRAM, 2 NUMA nodes                                │
└─────────────────────────────────────────────────────────────────────────────┘

numactl --hardware output for r6i.32xlarge:
  available: 2 nodes (0-1)
  node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
  node 0 size: 524288 MB
  node 0 free: 489234 MB
  node 1 cpus: 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63
  node 1 size: 524288 MB
  node 1 free: 501122 MB
  node distances:
    node   0   1
      0:  10  21    ← local access cost = 10 (normalized), remote = 21 (2.1x)
      1:  21  10
```

**4-socket AMD EPYC (e.g., AWS x2idn.32xlarge uses single-socket with NUMA-like domains)**
```
Some AMD EPYC CPUs (Milan, Genoa) expose multiple NUMA domains
within a single physical socket due to chiplet architecture:

  AMD EPYC 7R13 (Milan) in AWS x2idn.32xlarge:
  ┌──────────────────────────────────────────────────────────┐
  │  Single physical socket, 64 cores                        │
  │  But 8 Core Complex Dies (CCDs), 4 NUMA domains          │
  │                                                          │
  │  NUMA 0: cores 0-15,   128 GB DRAM, L3 32 MB           │
  │  NUMA 1: cores 16-31,  128 GB DRAM, L3 32 MB           │
  │  NUMA 2: cores 32-47,  128 GB DRAM, L3 32 MB           │
  │  NUMA 3: cores 48-63,  128 GB DRAM, L3 32 MB           │
  │                                                          │
  │  Cross-NUMA (within socket via Infinity Fabric):         │
  │    Latency: ~100 ns (vs ~70 ns local)                   │
  │    Bandwidth: ~150 GB/s cross-fabric                     │
  └──────────────────────────────────────────────────────────┘

  This is called "NUMA within a single socket" — the chiplet
  design means not all L3 cache is equally close to all cores.
  x2idn instances expose this as 4 NUMA nodes, not 1.
```

---

## 6. Deep Dive — NUMA Mechanics

### 6.1 NUMA Distance Table

Linux exposes NUMA distances via `numactl --hardware`. The distance table is a symmetric matrix where the diagonal (local access) is normalized to 10, and remote distances are multiples of 10:

```
Distance 10:  local (within same NUMA node)    — ~70 ns
Distance 21:  1-hop (adjacent socket via UPI)  — ~120–150 ns  (Intel)
Distance 31:  2-hop (across two UPI links)     — ~200 ns      (rare, 4-socket)
Distance 11:  within same socket, Infinity Fabric hop (AMD Milan intra-socket NUMA)
```

A distance of 21 means ~2.1× local latency. But the latency penalty is not the only cost — the **bandwidth penalty** is larger. A core on node 1 accessing node 0 memory gets at most the UPI bandwidth (50 GB/s) rather than the local DDR5 bandwidth (200 GB/s). For bandwidth-limited workloads (Spark shuffles, sorting, aggregations), the remote bandwidth penalty of 4× is more damaging than the latency penalty of 2×.

### 6.2 How the Linux Kernel Handles NUMA

The Linux kernel maintains a **NUMA zonelist**: for each NUMA node, a preference-ordered list of zones from which to allocate memory. By default, the zonelist for node 0 is `[node 0, node 1, node 2, ...]` — allocate from local first, fall back to remote if local is full.

**Memory policies (set by `set_mempolicy` syscall or `numactl`):**

| Policy | Behavior | When to use |
|---|---|---|
| `MPOL_DEFAULT` | Allocate on the thread's current NUMA node (first-touch) | Default — fine for single-socket, problematic for multi-socket |
| `MPOL_BIND` | Only allocate from specified nodes; OOM if unavailable | Pin a process to a specific node (strict locality) |
| `MPOL_INTERLEAVE` | Round-robin across specified nodes | Best for workloads that access all memory uniformly (benchmarking, shared caches) |
| `MPOL_LOCAL` | Always allocate on the calling thread's node | Explicit first-touch guarantee |
| `MPOL_PREFERRED` | Prefer specified node, fall back if full | Soft locality preference |

**The JVM default problem:** The JVM calls `mmap()` to reserve heap space, then writes to it lazily. In a typical JVM startup on a 2-socket server:
1. JVM main thread (socket 0) calls `mmap()` for 200 GB heap.
2. JVM main thread writes the first few pages (initialization) → pages land on socket 0.
3. Spark executor threads are created by a thread pool; OS schedules some on socket 1.
4. Executor threads allocate Spark task buffers in heap → JVM requests more pages from OS → these get written by the executor thread (socket 1) → land on socket 1.
5. But the GC root objects and task metadata (allocated by the main thread earlier) are on socket 0.
6. Executor threads on socket 1 access GC roots on socket 0 → remote NUMA access at every GC cycle.

This is why `-XX:+UseNUMA` matters for Spark: it enables JVM-level NUMA-awareness, allocating from thread-local NUMA nodes consistently.

### 6.3 The JVM's `-XX:+UseNUMA` Flag

Without `-XX:+UseNUMA`: The G1 or Parallel GC manages the heap as a single flat address space. Pages are allocated wherever the OS decides (typically first-touch, often causing remote accesses).

With `-XX:+UseNUMA`: The JVM's allocator is NUMA-aware:
- Young generation is divided into per-NUMA-node chunks. Each thread allocates from its local node's chunk (Thread-Local Allocation Buffers, TLABs, are NUMA-local).
- Objects allocated by socket 1 threads go to socket 1 DRAM. Objects allocated by socket 0 threads go to socket 0 DRAM.
- The GC interleaves collection across both nodes' chunks in parallel, keeping objects on their originating nodes where possible.

**Effect:** On a 2-socket server running a Spark shuffle (memory-bandwidth-limited), enabling `-XX:+UseNUMA` typically reduces wall-clock time by 15–35% by eliminating cross-socket memory traffic.

**How to set it in Spark:**
```bash
spark.executor.extraJavaOptions=-XX:+UseNUMA -XX:+UseG1GC -XX:G1HeapRegionSize=32m
```

### 6.4 `numactl` — The Essential Tool

`numactl` is the command-line interface to Linux's `set_mempolicy` and `sched_setaffinity` syscalls. Every data engineer running workloads on multi-socket servers should know it.

```bash
# Show topology
numactl --hardware

# Show current process's NUMA policy
numactl --show

# Run a process bound to NUMA node 0 (CPUs AND memory)
numactl --cpunodebind=0 --membind=0 python3 my_script.py

# Run with memory interleaved across all nodes
# (best for benchmarking to avoid variance from NUMA effects)
numactl --interleave=all python3 my_benchmark.py

# Run with local allocation (always allocate from thread's current node)
numactl --localalloc python3 my_script.py

# Check per-node memory stats for a running process
# (shows how many pages are local vs remote)
numastat -p <pid>

# Check system-wide NUMA hit/miss statistics
numastat
```

### 6.5 `numastat` — Detect NUMA Problems

`numastat` (with no arguments) shows system-wide NUMA statistics:

```
$ numastat
                          node0           node1
numa_hit            8,234,891,044   7,891,234,567   ← allocations that went to preferred node
numa_miss           1,234,567,890     987,654,321   ← allocations that went to wrong node
numa_foreign          987,654,321   1,234,567,890   ← allocations this node "stole" from other
interleave_hit                  0               0
local_node          8,234,891,044   7,891,234,567
other_node          1,234,567,890     987,654,321   ← cross-socket allocations

# NUMA miss rate = numa_miss / (numa_hit + numa_miss)
# If > 10%: NUMA problem. If > 30%: severe NUMA problem.
```

`numastat -p <pid>` shows per-process breakdown:
```
$ numastat -p 12345
Per-node process memory usage (in MBs) for PID 12345 (java)
                           Node 0          Node 1           Total
                  --------------- --------------- ---------------
Huge                         0.00            0.00            0.00
Heap                     12345.23         8234.12        20579.35  ← heap spread across both nodes
Stack                        8.00            8.00           16.00
Private                  15678.00         3456.00        19134.00  ← most private memory on node 0

# If a Spark executor shows 80% on one node and 20% on the other,
# the 20% on the other node is paying remote latency for all accesses.
```

---

## 7. Mental Models

### Mental Model 1 — NUMA as Distributed Shared Memory

Think of a 2-socket NUMA system not as one computer but as two computers sharing a slow network:

```
┌──────────────────┐    UPI (~50 GB/s)    ┌──────────────────┐
│  "Computer 0"    │ ←──────────────────► │  "Computer 1"    │
│  32 cores        │                      │  32 cores        │
│  512 GB RAM      │                      │  512 GB RAM      │
│  200 GB/s BW     │                      │  200 GB/s BW     │
└──────────────────┘                      └──────────────────┘

If Computer 0 needs data from Computer 1's RAM:
  Effective bandwidth: 50 GB/s (the UPI, not 200 GB/s)
  Latency: 1.7× higher

This is exactly NUMA — except the "network" is a PCB trace (UPI)
instead of a cable, and the latency is nanoseconds instead of
microseconds. But the bandwidth constraint is real and has the
same architectural implication: keep data near the code that processes it.

The NUMA-aware design principle mirrors microservices:
  "Data locality matters — move computation to data,
   or ensure data is collocated with the computation."
```

### Mental Model 2 — Spark Executor as a NUMA Domain

A well-configured Spark deployment on a 2-socket server treats each NUMA node as a separate executor:

```
❌ NUMA-unaware Spark (common default):
   1 executor, 64 cores, 1 TB heap
   → OS spreads heap across both NUMA nodes
   → Half of all memory accesses cross the UPI
   → Shuffle and sort: 30–50% slower than NUMA-optimal

✅ NUMA-aware Spark:
   2 executors, 32 cores each, 512 GB heap each
   executor 0: numactl --cpunodebind=0 --membind=0
   executor 1: numactl --cpunodebind=1 --membind=1
   → Each executor's heap is entirely on its local NUMA node
   → All memory accesses are local (70 ns, 200 GB/s)
   → Shuffle between executors is cross-NUMA, but Spark shuffle
     already buffers data — the cross-UPI transfer is amortized
     over large blocks, not per-record

The right unit of NUMA locality is the executor, not the task.
One executor per NUMA node, with all cores and memory bound to that node.
```

### Mental Model 3 — NUMA and the Bandwidth Budget

Each NUMA node has a bandwidth budget: its local DRAM's maximum throughput. Any memory traffic that crosses the UPI to a remote node does not come from the local budget — it comes from the UPI budget, which is 4× smaller.

```
Node 0 local budget:  200 GB/s (8 DDR5-4800 channels)
UPI budget:            50 GB/s (link saturation)

If a Spark executor on node 0 does:
  50% local accesses (data allocated on node 0): uses 100 GB/s of node 0 budget
  50% remote accesses (data on node 1):          uses 50 GB/s of UPI budget (saturated!)

Total effective throughput: 100 + 50 = 150 GB/s
vs NUMA-optimal (100% local): 200 GB/s

NUMA miss ratio of 50% = 25% throughput reduction.

For a shuffle that writes and reads 100 GB:
  NUMA-unaware: 100 GB / 150 GB/s = 667 ms
  NUMA-aware:   100 GB / 200 GB/s = 500 ms
  → NUMA-aware shuffle is 25% faster, just from memory placement.
```

---

## 8. Failure Scenarios

### Failure 1 — JVM Heap Allocated on Wrong Node

**Symptom:** A Spark executor on a 2-socket server shows good CPU utilization (70%+) but suspiciously low throughput on memory-bound stages (sort, shuffle write). `perf stat` shows IPC = 0.6 despite no compute bottleneck. `numastat -p <pid>` shows 80% of heap on node 0 but 50% of executor threads on node 1.

**Root cause:** Spark's executor JVM started with its main thread scheduled on node 0. The JVM allocated its heap with first-touch on node 0. Executor task threads are spread across both nodes by the OS scheduler. Threads on node 1 access heap objects on node 0's DRAM, paying ~150 ns instead of ~70 ns per LLC miss.

**How to verify:**
```bash
# Check where the executor's heap pages are
numastat -p $(pgrep -f "SparkSubmit")

# Check which CPUs the executor threads are using
taskset -p $(pgrep -f "SparkSubmit")  # shows CPU affinity mask
cat /proc/$(pgrep -f "SparkSubmit")/status | grep Cpus_allowed_list
```

**Expected bad output:**
```
Heap    node0: 45,234 MB   node1: 2,123 MB  ← 95% on node 0
Threads using node 1 CPUs: 16 (of 32)       ← half the threads on wrong node
```

---

### Failure 2 — interleave=all Kills Sequential Throughput

**Symptom:** After adding `numactl --interleave=all` to Spark launcher (a common "NUMA fix" found online), shuffle write throughput actually *decreases* compared to before. `perf stat` shows low IPC and high LLC misses.

**Root cause:** `--interleave=all` allocates memory pages in round-robin across NUMA nodes. For each sequential 4 KB page, the allocator alternates: page 0 → node 0, page 1 → node 1, page 2 → node 0, etc. Sequential array accesses now alternate between local and remote pages — every other 4 KB (512 float64 values) crosses the UPI. Interleaving is designed for uniform random access patterns (where distributing load across NUMA nodes helps) — not for sequential workloads (where it halves effective bandwidth).

For Spark's sequential Parquet reads and shuffle writes, `--localalloc` or `--membind=<node>` is correct. `--interleave=all` is only useful for benchmarking or for workloads with truly uniform random access patterns.

**Fix:** Replace `--interleave=all` with per-executor NUMA binding (one executor per NUMA node with `--cpunodebind` and `--membind`).

---

### Failure 3 — AMD EPYC "Fake NUMA" in Cloud Instances

**Symptom:** `numactl --hardware` on an `x2idn.32xlarge` (AMD EPYC Milan, 128 cores) shows 4 or 8 NUMA nodes even though the instance physically has a single socket. The NUMA distances show values of 10, 11, 12 — all very close, not the 21 of a true 2-socket system. Configuring the executor to bind to NUMA node 0 (32 cores) when 128 cores are available wastes 75% of the instance.

**Root cause:** AMD's chiplet architecture (CCD: Core Complex Die) creates NUMA-like topology even within a single socket. Each CCD has its own L3 cache and its own memory channel connections through the I/O die. AMD's BIOS exposes this as multiple NUMA nodes (`NPS4` mode = 4 NUMA nodes per socket) for performance tuning, but the inter-CCD latency (distance 11–12) is much less than true inter-socket latency (distance 21).

**How to distinguish:**
```bash
numactl --hardware
# True 2-socket NUMA: distances will show 10 (local) and 21+ (remote)
# AMD intra-socket NPS: distances will show 10 (local) and 11-12 (remote)

# Also check:
cat /sys/devices/system/node/node0/distance

# And physical socket count:
lscpu | grep "Socket(s)"
```

**Impact:** For AMD EPYC NPS4, the cross-CCD latency is only 10–20% higher than local (not 70–130% as in true 2-socket). A single Spark executor using all cores is less harmful on NPS4 than on a true 2-socket system. However, binding executors to per-CCD groupings still helps for workloads with > 32 MB working sets (each CCD has 32 MB L3).

---

### Failure 4 — Kubernetes Ignores NUMA

**Symptom:** Spark on Kubernetes shows inconsistent executor performance — some executors are 2× faster than others. Pod logs show identical configurations. `perf stat` inside fast pods shows low LLC misses; slow pods show high LLC misses.

**Root cause:** Kubernetes schedules pods without NUMA awareness by default. A pod requesting 32 CPUs might get cores 0–15 (all on node 0) or cores 16–31 (all on node 1) or mixed (e.g., cores 0–15 + 32–47 = spanning both nodes). Memory for the pod is allocated by the kubelet without NUMA binding.

Kubernetes's `TopologyManager` with `single-numa-node` policy fixes this — but it is disabled by default and requires `--topology-manager-policy=single-numa-node` in kubelet config. When enabled, the kubelet ensures that all CPU cores and memory for a pod come from the same NUMA node. The trade-off: pods requesting more than one NUMA node's resources cannot be scheduled, reducing cluster utilization.

---

## 9. Recovery Procedures

### Procedure 1 — One Executor Per NUMA Node

The most reliable NUMA fix: split one large Spark executor into N executors (one per NUMA node), each bound to its node.

```bash
# Before (NUMA-unaware):
spark-submit \
  --executor-cores 64 \
  --executor-memory 1000g \
  ...

# After (NUMA-aware, 2-socket server):
# Launch two separate executors via spark.executor configuration
# Note: in standalone/YARN, use --num-executors and smaller sizing

spark-submit \
  --num-executors 2 \
  --executor-cores 32 \
  --executor-memory 500g \
  --conf "spark.executor.extraJavaOptions=-XX:+UseNUMA -XX:+UseG1GC" \
  --conf "spark.yarn.executor.memoryOverhead=10240" \
  ...

# For bare-metal or VM (not Kubernetes), add numactl wrapper:
# In $SPARK_HOME/conf/spark-env.sh:
SPARK_EXECUTOR_OPTS="-XX:+UseNUMA -XX:+UseG1GC"
```

### Procedure 2 — Pin a Process with `numactl`

```bash
# Pin to NUMA node 0 (both CPUs and memory)
numactl --cpunodebind=0 --membind=0 \
    java -XX:+UseNUMA -Xmx400g -jar my-spark-executor.jar

# Pin to NUMA node 1
numactl --cpunodebind=1 --membind=1 \
    java -XX:+UseNUMA -Xmx400g -jar my-spark-executor.jar

# For Python workloads (no JVM):
numactl --cpunodebind=0 --membind=0 python3 my_pipeline.py

# Verify the binding took effect:
numactl --show   # shows current process's NUMA policy
```

### Procedure 3 — `numastat` Monitoring Loop

```bash
#!/bin/bash
# monitor_numa.sh — Watch NUMA stats for a running process
# Usage: bash monitor_numa.sh <pid>

PID=$1
echo "Monitoring NUMA stats for PID $PID every 5 seconds..."
echo "Column format: heap_node0_MB heap_node1_MB miss_rate%"

while kill -0 $PID 2>/dev/null; do
    STATS=$(numastat -p $PID 2>/dev/null | grep "Heap")
    if [ -n "$STATS" ]; then
        NODE0=$(echo $STATS | awk '{print $2}')
        NODE1=$(echo $STATS | awk '{print $3}')
        TOTAL=$(echo $STATS | awk '{print $4}')
        echo "$(date +%H:%M:%S)  Heap: node0=${NODE0}MB node1=${NODE1}MB total=${TOTAL}MB"
    fi
    
    # System-wide miss rate
    MISS=$(numastat 2>/dev/null | awk '/numa_miss/{print $2+$3}')
    HIT=$(numastat 2>/dev/null | awk '/numa_hit/{print $2+$3}')
    if [ -n "$MISS" ] && [ -n "$HIT" ]; then
        RATE=$(echo "scale=1; $MISS * 100 / ($HIT + $MISS)" | bc 2>/dev/null)
        echo "         NUMA miss rate: ${RATE}%"
    fi
    sleep 5
done
```

### Procedure 4 — Enable JVM NUMA in Spark

Add these to `spark-defaults.conf`:
```properties
# Enable JVM NUMA-awareness
spark.executor.extraJavaOptions=-XX:+UseNUMA -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:MaxGCPauseMillis=200

# Match executor count to NUMA node count
# (on a 2-socket server with 64 vCPUs and 1 TB RAM):
spark.executor.instances=2
spark.executor.cores=32
spark.executor.memory=480g
spark.executor.memoryOverhead=20g
```

---

## 10. Trade-offs

### One Executor per NUMA Node vs One Executor for All

| One executor per NUMA node | One executor for all CPUs |
|---|---|
| Memory accesses are local (~70 ns, 200 GB/s) | Some memory accesses cross UPI (~150 ns, 50 GB/s) |
| JVM GC operates on smaller heap → shorter GC pauses | One large heap → potentially long GC pauses |
| Shuffle between executors → serialization overhead | No inter-executor shuffle serialization needed |
| Task parallelism limited to one NUMA node's cores | Full CPU utilization without partition overhead |
| **15–35% faster for memory-bandwidth-limited workloads** | Simpler configuration; acceptable for compute-bound workloads |

**Decision rule:** Use one executor per NUMA node if the workload is memory-bandwidth-limited (IPC < 1.0 in `perf stat`, high LLC misses). Use one large executor if compute-bound (IPC > 2.5, low LLC misses), or if Spark's inter-executor shuffle overhead exceeds the NUMA savings.

### NUMA Binding Strictness

| Strict binding (`--membind`) | Soft binding (`--preferred`) | Interleave (`--interleave`) |
|---|---|---|
| No remote memory access | Remote access if local is full | Even spread across nodes |
| OOM kill if local node runs out of memory | Graceful fallback to remote | Good for uniform random access |
| Best performance when working set fits on one node | Best for slightly oversized working sets | Best for benchmarking; bad for sequential access |
| Can cause OOM on memory-heavy executors | Slight NUMA miss rate when local is full | 50% remote traffic for sequential workloads |

---

## 11. Comparisons

### NUMA-Aware vs NUMA-Unaware Spark Shuffle

```python
"""
Conceptual benchmark: what NUMA-awareness gives you on a shuffle.

This script simulates the bandwidth difference between:
- NUMA-local memory access: ~200 GB/s
- NUMA-remote (half remote): ~(200+50)/2 = 125 GB/s effective
- NUMA-remote (all remote): ~50 GB/s (UPI bottleneck)

Run on a NUMA system for real measurements. Without NUMA hardware,
this shows the bandwidth model.
"""
import numpy as np
import time

def simulate_bandwidth(label: str, bandwidth_gbps: float, data_gb: float) -> float:
    """Calculate time to process data_gb at bandwidth_gbps."""
    time_s = data_gb / bandwidth_gbps
    print(f"  {label:<40}: {time_s*1000:6.0f} ms  ({bandwidth_gbps:.0f} GB/s)")
    return time_s

print("Spark sort-merge join: 100 GB shuffle data, two-socket server")
print("=" * 70)
print()
print("Phase 1: Shuffle write (100 GB to local disk)")
t_local   = simulate_bandwidth("NUMA-aware (memory → local DRAM)", 200, 100)
t_mixed   = simulate_bandwidth("NUMA-unaware (50% remote via UPI)", 125, 100)
t_remote  = simulate_bandwidth("Worst case (all remote via UPI)",    50, 100)

print()
print("Phase 2: Shuffle read + sort (100 GB from disk, sort in memory)")
t_sort_local  = simulate_bandwidth("NUMA-aware sort buffer (200 GB/s)", 200, 100)
t_sort_remote = simulate_bandwidth("NUMA-unaware sort buffer (125 GB/s)", 125, 100)

print()
print(f"Total NUMA-aware:     {(t_local + t_sort_local)*1000:.0f} ms")
print(f"Total NUMA-unaware:   {(t_mixed + t_sort_remote)*1000:.0f} ms")
print(f"NUMA penalty:         {((t_mixed + t_sort_remote)/(t_local + t_sort_local) - 1)*100:.0f}%")
print()
print("Note: Actual Spark shuffle also involves disk I/O and network.")
print("For in-memory shuffles (spark.shuffle.file.buffer=0), the memory")
print("bandwidth dominates and NUMA penalty is ~35%.")
```

---

### Intel UPI vs AMD Infinity Fabric: Cross-NUMA Performance

| Metric | Intel UPI (Ice Lake) | AMD Infinity Fabric (Milan/Genoa) |
|---|---|---|
| Link bandwidth (per direction) | ~41 GB/s (UPI 2.0) | ~200+ GB/s (intra-socket via IOD) |
| Cross-NUMA latency multiplier | 1.7–2.3× | 1.1–1.2× (intra-socket NPS) / 1.5–1.8× (true 2-socket) |
| NUMA nodes per socket | 1 (or sub-NUMA via NPS mode) | 4–8 (NPS4 or NPS8 for 64-core Milan) |
| Remote bandwidth bottleneck | UPI is the bottleneck (41 GB/s) | Infinity Fabric is wider (less bottleneck) |
| Best practice | One executor per socket (strict) | One executor per CCD group (NPS4 = 4 executors) |

AMD's Infinity Fabric significantly reduces the cross-NUMA penalty compared to Intel UPI. On AWS instances running AMD EPYC (c7a, m7a, r7a), the NUMA problem is less severe than on equivalent Intel instances. However, NPS4 mode still creates 4 NUMA domains per socket, and the optimal Spark configuration still follows one executor per NUMA node.

---

## 12. Production Examples

### Kafka Broker NUMA Tuning

A Kafka broker on a 2-socket server handles two distinct I/O paths:
1. **Producer writes:** network → OS page cache → disk (async). The page cache is in DRAM.
2. **Consumer reads:** disk → OS page cache → network (if cached in page cache, zero-copy sendfile).

The page cache is allocated by the OS. Without NUMA control, Kafka's page cache pages may span both NUMA nodes. Network interrupts from the NIC (which is attached to one PCIe root complex, connected to one socket) may be handled by cores on the *other* socket, causing cross-NUMA memory traffic for every packet's page cache lookup.

**NUMA-aware Kafka setup:**
```bash
# Step 1: Pin network interrupts to socket 0 (where the NIC is attached)
# Check which CPUs are handling NIC interrupts:
cat /proc/interrupts | grep eth0
# Pin those IRQs to socket 0 cores:
echo "0-31" > /proc/irq/<IRQ_NUM>/smp_affinity_list

# Step 2: Start Kafka with socket 0 affinity
numactl --cpunodebind=0 --membind=0 \
    kafka-server-start.sh /etc/kafka/server.properties \
    -XX:+UseNUMA -XX:+UseG1GC -Xmx24g -Xms24g

# Step 3: Verify
numastat -p $(pgrep -f kafka.Kafka)
# Check: nearly all heap pages should be on node 0

# Measured improvement (typical 2-socket server, Kafka throughput):
# Before NUMA pinning: ~600 MB/s producer throughput
# After NUMA pinning:  ~820 MB/s producer throughput (~37% improvement)
```

**Why this works:** Both network packet processing (interrupt → softirq → network stack → page cache write) and consumer read path (page cache lookup → sendfile → NIC) now happen on the same socket. No cross-UPI traffic for the hot path.

---

### Flink TaskManager NUMA Placement

Apache Flink's TaskManagers (equivalent to Spark executors) have the same NUMA problem as Spark. Flink's configuration:

```yaml
# flink-conf.yaml for a 2-socket server
taskmanager.numberOfTaskSlots: 32       # One per NUMA node's cores
taskmanager.memory.process.size: 500g   # Half the server's DRAM per TM
# Launch two TaskManagers, one per NUMA node:
# TM0: numactl --cpunodebind=0 --membind=0 taskmanager.sh
# TM1: numactl --cpunodebind=1 --membind=1 taskmanager.sh

# Enable JVM NUMA support
env.java.opts.taskmanager: >
  -XX:+UseNUMA
  -XX:+UseG1GC
  -XX:G1HeapRegionSize=32m
```

Flink streaming jobs (continuous window aggregations, stateful operators) benefit especially from NUMA-awareness because they maintain large in-memory state stores (RocksDB or Flink's heap state backend). A stateful operator that spans NUMA nodes pays remote memory latency on every state lookup.

---

### NUMA and Parquet Reader Thread Count

The PyArrow Parquet reader is multi-threaded (controlled by `use_threads=True`, default since PyArrow 3.0). On a NUMA system, PyArrow spawns threads that may span NUMA nodes. For large Parquet files:

```python
import pyarrow.parquet as pq
import time

# Default: use_threads=True — spawns threads across all cores
t0 = time.perf_counter()
df = pq.read_table("large_file.parquet")
t_multi = time.perf_counter() - t0

# NUMA-aware: limit threads to one NUMA node's core count
# (if running on a 2-socket server, use 32 threads instead of 64)
t0 = time.perf_counter()
df = pq.read_table("large_file.parquet", use_threads=True)
# Before calling, set affinity:
# os.sched_setaffinity(0, set(range(32)))  # cores 0-31 (node 0 only)
t_numa = time.perf_counter() - t0

# On a 2-socket server without NUMA pinning:
# 64-thread read on 100 GB file: ~45 seconds
# 32-thread NUMA-bound read:     ~32 seconds (29% faster)
# Reason: 32 threads all access local DRAM (200 GB/s effective)
#         64 threads access mixed local/remote (125 GB/s effective)
```

---

## 13. Code

### `numa_detector.py` — Detect and Report NUMA Topology

```python
"""
numa_detector.py

Detects NUMA topology on the current machine and reports:
- Number of NUMA nodes
- CPU cores per node
- Memory per node
- Distance matrix
- Current process's NUMA affinity
- Whether the current process is NUMA-bound

Provides Spark configuration recommendations based on topology.

Run: python3 numa_detector.py
Dependencies: None (reads /sys and /proc directly)
"""
import os
import re
import subprocess
import platform
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class NumaNode:
    node_id: int
    cpus: list[int] = field(default_factory=list)
    memory_mb: int = 0
    free_mb: int = 0


@dataclass
class NumaTopology:
    nodes: list[NumaNode] = field(default_factory=list)
    distances: list[list[int]] = field(default_factory=list)  # distances[i][j]
    is_numa_system: bool = False
    max_remote_distance: int = 10  # 10 = local baseline

    @property
    def n_nodes(self) -> int:
        return len(self.nodes)

    @property
    def total_memory_gb(self) -> float:
        return sum(n.memory_mb for n in self.nodes) / 1024

    @property
    def is_true_multi_socket(self) -> bool:
        """True if remote distance > 15 (Intel 2-socket = 21, AMD NPS4 = 11-12)."""
        return self.max_remote_distance > 15

    @property
    def remote_latency_multiplier(self) -> float:
        """Approximate latency multiplier for remote access."""
        return self.max_remote_distance / 10.0


def detect_numa_topology() -> NumaTopology:
    """Detect NUMA topology by reading /sys/devices/system/node/."""
    topo = NumaTopology()

    node_dirs = sorted(Path('/sys/devices/system/node/').glob('node[0-9]*'))
    if not node_dirs:
        print("No NUMA nodes found (single-node or non-Linux system)")
        return topo

    topo.is_numa_system = len(node_dirs) > 1

    for node_dir in node_dirs:
        node_id = int(node_dir.name.replace('node', ''))
        node = NumaNode(node_id=node_id)

        # Read CPU list
        cpu_list_path = node_dir / 'cpulist'
        if cpu_list_path.exists():
            cpu_list_str = cpu_list_path.read_text().strip()
            node.cpus = _parse_cpu_list(cpu_list_str)

        # Read memory info
        meminfo_path = node_dir / 'meminfo'
        if meminfo_path.exists():
            meminfo = meminfo_path.read_text()
            m = re.search(r'MemTotal:\s+(\d+) kB', meminfo)
            if m:
                node.memory_mb = int(m.group(1)) // 1024
            m = re.search(r'MemFree:\s+(\d+) kB', meminfo)
            if m:
                node.free_mb = int(m.group(1)) // 1024

        topo.nodes.append(node)

    # Read distance matrix
    n = len(topo.nodes)
    topo.distances = [[10] * n for _ in range(n)]
    for node_dir in node_dirs:
        node_id = int(node_dir.name.replace('node', ''))
        distance_path = node_dir / 'distance'
        if distance_path.exists():
            distances = [int(d) for d in distance_path.read_text().split()]
            topo.distances[node_id] = distances

    # Find max remote distance
    for i in range(n):
        for j in range(n):
            if i != j:
                topo.max_remote_distance = max(topo.max_remote_distance,
                                               topo.distances[i][j])
    return topo


def _parse_cpu_list(cpu_list_str: str) -> list[int]:
    """Parse '0-7,16-23' into [0,1,2,3,4,5,6,7,16,17,18,19,20,21,22,23]."""
    cpus = []
    for part in cpu_list_str.split(','):
        part = part.strip()
        if '-' in part:
            start, end = part.split('-')
            cpus.extend(range(int(start), int(end) + 1))
        elif part.isdigit():
            cpus.append(int(part))
    return cpus


def get_current_numa_affinity() -> Optional[dict]:
    """Get the current process's CPU and memory affinity via numactl."""
    try:
        result = subprocess.run(['numactl', '--show'],
                                capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            return None
        info = {}
        for line in result.stdout.splitlines():
            if ':' in line:
                key, val = line.split(':', 1)
                info[key.strip()] = val.strip()
        return info
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


def get_numastat() -> Optional[dict]:
    """Get system-wide NUMA statistics."""
    try:
        result = subprocess.run(['numastat'],
                                capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            return None
        stats = {}
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 3 and parts[0] not in ('', 'node'):
                try:
                    stats[parts[0]] = [int(float(p)) for p in parts[1:]]
                except ValueError:
                    pass
        return stats
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


def print_report(topo: NumaTopology):
    """Print NUMA topology report and Spark configuration recommendations."""
    print("=" * 65)
    print("NUMA TOPOLOGY REPORT")
    print("=" * 65)
    print(f"\nSystem: {platform.node()} ({platform.processor() or 'unknown CPU'})")
    print(f"NUMA nodes: {topo.n_nodes}")
    print(f"Total memory: {topo.total_memory_gb:.0f} GB")
    print(f"Is NUMA system: {'YES' if topo.is_numa_system else 'NO (single node)'}")

    if topo.is_numa_system:
        print(f"Remote latency multiplier: {topo.remote_latency_multiplier:.1f}×")
        print(f"System type: {'True multi-socket (UPI)' if topo.is_true_multi_socket else 'Intra-socket NUMA (Infinity Fabric / chiplet)'}")

    print("\nPer-node topology:")
    for node in topo.nodes:
        cpu_range = (f"cores {min(node.cpus)}–{max(node.cpus)}"
                     if node.cpus else "none")
        print(f"  Node {node.node_id}: {len(node.cpus):3d} CPUs ({cpu_range}), "
              f"{node.memory_mb/1024:.0f} GB RAM "
              f"({node.free_mb/1024:.0f} GB free)")

    if topo.n_nodes > 1:
        print("\nDistance matrix (10=local baseline):")
        header = "     " + "".join(f"  N{j}" for j in range(topo.n_nodes))
        print(header)
        for i in range(topo.n_nodes):
            row = f"  N{i}:" + "".join(f"  {topo.distances[i][j]:2d}"
                                       for j in range(topo.n_nodes))
            print(row)

    # NUMA affinity check
    print("\nCurrent process NUMA affinity:")
    affinity = get_current_numa_affinity()
    if affinity:
        for k, v in affinity.items():
            print(f"  {k}: {v}")
        if 'policy' in affinity and affinity['policy'] == 'default':
            print("  ⚠️  Policy is 'default' (first-touch) — NUMA-unaware")
    else:
        print("  (numactl not available)")

    # NUMA statistics
    print("\nSystem NUMA statistics:")
    stats = get_numastat()
    if stats:
        hit_total  = sum(stats.get('numa_hit', [0]))
        miss_total = sum(stats.get('numa_miss', [0]))
        if hit_total + miss_total > 0:
            miss_rate = miss_total / (hit_total + miss_total) * 100
            status = ("✅ healthy" if miss_rate < 5 else
                      "⚠️  moderate" if miss_rate < 20 else
                      "🔴 high NUMA miss rate")
            print(f"  NUMA miss rate: {miss_rate:.1f}% ({status})")
    else:
        print("  (numastat not available)")

    # Spark configuration recommendations
    if topo.is_numa_system:
        print("\n" + "=" * 65)
        print("SPARK CONFIGURATION RECOMMENDATIONS")
        print("=" * 65)
        n = topo.n_nodes
        cores_per_node = min(len(nd.cpus) for nd in topo.nodes) if topo.nodes else 0
        mem_per_node_gb = min(nd.memory_mb for nd in topo.nodes) / 1024 if topo.nodes else 0
        executor_mem_gb = int(mem_per_node_gb * 0.85)  # leave 15% for OS

        print(f"""
  # {n}-NUMA-node system detected
  # Recommended: {n} executors, one per NUMA node

  spark.executor.instances={n}
  spark.executor.cores={cores_per_node}
  spark.executor.memory={executor_mem_gb}g
  spark.executor.extraJavaOptions=-XX:+UseNUMA -XX:+UseG1GC -XX:G1HeapRegionSize=32m

  # Launch commands (adjust spark-submit as needed):""")

        for node in topo.nodes:
            if node.cpus:
                cpu_range = f"{min(node.cpus)}-{max(node.cpus)}"
                print(f"  # Node {node.node_id}: numactl --cpunodebind={node.node_id} "
                      f"--membind={node.node_id} spark-executor ...")
    else:
        print("\nSingle NUMA node — no NUMA configuration changes needed.")


if __name__ == '__main__':
    topo = detect_numa_topology()
    print_report(topo)
```

### `numa_bench.py` — Measure Local vs Remote Memory Access Latency

```python
"""
numa_bench.py

Measures the actual NUMA memory access latency difference on your machine.
Uses pointer-chasing (same technique as M01 Lab 1) to defeat the prefetcher.

For accurate results, run on a machine with ≥ 2 NUMA nodes.
On a single-NUMA machine, both "local" and "remote" will show similar latency.

Run: python3 numa_bench.py
Note: For more accurate results, run as:
      numactl --cpunodebind=0 --membind=0 python3 numa_bench.py (local)
      numactl --cpunodebind=0 --membind=1 python3 numa_bench.py (remote)
"""
import numpy as np
import time
import subprocess
import os
from pathlib import Path


def build_chase_array(n_elements: int, seed: int = 42) -> np.ndarray:
    """
    Build a pointer-chasing array: chase[i] = next index to visit.
    Creates one cycle through all elements (random permutation).
    """
    rng = np.random.default_rng(seed)
    perm = rng.permutation(n_elements).astype(np.int64)
    chase = np.empty(n_elements, dtype=np.int64)
    for i in range(n_elements - 1):
        chase[perm[i]] = perm[i + 1]
    chase[perm[-1]] = perm[0]
    return chase


def measure_pointer_chase_latency(chase: np.ndarray,
                                   n_accesses: int = 5_000_000) -> float:
    """
    Measure pointer-chasing latency in nanoseconds per access.
    Each access depends on the previous result — prefetcher cannot help.
    """
    # Warmup pass
    pos = 0
    n_warmup = min(100_000, len(chase))
    for _ in range(n_warmup):
        pos = chase[pos]

    # Timed pass
    pos = int(pos)  # ensure Python int, not numpy int64 (slightly faster loop)
    chase_list = chase.tolist()  # convert to Python list for faster element access
    t0 = time.perf_counter()
    for _ in range(n_accesses):
        pos = chase_list[pos]
    elapsed = time.perf_counter() - t0

    _ = pos  # prevent dead-code elimination
    latency_ns = elapsed * 1e9 / n_accesses
    return latency_ns


def get_current_numa_node() -> int:
    """Return the NUMA node the current process is bound to (or -1 if unknown)."""
    try:
        result = subprocess.run(['numactl', '--show'],
                                capture_output=True, text=True, timeout=3)
        for line in result.stdout.splitlines():
            if 'membind:' in line:
                nodes = line.split(':')[1].strip()
                return int(nodes.split()[0])
    except Exception:
        pass
    return -1


def run_benchmark():
    print("=" * 65)
    print("NUMA MEMORY ACCESS LATENCY BENCHMARK")
    print("=" * 65)

    # Check available NUMA nodes
    numa_nodes = sorted(Path('/sys/devices/system/node/').glob('node[0-9]*'))
    print(f"\nDetected NUMA nodes: {len(numa_nodes)}")

    if len(numa_nodes) < 2:
        print("Only 1 NUMA node detected — running single-node latency test.")
        print("For NUMA comparison, run on a multi-socket server.")
        print("(AWS c6i.16xlarge and larger are 2-socket systems)")

    current_node = get_current_numa_node()
    if current_node >= 0:
        print(f"Current process bound to: NUMA node {current_node}")
    else:
        print("Current process: NUMA binding unknown (numactl not available)")

    # Array sizes to test
    sizes = {
        "L2 (fits in L2)":  256 * 1024 // 8,          # 256 KB / 8 bytes = 32K elements
        "L3 (fits in L3)":  16 * 1024 * 1024 // 8,     # 16 MB / 8 bytes = 2M elements
        "DRAM (16 MB)":     16 * 1024 * 1024 // 8,     # same but cold
        "DRAM (256 MB)":    256 * 1024 * 1024 // 8,    # 256 MB array
    }

    print(f"\n{'Array Size':>20}  {'Latency (ns)':>14}  {'Throughput (M acc/s)':>22}")
    print("-" * 62)

    for name, n_elem in sizes.items():
        chase = build_chase_array(n_elem)
        latency_ns = measure_pointer_chase_latency(chase, n_accesses=2_000_000)
        throughput = 1000 / latency_ns  # million accesses per second
        print(f"{name:>20}  {latency_ns:>14.1f}  {throughput:>22.2f}")

    print("\nInterpretation:")
    print("  < 5 ns:   L1/L2 cache (registers)")
    print("  5–15 ns:  L2 cache")
    print("  15–50 ns: L3 cache")
    print("  50–80 ns: DRAM (local NUMA node)")
    print("  80–160 ns: DRAM (remote NUMA node, cross-UPI)")
    print()
    print("To measure remote NUMA latency explicitly:")
    print("  Run with:  numactl --cpunodebind=0 --membind=1 python3 numa_bench.py")
    print("  vs local:  numactl --cpunodebind=0 --membind=0 python3 numa_bench.py")
    print("  The difference in DRAM latency is the NUMA penalty for your hardware.")


if __name__ == '__main__':
    run_benchmark()
```

---

## 14. Labs

### Lab 1 — Map the NUMA Topology of Your Instance

**Goal:** Use `numactl`, `numastat`, and `lstopo` to fully characterize the NUMA topology of a Linux machine. Determine whether the instance is a single-socket (no NUMA), a true multi-socket system, or an AMD chiplet-based single-socket with NUMA domains.

```bash
#!/bin/bash
# lab1_numa_map.sh
# Install tools if needed: sudo apt install numactl hwloc

echo "=== 1. Basic NUMA topology ==="
numactl --hardware

echo ""
echo "=== 2. CPU topology ==="
lscpu | grep -E "(Socket|NUMA|Core|Thread|CPU\(s\))"

echo ""
echo "=== 3. NUMA distance matrix ==="
cat /sys/devices/system/node/node*/distance | head -20
# Each line = distances from node N to [node0, node1, ...]

echo ""
echo "=== 4. Current process NUMA policy ==="
numactl --show

echo ""
echo "=== 5. System NUMA statistics ==="
numastat

echo ""
echo "=== 6. Topology visualization (if lstopo available) ==="
which lstopo && lstopo --of ascii 2>/dev/null || echo "lstopo not found (install hwloc)"

echo ""
echo "=== Analysis ==="
echo "Questions:"
echo "1. How many NUMA nodes does this machine have?"
echo "2. What is the NUMA distance from node 0 to node 1?"
echo "   (10=local, 21=2-socket Intel, 11-12=AMD intra-socket NPS)"
echo "3. Does the current process have a specific NUMA policy?"
echo "4. What is the current system-wide NUMA miss rate?"
echo "5. Is this a true multi-socket system or AMD intra-socket NUMA?"
```

---

### Lab 2 — Measure the NUMA Memory Penalty

**Goal:** Directly measure the latency penalty of accessing remote NUMA memory vs local NUMA memory. Requires a 2-node NUMA system (e.g., AWS `r6i.16xlarge`, `c6i.16xlarge`, or any bare-metal server with 2 sockets). If only a single-NUMA machine is available, run the benchmark with `numastat` to observe the miss rate.

```bash
#!/bin/bash
# lab2_numa_penalty.sh
# Requires: numactl, Python 3, numpy
# Best run on a 2-node NUMA machine

echo "=== NUMA Latency Comparison ==="
echo ""

if ! numactl --hardware 2>/dev/null | grep -q "node 1"; then
    echo "Only 1 NUMA node detected. Running single-node benchmark."
    python3 numa_bench.py
    exit 0
fi

echo "--- Local NUMA access (CPU on node 0, memory on node 0) ---"
numactl --cpunodebind=0 --membind=0 python3 numa_bench.py

echo ""
echo "--- Remote NUMA access (CPU on node 0, memory on node 1) ---"
numactl --cpunodebind=0 --membind=1 python3 numa_bench.py

echo ""
echo "Expected:"
echo "  Local DRAM (256 MB array):  ~70-80 ns per access"
echo "  Remote DRAM (256 MB array): ~120-160 ns per access"
echo "  NUMA penalty: 1.5-2.0×"
echo ""
echo "The difference you measure is the UPI/Infinity Fabric penalty."
echo "This penalty applies to every memory access that crosses NUMA nodes."
echo "For a Spark executor with 50% remote accesses (typical NUMA-unaware):"
echo "  Effective latency = 0.5 × 70 + 0.5 × 140 = 105 ns average (vs 70 ns optimal)"
```

---

### Lab 3 — Fix a NUMA-Unaware Spark Configuration

**Goal:** Given a Spark job configuration and server topology, identify the NUMA problem and write the corrected configuration.

```python
"""
lab3_numa_spark_config.py

Generates NUMA-aware Spark configurations for different server topologies.
Given server topology (from numactl --hardware), produces the optimal
Spark configuration and numactl launch commands.

Run: python3 lab3_numa_spark_config.py
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class ServerTopology:
    name: str
    n_numa_nodes: int
    cores_per_node: int
    memory_gb_per_node: int
    remote_distance: int          # normalized (10=local, 21=Intel 2-socket)
    dram_bandwidth_gbps: float    # per node


# Common AWS instance topologies
TOPOLOGIES = [
    ServerTopology(
        name="r6i.16xlarge (Intel Ice Lake, 2-socket)",
        n_numa_nodes=2,
        cores_per_node=32,
        memory_gb_per_node=256,
        remote_distance=21,
        dram_bandwidth_gbps=200,
    ),
    ServerTopology(
        name="r6i.32xlarge (Intel Ice Lake, 2-socket)",
        n_numa_nodes=2,
        cores_per_node=64,
        memory_gb_per_node=512,
        remote_distance=21,
        dram_bandwidth_gbps=200,
    ),
    ServerTopology(
        name="x2idn.32xlarge (AMD EPYC Milan, NPS4)",
        n_numa_nodes=4,
        cores_per_node=32,
        memory_gb_per_node=512,
        remote_distance=12,  # intra-socket AMD
        dram_bandwidth_gbps=150,
    ),
    ServerTopology(
        name="c6i.8xlarge (Intel Ice Lake, single-socket)",
        n_numa_nodes=1,
        cores_per_node=32,
        memory_gb_per_node=64,
        remote_distance=10,   # no remote node
        dram_bandwidth_gbps=100,
    ),
]


def generate_numa_config(topo: ServerTopology) -> str:
    """
    Generate the optimal NUMA-aware Spark configuration for a given topology.
    """
    if topo.n_numa_nodes == 1:
        return f"""
# {topo.name}
# Single NUMA node — no NUMA binding needed.

spark.executor.instances=1
spark.executor.cores={topo.cores_per_node}
spark.executor.memory={int(topo.memory_gb_per_node * 0.85)}g
spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=32m

# No numactl needed for single-socket machines.
"""

    remote_penalty = topo.remote_distance / 10.0
    executor_mem = int(topo.memory_gb_per_node * 0.85)
    overhead_mem = int(topo.memory_gb_per_node * 0.10)

    remote_bw = topo.dram_bandwidth_gbps / (remote_penalty * 0.75)
    numa_unaware_bw = (topo.dram_bandwidth_gbps * 0.5 +
                       remote_bw * 0.5)
    benefit_pct = (1 - numa_unaware_bw / topo.dram_bandwidth_gbps) * 100

    numactl_cmds = "\n".join(
        f"  # Node {i}: numactl --cpunodebind={i} --membind={i} "
        f"spark-executor-launch-script.sh"
        for i in range(topo.n_numa_nodes)
    )

    return f"""
# {topo.name}
# NUMA nodes: {topo.n_numa_nodes}, remote distance: {topo.remote_distance}
# Remote latency multiplier: {remote_penalty:.1f}×
# Estimated NUMA-aware throughput benefit: ~{benefit_pct:.0f}%

spark.executor.instances={topo.n_numa_nodes}
spark.executor.cores={topo.cores_per_node}
spark.executor.memory={executor_mem}g
spark.executor.memoryOverhead={overhead_mem}g
spark.executor.extraJavaOptions=\\
  -XX:+UseNUMA \\
  -XX:+UseG1GC \\
  -XX:G1HeapRegionSize=32m \\
  -XX:MaxGCPauseMillis=200

# NUMA launch commands (run these as separate processes):
{numactl_cmds}

# WARNING: Set spark.executor.instances={topo.n_numa_nodes} so exactly
# one executor runs per NUMA node. If using YARN, set:
# spark.yarn.executor.nodeLabelExpression for placement control.
"""


def analyze_bad_config(topo: ServerTopology) -> str:
    """Analyze what's wrong with the typical default Spark configuration."""
    total_cores = topo.n_numa_nodes * topo.cores_per_node
    total_mem = topo.n_numa_nodes * topo.memory_gb_per_node

    return f"""
❌ COMMON BAD CONFIGURATION for {topo.name}:

   spark.executor.instances=1
   spark.executor.cores={total_cores}
   spark.executor.memory={int(total_mem * 0.85)}g
   # (no -XX:+UseNUMA, no numactl)

Problems:
  1. One JVM heap spans {total_mem} GB across {topo.n_numa_nodes} NUMA nodes
  2. OS uses first-touch: JVM heap pages land on whichever node
     the JVM main thread starts on (node 0 in most cases)
  3. Executor threads on node(s) {','.join(str(i) for i in range(1, topo.n_numa_nodes))}
     access heap pages on node 0 → cross-UPI traffic
  4. Effective bandwidth: ~{int(topo.dram_bandwidth_gbps * 0.6)} GB/s
     (vs {topo.dram_bandwidth_gbps:.0f} GB/s NUMA-optimal)
  5. GC pauses longer due to cross-NUMA pointer following during GC
"""


if __name__ == '__main__':
    print("=" * 70)
    print("LAB 3: NUMA-Aware Spark Configuration Generator")
    print("=" * 70)

    for topo in TOPOLOGIES:
        print("\n" + "─" * 70)
        print(analyze_bad_config(topo))
        print(generate_numa_config(topo))

    print("=" * 70)
    print("EXERCISE:")
    print("1. Run 'numactl --hardware' on your machine")
    print("2. Identify which topology from above best matches")
    print("3. Modify the generated config for your specific memory and core count")
    print("4. If you have access to a NUMA machine, measure throughput before/after")
```

---

## 15. Summary

NUMA is what happens when you add a second CPU socket: the memory controller is on each socket, and accessing the other socket's memory requires traversing the inter-socket interconnect. The latency penalty is 1.5–2.3×; the bandwidth penalty is up to 4×.

For data engineering systems on multi-socket servers:
- Linux's default first-touch policy allocates heap pages on whichever NUMA node touches them first — which for JVM processes is usually the main thread's socket, causing remote accesses for worker threads on the other socket.
- The fix is one executor per NUMA node, each bound with `numactl --cpunodebind=N --membind=N`, plus `-XX:+UseNUMA` in the JVM to enable NUMA-aware heap allocation.
- AMD EPYC instances (AWS c7a, m7a, r7a) expose intra-socket NUMA via chiplets (NPS4 mode), with smaller penalties (distance 11–12 vs Intel's 21). The same configuration principle applies but the benefit is smaller.
- `numastat` is the primary diagnostic tool: a NUMA miss rate > 10% indicates a NUMA problem; > 30% is severe.
- On Kubernetes, `TopologyManager` with `single-numa-node` policy is the container-level equivalent of `numactl --cpunodebind --membind`.

The performance improvement from NUMA-aware placement for memory-bandwidth-limited workloads (shuffle, sort, large aggregations) is typically 15–35% on Intel 2-socket systems and 5–15% on AMD NPS4 systems.

---

## 16. Interview Q&A

### Q1: What is NUMA, and why does it matter for Spark?

**Answer:**

NUMA stands for Non-Uniform Memory Access. It describes a multi-processor architecture where each CPU socket has its own local DRAM, connected to other sockets via a high-speed interconnect (Intel UPI at ~41 GB/s, AMD Infinity Fabric at higher bandwidth). Memory access latency is not uniform: a core accessing its own socket's DRAM pays ~70 ns, while accessing the other socket's DRAM pays ~120–160 ns via the inter-socket link. Bandwidth is similarly asymmetric: local DRAM provides 200 GB/s per socket on DDR5 systems, while the UPI link provides only ~41–50 GB/s — a 4× bandwidth reduction for cross-socket traffic.

For Spark, NUMA matters because most production Spark jobs are memory-bandwidth-limited: sorting, shuffling, Parquet reads, and large aggregations all scan large datasets sequentially through DRAM. If 50% of those memory accesses go to the remote socket (typical for a NUMA-unaware executor on a 2-socket server), the effective memory bandwidth drops from 200 GB/s to roughly 125 GB/s. For a shuffle that reads and writes 200 GB of data, this 37% bandwidth reduction adds directly to stage runtime.

The default Spark configuration is NUMA-unaware: one large executor uses all cores on the server. The JVM heap is allocated by the OS using first-touch policy, landing mostly on the socket where the JVM main thread runs. Worker threads on the other socket then access those heap pages remotely. The fix is one Spark executor per NUMA node — each executor bound to its node's CPUs and DRAM using `numactl --cpunodebind=N --membind=N` and `-XX:+UseNUMA` in the JVM.

---

### Q2: What is Linux's first-touch NUMA policy, and why does it cause problems for JVM workloads?

**Answer:**

Linux's first-touch policy is the OS's default for deciding which physical NUMA node a virtual memory page gets allocated on. When a process calls `malloc()` or `mmap()`, the OS does not immediately allocate physical RAM — it only creates a virtual address range. Physical pages are allocated on the first write to each virtual address, and they land on the NUMA node of the thread making that first write.

This causes a well-known JVM problem. The JVM starts on its main thread, which the OS typically schedules on socket 0. The JVM then calls `mmap()` to reserve a large virtual address range for the heap (say, 200 GB). During JVM initialization, the main thread writes initial data structures — they land on socket 0's DRAM. But the JVM then creates a thread pool for Spark executor tasks, and the OS scheduler may place some threads on socket 1. Those threads try to allocate Java objects on the heap — the OS writes their pages, and since those threads are on socket 1, the new pages go to socket 1's DRAM.

The result: the heap is split across two NUMA nodes, and the precise split is non-deterministic (depends on OS scheduler decisions). Spark's main thread holds references to objects on socket 0; task threads on socket 1 follow those references and pay remote latency for each access. During garbage collection, the GC must trace all live objects regardless of which node they're on — if the GC thread is on socket 0 and must trace objects on socket 1, every GC root traversal crossing the socket boundary pays 150 ns instead of 70 ns.

The `-XX:+UseNUMA` JVM flag fixes this by making the G1/Parallel GC aware of NUMA: it divides the heap into NUMA-local regions, ensures TLABs (Thread-Local Allocation Buffers) are on the thread's local node, and performs GC in a NUMA-aware manner. Combined with `numactl --membind=N`, this guarantees that all heap pages for a given executor are on the same NUMA node.

---

### Q3: How do you detect whether a NUMA problem is causing performance degradation in a running Spark job?

**Answer:**

There are three tools to use in sequence.

First, `numastat` identifies whether NUMA misses are occurring at all. Running `numastat` without arguments shows system-wide NUMA hit and miss counts. The NUMA miss rate is `numa_miss / (numa_hit + numa_miss)`. A miss rate below 5% is healthy. Above 10% is a problem. Above 30% is severe. Running `numastat -p <pid>` for the Spark executor's JVM process shows how heap pages are distributed across nodes — if 90% of heap is on node 0 but 50% of executor threads run on node 1, you have the classic first-touch imbalance.

Second, `perf stat` with IPC and LLC miss rate can confirm whether the degradation is memory-related. A NUMA-degraded Spark job shows low IPC (0.4–0.7) despite no compute bottleneck, combined with high LLC-load-misses. The LLC misses may be lower than a true capacity miss situation, but the access latency per miss is higher (150 ns remote vs 70 ns local), which `perf stat` does not distinguish directly. You need to combine low IPC with high LLC misses and the `numastat` evidence to confirm NUMA.

Third, `perf stat -e node-load-misses,node-store-misses` (if the CPU supports it) directly counts loads and stores that resulted in remote NUMA node accesses, distinguishing them from regular LLC misses. This is the most direct measurement but requires PMU support for NUMA-specific events (Intel calls them `offcore_response` events).

If all three indicators align — high `numastat` miss rate, low IPC with high LLC misses, and the heap spread across both nodes — the diagnosis is NUMA degradation and the fix is per-NUMA-node executor binding.

---

### Q4: AWS `r6i.32xlarge` has 128 vCPUs and 1 TB of RAM. How should you configure Spark executors on this instance?

**Answer:**

The `r6i.32xlarge` is a 2-socket Intel Ice Lake Xeon server: 64 physical cores per socket (128 total with hyperthreading), 512 GB DRAM per socket (1 TB total), connected by Intel UPI at ~41 GB/s per link direction. The `numactl --hardware` output shows 2 NUMA nodes with distance 21 (2.1× remote latency vs local).

The optimal configuration is two Spark executors, one per NUMA node:

```
spark.executor.instances=2
spark.executor.cores=32   (32 physical cores per socket; or 64 if using HT)
spark.executor.memory=430g  (512 GB × 0.85, leaving 15% for OS/overhead)
spark.executor.memoryOverhead=50g
spark.executor.extraJavaOptions=-XX:+UseNUMA -XX:+UseG1GC -XX:G1HeapRegionSize=32m
```

The two executors are launched with numactl binding:
```bash
numactl --cpunodebind=0 --membind=0 [executor 0 launch command]
numactl --cpunodebind=1 --membind=1 [executor 1 launch command]
```

Hyperthreading deserves mention: on a 64-physical-core socket with HT enabled, the OS sees 128 logical CPUs per socket. For compute-bound workloads, disabling HT or using only physical cores gives better cache utilization (two HT siblings share the same L1/L2 cache). For I/O-bound workloads, HT helps by providing additional memory-level parallelism. For Spark's mixed workload profile, using 32–64 cores per executor (not all 128 on the socket) is typically optimal.

The alternative — one executor with 128 cores and 1 TB — is the NUMA-unaware configuration. It's simpler to configure but leaves 25–35% memory bandwidth on the table for bandwidth-limited stages. For a Spark job that shuffles 500 GB, the NUMA-aware configuration saves roughly 30 seconds per shuffle stage on a 30-minute job — meaningful at scale.

---

### Q5: Explain the difference between AMD's NPS (NUMA Per Socket) modes and true 2-socket NUMA.

**Answer:**

True 2-socket NUMA involves two physically separate CPU dies connected by an inter-die interconnect (Intel UPI). The latency from a core on socket 0 to memory on socket 1 crosses the UPI: ~30 ns of wire latency plus protocol overhead, resulting in ~120–160 ns total remote access latency (versus ~70 ns local). The NUMA distance reported by the OS for true 2-socket Intel systems is typically 21 — a 2.1× multiplier.

AMD's NPS (NUMA Per Socket) modes are a different phenomenon: NUMA-like topology within a single physical socket, created by AMD's chiplet architecture. An AMD EPYC Milan or Genoa processor contains multiple Core Complex Dies (CCDs), each with 8 cores and 32 MB of L3 cache, connected to a central I/O die (IOD) via AMD's Infinity Fabric. In NPS1 mode (default for many instances), the OS sees one large NUMA node covering all CCDs. In NPS4 mode (configured in BIOS or by cloud provider), the OS sees 4 NUMA nodes, each covering a subset of CCDs.

The key difference: in NPS4 mode, crossing a CCD boundary goes through the Infinity Fabric on the same physical die — roughly 8–15 ns of latency instead of the 30 ns of an inter-socket UPI link. The NUMA distance for AMD NPS4 is typically 11–12 (a 1.1–1.2× multiplier), compared to Intel's 21 (2.1×). The bandwidth across the Infinity Fabric is also much higher (the IOD has enough bandwidth to service all CCDs simultaneously without a bottleneck) compared to Intel UPI's 41 GB/s limit.

The practical implication: NUMA misconfigurations on AMD NPS4 instances are less damaging than on true Intel 2-socket systems. An untuned Spark executor on an AMD EPYC instance loses 10–15% throughput from NUMA misses, while an untuned executor on a 2-socket Intel server loses 25–35%. NUMA tuning is still beneficial on AMD but less urgent. The detection approach is the same — `numactl --hardware`, `numastat` — but the threshold for "action required" is higher.

---

### Q6: A Kafka broker is running on a 2-socket server. The broker handles 1 GB/s of produce traffic. After NUMA tuning, you expect 30% throughput improvement. Walk through exactly what you would change and why it helps.

**Answer:**

Before tuning, the Kafka broker JVM runs without NUMA binding. The JVM heap (let's say 24 GB) is allocated by first-touch, landing mostly on socket 0 (where the main thread starts). Network interrupts from the NIC (attached to socket 0's PCIe root complex) are handled by IRQ affinity — by default, the kernel may spread IRQ handlers across all CPUs including socket 1. When a socket 1 CPU handles a NIC interrupt, it copies the packet data to the page cache using a socket 1 kernel thread, but the page cache pages for that partition's log segment were initially created by a Kafka writer thread on socket 0, so they're on socket 0's DRAM. The socket 1 interrupt handler must cross the UPI to write to that page — remote NUMA write.

Similarly, consumer reads: a consumer polls for data, Kafka calls `sendfile()` to zero-copy from page cache to NIC. If the page is on socket 0's DRAM but the `sendfile()` DMA operation reads through socket 1 (because the kernel thread processing the consumer connection is on socket 1), the DMA read crosses the UPI.

The changes I would make:

**Step 1 — Pin NIC interrupts to socket 0:** Find the IRQ numbers for the NIC (`cat /proc/interrupts | grep eth`), then write socket 0's CPU mask to `/proc/irq/<N>/smp_affinity_list` for each interrupt. This ensures all NIC packet processing happens on socket 0, where the NIC's DMA memory is closest.

**Step 2 — Bind the Kafka JVM to socket 0:** Launch Kafka with `numactl --cpunodebind=0 --membind=0 kafka-server-start.sh`. This ensures all Kafka threads (network threads, I/O threads, replication threads) run on socket 0 cores and allocate memory on socket 0 DRAM. The page cache — managed by the kernel but filled by Kafka's write path — will be on socket 0's DRAM because the Kafka I/O threads writing to the page cache are on socket 0.

**Step 3 — Add `-XX:+UseNUMA` to JVM options:** This ensures the JVM heap allocator uses NUMA-local memory for all object allocations. With `--membind=0`, this is redundant but adds safety if thread scheduling ever migrates briefly.

Why 30% improvement: the broker's hot path is network receive → kernel write to page cache → `sendfile()` from page cache to network. Each step involves DRAM accesses. Without tuning, ~40% of those accesses cross the UPI (from socket 1 interrupt handling writing to socket 0 pages). With tuning, all accesses are local. At 200 GB/s local vs 125 GB/s mixed effective bandwidth, the throughput improvement for a bandwidth-limited broker is `(200 - 125) / 200 = 37%` — close to the 30% estimate.

---

## 17. Cross-Question Chain

**Topic:** From "what is NUMA?" to "how do you size Spark executors for a 2-socket server?"

---

**Interviewer:** What is NUMA?

**Candidate:** NUMA (Non-Uniform Memory Access) describes a multi-processor system where each CPU socket has its own directly-attached DRAM, and accessing another socket's DRAM requires crossing a high-speed interconnect. On Intel 2-socket servers, that interconnect is UPI (UltraPath Interconnect) at ~41 GB/s. Local DRAM access latency is ~70 ns; remote (cross-UPI) is ~120–160 ns. In a 2-socket server with 200 GB/s of local DRAM bandwidth per socket, the UPI provides only 41 GB/s cross-socket — a 4.9× bandwidth asymmetry. The critical rule: access latency and bandwidth both depend on where the data lives relative to the CPU making the request.

---

**Interviewer:** Why does this matter specifically for JVM workloads?

**Candidate:** Because the JVM allocates its heap as a large virtual address block at startup, and Linux's first-touch policy assigns physical DRAM pages based on which thread first writes them. In most JVM startups, the main thread initializes the heap — landing those pages on socket 0's DRAM. When the application creates a thread pool and the OS schedules some threads on socket 1, those threads allocate new objects on the heap. But the initial JVM data structures, class metadata, and any objects written by the main thread during startup are already on socket 0's DRAM. Socket 1 threads accessing those objects pay the remote latency. For Spark, which creates many executor threads that access the same task buffers and heap-resident data structures across the job, this remote access penalty adds up across billions of memory operations.

---

**Interviewer:** How does `-XX:+UseNUMA` fix this?

**Candidate:** The flag makes the JVM's garbage collector NUMA-aware. Without it, the GC treats the heap as a single flat address space and places objects wherever the allocator finds free space, regardless of NUMA node. With it, the GC (specifically G1GC and Parallel GC) divides the heap into NUMA-local regions: each CPU core allocates from its local NUMA node's region. Thread-Local Allocation Buffers (TLABs) are assigned from NUMA-local regions. A thread on socket 1 gets a TLAB backed by socket 1's DRAM; a thread on socket 0 gets a socket 0-backed TLAB. Objects created by each thread live on that thread's local DRAM. GC collection is parallelized across both nodes, each GC thread collecting objects on its local node.

The flag is necessary but not sufficient — you also need `numactl --membind=N` to prevent the OS from placing pages on the wrong node when local memory is full, and the number of executors should match the number of NUMA nodes so no executor spans nodes.

---

**Interviewer:** What happens if you put too many executors on one NUMA node?

**Candidate:** Each executor competes with others on the same node for L3 cache and DRAM bandwidth. An executor doing a hash join builds a hash table; if that hash table exceeds the L3 capacity available to it (which shrinks as more executors share the same node's L3), cache misses increase. Two executors on the same 32-core socket each doing shuffle writes are competing for the same 200 GB/s of DRAM bandwidth. With two executors at 100 GB/s each, they saturate the node's bandwidth — one of them will stall waiting for memory. The optimal configuration is exactly one executor per NUMA node, giving each executor exclusive access to that node's L3 and DRAM bandwidth.

---

**Interviewer:** How do you size `spark.executor.memory` and `spark.executor.cores` for a 2-socket `r6i.32xlarge`?

**Candidate:** The `r6i.32xlarge` has 64 physical cores (128 with hyperthreading) and 1 TB total DRAM, split evenly: 32 physical cores and 512 GB per socket.

For executor count: 2 — one per NUMA node.
For cores: 32 per executor — one executor gets all physical cores on its socket. Using hyperthreading cores (going to 64) helps for I/O-bound tasks but hurts for cache-heavy tasks (HT siblings share L1/L2). For Spark's mixed workload, 32–48 cores per executor is typical.
For memory: 512 GB × 0.85 = ~435 GB per executor. Leave 15% for the OS, kernel, and system processes. With `spark.executor.memoryOverhead` of 50 GB, the JVM heap is 385 GB.

The launch command:
```bash
# Two separate executor JVMs, one per NUMA node:
numactl --cpunodebind=0 --membind=0 \
  java -Xmx385g -XX:+UseNUMA -XX:+UseG1GC \
  -cp spark-executor.jar org.apache.spark.executor.CoarseGrainedExecutorBackend \
  --executor-id 0 --cores 32 ...

numactl --cpunodebind=1 --membind=1 \
  java -Xmx385g -XX:+UseNUMA -XX:+UseG1GC \
  -cp spark-executor.jar org.apache.spark.executor.CoarseGrainedExecutorBackend \
  --executor-id 1 --cores 32 ...
```

---

## 18. What's Next

**CSF-ARC-102 M03: TLB, Virtual Memory, and Huge Pages** — This module and M01 both described memory access in terms of physical addresses. In reality, all CPU memory accesses use virtual addresses that the hardware translates to physical addresses through the MMU (Memory Management Unit) and page tables. The TLB (Translation Lookaside Buffer) caches these translations — a TLB miss requires a page table walk, adding 4–8 extra memory accesses per missed translation. For a NumPy array with 10M elements, the first access to each 4 KB page (512 elements) incurs a TLB miss. Huge pages (2 MB instead of 4 KB) reduce TLB misses by 512×. M03 explains virtual memory, page tables, TLB operation, TLB miss cost, and how to enable transparent huge pages or explicit `mmap(MAP_HUGETLB)` for large array workloads in data engineering.

**CSF-ARC-102 M04: The Hardware Prefetcher** — We used the prefetcher as a black box in M01 and M05 of ARC-101. M04 opens it up: what algorithms the hardware prefetcher uses (stride detector, stream buffer, next-line prefetcher, MSHR-based prefetching), when it succeeds (sequential patterns, fixed strides), when it fails (random access, pointer-chasing, variable strides), and how to write data structures that maximize prefetcher effectiveness.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What does NUMA stand for, and what is the core property it describes? | Non-Uniform Memory Access. In a NUMA system, the latency and bandwidth of memory access depends on whether the memory is local (on the same socket's DRAM) or remote (on another socket, accessed via interconnect). |
| 2 | What is Intel's inter-socket interconnect, and what bandwidth does it provide? | UltraPath Interconnect (UPI). ~41 GB/s per direction on UPI 2.0 (Ice Lake). Compare to ~200 GB/s local DDR5 bandwidth — 5× less. The UPI bottleneck makes cross-socket memory traffic 4–5× slower in bandwidth terms. |
| 3 | What is the typical NUMA remote access latency multiplier on a 2-socket Intel server? | 2.1× (distance 21 vs local distance 10). Local DRAM: ~70 ns. Remote DRAM: ~120–160 ns. The extra ~50–90 ns comes from UPI link traversal (~30 ns wire latency) + remote memory controller overhead. |
| 4 | What is Linux's first-touch NUMA memory allocation policy? | Physical pages are assigned to the NUMA node of the thread that first writes to the virtual address. `malloc()` and `mmap()` only create virtual ranges — no physical pages until first write (demand paging). |
| 5 | Why does first-touch cause NUMA problems for JVM workloads? | JVM's main thread initializes the heap → heap pages land on main thread's NUMA node. Worker threads on the other node access those pages → remote NUMA access on every heap operation. |
| 6 | What does `-XX:+UseNUMA` do in the JVM? | Makes the JVM's G1/Parallel GC NUMA-aware: allocates TLABs from the calling thread's local NUMA node, divides the heap into NUMA-local regions, parallelizes GC across nodes. Eliminates forced cross-socket heap access. |
| 7 | What is the optimal Spark executor configuration for a 2-socket server? | One executor per NUMA node, each bound with `numactl --cpunodebind=N --membind=N` and `-XX:+UseNUMA`. This gives each executor exclusive access to its node's L3 and DRAM bandwidth. |
| 8 | What does `numactl --hardware` show? | Number of NUMA nodes, CPUs per node, memory per node, and the distance matrix (normalized latency between all node pairs). |
| 9 | What does `numastat` tell you? | System-wide NUMA hit/miss counts. `numa_miss / (numa_hit + numa_miss)` = NUMA miss rate. > 10% = problem; > 30% = severe. `numastat -p <pid>` shows per-process heap distribution. |
| 10 | What is AMD's NPS (NUMA Per Socket) mode? | A BIOS setting that exposes intra-socket NUMA nodes based on AMD's chiplet CCD topology. NPS4 = 4 NUMA nodes per socket (one per 2 CCDs on Milan). Remote distance ~11–12 (1.1–1.2× vs Intel's 2.1×). Less severe than true 2-socket NUMA. |
| 11 | What is the NUMA bandwidth penalty (not latency) on 2-socket Intel? | Local: ~200 GB/s (DDR5-4800, 8 channels). Cross-socket via UPI: ~41 GB/s. Bandwidth penalty: 4.9×. For bandwidth-limited workloads (shuffle, sort), the bandwidth penalty matters more than the latency penalty. |
| 12 | What is `numactl --interleave=all` and when is it wrong to use? | Allocates pages in round-robin across NUMA nodes. Wrong for sequential access patterns — makes 50% of sequential array accesses remote (alternating pages are on different nodes). Only beneficial for truly uniform random access or benchmarking. |
| 13 | How does Kubernetes interact with NUMA? | Kubernetes ignores NUMA by default — pods may get CPUs spanning both sockets. Fix: `TopologyManager` with `single-numa-node` policy in kubelet configuration. Trade-off: pods cannot be larger than one NUMA node. |
| 14 | What is the UPI link, and what happens when it saturates? | Intel UltraPath Interconnect — the PCB trace connecting two sockets. Saturates at ~41 GB/s per direction. When saturated: additional remote memory requests queue, adding latency and throughput collapses. Visible as CPU stall cycles in `perf stat`. |
| 15 | Why does Kafka benefit from NUMA pinning? | NIC interrupts (on socket 0's PCIe) handled by socket 0 cores → page cache fills by socket 0 threads → all on socket 0 DRAM. Without pinning: socket 1 handles some interrupts → remote DRAM writes for page cache updates → reduced broker throughput. |
| 16 | What does `numactl --localalloc` do? | Sets the NUMA policy to always allocate from the calling thread's current NUMA node. Stricter than default (which may drift) but softer than `--membind` (which enforces a specific node even if the thread migrates). |
| 17 | How do you check which NUMA node a running process has its heap on? | `numastat -p <pid>` — shows MB of heap on each NUMA node. If the heap is skewed (e.g., 90% on node 0, process has threads on both nodes) → NUMA problem. |
| 18 | What is the NUMA miss rate threshold that warrants action? | > 10%: investigate. > 30%: severe — NUMA tuning is likely the biggest performance lever. < 5%: NUMA is not a problem. |
| 19 | Why does one Spark executor per NUMA node outperform one big executor on 2 sockets? | One big executor: heap spans 2 nodes, half of accesses are remote (125 GB/s effective). Two bound executors: each executor's heap is entirely local (200 GB/s each = 400 GB/s total combined). 37% more effective bandwidth. |
| 20 | What is the Infinity Fabric advantage over Intel UPI for cross-NUMA traffic? | AMD's Infinity Fabric connects CCDs within a socket through a shared IOD with much higher internal bandwidth than UPI. Cross-CCD latency: ~100 ns vs Intel UPI's ~130–160 ns. Cross-NUMA bandwidth on Infinity Fabric within a socket: ~150–200 GB/s (higher than UPI's 41 GB/s). |

---

## 20. References

**NUMA Architecture**

- Drepper, U. (2007). What every programmer should know about memory. LWN.net. Part 5 (NUMA Support). https://lwn.net/Articles/254445/ — The most thorough treatment of NUMA from a Linux programmer's perspective. Covers `set_mempolicy`, NUMA allocation, and performance measurement.
- Intel (2024). Intel Xeon Scalable Processor Technical Overview: UPI Architecture. https://www.intel.com/content/www/us/en/developer/articles/technical/xeon-processor-scalable-family-technical-overview.html — UPI bandwidth, topology, and latency specifications.
- AMD (2023). AMD EPYC 7003 Series Processor Technical Overview (Milan). NUMA domains, NPS modes, Infinity Fabric interconnect speeds. https://www.amd.com/en/processors/epyc-7003-series

**Linux NUMA Tools**

- `man 8 numactl` — complete documentation for all numactl modes: `--cpunodebind`, `--membind`, `--localalloc`, `--interleave`, `--preferred`.
- `man 7 numa` — Linux NUMA policy system call documentation (`set_mempolicy`, `get_mempolicy`, `mbind`).
- numastat man page: `man 8 numastat` — interpretation of hit/miss/foreign counts.
- hwloc project: https://www.open-mpi.org/projects/hwloc/ — `lstopo` visualization, programmatic NUMA topology detection.

**JVM NUMA**

- OpenJDK documentation: `-XX:+UseNUMA` flag. https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html — JVM flag reference.
- Schatzl, T. (2022). NUMA awareness in G1GC. Oracle JDK blog. Covers how G1GC divides heap regions by NUMA node and schedules GC threads.

**Data Engineering Systems**

- Apache Spark documentation: `spark.executor.extraJavaOptions` configuration guide. https://spark.apache.org/docs/latest/configuration.html
- Kafka performance tuning guide: https://kafka.apache.org/documentation/#prodconfig — includes `num.network.threads` and NUMA considerations for multi-socket brokers.
- Flink deployment guide — TaskManager configuration for multi-socket servers: https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/
- Rabl, T., et al. (2012). Solving big data challenges for enterprise application performance management. VLDB. — Includes NUMA measurements for multi-socket database servers.
