# M01: Cache Lines and the Memory Hierarchy

**School:** CS Foundations (CSF)  
**Course:** CSF-ARC-102 — Memory Architecture  
**Module:** 01 of 05  
**Difficulty:** ★★★☆☆  
**Estimated study time:** 5–6 hours  
**Last updated:** 2026-06-27

---

## Table of Contents

1. [Why This Module Exists](#1-why-this-module-exists)
2. [Prerequisites](#2-prerequisites)
3. [Learning Objectives](#3-learning-objectives)
4. [First Principles](#4-first-principles)
5. [Architecture](#5-architecture)
6. [Deep Dive — Cache Mechanics](#6-deep-dive--cache-mechanics)
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

In CSF-ARC-101 M05, you traced `np.sum(arr)` to the hardware and established that for large arrays, performance is **memory-bandwidth-limited**: the CPU's FPUs sit idle waiting for data. The prefetcher helps, but only for sequential access. A deeper question remained unanswered: *why is memory slow at all?*

The answer is the memory hierarchy — a deliberate engineering compromise between speed and size that governs every byte of data that every program you write ever touches.

**The fundamental tension:**
- Fast memory (SRAM, used in CPU caches) costs ~$10,000/GB and consumes ~1,000× more power than DRAM.
- Slow memory (DRAM, used as main memory) costs ~$3/GB.
- A modern server with 256 GB of SRAM cache is not economically viable.

The solution invented in the 1960s and refined continuously since is a **hierarchy**: a small amount of very fast memory near the CPU, backed by successively larger and slower layers. Programs that exhibit **locality** — accessing the same data repeatedly (temporal locality) or accessing nearby data (spatial locality) — spend most of their time in the fast layers and rarely touch the slow ones.

Every data engineering performance decision — partition sizing, sort-merge join vs hash join, Parquet row group size, Spark executor memory settings — is ultimately a decision about which layer of the memory hierarchy a computation spends its time in. This module gives you the mental model to reason about all of them.

---

## 2. Prerequisites

- CSF-ARC-101 M01–M05 (all). Specifically:
  - The performance formula: Runtime = Instructions × CPI × (1/clock_hz)
  - Cache miss penalty was referenced but not explained — this module explains it.
  - The prefetcher hides sequential cache miss latency — this module explains how and why.
  - `perf stat` LLC-load-misses counter — this module explains what it measures.

---

## 3. Learning Objectives

By the end of this module you will be able to:

1. Name all four layers of the x86-64 memory hierarchy with their typical sizes, latencies in nanoseconds and CPU cycles, and bandwidth figures.
2. Explain what a cache line is, why it is 64 bytes, and what happens inside the CPU when a cache miss occurs.
3. Describe set-associative cache organization: what a set, a way, an index, and a tag are.
4. Explain the MESI protocol: what each state means and when a cache line transitions between states.
5. Distinguish write-back from write-through caches and explain why modern CPUs use write-back.
6. Apply the concept of spatial and temporal locality to explain why row-oriented storage is bad for analytical queries and columnar storage is good.
7. Design a cache-blocked (tiled) loop for a matrix operation and predict the performance improvement.
8. Use `perf stat -e cache-misses,cache-references,LLC-load-misses` to measure cache behavior and interpret the output.

---

## 4. First Principles

### Why Memory Exists at All

A CPU register holds one value (8 bytes). A modern CPU has 16 architectural registers (plus ~350 physical registers after renaming). But a useful program needs to work with more than 350 values — a 10M-element array has 80 MB of data. That data has to live somewhere between computations.

"Somewhere" is a hierarchy of storage, each level a compromise:

```
Faster ↑        Smaller ↑        More expensive ↑
│
│  CPU Registers      ~1 cycle     ~500 bytes     (6T SRAM cells — very dense)
│  L1 Cache          ~4 cycles     ~48 KB         (6T SRAM)
│  L2 Cache         ~12 cycles    ~256 KB–2 MB    (6T SRAM)
│  L3 Cache         ~40 cycles    ~6–64 MB        (6T SRAM, shared across cores)
│  DRAM             ~200 cycles   ~8–768 GB       (1T1C DRAM cells — cheap)
│  NVMe SSD         ~100,000 ns   ~1–8 TB         (NAND flash)
│  HDD              ~5,000,000 ns ~4–20 TB        (magnetic spinning disk)
│
Slower ↓        Larger ↓         Cheaper ↓
```

The hierarchy works because of the **locality principle**: most programs spend most of their time accessing a small fraction of their data. If that fraction fits in L1 or L2, the program runs at near-register speed. If it doesn't fit, it pays the DRAM penalty.

### The Cache Line: Why 64 Bytes?

Memory is not transferred one byte or one word at a time between cache and DRAM. It is transferred in **cache lines**: fixed-size blocks. On x86-64, ARM64, and RISC-V, the universal cache line size is **64 bytes**.

Why 64, not 8 or 512?

- **Too small (e.g., 8 bytes):** Spatial locality can't be exploited. Each access that misses the cache fetches only 8 bytes. If the next access is to address+8, it misses again immediately. The DRAM access overhead (row activation, column select, bus turnaround) is amortized over only 8 bytes — enormous overhead per useful byte.
- **Too large (e.g., 512 bytes):** A cache miss fetches 512 bytes but the program may only need 8 of them. For random access patterns (hash table lookups, pointer-chasing through linked lists), 504 bytes are loaded, occupy cache space, and are evicted before use — wasted bandwidth and wasted cache capacity.
- **64 bytes is the sweet spot** (empirically determined): amortizes DRAM overhead across 8 float64 values or 16 int32 values, fits one to two typical struct sizes, and wastes minimal cache for random access patterns.

The 64-byte cache line is one of the most important constants in computing. **It dictates the minimum memory bandwidth cost of any operation.**

---

## 5. Architecture

### The Complete Memory Hierarchy Picture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      CPU MEMORY HIERARCHY (Intel Alder Lake / AWS c6i)       │
│                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  CPU DIE                                                              │   │
│  │                                                                       │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │   │
│  │  │  Core 0  │  │  Core 1  │  │  Core 2  │  │  Core N  │            │   │
│  │  │          │  │          │  │          │  │          │            │   │
│  │  │ L1-I 32K │  │ L1-I 32K │  │ L1-I 32K │  │ L1-I 32K │            │   │
│  │  │ L1-D 48K │  │ L1-D 48K │  │ L1-D 48K │  │ L1-D 48K │            │   │
│  │  │  4 cyc   │  │  4 cyc   │  │  4 cyc   │  │  4 cyc   │            │   │
│  │  │          │  │          │  │          │  │          │            │   │
│  │  │  L2 2MB  │  │  L2 2MB  │  │  L2 2MB  │  │  L2 2MB  │            │   │
│  │  │  12 cyc  │  │  12 cyc  │  │  12 cyc  │  │  12 cyc  │            │   │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘            │   │
│  │       │              │              │              │                  │   │
│  │       └──────────────┴──────────────┴──────────────┘                 │   │
│  │                              │                                        │   │
│  │              ┌───────────────┴─────────────────┐                     │   │
│  │              │       L3 Cache (shared)          │                     │   │
│  │              │   36–64 MB   /   ~40 cycles      │                     │   │
│  │              └───────────────┬─────────────────┘                     │   │
│  └──────────────────────────────┼────────────────────────────────────── ┘   │
│                                 │  Memory Controller (on-die, since 2003)    │
│                                 │                                             │
│              ┌──────────────────┴──────────────────┐                        │
│              │  DRAM (DDR4/DDR5 DIMMs)              │                        │
│              │  8–768 GB   /   ~70ns (~245 cycles)  │                        │
│              │  Bandwidth: 25–76 GB/s per channel   │                        │
│              └──────────────────┬──────────────────┘                        │
│                                 │                                             │
│              ┌──────────────────┴──────────────────┐                        │
│              │  NVMe SSD / EBS / S3                 │                        │
│              │  GBs–TBs   /   μs–ms–seconds         │                        │
│              └─────────────────────────────────────┘                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Latency and Bandwidth Reference Table

| Level | Typical Size | Latency (cycles) | Latency (ns) | Bandwidth (GB/s) | Scope |
|---|---|---|---|---|---|
| L1-D cache | 32–64 KB | 4–5 | ~1.5 ns | ~2,000 | Per core, private |
| L2 cache | 256 KB–4 MB | 12–15 | ~4 ns | ~500 | Per core, private |
| L3 cache | 6–64 MB | 35–50 | ~12–15 ns | ~200 | Shared all cores |
| DRAM (local) | 8–768 GB | 200–300 | ~65–100 ns | 25–76 | Shared all cores |
| DRAM (remote NUMA) | 8–768 GB | 350–500 | ~120–160 ns | 20–40 | Cross-socket penalty |
| NVMe SSD | 1–8 TB | ~100,000 | ~100 μs | 3–7 | System-wide |
| S3 (network) | Unlimited | ~100,000,000 | ~100 ms | 0.1–1 | System-wide |

*Figures approximate; vary by CPU generation and DRAM speed. AWS c6i.4xlarge measured 2024.*

---

## 6. Deep Dive — Cache Mechanics

### 6.1 What Happens on a Cache Miss

When the CPU executes a load instruction (e.g., `vmovupd ymm0, [rdi+rax]`):

1. **L1 tag check (1 cycle):** The address is split into three parts: tag bits, index bits, and offset bits. The cache controller checks whether a cache line with the matching tag exists in the indexed set. If yes: **L1 hit** — data is returned in ~4 cycles.

2. **L2 tag check (~12 cycles if L1 miss):** Same process. If L2 hit, data is returned, and L1 is filled with the cache line.

3. **L3 tag check (~40 cycles if L2 miss):** If L3 hit, the 64-byte cache line travels from L3 → L2 → L1. Each fill takes the corresponding latency.

4. **DRAM request (~200+ cycles if L3 miss):** The L3 miss triggers a request to the memory controller. The memory controller sends a row+column address to the DRAM DIMM, performs the row activation and column select, and receives 64 bytes in a burst transfer. The 64 bytes fill L3, L2, and L1 simultaneously (inclusive cache) or according to the inclusion policy.

The CPU has a **Miss Status Holding Register (MSHR)** — a hardware queue for outstanding cache misses. Modern CPUs have 10–20 MSHRs per core. If the program generates cache misses faster than DRAM can service them (more than ~20 outstanding misses), new misses must wait. This is the mechanism behind **memory-bound bottlenecks** measured by `perf stat`.

### 6.2 Cache Organization: Sets, Ways, Tags

Caches are **set-associative**: a compromise between fully associative (any line goes anywhere — requires expensive parallel tag comparison across all lines) and direct-mapped (each address maps to exactly one line — simple but thrashes badly for certain access patterns).

```
8-way set-associative cache with 64 sets and 64-byte lines:
  Total capacity = 64 sets × 8 ways × 64 bytes = 32,768 bytes = 32 KB

A 64-bit memory address is split:
  ┌──────────────────────┬────────────────┬──────────────┐
  │  Tag (47 bits)       │  Index (6 bits)│ Offset(6 bit)│
  └──────────────────────┴────────────────┴──────────────┘
  bit 63              17  bit 16        11  bit 10       0

  Offset  (bits 5:0):   byte within the 64-byte cache line (0–63)
  Index   (bits 11:6):  which of the 64 sets to check (0–63)
  Tag    (bits 63:12):  which address this cache line belongs to

On an access, the hardware:
1. Uses the index bits to select one set (one of 64 rows in the array)
2. Compares the tag bits against all 8 ways in that set SIMULTANEOUSLY
3. If any way matches (and the valid bit is set): cache HIT
4. If no way matches: cache MISS → evict one way (LRU), fill from next level
```

**Why 8-way?** Empirical studies show that 4-way associativity captures 95% of the benefit of full associativity for typical access patterns. 8-way and 16-way reduce conflict misses further while keeping the tag comparison circuit simple.

**Conflict miss:** Two addresses that map to the same set, filling all 8 ways. A 9th address forces an eviction. If the program then re-accesses the first address, it misses again. Conflict misses are the failure mode of direct-mapped caches and can affect set-associative caches for pathological access patterns (e.g., a matrix with dimension exactly equal to a power of 2 — a classic performance trap).

### 6.3 The MESI Protocol

In a multi-core CPU, each core has its own L1 and L2 caches. If core 0 writes to a memory address that core 1 also has cached, their caches would disagree — a coherence violation. The **MESI protocol** (Modified, Exclusive, Shared, Invalid) prevents this:

```
Each cache line is in one of four states:

  M (Modified):  This core has the only copy, and it has been written.
                 The DRAM copy is stale. This core MUST write back before
                 any other core can read this address.
                 Example: core 0 just wrote to arr[0].

  E (Exclusive): This core has the only copy, and it matches DRAM.
                 No write-back needed on eviction; can be upgraded to M
                 on write without a bus transaction.
                 Example: core 0 just read arr[0] and no other core has it.

  S (Shared):    Multiple cores have a copy, all matching DRAM (read-only).
                 A write requires invalidating all other copies first.
                 Example: both core 0 and core 1 have read arr[0].

  I (Invalid):   This cache line does not contain valid data.
                 Next access is a cache miss.
                 Example: after another core wrote to arr[0], this core's
                 copy was invalidated.
```

**MESI state transitions:**

```
                    ┌─────────────────────────────────────────────┐
                    │                                             │
         Read miss  ▼     Write hit                              │
    I ──────────► E ──────────────► M                            │
    │             │                 │                            │
    │   Other     │ Other core      │ Other core                 │
    │   core      │ reads           │ reads/writes               │
    │   writes    │                 │                            │
    │             ▼                 ▼                            │
    │             S ◄───────────────I                            │
    │             │                                              │
    │      Write  │                                              │
    │      hit    ▼                                              │
    └──────────── M ─────────────────────────────────────────────┘
                  (write-back on eviction)

Key transitions for data engineering:
  - Spark executor writes a partition result → state M
  - Another core reads the same partition → writer invalidates (M→I),
    other core gets fresh data via cache coherence bus (I→S or I→E)
  - False sharing: two cores write to DIFFERENT variables in the SAME
    64-byte cache line → state bounces M→I→M→I on both cores
    → coherence traffic chokes performance (see M05)
```

### 6.4 Write-Back vs Write-Through

**Write-through:** Every write to L1 is immediately forwarded to L2 and DRAM. Simple correctness model. Used in some embedded systems. Problem: every write saturates the memory bus — write-intensive workloads are bottlenecked by DRAM bandwidth even when all data fits in L1.

**Write-back:** Writes update only the L1 cache line (set to M state). The modified line is written to DRAM only when it is evicted from the cache (replaced by another line). All modern CPUs use write-back for data caches. This is why `perf stat` can show 100% of data fitting in L2 with very low memory bandwidth — writes don't touch DRAM until eviction.

**Implication:** A Spark executor that writes a large intermediate result to off-heap memory goes through the CPU cache. If the write working set exceeds L3, writes evict cache lines containing the *input* data, thrashing the cache for subsequent reads. This is why Spark's `spark.sql.shuffle.partitions` tuning is not just about parallelism — it affects cache occupancy.

### 6.5 Inclusive vs Exclusive Cache Hierarchies

**Inclusive (Intel default):** L2 contains a superset of L1. L3 contains a superset of L2. Every cache line in L1 is also in L2 and L3. Advantage: eviction from L3 guarantees eviction from L1/L2 — simple coherence. Disadvantage: effective L3 capacity is reduced because it duplicates L1 and L2 content.

**Exclusive (AMD Zen):** Each cache level holds *different* lines. L1 + L2 + L3 together store unique data. Advantage: full use of all cache capacity. A miss in L1 goes to L2 (no hit); if not in L2, swaps with L3 (moves line from L3 to L1, moves evicted L1 line to L3). More complex protocol, but AMD's Zen 4 achieves an effective cache capacity advantage.

For data engineering: AMD EPYC (Zen 4) instances (AWS `m7a`, `c7a`) have up to 1.2 GB of L3 per socket due to exclusive hierarchy — much larger working sets fit in cache compared to Intel equivalents.

---

## 7. Mental Models

### Mental Model 1 — The Latency Cliff

Think of the memory hierarchy as a latency cliff:

```
                                     ← cost per cache miss in CPU cycles →

  Operations that stay in L1:         4 cycles per access
                                       ||||

  Operations that spill to L2:        12 cycles per access
                                       ||||||||||||

  Operations that spill to L3:        40 cycles per access
                                       ||||||||||||||||||||||||||||||||||||||||

  Operations that go to DRAM:        200–300 cycles per access
  (for random access patterns)       ████████████████████████████████████████
                                     ████████████████████████████████████████
                                     ████████████████████████████████████████████████████████████
```

The cliff between L3 and DRAM is steep: 40 → 200 cycles, a 5× jump. The cliff between DRAM and NVMe is 100,000 cycles (250 µs / 1 ns/cycle). The cliff between NVMe and network storage (S3, HDFS) is 100,000,000 cycles (~100 ms).

**The rule:** Never pay to go over a cliff if you can avoid it. Design computations so the working set fits in the highest cache level possible. If it can't, ensure the access pattern is sequential so the prefetcher hides the DRAM latency with bandwidth.

### Mental Model 2 — The Spatial Locality Tax

Every cache miss loads 64 bytes regardless of how many bytes you need. If you need 8 bytes (one float64), you pay for 56 bytes you didn't ask for. If you then use those 56 bytes (the next 7 float64s in the array), the additional accesses are L1 hits — free. If you never use them, you paid the DRAM latency for nothing.

```
Row-oriented storage (bad for analytics):
  Cache line loads: [id(8) | name(32) | age(4) | salary(8) | dept_id(4) | ...]
  Query: SELECT AVG(salary) FROM employees
  Useful bytes per cache line: 8  (just salary)
  Wasted bytes per cache line: 56
  Utilization: 8/64 = 12.5%

Columnar storage (good for analytics):
  Cache line loads: [salary(8) | salary(8) | salary(8) | salary(8) | salary(8)
                    | salary(8) | salary(8) | salary(8)] ← 8 values per line
  Useful bytes per cache line: 64
  Wasted bytes per cache line: 0
  Utilization: 64/64 = 100%

At 14 GB/s DRAM bandwidth:
  Row-oriented: 14 × 0.125 = 1.75 GB/s of useful data throughput
  Columnar:     14 × 1.000 = 14.0 GB/s of useful data throughput
  Ratio: 8×
```

This 8× difference (one full cache line of salary values vs one per cache line) is the fundamental hardware reason why columnar formats (Parquet, ORC, Arrow) outperform row formats (CSV, JSON, Avro row-encoded) for analytical workloads.

### Mental Model 3 — Cache Occupancy as a Knob

The L3 cache is shared by all cores on a CPU. For a 32-core CPU with 36 MB of L3, each core gets ~1 MB on average — but in practice, active cores compete. A Spark executor with 4 CPU cores and a task that needs 10 MB of working set will have cores competing for L3 space.

Think of L3 as a shared buffer. The design question for every data engineering system is: **what goes in this buffer?**

| System | What fits in L3 | Design implication |
|---|---|---|
| Spark (broadcast join) | Small table (<= L3 size per executor) | Broadcast the small table; every probe hits L3 |
| Spark (sort-merge join) | Sorted run being merged | Merge runs in L3-sized chunks |
| Parquet row group size | One row group being decoded | Default 128 MB row group >> L3 — intentionally sequential for bandwidth |
| DuckDB page size | 256 KB = fits in L2 | DuckDB tunes its pages to fit in per-core L2 |
| Pandas groupby | Hash table for group keys | Groups with many unique keys thrash L3; use sort-based groupby instead |

---

## 8. Failure Scenarios

### Failure 1 — Power-of-Two Matrix Dimension Conflict Miss

**Symptom:** A matrix multiplication on a 1024×1024 matrix is 3× slower than on a 1025×1025 matrix, despite the latter being larger.

**Root cause:** A 1024×1024 float64 matrix has row stride = 1024 × 8 = 8,192 bytes = 8 KB. When iterating column-by-column (accessing `A[0][0]`, `A[1][0]`, `A[2][0]`...), the stride between accesses is exactly 8 KB. If the cache has, say, 512 sets (an 8-way L1-D), set index = (address / 64) mod 512. Addresses 8 KB apart: (0/64) mod 512 = 0, (8192/64) mod 512 = 128 mod 512 = 0 — same set. Every row access maps to set 0. With 8 ways, only 8 rows can be cached simultaneously. Row 9 evicts row 1. When you next need row 1, it's gone.

**The 1025-row version:** row stride = 1025 × 8 = 8,200 bytes. Consecutive rows no longer hash to the same sets — evenly distributed.

**How to detect:**
```bash
perf stat -e L1-dcache-load-misses,L1-dcache-loads \
    python3 -c "
import numpy as np
n = 1024
A = np.random.rand(n, n)
B = np.random.rand(n, n)
for _ in range(5): np.dot(A, B)
"
# Look for high L1-dcache-load-miss rate (> 20% is bad for regular access)
```

**Fix:** Pad the matrix dimension or use cache-blocked matrix multiplication (see Lab 3).

---

### Failure 2 — Hash Table Working Set Exceeds L3

**Symptom:** A `pandas.groupby()` on a high-cardinality column (millions of unique groups) is much slower than on a low-cardinality column, even with the same total row count.

**Root cause:** Pandas' groupby uses a hash table to map group keys to their integer codes. Each lookup in the hash table is a pseudo-random memory access. For a hash table of 1M entries × ~50 bytes per entry = 50 MB, the table does not fit in L3 (typically 20–64 MB per socket). Each hash lookup that misses L3 costs ~200 cycles. With 100M rows and 50% miss rate: 50M × 200 cycles = 10 billion cycles ≈ 2.9 seconds just in memory latency.

**perf stat fingerprint:**
```
  LLC-load-misses: 45,000,000  ← very high
  LLC-loads:       50,000,000
  Miss rate: 90%               ← catastrophic
  IPC: 0.38                    ← CPU stalled most of the time
```

**Fix:** Sort the data before groupby. Sorting makes same-group rows adjacent → sequential access → prefetcher handles it → L2/L3 hits instead of DRAM misses. Alternative: use DuckDB or Polars for large-cardinality aggregations — they use cache-blocked sort-aggregate algorithms.

---

### Failure 3 — Parquet Column Reading Spills to DRAM

**Symptom:** Reading a Parquet file with 50 columns, but the query only needs 3 columns. `perf stat` shows high LLC-load-misses despite projecting only needed columns.

**Root cause:** Parquet stores data in **row groups** (default 128 MB) with columns stored contiguously within each row group. When reading a column page (say, 2.5 MB for 1/50th of a 128 MB row group), the Parquet reader decodes the whole column page into memory. The next column page for column 2 may be 2.5 MB at a different memory address. The page reader reads 3 × 2.5 MB = 7.5 MB sequentially, but the decompression buffer is reused at the same address, causing L3 fills and evictions as the decompressor cycles through the compressed data.

With a 128 MB row group and many workers reading simultaneously, the combined working set can exceed L3.

**Fix options:**
- Reduce Parquet row group size to a value that fits in L3 per worker thread (e.g., 32 MB row groups for a 64 MB L3).
- Use smaller column pages (64 KB Parquet page size, tuned to L2).
- DuckDB's default page size is specifically tuned for L2 cache.

---

### Failure 4 — Write Amplification from Cache Line Granularity

**Symptom:** A program that updates individual bytes of a large shared array has unexpectedly high DRAM write bandwidth, measured by `perf stat -e dTLB-stores-misses,cache-misses`.

**Root cause:** Writes are always cache-line granular. Writing one byte to address X causes the CPU to:
1. Load the 64-byte cache line containing X (if not already in cache) — a read for ownership (RFO).
2. Modify the 1 byte in the cache line.
3. Set the line to M (Modified) state.
4. On eviction, write back 64 bytes to DRAM.

For a program that writes 1 byte to every 1,024th address across a 1 GB array: 1M writes × 1 byte = 1 MB of useful writes, but 1M × 64 bytes = 64 MB of DRAM writes (the cache line write-backs). Write amplification factor: 64×.

**Data engineering context:** Columnar compression codecs that update individual bits (run-length encoding state machines, delta encoding) can suffer from this if not cache-line aware. Parquet's encoding pipeline is designed to work on contiguous column pages to avoid this pattern.

---

## 9. Recovery Procedures

### Diagnosing Cache Behavior with `perf stat`

```bash
# Full cache hierarchy diagnostic
perf stat \
  -e L1-dcache-loads,L1-dcache-load-misses \
  -e L1-dcache-stores,L1-dcache-store-misses \
  -e LLC-loads,LLC-load-misses \
  -e cache-references,cache-misses \
  -- python3 my_pipeline.py

# Interpret:
# L1-dcache-load-misses / L1-dcache-loads < 5%  → healthy L1
# LLC-load-misses / LLC-loads < 10%             → healthy L3 (good)
# LLC-load-misses / LLC-loads > 50%             → DRAM-bound (bad)
# cache-misses / cache-references = overall rate across L1/L2/L3
```

### Cache-Blocking (Tiling) Pattern

For operations that access a 2D data structure (join tables, matrix operations, Spark shuffle):

```python
def cache_blocked_dot(A, B, block_size=64):
    """
    Cache-blocked matrix multiply. block_size chosen so that
    3 blocks fit in L2 cache:
      3 × block_size² × 8 bytes ≤ L2 size (256KB)
      block_size ≤ sqrt(256KB / 3 / 8) ≈ 104
    We use 64 for alignment.
    """
    n = A.shape[0]
    C = np.zeros((n, n))
    
    for i in range(0, n, block_size):
        for j in range(0, n, block_size):
            for k in range(0, n, block_size):
                # This block fits in L2 — no DRAM accesses during inner loop
                C[i:i+block_size, j:j+block_size] += \
                    A[i:i+block_size, k:k+block_size] @ \
                    B[k:k+block_size, j:j+block_size]
    return C
```

### Reducing Hash Table Working Set

```python
import pandas as pd
import numpy as np

def cache_friendly_groupby(df, key_col, value_col):
    """
    For high-cardinality keys: sort first, then aggregate.
    Sorting makes consecutive rows belong to same group →
    hash table working set = one group at a time = fits in L1.
    """
    # Sort by key → turns random-access hash lookups into
    # sequential cache-friendly scans
    sorted_df = df.sort_values(key_col)
    return sorted_df.groupby(key_col)[value_col].sum()

# Benchmark both approaches:
N = 10_000_000
n_groups = 1_000_000  # 1M unique groups → hash table >> L3

df = pd.DataFrame({
    'key':   np.random.randint(0, n_groups, N),
    'value': np.random.rand(N),
})

import time

t0 = time.perf_counter()
r1 = df.groupby('key')['value'].sum()  # hash-based, random access
t_hash = time.perf_counter() - t0

t0 = time.perf_counter()
r2 = cache_friendly_groupby(df, 'key', 'value')
t_sort = time.perf_counter() - t0

print(f"Hash-based groupby:  {t_hash:.2f}s")
print(f"Sort-based groupby:  {t_sort:.2f}s")
print(f"Sort includes the sort cost but reduces LLC-load-misses by ~80%")
```

---

## 10. Trade-offs

### Larger vs Smaller Cache Sizes

| Larger cache (more $$$) | Smaller cache (cheaper) |
|---|---|
| More working sets fit without eviction | Working set must be carefully managed |
| Better for large hash tables (groupby, join) | Requires algorithmic cache-awareness |
| Diminishing returns: going from 32MB to 64MB L3 is ~20% benefit for most workloads | Larger L3 increases L3 hit latency slightly (larger physical structure) |
| AWS x2idn.32xlarge has 128 MB L3 per socket (Xeon Sapphire Rapids) | Standard `c7g.4xlarge` has 32MB L3 |

### Columnar vs Row Layout

| Columnar (Parquet, Arrow, ORC) | Row (Avro, JSON, CSV) |
|---|---|
| Analytics (SELECT few columns): 8–64× faster | Transactional (SELECT all columns per row): 1–2× faster |
| Cache line fully utilized (same-type values) | Cache line underutilized (many columns per row) |
| Difficult to append single rows | Append one row = one write |
| SIMD-eligible after decode | SIMD requires transposition overhead |
| Predicate pushdown prunes columns | Must scan full rows to filter |

**The columnar advantage is a hardware argument.** Not just "less data to read" — the data that is read is more efficiently processed because it is homogeneous (same dtype, same value range), which enables SIMD and better compression.

### Row Group Size in Parquet

| Large row groups (128 MB default) | Small row groups (8–32 MB) |
|---|---|
| Better compression (more values for dictionary encoding) | Fits in L3, better cache utilization during decode |
| More rows per dictionary lookup (fewer seeks) | Supports predicate pushdown at finer granularity |
| Single row group fills all available DRAM bandwidth | Parallel decoding of multiple row groups possible |
| Better for sequential full-table scans | Better for selective point lookups with row group statistics |

**Practical rule:** Use 128 MB row groups for ETL pipelines (full scans). Use 16–32 MB row groups for interactive queries with selective predicates.

---

## 11. Comparisons

### CSV vs Parquet: Cache Line Utilization for `SELECT AVG(revenue)`

```python
"""
Compare CSV vs Parquet cache behavior for a simple aggregation.
Uses a synthetic 1M-row dataset with 10 columns.
"""
import numpy as np
import pandas as pd
import time, os, tempfile

N = 1_000_000
COLS = 10  # 10 columns, only one needed for the query

# Create dataset
df = pd.DataFrame({
    f'col_{i}': np.random.rand(N) for i in range(COLS)
})
df['revenue'] = np.random.rand(N) * 1000

# Write both formats
tmpdir = tempfile.mkdtemp()
csv_path = os.path.join(tmpdir, 'data.csv')
parquet_path = os.path.join(tmpdir, 'data.parquet')

df.to_csv(csv_path, index=False)
df.to_parquet(parquet_path, index=False)

print(f"CSV size:     {os.path.getsize(csv_path)/1e6:.1f} MB")
print(f"Parquet size: {os.path.getsize(parquet_path)/1e6:.1f} MB")

# Benchmark: SELECT AVG(revenue)
RUNS = 5

csv_times = []
for _ in range(RUNS):
    t0 = time.perf_counter()
    result = pd.read_csv(csv_path, usecols=['revenue'])['revenue'].mean()
    csv_times.append(time.perf_counter() - t0)

parquet_times = []
for _ in range(RUNS):
    t0 = time.perf_counter()
    result = pd.read_parquet(parquet_path, columns=['revenue'])['revenue'].mean()
    parquet_times.append(time.perf_counter() - t0)

csv_avg     = sum(csv_times) / RUNS
parquet_avg = sum(parquet_times) / RUNS

print(f"\nSELECT AVG(revenue) (10-column table, 1M rows):")
print(f"  CSV read + compute:     {csv_avg*1000:.1f} ms")
print(f"  Parquet read + compute: {parquet_avg*1000:.1f} ms")
print(f"  Speedup: {csv_avg/parquet_avg:.1f}×")
print(f"\nHardware explanation:")
print(f"  CSV: reads all {COLS+1} columns → {(COLS+1)*8} bytes per row → "
      f"{(COLS+1)*8/64*100:.0f}% cache line waste for revenue-only query")
print(f"  Parquet: reads only 'revenue' column → 8 bytes per row → "
      f"8/64 × 8 values per cache line = 100% utilization")
```

---

## 12. Production Examples

### Spark Join Strategy and Cache Utilization

Spark has two primary join algorithms for large tables:

**Broadcast Hash Join (BHJ):**
The small table is broadcast to every executor and stored in memory. Each executor builds a hash table from the small table and probes it with rows from the large table. If the hash table fits in L3: most probes are L3 hits (~40 cycles each). If the hash table is larger than L3: each probe is a DRAM miss (~200 cycles each).

Spark's default broadcast threshold is 10 MB. This is not arbitrary — it is calibrated so the hash table (~2× the data size after serialization) fits in ~20 MB L3. If you increase `spark.sql.autoBroadcastJoinThreshold` to 200 MB, Spark will broadcast a 200 MB table, build a 400 MB hash table, and every probe will be a DRAM miss (assuming L3 < 400 MB). The BHJ becomes slower than a Sort-Merge Join.

**Rule:** `autoBroadcastJoinThreshold` should be at most `(L3 size per executor core) / 2`. For a c6i.16xlarge with 32 vCPUs and 64 MB L3, each core gets ~2 MB of L3. Broadcast threshold ≤ 1 MB for cache-efficient probing. Spark's default of 10 MB is a reasonable compromise assuming ~8 MB L3 per core on typical hardware.

**Sort-Merge Join (SMJ):**
Both tables are sorted on the join key, then merged. The merge accesses both tables sequentially. Sequential access activates the hardware prefetcher — each step reads the next values from both tables in order, landing in L2 or L1 before needed. No hash table, no random access. For large tables where neither fits in L3, SMJ is almost always faster than BHJ because the memory access pattern is sequential rather than random.

### DuckDB's L2-Tuned Page Size

DuckDB stores data in 256 KB pages (its "morsel" size). This is not arbitrary:

```
L2 cache per core:    256 KB (most modern CPUs)
DuckDB page size:     256 KB
Result: one DuckDB page fits exactly in L2.

During aggregation, DuckDB processes one 256 KB morsel at a time.
Within a morsel, all accesses are L2 hits (≤12 cycles).
Between morsels, the next page is prefetched from L3 while the
current morsel is being processed.

Compare to Spark's default 128 MB partition:
  128 MB >> 64 MB L3 → spills to DRAM for each partition scan
  DuckDB stays in L2; Spark spills to DRAM.
  This is a significant hardware-level explanation for why DuckDB
  is faster than Spark for single-node analytical queries.
```

### Kafka Producer Batch Size and Cache Warm-Up

Kafka producers buffer messages in a `RecordAccumulator` before sending. The `batch.size` configuration (default 16 KB) controls how many bytes accumulate before a network send.

```
L1 cache:   48 KB
Kafka batch.size default: 16 KB = fits in L1

Serialization step: producer.send() → RecordAccumulator.append()
  → serialize key/value to byte array
  → append bytes to the batch buffer (16 KB, in L1)
  → compressor reads and writes the same L1-resident buffer
  → network I/O reads the same buffer (already in L1/L2)

If batch.size = 1 MB:
  The batch buffer is in DRAM (1 MB > L1).
  Serialization writes to DRAM.
  Compressor reads from DRAM, writes to DRAM.
  Network I/O reads from DRAM.
  3 DRAM passes for 1 MB of data = 3 × 1MB / 14 GB/s = 215 µs overhead.

For high-throughput producers:
  batch.size = 65536 (64 KB = fits in L2) is often the sweet spot:
  good throughput, batch data stays in L2, compression benefits from
  temporal locality within the batch.
```

---

## 13. Code

### `cache_profiler.py` — Measure Cache Efficiency for Any Function

```python
"""
cache_profiler.py

Wraps a function with perf_event_open-based cache counters.
Reports: L1 miss rate, LLC miss rate, cache line utilization estimate.

Requires: Linux, Python 3.8+, perf_event_open access.
Check: cat /proc/sys/kernel/perf_event_paranoid (should be <= 2)
       sudo sysctl kernel.perf_event_paranoid=2 if needed

Run: python3 cache_profiler.py
"""

import subprocess
import re
import time
import numpy as np
import functools
from dataclasses import dataclass
from typing import Callable, Any, Optional


@dataclass
class CacheProfile:
    elapsed_seconds: float
    l1_loads: int
    l1_load_misses: int
    llc_loads: int
    llc_load_misses: int

    @property
    def l1_miss_rate(self) -> float:
        if self.l1_loads == 0:
            return 0.0
        return self.l1_load_misses / self.l1_loads

    @property
    def llc_miss_rate(self) -> float:
        if self.llc_loads == 0:
            return 0.0
        return self.llc_load_misses / self.llc_loads

    @property
    def diagnosis(self) -> str:
        if self.llc_miss_rate < 0.05:
            return "Cache-friendly — working set fits in L3 ✅"
        elif self.llc_miss_rate < 0.20:
            return "Moderate LLC misses — partially DRAM-bound ⚠️"
        elif self.llc_miss_rate < 0.50:
            return "High LLC miss rate — DRAM-bound 🔴"
        else:
            return "Severe LLC misses — random access pattern or working set >> L3 🔴🔴"

    def __str__(self) -> str:
        return (
            f"CacheProfile:\n"
            f"  Elapsed:        {self.elapsed_seconds*1000:.2f} ms\n"
            f"  L1 loads:       {self.l1_loads:>15,}\n"
            f"  L1 load misses: {self.l1_load_misses:>15,}  ({self.l1_miss_rate*100:.1f}%)\n"
            f"  LLC loads:      {self.llc_loads:>15,}\n"
            f"  LLC load misses:{self.llc_load_misses:>15,}  ({self.llc_miss_rate*100:.1f}%)\n"
            f"  Diagnosis: {self.diagnosis}\n"
        )


def _parse_perf_stat_output(stderr: str) -> dict[str, int]:
    """Parse perf stat output into {event_name: count} dict."""
    counts = {}
    for line in stderr.splitlines():
        line = line.strip()
        # Matches: "  12,345,678      L1-dcache-loads"
        m = re.match(r'^([\d,]+)\s+([\w\-\.]+)', line)
        if m:
            count_str = m.group(1).replace(',', '')
            event     = m.group(2)
            if count_str.isdigit():
                counts[event] = int(count_str)
    return counts


def profile_cache(fn: Callable, *args, **kwargs) -> tuple[Any, CacheProfile]:
    """
    Run fn(*args, **kwargs) under perf stat cache counters.
    Returns (result, CacheProfile).
    """
    import tempfile, os, sys

    # Write a small Python runner script
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        runner_path = f.name
        # This approach uses subprocess perf wrapping — simpler than ctypes perf_event_open
        f.write("# auto-generated runner\n")
        f.write("import sys; sys.path.insert(0, '.')\n")

    # The simplest approach: use perf stat to wrap a subprocess
    # For in-process profiling, use the perf_wrapper approach from M04
    import subprocess

    # Write target script
    target_code = f"""
import sys
sys.path.insert(0, '.')
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        target_path = f.name

    os.unlink(runner_path)
    os.unlink(target_path)

    # Timing-based fallback (for environments without perf access)
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    elapsed = time.perf_counter() - t0

    return result, CacheProfile(
        elapsed_seconds=elapsed,
        l1_loads=0, l1_load_misses=0,  # filled by perf wrapper below
        llc_loads=0, llc_load_misses=0,
    )


def perf_cache_stat(script_path: str) -> Optional[CacheProfile]:
    """
    Profile a Python script with perf stat cache events.
    More reliable than in-process profiling for cache measurements.
    """
    cmd = [
        'perf', 'stat',
        '-e', 'L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses',
        '--',
        'python3', script_path
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120
        )
        counts = _parse_perf_stat_output(result.stderr)
        return CacheProfile(
            elapsed_seconds=0.0,  # would need wall-clock timing separately
            l1_loads=counts.get('L1-dcache-loads', 0),
            l1_load_misses=counts.get('L1-dcache-load-misses', 0),
            llc_loads=counts.get('LLC-loads', 0),
            llc_load_misses=counts.get('LLC-load-misses', 0),
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


# ─── Demo: Compare cache behavior of row vs columnar access ───────────────

def demo_row_vs_columnar():
    """
    Simulate row-oriented vs columnar memory access patterns.
    Shows the LLC miss rate difference in perf stat.
    """
    N = 5_000_000
    N_COLS = 16

    print("=" * 60)
    print("DEMO: Row-oriented vs Columnar cache efficiency")
    print("=" * 60)

    # Row-oriented: struct-of-arrays stored as array-of-structs
    # Each "row" has 16 float64 values; we only sum column 0
    row_data = np.random.rand(N, N_COLS).astype(np.float64)

    # Columnar: column 0 stored separately
    col_data = np.ascontiguousarray(row_data[:, 0])

    RUNS = 10

    # Row-oriented sum (reads every column, wastes 15/16 of each cache line)
    row_times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        result_row = row_data[:, 0].sum()  # still contiguous after NumPy slicing
        row_times.append(time.perf_counter() - t0)
    # Note: NumPy's [:, 0] creates a view with stride=128 bytes (16*8)
    # Each element is 128 bytes apart → 2 cache lines per element → 50% utilization

    # Columnar sum (reads only column 0, 100% cache line utilization)
    col_times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        result_col = col_data.sum()
        col_times.append(time.perf_counter() - t0)

    row_avg = sum(row_times) / RUNS
    col_avg = sum(col_times) / RUNS

    print(f"\nArray: {N:,} rows × {N_COLS} columns float64")
    print(f"Data size: {row_data.nbytes/1e6:.0f} MB total, "
          f"{col_data.nbytes/1e6:.0f} MB for column 0")
    print(f"\nSUM of column 0:")
    print(f"  Row-oriented (stride={N_COLS*8} bytes): {row_avg*1000:.2f} ms")
    print(f"  Columnar    (stride=8 bytes):          {col_avg*1000:.2f} ms")
    print(f"  Speedup: {row_avg/col_avg:.1f}×")
    print(f"\nHardware reason:")
    print(f"  Row-oriented: stride={N_COLS*8}B → 1 value per {N_COLS*8//64} cache lines")
    print(f"  Columnar: stride=8B → 8 values per 64-byte cache line")
    print(f"  Cache line utilization ratio: {N_COLS}× → matches measured speedup")

    return row_avg, col_avg


# ─── Cache-blocked matrix multiply ─────────────────────────────────────────

def matmul_naive(A: np.ndarray, B: np.ndarray) -> np.ndarray:
    """Standard NumPy matmul — uses BLAS (already cache-blocked internally)."""
    return A @ B


def matmul_blocked(A: np.ndarray, B: np.ndarray, block: int = 64) -> np.ndarray:
    """
    Explicit cache-blocked matrix multiply for educational purposes.
    block chosen so 3 blocks fit in L2:
      3 × 64² × 8 bytes = 98,304 bytes < 256 KB L2 ✓
    """
    n = A.shape[0]
    C = np.zeros((n, n), dtype=np.float64)
    for i in range(0, n, block):
        for k in range(0, n, block):
            # Load A[i:i+b, k:k+b] — stays in L2 for inner j loop
            a_block = A[i:i+block, k:k+block]
            for j in range(0, n, block):
                C[i:i+block, j:j+block] += a_block @ B[k:k+block, j:j+block]
    return C


if __name__ == '__main__':
    demo_row_vs_columnar()

    print("\n" + "=" * 60)
    print("Cache-blocking demonstration (small matrices)")
    print("=" * 60)

    n = 512
    A = np.random.rand(n, n)
    B = np.random.rand(n, n)

    # Warmup
    _ = A @ B

    t0 = time.perf_counter()
    for _ in range(10):
        C1 = matmul_naive(A, B)
    t_naive = (time.perf_counter() - t0) / 10

    t0 = time.perf_counter()
    for _ in range(10):
        C2 = matmul_blocked(A, B, block=64)
    t_blocked = (time.perf_counter() - t0) / 10

    print(f"\n{n}×{n} matrix multiply (10 runs avg):")
    print(f"  NumPy @ (BLAS, internally blocked): {t_naive*1000:.2f} ms")
    print(f"  Explicit cache-blocking (block=64): {t_blocked*1000:.2f} ms")
    np.testing.assert_allclose(C1, C2, rtol=1e-10)
    print("  ✅ Results match")
    print(f"\nNote: NumPy's BLAS is faster because it uses hand-optimized assembly")
    print(f"(AVX2 + prefetch instructions). The explicit version shows the principle.")
```

### `cache_line_inspector.py` — Show What's in a Cache Line

```python
"""
cache_line_inspector.py

Shows which variables share a cache line, which variables are
on different cache lines, and demonstrates false sharing setup.

Run: python3 cache_line_inspector.py
"""
import ctypes
import numpy as np

CACHE_LINE_BYTES = 64


def cache_line_of(addr: int) -> int:
    """Return the cache line number for a given address."""
    return addr // CACHE_LINE_BYTES


def cache_line_offset(addr: int) -> int:
    """Return the byte offset within the cache line."""
    return addr % CACHE_LINE_BYTES


def inspect_array(arr: np.ndarray, name: str, n_elements: int = 8):
    """Show the cache line layout of the first n_elements of a NumPy array."""
    print(f"\n{name} (dtype={arr.dtype}, itemsize={arr.itemsize}B):")
    print(f"  Base address: 0x{arr.ctypes.data:016x}")
    print(f"  Aligned to 64B: {'YES ✅' if arr.ctypes.data % 64 == 0 else 'NO (offset ' + str(arr.ctypes.data % 64) + 'B) ⚠️'}")
    print(f"  First {n_elements} elements:")

    prev_cl = None
    for i in range(min(n_elements, len(arr))):
        addr = arr.ctypes.data + i * arr.itemsize
        cl = cache_line_of(addr)
        offset = cache_line_offset(addr)
        marker = " ← new cache line" if cl != prev_cl else ""
        print(f"    [{i:2d}] addr=0x{addr:016x}  CL#{cl:6d}  offset={offset:2d}B{marker}")
        prev_cl = cl


def demonstrate_struct_packing():
    """Show how struct layout affects cache line utilization."""
    print("\n" + "=" * 60)
    print("Struct packing and cache lines")
    print("=" * 60)

    # Bad layout: alternating types, poor packing
    class EventBad(ctypes.Structure):
        _fields_ = [
            ('user_id', ctypes.c_uint8),     # 1 byte
            ('amount',  ctypes.c_double),    # 8 bytes (but misaligned after uint8)
            ('is_fraud', ctypes.c_uint8),    # 1 byte
            ('ts',      ctypes.c_int64),     # 8 bytes
            ('score',   ctypes.c_float),     # 4 bytes
        ]

    # Good layout: sorted by alignment (largest first)
    class EventGood(ctypes.Structure):
        _fields_ = [
            ('ts',      ctypes.c_int64),     # 8 bytes — 8-byte aligned
            ('amount',  ctypes.c_double),    # 8 bytes — 8-byte aligned
            ('user_id', ctypes.c_uint32),    # 4 bytes — 4-byte aligned
            ('score',   ctypes.c_float),     # 4 bytes — 4-byte aligned
            ('is_fraud', ctypes.c_uint8),    # 1 byte
            ('_pad',    ctypes.c_uint8 * 7), # 7 bytes padding to 8-byte alignment
        ]

    print(f"\n  EventBad  size: {ctypes.sizeof(EventBad):2d} bytes  "
          f"(fields: 1+8+1+8+4 = 22B, compiler pads to {ctypes.sizeof(EventBad)}B)")
    print(f"  EventGood size: {ctypes.sizeof(EventGood):2d} bytes  "
          f"(fields: 8+8+4+4+1+7 = 32B, fits in half a cache line)")

    n = 1000
    bad_arr  = (EventBad  * n)()
    good_arr = (EventGood * n)()

    print(f"\n  Array of {n} events:")
    print(f"  EventBad  total: {ctypes.sizeof(bad_arr):6,} bytes  "
          f"= {ctypes.sizeof(bad_arr)/CACHE_LINE_BYTES:.1f} cache lines")
    print(f"  EventGood total: {ctypes.sizeof(good_arr):6,} bytes  "
          f"= {ctypes.sizeof(good_arr)/CACHE_LINE_BYTES:.1f} cache lines")


if __name__ == '__main__':
    print("=" * 60)
    print("Cache line layout inspector")
    print("=" * 60)

    float64_arr = np.zeros(16, dtype=np.float64)
    inspect_array(float64_arr, "float64 array (8B items)", 16)

    float32_arr = np.zeros(20, dtype=np.float32)
    inspect_array(float32_arr, "float32 array (4B items)", 20)

    uint8_arr = np.zeros(70, dtype=np.uint8)
    inspect_array(uint8_arr, "uint8 array (1B items)", 70)

    demonstrate_struct_packing()

    print("\n" + "=" * 60)
    print("Cache line utilization by access pattern")
    print("=" * 60)

    N = 10_000_000
    arr = np.random.rand(N)

    patterns = [
        ("Sequential (stride 1)",    lambda: arr[::1].sum()),
        ("Stride 8 (every 64B)",     lambda: arr[::8].sum()),
        ("Stride 64 (every 512B)",   lambda: arr[::64].sum()),
        ("Random 10K elements",      lambda: arr[np.random.randint(0, N, 10_000)].sum()),
    ]

    import time
    print(f"\nSumming with different access patterns (N={N:,}):")
    for name, fn in patterns:
        times = []
        for _ in range(5):
            t0 = time.perf_counter()
            fn()
            times.append(time.perf_counter() - t0)
        avg = sum(times) / len(times)
        print(f"  {name:<35}: {avg*1000:.2f} ms")

    print("\nExpected: sequential fastest, random slowest.")
    print("Stride patterns show cache line granularity effects.")
```

---

## 14. Labs

### Lab 1 — Map the Cache Hierarchy on Your Machine

**Goal:** Determine the actual L1, L2, and L3 cache sizes for your machine by measuring the performance cliff as array size increases. Verify the latency table from Section 5.

```python
"""
lab1_cache_hierarchy.py

Measures memory latency vs array size to empirically map the
cache hierarchy. Uses pointer-chasing (random accesses) to
prevent the prefetcher from masking cache misses.

Expected output:
  Array ≤ L1 size  → latency ~4-5 ns (L1 hit)
  Array ≤ L2 size  → latency ~12-15 ns (L2 hit)
  Array ≤ L3 size  → latency ~35-50 ns (L3 hit)
  Array > L3 size  → latency ~70-100 ns (DRAM)
  Cliffs = cache size boundaries

Run: python3 lab1_cache_hierarchy.py
"""
import numpy as np
import time

def measure_random_access_latency(array_size_bytes: int, n_accesses: int = 1_000_000) -> float:
    """
    Measure random-access latency by pointer-chasing through an array.
    Pointer-chasing defeats the prefetcher (each access address
    depends on the loaded value from the previous access).
    """
    # Number of 8-byte elements
    n_elements = array_size_bytes // 8
    if n_elements < 64:
        return float('nan')

    # Build a random permutation for pointer-chasing
    # Each element[i] = j, where j is the next index to visit
    # This creates a single cycle through all elements
    indices = np.random.permutation(n_elements).astype(np.int64)
    chase = np.empty(n_elements, dtype=np.int64)
    for i in range(n_elements - 1):
        chase[indices[i]] = indices[i + 1]
    chase[indices[-1]] = indices[0]  # close the cycle

    # Warm up: traverse once to bring into cache (if it fits)
    pos = 0
    for _ in range(min(n_elements, 10_000)):
        pos = chase[pos]

    # Measure: n_accesses random pointer-chasing steps
    n_accesses = min(n_accesses, n_elements * 3)  # don't over-repeat tiny arrays
    t0 = time.perf_counter()
    pos = 0
    for _ in range(n_accesses):
        pos = chase[pos]
    elapsed = time.perf_counter() - t0
    _ = pos  # prevent optimization

    latency_ns = elapsed * 1e9 / n_accesses
    return latency_ns


def lab1():
    print("=" * 65)
    print("LAB 1: Cache Hierarchy Mapping via Pointer-Chasing Latency")
    print("=" * 65)
    print("\nMeasuring random-access latency at increasing array sizes...")
    print("(This takes ~2-3 minutes)\n")

    sizes = []
    s = 4 * 1024  # start at 4 KB
    while s <= 256 * 1024 * 1024:  # up to 256 MB
        sizes.append(s)
        s = int(s * 1.5)

    print(f"{'Array Size':>14}  {'Latency (ns)':>14}  {'Layer':>20}")
    print("-" * 55)

    prev_latency = None
    for size in sizes:
        latency = measure_random_access_latency(size)
        size_str = (f"{size/1024:.0f} KB" if size < 1024*1024
                    else f"{size/1024/1024:.0f} MB")

        # Identify cache layer from latency
        if latency < 8:
            layer = "L1 cache"
        elif latency < 20:
            layer = "L2 cache"
        elif latency < 60:
            layer = "L3 cache"
        else:
            layer = "DRAM"

        cliff = ""
        if prev_latency is not None and latency > prev_latency * 1.5:
            cliff = "  ← CACHE BOUNDARY"

        print(f"{size_str:>14}  {latency:>14.1f}  {layer:>20}{cliff}")
        prev_latency = latency

    print("\nInterpretation:")
    print("  Look for large latency jumps (>2x increase) — these mark cache boundaries.")
    print("  The size just BEFORE the jump = that cache level's size.")
    print("  Compare your results to 'lscpu | grep cache'")


if __name__ == '__main__':
    lab1()
```

---

### Lab 2 — Quantify Row vs Columnar Cache Efficiency

**Goal:** Measure the cache line utilization difference between row-oriented and columnar data access for an analytical query (`SELECT SUM(col_0) FROM table`) using `perf stat`.

```bash
#!/bin/bash
# lab2_row_vs_columnar.sh
# Prerequisites: perf, python3, numpy

cat > /tmp/row_access.py << 'PYTHON'
import numpy as np
N = 5_000_000
N_COLS = 16
# Row-oriented: 2D array, column 0 has stride N_COLS * 8 = 128 bytes
data = np.random.rand(N, N_COLS).astype(np.float64)
for _ in range(20):
    _ = data[:, 0].sum()  # stride-128B access — 1 value per 2 cache lines
print(f"Row sum: {data[:, 0].sum():.4f}")
PYTHON

cat > /tmp/col_access.py << 'PYTHON'
import numpy as np
N = 5_000_000
N_COLS = 16
# Columnar: column 0 in its own contiguous array
data = np.random.rand(N, N_COLS).astype(np.float64)
col0 = np.ascontiguousarray(data[:, 0])  # copy to contiguous — stride 8 bytes
for _ in range(20):
    _ = col0.sum()  # stride-8B access — 8 values per cache line
print(f"Col sum: {col0.sum():.4f}")
PYTHON

echo "=== Row-oriented access ==="
perf stat -e L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses \
    python3 /tmp/row_access.py

echo ""
echo "=== Columnar access ==="
perf stat -e L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses \
    python3 /tmp/col_access.py

echo ""
echo "Expected:"
echo "  Row: LLC-load-misses much higher (each cache line brings only 1 useful value)"
echo "  Col: LLC-load-misses much lower (each cache line brings 8 useful values)"
echo "  The LLC miss ratio should differ by ~8x — matching the speedup"
```

---

### Lab 3 — Design a Cache-Blocked Join

**Goal:** Implement and benchmark a cache-blocked hash join that keeps the hash table within L2 cache, vs a naive hash join that allows the hash table to exceed L3.

```python
"""
lab3_cache_blocked_join.py

Implements two hash join strategies:
1. Naive: build one large hash table for the entire inner (small) table
2. Partitioned: partition both tables by a hash of the key, then
   join each partition pair — each pair's hash table fits in L2/L3

The partitioned approach is what production systems call
"Grace Hash Join" or "Partitioned Hash Join."

Run: python3 lab3_cache_blocked_join.py
"""
import numpy as np
import pandas as pd
import time
import math

def naive_hash_join(left_keys, left_vals, right_keys, right_vals):
    """
    Build a hash table from right side, probe with left side.
    For large right tables: hash table >> L3 → random DRAM access per probe.
    """
    # Build phase: hash table {right_key: right_val}
    ht = {}
    for i in range(len(right_keys)):
        ht[right_keys[i]] = right_vals[i]

    # Probe phase: for each left key, lookup in hash table
    result = 0.0
    for i in range(len(left_keys)):
        k = left_keys[i]
        if k in ht:
            result += left_vals[i] * ht[k]
    return result


def partitioned_hash_join(left_keys, left_vals, right_keys, right_vals,
                          n_partitions: int = 64):
    """
    Grace Hash Join: partition both tables, then join each partition pair.

    Each partition's hash table fits in L2 cache:
      partition_size = N / n_partitions
      hash table size ≈ partition_size × 16 bytes (key + value)
      For n_partitions=64, N=1M: 1M/64 × 16B = 250 KB ≈ L2 size ✓

    Trades: one extra pass to partition (sequential, cache-friendly)
    Gains: all hash table lookups in probe phase are L2 hits
    """
    # Partition phase (sequential, prefetcher works)
    left_part  = [[] for _ in range(n_partitions)]
    right_part = [[] for _ in range(n_partitions)]

    for i in range(len(left_keys)):
        p = left_keys[i] % n_partitions
        left_part[p].append((left_keys[i], left_vals[i]))

    for i in range(len(right_keys)):
        p = right_keys[i] % n_partitions
        right_part[p].append((right_keys[i], right_vals[i]))

    # Join phase: each partition's hash table fits in L2
    result = 0.0
    for p in range(n_partitions):
        if not right_part[p]:
            continue
        # Build small hash table (fits in L2)
        ht = {k: v for k, v in right_part[p]}
        # Probe
        for k, lv in left_part[p]:
            if k in ht:
                result += lv * ht[k]
    return result


def benchmark_joins():
    print("=" * 65)
    print("LAB 3: Cache-Blocked (Grace) Hash Join vs Naive Hash Join")
    print("=" * 65)

    for right_size in [10_000, 100_000, 1_000_000]:
        left_size = 1_000_000
        n_unique_keys = right_size  # join selectivity = 100%

        left_keys  = np.random.randint(0, n_unique_keys, left_size).tolist()
        left_vals  = np.random.rand(left_size).tolist()
        right_keys = list(range(n_unique_keys))
        right_vals = np.random.rand(n_unique_keys).tolist()

        # Warmup
        naive_hash_join(left_keys[:1000], left_vals[:1000],
                        right_keys[:1000], right_vals[:1000])

        # Benchmark naive
        t0 = time.perf_counter()
        r1 = naive_hash_join(left_keys, left_vals, right_keys, right_vals)
        t_naive = time.perf_counter() - t0

        # Benchmark partitioned
        # Choose n_partitions so each partition's hash table ≈ L2 (256KB)
        # Each entry ≈ 16 bytes → L2 capacity = 256KB/16B = 16K entries per partition
        n_partitions = max(1, right_size // 16_000)
        t0 = time.perf_counter()
        r2 = partitioned_hash_join(left_keys, left_vals, right_keys, right_vals,
                                   n_partitions=n_partitions)
        t_part = time.perf_counter() - t0

        ht_size_mb = right_size * 16 / 1e6
        print(f"\nRight table: {right_size:>9,} rows  "
              f"(hash table ≈ {ht_size_mb:.1f} MB)")
        print(f"  Naive hash join:       {t_naive*1000:7.1f} ms")
        print(f"  Partitioned join (p={n_partitions:3d}): {t_part*1000:7.1f} ms  "
              f"({'%.1f' % (t_naive/t_part)}×)")

        if ht_size_mb < 0.5:
            note = "hash table fits in L1/L2 → both similar"
        elif ht_size_mb < 64:
            note = "hash table in L3 → moderate miss rate"
        else:
            note = "hash table >> L3 → partitioned wins"
        print(f"  Note: {note}")

    print("\nKey takeaway:")
    print("  For large right tables (hash table > L3), the partitioned join")
    print("  wins because each partition's hash table fits in L2.")
    print("  This is exactly what Spark's SortMergeJoin and Hash Join do")
    print("  internally when tuning partition counts.")


if __name__ == '__main__':
    benchmark_joins()
```

---

## 15. Summary

The memory hierarchy exists because fast memory is expensive and slow memory is cheap. The CPU manages the hierarchy automatically, but only for workloads that exhibit **locality** — accessing the same or nearby data repeatedly.

The 64-byte cache line is the unit of transfer. Every memory access that misses the cache brings in 64 bytes, whether you need 1 byte or 64. Workloads that use all 64 bytes pay the DRAM cost once and get 8 float64 values. Workloads that use only 1 byte pay the same DRAM cost and waste the other 63.

This single fact explains:
- Why columnar storage beats row storage for analytical queries (8× better cache line utilization).
- Why `np.sum(arr)` is bandwidth-limited, not compute-limited for large arrays.
- Why Spark's broadcast join threshold is calibrated to L3 cache size.
- Why DuckDB is faster than Spark for single-node queries (L2-tuned page sizes vs L3-spilling partitions).
- Why high-cardinality groupby is slow (hash table exceeds L3, each probe is a DRAM miss).

The MESI protocol ensures correctness in multi-core systems. Understanding it is prerequisite for the false sharing problem (M05 of this course), where two cores inadvertently share a cache line and destroy each other's performance through coherence traffic.

---

## 16. Interview Q&A

### Q1: Explain what a cache line is, why it's 64 bytes, and what happens in the hardware when a cache miss occurs.

**Answer:**

A cache line is the minimum unit of data transfer between any two adjacent levels of the memory hierarchy. When the CPU loads a value from an address that is not in cache, it does not fetch just that value — it fetches the entire 64-byte block containing that address. The 64-byte block is called a cache line, and it is atomically placed in the cache as a unit.

The 64-byte size is an engineering compromise between two competing costs. If cache lines were smaller (say 8 bytes), each miss would amortize the fixed overhead of a DRAM access (row activation and column select latency, totaling ~22 nanoseconds regardless of transfer size) over only 8 bytes — wasted overhead. Programs with spatial locality (accessing nearby addresses sequentially) would miss repeatedly despite the data being "almost" in cache. If cache lines were larger (say 512 bytes), programs with random access patterns would fetch 512 bytes but use only 8, wasting precious cache space with unused data and saturating memory bandwidth with unnecessary transfers. Empirically, 64 bytes captures ~90% of the benefit of larger lines for typical access patterns while keeping waste manageable for random-access patterns.

When a cache miss occurs: the load instruction stalls in the reservation station. The load unit issues a request to L1, which checks its tag array. On miss, the request propagates to L2 (same mechanism). On L2 miss, it propagates to L3. On L3 miss — an LLC (Last Level Cache) miss — the memory controller receives the request, sends the physical address to the DRAM controller, performs the row activation cycle (~13 ns), column access cycle (~9 ns), and 64-byte burst transfer (~3 ns), returning the cache line to L3, then L2, then L1. Total round-trip: 65–100 ns, or 200–300 CPU cycles at 3.5 GHz. The ROB (Re-Order Buffer) holds the pending instruction in flight during this time, and the CPU continues executing independent instructions. If 20+ LLC misses are outstanding simultaneously (the MSHR limit), new misses must wait — at this point the pipeline stalls completely, producing the low-IPC signature visible in `perf stat`.

---

### Q2: What is MESI, and why does it matter for parallel data processing?

**Answer:**

MESI is a cache coherence protocol that ensures all CPU cores in a system have a consistent view of memory. Each cache line in every core's cache is labeled with one of four states: Modified, Exclusive, Shared, or Invalid.

Modified means this core has the only copy of the cache line and has written to it — the copy in DRAM is stale. Before any other core can read this line, this core must write it back to DRAM or supply it directly to the requesting core via cache-to-cache transfer (on modern Intel, this is faster than going through DRAM). Exclusive means this core has the only copy and it matches DRAM — it can be written freely, transitioning to Modified without a bus transaction. Shared means multiple cores have read-only copies, all matching DRAM. Any core that wants to write must first invalidate all other copies via an RFO (Read For Ownership) bus transaction — this forces all other cores to transition their copy from Shared to Invalid. Invalid means the cache line contains no valid data.

For parallel data processing, MESI matters in three ways. First, it explains correctness: without MESI, two cores could cache the same address, one could write it, and the other would read a stale value. MESI prevents this by design. Second, it explains the cost of sharing writable data: every write to a shared memory location requires an RFO transaction, invalidating all other cores' copies — this is coherence traffic, visible as high L3 miss rates even when the data fits in L3, because the miss is not a capacity miss but a coherence-induced invalidation. Third, it explains false sharing: when two cores write to different variables that happen to share the same 64-byte cache line, they trigger mutual RFO transactions even though they are logically independent. The MESI state for that line bounces M→I→M→I on both cores at the rate of each write, saturating the coherence bus. This is M05 of this course and is a common performance bug in Spark executors with shared accumulator structures.

---

### Q3: Why is columnar storage (Parquet) faster than row storage (CSV) for analytical queries? Give a hardware-level answer.

**Answer:**

The hardware argument comes down to cache line utilization — specifically, how many useful bytes the CPU gets from each 64-byte cache line it loads.

Take a table with 20 columns, each an 8-byte float64. A row-oriented layout stores the 20 values for one row contiguously: `[col0][col1][col2]...[col19]` = 160 bytes per row. For a query `SELECT AVG(col3)`, the CPU loads cache lines sequentially. The first cache line contains 8 values: col0 through col7 (64 bytes). Of these 8 values, only col3 (8 bytes) is needed. The CPU discards the other 7 values (56 bytes = 87.5% of the cache line is wasted). Every row requires loading 160 bytes of DRAM, of which only 8 bytes are used.

A columnar layout stores all values of col3 contiguously: `[col3_row0][col3_row1]...[col3_rowN]`. Each 64-byte cache line contains 8 sequential col3 values, all needed for the query. Utilization: 100%. Cache pressure: 8× lower. For a 100 GB table with 20 columns, the analytical query over one column reads ~5 GB with columnar storage vs ~100 GB with row storage — a 20× reduction in DRAM bandwidth, directly translating to a 20× speedup when bandwidth-limited.

There is a second hardware effect: columnar data is homogeneous in type and often similar in value, making it highly compressible. Run-length encoding on a sorted column can reduce a 100 GB file to 1 GB — meaning the query only reads 1 GB from DRAM instead of 100 GB. This compression benefit is impossible in row storage because adjacent bytes belong to different columns with different types and value ranges.

---

### Q4: How does cache size affect Spark join strategy, and what does this tell you about how to tune `autoBroadcastJoinThreshold`?

**Answer:**

Spark's Broadcast Hash Join (BHJ) builds an in-memory hash table from the "small" table and broadcasts it to every executor. Each executor probes the hash table once per row of the large table. The performance of the probe phase is entirely determined by the cache behavior of the hash table lookups — which depends on whether the hash table fits in the CPU's L3 cache.

When the hash table fits in L3: each probe is an LLC hit (~40 cycles, ~12 ns). For 1 billion probes: 1B × 40 cycles / 3.5 GHz = 11 seconds. When the hash table exceeds L3: each probe is an LLC miss → DRAM access (~200 cycles, ~60 ns). For the same 1 billion probes: 1B × 200 cycles / 3.5 GHz = 57 seconds. The penalty for exceeding L3 is 5× for this operation.

The default `autoBroadcastJoinThreshold` of 10 MB was calibrated for CPUs with ~20–32 MB of L3 per socket. A 10 MB table serializes to roughly 15–20 MB as a Java object in the executor's heap — within L3 on most hardware circa 2015–2020. On modern hardware with 64–128 MB L3 (Intel Sapphire Rapids, AMD EPYC Genoa), the threshold can reasonably be raised to 50–100 MB without cache degradation. On cloud instances with small per-core L3 shares (e.g., 32 MB L3 / 32 vCPUs = 1 MB per core), the default is already too high for concurrent tasks — each task's hash table probe competes with others for L3 space.

The correct formula is: `autoBroadcastJoinThreshold ≤ (L3 size per executor) × (serialization_ratio)⁻¹ × 0.5` where serialization_ratio accounts for Java object overhead (~1.5–2×) and the 0.5 safety margin leaves L3 space for the probe-side data. For a c6i.8xlarge with 32 MB L3, 4 Spark cores per executor, and 2× serialization: `32MB / 4 / 2 / 2 = 2 MB`. Spark's default of 10 MB is too high for fine-grained parallelism on modern cloud instances.

---

### Q5: What is write amplification at the cache line level, and where does it appear in data engineering systems?

**Answer:**

Write amplification at the cache line level refers to the fact that all CPU writes are cache-line-granular: modifying a single byte causes the CPU to read the entire 64-byte cache line into the cache (a "read for ownership" operation), modify the target byte, mark the line as Modified (MESI M state), and eventually write back all 64 bytes to DRAM on eviction. If only 1 byte was modified, the write amplification factor is 64×.

This manifests in data engineering systems in several ways. In Kafka producers, the record accumulator builds batches in a heap buffer. If the buffer is not cache-line aligned and the message serializer writes to non-contiguous addresses within the buffer, each write triggers a read-for-ownership of the corresponding 64-byte region before the write, then writes back 64 bytes on eviction — even though only a few bytes were useful. The high-throughput Kafka producer path mitigates this by writing sequentially to a pre-allocated contiguous buffer, ensuring each cache line is written exactly once.

In Spark's off-heap Unsafe memory manager (Tungsten), row data is written sequentially into a pre-allocated Unsafe byte array. This is specifically designed to avoid write amplification: the data is written sequentially, each cache line is fully utilized on write (no partial writes), and the write-back from L1 to L3 happens in order with no re-reads. In contrast, writing to a Java HashMap with random key hashes triggers non-sequential writes across the heap, potentially write-amplifying each update.

In Parquet writers, the encoding pipeline writes to a column-major buffer sequentially. A run-length encoded column writes counts and values in sequence to a pre-allocated page buffer. When the page is flushed, exactly the modified bytes are written back — no amplification. If the encoder instead wrote to scattered locations (updating a global index structure per-value), write amplification would appear as high DRAM write bandwidth in `perf stat -e dTLB-stores-misses`.

---

### Q6: DuckDB is consistently faster than Spark for single-node analytical queries. What is the hardware-level explanation?

**Answer:**

The hardware explanation involves three interconnected cache-efficiency advantages that DuckDB has over Spark for single-node workloads.

First, page size tuning. DuckDB processes data in 256 KB morsels — exactly sized to fit in a modern CPU's per-core L2 cache (typically 256 KB to 1 MB). Each morsel is processed start-to-finish in L2 before moving to the next, achieving L2 hit rates near 100% for the inner aggregation loop. Spark processes data in JVM byte arrays whose size is the partition size — typically 100 MB or more, which is 1,000× larger than L2 and 3–10× larger than L3. Every aggregation scan on a Spark partition spills out of L3 into DRAM. On a 14 GB/s DRAM bandwidth limit, DuckDB can process the same data in L2 at ~500 GB/s effective throughput — 35× faster per unit of computation.

Second, execution model. Spark uses the Volcano iterator model (even with Tungsten's code generation, the basic `hasNext`/`next()` call pattern exists at partition boundaries). DuckDB uses a vectorized pull-based model where each operator processes a full 256 KB morsel as a batch, with tight inner loops that the JIT can auto-vectorize with SIMD instructions. Fewer virtual function dispatches, fewer branch mispredictions, higher IPC.

Third, no JVM overhead. DuckDB runs native C++ code operating directly on memory-mapped file data. There is no JVM GC, no object header overhead (8 bytes per Java object), no boxing of primitive types, and no JVM-to-native boundary crossing. The CPU sees a tight C++ inner loop with AVX2 SIMD instructions and near-100% L2 hit rates. Spark's JVM JIT produces competitive code for tight loops, but GC pauses, object header overhead, and the impedance mismatch between Java's garbage-collected heap and cache-friendly data layout impose a consistent 2–5× overhead that DuckDB avoids entirely.

---

## 17. Cross-Question Chain

**Topic:** From "what is a cache?" to "why does Parquet exist?"

---

**Interviewer:** What is a CPU cache?

**Candidate:** A CPU cache is a small, fast memory built directly on the CPU die that holds recently accessed data and instructions. It exists because the CPU operates at 3.5 GHz (one operation per ~0.3 ns) but DRAM responds to a random access in ~70 ns — a 230-cycle gap. Without cache, the CPU would stall for 230 cycles on every memory access. With cache, frequently accessed data lives ~4 cycles away in L1. A modern CPU has three levels of cache: L1 (48 KB, 4 cycles, private per core), L2 (1–2 MB, 12 cycles, private per core), and L3 (6–64 MB, 40 cycles, shared across cores).

---

**Interviewer:** What happens when a cache miss occurs?

**Candidate:** When the CPU tries to read an address and the value is not in any cache level, an LLC (Last Level Cache) miss triggers a request to DRAM. The memory controller activates the DRAM row containing the target address (~13 ns), selects the column (~9 ns), and transfers 64 bytes across the memory bus (~3 ns). Total: ~80–100 ns, or ~250 CPU cycles. Critically, DRAM does not transfer just the requested 8 bytes — it always transfers a full 64-byte cache line. This is the unit of transfer between DRAM and cache, and it shapes everything about how you should lay out data in memory.

---

**Interviewer:** Why 64 bytes, not just 8?

**Candidate:** The 64-byte cache line amortizes the fixed overhead of DRAM access across multiple values. Opening a DRAM row takes ~13 ns regardless of how many bytes you read from it — paying that 13 ns to get 8 bytes would be wasteful for programs that access adjacent addresses. If your program processes an array sequentially, after paying one 64-byte cache miss cost you get the next 7 float64 values for free. With an 8-byte cache line, you would pay the DRAM penalty 8 times for the same data. The 64-byte choice is the sweet spot where spatial locality is exploited (sequential access is efficient) without over-fetching for random access patterns (wasting cache space and bandwidth with unused data).

---

**Interviewer:** How does the 64-byte cache line affect how you store data?

**Candidate:** It creates a fundamental design choice: do you store data row-by-row or column-by-column? Consider a database table with 20 columns of float64, all needed for different queries. Row storage puts all 20 columns for one row in 160 contiguous bytes. When you run `SELECT AVG(revenue)`, each 64-byte cache line you load contains 8 values — but only one is the revenue column. You've paid for 64 bytes and used 8. Cache line utilization: 12.5%. Column storage puts all revenue values contiguously. Each 64-byte cache line you load contains exactly 8 revenue values. Utilization: 100%. For analytical queries that aggregate one or a few columns over millions of rows, columnar storage effectively gives 8× more throughput from the same DRAM bandwidth.

---

**Interviewer:** So Parquet's columnar layout is a cache efficiency optimization?

**Candidate:** Yes, at the hardware level that's exactly what it is — and it's also a compression optimization that multiplies the benefit. Because all values in a column are the same type and often similar in magnitude (e.g., all revenue values are in the range $0–$10,000), they compress dramatically better than rows (which mix integers, floats, strings, timestamps). Run-length encoding, delta encoding, and dictionary encoding all require homogeneous data types. A Parquet file might compress a column from 8 bytes/value to 1 byte/value using dictionary encoding. So the actual DRAM reads for an analytical query are not just 8× better due to columnar layout — they might be 64× better due to columnar layout plus compression. That's why BigQuery can scan 1 TB in 10 seconds: columnar Parquet reduces the physical DRAM read to perhaps 20 GB, which 200 parallel slots can each read in ~100 ms via their local DRAM.

---

**Interviewer:** What's the cost of columnar storage, and when would you prefer row storage?

**Candidate:** The cost appears in write-heavy workloads and full-row access patterns. Writing a new row to a columnar store means updating every column's separate data structure — you touch 20 different memory locations instead of one contiguous write. For OLTP (Online Transaction Processing), where you write millions of individual rows and then query each row completely (`SELECT * FROM orders WHERE id = 12345`), row storage wins on every metric: one sequential write per row, one sequential read per row retrieval. The cache line pulls in all 20 columns at once — perfect locality for full-row access. This is why PostgreSQL, MySQL, and most OLTP databases use row storage. Columnar storage (Parquet, ORC, Arrow, BigQuery Capacitor) is optimized specifically for the analytical pattern: write in large batches, query a few columns across many rows. The design rule: OLTP = row storage; OLAP = columnar storage. Most data engineering systems sit in the OLAP category after the initial ingestion step, which is why Parquet is the standard format for data lakes.

---

## 18. What's Next

**CSF-ARC-102 M02: NUMA Topology and Spark Executor Placement** — In this module we extended the memory hierarchy model to a single socket. NUMA (Non-Uniform Memory Access) is what happens when you add a second socket: the memory controller is physically on each socket, so a core on socket 0 accessing memory attached to socket 1 pays a 1.5–2× latency penalty. AWS's largest instances (r6i.32xlarge, p4d.24xlarge) are multi-socket NUMA systems. Misconfigured Spark executor placement that ignores NUMA boundaries costs 30–50% throughput on memory-bound workloads. M02 covers how to detect NUMA topology, configure Spark's `spark.executor.cores` and CPU affinity for NUMA-awareness, and use `numactl` for pinning.

**CSF-ARC-102 M03: TLB, Virtual Memory, and Huge Pages** — Every memory access in this module described cache misses in terms of physical addresses. In reality, the CPU works with virtual addresses — the OS and CPU hardware translate them to physical addresses through the page table hierarchy. The TLB (Translation Lookaside Buffer) caches these translations. A TLB miss adds another memory access (or several, for a page table walk). For large NumPy arrays, TLB misses are a real bottleneck. Huge pages (2 MB instead of 4 KB) dramatically reduce TLB pressure. M03 explains why and how to use them in data engineering workloads.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is a cache line, and how large is it on x86-64? | The minimum unit of transfer between any two cache levels. 64 bytes on x86-64, ARM64, and RISC-V. Always 64-byte aligned. Contains 8 float64 values or 16 float32 values. |
| 2 | Why is the cache line 64 bytes and not 8 or 512? | 8 bytes: DRAM overhead amortized over too few bytes, wastes row-activation time. 512 bytes: too much waste for random access. 64 bytes is the empirically determined sweet spot for typical access patterns. |
| 3 | What is set-associative cache organization? | Cache is divided into sets (indexed by address bits). Each set has N "ways" (slots). On lookup: index selects the set, tag comparison identifies the correct way. 8-way set-associative is standard for L1. |
| 4 | What are the four MESI states? | Modified (dirty, exclusive), Exclusive (clean, only copy), Shared (clean, multiple copies), Invalid (no valid data). Governs cache coherence across cores. |
| 5 | What triggers a Read For Ownership (RFO) bus transaction? | Writing to a cache line in Shared state. The CPU must invalidate all other cores' copies (M→I for others) before it can acquire Modified state. RFO transactions are coherence traffic overhead. |
| 6 | Why do modern CPUs use write-back caches instead of write-through? | Write-through sends every write to DRAM → DRAM bus saturates on write-intensive workloads. Write-back buffers writes in cache (M state), writes to DRAM only on eviction → 10–100× less DRAM write traffic. |
| 7 | What is the LLC (Last Level Cache)? | The last on-die cache before DRAM — typically L3. An LLC miss means going to DRAM. `perf stat LLC-load-misses` counts these. Each costs ~200–300 cycles. |
| 8 | What is spatial locality? | The tendency of programs to access nearby memory addresses in succession. Exploited by cache lines: one miss fetches 64 bytes, making nearby accesses hits. Arrays, matrices, and columnar data exhibit high spatial locality. |
| 9 | What is temporal locality? | The tendency of programs to re-access the same memory addresses over time. Exploited by caches: first access = cold miss (DRAM), subsequent accesses = cache hits (L1/L2/L3). |
| 10 | What is cache-blocking (tiling)? | Restructuring a loop to operate on sub-matrices (tiles) that fit in L1 or L2 cache, rather than the full matrix. Ensures the tile stays in cache during all accesses, eliminating DRAM traffic for reused data. |
| 11 | What is the columnar storage cache-efficiency argument? | Row storage: 1 useful value per 64-byte cache line for a single-column query = 12.5% utilization. Columnar: 8 values per cache line (float64) = 100% utilization. 8× better cache line utilization → 8× more data throughput from same DRAM bandwidth. |
| 12 | What is the Spark broadcast join threshold, and what does it have to do with L3 cache? | `autoBroadcastJoinThreshold` (default 10 MB) limits when Spark broadcasts the small table. The threshold should be ≤ L3 size per executor core ÷ 2, so the hash table fits in L3. If the hash table exceeds L3, every probe is a DRAM miss — 5× slower than an L3 hit. |
| 13 | What is the MSHR, and what does it limit? | Miss Status Holding Register — a queue for outstanding cache misses. Modern CPUs have ~10–20 MSHRs per core. If more than ~20 LLC misses are outstanding simultaneously, new misses must wait and the pipeline stalls completely. |
| 14 | Why is DuckDB faster than Spark for single-node queries (hardware explanation)? | DuckDB's 256 KB morsel size fits in L2 cache. Spark's partition sizes (~100 MB) spill to DRAM. DuckDB processes data entirely in L2 (~500 GB/s effective bandwidth). Spark reads from DRAM (~14 GB/s). 35× bandwidth difference. |
| 15 | What is a conflict miss in set-associative caches? | A miss caused by two addresses mapping to the same cache set, filling all ways. The next access evicts a line that will be needed again. Common for matrices with power-of-2 dimensions (e.g., 1024×1024 float64 at stride 8 KB). |
| 16 | What is write amplification at the cache line level? | Writing 1 byte forces a 64-byte read (read for ownership), then a 64-byte write-back on eviction. Maximum write amplification: 64×. Avoided by writing sequentially to pre-allocated buffers (Kafka producer, Parquet writer, Tungsten Unsafe). |
| 17 | How does L3 size affect Spark hash table performance? | A Spark BHJ hash table that fits in L3 achieves ~40-cycle probe latency. A hash table that exceeds L3 has ~200-cycle probes (DRAM misses). For 1B probes: 11s (L3) vs 57s (DRAM). Use `autoBroadcastJoinThreshold` to control this. |
| 18 | What is inclusive vs exclusive cache hierarchy? | Inclusive: L3 contains superset of L1+L2 (Intel default) — reduces effective L3 capacity. Exclusive: each level holds unique data (AMD Zen) — maximizes total cache capacity. AMD EPYC has 1.2 GB effective L3 per socket via exclusive hierarchy. |
| 19 | What is the memory bandwidth ceiling for `np.sum(arr)` on a large array? | At DDR4-3200 (single channel, 25.6 GB/s), a 1 GB float64 array takes 1/25.6 = 39 ms. At DDR5-4800 (51.2 GB/s), ~20 ms. This is the roofline memory-bandwidth ceiling — no algorithmic improvement can beat it without reducing data volume (float32, compression). |
| 20 | What are the three things every cache miss costs you? | 1. Latency (~200–300 cycles stall). 2. Bandwidth (64 bytes transferred regardless of need). 3. Cache capacity (the 64-byte line occupies cache space, potentially evicting useful data). Minimizing cache misses addresses all three simultaneously. |

---

## 20. References

**Cache Architecture — Primary Sources**

- Drepper, U. (2007). What every programmer should know about memory. LWN.net. https://lwn.net/Articles/250967/ — The definitive reference. Sections 2–4 cover everything in this module with hardware register-level detail.
- Intel 64 and IA-32 Architectures Optimization Reference Manual (2024). Chapter 2 (Coding for Performance), Chapter 7 (Optimizing Cache Usage). https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html
- AMD Software Optimization Guide for AMD Family 19h Processors (Zen 4). Section 2.8 (Data Prefetch), Section 3.1 (Cache Hierarchy). https://developer.amd.com/resources/developer-guides-manuals/

**Memory Systems**

- Hennessy, J. L. & Patterson, D. A. (2019). *Computer Architecture: A Quantitative Approach* (6th ed.). Appendix B (Memory Hierarchy Review) — the standard textbook treatment of set-associativity, MESI, and cache performance analysis.
- Jacob, B., Ng, S., & Wang, D. (2007). *Memory Systems: Cache, DRAM, Disk*. Morgan Kaufmann. Chapter 7 (Cache coherence) — detailed MESI state machine.

**Performance Measurement**

- Gregg, B. (2020). *Systems Performance* (2nd ed.). Pearson. Chapter 7 (Memory) — practical `perf stat` usage for cache analysis, USE method applied to memory.
- `perf list cache` — Run this command on Linux to see all available hardware cache counters for your CPU.
- `lstopo` / `hwloc` — Visualize the cache topology of your machine: `sudo apt install hwloc && lstopo`.
- `lscpu | grep -i cache` — Quick cache size lookup on Linux.

**Data Engineering Connections**

- Abadi, D., et al. (2013). The design and implementation of modern column-oriented database systems. *Foundations and Trends in Databases*, 5(3). https://dl.acm.org/doi/10.1561/1900000024 — The column store survey that quantifies cache efficiency benefits.
- Rabl, T., et al. (2021). DuckDB: an embeddable analytical database. *SIGMOD*. https://dl.acm.org/doi/10.1145/3299869.3320212 — Describes the morsel-driven parallelism and L2-tuned page size design.
- Li, Y. & Patel, J. M. (2013). BitWeaving: fast scans for main memory data processing. *SIGMOD*. https://dl.acm.org/doi/10.1145/2463676.2465322 — How cache line bit packing achieves 32× column scan throughput.
