# M05: False Sharing and Cache Coherence

**School:** CS Foundations (CSF)  
**Course:** CSF-ARC-102 — Memory Architecture  
**Module:** 05 of 05  
**Difficulty:** ★★★★☆  
**Estimated study time:** 5–6 hours  
**Last updated:** 2026-06-27

---

## Table of Contents

1. [Why This Module Exists](#1-why-this-module-exists)
2. [Prerequisites](#2-prerequisites)
3. [Learning Objectives](#3-learning-objectives)
4. [First Principles](#4-first-principles)
5. [Architecture — MESI Protocol and Coherence Hardware](#5-architecture--mesi-protocol-and-coherence-hardware)
6. [Deep Dive — False Sharing Mechanics](#6-deep-dive--false-sharing-mechanics)
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

You can write a parallel program that has no race conditions, no lock contention, no shared mutable state — every thread writes only to its own variable — and still see near-zero parallelism speedup. This module explains why.

The cause is **false sharing**: two CPU cores writing to logically independent variables that happen to occupy the same 64-byte cache line. The MESI cache coherence protocol treats the cache line as an atomic unit. When core A writes its variable and core B writes its variable, the protocol bounces the entire cache line between them on every write — M→I on one core, then M→I on the other, endlessly. The coherence bus saturates. Effective throughput drops to near-serial. The two cores are fighting each other over 64 bytes despite never actually sharing data.

This is the most counterintuitive performance problem in parallel data engineering. It strikes in Spark shuffle write buffers, Python multiprocessing counter arrays, JVM `AtomicLong[]` metrics, and any Cython or C extension that packs per-thread state into a struct. The fix requires understanding what the hardware actually does, not just what the language memory model says.

False sharing is also the gateway to understanding why cache coherence protocols exist at all, how they interact with NUMA (M02), and why the JVM's memory model specifies `volatile` and `AtomicLong` in terms of hardware visibility guarantees rather than just software correctness.

---

## 2. Prerequisites

- CSF-ARC-102 M01: Cache lines (64 bytes, the minimum coherence unit), MESI states introduced.
- CSF-ARC-102 M02: NUMA topology (coherence traffic travels different paths on single-socket vs cross-socket).
- CSF-ARC-101 M03 or equivalent: Basic parallel programming concepts (threads, cores, shared memory).

---

## 3. Learning Objectives

By the end of this module you will be able to:

1. State the four MESI states and the transitions that a write triggers from each starting state.
2. Explain exactly what happens at the hardware level when two cores write to the same cache line simultaneously.
3. Define false sharing precisely and distinguish it from true sharing.
4. Predict whether a data structure layout will cause false sharing from its field sizes and alignment.
5. Use `perf stat -e cache-misses` and `perf c2c` to detect false sharing in a running program.
6. Apply padding, alignment, and thread-local-then-merge patterns to eliminate false sharing.
7. Explain why `volatile` in Java and `_Atomic` in C map to specific hardware coherence operations.
8. Identify false sharing risks in Spark accumulators, Python multiprocessing arrays, and JVM metric counter arrays.

---

## 4. First Principles

### Why Caches Need Coherence

In M01, we established that each CPU core has a private L1 and L2 cache. If two cores both cache the same memory address and one core writes a new value, the other core's cached copy is now stale — it holds the old value. If the second core reads from its stale cache, it gets wrong data. This is the **cache coherence problem**.

The solution is a cache coherence protocol: a distributed agreement among all CPU caches about which core "owns" each cache line and which copies are valid. The most widely implemented protocol on x86-64 is **MESI** (Modified, Exclusive, Shared, Invalid). Every cache line in every cache has a 2-bit MESI tag at all times.

The protocol guarantees: at any moment, at most one core has a Modified (dirty) copy of any cache line. All other cores either have Shared (clean, read-only) copies or Invalid (stale/absent) copies. This invariant is maintained by the **cache coherence interconnect** — the ring bus on single-socket Intel CPUs, Intel UPI for multi-socket, AMD Infinity Fabric for EPYC.

### The Cost of Coherence

Maintaining this invariant is not free. When core A wants to write to a cache line that another core B holds in Shared state, core A must:

1. Send an **invalidation** message to core B's cache controller.
2. Wait for B's acknowledgement (B's copy is now Invalid).
3. Receive the cache line from B (if B had Modified) or from L3/DRAM.
4. Set its own copy to Modified.
5. Perform the write.

This is called a **Read For Ownership (RFO)** request. On a single socket with a shared L3, the RFO round-trip takes ~40–60 cycles (L3 latency). On a multi-socket system (cross-UPI), it takes ~300–500 cycles — the same cost as a DRAM access.

For true sharing (two cores legitimately sharing data), this cost is unavoidable and necessary. For false sharing, this cost is paid even though the two cores are writing completely independent values — the protocol cannot distinguish at sub-cache-line granularity.

### The 64-Byte Granularity Problem

The entire coherence protocol operates at cache line granularity: 64 bytes. There is no sub-line coherence in hardware. If variable A is at offset 0 and variable B is at offset 8 within the same 64-byte cache line:

- Core 0 writes `A` → the entire 64-byte line goes Modified on core 0, Invalid on all other cores.
- Core 1 writes `B` → core 1 issues RFO for the entire 64-byte line, core 0's copy goes Invalid, core 1's copy goes Modified.
- Core 0 writes `A` again → RFO again. Core 1's copy goes Invalid.

This is false sharing. The hardware enforces coherence on the entire line even though the writes are to different bytes within it.

---

## 5. Architecture — MESI Protocol and Coherence Hardware

### 5.1 MESI State Machine

```
                    MESI STATE MACHINE (per cache line, per core)
                    ═══════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│           ┌──────────────────────────────────────────┐             │
│           │                                          │             │
│           │  ┌──────────┐    local write    ┌──────────┐           │
│           │  │          │ ────────────────► │          │           │
│           │  │  SHARED  │                   │ MODIFIED │           │
│           │  │    (S)   │ ◄──────────────── │    (M)   │           │
│           │  │          │   other core read │          │           │
│           │  └──────────┘   (writeback +    └──────────┘           │
│           │       │          share)              │                 │
│           │       │ other core write             │ other core write │
│           │       │ (invalidate)                 │ (invalidate)    │
│           │       ▼                              ▼                 │
│           │  ┌──────────┐                   ┌──────────┐           │
│           │  │          │                   │          │           │
│           │  │ INVALID  │ ◄─────────────────│ INVALID  │           │
│           │  │    (I)   │                   │    (I)   │           │
│           │  │          │                   │          │           │
│           │  └──────────┘                   └──────────┘           │
│           │       │                              ▲                 │
│           │       │ local read (cache miss)      │                 │
│           │       │ → fetch from L3/DRAM/peer    │                 │
│           │       ▼                              │                 │
│           │  ┌──────────┐    local write         │                 │
│           │  │          │ ───────────────────────┘                 │
│           │  │EXCLUSIVE │  (no RFO needed: only copy)             │
│           │  │    (E)   │                                          │
│           │  │          │                                          │
│           │  └──────────┘                                          │
│           │                                                        │
│           └────────────────────────────────────────────────────────┘
│                                                                     │
│  State meanings:                                                    │
│    M (Modified):   Only this core has the line. Dirty (DRAM stale).│
│                    Can write freely. Must writeback on eviction.   │
│    E (Exclusive):  Only this core has the line. Clean (= DRAM).   │
│                    Can write without RFO (transitions to M).       │
│    S (Shared):     Multiple cores have clean copies. Read-only.    │
│                    Write requires RFO → invalidates all S copies.  │
│    I (Invalid):    No valid copy in this cache. Next access = miss.│
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 Coherence Hardware

```
SINGLE-SOCKET INTEL (Alder Lake, Sapphire Rapids):

  Core 0          Core 1          Core 2          Core 3
  ┌───────┐       ┌───────┐       ┌───────┐       ┌───────┐
  │ L1/L2 │       │ L1/L2 │       │ L1/L2 │       │ L1/L2 │
  │ cache │       │ cache │       │ cache │       │ cache │
  └───┬───┘       └───┬───┘       └───┬───┘       └───┬───┘
      │               │               │               │
      └───────────────┴───────────────┴───────────────┘
                              │
                   ┌──────────┴──────────┐
                   │   L3 Cache + LLC     │
                   │   Coherence Agent   │
                   │   (Snoop Filter)    │
                   └──────────┬──────────┘
                              │
                           DRAM

  Coherence mechanism: snoop filter in L3 tracks which cores
  have which cache lines. On write: L3 sends targeted invalidations
  (not broadcast) to only the cores that have the line.
  RFO round-trip: ~40–60 cycles (L3 to core and back).

TWO-SOCKET INTEL (via UPI):

  Socket 0 (cores 0-31)              Socket 1 (cores 32-63)
  ┌─────────────────────┐            ┌─────────────────────┐
  │ L3 + Coherence Agent│◄──UPI─────►│ L3 + Coherence Agent│
  │  (~41 GB/s each way)│            │                     │
  └─────────────────────┘            └─────────────────────┘

  Cross-socket RFO: ~300–500 cycles.
  False sharing across sockets is ~5–8× worse than same-socket.
```

### 5.3 False Sharing Illustrated

```
MEMORY LAYOUT (one 64-byte cache line):
  Offset:  0    8    16   24   32   40   48   56
           [A₀ ][A₁ ][A₂ ][A₃ ][A₄ ][A₅ ][A₆ ][A₇ ]
                          ← 64 bytes = 1 cache line →

Case 1 — NO false sharing (independent cache lines):
  Thread 0 writes counter0 at address 0x1000  (cache line 0x1000..0x103F)
  Thread 1 writes counter1 at address 0x1040  (cache line 0x1040..0x107F)
  → Different cache lines → no coherence traffic between threads.

Case 2 — FALSE SHARING (same cache line, different bytes):
  Thread 0 writes counter0 at address 0x1000  (offset 0 of line 0x1000)
  Thread 1 writes counter1 at address 0x1008  (offset 8 of same line!)
  
  Timeline:
  T=0:  Thread 0 writes counter0 → line enters M state on core 0
  T=1:  Thread 1 writes counter1 → RFO issued: core 0's copy → I
                                    line enters M state on core 1
  T=2:  Thread 0 writes counter0 → RFO issued: core 1's copy → I
                                    line enters M state on core 0
  T=3:  Thread 1 writes counter1 → RFO issued: core 0's copy → I
  ... forever ...

  Each write: ~40–60 cycles for RFO on same socket
              ~300–500 cycles for RFO cross-socket
  Sequential single-thread write: ~4 cycles (L1 hit)
  False sharing write: 40–500 cycles → 10–125× slower
```

---

## 6. Deep Dive — False Sharing Mechanics

### 6.1 The RFO Sequence in Detail

When core 1 attempts to write to a cache line that core 0 holds in Modified state:

```
Step 1: Core 1 L1 cache: LINE is INVALID → cache miss on write.
        Core 1 issues RFO to L3 coherence agent.

Step 2: L3 coherence agent: checks snoop filter.
        "Core 0 has this line in M state."
        Sends INVALIDATE to core 0.

Step 3: Core 0 receives INVALIDATE.
        L1 cache: M → I transition.
        Core 0 must write the line back to L3 (writeback).
        Core 0 sends ACKNOWLEDGEMENT to L3.

Step 4: L3 receives writeback from core 0.
        L3 now has the up-to-date line.
        L3 sends the line to core 1.

Step 5: Core 1 receives the line.
        L1 cache: I → M transition.
        Core 1 performs the write.

Total latency: steps 2-5 ≈ 40–60 cycles (same socket, L3 path).
               steps 2-5 ≈ 300–500 cycles (cross-socket, UPI path).

In the false sharing scenario:
  Core 0 wants to write A₀ (byte 0–7).
  Core 1 wants to write A₁ (byte 8–15).
  The hardware has NO IDEA they're writing different bytes.
  The RFO is for the entire 64-byte line regardless.
```

### 6.2 Detecting False Sharing with perf

False sharing produces a specific `perf` signature:

```bash
# Run with perf stat looking for LLC-load-misses under parallel write load
perf stat -e cache-references,cache-misses,LLC-loads,LLC-load-misses,\
             mem_inst_retired.all_loads,mem_inst_retired.all_stores \
    ./your_parallel_program

# Key signal: high LLC-load-misses rate despite small working set.
# If the working set fits in L3 (no DRAM misses expected) but
# LLC-load-misses is high → false sharing is invalidating L3 lines.

# More precise: perf c2c (cache-to-cache transfer analysis)
# Available on Linux 4.3+, requires root or perf_event_paranoid=0:
perf c2c record -- ./your_parallel_program
perf c2c report --stdio

# perf c2c output columns:
#   "Hitm" = Hit in Modified state (another core had the line Modified)
#   High Hitm count = false sharing confirmed.
```

The `perf c2c` tool was specifically designed for false sharing detection. It identifies the exact memory addresses where cache-to-cache transfers occur, letting you pinpoint the offending struct field.

### 6.3 The False Sharing vs True Sharing Distinction

**True sharing:** Multiple threads access and legitimately share the same data. Example: a lock variable that all threads read and write to acquire the lock. The coherence traffic is unavoidable — it enforces the serialization that makes the lock work correctly.

**False sharing:** Multiple threads access different data that happens to occupy the same cache line. Example: per-thread counters packed into an array without padding. The coherence traffic is entirely unnecessary — the threads are not logically sharing data at all.

```
struct Counters {           ← PACKED (false sharing if threads write to different fields)
    int64_t thread0_count;  // offset 0   ┐
    int64_t thread1_count;  // offset 8   │ All in same 64-byte cache line!
    int64_t thread2_count;  // offset 16  │
    int64_t thread3_count;  // offset 24  │
    int64_t thread4_count;  // offset 32  │
    int64_t thread5_count;  // offset 40  │
    int64_t thread6_count;  // offset 48  │
    int64_t thread7_count;  // offset 56  ┘
};
// 8 fields × 8 bytes = 64 bytes = 1 cache line.
// If thread i exclusively writes thread_i_count: FALSE SHARING.
// All 8 threads fight over one cache line constantly.

struct PaddedCounter {      ← PADDED (no false sharing)
    int64_t count;
    char    _pad[56];       // pad to 64 bytes total
};
// Each thread gets its own 64-byte cache line.
// Thread writes are completely independent at the hardware level.
```

### 6.4 MESI and the JVM Memory Model

Java's `volatile` and `java.util.concurrent.atomic.AtomicLong` map directly to MESI state transitions and hardware memory barriers:

```
Java volatile write:
  1. Store the new value to the field.
  2. Issue a StoreLoad fence (x86: MFENCE or LOCK XCHG).
  3. This forces the CPU to flush the store buffer and make the write
     visible to other cores before any subsequent load can proceed.
  4. Other cores' caches: the line transitions M→I on those cores
     on the next snoop (they'll see the new value on next read).

Java volatile read:
  1. Issue a LoadLoad fence.
  2. Load the value.
  3. If the cache line is I (stale due to another core's volatile write):
     cache miss → RFO-like read → get the latest value from the
     writing core's L3 or the coherence interconnect.

AtomicLong.incrementAndGet():
  Compiles to LOCK XADD (x86 atomic add):
  1. The LOCK prefix acquires exclusive ownership of the cache line
     (same as RFO if another core holds it Modified or Shared).
  2. The add is performed atomically while the line is held Modified.
  3. The line remains Modified on this core — other cores' copies → I.

AtomicLong[] array — classic false sharing target:
  AtomicLong[] counters = new AtomicLong[8];
  for (int i = 0; i < 8; i++) counters[i] = new AtomicLong(0);
  
  // Each AtomicLong object: 16-byte header + 8-byte value = 24 bytes
  // JVM may allocate contiguously → 2-3 AtomicLong per 64B line
  // → threads writing different counters = FALSE SHARING
  // 
  // Fix: use LongAdder (striped internally) or pad each AtomicLong.
```

---

## 7. Mental Models

### Mental Model 1 — The Hot Potato

Imagine a cache line as a physical envelope containing 64 bytes of data. The MESI protocol's Modified state means one core "holds" the envelope and can write on it. When another core wants to write to the same envelope:

1. The first core must physically hand the envelope to the second core (the RFO round-trip).
2. The second core writes on it.
3. If the first core wants to write again, the second core must hand it back.

With true sharing, the envelope is being passed around because its contents are genuinely shared — all cores need the latest value before proceeding. The passing is unavoidable.

With false sharing, core 0 is writing on the top-left corner of the envelope, and core 1 is writing on the bottom-right corner. They never need each other's data. But the protocol only knows about envelopes, not corners. The envelope keeps getting passed back and forth, 40–500 cycles per pass, for no logical reason.

**The fix:** Give each thread its own envelope. Padding to 64 bytes ensures each thread's variable is on its own cache line — its own envelope that no other thread ever touches.

### Mental Model 2 — The Coherence Tax per Write

Think of every write in a parallel program as having a "coherence tax" that depends on the current state of the cache line:

```
Write cost breakdown:

  Line state  │  Who has it?      │  Write cost (same socket)
  ────────────┼───────────────────┼──────────────────────────
  M (you own) │  This core only   │  ~4 cycles (L1 write, no traffic)
  E (excl.)   │  This core only   │  ~4 cycles (M→E is silent)
  S (shared)  │  Multiple cores   │  ~40–60 cycles (RFO: invalidate others)
  I (invalid) │  Another core (M) │  ~40–60 cycles (RFO: get line, invalidate)
  I (invalid) │  L3 only          │  ~40 cycles (L3 miss, no cross-core traffic)
  I (invalid) │  DRAM only        │  ~245 cycles (DRAM miss)

False sharing scenario: line alternates M (other core) ↔ M (you)
→ Every write costs 40–500 cycles instead of 4 cycles
→ 10–125× write slowdown
```

The coherence tax only applies when another core holds a Modified copy. The solution is to ensure each thread writes only to cache lines that no other thread touches — keeping the cache line in M state on the writing thread's core permanently.

### Mental Model 3 — The Two-Layer Sharing Hierarchy

All sharing between threads in a parallel program can be classified into two layers:

**Layer 1 — Logical sharing:** Data that threads *need* to share for the algorithm to be correct. Lock variables, shared output arrays, producer-consumer queues. This sharing is intentional and necessary. Coherence traffic here is correct and expected.

**Layer 2 — Accidental sharing (false sharing):** Data that threads *appear* to share at the hardware level (same cache line) but don't need to share logically. Per-thread counters, thread-local accumulators, independent output buckets. This sharing is unintentional. Coherence traffic here is wasted overhead.

The goal of false sharing elimination is to confine all coherence traffic to Layer 1 (where it's necessary) and eliminate Layer 2 entirely.

For data engineering: Spark's accumulator model works hard to minimize Layer 2. Each task writes to a local accumulator variable (no sharing), and the driver merges them at the end (one controlled merge). The false sharing risk appears when multiple Python tasks run on the same JVM executor and write to an array that was allocated without padding between per-task regions.

---

## 8. Failure Scenarios

### Failure 1 — Per-Thread Counter Array Without Padding

**Symptom:** A multi-threaded C/Cython extension that counts events per thread runs at 2× the speed of a sequential version, even though 8 threads are available and the work is embarrassingly parallel.

**Root cause:** The per-thread counters are in a plain array: `int64_t counters[N_THREADS]`. Array elements are 8 bytes each. Eight consecutive elements occupy one 64-byte cache line. All 8 threads write to the same cache line on every count. The cache line bounces continuously between cores in M state.

```python
import ctypes, threading, time

# BAD: array of int64 without padding
N_THREADS = 8

# Each element is 8 bytes → 8 elements per cache line → false sharing
BadCounterArray = ctypes.c_int64 * N_THREADS
bad_counters = BadCounterArray(*([0] * N_THREADS))

ITERS = 10_000_000

def increment_bad(thread_id):
    for _ in range(ITERS):
        bad_counters[thread_id] += 1

def increment_good(local_ref, thread_id):
    # Thread-local variable: no sharing until end
    count = 0
    for _ in range(ITERS):
        count += 1
    local_ref[thread_id] = count   # one write at the end

# Benchmark
t0 = time.perf_counter()
threads = [threading.Thread(target=increment_bad, args=(i,))
           for i in range(N_THREADS)]
for t in threads: t.start()
for t in threads: t.join()
t_bad = (time.perf_counter() - t0) * 1000

local_results = [0] * N_THREADS
t0 = time.perf_counter()
threads = [threading.Thread(target=increment_good,
                             args=(local_results, i))
           for i in range(N_THREADS)]
for t in threads: t.start()
for t in threads: t.join()
t_good = (time.perf_counter() - t0) * 1000

print(f"Shared counter array (false sharing): {t_bad:.0f} ms")
print(f"Thread-local then merge:              {t_good:.0f} ms")
print(f"Ratio: {t_bad/t_good:.1f}×")
# Expected: 10–30× slowdown from false sharing (Python GIL limits the
# effect here; actual C threads would show larger ratios)
```

---

### Failure 2 — Spark Accumulator Contention

**Symptom:** A Spark job that uses multiple accumulators to track per-output-bucket row counts runs noticeably slower than the same job without accumulator tracking. Profiling shows the accumulator updates consume 15% of total task time.

**Root cause:** Spark accumulators are implemented as per-task local variables that are merged at the end of each task. However, if a pipeline author creates an array of accumulators (e.g., one per output partition key) and updates them in a tight loop, the JVM's allocation of these accumulators may place them in a contiguous array in the JVM heap. Multiple tasks running on the same executor (sharing the same JVM) may then write to adjacent accumulator objects, causing false sharing between executor threads at the JVM level.

The issue is not in Spark's accumulator merge logic (which is already thread-safe and uses local updates) but in the JVM object layout: a `LongAccumulator[]` array where each `LongAccumulator` is a small object allocated contiguously may pack multiple accumulator values within one cache line.

**Fix:** Use `LongAdder` instead of `AtomicLong` or single `LongAccumulator`. `LongAdder` internally uses a striped array where each stripe is padded to avoid false sharing. For custom metric tracking in Spark tasks, use `ThreadLocal` variables and merge at task end.

---

### Failure 3 — Python multiprocessing SharedMemory Array

**Symptom:** A Python multiprocessing pool that increments per-process statistics in a `multiprocessing.Array` runs slower when using more processes, even though the processes are fully CPU-bound and there is no I/O or Python GIL interaction.

**Root cause:** `multiprocessing.Array('q', N_PROCESSES)` creates a shared memory segment with N int64 values contiguously. If N is small (≤ 8), all values fit in one or two cache lines. Multiple processes on different CPU cores all writing to this array cause false sharing — the cache line with the statistics bounces between cores via the MESI protocol, serializing writes.

Note: in Python multiprocessing, processes have separate address spaces. But on Linux, `multiprocessing.Array` uses POSIX shared memory — a region mapped into multiple processes' virtual address spaces, backed by the same physical pages. These physical pages are cached in each core's L1/L2 with MESI-state tracking. Writes by different processes on different cores trigger the same RFO protocol as multi-threaded false sharing.

---

### Failure 4 — JVM AtomicLong Array as Metrics Counters

**Symptom:** A Kafka Streams application tracks per-partition processing metrics using `AtomicLong[] partitionMetrics = new AtomicLong[N_PARTITIONS]`. Profiling shows high contention on the array even though each thread writes only to its own `partitionMetrics[partitionId]`.

**Root cause:** Java's `AtomicLong` object is 24 bytes (16-byte object header + 8-byte value). Two to three `AtomicLong` objects fit in one 64-byte cache line. If partitions 0, 1, and 2 are processed by different threads, and their `AtomicLong` objects are allocated contiguously (common in array initialization loops), all three threads write to the same cache line — false sharing.

The LOCK XADD instruction used by `AtomicLong.incrementAndGet()` acquires exclusive ownership of the cache line. If three threads each call `incrementAndGet()` on contiguous `AtomicLong` objects, they each acquire exclusive ownership sequentially → the line bounces M→I→M→I→M→I between three cores on every increment.

```
Java AtomicLong layout:
  Object header: 12 bytes (compressed oops) or 16 bytes (classic)
  Value field:    8 bytes
  Total:         ~24 bytes

  AtomicLong[0]: bytes 0–23  ┐
  AtomicLong[1]: bytes 24–47 │  Same 64-byte cache line!
  AtomicLong[2]: bytes 48–63 ┘  (2.6 AtomicLongs per line)

Fix: Use LongAdder (internally padded via @jdk.internal.vm.annotation.Contended)
or use Striped64 from Guava, or pad manually with dummy fields.
```

---

## 9. Recovery Procedures

### Fix 1 — Padding to Cache Line Boundary

The most direct fix: ensure each thread's mutable data occupies an entire cache line by itself.

```python
"""
padding_demo.py

Demonstrates cache line padding to eliminate false sharing.
Uses ctypes structures with explicit padding.

In production C/C++: use alignas(64) and __attribute__((aligned(64))).
In Python ctypes: pack each counter into a 64-byte structure.
"""
import ctypes
import threading
import time


# BAD: packed array — all counters in same cache line(s)
class BadCounters(ctypes.Structure):
    _fields_ = [
        ('c0', ctypes.c_int64),   # offset 0
        ('c1', ctypes.c_int64),   # offset 8
        ('c2', ctypes.c_int64),   # offset 16
        ('c3', ctypes.c_int64),   # offset 24
        ('c4', ctypes.c_int64),   # offset 32
        ('c5', ctypes.c_int64),   # offset 40
        ('c6', ctypes.c_int64),   # offset 48
        ('c7', ctypes.c_int64),   # offset 56
    ]  # total: 64 bytes = 1 cache line — false sharing when written by 8 threads


# GOOD: padded — each counter on its own cache line
CACHE_LINE = 64

class PaddedCounter(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ('value', ctypes.c_int64),           # 8 bytes (the actual counter)
        ('_pad',  ctypes.c_uint8 * (CACHE_LINE - 8)),  # 56 bytes padding
    ]  # total: 64 bytes — counter and padding fill exactly one cache line

assert ctypes.sizeof(PaddedCounter) == CACHE_LINE, \
    f"PaddedCounter is {ctypes.sizeof(PaddedCounter)} bytes, expected {CACHE_LINE}"


# Array of 8 padded counters = 8 cache lines (no false sharing)
PaddedCounterArray = PaddedCounter * 8


def benchmark(n_threads: int = 8, iters: int = 5_000_000):
    print(f"\n{'Counter layout':^40}  {'Time (ms)':>10}")
    print("-" * 55)

    # BAD: packed
    bad = BadCounters()

    field_names = ['c0','c1','c2','c3','c4','c5','c6','c7']
    def write_bad(tid):
        name = field_names[tid]
        for _ in range(iters):
            setattr(bad, name, getattr(bad, name) + 1)

    threads = [threading.Thread(target=write_bad, args=(i,))
               for i in range(min(n_threads, 8))]
    t0 = time.perf_counter()
    for th in threads: th.start()
    for th in threads: th.join()
    t_bad = (time.perf_counter() - t0) * 1000
    print(f"  {'Packed (false sharing)':<38}  {t_bad:>10.1f}")

    # GOOD: padded
    good = PaddedCounterArray()

    def write_good(tid):
        for _ in range(iters):
            good[tid].value += 1

    threads = [threading.Thread(target=write_good, args=(i,))
               for i in range(min(n_threads, 8))]
    t0 = time.perf_counter()
    for th in threads: th.start()
    for th in threads: th.join()
    t_good = (time.perf_counter() - t0) * 1000
    print(f"  {'Padded (no false sharing)':<38}  {t_good:>10.1f}")

    if t_good > 0:
        print(f"\n  Speedup from padding: {t_bad/t_good:.1f}×")

    print(f"\n  Note: Python GIL limits multi-thread parallelism.")
    print(f"  In C/Cython with nogil: ratio is typically 10–30×.")


if __name__ == '__main__':
    benchmark()
```

### Fix 2 — Thread-Local Then Merge

When each thread needs to accumulate a value and the final result is a merge (sum, max, concatenation), never share mutable state during accumulation:

```python
"""
thread_local_merge.py

Demonstrates the thread-local-then-merge pattern:
  1. Each thread accumulates into a local variable (no sharing).
  2. After all threads complete, a single thread merges the results.

This eliminates ALL coherence traffic during the parallel phase.
The merge is sequential but O(n_threads) — negligible.
"""
import threading
import time
from typing import Callable


def parallel_sum_false_sharing(data: list, n_threads: int) -> int:
    """Accumulates into a shared list — false sharing if list is contiguous."""
    import ctypes
    # Shared array: all in same or adjacent cache lines
    result = (ctypes.c_int64 * n_threads)(*([0] * n_threads))
    chunk = len(data) // n_threads

    def worker(tid):
        start = tid * chunk
        end   = start + chunk if tid < n_threads - 1 else len(data)
        for i in range(start, end):
            result[tid] += data[i]  # write to shared array → false sharing

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n_threads)]
    t0 = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()
    t_elapsed = time.perf_counter() - t0

    return sum(result), t_elapsed


def parallel_sum_local_then_merge(data: list, n_threads: int) -> int:
    """Thread-local accumulation, then merge — zero false sharing."""
    local_results = [0] * n_threads  # pre-allocate slots
    chunk = len(data) // n_threads

    def worker(tid):
        start = tid * chunk
        end   = start + chunk if tid < n_threads - 1 else len(data)
        local = 0                        # STACK variable: not shared
        for i in range(start, end):
            local += data[i]             # pure local write
        local_results[tid] = local       # ONE write at the end

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n_threads)]
    t0 = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()
    t_elapsed = time.perf_counter() - t0

    # O(n_threads) merge — negligible vs O(n_data) work
    return sum(local_results), t_elapsed


def benchmark():
    import random
    N = 10_000_000
    N_THREADS = 8
    data = [random.randint(0, 100) for _ in range(N)]

    r1, t1 = parallel_sum_false_sharing(data, N_THREADS)
    r2, t2 = parallel_sum_local_then_merge(data, N_THREADS)

    assert r1 == r2, f"Results differ: {r1} != {r2}"

    print(f"\nParallel sum of {N:,} integers, {N_THREADS} threads:")
    print(f"  Shared array (false sharing):   {t1*1000:.1f} ms")
    print(f"  Local then merge:               {t2*1000:.1f} ms")
    print(f"  Speedup:  {t1/t2:.1f}×")
    print(f"\nNote: Python's GIL means threads aren't truly parallel here.")
    print(f"In real parallel C code (OpenMP), the ratio is 10–50×.")
    print(f"The pattern is correct regardless — local variables never")
    print(f"generate coherence traffic even when threads ARE parallel.")


if __name__ == '__main__':
    benchmark()
```

### Fix 3 — Java's @Contended Annotation

Java 8+ provides `@jdk.internal.vm.annotation.Contended` (or `@sun.misc.Contended` in earlier JDKs) which instructs the JVM to add 128 bytes of padding around annotated fields, placing them on isolated cache lines:

```java
// BAD: AtomicLong array with false sharing
AtomicLong[] counters = new AtomicLong[N_THREADS];
for (int i = 0; i < N_THREADS; i++) counters[i] = new AtomicLong(0);
// Objects packed contiguously → false sharing

// BETTER: LongAdder (uses Striped64 which is internally @Contended-padded)
LongAdder[] counters = new LongAdder[N_THREADS];
for (int i = 0; i < N_THREADS; i++) counters[i] = new LongAdder();
// LongAdder's Cell class is @Contended internally → no false sharing

// BEST for throughput: thread-local long, merge at end
ThreadLocal<Long> counter = ThreadLocal.withInitial(() -> 0L);
// ... in each thread:
counter.set(counter.get() + 1);
// ... after all threads:
long total = threads.stream().mapToLong(t -> t.getLocalCounter()).sum();
```

JVM flags for @Contended:
- Requires `-XX:-RestrictContended` or `--add-opens java.base/jdk.internal.vm.annotation=ALL-UNNAMED`
- JVM adds 128 bytes of padding (not just 64) because some CPUs use 128-byte sectors

---

## 10. Trade-offs

### Padding vs Memory Waste

Adding 56 bytes of padding to each 8-byte counter increases memory usage 8×. For 8 threads this means 512 bytes instead of 64 bytes — negligible. For 10,000 threads or 10,000 per-partition counters: 640 KB padded vs 80 KB unpadded — still likely acceptable.

The crossover where padding becomes a concern: when the number of padded objects × 64 bytes exceeds L3 cache. With 64 MB L3 and 64 bytes per padded counter: up to 1,000,000 padded counters before the counter array itself becomes an L3 pressure concern.

| # padded counters | Padded size | L3 impact (32 MB L3) | Recommendation |
|---|---|---|---|
| 8 (one per thread) | 512 B | Negligible | Always pad |
| 1,000 | 64 KB | Negligible | Pad |
| 100,000 | 6.4 MB | Moderate | Pad or use thread-local |
| 1,000,000 | 64 MB | Exceeds L3 | Use thread-local + merge |

### False Sharing vs Lock Contention vs True Sharing

These three are often confused in profiling. All three show up as high cache-miss rates under parallel write load, but they have different root causes and different fixes:

| Problem | Root cause | Fix |
|---|---|---|
| False sharing | Different data in same cache line | Padding, thread-local |
| Lock contention | Serialized access to same lock | Fine-grained locks, lock-free structures |
| True sharing | Same data legitimately shared | Reduce sharing frequency, reduce writers |

The distinguishing test: if you comment out all but one thread's writes and the performance is the same as multi-threaded, it's false sharing (one thread has no one to fight with). If performance improves linearly with fewer threads only when they access the same logical data, it may be true sharing or lock contention.

---

## 11. Comparisons

### LongAdder vs AtomicLong: Why LongAdder Wins Under Contention

`java.util.concurrent.atomic.LongAdder` is specifically designed to solve the false sharing and true sharing problems with `AtomicLong` under high contention:

```
AtomicLong.incrementAndGet() under high thread contention:
  All threads → LOCK XADD on the same memory location
  Only one thread can hold the cache line in M state at a time
  Other threads queue, waiting for M-state ownership
  Throughput: ~one increment per RFO latency (40 cycles)
  For 16 threads: effective throughput = 1/16th of one thread alone

LongAdder.increment() under high thread contention:
  Threads → check "base" field first (low contention path)
  On contention → hash to a Cell in the cells[] array
  Each Cell is @Contended (padded to full cache line)
  Thread i → Cell[hash(i)] → its own cache line → no fighting
  Only at read time (sum()) → iterate and sum all cells
  Throughput: scales with number of threads (each thread → own line)

Structure:
  LongAdder extends Striped64 {
      Cell[] cells;    // array of @Contended longs — no false sharing
      long base;       // used when no contention
  }
  
  @Contended class Cell {
      volatile long value;  // 8 bytes value + 128 bytes padding = 136 bytes
  }
```

`LongAdder` trades space (more memory per counter) for parallelism (no coherence traffic between threads). This is exactly the padding solution, done automatically by the JVM.

---

## 12. Production Examples

### Spark Task Metrics: The Right and Wrong Way

Spark tracks per-task metrics (rows read, bytes shuffled, spill size) using `TaskMetrics`, which contains a collection of `AccumulatorV2` instances. The design is intentional: each task has its own `TaskMetrics` object (no sharing during task execution), and results are merged into the driver's global state only at task completion.

```
Spark TaskMetrics design (correct):
  Task 0: TaskMetrics₀ → local accumulator updates → merge at end
  Task 1: TaskMetrics₁ → local accumulator updates → merge at end
  Task 2: TaskMetrics₂ → local accumulator updates → merge at end
  
  Executor thread pool: each thread handles one task at a time.
  TaskMetrics are per-task objects → no sharing during execution.
  Merge phase: driver receives TaskMetrics from each task → sequential merge.

  Result: zero false sharing during task execution.
  This is the thread-local-then-merge pattern applied at the task level.

Anti-pattern (user-defined accumulators done wrong):
  // In driver:
  AtomicLong[] partitionCounters = new AtomicLong[200];
  // Pass reference to all tasks:
  rdd.mapPartitionsWithIndex((partitionId, iter) -> {
      while (iter.hasNext()) {
          partitionCounters[partitionId].incrementAndGet();  // FALSE SHARING!
          // partitionCounters[0] and partitionCounters[1] may be in same cache line
          // Multiple threads on same executor → M→I→M→I bounce
      }
  });
  
  Correct:
  rdd.mapPartitions(iter -> {
      long localCount = 0;
      while (iter.hasNext()) {
          localCount++;  // stack variable: no sharing
          iter.next();
      }
      // Emit the local count; Spark aggregates in reduce step
      return Collections.singleton(localCount).iterator();
  }).reduce(Long::sum);
```

---

### DuckDB Parallel Aggregation: The Striped Buffer

DuckDB's parallel aggregation operator partitions the output hash table into stripes, where each thread owns a set of stripes. Each stripe is allocated at cache-line-aligned boundaries:

```
DuckDB parallel groupby:
  N threads, each assigned stride of partitions.
  Partition array: 256 hash table buckets per partition.
  Each partition starts at 64-byte-aligned address.
  
  Thread 0 → partitions 0, 4, 8, ...  (stride = N_THREADS)
  Thread 1 → partitions 1, 5, 9, ...
  
  Adjacent partitions are 64-byte aligned → different cache lines.
  Threads never write to the same cache line during aggregation.
  
  Final merge: single thread iterates all partitions → sequential merge.
  
  Result: parallel aggregation scales to N_THREADS with near-zero coherence overhead.
```

---

### Kafka's Per-Partition Metrics

Kafka brokers track per-partition metrics (bytes in, bytes out, request rate) using a `MetricsRegistry`. Each partition's metrics object is allocated independently:

```java
// Kafka MetricName uses a separate object per partition:
// Each PartitionMetrics is a separate heap object → separate cache line(s)
// Thread i handles partition i's metrics → no false sharing between partitions

// The risk: metrics are often read by a monitoring thread simultaneously
// with writes by the partition I/O threads.
// Read path: Shared (S) state → no coherence traffic for readers.
// Write path: producer thread → Modified (M) → monitoring thread read
//   invalidates producer's copy → RFO.
// This is TRUE sharing (monitoring genuinely needs fresh values),
// not false sharing. LongAdder handles this correctly.
```

---

## 13. Code

### `false_sharing_detector.py` — Detect and Measure False Sharing

```python
"""
false_sharing_detector.py

Analyzes Python data structures for false sharing risk based on
field sizes, alignment, and access patterns.

Also benchmarks parallel write performance to confirm or deny
false sharing empirically.

Run: python3 false_sharing_detector.py
"""
import ctypes
import inspect
import threading
import time
from dataclasses import dataclass
from typing import Any, List, Tuple

CACHE_LINE_SIZE = 64  # bytes


@dataclass
class FieldLayout:
    name: str
    offset: int
    size: int
    cache_line: int  # which cache line does this field start on?

    @property
    def end_offset(self) -> int:
        return self.offset + self.size

    @property
    def end_cache_line(self) -> int:
        return (self.offset + self.size - 1) // CACHE_LINE_SIZE


def analyze_ctypes_struct(struct_type: type) -> List[FieldLayout]:
    """
    Compute field layout for a ctypes Structure type.
    Returns FieldLayout for each field, sorted by offset.
    """
    layouts = []
    for name, ctype in struct_type._fields_:
        offset = getattr(struct_type, name).offset
        size   = ctypes.sizeof(ctype)
        layouts.append(FieldLayout(
            name=name,
            offset=offset,
            size=size,
            cache_line=offset // CACHE_LINE_SIZE,
        ))
    return sorted(layouts, key=lambda f: f.offset)


def find_false_sharing_pairs(
    layouts: List[FieldLayout],
    thread_field_map: dict  # {thread_id: field_name}
) -> List[Tuple[str, str, int]]:
    """
    Given a mapping of which thread writes which field,
    find pairs of fields written by different threads that
    share a cache line.
    
    Returns: list of (thread_a, thread_b, shared_cache_line) tuples.
    """
    # Build: cache_line → list of (thread_id, field_name)
    line_to_fields = {}
    field_to_layout = {f.name: f for f in layouts}

    for tid, fname in thread_field_map.items():
        layout = field_to_layout.get(fname)
        if layout is None:
            continue
        for cl in range(layout.cache_line, layout.end_cache_line + 1):
            line_to_fields.setdefault(cl, []).append((tid, fname))

    # Find cache lines written by more than one thread
    conflicts = []
    for cl, writers in line_to_fields.items():
        if len(writers) > 1:
            for i in range(len(writers)):
                for j in range(i + 1, len(writers)):
                    tid_a, fn_a = writers[i]
                    tid_b, fn_b = writers[j]
                    if tid_a != tid_b:
                        conflicts.append((
                            f"Thread {tid_a} (field '{fn_a}')",
                            f"Thread {tid_b} (field '{fn_b}')",
                            cl,
                        ))
    return conflicts


def report_layout(struct_type: type, thread_field_map: dict):
    """Print cache line layout report for a ctypes Structure."""
    layouts = analyze_ctypes_struct(struct_type)
    total_size = ctypes.sizeof(struct_type)
    n_cache_lines = (total_size + CACHE_LINE_SIZE - 1) // CACHE_LINE_SIZE

    print(f"\nStruct: {struct_type.__name__}")
    print(f"  Total size: {total_size} bytes = {n_cache_lines} cache line(s)")
    print(f"\n  Field layout:")
    print(f"  {'Field':<20}  {'Offset':>7}  {'Size':>5}  {'CacheLine':>10}")
    print(f"  " + "-" * 50)

    current_line = -1
    for f in layouts:
        if f.cache_line != current_line:
            current_line = f.cache_line
            print(f"  ── Cache line {current_line} (bytes {current_line*64}–{current_line*64+63}) ──")
        print(f"  {f.name:<20}  {f.offset:>7}  {f.size:>5}  {f.cache_line:>10}")

    # Find false sharing
    conflicts = find_false_sharing_pairs(layouts, thread_field_map)
    if conflicts:
        print(f"\n  ⚠️  FALSE SHARING DETECTED:")
        for a, b, cl in conflicts:
            print(f"    {a} and {b} share cache line {cl}")
    else:
        print(f"\n  ✅  No false sharing (all writers on different cache lines)")


def benchmark_parallel_writes(n_threads: int = 8,
                               iters: int = 3_000_000):
    """
    Empirically measure the cost of false sharing vs padded writes.
    Uses ctypes arrays for direct memory control.
    """
    print(f"\n{'=' * 60}")
    print(f"Parallel write benchmark: {n_threads} threads × {iters:,} writes")
    print(f"{'=' * 60}")

    results = {}

    # 1) Packed array (false sharing)
    PackedArray = ctypes.c_int64 * n_threads
    packed = PackedArray(*([0] * n_threads))

    def write_packed(tid):
        for _ in range(iters):
            packed[tid] += 1

    threads = [threading.Thread(target=write_packed, args=(i,))
               for i in range(n_threads)]
    t0 = time.perf_counter()
    for th in threads: th.start()
    for th in threads: th.join()
    results['packed'] = time.perf_counter() - t0

    # 2) Padded array (no false sharing)
    class PaddedEntry(ctypes.Structure):
        _pack_ = 1
        _fields_ = [
            ('value', ctypes.c_int64),
            ('_pad',  ctypes.c_uint8 * 56),
        ]
    PaddedArray = PaddedEntry * n_threads
    padded = PaddedArray()

    def write_padded(tid):
        for _ in range(iters):
            padded[tid].value += 1

    threads = [threading.Thread(target=write_padded, args=(i,))
               for i in range(n_threads)]
    t0 = time.perf_counter()
    for th in threads: th.start()
    for th in threads: th.join()
    results['padded'] = time.perf_counter() - t0

    # 3) Thread-local then merge (best case)
    local_vals = [0] * n_threads

    def write_local(tid):
        count = 0
        for _ in range(iters):
            count += 1
        local_vals[tid] = count

    threads = [threading.Thread(target=write_local, args=(i,))
               for i in range(n_threads)]
    t0 = time.perf_counter()
    for th in threads: th.start()
    for th in threads: th.join()
    _ = sum(local_vals)  # merge step (O(n_threads), negligible)
    results['local'] = time.perf_counter() - t0

    print(f"\n  {'Method':<35}  {'Time (ms)':>10}  {'vs packed'}")
    print(f"  " + "-" * 60)
    for name, t in results.items():
        ratio = results['packed'] / t
        bar = "▓" * int(ratio * 5)
        print(f"  {name:<35}  {t*1000:>10.1f}  {ratio:.1f}× faster {bar}")

    print(f"\n  Note: Python GIL limits true parallelism in this benchmark.")
    print(f"  In Cython with 'nogil', ratios are typically 10–50×.")


if __name__ == '__main__':
    # Demo: analyze a packed counter struct
    class PackedCounters(ctypes.Structure):
        _fields_ = [(f'c{i}', ctypes.c_int64) for i in range(8)]

    class PaddedCounters(ctypes.Structure):
        class PaddedEntry(ctypes.Structure):
            _pack_ = 1
            _fields_ = [('value', ctypes.c_int64), ('_pad', ctypes.c_uint8 * 56)]
        _fields_ = [(f'c{i}', PaddedEntry) for i in range(8)]

    thread_map = {i: f'c{i}' for i in range(8)}

    report_layout(PackedCounters, thread_map)
    report_layout(PaddedCounters, thread_map)

    benchmark_parallel_writes()
```

### `cache_coherence_visualizer.py` — Simulate MESI State Transitions

```python
"""
cache_coherence_visualizer.py

Simulates MESI state transitions for a cache line across multiple cores.
Shows exactly which state transitions occur during true sharing and
false sharing scenarios.

Run: python3 cache_coherence_visualizer.py
"""
from enum import Enum, auto
from typing import List, Optional
from dataclasses import dataclass, field


class MESIState(Enum):
    M = "Modified"    # Dirty, exclusive copy
    E = "Exclusive"   # Clean, exclusive copy
    S = "Shared"      # Clean, shared copy
    I = "Invalid"     # No valid copy


@dataclass
class CacheEvent:
    core: int
    operation: str   # "read" or "write"
    logical_var: str  # which variable within the cache line
    rfo_cycles: int  # estimated coherence overhead
    description: str


class MESISimulator:
    """
    Simulates MESI protocol for a single cache line across N cores.
    Tracks state per-core and generates event logs.
    """

    RFO_LATENCY_SAME_SOCKET = 50    # cycles
    RFO_LATENCY_CROSS_SOCKET = 400  # cycles
    L1_HIT_LATENCY = 4              # cycles
    L3_LATENCY = 40                 # cycles

    def __init__(self, n_cores: int, cross_socket: bool = False):
        self.n_cores = n_cores
        self.cross_socket = cross_socket
        self.states: List[MESIState] = [MESIState.I] * n_cores
        self.events: List[CacheEvent] = []
        self.total_cycles = 0

    @property
    def rfo_latency(self) -> int:
        return (self.RFO_LATENCY_CROSS_SOCKET if self.cross_socket
                else self.RFO_LATENCY_SAME_SOCKET)

    def _has_modified(self) -> Optional[int]:
        """Return the core holding Modified state, or None."""
        for i, s in enumerate(self.states):
            if s == MESIState.M:
                return i
        return None

    def _has_shared(self) -> List[int]:
        """Return all cores holding Shared state."""
        return [i for i, s in enumerate(self.states) if s == MESIState.S]

    def read(self, core: int, var: str = "x") -> CacheEvent:
        """Simulate a read by the given core."""
        my_state = self.states[core]
        modified_core = self._has_modified()

        if my_state in (MESIState.M, MESIState.E, MESIState.S):
            # Cache hit
            cycles = self.L1_HIT_LATENCY
            desc = f"L1 hit ({my_state.name} state)"
        elif modified_core is not None and modified_core != core:
            # Another core has Modified → writeback + share
            cycles = self.rfo_latency
            # Transition: M→S on holder, I→S on requester
            self.states[modified_core] = MESIState.S
            self.states[core] = MESIState.S
            desc = f"Core {modified_core} had M → writeback, both → S ({cycles} cycles)"
        else:
            # No one has it or it's in L3
            cycles = self.L3_LATENCY
            if all(s == MESIState.I for s in self.states):
                # We're the only one loading → Exclusive
                self.states[core] = MESIState.E
                desc = f"L3 miss → E ({cycles} cycles)"
            else:
                self.states[core] = MESIState.S
                desc = f"L3 hit → S ({cycles} cycles)"

        ev = CacheEvent(core=core, operation="read", logical_var=var,
                        rfo_cycles=cycles, description=desc)
        self.events.append(ev)
        self.total_cycles += cycles
        return ev

    def write(self, core: int, var: str = "x") -> CacheEvent:
        """Simulate a write by the given core."""
        my_state = self.states[core]
        modified_core = self._has_modified()
        shared_cores = self._has_shared()

        if my_state == MESIState.M:
            # Already have exclusive modified copy → free write
            cycles = self.L1_HIT_LATENCY
            desc = f"L1 hit (M state) — free write"

        elif my_state == MESIState.E:
            # Exclusive clean → just set Modified (silent upgrade)
            cycles = self.L1_HIT_LATENCY
            self.states[core] = MESIState.M
            desc = f"E → M (silent upgrade, no bus traffic)"

        elif my_state == MESIState.S:
            # Shared → need to invalidate all other S copies
            cycles = self.rfo_latency
            for c in shared_cores:
                if c != core:
                    self.states[c] = MESIState.I
            self.states[core] = MESIState.M
            inv_cores = [c for c in shared_cores if c != core]
            desc = (f"S → RFO → invalidate cores {inv_cores}, "
                    f"this core → M ({cycles} cycles)")

        elif modified_core is not None and modified_core != core:
            # Another core has Modified → writeback → we get Modified
            cycles = self.rfo_latency
            self.states[modified_core] = MESIState.I
            self.states[core] = MESIState.M
            desc = (f"Core {modified_core} had M → invalidate, "
                    f"this core → M ({cycles} cycles)")

        else:
            # Invalid, no other core has it
            cycles = self.L3_LATENCY + self.rfo_latency // 2
            self.states[core] = MESIState.M
            desc = f"I → fetch from L3 → M ({cycles} cycles)"

        ev = CacheEvent(core=core, operation="write", logical_var=var,
                        rfo_cycles=cycles, description=desc)
        self.events.append(ev)
        self.total_cycles += cycles
        return ev

    def print_state(self, title: str = ""):
        if title:
            print(f"\n  {title}")
        states_str = "  ".join(
            f"Core {i}: {s.name}" for i, s in enumerate(self.states)
        )
        print(f"  States: {states_str}")

    def print_events(self):
        print(f"\n  Event log:")
        for ev in self.events:
            op_sym = "R" if ev.operation == "read" else "W"
            print(f"    [{op_sym}] Core {ev.core} {ev.operation}s {ev.logical_var}: "
                  f"{ev.description}")
        print(f"  Total coherence cycles: {self.total_cycles:,}")


def demo_false_sharing():
    print("=" * 65)
    print("SCENARIO: FALSE SHARING — Two threads writing different variables")
    print("in the same 64-byte cache line")
    print("=" * 65)

    sim = MESISimulator(n_cores=2)
    sim.print_state("Initial:")

    # Simulate 5 alternating writes (cores 0 and 1 write to A and B
    # respectively, both in the same cache line)
    for i in range(5):
        sim.write(core=0, var="A (offset 0)")
        sim.write(core=1, var="B (offset 8, SAME LINE)")

    sim.print_state("After 10 alternating writes:")
    sim.print_events()

    print(f"\n  → Every write is an RFO ({sim.rfo_latency} cycles).")
    print(f"  → 10 writes × {sim.rfo_latency} cycles = {sim.total_cycles} cycles total.")
    print(f"  → Sequential alternative: 10 × 4 cycles = 40 cycles.")
    print(f"  → False sharing overhead: {sim.total_cycles/40:.0f}× slower!")


def demo_true_sharing():
    print("\n" + "=" * 65)
    print("SCENARIO: TRUE SHARING — Two threads sharing a single counter")
    print("(e.g., a LOCK-protected shared variable)")
    print("=" * 65)

    sim = MESISimulator(n_cores=2)

    # Thread 0 reads the shared counter, increments, writes back
    # Thread 1 does the same — they genuinely need each other's writes
    for i in range(3):
        sim.read(core=0, var="counter")
        sim.write(core=0, var="counter")
        sim.read(core=1, var="counter")   # needs updated value
        sim.write(core=1, var="counter")

    sim.print_events()
    print(f"\n  → Coherence traffic is NECESSARY here.")
    print(f"  → Thread 1 must see thread 0's increment before proceeding.")
    print(f"  → Use a lock or atomic operation — the RFO cost is unavoidable.")


def demo_padded_counters():
    print("\n" + "=" * 65)
    print("SCENARIO: PADDED COUNTERS — Different cache lines")
    print("No false sharing: each thread owns its own line")
    print("=" * 65)

    # Two separate cache line simulations
    sim_a = MESISimulator(n_cores=2)  # line for counter A (core 0's line)
    sim_b = MESISimulator(n_cores=2)  # line for counter B (core 1's line)

    print("\n  Core 0 writes counter A (its own cache line):")
    for i in range(5):
        sim_a.write(core=0, var="A (core 0 line)")
    sim_a.print_events()

    print("\n  Core 1 writes counter B (separate cache line):")
    for i in range(5):
        sim_b.write(core=1, var="B (core 1 line)")
    sim_b.print_events()

    total = sim_a.total_cycles + sim_b.total_cycles
    print(f"\n  Total cycles: {total}")
    print(f"  Core 0: first write → E→M (L3 fetch), then M→M (L1 hits)")
    print(f"  Core 1: same pattern, completely independent.")
    print(f"  → Zero cross-core RFO traffic. Both threads scale linearly.")


if __name__ == '__main__':
    demo_false_sharing()
    demo_true_sharing()
    demo_padded_counters()
```

---

## 14. Labs

### Lab 1 — Measure the False Sharing Penalty with perf c2c

**Goal:** Observe false sharing in `perf c2c` output. Write a C wrapper (invokable from Python via subprocess) that induces false sharing, then confirm it disappears after padding.

```python
"""
lab1_perf_c2c.py

Generates C programs that exhibit false sharing (unpadded)
and no false sharing (padded), compiles them, and runs perf c2c.

Requirements:
  - gcc (or clang)
  - Linux kernel ≥ 4.3
  - perf tool with c2c support
  - sudo or perf_event_paranoid ≤ 1

Run: sudo python3 lab1_perf_c2c.py
"""
import subprocess
import tempfile
import os
import sys


FALSE_SHARING_C = r"""
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#define ITERS 50000000
#define N_THREADS 4

// BAD: all counters in the same cache line
typedef struct {
    int64_t c[N_THREADS];  // 4 × 8 = 32 bytes, fits in one 64-byte line
} Counters;

Counters counters;

void* worker(void* arg) {
    int tid = (int)(intptr_t)arg;
    for (long i = 0; i < ITERS; i++) {
        counters.c[tid]++;  // False sharing: adjacent int64s
    }
    return NULL;
}

int main(void) {
    pthread_t threads[N_THREADS];
    for (int i = 0; i < N_THREADS; i++)
        pthread_create(&threads[i], NULL, worker, (void*)(intptr_t)i);
    for (int i = 0; i < N_THREADS; i++)
        pthread_join(threads[i], NULL);
    int64_t total = 0;
    for (int i = 0; i < N_THREADS; i++) total += counters.c[i];
    printf("Total: %ld\n", total);
    return 0;
}
"""

PADDED_C = r"""
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#define ITERS 50000000
#define N_THREADS 4
#define CACHE_LINE 64

// GOOD: each counter on its own cache line
typedef struct {
    int64_t value;
    char    _pad[CACHE_LINE - sizeof(int64_t)];
} PaddedCounter __attribute__((aligned(CACHE_LINE)));

PaddedCounter counters[N_THREADS];

void* worker(void* arg) {
    int tid = (int)(intptr_t)arg;
    for (long i = 0; i < ITERS; i++) {
        counters[tid].value++;  // No sharing: each on its own cache line
    }
    return NULL;
}

int main(void) {
    pthread_t threads[N_THREADS];
    for (int i = 0; i < N_THREADS; i++)
        pthread_create(&threads[i], NULL, worker, (void*)(intptr_t)i);
    for (int i = 0; i < N_THREADS; i++)
        pthread_join(threads[i], NULL);
    int64_t total = 0;
    for (int i = 0; i < N_THREADS; i++) total += counters[i].value;
    printf("Total: %ld\n", total);
    return 0;
}
"""


def compile_and_time(name: str, source: str) -> float:
    """Compile C source, run it, return wall-clock time in seconds."""
    with tempfile.TemporaryDirectory() as tmpdir:
        src = os.path.join(tmpdir, f'{name}.c')
        exe = os.path.join(tmpdir, name)

        with open(src, 'w') as f:
            f.write(source)

        # Compile
        result = subprocess.run(
            ['gcc', '-O2', '-pthread', '-o', exe, src],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"Compile error: {result.stderr}")
            return float('nan')

        # Time the run
        import time
        t0 = time.perf_counter()
        result = subprocess.run([exe], capture_output=True, text=True)
        elapsed = time.perf_counter() - t0

        if result.returncode != 0:
            print(f"Run error: {result.stderr}")
            return float('nan')

        print(f"  {name}: {elapsed:.3f}s  (output: {result.stdout.strip()})")
        return elapsed


def run_perf_c2c(name: str, source: str):
    """Run perf c2c on the program if available."""
    with tempfile.TemporaryDirectory() as tmpdir:
        src = os.path.join(tmpdir, f'{name}.c')
        exe = os.path.join(tmpdir, name)
        data = os.path.join(tmpdir, 'perf.data')

        with open(src, 'w') as f:
            f.write(source)

        subprocess.run(['gcc', '-O2', '-g', '-pthread', '-o', exe, src],
                       check=True, capture_output=True)

        # Run perf c2c record
        record = subprocess.run(
            ['perf', 'c2c', 'record', '-o', data, '--', exe],
            capture_output=True, text=True
        )
        if record.returncode != 0:
            print(f"  perf c2c record failed (may need root or perf_event_paranoid=0):")
            print(f"  {record.stderr.strip()[:200]}")
            return

        # Run perf c2c report
        report = subprocess.run(
            ['perf', 'c2c', 'report', '-i', data, '--stdio', '--no-header'],
            capture_output=True, text=True
        )
        print(f"\nperf c2c report for {name}:")
        # Show only the top of the report (hotspot lines)
        lines = report.stdout.splitlines()
        for line in lines[:40]:
            print(f"  {line}")


def lab1():
    print("=" * 65)
    print("LAB 1: False Sharing Detection with perf c2c")
    print("=" * 65)

    if sys.platform != 'linux':
        print("perf c2c is Linux-only. Running timing comparison only.")

    print("\nStep 1: Timing comparison (false sharing vs padded):")
    t_bad   = compile_and_time('false_sharing', FALSE_SHARING_C)
    t_good  = compile_and_time('padded',        PADDED_C)

    if not (t_bad != t_bad or t_good != t_good):  # not NaN
        print(f"\n  Speedup from padding: {t_bad/t_good:.1f}×")
        print(f"  Expected: 2–20× depending on CPU and core count")

    if sys.platform == 'linux':
        print("\nStep 2: perf c2c analysis (requires sudo):")
        print("  Checking for perf c2c support...")
        check = subprocess.run(['perf', 'c2c', '--help'],
                                capture_output=True, text=True)
        if 'c2c' in check.stdout + check.stderr:
            print("  Running perf c2c on false_sharing program...")
            run_perf_c2c('false_sharing', FALSE_SHARING_C)
            print("\n  Running perf c2c on padded program...")
            run_perf_c2c('padded', PADDED_C)
            print("\n  Look for 'Hitm' column in the output.")
            print("  High Hitm = hit in Modified state = false sharing confirmed.")
            print("  After padding: Hitm count should drop to near zero.")
        else:
            print("  perf c2c not available. Install linux-tools-$(uname -r).")


if __name__ == '__main__':
    lab1()
```

---

### Lab 2 — Find False Sharing in a Spark-Like Pipeline

**Goal:** Simulate a Spark-style per-partition metrics pipeline with and without false sharing, measure the difference, and apply the thread-local-then-merge fix.

```python
"""
lab2_spark_metrics_false_sharing.py

Simulates a Spark-like pipeline where multiple executor threads
track per-partition row counts. Demonstrates false sharing in a
shared counter array vs the correct per-task local accumulation.

Run: python3 lab2_spark_metrics_false_sharing.py
"""
import threading
import time
import ctypes
import random
import numpy as np


def simulate_task_false_sharing(
    data: list,
    n_partitions: int,
    n_threads: int,
) -> dict:
    """
    Simulates per-partition row counting where all threads write
    to a shared counter array (false sharing when n_partitions is small).
    """
    # Shared counter array: n_partitions int64s, packed (potential false sharing)
    CounterArray = ctypes.c_int64 * n_partitions
    counters = CounterArray(*([0] * n_partitions))

    partition_size = len(data) // n_threads
    lock = threading.Lock()  # only for result collection, not counting

    def process_partition(thread_id):
        start = thread_id * partition_size
        end   = start + partition_size if thread_id < n_threads - 1 else len(data)
        # Write directly to shared counter array — FALSE SHARING if counters close together
        for row in data[start:end]:
            partition_key = row % n_partitions
            counters[partition_key] += 1  # multiple threads may write same/adjacent slots

    threads = [threading.Thread(target=process_partition, args=(i,))
               for i in range(n_threads)]
    t0 = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed = time.perf_counter() - t0

    return {i: counters[i] for i in range(n_partitions)}, elapsed


def simulate_task_thread_local(
    data: list,
    n_partitions: int,
    n_threads: int,
) -> dict:
    """
    Thread-local-then-merge pattern: each thread accumulates into
    a private dict, then results are merged after all threads complete.
    Zero false sharing during the parallel phase.
    """
    local_results = [None] * n_threads
    partition_size = len(data) // n_threads

    def process_partition(thread_id):
        start = thread_id * partition_size
        end   = start + partition_size if thread_id < n_threads - 1 else len(data)
        local = {}  # private dict: no sharing
        for row in data[start:end]:
            pk = row % n_partitions
            local[pk] = local.get(pk, 0) + 1
        local_results[thread_id] = local

    threads = [threading.Thread(target=process_partition, args=(i,))
               for i in range(n_threads)]
    t0 = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()

    # Sequential merge (O(n_threads × n_partitions), fast)
    merged = {}
    for local in local_results:
        for pk, cnt in local.items():
            merged[pk] = merged.get(pk, 0) + cnt
    elapsed = time.perf_counter() - t0

    return merged, elapsed


def lab2():
    print("=" * 65)
    print("LAB 2: Spark-Like Per-Partition Metrics — False Sharing Fix")
    print("=" * 65)

    N_ROWS = 5_000_000
    N_THREADS = 8

    rng = random.Random(42)
    data = [rng.randint(0, 1000) for _ in range(N_ROWS)]

    for n_partitions in [8, 64, 512, 1000]:
        r1, t1 = simulate_task_false_sharing(data, n_partitions, N_THREADS)
        r2, t2 = simulate_task_thread_local(data, n_partitions, N_THREADS)

        # Verify correctness
        assert sum(r1.values()) == sum(r2.values()), "Row count mismatch!"

        print(f"\n  n_partitions={n_partitions}:")
        print(f"    Shared array:    {t1*1000:.1f} ms  (false sharing risk)")
        print(f"    Thread-local:    {t2*1000:.1f} ms  (no sharing)")
        if t2 > 0:
            print(f"    Speedup: {t1/t2:.1f}×")

        if n_partitions <= 8:
            print(f"    ← High false sharing risk: {n_partitions} counters "
                  f"fit in {(n_partitions*8+63)//64} cache line(s)")
        else:
            print(f"    ← Lower false sharing risk: {n_partitions} counters "
                  f"span {(n_partitions*8+63)//64} cache lines")

    print(f"\n  Key insight:")
    print(f"  Thread-local accumulation is always correct and avoids false sharing.")
    print(f"  This is exactly what Spark's TaskMetrics design does:")
    print(f"  each task has its own local metrics, merged at task completion.")
    print(f"  The merge step is O(n_tasks) — negligible vs O(n_rows) work.")


if __name__ == '__main__':
    lab2()
```

---

### Lab 3 — Profile and Fix False Sharing in a Custom Aggregator

**Goal:** Build a multi-threaded histogram aggregator, observe its performance under false sharing, then fix it using padding and thread-local strategies. Verify correctness and measure the improvement.

```python
"""
lab3_histogram_aggregator.py

Implements a parallel histogram aggregator with three strategies:
  1. Shared histogram (false sharing for narrow histograms)
  2. Padded per-thread histogram (no false sharing)
  3. Thread-local histogram + merge (no false sharing + better cache utilization)

Compares correctness and performance across strategies.

Run: python3 lab3_histogram_aggregator.py
"""
import ctypes
import threading
import time
import random
from typing import List

CACHE_LINE = 64
N_BINS = 16   # small enough that all bins fit in < 2 cache lines


def parallel_histogram_shared(data: List[int], n_bins: int,
                               n_threads: int) -> List[int]:
    """
    Strategy 1: All threads write to shared histogram array.
    Classic false sharing scenario for small n_bins.
    """
    HistArray = ctypes.c_int64 * n_bins
    hist = HistArray(*([0] * n_bins))
    chunk = len(data) // n_threads

    def worker(tid):
        start = tid * chunk
        end = start + chunk if tid < n_threads - 1 else len(data)
        for i in range(start, end):
            hist[data[i] % n_bins] += 1  # race + false sharing

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    return list(hist)


def parallel_histogram_padded(data: List[int], n_bins: int,
                               n_threads: int) -> List[int]:
    """
    Strategy 2: Each thread has its own padded histogram.
    No false sharing: each thread's histogram is on separate cache lines.
    """
    # Each per-thread histogram: n_bins × 8 bytes, padded to cache line multiple
    padded_size = ((n_bins * 8 + CACHE_LINE - 1) // CACHE_LINE) * CACHE_LINE
    # Total buffer: n_threads × padded_size bytes
    TotalBuffer = ctypes.c_uint8 * (n_threads * padded_size)
    buffer = TotalBuffer()

    def get_slot(tid, bin_idx) -> ctypes.c_int64:
        """Get pointer to thread tid's bin bin_idx."""
        offset = tid * padded_size + bin_idx * 8
        return ctypes.c_int64.from_buffer(buffer, offset)

    chunk = len(data) // n_threads

    def worker(tid):
        start = tid * chunk
        end = start + chunk if tid < n_threads - 1 else len(data)
        for i in range(start, end):
            slot = get_slot(tid, data[i] % n_bins)
            slot.value += 1

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n_threads)]
    for t in threads: t.start()
    for t in threads: t.join()

    # Merge: sum per-thread histograms
    result = [0] * n_bins
    for tid in range(n_threads):
        for b in range(n_bins):
            result[b] += get_slot(tid, b).value
    return result


def parallel_histogram_local(data: List[int], n_bins: int,
                               n_threads: int) -> List[int]:
    """
    Strategy 3: Thread-local Python list, merge at end.
    Zero false sharing. Best cache utilization (local list stays in L1).
    """
    local_hists = [None] * n_threads
    chunk = len(data) // n_threads

    def worker(tid):
        start = tid * chunk
        end = start + chunk if tid < n_threads - 1 else len(data)
        local = [0] * n_bins  # pure Python list, stack-like locality
        for i in range(start, end):
            local[data[i] % n_bins] += 1
        local_hists[tid] = local

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n_threads)]
    for t in threads: t.start()
    for t in threads: t.join()

    # Merge
    result = [0] * n_bins
    for h in local_hists:
        for b in range(n_bins):
            result[b] += h[b]
    return result


def lab3():
    print("=" * 65)
    print("LAB 3: Parallel Histogram — False Sharing to Padded Fix")
    print("=" * 65)

    N = 10_000_000
    N_THREADS = 8
    N_BINS = 16  # 16 bins × 8 bytes = 128 bytes = 2 cache lines

    rng = random.Random(42)
    data = [rng.randint(0, 1000) for _ in range(N)]

    RUNS = 3
    strategies = [
        ("Shared histogram (false sharing)", parallel_histogram_shared),
        ("Padded per-thread histogram",      parallel_histogram_padded),
        ("Thread-local + merge (best)",      parallel_histogram_local),
    ]

    results = {}
    print(f"\n{N:,} elements, {N_BINS} bins, {N_THREADS} threads")
    print(f"Cache line impact: {N_BINS} × 8 bytes = {N_BINS*8} bytes "
          f"= {(N_BINS*8+63)//64} cache lines\n")

    for name, fn in strategies:
        times = []
        hist = None
        for _ in range(RUNS):
            t0 = time.perf_counter()
            hist = fn(data, N_BINS, N_THREADS)
            times.append(time.perf_counter() - t0)
        avg = min(times)
        results[name] = (avg, hist)
        print(f"  {name:<40}  {avg*1000:.1f} ms")

    # Verify all produce the same result (shared may have races)
    ref_hist = results["Thread-local + merge (best)"][1]
    print(f"\n  Reference histogram (thread-local): {ref_hist[:8]}...")

    print(f"\n  Correctness check:")
    for name, (_, hist) in results.items():
        match = (sum(hist) == N)
        race  = (hist != ref_hist) and "shared" in name.lower()
        status = ("✅ correct" if match and not race
                  else "⚠️  races detected" if race
                  else "❌ wrong count")
        print(f"    {name:<40}: {status}")

    print(f"\n  Note: 'Shared histogram' may show incorrect counts due to")
    print(f"  non-atomic increments (race condition) in addition to false sharing.")
    print(f"  In practice, per-thread histograms + merge is always correct AND fast.")


if __name__ == '__main__':
    lab3()
```

---

## 15. Summary

False sharing is the performance pathology that breaks the assumption that independent writes scale linearly with thread count. Two threads writing to logically independent variables suffer full cache coherence penalties — RFO round-trips of 40–500 cycles per write — if those variables happen to share a 64-byte cache line. The penalty is identical in magnitude to two threads genuinely fighting over the same data, but there is no logical conflict — only a physical layout accident.

**The MESI protocol** maintains cache coherence by allowing at most one Modified copy of any cache line at a time. When a second core wants to write, it must acquire exclusive ownership via an RFO, invalidating all other copies. This protocol operates at cache line granularity — it cannot distinguish bytes within a line. False sharing exploits this: two "independent" writes to bytes within the same line trigger the full RFO sequence.

**Detection:** `perf stat -e LLC-load-misses` shows unexpectedly high LLC miss rate for a small working set. `perf c2c` pinpoints the exact memory addresses experiencing cache-to-cache transfers (the "Hitm" column — Hit in Modified state).

**Fixes in priority order:**

1. **Thread-local accumulation + merge:** Each thread writes to a stack or thread-local variable. Zero coherence traffic during the parallel phase. Merge is O(n_threads), always negligible. This is the correct design for any per-thread accumulator (Spark TaskMetrics, custom metrics counters).

2. **Padding to 64 bytes:** When threads must write to a shared structure, ensure each thread's field is on its own cache line by padding to 64 bytes. `alignas(64)` in C/C++, `@Contended` in Java, explicit `_pad` fields in ctypes.

3. **Use contention-aware types:** In Java, `LongAdder` over `AtomicLong` for high-contention counters. `LongAdder` internally uses `@Contended`-padded cells.

---

## 16. Interview Q&A

### Q1: What is false sharing and why does it cause performance degradation even when threads write to different variables?

**Answer:**

False sharing occurs when two or more CPU cores write to distinct variables that happen to reside within the same 64-byte cache line. The CPU's cache coherence protocol (MESI) operates at cache line granularity — it cannot distinguish accesses to individual bytes within a line. From the hardware's perspective, any write to the line requires exclusive ownership of the entire line.

The mechanism: when core 0 writes its variable and the cache line is in Modified state on core 0, core 1's write to a different variable in the same line triggers a Read For Ownership request. Core 0 must invalidate its copy (M→I transition) and send the line to the coherence interconnect. Core 1 receives the line and transitions to Modified (I→M). The next time core 0 writes, the same sequence repeats in reverse. The cache line bounces continuously between cores in Modified state — M→I→M→I — with each transition costing 40–60 cycles on the same socket (300–500 cycles cross-socket via Intel UPI).

The result: what should be a ~4-cycle L1 cache write (line already Modified on the writing core) becomes a 40–500 cycle operation. For a tight loop that writes a counter per iteration, this is a 10–125× slowdown per write. Since the threads are logically independent — neither reads the other's data — the coherence traffic is pure overhead with zero logical necessity.

The counterintuitive aspect: adding more threads makes the situation *worse*, not better. Eight threads writing to eight logically independent variables in the same cache line create an 8-way ping-pong of the line among eight cores. Each write must wait its turn for M-state ownership, serializing all eight threads through a shared hardware bottleneck.

---

### Q2: How do you detect false sharing in production JVM code, and what are the standard fixes?

**Answer:**

Detection has two levels. The first is behavioral: a multithreaded program that shows worse-than-expected scaling under parallel write load. If adding threads beyond a certain point provides no speedup (or degrades performance), false sharing is a candidate. Profile with `perf stat -e cache-misses,LLC-load-misses` — a small working set with high LLC-load-miss rate suggests that cache lines are being invalidated and refetched despite fitting in L3, which is the false sharing signature.

The second level is precise identification via `perf c2c record && perf c2c report`. The `c2c` (cache-to-cache) tool tracks cross-core cache line transfers. The output's "Hitm" column counts accesses that hit a cache line in the Modified state on another core — the precise signature of false sharing. This points to the exact memory address and code location of the offending writes.

Within the JVM, the standard diagnostic is to look for `AtomicLong[]` arrays, `long[]` arrays, or collections of small objects that are written by different threads. Java's `AtomicLong` is 24 bytes (header + value). Two or three `AtomicLong` objects fit per cache line if allocated contiguously.

The fixes in the JVM ecosystem:

The first is using `LongAdder` instead of `AtomicLong` for high-contention incrementing. `LongAdder` extends `Striped64`, which maintains a `Cell[]` array where each `Cell` is annotated with `@jdk.internal.vm.annotation.Contended`. The JVM adds 128 bytes of padding around any `@Contended`-annotated field, ensuring each cell is isolated on its own cache line. At low contention, `LongAdder` uses its base field; at high contention, threads hash to their own `Cell` — no false sharing.

The second is the thread-local-then-merge pattern: each thread accumulates into a `ThreadLocal<Long>` or a stack variable, and a sequential merge step sums all thread-local values after the parallel phase. This produces zero coherence traffic during accumulation. The merge is O(n_threads), always negligible relative to O(n_rows) work.

The third, for low-level struct layouts (Cython/JNI extensions, off-heap memory), is explicit padding: ensure each thread's writable field is at a 64-byte-aligned offset and occupies a full 64-byte region. `__attribute__((aligned(64)))` in C, `@Contended` in Java, `alignas(64)` in C++17.

---

### Q3: How does false sharing interact with NUMA topology on a two-socket server?

**Answer:**

False sharing on a two-socket NUMA server is dramatically more expensive than on a single socket because cache coherence across sockets travels via the inter-socket interconnect (Intel UPI at ~41 GB/s, AMD Infinity Fabric) rather than the shared L3. The RFO round-trip latency increases from ~40–60 cycles (same-socket L3 path) to ~300–500 cycles (cross-socket UPI path) — a 5–8× increase in the coherence penalty.

In a practical deployment: a Spark executor running 4 threads, with 2 threads pinned to socket 0 and 2 threads pinned to socket 1 (common when using `-XX:+UseNUMA` with multiple executors per node), has false sharing penalties that are cross-socket. A shared metrics array whose cache lines bounce between socket 0 and socket 1 via UPI pays the full 300–500 cycle cost per write. This is equivalent to a DRAM access — the "false sharing" overhead for a simple counter increment becomes 300 cycles rather than 40.

The interaction compounds: NUMA-aware JVM configurations (M02's `-XX:+UseNUMA` discussion) partition heap regions by NUMA node to avoid cross-socket heap accesses. But if a per-thread counter array is allocated on socket 0's heap and threads on socket 1 write to it, the cross-socket access is a first-touch NUMA placement issue *and* a false sharing issue simultaneously. Each write requires a cross-socket RFO: get the line from socket 0's L3 (300+ cycles), modify it, write it back to socket 1's L3 as Modified — only for socket 0's threads to issue another RFO the next time they write.

The fix is the same as for single-socket false sharing (padding or thread-local), but the urgency is higher because the penalty is 5–8× larger. Detection: `perf c2c` on a NUMA server shows cross-socket cache transfers specifically in the NUMA-remote access columns.

---

### Q4: Why does `volatile` in Java not prevent false sharing, and what does it actually guarantee?

**Answer:**

`volatile` in Java provides two guarantees: visibility (a write to a `volatile` field is visible to all subsequent reads of that field by any thread) and ordering (writes before a `volatile` write happen-before reads after the corresponding `volatile` read). It does *not* provide padding. It does not alter the memory layout of the object containing the `volatile` field. Two `volatile long` fields declared adjacently in a class still occupy adjacent 8-byte slots — still potentially within the same 64-byte cache line.

What `volatile` does at the hardware level: a `volatile` write compiles to a store followed by a StoreLoad fence (on x86, this is a locked instruction or MFENCE). The fence ensures the store is flushed from the CPU's store buffer and the write is visible to other cores' caches before any subsequent load on this core executes. The fence triggers the MESI M→S→I transition sequence: the written line transitions to Modified on this core, and on the next access by another core, the snooping protocol ensures that core sees the updated value (either from this core's cache or from L3 after this core writes back).

But this is the mechanism by which `volatile` enforces visibility — which is exactly the same mechanism that causes false sharing. A `volatile` write to one field in a cache line triggers the same RFO sequence that any write to the line would trigger. The MESI state machine doesn't distinguish `volatile` from non-`volatile` writes at the hardware level: both result in Modified state on the writing core, Shared or Invalid on other cores.

So: if thread 0 writes to `volatile long a` and thread 1 writes to `volatile long b`, and `a` and `b` are in the same cache line, both writes will trigger cross-core RFOs — false sharing is present, and the `volatile` semantics make it slightly worse (the fence ensures the write is immediately visible, rather than potentially staying in the store buffer for a few cycles).

The correct fix remains padding: place `a` and `b` in separate cache lines via `@Contended`, explicit padding fields, or thread-local storage.

---

### Q5: How does Spark's accumulator design prevent false sharing, and when can it reintroduce it?

**Answer:**

Spark's accumulator design follows the thread-local-then-merge pattern at the task level. Each task executes with its own `TaskMetrics` object, which contains the task's local accumulator state. During task execution, the task updates only its own `TaskMetrics` — no other task touches these objects. At task completion, the executor sends the accumulated values to the driver, which merges them into the global accumulator state. The driver-side merge is sequential (single thread) and operates on final values, not on continuously-updated hot counters.

This architecture ensures zero false sharing during task execution: each task's accumulators are in separate `TaskMetrics` objects, allocated independently on the JVM heap. Different tasks' `TaskMetrics` objects are unlikely to share cache lines because they are separate heap-allocated objects — even if contiguous, the objects are large enough (containing many fields) that individual accumulator fields from different tasks will rarely collide on the same cache line.

False sharing can be reintroduced in two ways. First, if a user creates a custom accumulator backed by an `AtomicLong[]` or `long[]` array (rather than per-task local variables) and multiple executor threads write to adjacent array slots, the array elements are 8 bytes each — multiple elements per cache line — and false sharing results. This typically happens when someone tries to track per-partition output statistics using a shared array instead of aggregating in the reduce step.

Second, if a user stores many small `LongAccumulator` objects in a list and multiple tasks share references to adjacent accumulators in that list, the JVM may allocate them contiguously (e.g., from the same TLAB on executor initialization), and tasks on different cores writing to adjacent accumulators in the list cause false sharing. The fix: use `LongAdder` (which is internally `@Contended`-padded) or, better, redesign the accumulation to use task-local variables that are summed in a reduce step rather than incrementing a shared counter per row.

---

### Q6: A data engineering team reports that their Cython aggregation extension runs fast on 1 thread, but performance stops improving beyond 2 threads on a 16-core machine. How would you diagnose and fix this?

**Answer:**

Non-linear scaling that plateaus early (near 2 threads on a 16-core machine) with CPU-bound work is a textbook false sharing signature, though I'd rule out other causes first. The diagnostic approach:

Confirm it's CPU-bound and not I/O-bound: `perf stat` should show high CPU utilization, close to 200% for 2 threads and near 100% for 1. If CPU utilization scales but wall-clock time does not, the bottleneck is within the CPU — coherence, lock contention, or NUMA.

Run `perf stat -e cache-misses,LLC-load-misses` with 1 and 2 threads. If LLC-load-misses scales faster than the number of threads (e.g., 5× more misses with 2 threads despite 2× more work), false sharing is very likely. With 16-thread false sharing, LLC-load-misses can increase 50–100× vs sequential, while actual work only doubles.

If available, run `perf c2c record -- ./your_program` and look at the Hitm count in the report. High Hitm on aggregation output fields confirms false sharing.

For the fix: examine the Cython extension's struct layout. The likely culprit is a per-thread result struct or a shared output array where multiple threads write to adjacent elements. Specific patterns to find:

A `cdef int64_t results[N_THREADS]` array used as `results[thread_id] += count`. This is 8 bytes per slot — false sharing guaranteed for ≤ 8 threads on one cache line. Fix: `cdef int64_t results[N_THREADS * 8]` and write to `results[thread_id * 8]` instead — this spaces each thread's result 64 bytes apart.

A shared accumulator struct with multiple fields written by different threads: pad each field to 64 bytes with `cdef int64_t _pad[7]` after each active field.

The deepest fix: have each thread accumulate into a local C variable (`cdef int64_t local_count = 0`) in a `nogil` block, and write the result back to the shared array exactly once at the end of the thread's work. This eliminates all coherence traffic during the parallel phase and reduces the shared-array write to a single write per thread — at most N_THREADS writes total, which is negligible.

---

## 17. Cross-Question Chain

**Topic:** From "what is false sharing?" to "how do you design a zero-false-sharing parallel aggregation for Spark?"

---

**Interviewer:** What is false sharing?

**Candidate:** False sharing is when two CPU cores write to different variables that happen to occupy the same 64-byte cache line. The MESI coherence protocol invalidates the cache line on every write regardless of which bytes were actually modified — it operates at cache line granularity, not byte granularity. So even though the threads have no logical dependency, the hardware forces them to take turns acquiring exclusive ownership of the line. Each write costs ~40–500 cycles instead of ~4 cycles, depending on whether the cores are on the same socket or different sockets.

---

**Interviewer:** How does MESI cause that overhead?

**Candidate:** MESI maintains the invariant that at most one core holds a Modified (dirty) copy of any cache line at a time. When core 1 wants to write to a line that core 0 holds Modified, core 1 issues a Read For Ownership request to the L3 coherence agent. The agent sends an invalidation to core 0 — core 0's copy transitions from Modified to Invalid and is written back. Core 1 receives the line, transitions to Modified, and performs its write. This round-trip is ~40–60 cycles on the same socket via the shared L3. If core 0 wants to write next, the entire sequence reverses. False sharing makes this happen on every write, converting a 4-cycle L1 store into a 40-cycle coherence operation.

---

**Interviewer:** How would you detect it in a running program?

**Candidate:** Two tools. First, `perf stat -e LLC-load-misses` compared between 1 thread and N threads. If LLC misses scale superlinearly with thread count despite a small working set (which should fit in L3), something is invalidating the lines — likely false sharing. Second, `perf c2c record` followed by `perf c2c report` on Linux kernel ≥ 4.3. The report's "Hitm" column counts accesses that hit a cache line in the Modified state on another core — the exact false sharing signature. It also shows the address and code location, so you can see which field in which struct is the problem.

---

**Interviewer:** What are your fix options?

**Candidate:** Three options in order of preference. The first and best is thread-local accumulation: each thread writes to a stack variable or `ThreadLocal`, and the results are merged after the parallel phase. This produces zero coherence traffic during accumulation. The merge is O(n_threads) — always negligible. The second is padding: ensure each thread's mutable field is on its own 64-byte cache line by adding padding bytes between fields (`alignas(64)` in C, `@Contended` in Java, explicit `char _pad[56]` in ctypes). The third is using contention-aware types: in Java, `LongAdder` instead of `AtomicLong` — `LongAdder` internally uses `@Contended`-padded cells that automatically hash threads to separate cache lines.

---

**Interviewer:** How does this apply to a Spark aggregation job where you need to count rows per output partition?

**Candidate:** Spark's correct design is already thread-local-then-merge at the task level. Each task has its own `TaskMetrics` with local counters. But there's a common anti-pattern: creating a shared `LongAccumulator[]` array with one entry per output partition and having all tasks increment their slot. If n_partitions is small (≤ 8), all slots fit in one cache line — all executor threads fight over it. For a job with 8 executor threads and 8 output partitions, this is worst-case false sharing.

The correct implementation: each task accumulates a local `HashMap<partitionId, Long>` (stack variable, zero sharing), then emits the map at the end of the task. A Spark `reduceByKey` or a driver-side merge sums the per-task maps. Total coherence traffic during the parallel phase: zero. The merge is sequential over a tiny map.

---

**Interviewer:** And if you need real-time visibility into those counts during the job?

**Candidate:** That requires true sharing — you genuinely need other threads or the driver to read live values — so some coherence traffic is unavoidable. The right tool here is `LongAdder` (one per partition), which uses internal striping: each thread hashes to its own `@Contended`-padded cell, so concurrent increments to the *same* `LongAdder` don't cause false sharing. Multiple `LongAdder` objects for different partitions are separate heap objects — no false sharing between partitions. Reading a `LongAdder`'s current value (`sum()`) iterates and sums all cells sequentially — accurate enough for monitoring. This is the correct design for Kafka Streams per-partition metrics and Spark's built-in streaming accumulators.

---

## 18. What's Next

**CSF-ARC-102 Course Complete.** You have now covered the full Memory Architecture curriculum:

- **M01** — Cache lines: the 64-byte transfer unit, cache hierarchy latencies, DRAM physics.
- **M02** — NUMA: non-uniform memory access, first-touch placement, inter-socket bandwidth penalty.
- **M03** — TLB and virtual memory: page table walks, huge pages, THP vs HugeTLB.
- **M04** — Hardware prefetcher: sequential vs random access, MSHR budget, bandwidth vs latency.
- **M05** — False sharing and cache coherence: MESI protocol, RFO penalty, padding, thread-local.

These five modules form a unified model of how the CPU interacts with memory at every level. Every performance number in data engineering systems — scan throughput, join latency, shuffle overhead, GC pause duration — is ultimately explained by some combination of these five topics.

**CSF-ARC-103: CPU Internals** — The next course moves from memory architecture to compute architecture. M01 covers the instruction pipeline and branch prediction (why branch mispredictions cost 15–20 cycles and how to write branch-predictor-friendly code). M02 covers SIMD (AVX2, AVX-512) and how vectorized operations process 4–16 values per cycle. M03 covers IPC (instructions per cycle) measurement and optimization. M04 covers the relationship between CPU IPC and JVM JIT compilation — why hot loops get vectorized, and how to verify that vectorization occurred.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is false sharing? | Two CPU cores writing to logically independent variables that occupy the same 64-byte cache line. The MESI protocol treats the line atomically, forcing RFO overhead on every write even though the data is independent. |
| 2 | What is an RFO (Read For Ownership)? | A coherence request issued when a core wants to write to a cache line it doesn't hold exclusively. The protocol invalidates all other copies and delivers the line in Modified state to the requesting core. Cost: ~40–60 cycles (same socket), ~300–500 cycles (cross-socket). |
| 3 | What are the four MESI states? | Modified (dirty, exclusive — one core has writable copy), Exclusive (clean, exclusive — one copy, matches DRAM), Shared (clean, multiple cores have read-only copies), Invalid (no valid copy). |
| 4 | Which MESI state transition is "free" (no coherence traffic)? | E→M (Exclusive to Modified): the core already has the only copy, so it can silently start writing without notifying anyone. No RFO, no invalidation. |
| 5 | Which MESI state transition costs an RFO? | S→M (Shared to Modified) and I→M (Invalid to Modified, another core has it Modified). Both require invalidating other cores' copies before the write can proceed. |
| 6 | What is the write cost difference between an L1 hit (M state) and false sharing? | L1 hit in M state: ~4 cycles. False sharing (I state, another core has M): ~40–60 cycles same socket, ~300–500 cycles cross-socket. 10–125× slower. |
| 7 | How do you detect false sharing with perf? | `perf stat -e LLC-load-misses` for symptom (high miss rate with small working set). `perf c2c record && perf c2c report` for precise identification — look for high "Hitm" (Hit in Modified) count. |
| 8 | What does "Hitm" mean in perf c2c output? | "Hit in Modified" — a cache access that found the line in the Modified state on another core. Requires RFO. High Hitm count = false sharing confirmed at that address. |
| 9 | Why does `volatile` in Java not prevent false sharing? | `volatile` controls visibility and memory ordering, not memory layout. Two `volatile long` fields declared adjacently still occupy adjacent 8-byte offsets in the same object — potentially within the same 64-byte cache line. The volatile write triggers the same MESI M-state transitions that cause false sharing. |
| 10 | What is the primary fix for false sharing? | Thread-local accumulation + merge: each thread writes to a stack variable or ThreadLocal, and results are merged sequentially after the parallel phase. Zero coherence traffic during accumulation. |
| 11 | How does padding eliminate false sharing? | Padding adds unused bytes after a variable so it occupies an entire 64-byte cache line by itself. Other threads' variables are on different cache lines → no cross-core RFO traffic when they write. |
| 12 | How much padding is needed per counter to eliminate false sharing? | 64 - sizeof(counter) bytes. For int64 (8 bytes): 56 bytes of padding → 64 bytes total → one full cache line. In Java: 128 bytes of padding (to handle both 64-byte and 128-byte sector CPUs). |
| 13 | Why is `LongAdder` better than `AtomicLong` under high contention? | `LongAdder` uses an internal `Cell[]` array where each `Cell` is `@Contended`-padded. Threads hash to different cells → no false sharing. `AtomicLong` is one variable → all threads fight over one cache line → RFO on every increment. |
| 14 | What is `@Contended` in Java? | JVM annotation (`@jdk.internal.vm.annotation.Contended`) that instructs the JVM to add 128 bytes of padding around the annotated field. Used internally by `LongAdder`'s `Cell` class and `Thread`'s thread-local fields. |
| 15 | How does Spark prevent false sharing in task metrics? | Each task has its own `TaskMetrics` object (separate heap allocation). Tasks only write to their own `TaskMetrics` during execution. Results are merged by the driver after task completion. This is the thread-local-then-merge pattern at the task granularity. |
| 16 | Why is false sharing worse on a two-socket NUMA server? | Cross-socket RFO travels via Intel UPI or AMD Infinity Fabric: ~300–500 cycles vs ~40–60 cycles for same-socket. False sharing between threads on different sockets pays 5–8× higher coherence penalty. |
| 17 | True sharing vs false sharing: what distinguishes them? | True sharing: threads access the same logical data (the coherence traffic enforces necessary ordering). False sharing: threads access different data that is physically co-located. True sharing is unavoidable. False sharing is a layout bug — cured by padding or thread-local patterns. |
| 18 | Why does adding threads make false sharing worse? | More threads → more writers competing for M-state ownership of the same cache line → the line bounces among more cores → more total RFO round-trips per second. 8 threads on one cache line is 8× worse than 2 threads (in the worst case). |
| 19 | What is the `thread-local then merge` pattern? | Each thread accumulates into a private (stack or ThreadLocal) variable during the parallel phase. After all threads complete, one thread iterates the results and computes the aggregate. Zero coherence traffic during accumulation; merge is O(n_threads). |
| 20 | What MESI state does a cache line return to after an RFO-write by core 1 when core 0 held M? | Core 0: M → I (invalidated). Core 1: I → M (acquires exclusive modified ownership). DRAM: still stale (write-back happens on eviction, not immediately). |

---

## 20. References

**Cache Coherence Protocols**

- Sorin, D. J., Hill, M. D., & Wood, D. A. (2011). A Primer on Memory Consistency and Cache Coherence. *Synthesis Lectures on Computer Architecture*. https://doi.org/10.2200/S00346ED1V01Y201104CAC016 — The definitive academic treatment of MESI and related protocols.
- Hennessy, J. L., & Patterson, D. A. (2019). *Computer Architecture: A Quantitative Approach* (6th ed.). Section 5.2 (Cache Coherence). — MESI state machine, directory protocols, RFO cost models.

**False Sharing Detection**

- Dickey, J. (2016). perf-c2c: A tool for shared data false-sharing analysis. *Linux Kernel mailing list*. — The original documentation for `perf c2c`. Available in the Linux kernel tree: `tools/perf/Documentation/perf-c2c.txt`.
- Intel (2024). Intel VTune Profiler User Guide: Memory Access Analysis. https://www.intel.com/content/www/us/en/docs/vtune-profiler/user-guide/ — VTune's "Memory Access" view specifically identifies false sharing with hardware performance counters.

**JVM Memory Model**

- Shipilev, A. (2014). Close Encounters of The Java Memory Model Kind. https://shipilev.net/blog/2016/close-encounters-of-jmm-kind/ — The best practical explanation of how Java's `volatile` and `Atomic*` map to hardware MESI operations.
- OpenJDK — `java.util.concurrent.atomic.Striped64` source. https://github.com/openjdk/jdk/blob/master/src/java.base/share/classes/java/util/concurrent/atomic/Striped64.java — The `LongAdder` implementation with `@Contended` cells.

**Data Engineering Applications**

- Zaharia, M., et al. (2016). Apache Spark: A Unified Engine for Big Data Processing. *CACM*, 59(11). https://dl.acm.org/doi/10.1145/2934664 — Section 3 (programming model) describes Spark's accumulator and task-local-then-merge design.
- Bakhvalov, D. (2021). False Sharing. easyperf.net. https://easyperf.net/blog/2019/12/17/Improving-performance-by-better-code-patterns — Practical code-level false sharing examples with perf measurements.
- Agner Fog (2024). Optimizing software in C++. Chapter 13 (Multi-threaded programs). https://www.agner.org/optimize/optimizing_cpp.pdf — False sharing, padding, and NUMA interactions.
