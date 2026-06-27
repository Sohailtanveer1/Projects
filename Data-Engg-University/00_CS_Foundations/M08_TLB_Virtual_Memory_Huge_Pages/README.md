# M03: TLB, Virtual Memory, and Huge Pages

**School:** CS Foundations (CSF)  
**Course:** CSF-ARC-102 — Memory Architecture  
**Module:** 03 of 05  
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
6. [Deep Dive — TLB Mechanics](#6-deep-dive--tlb-mechanics)
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

Every memory access described in M01 and M02 was stated in terms of physical addresses: "the CPU loads from address 0x7f0000000040." This was a simplification. In reality, the CPU never sees physical addresses directly. Programs work entirely in **virtual addresses** — a per-process fiction maintained by the OS and hardware. The mapping from virtual to physical addresses must be resolved before every single memory access.

That resolution — address translation — has a cost. If you had to look up the virtual→physical mapping in a data structure in DRAM for every single instruction fetch and data load, your program would run at a fraction of its potential speed. The solution is the **Translation Lookaside Buffer (TLB)**: a tiny, extremely fast cache of recent virtual→physical translations built into the CPU.

The TLB has room for only 64–1536 translations. Each translation covers one **page** — 4 KB by default. That means the TLB covers only 64 × 4 KB = 256 KB of unique addresses. If your program accesses more than 256 KB of unique memory (nearly every data engineering workload does), the TLB is the bottleneck: each new page accessed outside the TLB requires a hardware **page table walk** — 3–4 sequential DRAM accesses costing 200+ nanoseconds.

**Huge pages** (2 MB instead of 4 KB) solve this. Each TLB entry covers 2 MB instead of 4 KB — 512× more coverage. A Spark executor with a 64 GB heap accessing it through 4 KB pages generates TLB misses on every new 4 KB region touched. The same heap through 2 MB huge pages has 512× fewer TLB misses and runs measurably faster on memory-intensive workloads.

For data engineers: every large array (NumPy, Arrow, Spark off-heap, JVM heap), every Parquet read buffer, every shuffle write buffer — these are workloads where TLB behavior is a real production performance factor. This module explains the mechanism and the fix.

---

## 2. Prerequisites

- CSF-ARC-101 M03: ISA contract, syscall mechanics, system privilege levels (ring 0 vs ring 3).
- CSF-ARC-102 M01: Cache lines, LLC misses, DRAM access patterns.
- CSF-ARC-102 M02: NUMA topology, first-touch NUMA policy (relevant to THP page placement).

---

## 3. Learning Objectives

By the end of this module you will be able to:

1. Explain the purpose of virtual memory and why programs use virtual addresses rather than physical addresses.
2. Describe the 4-level x86-64 page table structure and trace a virtual address to a physical address through each level.
3. Define the TLB: what it stores, how large it is, and what the coverage gap means for data engineering workloads.
4. Calculate the TLB miss rate and its performance impact for a given access pattern and working set size.
5. Explain the difference between 4 KB standard pages, 2 MB huge pages, and 1 GB gigantic pages.
6. Enable and verify Transparent Huge Pages (THP) on Linux, and explain when THP helps vs when it hurts.
7. Allocate explicit huge pages with `mmap(MAP_HUGETLB)` or Python `mmap` and verify the page size.
8. Measure TLB miss rate with `perf stat -e dTLB-load-misses,dTLB-loads` and interpret the result.
9. Configure the JVM and Spark to use huge pages for executor heap memory.

---

## 4. First Principles

### Why Virtual Memory Exists

Without virtual memory, every program would need to know exactly which physical RAM addresses it uses — and never overlap with another program. On a machine running 50 processes simultaneously, this requires either:
- **Static allocation:** assign fixed physical regions to each process at compile time. Inflexible; wastes RAM if a process uses less than allocated.
- **Manual relocation:** the OS rewrites all addresses when loading a program. Slow; breaks when programs store pointers.

Virtual memory solves both problems by giving each process a private address space that looks like it starts at 0 and can grow to terabytes — regardless of physical RAM size and other processes. The hardware (MMU — Memory Management Unit) transparently translates virtual addresses to physical addresses on every access, and the OS decides which physical frames each virtual page maps to.

**The four benefits of virtual memory:**

1. **Isolation:** Process A's address 0x1000 maps to different physical RAM than Process B's address 0x1000. They cannot accidentally corrupt each other.
2. **Larger-than-RAM working sets:** Pages not in physical RAM are stored on disk (swap). Accessed infrequently → evicted to swap. Accessed again → page fault → OS loads from swap. (Modern data engineering avoids swap at all costs, but the mechanism enables it.)
3. **Shared libraries:** One copy of `libc.so` can be mapped into all process address spaces simultaneously (same physical pages, different virtual addresses). Saves RAM.
4. **Memory-mapped files:** Files on disk can be accessed like memory — the OS handles loading pages on demand (this is how `mmap()` works, and how the OS implements the page cache).

### The Physical Address Bit

On x86-64, virtual addresses are 48 bits (64 TB of virtual address space per process). Physical addresses on modern hardware are 36–52 bits (64 GB to 4 PB of physical RAM). The mapping is many-to-one: many virtual addresses can map to the same physical page (shared libraries), and some virtual addresses map to no physical page at all (reserved but not yet allocated — demand paging).

---

## 5. Architecture

### The 4-Level x86-64 Page Table

```
64-bit virtual address (only 48 bits used):
  ┌──────┬──────────┬──────────┬──────────┬──────────┬──────────────┐
  │  16  │    9     │    9     │    9     │    9     │     12       │
  │ sign │  PML4    │  PDPT    │   PD     │   PT     │   offset     │
  │ ext  │  index   │  index   │  index   │  index   │ (in-page)    │
  └──────┴──────────┴──────────┴──────────┴──────────┴──────────────┘
  bit 63            47        38        29        20        11        0

  PML4  = Page Map Level 4 (1 per process, pointed to by CR3 register)
  PDPT  = Page Directory Pointer Table
  PD    = Page Directory
  PT    = Page Table
  offset = byte offset within the 4 KB page (0–4095)

Page table walk for virtual address 0x00007f0000041234:

  1. Read CR3 register → physical address of PML4 table
  2. PML4[0x00] → PDPT base address      (PML4 index = bits 47:39 = 0)
  3. PDPT[0x1FC] → PD base address       (PDPT index = bits 38:30)
  4. PD[0x000] → PT base address         (PD index = bits 29:21)
  5. PT[0x041] → physical frame number   (PT index = bits 20:12)
  6. Physical address = frame × 4096 + 0x234 (offset = bits 11:0)

Each step requires one DRAM read (if not in cache):
  4 DRAM reads × ~70 ns each = 280 ns total for one page table walk.
```

The 4-level page table is a radix tree with branching factor 512 at each level (9 bits = 512 entries). Each entry is 8 bytes. The total size of a full page table for a 4 TB virtual address space would be enormous — but in practice, most entries are null (page not mapped), and the tree is sparse.

```
Physical memory layout of the page table for one process:

  CR3 → PML4 table (4 KB, 512 entries)
            ↓ one entry per 512 GB of virtual space
         PDPT tables (4 KB each, one per PML4 entry used)
            ↓ one entry per 1 GB of virtual space
          PD tables (4 KB each, one per PDPT entry used)
            ↓ one entry per 2 MB of virtual space
           PT tables (4 KB each, one per PD entry used)
            ↓ one entry per 4 KB of virtual space (one per physical page)
```

### TLB Structure

```
CPU die (Intel Alder Lake example):

  Per-core:
  ┌──────────────────────────────────────────────────────────┐
  │  L1 dTLB (Data TLB)                                      │
  │  64 entries × 4 KB pages = 256 KB coverage              │
  │  4 entries × 2 MB pages = 8 MB coverage                 │
  │  Latency: 0–1 cycles (parallel with cache lookup)        │
  │  4-way set-associative                                   │
  │                                                          │
  │  L1 iTLB (Instruction TLB)                               │
  │  128 entries × 4 KB pages                               │
  └──────────────────────────────────────────────────────────┘
           │ on miss
           ▼
  ┌──────────────────────────────────────────────────────────┐
  │  L2 STLB (Second-Level TLB, Shared)                      │
  │  2048 entries × 4 KB pages = 8 MB coverage              │
  │  32 entries × 2 MB pages = 64 MB coverage               │
  │  Latency: ~7–8 cycles                                   │
  │  8-way set-associative                                   │
  └──────────────────────────────────────────────────────────┘
           │ on miss
           ▼
  ┌──────────────────────────────────────────────────────────┐
  │  Hardware Page Table Walker                              │
  │  Walks the 4-level page table in DRAM                    │
  │  Cost: 3–4 cache/DRAM accesses (~200–400 ns)            │
  │  Fills TLB entry on success                              │
  └──────────────────────────────────────────────────────────┘
```

---

## 6. Deep Dive — TLB Mechanics

### 6.1 TLB Hit vs TLB Miss

On every load/store instruction, the CPU must translate the virtual address to a physical address. This happens in parallel with the L1 cache tag lookup (the "VIPT" — Virtually Indexed, Physically Tagged — cache design used in modern x86-64 CPUs):

**TLB hit (address in L1 dTLB, ~0 extra cycles):**
```
CPU issues load from virtual address VA
  → L1 dTLB looked up in parallel with L1 cache:
      TLB returns physical frame number (PFN)
      L1 cache compares PFN to tag
  → If L1 cache hit: data returned in 4 cycles total
  → If L1 cache miss: physical address known, cache miss proceeds
```

**TLB miss, STLB hit (~8 cycles extra):**
```
L1 dTLB miss
  → L2 STLB lookup (8 cycles)
  → STLB hit: PFN returned, L1 dTLB filled
  → Memory access proceeds with 8-cycle penalty
```

**TLB miss, page table walk (~200–400 ns extra):**
```
L1 dTLB miss → L2 STLB miss
  → Hardware page table walker activated:
      Step 1: Read CR3 → PML4 base (register, 0 cycles)
      Step 2: PML4[index] → if in L1 cache: 4 cycles; else DRAM: ~70 ns
      Step 3: PDPT[index] → if in L1 cache: 4 cycles; else DRAM: ~70 ns
      Step 4: PD[index]   → if in L1 cache: 4 cycles; else DRAM: ~70 ns
      Step 5: PT[index]   → if in L1 cache: 4 cycles; else DRAM: ~70 ns
  → Best case (all in L1 cache): ~16 cycles overhead
  → Worst case (all in DRAM): ~280 ns overhead (~980 cycles at 3.5 GHz)
  → TLB filled with new entry (replaces one existing entry, LRU)
  → Memory access finally proceeds
```

**The key insight:** A page table walk is not one DRAM access — it is up to four sequential DRAM accesses. And because page table entries for different virtual pages reside at different physical addresses, these DRAM accesses are pseudo-random (dependent on the virtual address being translated). The hardware prefetcher cannot predict them. Page table walk latency is not hidden by prefetching.

### 6.2 TLB Coverage Calculation

```
L1 dTLB:  64 entries × 4 KB pages = 256 KB of unique virtual addresses
L2 STLB:  2048 entries × 4 KB pages = 8 MB of unique virtual addresses

If your working set > 8 MB (virtually certain in data engineering):
  Any access to a virtual page not in the STLB → page table walk

L1 dTLB with 2 MB huge pages:  4 entries × 2 MB = 8 MB coverage
L2 STLB with 2 MB huge pages: 32 entries × 2 MB = 64 MB coverage

With huge pages, an 8 MB working set fits entirely in L1 dTLB.
With 4 KB pages, it requires the L2 STLB.

For a 100 MB working set:
  4 KB pages: 100 MB / 4 KB = 25,600 pages → STLB (2,048 entries) thrashes
  2 MB pages: 100 MB / 2 MB = 50 pages     → fits in STLB (32 entries for 2 MB)
  1 GB pages: 1 page covers the whole 100 MB → 1 entry
```

### 6.3 TLB Miss Rate and Performance Impact

The TLB miss rate matters only when the page table walk cannot be hidden by out-of-order execution. For sequential access patterns with prefetching, the CPU may hide TLB miss latency in the pipeline (the page table walk and the next computation can overlap). For random access (hash table probes, pointer-chasing), each access stalls on both the TLB miss AND the subsequent cache miss — they are sequential.

**Formula for TLB miss overhead:**

```
TLB_overhead = miss_rate × miss_penalty × (1 - overlap_factor)

Where:
  miss_rate     = STLB misses / total loads
  miss_penalty  = ~280 ns (worst case, all 4 levels in DRAM)
                  ~16 cycles (best case, all 4 levels in L1 cache)
  overlap_factor = 0 for random access (cannot hide)
                   0.5–0.8 for sequential (prefetcher partially hides it)

For a hash join probe (random access):
  Working set 2 GB / 4 KB pages = 500,000 pages → all miss STLB (2,048 entries)
  Miss rate: ~100% for first access to each page; then TLB-warm for temporal reuse
  Effective miss rate after warmup: depends on reuse distance

  100M probes, 1% STLB miss rate, 280 ns miss penalty:
  = 1,000,000 × 280 ns = 280 ms overhead just from TLB misses

  Same with 2 MB huge pages (50 pages for 100 MB working set):
  = 100M probes × 0% STLB miss = 0 ms overhead (all in STLB)
```

### 6.4 Page Sizes: 4 KB vs 2 MB vs 1 GB

| Page size | TLB coverage (L2 STLB) | Use case | Linux name |
|---|---|---|---|
| 4 KB | 8 MB (2,048 entries × 4 KB) | Default; everything | standard page |
| 2 MB | 64 MB (32 entries × 2 MB) | Large arrays, JVM heap | hugepage / HugeTLB |
| 1 GB | Depends on CPU (rare) | Very large static mappings | gigantic page |

**Why not always use 2 MB pages?**
- 2 MB pages must be **physically contiguous**: a single 2 MB page occupies 2 MB of physically consecutive RAM. Over time, RAM becomes fragmented; the OS cannot always find a 2 MB contiguous block. Small allocations stick to 4 KB.
- Wasted memory for small allocations: a 10-byte allocation in a 2 MB page wastes 2 MB - 10 bytes = 2,097,142 bytes.
- mmap semantics: some memory-mapped operations require 4 KB granularity.

For large, long-lived allocations (JVM heap, large NumPy arrays, Spark off-heap), 2 MB huge pages are almost always beneficial.

### 6.5 Transparent Huge Pages (THP)

Linux's **Transparent Huge Pages** (THP) is the kernel's automatic huge page system. When enabled, the kernel promotes contiguous 4 KB pages to a single 2 MB huge page when possible — automatically, without application changes.

**THP modes:**
```bash
# Check current THP mode:
cat /sys/kernel/mm/transparent_hugepage/enabled
# Possible outputs:
#   [always] madvise never    ← always try to use THP (default on most distros)
#   always [madvise] never    ← only use THP when application explicitly requests it
#   always madvise [never]    ← THP disabled

# Enable THP (always):
echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

# Enable THP (madvise only — application must call madvise(MADV_HUGEPAGE)):
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

# Disable THP:
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

**How THP works:**
1. Application calls `malloc(10 GB)` or `mmap()`.
2. Pages start as 4 KB (demand paged).
3. `khugepaged` (kernel thread) periodically scans for groups of 512 contiguous 4 KB pages that could be collapsed into one 2 MB page.
4. If a group qualifies (same virtual mapping, physically contiguous), `khugepaged` collapses them.
5. Future accesses to that 2 MB region use a single TLB entry.

**Check THP usage:**
```bash
cat /proc/meminfo | grep -i huge
# AnonHugePages:  12345678 kB  ← RAM currently used by THP
# HugePages_Total: 0            ← explicit hugepage pool (Section 6.6)
```

**THP defect: latency spikes.** When `khugepaged` collapses 512 pages into one, it must hold a write lock on the affected VMA (Virtual Memory Area) briefly. For latency-sensitive applications (Kafka, real-time Flink), THP can introduce 1–50 ms latency spikes during this collapse operation. The recommendation for latency-sensitive workloads is `madvise` mode (opt-in) or `never`.

### 6.6 Explicit Huge Pages (HugeTLB)

Linux also supports **explicit huge pages** via the HugeTLB subsystem. Unlike THP (which is automatic and transparent), HugeTLB requires pre-allocating a pool of huge pages at boot/runtime:

```bash
# Allocate 512 huge pages of 2 MB each = 1 GB of hugepage pool
echo 512 | sudo tee /proc/sys/vm/nr_hugepages
# Or: sudo sysctl -w vm.nr_hugepages=512

# Verify allocation
cat /proc/meminfo | grep HugePages
# HugePages_Total:     512    ← pool size
# HugePages_Free:      512    ← available
# Hugepagesize:       2048 kB ← 2 MB each

# Mount the hugetlbfs
sudo mkdir -p /mnt/hugepages
sudo mount -t hugetlbfs nodev /mnt/hugepages

# Now applications can use mmap(MAP_HUGETLB) to allocate from this pool
```

Applications access the HugeTLB pool via `mmap()` with `MAP_HUGETLB` flag, or by allocating from the hugetlbfs mount point. The JVM uses this via `-XX:+UseLargePages`.

---

## 7. Mental Models

### Mental Model 1 — The TLB as a Phone Book Cache

The page table is a phone book: virtual address → physical address. It lives in DRAM and has millions of entries. Looking it up on every call takes ~280 ns.

The TLB is a speed-dial list: the last 2,048 numbers you called (for STLB). Looking up an entry in speed-dial takes ~8 cycles. If the number isn't on your speed-dial, you look it up in the full phone book.

The tragedy: the phone book (page table) is in the same slow storage as your data (DRAM). You need to consult the phone book to figure out where your data is, but the phone book itself might not be cached.

**Huge pages expand speed-dial coverage:** with 4 KB pages, each speed-dial entry covers one phone number (one 4 KB page = one apartment). With 2 MB pages, each speed-dial entry covers a whole building (2 MB = 512 apartments). Same size speed-dial list — but you can cover 512× more unique addresses.

### Mental Model 2 — The Working Set and the TLB Budget

Draw two numbers for any workload:
1. **Working set size** (unique bytes accessed)
2. **TLB coverage** (L2 STLB entries × page size)

If working set ≤ TLB coverage: every repeated access is a TLB hit. Cost: 0 extra cycles.
If working set > TLB coverage: every access to a page not recently used causes a TLB miss. Cost: 16–980 cycles per miss.

```
Workload                    Working set    4 KB TLB cvg   2 MB TLB cvg
────────────────────────────────────────────────────────────────────────
np.sum on 1 MB array        1 MB           8 MB ✅        64 MB ✅
np.sum on 100 MB array      100 MB         8 MB ❌        64 MB ✅
Spark executor (100 GB heap) 100 GB        8 MB ❌        64 MB ❌ → use 1 GB pages
Hash join, 1 GB probe table  1 GB          8 MB ❌        64 MB ❌ → need huge pages
GroupBy, 10 MB hash table    10 MB         8 MB ❌        64 MB ✅

(✅ = working set fits in STLB, few TLB misses)
(❌ = working set exceeds STLB, TLB misses on every new-page access)
```

### Mental Model 3 — TLB Miss as a Hidden Tax

TLB misses are invisible in most profiling. They don't show up in flame graphs (the page table walk is hardware, not software). They don't show up in `top` or application-level profiling. Yet for random-access workloads on large datasets, they can account for 20–40% of total runtime.

The only way to see them is `perf stat -e dTLB-load-misses,dTLB-loads`. If `dTLB-load-misses / dTLB-loads > 5%`, TLB misses are costing meaningful cycles. If > 20%, huge pages are the highest-leverage fix available.

---

## 8. Failure Scenarios

### Failure 1 — Large NumPy Array with Random Access Pattern

**Symptom:** A Python script performing random-index lookups on a 4 GB NumPy array runs at 5 million lookups/second — much slower than expected given the array fits in DRAM. `perf stat` shows IPC = 0.15 and `dTLB-load-misses / dTLB-loads = 45%`.

**Root cause:** 4 GB / 4 KB = 1,048,576 pages. Random access means each lookup may hit a different page. The STLB holds only 2,048 entries × 4 KB = 8 MB of coverage. For a 4 GB array with random access: ~99.8% of accesses miss the STLB and require a page table walk.

At random: page table entries for a 4 GB array span ~4 GB / 512 = 8 MB of PT entries (each PT covers 512 pages × 4 KB = 2 MB). These PT pages scatter across DRAM. Each page table walk requires 4 DRAM accesses = 280 ns. At 280 ns per lookup: theoretical maximum 3.57M lookups/second just from TLB misses — matching the observed 5M/s (somewhat optimistic due to out-of-order execution overlap).

**With 2 MB huge pages:** 4 GB / 2 MB = 2,048 huge pages. STLB holds 32 huge-page entries = 64 MB coverage. For a 4 GB array: the STLB still misses after the first 64 MB, but each huge-page miss covers 2 MB = 512 consecutive accesses. The amortized TLB miss cost per lookup drops from ~280 ns to ~280 ns / 512 ≈ 0.5 ns. Effectively zero.

---

### Failure 2 — JVM Heap TLB Thrashing

**Symptom:** A Spark job with a 400 GB executor heap shows consistently high GC times (20–30 seconds per GC cycle) even though the live object set is only 100 GB. `perf stat` during GC shows IPC = 0.22 and dTLB miss rate 35%.

**Root cause:** During GC, the G1 collector's marking phase traverses all live objects. Live objects scatter across the 400 GB heap — millions of objects at random addresses spanning hundreds of thousands of unique 4 KB pages. Each GC thread's access pattern is essentially random within the heap: follow a pointer → TLB miss → page table walk → follow next pointer → TLB miss. Every pointer dereference during GC marking pays ~280 ns for the TLB miss.

With 400 GB of heap using 4 KB pages: 400 GB / 4 KB = 104 million pages. GC marking threads' working set is the entire live set (100 GB = 25M pages) → all miss the STLB (2,048 entries). GC is serialized on TLB misses.

**Fix:** Enable JVM huge pages: `-XX:+UseLargePages`. On Linux with HugeTLB pool configured, the JVM allocates its heap from 2 MB huge pages. 400 GB / 2 MB = 200,000 pages. STLB with huge pages: 32 entries × 2 MB = 64 MB. GC marking throughput improves because TLB misses cover 512× more heap per miss.

**Measured improvement:** On a 400 GB JVM heap, enabling `-XX:+UseLargePages` typically reduces GC cycle duration by 30–50%.

---

### Failure 3 — THP Latency Spikes in Kafka

**Symptom:** A Kafka broker with THP enabled (`always` mode) shows periodic P99 produce latency spikes to 50–100 ms, while median latency is 2 ms. Spikes correlate with `khugepaged` CPU usage on the host.

**Root cause:** `khugepaged` is collapsing 4 KB pages into 2 MB huge pages in the Kafka broker's memory mappings (log segments mmap'd via `FileChannel`). The collapse operation requires taking a write lock on the VMA (Virtual Memory Area) briefly — any thread trying to access that region during the collapse is blocked until the lock is released. The collapse of 512 pages takes ~1–5 ms. During this time, any Kafka network thread trying to read from or write to that log segment stalls.

**Fix:** Set THP to `madvise` mode. The JVM can explicitly opt in with `madvise(MADV_HUGEPAGE)` for its heap, but Kafka's log segment mmap'd regions opt out (they don't call `madvise`). `khugepaged` will not touch regions that haven't opted in.

```bash
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
# Then add to Kafka launch:
# -XX:+UseLargePages (JVM heap → huge pages)
# Kafka log segments remain on 4 KB pages (no madvise = no THP for mmap regions)
```

---

### Failure 4 — HugeTLB Pool Exhaustion

**Symptom:** Spark executor JVM fails to start with error: `java.lang.OutOfMemoryError: Unable to allocate 2097152 bytes of memory, or allocate a large page.` The system has plenty of free DRAM, but HugeTLB pool reports `HugePages_Free: 0`.

**Root cause:** The HugeTLB pool was pre-allocated (e.g., `vm.nr_hugepages=256` = 512 MB) but multiple Spark executors are each requesting huge pages for their heap. The pool is exhausted.

**Fix options:**

```bash
# Option 1: Increase hugepage pool size
# (number of 2 MB pages needed = total JVM heap / 2 MB)
# Example: 4 executors × 50 GB heap = 200 GB / 2 MB = 102,400 huge pages
echo 102400 | sudo tee /proc/sys/vm/nr_hugepages

# Option 2: Switch to Transparent Huge Pages (no pre-allocation needed)
echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
# Remove -XX:+UseLargePages from JVM args (THP is automatic)
# Add -XX:+UseTransparentHugePages instead (JVM-level THP awareness)

# Option 3: Use HugeTLB for heap only, not for other JVM allocations
# -XX:+UseLargePages -XX:LargePageSizeInBytes=2m
# The JVM will fall back to 4 KB pages if huge pages are unavailable
# (default behavior — no need to set unless debugging)
```

---

## 9. Recovery Procedures

### Enabling THP for Spark and NumPy Workloads

```bash
#!/bin/bash
# enable_thp.sh — Configure THP for data engineering workloads

# Strategy:
# - Compute processes (Spark, NumPy): use THP for heap
# - Latency-sensitive processes (Kafka, real-time Flink): opt out of THP
# - Set system-wide mode to 'madvise' — let each process choose

# Step 1: Set system-wide THP mode to madvise
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag

# Step 2: Speed up khugepaged (default is very slow)
# scan_sleep_millisecs: how often khugepaged scans for promotable pages
echo 0 | sudo tee /sys/kernel/mm/transparent_hugepage/khugepaged/scan_sleep_millisecs
# pages_to_scan: pages scanned per pass
echo 4096 | sudo tee /sys/kernel/mm/transparent_hugepage/khugepaged/pages_to_scan

# Step 3: For Spark/Java processes, enable THP via JVM flag:
# -XX:+UseTransparentHugePages (OpenJDK 8+)
# This makes the JVM call madvise(MADV_HUGEPAGE) on its heap regions

# Step 4: For Python/NumPy processes, use ctypes or explicit madvise:
python3 - << 'PYTHON'
import ctypes, mmap, numpy as np

MADV_HUGEPAGE = 14  # Linux constant

def enable_thp_for_array(arr: np.ndarray):
    """Call madvise(MADV_HUGEPAGE) on a NumPy array's memory."""
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    addr = arr.ctypes.data
    size = arr.nbytes
    # Align to page boundary
    page_size = 4096
    aligned_addr = (addr // page_size) * page_size
    aligned_size = size + (addr - aligned_addr)
    result = libc.madvise(ctypes.c_void_p(aligned_addr),
                          ctypes.c_size_t(aligned_size),
                          ctypes.c_int(MADV_HUGEPAGE))
    if result != 0:
        errno = ctypes.get_errno()
        print(f"madvise failed: errno {errno}")
    else:
        print(f"THP enabled for array at 0x{addr:016x}, {size/1e6:.0f} MB")

arr = np.zeros(100_000_000, dtype=np.float64)  # 800 MB
enable_thp_for_array(arr)
PYTHON

echo "THP configuration complete."
echo "Verify: cat /proc/meminfo | grep -i huge"
```

### Configuring Explicit HugeTLB for Spark

```bash
#!/bin/bash
# configure_hugepages_spark.sh

EXECUTOR_HEAP_GB=${1:-50}   # GB of heap per executor
N_EXECUTORS=${2:-2}          # number of executors on this machine
HUGE_PAGE_MB=2               # 2 MB huge pages

TOTAL_HEAP_MB=$((EXECUTOR_HEAP_GB * N_EXECUTORS * 1024))
N_HUGEPAGES=$((TOTAL_HEAP_MB / HUGE_PAGE_MB + 1000))  # +1000 for overhead

echo "Configuring $N_HUGEPAGES huge pages (${HUGE_PAGE_MB}MB each)"
echo "Total: $((N_HUGEPAGES * HUGE_PAGE_MB / 1024)) GB reserved"

# Reserve huge pages (do this at boot for best results — fragmentation grows over time)
echo $N_HUGEPAGES | sudo tee /proc/sys/vm/nr_hugepages

# Verify
echo "After allocation:"
grep -E "HugePages_|Hugepagesize" /proc/meminfo

# Add to JVM startup
echo ""
echo "Add these JVM flags to spark.executor.extraJavaOptions:"
echo "  -XX:+UseLargePages"
echo "  (falls back to 4KB pages if huge pages unavailable)"
echo ""
echo "Or for Spark on Linux with THP:"
echo "  -XX:+UseTransparentHugePages"
```

### Measuring TLB Performance Before/After

```bash
#!/bin/bash
# measure_tlb_impact.sh

SCRIPT=$1  # Python script to benchmark

echo "=== WITH 4 KB PAGES (THP disabled) ==="
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null
perf stat \
  -e dTLB-loads,dTLB-load-misses \
  -e iTLB-loads,iTLB-load-misses \
  -e instructions,cycles \
  -- python3 "$SCRIPT"

echo ""
echo "=== WITH TRANSPARENT HUGE PAGES (THP enabled) ==="
echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null
perf stat \
  -e dTLB-loads,dTLB-load-misses \
  -e iTLB-loads,iTLB-load-misses \
  -e instructions,cycles \
  -- python3 "$SCRIPT"

# Restore THP to madvise
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null

echo ""
echo "Key metrics:"
echo "  dTLB-load-misses / dTLB-loads = STLB miss rate"
echo "  < 1%:  TLB-healthy"
echo "  1-5%:  moderate TLB pressure"
echo "  > 5%:  TLB bottleneck — huge pages strongly recommended"
```

---

## 10. Trade-offs

### 4 KB Pages vs 2 MB Huge Pages

| 4 KB pages | 2 MB huge pages |
|---|---|
| No physical contiguity required | Requires 2 MB contiguous physical block |
| Pre-allocates no memory (demand paged) | May require pre-allocation (HugeTLB pool) |
| Fine-grained allocation | Minimum 2 MB per allocation (wastes memory for small allocations) |
| TLB coverage: 8 MB (STLB, 4 KB) | TLB coverage: 64 MB (STLB, 2 MB) — 8× more |
| No THP latency spikes | THP may cause latency spikes during page promotion |
| Always available | May fail if memory fragmented (HugeTLB) |
| Good for: small, scattered allocations | Good for: large long-lived arrays, JVM heap |

### THP (`always`) vs THP (`madvise`) vs Explicit HugeTLB

| THP `always` | THP `madvise` | Explicit HugeTLB |
|---|---|---|
| Automatic — no code changes | App must call `madvise(MADV_HUGEPAGE)` | App must use `mmap(MAP_HUGETLB)` |
| May cause latency spikes (khugepaged collapse) | No background collapse | No background collapse |
| Good for batch/analytics workloads | Good for mixed latency/throughput | Best performance; hardest to configure |
| May not promote pages quickly | Promoted immediately on allocation | Huge from first allocation |
| No pool pre-allocation needed | No pool pre-allocation needed | Pool must be pre-allocated at boot |

**Recommendation by workload:**

| Workload | Recommendation |
|---|---|
| Spark batch jobs | THP `always` or THP `madvise` + `-XX:+UseTransparentHugePages` |
| Kafka broker | THP `madvise` + explicit HugeTLB for JVM heap only |
| Latency-sensitive Flink | THP `never` for mmap regions; HugeTLB for JVM heap |
| NumPy large array workloads | THP `always` or `madvise(MADV_HUGEPAGE)` per array |
| PostgreSQL | THP `never` (PG manages its own shared_buffers; THP interferes) |

---

## 11. Comparisons

### TLB Performance: Small Array vs Large Array

```python
"""
tlb_comparison.py

Demonstrates TLB effect by comparing random access to arrays of
increasing size. At the STLB coverage boundary (8 MB for 4 KB pages),
performance should drop sharply due to TLB misses.

Run: python3 tlb_comparison.py
Then: perf stat -e dTLB-load-misses,dTLB-loads python3 tlb_comparison.py
"""
import numpy as np
import time

print("Random access latency at increasing array sizes")
print("Expected: performance cliff at ~8 MB (STLB coverage boundary for 4 KB pages)")
print()
print(f"{'Array size':>15}  {'Accesses':>12}  {'Latency (ns)':>14}  {'STLB fit?':>10}")
print("-" * 60)

for log2_size in range(14, 30):  # 16 KB to 512 MB
    n_bytes = 1 << log2_size
    n_elements = n_bytes // 8  # float64

    arr = np.zeros(n_elements, dtype=np.float64)

    # Random indices — defeats prefetcher and generates TLB misses
    n_accesses = min(5_000_000, n_elements * 5)
    indices = np.random.randint(0, n_elements, n_accesses)

    # Warmup
    _ = arr[indices[:1000]].sum()

    t0 = time.perf_counter()
    _ = arr[indices].sum()
    elapsed = time.perf_counter() - t0

    latency_ns = elapsed * 1e9 / n_accesses

    size_str = (f"{n_bytes/1024:.0f} KB" if n_bytes < 1024*1024
                else f"{n_bytes/1024/1024:.0f} MB")

    stlb_4k  = "✅" if n_bytes <= 8*1024*1024 else "❌"
    stlb_2m  = "✅" if n_bytes <= 64*1024*1024 else "(partial)"

    print(f"{size_str:>15}  {n_accesses:>12,}  {latency_ns:>14.1f}  {stlb_4k} 4K / {stlb_2m} 2M")

print()
print("Expected behavior:")
print("  < 8 MB:  latency stable (~15-40 ns, L3 or STLB limited)")
print("  > 8 MB:  latency increases (STLB misses begin)")
print("  > 32 MB: latency stabilizes (~70-100 ns, DRAM + TLB miss)")
print()
print("With 2 MB huge pages enabled (THP or hugectl):")
print("  < 64 MB: STLB-covered, no TLB miss overhead")
print("  > 64 MB: TLB misses, but 512× less frequent")
```

---

## 12. Production Examples

### Spark Executor Heap with Huge Pages

A Spark executor on a 2-socket server with 400 GB heap, running a sort-merge join:

**Without huge pages:**
```
Sort phase: Spark sorts 100 GB of data in the executor heap.
Sorting requires random access to comparison keys at arbitrary heap addresses.
100 GB / 4 KB = 25,600,000 unique pages.
STLB holds 2,048 entries = 8 MB coverage.
25,600,000 / 2,048 = ~12,500 STLB evictions per full heap scan.

Measured: GC pause 45 seconds, sort phase 120 seconds.
dTLB-load-misses rate: 42% (perf stat)
```

**With huge pages (`-XX:+UseLargePages`, pool pre-allocated):**
```
100 GB / 2 MB = 50,000 unique huge pages.
STLB holds 32 huge-page entries = 64 MB coverage.
50,000 / 32 = 1,562 STLB evictions per full heap scan — 8× fewer.

Each STLB miss (page table walk) now covers a 2 MB page instead of 4 KB —
512 cache lines (512 × 64 bytes = 32 KB) benefit from each translation.

Measured: GC pause 28 seconds, sort phase 82 seconds.
dTLB-load-misses rate: 8% (perf stat)
Speedup: ~32% for the sort+GC combined.
```

**Configuration for Spark on Linux:**
```properties
# spark-defaults.conf
spark.executor.extraJavaOptions= \
  -XX:+UseLargePages \
  -XX:+UseNUMA \
  -XX:+UseG1GC \
  -XX:G1HeapRegionSize=32m \
  -XX:LargePageSizeInBytes=2m

# Before starting Spark, allocate hugepage pool:
# sudo sysctl -w vm.nr_hugepages=204800  # 400 GB / 2 MB = 200,000 huge pages
```

---

### PyArrow Parquet Reader and TLB

PyArrow's Parquet reader memory-maps the Parquet file and decodes column pages into Arrow arrays (contiguous C arrays). For a 10 GB Parquet file with 100 MB column pages:

```python
import pyarrow.parquet as pq
import ctypes, os
import numpy as np

MADV_HUGEPAGE     = 14
MADV_SEQUENTIAL   = 2

def read_with_thp_hint(parquet_path: str, columns: list[str]):
    """
    Read a Parquet file and apply THP and prefetch hints to the
    resulting Arrow arrays to minimize TLB misses on subsequent processing.
    """
    libc = ctypes.CDLL("libc.so.6")
    
    table = pq.read_table(parquet_path, columns=columns)
    
    for col_name in columns:
        col = table.column(col_name)
        for chunk in col.chunks:
            # Get the underlying buffer
            buf = chunk.buffers()[-1]  # data buffer (last one is values)
            if buf is not None and buf.size > 2 * 1024 * 1024:
                # Only worth using huge pages for arrays > 2 MB
                addr = buf.address
                size = buf.size
                # Align to page boundary
                aligned_addr = (addr // 4096) * 4096
                # Hint: use huge pages for this buffer
                libc.madvise(ctypes.c_void_p(aligned_addr),
                             ctypes.c_size_t(size),
                             ctypes.c_int(MADV_HUGEPAGE))
    
    return table


# For sequential scans, also add MADV_SEQUENTIAL:
def read_with_prefetch_hint(parquet_path: str, columns: list[str]):
    """Apply sequential prefetch hint for full-column scans."""
    libc = ctypes.CDLL("libc.so.6")
    table = pq.read_table(parquet_path, columns=columns)
    for col_name in columns:
        col = table.column(col_name)
        for chunk in col.chunks:
            buf = chunk.buffers()[-1]
            if buf is not None:
                addr = buf.address
                size = buf.size
                aligned_addr = (addr // 4096) * 4096
                libc.madvise(ctypes.c_void_p(aligned_addr),
                             ctypes.c_size_t(size),
                             ctypes.c_int(MADV_SEQUENTIAL))
    return table
```

**Why THP helps for Parquet:** A 100 MB column buffer decoded by the Parquet reader is a single contiguous array. After THP promotion, it occupies 50 huge pages instead of 25,600 standard pages. Processing that column with `pc.sum()` or `pc.filter()` generates 50 TLB entries (all fit in STLB with 2 MB huge pages) instead of 25,600 (far exceeding STLB capacity). The reduction in TLB misses during column processing is ~512×.

---

### JVM GC and TLB: The Marking Phase

During G1GC's Concurrent Marking phase (runs in parallel with application threads), GC threads traverse the live object graph. Every pointer dereference is a random heap access — the graph is not laid out in any predictable order.

```
GC marking on a 300 GB heap:
  Live objects: 100 GB spread across 300 GB of virtual address space
  Unique pages accessed during marking:
    4 KB pages:   100 GB / 4 KB = 26 million unique pages → all miss STLB
    2 MB pages:   100 GB / 2 MB = 50,000 unique huge pages → STLB misses but rare

  Time per pointer dereference (marking):
    4 KB pages:  STLB miss rate ~99% → avg latency ~0.99×280 + 0.01×8 = 278 ns
    2 MB pages:  STLB miss rate ~50% (32 huge-page entries, accessing 50K pages)
                 → avg latency ~0.50×280 + 0.50×8 = 144 ns (1.9× faster)

  Total GC marking time (simplified):
    4 KB pages:  100M objects × 278 ns = 27.8 seconds
    2 MB pages:  100M objects × 144 ns = 14.4 seconds
    Improvement: 1.93× faster GC marking with huge pages.

  Actual measured improvements vary (3–10 pointers per object on average,
  L3 cache partially covers recently-traced objects), but 30–50% GC
  speedup with huge pages is commonly observed in production.
```

---

## 13. Code

### `tlb_profiler.py` — Measure TLB Miss Rate and Impact

```python
"""
tlb_profiler.py

Measures TLB miss rate for access patterns of varying array sizes and
access strides. Uses subprocess perf stat for hardware counter access.

Run: python3 tlb_profiler.py
(For perf counters: sudo sysctl kernel.perf_event_paranoid=2 first)
"""
import subprocess
import re
import time
import tempfile
import os
import sys
import numpy as np
from dataclasses import dataclass
from typing import Optional


@dataclass
class TLBProfile:
    array_size_bytes: int
    access_pattern: str
    n_accesses: int
    elapsed_ms: float
    dtlb_loads: int
    dtlb_load_misses: int

    @property
    def miss_rate_pct(self) -> float:
        if self.dtlb_loads == 0:
            return 0.0
        return 100.0 * self.dtlb_load_misses / self.dtlb_loads

    @property
    def latency_ns(self) -> float:
        return self.elapsed_ms * 1e6 / self.n_accesses

    @property
    def diagnosis(self) -> str:
        mr = self.miss_rate_pct
        if mr < 1:
            return "TLB-healthy ✅"
        elif mr < 5:
            return "Low TLB pressure ⚠️"
        elif mr < 20:
            return "Moderate TLB pressure 🔶"
        else:
            return "High TLB pressure — consider huge pages 🔴"


def generate_test_script(array_bytes: int, pattern: str, n_accesses: int) -> str:
    """Generate a Python script for TLB benchmarking."""
    n_elements = array_bytes // 8

    if pattern == 'sequential':
        access_code = f"""
indices = np.arange({n_accesses}) % {n_elements}
_ = arr[indices].sum()
"""
    elif pattern == 'random':
        access_code = f"""
rng = np.random.default_rng(42)
indices = rng.integers(0, {n_elements}, {n_accesses})
_ = arr[indices].sum()
"""
    elif pattern == 'stride_256':
        stride = 256 // 8  # 256 bytes / 8 bytes per float64 = 32 elements
        access_code = f"""
indices = (np.arange({n_accesses}) * {stride}) % {n_elements}
_ = arr[indices].sum()
"""
    else:
        raise ValueError(f"Unknown pattern: {pattern}")

    return f"""
import numpy as np
arr = np.zeros({n_elements}, dtype=np.float64)
# Warmup
_ = arr[:1000].sum()
# Timed access
{access_code}
"""


def profile_tlb(array_bytes: int, pattern: str,
                n_accesses: int = 5_000_000) -> Optional[TLBProfile]:
    """Profile TLB behavior for a given array size and access pattern."""

    script_content = generate_test_script(array_bytes, pattern, n_accesses)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(script_content)
        script_path = f.name

    try:
        t0 = time.perf_counter()
        result = subprocess.run(
            ['perf', 'stat', '-e', 'dTLB-loads,dTLB-load-misses',
             '--', sys.executable, script_path],
            capture_output=True, text=True, timeout=60
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000

        if result.returncode != 0:
            return None

        # Parse perf stat output
        dtlb_loads = 0
        dtlb_misses = 0
        for line in result.stderr.splitlines():
            line = line.strip()
            m = re.match(r'^([\d,]+)\s+dTLB-loads', line)
            if m:
                dtlb_loads = int(m.group(1).replace(',', ''))
            m = re.match(r'^([\d,]+)\s+dTLB-load-misses', line)
            if m:
                dtlb_misses = int(m.group(1).replace(',', ''))

        return TLBProfile(
            array_size_bytes=array_bytes,
            access_pattern=pattern,
            n_accesses=n_accesses,
            elapsed_ms=elapsed_ms,
            dtlb_loads=dtlb_loads,
            dtlb_load_misses=dtlb_misses,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    finally:
        os.unlink(script_path)


def timing_only_benchmark():
    """
    Timing-based TLB benchmark (works without perf access).
    Uses the latency cliff to infer TLB pressure.
    """
    print("=" * 70)
    print("TLB LATENCY CLIFF BENCHMARK (timing only)")
    print("=" * 70)
    print()
    print(f"{'Array Size':>14}  {'Sequential (ns)':>16}  {'Random (ns)':>14}  {'TLB pressure':>14}")
    print("-" * 65)

    sizes = [64*1024, 256*1024, 1*1024**2, 4*1024**2,
             8*1024**2, 16*1024**2, 64*1024**2, 256*1024**2]

    for size_bytes in sizes:
        n_elem = size_bytes // 8
        arr = np.zeros(n_elem, dtype=np.float64)
        N = min(3_000_000, n_elem * 3)
        idx_seq = np.arange(N, dtype=np.int64) % n_elem
        rng = np.random.default_rng(42)
        idx_rand = rng.integers(0, n_elem, N).astype(np.int64)

        # Warmup
        _ = arr[idx_seq[:100]].sum()

        times_seq = []
        for _ in range(3):
            t0 = time.perf_counter()
            _ = arr[idx_seq].sum()
            times_seq.append(time.perf_counter() - t0)
        lat_seq = min(times_seq) * 1e9 / N

        times_rand = []
        for _ in range(3):
            t0 = time.perf_counter()
            _ = arr[idx_rand].sum()
            times_rand.append(time.perf_counter() - t0)
        lat_rand = min(times_rand) * 1e9 / N

        size_str = (f"{size_bytes/1024:.0f} KB" if size_bytes < 1024**2
                    else f"{size_bytes/1024**2:.0f} MB")

        # TLB pressure from STLB coverage (8 MB for 4 KB pages)
        if size_bytes <= 8 * 1024**2:
            pressure = "Low (STLB covered)"
        elif size_bytes <= 64 * 1024**2:
            pressure = "Med (STLB partial)"
        else:
            pressure = "High (STLB misses)"

        print(f"{size_str:>14}  {lat_seq:>16.1f}  {lat_rand:>14.1f}  {pressure}")

    print()
    print("Key: Random access latency should increase at ~8 MB (STLB boundary).")
    print("     With huge pages (2 MB), the cliff moves to ~64 MB.")


def run_perf_benchmark():
    """Run the perf-based benchmark if perf is available."""
    print("=" * 70)
    print("TLB MISS RATE BENCHMARK (requires perf)")
    print("=" * 70)
    print()
    print(f"{'Array':>10}  {'Pattern':>12}  {'Miss Rate':>11}  {'Latency ns':>12}  {'Status':>30}")
    print("-" * 80)

    configs = [
        (1 * 1024**2,  'sequential',  "1 MB seq"),
        (64 * 1024**2, 'sequential',  "64 MB seq"),
        (256 * 1024**2,'sequential',  "256 MB seq"),
        (1 * 1024**2,  'random',      "1 MB rand"),
        (64 * 1024**2, 'random',      "64 MB rand"),
        (256 * 1024**2,'random',      "256 MB rand"),
    ]

    for size_bytes, pattern, label in configs:
        profile = profile_tlb(size_bytes, pattern, n_accesses=2_000_000)
        if profile:
            print(f"{label:>10}  {pattern:>12}  {profile.miss_rate_pct:>10.1f}%"
                  f"  {profile.latency_ns:>12.1f}  {profile.diagnosis}")
        else:
            print(f"{label:>10}  {pattern:>12}  (perf not available)")
            break


if __name__ == '__main__':
    timing_only_benchmark()
    print()
    run_perf_benchmark()
```

### `huge_page_allocator.py` — Allocate and Verify Huge Pages from Python

```python
"""
huge_page_allocator.py

Demonstrates allocating memory with huge pages from Python using:
1. mmap with MAP_HUGETLB (requires pre-allocated hugepage pool)
2. madvise(MADV_HUGEPAGE) on existing arrays (THP)
3. Verifying huge page usage via /proc/<pid>/smaps

Run: python3 huge_page_allocator.py
Note: MAP_HUGETLB requires: sudo sysctl -w vm.nr_hugepages=512 first
"""
import ctypes
import mmap
import os
import re
import numpy as np
import time
from pathlib import Path


# Linux constants
MAP_ANONYMOUS  = 0x20
MAP_PRIVATE    = 0x02
MAP_HUGETLB    = 0x40000
MAP_HUGE_2MB   = (21 << 26)  # 2 MB huge pages (log2(2MB) << MAP_HUGE_SHIFT)
PROT_READ      = 0x1
PROT_WRITE     = 0x2
MADV_HUGEPAGE  = 14
MADV_NOHUGEPAGE= 15


libc = ctypes.CDLL("libc.so.6", use_errno=True)


def mmap_huge_pages(size_bytes: int) -> ctypes.c_void_p:
    """
    Allocate memory using 2 MB huge pages via mmap(MAP_HUGETLB).
    Requires pre-allocated hugepage pool: vm.nr_hugepages.
    Returns None if huge pages unavailable.
    """
    # Align to 2 MB
    huge_page_size = 2 * 1024 * 1024
    aligned_size = ((size_bytes + huge_page_size - 1)
                    // huge_page_size * huge_page_size)

    flags = MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_HUGE_2MB
    ptr = libc.mmap(
        ctypes.c_void_p(0),       # let OS choose address
        ctypes.c_size_t(aligned_size),
        ctypes.c_int(PROT_READ | PROT_WRITE),
        ctypes.c_int(flags),
        ctypes.c_int(-1),          # no file descriptor
        ctypes.c_long(0),          # no offset
    )

    if ptr == ctypes.c_void_p(-1).value:
        errno = ctypes.get_errno()
        print(f"mmap(MAP_HUGETLB) failed: errno={errno} "
              f"({'ENOMEM: no huge pages available' if errno == 12 else 'check vm.nr_hugepages'})")
        return None
    return ctypes.c_void_p(ptr)


def advise_hugepage(arr: np.ndarray):
    """Advise OS to use huge pages for an existing NumPy array."""
    addr = arr.ctypes.data
    size = arr.nbytes
    page_size = 4096
    aligned_addr = (addr // page_size) * page_size
    aligned_size = size + (addr - aligned_addr)
    result = libc.madvise(
        ctypes.c_void_p(aligned_addr),
        ctypes.c_size_t(aligned_size),
        ctypes.c_int(MADV_HUGEPAGE)
    )
    return result == 0


def get_huge_page_usage_mb(pid: int = None) -> dict:
    """
    Read /proc/<pid>/smaps to find how much memory is backed by huge pages.
    Returns dict with 'anonymous_mb' and 'huge_pages_mb'.
    """
    if pid is None:
        pid = os.getpid()
    smaps_path = Path(f'/proc/{pid}/smaps')
    if not smaps_path.exists():
        return {}

    content = smaps_path.read_text()
    anon_hugepages_kb = 0
    rss_kb = 0

    for line in content.splitlines():
        m = re.match(r'AnonHugePages:\s+(\d+) kB', line)
        if m:
            anon_hugepages_kb += int(m.group(1))
        m = re.match(r'Rss:\s+(\d+) kB', line)
        if m:
            rss_kb += int(m.group(1))

    return {
        'rss_mb': rss_kb / 1024,
        'huge_pages_mb': anon_hugepages_kb / 1024,
        'huge_page_pct': (anon_hugepages_kb / max(rss_kb, 1)) * 100
    }


def benchmark_with_without_hugepages(size_mb: int = 512):
    """
    Benchmark random access to a large array with and without huge pages.
    Shows the TLB miss impact directly via latency measurement.
    """
    size_bytes = size_mb * 1024 * 1024
    n_elements = size_bytes // 8
    N_ACCESSES = 3_000_000

    rng = np.random.default_rng(42)
    indices = rng.integers(0, n_elements, N_ACCESSES)

    print(f"\nBenchmark: {size_mb} MB array, {N_ACCESSES:,} random accesses")

    # --- Without huge pages ---
    arr_normal = np.zeros(n_elements, dtype=np.float64)
    # Advise NO huge pages
    libc.madvise(
        ctypes.c_void_p(arr_normal.ctypes.data),
        ctypes.c_size_t(arr_normal.nbytes),
        ctypes.c_int(MADV_NOHUGEPAGE)
    )
    # Touch pages to force allocation
    arr_normal[:] = np.arange(n_elements, dtype=np.float64)

    times = []
    for _ in range(5):
        t0 = time.perf_counter()
        _ = arr_normal[indices].sum()
        times.append(time.perf_counter() - t0)
    lat_normal = min(times) * 1e9 / N_ACCESSES
    usage_before = get_huge_page_usage_mb()

    # --- With THP hint ---
    arr_thp = np.zeros(n_elements, dtype=np.float64)
    advise_hugepage(arr_thp)
    # Touch pages AFTER advising (so OS can allocate as huge pages)
    arr_thp[:] = np.arange(n_elements, dtype=np.float64)

    # Give khugepaged a moment to promote (in production, allocation is immediate)
    time.sleep(0.5)

    times = []
    for _ in range(5):
        t0 = time.perf_counter()
        _ = arr_thp[indices].sum()
        times.append(time.perf_counter() - t0)
    lat_thp = min(times) * 1e9 / N_ACCESSES
    usage_after = get_huge_page_usage_mb()

    print(f"\n  4 KB pages (madvise NOHUGEPAGE):")
    print(f"    Latency: {lat_normal:.1f} ns/access")
    print(f"    Huge page usage: {usage_before.get('huge_pages_mb', 0):.0f} MB")

    print(f"\n  With THP hint (madvise HUGEPAGE):")
    print(f"    Latency: {lat_thp:.1f} ns/access")
    print(f"    Huge page usage: {usage_after.get('huge_pages_mb', 0):.0f} MB")
    print(f"    Huge page %: {usage_after.get('huge_page_pct', 0):.0f}%")

    if lat_normal > lat_thp:
        speedup = lat_normal / lat_thp
        print(f"\n  Speedup from huge pages: {speedup:.2f}×")
    else:
        print(f"\n  (THP not yet promoted — run again after khugepaged has had time)")
        print(f"  Or: ensure THP is set to 'always': "
              f"cat /sys/kernel/mm/transparent_hugepage/enabled")


def check_hugepage_availability():
    """Check system huge page configuration."""
    print("=" * 60)
    print("HUGE PAGE SYSTEM CHECK")
    print("=" * 60)

    # THP mode
    thp_path = Path('/sys/kernel/mm/transparent_hugepage/enabled')
    if thp_path.exists():
        thp_mode = thp_path.read_text().strip()
        current = re.search(r'\[(\w+)\]', thp_mode)
        mode = current.group(1) if current else "unknown"
        print(f"\nTransparent Huge Pages mode: {mode}")
        print(f"  Full setting: {thp_mode}")
        status = {
            'always': '✅ THP enabled for all allocations',
            'madvise': '⚠️  THP only for madvise(MADV_HUGEPAGE) regions',
            'never': '❌ THP disabled',
        }.get(mode, '?')
        print(f"  Status: {status}")

    # Explicit HugeTLB pool
    meminfo = Path('/proc/meminfo')
    if meminfo.exists():
        content = meminfo.read_text()
        for line in content.splitlines():
            if 'HugePage' in line or 'Hugepage' in line:
                print(f"  {line.strip()}")

    # Per-process huge page usage
    usage = get_huge_page_usage_mb()
    if usage:
        print(f"\nCurrent process huge page usage:")
        print(f"  RSS: {usage['rss_mb']:.0f} MB")
        print(f"  Huge pages: {usage['huge_pages_mb']:.0f} MB ({usage['huge_page_pct']:.0f}%)")


if __name__ == '__main__':
    check_hugepage_availability()
    benchmark_with_without_hugepages(size_mb=256)
```

---

## 14. Labs

### Lab 1 — Find the TLB Cliff

**Goal:** Empirically locate the STLB coverage boundary on your machine by measuring random-access latency at increasing array sizes. The cliff should appear around 8 MB (2,048 entries × 4 KB) for 4 KB pages.

```bash
#!/bin/bash
# lab1_tlb_cliff.sh

echo "=== Step 1: Check your TLB sizes ==="
# dmesg often shows TLB info at boot:
dmesg 2>/dev/null | grep -i tlb | head -10

# cpuid shows TLB information
if which cpuid > /dev/null 2>&1; then
    cpuid -1 | grep -i tlb | head -20
fi

# perf can also show:
echo ""
echo "=== Step 2: Run the TLB cliff benchmark ==="
python3 tlb_profiler.py

echo ""
echo "=== Step 3: Check current THP settings ==="
cat /sys/kernel/mm/transparent_hugepage/enabled
cat /proc/meminfo | grep -i huge

echo ""
echo "=== Step 4: Measure with perf (if available) ==="
cat > /tmp/tlb_test.py << 'PYTHON'
import numpy as np
# 256 MB array — well beyond STLB coverage for 4 KB pages
N = 256 * 1024 * 1024 // 8
arr = np.zeros(N, dtype=np.float64)
arr[:] = 1.0
rng = np.random.default_rng(42)
idx = rng.integers(0, N, 5_000_000)
print(arr[idx].sum())
PYTHON

perf stat -e dTLB-loads,dTLB-load-misses -- python3 /tmp/tlb_test.py 2>&1

echo ""
echo "Expected:"
echo "  dTLB-load-misses / dTLB-loads > 30% for 256 MB random access"
echo "  This confirms TLB misses are the bottleneck"
```

---

### Lab 2 — Enable Huge Pages and Measure the Impact

**Goal:** Enable THP or pre-allocate HugeTLB pages and verify the performance improvement on a 256 MB random-access benchmark.

```bash
#!/bin/bash
# lab2_huge_pages_impact.sh

echo "=== Baseline: 4 KB pages ==="
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

perf stat -e dTLB-loads,dTLB-load-misses,instructions,cycles \
    python3 -c "
import numpy as np, time
N = 256*1024*1024//8
arr = np.zeros(N, dtype=np.float64); arr[:] = 1.0
rng = np.random.default_rng(42)
idx = rng.integers(0, N, 3_000_000)
t0 = time.perf_counter()
for _ in range(5): _ = arr[idx].sum()
print(f'Time: {(time.perf_counter()-t0)*200:.1f} ms/iter')
"

echo ""
echo "=== With Transparent Huge Pages ==="
echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

perf stat -e dTLB-loads,dTLB-load-misses,instructions,cycles \
    python3 -c "
import numpy as np, time, ctypes
MADV_HUGEPAGE = 14
libc = ctypes.CDLL('libc.so.6')
N = 256*1024*1024//8
arr = np.zeros(N, dtype=np.float64)
# Request huge pages BEFORE touching memory
libc.madvise(ctypes.c_void_p(arr.ctypes.data), ctypes.c_size_t(arr.nbytes),
             ctypes.c_int(MADV_HUGEPAGE))
arr[:] = 1.0  # touch pages (allocated as huge pages now)
rng = np.random.default_rng(42)
idx = rng.integers(0, N, 3_000_000)
t0 = time.perf_counter()
for _ in range(5): _ = arr[idx].sum()
print(f'Time: {(time.perf_counter()-t0)*200:.1f} ms/iter')
"

# Restore THP to madvise
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

echo ""
echo "Expected:"
echo "  4 KB pages: dTLB-load-misses ~35-45%, higher latency"
echo "  Huge pages: dTLB-load-misses ~3-8%, lower latency"
echo "  Speedup: 1.5-3x for random access workloads on 256 MB arrays"
```

---

### Lab 3 — Trace a Virtual Address Through the Page Table

**Goal:** Use `/proc/<pid>/maps` and `/proc/<pid>/pagemap` to trace a Python array's virtual address to its physical page number, and verify the page size (4 KB vs 2 MB).

```python
"""
lab3_page_table_trace.py

Traces a virtual address through the Linux page table using:
- /proc/self/maps (virtual memory layout)
- /proc/self/pagemap (virtual-to-physical mapping)

Shows: virtual address, page frame number, page size, huge page flag.

Run: python3 lab3_page_table_trace.py
Note: May require root or kernel.perf_event_paranoid=-1 for pagemap access.
"""
import ctypes
import struct
import os
import re
import numpy as np

PAGEMAP_ENTRY_SIZE = 8   # 64-bit entry
PAGE_SHIFT         = 12  # log2(4096)
HUGE_PAGE_SHIFT    = 21  # log2(2MB)


def get_virtual_to_physical(virtual_addr: int) -> dict:
    """
    Use /proc/self/pagemap to get physical frame number for a virtual address.
    pagemap entry format (64 bits):
      bits 54:0   = page frame number (if present)
      bit  55     = PTE is soft-dirty
      bit  56     = page is exclusively mapped
      bit  57-60  = zero
      bit  61     = page is file-page or shared-anonymous
      bit  62     = page swapped
      bit  63     = page is present
    """
    pagemap_path = '/proc/self/pagemap'
    page_size = 4096
    page_num = virtual_addr // page_size
    offset = page_num * PAGEMAP_ENTRY_SIZE

    try:
        with open(pagemap_path, 'rb') as f:
            f.seek(offset)
            entry_bytes = f.read(PAGEMAP_ENTRY_SIZE)
            if len(entry_bytes) < PAGEMAP_ENTRY_SIZE:
                return {'error': 'Could not read pagemap entry'}
            entry = struct.unpack('<Q', entry_bytes)[0]
    except PermissionError:
        return {'error': 'Permission denied — try: sudo python3 lab3_page_table_trace.py'}
    except FileNotFoundError:
        return {'error': '/proc/self/pagemap not available (not Linux)'}

    present     = (entry >> 63) & 1
    swapped     = (entry >> 62) & 1
    file_mapped = (entry >> 61) & 1
    pfn         = entry & ((1 << 55) - 1)

    return {
        'virtual_addr': virtual_addr,
        'page_number':  page_num,
        'present':      bool(present),
        'swapped':      bool(swapped),
        'pfn':          pfn if present else None,
        'physical_addr': pfn * page_size + (virtual_addr % page_size) if present else None,
    }


def get_vma_info(virtual_addr: int) -> dict:
    """Look up the VMA containing virtual_addr in /proc/self/maps."""
    maps_path = '/proc/self/maps'
    try:
        content = open(maps_path).read()
    except Exception:
        return {}

    for line in content.splitlines():
        parts = line.split()
        if not parts:
            continue
        addr_range = parts[0]
        start_str, end_str = addr_range.split('-')
        start = int(start_str, 16)
        end   = int(end_str,   16)
        if start <= virtual_addr < end:
            perms = parts[1] if len(parts) > 1 else ''
            name  = parts[-1] if len(parts) > 5 else '[anonymous]'
            return {
                'start': start,
                'end':   end,
                'size_mb': (end - start) / 1024**2,
                'perms': perms,
                'name':  name,
            }
    return {}


def check_huge_page_smaps(virtual_addr: int) -> dict:
    """Check if the page at virtual_addr is backed by a huge page via smaps."""
    smaps_path = '/proc/self/smaps'
    try:
        content = open(smaps_path).read()
    except Exception:
        return {}

    # Find the VMA containing our address
    in_target_vma = False
    result = {'anon_huge': 0}

    for line in content.splitlines():
        # VMA header line: "7f0000000000-7f0040000000 rw-p ..."
        m = re.match(r'^([0-9a-f]+)-([0-9a-f]+)\s', line)
        if m:
            start = int(m.group(1), 16)
            end   = int(m.group(2), 16)
            in_target_vma = (start <= virtual_addr < end)
            continue

        if in_target_vma:
            m = re.match(r'AnonHugePages:\s+(\d+) kB', line)
            if m:
                result['anon_huge'] = int(m.group(1))
                break

    return result


def lab3():
    print("=" * 65)
    print("LAB 3: Virtual Address → Physical Address Trace")
    print("=" * 65)

    # Allocate a 10 MB array
    SIZE_MB = 10
    arr = np.zeros(SIZE_MB * 1024 * 1024 // 8, dtype=np.float64)
    arr[:] = 42.0  # touch all pages to force physical allocation

    # Get the virtual address of arr[0]
    va = arr.ctypes.data
    va_middle = va + SIZE_MB * 1024 * 1024 // 2  # middle of array

    print(f"\nArray: {SIZE_MB} MB NumPy float64")
    print(f"Base virtual address: 0x{va:016x}")

    # Decode the virtual address
    PAGE_SIZE = 4096
    pml4_idx  = (va >> 39) & 0x1ff
    pdpt_idx  = (va >> 30) & 0x1ff
    pd_idx    = (va >> 21) & 0x1ff
    pt_idx    = (va >> 12) & 0x1ff
    offset    = va & 0xfff

    print(f"\nPage table indices for 0x{va:016x}:")
    print(f"  PML4 index: {pml4_idx:3d} (bits 47:39)")
    print(f"  PDPT index: {pdpt_idx:3d} (bits 38:30)")
    print(f"  PD   index: {pd_idx:3d} (bits 29:21)")
    print(f"  PT   index: {pt_idx:3d} (bits 20:12)")
    print(f"  Offset:     {offset:#x} (bits 11:0)")

    # VMA info
    vma = get_vma_info(va)
    if vma:
        print(f"\nVMA containing this array:")
        print(f"  Range: 0x{vma['start']:016x} – 0x{vma['end']:016x}")
        print(f"  Size:  {vma['size_mb']:.1f} MB")
        print(f"  Perms: {vma['perms']}")
        print(f"  Name:  {vma['name']}")

    # Physical address lookup
    pmap = get_virtual_to_physical(va)
    if 'error' in pmap:
        print(f"\nPhysical address lookup: {pmap['error']}")
    elif pmap['present']:
        print(f"\nPhysical address mapping:")
        print(f"  Virtual:  0x{pmap['virtual_addr']:016x}")
        print(f"  PFN:      0x{pmap['pfn']:x} (page frame {pmap['pfn']})")
        print(f"  Physical: 0x{pmap['physical_addr']:016x}")
    else:
        print(f"\nPage not yet physically mapped (demand paging)")

    # Huge page check
    smaps = check_huge_page_smaps(va)
    huge_mb = smaps.get('anon_huge', 0) / 1024
    print(f"\nHuge page usage in this VMA:")
    print(f"  AnonHugePages: {huge_mb:.1f} MB")
    if huge_mb > 0:
        print(f"  ✅ {huge_mb:.0f} MB of this array is backed by 2 MB huge pages")
        print(f"  TLB entries needed: {huge_mb:.0f} (vs {SIZE_MB*512:.0f} for 4 KB pages)")
    else:
        print(f"  ❌ No huge pages — all {SIZE_MB*256:.0f} pages are 4 KB")
        print(f"  Enable THP: echo always | sudo tee "
              f"/sys/kernel/mm/transparent_hugepage/enabled")

    # Now enable THP and recheck
    print(f"\n--- Enabling THP for this array ---")
    libc = ctypes.CDLL("libc.so.6")
    MADV_HUGEPAGE = 14
    result = libc.madvise(ctypes.c_void_p(va), ctypes.c_size_t(arr.nbytes),
                          ctypes.c_int(MADV_HUGEPAGE))
    if result == 0:
        print("madvise(MADV_HUGEPAGE) succeeded")
        print("Re-touching pages to trigger huge page allocation...")
        arr[:] = 99.0  # re-touch to help khugepaged
        import time; time.sleep(0.5)  # give khugepaged a moment
        smaps2 = check_huge_page_smaps(va)
        huge_mb2 = smaps2.get('anon_huge', 0) / 1024
        print(f"AnonHugePages after madvise: {huge_mb2:.1f} MB")
    else:
        print(f"madvise failed (errno {ctypes.get_errno()})")


if __name__ == '__main__':
    lab3()
```

---

## 15. Summary

Virtual memory gives each process a private, isolated address space. The hardware MMU translates virtual addresses to physical addresses using the 4-level x86-64 page table (PML4 → PDPT → PD → PT). Each translation requires traversing up to 4 levels, costing up to 4 DRAM accesses (~280 ns) on a page table walk.

The TLB caches recent translations:
- L1 dTLB: 64 entries × 4 KB = **256 KB** coverage
- L2 STLB: 2048 entries × 4 KB = **8 MB** coverage

Any data engineering workload that accesses more than 8 MB of unique address space generates TLB misses. For random-access patterns (hash joins, high-cardinality groupby, pointer-chasing), every TLB miss adds 280 ns of sequential overhead that cannot be hidden by prefetching or OOO execution. This is an invisible tax — absent from flame graphs, invisible in CPU utilization, only visible in `perf stat -e dTLB-load-misses`.

**Huge pages (2 MB) expand TLB coverage 512×:**
- L2 STLB: 32 entries × 2 MB = **64 MB** coverage

For Spark executor heaps, NumPy large arrays, and Kafka page cache, enabling huge pages (via THP `madvise` + `madvise(MADV_HUGEPAGE)`, or explicit HugeTLB with `-XX:+UseLargePages`) reduces TLB miss rates by 50–99% and accelerates memory-intensive operations by 20–50%.

The configuration matrix:
- **Spark batch jobs:** THP `always` or `-XX:+UseLargePages` in executor JVM
- **Kafka/latency-sensitive:** THP `madvise`, explicit HugeTLB for JVM heap only
- **NumPy analytics:** `madvise(MADV_HUGEPAGE)` on large arrays, or THP `always`
- **PostgreSQL:** THP `never` (PG manages shared_buffers directly)

---

## 16. Interview Q&A

### Q1: Explain what the TLB is, what it stores, and what happens when it misses.

**Answer:**

The TLB (Translation Lookaside Buffer) is a small, extremely fast cache built into the CPU that stores recent virtual-to-physical address translations. Every memory access requires knowing the physical address corresponding to the program's virtual address — the MMU performs this translation using the OS-maintained page table. But the page table lives in DRAM: traversing its four levels on every access would add up to 280 ns of latency per instruction, making any program thousands of times slower.

The TLB solves this by caching translations. Modern x86-64 CPUs have two TLB levels. The L1 dTLB (data TLB) holds 64 entries for 4 KB pages, covering 64 × 4 KB = 256 KB of unique virtual addresses. It is checked in parallel with the L1 data cache lookup, so a TLB hit adds zero cycles to the memory access latency. The L2 STLB (Shared TLB) holds 2,048 entries × 4 KB = 8 MB coverage; an STLB hit adds ~7–8 cycles.

When both TLB levels miss, the CPU's hardware page table walker is activated. It reads CR3 (the register pointing to the PML4 root), then traverses four page table levels in memory: PML4 → PDPT → PD → PT. Each level is a separate memory read. If all four levels miss the L1 data cache (cold page table), that is four DRAM reads × ~70 ns each = ~280 ns total. If the page table levels happen to be in L1 cache (hot page table, common for recently-used mappings), the walk takes ~16 cycles. After the walk, the translation is installed in the TLB and the memory access completes. The page table walk is entirely hardware — it does not involve the OS or any software interrupt (unless the page is swapped out, which triggers a page fault).

For data engineering: hash join probes, high-cardinality groupby, and GC marking all perform random memory accesses across large data sets. When the working set exceeds 8 MB (the STLB coverage), each access to a new page costs 280 ns for the page table walk. On 100 million hash table probes with a 2 GB probe table, this adds ~28 seconds of TLB miss overhead that does not appear in any profiler — only in `perf stat -e dTLB-load-misses`.

---

### Q2: What is a huge page, and why does it help data engineering workloads?

**Answer:**

A huge page is a memory page larger than the default 4 KB. x86-64 CPUs support 2 MB huge pages (and 1 GB gigantic pages). The MMU's page table structure is adjusted so that the PD entry directly points to a 2 MB physical block rather than pointing to another level of page table.

The TLB benefit is the key reason to use huge pages. Each TLB entry covers one page. With 4 KB pages, the L2 STLB covers 2,048 × 4 KB = 8 MB of unique addresses. With 2 MB huge pages, the L2 STLB's 32 huge-page entries cover 32 × 2 MB = 64 MB. The coverage increase is 512×: the same number of TLB entries covers 512 times more address space. For a 100 MB data structure, 4 KB pages need 25,600 TLB entries (all miss the STLB) while 2 MB huge pages need 50 entries (fit in the STLB with room to spare).

For data engineering specifically: Spark executor heaps are routinely 100–500 GB. A 400 GB heap with 4 KB pages requires 104 million unique TLB entries — every random-access operation (GC marking, hash join probes) misses the STLB and pays 280 ns per access. With 2 MB huge pages, the same heap requires 200,000 entries; the STLB's 32 entries cover only 64 MB, so large heaps still miss the STLB, but each miss now covers 2 MB × 512 consecutive accesses instead of 4 KB × 1. The amortized TLB overhead per access drops from 280 ns to ~0.5 ns. GC marking throughput on a 400 GB heap improves by 30–50%; Spark shuffle sort performance improves by 15–30%.

On Linux, huge pages are available via Transparent Huge Pages (THP, automatic, kernel-managed) or explicit HugeTLB (pre-allocated pool, accessed via `mmap(MAP_HUGETLB)` or the JVM's `-XX:+UseLargePages` flag).

---

### Q3: Explain the 4-level x86-64 page table. Why four levels?

**Answer:**

The x86-64 virtual address space is 48 bits wide — 256 TB per process. A flat array mapping every possible 4 KB page would require 256 TB / 4 KB × 8 bytes per entry = 512 GB of page table per process. That is not viable.

The four-level page table is a radix tree that stores only entries for mappings that actually exist. The 48-bit virtual address is split into five fields: four 9-bit indices (9 bits × 4 levels = 36 bits) and a 12-bit page offset. Each 9-bit index selects one of 512 entries in a 4 KB table (512 × 8 bytes = 4 KB). The levels are PML4 (one per process, pointed to by CR3), PDPT, PD, and PT. Following one path from PML4 to a page table entry requires four reads.

Why four levels? It is a deliberate compromise. With four 9-bit levels:
- Max virtual address space: 2^(9×4+12) = 2^48 = 256 TB per process — generous for current hardware.
- Page table overhead for a sparse process (using 1 GB of memory): only PML4 (4 KB) + 2 PDPT pages (8 KB) + a few PD/PT pages — total overhead ~100 KB per process regardless of virtual address space size.
- Walk depth: 4 levels × ~70 ns/level = 280 ns worst case. A 5th level (PML5, for 57-bit virtual addresses) would add 70 ns per miss — only worthwhile for systems needing > 128 PB of virtual space.

Linux has recently added PML5 support for specialized use cases (large memory systems), making the walk 5 levels deep. For current data engineering deployments, 4 levels is standard.

---

### Q4: How do you configure Spark to use huge pages, and what measurable improvement should you expect?

**Answer:**

There are two approaches: Transparent Huge Pages (no pool pre-allocation needed) and explicit HugeTLB (requires pre-allocation).

For Transparent Huge Pages on Spark:

First, set the system-wide THP mode. For batch Spark jobs where latency spikes from THP promotion are acceptable: `echo always > /sys/kernel/mm/transparent_hugepage/enabled`. For more controlled promotion: `echo madvise > /sys/kernel/mm/transparent_hugepage/enabled`. Then add to `spark.executor.extraJavaOptions`:

```
-XX:+UseTransparentHugePages
```

This makes the JVM call `madvise(MADV_HUGEPAGE)` on its heap regions, requesting THP promotion. The JVM heap will be backed by 2 MB huge pages after the OS promotes the initial 4 KB pages.

For explicit HugeTLB (better performance, requires setup):

Pre-allocate the pool:
```bash
echo 204800 | sudo tee /proc/sys/vm/nr_hugepages
# 204,800 × 2 MB = 400 GB for the Spark executor heap
```

Add to Spark executor JVM options:
```
-XX:+UseLargePages -XX:LargePageSizeInBytes=2m
```

The JVM allocates its heap from the HugeTLB pool at startup, with 2 MB pages from the first allocation.

Measurable improvement: use `perf stat -e dTLB-load-misses,dTLB-loads` before and after. Before huge pages: `dTLB-load-misses / dTLB-loads` typically 25–45% for memory-intensive Spark stages. After huge pages: 2–8%. The runtime improvement for a sort-merge shuffle: 15–30%. For a GC-heavy job with a large live set: GC pause duration decreases 30–50%. The improvement is greatest for workloads with large random-access working sets — hash joins, high-cardinality aggregations, and GC marking.

---

### Q5: What is the difference between `madvise(MADV_HUGEPAGE)` and `mmap(MAP_HUGETLB)`? When would you use each?

**Answer:**

`madvise(MADV_HUGEPAGE)` is an advisory syscall: it asks the kernel's `khugepaged` daemon to promote existing 4 KB pages in a virtual address range to 2 MB huge pages "when convenient." The key word is advisory — the kernel may decline (if no contiguous 2 MB physical block is available due to memory fragmentation, or if the region is not aligned). The promotion happens asynchronously after the call returns. The memory region is initially allocated as 4 KB pages (by `malloc` or `mmap`), and the promotion happens in the background.

`mmap(MAP_HUGETLB)` allocates memory directly from the pre-allocated HugeTLB pool. The allocation is synchronous: the call either succeeds (returning a huge-page-backed region) or fails immediately (with ENOMEM if the pool is exhausted). There is no delay, no background promotion, and no dependence on `khugepaged`. The memory is immediately backed by huge pages from the first allocation.

Choose `madvise(MADV_HUGEPAGE)` when: you don't control the allocator (JVM heap, system libraries), or when you want automatic huge page promotion without managing a pool. This is the approach for `-XX:+UseTransparentHugePages` in the JVM. The trade-off: the timing of promotion is non-deterministic; for the first few seconds after JVM startup, the heap may still be on 4 KB pages.

Choose `mmap(MAP_HUGETLB)` (and manage the HugeTLB pool) when: you need guaranteed huge pages from the moment of allocation (no warm-up period), you can tolerate the operational complexity of pool management, or you want to avoid THP latency spikes from `khugepaged`. This is the approach for `-XX:+UseLargePages` in the JVM. The trade-off: the pool must be pre-allocated (consuming RAM even if not all huge pages are used), and the JVM fails to start if the pool is exhausted.

---

### Q6: Why does PostgreSQL's documentation recommend `vm.transparent_hugepage=never`?

**Answer:**

PostgreSQL has two primary memory regions: the `shared_buffers` pool (a large shared memory segment, typically 25% of RAM) and the private process heap (for query working memory). PostgreSQL itself manages the cache replacement policy for `shared_buffers` — it decides which 8 KB database blocks to keep warm and which to evict, implementing its own LRU/clock algorithm.

Transparent Huge Pages interfere with PostgreSQL's memory management in two ways. First, `khugepaged`'s background page promotion can freeze PostgreSQL's shared_buffers region for 1–10 ms during the VMA write-lock acquisition that precedes the collapse of 512 pages into one huge page. For a database, any stall > 1 ms in shared buffer access can cascade into client-visible query latency spikes. PostgreSQL's default target for P99 latency is well under 1 ms; THP-induced stalls violate this directly.

Second, huge page granularity conflicts with PostgreSQL's page eviction granularity. PostgreSQL evicts 8 KB database blocks one at a time, implementing precise buffer cache management. With THP, if 512 consecutive 4 KB pages (256 × 8 KB database blocks) are collapsed into one 2 MB huge page, evicting any one of those 256 blocks requires breaking the huge page back into 512 individual 4 KB pages first (a "huge page split" — also requiring a brief write lock). This adds latency to every eviction operation.

PostgreSQL instead uses explicit `shmget(IPC_HUGETLB)` or `mmap(MAP_HUGETLB)` with `vm.nr_hugepages` for its `shared_buffers` when you configure `huge_pages=on` in `postgresql.conf`. This gives PostgreSQL the TLB benefits of huge pages without surrendering control of page granularity to `khugepaged`. The recommendation to set `transparent_hugepage=never` at the OS level (while using `huge_pages=on` in PostgreSQL) reflects this: PostgreSQL wants huge pages on its own terms, not the kernel's.

---

## 17. Cross-Question Chain

**Topic:** From "what is virtual memory?" to "why does Spark need `-XX:+UseLargePages`?"

---

**Interviewer:** What is virtual memory, and why do we use it?

**Candidate:** Virtual memory is an abstraction where each process sees a private address space — typically 48 bits on x86-64, giving 256 TB of virtual addresses — regardless of how much physical RAM the machine has or what other processes are running. Programs access their variables through virtual addresses; the MMU (Memory Management Unit) hardware transparently translates these to physical addresses on every memory access. The OS maintains the mapping in data structures called page tables. Virtual memory provides isolation (process A cannot read process B's memory), enables demand paging (pages not currently needed can live on disk), and allows shared libraries to be mapped once into all processes without consuming extra physical RAM.

---

**Interviewer:** How does the MMU know which physical address a virtual address maps to?

**Candidate:** The MMU uses the page table, a multi-level radix tree. On x86-64, it has four levels (PML4, PDPT, PD, PT). The virtual address is split into five fields: four 9-bit indices and a 12-bit page offset. The CPU's CR3 register points to the PML4 table. To translate an address, the MMU reads PML4[index0], which gives the base address of the PDPT table. Then it reads PDPT[index1] to get the PD base, PD[index2] for the PT base, and PT[index3] for the physical frame number. The physical address is: frame_number × 4096 + page_offset. In the worst case, this requires four DRAM reads — approximately 280 ns. That's the cost of a page table walk when nothing is cached.

---

**Interviewer:** So every memory access costs 280 ns for the translation?

**Candidate:** No — that is what the TLB prevents. The TLB (Translation Lookaside Buffer) is a hardware cache of recent virtual→physical translations. It sits in the CPU core, between the execution unit and the cache/DRAM hierarchy. When the CPU needs to translate an address, it checks the TLB first. If the translation is there (a TLB hit), it takes 0 extra cycles — the lookup happens in parallel with the L1 cache access. If not there, the CPU walks the page table as described. Modern CPUs have a two-level TLB: the L1 dTLB (64 entries, 4 KB coverage = 256 KB) and the L2 STLB (2,048 entries, 4 KB coverage = 8 MB). As long as all memory accesses fall within those 8 MB of recently-used pages, translation is free.

---

**Interviewer:** What happens when a Spark executor's working set exceeds 8 MB?

**Candidate:** Every access to a new 4 KB page outside the STLB triggers a page table walk — 280 ns of sequential overhead. For data engineering workloads, this is the norm: a 50 GB Spark executor heap accessed randomly during sorting, a 2 GB hash table probed during a join, a GC marking pass through 100 GB of live objects. All of these access millions of unique 4 KB pages, far exceeding the 2,048-entry STLB. The TLB miss rate for random-access patterns on large data sets is typically 30–50%. At 280 ns per miss and 100 million accesses, that is 14–28 seconds of TLB overhead added to each stage — invisible in flame graphs, silent in application profiling, only measurable with `perf stat -e dTLB-load-misses`.

---

**Interviewer:** How does `-XX:+UseLargePages` fix this?

**Candidate:** With 2 MB huge pages, each TLB entry covers 512× more address space. The STLB's 32 huge-page entries cover 32 × 2 MB = 64 MB instead of 8 MB. More importantly, a 50 GB heap that required 13 million unique page table entries now requires only 25,000. Each TLB miss now covers 2 MB of heap instead of 4 KB — meaning 512 consecutive allocations benefit from one translation, rather than just one. The amortized TLB miss cost per memory access drops from ~140 ns (50% miss rate × 280 ns) to ~0.5 ns.

`-XX:+UseLargePages` makes the JVM allocate its heap from the OS's pre-allocated HugeTLB pool (configured via `vm.nr_hugepages`). The heap is backed by 2 MB pages from the first allocation — no warm-up period, no dependence on `khugepaged`. The measured improvement on Spark jobs with large heaps (> 100 GB): GC pause reduction of 30–50%, sort stage improvement of 15–25%, hash join probe improvement of 20–40%. The exact numbers depend on the access pattern's randomness — sequential access patterns benefit less (prefetcher hides TLB miss latency for sequential patterns), while random-access patterns see the full benefit.

---

## 18. What's Next

**CSF-ARC-102 M04: The Hardware Prefetcher** — We have used the prefetcher as a given in M01, M02, and M03: "sequential access activates the prefetcher and hides DRAM latency." M04 opens the prefetcher up: what algorithms it uses (next-line prefetcher, stride detector, stream buffer), what stride patterns it can detect (and which it cannot), how many outstanding prefetch requests it can manage, the difference between hardware prefetching and software prefetch instructions (`_mm_prefetch`), and how to write NumPy array access patterns that maximize prefetcher effectiveness. This module explains precisely why sequential Parquet column reads are fast and why certain join implementations are slow despite sequential-looking code.

**CSF-ARC-102 M05: False Sharing and Cache Coherence** — The final module of CSF-ARC-102. M01 introduced MESI briefly; M05 is its full treatment. False sharing is the failure mode where two threads write to different variables that share a 64-byte cache line, causing the MESI state to bounce M→I→M→I on both cores for every write — saturating the coherence bus and reducing throughput to near-serial. This is a real production bug in Spark's accumulator implementation, in multi-threaded Python (via ctypes or Cython with shared arrays), and in any C++ extension that packs per-thread counters naively. M05 covers detection (via `perf stat -e cache-misses` under lock contention), padding, alignment, and alternative data structures.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is virtual memory? | An OS abstraction giving each process a private address space (48 bits = 256 TB on x86-64). Programs use virtual addresses; the MMU translates them to physical addresses on every access using the page table. |
| 2 | What is the x86-64 page table structure? | 4 levels: PML4 → PDPT → PD → PT. Virtual address split: 9 bits per level (4×) + 12-bit page offset. CR3 points to PML4. Walk cost: up to 4 DRAM reads = ~280 ns. |
| 3 | What is the TLB? | Translation Lookaside Buffer — a hardware cache of recent virtual→physical address translations. L1 dTLB: 64 entries × 4 KB = 256 KB coverage, 0 extra cycles. L2 STLB: 2,048 entries × 4 KB = 8 MB coverage, ~8 cycles. |
| 4 | What happens on a TLB miss with no STLB hit? | Hardware page table walk: 4 sequential memory reads (PML4/PDPT/PD/PT). Best case (all in L1 cache): ~16 cycles. Worst case (all in DRAM): ~280 ns = ~980 cycles at 3.5 GHz. |
| 5 | What is the STLB coverage for 4 KB pages? | 2,048 entries × 4 KB = 8 MB. Any working set > 8 MB (virtually all data engineering workloads) will miss the STLB on new page accesses. |
| 6 | What is the STLB coverage for 2 MB huge pages? | 32 entries × 2 MB = 64 MB. Huge pages give 8× more coverage from the same STLB capacity. |
| 7 | Why does the TLB miss matter more for random access than sequential? | For sequential access, the prefetcher hides DRAM latency (including page table walk overhead). For random access (hash joins, pointer-chasing), each access stalls on both TLB miss + cache miss sequentially — no hiding possible. |
| 8 | What is a page table walk? | The CPU's hardware process of translating a virtual address to physical: CR3 → PML4[idx0] → PDPT[idx1] → PD[idx2] → PT[idx3] → physical frame number. Activated on every TLB miss. |
| 9 | What are the three page sizes on x86-64? | 4 KB (default), 2 MB (huge pages, PD entry points directly to physical block, skips PT), 1 GB (gigantic pages, PDPT entry points directly, skips PD and PT). |
| 10 | What is Transparent Huge Pages (THP)? | Linux kernel feature that automatically promotes contiguous 4 KB pages to 2 MB huge pages in background via `khugepaged`. Modes: `always`, `madvise`, `never`. |
| 11 | What does `madvise(MADV_HUGEPAGE)` do? | Asks the kernel to use THP for a specific virtual address range. With THP in `madvise` mode, only regions that explicitly call this get huge pages. Used by JVM with `-XX:+UseTransparentHugePages`. |
| 12 | What is HugeTLB (explicit huge pages)? | A pre-allocated pool of huge pages (configured via `vm.nr_hugepages`). Applications access it via `mmap(MAP_HUGETLB)` or JVM `-XX:+UseLargePages`. Guaranteed huge pages from first allocation; no THP latency spikes. |
| 13 | Why does THP cause latency spikes for Kafka? | `khugepaged` takes a write lock on the VMA during page promotion. Any thread accessing the locked region is blocked (1–50 ms). For Kafka brokers needing P99 < 5 ms, this is unacceptable. Fix: `madvise` mode (opt-in per process). |
| 14 | What does `-XX:+UseLargePages` do for Spark? | Makes the JVM allocate its heap from the HugeTLB pool (2 MB huge pages). Result: GC marking 30–50% faster, shuffle sort 15–25% faster, hash join probes 20–40% faster for large working sets. |
| 15 | What is the TLB miss overhead formula? | `overhead = miss_rate × miss_penalty × (1 - overlap_factor)`. Miss penalty: 280 ns worst case. Overlap factor: ~0.7 for sequential (prefetcher covers some); ~0 for random access. |
| 16 | How do you measure TLB miss rate? | `perf stat -e dTLB-loads,dTLB-load-misses`. Miss rate = `dTLB-load-misses / dTLB-loads`. < 1%: healthy. 1–5%: moderate. > 5%: TLB bottleneck — consider huge pages. |
| 17 | Why does PostgreSQL recommend `transparent_hugepage=never`? | THP promotion (`khugepaged` VMA write lock) can stall PG's shared_buffers access for 1–10 ms. PG instead uses explicit `shmget(IPC_HUGETLB)` with `huge_pages=on` in postgresql.conf — huge pages on PG's own terms. |
| 18 | What is the first-touch NUMA policy's interaction with THP? | THP pages are allocated on the NUMA node of the thread that first writes to the virtual range. Combining NUMA binding (`numactl --membind=N`) with THP ensures huge pages land on the intended NUMA node. |
| 19 | How many huge-page TLB entries does a 50 GB Spark heap need? | 4 KB pages: 50 GB / 4 KB = 13.1 million entries. 2 MB huge pages: 50 GB / 2 MB = 25,600 entries. STLB has 32 huge-page entries. Both exceed STLB, but huge-page misses cover 512× more heap per miss — 512× less TLB overhead amortized per access. |
| 20 | What is the page offset, and how many bits is it? | The byte offset within a page. For 4 KB pages: 12 bits (2^12 = 4,096). For 2 MB huge pages: 21 bits (2^21 = 2,097,152). The physical address = physical_frame_number × page_size + page_offset. |

---

## 20. References

**Virtual Memory and Page Tables**

- Intel (2024). Intel 64 and IA-32 Architectures Software Developer's Manual, Volume 3A, Chapter 4 (Paging). https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html — Authoritative source on 4-level paging, TLB structure, and CR3.
- Drepper, U. (2007). What every programmer should know about memory. Section 4 (Virtual Memory). https://lwn.net/Articles/253361/ — Practical coverage of page tables, TLB miss costs, and huge pages.
- Love, R. (2010). *Linux Kernel Development* (3rd ed.). Addison-Wesley. Chapter 15 (The Process Address Space) — Linux VMA implementation and page fault handling.

**Huge Pages**

- Linux kernel documentation: https://www.kernel.org/doc/html/latest/admin-guide/mm/hugetlbpage.html — HugeTLB configuration, `nr_hugepages`, `hugetlbfs`, `MAP_HUGETLB`.
- Linux kernel documentation: https://www.kernel.org/doc/html/latest/admin-guide/mm/transhuge.html — THP `always`/`madvise`/`never` modes, `khugepaged` tuning.
- Corbet, J. (2011). Transparent huge pages. LWN.net. https://lwn.net/Articles/423584/ — Original THP design, trade-offs, and khugepaged algorithm.

**TLB Performance Measurement**

- Peleg, A., et al. (1994). Intel MMX for Multimedia PCs. *Communications of the ACM*. — Historical context for STLB design decisions.
- Bakhvalov, D. (2020). TLB misses and their impact. easyperf.net. https://easyperf.net/blog/2020/09/28/Measuring-TLB-pressure — Practical `perf stat` measurement guide with workload examples.
- AMD Software Optimization Guide: Section 2.7 (TLB Architecture) — AMD's ITLB/DTLB/L2-TLB sizes and huge page TLB entries.

**Data Engineering Context**

- JVM Flags reference: `-XX:+UseLargePages`, `-XX:+UseTransparentHugePages`, `-XX:LargePageSizeInBytes`. https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html
- PostgreSQL documentation: `huge_pages` parameter. https://www.postgresql.org/docs/current/runtime-config-resource.html — Why PG uses explicit huge pages instead of THP.
- Martin, M. (2019). Large pages in Java. https://shipilev.net/jvm/large-pages/ — Aleksey Shipilev's deep analysis of `-XX:+UseLargePages` vs `-XX:+UseTransparentHugePages` for JVM workloads, including measured TLB miss rates and GC improvements.
