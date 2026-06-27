# CSF-OS-101 M03: Virtual Memory and Page Cache

**Course:** CSF-OS-101 — Operating System Internals for Data Engineers  
**Module:** 03 of 05  
**Filesystem position:** 00_CS_Foundations/M19_Virtual_Memory_and_Page_Cache  
**Prerequisites:** CSF-OS-101 M01 (Processes and Threads), M02 (Process Scheduling)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: Virtual Memory](#4-core-theory-virtual-memory)
5. [Page Tables and the TLB](#5-page-tables-and-the-tlb)
6. [Page Faults](#6-page-faults)
7. [The OS Page Cache](#7-the-os-page-cache)
8. [Memory-Mapped Files (mmap)](#8-memory-mapped-files-mmap)
9. [Deep Dive: Why Spark Tungsten Bypasses the JVM Heap](#9-deep-dive-why-spark-tungsten-bypasses-the-jvm-heap)
10. [Mental Models](#10-mental-models)
11. [Failure Scenarios](#11-failure-scenarios)
12. [Data Engineering Connections](#12-data-engineering-connections)
13. [Code Toolkit](#13-code-toolkit)
14. [Hands-On Labs](#14-hands-on-labs)
15. [Interview Q&A](#15-interview-qa)
16. [Cross-Question Chain](#16-cross-question-chain)
17. [Common Misconceptions](#17-common-misconceptions)
18. [Performance Reference Card](#18-performance-reference-card)
19. [Connections to Other Modules](#19-connections-to-other-modules)
20. [Flashcards](#20-flashcards)
21. [Module Summary](#21-module-summary)

---

## 1. The Problem This Module Solves

A Spark job reading 500 GB of Parquet files from local disk is fast the first time and three times faster the second time — even though no caching was configured in Spark. Why? The OS page cache silently retained the file data in RAM between runs.

An Airflow worker running 16 concurrent tasks appears to use 16 × 2 GB = 32 GB of memory in `top`, but the machine only has 24 GB of RAM and nothing is swapping. How is this possible? Virtual memory with shared pages and copy-on-write.

A Spark executor crashes with `OutOfMemoryError: Direct buffer memory` — not the standard JVM heap OOM. A different executor crashes with `Cannot allocate memory in static TLS block`. Neither error is about the heap. Both are about memory regions outside the JVM heap that Spark's Tungsten engine manages directly.

A Pandas DataFrame holding 10 GB of data is passed to a Python `multiprocessing.Pool`. The 8 worker processes each show 10 GB in their RSS (resident set size). Is 80 GB of RAM actually being used? No — because `fork()` + copy-on-write means the workers share the same physical pages as the parent until they write to them.

All four situations require understanding virtual memory: how the OS maps virtual addresses to physical RAM, what the page cache is and how it works, what memory-mapped I/O enables, and how the JVM heap interacts with the OS memory system. This module builds the foundation for diagnosing memory-related performance issues in any data engineering context.

---

## 2. How to Use This Module

**For production debugging:** Sections 7, 9, 11. Section 7 explains why Parquet re-reads are faster (page cache). Section 9 explains Spark's `spark.memory.offHeap.enabled` and why Tungsten uses unsafe memory. Section 11 covers the four most common memory failures: OOM from page cache eviction under memory pressure, Spark direct buffer exhaustion, JVM GC stop-the-world pauses from heap fragmentation, and swap thrashing.

**For interview prep:** Sections 4, 5, 6, 8, 15, 16. Virtual memory, page faults, and the page cache are canonical OS interview topics for staff data engineering roles. The `mmap` question (Section 8) bridges OS concepts directly to Parquet, Arrow, and Spark's shuffle implementation.

**For system design:** Sections 8, 9, 12. mmap and off-heap memory are architectural choices that appear in every high-performance data system. Understanding when to use them and why requires this module.

---

## 3. Prerequisites Check

- **Process address space (M01):** You know processes have virtual address spaces with separate regions (stack, heap, text, mmap). This module explains how those virtual addresses become physical memory accesses.
- **Context switching (M02):** You know context switches flush the TLB. This module explains what the TLB is and why flushing it is expensive.
- **File I/O intuition:** You know that reading a file involves the OS. This module explains exactly what happens between `open()` and the data appearing in a Python variable.

---

## 4. Core Theory: Virtual Memory

### 4.1 The Problem Virtual Memory Solves

Physical RAM is limited and shared. In a system with 128 GB of RAM and 500 running processes (typical for a busy Linux server), there is not enough RAM for every process to use its entire address space at once. Virtual memory solves three problems simultaneously:

**Isolation:** Process A cannot read or write Process B's memory — even if they both "think" they're using address 0x7fff000. Each process has its own mapping from virtual addresses to physical addresses.

**Overcommitment:** A process can claim (allocate) more virtual address space than physical RAM exists. The OS only maps virtual pages to physical pages when the process actually accesses them — lazy allocation. A Python process that allocates a 100 GB array but only reads 10 GB of it at a time never needs 100 GB of physical RAM simultaneously.

**Shared pages:** Multiple processes can map the same physical page at different virtual addresses. The `libc.so` shared library (used by every process on the system) occupies one copy of physical RAM shared among all processes — not N copies for N processes. Post-`fork()` copy-on-write uses the same mechanism.

### 4.2 Pages: The Unit of Virtual Memory

Virtual memory is managed in fixed-size chunks called **pages**. On x86-64 Linux, the standard page size is **4 KB** (4096 bytes). Huge pages are 2 MB or 1 GB (more on those in Section 5.3).

A virtual address is interpreted as two parts:
- **Page number:** the upper bits — identifies which page
- **Page offset:** the lower 12 bits (for 4 KB pages) — identifies the byte within the page

```
Virtual address (48-bit on x86-64):
  Bits 47–12: virtual page number (36 bits → 2^36 = 64 billion pages per process)
  Bits 11–0:  page offset (12 bits → 4096 bytes per page)

Physical address:
  Bits 51–12: physical frame number (hardware-dependent: 40 bits → 1 TB addressable RAM)
  Bits 11–0:  offset (same as virtual — offset within a page is preserved)

Translation:
  virtual address → page table lookup → physical frame number
  physical address = (physical_frame_number << 12) | page_offset
```

### 4.3 The Page Table

The **page table** is the OS data structure that maps virtual page numbers to physical frame numbers. The CPU's **MMU (Memory Management Unit)** hardware performs the translation on every memory access using the page table.

On x86-64, the page table is a four-level hierarchical structure (called PML4 or CR3 after the register that points to the root):

```
CR3 register → Physical address of PML4 (Level 4) table
                  ↓ (indexed by vaddr bits 47-39)
              PML4 Entry → Physical address of PDPT (Level 3) table
                  ↓ (indexed by vaddr bits 38-30)
              PDPT Entry → Physical address of PD (Level 2) table
                  ↓ (indexed by vaddr bits 29-21)
              PD Entry   → Physical address of PT (Level 1) table
                  ↓ (indexed by vaddr bits 20-12)
              PT Entry   → Physical frame number + flags
                  ↓ + page offset (bits 11-0)
              Physical address
```

Each level table has 512 entries × 8 bytes = 4 KB (one page). Only the entries that are actually needed are allocated — a process that uses 1 GB of its virtual address space doesn't need a full 512 GB page table; only the entries along the paths to the 1 GB of used pages are allocated.

**Page table entry flags:**
- **Present bit:** 1 = page is in physical RAM; 0 = page is not in RAM (access causes page fault)
- **Read/Write bit:** 0 = read-only (write causes protection fault)
- **User/Supervisor bit:** 0 = kernel-only; 1 = user-accessible
- **Dirty bit:** set by hardware when page is written (used by OS for copy-on-write)
- **Accessed bit:** set by hardware when page is read or written (used by page replacement algorithms)

### 4.4 Resident Set Size vs Virtual Set Size

Two memory metrics visible in `top` and `ps`:

**VSZ (Virtual Size):** Total virtual address space claimed by the process. Includes all mapped regions, whether or not they have physical RAM backing. A Python process that allocates a 10 GB list has it in VSZ immediately.

**RSS (Resident Set Size):** Physical RAM currently occupied by the process's pages. Only pages that have been accessed (and thus had physical frames assigned) count. RSS ≤ VSZ always. RSS > available_RAM means the system is swapping.

**PSS (Proportional Set Size):** RSS adjusted for shared pages — each shared page is counted once per process sharing it. `smem` tool reports PSS. For understanding true memory consumption, PSS is more accurate than RSS.

```bash
# View per-process memory
ps aux --sort=-%mem | head -20
# VSZ and RSS columns are in KB

# Detailed memory breakdown
cat /proc/<PID>/status
# VmRSS:    ← resident set size (physical RAM actually used)
# VmPeak:   ← peak virtual memory size
# VmSize:   ← current virtual memory size
# VmSwap:   ← pages currently swapped to disk

# Per-region memory map
cat /proc/<PID>/maps          # virtual regions with flags
cat /proc/<PID>/smaps         # per-region RSS, PSS, swap
```

---

## 5. Page Tables and the TLB

### 5.1 The TLB: Translation Lookaside Buffer

Every memory access requires translating a virtual address to a physical address. A four-level page table walk requires **4 memory accesses** (one per level). If every memory access required 4 additional memory accesses just for translation, programs would run 5× slower. The hardware solves this with the **TLB (Translation Lookaside Buffer)** — a small, fast associative cache of recent virtual→physical translations.

```
Memory access: load value at virtual address V

Step 1: Check TLB for V
  TLB hit (most common):
    Translation found in TLB → get physical address directly
    Cost: ~1 cycle (same as L1 cache access)
    
  TLB miss (less common):
    Translation not in TLB → hardware page table walk
    Walk 4 levels of page table (4 memory accesses, each ~100 cycles if in L3)
    Store result in TLB
    Total cost: ~400–1000 cycles
    
Step 2: Access physical memory at translated address
  L1 cache hit: ~4 cycles
  L2 cache hit: ~12 cycles
  L3 cache hit: ~40 cycles
  RAM (DRAM): ~200 cycles
```

TLB hit rate is critical for performance. Modern CPUs have:
- **L1 TLB:** 64 entries (instruction) + 64 entries (data), covers 64 × 4 KB = 256 KB
- **L2 TLB (unified):** 1024–4096 entries, covers ~16 MB
- **STLB (second-level):** some CPUs add a third TLB level

For a process with a 100 MB working set (10 MB × 10 concurrent tasks), the 1024-entry L2 TLB covers only 4 MB. 60% of accesses in a 100 MB working set miss the TLB → 60% of accesses trigger a page table walk → significant performance overhead.

### 5.2 TLB Flush on Context Switch

When the OS switches from Process A to Process B, Process A's virtual→physical mappings are different from Process B's mappings. The TLB must be **flushed** — all cached translations invalidated — otherwise Process B might use Process A's stale translations (security violation + correctness error).

TLB flush cost on x86-64:
- Full TLB flush (on process context switch): ~200–500 ns
- Subsequent TLB misses until TLB is "warm" again: ~1000 ns per miss

This is why process context switches are more expensive than thread context switches (Section M02): thread switches within the same process don't need a TLB flush (same virtual→physical mappings), but process switches always do.

**ASIDs (Address Space Identifiers):** Some architectures (ARM, newer x86-64 with PCID support) tag TLB entries with a process ID (PCID on x86-64, introduced in Intel Westmere). This allows TLB entries from different processes to coexist without flushing on context switch. Linux uses PCID on supported hardware (`/proc/cpuinfo` flag `pcid`). With PCID, context switches are significantly faster — the TLB is not flushed, and per-process entries are reused.

### 5.3 Huge Pages and Transparent Huge Pages (THP)

Standard 4 KB pages require one TLB entry per 4 KB of mapped memory. For a 4 GB JVM heap:
- 4 GB / 4 KB = 1,048,576 pages → 1 million TLB entries needed
- L2 TLB holds ~4096 entries → TLB covers only 16 MB of the 4 GB heap
- Most heap accesses miss the TLB → page table walk on most heap accesses

**Huge pages (2 MB or 1 GB)** solve this. One 2 MB huge page uses one TLB entry but covers 512× more memory:
- 4 GB heap using 2 MB huge pages → 2048 pages → fits in L2 TLB (4096 entries)
- TLB covers the entire heap → virtually no TLB misses

```bash
# Check huge page availability
cat /proc/meminfo | grep -i huge
# HugePages_Total:    1024
# HugePages_Free:     1024
# Hugepagesize:       2048 kB

# Enable Transparent Huge Pages (THP): kernel auto-promotes 4KB pages to 2MB
cat /sys/kernel/mm/transparent_hugepage/enabled
# [always] madvise never
# 'always': kernel converts aligned 4KB page regions to 2MB huge pages automatically

# Configure JVM to use huge pages
java -XX:+UseLargePages -Xmx16g -jar spark-executor.jar
# Linux also requires: vm.nr_hugepages = <number_of_2MB_pages_needed>
# For 16 GB heap: 16384 MB / 2 MB = 8192 huge pages
echo 8192 > /proc/sys/vm/nr_hugepages
```

**THP and Spark:** THP is enabled by default on most Linux distributions. For Spark, THP provides measurable benefit for large JVM heaps (16+ GB) — TLB miss rates drop, random-access operations (hash joins, sort buffer access) are faster. The caveat: THP can cause latency spikes when the kernel promotes pages (requires scanning memory regions), and THP with defragmentation enabled (`defrag=always`) can cause multi-second pauses. For Spark, `defrag=madvise` is recommended — only defragment when `madvise(MADV_HUGEPAGE)` is called explicitly.

---

## 6. Page Faults

### 6.1 What Is a Page Fault?

A **page fault** is a hardware exception raised by the CPU's MMU when a memory access cannot be completed through the TLB and page table — either because the page is not present in physical RAM, or because the access violates the page's permissions.

When a page fault occurs:
1. CPU saves the current instruction pointer and registers
2. CPU transfers control to the OS's page fault handler
3. OS examines the faulting address and determines the cause
4. OS resolves the fault (allocates a page, reads from disk, etc.) or kills the process
5. OS returns to the faulting instruction, which is retried

### 6.2 Types of Page Faults

**Minor page fault (soft fault):** The page is in memory (or can be set up) without I/O:
- **Demand allocation:** A process calls `malloc()`. The memory allocator reserves virtual address space but the OS doesn't assign physical pages yet. When the process first writes to the address, a minor page fault fires. The OS assigns a physical page (zeroed), updates the page table entry, and returns. Cost: ~1–10 µs.
- **Copy-on-write:** After `fork()`, parent and child share read-only physical pages. When either process writes, a minor page fault fires. The OS copies the page to a new physical frame, updates the writing process's page table to point to the new frame, and marks it writable. Cost: ~1–10 µs.
- **Shared mapping access:** A file was memory-mapped; the page is in the page cache but not yet mapped into this process's page table. The OS updates the page table entry to point to the existing page cache frame. Cost: ~1–2 µs.

**Major page fault (hard fault):** The page must be read from disk (or from the swap partition):
- **Demand paging from executable:** When a program first runs, its code is not in RAM. The first execution of each code page triggers a major page fault — the OS reads it from the `.so` file on disk. Cost: ~1–10 ms (disk I/O).
- **Swapped-out page:** A page was evicted from RAM to the swap partition. The OS reads it back. Cost: ~5–100 ms (disk I/O, depends on storage type).
- **First access to a memory-mapped file:** A file was `mmap()`-ed, the process accesses a page not yet in the page cache. The OS reads the 4 KB page from disk into the page cache and maps it. Cost: ~1–10 ms.

```bash
# View major and minor page faults per process
pidstat -r -p <PID> 1
# minflt/s  majflt/s

# ps also shows total faults
ps -o pid,minflt,majflt -p <PID>

# For a Spark executor:
# High majflt/s during job execution → swapping or first access to mmapped files
# High minflt/s during JVM startup → normal (loading .so files, JVM code)
# High minflt/s during job → COW faults from fork() calls (rare in Spark)
```

**Segmentation fault (SIGSEGV):** A page fault where the faulting address is not within any valid virtual memory region for the process. The OS sends SIGSEGV, which by default kills the process and optionally dumps a core. A Python `Segmentation fault (core dumped)` is a SIGSEGV — typically caused by a bug in a C extension, not in Python itself.

### 6.3 Swap: When RAM Is Exhausted

When physical RAM is full and a new page must be allocated, the OS must evict an existing physical page to make room. The **page replacement algorithm** (Linux uses a variant of LRU — Least Recently Used) selects a victim page:

- **Clean page:** The page's contents match what is on disk (either a read-only file page or a page that hasn't been modified since it was loaded). Evicting a clean page requires only updating the page table — no write to disk. The page can be reloaded from its original file if needed.

- **Dirty page:** The page has been modified and the modifications are not yet on disk. Evicting a dirty page requires writing it to the **swap partition** (or swap file) first, then updating the page table entry to point to the swap slot.

```
Swap sequence:
  1. RAM is full; process needs a new page
  2. OS selects victim page (dirty, not recently used)
  3. OS writes victim to swap partition (disk I/O: ~5-100 ms)
  4. OS updates victim's page table entry: present=0, swap_slot=N
  5. OS assigns vacated physical frame to requesting process

Later access to swapped-out page:
  1. CPU generates page fault (present bit = 0)
  2. OS reads swap slot back to a new physical frame (disk I/O: ~5-100 ms)
  3. OS updates page table entry: present=1, frame=new_frame
  4. Instruction retried
```

**Thrashing:** When a system is so memory-constrained that it spends more time swapping pages in and out than doing useful work. The OS is constantly evicting and re-loading the same pages. CPU utilization drops toward 0 while disk I/O is 100%.

```bash
# Monitor swapping
vmstat 1
# si (swap in MB/s): pages read from swap
# so (swap out MB/s): pages written to swap
# Sustained non-zero si/so = swapping is happening → memory pressure

# Check swap usage
free -h
# total  used  free  shared  buff/cache  available
# Mem:   128G   96G   2.1G   1.2G        29G         31G
# Swap:   32G    8G   24G                           ← 8 GB in use

# If Mem available < 1 GB and Swap used > 0, the system is under memory pressure
```

---

## 7. The OS Page Cache

### 7.1 What Is the Page Cache?

The **page cache** (also called the buffer cache) is the OS's mechanism for caching disk file data in RAM. When a process reads a file, the OS reads the data from disk into a page cache region, then copies it to the process's address space. On a subsequent read of the same file data (same inode, same byte offset), the OS finds the data already in the page cache and copies it without any disk I/O.

The page cache is not a separate memory pool — it lives in the same physical RAM as process memory. Linux's page cache uses the same page frames as everything else; the pages are simply tagged as "belonging to file X at offset Y." The `buff/cache` column in `free -h` shows how much RAM is currently used by the page cache (plus kernel buffers).

```
RAM layout on a running system (128 GB machine):
  Process memory:       64 GB (stacks, heaps, code of all running processes)
  Page cache:           48 GB (file data cached for fast re-reads)
  Kernel buffers:        4 GB (I/O buffers, dentry/inode caches)
  Free (unallocated):   12 GB
  
  "Available" in free -h = Free + reclaimable page cache
  (page cache can be evicted on demand to make room for process memory)
```

### 7.2 Page Cache Lifecycle

```
Read path (first access to a file):
  1. Process calls read(fd, buf, count)
  2. Kernel: is this file range in the page cache?
  3. Cache MISS: kernel schedules I/O to read page from disk
  4. I/O completes: kernel stores page in page cache (keyed by inode + offset)
  5. Kernel copies data from page cache into process's buffer (buf)
  6. read() returns

Read path (subsequent access to same data):
  1. Process calls read(fd, buf, count)
  2. Kernel: is this file range in the page cache?
  3. Cache HIT: data is in RAM, no disk I/O needed
  4. Kernel copies data from page cache into process's buffer
  5. read() returns  (latency: ~microseconds vs ~milliseconds for disk)

Write path:
  1. Process calls write(fd, buf, count)
  2. Kernel: allocate/find page cache page for this file range
  3. Kernel copies data from process buffer into page cache page
  4. Page is marked "dirty" (modified, not yet on disk)
  5. write() returns immediately (without waiting for disk write)
  6. OS writes dirty pages to disk asynchronously (pdflush/writeback threads)
     - Triggered by: dirty_expire_centisecs (30s default) or dirty_ratio (20% of RAM)
     - Can be forced: fsync(fd) waits for all dirty pages of fd to be flushed
```

### 7.3 Why the Page Cache Matters for Data Engineering

**Parquet re-read acceleration:** A 100 GB Parquet dataset read by Spark fills the page cache with the file data. The second Spark job reading the same dataset (perhaps different filters) reads from the page cache at ~10 GB/s (DRAM bandwidth) instead of from disk at ~500 MB/s — a 20× speedup. This is "warm cache" behavior and explains why repeated Spark job runs on the same data are dramatically faster.

**`drop_caches` for benchmarking:** Testing storage throughput requires a cold cache:
```bash
# Force drop of page cache before benchmarking disk reads
sync && echo 3 > /proc/sys/vm/drop_caches
# 1 = drop page cache only
# 2 = drop dentries and inodes
# 3 = drop all (page cache + dentries + inodes)
```

**Memory pressure evicts the page cache:** When a Spark executor needs more memory for its JVM heap (due to a memory-intensive shuffle), the OS evicts page cache entries to free physical frames. The next Spark job reads those files from disk again. This is the "Spark steals page cache from itself" problem — large Spark heaps leave little room for the page cache, reducing Parquet read speed for subsequent reads.

**Direct I/O bypasses the page cache:** Some systems (Kafka's log segments, some database engines) use `O_DIRECT` flag to bypass the page cache entirely. This avoids double-buffering (page cache + application buffer) and gives the application full control over what is cached. Kafka uses `sendfile()` (zero-copy: page cache → network socket directly) for log reads, which requires the page cache but avoids a user-space copy.

### 7.4 Reclaim and `vm.swappiness`

When the OS needs to reclaim memory (for new allocations or to reduce memory pressure), it can evict:
1. **Clean page cache pages** (free without I/O, can be reloaded from disk on demand)
2. **Anonymous pages** (process heap/stack, must be written to swap before eviction)

`vm.swappiness` (0–100, default 60) controls the balance:
- `swappiness=0`: strongly prefer evicting page cache over swapping anonymous pages
- `swappiness=60`: moderate balance (default Linux behavior)
- `swappiness=100`: treat anonymous pages and page cache equally

For data engineering workloads (Spark, Kafka, large Python processes):

```bash
# Prefer keeping process memory (anonymous pages) over page cache
# This prevents Spark executor memory from being swapped when page cache grows
echo 10 > /proc/sys/vm/swappiness

# Or set it permanently in /etc/sysctl.conf:
echo "vm.swappiness=10" >> /etc/sysctl.conf
sysctl -p
```

`vm.swappiness=10` means: only start swapping anonymous pages when there is very little clean page cache left to evict. For Spark executors where heap memory is critical, this is the standard recommendation — letting the page cache grow is acceptable (it's just a performance optimization), but swapping the heap is catastrophic (causes multi-second task pauses while pages are swapped in).

---

## 8. Memory-Mapped Files (mmap)

### 8.1 What Is mmap?

`mmap()` is a system call that maps a file (or a portion of a file) directly into a process's virtual address space. Instead of using `read()` to copy data from the page cache into a process buffer, `mmap()` makes the page cache pages themselves directly accessible at virtual addresses in the process's address space.

```
Without mmap (standard read):
  File on disk → page cache (physical page) → process buffer (another physical page)
  Two copies of the data in RAM: one in page cache, one in process heap
  CPU copies: page_cache → process_buffer (at each read() call)

With mmap:
  File on disk → page cache (physical page)
  Process's page table: virtual_addr → same physical page in page cache
  No copy! The process's virtual addresses point directly to the page cache pages.
  Only one copy of the data in RAM.
```

```python
import mmap
import os

# Memory-map a file for reading
with open("large_parquet_file.parquet", "rb") as f:
    # Map the entire file into virtual address space
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    
    # Read the first 4 bytes (Parquet magic number "PAR1")
    magic = mm[:4]
    
    # Access any byte range without explicit read() calls
    footer_offset = int.from_bytes(mm[-8:-4], 'little')
    footer = mm[-(8 + footer_offset):-8]
    
    mm.close()
# This is essentially how Parquet readers work under the hood
```

### 8.2 mmap Mechanics

```
mmap() system call:
  1. OS creates a Virtual Memory Area (VMA) in the process's address space
     VMA records: virtual_start, virtual_end, backing_file, file_offset, flags
  2. Page table entries for this VMA are initially NOT PRESENT
  3. Process accesses a virtual address in the VMA:
     a. CPU: TLB miss → page table walk
     b. Page table: present bit = 0 → PAGE FAULT
     c. OS page fault handler:
        - Checks if address is in a VMA backed by a file
        - If so: look up page cache for (file, page_offset)
        - Page cache hit: map page cache page into process's page table
        - Page cache miss: read 4 KB from disk into page cache, then map
        - Return to faulting instruction
     d. Process continues reading the data — it's now in the page table
```

The key insight: **`mmap()` uses demand paging.** No data is read from disk when `mmap()` is called — data is read only when the process actually accesses a specific virtual address. For Parquet files, reading only the footer (to get column statistics for predicate pushdown) requires reading only the footer's pages — not the entire file.

### 8.3 mmap in Data Engineering Systems

**Parquet reader (pyarrow):** PyArrow's Parquet reader uses memory mapping by default. The file is mapped into virtual address space; the reader accesses only the row group metadata (footer), evaluates column statistics for predicate pushdown, and then reads only the specific column chunks for rows that pass the filter. Non-accessed pages are never faulted in.

```python
import pyarrow.parquet as pq

# By default, PyArrow uses mmap for local files
table = pq.read_table(
    "events.parquet",
    memory_map=True,         # explicit mmap (default for local files)
    columns=["user_id", "amount"],          # only read these columns
    filters=[("amount", ">", 100.0)],       # predicate pushdown
)
# Only the footer + two column chunks are read from disk
# Other column chunks: never touched (page faults never fire for their VMA regions)
```

**Arrow IPC format:** Apache Arrow's inter-process communication (used in PySpark, Dask, Ray) writes data in a format designed for `mmap()`. Arrow IPC files can be memory-mapped and accessed at zero copy by any process — the same physical pages are shared among all readers.

**Spark shuffle files:** Spark's shuffle output is written as a sorted binary file per executor. The shuffle reader `mmap()`s the relevant byte range of the shuffle file directly into the reader's address space, allowing direct access without buffering.

**Kafka log segments:** Kafka's log segments are `mmap()`ed by the broker for index access (the `.index` files mapping message offsets to byte positions in the `.log` file). Consumer reads use `sendfile()` — a zero-copy path from the page cache directly to the socket.

### 8.4 mmap vs read(): When to Use Which

| Scenario | Prefer mmap | Prefer read() |
|---|---|---|
| Random access to large file | mmap — only touched pages are loaded | read() — must seek and read manually |
| Sequential scan of large file | read() — kernel readahead is optimized for sequential I/O | mmap — demand paging is less efficient for pure sequential access |
| Multiple processes sharing the same file | mmap — all processes share the same physical pages (page cache) | read() — each process has its own buffer (separate copies) |
| Very large files (> available RAM) | mmap — virtual address space is much larger than RAM | read() — fine, but must manage buffers manually |
| Frequent small random reads | mmap — amortizes per-read overhead via demand paging | read() — per-read syscall overhead adds up |
| Writing and reading same file | mmap with MAP_SHARED — writes immediately visible to other mappers | read()/write() with explicit synchronization |

**Readahead for mmap:** The OS can prefetch pages for `mmap()`ed regions using `madvise()`:

```python
import mmap
import ctypes

# Advise the OS about planned access patterns
MADV_SEQUENTIAL = 2   # expect sequential access (triggers aggressive readahead)
MADV_RANDOM = 1       # expect random access (disable readahead)
MADV_WILLNEED = 3     # prefetch this range now (async)
MADV_DONTNEED = 4     # evict this range from page cache (free up RAM)

# Example: prefetch Parquet column data before processing
with open("events.parquet", "rb") as f:
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    
    # Advise sequential access for column chunk (triggers readahead)
    # (In CPython, madvise is available as mmap.madvise in Python 3.8+)
    mm.madvise(mmap.MADV_SEQUENTIAL, col_chunk_offset, col_chunk_size)
    
    # Read the column chunk
    data = mm[col_chunk_offset:col_chunk_offset + col_chunk_size]
    
    # Release the pages after processing (free up page cache for next column)
    mm.madvise(mmap.MADV_DONTNEED, col_chunk_offset, col_chunk_size)
```

---

## 9. Deep Dive: Why Spark Tungsten Bypasses the JVM Heap

### 9.1 The JVM Heap and Garbage Collection

Spark runs on the JVM. The JVM heap is where all Java/Scala objects live — strings, arrays, DataFrames (as internal row format), hash tables for joins, sort buffers. The JVM allocates from the heap via `new` and frees memory via **garbage collection (GC)** — an automatic process that finds unreachable objects and reclaims their memory.

GC is the correct solution for most programs. For Spark's data processing workloads, it is a serious performance bottleneck:

**Stop-the-world (STW) pauses:** The G1GC and older CMS collectors periodically run a full mark-and-compact GC cycle. During this cycle, **all JVM threads stop** — all Spark tasks pause simultaneously, no data is processed. For large heaps (32+ GB), these pauses can last 2–30 seconds. A Spark job doing 10-second tasks that experiences a 5-second GC pause sees a 50% throughput reduction in that period.

**GC pressure from short-lived objects:** Spark creates enormous numbers of short-lived objects during data processing — each Row, each UnsafeRow pointer wrapper, each encoder/decoder chain object. These objects are allocated on the heap, quickly become unreachable, and must be GC'd. A Spark job processing 100 million rows might create 100 million short-lived objects, constantly pressuring the young-generation heap.

**Object overhead:** Every Java object has a 16-byte header (class pointer + mark word). A 4-byte integer stored as an `Integer` object uses 16 bytes of heap — a 4× overhead. A Spark DataFrame with 1 billion rows of integers stored as Java objects would use 16 GB instead of 4 GB.

**Heap fragmentation:** After many GC cycles, the heap becomes fragmented — many small free regions that cannot satisfy large allocations. G1GC's compaction addresses this but requires more frequent STW pauses.

### 9.2 Tungsten's Off-Heap Memory

Spark's Project Tungsten (introduced in Spark 1.4) addresses JVM GC overhead by **moving critical data structures off the JVM heap** — into memory managed directly by Spark using `sun.misc.Unsafe` (Java's unsafe memory API).

Off-heap memory in Spark is **not subject to JVM GC** — it is raw binary memory allocated directly from the OS via `malloc()`/`mmap()`, managed by Spark's own memory manager. Objects stored off-heap have no Java object headers, no GC overhead, no fragmentation from the JVM garbage collector.

```
JVM heap (managed by GC):
  - Spark operator state (DAG metadata, task context objects)
  - Small short-lived objects (iterator wrappers, etc.)
  - Any code that creates Java objects (user UDFs, Spark streaming state)

Off-heap (managed by Tungsten, outside JVM GC):
  - UnsafeRow binary format: fixed-width row data stored as packed bytes
    8-byte null bitmap + 8-byte field pointers + variable-length data region
  - Sort buffers: 128 MB pages of packed UnsafeRow data for Tungsten sort
  - Shuffle buffers: serialized partition data for ExternalSorter
  - Hash tables: for hash joins and aggregations (BytesToBytesMap)
  - Block manager: cached RDD/DataFrame data (with spark.memory.offHeap.enabled)
```

### 9.3 UnsafeRow: Compact Binary Format

The cornerstone of Tungsten is `UnsafeRow` — a binary format for rows that avoids Java object creation:

```
UnsafeRow binary layout for a row with 3 fields: (int, string, double)

  Bytes 0-7:  Null bitmap (one bit per field: 0=non-null, 1=null)
              For 3 fields: uses 1 byte, padded to 8 bytes
  
  Bytes 8-15: Field 0 (int): stored as 8-byte long (padded for alignment)
              value = 42
  
  Bytes 16-23: Field 1 (string): offset+length pointer into variable-length region
               high 32 bits: byte offset from start of UnsafeRow
               low 32 bits: byte length of string data
  
  Bytes 24-31: Field 2 (double): stored as 8-byte IEEE 754 double
               value = 3.14
  
  Bytes 32+:  Variable-length region: "hello" = 5 bytes + padding

Total: 32 fixed bytes + ceil(5/8)*8 = 8 bytes variable = 40 bytes

vs Java object representation:
  Row object header: 16 bytes
  int Integer(42): 16-byte object header + 4-byte value = 20 bytes → 24 bytes (alignment)
  String("hello"): 16-byte header + 4-byte count + 4-byte hash + char[5] object...
  double Double(3.14): 16 bytes
  Total: 16 + 24 + ~50 + 16 = 106 bytes (vs 40 bytes for UnsafeRow)
  → 2.7× more memory, all on GC-managed heap
```

UnsafeRow fields are accessed via fixed byte offsets — no object traversal, no pointer chasing. Sorting UnsafeRows requires only moving 8-byte sort keys (row pointers) — the data itself doesn't move until the final copy phase.

### 9.4 The Tungsten Sort (Sort-Merge Join and GroupBy)

The Tungsten sort algorithm (used in sort-merge joins and shuffle sort) illustrates how off-heap memory eliminates GC pressure:

```
Tungsten sort of N rows:

Phase 1: Ingestion into 128 MB sort pages
  - Incoming UnsafeRows are written to off-heap pages (128 MB each)
  - A compact pointer array is built: one 16-byte entry per row:
    - 8 bytes: sort key prefix (first 8 bytes of sort key for comparison)
    - 8 bytes: record pointer (page_number << 13 | page_offset → O(1) lookup)
  - The pointer array fits in a single 128 MB page for up to 8M rows

Phase 2: Sort the pointer array (not the data)
  - Sort the pointer array by sort key prefix
  - For ties in prefix: resolve by reading full key from off-heap page
  - No GC pressure: pointer array is off-heap (raw memory), Timsort in C/Unsafe

Phase 3: Spill to disk if memory exhausted
  - Write sorted pointer sequences + corresponding UnsafeRow data to disk
  - Multiple spill files → k-way merge on read

Why this beats heap-based sort:
  - No Java object creation during sort (GC pressure: ~zero)
  - Comparison operates on 8-byte prefixes (cache-friendly, no pointer chasing)
  - Spill writes are sequential (full 128 MB pages at once → optimal I/O)
  - Reuse of the same off-heap pages across tasks (no GC between tasks)
```

### 9.5 Configuring Off-Heap Memory in Spark

```python
# spark-submit configuration for off-heap memory
spark_conf = {
    # Enable off-heap storage (for caching DataFrames off-heap)
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "8g",    # off-heap storage pool size per executor
    
    # Executor heap for JVM (on-heap)
    "spark.executor.memory": "16g",       # JVM heap size
    "spark.executor.memoryOverhead": "4g", # off-heap JVM overhead (NIO buffers, etc.)
                                           # default: max(384 MB, executor_memory * 0.1)
    
    # Total memory per executor = executor.memory + executor.memoryOverhead
    # + off-heap.size (if enabled)
    # = 16g + 4g + 8g = 28g
    # Container request = 28g (YARN/Kubernetes must allocate this much per executor)
}
```

**The three memory regions per Spark executor:**

```
spark.executor.memory (16g) — JVM heap:
  ├── spark.memory.fraction (0.6) × 16g = 9.6g → Spark Memory Pool
  │   ├── spark.memory.storageFraction (0.5) × 9.6g = 4.8g → Storage Memory
  │   │   (RDD/DataFrame caching, broadcast variables)
  │   └── remaining 4.8g → Execution Memory
  │       (shuffle buffers, sort buffers, hash tables)
  └── 0.4 × 16g = 6.4g → User Memory
      (JVM overhead, Python UDF objects, Spark internal structures)

spark.executor.memoryOverhead (4g) — off-heap JVM:
  - JVM NIO direct buffers (network I/O)
  - JVM thread stacks, JVM metaspace
  - Python worker process memory (for PySpark)

spark.memory.offHeap.size (8g) — Tungsten off-heap:
  - UnsafeRow data for sort/join/agg
  - Off-heap cached DataFrames (if spark.memory.offHeap.enabled=true)
  - BytesToBytesMap hash tables
```

---

## 10. Mental Models

### 10.1 The Apartment Building Model for Virtual Memory

Physical RAM is a single apartment building with a fixed number of apartments (pages). Each tenant (process) believes they own the entire building — their apartment numbers (virtual addresses) go from 0 to the maximum. But the building manager (OS) knows the real room numbers (physical frames) and maintains a private translation table (page table) for each tenant.

When a tenant moves in (process starts), they're assigned a floor plan (virtual address space) but not actual rooms — they get rooms only when they actually open a door (access a page). If they never open door 1000 (never access page 1000), that room is never assigned (lazy allocation). Two tenants can share a room through a shared hallway (memory-mapped shared pages, like shared libraries).

The building manager also maintains a guest book (TLB) of recently used room assignments. Instead of consulting the full tenant registry (page table) every time, the manager checks the guest book first — much faster. But when a new tenant moves in (context switch), the guest book must be cleared (TLB flush) because room assignments are tenant-specific.

### 10.2 The Library Model for the Page Cache

The page cache is a public library. Books (file pages) are brought from storage (disk) when first requested. After they're returned, they stay on the library shelves (page cache) for a while in case someone wants to read them again. If you ask for a book that's on the shelf, you get it immediately (cache hit). If it's not on the shelf, the librarian fetches it from the warehouse (disk read) — slow.

When the library is full and a new book arrives, the librarian removes the least recently read book from the shelf to make room (LRU page eviction). The library always keeps popular books on the shelf; obscure books requested once are the first to be removed.

`vm.swappiness` decides: when the library is full, does the librarian first remove library books (page cache eviction) or ask tenants to temporarily store their personal belongings in a warehouse (swap)? Low swappiness means "remove library books first, don't touch tenants' stuff."

### 10.3 The Warehouse Receipt Model for mmap

Normally, reading a file is like ordering a product from a warehouse: the product (file data) is shipped to your address (copied to your process buffer). You have your own copy; the warehouse has its own copy. That's two copies of the product.

`mmap()` is like giving you a warehouse receipt — a document that says "the product is in shelf 7B, row 42" (a virtual address that maps directly to the page cache). You access the product in-place at the warehouse. There's only one copy. If you modify it, you're modifying the warehouse's copy (with MAP_SHARED). If you need an isolated copy, you use MAP_PRIVATE — the warehouse only makes you a physical copy of a shelf when you actually write to it (copy-on-write).

---

## 11. Failure Scenarios

### 11.1 Spark OOM from Page Cache Evicting Executor Memory

```
Scenario: Spark job on a machine with 256 GB RAM.
  Spark executors: 8 × 28 GB = 224 GB
  OS page cache: 30 GB (Parquet files)
  
  The executor uses Execution Memory for a large sort-merge join.
  The JVM GC pressure triggers a full GC that promotes many objects to old-gen.
  Old-gen heap grows beyond executor.memory (28g limit).
  
  JVM GC: "I need more heap space"
  OS memory allocator: "Evicting 4 GB of page cache to give JVM more native memory"
  JVM GC completes, but now the page cache is depleted.
  
  Next Spark task reads Parquet files → cache miss → disk I/O
  Disk reads are 20× slower → tasks take 20× longer
  More tasks in flight → more heap pressure → GC more frequently
  Spiral: GC pressure → page cache eviction → slower I/O → more data in memory →
          more GC → more page cache eviction → ...

Symptoms:
  - Spark tasks suddenly take much longer (minutes instead of seconds)
  - top shows high disk I/O and high kernel time (sy%)
  - /proc/meminfo shows Cached decreasing rapidly
  
Fix:
  1. Reduce spark.executor.memory (more room for page cache)
  2. Enable off-heap: spark.memory.offHeap.enabled=true reduces GC pressure
  3. Set vm.swappiness=10 (prefer evicting page cache over swapping)
  4. Use executor.memoryFraction=0.5 (less Spark memory, less GC)
```

### 11.2 Spark Direct Buffer OOM (Off-Heap, Not Heap OOM)

```
Error: java.lang.OutOfMemoryError: Direct buffer memory

This is NOT the JVM heap. It is JVM NIO direct buffers — off-heap memory
used by Java's NIO (Non-blocking I/O) for network operations.

Cause in Spark:
  Netty (Spark's network layer) allocates direct byte buffers for:
  - Shuffle data transfer (reading from remote executors)
  - RPC communication between driver and executors
  - Block transfer for broadcast variables
  
  Each executor's network thread pool allocates Netty direct buffers.
  If spark.executor.cores is high (many concurrent tasks, many network threads),
  direct buffer usage grows.
  
  JVM flag controlling max direct buffer size:
  -XX:MaxDirectMemorySize (default: equal to -Xmx heap size)
  
  Direct buffer memory is part of spark.executor.memoryOverhead.
  If memoryOverhead is too small for the workload, direct buffer OOM occurs.

Fix:
  # Increase memory overhead (adds more off-heap space for direct buffers)
  --conf spark.executor.memoryOverhead=4g  (increase from default 384MB)
  
  # Or set MaxDirectMemorySize explicitly:
  --conf "spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=8g"
  
  # Also consider reducing executor cores to reduce network concurrency
  --executor-cores 2  (fewer threads → fewer concurrent Netty buffers)
```

### 11.3 Swap Thrashing During Spark Shuffle

```
Scenario: Spark job with large shuffle (200 GB shuffle data).
  Machine: 32 GB RAM, 16 GB swap.
  Spark executor configured with 28 GB heap.
  
  During shuffle: executor reads data from remote executors AND
  writes output partitions. Memory demand spikes to 35 GB.
  
  OS: 35 GB demanded > 32 GB available → swap out 3 GB of executor pages
  Executor: page fault when accessing swapped pages → disk read (5-50 ms each)
  Executor: 100 million page faults → 5 GB × 5 ms average = 250,000 seconds of I/O
  (in practice: concurrent page reads + swap device bandwidth limits)
  
  Spark task timeout (spark.network.timeout default 120s) fires.
  Executor heartbeat fails → driver marks executor as dead.
  Job fails with: "Lost executor N: executor heartbeat timed out"
  
Detection:
  vmstat 1 during Spark run:
  # si  so
  # 0   0    ← before shuffle (OK)
  # 0   512  ← swapping out (trouble starting)
  # 256 512  ← swapping in AND out (thrashing)
  
Fix:
  Option 1: Increase memory — upgrade instance to 64 GB
  Option 2: Reduce executor heap — spark.executor.memory=24g (leaves 8 GB for OS)
  Option 3: Enable external shuffle service (reduces memory held per executor)
            spark.shuffle.service.enabled=true
  Option 4: Increase partition count — more, smaller tasks → less memory per task
            spark.sql.shuffle.partitions=2000 (from default 200)
```

### 11.4 TLB Thrashing from Huge Working Sets

```
Scenario: Spark sort-merge join with 50 GB of hash table data per executor.
  Hash table: random access pattern across 50 GB of off-heap memory.
  4 KB pages: 50 GB / 4 KB = 12.5 million pages.
  L2 TLB: 4096 entries → covers 16 MB of the 50 GB working set.
  TLB hit rate: ~16 MB / 50 GB = 0.03% → 99.97% TLB miss rate.
  Each miss: 400 cycles for page table walk.
  
  Result: hash table probes are 400 cycles each instead of 4 cycles.
          Join throughput is 100× worse than expected.
  
Detection:
  # Linux perf: measure TLB misses
  perf stat -e dTLB-load-misses,dTLB-loads -p <PID> sleep 10
  # dTLB-load-misses: 8,523,891,234
  # dTLB-loads:       8,524,000,000
  # → 99.99% miss rate ← catastrophic TLB thrashing
  
Fix:
  # Enable transparent huge pages for Spark (2 MB pages instead of 4 KB)
  echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
  
  # JVM flag to use huge pages for heap
  java -XX:+UseLargePages -Xmx40g ...
  
  # With 2 MB huge pages:
  # 50 GB / 2 MB = 25,600 pages
  # L2 TLB 4096 entries covers 8 GB of the 50 GB working set
  # TLB hit rate: ~16% (100× better than 4 KB page case)
```

### 11.5 OOM Kill from Memory Overcommitment

```
Scenario: Python multiprocessing.Pool with 16 workers on a 64 GB machine.
  Main process: 50 GB Pandas DataFrame.
  fork(): 16 worker processes each show 50 GB VSZ.
  With COW, physical RAM usage is still ~52 GB (parent + small child overhead).
  
  Workers begin processing: each writes to slices of the DataFrame.
  COW triggers: each written page is copied → physical RAM usage climbs.
  If each worker writes to 10% of the DataFrame: 16 × 10% × 50 GB = 80 GB copied.
  Total RAM needed: 50 GB original + 80 GB copies = 130 GB → exceeds 64 GB + swap.
  
  Linux OOM killer fires:
    oom_killer selects the highest-score process (highest RAM usage + lowest nice)
    Kills a worker process with SIGKILL.
    
  Symptoms:
    Sudden "Killed" message in Python (SIGKILL → no exception, just termination)
    dmesg: "Out of memory: Kill process NNNN (python) score NNN or sacrifice child"
    
Detection:
  dmesg | grep -i "oom\|kill"
  # [ 12345.678] Out of memory: Kill process 4567 (python) score 890 or sacrifice child
  
Fix:
  Option 1: Use memory_map=True in Pandas (or Arrow) to not copy
  Option 2: Pass indices/slices instead of data to workers
  Option 3: Use Pool.imap with a generator (never loads entire DataFrame at once)
  Option 4: Reduce COW by workers operating on read-only views
  Option 5: Use /dev/shm (shared memory) + multiprocessing.shared_memory
             for explicit zero-copy sharing between processes
```

---

## 12. Data Engineering Connections

### 12.1 PyArrow and Parquet: mmap in Practice

PyArrow's Parquet reader is one of the most sophisticated uses of mmap in data engineering. Understanding its memory behavior is essential for optimizing Python-based Parquet workflows.

```python
import pyarrow.parquet as pq
import pyarrow as pa
import os

# Reading with mmap=True (default for local files)
table = pq.read_table("large_file.parquet", memory_map=True)

# What actually happens:
# 1. open("large_file.parquet") → file descriptor
# 2. mmap(fd, 0, PROT_READ, MAP_PRIVATE) → VMA created in process address space
# 3. Read footer (last 8 bytes: footer_length + magic "PAR1")
#    → minor page fault: footer page loaded from disk into page cache
#    → page table entry: process_virtual_addr → page_cache_physical_frame
# 4. Seek to footer offset: read footer bytes
#    → minor/major page faults for each footer page
# 5. Parse footer: identify row groups, column chunks, min/max statistics
# 6. For each selected column chunk:
#    → access the mmap'd region for that chunk
#    → major page faults: each 4 KB page loaded from disk on demand
# 7. Decode column data (decompression, dictionary decoding)
# 8. Assemble as Arrow arrays (pointing to mmap'd memory for zero-copy where possible)

# Memory anatomy:
# RSS of Python process: only pages actually touched (not entire file)
# Page cache: populated with file pages as they are accessed
# Virtual address space: entire file mapped (even if only 10% read)

# Two parquet reads — second is much faster
import time

# Cold read (first time)
t0 = time.time()
pq.read_table("large_file.parquet", columns=["user_id", "amount"])
cold_time = time.time() - t0

# Warm read (page cache populated from first read)
t0 = time.time()
pq.read_table("large_file.parquet", columns=["user_id", "amount"])
warm_time = time.time() - t0

print(f"Cold: {cold_time:.2f}s, Warm: {warm_time:.2f}s, "
      f"Speedup: {cold_time/warm_time:.1f}x")
# Typical: cold=5.2s, warm=0.3s, Speedup=17x
```

### 12.2 Apache Arrow: Zero-Copy Data Sharing

Arrow's in-memory columnar format is designed to be compatible with virtual memory sharing. Arrow arrays are contiguous buffers — they can be `mmap()`ed or placed in shared memory and shared between processes without any copying.

```python
import pyarrow as pa
import pyarrow.plasma as plasma  # Plasma object store (used in Ray, old PySpark)

# Arrow buffer: contiguous memory region
arr = pa.array([1, 2, 3, 4, 5], type=pa.int64())
buf = arr.buffers()[1]   # the data buffer (40 bytes: 5 × 8-byte int64)
print(buf.address)       # virtual address of the buffer start
print(len(buf))          # 40 bytes

# Arrow IPC: write to shared memory, share with other processes
# (simplified: in practice, use multiprocessing.shared_memory or plasma)
shm = pa.memory_map("/dev/shm/arrow_data", "w")
writer = pa.ipc.new_file(shm, arr.schema)
writer.write_batch(pa.record_batch([arr], schema=pa.schema([('x', pa.int64())])))
writer.close()

# Another process reads from the same shared memory:
# reader = pa.memory_map("/dev/shm/arrow_data", "r")
# table = pa.ipc.open_file(reader).read_all()
# Zero copy: reader's virtual addresses point to the same physical pages
```

This is why PySpark DataFrames transferred between JVM and Python via Arrow are fast: the Arrow buffers are written to a byte channel; on the Python side, PyArrow constructs Arrow arrays pointing directly to those bytes — no additional copy. With `spark.sql.execution.arrow.pyspark.enabled=true`, entire partitions move from JVM → Python as Arrow IPC batches, and Python → JVM as Arrow IPC batches. The per-row serialization overhead of pickle is eliminated.

### 12.3 Kafka: sendfile() and Zero-Copy Consumer Reads

Kafka's consumer read path is a canonical example of using the OS's memory subsystem for zero-copy I/O:

```
Without zero-copy (naive):
  Kafka broker:
  1. read(log_file, kernel_buffer)    ← data: disk → page cache (if not cached)
  2. copy(kernel_buffer, user_buffer) ← data: page cache → application buffer (JVM heap)
  3. write(socket, user_buffer)       ← data: application buffer → socket kernel buffer
  4. TCP stack sends socket buffer    ← data: socket buffer → NIC DMA
  
  Total data copies: 4 (disk → page cache → app buffer → socket → NIC)

With sendfile() (Kafka's approach):
  Kafka broker:
  1. sendfile(log_file, socket_fd, offset, length)
     ← kernel reads from page cache (or disk if cold) directly to socket buffer
     ← DMA to NIC
  
  Total data copies: 2 (disk → page cache → NIC DMA)
  No user-space copy: Kafka broker's JVM heap never holds the log data
  
This is why Kafka can saturate a 10 Gbps NIC with a single thread.
The bottleneck is the NIC, not CPU or memory bandwidth.
```

```java
// Java sendfile() via FileChannel.transferTo
FileChannel logFileChannel = new RandomAccessFile(logFile, "r").getChannel();
SocketChannel socketChannel = consumerSocket.getChannel();

// Zero-copy transfer: OS sends directly from page cache to socket
// No data ever passes through the JVM heap
logFileChannel.transferTo(offset, count, socketChannel);
```

### 12.4 Spark Block Manager: Memory and Disk Tiering

Spark's block manager manages cached RDD/DataFrame blocks in a two-tier system: memory (JVM heap or off-heap) and disk. This tiering is implemented via the OS virtual memory system.

```
Block storage tiers (in preference order):

1. MEMORY_ONLY (default):
   - Block stored as Java objects in JVM heap
   - Fast access, GC pressure, limited by spark.executor.memory × storage_fraction
   - Eviction policy: LRU (least recently accessed block is dropped)

2. MEMORY_ONLY_SER (serialized, on-heap):
   - Block serialized to byte[] in JVM heap (Kryo or Java serialization)
   - ~2-5× less memory than MEMORY_ONLY (no Java object overhead)
   - Deserialization cost on each access

3. MEMORY_ONLY_2 (replicated):
   - MEMORY_ONLY but replicated to 2 executors for fault tolerance

4. MEMORY_AND_DISK:
   - Fits in memory: MEMORY_ONLY behavior
   - Overflow: writes excess blocks to disk (local file system)
   - Re-read from disk if evicted from memory

5. OFF_HEAP (requires spark.memory.offHeap.enabled=true):
   - Block stored in Tungsten off-heap memory
   - No GC pressure, faster for large blocks
   - Must be allocated from spark.memory.offHeap.size pool

// Parquet file in page cache + Spark MEMORY_ONLY cache = double caching:
// The page cache holds the compressed Parquet bytes
// MEMORY_ONLY holds the decompressed, decoded Java objects
// If memory is tight, prefer DISK_ONLY over MEMORY_ONLY for cold data
// and let the page cache handle "warm" Parquet file caching for free
```

---

## 13. Code Toolkit

### 13.1 `memory_inspector.py` — Virtual Memory and Page Cache Analysis

```python
"""
memory_inspector.py — Inspect process virtual memory and system page cache.

Provides:
  - Parse /proc/<pid>/smaps for per-region RSS, PSS, swap usage
  - Read system memory stats (page cache size, swap usage)
  - Monitor page faults for a running process
  - Demonstrate mmap for file access

Run directly for a profile of the current process's memory.
"""
from __future__ import annotations
import os
import sys
import mmap
import time
import struct
from pathlib import Path
from dataclasses import dataclass, field
from collections import defaultdict


# ─── System memory overview ──────────────────────────────────────────────────

@dataclass
class SystemMemory:
    total_kb: int
    available_kb: int
    cached_kb: int        # page cache
    buffers_kb: int       # kernel buffers
    swap_total_kb: int
    swap_used_kb: int
    dirty_kb: int         # dirty pages awaiting writeback

    @property
    def page_cache_gb(self) -> float:
        return (self.cached_kb + self.buffers_kb) / (1024 * 1024)

    @property
    def available_gb(self) -> float:
        return self.available_kb / (1024 * 1024)

    @property
    def swap_used_gb(self) -> float:
        return self.swap_used_kb / (1024 * 1024)

    @property
    def memory_pressure(self) -> str:
        if self.swap_used_kb > 0:
            return "HIGH (swapping)"
        if self.available_kb < 1 * 1024 * 1024:  # < 1 GB available
            return "MEDIUM (low available)"
        return "LOW (OK)"


def read_system_memory() -> SystemMemory | None:
    """Parse /proc/meminfo for system memory statistics."""
    if not sys.platform.startswith("linux"):
        return None
    try:
        fields = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            parts = line.split()
            if len(parts) >= 2:
                key = parts[0].rstrip(':')
                value = int(parts[1]) if parts[1].isdigit() else 0
                fields[key] = value
        return SystemMemory(
            total_kb=fields.get("MemTotal", 0),
            available_kb=fields.get("MemAvailable", 0),
            cached_kb=fields.get("Cached", 0),
            buffers_kb=fields.get("Buffers", 0),
            swap_total_kb=fields.get("SwapTotal", 0),
            swap_used_kb=fields.get("SwapTotal", 0) - fields.get("SwapFree", 0),
            dirty_kb=fields.get("Dirty", 0),
        )
    except Exception:
        return None


# ─── Per-process smaps ────────────────────────────────────────────────────────

@dataclass
class MemoryRegion:
    """One virtual memory region from /proc/<pid>/smaps."""
    start: int
    end: int
    flags: str          # rwxp etc.
    name: str           # [heap], [stack], /lib/libc.so.6, etc.
    rss_kb: int         # resident set size (physical RAM in use)
    pss_kb: int         # proportional set size (shared pages divided by sharers)
    shared_clean_kb: int
    shared_dirty_kb: int
    private_clean_kb: int
    private_dirty_kb: int
    swap_kb: int

    @property
    def size_kb(self) -> int:
        return (self.end - self.start) // 1024

    @property
    def category(self) -> str:
        if "[heap]" in self.name:
            return "heap"
        if "[stack" in self.name:
            return "stack"
        if self.name.startswith("/"):
            return "file-mapped"
        if not self.name:
            return "anonymous"
        return "special"


def parse_smaps(pid: int) -> list[MemoryRegion]:
    """Parse /proc/<pid>/smaps for detailed per-region memory info."""
    regions = []
    if not sys.platform.startswith("linux"):
        return regions

    smaps_path = Path(f"/proc/{pid}/smaps")
    if not smaps_path.exists():
        return regions

    current: dict = {}
    current_name = ""
    current_start = current_end = 0
    current_flags = ""

    def flush_region():
        if current_start and current.get("Rss"):
            regions.append(MemoryRegion(
                start=current_start, end=current_end,
                flags=current_flags, name=current_name,
                rss_kb=current.get("Rss", 0),
                pss_kb=current.get("Pss", 0),
                shared_clean_kb=current.get("Shared_Clean", 0),
                shared_dirty_kb=current.get("Shared_Dirty", 0),
                private_clean_kb=current.get("Private_Clean", 0),
                private_dirty_kb=current.get("Private_Dirty", 0),
                swap_kb=current.get("Swap", 0),
            ))
        current.clear()

    try:
        for line in smaps_path.read_text().splitlines():
            # Header line: "7f1234000000-7f1234010000 r--p 00000000 08:01 123456 /lib/libc.so"
            if '-' in line and line[0].isalnum() and not line.startswith(' '):
                flush_region()
                parts = line.split()
                addrs = parts[0].split('-')
                current_start = int(addrs[0], 16)
                current_end = int(addrs[1], 16)
                current_flags = parts[1] if len(parts) > 1 else ""
                current_name = parts[5] if len(parts) > 5 else ""
            else:
                # Key: value kB line
                if ':' in line:
                    key, _, val = line.partition(':')
                    try:
                        current[key.strip()] = int(val.strip().split()[0])
                    except (ValueError, IndexError):
                        pass
        flush_region()
    except PermissionError:
        pass

    return regions


def summarize_smaps(pid: int) -> dict:
    """Summarize memory regions by category."""
    regions = parse_smaps(pid)
    by_category: dict[str, dict] = defaultdict(lambda: {"rss_kb": 0, "pss_kb": 0, "swap_kb": 0, "count": 0})
    for r in regions:
        cat = by_category[r.category]
        cat["rss_kb"] += r.rss_kb
        cat["pss_kb"] += r.pss_kb
        cat["swap_kb"] += r.swap_kb
        cat["count"] += 1
    return dict(by_category)


# ─── Page fault monitoring ────────────────────────────────────────────────────

@dataclass
class PageFaultSnapshot:
    pid: int
    timestamp: float
    minor_faults: int
    major_faults: int


def read_page_faults(pid: int) -> tuple[int, int] | None:
    """Read cumulative page fault counts from /proc/<pid>/stat."""
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().split()
        # fields[9] = minflt, fields[11] = majflt
        return int(fields[9]), int(fields[11])
    except Exception:
        return None


def monitor_page_faults(pid: int, duration_s: float = 5.0, interval_s: float = 1.0) -> list[dict]:
    """Monitor page fault rates for a process over time."""
    if not sys.platform.startswith("linux"):
        return []

    snapshots = []
    prev_minor = prev_major = 0
    start = time.time()

    while time.time() - start < duration_s:
        result = read_page_faults(pid)
        if result is None:
            break
        minor, major = result
        if prev_minor:
            snapshots.append({
                "t": time.time() - start,
                "minflt_rate": (minor - prev_minor) / interval_s,
                "majflt_rate": (major - prev_major) / interval_s,
            })
        prev_minor, prev_major = minor, major
        time.sleep(interval_s)

    return snapshots


# ─── mmap demo ────────────────────────────────────────────────────────────────

def mmap_read_demo(file_path: str) -> dict:
    """
    Demonstrate mmap access vs read() for a file.
    Returns timing comparison and page fault counts.
    """
    if not os.path.exists(file_path):
        return {"error": f"File not found: {file_path}"}

    file_size = os.path.getsize(file_path)
    pid = os.getpid()

    # Method 1: standard read()
    faults_before = read_page_faults(pid)
    t0 = time.perf_counter()
    with open(file_path, "rb") as f:
        data_read = f.read()
    t_read = time.perf_counter() - t0
    faults_after_read = read_page_faults(pid)

    # Method 2: mmap access
    faults_before_mmap = read_page_faults(pid)
    t0 = time.perf_counter()
    with open(file_path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            data_mmap = bytes(mm[:])  # force access to all pages
    t_mmap = time.perf_counter() - t0
    faults_after_mmap = read_page_faults(pid)

    result = {
        "file_size_mb": file_size / (1024 * 1024),
        "read_time_s": t_read,
        "mmap_time_s": t_mmap,
        "read_throughput_MB_s": (file_size / 1024 / 1024) / t_read if t_read > 0 else 0,
        "mmap_throughput_MB_s": (file_size / 1024 / 1024) / t_mmap if t_mmap > 0 else 0,
    }
    if faults_before and faults_after_read and faults_before_mmap and faults_after_mmap:
        result["read_minor_faults"] = faults_after_read[0] - faults_before[0]
        result["mmap_minor_faults"] = faults_after_mmap[0] - faults_before_mmap[0]
    return result


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pid = os.getpid()
    print(f"=== Memory Inspector (PID={pid}) ===\n")

    # System memory
    sysmem = read_system_memory()
    if sysmem:
        print(f"System Memory:")
        print(f"  Available:    {sysmem.available_gb:.1f} GB")
        print(f"  Page cache:   {sysmem.page_cache_gb:.1f} GB  (Cached + Buffers)")
        print(f"  Swap used:    {sysmem.swap_used_gb:.1f} GB")
        print(f"  Dirty pages:  {sysmem.dirty_kb / 1024:.1f} MB  (awaiting writeback)")
        print(f"  Pressure:     {sysmem.memory_pressure}")

    # Per-process memory breakdown
    print(f"\nProcess Memory (PID={pid}):")
    summary = summarize_smaps(pid)
    if summary:
        for category, stats in sorted(summary.items(), key=lambda x: -x[1]['rss_kb']):
            print(f"  {category:<15} RSS={stats['rss_kb']//1024:>6} MB  "
                  f"PSS={stats['pss_kb']//1024:>6} MB  "
                  f"Swap={stats['swap_kb']//1024:>4} MB  "
                  f"({stats['count']} regions)")
    else:
        # Fallback: /proc/<pid>/status
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if any(k in line for k in ["VmRSS", "VmSize", "VmSwap", "VmPeak"]):
                    print(f"  {line.strip()}")
        except Exception:
            print("  (memory details not available)")

    # Current page faults
    faults = read_page_faults(pid)
    if faults:
        print(f"\nCumulative Page Faults:")
        print(f"  Minor (no I/O): {faults[0]:,}")
        print(f"  Major (disk I/O): {faults[1]:,}")
```

### 13.2 `page_cache_benchmark.py` — Cold vs Warm Read Comparison

```python
"""
page_cache_benchmark.py — Measures page cache impact on file read performance.

Creates a temp file, reads it cold (after dropping cache if possible),
then reads it warm (from page cache) and compares throughput.

Run directly to see the speedup from page cache.
"""
from __future__ import annotations
import os
import sys
import time
import tempfile
import subprocess
from pathlib import Path


def create_test_file(size_mb: int = 100) -> str:
    """Create a temporary file with random data."""
    path = tempfile.mktemp(suffix=".bin")
    chunk = os.urandom(1024 * 1024)   # 1 MB random data
    with open(path, "wb") as f:
        for _ in range(size_mb):
            f.write(chunk)
    return path


def drop_page_cache_if_possible() -> bool:
    """Attempt to drop page cache (requires root on Linux)."""
    if not sys.platform.startswith("linux"):
        return False
    try:
        os.system("sync")
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("1")
        return True
    except PermissionError:
        return False


def read_file_sequential(path: str, chunk_size: int = 4 * 1024 * 1024) -> tuple[float, float]:
    """Read file sequentially. Returns (bytes_read, elapsed_s)."""
    t0 = time.perf_counter()
    total = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            total += len(chunk)
    elapsed = time.perf_counter() - t0
    return total, elapsed


def read_file_mmap(path: str) -> tuple[float, float]:
    """Read file via mmap (force access all pages). Returns (bytes_read, elapsed_s)."""
    import mmap
    t0 = time.perf_counter()
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            # Force all pages to be accessed (madvise WILLNEED equivalent)
            total = sum(b for b in mm[::4096])  # access every 4 KB page
    elapsed = time.perf_counter() - t0
    return size, elapsed


if __name__ == "__main__":
    SIZE_MB = 200
    print(f"=== Page Cache Benchmark ({SIZE_MB} MB file) ===\n")

    print(f"Creating {SIZE_MB} MB test file...")
    test_file = create_test_file(SIZE_MB)
    print(f"  Created: {test_file}\n")

    # Cold read
    cache_dropped = drop_page_cache_if_possible()
    if not cache_dropped:
        print("  Note: running without root — cannot drop page cache.")
        print("  Cold read timing may be warm if file was recently accessed.\n")

    print("Cold read (first read — from disk or warm cache if no root):")
    bytes_read, t_cold = read_file_sequential(test_file)
    throughput_cold = (bytes_read / 1024 / 1024) / t_cold
    print(f"  {bytes_read/1024/1024:.0f} MB in {t_cold:.3f}s = {throughput_cold:.0f} MB/s")

    # Warm read
    print("\nWarm read (second read — from page cache):")
    bytes_read2, t_warm = read_file_sequential(test_file)
    throughput_warm = (bytes_read2 / 1024 / 1024) / t_warm
    print(f"  {bytes_read2/1024/1024:.0f} MB in {t_warm:.3f}s = {throughput_warm:.0f} MB/s")

    speedup = throughput_warm / throughput_cold if throughput_cold > 0 else 0
    print(f"\nPage cache speedup: {speedup:.1f}x")
    print(f"  Cold ({throughput_cold:.0f} MB/s) vs Warm ({throughput_warm:.0f} MB/s)")

    # mmap comparison
    print("\nmmap read (warm — using demand paging):")
    _, t_mmap = read_file_mmap(test_file)
    throughput_mmap = SIZE_MB / t_mmap
    print(f"  {SIZE_MB} MB in {t_mmap:.3f}s = {throughput_mmap:.0f} MB/s")

    # Cleanup
    os.unlink(test_file)
    print(f"\nTest file cleaned up.")
    print(f"\nExpected results:")
    print(f"  Cold read: 200-2000 MB/s (depends on NVMe SSD vs HDD vs warm cache)")
    print(f"  Warm read: 5000-20000 MB/s (limited by DRAM bandwidth, ~50 GB/s peak)")
    print(f"  mmap:      similar to warm read (both use page cache)")
```

---

## 14. Hands-On Labs

### Lab 1: Observe Page Cache with vmstat and free

**Goal:** Watch the page cache grow as large files are read, then shrink under memory pressure.

```bash
# lab1_page_cache_observation.sh

#!/bin/bash
echo "=== Initial memory state ==="
free -h
echo ""

echo "=== Reading a 2 GB file (creates page cache entries) ==="
# Create a 2 GB test file
dd if=/dev/urandom of=/tmp/testfile bs=1M count=2048 2>/dev/null
echo "File created. Memory state after write:"
free -h
echo ""

echo "Reading the file..."
dd if=/tmp/testfile of=/dev/null bs=4M
echo "Memory state after read:"
free -h
echo "(note: 'buff/cache' should have increased by ~2 GB)"
echo ""

echo "=== Dropping page cache (requires root) ==="
if [ "$EUID" -eq 0 ]; then
    sync && echo 1 > /proc/sys/vm/drop_caches
    echo "Page cache dropped. Memory state:"
    free -h
else
    echo "Not root — cannot drop page cache. Run with sudo to see cache drop."
fi

# Cleanup
rm /tmp/testfile
```

### Lab 2: Virtual Memory vs Physical Memory in a Python Process

**Goal:** Prove that fork + COW means child processes don't use as much physical RAM as VSZ suggests.

```python
# lab2_fork_cow_memory.py
"""
Demonstrates copy-on-write: forked processes share physical pages
until they write to them. RSS grows only when pages are actually modified.
"""
import os
import sys
import time
from pathlib import Path

def read_rss_kb(pid: int) -> int:
    """Read resident set size from /proc/<pid>/status."""
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except Exception:
        pass
    return 0

if __name__ == "__main__" and sys.platform.startswith("linux"):
    # Allocate a large array in the parent
    SIZE_MB = 100
    data = bytearray(SIZE_MB * 1024 * 1024)   # 100 MB
    # Touch all pages to ensure they're in physical RAM
    for i in range(0, len(data), 4096):
        data[i] = 1

    parent_rss = read_rss_kb(os.getpid())
    print(f"Parent RSS before fork: {parent_rss // 1024} MB")

    pid = os.fork()
    if pid == 0:
        # CHILD: just report RSS (COW → shares parent's pages, no copy yet)
        child_rss_before = read_rss_kb(os.getpid())
        print(f"Child RSS (immediately after fork, before write): {child_rss_before // 1024} MB")
        print(f"  (COW: shares parent's physical pages → low RSS)")

        # Now write to all pages → COW copies each page
        for i in range(0, len(data), 4096):
            data[i] = 2   # triggers COW for each 4 KB page

        child_rss_after = read_rss_kb(os.getpid())
        print(f"Child RSS (after writing all pages): {child_rss_after // 1024} MB")
        print(f"  (COW complete: child now has its own copies → high RSS)")
        os._exit(0)
    else:
        os.waitpid(pid, 0)
        parent_rss_after = read_rss_kb(os.getpid())
        print(f"Parent RSS after child completes: {parent_rss_after // 1024} MB")
        print(f"  (Parent's pages unchanged — still the original data)")
elif not sys.platform.startswith("linux"):
    print("This lab requires Linux. Run on a Linux machine.")
```

---

## 15. Interview Q&A

**Q1: What is virtual memory and what problems does it solve?**

Virtual memory is the OS abstraction that gives each process the illusion of having a private, large, contiguous address space — even though physical RAM is limited and shared among many processes. It solves three distinct problems simultaneously.

The first problem is isolation: without virtual memory, one process could read or corrupt another process's memory by accident or maliciously. With virtual memory, each process has its own page table mapping its virtual addresses to distinct physical frames. Process A's virtual address 0x7fff000 maps to physical frame 12345; Process B's virtual address 0x7fff000 maps to physical frame 67890 — completely different memory, enforced by hardware on every access.

The second problem is overcommitment: programs frequently allocate large memory regions they never fully use. Virtual memory enables lazy allocation — a `malloc(100GB)` call reserves virtual address space immediately but only maps physical pages when those addresses are actually accessed. A process that allocates 100 GB but uses only 10 GB at a time never needs 100 GB of physical RAM simultaneously.

The third problem is sharing: the `libc.so` standard library is used by every process on the system. Without sharing, every process would have its own copy of libc in physical RAM — hundreds of megabytes per process for a system running hundreds of processes. With virtual memory and shared pages, all processes map the same physical pages of libc code (mapped read-only, so changes in one process cannot affect others). This is also how copy-on-write works after `fork()`: parent and child share the same physical pages initially; the OS only copies a page when one of them writes to it.

**Q2: What is the page cache and how does it improve Spark performance?**

The OS page cache is a region of physical RAM used to cache file data. When a process reads a file, the OS reads the data from disk into page cache frames, then copies it to the process. The page cache frames remain populated after the read completes — if any process reads the same file data again, the OS returns the cached copy without disk I/O. The page cache is not a special memory pool; it is ordinary physical RAM tagged as "belonging to file X at offset Y" and shared among all processes on the system.

For Spark, the page cache provides two forms of acceleration. First, repeated reads of the same Parquet files are dramatically faster on a warm cache — a 100 GB Parquet dataset that takes 60 seconds to read from NVMe SSD takes 3–5 seconds from the page cache (DRAM bandwidth is ~50 GB/s vs NVMe at 2–7 GB/s). In a production environment where multiple Spark jobs read the same Bronze/Silver layer Parquet files, the page cache effectively provides free in-memory caching at the OS level without any Spark configuration.

Second, Spark's shuffle write and read path passes through the page cache. Shuffle files written by map tasks sit in the page cache; reduce tasks that run shortly after read from the page cache rather than disk. This is why co-locating shuffle data (using local disk for shuffles rather than remote HDFS) dramatically improves shuffle read performance.

The downside: the page cache competes for RAM with Spark's JVM heap and off-heap Tungsten buffers. On a machine with 128 GB RAM and Spark configured to use 100 GB, only 28 GB is available for the page cache. If the job reads 100 GB of Parquet files, the cache fills and data is repeatedly evicted, reducing the benefit. Reducing Spark's configured memory slightly (at the cost of some spill to disk) to give the page cache more room often results in better overall throughput.

**Q3: What is mmap and how does PyArrow's Parquet reader use it?**

`mmap()` is a system call that maps a file directly into a process's virtual address space, making the file's contents accessible at specific virtual addresses without explicit `read()` calls. The key difference from standard file I/O is that `mmap()` eliminates one data copy: standard `read()` copies data from the page cache into a user-space buffer (two copies of the data in RAM — one in page cache, one in the process buffer). With `mmap()`, the process's virtual addresses point directly to the page cache pages themselves. No additional copy is made — the page cache pages are the process's data.

PyArrow's Parquet reader uses `mmap()` as the default access mechanism for local files. When `read_table()` is called, PyArrow calls `mmap()` to map the entire Parquet file into virtual address space. No physical data is loaded yet — the mapping is just a virtual address range. The reader then reads the Parquet footer (the last few hundred bytes) to parse the file metadata: schema, row group offsets, column chunk locations, column statistics (min/max values per row group).

For queries with filter predicates, the reader evaluates the statistics for each row group against the filter. Row groups whose statistics exclude all rows (e.g., a row group where `max(amount) < 100` for a filter `amount > 200`) are skipped entirely — their virtual address range is never accessed, so no page faults fire and no disk I/O occurs for those row groups. For row groups that pass the statistics check, the reader accesses the specific column chunk virtual addresses, triggering page faults that load exactly those pages from disk. For a 1 TB Parquet dataset with a highly selective filter, PyArrow may read only 1–5 GB of physical data — 99%+ of the dataset is skipped at the page fault level.

**Q4: Why does Spark's Tungsten use off-heap memory, and what risks does that introduce?**

Tungsten moves critical data structures — sort buffers, hash tables for joins, shuffle buffers — off the JVM heap into raw memory managed by Spark directly using `sun.misc.Unsafe`. The motivation is eliminating three JVM GC overhead sources: GC pause time, object header overhead, and heap fragmentation.

Stop-the-world GC pauses are the most critical. During a full GC cycle, all JVM threads pause. For Spark tasks processing 100M rows in memory, a 10-second full GC pause means 10 seconds of zero progress across all tasks in the executor. With off-heap Tungsten buffers, the critical data (UnsafeRow binary data, sort pointers, hash table entries) is outside the GC's visibility — it cannot cause GC pressure and is not subject to GC pauses. The GC only sees the small Java objects that Spark creates for bookkeeping.

The risks are significant. First, memory leaks: off-heap memory is not GC'd. If Tungsten's memory manager has a bug (a buffer that is never freed after task completion), the leaked memory accumulates until the executor runs out of memory overhead and crashes. This has caused real production issues in older Spark versions. Second, memory corruption: `sun.misc.Unsafe` provides raw pointer access with no bounds checking. A bug in Spark's UnsafeRow addressing can write to an arbitrary memory address, corrupting data silently or causing a JVM crash with a `SIGSEGV`. This is dramatically harder to debug than a NullPointerException. Third, off-heap sizing: `spark.executor.memoryOverhead` must account for all off-heap usage (Netty buffers + Tungsten off-heap + JVM thread stacks + JVM metaspace). Undersizing this value causes `OutOfMemoryError: Direct buffer memory` or container OOM kills, which look unrelated to heap size.

**Q5: A Spark job on EC2 is slower the second day than the first, and you suspect page cache eviction. How do you diagnose and fix this?**

Day 1: the job is the only workload; the 128 GB machine has 80 GB of page cache filled with Parquet files. Parquet reads are fast (from page cache). Day 2: another workload (perhaps a memory-intensive ML training job) ran overnight and evicted the page cache. Parquet reads are slow (from disk).

Diagnosis involves two checks. First, check the current page cache size: `free -h` shows the `buff/cache` column. If it's much smaller than Day 1 (perhaps 10 GB vs 80 GB), the cache was evicted. Second, add instrumentation to the next Spark run: `perf stat -e cache-misses,cache-references` on the executor process, and monitor disk I/O with `iostat -x 1` during job execution. High read throughput from disk (not from RAM) on a file the job already read on Day 1 confirms cache eviction.

Three fixes, in increasing invasiveness. First, machine isolation: dedicate a machine to Spark ETL and prevent other workloads from running on it. The page cache is then preserved between Spark runs. Second, explicit Spark caching: cache the DataFrame in MEMORY_ONLY or MEMORY_AND_DISK — Spark's block manager explicitly manages this data, not the OS page cache, and it survives across jobs within the same SparkSession. Third, HDFS/Alluxio tiering: store frequently-read datasets in Alluxio (an in-memory distributed storage layer). Alluxio provides explicit memory-resident storage that is independent of the OS page cache and survives machine reboots.

**Q6: What is transparent huge pages (THP) and what is the trade-off for Spark?**

Transparent Huge Pages is a Linux kernel feature that automatically promotes groups of aligned, contiguous 4 KB pages into single 2 MB pages. The benefit is TLB efficiency: one TLB entry covers 2 MB instead of 4 KB. For a Spark executor with a 32 GB JVM heap, standard 4 KB pages create 8 million page table entries — far more than any TLB can hold (L2 TLB: ~4096 entries covers 16 MB). With 2 MB huge pages, the same 32 GB heap requires only 16,384 page table entries — the L2 TLB can cover 8 GB (half the heap), and TLB miss rates drop dramatically for random-access operations like hash joins.

The trade-off is latency spikes from huge page promotion and defragmentation. When the kernel decides to convert a 2 MB region of 4 KB pages into one huge page (promotion), it must allocate a contiguous 2 MB physical region, copy all 512 pages into it, and update all the page table entries. This takes several milliseconds and happens without notice. With THP `defrag=always`, the kernel continuously tries to defragment physical memory to create contiguous 2 MB regions for promotion — this can cause spikes in allocation latency of hundreds of milliseconds, which appear as unexplained pauses in Spark task execution.

The recommended configuration for Spark: `THP enabled=madvise` (only promote pages that explicitly call `madvise(MADV_HUGEPAGE)`) and `defrag=defer+madvise` (defer defragmentation until explicitly requested). For Java heap memory, pass `-XX:+UseLargePages` to the JVM — the JVM explicitly requests huge pages via `madvise` for its heap allocation, getting the TLB benefit with controlled promotion timing. Other process memory (stack, non-heap objects) remains at 4 KB and is unaffected by THP.

---

## 16. Cross-Question Chain

**Q1 [Interviewer]: Walk me through what happens when a Python process reads a 4 KB chunk from a Parquet file for the first time.**

The Python process calls `f.read(4096)` on an open file descriptor. The Python runtime calls the `read()` system call, which transitions the CPU from user mode to kernel mode. In the kernel, the VFS (Virtual File System) layer identifies the file's inode, computes the file offset, and checks the page cache for the corresponding page (inode + page_index). This is the first access, so it is a cache miss. The kernel schedules an I/O request to the storage device — reading 4 KB (one page) from the disk sector corresponding to this file offset. While waiting for I/O, the kernel puts the process in SLEEPING state (blocking I/O) and the scheduler runs another thread. When the I/O completes (~100 µs for NVMe, ~5 ms for spinning disk), the kernel places the page into the page cache, copies the 4 KB from the page cache frame into the process's buffer, marks the page's "accessed" bit, and returns. The process wakes up and `read()` returns 4096.

**Q2 [Interviewer]: You said the kernel copies from page cache to process buffer. With mmap, you said there's no copy. Explain exactly what "no copy" means in terms of physical pages.**

With standard `read()`: one physical page frame in the page cache holds the file data, and another physical page frame in the process's heap holds the copied data. Two frames used for the same data — doubling the physical RAM footprint.

With `mmap()`: the OS creates a page table entry in the process's page table that maps the process's virtual address directly to the same physical page frame that is in the page cache. There is only one physical page frame holding the data. The "copy" that `read()` performs — `memcpy` from page_cache_frame to process_buffer_frame — does not happen. The process's virtual address IS the page cache. When the process reads `mm[offset]`, the MMU translates the virtual address through the page table to the page cache frame and reads it directly. Absolutely zero data movement in RAM.

The caveat: if the process writes to an `mmap(MAP_PRIVATE)` region, the OS performs copy-on-write — creates a new private physical page for the process before allowing the write, so the page cache frame (which other processes might be sharing) is not modified.

**Q3 [Interviewer]: If mmap has no copy, why does PyArrow sometimes still show high memory usage (RSS) for large Parquet reads?**

RSS measures which physical pages are currently mapped into the process's page table and resident in RAM. When PyArrow accesses a Parquet column chunk via mmap, those page cache pages become part of the process's RSS — even though the same physical pages are also counted in the OS page cache statistics. RSS counts the page cache pages that the process's mmap has mapped in. So PyArrow's RSS grows proportionally to the amount of Parquet data it has accessed, even though no additional physical memory was allocated — the pages were already in the page cache.

Additionally, PyArrow must deserialize and decode the raw Parquet column data (RLE/bitpacking, dictionary encoding, Snappy/Zstd decompression). The decoded output — the actual Arrow arrays that user code works with — is allocated on the Python heap or in a separate memory allocation. This decoded data is separate from the mmap region and does consume new physical memory. For a 1 GB Parquet column chunk that decompresses to 4 GB of actual data, the process's RSS grows by ~5 GB: 1 GB for the mmap'd compressed data (page cache pages) + 4 GB for the decoded Arrow arrays.

**Q4 [Interviewer]: How does Spark's Tungsten avoid JVM GC overhead during a sort-merge join?**

A sort-merge join in Spark requires sorting both input sides by the join key, then merging the sorted streams. Without Tungsten, sorting would involve: allocating Java Row objects for each input row (16 bytes object header + fields), building a Java array of Row references, calling Arrays.sort() which compares Java objects, creating new Row objects for the output — all on the JVM heap, all subject to GC. For 100 million rows, this creates 100 million Row objects → massive GC pressure.

Tungsten replaces this with off-heap binary operations. Input rows are encoded as UnsafeRow binary format and written into off-heap 128 MB memory pages. A compact pointer array is built: for each row, an 8-byte sort key prefix (extracted from the UnsafeRow) plus an 8-byte pointer (page_number + page_offset). This pointer array is sorted in place using Timsort on the 8-byte keys — no Java object comparisons, no GC pressure. The sort operates entirely on the pointer array; the actual row data in the 128 MB pages doesn't move.

The merge phase reads from the sorted pointer arrays of both sides, following pointers into the off-heap pages to compare keys for merge decisions. Match rows are emitted directly from the off-heap format. The entire join — ingestion, sort, merge — produces virtually no JVM heap allocations. GC pause frequency drops to near-zero for the execution memory phase. The only heap allocations are the small Java driver objects (iterators, task context), which are trivially GC'd without stop-the-world pauses.

**Q5 [Interviewer]: A production Spark job is failing with "Direct buffer memory" OOM, but the JVM heap is only 60% utilized. What is happening and how do you fix it?**

Direct buffer memory is JVM memory outside the heap, allocated via `ByteBuffer.allocateDirect()` or through Netty's direct buffer pool. It is controlled by the JVM flag `-XX:MaxDirectMemorySize`, which defaults to the heap size (`-Xmx`). When the total direct memory allocated exceeds this limit, `OutOfMemoryError: Direct buffer memory` is thrown.

In Spark, direct memory is primarily consumed by Netty — the networking library that handles shuffle data transfer, RPC between driver and executors, and block transfer for broadcast variables. Each network thread maintains a pool of direct buffers for I/O. With a high `spark.executor.cores` (many tasks → many network threads) and active shuffle (many concurrent data transfers), Netty's direct buffer allocation grows.

The accounting: `spark.executor.memoryOverhead` (JVM memory outside the heap) must accommodate Netty's direct buffers, JVM metaspace, JVM thread stacks, and any other off-heap usage. If `memoryOverhead` is set to the minimum (384 MB default for small executors) but the executor has 8 cores with active shuffle, Netty may need 1–4 GB of direct buffers.

Fix: increase `spark.executor.memoryOverhead` to 4–8 GB for shuffle-heavy jobs with many cores. Alternatively, explicitly cap Netty's buffer pool: `spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=4g`. For jobs with very high parallelism, also consider reducing `spark.executor.cores` (fewer concurrent tasks → fewer Netty threads → less direct buffer usage). Monitor the fix: `jcmd <PID> VM.native_memory` reports direct memory usage in detail.

**Q6 [Interviewer]: Design an efficient file I/O layer for a column store (like Parquet) that minimizes both latency and memory usage.**

The design must balance three constraints: fast access to specific column ranges (random access, not full file reads), minimal physical RAM footprint (shared page cache, no double-copying), and low I/O latency (readahead for sequential column access, page fault avoidance for metadata).

The file format should be structured for mmap compatibility: fixed-size blocks (128 MB row groups), columns stored contiguously within each row group, and a compact footer with byte offsets for every column chunk. This allows a reader to mmap the entire file and access any column chunk at a specific offset without seeking or buffering.

The read path would use `mmap(PROT_READ, MAP_SHARED)` — one physical copy in page cache, shared among all readers of the same file (multiple Spark tasks, multiple Python processes reading the same data lake table). For the footer: read it once eagerly on file open via a `pread()` or direct mmap access — the footer is small (< 1 MB) and needed immediately. For column chunks: use `madvise(MADV_WILLNEED, chunk_offset, chunk_size)` to trigger asynchronous prefetch of needed column chunks before the reader reaches them. For skipped column chunks (predicate pushdown eliminates them): use `madvise(MADV_DONTNEED, skip_offset, skip_size)` to advise the OS to evict those pages — freeing page cache for pages the reader will actually need.

After processing a column chunk, `madvise(MADV_DONTNEED)` releases its pages. This "streaming" page cache usage — proactively advising access patterns and releasing unneeded pages — keeps the page cache footprint proportional to the working set, not the entire file. Combined with arrow-compatible column layout (zero-copy deserialization for uncompressed numeric columns), this design achieves near-DRAM-bandwidth throughput with minimal RAM overhead.

---

## 17. Common Misconceptions

**"RSS shows how much memory a process is actually using."**
RSS shows how many physical page frames are currently mapped by the process. It double-counts shared pages (the same page cache page counts in the RSS of every process that has mmapped it), and it excludes swapped-out pages (which are still "owned" by the process but not in RAM). PSS (proportional set size from `/proc/<pid>/smaps`) is more accurate: it divides shared pages by the number of processes sharing them. `smem` or `cat /proc/<pid>/smaps_rollup` reports PSS.

**"mmap is always faster than read()."**
For sequential full-file reads, `read()` with a large buffer (4–16 MB chunks) is often faster than mmap because the kernel's readahead is more aggressive for sequential `read()` patterns. `mmap()` reads in 4 KB page chunks triggered by page faults — each page fault has overhead (~1–10 µs), and sequential access generates many page faults. For random access to large files (Parquet column chunks at arbitrary offsets), mmap is faster because there is no explicit seek + read overhead and the page cache is shared.

**"Spark's off-heap memory means the executor doesn't count toward the node's memory."**
Off-heap memory (`spark.memory.offHeap.size`) and memory overhead (`spark.executor.memoryOverhead`) are allocated from the same physical RAM as heap memory. "Off-heap" means "outside the JVM heap" — not outside physical RAM. YARN and Kubernetes compute container memory requirements as heap + overhead + off-heap, and the container is OOM-killed if the total exceeds the container's memory limit. Setting `spark.memory.offHeap.size=8g` without increasing the YARN/Kubernetes container memory limit will cause OOM container kills.

**"The page cache persists across reboots."**
The page cache is in-memory only — it is lost on reboot. Files must be read from disk again after a reboot, making the first post-reboot Spark run on a dataset significantly slower than subsequent runs. Solutions for persistent caching: Alluxio (in-memory distributed storage with replication), HDFS with configured short-circuit reads (reads from local disk bypass network), or simply accepting that the first run is a cache warmup.

**"swap=0 means the system will never OOM-kill."**
Disabling swap (`/etc/fstab` no swap line, or `swapoff -a`) means there is nowhere to put evicted dirty anonymous pages when RAM is exhausted. Without swap, the OOM killer fires sooner (as soon as clean page cache eviction is insufficient to meet demand). With swap, the system can survive short memory spikes by swapping to disk (slowly). For Spark executors where swap causes multi-second pauses (GC waiting for swapped pages), the recommendation is 1–4 GB of swap (enough to survive brief spikes without thrashing) and `vm.swappiness=10` (avoid swapping until necessary).

---

## 18. Performance Reference Card

| Operation | Approximate Latency | Notes |
|---|---|---|
| L1 TLB hit | ~1 cycle | Translation in TLB |
| L2 TLB hit | ~5 cycles | Second-level TLB |
| TLB miss + page walk | ~400–1000 cycles | 4 memory accesses, each L3/RAM |
| Minor page fault | ~1–10 µs | Demand allocation or COW |
| Major page fault (NVMe) | ~100–500 µs | Read 4 KB from NVMe SSD |
| Major page fault (HDD) | ~5–20 ms | Read 4 KB from spinning disk |
| Page cache read (hit) | ~50 ns (DRAM bandwidth) | ~50 GB/s ÷ 4 KB = 80 ns per page |
| read() syscall overhead | ~1–3 µs | Syscall + copy |
| mmap access (warm) | ~50 ns | Direct DRAM access via TLB |
| mmap access (cold page) | = major page fault cost | Loads 4 KB from disk |
| TLB flush (process switch) | ~200–500 ns | Reload TLB after context switch |
| Context switch overhead (TLB) | ~1,000–10,000 ns | TLB warm-up after flush |
| Huge page TLB coverage | 2 MB per entry | vs 4 KB for standard pages (512× better) |

**Page cache effectiveness for Parquet:**

| Scenario | Read speed |
|---|---|
| Cold disk read (NVMe) | 2–7 GB/s |
| Cold disk read (S3/object storage) | 0.5–2 GB/s |
| Warm page cache (DRAM) | 20–50 GB/s |
| S3 with Alluxio local cache | 10–30 GB/s (Alluxio DRAM tier) |

---

## 19. Connections to Other Modules

**CSF-OS-101 M01 (Processes and Threads):** M01 describes process address space layout (stack, heap, text, mmap regions). This module explains the mechanism behind that layout: virtual address spaces, page tables, and the MMU. Fork + COW, shared library pages, and process isolation — all described abstractly in M01 — are mechanistically explained here.

**CSF-OS-101 M02 (Process Scheduling):** TLB flush on context switch (discussed in M02 as the reason process switches are more expensive than thread switches) is explained mechanically here. The PCID optimization that reduces TLB flush cost is covered in Section 5.2.

**CSF-OS-101 M04 (File I/O and System Calls):** File I/O (Section M04) builds directly on the page cache (Section M03). The `read()`, `write()`, `mmap()`, `fsync()` system calls interact with the page cache as described here. Section M04 covers the VFS layer, I/O scheduling, and buffered vs direct I/O.

**CSF-ALG-101 M03 (Trees):** The page table is a 4-level tree structure (the hardware radix tree). Each level is a B-tree-like node — an array of 512 entries. The TLB is a cache over this tree, analogous to how a database index cache works.

**DCS-SPK-101 (Spark Architecture):** Spark's Tungsten off-heap memory, `spark.executor.memoryOverhead`, `spark.memory.offHeap.*` configurations, and the behavior of Spark under GC pressure — all require the virtual memory foundation from this module.

**DE-PDA-101 (Pandas and Arrow):** PyArrow's mmap-based Parquet reader, Arrow zero-copy IPC, and Pandas memory usage (backed by NumPy arrays, which are contiguous memory regions similar to off-heap buffers) — all connect to the mmap and virtual memory concepts here.

---

## 20. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is a virtual address space? | The range of addresses a process believes it owns (0 to 2⁴⁸ on x86-64). Virtual addresses are mapped to physical frames by the page table. Each process has its own page table → isolation. |
| 2 | What is a page? | The unit of virtual memory management. Standard size: 4 KB (x86-64). Each page has a page table entry mapping it to a physical frame. Huge pages: 2 MB or 1 GB. |
| 3 | What is the TLB? | Translation Lookaside Buffer — a hardware cache of recent virtual→physical address translations. TLB hit: ~1 cycle. TLB miss: ~400–1000 cycles (page table walk). Critical for performance. |
| 4 | Why is the TLB flushed on process context switch? | Different processes have different page table mappings for the same virtual addresses. Stale TLB entries from the old process would cause incorrect (and potentially dangerous) translations for the new process. |
| 5 | What is a minor vs major page fault? | Minor: page is in memory (demand alloc, COW, shared mapping) — no disk I/O, ~1–10 µs. Major: page must be read from disk — disk I/O required, ~100 µs–20 ms. |
| 6 | What is the OS page cache? | Physical RAM used to cache file data. First read: disk → page cache → process buffer. Second read: page cache → process buffer (no disk I/O). Persists until evicted by memory pressure. |
| 7 | What does mmap() do? | Maps a file directly into process virtual address space. Process's virtual addresses point to page cache frames directly — no user-space copy. Zero-copy read: one physical page, many virtual readers. |
| 8 | When does mmap trigger disk I/O? | On the first access to each mapped page (demand paging). The access triggers a page fault; the OS reads the 4 KB page from disk into the page cache and maps it into the process's page table. |
| 9 | What is copy-on-write (COW)? | After fork(), parent and child share physical pages (read-only). On write: OS copies the written page to a new physical frame for the writing process. Unwritten pages are never copied. |
| 10 | What is vm.swappiness? | Linux kernel parameter (0–100) controlling preference for swapping anonymous pages vs evicting page cache. Low value (10): strongly prefer evicting page cache. High value: balance both. Default: 60. |
| 11 | Why does Spark use off-heap memory (Tungsten)? | JVM GC pauses, object header overhead, and heap fragmentation degrade Spark performance. Tungsten stores UnsafeRow data in raw binary format off-heap — outside GC visibility, no headers, no fragmentation. |
| 12 | What is UnsafeRow? | Spark's binary row format for off-heap storage. Fixed-length null bitmap + field data (8-byte aligned) + variable-length region. No Java object headers. Compact, fast to compare, pointer-addressed. |
| 13 | What is spark.executor.memoryOverhead? | Off-heap JVM memory per executor (not the JVM heap): Netty direct buffers, JVM metaspace, thread stacks. Must be large enough to avoid "Direct buffer memory" OOM. Default: max(384MB, heap×10%). |
| 14 | What are transparent huge pages (THP)? | Linux feature that automatically promotes 4 KB pages to 2 MB pages for large contiguous regions. Reduces TLB miss rate dramatically for large heaps. Risk: latency spikes during promotion/defragmentation. |
| 15 | What is RSS vs PSS? | RSS (Resident Set Size): physical pages mapped by the process, counting shared pages fully per process. PSS (Proportional Set Size): shared pages divided by number of sharers. PSS is more accurate for true memory consumption. |
| 16 | How does Kafka achieve zero-copy reads? | Uses sendfile(): transfers data from page cache directly to socket buffer via DMA. JVM heap never holds the data. Eliminates the user-space copy step. Enables near-NIC-saturation throughput with one thread. |
| 17 | What causes "OOM: Direct buffer memory" in Spark? | Total JVM direct memory (Netty buffers + others) exceeds MaxDirectMemorySize. Fix: increase spark.executor.memoryOverhead or explicitly set -XX:MaxDirectMemorySize. |
| 18 | What is demand paging? | Pages are only brought into physical RAM when first accessed (on page fault), not at allocation time. Enables overcommitment (large virtual allocations with small physical footprint) and fast fork() via COW. |
| 19 | What does `echo 3 > /proc/sys/vm/drop_caches` do? | Drops the OS page cache, dentries, and inode caches from physical RAM. Used to simulate cold-start conditions for benchmarking. Requires root. Does not affect dirty pages (data already on disk only). |
| 20 | What is the connection between the page cache and Parquet predicate pushdown? | PyArrow reads Parquet via mmap. It reads only the footer and column statistics (small, few pages). Rows that don't match filters have their column chunks skipped — their virtual address ranges are never accessed, so no page faults fire and no disk I/O occurs for those row groups. |

---

## 21. Module Summary

**Virtual memory** gives each process the illusion of a private address space. The hardware MMU translates virtual addresses to physical frames via the **page table** — a four-level radix tree on x86-64. The **TLB** caches recent translations (hit: ~1 cycle, miss: ~1000 cycles). Process context switches flush the TLB; within-process thread switches do not. **Huge pages** (2 MB) reduce TLB miss rates by 512× for large working sets — critical for Spark executors with 32+ GB heaps.

**Page faults** are hardware exceptions when a virtual access cannot be served without OS intervention. Minor faults (demand allocation, copy-on-write): ~1–10 µs. Major faults (disk read required): ~100 µs–20 ms. **Copy-on-write** after `fork()` shares pages between parent and child until one writes — fork of a 50 GB process is fast; actual memory consumption grows only as pages are dirtied.

The **OS page cache** caches file data in physical RAM. First reads hit disk; subsequent reads hit the page cache at DRAM speed (20–50× faster). The page cache competes for RAM with process heaps; `vm.swappiness=10` tells the OS to evict page cache before swapping process memory.

**`mmap()`** maps files directly into virtual address space — process addresses point to page cache frames. Zero additional copy. PyArrow's Parquet reader uses mmap with demand paging: only column chunks actually accessed trigger page faults, enabling file-format-level predicate pushdown at the OS level.

**Spark Tungsten** moves UnsafeRow binary data, sort buffers, and hash tables off the JVM heap into raw memory outside GC control. This eliminates stop-the-world GC pauses on execution memory, removes Java object header overhead, and prevents heap fragmentation. The cost: off-heap memory is not GC-managed (leaks are permanent until process exit) and is unsafe (pointer bugs cause data corruption or crashes). `spark.executor.memoryOverhead` must account for Netty direct buffers + Tungsten off-heap + JVM overhead or the executor will OOM-kill.

---

**CSF-OS-101: 3 of 5 complete.**  
**Next: CSF-OS-101 M04 — File I/O and System Calls**  
*(VFS, read/write/fsync, O_DIRECT, epoll, io_uring — and how Kafka, Parquet, and Spark shuffle use these primitives)*
