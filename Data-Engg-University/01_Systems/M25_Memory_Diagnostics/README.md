# SYS-LNX-101 M04: Memory Diagnostics

**Course:** SYS-LNX-101 — Linux for Data Engineers  
**Module:** 04 of 05  
**Filesystem position:** 01_Systems/M25_Memory_Diagnostics  
**Prerequisites:** SYS-LNX-101 M01 (Linux File System), SYS-LNX-101 M02 (Process Management), SYS-LNX-101 M03 (Storage and I/O)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: The Linux Memory Model](#4-core-theory-the-linux-memory-model)
5. [free — Memory Overview](#5-free--memory-overview)
6. [/proc/meminfo — The Full Picture](#6-procmeminfo--the-full-picture)
7. [vmstat — Memory and System Activity](#7-vmstat--memory-and-system-activity)
8. [The OOM Killer](#8-the-oom-killer)
9. [Per-Process Memory: /proc and smaps](#9-per-process-memory-proc-and-smaps)
10. [Swap Configuration and Behavior](#10-swap-configuration-and-behavior)
11. [Memory Pressure Detection](#11-memory-pressure-detection)
12. [Mental Models](#12-mental-models)
13. [Failure Scenarios](#13-failure-scenarios)
14. [Data Engineering Connections](#14-data-engineering-connections)
15. [Code Toolkit](#15-code-toolkit)
16. [Hands-On Labs](#16-hands-on-labs)
17. [Interview Q&A](#17-interview-qa)
18. [Cross-Question Chain](#18-cross-question-chain)
19. [Common Misconceptions](#19-common-misconceptions)
20. [Flashcards](#20-flashcards)
21. [Module Summary](#21-module-summary)

---

## 1. The Problem This Module Solves

A Spark executor dies mid-job with no error message visible in the Spark UI. The application log shows `ExecutorLostFailure` and the executor simply disappears. On the server, `dmesg | grep -i kill` reveals: `Out of memory: Kill process 8234 (java) score 723 or sacrifice child`. The OOM killer ran. Understanding why, and knowing how to prevent it, requires knowing how Linux accounts for memory — which is not intuitive.

`free -h` shows `MemAvailable: 12 GB` but the JVM still gets OOM-killed. How? Because `MemAvailable` is the kernel's estimate of how much RAM applications can use *without pushing anything to swap*, but a JVM that requested 32 GB of virtual address space with `-Xmx32g` has already committed that memory in the kernel's accounting even if only 8 GB of physical pages are resident. When the JVM tries to touch a new page, the kernel must allocate a physical page. If there are no physical pages and no reclaimable pages, the OOM killer fires — regardless of what `free` showed moments before.

A Kafka broker's page cache is repeatedly evicted. The JVM heap is only 4 GB on a 64 GB server, so there should be 60 GB for page cache — but some background process is allocating and releasing large buffers, constantly displacing Kafka's cached log segments. Consumer reads become disk reads, latency spikes, and lag grows. Diagnosing this requires `/proc/meminfo`, specifically the `Active(file)` and `Inactive(file)` lines that track page cache, and `vmstat -s` to see the eviction rate.

This module covers how Linux manages memory, how to read every important diagnostic tool, and how to configure memory behavior for Kafka, Spark, and HDFS workloads.

---

## 2. How to Use This Module

**For incident response:** Start with Section 5 (`free`) and Section 6 (`/proc/meminfo`) for a quick memory state snapshot. Go to Section 8 (OOM killer) if a process was killed. Use Section 7 (`vmstat`) to check if the system is currently under pressure (active swapping).

**For tuning:** Section 10 (swap), Section 11 (memory pressure detection), and the Data Engineering Connections (Section 14) contain the production-relevant sysctl settings and per-process OOM score adjustments.

**For interview prep:** Sections 4, 8, 17, and 18. The Linux memory model, OOM killer behavior, and the RSS/VSZ distinction are high-frequency staff-level interview topics.

---

## 3. Prerequisites Check

- **Processes and Threads (CSF-OS-101 M01):** Process virtual address space (stack, heap, code, mmap regions) from M01 is the foundation of this module's VSZ/RSS discussion.
- **File I/O and System Calls (CSF-OS-101 M04):** The page cache concept from M04 — data in RAM waiting to be written to disk — is central to understanding what `Cached` memory in `free` output represents.
- **Storage and I/O (SYS-LNX-101 M03):** The page cache writeback mechanism (dirty pages, `vm.dirty_ratio`) and `vm.swappiness` covered in M03 connect directly to the memory pressure discussion in this module.

---

## 4. Core Theory: The Linux Memory Model

### 4.1 Virtual vs Physical Memory

Every process sees a **virtual address space** — a private, contiguous range of addresses managed by the CPU's Memory Management Unit (MMU). On 64-bit Linux, each process has up to 128 TB of virtual address space. The virtual space is divided into regions:

```
Virtual address space of a JVM process (example):
  0xFFFF...  ← kernel space (mapped but not accessible from user space)
  ─────────────────────────────────────────────────────────
  0x7FFF...  ← stack (grows down; local variables, call frames)
              ← anonymous mmaps (mmap(MAP_ANONYMOUS): malloc, JVM metaspace)
              ← file-backed mmaps (mmap of jars, shared libraries)
  ─────────────────────────────────────────────────────────
  [heap]     ← JVM heap: grows up via brk()/mmap()
  [BSS]      ← uninitialized global variables (zero-initialized)
  [data]     ← initialized global variables
  [text]     ← executable code (read-only, shared among processes)
  0x0000...
```

**Virtual Size (VSZ / VmSize):** The total size of the virtual address space claimed by the process. A JVM with `-Xmx32g` has already claimed 32 GB of virtual address space at startup, before any Java object is allocated. VSZ is almost always much larger than physical RAM — it includes uninitialized heap space, memory-mapped files that aren't loaded yet, and guard pages.

**Resident Set Size (RSS / VmRSS):** The number of physical pages currently mapped into this process's page tables — the actual RAM being used. A JVM with `-Xmx32g` that has only allocated 8 GB of Java objects has RSS ≈ 8 GB even though VSZ ≈ 32 GB.

### 4.2 Physical Memory Categories

The kernel divides physical RAM into categories:

```
Total Physical RAM (e.g., 64 GB)
├── Kernel memory (kernel code, kernel data structures, hardware DMA buffers)
│     Typically 0.5–2 GB; not user-visible; shown in /proc/meminfo as Slab, KernelStack, etc.
├── Anonymous memory (process heaps, stacks, mmap(MAP_ANONYMOUS))
│     Tied to a specific process; evicted to SWAP if swap exists; shown as VmRSS
│     Examples: JVM heap pages, Python object allocations, C malloc
└── File-backed memory (page cache)
      Not tied to a process; used to cache file data and mmap'd files
      Can be evicted at any time (read back from disk if needed)
      Subdivided into:
        Active(file):   recently accessed file pages (harder to evict)
        Inactive(file): older file pages (evicted first when RAM is needed)
        Dirty:          modified pages not yet written to disk
        Writeback:      pages currently being written to disk
        Buffers:        kernel block device cache (raw block I/O, largely absorbed into Cached on modern kernels)
```

### 4.3 Memory Overcommit

Linux uses **overcommit** by default: when a process calls `malloc(32 GB)` or a JVM starts with `-Xmx32g`, the kernel grants the virtual address space immediately without allocating physical pages. Physical pages are allocated only when the process first **touches** (reads or writes) each page — this is called **demand paging**.

This means:
- A 32 GB JVM heap with only 8 GB of live Java objects uses only ~8 GB of RAM
- Starting 10 JVMs with `-Xmx32g` on a 64 GB server won't immediately fail (overcommit permits it)
- The OOM killer fires when the system can't satisfy a page fault — when a process touches a page and there are no physical pages to allocate

Overcommit behavior is controlled by `vm.overcommit_memory`:
```
0 (default): heuristic overcommit — kernel uses judgment; permits moderate overcommit
1:           always overcommit — every malloc/mmap succeeds regardless of available memory
2:           never overcommit — fail malloc if commit would exceed physical RAM + swap
```

Most data engineering workloads use the default (0). Setting `vm.overcommit_memory=2` prevents overcommit surprises but requires precise memory sizing.

### 4.4 Memory Zones

The kernel divides physical RAM into **zones** for hardware compatibility reasons:

| Zone | Purpose |
|---|---|
| ZONE_DMA | First 16 MB; old ISA devices that can only address 24-bit addresses |
| ZONE_DMA32 | First 4 GB; 32-bit PCI devices |
| ZONE_NORMAL | Main RAM (above 4 GB on 64-bit systems); almost all allocations happen here |
| ZONE_HIGHMEM | Only on 32-bit kernels; not relevant for modern 64-bit systems |

Each zone maintains a free list. When a zone's free list drops below a watermark, the kernel begins reclaiming pages from that zone. If it drops to 0 and reclaim fails, the OOM killer fires specifically for that zone — even if other zones have memory.

---

## 5. free — Memory Overview

`free` reports a summary of system memory, drawing from `/proc/meminfo`.

### 5.1 Reading free Output

```bash
free -h          # human-readable (MB/GB)
free -m          # megabytes
free -g          # gigabytes
free -s 5        # refresh every 5 seconds (like watch)

# Example output:
free -h
#               total        used        free      shared  buff/cache   available
# Mem:           62.9G       18.2G        1.1G      512.0M       43.6G       43.8G
# Swap:           4.0G        3.8G      204.0M

# Column definitions:
# total:       total installed RAM (physical memory)
# used:        total - free - buff/cache
#              = RAM used by processes (anonymous memory: heaps, stacks)
#              WARNING: this definition means "used" excludes page cache
# free:        RAM with no pages mapped at all (truly unused; rare on busy systems)
# shared:      RAM used by tmpfs (ramfs, /dev/shm, shared memory segments)
# buff/cache:  Buffer cache + Page cache
#              = file-backed memory the kernel manages on behalf of all processes
#              This is NOT wasted memory — it speeds up I/O and is immediately 
#              reclaimable when applications need RAM
# available:   Estimate of RAM available to start new applications WITHOUT swapping
#              = free + reclaimable page cache + reclaimable slabs
#              This is the number to monitor, not "free"

# Swap row:
# total:  total swap space (across all swap devices/files)
# used:   pages currently stored in swap
# free:   unused swap space
# → If "used" > 0: some process pages are in swap (possible performance issue)
# → If "used" is growing under watch: active swapping is occurring NOW (serious issue)
```

### 5.2 The Critical Distinction: free vs available

```bash
# Common WRONG interpretation:
# "free shows 1 GB free, server is out of memory!"

# Correct interpretation:
# free = 1 GB: only 1 GB has no pages mapped at all
# available = 43.8 GB: 43.8 GB can be given to new applications
#
# The page cache (buff/cache = 43.6 GB) is "used" by the kernel to cache
# filesystem data, but it's immediately reclaimable. When a new application
# needs RAM, the kernel evicts old page cache entries to make room — the
# application gets the RAM, and the evicted file data is simply read back
# from disk if accessed again later.
#
# "available" accounts for this reclamability and gives the realistic
# answer to "how much more can I allocate before swapping?"

# Good threshold alert:
# MemAvailable < 10% of MemTotal → consider intervention
# MemAvailable < 5% of MemTotal → likely under active memory pressure

# Watch available memory during a Spark job:
watch -n 1 'free -h | grep Mem | awk "{print \"Available:\", \$7}"'
```

### 5.3 Interpreting the Swap Line

```bash
# Scenario A: Swap used = 0 → No swapping ever occurred (healthy)
# Scenario B: Swap used = 200 MB → Some pages swapped out at some point,
#             not necessarily right now (possibly during a past memory spike)
# Scenario C: Swap used = 3.8 GB of 4 GB → Swap is nearly full
#             → Current processes may be paging → severe performance degradation

# Distinguish scenario B from active swapping:
vmstat 1 5 | awk '{print "si:", $7, "so:", $8}'
# si (swap in) > 0:  pages being read FROM swap to RAM (process needs a swapped page)
# so (swap out) > 0: pages being written TO swap FROM RAM (kernel evicting process memory)
# Both near 0: swap is "used" historically but not actively paging now
# Both > 0:    ACTIVE SWAPPING — serious performance problem

# Clear swap (only if there's enough free RAM to absorb the swap contents):
swapoff -a && swapon -a
# This moves all swap-resident pages back to RAM — only run if MemAvailable > Swap used
```

---

## 6. /proc/meminfo — The Full Picture

`/proc/meminfo` is the authoritative source for all memory statistics. `free`, `vmstat`, `top`, and most monitoring tools read from here.

### 6.1 Key Fields

```bash
cat /proc/meminfo
```

```
MemTotal:       65536000 kB   # total physical RAM (constant)
MemFree:         1024000 kB   # truly free (no pages mapped); nearly always low
MemAvailable:   44040192 kB   # IMPORTANT: usable without swapping (use this for alerting)
Buffers:          204800 kB   # block device buffer cache (raw I/O; mostly in Cached on modern kernels)
Cached:         43008000 kB   # page cache (file-backed pages: mmap'd files, read/write cache)
SwapCached:       102400 kB   # pages that were in swap but are now back in RAM; swap entry kept in case
                               # they need to be swapped again (avoids re-writing unchanged pages)
Active:         32768000 kB   # recently used pages (LRU; harder to evict)
Inactive:       14336000 kB   # older pages (evicted first when RAM is needed)
Active(anon):   18432000 kB   # recently used anonymous (process heap/stack) pages
Inactive(anon):  2048000 kB   # older anonymous pages (candidates for swap-out)
Active(file):   14336000 kB   # recently used page cache pages
Inactive(file): 12288000 kB   # older page cache pages (evicted first for file cache)
Unevictable:     512000 kB    # pages locked in RAM (mlock/mlockall; can't be swapped)
Mlocked:         512000 kB    # pages explicitly locked with mlock() by processes
SwapTotal:       4194304 kB   # total swap space
SwapFree:         409600 kB   # free swap (SwapTotal - SwapFree = swap used = 3.8 GB!)
Dirty:            204800 kB   # page cache dirty pages waiting to be written to disk
Writeback:         10240 kB   # pages currently being written to disk
AnonPages:      20480000 kB   # all anonymous pages (= process RAM usage roughly)
Mapped:          8192000 kB   # file-backed pages mapped into process address spaces (mmap'd files)
Shmem:            524288 kB   # shared memory (tmpfs, IPC shm, /dev/shm)
KReclaimable:    2048000 kB   # kernel memory the kernel can reclaim under pressure
Slab:            2097152 kB   # kernel slab allocator memory (caches for kernel objects)
SReclaimable:    1536000 kB   # reclaimable slab (e.g., dentry/inode caches)
SUnreclaim:       561152 kB   # unreclaimable slab (kernel data structures actively in use)
KernelStack:       32768 kB   # kernel stack pages (8 KB per thread)
PageTables:       163840 kB   # page table memory (grows with number of process mappings)
NFS_Unstable:          0 kB   # NFS pages not yet committed to server (usually 0)
Bounce:                0 kB   # bounce buffer pages (for DMA; rare on modern hardware)
WritebackTmp:          0 kB   # FUSE-based writeback temp pages
CommitLimit:    36962048 kB   # max memory that can be committed (if vm.overcommit_memory=2)
Committed_AS:   48234496 kB   # total committed virtual memory (sum of all process VSZ)
VmallocTotal: 34359738367 kB  # total vmalloc address space (virtual; essentially infinite)
VmallocUsed:    1048576 kB    # vmalloc actually allocated (kernel driver mappings)
VmallocChunk:          0 kB   # largest available vmalloc chunk
HugePages_Total:       0      # huge pages (2 MB or 1 GB) configured in the pool
HugePages_Free:        0      # available huge page pool entries
HugePages_Rsvd:        0      # reserved huge pages (mmap'd but not yet faulted in)
HugePages_Surp:        0      # surplus huge pages (beyond HugePages_Total limit)
Hugepagesize:       2048 kB   # size of each huge page (2 MB standard)
Hugetlb:               0 kB   # total huge page pool memory
DirectMap4k:    1048576 kB    # physical RAM mapped in 4 KB pages by kernel
DirectMap2M:   65011712 kB    # physical RAM mapped in 2 MB huge pages by kernel
DirectMap1G:    2097152 kB    # physical RAM mapped in 1 GB pages by kernel
```

### 6.2 Critical Fields for Data Engineering

```bash
# Extract the most operationally relevant fields:
grep -E "MemTotal|MemFree|MemAvailable|Cached:|SwapTotal|SwapFree|Dirty:|Writeback:|AnonPages|Shmem|Committed_AS|HugePages" /proc/meminfo

# Interpret for a Kafka/Spark server:
# MemAvailable:   → "how much more can I allocate without swapping?"
# Active(file):   → hot page cache (Kafka log segments being actively read)
# Inactive(file): → cold page cache (Kafka segments not recently accessed; evicted first)
# Dirty:          → data in page cache not yet written to disk
#                    If this grows unboundedly → vm.dirty_ratio too high or disk too slow
# Writeback:      → flush in progress; if consistently > 0 → disk bottleneck
# AnonPages:      → total process heap/stack RAM (JVM heaps + Python allocations)
# SwapFree:       → if this is near 0, system is critically low on virtual memory
# Committed_AS:   → total promised virtual memory
#                    If Committed_AS >> MemTotal+SwapTotal → overcommit risk

# One-liner for memory summary in scripts:
python3 -c "
data = dict(line.split()[:2] for line in open('/proc/meminfo') if ':' in line)
total_gb = int(data['MemTotal:']) / 1024 / 1024
avail_gb = int(data['MemAvailable:']) / 1024 / 1024
anon_gb  = int(data['AnonPages:']) / 1024 / 1024
cache_gb = (int(data['Cached:']) + int(data['Buffers:'])) / 1024 / 1024
dirty_mb = int(data['Dirty:']) / 1024
swap_used_gb = (int(data['SwapTotal:']) - int(data['SwapFree:'])) / 1024 / 1024
print(f'Total: {total_gb:.1f} GB | Available: {avail_gb:.1f} GB | Anon: {anon_gb:.1f} GB | Cache: {cache_gb:.1f} GB | Dirty: {dirty_mb:.0f} MB | Swap used: {swap_used_gb:.2f} GB')
"
```

### 6.3 Huge Pages

Huge pages (2 MB or 1 GB) reduce TLB pressure for processes with large working sets. A JVM with a 32 GB heap uses 8 million 4 KB page table entries — each TLB miss is expensive under high memory throughput. With 2 MB huge pages, the same 32 GB heap uses only 16,384 entries.

```bash
# Check current huge page configuration
grep HugePages /proc/meminfo

# Check Transparent Huge Pages (THP) setting
cat /sys/kernel/mm/transparent_hugepage/enabled
# [always] madvise never
# ↑ THP is enabled (default on most distros)
# THP automatically promotes 4 KB pages to 2 MB when possible

# Kafka and THP: Kafka's recommendation is to DISABLE THP
# Reason: THP's khugepaged daemon periodically promotes pages,
# causing periodic latency spikes (GC-like pauses but from THP compaction)
# Disable THP for Kafka nodes:
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
# Persistent via systemd unit or rc.local

# Spark and THP: Similar recommendation — disable to avoid GC pauses
# Both Cloudera and Databricks recommend: THP=never for Spark nodes

# Explicit huge pages (for applications that use them, e.g., Oracle DB):
echo 10240 > /proc/sys/vm/nr_hugepages   # allocate 10240 × 2 MB = 20 GB of huge pages
# Note: JVMs with -XX:+UseLargePages will use these if available
```

---

## 7. vmstat — Memory and System Activity

`vmstat` (virtual memory statistics) shows a rolling view of memory, swap, I/O, CPU, and process activity. It is the fastest tool for determining if a system is under active memory pressure.

### 7.1 vmstat Output Explained

```bash
vmstat 1 10   # report every 1 second, 10 times
# (first line is average since boot — often misleading; focus on subsequent lines)

# procs ───── memory ───────────────────── swap ───── io ─────── system ──────── cpu ─────────
#  r  b   swpd    free   buff  cache   si   so    bi    bo   in    cs  us  sy  id  wa  st
#  2  0      0  1024000  204800 43008000  0   0   120   892  1234  4567 12   2  78   8   0
#  1  0      0  1012000  204800 43020000  0   0     0   920  1289  4623 14   2  76   8   0
#  3  1   2048  998000  204800 43008000  12  45   135   895  1345  4701 16   3  72   9   0
#  ↑  ↑     ↑      ↑              ↑      ↑   ↑                         ↑↑       ↑   ↑
#  |  |     |      |              |      |   |                         ||       |   |
#  |  |     |      |              |     si  so                        in cs    wa  st
#  |  |   swap   free           cache  (swap in)(swap out)  (interrupts)(ctx sw)(iowait)(steal)
#  r  b
```

**Column-by-column:**

```
PROCS:
r  — processes RUNNING or in run queue (waiting for CPU)
     r > number of CPUs = CPU saturation
b  — processes in UNINTERRUPTIBLE SLEEP (D state = blocked on I/O)
     b > 0 = some processes waiting for I/O (not necessarily a problem; watch the trend)

MEMORY (all in KB):
swpd  — amount of virtual memory currently in swap
         > 0 doesn't mean active swapping; check si/so columns
free  — free memory (equivalent to `free` command's "free" column; often small = normal)
buff  — buffer cache (raw block I/O)
cache — page cache (file-backed memory; large = good, means fast I/O)

SWAP:
si  — swap IN: KB/s being read FROM swap to RAM (pages being brought back in)
      si > 0 = processes are accessing pages that were swapped out → ACTIVE SWAPPING
so  — swap OUT: KB/s being written TO swap FROM RAM (pages being evicted)
      so > 0 = kernel is actively moving process pages to swap → MEMORY PRESSURE

I/O (blocks/s = 512-byte blocks per second):
bi  — blocks IN: data read from block devices (includes reads from disk)
bo  — blocks OUT: data written to block devices (page cache writeback, direct writes)

SYSTEM:
in  — hardware interrupts per second (NIC, disk, timer)
cs  — context switches per second (between processes/threads)
     Very high cs (>100,000) can indicate too many threads competing for CPU

CPU (% of total CPU time):
us  — user: time in user-space code
sy  — system: time in kernel code (syscalls, interrupts)
id  — idle: truly idle CPU time
wa  — iowait: CPU idle while at least one process is waiting for I/O
     wa > 5% = I/O bottleneck
st  — stolen: time stolen by hypervisor (VM host taking CPU from this guest)
     st > 5% = VM is oversubscribed; switch to dedicated instances
```

### 7.2 Key Diagnostic Patterns

```bash
# Pattern 1: Are we swapping right now?
vmstat 1 | awk '{print "si:", $7, "so:", $8}'
# si=0, so=0: no active swapping (even if swpd > 0)
# si>0 or so>0: ACTIVE SWAPPING — serious performance problem

# Pattern 2: Is the system CPU-saturated?
vmstat 1 | awk '{print "r:", $1, "id:", $15}'
# r=0, id=80: CPU mostly idle, no queue
# r=8 on 4-core machine, id=0: 2× over-saturated for CPU

# Pattern 3: Is there a persistent I/O wait?
vmstat 1 10 | awk 'NR>1 {sum+=$16; count++} END {print "avg iowait:", sum/count "%"}'
# If avg > 5%: I/O is a bottleneck for CPU-bound tasks

# Pattern 4: Heavy context switching (too many threads)
vmstat 1 | awk '{print "cs:", $12}'
# Spark or Airflow with 1000 threads on a 32-core machine: cs > 200,000/s
# Excessive thread context switching wastes CPU on scheduler overhead

# Pattern 5: Memory pressure trend
watch -n 1 'vmstat 1 1 | tail -1 | awk "{print \"free:\", \$4\"KB\", \"cache:\", \$6\"KB\", \"si:\", \$7, \"so:\", \$8}"'

# vmstat -s: one-shot summary (useful for scripts)
vmstat -s | grep -E "total memory|free memory|used swap|running|blocked"
# 65536000 K total memory
#  1024000 K free memory
#  3784704 K used swap
#        2 running
#        0 blocked

# vmstat -d: disk statistics (like a simplified iostat)
vmstat -d 1
```

---

## 8. The OOM Killer

### 8.1 What Triggers the OOM Killer

The kernel's OOM (Out Of Memory) killer runs when the system cannot satisfy a memory allocation request after exhausting all reclamation strategies:

```
Page fault → kernel tries to allocate a physical page
              → check free lists: empty
              → reclaim page cache (evict inactive file pages): not enough
              → reclaim SReclaimable slab (dentry/inode caches): not enough
              → check swap: no swap configured, or swap is full
              → OOM killer invoked
```

The OOM killer can also be triggered by a specific memory **zone** being exhausted (even if other zones have pages), or by a cgroup memory limit being exceeded — in which case the OOM kill is scoped to that cgroup.

### 8.2 OOM Scoring

The OOM killer picks its victim using an **oom_score**, calculated per-process:

```
oom_score ≈ (RSS + swap used) / MemTotal × 1000  +  oom_score_adj

Range: 0–1000
Higher score = more likely to be killed

oom_score_adj: per-process adjustment, range -1000 to +1000
  -1000: never kill this process (protect it)
  +1000: always kill this first
  0:     default (OOM score determines fate)
```

```bash
# See OOM score for all running processes
ps -eo pid,comm,rss | awk 'NR>1 {print $1, $2, $3}' | while read pid name rss; do
    score=$(cat /proc/$pid/oom_score 2>/dev/null || echo "N/A")
    adj=$(cat /proc/$pid/oom_score_adj 2>/dev/null || echo "N/A")
    echo "$score $adj $pid $name ${rss}KB"
done | sort -rn | head -20

# Output: oom_score  oom_score_adj  pid  name  rss
# 723     0          8234   java   8192000KB   ← most likely to be killed
# 245     0          3456   java   2097152KB
# 45     -900        1234   kafka  524288KB     ← protected from OOM kill

# Check OOM score for Kafka broker
cat /proc/$(pgrep -f kafka.Kafka)/oom_score
# 45   (low score = rarely killed)
cat /proc/$(pgrep -f kafka.Kafka)/oom_score_adj
# -900 (heavily protected)

# Set OOM protection for Kafka (protect from OOM kill):
echo -900 > /proc/$(pgrep -f kafka.Kafka)/oom_score_adj
# Persistent: add to Kafka's startup script or systemd unit:
# [Service]
# OOMScoreAdjust=-900

# Make Spark executor the first to be killed during OOM:
echo 500 > /proc/$(pgrep -f CoarseGrainedExecutorBackend)/oom_score_adj
```

### 8.3 Reading the OOM Kill Log

```bash
# OOM kills are always logged in dmesg
dmesg | grep -i "oom\|killed\|out of memory" | tail -30

# Example OOM kill message:
# [1234567.890] oom-kill:constraint=CONSTRAINT_NONE,nodemask=(null),cpuset=/,
#               mems_allowed=0,global_oom,task_memcg=/,task=java,pid=8234,uid=1001
# [1234567.891] Out of memory: Killed process 8234 (java) total-vm:33554432kB,
#               anon-rss:16777216kB, file-rss:2097152kB, shmem-rss:0kB,
#               UID:1001 pgtables:131072kB oom_score_adj:0

# Parse the OOM message:
# total-vm:   33554432 kB = 32 GB virtual size (the -Xmx32g JVM)
# anon-rss:   16777216 kB = 16 GB physical anonymous pages (JVM heap in use)
# file-rss:    2097152 kB = 2 GB file-backed pages (mmapped JARs, class files)
# oom_score_adj: 0 = no protection set; default scoring applied

# Full OOM report in dmesg shows ALL processes at the time of OOM:
# [1234567.890] [ pid ]   uid  tgid total_vm    rss pgtables_bytes swapents oom_score_adj name
# [1234567.890] [  1234]  1001  1234  33554432 18874368   134217728        0             0 java
# [1234567.890] [  5678]  1001  5678   8388608  4194304    67108864        0             0 java
# [1234567.890] [  9012]     0  9012    131072    32768     2097152        0          -900 kafka

# Automate OOM monitoring:
# Set up a script to alert when OOM occurs:
journalctl -k -f | grep --line-buffered "Out of memory" | while read line; do
    echo "OOM KILL DETECTED: $line" | mail -s "OOM Kill Alert" ops@company.com
done
# Or use dmesg --follow:
dmesg --follow | grep --line-buffered -i "oom\|killed"
```

### 8.4 OOM Killer in Kubernetes and Docker

In containerized data engineering deployments:

```bash
# Kubernetes: OOM kill is scoped to the Pod's cgroup
# Each Pod has a memory limit; when the Pod's total RSS exceeds the limit,
# the kernel OOM-kills the process that caused the limit to be exceeded

# Check Kubernetes OOM kills:
kubectl describe pod <spark-executor-pod> | grep -A 5 "OOMKilled"
# Containers:
#   executor:
#     Last State: Terminated
#       Reason:   OOMKilled
#       Exit Code: 137    ← 128 + 9 (SIGKILL)

# Exit code 137 always means OOM kill in containers
# (128 + signal 9 = process killed with SIGKILL by OOM killer)

# Docker OOM kill:
docker inspect <container> | python3 -c "import json,sys; c=json.load(sys.stdin)[0]; print('OOMKilled:', c['State']['OOMKilled'])"

# Configure memory limits for Spark on Kubernetes:
# In SparkSession or spark-defaults.conf:
# spark.executor.memory=4g
# spark.executor.memoryOverhead=1g  ← off-heap overhead (JVM metaspace, native libs)
#                                       default: max(384MB, 10% of executor.memory)
# Total per executor: 4 + 1 = 5 GB
# Set Pod limit to: spark.executor.memory + spark.executor.memoryOverhead + some buffer
# spark.kubernetes.executor.limit.cores=2
# Kubernetes memory limit = 5.5g (must be >= executor.memory + memoryOverhead)
```

---

## 9. Per-Process Memory: /proc and smaps

### 9.1 /proc/<pid>/status Memory Fields

```bash
# Detailed memory breakdown for a process
cat /proc/$(pgrep -f kafka.Kafka)/status | grep -E "Vm|Threads|State"

# VmPeak:    9437184 kB   ← peak virtual memory size (high-water mark of VSZ)
# VmSize:    8388608 kB   ← current virtual memory size (VSZ)
# VmLck:          0 kB   ← locked memory (mlock; part of Unevictable in /proc/meminfo)
# VmPin:          0 kB   ← pinned memory (cannot be moved by kernel)
# VmHWM:     3145728 kB  ← peak RSS (high-water mark of physical memory usage)
# VmRSS:     2097152 kB  ← current RSS (physical pages resident in RAM)
# RssAnon:   1572864 kB  ← anonymous RSS (JVM heap pages in RAM)
# RssFile:    524288 kB  ← file-backed RSS (JAR files, class files mapped in)
# RssShmem:        0 kB  ← shared memory RSS
# VmData:    5242880 kB  ← data segment size (heap + bss)
# VmStk:       12288 kB  ← stack size
# VmExe:        1024 kB  ← executable code size
# VmLib:      262144 kB  ← shared library size
# VmPTE:       81920 kB  ← page table entries size
# VmSwap:          0 kB  ← pages of this process currently in swap
```

### 9.2 /proc/<pid>/smaps — Memory Region Detail

`smaps` shows a breakdown of every memory mapping in a process — heap, stack, each shared library, each mmapped file:

```bash
# Full smaps (very verbose for a JVM):
cat /proc/$(pgrep -f kafka.Kafka)/smaps | head -60

# Grep for the heap mapping:
cat /proc/$(pgrep -f kafka.Kafka)/smaps | awk '/\[heap\]/{found=1} found{print; if(/^$/)exit}'
# 00c00000-80c00000 rw-p 00000000 00:00 0  [heap]
# Size:         2097152 kB    ← virtual size of heap mapping
# Rss:          1572864 kB    ← physical pages resident
# Pss:          1572864 kB    ← proportional share (same as Rss for private mappings)
# Shared_Clean:       0 kB
# Shared_Dirty:       0 kB
# Private_Clean:      0 kB    ← heap pages not modified (not yet written)
# Private_Dirty: 1572864 kB   ← heap pages modified (= actual JVM heap in use)
# Referenced:   1572864 kB    ← accessed since last reset
# Anonymous:    1572864 kB    ← anonymous (no backing file)
# LazyFree:           0 kB
# AnonHugePages:  1048576 kB  ← backed by THP 2 MB pages (if THP is enabled)
# ShmemPmdMapped:     0 kB
# FilePmdMapped:      0 kB
# Swap:               0 kB    ← pages of this region currently in swap
# SwapPss:            0 kB    ← proportional swap

# smaps_rollup: summary of smaps (much faster than parsing all of smaps)
cat /proc/$(pgrep -f kafka.Kafka)/smaps_rollup
# VmFlags: rd wr mr mw me ac sd
# ...
# Rss:           2097152 kB   ← total RSS across all regions
# Pss:           2097152 kB   ← proportional RSS (useful for shared memory)
# Pss_Anon:      1572864 kB   ← anonymous PSS
# Pss_File:       524288 kB   ← file-backed PSS
# Pss_Shmem:           0 kB
# Shared_Clean:   262144 kB   ← shared libs (read-only, shared with other processes)
# Shared_Dirty:        0 kB
# Private_Clean:       0 kB
# Private_Dirty: 1572864 kB   ← private modified pages (= actual unique RAM usage)
# Swap:                0 kB
```

### 9.3 PSS: Proportional Set Size

**PSS (Proportional Set Size)** is a more accurate measure of a process's true memory footprint than RSS. It accounts for shared memory by counting each shared page proportionally:

```
Process A and B both mmap() the same shared library (100 MB):
  RSS(A) = 100 MB (includes the full 100 MB shared library)
  RSS(B) = 100 MB (includes the full 100 MB shared library)
  → Sum of RSS = 200 MB, but actual physical RAM used = 100 MB (shared once)

  PSS(A) = 50 MB (half of the 100 MB shared library)
  PSS(B) = 50 MB (half of the 100 MB shared library)
  → Sum of PSS = 100 MB = actual physical RAM used (accurate)

For a server with 10 JVMs sharing common JARs:
  Each JVM RSS: 4 GB (inflated by shared class files/JARs counted in each)
  Each JVM PSS: 3.2 GB (subtracts the shared portion, apportioned)
  Sum RSS: 40 GB → overestimates total memory by ~8 GB
  Sum PSS: 32 GB → accurate total

In practice: ps shows RSS, which overestimates. smaps_rollup shows PSS.
```

---

## 10. Swap Configuration and Behavior

### 10.1 Swap Setup

```bash
# Check current swap
swapon --show
# NAME       TYPE  SIZE  USED  PRIO
# /dev/sda3  partition  4G  3.8G    -2

# Add a swap file (when no swap partition is available):
fallocate -l 16G /swapfile     # allocate 16 GB
chmod 600 /swapfile            # only root can read/write
mkswap /swapfile               # set up swap area
swapon /swapfile               # activate

# Persistent via /etc/fstab:
echo "/swapfile none swap sw 0 0" >> /etc/fstab

# Multiple swap devices: use priority to prefer one over another
swapon -p 100 /dev/nvme0n1p3   # SSD swap (preferred: priority 100)
swapon -p 0   /swapfile        # HDD swap (fallback: lower priority)

# Disable swap entirely (for servers where swap causes more harm than good):
swapoff -a
# Also remove from /etc/fstab to prevent re-enabling on reboot

# ZRAM: compressed in-memory swap (good for memory-constrained servers):
modprobe zram num_devices=1
echo lz4 > /sys/block/zram0/comp_algorithm    # fast compression
echo 8G > /sys/block/zram0/disksize           # 8 GB compressed size
mkswap /dev/zram0
swapon -p 100 /dev/zram0       # highest priority (use ZRAM first)
```

### 10.2 vm.swappiness

`vm.swappiness` controls how aggressively the kernel prefers swapping anonymous memory over evicting page cache:

```bash
# View current setting
cat /proc/sys/vm/swappiness
# 60   ← default

# Values:
# 0:   avoid swap unless absolutely necessary
#       Kernel prefers evicting page cache over swapping.
#       For Kafka: good, keeps page cache warm for consumers.
#       Risk: if processes have huge heaps that fill all RAM, OOM kills happen
#             before swap saves them (no swap buffer zone)
# 1:   minimize swap (common recommendation for database/broker servers)
# 10:  low swap tendency (still uses swap as a last resort)
# 60:  default (kernel actively considers both swap and page cache eviction)
# 100: maximize swap; swap aggressively regardless of page cache presence

# For Kafka brokers: set to 1 (protect page cache, avoid swap latency)
sysctl -w vm.swappiness=1
echo "vm.swappiness=1" >> /etc/sysctl.d/60-kafka.conf

# For Spark workers: set to 1 (OOM kill is better than swap-induced latency)
# A Spark executor in swap runs at disk speed; it's better to kill it and retry
sysctl -w vm.swappiness=1

# For general-purpose servers with mixed workloads: keep at 10-30
# Swap as a safety net catches memory spikes while still prioritizing page cache

# Apply immediately without reboot:
sysctl -p /etc/sysctl.d/60-kafka.conf
```

### 10.3 When to Have Swap (and When Not To)

```
Have swap when:
  ✓ Server runs critical services (Kafka broker) that cannot crash
    → Swap provides a buffer before OOM kill; gives operators time to respond
  ✓ Server has intermittent memory spikes (batch ETL + broker on same node)
    → Swap absorbs the spike without killing the broker
  ✓ vm.swappiness=1 (minimize use, but have it as emergency buffer)
  ✓ Swap is on SSD/NVMe (swap latency < 1 ms; acceptable for occasional use)

Skip swap when:
  ✗ Low-latency streaming (Kafka at <10 ms p99)
    → Even brief swap-in adds 10+ ms latency for touched pages
  ✗ Spark executors (swap turns a 10-minute job into a 10-hour job; better to OOM-kill and retry)
  ✗ Kubernetes Pods with resource limits (limits should OOM-kill before swap is needed)
  ✗ Swap is on HDD (10+ ms per page = catastrophic under any real swap usage)
```

---

## 11. Memory Pressure Detection

### 11.1 PSI — Pressure Stall Information

Linux 4.20+ includes **PSI (Pressure Stall Information)** — a measure of time spent stalled waiting for a resource. This is more accurate than the indirect signals from `vmstat` si/so:

```bash
# Check if PSI is available
ls /proc/pressure/
# cpu  io  memory   ← all three if PSI is enabled

# Memory pressure:
cat /proc/pressure/memory
# some avg10=0.50 avg60=0.23 avg300=0.12 total=12345678
# full avg10=0.00 avg60=0.00 avg300=0.00 total=0

# "some": fraction of time at least ONE task was stalled waiting for memory
# "full": fraction of time ALL tasks were stalled waiting for memory (complete stall)
# avg10/60/300: exponential moving average over 10s/60s/300s windows
# total: cumulative microseconds of stall time

# Healthy: some avg10 < 1%, full avg10 ≈ 0%
# Moderate pressure: some avg10 1-5%
# Severe pressure: some avg10 > 10% or full avg10 > 0.5%

# Set a PSI threshold and receive an fd notification when exceeded:
# (Linux 5.2+, using cgroup2 + PSI polling)
# Useful for Kubernetes memcg pressure notifications
```

### 11.2 Kswapd and Direct Reclaim

```bash
# kswapd is the kernel's background memory reclaim daemon
# Monitor its CPU usage — if kswapd is using significant CPU, the system is under pressure:
top -p $(pgrep kswapd0)
# If kswapd0 shows >1% CPU over sustained periods: memory reclaim is active

# Alternatively, watch pgscank (pages scanned by kswapd) in vmstat:
vmstat 1 | awk '{print "pgscank:", $10}'
# If pgscank is high and growing: kswapd is scanning heavily = memory pressure

# Direct reclaim: when kswapd can't keep up, calling processes must reclaim pages
# synchronously (blocking the calling process during memory allocation)
# Evidence: high "wa" in vmstat CPU columns, high system call time

# /proc/vmstat for reclaim stats:
grep -E "pgscank|pgscand|pgsteal|pgscan" /proc/vmstat
# pgscank:    pages scanned by kswapd (background reclaim)
# pgscand:    pages scanned directly (application blocked during reclaim)
# pgsteal_kswapd: pages actually reclaimed by kswapd
# pgsteal_direct: pages reclaimed directly by processes
# If pgscand >> 0: direct reclaim is happening = latency impact on applications
```

### 11.3 Practical Memory Pressure Monitoring Script

```bash
#!/bin/bash
# memory_pressure.sh: alert when memory is under pressure
WARN_AVAIL_PCT=20   # warn when MemAvailable < 20% of MemTotal
CRIT_AVAIL_PCT=10   # critical when MemAvailable < 10%
SWAP_WARN_PCT=50    # warn when swap > 50% used

while true; do
    mem_total=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    mem_avail=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
    swap_total=$(grep SwapTotal /proc/meminfo | awk '{print $2}')
    swap_free=$(grep SwapFree /proc/meminfo | awk '{print $2}')
    swap_used=$((swap_total - swap_free))
    
    avail_pct=$((mem_avail * 100 / mem_total))
    swap_pct=$(( swap_total > 0 ? swap_used * 100 / swap_total : 0 ))
    
    # Check for active swapping
    si_so=$(vmstat 1 1 | tail -1 | awk '{print $7+$8}')
    
    if [ "$avail_pct" -lt "$CRIT_AVAIL_PCT" ]; then
        echo "CRITICAL: MemAvailable=${avail_pct}% (${mem_avail} kB)"
    elif [ "$avail_pct" -lt "$WARN_AVAIL_PCT" ]; then
        echo "WARN: MemAvailable=${avail_pct}%"
    fi
    
    if [ "$swap_pct" -gt "$SWAP_WARN_PCT" ]; then
        echo "WARN: Swap ${swap_pct}% used (${swap_used} kB / ${swap_total} kB)"
    fi
    
    if [ "$si_so" -gt "0" ]; then
        echo "WARN: Active swap I/O detected (si+so=${si_so} KB/s)"
    fi
    
    sleep 30
done
```

---

## 12. Mental Models

### 12.1 Memory as a Hotel

Think of physical RAM as a hotel with a fixed number of rooms (pages). There are two types of guests: **anonymous guests** (process heap/stack pages — like permanent residents who pay for their room and must be evicted with compensation, i.e., written to swap) and **file cache guests** (page cache — like day-trippers who have a home elsewhere on disk; they can be asked to leave instantly at no cost because they can always come back by re-reading the file).

The kernel is the hotel manager. When a new guest arrives (memory allocation) and all rooms are full, the manager first evicts day-trippers (page cache) — they don't complain and their room is instantly available. Only when all day-trippers are gone and rooms are still needed does the manager evict permanent residents (swap out process pages) — much more work because it requires finding storage for their belongings (writing to swap) and giving them a claim check (swap entry).

`MemAvailable` is the manager's estimate of how many rooms can be quickly freed (day-tripper rooms + a fraction of permanent resident rooms). `free` is only the rooms that are literally empty right now.

### 12.2 The OOM Killer as a Triage Doctor

The OOM killer is a doctor in an overcrowded emergency room with no more beds (memory). It cannot help everyone; it must choose who to discharge (kill) to make room for the patients who can still be saved (critical system processes). It applies a triage score (`oom_score`): patients with large resource use (`high RSS`) who have been there a long time or who aren't critical get discharged first. You can pin a patient to a bed with a "do not discharge" label (`oom_score_adj = -1000`) — but this means someone else gets discharged instead. If all patients have protection orders, the system may panic rather than kill anyone.

### 12.3 VSZ as a Reservation, RSS as Occupancy

Think of VSZ as a hotel reservation (you booked 32 rooms but haven't checked in yet — no rooms are physically blocked) and RSS as actual occupancy (the rooms that guests are actually sleeping in). Linux's overcommit means reservations are accepted even if the hotel might not have enough rooms when everyone shows up. The OOM killer fires when more guests show up than there are rooms — it evicts some guests to make room for the critical arrivals.

---

## 13. Failure Scenarios

### 13.1 Spark Executor OOM-Killed With MemAvailable Showing 12 GB

```
Symptom: Spark executor dies with exit code 137 (OOM kill).
  free -h shows: Available: 12 GB at the moment of failure.
  Team says "there was plenty of memory available."

  dmesg | grep -i "kill\|oom" | tail -10
  # Out of memory: Kill process 8234 (java) total-vm:34359738368kB,
  #   anon-rss:8589934592kB, file-rss:2097152kB
  # total-vm = 32 GB (JVM -Xmx32g)
  # anon-rss = 8 GB (8 GB of heap in use)

  But MemAvailable was 12 GB — why OOM?

  The Spark executor tried to allocate a new 2 GB shuffle buffer.
  At the moment of allocation, the kernel looked at:
  - Free pages in ZONE_NORMAL: essentially 0
  - Reclaimable page cache: only 1.5 GB (recently used, not ideal to evict)
  - Swap: 0 GB (no swap configured)
  - Total reclaim potential: ~1.5 GB < 2 GB needed

  MemAvailable = 12 GB included "reclaimable page cache" — but reclaiming
  12 GB of page cache is a prolonged process that takes seconds. During a
  single page fault, the kernel can only reclaim a limited number of pages
  synchronously. If the single allocation request is too large, it fails
  even though MemAvailable would eventually permit it.

  Diagnosis:
    cat /proc/meminfo | grep -E "MemAvailable|Active\(file\)|Inactive\(file\)|AnonPages"
    # MemAvailable:   12582912 kB
    # Active(file):   10485760 kB   ← 10 GB of hot page cache (don't want to evict this)
    # Inactive(file):  1572864 kB   ← 1.5 GB of cold page cache (evictable)
    # AnonPages:      46137344 kB   ← 44 GB of process memory (JVM heaps!)

    The "available" memory is mostly hot page cache — technically reclaimable
    but the kernel is reluctant to evict Active(file) for a single allocation.

  Fix:
    Option A: Reduce Spark executor heap size
      spark.executor.memory=24g  (instead of 32g)
      → Leaves more ACTUAL free pages for new allocations

    Option B: Add swap (gives kernel more options when page cache alone isn't enough)
      swapon /dev/nvme0n1p3

    Option C: Reduce executors per node
      Fewer concurrent JVMs = less total heap = more physical pages free

    Option D: Enable off-heap memory and reduce heap
      spark.memory.offHeap.enabled=true
      spark.memory.offHeap.size=8g
      spark.executor.memory=8g   (heap)
      → Total 16g per executor; split reduces peak anonymous RSS
```

### 13.2 Kafka Page Cache Eviction Under Spark Cotenancy

```
Symptom: Kafka consumer lag grows whenever Spark jobs run on the same node.
  Lag recovers slowly after Spark jobs finish.

  Diagnosis:
  During Spark job:
    cat /proc/meminfo | grep -E "Active\(file\)|Inactive\(file\)|AnonPages"
    # Active(file):    1048576 kB   ← only 1 GB hot file cache! (was 40 GB)
    # Inactive(file):  2097152 kB
    # AnonPages:      55834574 kB   ← 53 GB anonymous (Spark executor heaps!)

  The Spark executors' JVM heaps (53 GB of anonymous memory) forced the kernel
  to evict Kafka's page cache (Kafka log segments). Consumer reads now go to disk.

  Confirm: iostat shows Kafka's disk reads increasing when Spark runs:
    iostat -xm 5 | grep nvme
    # During Spark: rkB/s = 12000 (12 GB/s reads!) ← page cache miss, reading from disk
    # After Spark: rkB/s = 120 (serving from cache again)

  Fix Option A: Isolate Kafka to its own server (best practice for production)
    Never co-locate latency-sensitive brokers with batch Spark workloads.

  Fix Option B: Use cgroups/Kubernetes to enforce memory limits
    # Limit Spark executor cgroup to 48 GB
    # Kafka keeps the remaining 16 GB for page cache
    # In Kubernetes:
    # resources:
    #   limits:
    #     memory: "48Gi"
    #   requests:
    #     memory: "48Gi"

  Fix Option C: Use fadvise to tell the kernel about Kafka's cache needs
    # Kafka 2.4+ uses posix_fadvise(POSIX_FADV_WILLNEED) on log segments
    # when consumers are actively reading them — this hints to the kernel
    # to keep those pages in cache.
    # Enable in Kafka: socket.send.buffer.bytes and socket.receive.buffer.bytes
    # tuning doesn't help here; this is about memory.cache.max.bytes tuning.

  Fix Option D: vm.min_free_kbytes
    # Set a minimum free memory floor — kernel starts reclaim earlier
    sysctl -w vm.min_free_kbytes=1048576   # keep at least 1 GB truly free
    # This causes earlier reclaim of inactive file pages, potentially reducing
    # OOM events but also reducing cache efficiency
```

### 13.3 Server with 128 GB RAM Running Slow: It's Swapping

```
Symptom: Server with 128 GB RAM, running only 4 JVMs totaling 64 GB of -Xmx,
  is responding slowly. top shows high si/so in vmstat.

  free -h
  #               total   used    free  shared buff/cache available
  # Mem:          125.8G  124.9G  0.1G   0.5G      0.8G      0.2G
  # Swap:          32.0G   28.4G  3.6G

  vmstat 1 5 | awk '{print "si:", $7, "so:", $8}'
  # si: 0    so: 0    ← not actively swapping RIGHT NOW
  # si: 0    so: 0
  # si: 45   so: 0    ← sudden swap-in spike!
  # si: 0    so: 0
  # si: 234  so: 0    ← more swap-in

  The si > 0 means pages are being read FROM swap. Processes are touching
  pages that were previously swapped out. 28 GB in swap means a LOT
  of process pages are sitting in swap.

  Why? AnonPages check:
    grep AnonPages /proc/meminfo
    # AnonPages: 124 GB
    # 4 JVMs × 32 GB -Xmx = 128 GB total virtual heap
    # All 4 JVMs actually touched most of their heap = 124 GB physical anon pages
    # 124 GB anon + 0.8 GB cache + kernel = > 128 GB physical RAM
    # → kernel swapped out ~28 GB of "cold" anon pages to fit

  Root cause: All 4 JVMs have very large heaps that are actually fully populated
  with live objects (no garbage to collect). The system is genuinely over-provisioned.

  Fix:
    Option A: Reduce -Xmx per JVM from 32g to 28g (free up ~16 GB for page cache + headroom)

    Option B: Migrate 1 JVM to another server (reduce co-tenancy)

    Option C: Check for memory leaks in the JVMs
      jcmd <pid> GC.heap_info    # check live object count
      jstat -gcutil <pid> 1000   # check GC frequency
      # If old gen is 90%+ full: memory leak or too much retained data

    Option D: Force GC to reclaim leaked memory temporarily
      jcmd <pid> GC.run    # trigger full GC
      # Monitor if RSS drops after GC — if it does, there's garbage not being collected

    Option E (emergency): Expand swap to prevent OOM while you solve root cause
      fallocate -l 32G /swapfile2 && chmod 600 /swapfile2 && mkswap /swapfile2 && swapon /swapfile2
```

### 13.4 Python Airflow Worker Memory Leak

```
Symptom: Airflow worker Python process memory grows over 2 days from 512 MB to 8 GB,
  then gets OOM-killed. Cycle repeats.

  Monitor over time:
    while true; do
        PID=$(pgrep -f "airflow worker")
        RSS=$(cat /proc/$PID/status | grep VmRSS | awk '{print $2}')
        echo "$(date) PID=$PID RSS=${RSS}kB"
        sleep 60
    done >> /var/log/airflow_memory.log

  Confirm leak pattern:
    grep RSS /var/log/airflow_memory.log | awk '{print $1, $NF}' | \
      awk -F= '{print $1, $2}' | tail -100
    # 2026-06-27T00:00 512000
    # 2026-06-27T06:00 1048576
    # 2026-06-27T12:00 2097152
    # 2026-06-27T18:00 4194304    ← doubling every 6 hours
    # 2026-06-28T00:00 8388608    ← OOM kill imminent

  Diagnose the Python leak:
    # 1. Take a memory snapshot with tracemalloc (if you can modify the worker):
    # Add to the worker's startup:
    # import tracemalloc
    # tracemalloc.start()
    # At SIGUSR1 handler:
    # snapshot = tracemalloc.take_snapshot()
    # for stat in snapshot.statistics('lineno')[:20]:
    #     print(stat)

    # 2. Use memory_profiler for per-line analysis:
    # pip install memory_profiler
    # @profile decorator on the DAG task function

    # 3. Use objgraph for live object counting:
    # import objgraph
    # objgraph.show_growth()   # which object types grew since last call

    # 4. Check for global accumulators (common Airflow leak pattern):
    # - DAG context objects being accumulated in a class variable
    # - XCom data being cached without eviction
    # - Poorly written custom operators that accumulate state

  Fix:
    Workaround: Restart worker regularly (before it reaches OOM threshold)
      # In systemd unit, add periodic restart:
      # [Service]
      # RuntimeMaxSec=86400   # restart after 24 hours regardless
      
    Fix: Find and fix the leak with the profiling tools above.
    
    Protect Kafka from OOM kill if both are on the same node:
      echo -900 > /proc/$(pgrep -f kafka.Kafka)/oom_score_adj
      # → OOM killer will kill the leaking Airflow worker before Kafka
```

---

## 14. Data Engineering Connections

### 14.1 JVM Heap vs OS Memory: The Full Accounting

```
A Kafka JVM process with -Xmx4g uses MORE than 4 GB of OS memory:

  JVM heap:           4 GB    (controlled by -Xmx)
  JVM metaspace:      ~200 MB (class metadata; controlled by -XX:MaxMetaspaceSize)
  JVM code cache:     ~240 MB (JIT-compiled native code)
  JVM stack space:    ~8 KB × 157 threads = ~1.2 MB
  JVM off-heap:       varies  (DirectByteBuffer, NIO buffers, Netty buffers)
  Shared libraries:   ~200 MB (JDK, native libraries)
  ───────────────────────────
  Total RSS:          ~5-6 GB for a "4 GB heap" JVM

  In Kafka specifically:
    NetworkReceive buffers (DirectByteBuffer): can be large under load
    OS page cache: Kafka relies on page cache for log segments
    → Total physical memory Kafka needs: heap_RSS + buffers + page_cache_for_logs

  For a Kafka broker on a 64 GB server with -Xmx4g:
    JVM RSS:    ~5 GB
    Page cache: ~55 GB (Kafka's working set of log segments)
    OS/kernel:  ~2 GB
    Buffer:     ~2 GB
    Total:      ~64 GB — fully utilized, which is CORRECT for Kafka
```

### 14.2 Spark Memory Model

```
Spark's memory is divided into regions (spark.memory.fraction controls the split):

  ┌─────────────────────────────────────────────────────────────────┐
  │  JVM Heap  (-Xmx = spark.executor.memory)                       │
  │  ┌──────────────────────────────────┐  ┌───────────────────────┐│
  │  │ Unified Memory (default 60%)     │  │ User Memory (40%)     ││
  │  │ spark.memory.fraction=0.6        │  │ (your Python/Scala    ││
  │  │ ┌────────────────┬───────────────┤  │  objects, UDFs, etc.) ││
  │  │ │ Execution Mem  │ Storage Mem   │  │                       ││
  │  │ │ (shuffle,sort, │ (RDD/DF cache │  │                       ││
  │  │ │  join)         │  persist())   │  │                       ││
  │  │ └────────────────┴───────────────┘  └───────────────────────┘│
  │  └──────────────────────────────────────────────────────────────┘│
  └─────────────────────────────────────────────────────────────────┘
  Plus:
  ┌───────────────────────────────┐
  │ Off-heap (optional)           │  ← spark.memory.offHeap.enabled=true
  │ spark.memory.offHeap.size     │     stores columnar data outside JVM GC
  └───────────────────────────────┘
  ┌───────────────────────────────┐
  │ Overhead (memoryOverhead)     │  ← JVM metaspace, code cache, Netty buffers
  │ max(384MB, 10% of executor    │     default = max(384MB, 10% * executor.memory)
  │  memory)                      │     MUST be sized correctly for containerized deploys
  └───────────────────────────────┘

  If execution memory fills up: spill to disk (shuffle spill files)
  If storage memory fills up: evict cached RDDs (re-compute on next access)
  If total RSS exceeds Pod memory limit: OOM kill (exit 137)

  Key tuning levers:
    spark.executor.memory=8g          # JVM heap
    spark.executor.memoryOverhead=2g  # off-JVM overhead
    spark.memory.fraction=0.6         # fraction of heap for Spark (vs user objects)
    spark.memory.storageFraction=0.5  # within Spark memory, storage vs execution split
    spark.memory.offHeap.enabled=true
    spark.memory.offHeap.size=4g      # use 4 GB off-heap for columnar data
```

### 14.3 Kafka Memory Tuning

```bash
# /etc/kafka/jvm.options (or KAFKA_HEAP_OPTS environment variable):
export KAFKA_HEAP_OPTS="-Xmx6g -Xms6g"
# -Xms = -Xmx: pre-allocate the full heap at JVM startup
#   → prevents heap resizing pauses during operation
#   → disadvantage: RSS = 6 GB from startup, not just when needed
# Typical Kafka heap: 4-8 GB (Kafka mostly works through page cache, not heap)
# LARGE heaps hurt: more GC pause risk; less RAM left for page cache

# GC tuning for Kafka:
export KAFKA_JVM_PERFORMANCE_OPTS="-server \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=20 \
  -XX:InitiatingHeapOccupancyPercent=35 \
  -XX:+ExplicitGCInvokesConcurrent \
  -XX:MaxInlineLevel=15"
# G1GC with 20 ms max pause target (reduces latency spikes from GC)
# InitiatingHeapOccupancyPercent=35: start GC when 35% of heap is occupied
#   → earlier, more frequent small GCs vs rare large pauses

# Page cache sizing: leave enough RAM for Kafka's working set
# On a 64 GB server with 6 GB JVM:
# Available for page cache: 64 - 6 - 2 (OS) = 56 GB
# Kafka retention per partition × number of partitions = working set
# If working set < 56 GB: all consumer reads served from page cache (0 disk I/O)
# If working set > 56 GB: cache misses → disk reads → latency

# OOM protection for Kafka:
echo -900 > /proc/$(pgrep -f kafka.Kafka)/oom_score_adj
# Persistent in systemd unit:
# [Service]
# OOMScoreAdjust=-900

# Monitor Kafka memory health:
KAFKA_PID=$(pgrep -f kafka.Kafka)
echo "Kafka RSS: $(cat /proc/$KAFKA_PID/status | grep VmRSS | awk '{printf "%.1f GB\n", $2/1048576}')"
echo "Kafka Swap: $(cat /proc/$KAFKA_PID/status | grep VmSwap | awk '{printf "%.1f MB\n", $2/1024}')"
# VmSwap > 0 for Kafka: critical issue — Kafka pages swapped out = latency spike when accessed
```

### 14.4 Memory Monitoring Checklist for Data Engineering Servers

```bash
#!/bin/bash
# de_memory_check.sh — quick memory health check for data engineering servers

echo "=== Memory Health Check: $(hostname) ==="
echo ""

# 1. Available memory
MEM_TOTAL=$(grep MemTotal /proc/meminfo | awk '{print $2}')
MEM_AVAIL=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
AVAIL_PCT=$((MEM_AVAIL * 100 / MEM_TOTAL))
echo "1. Available: ${MEM_AVAIL} kB (${AVAIL_PCT}% of ${MEM_TOTAL} kB)"
[ $AVAIL_PCT -lt 10 ] && echo "   ⚠️  CRITICAL: < 10% available"
[ $AVAIL_PCT -lt 20 ] && [ $AVAIL_PCT -ge 10 ] && echo "   ⚠️  WARN: < 20% available"

# 2. Swap usage
SWAP_TOTAL=$(grep SwapTotal /proc/meminfo | awk '{print $2}')
SWAP_FREE=$(grep SwapFree /proc/meminfo | awk '{print $2}')
SWAP_USED=$((SWAP_TOTAL - SWAP_FREE))
echo "2. Swap used: ${SWAP_USED} kB of ${SWAP_TOTAL} kB"
[ $SWAP_USED -gt 0 ] && echo "   ℹ️  Some pages are in swap"

# 3. Active swapping right now
SI_SO=$(vmstat 1 1 | tail -1 | awk '{print $7+$8}')
echo "3. Active swap I/O: ${SI_SO} KB/s"
[ $SI_SO -gt 0 ] && echo "   ⚠️  ACTIVE SWAPPING DETECTED"

# 4. OOM recent events
OOM_COUNT=$(dmesg --since "1 hour ago" 2>/dev/null | grep -c "Out of memory" || echo 0)
echo "4. OOM kills (last hour): ${OOM_COUNT}"
[ "$OOM_COUNT" -gt 0 ] && dmesg --since "1 hour ago" 2>/dev/null | grep "Out of memory"

# 5. Java process RSS summary
echo "5. Java process memory:"
ps -eo pid,user,rss,comm | awk '$4=="java" {printf "   PID %d (%s): %.1f GB RSS\n", $1, $2, $3/1048576}'

# 6. Kafka OOM score
KAFKA_PID=$(pgrep -f kafka.Kafka 2>/dev/null)
if [ -n "$KAFKA_PID" ]; then
    KAFKA_OOM=$(cat /proc/$KAFKA_PID/oom_score 2>/dev/null)
    KAFKA_ADJ=$(cat /proc/$KAFKA_PID/oom_score_adj 2>/dev/null)
    echo "6. Kafka OOM score: ${KAFKA_OOM} (adj: ${KAFKA_ADJ})"
    [ "$KAFKA_ADJ" -gt -100 ] && echo "   ⚠️  Kafka not OOM-protected (oom_score_adj should be -900)"
fi

echo ""
echo "=== End Check ==="
```

---

## 15. Code Toolkit

### 15.1 `memory_monitor.py` — Comprehensive Memory Diagnostics

```python
"""
memory_monitor.py — Memory diagnostics for data engineering servers.

Functions:
  1. parse_meminfo()          — parse /proc/meminfo into a dict
  2. memory_summary()         — human-readable memory overview
  3. process_memory()         — per-process RSS, PSS, swap, OOM score
  4. get_oom_candidates()     — list processes sorted by OOM score
  5. check_swap_activity()    — detect active swapping via vmstat
  6. watch_memory()           — continuous memory monitoring loop
  7. memory_health_report()   — full diagnostic report

Run directly for a comprehensive memory health report.
"""
from __future__ import annotations
import os
import re
import time
import subprocess
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


# ── /proc/meminfo parsing ──────────────────────────────────────────────────────

def parse_meminfo() -> dict[str, int]:
    """Parse /proc/meminfo into {key: value_in_kB}."""
    data: dict[str, int] = {}
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    key = parts[0].rstrip(":")
                    try:
                        data[key] = int(parts[1])
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass
    return data


def memory_summary(meminfo: Optional[dict[str, int]] = None) -> dict[str, float]:
    """Return key memory metrics in GB."""
    m = meminfo or parse_meminfo()
    kb = 1024
    
    return {
        "total_gb":       m.get("MemTotal",        0) / kb / kb,
        "available_gb":   m.get("MemAvailable",    0) / kb / kb,
        "free_gb":        m.get("MemFree",         0) / kb / kb,
        "anon_gb":        m.get("AnonPages",       0) / kb / kb,
        "cache_gb":       (m.get("Cached", 0) + m.get("Buffers", 0)) / kb / kb,
        "active_file_gb": m.get("Active(file)",    0) / kb / kb,
        "inactive_file_gb": m.get("Inactive(file)", 0) / kb / kb,
        "dirty_mb":       m.get("Dirty",           0) / kb,
        "writeback_mb":   m.get("Writeback",       0) / kb,
        "swap_total_gb":  m.get("SwapTotal",       0) / kb / kb,
        "swap_free_gb":   m.get("SwapFree",        0) / kb / kb,
        "swap_used_gb":   (m.get("SwapTotal", 0) - m.get("SwapFree", 0)) / kb / kb,
        "slab_gb":        m.get("Slab",            0) / kb / kb,
        "committed_as_gb": m.get("Committed_AS",  0) / kb / kb,
        "available_pct":  (m.get("MemAvailable", 0) / m.get("MemTotal", 1)) * 100,
    }


# ── Per-process memory ──────────────────────────────────────────────────────────

@dataclass
class ProcessMemory:
    pid: int
    name: str
    user: str
    rss_mb: float           # RSS from /proc/<pid>/status
    vsz_mb: float           # VSZ from /proc/<pid>/status
    rss_anon_mb: float      # anonymous RSS (heap/stack)
    rss_file_mb: float      # file-backed RSS (mmap'd files)
    swap_mb: float          # pages in swap
    oom_score: int          # 0-1000; higher = more likely to be OOM-killed
    oom_score_adj: int      # -1000 to +1000; adjustment
    threads: int


def get_process_memory(pid: int) -> Optional[ProcessMemory]:
    """Read memory info for a single process from /proc/<pid>/status."""
    try:
        with open(f"/proc/{pid}/status") as f:
            status = dict(
                line.split(":\t", 1)
                for line in f.read().splitlines()
                if ":\t" in line
            )
    except (FileNotFoundError, PermissionError):
        return None
    
    # Parse user
    uid = int(status.get("Uid", "0").split()[0])
    try:
        import pwd
        user = pwd.getpwuid(uid).pw_name
    except (KeyError, ImportError):
        user = str(uid)
    
    def kb(key: str) -> float:
        val = status.get(key, "0 kB").split()[0]
        return int(val) / 1024  # convert kB to MB
    
    # OOM scores
    try:
        oom_score = int(Path(f"/proc/{pid}/oom_score").read_text().strip())
    except Exception:
        oom_score = -1
    try:
        oom_score_adj = int(Path(f"/proc/{pid}/oom_score_adj").read_text().strip())
    except Exception:
        oom_score_adj = 0
    
    return ProcessMemory(
        pid=pid,
        name=status.get("Name", "unknown"),
        user=user,
        rss_mb=kb("VmRSS"),
        vsz_mb=kb("VmSize"),
        rss_anon_mb=kb("RssAnon"),
        rss_file_mb=kb("RssFile"),
        swap_mb=kb("VmSwap"),
        oom_score=oom_score,
        oom_score_adj=oom_score_adj,
        threads=int(status.get("Threads", 1)),
    )


def all_process_memory() -> list[ProcessMemory]:
    """Get memory info for all processes."""
    procs = []
    for entry in os.scandir("/proc"):
        if entry.name.isdigit():
            pm = get_process_memory(int(entry.name))
            if pm:
                procs.append(pm)
    return procs


def get_oom_candidates(top_n: int = 10) -> list[ProcessMemory]:
    """Return top N processes sorted by OOM score (most likely to be killed first)."""
    procs = all_process_memory()
    procs = [p for p in procs if p.oom_score >= 0]
    return sorted(procs, key=lambda p: p.oom_score, reverse=True)[:top_n]


# ── Swap activity (via vmstat) ──────────────────────────────────────────────────

def check_swap_activity(samples: int = 3, interval_s: float = 1.0) -> dict[str, float]:
    """Check for active swap I/O by sampling /proc/vmstat."""
    def read_vmstat_field(field: str) -> int:
        try:
            with open("/proc/vmstat") as f:
                for line in f:
                    if line.startswith(field + " "):
                        return int(line.split()[1])
        except Exception:
            pass
        return 0
    
    pswpin_before = read_vmstat_field("pswpin")
    pswpout_before = read_vmstat_field("pswpout")
    
    time.sleep(interval_s * samples)
    
    pswpin_after = read_vmstat_field("pswpin")
    pswpout_after = read_vmstat_field("pswpout")
    
    elapsed = interval_s * samples
    page_size_kb = os.sysconf("SC_PAGE_SIZE") // 1024
    
    # Pages per second → KB/s
    si_kbps = (pswpin_after - pswpin_before) * page_size_kb / elapsed
    so_kbps = (pswpout_after - pswpout_before) * page_size_kb / elapsed
    
    return {
        "swap_in_kbps": si_kbps,
        "swap_out_kbps": so_kbps,
        "active_swapping": si_kbps > 0 or so_kbps > 0,
    }


# ── Main report ────────────────────────────────────────────────────────────────

def memory_health_report(protect_pids: Optional[dict[int, str]] = None) -> None:
    """
    Print a comprehensive memory health report.
    
    Args:
        protect_pids: {pid: "service_name"} — processes to check OOM protection for
    """
    print("=" * 70)
    print("Memory Health Report")
    print("=" * 70)
    
    # 1. System memory overview
    m = parse_meminfo()
    s = memory_summary(m)
    print(f"\n1. System Memory")
    print(f"   Total:      {s['total_gb']:>6.1f} GB")
    print(f"   Available:  {s['available_gb']:>6.1f} GB  ({s['available_pct']:.0f}%)")
    print(f"   Anon (RSS): {s['anon_gb']:>6.1f} GB  (process heaps + stacks)")
    print(f"   Page cache: {s['cache_gb']:>6.1f} GB  (file I/O cache)")
    print(f"     Active:   {s['active_file_gb']:>6.1f} GB  (recently used)")
    print(f"     Inactive: {s['inactive_file_gb']:>6.1f} GB  (evictable)")
    print(f"   Dirty:      {s['dirty_mb']:>6.0f} MB  (pending write to disk)")
    
    avail_pct = s["available_pct"]
    if avail_pct < 10:
        print(f"   ⚠️  CRITICAL: Only {avail_pct:.0f}% available")
    elif avail_pct < 20:
        print(f"   ⚠️  WARN: Only {avail_pct:.0f}% available")
    else:
        print(f"   ✓  Healthy availability ({avail_pct:.0f}%)")
    
    # 2. Swap
    print(f"\n2. Swap")
    if s["swap_total_gb"] == 0:
        print(f"   No swap configured")
    else:
        print(f"   Total: {s['swap_total_gb']:.1f} GB  |  Used: {s['swap_used_gb']:.1f} GB  |  Free: {s['swap_free_gb']:.1f} GB")
        swap_pct = (s["swap_used_gb"] / s["swap_total_gb"]) * 100 if s["swap_total_gb"] > 0 else 0
        if swap_pct > 80:
            print(f"   ⚠️  WARN: Swap {swap_pct:.0f}% used — nearly exhausted")
        elif swap_pct > 0:
            print(f"   ℹ️  Some swap in use ({swap_pct:.0f}%)")
    
    # 3. Active swapping
    print(f"\n3. Swap Activity (sampling 3s)")
    swap_act = check_swap_activity(samples=3, interval_s=1.0)
    if swap_act["active_swapping"]:
        print(f"   ⚠️  ACTIVE SWAPPING: in={swap_act['swap_in_kbps']:.0f} KB/s  out={swap_act['swap_out_kbps']:.0f} KB/s")
    else:
        print(f"   ✓  No active swap I/O")
    
    # 4. Top processes by RSS
    print(f"\n4. Top 10 Processes by RSS")
    procs = sorted(all_process_memory(), key=lambda p: p.rss_mb, reverse=True)[:10]
    print(f"   {'PID':>7}  {'Name':<16}  {'User':<10}  {'RSS':>7}  {'Anon':>7}  {'Swap':>6}  {'OOM':>4}")
    for p in procs:
        swap_str = f"{p.swap_mb:.0f}MB" if p.swap_mb > 0 else "  —"
        print(f"   {p.pid:>7}  {p.name:<16}  {p.user:<10}  "
              f"{p.rss_mb:>5.0f}MB  {p.rss_anon_mb:>5.0f}MB  "
              f"{swap_str:>6}  {p.oom_score:>4}")
    
    # 5. OOM candidates
    print(f"\n5. Top OOM Candidates (most likely to be killed)")
    candidates = get_oom_candidates(top_n=5)
    for p in candidates:
        adj_str = f"adj={p.oom_score_adj}" if p.oom_score_adj != 0 else ""
        print(f"   [{p.oom_score:>3}] {p.name}({p.pid}) — {p.rss_mb:.0f} MB RSS  {adj_str}")
    
    # 6. OOM protection check
    if protect_pids:
        print(f"\n6. OOM Protection Check")
        for pid, service_name in protect_pids.items():
            pm = get_process_memory(pid)
            if pm:
                status_str = "✓ Protected" if pm.oom_score_adj <= -500 else "⚠️  Unprotected"
                print(f"   {service_name}({pid}): score={pm.oom_score} adj={pm.oom_score_adj} → {status_str}")
            else:
                print(f"   {service_name}({pid}): ✗ Process not found")
    
    # 7. Huge pages / THP
    thp = Path("/sys/kernel/mm/transparent_hugepage/enabled")
    if thp.exists():
        thp_val = thp.read_text().strip()
        thp_active = "[always]" in thp_val
        print(f"\n7. Transparent Huge Pages: {thp_val}")
        if thp_active:
            print(f"   ℹ️  THP is enabled — consider disabling for Kafka/Spark (reduces latency jitter)")
    
    # 8. Recent OOM kills
    try:
        result = subprocess.run(
            ["dmesg", "--since", "1 hour ago"],
            capture_output=True, text=True, timeout=5
        )
        oom_lines = [l for l in result.stdout.splitlines() if "Out of memory" in l or "OOM" in l]
        print(f"\n8. Recent OOM Events (last hour): {len(oom_lines)}")
        for line in oom_lines[-3:]:
            print(f"   {line.strip()}")
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print(f"\n8. OOM Events: (dmesg unavailable)")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    import os
    # Find Kafka and Spark pids if running
    protect = {}
    
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        try:
            cmdline = Path(f"/proc/{entry.name}/cmdline").read_bytes().replace(b"\x00", b" ").decode()
            pid = int(entry.name)
            if "kafka.Kafka" in cmdline:
                protect[pid] = "kafka"
            elif "CoarseGrainedExecutorBackend" in cmdline:
                protect[pid] = "spark-executor"
        except Exception:
            pass
    
    memory_health_report(protect_pids=protect if protect else None)
```

---

## 16. Hands-On Labs

### Lab 1: Observe Memory Pressure Under Allocation

```bash
# Lab 1: deliberately consume memory and watch /proc/meminfo respond

# Create a Python script that allocates memory incrementally
cat > /tmp/mem_consumer.py << 'EOF'
import time
import sys

chunk_mb = int(sys.argv[1]) if len(sys.argv) > 1 else 100
hold_s = int(sys.argv[2]) if len(sys.argv) > 2 else 10

print(f"Allocating {chunk_mb} MB and holding for {hold_s}s...")
data = bytearray(chunk_mb * 1024 * 1024)  # allocate
# Touch all pages (force physical allocation)
for i in range(0, len(data), 4096):
    data[i] = 1
print(f"Allocated. Watching for {hold_s}s...")
time.sleep(hold_s)
print("Released.")
EOF

# Terminal 1: watch available memory
watch -n 0.5 'grep -E "MemAvailable|AnonPages|Cached:|Dirty:" /proc/meminfo'

# Terminal 2: allocate 2 GB in 500 MB chunks
for mb in 500 500 500 500; do
    python3 /tmp/mem_consumer.py $mb 5 &
    sleep 1
done
wait

# Observe: AnonPages grows, MemAvailable shrinks
# After scripts finish: AnonPages shrinks, MemAvailable recovers

rm /tmp/mem_consumer.py
```

### Lab 2: Observe and Interpret OOM Scores

```bash
# Lab 2: inspect OOM scores for running processes

# Print OOM score, adj, PID, name, RSS for top 15 by RSS
ps -eo pid,rss,comm | sort -k2 -rn | head -15 | while read pid rss comm; do
    score=$(cat /proc/$pid/oom_score 2>/dev/null || echo "?")
    adj=$(cat /proc/$pid/oom_score_adj 2>/dev/null || echo "?")
    echo "OOM=$score adj=$adj PID=$pid RSS=${rss}kB name=$comm"
done | sort -rn | head -15

# Set OOM protection for THIS shell process (demo):
echo -500 > /proc/$$/oom_score_adj
cat /proc/$$/oom_score
cat /proc/$$/oom_score_adj
# Then reset:
echo 0 > /proc/$$/oom_score_adj
```

---

## 17. Interview Q&A

**Q1: `free -h` shows 1 GB "free" on a 64 GB server. Is the server out of memory?**

Almost certainly not. The "free" column in `free -h` shows only memory with no pages mapped whatsoever — true empty pages. On any active Linux server, this number is nearly always small because the kernel aggressively fills unused RAM with the page cache. File reads, writes, and mmapped files all consume page cache; the kernel would rather use RAM as a disk cache than leave it idle.

The correct column to watch is "available" — not "free." `MemAvailable` is the kernel's estimate of how much memory can be given to new applications without pushing any existing work to swap. It counts truly free pages PLUS page cache pages that are reclaimable (inactive file pages that can be evicted and re-read from disk on demand). On a server with 1 GB "free" and 50 GB of page cache, MemAvailable might be 45 GB — the server is completely healthy. The right alert threshold is `MemAvailable < 10% of MemTotal`, not `MemFree < some threshold`.

**Q2: What is the difference between RSS and VSZ, and why does a Java process with `-Xmx4g` have VSZ of 8 GB?**

**VSZ** (Virtual Size) is the total size of a process's virtual address space — all the memory the process has mapped, whether or not physical pages are currently backing it. It includes the JVM heap (`-Xmx4g`), the JVM's own metaspace and code cache, all memory-mapped JAR files and native libraries, guard pages, and the Java thread stacks. **RSS** (Resident Set Size) is the subset of virtual mappings that are currently backed by physical pages — the RAM actually in use.

A JVM with `-Xmx4g` has VSZ of 8 GB or more because: (1) The JVM reserves the full `-Xmx` virtual space at startup — all 4 GB is mapped in the virtual address space even though only a fraction has been allocated as Java objects yet. (2) JVM metaspace (class metadata), code cache (JIT output), and Netty/NIO buffers add several hundred MB each. (3) Every loaded JAR file is mmapped — a complex application with 200 JARs maps several hundred MB of additional virtual space. (4) The JVM reserves address space for TLAB (Thread Local Allocation Buffers) and other internal structures. VSZ is mostly irrelevant as a resource concern; RSS is what actually consumes physical RAM.

**Q3: Explain the OOM killer: how does it choose its victim?**

The OOM killer runs when the kernel cannot satisfy a page allocation after exhausting all reclamation paths: evicting page cache, freeing slab caches, and checking swap — all insufficient. At that point it must free memory by killing a process.

Victim selection uses `oom_score`, a 0-1000 value calculated as approximately `(RSS + swap_used) / total_RAM × 1000`. A process using 50% of total RAM scores ~500. This raw score is then adjusted by `oom_score_adj` (range -1000 to +1000), which operators can set per-process. A Kafka broker with `oom_score_adj = -900` would score 500 - 900 = effectively -400, which gets clamped to 0 — the OOM killer will skip it and find another victim. A process with `oom_score_adj = +1000` is always killed first regardless of its memory usage. The process with the highest final `oom_score` is sent SIGKILL by the OOM killer.

A critical nuance: the kernel does NOT send SIGTERM first. The OOM killer sends SIGKILL directly — the process has no chance to run cleanup handlers, flush state, or checkpoint data. This is why Kafka's `oom_score_adj = -900` is critical: if Kafka is OOM-killed mid-write to the page cache, those uncommitted log segments may be lost.

**Q4: What is `vm.swappiness` and what value should you set for a Kafka broker?**

`vm.swappiness` controls how aggressively the Linux kernel trades off between two reclamation strategies when it needs to free memory: evicting file-backed page cache (readable back from disk) versus swapping anonymous process pages (readable back from swap). At `swappiness=0`, the kernel avoids swap until absolutely necessary, preferring to evict page cache first. At `swappiness=100`, the kernel swaps aggressively, often swapping process pages even when page cache could have been evicted instead.

For a Kafka broker, `vm.swappiness=1` is the recommended value. Kafka's performance depends on page cache: Kafka writes to the page cache and relies on the OS to serve consumer reads from cached log segments. If the kernel is aggressive about evicting page cache (high swappiness), consumer reads start hitting disk instead of cache, causing latency to spike. By setting swappiness low, the kernel preserves the page cache longer. The tradeoff is that if memory truly runs out, process pages get OOM-killed rather than swapped out — but for Kafka, a clean OOM kill of a co-located process is better than having Kafka's page cache thrashed. The correct defense against Kafka itself being OOM-killed is `oom_score_adj = -900`, not relying on swap to save it.

**Q5: A Spark executor on Kubernetes exits with code 137. What happened and how do you investigate?**

Exit code 137 is always 128 + 9, meaning the process received SIGKILL (signal 9) from outside itself. In a container context, this is the OOM killer — not a graceful shutdown. Kubernetes's OOM killer fires when a container's RSS exceeds the Pod's `resources.limits.memory` value. The kernel sends SIGKILL to the largest-RSS process in the cgroup (which is the JVM thread that tried to allocate the page that exceeded the limit).

Investigate in order: `kubectl describe pod <pod-name>` shows `OOMKilled: true` in the container's last state with exit code 137. The `resources.limits.memory` in the Pod spec shows the configured limit. The actual peak RSS is visible from `kubectl top pod <pod-name>` during the job, or from the Spark UI's executor metrics (look at "Peak Memory Used" vs the configured limit). Common root causes: `spark.executor.memoryOverhead` was not set (the default 10% is often too low for Python UDFs or complex Spark plans), the shuffle write buffer is larger than anticipated, or the task's data is skewed and one partition is far larger than expected. Fix by increasing `spark.executor.memoryOverhead` to 20-40% of `spark.executor.memory`, enabling off-heap memory, reducing partition size, or increasing the Pod memory limit.

**Q6: What is PSS, and why is it more useful than RSS for measuring total memory usage on a server with many JVM processes?**

**PSS (Proportional Set Size)** divides shared memory pages proportionally among all processes that have them mapped, while RSS counts shared pages fully for each process. When 10 JVM processes all load the same JDK runtime JARs (say 500 MB of shared mmapped files), RSS counts those 500 MB for each of the 10 JVMs — RSS sum is 5 GB for what is actually 500 MB of physical RAM. PSS counts 50 MB per JVM (500 MB / 10) — PSS sum is 500 MB, which matches reality.

This matters when planning server capacity: if you sum RSS across all processes and the total exceeds physical RAM, you might panic — but PSS sum would show you're actually within capacity because the apparent overuse is just shared libraries counted multiple times. PSS is read from `/proc/<pid>/smaps_rollup` (the `Pss:` line) or `/proc/<pid>/smaps` (summed across all regions). Tools like `smem` automate this accounting. For capacity planning, `sum(PSS) + kernel_memory` gives the actual physical memory requirement. A server can safely run `sum(RSS) > MemTotal` as long as `sum(PSS) + kernel_memory < MemTotal`.

---

## 18. Cross-Question Chain

**Q1 [Interviewer]: A Spark job fails with ExecutorLostFailure. Logs show the executor JVM process simply disappears with no Java exception. What's your first hypothesis?**

My first hypothesis is an OOM kill. When a JVM is killed by SIGKILL (whether from the OOM killer, Kubernetes, or a human), it dies instantly — no Java exception, no stack trace, no graceful shutdown. The JVM process simply vanishes. I'd check `dmesg | grep -i "kill\|oom"` on the executor's host to find an OOM kill message, and in Kubernetes I'd run `kubectl describe pod <pod>` to check for `OOMKilled: true` and exit code 137.

**Q2 [Interviewer]: `kubectl describe pod` shows `OOMKilled: true`. The Spark executor was configured with `spark.executor.memory=8g`. You have a 64 GB node with 8 executor Pods. 8 × 8 GB = 64 GB — it should fit. Why did one get OOM-killed?**

Because `spark.executor.memory=8g` configures only the JVM heap — not the executor's total memory footprint. Each JVM process consumes additional memory beyond the heap: JVM metaspace and code cache (~300-500 MB), JVM thread stacks (~8 KB × thread count), off-heap shuffle buffers and Netty network buffers (can be hundreds of MB under load), and native libraries loaded by the JVM. Kubernetes must be configured with `spark.executor.memoryOverhead` to account for this overhead. The default is `max(384 MB, 10% of executor.memory)` = 800 MB. So the real per-executor memory is 8 GB + 0.8 GB = 8.8 GB. For 8 executors: 8 × 8.8 = 70.4 GB — exceeding the 64 GB node. The Kubernetes Pod limit should be set to at least `executor.memory + memoryOverhead`, and the node's allocatable memory is typically less than its physical RAM due to OS/kubelet overhead.

**Q3 [Interviewer]: You increase `memoryOverhead` to 2 GB per executor (8 × 10 GB = 80 GB total) and reduce to 6 executors (6 × 10 GB = 60 GB). The job still occasionally OOM-kills executors on the largest reduce stage. What now?**

Data skew is the most likely culprit if it's only one executor and only on the reduce stage. I'd check the Spark UI's Stage details: if one task is processing 10× more data than others, its shuffle read buffer and sort/aggregation memory will be 10× larger. Run `df -h /tmp` on the executor host during the failure to see if it's spilling first, then check `spark.sql.adaptive.skewJoin.enabled` — if it's false, enable it. AQE (Adaptive Query Execution) in Spark 3+ detects skewed partitions and splits them. If AQE is already enabled, I'd look at what's in the `sql.shuffle.partitions` setting — too few partitions create large individual partition sizes. Increasing `spark.sql.shuffle.partitions` from the default 200 to 1000+ reduces per-partition size proportionally.

**Q4 [Interviewer]: It's a genuine skew problem — one key appears in 30% of the rows. AQE is enabled but the executor still OOM-kills. Walk through the memory math for that skewed executor.**

With 6 executors on a dataset of 1 TB and a skewed key at 30%, the skewed partition is 300 GB of data flowing to one executor. Even if AQE splits this into smaller sub-partitions, each sub-partition must fit in execution memory. If `spark.executor.memory=8g` and `spark.memory.fraction=0.6`, Spark's unified memory is 4.8 GB, split between execution and storage. The execution memory available for one task's sort buffer might be ~2 GB. A 300 GB skewed partition even after AQE splitting into 10 sub-partitions = 30 GB each — each sub-partition still can't fit in 2 GB and will spill to disk. If the shuffle spill write rate exceeds the available disk or causes too many small writes, the executor might also hit an OutOfMemory error from Java's shuffle write bookkeeping structures accumulating.

The correct fix: pre-salt the skewed key. Replace `key` with `key + "_" + FLOOR(RAND() * 10)` in the source data (adding a random suffix), increasing the key cardinality from 1 skewed value to 10 values each with 3% of the data. This distributes the work across 10× more partitions, none of which are hot. This is a data transformation fix — no amount of memory tuning fully solves a 30% skew without salting.

**Q5 [Interviewer]: The salting fix works but you now notice the server (which co-locates Kafka) is experiencing Kafka consumer lag spikes during the Spark job. The Spark executors are memory-limited with Kubernetes. What's the connection?**

Even with Kubernetes memory limits, Spark executors' page cache use is NOT limited by the Pod memory limit. The `resources.limits.memory` in Kubernetes limits the cgroup's anonymous (RSS) memory — not file-backed page cache. Spark's shuffle spill files and input Parquet files, when read, populate the node's page cache outside the Pod's cgroup. During the large reduce stage, Spark reads terabytes of shuffle data from disk — this populates the page cache, evicting Kafka's log segment pages that were previously cached. The Kafka broker (also on this node) then starts seeing cache misses: consumer reads that were previously served from cache now hit disk. This shows as latency spikes in Kafka's produce/consume metrics even though Kafka's own JVM is healthy.

The real fix is node isolation: Kafka brokers should not co-locate with Spark executors in production. If co-location is a cost constraint, use Linux cgroups memory.memsw.limit to limit the executor's total memory including page cache — though this is complex to configure outside Kubernetes. The simpler operational fix is scheduling Spark jobs during low Kafka traffic periods and monitoring `Active(file)` in `/proc/meminfo` — if it drops sharply, Kafka's page cache is being evicted.

**Q6 [Interviewer]: After separating Kafka to its own nodes, you still see occasional memory pressure on the Kafka node. free shows MemAvailable < 5 GB out of 64 GB. What would you check and how would you tune?**

First, identify what's consuming memory. `ps -eo pid,rss,comm | sort -k2 -rn | head -10` shows the largest processes. If Kafka's RSS is 8 GB (4 GB heap + overhead) and there are monitoring agents, syslog daemons, and other processes adding another 3-4 GB, total anonymous RSS is ~12 GB. With 64 GB total, 52 GB should be page cache — if MemAvailable is only 5 GB, either the page cache is very hot (unlikely to free) or there's another consumer. Check `AnonPages` in `/proc/meminfo` — if it's 58 GB+, some process is holding a large anonymous mapping.

For tuning: lower `vm.dirty_ratio` from 20 to 5 so the kernel starts flushing dirty pages earlier, preventing a large buildup. Set `vm.min_free_kbytes=2097152` (2 GB minimum free) to force the kernel to maintain a larger free pool, starting reclaim earlier. Verify THP is disabled: `cat /sys/kernel/mm/transparent_hugepage/enabled` should be `never` — THP compaction can temporarily spike memory usage. Set `oom_score_adj=-900` for Kafka so any monitoring agent gets killed first. And finally, ensure the monitoring stack (Prometheus node_exporter, Filebeat, etc.) has strict RSS limits to prevent it from competing with Kafka for page cache.

---

## 19. Common Misconceptions

**"The 'used' memory in `free` output is how much applications are using."**
The "used" column in `free` output is `total - free - buff/cache`. This means it explicitly excludes the page cache. The page cache is managed by the kernel on behalf of all applications — it's the file I/O buffer. A Kafka server with 4 GB JVM heap and 55 GB of page cache would show "used = 4 GB" (not 59 GB) in `free`'s used column, which is misleading — the page cache IS being actively used by Kafka for I/O, just not counted in "used." The meaningful number is `MemAvailable`, not `free`'s "used."

**"If swap is configured on a Kafka/Spark server, it will be used and hurt performance."**
Swap being present doesn't mean it will be used. With `vm.swappiness=1`, the kernel almost never voluntarily swaps process pages — it only uses swap as a last resort when both page cache eviction and all other reclamation strategies have failed. Having a small swap (4-8 GB) on a 64 GB Kafka server provides a safety net against sudden OOM kills during unexpected memory spikes, without impacting normal operation. The anti-pattern is having swap on HDDs and having it actively used — 10+ ms per page fault is catastrophic. Swap on SSD/NVMe at low swappiness is a reasonable safety net.

**"A process with large VSZ is consuming a lot of memory."**
VSZ is virtual address space — it costs almost nothing. A Java process with `-Xmx32g` has VSZ of 32+ GB but might have RSS of only 4 GB if the application is small. Linux's demand paging means virtual address space is allocated without physical pages. The physical memory cost is RSS (physical pages currently resident) not VSZ. Alerting on VSZ is meaningless; alert on RSS and on the system-level MemAvailable.

**"The OOM killer sends SIGTERM before SIGKILL."**
The OOM killer sends only SIGKILL — immediately, without warning. There is no SIGTERM phase, no cleanup window, no graceful shutdown. The process receives SIGKILL and dies at the next opportunity (which is effectively instantly). This is why `oom_score_adj = -900` for Kafka is important: Kafka has no chance to checkpoint or flush data when OOM-killed. Any uncommitted data in the page cache is lost only if the machine loses power; if the machine stays up, the kernel finishes flushing the page cache independently.

---

## 20. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What does `MemAvailable` in /proc/meminfo represent? | The kernel's estimate of RAM available to start new applications without swapping. Includes truly free pages + reclaimable page cache. Use this for alerting, NOT `MemFree`. Alert when < 10% of MemTotal. |
| 2 | What is the difference between RSS and VSZ? | RSS (Resident Set Size) = physical pages currently in RAM for this process. VSZ (Virtual Size) = total virtual address space claimed (including unallocated heap). A JVM with `-Xmx32g` has VSZ ≈ 32 GB but RSS = actual heap usage. |
| 3 | What is `AnonPages` in /proc/meminfo? | Total physical pages used by process anonymous memory — heap, stack, mmap(MAP_ANONYMOUS) — across all processes. This is the total process RAM usage. Growing AnonPages means more process memory is being allocated. |
| 4 | What does `si` and `so` mean in vmstat? | `si` = swap in (KB/s being read FROM swap to RAM). `so` = swap out (KB/s being written TO swap FROM RAM). Both > 0 = ACTIVE SWAPPING — serious performance problem. |
| 5 | How does the OOM killer choose its victim? | Sorts processes by `oom_score` (approximately RSS / total_RAM × 1000) and kills the highest scorer. Adjusted by `oom_score_adj` (-1000 to +1000). Sends SIGKILL directly — no SIGTERM warning. |
| 6 | What is `oom_score_adj = -900` used for? | Protects a critical process (e.g., Kafka broker) from OOM kill. Score adjusted down by 900 points, making it nearly immune unless no other process is available to kill. Set in systemd unit: `OOMScoreAdjust=-900`. |
| 7 | What exit code indicates an OOM kill? | 137 (= 128 + 9). A process killed with SIGKILL exits with code 128 + signal_number. SIGKILL = signal 9 → exit code 137. Verify: `kubectl describe pod` shows `OOMKilled: true`. |
| 8 | What is `vm.swappiness` and what value for Kafka? | Controls preference for swapping anonymous pages vs evicting page cache. 0=never swap voluntarily, 60=default, 100=aggressive swap. For Kafka: set to 1 to preserve page cache while still having swap as a safety net. |
| 9 | What is PSS (Proportional Set Size)? | Like RSS but shared pages are divided proportionally among all processes that map them. More accurate than RSS for measuring true per-process memory footprint. Read from /proc/<pid>/smaps_rollup. |
| 10 | What does `Dirty:` in /proc/meminfo measure? | Page cache pages that have been modified but not yet written to disk. Growing Dirty count means writes are accumulating faster than the kernel flushes them. If it hits `vm.dirty_ratio` × MemTotal, writes block until flushed. |
| 11 | What is `vm.overcommit_memory=0` (default)? | Heuristic overcommit: the kernel permits moderate overcommit. `malloc(32 GB)` succeeds even if only 16 GB RAM is available, because Linux uses demand paging — physical pages are only allocated when touched. OOM kill fires when physical pages run out. |
| 12 | What is Transparent Huge Page (THP) and why disable it for Kafka? | THP automatically promotes 4 KB pages to 2 MB huge pages to reduce TLB pressure. The `khugepaged` daemon periodically compacts memory to create huge pages, causing brief stalls. For Kafka (low-latency requirement), this causes unpredictable latency spikes. Disable: `echo never > /sys/kernel/mm/transparent_hugepage/enabled`. |
| 13 | What does `Committed_AS` in /proc/meminfo represent? | Total virtual memory that has been committed across all processes — the sum of all VSZ. If `Committed_AS > MemTotal + SwapTotal`, the system is overcommitted and OOM kill is possible when enough pages are touched simultaneously. |
| 14 | What is `spark.executor.memoryOverhead`? | Extra memory beyond `spark.executor.memory` (JVM heap) for JVM overhead: metaspace, code cache, Netty buffers, native libraries. Default: max(384 MB, 10% of executor.memory). Must be included in Kubernetes Pod memory limits. |
| 15 | What does `kswapd` do? | The kernel's background memory reclaim daemon. Runs when free pages drop below the low watermark. Evicts inactive page cache and may swap out anonymous pages. High kswapd CPU usage = system under active memory pressure. |
| 16 | What is `Active(file)` vs `Inactive(file)` in /proc/meminfo? | Both are page cache (file-backed). Active(file) = recently accessed pages (harder to evict). Inactive(file) = not recently accessed (evicted first). Kafka's hot log segments should be in Active(file); if they're being evicted by competing workloads, consumer reads hit disk. |
| 17 | How do you clear swap without rebooting? | `swapoff -a && swapon -a`. This moves all swap-resident pages back to RAM. Only safe if `MemAvailable > current swap used`. Forces the kernel to find RAM for all swapped pages; can trigger OOM kill if RAM is insufficient. |
| 18 | What is PSI (Pressure Stall Information)? | Linux 4.20+ metric measuring fraction of time tasks were stalled waiting for memory/CPU/IO. `cat /proc/pressure/memory` shows `some` (at least one task stalled) and `full` (all tasks stalled). More accurate memory pressure signal than `vmstat` si/so. |
| 19 | What is `Slab` in /proc/meminfo? | Kernel slab allocator memory — caches for frequently allocated kernel objects (inode structures, dentry cache, buffer_head). `SReclaimable` = can be freed under pressure. `SUnreclaim` = in use by kernel. High Slab growth often indicates inode/dentry cache growth from filesystem activity. |
| 20 | What command shows per-process memory with OOM score? | Read `/proc/<pid>/oom_score` and `/proc/<pid>/oom_score_adj` per process. No single built-in command shows both; use: `ps -eo pid,rss,comm | sort -k2 -rn | while read pid rss comm; do echo "$(cat /proc/$pid/oom_score 2>/dev/null) $pid $rss $comm"; done | sort -rn | head -10`. |

---

## 21. Module Summary

Memory on Linux is not what `free`'s "free" column suggests. The operating system's default behavior is to fill all unused RAM with page cache — treating empty RAM as wasted cache. A server showing 1 GB "free" and 55 GB "buff/cache" is typically healthy; `MemAvailable` is the correct metric for "how much more can I allocate before swapping."

The **Linux memory model** has two kinds of physical pages: anonymous (process heap/stack — tied to a process, must be swapped to free) and file-backed (page cache — cached from disk, can be evicted instantly by re-reading the file). The OOM killer fires when the kernel cannot satisfy a page fault by any combination of freeing page cache and swapping anonymous pages. It selects its victim by `oom_score` and sends SIGKILL immediately — no SIGTERM, no warning.

**`free -h`** is the quick snapshot: look at `available`, not `free` or `used`. **`/proc/meminfo`** is the authoritative source: `Active(file)` and `Inactive(file)` show the page cache split; `Dirty` shows pending writes; `AnonPages` shows total process heap usage; `SwapTotal` and `SwapFree` give swap state. **`vmstat 1`** is the activity monitor: `si` and `so` detect active swapping, `r` and `b` show CPU and I/O queue pressure, `wa` shows I/O wait.

For Kafka: set `vm.swappiness=1` to preserve page cache, set `OOMScoreAdjust=-900` to protect the broker from OOM kills, disable THP to prevent latency jitter, size the JVM heap conservatively (4-6 GB) to leave maximum RAM for page cache, and don't co-locate with memory-hungry batch workloads. For Spark on Kubernetes: always set `spark.executor.memoryOverhead` explicitly (20-40% of executor.memory), use AQE for skew handling, and set Pod memory limits to `executor.memory + memoryOverhead + buffer`.

---

**SYS-LNX-101: 4 of 5 complete.**  
**Next: SYS-LNX-101 M05 — CPU Diagnostics**  
*(perf, sar, mpstat, CPU throttling — diagnosing and tuning CPU behavior for data systems)*
