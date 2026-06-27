# SYS-LNX-101 M05: CPU Diagnostics

**Course:** SYS-LNX-101 — Linux for Data Engineers  
**Module:** 05 of 05  
**Filesystem position:** 01_Systems/M26_CPU_Diagnostics  
**Prerequisites:** SYS-LNX-101 M01–M04 (Linux File System, Process Management, Storage and I/O, Memory Diagnostics)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: CPU Architecture for Data Engineers](#4-core-theory-cpu-architecture-for-data-engineers)
5. [mpstat — Per-CPU Statistics](#5-mpstat--per-cpu-statistics)
6. [sar — System Activity Reporter](#6-sar--system-activity-reporter)
7. [perf — Linux Performance Profiling](#7-perf--linux-performance-profiling)
8. [CPU Throttling: Thermal and Power Limits](#8-cpu-throttling-thermal-and-power-limits)
9. [CPU Frequency Scaling and Governors](#9-cpu-frequency-scaling-and-governors)
10. [NUMA Architecture and Locality](#10-numa-architecture-and-locality)
11. [CPU Affinity and Isolation](#11-cpu-affinity-and-isolation)
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

A Kafka broker is using 80% CPU across all cores but network throughput is only 200 MB/s — well below the NIC's 10 Gbps capacity. The producer and consumer applications are reporting p99 latencies of 50 ms when baseline is 2 ms. You need to understand whether this CPU usage is productive work (serialization, compression, network syscalls) or wasted cycles (lock contention, excessive GC, cache misses, context switching) — and `top` alone cannot tell you.

A Spark job processes 1 TB of Parquet in 2 hours on one cluster but 4 hours on another cluster with twice as many cores. The hardware specs look identical. The difference turns out to be CPU frequency: the second cluster is running on shared VMs where the hypervisor's CPU steal time is 40% (`%st` in top), meaning 40% of wall-clock time the CPUs are physically executing another VM's workload. This is invisible from inside the application but measurable with `sar` and `mpstat`.

A Python Airflow task that runs a pandas aggregation takes 45 minutes. The CPU profiler reveals 80% of time is spent in `__hash__` inside a groupby operation — the Python dict accumulation is the bottleneck, not the data volume. `perf top` identifies this as user-space CPU time concentrated in a single hash function, pointing directly at the optimization target.

Understanding CPU behavior — where time goes, why frequency drops, how cores are utilized, and how NUMA topology affects memory access — is the final piece of the Linux systems puzzle for data engineering. This module covers it all.

---

## 2. How to Use This Module

**For incident response:** Section 5 (mpstat) for per-core utilization snapshots and identifying single-core saturation. Section 6 (sar) for historical CPU data — if the incident already passed, sar logs tell you what happened. Section 8 (CPU throttling) if performance degrades intermittently on the same hardware.

**For profiling and optimization:** Section 7 (perf) for identifying hot functions and cache misses in a running process. Section 14 (Data Engineering Connections) for JVM-specific profiling patterns.

**For infrastructure setup:** Sections 9 (CPU governors), 10 (NUMA), and 11 (CPU affinity) for correctly configuring hardware before deploying a production Kafka or Spark cluster.

**For interview prep:** Sections 4, 7, 8, 10, 17, and 18. CPU architecture (caches, NUMA, context switching) and the ability to describe a profiling workflow are high-signal staff-level topics.

---

## 3. Prerequisites Check

- **Processes and Threads (CSF-OS-101 M01):** Context switching, the scheduler, run queues, and the difference between user and kernel CPU time are all from M01 and appear throughout this module.
- **Process Management (SYS-LNX-101 M02):** `top`'s CPU columns (`us`, `sy`, `id`, `wa`, `st`), nice values, and process priority from M02 are prerequisite vocabulary.
- **Memory Diagnostics (SYS-LNX-101 M04):** NUMA memory allocation (Section 10 of this module) builds directly on the memory model from M04. CPU cache behavior connects to the page cache discussion from M03.

---

## 4. Core Theory: CPU Architecture for Data Engineers

### 4.1 The CPU Memory Hierarchy

Modern CPUs have multiple levels of cache between the processor and main RAM. Cache misses are the single most impactful performance variable for CPU-bound data processing:

```
CPU execution units (registers)
    ↕  ~0.3 ns (1 cycle at 3 GHz)
L1 cache: 32–64 KB per core, split I-cache/D-cache
    ↕  ~1 ns (3–4 cycles)
L2 cache: 256 KB–1 MB per core
    ↕  ~3–5 ns (10–15 cycles)
L3 cache: 8–64 MB shared across all cores on one socket
    ↕  ~10–40 ns (30–120 cycles)
DRAM (main memory): GB–TB capacity
    ↕  ~60–100 ns (200–300 cycles)
Local DRAM on other NUMA node:
    ↕  ~120–200 ns (400–600 cycles)
```

**Practical implication for data engineering:**

A row-based scan through 1 GB of data accesses memory sequentially → hardware prefetcher predicts the pattern → L1/L2 cache hit rate approaches 100% → ~3 ns per element. The same data in columnar format (Parquet, Arrow) processes even faster because fewer columns are loaded.

A hash join with a 32 MB hash table fits in L3 cache → good performance. A 512 MB hash table doesn't fit → each probe is an L3 miss → DRAM access at 60-100 ns per probe → 20-30× slower per probe than cache-resident. This is why Spark's broadcast hash join threshold (`spark.sql.autoBroadcastJoinThreshold`, default 10 MB) exists — above that threshold, broadcast joins cause L3 cache thrashing.

### 4.2 CPU Time Accounting

The kernel accounts for CPU time in 7 categories, visible in `top`, `mpstat`, and `sar`:

```
%us  (user):     time executing user-space code (your Java/Python/C code)
%sy  (system):   time in kernel code (syscalls: read/write/mmap/socket operations)
%ni  (nice):     user-space time for processes with positive nice values (low priority)
%id  (idle):     CPU truly idle (no runnable tasks, no I/O wait)
%wa  (iowait):   CPU idle while at least one process is blocked waiting for I/O
                 Note: iowait is a SUBSET of idle time — the CPU is not doing useful
                 work during iowait; it's just a more specific label for the idle time
%hi  (hardware interrupts): time handling hardware IRQs (NIC interrupts, disk interrupts)
%si  (software interrupts): time in software IRQs (NET_RX_SOFTIRQ, network processing)
%st  (steal):    time stolen by the hypervisor — the VM wanted CPU but the host gave
                 the physical core to another VM. Your workload received ZERO CPU
                 during this time. A silent performance killer in shared cloud environments.
```

**Additive check:** `%us + %sy + %ni + %id + %wa + %hi + %si + %st = 100%` always.

### 4.3 Context Switch Cost

A context switch is the CPU overhead of saving one process/thread's state and restoring another's. On modern hardware:

- **Voluntary context switch:** a process explicitly yields (e.g., calls `sleep()`, blocks on I/O). ~1–3 µs on modern hardware.
- **Involuntary context switch:** the scheduler preempts a running process when its time quantum expires (~1 ms on Linux). Same ~1–3 µs cost.

**Why it matters for data engineering:** A Spark executor with 200 threads and a 32-core CPU experiences ~(200/32) ≈ 6× more context switches per core than if it had 32 threads. Each context switch is 1–3 µs of wasted CPU. At 100,000 context switches/second (visible in `vmstat cs` column), that's 100,000 × 2 µs = 200 ms/second of a single core wasted on scheduling overhead — nearly 20% of one core's capacity. This is why Spark's `--executor-cores` should approximately match the physical core count, not be set to 100+.

### 4.4 IPC and CPU Efficiency

**IPC (Instructions Per Cycle)** measures how efficiently a CPU core is being used. A well-optimized loop might achieve IPC ≈ 3–4 (executing 3–4 instructions per clock cycle). A loop with frequent cache misses might achieve IPC ≈ 0.1 (the CPU spends 90% of time stalled waiting for memory).

`perf stat` measures IPC directly:
```bash
perf stat python3 aggregation.py
# Performance counter stats for 'python3 aggregation.py':
#      120,453 M   instructions
#       45,234 M   cycles
#            2.66  insn per cycle    ← IPC = 2.66 (good: efficient computation)
#        1,234 M   cache-misses      ← if high → memory-bound, not CPU-bound
```

A Spark shuffle sort might show IPC ≈ 0.5 (memory-bound comparison loop) while vectorized columnar aggregation shows IPC ≈ 3.5 (cache-resident, pipelined arithmetic). The same wall-clock CPU time on the profiler can represent 7× more useful work in the efficient case.

---

## 5. mpstat — Per-CPU Statistics

`mpstat` (multiprocessor statistics) from the `sysstat` package shows CPU utilization broken down per core and per category. It is the primary tool for detecting single-core saturation and identifying whether CPU time is user, kernel, or interrupt.

### 5.1 Basic mpstat Usage

```bash
# Install (if not present)
apt-get install sysstat

# All CPUs, summary, refresh every 2 seconds
mpstat -P ALL 2
# Linux 5.15.0 (server01)   06/27/2026   _x86_64_   (32 CPU)
#
# 10:23:45  CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle
# 10:23:47  all   23.45    0.00    2.34    8.12    0.00    0.23    0.00    0.00    0.00   65.86
# 10:23:47    0   89.00    0.00    3.00    2.00    0.00    1.00    0.00    0.00    0.00    5.00
# 10:23:47    1   12.00    0.00    1.00    9.00    0.00    0.00    0.00    0.00    0.00   78.00
# 10:23:47    2   14.00    0.00    1.50    7.00    0.00    0.00    0.00    0.00    0.00   77.50
# ...
# 10:23:47   31    8.00    0.00    2.00   12.00    0.00    0.00    0.00    0.00    0.00   78.00
# ↑
# CPU 0 is at 89% user — one core is heavily loaded while others are idle
# This indicates single-threaded bottleneck (Kafka network acceptor? GC thread?)

# Summary line only (no per-core breakdown)
mpstat 2

# Specific CPUs
mpstat -P 0,1,2,3 2    # only CPUs 0-3

# Show interrupt statistics per CPU
mpstat -I ALL 2
# or just hardware/software interrupts:
mpstat -I SUM 2
```

### 5.2 Reading mpstat Output

```
CPU     — "all" = aggregate; number = specific core
%usr    — user-space time (your application code)
%nice   — user-space time for niced (low-priority) processes
%sys    — kernel time (system calls, kernel threads)
%iowait — idle while waiting for I/O (subset of idle)
%irq    — hardware interrupt handling time
%soft   — software interrupt time (NET_RX_SOFTIRQ = network packet processing!)
%steal  — stolen CPU time (hypervisor gave CPU to another VM)
%guest  — time running a virtual CPU (this host is a VM hypervisor running a guest)
%gnice  — time running a niced guest
%idle   — truly idle
```

### 5.3 Diagnostic Patterns with mpstat

```bash
# Pattern 1: Find the hot core (single-threaded bottleneck)
mpstat -P ALL 1 | awk '$3=="all" || $NF+0 < 20 {print}' | head -40
# Look for one core at 95%+ while others are idle
# Means: single-threaded process (Python, or single JVM thread) is the bottleneck

# Pattern 2: High %sys — too many system calls
mpstat -P ALL 2 | awk '{if($6>20) print "HIGH SYS on CPU", $3, ":", $6"%"}'
# %sys > 20% suggests excessive syscall rate:
# - Too many small write() calls (use larger write buffers)
# - Too many socket operations (check Kafka batch settings)
# - Memory allocation/deallocation in a tight loop (check GC)

# Pattern 3: High %soft — network interrupt overload
mpstat -P ALL 2 | awk '{if($9>10) print "HIGH SOFT IRQ on CPU", $3, ":", $9"%"}'
# %soft > 10% on a few CPUs: all network interrupt processing is pinned to those cores
# Common on NICs without RSS (Receive Side Scaling): all packets go to CPU 0
# Fix: enable RSS to distribute NIC interrupts across cores

# Pattern 4: %steal on cloud VMs
mpstat 2 | awk '{if($NF~/steal/ || $10+0>5) print "STEAL:", $10"%", "at", $1}'
# %steal > 5% consistently: VM is oversubscribed on the host
# Consider: dedicated instances (c5.metal, i3.metal on AWS) for Kafka
# Or: scale out (more nodes, less CPU per node)

# Pattern 5: Comparing user vs system time
mpstat 1 5 | awk 'NR>3 && $3=="all" {us+=$4; sy+=$6; n++} END {print "avg us:", us/n"%", "sy:", sy/n"%"}'
# Healthy I/O-heavy workload: us 60%, sy 10%, wa 25%
# CPU-bound compute: us 85%, sy 5%, wa 5%
# System-call heavy: us 30%, sy 45%, wa 5% → too many syscalls

# Watch a specific Kafka thread:
# First: find Kafka's network thread CPU via top -H -p <kafka_pid>
# Then: cross-reference the core it runs on and watch that core:
CORE=$(ps -o psr -p $(pgrep -f kafka.Kafka | head -1) --no-headers | head -1 | tr -d ' ')
mpstat -P $CORE 1
```

---

## 6. sar — System Activity Reporter

`sar` (System Activity Reporter) is a historical performance monitoring tool that records system statistics periodically and allows you to replay them. It is the only standard Linux tool that can tell you what happened to CPU, memory, and I/O *yesterday at 3 AM* when the incident occurred.

### 6.1 sar Architecture

```
sadc (data collector): runs every 10 minutes via cron or systemd timer
  ↓ writes to
/var/log/sysstat/saXX  (XX = day of month, e.g., sa27 for June 27)
  ↓ read by
sar (report generator): replays the recorded data
```

```bash
# Enable sar collection (if not already enabled)
# Debian/Ubuntu:
systemctl enable --now sysstat
# RHEL/CentOS:
systemctl enable --now sysstat
# Edit collection interval (default 10 minutes, change to 1 minute for production):
vim /etc/sysstat/sysstat    # or /etc/default/sysstat on Debian
# HISTORY=7  (keep 7 days of data)
# COMPRESSAFTER=31  (compress after 31 days)
# Edit /etc/cron.d/sysstat to change from */10 to */1

# Start collecting right now (for immediate use):
sar -o /tmp/test_sar 1 60   # collect 60 samples, 1 per second, to file
```

### 6.2 Key sar Commands

```bash
# ── CPU reports ────────────────────────────────────────────────────────────────

# CPU utilization, all CPUs, today
sar -u
# 10:00:00  CPU    %user   %nice  %system  %iowait   %steal   %idle
# 10:10:00  all    23.45    0.00     2.34     8.12     0.00   66.09
# 10:20:00  all    45.67    0.00     4.56    10.23     0.00   39.54

# Per-CPU, specific time range
sar -P ALL -s 09:00:00 -e 10:00:00

# CPU utilization from a specific date's data file
sar -u -f /var/log/sysstat/sa26   # June 26

# Last 1 hour of CPU stats (replay from sadc data)
sar -u 1 3600
# Wait 1 hour... or read it from the history file instantly

# Context switches (very useful for threading diagnosis)
sar -w
# 10:00:00    proc/s   cswch/s
# 10:10:00      0.10  45234.56   ← 45,234 context switches/second
# cswch/s > 100,000 on a 32-core machine: likely over-threaded

# Run queue and load average
sar -q
# 10:00:00   runq-sz  plist-sz  ldavg-1  ldavg-5  ldavg-15  blocked
# 10:10:00         2       412     2.34     2.12      1.98        0
# runq-sz: processes in run queue (waiting for CPU)
# blocked: processes blocked in D state (waiting for I/O)

# ── Memory reports ─────────────────────────────────────────────────────────────
sar -r               # memory utilization
sar -B               # paging statistics (page faults, page scans)
sar -W               # swap stats (swpin/s, swpout/s)

# ── I/O reports ───────────────────────────────────────────────────────────────
sar -d               # disk device statistics (like iostat)
sar -b               # I/O transfer rates

# ── Network reports ────────────────────────────────────────────────────────────
sar -n DEV           # network device stats (rxkB/s, txkB/s per interface)
sar -n SOCK          # socket statistics (TCP, UDP)
sar -n TCP,ETCP      # TCP stats + TCP errors (retransmissions!)

# ── All stats at once ──────────────────────────────────────────────────────────
sar -A               # everything (very verbose)
```

### 6.3 Post-Incident Analysis with sar

```bash
# Scenario: Kafka latency spike reported at 14:30-14:45 yesterday.
# You're investigating this morning.

# Step 1: Check CPU during that window
sar -u -s 14:25:00 -e 14:50:00 -f /var/log/sysstat/sa26
# Look for: %user spike, %steal spike, %iowait spike

# Step 2: Check run queue (was CPU saturated with queued processes?)
sar -q -s 14:25:00 -e 14:50:00 -f /var/log/sysstat/sa26
# Look for: runq-sz > number of CPUs → CPU saturation

# Step 3: Check context switches (did thread count explode?)
sar -w -s 14:25:00 -e 14:50:00 -f /var/log/sysstat/sa26
# Look for: cswch/s jump → sudden increase in threads or blocking

# Step 4: Check I/O (did a disk flush cause the spike?)
sar -d -s 14:25:00 -e 14:50:00 -f /var/log/sysstat/sa26
# Look for: await spike, %util spike

# Step 5: Check memory (did paging start?)
sar -B -s 14:25:00 -e 14:50:00 -f /var/log/sysstat/sa26
# Look for: pgscank/s spike → kswapd running → memory pressure

# Step 6: Check network (was it a traffic burst?)
sar -n DEV -s 14:25:00 -e 14:50:00 -f /var/log/sysstat/sa26
# Look for: rxkB/s or txkB/s spike on eth0/ens3/bond0

# Build a combined incident timeline:
echo "=== CPU ===" && sar -u -s 14:25:00 -e 14:50:00 -f /var/log/sysstat/sa26
echo "=== I/O ===" && sar -d -s 14:25:00 -e 14:50:00 -f /var/log/sysstat/sa26
echo "=== Net ===" && sar -n DEV -s 14:25:00 -e 14:50:00 -f /var/log/sysstat/sa26
```

---

## 7. perf — Linux Performance Profiling

`perf` is the Linux kernel's built-in performance analysis tool. It reads hardware performance counters (CPU cycle counters, cache miss counters, branch misprediction counters) and software counters (page faults, context switches). For data engineering, it bridges the gap between "my process is using 100% CPU" and "this specific function is consuming 80% of those cycles."

### 7.1 Installation and Prerequisites

```bash
# Install
apt-get install linux-tools-common linux-tools-$(uname -r)   # Debian/Ubuntu
yum install perf                                               # RHEL/CentOS

# Allow unprivileged perf (optional, for development)
echo 0 > /proc/sys/kernel/perf_event_paranoid
# Or in /etc/sysctl.d/: kernel.perf_event_paranoid=0

# Verify
perf --version
# perf version 5.15.128
```

### 7.2 perf stat — CPU Counter Summary

`perf stat` attaches hardware counters to a process and reports aggregate statistics. It's the fastest way to characterize a workload.

```bash
# Profile a command
perf stat python3 pandas_aggregation.py

# Output:
# Performance counter stats for 'python3 pandas_aggregation.py':
#
#      45,234,120 ms   task-clock                   #    3.82 CPUs utilized
#             234       context-switches             #    5.17 /sec
#              12       cpu-migrations               #    0.27 /sec
#          12,456       page-faults                  #  275.44 /sec
#  89,234,567,890       cycles                       #    1.97 GHz
# 120,453,234,567       instructions                 #    1.35  insn per cycle
#  23,456,789,012       branches                     #  518.67 M/sec
#     456,789,012       branch-misses                #    1.95% of all branches
#
#      11.840 seconds time elapsed
#      45.123 seconds user
#       0.234 seconds sys

# Key metrics to watch:
# insn per cycle: 1.35 is moderate; > 2 is efficient; < 0.5 is memory-bound
# branch-misses %: < 3% good; > 10% suggests unpredictable branching
# page-faults: if very high → allocation-heavy workload
# context-switches: 234 for 11 seconds = 21/s (very low = no parallelism issues)

# Profile a running process
perf stat -p $(pgrep -n java) sleep 10    # attach to running JVM for 10 seconds

# Specific counters (hardware events)
perf stat -e cycles,instructions,cache-misses,cache-references,LLC-load-misses python3 script.py
# LLC = Last Level Cache (L3)
# LLC-load-misses / cache-references = miss rate
# High LLC miss rate (>10%) = memory-bound; working set doesn't fit in L3

# List all available events
perf list | head -50
perf list | grep -i "cache"
perf list | grep -i "mem"
```

### 7.3 perf top — Live Profiling

`perf top` is like `top` but shows which **functions** are consuming CPU cycles, not just which processes.

```bash
# Live CPU profiler (system-wide)
perf top
# Samples: 45K of event 'cycles', 4000 Hz, Event count (approx.): 3245678901
#
# Overhead  Shared Object            Symbol
#   32.45%  [kernel]                 [k] __memcpy_avx_unaligned
#   18.23%  libjvm.so                [.] G1CollectedHeap::collect
#   12.45%  python3.10               [.] _PyObject_GenericGetAttrWithDict
#    8.12%  libpandas.cpython-310    [.] _pandas_hash_table_lookup
#    6.78%  [kernel]                 [k] copy_user_enhanced_fast_string
#    4.56%  libc.so.6                [.] malloc

# Column meanings:
# Overhead: fraction of CPU cycles spent in this function
# Shared Object: which binary/library contains the function
#   [kernel]    → kernel code
#   [.] prefix  → user-space function
#   [k] prefix  → kernel function
# Symbol: function name (may require debug symbols for JVM code)

# Profile a specific process
perf top -p $(pgrep -n java)
perf top -p $(pgrep -f kafka.Kafka)

# Show callgraph (callers of the hot functions)
perf top -g
# Expand any function with ← → arrows to see what called it

# Filter to show only user space
perf top --no-children -p $(pgrep -n java)
```

### 7.4 perf record and perf report — Flame Graph Workflow

`perf record` captures samples to a file; `perf report` analyzes them; flame graphs visualize the call stack distribution.

```bash
# Step 1: Record CPU samples for a running process (30 seconds)
perf record -F 99 -p $(pgrep -n java) -g -- sleep 30
# -F 99: sample at 99 Hz (avoid lockstep with 100 Hz timer)
# -p: attach to this PID
# -g: record call graphs (stacks)
# Output: perf.data in current directory

# Step 2: View report in TUI
perf report
perf report --stdio   # text output (for logging/piping)

# Step 3: Generate a flame graph (requires brendangregg/FlameGraph)
git clone https://github.com/brendangregg/FlameGraph /opt/FlameGraph

perf record -F 99 -p $(pgrep -n java) -g -- sleep 30
perf script | /opt/FlameGraph/stackcollapse-perf.pl | \
    /opt/FlameGraph/flamegraph.pl > /tmp/kafka_flamegraph.svg

# View kafka_flamegraph.svg in a browser
# Wide bars = functions spending more time
# Tall stacks = deep call chains
# Looking for: wide bars at the top (leaf nodes) = where cycles are spent

# Profile with Java symbols (requires -XX:+PreserveFramePointer in JVM):
# Add to KAFKA_JVM_PERFORMANCE_OPTS:
# -XX:+PreserveFramePointer
# This allows perf to walk the JVM's frame pointer chain for stack unwinding
# Without this, perf only sees "java" with no function names

# For JVM profiling: use perf-map-agent to map JIT symbols
# https://github.com/jvm-profiling-tools/perf-map-agent
java -agentpath:/opt/perf-map-agent/libperfmap.so \
     -XX:+PreserveFramePointer \
     -Xmx4g kafka.Kafka /etc/kafka/server.properties &

# Then record and get JVM method names in perf output
perf record -F 99 -p $(pgrep -f kafka.Kafka) -g -- sleep 30
/opt/perf-map-agent/bin/create-java-perf-map.sh $(pgrep -f kafka.Kafka)
perf report --no-children | head -40
```

### 7.5 perf for Cache and Memory Analysis

```bash
# Count LLC (L3 cache) misses — identify memory-bound workloads
perf stat -e LLC-loads,LLC-load-misses,LLC-stores,LLC-store-misses \
    python3 spark_aggregation.py

# Output:
#  1,234,567,890  LLC-loads
#    123,456,789  LLC-load-misses    #   10.00% of all LL cache refs
#    234,567,890  LLC-stores
#     23,456,789  LLC-store-misses   #   10.00% of all LL cache refs
#
# 10% LLC miss rate: moderate; working set partially fits in L3
# >20% LLC miss rate: memory-bound; working set doesn't fit in L3

# Detailed memory access profiling
perf mem record -p $(pgrep -n java) -- sleep 10
perf mem report | head -30

# TLB miss rate (relevant for JVMs with large heaps and THP disabled)
perf stat -e dTLB-loads,dTLB-load-misses,iTLB-loads,iTLB-load-misses \
    python3 script.py
# High dTLB-load-misses (>1% of loads): many distinct memory locations accessed
# → Consider huge pages (-XX:+UseLargePages for JVM) to reduce TLB pressure
```

---

## 8. CPU Throttling: Thermal and Power Limits

### 8.1 What CPU Throttling Is

**CPU throttling** is a hardware mechanism that reduces CPU clock frequency to prevent damage from overheating or to stay within power limits. From the OS perspective, the CPU appears slower — more wall-clock time per instruction. From the application perspective, the same code takes longer to run for no visible reason. Throttling is silent unless you know where to look.

Three causes of throttling:

1. **Thermal throttling:** The CPU die temperature exceeds a threshold (typically 80-100°C for modern Intel/AMD server CPUs). The CPU reduces frequency until temperature falls. Caused by: inadequate cooling, high ambient data center temperature, thermal paste degradation, or dust-clogged heatsink fins.

2. **Power limit throttling (Intel RAPL):** Intel's Running Average Power Limit prevents the CPU from drawing more power than the motherboard/PSU can safely deliver. In cloud VMs, this is a fundamental constraint — the hypervisor enforces per-VM power envelopes.

3. **P-state limits:** The BIOS or OS has set a maximum P-state (performance state) below the CPU's rated maximum. Common in power-saving mode or after a BIOS misconfiguration.

### 8.2 Detecting Throttling

```bash
# Method 1: Check for thermal throttling events
dmesg | grep -i "thermal\|throttl\|temperature"
# [    1.234567] CPU0: Core temperature above threshold, cpu clock throttled

# Method 2: Monitor current CPU frequency vs rated max
# Check rated max
cat /proc/cpuinfo | grep "cpu MHz" | head -8
# cpu MHz  : 1800.000   ← only 1.8 GHz! (rated: 3.5 GHz) → throttled!
cat /proc/cpuinfo | grep "model name" | head -1
# Intel(R) Xeon(R) Gold 6148 @ 2.40GHz  ← base is 2.4 GHz, max turbo 3.7 GHz

# Check current frequency via cpufreq
for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq; do
    core=$(echo $cpu | grep -oP 'cpu\d+')
    freq=$(cat $cpu)
    echo "$core: $(echo "scale=2; $freq/1000000" | bc) GHz"
done | head -8
# cpu0: 3.70 GHz  (full turbo — not throttled)
# cpu1: 1.20 GHz  ← throttled!
# cpu2: 3.70 GHz

# Method 3: Intel turbostat (best tool for frequency/power/thermal diagnostics)
turbostat --quiet --show PkgTmp,CoreTmp,Busy%,Bzy_MHz,PkgWatt 5
# PkgTmp  CoreTmp  Busy%  Bzy_MHz  PkgWatt
#     82       78  98.23     3700    205.4   ← temperature 82°C, near throttle point
#     91       87  97.45     1800    198.2   ← temperature 91°C, throttling at 1.8 GHz!

# Method 4: Intel MSR (Model Specific Registers) — lowest level throttle detection
# Requires: apt-get install msr-tools; modprobe msr
rdmsr -p 0 0x19C  # Package Thermal Status register
# Bit 4 set = thermal throttling active right now on CPU 0

# Method 5: Perf — CPU frequency counting
perf stat -e cpu-clock,task-clock -I 1000 -p $(pgrep -n java)
# cpu-clock: wall-clock time multiplied by CPUs used
# task-clock: actual CPU time used
# ratio task-clock/cpu-clock ≈ 1.0 means running at expected speed
# ratio < 1.0 means the process is getting less CPU time than expected

# Method 6: Cloud VM steal time
mpstat 1 | awk '{print "steal:", $NF-1}'  # %st column
# steal > 5%: CPU time is being given to other VMs on the host
# This is NOT hardware throttling but has the same observable effect
```

### 8.3 Responding to Throttling

```bash
# Response to thermal throttling:
# 1. Immediate: check temperatures
sensors           # requires lm-sensors package
# coretemp-isa-0000
# Core 0:         +91.0°C  (high = +80.0°C, crit = +100.0°C)  ← near critical!
# Core 1:         +89.0°C
# → Contact data center ops; check cooling, airflow, heatsink

# 2. Mitigation: reduce CPU load until cooling is improved
renice -n 10 -p $(pgrep -f kafka.Kafka)     # reduce Kafka priority
nice -n 15 /opt/spark/sbin/start-worker.sh  # start Spark at low priority

# Response to power limit throttling:
# Check if BIOS has set power limits below Intel Boost spec
# In /sys/devices/virtual/powercap/intel-rapl/:
cat /sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/constraint_0_power_limit_uw
# 200000000  ← 200 W power limit (package 0)
cat /sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/constraint_0_max_power_uw
# 250000000  ← could go to 250 W if limit were removed

# Temporarily increase power limit (requires root; careful on systems without adequate PSU):
echo 250000000 > /sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/constraint_0_power_limit_uw

# Response to P-state ceiling:
# Check if BIOS has locked max P-state below turbo:
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq
# 2400000   ← max set to 2.4 GHz (base); turbo not enabled
cat /sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq
# 3700000   ← hardware max is 3.7 GHz (turbo available but not allowed)
# Fix: change governor or increase scaling_max_freq:
echo 3700000 > /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq
# Or use the performance governor (Section 9)
```

---

## 9. CPU Frequency Scaling and Governors

### 9.1 CPU P-States and Governors

Modern CPUs operate at different **P-states** (Performance states) — combinations of voltage and frequency. Higher P-states mean higher frequency and more power. The **CPU frequency governor** decides which P-state to use at any given moment.

```bash
# Check current governor for all CPUs
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | sort | uniq -c
#  32 powersave    ← all 32 CPUs using "powersave" governor

# Available governors
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_available_governors
# performance powersave

# Set all CPUs to "performance" governor (runs at max frequency always)
for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    echo performance > $cpu
done
# Verify
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq
# 3700000  ← 3.7 GHz (max turbo)

# Persistent setting (survives reboot):
# Method A: cpupower
apt-get install linux-cpupower
cpupower frequency-set -g performance
# Add to /etc/rc.local or systemd unit for persistence

# Method B: tuned (RHEL/CentOS)
yum install tuned
tuned-adm profile throughput-performance   # or latency-performance
systemctl enable --now tuned

# Common governors:
# performance:  always max frequency; best for Kafka latency, Spark throughput
# powersave:    always min frequency; saves power; bad for latency-sensitive workloads
# ondemand:     scales up when busy; can be slow to ramp up under burst workloads
# schedutil:    uses kernel scheduler signals to decide frequency; better than ondemand
#               for modern kernels (Linux 4.7+); default on many distros
# conservative: like ondemand but ramps up slowly; generally worse for DE workloads
# userspace:    manual frequency control; used by benchmarking tools

# For production Kafka brokers and Spark workers:
# → performance governor (always max freq; predictable latency)
# → Or schedutil (good balance on modern kernels with HWP/Intel Speed Select)
```

### 9.2 Intel HWP (Hardware P-states)

On modern Intel CPUs, the hardware itself manages P-states (Intel Speed Select / HWP). In this mode, the CPU's power management controller directly adjusts frequency without OS intervention, making software governors less relevant but still providing hints:

```bash
# Check if HWP is active
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_driver
# intel_pstate   ← Intel P-state driver
# acpi-cpufreq   ← ACPI driver (older; software-managed)

# With intel_pstate driver, "performance" governor = HWP hint to run at max
# "powersave" = HWP hint to save power
# The CPU firmware makes the final decision

# Disable HWP and use software governor (sometimes needed for benchmarking):
echo passive > /sys/devices/system/cpu/intel_pstate/status
cat /sys/devices/system/cpu/intel_pstate/status
# active   ← HWP active (normal)
# passive  ← software governor is in control

# For Kafka: performance governor is the correct setting regardless of HWP mode
```

### 9.3 C-States: CPU Idle States

When a CPU core is idle, it enters a **C-state** (idle state) to save power. Higher C-states save more power but have longer wake-up latency:

```
C0: active (executing instructions)
C1: halt (stop execution; wake up in ~1 µs)
C1E: enhanced halt (~10 µs wake-up)
C3: sleep (clock gated; ~50 µs wake-up; L2 flush)
C6: deep power down (~200 µs wake-up; L3 flush on some CPUs)
```

For Kafka (which handles sudden bursts of requests), deep C-states add latency:

```bash
# Check maximum C-state allowed
cat /sys/module/intel_idle/parameters/max_cstate
# 9   ← all C-states enabled including deep sleep

# Limit to C1 for Kafka (eliminates deep sleep wake-up latency):
# Method A: kernel boot parameter
# In /etc/default/grub:
# GRUB_CMDLINE_LINUX="... processor.max_cstate=1 intel_idle.max_cstate=1"
# update-grub && reboot

# Method B: cpupower
cpupower idle-set -D 1   # disable C-states deeper than C1

# Method C: /dev/cpu_dma_latency
# Write a 0 to /dev/cpu_dma_latency to request maximum responsiveness
# (keeps CPUs in C0/C1 while the fd is open):
echo 0 > /dev/cpu_dma_latency
# Note: this file must be kept open; close = reverts to normal C-state usage
# Use in a daemon or screen/tmux session:
python3 -c "
import os, signal
fd = os.open('/dev/cpu_dma_latency', os.O_WRONLY)
os.write(fd, b'\x00\x00\x00\x00')   # write uint32 = 0 (request 0 µs latency)
signal.pause()   # keep fd open until killed
" &
```

---

## 10. NUMA Architecture and Locality

### 10.1 NUMA Topology

**NUMA (Non-Uniform Memory Access)** refers to multi-socket server architectures where each CPU socket has its own local DRAM, and accessing another socket's memory crosses the inter-socket interconnect (Intel QPI/UPI or AMD Infinity Fabric).

```
2-socket NUMA server (128 GB total):

Socket 0 (NUMA node 0)        Socket 1 (NUMA node 1)
┌─────────────────────┐       ┌─────────────────────┐
│  CPUs 0-15          │       │  CPUs 16-31          │
│  L3 cache: 30 MB    │       │  L3 cache: 30 MB     │
│  Local RAM: 64 GB   │       │  Local RAM: 64 GB    │
│  (60-100 ns access) │  ←→   │  (60-100 ns access)  │
└─────────────────────┘ QPI   └─────────────────────┘
                               ↑
                         Remote RAM access: 120-200 ns
                         (2× latency penalty)
```

### 10.2 NUMA Diagnostics

```bash
# Show NUMA topology
numactl --hardware
# available: 2 nodes (0-1)
# node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
# node 0 size: 65536 MB
# node 0 free: 12345 MB
# node 1 cpus: 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
# node 1 size: 65536 MB
# node 1 free: 8765 MB
# node distances:
# node   0   1
#   0:  10  21    ← local = 10, remote = 21 (2.1× latency for cross-socket)
#   1:  21  10

# Check NUMA memory allocation statistics
numastat
# Per-node memory statistics:
#                            node0          node1
# numa_hit              1234567890      987654321   ← pages allocated to local node
# numa_miss                 234567          45678   ← pages allocated to remote node (performance hit)
# numa_foreign               45678         234567   ← intended local but allocated remote
# local_node            1234123456      987123456
# other_node               123456          45678
# numa_miss / (numa_hit + numa_miss) = NUMA miss rate; < 1% is healthy

# Per-process NUMA allocation
numastat -p $(pgrep -f kafka.Kafka)
# Per-node process memory usage (in MBs) for PID ... (java)
#                            Node 0          Node 1           Total
#                   --------------- --------------- ---------------
# Huge                         0.00            0.00            0.00
# Heap                      3456.78          234.56         3691.34
# Stack                        0.45            0.00            0.45
# Private                    345.67           45.67          391.34
# ----------------  --------------- --------------- ---------------
# Total                     3803.00          280.23         4083.23
#
# Most heap is on Node 0 (local), small amount on Node 1 — acceptable
# If Node 1 >> Node 0 for a process running on Node 0 CPUs: NUMA problem

# Check where a process's threads are running
ps -eLo pid,tid,psr,comm | awk '$4=="java" && $1==1234 {print $3, $2}'
# psr = processor (CPU core) the thread is currently running on
# If Kafka threads run on CPUs 16-31 (Node 1) but memory is on Node 0: remote access
```

### 10.3 NUMA Policy for JVMs

```bash
# Run a JVM pinned to a NUMA node (for latency-sensitive Kafka):
numactl --cpunodebind=0 --membind=0 \
    java -Xmx4g -Xms4g kafka.Kafka /etc/kafka/server.properties
# --cpunodebind=0: run only on CPUs from NUMA node 0
# --membind=0: allocate all memory on NUMA node 0
# → All CPU and memory on the same socket: minimum latency

# Run with interleaved memory (for throughput-heavy Spark executors):
numactl --interleave=all \
    java -Xmx8g spark.executor.CoarseGrainedExecutorBackend
# --interleave=all: round-robin allocate pages across all NUMA nodes
# → Uses full memory bandwidth of all sockets
# → Good for Spark sorts and aggregations that need maximum memory bandwidth

# Automatic NUMA balancing (kernel feature):
cat /proc/sys/kernel/numa_balancing
# 1   ← enabled (default on modern kernels)
# The kernel periodically migrates pages to the NUMA node where they're accessed most often
# For JVMs: can cause stop-the-world pauses while migrating JVM heap pages
# For Kafka with static access patterns: disable for more predictable latency:
echo 0 > /proc/sys/kernel/numa_balancing
sysctl -w kernel.numa_balancing=0
echo "kernel.numa_balancing=0" >> /etc/sysctl.d/60-kafka.conf

# Verify NUMA memory hit rate after numactl deployment:
numastat | awk '/numa_miss|numa_hit/ {print $1, "node0:", $2, "node1:", $3}'
# Good: numa_miss << numa_hit (< 1% miss rate)
# Bad:  numa_miss approaches numa_hit → process memory not local to its CPUs
```

---

## 11. CPU Affinity and Isolation

### 11.1 CPU Affinity

**CPU affinity** (CPU pinning) restricts a process or thread to run only on specified CPU cores. This improves cache reuse (hot L1/L2 data stays warm between time slices) and reduces NUMA effects.

```bash
# View current CPU affinity for a process
taskset -p $(pgrep -f kafka.Kafka)
# pid 1234's current affinity mask: ffffffff
# ffffffff = all 32 bits set = can run on any of CPUs 0-31

# Set affinity to CPUs 0-15 (NUMA node 0) for Kafka
taskset -c 0-15 -p $(pgrep -f kafka.Kafka)
# pid 1234's new affinity list: 0-15

# Start a new process with affinity
taskset -c 0-15 java -Xmx4g kafka.Kafka /etc/kafka/server.properties

# For systemd services, add to unit file:
# [Service]
# CPUAffinity=0-15   # pin to CPUs 0-15

# Set affinity in Python
import os
os.sched_setaffinity(0, {0, 1, 2, 3})  # restrict THIS process to CPUs 0-3
os.sched_getaffinity(0)                  # get current affinity

# Per-thread affinity (from C/C++, or via numactl for JVMs):
# Java doesn't expose per-thread CPU affinity natively; use numactl for JVM-level pinning
```

### 11.2 CPU Isolation with isolcpus

For the strictest CPU isolation (dedicating cores to a single process without any kernel overhead), use the `isolcpus` kernel boot parameter:

```bash
# Example: dedicate CPUs 16-31 to Kafka (CPUs 0-15 remain for OS and other processes)
# In /etc/default/grub:
GRUB_CMDLINE_LINUX="... isolcpus=16-31 nohz_full=16-31 rcu_nocbs=16-31"
# isolcpus: excludes CPUs from normal scheduler (they won't get OS tasks)
# nohz_full: disables the 1 ms timer tick on isolated CPUs (eliminates jitter)
# rcu_nocbs: offloads RCU callbacks from isolated CPUs

update-grub
reboot

# After reboot: start Kafka pinned to isolated CPUs
taskset -c 16-31 numactl --cpunodebind=1 --membind=1 \
    java -Xmx4g kafka.Kafka /etc/kafka/server.properties

# Verify: isolated CPUs should show 0% use by other processes
mpstat -P 16,17,18,19 2
# Expect: %usr=95%+, %sy<2%, %idle<5%
# vs without isolation: frequent jitter from OS scheduling
```

### 11.3 IRQ Affinity

Network interrupt requests (IRQs) from the NIC are handled by specific CPUs. By default on a busy server, all NIC interrupts may go to CPU 0, saturating it with `%soft` IRQ handling:

```bash
# Check which CPUs handle NIC interrupts
cat /proc/interrupts | grep eth0
#   48:  10234567   2345678  123456  456789   PCI-MSI  eth0-rx-0
#   49:   8234567   1234567   89012  234567   PCI-MSI  eth0-rx-1
#   50:        45        12       2       1   PCI-MSI  eth0-tx-0

# View affinity for interrupt 48 (eth0-rx-0)
cat /proc/irq/48/smp_affinity
# 00000001   ← binary mask, bit 0 = CPU 0 only

# Distribute NIC interrupts across CPUs 0-7 (for 8-queue NIC):
for i in 48 49 50 51 52 53 54 55; do
    cpu=$((i - 48))
    echo $((1 << cpu)) > /proc/irq/$i/smp_affinity
done
# This sets: IRQ 48 → CPU 0, IRQ 49 → CPU 1, ..., IRQ 55 → CPU 7

# Automate with irqbalance (daemon that distributes IRQs automatically)
apt-get install irqbalance
systemctl enable --now irqbalance
# irqbalance monitors interrupt rates and redistributes to balance the load
# Stop irqbalance if you want manual IRQ pinning (they conflict):
systemctl stop irqbalance

# For Kafka: pin NIC IRQs to CPUs on the same NUMA node as Kafka's network threads
# This ensures NIC DMA and Kafka's network processing share the same L3 cache
```

---

## 12. Mental Models

### 12.1 CPU Time as a Budget

Think of each CPU-second as a budget of 1000 units. The OS allocates these units across categories: user code gets some, kernel calls get some, interrupts get some. Each unit spent in `%steal` is stolen before the budget even arrives — you think you have 1000 units but the hypervisor took 200, leaving 800. High `%iowait` means the budget is arriving but being spent in idle mode because the process is blocked on disk. Only `%usr` represents work your application actually accomplished. When diagnosing a "slow" system, the first question is: which category is consuming the CPU budget?

### 12.2 The CPU Cache as a Warm Kitchen

A chef who cooks the same dish repeatedly keeps ingredients on the counter (L1/L2 cache) — fast access. Less-used ingredients go in the pantry (L3 cache). Rarely used items are in the warehouse (DRAM). If the chef must walk to the warehouse for every ingredient, cooking is 200× slower than if everything is on the counter. A data pipeline that processes each row independently and jumps across memory (row-format data, large hash tables) sends the chef to the warehouse constantly. A columnar scan processes one column at a time — all ingredients are on the counter, one after another. This is why vectorized column-oriented processing (Apache Arrow, Parquet with predicate pushdown) is 10-100× faster than row-by-row processing for analytical workloads.

### 12.3 NUMA as a Two-Building Office

NUMA is like having two office buildings connected by a slow elevator. Workers in building A (socket 0) have immediate access to their own filing cabinets (local DRAM). Fetching a file from building B requires taking the elevator (inter-socket interconnect) — 2× slower. If a process's threads run in building A but its data files are in building B, every access costs an elevator ride. `numactl --cpunodebind=0 --membind=0` means "put both the workers and all the filing cabinets in building A."

---

## 13. Failure Scenarios

### 13.1 Kafka Latency Spikes Every 30 Seconds

```
Symptom: Kafka p99 produce latency spikes from 2 ms to 200 ms for 100-500 ms
  every 25-35 seconds, very regularly.

  Hypothesis 1: GC pause
    jstat -gcutil $(pgrep -f kafka.Kafka) 1000 60 | awk '{print $6, $7}'
    # FGC (full GC count) and FGCT (full GC time):
    # 0.00  0.000   ← no full GCs
    # 0.00  0.000
    # 1.00  0.234   ← one full GC, 234 ms! (every ~30 lines = ~30 seconds)
    → Full GC every 30 seconds! G1GC taking 234 ms → latency spike
    
    Fix: Tune GC (increase heap to reduce frequency, or switch to ZGC):
    -XX:+UseZGC -XX:+ZGenerational    # ZGC: < 1 ms GC pauses
    -XX:ZAllocationSpikeTolerance=5   # handle allocation spikes

  Hypothesis 2: Transparent Huge Page compaction
    Check:
    grep thp /proc/vmstat | grep -i "thp_collapsed\|thp_fault_alloc\|thp_split"
    dmesg | grep -i "khugepaged"
    # If khugepaged is running: THP compaction is causing brief stalls
    
    Fix:
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
    echo never > /sys/kernel/mm/transparent_hugepage/defrag
    systemctl restart kafka

  Hypothesis 3: CPU frequency stepping
    turbostat --quiet --show Bzy_MHz,CoreTmp 1 60 | awk '{ if($2 < 2000) print "THROTTLE:", $0 }'
    # If frequency drops every ~30 seconds: Intel Turbo Boost running out of thermal budget
    # Sustained high load heats the CPU → Boost disabled → frequency drops → latency spike
    
    Fix:
    Check cooling first (sensors → look for temp > 80°C)
    Set performance governor to prevent frequency steps:
    for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
        echo performance > $cpu
    done
    # Or limit Kafka threads to fewer cores (less heat):
    taskset -c 0-15 -p $(pgrep -f kafka.Kafka)  # use only 16 of 32 cores
```

### 13.2 Spark Job 2× Slower on New Cluster

```
Symptom: Same Spark job takes 4 hours on new cluster vs 2 hours on old cluster.
  Both clusters: 8 nodes, 32 cores each, same data size.

  Step 1: Check for steal time
    sar -u -f /var/log/sysstat/sa27 | awk '{if($9+0>5) print "HIGH STEAL:", $0}'
    # ← if steal > 5%: hypervisor is oversubscribed

    mpstat 5 | awk '{print "steal:", $10"%"}'
    # steal: 38.5%   ← 38.5% of CPU time is stolen!

  This is a cloud VM steal time problem. The new cluster uses shared VMs on an
  oversubscribed host. On average, each Spark executor only gets 61.5% of the
  CPU it thinks it's getting. A 4-hour job at 61.5% efficiency = 2.46 hours
  effective work = slower than 2-hour job on dedicated nodes.

  Verification:
    # Run a CPU benchmark to confirm effective GHz:
    sysbench cpu --time=10 run
    # Old cluster: 45,678 events/s
    # New cluster: 28,234 events/s  ← 38% slower per CPU event

  Fix:
    Option A: Switch to dedicated instances
      AWS: i3.metal, c5.metal (bare metal, no hypervisor)
      GCP: n2-standard-64 with "SPREAD" placement policy
      Azure: dedicated host
    
    Option B: Use Spot/Preemptible instances (if job is restartable with checkpointing)
      spark.speculation=true  # re-launch slow tasks on other nodes
      spark.streaming.stopGracefullyOnShutdown=true
    
    Option C: Reserve instances or use dedicated VM tenancy
      AWS: "dedicated" tenancy → single-tenant physical host

  Note: %steal is only visible from INSIDE the VM.
  The host sees the VM's processes as normal; only the guest sees the theft.
```

### 13.3 Python Airflow Task Stuck at 100% Single CPU Core

```
Symptom: Airflow task using Python + pandas stuck at 100% on one CPU core for 45 minutes.
  Expected: 5 minutes.

  mpstat -P ALL 1 | awk '$NF+0 < 5 {print "HOT CORE:", $3, "usr:", $4}'
  # HOT CORE: 7 usr: 98.2   ← CPU 7 at 98% user-space

  Step 1: Identify the Python PID and thread
    pgrep -a -f "airflow tasks run my_dag"
    # 5678 python3 airflow tasks run my_dag slow_task 2026-06-27T09:00:00

  Step 2: Profile with perf
    perf top -p 5678 --no-children
    #  Overhead  Symbol
    #   45.23%  python3.10  [.] PyObject_RichCompareBool
    #   23.45%  python3.10  [.] dict_lookup
    #   12.34%  python3.10  [.] long_hash
    #    8.12%  python3.10  [.] unicode_hash
    
    → Heavy dict lookups and hash operations: pandas groupby using dict accumulation

  Step 3: Confirm with perf stat
    perf stat -p 5678 sleep 5
    # insn per cycle: 0.48   ← below 0.5: not compute-bound but cache-missy
    # cache-misses: 234M in 5 seconds   ← excessive cache pressure from hash table

  Root cause: pandas groupby with a large string key column.
  Each unique key lookup involves Python string hashing → dict lookup →
  potential hash collision chain. For 50M rows with 5M unique string keys,
  the Python dict is 5M entries = well above L3 cache capacity.

  Fix:
    # Option A: Use categoricals for groupby keys (avoids string hashing)
    df['key'] = df['key'].astype('category')
    df.groupby('key')['value'].sum()
    
    # Option B: Use numpy-based aggregation (bypasses Python dict entirely)
    import numpy as np
    codes, uniques = pd.factorize(df['key'])
    result = np.bincount(codes, weights=df['value'])
    
    # Option C: Move to PySpark/Polars (uses JVM/Rust execution, not CPython dict)
    import polars as pl
    pl.from_pandas(df).group_by('key').agg(pl.col('value').sum())
    
    # Speedup: 45 minutes → 3 minutes (15× improvement)
```

### 13.4 All CPUs 100% But Throughput is Low — Context Switch Explosion

```
Symptom: All 32 CPUs at 100% in top/mpstat.
  But Kafka throughput is only 100 MB/s (expected 800 MB/s).
  iowait = 0%, steal = 0%, so CPU is "doing work."

  vmstat 1 5 | awk '{print "cs:", $12, "r:", $1}'
  # cs: 450000  r: 180    ← 450,000 context switches/sec, 180 in run queue!
  # Expected for healthy 32-core system: cs < 100,000/s, r < 32

  ps -eLf | grep java | wc -l
  # 3200 threads   ← 3,200 Java threads in the Kafka JVM!

  Root cause: Kafka was misconfigured with:
  # num.network.threads=800  (should be 3-8)
  # num.io.threads=400       (should be 8-16)
  # num.replica.fetchers=200 (should be 4-8)
  # Total: ~1,400 Kafka threads + JVM internals = 3,200 threads
  
  3,200 threads on 32 cores = 100 threads per core competing.
  Context switch rate: each thread gets a ~1 ms time slice.
  At 3,200 threads: each thread runs once every 3,200 × 1 ms = 3.2 seconds!
  Context switch overhead: 3,200 × 1,000 switches/sec × 2 µs = 6.4 seconds of
  CPU time per second JUST on context switching = wasted on 3 of the 32 cores.
  
  Plus cache thrashing: 3,200 threads can't share L1/L2 cache effectively.
  Each context switch blows L1/L2 cache for the incoming thread.

  Fix:
    # Correct Kafka thread counts (for a 32-core broker):
    num.network.threads=8      # handles socket accept/read; 2-4 per core
    num.io.threads=16          # handles disk I/O; 4-8 per core
    num.replica.fetchers=4     # replication threads
    background.threads=10      # maintenance threads
    
    # After restart:
    ps -eLf | grep java | wc -l
    # 280 threads (manageable)
    vmstat 1 5 | awk '{print "cs:", $12}'
    # cs: 45000  (down from 450,000)
    # Throughput: 800 MB/s (back to expected)
```

---

## 14. Data Engineering Connections

### 14.1 Kafka CPU Profile

```bash
# Kafka's CPU usage profile (healthy broker at moderate load):
# %usr ≈ 60-75%: compression/decompression (snappy/lz4/gzip), serialization, CRC
# %sys ≈ 10-20%: sendfile() syscalls (zero-copy to consumer), socket operations
# %soft ≈ 5-10%: NIC interrupt processing (NET_RX_SOFTIRQ)
# %iowait ≈ 0-5%: disk writes (mostly async through page cache)
# total ≈ 75-95% under heavy load

# Profile a Kafka broker during a produce burst:
perf top -p $(pgrep -f kafka.Kafka) --no-children
# Expected hot functions:
# kafka.utils.CoreUtils$   ← CRC32 calculation
# sun.security.util.Math   ← crypto for SSL (if TLS is enabled)
# java.util.zip.           ← compression
# jvm internal alloc       ← object allocation

# If you see excessive time in GC functions:
# G1CollectedHeap::collect  → GC pressure → increase -Xmx or tune GC

# CPU optimization for Kafka:
# 1. Compression algorithm: lz4 has lowest CPU cost; gzip has best ratio
#    For CPU-constrained brokers: producer.compression.type=snappy
# 2. SSL: TLS 1.3 with AES-GCM is hardware-accelerated (AES-NI instructions)
#    Check: grep -m1 flags /proc/cpuinfo | grep -o aes
#    If "aes" present: TLS overhead is minimal (~5% CPU for AES-NI vs 30% without)
# 3. Disable THP (reduces latency jitter from khugepaged)
# 4. Performance governor (eliminates frequency stepping latency)
# 5. IRQ affinity: pin NIC IRQs to NUMA node 0 where Kafka runs

# Monitor Kafka JVM GC pauses:
jstat -gcutil $(pgrep -f kafka.Kafka) 1000 3600 | \
    awk 'NR>1 && $6>0 {printf "GC pause: %s ms at line %d\n", $7*1000, NR}'
```

### 14.2 Spark CPU Efficiency

```bash
# Spark's CPU usage profile (Shuffle read/sort stage):
# %usr ≈ 40-60%: Java sort comparisons, hash aggregation, data deserialization
# %sys ≈ 15-25%: JVM memory management, disk I/O for shuffle spill
# %iowait ≈ 10-20%: waiting for shuffle reads from disk
# %idle ≈ 5-20%: waiting for network data (shuffle read from remote executors)

# Find CPU-bound vs memory-bound stages:
perf stat -p $(pgrep -f CoarseGrainedExecutorBackend) sleep 30
# insn per cycle > 2.0: CPU-bound (compute is the bottleneck)
# insn per cycle < 0.5: memory-bound (cache misses; data > L3 cache)
# cache-misses > 10%: working set exceeds L3 → increase executor memory to reduce spill

# Detect vectorized vs interpreted execution:
# Check Spark UI: "Whole Stage Codegen" in plan = vectorized
# "Project" / "Filter" without codegen = row-by-row (interpret mode)
# Force codegen for batch workloads:
# spark.sql.codegen.wholeStage=true   (default: true)
# spark.sql.codegen.fallback=false    (don't fall back to interpreted on complex plans)

# Thread count optimization for Spark:
# Each executor should have executor-cores ≈ 2-5 per physical core
# More than 5 threads/core → context switching overhead dominates
# spark.executor.cores=4   (for 4-core physical node: 1 executor × 4 cores)
# spark.executor.cores=4   (for 16-core node: 4 executors × 4 cores)

# Monitor with sar during a Spark job:
sar -u 5 360 > /tmp/spark_job_cpu.txt   # capture 30 minutes of CPU stats
# After job: analyze
awk '/all/ {usr+=$3; sys+=$5; wa+=$6; n++} END {
    print "avg usr:", usr/n"%", "sys:", sys/n"%", "iowait:", wa/n"%"
}' /tmp/spark_job_cpu.txt
```

### 14.3 Python Data Processing CPU Patterns

```bash
# Python has the Global Interpreter Lock (GIL): only one thread executes Python
# bytecode at a time. Multi-threaded Python is NOT parallel for CPU-bound work.
# Evidence in mpstat: one CPU at 100%, all others idle despite many threads.

# GIL diagnosis:
mpstat -P ALL 1 5 | awk '$4+0 > 90 {print "HOT:", $3, $4"%"} $4+0 < 5 && $3 ~ /[0-9]+/ {print "IDLE:", $3, $4"%"}'
# HOT:  7 98.3%
# IDLE: 0 1.2%
# IDLE: 1 0.9%
# ... (all other cores idle) → GIL bottleneck

# Solutions for Python parallel CPU work:
# 1. multiprocessing.Pool: separate processes = no GIL, true parallelism
# 2. concurrent.futures.ProcessPoolExecutor: same as above, cleaner API
# 3. Cython/NumPy extensions: release GIL during C-level operations
#    (numpy operations release the GIL → threads CAN run in parallel during vectorized ops)
# 4. PySpark: serializes to Arrow IPC, processes in JVM (no GIL)
# 5. Polars: Rust-based, uses Rayon for parallel execution, no GIL

# Profile Python CPU usage:
python3 -m cProfile -o /tmp/profile.out my_script.py
python3 -c "
import pstats, io
p = pstats.Stats('/tmp/profile.out', stream=io.StringIO())
p.sort_stats('cumulative')
p.print_stats(20)
print(p.stream.getvalue())
"

# Or use py-spy (production-safe profiler, no code modification):
pip install py-spy --break-system-packages
py-spy top --pid $(pgrep -f "airflow tasks run")   # live top-like view
py-spy record --pid $(pgrep -f "airflow tasks run") --output /tmp/profile.svg --duration 30
# View profile.svg in browser for flame graph
```

---

## 15. Code Toolkit

### 15.1 `cpu_diagnostics.py` — CPU Health and Performance Monitor

```python
"""
cpu_diagnostics.py — CPU diagnostics for data engineering servers.

Functions:
  1. parse_cpu_stats()         — read /proc/stat for per-CPU utilization
  2. compute_cpu_utilization() — delta-based CPU % calculation
  3. get_cpu_freq()            — current frequency per core
  4. check_throttling()        — detect CPU throttling indicators
  5. get_context_switches()    — system-wide context switch rate
  6. check_numa_stats()        — NUMA hit/miss rates
  7. cpu_health_report()       — comprehensive CPU health summary

Run directly for a full CPU health report.
"""
from __future__ import annotations
import os
import re
import time
import subprocess
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


# ── /proc/stat CPU utilization ────────────────────────────────────────────────

@dataclass
class CPUTick:
    cpu: str
    user: int
    nice: int
    system: int
    idle: int
    iowait: int
    irq: int
    softirq: int
    steal: int
    guest: int
    guest_nice: int

    @property
    def total(self) -> int:
        return (self.user + self.nice + self.system + self.idle +
                self.iowait + self.irq + self.softirq + self.steal)

    @property
    def active(self) -> int:
        return self.total - self.idle - self.iowait


def parse_cpu_stats() -> dict[str, CPUTick]:
    """Parse /proc/stat into per-CPU tick counts."""
    ticks: dict[str, CPUTick] = {}
    try:
        with open("/proc/stat") as f:
            for line in f:
                if not line.startswith("cpu"):
                    continue
                parts = line.split()
                cpu_name = parts[0]
                vals = [int(x) for x in parts[1:11]] + [0] * (10 - len(parts[1:]))
                ticks[cpu_name] = CPUTick(
                    cpu=cpu_name,
                    user=vals[0], nice=vals[1], system=vals[2], idle=vals[3],
                    iowait=vals[4], irq=vals[5], softirq=vals[6], steal=vals[7],
                    guest=vals[8], guest_nice=vals[9]
                )
    except FileNotFoundError:
        pass
    return ticks


@dataclass
class CPUUtil:
    cpu: str
    usr_pct: float
    sys_pct: float
    iowait_pct: float
    steal_pct: float
    soft_pct: float
    idle_pct: float
    total_active_pct: float


def compute_cpu_utilization(before: dict[str, CPUTick],
                             after:  dict[str, CPUTick]) -> list[CPUUtil]:
    """Compute CPU utilization % from two /proc/stat snapshots."""
    utils = []
    for cpu_name in after:
        if cpu_name not in before:
            continue
        b, a = before[cpu_name], after[cpu_name]
        delta = a.total - b.total
        if delta == 0:
            continue

        def pct(field_after: int, field_before: int) -> float:
            return (field_after - field_before) / delta * 100

        usr   = pct(a.user,    b.user)
        sys_  = pct(a.system,  b.system)
        wa    = pct(a.iowait,  b.iowait)
        steal = pct(a.steal,   b.steal)
        soft  = pct(a.softirq, b.softirq)
        idle  = pct(a.idle,    b.idle)
        utils.append(CPUUtil(
            cpu=cpu_name,
            usr_pct=usr, sys_pct=sys_, iowait_pct=wa,
            steal_pct=steal, soft_pct=soft, idle_pct=idle,
            total_active_pct=100 - idle - wa
        ))

    return sorted(utils, key=lambda u: u.cpu)


def sample_cpu_utilization(interval_s: float = 2.0) -> list[CPUUtil]:
    """Sample CPU utilization over an interval."""
    before = parse_cpu_stats()
    time.sleep(interval_s)
    after = parse_cpu_stats()
    return compute_cpu_utilization(before, after)


# ── CPU frequency ──────────────────────────────────────────────────────────────

def get_cpu_frequencies() -> dict[str, float]:
    """Read current CPU frequency (MHz) for each core."""
    freqs: dict[str, float] = {}
    cpufreq_dir = Path("/sys/devices/system/cpu")
    for cpu_dir in sorted(cpufreq_dir.glob("cpu[0-9]*")):
        freq_file = cpu_dir / "cpufreq" / "scaling_cur_freq"
        if freq_file.exists():
            try:
                freq_khz = int(freq_file.read_text().strip())
                freqs[cpu_dir.name] = freq_khz / 1000  # convert to MHz
            except (ValueError, OSError):
                pass
    return freqs


def get_cpu_max_freq() -> Optional[float]:
    """Get the hardware maximum CPU frequency in MHz."""
    max_freq_file = Path("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq")
    if max_freq_file.exists():
        try:
            return int(max_freq_file.read_text().strip()) / 1000
        except (ValueError, OSError):
            pass
    return None


def get_cpu_governor() -> str:
    """Get the current CPU frequency governor."""
    gov_file = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    if gov_file.exists():
        try:
            return gov_file.read_text().strip()
        except OSError:
            pass
    return "unknown"


def check_throttling(threshold_pct: float = 80.0) -> list[dict]:
    """
    Detect CPUs running below threshold_pct of their maximum frequency.
    Returns list of throttled CPU info dicts.
    """
    max_freq = get_cpu_max_freq()
    if not max_freq:
        return []

    freqs = get_cpu_frequencies()
    throttled = []
    for cpu, freq in freqs.items():
        pct_of_max = freq / max_freq * 100
        if pct_of_max < threshold_pct:
            throttled.append({
                "cpu": cpu,
                "current_mhz": freq,
                "max_mhz": max_freq,
                "pct_of_max": pct_of_max,
            })
    return throttled


# ── Context switches ────────────────────────────────────────────────────────────

def get_context_switch_rate(interval_s: float = 1.0) -> float:
    """Measure context switches per second from /proc/stat."""
    def read_ctxt() -> int:
        try:
            with open("/proc/stat") as f:
                for line in f:
                    if line.startswith("ctxt"):
                        return int(line.split()[1])
        except Exception:
            pass
        return 0

    before = read_ctxt()
    time.sleep(interval_s)
    after = read_ctxt()
    return (after - before) / interval_s


# ── NUMA stats ─────────────────────────────────────────────────────────────────

def get_numa_stats() -> Optional[dict[str, dict[str, int]]]:
    """Read NUMA hit/miss stats from /sys/devices/system/node/*/numastat."""
    stats: dict[str, dict[str, int]] = {}
    node_dir = Path("/sys/devices/system/node")
    if not node_dir.exists():
        return None

    for node_path in sorted(node_dir.glob("node[0-9]*")):
        numa_file = node_path / "numastat"
        if not numa_file.exists():
            continue
        node_name = node_path.name
        stats[node_name] = {}
        try:
            for line in numa_file.read_text().splitlines():
                parts = line.split()
                if len(parts) == 2:
                    stats[node_name][parts[0]] = int(parts[1])
        except (OSError, ValueError):
            pass

    return stats if stats else None


# ── Main report ────────────────────────────────────────────────────────────────

def cpu_health_report(sample_s: float = 3.0) -> None:
    """Print a comprehensive CPU health report."""
    print("=" * 70)
    print("CPU Health Report")
    print("=" * 70)

    # 1. CPU utilization
    print(f"\n1. CPU Utilization (sampling {sample_s}s)")
    utils = sample_cpu_utilization(sample_s)

    # Print aggregate first
    agg = [u for u in utils if u.cpu == "cpu"]
    if agg:
        u = agg[0]
        print(f"   ALL: usr={u.usr_pct:.1f}%  sys={u.sys_pct:.1f}%  "
              f"wa={u.iowait_pct:.1f}%  steal={u.steal_pct:.1f}%  idle={u.idle_pct:.1f}%")
        if u.steal_pct > 5:
            print(f"   ⚠️  HIGH STEAL: {u.steal_pct:.1f}% — VM is oversubscribed on the host")
        if u.iowait_pct > 10:
            print(f"   ⚠️  HIGH IOWAIT: {u.iowait_pct:.1f}% — I/O bottleneck")

    # Print per-core, highlight hot/imbalanced
    per_core = [u for u in utils if u.cpu != "cpu"]
    if per_core:
        hot_cores = [u for u in per_core if u.usr_pct + u.sys_pct > 90]
        idle_cores = [u for u in per_core if u.idle_pct > 90]
        print(f"   Total cores: {len(per_core)} | Hot (>90%): {len(hot_cores)} | "
              f"Idle (>90%): {len(idle_cores)}")
        if hot_cores and idle_cores and len(hot_cores) <= 4:
            print(f"   ⚠️  SINGLE-CORE SATURATION: {[u.cpu for u in hot_cores]} are hot, "
                  f"others idle → single-threaded bottleneck")
        for u in sorted(hot_cores, key=lambda x: x.usr_pct, reverse=True)[:5]:
            print(f"   HOT {u.cpu}: usr={u.usr_pct:.0f}% sys={u.sys_pct:.0f}%")

    # 2. CPU frequency and throttling
    print(f"\n2. CPU Frequency")
    max_freq = get_cpu_max_freq()
    governor = get_cpu_governor()
    print(f"   Governor: {governor}")
    if governor == "powersave":
        print(f"   ⚠️  powersave governor — consider 'performance' for data engineering workloads")
    if max_freq:
        print(f"   Hardware max: {max_freq:.0f} MHz")
    throttled = check_throttling(threshold_pct=75.0)
    if throttled:
        sample_t = throttled[:3]
        avg_pct = sum(t["pct_of_max"] for t in throttled) / len(throttled)
        print(f"   ⚠️  THROTTLING: {len(throttled)} cores below 75% of max freq "
              f"(avg {avg_pct:.0f}% of {max_freq:.0f} MHz)")
        for t in sample_t:
            print(f"   {t['cpu']}: {t['current_mhz']:.0f} MHz ({t['pct_of_max']:.0f}% of max)")
    else:
        freqs = get_cpu_frequencies()
        if freqs:
            avg_mhz = sum(freqs.values()) / len(freqs)
            print(f"   Average current: {avg_mhz:.0f} MHz ({'%.0f' % (avg_mhz/max_freq*100 if max_freq else 0)}% of max)")

    # 3. Context switches
    print(f"\n3. Context Switches")
    cs_rate = get_context_switch_rate(1.0)
    print(f"   {cs_rate:.0f} /sec")
    ncpus = len([u for u in utils if u.cpu != "cpu"]) or 1
    cs_per_core = cs_rate / ncpus
    if cs_per_core > 5000:
        print(f"   ⚠️  HIGH: {cs_per_core:.0f}/sec/core — likely over-threaded "
              f"(check num.network.threads for Kafka, executor-cores for Spark)")
    else:
        print(f"   OK: {cs_per_core:.0f}/sec/core")

    # 4. THP status
    thp_path = Path("/sys/kernel/mm/transparent_hugepage/enabled")
    if thp_path.exists():
        thp = thp_path.read_text().strip()
        print(f"\n4. Transparent Huge Pages: {thp}")
        if "[always]" in thp:
            print(f"   ⚠️  THP enabled — may cause latency jitter for Kafka/Spark")
            print(f"   Fix: echo never > /sys/kernel/mm/transparent_hugepage/enabled")

    # 5. NUMA
    numa_stats = get_numa_stats()
    if numa_stats and len(numa_stats) > 1:
        print(f"\n5. NUMA Statistics")
        for node, ns in numa_stats.items():
            hit = ns.get("numa_hit", 0)
            miss = ns.get("numa_miss", 0)
            total = hit + miss
            miss_pct = (miss / total * 100) if total > 0 else 0
            print(f"   {node}: {hit} hits, {miss} misses ({miss_pct:.1f}% miss rate)")
            if miss_pct > 5:
                print(f"   ⚠️  HIGH NUMA MISS RATE on {node} — consider numactl --membind")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    cpu_health_report(sample_s=3.0)
```

---

## 16. Hands-On Labs

### Lab 1: Observe Per-Core Imbalance

```bash
# Lab 1: create single-threaded vs multi-threaded load and observe with mpstat

# Single-threaded CPU burn (should saturate exactly one core):
python3 -c "import time; t=time.time(); [_ for _ in range(10**9)]" &
SINGLE_PID=$!

# Observe:
mpstat -P ALL 1 5
# Look for ONE core at ~100% while others remain idle
# This is the GIL / single-threaded pattern common in Python

wait $SINGLE_PID

# Multi-threaded burn using multiprocessing:
python3 -c "
import multiprocessing, time
def burn(n):
    t = time.time()
    while time.time() - t < 10:
        x = sum(range(10000))

ncpus = multiprocessing.cpu_count()
procs = [multiprocessing.Process(target=burn, args=(i,)) for i in range(min(ncpus, 8))]
[p.start() for p in procs]
[p.join() for p in procs]
" &
MULTI_PID=$!

# Observe:
mpstat -P ALL 1 5
# Should see up to 8 cores at ~100% usr simultaneously
# (multiprocessing bypasses the GIL)

wait $MULTI_PID
```

### Lab 2: Measure Cache Effects on Throughput

```bash
# Lab 2: observe how working set size affects IPC

# Working set that fits in L1 cache (~32 KB):
python3 -c "
import array, time, subprocess, os

size_kb = 8    # 8 KB: fits in L1 cache
data = array.array('q', range(size_kb * 128))  # 8 bytes per long × 128 = 1 KB
loops = 10**7

# Run perf stat on this process
pid = os.getpid()
print(f'PID: {pid}')
print(f'Working set: {size_kb} KB')
t = time.time()
for _ in range(loops):
    x = data[_ % len(data)]
print(f'Time: {time.time()-t:.2f}s  ({loops/(time.time()-t)/1e6:.1f}M accesses/s)')
" 

# Working set that exceeds L3 cache (~64 MB per core):
python3 -c "
import array, time, os, random
size_mb = 128   # 128 MB: exceeds L3 cache
data = array.array('q', range(size_mb * 131072))  # 128 MB of longs
indices = [random.randrange(len(data)) for _ in range(10**6)]  # random access pattern
t = time.time()
s = 0
for idx in indices:
    s += data[idx]
elapsed = time.time() - t
print(f'Random access, 128 MB working set: {len(indices)/elapsed/1e6:.2f}M accesses/s')
print(f'(compare to L1 result to see cache effect)')
"
```

---

## 17. Interview Q&A

**Q1: What does `%steal` in CPU statistics mean and why does it matter for cloud-based Kafka?**

`%steal` is the fraction of wall-clock time during which a virtual CPU wanted to run but the hypervisor gave the physical CPU core to another virtual machine. From inside the VM, the CPU "disappeared" during that time — the process didn't progress, no instructions executed, but real time passed. It's effectively CPU time that was promised but not delivered.

For Kafka on a shared cloud VM, `%steal > 5%` is a significant problem for two reasons. First, throughput degrades proportionally: a broker that should process 500 MB/s at 0% steal processes only 475 MB/s at 5% steal and 300 MB/s at 40% steal. Second, and more critically for latency: steal introduces **jitter**. The steal doesn't happen at predictable intervals — it occurs in bursts when the host is busy. A network handler thread that should run every 1 ms might be delayed for 5 ms by a burst of steal, causing p99 latency to spike even though average latency looks acceptable. Kafka's p99 latency SLA can be violated by steal time that doesn't appear severe in average metrics. For production Kafka, use bare-metal instances or reserved dedicated hosts to eliminate steal time.

**Q2: Explain the three levels of CPU cache and describe a data engineering scenario where each level becomes the bottleneck.**

L1 cache (~32-64 KB per core, ~1 ns latency) fits only a tiny amount of working data. A bottleneck at L1 level is rare because it only happens when the CPU is doing extremely fine-grained random access in a very tight loop — for example, a Python list comprehension that dereferences pointers for each element (each dereference is a random memory access that may miss L1).

L2 cache (~256 KB–1 MB per core, ~3-5 ns latency) fits small to medium data structures. A Spark hash aggregation with a 500 KB hash table fits in L2 — very fast. The same aggregation with a 5 MB hash table overflows L2 into L3, increasing each probe from 3 ns to 10-40 ns. This is why Spark's broadcast hash join threshold of 10 MB represents approximately the point where a hash table starts causing significant L2 cache pressure.

L3 cache (~8-64 MB shared across all cores, ~10-40 ns latency) fits medium to large working sets. The classic L3 bottleneck in data engineering is a hash join where the build-side table doesn't fit in L3. A join against a 100 MB lookup table on every row: each probe misses L3 and goes to DRAM (60-100 ns). At 10 million rows/second, this adds 100 ns × 10M = 1 second of DRAM latency per second — effectively cutting throughput by 2×. The fix is the broadcast join optimization: replicate the table to each executor's memory and keep it warm in L3 by repeatedly accessing it in the inner loop.

**Q3: What is NUMA and how does it affect a Spark job running on a dual-socket server?**

NUMA (Non-Uniform Memory Access) describes multi-socket server architectures where each CPU socket has its own locally attached DRAM. Accessing local DRAM takes ~60-100 ns; accessing the other socket's DRAM via the inter-socket interconnect (Intel QPI/UPI) takes ~120-200 ns — roughly 2× the latency.

For a Spark executor running on a dual-socket server, NUMA affects performance when threads run on CPU cores from socket 0 while their data (JVM heap pages) was allocated from socket 1's DRAM. This happens because the Linux kernel allocates memory from whichever NUMA node has free pages, not necessarily the node where the requesting thread runs. The result is that every heap access by the Spark JVM incurs remote DRAM latency — 2× slower than local access, and competing for bandwidth on the inter-socket link.

For Spark on a NUMA machine, `numactl --interleave=all` is usually the right choice: it round-robins page allocation across both NUMA nodes, giving each socket's memory equal use. This avoids the pathological case where all pages are on node 1 but all threads run on node 0. For Kafka, `numactl --cpunodebind=0 --membind=0` is better: pin both threads and memory to the same node, minimizing latency at the cost of using only half the total DRAM bandwidth.

**Q4: What is `perf stat` and what does `insn per cycle = 0.4` tell you about a workload?**

`perf stat` attaches hardware performance counters — physical counters in the CPU that track cycles, instructions, cache misses, and branch mispredictions — to a process and reports aggregate statistics after execution. It has effectively zero overhead because the counters are in hardware.

`insn per cycle = 0.4` (IPC of 0.4) means the CPU is completing only 0.4 instructions per clock cycle, while a well-utilized CPU completes 2-4 instructions per cycle. An IPC of 0.4 indicates the CPU is spending most of its time stalled — waiting for data rather than executing instructions. The most common cause is **cache misses**: the CPU has instructions ready to execute but they need data that isn't in any cache level. The CPU must wait for DRAM to respond (60-100 ns = 200-300 cycles at 3 GHz) during which no useful instructions complete. This is called "memory-bound" behavior: adding more CPU cores won't speed up the workload because each core is sitting idle waiting for memory anyway. The fix is to restructure the algorithm to access memory in patterns that benefit from cache locality (sequential access, smaller working sets) or use columnar data formats that load fewer bytes per useful value.

**Q5: Describe the performance governor and why Kafka documentation recommends setting it to "performance."**

The CPU frequency governor decides at what P-state (frequency/voltage combination) the CPU operates at any given moment. The `powersave` governor (Linux default on most distros) runs at minimum frequency and ramps up only when it detects load. The `performance` governor runs at maximum frequency always.

The problem for Kafka with `powersave`: when a burst of producer traffic arrives, Kafka's network threads wake from `epoll_wait()` and begin processing messages. The CPU is in a low-frequency P-state (say 1.2 GHz). The governor detects the load increase and begins ramping up to maximum frequency (3.7 GHz) — but this ramp-up takes 10-50 ms depending on the governor implementation. During those 50 ms, Kafka's threads are running at 1.2 GHz while their work queue grows. From the producer's perspective, this manifests as a latency spike at the start of every traffic burst.

With the `performance` governor, CPUs are always at maximum frequency. There's no ramp-up delay. The first message in a burst is processed at 3.7 GHz, not 1.2 GHz. The power cost is real — the server draws more watts constantly — but for a Kafka broker that is the primary service on the machine, predictable low latency justifies the power overhead. The Kafka documentation, Confluent documentation, and all major Kafka deployment guides explicitly recommend: `performance` governor and `cpupower frequency-set -g performance`.

**Q6: What is CPU frequency throttling and how would you diagnose it on a Kafka server experiencing intermittent latency spikes?**

CPU throttling is a hardware protection mechanism that automatically reduces CPU clock frequency to prevent damage from overheating or to stay within power limits. From the application's perspective, the same code executes slower — more wall-clock time per instruction — with no visible error or warning in application logs.

To diagnose: first check thermal status with `sensors` (from lm-sensors) — temperatures above 80°C for extended periods indicate thermal throttling risk. Then check current CPU frequencies: `cat /proc/cpuinfo | grep "cpu MHz"` — if values are significantly below the CPU's rated frequency (visible in the "model name" line), throttling is active. For Intel CPUs, `turbostat --show PkgTmp,CoreTmp,Bzy_MHz,PkgWatt 5` is the definitive tool: it shows per-core temperature, current frequency, and package power draw at 5-second intervals. If `CoreTmp` is above 85°C and `Bzy_MHz` drops from 3700 MHz to 1800 MHz, thermal throttling is the cause.

For the Kafka latency correlation: generate a Kafka latency timeline from Kafka's own metrics (JMX: `NetworkProcessorAvgIdlePercent`, producer `record-send-rate`) and overlay the turbostat frequency data. If frequency drops coincide with latency spikes, thermal throttling is confirmed. Fix: improve cooling (check heatsink, ambient temperature), reduce workload (reduce num.io.threads), enable `performance` governor (which enables Turbo Boost management), or consider physical server upgrade. Setting `cpupower idle-set -D 1` to limit deep C-states can also help by keeping CPUs warmer between requests (less thermal cycling).

---

## 18. Cross-Question Chain

**Q1 [Interviewer]: Your Kafka broker's p99 produce latency is 50 ms, but p50 is 2 ms. Top shows 60% CPU utilization. What's your hypothesis?**

High p99 with low p50 is a classic sign of **occasional stalls rather than sustained overload**. If the system were CPU-saturated, both p50 and p99 would be high. Instead, most requests are fast (2 ms), but roughly 1 in 100 experiences a long delay (~50 ms). I'd think of causes that produce periodic, not constant, delays: JVM GC pauses, CPU frequency steps (powersave governor), THP compaction, OS scheduling jitter, or occasional resource contention. My first step is to look at GC logs: `jstat -gcutil $(pgrep -f kafka.Kafka) 1000 300` for 5 minutes — if I see GC pause events lasting 30-80 ms at irregular intervals, that's the culprit.

**Q2 [Interviewer]: jstat shows GC pauses at 2-5 ms (acceptable), but the latency spikes persist. What else could cause 50 ms spikes?**

Next hypothesis: CPU frequency stepping with `powersave` governor. When Kafka's threads are briefly idle (waiting for new requests between bursts), the CPU drops to low frequency. The first requests in the next burst run slowly while the CPU ramps back up. I'd check: `cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor` — if `powersave`, that's a likely cause. Switching to `performance` governor and running another 5-minute latency sample should eliminate the spikes. If the governor is already `performance`, I'd look at `turbostat` output for thermal events (CPU temperature causing Turbo Boost to be disabled) and check `%steal` in `mpstat` for hypervisor interference.

**Q3 [Interviewer]: You change to performance governor and the p99 improves to 8 ms, but it's still higher than expected. You notice in mpstat that one specific CPU core runs at 95% usr while all others are at 20-30%. What does this tell you?**

A single-core saturation pattern with one hot core and idle others is a strong indicator of a **single-threaded bottleneck**. In Kafka, the most common single-threaded hot spot is the `acceptor` thread (one thread handles all new connection accepts, then dispatches to network threads). With thousands of short-lived producer connections, the acceptor can saturate before the network threads do. Another candidate is the log compactor: Kafka's log compaction runs on a single thread per topic with `log.cleaner.threads=1` (default). If log compaction is running heavily on one partition family, it creates a single-threaded CPU hotspot.

I'd confirm with `top -H -p $(pgrep -f kafka.Kafka)` to see which specific Java thread corresponds to the hot Linux thread ID, then map that TID to a jstack thread name to identify the specific Kafka component.

**Q4 [Interviewer]: jstack identifies the hot thread as "kafka-log-cleaner-thread-0." Log compaction is saturating. What are your options?**

Log compaction being single-threaded means adding more compaction threads is the primary lever: increase `log.cleaner.threads` from the default 1 to 4-8 (bounded by available cores). Each additional thread handles compaction for different topic-partitions in parallel. However, more compaction threads also consume more I/O bandwidth — I'd check `iostat` to ensure the disk isn't already a bottleneck before increasing thread count.

A second option is to reduce the compaction work itself: if compaction is running because producers are writing the same keys at very high rate (producing many versions of each key), the root cause is producer behavior. Using log compaction with appropriate `segment.bytes` (larger segments = less frequent compaction triggers) or increasing `min.cleanable.dirty.ratio` (default 0.5 = don't compact until 50% of bytes are dirty) reduces compaction frequency. Third, if log compaction is not strictly needed for this topic, disabling it with `log.cleanup.policy=delete` eliminates the work entirely.

**Q5 [Interviewer]: After increasing log.cleaner.threads to 8 and observing for an hour, the p99 is now 5 ms. But you notice `sar -w` shows context switches jumped from 40,000/s to 280,000/s. Should you be concerned?**

An increase from 40,000 to 280,000 context switches per second is significant and worth investigating but not necessarily alarming in isolation. The 7× increase comes from adding 7 new compaction threads (from 1 to 8). Each new thread adds to the thread pool competing for CPU time slots. On a 32-core server with ~300 active Kafka threads total, 280,000 CS/s ÷ 32 cores = 8,750 CS/s/core. At 2 µs per context switch, that's 8,750 × 2 µs = 17.5 ms/s/core = 1.75% overhead per core — acceptable. The threshold I'd worry about is above ~200,000 CS/s on an 8-core machine or above 500,000 on a 32-core machine, where scheduling overhead becomes significant.

More importantly, the p99 latency improved (5 ms, down from 50 ms), which means the additional threads are delivering value without degrading performance. I'd monitor for a few hours: if p99 remains at 5 ms and CPU utilization stays reasonable, 8 cleaner threads is the right setting. If p99 starts rising again or CPU climbs above 85%, the cleaner threads are competing with network threads and I'd back down to 4.

**Q6 [Interviewer]: A week later, sar history shows that every night at 2 AM, CPU steal time jumps from 0% to 35% for 45 minutes on this server. How do you diagnose and fix this?**

A regular, time-scheduled steal time spike points directly to a neighboring VM's scheduled workload on the same physical host. The pattern: our Kafka VM shares a physical server with other VMs. Every night at 2 AM, some other VM starts a batch job (backup, database VACUUM, nightly ETL), consuming most of the physical CPUs and leaving our VM with only 65% of its requested CPU allocation.

To confirm: check if the steal time correlates with reduced Kafka throughput and increased latency using sar historical data: `sar -u -f /var/log/sysstat/sa$(date -d '1 day ago' +%d) -s 01:55:00 -e 02:50:00`. Cross-reference with customer reports of nightly latency issues.

Fix options in order of effectiveness: (1) Move to a dedicated/bare-metal instance — eliminates steal time entirely. This is the right choice for a latency-sensitive Kafka cluster. (2) Request a different physical host from the cloud provider (if supported) — may place the VM on a less-busy host. (3) Implement adaptive rate limiting in the Kafka clients: configure producers with a reduced `batch.size` and increased `linger.ms` during the 2-3 AM window, reducing the CPU demand from Kafka during the steal period. (4) Accept the nightly window and alert on it instead of treating it as an incident, while planning migration to dedicated instances.

---

## 19. Common Misconceptions

**"100% CPU utilization means the system is overloaded."**
100% CPU utilization means there are no idle cycles — the CPUs are always doing something. But "something" might be useful work (your application) or wasted cycles (GC, excessive context switching, kernel overhead). A Spark job at 100% `%usr` is running as fast as the hardware allows — that's the goal. A Kafka broker at 100% `%sys` is spending all its time in kernel code, likely from excessive small syscalls — that's a problem. Read the breakdown, not just the total.

**"High iowait means the disk is slow."**
`%iowait` is the fraction of time the CPU was idle while at least one process was blocked waiting for I/O. It says the CPU had nothing to do during that I/O wait — it doesn't say the disk was slow. A disk that takes 1 ms to serve a request produces exactly the same `%iowait` as a disk that takes 100 ms, as long as there's only one outstanding request. More critically, high `%iowait` can be caused by NFS I/O (network latency, not disk), memory-mapped file faults (not a "disk" at all from the application's perspective), or even swap I/O on a fast SSD that still takes 200 µs per access. Always cross-reference `%iowait` with `iostat` device statistics to identify the actual I/O device and its latency.

**"perf requires application code changes to be useful."**
`perf` works on any running process without source code access or recompilation. `perf top -p <pid>` shows which functions consume CPU cycles in a live JVM, Python process, or compiled binary. For JVMs, you get Java method names (not just "java" binary) by adding `-XX:+PreserveFramePointer` to the JVM flags and running `perf-map-agent` alongside — both can be done without rebuilding the application. For Python, `py-spy top --pid <pid>` requires no code changes. The only thing you miss without debug symbols is that some functions appear as memory addresses rather than names — but for identifying hot spots, even address-level profiling is often sufficient.

**"The performance governor wastes a lot of power."**
Modern Intel and AMD server CPUs with HWP (Hardware P-states) manage power very efficiently even with the `performance` governor. The governor sends a "I want maximum performance" hint, but the CPU's internal power management controller still reduces voltage and frequency on individual cores when they're idle, within the constraints set by Turbo Boost thermal management. The `performance` governor primarily prevents the ramp-up latency (the OS not knowing when to ramp frequency) and ensures Turbo Boost is available. On a Kafka broker running at 40% average CPU utilization with 60% idle cores, `performance` governor vs `powersave` is estimated at 5-15% total server power difference — significant in aggregate, but often justified by the latency predictability benefit.

---

## 20. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What does `%steal` in CPU statistics mean? | Fraction of time a VM's virtual CPU wanted to run but the hypervisor gave the physical core to another VM. The VM received ZERO CPU progress during this time. > 5% = significant performance impact; use dedicated instances for Kafka. |
| 2 | What is IPC (Instructions Per Cycle) and what does IPC < 0.5 indicate? | Measure of CPU efficiency: instructions completed per clock cycle. IPC < 0.5 means the CPU is stalled most of the time, typically waiting for data from memory (cache misses). The workload is memory-bound, not compute-bound. Adding CPU cores won't help. |
| 3 | What does `mpstat -P ALL 1` show? | Per-CPU utilization in 1-second intervals: %usr, %sys, %iowait, %soft, %steal, %idle per core. Use to detect single-core saturation (one core hot, others idle = single-threaded bottleneck) and steal time. |
| 4 | What command shows historical CPU stats from 2 hours ago? | `sar -u -s 10:00:00 -e 12:00:00` reads from today's sadc data file. For a specific date: `sar -u -f /var/log/sysstat/sa<DD>`. Requires sysstat installed and collecting. |
| 5 | What is `perf top` and what does it show? | A live profiler that shows which functions (symbols) are consuming the most CPU cycles, system-wide or for a specific PID (`perf top -p <pid>`). Shows "Overhead" per function — the fraction of cycles spent there. |
| 6 | How do you get Java method names in `perf top`? | Add `-XX:+PreserveFramePointer` to JVM flags (allows perf to walk the Java stack). Then run `perf-map-agent`'s `create-java-perf-map.sh <pid>` to create a JVM→symbol mapping file. Perf then shows Java method names instead of addresses. |
| 7 | What is CPU frequency throttling? | Hardware-level reduction of CPU clock speed to prevent overheating or stay within power limits. Silent from application perspective — code runs slower with no error. Detect with `turbostat`, `sensors`, or `cat /proc/cpuinfo | grep MHz`. |
| 8 | What CPU frequency governor should Kafka use and why? | `performance` — runs CPUs at maximum frequency always. Eliminates ramp-up latency (powersave takes 10-50 ms to reach max frequency on load burst). Set: `for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo performance > $cpu; done`. |
| 9 | What are C-states and how do they affect Kafka latency? | Idle states CPUs enter to save power. C0=active, C1=halt (~1 µs wake), C6=deep sleep (~200 µs wake). Deep C-states add wake-up latency when requests arrive. For Kafka: limit to C1 via `cpupower idle-set -D 1` or boot param `intel_idle.max_cstate=1`. |
| 10 | What is NUMA and how do you pin a Kafka JVM to a single NUMA node? | Non-Uniform Memory Access: each CPU socket has local DRAM (60-100 ns) and remote DRAM (120-200 ns, 2× slower). Pin with: `numactl --cpunodebind=0 --membind=0 java -Xmx4g kafka.Kafka`. Run `numactl --hardware` to see topology. |
| 11 | What does `%soft` in mpstat represent? | Time in software interrupt handlers (softirq). High `%soft` on a few cores means NIC interrupt processing is unbalanced. Fix: enable RSS (Receive Side Scaling) on the NIC to distribute interrupts, or manually set IRQ affinity via `/proc/irq/<N>/smp_affinity`. |
| 12 | What does `sar -w` show? | Process creation rate (`proc/s`) and context switch rate (`cswch/s`) over time. High `cswch/s` (> 5,000/core) suggests over-threading. Use to diagnose Kafka thread misconfiguration or Spark executor thread oversubscription. |
| 13 | What is THP and why disable it for Kafka? | Transparent Huge Pages: kernel promotes 4 KB pages to 2 MB automatically. The `khugepaged` daemon runs periodically, causing brief compaction stalls. These stalls manifest as periodic Kafka latency spikes. Disable: `echo never > /sys/kernel/mm/transparent_hugepage/enabled`. |
| 14 | How does the Python GIL affect CPU utilization on a multi-core server? | The Global Interpreter Lock ensures only one thread executes Python bytecode at a time. Multi-threaded Python programs appear as ONE core at 100% with all others idle in mpstat — despite having many threads. Fix: use `multiprocessing.Pool` (separate processes), or use C-extensions/NumPy that release the GIL. |
| 15 | What is `taskset` and when would you use it for Kafka? | Sets or reads a process's CPU affinity — which cores it can run on. Use to pin Kafka to NUMA node 0 CPUs: `taskset -c 0-15 -p $(pgrep -f kafka.Kafka)`. In systemd: `CPUAffinity=0-15`. Reduces cache pollution from other processes. |
| 16 | What does `perf stat` `-e LLC-load-misses` measure? | LLC (Last Level Cache = L3) load misses: how many memory reads required going to DRAM (L3 cache miss). High LLC miss rate (>10% of loads) means the working set doesn't fit in L3. The workload is memory-bound; optimize for cache locality or increase L3 size. |
| 17 | What does `numastat -p <pid>` show? | Per-NUMA-node memory allocation for a specific process: how many MB of its heap are on each NUMA node. Ideal: most memory on the same node as the process's CPUs. Large "remote" allocation = NUMA penalty on every heap access. |
| 18 | What is `irqbalance` and when should you stop it? | Daemon that automatically distributes hardware interrupt handling across CPUs. Useful for general-purpose servers. Stop it when you want manual IRQ affinity pinning (e.g., pin NIC IRQs to the same NUMA node as Kafka's network threads): `systemctl stop irqbalance`. |
| 19 | What is `py-spy` and why is it preferred over cProfile for production? | A sampling profiler for Python that attaches to a running process without modification. Uses ptrace — no code changes, no restart, minimal overhead (~0.1%). `py-spy top --pid <pid>` for live view; `py-spy record --output flame.svg` for flame graph. cProfile requires code instrumentation. |
| 20 | What does `context switch rate > 5,000/core/sec` indicate for a Kafka broker? | Over-threading: more Java threads than the CPU can efficiently schedule. Each context switch wastes ~2 µs. At 5,000/core, that's 10 ms/s/core = 1% overhead. Above 20,000/core, overhead becomes significant. Fix for Kafka: reduce `num.network.threads`, `num.io.threads`, `num.replica.fetchers` to values closer to core count. |

---

## 21. Module Summary

CPU is the final layer of the Linux systems stack — but it's rarely the first bottleneck in data engineering. Storage and memory pressure manifest as CPU iowait or excessive syscall time before CPU becomes the primary constraint. Understanding which category of CPU time is consumed is the prerequisite for any optimization.

**`mpstat -P ALL 1`** gives the live per-core view. The pattern of utilization tells the story: one core at 95% with others idle is a single-threaded bottleneck (Python GIL, Kafka acceptor, log compactor). All cores at 100% with high context switch rate is over-threading. `%steal` above 5% is hypervisor interference — invisible to the application, measurable from the OS, cured only by moving to dedicated hardware.

**`sar`** is the historical record. When an incident is reported the next morning, `sar -u -s 14:00:00 -e 15:00:00 -f /var/log/sysstat/sa26` reconstructs exactly what happened to CPU, I/O, memory, and network during the event window. Without `sar` data collection enabled, post-incident analysis is guesswork. Enable it and set the collection interval to 1 minute on any production server.

**`perf`** bridges the gap between "this process uses 100% CPU" and "this specific function is consuming 80% of those cycles." `perf stat` gives the IPC metric (memory-bound vs compute-bound), `perf top` identifies hot functions in real time, and `perf record` + flame graphs provide the full call-stack picture for systematic optimization.

**CPU throttling** — thermal or power-related — is the silent performance degrader. A Kafka broker that shows 50 ms p99 spikes every 30 seconds with no application-level cause almost always traces to GC, THP compaction, or CPU frequency stepping. The `performance` governor, disabled THP, and C-state limits are the three configuration settings that eliminate the most common sources of latency jitter for data engineering workloads.

**NUMA awareness** matters for dual-socket servers. `numactl --cpunodebind=0 --membind=0` for Kafka (minimize cross-socket latency) and `numactl --interleave=all` for Spark (maximize memory bandwidth) are the two patterns that cover the majority of production deployments.

---

**SYS-LNX-101: 5 of 5 complete. Course finished.**  
**Next course: SYS-LNX-102 — Linux Performance Engineering**  
*(covers advanced profiling, kernel tuning, BPF/eBPF, network performance)*

Say "go ahead" when ready for the next course.
