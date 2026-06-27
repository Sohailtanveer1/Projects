# M04: CPU Performance Analysis

**School:** CS Foundations (CSF)  
**Course:** CSF-ARC-101 — How Computers Execute Programs  
**Module:** 04 of 05  
**Difficulty:** ★★★★☆  
**Estimated study time:** 5–7 hours  
**Last updated:** 2026-06-27

---

## Table of Contents

1. [Why This Module Exists](#1-why-this-module-exists)
2. [Prerequisites](#2-prerequisites)
3. [Learning Objectives](#3-learning-objectives)
4. [First Principles](#4-first-principles)
5. [Architecture](#5-architecture)
6. [Execution Flow](#6-execution-flow)
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

M01 through M03 built a complete mental model of how CPUs execute programs: the fetch-decode-execute cycle, pipelining and out-of-order execution, SIMD, ISAs, system calls, memory models. This module is the bridge from theory to diagnosis. **Knowing how the CPU works is useless unless you can measure what it is actually doing.**

Every performance problem in data engineering has the same structure: something is slow, and you do not know why. The wrong approach — the one most engineers take — is to guess. "It's probably the shuffle." "The UDF must be the bottleneck." "We should add more partitions." Guessing at performance problems without measurement produces random changes that sometimes help, sometimes hurt, and never produce a systematic understanding.

The right approach is to measure first, then optimize. This module teaches the measurement tools for the CPU layer: `perf stat` for aggregate CPU efficiency, `perf record` and flame graphs for identifying where time is spent, hardware performance counters for cache miss and branch misprediction rates, `strace` for system call auditing, and Python-specific profilers (`cProfile`, `py-spy`, `tracemalloc`) for the Python layer that sits above the CPU.

**Why data engineers specifically need this:**

1. A Spark stage is "slow." Is it CPU-bound, I/O-bound, or network-bound? `perf stat` answers this in 30 seconds.
2. A Python UDF runs at 100K rows/second when you expected 5M rows/second. Is the bottleneck the Python interpreter overhead, a data-dependent branch, or a cache miss? A flame graph answers this in 2 minutes.
3. A Kafka broker is at 100% CPU but low throughput. Is it spending time in syscalls, in SSL, or in consumer group rebalancing? `perf record` answers this.
4. A dbt model takes 4× longer after adding a column. Is the Postgres query planner choosing a wrong join algorithm? `EXPLAIN ANALYZE` answers this — but understanding its output requires knowing what the CPU is doing during each node.

This module transforms you from an engineer who guesses into an engineer who measures.

---

## 2. Prerequisites

- **M01: The Von Neumann Machine** — fetch-decode-execute, registers, IPC.
- **M02: CPU Microarchitecture** — pipeline hazards, branch prediction, SIMD, out-of-order execution.
- **M03: The ISA Contract** — syscalls, system call overhead, calling conventions.
- Basic Linux command-line familiarity (navigating directories, `sudo`, pipes).

---

## 3. Learning Objectives

By the end of this module you will be able to:

1. Run `perf stat` on any process and interpret the output: IPC, branch-miss rate, cache-miss rate, context switches.
2. Generate a CPU flame graph for any Python or JVM process and identify the hot path.
3. Distinguish CPU-bound, memory-bound, I/O-bound, and syscall-bound workloads from `perf stat` output alone.
4. Apply Brendan Gregg's USE method (Utilization, Saturation, Errors) to a slow data pipeline.
5. Profile a Python process with `cProfile`, `py-spy`, and `tracemalloc` and identify the function consuming the most CPU time or memory.
6. Use `strace -c` to count syscalls and identify unexpected system call patterns in a data pipeline.
7. Interpret the key hardware performance counters: IPC, LLC-load-misses, branch-misses, instructions retired.
8. Connect profiling output directly to actionable data engineering optimizations (e.g., "this flame graph shows 60% of time in Python marshal/unmarshal — switch to Pandas UDF").

---

## 4. First Principles

### You Cannot Optimize What You Cannot Measure

The fundamental principle of performance engineering is: **measure before you optimize**. Without measurement:

- You cannot confirm a bottleneck exists (you might be optimizing something that contributes 2% of runtime).
- You cannot confirm a fix worked (placebo improvements are common).
- You cannot explain *why* the optimization works (so you cannot apply the same reasoning elsewhere).
- You will optimize the wrong thing (Amdahl's Law: optimizing a component that is 10% of runtime, even perfectly, only improves total runtime by 10%).

**Amdahl's Law:**

```
Speedup = 1 / ((1 - P) + P/S)

Where:
  P = fraction of time spent in the optimized component
  S = speedup factor of that component

Example: 
  A job takes 100s. The Python UDF (P=0.20, 20s) is replaced with a Pandas UDF (S=15x).
  Speedup = 1 / (0.80 + 0.20/15) = 1 / 0.813 = 1.23x
  
  Total time drops from 100s to 81s. Not 15x — because 80% of the time was elsewhere.
  
  Before optimizing the UDF, you should ask: what is the other 80%? Is THAT the real bottleneck?
```

### Hardware Performance Counters

Modern CPUs have dedicated hardware circuits called **Performance Monitoring Units (PMUs)** that count events at the silicon level. These counters are read without perturbing the workload — the CPU increments them as a side effect of normal execution.

Key counters:

| Counter | What It Counts | Healthy Value |
|---|---|---|
| `instructions` | Total instructions retired | — (baseline) |
| `cycles` | Total CPU clock cycles elapsed | — (baseline) |
| `IPC` | Instructions per cycle (= instructions/cycles) | > 2.0 for compute; < 0.5 = stalled |
| `branch-misses` | Predicted branches that were wrong | < 1% of `branches` |
| `cache-misses` | L1/L2/L3 cache lookups that missed | varies |
| `LLC-load-misses` | Last-level cache (L3) misses → DRAM | < 5% of `cache-references` |
| `context-switches` | Times the OS preempted this process | < 1000/s for a healthy compute process |
| `page-faults` | Virtual memory faults (new memory mapped) | < 1000/s in steady state |

These counters are accessible via Linux's `perf_event_subsystem` — the kernel interface that `perf` uses.

### The Profiling Spectrum

Performance analysis tools sit on a spectrum from **lightweight/inaccurate** to **heavy/accurate**:

```
Lightweight (low overhead, less detail)
    ↑
    │  wall clock timing     (time python script.py)
    │  Python cProfile       (function-level, ~10% overhead)
    │  perf stat             (aggregate counters, <1% overhead)
    │  perf record -F 99     (sampling, ~5% overhead)
    │  perf record -F 9999   (high-frequency sampling, ~20% overhead)
    │  eBPF / bpftrace       (tracing, variable overhead)
    │  strace                (syscall tracing, 2-10x slowdown)
    │  strace -f -e all      (full trace, 10-100x slowdown)
    ↓
Heavy (high overhead, full detail)
```

The key insight: **use the lightest tool that can identify the bottleneck**. Start with `perf stat` (< 1% overhead, tells you the class of problem). If you need to identify the specific function, use `perf record` (sampling profiler, ~5% overhead). Only use `strace` or full tracing if the other tools can't answer the question.

---

## 5. Architecture

### The Linux `perf` Tool Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    LINUX PERF ARCHITECTURE                               │
│                                                                           │
│  User Space:                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  perf tool (CLI)                                                    │ │
│  │  • perf stat   — reads aggregate counters at end of run            │ │
│  │  • perf record — samples call stacks at fixed frequency (e.g. 99Hz)│ │
│  │  • perf report — reads perf.data and renders a text report         │ │
│  │  • perf script — outputs raw samples for flamegraph.pl             │ │
│  └──────────────────────────────┬─────────────────────────────────────┘ │
│                                 │ ioctl() / perf_event_open()           │
│  Kernel Space:                  │                                        │
│  ┌──────────────────────────────▼─────────────────────────────────────┐ │
│  │  perf_event subsystem (kernel/events/core.c)                        │ │
│  │  • Manages PMU programming                                          │ │
│  │  • Handles NMI (non-maskable interrupt) for sampling                │ │
│  │  • Writes samples to perf.data ring buffer                         │ │
│  └──────────────────────────────┬─────────────────────────────────────┘ │
│                                 │                                        │
│  Hardware:                      │                                        │
│  ┌──────────────────────────────▼─────────────────────────────────────┐ │
│  │  PMU (Performance Monitoring Unit)                                  │ │
│  │  • 4–8 programmable counters per core                               │ │
│  │  • Configured to count specific events (branch-misses, LLC-misses) │ │
│  │  • Fires NMI when counter overflows → kernel records call stack     │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### Sampling Profilers vs Tracing Profilers

**Sampling profiler** (`perf record`, `py-spy`): At fixed intervals (e.g., every 10ms), interrupts the process and records the current call stack. After N samples, the functions that appear most often are the "hot path." Overhead: 1–10% depending on frequency.

**Tracing profiler** (`strace`, `ltrace`, eBPF): Records every occurrence of a specific event (every syscall, every function call, every memory allocation). Overhead: 10–100× for function-level tracing; eBPF is lower because it runs in the kernel.

**Instrumentation profiler** (`cProfile`): Inserts timing hooks at every function call/return. Complete data, but changes the timing behavior of the program (observer effect).

### Flame Graph Structure

A flame graph visualizes a sampling profiler's call stack data:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    FLAME GRAPH (reading guide)                            │
│                                                                           │
│  Y-axis: call stack depth (bottom = top of stack = on-CPU function)      │
│           NOT time — the vertical position just shows call depth         │
│  X-axis: sorted alphabetically within each stack frame level             │
│           width = fraction of total samples                               │
│  Width:  PROPORTIONAL TO TIME — wider = more CPU time                   │
│                                                                           │
│  ████████████████████████████████████████████████████  main()            │
│  ████████████████████████   ██████████████████████████  work() | cleanup()
│  ████████████████████████   ████████████████  ████████  inner() | fast() | slow()
│  ██████  █████  ███████████   (tall thin = deep call, rarely sampled)   │
│  │       │      └─ wide & deep = the HOT PATH                            │
│  │       └─ wide & flat = tail recursion or tight loop                   │
│  └─ narrow = called rarely                                               │
│                                                                           │
│  Rule: Find the WIDEST frame near the TOP of the stack.                  │
│        That is where the CPU is spending the most time.                  │
│        That is your optimization target.                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

### The Python Profiling Stack

Python has multiple layers, each with its own profiling tool:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    PYTHON PROFILING LAYERS                                │
│                                                                           │
│  Layer               Tool                  What it shows                 │
│  ─────────────────   ──────────────────    ──────────────────────────── │
│  Python functions    cProfile / profile    Time per Python function       │
│  Python + C calls    py-spy (sampling)     Wall time per call stack       │
│  Memory usage        tracemalloc           Allocations per call site      │
│  Memory usage        memory_profiler       Line-by-line memory delta      │
│  CPU native          perf record           x86-64 instruction-level       │
│  Syscalls            strace                Kernel calls and durations     │
│  All of the above    pyspy + perf together Full picture                   │
│                                                                           │
│  Note: cProfile only sees Python-level calls.                            │
│        A C extension that takes 10s appears as a 10s "leaf" in cProfile. │
│        py-spy or perf can show what happens inside that C extension.     │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Execution Flow

### Diagnosing a Slow PySpark Job: Step by Step

A PySpark job on 100M rows is taking 45 minutes. Expected time: 5 minutes. Here is the systematic diagnosis flow:

**Step 1 — Categorize with `perf stat` (2 minutes)**

```bash
# Find the Spark executor PID
jps | grep Executor
# 12345 CoarseGrainedExecutorBackend

# Attach perf to the running executor for 30 seconds
sudo perf stat -p 12345 --timeout 30000

# Output example (CPU-bound):
# Performance counter stats for process id '12345':
#
#      120,234,112,456  instructions          #    3.21  insn per cycle
#       37,456,789,012  cycles
#           12,456,789  branch-misses         #    0.89% of all branches
#           89,456,123  cache-misses          #    4.23% of all cache refs
#                2,134  context-switches
#
#       30.012 seconds time elapsed
```

Reading the output:
- IPC = 3.21 → CPU is working hard, well-utilized (> 2.0 is healthy for compute)
- Branch-miss rate = 0.89% → fine (< 1%)
- Cache-miss rate = 4.23% → borderline (> 5% is a problem)
- Context-switches = 2,134 in 30s → ~71/s, acceptable

Conclusion from this output: the job is **CPU-bound, not I/O-bound or syscall-bound**. The high IPC means the CPU is executing instructions efficiently. Look for algorithmic inefficiency or unnecessary computation.

**Output example (I/O or memory-bound):**

```bash
#      12,023,411,246  instructions          #    0.31  insn per cycle  ← LOW IPC
#      38,456,789,012  cycles
#           1,456,789  branch-misses         #    0.12% of all branches
#          89,456,123  LLC-load-misses       # ← HIGH LLC misses = DRAM accesses
#
# IPC < 0.5 = pipeline stalled waiting for memory
# High LLC-load-misses = data is not fitting in cache
# → This job is MEMORY/IO-BOUND, not CPU-bound
```

**Step 2 — Find the hot path with flame graph (5 minutes)**

```bash
# Record 30 seconds of stack samples at 99 Hz
sudo perf record -F 99 -p 12345 --call-graph dwarf -o perf.data -- sleep 30

# Convert to flame graph
sudo perf script -i perf.data | \
    stackcollapse-perf.pl | \
    flamegraph.pl > spark_executor.svg

# Open spark_executor.svg in a browser
```

Find the widest frame near the top. If it shows `org.apache.spark.sql.execution.python.EvaluatePython.makeFromJava`, that is Python serialization/deserialization — a Python UDF bottleneck. If it shows `org.apache.spark.sql.execution.aggregate.HashAggregateExec.processInputIter`, the aggregation itself is the bottleneck.

**Step 3 — Python-specific profiling with `py-spy` (5 minutes)**

For PySpark's Python worker processes:

```bash
# Find the Python worker PID (spawned by the executor)
ps aux | grep pyspark | grep -v grep

# py-spy top: live view of Python hotspots (like top, but for Python)
sudo py-spy top --pid 99999

# py-spy record: flamegraph for Python
sudo py-spy record -o py_flame.svg --pid 99999 --duration 30
```

`py-spy` samples the Python call stack using `/proc/PID/mem` without any instrumentation overhead and works even on running production processes.

**Step 4 — Verify with `strace -c` (1 minute)**

```bash
# Count syscalls for 10 seconds
sudo strace -c -p 12345 --timeout 10
```

If the output shows millions of `futex()` calls, the job is contending on locks. If it shows millions of `recvfrom()` calls, it is doing tiny reads. If it shows few syscalls of any kind, the bottleneck is in userspace computation.

---

## 7. Mental Models

### Mental Model 1 — IPC as a Car's Speedometer

IPC (Instructions Per Cycle) tells you how efficiently the CPU is using its execution units. Think of it as a car's speedometer, but for the CPU. A theoretical maximum IPC for a 4-wide superscalar CPU is 4.0 (all four execution ports busy every cycle). In practice, data-parallel numeric code achieves 2–4 IPC. Python interpreter loops achieve 0.3–0.8 IPC (many data-dependent branches, small operations). Memory-bound code bottlenecked on DRAM achieves 0.1–0.3 IPC (pipeline stalled waiting for data).

When you see low IPC, the CPU is a racing car idling at traffic lights — all the horsepower is there, but it's not moving. The question becomes: what are the traffic lights? Branch mispredictions? Cache misses? Syscalls?

### Mental Model 2 — A Flame Graph as a Self-Portrait

A flame graph is the CPU's self-portrait at a moment in time. Every sample says "right now, the CPU is here in this call stack." The frames that appear in the most samples are the frames where the CPU spent the most time. The widest frame at the top of the stack is the bottleneck — the function that the CPU was executing (not waiting for, not calling — actually executing) most often.

The key insight: a frame that is wide at the *bottom* of the stack is not necessarily the bottleneck — it just means many samples passed through it. The bottleneck is the widest frame at the *top* (the frame that didn't call anything further, i.e., the leaf frame where the CPU actually spent cycles).

### Mental Model 3 — `perf stat` as a Doctor's Vital Signs

`perf stat` is a 5-second checkup. It gives you vital signs: IPC (metabolic rate), branch-miss rate (coordination), cache-miss rate (memory health), context-switches (multitasking load). From these four numbers alone, you can categorize the illness:

- High IPC, low cache misses: healthy compute workload. Look for algorithmic improvement.
- Low IPC, high LLC misses: memory-bound. Look for data layout and locality improvements.
- Low IPC, high branch misses: branch-prediction-bound. Look for branchless alternatives.
- High context-switches, low IPC: over-threaded or I/O-blocked. Look at parallelism design.

### Mental Model 4 — Brendan Gregg's USE Method

The USE method is a systematic checklist for resource analysis:

**For every resource (CPU, memory, disk, network):**
- **U — Utilization:** How busy is the resource? (CPU: `mpstat` → % user + sys)
- **S — Saturation:** Is there more work queued than the resource can handle? (CPU: `vmstat` → run queue > CPU count)
- **E — Errors:** Are there hardware or software errors? (CPU: `perf stat` → retired instructions vs speculative)

Apply to a Spark cluster:

| Resource | U | S | E |
|---|---|---|---|
| CPU | `mpstat -P ALL 1` | `vmstat` run queue length | `perf stat` page-faults, branch-misses |
| Memory | `free -h` % used | `vmstat` swap activity | `dmesg` OOM killer messages |
| Disk | `iostat -x 1` %util | `iostat` await time | `dmesg` I/O errors |
| Network | `sar -n DEV 1` %ifutil | `ss -s` retransmit rate | `netstat -s` errors |

---

## 8. Failure Scenarios

### Failure 1 — Spark Job Reported as "CPU Bound" But Flame Graph Shows GC

**Symptom:** `perf stat` shows high IPC (> 2.5), but the Spark job is still slow. The Spark UI shows "GC time" in the executor metrics at 20–30% of task time.

**Root cause:** The JVM garbage collector is running frequently, stopping application threads. `perf stat` sees the GC threads doing work (compacting the heap, marking live objects) and reports high IPC — because GC itself is computationally intensive. But from the application's perspective, all progress is paused during GC.

**How to detect:**
```bash
# Option 1: Spark UI → Executors tab → "GC Time" column
# Healthy: GC time < 5% of task time
# Problem: GC time > 15% of task time

# Option 2: JVM GC log
spark-submit --conf spark.executor.extraJavaOptions="-XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/tmp/gc.log" ...
grep "Pause" /tmp/gc.log | awk '{print $NF}' | sort -n | tail -10

# Option 3: perf with GC filtering
sudo perf record -F 99 -p EXECUTOR_PID --call-graph dwarf -- sleep 30
sudo perf script | grep -i "gc\|safepoint" | wc -l
```

**Root causes of high GC:**
1. Executor memory too small → heap fills rapidly → frequent minor GC.
2. Many small Python objects created per row (Python UDF returning dicts or complex objects).
3. Spark's Dataset/DataFrame operations creating many intermediate Java objects.

**Fix:**
```bash
# Increase executor memory
--conf spark.executor.memory=8g
--conf spark.executor.memoryOverhead=2g

# Use Kryo serializer (smaller objects)
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer

# Switch Python UDFs to Pandas UDFs (Arrow, not Python objects)
```

---

### Failure 2 — Python Script Has 100% CPU But 20× Slower Than Expected

**Symptom:** A Python data processing script runs at 100% CPU. The developer expects it to process 10M records in 30 seconds; it takes 600 seconds. `perf stat` shows IPC = 0.45 (low).

**Root cause:** The script uses Python's `for` loop over a list of dicts, doing per-row arithmetic. The low IPC is from the Python interpreter's opcode dispatch (data-dependent branches) and pointer-chasing through Python object heap.

**How to detect:**
```bash
# cProfile: where is time spent?
python -m cProfile -s cumulative slow_script.py 2>&1 | head -30

# Expected output pattern:
# ncalls  tottime  percall  cumtime  percall filename:lineno(function)
# 10000000   45.2    0.000   45.2    0.000 slow_script.py:12(transform_row)
#            ← 45s in a single Python function called 10M times
#            ← The function IS the bottleneck, not I/O or network

# py-spy flamegraph for live process:
sudo py-spy record -o flame.svg --pid $(pgrep -f slow_script.py) --duration 30
# The flamegraph will show Python's eval loop dominating
```

**Fix:**
```python
# Before: Python for loop (IPC ~0.4)
result = [transform(row) for row in rows]

# After: NumPy vectorized (IPC ~3.5)
import numpy as np
values = np.array([row['value'] for row in rows])
result = np.where(values > 0, values * 2.0, 0.0)
```

---

### Failure 3 — Kafka Broker at 100% CPU, Low Throughput

**Symptom:** A Kafka broker is at 100% CPU (per `htop`) but throughput (per Kafka's JMX metrics) is only 30% of expected. Adding more CPU (via instance upgrade) doesn't help proportionally.

**Root cause:** The broker is spending CPU time in TLS encryption/decryption, not in Kafka's core produce/consume path. TLS is CPU-intensive: each `SSL_write()` and `SSL_read()` call involves asymmetric crypto (for handshakes) and symmetric crypto (AES-GCM for data).

**How to detect:**
```bash
# Flame graph the broker
sudo perf record -F 99 -p $(pgrep -f kafka.Kafka) --call-graph dwarf -- sleep 30
sudo perf script | stackcollapse-perf.pl | flamegraph.pl > kafka.svg

# Look for wide frames containing:
# SSL_write, SSL_read, EVP_EncryptUpdate, EVP_DecryptUpdate, AES
# These indicate TLS overhead

# Alternatively, strace to see SSL-related syscalls:
sudo strace -c -p $(pgrep -f kafka.Kafka) --timeout 10
# High counts of read()/write() vs sendfile() ratios indicate TLS
# (TLS prevents zero-copy sendfile — the data must go through user space)
```

**Fix options:**
1. Enable AES-NI hardware acceleration: verify `grep aes /proc/cpuinfo` shows `aes` flag. JVM on Graviton uses ARMv8 Crypto Extensions automatically. On x86-64, ensure JVM uses OpenSSL with AES-NI (`java.security.providers`).
2. Move to TLS 1.3 (more efficient handshake, better cipher selection).
3. For internal brokers with network-level security (VPC + security groups), consider whether inter-broker TLS is needed.
4. Use Kafka's `ssl.engine.factory.class` to provide a hardware-accelerated SSL provider.

---

### Failure 4 — `strace` Reveals Thousands of `stat()` Calls Per Row

**Symptom:** An Airflow task that reads 1000 Parquet files from S3 takes 45 minutes. The files total 2 GB, which should transfer in ~2 minutes at typical S3 throughput. `htop` shows low CPU usage and low memory pressure.

**Root cause:** The code opens each file individually, calls `stat()` to get the file size, then reads it. On a network filesystem (S3 via s3fs, HDFS via FUSE, or NFS), each `stat()` call is a network round-trip (~10–50 ms). 1000 files × 50 ms = 50 seconds just for the metadata lookups, before any data is read.

**How to detect:**
```bash
sudo strace -c -e trace=stat,openat,read,close python3 airflow_task.py

# Output:
# % time     seconds  usecs/call     calls    errors syscall
# 87.3      40.123456      40123      1000           stat
#  8.2       3.780234       3780      1000           openat
#  4.5       2.070123       2070      1000           read
#
# 1000 stat() calls each taking ~40ms → 40 seconds of pure metadata overhead
```

**Fix:**
- Use `pyarrow.dataset.dataset()` which batches file discovery and avoids per-file `stat()` calls.
- Use S3 SELECT or partition pruning to avoid opening unnecessary files.
- Cache file metadata when re-reading the same files repeatedly.
- If using HDFS, use `FileSystem.listStatus()` which returns metadata for many files in one RPC.

---

## 9. Recovery Procedures

### When `perf` Requires Kernel Parameters

`perf` often fails with permission errors in cloud environments:

```bash
# Symptom: perf stat: Permission denied
sudo sysctl kernel.perf_event_paranoid=-1
# Temporary (resets on reboot). For permanent: add to /etc/sysctl.conf

# For Docker containers (perf needs SYS_ADMIN capability):
docker run --cap-add SYS_ADMIN --security-opt seccomp=unconfined my_image
# Or use perf inside the container with /proc/sys/kernel/perf_event_paranoid

# Alternative if perf is unavailable: use /proc/PID/stat for CPU usage
# and Python's cProfile for function-level profiling
```

### When Flame Graphs Show Only Hex Addresses (Missing Symbols)

```bash
# JVM: Add frame pointers (required for perf call graph)
spark-submit --conf spark.executor.extraJavaOptions="-XX:+PreserveFramePointer" ...

# Python C extensions: ensure debug symbols are installed
apt install python3-dbg  # CPython with debug symbols
apt install libpython3.11-dbg

# For NumPy/PyArrow native libraries, install debug symbols:
apt install python3-numpy-dbg  # (if available) or build from source with -g

# Alternative: use py-spy instead of perf for Python — it reads Python frames
# directly from the Python interpreter's internal stack, bypassing symbol resolution
sudo py-spy record -o flame.svg --pid PID --duration 30
```

### When `strace` Slows the Process Too Much

```bash
# Use strace -c (counting mode) instead of full trace — much lower overhead
sudo strace -c -p PID --timeout 10

# Use eBPF for lower-overhead syscall tracing:
sudo bpftrace -e 'tracepoint:syscalls:sys_enter_* { @[probe] = count(); }' --timeout 10

# For production: use Linux's `auditd` for syscall auditing at minimal overhead
```

---

## 10. Trade-offs

### Sampling Rate vs Overhead

| Sampling frequency | Overhead | Granularity | Use case |
|---|---|---|---|
| 1 Hz | ~0% | Very coarse | Long-running jobs where any overhead is unacceptable |
| 99 Hz | ~1–2% | Coarse (10ms resolution) | Standard profiling — the conventional choice |
| 999 Hz | ~10% | Medium (1ms resolution) | Identifying fast functions missed at 99 Hz |
| 9999 Hz | ~40% | Fine (0.1ms resolution) | Micro-benchmarking only; too much overhead for production |

99 Hz is the conventional default because it avoids lockstep with 100 Hz system timer intervals (which would sample the same code point every time if they were synchronised), while keeping overhead below 2%.

### cProfile vs py-spy vs perf

| Tool | Overhead | Python stack | C extension stack | Live process | Production safe |
|---|---|---|---|---|---|
| `cProfile` | ~15% | ✅ Full detail | ❌ Opaque leaf | Must instrument | No (too slow) |
| `py-spy` | ~1% | ✅ Full detail | ❌ Opaque leaf | ✅ Attach live | Yes |
| `perf record` | ~5% | ❌ JIT frames hard | ✅ Full detail | ✅ Attach live | Yes (brief) |
| `py-spy` + `perf` | ~6% combined | ✅ | ✅ | ✅ | Brief only |

For diagnosing a Python data pipeline: start with `py-spy top` (zero-config live view), then `py-spy record` if you need a persistable flame graph. Only drop to `perf record` if the bottleneck is inside a C extension (NumPy, PyArrow, libssl).

---

## 11. Comparisons

### Interpreting `perf stat` for Different Workload Types

**Type 1 — Compute-bound (NumPy SIMD):**

```
  instructions:              45,234,567,890   #    3.82 insn per cycle
  cycles:                    11,843,112,345
  branch-misses:                  2,345,678   #    0.12% of all branches
  LLC-load-misses:                    4,567   #    0.002% of LLC loads
  context-switches:                      23

Interpretation:
  IPC = 3.82 → excellent, CPU near peak throughput
  branch-miss 0.12% → excellent
  LLC misses near zero → data fits in cache
  context-switches 23 → background noise, not the issue
  → This workload is well-optimized. Marginal gains only from algorithm changes.
```

**Type 2 — Memory-bound (random access pattern):**

```
  instructions:               3,456,789,012   #    0.28 insn per cycle
  cycles:                    12,345,678,901
  branch-misses:                  1,234,567   #    0.45% of all branches
  LLC-load-misses:               45,678,901   #   34.5% of LLC loads  ← VERY HIGH
  context-switches:                      18

Interpretation:
  IPC = 0.28 → very low, CPU mostly stalled
  LLC misses 34.5% → most L3 lookups go to DRAM (200-300 ns each)
  Pipeline stall = CPU waiting for DRAM 70%+ of cycles
  → Fix: improve data locality. Sort before join. Increase Spark partition size.
     Consider columnar layout. Use broadcast join if one side is small.
```

**Type 3 — Syscall-bound (Kafka consumer with tiny batches):**

```
  instructions:               2,345,678,901   #    0.45 insn per cycle
  cycles:                     5,234,567,890
  branch-misses:                    456,789   #    0.23% of all branches
  LLC-load-misses:                1,234,567   #    1.2% of LLC loads
  context-switches:               1,234,567   #   ← VERY HIGH

Interpretation:
  IPC = 0.45 → low
  LLC misses low → data is in cache, not the problem
  context-switches = 1.2M in the measurement period → extreme
  Each context-switch = kernel entry + TLB flush + cache pollution
  → Fix: increase batch sizes. Reduce syscall frequency. Use epoll properly.
     For Kafka: increase fetch.min.bytes, fetch.max.wait.ms.
```

**Type 4 — Branch-prediction-bound (Python UDF over random data):**

```
  instructions:              12,345,678,901   #    1.12 insn per cycle
  cycles:                    11,023,456,789
  branch-misses:              1,987,654,321   #   18.7% of all branches  ← VERY HIGH
  LLC-load-misses:                2,345,678   #    0.5% of LLC loads

Interpretation:
  IPC = 1.12 → moderate (not as low as memory-bound)
  branch-misses 18.7% → extremely high; ~20 cycles wasted per miss
  LLC misses low → data in cache, not the problem
  → Fix: branchless NumPy operations (np.where, np.maximum).
     Switch Python UDF to Pandas UDF. Sort data to improve predictability.
```

---

## 12. Production Examples

### Diagnosing a Slow dbt Model with EXPLAIN ANALYZE

A dbt model that ran in 2 minutes started taking 25 minutes after a new column was added. The database is Postgres.

```sql
-- Step 1: Run EXPLAIN ANALYZE (Postgres's built-in query profiler)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    o.order_id,
    c.customer_name,
    SUM(oi.price * oi.quantity) as total_value
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.created_at >= '2024-01-01'
GROUP BY o.order_id, c.customer_name;
```

```
-- EXPLAIN ANALYZE output (annotated):
Hash Join  (cost=234567.89..456789.01 rows=1234567 width=72)
           (actual time=12345.678..23456.789 rows=1234567 loops=1)
  Buffers: shared hit=12345 read=234567  ← read=234567 = disk I/O!
  ->  Seq Scan on orders  (cost=0.00..123456.78 rows=1234567 width=32)
      (actual time=0.012..4567.890 rows=1234567 loops=1)
      Filter: (created_at >= '2024-01-01'::date)
      Rows Removed by Filter: 9876543   ← Scanning 11M rows, keeping 1.2M
                                         ← MISSING INDEX on created_at!
  ->  Hash  (cost=78901.23..78901.23 rows=1234567 width=24)
      (actual time=4567.890..4567.890 rows=1234567 loops=1)
       Buckets: 2097152  Batches: 4  Memory Usage: 65536kB  ← hash spilled to disk!
       (Batches > 1 = hash table too large for work_mem)
```

Two problems visible in the EXPLAIN output:

1. **Seq Scan on orders** with `Rows Removed by Filter: 9876543` → the query scanned 11M rows to find 1.2M that pass the date filter. This means no index on `created_at`. Adding an index reduces this from a full table scan to an index range scan.

2. **Hash Batches: 4** → the hash table for the join spilled to disk because `work_mem` is too small. Each spill is a write + read to disk. Fix: `SET work_mem = '512MB'` in the dbt model's `pre-hook`, or increase `work_mem` in Postgres config.

**Connection to CPU performance:** The Seq Scan generates millions of random memory accesses through the buffer pool (high LLC-load-misses). The hash spill generates disk I/O (low IPC during the spill, high `iowait` in `iostat`). Both are visible with `perf stat` attached to the Postgres backend process.

### Py-spy Live View of a Running Spark Worker

```bash
# On the Spark worker node, identify PySpark Python worker processes
ps aux | grep pyspark | grep -v grep
# output: 45678 ... /usr/bin/python3 /opt/spark/python/pyspark/worker.py

# Live view: which Python function is on-CPU right now?
sudo py-spy top --pid 45678

# Output example (refreshes every 2 seconds):
# Collecting samples from 'pid: 45678' (python v3.11.4 /usr/bin/python3)
# Total Samples 2000
# GIL: 78.00%, Active: 78.00%, Threads: 1
#
#   %Own   %Total  OwnTime  TotalTime  Function (filename:line)
#  45.00%  45.00%    9.00s     9.00s   transform_row (udf.py:34)
#  20.00%  65.00%    4.00s    13.00s   process_batch (udf.py:22)
#  15.00%   15.00%   3.00s     3.00s   json.loads (json/__init__.py:346)
#   8.00%   8.00%    1.60s     1.60s   datetime.strptime (datetime.py:1720)

# Insight from this output:
# - 45% of time in transform_row → the UDF itself
# - 15% in json.loads → deserializing JSON inside the UDF
# - 8% in datetime.strptime → parsing date strings
#
# Fix: pre-parse JSON at the Spark SQL level before passing to Python.
#      Use Spark's from_json() function (runs in JVM, no Python overhead).
#      Replace strptime with Spark's to_date() function.
```

### eBPF/bpftrace for Zero-Overhead Production Tracing

For cases where `perf` or `strace` overhead is too high for production:

```bash
# Count all syscalls made by a Kafka broker for 10 seconds
sudo bpftrace -e '
tracepoint:syscalls:sys_enter_* /comm == "kafka-server-sta"/ {
    @[probe] = count();
}
END {
    print(@, 10);
}
' --timeout 10

# Output:
# @[tracepoint:syscalls:sys_enter_epoll_wait]: 876543
# @[tracepoint:syscalls:sys_enter_sendfile]:   234567
# @[tracepoint:syscalls:sys_enter_read]:       189234
# @[tracepoint:syscalls:sys_enter_write]:       12345

# Insight:
# epoll_wait dominates → the broker is waiting for I/O events, not processing
# This is EXPECTED for an idle or lightly-loaded broker
# If sendfile were very low but read/write were high, that would indicate
# TLS is preventing zero-copy (all data going through user space)
```

eBPF runs in the kernel at near-zero overhead (~0.1–0.5% CPU per trace) and does not require the 10–100× overhead of `strace`.

---

## 13. Code

### Tool 1 — Automated `perf stat` Wrapper for Data Pipelines

```python
"""
perf_wrapper.py

Wraps a subprocess call with perf stat and parses the output into
a structured dict. Useful for benchmarking data pipeline components.

Usage:
    from perf_wrapper import perf_stat
    result = perf_stat(["python3", "my_pipeline.py", "--input", "data.parquet"])
    print(f"IPC: {result['ipc']:.2f}")
    print(f"Branch miss rate: {result['branch_miss_pct']:.2f}%")

Requirements: Linux, perf installed, sudo or kernel.perf_event_paranoid <= 1
"""
import subprocess
import re
import shutil
from dataclasses import dataclass
from typing import Optional

@dataclass
class PerfResult:
    ipc: float                  # Instructions per cycle
    instructions: int           # Total instructions retired
    cycles: int                 # Total clock cycles
    branch_miss_pct: float      # Branch miss rate (%)
    llc_miss_pct: float         # Last-level cache miss rate (%)
    context_switches: int       # OS context switches
    elapsed_seconds: float      # Wall-clock time
    raw_output: str             # Full perf stat output

    def classify_bottleneck(self) -> str:
        """Classify the primary bottleneck from perf counters."""
        if self.ipc > 2.0 and self.llc_miss_pct < 2.0 and self.branch_miss_pct < 1.0:
            return "compute-bound (well-optimized)"
        elif self.llc_miss_pct > 10.0:
            return f"memory-bound (LLC miss rate {self.llc_miss_pct:.1f}% → DRAM accesses)"
        elif self.branch_miss_pct > 5.0:
            return f"branch-prediction-bound ({self.branch_miss_pct:.1f}% miss rate)"
        elif self.context_switches > 10000:
            return f"syscall/IO-bound ({self.context_switches:,} context switches)"
        elif self.ipc < 0.5:
            return "stalled (low IPC without obvious cause — check lock contention)"
        else:
            return "moderate compute (IPC 0.5–2.0 — some optimization possible)"

def perf_stat(cmd: list[str], timeout: int = 300) -> Optional[PerfResult]:
    """Run a command under perf stat and return parsed metrics."""
    if not shutil.which("perf"):
        print("WARNING: perf not found. Install with: apt install linux-perf")
        return None

    perf_cmd = [
        "perf", "stat",
        "-e", "instructions,cycles,branch-misses,branches,LLC-load-misses,LLC-loads,context-switches",
        "--"
    ] + cmd

    try:
        result = subprocess.run(
            perf_cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        stderr = result.stderr  # perf stat writes to stderr
    except subprocess.TimeoutExpired:
        print(f"Process timed out after {timeout}s")
        return None
    except FileNotFoundError:
        print(f"Command not found: {cmd[0]}")
        return None

    # Parse perf stat output
    def extract(pattern: str, text: str, default: float = 0.0) -> float:
        m = re.search(pattern, text, re.MULTILINE)
        if m:
            # Remove commas from numbers like "45,234,567"
            return float(m.group(1).replace(",", ""))
        return default

    instructions = int(extract(r"^\s*([\d,]+)\s+instructions", stderr))
    cycles       = int(extract(r"^\s*([\d,]+)\s+cycles", stderr))
    branch_miss  = int(extract(r"^\s*([\d,]+)\s+branch-misses", stderr))
    branches     = int(extract(r"^\s*([\d,]+)\s+branches", stderr))
    llc_miss     = int(extract(r"^\s*([\d,]+)\s+LLC-load-misses", stderr))
    llc_loads    = int(extract(r"^\s*([\d,]+)\s+LLC-loads", stderr))
    ctx_switches = int(extract(r"^\s*([\d,]+)\s+context-switches", stderr))
    elapsed_m    = re.search(r"([\d.]+) seconds time elapsed", stderr)
    elapsed      = float(elapsed_m.group(1)) if elapsed_m else 0.0

    ipc = instructions / cycles if cycles > 0 else 0.0
    branch_miss_pct = (branch_miss / branches * 100) if branches > 0 else 0.0
    llc_miss_pct    = (llc_miss / llc_loads * 100)   if llc_loads > 0 else 0.0

    return PerfResult(
        ipc=ipc,
        instructions=instructions,
        cycles=cycles,
        branch_miss_pct=branch_miss_pct,
        llc_miss_pct=llc_miss_pct,
        context_switches=ctx_switches,
        elapsed_seconds=elapsed,
        raw_output=stderr
    )


if __name__ == "__main__":
    import sys

    cmd = sys.argv[1:] if len(sys.argv) > 1 else ["python3", "-c",
        "import numpy as np; a = np.random.rand(10_000_000); print(np.sum(a))"]

    print(f"Running: {' '.join(cmd)}")
    r = perf_stat(cmd)
    if r:
        print(f"\nPerf stat results:")
        print(f"  Elapsed:           {r.elapsed_seconds:.3f}s")
        print(f"  IPC:               {r.ipc:.2f}")
        print(f"  Instructions:      {r.instructions:,}")
        print(f"  Branch miss rate:  {r.branch_miss_pct:.2f}%")
        print(f"  LLC miss rate:     {r.llc_miss_pct:.2f}%")
        print(f"  Context switches:  {r.context_switches:,}")
        print(f"\n  Bottleneck classification: {r.classify_bottleneck()}")
```

### Tool 2 — Python Profiling Harness

```python
"""
profile_harness.py

Comprehensive profiling for any Python function: cProfile, memory,
and timing. Generates a summary report.

Usage:
    from profile_harness import profile
    
    @profile(output_dir="/tmp/profiles")
    def my_pipeline(df):
        return df.transform(...)
    
    result = my_pipeline(my_dataframe)
    # Writes cProfile output and memory stats to /tmp/profiles/
"""
import cProfile
import pstats
import io
import tracemalloc
import time
import functools
import os
from pathlib import Path

def profile(output_dir: str = "/tmp/profiles", top_n: int = 20):
    """Decorator that profiles a function with cProfile and tracemalloc."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            func_name = func.__name__

            # ─── Memory profiling ─────────────────────────────────────────
            tracemalloc.start()
            mem_before = tracemalloc.get_traced_memory()[0]

            # ─── CPU profiling ────────────────────────────────────────────
            profiler = cProfile.Profile()
            t0 = time.perf_counter()
            profiler.enable()
            result = func(*args, **kwargs)
            profiler.disable()
            elapsed = time.perf_counter() - t0

            # ─── Memory snapshot ──────────────────────────────────────────
            mem_after, mem_peak = tracemalloc.get_traced_memory()
            snapshot = tracemalloc.take_snapshot()
            tracemalloc.stop()

            # ─── Save and print cProfile stats ────────────────────────────
            stats_path = os.path.join(output_dir, f"{func_name}.prof")
            profiler.dump_stats(stats_path)

            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s).sort_stats("cumulative")
            ps.print_stats(top_n)
            stats_text = s.getvalue()

            # ─── Top memory allocations ───────────────────────────────────
            top_stats = snapshot.statistics("lineno")

            # ─── Print summary ────────────────────────────────────────────
            print(f"\n{'='*70}")
            print(f"PROFILE: {func_name}")
            print(f"{'='*70}")
            print(f"  Wall time:        {elapsed:.3f}s")
            print(f"  Memory allocated: {(mem_after - mem_before) / 1024**2:.1f} MB")
            print(f"  Peak memory:      {mem_peak / 1024**2:.1f} MB")
            print()
            print(f"TOP {top_n} FUNCTIONS BY CUMULATIVE TIME:")
            print(stats_text)
            print(f"TOP 10 MEMORY ALLOCATIONS:")
            for stat in top_stats[:10]:
                print(f"  {stat.size / 1024:.1f} KB in {stat.traceback.format()[0]}")
            print(f"\nFull profile saved to: {stats_path}")
            print(f"View with: python -m pstats {stats_path}")
            print(f"{'='*70}\n")

            return result
        return wrapper
    return decorator


# ─── Example usage ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import numpy as np
    import json

    @profile(output_dir="/tmp/profiles", top_n=15)
    def slow_pipeline(n: int):
        """Simulates a slow Python UDF-style pipeline."""
        # Bad: Python loop with per-row JSON parsing
        data = [{"id": i, "value": float(i % 100)} for i in range(n)]
        result = []
        for row in data:
            parsed = json.dumps(row)  # unnecessary serialization
            val = json.loads(parsed)["value"]
            if val > 50:
                result.append(val * 2.0)
            else:
                result.append(0.0)
        return result

    @profile(output_dir="/tmp/profiles", top_n=15)
    def fast_pipeline(n: int):
        """Vectorized equivalent of slow_pipeline."""
        values = np.arange(n, dtype=np.float64) % 100
        return np.where(values > 50, values * 2.0, 0.0).tolist()

    N = 500_000
    print("Running slow pipeline...")
    r1 = slow_pipeline(N)
    print("Running fast pipeline...")
    r2 = fast_pipeline(N)
```

### Tool 3 — Syscall Auditor

```python
"""
syscall_auditor.py

Runs a command under strace -c and parses the syscall summary.
Useful for identifying unexpected I/O patterns in data pipelines.

Usage: python syscall_auditor.py python3 my_pipeline.py
Requirements: Linux, strace installed
"""
import subprocess
import re
import sys
import shutil
from dataclasses import dataclass

@dataclass
class SyscallStat:
    name: str
    calls: int
    total_seconds: float
    avg_microseconds: float
    errors: int

def audit_syscalls(cmd: list[str], timeout: int = 120) -> list[SyscallStat]:
    """Run a command under strace -c and return per-syscall stats."""
    if not shutil.which("strace"):
        print("strace not found. Install: apt install strace")
        return []

    strace_cmd = ["strace", "-c", "-q"] + cmd

    result = subprocess.run(
        strace_cmd, capture_output=True, text=True, timeout=timeout
    )
    stderr = result.stderr

    stats = []
    # Parse strace -c output table
    # % time     seconds  usecs/call     calls    errors syscall
    pattern = re.compile(
        r"^\s*([\d.]+)\s+([\d.]+)\s+([\d]+)\s+([\d]+)\s*(\d+)?\s+(\w+)\s*$",
        re.MULTILINE
    )
    for m in pattern.finditer(stderr):
        _, total_s, avg_us, calls, errors, name = m.groups()
        stats.append(SyscallStat(
            name=name,
            calls=int(calls),
            total_seconds=float(total_s),
            avg_microseconds=float(avg_us),
            errors=int(errors) if errors else 0,
        ))

    return sorted(stats, key=lambda x: x.total_seconds, reverse=True)

def print_report(stats: list[SyscallStat], cmd: list[str]) -> None:
    if not stats:
        print("No syscall data collected.")
        return

    total_time = sum(s.total_seconds for s in stats)
    total_calls = sum(s.calls for s in stats)

    print(f"\nSyscall audit: {' '.join(cmd)}")
    print(f"{'='*65}")
    print(f"{'Syscall':<20} {'Calls':>10} {'Total (s)':>10} {'Avg (µs)':>10} {'Errors':>8}")
    print(f"{'-'*65}")
    for s in stats[:15]:
        flag = " ← HIGH" if s.avg_microseconds > 10000 else ""
        print(f"{s.name:<20} {s.calls:>10,} {s.total_seconds:>10.3f} "
              f"{s.avg_microseconds:>10.1f} {s.errors:>8}{flag}")
    print(f"{'-'*65}")
    print(f"{'TOTAL':<20} {total_calls:>10,} {total_time:>10.3f}")

    # Detect anti-patterns
    print(f"\nDiagnostics:")
    stats_by_name = {s.name: s for s in stats}

    if "stat" in stats_by_name and stats_by_name["stat"].avg_microseconds > 5000:
        print(f"  ⚠️  stat() avg {stats_by_name['stat'].avg_microseconds:.0f}µs → network filesystem? "
              f"({stats_by_name['stat'].calls} calls)")
    if "read" in stats_by_name and stats_by_name["read"].avg_microseconds > 1000:
        print(f"  ⚠️  read() avg {stats_by_name['read'].avg_microseconds:.0f}µs → "
              f"tiny reads or slow disk ({stats_by_name['read'].calls} calls)")
    if "write" in stats_by_name and stats_by_name["write"].avg_microseconds > 1000:
        print(f"  ⚠️  write() avg {stats_by_name['write'].avg_microseconds:.0f}µs → "
              f"synchronous writes or slow disk")
    if "futex" in stats_by_name and stats_by_name["futex"].calls > 100000:
        print(f"  ⚠️  {stats_by_name['futex'].calls:,} futex() calls → lock contention")

if __name__ == "__main__":
    cmd = sys.argv[1:] if len(sys.argv) > 1 else [
        "python3", "-c",
        "import pyarrow.parquet as pq; import pyarrow as pa; "
        "t = pa.table({'x': list(range(1000))}); "
        "pq.write_table(t, '/tmp/test.parquet'); "
        "pq.read_table('/tmp/test.parquet')"
    ]
    stats = audit_syscalls(cmd)
    print_report(stats, cmd)
```

---

## 14. Labs

### Lab 1 — Profile Three Versions of the Same Operation

**Goal:** Measure IPC, branch-miss rate, and elapsed time for three versions of the same task: Python loop, NumPy vectorized, and Pandas. Produce a comparison table.

```python
"""
lab1_perf_comparison.py

Profile three implementations of the same computation.
Run each under perf stat and compare.

Run: python lab1_perf_comparison.py
"""
import numpy as np
import pandas as pd
import time

N = 5_000_000

data = np.random.uniform(-100.0, 100.0, N).astype(np.float64)
data_list = data.tolist()
data_series = pd.Series(data)

def version_python(lst):
    """Python loop: max(0, x * 1.5) for each element."""
    result = []
    for x in lst:
        result.append(x * 1.5 if x > 0 else 0.0)
    return result

def version_numpy(arr):
    """NumPy branchless vectorized."""
    return np.where(arr > 0, arr * 1.5, 0.0)

def version_pandas(s):
    """Pandas vectorized."""
    return s.where(s <= 0, s * 1.5).where(s > 0, 0.0)

# Warmup
_ = version_numpy(data[:1000])
_ = version_pandas(data_series[:1000])

RUNS = 3
print(f"Array: {N:,} float64 values ({data.nbytes / 1024**2:.0f} MB)")
print(f"{'Version':<20} {'Avg time (s)':>14} {'Throughput (M rows/s)':>22}")
print(f"{'-'*60}")

for name, fn, arg in [
    ("Python loop",    version_python,  data_list),
    ("NumPy",          version_numpy,   data),
    ("Pandas",         version_pandas,  data_series),
]:
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        fn(arg)
        times.append(time.perf_counter() - t0)
    avg = sum(times) / RUNS
    throughput = N / avg / 1e6
    print(f"{name:<20} {avg:>14.3f}s {throughput:>22.1f} M rows/s")

print()
print("Now run each version under perf stat:")
print("  perf stat python3 -c \"import numpy as np; ...")
print("  Compare IPC, branch-miss%, LLC-miss% for each version.")
print()
print("Questions:")
print("  1. What IPC does the Python loop achieve? (expect 0.3-0.5)")
print("  2. What IPC does NumPy achieve? (expect 2.5-4.0)")
print("  3. What is the branch-miss rate for each? Why does it differ?")
print("  4. Is any version memory-bound? (check LLC-load-misses)")
```

---

### Lab 2 — Generate and Interpret a Flame Graph

**Goal:** Generate a CPU flame graph for a real Python data processing workload and identify the top 3 optimization targets.

```bash
#!/bin/bash
# lab2_flamegraph.sh
# Prerequisites: perf, perl, flamegraph scripts
# Install: apt install linux-perf
#          git clone https://github.com/brendangregg/FlameGraph /opt/flamegraph

set -e

# Step 1: Write a profile target
cat > /tmp/lab2_target.py << 'PYTHON'
import json
import time
import random

# Simulate a data pipeline with multiple bottlenecks:
# 1. JSON parsing (expensive)
# 2. Date string parsing (expensive)
# 3. Numeric computation (cheap but called often)

def parse_record(raw: str) -> dict:
    return json.loads(raw)  # bottleneck 1

def validate_date(date_str: str) -> bool:
    from datetime import datetime
    try:
        datetime.strptime(date_str, "%Y-%m-%d")  # bottleneck 2
        return True
    except ValueError:
        return False

def compute_score(value: float) -> float:
    # Simulates a complex scoring function
    import math
    return math.log1p(abs(value)) * (1 if value > 0 else -1)

# Generate synthetic records
records = [
    json.dumps({
        "id": i,
        "date": f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
        "value": random.uniform(-100, 100),
        "name": f"user_{i}"
    })
    for i in range(200_000)
]

print(f"Processing {len(records):,} records...")
t0 = time.perf_counter()
results = []
for raw in records:
    rec = parse_record(raw)
    if validate_date(rec["date"]):
        score = compute_score(rec["value"])
        results.append({"id": rec["id"], "score": score})
print(f"Done: {len(results):,} results in {time.perf_counter() - t0:.2f}s")
PYTHON

echo "=== Step 1: Run normally to get baseline ==="
time python3 /tmp/lab2_target.py

echo ""
echo "=== Step 2: Profile with perf record ==="
perf record -F 99 -g --call-graph dwarf -o /tmp/lab2.perf -- python3 /tmp/lab2_target.py

echo ""
echo "=== Step 3: Generate flame graph ==="
perf script -i /tmp/lab2.perf | \
    /opt/flamegraph/stackcollapse-perf.pl | \
    /opt/flamegraph/flamegraph.pl \
        --title "Lab 2: Python Data Pipeline" \
        --subtitle "Width = CPU time" \
    > /tmp/lab2_flamegraph.svg

echo "Flame graph: /tmp/lab2_flamegraph.svg"
echo ""
echo "Open in browser: firefox /tmp/lab2_flamegraph.svg"
echo ""
echo "Questions:"
echo "  1. What are the top 3 widest frames near the top of the flame graph?"
echo "  2. Which function should you optimize first? (Amdahl's Law: the widest one)"
echo "  3. How would you replace json.loads() with a Spark-native operation?"
echo "  4. How would you replace strptime() with a vectorized date parse?"
```

---

### Lab 3 — Identify the Bottleneck Class of Four Different Pipelines

**Goal:** Given four synthetic pipelines with different bottleneck types, use `perf stat` to classify each as compute-bound, memory-bound, branch-prediction-bound, or syscall-bound. Match each pipeline to its correct class before looking at the answers.

```python
"""
lab3_bottleneck_classification.py

Four pipelines. Each has a different bottleneck.
Run each with: perf stat python3 lab3_bottleneck_classification.py N
where N = 1, 2, 3, or 4.

After running all four, classify each as:
  A) Compute-bound (IPC > 2.5, low misses)
  B) Memory-bound  (IPC < 0.5, high LLC misses)
  C) Branch-bound  (IPC ~1.0, high branch-miss%)
  D) Syscall-bound (IPC < 1.0, high context-switches)
"""
import sys
import numpy as np
import os
import random

def pipeline_1():
    """Hint: SIMD vectorized numeric operations on large arrays."""
    arr = np.random.rand(50_000_000).astype(np.float32)
    for _ in range(5):
        arr = arr * 1.0001 + 0.0001
    print(f"Pipeline 1 result: {arr.sum():.6f}")

def pipeline_2():
    """Hint: Random access into a very large array (exceeds L3 cache)."""
    SIZE = 100_000_000
    arr = np.zeros(SIZE, dtype=np.int32)
    indices = np.random.randint(0, SIZE, 5_000_000, dtype=np.int64)
    # Random reads from an 400 MB array (won't fit in L3)
    total = 0
    for idx in indices:
        total += arr[idx]
    print(f"Pipeline 2 result: {total}")

def pipeline_3():
    """Hint: Data-dependent comparisons on uniformly random data."""
    data = [random.randint(0, 255) for _ in range(10_000_000)]
    total = 0
    for x in data:
        if x < 128:       # branch 1: ~50% taken (unpredictable)
            if x < 64:    # branch 2: conditional on above
                total += x * 3
            else:
                total += x * 2
        else:
            if x > 192:   # branch 3: ~25% taken
                total -= x
            else:
                total += x
    print(f"Pipeline 3 result: {total}")

def pipeline_4():
    """Hint: Many small file I/O operations."""
    import tempfile
    tmpdir = tempfile.mkdtemp()
    # Write 1000 small files
    for i in range(1000):
        path = os.path.join(tmpdir, f"file_{i:04d}.txt")
        with open(path, "w") as f:
            f.write(f"record {i}\n" * 10)
    # Read them all back one by one
    total_bytes = 0
    for i in range(1000):
        path = os.path.join(tmpdir, f"file_{i:04d}.txt")
        with open(path) as f:
            total_bytes += len(f.read())
    import shutil
    shutil.rmtree(tmpdir)
    print(f"Pipeline 4 result: {total_bytes} bytes read")

pipelines = {
    "1": pipeline_1,
    "2": pipeline_2,
    "3": pipeline_3,
    "4": pipeline_4,
}

if len(sys.argv) < 2 or sys.argv[1] not in pipelines:
    print("Usage: perf stat python3 lab3_bottleneck_classification.py <1|2|3|4>")
    print("Run each pipeline and classify its bottleneck from perf stat output.")
    print()
    print("Pipelines: 1, 2, 3, 4")
    print("Classes: A=compute-bound, B=memory-bound, C=branch-bound, D=syscall-bound")
    sys.exit(0)

pipelines[sys.argv[1]]()

# Answers (run all four before reading):
# Pipeline 1 → A (compute-bound): SIMD vectorized, IPC ~3-4, low misses
# Pipeline 2 → B (memory-bound):  random DRAM access, IPC < 0.3, high LLC misses
# Pipeline 3 → C (branch-bound):  unpredictable comparisons, IPC ~1.0, miss% >15%
# Pipeline 4 → D (syscall-bound): 2000 file open/read/close, high context-switches
```

---

## 15. Summary

Performance analysis is a discipline, not a guess. The correct sequence is:

1. **Measure first.** Run `perf stat` before making any changes. Categorize the bottleneck class (compute, memory, branch, syscall) from IPC and miss rates.
2. **Find the hot path.** Generate a flame graph with `perf record` + `flamegraph.pl` or `py-spy record`. Identify the widest frame at the top of the stack.
3. **Apply Amdahl's Law.** Optimize the bottleneck that represents the largest fraction of runtime. A 10× improvement on a 5% bottleneck = 4.7% total improvement.
4. **Verify the fix.** Re-measure after the change. Confirm IPC changed in the expected direction.

**Key tools:**

| Tool | What it measures | Overhead | When to use |
|---|---|---|---|
| `perf stat` | Aggregate CPU efficiency | < 1% | First — always |
| `perf record` + flamegraph | Hot code path | ~5% | After `perf stat` identifies CPU-bound |
| `py-spy top` | Python call stack (live) | ~1% | Python UDF bottlenecks |
| `py-spy record` | Python flame graph | ~1% | Persistent Python profiling |
| `cProfile` | Python function timing | ~15% | Development profiling |
| `tracemalloc` | Python memory allocation | ~10% | Memory leak diagnosis |
| `strace -c` | Syscall count and timing | ~2× | Unexpected I/O patterns |
| `bpftrace` | Any kernel event | ~0.5% | Production tracing |

**IPC interpretation:**

| IPC | Interpretation |
|---|---|
| > 3.0 | Excellent — SIMD vectorized, compute-bound |
| 2.0–3.0 | Good — mixed compute, well-utilized |
| 1.0–2.0 | Moderate — some stalls, possible branch or cache issues |
| 0.5–1.0 | Poor — likely branch or memory pressure |
| < 0.5 | Bad — stalled on memory or syscalls |

---

## 16. Interview Q&A

### Q1: A Spark stage takes 45 minutes. Walk me through exactly how you would diagnose the bottleneck, starting from zero information.

**Answer:**

I start with the Spark UI before touching any Linux profiling tools, because the Spark UI is instrumented specifically for distributed jobs.

First, I look at the stage timeline. Which stage is slow — is it a shuffle boundary, an aggregation, or a task that runs much longer than others? Long tails usually indicate data skew: one partition is 100× larger than the others. The task time distribution (the input size histogram) will show this.

If tasks are uniformly slow, not a skew problem. I check the task metrics: input bytes vs shuffle read bytes vs output bytes. A stage reading 10 GB from shuffle but producing 100 MB output that takes 40 minutes is doing a lot of computation, not just moving data.

Next I check GC time. If GC time is > 10% of task time, the JVM is fighting for memory. Fix: increase executor memory, switch to Kryo serializer, or replace Python UDFs with Pandas UDFs (fewer Java objects).

If none of this explains it, I SSH to an executor node and run `perf stat -p EXECUTOR_PID --timeout 30`. The IPC tells me the bottleneck class immediately:
- IPC > 2.0, no other issues: compute-bound. I look for algorithmic improvements or unnecessary computation.
- IPC < 0.5, high LLC-load-misses: memory-bound. I look at data access patterns — is there a join causing random memory access into a large hash table?
- IPC < 0.5, high context-switches: syscall-bound. Probably a Python UDF making per-row I/O calls.
- IPC ~1.0, high branch-miss: branch-prediction-bound. A Python UDF with data-dependent comparisons.

If IPC says CPU-bound, I generate a flame graph with `perf record -F 99 -p PID --call-graph dwarf -- sleep 30` and look for the widest frame. If it shows Python serialization overhead (EvaluatePython.makeFromJava), the fix is to switch to Pandas UDFs. If it shows shuffle read overhead, the fix is to increase parallelism and reduce per-task shuffle size.

I document the baseline numbers before making any change. After the fix, I re-measure and confirm the IPC moved in the expected direction. If it didn't, the fix didn't address the real bottleneck.

---

### Q2: What is IPC and what does a low IPC tell you about a data pipeline?

**Answer:**

IPC — Instructions Per Cycle — is the ratio of instructions retired to clock cycles elapsed. It measures how efficiently the CPU is using its execution units. A modern 4-wide superscalar CPU can theoretically dispatch 4 instructions per cycle. In practice, well-optimized SIMD code achieves 3–4 IPC; mixed workloads achieve 1–2 IPC; and severely bottlenecked workloads can fall below 0.5 IPC.

Low IPC means the CPU has work to do but is blocked waiting for something. The CPU has instructions ready but cannot execute them because a required resource is unavailable. There are three main causes.

Memory stalls: a load instruction is waiting 200+ cycles for DRAM to respond to a cache miss. The ROB fills up with dependent instructions, the frontend stalls, and new instructions stop being fetched. IPC drops to 0.1–0.3. In data engineering, this happens when a hash join or shuffle accesses data in random order from a table larger than L3 cache. Fix: improve data locality — sort before joining, use broadcast joins for small tables, increase Spark's partition count to reduce per-partition size.

Branch mispredictions: the CPU speculatively executed the wrong path and must flush 15–20 cycles of work. Repeated at high frequency, this drops IPC to 0.5–1.0 with a characteristic high branch-miss-rate in `perf stat`. In data engineering, this appears in Python UDFs with data-dependent comparisons. Fix: switch to branchless NumPy operations or Pandas UDFs.

Syscall overhead: frequent context switches between user space and kernel space flush the TLB and pipeline, costing 100–300 ns per transition. IPC drops with a characteristic high context-switch count. In data engineering, this appears when code reads one row at a time from a file or makes per-row network calls. Fix: batch reads, use bulk APIs, increase buffer sizes.

---

### Q3: Explain a flame graph. How do you read one, and what does the width of a frame mean?

**Answer:**

A flame graph is a visualization of a sampling profiler's output. The profiler interrupts the program at fixed intervals (typically 99 times per second) and records the complete call stack at that moment — which function called which function down to the currently executing instruction. After collecting thousands of samples, the data is sorted and rendered.

The width of a frame is proportional to the fraction of total samples where that function appeared anywhere in the call stack. A frame that is 30% of the total width appeared in 30% of all samples — meaning the program was spending 30% of its wall-clock time in that function or in functions called by it.

The Y-axis is call stack depth, not time. The bottom frame is the root (usually `main` or the thread entry point). Higher frames are deeper in the call chain. A frame at the very top (with nothing above it) is a "leaf frame" — a function that was actually executing, not waiting for a callee to return.

The optimization target is the widest frame at the top of the stack — the leaf frame that appears in the most samples. That is where the CPU was actually spending cycles.

Common misreadings: a very wide frame at the bottom of the stack (like `main` or `SparkContext.runJob`) does not mean that function is slow — it means everything passes through it. A very tall, narrow spike means a deep call chain was observed in very few samples — probably not the bottleneck. The bottleneck is wide AND near the top.

For Python flame graphs, I use `py-spy record` rather than `perf record`, because py-spy reads Python frame objects from the interpreter's internal stack directly. `perf record` only sees native (C) frames and will show CPython's eval loop as an opaque block, which is not actionable.

---

### Q4: What does `strace -c` tell you, and give an example of how you've used it (or would use it) to diagnose a production problem.

**Answer:**

`strace -c` runs a process and records every system call, then prints a summary table at exit: for each syscall, it shows the total count, total time spent, average time per call, and error count.

The summary is diagnostic on two axes. Count tells you what kind of work the process is doing — a process with millions of `futex()` calls is contending on locks; millions of `read()` calls might be doing tiny reads. Time-per-call tells you whether each call is fast (microseconds, as expected) or slow (milliseconds, indicating network or disk latency).

A concrete example: I had a Python pipeline that read Parquet files from an NFS-mounted share. The job took 20 minutes for 50 GB of data. `iostat` showed the local disks were idle; `perf stat` showed low IPC with few cache misses. I ran `strace -c -e trace=stat,openat,read python3 pipeline.py` and saw 5,000 `stat()` calls averaging 8,000 µs each. 5,000 × 8 ms = 40 seconds just in `stat()` calls — but the job took 20 minutes, so there had to be more. I widened the trace to all syscalls and saw that `openat()` was averaging 12,000 µs — each file open was making a network round-trip to the NFS server. The code was opening files one at a time in a `for` loop.

The fix was to switch from a Python `for` loop opening each file to `pyarrow.dataset.dataset(directory)`, which uses a single batched directory listing (`getdents64`) followed by parallel file opens. The job went from 20 minutes to 4 minutes for the I/O-bound portion, and then I could see the actual compute bottleneck more clearly.

---

### Q5: How would you profile a PySpark Python UDF to determine if it's the bottleneck and what specifically is slow inside it?

**Answer:**

Profiling a PySpark Python UDF requires working at two levels: confirming it's the bottleneck at the Spark level, then drilling into the Python code.

At the Spark level, I add a timing instrumentation around the UDF stage. The Spark UI's task metrics show "executor CPU time" vs "elapsed time." If CPU time is high and GC time is low but the stage is slow, the UDF is using CPU efficiently but doing too much work — it might be correct but algorithmically expensive. If CPU time is low but elapsed time is high, the UDF is waiting — possibly for I/O or for the GIL.

To get inside the Python worker, I use `py-spy`. While the Spark job is running, I find the Python worker processes: `ps aux | grep pyspark/worker.py`. Then I run `sudo py-spy top --pid PID` for a live view, refreshed every second. This shows which Python function is on-CPU right now, with what percentage of time. If I see `json.loads` at 30% of time, the UDF is parsing JSON unnecessarily — that data should be pre-parsed at the Spark SQL layer. If I see `datetime.strptime` at 25%, the UDF is parsing date strings — replace with Spark's `to_date()` or pass pre-parsed integers.

For a more detailed analysis, I use `py-spy record -o udf_flame.svg --pid PID --duration 30` to generate a flame graph. This captures 30 seconds of the Python call stack at 100 Hz and renders it as an SVG.

After identifying the specific functions that are slow, the typical fixes are: replace per-row Python logic with vectorized Pandas/NumPy operations, switch from a Python UDF to a Pandas UDF (Arrow batch processing), pre-compute expensive fields (JSON parsing, date parsing) in Spark SQL before reaching the UDF, or eliminate the UDF entirely with Spark SQL functions.

To confirm the fix worked, I run the before and after versions on the same dataset and compare both the Spark UI task time and the `py-spy top` output.

---

### Q6: What is Brendan Gregg's USE method and how do you apply it to a Kafka broker that is "slow"?

**Answer:**

The USE method is a systematic checklist for resource analysis invented by Brendan Gregg. For every resource in the system, you check three things: Utilization (how busy is this resource on average), Saturation (is there more work queued than the resource can process, leading to queuing delay), and Errors (are there failures or anomalies). The method is systematic rather than intuitive — it prevents you from guessing at the bottleneck and forces you to check all resources.

For a Kafka broker showing "slow" (consumer lag growing, low produce/consume throughput):

**CPU** — Utilization: `mpstat -P ALL 1` → is any core at 100%? On a Kafka broker, high CPU usually means TLS encryption, log compaction, or consumer group rebalancing. Saturation: `vmstat 1` → is the run queue length consistently above the CPU count? Errors: `perf stat -e page-faults` for unusual CPU error events.

**Memory** — Utilization: `free -h` → how much of RAM is used by the page cache (the `buff/cache` line)? Kafka performance depends entirely on the OS page cache holding hot log segments. If free memory is high and page cache is small, read throughput will be bottlenecked by disk I/O. Saturation: `vmstat 1` → is there swap activity (`si`/`so` columns)? Any swap on a Kafka broker is a severe performance problem. Errors: `dmesg | grep OOM` for memory killer events.

**Disk** — Utilization: `iostat -x 1` → what is `%util` for the disk holding the log directory? A Kafka broker writing at 500 MB/s to a disk with 600 MB/s bandwidth is at 83% utilization — near saturation. Saturation: `iostat` `await` column — are disk operations waiting > 5ms on average? Errors: `dmesg | grep "I/O error"`.

**Network** — Utilization: `sar -n DEV 1` → what fraction of network bandwidth is in use? A 10 Gbps NIC carrying 9 Gbps of Kafka traffic has no headroom. Saturation: `ss -s` → rising retransmit count indicates TCP congestion. Errors: `ethtool -S eth0 | grep error`.

After running these checks, the bottleneck is usually obvious: disk throughput saturated for write-heavy topics, network saturated for high-replication topics, CPU saturated for TLS-heavy deployments, or memory saturated (swapping) when the page cache is under pressure from many concurrent consumers.

---

## 17. Cross-Question Chain

**Topic:** From "how do you know a job is slow?" to "how do you change the code?"

---

**Interviewer:** You deploy a new dbt model and it runs for 3 hours. How do you start investigating?

**Candidate:** First, I check the dbt run logs and Postgres's `pg_stat_statements` for the query text and execution time. dbt surfaces the slow model name; `pg_stat_statements` gives me the exact SQL and how long Postgres spent on it. If it's a single query taking 3 hours, the problem is almost certainly in the query plan, not in dbt or the machine. I run `EXPLAIN (ANALYZE, BUFFERS) <the query>` and read the output.

---

**Interviewer:** What are you looking for in the EXPLAIN output?

**Candidate:** Two things: plan correctness and resource utilization. For correctness, I check whether the planner chose appropriate join algorithms and indexes. If I see a sequential scan on a large table with a low-selectivity filter condition (like `created_at > '2024-01-01'` on a table with 100M rows), there's a missing index. The output shows `Rows Removed by Filter`, which quantifies the waste. For resource utilization, I look at the `Buffers: shared read` lines — high read counts mean the buffer pool is going to disk. For joins, `Batches > 1` in a hash join means the hash table spilled to disk because `work_mem` is too small.

---

**Interviewer:** Suppose the EXPLAIN looks fine — good indexes, no hash spills. But the query still takes 2 hours. Now what?

**Candidate:** I attach `perf stat` to the Postgres backend process handling the query. I find the PID with `SELECT pid FROM pg_stat_activity WHERE query LIKE '%my_table%'`, then run `sudo perf stat -p PID --timeout 60`. The IPC tells me the resource class. If IPC is below 0.5 with high LLC-load-misses, Postgres is doing computation but most of it is stalled waiting for DRAM — classic for a large hash join where the hash table doesn't fit in L3 cache. The join algorithm is correct but the data is too large for cache-efficient execution.

---

**Interviewer:** What would you do with that diagnosis?

**Candidate:** Two paths, depending on the data volume. If the smaller join input is small enough for `work_mem`, I increase `work_mem` for the session: `SET work_mem = '4GB'` before the query. This lets Postgres keep the hash table in RAM instead of spilling to disk, and more importantly gives the CPU cache more chance to reuse hot hash buckets. If the data is fundamentally too large for in-memory hashing, I consider whether a sort-merge join would be better. A sort-merge join accesses data sequentially from both sides after sorting — sequential access is cache-friendly, and the hardware prefetcher can stay ahead of the reads. In Postgres, I can hint toward sort-merge by reducing `enable_hashjoin = off` temporarily to see if the sort-merge plan is faster.

---

**Interviewer:** How would this look in a Spark context for the same type of join?

**Candidate:** In Spark, the equivalent diagnosis starts with the Spark UI. I look at the stage that contains the join — specifically the task time distribution and the shuffle read bytes. If shuffle read bytes are large and tasks are slow, I check whether Spark chose a sort-merge join or a hash join. For large tables, sort-merge is usually correct. If Spark chose a hash join (which I can see in `df.explain(mode="extended")`), the hash table may be too large for the executor's memory, causing spill to disk. I verify with `spark.sql.autoBroadcastJoinThreshold` settings and the Spark UI's Storage tab for spill metrics.

If the join algorithm is correct but performance is still poor, I use `perf stat` on an executor process during the join stage. High LLC-load-misses on a sort-merge join suggest the data within each sort partition is not fitting in cache — I can increase the number of partitions (more, smaller partitions) to reduce per-partition data volume and improve cache utilization. The connection back to the hardware: smaller partitions mean each partition's data fits in L3, sort-merge accesses within partition are sequential, and the hardware prefetcher can load the next cache lines before they're needed.

---

**Interviewer:** What measurement confirms the optimization actually worked?

**Candidate:** Three measurements, in order. First, the Spark UI stage duration — the simple wall-clock test. Second, `perf stat` IPC on the executor during the optimized stage. If the fix worked, IPC should be higher (more instructions completed per cycle, because fewer cycles are stalled on DRAM). Third, LLC-load-misses: if data fits better in cache after the partition count increase, the LLC miss rate should drop. I record the baseline numbers before the change (IPC = 0.28, LLC miss rate = 34%, stage time = 45 min) and compare after (IPC = 1.4, LLC miss rate = 8%, stage time = 12 min). If the numbers don't change in the expected direction, the hypothesis was wrong and I go back to the measurement step.

---

## 18. What's Next

**M05: From Python to Electrons** — The capstone of CSF-ARC-101. Trace one Python statement (`total = sum(arr)`) from CPython bytecode through the C runtime, through the System V calling convention, through the x86-64 ISA, through the microarchitecture's pipeline and SIMD units, to the physical memory hierarchy. Uses every tool from M04 to verify each step.

**CSF-ARC-102: Memory Architecture** — The other half. M01–M04 covered the CPU side; CSF-ARC-102 covers the memory side. Cache lines (64 bytes), NUMA topology, TLB, the hardware prefetcher, false sharing, and why NumPy strides matter. Directly explains why Parquet's columnar layout, Spark's executor memory configuration, and cache-conscious data structures are designed the way they are.

**CSF-ALG-101: Algorithms and Data Structures for Data Engineers** — Now that you can measure CPU performance, you have the tools to verify algorithmic complexity claims empirically. Sort 10M integers and measure IPC at each algorithm step. Hash join 100M rows and verify the cache-miss hypothesis with `perf stat`.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What does IPC measure? | Instructions Per Cycle: how efficiently the CPU is using its execution units. Healthy compute: > 2.0. Memory-stalled: < 0.5. |
| 2 | What does a branch-miss rate > 5% indicate in `perf stat`? | Branch-prediction-bound workload. The CPU is flushing the pipeline ~20 cycles per miss. Fix: use branchless operations (NumPy np.where, sorting data). |
| 3 | What does a high LLC-load-miss rate indicate? | Memory-bound: L3 cache misses require ~200ns DRAM accesses, stalling the pipeline. Fix: improve data locality, use sorted access patterns. |
| 4 | What is the conventional `perf record` sampling frequency? | 99 Hz (avoids lockstep with 100 Hz system timer, keeps overhead < 2%). |
| 5 | What does the width of a flame graph frame mean? | The fraction of total samples where that frame appeared in the call stack — proportional to CPU time spent in that function (including callees). |
| 6 | What is the optimization target in a flame graph? | The widest frame near the TOP of the stack (leaf frames) — the function that was actually executing (not waiting for a callee) in the most samples. |
| 7 | What tool gives a live Python call stack view with ~1% overhead? | `py-spy top --pid PID` (sampling profiler, reads Python interpreter internals directly). |
| 8 | What is `cProfile`'s main limitation vs `py-spy`? | cProfile is instrumentation-based (~15% overhead, changes timing), only sees Python frames (not C extension internals), and requires code modification or `-m cProfile` flag. |
| 9 | What does `strace -c` output? | A summary table: per syscall, the count, total time, average microseconds per call, and error count. Low overhead (~2×) compared to full `strace`. |
| 10 | What does a high `avg_microseconds` in `strace -c` for `stat()` indicate? | Network filesystem: each `stat()` requires a network round-trip. Fix: batch metadata operations, use dataset APIs that list directories rather than stat individual files. |
| 11 | What is the USE method? | Brendan Gregg's resource analysis checklist: Utilization (how busy), Saturation (queuing beyond capacity), Errors (failures). Applied to every resource: CPU, memory, disk, network. |
| 12 | What is Amdahl's Law? | Speedup = 1 / ((1-P) + P/S). Optimizing a component that is 10% of runtime, even perfectly (S=∞), gives only 10% total speedup. Always optimize the largest fraction first. |
| 13 | What `perf stat` counter indicates a syscall-heavy workload? | High `context-switches` count. Each context switch costs 100–300ns and pollutes TLB and cache. |
| 14 | How do you generate a CPU flame graph from `perf record` output? | `perf record -F 99 -g --call-graph dwarf -o perf.data -- sleep 30` then `perf script | stackcollapse-perf.pl | flamegraph.pl > out.svg`. |
| 15 | What tool provides low-overhead kernel tracing without the 10× `strace` overhead? | eBPF / `bpftrace`. Runs in the kernel, ~0.1–0.5% overhead, programmable tracing of any kernel event. |
| 16 | What does `tracemalloc` measure? | Python memory allocations: shows which call sites allocated the most bytes. Used for diagnosing memory leaks and excessive memory usage in Python pipelines. |
| 17 | What does the Spark UI `GC Time` metric tell you? | Fraction of executor time spent in JVM garbage collection. Healthy: < 5%. Problem: > 15% (increase executor memory, use Pandas UDFs, switch to Kryo). |
| 18 | Why does a Kafka broker's TLS overhead show as high IPC in `perf stat`? | TLS encryption (AES-GCM) is computationally intensive — the AES instruction throughput is high IPC. Fix: enable AES-NI hardware acceleration, verify with `grep aes /proc/cpuinfo`. |
| 19 | What is the `perf_event_paranoid` kernel parameter? | Controls who can use perf counters. `-1` = everyone (development), `0` = users can use CPU counters, `1` = restricted, `2` = only root. Default is usually 2 in cloud VMs. |
| 20 | How do you profile a Python function in production without code changes? | `py-spy record -o flame.svg --pid PID --duration 30` — attaches to a running process by PID, no code changes, no restart required, ~1% overhead. |

---

## 20. References

**Linux `perf` and Profiling**

- Gregg, B. (2020). *Systems Performance: Enterprise and the Cloud* (2nd ed.). Pearson. **Chapter 6** (CPU performance), **Chapter 13** (perf). https://www.brendangregg.com/systems-performance-2nd-edition-book.html — The canonical reference for Linux performance analysis.
- Gregg, B. (2016). The Flame Graph. *ACM Queue*, 14(2). https://dl.acm.org/doi/10.1145/2927299.2927301 — The original flame graph paper with interpretation guide.
- FlameGraph scripts: https://github.com/brendangregg/FlameGraph — Brendan Gregg's official flamegraph.pl and stackcollapse-perf.pl scripts.
- Linux `perf` documentation: https://perf.wiki.kernel.org/index.php/Tutorial — Official tutorial.
- Gregg, B. (2019). BPF Performance Tools. Addison-Wesley. — eBPF and bpftrace for production tracing.

**Python Profiling**

- py-spy: https://github.com/benfred/py-spy — Sampling profiler for Python processes (no instrumentation required).
- Python `cProfile` documentation: https://docs.python.org/3/library/profile.html
- Python `tracemalloc` documentation: https://docs.python.org/3/library/tracemalloc.html
- memory_profiler: https://github.com/pythonprofilers/memory_profiler — Line-by-line memory usage.

**Data Engineering Connections**

- Zaharia, M., et al. (2016). Apache Spark: A Unified Engine for Big Data Processing. *Communications of the ACM*, 59(11). — Tungsten code generation context.
- Dremel paper (Section 3): Melnik et al. (2010). https://research.google/pubs/pub36632/ — SIMD and columnar decode performance.
- Postgres EXPLAIN documentation: https://www.postgresql.org/docs/current/sql-explain.html — Complete EXPLAIN ANALYZE output reference.
- Spark UI documentation: https://spark.apache.org/docs/latest/web-ui.html — Interpreting stage metrics.

**Amdahl's Law and Performance Methodology**

- Amdahl, G. M. (1967). Validity of the single processor approach to achieving large-scale computing capabilities. *Proceedings of the April 18-20 Spring Joint Computer Conference*. The original paper.
- Gregg, B. (2013). "USE Method: Linux Performance Checklist." https://www.brendangregg.com/USEmethod/use-linux.html — The definitive USE method reference for Linux.
