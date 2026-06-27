# CSF-OS-101 M02: Process Scheduling

**Course:** CSF-OS-101 — Operating System Internals for Data Engineers  
**Module:** 02 of 05  
**Filesystem position:** 00_CS_Foundations/M18_Process_Scheduling  
**Prerequisites:** CSF-OS-101 M01 (Processes and Threads)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: Scheduling Goals and Metrics](#4-core-theory-scheduling-goals-and-metrics)
5. [Classic Scheduling Algorithms](#5-classic-scheduling-algorithms)
6. [Linux CFS: The Completely Fair Scheduler](#6-linux-cfs-the-completely-fair-scheduler)
7. [Priority, Nice Values, and cgroups](#7-priority-nice-values-and-cgroups)
8. [CPU Affinity and NUMA](#8-cpu-affinity-and-numa)
9. [Scheduler Impact on Data Engineering Workloads](#9-scheduler-impact-on-data-engineering-workloads)
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

A Spark job has 200 tasks. Each executor has 4 cores. The Spark UI shows tasks consistently taking 2× longer than expected. CPU utilization is at 100%, but the job is not finishing any faster with more cores. You add more executors — still no improvement. The problem is not Spark — it is the OS scheduler making poor decisions about which tasks run when.

A Kafka consumer process and a heavy ETL batch job run on the same machine. The consumer's processing latency spikes from 5 ms to 200 ms when the batch job is active, causing consumer lag to accumulate and triggering alerts. The fix is not hardware — it is giving the Kafka consumer higher scheduling priority so it is never preempted during the batch job's CPU bursts.

A dbt transformation runs on a machine with 64 logical CPUs, but each Python subprocess only uses one CPU at a time. You want to pin each subprocess to a specific set of CPU cores so the OS never migrates them (preventing cache eviction), and so NUMA-local memory is always used. This is CPU affinity management.

This module explains how the Linux kernel decides which process gets CPU time, how you influence that decision via nice values and cgroups, and how scheduling decisions directly affect Spark task throughput, Kafka consumer latency, and the behavior of any data pipeline running on shared infrastructure.

---

## 2. How to Use This Module

**For production tuning:** Sections 6, 7, 8, 9. These sections explain CFS mechanics, how to use `nice`, `renice`, `taskset`, `cpuset`, and cgroups to control scheduling behavior, and how to map those controls to Spark, Airflow, and Kafka configurations.

**For interview prep:** Sections 5, 6, 15, 16. Classic scheduling algorithms (FCFS, SJF, Round Robin, Priority) are standard OS interview questions. CFS is the expected follow-up for senior/staff roles. Sections 15 and 16 escalate from "explain Round Robin" to "design a multi-tenant Spark cluster scheduling policy."

**For debugging:** Sections 9, 11. Section 9 maps scheduling decisions to observable symptoms (task latency spikes, CPU steal time, NUMA penalties). Section 11 covers real production failures: CPU throttling in Kubernetes, noisy-neighbor interference, and priority inversion in Spark shuffle.

---

## 3. Prerequisites Check

- **Processes and threads (M01):** You must understand that the OS schedules threads, not processes. Every runnable thread competes for CPU time. A Spark executor with 4 task threads has 4 competing scheduling entities.
- **Priority queues (CSF-ALG-101):** CFS uses a red-black tree (a self-balancing BST) as its run queue. Knowing that a red-black tree supports O(log N) insert/delete and O(1) min-find is required context for why CFS scales to thousands of threads.
- **Big-O (CSF-ALG-101 M01):** Scheduling algorithm complexity is analyzed in terms of N runnable threads. Round Robin is O(1) per scheduling decision; CFS is O(log N).

---

## 4. Core Theory: Scheduling Goals and Metrics

### 4.1 What the Scheduler Must Do

The OS scheduler decides: given N runnable threads and K CPU cores (where N >> K), which K threads run right now?

The scheduler makes this decision thousands of times per second per core. Its goals are contradictory — optimizing one degrades others:

| Goal | Definition | Who wants this |
|---|---|---|
| **Throughput** | Maximize total work completed per unit time | Batch ETL jobs, Spark tasks |
| **Latency (response time)** | Minimize time from task arrival to completion | Kafka consumers, interactive queries |
| **Fairness** | Each thread gets proportional CPU share | Multi-tenant shared clusters |
| **CPU utilization** | Maximize fraction of time CPUs are busy | Infrastructure teams (cost efficiency) |
| **Turnaround time** | Minimize time from job submission to completion | Data engineers waiting for dbt runs |
| **Priority** | High-priority work preempts low-priority | Mixed-criticality workloads |

No scheduler satisfies all goals simultaneously. The Linux CFS scheduler prioritizes fairness and throughput. Real-time schedulers (SCHED_FIFO, SCHED_RR) prioritize latency and priority.

### 4.2 Key Metrics

**CPU utilization:** Fraction of time the CPU is not idle. Target ≥ 80% for batch workloads; 60–70% is acceptable to leave headroom for latency-sensitive processes.

**Turnaround time:** Time from task submission to task completion. Turnaround = Burst time + Wait time. For a Spark task: time from the DAGScheduler submitting the task to the executor returning the result.

**Waiting time:** Time a runnable thread spends in the run queue waiting for a CPU. The scheduler's main knob — reducing wait time improves latency. For N threads and K cores, average wait time ≈ (N/K − 1) × time_slice.

**CPU steal time (`st` in `top`):** On virtualized machines (AWS EC2, GCP VM), steal time measures the fraction of time the virtual machine wanted to run but the hypervisor gave the CPU to another VM. High steal time (> 5%) means the physical host is oversubscribed — adding more VMs won't help, you need larger instances or a less congested host.

**Context switches:** `vmstat 1` column `cs` shows context switches per second. A value above 100,000/s often indicates excessive thread switching overhead.

---

## 5. Classic Scheduling Algorithms

Understanding the history of scheduling algorithms is essential because each algorithm's trade-offs illuminate why CFS was designed the way it was.

### 5.1 First-Come, First-Served (FCFS)

Threads are served in arrival order. The first thread to enter the run queue runs to completion before the next thread starts.

```
Example: 3 threads arrive simultaneously
  Thread A: burst time = 24 ms
  Thread B: burst time = 3 ms
  Thread C: burst time = 3 ms

FCFS order: A → B → C
  A: wait=0,  turnaround=24
  B: wait=24, turnaround=27
  C: wait=27, turnaround=30
  Average turnaround: (24 + 27 + 30) / 3 = 27 ms
  Average wait:       (0 + 24 + 27) / 3 = 17 ms
```

**Problem: the convoy effect.** A long-running thread (A = 24 ms) blocks all shorter threads (B, C = 3 ms each). B and C finish 24 ms later than they would have if scheduled first. This is catastrophic for interactive or latency-sensitive workloads. A Kafka consumer thread blocked behind a 10-second ETL computation would accumulate 10 seconds of consumer lag.

### 5.2 Shortest Job First (SJF)

Schedule the thread with the shortest estimated burst time first.

```
Same 3 threads, optimal SJF order: B → C → A
  B: wait=0,  turnaround=3
  C: wait=3,  turnaround=6
  A: wait=6,  turnaround=30
  Average turnaround: (3 + 6 + 30) / 3 = 13 ms  ← 48% better than FCFS
  Average wait:       (0 + 3 + 6) / 3  = 3 ms   ← 82% better than FCFS
```

SJF is provably optimal for minimizing average wait time. The problem: **the scheduler cannot know burst times in advance.** You can estimate using exponential averaging of past burst times (`t_{n+1} = α × actual_t_n + (1-α) × predicted_t_n`), but predictions are imperfect. Long jobs may be starved if short jobs keep arriving.

SJF is the theoretical ideal that practical schedulers approximate. The Linux CFS's "give CPU time to whoever has received the least so far" is an approximation that tends toward SJF behavior for mixed workloads.

### 5.3 Round Robin (RR)

Each thread receives a fixed **time quantum** (time slice) of CPU time, then is preempted and put at the back of the queue. The next thread in line runs for its quantum.

```
3 threads, quantum = 4 ms:

Time:  0  4  8  12  16  20  24  27  30
       A  B  C  A   A   A   A   B   C
         
Thread A (24 ms burst): runs at 0, 12, 16, 20, 24 — finishes at 28 ms
Thread B (3 ms burst):  runs at 4  — needs only 3 ms, finishes at 7 ms
Thread C (3 ms burst):  runs at 8  — needs only 3 ms, finishes at 11 ms

(Simpler version — ignore preemption details):
Average turnaround: (28 + 7 + 11) / 3 = 15.3 ms
Average wait:       (4 + 4 + 8) / 3   = 5.3 ms
```

**Quantum size matters:**
- Quantum too small: most CPU time is spent context-switching, not doing work. Throughput collapses.
- Quantum too large: degenerates toward FCFS. Latency for short jobs increases.
- Linux default time slice: ~4 ms for normal processes (adjusted dynamically by CFS).

Round Robin is fair (each thread gets equal time) and provides bounded response time (any thread waits at most (N-1) × quantum before getting CPU). It is the foundation of CFS.

### 5.4 Priority Scheduling

Each thread has a priority. The highest-priority runnable thread always runs next.

**Static priority:** Priority is set at creation and never changes. Problem: **starvation** — low-priority threads may wait forever if high-priority threads keep arriving.

**Dynamic priority (aging):** Priority increases the longer a thread waits. A thread that has been waiting for 10 seconds gets priority-boosted so it eventually runs. Linux implements a form of aging in its interactive priority boost.

**Priority inversion:** A low-priority thread L holds a resource (lock) needed by a high-priority thread H. H is blocked waiting for L to release the lock. Meanwhile, medium-priority thread M runs — it preempts L (L has lower priority) but doesn't hold the resource H needs. H is blocked waiting for L, L is blocked waiting for M, even though H has the highest priority. The high-priority thread is effectively waiting for the lowest-priority thread.

Solution — **priority inheritance:** when H waits for a lock held by L, temporarily boost L's priority to H's level so L can run, release the lock, and let H proceed. Linux `futex` implements priority inheritance as an optional flag (`FUTEX_LOCK_PI`).

### 5.5 Multi-Level Feedback Queue (MLFQ)

MLFQ is the most sophisticated classical scheduler — it approximates SJF without knowing burst times in advance. It maintains multiple priority queues:

```
Queue 0 (highest priority, quantum = 4 ms): new threads start here
Queue 1 (medium priority, quantum = 8 ms):
Queue 2 (lowest priority, quantum = 16 ms): old CPU-hungry threads

Rules:
  1. New thread enters Queue 0.
  2. If a thread uses its full quantum: demote to next lower queue.
  3. If a thread voluntarily yields before quantum expires: stay in same queue.
  4. Every S seconds: boost all threads back to Queue 0 (prevent starvation).

Effect:
  Short/interactive threads (I/O-bound): voluntarily yield frequently → stay in
  Queue 0 (high priority, low latency).
  
  Long CPU-bound threads: use full quantum repeatedly → demote to Queue 2
  (low priority, high throughput but higher latency).

This is SJF approximation: short threads stay at high priority; long threads
are gradually deprioritized — exactly what SJF would compute from known burst times.
```

MLFQ is the conceptual predecessor to Linux's O(1) scheduler (used until 2.6.23) and informs the interactive priority boost in CFS.

---

## 6. Linux CFS: The Completely Fair Scheduler

### 6.1 The CFS Design Goal

CFS was introduced in Linux 2.6.23 (October 2007). Its goal, stated by author Ingo Molnár: "CFS basically models an 'ideal, precise multi-tasking CPU' on real hardware."

An ideal multi-tasking CPU would run N threads simultaneously, each getting 1/N of the CPU's processing power. CFS approximates this: over any large enough time window, every thread gets CPU time proportional to its weight (which is derived from its priority / nice value). No thread is ever completely starved.

### 6.2 Virtual Runtime: The Core Concept

CFS tracks one number per thread: **virtual runtime (vruntime)**. Vruntime is the total amount of CPU time the thread has received, normalized by the thread's weight:

```
vruntime += actual_cpu_time × (default_weight / thread_weight)
```

- A low-weight thread (nice +19): its vruntime accumulates faster (runs "in debt" quickly)
- A high-weight thread (nice -20): its vruntime accumulates slowly (runs "cheaply")

At every scheduling point, CFS selects the thread with the **lowest vruntime** — the thread that has received the least CPU time relative to its fair share. This guarantees fairness: over time, all threads converge to equal vruntime, meaning they've each received their proportional share.

```
Example: 3 equal-priority threads on 1 core, 1-second window

Ideal (ideal multi-tasking CPU):
  Thread A: 333 ms of CPU time → vruntime = 333
  Thread B: 333 ms of CPU time → vruntime = 333
  Thread C: 333 ms of CPU time → vruntime = 333

CFS reality (time slices of ~4 ms each):
  0 ms:   A has lowest vruntime (0) → A runs for 4 ms
  4 ms:   B and C both have vruntime 0, A has 4 → B runs for 4 ms
  8 ms:   C has vruntime 0, A and B have 4 → C runs for 4 ms
  12 ms:  All have vruntime 4 → A runs (tie-breaking by thread creation order)
  ...
  After 1 second: A, B, C each have ~333 ms CPU time. Fair.
```

### 6.3 The Run Queue: Red-Black Tree

CFS stores all runnable threads in a **red-black tree** keyed by vruntime. The leftmost node (minimum vruntime) is always the next thread to run.

```
Red-black tree of runnable threads:
         [vruntime=45]
        /             \
  [vruntime=38]    [vruntime=67]
  /            \
[vruntime=33] [vruntime=41]
     ↑
  leftmost node = next to schedule (minimum vruntime)
  O(1) to find: the CFS scheduler caches the leftmost pointer

Insert (new thread or returning from I/O): O(log N)
Delete (thread uses its timeslice): O(log N)
Find-min (next to schedule):        O(1) [cached leftmost pointer]
```

With N = 1000 runnable threads: O(log 1000) ≈ 10 tree operations per scheduling event. A scheduling event every 4 ms with 1000 threads = 250 scheduling events/s = 2,500 tree operations/s per core. Negligible CPU overhead.

### 6.4 Time Slice Calculation

CFS does not use a fixed time quantum. The time slice for each thread is dynamically calculated:

```
target_latency = max(6 ms, N × 0.75 ms)   where N = number of runnable threads
  (minimum time to complete one full round of all threads)

time_slice_for_thread_i = target_latency × (weight_i / total_weight)
  (proportional share of the target latency period)

With N=4 equal-weight threads, target_latency=6ms:
  Each thread gets 6ms / 4 = 1.5 ms per scheduling round
  
With N=100 equal-weight threads, target_latency=75ms:
  Each thread gets 75ms / 100 = 0.75 ms per scheduling round
  (more threads = more context switches = more overhead, CFS adapts)
```

`/proc/sys/kernel/sched_latency_ns` and `/proc/sys/kernel/sched_min_granularity_ns` control these parameters.

### 6.5 Handling Sleeping Threads

When a thread wakes up after sleeping (I/O completed, lock released, timer fired), its vruntime may be far behind the currently running threads. If CFS used the raw vruntime, the waking thread would monopolize the CPU for a long time to "catch up." CFS prevents this by setting the waking thread's vruntime to:

```
waking_vruntime = max(saved_vruntime, min_vruntime_in_tree − target_latency)
```

This places the waking thread slightly behind the current frontrunner but not so far back that it monopolizes the CPU. It gives the thread priority to run "soon" (because its vruntime is below most running threads) without giving it an unfair amount of CPU time.

This mechanism is what makes CFS good for interactive/I/O-bound threads — a thread that sleeps waiting for a Kafka message will get CPU time quickly when the message arrives, without starving other threads.

### 6.6 Observing CFS in Action

```bash
# View per-thread scheduling statistics
cat /proc/<PID>/schedstat
# Output: cpu_time_ns wait_time_ns timeslices_run

# View per-CPU scheduler statistics
cat /proc/schedstat

# Real-time thread scheduling info
cat /proc/<PID>/sched
# Shows: nr_voluntary_switches, nr_involuntary_switches, vruntime (ns), etc.

# Per-process scheduler stats via perf
perf sched record -p <PID> sleep 5
perf sched latency   # shows scheduling latency distribution

# View scheduling events in real time
perf sched record sleep 1 && perf sched timehist

# pidstat: per-process CPU stats including context switches
pidstat -u -w -p <PID> 1
# %CPU, cswch/s (voluntary), nvcswch/s (involuntary)
```

---

## 7. Priority, Nice Values, and cgroups

### 7.1 Nice Values

Linux inherits Unix's "nice" concept — a process that is "nice" voluntarily gives up CPU time to others. The nice value is an integer from -20 (highest priority) to +19 (lowest priority). Default is 0.

**Nice to weight mapping:**

```
Nice value  →  Weight  →  Relative CPU share (vs nice 0)
   -20           88761          ~10.0×
   -10            9548           ~1.1×  (slight boost)
     0            1024           1.0×   (default)
    +5             335           0.33×  (gets 1/3 the CPU of a nice-0 process)
   +10             110           0.11×
   +19              15           0.015× (almost no CPU)
```

The weight difference between adjacent nice values is ~1.25×. This means a process with nice -1 gets ~25% more CPU than a process with nice 0.

```bash
# Start a process with nice value +10 (lower priority)
nice -n 10 python etl_batch.py

# Change the nice value of a running process
renice -n 5 -p <PID>      # set nice to 5 for PID
renice -n -5 -p <PID>     # set nice to -5 (requires root for negative values)

# View nice values
ps aux     # NI column shows nice value
top        # NI column; press 'r' to renice interactively
```

**Data engineering use case:** On a shared machine running both a Kafka consumer (latency-sensitive) and a batch ETL job (throughput-sensitive):

```bash
# Give the Kafka consumer higher scheduling priority
renice -n -5 -p $(pgrep -f kafka_consumer)   # requires root

# Reduce the batch ETL job's priority so it doesn't starve the consumer
renice -n 10 -p $(pgrep -f etl_batch)

# After: CPU contention resolves in favor of the consumer
# Consumer's vruntime accumulates slower (higher weight) → always gets CPU first
```

### 7.2 Real-Time Scheduling Policies

Linux supports three scheduling policies:

| Policy | Type | Priority | Use Case |
|---|---|---|---|
| `SCHED_OTHER` (CFS) | Normal | nice -20 to +19 | Default for all user processes |
| `SCHED_FIFO` | Real-time | 1–99 (RT) | Always preempts SCHED_OTHER; runs until voluntary yield or higher-priority RT thread |
| `SCHED_RR` | Real-time | 1–99 (RT) | Same as SCHED_FIFO but with time quantum (preempted after quantum even within same priority) |
| `SCHED_BATCH` | Normal (batch) | nice-based | Like SCHED_OTHER but with larger time slices; hints to scheduler it's a batch job |
| `SCHED_IDLE` | Normal (idle) | N/A | Runs only when no other thread wants CPU; lower priority than nice +19 |

Real-time threads (SCHED_FIFO or SCHED_RR) always preempt CFS threads. A SCHED_FIFO priority 1 thread preempts any CFS thread, regardless of the CFS thread's nice value. This is why real-time audio processing, kernel drivers, and some network packet handlers use SCHED_FIFO.

For data engineering: SCHED_BATCH is useful for heavy ETL jobs — it tells the scheduler "this process is not interactive, give it large time slices for throughput, don't boost it for interactive response":

```bash
chrt -b 0 python batch_etl.py    # run with SCHED_BATCH policy
```

### 7.3 cgroups: Resource Isolation for Containers

**cgroups (control groups)** are a Linux kernel feature for grouping processes and applying resource limits to the group as a whole. They are the underlying mechanism for Docker, Kubernetes, and Mesos resource isolation.

**Relevant cgroup controllers for scheduling:**

```
cpu controller:
  cpu.shares     — relative CPU weight for the group (like nice for a group)
  cpu.cfs_period_us  — scheduling period in microseconds (default: 100,000 = 100ms)
  cpu.cfs_quota_us   — maximum CPU time allowed per period (-1 = unlimited)
                       quota/period = max CPU fraction
                       quota=50000, period=100000 → max 50% of one CPU
                       quota=200000, period=100000 → max 2 full CPUs

cpuset controller:
  cpuset.cpus    — which CPU cores this group's processes can run on
  cpuset.mems    — which NUMA nodes this group's processes can allocate memory from
```

**Kubernetes CPU limits and the cgroup connection:**

```yaml
# Kubernetes pod spec
resources:
  requests:
    cpu: "2"       # → cpu.shares = 2048 (2 × 1024 = proportional share)
  limits:
    cpu: "4"       # → cpu.cfs_quota_us = 400000 (4 CPUs × 100ms period)
                   #   cpu.cfs_period_us = 100000 (100ms)
```

When a Kubernetes pod exceeds its CPU limit, the kernel **throttles** it — the process's cgroup has exhausted its quota for the current period, and it is forced to sleep until the next period. This is "CPU throttling" — visible in Kubernetes metrics as `container_cpu_cfs_throttled_seconds_total`.

**CPU throttling is the most common hidden performance problem in Kubernetes data pipelines:**

```bash
# Check if a Kubernetes pod is being CPU throttled
kubectl exec <pod> -- cat /sys/fs/cgroup/cpu/cpu.stat
# Output:
# nr_periods 1000          ← number of 100ms periods observed
# nr_throttled 200         ← periods where the container was throttled
# throttled_time 19000000  ← nanoseconds of throttle time

# If nr_throttled / nr_periods > 5%, the CPU limit is too low
# Fix: increase the CPU limit in the pod spec
```

### 7.4 Setting CPU Shares for Spark Executors

In a YARN cluster, each Spark executor runs in a container. YARN controls CPU shares via cgroups:

```xml
<!-- yarn-site.xml — enable cgroup enforcement -->
<property>
  <name>yarn.nodemanager.linux-container-executor.cgroups.hierarchy</name>
  <value>/hadoop-yarn</value>
</property>
<property>
  <name>yarn.nodemanager.resource.cpu-vcores</name>
  <value>32</value>  <!-- total virtual cores on this node -->
</property>
```

```bash
# Spark submit: request specific vCores per executor
spark-submit \
  --executor-cores 4 \          # 4 virtual cores per executor
  --executor-memory 16g \
  --num-executors 8 \
  my_job.py

# Translates to YARN container with:
#   cpu.shares = (4/32) × 1024 = 128  (4/32 of the node's CPU weight)
#   or if strict enforcement: cpu.cfs_quota_us = 400000 per 100ms period
```

---

## 8. CPU Affinity and NUMA

### 8.1 CPU Affinity

**CPU affinity** is the binding of a thread or process to a specific set of CPU cores. The OS will only schedule that thread on the pinned cores, even if other cores are idle.

Benefits:
1. **Cache warmth:** A thread that always runs on core 3 keeps its working set in core 3's L1/L2 cache. If the OS migrates it to core 7, all cached data is cold (cache miss). Affinity prevents migration.
2. **Predictable latency:** No migration means no cache-miss spikes from core migration.
3. **NUMA locality:** On multi-socket machines, a thread pinned to cores on socket 0 should also be pinned to socket 0's memory (see NUMA below).

**Setting CPU affinity:**

```bash
# Pin process to cores 0 and 1 (bitmask 0x3 = binary 011 = cores 0,1)
taskset -c 0,1 python kafka_consumer.py

# Set affinity of a running process
taskset -cp 0,1 <PID>

# Pin to core ranges
taskset -c 0-7 python spark_executor.py    # cores 0 through 7

# Verify current affinity
taskset -cp <PID>
# Output: pid XXXXX's current affinity list: 0-7
```

In Python:

```python
import os

# Set CPU affinity for the current process (Linux only)
os.sched_setaffinity(0, {0, 1, 2, 3})   # pin to cores 0-3
print(os.sched_getaffinity(0))           # {0, 1, 2, 3}
```

### 8.2 NUMA: Non-Uniform Memory Access

Modern servers have multiple CPU sockets. Each socket has its own memory controller attached to its own DIMM slots. Memory attached to socket 0 is accessed quickly by cores on socket 0 but must cross the **QPI/UPI interconnect** (inter-socket bus) when accessed by cores on socket 1.

```
2-socket NUMA machine:

Socket 0:                    Socket 1:
  Cores 0–15                  Cores 16–31
  L3 Cache (30 MB)            L3 Cache (30 MB)
  Memory Controller           Memory Controller
  DIMM: 128 GB                DIMM: 128 GB
       ↕                            ↕
   ← ── ──── QPI/UPI Interconnect ──── → 
       ↕
  Local memory access: ~80 ns
  Remote memory access (cross-socket): ~140 ns   (75% slower)
```

**NUMA latency asymmetry:**

```bash
# Check NUMA topology
numactl --hardware
# Output:
#   available: 2 nodes (0-1)
#   node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
#   node 0 size: 128000 MB
#   node 1 cpus: 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
#   node 1 size: 128000 MB
#   node distances:
#   node   0   1
#     0:  10  21    ← local=10, remote=21 (2.1x slower)
#     1:  21  10

# Run a process on socket 0's cores with memory from socket 0 only
numactl --cpunodebind=0 --membind=0 python etl_job.py

# Run on socket 1
numactl --cpunodebind=1 --membind=1 python etl_job.py
```

**NUMA in Spark and data engineering:**

Spark executors running on NUMA-unaware configurations allocate memory from whichever NUMA node the Linux memory allocator picks — often the local socket, but not guaranteed. NUMA-aware Spark deployment pins each executor to one socket:

```bash
# Launch Spark executor on NUMA node 0 (cores 0-15, local memory)
numactl --cpunodebind=0 --membind=0 \
  java -Xmx16g -Xms16g \
  org.apache.spark.executor.CoarseGrainedExecutorBackend ...
```

NUMA effects are most pronounced for Spark executors doing hash operations (sort-merge join, groupBy) — these operations access large in-memory hash tables whose access pattern is random. If the hash table is on the remote NUMA node, every probe is a 140 ns access instead of an 80 ns access — 75% slower for memory-bound phases.

### 8.3 CPU Caches and the Scheduling Impact

Modern CPUs have three levels of cache:

```
Core 0:          Core 1:
  L1 data: 32 KB   L1 data: 32 KB   (per-core, fastest: ~4 cycles)
  L1 instr: 32 KB  L1 instr: 32 KB
  L2: 256 KB       L2: 256 KB       (per-core: ~12 cycles)
        ↓                 ↓
  L3: 30 MB (shared among all cores on one socket)  (~40 cycles)
        ↓
  DRAM: 128 GB                                       (~200-300 cycles)
```

When the scheduler migrates a thread from core 0 to core 2:
- Core 0's L1 and L2 caches contain the thread's working set (hot)
- Core 2's L1 and L2 caches are cold — the thread's data is not there
- First accesses after migration cause L1/L2 cache misses → go to L3 or DRAM

For a Spark task doing an in-memory sort (Tungsten sort), the working set is typically the sort buffer (128 MB). After a core migration, the entire sort buffer must be re-fetched from L3 or DRAM — this can add hundreds of milliseconds to the task.

Linux CFS tries to maintain **cache affinity** — it tracks which core a thread last ran on and prefers to schedule it on the same core. But under load (multiple runnable threads per core), the scheduler may migrate to balance load. The `kernel.sched_migration_cost_ns` tuning parameter controls how long CFS waits before migrating a thread:

```bash
# View migration cost threshold (default: 500,000 ns = 500 µs)
cat /proc/sys/kernel/sched_migration_cost_ns

# Increase to reduce migrations (better cache affinity, less balance)
echo 5000000 > /proc/sys/kernel/sched_migration_cost_ns
# (5 ms — CFS will only migrate if the imbalance has persisted for 5 ms)
```

---

## 9. Scheduler Impact on Data Engineering Workloads

### 9.1 Spark: Task Scheduling Latency

**Problem:** Spark's "scheduler delay" in the Spark UI is the time between when the DAGScheduler submits a task and when it starts executing on an executor. This includes network time (task serialization and transport) but also the time the executor's task thread spends waiting in the OS run queue.

```
Spark task execution timeline:
  T0: DAGScheduler serializes task, sends to executor via Netty
  T1: Executor receives task, submits to thread pool
  T2: Thread pool thread picks up task (may wait if all threads busy)
  T3: OS scheduler schedules the executor thread on a CPU core
  T4: Task begins executing
  T5: Task completes

"Scheduler delay" in Spark UI = T4 - T0
"Task deserialization time" = T4 - T3 (approximately)
"OS wait" = T3 - T2 (not directly visible in Spark UI)
```

When an executor runs more task threads than CPU cores (`spark.executor.cores` > physical cores per executor), tasks spend time in the OS run queue. CFS serves them fairly but all tasks are slower. The general rule: `spark.executor.cores` ≤ physical cores allocated to the executor.

**Tuning:**

```bash
# On YARN: ensure executor cores ≤ allocated CPU vCores
spark-submit \
  --executor-cores 4 \          # 4 task threads per executor
  --conf spark.yarn.executor.cpus=4 \  # match: 4 vCores allocated
  my_job.py
  
# On Kubernetes: ensure CPU requests ≥ executor cores
# If executor uses 4 cores but requests only 2 CPUs, 
# the pod will be throttled by cgroup quota → tasks are slow
```

### 9.2 Kafka Consumer: Scheduling Latency Causes Consumer Lag

A Kafka consumer's polling loop has a deadline: it must call `poll()` frequently enough that the broker does not consider it dead and trigger a rebalance (default: `max.poll.interval.ms = 300,000 ms = 5 minutes`, but `session.timeout.ms = 45,000 ms` for heartbeats). More importantly, processing latency directly causes consumer lag.

The `max.poll.records` records fetched per `poll()` call must be processed before the next `poll()`. If the consumer thread is descheduled by the OS for 200 ms between `poll()` calls (because a high-priority batch job consumed all CPU), processing falls behind and lag accumulates.

**Mitigation strategies:**

```bash
# Strategy 1: Increase Kafka consumer process priority
# (requires root)
renice -n -5 -p $(pgrep -f kafka_consumer)

# Strategy 2: Isolate Kafka consumer to dedicated CPU cores
# (separate from batch ETL)
taskset -c 0-3 python kafka_consumer.py    # cores 0-3 for consumer
taskset -c 4-31 python etl_batch.py       # cores 4-31 for ETL

# Strategy 3: Use cgroups to guarantee CPU for consumer
# Create a cgroup that always gets at least 20% CPU
mkdir /sys/fs/cgroup/cpu/kafka-consumer
echo 204800 > /sys/fs/cgroup/cpu/kafka-consumer/cpu.shares  # 204800/1024 ≈ 200 shares
                                                              # vs 1024 default → 20% guaranteed
echo <kafka_consumer_PID> > /sys/fs/cgroup/cpu/kafka-consumer/tasks
```

### 9.3 CPU Steal Time on Cloud VMs

All major cloud providers (AWS EC2, GCP Compute Engine, Azure VMs) use hypervisors (KVM, Xen) that time-share physical CPUs across multiple tenant VMs. When a physical CPU is oversubscribed (more vCPUs than physical CPUs), a VM may want to run but the hypervisor does not give it CPU time — this is reported as **steal time** (`st` in `top` on Linux).

```bash
# View steal time in top
top
# %Cpu(s): 45.2 us, 12.3 sy, 0.0 ni, 35.0 id, 5.0 wa, 0.0 hi, 2.5 si, 0.0 st
#                                                                          ↑
# st = steal time (%)
# This instance is losing 0% to steal — OK
# If st > 5%, the physical host is oversubscribed — upgrade instance type

# vmstat also shows steal time
vmstat 1
# us sy id wa st
# 45 12 35  5  3  ← 3% steal time
```

**Steal time effects on Spark:**

A Spark task that runs for 1000 ms wall-clock time with 10% steal time actually executes only 900 ms of work. For a job with 10,000 tasks all suffering 10% steal, the entire job takes 10% longer than it would on dedicated hardware. This is often mistaken for a Spark tuning problem — the actual cause is the cloud instance type being too small or the physical host being oversubscribed.

**Detection:** If `top` shows `st > 5%` consistently, try moving to a larger instance (fewer vCPUs share the same physical core) or a "compute optimized" instance type with dedicated CPU.

### 9.4 dbt and multiprocessing.Pool: NUMA-Aware Process Distribution

dbt uses `--threads N` to run N model SQL queries in parallel (via a thread pool). Each thread owns one database connection and executes one SQL model at a time. For Python-based dbt tests or custom macros using `multiprocessing.Pool`, process placement on NUMA nodes matters.

```python
# dbt-style parallel model execution with NUMA awareness (custom tooling)
import os
import subprocess
import multiprocessing

def run_model_on_numa_node(model_name: str, numa_node: int) -> bool:
    """Execute a dbt model's SQL with memory pinned to a NUMA node."""
    result = subprocess.run(
        ["numactl", f"--membind={numa_node}", "--", 
         "python", "run_single_model.py", model_name],
        capture_output=True
    )
    return result.returncode == 0

# Split models across NUMA nodes
models = ["stg_events", "stg_users", "int_engagement", "mart_revenue"]
with multiprocessing.Pool(4) as pool:
    # Round-robin assignment to NUMA nodes 0 and 1
    args = [(m, i % 2) for i, m in enumerate(models)]
    pool.starmap(run_model_on_numa_node, args)
```

---

## 10. Mental Models

### 10.1 The CFS Bank Account Model

Imagine each thread has a bank account measuring "CPU debt." When a thread runs, it accumulates debt (vruntime increases). When it sleeps, its debt stays frozen. CFS always serves the thread with the least debt — the one that has received the least CPU time relative to its fair share.

A high-priority thread (nice -10) accumulates debt more slowly — it's like a VIP customer whose bill is calculated at a lower rate. The same 10 ms of service costs a VIP only 2 ms of "debt," while a standard customer accumulates 10 ms of debt. CFS re-selects the lowest-debt customer after each service unit.

A thread that just woke up from a long sleep has stale (low) debt. CFS would normally give it a lot of CPU time to "catch up." But the sleep-wakeup vruntime reset prevents this: the waking thread's debt is set to "just behind the current minimum" rather than the old stale value.

### 10.2 The Highway On-Ramp Model for Priority Scheduling

Imagine a highway with a dedicated fast lane (high priority) and normal lanes. Cars in the fast lane (SCHED_FIFO / SCHED_RR) always enter the highway first, regardless of how long normal cars have been waiting. Once the fast lane is empty, normal cars enter in CFS order.

Nice values adjust lane width within the normal lanes: a nice -5 car gets 3 normal lanes, a nice +10 car gets only half a lane. The physical highway capacity (CPUs) is shared, but not equally.

The highway metaphor also explains starvation: if fast lane traffic never clears, normal-lane cars wait forever. The aging mechanism (vruntime boost for long-waiting CFS threads) is like a sign saying "if any normal car has waited more than X minutes, let it merge regardless of fast lane traffic."

### 10.3 NUMA as a Building Model

A 2-socket server is like a 2-floor office building. Each floor has its own storage room (local memory). Workers on floor 1 can access floor 1's storage quickly (walk downstairs). To access floor 2's storage, they must take an elevator (QPI interconnect) — 75% more time.

CPU affinity is like assigning workers to stay on one floor. NUMA binding is assigning their filing cabinets (memory) to the same floor. A thread running on socket 0 with memory on socket 1 is a worker on floor 1 constantly taking the elevator to retrieve documents from floor 2 — inefficient.

---

## 11. Failure Scenarios

### 11.1 Kubernetes CPU Throttling — Silent Performance Degradation

```yaml
# Kubernetes pod spec with CPU limits
resources:
  requests:
    cpu: "2"      # scheduler places pod on a node with 2+ available CPUs
  limits:
    cpu: "2"      # cgroup quota: 200ms per 100ms period (2 CPUs max)
```

```bash
# Spark executor with 4 task threads inside a container limited to 2 CPUs
# 4 threads compete for 2 CPUs → cgroup throttling when all 4 are CPU-active

# Observed symptom:
#   Spark task takes 30 seconds instead of 15 seconds
#   CPU utilization shows only 2 CPUs used (correct: limit is 2)
#   But tasks are slower than expected for 2-CPU work

# Why: 4 threads × 2-CPU limit = each thread gets 0.5 CPU average
#      Tasks designed for 1-thread-1-CPU run at half speed

# Check throttling:
kubectl exec spark-executor-pod -- cat /sys/fs/cgroup/cpu/cpu.stat
# nr_throttled: 500   ← throttled 500 out of 1000 periods = 50% throttle rate
# throttled_time: 25000000000  ← 25 seconds of throttle time

# Fix option 1: increase CPU limit to match executor cores
# resources.limits.cpu: "4"

# Fix option 2: reduce executor cores to match CPU limit
# spark.executor.cores = 2 (not 4)

# Fix option 3: remove CPU limits (use only requests)
# resources.limits: {}  (no limit — other pods may be impacted)
```

### 11.2 Noisy Neighbor: Batch Job Starving Kafka Consumer

```
Production scenario:
  Machine: 8-core server
  Processes:
    kafka_consumer.py: 1 thread, processing 5,000 msg/s, nice 0
    etl_batch.py:      8 threads, CPU-intensive transforms, nice 0

CFS assignment:
  9 threads competing for 8 cores.
  CFS distributes fairly: each thread gets 8/9 = 89% of one CPU.
  
  Consumer thread gets 89% of one CPU — OK.
  But: when all 8 ETL threads are actively running,
  the consumer thread loses scheduling contention moments.
  
  Result: consumer scheduling latency spikes from 1 ms to 10-50 ms.
  max.poll.interval.ms is not exceeded, but processing latency increases.
  Consumer lag starts to climb.

Detection:
  # On the kafka_consumer process:
  pidstat -u -w -p $(pgrep -f kafka_consumer) 1
  # nvcswch/s spikes from 0 to 50+ during ETL runs
  # (consumer is being preempted involuntarily = ETL is competing)

Fix:
  renice -n -10 -p $(pgrep -f kafka_consumer)   # consumer gets 4.5x more weight
  renice -n +10 -p $(pgrep -f etl_batch)        # ETL gets less weight

After:
  Consumer thread priority dominates → scheduling latency drops back to 1 ms
  ETL job throughput decreases proportionally (expected trade-off)
```

### 11.3 NUMA Memory Bandwidth Saturation

```
Scenario: Spark job sorting 100 GB of data on a 2-socket machine.
  64 cores total (32 per socket), 256 GB total (128 GB per socket).
  
  Spark allocates executors without NUMA awareness:
    16 executors, each 4 cores, 14 GB memory
    Executors may have cores on socket 0 but memory allocated on socket 1
    (Linux NUMA allocator: first-touch policy → memory allocated on
    the NUMA node where the first-touching thread runs, which may not
    be the socket where the executor's threads run later)

  Observed:
    Memory bandwidth: local = 50 GB/s per socket, remote = 20 GB/s per socket
    Sorting is memory-bandwidth bound (Tungsten sort scans the sort buffer)
    Tasks take 40% longer than on a NUMA-aware deployment

Detection:
  # numastat shows remote memory accesses
  numastat -n
  # node0  node1
  # numa_hit:     82000   65000   ← local accesses
  # numa_miss:    18000   35000   ← remote accesses (cross-socket)
  # numa_foreign: 35000   18000   ← local memory used by remote nodes

  # High numa_miss / (numa_hit + numa_miss) = NUMA efficiency problem

Fix:
  # Pin each executor to one NUMA node
  numactl --cpunodebind=0 --membind=0 spark-class CoarseGrainedExecutorBackend ...
  numactl --cpunodebind=1 --membind=1 spark-class CoarseGrainedExecutorBackend ...
  
  # Or use Spark's built-in NUMA awareness:
  spark.executor.extraJavaOptions=-Djna.library.path=/usr/lib
  # (NUMA support in Spark is limited; manual numactl is more reliable)
```

### 11.4 Priority Inversion in Spark Shuffle

```
Scenario: Spark sort-merge join, multiple executors.
  Task A (high Spark priority): needs shuffle data from Task B's output
  Task B (low Spark priority): currently writing shuffle output
  Task C (medium Spark priority): running on the same executor as B

  At the OS level (all tasks are equal-weight CFS threads):
    Thread-B: writing shuffle data (holding an OS lock on the shuffle file)
    Thread-C: preempts Thread-B (CFS — equal vruntime, context switch)
    Thread-A: waiting for Thread-B to finish writing shuffle data

  Thread-A is blocked (waiting for shuffle data)
  Thread-B is descheduled (CFS preempted it in favor of Thread-C)
  Thread-C is running (neither urgently needed nor blocked)
  
  → Thread-A (highest Spark priority task) is delayed waiting for
    Thread-B which is delayed by Thread-C — classic priority inversion.

  Mitigation at the OS level:
    Threads doing shuffle I/O benefit from SCHED_BATCH policy:
    chrt -b 0 -p <shuffle_writer_TID>
    (larger time slices = completes shuffle write faster = unblocks waiting tasks)
    
  Mitigation at the Spark level:
    spark.scheduler.mode=FAIR (Spark's own fair scheduling between jobs)
    FIFO scheduler ensures shuffle-critical tasks are not interrupted by
    unrelated tasks at the Spark level (OS-level priority inversion remains)
```

---

## 12. Data Engineering Connections

### 12.1 YARN Scheduler and CFS: Two Layers of Scheduling

Running Spark on YARN means two independent schedulers are active simultaneously:

**YARN scheduler** (cluster-level): decides which applications get containers (executor JVM processes), on which nodes, with how much memory and CPU. YARN offers three schedulers:

- **FifoScheduler:** Applications run in submission order. The first submitted application gets all resources until it completes. Simple but no fairness.
- **CapacityScheduler:** Partitions cluster into named queues with capacity guarantees. Queue A always gets at least 40% of the cluster; Queue B gets at least 60%. Applications within a queue are served FIFO.
- **FairScheduler:** All applications share the cluster equally. Each application gets 1/N of the cluster's resources. New applications get resources immediately (small allocation), and existing applications shrink proportionally.

**Linux CFS** (node-level): within each node, the kernel schedules the threads of all running executor JVM processes. YARN controls how many executor processes and how many vCores per executor run on a node, but within those limits, CFS fairly distributes CPU time.

The interaction: YARN's FairScheduler ensures job A and job B each get 50% of the cluster's containers. Within each container, CFS ensures each task thread gets equal CPU time. Together they implement multi-level fair scheduling.

### 12.2 Spark's Internal Task Scheduler and FIFO vs FAIR Mode

Within one Spark application, the Spark DAGScheduler assigns stages to the TaskScheduler, which submits tasks to executors. Spark has two internal scheduling modes:

**FIFO (default):** Stages from the first-submitted job run before stages from the second-submitted job. In a SparkContext shared between multiple notebooks (SparkContext multiplexed via Thrift/JDBC), one heavy query can starve all others.

```python
# Configure Spark's internal scheduler
spark.conf.set("spark.scheduler.mode", "FAIR")

# With FAIR: multiple jobs share executor resources
# round-robin across pools
# job1_task runs, then job2_task, then job1_task, ...
```

**FAIR:** Multiple jobs submitted to the same SparkContext are interleaved. Each job gets a pool, and pools are allocated CPU proportionally to `weight` and `minShare` settings defined in a `fairscheduler.xml` file.

```xml
<!-- fairscheduler.xml -->
<allocations>
  <pool name="production">
    <schedulingMode>FIFO</schedulingMode>
    <weight>10</weight>        <!-- 10× weight vs development -->
    <minShare>8</minShare>     <!-- always gets at least 8 task slots -->
  </pool>
  <pool name="development">
    <schedulingMode>FIFO</schedulingMode>
    <weight>1</weight>
    <minShare>2</minShare>
  </pool>
</allocations>
```

```python
# Assign a DataFrame operation to a pool
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
df.write.parquet("s3://...")   # this job uses the 'production' pool
```

This is CFS at the Spark application level: each pool has a weight, and the Spark scheduler gives task slots to pools proportionally.

### 12.3 Airflow: Process Priority for Time-Sensitive Tasks

Airflow does not natively control OS scheduling priority for task subprocesses. But critical tasks (SLA-bound) can be given OS priority via a wrapper:

```python
# Airflow operator that sets task process priority
from airflow.operators.python import PythonOperator
import os

def run_with_priority(priority: int, fn):
    """Decorator that sets the process nice value before running fn."""
    def wrapper(**context):
        current_priority = os.getpriority(os.PRIO_PROCESS, 0)
        try:
            os.setpriority(os.PRIO_PROCESS, 0, priority)
        except PermissionError:
            pass  # requires root for negative nice values
        result = fn(**context)
        os.setpriority(os.PRIO_PROCESS, 0, current_priority)
        return result
    return wrapper

def critical_sla_task(**context):
    # ... time-sensitive processing ...
    pass

with DAG("sla_dag", ...) as dag:
    task = PythonOperator(
        task_id="critical_task",
        python_callable=run_with_priority(-5, critical_sla_task),
    )
```

For container-based Airflow (KubernetesExecutor), set pod resource requests and limits to guarantee CPU:

```yaml
# airflow.cfg or Kubernetes pod template
[kubernetes]
worker_pods_creation_batch_size = 8

# Pod template for high-priority tasks
pod_template_file = /path/to/pod_template.yaml
```

```yaml
# pod_template.yaml for SLA-critical tasks
resources:
  requests:
    cpu: "2"
  limits:
    cpu: "2"      # exact — no throttling possible
priorityClassName: high-priority   # Kubernetes scheduling priority
```

---

## 13. Code Toolkit

### 13.1 `scheduler_observer.py` — CFS Metrics and Process Priority Tools

```python
"""
scheduler_observer.py — Observe and control Linux process scheduling.

Provides:
  - Read per-process scheduling statistics from /proc
  - Read and set nice values
  - Read and set CPU affinity
  - Observe CPU steal time from /proc/stat
  - Measure scheduling latency (time to acquire a lock as a proxy)

Requires Linux. Run directly for a system scheduling profile.
"""
from __future__ import annotations
import os
import sys
import time
import threading
import subprocess
from pathlib import Path
from dataclasses import dataclass


# ─── Process scheduling info ──────────────────────────────────────────────────

@dataclass
class ProcessSchedInfo:
    pid: int
    comm: str                # process name
    nice: int
    policy: str              # SCHED_OTHER, SCHED_FIFO, SCHED_RR, etc.
    affinity: set[int]       # CPU cores this process is allowed to run on
    voluntary_switches: int  # how many times the process voluntarily yielded CPU
    involuntary_switches: int  # how many times the OS preempted the process
    vruntime_ms: float       # virtual runtime in milliseconds (from /proc/<pid>/sched)

    @property
    def is_cpu_bound(self) -> bool:
        """Heuristic: high involuntary switches → process wants more CPU than it gets."""
        total = self.voluntary_switches + self.involuntary_switches
        if total == 0:
            return False
        return self.involuntary_switches / total > 0.3  # >30% involuntary → CPU hungry


def get_process_sched_info(pid: int) -> ProcessSchedInfo | None:
    """Read scheduling info for a process from /proc."""
    if not sys.platform.startswith("linux"):
        return None

    try:
        # Read comm (process name)
        comm = Path(f"/proc/{pid}/comm").read_text().strip()

        # Read nice value and scheduling policy
        stat_path = Path(f"/proc/{pid}/stat")
        stat_fields = stat_path.read_text().split()
        nice = int(stat_fields[18])
        # Policy encoding: 0=SCHED_OTHER, 1=SCHED_FIFO, 2=SCHED_RR, 3=SCHED_BATCH, 5=SCHED_IDLE
        policy_codes = {0: "SCHED_OTHER", 1: "SCHED_FIFO", 2: "SCHED_RR",
                        3: "SCHED_BATCH", 5: "SCHED_IDLE"}
        policy_code = int(stat_fields[40]) if len(stat_fields) > 40 else 0
        policy = policy_codes.get(policy_code, f"UNKNOWN({policy_code})")

        # Read CPU affinity
        try:
            affinity = os.sched_getaffinity(pid)
        except (PermissionError, ProcessLookupError):
            affinity = set()

        # Read voluntary/involuntary switches from /proc/<pid>/sched
        vswitches = 0
        nvswitches = 0
        vruntime_ms = 0.0
        sched_path = Path(f"/proc/{pid}/sched")
        if sched_path.exists():
            for line in sched_path.read_text().splitlines():
                if "nr_voluntary_switches" in line:
                    vswitches = int(line.split(":")[-1].strip())
                elif "nr_involuntary_switches" in line:
                    nvswitches = int(line.split(":")[-1].strip())
                elif "vruntime" in line and "sum_exec_runtime" not in line:
                    try:
                        vruntime_ms = float(line.split(":")[-1].strip()) / 1_000_000
                    except ValueError:
                        pass

        return ProcessSchedInfo(
            pid=pid, comm=comm, nice=nice, policy=policy,
            affinity=affinity, voluntary_switches=vswitches,
            involuntary_switches=nvswitches, vruntime_ms=vruntime_ms
        )
    except (FileNotFoundError, PermissionError, ValueError, IndexError):
        return None


# ─── CPU steal time ────────────────────────────────────────────────────────────

@dataclass
class CpuTimes:
    user: float
    system: float
    idle: float
    iowait: float
    steal: float

    @property
    def total(self) -> float:
        return self.user + self.system + self.idle + self.iowait + self.steal

    @property
    def steal_pct(self) -> float:
        return (self.steal / self.total * 100) if self.total > 0 else 0.0

    @property
    def iowait_pct(self) -> float:
        return (self.iowait / self.total * 100) if self.total > 0 else 0.0

    @property
    def cpu_util_pct(self) -> float:
        return ((self.user + self.system) / self.total * 100) if self.total > 0 else 0.0


def read_cpu_times() -> CpuTimes | None:
    """Read cumulative CPU times from /proc/stat (Linux only)."""
    if not sys.platform.startswith("linux"):
        return None
    try:
        line = Path("/proc/stat").read_text().splitlines()[0]
        fields = line.split()  # cpu user nice system idle iowait irq softirq steal guest
        user   = int(fields[1]) + int(fields[2])  # user + nice
        system = int(fields[3])
        idle   = int(fields[4])
        iowait = int(fields[5]) if len(fields) > 5 else 0
        steal  = int(fields[8]) if len(fields) > 8 else 0
        return CpuTimes(user=user, system=system, idle=idle, iowait=iowait, steal=steal)
    except (FileNotFoundError, IndexError, ValueError):
        return None


def measure_steal_time(duration_s: float = 2.0) -> dict:
    """Measure CPU steal time over a duration."""
    t1 = read_cpu_times()
    if t1 is None:
        return {"error": "not on Linux"}
    time.sleep(duration_s)
    t2 = read_cpu_times()

    delta = CpuTimes(
        user=t2.user - t1.user, system=t2.system - t1.system,
        idle=t2.idle - t1.idle, iowait=t2.iowait - t1.iowait,
        steal=t2.steal - t1.steal
    )
    return {
        "cpu_util_pct": delta.cpu_util_pct,
        "iowait_pct":   delta.iowait_pct,
        "steal_pct":    delta.steal_pct,
        "steal_warning": delta.steal_pct > 5.0,
    }


# ─── Scheduling latency measurement ──────────────────────────────────────────

def measure_scheduling_latency(n_samples: int = 1000) -> dict:
    """
    Measure scheduling latency: time to acquire an uncontended lock.
    This approximates the overhead of one scheduling event —
    not exactly, but gives a lower bound on per-thread dispatch cost.
    """
    lock = threading.Lock()
    samples = []
    for _ in range(n_samples):
        t0 = time.perf_counter_ns()
        with lock:
            pass
        samples.append(time.perf_counter_ns() - t0)

    samples.sort()
    return {
        "mean_ns":   sum(samples) // len(samples),
        "p50_ns":    samples[len(samples) // 2],
        "p99_ns":    samples[int(0.99 * len(samples))],
        "p999_ns":   samples[int(0.999 * len(samples))],
        "max_ns":    samples[-1],
    }


# ─── Nice and affinity controls ───────────────────────────────────────────────

def set_nice(pid: int, nice_value: int) -> bool:
    """Set nice value for a process. Returns True on success."""
    try:
        os.setpriority(os.PRIO_PROCESS, pid, nice_value)
        return True
    except PermissionError:
        print(f"  Permission denied: setting nice < 0 requires root. "
              f"Current nice: {os.getpriority(os.PRIO_PROCESS, pid)}")
        return False
    except ProcessLookupError:
        print(f"  Process {pid} not found.")
        return False


def set_affinity(pid: int, cores: set[int]) -> bool:
    """Pin a process to specific CPU cores."""
    if not hasattr(os, 'sched_setaffinity'):
        print("  sched_setaffinity not available (not Linux)")
        return False
    try:
        os.sched_setaffinity(pid, cores)
        return True
    except PermissionError:
        print(f"  Permission denied: cannot set affinity for PID {pid}")
        return False


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pid = os.getpid()
    print(f"=== Scheduling Observer (PID={pid}) ===\n")

    # Current process info
    info = get_process_sched_info(pid)
    if info:
        print(f"Process: {info.comm}  PID={info.pid}")
        print(f"  Nice value:     {info.nice}")
        print(f"  Policy:         {info.policy}")
        print(f"  CPU affinity:   {sorted(info.affinity)}")
        print(f"  Voluntary switches:   {info.voluntary_switches:,}")
        print(f"  Involuntary switches: {info.involuntary_switches:,}")
        print(f"  CPU-bound heuristic:  {info.is_cpu_bound}")
        print(f"  vruntime:       {info.vruntime_ms:.2f} ms")
    else:
        print("  (scheduling info not available on this platform)")
        print(f"  Nice value (os.getpriority): {os.getpriority(os.PRIO_PROCESS, 0)}")
        if hasattr(os, 'sched_getaffinity'):
            print(f"  CPU affinity: {sorted(os.sched_getaffinity(0))}")

    # CPU steal time
    print(f"\nMeasuring CPU steal time over 2 seconds...")
    steal = measure_steal_time(2.0)
    if "error" not in steal:
        print(f"  CPU util: {steal['cpu_util_pct']:.1f}%")
        print(f"  I/O wait: {steal['iowait_pct']:.1f}%")
        print(f"  Steal:    {steal['steal_pct']:.1f}%", end="")
        if steal['steal_warning']:
            print("  ← WARNING: >5% steal — host is oversubscribed!")
        else:
            print("  (OK)")
    else:
        print(f"  {steal['error']}")

    # Scheduling latency
    print(f"\nMeasuring scheduling/lock latency (1,000 samples)...")
    latency = measure_scheduling_latency(1000)
    print(f"  Mean:  {latency['mean_ns']:,} ns")
    print(f"  P50:   {latency['p50_ns']:,} ns")
    print(f"  P99:   {latency['p99_ns']:,} ns")
    print(f"  P999:  {latency['p999_ns']:,} ns")
    print(f"  Max:   {latency['max_ns']:,} ns")

    # Nice value demo
    print(f"\n=== Nice Value Control ===\n")
    current_nice = os.getpriority(os.PRIO_PROCESS, 0)
    print(f"  Current nice: {current_nice}")
    print(f"  Setting nice to +5 (lower priority)...")
    if set_nice(0, 5):
        new_nice = os.getpriority(os.PRIO_PROCESS, 0)
        print(f"  New nice: {new_nice}")
        set_nice(0, current_nice)   # restore
        print(f"  Restored to: {os.getpriority(os.PRIO_PROCESS, 0)}")

    # CPU affinity demo
    if hasattr(os, 'sched_setaffinity'):
        print(f"\n=== CPU Affinity Control ===\n")
        all_cores = os.sched_getaffinity(0)
        print(f"  Current affinity: cores {sorted(all_cores)}")
        if len(all_cores) > 1:
            # Pin to just one core
            first_core = min(all_cores)
            print(f"  Pinning to core {first_core}...")
            set_affinity(0, {first_core})
            print(f"  New affinity: {sorted(os.sched_getaffinity(0))}")
            set_affinity(0, all_cores)  # restore
            print(f"  Restored to: {sorted(os.sched_getaffinity(0))}")
```

### 13.2 `cfs_simulator.py` — CFS Red-Black Tree Simulation

```python
"""
cfs_simulator.py — Simulates the Linux CFS scheduler.

Models:
  - Virtual runtime (vruntime) accumulation per thread
  - Red-black tree run queue (implemented as a sorted list for clarity)
  - Nice-to-weight mapping
  - Sleep/wake vruntime adjustment
  - Scheduling decisions over a simulated time window

Run directly to see CFS fairly distributing CPU time.
"""
from __future__ import annotations
import heapq
from dataclasses import dataclass, field
from typing import Optional

# Nice value → CFS weight mapping (from Linux kernel sched.h)
NICE_TO_WEIGHT = {
    -20: 88761, -19: 71755, -18: 56483, -17: 46273, -16: 36291,
    -15: 29154, -14: 23254, -13: 18705, -12: 14949, -11: 11916,
    -10:  9548,  -9:  7620,  -8:  6100,  -7:  4904,  -6:  3906,
     -5:  3121,  -4:  2501,  -3:  1991,  -2:  1586,  -1:  1277,
      0:  1024,   1:   820,   2:   655,   3:   526,   4:   423,
      5:   335,   6:   272,   7:   215,   8:   172,   9:   137,
     10:   110,  11:    87,  12:    70,  13:    56,  14:    45,
     15:    36,  16:    29,  17:    23,  18:    18,  19:    15,
}

DEFAULT_WEIGHT = NICE_TO_WEIGHT[0]   # 1024


@dataclass(order=True)
class Thread:
    """A simulated thread with CFS vruntime tracking."""
    vruntime: float           # virtual runtime (ns) — sort key
    tid: int = field(compare=False)
    name: str = field(compare=False)
    nice: int = field(compare=False, default=0)
    remaining_work_ns: float = field(compare=False, default=0)  # simulated burst
    total_cpu_ns: float = field(compare=False, default=0.0)     # total actual CPU received
    state: str = field(compare=False, default="runnable")  # runnable, sleeping, done

    @property
    def weight(self) -> int:
        return NICE_TO_WEIGHT.get(self.nice, DEFAULT_WEIGHT)


class CFSSimulator:
    """
    Simulates the Linux Completely Fair Scheduler.
    
    Uses a min-heap as the run queue (keyed by vruntime).
    Simulates scheduling decisions over a total_time_ns window.
    """
    
    def __init__(self, target_latency_ns: float = 6_000_000):  # 6 ms default
        self.run_queue: list[Thread] = []    # min-heap by vruntime
        self.threads: dict[int, Thread] = {}
        self.target_latency_ns = target_latency_ns
        self.current_time_ns = 0.0
        self.log: list[str] = []
        self._next_tid = 1

    def add_thread(self, name: str, nice: int = 0, burst_ns: float = 10_000_000,
                   arrival_time_ns: float = 0) -> Thread:
        """Add a thread to the simulation."""
        tid = self._next_tid
        self._next_tid += 1
        
        # New thread vruntime: set to min_vruntime so it runs soon but not forever
        min_vr = self.run_queue[0].vruntime if self.run_queue else 0.0
        t = Thread(
            vruntime=min_vr, tid=tid, name=name, nice=nice,
            remaining_work_ns=burst_ns
        )
        self.threads[tid] = t
        if arrival_time_ns <= self.current_time_ns:
            heapq.heappush(self.run_queue, t)
            self.log.append(f"  t={self.current_time_ns/1e6:.2f}ms: {name} (nice={nice}) entered run queue  vruntime={min_vr/1e6:.2f}ms")
        return t

    def _total_weight(self) -> float:
        return sum(t.weight for t in self.run_queue)

    def _time_slice(self, thread: Thread) -> float:
        """CFS time slice: proportional share of target_latency."""
        n = len(self.run_queue)
        if n == 0:
            return self.target_latency_ns
        tw = self._total_weight()
        if tw == 0:
            return self.target_latency_ns / n
        return self.target_latency_ns * (thread.weight / tw)

    def run(self, total_time_ns: float) -> dict:
        """Run the CFS simulation for total_time_ns nanoseconds."""
        end_time = self.current_time_ns + total_time_ns
        schedule_count = 0

        while self.current_time_ns < end_time and self.run_queue:
            # Select thread with minimum vruntime (leftmost in red-black tree)
            current = heapq.heappop(self.run_queue)
            schedule_count += 1

            # Calculate time slice
            ts = min(self._time_slice(current), current.remaining_work_ns)
            ts = min(ts, end_time - self.current_time_ns)

            # Run for ts nanoseconds
            self.current_time_ns += ts
            current.total_cpu_ns += ts
            current.remaining_work_ns -= ts

            # Update vruntime: actual time × (DEFAULT_WEIGHT / thread_weight)
            # Higher weight → vruntime grows slower → thread stays at front of queue
            current.vruntime += ts * (DEFAULT_WEIGHT / current.weight)

            self.log.append(
                f"  t={self.current_time_ns/1e6:.2f}ms: {current.name:<20} "
                f"ran {ts/1e6:.2f}ms  "
                f"total_cpu={current.total_cpu_ns/1e6:.2f}ms  "
                f"vruntime={current.vruntime/1e6:.2f}ms"
            )

            if current.remaining_work_ns > 0:
                heapq.heappush(self.run_queue, current)
            else:
                current.state = "done"
                self.log.append(f"    → {current.name} COMPLETED at t={self.current_time_ns/1e6:.2f}ms")

        # Compute fairness: coefficient of variation of CPU allocation
        cpu_times = [t.total_cpu_ns for t in self.threads.values()]
        weights   = [t.weight for t in self.threads.values()]
        
        # Expected CPU per thread = total_time_ns × (weight / total_weight)
        total_w = sum(weights)
        expected = [total_time_ns * (w / total_w) for w in weights]
        deviations = [abs(c - e) / e for c, e in zip(cpu_times, expected) if e > 0]
        avg_deviation = sum(deviations) / len(deviations) if deviations else 0.0

        return {
            "total_time_ms": total_time_ns / 1e6,
            "schedule_count": schedule_count,
            "cpu_per_thread": {t.name: t.total_cpu_ns / 1e6 for t in self.threads.values()},
            "avg_fairness_deviation_pct": avg_deviation * 100,
        }


if __name__ == "__main__":
    print("=== CFS Simulation: 3 Equal-Priority Threads ===\n")
    sim = CFSSimulator(target_latency_ns=6_000_000)  # 6 ms target latency
    sim.add_thread("thread_A", nice=0, burst_ns=50_000_000)  # 50 ms of work
    sim.add_thread("thread_B", nice=0, burst_ns=50_000_000)
    sim.add_thread("thread_C", nice=0, burst_ns=50_000_000)

    result = sim.run(total_time_ns=30_000_000)  # simulate 30 ms
    for entry in sim.log[:20]:   # first 20 events
        print(entry)
    print(f"  ... ({result['schedule_count']} total scheduling events)")
    print(f"\nCPU time per thread (ms):")
    for name, cpu_ms in sorted(result['cpu_per_thread'].items()):
        print(f"  {name}: {cpu_ms:.2f} ms")
    print(f"Fairness deviation: {result['avg_fairness_deviation_pct']:.2f}% "
          f"(0% = perfectly fair)")

    print("\n=== CFS Simulation: Mixed Priorities ===\n")
    sim2 = CFSSimulator(target_latency_ns=6_000_000)
    sim2.add_thread("kafka_consumer", nice=-5,  burst_ns=100_000_000)  # higher priority
    sim2.add_thread("etl_batch",      nice=+10, burst_ns=100_000_000)  # lower priority
    sim2.add_thread("normal_task",    nice=0,   burst_ns=100_000_000)  # default

    result2 = sim2.run(total_time_ns=30_000_000)
    print(f"CPU time allocation over 30ms:")
    total_cpu = sum(result2['cpu_per_thread'].values())
    for name, cpu_ms in sorted(result2['cpu_per_thread'].items(), key=lambda x: -x[1]):
        pct = cpu_ms / total_cpu * 100
        weight = NICE_TO_WEIGHT.get(
            next(t.nice for t in sim2.threads.values() if t.name == name), DEFAULT_WEIGHT
        )
        print(f"  {name:<20} {cpu_ms:>6.2f} ms  ({pct:.1f}%)  weight={weight}")
    print(f"\nLesson: nice=-5 thread gets ~3× more CPU than nice=+10 thread")
    print(f"        (weight 3121 vs 110 = {3121//110:.0f}× ratio)")
```

---

## 14. Hands-On Labs

### Lab 1: Observe CFS in Action with Python

**Goal:** Run competing CPU-bound threads and observe their CPU usage via `/proc`. Confirm CFS distributes fairly.

```python
# lab1_cfs_fairness.py
"""
Run N competing threads doing CPU-bound work.
Observe that CFS distributes CPU time approximately equally.
Requires Linux (/proc filesystem).
"""
import threading
import time
import os
import sys
from pathlib import Path

def busy_work(stop_event: threading.Event) -> None:
    """CPU-bound loop until stop_event is set."""
    i = 0
    while not stop_event.is_set():
        i = (i * 7 + 13) % 1000003  # cheap arithmetic

def read_thread_cpu_ns(pid: int, tid: int) -> int:
    """Read CPU time for a thread from /proc/<pid>/task/<tid>/stat."""
    try:
        fields = Path(f"/proc/{pid}/task/{tid}/stat").read_text().split()
        utime = int(fields[13])  # user time in clock ticks
        stime = int(fields[14])  # kernel time in clock ticks
        ticks_per_sec = os.sysconf("SC_CLK_TCK")
        return (utime + stime) * (1_000_000_000 // ticks_per_sec)
    except Exception:
        return 0

if __name__ == "__main__" and sys.platform.startswith("linux"):
    N = 4
    stop = threading.Event()
    threads = [threading.Thread(target=busy_work, args=(stop,)) for _ in range(N)]
    
    pid = os.getpid()
    for t in threads:
        t.start()
    
    # Sample CPU time after 2 seconds
    time.sleep(2)
    stop.set()
    
    cpu_times = []
    for t in threads:
        t.join()
    
    # Get all thread TIDs from /proc
    try:
        tids = [int(d) for d in os.listdir(f"/proc/{pid}/task")
                if d.isdigit() and int(d) != pid]
        cpu_times = [(tid, read_thread_cpu_ns(pid, tid)) for tid in tids[:N]]
    except Exception as e:
        print(f"Could not read thread stats: {e}")
        cpu_times = []
    
    if cpu_times:
        total = sum(c for _, c in cpu_times)
        print(f"CPU time distribution ({N} equal-priority threads, 2s run):")
        for tid, cpu_ns in sorted(cpu_times, key=lambda x: -x[1]):
            pct = cpu_ns / total * 100 if total > 0 else 0
            print(f"  TID {tid}: {cpu_ns/1e6:>8.1f} ms ({pct:.1f}%)")
        print(f"\nExpected: each thread ~{100/N:.1f}%")
        print(f"CFS fairness: deviation from expected = "
              f"{max(abs(c/total*100 - 100/N) for _, c in cpu_times):.1f} percentage points")
elif not sys.platform.startswith("linux"):
    print("This lab requires Linux (/proc filesystem). Run on a Linux machine.")
```

### Lab 2: Nice Value Impact on CPU Allocation

**Goal:** Measure how different nice values change CPU allocation between competing processes.

```bash
# lab2_nice_values.sh
# Run two Python processes competing for CPU, one with nice +10
# Observe CPU allocation via top or pidstat

#!/bin/bash

# Start a CPU-bound Python process at default nice (0)
python3 -c "
import time
end = time.time() + 10
while time.time() < end:
    _ = sum(range(10000))
print('Normal done')
" &
NORMAL_PID=$!

# Start an identical process at nice +10 (lower priority)
nice -n 10 python3 -c "
import time
end = time.time() + 10
while time.time() < end:
    _ = sum(range(10000))
print('Low-priority done')
" &
LOW_PID=$!

echo "Normal PID: $NORMAL_PID (nice 0)"
echo "Low-priority PID: $LOW_PID (nice +10)"
echo ""
echo "Monitoring CPU for 8 seconds..."

# Monitor both processes
for i in $(seq 1 8); do
    sleep 1
    normal_cpu=$(ps -p $NORMAL_PID -o %cpu --no-headers 2>/dev/null | tr -d ' ')
    low_cpu=$(ps -p $LOW_PID -o %cpu --no-headers 2>/dev/null | tr -d ' ')
    echo "  Second $i: normal=${normal_cpu}%  low_priority=${low_cpu}%"
done

wait
echo ""
echo "Expected: normal process gets ~3x more CPU than nice+10 process"
echo "Weight ratio: 1024 / 110 = 9.3x (at full contention)"
```

---

## 15. Interview Q&A

**Q1: Explain the Linux CFS scheduler. How is it different from Round Robin?**

Round Robin assigns a fixed time quantum to each thread and cycles through all runnable threads in order. Every thread gets exactly equal CPU time per cycle, regardless of priority. CFS — the Completely Fair Scheduler, introduced in Linux 2.6.23 — achieves fairness through a different mechanism: tracking accumulated CPU time per thread as "virtual runtime" and always scheduling the thread that has received the least CPU time relative to its fair share.

The key difference is how priority affects CPU allocation. In Round Robin, priority is handled by giving high-priority threads more time slices per cycle — a complex, layered mechanism. In CFS, priority is handled by controlling how quickly vruntime accumulates: a high-priority thread (nice -10) accumulates vruntime at only ~10% of the rate of a normal thread. This means a high-priority thread's vruntime stays near the minimum — it is always selected next by the scheduler. The time-slice calculation is also proportional: in a window of `target_latency` milliseconds, each thread gets a fraction of that window proportional to its weight.

The run queue in CFS is a red-black tree sorted by vruntime. The scheduler always picks the leftmost node (minimum vruntime) — O(1) lookup because CFS caches the leftmost pointer. Insertions and deletions (when threads yield, sleep, or wake up) are O(log N). With 1000 runnable threads, that is roughly 10 tree operations per scheduling event — negligible overhead.

**Q2: What is a "nice" value and how does it interact with CPU scheduling?**

The nice value is a user-space control for CFS scheduling weight — an integer from -20 (highest priority) to +19 (lowest priority), defaulting to 0. It is called "nice" because a process with a positive nice value voluntarily gives up CPU time to others.

CFS maps each nice value to a weight using a table where adjacent nice values differ by approximately 1.25×. Nice 0 maps to weight 1024 (the default). Nice -5 maps to weight 3121; nice +5 maps to weight 335. In a system with one nice-0 thread and one nice-5 thread, their CPU shares would be 1024/(1024+335) = 76% and 335/(1024+335) = 24% respectively — the nice-0 thread gets about 3× more CPU.

Negative nice values require root privileges (or the `CAP_SYS_NICE` capability). Setting a process to nice -10 without root is not possible — it would allow unprivileged users to monopolize CPU resources. For data engineering: a latency-sensitive Kafka consumer can be given nice -5 (requires root or a setuid wrapper), while a batch ETL job can be given nice +10 to ensure it never interferes with more critical processes. The weight ratio between nice -5 and nice +10 is 3121/110 ≈ 28× — the consumer gets 28× more CPU weight than the ETL job when they compete.

**Q3: What is CPU steal time and why does it matter for Spark performance on EC2?**

CPU steal time is the fraction of time a virtual machine wanted to run but was denied CPU time by the hypervisor because the physical CPU was being used by another virtual machine. It is visible as `st` in `top` and `vmstat`. Steal time is zero on dedicated hardware (bare metal, dedicated instances) and non-zero on shared multi-tenant cloud instances.

For Spark on EC2, steal time directly reduces effective CPU throughput. A Spark task designed to run for 60 seconds on a 4-vCPU instance will take 60/(1 - steal_fraction) seconds if there is steal. At 20% steal time, a 60-second task takes 75 seconds — a 25% degradation that appears as task slowness with no apparent cause in the Spark logs or JVM profiling.

Steal time is invisible to the JVM and to Spark — the Spark UI shows tasks as "running" for the full wall-clock time including steal periods. The only place steal time is visible is in the OS metrics (`top`, `vmstat`, CloudWatch `CPUSurplusCreditBalance` for T-series instances). For Spark workloads requiring consistent performance, compute-optimized instance types (c5, c6i on AWS) with dedicated CPU performance are preferred over general-purpose types (m5) which may share CPU more aggressively.

**Q4: Explain cgroup CPU throttling in Kubernetes and how it affects Spark executors.**

Kubernetes translates CPU limits into Linux cgroup `cpu.cfs_quota_us` / `cpu.cfs_period_us` settings. The default period is 100 ms; the quota is `cpu_limit × period`. A pod with `cpu: "2"` (2 CPU limit) gets `quota = 200 ms per 100 ms period` — it can use up to 2 CPUs worth of time every 100 ms. When the pod exhausts its quota, the cgroup scheduler stops running it until the next period — this is throttling.

For a Spark executor pod with `spark.executor.cores=4` and `cpu: "2"` (Kubernetes limit), the executor creates 4 task threads. If all 4 threads are actively computing (common during sort, hash join, or shuffle), they collectively need 4 CPU × time = 4 CPU-seconds per second. The pod's quota allows only 2 CPU-seconds per second. Result: the pod is throttled 50% of the time — every period, it runs for 200 ms then is forced to wait for the next 100 ms period. This halves task throughput.

The fix is to set `cpu limits ≥ spark.executor.cores` in the pod spec, or to remove CPU limits entirely (using only requests) and rely on Kubernetes bin-packing. Removing limits risks one pod consuming all CPU on a node, but for batch workloads on dedicated Spark node pools, this is often acceptable. For multi-tenant shared clusters, enforcing limits with proper sizing is required — `cpu: "4"` for a 4-core executor.

**Q5: What is NUMA and when does it affect Spark performance?**

NUMA (Non-Uniform Memory Access) describes the memory architecture of multi-socket servers. Each CPU socket has its own memory controller and DIMM slots. A core on socket 0 accesses socket 0's memory at ~80 ns latency, but accesses socket 1's memory by crossing the inter-socket QPI/UPI bus at ~140 ns — 75% slower. A 2-socket server with 64 cores and 256 GB of RAM has 32 cores + 128 GB per socket; memory access latency depends on whether the accessing core is on the same socket as the memory.

NUMA affects Spark during memory-bandwidth-bound phases — specifically, Tungsten shuffle sort and sort-merge joins, where large hash tables or sort buffers are accessed randomly. If an executor's JVM threads run on socket 0 but the JVM heap is allocated from socket 1's memory (because the OS's first-touch allocator placed the memory where the thread first touched it, which might be different socket), every hash table probe crosses the QPI link at 140 ns instead of 80 ns. For a sort operation scanning 10 GB of random-access data, this is a sustained 75% memory latency penalty.

The fix is NUMA binding: `numactl --cpunodebind=0 --membind=0` pins both the executor's CPU cores and its memory to socket 0. On YARN with cgroup enforcement, setting `cpuset.cpus` and `cpuset.mems` in the executor container achieves the same effect. NUMA effects are most visible on large (192+ GB) server instances where memory bandwidth is the bottleneck — on smaller machines (< 32 GB), L3 cache dominates and NUMA effects are minimal.

**Q6: Design a multi-tenant Spark cluster scheduling policy where interactive queries must finish in under 10 seconds but batch ETL jobs can use idle capacity.**

This is a two-level scheduling problem — cluster-level resource allocation and application-level task scheduling — combined with workload priority isolation.

At the cluster level (YARN FairScheduler or Spark on Kubernetes):

Configure two pools — "interactive" and "batch". The interactive pool has a `minShare` of 30% of cluster capacity, meaning it always gets at least 30% of the cluster's executors regardless of batch demand. The batch pool gets the remaining 70% when interactive is idle, but must release capacity immediately when interactive submits a job (preemption enabled: `yarn.scheduler.fair.preemption=true`, preemption timeout 5 seconds). Interactive jobs land in the interactive pool; batch ETL jobs land in the batch pool.

At the application level (within the Spark SparkContext):

Interactive queries use FAIR scheduling mode with a high-weight pool. Batch jobs use FIFO within their pool. Interactive queries are marked with `spark.scheduler.pool=interactive`; batch jobs with `spark.scheduler.pool=batch`.

At the OS level (per executor node):

Executors running interactive jobs get nice -5 (higher CFS scheduling priority). Executors running batch jobs get nice +5. When an interactive executor and a batch executor compete for the same physical core (possible during preemption transition when batch executors are being replaced), the interactive executor wins the CFS scheduling race approximately 10× more often (weight ratio: 3121/335 ≈ 9.3×).

For the 10-second SLA on interactive queries: the most important constraint is that the interactive pool has dedicated minimum capacity with preemption. With 30% minShare, a query that requires 20 executor-minutes of work can complete in 2 minutes on 10 executors — well within 10 seconds for simple queries (< 10 second of work on 1 executor). For queries that cannot complete in 10 seconds at 30% capacity, the design must either increase the interactive pool size or implement result caching (Alluxio, Redis) for frequently-executed queries.

---

## 16. Cross-Question Chain

**Q1 [Interviewer]: What does the Linux scheduler do at a high level?**

The Linux CFS scheduler decides, for each CPU core, which runnable thread executes at any given moment. It tracks how much CPU time each thread has received — its "virtual runtime" — and always schedules the thread with the lowest virtual runtime: the thread that has been most starved of CPU time relative to its fair share. This runs continuously, making a new scheduling decision approximately every 4–6 ms per core (the target latency period). The scheduler is invoked by the timer interrupt (for preemption), by system calls that cause a thread to block (I/O wait, lock acquisition), and by wake-up events (I/O completion, lock release).

**Q2 [Interviewer]: You mentioned "virtual runtime." How does it handle threads with different nice values?**

When a thread runs for T nanoseconds, its vruntime increases by `T × (DEFAULT_WEIGHT / thread_weight)`. A thread with nice -10 has weight 9548 versus the default weight of 1024. Its vruntime increases at rate `DEFAULT_WEIGHT / 9548 = 1024/9548 ≈ 0.107×` — only 10.7% as fast as a default thread. This means the nice -10 thread stays near the front of the run queue (low vruntime), gets scheduled more frequently, and accumulates actual CPU time roughly 9.3× faster than a nice 0 thread. From the red-black tree's perspective, the high-priority thread has a vruntime that grows slowly and thus stays near the left (minimum) end of the tree — it wins the scheduling competition proportionally to its weight.

**Q3 [Interviewer]: What happens when a thread wakes up after sleeping on I/O for 500 ms?**

Without adjustment, the thread's saved vruntime would be 500 ms behind all currently running threads. CFS would give this thread 500 ms of CPU time to "catch up" — effectively monopolizing the CPU and starving all other threads, which is wrong. CFS prevents this by clamping the waking thread's vruntime: it sets it to `max(saved_vruntime, min_vruntime - target_latency)`. In practice, this places the waking thread slightly behind the current minimum vruntime in the tree — it gets scheduled "soon" (within one target_latency period, typically 6 ms) but not repeatedly. This is the mechanism that makes CFS responsive for I/O-bound threads: a Kafka consumer that wakes up when a message arrives runs within 6 ms — fast enough for low-latency processing.

**Q4 [Interviewer]: A Spark job's tasks are consistently slower on one executor than others. What scheduling issues could cause this?**

Three scheduling-related causes, in order of likelihood:

First, **CPU throttling from Kubernetes limits.** If this executor runs in a Kubernetes pod with `cpu: "2"` but runs 4 task threads (Spark configured with `executor-cores=4`), the cgroup quota is exhausted when all threads are active. The pod is throttled — suspended until the next 100 ms cgroup period. Tasks that should run in 10 seconds take 20 seconds. Check: `kubectl exec <pod> -- cat /sys/fs/cgroup/cpu/cpu.stat` — high `nr_throttled` confirms this.

Second, **NUMA memory penalty.** If this executor's JVM threads run on socket 0 cores but the JVM heap was allocated from socket 1 memory (first-touch by a different socket's threads), all heap accesses cross the QPI link at 140 ns instead of 80 ns. This affects shuffle-sort and hash-join tasks most severely. Check: `numastat -p <JVM_PID>` — high `numa_miss` on one node confirms cross-socket memory access.

Third, **noisy-neighbor interference.** Another process on the same node is consuming CPU, causing involuntary context switches for Spark tasks. Check: `pidstat -u -w -p <SPARK_EXECUTOR_PID> 1` — high `nvcswch/s` confirms OS preemption. Fix: `renice -n 5 -p <COMPETING_PID>` or `taskset` to isolate to different cores.

**Q5 [Interviewer]: How do YARN and Kubernetes differ in how they enforce CPU limits for Spark?**

YARN enforces CPU through **virtual core (vcore) allocation and cgroup CPU shares** (`cpu.shares`). A vcore is YARN's abstract unit of CPU. When a container requests 4 vcores on a 32-vcore node, YARN sets `cpu.shares = (4/32) × 1024 × total_cgroup_shares` for that container. This provides **proportional fairness** — the container gets approximately 4/32 = 12.5% of the node's CPU under contention. Critically, this is a soft limit: if no other containers want CPU, this container can use 100% of the available CPU. YARN optionally supports hard limits via `cpu.cfs_quota_us` (`yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage=true`), but many clusters leave this disabled.

Kubernetes enforces CPU through **cgroup quota** by default. `cpu limits: "4"` translates to `cpu.cfs_quota_us = 400000` per 100 ms period — a hard limit. If the container exceeds 4 CPUs of work in 100 ms, it is throttled until the next period. This is a ceiling, not a proportional share. Even if the node has 100% idle CPU, a pod with `cpu limit: "4"` cannot use more than 4 CPUs. Kubernetes `requests` use cpu.shares for proportional scheduling during contention; `limits` enforce the absolute ceiling via quota.

For Spark: YARN's soft limits are friendlier to bursty workloads (GC pauses, task skew) that occasionally need more than their allocated CPU. Kubernetes's hard limits ensure predictable resource isolation but can throttle Spark executors during bursts — requiring careful right-sizing of limits or removing limits for Spark workloads.

**Q6 [Interviewer]: A Kafka consumer's processing latency spikes from 2 ms to 100 ms every hour, correlated with a scheduled batch ETL job. Walk me through diagnosing and fixing this end-to-end.**

**Diagnosis, step by step:**

First, confirm correlation: overlay the Kafka consumer lag graph with the ETL job's execution window. If lag spikes align with ETL job start times, the correlation is confirmed.

Second, identify the mechanism. Run `pidstat -u -w -p $(pgrep -f kafka_consumer) 1` during an ETL spike. If `nvcswch/s` spikes from 0 to 50+ when ETL starts, the consumer is being involuntarily preempted by the OS — the ETL job's threads are winning scheduling contention. If `nvcswch/s` stays near 0 but `%CPU` drops, the consumer is not being preempted — instead, it may be blocked on I/O or a lock held by the ETL job.

Third, check if they share cores: `taskset -cp $(pgrep -f kafka_consumer)` and `taskset -cp $(pgrep -f etl_batch)` — if both show the same core range, they compete directly.

**Fix, in order of invasiveness:**

Option 1 (quick, no root needed): Increase the ETL job's nice value to deprioritize it — `renice -n 10 -p $(pgrep -f etl_batch)`. This requires only the user's own process, so no root is needed for positive nice values. The consumer gets ~9× more CPU weight than the ETL job during contention.

Option 2 (better, requires root): Give the consumer higher priority — `renice -n -5 -p $(pgrep -f kafka_consumer)` — AND lower ETL priority — `renice -n +10 -p $(pgrep -f etl_batch)`. The weight ratio becomes 3121:110 = 28×, ensuring the consumer is never starved.

Option 3 (best, requires planning): Pin the consumer to dedicated CPU cores — `taskset -c 0,1 python kafka_consumer.py` — and pin the ETL job to different cores — `taskset -c 2-31 python etl_batch.py`. Physical isolation means no CFS contention; the consumer runs on its own cores and is never preempted by ETL threads. In Kubernetes, this is `resources.limits.cpu = "2"` with `cpusetPolicy: static` in the Kubelet configuration.

Option 4 (architectural): Move the consumer and ETL to separate machines (or separate Kubernetes nodes with taints/tolerations). Hard isolation eliminates all CPU contention at the cost of more infrastructure.

Monitor post-fix: consumer's `nvcswch/s` should drop back to near 0 during ETL runs, and the latency spikes should disappear from the consumer metrics dashboard.

---

## 17. Common Misconceptions

**"Setting `spark.executor.cores=16` on a 16-core machine maximizes throughput."**
A Spark executor with 16 task threads on a 16-core machine may seem like perfect utilization. But Spark tasks also perform JVM GC (stop-the-world: all 16 threads pause simultaneously), network I/O for shuffle (threads block, releasing CPU), and JVM thread stack operations. The optimal `spark.executor.cores` is typically 4–5, not 16. With 16 task threads on 16 cores, one GC pause stops all 16 tasks at once; with 4 executors × 4 cores, GC pauses are staggered and other executors continue running.

**"CPU throttling is always visible as high CPU usage."**
CPU throttling is the opposite: the process wants to use CPU but is prevented. `top` will show the container using exactly its limit (e.g., 200% CPU for a 2-CPU limit) but tasks are slower than expected because the container is suspended for the fraction of each period when quota is exhausted. A throttled container shows normal or low CPU usage (the throttle periods count as "not running") — it is invisible without checking `cpu.stat` in the cgroup.

**"More CPU cores always mean better Spark performance."**
More cores help only when the bottleneck is CPU. A shuffle-heavy job that is network-bound (waiting for shuffle data to transfer between executors) runs at the same speed with 16 cores or 64 cores — all threads are blocked on network I/O. More cores help CPU-bound phases (sorting, hashing), make no difference for I/O-bound phases, and can hurt memory-bound phases (more threads access the same memory bus → bandwidth saturation).

**"CFS is preemptive — it interrupts running threads at any time."**
CFS is preemptive but not arbitrary. A running thread is only preempted when: (a) its time slice expires (set by the timer interrupt, typically every 4 ms), or (b) a higher-vruntime thread wakes up that has significantly lower vruntime than the current thread. CFS does not interrupt threads in the middle of system calls or kernel sections — a thread performing a long `write()` system call runs uninterrupted in kernel mode until the call completes. Only user-mode execution is preempted by the timer interrupt.

**"Nice values affect disk I/O priority as well as CPU priority."**
Nice values control only CPU scheduling priority in CFS. They have no direct effect on I/O priority. Linux has a separate I/O scheduler (CFQ, deadline, BFQ) with its own priority system (`ionice`). A process with nice +19 (lowest CPU priority) but `ionice` class 1 (real-time I/O) will get CPU slowly but disk I/O quickly. To deprioritize both CPU and I/O: `nice -n 19 ionice -c 3 python batch_etl.py` (idle-class I/O + lowest CPU priority).

---

## 18. Performance Reference Card

| Metric | Tool | What to Look For |
|---|---|---|
| CPU utilization | `top`, `htop` | `us` + `sy` > 80% sustained = CPU-bound |
| Steal time | `top` (`st` column) | > 5% = VM oversubscription; upgrade instance |
| I/O wait | `top` (`wa` column) | > 20% = I/O bound; check disk/network |
| Context switches | `vmstat 1` (`cs` column) | > 100,000/s = excessive thread switching |
| Involuntary switches | `pidstat -w -p <PID> 1` | High `nvcswch/s` = CPU-hungry, being preempted |
| Voluntary switches | `pidstat -w -p <PID> 1` | High `cswch/s` = I/O-bound, waiting frequently |
| CPU throttling | `cat /sys/fs/cgroup/cpu/cpu.stat` | `nr_throttled / nr_periods > 5%` |
| NUMA efficiency | `numastat -n` | `numa_miss / (numa_hit + numa_miss) > 10%` |
| Load average | `uptime` | > number of CPUs = overloaded |
| Scheduling latency | `/proc/<pid>/sched` (`wait_sum`) | Growing wait_sum/nr_waits = high latency |

**Nice value CPU weight ratios (key values):**

| Comparison | Weight ratio | Practical effect |
|---|---|---|
| nice -20 vs nice 0 | 88761:1024 = 87× | -20 process dominates completely |
| nice -5 vs nice 0 | 3121:1024 = 3× | -5 process gets 3× more CPU |
| nice 0 vs nice +5 | 1024:335 = 3× | +5 process gets 1/3 the CPU |
| nice -5 vs nice +10 | 3121:110 = 28× | Kafka consumer vs batch ETL |
| nice -20 vs nice +19 | 88761:15 = 5917× | Real-time vs background |

**cgroup CPU quota math:**

```
CPU limit in CPUs × period_us = quota_us
4 CPUs × 100,000 µs period = 400,000 µs quota

Throttle fraction = max(0, actual_cpu_demand - quota) / period
If 4 threads each want 100% CPU = 400% demand
Quota = 200% (cpu limit 2)
Throttle = (400% - 200%) / 400% = 50% of the time throttled
Task slowdown = 1 / (1 - 0.5) = 2× slower
```

---

## 19. Connections to Other Modules

**CSF-OS-101 M01 (Processes and Threads):** M01 defines what the scheduler operates on (threads). M02 explains how the scheduler decides which thread runs. The thread state diagram in M01 (READY → RUNNING → SLEEPING) corresponds directly to the CFS transitions: READY = in the red-black tree; RUNNING = dequeued, on a CPU; SLEEPING = not in the tree, waiting for an event.

**CSF-ALG-101 M03 (Trees):** CFS's run queue is a red-black tree — a self-balancing BST. The O(1) leftmost-node access (min vruntime) is the scheduling hot path. The O(log N) insert/delete operations run when threads are enqueued or dequeued. The MLFQ scheduler uses a priority queue (heap) — covered in M03's priority queue section.

**CSF-OS-101 M03 (Virtual Memory):** CFS context switches require saving/restoring register state and flushing the TLB (for process switches). The TLB flush cost is what makes process context switches more expensive than thread switches — M03 explains the TLB and virtual memory mapping in detail.

**DE-ORC-101 (Airflow):** Airflow's KubernetesExecutor creates pods with CPU requests and limits. Understanding cgroup throttling from this module is essential for diagnosing why Airflow tasks are slower on Kubernetes than on bare metal.

**DCS-SPK-101 (Spark Architecture):** YARN's CPU scheduling (CapacityScheduler, FairScheduler) interacts with the OS-level CFS scheduler. NUMA-aware executor placement for Spark. Executor `cores` sizing relative to CPU limits. CPU throttling of Spark containers. All require the scheduler fundamentals from this module.

**CPL-CLD-101 (Cloud Platforms):** CPU steal time on EC2, Azure, GCP. Instance type selection (compute-optimized vs general-purpose for Spark). Spot/preemptible instance scheduling preemption. Kubernetes CPU requests/limits on cloud Kubernetes services (EKS, GKE, AKS).

---

## 20. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What does CFS stand for and what is its core goal? | Completely Fair Scheduler. Goal: give every runnable thread CPU time proportional to its weight (nice-value-derived), so that over any time window, all threads receive their fair share. |
| 2 | What is "virtual runtime" (vruntime) in CFS? | The amount of CPU time a thread has received, normalized by its weight. vruntime += actual_cpu_time × (DEFAULT_WEIGHT / thread_weight). CFS always schedules the thread with the lowest vruntime. |
| 3 | What data structure does CFS use for its run queue? | A red-black tree (self-balancing BST) keyed by vruntime. O(1) to find the minimum (leftmost node, cached). O(log N) to insert/delete. |
| 4 | What is a "nice" value and what range does it cover? | A user-space scheduling priority hint: -20 (highest CPU priority) to +19 (lowest). Default is 0. Negative values require root. Maps to CFS weights where adjacent values differ by ~1.25×. |
| 5 | What is CPU steal time? | On virtualized machines: the fraction of time the VM wanted to run but the hypervisor gave the CPU to another VM. Visible as `st` in top. >5% indicates host oversubscription. |
| 6 | What is a cgroup CPU quota? | `cpu.cfs_quota_us` / `cpu.cfs_period_us` = maximum CPU fraction per period. Exhausting quota causes the process to be throttled (suspended) until the next period. Kubernetes CPU limits set this. |
| 7 | What is the "convoy effect" in FCFS scheduling? | A long-running task blocks all shorter tasks waiting behind it in the queue. Results in high average wait time for short tasks. Example: a 10s ETL task blocks a 50ms consumer task. |
| 8 | Why is SJF optimal but impractical? | SJF minimizes average wait time (provably optimal). Impractical because the scheduler cannot know future burst times — they must be estimated, and estimates are often wrong. |
| 9 | What is MLFQ and why does it approximate SJF? | Multi-Level Feedback Queue: multiple priority queues with different time quantums. New threads start at highest priority. CPU-hungry threads are demoted. Short/interactive threads stay at high priority — approximating SJF without knowing burst times. |
| 10 | How does CFS handle a thread waking up after a long sleep? | Sets waking thread's vruntime to max(saved_vruntime, min_vruntime_in_tree − target_latency). This places it near the front (it runs soon) but not so far back that it monopolizes CPU to "catch up." |
| 11 | What is NUMA and why does it affect Spark? | Non-Uniform Memory Access: cross-socket memory access is ~75% slower than local. Spark shuffle sort and joins are memory-bandwidth bound. Executors with threads on socket 0 but heap on socket 1 pay a 75% memory latency penalty. |
| 12 | What is `taskset` used for? | Setting CPU affinity — pinning a process or thread to specific CPU cores. Prevents OS from migrating the process (maintains cache warmth). `taskset -c 0-3 python app.py` pins to cores 0–3. |
| 13 | What is the difference between CPU `requests` and `limits` in Kubernetes? | `requests`: sets cgroup `cpu.shares` (proportional scheduling under contention). `limits`: sets cgroup `cpu.cfs_quota_us` (hard ceiling; throttles when exceeded). A pod that exceeds its limit is throttled, not killed. |
| 14 | What does high `nvcswch/s` in pidstat indicate? | High involuntary context switches — the process is being preempted by the OS because it wants more CPU than it currently gets. Indicates CPU competition (noisy neighbor or underprovisioned CPU). |
| 15 | What does high `cswch/s` in pidstat indicate? | High voluntary context switches — the process frequently yields CPU (blocking on I/O, sleep, lock wait). Indicates an I/O-bound process spending most time waiting, not computing. |
| 16 | What is priority inversion? | A high-priority thread waits for a resource held by a low-priority thread, which is preempted by a medium-priority thread. The high-priority thread effectively waits for the low-priority thread. Fixed by priority inheritance (temporary boost of lock-holder). |
| 17 | What command changes the nice value of a running process? | `renice -n <nice_value> -p <PID>`. Positive values (lowering priority) work without root. Negative values (raising priority) require root or `CAP_SYS_NICE`. |
| 18 | What is SCHED_BATCH and when should you use it? | A Linux scheduling policy for CPU-intensive batch work. Hints to the scheduler: "this process is not interactive, give large time slices." Reduces context switches and improves throughput at the cost of higher latency. `chrt -b 0 python etl.py` |
| 19 | How does the YARN FairScheduler differ from the CapacityScheduler? | FairScheduler: all applications share cluster equally; new apps get resources immediately (dynamic rebalancing). CapacityScheduler: partitioned queues with guaranteed minimums; within-queue FIFO. FairScheduler better for ad-hoc mixed workloads; CapacityScheduler better for defined SLA queues. |
| 20 | What is `numactl --membind` and why use it for Spark? | Pins a process's memory allocations to a specific NUMA node. Ensures Spark executor heap is on the same socket as its CPU cores. Eliminates cross-socket memory accesses (~75% slower than local). `numactl --cpunodebind=0 --membind=0 spark-class Executor ...` |

---

## 21. Module Summary

The Linux CFS scheduler tracks **virtual runtime** per thread — accumulated CPU time weighted by priority. It always schedules the thread with the lowest vruntime (stored in a red-black tree, O(1) to find minimum, O(log N) to update). Threads with higher weight (lower nice value) accumulate vruntime more slowly and thus get CPU time more frequently. Threads waking from I/O sleep get their vruntime set near the current minimum, ensuring low-latency scheduling without unfair CPU monopolization.

**Nice values** (−20 to +19) control CFS weights. Adjacent values differ by ~1.25×; nice −5 vs nice +10 is a 28× weight ratio. Setting Kafka consumers to nice −5 and batch ETL to nice +10 ensures consumers are never starved during ETL runs. Negative nice values require root.

**cgroups** extend scheduling to containers. `cpu.cfs_quota_us` enforces hard CPU limits in Kubernetes — exceeding the quota causes **throttling** (forced sleep until the next period). A Spark executor with 4 cores but a `cpu: "2"` Kubernetes limit is throttled 50% of the time when all 4 threads are active. Fix: set limits ≥ executor cores, or remove limits for dedicated Spark node pools.

**CPU affinity** (`taskset`, `os.sched_setaffinity()`) pins threads to specific cores, preventing migration and maintaining L1/L2 cache warmth. **NUMA binding** (`numactl`) ensures memory is allocated from the same socket as the CPU cores, eliminating the 75% latency penalty of cross-socket memory access.

In data engineering: CPU steal time (`st` in top) silently degrades Spark task throughput on oversubscribed cloud VMs. CFS involuntary context switches (`nvcswch/s` in pidstat) reveal noisy-neighbor interference. Kubernetes CPU throttling (checked via `cpu.stat`) is the most common hidden performance problem in containerized Spark. YARN's FairScheduler at the cluster level and Linux CFS at the node level together implement multi-level fair scheduling for multi-tenant Spark clusters.

---

**CSF-OS-101: 2 of 5 complete.**  
**Next: CSF-OS-101 M03 — Virtual Memory and Page Cache**  
*(virtual address space, page tables, TLB, page faults, the OS page cache, memory-mapped I/O — and why Spark's Tungsten bypasses the JVM heap)*
