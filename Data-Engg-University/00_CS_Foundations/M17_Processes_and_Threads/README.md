# CSF-OS-101 M01: Processes and Threads

**Course:** CSF-OS-101 — Operating System Internals for Data Engineers  
**Module:** 01 of 05  
**Filesystem position:** 00_CS_Foundations/M17_Processes_and_Threads  
**Prerequisites:** CSF-ALG-101 (all 5 modules)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: Processes](#4-core-theory-processes)
5. [Core Theory: Threads](#5-core-theory-threads)
6. [Visual: Process and Thread Memory Layout](#6-visual-process-and-thread-memory-layout)
7. [Deep Dive: The Python GIL](#7-deep-dive-the-python-gil)
8. [Deep Dive: Context Switching](#8-deep-dive-context-switching)
9. [Concurrency Models in Data Engineering](#9-concurrency-models-in-data-engineering)
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

A PySpark job runs on a cluster with 32 CPU cores. You write Python UDFs that transform data. The job is dramatically slower than equivalent Scala UDFs. You've heard it's "the GIL" but don't know why, or what to do about it.

A Spark executor process runs out of memory. The JVM heap is not even at capacity — it's the off-heap memory used for Tungsten shuffle buffers that's exhausted. You don't understand why a JVM process has memory outside its heap, or what controls it.

An Airflow worker runs 8 DAGs simultaneously. Each DAG has tasks. The worker is a single Python process. How are 8 DAGs running at the same time? Are they threads, processes, or something else? What happens when one hangs — does it block the others?

A Kafka consumer processes 1 million messages per second. It's I/O-bound: reading from the broker socket, deserializing bytes, and writing to a database. Would you use threads or processes to scale it? Why? What are the correctness risks?

All of these questions require a precise mental model of what a process is, what a thread is, how they share (or don't share) memory, what the Global Interpreter Lock is and why Python has it, how the OS switches between threads, and how different data engineering tools choose between processes and threads for concurrency. This module builds that foundation.

---

## 2. How to Use This Module

**For interview prep:** Sections 4, 5, 7, 15, 16. The GIL is a canonical Python interview topic for senior/staff data engineering roles. Sections 15 and 16 have six detailed Q&As and a six-exchange escalating chain covering process vs thread, the GIL, and Spark's concurrency model.

**For debugging production:** Sections 8, 11, 12. Context switching costs explain why 1000 Python threads don't give 1000× speedup. Section 11 covers real failure patterns: GIL-induced serialization of CPU work, zombie processes in Airflow, shared-memory corruption in multiprocessing forks.

**For system design:** Sections 9, 12. Section 9 maps the problem space (CPU-bound vs I/O-bound vs both) to the right concurrency model. Section 12 shows how Spark, Airflow, Dask, and Kafka each make this choice and why.

---

## 3. Prerequisites Check

- **Big-O complexity** (CSF-ALG-101 M01): Thread contention and lock overhead are analyzed in terms of number of threads and contention rate. A GIL-bound program with N Python threads degrades to near-O(1) parallelism — not O(N). You need to reason about why.
- **Hash tables** (CSF-ALG-101 M02): The OS process table is a hash table mapping PIDs to process control blocks. The kernel's thread scheduler uses priority queues (a heap). Both are data structures you've already studied.
- **Basic operating systems intuition:** You should know that programs run on CPUs and that modern computers have multiple CPU cores. Everything else in this module is built from first principles.

---

## 4. Core Theory: Processes

### 4.1 What Is a Process?

A **process** is an instance of a running program. It is the operating system's unit of isolation. When you run `python etl.py`, the OS creates a process with:

1. **Its own private virtual address space** — the process believes it has the entire address space to itself (0 to 2⁶⁴ on 64-bit systems). No other process's memory is visible at those addresses.
2. **Its own file descriptor table** — open files, sockets, pipes.
3. **Its own page table** — the OS-maintained mapping from the process's virtual addresses to physical RAM.
4. **CPU state** — register values, instruction pointer, stack pointer.
5. **Kernel resources** — signal handlers, timers, resource limits.

The OS identifies each process by a **PID (Process Identifier)** — a positive integer assigned at creation. `os.getpid()` in Python returns the current process's PID.

### 4.2 Process Creation: `fork()` and `exec()`

On Linux/macOS, processes are created via two system calls:

**`fork()`** — creates an exact copy (child process) of the calling process (parent):
- Child gets its own address space — a copy of the parent's memory
- Child inherits a copy of the parent's file descriptors
- Both parent and child continue execution from the line after `fork()`
- `fork()` returns 0 in the child, the child's PID in the parent

```c
pid_t pid = fork();
if (pid == 0) {
    // this code runs in the CHILD process
} else {
    // this code runs in the PARENT process
    // pid = the child's PID
}
```

**Copy-on-write (COW):** Modern OSes don't actually copy all the parent's memory at `fork()` time. The child's page table initially points to the same physical pages as the parent, but those pages are marked read-only. When either process writes to a shared page, the OS catches the write fault, copies that page, and updates the writer's page table to point to the new copy. Pages never written are never copied — `fork()` of a 10 GB process can be nearly instantaneous.

**`exec()`** — replaces the current process's address space with a new program. After `exec()`, the calling process's memory, code, and stack are replaced by the new program. The PID stays the same. The combination `fork() + exec()` is how shells launch programs: fork a child, then exec the desired program inside the child.

**Python's `multiprocessing` module** uses `fork()` (on Linux) or `spawn` (on macOS ≥ 3.8, Windows) to create new Python processes. The difference matters: `fork()` copies the parent's entire state (including open database connections, which may cause correctness issues); `spawn` starts a fresh Python interpreter and re-imports the module.

### 4.3 Process Control Block (PCB)

The kernel maintains a **Process Control Block** for every process — a kernel data structure (a struct in C) that stores all information about the process:

```
Process Control Block (Linux: task_struct):
  pid:            Process ID
  ppid:           Parent PID
  state:          RUNNING, SLEEPING, ZOMBIE, STOPPED...
  mm:             Pointer to memory descriptor (page tables)
  files:          File descriptor table
  signal:         Signal handler table
  thread_info:    CPU registers (saved when not running)
  priority:       Scheduling priority
  cpu:            Which CPU core is currently running this process
  start_time:     When process was created
  utime/stime:    CPU time in user/kernel mode
```

The kernel stores all PCBs in a hash table (keyed by PID) and a doubly-linked list (for scheduler traversal). `ps aux` and `top` read this information from `/proc/<pid>/` — a virtual filesystem that exposes kernel data structures as files.

### 4.4 Process States

A process is not always running. The OS transitions processes through states:

```
          fork()
            │
            ▼
        ┌──────────┐    scheduled      ┌──────────┐
        │  READY   │ ──────────────→  │ RUNNING  │
        │ (in run  │ ←──────────────  │ (on CPU) │
        │  queue)  │    preempted      └────┬─────┘
        └──────────┘                        │
                                            │ I/O request /
                                            │ sleep() / wait()
                                            ▼
                                    ┌──────────────┐
                                    │   SLEEPING   │
                                    │  (blocked on │
                                    │   I/O/event) │
                                    └──────┬───────┘
                                           │ I/O complete /
                                           │ event fires
                                           ▼
                                       READY again

exit():
  RUNNING → ZOMBIE (PCB kept until parent reads exit code via wait())
  Parent calls wait() → ZOMBIE → DEAD (PCB freed)
```

**Key insight for data engineering:** A Spark executor spending 90% of its time in SLEEPING state (waiting for network I/O from the shuffle service) is getting very little CPU work done, even if the CPU appears busy at the OS level (other processes are running). `vmstat` and `iostat` distinguish CPU time by state: `us` (user), `sy` (kernel/system), `wa` (I/O wait). High `wa` means processes are sleeping on I/O — adding more CPU cores won't help.

### 4.5 Inter-Process Communication (IPC)

Since processes have isolated address spaces, sharing data requires explicit IPC mechanisms:

| Mechanism | Description | Speed | Use Case |
|---|---|---|---|
| Pipes | One-directional byte stream | Fast (kernel buffer) | Parent → child data passing |
| Shared memory | Mapped physical page appears in both address spaces | Fastest (zero-copy) | Python multiprocessing `Value`/`Array` |
| Sockets | Network-style communication (even on same host) | Moderate | Spark shuffle, Kafka |
| Files | Write to file, other process reads | Slow | Airflow XCom (for small values) |
| Signals | Asynchronous notification | Instant (no data) | SIGTERM for graceful shutdown |
| Message queues | Kernel-managed message passing | Moderate | Celery task queue |

Python's `multiprocessing.Queue` uses a pipe under the hood with pickle serialization. Every object sent between processes is serialized to bytes and deserialized on the other side — this serialization cost is why sharing large DataFrames between processes is expensive.

---

## 5. Core Theory: Threads

### 5.1 What Is a Thread?

A **thread** is an independent sequence of execution within a process. All threads in a process share:
- The same virtual address space (same code, same heap, same global variables)
- The same file descriptors
- The same process ID

Each thread has its own:
- **Stack** — local variables, function call frames (typically 8 MB per thread on Linux, configurable)
- **CPU registers** — instruction pointer, stack pointer, general-purpose registers
- **Thread ID (TID)** — different from the PID
- **Signal mask** — which signals this thread handles

Because threads share the heap, one thread can directly read and write data created by another thread. This is both a feature (no serialization overhead) and the source of most concurrency bugs (data races, deadlocks).

### 5.2 Thread Creation

In Python, threads are created via the `threading` module:

```python
import threading

def worker(task_id: int) -> None:
    print(f"Thread {task_id} started")
    # ... do work ...

threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
for t in threads:
    t.start()     # OS creates a new thread, begins executing worker()
for t in threads:
    t.join()      # wait for all threads to complete
```

At the OS level, Python calls `pthread_create()` (POSIX threads) or `CreateThread()` (Windows). The OS schedules threads — not Python. The OS sees threads, not Python green threads or coroutines.

### 5.3 Thread vs Process: The Fundamental Tradeoff

| Property | Process | Thread |
|---|---|---|
| Address space | Isolated (private) | Shared (all threads in same process) |
| Creation cost | High (~1–10 ms, fork + COW setup) | Low (~50–100 µs) |
| Communication | IPC (pipes, sockets, shared memory) | Direct (shared heap), fast but unsafe |
| Crash isolation | One process crash doesn't affect others | One thread crash kills the entire process |
| Parallelism (CPython) | True parallel on multiple cores | Limited by GIL (see Section 7) |
| Memory overhead | High (separate address space) | Low (shared address space) |
| Use when | CPU-bound parallel work in Python; crash isolation needed | I/O-bound concurrent work; low latency data sharing |

### 5.4 The Thread Safety Problem

When two threads access the same memory without coordination, the result is undefined. Consider a simple counter:

```python
counter = 0

def increment():
    global counter
    for _ in range(1_000_000):
        counter += 1   # NOT ATOMIC — this is 3 machine instructions

t1 = threading.Thread(target=increment)
t2 = threading.Thread(target=increment)
t1.start(); t2.start()
t1.join();  t2.join()
print(counter)   # Expected: 2,000,000. Actual: < 2,000,000 (data race)
```

`counter += 1` compiles to three machine instructions: `LOAD_GLOBAL counter`, `BINARY_ADD 1`, `STORE_GLOBAL counter`. If two threads interleave between LOAD and STORE, both read the same value, both add 1, and both write back the same result — one increment is lost. This is a **data race**.

Solutions:
- **Lock (Mutex):** `threading.Lock()` — only one thread holds the lock at a time. While holding, it can safely read-modify-write. Others block (sleep) until the lock is released.
- **Atomic operations:** Some operations are atomically guaranteed at the hardware level (test-and-set). Python's `collections.Counter`, Kafka consumer offset commits, and database sequence reads rely on atomic primitives.
- **Thread-local storage:** `threading.local()` gives each thread its own copy of a variable — no sharing, no race.

### 5.5 Synchronization Primitives

```python
import threading

# Lock (Mutex): exclusive access to a critical section
lock = threading.Lock()
with lock:          # acquires on enter, releases on exit (even on exception)
    counter += 1

# RLock (Reentrant Lock): same thread can acquire multiple times
rlock = threading.RLock()

# Semaphore: allows up to N threads simultaneously
semaphore = threading.Semaphore(4)   # max 4 threads in the critical section at once
with semaphore:
    # at most 4 threads here at a time
    do_limited_resource_work()

# Event: thread A signals thread B that something happened
event = threading.Event()
# Thread A:
event.set()    # signal — all threads waiting on event.wait() unblock
# Thread B:
event.wait()   # block until event.set() is called

# Condition: combination of lock + event for producer-consumer patterns
condition = threading.Condition()
with condition:
    while not data_available:
        condition.wait()    # release lock, sleep until notified
    process_data()

# Producer:
with condition:
    data_available = True
    condition.notify_all()  # wake all waiting threads
```

---

## 6. Visual: Process and Thread Memory Layout

### 6.1 Single-Process Memory Layout

```
Virtual Address Space of a Process (Linux x86-64, simplified):

High addresses (0xFFFFFFFFFFFFFFFF)
┌─────────────────────────────────┐
│        Kernel Space             │  ← OS kernel; process cannot access directly
│   (mapped into every process,   │    Only accessible via system calls
│    but protected)               │
├─────────────────────────────────┤  0x00007FFFFFFFFFFF
│         Stack                   │  ← grows downward
│  local variables, call frames   │    Each function call: push frame
│  ...                            │    Function return: pop frame
│  ↓  (grows toward lower addrs)  │
├─────────────────────────────────┤
│           ↕ gap                 │  ← unmapped; stack overflow → segfault
├─────────────────────────────────┤
│   Memory-Mapped Region          │  ← mmap'd files, shared libraries (.so),
│   (shared libs, mmap files)     │    shared memory segments
├─────────────────────────────────┤
│           Heap                  │  ← grows upward
│  (malloc / Python objects)      │    Python allocator manages this
│  ↑  (grows toward higher addrs) │
├─────────────────────────────────┤
│         BSS Segment             │  ← uninitialized global/static variables
│ (uninitialized globals)         │    (zero-initialized at load time)
├─────────────────────────────────┤
│         Data Segment            │  ← initialized global/static variables
│ (initialized globals)           │    e.g., module-level constants
├─────────────────────────────────┤
│         Text Segment            │  ← compiled program code (read-only)
│  (executable code)              │    Python: bytecode + C extension code
└─────────────────────────────────┘
Low addresses (0x0000000000000000)
```

### 6.2 Multi-Thread Memory Layout (Threads Share One Address Space)

```
One Process with 3 Threads:

Virtual Address Space (shared by all 3 threads):
┌──────────────────────────────────────────────────────┐
│                    Kernel Space                      │
├──────────────────────────────────────────────────────┤
│  Stack (Thread 3)   │  Stack (Thread 2)  │  Stack T1 │
│   8 MB reserved     │    8 MB reserved   │  8 MB     │
│   per thread        │                    │           │
├──────────────────────────────────────────────────────┤
│                  Memory-Mapped Region                │
│              (shared libraries, mmap)                │
├──────────────────────────────────────────────────────┤
│                                                      │
│                    HEAP                              │
│       ALL THREE THREADS READ/WRITE HERE              │
│       Python objects, dictionaries, lists,           │
│       Pandas DataFrames — ALL SHARED MEMORY          │
│                                                      │
├──────────────────────────────────────────────────────┤
│                Data + BSS + Text                     │
│            (shared: same code, globals)              │
└──────────────────────────────────────────────────────┘

Key: The heap is shared → threads can pass objects without serialization
     But: concurrent writes to heap without locks → data race
```

### 6.3 Two Processes — No Shared Memory (Without Explicit IPC)

```
Process A                          Process B
Virtual Address Space A            Virtual Address Space B
┌──────────────────┐               ┌──────────────────┐
│    Stack A       │               │    Stack B       │
├──────────────────┤               ├──────────────────┤
│    Heap A        │               │    Heap B        │
│  my_df = ...     │  ← different  │  my_df = ...     │
│  (own copy)      │    physical   │  (own copy)      │
│                  │    memory     │                  │
├──────────────────┤               ├──────────────────┤
│    Text A        │               │    Text B        │
└──────────────────┘               └──────────────────┘
         │                                  │
         │    Shared Physical Pages         │
         │   (via mmap / shared memory)     │
         └──────────────┬───────────────────┘
                        │
              ┌─────────────────┐
              │  Physical RAM   │
              │  Page Frame N   │  ← A's virtual 0x7f... maps here
              │  Page Frame M   │  ← B's virtual 0x7f... maps here (different page)
              └─────────────────┘
```

---

## 7. Deep Dive: The Python GIL

### 7.1 What the GIL Is

The **Global Interpreter Lock (GIL)** is a mutex in CPython (the standard Python implementation) that ensures only one thread executes Python bytecode at any given moment. It is a single mutex protecting the entire Python interpreter state.

CPython's memory manager is not thread-safe. Python objects (lists, dicts, integers, strings) are reference-counted: every object has a `ob_refcnt` field that tracks how many references point to it. When `ob_refcnt` drops to zero, the object is deallocated. If two threads simultaneously increment or decrement the same object's reference count, the count becomes corrupted (this is a data race on the reference count). Instead of making every reference count operation atomic (which would be expensive — every assignment, every function call, every loop iteration mutates reference counts), Guido van Rossum chose a single global lock: the GIL.

### 7.2 How the GIL Works

The GIL is not held indefinitely by a single thread. CPython releases it periodically to allow other threads to run:

**CPython 3.2+:** The GIL uses a "request + timeout" model. A thread holds the GIL until it either:
1. Voluntarily releases it (e.g., when making a blocking system call: file I/O, socket read, `time.sleep()`)
2. Is interrupted after `sys.getswitchinterval()` seconds (default: 5 ms) — another thread can then request the GIL

When a thread makes a blocking system call (read, write, recv, send), CPython releases the GIL before entering the kernel and reacquires it after returning. This is why threading is effective for I/O-bound Python code: threads release the GIL while waiting for I/O, allowing other threads to run Python code during that wait.

```python
import sys
print(sys.getswitchinterval())  # 0.005 (5 milliseconds)
# A thread holds the GIL for at most 5ms of Python execution
# before yielding to allow another waiting thread to acquire it
```

### 7.3 The GIL and CPU-Bound Work

For CPU-bound Python code (pure Python loops, arithmetic, string manipulation), the GIL prevents true parallelism:

```
Without GIL (ideal):
  CPU Core 1: [Thread 1 running Python] [Thread 1 running Python] ...
  CPU Core 2: [Thread 2 running Python] [Thread 2 running Python] ...
  Time: T/2 for T work (N threads → T/N time)

With GIL (reality for CPU-bound):
  CPU Core 1: [T1 Python][idle][T1 Python][idle]...
  CPU Core 2: [idle][T2 Python][idle][T2 Python]...
  Time: ~T (N threads → still ~T time, only GIL switching overhead added)
  
  Even worse: GIL contention adds overhead — threads wake up, fail to
  acquire the GIL, go back to sleep. Slower than single-threaded.
```

**Why NumPy and Pandas bypass the GIL:** NumPy operations (array arithmetic, matrix multiplication) are implemented in C extensions. When a C extension runs, it can release the GIL explicitly (`Py_BEGIN_ALLOW_THREADS`) for the duration of the C computation. NumPy does this for large array operations — multiple threads can run NumPy math in parallel because the C code runs without holding the GIL. The GIL is only held when Python bytecode runs.

### 7.4 Workarounds for the GIL

| Workaround | Mechanism | When to Use |
|---|---|---|
| `multiprocessing` | Separate processes (each has own GIL) | CPU-bound Python code; when crash isolation is needed |
| NumPy/Pandas | C extensions release GIL for bulk ops | Array math; use vectorized ops, not Python loops |
| `concurrent.futures.ProcessPoolExecutor` | Process pool with task distribution | Embarrassingly parallel CPU tasks |
| Cython with `nogil` | Compile Python to C, explicitly release GIL | Custom numerical code; high-performance extensions |
| PyPy | Different Python runtime, no GIL | Experimental; not compatible with CPython C extensions |
| Python 3.13 "free-threaded" mode | Optional build without GIL (experimental) | Experimental; not production-ready as of 2025 |
| Avoid threading for CPU work entirely | Use `asyncio` for I/O, `multiprocessing` for CPU | Clean separation of concerns |

### 7.5 Why PySpark UDFs Are Slow

PySpark Python UDFs are the canonical example of GIL-limited performance:

```
Spark Executor (JVM process):
  ├── Executor JVM thread pool (runs Scala/Java tasks natively, no GIL)
  └── Python Worker process (separate process, runs UDF code)
      └── Python Interpreter (single-threaded, GIL applies)

For a Python UDF:
  1. JVM executor picks up a task batch
  2. JVM serializes each row to bytes (Arrow or pickle)
  3. JVM sends bytes to Python worker process via socket
  4. Python process deserializes, calls UDF on each row (GIL held)
  5. Python serializes results back to bytes
  6. Python sends bytes back to JVM via socket
  7. JVM deserializes result

Overhead:
  - Serialization/deserialization on every row
  - IPC (inter-process communication) socket round-trip
  - Python GIL prevents concurrent UDF execution within the Python worker

Fix: Pandas UDFs (a.k.a. Vectorized UDFs):
  - Receive an entire partition as a Pandas Series or DataFrame (Arrow format)
  - Apply vectorized operation (C-level, GIL released for bulk ops)
  - Return Pandas result → converted back to Arrow → JVM continues
  - 10-100× faster than row-at-a-time Python UDFs
```

---

## 8. Deep Dive: Context Switching

### 8.1 What Is a Context Switch?

A **context switch** is the OS operation of saving the CPU state of a running thread/process and restoring the CPU state of another. It is how one CPU core serves many threads — by rapidly alternating between them.

CPU state that must be saved/restored:
- General-purpose registers (x86-64: 16 × 64-bit = 128 bytes)
- Instruction pointer (where to resume execution)
- Stack pointer (where the thread's stack currently is)
- Floating-point/SIMD registers (x87, SSE, AVX — up to 2 KB)
- Control registers (flags, etc.)

For **process context switches** (switching between two different processes), additional work is required:
- Flush the Translation Lookaside Buffer (TLB) — the CPU's cache of virtual→physical address translations. The new process has different mappings, so the old translations are invalid.
- Potentially flush CPU caches (instruction cache, data cache) — if the new process's data is not in cache, the next accesses will be cache misses (expensive)

### 8.2 Context Switch Cost

Context switch cost varies by operation type:

| Switch type | Approximate cost | Notes |
|---|---|---|
| Thread switch (same process) | 1–10 µs | No TLB flush needed (same address space) |
| Process switch (different process) | 5–50 µs | TLB flush + potential cache eviction |
| Python thread switch (GIL release/acquire) | 2–10 µs | GIL mutex operation + OS thread wake-up |
| `asyncio` coroutine switch | ~1 µs | No OS involvement — pure Python call stack swap |

These numbers seem small, but they compound. A Python server with 1000 concurrent threads each switching 1000 times per second generates 1 billion context switch operations per second — potentially consuming entire CPU cores just for switching.

### 8.3 Why Context Switching Limits Thread Scalability

```
Scenario: 100 threads each doing 1 ms of CPU work between I/O waits.

Ideal (no switching cost):
  100 threads × 1 ms CPU work = 100 ms CPU time required
  On 4 cores: completes in 25 ms

Reality (with switching cost of 10 µs per switch):
  Each thread: 1 ms work + multiple context switches
  When thread gets scheduled: 10 µs context switch in
  When thread is preempted: 10 µs context switch out
  Net CPU time per "1 ms" of work: 1 ms + 20 µs overhead = 2% overhead
  
  At 1000 threads all wanting to run simultaneously:
  Only 4 can run at a time; the others sleep
  The scheduler must cycle through all 1000 — increasing latency
  Cache thrashing: 1000 threads' working sets compete for L3 cache
```

This is why nginx (event-driven) scales better than Apache (thread-per-connection) for HTTP serving, and why `asyncio` scales better than threading for I/O-bound Python.

### 8.4 Voluntary vs Involuntary Context Switches

```bash
# View context switch counts for a process
pidstat -w -p <PID> 1

# Output:
# cswch/s   nvcswch/s
# 105.0     12.0

# cswch/s   = voluntary context switches per second
#             (process blocked on I/O, sleep, lock — deliberately yielded)
# nvcswch/s = involuntary context switches per second
#             (OS preempted the process — it didn't want to stop)

# A high nvcswch/s means the process is being preempted often — 
# it has more CPU-bound work than CPU time available.
# A high cswch/s means the process is I/O-bound — spending most time waiting.
```

For a Spark executor task that is CPU-bound (sorting, hashing), `nvcswch/s` should be near zero — the process should run until it voluntarily yields. High `nvcswch/s` suggests other processes are competing for CPU time.

---

## 9. Concurrency Models in Data Engineering

### 9.1 The Problem Space

Two dimensions characterize concurrent workloads:

**CPU-bound:** Most time spent on CPU computation (sorting, hashing, compression, serialization, Python for loops). Adding CPU cores helps. Adding more threads in Python doesn't help (GIL).

**I/O-bound:** Most time spent waiting for I/O (network reads from S3, Kafka broker, database; disk reads for Parquet files; shuffle data over the network). CPU sits idle. Adding concurrency (threads, async, more partitions) helps to keep CPU busy during I/O waits.

Most data engineering workloads are mixed: the CPU work is in C/Java (fast), and the bottleneck is I/O.

### 9.2 Choosing the Right Concurrency Model

**Use `multiprocessing` (separate processes) when:**
- CPU-bound Python code that cannot be vectorized (e.g., custom parsing logic, rule evaluation, ML inference using CPU-bound libraries)
- Crash isolation is critical (one worker crashing must not kill others)
- Memory isolation is needed (workers should not share state)

**Use `threading` (threads within one process) when:**
- I/O-bound work: HTTP requests, database queries, socket reads
- Low-latency shared data (threads share the heap — no serialization)
- The work can release the GIL (C extensions, blocking system calls)

**Use `asyncio` (coroutines, single thread) when:**
- Very high concurrency I/O (10,000+ simultaneous connections)
- Low per-request overhead is critical (no thread creation, no OS scheduling)
- All I/O is async-compatible (asyncpg, aiohttp, aiokafka)
- No CPU-bound work (asyncio is cooperative — one coroutine blocks all others)

**Use JVM/Go/Rust (language with true parallelism) when:**
- CPU-bound parallel computation at scale (this is why Spark is JVM-based)
- Need millions of goroutines / lightweight threads (Go's scheduler)

### 9.3 The Worker Model in Data Engineering Tools

| Tool | Worker Type | Concurrency Model | Why |
|---|---|---|---|
| Spark Executor | JVM process per executor | JVM threads (no GIL) | CPU-bound computation, shuffle I/O |
| PySpark Python Worker | Forked Python process per task | Single-threaded | GIL-safe; isolation per task |
| Airflow Scheduler | Python process | Threading (for DAG parsing) | I/O-bound: reads files, polls DB |
| Airflow CeleryExecutor Worker | Python process per worker | Multiprocessing (one process per task) | Task isolation; tasks may crash |
| Airflow LocalExecutor | Python process | Multiprocessing (subprocess per task) | Same as above |
| Kafka Consumer | JVM thread or Python process | Threading (JVM) / Async (Python) | I/O-bound: network reads |
| Dask Distributed Worker | Python process | Multiprocessing + threading | CPU tasks use processes; async I/O for scheduler communication |
| FastAPI/uvicorn (REST APIs) | Python process | `asyncio` | High concurrency HTTP I/O |

---

## 10. Mental Models

### 10.1 The Restaurant Model

A **process** is a restaurant. It has its own kitchen (CPU state), its own tables (stack), its own pantry (heap), and its own front door (file descriptors). Two restaurants don't share ingredients — they are completely isolated. If one restaurant catches fire, the other is fine.

A **thread** is a chef in the restaurant's kitchen. Multiple chefs work in the same kitchen — they share the pantry (heap) and can read each other's ingredients. But if two chefs both grab the last egg at the same time and try to use it simultaneously, you get a mess (data race). The kitchen needs rules: a whiteboard showing who has reserved which ingredient (mutex locks).

The **GIL** is a rule that says: "only one chef can be actively cooking at any moment." Multiple chefs can prep (I/O — waiting for the oven), but only one can actually cook (execute Python bytecode). This is fine for a restaurant where most time is spent waiting for orders (I/O-bound), but terrible for a restaurant where the bottleneck is cooking time (CPU-bound).

### 10.2 The CPU Core as Valet Parking

A CPU core can only execute one thread at a time — like a valet who can only drive one car at a time. When 1000 cars (threads) arrive, the valet rapidly moves between cars: takes car 1 to its spot, comes back, takes car 2, etc. Each car still gets parked, but not simultaneously. The parking lot (CPU) only has N spaces for N valets (cores) — only N cars are actually moving at any moment.

Context switching is the cost of the valet handing off the keys and getting in a different car. For I/O-bound workloads, cars are mostly parked (sleeping) anyway — the valet just parks the few that need moving. For CPU-bound workloads, every car needs moving simultaneously — the valet is the bottleneck regardless of how many cars there are.

### 10.3 Fork and Copy-on-Write as a Paperback Book

`fork()` is like Xeroxing a book — in theory, you'd copy every page. Copy-on-write is the optimization where you instead give both people the same book, but tell them: "mark your changes on sticky notes. If you need to write in the book, I'll make a separate copy of that page for you first." Pages neither person writes are never copied.

Airflow's `LocalExecutor` uses this: the scheduler forks a child process per task. The child inherits the parent's memory (all the DAG definitions, configuration, imports) via COW. If the child doesn't write to those pages (it mostly doesn't — it just runs the task function), the memory is never actually copied.

---

## 11. Failure Scenarios

### 11.1 GIL-Induced CPU Bottleneck — Python Threading Slows Down

```python
# WRONG: using threads for CPU-bound Python work

import threading
import time

def cpu_heavy(n):
    """Pure Python loop — holds the GIL the entire time."""
    total = 0
    for i in range(n):
        total += i * i
    return total

# Single-threaded: ~2 seconds for N=50_000_000
start = time.time()
cpu_heavy(50_000_000)
print(f"Single-threaded: {time.time() - start:.2f}s")

# Two threads: ~2.5 seconds (SLOWER due to GIL contention)
start = time.time()
t1 = threading.Thread(target=cpu_heavy, args=(25_000_000,))
t2 = threading.Thread(target=cpu_heavy, args=(25_000_000,))
t1.start(); t2.start()
t1.join();  t2.join()
print(f"Two threads (CPU-bound): {time.time() - start:.2f}s")
# SLOWER than single-threaded! GIL contention adds overhead.

# FIX: use multiprocessing for CPU-bound Python
from multiprocessing import Pool
start = time.time()
with Pool(2) as pool:
    pool.map(cpu_heavy, [25_000_000, 25_000_000])
print(f"Two processes (CPU-bound): {time.time() - start:.2f}s")
# ~1 second — true parallelism, two separate Python interpreters
```

### 11.2 Zombie Process Accumulation in Airflow

```
Problem: Airflow LocalExecutor spawns a child process per task.
If the parent (scheduler) does not call wait() on completed children,
they become zombies: the OS keeps their PCB alive to hold the exit code.

Zombie accumulation:
  Parent PID 1234: Airflow scheduler
  Child PIDs: 1235, 1236, 1237 ... 10000+ (all finished, but not reaped)
  
  Zombies consume:
    - PID namespace entries (Linux default: 32768 PIDs)
    - Process table entries in the kernel
  
  Symptom: "Failed to fork: Resource temporarily unavailable"
  Cause: No more PIDs available — all consumed by zombies

Detection:
  $ ps aux | grep -c 'Z'    # count zombie processes
  $ ps -eo pid,ppid,stat | awk '$3=="Z"'   # list zombie PIDs

Root cause in Airflow: The scheduler process must periodically call
os.waitpid(-1, os.WNOHANG) to reap finished children.
LocalExecutor does this in its main loop — a bug that skips this
reaping will accumulate zombies over time.
```

### 11.3 Shared-Memory Corruption After Fork in Python

```python
# DANGEROUS: modifying shared state after fork

import multiprocessing
import psycopg2   # PostgreSQL driver

# At the module level, before forking:
conn = psycopg2.connect("host=localhost dbname=mydb user=myuser")

def worker(task_id):
    # PROBLEM: `conn` was created before fork.
    # After fork, parent and child both have the same connection object.
    # The connection is a socket — both processes share the same socket file descriptor.
    # Both processes sending SQL on the same socket → corrupted protocol stream.
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    # This may work sometimes, corrupt data other times, or crash.
    return cursor.fetchall()

if __name__ == "__main__":
    with multiprocessing.Pool(4) as pool:
        results = pool.map(worker, range(100))  # UNSAFE

# FIX: create connections INSIDE the worker function, after fork:
def safe_worker(task_id):
    # Each process creates its own connection — no sharing
    local_conn = psycopg2.connect("host=localhost dbname=mydb user=myuser")
    cursor = local_conn.cursor()
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    result = cursor.fetchall()
    local_conn.close()
    return result
```

This pattern breaks Airflow DAGs that open database connections at module level and then fork workers. SQLAlchemy connection pools have the same issue — always call `engine.dispose()` in the child process after fork, before using the engine.

### 11.4 Thread Starvation from Lock Contention

```python
# Thread starvation: one thread holds a lock for too long,
# starving all others waiting for it.

import threading
import time

lock = threading.Lock()
shared_results = []

def slow_producer():
    """Holds the lock while doing slow work — starves consumers."""
    with lock:
        time.sleep(1)   # holding the lock for 1 second!
        shared_results.append(expensive_computation())

def consumer():
    with lock:   # blocked for 1 second, doing nothing
        if shared_results:
            return shared_results.pop()

# FIX: minimize time spent holding the lock
def fast_producer():
    result = expensive_computation()  # compute OUTSIDE the lock
    with lock:
        shared_results.append(result)  # only the append is locked

# BETTER FIX for producer-consumer: use a thread-safe queue
import queue
q = queue.Queue()
def producer():
    result = expensive_computation()
    q.put(result)     # thread-safe, no explicit locking needed
def consumer_v2():
    result = q.get()  # blocks until item available; thread-safe
```

### 11.5 Deadlock from Lock Ordering

```python
# Classic deadlock: two threads, two locks, opposite acquisition order

lock_a = threading.Lock()
lock_b = threading.Lock()

def thread_1():
    with lock_a:         # acquires A
        time.sleep(0.1)  # sleep while holding A
        with lock_b:     # waits for B — but thread_2 holds B
            pass         # never reached

def thread_2():
    with lock_b:         # acquires B
        time.sleep(0.1)  # sleep while holding B
        with lock_a:     # waits for A — but thread_1 holds A
            pass         # never reached

# Thread 1: holds A, waiting for B
# Thread 2: holds B, waiting for A
# → DEADLOCK: neither can proceed

# FIX: always acquire locks in the same global order
LOCK_ORDER = [lock_a, lock_b]  # document and enforce this order
def thread_1_fixed():
    with lock_a:    # always A before B
        with lock_b:
            pass

def thread_2_fixed():
    with lock_a:    # always A before B (same order as thread_1!)
        with lock_b:
            pass
```

---

## 12. Data Engineering Connections

### 12.1 Spark Executor Architecture: JVM Process + Python Workers

A Spark executor is a JVM process. Each executor runs multiple tasks concurrently, using JVM threads (no GIL). Java's thread model gives true parallelism: 16 threads on a 16-core machine means 16 simultaneous tasks.

When a Spark job runs Python UDFs:
1. The Spark executor JVM process spawns a **Python worker process** for the task (using `fork()` on Linux, controlled by `spark.python.worker.reuse`).
2. The Python worker is a separate OS process — it has its own GIL.
3. The JVM communicates with the Python worker via a local socket (loopback), sending batches of rows serialized as Arrow or pickle.
4. The Python worker processes one batch at a time (single-threaded, GIL applies).
5. With `spark.python.worker.reuse=true`, Python workers are kept alive between tasks to avoid `fork()` overhead (at the cost of potentially leaking state between tasks).

This architecture means Pandas UDFs (`@pandas_udf`) are much faster: instead of serializing one row at a time, the JVM sends an entire partition as an Arrow buffer in one IPC call. The Python worker operates on a Pandas DataFrame (NumPy array under the hood — C-level, GIL released for bulk ops) and returns an Arrow buffer.

### 12.2 Airflow: CeleryExecutor vs LocalExecutor

**LocalExecutor:**
```
Airflow Scheduler Process (Python)
├── Main thread: DAG scheduling (Kahn's algorithm, Section M16)
└── TaskRunner subprocess per task:
    └── fork() → child process
        └── Runs the task's Python callable
        └── Parent calls waitpid() to reap on completion
```

LocalExecutor is limited by the single machine's CPU and memory. All tasks share the host — a task that consumes all RAM crashes the scheduler.

**CeleryExecutor:**
```
Airflow Scheduler Process:
  └── Submits task to Celery queue (Redis or RabbitMQ)

Celery Worker Processes (separate machines/containers):
  ├── Worker 1 (Python process): polls queue, runs tasks
  ├── Worker 2 (Python process): polls queue, runs tasks
  └── Worker N: ...
  
  Each Worker can spawn concurrent task subprocesses (multiprocessing)
  controlled by --concurrency N
```

The critical detail: each Celery worker task is a **process** (forked), not a thread. This gives crash isolation (one task crash doesn't kill other tasks on the same worker) and avoids GIL contention between concurrent tasks on the same worker.

### 12.3 Kafka Consumer Threading: Python vs JVM

**Python Kafka Consumer (kafka-python or confluent-kafka):**
```python
from confluent_kafka import Consumer

# Single-threaded consumer: max throughput = one partition per consumer instance
consumer = Consumer({'bootstrap.servers': 'kafka:9092', 'group.id': 'my-group'})
consumer.subscribe(['my-topic'])

while True:
    msg = consumer.poll(1.0)   # I/O: blocks waiting for Kafka broker response
    if msg:
        process(msg.value())    # CPU: GIL held
```

For scaling a Python Kafka consumer:
- **Partitions:** Each consumer instance can consume from multiple partitions. More parallelism requires more consumer instances (separate processes), not more threads (GIL-bound).
- **Thread-per-partition:** Possible only if processing is I/O-bound (e.g., writing to a database). Each thread does Kafka I/O (releases GIL during poll) + DB write (releases GIL during execute). For CPU-bound message processing, use processes.

**JVM Kafka Consumer (Spark Structured Streaming):**
```
Spark Structured Streaming micro-batch:
  KafkaSourceProvider → reads Kafka offsets (one JVM thread per partition)
  → creates a DataFrame of messages
  → DataFrame operations (map, filter, join) run as JVM tasks in parallel
     (N tasks × M executor threads = true parallel processing)
  → write to sink (parquet, delta, jdbc)
```

Spark's Kafka integration assigns one partition per Spark task, and tasks run truly in parallel across executor threads (no GIL). This is why Spark is the dominant choice for high-throughput Python Kafka processing — the actual processing runs in JVM threads.

### 12.4 Dask: Python's Answer to Spark's JVM Advantage

Dask uses Python multiprocessing (separate processes) to work around the GIL. Each Dask worker is a Python process with its own GIL. Tasks are distributed to workers via the Dask scheduler, which communicates over TCP sockets.

```
Dask Client (Python process)
  └── submits tasks to Dask Scheduler

Dask Scheduler (Python process)
  └── assigns tasks to workers (uses BFS/topological sort on the task graph)

Dask Worker 1 (Python process, 4 threads)
  └── thread pool for I/O overlap
  └── each CPU task runs in the worker process (no GIL contention vs other workers)

Dask Worker 2 (Python process, 4 threads)
  └── same

Dask Worker N: ...
```

The trade-off vs Spark: serialization overhead. Dask uses pickle to pass data between workers; Spark uses Arrow (zero-copy binary format). For numerical computation on large arrays, Dask with NumPy/Pandas is competitive with Spark because NumPy operations release the GIL. For arbitrary Python logic, Dask is limited by pickle serialization latency.

---

## 13. Code Toolkit

### 13.1 `process_thread_demo.py` — Processes, Threads, and GIL Benchmarking

```python
"""
process_thread_demo.py — Comparative benchmarking of concurrency models.

Benchmarks:
  1. CPU-bound: single-threaded, multi-threaded, multi-process
     → shows GIL serialization for threads on CPU-bound work
  2. I/O-bound: single-threaded, multi-threaded, multi-process
     → shows thread effectiveness for I/O-bound work
  3. Process creation overhead: fork vs spawn
  
Run directly to see the benchmark results.
"""
from __future__ import annotations
import os
import sys
import time
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Callable


# ─── Workloads ────────────────────────────────────────────────────────────────

def cpu_bound_work(n: int = 5_000_000) -> int:
    """Pure Python CPU-bound work: sum of squares. Holds GIL the entire time."""
    return sum(i * i for i in range(n))


def io_bound_work(delay_s: float = 0.05) -> str:
    """Simulated I/O-bound work: sleep releases the GIL."""
    import time
    time.sleep(delay_s)   # GIL released during sleep
    return f"done after {delay_s}s"


# ─── Benchmark harness ────────────────────────────────────────────────────────

def bench(label: str, fn: Callable, n_workers: int, use_processes: bool = False) -> float:
    """
    Run fn concurrently with n_workers workers.
    Returns wall-clock time in seconds.
    """
    executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    start = time.perf_counter()
    with executor_class(max_workers=n_workers) as ex:
        futures = [ex.submit(fn) for _ in range(n_workers)]
        [f.result() for f in futures]
    elapsed = time.perf_counter() - start
    print(f"  {label:<45} {elapsed:.3f}s")
    return elapsed


def benchmark_cpu_bound() -> None:
    print("\n=== CPU-Bound Benchmark (pure Python sum-of-squares) ===")
    print("Expected: threads ~= single-threaded (GIL), processes = faster\n")
    
    # Baseline: single-threaded
    start = time.perf_counter()
    for _ in range(4):
        cpu_bound_work()
    baseline = time.perf_counter() - start
    print(f"  {'Single-threaded (4 sequential calls)':<45} {baseline:.3f}s")
    
    bench("4 threads  (GIL-bound, expect ~same as serial)", cpu_bound_work, 4, use_processes=False)
    bench("4 processes (true parallel, expect ~4x faster)",  cpu_bound_work, 4, use_processes=True)


def benchmark_io_bound() -> None:
    print("\n=== I/O-Bound Benchmark (sleep 50ms per task) ===")
    print("Expected: threads ~= processes (both release GIL/CPU during I/O)\n")
    
    start = time.perf_counter()
    for _ in range(8):
        io_bound_work()
    baseline = time.perf_counter() - start
    print(f"  {'Single-threaded (8 sequential sleeps)':<45} {baseline:.3f}s  (8 × 50ms = 400ms)")
    
    bench("8 threads  (GIL released during sleep, expect ~50ms)", io_bound_work, 8, use_processes=False)
    bench("8 processes (separate processes, expect ~50ms)",        io_bound_work, 8, use_processes=True)


def benchmark_creation_overhead() -> None:
    print("\n=== Process vs Thread Creation Overhead ===\n")
    
    N = 20
    
    start = time.perf_counter()
    threads = [threading.Thread(target=lambda: None) for _ in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    thread_time = time.perf_counter() - start
    print(f"  Create + join {N} threads:   {thread_time * 1000:.1f}ms  "
          f"({thread_time / N * 1000:.2f}ms per thread)")
    
    start = time.perf_counter()
    processes = [multiprocessing.Process(target=lambda: None) for _ in range(N)]
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    proc_time = time.perf_counter() - start
    print(f"  Create + join {N} processes: {proc_time * 1000:.1f}ms  "
          f"({proc_time / N * 1000:.2f}ms per process)")
    
    print(f"\n  Process creation is ~{proc_time / thread_time:.0f}x more expensive than thread creation")


def show_process_info() -> None:
    print("\n=== Current Process Information ===\n")
    print(f"  PID:                 {os.getpid()}")
    print(f"  Parent PID (PPID):   {os.getppid()}")
    print(f"  Active thread count: {threading.active_count()}")
    print(f"  CPU count:           {os.cpu_count()}")
    print(f"  Python interpreter:  {sys.executable}")
    print(f"  GIL switch interval: {sys.getswitchinterval():.3f}s ({sys.getswitchinterval()*1000:.1f}ms)")
    print(f"  Multiprocessing start method: {multiprocessing.get_start_method()}")


def demonstrate_fork() -> None:
    print("\n=== Fork Demo: Parent and Child ===\n")
    
    shared_counter = 0   # This value is copied to the child (COW)
    
    pid = os.fork()
    
    if pid == 0:
        # CHILD PROCESS
        shared_counter += 100   # child modifies ITS OWN COPY — doesn't affect parent
        print(f"  Child  (PID={os.getpid()}): counter = {shared_counter}")
        os._exit(0)   # exit child without running Python cleanup
    else:
        # PARENT PROCESS
        os.waitpid(pid, 0)   # wait for child to finish (prevents zombie)
        print(f"  Parent (PID={os.getpid()}): counter = {shared_counter}  (unchanged — isolation!)")


if __name__ == "__main__":
    show_process_info()
    benchmark_cpu_bound()
    benchmark_io_bound()
    benchmark_creation_overhead()
    
    if hasattr(os, 'fork'):   # fork is not available on Windows
        demonstrate_fork()
    else:
        print("\n(fork() demo skipped: not available on Windows)")
```

### 13.2 `gil_bypass.py` — The Four Ways to Work Around the GIL

```python
"""
gil_bypass.py — Demonstrates four GIL bypass strategies.

1. multiprocessing.Pool: separate processes, no GIL constraint
2. numpy vectorization: C-level ops, GIL released for bulk work
3. concurrent.futures.ThreadPoolExecutor for I/O: GIL released during I/O
4. threading + C extension: demonstrates GIL release in C extensions

Run directly for a comparison of all four approaches.
"""
from __future__ import annotations
import time
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numpy as np


# Strategy 1: multiprocessing (separate GIL per process)

def compute_chunk(args):
    """CPU-bound: sum of squares from start to end. Each process has its own GIL."""
    start, end = args
    return sum(i * i for i in range(start, end))

def parallel_with_processes(n: int, n_workers: int) -> int:
    chunk_size = n // n_workers
    chunks = [(i * chunk_size, (i + 1) * chunk_size) for i in range(n_workers)]
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        results = list(executor.map(compute_chunk, chunks))
    return sum(results)


# Strategy 2: NumPy vectorization (GIL released in C)

def compute_numpy(n: int) -> float:
    """Equivalent computation using NumPy — runs in C, GIL released for bulk ops."""
    arr = np.arange(n, dtype=np.float64)
    return float(np.sum(arr * arr))


# Strategy 3: threading for I/O-bound work (GIL released during blocking I/O)

import urllib.request

def fetch_url(url: str) -> int:
    """I/O bound: HTTP GET — GIL released during the network round-trip."""
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return len(r.read())
    except Exception:
        return 0

def parallel_io_with_threads(urls: list[str], n_workers: int) -> list[int]:
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        return list(ex.map(fetch_url, urls))


# Strategy 4: threading where C extension releases GIL

def thread_numpy_parallel(arrays: list[np.ndarray]) -> list[float]:
    """
    NumPy sum operations run in C; GIL is released during the computation.
    Multiple threads can run NumPy in parallel on separate arrays.
    """
    results = [None] * len(arrays)
    
    def worker(idx, arr):
        # np.sum releases the GIL for large arrays — true parallel execution
        results[idx] = float(np.sum(arr * arr))
    
    threads = [threading.Thread(target=worker, args=(i, arr)) for i, arr in enumerate(arrays)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    N = 10_000_000
    print(f"=== GIL Bypass Strategies (N={N:,}) ===\n")

    # Strategy 1: multiprocessing
    n_workers = min(4, multiprocessing.cpu_count())
    t0 = time.perf_counter()
    result_mp = parallel_with_processes(N, n_workers)
    t_mp = time.perf_counter() - t0
    print(f"1. multiprocessing ({n_workers} workers): {t_mp:.3f}s  result={result_mp:,}")

    # Comparison: single Python loop
    t0 = time.perf_counter()
    result_py = sum(i * i for i in range(N))
    t_py = time.perf_counter() - t0
    print(f"   Single-threaded Python loop:    {t_py:.3f}s  result={result_py:,}")
    print(f"   Speedup from processes: {t_py/t_mp:.1f}x")

    # Strategy 2: NumPy
    t0 = time.perf_counter()
    result_np = compute_numpy(N)
    t_np = time.perf_counter() - t0
    print(f"\n2. NumPy vectorized (C-level, GIL released): {t_np:.3f}s  result={result_np:,.0f}")
    print(f"   Speedup over Python loop: {t_py/t_np:.0f}x")

    # Strategy 4: Threaded NumPy
    arrays = [np.arange(N // 4, dtype=np.float64) for _ in range(4)]
    t0 = time.perf_counter()
    results_tnp = thread_numpy_parallel(arrays)
    t_tnp = time.perf_counter() - t0
    print(f"\n4. 4 threads × NumPy (GIL released in C): {t_tnp:.3f}s")
    print(f"   Note: if NumPy releases GIL, this is faster than 4× Python loops")

    print(f"\nSummary:")
    print(f"  Python threads (CPU-bound):  ~{t_py:.2f}s  (GIL prevents true parallelism)")
    print(f"  Processes ({n_workers} workers):        ~{t_mp:.2f}s  (true parallelism)")
    print(f"  NumPy (vectorized):          ~{t_np:.2f}s  (C-level, GIL released)")
    print(f"\nLesson: For CPU-bound Python, use processes or C-backed vectorization.")
    print(f"        For I/O-bound Python, use threads (GIL released during I/O).")
```

### 13.3 `context_switch_profiler.py` — Measuring Context Switch Cost

```python
"""
context_switch_profiler.py — Measures concurrency model overhead.

Measures:
  - Thread creation and join latency
  - Lock acquisition cost (uncontended and contended)
  - Queue put/get throughput
  - Context switch approximation via timing

Run directly for a profile of your system's concurrency costs.
"""
from __future__ import annotations
import threading
import time
import queue
import statistics


def measure_thread_creation(n_rounds: int = 100) -> dict:
    """Measure time to create, start, and join one thread."""
    times = []
    for _ in range(n_rounds):
        t0 = time.perf_counter()
        t = threading.Thread(target=lambda: None)
        t.start()
        t.join()
        times.append((time.perf_counter() - t0) * 1e6)  # microseconds
    return {"mean_us": statistics.mean(times), "p99_us": sorted(times)[int(0.99 * len(times))]}


def measure_lock_uncontended(n_rounds: int = 100_000) -> dict:
    """Measure lock acquire/release when no contention (fast path)."""
    lock = threading.Lock()
    times = []
    for _ in range(n_rounds):
        t0 = time.perf_counter_ns()
        with lock:
            pass
        times.append(time.perf_counter_ns() - t0)
    return {"mean_ns": statistics.mean(times), "p99_ns": sorted(times)[int(0.99 * len(times))]}


def measure_lock_contended(n_threads: int = 4, n_acquisitions: int = 10_000) -> dict:
    """Measure lock acquire/release under contention (threads competing)."""
    lock = threading.Lock()
    times = []
    times_lock = threading.Lock()
    
    def worker():
        for _ in range(n_acquisitions):
            t0 = time.perf_counter_ns()
            with lock:
                pass
            elapsed = time.perf_counter_ns() - t0
            with times_lock:
                times.append(elapsed)
    
    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    return {
        "mean_ns": statistics.mean(times),
        "p99_ns": sorted(times)[int(0.99 * len(times))],
        "contention_ratio": statistics.mean(times) / max(1, measure_lock_uncontended(1000)['mean_ns'])
    }


def measure_queue_throughput(n_items: int = 100_000) -> dict:
    """Measure items-per-second through a threading.Queue."""
    q: queue.Queue = queue.Queue()
    received = []
    done_event = threading.Event()
    
    def producer():
        for i in range(n_items):
            q.put(i)
        q.put(None)  # sentinel
    
    def consumer():
        while True:
            item = q.get()
            if item is None:
                break
            received.append(item)
        done_event.set()
    
    t0 = time.perf_counter()
    tp = threading.Thread(target=producer)
    tc = threading.Thread(target=consumer)
    tp.start(); tc.start()
    tp.join()
    done_event.wait()
    elapsed = time.perf_counter() - t0
    
    return {
        "items_per_sec": int(n_items / elapsed),
        "us_per_item": elapsed / n_items * 1e6
    }


if __name__ == "__main__":
    print("=== Concurrency Overhead Profiler ===\n")
    
    print("Thread creation (create + start + join, 100 rounds):")
    tc = measure_thread_creation(100)
    print(f"  Mean: {tc['mean_us']:.1f} µs   P99: {tc['p99_us']:.1f} µs\n")
    
    print("Lock (uncontended, 100K acquire+release cycles):")
    lu = measure_lock_uncontended(100_000)
    print(f"  Mean: {lu['mean_ns']:.0f} ns   P99: {lu['p99_ns']:.0f} ns\n")
    
    print("Lock (4 threads contending, 10K acquisitions each):")
    lc = measure_lock_contended(4, 10_000)
    print(f"  Mean: {lc['mean_ns']:.0f} ns   P99: {lc['p99_ns']:.0f} ns")
    print(f"  Contention overhead: {lc['contention_ratio']:.1f}x slower than uncontended\n")
    
    print("Queue throughput (producer → consumer, 100K items):")
    qt = measure_queue_throughput(100_000)
    print(f"  Throughput: {qt['items_per_sec']:,} items/sec")
    print(f"  Latency:    {qt['us_per_item']:.1f} µs per item\n")
    
    print("Reference:")
    print("  Thread creation:          ~50–500 µs (OS overhead)")
    print("  Lock uncontended:         ~20–100 ns (single atomic operation)")
    print("  Lock contended (4 thr):   ~1–10 µs  (includes OS wake-up cost)")
    print("  Queue item:               ~1–5 µs   (2 locks + memory alloc)")
    print("  Context switch (thread):  ~1–10 µs  (register save/restore)")
    print("  Context switch (process): ~5–50 µs  (TLB flush + register save)")
```

---

## 14. Hands-On Labs

### Lab 1: GIL Impact Measurement

**Goal:** Empirically measure the GIL's impact by running identical CPU-bound work with 1, 2, and 4 threads vs processes and observing speedup ratios.

```python
# lab1_gil_impact.py
"""
Measure actual speedup from threads vs processes for CPU-bound work.
Expected: threads show ~1x speedup (GIL), processes show ~Nx speedup.
"""
import time
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def cpu_work(n=3_000_000):
    return sum(i * i for i in range(n))

def run_n_workers(n, use_processes=False):
    executor = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    t0 = time.perf_counter()
    with executor(max_workers=n) as ex:
        list(ex.map(lambda _: cpu_work(), range(n)))
    return time.perf_counter() - t0

if __name__ == "__main__":
    baseline = run_n_workers(1, use_processes=False)
    print(f"{'Workers':>8}  {'Threads':>10}  {'Speedup':>8}  {'Processes':>12}  {'Speedup':>8}")
    print("-" * 60)
    for n in [1, 2, 4]:
        t_thread = run_n_workers(n, use_processes=False)
        t_proc   = run_n_workers(n, use_processes=True)
        print(f"{n:>8}  {t_thread:>8.3f}s  {baseline/t_thread:>6.1f}x   "
              f"{t_proc:>10.3f}s  {baseline/t_proc:>6.1f}x")
    print("\nInterpretation:")
    print("  Thread speedup should stay near 1.0x (GIL serializes CPU work)")
    print("  Process speedup should approach Nx (true parallelism)")
```

### Lab 2: Process Isolation Demonstration

**Goal:** Prove that process memory is isolated — modifications in a child process do not affect the parent.

```python
# lab2_process_isolation.py
"""
Demonstrates that process memory is isolated after fork().
Shows that shared memory (multiprocessing.Value) is needed for inter-process data.
"""
import os
import multiprocessing
import multiprocessing.sharedctypes

if __name__ == "__main__":
    print("=== Process Isolation ===\n")
    
    # Regular variable: NOT shared after fork
    regular_counter = 0
    
    # Shared variable: explicitly shared via shared memory
    shared_counter = multiprocessing.Value('i', 0)  # 'i' = C int
    
    def child_work():
        global regular_counter
        regular_counter += 100      # modifies CHILD's copy only
        with shared_counter.get_lock():
            shared_counter.value += 100   # modifies SHARED memory
        print(f"  Child  (PID {os.getpid()}): regular_counter={regular_counter}, "
              f"shared_counter={shared_counter.value}")
    
    p = multiprocessing.Process(target=child_work)
    p.start()
    p.join()
    
    print(f"  Parent (PID {os.getpid()}): regular_counter={regular_counter}  "
          f"← unchanged (child had its own copy)")
    print(f"  Parent (PID {os.getpid()}): shared_counter={shared_counter.value}  "
          f"← changed (true shared memory)")
    
    print("\nLesson: regular variables are NOT shared across fork().")
    print("        Use multiprocessing.Value/Array/Manager for shared state.")
```

---

## 15. Interview Q&A

**Q1: What is the difference between a process and a thread?**

A process is the OS's unit of isolation — an independent running instance of a program with its own private virtual address space, its own file descriptors, its own page table, and its own CPU state. Two processes cannot access each other's memory unless they explicitly set up shared memory via IPC mechanisms. If one process crashes, the OS cleans up its resources and other processes are unaffected.

A thread is the OS's unit of execution within a process. Multiple threads exist within the same process and share the same virtual address space — the same heap, the same code, the same global variables, the same open file descriptors. Each thread has its own stack and its own CPU register state (instruction pointer, stack pointer), but reads and writes to the same heap. This shared memory makes communication between threads fast (no serialization) but requires synchronization (locks, semaphores) to prevent data races, where two threads modify the same memory location concurrently and produce incorrect results.

The practical trade-off: threads are cheaper to create (microseconds vs milliseconds for processes), communicate faster (direct shared memory vs IPC), but lack crash isolation (one thread crashing with a segfault kills the entire process) and share the GIL in CPython (preventing true CPU parallelism for Python code).

**Q2: What is the Python GIL and why does it exist?**

The Global Interpreter Lock is a mutex in CPython — the standard Python implementation — that ensures only one thread executes Python bytecode at any given moment. It is a single lock protecting the entire Python interpreter state.

The GIL exists because CPython manages memory via reference counting: every Python object has an `ob_refcnt` field that tracks how many references point to it. When the reference count drops to zero, the object is freed. Every Python operation that creates, copies, or destroys a reference mutates reference counts. Without synchronization, two threads incrementing and decrementing the same object's reference count simultaneously would corrupt the count — a data race. Rather than making every reference count operation an atomic CPU instruction (which would impose overhead on every Python assignment, every function call, every loop iteration), Guido van Rossum chose a single global lock: if only one thread runs at a time, there can be no concurrent reference count mutations.

The GIL was introduced when threading was added to Python in the 1990s. Removing it has been attempted multiple times (Jython has no GIL; PyPy's STM branch experimented without it). The problem is that CPython's C extension ecosystem — NumPy, pandas, scikit-learn, TensorFlow — all assume the GIL exists and use it for their own thread safety guarantees. Removing the GIL requires those extensions to be rewritten, or the interpreter to use fine-grained locking instead (complex and potentially slower for single-threaded code). Python 3.13 introduced an experimental "free-threaded" build (`python3.13t`) without the GIL, but it is not yet production-ready.

**Q3: When would you use `multiprocessing` vs `threading` in Python?**

The decision comes down to two factors: whether the work is CPU-bound or I/O-bound, and whether crash isolation or low-overhead communication is more important.

Use `multiprocessing` when the work is CPU-bound — when threads would compete for the GIL and serialize execution. Python data processing tasks that cannot be vectorized (custom parsing, rule evaluation, ML scoring with CPU-bound inference), tasks that must be isolated (a crashing task should not kill the orchestrator), or tasks that consume a lot of memory that should be cleaned up after completion (processes release their memory on exit; threads do not). The cost: process creation takes milliseconds, inter-process communication requires serialization (pickle), and memory is not shared by default.

Use `threading` when the work is I/O-bound — when threads spend most of their time blocked on I/O (network calls, database queries, disk reads) and release the GIL during those waits. Multiple HTTP requests to external APIs, concurrent database queries, reading multiple files simultaneously — all benefit from threading because the GIL is released while the thread waits for the OS to return data. Threads also work well when threads need to share data with low latency (updating a shared cache, collecting results into a shared list with a lock). The cost: no crash isolation (a bug in one thread can corrupt shared memory and crash all threads), and synchronization bugs (deadlocks, data races) are subtle.

For very high concurrency I/O (10,000+ simultaneous connections), neither threads nor processes scale well — use `asyncio` instead.

**Q4: Explain how a Spark executor handles concurrency and why that's different from a Python process.**

A Spark executor is a JVM (Java Virtual Machine) process, not a Python process. The JVM runs a true thread pool: multiple threads run simultaneously on different CPU cores without any global interpreter lock. A 16-core executor with 4 task slots runs 4 tasks in parallel — four JVM threads actually executing in parallel, each on its own CPU core, without any serialization of CPU work. Java/Scala's memory model handles thread safety via explicit synchronization (synchronized blocks, java.util.concurrent primitives), not a global lock.

This is why Spark operators written in Scala or Java — sorting, hashing, joins, aggregations — can fully utilize multi-core CPUs within a single executor process. The JVM garbage collector does introduce stop-the-world pauses (when GC runs, all threads pause), which is why Spark uses Tungsten's off-heap memory management for performance-critical shuffle buffers — bypassing the JVM heap and GC entirely.

When PySpark runs Python UDFs, it bridges into Python via separate Python worker processes (one per task). The JVM serializes row data to Arrow or pickle format, sends it to the Python worker via a local socket, the Python worker runs the UDF (with its own GIL applying within the Python worker), and sends results back. This serialization + IPC + Python GIL cost is why Pandas UDFs are preferred over row-at-a-time UDFs: Pandas UDFs send an entire partition as one Arrow buffer, and the UDF operates on a NumPy array in C (GIL released during bulk ops).

**Q5: How does Airflow achieve concurrent task execution and what are the concurrency limits?**

Airflow's concurrency model depends on the executor type. The LocalExecutor (single machine) spawns a subprocess per task — each task is an OS process forked from the scheduler process. The scheduler uses `multiprocessing` to fork children and `os.waitpid()` to reap completed children. Task isolation is achieved via process separation: one task's crash (Python exception, OOM kill, segfault) does not crash the scheduler or other tasks.

The CeleryExecutor distributes tasks across multiple worker machines via a message queue (Redis or RabbitMQ). Each Celery worker is a Python process that polls the queue for tasks. Each worker can run multiple tasks concurrently, controlled by `--concurrency N` — which spawns N worker processes (not threads). The practical concurrency limits are: `max_active_tasks_per_dag` (default 16 in recent Airflow versions), `max_active_runs_per_dag` (how many DAG runs of the same DAG can run simultaneously), and the pool slot count (named resource pools that throttle specific task types).

The scheduler process itself is single-process but multi-threaded: one thread runs the scheduling loop (Kahn's algorithm for task readiness), another thread handles DAG file parsing, another handles heartbeat and metadata database cleanup. These threads are all I/O-bound (database queries, file reads) so the GIL is released during most of their work — the GIL does not limit Airflow's scheduling throughput.

**Q6: You have a Python data pipeline that processes 10 million records per hour but needs to scale to 100 million. How do you approach the concurrency design?**

First, profile before redesigning. The bottleneck is almost never where you expect. Use `cProfile` (CPU), `line_profiler` (line-by-line CPU), `memory_profiler` (memory), and `strace` / `perf` (system calls) to identify whether the pipeline is CPU-bound, I/O-bound, memory-bound, or serialization-bound.

If it is I/O-bound (reading from S3, writing to a database, fetching from an API): threading or asyncio is the right tool. `ThreadPoolExecutor` with 8–32 threads for database writes, `aiohttp` for concurrent API calls. GIL is not a concern because I/O operations release it. The limit is usually the downstream system's throughput (database write IOPS, API rate limits) rather than the Python process.

If it is CPU-bound (parsing complex JSON, running regex on large strings, custom aggregation logic): use `multiprocessing.Pool` to distribute work across multiple Python processes, each with its own GIL. Shard the input by partition key, assign each shard to a process, collect results via a pipe or shared memory. For numerical computation, vectorize with NumPy/Pandas — a Python loop over 10 million records replaced by a single NumPy expression is typically 100–1000× faster and does not require multiprocessing.

If the pipeline cannot fit the data in memory or needs to scale beyond one machine: migrate to Spark or Dask. Spark's JVM task threads provide true CPU parallelism without GIL constraints; its distributed scheduler handles data partitioning across many machines. The migration cost is real — Spark's API differs from Pandas, and Spark has significant infrastructure overhead — so this decision should be driven by demonstrated inability to scale on a single large machine, not premature optimization.

For most data pipelines processing 100M records/hour: a 32-core machine running multiprocessing with 16 Python processes (leaving cores for OS and I/O), each processing 6.25M records/hour, is sufficient and simpler than a distributed system.

---

## 16. Cross-Question Chain

**Q1 [Interviewer]: What happens when you create a Python thread?**

When you call `threading.Thread(target=fn).start()`, Python calls the OS's `pthread_create()` system call (on Linux/macOS) or `CreateThread()` (on Windows). The OS allocates a stack for the new thread (typically 8 MB on Linux, adjustable), creates a kernel thread entry in the process's thread table, adds the thread to the OS scheduler's run queue, and returns. The new thread begins executing `fn()` on the next scheduler tick that assigns it CPU time. From the OS's perspective, Python threads are native OS threads — they are visible to `ps -T` and are scheduled by the kernel, not by Python.

**Q2 [Interviewer]: You said the OS scheduler assigns CPU time. How does the OS decide which thread runs next?**

The Linux scheduler (CFS — Completely Fair Scheduler) tracks a "virtual runtime" for each thread: the amount of CPU time the thread has actually received. At each scheduler tick (roughly every 4 ms by default), the scheduler preempts the currently-running thread if another thread has accumulated less virtual runtime — i.e., if another thread has been waiting longer and is more "behind" on CPU time. The CFS uses a red-black tree sorted by virtual runtime to efficiently find the thread that has received the least CPU time. This ensures fairness: all threads with equal priority get approximately equal CPU time over any measurement window. Python's `threading.Thread` creates OS threads with normal priority — they compete for CPU time on equal footing with all other threads in the process, and with other processes' threads.

**Q3 [Interviewer]: If the OS can preempt threads every 4ms, how does the GIL interact with OS scheduling?**

The GIL and the OS scheduler operate at different layers. The OS scheduler is unaware of the GIL — it will preempt a Python thread in the middle of executing bytecode, even while it holds the GIL. When the OS preempts a GIL-holding thread, the GIL is still held — no other Python thread can run Python bytecode, even though the OS has given CPU time to another thread. That other thread will spin or block trying to acquire the GIL until the OS re-schedules the original thread, which then releases the GIL.

CPython added its own GIL release mechanism on top of OS scheduling: every `sys.getswitchinterval()` seconds (5 ms by default), CPython signals the GIL-holding thread to voluntarily release the GIL and give another waiting thread a chance. This prevents one thread from starving all others at the Python level. The interaction produces a pattern: the GIL is held for up to 5 ms, then voluntarily released; but the OS may also preempt between those 5 ms windows. In practice, GIL-switching overhead dominates context-switch cost for CPU-bound multi-threaded Python.

**Q4 [Interviewer]: Airflow workers use multiprocessing. What are the downsides of processes vs threads in that context?**

The main downsides are: startup latency, memory overhead, and communication cost. Forking a process takes ~1–10 ms (copying the parent's address space metadata, setting up the child's page table entries). For a task that runs for 30 minutes, this is negligible. For a task that runs for 100 ms, a 10 ms fork overhead is 10% of execution time — significant if you're scheduling thousands of micro-tasks per hour.

Memory overhead: each forked process gets its own virtual address space. With copy-on-write, pages that are never modified are shared with the parent (zero cost). But pages that the child writes to (its own Python heap allocations, stack frames) are copied. A worker that imports Airflow, SQLAlchemy, and all the DAG code before forking may share most of those pages (they're read-only code and data), but each child accumulates its own heap. With 16 concurrent workers, you might see 16 × 200 MB = 3.2 GB of memory usage, even if most of it is redundant imports.

Communication: task result passing requires serialization. The task must serialize its return value (or exception) to bytes, write to a pipe, and the parent deserializes. For simple return values (a dict, a string), this is negligible. For tasks that produce large DataFrames as return values — this is a serious anti-pattern in Airflow. Airflow's XCom is designed for small metadata, not large data; tasks should write data to external storage (S3, a database) and return only a reference.

**Q5 [Interviewer]: You mentioned context switching costs ~5–50 µs for a process switch. How do you see this in practice in a data pipeline?**

The classic production manifestation is a Spark job that performs thousands of small tasks instead of fewer large tasks. Each Spark task is scheduled by the YARN/Kubernetes/standalone cluster manager, which involves the DAGScheduler submitting task batches to the TaskScheduler, which sends them to executors via Netty (JVM NIO). Within an executor, each task runs as a JVM thread. For tasks that run in 1–10 ms, the scheduling overhead (serializing the task description, network round-trip, executor thread context switch) can be 1–5 ms — 10–50% overhead.

The Spark UI reveals this as "scheduler delay" in the task metrics. When scheduler delay is high relative to task execution time, the fix is to increase partition size (fewer, larger tasks). This is the "right-size your partitions" guidance: a Spark job with 100,000 partitions of 1 KB each runs far slower than a job with 1,000 partitions of 100 KB each, because the 100,000 tasks generate 100,000 × scheduler_overhead = significant wasted time. The rule of thumb is partitions of 100 MB–1 GB, resulting in task durations of 10 seconds to several minutes.

**Q6 [Interviewer]: Design a concurrent Python pipeline that ingests 10,000 Kafka messages per second, transforms each message, and writes to PostgreSQL. Walk me through your concurrency choices.**

I would decompose the pipeline into three stages with different concurrency characteristics:

**Stage 1 — Kafka consumption (I/O-bound):** One thread (or a small thread pool) polls the Kafka consumer for messages. `confluent-kafka-python`'s `poll()` releases the GIL while waiting for the broker response — a single thread can sustain 10,000–50,000 msg/s with efficient batch polling. The consumer deposits messages into an in-memory queue (`queue.Queue`) bounded to 10,000 items — providing backpressure.

**Stage 2 — Transformation (depends on complexity):** If transformations are pure Python (JSON parsing, string manipulation, regex), the GIL is the bottleneck. I'd use a `ProcessPoolExecutor` with 4–8 worker processes. The consumer thread serializes batches of messages (pickle or msgpack) and sends them to the process pool; workers process and return results. For transformations that are vectorizable (arithmetic, type coercions), a single thread with `orjson` (fast JSON) and NumPy is likely sufficient without multiprocessing. The queue between Kafka consumption and transformation provides buffering so the consumer is never blocked by slow transformation.

**Stage 3 — PostgreSQL writes (I/O-bound):** Database writes release the GIL (psycopg2 releases the GIL during `execute()`). I'd use a thread pool of 4–8 threads, each maintaining a database connection from a connection pool (`psycopg2.pool.ThreadedConnectionPool`). Each thread picks up transformed message batches, executes a batched `INSERT ... ON CONFLICT DO UPDATE`, and commits. Batch size of 500–2000 rows per commit is the sweet spot for Postgres write throughput.

**Backpressure and monitoring:** bounded queues between stages prevent memory exhaustion when a downstream stage slows (Kafka fills, the queue hits its limit, the Kafka consumer pauses polling). I'd instrument queue depth, per-stage throughput, and PostgreSQL write latency with `prometheus_client` metrics exposed on a `/metrics` endpoint. At 10,000 msg/s this entire pipeline runs on a single machine; scaling to 100,000+ msg/s would require multiple Python consumer processes (each owning a partition range) or migrating to Spark Structured Streaming.

---

## 17. Common Misconceptions

**"Python threads don't actually run concurrently."**
This is true only for CPU-bound Python bytecode — the GIL prevents two threads from executing Python bytecode at the same time. But threads absolutely run concurrently when doing I/O: one thread can be executing Python while another is blocked in the OS kernel waiting for a socket read to complete. The socket-waiting thread has released the GIL; the Python-executing thread holds it. For I/O-bound work, Python threads provide real concurrency.

**"fork() copies all the parent's memory."**
`fork()` uses copy-on-write (COW). At fork time, the OS creates a new page table for the child that maps to the same physical pages as the parent. Pages are only copied when one of the processes writes to them. A freshly forked Python process that imports the same libraries as the parent shares those library pages in physical RAM — no copy. This is why `multiprocessing.Pool` is not as memory-expensive as it appears: the 16 Python workers sharing 500 MB of read-only library code don't actually consume 16 × 500 MB = 8 GB of RAM.

**"Threads are always better than processes for shared data."**
Threads share data without serialization, which seems efficient. But shared mutable state requires locking, and fine-grained locking in Python is notoriously error-prone (deadlocks, data races). For data pipelines where workers produce independent results, `ProcessPoolExecutor.map()` with immutable input and collected output is often safer and, for CPU-bound work, faster than a lock-heavy threading design.

**"The GIL means Python is single-threaded."**
Python is not single-threaded. Python runs multiple OS threads; they are scheduled by the kernel. The GIL limits how many Python threads execute Python bytecode simultaneously — but threads can still run OS-level operations (I/O, `time.sleep()`, C extension code) concurrently. A Python web server using threads (like Gunicorn's `--worker-class=gthread`) serves many concurrent HTTP requests in parallel, not sequentially — I/O dominates, and the GIL is not a bottleneck.

**"asyncio is faster than threading."**
`asyncio` has lower overhead per "concurrent unit" than threads (no OS thread creation, no kernel scheduler involvement for coroutine switching). But `asyncio` is single-threaded — one coroutine blocking on CPU work blocks all other coroutines. For a workload with thousands of concurrent I/O operations (HTTP requests, DB queries), `asyncio` scales better than threads because thread creation and context switching become the bottleneck. But for tens of concurrent I/O operations, threads are simpler and fast enough.

---

## 18. Performance Reference Card

| Operation | Approximate Cost | Notes |
|---|---|---|
| Thread creation | 50–500 µs | `pthread_create()` + OS thread setup |
| Process creation (fork) | 1–10 ms | Page table setup; COW amortizes memory copy |
| Context switch (thread) | 1–10 µs | Register save/restore; no TLB flush |
| Context switch (process) | 5–50 µs | Register save/restore + TLB flush |
| Lock acquire (uncontended) | 20–100 ns | Single atomic compare-and-swap |
| Lock acquire (contended, 4 threads) | 1–10 µs | OS futex wait + thread wake-up |
| GIL acquire | 2–10 µs | Mutex + potential OS thread yield |
| Queue put/get | 1–5 µs | 2 lock operations + memory alloc |
| Fork + pickle 100 MB | 100–500 ms | Dominant cost: serialization, not fork |
| `asyncio` task switch | ~1 µs | No OS involvement |
| `time.sleep(0)` (yield) | ~10 µs | GIL release + OS yield + reacquire |

**Rule of thumb for GIL-bound throughput:**

A Python process with N CPU-bound threads effectively runs at 1-thread speed (all N threads serialize through the GIL). A Python process with N I/O-bound threads effectively provides N× throughput (all N threads can be simultaneously in I/O wait). The crossover point is the fraction of time spent in Python bytecode vs I/O: if a task spends 90% of its time in I/O and 10% executing Python, 10 threads give ~9× throughput (limited by the 10% Python execution serialized through the GIL).

---

## 19. Connections to Other Modules

**CSF-ALG-101 M02 (Hash Tables):** The OS process table is a hash table mapping PIDs to PCBs. The kernel's hash function is `pid % table_size`, with chaining for collisions. `kill -9 <PID>` is a hash table lookup by PID to find the PCB, then a signal delivery operation.

**CSF-ALG-101 M04 (Sorting Algorithms):** The Linux CFS scheduler maintains threads in a red-black tree (a self-balancing BST) sorted by virtual runtime. The thread with the smallest virtual runtime is always at the leftmost node — O(log N) lookup for "which thread to schedule next." The scheduler uses the same B-tree concepts as database indexes.

**CSF-ALG-101 M05 (Graphs and DAGs):** The Airflow scheduler uses Kahn's algorithm to determine runnable tasks. Each runnable task becomes a process (LocalExecutor) or a Celery job (CeleryExecutor). The graph determines WHAT runs; this module determines HOW it runs (process vs thread model).

**CSF-OS-101 M02 (Process Scheduling):** Goes deeper into the CFS scheduler, priority queues, and how the OS decides which thread gets CPU time. M01 says "the OS schedules threads"; M02 explains exactly how.

**CSF-OS-101 M03 (Virtual Memory and Page Cache):** Explains virtual address spaces, page tables, TLB flushing on context switch, and copy-on-write in detail. M01 says "processes have virtual address spaces and COW"; M03 explains the mechanism.

**DE-ORC-101 (Airflow):** Airflow's LocalExecutor, CeleryExecutor, KubernetesExecutor are all implementations of the process/thread model decisions in this module. Understanding processes is essential for diagnosing zombie task accumulation, OOM kills, and fork-safety issues in production Airflow.

**DCS-SPK-101 (Spark Architecture):** Spark's executor concurrency model (JVM threads, no GIL, true parallelism) is the primary reason Spark is written in Scala/Java rather than Python. PySpark's Python worker process model, Pandas UDFs, and Arrow serialization are all GIL workarounds described in this module.

---

## 20. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is a process? | An instance of a running program with its own isolated virtual address space, file descriptors, and CPU state. The OS's unit of isolation. |
| 2 | What is a thread? | An independent execution context within a process. Threads share the same address space (heap, globals, file descriptors) but have their own stack and registers. |
| 3 | What system call creates a new process on Linux? | `fork()` — creates an exact copy of the calling process. Returns 0 in child, child PID in parent. Usually followed by `exec()` to load a new program. |
| 4 | What is copy-on-write (COW)? | After `fork()`, parent and child share the same physical pages. A page is only copied when one of the processes writes to it. Read-only pages are never copied. |
| 5 | What is the GIL? | Global Interpreter Lock — a mutex in CPython ensuring only one thread executes Python bytecode at a time. Prevents data races on reference counts. |
| 6 | Does the GIL prevent I/O-bound concurrency? | No. When a thread makes a blocking I/O call (socket read, file read, `time.sleep()`), it releases the GIL. Other threads can execute Python during the I/O wait. |
| 7 | Does the GIL prevent CPU-bound concurrency? | Yes. Multiple Python threads doing CPU-bound Python code are serialized through the GIL — total throughput is similar to one thread. Use `multiprocessing` instead. |
| 8 | How do NumPy/Pandas bypass the GIL? | They are C extensions that explicitly release the GIL (`Py_BEGIN_ALLOW_THREADS`) for bulk array operations. Multiple threads can run NumPy math in parallel. |
| 9 | What is a context switch? | The OS saving the CPU state (registers, instruction pointer, stack pointer) of one thread and restoring the CPU state of another. Costs 1–50 µs depending on type. |
| 10 | Why is a process context switch more expensive than a thread context switch? | Process switch requires flushing the TLB (translation lookaside buffer) because the new process has a different virtual→physical address mapping. Thread switch within the same process does not require TLB flush. |
| 11 | What is a zombie process? | A process that has exited but whose PCB (process control block) is still in the kernel because its parent has not yet called `wait()` to read its exit code. |
| 12 | When should you use `multiprocessing` instead of `threading`? | For CPU-bound Python work (GIL prevents thread parallelism), and when crash isolation is needed (one process crash doesn't kill others). |
| 13 | When should you use `threading` instead of `multiprocessing`? | For I/O-bound work (GIL released during I/O), when threads need to share data with low latency (no serialization), and when process creation overhead is too high. |
| 14 | How does a Spark executor achieve parallelism without the GIL? | Spark executors are JVM processes. The JVM has no global interpreter lock — multiple JVM threads run truly in parallel on separate CPU cores. Python UDFs run in separate Python worker processes. |
| 15 | What is `sys.getswitchinterval()`? | The maximum time a Python thread holds the GIL before CPython signals it to release (default: 5 ms). After this interval, another waiting thread can request the GIL. |
| 16 | What is a data race? | When two or more threads access shared memory concurrently, at least one access is a write, and there is no synchronization. The result is undefined — the final value depends on the timing of thread execution. |
| 17 | How does `multiprocessing.Value` differ from a regular variable? | `multiprocessing.Value` allocates memory in a shared memory segment visible to multiple processes. A regular variable is copied to each process on fork (COW) and modifications in one process are invisible to others. |
| 18 | Why do PySpark Python UDFs run slower than Scala UDFs? | Python UDFs require: (1) JVM→Python IPC (socket, serialization) for each row or batch, (2) Python GIL during execution. Scala UDFs run as JVM bytecode in the executor's thread pool with no serialization or GIL. |
| 19 | What is `asyncio` and when is it better than threads? | `asyncio` is Python's cooperative multitasking framework — coroutines (async functions) yield control at `await` points. Better than threads for very high I/O concurrency (10,000+ simultaneous connections) because it avoids OS thread scheduling overhead. Worse for CPU-bound work (one blocking coroutine blocks all others). |
| 20 | What is the `multiprocessing` start method and why does it matter? | On macOS (Python ≥ 3.8) and Windows, the default is `spawn` (starts a fresh Python interpreter). On Linux, the default is `fork` (copies the parent process). `fork` is faster but unsafe if the parent has open file descriptors or non-fork-safe resources (database connections, threading locks). |

---

## 21. Module Summary

A **process** is the OS's unit of isolation: private virtual address space, private file descriptors, crash independence. A **thread** is the OS's unit of execution: shared heap within one process, its own stack and registers. Threads are cheaper to create and communicate faster (no serialization), but share crash fate and require synchronization.

The **GIL** is CPython's global mutex: at most one thread executes Python bytecode at a time. For **I/O-bound** work (network, disk, database), threads are effective — the GIL is released during blocking I/O calls, allowing concurrent Python execution during waits. For **CPU-bound** work (pure Python computation), threads are counterproductive — use `multiprocessing` (separate processes, each with its own GIL) or vectorize with NumPy/Pandas (C-level operations that explicitly release the GIL).

**Context switching** costs 1–10 µs per thread switch and 5–50 µs per process switch. For tasks with durations comparable to these costs (sub-millisecond), scheduling overhead dominates — prefer fewer, larger tasks.

In data engineering: **Spark** solves the GIL problem by running computation in JVM threads (no GIL, true parallelism); PySpark Python UDFs run in separate Python worker processes with Arrow batching to minimize IPC overhead. **Airflow** uses process-per-task for task isolation; the scheduler is multi-threaded (I/O-bound operations benefit from threading). **Kafka** Python consumers use threads for I/O-bound message fetching; CPU-bound message processing requires multiple consumer processes. **Dask** uses Python multiprocessing to achieve parallelism without a JVM dependency.

The fundamental question for any Python concurrency decision: is the work CPU-bound or I/O-bound? I/O-bound → threads or asyncio. CPU-bound → processes or C-backed vectorization.

---

**CSF-OS-101: 1 of 5 complete.**  
**Next: CSF-OS-101 M02 — Process Scheduling**  
*(CFS scheduler, priority queues, nice values, CPU affinity, scheduler impact on Spark task throughput)*
