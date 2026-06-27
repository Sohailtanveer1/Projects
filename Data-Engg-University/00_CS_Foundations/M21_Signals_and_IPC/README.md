# CSF-OS-101 M05: Signals and IPC

**Course:** CSF-OS-101 — Operating System Internals for Data Engineers  
**Module:** 05 of 05  
**Filesystem position:** 00_CS_Foundations/M21_Signals_and_IPC  
**Prerequisites:** CSF-OS-101 M01 (Processes and Threads), M04 (File I/O and System Calls)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: Signals](#4-core-theory-signals)
5. [Signal Handlers and Safe Handling](#5-signal-handlers-and-safe-handling)
6. [Graceful Shutdown in Data Systems](#6-graceful-shutdown-in-data-systems)
7. [Pipes: The Simplest IPC](#7-pipes-the-simplest-ipc)
8. [Unix Domain Sockets](#8-unix-domain-sockets)
9. [Shared Memory and Memory-Mapped IPC](#9-shared-memory-and-memory-mapped-ipc)
10. [Message Queues and Event Loops](#10-message-queues-and-event-loops)
11. [Mental Models](#11-mental-models)
12. [Failure Scenarios](#12-failure-scenarios)
13. [Data Engineering Connections](#13-data-engineering-connections)
14. [Code Toolkit](#14-code-toolkit)
15. [Hands-On Labs](#15-hands-on-labs)
16. [Interview Q&A](#16-interview-qa)
17. [Cross-Question Chain](#17-cross-question-chain)
18. [Common Misconceptions](#18-common-misconceptions)
19. [Performance Reference Card](#19-performance-reference-card)
20. [Connections to Other Modules](#20-connections-to-other-modules)
21. [Flashcards](#21-flashcards)
22. [Module Summary](#22-module-summary)

---

## 1. The Problem This Module Solves

An Airflow worker is executing a long-running Spark job submission when Kubernetes decides to reschedule the pod. The platform sends SIGTERM to the worker process. The worker has 30 seconds before SIGKILL arrives. If the worker ignores SIGTERM and continues running, SIGKILL forcibly terminates it mid-execution — the Spark application it launched may keep running without any parent process to monitor it, orphaning the job. If the worker handles SIGTERM correctly, it cancels the Spark application, updates the task state to "failed" in the metadata database, and exits cleanly. Whether this goes right or wrong depends entirely on whether the worker registered a SIGTERM handler.

A Spark executor reports "lost" during a job. The driver sees the executor as gone, resubmits its tasks, and the job completes. But why did the executor die? It received SIGKILL from the OS out-of-memory killer (OOM killer) — the executor consumed more memory than the container allowed, so the kernel killed it instantly with no warning. SIGKILL cannot be caught or ignored; there was no graceful cleanup, no log flush, no "I'm dying" message. The entire incident is invisible unless you know to check `dmesg | grep "Out of memory"`.

A Python multiprocessing pipeline uses a `Queue` to pass records between a reader process and a writer process. Under low load it works perfectly. Under high load the writer falls behind, the queue grows unboundedly, and the reader eventually hits a `MemoryError` because the queue is backed by a Unix pipe which has been exhausted, or the process heap is full. The fix requires understanding that pipes have a kernel buffer (typically 64 KB on Linux) and that writing to a full pipe blocks — or that a `multiprocessing.Queue` with `maxsize=0` has no backpressure at all.

A team replaces a Python subprocess-based inter-process data handoff with a Unix domain socket and sees latency drop from 2 ms to 50 µs. The previous implementation serialized data to a temp file, then the second process read the file — two fsyncs and two file opens per record. The Unix domain socket is a direct kernel-to-kernel buffer copy with no disk I/O at all.

All of these are signals and IPC problems. This module explains what signals are, how to handle them correctly, how graceful shutdown works in Airflow, Spark, and Kafka, and how processes communicate via pipes, Unix domain sockets, shared memory, and in-memory queues.

---

## 2. How to Use This Module

**For production incident response:** Sections 4, 6, 12. When a process dies unexpectedly, signals are almost always involved — Section 4 explains which signals mean what. Section 6 shows the exact graceful shutdown sequences for Airflow, Spark, and Kafka. Section 12 covers OOM kills, SIGPIPE failures, zombie processes, and the broken pipe error that appears in Spark logs.

**For interview prep:** Sections 4, 5, 8, 16, 17. Signal semantics, signal safety, and IPC mechanisms are canonical OS interview questions. The chain in Section 17 starts at "what is a signal" and escalates to "design a graceful shutdown protocol for a distributed compute cluster."

**For system design:** Sections 7–10, 13. The IPC mechanism selection — pipe vs Unix socket vs shared memory — determines latency, throughput, and complexity. Section 13 maps each IPC type to its use in real data engineering systems.

---

## 3. Prerequisites Check

- **Processes (M01):** Signals are delivered to processes. The PCB (task_struct) contains the signal mask and pending signal set. Understanding what a process is, what its PID is, and the fork/exec model is necessary.
- **File descriptors (M04):** Pipes and Unix domain sockets are file descriptors. The read/write system calls work on them identically to regular files. Understanding fd tables and the VFS is required for Section 7 and Section 8.
- **Threads (M01):** Signal delivery to multithreaded processes has nuances — signals can be directed to a specific thread or to the process, and the signal mask is per-thread. Section 4.4 covers this.

---

## 4. Core Theory: Signals

### 4.1 What Is a Signal?

A **signal** is a limited, asynchronous notification sent to a process (or thread) by the kernel, by another process, or by the process itself. Signals are the Unix mechanism for:
- Notifying a process that something exceptional happened (segfault, divide by zero, broken pipe)
- Sending an administrative notification from one process to another (please terminate, please reload config, please dump your state)
- Delivering timer expiry notifications (SIGALRM)

Signals are asynchronous: a process can be in the middle of executing any instruction when a signal arrives. The kernel delivers the signal by suspending the process at its current instruction, saving its registers, running the signal handler, restoring the registers, and resuming — all transparently from the process's perspective (unless the handler changes the process's state).

### 4.2 Signal Numbers and Names

Linux supports 64 signals. The most important for data engineering:

| Signal | Number | Default action | Meaning |
|---|---|---|---|
| `SIGHUP` | 1 | Terminate | Terminal closed; often used to trigger config reload |
| `SIGINT` | 2 | Terminate | Interrupt from keyboard (Ctrl+C) |
| `SIGQUIT` | 3 | Core dump | Quit from keyboard (Ctrl+\\); generates core file |
| `SIGILL` | 4 | Core dump | Illegal instruction (corrupt binary, wrong architecture) |
| `SIGABRT` | 6 | Core dump | Process called abort() — assertion failure, C++ exception termination |
| `SIGFPE` | 8 | Core dump | Floating-point exception (divide by zero, overflow) |
| `SIGKILL` | 9 | Terminate | **Cannot be caught, blocked, or ignored** — unconditional kill |
| `SIGSEGV` | 11 | Core dump | Segmentation fault — invalid memory access |
| `SIGPIPE` | 13 | Terminate | Write to a pipe with no readers |
| `SIGALRM` | 14 | Terminate | Timer expired (set via alarm()) |
| `SIGTERM` | 15 | Terminate | Termination request — the standard polite "please exit" |
| `SIGCHLD` | 17 | Ignore | Child process state changed (exited, stopped, continued) |
| `SIGCONT` | 18 | Continue | Resume a stopped process |
| `SIGSTOP` | 19 | Stop | **Cannot be caught, blocked, or ignored** — unconditional pause |
| `SIGTSTP` | 20 | Stop | Stop from keyboard (Ctrl+Z); can be caught |
| `SIGUSR1` | 10 | Terminate | User-defined signal 1 — available for application use |
| `SIGUSR2` | 12 | Terminate | User-defined signal 2 — available for application use |
| `SIGWINCH` | 28 | Ignore | Terminal window size changed |

**The two signals that cannot be caught or ignored: SIGKILL (9) and SIGSTOP (19).** These are the kernel's final authority. No signal handler can intercept them. This is why `kill -9 <pid>` always terminates a process — even one that has blocked all other signals.

### 4.3 Signal Delivery Mechanics

```
Signal delivery sequence:

1. Signal is generated:
   - By kernel: SIGSEGV (invalid memory access detected by hardware MMU)
   - By another process: kill(pid, SIGTERM)
   - By the process itself: raise(SIGINT) or kill(getpid(), SIGTERM)
   
2. Signal is pending:
   The signal is added to the process's pending signal set (in task_struct).
   If the signal is currently blocked by the signal mask, it stays pending
   until unblocked. Signals are not queued (with exceptions) — sending SIGTERM
   twice while it's blocked results in ONE pending SIGTERM, not two.
   
   Exception: real-time signals (SIGRTMIN to SIGRTMAX) ARE queued.
   Standard signals (1–31) are NOT queued — multiple deliveries of the same
   standard signal before it's handled appear as a single pending signal.

3. Signal is delivered:
   The kernel delivers the signal when the process transitions from kernel
   mode to user mode (on return from a syscall or after an interrupt).
   
   The kernel checks: is this signal blocked? If yes → stays pending.
   If no → execute the registered action:
     a. Default action (SIG_DFL): terminate, core dump, stop, or ignore
     b. Ignore (SIG_IGN): discard the signal
     c. User handler: save process context, jump to signal handler function
     
4. Handler execution:
   The kernel sets up a signal stack frame in the process's user-space stack.
   The process starts executing the signal handler at the registered function pointer.
   The handler runs in user mode — it can call user-space functions but with restrictions.
   When the handler returns, the kernel restores the saved context (via sigreturn syscall).
   The process continues from where it was interrupted.
```

### 4.4 Signal Masks and Thread Delivery

Every thread has its own **signal mask** — a set of signals that are currently blocked for that thread. Blocked signals are not delivered until unblocked; they remain pending.

```python
import signal
import threading

# Block SIGTERM in a worker thread
# (prevent SIGTERM from being delivered to worker threads)
def worker():
    # Block SIGTERM in this thread — delivery deferred to main thread
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
    while True:
        do_work()   # SIGTERM won't interrupt us here

# In a multithreaded process, a signal sent to the process (kill(pid, SIGTERM))
# can be delivered to ANY thread that has the signal unblocked.
# Convention: block signals in all worker threads, handle in main thread only.
```

**Signal delivery to a multithreaded process:**
- A signal generated for the process (e.g., `kill(pid, SIGTERM)`) is delivered to one unblocked thread — the kernel picks any eligible thread.
- A signal generated for a specific thread (hardware exceptions like SIGSEGV, or `pthread_kill(tid, sig)`) is delivered to that specific thread.
- Best practice: block all signals in worker threads and have a dedicated signal-handling thread (or the main thread) call `sigwait()` or use `signalfd` to receive and handle signals synchronously.

### 4.5 `signalfd` — Signals as File Descriptors

`signalfd()` creates a file descriptor that receives signals as readable data. Instead of asynchronous signal handlers (which have the reentrancy restrictions covered in Section 5), signals are read synchronously from the fd using `read()` or `epoll`:

```python
import signal
import os
import struct

# Block SIGTERM and SIGINT from async delivery — we'll read them via signalfd
signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})

# Create a signalfd — signals arrive as readable data on this fd
sfd = signal.set_wakeup_fd(os.open("/dev/null", os.O_WRONLY))
# (Python's built-in wakeup fd mechanism uses a similar concept)

# In C/system code:
# sfd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
# struct signalfd_siginfo info;
# read(sfd, &info, sizeof(info));   // blocks until signal arrives
# → info.ssi_signo contains the signal number
# Integrate with epoll: register sfd with epoll, read on EPOLLIN event
```

`signalfd` is the modern approach for server applications: register it with epoll alongside sockets and pipes. The event loop handles signals, I/O, and timers in a single select/epoll loop without async signal handlers.

---

## 5. Signal Handlers and Safe Handling

### 5.1 Async-Signal Safety

Signal handlers are **asynchronous** — they can interrupt the process at any point, including in the middle of a function like `malloc()`, `printf()`, or Python's garbage collector. If the signal handler calls the same non-reentrant function that was interrupted, the result is undefined behavior (heap corruption, deadlock, etc.).

A function is **async-signal-safe** if it can be safely called from a signal handler. The POSIX standard defines a specific set of async-signal-safe functions. The most important for data engineering:

```
Async-signal-safe (safe to call from signal handlers):
  write()          — raw I/O to a file descriptor
  read()
  open(), close()
  kill()           — send a signal to another process
  getpid()         — get current PID
  _exit()          — terminate without cleanup (NOT exit())
  sigaction()      — install a signal handler
  sigprocmask()    — modify signal mask
  
NOT async-signal-safe (do NOT call from signal handlers):
  malloc() / free()   — heap allocator uses non-reentrant locks
  printf() / fprintf()  — uses buffered I/O, non-reentrant
  Python ANY Python function — CPython's GIL and internal state are not signal-safe
  logging.info()    — Python logging is not signal-safe
  database.commit() — obviously not
```

### 5.2 The Signal Flag Pattern

The correct Python pattern for signal handling: set a flag in the signal handler, and check the flag in the main loop. The handler does nothing except set a simple boolean — no Python object creation, no I/O, no locks.

```python
import signal
import time
import threading

# Thread-safe: use threading.Event for multi-threaded shutdown coordination
shutdown_event = threading.Event()

def handle_sigterm(signum: int, frame) -> None:
    """
    Signal handler — MUST be fast and simple.
    
    Safe operations: set a flag, write to a pre-opened fd, send a signal.
    Unsafe: any Python function that allocates memory, acquires locks, or does I/O.
    
    The main loop checks shutdown_event and performs the actual cleanup.
    """
    # threading.Event.set() is implemented as a mutex + condition variable notify.
    # Technically not async-signal-safe in POSIX terms.
    # In CPython, signal handlers run between bytecodes (not truly asynchronous)
    # so this pattern works in practice for Python programs.
    shutdown_event.set()

# Register the handler
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)   # also handle Ctrl+C

# Main processing loop
def main_loop():
    print("Worker started, PID:", os.getpid())
    while not shutdown_event.is_set():
        try:
            process_next_batch()
        except Exception as e:
            log.error(f"Batch failed: {e}")
            # Continue running — single batch failure doesn't stop the worker
    
    # Signal received — graceful shutdown
    print("Shutdown signal received, cleaning up...")
    finish_current_batch_if_safe()
    flush_output_buffers()
    close_database_connections()
    print("Clean exit.")

main_loop()
```

### 5.3 Python's Signal Delivery Mechanism

CPython does not deliver signals asynchronously like C programs do. Instead:
- The OS delivers the signal to the Python interpreter process
- The interpreter sets a flag in a per-process C-level variable (`Handlers[signum].tripped = 1`)
- Between every N bytecodes (the "check interval," `sys.getswitchinterval()`), the interpreter checks this flag
- If the flag is set, the interpreter calls the registered Python handler function

This means Python signal handlers do NOT run "between CPU instructions" like C signal handlers — they run between Python bytecodes. The handler runs in the main thread, in Python's normal execution context. This is why Python signal handlers are much safer than C signal handlers: they run in a controlled state, not in an async interrupt context.

The downside: if the main thread is blocked in a C extension (a long NumPy computation, or `time.sleep(100)`), Python signal handlers are not called until the C extension yields back to the Python interpreter. To handle signals during long C-extension calls, use `signal.set_wakeup_fd()` which writes a byte to a file descriptor when a signal arrives — use this fd with `select()` or `epoll` to wake up the event loop.

---

## 6. Graceful Shutdown in Data Systems

### 6.1 The Shutdown Contract: SIGTERM then SIGKILL

The standard Unix graceful shutdown protocol is a two-phase signal sequence:

```
Phase 1: SIGTERM (can be caught)
  → "Please clean up and exit at your earliest convenience"
  → Application runs its shutdown hook: drain queues, close connections, flush buffers
  → Application exits voluntarily with exit code 0
  
  Timeout: if the application does not exit within T seconds...
  
Phase 2: SIGKILL (cannot be caught)
  → "You are dead, right now, no exceptions"
  → Kernel immediately reclaims all resources (memory, fds, threads)
  → No cleanup code runs
  → Process is gone

Kubernetes default: T = 30 seconds (terminationGracePeriodSeconds)
Systemd default: T = 90 seconds (TimeoutStopSec)
Docker default: T = 10 seconds (docker stop --time=10)
```

The 30-second window is the application's entire budget for cleanup. This budget must be spent wisely:

```
Airflow worker shutdown budget (30s example):
  0s:   SIGTERM received → set shutdown flag
  0-2s: Stop accepting new tasks from the broker
  2-5s: Send SIGTERM to currently-running task subprocess
  5-25s: Wait for task subprocess to finish its graceful shutdown
  25-27s: Mark any still-running tasks as "failed" in the metadata DB
  27-29s: Close DB connections, flush logs
  29s:  Exit(0)
  
  If the task subprocess takes > 20s to shut down,
  Airflow force-terminates it (SIGKILL) at 25s to have time for DB cleanup.
```

### 6.2 Airflow's Shutdown Sequence

Airflow uses a multi-process architecture: the Scheduler process, Worker processes (one per worker node), and Task Runner subprocesses (one per running task). Each layer handles signals independently.

```
Kubernetes sends SIGTERM to the Airflow Worker pod:

1. Worker process receives SIGTERM
   worker._handle_sigterm() is called:
   → Sets self.num_runs = 0 (stop accepting new tasks from broker)
   → Sends SIGTERM to all active task runner subprocesses

2. Task runner subprocess receives SIGTERM:
   → If the task's operator supports it: calls operator.on_kill()
     - SparkSubmitOperator.on_kill(): sends "spark.stop()" to the running Spark app
     - BashOperator.on_kill(): sends SIGTERM to the bash subprocess PID group
     - PythonOperator: the Python function is mid-execution — kills the subprocess
   → Waits for the task process to complete (with a timeout)

3. If task process does not exit within timeout:
   → Worker sends SIGKILL to force-terminate the task
   → Marks task as "failed" (or "up_for_retry" based on retries setting)
   
4. Worker records final task states in metadata DB (PostgreSQL/MySQL)
5. Worker exits cleanly

Airflow configuration for graceful shutdown:
  [scheduler]
  scheduler_zombie_task_threshold = 300  # tasks "lost" after 5 min of no heartbeat
  
  [celery]
  worker_autoscale = 16,4   # max and min workers; SIGTERM drains current tasks
```

**The key pattern:** SIGTERM propagates down the process hierarchy. The worker receives SIGTERM from Kubernetes, sends SIGTERM to task runners, which send SIGTERM to their subprocesses (Spark, bash, etc.). Each layer has a timeout after which it escalates to SIGKILL.

### 6.3 Spark's Shutdown Sequence

Spark has a multi-JVM architecture: Driver (one JVM), Executors (one JVM per executor). Shutdown is coordinated via the SparkContext and its registered shutdown hooks.

```
Graceful shutdown of a Spark application:

Method 1: sparkContext.stop() (programmatic)
  1. Driver calls SparkContext.stop()
  2. SparkContext sends "StopExecutors" message to all executors via Akka/Netty RPC
  3. Each executor: flushes metrics, closes shuffle connections, exits JVM
  4. Driver de-registers from resource manager (YARN: unregister application)
  5. Driver closes all connections to shuffle service, history server
  6. Driver JVM exits (triggers Java shutdown hooks)

Method 2: SIGTERM to Driver JVM
  1. JVM receives SIGTERM
  2. JVM calls registered shutdown hooks in reverse registration order
  3. Spark registers a shutdown hook in SparkContext initialization:
     Runtime.getRuntime.addShutdownHook(new Thread {
       override def run(): Unit = SparkContext.stop()
     })
  4. Same sequence as Method 1

Method 3: SIGKILL to Executor (OOM kill by container manager)
  1. Executor JVM is killed with no warning (cannot catch SIGKILL)
  2. Executor's threads die mid-execution
  3. Executor's shuffle files may be incomplete
  4. Driver detects executor loss via heartbeat timeout
  5. Driver re-schedules all tasks that were running on the lost executor
  6. Shuffle data from lost executor is re-computed (unless external shuffle service)

Executor OOM kill pattern:
  Kubernetes: memory.limit exceeded → cgroup memory controller → OOM kill
  YARN: nodemanager monitors container memory → kills container if limit exceeded
  
  Detection in logs:
  Driver: "Lost executor N on host X: Executor heartbeat timed out after 120000 ms"
  dmesg: "Out of memory: Kill process <pid> (java) score 847 or sacrifice child"
  
  Fix: increase spark.executor.memory + spark.executor.memoryOverhead
```

### 6.4 Kafka's Graceful Shutdown

Kafka brokers and consumer clients have distinct shutdown sequences:

```
Kafka Broker graceful shutdown:
  1. SIGTERM received by broker JVM
  2. JVM shutdown hook triggers KafkaServer.shutdown():
  3. Controller: if this broker is the partition leader for any partitions,
     trigger a controlled leader election — elect a new leader for each
     partition before going offline. This prevents consumer disruption.
  4. Log manager: flush all dirty log segments to disk (fdatasync)
  5. Close network connections to all clients and other brokers
  6. Write the "clean shutdown" file to log.dirs → on restart, skip log recovery
  7. Exit JVM
  
  Key timing: step 3 (leader election propagation) takes 1-5 seconds.
  If SIGKILL arrives before step 3: consumers see a brief gap until the controller
  detects the dead broker (controlled.shutdown.enable=true protects against this).

Kafka Consumer graceful shutdown (Python: confluent-kafka):
  # The shutdown pattern
  running = True
  
  def handle_shutdown(signum, frame):
      global running
      running = False
  
  signal.signal(signal.SIGTERM, handle_shutdown)
  
  consumer = Consumer({'bootstrap.servers': 'localhost:9092',
                        'group.id': 'my-consumer-group',
                        'enable.auto.commit': False})
  consumer.subscribe(['my-topic'])
  
  try:
      while running:
          msg = consumer.poll(timeout=1.0)
          if msg is None:
              continue
          process(msg)
          consumer.commit()   # commit after processing
  finally:
      # This is critical: consumer.close() sends a LeaveGroup request
      # to the broker, triggering immediate rebalance.
      # Without close(): broker waits for session.timeout.ms (default 45s)
      # before detecting the consumer is gone and triggering rebalance.
      consumer.close()
  
  # consumer.close() behavior:
  # 1. Sends LeaveGroup request to broker → immediate rebalance
  # 2. Commits final offsets for auto-commit enabled consumers
  # 3. Closes all TCP connections
  # Result: other consumers in the group get the partitions within ~1-2 seconds
  # instead of waiting up to 45 seconds for session timeout
```

**The cost of missing consumer.close():** Without `consumer.close()`, the broker detects the consumer as dead only after `session.timeout.ms` (default 45 seconds) elapses with no heartbeat. For the 45-second window, the partitions assigned to the dead consumer are orphaned — no consumer is processing those partitions. Messages accumulate. `consumer.close()` triggers the LeaveGroup protocol, which causes an immediate rebalance, typically completing in 1–3 seconds.

---

## 7. Pipes: The Simplest IPC

### 7.1 What Is a Pipe?

A pipe is a unidirectional, kernel-buffered byte stream connecting two file descriptors — one for writing and one for reading. Data written to the write end becomes available to read from the read end. Pipes are the simplest form of IPC: they require no configuration, no addressing, and no network stack.

```
pipe() syscall:
  int fds[2];
  pipe(fds);
  // fds[0]: read end
  // fds[1]: write end
  
  Data flow: write(fds[1], data, len) → kernel buffer → read(fds[0], buf, len)
  
  Properties:
  - Unidirectional (write → read only; for bidirectional use two pipes)
  - Byte-stream (no message boundaries — read() may return partial writes)
  - Kernel buffer: 64 KB on Linux (configurable via F_SETPIPE_SZ up to /proc/sys/fs/pipe-max-size)
  - Blocking: write() blocks when buffer full; read() blocks when buffer empty
  - Close semantics: reading from a pipe with no writers returns EOF (read() = 0)
                     writing to a pipe with no readers raises SIGPIPE
```

### 7.2 Pipes in the Shell and in Python

```bash
# Shell pipes: stdout of grep → stdin of wc via a kernel pipe
grep "ERROR" spark.log | wc -l
# The shell creates a pipe, forks grep and wc,
# grep writes to the pipe's write end (its stdout),
# wc reads from the pipe's read end (its stdin)
```

```python
import subprocess
import os

# Python subprocess with pipe
result = subprocess.run(
    ["grep", "ERROR", "spark.log"],
    capture_output=True,   # creates pipes for stdout and stderr
    text=True
)
print(result.stdout)

# Manual pipe creation between two processes
r_fd, w_fd = os.pipe()   # create pipe; returns (read_fd, write_fd)

pid = os.fork()
if pid == 0:
    # Child: producer
    os.close(r_fd)   # child doesn't need read end
    os.write(w_fd, b"hello from child\n")
    os.close(w_fd)   # closing write end → EOF signal to reader
    os._exit(0)
else:
    # Parent: consumer
    os.close(w_fd)   # parent doesn't need write end
    data = os.read(r_fd, 1024)
    print("Parent received:", data)
    os.close(r_fd)
    os.waitpid(pid, 0)   # reap child to avoid zombie
```

### 7.3 Python `multiprocessing.Pipe` and `Queue`

```python
from multiprocessing import Process, Pipe, Queue
import time

# ── Pipe ──────────────────────────────────────────────────────────────────────
# multiprocessing.Pipe() wraps two file descriptors in Connection objects.
# Connection.send() serializes with pickle then writes to the pipe fd.
# Connection.recv() reads from the pipe fd and unpickles.

def producer(conn):
    for i in range(10):
        conn.send({"record_id": i, "value": i * 2})
    conn.close()

def consumer(conn):
    while True:
        try:
            record = conn.recv()
            print(f"Processing: {record}")
        except EOFError:
            break   # producer closed its end

parent_conn, child_conn = Pipe()
p = Process(target=producer, args=(child_conn,))
p.start()
child_conn.close()   # parent doesn't need the child end
consumer(parent_conn)
p.join()

# ── Queue ─────────────────────────────────────────────────────────────────────
# multiprocessing.Queue is built on a Pipe + background feeder thread.
# Put: serializes with pickle → background thread writes to pipe
# Get: reads from pipe → deserializes with pickle
#
# CRITICAL: maxsize=0 means UNBOUNDED queue.
# Without backpressure, a slow consumer + fast producer = unbounded memory growth.
# Always set maxsize based on your memory budget.

QUEUE_MAXSIZE = 1000   # backpressure: producer blocks when queue is full

q = Queue(maxsize=QUEUE_MAXSIZE)

def fast_producer(q: Queue) -> None:
    for i in range(100_000):
        q.put({"id": i})   # blocks when queue full → natural backpressure
    q.put(None)   # sentinel: signal consumer to stop

def slow_consumer(q: Queue) -> None:
    while True:
        item = q.get()
        if item is None:
            break
        time.sleep(0.0001)   # simulate slow processing
        process(item)

p_prod = Process(target=fast_producer, args=(q,))
p_cons = Process(target=slow_consumer, args=(q,))
p_prod.start()
p_cons.start()
p_prod.join()
p_cons.join()
```

### 7.4 Pipe Kernel Buffer and Backpressure

The pipe kernel buffer is **64 KB by default** on Linux. When the buffer is full:
- `write()` to the pipe blocks (or returns EAGAIN if non-blocking)
- The producer is naturally throttled to the consumer's processing rate

This is backpressure at the OS level — no application code needed. However, `multiprocessing.Queue` uses a background feeder thread: the `put()` call writes to an in-process deque (the feeder thread's queue), and the feeder thread writes to the underlying pipe. With `maxsize=0` (unbounded), the in-process deque can grow without limit before the pipe backpressure kicks in. The `maxsize` parameter limits the total size of the in-process deque, providing application-level backpressure.

---

## 8. Unix Domain Sockets

### 8.1 What Is a Unix Domain Socket?

A Unix domain socket (UDS) is a socket that communicates within the local machine using a file-system path as its address, rather than an IP address and port. It uses the same `socket()`, `bind()`, `connect()`, `send()`, `recv()`, `accept()` API as TCP sockets but all data transfer happens within the kernel — no network stack, no TCP/IP overhead, no loopback interface.

```
Unix domain socket vs TCP loopback comparison:

TCP (127.0.0.1):
  send() → TCP stack → IP stack → loopback interface → IP stack → TCP stack → recv()
  Overhead: TCP header, checksum, ACK, congestion control, loopback processing
  Latency: ~30–100 µs per round-trip
  Max throughput: ~10-20 GB/s (limited by TCP stack overhead)

Unix domain socket:
  send() → kernel buffer copy → recv()
  Overhead: single kernel memory copy (or zero-copy with SCM_RIGHTS)
  Latency: ~5–20 µs per round-trip
  Max throughput: ~50-100 GB/s (RAM bandwidth limited)
```

UDS supports two modes:
- **SOCK_STREAM:** byte-stream, like TCP — no message boundaries
- **SOCK_DGRAM:** datagram, like UDP — preserves message boundaries, no connection, delivery guaranteed within the local kernel

### 8.2 UDS in Data Engineering

```python
import socket
import os
import threading

SOCKET_PATH = "/tmp/data_pipeline.sock"

# ── Server ────────────────────────────────────────────────────────────────────

def uds_server():
    """Receives records from client processes via Unix domain socket."""
    # Remove stale socket file if it exists
    if os.path.exists(SOCKET_PATH):
        os.unlink(SOCKET_PATH)
    
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(SOCKET_PATH)
    server.listen(5)
    os.chmod(SOCKET_PATH, 0o666)   # allow other users to connect
    
    print(f"Server listening on {SOCKET_PATH}")
    
    while True:
        conn, _ = server.accept()
        t = threading.Thread(target=handle_client, args=(conn,))
        t.daemon = True
        t.start()

def handle_client(conn: socket.socket) -> None:
    """Handle one client connection: receive records until disconnected."""
    try:
        buf = b""
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break   # client disconnected
            buf += chunk
            # Process complete newline-delimited records
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                process_record(line)
    finally:
        conn.close()

# ── Client ────────────────────────────────────────────────────────────────────

def uds_client(records: list[bytes]) -> None:
    """Send records to the server via Unix domain socket."""
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(SOCKET_PATH)
    try:
        for record in records:
            client.sendall(record + b"\n")   # newline-delimited protocol
    finally:
        client.close()

# ── Datagram (for small, fixed-size messages) ─────────────────────────────────

def uds_metrics_sender(metric: dict) -> None:
    """
    Send a metric to a monitoring server via SOCK_DGRAM.
    DGRAM preserves message boundaries — no framing needed.
    Used by: StatsD, Prometheus node_exporter, Spark internal metrics.
    """
    import json
    client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    payload = json.dumps(metric).encode()
    # sendto to the server's socket path (no connect() needed for DGRAM)
    client.sendto(payload, "/var/run/metrics.sock")
    client.close()
```

### 8.3 File Descriptor Passing via UDS

Unix domain sockets have a unique capability: passing open file descriptors between processes using ancillary data (`SCM_RIGHTS`). This is used by:
- **systemd socket activation:** systemd opens a socket, passes the fd to the service process on startup — zero-downtime restarts
- **Chrome's sandboxed renderer:** the browser process passes specific fds to sandboxed renderers instead of giving them full file access
- **Spark external shuffle service:** the shuffle service can pass pre-opened data file fds to executors for zero-copy reads

```python
import socket
import struct
import array
import os

def send_fd(sock: socket.socket, fd: int) -> None:
    """Send a file descriptor over a Unix domain socket."""
    # SCM_RIGHTS ancillary data: pass the fd integer to another process
    # The receiving process gets a NEW fd number pointing to the SAME kernel file object
    ancdata = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))]
    sock.sendmsg([b'\x00'], ancdata)   # dummy byte required

def recv_fd(sock: socket.socket) -> int:
    """Receive a file descriptor from a Unix domain socket."""
    fds_buf = array.array('i', [0])   # buffer for one fd
    cmsg_space = socket.CMSG_SPACE(fds_buf.buffer_info()[1] * fds_buf.itemsize)
    msg, ancdata, flags, addr = sock.recvmsg(1, cmsg_space)
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            fds_buf.frombytes(cmsg_data[:fds_buf.itemsize])
            return fds_buf[0]
    raise RuntimeError("No file descriptor received")

# Example: parent opens a file, passes fd to child (no filename needed)
parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)

if os.fork() == 0:
    # Child: receive fd and read from it
    child_sock.close()
    received_fd = recv_fd(parent_sock)
    data = os.read(received_fd, 100)
    print(f"Child read: {data}")
    os.close(received_fd)
    os._exit(0)
else:
    # Parent: open file, send fd to child
    parent_sock.close()
    fd = os.open("/etc/hostname", os.O_RDONLY)
    send_fd(child_sock, fd)
    os.close(fd)
    os.wait()
    child_sock.close()
```

---

## 9. Shared Memory and Memory-Mapped IPC

### 9.1 Why Shared Memory?

Both pipes and Unix sockets require data to be **copied**: the sending process copies data into the kernel, the receiving process copies it out. For large data transfers (gigabytes of DataFrame chunks between processes), these copies are expensive.

**Shared memory** avoids copies entirely: multiple processes map the same physical memory pages into their virtual address spaces. A write by one process is immediately visible to another — no kernel involvement, no copy, no syscall per record. The transfer rate is limited only by DRAM bandwidth (~50 GB/s), not by kernel copy overhead.

```
Memory copy overhead comparison (transferring 1 GB between two processes):

Pipe / Unix socket:
  Process A write() → kernel copy → kernel buffer → kernel copy → Process B read()
  = 2 GB of memory copies
  Time: ~2 GB / 50 GB/s = ~40 ms (memory bandwidth limited)
  + syscall overhead for each write/read chunk

Shared memory:
  Process A writes to mmap'd region → Process B reads from same physical pages
  = 0 copies (data is already in both virtual address spaces)
  Time: ~0 ms additional cost (reads from the same physical RAM)
  + synchronization cost (mutex/semaphore to coordinate access)
```

### 9.2 POSIX Shared Memory

```python
import mmap
import os
import struct
import multiprocessing

# ── Producer: write to shared memory ─────────────────────────────────────────

SHM_SIZE = 100 * 1024 * 1024   # 100 MB shared region

def producer(shm_name: str, ready: multiprocessing.Event,
             consumed: multiprocessing.Event) -> None:
    """Write data into a POSIX shared memory region."""
    # Open shared memory object (created by consumer first)
    # In Python, use /dev/shm/ for POSIX shared memory on Linux
    shm_path = f"/dev/shm/{shm_name}"
    
    # Create the shared memory file
    fd = os.open(shm_path, os.O_CREAT | os.O_RDWR, 0o600)
    os.ftruncate(fd, SHM_SIZE)
    
    # Map it into virtual address space
    mem = mmap.mmap(fd, SHM_SIZE)
    os.close(fd)
    
    # Write data
    data = b"X" * (SHM_SIZE - 8)
    mem.seek(0)
    mem.write(struct.pack("Q", len(data)))   # 8-byte length prefix
    mem.write(data)
    mem.flush()   # ensure writes are visible to other processes
    
    ready.set()      # signal: data is ready
    consumed.wait()  # wait: consumer has read the data
    
    mem.close()
    os.unlink(shm_path)

# ── Consumer: read from shared memory ─────────────────────────────────────────

def consumer(shm_name: str, ready: multiprocessing.Event,
             consumed: multiprocessing.Event) -> None:
    """Read data from a POSIX shared memory region."""
    ready.wait()   # wait for producer to write data
    
    shm_path = f"/dev/shm/{shm_name}"
    fd = os.open(shm_path, os.O_RDONLY)
    mem = mmap.mmap(fd, SHM_SIZE, access=mmap.ACCESS_READ)
    os.close(fd)
    
    mem.seek(0)
    length = struct.unpack("Q", mem.read(8))[0]
    data = mem.read(length)
    print(f"Consumer received {len(data)} bytes")
    
    mem.close()
    consumed.set()   # signal: done reading

# ── Demo ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ready_event = multiprocessing.Event()
    consumed_event = multiprocessing.Event()
    
    p = multiprocessing.Process(
        target=producer, args=("ipc_demo", ready_event, consumed_event)
    )
    c = multiprocessing.Process(
        target=consumer, args=("ipc_demo", ready_event, consumed_event)
    )
    p.start()
    c.start()
    p.join()
    c.join()
```

### 9.3 Apache Arrow Plasma Store and PyArrow IPC

Apache Arrow's Plasma store (now available as `arrow.ipc`) is a shared-memory object store designed for zero-copy data sharing between processes. This is directly how PySpark with Arrow optimization works:

```python
import pyarrow as pa
import pyarrow.ipc as ipc
import io

# ── Arrow IPC: serialize DataFrame to shared memory for zero-copy handoff ─────

# In the JVM (Spark executor or driver):
# Spark uses Arrow IPC format to serialize a partition as a RecordBatch.
# The bytes are written to a shared memory region (or a socket buffer).
# Python receives the bytes and creates an Arrow RecordBatch pointing into the same memory.

# Simulated: producer writes Arrow batch to a buffer
schema = pa.schema([
    pa.field("user_id", pa.int64()),
    pa.field("amount", pa.float64()),
    pa.field("category", pa.string()),
])
batch = pa.record_batch({
    "user_id": [1, 2, 3, 4, 5],
    "amount": [10.5, 20.0, 5.75, 100.0, 15.25],
    "category": ["food", "tech", "food", "travel", "food"],
}, schema=schema)

# Serialize to bytes (Arrow IPC format — columnar, no serialization overhead)
buf = io.BytesIO()
writer = ipc.new_stream(buf, schema)
writer.write_batch(batch)
writer.close()
arrow_bytes = buf.getvalue()

# ── Consumer: zero-copy deserialization ────────────────────────────────────────

# Deserialize from bytes — Arrow arrays point DIRECTLY into the buffer
# No data copy: the arrow arrays' buffers ARE the bytes buffer
reader = ipc.open_stream(pa.py_buffer(arrow_bytes))
received_batch = reader.read_next_batch()

# Operations on received_batch read from the same memory pages as arrow_bytes
# When using real shared memory (mmap), this is truly zero-copy across processes
print(received_batch.to_pandas())

# This is why Pandas UDFs in PySpark are so much faster than regular Python UDFs:
# Regular UDF: each row pickled to Python → deserialized → function → pickled back to JVM
# Pandas UDF: entire partition serialized as Arrow IPC → ONE copy → Pandas DataFrame
#             result serialized as Arrow IPC → ONE copy back to JVM
# Arrow IPC avoids row-by-row serialization overhead: ~10-100x faster
```

---

## 10. Message Queues and Event Loops

### 10.1 POSIX Message Queues

POSIX message queues (`mq_open`, `mq_send`, `mq_receive`) provide kernel-managed message queues with:
- **Message boundaries preserved** (unlike pipes which are byte streams)
- **Priority ordering** (higher-priority messages received first)
- **Notification on new messages** (signal or thread notification)
- **Persistence** (messages survive sender exit, unlike pipes)

```python
import posix_ipc   # pip install posix-ipc

# Producer
mq = posix_ipc.MessageQueue("/spark_metrics", posix_ipc.O_CREAT,
                              max_messages=1000, max_message_size=4096)
mq.send(b'{"executor": 1, "tasks_completed": 100}', priority=0)
mq.send(b'{"executor": 1, "oom_warning": true}', priority=10)   # higher priority

# Consumer — higher-priority messages received first
while True:
    try:
        msg, priority = mq.receive(timeout=1.0)
        process_metric(msg, priority)
    except posix_ipc.BusyError:
        break   # queue empty

mq.unlink()   # remove the queue
```

### 10.2 In-Process Queues for Thread IPC

Within a single Python process, `queue.Queue` is the standard thread-safe FIFO queue:

```python
import queue
import threading
import time

# Classic producer-consumer pipeline with backpressure
work_queue: queue.Queue = queue.Queue(maxsize=500)
result_queue: queue.Queue = queue.Queue(maxsize=500)
shutdown_flag = threading.Event()

def reader_thread(path: str) -> None:
    """Read records from source and enqueue for processing."""
    with open(path) as f:
        for line in f:
            if shutdown_flag.is_set():
                break
            # put() blocks when maxsize reached → backpressure
            work_queue.put(line.strip(), timeout=1.0)
    work_queue.put(None)   # sentinel

def worker_thread(worker_id: int) -> None:
    """Process records from work queue."""
    while not shutdown_flag.is_set():
        try:
            record = work_queue.get(timeout=1.0)
        except queue.Empty:
            continue
        if record is None:
            work_queue.put(None)   # propagate sentinel to other workers
            break
        result = transform(record)
        result_queue.put(result)
        work_queue.task_done()   # signals that this item is processed

def writer_thread() -> None:
    """Write results from result queue to destination."""
    with open("output.jsonl", "w") as f:
        while True:
            try:
                result = result_queue.get(timeout=1.0)
            except queue.Empty:
                if shutdown_flag.is_set():
                    break
                continue
            f.write(result + "\n")
            result_queue.task_done()

# SIGTERM handler: set flag, let threads drain naturally
def handle_sigterm(signum, frame):
    shutdown_flag.set()

signal.signal(signal.SIGTERM, handle_sigterm)
```

---

## 11. Mental Models

### 11.1 Signals as Interrupts from the OS

Think of signals as the OS's way of tapping a process on the shoulder to say something. Some taps are suggestions (SIGTERM: "please wrap up"), some are mandatory (SIGKILL: "you're done right now"), and some are hardware emergencies that the OS converts to signals (SIGSEGV: "the CPU just faulted because you tried to read an unmapped address"). The process can choose to ignore polite taps (by registering SIG_IGN) or respond differently (custom handler), but it cannot refuse mandatory signals.

The asynchronous nature is the dangerous part: the tap can arrive between any two instructions. If the tap handler allocates memory and the process was interrupted in the middle of `malloc()`, both the handler and the interrupted code will try to manipulate the same heap allocator state — corruption follows. This is why signal handlers must be minimal: set a flag, write to a pre-opened pipe, and return. Nothing else.

### 11.2 IPC Mechanisms on a Spectrum

IPC mechanisms trade complexity for performance. At one end: a simple file. Two processes share data by writing and reading a file. Requires fsync for consistency. Latency: disk-speed. At the other end: shared memory. Two processes map the same physical pages. Requires explicit synchronization (mutexes, semaphores). Latency: nanoseconds.

```
IPC Mechanism Spectrum:

← Simple                                                  Fast →
File  →  Pipe  →  Unix Socket  →  TCP Socket  →  Shared Memory
 ↑           ↑            ↑               ↑              ↑
disk I/O  kernel buf  kernel buf      kernel buf+TCP   zero copy
 5-10ms    5-50µs      5-20µs          30-100µs         ~0µs
Durable   Local only   Local only      Network-capable   Must sync
```

Choose the mechanism based on: are the processes on the same machine? (if yes: pipes/UDS/shm vs TCP), do you need message boundaries? (pipes are byte streams; UDS DGRAM and message queues preserve boundaries), how large is the data? (for GB-scale: shared memory; for KB-scale: any of the above).

### 11.3 The Graceful Shutdown as a State Machine

A well-designed service shutdown is a state machine with transitions triggered by signals:

```
State: RUNNING
  → receive SIGTERM → State: DRAINING
    (stop accepting new work; finish current work)
  → all current work complete → State: FLUSHING
    (flush buffers, close connections, commit offsets)
  → flush complete → State: EXITING → exit(0)

At any state:
  → receive SIGKILL → (no state transition: instant death)
  → timeout expires → State: FORCED_EXIT → exit(1)
```

The contract with the orchestrator (Kubernetes, YARN, systemd) is: "I will exit cleanly within T seconds of SIGTERM." If T expires, the orchestrator sends SIGKILL — the application gets no further say. Designing services to honor this contract requires: fast state transitions, bounded cleanup time, and no blocking operations in the shutdown path.

---

## 12. Failure Scenarios

### 12.1 OOM Kill: Silent Executor Death

```
Symptom: Spark job fails with "Lost executor" messages; tasks re-run multiple times.
Driver logs: 
  "Lost executor 3 on spark-worker-2: Executor heartbeat timed out after 120000 ms"

Root cause: Executor exceeded its container memory limit → OOM kill

Diagnosis path:
  Step 1: Check dmesg on the worker node
    ssh spark-worker-2
    dmesg | grep -i "oom\|killed process" | tail -20
    # Out of memory: Kill process 12345 (java) score 847 or sacrifice child
    # Killed process 12345 (java) total-vm:8388608kB, anon-rss:6291456kB
    
  Step 2: Check cgroup memory accounting
    # For Kubernetes: find the pod's cgroup
    cat /sys/fs/cgroup/memory/kubepods/*/memory.failcnt
    # Non-zero value confirms the container hit its memory limit
    
  Step 3: Check container limits vs spark configuration
    kubectl describe pod <executor-pod> | grep -A5 "Limits:"
    # Limits: memory: 4Gi
    
    # But Spark executor uses:
    # spark.executor.memory = 3g (JVM heap)
    # spark.executor.memoryOverhead = 384m (off-heap: Netty, metaspace)
    # Total: 3.375 GB → container limit 4 GB → only 625 MB margin
    # Large Tungsten off-heap (spark.memory.offHeap.size) pushed it over

  Fix: Increase container limit or reduce Spark memory settings:
    spark.executor.memory = 3g
    spark.executor.memoryOverhead = 768m   # increase overhead allocation
    # Or set container limit to 5Gi
    
  Prevention: set spark.executor.memoryOverhead to at least:
    max(384MB, 0.1 × spark.executor.memory)
    For Python UDFs: add 256MB extra for Python worker processes
```

### 12.2 SIGPIPE: Broken Pipe in Subprocess Pipelines

```
Symptom: Python subprocess fails with BrokenPipeError or exit code 141 (128 + SIGPIPE=13)
  
  import subprocess
  proc = subprocess.Popen(["cat", "large_file.parquet"], stdout=subprocess.PIPE)
  first_line = proc.stdout.readline()
  proc.stdout.close()   # close the pipe — proc still writing to it
  proc.wait()
  # proc.returncode = -13 (killed by SIGPIPE)
  # OR BrokenPipeError if cat caught SIGPIPE
  
Root cause: 
  cat is writing to the pipe. The Python side closes the read end (proc.stdout.close()).
  The kernel sees: write to a pipe with no reader → deliver SIGPIPE to the writer (cat).
  Default action for SIGPIPE: terminate. cat exits with code 141 (128 + 13).

Fix: either read all of the pipe output, or terminate the process before closing:
  proc.terminate()   # SIGTERM first
  proc.wait(timeout=5)
  if proc.returncode is None:
      proc.kill()   # SIGKILL if still running
  
  # OR: suppress SIGPIPE in the child process
  proc = subprocess.Popen(
      ["cat", "large_file.parquet"],
      stdout=subprocess.PIPE,
      preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL)
  )

In data pipelines (head, grep, awk chains):
  cat access.log | grep ERROR | head -100
  # head closes stdin after 100 lines → grep gets SIGPIPE → exits
  # This is correct behavior: grep exits cleanly when head is done
  # The shell suppresses the SIGPIPE exit code in pipelines
```

### 12.3 Zombie Processes from Missing wait()

```
Symptom: "Too many processes" or process table full; ps shows <defunct> processes.

Root cause: A parent process forked children but never called wait() on them.
When a child exits, the kernel keeps its PCB (exit status, PID) in the process table
until the parent calls wait(). This is a "zombie" — the process is dead but not reaped.

  import os, time
  
  # BUG: fork without wait
  for i in range(100):
      pid = os.fork()
      if pid == 0:
          # Child: do work and exit
          time.sleep(1)
          os._exit(0)
      # Parent: never calls waitpid(pid, ...) → 100 zombies accumulate
  
  # ps output: 100 rows like: 12345 pts/0 Z+ 0:00 [python3] <defunct>
  
  Fix option 1: wait() for each child
  for i in range(100):
      pid = os.fork()
      if pid == 0:
          time.sleep(1)
          os._exit(0)
      else:
          os.waitpid(pid, 0)   # blocks until child exits, reaps it
  
  Fix option 2: signal(SIGCHLD, SIG_IGN) — tell kernel to auto-reap children
  signal.signal(signal.SIGCHLD, signal.SIG_IGN)
  # With SIG_IGN for SIGCHLD: children are automatically reaped on exit
  # No zombie accumulation
  
  Fix option 3: use subprocess.Popen with context manager (recommended)
  with subprocess.Popen(["worker.py"]) as proc:
      proc.wait()   # __exit__ calls wait() if not already done
```

### 12.4 Signal Handler Deadlock

```python
# BUG: signal handler acquires a lock that may already be held by the main thread

import threading, signal, logging

lock = threading.Lock()

def handler(signum, frame):
    # DEADLOCK RISK: if SIGTERM arrives while the main thread holds 'lock',
    # this handler (running in the main thread) will block forever trying to acquire it.
    with lock:   # ← BUG
        logging.info("Shutdown signal received")   # logging also uses a lock

signal.signal(signal.SIGTERM, handler)

def main():
    with lock:
        # If SIGTERM arrives here (between the with and the end of the block):
        # the handler tries to acquire 'lock' → deadlock
        do_long_operation()

# FIX: signal handler only sets a flag
_shutdown = False

def safe_handler(signum, frame):
    global _shutdown
    _shutdown = True   # simple assignment: always safe

signal.signal(signal.SIGTERM, safe_handler)

def main():
    while not _shutdown:
        with lock:
            do_long_operation()
    # shutdown logic runs outside the signal handler
    with lock:
        flush_and_close()
```

---

## 13. Data Engineering Connections

### 13.1 Airflow: SIGUSR1 for Task Interruption

Airflow uses `SIGUSR1` as an internal signal for marking task interruption vs. external termination:

```
When Airflow kills a task (e.g., user clicks "Clear and restart" in the UI):
  Airflow worker sends SIGUSR1 to the task runner process.
  Task runner catches SIGUSR1 → marks task as "up_for_retry" in the metadata DB
  → re-queues the task for another attempt.

When Kubernetes kills the worker pod (SIGTERM):
  Airflow worker catches SIGTERM → sends SIGTERM to task runner.
  Task runner catches SIGTERM → marks task as "failed" (NOT retried by default).

The distinction matters: SIGUSR1 = "retry me," SIGTERM = "you're done."

In Airflow's BaseTaskRunner:
  def terminate(self):
      """Terminate the task (used for retry)."""
      os.kill(self.task_process.pid, signal.SIGUSR1)   # SIGUSR1 = retry
  
  def kill(self):
      """Kill the task (used for failure)."""
      os.kill(self.task_process.pid, signal.SIGTERM)   # SIGTERM = fail
      time.sleep(5)
      if self.task_process.poll() is None:
          os.kill(self.task_process.pid, signal.SIGKILL)
```

### 13.2 Spark: SparkContext Shutdown Hooks and Signal Forwarding

```scala
// Spark SparkContext registers a JVM shutdown hook on initialization:
Runtime.getRuntime.addShutdownHook(new Thread("SparkContextShutdownHook") {
  override def run(): Unit = {
    logInfo("Invoking stop() from shutdown hook")
    stop()
  }
})

// JVM shutdown hooks run when:
// 1. All non-daemon threads complete
// 2. System.exit() is called
// 3. SIGTERM or SIGINT is received (not SIGKILL)

// Spark's signal handling (in Utils.scala):
// Spark registers handlers for SIGTERM, SIGINT, SIGHUP that call System.exit(0)
// → triggers the shutdown hook → SparkContext.stop()

// Signal forwarding in spark-submit:
// spark-submit (shell script) launches the Driver JVM.
// When spark-submit receives SIGTERM from YARN/Kubernetes:
//   trap 'kill -TERM $DRIVER_PID' TERM   # forward SIGTERM to the Driver JVM
//   wait $DRIVER_PID                      # wait for Driver to exit
// Without this trap, SIGTERM to spark-submit would kill the shell script
// but leave the Driver JVM running orphaned.
```

### 13.3 Kafka: SIGUSR1 for Log Rolling

Kafka uses `SIGUSR1` to trigger log file rolling — closing the current log file and opening a new one:

```
kafka-server-start.sh installs a SIGUSR1 handler in the JVM:
  signal(SIGUSR1, () => logManager.truncateFailed())
  
  # Also used for:
  # kill -USR1 <kafka-broker-pid>  →  flush all log segments to disk
  # Used in monitoring scripts to verify broker log persistence
  
  # In Kafka's log4j configuration:
  # SIGHUP → reload log4j configuration (change log levels without restart)
  kill -HUP <kafka-broker-pid>  →  reload log4j.properties → change DEBUG/INFO/WARN
```

### 13.4 Python Multiprocessing: Signal Propagation in Pools

```python
import multiprocessing
import signal
import os

def worker_function(x):
    """Worker function in Pool process."""
    import time
    time.sleep(10)   # simulate long work
    return x * 2

def main():
    pool = multiprocessing.Pool(processes=4)
    
    # IMPORTANT: Pool worker processes inherit signal handlers from the parent,
    # but multiprocessing resets SIGINT to SIG_IGN in worker processes.
    # This prevents Ctrl+C from killing only one worker; the parent handles it.
    
    def handle_sigterm(signum, frame):
        print("SIGTERM received, terminating pool...")
        pool.terminate()   # sends SIGTERM to all worker processes
        pool.join()        # wait for all workers to exit
        exit(0)
    
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    # Pool.map() blocks until all results are ready
    # If SIGTERM arrives during map(), handle_sigterm() calls pool.terminate()
    # pool.terminate() sends SIGTERM to all workers immediately (no drain)
    # Use pool.close() + pool.join() if you want workers to finish current tasks
    
    try:
        results = pool.map(worker_function, range(100))
    finally:
        pool.close()
        pool.join()

# Pool.close() vs Pool.terminate():
# close(): prevents submission of new tasks; workers finish current tasks then exit
# terminate(): sends SIGTERM to all workers immediately; tasks abandoned
```

---

## 14. Code Toolkit

### 14.1 `graceful_shutdown.py` — Production-Grade Shutdown Handler

```python
"""
graceful_shutdown.py — Reusable graceful shutdown infrastructure.

Provides:
  1. GracefulShutdown context manager: register cleanup callbacks,
     handle SIGTERM/SIGINT, enforce a timeout before force-exit.
  2. ProcessSupervisor: manage child processes with SIGTERM→SIGKILL escalation.
  3. ChildProcessGroup: send signals to a process group atomically.

Use in:
  - Airflow operators that launch subprocesses
  - Kafka consumers that must commit offsets before exit
  - Spark job launchers that must cancel the application on SIGTERM
"""
from __future__ import annotations
import os
import sys
import time
import signal
import threading
import subprocess
import logging
from typing import Callable

log = logging.getLogger(__name__)


class GracefulShutdown:
    """
    Context manager that installs SIGTERM/SIGINT handlers and runs
    registered cleanup callbacks on shutdown, with a hard timeout.
    
    Usage:
        with GracefulShutdown(timeout=25) as gs:
            gs.register_cleanup(consumer.close)
            gs.register_cleanup(db.commit)
            while gs.running:
                process_next()
    """
    
    def __init__(self, timeout: float = 25.0) -> None:
        self.timeout = timeout
        self._running = True
        self._shutdown_event = threading.Event()
        self._cleanup_callbacks: list[tuple[Callable, tuple, dict]] = []
        self._original_sigterm: signal.Handlers = signal.SIG_DFL
        self._original_sigint: signal.Handlers = signal.SIG_DFL
    
    @property
    def running(self) -> bool:
        return self._running
    
    def register_cleanup(
        self,
        fn: Callable,
        *args,
        **kwargs
    ) -> None:
        """Register a function to call during shutdown. Called in LIFO order."""
        self._cleanup_callbacks.append((fn, args, kwargs))
    
    def _signal_handler(self, signum: int, frame) -> None:
        sig_name = signal.Signals(signum).name
        log.info(f"Signal {sig_name} received, initiating graceful shutdown")
        self._running = False
        self._shutdown_event.set()
    
    def __enter__(self) -> "GracefulShutdown":
        self._original_sigterm = signal.signal(signal.SIGTERM, self._signal_handler)
        self._original_sigint = signal.signal(signal.SIGINT, self._signal_handler)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        # Only run cleanup on clean exit or signal (not on unexpected exceptions
        # that we want to propagate)
        if exc_type is None or self._shutdown_event.is_set():
            self._run_cleanup()
        
        # Restore original handlers
        signal.signal(signal.SIGTERM, self._original_sigterm)
        signal.signal(signal.SIGINT, self._original_sigint)
        
        return False   # don't suppress exceptions
    
    def _run_cleanup(self) -> None:
        """Run cleanup callbacks in LIFO order, with a total timeout."""
        deadline = time.monotonic() + self.timeout
        
        for fn, args, kwargs in reversed(self._cleanup_callbacks):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                log.warning(f"Shutdown timeout reached, skipping remaining cleanup")
                break
            
            try:
                # Run callback with a thread + timeout to prevent hangs
                result: list = []
                exc_holder: list[Exception] = []
                
                def run():
                    try:
                        result.append(fn(*args, **kwargs))
                    except Exception as e:
                        exc_holder.append(e)
                
                t = threading.Thread(target=run, daemon=True)
                t.start()
                t.join(timeout=min(remaining, 5.0))   # max 5s per callback
                
                if t.is_alive():
                    log.warning(f"Cleanup callback {fn.__name__} timed out")
                elif exc_holder:
                    log.error(f"Cleanup callback {fn.__name__} raised: {exc_holder[0]}")
                else:
                    log.info(f"Cleanup {fn.__name__} completed")
            
            except Exception as e:
                log.error(f"Error in cleanup {fn.__name__}: {e}")


class ProcessSupervisor:
    """
    Manages a child process with SIGTERM→wait→SIGKILL escalation.
    
    Usage:
        sup = ProcessSupervisor(["spark-submit", "--class", "MyApp", "job.jar"])
        sup.start()
        # ... main loop ...
        sup.terminate(timeout=20)   # SIGTERM, wait 20s, then SIGKILL
    """
    
    def __init__(self, cmd: list[str], **popen_kwargs) -> None:
        self.cmd = cmd
        self.popen_kwargs = popen_kwargs
        self._proc: subprocess.Popen | None = None
    
    def start(self) -> int:
        """Start the subprocess. Returns the PID."""
        self._proc = subprocess.Popen(self.cmd, **self.popen_kwargs)
        log.info(f"Started subprocess PID {self._proc.pid}: {' '.join(self.cmd)}")
        return self._proc.pid
    
    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc else None
    
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None
    
    def terminate(self, timeout: float = 20.0) -> int:
        """
        Gracefully terminate the subprocess:
        1. Send SIGTERM
        2. Wait up to `timeout` seconds
        3. Send SIGKILL if still running
        Returns the exit code.
        """
        if self._proc is None:
            return 0
        
        if self._proc.poll() is not None:
            return self._proc.returncode
        
        log.info(f"Sending SIGTERM to PID {self._proc.pid}")
        self._proc.send_signal(signal.SIGTERM)
        
        try:
            returncode = self._proc.wait(timeout=timeout)
            log.info(f"Process {self._proc.pid} exited with code {returncode}")
            return returncode
        except subprocess.TimeoutExpired:
            log.warning(
                f"Process {self._proc.pid} did not exit within {timeout}s, "
                f"sending SIGKILL"
            )
            self._proc.kill()
            returncode = self._proc.wait()
            log.info(f"Process {self._proc.pid} killed, exit code {returncode}")
            return returncode


class ChildProcessGroup:
    """
    Send signals to an entire process group atomically.
    Useful when a subprocess spawns its own children (e.g., spark-submit → driver JVM).
    
    Uses negative PID: kill(-pgid, sig) sends to all processes in the group.
    """
    
    def __init__(self, cmd: list[str]) -> None:
        self.cmd = cmd
        self._proc: subprocess.Popen | None = None
    
    def start(self) -> None:
        # Create a new process group for the child
        # (os.setpgrp() makes the child the group leader)
        self._proc = subprocess.Popen(
            self.cmd,
            preexec_fn=os.setpgrp   # new process group, pgid = child's pid
        )
        log.info(f"Started process group PGID {self._proc.pid}")
    
    def send_to_group(self, sig: signal.Signals) -> None:
        """Send signal to all processes in the group."""
        if self._proc:
            try:
                os.killpg(self._proc.pid, sig)
                log.info(f"Sent {sig.name} to process group {self._proc.pid}")
            except ProcessLookupError:
                pass   # group already gone
    
    def terminate_group(self, timeout: float = 20.0) -> None:
        """SIGTERM the group, then SIGKILL if necessary."""
        self.send_to_group(signal.SIGTERM)
        
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._proc.poll() is not None:
                return
            time.sleep(0.5)
        
        log.warning(f"Process group {self._proc.pid} timeout, sending SIGKILL")
        self.send_to_group(signal.SIGKILL)
        self._proc.wait()


# ── Demo: Kafka consumer with graceful shutdown ────────────────────────────────

if __name__ == "__main__":
    import random
    
    logging.basicConfig(level=logging.INFO)
    
    # Simulated consumer (without real Kafka dependency)
    class MockConsumer:
        def poll(self, timeout: float) -> dict | None:
            time.sleep(timeout)
            return {"offset": random.randint(0, 1000)} if random.random() > 0.3 else None
        
        def commit(self) -> None:
            log.info("Offsets committed")
        
        def close(self) -> None:
            log.info("Consumer closed (LeaveGroup sent)")
    
    consumer = MockConsumer()
    processed = 0
    
    with GracefulShutdown(timeout=10) as gs:
        gs.register_cleanup(consumer.commit)
        gs.register_cleanup(consumer.close)
        
        log.info(f"Consumer running, PID {os.getpid()}")
        log.info("Send SIGTERM to trigger graceful shutdown")
        
        while gs.running:
            msg = consumer.poll(timeout=1.0)
            if msg:
                processed += 1
                if processed % 10 == 0:
                    log.info(f"Processed {processed} messages")
    
    log.info(f"Clean exit. Total processed: {processed}")
```

### 14.2 `ipc_benchmark.py` — Compare IPC Mechanism Throughput

```python
"""
ipc_benchmark.py — Benchmark IPC mechanisms for data transfer.

Compares:
  1. Pipe (multiprocessing)
  2. Unix domain socket (SOCK_STREAM)
  3. Shared memory (mmap + Event)
  4. In-process queue (threading.Queue, single process)

Run directly to get an IPC performance profile for your system.
"""
from __future__ import annotations
import os
import mmap
import time
import struct
import socket
import threading
import multiprocessing
import queue
from pathlib import Path

PAYLOAD_SIZE = 1024 * 1024      # 1 MB per message
N_MESSAGES = 100                 # 100 messages = 100 MB total transfer


def bench_pipe() -> float:
    """Benchmark pipe throughput (bytes/second)."""
    r_fd, w_fd = os.pipe()
    
    def writer():
        data = b"X" * PAYLOAD_SIZE
        for _ in range(N_MESSAGES):
            written = 0
            while written < len(data):
                written += os.write(w_fd, data[written:])
        os.close(w_fd)
    
    t = threading.Thread(target=writer)
    t0 = time.perf_counter()
    t.start()
    
    received = 0
    while received < PAYLOAD_SIZE * N_MESSAGES:
        chunk = os.read(r_fd, 65536)
        if not chunk:
            break
        received += len(chunk)
    
    elapsed = time.perf_counter() - t0
    os.close(r_fd)
    t.join()
    return (PAYLOAD_SIZE * N_MESSAGES) / elapsed


def bench_unix_socket() -> float:
    """Benchmark Unix domain socket throughput."""
    SOCK_PATH = "/tmp/ipc_bench.sock"
    if os.path.exists(SOCK_PATH):
        os.unlink(SOCK_PATH)
    
    server_ready = threading.Event()
    total_bytes = [0]
    t0_holder = [0.0]
    
    def server():
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(SOCK_PATH)
        srv.listen(1)
        server_ready.set()
        conn, _ = srv.accept()
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break
            total_bytes[0] += len(chunk)
        conn.close()
        srv.close()
    
    s = threading.Thread(target=server, daemon=True)
    s.start()
    server_ready.wait()
    
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(SOCK_PATH)
    data = b"X" * PAYLOAD_SIZE
    
    t0_holder[0] = time.perf_counter()
    for _ in range(N_MESSAGES):
        client.sendall(data)
    client.close()
    s.join(timeout=5)
    elapsed = time.perf_counter() - t0_holder[0]
    
    try:
        os.unlink(SOCK_PATH)
    except FileNotFoundError:
        pass
    
    return (PAYLOAD_SIZE * N_MESSAGES) / elapsed


def bench_shared_memory() -> float:
    """Benchmark shared memory throughput via mmap."""
    SHM_SIZE = PAYLOAD_SIZE + 16   # 8 bytes length + 8 bytes ready flag
    shm_path = "/tmp/ipc_bench_shm.bin"
    
    # Create shared file
    with open(shm_path, "wb") as f:
        f.write(b"\x00" * SHM_SIZE)
    
    producer_done = threading.Event()
    consumer_done = threading.Event()
    total_received = [0]
    
    def producer():
        with open(shm_path, "r+b") as f:
            mem = mmap.mmap(f.fileno(), SHM_SIZE)
        data = b"X" * PAYLOAD_SIZE
        for i in range(N_MESSAGES):
            # Write: flag=0 (not ready), data, flag=1 (ready)
            mem[8:8 + len(data)] = data
            mem[0:8] = struct.pack("Q", len(data))   # signal ready
            # Busy-wait for consumer to signal consumed (simplified protocol)
            while struct.unpack("Q", mem[0:8])[0] != 0:
                pass
        producer_done.set()
        mem.close()
    
    # For simplicity, measure pure write bandwidth (no handshake)
    t0 = time.perf_counter()
    with open(shm_path, "r+b") as f:
        mem = mmap.mmap(f.fileno(), SHM_SIZE)
    
    data = b"X" * PAYLOAD_SIZE
    for _ in range(N_MESSAGES):
        mem[0:len(data)] = data
    
    elapsed = time.perf_counter() - t0
    mem.close()
    os.unlink(shm_path)
    
    return (PAYLOAD_SIZE * N_MESSAGES) / elapsed


def bench_thread_queue() -> float:
    """Benchmark in-process thread queue (no copy between processes)."""
    q: queue.Queue = queue.Queue(maxsize=10)
    data = b"X" * PAYLOAD_SIZE
    
    def producer():
        for _ in range(N_MESSAGES):
            q.put(data)
        q.put(None)
    
    t = threading.Thread(target=producer)
    t0 = time.perf_counter()
    t.start()
    
    received = 0
    while True:
        item = q.get()
        if item is None:
            break
        received += len(item)
    
    elapsed = time.perf_counter() - t0
    t.join()
    return (PAYLOAD_SIZE * N_MESSAGES) / elapsed


def to_gbps(bytes_per_sec: float) -> float:
    return bytes_per_sec / 1e9


if __name__ == "__main__":
    total_mb = PAYLOAD_SIZE * N_MESSAGES / 1024 / 1024
    
    print(f"IPC Benchmark: {N_MESSAGES} × {PAYLOAD_SIZE//1024//1024} MB "
          f"= {total_mb:.0f} MB total\n")
    print(f"{'Mechanism':<25} {'Throughput':>15} {'Latency est.':>15}")
    print("-" * 57)
    
    benchmarks = [
        ("Pipe (thread)", bench_pipe),
        ("Unix Domain Socket", bench_unix_socket),
        ("Shared Memory (mmap)", bench_shared_memory),
        ("Thread Queue", bench_thread_queue),
    ]
    
    for name, fn in benchmarks:
        try:
            bps = fn()
            gbps = to_gbps(bps)
            latency_us = (PAYLOAD_SIZE / bps) * 1e6
            print(f"{name:<25} {gbps:>12.2f} GB/s {latency_us:>12.1f} µs")
        except Exception as e:
            print(f"{name:<25} ERROR: {e}")
    
    print(f"\nNote: Shared memory measures write bandwidth (no process copy).")
    print(f"Unix socket and pipe include kernel copy overhead.")
    print(f"Thread queue measures Python object handoff (no data copy between processes).")
```

---

## 15. Hands-On Labs

### Lab 1: Map Graceful Shutdown to Your Own System

**Goal:** Understand the full signal chain for a tool you already use.

```bash
# Step 1: Start a long-running Python script
python3 -c "
import time, signal, os
print(f'PID: {os.getpid()}')
signal.signal(signal.SIGTERM, lambda s, f: print('SIGTERM received!'))
while True:
    time.sleep(1)
    print('tick')
" &

PID=$!
sleep 3

# Step 2: Send SIGTERM and observe
kill -TERM $PID   # polite
sleep 2

# Step 3: Try SIGKILL — always works
kill -9 $PID

# Step 4: Examine what the process was doing when killed
# (requires strace or /proc)
```

### Lab 2: Measure IPC Latency

```python
# lab2_ipc_latency.py
"""
Measure round-trip latency for different IPC mechanisms.
Sends a small ping message and waits for a pong.
"""
import os, time, socket, threading, queue, statistics

ROUNDS = 10_000
PING = b"ping"
PONG = b"pong"

def pipe_latency() -> float:
    r1, w1 = os.pipe()   # ping pipe
    r2, w2 = os.pipe()   # pong pipe
    
    def server():
        while True:
            msg = os.read(r1, 4)
            if msg == b"done":
                break
            os.write(w2, PONG)
    
    t = threading.Thread(target=server, daemon=True)
    t.start()
    
    times = []
    for _ in range(ROUNDS):
        t0 = time.perf_counter_ns()
        os.write(w1, PING)
        os.read(r2, 4)
        times.append(time.perf_counter_ns() - t0)
    
    os.write(w1, b"done")
    t.join()
    for fd in [r1, w1, r2, w2]:
        os.close(fd)
    return statistics.median(times) / 1000   # → µs

def uds_latency() -> float:
    SOCK = "/tmp/lat_bench.sock"
    if os.path.exists(SOCK):
        os.unlink(SOCK)
    
    ready = threading.Event()
    
    def server():
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(SOCK)
        s.listen(1)
        ready.set()
        conn, _ = s.accept()
        while True:
            msg = conn.recv(4)
            if not msg or msg == b"done":
                break
            conn.sendall(PONG)
        conn.close()
        s.close()
    
    t = threading.Thread(target=server, daemon=True)
    t.start()
    ready.wait()
    
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(SOCK)
    
    times = []
    for _ in range(ROUNDS):
        t0 = time.perf_counter_ns()
        client.sendall(PING)
        client.recv(4)
        times.append(time.perf_counter_ns() - t0)
    
    client.sendall(b"done")
    client.close()
    t.join()
    try:
        os.unlink(SOCK)
    except FileNotFoundError:
        pass
    return statistics.median(times) / 1000

def queue_latency() -> float:
    q1: queue.Queue = queue.Queue()
    q2: queue.Queue = queue.Queue()
    
    def server():
        while True:
            msg = q1.get()
            if msg == "done":
                break
            q2.put(PONG)
    
    t = threading.Thread(target=server, daemon=True)
    t.start()
    
    times = []
    for _ in range(ROUNDS):
        t0 = time.perf_counter_ns()
        q1.put(PING)
        q2.get()
        times.append(time.perf_counter_ns() - t0)
    
    q1.put("done")
    t.join()
    return statistics.median(times) / 1000

if __name__ == "__main__":
    print(f"Round-trip latency ({ROUNDS:,} rounds, median):\n")
    print(f"{'Mechanism':<25} {'Latency':>12}")
    print("-" * 40)
    for name, fn in [("Pipe", pipe_latency), ("Unix Domain Socket", uds_latency), ("Thread Queue", queue_latency)]:
        lat = fn()
        print(f"{name:<25} {lat:>10.1f} µs")
```

---

## 16. Interview Q&A

**Q1: What is the difference between SIGTERM and SIGKILL? When does each get used in a data platform?**

SIGTERM (signal 15) is a polite termination request — it can be caught, blocked, or ignored. When a process receives SIGTERM, it has the opportunity to run its signal handler or check a shutdown flag and execute cleanup code: flushing buffers, committing database transactions, finishing in-flight work, sending a LeaveGroup request to a Kafka broker, or canceling a Spark application. The process then calls exit() voluntarily. SIGTERM is the standard way to ask a process to shut down cleanly.

SIGKILL (signal 9) is unconditional termination enforced by the kernel — it cannot be caught, blocked, or ignored. When SIGKILL is delivered, the kernel immediately reclaims all the process's resources (memory, file descriptors, threads) without running any user-space code. No signal handler runs, no cleanup occurs, no buffers are flushed. The process is simply gone.

In a data platform, the two signals appear in a specific sequence. Kubernetes sends SIGTERM first, then waits `terminationGracePeriodSeconds` (default 30 seconds), then sends SIGKILL. This is the graceful shutdown protocol: SIGTERM gives the application a window to clean up; SIGKILL ensures the process actually terminates if it hangs during cleanup. Airflow workers receive SIGTERM from Kubernetes and propagate SIGTERM to their task runner subprocesses, which propagate it further to the operators they're running (SparkSubmit, Bash, etc.). The OOM killer in the Linux kernel uses SIGKILL directly — when a container exceeds its memory cgroup limit, the kernel selects a victim process and kills it with SIGKILL. There is no warning, no SIGTERM first. This is why OOM kills appear as unexplained executor losses in Spark: the executor received SIGKILL and the JVM had no chance to log a shutdown message.

**Q2: What makes a signal handler unsafe, and what is the correct pattern in Python?**

A signal handler is unsafe when it calls functions that are not async-signal-safe. The core problem is reentrancy: the signal is delivered asynchronously and can interrupt the process at any instruction — including in the middle of a function like `malloc()`, which maintains internal state in a non-reentrant data structure. If the signal handler calls `malloc()` after `malloc()` was interrupted mid-execution, both invocations corrupt the same heap state. This is a class of bug that produces rare, hard-to-reproduce crashes.

The POSIX standard defines exactly which functions are async-signal-safe: raw I/O (`write`, `read`), process control (`kill`, `getpid`, `_exit`), and signal manipulation (`sigaction`, `sigprocmask`). Notably absent: `printf`, `malloc`, `free`, any Python function, and `logging.info`. In a C program, the correct signal handler does nothing except write a single byte to a pre-opened pipe (the self-pipe trick) or set a `volatile sig_atomic_t` flag.

In Python, the constraint is somewhat relaxed because CPython doesn't deliver signals between arbitrary machine instructions — it delivers them between Python bytecodes, at a controlled interpreter checkpoint. Python signal handlers run in the main thread in a normal execution context, not in a true async interrupt context. This means setting a `threading.Event` or a global boolean works in practice. The correct Python pattern is: register a signal handler that only sets a shutdown flag (`threading.Event.set()` or `_shutdown = True`), and have the main processing loop check this flag between iterations. All cleanup work — committing offsets, flushing files, canceling subprocesses — runs in the main loop after the flag is checked, not inside the signal handler. This keeps the handler minimal and the cleanup logic debuggable.

**Q3: Compare pipes, Unix domain sockets, and shared memory for IPC in a data pipeline. When would you choose each?**

The three mechanisms differ in what they copy, where they place the message boundary, and how they handle backpressure.

Pipes are the simplest: a kernel-buffered byte stream with no message boundaries. `write()` to the write end copies data into a 64 KB kernel buffer; `read()` from the read end copies it out. The kernel provides natural backpressure — the writer blocks when the buffer is full. Latency is ~5–20 µs for small messages. Pipes are appropriate for low-to-moderate throughput streaming data between a producer and a single consumer on the same machine — the classic Unix pipeline model. Python's `subprocess.PIPE` and `multiprocessing.Pipe` use this mechanism. The limitation: pipes are unidirectional, require one fd pair per direction, and don't support multiple consumers.

Unix domain sockets provide the same semantics as TCP sockets (including `SOCK_DGRAM` for message-boundary-preserving datagrams) but entirely within the kernel. They are addressable by a file path, support multiple concurrent clients, support bidirectional communication on a single connection, and can pass file descriptors via `SCM_RIGHTS`. Latency is ~5–20 µs. They're appropriate when you need a client-server model (one server, many producer clients), when you need message boundaries (metrics delivery, RPC protocols), or when you need socket-like semantics without network overhead. Prometheus, StatsD, and systemd socket activation all use UDS.

Shared memory bypasses copying entirely — both processes map the same physical pages and access them directly. The transfer rate is limited by memory bandwidth (~50 GB/s), not kernel copy overhead. Latency for large transfers is effectively zero (data is already in memory). The cost: you must implement synchronization yourself (mutexes, semaphores, condition variables) to coordinate who is writing vs. reading. Shared memory is appropriate for bulk data transfer between cooperating processes on the same machine — Arrow IPC between a JVM Spark executor and a Python UDF worker, or transferring a 500 MB DataFrame partition between processes without copying. The complexity of synchronization makes it inappropriate for general-purpose messaging.

For most data engineering pipelines, the selection guideline is: if transferring large DataFrames (>10 MB) between cooperating processes, use shared memory with Arrow IPC. If streaming individual records between a producer and consumer, use a pipe or multiprocessing.Queue. If you need multiple clients sending to one server, use a Unix domain socket. If the two processes are on different machines, you have no choice: use a network socket (TCP).

**Q4: What is a zombie process and how does it occur in Python multiprocessing?**

A zombie process is a process that has completed execution (its code has exited, its threads have stopped) but whose entry in the kernel process table has not been removed because its parent process has not yet collected its exit status via the `wait()` system call. The process is technically "dead" — it holds no memory, no open file descriptors, no CPU — but its PCB (task_struct) entry persists in the kernel's process table, waiting for the parent to call `waitpid()`. In `ps` output, zombies appear as `<defunct>`.

In Python multiprocessing, zombies occur when a `Process` is started but not explicitly joined before the parent script completes. A common pattern:

```python
for item in work_items:
    p = Process(target=worker, args=(item,))
    p.start()
    # BUG: no p.join() — parent loop continues immediately
# All worker processes exit but become zombies until parent exits
```

Every `Process.start()` must eventually be matched with a `Process.join()` (or `Process.terminate()` + `join()`). The `multiprocessing.Pool` API handles this automatically — `pool.map()` waits for all workers, and `pool.close()` + `pool.join()` ensures all workers exit cleanly. For manual process management, the `concurrent.futures.ProcessPoolExecutor` is safer: it acts as a context manager and reaps workers in `__exit__`.

If zombies accumulate, the process table fills and `fork()` fails with `ENOMEM` — no new processes can be created. The fix for an existing zombie infestation (without restarting the parent) is `signal.signal(signal.SIGCHLD, signal.SIG_IGN)`, which instructs the kernel to auto-reap children on exit without requiring an explicit `wait()`.

**Q5: Describe how a Kafka consumer should handle SIGTERM to avoid causing a 45-second partition reassignment delay.**

A Kafka consumer poll loop that ignores SIGTERM and is killed by SIGKILL (or simply exits without calling `consumer.close()`) leaves its partitions orphaned. The Kafka broker has no way to distinguish "consumer died" from "consumer is busy but alive." It uses the session heartbeat timeout (`session.timeout.ms`, default 45 seconds) as its detection mechanism: if the broker hasn't received a heartbeat within 45 seconds, it declares the consumer dead and triggers a group rebalance to redistribute its partitions. For those 45 seconds, all messages to those partitions are unprocessed, lag grows, and downstream consumers see a data delay.

The fix is explicit: the consumer must call `consumer.close()` before exiting. `close()` sends a LeaveGroup request to the broker's group coordinator, which triggers an immediate rebalance without waiting for the session timeout. The rebalance typically completes in 1–3 seconds, depending on the number of consumers and the `max.poll.interval.ms` setting.

The implementation requires catching SIGTERM and ensuring `consumer.close()` runs. The cleanest Python approach is a `try`/`finally` block: the signal handler sets a flag, the polling loop checks the flag and breaks, and `consumer.close()` is in the `finally` block that always runs. A more production-grade approach uses the `GracefulShutdown` context manager and registers `consumer.close` as a cleanup callback. Either way, the key guarantee is: `consumer.close()` must run even if the process receives SIGTERM. A bare `while True: consumer.poll()` loop with no signal handling will always leave a 45-second rebalance penalty on restart.

**Q6: Explain how process groups and `kill(-pgid, SIGTERM)` are used in Spark's graceful shutdown.**

When `spark-submit` launches a Spark application, it typically creates a hierarchy of processes: the spark-submit shell script, the Driver JVM process, and potentially additional processes (Python workers for PySpark, external shuffle service). If you send SIGTERM only to the top-level spark-submit PID, the shell script exits but the Driver JVM (which is a child process with its own PID) may continue running orphaned.

Process groups solve this. Every process belongs to a process group identified by a PGID. By default, a forked child inherits its parent's process group. If you set up a dedicated process group for the Spark application (`os.setpgrp()` or `setsid()` in the child), you can send a signal to the entire group with one syscall: `os.killpg(pgid, signal.SIGTERM)` or equivalently `kill(-pgid, SIGTERM)` at the C level. This simultaneously delivers SIGTERM to every process in the group — the shell script, the Driver JVM, and any Python workers — ensuring none are left orphaned.

In practice, Airflow's `SparkSubmitOperator` uses exactly this pattern. It calls `spark-submit` in a subprocess with `preexec_fn=os.setpgrp` (a new process group whose PGID equals the subprocess's PID). When the operator receives a cancellation request (SIGUSR1 or SIGTERM from the worker), it calls `os.killpg(proc.pid, signal.SIGTERM)` to kill the entire Spark process group atomically. After waiting for the group to exit, it optionally calls `os.killpg(proc.pid, signal.SIGKILL)` if any processes remain. This ensures the Driver, executor-side processes, and all intermediate processes all receive the shutdown signal, with no orphaned JVMs consuming cluster resources after the Airflow task has been marked as failed.

---

## 17. Cross-Question Chain

**Q1 [Interviewer]: What happens when a Linux process receives SIGTERM?**

When a process receives SIGTERM, the kernel sets a pending signal bit in the process's task_struct. At the next opportunity — when the process transitions from kernel mode to user mode after a syscall or interrupt — the kernel checks pending signals against the thread's signal mask. If SIGTERM is not blocked, the kernel consults the process's signal action table. There are three possibilities: if the action is SIG_DFL (default), the kernel terminates the process; if SIG_IGN, the signal is discarded; if a user-provided function pointer, the kernel sets up a signal stack frame in user space and jumps to the handler. The handler runs in user mode in the main thread (or whichever thread is eligible). When the handler returns, execution resumes from where it was interrupted (after any interrupted system calls return EINTR).

**Q2 [Interviewer]: You said "any interrupted system calls return EINTR" — what does that mean for a Kafka consumer blocked in poll()?**

When a blocking system call — like `epoll_wait()` inside Kafka's consumer poll loop — is interrupted by a signal delivery, the kernel returns to user space immediately with the error code EINTR (Interrupted System Call). The system call did not complete; it returned early. In C, programs handle this by checking the return value and retrying: `if (ret < 0 && errno == EINTR) goto retry;`. Java's NIO layer handles EINTR internally — `selector.select()` retries automatically on EINTR. Python's socket and file I/O calls also retry on EINTR (PEP 475, implemented in Python 3.5+). But the key point is: when the Python signal handler runs (triggered by the EINTR), it runs between Python bytecodes. If the handler set `_shutdown = True`, then on the next bytecode check, the main thread resumes from the point after `consumer.poll()` returns. The poll loop then checks `if not running: break` and exits. `consumer.poll()` may return `None` (empty result) or a message that arrived before the signal; the application processes any returned messages, then exits the loop cleanly.

**Q3 [Interviewer]: So SIGTERM gives the consumer time to clean up. What if cleanup takes too long and Kubernetes sends SIGKILL?**

The 30-second terminationGracePeriodSeconds window is non-negotiable: after 30 seconds, SIGKILL arrives and no further code runs. This means the cleanup budget is fixed. If `consumer.close()` takes 25 seconds (say, it's waiting for a slow commit acknowledgment from the broker), and there's 5 seconds of other cleanup, the total is exactly at the limit — risky. To be safe, every cleanup step should have an explicit timeout. For Kafka consumers: `consumer.close()` with a timeout parameter (`close(timeout=10.0)` in confluent-kafka) will abandon the LeaveGroup if the broker doesn't acknowledge within 10 seconds — still triggering a rebalance, but the process exits cleanly. For database connections: use a connection timeout in the commit call. For subprocess termination: SIGTERM with a 15-second wait, then SIGKILL. The GracefulShutdown pattern enforces this: each cleanup callback runs with a per-callback timeout, and there is a total deadline beyond which remaining callbacks are skipped. The application then calls `sys.exit(0)` or returns from main, which causes normal JVM/Python exit — well within the 30-second window.

**Q4 [Interviewer]: Describe how pipes provide backpressure in a Python multiprocessing pipeline.**

Backpressure is the mechanism by which a slow consumer signals a fast producer to slow down, preventing unbounded queue growth and memory exhaustion. In a pipe-based pipeline, backpressure is provided by the kernel's pipe buffer. The kernel pipe buffer is 64 KB on Linux. When the producer writes faster than the consumer reads, the kernel buffer fills. Once full, the producer's next `write()` call blocks — the producer thread or process is descheduled and placed in a wait queue. It will not resume until the consumer reads data out of the pipe, creating space. The producer is naturally throttled to the consumer's processing rate without any application-level logic.

`multiprocessing.Queue` uses a pipe under the hood but adds a complication: `queue.put()` writes to an in-process deque (serviced by a background feeder thread), not directly to the pipe. With `maxsize=0` (the default — meaning unbounded), the deque grows without limit regardless of whether the pipe is full. The application is the bottleneck: Python's GC eventually fails if the deque consumes all memory. With `maxsize=N`, `put()` blocks when N items are in the deque, providing application-level backpressure. But the kernel-level backpressure (pipe buffer full) is the backstop: if the feeder thread fills the pipe, it blocks too, and the deque cannot grow further. For robust pipelines, set `maxsize` explicitly — for example `Queue(maxsize=1000)` — and design the producer to handle `queue.Full` exceptions by waiting with a timeout and checking the shutdown flag.

**Q5 [Interviewer]: Your Spark shuffle pipeline is producing millions of small records between executors. What IPC mechanism would you choose?**

For Spark shuffle between executors on the same machine, the data path is actually well-defined: Spark writes shuffle output to local disk (shuffle files) and serves them via HTTP over the shuffle block manager or external shuffle service. The transport is TCP sockets, even on the same machine, because Spark is designed for distributed execution and uses the same code path regardless of locality. This is suboptimal for local execution but acceptable because Spark's primary deployment is multi-machine.

For a pure same-machine high-throughput record passing scenario — say, between a JVM process and Python UDF workers within one Spark executor — the correct answer is Arrow IPC over a Unix domain socket or shared memory. Apache Arrow's Python-JVM bridge in PySpark uses exactly this: the JVM serializes a partition as an Arrow IPC stream, sends it to the Python worker via a socket (loopback or UDS), and Python reads it without deserialization overhead. For the highest throughput, you'd skip the copy entirely using shared memory: the JVM writes Arrow columnar data to `/dev/shm/`, and Python maps the same region via mmap. The Python process accesses the data without any copy — the Arrow arrays' memory buffers point directly into the shared memory pages. This is the design of Apache Arrow's Plasma store: a shared-memory object cache that JVM and Python processes access via a UDS control channel and zero-copy mmap for data. For millions of records, shared memory + Arrow IPC is the correct answer: it eliminates both the serialization overhead (Arrow is already columnar binary) and the copy overhead (shared memory = zero copies).

**Q6 [Interviewer]: Design the complete signal-handling and shutdown strategy for an Airflow worker that runs Spark jobs via SparkSubmitOperator.**

The design must address three failure modes: normal shutdown (SIGTERM from Kubernetes), operator cancellation (SIGTERM from user clicking "Clear" in the Airflow UI), and force-kill (SIGKILL from Kubernetes after timeout or OOM).

**Normal shutdown (SIGTERM to worker pod):** The Airflow worker registers a SIGTERM handler that sets `num_runs = 0` — the worker stops accepting new tasks from Celery/Redis. Currently running tasks are allowed to continue. The worker sends SIGTERM to each active task runner subprocess. Each task runner calls `operator.on_kill()` — for SparkSubmitOperator, this sends a `spark.stop()` command via the Spark REST API, triggering a controlled Spark shutdown (SparkContext.stop() → executors receive StopExecutors → JVM exits). The task runner then marks the task as "failed" in the metadata DB. The worker waits for all task processes to exit (with a 20-second timeout), then calls `worker.stop()`, disconnects from the broker, and exits. Total time budget: 25 seconds (leaving 5 seconds margin for the 30-second SIGKILL window).

**Operator cancellation (SIGUSR1 to task runner):** Airflow distinguishes kill-for-failure (SIGTERM) from kill-for-retry (SIGUSR1). When the user clicks "Clear" to re-run a task, Airflow sends SIGUSR1 to the task runner. The task runner calls `on_kill()` (same Spark cancellation), marks the task as "up_for_retry" instead of "failed," and exits. The task will be re-queued and run again.

**Force-kill (SIGKILL from Kubernetes):** No cleanup is possible. The Spark application continues running until its own heartbeat timeout detects the driver is gone (if the driver was in the worker pod). Mitigation: run the Spark driver in cluster mode (YARN/Kubernetes client submits but the driver runs in a separate cluster pod). With cluster mode, the Spark application is independent of the Airflow worker's lifecycle. SIGKILL to the worker pod does not kill the Spark driver. The SparkSubmitOperator records the application ID before exiting; on restart, Airflow can optionally check the application status and cancel it if it's still running.

**Process group management:** SparkSubmitOperator launches `spark-submit` with `preexec_fn=os.setpgrp` to create a new process group. On `on_kill()`, it calls `os.killpg(proc.pid, signal.SIGTERM)` to kill the entire group atomically — the submit shell, the driver process, and any Python worker processes. After a 20-second wait, `os.killpg(proc.pid, signal.SIGKILL)` ensures no orphaned processes remain.

---

## 18. Common Misconceptions

**"SIGTERM and SIGKILL both kill processes the same way."**
SIGTERM is a request; SIGKILL is a command. SIGTERM can be caught and allows cleanup. SIGKILL cannot be caught, blocked, or ignored — it is always delivered immediately by the kernel. A process can ignore SIGTERM forever (by registering `signal.SIG_IGN`); it cannot ignore SIGKILL. In practice, a well-written service handles SIGTERM to flush state and exit cleanly; SIGKILL is reserved for when the process is unresponsive.

**"Signal handlers can safely call any function."**
Only async-signal-safe functions are safe to call in C signal handlers. For Python, signal handlers run between bytecodes (not truly asynchronously), so Python functions work in practice — but complex operations in handlers are still risky. The safest Python signal handler is one that only sets a `threading.Event` or a global boolean, then returns. All cleanup logic runs in the main loop, not in the handler.

**"Closing a pipe automatically signals the other end with a message."**
Closing the write end of a pipe causes the read end to return 0 (EOF) on the next `read()` call. This is not a signal — it's a return value. The reader must check for `read() == 0` and handle it as "no more data." Closing the read end of a pipe causes the writer to receive SIGPIPE on the next `write()` call (or EPIPE error if SIGPIPE is ignored). These are kernel behaviors, not explicit messages.

**"`multiprocessing.Queue` with `maxsize=0` has no limit and will never block."**
With `maxsize=0`, `Queue.put()` never blocks at the Python level. But the underlying pipe has a 64 KB kernel buffer. The feeder thread that copies from the in-process deque to the pipe will eventually block when the pipe fills. What happens in practice: the in-process deque grows unboundedly in the producer's memory, consuming RAM until OOM. For production pipelines, always set `maxsize` to a non-zero value proportional to your memory budget.

**"Zombie processes consume significant resources."**
Zombies hold no memory (their heap and stack are freed), no open file descriptors, and use no CPU. They occupy only one entry in the kernel process table. The risk is not individual resource consumption — it's process table exhaustion. The Linux process table has a maximum (`/proc/sys/kernel/pid_max`, default 32768). If a process leaks thousands of zombies, `fork()` eventually fails with EAGAIN. This is rare in practice unless the zombie leak is severe.

---

## 19. Performance Reference Card

| Operation | Approximate Cost | Notes |
|---|---|---|
| `kill(pid, SIGTERM)` syscall | ~200 ns | Signal send: just a write to target's task_struct |
| Signal delivery (to same process) | ~1 µs | Mode switch + handler setup + mode switch back |
| Signal delivery (cross-process) | ~2–5 µs | Context switch may be needed |
| Pipe round-trip latency | 5–20 µs | write + read through 64 KB kernel buffer |
| Unix domain socket RTT | 5–20 µs | Same as pipe for small messages |
| TCP loopback RTT | 30–100 µs | TCP stack overhead on loopback |
| Shared memory write (1 MB) | ~20 µs | memcpy to mmap'd region ~50 GB/s |
| Pipe throughput | 5–20 GB/s | Limited by kernel copy bandwidth |
| Unix domain socket throughput | 5–20 GB/s | Same as pipe |
| Shared memory throughput | 30–50 GB/s | Limited by DRAM bandwidth, no copy |
| `multiprocessing.Queue.put()` | 10–100 µs | Pickle + deque insert + pipe write |
| Arrow IPC serialize (1M rows) | 1–10 ms | Columnar, minimal serialization |
| `consumer.close()` (Kafka) | 100–2000 ms | Wait for LeaveGroup ack from broker |
| `SparkContext.stop()` | 5–30 s | Executor shutdown + deregistration |
| Kafka broker controlled shutdown | 10–60 s | Leader election for each partition |
| SIGKILL delivery | ~0 (immediate) | Kernel action, no user-space involvement |

**Graceful shutdown time budgets:**

| System | Default SIGKILL timeout | Recommended cleanup budget |
|---|---|---|
| Kubernetes pod | 30 s | < 25 s |
| Systemd service | 90 s | < 80 s |
| Docker stop | 10 s | < 8 s |
| YARN container | 250 ms (default kill delay) | As fast as possible |
| Airflow task timeout | Configurable | Match to SIGKILL window − 5 s |

---

## 20. Connections to Other Modules

**CSF-OS-101 M01 (Processes and Threads):** Signals are delivered to processes. The PCB (task_struct) holds the signal mask, pending signal set, and signal action table. Fork/exec patterns from M01 determine signal inheritance: forked children inherit the parent's signal masks and handlers; exec() resets all handlers to SIG_DFL (but preserves the signal mask). SIGCHLD and zombie processes directly require understanding of the parent-child process relationship from M01.

**CSF-OS-101 M04 (File I/O and System Calls):** Pipes are file descriptors — all of M04's read/write/close mechanics apply to pipe fds identically to regular files. Unix domain sockets use the socket fd interface, also covered in M04. `signalfd` creates a file descriptor for signals, integrable with epoll from M04. EINTR (interrupted system call) from M04 is the direct consequence of signal delivery interrupting a blocking syscall.

**CSF-OS-101 M03 (Virtual Memory and Page Cache):** Shared memory via mmap is the IPC mechanism that maps physical pages into multiple virtual address spaces — a direct application of M03's virtual memory concepts. The mmap zero-copy property depends on the page table mappings allowing multiple virtual addresses to refer to the same physical frame.

**DCS-KFK-101 (Kafka):** Signal handling determines whether a Kafka consumer causes a 45-second rebalance delay or a 2-second one. The consumer.close() / LeaveGroup protocol, the signal-handler-sets-flag pattern, and the SIGTERM → shutdown event → poll loop → finally block structure are all directly applied in every production Kafka consumer.

**CPL-ORC-101 (Airflow):** Airflow's multi-process architecture (Scheduler, Worker, TaskRunner, Operator subprocess) is entirely governed by signal propagation. SIGTERM cascades from Kubernetes → Worker → TaskRunner → Operator process. SIGUSR1 triggers retries. SIGCHLD handles zombie cleanup. Understanding signals is prerequisite to understanding why Airflow tasks sometimes fail, retry, or hang on shutdown.

---

## 21. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What two signals cannot be caught, blocked, or ignored? | SIGKILL (9) and SIGSTOP (19). These are enforced by the kernel regardless of any signal handler registration. SIGKILL terminates; SIGSTOP pauses. |
| 2 | What is the standard graceful shutdown signal sequence? | SIGTERM first (catchable, allows cleanup), then wait T seconds, then SIGKILL (uncatchable, unconditional termination). Kubernetes default T = 30s. |
| 3 | What does SIGPIPE mean, and when does a data engineer see it? | A process wrote to a pipe/socket with no readers. Default action: terminate. Seen when: head terminates a pipeline early, a subprocess output pipe is closed before the subprocess finishes writing. Exit code 141 (128+13). |
| 4 | Why must Python signal handlers be minimal? | Python handlers run between bytecodes (not true async), but calling complex functions (logging, malloc, locks) risks deadlock if the signal interrupted those functions in the main thread. Correct pattern: set a flag only. |
| 5 | What is a zombie process? | A dead process whose PCB (task_struct) remains in the kernel process table because its parent has not called wait(). Holds no memory or CPU, but occupies a process table slot. Fix: always call waitpid() or set SIGCHLD=SIG_IGN. |
| 6 | What is the cost of NOT calling consumer.close() in Kafka? | The broker waits session.timeout.ms (default 45s) before declaring the consumer dead and triggering rebalance. Partitions are orphaned for 45s. consumer.close() sends LeaveGroup → immediate rebalance (~1-2s). |
| 7 | What is the kernel pipe buffer size and what happens when it fills? | 64 KB on Linux (configurable up to /proc/sys/fs/pipe-max-size). When full: write() blocks, providing natural backpressure. The producer is paused until the consumer reads data. |
| 8 | What is SIGUSR1 and SIGUSR2? | User-defined signals with no kernel-defined meaning. Applications assign their own semantics. Airflow: SIGUSR1 = retry task. Kafka: SIGUSR1 = flush logs. Available for any application-level notification. |
| 9 | What is `kill(-pgid, SIGTERM)` and why is it used for Spark? | Sends SIGTERM to all processes in process group `pgid` atomically. Used for Spark to kill spark-submit, the Driver JVM, and Python workers simultaneously — preventing orphaned processes when the submit script exits. |
| 10 | What is the difference between pipe and Unix domain socket? | Pipe: unidirectional, byte stream, created with pipe(), no address, connects related processes. UDS: bidirectional (or DGRAM), addressable by filesystem path, supports multiple clients, can pass file descriptors (SCM_RIGHTS). Both have ~5-20µs RTT. |
| 11 | How does multiprocessing.Queue provide backpressure? | Set maxsize=N. put() blocks when N items are in the queue. The underlying pipe buffer (64 KB) provides OS-level backpressure if the feeder thread fills the pipe. maxsize=0 (default) = unbounded → potential OOM. |
| 12 | What is SIGCHLD and when is it received? | Delivered to a parent process when a child changes state: exits, is stopped by SIGSTOP, or resumes with SIGCONT. Register handler to call waitpid() to reap the child and avoid zombies. Or set SIG_IGN to auto-reap. |
| 13 | What is an OOM kill and why can't Spark handle it gracefully? | The Linux OOM killer sends SIGKILL (not SIGTERM) to a process when the system or cgroup is out of memory. SIGKILL cannot be caught. No cleanup runs. Spark's JVM shutdown hooks do not execute. The driver sees the executor heartbeat time out after 120s. |
| 14 | What is the self-pipe trick? | Create a pipe(); in the signal handler, write one byte to the write end. The main loop monitors the read end with select()/epoll(). When a signal arrives, the write wakes up the event loop. Allows integrating signals into an epoll event loop without signal handlers. |
| 15 | What is the advantage of shared memory over pipes for large data? | Shared memory: zero copies — both processes map the same physical pages. Transfer cost = 0 (data is already accessible). Pipe: two kernel copies per transfer (write to kernel buffer + read from kernel buffer). At 1 GB transfer: ~40ms for pipe vs ~0ms for shm. |
| 16 | What does signal.pthread_sigmask() do? | Blocks or unblocks signals for the calling thread. Signals blocked in worker threads accumulate as pending until unblocked. Best practice: block all signals in worker threads, handle only in the main thread. |
| 17 | What is signalfd and when is it preferred over traditional signal handlers? | signalfd() creates a file descriptor that receives signals as readable data. Preferred when: integrating signal handling into an epoll/select event loop, avoiding async-signal-safety concerns, wanting synchronous signal processing. Used by systemd, modern server applications. |
| 18 | What happens to signal handlers after fork() and exec()? | fork(): child inherits parent's signal handlers and signal mask — child has the same handlers. exec(): all signal handlers that were set to user functions are reset to SIG_DFL. Signals set to SIG_IGN remain SIG_IGN. Signal mask is preserved across exec(). |
| 19 | What is Arrow IPC and why does it matter for Python UDFs in Spark? | Arrow IPC is a columnar binary format for serializing in-memory Arrow arrays. Pandas UDFs use Arrow IPC to pass a batch (partition) as one message — avoiding row-by-row pickle overhead. Result: 10-100x faster than regular Python UDFs for bulk DataFrame operations. |
| 20 | How does Airflow distinguish a task that should be retried from one that should fail? | Signal semantics: SIGUSR1 = retry (user cancelled task, re-queue it). SIGTERM = fail (orchestrator is shutting down, mark failed). The TaskRunner checks which signal it received and sets the task state accordingly in the metadata DB. |

---

## 22. Module Summary

**Signals** are asynchronous kernel notifications that interrupt a process at any point. Each signal has a default action (terminate, core dump, stop, ignore); applications override this with signal handlers. Only SIGTERM can be caught for graceful shutdown — SIGKILL is always immediate and unconditional. Signal handlers must be minimal: in C, only async-signal-safe functions; in Python, only setting a flag. All cleanup runs in the main loop, checked on every iteration.

**Graceful shutdown** follows a fixed contract: SIGTERM → cleanup window → exit, with SIGKILL as the enforcer after the timeout. The cleanup window (30 seconds in Kubernetes) must include: draining queues, committing offsets, sending LeaveGroup to Kafka brokers, canceling Spark applications via SparkContext.stop(), updating task states in Airflow's metadata DB, and sending SIGTERM to child processes. Every cleanup step needs an explicit timeout to guarantee the process exits within the window. Missing `consumer.close()` in a Kafka consumer costs a 45-second rebalance delay; missing `os.waitpid()` for child processes accumulates zombies.

**IPC mechanisms** exist on a spectrum from simple to fast. **Pipes** are the simplest: a kernel-buffered byte stream, 64 KB buffer, natural backpressure when full, 5–20 µs latency. `multiprocessing.Queue` wraps a pipe — always set `maxsize > 0` for backpressure. **Unix domain sockets** add addressing, bidirectionality, multiple clients, and SCM_RIGHTS fd passing. Same ~5–20 µs latency as pipes. epoll-compatible, DGRAM mode preserves message boundaries. **Shared memory** is the fastest: zero-copy, RAM-bandwidth limited, ~50 GB/s. Requires explicit synchronization. Used by Arrow IPC between JVM and Python workers — the mechanism behind PySpark Pandas UDFs' 10-100× speedup over regular Python UDFs.

The thread through this module and the entire CSF-OS-101 course is the same: Kafka, Spark, and Airflow are not magic — they are user-space programs running on Linux, subject to every constraint this course covered: process limits, scheduling, virtual memory, file I/O, and now signals and IPC. Understanding these primitives is what separates an engineer who can use these systems from an engineer who can diagnose why they fail at 3 AM and fix them before the morning report runs.

---

**CSF-OS-101: 5 of 5 complete. ✓**

**Next school: SYS — Linux and Networking for Data Engineers**  
**Next course: LNX-101 — Linux Administration for Data Engineers**  
*(Package management, filesystem layout, systemd, cgroups, kernel tuning for data workloads)*
