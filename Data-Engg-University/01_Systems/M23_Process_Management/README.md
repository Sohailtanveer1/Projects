# SYS-LNX-101 M02: Process Management

**Course:** SYS-LNX-101 — Linux for Data Engineers  
**Module:** 02 of 05  
**Filesystem position:** 01_Systems/M23_Process_Management  
**Prerequisites:** CSF-OS-101 M01 (Processes and Threads), SYS-LNX-101 M01 (Linux File System)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: The Linux Process Model](#4-core-theory-the-linux-process-model)
5. [ps — The Process Snapshot](#5-ps--the-process-snapshot)
6. [top and htop — Live Process Monitoring](#6-top-and-htop--live-process-monitoring)
7. [kill and Signals from the CLI](#7-kill-and-signals-from-the-cli)
8. [nice and renice — CPU Priority](#8-nice-and-renice--cpu-priority)
9. [systemd — Service Management](#9-systemd--service-management)
10. [Process Trees and Job Control](#10-process-trees-and-job-control)
11. [/proc — The Process Filesystem](#11-proc--the-process-filesystem)
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

A Kafka broker is consuming 100% of one CPU core. You SSH into the server. What do you run? If you type `ps`, you get a static snapshot sorted by PID — the Kafka JVM is buried somewhere in 200 lines of output. If you type `top`, you can see live CPU usage but you can't tell which thread inside the JVM is hot. You need `top -H -p <pid>` to show threads, or `htop` with tree view to understand the full process hierarchy. Without knowing what these tools show and how to read them, you're blind.

A Spark executor JVM is stuck — not consuming CPU but not progressing. The application has been "running" for 4 hours with no output. You need to know its state: is it sleeping? waiting on I/O? zombie? stuck in a GC pause? `ps -o stat` gives you the process state letter. `cat /proc/<pid>/wchan` tells you which kernel function it's sleeping in. `jstack <pid>` gives a Java thread dump, but only if you know the PID — which you get from `ps` or `pgrep`.

An Airflow task finishes but the systemd service reports the worker as "failed" even though the Python exit code was 0. You need to understand how systemd determines service health: it watches the main process PID, checks the exit code, and applies `RestartPolicy`. If Airflow workers are spawning children that outlive the main process, systemd may kill them with SIGKILL when the unit exits — which you'd only know by checking `systemctl status airflow-worker` and reading the `ExecStop` and `KillMode` settings.

All of these require knowing the Linux process model, the tools that expose it, and how to interpret what those tools report. This module covers all of it with production context throughout.

---

## 2. How to Use This Module

**For incident response:** Sections 5, 6, 11. `ps` and `top`/`htop` are the first tools you reach for on a slow server. Section 11 (`/proc`) gives you everything else — memory maps, open files, thread states, and the kernel function a stuck process is waiting in.

**For service management:** Section 9. systemd controls Kafka brokers, Airflow schedulers, and Spark history servers. Knowing `systemctl start/stop/status/journal` and how to write and modify unit files is essential for production deployments.

**For interview prep:** Sections 4, 7, 8, 17, 18. Process states, signal semantics, and nice/priority are classic Linux interview topics. The cross-question chain in Section 18 covers the full "a process is stuck, what do you do?" diagnostic workflow.

---

## 3. Prerequisites Check

- **Processes and threads (CSF-OS-101 M01):** This module uses the process model from M01 (PID, PCB, fork/exec, parent/child relationships, context switching). The process states (RUNNING, SLEEPING, ZOMBIE) from M01 appear in `ps` output as the `STAT` column.
- **Signals (CSF-OS-101 M05):** Section 7 covers `kill` from the CLI. The signal semantics (SIGTERM, SIGKILL, SIGHUP) are covered in M05; this module shows how to send them from the command line and how to interpret the results.
- **Basic Linux CLI (M01):** You should be comfortable with a shell, running commands, and reading man pages.

---

## 4. Core Theory: The Linux Process Model

### 4.1 Process States

Every Linux process is always in one of six states, visible in `ps` output as the `STAT` column:

| State | Letter | Meaning |
|---|---|---|
| Running | `R` | On CPU or in the run queue, ready to run |
| Interruptible sleep | `S` | Blocked waiting for an event (I/O, signal, timer); wakes on signal |
| Uninterruptible sleep | `D` | Blocked in a kernel operation that cannot be interrupted (typically I/O) |
| Zombie | `Z` | Exited but parent has not called `wait()` yet |
| Stopped | `T` | Paused by SIGSTOP or SIGTSTP (Ctrl+Z); resumes with SIGCONT |
| Tracing stop | `t` | Paused by a debugger (gdb/strace) |

Additional modifiers in the STAT column:
- `<` — high priority (negative nice value)
- `N` — low priority (positive nice value)
- `L` — has pages locked in memory (real-time processes, mlock)
- `s` — session leader (shell, daemon that created a new session)
- `l` — multithreaded (has multiple threads)
- `+` — in the foreground process group

```bash
ps aux
# USER  PID  %CPU %MEM    VSZ    RSS   STAT  START   TIME COMMAND
# kafka 1234  45.2  8.3 8388608 1638400 Sl   09:00  12:34 java -Xmx4g kafka.Kafka
#                                        ↑↑
#                                        S = sleeping (waiting for I/O)
#                                        l = multithreaded
```

### 4.2 The `D` State: Uninterruptible Sleep

The **`D` state** is the most operationally significant non-running state. A process in `D` is blocked inside a kernel system call that must complete before the process can be interrupted or killed. Common causes:

- Waiting for a disk read/write to complete (NFS, HDFS, local disk with I/O errors)
- Waiting for a kernel lock held by another process (filesystem journal lock)
- Waiting for a memory page fault to complete

**Critical property:** A process in `D` state **cannot be killed by SIGKILL**. `kill -9` is queued but not delivered until the process returns to user space — which it cannot do while stuck in `D`. This is the one scenario where `kill -9` appears to do nothing. The only fix is to resolve the underlying kernel condition (remount NFS, replace the failing disk, reboot the server).

```bash
# Detect D-state processes (hung processes)
ps aux | awk '$8 == "D" {print}'
# or
ps -eo pid,stat,comm | grep "^[[:space:]]*[0-9]* D"

# Find what kernel function the process is stuck in:
cat /proc/<pid>/wchan
# nfs4_wait_on_state   ← stuck waiting on NFS state machine
# jbd2_log_wait_commit ← stuck in ext4 journal commit (I/O issue)
# io_schedule          ← generic I/O wait
```

### 4.3 Process Identifiers

```
Every process has:
  PID  (Process ID): unique integer identifying this process
  PPID (Parent PID): PID of the process that forked this one
  PGID (Process Group ID): used for job control and kill(-pgid, sig)
  SID  (Session ID): used for terminal association and daemon isolation

Process tree example (Airflow worker node):
  PID 1 (systemd)
  └── PID 1001 (airflow-worker) [PPID=1, SID=1001, PGID=1001]
      ├── PID 2001 (airflow task runner) [PPID=1001]
      │   └── PID 3001 (spark-submit) [PPID=2001]
      │       └── PID 4001 (java Driver JVM) [PPID=3001]
      └── PID 2002 (airflow task runner) [PPID=1001]
```

---

## 5. ps — The Process Snapshot

`ps` (process status) takes a snapshot of current processes. Its syntax has two modes: BSD-style (no dash: `ps aux`) and POSIX-style (with dash: `ps -ef`). Both are widely used; `ps aux` is more common in practice.

### 5.1 Essential ps Formats

```bash
# Most common: all processes, user-oriented format
ps aux
# USER    PID  %CPU %MEM    VSZ   RSS TTY STAT  START  TIME COMMAND

# POSIX style: all processes, full format
ps -ef
# UID     PID  PPID C STIME TTY  TIME CMD

# With parent PID visible (useful for process tree debugging)
ps -ef | head -5
# UID    PID  PPID  C STIME  TTY   TIME    CMD
# root     1     0  0 Jun26  ?   00:00:04  /sbin/init

# Long format with all threads visible
ps -eLf
# UID    PID  PPID   LWP  C NLWP STIME TTY   TIME    CMD
# kafka 1234  1233  1234  0  157 09:00  ?   00:00:02  java -Xmx4g kafka.Kafka
# kafka 1234  1233  1235  0  157 09:00  ?   00:12:45  java -Xmx4g kafka.Kafka
# LWP = Light Weight Process (thread ID), NLWP = number of threads

# Custom output format: exactly the columns you need
ps -eo pid,ppid,user,stat,pcpu,pmem,comm,args --sort=-pcpu | head -20
# -o: custom format; --sort=-pcpu: sort by CPU descending

# Columns reference:
# pid:     process ID
# ppid:    parent PID
# pgid:    process group ID
# sid:     session ID
# user:    username
# uid:     numeric UID
# stat:    process state (R, S, D, Z, T + modifiers)
# pcpu:    CPU usage % (averaged since process start)
# pmem:    physical memory % of total RAM
# vsz:     virtual memory size (KB) — all claimed virtual space
# rss:     resident set size (KB) — physical pages currently in RAM
# comm:    command name (truncated, no args)
# args:    full command with arguments
# etime:   elapsed time since process started (HH:MM:SS)
# lstart:  exact start time (human readable)
# wchan:   kernel function the process is sleeping in
# nlwp:    number of threads
# ni:      nice value (-20 to 19)
# pri:     kernel priority (lower = higher priority)
```

### 5.2 Filtering with pgrep

```bash
# Find PIDs by name (cleaner than ps | grep)
pgrep kafka           # prints PIDs of processes named 'kafka'
pgrep -l kafka        # PIDs + names
pgrep -a kafka        # PIDs + full command lines
pgrep -u spark java   # Java processes running as the 'spark' user
pgrep -P 1234         # child processes of PID 1234

# pidof: find PID of a specific program by exact binary name
pidof java            # all Java JVMs
pidof python3

# Find process by listening port (requires ss or lsof)
ss -tlnp | grep 9092  # find process listening on Kafka port 9092
# LISTEN 0 128 0.0.0.0:9092 0.0.0.0:* users:(("java",pid=1234,fd=137))
```

### 5.3 Reading ps Output for Data Engineering

```bash
# Examine a Kafka broker process
ps aux | grep kafka
# kafka  1234 45.2  8.3 8388608 1638400 ?  Sl  09:00 12:34 java -server -Xmx4g -Xms4g \
#   -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
#   -Dkafka.logs.dir=/var/log/kafka \
#   kafka.Kafka /etc/kafka/server.properties

# Reading the output:
# VSZ = 8388608 KB = 8 GB (all virtual memory claimed, including JVM reserved space)
# RSS = 1638400 KB = 1.6 GB (physical RAM currently used)
# STAT = Sl: S=sleeping (waiting for I/O or connections), l=multithreaded
# TIME = 12:34 (12 hours 34 minutes of total CPU time consumed since start)
# %CPU = 45.2 (currently using 45.2% of ONE core)

# Check all Java processes with memory
ps -eo pid,user,rss,vsz,pcpu,comm,args | awk '$6=="java" {printf "%d\t%s\t%.1f GB\t%.1f GB\t%s%%\t%s\n", $1,$2,$3/1048576,$4/1048576,$5,$7}' | head -20

# Find stuck processes (D state = uninterruptible sleep)
ps -eo pid,stat,wchan,comm | awk '$2 ~ /D/ {print}'

# Find zombie processes
ps -eo pid,ppid,stat,comm | awk '$3 ~ /Z/ {print}'
```

---

## 6. top and htop — Live Process Monitoring

### 6.1 top: Built-in Linux Process Monitor

`top` updates the display every 3 seconds (configurable with `-d`). Press `?` inside top for help.

```
top - 10:23:45 up 14 days,  2:15,  3 users,  load average: 2.45, 2.12, 1.98
Tasks: 312 total,   2 running, 310 sleeping,   0 stopped,   0 zombie
%Cpu(s): 23.5 us,  2.1 sy,  0.0 ni, 73.8 id,  0.4 wa,  0.0 hi,  0.2 si,  0.0 st
MiB Mem :  64008.5 total,   1024.2 free,  48234.1 used,  14750.2 buff/cache
MiB Swap:   4096.0 total,   3982.4 free,    113.6 used.  14234.1 avail Mem

  PID USER     PR  NI    VIRT    RES  SHR S  %CPU  %MEM     TIME+ COMMAND
 1234 kafka    20   0    8.0g   1.6g 128m S  44.7   2.5  12:34.56 java
 2345 spark    20   0   16.0g   8.0g 256m S  32.1   12.8 08:12.34 java
 3456 airflow  20   0    512m   256m  64m S   2.3   0.4   0:45.12 python3
```

**Reading the header:**
- `load average: 2.45, 2.12, 1.98` — average number of runnable processes over 1/5/15 minutes. On a 4-core machine: load > 4 means CPU-saturated. On a 32-core machine: load up to 32 is fine.
- `%Cpu(s): 23.5 us` — user-space CPU; `2.1 sy` — kernel/syscall CPU; `0.4 wa` — CPU waiting for I/O (I/O wait); `0.0 st` — stolen by hypervisor (VM overhead).
- `wa` > 5%: I/O bottleneck. `us` near 100%: CPU-bound workload. `st` > 5%: VM is oversubscribed.
- `buff/cache`: OS page cache + buffer cache (14.7 GB here) — this memory is used but instantly freeable; it's NOT memory pressure.

**Key `top` interactive commands:**
```
1          — toggle per-CPU display (shows all cores individually)
H          — toggle thread display (shows individual threads instead of processes)
M          — sort by memory (%MEM)
P          — sort by CPU (%CPU, default)
T          — sort by time (cumulative CPU TIME)
k          — kill a process (prompts for PID and signal)
r          — renice a process (prompts for PID and new nice value)
u          — filter by user (prompts for username)
e          — cycle through memory units (KB → MB → GB → TB)
c          — toggle full command line display
f          — field management (add/remove columns)
d          — change refresh interval
q          — quit
```

**Critical top commands for data engineering:**

```bash
# Show per-core CPU (detect if one core is maxed while others idle)
top    # then press '1'
# Useful: one JVM GC thread pegging a single core

# Show threads of a specific process (find hot thread inside a JVM)
top -H -p 1234
# -H: show threads instead of processes
# -p: filter to one PID
# Shows all JVM threads with their individual CPU usage

# Non-interactive: capture top output for logging/alerting
top -b -n 5 -d 2 > /tmp/top_snapshot.txt
# -b: batch mode (non-interactive output)
# -n 5: run 5 iterations
# -d 2: 2-second interval between iterations

# Watch a single process
top -b -n 60 -d 1 -p $(pgrep -n java) | grep java
```

### 6.2 htop: The Interactive Upgrade

`htop` is not installed by default but is available in all package managers. It provides:
- Horizontal CPU/memory bars for each core
- Mouse support for clicking and scrolling
- Tree view (F5) showing parent/child relationships
- F6 sort by any column
- F9 kill (with signal picker)
- Filtering by user, command text
- Process tree collapsing

```bash
# Install
apt-get install htop    # Debian/Ubuntu
yum install htop        # RHEL/CentOS

# Run
htop

# Key bindings:
# F2: setup (configure columns, colors)
# F3: search by process name
# F4: filter (show only matching processes)
# F5: tree view (shows parent/child hierarchy)
# F6: sort by column
# F7 / F8: decrease / increase nice value
# F9: kill (shows signal picker, default SIGTERM)
# F10: quit
# Space: tag a process (for batch operations)
# t: toggle tree view
# u: filter by user
# H: toggle kernel threads display
# K: toggle user threads display
```

### 6.3 Interpreting CPU Metrics for JVM Processes

A Spark or Kafka JVM consumes CPU in ways that look surprising:

```bash
# Kafka JVM with 32 threads showing in top:
# PID   USER  STAT  %CPU  %MEM  COMMAND
# 1234  kafka  Sl   350.0  8.3  java -Xmx4g kafka.Kafka

# %CPU > 100 is CORRECT: it means the process is using 3.5 CPU cores simultaneously
# (top shows per-process CPU summed across all cores)
# One core = 100%, so 350% = 3.5 cores being used

# To see which THREADS are using CPU:
top -H -p 1234
# PID   USER  STAT  %CPU  COMMAND
# 1234  kafka  S    0.3   java     ← main thread
# 1267  kafka  S    45.0  java     ← this thread is hot!
# 1268  kafka  S    44.8  java     ← this thread too

# Convert thread ID to Java stack trace (Linux TID → Java NID):
# Linux thread ID 1267 in hex = 0x4f3
# In jstack output: "kafka-network-thread-0" #42 ... nid=0x4f3

# One-liner: identify the hottest Java thread
HOTPID=1234
HOTTID=$(top -b -n 1 -H -p $HOTPID | awk 'NR>7 {print $1, $9}' | sort -k2 -rn | head -1 | awk '{print $1}')
printf "0x%x\n" $HOTTID   # convert to hex for jstack
jstack $HOTPID | grep -A 20 "nid=$(printf '0x%x' $HOTTID)"
```

---

## 7. kill and Signals from the CLI

### 7.1 kill, killall, pkill

```bash
# kill: send a signal to a specific PID
kill 1234           # default: SIGTERM (15)
kill -TERM 1234     # explicit SIGTERM
kill -15 1234       # by number
kill -SIGTERM 1234  # with SIG prefix

kill -KILL 1234     # SIGKILL: cannot be caught, blocked, or ignored
kill -9 1234        # same as SIGKILL
kill -9 -1234       # send to entire process GROUP (negative PID = group)

# List all available signals
kill -l
# 1) SIGHUP  2) SIGINT  3) SIGQUIT  4) SIGILL  5) SIGTRAP
# 6) SIGABRT 7) SIGBUS  8) SIGFPE   9) SIGKILL 10) SIGUSR1
# 11) SIGSEGV 12) SIGUSR2 13) SIGPIPE 14) SIGALRM 15) SIGTERM
# ...

# killall: send signal to all processes matching a name
killall kafka         # SIGTERM to all processes named exactly "kafka"
killall -9 java       # SIGKILL all java processes (dangerous on shared server)
killall -u spark java # SIGTERM all java processes owned by 'spark' user
killall -v kafka      # verbose: print each PID killed

# pkill: send signal to processes matching a pattern
pkill kafka             # SIGTERM to processes matching "kafka"
pkill -9 -u spark java  # SIGKILL all java owned by spark
pkill -TERM -x kafka    # -x: exact match (not substring)
pkill -f "spark-submit" # -f: match against full command line (not just name)
                        # catches processes where 'spark-submit' appears anywhere in args

# Print what would be killed without killing:
pgrep -l -f "spark-submit"   # preview before pkill -f

# Send SIGHUP to reload config (Kafka, Nginx, etc.)
kill -HUP $(cat /var/run/kafka/kafka.pid)
# Or:
pkill -HUP -x java   # if only one Java JVM is running
```

### 7.2 Sending Specific Signals for Data Engineering

```bash
# Graceful shutdown (the right way)
kill -TERM $(pgrep -x java)     # SIGTERM → JVM shutdown hooks run
sleep 30
kill -KILL $(pgrep -x java)     # SIGKILL if still running after 30s
# Note: this is exactly what systemd's stop sequence does

# Reload Kafka log4j config without restart
kill -HUP $(cat /var/run/kafka/kafka.pid)

# Force JVM thread dump to logs (for hung JVM debugging)
kill -QUIT $(pgrep -f kafka.Kafka)   # SIGQUIT → JVM prints thread dump to stderr
# Or for more control: jstack <pid>

# Dump JVM heap for OOM analysis (JVM-specific)
# Method 1: via jcmd (preferred, attached to live JVM)
jcmd $(pgrep -f kafka.Kafka) GC.heap_dump /tmp/heap_dump.hprof
# Method 2: via kill signal (if -XX:+HeapDumpOnOutOfMemoryError not set)
jcmd $(pgrep -f kafka.Kafka) VM.command_line   # verify JVM flags first

# Trigger Python process to dump its current state via SIGUSR1
# (if the Python process has a SIGUSR1 handler that dumps state)
kill -USR1 $(pgrep -f airflow)
```

### 7.3 The Two-Phase Kill Pattern

The correct production kill sequence — always try SIGTERM first:

```bash
#!/bin/bash
# graceful_kill.sh — SIGTERM then SIGKILL with configurable timeout

PID=${1:?Usage: graceful_kill.sh PID [TIMEOUT_SECONDS]}
TIMEOUT=${2:-30}

if ! kill -0 "$PID" 2>/dev/null; then
    echo "Process $PID not found"
    exit 0
fi

echo "Sending SIGTERM to PID $PID..."
kill -TERM "$PID"

# Wait for the process to exit
elapsed=0
while kill -0 "$PID" 2>/dev/null; do
    sleep 1
    elapsed=$((elapsed + 1))
    if [ $elapsed -ge $TIMEOUT ]; then
        echo "Process $PID did not exit within ${TIMEOUT}s, sending SIGKILL..."
        kill -KILL "$PID"
        wait "$PID" 2>/dev/null
        echo "Process $PID killed"
        exit 1
    fi
done

echo "Process $PID exited cleanly after ${elapsed}s"
exit 0
```

---

## 8. nice and renice — CPU Priority

### 8.1 Nice Values

The **nice value** is a user-space hint to the Linux CFS scheduler about a process's relative priority. It ranges from **-20 (highest priority) to +19 (lowest priority)**. The default is 0.

The name "nice" comes from the idea of a process being "nice" to other processes — a high nice value means "I'm being nice, give other processes more CPU." A negative nice value means "I'm not nice — give me more CPU."

```bash
# View nice value of running processes
ps -eo pid,ni,comm | head -10
# PID  NI COMMAND
# 1     0 systemd
# 1234  0 java       ← Kafka broker at default nice
# 5678 19 backup.sh  ← backup job at lowest priority (was reniced)

# Start a process with a specific nice value
nice -n 10 python3 etl_job.py    # run with nice=10 (lower priority)
nice -n -5 python3 critical.py   # run with nice=-5 (higher priority; requires sudo for negative)
nice python3 batch_job.py        # default: nice=10 (nice with no -n defaults to +10)

# Change nice of a running process
renice -n 5 -p 1234              # set PID 1234 to nice=5
renice -n -10 -p 1234            # set nice=-10 (requires root)
renice -n 15 -u spark            # set all processes owned by 'spark' to nice=15

# Check the scheduler's actual priority (20 + nice, shown as PR in top)
# nice=-20 → PR=0  (highest)
# nice=0   → PR=20 (default)
# nice=19  → PR=39 (lowest)
```

### 8.2 Nice Values in Data Engineering

Nice values control which processes win when CPU is contested:

```bash
# Production pattern: run batch ETL at low priority so interactive queries are responsive
nice -n 15 spark-submit \
    --class com.company.BatchETL \
    --master yarn \
    /opt/spark/jobs/batch_etl.jar

# Set Kafka broker to slightly higher priority than default
# (useful on shared servers where Kafka shares a node with other services)
# Kafka runs as 'kafka' user; in its systemd unit:
# [Service]
# Nice=-5

# Set backup/compaction jobs to idle priority
nice -n 19 hdfs dfs -setrep 2 /data/old_partitions/
ionice -c 3 hdfs balancer    # also set I/O to idle class

# Why this matters:
# Kafka broker at nice=0, batch job at nice=0, both CPU-bound:
# → Each gets 50% of CPU → Kafka latency spikes during batch run
# Batch job at nice=15:
# → Kafka gets ~85% CPU; batch gets 15% when Kafka is idle
# → No latency impact on Kafka during batch
```

### 8.3 ionice — I/O Priority

`ionice` complements `nice` for I/O scheduling:

```bash
# I/O scheduling classes:
# 1: RT (Real Time) — gets I/O first (dangerous; use only for time-critical processes)
# 2: BE (Best Effort) — default; integer priority 0 (highest) to 7 (lowest)
# 3: Idle — only gets I/O when disk is idle

# Set a process to idle I/O priority
ionice -c 3 -p $(pgrep -f hadoop_balancer)

# Start a process with idle I/O priority
ionice -c 3 python3 hdfs_compaction.py

# Best effort, low priority (level 7 = lowest within BE class)
ionice -c 2 -n 7 -p $(pgrep rsync)

# Check current I/O priority
ionice -p 1234
# none: prio 0     ← default (no ionice set; uses nice value to derive I/O priority)
# idle             ← ionice -c 3 was set
```

---

## 9. systemd — Service Management

### 9.1 systemd Concepts

**systemd** is the init system and service manager on modern Linux distributions (RHEL 7+, Ubuntu 16+, Debian 8+, CentOS 7+). It manages system services as **units** — configuration files that describe how to start, stop, and monitor a process.

Key unit types:
- `.service` — a daemon or one-shot process (Kafka broker, Airflow scheduler)
- `.target` — a group of units (multi-user.target = normal system state)
- `.timer` — scheduled execution (like cron, but systemd-managed)
- `.socket` — socket-activated service (start service when first connection arrives)
- `.mount` — filesystem mount points

Units are stored in:
- `/lib/systemd/system/` — package-installed units (do not edit)
- `/etc/systemd/system/` — administrator-defined units (edit here; overrides /lib)
- `~/.config/systemd/user/` — user-level units (for non-root services)

### 9.2 Essential systemctl Commands

```bash
# ── Service lifecycle ────────────────────────────────────────────────────────
systemctl start kafka              # start the service now
systemctl stop kafka               # stop the service (SIGTERM → SIGKILL)
systemctl restart kafka            # stop then start
systemctl reload kafka             # send SIGHUP (reload config without restart)
systemctl status kafka             # current status + last 10 log lines

# ── Enable/disable (auto-start at boot) ──────────────────────────────────────
systemctl enable kafka             # create symlink: starts at boot
systemctl disable kafka            # remove symlink: does not start at boot
systemctl enable --now kafka       # enable AND start immediately
systemctl is-enabled kafka         # check: enabled / disabled / static

# ── Inspect units ─────────────────────────────────────────────────────────────
systemctl list-units               # all active units
systemctl list-units --type=service --state=running  # running services
systemctl list-units --failed      # failed units (start here after a restart)
systemctl cat kafka                # show the unit file content
systemctl show kafka               # all properties (machine-readable)
systemctl show kafka --property=MainPID  # single property

# ── Reload unit files after editing ──────────────────────────────────────────
systemctl daemon-reload            # reload unit files from disk (required after editing)
systemctl edit kafka               # create override file in /etc/systemd/system/kafka.service.d/

# ── Dependency inspection ─────────────────────────────────────────────────────
systemctl list-dependencies kafka  # what kafka requires
systemctl list-dependencies --reverse kafka  # what requires kafka
```

### 9.3 Reading systemctl status Output

```bash
systemctl status kafka
# ● kafka.service - Apache Kafka broker
#    Loaded: loaded (/etc/systemd/system/kafka.service; enabled; vendor preset: disabled)
#    Active: active (running) since Mon 2026-06-27 09:00:00 UTC; 1h 23min ago
#  Main PID: 1234 (java)
#    CGroup: /system.slice/kafka.service
#            ├─1234 java -server -Xmx4g -Xms4g ...kafka.Kafka
#            └─1267 java ...  (thread shown as separate process by cgroups)
#
# Jun 27 09:00:01 server01 kafka[1234]: [2026-06-27 09:00:01,234] INFO Started (kafka.server.KafkaServer)
# Jun 27 09:00:02 server01 kafka[1234]: [2026-06-27 09:00:02,345] INFO [KafkaServer id=1] started
# Jun 27 09:15:45 server01 kafka[1234]: [2026-06-27 09:15:45,678] WARN Slow DNS resolution...

# Key fields:
# Loaded: loaded = unit file found; enabled = starts at boot
# Active: active (running) = currently running; active (exited) = oneshot completed;
#         failed = last run exited non-zero; inactive (dead) = stopped
# Main PID: the primary process systemd monitors for health
# CGroup: the cgroup tree (all processes in this service are here)
```

### 9.4 journalctl — Reading Service Logs

All systemd service output (stdout + stderr) goes to the systemd journal. `journalctl` queries it:

```bash
# View logs for a specific service
journalctl -u kafka                        # all logs
journalctl -u kafka -n 100                 # last 100 lines
journalctl -u kafka -f                     # follow (like tail -f)
journalctl -u kafka --since "1 hour ago"   # last hour
journalctl -u kafka --since "2026-06-27 09:00:00" --until "2026-06-27 10:00:00"
journalctl -u kafka -p err                 # only error-level messages
journalctl -u kafka -p warning..err        # warning to error
journalctl -u kafka --no-pager             # disable paging (for scripting)

# Multiple services
journalctl -u kafka -u zookeeper           # both services
journalctl -u "airflow-*"                  # glob pattern

# System-wide (all services)
journalctl -b                              # since last boot
journalctl -b -1                           # previous boot (use for crash analysis)
journalctl --since "2026-06-27" -p err    # all errors today

# Output format
journalctl -u kafka -o json               # JSON output for log aggregation
journalctl -u kafka -o json-pretty        # pretty-printed JSON
journalctl -u kafka -o cat                # only message text (no metadata)
```

### 9.5 Writing a systemd Unit File

```ini
# /etc/systemd/system/airflow-worker.service
# Airflow Celery worker unit file

[Unit]
Description=Airflow Celery Worker
Documentation=https://airflow.apache.org/docs/
After=network.target postgresql.service rabbitmq.service
Requires=network.target
# After: start after these units
# Requires: fail if these units fail

[Service]
# Identity
User=airflow
Group=airflow
UMask=0022

# Working directory and environment
WorkingDirectory=/opt/airflow
EnvironmentFile=/etc/airflow/airflow.env
# EnvironmentFile: loads KEY=VALUE pairs as environment variables

# Process execution
ExecStart=/opt/airflow/venv/bin/airflow celery worker --concurrency 8
ExecStop=/opt/airflow/venv/bin/airflow celery stop
# If ExecStop not specified: systemd sends SIGTERM to MainPID

# Restart behavior
Restart=on-failure           # restart if exit code != 0 (not on success or SIGTERM stop)
RestartSec=10s               # wait 10s before restarting
StartLimitIntervalSec=5min   # within 5 minutes...
StartLimitBurst=3            # ...allow max 3 restarts (then stop trying)

# Timeouts
TimeoutStartSec=120          # fail if not started within 120s
TimeoutStopSec=60            # wait 60s for stop before SIGKILL
                             # (gives Airflow time to drain current tasks)

# Resource limits
LimitNOFILE=65536            # max open file descriptors (CRITICAL for Spark shuffle)
LimitNPROC=32768             # max processes/threads
LimitCORE=infinity           # allow core dumps for debugging

# Process management
KillMode=control-group       # kill ALL processes in the cgroup on stop
                             # (not just MainPID)
KillSignal=SIGTERM           # first signal on stop
SendSIGKILL=yes              # send SIGKILL if not dead after TimeoutStopSec
SuccessExitStatus=143        # SIGTERM exit (128+15) counts as success (not failure)
                             # Prevents spurious "failed" status on clean shutdown

# Security
NoNewPrivileges=yes          # prevent privilege escalation
PrivateTmp=yes               # private /tmp namespace

# Logging
StandardOutput=journal       # stdout → systemd journal
StandardError=journal        # stderr → systemd journal
SyslogIdentifier=airflow-worker  # tag in journal

[Install]
WantedBy=multi-user.target   # enable as part of normal system startup
```

```bash
# After creating the unit file:
systemctl daemon-reload                     # reload unit files
systemctl enable --now airflow-worker       # enable and start
systemctl status airflow-worker             # verify

# Edit an existing unit without modifying /lib/systemd:
systemctl edit kafka                        # opens /etc/systemd/system/kafka.service.d/override.conf
# Add overrides, e.g.:
# [Service]
# LimitNOFILE=131072       ← override the file descriptor limit
# After editing:
systemctl daemon-reload
systemctl restart kafka
```

---

## 10. Process Trees and Job Control

### 10.1 pstree — Visualize the Process Hierarchy

```bash
# Show full process tree
pstree -p         # with PIDs
pstree -u         # with usernames
pstree -a         # with command arguments
pstree -p 1234    # subtree rooted at PID 1234

# Example output for an Airflow worker:
pstree -p $(pgrep -n airflow)
# airflow-worker(1001)─┬─airflow-task-(2001)─┬─spark-submit(3001)─java(4001)─┬─{GC thread}(4002)
#                      │                      └─python3(3002)                  └─...
#                      └─airflow-task-(2002)───python3(3003)
```

### 10.2 Job Control: fg, bg, &, nohup

```bash
# Run a long job in the background
spark-submit --class MyJob app.jar &
# → [1] 5678   ← job number [1], PID 5678
# → shell prompt returns immediately

# List background jobs
jobs
# [1]+  Running    spark-submit --class MyJob app.jar &

# Bring to foreground
fg %1       # bring job 1 to foreground
fg          # bring most recent job to foreground

# Send to background (after Ctrl+Z to pause)
# Ctrl+Z → suspends current foreground process (sends SIGTSTP)
bg %1       # resume job 1 in background

# nohup: prevent SIGHUP from terminal disconnect
nohup spark-submit --class MyJob app.jar > spark.log 2>&1 &
# Without nohup: if SSH session disconnects, SIGHUP kills the job
# With nohup: SIGHUP is ignored; output goes to nohup.out (or redirected)

# disown: detach a running background job from the shell
spark-submit --class MyJob app.jar &
disown %1   # now the job survives shell exit even without nohup

# screen / tmux: the production solution
# Start a persistent session
tmux new-session -d -s spark_job
tmux send-keys -t spark_job "spark-submit --class MyJob app.jar" Enter
# Detach: Ctrl+B, D
# Reattach: tmux attach -t spark_job
```

---

## 11. /proc — The Process Filesystem

`/proc` is a virtual filesystem (no disk storage) that the kernel populates with process information. Every numeric directory under `/proc` is a PID; reading files from it reads live kernel data structures.

### 11.1 Key /proc Files per Process

```bash
# /proc/<PID>/ layout:
# cmdline     — full command line (null-separated)
# comm        — process name (first 16 chars)
# cwd         — symlink to current working directory
# environ     — environment variables (null-separated)
# exe         — symlink to the executable binary
# fd/         — directory of symlinks to open file descriptors
# fdinfo/     — file descriptor metadata (offset, flags)
# io          — I/O statistics (read_bytes, write_bytes)
# limits      — resource limits (ulimits)
# maps        — virtual memory map (address ranges, permissions, files)
# mem         — process memory (readable via ptrace)
# net/        — network stats (TCP connections, UDP sockets)
# oom_score   — OOM killer score (0-1000; higher = more likely to be killed)
# oom_adj     — OOM score adjustment
# smaps       — detailed memory map with RSS/PSS per region
# stat        — process status (PID, state, PPID, CPU times, etc.)
# statm       — memory stats in pages
# status      — human-readable process status
# task/       — per-thread directories
# wchan       — kernel function the process is sleeping in

# ── Practical examples ─────────────────────────────────────────────────────────

# What is this process running?
cat /proc/1234/cmdline | tr '\0' ' '
# java -Xmx4g -Xms4g -server ... kafka.Kafka

# What files does this process have open?
ls -la /proc/1234/fd
# lrwx------ 1 kafka kafka 64 Jun 27 /proc/1234/fd/0 -> /dev/null       (stdin)
# lrwx------ 1 kafka kafka 64 Jun 27 /proc/1234/fd/1 -> /dev/null       (stdout)
# l-wx------ 1 kafka kafka 64 Jun 27 /proc/1234/fd/2 -> /var/log/kafka/server.log
# lrwx------ 1 kafka kafka 64 Jun 27 /proc/1234/fd/137 -> socket:[98765] (TCP socket)
# lr-x------ 1 kafka kafka 64 Jun 27 /proc/1234/fd/150 -> /data/kafka/topic-0/00000000.log

# Count open file descriptors (useful for fd exhaustion diagnosis)
ls /proc/1234/fd | wc -l
# 2847

# What is the process waiting for (if in S or D state)?
cat /proc/1234/wchan
# futex    ← waiting on a mutex (Java lock)
# ep_poll  ← blocked in epoll_wait (event loop, normal for Kafka)
# nfs4_wait_on_state  ← stuck on NFS (problem)

# Process memory stats
cat /proc/1234/status | grep -E 'VmRSS|VmSize|VmPeak|Threads|State'
# State:  S (sleeping)
# Threads: 157
# VmPeak: 9437184 kB   (peak virtual memory)
# VmSize: 8388608 kB   (current virtual memory)
# VmRSS:  1638400 kB   (physical RAM = RSS)

# Resource limits
cat /proc/1234/limits
# Limit                     Soft Limit    Hard Limit    Units
# Max open files            65536         65536         files
# Max processes             32768         32768         processes
# Max locked memory         unlimited     unlimited     bytes
# Max address space         unlimited     unlimited     bytes

# OOM killer score (higher score = killed first during OOM)
cat /proc/1234/oom_score
# 234

# Adjust OOM score: -1000 to 1000 (-1000 = never kill; 1000 = always kill first)
echo -500 > /proc/1234/oom_score_adj   # protect this process from OOM killer
echo 500  > /proc/1234/oom_score_adj   # make this process OOM-killable first
```

### 11.2 System-Wide /proc Files

```bash
# Load average (same as uptime/top)
cat /proc/loadavg
# 2.45 2.12 1.98 3/412 5678
# 1m   5m  15m  running/total  last_created_PID

# All processes' CPU and I/O stats in one file (used by sar, atop)
cat /proc/stat

# Memory info (same as free -m)
cat /proc/meminfo | grep -E 'MemTotal|MemFree|MemAvailable|Cached|SwapTotal|SwapFree|Dirty|Writeback'
# MemTotal:       65544192 kB
# MemFree:         1048576 kB
# MemAvailable:   14348288 kB   ← free + reclaimable cache; what apps can actually use
# Cached:         12582912 kB   ← page cache
# Dirty:            204800 kB   ← page cache pages not yet written to disk
# Writeback:         10240 kB   ← pages currently being written to disk
# SwapTotal:       4194304 kB
# SwapFree:        4069376 kB

# System-wide file descriptor usage
cat /proc/sys/fs/file-nr
# 8192    0    9223372036854775807
# open    free  max

# Check CPU topology
cat /proc/cpuinfo | grep -E 'processor|physical id|core id|cpu MHz' | head -32
```

### 11.3 lsof — List Open Files

`lsof` (list open files) cross-references `/proc/<pid>/fd` with file and socket information:

```bash
# All open files by a process
lsof -p 1234

# All files opened by a specific user
lsof -u kafka

# Which process has a specific file open (crucial for "file in use" errors)
lsof /data/kafka/topic-0/00000000000000000000.log
# COMMAND  PID  USER   FD   TYPE DEVICE SIZE/OFF   NODE NAME
# java    1234 kafka  150r   REG    8,1 1073741824  5678 /data/kafka/topic-0/...

# Which process is listening on a port
lsof -i :9092
# COMMAND  PID  USER  FD   TYPE DEVICE SIZE/OFF NODE NAME
# java    1234 kafka 137u  IPv4  98765  0t0  TCP *:9092 (LISTEN)

# All network connections
lsof -i TCP -n -P   # -n: no hostname lookup, -P: no port-to-name resolution

# Find deleted files still held open (consuming disk space despite rm)
lsof | grep deleted
# java 1234 kafka mem REG 8,1 1073741824 5678 /data/kafka/server.log (deleted)
# → disk space not freed because PID 1234 still has it open
# → solution: restart the process or send SIGHUP to reopen log file
```

---

## 12. Mental Models

### 12.1 top's Load Average: The Checkout Queue

The load average is the average number of processes either running (on CPU) or waiting to run (in the run queue) over 1, 5, and 15 minute windows. Think of it as a supermarket checkout queue: the "load" is how many customers are either being served or waiting in line. On a 4-cashier store (4 CPUs), load 4 means all cashiers are busy but the queue is empty — running at capacity. Load 8 means 4 customers are being served and 4 more are waiting — double the capacity, lines are forming. Load 1 on a 4-cashier store means 1 cashier is busy and 3 are idle — plenty of headroom. On a 32-CPU machine, load 32 is normal; load 64 is overloaded.

### 12.2 The /proc Filesystem: The Kernel's Dashboard

`/proc` is not a real filesystem on disk — it's a virtual view the kernel presents of its own internal data structures. Reading `/proc/1234/status` doesn't read from disk; it causes the kernel to format its in-memory `task_struct` for PID 1234 into a text response. This is why `/proc` reads are always instant regardless of disk I/O. It's the kernel's live dashboard — but one with no GUI, only text files.

### 12.3 systemd as an Air Traffic Controller

systemd doesn't just start and stop services — it manages their full lifecycle and dependencies. Like an air traffic controller, it knows which services must be up before others can land (dependencies), holds planes on the tarmac if the runway isn't clear (ordering), records every communication with the tower (journal), and automatically dispatches rescue if a plane crashes (Restart=on-failure). The unit file is the flight plan: it tells systemd everything it needs to know about a service's normal operation and how to handle deviations.

---

## 13. Failure Scenarios

### 13.1 Kafka Broker Stuck in D State

```
Symptom: Kafka broker is unresponsive. No consumer lag changes.
  SSH to the server: broker JVM is running but ps shows D state.

  ps aux | grep kafka
  # kafka  1234  0.0  8.3  8388608 1638400  ?  D   09:00  0:12 java ...

  cat /proc/1234/wchan
  # nfs4_wait_on_state

  Diagnosis: Kafka's log directory is on an NFS mount. The NFS server has a
  network problem — the broker is stuck waiting for NFS to respond.
  The process CANNOT be killed with SIGKILL while in D state.

  Verification:
    mount | grep nfs
    # nfs-server:/data/kafka on /data/kafka type nfs4 (rw)
    
    nfsstat -c | head -20     # check NFS client stats
    showmount -e nfs-server   # verify NFS server is reachable
    
    dmesg | tail -20
    # nfs: server nfs-server not responding, timed out

  Fix:
    Option A: Fix the NFS server (restart the NFS service, fix the network)
    → broker exits D state, resumes normally

    Option B: Force-unmount the NFS (if NFS server is permanently gone)
    umount -f -l /data/kafka   # -f: force, -l: lazy (unmount when no longer in use)
    → broker gets EIO errors on file operations → JVM throws exceptions → exits
    → now you can restart the broker pointing to local storage

  Prevention: Use LOCAL storage for Kafka log directories.
  NFS for Kafka logs is a well-known operational anti-pattern.
  XFS on local NVMe is the recommended configuration.
```

### 13.2 Zombie Process Accumulation from Airflow

```
Symptom: ps shows increasing number of <defunct> processes.
  Server running out of PIDs (fork() returns EAGAIN).

  ps aux | grep Z | grep -v grep | wc -l
  # 347

  Zombie parent identification:
    ps -eo pid,ppid,stat,comm | awk '$3=="Z" {print $2}' | sort | uniq -c | sort -rn
    # 347 1001   ← PID 1001 has 347 zombie children

  What is PID 1001?
    ps -p 1001 -o pid,ppid,comm,args
    # PID  PPID  COMM     ARGS
    # 1001    1  airflow  airflow scheduler

  Root cause: Airflow scheduler spawns subprocesses for DAG parsing.
  If the subprocess exit happens faster than the scheduler calls wait(),
  zombies accumulate. Bug in this Airflow version: child process reaper
  is not running correctly.

  Immediate fix: send SIGCHLD to the parent (triggers child reaping)
    kill -CHLD 1001
    # Causes the parent to run its SIGCHLD handler → waitpid() on children
    ps aux | grep Z | grep -v grep | wc -l
    # 0   ← zombies reaped

  Long-term fix: upgrade Airflow, or set:
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    # SIG_IGN for SIGCHLD: kernel auto-reaps children without parent calling wait()
```

### 13.3 systemd Service Won't Start: Debugging the Failure

```
Symptom: systemctl start kafka returns no error but kafka fails immediately.
  systemctl status kafka shows: Active: failed

  Diagnosis steps:
  
  Step 1: Read the status output
    systemctl status kafka
    # Active: failed (Result: exit-code) since ...
    # Process: 5678 ExecStart=/opt/kafka/bin/kafka-server-start.sh ... (code=exited, status=1)
    # 
    # Jun 27 09:00:01 server01 kafka[5678]: Error: JVM settings not found...
    # Jun 27 09:00:01 server01 kafka[5678]: Java heap space: Cannot allocate -Xmx4g

  Step 2: Read the full journal
    journalctl -u kafka -n 50
    # ... 
    # Jun 27 09:00:00 server01 kafka[5678]: Error: Could not find or load main class kafka.Kafka
    # → CLASSPATH issue: jar file not where unit file expects it

  Step 3: Try running the command manually
    sudo -u kafka /opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties
    # → Runs in foreground with visible output
    # Error: JAVA_HOME is not set and java could not be found in PATH

  Step 4: Fix: add environment variable to unit file
    systemctl edit kafka
    # [Service]
    # Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
    
    systemctl daemon-reload
    systemctl start kafka
    systemctl status kafka
    # Active: active (running)

  Common systemd startup failures:
    - Missing environment variables (JAVA_HOME, KAFKA_HEAP_OPTS)
    - Wrong user: the ExecStart binary not accessible by the unit's User=
    - Port already in use: another process on the same port
    - Missing dependencies: database/zookeeper not ready when Requires= isn't set
    - Timeout: TimeoutStartSec too short for JVM warm-up
```

### 13.4 OOM Killer Kills Spark Executor Instead of the Right Process

```
Symptom: dmesg shows OOM kills on a server running Spark and a web service.
  The web service is being killed instead of Spark executors.

  dmesg | grep -i "oom\|killed"
  # Out of memory: Kill process 7890 (python3) score 42 or sacrifice child
  # Killed process 7890 (python3) total-vm:1024000kB, anon-rss:512000kB
  
  But python3 is the monitoring agent — it has low memory.
  The Spark executor JVM (4 GB) has a lower OOM score!

  Diagnosis:
    cat /proc/$(pgrep -n java)/oom_score
    # 18
    cat /proc/$(pgrep -n python3)/oom_score   # monitoring agent
    # 42
    
  Root cause: OOM score is calculated as:
    oom_score ≈ (RSS / total_RAM) × 1000 + oom_score_adj
    
    Spark JVM: 4 GB RSS out of 32 GB total = 12.5% × 1000 = 125 + oom_score_adj
    But if Spark was started with: echo -1000 > /proc/jvm_pid/oom_score_adj
    → JVM score = 125 - 1000 = negative → OOM killer skips it
    
    The Python monitoring agent has high relative RSS% → killed first.

  Fix: Don't protect the JVM from OOM killer if it's the memory hog.
    # Remove the protection:
    echo 0 > /proc/$(pgrep -n java)/oom_score_adj
    
  Better fix: configure proper container memory limits
    # Use Kubernetes or cgroups to set a memory limit for Spark executors
    # → OOM killer can't reach the monitoring agent because Spark is in its own cgroup
    
  Kafka best practice: protect the broker from OOM
    # In Kafka's systemd unit or /proc:
    echo -900 > /proc/$(pgrep -f kafka.Kafka)/oom_score_adj
    # Other processes killed before Kafka; Kafka is more valuable to keep alive
```

---

## 14. Data Engineering Connections

### 14.1 Monitoring a Multi-Process Spark Job

When a Spark job runs on a single machine (local mode or single-worker YARN), all processes are visible in `ps`:

```bash
# See the full Spark process hierarchy
ps -eo pid,ppid,user,pcpu,rss,comm | awk '$4 > 0 || $6 ~ /java|python/ {print}' | head -30

pstree -p $(pgrep -f SparkSubmit)
# python3(5000)───spark-submit(5001)───java(5002)Driver─┬─java(5003)Executor─┬─{GC}(5004)
#                                                        │                    ├─{I/O}(5005)
#                                                        │                    └─{Worker}(5006)
#                                                        └─python3(5007)PythonWorker

# Monitor memory per process
watch -n 2 'ps -eo pid,rss,vsz,comm | grep java | awk "{printf \"%d\\t%.1f GB\\t%.1f GB\\t%s\\n\", \$1, \$2/1048576, \$3/1048576, \$4}"'

# Identify GC pressure: look for GC daemon threads in top -H -p <executor_pid>
top -H -p $(pgrep -f Executor) | grep -E "G1|GC|Concurrent"
```

### 14.2 Kafka Broker Health Diagnostics

```bash
# Is Kafka responding? (check if it's in a bad state like D or Z)
KAFKA_PID=$(pgrep -f kafka.Kafka)
echo "PID: $KAFKA_PID"
echo "State: $(cat /proc/$KAFKA_PID/status | grep ^State)"
echo "Threads: $(cat /proc/$KAFKA_PID/status | grep Threads)"
echo "RSS: $(cat /proc/$KAFKA_PID/status | grep VmRSS)"
echo "Open FDs: $(ls /proc/$KAFKA_PID/fd | wc -l)"
echo "wchan: $(cat /proc/$KAFKA_PID/wchan)"

# Expected output for healthy Kafka:
# State:  S (sleeping)   ← normal: waiting for network events via epoll
# Threads: 157
# RSS:    1638400 kB
# Open FDs: 2847
# wchan: ep_poll          ← blocked in epoll_wait, perfectly normal

# Abnormal indicators:
# State: D → stuck in kernel (check wchan for NFS/I/O issue)
# Open FDs: 65536 → at the fd limit (add LimitNOFILE to systemd unit)
# wchan: nfs4_wait_on_state → NFS problem (move log dirs to local storage)

# Check Kafka's thread dump (for hangs or deadlocks)
jstack $(pgrep -f kafka.Kafka) > /tmp/kafka_threads_$(date +%s).txt
grep -A 5 "BLOCKED" /tmp/kafka_threads_*.txt  # find blocked threads
```

### 14.3 Airflow Worker Management

```bash
# Check how many Airflow workers are running and their task counts
systemctl status airflow-worker
ps -eo pid,ppid,comm,args | grep airflow | grep -v grep

# Find all running Airflow task processes
pgrep -la -f "airflow tasks run"
# 3001 airflow tasks run my_dag task_1 2026-06-27T09:00:00+00:00
# 3002 airflow tasks run my_dag task_2 2026-06-27T09:00:00+00:00

# Check if any tasks have been running too long (potential hangs)
ps -eo pid,etime,args | grep "airflow tasks run" | awk '$2 > "01:00:00" {print "LONG RUNNING:", $0}'

# Monitor Airflow scheduler process state
watch -n 5 'ps -p $(pgrep -f "airflow scheduler") -o pid,stat,pcpu,rss,wchan'

# Check scheduler heartbeat (should update every heartbeat_rate seconds)
python3 -c "
from airflow.utils.db import provide_session
from airflow.models import LatestRun
# Or check the metadata DB directly
"
```

### 14.4 Setting Process Priorities for a Shared Data Server

```bash
# A server runs: Kafka broker (critical), Spark executor (batch), monitoring agent
# Goal: Kafka never loses CPU to batch Spark jobs

# Strategy: 
# Kafka: nice=-5 (slightly elevated priority)
# Spark executor: nice=5 (slightly reduced)
# Monitoring: nice=10 (low priority)

# Set in systemd unit files (persistent across restarts):

# /etc/systemd/system/kafka.service.d/override.conf:
# [Service]
# Nice=-5

# /etc/systemd/system/spark-executor.service.d/override.conf:
# [Service]
# Nice=5

# At runtime (for currently running processes):
renice -n -5 -p $(pgrep -f kafka.Kafka)
renice -n  5 -p $(pgrep -f CoarseGrainedExecutorBackend)
renice -n 10 -p $(pgrep -f prometheus_kafka_exporter)

# Verify
ps -eo pid,ni,comm | awk '$3 ~ /java|python/ {print}' | sort -k2n
# PID  NI  COMM
# 1234  -5  java    ← Kafka
# 2345   5  java    ← Spark
# 3456  10  python3 ← monitoring
```

---

## 15. Code Toolkit

### 15.1 `process_monitor.py` — Production Process Health Monitor

```python
"""
process_monitor.py — Monitor process health from Python.

Functions:
  1. get_process_info()    — comprehensive process metrics via /proc
  2. find_by_name()        — find processes by name or command pattern
  3. check_service_health()  — check expected process state
  4. detect_stuck_processes() — find D-state and zombie processes
  5. monitor_fd_usage()    — alert when a process is near its fd limit
  6. top_processes()       — list top N processes by CPU or memory

Run directly for a system process health report.
"""
from __future__ import annotations
import os
import re
import time
import subprocess
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ProcessInfo:
    pid: int
    name: str
    state: str          # R, S, D, Z, T
    ppid: int
    uid: int
    user: str
    cpu_pct: float      # from /proc/stat delta
    rss_mb: float
    vsz_mb: float
    threads: int
    open_fds: int
    fd_limit: int
    wchan: str          # kernel function sleeping in
    cmdline: str


def _read_proc_status(pid: int) -> dict[str, str]:
    """Parse /proc/<pid>/status into a dict."""
    try:
        with open(f"/proc/{pid}/status") as f:
            return dict(
                line.split(":\t", 1)
                for line in f.read().splitlines()
                if ":\t" in line
            )
    except (FileNotFoundError, PermissionError):
        return {}


def _read_cmdline(pid: int) -> str:
    """Read the full command line for a process."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().replace(b"\x00", b" ").decode(errors="replace").strip()
    except (FileNotFoundError, PermissionError):
        return ""


def _count_fds(pid: int) -> int:
    """Count open file descriptors."""
    try:
        return len(os.listdir(f"/proc/{pid}/fd"))
    except (FileNotFoundError, PermissionError):
        return -1


def _get_fd_limit(pid: int) -> int:
    """Get the max open files limit for a process."""
    try:
        with open(f"/proc/{pid}/limits") as f:
            for line in f:
                if "Max open files" in line:
                    parts = line.split()
                    soft = parts[3]
                    return int(soft) if soft != "unlimited" else 999999999
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return -1


def _get_wchan(pid: int) -> str:
    """Get the kernel function the process is sleeping in."""
    try:
        return Path(f"/proc/{pid}/wchan").read_text().strip()
    except (FileNotFoundError, PermissionError):
        return ""


def _uid_to_username(uid: int) -> str:
    import pwd
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def get_process_info(pid: int) -> Optional[ProcessInfo]:
    """Get comprehensive process information."""
    status = _read_proc_status(pid)
    if not status:
        return None
    
    # Parse key fields
    name = status.get("Name", "unknown")
    state_line = status.get("State", "? (unknown)")
    state = state_line[0]   # first character: R, S, D, Z, T
    ppid = int(status.get("PPid", 0))
    uid = int(status.get("Uid", "0").split()[0])   # real UID
    
    # Memory (in kB from /proc/status)
    rss_kb = int(status.get("VmRSS", "0 kB").split()[0])
    vsz_kb = int(status.get("VmSize", "0 kB").split()[0])
    threads = int(status.get("Threads", 1))
    
    return ProcessInfo(
        pid=pid,
        name=name,
        state=state,
        ppid=ppid,
        uid=uid,
        user=_uid_to_username(uid),
        cpu_pct=0.0,    # requires delta calculation; left as 0 here
        rss_mb=rss_kb / 1024,
        vsz_mb=vsz_kb / 1024,
        threads=threads,
        open_fds=_count_fds(pid),
        fd_limit=_get_fd_limit(pid),
        wchan=_get_wchan(pid),
        cmdline=_read_cmdline(pid),
    )


def find_by_name(pattern: str, exact: bool = False) -> list[ProcessInfo]:
    """Find processes whose name or cmdline matches the pattern."""
    results = []
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        info = get_process_info(pid)
        if info is None:
            continue
        target = info.name if exact else info.cmdline
        if pattern.lower() in target.lower():
            results.append(info)
    return sorted(results, key=lambda p: p.pid)


def detect_stuck_processes() -> dict[str, list[ProcessInfo]]:
    """
    Find processes in problematic states:
      - D state: uninterruptible sleep (potentially stuck in I/O)
      - Z state: zombie (parent not reaping)
    """
    stuck: dict[str, list[ProcessInfo]] = {"D": [], "Z": []}
    
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        info = get_process_info(pid)
        if info and info.state in stuck:
            stuck[info.state].append(info)
    
    return stuck


def monitor_fd_usage(pid: int, threshold_pct: float = 80.0) -> dict:
    """
    Check if a process is approaching its file descriptor limit.
    Returns a warning dict if usage exceeds threshold_pct.
    """
    info = get_process_info(pid)
    if not info:
        return {"pid": pid, "error": "Process not found"}
    
    if info.fd_limit <= 0:
        return {"pid": pid, "error": "Cannot read fd limit"}
    
    usage_pct = (info.open_fds / info.fd_limit) * 100
    
    return {
        "pid": pid,
        "name": info.name,
        "open_fds": info.open_fds,
        "fd_limit": info.fd_limit,
        "usage_pct": usage_pct,
        "alert": usage_pct >= threshold_pct,
        "message": (
            f"WARNING: {info.name}({pid}) has {info.open_fds}/{info.fd_limit} "
            f"fds open ({usage_pct:.1f}%)"
            if usage_pct >= threshold_pct else "OK"
        )
    }


def top_processes(n: int = 10, sort_by: str = "rss") -> list[ProcessInfo]:
    """
    Get top N processes sorted by rss or vsz.
    sort_by: "rss" or "vsz"
    """
    procs = []
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        info = get_process_info(pid)
        if info:
            procs.append(info)
    
    key_fn = {"rss": lambda p: p.rss_mb, "vsz": lambda p: p.vsz_mb}.get(sort_by, lambda p: p.rss_mb)
    return sorted(procs, key=key_fn, reverse=True)[:n]


# ── Demo ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Process Health Monitor ===\n")
    
    # 1. Check for stuck processes
    print("1. Stuck processes:")
    stuck = detect_stuck_processes()
    if stuck["D"]:
        print(f"   D-state (uninterruptible sleep):")
        for p in stuck["D"]:
            print(f"   PID {p.pid} ({p.name}): wchan={p.wchan}")
    else:
        print("   No D-state processes (good)")
    
    if stuck["Z"]:
        print(f"   Zombies: {[p.pid for p in stuck['Z']]}")
    else:
        print("   No zombie processes (good)")
    
    # 2. Top processes by memory
    print("\n2. Top 5 processes by RSS:")
    top = top_processes(n=5, sort_by="rss")
    print(f"   {'PID':>7}  {'RSS':>8}  {'Threads':>8}  {'FDs':>6}  {'Name'}")
    for p in top:
        print(f"   {p.pid:>7}  {p.rss_mb:>6.0f}MB  {p.threads:>8}  {p.open_fds:>6}  {p.name}")
    
    # 3. Find Java processes and check their fd usage
    print("\n3. Java process fd usage:")
    java_procs = find_by_name("java", exact=True)
    if java_procs:
        for p in java_procs[:3]:
            result = monitor_fd_usage(p.pid)
            status = "⚠️ ALERT" if result.get("alert") else "✓ OK"
            print(f"   [{status}] {result['message']}")
    else:
        print("   No Java processes found")
    
    # 4. Current process info
    print(f"\n4. This Python process:")
    me = get_process_info(os.getpid())
    if me:
        print(f"   PID={me.pid} State={me.state} RSS={me.rss_mb:.1f}MB "
              f"FDs={me.open_fds}/{me.fd_limit} wchan={me.wchan}")
```

---

## 16. Hands-On Labs

### Lab 1: Read the Process State of a Running Program

```bash
# Lab 1: observe all process states by creating them deliberately

# State R: create a CPU-burning process
python3 -c "while True: pass" &
CPU_PID=$!
ps -p $CPU_PID -o pid,stat,comm
# stat should show 'R' (running)
kill $CPU_PID

# State S: create a process waiting for input
sleep 1000 &
SLEEP_PID=$!
ps -p $SLEEP_PID -o pid,stat,comm,wchan
# stat: 'S', wchan: hrtimer_nanosleep
kill $SLEEP_PID

# State Z: create a zombie by fork()ing without waiting
python3 -c "
import os, time
pid = os.fork()
if pid == 0:
    os._exit(0)   # child exits immediately
# parent never calls waitpid() → child becomes zombie
print(f'Child PID: {pid}')
time.sleep(30)   # parent stays alive for 30 seconds
" &
PARENT=$!
sleep 1
ps -eo pid,ppid,stat,comm | grep Z
# should show the zombie child
kill $PARENT    # parent exits → init adopts and reaps the zombie

# Read wchan for the sleep process
sleep 100 &
SLEEP_PID=$!
cat /proc/$SLEEP_PID/wchan
# hrtimer_nanosleep
kill $SLEEP_PID
```

### Lab 2: Find the Hot Thread in a JVM

```bash
# Lab 2: identify the hottest thread in a Java process
# Requires: Java installed

# Start a CPU-burning Java program
cat > /tmp/CpuBurner.java << 'EOF'
public class CpuBurner {
    public static void main(String[] args) throws InterruptedException {
        Thread burner = new Thread(() -> { while(true) Math.sqrt(Math.random()); }, "cpu-burner");
        burner.start();
        Thread.sleep(60000);
    }
}
EOF
javac /tmp/CpuBurner.java -d /tmp
java -cp /tmp CpuBurner &
JAVA_PID=$!

sleep 2

# Step 1: find the hot thread
echo "--- Hot threads (top -H) ---"
top -b -n 2 -H -p $JAVA_PID | awk 'NR>7 && /java/ {print $1, $9, "% CPU"}'

# Step 2: get the hot thread's PID and convert to hex
HOT_TID=$(top -b -n 2 -H -p $JAVA_PID | awk 'NR>7' | sort -k9 -rn | head -1 | awk '{print $1}')
echo ""
echo "Hot thread TID: $HOT_TID"
echo "Hex: $(printf '0x%x' $HOT_TID)"

# Step 3: look up in jstack
echo ""
echo "--- jstack for hot thread ---"
jstack $JAVA_PID 2>/dev/null | grep -A 10 "$(printf 'nid=0x%x' $HOT_TID)"

kill $JAVA_PID
rm /tmp/CpuBurner.java /tmp/CpuBurner.class
```

---

## 17. Interview Q&A

**Q1: What does the D state mean in `ps` output, and why can't you kill a process in D state with `kill -9`?**

The `D` state (uninterruptible sleep) means the process is blocked inside a kernel system call that must complete before the kernel can return control to user space. Common causes are waiting for a disk I/O to complete, waiting for a network file system operation (NFS, CIFS), or waiting to acquire a kernel-level lock. The process is inside the kernel — it hasn't returned to user space yet.

SIGKILL works by marking the signal as pending in the process's task_struct and setting a flag for signal delivery. Signal delivery happens at a specific point: when the process is about to return from kernel mode to user mode. A process in `D` state is currently inside the kernel and has not returned to user space — so the signal delivery point has not been reached. SIGKILL is queued but cannot be delivered until the kernel operation completes, which may never happen if the underlying resource (NFS server, disk) is permanently unavailable. The only resolution is to fix the underlying condition (restore the NFS server, replace the disk) or reboot the machine. This is why network filesystems like NFS are strongly discouraged for Kafka log directories and Spark shuffle storage.

**Q2: Explain the three load average numbers. When is a server "overloaded"?**

The three numbers in `uptime` or `top` (`load average: 2.45, 2.12, 1.98`) represent the average number of processes that are either running (on CPU) or runnable (in the run queue, waiting for CPU) over 1-minute, 5-minute, and 15-minute windows. The calculation uses an exponentially-weighted moving average — recent activity has more weight.

Whether the server is overloaded depends on the number of CPU cores. Load average represents the total demand on the CPU subsystem. On a 4-core machine, load 4.0 means all cores are exactly saturated — 4 processes competing for 4 CPUs, with no waiting. Load 8.0 on a 4-core machine means on average 4 processes are running and 4 more are waiting — the server is 2× overloaded for CPU. On a 32-core machine, load 32.0 is perfectly fine. The rule of thumb: load average / number of CPU cores gives a "utilization ratio." Above 1.0 means CPU-saturated; above 2.0 means significant queuing; above 4.0 is serious pressure.

Note that load average counts both CPU-bound and I/O-bound processes: a process blocked in `D` state (waiting for disk I/O) is counted. On storage-heavy workloads, high load average may be driven by I/O waits rather than CPU contention — check `%wa` in `top`'s CPU line to distinguish.

**Q3: What does `nice` do, and when would you use it for a data engineering workload?**

`nice` sets the priority hint for the Linux CFS scheduler. Values range from -20 (highest priority, gets more CPU time) to +19 (lowest priority, gets less CPU time when other processes need CPU). The default is 0. The OS does not honor nice values by reserving CPU — it interprets them as relative weights when processes compete for CPU cycles on a contested CPU. If a machine has spare CPU, even a nice=19 process runs at full speed.

In data engineering, nice values are useful when a server hosts both latency-sensitive and batch workloads. A Kafka broker at nice=0 competing with a batch Spark ETL job at nice=0 will both get 50% CPU, causing Kafka's network thread to be slow and consumer lag to grow. Setting the batch job to nice=10 or nice=15 gives the Kafka broker priority when the CPU is contested: the CFS scheduler allocates roughly proportionally to the weight difference (adjacent nice values differ by ~1.25× in weight; nice=15 vs nice=0 is about a 10× weight difference). The batch job still runs at full speed when Kafka is idle — only the contention case is improved. For permanent configuration, set `Nice=10` in the service's systemd unit file rather than calling `renice` manually.

**Q4: What is systemd's `Restart=on-failure` and how does it interact with a Kafka broker?**

`Restart=on-failure` instructs systemd to automatically restart the service whenever the main process exits with a non-zero exit code, is killed by a signal (other than SIGTERM/SIGHUP in certain configurations), or hits a timeout. It does not restart if the process exits with code 0 (clean exit) or if `systemctl stop` is used (which is handled separately as an intentional stop).

For Kafka, `Restart=on-failure` is appropriate because the broker should be resilient to crashes: if the JVM exits due to an OOM error or an uncaught exception, systemd restarts it automatically. However, there are nuances. Kafka's graceful shutdown exits with code 0 after SIGTERM — `Restart=on-failure` correctly does not restart it. But if Kafka exits with SIGTERM (exit code 143 = 128 + 15) due to Kubernetes sending the signal, the exit code 143 is non-zero — systemd would attempt a restart. The fix is `SuccessExitStatus=143` in the unit file, which tells systemd to treat exit code 143 as a clean stop, not a failure. Additionally, `StartLimitIntervalSec=5min` and `StartLimitBurst=3` prevent restart loops: if Kafka crashes 3 times within 5 minutes, systemd stops trying to restart it, preventing an infinite loop that could destabilize the server.

**Q5: How would you diagnose a Spark executor that appears to be running but making no progress?**

Start by confirming the process is actually alive and in what state: `ps -p <pid> -o pid,stat,wchan,pcpu,rss`. The `stat` column tells the first story. If it's `D`, the executor is stuck in a kernel I/O operation — check `cat /proc/<pid>/wchan` for the specific kernel function. If it's `S` (sleeping) with zero CPU usage, it's blocked waiting for something but not doing work. If it's `R` with 100%+ CPU but no Spark UI progress, it might be in a GC loop.

For a sleeping executor, look at what it's waiting for. `cat /proc/<pid>/wchan` might show `futex` (waiting on a Java lock/condition variable — possibly a deadlock) or `ep_poll` (waiting in epoll — possibly waiting for data from the driver that never comes). Request a thread dump: `jstack <pid> > /tmp/threads.txt` and scan for `BLOCKED` threads and `waiting on condition` — if multiple threads are blocked on the same lock, that's a deadlock. Check the Spark UI's Stage and Task tabs to see if the task has been running without progress metrics updating.

For a CPU-pegged executor, check if GC is the culprit: `jstat -gcutil <pid> 1000 10` shows GC frequency and heap usage. If `FGC` (full GC count) is increasing rapidly and `EU`/`OU` (eden/old utilization) are near 100%, the executor is in GC thrash — it's consuming CPU spinning in garbage collection rather than doing useful work. The fix is to increase heap (`spark.executor.memory`) or tune GC (switch to G1GC with `-XX:+UseG1GC`).

**Q6: You notice that `df -h` shows 10% disk usage on a server but you're getting "no space left on device" errors. What are the two possible explanations?**

The first explanation is inode exhaustion. `df -h` reports block usage but not inode usage. Run `df -i` to check inode utilization. If any filesystem shows `IUse% = 100%`, all inodes on that filesystem are allocated and no new files can be created — even though physical disk blocks are available. This is common in Hive warehouses with millions of small Parquet files, or in Python package caches with millions of tiny source files. Each file, regardless of size, consumes one inode. The fix is to consolidate small files and possibly recreate the filesystem with a higher inode-to-block ratio.

The second explanation is that the `df -h` output is for the wrong filesystem — the process is writing to a different mount point that is actually full. For example, `/tmp` might be 100% full (a tmpfs with a 2 GB limit) while `/data` is only 10% full. The error `ENOSPC: No space left on device` includes the device number — check `stat` on the path being written to and compare the device number against `df --output=source,target,pcent` to identify which mounted filesystem is full.

---

## 18. Cross-Question Chain

**Q1 [Interviewer]: Your Kafka broker is consuming 200% CPU. How do you start diagnosing?**

First, confirm this is a single process and not an accident of reporting: `ps aux | grep kafka` shows `%CPU = 200.0` which on a multi-core machine means 2 full cores are being used. This could be normal or abnormal depending on the server's core count and Kafka's expected load. Run `top` to see per-core utilization (press `1` in top): if 2 of 16 cores are at 100% and others are idle, 200% CPU is expected I/O thread activity. If 14 of 16 cores are at 100% for a broker that normally uses 4, something is wrong.

**Q2 [Interviewer]: top shows 2 cores at 100%. You suspect one Kafka thread is consuming abnormally. How do you identify it?**

Run `top -H -p <kafka_pid>` to show individual threads instead of the aggregate. This reveals which thread(s) are consuming CPU. In a healthy Kafka broker, CPU is spread across network threads, I/O threads, and log cleaner threads. An abnormal pattern would be one thread at 99% while others are near 0%. Note the problematic thread's PID (which is its Linux thread ID), then convert it to hex: `printf '0x%x\n' <tid>`. Run `jstack <kafka_pid>` and `grep -A 15 "nid=<hex>"` to find the Java thread's stack trace — which tells you exactly what code that thread is executing.

**Q3 [Interviewer]: jstack shows the hot thread is in a log compaction loop that won't stop. You want to pause the compactor without restarting Kafka. Can you?**

Yes, using `SIGSTOP` and `SIGCONT` on specific threads, or by using Kafka's admin API. Kafka exposes `kafka-configs.sh --alter --add-config log.cleaner.enable=false` for the broker dynamically in newer versions. But for a brute-force OS approach: identify the compactor thread's Linux TID from `jstack`, then `kill -STOP <tid>` to pause just that thread (SIGSTOP on a thread TID affects only that thread, not the whole process). The broker continues running, the compactor is paused. Later `kill -CONT <tid>` resumes it. This is a temporary measure — not a production solution — but useful to stop active damage while you diagnose. The better approach is to use Kafka's built-in controls: reduce `log.cleaner.threads` to 0 and restart.

**Q4 [Interviewer]: You restart Kafka with `systemctl restart kafka` and it immediately fails. How do you debug the failure?**

First, `systemctl status kafka` — read the status output and the last few log lines. The `code=exited, status=1` line tells me the exit code. The log lines may give the error message directly. If that's insufficient, `journalctl -u kafka -n 100` gives the full startup log. If still unclear, try running the exact command from the `ExecStart=` line manually as the service user: `sudo -u kafka /opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties`. This reproduces the startup in an interactive terminal where all output is immediately visible, catching issues like missing environment variables, wrong file paths, or port conflicts (`address already in use`).

**Q5 [Interviewer]: The manual start works fine but systemctl fails. What could cause the difference?**

The most common differences between a manual run and a systemd service: environment variables — the service has a clean environment without `~/.bashrc` or `~/.profile` loaded; `JAVA_HOME`, `KAFKA_HEAP_OPTS`, or `PATH` may not be set in the service's environment. Check with `systemctl show kafka --property=Environment`. Working directory — the service's `WorkingDirectory=` may differ from where you run manually. Resource limits — systemd applies ulimits from the unit file (`LimitNOFILE`, `LimitNPROC`) that may differ from your shell. File permissions — the service runs as `User=kafka`; your manual test may run as root or your personal account with broader permissions. Security policies — `NoNewPrivileges=yes` or `PrivateTmp=yes` in the unit file restrict what the service process can do that your manual test can do. Use `systemd-run --unit=kafka-test --uid=kafka /opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties` to test in a systemd-like environment.

**Q6 [Interviewer]: It turns out the issue is that `JAVA_HOME` is not set in the service environment. Walk me through the fix.**

Add the environment variable to the unit file without editing the original package-installed file — use a drop-in override to keep changes maintainable and upgrade-safe. Run `systemctl edit kafka`, which opens `/etc/systemd/system/kafka.service.d/override.conf` in an editor. Add: `[Service]` then `Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`. Save and exit. Run `systemctl daemon-reload` to pick up the new override — this is required after any unit file change. Then `systemctl start kafka`. Verify with `systemctl show kafka --property=Environment` which should now include `JAVA_HOME=...`. Finally, `systemctl status kafka` should show `active (running)`. The override.conf file is preserved across package upgrades, so future `apt upgrade` or `yum update` won't lose the customization.

---

## 19. Common Misconceptions

**"`kill -9` always works."**
SIGKILL cannot be caught or ignored in user space, but it can be delayed by the kernel. A process in `D` state (uninterruptible sleep) does not receive SIGKILL until it exits the kernel function it's blocked in. If that kernel function is waiting for an NFS server that is permanently unreachable, the process may never return from `D` state — and `kill -9` appears to do nothing. In this case, the only options are to fix the underlying resource, force-unmount the filesystem, or reboot the server.

**"High load average always means the server is in trouble."**
Load average measures runnable + running processes, which includes both CPU-bound and I/O-bound work. A server doing heavy parallel disk I/O may have load average of 20 on a 4-core machine — 16 processes in D state waiting for I/O and 4 running on CPU. The server may be performing exactly as expected. Compare load average to CPU count AND check `%wa` (I/O wait) in top. A 4-core server with load 8 and 50% I/O wait has 4 processes on CPU and 4 waiting for disk — it may be saturating the storage, not the CPU.

**"A zombie process is dangerous because it consumes memory."**
A zombie process is dead — its memory, file descriptors, and CPU resources have been freed. What remains is a tiny PCB entry in the kernel's process table. Individual zombies are harmless. The danger is accumulation: if thousands of zombies accumulate, the process table fills, and `fork()` fails system-wide — no new processes can be created. This is a denial-of-service risk for a process factory like Airflow that spawns many children.

**"systemd `Restart=always` is always better than `Restart=on-failure`."**
`Restart=always` restarts the service even when it exits cleanly (exit code 0). For a Kafka broker that is intentionally stopped with `systemctl stop kafka` (which sends SIGTERM, broker exits with code 0), `Restart=always` would immediately restart it — preventing clean maintenance windows. `Restart=on-failure` only restarts on non-zero exits or signal kills, respecting intentional stops. `Restart=always` is appropriate only for services that should never stop (like a container runtime) or when you specifically want no-op restarts on clean exits.

**"The `%CPU` column in `ps aux` shows current CPU usage."**
`ps aux`'s `%CPU` column is the average CPU utilization since the process was started — a cumulative average, not a real-time reading. A Kafka broker that used 50% CPU for its entire 8-hour runtime shows `%CPU = 50.0` even if it's currently at 0.1%. `top`'s `%CPU` is a recent average (over the last refresh interval, typically 3 seconds) which is much more useful for identifying currently hot processes.

---

## 20. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What are the 5 main Linux process states and their letters? | R=Running, S=Sleeping (interruptible), D=Uninterruptible sleep, Z=Zombie, T=Stopped. Modifiers: l=multithreaded, +=foreground, N=low priority, <=high priority. |
| 2 | Why can't SIGKILL kill a D-state process? | D state = process is inside the kernel waiting for a hardware operation. SIGKILL is delivered when returning to user space. A process stuck in D never returns — so SIGKILL is queued but not delivered. Fix the underlying I/O or reboot. |
| 3 | What does load average represent? | Average number of processes either running or runnable (in the run queue) over 1/5/15 minute windows. Load equal to CPU count = fully saturated. Load > CPU count = overloaded (processes waiting for CPU). |
| 4 | What column in `ps aux` shows process state? | STAT column. E.g., `Sl` = Sleeping + multithreaded. `D` = uninterruptible sleep. `Z` = zombie. `Ss` = sleeping session leader. |
| 5 | What is the difference between `ps aux` %CPU and top %CPU? | `ps aux` %CPU = average since process start (historical). top %CPU = average over the last refresh interval (~3 seconds) — much more useful for finding currently hot processes. |
| 6 | How do you find the hottest thread in a JVM? | `top -H -p <pid>` shows individual threads. Note the hot thread's Linux TID, convert to hex: `printf '0x%x' <tid>`. Run `jstack <pid>` and grep for `nid=0x<hex>`. |
| 7 | What does `cat /proc/<pid>/wchan` tell you? | The kernel function the process is sleeping in. `ep_poll` = waiting in epoll (normal for event-loop services). `futex` = waiting on a lock. `nfs4_wait_on_state` = stuck on NFS (problem). `io_schedule` = waiting for I/O. |
| 8 | What is a zombie process? | A process that has exited but whose PCB remains in the process table because its parent has not called wait(). Holds no resources except one process table slot. Fix: parent calls waitpid(), or set SIGCHLD=SIG_IGN. |
| 9 | What is the nice value range and what does -20 mean? | -20 to +19. -20 = highest priority (most CPU when contested). +19 = lowest priority (least CPU). Default = 0. Requires root for negative values. Adjacent values differ by ~1.25× in CFS weight. |
| 10 | What command changes the priority of a running process? | `renice -n <value> -p <pid>`. E.g., `renice -n 10 -p 1234` sets PID 1234 to nice=10. To set in systemd: add `Nice=10` to the `[Service]` section of the unit file. |
| 11 | What does `systemctl daemon-reload` do? | Tells systemd to re-read all unit files from disk. REQUIRED after creating or modifying any unit file. Does not restart services — only updates systemd's in-memory configuration. |
| 12 | What is `Restart=on-failure` vs `Restart=always`? | on-failure: restart only on non-zero exit or signal kill (not on clean exit). always: restart on any exit including code 0. Use on-failure for services that should be stoppable with systemctl stop. |
| 13 | What does `KillMode=control-group` do in a systemd unit? | On service stop, kill ALL processes in the service's cgroup — not just the main PID. Prevents orphaned child processes from continuing after the main service exits. |
| 14 | How do you view the last 50 log lines for a systemd service? | `journalctl -u <service> -n 50`. Follow with -f flag: `journalctl -u <service> -f`. Since boot: `journalctl -u <service> -b`. Last hour: `journalctl -u <service> --since "1 hour ago"`. |
| 15 | What does `LimitNOFILE=65536` in a systemd unit do? | Sets the maximum open file descriptors for the service to 65536. Equivalent to `ulimit -n 65536` but persistent and applied regardless of system default. Critical for Spark shuffle and Kafka with many topics/partitions. |
| 16 | How do you find which process has a specific file open? | `lsof <filepath>` or `fuser <filepath>`. For ports: `lsof -i :<port>` or `ss -tlnp | grep <port>`. |
| 17 | What does `nohup` do? | Prevents SIGHUP from terminating the process when the terminal session ends. Output goes to `nohup.out` unless redirected. Use `nohup command > log.txt 2>&1 &` for background persistence. |
| 18 | How do you find zombie processes and their parents? | `ps -eo pid,ppid,stat,comm \| awk '$3=="Z" {print}'`. The PPID column shows the parent; send SIGCHLD to the parent to trigger cleanup: `kill -CHLD <ppid>`. |
| 19 | What does `oom_score_adj` do? | Adjusts the OOM killer's score for a process. Range -1000 to 1000. -1000 = never kill (protect from OOM). +1000 = kill first. Set via `echo -900 > /proc/<pid>/oom_score_adj`. Used to protect Kafka/Spark from OOM kills in memory-constrained environments. |
| 20 | What does `pgrep -f` do vs `pgrep` with no flag? | Without -f: matches against the process name (first 15 chars of comm). With -f: matches against the full command line including arguments. Use -f to find processes like `spark-submit` whose name is just `java` but the args contain `spark-submit`. |

---

## 21. Module Summary

Linux process management is the first diagnostic layer for any production data engineering problem. Every Spark job, Kafka broker, and Airflow task is a Linux process; understanding what state it's in and why is the prerequisite to fixing anything.

**Process states** determine what the process is doing and what you can do to it. `S` (interruptible sleep) is normal for event-driven services like Kafka waiting on epoll. `D` (uninterruptible sleep) signals a kernel-level block — typically I/O — that SIGKILL cannot interrupt. `Z` (zombie) means a dead child whose parent has not reaped it. The `wchan` column (`/proc/<pid>/wchan`) names the kernel function responsible for the block, turning a cryptic state into a specific diagnosis.

**`ps`** gives accurate snapshots with rich filtering. `ps aux` for a quick list; `ps -eo pid,ppid,stat,wchan,comm` for state-focused debugging; `ps -eLf` for thread-level visibility. `pgrep -f` finds processes by full command line — essential when the binary name is `java` for both Kafka and Spark. **`top`** gives live CPU and memory rankings; `top -H -p <pid>` reveals which individual JVM thread is hot, which you correlate to a Java stack trace with `jstack`.

**`kill`** sends signals — always try SIGTERM first (allows graceful shutdown with JVM hooks, offset commits, and connection cleanup), then SIGKILL after a timeout. **`nice`** and **`renice`** adjust CPU priority, which matters when latency-sensitive (Kafka) and batch (Spark ETL) workloads compete for CPU on shared hardware.

**systemd** is the production service manager: `systemctl start/stop/status`, `journalctl -u <service>` for logs, and unit files that encode the correct resource limits (`LimitNOFILE=65536`), restart policy (`Restart=on-failure`), kill behavior (`KillMode=control-group`), and timeout (`TimeoutStopSec=60`). Drop-in overrides via `systemctl edit` keep customizations upgrade-safe.

**`/proc`** is the kernel's live dashboard: process state, open file descriptors, resource limits, OOM scores, and the wchan sleeping function — all readable as text files without any special tools.

---

**SYS-LNX-101: 2 of 5 complete.**  
**Next: SYS-LNX-101 M03 — Storage and I/O**  
*(iostat, iotop, lsblk, mount, XFS vs ext4 — diagnosing and understanding storage for Kafka/Spark)*
