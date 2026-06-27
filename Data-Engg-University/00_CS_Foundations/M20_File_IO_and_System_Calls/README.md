# CSF-OS-101 M04: File I/O and System Calls

**Course:** CSF-OS-101 — Operating System Internals for Data Engineers  
**Module:** 04 of 05  
**Filesystem position:** 00_CS_Foundations/M20_File_IO_and_System_Calls  
**Prerequisites:** CSF-OS-101 M01 (Processes and Threads), M03 (Virtual Memory and Page Cache)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: System Calls](#4-core-theory-system-calls)
5. [The Virtual File System (VFS)](#5-the-virtual-file-system-vfs)
6. [File I/O System Calls: open, read, write, fsync, close](#6-file-io-system-calls-open-read-write-fsync-close)
7. [Buffered vs Direct I/O (O_DIRECT)](#7-buffered-vs-direct-io-o_direct)
8. [I/O Multiplexing: select, poll, epoll](#8-io-multiplexing-select-poll-epoll)
9. [io_uring: The Modern Asynchronous I/O Interface](#9-io_uring-the-modern-asynchronous-io-interface)
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

A Kafka broker writes to its log segment files. The write returns immediately — `log.flush()` in your producer confirms delivery. Then the machine loses power. The data is gone. Why? Because `write()` in Linux writes to the OS page cache, not to disk. The data was in RAM; the power loss evicted it before the OS had a chance to flush.

A Python ETL job reads 10 GB of CSV files using a loop of `f.read(1024)` calls. It runs in 45 minutes. Switching to `f.read(8 * 1024 * 1024)` makes it run in 3 minutes. Why? Each `read()` call is a system call crossing the kernel boundary. At 1 KB per call, there are 10 million system calls; at 8 MB per call, there are 1250. The overhead of system call transitions dominates at small read sizes.

A web server using `select()` to handle 1000 concurrent socket connections degrades to O(N²) behavior — each call to `select()` scans all 1000 file descriptors. Switching to `epoll` makes it O(1) per event because `epoll` notifies only when something is ready. Kafka's network layer, Spark's shuffle server, and every high-throughput data system use epoll (or io_uring on newer kernels) for exactly this reason.

A Spark shuffle writer opens thousands of output files simultaneously (one per reducer partition). On a machine with a default open file limit (`ulimit -n 1024`), file descriptor exhaustion causes the shuffle to fail partway through with `Too many open files`. The fix requires understanding file descriptors, the system limit, and how to raise it.

All of these are system call and file I/O problems. This module explains what system calls are, how the Linux VFS works, what happens inside each file I/O call, the difference between buffered and direct I/O, how epoll enables high-concurrency I/O, and how io_uring eliminates syscall overhead for modern storage. Everything maps directly to how Kafka, Parquet, and Spark implement their I/O layers.

---

## 2. How to Use This Module

**For production debugging:** Sections 6, 7, 11. Section 6 explains why `write()` is not durable without `fsync()` and what Kafka's `log.flush.interval.messages` actually controls. Section 7 explains Kafka's O_DIRECT log reads and Spark's bypass of the page cache. Section 11 covers file descriptor exhaustion, `fsync` storms, and the `O_SYNC` vs `fsync()` trade-off.

**For interview prep:** Sections 4, 5, 8, 15, 16. System calls, VFS, and epoll are canonical OS interview questions. The escalating chain in Section 16 goes from "what is a system call" to "design Kafka's I/O layer."

**For system design:** Sections 8, 9, 12. The epoll vs io_uring distinction is architectural — knowing which event model a system uses and why determines how it scales. Section 12 maps epoll and io_uring directly to Kafka's network layer, Spark's shuffle server, and Python async I/O.

---

## 3. Prerequisites Check

- **Processes and threads (M01):** System calls are the interface between user-space processes and the kernel. Understanding what a process is — user mode vs kernel mode — is required.
- **Page cache (M03):** Buffered I/O writes to the page cache; `fsync()` flushes the page cache to disk. The interaction between `write()` and the page cache is the critical concept in Section 6. Review M03's page cache lifecycle before Section 7.
- **File descriptors:** You should know that `open()` returns a file descriptor (integer) and that `read()` and `write()` take a file descriptor. This module explains the kernel data structures behind those integers.

---

## 4. Core Theory: System Calls

### 4.1 User Mode vs Kernel Mode

Modern CPUs run at different **privilege levels** (called rings on x86, exception levels on ARM). Linux uses two levels:

**User mode (Ring 3):** Normal process execution. The process can run code, read/write its own memory, and perform arithmetic. It cannot: access I/O devices, modify other processes' memory, change the CPU's privilege level, or directly access hardware registers.

**Kernel mode (Ring 0):** The OS kernel runs here. The kernel has unrestricted access to all hardware, all memory, and all CPU registers. Every device driver, scheduler decision, and memory management operation runs in kernel mode.

**Why the separation?** Without privilege levels, any buggy or malicious program could corrupt the OS kernel, crash other processes, or access hardware directly. The CPU enforces the boundary in hardware — user-mode code that tries to execute a privileged instruction (like reading from a disk controller register) triggers a hardware fault.

### 4.2 What Is a System Call?

A **system call (syscall)** is the controlled crossing from user mode to kernel mode. It is the only way a user-space process can ask the OS to do something privileged — open a file, read network data, allocate memory, create a process.

```
System call mechanism (x86-64 Linux):

User space:
  Python calls open("data.parquet", "r")
  → CPython calls libc's open() wrapper
  → libc puts syscall number in %rax (2 = open / 257 = openat)
  → libc puts arguments in %rdi, %rsi, %rdx, %r10, %r8, %r9
  → libc executes: syscall instruction (single x86 instruction)

Hardware:
  CPU saves current stack pointer, instruction pointer, registers
  CPU switches to kernel mode (privilege level Ring 0)
  CPU jumps to kernel's syscall entry point (LSTAR register)

Kernel space (syscall handler):
  Kernel reads syscall number from %rax
  Kernel dispatches to sys_openat() handler
  sys_openat() does the work: creates file descriptor, checks permissions
  Kernel puts return value (fd number or -errno) in %rax
  Kernel executes: sysret (return from syscall)

Hardware:
  CPU restores to user mode (Ring 3)
  Execution resumes in libc after the syscall instruction

User space:
  libc returns the fd to CPython
  CPython returns the file object to Python user code
```

**Syscall cost:** Approximately 100–400 ns per syscall on modern hardware (measured with `getpid()`, the cheapest syscall). This includes: register saving, privilege switch, kernel dispatch, register restore, privilege switch back. Historically, x86 used the `int 0x80` instruction (~100 cycles); `syscall`/`sysret` instructions are faster (~100 ns); VDSO (virtual Dynamic Shared Object) maps some syscalls (like `clock_gettime`) into user space to eliminate the mode switch entirely.

**Spectre/Meltdown mitigations (KPTI):** Since 2018, Linux kernels include KPTI (Kernel Page Table Isolation) to mitigate Spectre/Meltdown CPU vulnerabilities. KPTI separates kernel and user page tables, requiring an additional TLB flush on every syscall. This increased syscall cost by 3–5× on affected systems. `grep cpu_insecure /proc/cpuinfo` reveals if mitigations are active.

### 4.3 Anatomy of a Syscall in Strace

```bash
# Trace all system calls made by a Python file-read program
strace -c python3 -c "open('/etc/passwd').read()"

# Output (summary):
# % time     seconds  usecs/call     calls    syscall
# ------  -----------  -----------  -------  --------
#  38.23     0.000821          102        8  read
#  21.45     0.000460          115        4  openat
#  15.32     0.000329           82        4  close
#  ...
#
# openat: 4 syscalls to open files
# read: 8 read calls (Python reads in chunks)
# Each syscall is ~100-200 µs

# Trace specific file operations for a Spark job
strace -e trace=read,write,fsync,fdatasync -p <PID>
```

### 4.4 The Syscall Table

Linux has ~400 system calls. The most important for data engineering:

| Syscall | Purpose |
|---|---|
| `openat` | Open (or create) a file; returns a file descriptor |
| `read` | Read bytes from a file descriptor into a buffer |
| `write` | Write bytes from a buffer to a file descriptor |
| `pread64` / `pwrite64` | Read/write at a specific offset (thread-safe, no seek) |
| `close` | Release a file descriptor |
| `fsync` | Flush dirty pages for one fd to storage; wait for completion |
| `fdatasync` | Like fsync but skips metadata (faster for append-only logs) |
| `mmap` | Map a file or anonymous memory into virtual address space |
| `munmap` | Unmap a virtual memory region |
| `epoll_create1` | Create an epoll event polling instance |
| `epoll_ctl` | Register/modify/delete file descriptors from an epoll set |
| `epoll_wait` | Wait for events on an epoll set; returns only ready fds |
| `socket` | Create a network socket |
| `bind` / `listen` / `accept` | TCP server setup |
| `sendfile` | Zero-copy file-to-socket transfer (Kafka log reads) |
| `io_uring_setup` | Create an io_uring ring for async I/O (modern kernels) |
| `ioctl` | Device-specific control (used by Kafka's O_DIRECT path) |
| `fallocate` | Pre-allocate file space (Kafka log segment creation) |
| `fstat` | Get file metadata (size, mtime, inode) |
| `getdents64` | Read directory entries (ls, find operations) |

---

## 5. The Virtual File System (VFS)

### 5.1 Why VFS Exists

Linux supports dozens of file systems: ext4, XFS, Btrfs, NTFS, FAT32, tmpfs, procfs, sysfs, NFS, FUSE (user-space file systems like HDFS via Hadoop FUSE, or S3FS). Each has different on-disk formats, different performance characteristics, and different feature sets.

A Python program using `open("file.txt")` should not need to know whether the file is on ext4, XFS, or NFS. The **Virtual File System (VFS)** is a kernel abstraction layer that presents a uniform API (`open`, `read`, `write`, `readdir`) regardless of the underlying file system.

### 5.2 VFS Data Structures

The VFS maintains four key data structures in kernel memory:

**Superblock:** Represents a mounted file system. Contains: file system type, block size, total/free blocks, pointer to the root inode.

**Inode:** Represents one file or directory, independent of its name or path. Contains: file type (regular, directory, symlink, device), permissions, owner, size, timestamps (atime, mtime, ctime), number of hard links, and block pointers (where the file's data blocks are on disk). The inode does NOT contain the filename.

**Dentry (directory entry):** Represents the name component of a path. Maps a filename string to an inode number. The dentry cache (`dcache`) caches recent name→inode lookups — resolving `/var/log/kafka/server.log` requires 5 dentry lookups (/, var, log, kafka, server.log); caching these lookups avoids repeated disk reads for directory metadata.

**File:** One open instance of a file. Contains: pointer to the inode, current file offset (the position for the next read/write), access mode (read/write/append), and flags. Multiple processes can open the same file — each gets its own `file` struct with its own offset, but they share the same inode.

```
Path resolution for open("/var/log/kafka/server.log"):

VFS path walk:
  "/" → lookup in root inode → find "var" entry → dentry cache?
    Cache hit: return inode for /var directly
  "/var" → lookup in /var's inode → find "log" entry → dentry cache?
    Cache miss: read /var's directory block from disk → find "log" → cache it
  "/var/log" → find "kafka" → likely cached (frequently accessed)
  "/var/log/kafka" → find "server.log" → find inode number
  
  Inode lookup: is inode N in the inode cache?
    Cache hit: return in-memory inode struct
    Cache miss: read inode from disk (disk block containing inode N)
  
  Permission check: does the calling process have read access?
    Check inode permissions (rwxrwxrwx) + ACLs + SELinux
  
  Create file struct: offset=0, mode=READ, flags=O_RDONLY
  Allocate file descriptor: smallest unused integer in process's fd table
  Return fd to user
```

### 5.3 File Descriptors

A **file descriptor (fd)** is a small non-negative integer that represents an open file in a process. It is an index into the process's **file descriptor table** — a kernel array mapping fd integers to `file` structs.

```
Process file descriptor table (in kernel, per process):

fd 0: struct file → stdin (keyboard, or pipe from parent)
fd 1: struct file → stdout (terminal, or pipe to parent)
fd 2: struct file → stderr (terminal, or log file)
fd 3: struct file → /var/log/kafka/server.log (offset=0)
fd 4: struct file → /tmp/shuffle.bin (offset=4096)
fd 5: struct file → TCP socket to 192.168.1.1:9092
...
fd 1023: (default max per process: ulimit -n)
```

**File descriptor limits** are one of the most common production issues in data engineering:

```bash
# Check per-process limit
ulimit -n
# 1024 (default on many systems — dangerously low for Spark)

# Check system-wide max open files
cat /proc/sys/fs/file-max
# 9223372036854775807 (practically unlimited on modern Linux)

# Increase per-process limit (current session)
ulimit -n 65536

# Permanent increase via /etc/security/limits.conf:
# spark  soft  nofile  65536
# spark  hard  nofile  65536

# Or via systemd service file:
# [Service]
# LimitNOFILE=65536

# Check current usage
ls /proc/<PID>/fd | wc -l        # count open fds for a process
cat /proc/sys/fs/file-nr          # system-wide: open, free, max
```

**Spark's file descriptor usage:** A Spark executor with 200 shuffle partitions reading from 32 map tasks needs to open 200 × (1 shuffle file per map + 1 index file per map) = 200 × 64 = 12,800 file descriptors simultaneously. The default limit of 1024 causes `Too many open files (EMFILE)` errors. Standard Spark deployment requires `ulimit -n 65536` or higher.

---

## 6. File I/O System Calls: open, read, write, fsync, close

### 6.1 `open()` / `openat()`

```c
int fd = openat(AT_FDCWD, "path/to/file", O_RDONLY | O_CLOEXEC);
```

**Key flags:**
- `O_RDONLY`, `O_WRONLY`, `O_RDWR`: access mode
- `O_CREAT`: create if not exists (requires `mode` argument for permissions)
- `O_TRUNC`: truncate existing file to zero length on open
- `O_APPEND`: all writes go to end of file (atomic with concurrent writers)
- `O_CLOEXEC`: close fd automatically on exec() (prevents fd leak into child processes)
- `O_NONBLOCK`: file/socket operations return immediately instead of blocking
- `O_DIRECT`: bypass page cache (covered in Section 7)
- `O_SYNC`: every write is immediately flushed to hardware (synchronous writes)
- `O_DSYNC`: like O_SYNC but only data, not metadata

**`O_APPEND` atomicity:** On Linux, `write()` to an `O_APPEND` file descriptor is atomic with respect to other `O_APPEND` writers to the same file — the OS holds a file lock during the seek-to-end + write sequence. Kafka's log segment writes use O_APPEND to guarantee that multiple producer threads cannot interleave partial writes.

### 6.2 `read()` and `pread64()`

```c
ssize_t bytes_read = read(fd, buffer, count);
ssize_t bytes_read = pread64(fd, buffer, count, offset);  // thread-safe
```

`read()` reads up to `count` bytes from the file at the current offset into `buffer`, advances the offset by the number of bytes read, and returns the number of bytes actually read. It may return fewer than `count` bytes (a "short read") even on a regular file when:
- The read reaches the end of file
- A signal interrupts the syscall (`EINTR`)
- The read crosses a buffer boundary in the kernel

**Short read handling is mandatory:** A correct `read()` loop always checks the return value and retries:

```python
import os

def read_exact(fd: int, size: int) -> bytes:
    """Read exactly `size` bytes, handling short reads."""
    buf = bytearray(size)
    view = memoryview(buf)
    pos = 0
    while pos < size:
        n = os.read(fd, size - pos)
        if not n:
            raise EOFError(f"Unexpected EOF after {pos} bytes (expected {size})")
        view[pos:pos + len(n)] = n
        pos += len(n)
    return bytes(buf)
```

`pread64()` is the threadsafe version: it reads at a specified offset without changing the file's shared offset. Multiple threads can call `pread64()` simultaneously on the same fd with different offsets — no locking required. This is why Parquet readers use `pread()` (or `mmap`) for column chunk access: multiple threads can read different column chunks from the same file simultaneously.

### 6.3 `write()` and Buffered Writes

```c
ssize_t bytes_written = write(fd, buffer, count);
```

`write()` copies `count` bytes from `buffer` into the OS page cache. It returns when the data is in the page cache — **not when the data is on disk**. The page cache page is marked dirty; the OS will write it to disk asynchronously (typically within 30 seconds, controlled by `vm.dirty_expire_centisecs`).

```
write() call path:
  User buffer → [copy] → Page cache (in RAM)  ← write() returns here
  Page cache → [async writeback by pdflush] → Disk
  
  Gap between write() return and disk persistence: up to 30 seconds by default
  Power failure in this window → data loss
```

**Write buffering across layers:**

```
Python user code:
  f.write(data)  →  Python file object buffer (8 KB default)
                    ↓ when buffer full or f.flush() called
  write() syscall →  OS page cache (kernel buffer)
                    ↓ when pdflush runs (every 30s) or fsync() called
  Storage device   →  Device write cache (persistent NVRAM or spinning buffer)
                    ↓ when device flushes its cache (barrier or FUA command)
  Storage medium   →  Magnetic platter or NAND flash (truly durable)
```

For durable writes, all three buffer layers must be explicitly flushed:
1. `f.flush()` (or `io.IOBase.flush()`) flushes Python's buffer → OS page cache
2. `os.fsync(fd)` flushes OS page cache → storage device (and waits for device acknowledgment)
3. The device's write cache is flushed by the `fsync()` → `fsync()` issues a storage flush command that forces the device to write its cache to the medium

### 6.4 `fsync()` and `fdatasync()`

```c
int ret = fsync(fd);       // flush data + metadata to storage
int ret = fdatasync(fd);   // flush data only (skip metadata update)
```

`fsync(fd)` does two things: writes all dirty page cache pages for this file to the storage device, and issues a storage flush command (HDDs: rotate platters to write; NVMe: flush write buffer to NAND). It **blocks** until the storage device confirms that the data is durable. For HDDs, this takes ~5–15 ms per fsync. For NVMe SSDs, ~50–200 µs per fsync.

`fdatasync(fd)` is faster: it flushes only the file data, not metadata (file size, mtime). For append-only log files where the size changes on every write, `fdatasync()` still must flush the file size (a structural metadata change). For in-place updates to fixed-size data files, `fdatasync()` is meaningfully faster.

**Kafka and fsync:**

```
Kafka's durability hierarchy (producer → broker):

Producer:
  acks=0: producer doesn't wait for any acknowledgment
  acks=1: broker writes to its page cache, sends ack → NOT durable
  acks=all: broker waits for all in-sync replicas to write to page cache

Broker persistence (log.flush.interval.messages):
  Default: log.flush.interval.messages=9223372036854775807 (effectively: never flush)
  Kafka does NOT call fsync on every write by default.
  Durability relies on replication: if 3 brokers have the data in their page caches,
  losing 1 broker (power failure) means 2 still have it.
  
  Explicit fsync: log.flush.interval.messages=1 → fsync after every message
    → throughput drops from 1M msg/s to ~50K msg/s (20× slower on NVMe)
    → 100× slower on HDD (fsync takes ~10 ms → max 100 fsyncs/s)
  
  Common production setting: log.flush.interval.messages=10000
    → fsync every 10,000 messages (balance: throughput vs durability window)
```

### 6.5 `close()`

```c
int ret = close(fd);
```

`close()` releases the file descriptor — the fd integer becomes available for reuse. It does NOT guarantee that the data is on disk (that requires `fsync()` before `close()`). It does NOT immediately evict the page cache pages. `close()` on the last open reference to an O_TMPFILE (temporary file without a directory entry) causes the file's data to be immediately freed.

**Resource leak pattern to avoid:**

```python
# WRONG: fd may leak if exception occurs before close()
fd = os.open("file.parquet", os.O_RDONLY)
data = os.read(fd, 4096)
# ... exception here → fd is never closed
os.close(fd)

# CORRECT: context manager guarantees close()
with open("file.parquet", "rb") as f:
    data = f.read(4096)
# f.close() is called in __exit__, even if an exception occurs
```

---

## 7. Buffered vs Direct I/O (O_DIRECT)

### 7.1 Buffered I/O: The Default

Every `read()` and `write()` in Linux is **buffered by default** — data passes through the OS page cache. Buffered I/O provides:
- **Read-ahead:** The kernel reads ahead of what you requested (typically 128 KB–1 MB) to reduce latency for sequential access
- **Write coalescing:** Multiple small writes to the same file region are merged into single disk writes
- **Page cache benefits:** Repeated reads from the same file region are served from RAM

The cost of buffered I/O:
- **Double copy on reads:** disk → page cache → process buffer (two copies in RAM)
- **Page cache occupancy:** The page cache is filled with data the application may not reuse — wasted for application-managed caches
- **Unpredictable write timing:** writes become durable unpredictably (whenever pdflush runs)

### 7.2 Direct I/O: Bypassing the Page Cache

`O_DIRECT` tells the kernel to skip the page cache for this file descriptor. Data is transferred directly between the process's memory buffer and the storage device, bypassing the kernel buffer entirely.

Requirements for O_DIRECT:
- Buffer address must be aligned to the logical block size (typically 512B or 4096B)
- Read/write offset must be aligned to the block size
- Read/write count must be a multiple of the block size

```python
import os
import mmap

# O_DIRECT with aligned buffer
O_DIRECT = 0x4000   # Linux-specific flag

# Allocate a 4 KB aligned buffer using mmap (guarantees page alignment)
buf = mmap.mmap(-1, 4096)   # anonymous mmap → page-aligned memory

fd = os.open("large_file.bin", os.O_RDONLY | O_DIRECT)
# Read aligned to block size
n = os.read(fd, 4096)   # reads directly from device, bypassing page cache
os.close(fd)
buf.close()
```

**Why use O_DIRECT?**

- **Avoid double-buffering:** When the application manages its own I/O cache (like databases, Kafka with its own page cache management), the OS page cache adds a redundant copy. O_DIRECT lets the application read directly into its own cache.
- **Predictable memory usage:** O_DIRECT reads don't pollute the page cache — the OS page cache remains available for other data.
- **Consistent latency:** Page cache-based reads have variable latency (cache hit: fast; cache miss: slow). O_DIRECT always goes to the device, providing consistent (though not necessarily lower) latency.
- **Benchmarking disk performance:** Testing actual disk throughput requires bypassing the page cache to avoid measuring RAM bandwidth.

**Kafka's use of O_DIRECT (and why it doesn't):**

Kafka explicitly does NOT use O_DIRECT for its log writes. Instead, Kafka relies on the OS page cache for write buffering and uses `sendfile()` for consumer reads (which reads from the page cache). This is a deliberate design decision: Kafka assumes the page cache is large enough to hold the "hot" portion of the log (recently produced messages), and `sendfile()` can serve consumers from the page cache without any user-space copies. If the page cache is evicted (by other processes), consumer reads become slow — which is why Kafka brokers should have dedicated machines with large amounts of RAM.

**Databases using O_DIRECT:** PostgreSQL and MySQL use O_DIRECT by default (or have an option for it). Their buffer pool (shared_buffers in Postgres, innodb_buffer_pool_size in MySQL) is the application-managed cache. Using O_DIRECT prevents the OS from double-caching: the database manages what's in its buffer pool; the OS does not need to duplicate it in the page cache.

### 7.3 Buffered vs Direct I/O Performance Comparison

```
Sequential read of 10 GB file:

Buffered (first read, cold cache):
  → Page cache miss on every 4 KB page
  → OS reads 4 KB from disk per page fault / readahead
  → OS readahead: prefetches 128 KB–1 MB ahead → effective sequential throughput
  → Throughput: ~NVMe raw throughput (2-7 GB/s)
  → After read: 10 GB in page cache (will persist until evicted)

Buffered (second read, warm cache):
  → Page cache hit on every page
  → Data served from RAM
  → Throughput: ~50 GB/s (DRAM bandwidth limited)

Direct (O_DIRECT):
  → DMA from NVMe directly to aligned process buffer
  → No page cache allocation
  → Throughput: ~NVMe raw throughput (2-7 GB/s)
  → After read: nothing in page cache (no pollution)

For data engineering:
  Read once, process: Buffered > Direct (second reads benefit from cache)
  Read once, never repeat: Direct >= Buffered (cache won't be used again)
  Application manages its own cache: Direct (prevent OS double-caching)
```

---

## 8. I/O Multiplexing: select, poll, epoll

### 8.1 The Problem: Monitoring Many File Descriptors

A Spark shuffle server must monitor thousands of incoming connections — executor A requesting partition 0, executor B requesting partition 1, etc. The naive approach is one thread per connection: 1000 connections × 1 thread = 1000 threads, each blocked in `recv()` waiting for data.

This doesn't scale: 1000 threads consume 1000 × 8 MB = 8 GB of stack space, require 1000 context switches to service any ready connection, and have high scheduling overhead when most connections are idle most of the time.

The alternative: one thread monitors all connections simultaneously and handles whichever ones are ready. This requires an efficient way to ask the kernel "which of these N file descriptors have data ready to read?"

### 8.2 `select()`: The Original — O(N) Scan

```c
fd_set read_fds;
FD_ZERO(&read_fds);
FD_SET(fd1, &read_fds);
FD_SET(fd2, &read_fds);
// ...
int ready = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);
// After: read_fds is modified to show which fds are ready
if (FD_ISSET(fd1, &read_fds)) { /* fd1 is ready to read */ }
```

**Problems with `select()`:**
1. **FD_SETSIZE limit:** Maximum 1024 file descriptors (hard-coded constant in `fd_set`)
2. **O(N) kernel scan:** On every call, the kernel scans all N fds to check readiness
3. **O(N) user scan:** The caller must scan all N bits to find which are ready
4. **Argument modification:** `read_fds` is overwritten by `select()` — must rebuild on every call

`poll()` fixes the FD_SETSIZE limit (no hard limit) and the rebuild requirement (struct array instead of bitset), but still performs an O(N) kernel scan on every call.

### 8.3 `epoll`: The Scalable O(1) Alternative

`epoll` (introduced in Linux 2.5.44) solves the O(N) scan problem with a different design:

```
epoll design:
  1. Create an epoll instance (a kernel data structure: a red-black tree)
  2. Register file descriptors with epoll (add to the red-black tree)
  3. When a registered fd becomes readable/writable:
     kernel adds it to an "ready list" (a doubly-linked list)
  4. epoll_wait() returns ONLY the ready fds (from the ready list)
     No scanning! The kernel maintains the list dynamically.

Cost:
  epoll_ctl (register fd): O(log N) — insert into red-black tree
  epoll_wait (wait for events): O(1) for returning K ready events
  → amortized O(1) per event, regardless of total registered fd count
```

```python
import select
import socket

# Create epoll instance
epoll = select.epoll()

# Create listening socket
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('0.0.0.0', 8080))
server.listen(1024)
server.setblocking(False)

# Register server socket for incoming connections
epoll.register(server.fileno(), select.EPOLLIN)

connections = {}   # fd → socket
responses = {}     # fd → data to send

try:
    while True:
        # Wait for events on any registered fd
        # Returns only ready fds — O(1) per event
        events = epoll.wait(timeout=1.0)  # max 1 second wait
        
        for fd, event in events:
            if fd == server.fileno():
                # New incoming connection
                conn, addr = server.accept()
                conn.setblocking(False)
                epoll.register(conn.fileno(), select.EPOLLIN)
                connections[conn.fileno()] = conn
                
            elif event & select.EPOLLIN:
                # Data ready to read on this fd
                data = connections[fd].recv(4096)
                if data:
                    # Echo back (demo)
                    responses[fd] = data
                    epoll.modify(fd, select.EPOLLOUT)  # switch to write mode
                else:
                    # Connection closed
                    epoll.unregister(fd)
                    connections[fd].close()
                    del connections[fd]
                    
            elif event & select.EPOLLOUT:
                # Socket ready to write
                sent = connections[fd].send(responses[fd])
                responses[fd] = responses[fd][sent:]
                if not responses[fd]:
                    epoll.modify(fd, select.EPOLLIN)  # switch back to read mode

finally:
    epoll.close()
    server.close()
```

### 8.4 Edge-Triggered vs Level-Triggered epoll

**Level-triggered (default, `EPOLLIN`):** epoll_wait returns the fd as "ready" as long as there is data available — every call to epoll_wait that includes an unread fd will return it. Similar to `poll()` semantics.

**Edge-triggered (`EPOLLIN | EPOLLET`):** epoll_wait returns the fd exactly once when the state transitions from "not ready" to "ready." If you don't read all available data in that notification, you won't be notified again until more data arrives. Requires reading in a loop until `EAGAIN` (no more data available without blocking).

Edge-triggered is more efficient (fewer epoll_wait wakeups) but requires careful coding — missing the EAGAIN check causes data to be silently ignored:

```python
import errno

def handle_readable_edge_triggered(fd: int, conn: socket.socket) -> None:
    """
    Edge-triggered epoll: must read ALL available data on each notification.
    If we stop before EAGAIN, we won't get another notification until new data arrives.
    """
    while True:
        try:
            data = conn.recv(65536)
            if not data:
                # Connection closed
                break
            process(data)
        except BlockingIOError:
            # EAGAIN: no more data currently available
            # Return and wait for next epoll notification
            break
```

**Kafka's network layer (Java NIO Selector):** Kafka's broker uses Java NIO's `Selector`, which is backed by `epoll` on Linux. The broker's network thread poll loop calls `selector.select(timeout)` (which calls `epoll_wait`) and processes only the ready channels. One network thread handles hundreds of connections; each connection is non-blocking with edge-triggered notification for writes and level-triggered for reads.

---

## 9. io_uring: The Modern Asynchronous I/O Interface

### 9.1 The Remaining Overhead in epoll

Even with epoll, each I/O operation requires at least two syscalls:
1. `epoll_wait()` — to find out the fd is ready
2. `read()` or `write()` — to perform the actual I/O

For high-throughput systems processing millions of events per second, even ~200 ns per syscall × 2 × millions = significant CPU overhead. Additionally, `epoll` still blocks the thread — while waiting in `epoll_wait()`, the thread cannot do other work.

Linux `aio` (POSIX asynchronous I/O) attempted to solve this but had significant limitations: it didn't work with buffered I/O (only O_DIRECT), required kernel memory allocation per request, and had poor error handling.

### 9.2 io_uring Architecture

`io_uring` (introduced in Linux 5.1, 2019) is a fundamentally different I/O interface built around two **shared memory ring buffers** between the kernel and user space:

```
io_uring shared memory layout:

User space                    Kernel space
    │                              │
    ▼                              ▼
┌─────────────────────────────────────┐
│      Submission Queue (SQ)          │
│  ┌──────────────────────────────┐   │
│  │ SQ Entry 0: read(fd, buf, n) │   │ ← user writes here
│  │ SQ Entry 1: write(fd2, buf2) │   │
│  │ SQ Entry 2: fsync(fd3)       │   │
│  └──────────────────────────────┘   │
│      SQ tail: 3 (user sets this)    │
│      SQ head: 0 (kernel reads this) │
├─────────────────────────────────────┤
│      Completion Queue (CQ)          │
│  ┌───────────────────────────────┐  │
│  │ CQ Entry 0: result=1024       │  │ ← kernel writes here
│  │ CQ Entry 1: result=512        │  │
│  └───────────────────────────────┘  │
│      CQ head: 0 (user reads this)   │
│      CQ tail: 2 (kernel sets this)  │
└─────────────────────────────────────┘
```

**The critical insight:** The ring buffers are in shared memory — **no syscalls required to submit I/O requests or read completions in the fast path.** The user writes SQ entries and advances the tail pointer (a memory write — no kernel involvement). The kernel reads SQ entries and processes them, writing CQ entries and advancing the CQ tail. The user reads CQ entries by scanning the CQ (memory reads — no kernel involvement).

The only syscall is `io_uring_enter()` — called to submit pending SQ entries (kick the kernel) and/or wait for CQ entries. Even this syscall can be eliminated with `IORING_SETUP_SQPOLL` — a kernel thread that polls the SQ continuously, processing entries without any user-space syscall.

### 9.3 io_uring Operations

```python
# Python io_uring bindings (liburing, as of 2024)
# Note: native Python io_uring support is still evolving
# The following illustrates the conceptual API

import liburing   # third-party binding (not yet stdlib)

ring = liburing.io_uring()
liburing.io_uring_queue_init(32, ring, 0)   # 32-entry queue

# Submit a read operation WITHOUT a syscall (in the fast path)
sqe = liburing.io_uring_get_sqe(ring)
liburing.io_uring_prep_read(sqe, fd, buf, 4096, offset)
sqe.user_data = 42   # tag to identify this operation in the completion

liburing.io_uring_submit(ring)   # ONE syscall can submit N pending operations

# Poll for completions (memory read, no syscall in fast path)
cqe = liburing.io_uring_wait_cqe(ring)
print(f"Operation {cqe.user_data} completed: {cqe.res} bytes")
liburing.io_uring_cqe_seen(ring, cqe)   # mark as consumed

liburing.io_uring_queue_exit(ring)
```

**io_uring operation types:**
- `IORING_OP_READ` / `IORING_OP_WRITE`: standard file I/O
- `IORING_OP_READV` / `IORING_OP_WRITEV`: scatter/gather I/O (iovec)
- `IORING_OP_RECV` / `IORING_OP_SEND`: socket I/O
- `IORING_OP_ACCEPT`: accept new connection
- `IORING_OP_FSYNC` / `IORING_OP_FDATASYNC`: flush to disk
- `IORING_OP_OPENAT` / `IORING_OP_CLOSE`: open/close files
- `IORING_OP_STATX`: file stat without blocking
- `IORING_OP_LINK`: link operations (chain requests: "do B after A completes")

### 9.4 io_uring vs epoll: When to Use Which

| Criterion | epoll | io_uring |
|---|---|---|
| Kernel version required | 2.5.44+ (2002) | 5.1+ (2019) |
| Language/ecosystem support | Excellent (Python select, Java NIO, Rust tokio) | Growing (Rust tokio 1.x, Kotlin Coroutines, some C/C++) |
| Syscall overhead | 2 syscalls per I/O (epoll_wait + read/write) | ~0 syscalls in fast path (SQE/CQE ring) |
| CPU efficiency (high throughput) | Good | Excellent (up to 50% less CPU for same throughput) |
| Latency (low-concurrency) | Similar | Similar (the fast path doesn't help at low QPS) |
| Batch submission | No (one syscall per operation) | Yes (one syscall can submit N operations) |
| Operation chaining | No | Yes (IOSQE_IO_LINK: B starts after A completes) |
| File I/O (not just sockets) | No (epoll doesn't support regular files) | Yes (works with all fd types) |
| Security (TOCTOU) | Existing | io_uring has been a source of security vulnerabilities; Cloudflare disabled it in their edge proxies in 2023 due to exploits |

**epoll for regular files:** A critical limitation of epoll is that it does not work with regular files — a regular file is always "ready" (you can always read from it; if you hit EOF, you get 0 bytes). `epoll_wait` on a regular file returns immediately without blocking. epoll is designed for sockets, pipes, and other fd types that can block.

**io_uring and regular files:** io_uring works with all file descriptor types including regular files. This is why io_uring is relevant for Parquet reading, Spark shuffle file access, and other data engineering patterns that involve regular file I/O with high parallelism.

---

## 10. Mental Models

### 10.1 The Post Office Model for System Calls

A user-space program trying to access the kernel is like a civilian trying to access a restricted government facility. Civilians cannot just walk in — they must go to a designated counter (system call interface), fill out a form (put arguments in registers), ring a bell (execute the `syscall` instruction), and wait for an officer (kernel) to process the request. The officer does the privileged work (reads files, sends data), hands back the result at the counter, and the civilian returns with what they need.

The overhead of this process (walking to the counter, waiting for the officer, walking back) is ~100–400 ns per syscall. For high-frequency operations, minimizing the number of trips to the counter (large read/write buffers, io_uring batch submission) is the key optimization.

### 10.2 The Restaurant Order Model for epoll vs select

`select()` is like a waiter who checks on every table every few minutes, regardless of whether they've ordered something. With 1000 tables (file descriptors), the waiter spends 90% of the time checking tables that don't need anything.

`epoll` is like a table call system where tables press a button when they're ready. The waiter only responds when a button is pressed. With 1000 tables, the waiter is idle until one of the 5 currently-active tables needs service. Much more efficient.

`io_uring` is like a drive-through window with a conveyor belt: customers (user space) put orders on the belt (SQ entries); the kitchen (kernel) processes them and puts results on a return belt (CQ entries). No conversation required — the customer writes orders and picks up results without any back-and-forth. Even better: a batch of 10 orders can be placed on the belt with one push (one `io_uring_enter` syscall for N SQ entries).

### 10.3 The Safe Deposit Box Model for fsync

Writing to a file without `fsync()` is like depositing money at a bank and receiving a receipt, but the bank hasn't yet put the money in the vault — it's on the teller's desk. If the bank burns down tonight (power failure), the money is gone despite the receipt. `fsync()` says: "put it in the vault now, and don't give me the confirmation until the vault door is closed." You wait at the counter (fsync blocks) while the teller physically carries the money to the vault, locks the door, and walks back.

This is why `fsync()` is slow on HDDs (~10 ms) — the teller has to physically walk to the vault (rotational latency) and wait for the platter to rotate to write the data. NVMe SSDs have a faster vault (~50–200 µs for a flush operation) — an elevator instead of a walk.

---

## 11. Failure Scenarios

### 11.1 File Descriptor Exhaustion in Spark Shuffle

```
Problem: Spark executor with spark.shuffle.partitions=500
  Map phase output: 500 shuffle files (one per partition)
  Reduce phase: each reducer opens files from all map tasks
  
  If 100 map tasks and 500 partitions:
    Each reducer needs: 100 file descriptors (one per map task shuffle file)
    500 reducers running simultaneously on one executor: 500 × 100 = 50,000 fds
    Default ulimit -n = 1024 → EMFILE error
    
  Error: java.io.FileNotFoundException: Too many open files (EMFILE)
  
  Diagnosis:
    ls /proc/<executor_PID>/fd | wc -l  # count open fds
    cat /proc/sys/fs/file-nr            # system-wide open fds
    
  Fix 1: Increase ulimit
    # In executor launch script or systemd service:
    ulimit -n 131072
    # Or spark-env.sh:
    export ULIMIT_NOFILE=131072
    
  Fix 2: Enable external shuffle service
    spark.shuffle.service.enabled=true
    # The external shuffle service (a separate process) manages shuffle files
    # Executors can die without losing shuffle data
    # External service handles its own fd pool with higher limits
    
  Fix 3: Reduce shuffle partitions
    spark.sql.shuffle.partitions=200  (instead of 500)
    # Fewer partitions = fewer concurrent open fds during reduce
```

### 11.2 Data Loss from Missing fsync

```
Scenario: ETL job writes results to a file and considers the job "done"
  when write() returns successfully. The machine crashes 10 seconds later.
  
  # UNSAFE: write() only goes to page cache
  with open("results.parquet", "wb") as f:
      f.write(parquet_bytes)
  # f.close() is called, but close() does NOT guarantee fsync
  # If machine loses power here: results.parquet may be 0 bytes on disk
  
  What actually happened:
    write() → page cache (in RAM) ← data is here
    close() → decrements file reference count (may flush Python buffer)
    Power loss → page cache lost → disk still has old/empty file
  
  # SAFE: explicit fsync before considering write complete
  with open("results.parquet", "wb") as f:
      f.write(parquet_bytes)
      f.flush()          # flush Python IO buffer to OS page cache
      os.fsync(f.fileno())  # flush page cache to storage device
  
  # Even safer: atomic write (write to temp file, fsync, then rename)
  import tempfile
  
  dir_path = os.path.dirname("results.parquet")
  with tempfile.NamedTemporaryFile(dir=dir_path, delete=False) as tmp:
      tmp.write(parquet_bytes)
      tmp.flush()
      os.fsync(tmp.fileno())   # ensure data is on disk
      os.rename(tmp.name, "results.parquet")
  # rename() is atomic: either the old or new file is visible, never a partial write
  # fsync the directory to ensure the rename is durable:
  dir_fd = os.open(dir_path, os.O_RDONLY)
  os.fsync(dir_fd)
  os.close(dir_fd)
```

### 11.3 fsync Storm: Too Many Files Being Synced Simultaneously

```
Problem: dbt pipeline finishes 50 model materialization queries simultaneously.
  Each model: runs INSERT INTO ... AS SELECT ..., then fsyncs the result file.
  50 simultaneous fsyncs on a single NVMe SSD.
  
  NVMe SSD: ~5,000 IOPS for random writes, ~50,000 for sequential.
  Each fsync: 1-5 "flush cache" commands to the device.
  50 simultaneous fsyncs: device queue depth = 50 concurrent flush commands.
  
  Symptoms:
    - dbt models all complete in ~30s but overall dbt run takes 3 minutes
    - iostat shows: util=100%, await=2000ms (2 second per-I/O wait time)
    - All 50 models are waiting in fsync() for the device
  
  Root cause: fsync operations to the same device are serialized in the device queue.
  The device processes them one at a time → 50 fsyncs × 50 ms average = 2.5 seconds.
  
  Fix 1: Stagger fsyncs (if application allows)
    Run models in waves of 5 instead of 50 simultaneously.
  
  Fix 2: Use a faster storage device
    NVMe with higher queue depth (IOPS: 500K+ vs 5K): 50 fsyncs × 1 ms = 50 ms.
  
  Fix 3: Use fdatasync instead of fsync
    fdatasync skips metadata → faster, fewer commands to device.
  
  Fix 4: Batch writes to fewer files
    50 model results → 1 database transaction → 1 fsync.
    (depends on the database: Postgres, DuckDB, etc.)
```

### 11.4 Short Read Causing Silent Data Truncation

```python
# BUG: assuming read() always returns exactly N bytes

def read_parquet_footer(filename: str) -> bytes:
    with open(filename, "rb") as f:
        f.seek(-8, 2)   # seek to last 8 bytes
        footer_info = f.read(8)   # BUG: may return < 8 bytes on slow network filesystem
    footer_size = int.from_bytes(footer_info[:4], 'little')
    # footer_size is wrong if footer_info is < 4 bytes → silent corruption
    return footer_info

# FIX: handle short reads explicitly
def read_parquet_footer_safe(filename: str) -> bytes:
    with open(filename, "rb") as f:
        f.seek(-8, 2)
        footer_info = b""
        while len(footer_info) < 8:
            chunk = f.read(8 - len(footer_info))
            if not chunk:
                raise ValueError(f"Unexpected EOF reading Parquet footer")
            footer_info += chunk
    return footer_info
```

Network file systems (NFS, HDFS via FUSE, S3FS) can return short reads even for regular files. This is particularly insidious: the read succeeds (returns > 0 bytes) but returns fewer bytes than requested. Code that assumes `f.read(N)` always returns exactly N bytes (unless EOF) silently processes truncated data.

### 11.5 The O_SYNC Performance Trap

```python
# O_SYNC: every write() call is synchronously flushed to disk
# This seems safe but is extremely slow

import os

# SLOW: O_SYNC means each write() is an implicit fsync()
fd = os.open("output.bin", os.O_WRONLY | os.O_CREAT | os.O_SYNC, 0o644)
for chunk in generate_data():
    os.write(fd, chunk)   # each call blocks until disk writes the data
                          # on HDD: 10ms per write → 100 writes/s max
                          # on NVMe: 200µs per write → 5000 writes/s max
os.close(fd)

# FAST and SAFE: write to page cache, single fsync at the end
fd = os.open("output.bin", os.O_WRONLY | os.O_CREAT, 0o644)
for chunk in generate_data():
    os.write(fd, chunk)   # write to page cache: ~nanoseconds
os.fsync(fd)              # ONE flush to disk at the end
os.close(fd)

# For Kafka: O_SYNC is never used on the log path.
# Kafka batches messages in memory, writes to page cache via write(),
# and calls fdatasync() periodically (log.flush.interval.messages).
# This achieves 1M+ msg/s instead of 5,000 msg/s.
```

---

## 12. Data Engineering Connections

### 12.1 Kafka's Complete I/O Model

Kafka's high throughput is directly the result of careful I/O design at the syscall level:

**Producer write path:**
```
Producer → Kafka Broker:
  1. TCP send: producer's socket → network → broker's network thread (epoll)
  2. Network thread: recv() the request bytes, parse protocol header
  3. Pass to I/O thread pool (purgatory): appends to partition's active log segment
  4. log.append(): write() to log segment file (buffered → page cache)
     - No fsync on every message (too slow for 1M msg/s)
     - Configurable: log.flush.interval.messages (default: never, rely on replication)
  5. Update in-memory offset index (no disk I/O)
  6. Send ack to producer via epoll-monitored socket

Key syscalls: recv(), write(), send()
Not used: fsync() on the critical path (only on interval/shutdown)
```

**Consumer read path (zero-copy):**
```
Consumer → Kafka Broker (zero-copy with sendfile):
  1. Consumer sends FETCH request → broker's network thread receives it
  2. Broker computes byte range to send: (partition, base_offset, max_bytes)
  3. Look up log segment file and byte range from index
  4. sendfile(log_segment_fd, consumer_socket_fd, offset, length)
     ← kernel sends data from page cache directly to socket → NIC DMA
     ← NO copy through broker's JVM heap
     ← NO copy through any user-space buffer
  5. Consumer receives the data, parses message batches

sendfile() in numbers:
  Without zero-copy: disk → page cache → JVM buffer → socket → NIC
    = 2 kernel copies + 2 DMA transfers
  With sendfile(): disk → page cache → NIC
    = 0 kernel copies + 2 DMA transfers
    
Throughput improvement: 10 Gbps NIC can sustain full line rate with sendfile().
Without zero-copy: CPU becomes the bottleneck at ~2-3 Gbps (copy bandwidth limited).
```

**Kafka's epoll usage (Java NIO):**
```java
// Kafka's network thread (simplified) - Java NIO Selector backed by epoll
Selector selector = Selector.open();
ServerSocketChannel serverChannel = ServerSocketChannel.open();
serverChannel.configureBlocking(false);
serverChannel.register(selector, SelectionKey.OP_ACCEPT);

while (true) {
    // blocks until at least one channel is ready
    int readyCount = selector.select(300);  // calls epoll_wait internally
    
    Set<SelectionKey> readyKeys = selector.selectedKeys();
    for (SelectionKey key : readyKeys) {
        if (key.isAcceptable()) {
            // New connection
            SocketChannel client = serverChannel.accept();
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ);
        } else if (key.isReadable()) {
            // Data available to read from this client
            SocketChannel client = (SocketChannel) key.channel();
            client.read(buffer);
            processRequest(buffer);
        }
    }
    readyKeys.clear();
}
```

One Kafka network thread handles hundreds of connections via epoll. Kafka defaults to 3 network threads per broker (`num.network.threads=3`), handling potentially thousands of producer and consumer connections.

### 12.2 Spark Shuffle: File I/O Under the Hood

**Shuffle write (map side):**
```
Spark task executing a groupBy or join (map side):
  1. Tungsten sort: sort records by (partition_id, sort_key) using off-heap sort
  2. SpillWriter: as sort memory fills:
     - open(shuffle_tmp_XXXXX.index, O_WRONLY | O_CREAT | O_TRUNC): index file
     - open(shuffle_tmp_XXXXX.data, O_WRONLY | O_CREAT | O_TRUNC): data file
     - For each partition: write(data_fd, partition_bytes) → page cache
     - After all partitions: write(index_fd, partition_offsets)
     - close() both files
  3. Rename temp files to final names (atomic via rename() syscall)
  4. No fsync on shuffle files (they're ephemeral: recomputed on failure)

Key design: no fsync needed for shuffle files.
If an executor dies, the DAGScheduler resubmits the map tasks to recompute.
Shuffle files are not durability-critical → no fsync overhead.
```

**Shuffle read (reduce side):**
```
Spark task reading shuffle data:
  1. RequestManager: one HTTP request per shuffle block to the shuffle service
     - HTTP GET /shuffle/appId/mapId/reduceId
  2. Shuffle service: uses Java NIO (epoll-backed) to serve requests
     - Locates shuffle file by path convention
     - Reads index file: pread(index_fd, &offset, 8, reduce_id * 8)
     - Serves data range: sendfile(data_fd, http_socket_fd, offset, length)
  3. Reduce task receives bytes, decodes records, applies reduce function
  
  External shuffle service uses sendfile() for zero-copy block serving.
  Without external shuffle service: executor reads shuffle blocks via HTTP
  from other executors' block managers (also using sendfile()).
```

### 12.3 Python asyncio and epoll

Python's `asyncio` framework is built on top of `epoll` (on Linux), `kqueue` (on macOS/BSD), and `IOCP` (on Windows). The `asyncio` event loop is essentially an epoll wrapper:

```python
import asyncio
import aiohttp
import asyncpg

async def etl_pipeline():
    """
    Async ETL: concurrent I/O without threads.
    All I/O operations release the event loop while waiting.
    One thread handles N concurrent I/O operations via epoll.
    """
    # Create async database connection pool
    pool = await asyncpg.create_pool("postgresql://...")
    
    # Fetch from multiple APIs concurrently (10 at a time)
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(10)   # limit concurrent requests
        
        async def fetch_and_insert(item_id: int) -> None:
            async with semaphore:
                async with session.get(f"https://api.example.com/{item_id}") as resp:
                    data = await resp.json()   # epoll waits for HTTP response
                # Insert to database
                await pool.execute(
                    "INSERT INTO items VALUES ($1, $2)", item_id, data['value']
                )   # epoll waits for DB ack
        
        # Launch 1000 coroutines; epoll handles all I/O concurrently
        tasks = [fetch_and_insert(i) for i in range(1000)]
        await asyncio.gather(*tasks)

# Under the hood:
# asyncio.get_event_loop() creates a DefaultEventLoop backed by epoll
# event_loop.run_until_complete() calls epoll_wait() in a loop
# When an await is hit: coroutine registers its socket fd with epoll and suspends
# When the socket becomes ready: epoll_wait returns it, event loop resumes the coroutine
asyncio.run(etl_pipeline())
```

**Why asyncio is appropriate for data engineering I/O:**
- Reading from multiple REST APIs, databases, and object stores concurrently
- One thread handling 1000+ concurrent I/O waits (no thread-per-connection overhead)
- GIL is released during I/O syscalls (epoll_wait, recv, send) → no GIL penalty for I/O

**Why asyncio is NOT appropriate for CPU-bound data engineering:**
- All coroutines share one thread → one blocking computation blocks all coroutines
- DataFrame manipulation, JSON parsing, NumPy math: do these synchronously or in process pools

### 12.4 PyArrow and Parquet: read() vs mmap() vs pread()

PyArrow offers three file I/O strategies for Parquet reading:

```python
import pyarrow.parquet as pq
import pyarrow as pa

# Strategy 1: Standard buffered read() (for small files or object stores)
table = pq.read_table("s3://bucket/data.parquet")
# Uses pyarrow.fs.S3FileSystem: reads in chunks via S3 GET requests
# Each column chunk: separate GET request → HTTP round trip (~50-200ms)
# Good for: S3, GCS, Azure Blob where random access is expensive

# Strategy 2: mmap (for local files, default)
table = pq.read_table("data.parquet", memory_map=True)
# File is mmap'd; column chunks accessed via demand paging
# Zero-copy: arrow arrays point into mmap'd page cache pages
# Good for: local SSD, high-speed NFS

# Strategy 3: pread() via ArrowFile abstraction
f = pa.memory_map("data.parquet", "r")
pf = pq.ParquetFile(f)
for i in range(pf.metadata.num_row_groups):
    rg = pf.read_row_group(i, columns=["user_id", "amount"])
# Uses pread() for thread-safe access to specific byte ranges
# Good for: parallel column reading with multiple threads

# Performance comparison for 1 GB Parquet file, 10 columns:
# mmap (warm):          0.1s (page cache hit)
# mmap (cold):          0.5s (NVMe read, demand paging)
# Standard read (warm): 0.2s (page cache hit + user-space copy)
# Standard read (cold): 0.6s (NVMe read + user-space copy)
# S3 GET (remote):      2-5s (network latency + throughput)
```

---

## 13. Code Toolkit

### 13.1 `syscall_profiler.py` — Measure Syscall Overhead and I/O Performance

```python
"""
syscall_profiler.py — Profile system call and I/O performance.

Measures:
  1. Syscall overhead: cost of a minimal syscall (getpid)
  2. read() performance vs buffer size
  3. write() performance vs buffer size  
  4. fsync() cost
  5. epoll vs select performance comparison

Run directly for an I/O performance profile of the current system.
"""
from __future__ import annotations
import os
import sys
import time
import select
import socket
import tempfile
import statistics
from pathlib import Path


def measure_syscall_overhead(n: int = 100_000) -> dict:
    """Measure bare syscall cost using getpid() — the cheapest syscall."""
    times = []
    for _ in range(n):
        t0 = time.perf_counter_ns()
        os.getpid()
        times.append(time.perf_counter_ns() - t0)
    times.sort()
    return {
        "mean_ns":   statistics.mean(times),
        "median_ns": times[len(times) // 2],
        "p99_ns":    times[int(0.99 * len(times))],
    }


def measure_read_throughput(file_size_mb: int = 100) -> dict:
    """Measure read() throughput for different buffer sizes."""
    # Create test file
    test_data = os.urandom(1024 * 1024)  # 1 MB random data
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
        for _ in range(file_size_mb):
            f.write(test_data)

    results = {}
    buffer_sizes = [512, 4096, 65536, 1024*1024, 8*1024*1024]

    try:
        for buf_size in buffer_sizes:
            # Drop page cache if possible, else accept warm-cache timing
            try:
                os.system("sync && echo 1 > /proc/sys/vm/drop_caches 2>/dev/null")
            except Exception:
                pass

            t0 = time.perf_counter()
            syscall_count = 0
            with open(path, "rb") as f:
                while True:
                    data = f.read(buf_size)
                    syscall_count += 1
                    if not data:
                        break
            elapsed = time.perf_counter() - t0

            total_mb = file_size_mb
            results[buf_size] = {
                "throughput_MBps": total_mb / elapsed,
                "elapsed_s": elapsed,
                "syscall_count": syscall_count,
                "syscall_overhead_s": syscall_count * 200e-9,  # estimate at 200ns/call
            }
    finally:
        os.unlink(path)

    return results


def measure_fsync_cost(n_writes: int = 100, write_size: int = 4096) -> dict:
    """Measure fsync() cost for different storage types."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name

    data = os.urandom(write_size)
    fsync_times = []

    try:
        with open(path, "wb") as f:
            for _ in range(n_writes):
                f.write(data)
                f.flush()
                t0 = time.perf_counter_ns()
                os.fsync(f.fileno())
                fsync_times.append(time.perf_counter_ns() - t0)
    finally:
        os.unlink(path)

    fsync_times.sort()
    return {
        "mean_us": statistics.mean(fsync_times) / 1000,
        "median_us": fsync_times[len(fsync_times) // 2] / 1000,
        "p99_us": fsync_times[int(0.99 * len(fsync_times))] / 1000,
        "max_us": fsync_times[-1] / 1000,
        "implied_iops": 1_000_000 / (statistics.mean(fsync_times) / 1000),
    }


def measure_write_patterns(file_size_mb: int = 100) -> dict:
    """Compare: many small writes vs few large writes."""
    results = {}
    total_bytes = file_size_mb * 1024 * 1024

    for (label, write_size) in [
        ("1KB writes", 1024),
        ("4KB writes", 4096),
        ("64KB writes", 65536),
        ("1MB writes", 1024 * 1024),
        ("8MB writes", 8 * 1024 * 1024),
    ]:
        data = os.urandom(write_size)
        n_writes = total_bytes // write_size

        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            t0 = time.perf_counter()
            with open(path, "wb") as f:
                for _ in range(n_writes):
                    f.write(data)
                # NO fsync — measuring page cache write speed
            elapsed = time.perf_counter() - t0

            results[label] = {
                "throughput_MBps": file_size_mb / elapsed,
                "syscall_count": n_writes,
                "elapsed_s": elapsed,
            }
        finally:
            os.unlink(path)

    return results


def demo_epoll_vs_select(n_sockets: int = 100) -> dict:
    """
    Compare epoll vs select latency for monitoring N sockets.
    Creates N socket pairs, writes data to all, measures time to detect readiness.
    """
    if not hasattr(select, 'epoll'):
        return {"error": "epoll not available (requires Linux)"}

    # Create N socket pairs
    pairs = [socket.socketpair() for _ in range(n_sockets)]
    read_sockets = [p[0] for p in pairs]
    write_sockets = [p[1] for p in pairs]

    # Write data to all write sockets (makes read sockets "ready")
    for ws in write_sockets:
        ws.send(b"x")

    ROUNDS = 1000

    # Measure select()
    t0 = time.perf_counter()
    for _ in range(ROUNDS):
        r, _, _ = select.select(read_sockets, [], [], 0)
    t_select = (time.perf_counter() - t0) / ROUNDS * 1e6   # µs per round

    # Measure epoll
    ep = select.epoll()
    for rs in read_sockets:
        ep.register(rs.fileno(), select.EPOLLIN)
    t0 = time.perf_counter()
    for _ in range(ROUNDS):
        events = ep.wait(timeout=0)
    t_epoll = (time.perf_counter() - t0) / ROUNDS * 1e6   # µs per round
    ep.close()

    # Cleanup
    for r, w in pairs:
        r.close()
        w.close()

    return {
        "n_sockets": n_sockets,
        "select_us_per_call": t_select,
        "epoll_us_per_call": t_epoll,
        "epoll_speedup": t_select / t_epoll if t_epoll > 0 else float('inf'),
    }


if __name__ == "__main__":
    print("=== System Call and I/O Profiler ===\n")

    # Syscall overhead
    print("1. Syscall overhead (getpid, 100K calls):")
    sc = measure_syscall_overhead(100_000)
    print(f"   Mean: {sc['mean_ns']:.0f} ns  Median: {sc['median_ns']:.0f} ns  P99: {sc['p99_ns']:.0f} ns\n")

    # Read throughput vs buffer size
    print("2. read() throughput vs buffer size (50 MB file):")
    rt = measure_read_throughput(50)
    print(f"   {'Buffer':>10}  {'Throughput':>15}  {'Syscalls':>10}")
    print(f"   {'-'*38}")
    for buf_size, stats in sorted(rt.items()):
        print(f"   {buf_size:>10,}B  {stats['throughput_MBps']:>12.0f} MB/s  {stats['syscall_count']:>10,}")
    print()

    # fsync cost
    print("3. fsync() cost (100 syncs, 4 KB each):")
    fc = measure_fsync_cost(100, 4096)
    print(f"   Mean: {fc['mean_us']:.0f} µs  P99: {fc['p99_us']:.0f} µs  Max: {fc['max_us']:.0f} µs")
    print(f"   Implied max fsync throughput: {fc['implied_iops']:.0f} fsyncs/s\n")

    # Write patterns
    print("4. Write patterns (100 MB total, different chunk sizes):")
    wp = measure_write_patterns(100)
    for label, stats in wp.items():
        print(f"   {label:<20} {stats['throughput_MBps']:>8.0f} MB/s  ({stats['syscall_count']:,} syscalls)")
    print()

    # epoll vs select
    print("5. epoll vs select (100 sockets):")
    es = demo_epoll_vs_select(100)
    if "error" not in es:
        print(f"   select:  {es['select_us_per_call']:.1f} µs/call")
        print(f"   epoll:   {es['epoll_us_per_call']:.1f} µs/call")
        print(f"   epoll speedup: {es['epoll_speedup']:.1f}x")
    else:
        print(f"   {es['error']}")
```

### 13.2 `file_io_patterns.py` — Safe File I/O Patterns for Data Engineering

```python
"""
file_io_patterns.py — Production-safe file I/O patterns.

Implements:
  1. Atomic write (write → fsync → rename → directory fsync)
  2. Large-file sequential reader with configurable buffer size
  3. Parallel column reader using pread() threads
  4. Direct I/O (O_DIRECT) reader for benchmark purposes

These are the patterns that appear in Parquet writers, Kafka log writers,
and shuffle file writers in production data systems.
"""
from __future__ import annotations
import os
import sys
import mmap
import struct
import tempfile
import threading
import concurrent.futures
from pathlib import Path
from typing import Iterator


# ─── Pattern 1: Atomic Write ──────────────────────────────────────────────────

def atomic_write(path: str, data: bytes, sync: bool = True) -> None:
    """
    Write data to path atomically.
    
    Process:
      1. Write to a temp file in the same directory
      2. fsync the temp file (ensure data is on disk)
      3. rename() to final path (atomic at the filesystem level)
      4. fsync the directory (ensure rename is durable)
    
    If the process crashes at any point:
      - Before rename: the final file is unchanged (old data preserved)
      - After rename: the final file has the new data (complete write)
      Never: a partial write is visible as the final file
    """
    dir_path = os.path.dirname(os.path.abspath(path))
    
    # Write to temp file in same directory (ensures same filesystem for rename)
    fd, tmp_path = tempfile.mkstemp(dir=dir_path)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            if sync:
                f.flush()
                os.fsync(f.fileno())
        
        # Atomic rename (POSIX guarantees rename is atomic)
        os.rename(tmp_path, path)
        
        if sync:
            # fsync the directory to ensure the rename is durable
            dir_fd = os.open(dir_path, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ─── Pattern 2: Large-File Sequential Reader ──────────────────────────────────

def sequential_reader(
    path: str,
    chunk_size: int = 8 * 1024 * 1024,   # 8 MB chunks
) -> Iterator[bytes]:
    """
    Read a large file sequentially in chunks.
    
    8 MB chunks: optimal for most filesystems and SSDs.
    - Minimizes syscall count (8 MB file / 8 MB chunks = 1 syscall per chunk)
    - Aligns well with Linux readahead (readahead_pages = 32 × 128 KB = 4 MB default)
    - Large enough to saturate NVMe bandwidth per syscall
    """
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk


def count_lines_fast(path: str) -> int:
    """Count newlines in a large text file using 8 MB buffered reads."""
    count = 0
    for chunk in sequential_reader(path):
        count += chunk.count(b"\n")
    return count


# ─── Pattern 3: Parallel Column Reader ───────────────────────────────────────

class ParallelColumnReader:
    """
    Reads multiple byte ranges from a single file in parallel using pread().
    Simulates how Parquet column chunk reading works in multi-threaded readers.
    
    pread() is used (not read()) because:
      - Thread-safe: each pread() specifies its own offset
      - No shared file offset to serialize on
      - Multiple threads can call pread() concurrently without locking
    """
    
    def __init__(self, path: str, n_threads: int = 4) -> None:
        self.path = path
        self.n_threads = n_threads
        self.fd = os.open(path, os.O_RDONLY)
    
    def __del__(self):
        try:
            os.close(self.fd)
        except Exception:
            pass
    
    def read_range(self, offset: int, length: int) -> bytes:
        """Read [offset, offset+length) from the file using pread()."""
        buf = bytearray(length)
        view = memoryview(buf)
        pos = 0
        while pos < length:
            n = os.pread(self.fd, length - pos, offset + pos)
            if not n:
                raise EOFError(f"Unexpected EOF at offset {offset + pos}")
            view[pos:pos + len(n)] = n
            pos += len(n)
        return bytes(buf)
    
    def read_ranges_parallel(
        self, ranges: list[tuple[int, int]]
    ) -> list[bytes]:
        """
        Read multiple byte ranges in parallel using a thread pool.
        
        Each thread calls pread() at its specific offset.
        Because pread() doesn't modify the file offset, no locking is needed.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n_threads) as ex:
            futures = [ex.submit(self.read_range, offset, length)
                      for offset, length in ranges]
            return [f.result() for f in futures]


# ─── Pattern 4: Reliable Read Loop ───────────────────────────────────────────

def reliable_read(fd: int, n_bytes: int) -> bytes:
    """
    Read exactly n_bytes from fd, handling short reads.
    
    Short reads can occur:
      - On pipes/sockets: when less data is immediately available
      - On NFS: when the server returns partial data
      - At filesystem boundaries
    Never assume read() returns exactly n_bytes.
    """
    result = bytearray()
    while len(result) < n_bytes:
        chunk = os.read(fd, n_bytes - len(result))
        if not chunk:
            raise EOFError(
                f"Unexpected EOF after {len(result)} bytes (expected {n_bytes})"
            )
        result.extend(chunk)
    return bytes(result)


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import time

    print("=== File I/O Patterns Demo ===\n")

    # Atomic write demo
    print("1. Atomic Write:")
    test_data = b"critical pipeline results\n" * 1000
    atomic_write("/tmp/test_atomic.bin", test_data)
    recovered = Path("/tmp/test_atomic.bin").read_bytes()
    print(f"   Written {len(test_data)} bytes atomically")
    print(f"   Read back: {len(recovered)} bytes (matches: {test_data == recovered})")
    os.unlink("/tmp/test_atomic.bin")

    # Sequential reader
    print("\n2. Sequential Reader (large buffer):")
    with tempfile.NamedTemporaryFile(delete=False) as f:
        for _ in range(100):
            f.write(os.urandom(1024 * 1024))  # 100 MB test file
        tmp_path = f.name

    t0 = time.perf_counter()
    total = sum(len(chunk) for chunk in sequential_reader(tmp_path, 8*1024*1024))
    elapsed = time.perf_counter() - t0
    print(f"   Read {total//1024//1024} MB in {elapsed:.3f}s = {total/1024/1024/elapsed:.0f} MB/s")

    # Parallel column reader
    print("\n3. Parallel Column Reader (pread):")
    reader = ParallelColumnReader(tmp_path, n_threads=4)
    # Simulate reading 4 column chunks at different offsets
    ranges = [
        (0,            10*1024*1024),   # first 10 MB (column 0)
        (25*1024*1024, 10*1024*1024),   # 10 MB at offset 25 MB (column 1)
        (50*1024*1024, 10*1024*1024),   # 10 MB at offset 50 MB (column 2)
        (75*1024*1024, 10*1024*1024),   # 10 MB at offset 75 MB (column 3)
    ]
    t0 = time.perf_counter()
    results = reader.read_ranges_parallel(ranges)
    elapsed = time.perf_counter() - t0
    total_read = sum(len(r) for r in results)
    print(f"   Read 4 × 10 MB column chunks in parallel: {elapsed:.3f}s = "
          f"{total_read/1024/1024/elapsed:.0f} MB/s effective throughput")

    os.unlink(tmp_path)
    print("\nDone.")
```

---

## 14. Hands-On Labs

### Lab 1: Measure System Call Overhead

**Goal:** Prove that read buffer size matters by measuring read() throughput at different buffer sizes, then calculating the syscall overhead.

```python
# lab1_buffer_size_impact.py
"""
Measure the impact of read buffer size on throughput.
Shows that syscall overhead dominates for small reads.
"""
import os, time, tempfile

def benchmark_read(path: str, buf_size: int, total_bytes: int) -> dict:
    n_calls = 0
    t0 = time.perf_counter()
    with open(path, "rb") as f:
        while True:
            data = f.read(buf_size)
            n_calls += 1
            if not data:
                break
    elapsed = time.perf_counter() - t0
    return {
        "buf_size": buf_size,
        "throughput_mbs": total_bytes / 1024 / 1024 / elapsed,
        "syscalls": n_calls,
        "syscall_overhead_pct": (n_calls * 200e-9) / elapsed * 100,
    }

if __name__ == "__main__":
    SIZE_MB = 100
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(os.urandom(SIZE_MB * 1024 * 1024))
        path = f.name

    print(f"{'Buffer':>10}  {'MB/s':>8}  {'Syscalls':>10}  {'SC overhead%':>14}")
    print("-" * 50)
    for buf in [512, 4096, 65536, 1024*1024, 8*1024*1024]:
        r = benchmark_read(path, buf, SIZE_MB * 1024 * 1024)
        print(f"{r['buf_size']:>10,}  {r['throughput_mbs']:>8.0f}  "
              f"{r['syscalls']:>10,}  {r['syscall_overhead_pct']:>13.1f}%")
    os.unlink(path)
```

### Lab 2: fsync Durability — Prove write() Is Not Synchronous

**Goal:** Show that write() returns before data is on disk by measuring time to detect written data after killing a process.

```bash
# lab2_write_durability.sh
#!/bin/bash
# This lab shows that write() without fsync is not durable.

TEST_FILE="/tmp/durability_test.bin"

echo "=== Write Without fsync ==="
python3 -c "
import os, time
f = open('$TEST_FILE', 'wb')
f.write(b'X' * 100_000_000)  # 100 MB
print('write() returned — data in page cache')
# No fsync — data is NOT necessarily on disk
time.sleep(0.01)  # simulate: power failure here
# In reality, if you unplug the machine now, the data may be lost
" &

sleep 0.1
# Check if file is 100 MB on disk (it may be 0 bytes if kernel hasn't written yet)
SIZE=$(stat -c%s "$TEST_FILE" 2>/dev/null || echo "0")
echo "File size immediately after write(): $SIZE bytes"
echo "(May be 0 if kernel hasn't flushed yet — data is only in page cache)"
wait

echo ""
echo "=== Write With fsync ==="
python3 -c "
import os, time
with open('$TEST_FILE', 'wb') as f:
    f.write(b'Y' * 100_000_000)
    f.flush()
    os.fsync(f.fileno())
print('fsync() returned — data IS on disk')
" &
wait

SIZE=$(stat -c%s "$TEST_FILE")
echo "File size after fsync(): $SIZE bytes (guaranteed on disk)"
rm -f "$TEST_FILE"
```

---

## 15. Interview Q&A

**Q1: What is a system call and what happens at the hardware level when one is made?**

A system call is the mechanism by which user-space processes request services from the OS kernel that they cannot perform themselves due to hardware privilege restrictions. User processes run at CPU privilege level Ring 3 (on x86), which prohibits direct access to I/O devices, other processes' memory, and hardware registers. The OS kernel runs at Ring 0 with unrestricted access.

When a system call is made, a precise sequence of hardware events occurs. The calling program places the syscall number in the `%rax` register and arguments in the next six registers (`%rdi`, `%rsi`, `%rdx`, `%r10`, `%r8`, `%r9`). It then executes the `syscall` instruction. The CPU saves the current instruction pointer and stack pointer, disables interrupts briefly, switches the CPU privilege level from Ring 3 to Ring 0, and jumps to the address stored in the kernel's `LSTAR` register — the syscall entry point. The kernel reads the syscall number from `%rax`, dispatches to the appropriate handler (e.g., `sys_read`), performs the privileged work, places the return value in `%rax`, and executes `sysret` to restore user-mode context and resume the caller.

The cost of this round-trip is approximately 100–400 ns on modern hardware. This overhead exists even if the syscall does trivial work — the minimum cost is the privilege switch itself. For a data engineering program making 10 million `read()` calls at 1 KB each to read 10 GB, the syscall overhead alone is 10M × 200 ns = 2 seconds. Switching to 8 MB reads reduces this to 1250 × 200 ns = 250 µs — a 8000× reduction in syscall overhead.

**Q2: What is the VFS and why does it matter for data engineering?**

The Virtual File System is a kernel abstraction layer that provides a uniform system call interface for file operations regardless of the underlying file system implementation. An application calling `open()`, `read()`, `write()`, and `close()` on a file gets the same semantics whether the file is on ext4, XFS, NFS, HDFS via FUSE, or a tmpfs in-memory file system. The VFS defines four key abstractions — superblock (mounted filesystem), inode (file), dentry (name→inode mapping), and file (open instance) — and each filesystem implements the VFS operations for these abstractions.

For data engineering, VFS matters in three ways. First, FUSE (Filesystem in User Space) allows mounting HDFS, S3, and GCS as local directories. The FUSE driver implements VFS operations in user space; the kernel routes file calls through FUSE to the user-space driver. This is why `hadoop fs` and `s3fs` present object storage as local files — they implement the VFS interface. The downside: FUSE has higher latency than native file systems (every I/O call crosses the kernel-user boundary twice: once for the initial syscall, once for the FUSE response), which is why FUSE-mounted S3 is slower than the native boto3 client.

Second, the dentry cache accelerates path resolution. A Spark job reading 10,000 Parquet files in the same directory resolves the directory path once and caches it. If the dentry cache is cold (e.g., right after `drop_caches`), each file open requires a dentry lookup per path component — adding overhead for large directory listings. Third, inode caching affects `stat()` performance — checking file sizes and modification times for Iceberg/Delta table manifests hits the inode cache. On NFS with many small files, inode cache misses can significantly slow metadata operations.

**Q3: What is the difference between write() and fsync(), and why does Kafka rely on replication rather than fsync for durability?**

`write(fd, buf, count)` copies data from the process's buffer into the OS page cache and returns immediately. The data is now in RAM but not necessarily on any persistent storage medium. If the machine loses power before the OS writes the dirty page cache pages to disk (the default writeback interval is 30 seconds), the data is permanently lost.

`fsync(fd)` instructs the OS to write all dirty pages for the file to the storage device and issue a storage flush command (HDDs: force the drive's write cache to platter; NVMe: force the controller's DRAM write buffer to NAND flash). It blocks until the storage device acknowledges that the data is durable. On NVMe SSDs, fsync takes 50–200 µs. On HDDs, it takes 5–15 ms. Because `fsync()` serializes with the storage device, it limits throughput: an HDD can do ~100 fsyncs per second; an NVMe can do ~5,000–50,000. Kafka's target throughput is 1 million messages per second — fsync on every message on HDD storage would limit throughput to ~100 messages per second.

Kafka solves the durability problem without per-message fsync using replication. With `replication.factor=3` and `min.insync.replicas=2`, at least 2 brokers must write the message to their page caches before acknowledging to the producer. To lose data, all 3 machines must fail simultaneously before any of them has flushed their page cache to disk. The probability of 3 independent machines failing within 30 seconds (the page cache writeback window) is negligible. This is a deliberate trade-off: eventual durability (within seconds) for throughput (millions of messages per second), rather than synchronous durability (immediate but expensive). Operators who require synchronous durability can set `log.flush.interval.messages=1` at the cost of dramatically lower throughput, or rely on `acks=all` combined with replication for a practical safety guarantee.

**Q4: Explain epoll and why it scales better than select for a system like Kafka's network layer.**

`select()` takes a set of file descriptors and scans all of them on every call to determine which are ready for I/O. This scan happens in the kernel: for N file descriptors, the kernel examines each one — O(N) work per `select()` call. Additionally, the user must scan the returned bitset to find which fds became ready — another O(N) operation. With 10,000 connections (a single Kafka broker may have this many), each call to `select()` involves 10,000 checks in the kernel and 10,000 checks in user space — even if only 5 connections have data ready.

`epoll` inverts this model. The kernel maintains a red-black tree of registered file descriptors and a "ready list" of fds that have become readable or writable since the last `epoll_wait()`. When a registered socket receives data, the kernel adds it to the ready list. When `epoll_wait()` is called, the kernel copies only the ready list — not the full registered set — to user space. The cost is O(K) where K is the number of ready events, regardless of how many total file descriptors are registered. For Kafka with 10,000 connections where typically 10–100 have active I/O at any moment, `epoll_wait()` returns 10–100 events without scanning the 9,900 idle connections.

Kafka's Java NIO `Selector` is backed by epoll on Linux. Each Kafka network thread calls `selector.select()` (epoll_wait under the hood) with a timeout. The selector returns only the channels with ready I/O — accept for new connections, read for incoming requests, write for responses. This is how one Kafka network thread handles hundreds of simultaneous producer and consumer connections with low CPU overhead.

**Q5: What is io_uring and what advantage does it have over epoll for data engineering I/O?**

io_uring is a Linux kernel I/O interface introduced in kernel 5.1 (2019) that allows user space to submit I/O requests and collect completions via shared memory ring buffers, eliminating the need for system calls in the common case. The fundamental limitation of epoll is that it still requires two syscalls per I/O operation: one `epoll_wait()` to discover that an fd is ready, and one `read()` or `write()` to perform the actual I/O. For a system doing 10 million I/O operations per second, this is 20 million syscalls per second, consuming significant CPU just for mode switching.

io_uring provides two shared ring buffers — the Submission Queue (SQ) and Completion Queue (CQ) — mapped into both user space and kernel space. User code writes I/O requests directly to the SQ (memory writes, no syscall) and reads completions from the CQ (memory reads, no syscall). A single `io_uring_enter()` syscall can submit a batch of N requests — amortizing the syscall overhead across many operations. With the `IORING_SETUP_SQPOLL` flag, a kernel thread polls the SQ continuously, eliminating even the submission syscall.

For data engineering, io_uring is relevant in three scenarios. First, high-throughput file I/O: when Parquet column readers issue thousands of `pread()` operations per second across many files, io_uring reduces the per-request syscall overhead. Second, linked I/O chains: `IOSQE_IO_LINK` allows expressing dependencies between I/O operations without synchronization — "open this file, then read the footer, then read these column chunks" as a single submission without polling for intermediate results. Third, mixed file and network I/O: unlike epoll, io_uring works for regular file reads/writes as well as sockets, enabling a single unified I/O model for systems that write Parquet files and serve them over the network simultaneously.

**Q6: Design the I/O layer for a high-throughput Parquet writer that guarantees durability without sacrificing throughput.**

The design must balance three requirements: high write throughput (minimize sync overhead), durability guarantees (data survives crashes), and read compatibility (the written files must be readable by any Parquet reader). I'd decompose the problem into the write path, the durability path, and the recovery path.

**Write path — maximize throughput:** Accumulate rows in memory (in Arrow format, columnar) until reaching a row group size threshold (128 MB is standard). Write the row group data to a temporary file using large buffered writes — no fsync on individual column chunks. Use `fallocate()` to pre-allocate the file size before writing (avoids fragmentation on HDD and reduces metadata updates). Write the Parquet footer after all row groups are written, then sync.

**Durability path — group commit:** Do not fsync after every row group. Instead, accumulate multiple row groups across potentially multiple files, then fsync in a single coordinated flush. Group commit is the standard technique from databases: batch N writes and then issue one fsync, spreading the fsync cost across N operations. For row groups of 128 MB, fsync of 3 row groups per second means one fsync per 384 MB of output — on NVMe at 200 µs per fsync, the overhead is negligible. Implement this as a background flush thread that issues fsync every N row groups or every T seconds.

**Atomic rename — ensure readers never see partial files:** Write each Parquet file to a temporary path (`part-0001.parquet.tmp`). After the final fsync of the file data, atomically rename it to the final path (`part-0001.parquet`). Fsync the parent directory after rename to ensure the rename is durable. Readers who list the directory only see complete files — never partially written ones. Any file that exists at the permanent path is guaranteed to be complete and durable.

**Recovery path:** On restart after a crash, scan for `.tmp` files and delete them (they represent incomplete writes). No partial data is visible to readers because the atomic rename was either completed (file exists at final path) or not completed (only `.tmp` exists, which is cleaned up).

This design achieves near-maximum NVMe throughput (3–7 GB/s) with one fsync per file (one blocking call per ~1 GB of data), durability at the file granularity, and atomic visibility via rename. The only trade-off: data is durable at the row-group boundary granularity, not at the individual row boundary. For append-only pipeline outputs (ETL results, reports), this is acceptable.

---

## 16. Cross-Question Chain

**Q1 [Interviewer]: What happens at the kernel level when Python calls `f.read(1000)`?**

Python's `io.BufferedReader` first checks its internal 8 KB read buffer. If the requested 1000 bytes are already in the buffer, it copies them to the caller without any syscall. If the buffer is empty or insufficient, Python calls the `read()` system call with a request for its buffer size (typically 8 KB, not just 1000 bytes). The `syscall` instruction transitions the CPU from user mode (Ring 3) to kernel mode (Ring 0). The kernel's `sys_read()` handler is invoked with the file descriptor, a kernel buffer address, and the count. The handler looks up the file descriptor in the process's fd table to find the `file` struct, reads the current file offset, and asks the VFS to read data. The VFS checks the page cache for the file's inode at the current offset. On a cache hit, the kernel copies the data from the page cache frame to the process's buffer (a `copy_to_user()` call — must handle the user/kernel boundary safely). The file offset is advanced by the bytes read. The kernel places the byte count in `%rax` and executes `sysret`. Python's IO buffer now contains up to 8 KB; the first 1000 bytes are returned to the caller.

**Q2 [Interviewer]: You mentioned the page cache. What if the page is not in the page cache?**

On a page cache miss, the VFS cannot immediately satisfy the read. The kernel must read the data from disk. The process is placed in SLEEPING state (TASK_INTERRUPTIBLE). The kernel submits a block I/O request to the block device layer, which queues it for the storage device driver. Modern NVMe drivers use multiple I/O queues (up to one per CPU core in NVMe 1.3+), submitting the request to the queue associated with the current CPU. The storage device processes the request — on NVMe, the controller reads the NAND flash and DMA-transfers 4 KB to a kernel DMA buffer. The DMA transfer completes; the storage controller signals an interrupt. The interrupt handler runs, marks the I/O as complete, and wakes up the sleeping process. The scheduler places the process back in READY state. When the CFS scheduler next selects this process, it resumes in the kernel, copies from the now-populated page cache frame to the process's buffer, and returns to user space. Total elapsed time: ~100 µs (NVMe) or ~5 ms (HDD). The calling thread was blocked for this entire duration — from the user's perspective, `f.read()` simply took longer.

**Q3 [Interviewer]: If every file read goes through the page cache, how does Kafka serve consumers at 10 Gbps without running out of CPU for copying?**

Kafka uses `sendfile()` — a system call that transfers data directly from the page cache to a socket without any user-space copy. In the standard path (page cache → user buffer → socket buffer), the CPU executes two memory copies: `copy_to_user()` (page cache → user buffer) and `copy_from_user()` (user buffer → socket buffer). At 10 Gbps = 1.25 GB/s, copying 1.25 GB/s twice = 2.5 GB/s of memory bandwidth just for the copies — consuming significant CPU on even a powerful machine.

`sendfile(file_fd, socket_fd, offset, count)` tells the kernel: "transfer `count` bytes starting at `offset` from the page cache of `file_fd` directly into the send buffer of `socket_fd`." The kernel performs this transfer within the kernel (no user-space involvement) using DMA chaining where the NIC supports it: the NIC DMA-reads directly from the page cache page, without any CPU copy. This "zero-copy" path requires the consumer socket and the file to use compatible I/O paths — both must go through kernel buffers (no O_DIRECT, no scatter-gather across different NUMA nodes). Kafka's log segment files are managed to always be in the page cache when consumers are active (by sizing Kafka broker RAM to fit the "hot" portion of the log). With `sendfile()`, one Kafka network thread can saturate a 10 Gbps NIC because the bottleneck shifts from CPU (copying) to the NIC hardware.

**Q4 [Interviewer]: epoll scales to 10,000 connections. What are its remaining limitations?**

Three significant limitations remain after epoll. First, epoll does not work with regular files. A regular file is always "ready" — `epoll_wait` on a file fd returns immediately, providing no useful notification. For high-parallelism file I/O (reading from many Parquet files simultaneously), epoll provides no benefit — you still need threads blocked in `read()` calls or an explicit async mechanism like io_uring.

Second, epoll has two syscalls per I/O operation: `epoll_wait()` to detect readiness, then `read()` or `write()` to perform the I/O. At millions of events per second, these two syscalls per operation consume significant CPU just for the mode-switch overhead (~200 ns × 2 × 1,000,000/s = 400 ms of CPU per second, dedicated to syscall overhead alone).

Third, epoll's level-triggered mode can cause CPU spin on high-throughput connections: if a socket always has data available, `epoll_wait` always returns it immediately (next call too), causing a tight loop. Edge-triggered mode avoids this but requires careful coding to drain the socket to EAGAIN. Both limitations are addressed by io_uring: it works for all fd types including regular files, and the SQ/CQ ring eliminates per-I/O syscalls.

**Q5 [Interviewer]: A Spark shuffle job is failing with "Too many open files." Walk me through diagnosing and fixing it.**

First, confirm the failure mode: the error `java.io.FileNotFoundException: ... (Too many open files)` or the Java exception wrapping EMFILE (error code 24) confirms we've hit the per-process file descriptor limit, not a permission or path issue.

Second, quantify the current usage and limits:

```bash
# Current limit for the executor JVM process
cat /proc/<spark_executor_PID>/limits | grep "open files"
# Current open fd count
ls /proc/<spark_executor_PID>/fd | wc -l
```

Third, understand why so many fds are open. A Spark executor during shuffle read opens one file per shuffle block it's fetching. With `spark.sql.shuffle.partitions=500` and 200 map tasks, each reducer executor may need to open 200 shuffle index files + 200 shuffle data files = 400 file descriptors just for one wave of reduce tasks. With 4 concurrent reduce tasks per executor, that's potentially 1600 fds, exceeding the typical 1024 limit.

Fourth, apply the fixes in order:

The immediate fix is increasing the ulimit: add `ulimit -n 65536` to `spark-env.sh` or set `LimitNOFILE=65536` in the executor systemd service. This addresses the symptom.

For the root cause — too many concurrent shuffle reads — enable the external shuffle service (`spark.shuffle.service.enabled=true`). The external shuffle service is a long-running daemon that manages shuffle file serving independently of executors. Executors request blocks from the external shuffle service (one HTTP range request per block, not one file open per block), dramatically reducing the executor's fd usage. The external shuffle service itself needs a higher ulimit, but it's a dedicated service designed for this load.

Additionally, review whether `spark.sql.shuffle.partitions` is unnecessarily high. Fewer partitions mean fewer shuffle files mean fewer fds. For a 100 GB shuffle, 200 partitions (500 MB each) may be optimal — reducing from 500 to 200 partitions reduces fd usage by 2.5×.

**Q6 [Interviewer]: Design the durability model for a Delta Lake-style transaction log where writes must be ACID-compliant.**

Delta Lake's transaction log guarantees ACID properties for table operations (INSERT, UPDATE, DELETE, MERGE). Each transaction appends a JSON commit file to the `_delta_log/` directory. Atomicity and durability require that either a commit file fully exists or does not exist — never a partial commit visible to readers.

The write sequence for a transaction commit: first, collect all the changes the transaction makes — which Parquet data files are added, which are removed, what metadata is changed. Second, serialize these changes as a JSON commit entry. Third, attempt to write the commit file atomically. On local filesystems, this is `write(temp_file)` + `fsync(temp_file)` + `atomic rename(temp_file, _delta_log/0000000N.json)` + `fsync(_delta_log/)`. Readers who list the directory only see complete commit files.

On object storage (S3, GCS, Azure Blob), there is no atomic rename — object stores use put operations which are atomic (an object either fully exists or doesn't) but not rename-equivalent. Delta Lake uses a different atomicity protocol for object stores: it uses an atomic `putIfAbsent` (conditional put) — write the commit file only if no object at that path already exists. This prevents two concurrent writers from both committing as `0000000N.json` (concurrent write conflict). The writer who wins the conditional put becomes the committed transaction; the loser must retry with a new transaction number after reading the winning commit and resolving any conflicts.

Durability after the commit is written: on S3, object stores are designed for 11 nines of durability (data is replicated across at least 3 availability zones). The "commit" of the transaction is the moment the object becomes visible via GET — S3's strong consistency guarantee (post-2020) ensures that a successful PUT is immediately visible to all subsequent GETs. No explicit fsync is needed — S3 provides this guarantee natively. On local filesystems, the fsync + directory fsync after rename is the durability guarantee.

---

## 17. Common Misconceptions

**"close() ensures data is on disk."**
`close()` does not call `fsync()`. It releases the file descriptor and may flush Python's or the C library's user-space buffer, but the data remains in the OS page cache until the OS writes it asynchronously. If the machine crashes after `close()` but before the OS flushes the page cache, the data is lost. For durable writes, call `os.fsync(fd)` before `close()`.

**"write() is slow because it writes to disk."**
`write()` to a buffered file writes to the OS page cache in RAM — it does not touch the disk. This is why `write()` is very fast (nanoseconds to microseconds). The disk write happens asynchronously, decoupled from the `write()` call. `write()` only becomes disk-speed slow if the page cache is full and the kernel must evict dirty pages before allocating new ones (memory pressure causing synchronous writeback).

**"epoll works for all file types."**
epoll works for file descriptors that can be in a "not ready" state: sockets, pipes, terminals, eventfds. It does NOT work for regular files — a regular file on a local filesystem is always "ready" (returning EPOLLIN immediately), providing no useful notification. For parallelizing regular file I/O, use threads with blocking `pread()` calls or io_uring.

**"select() and epoll() are equivalent but epoll is faster."**
They have different interfaces that lead to different programming patterns. `select()` has a hard limit of 1024 fds (FD_SETSIZE). `select()` modifies its input arguments (fd sets), requiring the caller to rebuild them before each call. `select()` scans all fds on every call (O(N)). epoll maintains state between calls, adds fds once, and returns only ready fds. These are fundamental design differences, not just performance optimizations.

**"fsync() guarantees data is on stable storage after the call returns."**
`fsync()` guarantees that data has been transmitted to the storage device and the device has acknowledged durability. But if the "storage device" is a SAN, NAS, or network-attached RAID, the device's acknowledgment may mean "written to the SAN controller's battery-backed NVRAM" rather than "written to every disk in the RAID." For true end-to-end durability in distributed storage, application-level confirmation (replication acknowledgment from multiple nodes) is the correct mechanism.

---

## 18. Performance Reference Card

| Operation | Approximate Cost | Notes |
|---|---|---|
| Syscall overhead (empty syscall) | 100–400 ns | Ring 3→0→3 transition |
| Syscall with KPTI mitigation | 300–1200 ns | TLB flush on each syscall |
| `open()` / `openat()` (cached path) | 1–5 µs | Dentry + inode cache hit |
| `open()` (cold path, disk read) | 1–10 ms | Dentry/inode cache miss |
| `read()` (page cache hit, 4 KB) | 1–3 µs | Copy from page cache to user buf |
| `read()` (page cache miss, NVMe) | 100–500 µs | NVMe read + copy |
| `write()` (to page cache) | 1–3 µs | Copy from user buf to page cache |
| `fsync()` (NVMe SSD) | 50–200 µs | Flush NVMe write buffer |
| `fsync()` (HDD) | 5–15 ms | Rotational seek + flush |
| `sendfile()` (page cache → socket) | ~same as memcpy rate | ~50 GB/s, CPU bound |
| `epoll_ctl()` (register fd) | 1–5 µs | O(log N) red-black tree insert |
| `epoll_wait()` (no ready events) | blocks up to timeout | efficient sleep |
| `epoll_wait()` (K ready events) | ~K × 100 ns | O(K) — only ready events |
| `select()` (N fds, K ready) | N × 10–50 ns | O(N) scan regardless of K |
| io_uring SQE submission | ~10 ns | Memory write to SQ ring |
| io_uring CQE read | ~10 ns | Memory read from CQ ring |
| `io_uring_enter()` with N SQEs | ~200 ns fixed | One syscall for N submissions |

**Read throughput vs buffer size (NVMe SSD, warm page cache):**

| Buffer size | Throughput | Syscalls for 1 GB | SC overhead |
|---|---|---|---|
| 512 B | ~200 MB/s | 2,097,152 | ~400 ms |
| 4 KB | ~1,000 MB/s | 262,144 | ~50 ms |
| 64 KB | ~3,000 MB/s | 16,384 | ~3 ms |
| 1 MB | ~5,000 MB/s | 1,024 | ~200 µs |
| 8 MB | ~7,000 MB/s | 128 | ~25 µs |

---

## 19. Connections to Other Modules

**CSF-OS-101 M01 (Processes and Threads):** System calls are the interface between user-space processes (M01) and the kernel. File descriptors live in the process's fd table — a kernel data structure per process. `open()` creates an entry; `close()` removes it.

**CSF-OS-101 M03 (Virtual Memory and Page Cache):** `write()` writes to the page cache (M03). `mmap()` maps files into virtual address space (M03). `fsync()` flushes the page cache to disk (bridges M03 and M04). O_DIRECT bypasses the page cache (M03's page cache is not involved). The entire Section 7 (Direct I/O) is a direct application of M03's page cache concepts.

**CSF-OS-101 M05 (Signals and IPC):** Pipes (one-directional byte stream IPC between processes) use the same `read()`/`write()` interface as files — they are file descriptors in the kernel. The `epoll` in this module can monitor pipe fds as well as socket fds. Signals can interrupt `read()` and `write()` calls, returning `EINTR` — the correct handling of EINTR connects M04 and M05.

**DCS-KFK-101 (Kafka):** Kafka's entire I/O model — `write()` to page cache, `sendfile()` for zero-copy consumer reads, no per-message fsync, epoll-backed NIO for network handling — is a direct application of this module's concepts. Section 12.1 maps each Kafka I/O decision to the specific syscall that implements it.

**DCS-SPK-101 (Spark Architecture):** Spark's shuffle file management (write without fsync, atomic rename, external shuffle service using sendfile), file descriptor limits (`ulimit -n`), and the Spark shuffle server (epoll-backed Java NIO) are all explained by the concepts in this module.

---

## 20. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is a system call? | The controlled transition from user-mode (Ring 3) to kernel-mode (Ring 0) to request a privileged OS operation. Costs ~100–400 ns per call (mode switch + kernel dispatch). |
| 2 | What does open() return? | A file descriptor: a small non-negative integer indexing the process's kernel fd table. fd 0=stdin, 1=stdout, 2=stderr; user fds start at 3. |
| 3 | What flag makes open() writes thread-safe for concurrent appenders? | `O_APPEND` — each write() atomically seeks to end-of-file and writes, preventing interleaving between concurrent writers to the same file. |
| 4 | Does write() guarantee data is on disk? | No. write() copies data to the OS page cache (RAM) and returns. Data reaches disk asynchronously (up to 30s later). Use fsync() for durability guarantees. |
| 5 | What does fsync() do that write() doesn't? | fsync() flushes all dirty page cache pages for the file to the storage device AND issues a storage flush command, waiting until the device confirms durability. Costs 50µs (NVMe) to 15ms (HDD). |
| 6 | What is fdatasync() and when is it faster than fsync()? | fdatasync() flushes file data but not metadata (mtime, etc.). Faster for in-place overwrites where file size doesn't change. For appends (size changes), must still flush metadata. |
| 7 | What is O_DIRECT and why do databases use it? | O_DIRECT bypasses the page cache — data transfers directly between user buffer and storage device (requires aligned buffer/offset/count). Databases use it to manage their own buffer pools without OS double-caching. |
| 8 | What is O_APPEND and what race condition does it prevent? | O_APPEND makes each write() atomically seek-then-write to the end of the file. Without it, two processes could both read the same EOF offset and write at the same position, overwriting each other. |
| 9 | What is a short read and why must you handle it? | A read() that returns fewer bytes than requested (but > 0, not EOF). Occurs on pipes, sockets, NFS, at EOF. Always check return value and loop until all bytes are received. |
| 10 | What is the VFS? | Virtual File System — a kernel abstraction layer providing a uniform file I/O interface (open/read/write/close) across all file system implementations (ext4, XFS, NFS, FUSE, procfs, etc.). |
| 11 | What is a file descriptor limit and how does it affect Spark? | ulimit -n: max open fds per process (default 1024). Spark shuffle reads open one fd per shuffle block per map task. 500 partitions × 200 maps = 100,000 fds — requires ulimit -n 131072 or higher. |
| 12 | Why does select() not scale to 10,000 connections? | O(N) kernel scan per call: kernel checks all N fds for readiness every time select() is called, even if only 5 are active. Hard limit of 1024 fds (FD_SETSIZE). |
| 13 | How does epoll achieve O(1) per event instead of O(N)? | epoll maintains a red-black tree of registered fds. When a fd becomes ready, the kernel adds it to a "ready list." epoll_wait() returns only the ready list — O(K) where K = ready events, regardless of total registered fds. |
| 14 | What is sendfile() and how does Kafka use it? | sendfile(file_fd, socket_fd, offset, count) transfers data from page cache directly to socket via kernel DMA — no user-space copy. Kafka uses it for zero-copy consumer reads: log page cache → NIC directly. |
| 15 | What is the difference between level-triggered and edge-triggered epoll? | Level-triggered: epoll_wait returns the fd as ready on every call as long as data is available. Edge-triggered: returns only on transitions from not-ready to ready. ET requires draining the fd to EAGAIN; more efficient but error-prone. |
| 16 | What is io_uring and how does it differ from epoll? | io_uring uses shared memory ring buffers (SQ/CQ) between user and kernel. I/O requests are written to SQ (memory write, no syscall); completions are read from CQ (memory read, no syscall). Eliminates per-I/O syscall overhead. Works for regular files; epoll doesn't. |
| 17 | What syscall enables atomic file writes in Linux? | rename() (or renameat2()). Write to a temp file, fsync it, then rename to the final path. rename() is atomic — readers see either the old file or the new file, never a partial write. |
| 18 | What is pread() and why is it preferred for parallel Parquet reading? | pread(fd, buf, count, offset) reads from a specific offset without changing the file's shared position pointer. Thread-safe: multiple threads can call pread() concurrently on the same fd with different offsets. |
| 19 | Why does Kafka not use O_DIRECT? | Kafka relies on the OS page cache to serve consumer reads (via sendfile). O_DIRECT bypasses the page cache, preventing sendfile and eliminating the cache benefit. Kafka's design assumes consumers read recently-produced messages already in the page cache. |
| 20 | What does fallocate() do and why do log writers use it? | fallocate(fd, 0, 0, size) pre-allocates disk space for a file without writing data. Avoids fragmentation on HDD (extends the file in one seek), eliminates metadata updates during write, and prevents space exhaustion surprises mid-write. Kafka uses fallocate() for log segment pre-allocation. |

---

## 21. Module Summary

**System calls** are the controlled crossing from user-mode (Ring 3) to kernel-mode (Ring 0) — the only way processes access I/O, allocate memory, or interact with the OS. Each syscall costs ~100–400 ns. For high-frequency operations (file reads at 1 KB per call = 10 million calls for 10 GB), syscall overhead dominates. The fix is always to batch: large read/write buffers (8 MB), io_uring batch submission, or `sendfile()` for file-to-socket transfers.

The **VFS** provides a uniform file I/O interface across all file systems. Inodes (file metadata), dentries (name→inode), and the dentry/inode caches are the kernel structures behind fast path resolution. File descriptors are integers indexing the process's fd table; the default limit of 1024 is insufficient for Spark shuffle operations requiring tens of thousands of simultaneous open files.

`write()` writes to the **page cache** — not disk. `fsync()` flushes the page cache to storage and waits for device acknowledgment. The gap between `write()` and `fsync()` is the durability window — data in this gap is lost on power failure. Kafka accepts this risk for throughput (1M+ msg/s), relying on replication rather than per-message fsync. `fdatasync()` is faster by skipping metadata. `O_SYNC` is `fsync()` on every `write()` — use it only when per-write durability is required and throughput is not critical.

**`O_DIRECT`** bypasses the page cache for application-managed I/O (databases, benchmarks). Requires aligned buffers. Kafka does NOT use O_DIRECT because its design depends on the page cache for consumer zero-copy reads via `sendfile()`.

**`epoll`** scales to thousands of concurrent connections in O(K) per ready event (K = ready fd count), compared to `select()`'s O(N) per call. Kafka's network layer, Spark's shuffle server, and Python's `asyncio` all use epoll. Its limitations: doesn't work for regular files; still requires 2 syscalls per I/O. **`io_uring`** eliminates per-I/O syscalls via shared ring buffers and works for all fd types including regular files — the direction for future high-throughput data systems.

**Atomic writes** require: write → fsync → rename → directory fsync. The rename is atomic; the fsync ensures the data survives before the rename makes it visible. Delta Lake uses this pattern on local filesystems and conditional put (object store's native atomicity) on S3.

---

**CSF-OS-101: 4 of 5 complete.**  
**Next: CSF-OS-101 M05 — Signals and IPC**  
*(SIGTERM, SIGKILL, signal handlers, pipes, Unix domain sockets, shared memory — and how Airflow, Spark, and Kafka implement graceful shutdown)*
