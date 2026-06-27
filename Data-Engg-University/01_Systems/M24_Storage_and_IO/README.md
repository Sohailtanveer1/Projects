# SYS-LNX-101 M03: Storage and I/O

**Course:** SYS-LNX-101 — Linux for Data Engineers  
**Module:** 03 of 05  
**Filesystem position:** 01_Systems/M24_Storage_and_IO  
**Prerequisites:** SYS-LNX-101 M01 (Linux File System), SYS-LNX-101 M02 (Process Management)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: The Linux I/O Stack](#4-core-theory-the-linux-io-stack)
5. [lsblk — Block Device Topology](#5-lsblk--block-device-topology)
6. [mount — Filesystem Mounting](#6-mount--filesystem-mounting)
7. [iostat — I/O Performance Statistics](#7-iostat--io-performance-statistics)
8. [iotop — Per-Process I/O Monitor](#8-iotop--per-process-io-monitor)
9. [XFS vs ext4 — Filesystem Choice](#9-xfs-vs-ext4--filesystem-choice)
10. [Storage Configuration for Data Workloads](#10-storage-configuration-for-data-workloads)
11. [/proc and /sys Storage Interfaces](#11-proc-and-sys-storage-interfaces)
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

A Kafka broker that normally handles 500 MB/s of writes is suddenly achieving only 50 MB/s. The CPU is idle. The network has capacity. The JVM heap is fine. The bottleneck is somewhere in the storage stack. Without the right tools you have no way to tell whether the problem is a single hot disk being overloaded, a RAID controller with a failed cache battery falling back to write-through mode, a filesystem journal that's serializing concurrent writes, a mount option that was silently downgraded after a reboot, or an XFS log that's too small and causing contention under high write rates.

`iostat` tells you the throughput, IOPS, and latency of every block device on the system — updated in real time — and crucially the `%util` column that tells you when a device is saturated. `iotop` tells you which process is responsible for that I/O. `lsblk` shows you the device topology: how physical disks, partitions, RAID arrays, and logical volumes are assembled into the mountpoints the operating system uses. `mount` (and `/proc/mounts`) shows you exactly how each filesystem is mounted and what options are active.

Kafka, Spark, and HDFS are all I/O-intensive workloads. Kafka's write path is essentially a write-append to a log file — it achieves high throughput by relying on the OS page cache and sequential I/O, which are destroyed by fragmentation, wrong filesystem choice, or misconfigured mount options. Spark shuffle writes tens of thousands of small files; ext4 directory handling falls apart under that load while XFS scales. HDFS DataNodes write large sequential blocks where ext4 performs fine, but maintenance (`fsck`) on a 20 TB ext4 filesystem can take hours, while XFS metadata operations scale better.

This module gives you the tools and knowledge to diagnose, understand, and fix storage problems in production data systems.

---

## 2. How to Use This Module

**For incident response:** Start with Section 7 (iostat) to identify which device is saturated, then Section 8 (iotop) to identify which process is causing the I/O. Section 13 (Failure Scenarios) covers the most common production I/O emergencies.

**For infrastructure setup:** Section 9 (XFS vs ext4) and Section 10 (Storage Configuration) are the design reference for provisioning storage for Kafka, HDFS, or Spark shuffle. These sections translate directly into `mkfs` commands and `/etc/fstab` entries.

**For interview prep:** Sections 4, 9, 17, and 18. The I/O stack theory, filesystem comparison, and the diagnostic workflow chain are all common staff-level interview topics.

---

## 3. Prerequisites Check

- **Linux File System (SYS-LNX-101 M01):** This module builds on inodes, block devices, mount points, the page cache, and `fsync()` covered in M01. The page cache is central to understanding why mount options like `noatime` matter and how Kafka's sendfile optimization works.
- **File I/O and System Calls (CSF-OS-101 M04):** The `O_DIRECT`, `fsync()`, `sendfile()`, and `fallocate()` syscalls from M04 appear throughout this module as the mechanisms behind Kafka's I/O strategy.
- **Process Management (SYS-LNX-101 M02):** `/proc` and `/sys` filesystem reading from M02 extends to storage device interfaces in Section 11.

---

## 4. Core Theory: The Linux I/O Stack

### 4.1 Layers from Application to Disk

When a data engineering process writes data, it passes through a layered stack before reaching physical storage:

```
Application (Kafka, Spark, Python)
    │
    │  write(fd, buf, count)      ← system call
    ↓
VFS (Virtual Filesystem Switch)   ← unified interface for all filesystems
    │
    ↓
Filesystem (XFS / ext4)           ← translates file operations to block operations
    │                                allocates blocks, maintains metadata, journaling
    ↓
Page Cache                        ← kernel's write-back buffer
    │                                write() returns here; data is "dirty" in RAM
    │                                kernel flushes dirty pages to block layer async
    │                                OR fsync() forces immediate flush
    ↓
Block Layer (I/O Scheduler)       ← merges and reorders I/O requests for efficiency
    │                                CFQ (legacy) / mq-deadline / kyber / none
    ↓
Block Device Driver               ← talks to hardware: SATA/NVMe/SAS controller
    │
    ↓
Physical Storage (HDD / SSD / NVMe)
```

**Key insight:** `write()` returns after copying data to the **page cache** — not after writing to disk. The data is safe in DRAM and will be written eventually (dirty page writeback), but it is NOT durable until `fsync()` is called. This is why Kafka's default `acks=all` + replication provides durability without per-message `fsync()`: the data is in memory on multiple brokers, and a single server power failure won't lose it.

### 4.2 Block Devices vs Filesystems

A **block device** is a raw storage abstraction: a sequence of fixed-size blocks (typically 512 bytes or 4096 bytes). Block devices include `/dev/sda` (whole disk), `/dev/sda1` (partition), `/dev/md0` (software RAID), `/dev/mapper/data-lv0` (LVM logical volume).

A **filesystem** is a structure imposed on a block device that provides files, directories, and metadata. You format a block device with `mkfs.xfs` or `mkfs.ext4`, then `mount` it at a directory (the **mount point**). The OS then uses the filesystem's logic to translate file operations into block reads/writes.

```
Physical disk: /dev/nvme0n1       (NVMe SSD, 1 TB)
    └── Partition: /dev/nvme0n1p1 (1 TB partition)
        └── Filesystem: XFS       (mkfs.xfs /dev/nvme0n1p1)
            └── Mount point: /data/kafka  (mount /dev/nvme0n1p1 /data/kafka)
```

### 4.3 I/O Schedulers

The **I/O scheduler** in the block layer decides the order in which queued I/O requests are sent to the device. The right scheduler depends on the storage type:

| Scheduler | Description | Best for |
|---|---|---|
| `none` (noop) | First-in, first-out. No reordering. | NVMe/SSDs with their own internal queuing. NVMe controllers handle optimization internally — adding OS scheduler overhead is wasteful. |
| `mq-deadline` | Sets deadlines on requests to prevent starvation; lightweight merge. | SSDs, general purpose. Good default for most production systems. |
| `bfq` | Budget Fair Queueing. Per-process fairness and bandwidth isolation. | Shared systems where I/O fairness between users matters. Latency-sensitive interactive workloads. |
| `kyber` | Low-overhead, targets latency. Separate queues for reads and writes. | SSDs, NVMe in mixed read/write workloads. |

```bash
# Check current scheduler for a device
cat /sys/block/sda/queue/scheduler
# [mq-deadline] kyber bfq none
# The active scheduler is in brackets.

# Check for NVMe (should be 'none' — let the NVMe controller handle it)
cat /sys/block/nvme0n1/queue/scheduler
# [none]

# Change scheduler (immediate, not persistent)
echo mq-deadline > /sys/block/sda/queue/scheduler

# Persistent: add to /etc/udev/rules.d/60-scheduler.rules
# ACTION=="add|change", KERNEL=="sd[a-z]", ATTR{queue/rotational}=="1", \
#   ATTR{queue/scheduler}="mq-deadline"
# ACTION=="add|change", KERNEL=="nvme[0-9]n[0-9]", \
#   ATTR{queue/scheduler}="none"

# Check if device is rotational (1=HDD, 0=SSD/NVMe)
cat /sys/block/sda/queue/rotational
# 1   ← HDD
cat /sys/block/nvme0n1/queue/rotational
# 0   ← SSD/NVMe
```

### 4.4 Read-Ahead

The kernel prefetches data from disk ahead of sequential reads (read-ahead) to hide I/O latency. The default is 128 KB (256 × 512-byte sectors). For large sequential scans (Spark reading Parquet), larger read-ahead improves throughput. For random-access workloads (database random reads), large read-ahead wastes I/O bandwidth.

```bash
# View current read-ahead in 512-byte sectors
cat /sys/block/sda/queue/read_ahead_kb
# 128

# Increase for sequential scan workloads (e.g., HDFS DataNode)
blockdev --setra 4096 /dev/sda    # set to 4096 × 512 bytes = 2 MB read-ahead
echo 4096 > /sys/block/sda/queue/read_ahead_kb

# Or via hdparm (persistent across reboots with proper udev rule)
hdparm -a 4096 /dev/sda
```

---

## 5. lsblk — Block Device Topology

`lsblk` (list block devices) shows the hierarchical structure of block devices — from physical disks through partitions, RAID arrays, LVM, and down to filesystems and mount points.

### 5.1 Basic lsblk Usage

```bash
lsblk
# NAME          MAJ:MIN  RM   SIZE  RO  TYPE  MOUNTPOINT
# sda             8:0     0  800G   0  disk
# ├─sda1          8:1     0    1G   0  part  /boot
# └─sda2          8:2     0  799G   0  part
#   └─data-root 253:0     0  799G   0  lvm   /
# nvme0n1       259:0     0   1.8T  0  disk
# ├─nvme0n1p1   259:1     0    1G   0  part
# └─nvme0n1p2   259:2     0   1.8T  0  part
#   ├─kafka-log0 253:1    0  900G   0  lvm   /data/kafka/log0
#   └─kafka-log1 253:2    0  900G   0  lvm   /data/kafka/log1
# sdb             8:16    0   800G  0  disk
# └─md0           9:0     0  1.6T  0  raid0
#   └─spark-shuf 253:3    0  1.6T  0  lvm   /data/spark/shuffle

# Column meanings:
# NAME:       device name (sda, sda1, nvme0n1, md0, dm-0...)
# MAJ:MIN:    major:minor device numbers (kernel identifiers)
# RM:         removable (1=yes, 0=no)
# SIZE:       device size
# RO:         read-only (1=yes, 0=no)
# TYPE:       disk, part (partition), lvm, raid0/1/5, loop, rom
# MOUNTPOINT: where mounted (blank if not mounted)
```

### 5.2 Richer lsblk Output

```bash
# With filesystem type and UUID
lsblk -f
# NAME         FSTYPE   LABEL  UUID                                 MOUNTPOINT
# sda
# ├─sda1       ext4            a1b2c3d4-1111-2222-3333-444455556666  /boot
# └─sda2
#   └─data-root xfs             b2c3d4e5-aaaa-bbbb-cccc-ddddeeee1111  /
# nvme0n1
# └─nvme0n1p1  xfs             c3d4e5f6-0000-1111-2222-333344445555  /data/kafka

# With sizes in bytes (for scripting)
lsblk -b

# Full detail: all columns
lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT,LABEL,UUID,MODEL,SERIAL,ROTA,RQ-SIZE,MIN-IO,OPT-IO,PHY-SEC,LOG-SEC,SCHED,RO,RM

# Key additional columns:
# MODEL:    disk model (e.g., "Samsung SSD 970 PRO")
# SERIAL:   disk serial number (for RMA)
# ROTA:     rotational (1=HDD, 0=SSD)
# RQ-SIZE:  I/O request queue depth
# PHY-SEC:  physical sector size (4096 for Advanced Format drives)
# LOG-SEC:  logical sector size (512 or 4096)
# SCHED:    I/O scheduler (mq-deadline, none, etc.)

# Check disk model and rotation for all disks
lsblk -o NAME,MODEL,ROTA,SIZE,TYPE | grep disk

# Find the device backing a mount point
lsblk -o NAME,MOUNTPOINT | grep "/data/kafka"
```

### 5.3 Understanding LVM and RAID in lsblk

```bash
# Common data engineering storage topologies:

# Pattern 1: Single NVMe for Kafka (simple, fastest)
# nvme0n1 (1.8 TB NVMe)
# └── nvme0n1p1 (XFS) → mounted at /data/kafka

# Pattern 2: JBOD (Just a Bunch Of Disks) for Kafka (multiple independent disks)
# sda → /data/kafka/disk0   (separate mount per disk)
# sdb → /data/kafka/disk1   (Kafka distributes logs across dirs)
# sdc → /data/kafka/disk2
# Kafka config: log.dirs=/data/kafka/disk0,/data/kafka/disk1,/data/kafka/disk2

# Pattern 3: RAID-0 for Spark shuffle (maximum throughput, no redundancy needed)
# sda + sdb → md0 (RAID-0, 2×write bandwidth)
# md0 → XFS → /data/spark/shuffle

# Check RAID status
cat /proc/mdstat
# Personalities : [raid0] [raid1] [raid5]
# md0 : active raid0 sda[0] sdb[1]
#       1677721600 blocks super 1.2 512k chunks
# md1 : active raid1 sdc[0] sdd[1]
#       976773120 blocks super 1.2 [2/2] [UU]
#       ← UU = both disks OK; U_ = one disk failed

# LVM commands
pvs    # physical volumes (disks/partitions backing LVM)
vgs    # volume groups
lvs    # logical volumes
lvdisplay /dev/kafka/log0   # detailed LV info
```

---

## 6. mount — Filesystem Mounting

### 6.1 Viewing Current Mounts

```bash
# Show all mounted filesystems (human-readable)
mount
# sysfs on /sys type sysfs (rw,nosuid,nodev,noexec,relatime)
# /dev/nvme0n1p1 on /data/kafka type xfs (rw,noatime,nodiratime,nobarrier,logbufs=8)
# /dev/sda1 on /data/hdfs type xfs (rw,noatime,nodiratime,allocsize=64m)
# tmpfs on /tmp type tmpfs (rw,nosuid,nodev,size=8G)

# More parseable: /proc/mounts (always accurate, even for bind mounts)
cat /proc/mounts
# Same format but comes directly from the kernel

# findmnt: the modern, structured alternative
findmnt
findmnt /data/kafka
# TARGET       SOURCE        FSTYPE  OPTIONS
# /data/kafka  /dev/nvme0n1p1 xfs    rw,noatime,nodiratime,nobarrier

findmnt --df   # show disk usage per mount
findmnt -t xfs # show only XFS mounts
```

### 6.2 Mount Options

Mount options are critical for performance. The wrong options are silent — `mount` succeeds but the filesystem performs 2-3× slower than it should.

```bash
# General options:
# rw / ro        — read-write or read-only
# noatime        — don't update atime on reads (eliminates per-read metadata write)
# nodiratime     — don't update atime for directory reads
# relatime       — update atime only if mtime is newer (compromise; still causes writes)
# noexec         — don't allow execution of binaries (security)
# nosuid         — don't honor setuid/setgid bits (security)
# nodev          — don't allow special device files (security)
# sync           — ALL writes are synchronous (write to disk before returning from write())
#                  EXTREME performance penalty; use only for boot/tiny filesystems
# async          — writes go through page cache (default, correct for almost everything)

# ext4-specific options:
# data=writeback   — fastest: metadata journaled, data NOT journaled; risk of stale data on crash
# data=ordered     — default: ensures data written to disk before metadata journal commit
# data=journal     — safest: both metadata and data journaled; ~2× slower
# barrier=0        — disable write barriers (DANGEROUS without battery-backed cache)
# nobarrier        — same as barrier=0; disables fsync barriers to disk cache
# commit=N         — fsync interval in seconds (default 5); higher = faster, more data at risk
# journal_async_commit — allow async journal commits (slightly less durable but faster)

# XFS-specific options:
# nobarrier        — disable write barriers (same warning as ext4)
# logbufs=N        — number of log buffers (default 8; increase for high write throughput)
# logbsize=N       — log buffer size (default 32k; increase to 256k for large writes)
# allocsize=N      — speculative preallocation size (default 64k; increase for large files)
#                    kafka: allocsize=1g pre-allocates 1 GB for new log segments
# largeio          — prefer large I/O sizes (good for sequential workloads)
# inode64          — place inodes anywhere on disk (not just first 1 TB; required for large volumes)
# norecovery       — mount without replaying log (emergency read-only recovery only)

# tmpfs options:
# size=N           — max size (e.g., size=8G, size=50%)
# mode=1777        — sticky world-writable (like /tmp)
```

### 6.3 Persistent Mounts with /etc/fstab

```bash
# /etc/fstab format:
# <device>           <mountpoint>      <fstype>  <options>                     <dump> <pass>

# Example: Kafka log directory on NVMe
UUID=c3d4e5f6-0000-1111-2222-333344445555  /data/kafka  xfs  noatime,nodiratime,nobarrier,logbufs=8,allocsize=1g  0  2

# Example: HDFS DataNode on HDD (sequential, large files)
UUID=d4e5f6a7-1111-2222-3333-444455556666  /data/hdfs   xfs  noatime,nodiratime,nobarrier,allocsize=64m,largeio   0  2

# Example: Spark shuffle on RAID-0 (throughput priority, no persistence needed)
/dev/md0  /data/spark/shuffle  xfs  noatime,nodiratime,nobarrier  0  0

# <dump> field: 0 = don't dump (backup utility), almost always 0
# <pass> field: fsck order at boot. 0=skip, 1=root (check first), 2=others (check after root)
#   For data disks: use 2, not 0, so fsck catches corruption after unclean shutdown

# Use UUID (not /dev/sdX) because /dev/sda can change if you add disks.
# Get UUID:
blkid /dev/nvme0n1p1
# /dev/nvme0n1p1: UUID="c3d4e5f6-..." TYPE="xfs"
lsblk -f | grep nvme0n1p1
```

### 6.4 Remounting and Emergency Operations

```bash
# Remount with new options (without unmounting — for running filesystems)
mount -o remount,noatime /data/kafka
# Useful for applying a missed performance option without service restart

# Mount a new disk immediately (before fstab entry)
mount /dev/nvme0n1p1 /data/kafka

# Lazy unmount (detach from filesystem namespace; complete when last fd is closed)
umount -l /data/kafka
# Useful when a process has a file open and you need to unmount gracefully

# Force unmount (use only for stuck NFS/FUSE mounts)
umount -f /mnt/nfs-share

# Mount all fstab entries (after editing fstab)
mount -a

# Verify fstab syntax without mounting
mount --fake -a -v    # -fake: don't actually mount; just validate
```

---

## 7. iostat — I/O Performance Statistics

`iostat` is the primary tool for measuring block device throughput, IOPS, and utilization. It comes from the `sysstat` package.

### 7.1 Basic iostat Usage

```bash
# Install
apt-get install sysstat    # Debian/Ubuntu
yum install sysstat        # RHEL/CentOS

# One-shot summary since boot (less useful, averages over entire uptime)
iostat

# REAL-TIME: update every 2 seconds, extended statistics
iostat -x 2
# ^ This is the production command for I/O diagnosis.
# -x: extended statistics (adds utilization, queue depth, service time, wait time)
# 2: refresh every 2 seconds

# Device and partition statistics
iostat -x -p sda 2        # one device
iostat -x -p ALL 2        # all devices including partitions

# Human-readable sizes (MB/s instead of KB/s)
iostat -xm 2              # -m: megabytes

# Compact: only show devices with activity
iostat -x 2 | grep -v "^$" | grep -v "Device"
```

### 7.2 Reading iostat -x Output

```
Linux 5.15.0 (data-server-01)  06/27/2026  _x86_64_  (32 CPU)

avg-cpu: %user  %nice  %system  %iowait  %steal   %idle
          12.45   0.00     1.23     8.34    0.00   78.98

Device     r/s    w/s    rkB/s   wkB/s  rrqm/s  wrqm/s  %rrqm  %wrqm  r_await  w_await  aqu-sz  rareq-sz  wareq-sz  svctm  %util
nvme0n1   45.2  892.3    720.4 114214.3     0.0    45.6    0.0   4.9      0.12     0.48    0.43    15.94    128.00   0.47   43.6
sda        2.3   12.4     18.4    198.1     0.0     1.2    0.0   8.8      1.23    12.45    0.16     8.00     16.00   5.61    8.2
sdb        0.1    0.0      0.8      0.0     0.0     0.0    0.0   0.0      2.10     0.00    0.00     8.00      0.00   1.20    0.01
```

**Column-by-column explanation:**

```
r/s        — reads per second (IOPS: read)
w/s        — writes per second (IOPS: write)
rkB/s      — read throughput in KB/s
wkB/s      — write throughput in KB/s
rrqm/s     — read requests merged per second (kernel merged adjacent reads into one)
wrqm/s     — write requests merged per second
%rrqm      — percent of read requests that were merged (higher = more sequential)
%wrqm      — percent of write requests that were merged (higher = more sequential)
r_await    — average time (ms) from request submission to completion for READS
             = wait in queue + device service time
w_await    — same for WRITES
aqu-sz     — average I/O queue size (requests waiting + being served)
             > 1 means requests are queuing (possible saturation)
rareq-sz   — average read request size in KB
wareq-sz   — average write request size in KB
             small wareq-sz = many small writes = random I/O pattern
             large wareq-sz = few large writes = sequential I/O pattern (good for HDD/Kafka)
svctm      — DEPRECATED. Ignore. Was "service time" but never accurate for SSDs.
%util      — percentage of time the device was busy (at least one request in service)
             < 70%: healthy headroom
             70-90%: moderate load
             > 90%: approaching saturation (latency starts increasing)
             100%: saturated (all requests are queuing)
             NOTE: for NVMe with internal parallelism, %util can be 100% while
             throughput is still well below maximum — check aqu-sz and await instead
```

**Reading the example output:**

```
nvme0n1: 43.6% utilized, writing 114 MB/s at 892 w/s with 0.48ms write latency
  → Healthy NVMe under moderate Kafka write load.
  → wareq-sz = 128 KB: Kafka is writing in large sequential chunks (good).
  → aqu-sz = 0.43: slight queuing but not saturated.

sda: 8.2% utilized but w_await = 12.45ms
  → HDD, low utilization but high latency per write.
  → wareq-sz = 16 KB: small writes → random I/O pattern on an HDD → high latency.
  → This is suspicious for an application that should be writing sequentially.
```

### 7.3 Diagnosing with iostat

```bash
# Pattern 1: Check if a specific device is saturated
iostat -xm 1 5 | grep nvme0n1
# Watch for: %util approaching 100%, aqu-sz > 2, await growing

# Pattern 2: Identify random vs sequential I/O
iostat -x 5 | awk 'NR>3 {printf "%s: wareq=%.1fKB (%s)\n", $1, $NF*1024, ($NF < 32 ? "RANDOM" : "SEQUENTIAL")}'
# Small wareq-sz (<32KB) = random I/O = bad for HDDs, bad for Kafka log writes

# Pattern 3: Check I/O wait on CPU
iostat -x 5 | awk '/avg-cpu/ {getline; print "iowait:", $4"%"}'
# iowait > 5% means CPUs are frequently waiting for I/O to complete

# Pattern 4: Watch a Kafka write burst
iostat -xm 1 | grep -E "Device|nvme"
# Should show wkB/s jumping during produce bursts, %util staying below 70%

# Pattern 5: Baseline comparison (capture 60 seconds of metrics)
iostat -xm 1 60 > /tmp/iostat_baseline.txt
# Compare during incident vs baseline to identify what changed

# Device throughput limits (theoretical maximums for reference):
# SATA HDD:     ~150 MB/s sequential read, ~100-120 MB/s sequential write, ~150 IOPS random
# SATA SSD:     ~550 MB/s sequential, ~10,000-50,000 IOPS random
# NVMe (PCIe 3): ~3,500 MB/s sequential read, ~3,000 MB/s write, ~500,000 IOPS random
# NVMe (PCIe 4): ~7,000 MB/s sequential read, ~6,500 MB/s write, ~1,000,000 IOPS random
```

---

## 8. iotop — Per-Process I/O Monitor

`iotop` shows real-time I/O statistics per process — like `top` but for disk I/O. It requires root or `CAP_NET_ADMIN`.

### 8.1 Basic iotop Usage

```bash
# Install
apt-get install iotop

# Interactive mode (most useful)
iotop
# Total DISK READ:       0.00 B/s | Total DISK WRITE:     112.45 M/s
# Current DISK READ:     0.00 B/s | Current DISK WRITE:   108.32 M/s
#   TID  PRIO  USER     DISK READ   DISK WRITE  SWAPIN     IO>    COMMAND
#  1234  be/4  kafka        0.00 B  98.23 M/s    0.00 %  12.34 %  java -Xmx4g kafka.Kafka
#  2345  be/4  spark        0.00 B  14.22 M/s    0.00 %   2.11 %  java -Xmx8g CoarseGrainedExec
#  3456  be/4  root         0.00 B   0.00 B       0.00 %   0.00 %  kworker/u64:4

# Non-interactive (for logging/alerting)
iotop -b -o -n 5 -d 2
# -b: batch mode (non-interactive, suitable for piping/logging)
# -o: only show processes currently doing I/O (hides idle ones)
# -n 5: run 5 iterations
# -d 2: 2-second interval

# Sort by I/O (default is by I/O %)
iotop -b -o -d 2 | head -20

# Show only a specific PID
iotop -p 1234

# Per-process accumulated totals (not rate)
iotop -a   # accumulated totals since iotop started
```

### 8.2 Reading iotop Output

```
TID   — Thread ID (Linux thread, not PID; for single-threaded processes TID=PID)
PRIO  — I/O priority class and level
        be/4 = Best Effort, level 4 (default)
        rt/0 = Real Time, level 0 (highest)
        idle = Idle class (only gets I/O when disk is free)
USER  — owning user
DISK READ  — current read throughput for this process
DISK WRITE — current write throughput for this process (from write() calls)
             Note: actual disk writes may be delayed (page cache writeback)
             iotop shows what the process is writing, not what's hitting disk
SWAPIN     — percent time process is waiting for swap reads (0 = no swap pressure)
IO>        — percent time process is blocked waiting for I/O (actual I/O wait)
             High IO> = this process is I/O bound
COMMAND    — process name
```

### 8.3 iotop for Data Engineering Diagnosis

```bash
# Scenario: iostat shows nvme0n1 at 95% util, but you don't know which process
# Step 1: identify the I/O culprit
sudo iotop -b -o -d 1 -n 10 | grep -v kworker | head -30

# Scenario: Airflow task is slow; suspect excessive small file writes
# Run iotop with accumulation to see total bytes written
sudo iotop -a -b -n 60 -d 1 -p $(pgrep -f "airflow tasks run") 2>/dev/null

# Scenario: identify which Spark task is writing shuffle data
sudo iotop -b -o -d 2 | grep java | head -5
# Then correlate PID with the Spark UI to find the specific stage/task

# iotop + strace for ultimate I/O diagnosis: find exactly which files are being written
PID=$(sudo iotop -b -o -n 3 -d 1 | grep java | head -1 | awk '{print $1}')
sudo strace -f -e trace=write,pwrite64,writev -p $PID -o /tmp/writes.txt 2>&1 &
sleep 5
kill %1
# Then:
grep "write(" /tmp/writes.txt | awk '{print length($0), $0}' | sort -rn | head -20
# Shows the largest writes — which usually reveals the pattern (small vs large writes)

# Monitor continuous I/O to a specific directory:
watch -n 1 'lsof +D /data/kafka | grep -v lsof | awk "{print \$2, \$1, \$7, \$9}" | sort | uniq -c | sort -rn | head -20'
```

---

## 9. XFS vs ext4 — Filesystem Choice

### 9.1 Design Philosophy

**ext4** is the evolution of the traditional Linux ext family. It uses a **block group** structure dividing the disk into fixed-size regions, each containing inodes and data blocks. ext4 favors simplicity and is the default on many Linux distributions. It is mature, well-tested, and has predictable behavior.

**XFS** was designed by SGI in 1993 for high-performance workloads. It uses **B-tree based** data structures throughout: B-tree for free space, B-tree for extent records, B-tree for directory entries. This gives XFS logarithmic lookup time for all operations regardless of scale — while ext4's linear structures degrade as the filesystem grows large.

### 9.2 Structural Comparison

| Feature | ext4 | XFS |
|---|---|---|
| Max filesystem size | 1 EiB (effective ~16 TB with standard tools) | 8 EiB |
| Max file size | 16 TiB | 8 EiB |
| Max file count | Limited by inode count (set at mkfs time) | Dynamic; limited by free space |
| Block allocation | Multiblock allocator with delayed allocation | Extent-based with speculative preallocation |
| Directory structure | HTree (hash tree, scales to ~10M entries) | B-tree (scales to billions of entries) |
| Metadata journaling | Journal (separate journal device supported) | Log (circular log on the filesystem) |
| Online resize | Grow only (cannot shrink) | Grow only (cannot shrink) |
| Online defragmentation | Limited (`e4defrag`) | Yes (`xfs_fsr`) |
| Checksums | Metadata only (ext4 with metadata_csum) | Both metadata and data (with `-n crc=1`, default since XFS v5) |
| fsck time | O(filesystem size) — hours for 20 TB | O(metadata size) — minutes even for 20 TB |
| Freeze/thaw for snapshots | `fsfreeze -f/-u` | `xfs_freeze -f/-u` |

### 9.3 Performance Characteristics

**Large file sequential I/O (Kafka, HDFS blocks):**
Both perform comparably for pure sequential writes. XFS's speculative preallocation (`allocsize` mount option) reduces fragmentation by pre-allocating disk space when a file is opened for writing — important for Kafka log segments that grow over time.

**Small file creation (Spark shuffle output, Hive partitioning):**
XFS outperforms ext4 significantly. Spark shuffle writes thousands of small files per task. ext4 directory operations degrade with many entries; XFS's B-tree directory structure maintains O(log n) performance. Benchmark: creating 100,000 files in one directory takes ~60s on ext4, ~8s on XFS.

**Parallel I/O (multiple Kafka partitions, concurrent Spark tasks):**
XFS uses per-allocation group locking. Each XFS filesystem is divided into allocation groups (AGS), and concurrent file operations in different AGs proceed independently. ext4 has a single allocation lock, which serializes concurrent allocation requests — a bottleneck when multiple Kafka partitions simultaneously open new log segments.

**fsck (repair after unclean shutdown):**
XFS's journal-based recovery replays only uncommitted transactions — typically seconds regardless of filesystem size. ext4's `e2fsck` scans the entire filesystem structure — on a 20 TB volume this can take 4+ hours, blocking the service from mounting.

**Truncation and deletion:**
XFS truncation is instantaneous from the application's perspective (the inode is updated, and the extent tree entries are removed asynchronously in the background). ext4's free block accounting updates can hold locks longer. For Kafka log retention (frequent segment deletion), XFS's background truncation is a meaningful advantage.

### 9.4 Making Filesystems

```bash
# ── XFS ──────────────────────────────────────────────────────────────────────

# Basic XFS filesystem (good defaults for most workloads)
mkfs.xfs /dev/nvme0n1p1

# Kafka-optimized XFS:
mkfs.xfs \
  -b size=4096 \          # 4 KB blocks (default; matches most OS page sizes)
  -d agcount=32 \         # 32 allocation groups (= 32 parallel allocation paths)
                          # Rule of thumb: min(threads, disk_size_GB/4)
  -l size=256m,version=2 \ # 256 MB internal log (default 10 MB is often too small for write bursts)
  -n ftype=1 \            # store file type in directory entries (enables faster lookups)
  /dev/nvme0n1p1

# HDFS DataNode XFS (large sequential writes):
mkfs.xfs \
  -b size=4096 \
  -d agcount=8 \          # fewer AGs: HDFS writes large sequential blocks, not many small files
  -l size=128m,version=2 \
  /dev/sda1

# ── ext4 ──────────────────────────────────────────────────────────────────────

# Basic ext4
mkfs.ext4 /dev/sda1

# Tune inode count (critical for small-file workloads)
# Default: 1 inode per 16 KB of space (~6.25% overhead)
# For Hive/Spark small-file workloads, reduce bytes-per-inode:
mkfs.ext4 \
  -i 4096 \               # 1 inode per 4 KB: 4× more inodes than default
  -b 4096 \               # 4 KB blocks
  -E lazy_itable_init=0 \ # initialize inode table at mkfs (slower mkfs but no runtime overhead)
  /dev/sda1

# Disable journal (for temp/scratch volumes where data loss is OK on crash)
mkfs.ext4 -O ^has_journal /dev/sdb1
# Or disable journal on existing filesystem:
tune2fs -O ^has_journal /dev/sdb1
e2fsck -f /dev/sdb1    # fsck required after removing journal

# Check filesystem parameters
xfs_info /dev/nvme0n1p1      # XFS
tune2fs -l /dev/sda1          # ext4
```

### 9.5 Decision Matrix

```
Use XFS when:
  ✓ Kafka broker log directories (frequent large sequential writes + concurrent partition appends)
  ✓ HDFS DataNode data directories (large files; fast fsck critical for recovery time)
  ✓ Spark shuffle directories (many small files, concurrent writes from many tasks)
  ✓ Filesystem > 1 TB
  ✓ High concurrency (many processes writing to the same filesystem simultaneously)
  ✓ You need predictable fsck time (SLA for recovery)
  ✓ RHEL/CentOS environment (XFS is the default; better support/tooling)

Use ext4 when:
  ✓ Boot filesystem, system volumes (mature, widely supported)
  ✓ Single-writer sequential workloads (ext4 competitive; simpler tooling)
  ✓ Ubuntu/Debian environments where ext4 is the default
  ✓ Small volumes (<100 GB) where scale doesn't matter
  ✓ You need to shrink the filesystem (XFS cannot shrink; ext4 can with resize2fs)

Avoid for data engineering:
  ✗ btrfs: copy-on-write overhead hurts write-heavy workloads; Kafka explicitly warns against it
  ✗ NFS for Kafka/Spark shuffle: network filesystem latency + D-state risk (see M02)
  ✗ FAT32/exFAT: no permissions, no journaling, 4 GB file size limit
```

---

## 10. Storage Configuration for Data Workloads

### 10.1 Kafka Storage Configuration

```bash
# 1. Format the disk
mkfs.xfs -b size=4096 -d agcount=32 -l size=256m,version=2 /dev/nvme0n1p1

# 2. Mount with performance options
mkdir -p /data/kafka
mount -o noatime,nodiratime,nobarrier,logbufs=8,allocsize=1g /dev/nvme0n1p1 /data/kafka

# 3. Add to /etc/fstab (persistent)
echo "UUID=$(blkid -s UUID -o value /dev/nvme0n1p1) /data/kafka xfs noatime,nodiratime,nobarrier,logbufs=8,allocsize=1g 0 2" >> /etc/fstab

# 4. Set ownership
chown -R kafka:kafka /data/kafka
chmod 755 /data/kafka

# 5. Configure Kafka to use this directory
# In server.properties:
# log.dirs=/data/kafka/logs
# log.segment.bytes=1073741824      # 1 GB segments (matches allocsize=1g)
# log.retention.bytes=107374182400  # 100 GB retention per partition

# 6. Verify settings
cat /proc/mounts | grep kafka
# /dev/nvme0n1p1 /data/kafka xfs rw,noatime,nodiratime,nobarrier,logbufs=8,allocsize=1g 0 0

# 7. Performance test before going live
fio --name=kafka_write_test \
    --filename=/data/kafka/fio_test \
    --rw=write \
    --bs=1m \
    --size=4g \
    --numjobs=4 \
    --iodepth=8 \
    --time_based \
    --runtime=30 \
    --group_reporting \
    --output-format=normal
# Remove test file after:
rm /data/kafka/fio_test
```

### 10.2 HDFS DataNode Storage

```bash
# HDFS writes 128 MB blocks sequentially; up to N concurrent write streams
# Prefer multiple independent disks (JBOD) over RAID for HDFS
# (HDFS provides its own replication; RAID wastes I/O bandwidth on parity)

# Format each disk
for dev in /dev/sdb /dev/sdc /dev/sdd /dev/sde; do
    mkfs.xfs -b size=4096 -d agcount=8 -l size=128m,version=2 $dev
done

# Mount each disk at a separate path
for i in 0 1 2 3; do
    mkdir -p /data/hdfs/disk${i}
    dev=$(ls /dev/sd[b-e] | sed -n "$((i+1))p")
    UUID=$(blkid -s UUID -o value $dev)
    echo "UUID=$UUID /data/hdfs/disk${i} xfs noatime,nodiratime,nobarrier,allocsize=64m,largeio 0 2" >> /etc/fstab
done
mount -a

# HDFS config (hdfs-site.xml):
# dfs.datanode.data.dir = /data/hdfs/disk0,/data/hdfs/disk1,/data/hdfs/disk2,/data/hdfs/disk3
# dfs.block.size = 134217728      # 128 MB blocks
# dfs.datanode.max.transfer.threads = 4096

# Verify disk layout
lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINT,ROTA | grep -E "sdb|sdc|sdd|sde"
```

### 10.3 Spark Shuffle Storage

```bash
# Spark shuffle: many small files, high concurrent write/read, disposable on job completion
# Use RAID-0 for maximum throughput (redundancy not needed; Spark retries on failure)

# Create RAID-0 array from 2 SSDs
mdadm --create /dev/md0 --level=0 --raid-devices=2 /dev/sdb /dev/sdc
# Save RAID config
mdadm --detail --scan >> /etc/mdadm/mdadm.conf
update-initramfs -u

# Format and mount
mkfs.xfs -b size=4096 -d agcount=32 /dev/md0
mkdir -p /data/spark/shuffle
echo "$(blkid -s UUID -o value /dev/md0) /data/spark/shuffle xfs noatime,nodiratime,nobarrier 0 0" >> /etc/fstab
# Note: pass field = 0 for shuffle: no fsck needed (data is disposable)
mount -a

# Spark config (spark-defaults.conf):
# spark.local.dir = /data/spark/shuffle
# spark.shuffle.compress = true        # compress shuffle data (CPU trade for I/O)
# spark.shuffle.spill.compress = true
```

---

## 11. /proc and /sys Storage Interfaces

```bash
# ── Block device parameters via /sys/block/<dev>/queue/ ──────────────────────

# Key tunables:
cat /sys/block/nvme0n1/queue/scheduler       # I/O scheduler
cat /sys/block/nvme0n1/queue/rotational      # 0=SSD, 1=HDD
cat /sys/block/nvme0n1/queue/read_ahead_kb   # read-ahead in KB
cat /sys/block/nvme0n1/queue/nr_requests     # I/O queue depth (default 256)
cat /sys/block/nvme0n1/queue/max_sectors_kb  # max single I/O size
cat /sys/block/nvme0n1/queue/physical_block_size  # physical sector size
cat /sys/block/nvme0n1/queue/logical_block_size   # logical sector size

# Set queue depth (more depth = more parallelism; diminishing returns after ~64 for SSDs)
echo 128 > /sys/block/nvme0n1/queue/nr_requests

# ── Disk I/O statistics via /proc/diskstats ──────────────────────────────────
cat /proc/diskstats
# 259       0 nvme0n1 14523 0 2302096 4120 1234567 567890 345678901 23456 0 12340 27576
# Fields:  major minor name  reads_completed reads_merged sectors_read time_reading
#          writes_completed writes_merged sectors_written time_writing
#          ios_in_progress total_time_ios weighted_io_time

# Compute throughput from /proc/diskstats (what iostat uses internally):
# Delta sectors_read × 512 bytes / elapsed_seconds = read throughput
# Delta sectors_written × 512 bytes / elapsed_seconds = write throughput

# ── Global I/O statistics via /proc/vmstat ──────────────────────────────────
grep -E "pgpgin|pgpgout|pswpin|pswpout|nr_dirty|nr_writeback" /proc/vmstat
# pgpgin:     pages read from disk (into page cache)
# pgpgout:    pages written to disk (from page cache)
# pswpin:     pages swapped in (non-zero = swap is being used!)
# pswpout:    pages swapped out
# nr_dirty:   pages in page cache waiting to be written to disk
# nr_writeback: pages currently being written to disk

# ── Dirty page thresholds (control page cache writeback aggressiveness) ──────
cat /proc/sys/vm/dirty_ratio        # 20: start synchronous writes when 20% of RAM is dirty
cat /proc/sys/vm/dirty_background_ratio  # 10: start background writeback at 10% dirty
cat /proc/sys/vm/dirty_writeback_centisecs  # 500: flush dirty pages every 5 seconds
cat /proc/sys/vm/dirty_expire_centisecs     # 3000: data older than 30s gets flushed

# Kafka performance tuning: reduce dirty_ratio to limit write spikes
# (default 20% of 64GB RAM = 12.8 GB of dirty data before blocking applications)
sysctl -w vm.dirty_ratio=10           # block at 6.4 GB dirty
sysctl -w vm.dirty_background_ratio=5  # background flush at 3.2 GB dirty
# Make persistent:
echo "vm.dirty_ratio=10" >> /etc/sysctl.d/60-kafka.conf
echo "vm.dirty_background_ratio=5" >> /etc/sysctl.d/60-kafka.conf

# ── Filesystem usage ──────────────────────────────────────────────────────────
df -h         # disk space (human readable)
df -i         # inode usage
df -hT        # include filesystem type
df --output=source,target,fstype,size,used,avail,pcent | column -t

# Per-directory usage
du -sh /data/kafka/logs/*       # size of each Kafka log directory
du -sh /data/kafka/logs/topic-0 # size of one topic
du --max-depth=2 /data/ | sort -rh | head -20  # find largest directories
```

---

## 12. Mental Models

### 12.1 The Page Cache as a Write Buffer

When Kafka calls `write(fd, data, size)`, the data goes into the **page cache** — an in-kernel ring buffer that accumulates dirty pages. The call returns immediately. The kernel's `pdflush`/`flush` threads periodically flush dirty pages to disk based on `dirty_background_ratio` and `dirty_expire_centisecs`. This is why Kafka can achieve 500 MB/s write throughput on a 500 MB/s disk — writes from the application and writes to disk proceed in parallel, batched by the page cache.

Think of the page cache as a sink at a restaurant: the kitchen (application) pours dirty water (data) into the sink; the drain (disk) carries it away at its own pace; the sink overflows (dirty_ratio reached) only if the kitchen pours faster than the drain can carry for too long.

`fsync()` is like a kitchen inspector saying "drain everything right now before I sign off." It blocks until the sink is empty for that file's pages. Kafka normally skips per-message fsync and relies on replication — if the data is in the page cache of 3 brokers, losing one broker doesn't lose data.

### 12.2 %util in iostat is not CPU %util

The `%util` column in `iostat` measures the fraction of time the device had at least one request in service — it is a **time** metric, not a throughput or capacity metric. For a single-queue device like an HDD, 100% util means the device was busy every second, and any new request has to wait. For an NVMe with 64 internal queues, 100% util might still mean the device is delivering maximum throughput because multiple requests are being served simultaneously. For NVMe, rely on `aqu-sz` (queue depth) and `await` (latency) as the real saturation indicators. `%util` is most meaningful for HDDs.

### 12.3 XFS Allocation Groups as Parallel Highways

XFS divides the disk into **Allocation Groups (AGs)**. Each AG is an independent mini-filesystem with its own free space B-tree, inode B-tree, and locks. Concurrent file operations in different AGs proceed without blocking each other. When 16 Kafka partition writers are all appending simultaneously, XFS routes them to different AGs — they write in parallel. ext4's single global allocation lock would serialize their allocation requests, adding lock contention to every file creation and extension.

---

## 13. Failure Scenarios

### 13.1 Kafka Write Throughput Collapses After Mount Options Change

```
Symptom: Kafka throughput drops from 500 MB/s to 50 MB/s after server reboot.
  iostat shows nvme0n1 at 100% util. Consumer lag is growing.

  iostat -xm 1 | grep nvme
  # nvme0n1  892.3  892.3  450.2  450.2  0.0  45.6  0.48  0.47  2.34  1.23  100.0
  #                                                         ↑               ↑
  #                                          write latency 2.34ms  ← was 0.48ms
  #                                                                    100% util

  Check current mount options:
  cat /proc/mounts | grep kafka
  # /dev/nvme0n1p1 /data/kafka xfs rw,relatime 0 0
  #                                    ↑
  #                                    Missing: noatime,nobarrier,logbufs=8
  #                                    relatime = still updating atime (metadata writes)
  
  Root cause: A server patch updated /etc/fstab but the admin used the wrong
  options — dropped nobarrier and the performance options.
  Also missing: the systemd mount override was not updated.

  nobarrier effect:
    Without nobarrier: XFS calls flush barriers (cache flush + FUA) to ensure
    journal commit ordering. On each journal commit, this adds ~1ms of latency.
    Under high write throughput, this serializes commits, limiting throughput.
    With battery-backed RAID cache or NVMe (which has capacitor-backed write cache),
    barriers are safe to disable.

  Fix:
    # Remount with correct options (no service restart needed)
    mount -o remount,noatime,nodiratime,nobarrier,logbufs=8,allocsize=1g /data/kafka
    
    # Verify
    cat /proc/mounts | grep kafka
    # /dev/nvme0n1p1 /data/kafka xfs rw,noatime,nodiratime,nobarrier,logbufs=8,allocsize=1g 0 0
    
    # Fix fstab so it survives future reboots
    UUID=$(blkid -s UUID -o value /dev/nvme0n1p1)
    sed -i "s|.*data/kafka.*|UUID=$UUID /data/kafka xfs noatime,nodiratime,nobarrier,logbufs=8,allocsize=1g 0 2|" /etc/fstab

  iostat after fix:
  # nvme0n1  892.3  892.3  450.2  112000.0  0.0  45.6  0.48  0.48  0.43  43.6
  # Throughput back to 112 MB/s writes, 0.48ms latency, 43.6% util. 
```

### 13.2 Disk Full Despite rm of Large Files

```
Symptom: df -h shows 100% on /data/kafka. Kafka can't write.
  Files were deleted via log rotation. Space was not freed.

  df -h /data/kafka
  # Filesystem   Size  Used  Avail  Use%  Mounted on
  # /dev/sda1    2.0T  2.0T     0   100%  /data/kafka

  Disk is full despite log rotation deleting segments 30 minutes ago.

  Find files that are deleted but still held open:
  lsof | grep deleted | grep kafka | head -20
  # java  1234  kafka  150r  REG  8,1  1073741824  5678  /data/kafka/logs/topic-0/...00000.log (deleted)
  # java  1234  kafka  151r  REG  8,1  2147483648  5679  /data/kafka/logs/topic-0/...01000.log (deleted)

  Root cause: Kafka log retention ran, called unlink() to delete old segments.
  But Kafka has those same segment files open for reading (consumer fetch path).
  The disk space is not freed until the last file descriptor is closed.
  The inode's link count reached 0 (name removed), but the open FD count is > 0.

  Fix Option A: Restart Kafka (closes all FDs, frees space)
    systemctl restart kafka
    df -h /data/kafka
    # Now shows reclaimed space

  Fix Option B: Send SIGHUP if Kafka supports log rotation reload
    kill -HUP $(pgrep -f kafka.Kafka)
    # Forces Kafka to close and reopen log files → closes deleted file FDs

  Fix Option C: Delete files from process FD table via /proc
    # For each deleted file still open:
    ls -la /proc/1234/fd | grep deleted
    # Closing the FD from outside the process requires gdb or /proc/pid/fd tricks
    # This is a last resort for an unrestartable service:
    gdb -p 1234 -ex "call close(150)" -ex "call close(151)" -ex "detach" -ex "quit"

  Prevention: Monitor for deleted-but-open files as part of disk usage alerting
    # Alert when: df shows >80% AND lsof shows large deleted files
    DELETED_SIZE=$(lsof /data/kafka 2>/dev/null | grep deleted | \
      awk '{sum += $7} END {print sum+0}')
    echo "Held-open deleted file bytes: $DELETED_SIZE"
```

### 13.3 XFS Filesystem Won't Mount After Power Failure

```
Symptom: After an abrupt server shutdown (power failure), Kafka disk won't mount.
  journalctl -b -1 shows kernel errors during boot.

  mount /dev/nvme0n1p1 /data/kafka
  # mount: /data/kafka: can't read superblock from /dev/nvme0n1p1.

  Check dmesg for XFS errors:
  dmesg | grep -i xfs
  # [   2.345678] XFS (nvme0n1p1): Unmount and run xfs_repair.
  # [   2.345679] XFS (nvme0n1p1): First 128 bytes of corrupted metadata...

  XFS requires repair (log replay failed or metadata corrupted):
  
  Step 1: Run xfs_repair (read-only check first)
    xfs_repair -n /dev/nvme0n1p1     # -n: dry run, no changes
    # Phase 1 - find and verify superblock...
    # Phase 2 - using internal log
    #   - zero log...   ← XFS is going to zero the unplayable log
    #   - scan filesystem freespace and inode maps...
    # Phase 7 - verify and correct link counts...
    # No modify flag set, skipping filesystem flush and exiting.

  Step 2: Run actual repair
    xfs_repair /dev/nvme0n1p1       # will zero the log and repair metadata
    # NOTE: zeroing the XFS log means any uncommitted writes at the time
    # of the power failure will be lost. This is expected — durability
    # requires battery-backed RAID cache or NVMe with supercapacitors,
    # or application-level fsync().

  Step 3: Mount
    mount /data/kafka
    # Should succeed now

  For ext4 comparison:
    e2fsck -f /dev/sda1      # full filesystem check (can take hours on large volumes)
    # ext4 fsck is O(filesystem blocks), not O(metadata) like xfs_repair

  Prevention: 
    - Use UPS for Kafka servers to allow graceful shutdown
    - Use NVMe with power-loss protection (PLP) — enterprise NVMe maintains
      write buffer integrity during power loss via supercapacitors
    - Set nobarrier ONLY when your storage has verified power-loss protection
```

### 13.4 iostat Shows High iowait but Disk is Underutilized

```
Symptom: CPUs show 60% iowait in top/iostat, but iostat shows all disks at <10% util.
  Spark job is crawling.

  iostat -xm 2
  # avg-cpu: %user  %nice  %system  %iowait
  #           5.23   0.00     0.78   61.45    ← 61% iowait

  # Device  r/s    rkB/s   %util
  # sda     0.2     3.2     0.8     ← barely any I/O on all physical disks

  What else could cause high iowait?

  Check swap:
    free -h
    # Swap:  4.0G  3.8G  0.2G   ← 95% of swap is used!
    vmstat 1 5
    # si   so        ← swap in / swap out
    # 1234  456      ← active swapping!

  Root cause: Spark executors consumed all RAM. The OS is swapping application
  pages to the swap device. CPUs are waiting for swap reads to bring back
  evicted pages — this shows as iowait because swap I/O goes through the block
  layer, just like disk I/O. But swap devices often have random I/O patterns
  which don't show high iostat %util because each request is tiny.

  Diagnosis:
    cat /proc/meminfo | grep -E "MemFree|MemAvailable|SwapTotal|SwapFree"
    # MemFree:        102400 kB   ← 100 MB free
    # MemAvailable:   512000 kB   ← only 500 MB available
    # SwapTotal:     4194304 kB
    # SwapFree:       204800 kB   ← only 200 MB swap left

    # Find which Spark executor is consuming memory
    ps -eo pid,rss,comm | grep java | sort -k2 -rn | head -5

  Fix:
    # Short term: reduce Spark executor memory usage
    # - Reduce spark.executor.memory
    # - Reduce spark.executor.cores (fewer concurrent tasks per executor)
    
    # Medium term: add swap space
    fallocate -l 16G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
    
    # Long term: add more RAM or use fewer executors per node
    
    # Prevent Spark from swapping (prefer OOM kill over swapping):
    # sysctl vm.swappiness=1   (0=never swap unless desperate; 60=default; 100=aggressive swap)
    sysctl -w vm.swappiness=1
    echo "vm.swappiness=1" >> /etc/sysctl.d/60-spark.conf
```

---

## 14. Data Engineering Connections

### 14.1 Kafka's Storage Dependency

Kafka is essentially a distributed write-ahead log. Its performance and durability are directly tied to storage:

```
Kafka write path:
  Producer → Kafka broker → write() to log segment file
  
  write() behavior:
    → data lands in page cache (fast, no disk I/O yet)
    → write() returns success to producer
    → Kafka sends ack to producer (if acks=1 or acks=all after replication)
    
  Durability model:
    acks=0: no ack (fire and forget, data can be lost)
    acks=1: ack after page cache write (data safe if broker doesn't crash before flush)
    acks=all: ack after all in-sync replicas have the data in page cache
              → even if one broker crashes, data is on 2 others
    
  fsync():
    log.flush.interval.messages=Long.MAX_VALUE (default: effectively never)
    log.flush.interval.ms=Long.MAX_VALUE       (default: effectively never)
    → Kafka NEVER calls fsync() in the default configuration
    → Durability comes from replication, not fsync()
    → This is intentional and correct for Kafka's design
    
  XFS allocsize=1g effect:
    When Kafka opens a new 1 GB log segment (fallocate), XFS pre-allocates 1 GB
    of contiguous disk space. All writes to that segment land in contiguous blocks.
    → Sequential writes at disk speed
    → No fragmentation over time
    → Reads for consumer fetches are sequential (page cache + sendfile())
```

### 14.2 HDFS DataNode I/O Pattern

```bash
# HDFS DataNode writes 128 MB blocks to local disk:
# - Sequential writes to a block file, then fsync() on block completion
# - Block replicated to 2 other DataNodes via network
# - Unlike Kafka: DataNode DOES fsync() each block (durability per block)

# Check DataNode disk usage
for dir in /data/hdfs/disk*; do
    echo -n "$dir: "
    df -h $dir | tail -1 | awk '{print $5, "used of", $2}'
done
# /data/hdfs/disk0: 45% used of 3.6T
# /data/hdfs/disk1: 47% used of 3.6T
# /data/hdfs/disk2: 43% used of 3.6T

# Monitor DataNode I/O throughput
iostat -xm 5 | grep -E "sdb|sdc|sdd|sde"
# sdb  0.0  45.2  0.0  180.0  ...  36.0%   ← 180 MB/s write (HDFS replication incoming)
# sdc  0.0  44.8  0.0  178.4  ...  35.6%
# sdd  0.0  45.0  0.0  179.2  ...  35.8%
# sde  0.0  12.3  0.0   49.2  ...   9.9%   ← this disk is underutilized

# Uneven distribution = one DataNode directory has a stale/failed disk
# OR HDFS block placement is unbalanced
# Fix: run HDFS balancer
hdfs balancer -threshold 5   # balance until no DataNode is >5% from average
```

### 14.3 Spark Shuffle I/O

```bash
# Spark shuffle: write phase writes output of each task to local disk
# as sorted, partitioned spill files. Read phase reads other executor's spill files.

# Monitoring during a shuffle-heavy job:
# Terminal 1: iostat for throughput
iostat -xm 2 | grep md0   # RAID-0 shuffle device

# Terminal 2: iotop for per-task I/O
sudo iotop -b -o -d 2 | grep java

# Terminal 3: disk utilization trend
watch -n 2 'df -h /data/spark/shuffle'

# Common Spark storage problems:
# 1. Shuffle write too slow → increase shuffle.file.buffer (default 32 KB)
#    spark.shuffle.file.buffer=1m   # 1 MB write buffer per shuffle output file
#
# 2. Too many small shuffle files → enable sort-based shuffle
#    spark.shuffle.manager=sort  (default since Spark 1.2)
#    Consolidates N output files per reducer into fewer large files
#
# 3. Shuffle read bottleneck → check reducer count
#    Too few reducers: large individual files, all reducers hot at the same time
#    Too many reducers: many tiny files, open() overhead dominates
#    Rule of thumb: partition size 100-500 MB in shuffle output

# Check shuffle file sizes after a job
ls -lh /data/spark/shuffle/blockmgr-*/00/ | sort -k5 -rn | head -20
du -sh /data/spark/shuffle/
```

---

## 15. Code Toolkit

### 15.1 `io_diagnostics.py` — Storage Health and Performance Monitor

```python
"""
io_diagnostics.py — Storage diagnostics for data engineering servers.

Functions:
  1. parse_diskstats()          — read /proc/diskstats
  2. compute_io_metrics()       — compute throughput/IOPS from diskstats delta
  3. check_mount_options()      — verify critical mount options are set
  4. check_filesystem_usage()   — disk space + inode utilization
  5. find_deleted_open_files()  — find lsof-visible deleted files consuming space
  6. monitor_dirty_pages()      — read page cache writeback stats
  7. io_health_report()         — comprehensive storage health summary

Run directly for a full storage health report.
"""
from __future__ import annotations
import os
import re
import time
import subprocess
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


# ── Disk statistics ────────────────────────────────────────────────────────────

@dataclass
class DiskStats:
    name: str
    reads_completed: int
    reads_merged: int
    sectors_read: int
    time_reading_ms: int
    writes_completed: int
    writes_merged: int
    sectors_written: int
    time_writing_ms: int
    ios_in_progress: int
    total_io_ms: int


def parse_diskstats() -> dict[str, DiskStats]:
    """Parse /proc/diskstats into a dict keyed by device name."""
    stats: dict[str, DiskStats] = {}
    try:
        with open("/proc/diskstats") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 14:
                    continue
                name = parts[2]
                # Skip partition entries (sdXN, nvme0n1pN) — only track whole disks
                if re.match(r"(sd[a-z]+|nvme\d+n\d+|md\d+|dm-\d+)$", name):
                    stats[name] = DiskStats(
                        name=name,
                        reads_completed=int(parts[3]),
                        reads_merged=int(parts[4]),
                        sectors_read=int(parts[5]),
                        time_reading_ms=int(parts[6]),
                        writes_completed=int(parts[7]),
                        writes_merged=int(parts[8]),
                        sectors_written=int(parts[9]),
                        time_writing_ms=int(parts[10]),
                        ios_in_progress=int(parts[11]),
                        total_io_ms=int(parts[12]),
                    )
    except FileNotFoundError:
        pass
    return stats


@dataclass
class IOMetrics:
    name: str
    read_mbps: float
    write_mbps: float
    read_iops: float
    write_iops: float
    avg_read_kb: float      # average read request size in KB
    avg_write_kb: float     # average write request size in KB
    util_pct: float         # estimated utilization (fraction of time busy)
    ios_in_progress: int    # current I/O queue depth


def compute_io_metrics(before: DiskStats, after: DiskStats, elapsed_s: float) -> IOMetrics:
    """Compute I/O rates from two /proc/diskstats snapshots."""
    # Sectors are 512 bytes each
    read_sectors  = after.sectors_read    - before.sectors_read
    write_sectors = after.sectors_written - before.sectors_written
    reads  = after.reads_completed  - before.reads_completed
    writes = after.writes_completed - before.writes_completed
    total_io_ms = after.total_io_ms - before.total_io_ms

    read_mbps  = (read_sectors  * 512) / (1024 * 1024 * elapsed_s)
    write_mbps = (write_sectors * 512) / (1024 * 1024 * elapsed_s)
    read_iops  = reads  / elapsed_s
    write_iops = writes / elapsed_s
    avg_read_kb  = ((read_sectors  * 512) / reads  / 1024) if reads  > 0 else 0
    avg_write_kb = ((write_sectors * 512) / writes / 1024) if writes > 0 else 0
    # Utilization: fraction of elapsed time the device was busy
    util_pct = min(100.0, (total_io_ms / 1000.0) / elapsed_s * 100)

    return IOMetrics(
        name=after.name,
        read_mbps=read_mbps,
        write_mbps=write_mbps,
        read_iops=read_iops,
        write_iops=write_iops,
        avg_read_kb=avg_read_kb,
        avg_write_kb=avg_write_kb,
        util_pct=util_pct,
        ios_in_progress=after.ios_in_progress,
    )


def sample_io_metrics(interval_s: float = 3.0) -> list[IOMetrics]:
    """Sample /proc/diskstats twice, return computed metrics."""
    before = parse_diskstats()
    time.sleep(interval_s)
    after = parse_diskstats()
    metrics = []
    for name, b in before.items():
        if name in after:
            m = compute_io_metrics(b, after[name], interval_s)
            metrics.append(m)
    return sorted(metrics, key=lambda m: m.write_mbps + m.read_mbps, reverse=True)


# ── Mount option verification ──────────────────────────────────────────────────

def check_mount_options(mountpoints: dict[str, list[str]]) -> dict[str, dict]:
    """
    Check that critical mount options are set for given mountpoints.
    
    Args:
        mountpoints: {mountpoint: [required_options]}
        e.g., {"/data/kafka": ["noatime", "nobarrier", "nodiratime"]}
    
    Returns:
        {mountpoint: {"options": [...], "missing": [...], "ok": bool}}
    """
    # Parse /proc/mounts
    mounts: dict[str, str] = {}
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 4:
                    mounts[parts[1]] = parts[3]  # mountpoint → options string
    except FileNotFoundError:
        pass
    
    results = {}
    for mp, required in mountpoints.items():
        current_opts = mounts.get(mp, "")
        current_set = set(current_opts.split(","))
        missing = [opt for opt in required if opt not in current_set]
        results[mp] = {
            "options": current_opts,
            "required": required,
            "missing": missing,
            "ok": len(missing) == 0,
        }
    return results


# ── Filesystem usage ──────────────────────────────────────────────────────────

@dataclass
class FSUsage:
    mountpoint: str
    device: str
    fstype: str
    total_gb: float
    used_gb: float
    avail_gb: float
    use_pct: float
    inode_total: int
    inode_used: int
    inode_avail: int
    inode_pct: float


def check_filesystem_usage(mountpoints: Optional[list[str]] = None) -> list[FSUsage]:
    """Return disk space and inode usage for filesystems."""
    usage = []
    
    # Get block usage from os.statvfs
    targets = mountpoints or ["/"]
    
    # Parse /proc/mounts to get device and fstype
    mount_info: dict[str, tuple[str, str]] = {}
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3:
                    mount_info[parts[1]] = (parts[0], parts[2])
    except FileNotFoundError:
        pass
    
    for mp in targets:
        try:
            stat = os.statvfs(mp)
        except (FileNotFoundError, PermissionError, OSError):
            continue
        
        bs = stat.f_frsize  # fundamental block size
        total_gb = (stat.f_blocks * bs) / (1024 ** 3)
        avail_gb = (stat.f_bavail * bs) / (1024 ** 3)
        used_gb  = total_gb - (stat.f_bfree * bs / (1024 ** 3))
        use_pct  = (used_gb / total_gb * 100) if total_gb > 0 else 0
        
        inode_total = stat.f_files
        inode_avail = stat.f_favail
        inode_used  = inode_total - stat.f_ffree
        inode_pct   = (inode_used / inode_total * 100) if inode_total > 0 else 0
        
        device, fstype = mount_info.get(mp, ("unknown", "unknown"))
        usage.append(FSUsage(
            mountpoint=mp,
            device=device,
            fstype=fstype,
            total_gb=total_gb,
            used_gb=used_gb,
            avail_gb=avail_gb,
            use_pct=use_pct,
            inode_total=inode_total,
            inode_used=inode_used,
            inode_avail=inode_avail,
            inode_pct=inode_pct,
        ))
    return usage


# ── Dirty page monitoring ──────────────────────────────────────────────────────

def monitor_dirty_pages() -> dict[str, int]:
    """Read page cache writeback state from /proc/vmstat."""
    data: dict[str, int] = {}
    keys_of_interest = {
        "nr_dirty", "nr_writeback", "nr_dirty_threshold",
        "nr_dirty_background_threshold", "pgpgout", "pgpgin",
        "pswpin", "pswpout"
    }
    try:
        with open("/proc/vmstat") as f:
            for line in f:
                parts = line.split()
                if len(parts) == 2 and parts[0] in keys_of_interest:
                    data[parts[0]] = int(parts[1])
    except FileNotFoundError:
        pass
    return data


def get_dirty_thresholds() -> dict[str, str]:
    """Read vm.dirty_ratio and related sysctl values."""
    settings = {
        "vm.dirty_ratio": "/proc/sys/vm/dirty_ratio",
        "vm.dirty_background_ratio": "/proc/sys/vm/dirty_background_ratio",
        "vm.dirty_writeback_centisecs": "/proc/sys/vm/dirty_writeback_centisecs",
        "vm.dirty_expire_centisecs": "/proc/sys/vm/dirty_expire_centisecs",
        "vm.swappiness": "/proc/sys/vm/swappiness",
    }
    result = {}
    for name, path in settings.items():
        try:
            result[name] = Path(path).read_text().strip()
        except FileNotFoundError:
            result[name] = "N/A"
    return result


# ── Main health report ────────────────────────────────────────────────────────

def io_health_report(
    monitor_mounts: Optional[list[str]] = None,
    required_options: Optional[dict[str, list[str]]] = None,
    io_sample_s: float = 3.0
) -> None:
    """
    Print a comprehensive I/O health report.
    
    Args:
        monitor_mounts: list of mountpoints to check usage for
        required_options: {mountpoint: [required_mount_options]}
        io_sample_s: seconds to sample I/O metrics
    """
    print("=" * 70)
    print("Storage I/O Health Report")
    print("=" * 70)
    
    # 1. I/O metrics
    print(f"\n1. Device I/O (sampling {io_sample_s}s)")
    print(f"   {'Device':<12}  {'Read':>8}  {'Write':>8}  {'R IOPS':>7}  {'W IOPS':>7}  "
          f"{'Avg W KB':>8}  {'Util':>5}  {'Status'}")
    metrics = sample_io_metrics(io_sample_s)
    for m in metrics:
        if m.read_mbps + m.write_mbps < 0.01:
            continue  # skip idle devices
        if m.util_pct < 70:
            status = "OK"
        elif m.util_pct < 90:
            status = "MODERATE"
        else:
            status = "SATURATED"
        io_pattern = "seq" if m.avg_write_kb >= 64 else "rand"
        print(f"   {m.name:<12}  {m.read_mbps:>6.1f}MB/s  {m.write_mbps:>6.1f}MB/s  "
              f"{m.read_iops:>7.0f}  {m.write_iops:>7.0f}  "
              f"{m.avg_write_kb:>6.0f}KB({io_pattern})  {m.util_pct:>4.0f}%  {status}")
    
    # 2. Mount option check
    if required_options:
        print("\n2. Mount Option Verification")
        results = check_mount_options(required_options)
        for mp, result in results.items():
            status = "✓ OK" if result["ok"] else f"✗ MISSING: {', '.join(result['missing'])}"
            print(f"   {mp}: {status}")
    
    # 3. Filesystem usage
    targets = monitor_mounts or ["/"]
    usage_list = check_filesystem_usage(targets)
    if usage_list:
        print("\n3. Filesystem Usage")
        print(f"   {'Mount':<25}  {'Used':>8}  {'Avail':>8}  {'Use%':>5}  "
              f"{'IUse%':>6}  {'Status'}")
        for u in usage_list:
            disk_alert = u.use_pct > 85
            inode_alert = u.inode_pct > 85
            status_parts = []
            if disk_alert:
                status_parts.append(f"DISK {u.use_pct:.0f}%")
            if inode_alert:
                status_parts.append(f"INODES {u.inode_pct:.0f}%")
            status = " ".join(status_parts) if status_parts else "OK"
            print(f"   {u.mountpoint:<25}  {u.used_gb:>6.1f}GB  {u.avail_gb:>6.1f}GB  "
                  f"{u.use_pct:>4.0f}%  {u.inode_pct:>5.0f}%  {status}")
    
    # 4. Dirty page state
    dirty = monitor_dirty_pages()
    thresholds = get_dirty_thresholds()
    if dirty:
        print("\n4. Page Cache State")
        
        # Get total RAM for context
        mem_kb = 0
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        mem_kb = int(line.split()[1])
                        break
        except Exception:
            pass
        
        dirty_pages = dirty.get("nr_dirty", 0)
        writeback_pages = dirty.get("nr_writeback", 0)
        page_size_kb = os.sysconf("SC_PAGE_SIZE") // 1024
        dirty_mb = dirty_pages * page_size_kb / 1024
        writeback_mb = writeback_pages * page_size_kb / 1024
        swapping = dirty.get("pswpin", 0) + dirty.get("pswpout", 0) > 0
        
        print(f"   Dirty pages:     {dirty_mb:.0f} MB  ({dirty_pages} pages)")
        print(f"   In writeback:    {writeback_mb:.0f} MB  ({writeback_pages} pages)")
        print(f"   vm.dirty_ratio:  {thresholds.get('vm.dirty_ratio', '?')}%  "
              f"(blocks at {mem_kb * int(thresholds.get('vm.dirty_ratio', '20')) // 100 // 1024} MB dirty)")
        print(f"   vm.swappiness:   {thresholds.get('vm.swappiness', '?')}")
        if swapping:
            print(f"   ⚠️  SWAP ACTIVITY DETECTED — check for memory pressure")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    # Example: monitor Kafka and HDFS storage
    io_health_report(
        monitor_mounts=["/", "/data/kafka", "/data/hdfs"],
        required_options={
            "/data/kafka": ["noatime", "nobarrier", "nodiratime"],
            "/data/hdfs":  ["noatime", "nobarrier", "nodiratime"],
        },
        io_sample_s=3.0,
    )
```

---

## 16. Hands-On Labs

### Lab 1: Read and Interpret iostat Output

```bash
# Lab 1: generate and interpret I/O load

# Generate sequential writes (simulating Kafka log writes)
dd if=/dev/urandom of=/tmp/seq_write_test bs=1M count=2048 oflag=direct &
DD_PID=$!

# In parallel, run iostat
iostat -xm 1 10

# Observe:
# - wkB/s rises (write throughput)
# - wareq-sz: should be large (1 MB if dd is writing in 1 MB blocks)
# - %util: fraction of time the device is busy
# - aqu-sz: queue depth

wait $DD_PID
rm /tmp/seq_write_test

# Generate random reads (simulating many small Spark operations)
# First create test file
dd if=/dev/urandom of=/tmp/random_read_test bs=1M count=1024 oflag=direct
fio --name=random_read \
    --filename=/tmp/random_read_test \
    --rw=randread \
    --bs=4k \
    --numjobs=8 \
    --iodepth=32 \
    --time_based \
    --runtime=15 \
    --group_reporting &
FIO_PID=$!

iostat -xm 1 15

# Observe:
# - r/s: high IOPS (random 4k reads from 8 jobs × 32 depth)
# - rareq-sz: small (4 KB = random access pattern)
# - r_await: latency per read (higher than sequential)

wait $FIO_PID
rm /tmp/random_read_test
```

### Lab 2: Verify and Fix Mount Options

```bash
# Lab 2: check and fix mount options on a test filesystem

# Create a test filesystem in a file (loopback device)
fallocate -l 1G /tmp/test_fs.img
mkfs.xfs /tmp/test_fs.img

# Mount with minimal (incorrect) options
mkdir -p /tmp/test_mount
mount -o loop /tmp/test_fs.img /tmp/test_mount

# Check current options
cat /proc/mounts | grep test_mount
# /dev/loop0 /tmp/test_mount xfs rw,relatime 0 0
# Missing: noatime, nodiratime

# Remount with correct options
mount -o remount,noatime,nodiratime /tmp/test_mount

# Verify
cat /proc/mounts | grep test_mount
# /dev/loop0 /tmp/test_mount xfs rw,noatime,nodiratime 0 0

# Check filesystem info
xfs_info /tmp/test_mount

# Cleanup
umount /tmp/test_mount
rm /tmp/test_fs.img
```

---

## 17. Interview Q&A

**Q1: Kafka is writing to disk at 50 MB/s but you expect 400 MB/s. Walk through your diagnostic approach.**

Start with `iostat -xm 1` and watch the device backing Kafka's log directory for 30 seconds. The `%util` column tells me if the device is saturated (>90% util means the disk can't keep up). The `wareq-sz` column tells me about I/O pattern: if it's small (4-16 KB), Kafka is writing in small chunks rather than large sequential batches, which on an HDD would cause random-write performance — but Kafka should always write sequentially. If `%util` is low (say 30%) but throughput is only 50 MB/s, the bottleneck is not the disk itself.

Next I'd check the mount options: `cat /proc/mounts | grep kafka`. If `nobarrier` is missing, XFS is calling flush barriers on every journal commit, serializing writes. If `noatime` is missing, every Kafka read operation triggers a metadata write for access time. Either can reduce throughput by 5-10×. I'd also check the Kafka JVM: is the GC pausing the producer threads? `jstat -gcutil <pid> 1000` shows GC frequency. And I'd check if the page cache is doing writeback at full speed: `cat /proc/vmstat | grep nr_dirty` — if dirty pages are very high and writeback is slow, vm.dirty_ratio may be misconfigured, causing the kernel to block application writes while it catches up with flushing.

**Q2: What is the difference between `%util` in iostat and `%iowait` in top? Can you have high iowait with low %util?**

They measure different things. `%util` (in iostat) is a property of a specific block **device** — it reports what fraction of elapsed time that device had at least one I/O request in service. `%iowait` (in top/iostat's avg-cpu section) is a property of the **CPU** — it reports the fraction of time the CPU was idle while at least one process in the system was blocked waiting for I/O. Both are time percentages but they count different time.

Yes, you absolutely can have high iowait with low %util — this is actually a common diagnostic trap. The most common causes: the I/O is going to swap space, not a data disk. Each swap request is tiny (one page = 4 KB), so the swap device's `%util` stays low (many small random requests, but each completes quickly), while CPUs are constantly waiting for swap-in to complete. Another cause: NFS I/O. Network I/O doesn't appear in `iostat` (which only shows block devices), but NFS reads block processes and show up as `%iowait`. A third cause: many processes are doing random I/O that each complete quickly but arrive frequently from different CPUs — the per-device `%util` looks moderate but CPUs are cumulatively spending a lot of time waiting.

**Q3: Why does Kafka recommend XFS and not ext4? Be specific about the differences that matter for Kafka's workload.**

Kafka's workload has three I/O characteristics that make XFS preferable. First, **concurrent partition append**: each partition is an independent log file being appended to. On a broker with 500 partitions, hundreds of these might be written concurrently. ext4 uses a single global lock for block allocation; when 100 partitions simultaneously try to allocate new blocks (when opening a new 1 GB log segment), they all serialize on this lock. XFS uses per-allocation-group locking — with 32 AGs, 32 writes can allocate concurrently without contention.

Second, **log segment creation and deletion**: Kafka creates 1 GB log segments with `fallocate()` and deletes old segments with `unlink()`. XFS performs background extent tree cleanup for unlinked files, so the `unlink()` call returns quickly and the actual block deallocation happens asynchronously. ext4's deallocation holds metadata locks longer, which can cause brief stalls visible as Kafka "jitter" in produce latency during log retention runs.

Third, **fsck recovery time**: after a power failure, ext4's `e2fsck` scans the entire filesystem block map — on a 20 TB Kafka volume this can take 4+ hours before the broker can mount the filesystem and restart. XFS's `xfs_repair` replays only the uncommitted transactions in the journal — typically seconds on any filesystem size. For a Kafka cluster, recovery time directly maps to replication lag and out-of-sync replicas, so fast recovery matters significantly.

**Q4: What is the page cache and how does it relate to Kafka's durability model?**

The page cache is a region of RAM managed by the Linux kernel that serves as a write buffer and read cache for filesystem I/O. When a process calls `write()`, the data is copied into page cache pages which are marked "dirty" — they contain data not yet written to disk. The kernel's writeback threads flush dirty pages to disk asynchronously based on configurable thresholds (`vm.dirty_background_ratio`, `vm.dirty_expire_centisecs`). `write()` returns to the caller immediately after the page cache copy, without waiting for disk I/O. `fsync()` forces all dirty pages for a specific file descriptor to be written to disk and waits until the disk confirms completion.

Kafka's durability model is built around replication rather than fsync. In the default configuration, `log.flush.interval.messages` is effectively infinity — Kafka never explicitly calls `fsync()`. A single `write()` puts the message in the broker's page cache; the ack is sent once the required number of in-sync replicas (`acks=all`) have also written it to their page caches. If broker A crashes before the page cache is flushed to disk, the data is still safe on brokers B and C in their page caches — and will eventually be flushed to their disks. The data would only be lost if all replicas failed simultaneously before any of their page caches were flushed to disk — an unlikely correlated failure. This design allows Kafka to achieve sustained write throughput matching the raw disk speed without the serialization overhead of per-message fsync.

**Q5: What does `nobarrier` do as a mount option, and when is it safe to use?**

A write barrier is an ordered sequence of cache flush commands sent to the storage device to ensure that writes are committed to persistent storage in the correct order. Filesystems use barriers to guarantee journal integrity: before committing a transaction to the filesystem journal, the journal must confirm that previous operations are durable — not just in the disk's volatile write cache.

Without `nobarrier`, each XFS journal commit issues a cache flush to the disk (or an FUA — Force Unit Access — write that bypasses the cache). On a single HDD, this adds 5-10 ms of latency per commit because the drive must rotate to the journal location and physically write. On a write-heavy Kafka broker issuing thousands of journal commits per second, this can be the dominant bottleneck.

`nobarrier` is safe only when the storage has **power-loss protection for its write buffer**. This means: a RAID controller with a battery-backed write cache (BBWC) or a flash-backed write cache (FBWC); or an enterprise NVMe/SSD with a supercapacitor that ensures the volatile write buffer is flushed to NAND on power loss. Consumer SSDs and cheap VMs typically do NOT have power-loss protection — using `nobarrier` on these risks filesystem corruption (including loss of the journal) after a power failure. For VMs where the hypervisor provides a reliable ordered write path, `nobarrier` is often safe, but this requires understanding the specific virtualization stack.

**Q6: You run `df -h` and see a filesystem at 98% usage. `du -sh /*` shows only 40% of the disk is used by files you can find. What explains the gap?**

There are three common explanations. First, **deleted-but-open files**: processes have files open that have been `unlink()`-ed. The files have no directory entry but the kernel still holds the inode and data blocks for the file because there are open file descriptors referencing them. `lsof | grep deleted` shows these files and their sizes. The space is freed only when the last file descriptor is closed. Common cause in data engineering: log rotation deletes old log files while the logging daemon still has them open. Fix: restart the process, or send SIGHUP if it supports log rotation.

Second, **reserved blocks for root**: ext4 reserves 5% of blocks (by default) for the root user, so normal users hit "no space left" at 95% usage while `df` shows 5% free. `tune2fs -l /dev/sda1 | grep "Reserved block"` shows the count; `tune2fs -m 1 /dev/sda1` reduces the reserve to 1% for data disks where the system doesn't need emergency root access.

Third, **filesystem overhead itself**: XFS and ext4 both use disk space for journal/log, inode tables, and metadata structures. These are allocated at `mkfs` time and not counted as "used files" in `du`, but are included in `df`'s "used" figure. On XFS, the internal log takes space; on ext4, the inode table and journal are pre-allocated. This is typically 2-5% of disk size and explains a small gap, not a 40% discrepancy.

---

## 18. Cross-Question Chain

**Q1 [Interviewer]: A Kafka broker's disk is at 100% utilization in iostat but throughput is only 150 MB/s — less than the disk's rated 500 MB/s. What's your first hypothesis?**

My first hypothesis is that the I/O pattern is random rather than sequential, or the device is being constrained by something above the block layer. A disk can be "100% utilized" — meaning it has at least one I/O request in service every second — while delivering far less than peak throughput if the requests are small and random. Each small random I/O spends most of its time seeking (for HDDs) or doing address translation lookups (for SSDs with a fragmented LBA map). I'd check `wareq-sz` in iostat: if it's small (say 8-16 KB) when I expect sequential Kafka writes at 1 MB, something is causing Kafka to write in small chunks.

**Q2 [Interviewer]: `wareq-sz` is showing 16 KB — you're right, writes are small. What would cause Kafka to write in 16 KB chunks instead of large sequential writes?**

Kafka's producer batches writes into the log via `write()` calls. The size of each write depends on how many messages are batched before the write is issued. Small `wareq-sz` suggests either very low-throughput producer activity (each produce request has few messages, so each `write()` is small) or that Kafka's `batch.size` and `linger.ms` are not configured to allow batching. But more likely in this scenario — since the disk is at 100% util — there's also heavy read activity interleaved with writes. Run `iotop` to see who is doing I/O: if I see many Java threads with both read and write activity, Kafka follower fetch threads are reading from disk to replicate data while producer write threads are appending. If the page cache is warm (recently written data is in RAM), reads should be served from cache. If the page cache has been evicted — perhaps because other processes consumed RAM — follower reads hit disk, causing a read/write mix that destroys sequential write performance.

**Q3 [Interviewer]: iotop confirms both read and write I/O from the same JVM. The server has 64 GB RAM but free shows only 500 MB available. What happened to the page cache?**

If 64 GB of RAM is installed, MemAvailable showing 500 MB means the page cache has been evicted — something consumed almost all physical RAM. I'd check `/proc/meminfo` for the RSS breakdown: `MemTotal`, `Buffers`, `Cached`, and the anon RSS of running processes. If the Kafka JVM's RSS is 4 GB but there are also Spark executor JVMs with 12 GB each on the same node, they may have evicted Kafka's page cache. Kafka's design assumes the OS keeps recently written log segment data in the page cache for consumer reads — if consumers are reading data that was just written, those reads go straight from the page cache, bypassing the disk entirely. When another process evicts Kafka's pages, every consumer read becomes a disk read.

**Q4 [Interviewer]: Confirmed: a Spark executor on the same node consumed 50 GB of RAM during a large aggregation, evicting Kafka's page cache. How do you fix this without relocating Kafka?**

There are two layers of fix. At the OS level, set `vm.swappiness=1` to prevent the kernel from unnecessarily swapping Kafka's JVM pages in favor of keeping file cache. More importantly, use `cgroups` to limit the Spark executor's memory ceiling: configure Spark with `spark.executor.memoryOverheadFactor` and run the executor inside a cgroup that enforces a memory hard limit. When the Spark executor hits the limit, the OOM killer kills it (contained within the cgroup) rather than allowing it to evict Kafka's cache. This is exactly what Kubernetes does when you set `resources.limits.memory` on a Pod. At the Kafka level, configure `log.retention.bytes` to keep Kafka's working set small enough to fit in the RAM budget allocated for page cache — if Kafka is retaining 2 TB of logs but consumers are only reading the last 10 GB, 2 TB of disk I/O is needed for follower replication while only 10 GB needs to be warm in cache.

**Q5 [Interviewer]: The Spark executor is now memory-limited. But you notice the Spark job is now spilling shuffle data to disk constantly. iostat shows the shuffle disk at 90% util. You run out of space on the shuffle volume. How do you diagnose and address this?**

First, confirm with `df -h /data/spark/shuffle` — if it's 100%, new spill files can't be created and Spark tasks will fail with an IOException. Check how much is actually being used versus what the cleanup process should have freed: `du -sh /data/spark/shuffle/*` to see if old shuffle files from completed stages are still present. Spark normally deletes shuffle files after a stage completes, but if an executor died or the shuffle service is not running, files may be orphaned. Look for directories named with job IDs from jobs that finished hours ago.

For the space issue itself, I'd look at `lsblk` to see if there's any additional unpartitioned space on the server I can expand into. If the shuffle volume is on XFS, I can grow it online: `xfs_growfs /data/spark/shuffle` after expanding the underlying partition or LVM volume. If I have additional disks, I'd add them as additional `spark.local.dir` entries — Spark distributes shuffle data across all local directories in a round-robin pattern, effectively adding capacity and throughput proportionally.

**Q6 [Interviewer]: After fixing the space, the Spark job still spills excessively. How do you address the spill itself — not just the disk space symptoms?**

Excessive spill means Spark's executor doesn't have enough JVM heap to hold the shuffle data in memory. The fix chain: first, check `spark.executor.memory` — if it was reduced to share the node with Kafka, it may now be too small for the aggregation. Second, check the partition count: if the aggregation has too few output partitions, each partition has too much data, exceeding the per-partition memory budget. Increasing `spark.sql.shuffle.partitions` (default 200) distributes the aggregation across more, smaller partitions. Third, check for data skew: one partition might be 100× larger than others because of skewed keys. `spark.sql.adaptive.skewJoin.enabled=true` handles this automatically in Spark 3+. Fourth, consider enabling off-heap memory: `spark.memory.offHeap.enabled=true` with `spark.memory.offHeap.size=8g` lets Spark use off-heap memory for shuffle data, which doesn't count against the JVM heap and is not subject to GC pressure — which can itself be a cause of spill when GC pauses interrupt in-progress shuffle operations.

---

## 19. Common Misconceptions

**"100% disk utilization in iostat means the disk is the bottleneck."**
`%util = 100%` means the device was busy (had an I/O request in service) every second of the measurement interval — but "busy" doesn't mean "saturated." A single-queue HDD at 100% util with 200 IOPS is genuinely saturated. A modern NVMe at 100% util might still have 500,000 IOPS of headroom — it processes requests in parallel internally, and the OS-level scheduler sees it as "always busy" because requests arrive as fast as they complete. For NVMe, `aqu-sz` (queue depth) and `await` (latency) are better saturation indicators. If `aqu-sz < 4` and `await < 0.5ms`, the NVMe is running well below saturation regardless of `%util`.

**"nobarrier is always safe on SSDs."**
`nobarrier` is safe only when the storage device has power-loss protection — meaning its write buffer is flushed to persistent storage automatically if power is lost. Consumer-grade SSDs typically do NOT have this feature; they have volatile DRAM write caches that lose data on power loss. Using `nobarrier` on a consumer SSD can result in journal corruption after a power failure: the journal appears committed (written to the SSD's cache) but was never actually written to NAND. Always verify your SSD's power-loss protection before using `nobarrier`.

**"XFS is always faster than ext4."**
XFS outperforms ext4 in specific scenarios: high-concurrency, large files, or large filesystems. For single-writer, sequential, small-file workloads, ext4 can match or slightly exceed XFS. For metadata-heavy workloads (millions of small files, many `stat()` calls), XFS's B-tree directory scales better, but the difference only emerges at scale. The practical guidance: use XFS for Kafka, HDFS, and Spark shuffle because these workloads specifically hit XFS's strengths. For a small development VM with a single application, ext4 is fine and has better tooling familiarity on Ubuntu/Debian.

**"The `sync` mount option is the safest choice for important data."**
`sync` makes every `write()` synchronous — it flushes the data to disk before returning to the application. This is safe in the sense that no data is lost on crash, but it has a catastrophic performance penalty: a Kafka broker that achieves 500 MB/s with async writes might achieve 2-5 MB/s with `sync` because every `write()` waits for the disk round-trip. The correct solution for data durability is `fsync()` by the application at the right granularity (after a complete transaction or log segment), not the blunt `sync` mount option that makes every individual write synchronous.

---

## 20. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What does `%util` measure in `iostat -x`? | The fraction of elapsed time the device had at least one I/O request in service. Does NOT directly mean capacity is exhausted — for NVMe with internal parallelism, 100% util may still have headroom. Use `aqu-sz` and `await` for NVMe saturation assessment. |
| 2 | What does `wareq-sz` in iostat tell you? | Average write request size in KB. Large `wareq-sz` (>64 KB) = sequential writes (good for Kafka). Small `wareq-sz` (<16 KB) = random writes (bad for HDD, potentially problematic for Kafka). |
| 3 | What is `w_await` in iostat? | Average latency in milliseconds from when a write request was submitted to the block layer until it completed (including time waiting in queue + device service time). HDD: ~10 ms is normal. SSD: <1 ms. NVMe: <0.2 ms. |
| 4 | What is `aqu-sz` in iostat? | Average I/O queue size — number of requests waiting or being served. >1 means requests are queueing. For NVMe, aqu-sz > 4 with high await = saturated. For HDD, aqu-sz > 2 = saturated. |
| 5 | What does `noatime` mount option do? | Disables access time (atime) update when a file is read. Eliminates per-read metadata writes, reducing I/O load. Especially important for Kafka where consumer reads would otherwise trigger inode writes. |
| 6 | What does `nobarrier` do and when is it safe? | Disables write barriers (cache flush commands) that enforce journal ordering. Safe ONLY when storage has power-loss protection (battery-backed RAID cache or NVMe with supercapacitor). Dangerous on consumer SSDs and unconfigured VMs. |
| 7 | What is `allocsize=1g` for XFS? | Speculative preallocation: XFS pre-allocates 1 GB of contiguous disk space when a file is opened for writing. Ensures Kafka log segments are written to contiguous blocks (sequential I/O), preventing fragmentation over time. |
| 8 | What I/O scheduler should NVMe use? | `none` (noop). NVMe has its own internal parallelism and scheduling; adding OS-level I/O scheduling adds overhead without benefit. `cat /sys/block/nvme0n1/queue/scheduler` should show `[none]`. |
| 9 | What is an Allocation Group in XFS? | An independent mini-filesystem partition within XFS with its own free space B-tree, inode B-tree, and lock. Concurrent operations in different AGs proceed without blocking each other. Key for Kafka's concurrent partition writes. |
| 10 | Why does XFS fsck faster than ext4? | XFS `xfs_repair` replays only the committed journal transactions — typically seconds on any filesystem size. ext4 `e2fsck` scans the entire block map — O(filesystem size), taking hours on 20 TB volumes. |
| 11 | What does `lsblk -f` show? | Block device tree with filesystem type (FSTYPE) and UUID for each device. Shows whole disks, partitions, LVM, RAID layers with their mount points. Essential for mapping logical paths to physical devices. |
| 12 | How do you find which process is doing the most disk writes? | `sudo iotop -b -o -d 2` lists processes doing I/O sorted by throughput. `-o` shows only processes currently doing I/O. `-b` is non-interactive for scripting. |
| 13 | What does `vm.dirty_ratio` control? | The percentage of total RAM that can hold dirty pages before the kernel starts blocking application writes (synchronous writeback). Default 20%. Lower it (10%) for Kafka to prevent large write stalls when the background flush can't keep up. |
| 14 | Why is the `<pass>` field in /etc/fstab important? | Controls fsck order at boot. 0=skip fsck, 1=check first (root), 2=check after root. Data disks should use `2` (not `0`) so fsck detects corruption after unclean shutdown. Disposable volumes (Spark shuffle) can use `0`. |
| 15 | What command shows filesystem type and UUID for all mounted filesystems? | `lsblk -f` or `blkid`. Use UUID (not `/dev/sdX`) in `/etc/fstab` because device names can change when disks are added or reordered. |
| 16 | What is read-ahead and how do you tune it? | The kernel prefetches data ahead of sequential reads to hide I/O latency. Default 128 KB. For large sequential scan workloads (HDFS block reads): `echo 4096 > /sys/block/sda/queue/read_ahead_kb` (2 MB). For random access: set low or 0. |
| 17 | How does Kafka achieve durability without fsync? | Through replication (`acks=all`): the message is in the page cache of all in-sync replicas. A single broker crash doesn't lose data because 2+ replicas still have it in RAM (and will flush to disk eventually). Only a correlated failure of all replicas would lose data. |
| 18 | What does `xfs_info <mountpoint>` show? | XFS filesystem parameters: block size, allocation group count, log size, inode size, and feature flags. Use it to verify `mkfs` parameters after formatting. Example: `xfs_info /data/kafka`. |
| 19 | What is `%iowait` in top/iostat CPU stats? | Percentage of CPU time spent idle while at least one process in the system is waiting for I/O. High iowait with low `%util` on disks suggests swap I/O or NFS I/O (which doesn't appear in iostat block device stats). |
| 20 | What command shows disk space AND inode usage? | `df -h` for disk space, `df -i` for inodes. Use `df -hi` to see both in human-readable format. Inode exhaustion (`IUse% = 100%`) causes "no space left on device" even when disk blocks are available. Fix: compact small files or use a filesystem with dynamic inodes (XFS). |

---

## 21. Module Summary

Storage is the substrate that Kafka, HDFS, and Spark sit on. Every throughput number, every durability guarantee, and every latency SLA ultimately depends on how well the Linux storage stack is configured and understood.

The **I/O stack** runs from application `write()` → page cache → block layer → I/O scheduler → device driver → disk. Understanding this stack explains why `write()` returns before data hits disk, why `fsync()` exists and when to use it, why Kafka doesn't need per-message fsync (replication provides the durability), and why wrong mount options silently cut throughput by 5-10×.

**`lsblk`** maps the physical device topology: which disks, partitions, LVM volumes, and RAID arrays feed each mountpoint. This is the starting point for any storage investigation — you need to know what you're measuring before you measure it. **`mount`** (and `/proc/mounts`) shows how each filesystem is configured. The critical Kafka options are `noatime,nodiratime,nobarrier,logbufs=8,allocsize=1g`. Missing any of these is silent but consequential.

**`iostat -xm 1`** is the primary diagnostic tool: `%util` for device utilization (meaningful for HDDs, less so for NVMe), `wareq-sz` for I/O pattern (large = sequential, small = random), `await` for latency (the user-visible impact), and `aqu-sz` for queue depth (the real saturation indicator for high-parallelism devices). **`iotop`** attributes I/O to specific processes, turning a device-level saturation observation into a process-level diagnosis.

**XFS** is the correct filesystem choice for data engineering workloads: per-AG concurrent allocation for Kafka's parallel partition writes, B-tree directories for Spark's small-file shuffle, fast `xfs_repair` for production recovery time, and speculative preallocation for sequential log growth. ext4 is the right choice for simplicity, boot volumes, and Ubuntu/Debian defaults.

---

**SYS-LNX-101: 3 of 5 complete.**  
**Next: SYS-LNX-101 M04 — Memory Diagnostics**  
*(free, vmstat, /proc/meminfo, OOM killer — understanding and diagnosing memory on Linux data servers)*
