# SYS-LNX-101 M01: Linux File System

**Course:** SYS-LNX-101 — Linux for Data Engineers  
**Module:** 01 of 05  
**Filesystem position:** 01_Systems/M22_Linux_File_System  
**Prerequisites:** CSF-OS-101 M04 (File I/O and System Calls)

---

## Table of Contents

1. [The Problem This Module Solves](#1-the-problem-this-module-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: How Linux Stores Files](#4-core-theory-how-linux-stores-files)
5. [Inodes: The File's Identity Card](#5-inodes-the-files-identity-card)
6. [Directory Entries and Path Resolution](#6-directory-entries-and-path-resolution)
7. [Hard Links and Soft Links](#7-hard-links-and-soft-links)
8. [POSIX Permissions: User, Group, Other](#8-posix-permissions-user-group-other)
9. [Special Permission Bits: Setuid, Setgid, Sticky](#9-special-permission-bits-setuid-setgid-sticky)
10. [Access Control Lists (ACLs)](#10-access-control-lists-acls)
11. [Extended Attributes and Immutable Files](#11-extended-attributes-and-immutable-files)
12. [Essential CLI Toolkit](#12-essential-cli-toolkit)
13. [Mental Models](#13-mental-models)
14. [Failure Scenarios](#14-failure-scenarios)
15. [Data Engineering Connections](#15-data-engineering-connections)
16. [Code Toolkit](#16-code-toolkit)
17. [Hands-On Labs](#17-hands-on-labs)
18. [Interview Q&A](#18-interview-qa)
19. [Cross-Question Chain](#19-cross-question-chain)
20. [Common Misconceptions](#20-common-misconceptions)
21. [Flashcards](#21-flashcards)
22. [Module Summary](#22-module-summary)

---

## 1. The Problem This Module Solves

A Spark job fails with `Permission denied` accessing a Parquet file on HDFS. The file exists — `ls` shows it — but the executor cannot read it. You stare at the permissions string `rw-r-----` and don't know what it means. You don't know whether the executor runs as the `spark` user or the `yarn` user, and you don't know whether the file's group needs to match. You change permissions to `777` to make it work, creating a security hole you don't fully understand, and move on.

An Airflow DAG directory fills up with `.pyc` files and leftover temp files from failed runs. You need to find all files older than 7 days and delete them. You know the answer involves `find` but you always have to Google the syntax because you've never understood how `find`'s `-mtime` flag works with respect to modification time vs access time vs change time — and you don't know the difference between those three timestamps.

A dbt project on a shared server produces model files that are readable by the `dbt` service account but not by the BI tool's service account, even though both accounts are in the `data` group. You discover that files are being created with a `umask` of `0022`, stripping group-write permission, but you don't know how to fix this for all new files created by the dbt process.

An Airflow deployment uses a shared NFS volume for DAG files. Files created by one team's deployment user aren't readable by the Airflow scheduler process. You reach for ACLs — a feature that grants permissions to a specific user without changing the file's owner or group — but you've never used `setfacl` before.

All of these are Linux file system problems. This module explains how Linux stores files (inodes, directory entries, block allocation), how permissions work at every level (POSIX rwx, setuid/setgid, umask, ACLs), and how to navigate and diagnose the file system with CLI tools.

---

## 2. How to Use This Module

**For production debugging:** Sections 8, 10, 14. `Permission denied` errors require understanding POSIX permissions (Section 8) and ACLs (Section 10). Section 14 covers the most common file permission failures in Spark, Airflow, and dbt deployments.

**For interview prep:** Sections 4, 5, 7, 18, 19. "What is an inode?" and "What is the difference between a hard link and a soft link?" are canonical Linux interview questions that appear at all levels.

**For daily CLI work:** Section 12. The `ls`, `stat`, `find`, `chmod`, `chown`, `getfacl`, `setfacl` command reference.

---

## 3. Prerequisites Check

- **VFS and file descriptors (CSF-OS-101 M04):** This module goes one level deeper than VFS — into the on-disk data structures (inodes, blocks) that the VFS abstracts. M04 explained the kernel's view; this module explains what's on disk.
- **Basic Linux CLI familiarity:** You should already know what a terminal is and how to run commands. This module assumes you can `cd`, `ls`, `cat`, and `cp`.

---

## 4. Core Theory: How Linux Stores Files

### 4.1 Block Devices and Filesystems

A filesystem is a layer that organizes raw storage (a disk, a partition, an LVM volume) into the familiar file-and-directory abstraction. The raw storage device provides a linear array of fixed-size **blocks** (typically 512 bytes on disk hardware, 4096 bytes as the filesystem's logical block size). The filesystem decides which blocks belong to which files and stores that mapping on the same device.

```
Physical view (raw block device):
  Block 0: [partition table / superblock]
  Block 1: [block group descriptor]
  Block 2: [block bitmap]
  Block 3: [inode bitmap]
  Blocks 4-N: [inode table]
  Blocks N+1-M: [data blocks]

Logical view (what applications see via VFS):
  /                         ← root directory
  /home/spark/              ← directory
  /home/spark/data.parquet  ← regular file
  /tmp/shuffle-0.bin        ← regular file
```

The **ext4** filesystem (the Linux default) divides the disk into **block groups** — each group contains its own bitmap of used/free blocks, its own inode table, and its own data blocks. This locality reduces seek time: a file's inode and its first data blocks are usually in the same block group, minimizing head movement on HDDs and keeping related pages in NVMe's spatial prefetcher.

### 4.2 The Superblock

Every filesystem has one **superblock** — a data structure near the start of the partition containing:
- Filesystem type (ext4, XFS, Btrfs)
- Total block count and free block count
- Total inode count and free inode count
- Block size (1 KB, 2 KB, 4 KB)
- Filesystem UUID
- Mount count and last check time
- Journal location

```bash
# View superblock information
sudo tune2fs -l /dev/sda1 | head -30

# Output:
# Filesystem magic number:  0xEF53  (ext4)
# Filesystem state:         clean
# Block count:              20971520
# Free blocks:              15234816
# Inode count:              5242880
# Free inodes:              5199001
# Block size:               4096
# Inode size:               256
# Journal size:             128M
# Last mount time:          Sat Jun 27 09:12:05 2026
```

The superblock is so critical that ext4 maintains **backup copies** in every block group. If the primary superblock is corrupted (bad sector), `fsck` can recover the filesystem from a backup.

### 4.3 Block Allocation: extents vs indirect blocks

**ext2/ext3** used a block pointer model: each inode contained 12 direct block pointers (pointing to data blocks directly), one single-indirect pointer (pointing to a block containing 1024 block pointers), one double-indirect, and one triple-indirect. For large files, navigating the indirect tree required multiple disk reads just to find data.

**ext4** uses **extents** — a run-length encoding of contiguous block ranges. Instead of storing 10,000 individual block pointers for a 40 MB file, an extent stores `(start_block=5000, length=10000)`. Extents reduce metadata overhead dramatically for large contiguous files (like Parquet column chunks or Kafka log segments) and speed up sequential I/O by enabling larger readahead.

```
ext4 extent tree in inode (simplified):
  inode.i_block[0-3] = extent header + 4 extents (for small files: inline extents)
  
  For a 1 GB Parquet file (sequential write):
    Extent 0: start=100000, length=262144  (1 GB = 262144 × 4KB blocks)
    → one extent describes the entire file
  
  For a 1 GB fragmented file (written piecemeal):
    Extent 0: start=100000, length=256
    Extent 1: start=200000, length=256
    ... (many extents = fragmentation = slower sequential read)
```

**XFS** (default on RHEL/CentOS, preferred for large files) also uses extents and adds a B-tree index for extents when a file has many of them. XFS handles large files (>1 GB) and high-concurrency I/O better than ext4, which is why Kafka and Spark deployments frequently use XFS volumes for data directories.

---

## 5. Inodes: The File's Identity Card

### 5.1 What an Inode Contains

An **inode** (index node) is a fixed-size data structure on disk that stores all metadata about one file, directory, or symlink. Every file has exactly one inode. The inode is identified by its **inode number** — a positive integer unique within the filesystem.

```bash
# See the inode number of a file
ls -i /var/log/syslog
# 1234567 /var/log/syslog

# See full inode contents
stat /var/log/syslog
# File: /var/log/syslog
# Size: 2458623        Blocks: 4808       IO Block: 4096   regular file
# Device: 8,1          Inode: 1234567     Links: 1
# Access: (0640/-rw-r-----)  Uid: (   0/root)   Gid: (   4/adm)
# Access: 2026-06-27 09:10:01.423891272 +0000  ← atime
# Modify: 2026-06-27 09:15:32.771234512 +0000  ← mtime
# Change: 2026-06-27 09:15:32.771234512 +0000  ← ctime
```

**What an inode contains:**
- File type (regular file `-`, directory `d`, symlink `l`, block device `b`, char device `c`, pipe `p`, socket `s`)
- Permissions (user/group/other rwx bits + setuid/setgid/sticky)
- Owner UID (numeric user ID)
- Owner GID (numeric group ID)
- File size in bytes
- Link count (number of hard links pointing to this inode)
- Three timestamps: atime (last access), mtime (last content modification), ctime (last metadata change)
- Block pointers / extents (where the file's data is on disk)
- Extended attributes pointer (for ACLs, SELinux labels, etc.)

**What an inode does NOT contain:**
- The filename — filenames live in directory entries, not inodes
- The file's path — a single inode can be reached by multiple paths (hard links)
- The file's data — data lives in data blocks; the inode only points to those blocks

### 5.2 The Three Timestamps

```bash
stat myfile.parquet
# Access: 2026-06-27 08:00:00  ← atime: last time any process called read() on this file
# Modify: 2026-06-25 14:30:00  ← mtime: last time the file's data was modified (write())
# Change: 2026-06-25 14:30:00  ← ctime: last time the inode was modified (chmod, chown, rename, write)

# KEY DISTINCTION:
# mtime: data changed (write to file content)
# ctime: metadata changed OR data changed
# atime: any read access

# ctime vs mtime example:
chmod 644 myfile.parquet   # changes inode metadata (permissions)
stat myfile.parquet
# Modify: 2026-06-25 14:30:00  ← UNCHANGED (file data not touched)
# Change: 2026-06-27 10:00:00  ← CHANGED (inode metadata changed by chmod)
```

**atime performance note:** Updating atime on every file read requires a disk write (to update the inode) for every read — even for read-only access patterns. On busy data systems, this doubles the I/O. Linux mount options mitigate this:

```bash
# /etc/fstab options:
# noatime:    never update atime (fastest; breaks applications that depend on atime)
# relatime:   only update atime if mtime > atime or atime is older than 1 day (default on Linux)
# strictatime: always update atime (slowest, POSIX-compliant)

# For Kafka, Spark data directories: use noatime
# /dev/sdb1  /data/kafka  xfs  noatime,nodiratime  0 0
```

### 5.3 Inode Exhaustion

The number of inodes is fixed at filesystem creation time. On ext4, the default is approximately one inode per 16 KB of space. A 1 TB filesystem with the default ratio has ~65 million inodes — usually sufficient. But a filesystem that stores millions of tiny files (like a Hive warehouse with small Parquet files, or a Python package cache) can exhaust inodes while disk space remains available.

```bash
# Check inode usage
df -i
# Filesystem      Inodes  IUsed   IFree IUse% Mounted on
# /dev/sda1      5242880 5242880      0  100% /data/warehouse
# → 0 free inodes → cannot create new files even if disk has space

# Find directories with most inodes used
find /data/warehouse -xdev -type f | awk -F'/' '{print NF-1, $0}' | \
  sort -n | tail -20

# Fix for the future: create filesystem with more inodes
# (must be done at mkfs time, cannot be changed after)
mkfs.ext4 -N 100000000 /dev/sdb1   # 100M inodes for small-file workload

# For Spark: consolidate small files before writing
# For Kafka: fewer topics with more partitions, not many tiny topics
```

---

## 6. Directory Entries and Path Resolution

### 6.1 Directories as Files

A **directory** is a special type of file whose data blocks contain a list of **(name, inode_number)** pairs — directory entries (dentries). The directory file itself has an inode, just like regular files.

```
/home/spark/ directory data block:

Entry 1: inode=2        name="."      (reference to itself)
Entry 2: inode=1        name=".."     (reference to parent: /home/)
Entry 3: inode=1234567  name="data.parquet"
Entry 4: inode=1234568  name="config.yaml"
Entry 5: inode=1234569  name="logs"   (a subdirectory)
```

`.` and `..` are always the first two entries in every directory. They are hard links — `.` is a hard link to the directory itself (which is why `stat .` shows Link count ≥ 2 for any directory: one from its parent, one from its own `.` entry, plus one for each subdirectory's `..` entry).

### 6.2 Path Resolution Step by Step

```
Resolving /home/spark/data.parquet:

1. Start at root inode (inode 2 by convention on ext4)
2. Look up "home" in root directory's data:
   → find entry {name="home", inode=N1}
   → read inode N1 (verify it's a directory, check execute permission)
3. Look up "spark" in inode N1's directory data:
   → find entry {name="spark", inode=N2}
   → read inode N2 (verify directory, check execute permission)
4. Look up "data.parquet" in inode N2's directory data:
   → find entry {name="data.parquet", inode=N3}
   → read inode N3 (verify regular file, check read permission)
5. Return inode N3 → open() creates a file struct pointing to this inode

Total disk reads (cold dcache):
  root inode + root data block +
  "home" inode + "home" data block +
  "spark" inode + "spark" data block +
  "data.parquet" inode
  = 7 reads

Warm dcache (all dentries cached in kernel): 0 disk reads (all from RAM)
```

**Execute permission on directories** (the `x` bit) means "permission to traverse" — you need execute permission on every directory in the path to reach a file. You can read a file (`r` on the file) without being able to list the directory (`r` on the dir), and you can list a directory without being able to traverse it (missing `x`).

---

## 7. Hard Links and Soft Links

### 7.1 Hard Links

A **hard link** is a directory entry that points to an existing inode. When you create a hard link, you add a new name→inode mapping. Both the original path and the hard link path point to the **same inode** — the same file data, the same permissions, the same timestamps.

```bash
# Create a hard link
ln /data/spark/output.parquet /backups/output_20260627.parquet

# Now both paths point to inode N:
ls -li /data/spark/output.parquet /backups/output_20260627.parquet
# 1234567 -rw-r--r-- 2 spark data 104857600 Jun 27 /data/spark/output.parquet
# 1234567 -rw-r--r-- 2 spark data 104857600 Jun 27 /backups/output_20260627.parquet
# ↑ same inode number              ↑ link count = 2

# Deleting the original: the inode is NOT freed
rm /data/spark/output.parquet
# Now only /backups/output_20260627.parquet exists, link count = 1
# inode N still exists, data blocks still allocated

# Deleting the last link: inode freed, data blocks returned to free pool
rm /backups/output_20260627.parquet
# Link count drops to 0 → inode freed → data blocks deallocated
```

**Hard link constraints:**
- Cannot hard-link across filesystems (inode numbers are only unique within a filesystem)
- Cannot hard-link to a directory (would allow directory cycles, breaking `find`, `du`, etc.; only the kernel itself uses directory hard links for `.` and `..`)
- The inode's link count tracks hard links; when it reaches 0, the data is freed

### 7.2 Symbolic Links (Soft Links)

A **symlink** (symbolic link) is a special file whose data content is a path string. When the kernel resolves a path containing a symlink, it substitutes the symlink's target path and continues resolution. The symlink has its own inode (separate from the target).

```bash
# Create a symlink
ln -s /data/spark/current_run /data/spark/latest
# Creates inode M (symlink type) with data = "/data/spark/current_run"

ls -li /data/spark/
# 1234567 -rw-r--r-- 1 spark data 104857600 Jun 27 output.parquet
# 1234999 lrwxrwxrwx 1 spark data        26 Jun 27 latest -> /data/spark/current_run
# ↑ symlink inode     ↑ permissions always 777 (actual check is on the target)

# Read through symlink: follows the link transparently
cat /data/spark/latest/output.parquet
# → resolves to: cat /data/spark/current_run/output.parquet

# Broken symlink: target doesn't exist
ln -s /data/spark/nonexistent /data/spark/broken
ls -l /data/spark/broken  # shows the symlink (it exists)
cat /data/spark/broken    # → No such file or directory

# Difference: hard links survive target deletion; symlinks break
```

### 7.3 Hard Link vs Symlink: When to Use Each

| Property | Hard Link | Symbolic Link |
|---|---|---|
| Has its own inode? | No (shares target's inode) | Yes (separate inode) |
| Survives target deletion? | Yes (data freed only when link count = 0) | No (becomes a broken link) |
| Can cross filesystems? | No | Yes |
| Can link directories? | No | Yes |
| Shows as different file type? | No (appears as regular file) | Yes (`l` type) |
| Follows on `ls -l`? | Transparent | Shown with `->` |

**Data engineering use cases:**
- **Hard links:** Kafka's log retention uses hard links to implement log cleaner atomicity. Atomic file replacement uses hard links for zero-copy "backup before overwrite."
- **Symlinks:** `current` → `v3.2.1` deployment patterns; Python virtual environment activation (`/usr/bin/python` → `python3.11`); Airflow DAG directories pointing to versioned DAG repositories; `/data/latest` → `/data/2026-06-27`.

---

## 8. POSIX Permissions: User, Group, Other

### 8.1 The Permission Triplets

Every file and directory has a 9-bit permission field divided into three triplets of **read (r=4), write (w=2), execute (x=1)**:

```
ls -l output.parquet
-rw-r--r-- 1 spark data 104857600 Jun 27 output.parquet
│├┤├┤├─┤
││ │ └── Other: r-- = read only
││ └──── Group: r-- = read only  
│└────── User:  rw- = read + write
└─────── File type: - = regular file
```

The numeric (octal) representation:
- `rw-r--r--` = 110 100 100 binary = 644 octal
- `rwxr-xr-x` = 111 101 101 binary = 755 octal
- `rwxrwxrwx` = 111 111 111 binary = 777 octal
- `rw-------` = 110 000 000 binary = 600 octal

**Permission semantics differ for files vs directories:**

| Bit | On a File | On a Directory |
|---|---|---|
| `r` | Can read file content (`cat`, `cp`, `open()`) | Can list entries (`ls`) |
| `w` | Can modify file content (`write()`, `echo >> file`) | Can create/delete/rename entries within it |
| `x` | Can execute the file as a program | Can traverse (use in a path: `cd`, `open("dir/file")`) |

```bash
# Common patterns:
chmod 644 config.yaml       # owner: rw, group+others: r  — readable by all, writable by owner
chmod 755 /opt/spark/bin/   # owner: rwx, group+others: rx — executable dir, readable scripts
chmod 700 /home/spark/.ssh/ # owner: rwx, others: none    — private directory
chmod 640 /etc/kafka/creds  # owner: rw, group: r, others: none — shared group read
chmod 600 ~/.ssh/id_rsa     # owner: rw, others: none     — SSH requires this; 644 fails
chmod 1777 /tmp             # sticky + rwxrwxrwx          — writable by all, delete own files only
```

### 8.2 Ownership and Groups

Every file has one **owner** (a UID) and one **group** (a GID). Permission checks work as follows:

```
For a process with UID=spark_uid, GIDs=[data_gid, users_gid] accessing file:
  Owner: spark  (UID=1001)
  Group: data   (GID=2001)
  Permissions: rw-r-----  (640)

Access check:
  1. Is process UID == file owner UID (1001)?  YES → apply owner permissions (rw-)
  2. (If no match) Is any process GID in file's group (2001)?  → apply group permissions (r--)
  3. (If no match) Apply other permissions (--)
  
IMPORTANT: The check stops at the first match.
  If the process IS the owner, only owner permissions apply — even if group permissions
  are more permissive. Being in the group does not additionally grant group permissions
  if you are the owner.
```

```bash
# Change owner
chown spark output.parquet           # change user only
chown spark:data output.parquet      # change user and group
chown :data output.parquet           # change group only (same as chgrp)
chown -R spark:data /data/output/    # recursive

# Change group
chgrp data output.parquet
chgrp -R data /data/output/

# Common pattern: make Spark output readable by the BI service account
chown spark:analytics output.parquet
chmod 640 output.parquet
# → spark can read+write; anyone in 'analytics' group can read; others cannot
```

### 8.3 umask: The Permission Subtracter

**`umask`** is a process-level mask that removes bits from the permissions of newly created files and directories. When a process creates a file with `open(..., O_CREAT, 0666)`, the kernel ANDs with `~umask` to produce the actual permissions:

```
actual_permissions = requested_permissions & ~umask

Example with umask=0022:
  File creation request: 0666 (rw-rw-rw-)
  ~umask: ~0022 = 7755 = 111 111 101 101 binary = 0755
  Result: 0666 & 0755 = 0644 (rw-r--r--)

  Directory creation request: 0777 (rwxrwxrwx)
  Result: 0777 & 0755 = 0755 (rwxr-xr-x)
```

Common umask values:
- `0022` — standard default: removes group-write and other-write. Files: 644. Dirs: 755.
- `0027` — security-conscious: removes group-write AND other-read/write/execute. Files: 640. Dirs: 750.
- `0002` — collaborative: removes only other-write. Files: 664. Dirs: 775. (groups can write)
- `0077` — paranoid: private files. Files: 600. Dirs: 700.

```bash
# Check current umask
umask
# 0022

# Set umask for current shell and all processes it spawns
umask 0027

# Set umask for a specific service (in systemd unit file):
# [Service]
# UMask=0027

# Set umask for Airflow workers:
# In airflow.cfg:
# [core]
# donot_pickle = True
# And set umask in the worker's launch script:
# export AIRFLOW__CORE__SECURITY_LEVEL=...
# Or set in systemd: UMask=0002 (so group can read new files)
```

**Practical problem:** A dbt service account writes model files with `umask=0022` → files are created `rw-r--r--` (644). The BI tool's service account is in the `data` group but other-read grants it access anyway. Then the team changes to `umask=0027` for security → BI tool can no longer read the files (other-read is now stripped). Fix: ensure dbt runs with `umask=0002` (group-writable) or `umask=0007` (group-readable) and the BI account is in the `data` group.

---

## 9. Special Permission Bits: Setuid, Setgid, Sticky

### 9.1 Setuid (SUID) — Run as File Owner

When the **setuid bit** is set on an executable file, the process that executes it runs with the file owner's UID rather than the executing user's UID.

```bash
ls -l /usr/bin/passwd
# -rwsr-xr-x 1 root root 59976 /usr/bin/passwd
#    ↑ s = setuid bit (replaces x in owner position)

# passwd needs to write /etc/shadow (owned by root, mode 640).
# Normal users can't write /etc/shadow.
# With setuid: when user runs passwd, the process's effective UID = root
# → the process can write /etc/shadow.

# Check for setuid files (security audit):
find / -perm -4000 -type f 2>/dev/null

# Set setuid on a file:
chmod u+s /opt/spark/bin/spark-class   # rarely done; security risk
chmod 4755 executable                   # 4 = setuid bit
```

**Data engineering relevance:** Setuid is a security concern, not a feature you configure. Finding unexpected setuid files is part of Linux security hardening for Kafka/Spark servers. A setuid root binary exploited by an attacker grants root.

### 9.2 Setgid (SGID) — Inherit Group on Directories

On **directories**, the setgid bit causes newly created files and subdirectories to inherit the **directory's group** rather than the creating process's primary group. This is the standard way to create shared directories in data engineering.

```bash
ls -ld /data/shared/
# drwxrwsr-x 2 root data 4096 /data/shared/
#       ↑ s = setgid bit

# Without setgid: each file inherits the creating user's primary group
# spark (primary group: spark) creates file → owned by spark:spark
# airflow (primary group: airflow) creates file → owned by airflow:airflow
# → files in the same directory have different groups → permissions break

# With setgid: all new files/dirs inherit the parent directory's group
# spark creates file → owned by spark:data
# airflow creates file → owned by airflow:data
# → all files in /data/shared/ are in 'data' group → consistent access

# Setting up a shared data directory:
mkdir /data/shared
chgrp data /data/shared
chmod 2775 /data/shared   # 2 = setgid, 775 = rwxrwxr-x
# Now: any file created in /data/shared/ by any user gets group=data
```

**This is the standard pattern for Spark output directories, dbt project directories, and Airflow DAG directories that multiple service accounts must access.**

### 9.3 Sticky Bit — Delete Own Files Only

When the **sticky bit** is set on a **directory**, only the file's owner (or root) can delete or rename a file within that directory — even if other users have write permission on the directory.

```bash
ls -ld /tmp
# drwxrwxrwt 20 root root 4096 /tmp
#          ↑ t = sticky bit

# Without sticky bit on /tmp:
# Anyone with write access to /tmp can delete anyone else's files.

# With sticky bit:
# User A creates /tmp/data_pipeline_abc.csv
# User B has write permission on /tmp but CANNOT delete A's file
# Only User A (owner) or root can delete it

# Practical use: shared pipeline staging directories
mkdir /data/pipeline_staging
chmod 1777 /data/pipeline_staging  # 1 = sticky, 777 = rwxrwxrwx
# → Any user can write to it; only the file owner or root can delete files
```

---

## 10. Access Control Lists (ACLs)

### 10.1 Why POSIX Permissions Are Sometimes Insufficient

POSIX permissions allow three permission subjects per file: owner, group, other. This is often not flexible enough:

```
Problem: /data/reports/quarterly.parquet
  owner: dbt_service (user account for dbt runs)
  group: data_team

  Requirements:
  - dbt_service: rw (write results)
  - data_team: r (read results)
  - bi_service: r (BI tool needs to read, but is NOT in data_team)
  - audit_service: r (compliance needs read access)
  - everyone else: no access

  POSIX cannot express this:
  - If we set "other: r" → everyone can read, not just bi_service/audit_service
  - If we change group from data_team to a super-group that includes everyone: messy, hard to maintain
  - Adding bi_service to data_team: gives them access to ALL data_team files, not just reports/
```

**ACLs** (Access Control Lists) extend POSIX permissions to allow per-user and per-group permissions beyond the standard owner/group/other triplet.

### 10.2 ACL Structure

An ACL is a list of entries, each specifying a subject (user or group) and a permission set (rwx):

```
Default POSIX: (stored in inode)
  user::rw-       (owner permissions)
  group::r--      (group permissions)
  other::---      (other permissions)

With ACL: (stored in extended attributes of the inode)
  user::rw-              (file owner: read+write)
  user:bi_service:r--    (specific user bi_service: read-only)
  user:audit_service:r-- (specific user audit_service: read-only)
  group::r--             (file group data_team: read-only)
  group:ops_team:---     (specific group ops_team: no access)
  mask::r--              (ACL mask: maximum permission for any named ACL entry)
  other::---             (everyone else: no access)
```

### 10.3 ACL Commands

```bash
# Install ACL tools (if not present)
apt-get install acl            # Debian/Ubuntu
yum install acl                # RHEL/CentOS

# Check if filesystem is mounted with ACL support
mount | grep /data
# /dev/sdb1 on /data type xfs (rw,relatime,attr2,inode64,logbufs=8,logbsize=32k,noquota)
# XFS supports ACLs by default
# ext4: add "acl" mount option if not present: /dev/sdb1 /data ext4 defaults,acl 0 0

# View ACLs on a file
getfacl /data/reports/quarterly.parquet
# file: data/reports/quarterly.parquet
# owner: dbt_service
# group: data_team
# user::rw-
# group::r--
# other::---

# Add ACL entry: grant bi_service read access
setfacl -m u:bi_service:r /data/reports/quarterly.parquet
# -m = modify (add or update)
# u: = user entry
# bi_service = the user
# r = read permission

# Add group ACL
setfacl -m g:audit_team:r /data/reports/quarterly.parquet

# Verify
getfacl /data/reports/quarterly.parquet
# user::rw-
# user:bi_service:r--
# group::r--
# group:audit_team:r--
# mask::r--         ← automatically created; limits named entries
# other::---

# Remove a specific ACL entry
setfacl -x u:bi_service /data/reports/quarterly.parquet

# Remove ALL ACLs (revert to POSIX permissions only)
setfacl -b /data/reports/quarterly.parquet

# Recursive ACL: apply to directory and all existing contents
setfacl -R -m u:bi_service:rX /data/reports/
# X (capital X) = execute only if already has execute bit OR is a directory
# prevents accidentally making regular files executable

# Default ACLs: inherit by new files/dirs created inside this directory
setfacl -m d:u:bi_service:r /data/reports/
# d: = default ACL entry
# Any new file created in /data/reports/ automatically gets bi_service:r
```

### 10.4 The ACL Mask

The **mask** entry in an ACL defines the maximum effective permissions for all **named** ACL entries (named users and named groups). The file owner and other permissions are not affected by the mask.

```
File ACL:
  user::rw-         (owner — NOT masked)
  user:bi_service:rwx   (named user: wants rwx)
  group::r--         (file group — IS masked)
  mask::r--          (mask = r only)
  other::---         (other — NOT masked)

Effective permissions:
  bi_service effective: rwx & r-- (mask) = r-- (read only, despite ACL saying rwx)
  group effective:      r-- & r-- (mask) = r--
```

The mask is automatically updated when you use `setfacl -m` to add entries. The mask equals the union of all group and named-user permissions. You can manually set it: `setfacl -m m::rx /data/file` — but this is rarely needed.

**Why the mask matters:** `chmod g+x file` on a file with ACLs modifies the mask, not the group entry. This can silently restrict named ACL entries. Check with `getfacl` after `chmod` operations on ACL-enabled files.

---

## 11. Extended Attributes and Immutable Files

### 11.1 Extended Attributes (xattr)

Extended attributes store arbitrary name-value metadata associated with a file, beyond the standard inode fields. There are four namespaces:

- `user.*` — application-defined attributes (requires the filesystem to be mounted with user_xattr)
- `system.*` — kernel-managed (ACLs are stored as `system.posix_acl_access`)
- `security.*` — SELinux labels, AppArmor labels
- `trusted.*` — root-only attributes

```bash
# Set a custom extended attribute
setfattr -n user.pipeline_version -v "v3.2.1" /data/output/results.parquet
setfattr -n user.source_job -v "spark_etl_20260627" /data/output/results.parquet

# Read extended attributes
getfattr -d /data/output/results.parquet
# file: data/output/results.parquet
# user.pipeline_version="v3.2.1"
# user.source_job="spark_etl_20260627"

# SELinux context (security namespace)
ls -Z /data/kafka/server.log
# -rw-r--r--. root root system_u:object_r:var_log_t:s0 /data/kafka/server.log
#                       ↑ SELinux security context label
```

### 11.2 Immutable and Append-Only Files

The `chattr` command sets special file attributes beyond standard permissions:

```bash
# Make a file immutable (even root cannot modify or delete it)
chattr +i /etc/kafka/server.properties   # +i = immutable
# Immutable: cannot rename, delete, hard link, or write to the file
# Even root is blocked at the VFS layer

# Check attributes
lsattr /etc/kafka/server.properties
# ----i--------e-- /etc/kafka/server.properties
# ↑ i = immutable, e = uses extents

# Remove immutable flag (requires root)
chattr -i /etc/kafka/server.properties

# Append-only (can only add data to end; cannot overwrite or delete)
chattr +a /var/log/pipeline_audit.log
# +a = append only: write() only works at EOF; O_WRONLY|O_TRUNC fails
# Useful for audit logs that must not be tampered with

lsattr /var/log/pipeline_audit.log
# -----a-------e-- /var/log/pipeline_audit.log

# Other useful chattr flags:
# +A: don't update atime (per-file, avoids mount option changes)
# +s: secure delete (overwrite with zeros on unlink)
# +u: undeletable (save data for recovery)
# +d: don't include in dump backups
```

**Data engineering use:** `chattr +a` on Kafka log directories prevents accidental truncation of commit logs. `chattr +i` on critical configuration files (Spark master's `slaves` file, Kafka's `server.properties`) protects against accidental modification during incident response.

---

## 12. Essential CLI Toolkit

### 12.1 Listing and Inspecting Files

```bash
# Long listing with inode numbers
ls -lai /data/kafka/
# total 48
# 524288 drwxr-xr-x 3 kafka kafka 4096 Jun 27 .
# 524289 drwxr-xr-x 8 root  root  4096 Jun 27 ..
# 524300 -rw-r--r-- 1 kafka kafka 8192 Jun 27 00000000000000000000.index
# 524301 -rw-r--r-- 1 kafka kafka 1048576000 Jun 27 00000000000000000000.log
# ls flags: -l long, -a all (including hidden), -i show inode number
# -h human-readable sizes: ls -lah
# -S sort by size: ls -lS
# -t sort by mtime: ls -lt
# -u show atime: ls -lu
# -c show ctime: ls -lc

# Full file metadata
stat /data/kafka/00000000000000000000.log
# File: /data/kafka/00000000000000000000.log
# Size: 1073741824  Blocks: 2097152  IO Block: 4096  regular file
# Device: 8,1  Inode: 524301  Links: 1
# Access: (0644/-rw-r--r--)  Uid: (1000/kafka)  Gid: (1000/kafka)
# Access: 2026-06-27 09:00:00.000000000
# Modify: 2026-06-27 09:15:32.771234512
# Change: 2026-06-27 09:15:32.771234512

# Which filesystem type is this on?
df -T /data/kafka/
# Filesystem     Type  1K-blocks    Used Available Use% Mounted on
# /dev/sdb1      xfs   209715200 4194304 205520896   2% /data/kafka
```

### 12.2 find — The Essential Search Tool

```bash
# Find by name
find /data/warehouse -name "*.parquet"
find /data/warehouse -name "part-*.snappy.parquet"

# Find by type
find /tmp -type f        # regular files only
find /tmp -type d        # directories only
find /tmp -type l        # symlinks only

# Find by time (crucial distinction):
# -mtime: modification time of file DATA
# -ctime: change time of inode (any metadata change, including rename/chmod)
# -atime: access time (last read)
# N = exactly N days ago; +N = more than N days ago; -N = less than N days ago

find /data/pipeline/tmp -mtime +7 -type f   # data files not modified in >7 days
find /var/log -mtime -1 -name "*.log"       # log files modified in last 24 hours
find /tmp -ctime +1 -type f                 # temp files with inode changed >1 day ago

# Find by size
find /data -size +1G -type f                # files larger than 1 GB
find /data -size +100M -size -500M          # files between 100MB and 500MB
# Suffixes: c=bytes, k=KB, M=MB, G=GB

# Find by permissions
find /data -perm 777 -type f                # world-writable files (security risk)
find /data -perm -006 -type f               # files with other read+write (insecure)
find / -perm -4000 -type f 2>/dev/null      # setuid files

# Find by owner
find /data/spark -user spark -type f
find /data -user spark -group data

# Find and execute (xargs for performance)
find /data/tmp -mtime +7 -type f -print0 | xargs -0 rm -f
# -print0 and -0: null-delimited (handles filenames with spaces)

# Find and execute directly (-exec is simpler but slower: one process per file)
find /data/tmp -mtime +7 -type f -exec rm {} \;   # slow: one rm per file
find /data/tmp -mtime +7 -type f -exec rm {} +    # faster: batches files into one rm call

# Find large files consuming disk space
find /data -type f -size +100M -printf '%s\t%p\n' | sort -n | tail -20
# Prints: size_bytes  path, sorted ascending
```

### 12.3 Disk Usage

```bash
# Overall disk usage
df -h
# Filesystem      Size  Used Avail Use% Mounted on
# /dev/sda1        50G   20G   30G  40% /
# /dev/sdb1       200G  180G   20G  90% /data/kafka   ← getting full

# Inode usage
df -i
# Filesystem      Inodes  IUsed   IFree IUse% Mounted on
# /dev/sdc1      6553600 6553599       1  100% /data/warehouse ← inode exhausted!

# Directory sizes (finds disk hogs)
du -sh /data/*                     # summary per top-level directory
du -sh /data/*/* | sort -h         # summary per second level, sorted by size
du -sh --max-depth=2 /data/        # same as above

# Find top 20 largest directories
du -h /data | sort -h | tail -20

# Specific directory tree
du -sh /data/spark/shuffle/        # how much is this shuffle data?
```

### 12.4 chmod / chown / chgrp Reference

```bash
# Symbolic mode (relative: add/remove/set)
chmod u+x script.sh              # add execute for user
chmod g-w config.yaml            # remove write for group
chmod o= secrets.env             # remove all permissions for other
chmod a+r *.parquet              # add read for all (user, group, other)
chmod u=rwx,g=rx,o= private.sh   # set exact permissions

# Numeric mode (octal: absolute set)
chmod 644 config.yaml            # rw-r--r--
chmod 755 /opt/scripts/          # rwxr-xr-x
chmod 700 ~/.ssh/                # rwx------
chmod 640 /etc/kafka/creds.properties  # rw-r-----

# Recursive (use carefully on production data)
chmod -R 644 /data/output/       # sets all files to 644 (wrong for dirs: loses x)
chmod -R u=rwX,g=rX,o= /data/   # correct: X = execute only if dir or already executable

# Change owner
chown kafka /data/kafka/server.log
chown kafka:kafka /data/kafka/
chown -R spark:data /data/spark-output/

# ACL reference
setfacl -m u:bi_service:r-- file.parquet     # add user ACL
setfacl -m g:analytics:r-x dir/              # add group ACL
setfacl -m d:g:analytics:r-x dir/            # add default ACL (inherited by new files)
setfacl -R -m u:bi_service:rX /data/reports/ # recursive, capital X for dirs
setfacl -x u:bi_service file.parquet         # remove one ACL entry
setfacl -b file.parquet                      # remove all ACLs
getfacl file.parquet                         # show all ACLs
```

---

## 13. Mental Models

### 13.1 The Filing Cabinet: Inode = Folder Contents, Directory Entry = Label on the Tab

A physical filing cabinet has folders (the data). Each folder has contents (the file data) and a metadata card on the tab (the inode: what's inside, date created, who owns it). The filing cabinet's index (the directory) maps tab labels (filenames) to folder locations. You can put two labels on the same folder (hard links) — both labels refer to the same folder with the same contents. If you remove one label, the folder still exists. Remove the last label, the folder is discarded.

A symlink is a sticky note on one tab that says "see tab XXXX." Follow the arrow to find the real folder. If tab XXXX is removed, the sticky note becomes useless (broken symlink).

### 13.2 Permissions as a Bouncer with Three Questions

When a process tries to access a file, the kernel is a bouncer asking three questions in order: "Are you the owner?" (check owner bits). "Are you in the group?" (check group bits). "Are you someone else?" (check other bits). The bouncer stops at the first YES and applies those rules — even if later rules would be more permissive. A root user bypasses the bouncer entirely (UID 0 skips permission checks for most operations).

### 13.3 umask as a Permission Eraser

When you create a file, you request permissions. umask is a stencil that erases certain bits from your request. umask 022 erases the "group write" and "other write" bits from any request. If you request `rw-rw-rw-` (666), the eraser removes the two write bits it targets, leaving `rw-r--r--` (644). You cannot ADD permissions with umask — you can only remove them.

---

## 14. Failure Scenarios

### 14.1 `Permission denied` on a Spark Output Directory

```
Symptom: Spark job fails with:
  org.apache.hadoop.security.AccessControlException: Permission denied:
  user=spark, access=WRITE, inode="/data/output":root:supergroup:drwxr-xr-x

Diagnosis:
  hdfs dfs -ls /data/output
  # drwxr-xr-x   - root supergroup    /data/output
  # → directory owned by root:supergroup, permissions 755
  # → user 'spark' is not root, not in supergroup, and 'other' has no write

  hdfs dfs -stat %u/%g/%a /data/output
  # root/supergroup/755

Fix:
  # Option A: Change owner to spark
  hdfs dfs -chown spark:spark /data/output
  
  # Option B: Make the directory group-writable and add spark to the group
  hdfs dfs -chgrp data_engineers /data/output
  hdfs dfs -chmod 775 /data/output
  # Ensure 'spark' user is member of 'data_engineers' group on NameNode

  # Option C: Add ACL for spark user
  hdfs dfs -setfacl -m u:spark:rwx /data/output
```

### 14.2 Airflow DAG Not Picked Up by Scheduler

```
Symptom: New DAG file placed in $AIRFLOW_HOME/dags/ but not appearing in the UI.
  Airflow scheduler logs: PermissionError: [Errno 13] Permission denied: '/opt/airflow/dags/my_dag.py'

Diagnosis:
  ls -la /opt/airflow/dags/my_dag.py
  # -rw------- 1 developer developer 2048 Jun 27 my_dag.py
  # → permissions 600: only owner (developer) can read
  # → Airflow scheduler runs as 'airflow' user: cannot read the file

  id airflow
  # uid=1001(airflow) gid=1001(airflow) groups=1001(airflow)
  # airflow is not in any group that can read the file

Fix:
  # Fix permissions: group-readable at minimum
  chmod 644 /opt/airflow/dags/my_dag.py
  
  # Or set the DAG directory setgid so new files inherit the group:
  chgrp airflow /opt/airflow/dags/
  chmod 2775 /opt/airflow/dags/    # setgid: new files inherit 'airflow' group
  umask 0002                        # in developer's shell/deploy script

  # Or set default ACL so airflow can always read new files:
  setfacl -m d:u:airflow:r-x /opt/airflow/dags/
  setfacl -R -m u:airflow:r-x /opt/airflow/dags/  # fix existing files too
```

### 14.3 `No space left on device` Despite Available Disk Space

```
Symptom: Python/Spark job fails with: OSError: [Errno 28] No space left on device
  But df -h shows 40% disk space available.

Diagnosis:
  df -i   # check inode usage
  # Filesystem      Inodes  IUsed  IFree IUse% Mounted on
  # /dev/sdc1      1048576 1048575     1  100% /data/hive_warehouse
  # → 100% inode usage, 0 free inodes

  # Find directories with most files (inode consumers):
  find /data/hive_warehouse -type f | awk -F'/' 'BEGIN{OFS="/"} {path=""; for(i=1;i<=NF-1;i++) path=path"/"$i; counts[path]++} END{for(p in counts) print counts[p], p}' | sort -rn | head -20

Root cause: Hive table with thousands of small partitions (e.g., hourly partitioned table
  with 5 years of data = 43,800 partitions, each with multiple small files).
  Each file = one inode. 1M inodes exhausted.

Fix short-term:
  # Compact small files into fewer large files
  spark.sql("INSERT OVERWRITE TABLE hive_db.events PARTITION(date, hour) SELECT * FROM hive_db.events")
  # This rewrites data into fewer, larger files

Fix long-term:
  # Delete old partitions: ALTER TABLE ... DROP PARTITION (date < '2024-01-01')
  # Switch to Iceberg/Delta: manifest files consolidate metadata, fewer filesystem inodes
  # Increase inode count: requires reformatting (plan ahead at mkfs time)
  mkfs.ext4 -T largefile4 /dev/sdc   # optimized for large files, fewer inodes
  mkfs.ext4 -N 10000000 /dev/sdc    # explicit inode count: 10M inodes
```

### 14.4 Files Disappear After Cron Job Runs

```
Symptom: Nightly cleanup cron job deletes files that should not have been deleted.

Cron job:
  0 2 * * * find /data/tmp -mtime +1 -type f -delete

Actual behavior: deleted all files not modified in >24 hours. But "not modified" is
  measured from when the job runs (2am), not from a calendar boundary. A file
  created at 11pm the previous day is only 3 hours old — but if the cron ran
  at 2am the day AFTER that, it's 27 hours old → deleted.
  
Also common: confusing -mtime (modification time of data) with -ctime (inode change
  time including renames). A file's data may be unchanged (-mtime shows old) but its
  inode was changed by a chmod → -ctime is recent. Using -mtime misses recently-chmod'd files.

Fix: be explicit about what "old" means.
  # Delete files whose DATA has not changed in >7 days AND whose INODE has not changed
  find /data/tmp -mtime +7 -ctime +7 -type f -delete

  # For staging areas: delete files older than a specific timestamp, not relative time
  find /data/pipeline/staging -not -newer /tmp/pipeline_start_marker -type f -delete
  # Create the marker: touch -t 202606270200 /tmp/pipeline_start_marker

  # Use -ls before -delete to preview what would be deleted:
  find /data/tmp -mtime +7 -type f -ls
  # Review output, then re-run with -delete
```

---

## 15. Data Engineering Connections

### 15.1 HDFS Permissions and the Hadoop Supergroup

HDFS mirrors POSIX permissions but with Hadoop-specific extensions:

```bash
# HDFS uses the same UGO model (User, Group, Other)
hdfs dfs -ls /user/spark/
# -rw-r--r--   3 spark supergroup 104857600 /user/spark/output.parquet
# ↑ replication factor (not a link count)

# HDFS supergroup: users in this group can bypass permission checks
# Default: 'supergroup' (configured in core-site.xml: hadoop.supergroup)
# Used by: HDFS administrative operations, cluster management

# HDFS sticky bit: same semantics as Linux
hdfs dfs -chmod 1777 /tmp/hive/         # world-writable, sticky

# HDFS ACLs (must be enabled in hdfs-site.xml: dfs.namenode.acls.enabled=true)
hdfs dfs -setfacl -m user:bi_service:r-x /data/reports/
hdfs dfs -getfacl /data/reports/

# HDFS umask (set in hadoop env, not shell umask)
# hdfs-site.xml: <property><name>fs.permissions.umask-mode</name><value>022</value></property>
```

### 15.2 S3 and Object Storage: No POSIX Permissions

S3 (and GCS, Azure Blob) does not have POSIX permissions, inodes, or hard links. Object storage uses:
- **Bucket policies** (JSON-based IAM policies controlling access to the entire bucket or key prefixes)
- **Object ACLs** (per-object ACLs, now mostly deprecated in favor of bucket policies)
- **IAM roles** (the primary access control mechanism in production)

```bash
# S3 has no 'chmod' or 'chown' — there are no file owners in the POSIX sense
# Access is controlled by IAM policies attached to the accessing principal (user/role)

# Common data engineering IAM pattern:
# - spark_execution_role: s3:GetObject, s3:PutObject on s3://data-lake/
# - bi_read_role: s3:GetObject on s3://data-lake/gold/
# - data_engineer_role: s3:* on s3://data-lake/

# When your Spark job on EMR/Glue says "Access Denied" on S3:
# → check the EC2 instance profile (which role the EMR cluster uses)
# → check the bucket policy (does it explicitly deny the role?)
# → check S3 Block Public Access settings (if trying to make files "public")
# → check KMS key policy (if bucket uses SSE-KMS encryption)
```

### 15.3 dbt File Ownership and the Shared Directory Problem

```bash
# Common dbt deployment: multiple team members push models from CI/CD
# Each CI runner runs as a different system user (ubuntu, jenkins, deploy_bot)
# dbt writes compiled models and artifacts to a shared directory

# Problem without setgid:
ls -la /opt/dbt/target/
# -rw-r--r-- 1 ubuntu  ubuntu  102400 Jun 27 09:00 run_results.json  ← from CI run #1
# -rw-r--r-- 1 jenkins jenkins 98304  Jun 27 10:00 manifest.json     ← from CI run #2
# dbt service (runs as 'dbt_user') cannot write 'manifest.json' because owner is 'jenkins'

# Fix: shared directory with setgid + group membership
groupadd dbt_team
usermod -aG dbt_team ubuntu
usermod -aG dbt_team jenkins
usermod -aG dbt_team dbt_user

mkdir -p /opt/dbt/target
chgrp dbt_team /opt/dbt/target
chmod 2775 /opt/dbt/target    # setgid: new files inherit dbt_team group

# Now:
# ubuntu runs CI → creates manifest.json owned by ubuntu:dbt_team (mode 664)
# jenkins runs CI → creates manifest.json owned by jenkins:dbt_team (mode 664)
# dbt_user runs scheduler → can write to manifest.json (group dbt_team has w)
```

### 15.4 Kafka Log Directory Permissions

```bash
# Kafka broker runs as 'kafka' user, group 'kafka'
# Log directory must be owned by kafka:kafka with mode 700 or 750
# Typical setup:

ls -la /data/kafka/logs/my_topic-0/
# total 1048588
# drwxr-x--- 2 kafka kafka       4096 Jun 27 /data/kafka/logs/my_topic-0/
# -rw-r----- 1 kafka kafka 1073741824 Jun 27 00000000000000000000.log
# -rw-r----- 1 kafka kafka  10485760 Jun 27 00000000000000000000.index
# -rw-r----- 1 kafka kafka  10485760 Jun 27 00000000000000000000.timeindex

# Why 640 (rw-r-----) on log files instead of 644:
# Security: log files may contain sensitive data from producers
# Only 'kafka' user (broker) can write; group 'kafka' can read (monitoring agents)
# 'other' has no access

# Monitoring agents (prometheus kafka_exporter) run as 'prometheus' user:
# Add to kafka group: usermod -aG kafka prometheus
# → prometheus can read log files for metric collection

# Pre-allocate log segments using fallocate: owned by kafka, mode 640
# Done automatically by Kafka broker using its uid/gid
```

---

## 16. Code Toolkit

### 16.1 `fs_inspector.py` — Filesystem Metadata Analysis

```python
"""
fs_inspector.py — Inspect filesystem metadata for data engineering diagnostics.

Functions:
  1. inspect_file()    — full inode metadata (like stat, plus ACLs)
  2. find_large_files() — locate files above a size threshold
  3. find_old_files()  — locate files by mtime/ctime/atime age
  4. check_permissions() — validate expected permissions and ownership
  5. count_inodes()    — count inode usage per directory subtree
  6. check_acls()      — display and validate ACL entries

Run directly to demonstrate each function on /tmp test files.
"""
from __future__ import annotations
import os
import pwd
import grp
import stat
import time
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Iterator


def inspect_file(path: str) -> dict:
    """
    Return full inode metadata for a file.
    Equivalent to `stat` command output plus ACL summary.
    """
    st = os.stat(path, follow_symlinks=False)
    
    # Resolve numeric uid/gid to names
    try:
        owner = pwd.getpwuid(st.st_uid).pw_name
    except KeyError:
        owner = str(st.st_uid)
    
    try:
        group = grp.getgrgid(st.st_gid).gr_name
    except KeyError:
        group = str(st.st_gid)
    
    # File type
    mode = st.st_mode
    if stat.S_ISREG(mode):
        ftype = "regular file"
    elif stat.S_ISDIR(mode):
        ftype = "directory"
    elif stat.S_ISLNK(mode):
        ftype = "symbolic link"
    elif stat.S_ISFIFO(mode):
        ftype = "pipe"
    elif stat.S_ISSOCK(mode):
        ftype = "socket"
    else:
        ftype = "other"
    
    # Permission string
    perm_str = stat.filemode(mode)
    perm_octal = oct(mode & 0o7777)[2:]
    
    return {
        "path": str(path),
        "inode": st.st_ino,
        "type": ftype,
        "permissions": perm_str,
        "permissions_octal": perm_octal,
        "owner": owner,
        "owner_uid": st.st_uid,
        "group": group,
        "group_gid": st.st_gid,
        "size_bytes": st.st_size,
        "link_count": st.st_nlink,
        "atime": datetime.fromtimestamp(st.st_atime).isoformat(),
        "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(),
        "ctime": datetime.fromtimestamp(st.st_ctime).isoformat(),
        "device": st.st_dev,
        "blocks": st.st_blocks,
        "block_size": st.st_blksize if hasattr(st, 'st_blksize') else 4096,
    }


def find_large_files(
    root: str,
    min_size_mb: float = 100,
    max_results: int = 20
) -> list[tuple[int, str]]:
    """
    Find files larger than min_size_mb under root.
    Returns list of (size_bytes, path) sorted largest first.
    """
    threshold = int(min_size_mb * 1024 * 1024)
    results: list[tuple[int, str]] = []
    
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        # Skip inaccessible directories
        dirnames[:] = [d for d in dirnames
                       if os.access(os.path.join(dirpath, d), os.R_OK)]
        
        for fname in filenames:
            fpath = os.path.join(dirpath, fname)
            try:
                size = os.path.getsize(fpath)
                if size >= threshold:
                    results.append((size, fpath))
            except (OSError, PermissionError):
                pass
    
    results.sort(key=lambda x: x[0], reverse=True)
    return results[:max_results]


def find_old_files(
    root: str,
    older_than_days: int = 7,
    time_type: str = "mtime",   # "mtime", "ctime", or "atime"
    extensions: list[str] | None = None
) -> Iterator[tuple[float, str]]:
    """
    Yield (age_days, path) for files older than older_than_days.
    
    time_type:
      "mtime" — file data not modified for N days
      "ctime" — inode not changed (metadata or data) for N days
      "atime" — file not accessed for N days
    
    extensions: filter by extension (e.g., [".tmp", ".log"])
    """
    cutoff = time.time() - (older_than_days * 86400)
    
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = [d for d in dirnames
                       if os.access(os.path.join(dirpath, d), os.R_OK)]
        
        for fname in filenames:
            if extensions and not any(fname.endswith(ext) for ext in extensions):
                continue
            
            fpath = os.path.join(dirpath, fname)
            try:
                st = os.stat(fpath, follow_symlinks=False)
                t = {"mtime": st.st_mtime, "ctime": st.st_ctime, "atime": st.st_atime}[time_type]
                if t < cutoff:
                    age_days = (time.time() - t) / 86400
                    yield (age_days, fpath)
            except (OSError, PermissionError):
                pass


def check_permissions(
    path: str,
    expected_mode: int,      # e.g., 0o644
    expected_owner: str | None = None,
    expected_group: str | None = None,
) -> dict:
    """
    Check that a file has the expected permissions, owner, and group.
    Returns a dict with 'ok' bool and 'issues' list.
    """
    issues = []
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return {"ok": False, "issues": [f"File not found: {path}"]}
    except PermissionError:
        return {"ok": False, "issues": [f"Cannot stat {path}: Permission denied"]}
    
    actual_mode = stat.S_IMODE(st.st_mode)
    if actual_mode != expected_mode:
        issues.append(
            f"Mode: expected {oct(expected_mode)}, got {oct(actual_mode)} "
            f"({stat.filemode(st.st_mode)})"
        )
    
    if expected_owner:
        try:
            actual_owner = pwd.getpwuid(st.st_uid).pw_name
        except KeyError:
            actual_owner = str(st.st_uid)
        if actual_owner != expected_owner:
            issues.append(f"Owner: expected '{expected_owner}', got '{actual_owner}'")
    
    if expected_group:
        try:
            actual_group = grp.getgrgid(st.st_gid).gr_name
        except KeyError:
            actual_group = str(st.st_gid)
        if actual_group != expected_group:
            issues.append(f"Group: expected '{expected_group}', got '{actual_group}'")
    
    return {"ok": len(issues) == 0, "issues": issues, "path": path}


def count_inodes(root: str, max_depth: int = 3) -> dict[str, int]:
    """
    Count the number of inodes (files + directories) under each subdirectory.
    Useful for finding inode-heavy directories.
    Returns dict of {path: count} for the top-level subdirectories.
    """
    counts: dict[str, int] = {}
    
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        depth = dirpath.replace(root, "").count(os.sep)
        if depth >= max_depth:
            dirnames.clear()   # don't recurse deeper
            continue
        
        if depth == 1:
            # Count total files under each first-level subdirectory
            subdir = dirpath
            total = 0
            for _, _, files in os.walk(dirpath):
                total += len(files)
            counts[subdir] = total
    
    return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))


def get_acls(path: str) -> list[str]:
    """
    Get ACL entries for a file using getfacl command.
    Returns list of ACL entry strings, or empty list if no ACL support.
    """
    try:
        result = subprocess.run(
            ["getfacl", "--omit-header", path],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return [line for line in result.stdout.splitlines() if line and not line.startswith("#")]
        return []
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []   # getfacl not installed


# ── Demo ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import tempfile, json

    print("=== Filesystem Inspector Demo ===\n")
    
    # Create a test file
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False, mode="wb") as f:
        f.write(b"PAR1" + b"\x00" * 1000)
        test_path = f.name
    
    # 1. Inspect file
    print("1. File metadata:")
    info = inspect_file(test_path)
    for k, v in info.items():
        print(f"   {k:<20} {v}")
    
    # 2. Check permissions
    print("\n2. Permission check (expect 600 on tempfile):")
    result = check_permissions(test_path, 0o600)
    print(f"   OK: {result['ok']}")
    if result['issues']:
        for issue in result['issues']:
            print(f"   ISSUE: {issue}")
    
    # 3. Check wrong permissions
    os.chmod(test_path, 0o777)
    result = check_permissions(test_path, 0o644)
    print(f"\n3. After chmod 777, checking for 644:")
    print(f"   OK: {result['ok']}")
    for issue in result['issues']:
        print(f"   ISSUE: {issue}")
    
    # 4. Find large files in /tmp
    print(f"\n4. Large files in /tmp (>1 MB):")
    large = find_large_files("/tmp", min_size_mb=1, max_results=5)
    for size, path in large:
        print(f"   {size/1024/1024:.1f} MB  {path}")
    if not large:
        print("   (none found)")
    
    # 5. ACLs
    print(f"\n5. ACLs on test file:")
    acls = get_acls(test_path)
    if acls:
        for entry in acls:
            print(f"   {entry}")
    else:
        print("   (no ACLs or getfacl not available)")
    
    os.unlink(test_path)
    print("\nDone.")
```

---

## 17. Hands-On Labs

### Lab 1: Explore an Inode

```bash
# Lab 1: examine what an inode contains and what it does NOT contain

# Create a test file
echo "hello world" > /tmp/test_inode.txt

# Step 1: find the inode number
ls -i /tmp/test_inode.txt
# e.g.: 1234567 /tmp/test_inode.txt

# Step 2: view full inode
stat /tmp/test_inode.txt
# Observe: size, permissions, link count, three timestamps

# Step 3: create a hard link — same inode, different name
ln /tmp/test_inode.txt /tmp/test_inode_link.txt
ls -li /tmp/test_inode.txt /tmp/test_inode_link.txt
# Both show the SAME inode number, link count = 2

# Step 4: prove data is shared
echo "appended" >> /tmp/test_inode.txt
cat /tmp/test_inode_link.txt   # shows "hello world\nappended" — same file

# Step 5: delete original — data persists via hard link
rm /tmp/test_inode.txt
cat /tmp/test_inode_link.txt   # still works! link count now = 1

# Step 6: delete last link — data gone
rm /tmp/test_inode_link.txt
# inode freed, data blocks returned to free pool

# Step 7: create a symlink and observe the difference
ln -s /tmp/target_does_not_exist.txt /tmp/broken_symlink.txt
ls -la /tmp/broken_symlink.txt  # shows symlink even though target missing
cat /tmp/broken_symlink.txt     # No such file or directory
rm /tmp/broken_symlink.txt
```

### Lab 2: Debug a Permission Problem

```bash
# Lab 2: simulate and solve a common Airflow permission problem

# Setup: create a DAG-like directory structure
mkdir -p /tmp/airflow_dags
chmod 700 /tmp/airflow_dags   # too restrictive

# Simulate: create a dag file owned by another user (via sudo if available)
# or just create it as yourself with restrictive permissions
echo "# airflow dag" > /tmp/airflow_dags/my_dag.py
chmod 600 /tmp/airflow_dags/my_dag.py  # owner-only read/write

# Diagnosis: what can user 'nobody' see?
ls -la /tmp/airflow_dags/      # check directory permissions
stat /tmp/airflow_dags/my_dag.py   # check file permissions

# Ask: which step of path resolution fails?
# /tmp:          drwxrwxrwt (1777) — anyone can traverse
# /tmp/airflow_dags: drwx------ (700) — only owner can traverse → BLOCKED HERE

# Fix step 1: directory must be traversable
chmod 755 /tmp/airflow_dags

# Fix step 2: dag file must be readable
chmod 644 /tmp/airflow_dags/my_dag.py

# Verify with access check:
python3 -c "
import os
print('Dir accessible:', os.access('/tmp/airflow_dags', os.R_OK | os.X_OK))
print('File readable:', os.access('/tmp/airflow_dags/my_dag.py', os.R_OK))
"

# Cleanup
rm -rf /tmp/airflow_dags
```

---

## 18. Interview Q&A

**Q1: What is an inode and what does it contain?**

An inode (index node) is a fixed-size data structure stored on disk that holds all metadata about a file except its name and its data. Each file on a Linux filesystem has exactly one inode, identified by an inode number — a positive integer unique within that filesystem. The inode contains: the file type (regular, directory, symlink, pipe, socket, device), the permission bits (owner/group/other rwx and the special bits setuid/setgid/sticky), the numeric owner UID and group GID, the file size in bytes, the hard link count, three timestamps (atime for last access, mtime for last data modification, ctime for last metadata change), and block pointers or extent records that locate the file's actual data on disk. Extended attributes, including ACLs and SELinux labels, are also referenced from the inode via a pointer to a separate attribute block.

The inode does not contain the filename. That lives in a directory entry — a pairing of a name string with an inode number stored in the directory's own data blocks. This separation enables hard links: two directory entries in different paths can point to the same inode number, making them two names for the same file. The file's data is not freed until the inode's link count drops to zero, meaning all directory entries pointing to it have been removed. This is how Kafka's log cleaner achieves atomic log replacement — it hard-links the destination file before unlinking the source, guaranteeing the data is never inaccessible during the swap.

**Q2: What is the difference between a hard link and a symbolic link?**

A hard link is a directory entry that directly references an inode number. Creating a hard link with `ln source dest` adds a new directory entry pointing to the same inode as the source. The inode's link count increments. Both paths are completely equivalent — there is no "original" and no "copy." Either can be used to read the file, modify it, or check its metadata. If the source is deleted, the inode and data remain accessible through the hard link. The data is only freed when the last hard link is removed. Hard links cannot cross filesystem boundaries (inode numbers are only unique within one filesystem) and cannot link directories.

A symbolic link is a file of its own — it has its own inode, its own inode number, its own permissions (always `lrwxrwxrwx`). Its data content is a path string pointing to the target. When the kernel resolves a path containing a symlink, it reads the symlink's data and substitutes the target path, then continues resolving. If the target doesn't exist, the symlink is "broken" — `stat` will fail with ENOENT even though the symlink itself exists. Symlinks can cross filesystem boundaries and can point to directories. They are used for versioned paths (`/data/current` → `/data/v3.2.1`), alternative names for executables (`python` → `python3.11`), and mounted HDFS or S3 paths.

**Q3: What happens at the kernel level when a process tries to open `/home/spark/data.parquet`?**

Path resolution starts from the root inode (inode 2 on ext4 by convention). The kernel looks up `home` in the root directory's data block — this requires reading the root directory's data (either from disk or from the dentry cache). Finding the entry `{name="home", inode=N1}`, the kernel reads inode N1 and verifies: is it a directory (type check) and does the calling process have execute permission (traverse permission check)? It then reads N1's data block, looks up `spark`, finds `{name="spark", inode=N2}`, reads inode N2, performs the same type and permission checks, reads N2's data block, looks up `data.parquet`, finds `{name="data.parquet", inode=N3}`, and reads inode N3. Now the kernel checks whether the process has read permission on N3 (since the syscall is `open` with `O_RDONLY`). If all checks pass, the kernel allocates a `file` struct pointing to inode N3 with offset=0, allocates a file descriptor in the process's fd table, and returns the fd integer.

On a warm system with a populated dentry cache and inode cache, all of these lookups are served from RAM — zero disk I/O for the metadata path. This is why repeated access to the same files in the same directories is fast: the kernel caches the name→inode mappings (dentry cache) and the inode structs (inode cache) in memory.

**Q4: What is umask and how does it affect files created by an ETL process?**

`umask` is a per-process bitmask that specifies which permission bits to remove from newly created files and directories. It is inherited by child processes (including spawned worker threads and subprocesses) and applies to all file creation operations regardless of what permissions the creating code requests.

When a process calls `open(path, O_CREAT, 0666)` to create a file, the kernel computes the actual permissions as `0666 & ~umask`. With the default `umask=0022`, the result is `0644` (`rw-r--r--`). The bits the umask specifies (022 = group-write and other-write) are stripped. The application cannot override the umask — it can only request permissions; the kernel subtracts the mask.

For ETL processes this matters in several practical ways. A dbt process running with `umask=0022` creates model files that are not writable by the file's group — which breaks collaborative workflows where multiple users need to overwrite the same output file. An Airflow worker running with `umask=0027` creates log and artifact files that are not readable by any user outside the primary group — which can prevent monitoring agents or BI tools from reading pipeline outputs. The fix is usually to set an appropriate umask in the service's systemd unit file (`UMask=0002` for collaborative environments) or in the launch script, and to use setgid directories so that all files in a shared directory inherit the correct group.

**Q5: When would you use ACLs instead of standard POSIX permissions?**

Standard POSIX permissions allow exactly three subjects per file: the owner, the group, and everyone else. Any scenario requiring per-user or per-group permissions beyond these three cannot be expressed with standard permissions without compromising security.

The canonical data engineering scenario: a pipeline output file needs to be writable by the service account that produces it (`dbt_svc`), readable by the data team (`data_team` group), readable by a BI tool's service account (`bi_svc`), and readable by an audit process (`audit_svc`). Standard permissions cannot express "readable by data_team AND bi_svc AND audit_svc but not by others" without setting `other:r` (which grants read to everyone) or creating a new group containing exactly those accounts (which requires group management for every new requirement). ACLs solve this cleanly: `setfacl -m u:bi_svc:r,u:audit_svc:r /data/output.parquet` grants exactly the needed access without changing the file's group or other permissions.

Other scenarios where ACLs are appropriate: a shared NFS data directory where multiple CI/CD pipelines (running as different users) produce files that a single scheduler (different user) must read; HDFS directories shared between a Spark cluster and a Hive metastore, where the default POSIX model requires either overly broad group membership or ACLs to grant specific service accounts access; and Airflow DAG directories where multiple development teams deploy DAGs but the scheduler account must be able to read them all regardless of who created each file.

**Q6: How would you debug a `Permission denied` error when a Spark executor fails to write to an HDFS directory?**

Start by gathering the exact error message — it will name the specific path, the acting user, the requested access type (WRITE, READ, EXECUTE), and the inode's current permissions. A typical message: `Permission denied: user=spark, access=WRITE, inode="/data/output":etl_admin:data_team:drwxr-xr-x`.

Decompose the information: the acting user is `spark`; the directory is owned by `etl_admin` in group `data_team` with permissions `755`. For `spark` to write, it needs write permission. The owner is `etl_admin` (not `spark`), so owner write doesn't apply. If `spark` is in `data_team`, it gets group permissions `r-x` — no write. If `spark` is not in `data_team`, it gets other permissions `r-x` — no write either. Neither path grants write.

Resolution options in increasing order of privilege granted: first, check whether `spark` should simply be added to `data_team` (`usermod -aG data_team spark` on the NameNode and all DataNodes, then `hdfs dfsadmin -refreshUserToGroupsMappings`); second, if group membership changes are not desirable, add an ACL: `hdfs dfs -setfacl -m u:spark:rwx /data/output`; third, if the directory is a pipeline output directory owned by the wrong user, change ownership: `hdfs dfs -chown spark:spark /data/output`. Always prefer the minimal-privilege option. Changing permissions to 777 solves the immediate problem but grants write access to every user on the cluster — a security regression in production.

---

## 19. Cross-Question Chain

**Q1 [Interviewer]: Your Airflow scheduler can't read a new DAG file. What's your debugging process?**

Start by identifying exactly what error the scheduler logs: typically `PermissionError: [Errno 13] Permission denied: '/opt/airflow/dags/new_dag.py'`. This tells us the error is at file-open time. Check the file's permissions and ownership with `stat /opt/airflow/dags/new_dag.py` — look for the mode string (`-rw-------`), owner UID, and group GID. Then check what user the Airflow scheduler runs as: `ps aux | grep airflow` or `systemctl show airflow-scheduler --property=User`. If the scheduler runs as `airflow` and the file is owned by `developer` with mode `600`, the kernel's permission check fails: the scheduler's UID doesn't match the owner UID, the scheduler may not be in the file's group, and other has no permissions.

**Q2 [Interviewer]: You've confirmed the scheduler runs as `airflow` and the file has mode 600. What are the three ways to fix it?**

The three approaches with different trade-offs: first, `chmod 644 /opt/airflow/dags/new_dag.py` — grants read to everyone including `airflow` via the "other" permission. Simple, but grants global read access to the DAG file. Second, `chgrp airflow /opt/airflow/dags/new_dag.py && chmod 640 /opt/airflow/dags/new_dag.py` — changes the group to `airflow` and grants group-read; only the owner (developer) and any process in the `airflow` group can read. More restrictive. Third, `setfacl -m u:airflow:r /opt/airflow/dags/new_dag.py` — grants a specific ACL entry for the `airflow` user without changing the file's owner, group, or mode. Most surgical, no side effects on other files.

**Q3 [Interviewer]: You choose the ACL approach. Now new DAG files from any developer have the same problem. How do you make it persistent?**

Set a **default ACL** on the directory: `setfacl -m d:u:airflow:r-x /opt/airflow/dags/`. The `d:` prefix creates a default ACL entry. Any file or subdirectory created inside `/opt/airflow/dags/` from this point forward will automatically inherit the ACL `u:airflow:r-x`. Apply `setfacl -R -m u:airflow:r-x /opt/airflow/dags/` to fix all existing files recursively (using capital `X` for the execute bit on directories: `setfacl -R -m u:airflow:rX /opt/airflow/dags/` — `X` means execute only if directory or already executable). After this, any developer who deploys a new DAG file will have the `airflow` read ACL automatically applied.

**Q4 [Interviewer]: A junior engineer suggests using `chmod 777` on the DAGs directory instead. What's the security problem?**

Mode `777` (`rwxrwxrwx`) grants read, write, and execute to all users on the system. Any process running as any user can read the DAG files (potentially exposing database credentials, API keys, or business logic embedded in DAG code), write to or overwrite existing DAGs (a compromised service account could inject malicious code into a DAG that runs as the Airflow executor with potentially elevated privileges), and create new DAGs. In a multi-tenant environment or any system with more than one service account, `chmod 777` violates the principle of least privilege and is a critical security misconfiguration. Additionally, if the directory has mode `777` without the sticky bit (`1777`), any user can delete other users' DAG files — a denial-of-service risk.

**Q5 [Interviewer]: What would you add to the directory to prevent users from deleting each other's DAG files while still allowing writes?**

Add the **sticky bit**: `chmod 1755 /opt/airflow/dags/` (or `chmod +t /opt/airflow/dags/`). With the sticky bit, users can create files in the directory (if they have write+execute permission on the directory), but they can only delete or rename their own files. The scheduler (`airflow` user) can read all files. The DAG owner can delete their own file. No user can delete another user's DAG file. This is the same mechanism that protects `/tmp`: world-writable but each user can only remove their own temporary files.

**Q6 [Interviewer]: How does inode exhaustion manifest, and how would you diagnose it on a production Spark cluster that's throwing "No space left on device"?**

Inode exhaustion produces the same error as disk space exhaustion (`ENOSPC: No space left on device`) because both conditions prevent file creation. The first diagnostic step is `df -i` — if any mounted filesystem shows `IUse% = 100%`, inodes are the constraint, not disk space. To find which directory subtree is consuming the most inodes, use `find /data -xdev -type f | wc -l` for a quick total count, or `find /data -xdev -type d -exec sh -c 'echo "$(ls -1 {} | wc -l) {}"' \; | sort -n | tail -20` to find directories with the most entries. On a Spark cluster, the typical cause is the Hive warehouse or shuffle scratch directories accumulating millions of small files — each small file occupies one inode. The tactical fix is to compact small files into larger ones and delete old partitions. The strategic fix is migrating to a table format like Iceberg or Delta Lake, which consolidates metadata into manifest files and dramatically reduces the number of filesystem objects per table.

---

## 20. Common Misconceptions

**"Deleting a file with `rm` frees its disk space immediately."**
`rm` removes a directory entry. The inode's link count decrements. If the link count reaches zero AND no process has the file open (has an open file descriptor to it), then the inode and data blocks are marked free. If a process has the file open — common with log files — the data remains on disk until the last file descriptor is closed. This is why `rm /var/log/app.log` may not free disk space if the application is still running: the application holds an open fd, and the kernel keeps the data alive. The fix: send the application SIGHUP (or `SIGUSR1`) to reopen its log file, then the old file descriptor closes and the data is freed.

**"chmod -R 755 /data/ is the right way to fix permissions on a data directory."**
`chmod -R 755` sets all files and directories to `rwxr-xr-x`. The execute bit on regular files (`-rwxr-xr-x`) means "this is an executable program." Setting execute on a Parquet file or CSV doesn't cause harm but is semantically wrong and a mild security issue. The correct recursive pattern is `chmod -R u=rwX,g=rX,o= /data/` — capital `X` means "set execute only if the entry is a directory or already has execute set." This gives directories `rwxr-x---` and files `rw-r-----` (with the appropriate group permissions).

**"The file's mtime tells you when it was last modified."**
mtime is updated when the file's data is written. But several common operations do not update mtime: `chmod` (changes inode metadata, updates ctime, not mtime), `chown` (same), `rename()` / `mv` within the same filesystem (moves the directory entry, updates ctime of both old and new directories and the inode, but not the file's mtime). This causes confusion when using `find -mtime` to detect recently "changed" files — a file that was renamed or had its permissions changed may not appear in the results because its mtime is old even though it was recently touched.

**"The ACL mask always limits what the file owner can do."**
The ACL mask limits named ACL entries (named users and named groups) but does not affect the file owner's permissions or the "other" permissions. The owner's permission in the ACL (`user::rwx`) is always applied directly — the mask does not constrain it. This is a common misread of `getfacl` output: seeing `mask::r--` might suggest no one can write, but the owner still has their `user::rw-` permissions unmasked.

**"An inode number uniquely identifies a file across the entire system."**
Inode numbers are unique only within a single filesystem. Two different filesystems mounted at `/data/ssd` and `/data/hdd` can both have a file with inode number 1234567. This is why hard links cannot cross filesystem boundaries: the inode number in the destination directory entry would be ambiguous without specifying which filesystem. It's also why `find -inum 1234567 /` can find multiple unrelated files if `/` spans multiple mounted filesystems — use `-xdev` (don't cross filesystem boundaries) with find to limit the search to one filesystem.

---

## 21. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is an inode? | A fixed-size on-disk structure holding all file metadata EXCEPT the filename. Contains: type, permissions, owner UID/GID, size, link count, 3 timestamps (atime/mtime/ctime), and block pointers. |
| 2 | What does an inode NOT contain? | The filename (lives in a directory entry) and the file data (lives in data blocks that the inode points to). |
| 3 | What are the three timestamps in an inode? | atime: last access (read). mtime: last data modification (write). ctime: last metadata change (chmod, chown, rename, write). ctime ≥ mtime always. |
| 4 | What is a hard link? | A directory entry pointing to an existing inode. Both paths share one inode and one set of data blocks. Data freed only when ALL hard links are removed (link count = 0 AND no open fds). |
| 5 | What is a symbolic link? | A file with its own inode whose data content is a path string. The kernel substitutes the target path during path resolution. Breaks if the target is deleted. Can cross filesystems; can link directories. |
| 6 | What does `rm` actually do? | Removes a directory entry and decrements the inode's link count. Data and inode are freed only when link count = 0 AND no process has the file open. |
| 7 | What is inode exhaustion? | All inodes on a filesystem are in use. New files cannot be created even if disk space is available. Caused by millions of small files. Diagnosed with `df -i`. Fixed by compacting files or recreating filesystem with more inodes. |
| 8 | What are the three permission triplets in POSIX? | User (owner), Group, Other — each with Read (4), Write (2), Execute (1) bits. Execute on a directory means "traverse" (use it in a path), not "run as program". |
| 9 | What does the execute bit on a directory mean? | Traverse permission: the right to use the directory in a path (cd into it, access files inside it). You can have r (list) without x (traverse), or x (traverse) without r (list). |
| 10 | What is umask? | A per-process mask that strips bits from newly created files. `actual = requested & ~umask`. umask 022 strips group-write and other-write. Set in shell, inherited by child processes. Set persistently in systemd via `UMask=`. |
| 11 | What is the setgid bit on a directory? | New files/subdirs created inside the directory inherit the directory's group (instead of the creator's primary group). Used to create shared team directories where all files are in a common group. Set with `chmod g+s dir` or `chmod 2755 dir`. |
| 12 | What is the sticky bit? | On a directory: users can only delete/rename their own files, even if they have write permission on the directory. Set with `chmod +t dir` or `chmod 1777 /tmp`. |
| 13 | When do standard POSIX permissions fail? | When you need more than 3 subjects (owner, group, other): e.g., granting access to a specific user without changing the group, or granting access to two separate groups. Use ACLs. |
| 14 | What command shows ACLs on a file? | `getfacl file`. Shows: owner/group entries, named user entries (`user:bi_svc:r--`), named group entries, mask, other. `ls -l` shows `+` at end of permission string when ACLs exist. |
| 15 | What does the ACL mask do? | Limits the maximum effective permissions for all named ACL entries (named users and named groups). Does NOT affect file owner or "other" permissions. Automatically set by `setfacl -m`. |
| 16 | What is a default ACL? | An ACL on a directory that is automatically inherited by all new files and subdirectories created inside it. Set with `setfacl -m d:u:username:r-x dir/`. Solves the "new files don't have correct permissions" problem. |
| 17 | What does `chattr +i` do? | Makes a file immutable: cannot be written, renamed, deleted, or hard-linked — even by root. Used to protect critical config files. Remove with `chattr -i`. View with `lsattr`. |
| 18 | What does `chattr +a` do? | Makes a file append-only: write() only succeeds at EOF; truncation and deletion fail. Used for audit logs. Cannot be modified without removing the flag (requires root). |
| 19 | How do you find all files larger than 500 MB in /data? | `find /data -type f -size +500M` — or for human-readable output with sizes: `find /data -type f -size +500M -printf '%s\t%p\n' \| sort -n` |
| 20 | What is the difference between `-mtime +7` and `-mtime 7` in find? | `-mtime +7`: files modified MORE than 7 days ago (older than 7 days). `-mtime 7`: files modified exactly 7 days ago (within the 24h window of 7 days ago). `-mtime -7`: files modified LESS than 7 days ago (newer than 7 days). |

---

## 22. Module Summary

The Linux file system builds the familiar file-and-directory abstraction on top of raw block storage using three key data structures: **inodes** (file metadata — permissions, timestamps, block pointers), **directory entries** (name→inode mappings, living in a directory's data blocks), and **data blocks** (the actual file content, located by the inode's extent tree). An inode number uniquely identifies a file within a filesystem; the filename lives in a directory entry, not the inode. This separation enables hard links: multiple names pointing to one inode, with data freed only when the last link and last open file descriptor are gone.

**POSIX permissions** grant three permission sets per file — owner, group, and other — each with read/write/execute bits that have different meanings for files (content access) versus directories (listing vs traversal). The **umask** is the process-level mechanism that strips permission bits from newly created files. Setting the **setgid bit** on a shared directory solves the most common "new files have wrong group" problem in multi-user data engineering environments. The **sticky bit** on `/tmp`-style directories prevents users from deleting each other's files.

**ACLs** extend the three-subject POSIX model to per-user and per-group entries, solving the case where a file must be accessible to multiple specific accounts without granting broad group membership. Default ACLs on directories ensure new files automatically inherit the correct ACL entries — eliminating the "permission problem on every new file" failure mode.

**Key CLI tools:** `stat` (full inode metadata), `ls -li` (inode numbers and link counts), `find` (locate files by name, size, time, and permissions), `chmod/chown/chgrp` (modify permissions and ownership), `getfacl/setfacl` (manage ACLs), `lsattr/chattr` (extended file attributes), `df -h/-i` (disk space and inode usage).

---

**SYS-LNX-101: 1 of 5 complete.**  
**Next: SYS-LNX-101 M02 — Process Management**  
*(ps, top, htop, kill, nice, systemd — diagnosing and managing processes on a Linux data server)*
