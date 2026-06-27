# M03: The ISA Contract

**School:** CS Foundations (CSF)  
**Course:** CSF-ARC-101 — How Computers Execute Programs  
**Module:** 03 of 05  
**Difficulty:** ★★★☆☆  
**Estimated study time:** 4–6 hours  
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

M01 and M02 described the CPU's internal mechanics: how instructions flow through the pipeline, how the out-of-order engine exploits ILP, how SIMD processes vectors. But all of that assumed the instructions were already correct machine code on the right CPU. This module answers the question that precedes all of that: **what exactly is the contract between software and hardware?**

The ISA (Instruction Set Architecture) is that contract. It defines what instructions exist, what registers are available, how memory is addressed, how the CPU handles exceptions, and how software and hardware interact at the boundary. The ISA is the interface — like an API — that software is compiled against. The microarchitecture from M02 is the implementation beneath that interface.

**Why data engineers must understand this:**

In 2018, AWS launched Graviton1 (ARM-based EC2 instances). By 2026, AWS Graviton3 and Graviton4 power a substantial fraction of AWS compute — including EMR (Spark), MSK (Kafka), and ElastiCache. ARM AArch64 is a different ISA from Intel x86-64. A Python wheel compiled for x86-64 does not run on AArch64. A JVM running on Graviton JIT-compiles to AArch64 bytecode, not x86-64. When you choose `m6g` vs `m6i` EC2 instances for your Spark cluster, you are making an ISA choice.

More concretely: when a NumPy binary wheel is installed, pip selects the correct wheel for your CPU architecture. When a Kafka broker's `sendfile()` system call is made, the Linux kernel on Graviton handles it via the AArch64 system call table. When you run `spark-submit` and it calls into native Arrow libraries, those libraries must be compiled for the correct ISA. Understanding the ISA contract explains why these choices matter, why cross-architecture bugs exist, and why "just run the Docker container" sometimes fails on an ARM Mac.

---

## 2. Prerequisites

- **M01: The Von Neumann Machine** — registers, fetch-decode-execute, clock speed vs IPC.
- **M02: CPU Microarchitecture** — pipeline stages, SIMD registers, execution units.
- Basic understanding of binary and hexadecimal (memory addresses and register values are in hex).

---

## 3. Learning Objectives

By the end of this module you will be able to:

1. Define ISA and distinguish it from microarchitecture. Explain why this distinction matters.
2. List the x86-64 general-purpose registers, their conventional roles, and the calling convention (which registers are caller-saved vs callee-saved).
3. Explain what a system call is, how it works mechanically (the transition from user space to kernel space), and why `syscall` is expensive relative to a regular function call.
4. Explain what the ABI (Application Binary Interface) is and how it differs from the ISA.
5. Describe the key structural differences between x86-64 (CISC) and AArch64 (RISC) and why they produce different performance characteristics.
6. Explain what RISC-V is, why it exists, and where it is used in data engineering infrastructure.
7. Explain endianness, when it matters in data engineering (Kafka wire protocol, Parquet file format, network byte order), and how to detect and handle it.
8. Explain the x86-64 memory model (total store order) and what it guarantees to a data engineer writing concurrent code.

---

## 4. First Principles

### The Abstraction Stack

Software does not run directly on transistors. There is a stack of abstractions between your Python script and the electrons:

```
┌──────────────────────────────────────────┐
│  Python source code                       │  you write this
├──────────────────────────────────────────┤
│  CPython bytecode (.pyc)                  │  Python compiler
├──────────────────────────────────────────┤
│  CPython C runtime                        │  CPython interpreter
├──────────────────────────────────────────┤
│  Machine code (x86-64 or AArch64)         │  C compiler (gcc/clang)
├──────────────────────────────────────────┤
│  ISA  ← THIS MODULE                      │  the contract
├──────────────────────────────────────────┤
│  Microarchitecture (M02)                  │  implementation
├──────────────────────────────────────────┤
│  Logic gates, transistors                 │  physics
└──────────────────────────────────────────┘
```

The ISA sits at the center. It is a formal specification — published as a document — that defines exactly what instructions a CPU must execute and what the results must be. Any CPU that correctly implements the spec is a valid x86-64 (or AArch64, or RISC-V) CPU. Intel, AMD, Via, and Zhaoxin all make x86-64 CPUs. Apple, AWS, Ampere, and Qualcomm all make AArch64 CPUs. They all run the same compiled binaries.

### The ISA as a Contract

The ISA is a promise made in two directions:

**Hardware promises to software:** "If you give me valid machine code that follows the ISA specification, I will execute it correctly, in the order the ISA defines, and produce the results the ISA specifies."

**Software promises to hardware:** "I will only use instructions defined in the ISA. I will not depend on any internal microarchitectural detail (pipeline depth, ROB size, branch predictor behavior) that is not guaranteed by the ISA."

This separation is what makes it possible to run software compiled in 2005 on a CPU designed in 2025, and vice versa. Intel has maintained backward compatibility with the x86-64 ISA since 2003 (the 64-bit extension of x86 introduced by AMD). Code compiled for a Pentium 4 runs on a modern Intel Core i9. The microarchitecture changed radically; the ISA interface was preserved.

### ISA vs Microarchitecture

This distinction is the most important concept in this module:

| | ISA | Microarchitecture |
|---|---|---|
| What it is | Specification document | Physical implementation |
| Who writes it | ISA architects (Intel, ARM Ltd, RISC-V International) | CPU designers (Intel, AMD, Apple, AWS) |
| Changes how often | Rarely (major extensions every 5–10 years) | Every generation (1–3 years) |
| Is it observable? | Yes — defined behavior | Partially — performance is observable, implementation is not |
| Example | "ADD rax, rbx adds rbx to rax and stores the result in rax" | How many execution units, ROB depth, pipeline stages |
| Multiple implementations? | Yes — many CPUs per ISA | No — each CPU has one microarchitecture |

Two CPUs can implement the same ISA but have radically different microarchitectures. An Apple M3 (AArch64) and an AWS Graviton3 (also AArch64) run the same binaries, but Apple's microarchitecture has a much wider out-of-order window (30-stage pipeline, ~600-entry ROB) optimized for single-thread performance, while Graviton3 has a shallower pipeline optimized for power efficiency and many-core throughput.

---

## 5. Architecture

### x86-64 Register File

x86-64 has 16 general-purpose registers (64-bit), each with 32-bit, 16-bit, and 8-bit sub-registers for backward compatibility.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    x86-64 GENERAL-PURPOSE REGISTERS                       │
│                                                                            │
│  64-bit    32-bit   16-bit   8-bit (high) 8-bit (low)   Conventional Use  │
│  ────────  ──────   ──────   ──────────── ─────────────  ────────────────  │
│  rax       eax      ax       ah           al             Return value      │
│  rbx       ebx      bx       bh           bl             Callee-saved      │
│  rcx       ecx      cx       ch           cl             4th arg (Linux)   │
│  rdx       edx      dx       dh           dl             3rd arg / ret hi  │
│  rsi       esi      si       —            sil            2nd argument      │
│  rdi       edi      di       —            dil            1st argument      │
│  rbp       ebp      bp       —            bpl            Frame pointer     │
│  rsp       esp      sp       —            spl            Stack pointer     │
│  r8        r8d      r8w      —            r8b             5th argument     │
│  r9        r9d      r9w      —            r9b             6th argument     │
│  r10       r10d     r10w     —            r10b            Caller-saved      │
│  r11       r11d     r11w     —            r11b            Caller-saved      │
│  r12       r12d     r12w     —            r12b            Callee-saved      │
│  r13       r13d     r13w     —            r13b            Callee-saved      │
│  r14       r14d     r14w     —            r14b            Callee-saved      │
│  r15       r15d     r15w     —            r15b            Callee-saved      │
│                                                                            │
│  Special registers:                                                        │
│  rip  — instruction pointer (program counter, not directly writeable)     │
│  rflags — condition flags (ZF zero, SF sign, CF carry, OF overflow)        │
│                                                                            │
│  SIMD (M02):  xmm0–xmm15 (128-bit SSE), ymm0–ymm15 (256-bit AVX2)        │
│               zmm0–zmm31 (512-bit AVX-512)                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

### The x86-64 System V Calling Convention (Linux/macOS)

The **calling convention** is part of the ABI. It defines how function arguments are passed and how the stack is managed. Any code that calls functions must follow it — otherwise callee and caller will read arguments from the wrong places.

```
┌─────────────────────────────────────────────────────────────────────────┐
│              SYSTEM V AMD64 CALLING CONVENTION                           │
│                                                                           │
│  INTEGER / POINTER ARGUMENTS (in order):                                 │
│    Arg 1 → rdi    Arg 2 → rsi    Arg 3 → rdx                            │
│    Arg 4 → rcx    Arg 5 → r8     Arg 6 → r9                             │
│    Args 7+ → pushed onto stack (right-to-left)                           │
│                                                                           │
│  FLOATING-POINT ARGUMENTS:  xmm0 → xmm7                                 │
│                                                                           │
│  RETURN VALUE:                                                            │
│    Integer/pointer → rax   (high 64 bits in rdx if > 64 bits)           │
│    Float → xmm0                                                           │
│                                                                           │
│  CALLER-SAVED (caller must preserve these around a call):                │
│    rax, rcx, rdx, rsi, rdi, r8, r9, r10, r11                            │
│    All xmm/ymm registers                                                  │
│    (The callee can clobber these freely)                                  │
│                                                                           │
│  CALLEE-SAVED (callee must restore these before returning):              │
│    rbx, rbp, r12, r13, r14, r15                                          │
│    (If a function uses these, it must push/pop them)                     │
│                                                                           │
│  STACK:  16-byte aligned at the point of a call instruction              │
│  RED ZONE: 128 bytes below rsp are reserved (leaf functions can use)    │
└─────────────────────────────────────────────────────────────────────────┘
```

**Why data engineers encounter this:** When you write a Python C extension (e.g., a custom NumPy ufunc), your C function is called with arguments in rdi, rsi, rdx. When you debug a segfault in a Cython extension using `gdb`, the calling convention tells you where to find function arguments in the registers. When you read Spark's generated JVM bytecode in a flame graph, the JVM's calling convention (similar to System V but not identical) explains the register usage.

### The System Call Interface

A **system call** (syscall) is the mechanism by which user-space code requests a service from the OS kernel. The syscall instruction is the boundary between user space (ring 3) and kernel space (ring 0).

```
┌─────────────────────────────────────────────────────────────────────────┐
│                   SYSCALL MECHANICS (x86-64 Linux)                       │
│                                                                           │
│  User space sets up:                                                      │
│    rax  = syscall number  (e.g., 1 = write, 0 = read, 60 = exit)        │
│    rdi  = arg 1                                                           │
│    rsi  = arg 2                                                           │
│    rdx  = arg 3                                                           │
│    r10  = arg 4  (note: r10, not rcx, because rcx is clobbered)         │
│    r8   = arg 5                                                           │
│    r9   = arg 6                                                           │
│                                                                           │
│  Then executes:  syscall instruction                                      │
│                                                                           │
│  CPU actions on syscall:                                                  │
│    1. Save user-space rip (return address) in rcx                        │
│    2. Save user-space rflags in r11                                       │
│    3. Switch from ring 3 → ring 0 (privilege escalation)                │
│    4. Load kernel rip from IA32_LSTAR MSR (kernel entry point)           │
│    5. Switch to kernel stack                                               │
│    6. Kernel handles the request                                          │
│    7. sysret instruction: restore rip from rcx, rflags from r11          │
│    8. Switch from ring 0 → ring 3 (privilege drop)                      │
│                                                                           │
│  Cost: ~100–300 ns (vs ~1 ns for a regular function call)                │
│  Why expensive:                                                            │
│    - Privilege level switch (hardware gate)                               │
│    - Pipeline flush (security: clear speculative state)                   │
│    - Stack switch (new pointer, TLB effects)                              │
│    - Post-Spectre mitigations (KPTI page table switch, retpoline)        │
└─────────────────────────────────────────────────────────────────────────┘
```

**Every I/O operation in your data pipelines goes through syscalls.** `open()`, `read()`, `write()`, `send()`, `recv()`, `epoll_wait()` — all syscalls. When a Kafka broker serves 1 million messages per second, it is making hundreds of thousands of `sendfile()` syscalls per second. The 100–300 ns cost per syscall contributes directly to broker throughput limits. This is why Kafka's design minimizes syscall count: it batches writes, uses `sendfile()` to zero-copy from the page cache directly to the network socket, and uses `epoll` for I/O multiplexing rather than one thread per connection.

### AArch64 (ARM 64-bit) Register File

AArch64 is ARM's 64-bit ISA, used in Apple Silicon (M-series), AWS Graviton, and mobile processors.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    AArch64 REGISTER FILE                                  │
│                                                                           │
│  General-purpose: x0–x30 (64-bit), w0–w30 (32-bit alias of low half)   │
│  Stack pointer:   sp (dedicated, not x31 in most contexts)               │
│  Zero register:   xzr / wzr (reads as zero, writes discarded)            │
│  Program counter: pc (not directly accessible as a GP register)          │
│  Link register:   x30 (holds return address for function calls)          │
│                                                                           │
│  Calling convention (AAPCS64):                                           │
│    Args 1–8:  x0–x7                                                      │
│    Return:    x0 (x1 for 128-bit returns)                                │
│    Callee-saved: x19–x28, x29 (frame pointer), x30 (link register)      │
│    Caller-saved: x0–x18                                                   │
│                                                                           │
│  SIMD/FP: v0–v31                                                          │
│    128-bit full width (q0–q31)                                           │
│    64-bit double (d0–d31)                                                 │
│    32-bit float  (s0–s31)                                                 │
│    NEON: 16-byte (128-bit) SIMD, always available                        │
│    SVE:  scalable vector extension, 128–2048 bits, available on          │
│           Graviton3, Fujitsu A64FX                                        │
│                                                                           │
│  Special system register:  NZCV (condition flags: negative, zero,        │
│                                   carry, overflow)                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### AArch64 System Call Interface

```bash
# Linux AArch64 syscall convention:
#   x8  = syscall number  (NOT x0, unlike x86-64's rax)
#   x0–x5 = arguments 1–6
#   svc #0  (supervisor call instruction, not "syscall")
#   Return value in x0
```

The AArch64 syscall numbers are different from x86-64 syscall numbers. This is why you cannot run an x86-64 binary on an AArch64 kernel without emulation — the system call numbers, instructions, and register layouts are all different.

### RISC-V Register File

RISC-V is an open-standard ISA (no royalties, publicly specified). It is used in embedded systems, research CPUs, and increasingly in data center hardware (SiFive, StarFive, Milk-V).

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    RISC-V (RV64GC) REGISTER FILE                         │
│                                                                           │
│  32 integer registers: x0–x31 (64-bit in RV64)                          │
│  ABI names and roles:                                                     │
│    x0 / zero   — hardwired zero                                          │
│    x1 / ra     — return address                                           │
│    x2 / sp     — stack pointer                                            │
│    x3 / gp     — global pointer                                           │
│    x4 / tp     — thread pointer                                           │
│    x5–x7 / t0–t2 — temporaries (caller-saved)                           │
│    x8 / s0/fp  — saved / frame pointer (callee-saved)                   │
│    x9 / s1     — saved (callee-saved)                                    │
│    x10–x11 / a0–a1 — args 1–2 / return value                           │
│    x12–x17 / a2–a7 — args 3–8                                           │
│    x18–x27 / s2–s11 — saved (callee-saved)                              │
│    x28–x31 / t3–t6 — temporaries (caller-saved)                         │
│                                                                           │
│  32 float registers: f0–f31                                               │
│  Vector extension (RVV): scalable, similar to ARM SVE                    │
│                                                                           │
│  Key RISC-V properties:                                                   │
│    - Fixed 32-bit instruction width (base ISA; compressed "C" = 16-bit) │
│    - Load/store architecture: only ld/sd touch memory                    │
│    - No complex micro-op decoding needed → simpler pipelines             │
│    - Modular extensions: I (integer), M (multiply), A (atomic),          │
│      F (float), D (double), C (compressed), V (vector)                   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Endianness

**Endianness** defines the byte order in which multi-byte values are stored in memory.

```
Value: 0x01234567 (the 32-bit integer 19,088,743)

Big-endian (network byte order):
  Address:  0x00  0x01  0x02  0x03
  Value:    0x01  0x23  0x45  0x67
  (Most significant byte first — like reading left-to-right)

Little-endian (x86-64, AArch64 default):
  Address:  0x00  0x01  0x02  0x03
  Value:    0x67  0x45  0x23  0x01
  (Least significant byte first)
```

x86-64 is **little-endian**. AArch64 is configurable (LE by default on Linux). RISC-V is little-endian by default. The **network byte order** (TCP/IP, used in Kafka wire protocol) is **big-endian**.

**Data engineering relevance:**

The Kafka wire protocol sends multi-byte integers in big-endian (network byte order). When a Kafka consumer reads a message size field from a socket on an x86-64 machine, it must byte-swap: `ntohs()` / `ntohl()` in C, or `ByteBuffer.order(ByteOrder.BIG_ENDIAN)` in Java. This is handled automatically by Kafka's client libraries, but when debugging a Kafka protocol issue with Wireshark or writing a custom Kafka protocol parser, you see the raw big-endian bytes.

The Parquet file format header uses little-endian for its Thrift-encoded metadata (following Thrift's compact protocol, which is little-endian). The actual column data pages are little-endian (matching x86-64 and AArch64 native byte order). When Parquet is read on a big-endian platform (e.g., IBM POWER), a byte-swap is required — this is why Parquet support on big-endian platforms is often an afterthought and can have bugs.

### The x86-64 Memory Model (Total Store Order)

The **memory model** is part of the ISA. It defines what order writes and reads to memory are visible to other threads.

x86-64 implements **Total Store Order (TSO)**:

```
TSO Guarantees:
  ✅ Reads are NEVER reordered with other reads
  ✅ Writes are NEVER reordered with other writes
  ✅ Reads are NEVER reordered with earlier writes (to a different address)
  ❌ Writes MAY be delayed (buffered in the store buffer before becoming
     globally visible) — a thread can read a value before another thread's
     write to the same address has flushed from its store buffer

Practical meaning:
  Thread A:           Thread B:
  store x = 1         store y = 1
  load  r1 = y        load  r2 = x

  Under TSO, it is POSSIBLE for r1 = 0 AND r2 = 0 simultaneously.
  (Both threads' stores are in their respective store buffers, not globally visible yet)
  This is the only reordering TSO allows.
```

AArch64 uses a **weaker memory model** (Weak Memory Ordering). Many more reorderings are permitted. Code that works correctly on x86-64 without explicit memory barriers may fail on AArch64 if it relies on x86-64's TSO implicit ordering.

**Data engineering relevance:** Java (and therefore the JVM-based Spark and Kafka) abstracts over the hardware memory model using the Java Memory Model (JMM) and `volatile`/`synchronized` keywords. The JVM's JIT compiler emits the appropriate memory fence instructions (`mfence` on x86-64, `dmb ish` on AArch64) when the JMM requires them. As long as you write correct JMM code, it runs correctly on both ISAs. If you write unsafe Java (using `sun.misc.Unsafe` directly) or Rust/C code that runs in a JVM native library, you must use explicit barriers appropriate for the target ISA.

---

## 6. Execution Flow

### Tracing a Python `open()` Call to a Syscall and Back

```
Python:  f = open("data.parquet", "rb")

CPython (Python/bltinmodule.c):
  Calls C function  builtin_open()
  → calls io_open() in Modules/_io/_iomodule.c
  → creates FileIO object
  → calls FileIO_init()
  → calls os.open()  (which maps to POSIX open())

C library (glibc, libc.so):
  open() in glibc:
    load rax = 2          # syscall number for open (x86-64 Linux)
    load rdi = ptr_to_path
    load rsi = O_RDONLY = 0
    load rdx = 0          # mode (irrelevant for O_RDONLY)
    syscall               # kernel entry point

x86-64 CPU (hardware):
  rip saved to rcx
  rflags saved to r11
  privilege level: ring 3 → ring 0
  load new rip from IA32_LSTAR (kernel entry: entry_SYSCALL_64)
  switch to kernel stack

Linux kernel (arch/x86/entry/entry_64.S → fs/open.c):
  sys_open():
    validate path string
    check permissions (DAC, SELinux, AppArmor)
    allocate file descriptor in process's fd table
    return fd number in rax

CPU (sysret instruction):
  privilege level: ring 0 → ring 3
  restore rip from rcx (back to glibc's instruction after syscall)
  restore rflags from r11

glibc:
  check if rax < 0 (error)
  if error: set errno, return -1
  return fd (file descriptor integer)

CPython:
  wraps fd in a Python file object
  returns Python file object to user
```

Total path: ~50 function calls, 1 privilege transition, ~200–500 ns.

### What `perf trace` Shows

```bash
perf trace -p $(pgrep python3) -- python3 -c "open('test.txt', 'r')"

# Output:
#  0.000 openat(AT_FDCWD, "test.txt", O_RDONLY|O_CLOEXEC) = 3
#  0.012 fstat(3, {st_mode=S_IFREG|0644, st_size=1024,...}) = 0
#  0.015 fadvise64(3, 0, 0, POSIX_FADV_SEQUENTIAL) = 0
#  0.018 mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,...) = 0x7f...
```

Each line is one syscall, with arguments and return value. The hex argument values directly correspond to the register values passed at the `syscall` instruction — you can verify against the Linux syscall table.

---

## 7. Mental Models

### Mental Model 1 — The ISA as a USB Port Standard

The ISA is to CPUs what the USB-C standard is to devices. Any USB-C device works in any USB-C port — the physical and electrical spec is the contract. What's inside the charger or laptop is the implementation. Two chargers with identical USB-C ports may have completely different internal circuits. Two CPUs with the same ISA may have completely different pipelines, cache sizes, and branch predictors. But any binary compiled against the ISA runs on any conforming CPU, just as any USB-C cable works in any USB-C port.

### Mental Model 2 — The Calling Convention as a Form Template

A calling convention is like a government form. It says: "Put your name in field 1 (rdi), your ID in field 2 (rsi), your date in field 3 (rdx)." If the caller fills in different fields than the callee expects to read from, the communication fails — not because of a hardware error, but because both sides are following different form templates. The calling convention ensures that every function in the system — regardless of which compiler or language generated it — reads from and writes to the same registers in the same order.

### Mental Model 3 — The Syscall as a Bank Teller Window

A syscall is the security-conscious boundary between citizen (user space) and vault (kernel). You cannot walk into the vault yourself. You submit a request through the teller window (the syscall instruction), the teller (kernel) checks your credentials, performs the action on your behalf, and hands back the result. The window is deliberately slow (100–300 ns): the security check (privilege escalation), the ID verification (capability checks), and the handover all take time. Every optimization that reduces syscall count (batching writes, zero-copy `sendfile`, `epoll` over `select`) is optimizing for fewer trips to the teller.

### Mental Model 4 — Endianness as Column-Major vs Row-Major

Endianness is which end of a multi-byte number you store first. Big-endian stores the most significant byte (the "big end") first — like writing a number from left to right (123 = 1, then 2, then 3). Little-endian stores the least significant byte first — like writing 123 as 3, then 2, then 1. Neither is more "correct." The problem arises when two systems with different conventions communicate — the receiving system sees `0x67452301` when the sender sent `0x01234567`. Every network protocol must define its byte order. Most modern hardware is little-endian; most network protocols are big-endian. Converting between them is a constant low-level concern in network and file format code.

---

## 8. Failure Scenarios

### Failure 1 — Wrong Architecture Binary in a Docker Image

**Symptom:** A Docker container that runs perfectly on a developer's Apple M3 Mac (AArch64) crashes immediately on an AWS EC2 `c5` (x86-64) instance with `exec format error`. Or: a Spark worker node on a Graviton `c6g` instance fails to load a native library: `cannot execute binary file`.

**Root cause:** The binary was compiled for a different ISA. An ELF binary contains an `e_machine` header field identifying its target ISA (`EM_X86_64 = 62`, `EM_AARCH64 = 183`). The Linux kernel checks this header on `execve()` and rejects binaries for the wrong architecture.

**In practice:**
```bash
# Check the architecture of a binary
file /usr/bin/python3
# python3: ELF 64-bit LSB executable, x86-64, ...
#                                      ^^^^^^^^ this is the ISA

# Check a shared library
file /usr/lib/python3/dist-packages/numpy/core/_multiarray_umath.cpython-311-x86_64-linux-gnu.so
# ... ELF 64-bit LSB shared object, x86-64, ...
```

**Fix:** Build multi-architecture Docker images using `docker buildx --platform linux/amd64,linux/arm64`. Use Python wheels from PyPI that are published with `manylinux_2_28_aarch64` tags. Verify NumPy, PyArrow, and any C extensions have aarch64 wheels before switching EC2 instance types to Graviton.

---

### Failure 2 — Endianness Bug in a Custom Kafka Protocol Parser

**Symptom:** A custom Kafka consumer written in Python reads garbage message sizes and crashes with `struct.error: unpack requires a buffer of N bytes` when the actual message content is valid.

**Root cause:** The Kafka wire protocol is big-endian. Python's `struct.unpack` uses native little-endian byte order by default on x86-64. The developer forgot the `>` (big-endian) format prefix.

```python
# Bug: reads message size in native (little-endian) byte order
size = struct.unpack('i', buffer[:4])[0]  # WRONG on little-endian systems

# Fix: explicitly use big-endian (network byte order)
size = struct.unpack('>i', buffer[:4])[0]  # CORRECT
# or equivalently:
size = int.from_bytes(buffer[:4], byteorder='big', signed=True)
```

**Diagnosis:** The received size value is the byte-reversed integer. If the correct size is `0x000003E8` (1000), a little-endian misread gives `0xE8030000` (3892314112) — obviously wrong.

**Fix:** Always specify byte order explicitly in any binary protocol parsing code. Use `>` for network/Kafka protocol, `<` for Parquet column data, `=` only for structures that don't cross machine boundaries.

---

### Failure 3 — Memory Ordering Bug when Migrating from x86-64 to Graviton

**Symptom:** A Java-based data pipeline that has run correctly for years on `c5` (x86-64) instances starts producing inconsistent read-your-writes results on `c6g` (Graviton, AArch64) instances. One thread writes a record; another thread reads it back and sees the old value.

**Root cause:** x86-64's Total Store Order (TSO) is stronger than AArch64's weak memory ordering. Code that was inadvertently relying on TSO's implicit ordering (e.g., using a plain `boolean` flag without `volatile` as a publication guard between two threads) works correctly on x86-64 because TSO prevents most reorderings. On AArch64, the JVM may reorder the flag write after the data write, making the publication unsound.

```java
// Bug: missing volatile keyword
boolean dataReady = false;   // should be: volatile boolean dataReady = false;
String data = null;

// Thread 1:
data = "hello";
dataReady = true;  // on AArch64, this store may become visible BEFORE data = "hello"

// Thread 2:
while (!dataReady) {}
System.out.println(data);  // may print null on AArch64!
```

**Fix:** Use `volatile` for publication flags. Use `java.util.concurrent` primitives (CountDownLatch, Phaser, AtomicBoolean) which include the correct memory barriers. The JVM correctly emits `dmb ish` (data memory barrier) on AArch64 for volatile accesses, making the code portable. Test on both ISAs before declaring a Graviton migration "complete."

---

### Failure 4 — Syscall Overhead Regression After Kernel Upgrade

**Symptom:** A Kafka broker's `send()` throughput drops 15% after a routine Linux kernel upgrade. No code changes were made. `perf stat` shows the same number of `sendfile()` syscalls but each takes ~100 ns longer.

**Root cause:** The kernel upgrade included new mitigations for a hardware vulnerability (e.g., a new variant of Spectre requiring additional serialization on the syscall entry path). The mitigation adds a `LFENCE` or `cpuid` instruction at kernel entry, serializing the CPU pipeline and flushing speculative state — adding 50–150 ns to every syscall.

**Diagnosis:**
```bash
# Check for mitigation status
cat /sys/devices/system/cpu/vulnerabilities/*
# spectre_v2: Mitigation: Enhanced / Automatic IBRS
# mds: Mitigation: Clear CPU buffers; SMT disabled

# Measure syscall cost directly
strace -c -e trace=sendfile kafka-run-class.sh kafka.tools.ProducerPerformance ...
# Look at "seconds" / "calls" for sendfile
```

**Fix options:**
1. Accept the regression (security mitigations are non-optional for most organizations).
2. Reduce syscall count: increase Kafka's `socket.send.buffer.bytes` and batch sizes so each `sendfile()` carries more data.
3. Evaluate if newer hardware (with hardware mitigations instead of software mitigations) eliminates the overhead.

---

## 9. Recovery Procedures

### Recovering from a Cross-Architecture Binary Problem

1. Identify all C extensions in your Python environment: `pip list` + check for `.so` files in site-packages.
2. For each package, verify wheel availability for the target architecture: `pip install --dry-run --only-binary :all: numpy --platform manylinux_2_28_aarch64`.
3. Build a multi-arch Docker image: use `docker buildx` with `--platform linux/amd64,linux/arm64`. Test both variants in CI before deployment.
4. For packages without pre-built wheels for AArch64, compile from source during the Docker build (requires build tools: `gcc`, `python3-dev`, `libopenblas-dev` for NumPy).

### Recovering from an Endianness Bug

1. Add a byte-order assertion test that runs at startup: read a known byte sequence and check the interpreted value.
2. Audit all `struct.pack/unpack` calls and `int.from_bytes` calls for explicit byte order.
3. For Kafka protocol code, use the official Kafka client libraries (which handle byte order correctly) rather than raw socket parsing.

### Recovering from a Graviton Memory Ordering Bug

1. Enable thread sanitizer (`-Djava.util.concurrent.ForkJoinPool.common.threadFactory=...` + TSAN for JVM code) to detect data races.
2. Add `volatile` to all flags used for inter-thread publication.
3. Replace ad-hoc synchronization with `java.util.concurrent` primitives.
4. Run the full test suite on both x86-64 and AArch64 CI runners before declaring the migration complete.

---

## 10. Trade-offs

### CISC (x86-64) vs RISC (AArch64)

**CISC** (Complex Instruction Set Computer): x86-64 has ~2000 instructions, including complex multi-operation instructions like `MOVS` (memory-to-memory copy), string operations, and `REP` prefix for repeated operations. Instructions have variable length (1–15 bytes).

**RISC** (Reduced Instruction Set Computer): AArch64 has ~500–800 instructions. All instructions are 32-bit fixed-width (with the exception of compressed 16-bit "Thumb-2" instructions which AArch64 doesn't use in 64-bit mode). Only `LD`/`ST` instructions access memory.

| Dimension | x86-64 (CISC) | AArch64 (RISC) |
|---|---|---|
| Instruction count (per task) | Fewer, denser | More, but simpler |
| Decode complexity | High (variable-length → complex decoder) | Low (fixed-width → simple decoder) |
| Code density | Higher | Lower |
| Pipeline design | Harder (CISC → μop cracking) | Easier |
| Power efficiency | Lower | Higher |
| Backward compatibility burden | High (must execute 1978 8086 code) | Low (AArch64 introduced 2011) |
| Typical use | Server, desktop, laptop | Mobile, embedded, hyperscaler server |

**Modern reality:** The CISC/RISC distinction is blurred. x86-64 CPUs crack all instructions into RISC-like μops internally (M02). The performance difference between modern x86-64 and AArch64 for data workloads is <20% in either direction and varies by workload. The relevant practical differences are power efficiency (AArch64 wins) and ecosystem maturity (x86-64 has more binary-compatible software).

### Compiled vs JIT-Compiled vs Interpreted on Multiple ISAs

| Runtime | ISA handling | Example |
|---|---|---|
| Ahead-of-time compiled (C, Rust, Go) | Separate binary per ISA | NumPy wheel: `numpy-1.26-cp311-cp311-manylinux_2_28_x86_64.whl` vs `_aarch64.whl` |
| JIT-compiled (JVM, .NET) | Portable bytecode → ISA-specific native code at runtime | Spark's JVM: same `.jar` on x86-64 and AArch64, JIT generates different machine code |
| Interpreted (Python, Ruby) | Interpreter is ISA-specific; scripts are portable | Python scripts are portable; CPython binary is not |
| Mixed (Python + C extensions) | Scripts portable; C extensions ISA-specific | `import numpy` loads an ISA-specific `.so` |

Spark benefits from "write once, run anywhere" because the JVM JIT adapts to the host ISA. But native libraries called from Spark (Arrow C++ for PySpark, libz for compression, libssl for TLS) must be compiled for the host ISA.

---

## 11. Comparisons

### x86-64 vs AArch64 vs RISC-V Summary

| Feature | x86-64 | AArch64 | RISC-V |
|---|---|---|---|
| Origin | Intel/AMD, 2003 | ARM Ltd, 2011 | UC Berkeley, 2010 (open, 2015) |
| Instruction width | Variable (1–15 bytes) | Fixed 32-bit | Fixed 32-bit (base); 16-bit compressed extension |
| GP registers | 16 (rax–r15) | 31 (x0–x30) + xzr | 32 (x0–x31) |
| SIMD | SSE/AVX2/AVX-512 | NEON (128-bit), SVE (scalable) | V extension (scalable) |
| Memory model | TSO (strong) | Weak (requires explicit barriers) | Weak (requires explicit barriers) |
| Syscall instruction | `syscall` | `svc #0` | `ecall` |
| Syscall number register | rax | x8 | a7 (x17) |
| Licensing | Proprietary (Intel/AMD) | Proprietary (ARM Ltd) | Open (royalty-free) |
| AWS instance type | m5/c5/r5/m6i/c6i | m6g/c6g/r6g (Graviton2), m7g/c7g (Graviton3) | Not yet in major clouds |
| Primary use in DE | Default (historical) | Growing: Graviton for Spark/Kafka | Emerging: RISC-V cloud announced 2024 |

### Linux Syscall Numbers (Selected)

| Syscall | x86-64 number | AArch64 number | Description |
|---|---|---|---|
| `read` | 0 | 63 | Read from file descriptor |
| `write` | 1 | 64 | Write to file descriptor |
| `open` | 2 | (use openat: 56) | Open a file |
| `close` | 3 | 57 | Close a file descriptor |
| `mmap` | 9 | 222 | Map memory |
| `sendfile` | 40 | 71 | Zero-copy file-to-socket send |
| `epoll_wait` | 232 | 281 | Wait for I/O events |
| `exit` | 60 | 93 | Exit process |

The numbers are completely different between ISAs. A binary that uses `syscall` with `rax = 60` on x86-64 calls `exit`. The same number (60) on AArch64 does not map to `exit` — AArch64's exit is syscall 93. This is why compiled binaries are ISA-specific at the ABI level, even if the source code is ISA-agnostic.

---

## 12. Production Examples

### AWS Graviton3 for Spark EMR

AWS Graviton3 (AArch64, Neoverse V1 microarchitecture) offers ~40% better price/performance than equivalent x86-64 `c6i` instances for Spark workloads. The reasons connect directly to this module:

1. **ISA efficiency:** AArch64's fixed-width instructions require less decoder hardware, leaving more transistor budget for execution units and cache.
2. **SVE (Scalable Vector Extension):** Graviton3 supports SVE at 256-bit width for many operations. For numeric Spark workloads with Parquet columnar data and Arrow-based Pandas UDFs, SVE provides throughput equivalent to AVX-512 without the frequency scaling penalty Intel CPUs suffer.
3. **Memory bandwidth:** Graviton3 connects to DDR5 memory with higher bandwidth than `c6i`, which uses DDR4. Columnar data processing is memory-bandwidth-bound. Higher bandwidth = more data per second.
4. **ABI compatibility:** The JVM's Spark runtime runs identically on AArch64 — the JIT compiles the same Spark bytecode to AArch64 native instructions. No source code changes needed.

**Migration concern:** Native libraries. Before migrating a Spark cluster to Graviton:
- Verify PyArrow has AArch64 wheels (it does, since Arrow 3.0).
- Verify all Python packages your jobs use have AArch64 wheels.
- Test your dbt profiles — if your Spark jobs call into Parquet C++ libraries, they must be AArch64-compiled.

### Kafka on Graviton2 (`m6g`)

The Kafka broker's performance bottleneck is I/O: accepting connections (`accept4()` syscall), reading from producers (`recvfrom()`), writing to log files (`writev()`), serving consumers via zero-copy (`sendfile()`). All of these are syscalls. The actual bytes are handled by the kernel's page cache and network stack, not by Kafka Java code.

Graviton2 benchmarks show Kafka throughput ~30% higher than equivalent `m5` (x86-64) at the same cost, for two reasons:
1. Graviton2's higher network bandwidth (up to 25 Gbps vs 10 Gbps for `m5n`) is the primary factor.
2. Graviton2's lower per-syscall overhead (fewer Spectre mitigations needed, as ARM was less affected by the original Spectre variant 2) provides a secondary benefit.

This is directly from the ISA: ARM's indirect branch predictor is architecturally separate from the return stack predictor, making the mitigation for Spectre variant 2 (Retpoline on x86-64) unnecessary on Graviton2.

### Parquet's Endianness Handling

Apache Parquet's file format specification states:

> "All multi-byte values in Parquet are stored in **little-endian** byte order."

This is documented in the Parquet spec (parquet-format/README.md). When a Parquet reader on a big-endian machine (IBM POWER with AIX, or a rare big-endian ARM build) reads a Parquet file, it must byte-swap every 32-bit and 64-bit value. The Arrow C++ Parquet reader handles this via a compile-time endianness check (`#if ARROW_LITTLE_ENDIAN`). The PyArrow wheel distributed on PyPI is compiled for little-endian x86-64 and AArch64 — both little-endian — so the byte-swap path is never hit in typical data engineering deployments.

The Thrift encoding used for Parquet metadata (the file header and footer) uses Thrift's compact protocol, which is also little-endian. If you write a Parquet file on x86-64 and read it on a big-endian platform without proper byte-swap, the schema metadata is also corrupted — the column names will appear as garbage.

---

## 13. Code

### Inspect Any Binary's ISA

```python
"""
inspect_isa.py

Reads the ELF header of a binary or shared library to determine its ISA.
Works on Linux. Useful for debugging "exec format error" on cross-arch deployments.

Run: python inspect_isa.py /usr/bin/python3
     python inspect_isa.py $(python3 -c "import numpy; import os; \
         print(os.path.join(os.path.dirname(numpy.__file__), 'core', '_multiarray_umath.so'))")
"""
import struct
import sys
import os

EI_CLASS = {1: "32-bit ELF", 2: "64-bit ELF"}
EI_DATA  = {1: "Little-endian", 2: "Big-endian"}
E_MACHINE = {
    3:   "x86 (EM_386)",
    40:  "ARM 32-bit (EM_ARM)",
    62:  "x86-64 (EM_X86_64)",
    183: "AArch64 / ARM64 (EM_AARCH64)",
    243: "RISC-V (EM_RISCV)",
    20:  "PowerPC (EM_PPC)",
    21:  "PowerPC 64-bit (EM_PPC64)",
}

def inspect_elf(path: str) -> None:
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    with open(path, "rb") as f:
        magic = f.read(4)
        if magic != b"\x7fELF":
            print(f"{path}: Not an ELF binary (magic bytes: {magic.hex()})")
            return

        ei_class = struct.unpack("B", f.read(1))[0]
        ei_data  = struct.unpack("B", f.read(1))[0]
        f.seek(18)  # e_machine is at offset 18 in ELF header
        e_machine = struct.unpack("<H", f.read(2))[0]  # always LE in ELF header

    print(f"\nFile:        {path}")
    print(f"Class:       {EI_CLASS.get(ei_class, f'Unknown ({ei_class})')}")
    print(f"Endianness:  {EI_DATA.get(ei_data, f'Unknown ({ei_data})')}")
    print(f"ISA:         {E_MACHINE.get(e_machine, f'Unknown ({e_machine})')}")
    print(f"e_machine:   0x{e_machine:04x}")

if __name__ == "__main__":
    paths = sys.argv[1:] if len(sys.argv) > 1 else ["/usr/bin/python3"]
    for path in paths:
        inspect_elf(path)

    # Bonus: inspect all .so files in NumPy
    try:
        import numpy
        import glob
        numpy_dir = os.path.dirname(numpy.__file__)
        so_files = glob.glob(f"{numpy_dir}/**/*.so", recursive=True)
        if so_files:
            print(f"\n--- NumPy native libraries ---")
            for so in so_files[:5]:
                inspect_elf(so)
    except ImportError:
        pass
```

### Measure Syscall Overhead

```python
"""
syscall_cost.py

Measures the per-call overhead of a minimal syscall (getpid) vs a
pure-Python function call, to quantify the user→kernel transition cost.

Run: python syscall_cost.py
"""
import os
import time
import ctypes

ITERATIONS = 1_000_000

# ─── Method 1: Python function call (no syscall) ─────────────────────────
def pure_python_noop(x):
    return x + 1

t0 = time.perf_counter()
for i in range(ITERATIONS):
    pure_python_noop(i)
python_call_ns = (time.perf_counter() - t0) * 1e9 / ITERATIONS
print(f"Python function call:  {python_call_ns:.1f} ns/call")

# ─── Method 2: os.getpid() — Python overhead + syscall ───────────────────
t0 = time.perf_counter()
for _ in range(ITERATIONS):
    os.getpid()
getpid_ns = (time.perf_counter() - t0) * 1e9 / ITERATIONS
print(f"os.getpid() (Python):  {getpid_ns:.1f} ns/call")

# ─── Method 3: getpid via ctypes — minimal Python overhead ───────────────
libc = ctypes.CDLL("libc.so.6", use_errno=True)
libc.getpid.restype = ctypes.c_int
libc.getpid.argtypes = []

# Warm up
for _ in range(1000):
    libc.getpid()

t0 = time.perf_counter()
for _ in range(ITERATIONS):
    libc.getpid()
ctypes_ns = (time.perf_counter() - t0) * 1e9 / ITERATIONS
print(f"getpid via ctypes:     {ctypes_ns:.1f} ns/call")

print(f"\nEstimated syscall overhead: ~{ctypes_ns - python_call_ns:.0f} ns")
print(f"  (ctypes call minus pure Python function call)")
print(f"\nFor context:")
print(f"  L3 cache hit:    ~30 ns")
print(f"  DRAM access:     ~80 ns")
print(f"  1 Gbps network RTT: ~100,000 ns")

# Expected output (Linux, x86-64, no KPTI bypass):
# Python function call:  100-200 ns/call  (Python overhead)
# os.getpid() (Python):  300-500 ns/call
# getpid via ctypes:     200-350 ns/call
# Estimated syscall overhead: ~100-200 ns
```

### Endianness in Practice

```python
"""
endianness.py

Demonstrates endianness effects in:
1. Raw memory layout
2. Kafka-style protocol parsing (big-endian)
3. Parquet-style data (little-endian)

Run: python endianness.py
"""
import struct
import sys

print(f"This machine is: {'little' if sys.byteorder == 'little' else 'big'}-endian\n")

# ─── 1. Raw bytes of an integer ──────────────────────────────────────────
value = 0x01234567
le_bytes = struct.pack("<I", value)  # little-endian
be_bytes = struct.pack(">I", value)  # big-endian

print(f"Integer value: 0x{value:08x} ({value:,})")
print(f"Little-endian bytes: {le_bytes.hex()} ({''.join(f'{b:02x} ' for b in le_bytes)}.strip())")
print(f"Big-endian bytes:    {be_bytes.hex()} ({''.join(f'{b:02x} ' for b in be_bytes)}.strip())")
print()

# ─── 2. Kafka wire protocol simulation (big-endian) ───────────────────────
def encode_kafka_message(key: bytes, value: bytes) -> bytes:
    """Encode a simplified Kafka message frame (big-endian lengths)."""
    key_len = struct.pack(">i", len(key))      # >i = big-endian int32
    value_len = struct.pack(">i", len(value))  # >i = big-endian int32
    return key_len + key + value_len + value

def decode_kafka_message(frame: bytes) -> tuple:
    """Decode the simplified Kafka message frame."""
    key_len = struct.unpack(">i", frame[0:4])[0]
    key = frame[4:4+key_len]
    offset = 4 + key_len
    value_len = struct.unpack(">i", frame[offset:offset+4])[0]
    value = frame[offset+4:offset+4+value_len]
    return key, value

key = b"user-123"
value = b'{"event": "purchase", "amount": 99.99}'
frame = encode_kafka_message(key, value)

print("Kafka-style encoding (big-endian):")
print(f"  Key:   {key!r}")
print(f"  Value: {value!r}")
print(f"  Wire:  {frame[:16].hex()}...  (first 16 bytes)")
print(f"  First 4 bytes (key length): {frame[:4].hex()} = {struct.unpack('>i', frame[:4])[0]}")

# Bug demonstration: reading with wrong byte order
wrong_size = struct.unpack("<i", frame[:4])[0]
print(f"  WRONG (little-endian read): {wrong_size:,} bytes  ← corrupt!")
k, v = decode_kafka_message(frame)
print(f"  Correct decode: key={k!r}, value={v!r}")
print()

# ─── 3. Parquet-style encoding (little-endian) ───────────────────────────
def encode_parquet_int32_page(values: list) -> bytes:
    """Encode a Parquet-style data page (little-endian int32 values)."""
    return struct.pack(f"<{len(values)}i", *values)

def decode_parquet_int32_page(data: bytes, count: int) -> list:
    return list(struct.unpack(f"<{count}i", data[:count*4]))

parquet_values = [100, 200, 300, 400, 500]
parquet_page = encode_parquet_int32_page(parquet_values)

print("Parquet-style encoding (little-endian):")
print(f"  Values: {parquet_values}")
print(f"  Bytes:  {parquet_page.hex()}")
print(f"  First 4 bytes (value 100 = 0x64): {parquet_page[:4].hex()}")
decoded = decode_parquet_int32_page(parquet_page, 5)
print(f"  Decoded: {decoded}")
print()

# ─── 4. How Python's struct module handles this ───────────────────────────
print("struct format character reference:")
print("  >  = big-endian (Kafka, network protocols, Java DataOutputStream)")
print("  <  = little-endian (Parquet, x86 native, Windows)")
print("  =  = native byte order (matches sys.byteorder)")
print("  !  = network byte order (= big-endian, POSIX convention)")
```

---

## 14. Labs

### Lab 1 — Map Python I/O to Syscalls

**Goal:** Use `strace` to watch a Python file read at the syscall level and identify each step in the kernel boundary crossing.

```bash
# Step 1: Create a test file
echo "hello kafka world" > /tmp/test.txt

# Step 2: Trace the syscalls made by a Python open+read
strace -e trace=openat,read,close,fstat python3 -c "
f = open('/tmp/test.txt')
data = f.read()
f.close()
print(data)
"
# Expected output (annotated):
# openat(AT_FDCWD, "/tmp/test.txt", O_RDONLY|O_CLOEXEC) = 3
# fstat(3, {..., st_size=18, ...}) = 0
# read(3, "hello kafka world\n", 4096) = 18
# read(3, "", 4096) = 0      ← EOF
# close(3) = 0

# Step 3: Trace open() on a Parquet file
pip install pyarrow
strace -e trace=openat,read,lseek,pread64 python3 -c "
import pyarrow.parquet as pq
import pyarrow as pa
# Create a tiny Parquet file
t = pa.table({'x': [1,2,3], 'y': [4,5,6]})
pq.write_table(t, '/tmp/test.parquet')
# Read it back
result = pq.read_table('/tmp/test.parquet')
print(result)
"
# Question: How many read() syscalls does PyArrow make to read the Parquet file?
# Hint: Parquet reads the footer first (last bytes of file) to get metadata,
#       then reads specific column chunks. Count the pread64 calls.
```

**Questions to answer:**
1. How many syscalls does `open()` in Python generate?
2. Why does `read()` appear twice — once with 18 bytes returned and once with 0?
3. How many `pread64` calls does PyArrow make for a 3-row Parquet file? Why?

---

### Lab 2 — Detect Architecture and Calling Convention

**Goal:** Use Python ctypes to call a C function and observe how arguments map to registers using `gdb` (or just verify the calling convention by reading assembly).

```python
"""
lab2_calling_convention.py

Creates a tiny C shared library, calls it from Python via ctypes,
and demonstrates how Python arguments map to registers per the System V ABI.

Requirements: gcc or clang
Run: python lab2_calling_convention.py
"""
import ctypes
import os
import subprocess
import struct
import sys
import tempfile

# Write a tiny C function
c_code = """
#include <stdint.h>
#include <stdio.h>

// This function takes 6 args to show register allocation
// On x86-64: arg1→rdi, arg2→rsi, arg3→rdx, arg4→rcx, arg5→r8, arg6→r9
int64_t add_six_args(int64_t a, int64_t b, int64_t c,
                     int64_t d, int64_t e, int64_t f) {
    printf("  Inside C: a=%ld b=%ld c=%ld d=%ld e=%ld f=%ld\\n", a, b, c, d, e, f);
    return a + b + c + d + e + f;
}

// Float args go in xmm registers
double add_floats(double x, double y) {
    return x + y;
}
"""

# Compile
tmpdir = tempfile.mkdtemp()
c_path = os.path.join(tmpdir, "abi.c")
so_path = os.path.join(tmpdir, "abi.so")

with open(c_path, "w") as f:
    f.write(c_code)

ret = subprocess.run(["gcc", "-O0", "-shared", "-fPIC", "-o", so_path, c_path],
                     capture_output=True, text=True)
if ret.returncode != 0:
    print("Compilation failed:", ret.stderr)
    sys.exit(1)

# Load and call
lib = ctypes.CDLL(so_path)
lib.add_six_args.restype = ctypes.c_int64
lib.add_six_args.argtypes = [ctypes.c_int64] * 6

lib.add_floats.restype = ctypes.c_double
lib.add_floats.argtypes = [ctypes.c_double, ctypes.c_double]

print("Calling add_six_args(10, 20, 30, 40, 50, 60):")
print(f"  System V ABI register mapping:")
print(f"    10 → rdi,  20 → rsi,  30 → rdx")
print(f"    40 → rcx,  50 → r8,   60 → r9")
result = lib.add_six_args(10, 20, 30, 40, 50, 60)
print(f"  Result: {result}  (expected: 210)")

print(f"\nCalling add_floats(3.14, 2.71):")
print(f"  System V ABI: floats go in xmm0, xmm1 (not rdi, rsi!)")
result_f = lib.add_floats(3.14, 2.71)
print(f"  Result: {result_f:.4f}  (expected: 5.8500)")

# Verify architecture
arch = os.uname().machine
print(f"\nThis system: {arch}")
if arch == "x86_64":
    print("Registers used: rdi, rsi, rdx, rcx, r8, r9 for integer args")
    print("                xmm0, xmm1 ... xmm7 for float args")
elif arch == "aarch64":
    print("Registers used: x0, x1, x2, x3, x4, x5, x6, x7 for integer args")
    print("                v0, v1 ... v7 for float args (AAPCS64)")

# Cleanup
import shutil
shutil.rmtree(tmpdir)
```

---

### Lab 3 — Graviton Feasibility Check for Your Python Stack

**Goal:** Check whether all packages in your current Python environment have AArch64 wheels available on PyPI, simulating a Graviton migration assessment.

```python
"""
lab3_graviton_feasibility.py

Checks PyPI for AArch64 (Graviton) wheel availability for all installed packages.
Useful before migrating a Spark cluster to AWS Graviton.

Run: python lab3_graviton_feasibility.py
"""
import subprocess
import sys
import json
import urllib.request

def get_installed_packages():
    result = subprocess.run(
        [sys.executable, "-m", "pip", "list", "--format=json"],
        capture_output=True, text=True
    )
    return json.loads(result.stdout)

def check_aarch64_wheel(package_name: str, version: str) -> dict:
    """Check if a package has an aarch64 wheel on PyPI."""
    url = f"https://pypi.org/pypi/{package_name}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
            urls = data.get("urls", [])
            wheels = [u for u in urls if u["filename"].endswith(".whl")]
            aarch64_wheels = [w for w in wheels if "aarch64" in w["filename"]]
            x86_wheels = [w for w in wheels if "x86_64" in w["filename"]]
            return {
                "has_aarch64_wheel": len(aarch64_wheels) > 0,
                "has_x86_wheel": len(x86_wheels) > 0,
                "is_pure_python": all(
                    "py3-none-any" in w["filename"] or "py2.py3-none-any" in w["filename"]
                    for w in wheels
                ) if wheels else False,
                "aarch64_wheels": [w["filename"] for w in aarch64_wheels],
            }
    except Exception as e:
        return {"error": str(e)}

packages = get_installed_packages()
print(f"Checking {len(packages)} installed packages for AArch64 (Graviton) compatibility...\n")

issues = []
for pkg in packages[:30]:  # limit to 30 for demo
    name = pkg["name"]
    version = pkg["version"]
    result = check_aarch64_wheel(name, version)

    if result.get("error"):
        status = f"⚠️  ERROR: {result['error']}"
    elif result.get("is_pure_python"):
        status = "✅  Pure Python (no binary needed)"
    elif result.get("has_aarch64_wheel"):
        status = "✅  AArch64 wheel available"
    else:
        status = "❌  NO aarch64 wheel — needs source build or alternative"
        issues.append(f"{name}=={version}")

    print(f"{name:30s} {version:10s}  {status}")

if issues:
    print(f"\n⚠️  Packages requiring attention for Graviton migration:")
    for pkg in issues:
        print(f"   {pkg}")
else:
    print("\n✅  All checked packages have AArch64 wheels.")

print("\nNote: Pure Python packages work on any ISA without recompilation.")
print("Only packages with C extensions (.so files) need ISA-specific wheels.")
```

---

## 15. Summary

The ISA (Instruction Set Architecture) is the contract between software and hardware. It defines what instructions exist, what registers are available, how the stack is managed (calling convention), how user code requests OS services (syscalls), how memory is accessed, and what byte order is used. The microarchitecture is the implementation beneath this contract; the ISA is the stable interface above it.

**x86-64** (Intel/AMD): 16 general-purpose registers, variable-length CISC instructions, System V calling convention on Linux (rdi/rsi/rdx/rcx/r8/r9 for integer args), strong TSO memory model, AVX2/AVX-512 SIMD, `syscall` instruction for kernel entry.

**AArch64** (ARM): 31 general-purpose registers, fixed-width RISC instructions, AAPCS64 calling convention (x0–x7 for args), weak memory model (requires explicit barriers), NEON/SVE SIMD, `svc #0` for kernel entry. Used in AWS Graviton, Apple Silicon. Graviton3 offers ~40% better price-performance than `c5`/`c6i` for Spark workloads.

**RISC-V**: Open, royalty-free ISA. 32 GP registers, fixed-width instructions, `ecall` for syscalls. Growing in embedded and hyperscaler research use.

**Endianness:** x86-64 and AArch64 are little-endian. Network protocols (including Kafka wire protocol) are big-endian. Parquet data is little-endian. Always specify byte order explicitly in binary protocol parsing.

**System calls:** The mechanism for user-space code to request kernel services. Each ISA has its own syscall instruction and syscall number table. Cost: ~100–300 ns, driven by privilege escalation, pipeline flush, and Spectre mitigations. Kafka, Spark, and all I/O-heavy data systems are designed to minimize syscall count.

**The practical data engineering takeaway:** When you choose an EC2 instance type, you are choosing an ISA. When you install a Python package, you download an ISA-specific wheel. When you write concurrent code that runs on Graviton, you must use explicit synchronization — you cannot rely on x86-64's TSO implicit ordering.

---

## 16. Interview Q&A

### Q1: What is the difference between an ISA and a microarchitecture? Give a concrete data engineering example where the distinction matters.

**Answer:**

The ISA (Instruction Set Architecture) is a specification — a formal document that defines what instructions a CPU must execute, what registers exist, how memory is addressed, and what the results must be. The microarchitecture is the physical implementation of that specification: the pipeline depth, the number of execution units, the branch predictor algorithm, the cache sizes.

The distinction is the same as the difference between an API contract and its implementation. Multiple CPUs can implement the same ISA — Intel Core, AMD Ryzen, and Via C7 all implement x86-64. Multiple ISAs are implemented by the same company — ARM Ltd writes the AArch64 ISA, but Apple, AWS (Graviton), Qualcomm, and Ampere all implement it differently with different microarchitectures.

A concrete data engineering example: AWS Graviton3 and Apple M3 both implement AArch64. They run the same PySpark JAR, the same Python bytecode, the same compiled NumPy wheels. From the ISA perspective, they are interchangeable. But their microarchitectures are completely different: Apple M3 has a 600-entry ROB optimized for single-thread performance; Graviton3 has a smaller ROB optimized for many-core throughput and power efficiency. The same Spark job may run 30% faster on Apple M3 for single-partition tasks and 20% faster on Graviton3 for large multi-partition shuffles. The ISA didn't change; the microarchitectural trade-offs explain the difference.

---

### Q2: A colleague says "I'll just use the same Docker image on our Graviton Spark cluster as we use for x86 development." What problems might they encounter and how would you resolve them?

**Answer:**

The JVM-based parts — the Spark runtime, Scala/Java code, any pure Python — will work fine, because the JVM JIT compiles bytecode to native instructions at runtime. The problems are in the native layers.

First, any Python packages with C extensions — NumPy, PyArrow, pandas, pyarrow, cryptography, lz4, snappy — are compiled to x86-64 `.so` files inside the image. When the Graviton kernel's ELF loader tries to execute them, it reads `e_machine = EM_X86_64` in the ELF header and returns `ENOEXEC` — you'll see `exec format error` or `cannot open shared object file: wrong ELF class`.

Second, any compiled binaries in the image — `hdfs`, `hadoop`, native tools — face the same issue.

Third, native libraries called by Spark itself: the Arrow C++ library (used for PySpark Pandas UDFs), libz (zlib compression for Parquet), libssl (TLS for secure Kafka connections) — all must be AArch64.

The solution is to build a multi-architecture Docker image using `docker buildx --platform linux/amd64,linux/arm64`. PyPI now provides `manylinux_2_28_aarch64` wheels for all major data engineering packages (NumPy, PyArrow, pandas, cryptography) so `pip install` will select the correct wheel automatically. For packages without pre-built wheels, you compile from source during `docker build` by installing `gcc` and the appropriate dev headers.

The migration checklist: inventory all `.so` files in your image with `find / -name '*.so' -exec file {} \;`, identify x86-64-only ones, and find or build AArch64 replacements.

---

### Q3: Explain what a system call is. Why does Kafka's architecture specifically try to minimize syscall count?

**Answer:**

A system call is the mechanism by which user-space code (ring 3) requests a service from the OS kernel (ring 0). On x86-64 Linux, the user-space code places the syscall number in `rax`, arguments in `rdi/rsi/rdx/r10/r8/r9`, then executes the `syscall` instruction. The CPU atomically saves the return address, escalates privilege to ring 0, switches to the kernel stack, and jumps to the kernel's syscall handler. After the kernel completes the request, `sysret` reverses the process.

The cost is high relative to a function call: ~100–300 nanoseconds versus ~1 nanosecond for a function call. Three factors drive this: the hardware privilege escalation (a serializing event), the pipeline flush required to prevent speculative execution from leaking kernel data across the privilege boundary (the fundamental mechanism behind Spectre mitigations), and the context switch to the kernel's page tables (KPTI, the Meltdown mitigation).

Kafka's architecture minimizes syscall count in three specific ways. First, `sendfile()` instead of `read()` + `write()`: a conventional serve-from-disk path reads file data into a user-space buffer (`read()` syscall) then sends it to the socket (`write()` or `send()` syscall) — two syscalls and a data copy. `sendfile()` does both in one syscall using the kernel's zero-copy path: the page cache serves data directly to the socket buffer without ever copying it to user space. Second, batching: Kafka's producer batches multiple messages into a single `send()` call. Instead of 1,000 `send()` syscalls for 1,000 messages, it makes one `send()` with the entire batch. Third, `epoll` for I/O multiplexing: instead of one thread per connection (each blocking on `read()` syscalls), Kafka uses a single thread with `epoll_wait()` that wakes up only when a socket has data, handling thousands of connections per thread.

---

### Q4: What is endianness, and how does it affect a Kafka consumer reading from a raw socket?

**Answer:**

Endianness defines which byte of a multi-byte integer is stored at the lowest memory address. Little-endian stores the least significant byte first (x86-64 and AArch64 default). Big-endian stores the most significant byte first (historically: network protocols, IBM POWER, Java's DataOutputStream).

The Kafka wire protocol is big-endian. Every length field, offset, and integer in the Kafka protocol is encoded as a big-endian value before being written to the socket. This is the POSIX convention called "network byte order" and is consistent with TCP/IP protocol headers.

If a Kafka consumer running on an x86-64 (little-endian) machine reads 4 bytes from the socket and interprets them as a native little-endian int32, it gets the byte-reversed value. For example, a message size of 1000 bytes is encoded as `0x000003E8` in big-endian, which is the byte sequence `00 00 03 E8`. A little-endian misread of this sequence interprets `E8 03 00 00` as the integer `0x000003E8` — wait, that's actually the same value, because this particular value has a zero in the high byte. The corruption is obvious when the high byte is non-zero: a message size of `0x01000000` (16,777,216 bytes) misread as little-endian becomes `0x00000001` (1 byte), causing the consumer to believe the next message is 1 byte long and fail to parse it.

Official Kafka client libraries (kafka-python, confluent-kafka, Java KafkaConsumer) handle byte-order correctly. The issue appears only when writing a custom protocol parser or debugging at the packet level with Wireshark or a raw socket. The fix is always explicit: use `struct.unpack('>i', ...)` (big-endian) for Kafka protocol fields in Python, or `ByteBuffer.order(ByteOrder.BIG_ENDIAN)` in Java.

---

### Q5: How does the AArch64 memory model differ from x86-64, and why does this matter when migrating a Java-based data pipeline to AWS Graviton?

**Answer:**

x86-64 implements Total Store Order (TSO): the only reordering it permits is a write being delayed in the store buffer before becoming globally visible to other cores. Reads are never reordered relative to other reads; writes are never reordered relative to other writes. This means that in practice, x86-64 programs can often skip explicit memory barriers and still work correctly — the hardware provides enough implicit ordering.

AArch64 implements a significantly weaker memory model. Loads can be reordered with other loads, stores can be reordered with other stores, and loads can be reordered with prior stores to different addresses. The only guarantee is that a core sees its own writes in program order. Without explicit memory barriers (`dmb ish` — data memory barrier, inner shareable), two cores can observe each other's writes in an order completely different from program order.

For Java-based systems like Spark and Kafka, this is handled correctly by the JVM: `volatile` fields generate `dmb ish` barriers on AArch64, `synchronized` blocks generate acquire/release barriers, and `java.util.concurrent` classes use Unsafe internally with the correct barriers. Correct JMM code is portable across ISAs.

The problem arises in three situations. First, if a Java codebase uses `sun.misc.Unsafe` directly and relies on x86-64's TSO to avoid explicit barriers — this code is incorrect on AArch64. Second, if native libraries called from JNI (C/C++ code for compression, crypto, or custom serialization) contain lock-free data structures that were only tested on x86-64. Third, if you have Rust or C extensions that use relaxed atomic operations and were implicitly relying on x86-64's stronger hardware ordering.

The migration test: run the full integration test suite on an AArch64 CI runner before deploying to Graviton. Use the ThreadSanitizer (`-fsanitize=thread` for C/C++, thread sanitizer agent for JVM) to detect data races that only manifest under weak memory ordering.

---

### Q6: What is a calling convention, and what breaks if two functions compiled by different compilers (or languages) disagree on it?

**Answer:**

A calling convention is the agreed-upon protocol for how function arguments are passed, where the return value is placed, which registers the caller must save before a call, and how the stack is managed. On x86-64 Linux, the System V AMD64 ABI specifies: the first six integer arguments go in rdi, rsi, rdx, rcx, r8, r9 in that order; the return value goes in rax; the caller must save rax/rcx/rdx/rsi/rdi/r8/r9/r10/r11 before any call (since the callee may clobber them); and the callee must preserve rbx/rbp/r12/r13/r14/r15. The stack pointer must be 16-byte aligned when a `call` instruction is executed.

If two functions disagree on the calling convention, the symptoms depend on which aspect they disagree on.

If they disagree on which register holds argument 1: the callee reads garbage from the register it expects, computes a wrong result, and returns it. The symptoms can range from obviously wrong output to memory corruption (if the garbage value is used as a pointer) to segfaults.

If they disagree on which registers are caller-saved: the callee overwrites a register the caller expected to be preserved. The caller reads a wrong value after the call returns, potentially corrupting data structures. This is particularly subtle because the corruption happens after the call returns — the stack trace points to the caller's code, not the callee.

If the stack is not 16-byte aligned: SSE/AVX instructions that require aligned access (`movaps`, `movdqa`) will fault with a SIGSEGV. This is a common source of segfaults when calling SIMD-using library code from handwritten assembly.

In Python's ctypes, this manifests as incorrect results or crashes when you call a C function with the wrong `argtypes`/`restype` specification — ctypes generates the call based on your specification, and if it doesn't match what the C function expects, you get UB. The solution is always to look up the function's C signature, map each type to the correct ctypes type, and verify with a debug build before trusting results.

---

## 17. Cross-Question Chain

**Topic:** From "what is an ISA?" to "why does Graviton need different Docker images?"

---

**Interviewer:** What is an ISA?

**Candidate:** An ISA — Instruction Set Architecture — is the contract between software and hardware. It is a specification document that defines what instructions a CPU must execute, what registers exist, how memory addressing works, how the stack is managed for function calls, and how user-space code requests OS services. Any CPU that implements the spec runs the same compiled binaries. The ISA is stable across CPU generations; the microarchitecture beneath it changes every few years. Intel has maintained x86-64 backward compatibility since 2003.

---

**Interviewer:** What are the two major ISAs used in cloud data engineering today?

**Candidate:** x86-64 and AArch64. x86-64 is the ISA from Intel and AMD, used in AWS EC2 `c5`, `m5`, `r5`, `c6i`, `m6i` instances — historically the default for Spark, Kafka, and all data workloads. AArch64 is ARM's 64-bit ISA, used in AWS Graviton2 (`m6g`, `c6g`, `r6g`) and Graviton3 (`m7g`, `c7g`). They have different register files, different calling conventions, different system call instructions, different SIMD register widths, and — critically — different memory models. AArch64 is becoming significant because Graviton instances offer roughly 40% better price-performance for memory-bandwidth-bound workloads like Spark columnar processing.

---

**Interviewer:** If the JVM is "write once, run anywhere," why do Spark clusters on Graviton need anything different from x86-64?

**Candidate:** The JVM itself is architecture-neutral for JVM bytecode — a `.jar` file runs on any JVM regardless of ISA. The JIT compiler handles the ISA translation at runtime. But modern data engineering is not pure JVM. PySpark uses PyArrow, which has a C++ native library (the Arrow C++ implementation). NumPy is a C extension. The `lz4` and `snappy` compression libraries used in Parquet are native. These are compiled to ISA-specific machine code — x86-64 `.so` files. When the JVM calls into these libraries via JNI or when Python imports them, the OS ELF loader checks the `e_machine` field in the binary header. An x86-64 binary has `e_machine = 62`; AArch64 is `183`. If they don't match the running kernel's architecture, the loader returns `ENOEXEC` — "exec format error."

---

**Interviewer:** So what's the practical fix when building a Docker image for a Graviton Spark cluster?

**Candidate:** Two things. First, build the Docker image for the correct architecture using `docker buildx --platform linux/arm64`. This instructs the builder to run `pip install` under an AArch64 environment, which causes pip to download `manylinux_2_28_aarch64` wheels instead of `x86_64` wheels. All major data engineering packages — NumPy, PyArrow, pandas, cryptography — publish AArch64 wheels on PyPI. For packages without pre-built AArch64 wheels, pip compiles from source, which requires `gcc` and the appropriate `dev` packages in the Docker image.

Second, verify before deploying. Run `find /usr -name '*.so' | xargs file | grep x86` to check that no x86-64 binaries slipped in. Test the full job on a single Graviton instance before migrating the cluster. Pay special attention to lock-free data structures in any native code — AArch64's weaker memory model can expose races that x86-64's TSO ordering was masking.

---

**Interviewer:** You mentioned memory models. What's the difference between x86-64's TSO and AArch64's model, and when would a Spark job care?

**Candidate:** x86-64 Total Store Order means the only reordering allowed is a store being delayed in the store buffer before becoming visible to other cores. Reads always see prior writes from the same core, and reads from other cores are always in write order. This is a strong guarantee that implicitly provides acquire/release semantics for most patterns — a flag write followed by a data write is visible to other cores in that order, without a memory barrier.

AArch64's weak memory model allows nearly any reordering: loads before other loads, stores before other stores, loads before prior stores to different addresses. Without explicit `dmb ish` barrier instructions, two AArch64 cores can observe each other's stores in any order.

A Spark job typically doesn't care, because Spark runs on the JVM, and the JVM's memory model (JMM) explicitly requires `volatile` and `synchronized` to insert the correct barriers. The JIT emits `mfence` on x86-64 and `dmb ish` on AArch64 for `volatile` accesses. The problem appears in three scenarios: native libraries (C/C++) with lock-free data structures that were only tested on x86-64; custom serializers using `sun.misc.Unsafe` without explicit memory ordering; and any Python C extension that uses atomic operations without ISA-appropriate barriers. The standard test-first, migrate-second discipline — running the full integration suite on AArch64 CI before deploying to Graviton — catches these.

---

**Interviewer:** One more level down: what physically happens when a Spark worker makes a `read()` system call on Graviton?

**Candidate:** The Kafka consumer thread, running on Graviton under the JVM, eventually calls Java NIO's `read()`, which calls into the JVM's native layer, which calls the C library's `read()` wrapper. The C library sets `x8 = 63` (the Linux AArch64 syscall number for `read` — note it's not the same as x86-64's `0`), loads the file descriptor into `x0`, the buffer pointer into `x1`, the byte count into `x2`, then executes `svc #0` — the AArch64 supervisor call instruction. The CPU saves the user-space program counter to the link register, escalates privilege from EL0 (user) to EL1 (kernel), switches to the kernel stack, and jumps to the Linux AArch64 exception vector table entry for synchronous exceptions. The kernel's `el0_svc` handler dispatches to `sys_read()` in `fs/read_write.c`. The kernel reads from the socket buffer or the page cache, copies bytes to the user-space buffer, and returns the byte count in `x0`. `eret` reverses the privilege escalation. Total elapsed time: ~100–250 ns on Graviton3, slightly less than on x86-64 because Graviton3 has hardware mitigations for Spectre variant 2 that eliminate the need for the software Retpoline mitigation used on older x86-64 CPUs.

---

## 18. What's Next

**M04: CPU Performance Analysis** — Now that you know what the CPU is doing (M01–M03), the next step is knowing how to measure it. `perf stat` for IPC and branch misses; `perf record` + flamegraphs to identify hot paths; hardware performance counters (PMCs) for cache misses and SIMD utilization; `strace` and `ltrace` for syscall tracing. The tools that make the theory observable.

**M05: From Python to Electrons** — The capstone of CSF-ARC-101: trace one Python statement all the way from CPython bytecode through the C runtime, through the ISA, through the microarchitecture, to the electrons in the transistors. Synthesizes M01–M04.

**CSF-ARC-102: Memory Architecture** — The other half of computer architecture. The ISA defines how the CPU accesses memory; CSF-ARC-102 explains what happens once that access leaves the CPU: cache lines, NUMA, TLB, the prefetcher, and why memory access patterns dominate the performance of data-intensive workloads.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is an ISA? | The specification of what instructions a CPU executes, what registers exist, how memory is addressed, and how user code interacts with the OS. The contract between software and hardware. |
| 2 | What is the difference between ISA and microarchitecture? | ISA = the contract (the spec). Microarchitecture = the implementation (the physical CPU design). Multiple CPUs can implement the same ISA differently. |
| 3 | How many GP registers does x86-64 have? | 16 (rax, rbx, rcx, rdx, rsi, rdi, rbp, rsp, r8–r15). Plus rip and rflags. Plus 16 SIMD registers (xmm/ymm/zmm). |
| 4 | In the x86-64 System V ABI, what register holds the 1st function argument? | rdi |
| 5 | In the x86-64 System V ABI, what register holds the function return value? | rax (for integers/pointers); xmm0 (for floats/doubles) |
| 6 | What are caller-saved registers in the System V ABI? | rax, rcx, rdx, rsi, rdi, r8, r9, r10, r11 and all xmm/ymm. The callee can overwrite them freely. |
| 7 | What are callee-saved registers in the System V ABI? | rbx, rbp, r12, r13, r14, r15. The callee must push/pop these if it uses them. |
| 8 | What instruction triggers a syscall on x86-64 Linux? | `syscall`. The syscall number is in rax; args in rdi, rsi, rdx, r10, r8, r9. |
| 9 | What instruction triggers a syscall on AArch64 Linux? | `svc #0`. The syscall number is in x8; args in x0–x5. |
| 10 | Why does a syscall cost 100–300 ns? | Privilege escalation (hardware gate), pipeline flush (Spectre mitigation), kernel stack switch, and post-Meltdown KPTI page table switch. |
| 11 | What is endianness? | The byte order in which multi-byte values are stored. Little-endian: LSB first (x86-64, AArch64). Big-endian: MSB first (network byte order, Kafka wire protocol). |
| 12 | What byte order does the Kafka wire protocol use? | Big-endian (network byte order). |
| 13 | What byte order does Parquet use for column data? | Little-endian. |
| 14 | What is the Python struct format for big-endian int32? | `'>i'` (or `'!i'` for network byte order, which is also big-endian). |
| 15 | What memory model does x86-64 use? | Total Store Order (TSO). The only reordering allowed: a write may be delayed in the store buffer before becoming globally visible. |
| 16 | What memory model does AArch64 use? | Weak memory ordering. Loads and stores can be reordered in nearly any way without explicit `dmb` barrier instructions. |
| 17 | What is the ELF `e_machine` value for x86-64? | 62 (EM_X86_64) |
| 18 | What is the ELF `e_machine` value for AArch64? | 183 (EM_AARCH64) |
| 19 | Why does a Spark `.jar` file run on both x86-64 and AArch64, but a NumPy wheel does not? | A JAR contains JVM bytecode; the JVM JIT compiles to native instructions at runtime. A NumPy wheel contains a compiled `.so` for a specific ISA — the ELF loader rejects the wrong ISA. |
| 20 | How does `docker buildx --platform linux/arm64` help for Graviton? | It builds the Docker image under an AArch64 environment, causing `pip install` to download AArch64-specific wheels instead of x86-64 wheels. |

---

## 20. References

**ISA Specifications (Primary Sources)**

- Intel Corporation (2023). *Intel® 64 and IA-32 Architectures Software Developer's Manual*, Volumes 1–3. Document 325462. https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html — The authoritative x86-64 ISA specification.
- ARM Limited (2023). *Arm Architecture Reference Manual for A-profile architecture* (ARM DDI 0487). https://developer.arm.com/documentation/ddi0487/latest — The AArch64 ISA specification.
- RISC-V International (2023). *The RISC-V Instruction Set Manual, Volume I: Unprivileged ISA*. https://github.com/riscv/riscv-isa-manual/releases — The open RISC-V ISA spec.
- AMD (2023). *AMD64 Architecture Programmer's Manual*, Volumes 1–5. https://www.amd.com/content/dam/amd/en/documents/processor-tech-docs/programmer-references/24592.pdf

**Calling Conventions and ABI**

- System V Application Binary Interface AMD64 Architecture Processor Supplement (2023). https://gitlab.com/x86-psABIs/x86-64-ABI — The definitive System V AMD64 ABI document.
- ARM IHI0055B. *Procedure Call Standard for the ARM 64-bit Architecture (AArch64)* (AAPCS64). https://developer.arm.com/documentation/ihi0055/latest

**Memory Models**

- Sewell, P., Sarkar, S., Owens, S., Nardelli, F. Z., & Myreen, M. O. (2010). x86-TSO: A Rigorous and Usable Programmer's Model for x86 Multiprocessors. *Communications of the ACM*, 53(7). https://dl.acm.org/doi/10.1145/1785414.1785443 — The formal TSO model.
- Lochbihler, A. (2013). Making the Java Memory Model Safe. *ACM Transactions on Programming Languages and Systems*, 35(4). — JMM formal analysis.

**Graviton and Cloud Architecture**

- AWS (2023). *Amazon EC2 Graviton Processors*. https://aws.amazon.com/ec2/graviton/ — AWS Graviton3 performance claims and instance types.
- Nair, R., et al. (2021). AWS Graviton2: A new era in cloud computing. *IEEE Micro*, 41(2). — Technical deep dive into Graviton2 microarchitecture.
- Brooker, M. (2023). "Graviton3 and Data Processing." AWS Blog. https://aws.amazon.com/blogs/database/ — Real-world benchmark data for database workloads.

**Practical References**

- Kerrisk, M. (2010). *The Linux Programming Interface*. No Starch Press. **Chapter 3** (system calls), **Chapter 6** (processes). https://man7.org/tlpi/ — The definitive Linux system programming reference.
- Linux Kernel. `arch/x86/entry/entry_64.S` and `arch/arm64/kernel/entry.S` — The actual kernel syscall entry assembly for x86-64 and AArch64.
- Python `struct` module documentation: https://docs.python.org/3/library/struct.html — Format characters including byte-order prefixes.
