# M01: Branch Prediction and the Instruction Pipeline

**School:** CS Foundations (CSF)  
**Course:** CSF-ARC-103 — CPU Internals  
**Module:** 01 of 04  
**Difficulty:** ★★★★☆  
**Estimated study time:** 5–6 hours  
**Last updated:** 2026-06-27

---

## Table of Contents

1. [Why This Module Exists](#1-why-this-module-exists)
2. [Prerequisites](#2-prerequisites)
3. [Learning Objectives](#3-learning-objectives)
4. [First Principles](#4-first-principles)
5. [Architecture — The Instruction Pipeline](#5-architecture--the-instruction-pipeline)
6. [Deep Dive — Branch Predictor Algorithms](#6-deep-dive--branch-predictor-algorithms)
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

CSF-ARC-102 explained why data moves slowly: cache misses, NUMA penalties, TLB misses, and coherence traffic. This module explains why the CPU itself can stall even when data is already in L1 cache. The culprit is the instruction pipeline and its interaction with conditional branches.

A modern CPU executes up to 5–6 instructions per clock cycle by overlapping many instructions simultaneously in a deep pipeline (14–20 stages). This works beautifully for straight-line code. It breaks down at every `if`, `for` condition check, and function pointer dispatch, because the CPU does not know which instruction comes next until the branch condition is evaluated — which is several pipeline stages downstream. The CPU's solution is the **branch predictor**: a pattern-matching engine that guesses the outcome of every branch before the condition is computed, and speculatively executes the predicted path.

When the prediction is correct (the common case for predictable branches), the pipeline runs at full speed. When it is wrong, the CPU must flush the speculatively-executed instructions from the pipeline — a 15–20 cycle penalty — and restart from the correct path. For a loop body executing in 4 cycles, one misprediction every 10 iterations reduces throughput by 30–50%.

For data engineers: every `if` in a hot Python loop, every Pandas `apply` with a conditional, every Spark `filter` on a high-cardinality column is subject to branch predictor pressure. Understanding the predictor lets you rewrite these patterns as branchless vectorized operations that never stall the pipeline.

---

## 2. Prerequisites

- CSF-ARC-101 M02: Out-of-order execution, the ROB (Reorder Buffer), and instruction-level parallelism.
- CSF-ARC-101 M05: IPC measurement with `perf stat` (instructions, cycles, IPC).
- CSF-ARC-102 M04: Prefetcher and bandwidth-limited vs latency-limited workloads.

---

## 3. Learning Objectives

By the end of this module you will be able to:

1. Describe the stages of a modern CPU instruction pipeline and explain where branches cause stalls.
2. Explain the branch misprediction penalty (15–20 cycles) and calculate its impact on loop throughput.
3. Describe three branch predictor algorithms (bimodal, GShare/two-level, TAGE) and their strengths.
4. Predict which access patterns will cause high branch misprediction rates.
5. Use `perf stat -e branch-misses,branches` to measure branch predictor effectiveness.
6. Write branchless equivalents of conditional operations using `np.where`, boolean arithmetic, and CMOV-friendly patterns.
7. Explain why Spark predicate pushdown is a branch elimination technique at the data level.
8. Identify data-dependent branching in Pandas `apply` and replace it with vectorized operations.

---

## 4. First Principles

### The Pipeline Problem

A CPU pipeline breaks instruction execution into sequential stages. Each stage handles one step of processing, and multiple instructions flow through the pipeline simultaneously — like an assembly line. A typical modern pipeline:

```
Stage 1: Fetch    — Read instruction bytes from L1 instruction cache
Stage 2: Decode   — Determine what operation the instruction performs
Stage 3: Rename   — Map architectural registers to physical registers (OOO prep)
Stage 4: Dispatch — Send micro-ops to execution unit queues
Stage 5: Execute  — Perform the operation (ALU, load, store, etc.)
Stage 6: Writeback— Write result to register file / commit to architectural state
```

With a 6-stage pipeline, the CPU can have up to 6 instructions in flight simultaneously, giving up to 6× throughput over a single-cycle processor. Modern CPUs have 14–20 stages and execute up to 6 micro-ops per cycle (superscalar), giving potentially 80–120 instructions in flight.

### Why Branches Break the Pipeline

At stage 1 (Fetch), the CPU needs to know the address of the next instruction to fetch. For most instructions, this is simply the current address + instruction length — trivial. But for a conditional branch (`if x > 0: goto LABEL_A, else: goto LABEL_B`), the next fetch address depends on the outcome of the comparison, which is not known until stage 5 (Execute) — four stages later.

The CPU has three options:

1. **Stall:** Stop fetching until stage 5 computes the branch outcome. Wait 4+ cycles per branch. Unacceptable — most loops have a branch every 2–5 instructions.
2. **Always predict not-taken:** Always fetch the next sequential instruction. Correct for not-taken branches; flushed and refetched for taken branches. Simple but wrong ~50% of the time for loops.
3. **Predict using historical pattern:** Use a hardware prediction table populated by past branch behavior to guess the outcome, speculatively fetch and execute the predicted path, and flush if wrong. This is what modern CPUs do.

### The Cost of a Misprediction

When the predictor is wrong, the CPU must:
1. Detect the misprediction at Execute stage (cycle ~15–20 after Fetch).
2. Flush all ~15–20 instructions in the pipeline that were speculatively fetched on the wrong path.
3. Fetch from the correct target address.
4. Re-fill the pipeline from scratch.

The penalty is the pipeline depth: 15–20 cycles on modern Intel and AMD CPUs. For a loop body that executes in 2 cycles (two instructions, both in L1 cache), one misprediction every 20 iterations means: 20 × 2 = 40 cycles of useful work + 1 × 16 cycles penalty = 56 cycles total. Misprediction overhead: 28%.

For data-engineering hot loops processing millions of rows, this penalty dominates whenever branch outcomes are data-dependent and unpredictable.

---

## 5. Architecture — The Instruction Pipeline

### 5.1 Modern CPU Front-End and Back-End

```
╔══════════════════════════════════════════════════════════════════════╗
║              MODERN CPU PIPELINE (Intel Golden Cove / AMD Zen 4)    ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  ┌─────────────────── FRONT END ──────────────────────┐             ║
║  │                                                    │             ║
║  │  ┌──────────┐   ┌──────────┐   ║  ┌────────────┐  │             ║
║  │  │  Branch  │   │  I-Cache │   ║  │   Decode   │  │             ║
║  │  │ Predictor│──►│  Fetch   │──►║  │  (4–6 ops/ │  │             ║
║  │  │(TAGE-SC-L│   │          │   ║  │   cycle)   │  │             ║
║  │  │ history) │   │ 32–64 KB │   ║  └─────┬──────┘  │             ║
║  │  └──────────┘   └──────────┘   ║        │          │             ║
║  │        ▲               Branch target    │          │             ║
║  │        │               resolved here   │          │             ║
║  │        │ (misprediction feedback)       │          │             ║
║  └────────────────────────────────────────│──────────┘             ║
║                                           ▼                         ║
║  ┌─────────────────── BACK END ───────────────────────┐             ║
║  │                                                    │             ║
║  │  ┌─────────────────────────────────────────────┐   │             ║
║  │  │  Rename / Allocate (ROB, RS, store buffer)  │   │             ║
║  │  └───────────────────┬─────────────────────────┘   │             ║
║  │                      │                             │             ║
║  │  Execution ports (out-of-order):                   │             ║
║  │  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐   │             ║
║  │  │ Port 0 │  │ Port 1 │  │ Port 2 │  │ Port 5 │   │             ║
║  │  │  ALU   │  │  ALU   │  │  Load  │  │ Branch │   │             ║
║  │  │  SIMD  │  │  MUL   │  │  Store │  │  Jump  │   │             ║
║  │  └────────┘  └────────┘  └────────┘  └────────┘   │             ║
║  │                                                    │             ║
║  │  ┌─────────────────────────────────────────────┐   │             ║
║  │  │  Retire / Commit (ROB head, in-order)       │   │             ║
║  │  └─────────────────────────────────────────────┘   │             ║
║  └────────────────────────────────────────────────────┘             ║
║                                                                      ║
║  Pipeline depth:  ~14–20 stages (front-end to execute to retire)    ║
║  Misprediction penalty: 15–20 cycles (flush + refill pipeline)      ║
║  Superscalar width:     4–6 µops/cycle (Intel Alder Lake: 6)        ║
╚══════════════════════════════════════════════════════════════════════╝
```

### 5.2 Branch Prediction in the Fetch Stage

The branch predictor must supply the next fetch address before the current instruction is even decoded. This means it must:

1. Predict whether the current instruction is a branch at all (before decode confirms it).
2. Predict whether the branch will be taken or not-taken.
3. Predict the target address for taken branches.

Three hardware structures work together:

```
BRANCH PREDICTION HARDWARE:

1. Branch Target Buffer (BTB):
   Hash table: {instruction_PC → predicted_target_address}
   Size: ~4K–8K entries (Intel) / ~8K entries (AMD Zen 4)
   Hit: supplies the target address for taken branches
   Miss: must wait for decode to identify the branch → 1–2 cycle penalty

2. Directional Predictor (the "taken/not-taken" guesser):
   TAGE (Tagged Geometric history) on modern CPUs
   See Section 6 for algorithm details
   Accuracy: 95–99% for predictable code, ~50% for random branches

3. Return Address Stack (RAS):
   Hardware stack of ~16–32 return addresses
   Handles function call/return pairs (CALL pushes, RET pops)
   Accuracy: ~100% for regular call/return (falls back to BTB for indirect calls)
```

### 5.3 Speculative Execution and Flush

When the predictor fires and the front-end fetches instructions from the predicted path, those instructions enter the pipeline speculatively. They are marked as "speculative" in the Reorder Buffer (ROB). They may read memory, compute values, and update internal state — but they cannot **commit** (become architecturally visible) until the branch is resolved.

When the branch executes at the Execute stage (15–20 cycles after fetch):
- **Prediction correct:** ROB entries become eligible for commit. Pipeline continues uninterrupted.
- **Prediction wrong:** All ROB entries younger than the branch are marked for squash. Their register writes are discarded (the rename map is restored to the pre-speculation state). Memory loads they issued are invalidated. The front-end is redirected to the correct target. The BTB and directional predictor are updated with the correct outcome.

The squash + redirect cost is the misprediction penalty: 15–20 cycles.

---

## 6. Deep Dive — Branch Predictor Algorithms

### 6.1 Bimodal Predictor (1990s baseline)

The simplest dynamic predictor. A table of 2-bit saturating counters indexed by the low bits of the branch instruction's PC.

```
Bimodal predictor table (4K entries example):
  Index: PC[13:2] (12 bits → 4096 entries)
  Each entry: 2-bit saturating counter

  Counter states:
    00 = Strongly Not Taken  →  predict NOT TAKEN
    01 = Weakly Not Taken    →  predict NOT TAKEN
    10 = Weakly Taken        →  predict TAKEN
    11 = Strongly Taken      →  predict TAKEN

  Update rule:
    If branch was TAKEN:     increment counter (saturate at 11)
    If branch was NOT TAKEN: decrement counter (saturate at 00)

Example — loop branch (taken 99 times, not-taken once):
  After warmup: counter = 11 (Strongly Taken)
  Iterations 1–99: predict TAKEN → correct
  Iteration 100 (exit): predict TAKEN → WRONG (1 miss per 100)
  Miss rate: 1/100 = 1%  ← good prediction

Example — alternating branch (T, NT, T, NT, ...):
  Counter oscillates: 10 → 01 → 10 → 01 ...
  Every prediction wrong
  Miss rate: 100%  ← terrible for this pattern
```

The bimodal predictor's weakness: it uses only the branch's own history, not the context of what happened before. Two branches that alias to the same table entry interfere with each other.

### 6.2 Two-Level / GShare Predictor (1990s–2000s)

The key insight: branch behavior often depends on *which path was taken to reach it*. If the code is `if (x > 0 && y > 0)`, the second branch's outcome correlates with the first branch's outcome.

The two-level predictor maintains a **Global History Register (GHR)**: a shift register that records the outcomes of the last N branches (1 = taken, 0 = not-taken). The predictor table is indexed by `GHR XOR PC` (GShare), blending program-counter-based locality with global correlation.

```
GShare predictor:
  GHR: 16-bit shift register of recent branch outcomes
       e.g., after branches: T, NT, T, T, NT → GHR = ...1011 0
  
  Table index: GHR XOR PC[15:0]
  Entry: 2-bit saturating counter (same as bimodal)

  Advantage: different execution contexts (different GHR values)
  map to different table entries → reduces aliasing between
  unrelated branches that share a PC.

  Example — nested loop:
    Outer loop check: PC=0x1000, taken/not-taken alternates with inner loop
    GHR captures the inner loop pattern → different table entry for
    each outer-loop vs inner-loop context
    Prediction accuracy: ~95% vs ~60% for bimodal on nested patterns
```

### 6.3 TAGE Predictor (2006–present, current state-of-the-art)

TAGE (Tagged Geometric history length predictor) is the algorithm used in modern Intel and AMD CPUs. It maintains multiple tables with **geometrically increasing history lengths** and uses **tagged entries** to verify that the prediction is for the right branch-in-context.

```
TAGE structure:
  T0: bimodal table (base predictor, no history tag)
  T1: table indexed by (PC XOR GHR[8:0])   — 9-bit history
  T2: table indexed by (PC XOR GHR[16:0])  — 16-bit history
  T3: table indexed by (PC XOR GHR[32:0])  — 32-bit history
  T4: table indexed by (PC XOR GHR[64:0])  — 64-bit history

  Each T1–T4 entry: {tag, 3-bit counter, 2-bit usefulness}
  Tag: partial hash of PC and history — used to verify the entry
       is relevant to this branch (not an alias from another branch)

Prediction:
  1. Check T4 first (longest history). If tag matches: use it.
  2. If not: check T3. If tag matches: use it.
  3. Continue down to T0 (always matches, no tag).
  Use the prediction from the matching table with the longest history.

Why TAGE excels:
  Short patterns (loop counters, simple conditions):
    T1 or T2 detects the pattern quickly.
  Long correlated patterns (outer/inner loop interactions, state machines):
    T4 uses 64 bits of history → detects sequences invisible to shorter predictors.
  Tagged entries prevent aliasing:
    An unrelated branch with the same PC[low bits] hits the same index
    but a different tag → T4 declines → falls back to T3 → avoids pollution.

Practical accuracy:
  Simple loops: 99%+
  Complex data-dependent branches (sorted array): 95–99%
  Truly random branches (data = random, branch = data > threshold): 50–55%
```

### 6.4 What Defeats All Predictors

Regardless of predictor sophistication, any branch whose outcome is determined by **unpredictable data** is fundamentally hard to predict:

- Random numbers above/below a threshold.
- Hash values even/odd (effectively random).
- First access to a new data structure (cold, no history).
- Branches that execute only once (no warmup possible).

The predictor's accuracy ceiling for purely random data is 50% (no better than a coin flip). For mixed data (some pattern, some noise), accuracy falls between 50% and the pattern-only accuracy.

---

## 7. Mental Models

### Mental Model 1 — The Speculative Worker

Think of the CPU's pipeline as a construction crew with 20 workers in a chain. Worker 1 reads the blueprint, passes to worker 2 who interprets it, and so on. When a blueprint says "if condition A: build wall; else: build door", the crew can't stop and wait — that would idle 19 workers for 15 cycles. Instead, a foreman (the branch predictor) guesses: "I've seen this blueprint before — build the wall." All 20 workers start building the wall speculatively.

When the condition is checked (worker 15) and the foreman was right: great, the wall is already built. When the foreman was wrong: tear down everything workers 1–14 built (squash the speculative work), and start building the door from scratch. 15 workers' worth of wasted effort — the misprediction penalty.

The foreman improves over time by remembering patterns. A loop condition (keep going / exit) is easy to predict: almost always "keep going." A data-dependent condition on a random number is impossible: the foreman guesses and is wrong half the time.

### Mental Model 2 — The Branch Miss Rate Budget

Every hot loop has a branch miss budget. If you exceed it, the loop's throughput collapses regardless of how fast the computation itself is.

```
Branch miss rate budget for a tight loop:

  Loop body:       N instructions, IPC = 4 → N/4 cycles
  Branch penalty:  16 cycles per miss
  Miss rate:       p (fraction of branches that mispredict)

  Effective cycles per iteration:
    = (N/4) + p × 16

  For N = 8 instructions (2 cycles/iter):
    p = 0%:   2.0 cycles/iter  (ideal)
    p = 5%:   2.8 cycles/iter  (1.4× slower)
    p = 10%:  3.6 cycles/iter  (1.8× slower)
    p = 50%:  10 cycles/iter   (5× slower)  ← data-dependent branch

  Rule of thumb: if p > 5%, branch prediction is a significant bottleneck.
  perf stat target: branch-misses / branches < 1% for well-written hot loops.
```

### Mental Model 3 — Predictable vs Unpredictable Branch Taxonomy

Not all branches are equal. Classifying branches by predictability guides optimization effort:

```
ALWAYS PREDICTABLE:
  - Loop counter branches (for i in range(N)): taken N times, not-taken once
    → Predictor: Strongly Taken after first iteration. Miss: 1 per N.
  - Invariant conditions (if DEBUG_MODE: ...): always same outcome
    → Miss rate: ~0%
  - Switch on a small enum: pattern repeats → predictor learns
    → Miss rate: ~0–2% after warmup

USUALLY PREDICTABLE:
  - Sorted data threshold (if arr[i] > MEDIAN: ...):
    long runs of True, then long runs of False
    → TAGE with long history detects the transition
    → Miss rate: ~1–3%

  - State machine with few states:
    → TAGE remembers state sequences
    → Miss rate: ~1–5%

UNPREDICTABLE:
  - if arr[i] > random_threshold:
    each element independently random
    → Miss rate: ~50%
  - if hash(key) % 2 == 0:
    hash output: random
    → Miss rate: ~50%
  - if unsorted_arr[i] > MEDIAN:
    data randomly above/below threshold
    → Miss rate: ~50%
```

---

## 8. Failure Scenarios

### Failure 1 — Unsorted Data in a Filter Loop

**Symptom:** A Python loop that filters array elements based on a value threshold runs at the same speed whether it's in a tight C inner loop or a Python loop, despite the C loop being 10× faster in other benchmarks.

**Root cause:** The branch `if arr[i] > threshold` has a ~50% miss rate when `arr` is randomly ordered. Each misprediction flushes the pipeline. In C, the loop body is 2–3 instructions (2 cycles); each miss adds 16 cycles → effective 9 cycles/element. In NumPy's vectorized form (`arr > threshold`), the comparison is done without branching (SIMD comparison + mask) → no mispredictions.

```python
import numpy as np
import time

N = 10_000_000
arr = np.random.rand(N).astype(np.float32)
threshold = 0.5

# SLOW: Python loop with data-dependent branch
def filter_python(arr, t):
    result = []
    for x in arr:
        if x > t:         # data-dependent branch → ~50% miss rate
            result.append(x)
    return result

# FAST: NumPy boolean mask — SIMD comparison, no branches
def filter_numpy(arr, t):
    return arr[arr > t]   # vectorized comparison → no branch misses

# FAST: np.where — branchless select
def filter_where(arr, t):
    return arr[np.where(arr > t)]

t0 = time.perf_counter()
r1 = filter_python(arr, threshold)
t_py = time.perf_counter() - t0

t0 = time.perf_counter()
r2 = filter_numpy(arr, threshold)
t_np = time.perf_counter() - t0

print(f"Python loop (50% miss rate): {t_py*1000:.0f} ms")
print(f"NumPy boolean mask:          {t_np*1000:.0f} ms")
print(f"Speedup:                     {t_py/t_np:.0f}×")
```

---

### Failure 2 — Pandas apply With a Conditional

**Symptom:** A Pandas `apply` that categorizes rows based on a value condition is 30× slower than an equivalent vectorized operation.

**Root cause:** `apply` calls a Python function for every row. The function contains an `if` statement. For N=1M rows with random values: 1M Python function calls × branch overhead × Python interpreter overhead. The branch mispredictions compound with Python's interpreter overhead.

```python
import pandas as pd
import numpy as np
import time

N = 1_000_000
df = pd.DataFrame({'val': np.random.rand(N)})

# SLOW: apply with conditional
def categorize_slow(x):
    if x > 0.7:
        return 'high'
    elif x > 0.3:
        return 'mid'
    else:
        return 'low'

t0 = time.perf_counter()
df['cat_slow'] = df['val'].apply(categorize_slow)
t_slow = time.perf_counter() - t0

# FAST: np.select — vectorized, no per-row branches
conditions = [df['val'] > 0.7, df['val'] > 0.3]
choices    = ['high', 'mid']

t0 = time.perf_counter()
df['cat_fast'] = np.select(conditions, choices, default='low')
t_fast = time.perf_counter() - t0

print(f"apply (branchy Python): {t_slow*1000:.0f} ms")
print(f"np.select (branchless): {t_fast*1000:.0f} ms")
print(f"Speedup: {t_slow/t_fast:.0f}×")
```

---

### Failure 3 — Sorting Data Before Filtering Hurts (Sometimes)

**Symptom:** A developer sorts a large array before filtering, expecting to improve cache locality. Branch prediction improves, but overall performance decreases.

**Root cause:** Sorting is O(N log N). For a filter that processes N elements, the sort dominates total work. Sorting helps branch prediction (transitions from False to True happen once, not randomly), but the SIMD branchless vectorized approach (`arr[arr > threshold]`) never mispredicts regardless of sort order — so the branch prediction improvement from sorting is irrelevant once the code uses vectorized comparisons.

The lesson: **branch prediction matters for scalar branchy code; branchless vectorized code makes the question moot.** Always choose branchless SIMD over sort-then-branch.

---

### Failure 4 — Indirect Function Call Dispatch

**Symptom:** A Spark UDF pipeline that calls a Python function via `rdd.map(fn)` runs at 10% the throughput of an equivalent Spark SQL expression. The UDF has no I/O, minimal logic.

**Root cause:** Python function dispatch is an indirect branch: `fn` is a Python callable, and calling it requires loading the function pointer from the object header and jumping to it. The branch target changes each time for different closures or lambda variations. Indirect branches stress the **Indirect Branch Target Buffer (IBTB)**, which is smaller than the BTB. For polymorphic dispatch (different function objects per row), the IBTB misses frequently — each miss is a 15–20 cycle pipeline flush in addition to the Python interpreter overhead.

Spark SQL expressions, by contrast, compile to JVM bytecode that JIT-compiles to direct x86 machine code — no indirect dispatch, no interpreter, no branch misses from polymorphism.

---

## 9. Recovery Procedures

### Branchless Coding Patterns

The most powerful fix for unpredictable branches is **branch elimination**: rewrite the conditional computation without a branch instruction, so the CPU executes both paths (or uses a selection instruction) regardless of the condition.

```python
"""
branchless_patterns.py

Demonstrates branchless equivalents of common conditional operations.
Branchless code compiles to CMOV (conditional move) or SIMD select
instructions — no pipeline flush on any data value.
"""
import numpy as np
import time


# ─────────────────────────────────────────────────────────────
# Pattern 1: Conditional assignment
# ─────────────────────────────────────────────────────────────

def branchy_clamp(arr: np.ndarray, lo: float, hi: float) -> np.ndarray:
    """Scalar clamp with branches — mispredicts on random data."""
    result = np.empty_like(arr)
    for i, x in enumerate(arr):
        if x < lo:         # branch 1: ~50% miss if random
            result[i] = lo
        elif x > hi:       # branch 2: ~50% miss if random
            result[i] = hi
        else:
            result[i] = x
    return result


def branchless_clamp(arr: np.ndarray, lo: float, hi: float) -> np.ndarray:
    """
    SIMD clamp using np.clip — compiles to vminpd/vmaxpd instructions.
    No branch: the min/max hardware units select the value unconditionally.
    """
    return np.clip(arr, lo, hi)  # compiles to: vmaxpd + vminpd (AVX2)


# ─────────────────────────────────────────────────────────────
# Pattern 2: Conditional select (ternary)
# ─────────────────────────────────────────────────────────────

def branchy_abs(arr: np.ndarray) -> np.ndarray:
    """Absolute value with branch."""
    result = np.empty_like(arr)
    for i, x in enumerate(arr):
        result[i] = x if x >= 0 else -x  # branch on sign
    return result


def branchless_abs(arr: np.ndarray) -> np.ndarray:
    """
    np.abs uses SIMD andpd (mask the sign bit) — no branch.
    For float64: abs = value & ~(1 << 63)  (clear sign bit)
    """
    return np.abs(arr)  # vandpd ymm: clears sign bit, no condition


def branchless_select(condition: np.ndarray,
                      true_vals: np.ndarray,
                      false_vals: np.ndarray) -> np.ndarray:
    """
    General branchless select via np.where.
    Compiles to: vpblendvb or vblendvpd (blend based on mask).
    Both true_vals AND false_vals are computed; the mask selects.
    """
    return np.where(condition, true_vals, false_vals)


# ─────────────────────────────────────────────────────────────
# Pattern 3: Counting elements matching a condition
# ─────────────────────────────────────────────────────────────

def branchy_count(arr: np.ndarray, threshold: float) -> int:
    """Count elements above threshold — branch on each element."""
    count = 0
    for x in arr:
        if x > threshold:  # data-dependent branch → ~50% miss
            count += 1
    return count


def branchless_count(arr: np.ndarray, threshold: float) -> int:
    """
    SIMD comparison produces boolean array (0 or 1 as int8).
    np.sum adds them — no branch per element.
    Compiles to: vcmpgtpd + vpaddd + vphaddw reduction
    """
    return int(np.sum(arr > threshold))  # comparison → 0/1 mask → sum


# ─────────────────────────────────────────────────────────────
# Pattern 4: Conditional accumulation
# ─────────────────────────────────────────────────────────────

def branchy_conditional_sum(arr: np.ndarray, threshold: float) -> float:
    """Sum elements above threshold using a loop with branch."""
    total = 0.0
    for x in arr:
        if x > threshold:
            total += x     # branch + conditional add
    return total


def branchless_conditional_sum(arr: np.ndarray, threshold: float) -> float:
    """
    Multiply by boolean mask (0.0 or 1.0) — computes x*True or x*False.
    Single SIMD fused multiply: no branch, both paths computed.
    """
    return float(np.dot(arr, arr > threshold))  # SIMD dot product with mask


def branchless_conditional_sum_v2(arr: np.ndarray, threshold: float) -> float:
    """
    Boolean index then sum — gathers matching elements, no per-element branch.
    Better when selectivity is low (few elements match) → less data to sum.
    """
    mask = arr > threshold
    return float(arr[mask].sum())


# ─────────────────────────────────────────────────────────────
# Benchmark
# ─────────────────────────────────────────────────────────────

def benchmark(n: int = 5_000_000):
    rng = np.random.default_rng(42)
    arr = rng.random(n).astype(np.float64)
    threshold = 0.5

    patterns = [
        ("branchy_clamp",              lambda: branchy_clamp(arr, 0.2, 0.8)),
        ("branchless_clamp (np.clip)", lambda: branchless_clamp(arr, 0.2, 0.8)),
        ("branchy_count",              lambda: branchy_count(arr, threshold)),
        ("branchless_count",           lambda: branchless_count(arr, threshold)),
        ("branchy_conditional_sum",    lambda: branchy_conditional_sum(arr, threshold)),
        ("branchless dot-product sum", lambda: branchless_conditional_sum(arr, threshold)),
    ]

    print(f"\n{'Pattern':<40}  {'Time (ms)':>10}")
    print("-" * 55)
    for name, fn in patterns:
        times = [None] * 5
        for i in range(5):
            t0 = time.perf_counter()
            fn()
            times[i] = time.perf_counter() - t0
        print(f"  {name:<38}  {min(times)*1000:>10.2f}")

    print(f"\nNote: 'branchy_clamp' is a Python loop — Python overhead dominates.")
    print(f"The key insight is the branchless PATTERN, not just the speed ratio.")
    print(f"In Cython/C with nogil: branchless versions are 5–20× faster.")


if __name__ == '__main__':
    benchmark()
```

### Sorting Data to Improve Branch Prediction (Scalar Code Only)

When branchless SIMD is not available (e.g., in JVM user-defined functions that the JIT doesn't vectorize), sorting the input data before processing can improve prediction:

```python
import numpy as np
import time

def scalar_threshold_filter_sum(arr: np.ndarray, threshold: float) -> float:
    """Scalar loop — branch prediction quality matters here."""
    total = 0.0
    for x in arr:
        if x > threshold:
            total += x
    return total


def benchmark_sort_vs_nosort(n: int = 1_000_000):
    rng = np.random.default_rng(42)
    arr_random = rng.random(n).astype(np.float64)
    arr_sorted = np.sort(arr_random)

    threshold = 0.5
    RUNS = 5

    # Note: we time only the loop, not the sort itself
    times_random = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        scalar_threshold_filter_sum(arr_random, threshold)
        times_random.append(time.perf_counter() - t0)

    times_sorted = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        scalar_threshold_filter_sum(arr_sorted, threshold)
        times_sorted.append(time.perf_counter() - t0)

    print(f"Scalar loop on {n:,} elements:")
    print(f"  Random data (50% misprediction):  {min(times_random)*1000:.1f} ms")
    print(f"  Sorted data (~1% misprediction):  {min(times_sorted)*1000:.1f} ms")
    r = min(times_random) / min(times_sorted)
    print(f"  Speedup from sorting: {r:.1f}×")
    print()
    print(f"BUT: sorting costs O(N log N). For N=1M at ~100M sort ops/sec:")
    sort_cost_ms = n * np.log2(n) / 1e8 * 1000
    print(f"  Sort cost ≈ {sort_cost_ms:.0f} ms")
    print(f"  Prediction gain: {(min(times_random)-min(times_sorted))*1000:.1f} ms")
    print(f"  Net: sort-then-scan is {'WORSE' if sort_cost_ms > (min(times_random)-min(times_sorted))*1000 else 'BETTER'}")
    print()
    print(f"CONCLUSION: Use branchless SIMD (np.sum(arr[arr > threshold]))")
    print(f"instead of sorting. SIMD is always faster regardless of order.")
```

---

## 10. Trade-offs

### Branchless Code: Performance vs Readability vs Correctness

Branchless code computes both the true and false branches and selects the result. This has implications:

**Side effects:** If the true/false branches have side effects (e.g., allocating memory, incrementing a counter), branchless code cannot always be used. `np.where` evaluates both arrays before selecting — if one path would cause a division-by-zero, the exception occurs even when that path is not "selected".

```python
# WRONG: both paths evaluated — ZeroDivisionError when denom == 0
result = np.where(denom != 0, numerator / denom, 0.0)

# CORRECT: avoid the division before applying the mask
safe_denom = np.where(denom != 0, denom, 1.0)  # replace 0 with 1 (never used)
result = np.where(denom != 0, numerator / safe_denom, 0.0)
```

**Wasted computation:** Branchless code always computes both paths. For expensive computations (e.g., `np.sqrt`, `np.log`), computing the unneeded path wastes cycles. A branchy approach that correctly skips the expensive path may be faster when branch prediction accuracy is high (>95%) and the expensive operation is much costlier than the misprediction penalty.

**Decision rule:**
- Unpredictable branch (miss rate > 5%) + cheap operation → branchless wins.
- Predictable branch (miss rate < 1%) + expensive operation → branchy wins (correctly predicted branch skips expensive work; branchless always pays full cost).

---

## 11. Comparisons

### Branch-Dependent vs Branch-Free Aggregation Performance

```python
"""
aggregation_comparison.py

Compares four approaches to conditional aggregation:
1. Python loop with branch
2. Sorted Python loop with branch (better prediction)
3. NumPy boolean mask (branchless gather)
4. NumPy dot product (fully branchless, no gather)
"""
import numpy as np
import time


def benchmark_conditional_aggregation():
    N = 5_000_000
    rng = np.random.default_rng(0)
    arr = rng.random(N).astype(np.float64)
    threshold = 0.5
    arr_sorted = np.sort(arr)

    RUNS = 5

    methods = [
        ("Python loop (random)",
         lambda: sum(x for x in arr if x > threshold)),

        ("Python loop (sorted, better pred.)",
         lambda: sum(x for x in arr_sorted if x > threshold)),

        ("NumPy boolean mask",
         lambda: arr[arr > threshold].sum()),

        ("NumPy dot product (fully branchless)",
         lambda: float(arr @ (arr > threshold))),

        ("NumPy sum with mask multiply",
         lambda: float(np.sum(arr * (arr > threshold)))),
    ]

    print(f"\nConditional sum benchmark (N={N:,}, threshold={threshold})")
    print(f"{'Method':<45}  {'Time (ms)':>10}  {'vs Python':>10}")
    print("-" * 70)

    baseline = None
    for name, fn in methods:
        times = []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            fn()
            times.append(time.perf_counter() - t0)
        best = min(times) * 1000
        if baseline is None:
            baseline = best
        print(f"  {name:<43}  {best:>10.1f}  {baseline/best:>9.1f}×")


if __name__ == '__main__':
    benchmark_conditional_aggregation()
```

---

## 12. Production Examples

### Spark Predicate Pushdown: Branch Elimination at the Data Level

Spark's query optimizer pushes filter predicates down to the data scan layer (Parquet reader, ORC reader). This is not just an I/O optimization — it is a branch elimination technique at the CPU level.

```
Without predicate pushdown:
  1. Read entire Parquet column → deserialize all N values into JVM objects
  2. For each row in the scan:
       if row.date_col > '2024-01-01':   ← branch per row (data-dependent)
           output_buffer.add(row)
  
  Branch miss rate: ~50% for uniform date distribution
  CPU cost: N × (deserialize cost + branch cost + possible miss penalty)

With predicate pushdown (Parquet column statistics):
  1. Check row group metadata: min_date ≤ '2024-01-01' ≤ max_date?
       If max_date < '2024-01-01': skip entire 128K-row row group (no deser.)
       If min_date > '2024-01-01': include entire row group (no per-row branch)
  2. Within passing row groups: use Parquet page-level statistics
  3. Only rows without metadata certainty need per-row evaluation

Result:
  - Row groups fully outside predicate: zero CPU branch cost (skipped entirely)
  - Row groups fully inside predicate: zero CPU branch cost (all rows included)
  - Row groups crossing the boundary: normal per-row evaluation
  - For temporal data with ordered columns: >90% of row groups fall into
    skip or include category → <10% of rows need per-row branching

This is why sorted/clustered Parquet files with predicate pushdown
outperform unsorted files: the sorted order makes predicate pushdown
eliminate almost all per-row branches, not just reduce I/O.
```

### DuckDB Vectorized Filter: Branchless Selection Vector

DuckDB's vectorized execution engine processes data in vectors of 1,024–2,048 values. Its filter operator does not branch on each element:

```
DuckDB filter operation (conceptual x86 AVX2):
  Input: float64 column[2048]
  Predicate: val > 0.5

  For each group of 4 float64 values:
    vmovupd ymm0, [column_ptr + offset]       ; load 4 float64
    vcmpgtpd ymm1, ymm0, threshold_broadcast   ; compare all 4 → 4-bit mask
    ; ymm1 = 0xFF...FF where val > threshold, 0x00...00 where not
    
    ; No branch! SIMD comparison produces a mask, not a branch.
    ; The mask is used to scatter-select values into output buffer.

  The entire 2048-element batch: ~512 SIMD operations (4 values each)
  No branch per element. Branch miss rate: 0%.
  Throughput: limited by SIMD throughput, not branch prediction.

Compare to Spark Row-at-a-time UDF:
  For each of 2048 rows:
    call UDF → Python dispatch → function body → if branch → return
  2048 function calls, 2048 potentially-missing branches.
```

### NumPy Boolean Indexing Internals

When you write `arr[arr > 0.5]`, NumPy executes two SIMD passes:

1. **Comparison pass:** Vectorized `arr > 0.5` → produces a boolean (uint8) array. Compiled to `vcmpgtpd` (AVX2). Zero branches.

2. **Gather pass:** Gather all elements where the mask is True into a contiguous output array. This involves a compact/compress operation. On CPUs with AVX-512: `vpcompressd` / `vpcompressq` — hardware gather/compress, no branch. On AVX2-only CPUs: a sequential scan with a scalar `if mask[i]: output[out_idx++] = arr[i]` loop — this loop *does* have a branch, but the comparison itself remains branchless.

The key insight: even when the gather pass has a branch (on non-AVX-512 hardware), it is a predictable branch (`mask[i]` from the vectorized comparison, accessed sequentially) — not a data-dependent branch on raw values.

---

### Kafka Consumer Poll Loop: Predictable Branch

Kafka's consumer poll loop has this structure:

```java
while (!shouldStop()) {                    // branch: almost always TAKEN
    ConsumerRecords<K,V> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<K,V> record : records) {   // loop: N iterations
        process(record);
    }
}
```

The `while (!shouldStop())` branch: taken thousands of times before stop is requested. The predictor learns "always taken" after a few iterations → near-zero miss rate. The inner `for` loop: N records per poll, N usually in range 100–500. Loop-exit branch is taken once per `poll()` → 1 misprediction per poll → 16 cycles × polls_per_second ≈ negligible.

Kafka's main performance concern is not branch prediction but rather: throughput-limited deserialization (CPU), network bandwidth (NIC), and disk I/O (for log reads). Branch prediction in the consumer loop is a non-issue because the loop structure is highly predictable.

---

## 13. Code

### `branch_profiler.py` — Measure Branch Miss Rate

```python
"""
branch_profiler.py

Measures branch miss rate for different access patterns using
either perf stat (Linux, hardware counters) or timing-based
estimation (fallback).

Key metrics:
  branches: total branch instructions executed
  branch-misses: branches predicted incorrectly
  miss_rate = branch-misses / branches × 100%

  < 1%:  excellent (loop-dominated, predictable code)
  1–5%:  acceptable
  5–10%: investigate — possible data-dependent branches
  > 10%: significant overhead — consider branchless alternatives

Run: python3 branch_profiler.py
     sudo perf stat -e branches,branch-misses python3 branch_profiler.py
"""
import numpy as np
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Optional, Callable


@dataclass
class BranchProfile:
    pattern_name: str
    n_elements: int
    elapsed_s: float
    branches: Optional[int]          # from perf, if available
    branch_misses: Optional[int]     # from perf, if available

    @property
    def miss_rate_pct(self) -> Optional[float]:
        if self.branches and self.branch_misses:
            return self.branch_misses / self.branches * 100
        return None

    @property
    def ns_per_element(self) -> float:
        return self.elapsed_s * 1e9 / self.n_elements

    @property
    def diagnosis(self) -> str:
        r = self.miss_rate_pct
        if r is None:
            return "(perf not available)"
        if r < 1:
            return "✅ excellent (<1% miss rate)"
        elif r < 5:
            return "🟡 acceptable (1–5%)"
        elif r < 10:
            return "⚠️  investigate (5–10%)"
        else:
            return "🔴 high miss rate (>10%) — use branchless"


def time_function(fn: Callable, runs: int = 5) -> float:
    """Return minimum wall-clock time in seconds."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times)


def run_perf(fn_source: str, n: int) -> Optional[tuple]:
    """
    Attempt to run a Python snippet under perf stat.
    Returns (branches, branch_misses) or None if perf unavailable.
    """
    if sys.platform != 'linux':
        return None
    try:
        result = subprocess.run(
            ['perf', 'stat', '-e', 'branches,branch-misses',
             sys.executable, '-c', fn_source],
            capture_output=True, text=True, timeout=30
        )
        branches = branch_misses = None
        for line in (result.stdout + result.stderr).splitlines():
            line = line.strip().replace(',', '')
            if 'branches' in line and 'misses' not in line:
                try:
                    branches = int(line.split()[0])
                except (ValueError, IndexError):
                    pass
            if 'branch-misses' in line:
                try:
                    branch_misses = int(line.split()[0])
                except (ValueError, IndexError):
                    pass
        return branches, branch_misses
    except Exception:
        return None


def profile_patterns(n: int = 5_000_000):
    print(f"\nBranch Profiler — {n:,} elements")
    print("=" * 70)

    rng = np.random.default_rng(42)
    arr_random = rng.random(n).astype(np.float64)
    arr_sorted = np.sort(arr_random)
    threshold = 0.5

    patterns = [
        ("Sequential (loop, always True)",
         lambda: sum(1 for x in np.ones(n) if x > 0),
         "sum(1 for x in np.ones({n}) if x > 0)"),
        ("Random (50% miss rate)",
         lambda: sum(1 for x in arr_random if x > threshold),
         None),
        ("Sorted (1 transition → 1 miss)",
         lambda: sum(1 for x in arr_sorted if x > threshold),
         None),
        ("Branchless (np.sum)",
         lambda: int(np.sum(arr_random > threshold)),
         None),
    ]

    print(f"\n{'Pattern':<42}  {'ns/elem':>8}  {'Miss rate':>12}  {'Diagnosis'}")
    print("-" * 80)

    for name, fn, _ in patterns:
        elapsed = time_function(fn)
        p = BranchProfile(
            pattern_name=name,
            n_elements=n,
            elapsed_s=elapsed,
            branches=None,
            branch_misses=None,
        )
        print(f"  {name:<40}  {p.ns_per_element:>8.1f}  "
              f"{'(no perf)':>12}  {p.diagnosis}")

    print()
    print("To see actual branch miss rates, run:")
    print("  sudo perf stat -e branches,branch-misses python3 branch_profiler.py")
    print()
    print("Expected miss rates (with perf):")
    print("  Sequential always-True loop:  ~0% (predictor: Strongly Taken)")
    print("  Random 50/50 loop:            ~50% (predictor: coin-flip performance)")
    print("  Sorted loop:                  ~1% (1 miss at the transition point)")
    print("  Branchless np.sum:            ~0% (no conditional branch per element)")


if __name__ == '__main__':
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5_000_000
    profile_patterns(n)
```

### `predicate_optimizer.py` — Rewrite Branchy Predicates

```python
"""
predicate_optimizer.py

Provides a toolkit for rewriting branchy conditional expressions
as branchless NumPy/Pandas vectorized operations.

Covers the most common data engineering conditional patterns:
  - Threshold filter (>, <, ==)
  - Multi-condition categorization
  - Conditional accumulation
  - Null/NaN handling
  - String prefix matching

Run: python3 predicate_optimizer.py
"""
import numpy as np
import pandas as pd
import time
from typing import Any


def time_ms(fn, runs=5) -> float:
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times) * 1000


# ─────────────────────────────────────────────────────
# 1. Threshold filter
# ─────────────────────────────────────────────────────

def demo_threshold(n=2_000_000):
    print("1. Threshold Filter")
    arr = np.random.rand(n).astype(np.float64)

    branchy   = lambda: [x for x in arr if x > 0.7]
    branchless= lambda: arr[arr > 0.7]

    t_b = time_ms(branchy)
    t_n = time_ms(branchless)
    print(f"   Python list comp:  {t_b:.1f} ms")
    print(f"   NumPy boolean idx: {t_n:.1f} ms  ({t_b/t_n:.0f}× faster)")


# ─────────────────────────────────────────────────────
# 2. Multi-condition categorization
# ─────────────────────────────────────────────────────

def demo_categorization(n=1_000_000):
    print("\n2. Multi-Condition Categorization")
    df = pd.DataFrame({'revenue': np.random.rand(n) * 1_000_000,
                       'cost':    np.random.rand(n) * 500_000})

    def apply_fn(row):
        margin = (row['revenue'] - row['cost']) / row['revenue']
        if margin > 0.4:
            return 'high_margin'
        elif margin > 0.2:
            return 'mid_margin'
        else:
            return 'low_margin'

    branchy    = lambda: df.apply(apply_fn, axis=1)

    # Branchless: compute margin once, use np.select
    def branchless_fn():
        margin = (df['revenue'] - df['cost']) / df['revenue']
        return np.select(
            [margin > 0.4, margin > 0.2],
            ['high_margin', 'mid_margin'],
            default='low_margin'
        )

    t_b = time_ms(branchy, runs=2)
    t_n = time_ms(branchless_fn)
    print(f"   df.apply (branchy):   {t_b:.1f} ms")
    print(f"   np.select (branchless): {t_n:.1f} ms  ({t_b/t_n:.0f}× faster)")


# ─────────────────────────────────────────────────────
# 3. Conditional accumulation
# ─────────────────────────────────────────────────────

def demo_conditional_sum(n=5_000_000):
    print("\n3. Conditional Accumulation")
    arr = np.random.rand(n).astype(np.float64)
    threshold = 0.5

    branchy    = lambda: sum(x for x in arr if x > threshold)
    branchless = lambda: float(arr[arr > threshold].sum())
    dot_method = lambda: float(arr @ (arr > threshold).astype(np.float64))

    t_b   = time_ms(branchy, runs=2)
    t_n   = time_ms(branchless)
    t_dot = time_ms(dot_method)
    print(f"   Python genexp (branchy):    {t_b:.1f} ms")
    print(f"   Boolean mask + sum:         {t_n:.1f} ms  ({t_b/t_n:.0f}× faster)")
    print(f"   Dot product (all branchless):{t_dot:.1f} ms  ({t_b/t_dot:.0f}× faster)")


# ─────────────────────────────────────────────────────
# 4. NaN/null handling
# ─────────────────────────────────────────────────────

def demo_nan_handling(n=2_000_000):
    print("\n4. NaN Handling")
    arr = np.random.rand(n).astype(np.float64)
    # Inject 10% NaN
    nan_idx = np.random.choice(n, n // 10, replace=False)
    arr[nan_idx] = np.nan

    def branchy_nan():
        result = []
        for x in arr:
            if not np.isnan(x) and x > 0.5:
                result.append(x)
        return result

    def branchless_nan():
        # np.nan_to_num converts NaN to 0 — then no NaN-check branch
        clean = np.nan_to_num(arr, nan=0.0)
        return clean[clean > 0.5]  # NaN→0, so condition False for NaN slots

    def branchless_nan_v2():
        # Boolean AND of two conditions: not-nan AND > threshold
        mask = ~np.isnan(arr) & (arr > 0.5)
        return arr[mask]  # two boolean arrays ANDed — no branch per element

    t_b  = time_ms(branchy_nan, runs=2)
    t_n1 = time_ms(branchless_nan)
    t_n2 = time_ms(branchless_nan_v2)
    print(f"   Python loop + isnan (branchy):  {t_b:.1f} ms")
    print(f"   nan_to_num + threshold:         {t_n1:.1f} ms")
    print(f"   Boolean AND mask (canonical):   {t_n2:.1f} ms  ({t_b/t_n2:.0f}× faster)")


if __name__ == '__main__':
    print("Branchless Predicate Optimization Demos")
    print("=" * 55)
    demo_threshold()
    demo_categorization()
    demo_conditional_sum()
    demo_nan_handling()
    print("\nKey takeaway: np.where, np.select, boolean indexing,")
    print("and np.nan_to_num are branchless SIMD operations.")
    print("Replace Python apply/comprehension with these to")
    print("eliminate data-dependent branch mispredictions.")
```

---

## 14. Labs

### Lab 1 — Observe the Branch Misprediction Cliff

**Goal:** Empirically find the point at which branch mispredictions become the dominant cost. Vary the fraction of True values in a boolean condition and measure how throughput degrades as the fraction approaches 50%.

```python
"""
lab1_misprediction_cliff.py

Varies the fraction of True values in a threshold filter and
measures throughput. Expect a V-shape or U-shape:
  - Near 0% or 100% True: predictor learns quickly → near-peak throughput
  - Near 50% True: predictor wrong ~50% → worst-case throughput

Run: python3 lab1_misprediction_cliff.py
Then optionally: perf stat -e branch-misses python3 lab1_misprediction_cliff.py
"""
import numpy as np
import time


def scalar_filter_count(arr: np.ndarray, threshold: float) -> int:
    """Python scalar loop — branch prediction fully exposed."""
    count = 0
    for x in arr:
        if x > threshold:
            count += 1
    return count


def simd_filter_count(arr: np.ndarray, threshold: float) -> int:
    """Branchless SIMD — prediction irrelevant."""
    return int(np.sum(arr > threshold))


def lab1():
    print("LAB 1: Branch Misprediction Cliff")
    print("=" * 65)

    N = 2_000_000
    arr = np.random.rand(N).astype(np.float64)

    # Sweep threshold from 0.0 (100% True) to 1.0 (0% True)
    # At threshold=0.5: 50% True → worst-case misprediction
    thresholds = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5,
                  0.6, 0.7, 0.8, 0.9, 1.0]

    print(f"\n{'Threshold':>10}  {'% True':>8}  "
          f"{'Scalar (ms)':>13}  {'SIMD (ms)':>11}  {'Ratio':>7}")
    print("-" * 60)

    for t in thresholds:
        pct_true = float(np.sum(arr > t)) / N * 100

        times_scalar = []
        for _ in range(3):
            t0 = time.perf_counter()
            scalar_filter_count(arr, t)
            times_scalar.append(time.perf_counter() - t0)

        times_simd = []
        for _ in range(5):
            t0 = time.perf_counter()
            simd_filter_count(arr, t)
            times_simd.append(time.perf_counter() - t0)

        ms_scalar = min(times_scalar) * 1000
        ms_simd   = min(times_simd) * 1000
        ratio = ms_scalar / ms_simd

        # Mark worst prediction point
        marker = " ← worst prediction" if abs(pct_true - 50) < 5 else ""
        print(f"{t:>10.1f}  {pct_true:>7.1f}%  "
              f"{ms_scalar:>13.1f}  {ms_simd:>11.2f}  {ratio:>7.1f}×{marker}")

    print()
    print("Observations:")
    print("  Scalar: slowest at ~50% True (random prediction, ~50% miss rate)")
    print("  Scalar: fastest at 0% or 100% True (always False/True → predictor correct)")
    print("  SIMD:   nearly flat across all thresholds (no branches)")
    print("  The U-shape in scalar times = the misprediction cliff")


if __name__ == '__main__':
    lab1()
```

---

### Lab 2 — Rewrite a Branchy Pandas Pipeline as Branchless

**Goal:** Take a realistic Pandas pipeline with multiple conditional transformations and rewrite it using vectorized operations. Measure the speedup.

```python
"""
lab2_pandas_branchless.py

Rewrites a multi-step branchy Pandas pipeline:
  - Revenue categorization (3 tiers)
  - Discount application (conditional on category)
  - Flag high-value customers (conditional on multiple fields)

Original: df.apply() with Python if-elif-else
Optimized: np.select, np.where, boolean arithmetic

Run: python3 lab2_pandas_branchless.py
"""
import pandas as pd
import numpy as np
import time

def generate_orders(n: int) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        'order_id':   np.arange(n),
        'revenue':    rng.uniform(10, 10_000, n).astype(np.float64),
        'quantity':   rng.integers(1, 100, n).astype(np.int32),
        'is_member':  rng.random(n) > 0.7,
        'country':    rng.choice(['US', 'CA', 'GB', 'AU', 'DE'], n),
    })


def pipeline_branchy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Original branchy pipeline using apply.
    Each row: 3 branch-heavy Python function calls.
    """
    def categorize(row):
        if row['revenue'] > 1000:
            return 'premium'
        elif row['revenue'] > 200:
            return 'standard'
        else:
            return 'budget'

    def apply_discount(row):
        cat = row['tier']
        base = row['revenue']
        if cat == 'premium':
            return base * 0.85 if row['is_member'] else base * 0.90
        elif cat == 'standard':
            return base * 0.95 if row['is_member'] else base
        else:
            return base

    def flag_high_value(row):
        return (row['discounted_revenue'] > 800
                and row['is_member']
                and row['country'] in ('US', 'CA'))

    result = df.copy()
    result['tier'] = result.apply(categorize, axis=1)
    result['discounted_revenue'] = result.apply(apply_discount, axis=1)
    result['high_value'] = result.apply(flag_high_value, axis=1)
    return result


def pipeline_branchless(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized pipeline using np.select, np.where, boolean arithmetic.
    Zero per-row branches.
    """
    result = df.copy()

    # Step 1: Tier categorization — np.select (compiled SIMD select)
    result['tier'] = np.select(
        [df['revenue'] > 1000, df['revenue'] > 200],
        ['premium',             'standard'],
        default='budget'
    )

    # Step 2: Discount — boolean arithmetic, no branch
    # premium member: ×0.85, premium non-member: ×0.90
    # standard member: ×0.95, standard non-member: ×1.00
    # budget: ×1.00
    is_premium  = (result['tier'] == 'premium').to_numpy()
    is_standard = (result['tier'] == 'standard').to_numpy()
    is_member   = df['is_member'].to_numpy()
    revenue     = df['revenue'].to_numpy()

    # Multiplier computed via boolean arithmetic — all elements computed
    multiplier = (
        is_premium  * is_member   * 0.85
      + is_premium  * ~is_member  * 0.90
      + is_standard * is_member   * 0.95
      + is_standard * ~is_member  * 1.00
      + (~is_premium & ~is_standard) * 1.00
    )
    # Alternatively, using np.select (cleaner):
    multiplier = np.select(
        [is_premium & is_member,  is_premium & ~is_member,
         is_standard & is_member],
        [0.85,                    0.90,
         0.95],
        default=1.00
    )
    result['discounted_revenue'] = revenue * multiplier

    # Step 3: High-value flag — boolean AND of three conditions (no branch)
    is_us_ca = df['country'].isin(['US', 'CA']).to_numpy()
    result['high_value'] = (
        (result['discounted_revenue'].to_numpy() > 800)
        & is_member
        & is_us_ca
    )

    return result


def lab2():
    print("LAB 2: Branchy vs Branchless Pandas Pipeline")
    print("=" * 60)

    N = 500_000
    df = generate_orders(N)

    print(f"\nDataFrame: {N:,} orders")

    t0 = time.perf_counter()
    r1 = pipeline_branchy(df)
    t_branchy = time.perf_counter() - t0

    t0 = time.perf_counter()
    r2 = pipeline_branchless(df)
    t_branchless = time.perf_counter() - t0

    # Verify
    assert (r1['tier'] == r2['tier']).all(), "Tier mismatch"
    rev_match = np.allclose(r1['discounted_revenue'], r2['discounted_revenue'], rtol=1e-9)
    assert rev_match, "Discounted revenue mismatch"
    assert (r1['high_value'] == r2['high_value']).all(), "High value flag mismatch"

    print(f"\n  Branchy (df.apply × 3):    {t_branchy*1000:.0f} ms")
    print(f"  Branchless (np.select):    {t_branchless*1000:.0f} ms")
    print(f"  Speedup:                   {t_branchy/t_branchless:.0f}×")
    print(f"\n  Both produce identical results. ✅")
    print(f"\n  Distribution of tiers:")
    for tier, count in r2['tier'].value_counts().items():
        print(f"    {tier}: {count:,} ({count/N*100:.1f}%)")
    print(f"  High-value customers: {r2['high_value'].sum():,} "
          f"({r2['high_value'].mean()*100:.1f}%)")


if __name__ == '__main__':
    lab2()
```

---

### Lab 3 — Profile a Spark Filter Query with Predicate Pushdown

**Goal:** Demonstrate the effect of predicate pushdown vs no pushdown in a Spark query on Parquet data, and measure the difference in task execution time.

```python
"""
lab3_spark_predicate_pushdown.py

Demonstrates Spark predicate pushdown by:
1. Writing a Parquet file with sorted and unsorted data
2. Running a filter query with and without pushdown
3. Using explain() to verify pushdown is happening
4. Timing the difference

Requires: PySpark, a local Spark installation
Run: python3 lab3_spark_predicate_pushdown.py
"""
import time
import os
import tempfile
import numpy as np

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False


def lab3_with_spark():
    spark = (SparkSession.builder
             .appName("BranchPredictorLab3")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.driver.memory", "2g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    N = 2_000_000
    rng = np.random.default_rng(42)

    with tempfile.TemporaryDirectory() as tmpdir:
        # Write two Parquet files: sorted and unsorted on 'date_val'
        unsorted_path = os.path.join(tmpdir, 'unsorted.parquet')
        sorted_path   = os.path.join(tmpdir, 'sorted.parquet')

        dates = rng.integers(20230101, 20241231, N).astype(np.int64)
        values = rng.random(N).astype(np.float64)

        import pandas as pd
        df_pd = pd.DataFrame({'date_val': dates, 'value': values})

        # Unsorted: random order
        spark.createDataFrame(df_pd).write.parquet(unsorted_path, mode='overwrite')

        # Sorted: by date_val (enables aggressive predicate pushdown)
        df_pd_sorted = df_pd.sort_values('date_val')
        spark.createDataFrame(df_pd_sorted).write.parquet(sorted_path, mode='overwrite')

        predicate = F.col('date_val') > 20240101

        print("LAB 3: Spark Predicate Pushdown Timing")
        print("=" * 55)

        for label, path in [('Unsorted Parquet', unsorted_path),
                             ('Sorted Parquet', sorted_path)]:
            df = spark.read.parquet(path)

            # Show query plan
            print(f"\n--- {label} ---")
            print("Query plan (check for 'PushedFilters' in the plan):")
            df.filter(predicate).selectExpr("sum(value)").explain(mode="extended")

            # Time the query (multiple runs to warm up)
            for _ in range(2):  # warmup
                df.filter(predicate).selectExpr("sum(value)").collect()

            times = []
            for _ in range(3):
                t0 = time.perf_counter()
                result = df.filter(predicate).selectExpr("sum(value)").collect()
                times.append(time.perf_counter() - t0)
            print(f"  Filter time: {min(times)*1000:.0f} ms  "
                  f"(result: {result[0][0]:.2f})")

        spark.stop()

    print("\nKey: look for 'PushedFilters' in the EXPLAIN output.")
    print("Sorted Parquet: more row groups are entirely excluded →")
    print("fewer row-level branch evaluations → faster scan.")


def lab3_without_spark():
    """
    Simulate predicate pushdown using NumPy and Parquet statistics.
    Demonstrates the concept even without Spark installed.
    """
    print("LAB 3: Predicate Pushdown Concept (NumPy simulation)")
    print("=" * 60)
    print("(Spark not installed — simulating with NumPy)")
    print()

    N = 2_000_000
    ROW_GROUP_SIZE = 131_072   # 128K rows per row group (Parquet default)
    rng = np.random.default_rng(42)

    dates_random = rng.integers(20230101, 20241231, N).astype(np.int64)
    dates_sorted = np.sort(dates_random)
    values       = rng.random(N).astype(np.float64)
    predicate    = 20240101

    def scan_with_row_group_stats(dates, values, predicate, label):
        """Simulate Parquet row group predicate pushdown."""
        n_groups = (N + ROW_GROUP_SIZE - 1) // ROW_GROUP_SIZE
        rows_evaluated = 0
        groups_skipped = 0
        result_sum = 0.0

        t0 = time.perf_counter()
        for g in range(n_groups):
            start = g * ROW_GROUP_SIZE
            end   = min(start + ROW_GROUP_SIZE, N)
            group_dates = dates[start:end]

            # Parquet row group stats: min and max date
            rg_min = group_dates.min()
            rg_max = group_dates.max()

            if rg_max <= predicate:
                # Entire group below predicate → skip (0 rows evaluated)
                groups_skipped += 1
            elif rg_min > predicate:
                # Entire group above predicate → include all (0 per-row branches)
                result_sum += values[start:end].sum()
                rows_evaluated += (end - start)
            else:
                # Mixed group: per-row evaluation needed
                mask = group_dates > predicate
                result_sum += values[start:end][mask].sum()
                rows_evaluated += (end - start)

        elapsed = time.perf_counter() - t0

        print(f"  {label}:")
        print(f"    Total row groups:  {n_groups}")
        print(f"    Skipped entirely:  {groups_skipped} "
              f"({groups_skipped/n_groups*100:.0f}%)")
        print(f"    Rows evaluated:    {rows_evaluated:,} "
              f"({rows_evaluated/N*100:.0f}%)")
        print(f"    Time:              {elapsed*1000:.1f} ms")
        print(f"    Result sum:        {result_sum:.2f}")
        return elapsed

    t1 = scan_with_row_group_stats(dates_random, values, predicate, "Random (unsorted)")
    print()
    t2 = scan_with_row_group_stats(dates_sorted, values, predicate, "Sorted by date")

    print(f"\n  Speedup from sorting: {t1/t2:.1f}×")
    print(f"  Sorted data allows the Parquet reader to skip most row groups")
    print(f"  without per-row branch evaluation → fewer branch instructions total.")


if __name__ == '__main__':
    if HAS_SPARK:
        lab3_with_spark()
    else:
        lab3_without_spark()
```

---

## 15. Summary

The instruction pipeline executes many instructions simultaneously by overlapping fetch, decode, and execute stages for different instructions. Conditional branches break this overlap: the CPU does not know which instruction to fetch next until the branch condition is evaluated at the Execute stage, ~15–20 pipeline stages after Fetch. The branch predictor bridges this gap by guessing the branch outcome from historical patterns, allowing speculative execution of the predicted path. A wrong prediction costs the misprediction penalty: 15–20 cycles to flush the pipeline and restart from the correct path.

Modern branch predictors (TAGE-SC-L) achieve 99%+ accuracy for predictable patterns (loop counters, invariant conditions, sorted data). They fail — dropping to ~50% accuracy — for data-dependent branches whose outcomes are effectively random (threshold on random data, hash-based conditions, unsorted array comparisons).

For data engineering, the practical fix is to move conditional logic from per-element branches to SIMD branchless operations:

- `if x > threshold` in a Python loop → `arr > threshold` (boolean mask, no branch)
- `df.apply(conditional_fn)` → `np.select([cond1, cond2], [val1, val2])` (vectorized select)
- Conditional sum → `np.sum(arr * (arr > threshold))` (multiply-accumulate, no branch)
- NaN handling → `~np.isnan(arr) & (arr > threshold)` (boolean AND, no branch)

Spark's predicate pushdown converts row-level branching into row-group-level metadata comparison — for sorted columnar data, this eliminates 80–99% of per-row branch evaluations entirely.

---

## 16. Interview Q&A

### Q1: What is the branch misprediction penalty and why is it 15–20 cycles specifically?

**Answer:**

The branch misprediction penalty is the number of cycles lost when the branch predictor guesses wrong. The magnitude comes directly from the pipeline depth: the number of stages between the Fetch stage (where the branch prediction is used to select the next instruction to fetch) and the Execute stage (where the branch condition is actually computed and the prediction can be verified).

On modern Intel and AMD CPUs, this depth is roughly 14–20 pipeline stages. When the CPU fetches instructions based on a prediction at stage 1, those instructions flow through decode, rename, dispatch, and into the execution unit queues. By the time the branch instruction reaches the Execute stage at cycle ~15–20, the CPU has already fetched and partially executed 15–20 instructions on the incorrectly predicted path. All of these must be squashed: their register writes are discarded via the rename table rollback, their memory loads are invalidated, their reservation station entries are freed. The Fetch stage is then redirected to the correct path and must refill the pipeline from scratch.

The penalty is roughly equal to the pipeline depth because that is how many instructions were speculated on the wrong path. A shallower pipeline (like the classic RISC 5-stage pipeline of the 1990s) would have a 3–5 cycle penalty. The deeper pipelines of modern high-performance CPUs enable higher clock frequencies and more ILP — but the tradeoff is a higher misprediction penalty. Intel's Pentium 4 "Netburst" pushed pipeline depth to 31 stages, achieving very high clock frequencies but suffering a ~30-cycle misprediction penalty; this was part of why the architecture was abandoned in favor of the Core architecture.

For a data engineering hot loop with a 2-cycle body (8 instructions at IPC=4), a 16-cycle misprediction means any branch missing more than ~12% of the time makes the misprediction the dominant cost.

---

### Q2: Why does sorting data before a threshold filter improve performance for scalar code but not for vectorized NumPy operations?

**Answer:**

The answer lies in where the branching actually occurs.

For scalar code (a Python loop with `if arr[i] > threshold`), the comparison generates a conditional branch instruction in the CPU's instruction stream. The branch predictor must predict its outcome before the comparison is computed. For random data, outcomes are 50% True / 50% False with no pattern — the predictor is essentially random, achieving ~50% miss rate. Sorting the data transforms the branch pattern: all False outcomes occur first (elements below threshold), followed by a single True transition, then all True outcomes. The TAGE predictor detects the long run of False, then the long run of True, and misses only at the single transition point — one miss total instead of N/2 misses. For N=10M: 1 miss vs 5M misses.

For vectorized NumPy code (`arr > threshold`), there is no conditional branch per element. The comparison is a SIMD instruction: `vcmpgtpd ymm1, ymm0, threshold_broadcast` (AVX2). This instruction compares 4 float64 values simultaneously and produces a bitmask — all four comparisons happen in the ALU unconditionally, producing a mask of 1s and 0s. There is no branch taken/not-taken decision and therefore no branch prediction needed. The result is a bitmask that is used for subsequent masked operations or boolean indexing. The CPU's branch predictor hardware is completely uninvolved.

The lesson: sorting helps branch-based scalar code because it converts random branch outcomes into a predictable pattern. It provides zero benefit for SIMD branchless code because there are no branches to predict. Since SIMD code is typically 10–100× faster than scalar code even without the prediction improvement, the correct optimization path is always: use SIMD branchless operations rather than trying to sort data to improve scalar branch prediction.

---

### Q3: Explain why Pandas `apply` with a conditional is slow and how to rewrite it.

**Answer:**

`apply` with a Python function is slow for three compounding reasons, all related to the instruction pipeline and branch prediction.

First, function call overhead. For each of N rows, Pandas calls a Python function. Python function calls involve: looking up the function object in memory (pointer dereference), setting up a new stack frame, entering the CPython eval loop, and exiting on return. This is ~100–300 ns per call — 100–300 machine instructions for bookkeeping before any real work happens. For 1M rows: 100–300 ms of pure function-call overhead.

Second, Python interpreter overhead. The function body is interpreted bytecode. The CPython eval loop's main `switch` statement dispatches on opcode via a computed goto — an indirect branch per bytecode instruction. The branch predictor must predict the target of each indirect dispatch. Python bytecode is ~3–5 opcodes per Python statement; each opcode dispatch is an indirect branch. With 5 opcodes per `if-elif-else` body: 5 indirect branches per row × 1M rows = 5M indirect branch predictions needed. Indirect branches stress the Indirect Branch Target Buffer, which is smaller than the direct BTB — higher miss rate than direct branches.

Third, the conditional branch itself. The `if condition` in the Python function is a conditional branch whose outcome depends on the row value — data-dependent, potentially ~50% miss rate for random data.

The fix is `np.select`:

```python
# Branchy:
df['tier'] = df.apply(lambda r: 'A' if r['v'] > 0.7 else ('B' if r['v'] > 0.3 else 'C'), axis=1)

# Branchless:
df['tier'] = np.select([df['v'] > 0.7, df['v'] > 0.3], ['A', 'B'], default='C')
```

`np.select` compiles to: (1) vectorized comparison `df['v'] > 0.7` → boolean array via SIMD (no branch), (2) vectorized comparison `df['v'] > 0.3` → boolean array (no branch), (3) SIMD blend of the three output arrays based on the two masks. All three steps are SIMD instructions on all N values simultaneously. No Python function calls, no interpreter overhead, no conditional branches per element. Typical speedup: 20–100×.

---

### Q4: How does the TAGE branch predictor differ from a simple bimodal predictor, and what patterns does it detect that bimodal misses?

**Answer:**

The bimodal predictor uses a table of 2-bit saturating counters indexed by the branch instruction's PC. Each counter remembers only whether the branch was recently taken or not-taken — it has no context about what happened at other branches or in previous iterations. For a branch whose behavior is context-dependent, the bimodal predictor fails.

Example: consider a loop body:

```
if (input[i] % 2 == 0):     # branch A: alternates T/NT based on data
    process_even(input[i])
if (n > 100 && branch_A_was_taken):  # branch B: depends on branch A's outcome
    do_extra()
```

The bimodal predictor for branch B has a single counter that sees the sequence: T, NT, T, NT, ... (alternating, because it correlates with A). Its counter oscillates between states — it never settles. Miss rate: ~100%.

TAGE maintains multiple prediction tables with geometrically increasing history lengths. In this example, the two-bit history table (T₁, 2-bit GHR) captures that branch B is Taken when GHR's last 2 bits show branch A was Taken. The table entry indexed by (PC_B XOR GHR[1:0]) where GHR[1:0]="01" (A was Taken) always predicts Taken — correct. The entry for GHR="00" (A was Not Taken) always predicts Not Taken — correct. TAGE detects the correlation that bimodal misses entirely.

More generally: TAGE's geometric history lengths (2, 4, 8, 16, 32, 64 bits or similar) allow it to detect patterns that repeat with periods of 2 to 64 iterations. Nested loops (outer loop counter influences inner loop branch), state machines (current state influences transition probability), and correlated conditionals (condition A's outcome predicts condition B) are all capturable by some table in the TAGE hierarchy.

The tagged entries (unlike bimodal's untagged counters) prevent aliasing: two unrelated branches that map to the same table index have different tags → the prediction is declined and falls back to the next shorter history table. This is why TAGE achieves 99%+ accuracy on typical code while bimodal achieves only 90–95%.

---

### Q5: A Spark filter on a high-cardinality column is slow even with predicate pushdown enabled. What could be wrong?

**Answer:**

Predicate pushdown to the Parquet reader eliminates row groups that are entirely outside the predicate range using row group statistics (min/max per column per row group). For this to eliminate work efficiently, two conditions must hold: the column values must be correlated with their position in the file (sorted or clustered), and the predicate must select a minority of row groups.

If the filter column is unsorted (random order within the file), row group statistics are nearly useless: each row group's min/max spans a wide range, few row groups are entirely excluded, and almost all rows require per-row evaluation. Predicate pushdown still helps by skipping the decode/materialization of other columns (column pruning), but it does not reduce per-row branch evaluations for the filter column itself.

Other possible causes: the column is not in the Parquet metadata's statistics (statistics are not written for columns with very high cardinality or when `parquet.statistics.truncate-length` is set). Verify with `parquet-cli meta` on the file — look for min/max statistics per column per row group. If statistics say "null" for min/max, pushdown cannot evaluate them.

A different issue: the predicate is on a derived column computed at scan time (e.g., a function of two columns). Spark can push down simple predicates on raw columns but cannot push down function-based predicates to the Parquet reader. The `EXPLAIN` output will show the filter in the Parquet `PushedFilters` if pushdown succeeded, or in a separate `Filter` node above the scan if it did not.

For a high-cardinality column that is frequently filtered, the correct fix is to write Parquet sorted by that column (e.g., `df.orderBy('timestamp').write.parquet(...)`) and set Parquet row group size large enough that sorting meaningfully clusters values (~128 MB default). Then re-check `EXPLAIN`: the row group skip ratio should be visible in the Spark UI under "Files pruned."

---

### Q6: Why is `np.where(condition, a, b)` faster than a Python conditional expression for large arrays, even though both "compute" both branches?

**Answer:**

The performance difference is not about which branches are computed — it is about whether the computation uses scalar interpretation or SIMD parallelism, and whether conditional branches appear in the instruction stream.

In a Python conditional expression (`[a[i] if cond[i] else b[i] for i in range(N)]`), the CPU executes a per-element branch instruction for each `if cond[i]`. With random data, this branch has a ~50% miss rate. Additionally, each iteration requires a Python interpreter dispatch (computed goto on opcode), list append machinery, and PyObject reference counting. The combination gives ~200–500 ns per element.

`np.where(condition, a, b)` compiles at the NumPy layer to a C inner loop that uses SIMD blend instructions. On AVX2: `vblendvpd ymm0, a_ymm, b_ymm, cond_mask` selects 4 float64 values in a single instruction based on a mask. There is no branch instruction — the blend hardware takes both `a` and `b` as inputs and produces the output in a single ALU operation. The instruction latency is 2–3 cycles; it processes 4 values simultaneously; it never mispredicts because there is no prediction.

Both approaches compute (or at least load) both `a` and `b` values. But `np.where` processes them in SIMD units (4–8 values per cycle, no branch), while the Python loop processes them in the interpreter (1 value per 200–500 ns, 50% branch misses). The SIMD version is 100–500× faster not because it avoids computing the false branch, but because it eliminates the branch prediction hardware entirely.

The subtle implication: even when one branch is expensive (e.g., `np.sqrt`), `np.where` still evaluates it for all elements. If only 1% of elements need `np.sqrt`, then `np.where(mask, np.sqrt(arr), arr)` computes `np.sqrt` for all N elements — 100× more sqrt calls than necessary. In this case, a hybrid: compute the mask first, then apply sqrt only to the 1% of matching elements with `result[mask] = np.sqrt(arr[mask])` — one branch per batch (the boolean indexing), not per element.

---

## 17. Cross-Question Chain

**Topic:** From "what is a branch misprediction?" to "how do you eliminate branch pressure in a Spark analytics pipeline?"

---

**Interviewer:** What happens when a CPU branch prediction is wrong?

**Candidate:** The CPU speculatively executes instructions on the predicted path while waiting for the branch condition to resolve. When the branch condition reaches the Execute stage ~15–20 cycles later and the prediction is wrong, all speculatively-executed instructions must be squashed from the Reorder Buffer. Their results are discarded, the rename table is rolled back to the pre-speculation state, and the Fetch stage is redirected to the correct target. The pipeline refills from scratch. This penalty is 15–20 cycles on modern Intel and AMD CPUs — equivalent to 60–80 L1-cached instructions of wasted work per misprediction.

---

**Interviewer:** What determines whether the predictor gets it right?

**Candidate:** Pattern predictability. The TAGE predictor (used in modern CPUs) maintains tables with geometrically increasing history lengths — from 2-bit up to 64-bit histories of recent branch outcomes. It can detect patterns that repeat within 64 iterations: loop counters, sorted-data transitions, correlated conditionals, state machine sequences. It achieves 99%+ accuracy for these. But for a branch whose outcome depends on genuinely random data — a threshold filter on randomly-ordered values — the pattern length is infinite and TAGE's history is too short. It degrades to ~50% accuracy, the same as a coin flip. No predictor can beat this fundamental limit.

---

**Interviewer:** How does this show up in data engineering code?

**Candidate:** Most visibly in Python loops with conditionals over random data. `if arr[i] > threshold` in a for-loop on randomly-ordered float64 data: ~50% miss rate, ~16 cycles per miss. For a 10M-row array: 5M misses × 16 cycles = 80M wasted cycles, roughly 27 ms at 3 GHz — in addition to Python interpreter overhead. The same pattern appears in Pandas `apply` with conditionals, any row-level processing that branches on a computed value, and indirectly in Spark row-iteration UDFs where Python handles each row.

---

**Interviewer:** How do you fix it?

**Candidate:** Replace the branching conditional with a SIMD branchless equivalent. Instead of `if arr[i] > threshold: result.append(arr[i])`, use `arr[arr > threshold]`. The expression `arr > threshold` compiles to `vcmpgtpd` — an AVX2 instruction that compares 4 float64 values against the threshold simultaneously, producing a bitmask. No branch instruction; no prediction; no misprediction. The resulting mask is used for boolean indexing, which is a hardware gather or sequential scan with a predictable branch (mask values in order). Speedup: 50–200× over a Python loop with conditional, with the branch misprediction eliminated entirely.

---

**Interviewer:** How does this apply to a Spark query pipeline?

**Candidate:** Three places to eliminate branch pressure. First: Spark SQL expressions over raw columns compile (via Tungsten's code generation) to JVM bytecode that JIT-compiles to direct SIMD-capable x86 code. A `WHERE date_col > '2024-01-01'` in Spark SQL is vectorized and branchless at the execution level. An equivalent Python UDF processes each row through the Python interpreter — indirect branches per opcode, data-dependent branch per conditional. Always prefer Spark SQL expressions over Python UDFs for filtering.

Second: predicate pushdown to Parquet. Spark pushes `WHERE date_col > threshold` down to the Parquet reader, which checks row group statistics (min/max per 128K-row group) before deserializing any data. For date-sorted Parquet, most row groups are entirely before or entirely after the threshold — they are skipped without deserializing a single row, let alone evaluating a branch. This converts N per-row branch evaluations into N/128K row-group metadata comparisons — a ~10,000× reduction in branch instructions for the filter, independent of predictor accuracy.

Third: UDF vectorization via Pandas UDFs (Arrow-based). Spark's Pandas UDF passes Arrow column buffers to Python, where NumPy/Pandas vectorized operations replace per-row branching with SIMD operations. This eliminates the Python function-call overhead and replaces N data-dependent branches with SIMD comparisons.

---

## 18. What's Next

**CSF-ARC-103 M02: SIMD and Vectorized Execution** — M01 showed that branchless SIMD is the cure for branch mispredictions. M02 opens up the SIMD architecture: what AVX2 and AVX-512 instructions look like, how they process 4–16 values per clock cycle, how NumPy and Arrow leverage them, and how to verify that a hot loop is actually being vectorized. M02 explains why `np.sum(arr)` achieves 20–30 GB/s throughput and how JVM JIT (and Spark's Tungsten) compiles SQL expressions to SIMD instructions — connecting the branch-free patterns from M01 to the hardware that executes them.

**CSF-ARC-103 M03: IPC Measurement and Optimization** — M02 introduces SIMD as the mechanism for high IPC; M03 teaches measurement. Using `perf stat -e cycles,instructions,cache-misses,branch-misses`, you can compute the full performance equation from CSF-ARC-101 M05: `runtime = instructions × (1/IPC) × (1/clock_hz)`. M03 shows how to decompose IPC degradation into its components: cache misses (from M01–M03), branch misses (M01 of this course), execution port pressure (M02), and memory bandwidth saturation (M04).

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is the branch misprediction penalty? | 15–20 cycles on modern Intel/AMD CPUs — the number of pipeline stages between Fetch (where prediction is used) and Execute (where branch outcome is known). All speculatively-fetched instructions on the wrong path are squashed and must be refetched. |
| 2 | Why does the CPU speculate past branches instead of stalling? | Stalling would idle all pipeline stages downstream for 15–20 cycles per branch. At ~1 branch per 5 instructions, stalling would reduce throughput by ~75%. Speculation keeps the pipeline full at the cost of an occasional flush. |
| 3 | What is the TAGE branch predictor? | Tagged Geometric history length predictor. Maintains multiple prediction tables with geometrically increasing history lengths (2, 4, 8, 16, 32, 64 bits). Detects patterns in up to 64 recent branches. Current state-of-the-art, used in Intel and AMD CPUs. |
| 4 | What patterns defeat all branch predictors? | Branches whose outcomes are determined by genuinely random data (random numbers, hash outputs, unsorted array values). Accuracy degrades to ~50% — no predictor can predict a fair coin. |
| 5 | What is the branch miss rate formula for loop throughput? | `cycles/iter = (N_instructions / IPC) + miss_rate × penalty`. For 8 instructions at IPC=4, 16-cycle penalty: at 50% miss rate: `2 + 0.5×16 = 10 cycles/iter` (5× slower than miss-free). |
| 6 | How does sorting data improve branch prediction for scalar code? | Sorted data transforms a random sequence of True/False outcomes into one run of False followed by one run of True. The predictor misses only once (at the transition point) instead of ~N/2 times. |
| 7 | Why doesn't sorting help for `np.where` or `arr[arr > threshold]`? | These are branchless SIMD operations. No conditional branch instruction is generated per element — the comparison produces a mask via `vcmpgtpd`. No branch prediction hardware is involved. |
| 8 | What x86 instruction does `np.clip(arr, lo, hi)` compile to? | `vmaxpd` (for lo) and `vminpd` (for hi) — AVX2 instructions that compute max/min of 4 float64 values simultaneously without any conditional branch. |
| 9 | What x86 instruction does `np.where(cond, a, b)` compile to? | `vblendvpd` (AVX2) or `vpblendvb` — SIMD blend instruction that selects from a or b based on a mask, processing 4 values per instruction. No branch taken/not-taken. |
| 10 | How does Spark predicate pushdown reduce branch instructions? | Pushes filter predicates to the Parquet row group statistics check (1 comparison per 128K-row group). For sorted columns: most row groups are entirely inside or outside the predicate → excluded/included wholesale → near-zero per-row branch evaluations. |
| 11 | Why is Pandas `apply` slow for conditional functions? | Three compounding costs: Python function call overhead (~200 ns/call), CPython interpreter indirect branch per opcode (~5 branches per statement), and data-dependent conditional branch with ~50% miss rate on random data. |
| 12 | What is `np.select` and why is it branchless? | `np.select([cond1, cond2], [val1, val2], default)` computes all conditions as boolean arrays (vectorized comparisons), then uses SIMD blend to select from the value arrays. All N evaluations happen simultaneously in SIMD units — no per-element branch instruction. |
| 13 | What is speculative execution? | Executing instructions that follow a predicted branch before the branch outcome is confirmed. If correct: results are committed, pipeline runs at full speed. If wrong: speculative results are discarded (squashed), correct path is fetched. |
| 14 | What is the Branch Target Buffer (BTB)? | A hardware cache table indexed by branch instruction PC, storing the predicted target address for taken branches. Allows the Fetch stage to redirect to the predicted target before the branch is even decoded. Size: 4K–8K entries. |
| 15 | What is the Return Address Stack (RAS)? | A hardware stack (~16–32 entries) that stores return addresses for CALL instructions. When a CALL is executed, the return address is pushed. When RET is executed, the RAS is popped to predict the return target. ~100% accuracy for regular call/return. |
| 16 | When is a branchy conditional faster than a branchless one? | When the branch is highly predictable (>99% accurate) AND the true/false computation is expensive. Branchless code always computes both paths — if only 1% of elements take the expensive path, branchless computes it 100× more often than necessary. |
| 17 | What does `perf stat -e branch-misses,branches` measure? | `branches`: total branch instructions executed. `branch-misses`: branches predicted incorrectly. `miss_rate = branch-misses/branches`. Target: <1% for well-optimized hot loops. |
| 18 | How does `np.dot(arr, arr > threshold)` eliminate the conditional? | `arr > threshold` produces a 0.0/1.0 float array (branchless comparison). `np.dot` computes `Σ arr[i] * mask[i]` — elements where mask=0 contribute 0 to the sum (multiply-accumulate, no branch). Compiled to FMA (fused multiply-add) SIMD instructions. |
| 19 | Why does Python `for x in arr: if x > t: count += 1` have both branch miss overhead AND Python overhead? | The Python interpreter executes each opcode via a computed goto (indirect branch) + the data-dependent `if` branch. Both are branches the CPU must predict. The indirect goto is a IBTB miss for each new opcode type encountered; the data-dependent branch misses ~50% of the time on random data. |
| 20 | What is the bimodal predictor's fundamental limitation? | It uses only the branch's own PC to index the prediction table — no global history context. Two unrelated branches that share a table index interfere. Cannot detect patterns that depend on the outcomes of other branches (correlation-based patterns). |

---

## 20. References

**Branch Prediction Architecture**

- Seznec, A., & Michaud, P. (2006). A case for (partially) tagged geometric history length branch prediction. *Journal of Instruction-Level Parallelism*, 8(1). — The TAGE predictor paper; the algorithm implemented in Intel Haswell and later, AMD Zen 3 and later.
- Fog, A. (2024). The microarchitecture of Intel, AMD, and VIA CPUs. Chapter 3 (Branch prediction). https://www.agner.org/optimize/microarchitecture.pdf — Precise cycle counts for branch misprediction penalties across CPU generations.
- Intel (2024). Intel 64 and IA-32 Architectures Optimization Reference Manual. Section 3.4 (Branch Prediction Optimization). https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html

**Practical Measurement**

- Bakhvalov, D. (2020). Branch Mispredictions. easyperf.net. https://easyperf.net/blog/2019/02/09/Improvement-of-the-algorithm — Concrete examples of measuring and reducing branch miss rate with perf.
- `perf c2c` and `perf stat` documentation: `man perf-stat`, `man perf-c2c`. Available in the Linux kernel source tree: `tools/perf/Documentation/`.

**Vectorized / Branchless Patterns**

- Harris, M., & Coatney, M. (2020). CUDA Parallel Reduction. GTC Conference. — While GPU-focused, the branchless reduction patterns apply directly to CPU SIMD.
- NumPy documentation: Universal Functions (ufuncs). https://numpy.org/doc/stable/reference/ufuncs.html — Which NumPy operations are branchless SIMD and which are not.

**Data Engineering Context**

- Dreyer, P. (2019). DuckDB: An Embeddable Analytical Database. *SIGMOD*. https://dl.acm.org/doi/10.1145/3299869.3320212 — Section 4 covers vectorized execution and how filter/projection operators eliminate per-row branches.
- Apache Spark documentation: Performance Tuning — Predicate Pushdown. https://spark.apache.org/docs/latest/sql-performance-tuning.html#predicate-pushdown — Official documentation on how Spark pushes predicates to the Parquet/ORC layer.
