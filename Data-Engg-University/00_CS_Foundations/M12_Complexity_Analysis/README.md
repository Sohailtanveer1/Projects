# M01: Complexity Analysis

**School:** CS Foundations (CSF)  
**Course:** CSF-ALG-101 — Algorithms and Data Structures for Data Engineers  
**Module:** 01 of 05  
**Difficulty:** ★★★☆☆  
**Estimated study time:** 4–5 hours  
**Last updated:** 2026-06-27

---

## Table of Contents

1. [Why This Module Exists](#1-why-this-module-exists)
2. [Prerequisites](#2-prerequisites)
3. [Learning Objectives](#3-learning-objectives)
4. [First Principles](#4-first-principles)
5. [Architecture — The Complexity Hierarchy](#5-architecture--the-complexity-hierarchy)
6. [Deep Dive — Big-O, Amortized Analysis, Space vs Time](#6-deep-dive--big-o-amortized-analysis-space-vs-time)
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

Data engineering interviews consistently test complexity analysis — not because interviewers want to quiz you on textbook definitions, but because complexity analysis is the vocabulary used to reason about whether a system will work at scale. "This join is O(N²)" means something precise: doubling the input quadruples the cost. "This aggregation is O(N log N)" means something precise about Spark shuffle. "This broadcast join is O(N)" means something about why it's faster.

Without this vocabulary, design conversations devolve into vague intuitions. With it, you can make quantitative predictions: if today's dataset is 1 TB and takes 1 hour, what happens at 10 TB? If the answer is "10 hours" (O(N)), you scale with more hardware. If the answer is "100 hours" (O(N²)), you need a different algorithm, not more hardware.

This module establishes the formal foundation — Big-O, Big-Θ, Big-Ω, amortized analysis, and space-time trade-offs — then immediately grounds each concept in data engineering systems: Spark shuffle complexity, Parquet scan complexity, Python dict amortized insertion, and the space-time trade-off embodied by bloom filters and hash joins.

---

## 2. Prerequisites

- CSF-ARC-101 M01–M05: Execution model, cache hierarchy, memory access costs.
- Basic Python: loops, lists, dicts, functions.

---

## 3. Learning Objectives

By the end of this module you will be able to:

1. Define O, Ω, and Θ notation precisely and distinguish them.
2. Derive the complexity of a given algorithm by analyzing its loop structure.
3. Apply amortized analysis to Python list append and dict resize operations.
4. Explain the space-time trade-off and give three concrete data engineering examples.
5. Map Spark operations to their complexity class (O(N), O(N log N), O(N²)).
6. Identify when a system is hitting a complexity wall vs a constant-factor hardware issue.
7. Calculate concrete predictions: "at 10× data, runtime increases by X."

---

## 4. First Principles

### What Complexity Measures

Complexity analysis answers one question: **how does resource consumption grow as input size grows?**

It deliberately ignores constants. It does not tell you whether an O(N log N) algorithm is faster than an O(N²) algorithm on a specific machine with a specific dataset of size 100 — it tells you that for large enough N, the O(N log N) algorithm will eventually be faster, and that at 10× the data, the O(N log N) algorithm takes ~33× longer while the O(N²) algorithm takes 100× longer.

This is useful because:
1. Data engineering works at scales where N is large enough for asymptotics to dominate.
2. Hardware improvements scale constants, not complexity classes. A 2× faster CPU makes both algorithms 2× faster but does not change the ratio between them.
3. Complexity mispredictions cause production incidents. A pipeline that works at 100 GB can break at 1 TB not because of hardware failure but because an O(N²) join that was "fast enough" at small N becomes 100× slower at 10× N.

### Formal Definitions

Let f(N) be the actual resource consumption (time or space) of an algorithm on input of size N.

**Big-O (upper bound):** f(N) = O(g(N)) means there exist constants c > 0 and N₀ such that for all N ≥ N₀: f(N) ≤ c × g(N). In English: "f grows no faster than g, up to a constant." Used to describe worst-case bounds.

**Big-Ω (lower bound):** f(N) = Ω(g(N)) means there exist constants c > 0 and N₀ such that for all N ≥ N₀: f(N) ≥ c × g(N). Used to describe best-case bounds or prove that no algorithm can do better.

**Big-Θ (tight bound):** f(N) = Θ(g(N)) means f(N) = O(g(N)) AND f(N) = Ω(g(N)). The algorithm grows exactly at rate g(N), up to constants. The most informative characterization.

**Little-o (strict upper bound):** f(N) = o(g(N)) means the ratio f(N)/g(N) → 0 as N → ∞. f grows strictly slower than g — not just bounded, but dominated.

In practice, data engineers use Big-O loosely to mean Big-Θ (tight bound). When someone says "merge sort is O(N log N)", they typically mean Θ(N log N).

### What "N" Means

N is the input size, but "input size" requires careful definition:

- **Array/list:** N = number of elements.
- **String:** N = number of characters.
- **Graph:** N = number of vertices, M = number of edges. Graph complexity uses both: BFS is O(N + M).
- **Spark RDD/DataFrame:** N = number of rows. But partition count P also matters: shuffle cost is O(N log N) for the data movement but O(N/P × log(N/P)) per executor.
- **Join:** N = rows in left table, M = rows in right table. Hash join build: O(M). Hash join probe: O(N). Total: O(N + M).

---

## 5. Architecture — The Complexity Hierarchy

```
COMPLEXITY HIERARCHY (from fastest to slowest growth)
══════════════════════════════════════════════════════

Class       │ Name           │ Example at N=1M    │ Data Eng Example
────────────┼────────────────┼────────────────────┼──────────────────────────────
O(1)        │ Constant       │ 1 op               │ Hash table lookup (avg)
            │                │                    │ Parquet row group skip
            │                │                    │ Redis GET
────────────┼────────────────┼────────────────────┼──────────────────────────────
O(log N)    │ Logarithmic    │ ~20 ops            │ Binary search in sorted array
            │                │                    │ B-tree index lookup
            │                │                    │ Parquet column chunk seek
────────────┼────────────────┼────────────────────┼──────────────────────────────
O(N)        │ Linear         │ 1,000,000 ops      │ Full table scan
            │                │                    │ Parquet column scan
            │                │                    │ Kafka consumer read
────────────┼────────────────┼────────────────────┼──────────────────────────────
O(N log N)  │ Linearithmic   │ ~20,000,000 ops    │ External merge sort (Spark)
            │                │                    │ Sort-merge join
            │                │                    │ Parquet write (sorted)
────────────┼────────────────┼────────────────────┼──────────────────────────────
O(N²)       │ Quadratic      │ 10¹² ops           │ Nested loop join (no index)
            │                │                    │ Naive string matching
            │                │                    │ Bubble sort
────────────┼────────────────┼────────────────────┼──────────────────────────────
O(N³)       │ Cubic          │ 10¹⁸ ops           │ Naive matrix multiplication
            │                │                    │ (never in production DE)
────────────┼────────────────┼────────────────────┼──────────────────────────────
O(2^N)      │ Exponential    │ 10^301,030 ops     │ Brute-force NP-complete
            │                │                    │ (avoid entirely)
────────────┴────────────────┴────────────────────┴──────────────────────────────

SCALE IMPACT: What happens at 10× data?
  O(1):       ×1    (same cost)
  O(log N):   ×1.07 (barely changes: log(10N) = log(10) + log(N))
  O(N):       ×10   (linear: 10× data = 10× time)
  O(N log N): ×11.5 (slightly more than linear)
  O(N²):      ×100  (10× data = 100× time — the quadratic wall)
  O(N³):      ×1000 (10× data = 1000× time)
```

---

## 6. Deep Dive — Big-O, Amortized Analysis, Space vs Time

### 6.1 Deriving Complexity from Code

The rules for reading complexity from loop structure:

**Rule 1 — Sequential steps: add.**
```python
def two_scans(arr):
    for x in arr: process_a(x)   # O(N)
    for x in arr: process_b(x)   # O(N)
    # Total: O(N) + O(N) = O(2N) = O(N)  (constants drop)
```

**Rule 2 — Nested loops: multiply.**
```python
def nested(arr):
    for i in arr:          # N iterations
        for j in arr:      # N iterations (inner)
            compare(i, j)  # O(1)
    # Total: N × N × O(1) = O(N²)
```

**Rule 3 — Halving at each step: log.**
```python
def binary_search(arr, target):
    lo, hi = 0, len(arr) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if arr[mid] == target: return mid
        elif arr[mid] < target: lo = mid + 1
        else: hi = mid - 1
    return -1
# Each iteration halves the search space.
# After k iterations: N / 2^k = 1 → k = log₂N.
# Total: O(log N)
```

**Rule 4 — Recursion: solve the recurrence.**
```
Merge sort recurrence: T(N) = 2T(N/2) + O(N)
  [two recursive calls on half-size input] + [O(N) merge step]

Master theorem: T(N) = aT(N/b) + O(N^d)
  a=2, b=2, d=1: log_b(a) = log_2(2) = 1 = d
  → T(N) = O(N^d × log N) = O(N log N)  [case 2 of Master theorem]
```

**Rule 5 — Drop lower-order terms.**
```python
def mixed(arr):
    # Step 1: O(N²)
    for i in range(len(arr)):
        for j in range(len(arr)):
            work(arr[i], arr[j])
    # Step 2: O(N log N)
    sorted_arr = sorted(arr)
    # Step 3: O(N)
    for x in sorted_arr:
        scan(x)
    # Total: O(N²) + O(N log N) + O(N) = O(N²)
    # (N² dominates; lower terms are negligible for large N)
```

### 6.2 Amortized Analysis

Amortized analysis asks: what is the **average cost per operation** over a sequence of N operations, even if individual operations have wildly varying costs?

The classic example is Python list `append`:

```
Python list.append() — dynamic array growth:

  Internal structure: contiguous array with a capacity.
  When len == capacity: allocate new array of capacity × 2, copy all elements.
  
  Individual costs:
    append when len < capacity:    O(1)  (just write the element)
    append when len == capacity:   O(N)  (allocate + copy N elements)

  Sequence of N appends starting from empty list:
    Resizes occur at: N=1, 2, 4, 8, 16, 32, ...
    Cost of resize at size k: O(k) (copying k elements)
    Total resize cost: 1 + 2 + 4 + 8 + ... + N/2 + N = O(2N) = O(N)
    Cost of non-resize appends: N × O(1) = O(N)
    
    Total cost of N appends: O(N) + O(N) = O(N)
    Amortized cost per append: O(N) / N = O(1)
    
  "Amortized O(1)" means: any single append may cost O(N),
  but if you average the cost over N appends, each costs O(1).

Contrast with a list that grows by 1 each time (not Python's behavior):
    Resizes at every append: 1 + 2 + 3 + ... + N = O(N²) total
    Amortized cost per append: O(N²)/N = O(N)  ← much worse
```

**Python dict amortized analysis:**

```
Python dict uses open addressing with load factor threshold ~2/3.
When len(d) > capacity × 2/3: resize to 4× current capacity, rehash all.

Sequence of N insertions:
  Rehashes at: N ≈ 5, 11, 22, 45, 90, 181, ...  (2/3 × power-of-2 capacities)
  Cost of rehash at size k: O(k) (rehash k entries)
  Total rehash cost: sum of rehash sizes ≈ O(3N) = O(N)
  Non-rehash inserts: N × O(1) = O(N)
  
  Total: O(N).  Amortized cost per insert: O(1).
  
  BUT: worst-case single insert after rehash trigger: O(N).
  Latency-sensitive systems (streaming, low-latency APIs) must account for
  this O(N) spike — it's why some production systems pre-size dicts.
```

### 6.3 Space vs Time Trade-offs

Most algorithmic improvements trade more memory for less time (or vice versa). The pattern is universal in data engineering:

**Example 1 — Hash join vs nested loop join:**
```
Nested loop join (no extra space):
  For each row in left (N rows):
    Scan all rows in right (M rows) to find matches
  Time: O(N × M)    Space: O(1) extra

Hash join (extra space for hash table):
  Build: scan right table, build hash table: O(M) time, O(M) space
  Probe: scan left table, probe hash table: O(N) time, O(1) per probe
  Total: O(N + M) time,  O(M) space
  
  Space trade: O(M) extra memory (hash table)
  Time gain:   O(N×M) → O(N+M)  [a dramatic improvement for large N, M]
  
  At N=M=10M: nested loop = 10¹⁴ ops; hash join = 2×10⁷ ops
  This is why Spark uses hash join by default when the inner table fits in memory.
```

**Example 2 — Bloom filter:**
```
Bloom filter: probabilistic set membership test.

Without bloom filter (check if key exists in large external table):
  Query the table: O(disk I/O per lookup) — expensive

With bloom filter (in-memory bit array, ~10 bits per element):
  Check the filter: O(k) where k = number of hash functions (typically k=7)
  → O(1) in practice (k is constant)
  Result: "definitely NOT in table" → skip the expensive lookup (no false negatives)
          "POSSIBLY in table"       → do the lookup (false positive rate ~1%)

  Space: 10 bits per element × N = 1.25 bytes/element × N
  For N = 1 billion keys: ~1.25 GB in memory
  
  Time saved: skip ~99% of disk lookups for absent keys
  
  Used in: Parquet row group filters, BigTable, Cassandra (SSTables),
           Kafka's log compaction, Flink's state backend.
```

**Example 3 — Sort for repeated lookups:**
```
Repeated binary search after sorting:
  Sort cost:      O(N log N) one time
  Per lookup:     O(log N) instead of O(N) linear scan
  Breakeven:      after K lookups where K × O(N) > O(N log N) + K × O(log N)
                  K > log N / (1 - log N / N) ≈ log N for large N
  
  For N=1M: sort costs O(20M ops). Each scan costs O(1M). Each binary search costs O(20).
  After 21 lookups: sort+search total < scan×21. Every lookup after that is profit.
  
  Data engineering equivalent: building a Parquet index / writing sorted Parquet.
  One-time O(N log N) sort → every subsequent query benefits from O(log N) row group seeks.
```

---

## 7. Mental Models

### Mental Model 1 — The Scale Multiplier Table

Whenever someone proposes a new algorithm or data structure, the first question is: what is the complexity class, and what does that mean at our production scale?

```
If today N = 1 TB (≈ 10¹² bytes, ~10¹⁰ rows at 100 bytes/row):
  And tomorrow N = 10 TB (10× growth):

  O(1):         1 TB pipeline → 1 TB pipeline        (no change)
  O(log N):     1 TB pipeline → 1.07 TB pipeline     (barely changes)
  O(N):         1 hr at 1 TB → 10 hr at 10 TB        (add hardware: 10× machines → 1 hr)
  O(N log N):   1 hr at 1 TB → 11.5 hr at 10 TB      (still scalable with hardware)
  O(N²):        1 hr at 1 TB → 100 hr at 10 TB       (no hardware fix; algorithm change needed)

Rule: if the complexity exponent is > 1 (O(N^k) for k > 1), horizontal scaling
cannot solve the problem — you need a better algorithm.
If the exponent is ≤ 1 (O(N) or O(N log N)), horizontal scaling works.
```

### Mental Model 2 — The Amortized Piggy Bank

For amortized analysis, think of each operation depositing "credits" into a piggy bank. Cheap operations deposit extra credits. Expensive operations withdraw from the bank.

For Python list append:
- Each O(1) append deposits 2 credits: 1 pays for itself, 1 is saved.
- An O(N) resize at capacity N: withdraws N credits (already saved by N previous appends).
- Net: the piggy bank never goes negative → average cost is O(1) per operation.

This mental model predicts which systems will have latency spikes. If a system builds up "debt" (deferred expensive operations), any single operation that triggers the payoff will spike. This is why Kafka's log compaction, Cassandra's compaction, and Python's dict resize can cause sudden latency spikes in otherwise O(1) systems.

### Mental Model 3 — The Complexity Stack

Every data pipeline is a composition of operations. The pipeline's overall complexity is determined by its most expensive step.

```
Spark pipeline example:
  Step 1: Read Parquet column             O(N)     — sequential scan
  Step 2: Filter rows (predicate)         O(N)     — linear scan
  Step 3: Group by + aggregate            O(N log N) — sort-based groupby
  Step 4: Join with lookup table          O(N + M) — hash join, M = lookup size
  Step 5: Write result Parquet            O(N)     — sequential write

  Pipeline complexity: max(O(N), O(N), O(N log N), O(N+M), O(N))
                     = O(N log N)  [dominated by the groupby step]

  Optimization target: the groupby. If N_groups << N, use hash aggregation
  O(N) instead of sort-based O(N log N). This changes the pipeline from
  O(N log N) to O(N).
```

---

## 8. Failure Scenarios

### Failure 1 — The O(N²) Cross Join

**Symptom:** A Spark job that worked fine at 1 GB runs for 6 hours and fails at 100 GB. Memory errors on executors.

**Root cause:** Somewhere in the pipeline, an unintentional cross join (Cartesian product) was introduced. A cross join of N rows with M rows produces N×M output rows. At N=M=10M (100M row tables at ~10 bytes/row = ~1 GB each): output = 10¹⁴ rows — impossible to materialize.

**How it happens:** A Spark join with no join condition, or a join on a column with `null` values and `spark.sql.joins.nullEquality=false`, or a UDF that calls `collect()` on a large dataset and then iterates over it inside a `map`.

**Detection:** `df.explain()` shows `CartesianProduct` in the plan. Output row count estimate is N×M. Job stage metrics show output records >> input records.

---

### Failure 2 — Amortized Cost Spike in Production

**Symptom:** A streaming Kafka consumer that processes records in a Python dict accumulates state. Every few thousand records, processing latency spikes from 1 ms to 50 ms, then returns to normal.

**Root cause:** The Python dict holding accumulated state hits its load factor threshold and triggers a resize + rehash of all N entries. The spike interval is proportional to the current dict size: every time the dict doubles in capacity, a rehash occurs. At 10K entries: rehash cost ≈ 10K × hash + copy operations. At 100K entries: 100K operations. The spike magnitude grows with state size.

**Fix:** Pre-size the dict with `dict = {}` followed by `dict.update({k: 0 for k in known_keys})` to allocate sufficient capacity upfront, avoiding mid-stream resizes.

---

### Failure 3 — O(N log N) vs O(N²) Sort Algorithm Choice

**Symptom:** A Pandas sort on a DataFrame with a custom key function takes 10× longer than expected.

**Root cause:** Python's `sort` with a `key` function uses Timsort (O(N log N)), but if the key function is called O(N²) times due to incorrect implementation (e.g., a comparison function used instead of a key function), effective complexity becomes O(N²).

```python
import pandas as pd

# WRONG: cmp_to_key wraps a comparison function — works but may be slower
from functools import cmp_to_key
def bad_sort(df):
    return df.sort_values(by='col', key=cmp_to_key(lambda a, b: a - b))

# CORRECT: key= takes a transform function applied O(N) times, not O(N log N)
def good_sort(df):
    return df.sort_values(by='col')  # built-in comparator, O(N log N) total key ops
```

---

### Failure 4 — Complexity Hidden by Small N in Testing

**Symptom:** A unit test with N=1,000 passes in 0.1 seconds. Production with N=10,000,000 takes 100,000 seconds (28 hours). The test gave no warning.

**Root cause:** The algorithm is O(N²). At N=1K: 10⁶ ops ≈ 0.1s. At N=10M: 10¹⁴ ops ≈ 10⁸ seconds (years in practice; the job times out).

**Prevention:** Complexity analysis before merging. Alternatively: test at N=10, 100, 1,000, 10,000 and check that the runtime ratio between consecutive tests is consistent with the claimed complexity class. If runtime quadruples when N doubles, the algorithm is O(N²).

---

## 9. Recovery Procedures

### Identifying the Complexity Class of an Existing Algorithm

```python
"""
complexity_probe.py

Empirically determines the complexity class of any black-box function
by timing it at exponentially increasing input sizes and computing
the growth ratio.

Growth ratio interpretation:
  ratio ≈ 1:    O(1)      or O(log N)
  ratio ≈ 2:    O(N)      (doubling N doubles time)
  ratio ≈ 2.1:  O(N log N) (slightly more than linear)
  ratio ≈ 4:    O(N²)     (doubling N quadruples time)
  ratio ≈ 8:    O(N³)

Run: python3 complexity_probe.py
"""
import time
import math
import random
from typing import Callable, Any


def time_fn(fn: Callable, *args, runs: int = 3) -> float:
    """Return minimum wall-clock time in seconds."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn(*args)
        times.append(time.perf_counter() - t0)
    return min(times)


def probe_complexity(
    fn: Callable,
    input_generator: Callable[[int], Any],
    sizes: list = None,
    runs: int = 3,
) -> str:
    """
    Probe the empirical complexity of fn by timing at increasing sizes.

    Args:
        fn:               The function to probe (fn(input) → any).
        input_generator:  Generates input of given size N.
        sizes:            List of N values to try.
        runs:             Timing runs per size (min is taken).

    Returns:
        Estimated complexity class as a string.
    """
    if sizes is None:
        sizes = [100, 200, 400, 800, 1600, 3200, 6400]

    results = []
    for N in sizes:
        data  = input_generator(N)
        t     = time_fn(fn, data, runs=runs)
        results.append((N, t))

    print(f"\n{'N':>8}  {'Time (ms)':>12}  {'Ratio':>8}  {'Implied complexity'}")
    print("-" * 55)

    prev_N, prev_t = results[0]
    print(f"{prev_N:>8}  {prev_t*1000:>12.3f}  {'—':>8}")

    ratios = []
    for N, t in results[1:]:
        ratio = t / prev_t if prev_t > 0 else float('nan')
        n_ratio = N / prev_N

        # Infer complexity from timing ratio given input ratio
        # ratio ≈ n_ratio^k → k = log(ratio) / log(n_ratio)
        if ratio > 0 and n_ratio > 1:
            k = math.log(ratio) / math.log(n_ratio)
        else:
            k = float('nan')

        if k < 0.1:    implied = "O(1)"
        elif k < 0.3:  implied = "O(log N)"
        elif k < 1.1:  implied = "O(N)"
        elif k < 1.25: implied = "O(N log N)"
        elif k < 2.3:  implied = "O(N²)"
        elif k < 3.3:  implied = "O(N³)"
        else:          implied = f"O(N^{k:.1f})"

        print(f"{N:>8}  {t*1000:>12.3f}  {ratio:>8.2f}  {implied}")
        ratios.append(k)
        prev_N, prev_t = N, t

    if ratios:
        avg_k = sum(ratios) / len(ratios)
        if avg_k < 0.15:    conclusion = "O(1) — constant time"
        elif avg_k < 0.35:  conclusion = "O(log N) — logarithmic"
        elif avg_k < 1.1:   conclusion = "O(N) — linear"
        elif avg_k < 1.3:   conclusion = "O(N log N) — linearithmic"
        elif avg_k < 2.3:   conclusion = "O(N²) — quadratic ⚠️"
        elif avg_k < 3.3:   conclusion = "O(N³) — cubic ⚠️⚠️"
        else:               conclusion = f"O(N^{avg_k:.1f}) — ⚠️⚠️⚠️"
        print(f"\nConclusion: {conclusion}")
        return conclusion
    return "unknown"


# ── Demo: probe several common operations ──────────────────────────

def demo():
    rng = random.Random(42)

    print("=" * 55)
    print("PROBING: Python list.index() — O(N) linear search")
    print("=" * 55)
    probe_complexity(
        fn=lambda arr: arr.index(arr[-1]),  # always finds at end → O(N)
        input_generator=lambda N: list(range(N)),
        sizes=[1_000, 2_000, 4_000, 8_000, 16_000, 32_000],
    )

    print("\n" + "=" * 55)
    print("PROBING: Python dict lookup — O(1) amortized")
    print("=" * 55)
    d = {i: i for i in range(1_000_000)}
    probe_complexity(
        fn=lambda keys: [d.get(k) for k in keys],
        input_generator=lambda N: [rng.randint(0, 999_999) for _ in range(N)],
        sizes=[1_000, 2_000, 4_000, 8_000, 16_000, 32_000],
    )

    print("\n" + "=" * 55)
    print("PROBING: Python sorted() — O(N log N)")
    print("=" * 55)
    probe_complexity(
        fn=lambda arr: sorted(arr),
        input_generator=lambda N: [rng.random() for _ in range(N)],
        sizes=[1_000, 2_000, 4_000, 8_000, 16_000, 32_000],
    )

    print("\n" + "=" * 55)
    print("PROBING: Nested loop — O(N²)")
    print("=" * 55)
    def nested_sum(arr):
        total = 0
        for x in arr:
            for y in arr:
                total += x * y
        return total

    probe_complexity(
        fn=nested_sum,
        input_generator=lambda N: list(range(N)),
        sizes=[100, 200, 400, 800, 1_600],
    )


if __name__ == '__main__':
    demo()
```

---

## 10. Trade-offs

### Time-Space Trade-off Summary Table

| Technique | Time complexity | Space cost | Data Eng Example |
|---|---|---|---|
| Linear scan (no index) | O(N) | O(1) | Full Parquet scan |
| Hash table | O(1) avg lookup | O(N) | Spark hash join build |
| Sorted array + binary search | O(log N) lookup | O(N) | Parquet page index |
| Bloom filter | O(k) ≈ O(1) | O(N × 10 bits) | Parquet row group filter |
| Count-Min Sketch | O(k) ≈ O(1) | O(w × d) fixed | Stream cardinality estimate |
| Broadcast join | O(N+M) | O(M) in every executor | Small-large join in Spark |
| External sort | O(N log N) | O(B) buffer | Spark shuffle sort |

The fundamental principle: **space and time trade in opposite directions.** More memory → better time. Less memory → more computation. Choose based on which resource is constrained in production.

### When O(N²) Is Acceptable

O(N²) is not always wrong. If N is guaranteed small:

```
If N ≤ 10,000 and the constant factor is small:
  N² = 10⁸ operations.
  At 10⁹ ops/second: 0.1 seconds.  ← acceptable

  Bubble sort on 1,000 elements: fast enough.
  Nested loop join on two 1,000-row lookup tables: fast enough.

If N = 1,000,000:
  N² = 10¹² operations.
  At 10⁹ ops/second: 1,000 seconds (17 minutes) for a single operation.
  In a Spark stage: unacceptable.

Rule: if N is bounded by a small constant (e.g., number of columns in a schema,
number of partitions in a small dimension table), O(N²) is fine.
If N is proportional to data volume (rows, bytes), O(N²) is never acceptable.
```

---

## 11. Comparisons

### Sorting Algorithms and Their Complexity Classes

```python
"""
sort_complexity_comparison.py

Compares four sorting approaches with different complexity classes
to demonstrate why complexity class determines scalability.
"""
import time
import random
import math


def insertion_sort(arr: list) -> list:
    """O(N²) — good for nearly-sorted, tiny N."""
    a = arr[:]
    for i in range(1, len(a)):
        key = a[i]
        j = i - 1
        while j >= 0 and a[j] > key:
            a[j + 1] = a[j]
            j -= 1
        a[j + 1] = key
    return a


def merge_sort(arr: list) -> list:
    """O(N log N) — optimal comparison-based sort."""
    if len(arr) <= 1:
        return arr[:]
    mid = len(arr) // 2
    left  = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            result.append(left[i]); i += 1
        else:
            result.append(right[j]); j += 1
    return result + left[i:] + right[j:]


def python_sort(arr: list) -> list:
    """O(N log N) — Python's Timsort (optimized for real data)."""
    return sorted(arr)


def counting_sort(arr: list, max_val: int) -> list:
    """O(N + max_val) — linear when max_val = O(N)."""
    counts = [0] * (max_val + 1)
    for x in arr:
        counts[x] += 1
    return [x for x, c in enumerate(counts) for _ in range(c)]


def benchmark_sorts():
    rng = random.Random(42)
    print("Sorting Algorithm Complexity Comparison")
    print("=" * 65)

    for N in [100, 500, 1_000, 5_000, 10_000]:
        arr = [rng.randint(0, N) for _ in range(N)]
        max_val = max(arr)

        t_ins = t_merge = t_py = t_count = None

        if N <= 2_000:
            t0 = time.perf_counter()
            insertion_sort(arr)
            t_ins = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        for _ in range(3): merge_sort(arr)
        t_merge = (time.perf_counter() - t0) / 3 * 1000

        t0 = time.perf_counter()
        for _ in range(3): python_sort(arr)
        t_py = (time.perf_counter() - t0) / 3 * 1000

        t0 = time.perf_counter()
        for _ in range(3): counting_sort(arr, max_val)
        t_count = (time.perf_counter() - t0) / 3 * 1000

        print(f"\n  N = {N:>6,}")
        if t_ins:
            print(f"    Insertion sort O(N²):      {t_ins:>8.3f} ms")
        print(f"    Merge sort    O(N log N):  {t_merge:>8.3f} ms")
        print(f"    Python sort   O(N log N):  {t_py:>8.3f} ms")
        print(f"    Counting sort O(N+max):    {t_count:>8.3f} ms")

    print("\n  Key observation:")
    print("  Insertion sort vs merge sort ratio grows with N.")
    print("  At N=1K: ~N/log(N) ≈ 100× slower.")
    print("  Python's Timsort beats naive merge sort by constant factor.")
    print("  Counting sort beats all when max_val = O(N) (integer keys).")


if __name__ == '__main__':
    benchmark_sorts()
```

---

## 12. Production Examples

### Spark Operations Mapped to Complexity Classes

```
SPARK OPERATION COMPLEXITY REFERENCE

df.filter(condition)          → O(N)       — linear scan, no shuffle
df.select(cols)               → O(N)       — projection, no shuffle
df.withColumn(expr)           → O(N)       — per-row computation
df.groupBy(key).agg(fn)       → O(N log N) — sort-based shuffle + aggregate
df.sort(col)                  → O(N log N) — external merge sort via shuffle
df.join(other, on=key)        → O(N + M)   — hash join (M fits in memory)
                              → O(N log N) — sort-merge join (both large)
df.join(other, how='cross')   → O(N × M)   — NEVER for large tables
df.distinct()                 → O(N log N) — sort + dedup via shuffle
df.repartition(N)             → O(N)       — full shuffle, no sort
df.coalesce(N)                → O(N/P)     — partial merge, less shuffle
df.count()                    → O(N)       — full scan
df.collect()                  → O(N)       — BUT transmits all data to driver!

df.limit(K)                   → O(K)       — scans until K rows found
                                             (but Spark may still scan all partitions)

Window function (unbounded):  → O(N log N) — sort + running aggregate
Window function (bounded):    → O(N × window_size) if window_size >> 1

Broadcast join (M small):     → O(N + M)   — M broadcast to all executors,
                                             N scanned once per executor

The complexity wall in Spark:
  Any O(N²) operation at N=10M rows = 10¹⁴ operations → always fails.
  Any cross join = O(N²) → never do this on large tables.
  Any UDF that calls df.collect() inside → O(N) per row = O(N²) total.
```

### Python Dict: Amortized O(1) in Practice

```python
"""
dict_amortized_demo.py

Tracks Python dict resize events to demonstrate amortized analysis.
Shows that resize events are rare (O(log N) total) and cost O(current_size),
but amortized over N operations, the average is O(1).
"""
import sys
import time


def track_dict_resizes(n_insertions: int):
    """
    Insert N items into a dict, tracking when resizes occur
    by monitoring sys.getsizeof() changes.
    """
    d = {}
    prev_size = sys.getsizeof(d)
    resizes = []
    resize_costs = []

    for i in range(n_insertions):
        d[i] = i
        new_size = sys.getsizeof(d)
        if new_size != prev_size:
            # A resize occurred
            n_entries = len(d)
            resizes.append((i, n_entries, prev_size, new_size))
            resize_costs.append(n_entries)  # approximate rehash cost
            prev_size = new_size

    total_resize_cost = sum(resize_costs)
    avg_cost = total_resize_cost / n_insertions

    print(f"\nDict insertion analysis: {n_insertions:,} insertions")
    print(f"  Total resize events: {len(resizes)}")
    print(f"  Total rehash cost (elements moved): {total_resize_cost:,}")
    print(f"  Amortized cost per insert: {avg_cost:.2f} element-moves")
    print(f"  → Amortized O(1) confirmed (avg moves per insert ≈ constant)")
    print()
    print(f"  {'Insert #':>10}  {'Dict size':>10}  {'Mem before':>12}  {'Mem after':>11}")
    for insert_n, entries, mem_before, mem_after in resizes[:15]:
        print(f"  {insert_n:>10,}  {entries:>10,}  {mem_before:>12,}  {mem_after:>11,}")
    if len(resizes) > 15:
        print(f"  ... ({len(resizes) - 15} more resizes)")


def measure_insert_latency_distribution(n: int = 100_000):
    """
    Measure per-insert latency to show latency spikes at resize events.
    """
    import statistics

    d = {}
    latencies = []

    for i in range(n):
        t0 = time.perf_counter_ns()
        d[i] = i
        latencies.append(time.perf_counter_ns() - t0)

    # Find spike inserts (> 10× median)
    median = statistics.median(latencies)
    spikes = [(i, lat) for i, lat in enumerate(latencies) if lat > median * 10]

    print(f"\n  Latency distribution ({n:,} inserts):")
    print(f"    Median:  {median:.0f} ns")
    print(f"    P99:     {sorted(latencies)[int(n*0.99)]:.0f} ns")
    print(f"    Max:     {max(latencies):.0f} ns")
    print(f"    Spikes (>10× median): {len(spikes)} events")
    if spikes[:5]:
        print(f"    First few spikes: {[(i, f'{lat}ns') for i, lat in spikes[:5]]}")
    print(f"\n  → The latency spikes = resize events. Amortized O(1), but")
    print(f"    individual inserts at resize points spike to O(N) latency.")
    print(f"    Pre-sizing dicts avoids these spikes in latency-sensitive code.")


if __name__ == '__main__':
    track_dict_resizes(100_000)
    measure_insert_latency_distribution(100_000)
```

---

### BigQuery Slot Complexity

BigQuery abstracts hardware into "slots" (units of compute), but the SQL operations still have their complexity classes:

```
BigQuery operation complexity:

SELECT col FROM table WHERE condition
  → O(N) scan of columnar storage  [N = bytes in column pages]
  → With partition pruning: O(N/P) where P = partitions eliminated

SELECT COUNT(DISTINCT key) FROM table
  → O(N) scan + O(N) HyperLogLog sketch aggregation
  → NOT O(N log N): BigQuery uses probabilistic cardinality estimation

SELECT a.*, b.* FROM a JOIN b ON a.id = b.id
  WHERE a.date = '2024-01-01'
  → With predicate pushdown: O(N_filtered) for a, O(M) for b
  → Hash join: O(N_filtered + M)
  → Sort-merge join: O(N log N + M log M)
  BigQuery chooses based on statistics.

GROUP BY key, ORDER BY count DESC LIMIT 10
  → GROUP BY: O(N) hash aggregate  [hash aggregate avoids sort for high-card]
  → TOP-K extraction: O(groups × log 10) ≈ O(groups)
  → Total: O(N)  [not O(N log N) — the ORDER BY with LIMIT avoids full sort]

Full ORDER BY (no LIMIT):
  → O(N log N) — distributed sort across slots
```

---

## 13. Code

### `complexity_analyzer.py` — Static Code Complexity Estimation

```python
"""
complexity_analyzer.py

Analyzes Python code structure to estimate algorithmic complexity.
Examines loop nesting depth and recursion patterns.

Note: this is heuristic — correct analysis requires understanding
semantics, not just structure. Use as a first-pass tool.

Run: python3 complexity_analyzer.py
"""
import ast
import textwrap
from typing import Union


def estimate_loop_complexity(node: ast.AST, depth: int = 0) -> str:
    """
    Recursively estimate complexity from AST loop structure.
    Returns a complexity string like 'O(N)', 'O(N²)', etc.
    """
    loop_types = (ast.For, ast.While)

    if not isinstance(node, ast.AST):
        return 'O(1)'

    # Count nested loop depth in this subtree
    def max_nesting(n, current_depth=0) -> int:
        if isinstance(n, loop_types):
            inner = max((max_nesting(child, current_depth + 1)
                         for child in ast.walk(n)
                         if child is not n and isinstance(child, loop_types)),
                        default=current_depth)
            return max(current_depth + 1, inner)
        results = [max_nesting(child, current_depth)
                   for child in ast.iter_child_nodes(n)]
        return max(results, default=0)

    nesting = max_nesting(node)

    if nesting == 0:
        return 'O(1)'
    elif nesting == 1:
        # Check if the loop body contains a sort/bisect call (→ O(N log N))
        source = ast.dump(node)
        if any(kw in source for kw in ['sort', 'sorted', 'bisect', 'heappush']):
            return 'O(N log N) [loop with sort]'
        return 'O(N)'
    elif nesting == 2:
        return 'O(N²)'
    elif nesting == 3:
        return 'O(N³)'
    else:
        return f'O(N^{nesting})'


def analyze_function(source: str) -> None:
    """Parse and analyze complexity of all functions in source."""
    tree = ast.parse(textwrap.dedent(source))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            complexity = estimate_loop_complexity(node)
            print(f"  {node.name}(): estimated {complexity}")

            # Check for recursive calls (suggests log or worse)
            calls = [n.func.id for n in ast.walk(node)
                     if isinstance(n, ast.Call)
                     and isinstance(n.func, ast.Name)]
            if node.name in calls:
                print(f"    ↳ Recursive — actual complexity depends on recurrence")


def demo():
    examples = {
        "O(1) dict lookup": """
            def get_user(user_id, cache):
                return cache.get(user_id)
        """,
        "O(N) linear scan": """
            def find_first(arr, target):
                for i, x in enumerate(arr):
                    if x == target:
                        return i
                return -1
        """,
        "O(N log N) sort": """
            def sort_and_dedup(arr):
                return sorted(set(arr))
        """,
        "O(N²) nested loop": """
            def pairwise_distances(points):
                dists = []
                for a in points:
                    for b in points:
                        dists.append(abs(a - b))
                return dists
        """,
        "Recursive (merge sort)": """
            def merge_sort(arr):
                if len(arr) <= 1:
                    return arr
                mid = len(arr) // 2
                left = merge_sort(arr[:mid])
                right = merge_sort(arr[mid:])
                return merge(left, right)
        """,
    }

    print("Static Complexity Analysis (heuristic):")
    print("=" * 55)
    for label, source in examples.items():
        print(f"\n  Example: {label}")
        analyze_function(source)


if __name__ == '__main__':
    demo()
```

---

## 14. Labs

### Lab 1 — Empirically Verify Complexity Classes

```python
"""
lab1_verify_complexity.py

Times five algorithms at N = 100, 200, 400, ..., 51200.
Computes the empirical growth ratio and compares to theoretical.

Expected:
  linear_scan:   ratio ≈ 2.0  (O(N))
  binary_search: ratio ≈ 1.07 (O(log N))
  timsort:       ratio ≈ 2.1  (O(N log N))
  nested_loop:   ratio ≈ 4.0  (O(N²))

Run: python3 lab1_verify_complexity.py
"""
import time, random, math


def linear_scan(arr, target):
    for x in arr:
        if x == target: return True
    return False


def binary_search(arr, target):
    lo, hi = 0, len(arr) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if arr[mid] == target: return True
        elif arr[mid] < target: lo = mid + 1
        else: hi = mid - 1
    return False


def timsort(arr):
    return sorted(arr)


def nested_loop(arr):
    count = 0
    for x in arr:
        for y in arr:
            if x == y: count += 1
    return count


def time_it(fn, *args, runs=3):
    times = [None] * runs
    for i in range(runs):
        t0 = time.perf_counter()
        fn(*args)
        times[i] = time.perf_counter() - t0
    return min(times)


def lab1():
    rng = random.Random(42)
    sizes = [100, 200, 400, 800, 1600, 3200, 6400]

    algorithms = [
        ("linear_scan   O(N)",
         lambda arr: linear_scan(arr, arr[-1])),
        ("binary_search O(log N)",
         lambda arr: binary_search(arr, arr[len(arr)//2])),
        ("timsort       O(N log N)",
         lambda arr: timsort(arr)),
    ]
    # nested_loop on small sizes only
    nested_sizes = [50, 100, 200, 400, 800]

    print("LAB 1: Empirical Complexity Verification")
    print("=" * 65)

    for name, fn in algorithms:
        print(f"\n  {name}")
        print(f"  {'N':>7}  {'Time (ms)':>11}  {'Ratio':>8}  {'Expected'}")
        print("  " + "-" * 45)

        prev_t = None
        for N in sizes:
            arr = sorted([rng.randint(0, N * 10) for _ in range(N)])
            t = time_it(fn, arr) * 1000
            if prev_t and prev_t > 0:
                ratio = t / prev_t
                # Expected ratio for doubling N:
                if "O(N) " in name:     expected = "~2.0"
                elif "log N" in name:    expected = f"~{1 + math.log2(2*sizes[sizes.index(N)-1]) / (sizes[sizes.index(N)-1]):.2f}"
                elif "N log N" in name:  expected = "~2.1"
                else:                    expected = "—"
                print(f"  {N:>7,}  {t:>11.3f}  {ratio:>8.2f}  {expected}")
            else:
                print(f"  {N:>7,}  {t:>11.3f}  {'—':>8}")
            prev_t = t

    print(f"\n  nested_loop O(N²)")
    print(f"  {'N':>7}  {'Time (ms)':>11}  {'Ratio':>8}  {'Expected'}")
    print("  " + "-" * 45)
    prev_t = None
    for N in nested_sizes:
        arr = [rng.randint(0, N) for _ in range(N)]
        t = time_it(nested_loop, arr) * 1000
        if prev_t and prev_t > 0:
            ratio = t / prev_t
            print(f"  {N:>7,}  {t:>11.3f}  {ratio:>8.2f}  ~4.0")
        else:
            print(f"  {N:>7,}  {t:>11.3f}  {'—':>8}")
        prev_t = t


if __name__ == '__main__':
    lab1()
```

---

### Lab 2 — Space-Time Trade-off: Bloom Filter vs Linear Scan

```python
"""
lab2_bloom_filter_tradeoff.py

Implements a simple bloom filter and demonstrates the space-time
trade-off: O(1) lookup using O(N × 10 bits) space vs O(N) linear scan.

Run: python3 lab2_bloom_filter_tradeoff.py
"""
import math
import hashlib
import time
import random


class BloomFilter:
    """
    Bloom filter: probabilistic set membership.
    False positive possible, false negative impossible.
    """
    def __init__(self, n_elements: int, false_positive_rate: float = 0.01):
        # Optimal bit array size: m = -N × ln(p) / (ln(2))²
        self.m = math.ceil(
            -n_elements * math.log(false_positive_rate) / (math.log(2) ** 2)
        )
        # Optimal hash count: k = (m/N) × ln(2)
        self.k = math.ceil((self.m / n_elements) * math.log(2))
        self.bits = bytearray(math.ceil(self.m / 8))
        self.n = 0

    def _hashes(self, item: str):
        """Generate k hash values for item."""
        h1 = int(hashlib.sha256(item.encode()).hexdigest(), 16) % self.m
        h2 = int(hashlib.md5(item.encode()).hexdigest(), 16) % self.m
        for i in range(self.k):
            yield (h1 + i * h2) % self.m

    def add(self, item: str):
        for h in self._hashes(item):
            self.bits[h // 8] |= (1 << (h % 8))
        self.n += 1

    def __contains__(self, item: str) -> bool:
        """O(k) ≈ O(1) lookup — definitely not in set, or possibly in set."""
        return all(
            self.bits[h // 8] & (1 << (h % 8))
            for h in self._hashes(item)
        )

    @property
    def size_bytes(self) -> int:
        return len(self.bits)


def lab2():
    print("LAB 2: Bloom Filter — Space-Time Trade-off")
    print("=" * 60)

    N = 100_000   # number of known items
    N_QUERIES = 50_000

    rng = random.Random(42)
    known = {f"user:{rng.randint(0, 10**9)}" for _ in range(N)}
    unknown = [f"user:{rng.randint(0, 10**9)}" for _ in range(N_QUERIES)]

    # Build bloom filter
    bf = BloomFilter(n_elements=N, false_positive_rate=0.01)
    for item in known:
        bf.add(item)

    # Build set (exact, for comparison)
    known_set = set(known)

    # Measure lookup time: linear scan vs set vs bloom filter
    known_list = list(known)

    print(f"\n  N known items: {N:,}")
    print(f"  N queries:     {N_QUERIES:,}")

    # 1. Linear scan (O(N) per lookup)
    SCAN_N = 1_000  # only scan a subset — too slow otherwise
    t0 = time.perf_counter()
    for q in unknown[:SCAN_N]:
        _ = q in known_list  # O(N) linear scan
    t_scan = (time.perf_counter() - t0) / SCAN_N * 1e6  # µs per lookup

    # 2. Python set (O(1) amortized hash lookup)
    t0 = time.perf_counter()
    for q in unknown:
        _ = q in known_set
    t_set = (time.perf_counter() - t0) / N_QUERIES * 1e6

    # 3. Bloom filter (O(k) ≈ O(1), with space savings)
    t0 = time.perf_counter()
    for q in unknown:
        _ = q in bf
    t_bloom = (time.perf_counter() - t0) / N_QUERIES * 1e6

    # Measure false positive rate
    fp = sum(1 for q in unknown if q in bf and q not in known_set)
    fp_rate = fp / N_QUERIES * 100

    print(f"\n  {'Method':<30}  {'Lookup (µs)':>12}  {'Space':>15}")
    print("  " + "-" * 62)
    print(f"  {'Linear scan (list)  O(N)':<30}  {t_scan:>12.2f}  {'O(N) pointers':>15}")
    print(f"  {'Python set          O(1)':<30}  {t_set:>12.4f}  {'O(N) × ~50 bytes':>15}")
    print(f"  {'Bloom filter        O(k)':<30}  {t_bloom:>12.4f}  {bf.size_bytes/1024:.1f} KB  ({bf.m//N} bits/elem)")

    print(f"\n  Bloom filter false positive rate: {fp_rate:.2f}% (target: 1.00%)")
    print(f"  Bloom filter hash functions (k): {bf.k}")
    print(f"\n  Space comparison:")
    print(f"    Python set:     {len(known_set) * 50 // 1024:,} KB (estimated)")
    print(f"    Bloom filter:   {bf.size_bytes // 1024:,} KB")
    print(f"    Savings:        {len(known_set) * 50 / bf.size_bytes:.0f}×")
    print(f"\n  This is the bloom filter trade-off:")
    print(f"  Use {bf.size_bytes//1024:,} KB instead of ~{len(known_set)*50//1024:,} KB,")
    print(f"  accept {fp_rate:.1f}% false positives,")
    print(f"  gain O(1) lookup (no false negatives).")
    print(f"\n  In Parquet: bloom filters per column let the reader skip")
    print(f"  entire row groups for point lookups — O(1) per row group")
    print(f"  vs O(row_group_size) scan. Used in Spark 3.x+.")


if __name__ == '__main__':
    lab2()
```

---

### Lab 3 — Identify and Fix a Complexity Bug in a Spark-Like Pipeline

```python
"""
lab3_complexity_bug.py

Simulates two versions of a Spark-like pipeline:
  Version A: O(N²) — a hidden nested loop join
  Version B: O(N)  — hash join (the correct approach)

Measures both at increasing N to show the complexity wall.

Run: python3 lab3_complexity_bug.py
"""
import time
import random


def pipeline_quadratic(events: list, lookup: list) -> list:
    """
    O(N × M) — nested loop join (the BUG).
    For each event, scan the entire lookup table.
    Equivalent to a Spark cross join or a UDF with collect().
    """
    result = []
    for event in events:
        # Bug: scanning lookup list for every event
        for entry in lookup:
            if entry['id'] == event['user_id']:
                result.append({**event, 'name': entry['name']})
                break
    return result


def pipeline_linear(events: list, lookup: list) -> list:
    """
    O(N + M) — hash join (the FIX).
    Build a hash table from lookup once, probe for each event.
    """
    # Build phase: O(M)
    lookup_map = {entry['id']: entry['name'] for entry in lookup}
    # Probe phase: O(N)
    return [
        {**event, 'name': lookup_map[event['user_id']]}
        for event in events
        if event['user_id'] in lookup_map
    ]


def lab3():
    print("LAB 3: Complexity Bug — O(N²) Nested Loop vs O(N) Hash Join")
    print("=" * 65)

    rng = random.Random(42)
    M = 1_000   # lookup table size (fixed)

    print(f"\n  M (lookup size) = {M:,} (fixed)")
    print(f"  Varying N (events):\n")
    print(f"  {'N events':>10}  {'O(N²) (ms)':>13}  {'O(N) (ms)':>12}  "
          f"{'Ratio':>8}  {'O(N²) pred.':>13}")
    print("  " + "-" * 65)

    lookup = [{'id': i, 'name': f'user_{i}'} for i in range(M)]
    prev_quad = None

    for N in [100, 500, 1_000, 2_000, 5_000, 10_000]:
        events = [{'event_id': i,
                   'user_id': rng.randint(0, M - 1),
                   'value': rng.random()}
                  for i in range(N)]

        # Time quadratic (limit to avoid multi-minute waits)
        t0 = time.perf_counter()
        r1 = pipeline_quadratic(events, lookup)
        t_quad = (time.perf_counter() - t0) * 1000

        # Time linear
        t0 = time.perf_counter()
        r2 = pipeline_linear(events, lookup)
        t_lin = (time.perf_counter() - t0) * 1000

        ratio = t_quad / t_lin if t_lin > 0 else float('nan')

        # Predict quadratic from previous measurement
        if prev_quad and N > 100:
            n_prev = N // 2
            predicted = prev_quad * (N / n_prev) ** 2  # O(N²) prediction
            pred_str = f"{predicted:>13.2f}"
        else:
            pred_str = f"{'—':>13}"

        print(f"  {N:>10,}  {t_quad:>13.2f}  {t_lin:>12.3f}  "
              f"{ratio:>8.1f}×  {pred_str}")

        prev_quad = t_quad

    print(f"\n  Observations:")
    print(f"  - O(N²) column: each 2× in N → ~4× time (quadratic)")
    print(f"  - O(N) column:  each 2× in N → ~2× time (linear)")
    print(f"  - 'O(N²) pred.' column verifies the quadratic growth model")
    print(f"\n  In Spark, the O(N²) pattern appears when:")
    print(f"  1. A UDF calls df.collect() on a large dataset inside map()")
    print(f"  2. An unintentional cross join (missing join condition)")
    print(f"  3. A Python loop that scans a list for every row")
    print(f"\n  The fix is always the same: build a hash table once (O(M)),")
    print(f"  then probe it per row (O(1) per row) → O(N + M) total.")


if __name__ == '__main__':
    lab3()
```

---

## 15. Summary

Complexity analysis is the language of scalability. It answers: at 10× data, what happens to runtime? O(N) → 10× slower (more hardware fixes it). O(N log N) → 11.5× slower (hardware still fixes it). O(N²) → 100× slower (no hardware fix — algorithm change required).

**Big-O** gives an upper bound on growth rate, ignoring constants. **Amortized O(1)** means individual operations can be expensive, but averaged over N operations the cost is constant — Python list append and dict insert both work this way, with rare but expensive resize events. **Space-time trade-offs** are everywhere in data engineering: hash joins trade O(M) memory for O(N+M) time instead of O(N×M) time; bloom filters trade ~10 bits/element for O(1) set membership instead of O(N) scan.

**Practical rules:**
- Any O(N²) algorithm on production data volumes is broken by definition. Find and eliminate nested loops over row-count-proportional data.
- Use `complexity_probe.py` to empirically verify complexity when in doubt.
- Amortized costs hide latency spikes. Pre-size Python dicts and lists in latency-sensitive code.
- Bloom filters and hash tables are the two most important space-time trades in data engineering — they appear in Parquet readers, Spark join selection, Kafka log compaction, and Cassandra SSTable lookup.

---

## 16. Interview Q&A

### Q1: What is the difference between O, Ω, and Θ notation, and which do you use in practice?

**Answer:**

The three notations describe different bounds on how a function grows as input size N increases.

Big-O (O) is an asymptotic upper bound. Saying f(N) = O(g(N)) means f grows no faster than g, up to a constant factor. It is used to describe worst-case behavior — the guarantee that an algorithm never does worse than the stated bound. Merge sort is O(N log N) because no input causes it to take longer than c × N log N steps for some constant c and large enough N.

Big-Ω (Omega) is a lower bound. f(N) = Ω(g(N)) means f grows at least as fast as g. It is used to make impossibility claims: any comparison-based sorting algorithm must take Ω(N log N) comparisons in the worst case. No matter how clever the algorithm, there is no sub-N log N comparison sort.

Big-Θ (Theta) is a tight bound — both upper and lower. f(N) = Θ(g(N)) means f grows at exactly rate g (up to constants). Merge sort is Θ(N log N) because it is both O(N log N) (no worse) and Ω(N log N) (no better in the worst case). Θ is the most informative characterization.

In practice, data engineers use O informally to mean Θ — "the hash join is O(N+M)" typically means it's tight at Θ(N+M). The distinction matters when proving algorithm optimality (you need Ω to establish a lower bound) or when comparing algorithms in best vs worst case (quicksort is O(N²) worst case but Θ(N log N) average case, making the distinction critical).

---

### Q2: Explain amortized analysis and give a concrete example relevant to data engineering.

**Answer:**

Amortized analysis distributes the cost of occasional expensive operations across the sequence of operations that surround them, yielding an average cost per operation that is lower and more informative than the worst-case cost of any single operation.

The classic example is Python's list `append`. Most appends take O(1) — the element is written to the next slot in the pre-allocated array. But when the array is full, Python allocates a new array of twice the size and copies all existing elements — an O(N) operation. If you look only at worst-case single-operation cost, append is O(N). But this O(N) resize happens exponentially rarely: resizes occur at sizes 1, 2, 4, 8, 16, ..., N. The total copy work over N appends is 1 + 2 + 4 + ... + N ≈ 2N = O(N). Dividing O(N) total work by N operations: O(1) amortized per append.

This matters in data engineering in two ways. First, pre-sizing: if you know you will insert N items into a dict or list, pre-allocate with `list.reserve(N)` equivalent patterns (`[None] * N` then overwrite, or `dict.fromkeys(keys, 0)`) to avoid resize events and their latency spikes. In streaming applications where tail latency matters, a sudden O(N) dict resize at an unfortunate moment can cause a consumer lag spike.

Second, it explains Parquet write amortization: writing small Parquet files and then compacting them is O(total_rows × n_compaction_passes) amortized over all writes — each row is touched a small constant number of times on average, even though compaction itself is O(file_size). This is the same geometric series argument: as long as compaction size doubles each time, total work is O(N).

---

### Q3: How do you decide which join algorithm Spark should use for a given query?

**Answer:**

The choice between Spark's three main join strategies — broadcast hash join, shuffle hash join, and sort-merge join — is fundamentally a complexity analysis decision based on the sizes of the input tables and the available memory per executor.

Broadcast hash join is used when one table is small enough to fit in each executor's memory. Spark broadcasts the smaller table (let's call it M rows) to every executor. Each executor builds a local hash table from the broadcast: O(M) time and O(M) space. The larger table (N rows) is scanned sequentially, and each row probes the local hash table in O(1). Total: O(N + M × E) where E is the number of executors, but since M is small and the broadcast is the network cost, the effective per-row complexity is O(N). This is the fastest strategy when M fits in memory. Spark's default threshold is 10 MB (`spark.sql.autoBroadcastJoinThreshold`).

Shuffle hash join is used when neither table fits in a broadcast but one fits in a partition's memory after shuffling. Both tables are shuffled by join key (O(N log N) and O(M log M) for the sort portion of the shuffle, or O(N) for a hash shuffle). Within each partition, the smaller side builds a hash table and the larger side probes it. Total per partition: O(partition_N + partition_M). If partition_M fits in executor memory, this works. Total complexity: O(N + M).

Sort-merge join is the fallback for large-large joins. Both sides are sorted by join key (via shuffle sort: O(N log N) and O(M log M)), then merged with a sequential scan: O(N + M). Total: O(N log N + M log M). It requires no in-memory hash table — just sequential I/O — making it robust for very large joins. The downside is the sort cost and the fact that it requires both sides to be sorted, which means a full shuffle sort.

In production, I verify which plan was chosen via `df.join(other, key).explain()` and look for `BroadcastHashJoin`, `ShuffledHashJoin`, or `SortMergeJoin` in the output. If sort-merge join is chosen but the smaller table fits in memory, I raise `autoBroadcastJoinThreshold` or use `broadcast(df_small)` hint explicitly.

---

### Q4: What is the complexity of a Spark groupBy-aggregate, and how does increasing shuffle partitions affect it?

**Answer:**

A Spark groupBy aggregate has two phases with different complexity characteristics.

The map-side partial aggregate (combiner phase) runs within each partition before the shuffle. For each partition of size N/P (N rows, P partitions), it builds a local hash table of partial aggregates: O(N/P) time and O(min(N/P, G/P)) space, where G is the number of distinct groups. This phase is O(N/P) per partition, O(N) total — linear.

The reduce-side aggregate runs after the shuffle. If Spark uses a hash aggregate (when the number of distinct groups is estimated to fit in memory): each reduce partition receives rows for a subset of groups, aggregates them in a hash table, and outputs the final result. Time: O(N/P) per reduce partition, O(N) total. Space: O(G/P) per partition for the group hash table.

If Spark spills to disk (hash table exceeds executor memory), it switches to sort-aggregate: sort the partition by group key, then do a sequential pass. Sort cost: O((N/P) log(N/P)) per partition. Total: O(N log(N/P)).

How increasing shuffle partitions (P) affects this: larger P means smaller partitions, reducing spill risk (each partition's group hash table is smaller: O(G/P)). But larger P increases scheduling overhead (more tasks) and network overhead (more small files in the shuffle). The optimal P balances partition size (~100–200 MB per partition) against the number of executors and cores.

The key complexity insight: groupBy aggregate is O(N) for hash aggregate (no spill), O(N log N) for sort aggregate (with spill or explicit sort), and the transition between these is determined by whether G × (bytes per group entry) fits in the reduce partition's memory budget. Tuning `spark.sql.shuffle.partitions` is fundamentally a way to control the space per partition and thus whether hash aggregate stays in O(N) territory.

---

### Q5: A Parquet table has 1 trillion rows. How does a `SELECT col WHERE key = 'X'` query achieve sub-second response?

**Answer:**

A naive O(N) scan of 1 trillion rows at even 1 billion rows per second would take 1,000 seconds. Sub-second response requires reducing the effective N through a sequence of O(log N) and O(1) operations before any row-level work.

First: partition pruning. If the table is partitioned by date, and the query has a date predicate, the planner identifies which Hive or Iceberg partitions match and reads only those directory listings. Complexity: O(P) where P = number of partitions, typically << N.

Second: row group pruning. Each Parquet file contains row groups of ~128K rows. Each row group stores min/max statistics per column. If `key` is sorted or clustered, the planner checks each row group's min/max for `key`. Complexity: O(N/128K) = O(N × 10⁻⁵) — orders of magnitude less than N.

Third: bloom filter lookup. Modern Parquet writers (Spark 3.x, Arrow, Iceberg) write per-column bloom filters per row group. A bloom filter lookup for `key = 'X'` is O(k) ≈ O(1). This eliminates row groups where X is definitely absent with zero false negatives. Complexity: O(surviving_row_groups) after the bloom filter pass.

Fourth: page-level index. Within a surviving row group, the column page index gives byte offsets of pages within the sorted key range. Binary search: O(log(pages_per_row_group)) ≈ O(1) for typical row group sizes.

Fifth: actual data read. Only the pages containing `key = 'X'` are read and decoded. For a high-cardinality key in a 128K-row group, this might be 1–10 pages of 8 KB each.

Total complexity: O(log N) for the metadata traversal, O(1) for the bloom filter, O(1) for the data read. The sub-second response is achievable because each layer reduces the problem by orders of magnitude before handing off to the next — this is why table organization (partitioning, sorting, bloom filters) is a complexity optimization, not just an I/O optimization.

---

### Q6: Why is external merge sort O(N log N) even when data doesn't fit in memory?

**Answer:**

External merge sort adapts the in-memory merge sort algorithm to work when N exceeds available memory by using disk as a secondary storage tier, but the fundamental recurrence relation remains the same: T(N) = 2T(N/2) + O(N), which the Master theorem solves to O(N log N).

The algorithm runs in two phases. The run-creation phase divides N rows into chunks that fit in memory (say, M rows per chunk = N/M chunks). Each chunk is loaded, sorted in memory (O(M log M)), and written to disk as a sorted run. Total run-creation cost: (N/M) chunks × O(M log M) per chunk = O(N log M). Since M is a constant relative to N (it's the memory size, which does not grow with N), this is O(N log M) = O(N) up to constant factors.

The merge phase does a K-way merge of the N/M sorted runs. A K-way merge using a min-heap processes each of the N elements once: O(N log K). If K = N/M (merging all runs at once), this is O(N log(N/M)) = O(N log N - N log M) = O(N log N).

Total: O(N log M) + O(N log N) = O(N log N).

For Spark's shuffle sort specifically: N rows are distributed across P partitions. Each partition sorts its N/P rows locally (O((N/P) log(N/P))). The shuffle then merges sorted runs from all map partitions into reduce partitions. The full sort is O(N log(N/P)) per reducer, O(N log N) total across all reducers. Adding more executors (larger P) reduces the per-partition sort to O((N/P) log(N/P)) while the merge grows more complex — but total work stays O(N log N). Parallelism reduces wall-clock time by P, but the total compute work is unchanged. This is why adding more Spark executors to a sort-limited job reduces clock time proportionally (more parallel workers doing the same O(N log N) total work), but cannot change the algorithmic complexity.

---

## 17. Cross-Question Chain

**Topic:** From "what is Big-O?" to "why did our Spark job fail at 10 TB but not at 1 TB?"

---

**Interviewer:** What is Big-O notation?

**Candidate:** Big-O describes how the resource cost of an algorithm grows as input size N increases, ignoring constant factors. f(N) = O(g(N)) means f grows no faster than g for large N. The constant factors depend on hardware and implementation — Big-O abstracts them away to focus on the growth rate. O(N) means doubling the data doubles the cost. O(N²) means doubling the data quadruples the cost. This is the vocabulary we use to predict scalability.

---

**Interviewer:** Why does the constant factor not matter?

**Candidate:** For large N, the growth rate dominates. An O(N²) algorithm with a tiny constant eventually overtakes an O(N) algorithm with a large constant. For a 2× faster machine running an O(N²) algorithm: at 10× data, the fast machine takes 100×/2 = 50× longer. You need 50× faster hardware to compensate — you can't buy that. For an O(N) algorithm at 10× data: just add 10× hardware. Constants matter at small N, but at the data volumes we work with (billions of rows), the complexity class almost always dominates.

---

**Interviewer:** A Spark job processed 1 TB in 2 hours and failed after 6 hours at 10 TB. What happened?

**Candidate:** The failure pattern — much worse than 10× slowdown — suggests an O(N²) or worse component in the pipeline. At 10× data, O(N) would give 10× runtime (20 hours, not a failure). O(N log N) would give ~11.5× (23 hours). O(N²) gives 100× — from 2 hours to 200 hours, causing timeout. I'd look for: an unintentional cross join in the plan (`df.explain()` showing `CartesianProduct`), a UDF that calls `collect()` on a growing dataset inside a `map`, a window function without `partitionBy` causing a global sort, or a Python loop inside a UDF that iterates over a list whose size grows with N.

---

**Interviewer:** How do you find it?

**Candidate:** First, `df.explain(mode="extended")` on the failing query — look for `CartesianProduct`, or any plan node where estimated output rows is >> input rows. Second, Spark UI → Stages tab: find the stage that's taking all the time. Check "Input Size" vs "Output Size" — if output >> input on a non-join stage, something is exploding the row count. Third, `complexity_probe.py` on a sample: time the job at N=1M, 2M, 4M rows and compute the ratio. If the ratio is ~4 at each doubling, it's O(N²).

---

**Interviewer:** How do you fix an O(N²) join?

**Candidate:** Replace the nested loop with a hash join. Build a hash table from the smaller table once — O(M) time and space. Probe it for each row of the larger table — O(1) per row. Total: O(N + M) instead of O(N × M). In Spark: ensure the join has a join condition (`on=key`), not a cross join. If the smaller table is under the broadcast threshold, use a broadcast hint to force a broadcast hash join — O(N + M) with M in memory. If both tables are large, use sort-merge join — O(N log N + M log M) but still manageable at scale. The key is that both alternatives have complexity exponent ≤ 1 when the input is large, meaning horizontal scaling with more executors proportionally reduces wall-clock time.

---

## 18. What's Next

**CSF-ALG-101 M02: Hash Tables** — The most important data structure in data engineering. M02 covers how hash tables work at the algorithmic level (collision resolution with open addressing vs chaining, load factors, why Python dicts use open addressing with 2/3 load factor), then connects to data engineering: why Spark uses hash joins by default for small-medium tables, how hash aggregation works in DuckDB and Spark, and why hash tables are O(1) average but O(N) worst case in theory (and why that theoretical worst case almost never triggers in practice). M02 also covers consistent hashing — the algorithm behind Kafka partition assignment and distributed cache sharding.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What does O(g(N)) mean formally? | There exist constants c > 0 and N₀ such that f(N) ≤ c × g(N) for all N ≥ N₀. Informally: f grows no faster than g, ignoring constant factors. Upper bound on growth rate. |
| 2 | What is the difference between O and Θ? | O is an upper bound (f grows no faster than g). Θ is a tight bound (f grows at exactly rate g — both upper and lower bounded). "Merge sort is O(N log N)" is technically correct but imprecise; "Θ(N log N)" is the tight statement. |
| 3 | At 10× data, how much slower is an O(N²) algorithm? | 100× slower. O(N²) at 10N: (10N)² = 100N². The complexity wall: doubling N quadruples cost, 10× N gives 100× cost. No amount of hardware fixes this — algorithm change required. |
| 4 | What is amortized analysis? | The average cost per operation over a sequence of N operations, accounting for the fact that rare expensive operations are "paid for" by the savings from many cheap operations. Python list.append is O(1) amortized despite occasional O(N) resize events. |
| 5 | Why is Python list.append O(1) amortized? | When the array is full, Python allocates 2× capacity and copies all elements (O(N)). But resizes occur at sizes 1, 2, 4, 8, ..., N — geometric series totaling O(2N) = O(N) copy work for N appends. Dividing by N operations: O(1) amortized per append. |
| 6 | What is the space-time trade-off? | Using more memory to reduce computation time (or vice versa). Example: hash join uses O(M) memory for a hash table to reduce join time from O(N×M) to O(N+M). Bloom filter uses O(N × 10 bits) to reduce lookup from O(N) scan to O(1). |
| 7 | What is the complexity of Spark's sort-merge join? | O(N log N + M log M) — both sides must be sorted (shuffle sort), then merged in O(N + M). The sort dominates. Better than O(N×M) nested loop, but worse than O(N+M) hash join for the same reason sort is worse than hash lookup. |
| 8 | What is the complexity of a Spark broadcast join? | O(N + M) total — O(M) to build hash table from the broadcast side, O(N) to probe it for each row of the large side. Fastest join strategy when the smaller table (M rows) fits in executor memory. |
| 9 | What is a bloom filter's time and space complexity? | Time: O(k) ≈ O(1) per lookup (k = number of hash functions, typically 7–10). Space: O(N × 10 bits) ≈ 1.25 bytes per element for 1% false positive rate. No false negatives. Trades small probability of false positives for O(1) membership test. |
| 10 | Why can O(1) operations still cause latency spikes? | Amortized O(1) hides occasional O(N) operations (dict resize, GC collection, Kafka log compaction). Individual spikes exist even when the average is O(1). Pre-sizing data structures eliminates mid-stream resize spikes. |
| 11 | How do you derive the complexity of a recursive algorithm? | Write the recurrence relation: T(N) = aT(N/b) + f(N). Apply the Master theorem: if log_b(a) > d (where f(N) = O(N^d)): T(N) = O(N^log_b(a)). If log_b(a) = d: T(N) = O(N^d × log N). If log_b(a) < d: T(N) = O(N^d). Merge sort: a=2, b=2, d=1 → O(N log N). |
| 12 | What is the complexity of Python dict lookup? | O(1) amortized. Python dict uses open addressing with 2/3 load factor. Individual lookups may degrade to O(N) in the presence of pathological hash collisions, but this is extremely rare with Python's hash function. |
| 13 | What does O(N log N) mean at 10× data? | log(10N) = log(10) + log(N) ≈ log(N) + 3.3. So 10N × log(10N) ≈ 10 × (log(N) + 3.3). Growth ratio ≈ 10 × (1 + 3.3/log(N)). For N=10⁶: log(N)≈20 → ratio ≈ 10 × 1.165 ≈ 11.6×. Slightly more than linear but still hardware-scalable. |
| 14 | Why is an unintentional cross join in Spark catastrophic? | Cross join of N × M rows is O(N × M) — quadratic when N ≈ M. At N=M=10M rows: 10¹⁴ output rows. At 100 bytes/row = 10¹⁶ bytes = 10 petabytes. Impossible to materialize. Detected via `df.explain()` showing `CartesianProduct`. |
| 15 | How does Parquet achieve O(log N) lookup for point queries? | Layered metadata: partition pruning (O(P_partitions)), row group statistics min/max check (O(N/128K)), bloom filter lookup (O(1)), page index binary search (O(log pages_per_group)). Each layer reduces effective N by orders of magnitude before any row-level scan. |
| 16 | What is the empirical test for O(N²) vs O(N)? | Double N repeatedly and measure the time ratio. O(N): ratio ≈ 2 at each doubling. O(N log N): ratio ≈ 2.1. O(N²): ratio ≈ 4. If measured ratio is ~4, the algorithm is quadratic. This can be done without knowing the code — just time the black box at N, 2N, 4N. |
| 17 | When is O(N²) acceptable? | When N is bounded by a small constant (e.g., number of columns, number of partitions in a small lookup table, N ≤ 10,000 with a fast constant). If N is proportional to data volume (rows in a production table), O(N²) is never acceptable. |
| 18 | What is the complexity of external merge sort? | O(N log N) — same as in-memory merge sort. Run creation: O(N log M) where M = memory size (constant). Merge phase: O(N log(N/M)) = O(N log N). Total: O(N log N). Adding more executors in Spark reduces wall-clock time proportionally but does not change the total algorithmic complexity. |
| 19 | What complexity class does adding more Spark executors help? | O(N) and O(N log N) — these parallelize well. With E executors, wall-clock time ≈ O(N/E) and O((N/E) log(N/E)). O(N²) does NOT benefit: O((N/E) × N) — the join between all N rows on one side and all N rows on the other cannot be avoided by partitioning. |
| 20 | How does a Count-Min Sketch trade space for time in streaming? | A fixed-size 2D array of counters (w × d cells). Increment/query: O(d) ≈ O(1) (d is constant, typically 5–10 hash functions). Space: O(w × d) independent of N — bounded regardless of stream size. Trade-off: overestimates frequencies but never underestimates. Used for top-K and heavy hitter detection in Flink and Spark Streaming. |

---

## 20. References

**Foundational**

- Cormen, T. H., Leiserson, C. E., Rivest, R. L., & Stein, C. (2022). *Introduction to Algorithms* (4th ed.). MIT Press. Chapters 3 (Growth of functions) and 17 (Amortized analysis). — The definitive reference. Chapter 17's aggregate, accounting, and potential methods for amortized analysis are essential.
- Skiena, S. S. (2020). *The Algorithm Design Manual* (3rd ed.). Springer. Chapter 2 (Algorithm analysis). — More accessible than CLRS; excellent worked examples of empirical complexity measurement.

**Data Engineering Applications**

- Zaharia, M., et al. (2016). Apache Spark: A Unified Engine for Big Data Processing. *CACM*, 59(11). https://dl.acm.org/doi/10.1145/2934664 — Section 3 discusses the complexity of core Spark operations (map, reduce, join, sort).
- Vohra, D. (2016). Apache Parquet: columnar storage for Hadoop. *Packt*. Chapter 4 (Parquet internals). — Explains how Parquet row group statistics, page indexes, and bloom filters achieve O(log N) point query performance.
- Apache Spark documentation: Performance Tuning. https://spark.apache.org/docs/latest/sql-performance-tuning.html — `autoBroadcastJoinThreshold`, `adaptiveQueryExecution`, join strategy hints.

**Probabilistic Data Structures**

- Bloom, B. H. (1970). Space/time trade-offs in hash coding with allowable errors. *CACM*, 13(7), 422–426. — The original bloom filter paper.
- Cormode, G., & Muthukrishnan, S. (2005). An improved data stream summary: the count-min sketch and its applications. *Journal of Algorithms*, 55(1), 58–75. — Count-Min Sketch for streaming frequency estimation.
