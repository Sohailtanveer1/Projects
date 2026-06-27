# CSF-ALG-101 M04: Sorting Algorithms

**Course:** CSF-ALG-101 — Algorithms and Data Structures for Data Engineers  
**Module:** 04 of 05  
**Filesystem position:** 00_CS_Foundations/M15_Sorting_Algorithms  
**Prerequisites:** CSF-ALG-101 M01 (Complexity Analysis), M03 (Trees)

---

## Table of Contents

1. [The Problem Sorting Solves](#1-the-problem-sorting-solves)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: Comparison Sorts and the Lower Bound](#4-core-theory-comparison-sorts-and-the-lower-bound)
5. [Visual: Merge Sort and Quicksort](#5-visual-merge-sort-and-quicksort)
6. [Deep Dive: Merge Sort, Quicksort, Timsort](#6-deep-dive-merge-sort-quicksort-timsort)
7. [Mental Models](#7-mental-models)
8. [External Merge Sort: Sorting Beyond Memory](#8-external-merge-sort-sorting-beyond-memory)
9. [Data Engineering Connections](#9-data-engineering-connections)
10. [Spark Shuffle: External Sort in a Distributed System](#10-spark-shuffle-external-sort-in-a-distributed-system)
11. [Sort-Based vs Hash-Based Operations](#11-sort-based-vs-hash-based-operations)
12. [Code Toolkit](#12-code-toolkit)
13. [Hands-On Labs](#13-hands-on-labs)
14. [Interview Q&A](#14-interview-qa)
15. [Cross-Question Chain](#15-cross-question-chain)
16. [Common Misconceptions](#16-common-misconceptions)
17. [Performance Reference Card](#17-performance-reference-card)
18. [Connections to Other Modules](#18-connections-to-other-modules)
19. [Flashcards](#19-flashcards)
20. [Further Reading](#20-further-reading)
21. [Module Summary](#21-module-summary)

---

## 1. The Problem Sorting Solves

A data engineer joins two billion-row tables. A Spark job groups a trillion events by user. A Kafka consumer needs to process events in event-time order across 100 partitions. A dbt model builds a slowly changing dimension that needs historical rows ordered by effective date. All of these require sorting.

Sorting is not just a coding interview exercise. It is the mechanism behind:

- **Spark shuffle**: every GROUP BY, JOIN, DISTINCT, and repartition operation that does not fit in memory goes through an external sort
- **Sort-merge join**: the algorithm Spark falls back to when neither table fits in a hash table
- **Parquet row group ordering**: writing Parquet files sorted by a column enables min/max statistics to be tight, which makes predicate pushdown effective (from the previous module)
- **B-tree index builds**: `CREATE INDEX` in Postgres sorts the table rows by the index key before building the B-tree — it is faster to sort once and insert in order than to insert unsorted rows into the tree
- **ORDER BY in SQL**: the final sort of query results before returning to the client

Understanding sorting means understanding why Spark shuffle is expensive, why sort-merge join has predictable performance while hash join can OOM, and why the order in which you write Parquet files affects query performance for months afterward.

---

## 2. How to Use This Module

**For interview prep:** Sections 4, 6, 8, 14, 15. Section 14 has six staff-level Q&As including "explain Spark shuffle from first principles." Section 15 escalates from merge sort basics to designing a distributed external sort.

**For production engineering:** Sections 8, 9, 10, 11. These connect algorithm theory directly to Spark configuration, Parquet write strategy, and when to choose sort-merge vs hash join.

**For fundamental understanding:** Read in order. The progression is: why O(N log N) is a hard lower bound → how merge sort achieves it → how quicksort and timsort work in practice → what happens when data doesn't fit in memory → how Spark shuffle implements distributed external sort.

---

## 3. Prerequisites Check

- **Big-O notation and recurrences** (M01): merge sort's complexity comes from the recurrence T(N) = 2T(N/2) + O(N), which resolves to O(N log N) via the Master Theorem. You must be comfortable with this.
- **Memory hierarchy** (CSF-ARC-102 M01): external sort exists to work around the fact that DRAM is finite and disk is slow. The latency numbers (70 ns DRAM, 100 μs NVMe) motivate every design decision in external merge sort.
- **Trees** (M03): the k-way merge in external sort uses a min-heap to efficiently find the minimum across k sorted runs — the heap from M03 is the core data structure.

---

## 4. Core Theory: Comparison Sorts and the Lower Bound

### 4.1 The Sorting Problem

Given N elements with a defined comparison relation (can tell whether a < b), produce a permutation of the elements such that each element is ≤ the next.

The only operation allowed is pairwise comparison. This restriction defines the **comparison sort** model. Every sorting algorithm covered in this module — merge sort, quicksort, heapsort, timsort — is a comparison sort.

### 4.2 The O(N log N) Lower Bound

**Claim:** Any comparison sort algorithm requires at least Ω(N log N) comparisons in the worst case.

**Proof via decision tree:**

Every comparison sort can be modeled as a binary decision tree. Each internal node represents a comparison (is element i < element j?). Each leaf represents one permutation of the input. Since there are N! possible permutations, the decision tree has at least N! leaves.

A binary tree with N! leaves has height at least log₂(N!). By Stirling's approximation:

```
log₂(N!) = log₂(1 × 2 × 3 × ... × N)
          = Σ log₂(i) for i = 1 to N
          ≥ Σ log₂(N/2) for i = N/2 to N   (taking the upper half)
          = (N/2) × log₂(N/2)
          = (N/2) × (log₂(N) - 1)
          = Ω(N log N)
```

Therefore, any comparison sort must make at least Ω(N log N) comparisons in the worst case. No comparison sort can do better. Merge sort, heapsort, and timsort achieve exactly O(N log N) — they are optimal.

**The implication for data engineering:** If you sort 10 billion events by timestamp, no algorithm can do it with fewer than ~330 billion comparisons. You cannot optimize your way below this bound without changing the problem (e.g., by using a non-comparison sort like radix sort, which has different constraints).

### 4.3 Non-Comparison Sorts

**Counting sort:** O(N + K) where K is the range of values. Works only when values are bounded integers. Sort 1 billion events where event_type is an integer in [0, 9]: O(N + 10) = O(N). But K must be small — sorting by timestamp (64-bit integer) with K = 2⁶⁴ is not feasible.

**Radix sort:** Sort by one digit at a time, from least significant to most significant. Time: O(d × N) where d is the number of digits. For 64-bit integers, d = 64/8 = 8 (with 256 buckets per pass). Effectively O(N) for fixed-width integer keys. Used internally by some Spark executors for integer partition keys.

**Bucket sort:** Distribute elements into K buckets by range, sort each bucket independently. O(N + K) expected for uniform distributions. Fails for skewed distributions (all elements in one bucket).

For data engineering at scale, non-comparison sorts are rarely used directly — but Spark's radix sort is used for integer partition IDs during shuffle to avoid the comparison overhead on the partition routing step.

### 4.4 Stability

A sort is **stable** if equal elements appear in the same relative order in the output as in the input. Stability matters when:

1. Sorting by multiple keys in sequence (sort by date, then sort by user — a stable sort preserves date order among users with the same date)
2. Maintaining row order for equal keys in SQL `ORDER BY` with a tie-breaking requirement
3. Iceberg's MERGE INTO — row ordering within equal keys must be deterministic

| Algorithm | Stable? |
|---|---|
| Merge sort | Yes |
| Timsort | Yes |
| Heapsort | No |
| Quicksort (standard) | No |
| Counting sort | Yes |
| Radix sort | Yes (if stable counting sort per digit) |

Python's `sorted()` and `list.sort()` use Timsort — stable. Java's `Arrays.sort()` for objects uses Timsort — stable. Java's `Arrays.sort()` for primitives uses dual-pivot quicksort — not stable (but primitives don't have identity, so stability is irrelevant).

---

## 5. Visual: Merge Sort and Quicksort

### 5.1 Merge Sort — Divide and Conquer

```
Input: [5, 2, 8, 1, 9, 3, 7, 4]

─── Divide ────────────────────────────────────────────────────
Level 0: [5, 2, 8, 1, 9, 3, 7, 4]          (N=8, 1 subarray)
Level 1: [5, 2, 8, 1]    [9, 3, 7, 4]      (N=4, 2 subarrays)
Level 2: [5,2]  [8,1]    [9,3]  [7,4]      (N=2, 4 subarrays)
Level 3: [5][2] [8][1]   [9][3] [7][4]     (N=1, 8 subarrays — base case)

─── Conquer (merge) ───────────────────────────────────────────
Level 2: [2,5]  [1,8]    [3,9]  [4,7]      (merge pairs)
Level 1: [1,2,5,8]       [3,4,7,9]         (merge halves)
Level 0: [1,2,3,4,5,7,8,9]                 (merge → sorted)

Each level does O(N) total merge work.
There are log₂(N) levels.
Total: O(N log N) comparisons.
```

### 5.2 Merge Step Detail

```
Merge [1,2,5,8] and [3,4,7,9]:

Left ptr: ↑           Right ptr: ↑         Output: []
          [1,2,5,8]             [3,4,7,9]

Compare 1 vs 3: 1 < 3 → take 1
Left ptr:  ↑           Right ptr: ↑         Output: [1]
          [_,2,5,8]             [3,4,7,9]

Compare 2 vs 3: 2 < 3 → take 2
Output: [1,2]

Compare 5 vs 3: 5 > 3 → take 3
Output: [1,2,3]

Compare 5 vs 4: 5 > 4 → take 4
Output: [1,2,3,4]

Compare 5 vs 7: 5 < 7 → take 5
Output: [1,2,3,4,5]

Compare 8 vs 7: 8 > 7 → take 7
Output: [1,2,3,4,5,7]

Left exhausted? No. Right ptr → 9.
Compare 8 vs 9: 8 < 9 → take 8
Output: [1,2,3,4,5,7,8]

Left exhausted. Append remaining right: [9]
Final output: [1,2,3,4,5,7,8,9]

Total comparisons for this merge: 7 (out of 8 possible)
```

### 5.3 Quicksort — Partition and Recurse

```
Input: [5, 2, 8, 1, 9, 3, 7, 4]

Pivot: 4 (last element, Lomuto partition scheme)

Partition pass:
  i = -1 (last index of "smaller" region)
  Compare each element to pivot 4:
    5 > 4: skip
    2 < 4: i=0, swap arr[0] and arr[1] → [2, 5, 8, 1, 9, 3, 7, 4]
    8 > 4: skip
    1 < 4: i=1, swap arr[1] and arr[2] → [2, 1, 8, 5, 9, 3, 7, 4]... wait
    
  After full pass: [2, 1, 3, 5, 9, 8, 7, 4]
  Place pivot: swap arr[i+1] and pivot → [2, 1, 3, 4, 9, 8, 7, 5]
                                          ←smaller→ p ←larger→
  
Pivot 4 is now at its final sorted position (index 3).
Recurse on [2, 1, 3] and [9, 8, 7, 5].

Key property: pivot is placed exactly once, in its final position.
Average depth: O(log N) → O(N log N) total comparisons.
Worst case: pivot always smallest/largest → O(N²) depth.
```

### 5.4 Timsort — Natural Runs

```
Input: [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9]

Step 1: Identify natural runs (already sorted sequences):
  Run 1: [3]       ← needs extension to MIN_RUN (≥32 in CPython)
  Run 2: [1, 4]    ← ascending
  Run 3: [1, 5, 9] ← ascending
  ...

Step 2: Use insertion sort to extend short runs to MIN_RUN (32–64 elements).
  Insertion sort on small arrays: O(N²) but with tiny constant → fast in practice

Step 3: Merge adjacent runs using the merge algorithm.
  Timsort tracks a stack of runs and merges them when size invariants are violated:
  - |run[i-2]| > |run[i-1]| + |run[i]|
  - |run[i-1]| > |run[i]|

Step 4: Result is a fully sorted array.

Why Timsort wins on real data:
  Real-world data has natural sorted subsequences (timestamps, IDs in order, etc.)
  Timsort exploits them — nearly-sorted input: O(N)
  Truly random input: O(N log N)
```

---

## 6. Deep Dive: Merge Sort, Quicksort, Timsort

### 6.1 Merge Sort

**Algorithm:**

```python
def merge_sort(arr):
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge(left, right)

def merge(left, right):
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:   # ≤ not < makes it stable
            result.append(left[i]); i += 1
        else:
            result.append(right[j]); j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result
```

**Recurrence:** T(N) = 2T(N/2) + O(N)

By the Master Theorem (case 2: a=2, b=2, d=1, log_b(a) = log₂(2) = 1 = d):
T(N) = O(N^d × log N) = O(N log N)

**Properties:**
- Stable (≤ in the merge step preserves relative order)
- O(N) extra space (the merge output array)
- Predictable: always exactly O(N log N), no worst case
- Cache behavior: the recursive structure accesses memory in a pattern that is moderately cache-friendly (each merge pass is linear, but the recursion creates many small passes that thrash cache for large N)

**Why merge sort is the foundation of external sort:**  
Merge sort naturally splits into independent sorted runs that can be written to disk and later merged. This property does not hold for quicksort or heapsort — their in-place partitioning does not produce independently sortable chunks.

### 6.2 Quicksort

**Algorithm:**

```python
def quicksort(arr, lo, hi):
    if lo >= hi:
        return
    p = partition(arr, lo, hi)
    quicksort(arr, lo, p - 1)
    quicksort(arr, p + 1, hi)

def partition(arr, lo, hi):
    pivot = arr[hi]  # Lomuto: last element as pivot
    i = lo - 1
    for j in range(lo, hi):
        if arr[j] <= pivot:
            i += 1
            arr[i], arr[j] = arr[j], arr[i]
    arr[i+1], arr[hi] = arr[hi], arr[i+1]
    return i + 1
```

**Complexity:**
- Average case: O(N log N) — pivot lands at the median each time (on average for random input)
- Worst case: O(N²) — pivot always at min or max (sorted or reverse-sorted input)
- Best case: O(N log N)
- Space: O(log N) stack space average (O(N) worst case for call stack)

**In-place:** Quicksort rearranges elements without allocating O(N) extra space. This is its main advantage over merge sort for in-memory sorting when memory is tight.

**Dual-pivot quicksort (Java's Arrays.sort for primitives):**
Uses two pivots per partition step, dividing the array into three parts (< p1, p1 ≤ x ≤ p2, > p2). Better cache behavior and fewer comparisons than single-pivot in practice. Achieves ~5% fewer comparisons than single-pivot on average.

**Why quicksort is not used for external sort:**
Quicksort's partition step mixes elements globally — after one partition, elements from anywhere in the array may have moved. You cannot write half of the array to one disk file and continue sorting the other half independently. Merge sort's divide-and-conquer naturally produces independent sorted subproblems that can be serialized.

### 6.3 Timsort

Timsort (Tim Peters, 2002) is the sort algorithm used by Python and Java's object sort. It is an adaptive merge sort that exploits natural runs in partially-sorted data.

**Key observations that motivated Timsort:**
1. Real-world data is rarely uniformly random. Database query results, log files, and time-series data have natural sorted subsequences.
2. Insertion sort has a tiny constant — for N ≤ 64, it beats merge sort even though it's O(N²).
3. Merging two sorted lists of sizes M and N requires exactly M+N-1 comparisons.

**Algorithm:**

```
1. MIN_RUN = 32 (CPython) or 64 (Java):
   Scan left to right. When a run is found (consecutive ascending or descending elements),
   extend it to MIN_RUN using binary insertion sort.
   
2. Push each run onto a stack.

3. After each push, check the stack invariant:
   |Z| > |Y| + |X|   (Z is third-from-top, Y second, X top)
   |Y| > |X|
   
   If either invariant is violated, merge the two smallest adjacent runs.
   
4. When all runs are found, merge remaining runs until one sorted array.
```

**Why the stack invariant matters:** The invariant ensures that merges happen between runs of similar size. Merging a run of 1 element with a run of 1000 elements repeatedly would be O(N²). The invariant forces a balanced merge tree, maintaining O(N log N) in the worst case.

**Timsort on real data:**

| Input pattern | Comparisons | vs O(N log N) |
|---|---|---|
| Already sorted | O(N) | 1/log N times fewer |
| Reverse sorted | O(N) | 1/log N (detected, reversed) |
| k sorted runs | O(N log k) | log(k)/log(N) times fewer |
| Random | O(N log N) | Baseline |

For a Spark executor sorting a partition that was produced by a previous sorted stage, Timsort exploits the partial order — significantly outperforming a plain merge sort.

### 6.4 Heapsort

Heapsort builds a max-heap from the array (O(N)), then repeatedly extracts the maximum (O(log N) each) to produce a sorted array. Total: O(N log N), in-place, no extra space.

Heapsort is theoretically appealing but rarely used in production due to poor cache behavior: the heap's tree structure accesses memory in a non-sequential pattern, causing frequent cache misses. In benchmarks, heapsort typically runs 2–5× slower than quicksort or timsort despite identical asymptotic complexity.

Heapsort is covered here because the **min-heap** structure it uses is the data structure for the k-way merge in external sort — extracting the minimum from k sorted runs is exactly the heap operation repeated N times.

---

## 7. Mental Models

### 7.1 The Merge Sort Stack of Paper

Imagine you have N sheets of paper, each with one number. Merge sort works like this: split the stack into two halves, hand each half to a helper, have each helper return a sorted stack, then merge the two sorted stacks by repeatedly picking the smaller of the two top sheets.

The key insight: each merge step looks at only the tops of two stacks — the two smallest remaining elements. This is O(1) work per output element, and there are N output elements per level, across log N levels.

The **external sort extension:** instead of stacks of paper, imagine they are files on disk. You can only look at the top of each stack (the first line of each file). You pick the smallest, write it to the output file, advance that stack. This is exactly how k-way merge works for external sort.

### 7.2 The O(N²) Pit: Why Quicksort Needs Randomization

Quicksort on a sorted array: the pivot (last element, Lomuto scheme) is always the largest. The partition puts N-1 elements on the left and 0 on the right. Recursion depth is N. Total comparisons: N + (N-1) + (N-2) + ... + 1 = N(N-1)/2 = O(N²).

Sorted input is the most common case in data engineering: log files with timestamps, tables with auto-increment IDs, already-sorted Parquet files being re-processed.

The fix: **random pivot selection**. Pick the pivot uniformly at random from the array. Now, the probability that the pivot is always the extreme element is (2/N)^N → 0 exponentially. The expected height is O(log N) regardless of input order.

Python's `list.sort()` uses Timsort, not quicksort, specifically to avoid this worst case — sorted input is O(N) for Timsort.

### 7.3 The N log N Wall

You have 1 billion events (N = 10⁹). The lower bound requires N log N = 10⁹ × 30 = 3 × 10¹⁰ comparisons.

At 10⁹ comparisons per second (modern CPU, with SIMD): 30 seconds just for comparisons, ignoring I/O.

At 64 bytes per event (typical Spark row): 64 GB of data. NVMe bandwidth: 7 GB/s. Two passes (read + write per merge level): 30 passes × 64 GB × 2 / 7 = unsustainable.

This is why Spark sort tasks are carefully optimized: Tungsten's sort uses binary compact representation (UnsafeRow), sort on 8-byte key+pointer pairs instead of full rows, and serialized bytes to minimize copies. Each optimization reduces the constant, but nothing beats the N log N lower bound.

---

## 8. External Merge Sort: Sorting Beyond Memory

### 8.1 The Problem

You have 500 GB of data to sort. Your executor has 8 GB of RAM. You cannot load all data into memory for an in-memory sort. External merge sort is the solution.

### 8.2 Phase 1: Sorted Run Generation

```
Data: 500 GB, Memory: 8 GB, Disk: unlimited

Phase 1: Create sorted runs

  Chunk 1: Read 8 GB into memory → sort with quicksort/timsort → write to disk as "run_001.sorted"
  Chunk 2: Read next 8 GB → sort → write "run_002.sorted"
  ...
  Chunk 63: Read last ~4 GB → sort → write "run_063.sorted"

Result: 63 sorted runs on disk, each 8 GB (or less for the last)
```

**Within each run:** The sorting algorithm used is a standard in-memory sort (quicksort or timsort). The runs are perfectly sorted but independent of each other — run_001 and run_002 are each sorted, but element i in run_001 might be larger than element j in run_002.

### 8.3 Phase 2: K-Way Merge

Now you have 63 sorted runs. You need to merge them into one sorted output.

A naïve approach: merge pairs (run_001 + run_002 → merged_01, run_003 + run_004 → merged_02, ...). This requires ⌈log₂(63)⌉ = 6 merge passes, each reading and writing 500 GB. Total I/O: 12 × 500 GB = 6 TB.

The efficient approach is a **single k-way merge**: read from all 63 runs simultaneously, always outputting the smallest element across all run heads.

```
63 sorted runs (files), one read buffer per run:

Run_001 buffer: [3,  7, 15, ...]
Run_002 buffer: [1,  4, 12, ...]
Run_003 buffer: [2,  9, 18, ...]
...
Run_063 buffer: [5, 11, 20, ...]

Min-heap of (first element, run index):
  Heap: [(1, run_002), (2, run_003), (3, run_001), (5, run_063), ...]

Step: Extract minimum from heap (1, run_002) → write 1 to output
      Read next element from run_002 → push (4, run_002) into heap
      Heap: [(2, run_003), (3, run_001), (4, run_002), (5, run_063), ...]

Step: Extract (2, run_003) → write 2 to output
      Read next from run_003 → push (9, run_003) into heap
      ...

Total heap operations: N extractions + N insertions = O(N log k) comparisons
  where k = 63 (number of runs) and N = total elements (500 GB / bytes_per_row)
```

**Complexity of k-way merge:**
- Each of N elements requires one heap extraction (O(log k)) and one heap insertion (O(log k))
- Total comparisons: O(N log k) = O(N log(memory_size / row_size))

**Total external sort complexity:**
```
Phase 1: O(N log N) — sort each in-memory chunk (N/M chunks, each O(M log M) → O(N log M))
Phase 2: O(N log k) — k-way merge (k = N/M runs)

Combined: O(N log N) — same as in-memory sort
```

External merge sort achieves O(N log N) regardless of the memory/data ratio. The constant factor is determined by the number of disk I/O passes.

### 8.4 Number of Passes

```
Single-pass k-way merge (ideal):
  If k = N/M (number of runs), one merge pass suffices.
  Total disk reads: N (Phase 1 reads) + N (Phase 2 reads) = 2N
  Total disk writes: N (Phase 1 writes) + N (Phase 2 writes) = 2N
  Total I/O: 4N (2 reads + 2 writes of the full dataset)

Multi-pass (if k is too large for memory):
  If k > memory_pages (can't have one buffer per run):
  Use a fan-in of F (F runs merged at a time)
  Number of merge passes: ⌈log_F(k)⌉
  Total I/O: 2N × (1 + ⌈log_F(k)⌉)

Example: 500 GB data, 8 GB memory, 64 KB buffers per run, F = 8 GB / 64 KB = 128,000
  k = 63 runs. F = 128,000. Can merge all 63 in one pass.
  Total I/O: 4 × 500 GB = 2 TB
```

### 8.5 Replacement Selection (Double Buffering)

A variant of Phase 1 that produces longer initial runs using a priority queue:

```
Memory: 8 GB (holds M records)
Initialize: Fill heap with M records from input

While input is not exhausted:
  min = heap.extract_min()
  Write min to current run
  Read next record x from input
  If x >= min (i.e., x can extend the current sorted run):
    heap.insert(x)  ← x stays in the current run
  Else:
    x goes to a "next run" buffer → when current run ends, start new run

Expected run length with replacement selection: 2M (twice the buffer size!)
Reduces number of runs by ~2×, reducing merge passes.
```

Spark uses a form of replacement selection in its external sorter — runs are often longer than the buffer size because partially-sorted data allows the current run to absorb more elements before being closed.

---

## 9. Data Engineering Connections

### 9.1 Parquet Write Order and Query Performance

The order in which rows are written to a Parquet file directly affects query performance for months or years afterward. This is because Parquet stores min/max statistics per column per row group. If rows are sorted by the query predicate column, those statistics are tight:

```
Scenario: 1 billion events, each 200 bytes, written to Parquet

Case 1: Random write order (events arrive in random timestamp order)
  Row Group 0 (128 MB = ~640K rows):
    timestamp_min = 2024-01-01 00:00:01
    timestamp_max = 2024-12-31 23:59:59   ← spans the entire year
  Row Group 1: same range
  ...
  Query: WHERE timestamp BETWEEN '2024-06-01' AND '2024-06-30'
  → Cannot skip ANY row group (all have min/max spanning the full year)
  → Must read 100% of data → 200 GB I/O

Case 2: Sorted write order (pre-sort by timestamp before writing)
  Row Group 0 (128 MB):
    timestamp_min = 2024-01-01 00:00:00
    timestamp_max = 2024-01-04 12:30:00   ← tight range, ~3.5 days
  Row Group 10:
    timestamp_min = 2024-06-01 00:00:00
    timestamp_max = 2024-06-04 11:00:00
  ...
  Query: WHERE timestamp BETWEEN '2024-06-01' AND '2024-06-30'
  → Skip ~90% of row groups based on statistics
  → Read only ~20 GB instead of 200 GB → 10× faster
```

**In practice:** Spark's `df.sort("timestamp").write.parquet(path)` triggers an external sort (Spark shuffle) before writing. This costs O(N log N) comparisons and shuffle I/O once, but pays dividends on every query against the table forever.

### 9.2 Postgres B-Tree Index Build Uses External Sort

```
CREATE INDEX events_ts_idx ON events(created_at);

Internal process (simplified):
  1. Scan the heap (table) sequentially: read every row
  2. Extract (created_at, TID) for each row
  3. Sort all (created_at, TID) pairs using external merge sort
     (if N × key_size > work_mem, spills to disk)
  4. Walk the sorted pairs in order, building B-tree pages bottom-up
  5. Link leaf pages as B+ tree siblings

Why sort first rather than insert one by one?
  - Inserting N rows into a B-tree one at a time: each insert requires
    traversing 4 levels → 4 I/Os. Total: 4N random I/Os.
  - Sort first, then bulk-load: reads sorted pages sequentially → ~N/500 leaf writes
    (500 rows per 8 KB page). Sequential writes are ~10× faster on spinning disk.
  - Also: no page splits during bulk-load (sequential inserts never split if pre-sorted)
```

PostgreSQL parameter `maintenance_work_mem` controls how much memory is used for the external sort during `CREATE INDEX`. Default is 64 MB — increasing it to 1–4 GB dramatically speeds up index builds on large tables.

### 9.3 SQL ORDER BY: Sort vs Index Scan

```sql
-- Query 1: ORDER BY with a supporting index
SELECT user_id, created_at, event_type
FROM events
ORDER BY created_at DESC
LIMIT 100;

-- If B-tree index on created_at exists:
--   Index scan in reverse order: read 100 leaf entries → O(log N + 100)
--   NO sort step. Postgres uses the index to produce rows in order.

-- If no index:
--   Sort all N rows by created_at → O(N log N) + O(N) to scan the result
--   For 500M rows: sort may spill to disk if work_mem is insufficient

-- Query 2: ORDER BY with no supporting index
EXPLAIN ANALYZE SELECT * FROM events ORDER BY amount DESC LIMIT 100;
-- → Sort (cost=... rows=... width=...)
--     → Seq Scan on events

-- The "Sort" node uses quicksort for in-memory sorts (work_mem controlled)
-- and external merge sort when result exceeds work_mem.
-- PostgreSQL parameter: SET work_mem = '256MB';  ← per-sort allocation
```

### 9.4 dbt and Sort Keys in Redshift / BigQuery

In **Amazon Redshift**, tables have a sort key — the column(s) by which data is physically sorted on disk. Redshift's architecture is column-oriented, and zone maps (like Parquet statistics) store min/max per block per column. When data is sorted by the sort key, zone maps are tight and queries can skip entire blocks — achieving "index-like" performance without a traditional B-tree.

In **BigQuery**, `CLUSTER BY` achieves the same thing within each partition — data is physically sorted by the clustering columns, enabling column-level block pruning.

The mechanism in both cases is external merge sort at table creation time (or VACUUM SORT in Redshift). The O(N log N) cost is paid once at write time to benefit all subsequent reads.

---

## 10. Spark Shuffle: External Sort in a Distributed System

### 10.1 What Triggers a Shuffle

A Spark shuffle occurs whenever data must be repartitioned — rows currently on one executor need to move to a different executor based on a key:

```python
# These all trigger a shuffle:
df.groupBy("user_id").agg(...)           # GROUP BY
df.join(other, on="user_id")             # JOIN (non-broadcast)
df.orderBy("timestamp")                  # ORDER BY
df.repartition(200, "user_id")           # explicit repartition
df.distinct()                            # DISTINCT
df.coalesce(1)                           # if reducing partitions with data movement
```

### 10.2 Spark's Sort-Based Shuffle (Default Since Spark 1.6)

```
Map Phase (happens on every executor before the shuffle):

  Executor 1 has partition 0 (raw data, unsorted):
  
  Step 1: For each row, compute partitionId = hash(key) % numReducePartitions
  Step 2: Insert (partitionId, row) into an in-memory sort buffer (ExternalSorter)
  Step 3: When buffer is full:
            Sort buffer in memory by (partitionId, sort_key) using Timsort
            Spill to disk: write sorted spill file
            Clear buffer
  Step 4: Merge all spill files (k-way merge by partitionId + sort_key)
          Write final sorted shuffle file + index file
          
  Output: One sorted shuffle file per executor
          Index file: byte offsets for each partitionId's data within the shuffle file

Reduce Phase (fetch + merge):

  Executor receiving partition 5 (all rows with partitionId=5):
  
  Step 1: Fetch partition 5's data from every map executor's shuffle file
          (uses the index file to find the byte offset)
  Step 2: Merge the incoming sorted streams (k-way merge again)
          Each map executor's data for partition 5 is already sorted → true merge
  Step 3: Process the merged, sorted stream (GROUP BY aggregate, JOIN, etc.)
```

### 10.3 Spill Files and Memory Pressure

```
ExternalSorter buffer (Spark Tungsten):

  Layout: Compact binary array of (8-byte pointer + 8-byte sort key) per row
  Why not store full rows? Full rows can be 1-10 KB each.
  Sorting full rows means copying 1 KB per comparison swap.
  Sorting 16-byte records (pointer + key) means copying 16 bytes per swap → 64x less.
  
  When buffer_memory > spark.executor.memory × 0.2 (shuffle fraction):
    Sort the 16-byte records by sort key
    Follow pointers to serialize full rows in sorted order
    Write spill file to spark.local.dir (local SSD/HDD)

Number of spill files:
  Input partition size / buffer size = number of spills
  If a 10 GB partition is sorted with a 500 MB buffer: 20 spill files
  k-way merge: merge all 20 spill files → one sorted output per reducer partition
```

### 10.4 Shuffle Skew: The Sorting Performance Killer

```
Ideal (no skew):
  200 reduce partitions, 1 TB total shuffle data
  Each partition: ~5 GB → sort 5 GB in memory or with few spills → fast

Skew (one hot key):
  hash("bot_user_12345") % 200 = 73
  Partition 73: 200 GB    ← one user with 200 GB of events
  Partitions 0-72, 74-199: ~4 GB each

  Partition 73 task:
    Sort 200 GB with 500 MB buffer: 400 spill files → 400-way merge
    Each row read/written 2× (spill + merge) → 400 GB disk I/O for one task
    All other tasks: done in 5 minutes
    Task 73: still running 4 hours later → holds up the entire stage
```

**AQE Skew Join Fix (Spark 3.x):**
```
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5  # > 5x median is skewed
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB

When partition 73 is detected as skewed:
  Split partition 73 into sub-partitions (e.g., 40 × 5 GB sub-partitions)
  Each sub-partition becomes its own sort task
  Parallelism restored: 40 tasks each sorting 5 GB → fast
```

### 10.5 Shuffle File Management and GC Pressure

Each Spark task writes one shuffle file per reducer partition. With 200 executors × 200 tasks per executor × 200 reduce partitions: 200 × 200 × 200 = 8,000,000 shuffle files per stage.

Opening 8 million files simultaneously on the executors' local filesystems causes:
- inode exhaustion (ext4 default: ~6 million inodes per filesystem)
- File descriptor limits (`ulimit -n` default: 4096 per process)
- Shuffle service memory pressure

Mitigation: `spark.shuffle.file.buffer` controls write buffer size (default 32 KB — increase to 1 MB for NVMe); `spark.shuffle.unsafe.file.output.buffer` controls Tungsten shuffle write buffer.

---

## 11. Sort-Based vs Hash-Based Operations

For each operation that requires grouping or joining, there are two algorithmic approaches. Understanding when each is better is a core Staff-level skill.

### 11.1 GROUP BY: Hash Aggregate vs Sort Aggregate

```
Data: 1 billion events, GROUP BY user_id, COUNT(*)

Hash aggregate:
  Build: hash_table[user_id] → count
  For each row: count = hash_table.get(user_id, 0) + 1
  Output: iterate hash table entries
  
  Time: O(N) expected
  Space: O(distinct_user_ids) — must fit in memory!
  
  If distinct_user_ids = 100M and each entry is 32 bytes: 3.2 GB
  If executor has 4 GB for aggregation: fits! → hash aggregate wins
  
  If distinct_user_ids = 500M × 32 bytes = 16 GB: OOM → spill to disk
  Hash table spill is messy: re-partition on disk, re-aggregate per partition

Sort aggregate:
  Sort all rows by user_id: O(N log N) time, O(1) extra space (streaming)
  Walk sorted stream: when user_id changes, emit (prev_user_id, count)
  
  Time: O(N log N) — slower than hash for in-memory case
  Space: O(1) working memory + external sort spill files
  
  Predictable: never OOM on the aggregate step itself (sort may spill, but controllably)
```

**Spark's choice:**
- Default: hash aggregate (faster when data fits in memory)
- Fallback: sort aggregate when hash table exceeds memory
- Forced: `spark.sql.execution.useObjectHashAggregateExec=false`

**BigQuery:** Always uses a variant of hash aggregate with automatic spill. No user control — the query engine manages it.

### 11.2 JOIN: Hash Join vs Sort-Merge Join

```
Data: events (10B rows), users (100M rows), JOIN ON user_id

Hash join (broadcast variant):
  Build: hash_table[user_id] → user_row  (from users, 100M entries)
  Probe: for each event, hash_table.get(event.user_id) → match
  
  Time: O(N+M), Space: O(M) for hash table
  Constraint: M (users) must fit in executor memory
  
  100M × 200 bytes = 20 GB. If executor has 32 GB: broadcast hash join works.
  If executor has 8 GB: OOM → fall back to sort-merge join

Sort-merge join:
  Sort events by user_id: O(N log N) external sort
  Sort users by user_id: O(M log M) external sort
  Merge sorted streams: O(N+M) — two pointers walk sorted data
  
  Time: O(N log N + M log M), Space: O(1) merge + sort spill
  Predictable: sort spills are well-controlled, merge is streaming
  Works for any size: both tables can be larger than memory

When sort-merge join beats hash join:
  - Both tables are already sorted on the join key (no sort step needed!)
  - Build side doesn't fit in memory
  - Join produces a large output that would overwhelm hash table eviction
  - Many duplicate join keys (hash table grows unboundedly for M-to-N joins)
```

### 11.3 The Sort-Once Principle

If a dataset is sorted once and written back sorted, all subsequent operations on that column are cheaper:

```python
# Sort events once, write sorted Parquet
events_sorted = spark.read.parquet("events/") \
    .sort("user_id", "event_time") \
    .write.parquet("events_sorted/")

# All future GROUP BY user_id operations:
#   Can use sort aggregate (streaming) instead of hash aggregate
#   Parquet statistics are tight → predicate pushdown skips files
#   JOIN on user_id → if user dimension is also sorted, sort-merge join = O(N+M) not O(N log N+M log M)

# This is the "sort once, read many" pattern for data warehouses:
# Redshift sort keys, BigQuery CLUSTER BY, Iceberg sort order spec
```

---

## 12. Code Toolkit

### 12.1 `sorting_algorithms.py` — All Four Algorithms with Timing

```python
"""
sorting_algorithms.py — Merge sort, quicksort, heapsort, timsort with comparison counting.

Run directly to see benchmark comparison on different input distributions:
  - Random
  - Already sorted (Timsort's best case, Quicksort's worst case)
  - Reverse sorted
  - Nearly sorted (10% random swaps)
"""
from __future__ import annotations
import random
import time
import heapq
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class SortStats:
    name: str
    n: int
    comparisons: int
    time_ms: float
    extra_space: int  # approximate, in elements

    def summary(self) -> str:
        n_log_n = self.n * (self.n.bit_length() - 1)  # approx N log₂ N
        ratio = self.comparisons / n_log_n if n_log_n else 0
        return (
            f"{self.name:<20} n={self.n:>8,}  "
            f"comparisons={self.comparisons:>12,}  "
            f"ratio_to_NlogN={ratio:.2f}  "
            f"time={self.time_ms:.2f}ms"
        )


# ─── Merge Sort ─────────────────────────────────────────────────────────────

def merge_sort(arr: list, comparisons: list[int]) -> list:
    """Stable O(N log N) merge sort. comparisons[0] counts total comparisons."""
    if len(arr) <= 1:
        return arr[:]
    mid = len(arr) // 2
    left = merge_sort(arr[:mid], comparisons)
    right = merge_sort(arr[mid:], comparisons)
    return _merge(left, right, comparisons)


def _merge(left: list, right: list, comparisons: list[int]) -> list:
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        comparisons[0] += 1
        if left[i] <= right[j]:
            result.append(left[i]); i += 1
        else:
            result.append(right[j]); j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result


# ─── Quicksort ──────────────────────────────────────────────────────────────

def quicksort(arr: list, comparisons: list[int]) -> list:
    """In-place quicksort with random pivot. Average O(N log N)."""
    arr = arr[:]
    _quicksort(arr, 0, len(arr) - 1, comparisons)
    return arr


def _quicksort(arr: list, lo: int, hi: int, comparisons: list[int]) -> None:
    if lo >= hi:
        return
    pivot_idx = random.randint(lo, hi)
    arr[pivot_idx], arr[hi] = arr[hi], arr[pivot_idx]
    p = _partition(arr, lo, hi, comparisons)
    _quicksort(arr, lo, p - 1, comparisons)
    _quicksort(arr, p + 1, hi, comparisons)


def _partition(arr: list, lo: int, hi: int, comparisons: list[int]) -> int:
    pivot = arr[hi]
    i = lo - 1
    for j in range(lo, hi):
        comparisons[0] += 1
        if arr[j] <= pivot:
            i += 1
            arr[i], arr[j] = arr[j], arr[i]
    arr[i+1], arr[hi] = arr[hi], arr[i+1]
    return i + 1


# ─── Heapsort ───────────────────────────────────────────────────────────────

def heapsort(arr: list, comparisons: list[int]) -> list:
    """In-place heapsort. O(N log N), not stable, poor cache behavior."""
    arr = arr[:]
    n = len(arr)

    def sift_down(root, end):
        while True:
            child = 2 * root + 1
            if child >= end:
                break
            if child + 1 < end:
                comparisons[0] += 1
                if arr[child] < arr[child + 1]:
                    child += 1
            comparisons[0] += 1
            if arr[root] < arr[child]:
                arr[root], arr[child] = arr[child], arr[root]
                root = child
            else:
                break

    # Build max-heap
    for i in range(n // 2 - 1, -1, -1):
        sift_down(i, n)

    # Extract elements
    for end in range(n - 1, 0, -1):
        arr[0], arr[end] = arr[end], arr[0]
        sift_down(0, end)

    return arr


# ─── Timsort (Python built-in, instrumented separately) ─────────────────────

def timsort_instrumented(arr: list, comparisons: list[int]) -> list:
    """
    Python's built-in sort — Timsort.
    We cannot count individual comparisons from Python,
    so we time it and estimate from theory.
    """
    arr = arr[:]
    arr.sort()
    # Estimate: Timsort on sorted data ≈ N comparisons; random ≈ N log N
    comparisons[0] = -1  # sentinel: "not countable from Python"
    return arr


# ─── Benchmark ──────────────────────────────────────────────────────────────

def benchmark(
    name: str,
    sort_fn: Callable,
    data: list,
) -> SortStats:
    comparisons = [0]
    t0 = time.perf_counter()
    result = sort_fn(data[:], comparisons)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    assert result == sorted(data), f"{name} produced incorrect result"
    return SortStats(
        name=name,
        n=len(data),
        comparisons=comparisons[0],
        time_ms=elapsed_ms,
        extra_space=len(data) if "merge" in name.lower() else 0,
    )


if __name__ == "__main__":
    import math
    random.seed(42)
    N = 10_000

    distributions = {
        "Random":        lambda: random.sample(range(N * 10), N),
        "Sorted":        lambda: list(range(N)),
        "Reverse sorted":lambda: list(range(N, 0, -1)),
        "Nearly sorted": lambda: _nearly_sorted(N, swap_pct=0.05),
    }

    def _nearly_sorted(n, swap_pct):
        arr = list(range(n))
        swaps = int(n * swap_pct)
        for _ in range(swaps):
            i, j = random.randint(0, n-1), random.randint(0, n-1)
            arr[i], arr[j] = arr[j], arr[i]
        return arr

    algorithms = [
        ("Merge Sort",   lambda d, c: merge_sort(d, c)),
        ("Quicksort",    lambda d, c: quicksort(d, c)),
        ("Heapsort",     lambda d, c: heapsort(d, c)),
        ("Timsort",      lambda d, c: timsort_instrumented(d, c)),
    ]

    for dist_name, data_fn in distributions.items():
        data = data_fn()
        print(f"\n=== {dist_name} (N={N:,}) ===")
        for alg_name, sort_fn in algorithms:
            stats = benchmark(alg_name, sort_fn, data)
            cmp_str = f"{stats.comparisons:,}" if stats.comparisons >= 0 else "N/A (built-in)"
            print(f"  {alg_name:<20} {stats.time_ms:>8.2f} ms  comparisons: {cmp_str}")

    n_log_n = N * math.log2(N)
    print(f"\nTheoretical N log N for N={N:,}: {n_log_n:,.0f}")
```

### 12.2 `external_merge_sort.py` — File-Based External Sort

```python
"""
external_merge_sort.py — External merge sort for files larger than memory.

Demonstrates:
- Phase 1: Sorted run generation (in-memory sort, spill to temp files)
- Phase 2: K-way merge using a min-heap
- Tracks I/O volume and heap operations

Usage:
  python external_merge_sort.py          # generates a test file and sorts it
  python external_merge_sort.py --demo   # shows the algorithm on small data
"""
from __future__ import annotations
import heapq
import os
import tempfile
import time
import random
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SortMetrics:
    phase1_reads: int = 0    # rows read from input
    phase1_writes: int = 0   # rows written to spill files
    phase2_reads: int = 0    # rows read from spill files
    phase2_writes: int = 0   # rows written to output
    heap_operations: int = 0
    num_runs: int = 0
    elapsed_ms: float = 0.0

    def summary(self) -> str:
        total_io = self.phase1_reads + self.phase1_writes + self.phase2_reads + self.phase2_writes
        return (
            f"Runs: {self.num_runs}  "
            f"Total I/O rows: {total_io:,}  "
            f"Heap ops: {self.heap_operations:,}  "
            f"Time: {self.elapsed_ms:.1f}ms"
        )


def external_merge_sort(
    input_rows: list[int],
    memory_limit: int = 10,  # max rows to hold in memory at once
) -> tuple[list[int], SortMetrics]:
    """
    Sort input_rows using external merge sort with given memory_limit.

    Returns (sorted_rows, metrics).
    Uses temp files to simulate disk-based sorted runs.
    """
    metrics = SortMetrics()
    t0 = time.perf_counter()
    tmpdir = Path(tempfile.mkdtemp(prefix="ext_sort_"))
    run_files: list[Path] = []

    # ─── Phase 1: Generate sorted runs ──────────────────────────────────────
    buffer: list[int] = []
    for row in input_rows:
        metrics.phase1_reads += 1
        buffer.append(row)
        if len(buffer) >= memory_limit:
            run_path = _write_sorted_run(buffer, tmpdir, len(run_files), metrics)
            run_files.append(run_path)
            buffer.clear()

    if buffer:
        run_path = _write_sorted_run(buffer, tmpdir, len(run_files), metrics)
        run_files.append(run_path)
        buffer.clear()

    metrics.num_runs = len(run_files)

    # ─── Phase 2: K-way merge ───────────────────────────────────────────────
    result = _kway_merge(run_files, metrics)

    # Cleanup temp files
    for f in run_files:
        f.unlink(missing_ok=True)
    tmpdir.rmdir()

    metrics.elapsed_ms = (time.perf_counter() - t0) * 1000
    return result, metrics


def _write_sorted_run(
    buffer: list[int],
    tmpdir: Path,
    run_idx: int,
    metrics: SortMetrics,
) -> Path:
    """Sort buffer and write to a temp file. Returns the file path."""
    buffer.sort()
    run_path = tmpdir / f"run_{run_idx:04d}.tmp"
    run_path.write_text("\n".join(str(x) for x in buffer))
    metrics.phase1_writes += len(buffer)
    return run_path


def _kway_merge(run_files: list[Path], metrics: SortMetrics) -> list[int]:
    """
    Merge k sorted run files using a min-heap.
    Heap entries: (value, run_index, iterator)
    """
    result: list[int] = []
    iterators = [
        (int(line) for line in f.read_text().splitlines() if line)
        for f in run_files
    ]

    # Initialize heap with the first element from each run
    heap: list[tuple[int, int]] = []
    iters = list(iterators)
    for run_idx, it in enumerate(iters):
        try:
            val = next(it)
            heapq.heappush(heap, (val, run_idx))
            metrics.heap_operations += 1
            metrics.phase2_reads += 1
        except StopIteration:
            pass

    # K-way merge
    while heap:
        val, run_idx = heapq.heappop(heap)
        metrics.heap_operations += 1
        result.append(val)
        metrics.phase2_writes += 1

        try:
            next_val = next(iters[run_idx])
            heapq.heappush(heap, (next_val, run_idx))
            metrics.heap_operations += 1
            metrics.phase2_reads += 1
        except StopIteration:
            pass  # this run is exhausted

    return result


if __name__ == "__main__":
    print("=== External Merge Sort Demo ===\n")

    # Small demo showing the algorithm visibly
    demo_data = [5, 2, 8, 1, 9, 3, 7, 4, 6, 10, 15, 11, 13, 12, 14]
    print(f"Input ({len(demo_data)} elements): {demo_data}")
    print(f"Memory limit: 5 elements at a time\n")

    sorted_result, metrics = external_merge_sort(demo_data, memory_limit=5)
    print(f"Output: {sorted_result}")
    print(f"Metrics: {metrics.summary()}")
    print(f"\nPhase 1 created {metrics.num_runs} sorted runs")
    print(f"Phase 2 merged them with {metrics.heap_operations} heap operations")
    print(f"Correct: {sorted_result == sorted(demo_data)}")

    print("\n=== Scaling: Memory limit vs Number of Runs and I/O ===\n")
    N = 1000
    data = random.sample(range(N * 10), N)
    print(f"{'Memory limit':>14}  {'Runs':>6}  {'Total I/O':>10}  {'Heap ops':>10}  {'Time':>8}")
    print("-" * 55)
    for mem in [10, 20, 50, 100, 250, 500]:
        _, m = external_merge_sort(data[:], memory_limit=mem)
        total_io = m.phase1_reads + m.phase1_writes + m.phase2_reads + m.phase2_writes
        print(f"{mem:>14}  {m.num_runs:>6}  {total_io:>10,}  "
              f"{m.heap_operations:>10,}  {m.elapsed_ms:>8.1f}ms")
    print("\nNote: larger memory limit → fewer runs → fewer heap operations → faster merge")
```

### 12.3 `spark_sort_advisor.py` — Shuffle Configuration Advisor

```python
"""
spark_sort_advisor.py — Advises on Spark shuffle and sort configuration.

Given table size, executor memory, and desired operation (sort, groupby, join),
recommends Spark configuration parameters and predicts spill behavior.

No Spark required — uses the math from Section 10.
"""
from __future__ import annotations
from dataclasses import dataclass
import math


@dataclass
class SparkSortAdvisor:
    """
    Estimates Spark external sort behavior for a given workload.
    
    All memory sizes in bytes unless noted.
    """
    data_size_gb: float
    executor_memory_gb: float
    num_executors: int
    num_cores_per_executor: int = 4

    # Spark memory fractions (from SparkConf defaults)
    storage_fraction: float = 0.5       # spark.memory.storageFraction
    execution_fraction: float = 0.5     # of total heap for execution
    shuffle_fraction: float = 0.2       # of execution memory for shuffle

    @property
    def data_size_bytes(self) -> float:
        return self.data_size_gb * 1024 ** 3

    @property
    def executor_heap_bytes(self) -> float:
        return self.executor_memory_gb * 1024 ** 3

    @property
    def execution_memory_bytes(self) -> float:
        """Memory available for execution (sort buffers, hash tables)."""
        return self.executor_heap_bytes * self.execution_fraction

    @property
    def shuffle_buffer_bytes(self) -> float:
        """Memory per task available for shuffle sort buffer."""
        return (self.execution_memory_bytes * self.shuffle_fraction
                / self.num_cores_per_executor)

    @property
    def data_per_executor_bytes(self) -> float:
        return self.data_size_bytes / self.num_executors

    @property
    def data_per_task_bytes(self) -> float:
        return self.data_per_executor_bytes / self.num_cores_per_executor

    def analyze(self, operation: str = "sort") -> dict:
        """Return analysis and recommendations for the given operation."""
        shuffle_buffer_mb = self.shuffle_buffer_bytes / 1024 ** 2
        data_per_task_mb = self.data_per_task_bytes / 1024 ** 2

        spills = max(0, math.ceil(data_per_task_mb / shuffle_buffer_mb) - 1)
        num_spill_files = spills
        spill_io_gb = (spills * data_per_task_mb * self.num_executors
                       * self.num_cores_per_executor) / 1024

        recommendations = []

        if spills == 0:
            sort_method = "In-memory sort (no spill)"
        else:
            sort_method = f"External sort: {spills} spill(s) per task"
            if spills > 10:
                recommendations.append(
                    f"High spill count ({spills}). Consider increasing executor memory "
                    f"or reducing partition size with more shuffle partitions."
                )
            if spill_io_gb > 100:
                recommendations.append(
                    f"Estimated spill I/O: {spill_io_gb:.0f} GB — "
                    f"ensure fast local SSDs (spark.local.dir)."
                )

        if data_per_task_mb > 2048:
            recommendations.append(
                f"Partition size {data_per_task_mb:.0f} MB is large. "
                f"Consider increasing spark.sql.shuffle.partitions from default 200."
            )

        ideal_partitions = max(200, math.ceil(self.data_size_gb * 1024 / 128))

        return {
            "operation": operation,
            "data_total_gb": round(self.data_size_gb, 1),
            "data_per_task_mb": round(data_per_task_mb, 1),
            "shuffle_buffer_mb": round(shuffle_buffer_mb, 1),
            "sort_method": sort_method,
            "spill_files_per_task": num_spill_files,
            "estimated_spill_io_gb": round(spill_io_gb, 1),
            "recommended_shuffle_partitions": ideal_partitions,
            "recommendations": recommendations,
        }


if __name__ == "__main__":
    print("=== Spark Sort Advisor ===\n")

    scenarios = [
        ("Small job",     SparkSortAdvisor(10,   4,  10, 4)),
        ("Medium job",    SparkSortAdvisor(100,  8,  20, 4)),
        ("Large job",     SparkSortAdvisor(1000, 16, 50, 8)),
        ("Underpowered",  SparkSortAdvisor(500,  4,  10, 4)),
    ]

    for label, advisor in scenarios:
        result = advisor.analyze("sort")
        print(f"--- {label} ---")
        print(f"  Data: {result['data_total_gb']} GB, "
              f"Per-task: {result['data_per_task_mb']} MB, "
              f"Buffer: {result['shuffle_buffer_mb']} MB")
        print(f"  Method: {result['sort_method']}")
        if result['estimated_spill_io_gb'] > 0:
            print(f"  Spill I/O: ~{result['estimated_spill_io_gb']} GB")
        print(f"  Recommended partitions: {result['recommended_shuffle_partitions']}")
        for rec in result['recommendations']:
            print(f"  ⚠ {rec}")
        print()
```

---

## 13. Hands-On Labs

### Lab 1: Verify O(N log N) — Comparison Count vs Theory

**Goal:** Empirically verify that merge sort makes approximately N log₂ N comparisons, and observe how Timsort exploits partially-sorted input.

```python
# lab1_comparison_count.py
"""
Verifies comparison counts for merge sort and quicksort across different
input sizes and distributions.
"""
import math
import random
from sorting_algorithms import merge_sort, quicksort

def run_lab(sizes: list[int]) -> None:
    print(f"{'N':>8}  {'N log N':>10}  {'MergeSort':>12}  {'ratio':>7}  {'QS avg':>12}  {'ratio':>7}")
    print("-" * 65)
    for n in sizes:
        n_log_n = n * math.log2(n) if n > 1 else 1
        
        # Merge sort — deterministic
        data = random.sample(range(n * 10), n)
        c = [0]
        merge_sort(data, c)
        ms_ratio = c[0] / n_log_n
        
        # Quicksort — average over 5 runs
        qs_totals = []
        for _ in range(5):
            data = random.sample(range(n * 10), n)
            c = [0]
            quicksort(data, c)
            qs_totals.append(c[0])
        qs_avg = sum(qs_totals) / len(qs_totals)
        qs_ratio = qs_avg / n_log_n
        
        print(f"{n:>8,}  {n_log_n:>10,.0f}  {c[0]:>12,}  {ms_ratio:>7.2f}  "
              f"{qs_avg:>12,.0f}  {qs_ratio:>7.2f}")

if __name__ == "__main__":
    random.seed(42)
    print("=== Comparison Count vs Theory ===\n")
    run_lab([100, 1_000, 10_000, 100_000])
    
    print("\n=== Timsort Advantage on Partially Sorted Input ===\n")
    import time
    N = 100_000
    
    for label, data in [
        ("Random",         random.sample(range(N*10), N)),
        ("Sorted",         list(range(N))),
        ("10 sorted runs", sum([sorted(random.sample(range(N*10), N//10)) for _ in range(10)], [])),
    ]:
        arr = data[:]
        t0 = time.perf_counter()
        arr.sort()
        elapsed = (time.perf_counter() - t0) * 1000
        print(f"  {label:<25} {elapsed:>8.2f} ms")
```

---

### Lab 2: External Merge Sort — Memory vs I/O Trade-off

**Goal:** Observe how increasing the memory limit reduces the number of runs and heap operations in external sort.

```python
# lab2_external_sort_tradeoff.py
"""
Runs external_merge_sort at different memory limits and plots the trade-off
between memory usage and I/O volume.
"""
import random
from external_merge_sort import external_merge_sort

if __name__ == "__main__":
    random.seed(42)
    N = 5_000
    data = random.sample(range(N * 10), N)

    print(f"External sort of {N:,} elements at varying memory limits\n")
    print(f"{'Memory (rows)':>14}  {'Runs':>6}  {'Phase1 I/O':>11}  "
          f"{'Phase2 I/O':>11}  {'Heap ops':>10}  {'Time(ms)':>9}")
    print("-" * 70)

    for mem in [10, 25, 50, 100, 250, 500, 1000, 2500]:
        sorted_result, m = external_merge_sort(data[:], memory_limit=mem)
        assert sorted_result == sorted(data)
        p1_io = m.phase1_reads + m.phase1_writes
        p2_io = m.phase2_reads + m.phase2_writes
        print(f"{mem:>14,}  {m.num_runs:>6}  {p1_io:>11,}  "
              f"{p2_io:>11,}  {m.heap_operations:>10,}  {m.elapsed_ms:>9.2f}")

    print("\nObservations:")
    print("  - Phase 1 I/O is always 2N (read all + write all sorted runs)")
    print("  - Phase 2 I/O is always 2N (read all runs + write output)")
    print("  - Heap ops decrease as memory limit increases (fewer runs)")
    print("  - With memory_limit = N: 1 run, 0 heap ops (pure in-memory sort)")
```

---

### Lab 3: Sort Order Impact on Parquet Predicate Pushdown

**Goal:** Generate two sets of Parquet-like data (sorted vs random) and measure how sort order affects simulated predicate pushdown.

```python
# lab3_sort_order_parquet.py
"""
Simulates Parquet row group statistics and measures predicate pushdown
effectiveness for sorted vs unsorted data.
"""
import random
import math
from dataclasses import dataclass
from typing import Optional

@dataclass
class RowGroup:
    """Simulates a Parquet row group with column statistics."""
    rows: list[int]
    
    @property
    def min_val(self) -> int:
        return min(self.rows)
    
    @property
    def max_val(self) -> int:
        return max(self.rows)
    
    def might_contain(self, lo: int, hi: int) -> bool:
        """Can the predicate [lo, hi] match any row in this group?"""
        return self.max_val >= lo and self.min_val <= hi


def simulate_parquet(rows: list[int], rows_per_group: int = 100) -> list[RowGroup]:
    """Simulate Parquet file as list of RowGroups."""
    groups = []
    for i in range(0, len(rows), rows_per_group):
        chunk = rows[i:i+rows_per_group]
        if chunk:
            groups.append(RowGroup(chunk))
    return groups


def predicate_pushdown(groups: list[RowGroup], lo: int, hi: int) -> dict:
    """Simulate predicate pushdown: count groups read vs skipped."""
    groups_read = 0
    groups_skipped = 0
    rows_scanned = 0
    rows_matched = 0
    
    for g in groups:
        if g.might_contain(lo, hi):
            groups_read += 1
            for row in g.rows:
                rows_scanned += 1
                if lo <= row <= hi:
                    rows_matched += 1
        else:
            groups_skipped += 1
    
    return {
        "groups_total": len(groups),
        "groups_read": groups_read,
        "groups_skipped": groups_skipped,
        "rows_scanned": rows_scanned,
        "rows_matched": rows_matched,
        "skip_rate": groups_skipped / len(groups) if groups else 0,
    }


if __name__ == "__main__":
    random.seed(42)
    N = 10_000
    ROWS_PER_GROUP = 100

    # 1% selectivity range
    lo, hi = 4_500, 4_600

    # Case 1: Random order
    data_random = random.sample(range(N), N)
    groups_random = simulate_parquet(data_random, ROWS_PER_GROUP)
    stats_random = predicate_pushdown(groups_random, lo, hi)

    # Case 2: Sorted order
    data_sorted = sorted(data_random)
    groups_sorted = simulate_parquet(data_sorted, ROWS_PER_GROUP)
    stats_sorted = predicate_pushdown(groups_sorted, lo, hi)

    print("=== Predicate Pushdown: Sorted vs Unsorted Parquet ===\n")
    print(f"N={N:,}, rows_per_group={ROWS_PER_GROUP}, "
          f"predicate: value IN [{lo}, {hi}] (~1% selectivity)\n")

    for label, stats in [("Unsorted (random)", stats_random), ("Sorted", stats_sorted)]:
        print(f"{label}:")
        print(f"  Row groups total:   {stats['groups_total']}")
        print(f"  Row groups read:    {stats['groups_read']} "
              f"({stats['groups_read']/stats['groups_total']:.0%})")
        print(f"  Row groups skipped: {stats['groups_skipped']} "
              f"({stats['skip_rate']:.0%})")
        print(f"  Rows scanned:       {stats['rows_scanned']:,}")
        print(f"  Rows matched:       {stats['rows_matched']:,}")
        print()

    speedup = stats_random['rows_scanned'] / max(stats_sorted['rows_scanned'], 1)
    print(f"Speedup from sort order: {speedup:.1f}x fewer rows scanned")
```

---

## 14. Interview Q&A

**Q1: What is the O(N log N) lower bound for comparison sorting, and why does it matter in data engineering?**

The lower bound comes from a decision-tree argument. Any comparison sort algorithm can be modeled as a binary decision tree where each internal node represents one comparison between two elements. Each leaf represents a distinct sorted permutation of the input. Since there are N! possible input permutations and the algorithm must distinguish between all of them, the tree needs at least N! leaves. A binary tree with N! leaves has height at least log₂(N!), and by Stirling's approximation, log₂(N!) = Ω(N log N). Since the height equals the worst-case number of comparisons, any comparison sort requires at least Ω(N log N) comparisons — no matter how clever.

The data engineering implication is that sorting is always a significant operation, not something to do carelessly. A Spark job that sorts a 1 TB partition requires at minimum ~10¹² × 40 = 4 × 10¹³ comparisons, which takes tens of seconds at modern CPU speeds regardless of algorithm choice. This is why the "sort once, read many" principle matters: paying the O(N log N) sort cost once when writing Parquet files (with tight min/max statistics per row group) avoids O(N) scans on every subsequent query. The one-time sort cost is amortized across every read over the table's lifetime.

**Q2: Walk me through how Spark shuffle works at the algorithm level. What sorting algorithm does it use and why?**

Spark's sort-based shuffle (default since 1.6) operates in two phases on each executor. In the map phase, each task receives a partition of raw data and must route rows to the correct reduce partition based on a partition key (e.g., a GROUP BY key or JOIN key). For each row, Spark computes `partitionId = hash(key) % numReducePartitions`. Rather than writing one file per partition (the old hash shuffle approach, which creates numMapTasks × numReducePartitions files and caused small file explosion), it inserts `(partitionId, row_pointer)` into an in-memory buffer.

The in-memory buffer uses Tungsten's binary sort: instead of sorting full rows (which may be 1 KB each), it sorts 16-byte records consisting of an 8-byte encoded sort key and an 8-byte pointer to the serialized row. This reduces sort payload by 64–1000× compared to sorting full row objects. When the buffer is full, it is sorted by (partitionId, sort_key) using a Timsort variant, then spilled to a temporary file on local disk. At the end of the map phase, all spill files are k-way merged (using a min-heap) to produce a single sorted shuffle file, plus an index file recording the byte range for each partitionId's data.

Timsort is chosen because real shuffle data has significant partial order — rows for the same partition are often adjacent in the input since they were produced by the previous stage's partitioning. Timsort's O(N) performance on presorted runs means partially-ordered shuffle data sorts much faster than a pure merge sort or quicksort would manage. In the reduce phase, each reduce task fetches its partition's data (one byte range from each map task's shuffle file), which is already sorted within each map task's contribution. A k-way merge over all incoming sorted streams produces the final sorted order for the reduce task, enabling streaming aggregation or sort-merge join without a full re-sort.

**Q3: A Spark job is slow because of excessive shuffle spill. How do you diagnose and fix it?**

The Spark UI is the first stop. In the Stages tab, I look at the "Shuffle Write" and "Shuffle Read" columns and then drill into individual tasks. The "Spill (Memory)" and "Spill (Disk)" metrics per task tell me how much data was evicted from the in-memory sort buffer to disk. If I see large spill values — say, 50 GB spill on a task that processed 60 GB — the sort buffer is far too small for the partition size.

The root causes are: (1) partition size is too large relative to the shuffle memory available per task, (2) data is skewed so one or a few tasks have dramatically larger partitions than others.

For case 1, the fix is to increase `spark.sql.shuffle.partitions` (default 200). If the total shuffle data is 500 GB and 200 partitions means 2.5 GB per partition, but the executor's shuffle buffer is only 200 MB, every task will spill 12+ times. Increasing to 2000 partitions reduces per-partition size to 250 MB — fits in the buffer, no spill. Alternatively, increase `spark.executor.memory` or tune `spark.memory.fraction` (default 0.6, which controls execution + storage memory) and `spark.memory.storageFraction` to give more memory to execution.

For case 2 (skew), I check the task distribution in the Spark UI — if one task takes 10× longer than the median, I look at its shuffle read size. If it's 10–100× larger than median, skew is the cause. The fix is AQE skew join handling (`spark.sql.adaptive.skewJoin.enabled=true`) which automatically splits skewed partitions, or manual salting of the hot key. For a GROUP BY with a highly repeated key (e.g., aggregating events where `user_id = 'bot'` has 1 billion rows), I add a random salt to the key before the first aggregation, aggregate with the salted key, then re-aggregate by the true key after.

**Q4: Explain why merge sort is used as the basis for external sort while quicksort is not.**

Quicksort's partition step works by rearranging elements within a single contiguous array — the pivot ends up at its final position, and all elements smaller than the pivot are to its left, larger to its right. This rearrangement is inherently in-place and global: after one partition step, elements from anywhere in the array may have moved to any other position. If you have 100 GB of data and want to split it into sorted chunks to write to disk, quicksort's partition cannot produce independently sortable halves — after the first partition, you still have all 100 GB of data rearranged in a single array, not two independently sortable chunks.

Merge sort, by contrast, divides the array into two physically independent halves at the midpoint, sorts each half recursively, then merges. The divide step does not move any elements — it simply decides a boundary. The independent sorted halves can be written to separate disk files, and the merge step can then work by reading one element at a time from each sorted file — a streaming operation that never requires more than O(k) memory for k sorted files.

The merge step generalizes perfectly to arbitrary numbers of sorted runs: k-way merge reads the minimum across all run heads (using a min-heap in O(log k)) and outputs it. This is the heart of external sort. Quicksort has no equivalent generalization to on-disk data — its partition requires random access across the full dataset, which is O(N) disk seeks. External merge sort's access pattern is purely sequential: read each run from beginning to end, write the output sequentially. Sequential I/O is 10–100× faster than random I/O on both SSDs and spinning disks.

**Q5: Compare hash aggregate and sort aggregate for a GROUP BY operation. When would you choose each in Spark, and how does the sort approach relate to external merge sort?**

Hash aggregate builds an in-memory hash table: for each input row, it looks up the group key in the hash table and updates the accumulator. Expected O(N) time, O(G) space where G is the number of distinct groups. The advantage is simplicity and speed when G is small relative to executor memory — the entire state fits in RAM and there's no sorting cost. The disadvantage is that when G is large (hundreds of millions of distinct users in a global aggregation), the hash table exceeds available memory and must spill, which in Spark's implementation involves re-partitioning the hash table into smaller chunks and re-aggregating them — a messy, hard-to-configure process.

Sort aggregate first sorts all input rows by the group key using external merge sort, producing a sorted stream. Then it walks the sorted stream and whenever the group key changes, emits the completed aggregate for the previous group and starts a new accumulator. The sort step may spill to disk (external merge sort with controlled spill behavior), but the aggregate step itself is O(1) streaming — it never needs more than one group's accumulator in memory at a time.

For a GROUP BY with low cardinality (1,000 distinct countries), hash aggregate wins by a large margin — no sort needed. For a GROUP BY with very high cardinality (1 billion distinct users), sort aggregate is safer — it guarantees that the aggregate step itself never OOMs, only the sort might spill. For a GROUP BY where the input is already sorted by the group key (a common case after a previous JOIN or SORT step), sort aggregate is O(N) total with zero sort cost — Timsort recognizes the already-sorted runs and performs no comparisons. The sort-once principle applies here: if you design your pipeline to keep data sorted by the common GROUP BY key, sort aggregate becomes essentially free.

In Spark, hash aggregate is the default but AQE can switch to sort aggregate when hash spill is detected. You can force sort aggregate with `spark.sql.execution.useObjectHashAggregateExec=false`, though this is rarely needed — AQE handles most cases correctly.

**Q6: Your team is designing a data lakehouse where most queries filter on event_date and user_id. How do you structure the sort order when writing Parquet files to maximize query performance?**

The first question is whether to partition by event_date or sort by it. These are different mechanisms with different trade-offs. Partition by event_date creates separate directories for each date's data, enabling entire partition directories to be pruned before reading any file. For a filter like `WHERE event_date = '2024-01-15'`, partition pruning skips all other dates at the filesystem level — no file reads at all. This is strictly more powerful than row group statistics for high-granularity predicates.

Within each date partition, I would sort by user_id. This achieves tight row group min/max statistics for user_id within each partition's Parquet files. A query like `WHERE event_date = '2024-01-15' AND user_id BETWEEN 'u_1000' AND 'u_2000'` first prunes all partitions except Jan 15, then within the Jan 15 files, the sorted user_id order means each row group covers a contiguous range of user IDs — most row groups are completely outside the predicate range and are skipped via Parquet column statistics.

In Iceberg syntax: `PARTITIONED BY (event_date) SORTED BY (user_id)`. In Spark: write with `.partitionBy("event_date").sortBy("user_id")` before calling `.write.parquet()`. The sort within each partition requires an external sort (if partition size exceeds executor memory), which is O(N log N) in rows per partition.

The remaining question is what to do with compound queries that filter on both user_id and event_type without event_date. If those are common, Z-order (multi-dimensional) sorting provides a space-filling curve that co-locates rows sharing both user_id and event_type ranges, at the cost of slightly looser single-column statistics. Databricks Delta Lake supports Z-order; Iceberg supports it via its sort order specification. For the team's stated use case (event_date + user_id), the simpler partition-by-date, sort-by-user_id design is correct and avoids the complexity of multi-dimensional ordering.

---

## 15. Cross-Question Chain

**Q1 [Interviewer]: What is merge sort?**

Merge sort is a comparison-based sorting algorithm that follows the divide-and-conquer paradigm. It recursively splits the input array into two halves, sorts each half independently, then merges the two sorted halves into a single sorted array. The merge step is the key: it takes two sorted sequences and produces one sorted sequence by repeatedly comparing the smallest unprocessed element from each half and outputting the smaller. The recurrence is T(N) = 2T(N/2) + O(N), which by the Master Theorem (case 2, a=2, b=2, d=1, log₂2=1=d) resolves to O(N log N). Merge sort is stable — equal elements preserve their original relative order — and requires O(N) extra space for the merge output buffer.

**Q2 [Interviewer]: Why O(N log N) and not O(N)? Can we sort faster?**

O(N log N) is a provable lower bound for any comparison-based sort. The argument: imagine representing every possible comparison sort as a binary decision tree where each node is a comparison and each leaf is one permutation of the output. A tree sorting N elements has at least N! leaves (one per possible input permutation). A binary tree with N! leaves has depth at least log₂(N!) = Ω(N log N) by Stirling's approximation. The depth is the worst-case number of comparisons. Therefore no comparison sort can do better than O(N log N).

However, if you don't use comparisons — if you exploit structure of the keys — you can beat this bound. Counting sort sorts N integers in range [0, K] in O(N + K). Radix sort processes integer keys digit by digit in O(d × N) where d is the number of digits — effectively O(N) for fixed-width integers. These are not comparison sorts; they're incompatible with arbitrary orderings. In data engineering, keys are often integers, timestamps, or fixed-length strings — radix sort is possible — but the flexibility and correctness guarantees of comparison sorts make them the default for general-purpose use.

**Q3 [Interviewer]: Now explain how this connects to Spark GROUP BY performance.**

A Spark GROUP BY operation needs to collect all rows with the same key together. There are two algorithmic approaches. Hash aggregate: for each input row, hash the key to find a bucket in an in-memory hash table, update the accumulator. O(N) expected, O(G) space for G distinct groups. Sort aggregate: sort all rows by group key (O(N log N)), then walk the sorted stream and emit a result whenever the key changes — O(1) memory for the streaming pass, but requires the sort upfront.

For in-memory GROUP BY, hash aggregate wins — O(N) beats O(N log N). The problem is when G is large relative to available memory. A hash table holding 100 million distinct group keys at 32 bytes each needs 3.2 GB, and it can't stream — it must hold all groups simultaneously. Sort aggregate is more memory-efficient: the external sort can spill controlled sorted runs to disk, and the streaming aggregate pass needs only one accumulator at a time. This is why AQE in Spark can fall back from hash aggregate to sort aggregate mid-query when the hash table approaches memory limits.

**Q4 [Interviewer]: How does Spark shuffle work when data exceeds executor memory?**

Spark uses external merge sort. During the map phase, each task accumulates (partitionId, row) pairs in a memory buffer — specifically Tungsten's binary sort buffer, where rows are represented as (8-byte sort key, 8-byte row pointer) pairs to minimize copy overhead. When the buffer is full, it is sorted in-memory by (partitionId, sort_key) using a Timsort variant, then written as a sorted spill file to local disk. This repeats until all input rows are processed.

At the end of the map phase, all spill files (sorted by partitionId+key within each file) are merged using a k-way merge with a min-heap — one heap entry per spill file, always extracting the minimum. The merged output is written as a single sorted shuffle file on local disk, plus an index file with byte offsets per partitionId. The reduce tasks then fetch their partitionId's byte range from every map task's shuffle file, and merge the incoming sorted streams (again a k-way merge) before processing. The key property is that the sort happens twice: once per map task (local spill merge) and once per reduce task (cross-executor merge). Both use external merge sort because shuffle data frequently exceeds executor memory.

**Q5 [Interviewer]: The Spark shuffle is taking 4 hours on a 200 GB dataset. What would you check?**

First I open the Spark UI and look at the Stage metrics for the shuffle. I check three things: (1) the Spill (Memory) and Spill (Disk) columns per task — large spill means the sort buffer is too small relative to partition size. (2) task duration distribution — if one task takes 3 hours and the rest take 10 minutes, I have skew. (3) GC time column — if GC time is >10% of task time, the JVM heap is under pressure and object-heavy sorting is causing GC pauses.

For spill: I check `spark.sql.shuffle.partitions` (likely at default 200 for 200 GB = 1 GB per partition, fine), but if data is skewed, some partitions might be 20 GB+. I would increase partitions to 2000 and ensure each executor has enough memory (`spark.executor.memory`) to hold a 100 MB buffer per core.

For skew: I run `df.groupBy(key).count().orderBy(desc("count")).show(10)` to find hot keys. If one key has 80% of rows, AQE's skew join detection (`spark.sql.adaptive.skewJoin.enabled=true`) will split that partition into sub-partitions automatically in Spark 3.x. For earlier versions, I manually salt the key.

For GC: I switch to off-heap memory (`spark.memory.offHeap.enabled=true`) and allocate the sort buffer off-heap. This removes the sort buffer from GC scope entirely — Tungsten's UnsafeRow encoding means the sort operates on raw bytes in off-heap memory with no Java object overhead.

**Q6 [Interviewer]: We're building a new data lake. Convince me to sort Parquet files on write even though it costs time.**

Sorting on write is a one-time cost that pays off on every read. Here's the math for a concrete case: 10 TB events table, queried 200 times per day, median query filter on `user_id` with 0.1% selectivity.

Without sorting: every query reads all 10 TB (Parquet row group statistics are useless for user_id because each row group contains all user IDs). 200 queries × 10 TB = 2 PB of data scanned per day. At $5/TB in BigQuery or $0.005/GB for S3 data transfer, that's significant compute and storage I/O cost.

With sorting by user_id: each row group covers a contiguous range of ~1/10,000th of the user ID space (at 100 million rows per file ÷ 100,000 row groups per file). A query for user_id 'u_12345' skips ~99.9% of row groups. 200 queries × 10 TB × 0.001 = 2 TB scanned per day — 1000× reduction.

The sort cost: sorting 10 TB requires an external sort, approximately 2× the data volume in I/O (one read pass, one write pass) = 20 TB of I/O at write time. At the cluster's write throughput (say 10 GB/s), that's ~30 minutes per full table sort.

Break-even: 2 PB/day × $5/TB = $10,000/day (without sort). 2 TB/day × $5/TB = $10/day (with sort). The sort pays for itself in the first 30 minutes of query traffic. After that, every day the sorted layout saves ~$9,990 in query costs.

---

## 16. Common Misconceptions

**"Quicksort is always faster than merge sort."**
In practice, quicksort has a smaller constant factor than merge sort for in-memory sorting of primitives (no extra allocation, cache-friendly in-place operations). But Python's `list.sort()` uses Timsort, not quicksort, because Python's actual workload includes partially-sorted data and object comparisons where Timsort wins. Java's `Arrays.sort()` uses dual-pivot quicksort for primitives and Timsort for objects. The "quicksort is faster" statement is hardware- and workload-specific, not universally true.

**"Sorting 10 times more data takes 10 times longer."**
Sorting is O(N log N), not O(N). At 10× data: 10N × log(10N) = 10N × (log N + log 10) ≈ 10N × log N × (1 + log10/logN). For N = 1 billion, log10/logN ≈ 0.1, so 10× data takes about 11× time — slightly more than 10×. At N = 1,000, log10/logN ≈ 0.3, so 10× data takes about 13× time. The log factor grows, but slowly — sorting is "almost linear" in practice.

**"Spark shuffle partitioning only affects parallelism, not sort performance."**
Increasing `spark.sql.shuffle.partitions` reduces per-partition data size. If per-partition data fits in the sort buffer, no spill occurs. If it doesn't fit, spill files are created and the k-way merge adds I/O cost. More partitions = smaller per-partition sort = fewer spills = faster per-task sort. The trade-off: too many partitions creates scheduling overhead and small file problems downstream. The practical sweet spot is 100–200 MB per partition.

**"ORDER BY in SQL always requires a sort."**
If a B-tree index exists on the ORDER BY column, Postgres can use the index to produce rows in order without sorting — an index scan in the correct direction is already sorted. Adding `LIMIT 100` makes this extremely efficient: read the first 100 leaf entries in the B-tree (or last 100 for `DESC`), fetch those heap pages, done. The sort is needed only when no supporting index exists.

**"External sort requires many passes over the data."**
A single-pass k-way merge is achievable if the number of initial sorted runs fits in memory for the merge phase. If memory is 8 GB and each run needs a 64 KB read buffer: 8 GB / 64 KB = 131,072 simultaneous runs. Most practical datasets (even petabytes) have far fewer initial runs than this buffer limit. In practice, well-configured external sort does exactly 2 full passes over the data: one read+write for Phase 1, one read+write for Phase 2.

---

## 17. Performance Reference Card

| Algorithm | Best | Average | Worst | Space | Stable | Notes |
|---|---|---|---|---|---|---|
| Merge sort | O(N log N) | O(N log N) | O(N log N) | O(N) | Yes | Predictable, external-sort foundation |
| Quicksort | O(N log N) | O(N log N) | O(N²) | O(log N) | No | Fast in practice, bad on sorted input |
| Heapsort | O(N log N) | O(N log N) | O(N log N) | O(1) | No | Poor cache behavior, rarely used in practice |
| Timsort | O(N) | O(N log N) | O(N log N) | O(N) | Yes | Python/Java default, exploits real-world order |
| Counting sort | O(N+K) | O(N+K) | O(N+K) | O(K) | Yes | Only for small-range integers |
| Radix sort | O(dN) | O(dN) | O(dN) | O(N+K) | Yes | Fast for fixed-width integer keys |

**External merge sort at scale:**

| Dataset | Memory | Runs | Merge passes | Total I/O |
|---|---|---|---|---|
| 100 GB | 8 GB | 13 | 1 | 400 GB |
| 1 TB | 8 GB | 125 | 1 | 4 TB |
| 10 TB | 64 GB | 157 | 1 | 40 TB |
| 100 TB | 64 GB | 1,563 | 1 (if 64 KB bufs) | 400 TB |

All cases: 2 passes total (1 for runs + 1 for merge). Total I/O = 4× dataset size.

---

## 18. Connections to Other Modules

**CSF-ALG-101 M01 (Complexity Analysis):** The O(N log N) lower bound proof uses the decision tree model. The Master Theorem analysis of merge sort's recurrence T(N) = 2T(N/2) + O(N) → O(N log N) is the canonical example of case 2 of the Master Theorem. The "sorting is O(N log N)" fact was stated in M01; this module provides the proof.

**CSF-ALG-101 M02 (Hash Tables):** Hash aggregate (O(N) expected, requires O(G) memory) vs sort aggregate (O(N log N), O(1) streaming memory) is the core trade-off between hash tables and sorting. Both approaches are used in Spark; choosing correctly requires understanding both.

**CSF-ALG-101 M03 (Trees):** The min-heap used in k-way merge is a tree data structure. Each heap extraction is O(log k) — the tree height — giving O(N log k) total merge cost. The B-tree index build in Postgres uses external merge sort followed by bulk-load — trees and sorting are directly coupled in production databases.

**DCS-SPK-101 (Spark Architecture):** Spark's shuffle is built on external merge sort. The stage boundary concept (every shuffle creates a stage boundary) is a direct consequence of the map→sort→spill→reduce→merge structure of sort-based shuffle.

**DCS-SPK-102 (Spark Performance Engineering):** Spill metrics, skew detection, AQE partition coalescing, and shuffle partition tuning are all applications of the external sort principles covered here.

**CPL-CLD-102 (Open Table Formats):** Iceberg's sort order specification (which columns to sort within each partition), Delta Lake's Z-order, and the "data skipping" feature of all modern table formats depend on sorting data at write time to make row group statistics tight.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is the comparison sort lower bound and why? | Ω(N log N). Decision tree argument: N! permutations → N! leaves → tree height ≥ log₂(N!) = Ω(N log N). No comparison sort can beat this. |
| 2 | What is the recurrence for merge sort and its solution? | T(N) = 2T(N/2) + O(N). Master Theorem case 2 (a=2, b=2, d=1, log₂2=1=d): T(N) = O(N log N). |
| 3 | Why is merge sort stable and quicksort (standard) not? | Merge sort uses ≤ in the merge comparison — equal left elements are taken before right elements, preserving insertion order. Quicksort's partition swaps equal elements arbitrarily. |
| 4 | What is the worst case for quicksort and how is it triggered? | O(N²). Triggered when the pivot is always the minimum or maximum element. Sorted or reverse-sorted input with Lomuto (last-element) pivot causes this. Fix: random pivot selection. |
| 5 | What is Timsort and why does Python use it? | Adaptive merge sort that exploits natural runs. O(N) on sorted/nearly-sorted input, O(N log N) worst case. Python uses it because real-world data has significant partial order. |
| 6 | What is a "natural run" in Timsort? | A maximal already-sorted (ascending or descending) subsequence in the input. Timsort detects them, extends short ones to MIN_RUN (32–64) with insertion sort, then merges using the merge stack. |
| 7 | What are the two phases of external merge sort? | Phase 1: read chunks, sort in memory, write as sorted run files. Phase 2: k-way merge of all run files using a min-heap, writing the final sorted output. |
| 8 | What data structure powers the k-way merge in external sort? | A min-heap of (value, run_index) pairs. Size = k (number of runs). Each output element costs O(log k): one heap extraction + one heap insertion. Total: O(N log k). |
| 9 | What is the total I/O cost of a well-configured external sort? | 4 × dataset size: 1 read + 1 write in Phase 1 (run generation) + 1 read + 1 write in Phase 2 (merge). Achievable in 2 disk passes if number of runs fits in the read buffer count. |
| 10 | What triggers a Spark shuffle? | Any operation requiring data repartitioning: GROUP BY, JOIN (non-broadcast), ORDER BY, repartition(), distinct(), UNION ALL with matching schemas. |
| 11 | What is Tungsten's sort optimization in Spark shuffle? | Sort 16-byte (key, row_pointer) records instead of full rows. After sorting pointers, serialize rows in order. Reduces per-element sort payload by 64–1000×, cutting copy overhead proportionally. |
| 12 | What does the Spark shuffle index file contain? | Byte offsets for each reduce partitionId's data within the map task's sorted shuffle file. Allows reduce tasks to fetch exactly one byte range per map task instead of reading the whole file. |
| 13 | What is AQE skew join handling? | When a shuffle partition is detected as >5× the median size, Spark splits it into sub-partitions. Each sub-partition becomes its own sort task. Restores parallelism without manual salting. |
| 14 | Why does sorting Parquet data before writing improve query performance? | Sorted data produces tight row group min/max statistics. Predicate pushdown can skip row groups whose range doesn't overlap the query filter — often 90%+ of the data. |
| 15 | Hash aggregate vs sort aggregate — when does sort aggregate win? | When the number of distinct group keys is so large that the hash table exceeds executor memory. Sort aggregate is a streaming pass over sorted data — O(1) memory for the aggregate step itself. |
| 16 | What is the sort-merge join algorithm? | Sort both relations by the join key (external sort if needed). Then scan both sorted streams simultaneously with two pointers, matching rows with equal join keys. O(N log N + M log M + N + M). |
| 17 | Hash join vs sort-merge join — when does sort-merge join win? | When the build side is too large to fit in executor memory for hash join. Sort-merge join can sort and merge data larger than memory using external sort with bounded spill. |
| 18 | What Spark config controls shuffle partition count? | `spark.sql.shuffle.partitions` (default 200). Rule of thumb: 100–200 MB per partition. For 1 TB shuffle: use 5,000–10,000 partitions. |
| 19 | What is the Redshift sort key and why does it matter? | The column(s) by which Redshift physically stores rows on disk. Zone maps (min/max per disk block) are tight for the sort key, enabling block-level pruning equivalent to a database index for range queries. |
| 20 | Why is insertion sort used within Timsort's MIN_RUN phase? | For small N (≤64), insertion sort's tiny constant outweighs its O(N²) complexity. It also has excellent cache behavior (sequential writes into a small buffer) and preserves stability. |

---

## 20. Further Reading

**Foundational:**
- Knuth, D. (1998). *The Art of Computer Programming, Volume 3: Sorting and Searching*. The definitive reference — Chapter 5 covers every sorting algorithm with exhaustive analysis.
- Peters, T. (2002). Timsort description: https://bugs.python.org/file4451/timsort.txt — the original specification by Timsort's author. Remarkably readable.

**External sort:**
- Graefe, G. (1994). *Volcano — An Extensible and Parallel Query Evaluation System*. IEEE Transactions on Knowledge and Data Engineering. Covers external sort as implemented in database query engines.
- Ramakrishnan, R. & Gehrke, J. (2003). *Database Management Systems*, Chapter 13 (External Sorting). Standard textbook treatment with the two-pass and multi-pass models.

**Spark shuffle:**
- Spark internals documentation: https://databricks.com/session/deep-dive-apache-spark-shuffling — covers Tungsten sort and the sort-based shuffle history
- "A Deeper Understanding of Spark's Internals" by Aaron Davidson (Spark Summit 2014) — the sort-based shuffle design rationale

**Data lake sort order:**
- Iceberg sort order specification: https://iceberg.apache.org/spec/#sort-orders
- Delta Lake Z-order: https://docs.delta.io/latest/optimizations-oss.html

---

## 21. Module Summary

Sorting is governed by a hard lower bound: any algorithm that sorts by comparison requires at least Ω(N log N) comparisons, provably. Merge sort achieves this bound via the recurrence T(N) = 2T(N/2) + O(N) → O(N log N). It is stable and produces independently sortable subproblems — the property that makes it the foundation of external sort. Quicksort is O(N log N) on average but O(N²) on sorted input (the common case in data engineering), requires random pivot selection to be safe, and cannot produce independently sortable subproblems. Timsort hybridizes insertion sort and merge sort, exploiting natural sorted runs to achieve O(N) on real-world partially-ordered data.

External merge sort solves the problem that data exceeds available memory. Phase 1 reads memory-sized chunks, sorts them, and writes sorted run files. Phase 2 k-way merges all runs via a min-heap, streaming the global minimum to the output. The total cost is 4× the dataset size in I/O — regardless of how much data there is relative to memory, in exactly two passes.

In data engineering, sorting appears everywhere: Spark shuffle (sort-based, external, distributed), Spark GROUP BY (sort aggregate for high-cardinality keys), sort-merge join (when hash join would OOM), Postgres B-tree index builds (external sort before bulk-load), Parquet write order (tighter row group statistics → better predicate pushdown), and column-oriented data warehouses (Redshift sort keys, BigQuery CLUSTER BY, Iceberg sort order). Every performance decision that involves "sort once, read many" applies the principle that O(N log N) write cost amortized across all reads is cheaper than O(N) read cost paid on every query.

---

**CSF-ALG-101: 4 of 5 modules complete.**  
**Next: M05 — Graphs and DAGs (BFS/DFS, topological sort — the algorithm behind Airflow and Spark DAGs)**
