# CSF-ALG-101 M02: Hash Tables

**Course:** CSF-ALG-101 — Algorithms and Data Structures for Data Engineers  
**Module:** 02 of 05  
**Filesystem position:** 00_CS_Foundations/M13_Hash_Tables  
**Prerequisites:** CSF-ALG-101 M01 (Complexity Analysis)

---

## Table of Contents

1. [The Problem Hash Tables Solve](#1-the-problem-hash-tables-solve)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: Hash Functions and the Hash Table Contract](#4-core-theory-hash-functions-and-the-hash-table-contract)
5. [Visual: Hash Table Anatomy](#5-visual-hash-table-anatomy)
6. [Deep Dive: Collision Resolution](#6-deep-dive-collision-resolution)
7. [Mental Models](#7-mental-models)
8. [Failure Scenarios](#8-failure-scenarios)
9. [Data Engineering Connections](#9-data-engineering-connections)
10. [Python Internals: How dict Works](#10-python-internals-how-dict-works)
11. [Consistent Hashing](#11-consistent-hashing)
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

## 1. The Problem Hash Tables Solve

Consider this scenario: you have a 500 GB events table and a 2 GB users dimension table. You need to join them on `user_id`. The naive approach — for every event row, scan all 2 GB of users to find the matching row — is O(N×M). At a billion events and 10 million users, that is 10^16 comparisons. No hardware can save you.

The hash table is the data structure that turns this from O(N×M) to O(N+M). It does this by solving one problem with extraordinary elegance: **given a key, find the associated value in O(1) time regardless of how many keys are stored.**

This module explains how that works — and why understanding the mechanism matters for every data engineer who has written a Spark join, configured a Kafka partition, or wondered why their Python dict lookup is faster than a database index scan.

---

## 2. How to Use This Module

**For interview prep:** Sections 4, 6, 10, 11, 14, 15 are your core. Section 14 has six staff-level interview questions with long-form answers. Section 15 has a six-exchange escalating chain.

**For production debugging:** Sections 8, 9, 10 explain real failure modes — Python dict degradation, Spark hash join OOM, Kafka partition skew.

**For fundamental understanding:** Read all sections in order. The progression is: contract → mechanism → failure modes → production systems → code.

---

## 3. Prerequisites Check

Before this module, you need:

- **Big-O notation** (CSF-ALG-101 M01): you must understand O(1) amortized vs O(N) worst case
- **Amortized analysis** (M01): hash table resizing is geometric doubling — same as Python list
- **Memory hierarchy** (CSF-ARC-102 M01): hash tables have poor cache behavior; understanding why requires knowing what a cache miss costs

---

## 4. Core Theory: Hash Functions and the Hash Table Contract

### 4.1 The Contract

A hash table provides these operations:

| Operation | Average Case | Worst Case |
|-----------|-------------|------------|
| `get(key)` | O(1) | O(N) |
| `set(key, value)` | O(1) amortized | O(N) |
| `delete(key)` | O(1) | O(N) |
| `contains(key)` | O(1) | O(N) |

The gap between average and worst case is not theoretical. It happens in production, and understanding why requires understanding the mechanism.

### 4.2 The Hash Function

A hash function maps an arbitrary key to an integer index in a fixed-size array:

```
hash(key) → integer in [0, capacity)
```

For this to produce O(1) lookup, the hash function must satisfy three requirements:

**Determinism:** The same key must always produce the same hash. `hash("user_123")` must return 42 every time, on every machine, in every process (with caveats for Python's hash randomization, discussed in Section 10).

**Uniformity:** The output distribution must be approximately uniform across the output range. If all keys map to index 0, every lookup is O(N) instead of O(1). A good hash function spreads keys evenly so that the expected number of keys per bucket is N/capacity — the load factor.

**Speed:** The hash function itself must be O(1) in the size of the key. For fixed-size keys (int64, UUID), this is trivially satisfied. For variable-length keys (strings, byte arrays), it means processing every byte — a string of length L requires O(L) work to hash. This is not O(1) in the general case, but in practice string keys have bounded length, so it is treated as O(1) amortized.

### 4.3 The Load Factor

The load factor α is defined as:

```
α = N / capacity
```

where N is the number of stored key-value pairs and capacity is the number of buckets in the backing array.

The load factor is the single most important parameter governing hash table performance. As α increases:

- The probability of collision increases
- Average lookup time increases (because more collisions mean longer probe sequences or longer chains)
- Cache efficiency decreases (more memory accessed per lookup)

The relationship between α and expected lookup time depends on the collision resolution strategy. For chaining, the expected number of comparisons per lookup is `1 + α/2`. For open addressing with linear probing, it is approximately `1/(1 - α)` — which approaches infinity as α → 1.

This is why every industrial hash table implementation enforces a maximum load factor and resizes when it is exceeded:

| Implementation | Max Load Factor | Notes |
|---|---|---|
| Python dict | 0.667 (2/3) | Resize triggers at 2/3 capacity |
| Java HashMap | 0.75 | Default, configurable |
| Java Hashtable | 0.75 | Legacy, synchronized |
| C++ unordered_map | 1.0 | Default, configurable |
| Go map | ~6.5 avg per bucket | Different structure |
| Redis hash | 1.0 | Incremental rehash |

### 4.4 Resizing (Rehashing)

When the load factor exceeds the threshold, the hash table must grow. The standard approach is geometric doubling: allocate a new array of size `2 × old_capacity`, then re-insert every existing key-value pair into the new array.

Why doubling? Because it keeps the amortized cost of insertions O(1). The same geometric series argument from Python list (M01) applies: if you double each time, the total resize work across N insertions is O(N), making each insertion O(1) amortized.

The cost of a single resize is O(N) — every key must be re-hashed and re-inserted. This is the source of the occasional latency spike in production systems. Python dict has this spike; Redis uses incremental rehashing to spread the cost across subsequent operations.

### 4.5 Why Hash Table Lookup Can Be O(N) in the Worst Case

If all N keys hash to the same bucket, every lookup must traverse a chain of length N. This is the O(N) worst case. It is not just theoretical: a carefully crafted adversarial input — one that exploits a known hash function — can force all keys into one bucket. This is a **hash flooding attack**, which is why Python 3.3 introduced hash randomization (PYTHONHASHSEED) by default for string and bytes objects. Without randomization, a web server that uses a dict keyed by user-supplied strings is vulnerable to denial-of-service via hash flooding.

---

## 5. Visual: Hash Table Anatomy

### 5.1 Chaining

```
Backing array (capacity = 8)
┌────────────────────────────────────────────────────────────┐
│ Index │ Bucket                                              │
├───────┼─────────────────────────────────────────────────────┤
│  [0]  │ → ("alice", 99) → ("ivan", 7) → None               │
│  [1]  │ → None                                              │
│  [2]  │ → ("bob", 42) → None                               │
│  [3]  │ → None                                              │
│  [4]  │ → ("carol", 11) → ("zara", 55) → None              │
│  [5]  │ → None                                              │
│  [6]  │ → ("dave", 3) → None                               │
│  [7]  │ → ("eve", 88) → None                               │
└────────────────────────────────────────────────────────────┘

N = 7 keys, capacity = 8, load factor α = 7/8 = 0.875 (too high)
Bucket 0 has a chain of length 2 → collision between "alice" and "ivan"
Lookup "ivan": hash("ivan") % 8 = 0 → traverse chain → compare "alice" (no) → compare "ivan" (yes) → return 7
```

### 5.2 Open Addressing (Linear Probing)

```
Backing array (capacity = 8)
┌─────────────────────────────────────────────────┐
│ Index │ Slot                                     │
├───────┼──────────────────────────────────────────┤
│  [0]  │ ("alice", 99)   ← hash("alice") % 8 = 0 │
│  [1]  │ ("ivan", 7)     ← collision! probe +1    │
│  [2]  │ ("bob", 42)     ← hash("bob") % 8 = 2   │
│  [3]  │ EMPTY                                    │
│  [4]  │ ("carol", 11)   ← hash("carol") % 8 = 4 │
│  [5]  │ ("zara", 55)    ← collision! probe +1    │
│  [6]  │ ("dave", 3)     ← hash("dave") % 8 = 6  │
│  [7]  │ ("eve", 88)     ← hash("eve") % 8 = 7   │
└─────────────────────────────────────────────────┘

Lookup "ivan": hash("ivan") % 8 = 0 → slot 0 has "alice" (no) → slot 1 has "ivan" (yes) → return 7
Delete "alice": must leave a TOMBSTONE, not EMPTY — otherwise "ivan" lookup stops prematurely at slot 0
```

### 5.3 MESI Implication (Cache Behavior)

```
Chaining                        │  Open Addressing
─────────────────────────────── │ ────────────────────────────────
Array access:   1 cache line    │  Array access:   1 cache line
Pointer chase:  +1 cache miss   │  Next probe:     likely same line
Per collision:  70–200 ns miss  │  Per probe:      ~1 ns (L1 hit)

→ Open addressing is cache-friendly; chaining is cache-hostile for long chains
```

---

## 6. Deep Dive: Collision Resolution

### 6.1 Separate Chaining

Each bucket in the array holds a pointer to a linked list of key-value pairs. On collision, the new pair is appended to the bucket's chain.

**Lookup:** Hash the key → find the bucket → traverse the chain comparing keys.

**Pros:**
- Simple to implement
- Load factor can exceed 1.0 without performance cliff
- Deletions are simple (unlink the node)

**Cons:**
- Poor cache behavior: each node in the chain is a separate heap allocation, typically not adjacent in memory. Every pointer dereference in a long chain is a potential cache miss (70 ns on modern hardware)
- Memory overhead: each node carries a next pointer (8 bytes)

**Used by:** Java HashMap, most database hash join implementations

### 6.2 Open Addressing

All key-value pairs are stored in the backing array itself. On collision, a probe sequence finds the next empty slot.

**Linear probing:** Probe sequence is `h, h+1, h+2, ...` mod capacity. Cache-friendly (consecutive slots are in the same or adjacent cache lines), but suffers from **primary clustering** — long runs of occupied slots form and grow, degrading performance.

**Quadratic probing:** Probe sequence is `h, h+1², h+2², ...` mod capacity. Reduces primary clustering but introduces secondary clustering (all keys with the same initial hash follow the same probe sequence).

**Double hashing:** Probe sequence is `h₁, h₁+h₂, h₁+2h₂, ...` mod capacity, where h₂ is a second hash of the key. Eliminates clustering but is cache-hostile.

**Deletion problem in open addressing:** You cannot simply clear a deleted slot. If you do, a subsequent lookup for a key that probed past that slot will stop at the empty slot and report "not found" incorrectly. The solution is to use a **tombstone** — a special marker that says "a key was here, keep probing" during lookups, but counts as empty for insertions.

Tombstones accumulate over time and degrade performance — eventually triggering a full rehash that clears them. This is why high-delete workloads are expensive for open-addressing tables.

**Used by:** Python dict (compact open addressing since Python 3.6), Google's Abseil flat_hash_map, many high-performance caches

### 6.3 Robin Hood Hashing

A variant of open addressing where, during insertion, if the new key's probe length (distance from its ideal slot) is greater than the incumbent key's probe length, they swap. This keeps the maximum probe length small and reduces variance in lookup time.

Lookup can short-circuit: if the current slot's key has a smaller probe length than what we've traveled, the target key cannot be further down the sequence.

**Used by:** Rust's HashMap (via hashbrown), many embedded/systems implementations

### 6.4 Choosing Between Chaining and Open Addressing

| Criterion | Chaining | Open Addressing |
|---|---|---|
| Load factor > 0.8 | Acceptable | Severe degradation |
| Cache performance | Poor (pointer chasing) | Good (contiguous array) |
| Deletion frequency | Easy, no tombstones | Tombstone accumulation |
| Memory overhead | High (pointers) | Low (flat array) |
| Concurrent access | Easier (per-bucket lock) | Harder |
| Large values | Fine (pointer indirection) | Wasteful (inline storage) |

---

## 7. Mental Models

### 7.1 The Locker Room Model

Imagine a school with 100 lockers numbered 0–99. Students are assigned lockers by their student ID modulo 100. If two students have IDs 12 and 112, they are both assigned locker 12 — a collision.

**Chaining:** Each locker has a hook with an expanding chain of padlocks. Both students can hang their padlocks on the chain. Finding your padlock means inspecting the chain until you find yours.

**Open addressing:** If locker 12 is taken, you try 13, then 14, until you find an empty one. You remember your padlock is somewhere starting from 12 — so to retrieve it, you start at 12 and try each subsequent locker until you find your combination or hit an empty one.

**Tombstone:** If you remove your padlock from locker 13 in the open-addressing school, locker 13 goes back to "apparently empty." But the student who is at locker 14 would now be invisible to anyone starting a search at locker 12 — they would stop at 13. Instead, you leave a "locker was occupied here, keep looking" sign. That is a tombstone.

### 7.2 The Pigeonhole Reality

The pigeonhole principle guarantees that collisions are unavoidable if you store more keys than buckets. But even with more buckets than keys (load factor < 1), the Birthday Paradox guarantees collisions appear much earlier than intuition suggests.

With α = 0.5 (50% full array), the expected number of comparisons per lookup is still approximately 1.5 for chaining — not 1. This is why hash tables operate well below 100% capacity.

### 7.3 The Uniformity Requirement Is Non-Negotiable

A hash function that maps "nearly all keys to buckets 0–9 and almost nothing to 10–99" turns your O(1) data structure into an O(N) data structure for those heavily-loaded buckets. The entire performance guarantee of the hash table rests on the hash function distributing keys uniformly.

This is why integer keys are often better than string keys: `hash(integer)` can be the identity function (or a simple multiply-and-shift), and for random integers this is uniform. String hashing must process every byte and mix them to avoid patterns — `"hello" → h → e → l → l → o → fold → index`.

---

## 8. Failure Scenarios

### 8.1 Python dict Degradation from Hash Clustering

```python
# This looks O(1) but can degrade in a specific CPython version
# if you use integer keys that all hash to the same slot

# In CPython, hash(int) == int for small integers
# If you use keys that are multiples of the capacity:
capacity = 8  # after several doubles, capacity might be 8
keys_that_collide = [0, 8, 16, 24, 32]  # all hash to slot 0

d = {}
for k in keys_that_collide:
    d[k] = k  # all collide at slot 0 (or same probe sequence)
```

In practice, Python's compact dict layout mitigates this. But contrived integer key sets or hash functions that produce correlated outputs can cause clustering.

**Signal:** Dict operations become measurably slower as the dict grows beyond 2/3 capacity without a resize trigger (shouldn't happen normally, but can if capacity calculation is wrong in a custom implementation).

### 8.2 Spark Hash Join OOM: The Right Table Must Fit in Memory

```python
# This join will OOM if 'users' does not fit in executor memory
events.join(users, on="user_id", how="inner")
# Spark chooses hash join if users is below spark.sql.autoBroadcastJoinThreshold
# If users is 10 GB and executor memory is 8 GB → OOM

# Diagnostic: check the physical plan
events.join(users, on="user_id").explain(mode="extended")
# Look for: BroadcastHashJoin vs SortMergeJoin in physical plan
```

Spark's hash join builds an in-memory hash table from the smaller (build) side and probes it with every row of the larger (probe) side. If the build side doesn't fit in executor memory, you get an OOM error or, with `spark.sql.autoBroadcastJoinThreshold` too high, incorrect results from spill-to-disk issues.

**Fix:** Either increase executor memory, reduce the build side (pre-filter), or let AQE choose sort-merge join automatically by setting `spark.sql.adaptive.enabled=true`.

### 8.3 Kafka Partition Skew from Non-Uniform Key Hashing

```
Topic: user_events, 8 partitions

Keys and their partition assignments (hash(key) % 8):
  "user_1"  → partition 0  ← 10M messages/day
  "user_2"  → partition 0  ← 8M messages/day  (collision in partition space)
  "user_3"  → partition 3  ← 50K messages/day
  ...

Result:
  Partition 0: 18M messages/day  ← hot partition
  Partition 3: 50K messages/day  ← cold partition
  
Consumer for partition 0: 360× more work than partition 3
```

Kafka uses `murmur2(key) % numPartitions` by default. If your key distribution is non-uniform (e.g., a few users generate 99% of events), the hash table's uniformity assumption fails at the partition level — not in the hash computation, but in the key distribution itself.

**Fix:** Either use a more granular key (e.g., `user_id:event_type` or `user_id:timestamp_bucket`) to spread load, or accept skew and over-provision consumers for hot partitions.

### 8.4 Consistent Hashing Mismatch After Node Removal

```
Before node removal (3 nodes):
  Node A owns tokens: [0, 120]
  Node B owns tokens: [121, 240]
  Node C owns tokens: [241, 360]

Simple hash(key) % 3:
  hash("user_1") % 3 = 1 → Node B
  hash("user_2") % 3 = 2 → Node C

After node B removed, simple hash:
  hash("user_1") % 3 = 1 → Node C  ← REMAPPED (cache miss / routing error)
  hash("user_2") % 3 = 2 → Node A  ← REMAPPED

All keys remapped → 100% cache invalidation → thundering herd → outage
```

This is why distributed caches and Kafka use consistent hashing rather than simple modulo hashing. Section 11 covers this in depth.

### 8.5 Hash Flooding Attack on User-Supplied Keys

```python
import sys

# Python < 3.3: hash("abc") is deterministic across processes
# An attacker who knows the hash function can craft inputs
# that all map to the same bucket:

# Malicious HTTP POST with many parameters, all hashing to slot 0:
# param_0=value, param_DEADBEEF=value, param_CAFEBABE=value, ...
# Server parses params into a dict → O(N²) dict operations
# 10,000 crafted params → 100,000,000 comparisons → CPU saturation

# Python 3.3+ fix: PYTHONHASHSEED randomizes the seed per process
import os
print(os.environ.get('PYTHONHASHSEED', 'random'))  # 'random' by default
```

Python's hash randomization (enabled by default since 3.3) means `hash("abc")` returns a different value each process startup. This prevents adversarial key crafting but means you cannot rely on `hash()` values being stable across Python processes — don't serialize them or use them as database keys.

---

## 9. Data Engineering Connections

### 9.1 Spark Hash Join: The O(N+M) Join

The hash join algorithm, which is the reason hash tables exist in Spark, works as follows:

**Phase 1 — Build:** Iterate over the smaller relation (M rows). For each row, compute `hash(join_key)` and insert the row into an in-memory hash table. Cost: O(M) time, O(M) space.

**Phase 2 — Probe:** Iterate over the larger relation (N rows). For each row, compute `hash(join_key)` and look up the matching row(s) in the hash table. Cost: O(N) time.

**Total:** O(N + M), versus O(N × M) for nested loop join and O(N log N + M log M) for sort-merge join.

The critical constraint is that the build-side hash table must fit in memory. For Spark's broadcast hash join, the small table is broadcast to every executor, and each executor builds a local hash table. For Spark's shuffle hash join (non-broadcast), each partition builds a partial hash table from its chunk of the build side.

```
Spark hash join configuration:
  spark.sql.autoBroadcastJoinThreshold = 10MB  # default
    → tables smaller than 10MB get broadcast hash join
  
  spark.sql.join.preferSortMergeJoin = false   # default
    → AQE can choose hash join for medium tables if they fit

  spark.sql.adaptive.enabled = true             # default Spark 3.x
    → AQE dynamically switches strategies based on actual data sizes
```

### 9.2 Aggregation: The GROUP BY Hash Table

`GROUP BY` in SQL engines (Spark, BigQuery, Postgres) is implemented as a hash table:

```
SELECT user_id, COUNT(*), SUM(amount)
FROM events
GROUP BY user_id

Internal hash table: { user_id → (count_accumulator, sum_accumulator) }

For each row:
  bucket = hash(user_id) % capacity
  if bucket not found: insert (1, amount)
  else: update (count+1, sum+amount)

Final pass: iterate all buckets → emit (user_id, count, sum)
```

This is O(N) in the number of input rows and O(K) in the number of distinct groups. When K exceeds available memory (too many distinct groups), the engine spills to disk and performs a multi-pass external aggregation.

### 9.3 Python dict as the Foundation of Data Engineering

Almost every Python data engineering pattern depends on dict performance:

```python
# GroupBy aggregation — O(N) via dict
from collections import defaultdict
grouped = defaultdict(list)
for row in events:
    grouped[row['user_id']].append(row)  # O(1) per insert

# Lookup table join — O(N+M) via dict
user_lookup = {u['id']: u for u in users}  # O(M) build
enriched = [
    {**event, **user_lookup.get(event['user_id'], {})}
    for event in events  # O(1) per lookup
]

# Deduplication — O(N) via set (hash table without values)
seen = set()
deduped = []
for row in events:
    key = (row['user_id'], row['event_date'])
    if key not in seen:
        seen.add(key)
        deduped.append(row)
```

Each of these patterns is O(N) precisely because dict provides O(1) amortized insert and lookup. Replace the dict with a list scan and each becomes O(N²).

---

## 10. Python Internals: How dict Works

### 10.1 The Compact Dict (Python 3.6+)

Python's dict changed fundamentally in CPython 3.6. Before 3.6, the backing array stored (hash, key, value) triples directly and was kept sparse. From 3.6 onward, the design splits into two arrays:

```
Compact dict structure (Python 3.6+):

Indices array (sparse, small integers):
  [3, -1, 0, -1, 1, -1, -1, 2]
   ↑                            ← slot index (or -1 for empty)
   
Entries array (dense, contiguous):
  index 0: (hash("bob"),   "bob",   42)
  index 1: (hash("carol"), "carol", 11)
  index 2: (hash("dave"),  "dave",   3)
  index 3: (hash("alice"), "alice", 99)
  
Lookup "alice":
  1. Compute i = hash("alice") % len(indices_array) = say 2
  2. indices_array[2] = 0  ← entry is at entries[0]
  3. entries[0] = (hash_alice, "alice", 99) → compare key → match → return 99
```

**Why this matters:**
- Iteration over a dict is now in **insertion order** (guaranteed since Python 3.7) because the entries array is dense and ordered by insertion
- Memory footprint is smaller: the indices array uses 1-byte integers for small dicts, 2-byte for medium, 4-byte for large — versus full (hash, key, value) triples in every slot
- The dense entries array is cache-friendly for iteration

### 10.2 Python Hash Function for Common Types

```python
# Integers: hash(n) == n for most small integers
hash(0)    # → 0
hash(1)    # → 1
hash(42)   # → 42
hash(-1)   # → -2  (CPython special case: -1 is reserved for "error")

# Floats: hash(float) == hash(int) if float is integral
hash(1.0)  # → 1   (same as hash(1))
hash(1.5)  # → 1152921504606846977  (arbitrary large integer)

# Strings: SipHash-1-3 with random seed (PYTHONHASHSEED)
# Different every process, same within one process
hash("hello")  # → varies between runs, but stable within run

# Tuples: hash of the combined hashes of elements
hash(("alice", 42))  # depends on hash("alice") and hash(42)

# Lists: UNHASHABLE — mutable objects cannot be dict keys
hash([1, 2, 3])  # → TypeError: unhashable type: 'list'
```

### 10.3 Load Factor and Resize Triggers

```python
import sys

d = {}
sizes = []
for i in range(100):
    d[i] = i
    sizes.append(sys.getsizeof(d))

# You will see size jumps at specific insertion counts:
# 0 keys → 232 bytes (empty dict, 8 slots)
# 6 keys → resize to 16 slots  (6/8 ≈ 0.75 > 2/3 threshold → resize)
# Actually CPython resizes when used * 3 > alloc * 2 (i.e., at 2/3 capacity)
```

The resize trigger is when `used * 3 > alloc * 2`, which corresponds to load factor > 2/3 ≈ 0.667.

### 10.4 Custom __hash__ and __eq__

For user-defined classes to work as dict keys, they must implement `__hash__` and `__eq__` correctly:

```python
class UserId:
    def __init__(self, value: str):
        self.value = value
    
    def __hash__(self) -> int:
        # Must be consistent with __eq__:
        # if a == b then hash(a) == hash(b) (REQUIRED)
        # if hash(a) == hash(b) then a may or may not == b (collision)
        return hash(self.value)
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, UserId):
            return NotImplemented
        return self.value == other.value

# CRITICAL CONTRACT: objects that compare equal must have the same hash
# Violating this silently corrupts dict lookups:
class BadId:
    def __init__(self, v): self.v = v
    def __eq__(self, other): return self.v == other.v
    # __hash__ not defined → inherits object's id-based hash → violates contract
    # Two BadId("x") objects would be equal but have different hashes
    # → dict lookup fails to find the second one even though it's "equal"
```

---

## 11. Consistent Hashing

### 11.1 The Scaling Problem

Simple modulo hashing — `hash(key) % N` — breaks when N changes. If you remove one node from a 10-node cluster, the formula becomes `hash(key) % 9`. Almost all keys map to a different node than before. For a distributed cache, this means 100% cache miss immediately after any topology change — a thundering herd that can take down a backend.

Consistent hashing solves this by ensuring that when a node is added or removed, only `K/N` keys need to be remapped on average (where K is total keys, N is number of nodes), not all K keys.

### 11.2 The Hash Ring

```
Hash ring (modular space 0 → 2^32 - 1, visualized as a circle):

                    0
                    │
          340   ────┼────  60
                    │
         300   ┌────┼────┐  100
                │   │    │
     ───Node A──┤   │    ├──Node B───
         (pos   │   │    │  (pos
         280)   └────┼────┘  120)
                    │
          240   ────┼────  160
                    │
                   200
                Node C
                (pos 200)

Key assignment: walk clockwise from hash(key) until you hit a node
  hash("user_1") = 50  → clockwise to 60? No node at 60... → 120 → Node B
  hash("user_2") = 150 → clockwise → 200 → Node C
  hash("user_3") = 220 → clockwise → 280 → Node A
```

### 11.3 Node Addition and Removal

```
Add Node D at position 100 on the ring:

Before: keys in [61, 120] → Node B
After:  keys in [61, 100] → Node D (NEW)
        keys in [101, 120] → Node B (unchanged)

Only the keys in [61, 100] need to move from B to D.
Everything else: unchanged.
Expected remapped fraction: 1/(N+1) = 1/4 = 25% instead of 100%

Remove Node B (at position 120):

Before: keys in [101, 120] → Node B
After:  keys in [101, 120] → Node C (next clockwise)

Only Node B's keys need remapping. ~1/N = 1/3 of all keys.
```

### 11.4 Virtual Nodes (Vnodes)

A single node per physical server creates uneven load distribution — if nodes fall at positions 100, 120, 280, the arc lengths (and thus key counts) are wildly unequal. **Virtual nodes** place each physical server at multiple positions on the ring.

```
Without vnodes (3 physical nodes):
  Node A at position 280  → owns arc [201, 280] = 80 units
  Node B at position 120  → owns arc [ 61, 120] = 60 units
  Node C at position 200  → owns arc [121, 200] = 80 units
  (position 0-60 and 281-360 also assigned clockwise)

With vnodes (3 physical nodes × 3 vnodes each = 9 ring positions):
  Node A at positions:  30,  150,  280
  Node B at positions:  70,  200,  320
  Node C at positions: 100,  240,  360
  → each node owns ~3 × (360/9) = ~120 arc units
  → load is approximately uniform regardless of physical placement
```

Cassandra uses 256 vnodes per node by default. Kafka does not use consistent hashing natively — it uses `murmur2(key) % numPartitions` — but Kafka Streams uses consistent assignment for stream partitions.

### 11.5 Consistent Hashing in Data Engineering

**Distributed cache (Memcached, Redis Cluster):**
- Keys are assigned to nodes via consistent hashing
- When a node is added, only 1/N of keys are remapped (no thundering herd)
- Redis Cluster uses a fixed 16,384 hash slots rather than a true ring; each node owns a range of slots

**Kafka partition assignment:**
- Default partitioner: `murmur2(key.getBytes()) % numPartitions`
- This is NOT consistent — adding partitions remaps all keys
- For order-sensitive consumers (e.g., CDC change events per entity), adding partitions requires a planned cutover, not live rebalancing

**BigQuery slot assignment:**
- BigQuery uses consistent hash sharding internally for DML operations (MERGE, UPDATE) to route row mutations to the correct slot

---

## 12. Code Toolkit

### 12.1 `hash_table.py` — From-Scratch Hash Table Implementation

```python
"""
hash_table.py — A from-scratch hash table implementation.

Implements open addressing with linear probing and tombstones.
Shows every mechanism: hashing, probing, resizing, tombstone handling.

Run directly to see demo output.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, Hashable, TypeVar, Optional, Iterator
import math

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

_EMPTY = object()    # sentinel: slot has never been occupied
_TOMBSTONE = object()  # sentinel: slot was occupied, then deleted


@dataclass
class _Slot:
    """A single slot in the backing array."""
    key: object = _EMPTY
    value: object = _EMPTY

    def is_empty(self) -> bool:
        return self.key is _EMPTY

    def is_tombstone(self) -> bool:
        return self.key is _TOMBSTONE

    def is_occupied(self) -> bool:
        return not self.is_empty() and not self.is_tombstone()


class HashTable(Generic[K, V]):
    """
    Open-addressing hash table with linear probing.
    
    Shows:
    - O(1) amortized insert/lookup/delete
    - Geometric doubling on resize
    - Tombstone handling for deletes
    - Load factor enforcement
    
    NOT thread-safe. For demonstration only.
    """

    INITIAL_CAPACITY = 8
    MAX_LOAD_FACTOR = 0.6667  # 2/3, same as CPython dict

    def __init__(self) -> None:
        self._capacity = self.INITIAL_CAPACITY
        self._slots: list[_Slot] = [_Slot() for _ in range(self._capacity)]
        self._size = 0           # occupied (excludes tombstones)
        self._used = 0           # occupied + tombstones
        self._resize_count = 0
        self._probe_total = 0    # total probes (for stats)
        self._op_count = 0       # total operations

    # ─── Public interface ────────────────────────────────────────────────

    def set(self, key: K, value: V) -> None:
        """Insert or update key→value. O(1) amortized."""
        self._maybe_resize()
        slot_index, found = self._find_slot(key)
        slot = self._slots[slot_index]
        if found:
            slot.value = value  # update
        else:
            if slot.is_tombstone():
                self._used += 1  # tombstone → occupied (already counted in _used)
            else:
                self._used += 1  # empty → occupied
            slot.key = key
            slot.value = value
            self._size += 1

    def get(self, key: K, default: V = None) -> Optional[V]:
        """Retrieve value for key. O(1) average."""
        slot_index, found = self._find_slot(key)
        if found:
            return self._slots[slot_index].value
        return default

    def delete(self, key: K) -> bool:
        """Delete key. Returns True if key existed. O(1) average."""
        slot_index, found = self._find_slot(key)
        if not found:
            return False
        slot = self._slots[slot_index]
        slot.key = _TOMBSTONE
        slot.value = _EMPTY
        self._size -= 1
        # Note: _used unchanged (tombstone still counts as "used" for load factor)
        return True

    def __contains__(self, key: K) -> bool:
        _, found = self._find_slot(key)
        return found

    def __len__(self) -> int:
        return self._size

    def items(self) -> Iterator[tuple[K, V]]:
        for slot in self._slots:
            if slot.is_occupied():
                yield slot.key, slot.value

    # ─── Internal mechanics ─────────────────────────────────────────────

    def _hash(self, key: K) -> int:
        """Map key to initial slot index."""
        return hash(key) % self._capacity

    def _find_slot(self, key: K) -> tuple[int, bool]:
        """
        Probe from the initial hash slot until:
        - We find the key (found=True)
        - We find an EMPTY slot (key doesn't exist, found=False)
        
        Returns (slot_index, found).
        Stops at tombstones (key might be further along the probe chain).
        """
        start = self._hash(key)
        first_tombstone = None
        probes = 0

        for i in range(self._capacity):
            index = (start + i) % self._capacity
            probes += 1
            slot = self._slots[index]

            if slot.is_empty():
                # Key cannot be beyond an empty slot
                self._probe_total += probes
                self._op_count += 1
                # Return first tombstone index if found (for insertion reuse)
                return (first_tombstone if first_tombstone is not None else index), False

            if slot.is_tombstone():
                # Record first tombstone for potential insertion reuse
                if first_tombstone is None:
                    first_tombstone = index
                continue

            if slot.key == key:
                self._probe_total += probes
                self._op_count += 1
                return index, True

        # Table is full (only tombstones) — should never happen if load factor maintained
        raise RuntimeError("Hash table is full (tombstone overflow)")

    def _maybe_resize(self) -> None:
        """Resize if load factor (including tombstones) exceeds threshold."""
        if self._used / self._capacity >= self.MAX_LOAD_FACTOR:
            self._resize()

    def _resize(self) -> None:
        """Double capacity and rehash all live entries."""
        old_slots = self._slots
        self._capacity *= 2
        self._slots = [_Slot() for _ in range(self._capacity)]
        self._size = 0
        self._used = 0
        self._resize_count += 1

        for slot in old_slots:
            if slot.is_occupied():
                self.set(slot.key, slot.value)

    # ─── Diagnostic stats ───────────────────────────────────────────────

    def stats(self) -> dict:
        """Return performance statistics."""
        return {
            "size": self._size,
            "capacity": self._capacity,
            "load_factor": round(self._size / self._capacity, 3),
            "resize_count": self._resize_count,
            "avg_probes": round(
                self._probe_total / self._op_count if self._op_count else 0, 2
            ),
        }


# ─── Demo ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Hash Table Demo ===\n")

    ht: HashTable[str, int] = HashTable()

    # Insert
    users = [("alice", 1), ("bob", 2), ("carol", 3), ("dave", 4),
             ("eve", 5), ("frank", 6), ("grace", 7)]
    for name, uid in users:
        ht.set(name, uid)
        print(f"  set({name!r}, {uid}) → stats: {ht.stats()}")

    print(f"\nAfter inserts: {ht.stats()}")

    # Lookup
    print(f"\nget('alice') = {ht.get('alice')}")
    print(f"get('zara')  = {ht.get('zara', 'NOT FOUND')}")

    # Delete + tombstone effect
    print(f"\ndelete('bob') = {ht.delete('bob')}")
    print(f"get('bob') after delete = {ht.get('bob', 'NOT FOUND')}")
    print(f"After delete: {ht.stats()}")

    # Verify all remaining items
    print("\nAll items:")
    for k, v in ht.items():
        print(f"  {k!r}: {v}")
```

### 12.2 `consistent_hash.py` — Consistent Hash Ring

```python
"""
consistent_hash.py — Consistent hash ring with virtual nodes.

Demonstrates:
- O(log N) key assignment (binary search on sorted ring)
- Minimal remapping on node addition/removal
- Virtual node distribution analysis

Run directly for demo.
"""

from __future__ import annotations
import hashlib
import bisect
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class ConsistentHashRing:
    """
    Consistent hash ring using virtual nodes.
    
    Uses SHA-256 (truncated to 32 bits) as the ring hash function.
    Each physical node appears at `vnodes` positions on the ring.
    Key assignment: walk clockwise (bisect_right) to find the node.
    """
    vnodes: int = 150  # virtual nodes per physical node

    _ring: list[int] = field(default_factory=list, init=False)  # sorted ring positions
    _position_to_node: dict[int, str] = field(default_factory=dict, init=False)

    def _hash(self, key: str) -> int:
        """Map a string key to a 32-bit ring position."""
        digest = hashlib.sha256(key.encode()).digest()
        return int.from_bytes(digest[:4], byteorder='big')

    def add_node(self, node: str) -> None:
        """Add a physical node to the ring at `vnodes` positions."""
        for i in range(self.vnodes):
            position = self._hash(f"{node}:vnode:{i}")
            self._ring.append(position)
            self._position_to_node[position] = node
        self._ring.sort()

    def remove_node(self, node: str) -> None:
        """Remove a physical node and all its virtual positions."""
        for i in range(self.vnodes):
            position = self._hash(f"{node}:vnode:{i}")
            self._ring.remove(position)
            del self._position_to_node[position]

    def get_node(self, key: str) -> str | None:
        """Return the node responsible for key. O(log N) via binary search."""
        if not self._ring:
            return None
        position = self._hash(key)
        idx = bisect.bisect_right(self._ring, position)
        # Wrap around: if past last position, go to first (ring is circular)
        if idx == len(self._ring):
            idx = 0
        return self._position_to_node[self._ring[idx]]

    def distribution(self, keys: list[str]) -> dict[str, int]:
        """Return count of keys assigned to each node."""
        counts: dict[str, int] = defaultdict(int)
        for key in keys:
            node = self.get_node(key)
            if node:
                counts[node] += 1
        return dict(counts)

    def remapping_ratio(self, keys: list[str], removed_node: str) -> float:
        """
        Calculate what fraction of keys get remapped when `removed_node` is removed.
        Compares assignments before and after removal.
        """
        before = {key: self.get_node(key) for key in keys}
        self.remove_node(removed_node)
        after = {key: self.get_node(key) for key in keys}
        self.add_node(removed_node)  # restore

        remapped = sum(1 for k in keys if before[k] != after[k])
        return remapped / len(keys)


if __name__ == "__main__":
    import random
    random.seed(42)

    print("=== Consistent Hash Ring Demo ===\n")

    ring = ConsistentHashRing(vnodes=150)
    nodes = ["node-A", "node-B", "node-C"]
    for n in nodes:
        ring.add_node(n)

    # Generate test keys
    keys = [f"user_{i}" for i in range(10_000)]

    # Distribution before any topology change
    dist = ring.distribution(keys)
    print("Distribution (10,000 keys, 3 nodes, 150 vnodes each):")
    for node, count in sorted(dist.items()):
        bar = "█" * (count // 100)
        pct = count / len(keys) * 100
        print(f"  {node}: {count:,} keys ({pct:.1f}%)  {bar}")

    # Remapping on node removal
    ratio = ring.remapping_ratio(keys, "node-B")
    print(f"\nRemapping ratio when node-B removed: {ratio:.1%}")
    print(f"Expected (1/N = 1/3):                {1/3:.1%}")

    # Simple modulo for comparison
    print("\nSimple modulo hash remapping (for comparison):")

    def modulo_assign(key: str, n: int) -> int:
        h = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return h % n

    before_modulo = {k: modulo_assign(k, 3) for k in keys}
    after_modulo = {k: modulo_assign(k, 2) for k in keys}  # after removing 1 node
    remapped_modulo = sum(1 for k in keys if before_modulo[k] != after_modulo[k])
    print(f"  Remapped: {remapped_modulo:,}/{len(keys):,} = {remapped_modulo/len(keys):.1%}")
    print(f"  (vs consistent hash: {ratio:.1%})")
```

### 12.3 `spark_join_analyzer.py` — Hash Join vs Sort-Merge Join Decision

```python
"""
spark_join_analyzer.py — Analyze Spark join strategy choices.

For each join, determines whether hash join or sort-merge join is appropriate
based on table sizes, available memory, and Spark configuration.

Does NOT require a running Spark cluster — uses simulated table metadata.
"""

from __future__ import annotations
from dataclasses import dataclass
import math


@dataclass
class TableMeta:
    """Metadata for a Spark table (simulated, no actual Spark required)."""
    name: str
    row_count: int
    row_size_bytes: int = 200  # average serialized row size

    @property
    def size_bytes(self) -> int:
        return self.row_count * self.row_size_bytes

    @property
    def size_mb(self) -> float:
        return self.size_bytes / (1024 ** 2)

    @property
    def size_gb(self) -> float:
        return self.size_bytes / (1024 ** 3)


@dataclass
class SparkConfig:
    """Simulated Spark configuration."""
    broadcast_threshold_mb: float = 10.0      # spark.sql.autoBroadcastJoinThreshold
    executor_memory_gb: float = 8.0
    executor_cores: int = 4
    aqe_enabled: bool = True


def analyze_join(
    left: TableMeta,
    right: TableMeta,
    cfg: SparkConfig,
) -> dict:
    """
    Recommend a join strategy and explain why.
    
    Returns a dict with:
      - strategy: "broadcast_hash_join", "shuffle_hash_join", or "sort_merge_join"
      - build_side: which table builds the hash table
      - reason: explanation
      - risks: list of potential issues
    """
    smaller, larger = (right, left) if right.size_mb <= left.size_mb else (left, right)
    risks = []

    # --- Broadcast hash join ---
    if smaller.size_mb <= cfg.broadcast_threshold_mb:
        return {
            "strategy": "broadcast_hash_join",
            "build_side": smaller.name,
            "reason": (
                f"{smaller.name} is {smaller.size_mb:.1f} MB ≤ "
                f"broadcast threshold {cfg.broadcast_threshold_mb} MB. "
                f"Will be broadcast to all executors and joined in memory."
            ),
            "complexity": "O(N + M)",
            "memory_needed_gb": smaller.size_mb / 1024,
            "risks": [],
        }

    # --- Shuffle hash join (non-broadcast) ---
    # Each executor handles one partition of each table.
    # Executor must hold one partition's worth of the build side in memory.
    # Assume default 200 partitions.
    DEFAULT_PARTITIONS = 200
    partition_size_gb = smaller.size_gb / DEFAULT_PARTITIONS
    available_memory_gb = cfg.executor_memory_gb * 0.3  # 30% for join

    if partition_size_gb <= available_memory_gb:
        if smaller.size_gb > 2.0:
            risks.append(
                f"Build-side partition is {partition_size_gb:.2f} GB per executor. "
                f"May OOM if partitions are skewed."
            )
        return {
            "strategy": "shuffle_hash_join",
            "build_side": smaller.name,
            "reason": (
                f"Neither table fits in broadcast threshold. "
                f"{smaller.name} ({smaller.size_gb:.1f} GB) is smaller; "
                f"one partition ({partition_size_gb:.2f} GB) fits in executor memory "
                f"({available_memory_gb:.2f} GB available)."
            ),
            "complexity": "O(N + M) per partition",
            "memory_needed_gb": partition_size_gb,
            "risks": risks,
        }

    # --- Sort-merge join ---
    risks.append(
        f"Sort-merge join requires shuffling BOTH tables ({left.size_gb:.1f} GB + "
        f"{right.size_gb:.1f} GB). Shuffle is expensive."
    )
    if cfg.aqe_enabled:
        risks.append("AQE enabled: may dynamically coalesce partitions to reduce shuffle overhead.")

    return {
        "strategy": "sort_merge_join",
        "build_side": "N/A (no build phase)",
        "reason": (
            f"Neither table fits in memory for hash join. "
            f"Sort-merge join required: sort both tables on join key, "
            f"then merge in O(N log N + M log M)."
        ),
        "complexity": "O(N log N + M log M)",
        "memory_needed_gb": None,
        "risks": risks,
    }


if __name__ == "__main__":
    cfg = SparkConfig(
        broadcast_threshold_mb=10.0,
        executor_memory_gb=8.0,
        aqe_enabled=True,
    )

    scenarios = [
        ("events 1B rows", "users 50K rows",
         TableMeta("events", 1_000_000_000), TableMeta("users", 50_000)),
        ("events 1B rows", "accounts 5M rows",
         TableMeta("events", 1_000_000_000), TableMeta("accounts", 5_000_000)),
        ("events 1B rows", "large_dim 50M rows",
         TableMeta("events", 1_000_000_000), TableMeta("large_dim", 50_000_000)),
    ]

    for label_l, label_r, left, right in scenarios:
        result = analyze_join(left, right, cfg)
        print(f"\n{'='*60}")
        print(f"Join: {label_l} ⋈ {label_r}")
        print(f"  Left:  {left.size_gb:.2f} GB")
        print(f"  Right: {right.size_gb:.2f} GB")
        print(f"  Strategy:   {result['strategy']}")
        print(f"  Build side: {result['build_side']}")
        print(f"  Complexity: {result['complexity']}")
        print(f"  Reason: {result['reason']}")
        if result['risks']:
            print(f"  Risks:")
            for r in result['risks']:
                print(f"    ⚠ {r}")
```

---

## 13. Hands-On Labs

### Lab 1: Verify Load Factor and Resize Behavior

**Goal:** Observe empirically how Python dict resizes and how load factor affects performance.

```python
# lab1_dict_load_factor.py
"""
Measures Python dict size and lookup time at different load factors.
Shows the amortized O(1) cost of insertions and the resize spike.
"""
import sys
import time
import random

def measure_dict_growth(max_keys: int = 500) -> None:
    d = {}
    prev_size = sys.getsizeof(d)
    
    print(f"{'Keys':>6}  {'Size(B)':>8}  {'Resize':>6}  {'Insert_ns':>10}")
    print("-" * 40)
    
    for i in range(max_keys):
        start = time.perf_counter_ns()
        d[i] = i
        elapsed = time.perf_counter_ns() - start
        
        curr_size = sys.getsizeof(d)
        resized = curr_size != prev_size
        
        if resized or i < 10 or i % 50 == 0:
            print(f"{i+1:>6}  {curr_size:>8}  {'YES' if resized else '':>6}  {elapsed:>10}")
        prev_size = curr_size

def measure_lookup_time(sizes: list[int]) -> None:
    print(f"\n{'Dict_size':>10}  {'Avg_lookup_ns':>14}")
    print("-" * 28)
    
    for n in sizes:
        d = {i: i for i in range(n)}
        keys = list(d.keys())
        random.shuffle(keys)
        
        start = time.perf_counter_ns()
        for k in keys[:1000]:
            _ = d[k]
        elapsed = time.perf_counter_ns() - start
        
        print(f"{n:>10,}  {elapsed/1000:>14.1f}")

if __name__ == "__main__":
    print("=== Dict Growth and Resize ===")
    measure_dict_growth(200)
    
    print("\n=== Lookup Time at Different Sizes ===")
    measure_lookup_time([100, 1_000, 10_000, 100_000, 1_000_000])
```

**Expected result:** Lookup time should be approximately constant across all sizes (O(1)). Resize events should show a spike in insert time.

---

### Lab 2: Consistent Hash vs Modulo Hash — Remapping Under Topology Change

**Goal:** Demonstrate empirically that consistent hashing remaps ~1/N keys on node removal versus ~(N-1)/N for modulo hashing.

```python
# lab2_consistent_vs_modulo.py
"""
Compares key remapping between consistent hashing and simple modulo hashing
when a node is added or removed from a 3-node cluster.
"""
import hashlib
import bisect
import random
from collections import defaultdict

def modulo_assign(key: str, n_nodes: int) -> int:
    h = int(hashlib.sha256(key.encode()).hexdigest(), 16)
    return h % n_nodes

def build_ring(nodes: list[str], vnodes: int = 150) -> tuple[list[int], dict[int, str]]:
    ring = []
    pos_to_node = {}
    for node in nodes:
        for i in range(vnodes):
            pos = int(hashlib.sha256(f"{node}:vnode:{i}".encode()).hexdigest()[:8], 16)
            ring.append(pos)
            pos_to_node[pos] = node
    ring.sort()
    return ring, pos_to_node

def ring_assign(key: str, ring: list[int], pos_to_node: dict[int, str]) -> str:
    h = int(hashlib.sha256(key.encode()).hexdigest()[:8], 16)
    idx = bisect.bisect_right(ring, h) % len(ring)
    return pos_to_node[ring[idx]]

if __name__ == "__main__":
    random.seed(42)
    keys = [f"key_{i}" for i in range(10_000)]
    nodes_3 = ["node-A", "node-B", "node-C"]
    nodes_2 = ["node-A", "node-C"]  # node-B removed

    # Modulo: 3 nodes → 2 nodes
    before_mod = [modulo_assign(k, 3) for k in keys]
    after_mod  = [modulo_assign(k, 2) for k in keys]
    remapped_mod = sum(b != a for b, a in zip(before_mod, after_mod))
    print(f"Modulo hash   — remapped: {remapped_mod:,}/{len(keys):,} = {remapped_mod/len(keys):.1%}")

    # Consistent: 3 nodes → 2 nodes (remove node-B)
    ring3, p2n3 = build_ring(nodes_3)
    ring2, p2n2 = build_ring(nodes_2)
    before_ch = [ring_assign(k, ring3, p2n3) for k in keys]
    after_ch  = [ring_assign(k, ring2, p2n2) for k in keys]
    remapped_ch = sum(b != a for b, a in zip(before_ch, after_ch))
    print(f"Consistent    — remapped: {remapped_ch:,}/{len(keys):,} = {remapped_ch/len(keys):.1%}")
    print(f"Theory (1/N=1/3):                   {1/3:.1%}")

    # Distribution evenness
    dist = defaultdict(int)
    for assignment in before_ch:
        dist[assignment] += 1
    print(f"\nConsistent hash distribution (3 nodes):")
    for node in sorted(dist):
        print(f"  {node}: {dist[node]:,} ({dist[node]/len(keys):.1%})")
```

---

### Lab 3: Hash Table Failure Mode — Adversarial Key Input

**Goal:** Demonstrate how non-uniform key distributions degrade hash table performance.

```python
# lab3_hash_collision_stress.py
"""
Demonstrates hash table degradation under adversarial key patterns.
Compares lookup time for uniform keys vs skewed keys vs integer multiples.
"""
import time
import random
import string

def random_string(length: int = 16) -> str:
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def measure_dict_ops(keys: list, label: str) -> None:
    d = {}
    
    # Build phase
    t0 = time.perf_counter()
    for k in keys:
        d[k] = 1
    build_ns = (time.perf_counter() - t0) * 1e9 / len(keys)
    
    # Lookup phase (all keys present)
    t0 = time.perf_counter()
    for k in keys:
        _ = d[k]
    lookup_ns = (time.perf_counter() - t0) * 1e9 / len(keys)
    
    print(f"  {label:<30}  build: {build_ns:>8.1f} ns/op  lookup: {lookup_ns:>8.1f} ns/op")

if __name__ == "__main__":
    N = 100_000
    random.seed(42)
    
    print(f"=== Hash Table Performance Under Different Key Distributions ===")
    print(f"N = {N:,} keys\n")

    # Uniform random strings
    uniform_keys = [random_string(16) for _ in range(N)]
    measure_dict_ops(uniform_keys, "Uniform random strings")

    # Sequential integers (good distribution for CPython)
    sequential_keys = list(range(N))
    measure_dict_ops(sequential_keys, "Sequential integers")

    # Large integer multiples — can cause clustering in some implementations
    # (not CPython's fault — just demonstrates the concept)
    step = 2**16  # large power of 2 step
    step_keys = [i * step for i in range(N)]
    measure_dict_ops(step_keys, "Integer multiples of 2^16")

    # String keys with common prefix (common in user IDs)
    prefixed_keys = [f"user_{i:08d}" for i in range(N)]
    measure_dict_ops(prefixed_keys, "Prefixed string keys")
    
    print("\nNote: CPython dict uses hash randomization and a well-tuned")
    print("probe sequence — degradation is minimal for most key types.")
    print("Adversarial inputs matter more for naive implementations.")
```

---

## 14. Interview Q&A

**Q1: Explain how a hash table achieves O(1) average-case lookup. Why is it O(1) amortized for insertion, not just O(1)?**

The O(1) average lookup comes from two things working together: a hash function that distributes keys uniformly across an array of N slots, and a load factor kept below a threshold so that the expected number of collisions per bucket is bounded by a small constant. If the hash function sends your key to slot 42, you check slot 42 (or its probe sequence or chain). With a load factor of 0.67, the expected chain length is 0.67 — so on average you inspect slightly more than one entry. That expected constant work gives you O(1).

Insertion is O(1) amortized, not O(1) strictly, because of resizing. When the load factor exceeds the threshold, you must allocate a new array twice as large and re-hash every existing key into it. That single resize costs O(N). But it only happens when the table has grown to N keys — and the previous resize happened at N/2 keys, which cost O(N/2). If you total all resize costs from 0 to N insertions, the sum is N + N/2 + N/4 + ... = 2N, making each insertion O(1) amortized via the same geometric series argument as Python list append. The key point in interviews: "amortized O(1)" acknowledges that individual insertions can be O(N), but their average cost across N operations is O(1).

**Q2: Python's dict uses open addressing. Java's HashMap uses chaining. Walk me through the trade-offs and explain why each language made that choice.**

Open addressing stores all data in a flat array, so cache behavior is excellent — probing adjacent slots means consecutive cache-line accesses. There are no pointer-chasing overheads. Python's CPython implementation (since 3.6) uses a compact split design: a sparse indices array and a dense entries array, which is even more cache-friendly for iteration since entries are stored contiguously in insertion order. The downside is that load factor must be kept low (2/3 in CPython) to prevent probe chain explosion, and deletions leave tombstones that must be cleared on resize.

Java's HashMap uses chaining with each bucket holding a linked list (or a red-black tree for chains of length > 8, since Java 8). Chaining degrades gracefully at high load factors — you can push the load factor toward 1.0 without a performance cliff, though every node in the chain is a separate heap allocation and a separate cache miss. Java chose chaining partly because the JVM's object model makes pointer-based structures natural, and because Java HashMap is general-purpose and must handle high-delete workloads well where tombstones would accumulate.

The practical difference for data engineers: a Python dict with 10 million entries and random access is extremely fast because all data is in a flat array in L3/DRAM linearly. A Java HashMap doing the same thing pays extra per lookup for the pointer dereference from bucket to the first chain node. This matters when you're building an in-memory lookup table in a Spark executor.

**Q3: You're designing a Kafka-backed event processing system with 6 partitions. Your team wants to add 2 more partitions to handle increased load. What are the consequences for consumers that rely on key-based partitioning?**

Kafka's default partitioner assigns a message to a partition using `murmur2(key.getBytes()) % numPartitions`. This is simple modulo hashing, not consistent hashing. When numPartitions changes from 6 to 8, the mapping changes for every key — `hash(key) % 6` and `hash(key) % 8` produce different results for the same key.

The consequence depends on what the consumer does with partition assignment. If consumers are processing events in order per key — for example, a CDC consumer maintaining a per-user state machine — a key that was always on partition 3 may now be on partition 5. Any in-flight messages on partition 3 for that key will be processed before the consumer moves to partition 5, which means you need to ensure partition 3 is fully drained before consumers start reading partition 5. Otherwise you have out-of-order processing.

For a stateful stream processor (Flink, Kafka Streams), adding partitions requires a planned cutover: drain all in-flight data on old partitions, take a savepoint/checkpoint, repartition, restart with the new partition count. This is not a live-reconfiguration operation. At Staff level I would also flag that if the team is using Kafka for CDC (Debezium), the connector's partition assignment must be reconfigured alongside the topic partition count — the two are not automatically kept in sync.

**Q4: Describe the consistent hashing algorithm and explain exactly what problem it solves that simple modulo hashing cannot.**

Consistent hashing maps both nodes and keys to positions on a circular integer ring (usually 0 to 2^32 - 1). Each node is placed at one or more positions on the ring. To find the node responsible for a key, you hash the key to a ring position and walk clockwise until you hit a node.

The critical property: when a node is added or removed, only the keys whose clockwise walk was previously terminated by that node are remapped. In a ring with N nodes and K total keys, adding or removing one node remaps approximately K/N keys. Compare this to simple modulo hashing, where `hash(key) % N` and `hash(key) % (N-1)` produce different results for most keys — you remap approximately (N-1)/N of all keys on a single node removal, which for N=10 means 90% remapping.

The 90% remapping case is catastrophic for a distributed cache: every key that maps to a different node is a cache miss, and all those misses hit the database simultaneously. This is the thundering herd problem that takes down production systems after unexpected node failures. Consistent hashing converts a 90% cache-miss event into a 10% cache-miss event.

Virtual nodes solve the uneven arc length problem: without them, physical nodes land at random positions on the ring and some own large arcs (many keys) while others own small arcs. With 150–200 virtual nodes per physical node, the law of large numbers ensures each physical node owns approximately 1/N of the keyspace regardless of physical placement.

**Q5: You have a 500 GB events table and a 50 GB users table. A junior engineer says "let's just use a hash join." What questions do you ask, and what could go wrong?**

The first question is whether the 50 GB build-side table fits in executor memory. Spark's broadcast hash join only works for tables below `spark.sql.autoBroadcastJoinThreshold` (10 MB by default). A 50 GB table will not be broadcast. For the non-broadcast shuffle hash join, Spark partitions both tables and each executor builds a hash table from its chunk of the 50 GB table. With 200 shuffle partitions, each partition is 50 GB / 200 = 250 MB. If the executor has 8 GB of memory, that's fine for the hash table. But if data is skewed — some partitions are 2 GB while others are 50 MB — the large partition will OOM.

The second question is whether the join key has high cardinality on both sides. If users has a unique user_id, the hash table has one entry per user. If users has duplicated user_ids (which shouldn't happen for a users table but can happen for intermediate aggregations), the hash table grows unexpectedly large.

The third question is whether AQE is enabled. With `spark.sql.adaptive.enabled=true`, Spark 3.x AQE can detect that a hash join would OOM and fall back to sort-merge join mid-query. Without AQE, you need to specify the join strategy explicitly or set the thresholds correctly.

What could go wrong at scale: (1) partition skew causes OOM on hot executors while others are idle; (2) the 50 GB table expands after deserialization due to wide schemas with variable-length strings — the in-memory hash table can be 3-5× the on-disk size; (3) the probe side (500 GB) has many-to-many join keys, which means the output is much larger than the input and overwhelms the shuffle writer.

**Q6: Your team uses a Python dict as an in-memory lookup cache in a PySpark Python UDF. The UDF runs on 100 executors. Describe the memory model and the potential failure modes.**

When a Python UDF runs in PySpark, each executor spawns a Python worker process (via py4j or the Arrow-based Pandas UDF path). If you create a global dict inside the UDF function, it is re-created for every task — 100 executors × tasks_per_executor instances of the dict. If the dict is 500 MB, you need 500 MB per task slot, which is 500 MB × (executor cores) per executor, quickly exhausting Python worker memory.

The better pattern is to initialize the dict once per worker (outside the UDF function, or in an `__init__` method of a class registered as a UDF context) using a broadcast variable. Spark broadcasts the dict from the driver to every executor, and executors deserialize it once per executor JVM process — not once per task. The dict lives in the executor's Python worker memory, not the JVM heap, so it doesn't interact with the executor's managed memory pool.

Failure modes: (1) If the dict is too large to broadcast (above `spark.broadcast.blockSize` or driver OOM during serialization), broadcast fails. Typical safe limit is under 2 GB after serialization. (2) Pickle serialization of a Python dict is not zero-copy — the 500 MB dict gets pickled to bytes, broadcast across the network, and unpickled on each executor. For large dicts, PyArrow serialization (via broadcast of a pandas DataFrame then convert to dict) is 2–5× faster. (3) Hash randomization (PYTHONHASHSEED) means the pickled dict has the same key-value contents but a different internal hash seed on the receiving executor. This is fine for correctness (the unpickled dict rehashes everything with its own seed) but means you cannot serialize the raw slot array — pickle always serializes by content, not by internal structure.

---

## 15. Cross-Question Chain

**Q1 [Interviewer]: What is a hash table?**

A hash table is a data structure that maps keys to values in expected O(1) time. It works by applying a hash function to the key to compute an index into an array, then storing the key-value pair at that index. When two keys map to the same index — a collision — the table uses a collision resolution strategy, either chaining (a linked list per bucket) or open addressing (probing to a nearby slot), to store both. The O(1) guarantee holds on average when the hash function distributes keys uniformly and the load factor is kept below a threshold.

**Q2 [Interviewer]: You said "expected O(1)." What's the worst case, and how does it happen?**

Worst case is O(N) for a lookup if all N keys collide into the same bucket. With chaining, every lookup traverses a chain of length N. This happens when the hash function maps all keys to the same output — either a poorly designed hash function, or a deliberately crafted adversarial input. For string keys in Python before version 3.3, `hash("abc")` was deterministic across all processes, so an attacker who knew the hash function could construct thousands of strings that all hash to the same bucket, turning an O(1) web server parameter parse into O(N²) work — a denial-of-service attack called hash flooding. Python 3.3 introduced `PYTHONHASHSEED` randomization by default, which seeds the string hash with a random value per process, making adversarial precomputation impossible.

**Q3 [Interviewer]: How does Python's dict handle deletions?**

Python dict uses open addressing, which means all entries are stored in a flat array. If you delete a key by simply clearing its slot, subsequent lookups for keys that probed past that slot during insertion would incorrectly stop at the now-empty slot and report "not found." Python solves this with tombstones: a deleted slot is marked with a sentinel value that says "keep probing" during lookups but can be overwritten during insertions. Over time, tombstones accumulate and degrade performance because lookups must probe through them. This is why Python dict compacts itself during resize: the resize copies all live entries to a new array and drops all tombstones, fully resetting the probe chains.

**Q4 [Interviewer]: You're designing a distributed system where a Spark job builds a large lookup dict and shares it with 500 executor cores. How do you handle this?**

Use a broadcast variable. The driver serializes the dict once, stores it in the Spark broadcast manager, and executors fetch it once per executor JVM process — not once per task. On 500 executor cores across 50 executors (10 cores each), the dict is deserialized 50 times total, not 500 times. Executors cache the broadcast value in their off-heap memory (if configured) or heap, and all tasks on that executor share a reference to the same deserialized object. The critical constraint is size: the driver must be able to serialize the entire dict, which for 100 million key-value pairs of string→int can be 3–6 GB after pickle serialization overhead. If the dict is above ~2 GB, you need to either compress it (pickle protocol 4 + lz4), partition it by key range so each executor only caches its relevant shard, or replace it with a distributed lookup structure (broadcast of a Parquet file that Spark reads with predicate pushdown).

**Q5 [Interviewer]: You mention Kafka partition assignment uses `murmur2(key) % numPartitions`. Why is this not consistent hashing, and when does the difference matter?**

Modulo hashing (`hash(key) % N`) is not consistent because changing N remaps nearly every key. With N=6 and N=8, the sets `{hash(key) % 6}` and `{hash(key) % 8}` have almost no overlap. When you add 2 Kafka partitions to handle load, every key that was on partition 3 may now be on partition 5 — you cannot predict which partition a given key goes to without knowing the current partition count.

This matters in two scenarios. First, if consumers maintain ordered per-key processing (CDC, event sourcing), a key mid-flight on partition 3 may get a new message routed to partition 5 while partition 3 is still being consumed — resulting in out-of-order processing. Second, Kafka Streams stateful applications store their local state keyed by partition — if the partition changes, the state is on the wrong node after repartitioning. Consistent hashing would solve this for adding partitions: only 1/N of keys would remap. But Kafka does not implement consistent hashing natively because Kafka's partition count is a hard configuration parameter, not a dynamic ring. The practical solution for Kafka is to never repartition a topic without a planned cutover: drain, compact if needed, delete and recreate with the new partition count, replay from the source if necessary.

**Q6 [Interviewer]: The team is seeing Spark job OOM errors in hash join. Walk me through your full diagnosis and fix.**

I start by checking the physical plan with `df.explain(mode="extended")`. I'm looking for whether Spark chose `BroadcastHashJoin`, `ShuffledHashJoin`, or `SortMergeJoin`. If it chose `ShuffledHashJoin` and the build side is large, that is the immediate candidate.

Next I open the Spark UI, navigate to the failing stage, and look at task-level metrics. I want to know if the OOM is uniform (all tasks fail) or skewed (only a few tasks with large partitions fail). Uniform OOM means the build-side partition is consistently too large for executor memory. Skewed OOM means one or a few hot partitions are larger than average.

For uniform OOM on shuffle hash join: either increase executor memory, reduce the build side (filter before join, or pre-aggregate if the join key has duplicates), or force sort-merge join with `spark.sql.join.preferSortMergeJoin=true`. If AQE is already enabled and still choosing shuffle hash join, I would raise `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` to a value that reflects actual executor memory.

For skewed OOM: I check the join key distribution with `df.groupBy("join_key").count().orderBy(desc("count")).show(20)`. If a small number of keys produce most rows (e.g., 1% of user_ids are bot accounts with 10,000× more events), I handle them separately: filter out hot keys, join them with a broadcast of the hot-key lookup subset, join the remaining tail with the standard hash join, then union the results. With AQE and Spark 3.x, I can also enable `spark.sql.adaptive.skewJoin.enabled=true`, which splits skewed partitions automatically.

---

## 16. Common Misconceptions

**"O(1) means always exactly one operation."**
O(1) means bounded by a constant — and that constant can be surprisingly large. A hash table lookup involves hashing the key (proportional to key length), computing the index, a memory access (potentially a cache miss at 70 ns), and one or more key comparisons if there are collisions. "O(1) amortized" for insertion additionally means individual operations can be O(N) during resize.

**"A bigger hash table is always faster."**
Beyond a certain point, a larger hash table wastes memory and reduces cache efficiency. If the table is much larger than L3 cache (typically 8–48 MB), every lookup is a cache miss regardless of probe length. A perfectly tuned load factor of 0.3 in a 1 GB table is slower than a load factor of 0.6 in a 10 MB table if the 10 MB table fits in L3.

**"Python dict is ordered — so it's not a hash table."**
Python dict is a hash table that also maintains insertion order. Since Python 3.7, iteration over a dict returns keys in insertion order. This is a property of the compact dict design (dense entries array ordered by insertion), not a separate order data structure. The dict is still O(1) amortized for all hash table operations.

**"Consistent hashing is used by Kafka."**
Kafka uses `murmur2(key) % numPartitions` — simple modulo hashing, not consistent hashing. Kafka Streams uses consistent assignment internally for stream task distribution, but the underlying topic partition assignment is modulo-based. Redis Cluster uses 16,384 hash slots (a form of consistent hashing with fixed slot count), and Cassandra uses a genuine consistent hash ring with virtual nodes.

**"A set and a dict are different data structures."**
A Python set is a hash table that stores only keys (no values). It uses the same underlying mechanism: hash function, open addressing, load factor, resize-by-doubling. `x in some_set` is O(1) average for the same reason `d[x]` is O(1) average. Frozen sets (`frozenset`) are immutable, so they can be used as dict keys.

---

## 17. Performance Reference Card

| Operation | Python dict | Spark Hash Join | Consistent Hash Ring |
|---|---|---|---|
| Lookup | O(1) avg, O(N) worst | O(1) per row in probe phase | O(log N) via binary search |
| Insert | O(1) amortized | O(1) per row in build phase | O(log N + vnodes) |
| Delete | O(1) + tombstone | N/A | O(log N × vnodes) |
| Resize | O(N) when triggered | OOM if build side too large | O(K/N) keys remapped |
| Memory | 50–200 bytes/entry | Row_size × build_rows | O(N × vnodes) positions |
| Cache behavior | Good (flat array) | L3-dependent | Binary search in sorted array |

**Rule of thumb for Spark hash joins:**
- Build side < 10 MB: broadcast hash join (free, fast)
- Build side 10 MB – 2 GB: shuffle hash join (requires memory per partition)
- Build side > 2 GB: sort-merge join (disk-spill safe, but 2–5× slower)

---

## 18. Connections to Other Modules

**CSF-ALG-101 M01 (Complexity Analysis):** The O(1) amortized analysis of hash table insertions uses the identical geometric series argument as Python list append. The space-time tradeoff (O(M) space for the hash table → O(N+M) time for the join) is the canonical example of trading space for time.

**CSF-ARC-102 M02 (Cache Lines and Coherence):** Open addressing is cache-friendly because probing accesses adjacent memory — often the same 64-byte cache line. Chaining is cache-hostile because each linked list node is a separate allocation. Understanding why matters for implementing hot-path data structures in Spark Executor JVM memory.

**CSF-ALG-101 M03 (Trees):** B-trees are the alternative to hash tables for database indexes. B-trees provide O(log N) lookup but support range queries (`WHERE date BETWEEN ...`). Hash indexes provide O(1) lookup but cannot support range queries. Understanding both lets you choose correctly: Postgres B-tree index for range queries, Postgres hash index for equality lookups.

**DCS-KFK-101 (Kafka Internals):** Kafka partition assignment uses `murmur2(key) % N`. Understanding hash uniformity and the partition-skew failure mode (Section 8.3) is directly applicable to Kafka key design.

**DCS-SPK-101 (Spark Architecture):** Spark's hash join, aggregation, and GROUP BY all use in-memory hash tables. The Catalyst optimizer decides between hash join and sort-merge join based on table sizes — which is a direct application of the hash table memory constraint.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is a hash table's average-case lookup complexity? | O(1) — bounded by a constant independent of N, assuming uniform key distribution and bounded load factor |
| 2 | What is the worst-case lookup complexity for a hash table, and when does it occur? | O(N) — when all N keys collide into one bucket (adversarial input or poor hash function) |
| 3 | Define load factor. | α = N / capacity (N = stored keys, capacity = array slots). Controls collision probability and average probe length |
| 4 | At what load factor does Python dict resize? | 2/3 ≈ 0.667 (when `used * 3 > alloc * 2`) |
| 5 | Why is hash table insertion O(1) amortized rather than O(1)? | Periodic resizes cost O(N) each. Geometric doubling means total resize cost is O(2N) across N insertions → O(1) per insert amortized |
| 6 | What is a tombstone in open addressing? | A deleted-slot marker that tells the probe sequence to keep searching, preventing premature "not found" reports for keys that probed past the deleted slot |
| 7 | What is the key difference between chaining and open addressing? | Chaining stores overflow in linked lists (pointer-heavy, cache-hostile); open addressing stores all entries inline (cache-friendly, probe chain) |
| 8 | Why can't mutable objects be Python dict keys? | Dict keys must have a stable `__hash__`. If a mutable object changes after insertion, its hash changes, making it impossible to find via the new hash |
| 9 | What problem does consistent hashing solve? | When cluster topology changes (node add/remove), only K/N keys remapped. Simple modulo hashing remaps ~(N-1)/N keys, causing thundering-herd cache invalidation |
| 10 | How does consistent hashing assign a key to a node? | Hash key to ring position; walk clockwise to find the first node. Each node is placed at multiple positions (virtual nodes) for even distribution |
| 11 | What are virtual nodes (vnodes) in consistent hashing? | Multiple ring positions per physical node. Ensures each physical node owns ~1/N of the keyspace regardless of physical placement, improving load balance |
| 12 | What join algorithm does Spark use for tables below `autoBroadcastJoinThreshold`? | Broadcast hash join — small table is broadcast to all executors, each builds a local hash table, probe side rows are looked up locally |
| 13 | What is the time complexity of Spark's hash join? | O(N + M): O(M) to build the hash table, O(N) to probe it |
| 14 | Why does Spark sometimes choose sort-merge join over hash join? | When the build side is too large to fit in executor memory — hash join OOMs, sort-merge join can spill to disk |
| 15 | What hash function does Kafka use for partition assignment? | `murmur2(key.getBytes()) % numPartitions` — simple modulo hashing, NOT consistent hashing |
| 16 | What happens to Kafka key→partition mapping when you add partitions? | Remapping of nearly all keys (non-consistent modulo). Keys that were on partition X may now be on a different partition — breaks ordering guarantees |
| 17 | What is the compact dict design (Python 3.6+)? | Separates sparse indices array (small integers pointing to entries) from dense entries array (ordered by insertion). Enables insertion-order iteration and lower memory usage |
| 18 | Why does Python hash randomize strings? | To prevent hash flooding attacks: an attacker could craft inputs that all hash to bucket 0, turning O(1) dict ops into O(N²). PYTHONHASHSEED is random per process since Python 3.3 |
| 19 | What is the `__hash__` / `__eq__` contract? | If `a == b` then `hash(a) == hash(b)`. Violation silently corrupts dict: two equal objects would be stored in different buckets and never found via the other |
| 20 | Redis Cluster uses 16,384 hash slots. How is this different from a true consistent hash ring? | Fixed number of slots (not positions proportional to keys). Each physical node owns a range of slots. Adding a node moves some slot ranges from existing nodes to the new one. Simpler to implement and reason about than a variable ring |

---

## 20. Further Reading

**Original papers and references:**
- Knuth, D. (1998). *The Art of Computer Programming, Volume 3: Sorting and Searching*, Section 6.4 (Hashing). The canonical reference for hash table theory.
- Karger, D. et al. (1997). *Consistent Hashing and Random Trees*. The paper that introduced consistent hashing for distributed caching.

**CPython internals:**
- `Objects/dictobject.c` in the CPython source — the compact dict implementation
- Raymond Hettinger's PyCon talk "Modern Dictionaries" (2017) — best explanation of the 3.6 redesign

**Production systems:**
- Redis Cluster specification: https://redis.io/docs/reference/cluster-spec/ — explains the 16,384 hash slot design
- Cassandra documentation on virtual nodes: explains the 256-vnode default and rebalancing behavior

**Data engineering specific:**
- Spark AQE documentation (`spark.sql.adaptive.*`) — explains how AQE automatically chooses between hash join and sort-merge join at runtime

---

## 21. Module Summary

A hash table maps keys to values in O(1) average time by hashing each key to an index in a fixed-size array. The performance guarantee requires three things: a uniform hash function, a bounded load factor (enforced by geometric-doubling resize), and a collision resolution strategy that keeps probe sequences short.

The two dominant strategies are chaining (linked list per bucket, cache-hostile but graceful at high load) and open addressing (inline probing, cache-friendly but requires tombstones for deletes and breaks above ~0.75 load factor). Python dict uses open addressing with a compact split design that makes iteration insertion-ordered and reduces memory footprint. Java HashMap uses chaining.

In data engineering, hash tables underpin three critical patterns: Spark hash joins (O(N+M) vs O(N×M) for nested loop), Python dict-based GroupBy and enrichment pipelines (O(N) via O(1) lookup), and consistent hashing for distributed key assignment (remapping 1/N keys on topology change vs nearly 100% for modulo hashing). Understanding load factors lets you predict when hash join chooses sort-merge join as a fallback; understanding consistent hashing lets you explain why Kafka does not use it (and why that matters for partition rekeying).

---

**CSF-ALG-101: 2 of 5 modules complete.**  
**Next: M03 — Trees (BST, AVL, B-tree — and why databases use B-trees, not BSTs)**
