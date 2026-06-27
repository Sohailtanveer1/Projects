# CSF-ALG-101 M03: Trees

**Course:** CSF-ALG-101 — Algorithms and Data Structures for Data Engineers  
**Module:** 03 of 05  
**Filesystem position:** 00_CS_Foundations/M14_Trees  
**Prerequisites:** CSF-ALG-101 M01 (Complexity Analysis), M02 (Hash Tables)

---

## Table of Contents

1. [The Problem Trees Solve](#1-the-problem-trees-solve)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: Binary Search Trees](#4-core-theory-binary-search-trees)
5. [Visual: Tree Anatomy and Traversals](#5-visual-tree-anatomy-and-traversals)
6. [Deep Dive: Self-Balancing Trees and B-Trees](#6-deep-dive-self-balancing-trees-and-b-trees)
7. [Mental Models](#7-mental-models)
8. [Failure Scenarios](#8-failure-scenarios)
9. [Data Engineering Connections](#9-data-engineering-connections)
10. [B-Tree Internals: The Database Index](#10-b-tree-internals-the-database-index)
11. [Trees in Data Lake Formats](#11-trees-in-data-lake-formats)
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

## 1. The Problem Trees Solve

Hash tables, from the previous module, give O(1) average lookup. Why would anyone use a tree that offers O(log N)?

The answer is **order**. Hash tables destroy order — the hash of "2024-01-01" bears no relationship to the hash of "2024-01-02." You cannot ask a hash table "give me all events between January and March." You cannot ask "what is the next key after this one?" You cannot do a range scan.

Trees preserve order. A B-tree index on a `timestamp` column can answer `WHERE timestamp BETWEEN '2024-01-01' AND '2024-03-31'` by walking to the start of the range and scanning forward. A B-tree index on `user_id` can answer `ORDER BY user_id LIMIT 100` without sorting the full table. A hash index can answer neither.

This is why Postgres, MySQL, Oracle, and every production relational database use B-tree indexes as their default index type — not hash indexes. This is why Parquet organizes its metadata as a tree of row groups and column chunks. This is why Iceberg organizes its table metadata as a three-level tree of snapshots, manifests, and data files.

This module explains how BSTs work, why they fail in production, how AVL trees and B-trees solve the failure mode, and how the B-tree became the foundation of every database index you have ever used.

---

## 2. How to Use This Module

**For interview prep:** Sections 4, 6, 10, 14, 15 are essential. Section 14 contains six staff-level Q&As on tree data structures including "why B-trees and not hash indexes in Postgres." Section 15 escalates from BST basics to production database index design in six exchanges.

**For production engineering:** Sections 9, 10, 11 connect tree theory directly to Postgres indexes, Parquet layout, and Iceberg metadata — the systems you use daily.

**For fundamental understanding:** Read all sections in order. The structure is: BST correctness → BST failure (degenerate case) → AVL balancing → B-tree (why large branching factor matters for disk) → production systems.

---

## 3. Prerequisites Check

Before this module, you need:

- **Big-O notation** (M01): O(log N) vs O(N) comparison — this module's core is about why O(log N) degrades to O(N) without balancing
- **Memory hierarchy** (CSF-ARC-102 M01): B-trees exist specifically to minimize disk I/O. Understanding why DRAM is 70 ns but SSD is 100,000 ns makes the "minimize tree height" motivation concrete
- **Hash tables** (M02): You need to understand why hash tables cannot answer range queries, to appreciate why B-trees are the right structure for database indexes

---

## 4. Core Theory: Binary Search Trees

### 4.1 The Binary Search Tree Property

A binary search tree is a rooted binary tree where every node satisfies the **BST property**:

```
For every node X:
  - All keys in X's left subtree are LESS THAN X's key
  - All keys in X's right subtree are GREATER THAN X's key
```

This property makes binary search possible on a dynamic data structure. Given a key K:
1. Start at the root
2. If K equals current node's key: found
3. If K < current node's key: recurse left
4. If K > current node's key: recurse right
5. If you reach a null child: not found

In a balanced BST with N nodes, the height is O(log N). Each step down the tree halves the remaining search space — the same halving logic as binary search on a sorted array. This gives O(log N) for search, insert, and delete.

### 4.2 BST Operations

**Search:**
```
search(node, key):
  if node is null: return NOT_FOUND
  if key == node.key: return node.value
  if key < node.key: return search(node.left, key)
  else: return search(node.right, key)
```
Cost: O(height). In a balanced tree, height = O(log N). In a degenerate tree, height = O(N).

**Insert:**
```
insert(node, key, value):
  if node is null: return new Node(key, value)
  if key < node.key: node.left = insert(node.left, key, value)
  elif key > node.key: node.right = insert(node.right, key, value)
  else: node.value = value  # update
  return node
```
Cost: O(height). The new node is always inserted as a leaf.

**Delete:** Three cases:
- Leaf node: remove directly
- One child: replace the node with its child
- Two children: find the in-order successor (smallest key in right subtree), copy its key/value to the current node, then delete the successor (which has at most one child)

**In-order traversal (left → node → right):** Visits all nodes in sorted key order. This is why BSTs are also called "sorted trees" — an in-order walk produces a sorted sequence. Hash tables cannot produce a sorted sequence without O(N log N) work.

### 4.3 Why O(log N) Height Requires Balance

Consider inserting keys in sorted order: 1, 2, 3, 4, 5, 6, 7.

Each new key is larger than all existing keys, so it always goes to the right child. The result:

```
1
 \
  2
   \
    3
     \
      4
       \
        5
         \
          6
           \
            7
```

This is a **degenerate BST** — a linked list in disguise. Height = N = 7. Search for 7 requires 7 comparisons. For N = 1,000,000 rows inserted in sorted order (a very common pattern — sequential IDs, timestamps), a naive BST has height 1,000,000, making every lookup O(N).

Sorted insertion is the realistic case for data engineering: tables are often sorted by primary key (auto-increment) or by event time. Insertion of sorted data into a BST is the worst possible case.

The fix is self-balancing: after every insert or delete, restructure the tree to keep height at O(log N).

### 4.4 The Balance Factor

For a node X, the **balance factor** is:

```
balance_factor(X) = height(X.right) - height(X.left)
```

In an AVL tree, balance factors are maintained in {-1, 0, 1} for every node. A balance factor of +2 or -2 after an insert means the tree has become unbalanced and needs a rotation to restore balance.

---

## 5. Visual: Tree Anatomy and Traversals

### 5.1 Balanced BST

```
Balanced BST (height = 3, N = 7):

              ┌────────────────┐
              │       4        │  ← root
              └───┬───────┬───┘
                  │       │
         ┌────────┘       └────────┐
    ┌────┴────┐               ┌────┴────┐
    │    2    │               │    6    │
    └──┬───┬──┘               └──┬───┬──┘
       │   │                     │   │
  ┌────┘   └────┐           ┌────┘   └────┐
┌─┴─┐         ┌─┴─┐       ┌─┴─┐         ┌─┴─┐
│ 1 │         │ 3 │       │ 5 │         │ 7 │
└───┘         └───┘       └───┘         └───┘

Height = 3 = O(log 7) ≈ 2.81
Search for 5: root(4) → right(6) → left(5) = 3 comparisons
In-order walk: 1, 2, 3, 4, 5, 6, 7 (sorted)
```

### 5.2 Degenerate BST (Sorted Insertion)

```
Degenerate BST (height = 7, N = 7, keys inserted 1→7):

1
 \
  2
   \
    3
     \
      4
       \
        5
         \
          6
           \
            7

Height = 7 = O(N)
Search for 7: 7 comparisons — same as linear scan
This is what happens when you insert auto-increment primary keys into a naive BST
```

### 5.3 AVL Rotation — Left Rotation

```
Before (balance factor at 2 → right-heavy, unbalanced):

    2                      
     \                     
      4   balance(2) = 2   (right height 2, left height 0)
       \
        6

Left rotation at node 2:

      4
     / \
    2   6

After: balance(4) = 0, balance(2) = 0, balance(6) = 0 — all balanced
```

### 5.4 B-Tree Node (Order 4 — up to 3 keys per node)

```
B-tree of order 4 (max 3 keys, max 4 children per node):

                    ┌──────────────────┐
                    │   10  │  20  │   │
                    └──┬───┬────┬───┬──┘
                       │   │    │   │
         ┌─────────────┘   │    │   └──────────────┐
         │          ┌──────┘    └──────┐            │
    ┌────┴────┐  ┌──┴──────┐  ┌───────┴─┐  ┌───────┴─┐
    │ 1│ 5│ 7│  │12│15│17  │  │21│25│   │  │30│40│50 │
    └─────────┘  └─────────┘  └─────────┘  └─────────┘

Every leaf at the same depth.
Search for 25: root → compare 10,20 → right subtree → compare 21,25 → found.
2 node reads for 13 keys. Height stays O(log_order N).
```

### 5.5 B+ Tree Leaf Linking

```
B+ tree (leaves linked as a doubly linked list):

Internal nodes: keys only (routing)
Leaf nodes: keys + values + sibling pointers

         ┌────────────┐
         │    10 │ 20  │ (internal node, keys only)
         └──┬────┬──┬──┘
            │    │  │
    ┌────────┘    │  └────────┐
    ↓             ↓           ↓
┌──────────┐  ┌──────────┐  ┌──────────┐
│1,5,7,9   │←→│10,12,15  │←→│20,25,30  │  (leaf nodes with values)
└──────────┘  └──────────┘  └──────────┘
      ↑ doubly linked for range scan

Range scan [9, 20]:
  1. Traverse to leaf containing 9
  2. Follow right sibling pointer → leaf containing 10,12,15
  3. Follow right sibling pointer → leaf containing 20
  4. Stop (25 > 20)
  No need to traverse back up to the root!
```

---

## 6. Deep Dive: Self-Balancing Trees and B-Trees

### 6.1 AVL Trees

An AVL tree (Adelson-Velsky and Landis, 1962) is a BST that maintains the invariant that for every node, the heights of its left and right subtrees differ by at most 1. After every insert or delete, the tree walks back up to the root checking balance factors and applying rotations where needed.

**Four rotation cases:**

**Left rotation** (right-heavy at node Z):
```
    Z              Y
   / \           /   \
  T1  Y    →   Z      X
     / \       / \   / \
    T2  X     T1 T2 T3 T4
       / \
      T3 T4
```

**Right rotation** (left-heavy at node Z): mirror of left rotation.

**Left-Right rotation** (left-heavy with right-heavy left child):
1. Left rotate the left child
2. Right rotate the current node

**Right-Left rotation** (right-heavy with left-heavy right child):
1. Right rotate the right child
2. Left rotate the current node

**AVL complexity:**
| Operation | Complexity |
|---|---|
| Search | O(log N) |
| Insert | O(log N) — insert + at most 1 rotation |
| Delete | O(log N) — delete + O(log N) rotations climbing up |
| Space | O(N) — plus 1 integer per node for height |

AVL trees maintain height ≤ 1.44 log₂(N+2) — tighter than any other self-balancing BST. This makes them faster for lookup-heavy workloads where the extra rotation cost on insert is acceptable.

**Red-Black trees** (used by Java TreeMap, C++ std::map, Linux kernel) are less rigidly balanced (height ≤ 2 log N) but require fewer rotations on insert and delete. They are preferred when the workload has more mutations than lookups. The trade-off: slightly worse lookup performance, better mutation performance.

### 6.2 Why BSTs (Even AVL) Fail for Disk-Based Storage

An AVL tree with N = 1,000,000 nodes has height ≈ 28. That means 28 node accesses per lookup. If the tree is in DRAM, each access is ~70 ns → total 2 μs. Fast enough.

But for a database index, the data does not fit in DRAM. A Postgres table with 100 million rows and a 16-byte index key per row needs 1.6 GB just for the keys — plus overhead. This does not fit in L3 cache (typically 8–48 MB) and may not even fit in available DRAM. Every node access that is not in the page cache is a disk read.

**Disk access latency:**
- DRAM: ~70 ns
- NVMe SSD: ~100,000 ns (100 μs) — 1,400× slower
- SATA SSD: ~500,000 ns (500 μs) — 7,000× slower
- HDD: ~10,000,000 ns (10 ms) — 140,000× slower

For a disk-based AVL tree with height 28:
- 28 random disk reads (each node likely on a different page)
- On NVMe SSD: 28 × 100 μs = 2.8 ms per lookup
- On HDD: 28 × 10 ms = 280 ms per lookup

For an online transaction processing system doing 10,000 lookups/second: 280 ms × 10,000 = 2,800,000 ms of disk time per second. That's impossible — the disk can handle maybe 200 random reads/second on an HDD.

The solution is to make each tree node much larger — holding hundreds or thousands of keys — so that the tree height becomes tiny and the number of disk reads per lookup is 2–4, not 28.

### 6.3 B-Trees: The Key Insight

A B-tree of order M (also called degree M, or branching factor M):

- Each **internal node** holds between ⌈M/2⌉ and M-1 keys, and between ⌈M/2⌉ and M children
- Each **leaf node** holds between ⌈M/2⌉ and M-1 keys (and in a B+ tree, the actual values or row pointers)
- All leaves are at the **same depth** (the tree is perfectly balanced by height)
- Keys within each node are sorted; a key K at position i routes to child i if K < node.key[i], child i+1 if K > node.key[i]

With M = 1000 (a common branching factor for a Postgres B-tree page of 8 KB):

```
Height 1: 1 node, up to 999 keys
Height 2: up to 1,000 nodes, up to 999,000 keys
Height 3: up to 1,000,000 nodes, up to 999,000,000 keys (≈ 1 billion)
```

A billion-row table has a B-tree index with height 3. Every lookup requires 3 disk reads. Compare that to an AVL tree's 30 disk reads for the same table. The B-tree wins by an order of magnitude because each node read from disk is a full page (4 KB or 8 KB), and a single page holds hundreds of keys.

**B-tree height formula:**
```
height = ⌈log_⌈M/2⌉(N)⌉

For N = 10^9, M = 1000:
  height = ⌈log_500(10^9)⌉ = ⌈log(10^9)/log(500)⌉ = ⌈9/2.7⌉ = ⌈3.33⌉ = 4
```

Four disk reads to find any row in a billion-row table. This is why B-trees are the default index for every production relational database.

### 6.4 B-Tree vs B+ Tree

The distinction matters in production:

**B-tree:** Internal nodes store both keys and values (or row pointers). A search can terminate at any level if the key is found in an internal node.

**B+ tree:** Internal nodes store **keys only** (routing information). All values live in the leaf nodes. All leaf nodes are linked together as a doubly linked list.

| Property | B-tree | B+ tree |
|---|---|---|
| Values in internal nodes | Yes | No |
| Leaf linking | No | Yes (doubly linked) |
| Range scan efficiency | Requires tree traversal | O(K) after finding start leaf — follow sibling pointers |
| Internal node capacity | Lower (keys + values) | Higher (keys only → more keys per page → lower height) |
| Point lookup | Slightly faster (may terminate early) | Slightly slower (always reaches leaf) |

Postgres uses B+ trees for all indexes. The leaf linking means a range scan `WHERE id BETWEEN 1000 AND 2000` navigates to the leaf containing 1000, then follows sibling pointers forward until it passes 2000 — no tree traversal needed for the scan portion.

### 6.5 B-Tree Insert and Split

When a node becomes full (M keys), it splits into two nodes and pushes the median key up to the parent. If the parent is also full, it splits too — the split can propagate up to the root.

```
B-tree of order 4 (max 3 keys per node). Insert 8:

Before:
       [4, 7]
      /   |   \
  [1,2,3] [5,6]  [9,10]

Insert 8 → goes to [5,6] → [5,6,8] → full? No (max 3 keys = full at 3, now at 3) → full!
Split [5,6,8]: median = 6, left=[5], right=[8]
Push 6 up to parent: parent becomes [4,6,7]

       [4, 6, 7]
      /   |   |  \
 [1,2,3] [5] [8] [9,10]
```

If the root splits, the tree grows taller — but only at the root. B-trees grow from the top (root split), not from the leaves. This is what keeps all leaves at the same depth — the perfect height balance that eliminates worst-case disk reads.

---

## 7. Mental Models

### 7.1 The Library Catalog Model

A BST is like a binary decision tree: at each node, you go left or right based on a single comparison. A library with alphabetically shelved books where you can only compare "is the book before or after this midpoint?" — and you must make that comparison at each of 28 shelves to find one book.

A B-tree is like a real library catalog: each card in the catalog tells you "books starting with A-L are in room 1, M-R in room 2, S-Z in room 3." One card access narrows the search to one room. A second access within the room narrows it to a shelf. A third access gives you the book. Three accesses, millions of books. Each "card" in the catalog holds many keys — not just one.

The critical insight: **the cost of each access is the same regardless of whether the node has 1 key or 1,000 keys** (both are one disk read). So you want the maximum possible information per node access.

### 7.2 The Phone Book Model for Range Scans

A hash table is like a phone book where names are randomly scrambled — you can look up "Smith, John" instantly if you know the hash, but you cannot find "all Smiths" without reading the entire book.

A B+ tree is like a real phone book — sorted, with a table of contents (internal nodes) pointing to pages (leaf nodes), and all pages linked in order. "All Smiths" is a single lookup to the S page, then a forward scan.

### 7.3 The Elevator Model for B-Tree Height

Imagine a building with 1,000,000 rooms. An AVL tree elevator stops at every floor — 20 floors, 20 stops. A B-tree elevator has 1,000 rooms per floor — so 3 floors cover 1,000^3 = 1 billion rooms. Three stops, billions of destinations. Each stop costs the same (one disk read), but the B-tree packs 1,000× more information per stop.

---

## 8. Failure Scenarios

### 8.1 Missing Index Causing Full Table Scan

```sql
-- Table: events, 500 million rows, no index on user_id
-- B-tree on event_id (primary key) exists, but not on user_id

SELECT * FROM events WHERE user_id = 'u_12345';

-- EXPLAIN ANALYZE output:
-- Seq Scan on events (cost=0.00..15234567.89 rows=1 width=200)
--   Filter: (user_id = 'u_12345')
--   Rows Removed by Filter: 499999999
-- Execution Time: 45000 ms

-- Postgres reads every 8 KB page from disk to find the matching rows.
-- 500M rows × 200 bytes / 8192 bytes/page ≈ 12,200,000 pages
-- At 100 μs per page on NVMe SSD: 1,220 seconds. Not viable.

-- Fix: CREATE INDEX events_user_id_idx ON events(user_id);
-- After index: 3 B-tree page reads → ~300 μs total
```

### 8.2 Index Bloat from Dead Tuples in Postgres

Postgres uses MVCC (Multi-Version Concurrency Control) — updates do not modify rows in place; they insert a new row version and mark the old one as dead. Dead tuples accumulate in both the table heap and the B-tree indexes.

```sql
-- Table with many updates/deletes — index bloat example
-- Check index bloat:
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    idx_scan,
    idx_tup_read
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Bloated B-tree index: internal nodes may point to pages full of dead tuples
-- Effect: index height increases, more disk reads per lookup
-- Fix: VACUUM ANALYZE table_name; (or autovacuum if configured correctly)
-- Aggressive fix: REINDEX INDEX events_user_id_idx; (locks, avoid in production)
```

A B-tree with 20% dead tuples has an effective branching factor of ~800 instead of ~1000 — the height may increase by 1 level, adding one disk read to every lookup across the entire table.

### 8.3 Postgres B-Tree Index Not Used for Low-Selectivity Queries

```sql
-- Index exists on status column, but the planner ignores it:
CREATE INDEX events_status_idx ON events(status);

EXPLAIN SELECT * FROM events WHERE status = 'active';
-- Seq Scan on events  (cost=0.00.....) — planner chose table scan!

-- Why? If 90% of rows have status='active', the B-tree navigates to the leaf,
-- then needs to retrieve 90% of pages anyway.
-- A sequential table scan (reading pages in order) is faster than
-- 450M random leaf lookups via the B-tree index.

-- Postgres's rule of thumb: if selectivity > ~5-10%, skip the index.
-- For high-cardinality selective columns (user_id, event_id): index wins
-- For low-cardinality columns (status, country): partial index or don't index

-- Partial index for low-cardinality:
CREATE INDEX events_pending_idx ON events(created_at) WHERE status = 'pending';
-- Only indexes the 10% of rows that are 'pending' — index is small, always selective
```

### 8.4 B-Tree Write Amplification on High-Cardinality Inserts

```
Scenario: 10 million inserts/second into a time-series table

B-tree behavior:
  - Each insert navigates to the correct leaf page
  - If the leaf is full → split → propagate up → potential cascade of splits
  - Each split writes 2 new pages + updates parent page = 3 disk writes per insert (worst case)
  - At 10M inserts/sec: 30M disk writes/sec → NVMe SSD write limit hit

LSM-tree (RocksDB, InfluxDB, Apache Cassandra) behavior:
  - Write goes to MemTable (in-memory) → immediate, no disk I/O
  - Background compaction sorts and merges into SSTables
  - Write latency: <1 ms (in-memory write)
  - Read latency: higher (must check multiple SSTables)

This is why time-series databases and Kafka (append-only log) use LSM-tree variants,
not B-trees — write throughput is the bottleneck, not read latency.
```

### 8.5 AVL Tree Over-Rotation Under Batch Inserts

```python
# Demonstration of why insertion order matters for BST performance

# Worst case: sorted insertion
sorted_keys = list(range(1_000_000))
# Naive BST: height = 1,000,000 → every lookup is O(N)
# AVL tree: automatically rotates → height stays ≈ 28 → fine

# But: AVL rotation cost per insert is O(log N) with potential O(log N) rotations on delete
# For a write-heavy workload (1M inserts/sec):
# AVL: log₂(1M) ≈ 20 comparisons + rotations per insert → acceptable
# Red-Black: ~2 rotations per insert on average → slightly faster for inserts

# The real problem: AVL/RB trees are in-memory structures.
# For on-disk data, neither is appropriate — use B-trees.
```

---

## 9. Data Engineering Connections

### 9.1 Postgres Index Types: When to Use Each

```sql
-- B-tree (default) — ORDER BY, range scans, equality, LIKE 'prefix%'
CREATE INDEX idx_bt ON events(user_id);  -- equality + range
CREATE INDEX idx_bt2 ON events(created_at);  -- ORDER BY, date ranges
CREATE INDEX idx_prefix ON events(event_name) WHERE event_name LIKE 'page_%';

-- Hash index — equality only, slightly faster than B-tree for pure equality
CREATE INDEX idx_hash ON sessions USING HASH (session_id);
-- Cannot be used for: ORDER BY, BETWEEN, >, <, LIKE
-- Also: hash indexes are not WAL-logged in Postgres < 10 (unsafe)

-- GIN (Generalized Inverted Index) — for array, JSONB, full-text search
CREATE INDEX idx_gin ON events USING GIN (properties);  -- JSONB column
-- Enables: properties @> '{"country": "US"}' — containment queries on JSONB

-- BRIN (Block Range Index) — for naturally ordered large tables
CREATE INDEX idx_brin ON events USING BRIN (created_at);
-- Stores min/max per block range — tiny index, fast for ordered data
-- Cannot be used for: random access by key, JOIN lookups

-- GiST (Generalized Search Tree) — geometric data, PostGIS, exclusion constraints
CREATE INDEX idx_gist ON locations USING GIST (geom);
-- Enables: geometry && other_geom (bounding box overlap)
```

For data engineering workloads:
- `user_id`, `event_id`, `order_id` — B-tree (equality + join)
- `created_at`, `event_date` — B-tree (range scan) or BRIN (very large, naturally ordered)
- JSON metadata column — GIN
- Status, country (low cardinality) — partial B-tree or no index

### 9.2 The dbt Incremental Model and Table Scanning

```sql
-- dbt incremental model relies on correct index strategy:
{{ config(materialized='incremental', unique_key='event_id') }}

SELECT ...
FROM source_events
{% if is_incremental() %}
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}

-- This WHERE clause works efficiently ONLY if there is a B-tree index on created_at.
-- Without it, every incremental run does a full table scan of the target table
-- to find the MAX(created_at) — O(N) instead of O(log N) + one leaf scan.

-- BigQuery note: BigQuery uses partitioning instead of B-tree indexes.
-- The equivalent is: PARTITION BY DATE(created_at)
-- which physically separates data into date-partitioned files.
-- Partition pruning achieves the same selectivity that a B-tree index would in Postgres.
```

### 9.3 Airflow's Task Dependency DAG

Airflow's task dependency graph is a DAG (Directed Acyclic Graph) — a tree-like structure where tasks (nodes) have edges representing dependencies. The Airflow scheduler uses topological sort (covered in M05) to determine which tasks can run.

Within the Airflow metadata database (Postgres), each DAG run and task instance is stored in tables with B-tree indexes on (`dag_id`, `run_id`, `task_id`) — compound B-tree indexes. Airflow queries like "get all pending task instances for this DAG run" use these indexes for O(log N) lookups instead of full table scans.

---

## 10. B-Tree Internals: The Database Index

### 10.1 Postgres Page Layout

Postgres stores the B-tree index in 8 KB pages. Each page contains:

```
Postgres B-tree leaf page (8192 bytes):

┌──────────────────────────────────────────────────┐
│ Page header (24 bytes)                            │
│   - LSN (Log Sequence Number): 8 bytes            │
│   - flags (2 bytes): is_leaf, has_garbage, etc.   │
│   - lower/upper (offsets to free space)           │
├──────────────────────────────────────────────────┤
│ Item ID array (4 bytes per entry)                 │
│   ItemID[0]: offset=132, length=36                │
│   ItemID[1]: offset=168, length=36                │
│   ...                                             │
├──────────────────────────────────────────────────┤
│ Free space                                        │
├──────────────────────────────────────────────────┤
│ Tuples (index entries, packed from end)           │
│   IndexTuple: (heap TID[6] + key_data[N])         │
│   Each entry: 6-byte TID (pointing to heap row)   │
│              + actual index key (e.g., int64 = 8B)│
│   Total per entry: ~14 bytes for int64 key        │
│   Entries per 8KB page: (8192 - 24) / 14 ≈ 584   │
└──────────────────────────────────────────────────┘

Branching factor for 8-byte integer key:
  ~584 entries per leaf page
  Internal node: (8192 - 24) / (8 + 6) ≈ 585 children
  Height for 10^9 rows: ⌈log_585(10^9)⌉ = ⌈9/2.77⌉ = 4 levels
  → 4 disk reads to find any row in a billion-row table
```

### 10.2 The Postgres Index Scan vs Heap Fetch

When Postgres uses a B-tree index, the process is:

```
Query: SELECT * FROM events WHERE user_id = 'u_12345';

Step 1: B-tree descent (3–4 page reads)
  Root page → internal pages → leaf page containing 'u_12345'
  Leaf page contains: (key='u_12345', TID=(page=4522, slot=7))
                      (key='u_12345', TID=(page=8901, slot=3))  ← if duplicates

Step 2: Heap fetch (1 read per matching row)
  Fetch heap page 4522, slot 7 → the full event row
  Fetch heap page 8901, slot 3 → another matching row

Step 3: Visibility check (MVCC)
  For each heap row: check xmin/xmax against current transaction snapshot
  If the row is a dead tuple: skip it

Total: 3-4 index reads + N heap reads (N = number of matching rows)

Index-Only Scan (available since Postgres 9.2):
  If all needed columns are IN the index, skip heap fetch entirely
  Requires: VACUUM to clear the visibility map (so Postgres knows which pages are clean)
```

### 10.3 Write-Ahead Log and B-Tree Durability

Every B-tree modification (insert, split, delete) is written to the WAL before the actual page is modified. This is the "W" in WAL — Write-Ahead Log.

```
Insert into B-tree index:

1. WAL write: "Page 4522 will be modified: insert entry (key='u_12345', TID=...)"
              WAL record → WAL buffer → fsync to WAL segment file
2. Page modification: actually modify page 4522 in the shared buffer pool
3. Page eventually flushed to disk by bgwriter/checkpointer

Crash recovery:
  - WAL is replayed from last checkpoint forward
  - Any B-tree modification with a WAL record but not yet flushed is replayed
  - Tree is restored to a consistent state
  - Property guaranteed: all committed transactions are durable
```

This WAL-first design means B-tree writes incur two writes: WAL write (sequential, fast) + eventual page write (random, amortized). Sequential WAL writes are fast even on HDD because the disk head doesn't need to seek.

### 10.4 Covering Indexes and Index-Only Scans

```sql
-- Covering index: includes all columns needed by the query
-- Enables index-only scan — no heap fetch needed

-- Without covering index:
CREATE INDEX idx_events_user ON events(user_id);
SELECT user_id, created_at, event_type FROM events WHERE user_id = 'u_12345';
-- Must fetch heap pages to get created_at and event_type

-- With covering index:
CREATE INDEX idx_events_user_covering ON events(user_id) INCLUDE (created_at, event_type);
SELECT user_id, created_at, event_type FROM events WHERE user_id = 'u_12345';
-- Index leaf pages already contain created_at and event_type
-- No heap fetch needed → 3-4 page reads total regardless of result size

-- Trade-off: covering index is larger (stores more data per leaf entry)
-- → fewer entries per page → slightly higher tree height
-- → worth it when you eliminate many heap fetches
```

---

## 11. Trees in Data Lake Formats

### 11.1 Parquet Metadata Tree

A Parquet file is organized as a three-level structure that functions as a tree:

```
Parquet file structure:

Magic bytes "PAR1"
│
├── Row Group 0  (e.g., 128 MB of data)
│   ├── Column Chunk: user_id
│   │   ├── Page 0: compressed data (128 KB)
│   │   ├── Page 1: compressed data
│   │   ├── Dictionary page (if dictionary-encoded)
│   │   └── Column statistics:
│   │         min = "u_00001"
│   │         max = "u_99999"
│   ├── Column Chunk: created_at
│   │   └── Column statistics:
│   │         min = 2024-01-01 00:00:00
│   │         max = 2024-01-01 23:59:59
│   └── ...
│
├── Row Group 1  (next 128 MB)
│   └── ...
│
└── Footer (at end of file)
    ├── Schema (column names, types, nesting)
    ├── Row group metadata (offset, size, row count)
    └── Column statistics per row group

Reading for predicate pushdown:
  Query: WHERE created_at BETWEEN '2024-01-01' AND '2024-01-31'
  
  Step 1: Read footer (one small read, usually <1 MB)
  Step 2: Check each row group's column statistics for created_at
    Row Group 0: min=2024-01-01, max=2024-01-01 → MIGHT match → read
    Row Group 5: min=2024-02-01, max=2024-02-28 → SKIP (no overlap)
  Step 3: Read only matching row groups → skip 90% of file

This is not a B-tree, but uses the same principle:
hierarchical metadata enables skipping large portions of data.
```

### 11.2 Iceberg Metadata Tree

Apache Iceberg organizes table metadata as a three-level tree:

```
Iceberg metadata tree:

catalog
└── table pointer → metadata.json (current snapshot)
                        │
                        ├── snapshot-id: 1234567
                        ├── schema
                        └── manifest-list (snapshot manifest file)
                                │
                                ├── manifest-file-0001.avro
                                │     ├── data-file-0001.parquet
                                │     │     └── file stats (record count, column bounds)
                                │     ├── data-file-0002.parquet
                                │     └── data-file-0003.parquet
                                │
                                └── manifest-file-0002.avro
                                      └── data-file-0004.parquet

Query: WHERE event_date = '2024-01-15'

Step 1: Read metadata.json (1 object store read)
Step 2: Read manifest-list (1 read, tiny file)
Step 3: Read each manifest file, check partition statistics
  manifest-0001: partition event_date IN [2024-01-01, 2024-01-31] → might match → check data files
  manifest-0002: partition event_date IN [2024-02-01, 2024-02-28] → skip entirely
Step 4: For matching manifest: read file-level stats → skip files where date is out of range
Step 5: Read only the qualifying Parquet files

3–4 metadata reads to skip terabytes of data files.
Same principle as B-tree: hierarchical structure enables O(log N) elimination.
```

### 11.3 Delta Lake Transaction Log

Delta Lake stores its transaction log as a sequence of JSON files (`_delta_log/00000000000000000001.json`, etc.) with periodic Parquet checkpoints. The log is a journal of all operations, and the checkpoint is a snapshot of the current table state.

```
Delta Lake log structure:

_delta_log/
  00000000000000000000.json  ← table creation
  00000000000000000001.json  ← first INSERT
  ...
  00000000000000000009.json  ← 10th operation
  00000000000000000010.checkpoint.parquet  ← checkpoint at 10 ops
  00000000000000000011.json  ← 11th operation (after checkpoint)

Reading table state:
  1. Find latest checkpoint file
  2. Read checkpoint.parquet → current set of active data files
  3. Apply any JSON log files after the checkpoint
  4. Result: complete list of data files that comprise the current table

Time travel:
  SELECT * FROM events VERSION AS OF 5;
  1. Walk back through log files to reconstruct state at version 5
  2. This is O(N) in the number of log files since last checkpoint
  → Delta Lake automatically creates checkpoints every 10 commits to bound this cost
```

---

## 12. Code Toolkit

### 12.1 `bst.py` — Binary Search Tree with Balance Checking

```python
"""
bst.py — Binary Search Tree with performance analysis.

Shows:
- BST insert, search, delete
- Height calculation
- Balance factor analysis
- Degenerate case detection
- In-order traversal (produces sorted output)

Run directly for demo including sorted-insertion degenerate case.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import TypeVar, Generic, Optional, Iterator
from collections import deque

K = TypeVar("K")
V = TypeVar("V")


@dataclass
class _Node(Generic[K, V]):
    key: K
    value: V
    left: Optional[_Node] = field(default=None, repr=False)
    right: Optional[_Node] = field(default=None, repr=False)


class BST(Generic[K, V]):
    """
    Unbalanced Binary Search Tree.

    Used to demonstrate degenerate behavior under sorted insertion.
    For production use, prefer AVL or Red-Black trees.
    """

    def __init__(self) -> None:
        self._root: Optional[_Node[K, V]] = None
        self._size: int = 0

    def insert(self, key: K, value: V) -> None:
        """Insert key→value. O(height). Worst case O(N) for sorted input."""
        self._root = self._insert(self._root, key, value)

    def _insert(self, node: Optional[_Node], key: K, value: V) -> _Node:
        if node is None:
            self._size += 1
            return _Node(key, value)
        if key < node.key:
            node.left = self._insert(node.left, key, value)
        elif key > node.key:
            node.right = self._insert(node.right, key, value)
        else:
            node.value = value  # update
        return node

    def search(self, key: K) -> Optional[V]:
        """Return value for key or None. O(height)."""
        node = self._root
        while node is not None:
            if key == node.key:
                return node.value
            node = node.left if key < node.key else node.right
        return None

    def inorder(self) -> Iterator[tuple[K, V]]:
        """Yield (key, value) pairs in sorted key order. O(N)."""
        def _inorder(node):
            if node is None:
                return
            yield from _inorder(node.left)
            yield (node.key, node.value)
            yield from _inorder(node.right)
        yield from _inorder(self._root)

    def height(self) -> int:
        """Return tree height. O(N)."""
        def _height(node):
            if node is None:
                return 0
            return 1 + max(_height(node.left), _height(node.right))
        return _height(self._root)

    def balance_factor(self) -> float:
        """
        Return the max absolute balance factor across all nodes.
        0.0 = perfectly balanced, high values = degenerate.
        """
        max_imbalance = [0]

        def _check(node):
            if node is None:
                return 0
            lh = _check(node.left)
            rh = _check(node.right)
            imbalance = abs(rh - lh)
            if imbalance > max_imbalance[0]:
                max_imbalance[0] = imbalance
            return 1 + max(lh, rh)

        _check(self._root)
        return max_imbalance[0]

    def is_degenerate(self) -> bool:
        """Return True if tree has become a linked list (height == size)."""
        return self.height() == self._size

    def level_order(self) -> list[list[K]]:
        """Return keys level by level (BFS). For visualization."""
        if self._root is None:
            return []
        result = []
        queue = deque([self._root])
        while queue:
            level = []
            for _ in range(len(queue)):
                node = queue.popleft()
                level.append(node.key)
                if node.left:
                    queue.append(node.left)
                if node.right:
                    queue.append(node.right)
            result.append(level)
        return result

    def stats(self) -> dict:
        h = self.height()
        import math
        ideal_height = math.ceil(math.log2(self._size + 1)) if self._size > 0 else 0
        return {
            "size": self._size,
            "height": h,
            "ideal_height": ideal_height,
            "height_ratio": round(h / ideal_height, 2) if ideal_height else 0,
            "max_balance_factor": self.balance_factor(),
            "is_degenerate": self.is_degenerate(),
        }


if __name__ == "__main__":
    print("=== BST Demo ===\n")

    # Balanced (random) insertion
    import random
    random.seed(42)
    keys_random = list(range(1, 16))
    random.shuffle(keys_random)

    bst_random = BST()
    for k in keys_random:
        bst_random.insert(k, k)

    print("Random insertion (15 keys):")
    print(f"  Stats: {bst_random.stats()}")
    print(f"  Levels: {bst_random.level_order()}")

    # Degenerate (sorted) insertion
    bst_sorted = BST()
    for k in range(1, 16):
        bst_sorted.insert(k, k)

    print(f"\nSorted insertion (15 keys):")
    print(f"  Stats: {bst_sorted.stats()}")
    print(f"  Height {bst_sorted.height()} vs ideal {bst_random.stats()['ideal_height']}")
    print(f"  This is {bst_sorted.height() / bst_random.stats()['ideal_height']:.1f}x taller than optimal")

    # In-order traversal produces sorted output
    print(f"\nIn-order (sorted) output from random BST:")
    print(f"  {[k for k, v in bst_random.inorder()]}")
```

### 12.2 `avl_tree.py` — Self-Balancing AVL Tree

```python
"""
avl_tree.py — AVL tree with rotation counting.

Shows:
- All four rotation cases (LL, RR, LR, RL)
- Height maintenance after every insert
- Comparison: rotations on random vs sorted input

Run directly for demo.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import TypeVar, Generic, Optional
import math

K = TypeVar("K")
V = TypeVar("V")


@dataclass
class _AVLNode(Generic[K, V]):
    key: K
    value: V
    left: Optional[_AVLNode] = field(default=None, repr=False)
    right: Optional[_AVLNode] = field(default=None, repr=False)
    height: int = 1


class AVLTree(Generic[K, V]):
    """
    AVL tree — self-balancing BST.

    Maintains |height(left) - height(right)| <= 1 for every node.
    All operations are O(log N) guaranteed.
    """

    def __init__(self) -> None:
        self._root: Optional[_AVLNode] = None
        self._size: int = 0
        self.rotation_count: int = 0

    # ─── Height helpers ─────────────────────────────────────────────────

    def _h(self, node: Optional[_AVLNode]) -> int:
        return node.height if node else 0

    def _bf(self, node: _AVLNode) -> int:
        """Balance factor = right height - left height."""
        return self._h(node.right) - self._h(node.left)

    def _update_height(self, node: _AVLNode) -> None:
        node.height = 1 + max(self._h(node.left), self._h(node.right))

    # ─── Rotations ──────────────────────────────────────────────────────

    def _rotate_left(self, z: _AVLNode) -> _AVLNode:
        """Left rotation at z. Used when right-heavy (bf = +2)."""
        self.rotation_count += 1
        y = z.right
        T2 = y.left
        y.left = z
        z.right = T2
        self._update_height(z)
        self._update_height(y)
        return y  # new root of this subtree

    def _rotate_right(self, z: _AVLNode) -> _AVLNode:
        """Right rotation at z. Used when left-heavy (bf = -2)."""
        self.rotation_count += 1
        y = z.left
        T3 = y.right
        y.right = z
        z.left = T3
        self._update_height(z)
        self._update_height(y)
        return y

    def _rebalance(self, node: _AVLNode) -> _AVLNode:
        """Check and apply rotation(s) if balance factor is ±2."""
        self._update_height(node)
        bf = self._bf(node)

        # Right-heavy (bf = +2)
        if bf == 2:
            if self._bf(node.right) < 0:
                # Right-Left case: right rotate right child first
                node.right = self._rotate_right(node.right)
            return self._rotate_left(node)

        # Left-heavy (bf = -2)
        if bf == -2:
            if self._bf(node.left) > 0:
                # Left-Right case: left rotate left child first
                node.left = self._rotate_left(node.left)
            return self._rotate_right(node)

        return node  # already balanced

    # ─── Public interface ────────────────────────────────────────────────

    def insert(self, key: K, value: V) -> None:
        """O(log N) insert with automatic rebalancing."""
        self._root = self._insert(self._root, key, value)

    def _insert(self, node: Optional[_AVLNode], key: K, value: V) -> _AVLNode:
        if node is None:
            self._size += 1
            return _AVLNode(key, value)
        if key < node.key:
            node.left = self._insert(node.left, key, value)
        elif key > node.key:
            node.right = self._insert(node.right, key, value)
        else:
            node.value = value
            return node
        return self._rebalance(node)

    def search(self, key: K) -> Optional[V]:
        """O(log N) search."""
        node = self._root
        while node:
            if key == node.key:
                return node.value
            node = node.left if key < node.key else node.right
        return None

    def height(self) -> int:
        return self._h(self._root)

    def stats(self) -> dict:
        ideal = math.ceil(math.log2(self._size + 1)) if self._size > 0 else 0
        return {
            "size": self._size,
            "height": self.height(),
            "ideal_height": ideal,
            "rotations": self.rotation_count,
            "rotations_per_insert": round(self.rotation_count / self._size, 3) if self._size else 0,
        }


if __name__ == "__main__":
    import random
    random.seed(42)
    N = 1000

    # Sorted insertion
    avl_sorted = AVLTree()
    for i in range(N):
        avl_sorted.insert(i, i)
    print(f"Sorted insertion ({N} keys):")
    print(f"  {avl_sorted.stats()}")

    # Random insertion
    avl_random = AVLTree()
    keys = list(range(N))
    random.shuffle(keys)
    for k in keys:
        avl_random.insert(k, k)
    print(f"\nRandom insertion ({N} keys):")
    print(f"  {avl_random.stats()}")

    print(f"\nNote: sorted insertion triggers MORE rotations ({avl_sorted.rotation_count})")
    print(f"      but tree height stays O(log N) = {avl_sorted.height()} in both cases")
    print(f"      vs BST which would reach height {N} under sorted insertion")
```

### 12.3 `btree_simulator.py` — B-Tree Branching Factor and Height Analysis

```python
"""
btree_simulator.py — B-tree height and disk I/O analysis.

Computes:
- B-tree height for given row count and branching factor
- Disk reads per lookup at different storage media speeds
- Comparison: AVL tree vs B-tree at database scale

No actual tree implementation — focuses on the math that explains
why B-trees win for on-disk storage.

Run directly for comparison tables.
"""
from __future__ import annotations
import math


def btree_height(n_rows: int, branching_factor: int) -> int:
    """Minimum height of a B-tree holding n_rows with given branching factor."""
    if n_rows <= 0:
        return 0
    return math.ceil(math.log(n_rows, branching_factor))


def avl_height(n_rows: int) -> int:
    """Maximum height of an AVL tree (1.44 * log2(N+2))."""
    if n_rows <= 0:
        return 0
    return math.ceil(1.44 * math.log2(n_rows + 2))


def lookup_latency_ms(
    tree_height: int,
    storage_latency_us: float,
) -> float:
    """Total lookup latency in ms (tree_height random reads × per-read latency)."""
    return tree_height * storage_latency_us / 1000.0


# Storage latency constants (microseconds)
DRAM_US = 0.07      # 70 ns
NVME_US = 100.0     # 100 μs
SATA_SSD_US = 500.0 # 500 μs
HDD_US = 10_000.0   # 10 ms


def format_latency(ms: float) -> str:
    if ms < 0.001:
        return f"{ms*1000*1000:.0f} ns"
    if ms < 1.0:
        return f"{ms*1000:.0f} μs"
    return f"{ms:.1f} ms"


if __name__ == "__main__":
    print("=" * 70)
    print("B-tree vs AVL Tree: Height and Disk Reads at Database Scale")
    print("=" * 70)

    configs = [
        ("10K rows",      10_000),
        ("1M rows",    1_000_000),
        ("100M rows",100_000_000),
        ("1B rows", 1_000_000_000),
    ]

    btree_bf = 500  # Postgres 8KB page, 8-byte integer key: ~585 entries

    print(f"\n{'Table Size':<15} {'AVL Height':>12} {'B-tree Height':>14} "
          f"{'AVL NVMe':>10} {'B-tree NVMe':>12} {'AVL HDD':>10} {'B-tree HDD':>12}")
    print("-" * 85)

    for label, n in configs:
        avl_h = avl_height(n)
        bt_h = btree_height(n, btree_bf)

        avl_nvme = lookup_latency_ms(avl_h, NVME_US)
        bt_nvme = lookup_latency_ms(bt_h, NVME_US)
        avl_hdd = lookup_latency_ms(avl_h, HDD_US)
        bt_hdd = lookup_latency_ms(bt_h, HDD_US)

        print(f"{label:<15} {avl_h:>12} {bt_h:>14} "
              f"{format_latency(avl_nvme):>10} {format_latency(bt_nvme):>12} "
              f"{format_latency(avl_hdd):>10} {format_latency(bt_hdd):>12}")

    print(f"\nB-tree branching factor: {btree_bf} (Postgres 8KB page, 8-byte key)")
    print()

    print("=" * 60)
    print("Branching Factor Sensitivity (1 billion rows)")
    print("=" * 60)
    n = 1_000_000_000
    print(f"\n{'Branching Factor':>18} {'Height':>8} {'NVMe latency':>14} {'HDD latency':>12}")
    print("-" * 56)
    for bf in [2, 10, 50, 100, 500, 1000, 2000]:
        h = btree_height(n, bf)
        nvme_ms = lookup_latency_ms(h, NVME_US)
        hdd_ms = lookup_latency_ms(h, HDD_US)
        avl_label = " ← AVL tree equivalent" if bf == 2 else ""
        print(f"{bf:>18} {h:>8} {format_latency(nvme_ms):>14} {format_latency(hdd_ms):>12}{avl_label}")

    print(f"\nConclusion: branching factor 500-1000 keeps B-tree height at 3-4")
    print(f"regardless of table size up to 1 billion rows.")
    print(f"Each additional level multiplies latency by the storage medium's RTT.")
```

---

## 13. Hands-On Labs

### Lab 1: Observe BST Degeneration Under Sorted Insertion

**Goal:** Empirically measure the performance difference between random and sorted insertion into a BST, and verify that AVL self-balancing prevents degeneration.

```python
# lab1_bst_degeneration.py
"""
Inserts N keys in sorted and random order into BST and AVL tree.
Measures actual search time to demonstrate O(N) vs O(log N) behavior.
"""
import random
import time
import math

from bst import BST
from avl_tree import AVLTree

def measure_search_time(tree, keys_to_find: list, label: str) -> None:
    start = time.perf_counter()
    found = 0
    for k in keys_to_find:
        if tree.search(k) is not None:
            found += 1
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"  {label:<35}: {elapsed_ms:.1f} ms for {len(keys_to_find):,} searches ({found} found)")

if __name__ == "__main__":
    N = 5_000  # Keep small — sorted BST recursion depth can hit Python's limit
    search_keys = list(range(N))

    print(f"=== BST vs AVL: Sorted vs Random Insertion, N={N} ===\n")

    # Sorted insertion
    bst_sorted = BST()
    avl_sorted = AVLTree()
    for k in range(N):
        bst_sorted.insert(k, k)
        avl_sorted.insert(k, k)

    print(f"After sorted insertion:")
    print(f"  BST  height: {bst_sorted.height()} (ideal: {math.ceil(math.log2(N+1))})")
    print(f"  AVL  height: {avl_sorted.height()} (ideal: {math.ceil(math.log2(N+1))})")

    measure_search_time(bst_sorted, search_keys, "BST sorted insertion")
    measure_search_time(avl_sorted, search_keys, "AVL sorted insertion")

    # Random insertion
    random.seed(42)
    keys_rand = list(range(N))
    random.shuffle(keys_rand)

    bst_random = BST()
    avl_random = AVLTree()
    for k in keys_rand:
        bst_random.insert(k, k)
        avl_random.insert(k, k)

    print(f"\nAfter random insertion:")
    print(f"  BST  height: {bst_random.height()} (ideal: {math.ceil(math.log2(N+1))})")
    print(f"  AVL  height: {avl_random.height()} (ideal: {math.ceil(math.log2(N+1))})")

    measure_search_time(bst_random, search_keys, "BST random insertion")
    measure_search_time(avl_random, search_keys, "AVL random insertion")
```

---

### Lab 2: B-Tree Height vs Table Size — Database Scale Simulation

**Goal:** Compute B-tree height at database scale and calculate lookup latency on different storage media.

```python
# lab2_btree_scale.py
"""
Runs btree_simulator.py calculations and extends them with
a cost model for batch lookups (realistic data engineering scenario).
"""
from btree_simulator import btree_height, avl_height, lookup_latency_ms, NVME_US, HDD_US

if __name__ == "__main__":
    print("=== Batch Lookup Cost: B-tree vs Full Table Scan ===\n")

    n = 100_000_000  # 100M rows
    bf = 500
    row_size_bytes = 200
    page_size_bytes = 8192
    rows_per_page = page_size_bytes // row_size_bytes  # ~40

    bt_h = btree_height(n, bf)
    avl_h = avl_height(n)
    total_pages = n // rows_per_page

    # 1,000 random lookups
    n_lookups = 1000

    # B-tree: bt_h reads per lookup (index) + 1 heap fetch
    btree_reads = n_lookups * (bt_h + 1)
    # Full scan: read every page sequentially
    scan_reads = total_pages

    # NVMe: sequential reads are ~10x faster than random (200 MB/s sequential vs 500K IOPS random)
    nvme_random_us = NVME_US
    nvme_seq_us = 0.01  # ~10 GB/s sequential → 8KB page in ~1 μs, but pipeline 0.01μs effective

    btree_latency_s = btree_reads * nvme_random_us / 1e6
    scan_latency_s = scan_reads * nvme_seq_us / 1e6

    print(f"Table: {n:,} rows, ~{n*row_size_bytes/1024**3:.1f} GB, page size {page_size_bytes} B")
    print(f"B-tree height: {bt_h} levels (branching factor {bf})")
    print(f"Task: {n_lookups:,} random lookups by primary key\n")

    print(f"B-tree index:     {btree_reads:,} random reads   → {btree_latency_s:.3f} s")
    print(f"Full table scan:  {scan_reads:,} seq reads      → {scan_latency_s:.3f} s")
    print(f"Speedup: {scan_latency_s/btree_latency_s:.0f}×")

    print(f"\nConclusion: for {n_lookups} lookups, B-tree is ~{scan_latency_s/btree_latency_s:.0f}x faster.")
    print(f"For full table scans (Analytics), Seq Scan is preferred — no index traversal overhead.")
```

---

### Lab 3: Postgres Index Selectivity — When Does the B-Tree Index Get Used?

**Goal:** Observe how Postgres's query planner decides between a B-tree index scan and a sequential scan based on result selectivity.

```python
# lab3_index_selectivity.py
"""
Simulates Postgres planner selectivity decisions.
Demonstrates when a B-tree index is used vs a sequential scan.

Note: this simulates the cost model — for real observation,
run the SQL commands in a Postgres instance with EXPLAIN ANALYZE.
"""

def planner_decision(
    n_rows: int,
    selectivity: float,  # fraction of rows returned (0.0 to 1.0)
    seq_page_cost: float = 1.0,   # Postgres default
    random_page_cost: float = 4.0,  # Postgres default for spinning disk
    rows_per_page: int = 40,
    btree_height: int = 4,
) -> dict:
    """
    Simplified Postgres cost model for index scan vs sequential scan.
    Postgres uses costs in abstract 'page fetch units'.
    """
    n_matching = int(n_rows * selectivity)
    total_pages = n_rows // rows_per_page

    # Sequential scan cost
    seq_cost = total_pages * seq_page_cost

    # Index scan cost (simplified):
    # - btree_height random reads to find the leaf
    # - n_matching random heap fetches (each row may be on a different page)
    # - correlation factor: if table is ordered by index key, heap fetches are sequential
    index_descent = btree_height * random_page_cost
    heap_fetch_cost = n_matching * random_page_cost  # worst case: random
    index_cost = index_descent + heap_fetch_cost

    chosen = "Index Scan" if index_cost < seq_cost else "Seq Scan"
    return {
        "n_rows": n_rows,
        "n_matching": n_matching,
        "selectivity": f"{selectivity:.1%}",
        "seq_cost": round(seq_cost, 1),
        "index_cost": round(index_cost, 1),
        "chosen": chosen,
    }

if __name__ == "__main__":
    print("=== Simulated Postgres Planner: Index vs Seq Scan ===\n")
    print(f"{'Selectivity':>12}  {'Matching rows':>14}  {'Seq cost':>10}  {'Idx cost':>10}  {'Decision':>12}")
    print("-" * 65)

    n = 10_000_000  # 10M rows
    for sel in [0.0001, 0.001, 0.01, 0.05, 0.10, 0.20, 0.50, 1.0]:
        r = planner_decision(n, sel)
        print(f"{r['selectivity']:>12}  {r['n_matching']:>14,}  {r['seq_cost']:>10.1f}  "
              f"{r['index_cost']:>10.1f}  {r['chosen']:>12}")

    print(f"\nNote: crossover typically at 5-10% selectivity for spinning disk.")
    print(f"On NVMe (random_page_cost closer to 1.1), index wins at higher selectivity.")
    print(f"Set 'random_page_cost = 1.1' in postgresql.conf for NVMe-backed Postgres.")
```

---

## 14. Interview Q&A

**Q1: Explain why Postgres uses B-tree indexes instead of hash indexes for most queries, even though hash lookup is O(1) and B-tree is only O(log N).**

The O(1) vs O(log N) comparison is misleading when applied to database indexes, because it ignores the constant factors involved in disk-based data structures. A hash index provides O(1) lookup for exact equality, but it cannot answer range queries, order queries, or LIKE prefix queries — all of which are extremely common in data engineering workloads. `WHERE event_date BETWEEN '2024-01-01' AND '2024-03-31'` is fundamentally unanswerable with a hash index because hash functions destroy order: `hash('2024-01-01')` has no mathematical relationship to `hash('2024-01-02')`.

A B-tree index answers all of these queries because it maintains sorted order by key. The tree structure lets you navigate to the start of a range in O(log N) node reads, then scan forward through linked leaf pages for the range portion. The O(log N) cost is practically small: for a billion-row table with a branching factor of 500, the B-tree height is 4. Four disk reads to find any row in a billion-row table. At 100 μs per NVMe read, that's 400 μs total — fast enough for OLTP.

Hash indexes also have operational downsides in Postgres specifically: prior to Postgres 10, hash indexes were not WAL-logged, making them unsafe across crash recovery. They are not used for multi-column indexes. They cannot be used with partial indexes. The combination of functional limitations and historical reliability issues means B-tree is the correct default, and hash indexes are only appropriate for the specific case of equality lookups on a high-selectivity, single column where range queries truly never occur.

**Q2: Walk me through exactly what happens when Postgres executes `SELECT * FROM events WHERE user_id = 'u_12345'` with a B-tree index on user_id.**

The query planner checks whether a B-tree index on `user_id` exists and estimates the query's selectivity — the fraction of rows that match the predicate. If `user_id` is high-cardinality (each value appears rarely), selectivity is very low and the planner chooses an index scan.

The executor starts at the B-tree root page, which is cached in the shared buffer pool after the first access. The root page contains sorted key values and child page pointers. The executor compares 'u_12345' against the keys in the root page using binary search — this takes O(log(keys_per_page)) comparisons, effectively O(1) since keys_per_page is bounded. It follows the appropriate child pointer to an internal page, repeating the comparison until it reaches a leaf page. The leaf page contains entries of the form (key='u_12345', TID=(page_number=4522, slot=7)), where TID is the heap tuple identifier.

The executor then fetches heap page 4522 from the shared buffer pool (or from disk if not cached). Within that page, it reads slot 7 — a fixed-offset access within the 8 KB page. It then performs a visibility check: each heap tuple carries transaction IDs (xmin for the inserting transaction, xmax for the deleting/updating transaction). The executor compares these against its current transaction snapshot to determine if the tuple is visible. If visible, the row is returned.

For multiple matching rows (non-unique index), the executor returns to the leaf page and follows the next matching entry, potentially fetching additional heap pages. If the index is a covering index (contains all needed columns), the heap fetch is skipped entirely (index-only scan), reducing I/O to only the B-tree traversal.

**Q3: You're designing an Airflow metadata database that handles 100 million task instance rows. Which indexes would you create and why?**

Airflow's most frequent query patterns are: (1) find all pending task instances for a specific DAG run, (2) find all task instances for a specific task ID across all runs, (3) find the most recent run of a DAG, (4) update task state as tasks start and complete.

For pattern 1, I'd create a compound B-tree index on `(dag_id, run_id, state)`. The compound index serves both the DAG+run lookup and the state filter within that run. The leftmost-prefix rule means this index also serves queries on `(dag_id, run_id)` and `(dag_id)` alone. The `state` column in the index means Postgres can filter `WHERE state = 'queued'` without fetching heap rows for all task instances in the run.

For pattern 3, I'd create an index on `(dag_id, execution_date DESC)`. The `DESC` ordering lets Postgres use the index for `ORDER BY execution_date DESC LIMIT 1` without sorting — it just reads the first leaf entry in the reverse-ordered index. Without this, finding the latest run requires a full scan of the dag_runs table.

I would explicitly NOT create an index on `state` alone. State has very low cardinality (queued, running, success, failed — maybe 8 values), so a query for `WHERE state = 'running'` might match 10% of all rows, at which point Postgres's cost model correctly ignores the index in favor of a sequential scan. A partial index `ON task_instance(dag_id, run_id) WHERE state IN ('queued', 'running')` is much better — it's small (only active tasks are indexed) and always selective.

**Q4: What is the difference between a B-tree and a B+ tree, and which does Postgres use?**

A B-tree stores data in both internal nodes and leaf nodes. A search can terminate at any level of the tree if the key is found in an internal node. This means some lookups take fewer disk reads — if a key happens to be in an internal node near the root, you avoid descending to the leaves.

A B+ tree stores data only in leaf nodes. Internal nodes contain only keys for routing, no actual values or row pointers. All leaf nodes are connected as a doubly linked list. A lookup always terminates at a leaf, so the height is the lookup cost in all cases. But because internal nodes contain only keys (not values), each internal page can hold more keys — higher branching factor, lower tree height. And the leaf linking makes range scans extremely efficient: after a B-tree descent to the start of the range, you follow sibling pointers forward without ever climbing back up the tree.

Postgres uses B+ trees for all standard indexes. The leaf-linking property is why Postgres can efficiently execute range queries (`BETWEEN`, `>`, `<`), `ORDER BY` without sorting, and `LIMIT` with ordered scans. The range scan `WHERE id BETWEEN 1000 AND 2000` does three-to-four pages of tree descent, then an in-order scan of leaf pages until the upper bound — O(log N + K) where K is the number of matching rows, which is optimal.

**Q5: A data engineer at your company creates a B-tree index on every column in a 50-column wide table to "make all queries fast." What are the consequences?**

Over-indexing is a well-understood production problem. Each B-tree index is a separate on-disk data structure that must be updated on every INSERT, UPDATE, and DELETE to the table. For a 50-column table with 50 indexes, every row insert touches 51 structures: the heap table plus 50 index trees. Each of those 50 index updates is a B-tree traversal plus a page write, plus a WAL entry. The write amplification factor is approximately 50×.

The concrete consequences: first, INSERT and UPDATE throughput drops dramatically. A table that could sustain 50,000 inserts/second with 2 indexes might only sustain 2,000 inserts/second with 50 indexes. For a data engineering pipeline loading events, this directly limits pipeline throughput. Second, autovacuum workload increases 50× — every dead tuple left by an UPDATE must be cleaned from 50 index trees, not just the heap. Third, the total index size may exceed the table size — a 100 GB events table with 50 indexes on various columns might have 200 GB of index data, consuming disk and DRAM page cache. Fourth, the query planner has 50 index candidates to evaluate for every query, which slightly increases planning time.

The correct approach is to index selectively: only columns that appear in WHERE clauses of frequently-executed queries, with attention to selectivity (don't index low-cardinality columns), compound indexes for multi-column filters, and covering indexes only when heap fetch elimination justifies the extra size. A good rule of thumb for OLTP tables is 3–6 indexes maximum.

**Q6: How does Iceberg's metadata tree achieve query pruning similar to a B-tree index, and what are the limitations?**

Iceberg's three-level metadata tree — snapshot → manifest list → manifest files → data files — stores column-level statistics (min/max values per column, per data file) in the manifest files. When a query includes a filter predicate like `WHERE event_date = '2024-01-15'`, Iceberg evaluates the predicate against the min/max bounds in each manifest file. If a manifest's max date is '2024-01-10' and the query asks for '2024-01-15', the entire manifest (and all its data files) is skipped without reading a single Parquet byte.

This is structurally similar to a B-tree in that it uses hierarchical metadata to eliminate data without reading it. The key difference is granularity and ordering. A B-tree index is sorted by key — every row in the table is exactly positioned in the index tree, enabling O(log N) lookup of any single row. Iceberg's statistics are per-file, not per-row — if a data file contains rows with event_date from '2024-01-01' to '2024-01-31', a query for '2024-01-15' cannot eliminate that file even if '2024-01-15' represents only 1/31 of the rows in the file.

The practical consequence is that Iceberg query pruning is most effective when data files are narrowly bounded in the filter column — ideally through partitioning by that column (each partition file covers exactly one date) or through data clustering (files are sorted by the filter column so min/max ranges are tight). Without partitioning, a single data file might span a year of data, making min/max pruning useless. Iceberg is a file-level filter, not a row-level filter. For row-level filtering, you still need full Parquet column reads with predicate pushdown to the column chunk level.

---

## 15. Cross-Question Chain

**Q1 [Interviewer]: What is a binary search tree?**

A binary search tree is a rooted binary tree where every node stores a key, with the BST property: all keys in the left subtree are strictly less than the node's key, and all keys in the right subtree are strictly greater. This property makes binary search possible on a dynamic, mutable data structure. At each node, you compare your target key and descend left or right — eliminating half the remaining search space at each step. In a balanced BST of height h, lookup takes O(h) = O(log N) comparisons. In-order traversal of a BST — left child, then node, then right child — visits all nodes in sorted key order, which hash tables cannot do.

**Q2 [Interviewer]: You said "in a balanced BST." What does balanced mean, and what happens when it's not balanced?**

A BST is balanced when its height is O(log N) — the left and right subtrees at every node are approximately equal in size. An AVL tree enforces that the heights of any node's children differ by at most 1. A Red-Black tree enforces a looser constraint but still guarantees O(log N) height. Without self-balancing, a BST degenerates under adversarial insertion orders. The worst case is sorted insertion: inserting keys 1, 2, 3, ..., N in order produces a right-leaning chain, height N, every lookup O(N) — identical to a linked list. This is not a theoretical concern: database rows inserted in primary key order (auto-increment, sequential IDs) would produce exactly this degenerate BST. This is precisely why databases don't use BSTs for their indexes.

**Q3 [Interviewer]: So databases use AVL trees instead?**

No — AVL trees are in-memory data structures. The problem with any binary tree for a database index is not the algorithmic complexity but the physical I/O model. An AVL tree with N = 100 million rows has height ≈ 27. Every node is a small object (maybe 40 bytes). The tree doesn't fit in DRAM for a large table, so each node access that isn't in cache requires a random disk read — 100 μs on NVMe SSD, 10 ms on HDD. That's 27 × 100 μs = 2.7 ms per lookup on NVMe, or 27 × 10 ms = 270 ms per lookup on HDD. Neither is acceptable for a production OLTP database handling thousands of queries per second. The solution is not a different self-balancing algorithm but a fundamentally different node design: make each node much larger so the tree height collapses.

**Q4 [Interviewer]: That's the B-tree. Explain the key design decision that makes B-trees fast for disk.**

The key decision is the large branching factor. Each B-tree node holds hundreds to thousands of keys — exactly as many as fit in one disk page (typically 4 or 8 KB for databases). When you read one B-tree node from disk, you bring in one full page, which contains say 500 keys and 500 child pointers. Binary search within that page takes O(log 500) = ~9 comparisons, all in-memory (no additional disk reads). At each of the ~4 levels of the tree, you pay one disk read. For a billion-row table with branching factor 500, height is 4 — four disk reads to find any row. The tree height is logarithm base 500 of N, not logarithm base 2, and each disk read retrieves 500× more information than an AVL node read. This is why the practical difference between AVL and B-tree on disk is not the algorithmic class (both are O(log N)) but the constant: height 27 vs height 4.

**Q5 [Interviewer]: We're at a company using Postgres. A particular query doing `WHERE user_id = ?` is slow. How do you diagnose and fix it?**

First I run `EXPLAIN ANALYZE` on the query to see what Postgres's planner chose. If I see `Seq Scan on events` with a large `rows` estimate, the planner is doing a full table scan either because no index exists or because the index exists but the planner's selectivity estimate makes the sequential scan cheaper.

If no index exists: `CREATE INDEX CONCURRENTLY events_user_id_idx ON events(user_id)`. The `CONCURRENTLY` flag is critical in production — without it, the index build takes an `ACCESS SHARE` lock that blocks writes for the entire build duration.

If the index exists but isn't used: I check whether the statistics are fresh with `\d+ events` and `ANALYZE events`. If `pg_stats` shows a low correlation (heap data is physically ordered differently from the index order), Postgres assumes every matching row requires a separate random I/O, which raises the index scan cost. I also check whether the query parameters have low selectivity — if `user_id = 'admin'` matches 40% of the table, the sequential scan is correctly chosen. In that case, the correct fix is a partial index: `CREATE INDEX events_rare_users ON events(user_id) WHERE user_id NOT IN ('admin', 'system')`.

If the index exists and is used but is still slow: I check for index bloat with `pgstattuple` — high dead_tuple_percent means autovacuum isn't keeping up. I also verify `random_page_cost` in `postgresql.conf` reflects the actual storage: default is 4.0 (spinning disk), but for NVMe it should be 1.1. An incorrect `random_page_cost` makes Postgres over-weight the cost of index scans, causing it to prefer sequential scans inappropriately.

**Q6 [Interviewer]: You're designing a data lake on Iceberg for a 100 TB event table. How do you structure the metadata and partitioning to achieve fast queries without a traditional B-tree index?**

Iceberg doesn't have B-tree indexes — it uses file-level metadata statistics for query pruning. The goal is to make those statistics as tight as possible so the pruning eliminates as many data files as possible before any Parquet data is read.

The primary lever is partition design. For an event table, I'd partition by `DATE(event_time)` — this creates one set of manifest files per day, and each data file covers at most one day. A query for `WHERE event_time BETWEEN '2024-01-01' AND '2024-01-31'` eliminates all other days' manifest files in step 2 of the metadata traversal. For a 100 TB table spanning 3 years, that's 2 years and 11 months of data (about 97%) eliminated before reading any actual Parquet.

The secondary lever is sort order within each partition. If events within a day are sorted by `user_id`, then the column statistics in each row group will have a tight min/max range for `user_id` — perhaps [u_00001, u_03000] for one row group and [u_03001, u_06000] for the next. A query for `WHERE event_time = '2024-01-15' AND user_id = 'u_05000'` first prunes to January 15th data files (via partition), then within those files, Parquet predicate pushdown eliminates all row groups whose user_id range doesn't include 'u_05000'. This is a two-level pruning cascade — partition-level via Iceberg manifest statistics, then column-chunk-level via Parquet statistics.

What I would not do is rely solely on Iceberg statistics for point lookups. For workloads that need to retrieve a single event by ID, I'd use a secondary index layer — either a Z-order (space-filling curve) sort that co-locates events by multiple columns, or an external inverted index maintained alongside the Iceberg table. Iceberg's metadata tree is optimized for scan elimination, not point lookup.

---

## 16. Common Misconceptions

**"B-tree and binary tree are similar — both are trees."**
A binary tree has branching factor 2 — every node has at most 2 children. A B-tree has branching factor M (typically 100–1000 for database pages). The name "B-tree" does not stand for "binary tree" — it stands for "balanced tree" (or "Boeing tree" or "Bayer tree," depending on the source). The difference is not cosmetic: branching factor 2 gives O(log₂ N) height, while branching factor 500 gives O(log₅₀₀ N) height. For 1 billion rows: 30 levels vs 4 levels.

**"Hash indexes are always faster than B-tree indexes for equality lookups."**
Hash indexes provide O(1) lookup, but in Postgres specifically, the constant factor often makes them similar to B-tree in practice. B-tree height for a billion-row table is 4 disk reads — the same order of magnitude as a hash index. The B-tree wins on functionality: it handles range queries, `ORDER BY`, `LIKE 'prefix%'`, and multi-column index use cases that hash indexes cannot.

**"A BST is sorted, so it's like a sorted array."**
The BST property maintains sorted order in the sense that in-order traversal produces sorted output. But the physical layout in memory is not sorted — nodes point to each other via pointers, and the physical addresses are arbitrary. A sorted array has O(log N) binary search AND sequential access (cache-friendly). A BST has O(log N) binary search but pointer-chasing traversal (cache-hostile). B+ tree leaf pages are sorted AND linked, giving the best of both: O(log N) search and O(K) cache-friendly range scan.

**"You should always create indexes on foreign keys."**
In Postgres, foreign keys are not automatically indexed (unlike MySQL's InnoDB). An unindexed foreign key causes full table scans on joins. But blindly indexing all foreign keys without considering whether those joins are actually executed creates unnecessary write overhead. The correct approach is to index foreign keys that appear in JOIN conditions in executed queries.

**"Iceberg's metadata replaces the need for any index."**
Iceberg's metadata statistics (min/max per file) provide file-level pruning, which is coarse-grained. Row-level filtering still requires reading Parquet columns and applying predicates at the column-chunk level (Parquet statistics) or row-level (full column decode + filter). For point lookups or high-selectivity queries on non-partitioned columns, Iceberg provides no better pruning than a sequential file scan. It excels at eliminating entire files/partitions, not at accelerating within-file row selection.

---

## 17. Performance Reference Card

| Structure | Search | Insert | Delete | Range Scan | Space |
|---|---|---|---|---|---|
| Unsorted array | O(N) | O(1) amortized | O(N) | O(N) | O(N) |
| Sorted array | O(log N) | O(N) | O(N) | O(log N + K) | O(N) |
| BST (balanced) | O(log N) | O(log N) | O(log N) | O(log N + K) | O(N) |
| BST (degenerate) | O(N) | O(N) | O(N) | O(N) | O(N) |
| AVL tree | O(log N) | O(log N) | O(log N) | O(log N + K) | O(N) |
| B-tree (order M) | O(log_M N) | O(log_M N) | O(log_M N) | O(log_M N + K) | O(N) |
| B+ tree | O(log_M N) | O(log_M N) | O(log_M N) | O(log_M N + K) | O(N) |
| Hash table | O(1) avg | O(1) amortized | O(1) avg | **Impossible** | O(N) |

**Disk reads per lookup (1 billion rows, NVMe SSD):**

| Structure | Reads | Latency |
|---|---|---|
| AVL tree (in-memory) | 0 (L3/DRAM) | ~2 μs |
| AVL tree (on-disk) | ~30 | ~3,000 μs |
| B-tree (branching=500) | 4 | ~400 μs |
| Full table scan | ~2.5M seq pages | ~25,000 μs (if cached) |
| Hash index | 1–2 | ~200 μs |

---

## 18. Connections to Other Modules

**CSF-ALG-101 M01 (Complexity Analysis):** The O(log N) analysis of BST search uses the same halving argument as binary search. The O(log_M N) analysis of B-tree adds the branching factor insight: log base 2 vs log base 500 for the same N.

**CSF-ALG-101 M02 (Hash Tables):** The core contrast of this module: hash tables give O(1) but can't range scan; B-trees give O(log N) but support all query types. Choosing between them requires understanding both — Postgres exposes this choice via `CREATE INDEX USING HASH` vs the default B-tree.

**CSF-ARC-102 M01 (Memory Hierarchy):** The entire motivation for B-trees is disk I/O latency. Without understanding that NVMe is 1,400× slower than DRAM per random read, the branching factor optimization seems arbitrary. With it, the math is clear: reduce disk reads from 30 to 4 by packing 500 keys per node.

**DBE-INT-101 M01 (B-Tree Storage Engines):** That module covers B-tree page layout, splits, and WAL in depth from the database implementation perspective. This module provides the algorithmic foundation (why B-trees, what height guarantees they provide) that makes the implementation details interpretable.

**CPL-CLD-102 M01 (Apache Iceberg):** Iceberg's metadata tree (snapshot → manifests → data files) applies the hierarchical pruning principle of B-trees to distributed file storage. Understanding B-tree pruning (why hierarchical structure enables O(log N) elimination) makes Iceberg's partition pruning and manifest statistics immediately intuitive.

---

## 19. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is the BST property? | For every node X: left subtree keys < X.key < right subtree keys. Enables O(log N) binary search in a balanced tree. |
| 2 | Why does sorted insertion degenerate a BST? | Each new key is always larger than all existing keys → always goes right → tree becomes a right-leaning linked list of height N |
| 3 | What is the balance factor of an AVL tree node? | height(right subtree) − height(left subtree). AVL maintains |balance_factor| ≤ 1 for every node. |
| 4 | What are the four AVL rotation cases? | LL (right rotate), RR (left rotate), LR (left rotate child, then right rotate), RL (right rotate child, then left rotate) |
| 5 | What is the maximum height of an AVL tree with N nodes? | 1.44 × log₂(N+2) — tighter than Red-Black but same O(log N) class |
| 6 | Why can't AVL trees (or any binary tree) be used for database indexes efficiently? | Binary tree height for N=10⁹ is ~30. Each node is a separate disk read (100 μs on NVMe). 30 reads × 100 μs = 3 ms per lookup. B-tree gets this to 4 reads. |
| 7 | What is the B-tree invariant for all leaf nodes? | All leaf nodes are at the same depth — the tree is height-balanced by construction, not by rotation |
| 8 | What is the branching factor of a Postgres B-tree for an 8-byte integer key on an 8 KB page? | ~500–585 entries per page. Height for 10⁹ rows: ⌈log₅₀₀(10⁹)⌉ = 4 levels |
| 9 | What is the key difference between a B-tree and a B+ tree? | B-tree: values in all nodes, search can terminate early. B+ tree: values only in leaves, leaves linked as doubly linked list for range scans |
| 10 | Why does Postgres use B+ trees instead of B-trees? | Leaf linking enables efficient range scans (follow sibling pointers, no backtracking). Internal nodes store keys only → higher branching factor → lower height. |
| 11 | What happens to a B-tree when a leaf node is full and a new key is inserted? | The leaf splits into two nodes; the median key is pushed up to the parent. If the parent is also full, the split propagates upward (possibly to root). |
| 12 | Why is B-tree growth from the root (not the leaves)? | Splits propagate upward and only at the root does the tree gain a new level. This preserves the invariant that all leaves are at the same depth. |
| 13 | What is a Postgres heap TID? | A 6-byte tuple identifier: (page_number, slot_number). Stored in B-tree leaf entries to point back to the actual row in the heap table. |
| 14 | What is an index-only scan in Postgres? | A scan where all needed columns are in the index itself — no heap fetch required. Requires the visibility map to be clean (set by VACUUM). |
| 15 | When does Postgres choose a sequential scan over a B-tree index scan? | When estimated selectivity is high (many rows match), making random heap fetches more expensive than a sequential full-table scan. Crossover typically at 5–10% for spinning disk, higher for NVMe. |
| 16 | What is the purpose of Parquet row group statistics? | Store min/max values per column per row group. Enable predicate pushdown: Spark/engines skip row groups whose min/max range excludes the query predicate. |
| 17 | What does Iceberg's manifest file contain? | A list of data files in one partition, with per-file statistics (record count, column min/max). Used for partition pruning and file-level filtering before reading any Parquet. |
| 18 | Why does Postgres default random_page_cost to 4.0 and what should it be for NVMe? | 4.0 reflects spinning disk (10 ms random vs 2.5 ms sequential = 4× ratio). For NVMe, random and sequential are much closer — set to 1.1 so the planner correctly favors index scans. |
| 19 | What is a covering index? | An index that includes all columns needed by a query (via INCLUDE clause in Postgres). Enables index-only scans by storing extra data in leaf nodes, eliminating heap fetches. |
| 20 | What is write amplification in B-tree indexes? | Each row INSERT/UPDATE/DELETE must update every index on the table. A table with 10 indexes has ~11× write amplification. This is why over-indexing degrades write throughput. |

---

## 20. Further Reading

**Foundational papers:**
- Bayer, R. & McCreight, E. (1972). *Organization and Maintenance of Large Ordered Indices*. Acta Informatica. The original B-tree paper.
- Comer, D. (1979). *The Ubiquitous B-Tree*. ACM Computing Surveys. The most-cited B-tree survey; explains all variants.

**Database internals:**
- Kleppmann, M. (2017). *Designing Data-Intensive Applications*, Chapter 3 (Storage and Retrieval). Best practical treatment of B-trees vs LSM-trees for data engineers.
- Postgres source: `src/backend/access/nbtree/` — the Postgres B-tree implementation. `README` in that directory is unusually readable.
- Postgres documentation: "Index Internals" section explains page layout and the access method API.

**Data lake formats:**
- Apache Iceberg specification: https://iceberg.apache.org/spec/ — Section 2 (Metadata) describes the snapshot/manifest/file tree structure.
- Parquet format specification: https://parquet.apache.org/docs/file-format/ — Row group and column chunk structure with statistics.

---

## 21. Module Summary

Trees solve the problem that hash tables cannot: maintaining order while providing efficient search. A binary search tree places all smaller keys left and all larger keys right, enabling O(log N) search in a balanced tree and O(K) range scans via in-order traversal. The failure mode is degeneration under sorted insertion — turning O(log N) into O(N). AVL trees solve this with rotations that maintain height ≤ 1.44 log₂ N after every operation.

Neither BST nor AVL trees are suitable for disk-based storage. With node size of ~40 bytes and 100 million rows, the tree doesn't fit in DRAM; each node access is a disk read at 100 μs on NVMe. Height 27 × 100 μs = 2.7 ms per lookup — too slow for OLTP. B-trees solve this by maximizing branching factor: each 8 KB page holds ~500 keys, collapsing tree height to 3–4 for billion-row tables. Four disk reads at 100 μs = 400 μs — fast enough for production.

B+ trees extend B-trees by storing values only in leaves (maximizing internal node branching factor) and linking leaves as a doubly linked list (enabling O(log N + K) range scans without backtracking). Postgres uses B+ trees as its default index type precisely because data engineering workloads are dominated by range queries, ORDER BY, and joins — all of which hash indexes cannot serve.

The same hierarchical pruning principle appears across data lake formats: Parquet uses per-row-group statistics to skip column data, and Iceberg uses a three-level metadata tree (snapshot → manifests → files) to skip entire data files. These are not B-trees, but they apply the same insight: hierarchical structure enables logarithmic elimination before reading the data.

---

**CSF-ALG-101: 3 of 5 modules complete.**  
**Next: M04 — Sorting Algorithms (merge sort, external merge sort — the algorithm behind Spark shuffle)**
