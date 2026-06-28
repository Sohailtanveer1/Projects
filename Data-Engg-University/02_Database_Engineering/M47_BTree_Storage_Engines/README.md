# M47: B-Tree Storage Engines

**Course:** DBE-INT-101 — Storage Engine Internals  
**Module:** 01 of 05  
**Global Module ID:** M47  
**Semester:** 2  
**School:** Database Engineering (DBE)

---

## Table of Contents

1. [The Why](#1-the-why)
2. [Mental Model](#2-mental-model)
3. [Core Concepts](#3-core-concepts)
4. [Hands-On Walkthrough](#4-hands-on-walkthrough)
5. [Code Toolkit](#5-code-toolkit)
6. [Visual Reference](#6-visual-reference)
7. [Common Mistakes](#7-common-mistakes)
8. [Production Failure Scenarios](#8-production-failure-scenarios)
9. [Performance and Tuning](#9-performance-and-tuning)
10. [Interview Q&A](#10-interview-qa)
11. [Cross-Question Chain](#11-cross-question-chain)
12. [Flashcards](#12-flashcards)
13. [Further Reading](#13-further-reading)
14. [Lab Exercises](#14-lab-exercises)
15. [Key Takeaways](#15-key-takeaways)
16. [Connections to Other Modules](#16-connections-to-other-modules)
17. [Anti-Patterns](#17-anti-patterns)
18. [Tools Reference](#18-tools-reference)
19. [Glossary](#19-glossary)
20. [Self-Assessment](#20-self-assessment)
21. [Module Summary](#21-module-summary)

---

## 1. The Why

A data engineer who treats a database as a black box will cargo-cult index design, misunderstand query plan choices, and be unable to explain production performance problems. The person who understands B-trees — what a page is, why the branching factor matters, what a page split costs, and what write amplification means — can answer questions that the black-box engineer cannot: Why is this UPDATE slow? Why is this index hurting write performance? Why does an index with low selectivity sometimes make reads slower? Why does Postgres VACUUM exist?

The B-tree is the dominant data structure for read-optimized storage engines. Postgres, MySQL InnoDB, SQLite, Oracle — all use B-tree variants as their primary index structure. BigQuery's underlying Capacitor format is column-oriented but its metadata index uses a B-tree-like structure. Understanding B-trees is prerequisite knowledge for every database you will work with as a data engineer.

This module does not assume you remember your undergraduate data structures course. It builds B-trees from first principles — starting from the physical constraint (disk access is slow; sequential reads are fast; random reads are slow) and deriving the B-tree as the optimal response to that constraint. Every property of B-trees — the high branching factor, the guaranteed O(log N) access, the page-aligned node sizes, the split-on-overflow strategy — is a design choice forced by the hardware.

---

## 2. Mental Model

### Why Trees at All?

A database must support range queries: "find all orders between dates D1 and D2." A hash table is O(1) for exact lookups but cannot answer range queries. A sorted array supports binary search (O(log N)) and range scans, but insertions require shifting elements — O(N). A binary search tree (BST) supports O(log N) search and O(log N) insert, but BSTs are not disk-friendly: each node is ~40 bytes, and accessing a child requires following a pointer — potentially a random disk read for each level of the tree.

The B-tree solves the disk problem with one design change: instead of two children per node (binary), allow hundreds or thousands of children per node. The entire node (with all its keys and child pointers) fits in one disk page (typically 4KB or 8KB). Reading one node requires one I/O regardless of the branching factor. With a branching factor of 500, a 3-level B-tree can index 500^3 = 125 million rows — with at most 3 disk reads per lookup.

### The Page as the Unit of I/O

Everything in a B-tree storage engine revolves around the **page** (also called a "block" or "node"). A page is a fixed-size region of disk (typically 4KB, 8KB, or 16KB). The page is the unit of:
- **I/O:** The OS reads and writes one page at a time. Reading one byte from a page requires reading the entire page.
- **Locking:** Concurrent transactions lock pages (or smaller portions), not individual rows.
- **Memory allocation:** The buffer pool (covered in M50) caches pages in RAM, evicting whole pages.
- **B-tree structure:** Each B-tree node is exactly one page. The branching factor is the number of key-child pairs that fit in one page.

---

## 3. Core Concepts

### 3.1 B-Tree vs B+-Tree

The original **B-tree** stores data records at all levels (internal nodes and leaf nodes). The **B+-tree** stores data records only in leaf nodes; internal nodes contain only keys and child pointers (routing keys). B+-trees dominate in practice because:
- Internal nodes with only keys (no data) achieve higher branching factor (more keys fit per page)
- Leaf nodes are linked in a doubly-linked list — range scans scan the leaf level without traversing the tree
- All range query access is at the leaf level — predictable I/O pattern

When database engineers say "B-tree," they almost always mean B+-tree. This module uses the terms interchangeably.

### 3.2 Page Layout

A B+-tree leaf page in Postgres (8KB default):

```
Leaf Page (8192 bytes):
┌──────────────────────────────────────────────────────────┐
│ PageHeader (24 bytes)                                     │
│   lsn (8): WAL position of last change to this page      │
│   checksum (2): page integrity check                      │
│   flags (2): all_visible, all_frozen, has_free_lines      │
│   lower (2): pointer to start of free space               │
│   upper (2): pointer to end of free space                 │
│   special (2): pointer to special space (index type data) │
├──────────────────────────────────────────────────────────┤
│ ItemIdArray (4 bytes × N items)                           │
│   [offset, flags, length] for each item                   │
├──────────────────────────────────────────────────────────┤
│ Free Space                                                │
│   (grows from lower upward, items inserted from upper     │
│    downward — meets in the middle)                        │
├──────────────────────────────────────────────────────────┤
│ Item data (heap tuples or index entries)                   │
│   Each index entry: (key, heap_tid)                       │
│   heap_tid: (page_number, offset) in the heap file        │
├──────────────────────────────────────────────────────────┤
│ Special Space (varies by index type)                       │
│   B-tree: left_sibling, right_sibling, level, flags       │
└──────────────────────────────────────────────────────────┘
```

The `right_sibling` pointer in the special space is how Postgres links leaf pages for range scans — following the linked list at the leaf level without re-traversing from the root.

### 3.3 The Branching Factor and Tree Height

For a B+-tree with branching factor `b`:
- Each internal node holds `b` child pointers and `b-1` keys
- Tree height `h = ceil(log_b(N))` where N = number of leaf pages
- Lookups require `h` random I/Os (one per level)

Example: Postgres, 8KB pages, 64-byte key+pointer entries:
- Keys per internal page ≈ 8192 / 64 ≈ 128 (conservative — actual is higher with variable-length keys)
- Each leaf page holds ≈ 100-200 row pointers
- Height for 100M rows: h = ceil(log_128(100M / 150)) ≈ ceil(log_128(667K)) ≈ 3
- **3 random disk reads for any lookup in a 100M row table**

This is why B-trees are fast: the high branching factor (128-500 in practice) gives very low tree height. Even for billion-row tables, height rarely exceeds 4-5.

### 3.4 Page Split

When a B+-tree page fills up, it must split:

1. Allocate a new page
2. Move approximately half the keys/items to the new page
3. Insert the "middle key" (the split key) into the parent page, with a pointer to the new page
4. If the parent also overflows → split the parent recursively (can cascade to the root)
5. If the root splits → create a new root (tree grows taller by 1)

**Why splits are expensive:**
- Requires writing 2-3 pages (original, new sibling, parent with updated split key)
- Requires WAL entries for all modified pages (for crash recovery)
- Requires exclusive lock on affected pages during the split

**Fragmentation from splits:** When a split divides a full page (100% full) into two ~50% full pages, the effective space utilization drops. Over time, with many splits, index pages average 50-70% full — wasting disk space and increasing tree height slightly.

### 3.5 Write Amplification in B-Trees

**Write amplification** = the ratio of bytes written to storage vs bytes sent by the application.

For a B-tree:
- An INSERT of one row modifies one leaf page (write 1 page = 8KB for 100-byte row → amplification = 80×)
- If the insert causes a split: writes 2-3 pages → amplification = 240×
- With WAL: each page modification is also written to the WAL → amplification doubles
- With fsync: all modified pages must be flushed to disk before commit

B-tree write amplification: typically 10-100× for random inserts. This is the fundamental performance characteristic that motivates LSM-trees (M48) for write-heavy workloads.

### 3.6 Postgres B-Tree Specifics

Postgres uses a **B+-tree variant** called nbtree (Lehman-Yao B-tree) with these additional properties:

**Rightmost-child optimization:** When inserting a monotonically increasing key (e.g., `id SERIAL`), Postgres detects this and avoids random page splits — inserts always go to the rightmost leaf, causing sequential writes.

**Deduplication (Postgres 13+):** Multiple index entries for the same key (from duplicate values) are stored once with a posting list of heap TIDs. Reduces index size for low-cardinality indexes.

**Page recycling:** Deleted index entries are marked as dead but the page is not immediately reclaimed. VACUUM physically removes dead entries and marks pages as available for reuse. Without VACUUM, index bloat accumulates.

---

## 4. Hands-On Walkthrough

### 4.1 Inspecting B-Tree Pages in Postgres

```sql
-- Install pageinspect extension (requires superuser)
CREATE EXTENSION pageinspect;

-- Inspect a B-tree index's metadata
SELECT * FROM bt_metap('users_pkey');
-- magic     | version | root | level | fastroot | fastlevel | oldest_xact | last_cleanup_num_heap_tuples
-- 340322    | 4       | 3    | 2     | 3        | 2         | 738         | 1000000
-- root=3 (page 3 is the root), level=2 (tree is 3 levels: root + 1 internal + leaves)

-- Inspect an internal page (page 3 = root in this example)
SELECT * FROM bt_page_items('users_pkey', 3) LIMIT 10;
-- itemoffset | ctid     | itemlen | nulls | vars | data
-- 1          | (1,0)    | 8       | f     | f    | (no key — high-key placeholder)
-- 2          | (2,0)    | 16      | f     | f    | 01 27 4b 00 00 00 ...  (key = 75585)
-- 3          | (5,0)    | 16      | f     | f    | 01 4e 96 00 ...         (key = 151169)
-- ctid references the child page containing keys ≥ this key

-- Inspect a leaf page
SELECT * FROM bt_page_items('users_pkey', 1) LIMIT 5;
-- itemoffset | ctid      | itemlen | nulls | vars | data
-- 1          | (0,0)     | 8       | f     | f    | (high-key: max key in this page)
-- 2          | (100,43)  | 16      | f     | f    | 01 00 00 00 ...   (key = 1, heap TID = page 100, offset 43)
-- 3          | (100,44)  | 16      | f     | f    | 02 00 00 00 ...   (key = 2)
-- ctid = (heap_page, offset) — points to the actual row in the heap

-- Check index statistics
SELECT
    relname,
    relpages,                   -- Number of 8KB pages
    pg_size_pretty(pg_relation_size(oid)) AS index_size,
    reltuples::bigint AS index_entries
FROM pg_class
WHERE relname = 'users_pkey';
-- relname   | relpages | index_size | index_entries
-- users_pkey| 2746     | 21 MB      | 1000000

-- Check fragmentation (average fill factor)
SELECT 100 * reltuples / (relpages::float * 200) AS avg_fill_pct
FROM pg_class WHERE relname = 'users_pkey';
-- avg_fill_pct ≈ 72  (72% of each page is used — good)
-- If < 50%: significant fragmentation → consider REINDEX
```

### 4.2 Observing Page Splits

```sql
-- Create a table and watch page count grow as we insert
CREATE TABLE split_test (id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX ON split_test(val);

-- Check initial page count
SELECT relpages FROM pg_class WHERE relname = 'split_test_val_idx';
-- 0 or 1 pages initially

-- Insert random values (triggers many splits due to non-sequential keys)
INSERT INTO split_test
SELECT i, md5(random()::text)
FROM generate_series(1, 100000) i;

ANALYZE split_test;

-- Check page count after random inserts
SELECT relpages, pg_size_pretty(pg_relation_size('split_test_val_idx')) AS size
FROM pg_class WHERE relname = 'split_test_val_idx';
-- relpages ≈ 550 pages (about 4.3MB for 100K rows of 32-char md5 keys)

-- Compare: sequential inserts (much fewer splits, better fill factor)
CREATE TABLE split_test_seq (id SERIAL PRIMARY KEY, val TEXT);
INSERT INTO split_test_seq(val) SELECT md5(i::text) FROM generate_series(1, 100000) i;
ANALYZE split_test_seq;
SELECT relpages FROM pg_class WHERE relname = 'split_test_seq_pkey';
-- relpages ≈ 274 (half the pages! Sequential inserts = 100% fill on each new page)
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
btree_engine.py

A complete B+-tree storage engine implementation in Python.

Components:
  - BTreePage: Fixed-size page with header, item directory, and items
  - BTreeNode: Abstract node (internal vs leaf)
  - LeafNode: Stores (key, value) pairs, linked to siblings
  - InternalNode: Stores (key, child_pointer) routing entries
  - BTreeIndex: Full B+-tree with insert, lookup, range scan
  - BTreeStats: Tracks write amplification, split count, page count
  - PageSimulator: Simulates disk I/O for visualizing access patterns

Design choices that match production B+-trees:
  - Fixed page size (configurable)
  - High branching factor (keys per node bounded by PAGE_SIZE)
  - Leaf-level linked list for range scans
  - Root split handled as a special case (tree grows taller by 1)

No external dependencies.
"""

from __future__ import annotations
import bisect
import sys
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional


# ─── Page/Node Constants ─────────────────────────────────────────────────────────

PAGE_SIZE_BYTES = 4096       # 4KB pages (Postgres default is 8192)
KEY_SIZE_BYTES  = 8          # Simulated 8-byte integer key
PTR_SIZE_BYTES  = 8          # Simulated 8-byte pointer
VAL_SIZE_BYTES  = 64         # Simulated 64-byte value (e.g., heap TID + metadata)
HEADER_BYTES    = 24         # Page header

# How many (key, ptr) pairs fit in an internal node
INTERNAL_ORDER = (PAGE_SIZE_BYTES - HEADER_BYTES) // (KEY_SIZE_BYTES + PTR_SIZE_BYTES)
# How many (key, value) pairs fit in a leaf node
LEAF_ORDER = (PAGE_SIZE_BYTES - HEADER_BYTES - 16) // (KEY_SIZE_BYTES + VAL_SIZE_BYTES)
# Minimum fill for leaf nodes (split when full, merge when below this)
MIN_LEAF_FILL = LEAF_ORDER // 2

print(f"B-tree configuration: internal_order={INTERNAL_ORDER}, leaf_order={LEAF_ORDER}")


# ─── Page Header ────────────────────────────────────────────────────────────────

@dataclass
class PageHeader:
    """Simulates a B-tree page header."""
    page_id:      int
    is_leaf:      bool
    level:        int           # 0 = leaf, 1 = first internal level, etc.
    prev_sibling: Optional[int] = None  # Leaf left sibling page_id
    next_sibling: Optional[int] = None  # Leaf right sibling page_id
    num_items:    int = 0
    lsn:          int = 0       # WAL position of last modification


# ─── B+-Tree Nodes ──────────────────────────────────────────────────────────────

class LeafNode:
    """
    B+-tree leaf node. Stores (key, value) pairs sorted by key.
    Linked to left and right siblings for range scan traversal.
    """

    def __init__(self, page_id: int, level: int = 0):
        self.page_id = page_id
        self.level   = level
        self.is_leaf = True
        self.keys:   list[Any] = []
        self.values: list[Any] = []
        self.prev: Optional['LeafNode'] = None
        self.next: Optional['LeafNode'] = None

    @property
    def is_full(self) -> bool:
        return len(self.keys) >= LEAF_ORDER

    @property
    def is_underfull(self) -> bool:
        return len(self.keys) < MIN_LEAF_FILL

    @property
    def size_bytes(self) -> int:
        return HEADER_BYTES + len(self.keys) * (KEY_SIZE_BYTES + VAL_SIZE_BYTES)

    def find(self, key: Any) -> Optional[Any]:
        """Binary search for key. Returns value or None."""
        idx = bisect.bisect_left(self.keys, key)
        if idx < len(self.keys) and self.keys[idx] == key:
            return self.values[idx]
        return None

    def insert(self, key: Any, value: Any):
        """Insert (key, value) in sorted order."""
        idx = bisect.bisect_left(self.keys, key)
        if idx < len(self.keys) and self.keys[idx] == key:
            self.values[idx] = value   # Update existing
        else:
            self.keys.insert(idx, key)
            self.values.insert(idx, value)

    def delete(self, key: Any) -> bool:
        """Delete key. Returns True if found."""
        idx = bisect.bisect_left(self.keys, key)
        if idx < len(self.keys) and self.keys[idx] == key:
            self.keys.pop(idx)
            self.values.pop(idx)
            return True
        return False

    def split(self, new_page_id: int) -> tuple['LeafNode', Any]:
        """
        Split this leaf into two halves.
        Returns (new_right_sibling, split_key).
        split_key is the first key in the new right sibling
        (promoted to the parent as the routing key).
        """
        mid = len(self.keys) // 2
        new_leaf = LeafNode(new_page_id, self.level)
        new_leaf.keys   = self.keys[mid:]
        new_leaf.values = self.values[mid:]
        self.keys   = self.keys[:mid]
        self.values = self.values[:mid]

        # Maintain the linked list
        new_leaf.next = self.next
        new_leaf.prev = self
        if self.next:
            self.next.prev = new_leaf
        self.next = new_leaf

        split_key = new_leaf.keys[0]
        return new_leaf, split_key

    def range_scan(self, low: Any, high: Any) -> Iterator[tuple[Any, Any]]:
        """Scan this leaf and successors for keys in [low, high]."""
        node = self
        while node is not None:
            for k, v in zip(node.keys, node.values):
                if k > high:
                    return
                if k >= low:
                    yield k, v
            node = node.next


class InternalNode:
    """
    B+-tree internal node. Stores routing keys and child pointers.
    children[i] is the subtree for keys < keys[i].
    children[-1] is the rightmost subtree (keys >= all routing keys).
    """

    def __init__(self, page_id: int, level: int):
        self.page_id  = page_id
        self.level    = level
        self.is_leaf  = False
        self.keys:     list[Any] = []
        self.children: list[Any] = []    # LeafNode or InternalNode references

    @property
    def is_full(self) -> bool:
        return len(self.keys) >= INTERNAL_ORDER - 1

    @property
    def size_bytes(self) -> int:
        return HEADER_BYTES + len(self.keys) * KEY_SIZE_BYTES + len(self.children) * PTR_SIZE_BYTES

    def child_for_key(self, key: Any) -> Any:
        """Return the child that should contain the given key."""
        idx = bisect.bisect_right(self.keys, key)
        return self.children[idx]

    def insert_child(self, split_key: Any, new_child: Any):
        """
        Insert a new child after a split.
        split_key is the first key in new_child (promoted from leaf split).
        """
        idx = bisect.bisect_left(self.keys, split_key)
        self.keys.insert(idx, split_key)
        self.children.insert(idx + 1, new_child)

    def split(self, new_page_id: int) -> tuple['InternalNode', Any]:
        """
        Split this internal node. Returns (new_right_sibling, promoted_key).
        The promoted_key goes up to the parent; it's removed from both siblings.
        """
        mid = len(self.keys) // 2
        promoted_key = self.keys[mid]

        new_internal = InternalNode(new_page_id, self.level)
        new_internal.keys     = self.keys[mid + 1:]
        new_internal.children = self.children[mid + 1:]

        self.keys     = self.keys[:mid]
        self.children = self.children[:mid + 1]

        return new_internal, promoted_key


# ─── B+-Tree Index ────────────────────────────────────────────────────────────────

@dataclass
class BTreeStats:
    inserts:      int = 0
    deletes:      int = 0
    lookups:      int = 0
    range_scans:  int = 0
    leaf_splits:  int = 0
    internal_splits: int = 0
    pages_written: int = 0   # Total page writes (for write amplification)
    rows_inserted: int = 0   # Rows inserted (denominator for amplification)

    @property
    def write_amplification(self) -> float:
        if self.rows_inserted == 0:
            return 0.0
        return self.pages_written / self.rows_inserted

    @property
    def total_splits(self) -> int:
        return self.leaf_splits + self.internal_splits


class BTreeIndex:
    """
    A complete B+-tree index implementation.

    Supports: lookup, insert, delete, range scan.
    Tracks: page allocations, splits, write amplification.
    """

    def __init__(self):
        self._next_page_id = 0
        self.root: Any = None
        self.stats = BTreeStats()
        self._init_root()

    def _alloc_page(self) -> int:
        pid = self._next_page_id
        self._next_page_id += 1
        return pid

    def _init_root(self):
        self.root = LeafNode(self._alloc_page(), level=0)
        self.stats.pages_written += 1

    def _page_count(self) -> int:
        return self._next_page_id

    # ── Lookup ──────────────────────────────────────────────────────────────────

    def lookup(self, key: Any) -> Optional[Any]:
        """Find the value for a key. Returns None if not found."""
        self.stats.lookups += 1
        leaf = self._find_leaf(key)
        return leaf.find(key)

    def _find_leaf(self, key: Any) -> LeafNode:
        """Traverse from root to the leaf that should contain key."""
        node = self.root
        while not node.is_leaf:
            node = node.child_for_key(key)
        return node

    # ── Range Scan ───────────────────────────────────────────────────────────────

    def range_scan(self, low: Any, high: Any) -> Iterator[tuple[Any, Any]]:
        """Return all (key, value) pairs with low <= key <= high."""
        self.stats.range_scans += 1
        leaf = self._find_leaf(low)
        yield from leaf.range_scan(low, high)

    # ── Insert ──────────────────────────────────────────────────────────────────

    def insert(self, key: Any, value: Any):
        """Insert (key, value). Splits nodes as necessary."""
        self.stats.inserts += 1
        self.stats.rows_inserted += 1

        # Walk the path from root to leaf, keeping track for backtracking
        path = self._path_to_leaf(key)

        leaf = path[-1]
        leaf.insert(key, value)
        self.stats.pages_written += 1

        # If leaf is full, split and propagate up the path
        if leaf.is_full:
            self._split_and_propagate(path)

    def _path_to_leaf(self, key: Any) -> list:
        """Return path from root to the leaf that should contain key."""
        path = [self.root]
        node = self.root
        while not node.is_leaf:
            child = node.child_for_key(key)
            path.append(child)
            node = child
        return path

    def _split_and_propagate(self, path: list):
        """Handle splits from leaf up to root."""
        node = path.pop()   # The node that overflowed

        if node.is_leaf:
            new_node, split_key = node.split(self._alloc_page())
            self.stats.leaf_splits += 1
            self.stats.pages_written += 2   # Old leaf + new sibling
        else:
            new_node, split_key = node.split(self._alloc_page())
            self.stats.internal_splits += 1
            self.stats.pages_written += 2

        if not path:
            # Node was the root — create a new root
            new_root = InternalNode(self._alloc_page(), level=node.level + 1)
            new_root.children = [node, new_node]
            new_root.keys = [split_key]
            self.root = new_root
            self.stats.pages_written += 1
            return

        parent = path[-1]
        parent.insert_child(split_key, new_node)
        self.stats.pages_written += 1

        # Recursively split parent if needed
        if parent.is_full:
            self._split_and_propagate(path)

    # ── Delete ──────────────────────────────────────────────────────────────────

    def delete(self, key: Any) -> bool:
        """Delete a key. Returns True if found. Does not rebalance (like Postgres)."""
        self.stats.deletes += 1
        leaf = self._find_leaf(key)
        deleted = leaf.delete(key)
        if deleted:
            self.stats.pages_written += 1
        return deleted

    # ── Statistics ───────────────────────────────────────────────────────────────

    def height(self) -> int:
        """Return the tree height (number of levels)."""
        node = self.root
        h = 0
        while not node.is_leaf:
            node = node.children[0]
            h += 1
        return h

    def count_pages(self) -> dict:
        """Count pages by type (BFS traversal)."""
        leaves = 0
        internals = 0
        queue = [self.root]
        while queue:
            node = queue.pop(0)
            if node.is_leaf:
                leaves += 1
            else:
                internals += 1
                queue.extend(node.children)
        return {'leaves': leaves, 'internal': internals, 'total': leaves + internals}

    def print_stats(self):
        pages = self.count_pages()
        print(f"\n  B+-Tree Statistics:")
        print(f"    Rows inserted:    {self.stats.rows_inserted:,}")
        print(f"    Tree height:      {self.height() + 1} levels")
        print(f"    Total pages:      {pages['total']} "
              f"(leaf={pages['leaves']}, internal={pages['internal']})")
        print(f"    Page size:        {PAGE_SIZE_BYTES:,} bytes")
        print(f"    Leaf capacity:    {LEAF_ORDER} entries/page")
        print(f"    Internal capacity:{INTERNAL_ORDER} entries/page")
        print(f"    Leaf splits:      {self.stats.leaf_splits:,}")
        print(f"    Internal splits:  {self.stats.internal_splits:,}")
        print(f"    Pages written:    {self.stats.pages_written:,}")
        print(f"    Write amplif.:    {self.stats.write_amplification:.1f}× "
              f"(pages written per row inserted)")


# ─── Write Amplification Analyzer ────────────────────────────────────────────────

def analyze_write_patterns():
    """
    Compare write amplification for:
    1. Sequential inserts (monotonically increasing key, like SERIAL id)
    2. Random inserts (UUID-like random keys)
    """
    N = 50_000

    # Sequential inserts
    tree_seq = BTreeIndex()
    for i in range(N):
        tree_seq.insert(i, f"value-{i}")

    # Random inserts
    import random
    rng = random.Random(42)
    tree_rand = BTreeIndex()
    keys = list(range(N))
    rng.shuffle(keys)
    for k in keys:
        tree_rand.insert(k, f"value-{k}")

    print("=" * 60)
    print(f"Sequential inserts ({N:,} rows):")
    tree_seq.print_stats()
    print(f"\nRandom inserts ({N:,} rows):")
    tree_rand.print_stats()
    print(f"\nKey insight: sequential inserts cause fewer splits and lower")
    print(f"write amplification because each new page is filled to capacity")
    print(f"before splitting, while random inserts trigger splits on half-full pages.")


# ─── Demo ─────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"=== B+-Tree Storage Engine Demo ===\n")

    # ── 1. Basic Operations ──────────────────────────────────────────────────────
    print("─── Phase 1: Basic Operations ───")
    tree = BTreeIndex()

    # Insert 1000 entries
    for i in range(1000):
        tree.insert(i * 3, f"row-{i}")   # Keys: 0, 3, 6, ..., 2997

    print(f"  Inserted 1000 rows. Height: {tree.height() + 1} levels")

    # Lookup
    val = tree.lookup(300)
    print(f"  Lookup key=300: {val}")
    val = tree.lookup(301)
    print(f"  Lookup key=301 (not inserted): {val}")

    # Range scan
    print(f"  Range scan [150, 165]:")
    for k, v in tree.range_scan(150, 165):
        print(f"    key={k} value={v}")

    # ── 2. Page Split Observation ───────────────────────────────────────────────
    print("\n─── Phase 2: Page Split Observation ───")
    tree2 = BTreeIndex()
    prev_splits = 0
    for i in range(200):
        tree2.insert(i, f"v{i}")
        if tree2.stats.total_splits > prev_splits:
            prev_splits = tree2.stats.total_splits
            print(f"  Split #{prev_splits} after inserting key={i} "
                  f"(tree height={tree2.height() + 1}, pages={tree2.count_pages()['total']})")
            if prev_splits >= 5:
                print("  ... (subsequent splits omitted)")
                break

    # ── 3. Write Amplification Analysis ─────────────────────────────────────────
    print("\n─── Phase 3: Write Amplification Analysis ───")
    analyze_write_patterns()

    # ── 4. Range Scan Efficiency ─────────────────────────────────────────────────
    print("\n─── Phase 4: Range Scan Efficiency ───")
    tree3 = BTreeIndex()
    for i in range(10000):
        tree3.insert(i, f"data-{i}")

    # Count results in range — linked leaf list means no tree traversal
    results = list(tree3.range_scan(5000, 5099))
    print(f"  Range scan [5000, 5099]: {len(results)} results")
    print(f"  First: key={results[0][0]}, Last: key={results[-1][0]}")
    print(f"  (Range scan traverses the leaf linked list — no random I/O beyond first leaf)")
```

---

## 6. Visual Reference

### B+-Tree Structure

```
                      ROOT (Internal, level=2)
                      [key=50, key=100]
                     /         |          \
          Internal            Internal            Internal
          [25, 37]            [62, 75]            [112, 150]
          /  |  \             /  |  \             /   |   \

    Leaf  Leaf  Leaf     Leaf  Leaf  Leaf     Leaf  Leaf  Leaf
    [1-24][25-36][37-49] [50-61][62-74][75-99] [100-111][112-149][150+]
       ↔     ↔     ↔       ↔      ↔     ↔        ↔       ↔       ↔
       (leaf nodes linked as a doubly-linked list for range scans)

Key count × branching factor:
  3 leaves × LEAF_ORDER keys each
  Range scan: follow left sibling → no tree re-traversal
  Lookup: ROOT → Internal (1 I/O) → Internal (1 I/O) → Leaf (1 I/O) → found
  Height-3 tree: 3 random I/Os for any point lookup
```

### Page Split Walkthrough

```
BEFORE: Leaf page FULL (LEAF_ORDER=4 in this example)
  Leaf A: [10][20][30][40]   (prev=Leaf0, next=Leaf C)

INSERT key=25:
  1. Find leaf A (key 25 belongs between 20 and 30)
  2. Try to insert: FULL → SPLIT
  3. Allocate new page: Leaf B
  4. Move half to Leaf B: A=[10][20], B=[25][30][40]
  5. Update linked list: A.next=B, B.prev=A, B.next=LeafC, LeafC.prev=B
  6. Promote split key (25) to parent with pointer to B:
     Parent: [...][key=25→LeafB][...]

AFTER:
  Leaf A: [10][20]   (next=Leaf B)
  Leaf B: [25][30][40]  (prev=Leaf A, next=Leaf C)

Pages written: A (updated), B (new), Parent (updated) = 3 page writes for 1 row

If parent also FULL:
  Parent splits → promotes to grandparent → ... → root splits → new root created
  Tree height increases by 1
```

### Write Amplification Components

```
Single INSERT of 1 row (100 bytes):

In-memory:
  Modify B-tree leaf page (8KB) in buffer pool: negligible CPU cost

On disk (worst case — no batching, cold buffer pool):
  1. Read leaf page from disk:          8KB read   (to find insert position)
  2. Modify leaf page in memory:        -
  3. Write WAL record:                  ~200 bytes write (append)
  4. fsync WAL:                         I/O cost
  5. Write modified leaf page to disk:  8KB write
  6. fsync leaf page:                   I/O cost (or double-write buffer first)

Pages written: 1 (minimum) to 3 (if split occurred)
Write amplification: 8KB page / 100 bytes row = 80× minimum

Why this matters:
  A Kafka producer can write 100MB/s of sequential log data
  The same I/O bandwidth supports 100MB / (80 × 8KB) = ~156 rows/second
  for a B-tree with synchronous writes to a single disk

  Solution: batch commits (group commit), async writes, SSDs for random I/O
```

---

## 7. Common Mistakes

**Mistake 1: Creating too many indexes on a write-heavy table.** Each INSERT into a table must update all indexes on that table. A table with 6 indexes means 6 B-tree modifications per row insert — each potentially causing splits, each requiring WAL records. For a table receiving 10,000 inserts/second, 6 indexes can reduce throughput to 1,000 inserts/second. Evaluate each index: is the read benefit worth the write cost? OLTP tables under heavy insert load should have the minimum necessary indexes.

**Mistake 2: Not running VACUUM or assuming it runs automatically and is sufficient.** Postgres marks deleted rows and updated rows as dead (old version) in the heap, and marks old index entries as dead in the index pages. These dead entries waste space and slow index scans. VACUUM reclaims them. Autovacuum runs automatically but may not keep up with high-write tables — especially tables that receive many UPDATE or DELETE operations. Monitor `pg_stat_user_tables.n_dead_tup` and tune autovacuum parameters (`autovacuum_vacuum_scale_factor`, `autovacuum_vacuum_cost_delay`) for heavy-write tables.

**Mistake 3: Treating index size as proportional to row count without considering key type.** An index on a UUID column (36 bytes per key) will be dramatically larger than an index on a 4-byte integer with the same row count. Larger keys mean fewer keys fit per page → higher tree height → more I/Os per lookup → worse range scan throughput. For high-write tables, prefer integer or bigint primary keys over UUIDs when the application allows it.

---

## 8. Production Failure Scenarios

### Scenario 1: Index Bloat from Bulk Delete Without VACUUM

**Symptoms:** After deleting 40% of rows from a 500M-row events table (a data retention cleanup), the table's primary key index size did not shrink — it remained at 28GB. Query performance on the index actually got worse, and `EXPLAIN ANALYZE` showed "index scans" returning many dead rows (heap fetches returning invisible rows).

**Root cause:** Postgres's MVCC model marks deleted rows as dead but does not immediately reclaim their space. The index entries for deleted rows remain in the B-tree leaf pages, marked as dead. Without VACUUM, these dead entries accumulate — pages remain the same size but have fewer live entries (lower effective fill factor). Index scans waste time visiting dead index entries and then checking the heap to confirm each is dead.

**Fix:**
```sql
-- After bulk delete: run VACUUM ANALYZE to reclaim dead rows
VACUUM ANALYZE events;

-- For aggressive reclaim (requires exclusive lock — downtime):
VACUUM FULL events;   -- Rewrites entire table + all indexes

-- Better: REINDEX CONCURRENTLY (Postgres 12+) just for the index
-- Does not reclaim heap space but fixes index bloat without locking
REINDEX INDEX CONCURRENTLY events_pkey;

-- Monitor dead tuples
SELECT relname, n_dead_tup, n_live_tup,
       round(n_dead_tup::numeric / nullif(n_live_tup + n_dead_tup, 0) * 100, 2) AS dead_pct
FROM pg_stat_user_tables
WHERE relname = 'events'
ORDER BY n_dead_tup DESC;
```

### Scenario 2: B-Tree Fill Factor Causing Index Bloat on Monotonically Increasing Key

**Symptoms:** A `orders` table with `id BIGSERIAL PRIMARY KEY` receives ~500 inserts/second. Monthly `REINDEX` is needed because the index grows to 3× expected size. The DBA notices that `VACUUM ANALYZE` does not reduce the index size.

**Root cause:** The primary key index has no dead tuples (orders are never deleted). The fragmentation comes from the fill factor. When Postgres inserts with a monotonically increasing key, it fills each leaf page to near 100% capacity. This is actually correct behavior — `fillfactor=100` for the index is appropriate here. The bloat comes from the fact that `UPDATE orders SET status='complete'` creates new heap tuples and new index entries for non-indexed columns (MVCC), but the primary key index entries are never updated — the primary key doesn't change. The growth is real data growth, not bloat.

**Diagnosis mistake:** The DBA was comparing to "expected size" based on row count without accounting for actual index entry size (8 bytes for the bigint key + 6 bytes for heap TID = 14 bytes per entry; with overhead, ~20 bytes). At 500 inserts/second × 30 days = 1.3 billion rows → 1.3B × 20 bytes = 26GB. The index size was correct.

**Real bloat scenario (and fix):** B-tree bloat is most common on indexes with `fillfactor=100` (default) receiving random-order inserts (e.g., UUID primary keys, text fields). Random inserts cause splits that produce 50% full pages — the effective fill drops to ~50%. Use `fillfactor=70` for such indexes:
```sql
CREATE INDEX ON events(uuid_key) WITH (fillfactor=70);
-- Pages are only filled to 70% before splitting
-- New inserts fit in existing pages without causing splits (up to 100%)
-- Better for write-heavy + random key patterns
```

---

## 9. Performance and Tuning

### B-Tree Access Costs

```
B-tree lookup cost model:
  tree_height = ceil(log(INTERNAL_ORDER, N / LEAF_ORDER))
  = ceil(log_128(1_000_000_rows / 150 rows_per_leaf))
  = ceil(log_128(6667))
  ≈ ceil(2.4) = 3

  Random I/O per level: SSD P99 = 100μs, HDD = 10ms
  SSD lookup latency: 3 × 100μs = 300μs  (0.3ms per lookup)
  HDD lookup latency: 3 × 10ms  = 30ms   (catastrophic for OLTP)

  Implication: B-trees with HDDs are only viable for bulk operations.
  Production OLTP databases require SSDs or NVMe for acceptable lookup latency.

B-tree range scan cost:
  leaf_pages_to_scan = ceil(range_size / LEAF_ORDER)
  = ceil(10000 rows / 150 rows_per_leaf) = 67 pages
  = 67 sequential I/Os (sequential — much faster than random)
  SSD: 67 × 50μs = 3.35ms  (sequential is 2× random on SSDs)
  HDD: 67 × 0.5ms = 33ms   (sequential is 20× random on HDDs)
```

### Postgres B-Tree Tuning

```sql
-- ── fillfactor: reserve space for updates ────────────────────────────────────
-- Default fillfactor=90 for indexes
-- Lower = less dense pages = more pages = more disk space
--       = UPDATES don't need page splits (HOT update optimization)
-- For append-only tables (no updates/deletes): fillfactor=100
-- For heavy-update tables: fillfactor=70-80
ALTER INDEX users_pkey SET (fillfactor = 90);

-- ── REINDEX for bloated indexes ──────────────────────────────────────────────
-- Non-locking (Postgres 12+):
REINDEX INDEX CONCURRENTLY idx_orders_created_at;

-- Check if REINDEX is needed:
SELECT
    schemaname, tablename, indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;

-- ── parallel index build (Postgres 11+) ─────────────────────────────────────
SET max_parallel_maintenance_workers = 4;   -- Use 4 workers for index build
CREATE INDEX ON large_table(col1, col2);    -- Uses parallel workers

-- ── autovacuum tuning for B-tree maintenance ─────────────────────────────────
ALTER TABLE high_write_table SET (
    autovacuum_vacuum_scale_factor = 0.01,   -- Trigger at 1% dead tuples (not default 20%)
    autovacuum_vacuum_cost_delay = 2         -- Less aggressive throttling (default 20ms)
);
```

---

## 10. Interview Q&A

**Q1: Why do databases use B-trees instead of binary search trees?**

The answer comes from the hardware constraint: disk and SSD I/O operates at page granularity. Accessing one byte of data from disk requires reading the entire page (typically 4-16KB). A binary search tree has two children per node — each node is ~40-100 bytes. Traversing a 3-level BST requires 3 disk reads, accessing 3 nodes of 100 bytes each (300 bytes useful) while reading 3 × 4KB = 12KB of disk data. Utilization: 300/12,288 = 2.4%.

A B+-tree node is sized to fill exactly one disk page. An 8KB page holds ~100-500 key-pointer pairs per internal node. A 3-level B+-tree with branching factor 200 can index 200^3 = 8 million leaf nodes × 150 rows each = 1.2 billion rows — with at most 3 disk reads per lookup, each reading a full page (100% utilization of each I/O). The B-tree's high branching factor is not a data structure elegance — it is a direct response to the page-granular I/O of storage hardware.

The secondary benefit is cache efficiency: a single B-tree page with 200 entries fits in one CPU cache line group, and the entire root-to-leaf path (24KB for a 3-level tree) fits in L3 cache for hot tables. BST traversal would require 3 separate cache line loads at random addresses.

**Q2: What is write amplification in a B-tree context, and when does it become a problem?**

Write amplification in a B-tree is the ratio of bytes written to storage per byte of application data. For a B-tree on 8KB pages, inserting one 100-byte row requires writing at minimum one 8KB page — an amplification of 80×. If the insert causes a page split, two pages are written (160×). With WAL overhead (the change is also written sequentially to the write-ahead log before the page is modified), amplification roughly doubles again. In practice: 20-200× for random inserts on a B-tree with WAL.

Write amplification becomes a problem under two conditions. First, write-heavy OLTP: if you're receiving 100,000 random inserts per second and each requires writing two 8KB pages plus WAL, your I/O budget is 100,000 × (2 × 8KB + ~500 bytes WAL) ≈ 1.7GB/second of writes. Many cloud disks provide 500MB/s write throughput — the I/O becomes the bottleneck. Second, SSD wear: SSDs have limited write endurance (TBW — terabytes written). High write amplification consumes TBW faster, reducing SSD lifespan. Both problems motivate LSM-trees, which trade write amplification for sequential writes and much lower per-insert I/O cost.

---

## 11. Cross-Question Chain

**Interviewer:** A Postgres table with 10 million rows has an index on a timestamp column. Range queries like "last 7 days" are fast. But a point lookup on the timestamp ("exact timestamp = X") uses a sequential scan instead of the index. Why?

**Candidate:** The query planner chose a sequential scan because the index's selectivity for that specific timestamp is very low from the planner's perspective, or — more likely — because timestamp columns often have many rows per distinct value. If the column has low cardinality relative to the range being queried (say, 1 million rows share the same timestamp value to the second precision), the planner estimates that the index lookup would return 1 million rows, requiring 1 million heap fetches. A sequential scan of the entire heap might be cheaper.

I'd start by checking the column's data distribution: `SELECT count(*) FROM orders WHERE created_at = 'specific-timestamp'`. If this returns many rows, the planner's decision is correct — the index is useful for ranges, not point lookups on non-unique values. For truly unique timestamps, a `UNIQUE` constraint changes the selectivity estimate and the planner will use the index.

**Interviewer:** You add a partial index on the timestamp column with a WHERE clause for the last 30 days. Does that help for point lookups on old timestamps?

**Candidate:** No. A partial index `CREATE INDEX ON orders(created_at) WHERE created_at > now() - interval '30 days'` only indexes rows matching the WHERE predicate. A query for a timestamp 60 days ago won't use this partial index — the row isn't in the index. The query must either use the full index on `created_at` (if it exists) or do a sequential scan.

Partial indexes are valuable for queries that always include the same filter condition: if 90% of queries are "last 30 days," the partial index covers the common case while being 90% smaller than the full index — fitting better in the buffer pool, faster to scan.

**Interviewer:** The table receives 500 inserts per second and 50 updates per second on the timestamp column. Explain the B-tree write cost for the update.

**Candidate:** An UPDATE on an indexed column in Postgres is the most expensive B-tree operation. Because of MVCC, an UPDATE doesn't modify a row in place — it creates a new heap tuple (the updated version) and marks the old tuple as dead. The index must be updated to reflect the new heap TID for the new tuple. So a single UPDATE on `created_at` causes: (1) a new heap tuple write, (2) a new index entry insertion in the timestamp index (write to leaf page), (3) the old index entry is marked dead (write to old leaf page), (4) WAL records for all three changes. Two leaf page writes plus WAL for one UPDATE.

If the old and new timestamps are in different parts of the index (likely with random timestamp values), those are two random writes to different leaf pages. At 50 updates/second: 100 random leaf page writes per second to the index, plus 100 random writes to the heap. At SSD random write latency of 100μs, 200 random writes/second = 20ms of I/O per second — well within SSD capability. But at 500 updates/second (10× the rate), this saturates a single SSD. The long-term answer is HOT (Heap Only Tuple) updates, which avoid index updates when the indexed column doesn't change — ensuring your application only updates `created_at` when genuinely changing it, not as part of a general status update.

**Interviewer:** What is a HOT update and when does it apply?

**Candidate:** HOT — Heap Only Tuple — is a Postgres optimization that skips index updates for certain UPDATE operations. It applies when: the updated columns are NOT indexed AND the new tuple fits on the same heap page as the old tuple (free space available). In a HOT update: only the heap page is modified, a forward pointer chains the old tuple to the new tuple, and no index entries are changed. Index scans follow the chain in the heap to find the latest version. This eliminates all B-tree writes for the update — the index cost is zero.

HOT applies surprisingly often for wide tables (many columns, only a few indexed) where most updates modify non-indexed columns (status, metadata, counters). Keeping fillfactor < 100 on the heap (the table itself, not the index) reserves space on each page for HOT updates to place the new tuple on the same page as the old one.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | Why do databases use B+-trees instead of binary search trees? | Page-granular I/O: reading one byte reads an entire page (8KB). B+-tree nodes fill a full page with hundreds of entries → high branching factor → low tree height → few I/Os per lookup. BST nodes are tiny → same I/O, far less useful data per read. |
| 2 | What is the branching factor of a B+-tree and how is it determined? | Number of children per internal node. Determined by: (page_size - header_bytes) / (key_size + pointer_size). For 8KB pages, 8-byte keys, 8-byte pointers: ≈ (8192-24) / 16 ≈ 512. More children per node = lower tree height. |
| 3 | What is write amplification in a B-tree? | Ratio of bytes written to storage per byte of application data. For a B-tree on 8KB pages, inserting one 100-byte row writes one 8KB page → 80× amplification. With splits and WAL: can reach 200-300×. |
| 4 | When does a B+-tree page split occur? | When a leaf (or internal) node is full (contains LEAF_ORDER entries). The full node is split in half, the middle key is promoted to the parent. If the parent is also full, splits cascade up to the root. Root splits increase tree height by 1. |
| 5 | What is the B+-tree leaf-level linked list and why does it exist? | Leaf nodes are linked as a doubly-linked list. Range scans find the first qualifying leaf (tree traversal), then follow the linked list — sequential I/O, no tree re-traversal per page. Without the linked list, each page in a range scan would require traversing from the root. |
| 6 | What is the tree height for a B+-tree with 100M rows, branching factor 128, and 150 rows per leaf? | height = ceil(log_128(100M / 150)) = ceil(log_128(667K)) ≈ 3. Three random I/Os for any point lookup regardless of table size. |
| 7 | What is index bloat in Postgres? | Accumulation of dead index entries from DELETE and UPDATE operations. Dead entries occupy space in B-tree leaf pages but serve no queries. Reduces effective fill factor, increases tree size. Fixed by VACUUM (marks pages available) or REINDEX (rewrites entirely). |
| 8 | What does Postgres pageinspect's bt_metap() show? | Metadata for a B-tree index: root page number, tree level (height), fast root (optimization for root lookups), oldest transaction ID. Used to diagnose tree structure. |
| 9 | What is a HOT update in Postgres? | Heap Only Tuple: an UPDATE that skips index modification. Applies when: (1) no indexed columns are changed, (2) the new tuple fits on the same heap page. Only the heap page is modified; index is unchanged. Eliminates all B-tree write cost for qualifying updates. |
| 10 | What is write amplification and how does it compare between sequential and random B-tree inserts? | Sequential inserts (monotonically increasing key): each page is filled to capacity before splitting → fewer splits, lower amplification. Random inserts: splits occur on half-full pages → more splits, pages average 50-70% full, higher amplification. Difference: 2-4× in typical workloads. |
| 11 | What is fillfactor in a Postgres B-tree index? | The percentage of each page filled before a new page is allocated. Default 90. Lower fillfactor reserves space for future inserts without splitting (useful for update-heavy patterns). Higher = more dense pages = smaller index = faster scans but more splits on insert. |
| 12 | What is REINDEX CONCURRENTLY and when should you use it? | Rebuilds an index without taking an exclusive lock on the table (reads and writes continue during rebuild). Use when an index is heavily bloated (from bulk deletes) or has incorrect statistics. Takes 2-3× longer than normal REINDEX but does not cause downtime. |
| 13 | How does Postgres store B-tree metadata for range scan sibling traversal? | Each leaf page's "special space" contains `left_sibling` and `right_sibling` page IDs. After reaching the first qualifying leaf via tree traversal, range scans follow `right_sibling` pointers without re-traversing the tree — sequential page access. |
| 14 | What is write amplification's relationship to SSD lifespan? | SSDs have a limited TBW (Terabytes Written) rating. High write amplification exhausts TBW faster. A 10TB SSD with 20,000 TBW and 200× write amplification can handle 100TB of application writes before wearing out. Reducing write amplification (batching, LSM-trees) extends SSD life. |
| 15 | What happens to a B+-tree when the root node splits? | A new root is created. The original root is split in half, and the new root contains one routing key (the promoted middle key) and two children (the two halves of the original root). Tree height increases by exactly 1. Root split is rare but O(1) — doesn't cascade further (the new root is never full immediately after creation). |

---

## 13. Further Reading

- **"Database Internals" by Alex Petrov, Chapters 2-4 (B-Tree):** The most rigorous treatment of B-tree implementations in a data engineering context. Chapter 4 on B-tree splits and deletes is excellent.
- **Postgres source: `src/backend/access/nbtree/`:** The actual Postgres B-tree implementation. `nbtinsert.c` contains the split logic; `nbtsearch.c` contains the traversal and range scan code.
- **Postgres documentation — "Indexes" chapter:** The official reference for Postgres index types, fillfactor, REINDEX, and VACUUM interaction with indexes.
- **"The Design and Implementation of Modern Column-Oriented Database Systems" (Abadi et al.):** Contrasts B-tree (row-oriented) with column-oriented storage — gives context for why B-trees dominate OLTP but are less suitable for analytics.
- **CMU Database Group lectures on B-Trees (YouTube):** Andy Pavlo's CMU 15-445 lecture on B-trees is one of the best visual explanations available. The slides are freely available.

---

## 14. Lab Exercises

**Exercise 1: Observe Tree Height Growth**
Run `btree_engine.py`. Modify the demo to insert 1000, 10000, and 100000 rows and print the tree height after each. Verify that height grows as log(N). Calculate the expected height from the formula and compare to actual.

**Exercise 2: Measure Split Frequency**
Insert 100,000 random keys (using `random.randrange(0, 1000000)`). Count total splits. Insert 100,000 sequential keys. Count total splits. Calculate write amplification for both. Explain why sequential inserts have fewer splits.

**Exercise 3: Range Scan Traversal**
Insert 10,000 entries. Perform a range scan for keys 4000-5000. Add a print statement in `range_scan()` every time it advances to the next leaf node (`node = node.next`). Count how many leaf pages were accessed. Compare to the theoretical count: `ceil(1000 / LEAF_ORDER)`.

**Exercise 4: Simulate the VACUUM Pattern**
Add a `mark_deleted(key)` method that sets a "deleted" flag on a leaf entry instead of removing it. Implement a `vacuum_leaf(leaf_node)` that removes all marked-deleted entries from a leaf and returns the space reclaimed. Observe the fill factor before and after vacuum.

---

## 15. Key Takeaways

The B+-tree is the dominant storage structure for read-optimized databases because it optimally matches the page-granular I/O model of disk and SSD. By sizing each node to exactly one page and using a high branching factor (128-512), B+-trees achieve O(log_b N) lookups in 3-4 I/Os for billion-row tables — orders of magnitude better than the same depth in a binary tree. Leaf-level linked lists make range scans sequential without re-traversing the tree.

The cost is write amplification: inserting one row modifies one or more 8KB pages, producing 80-200× amplification relative to the row size. This is acceptable for read-heavy OLTP (where the fast lookup performance justifies the write cost) but imposes real I/O limits at high write throughput. Understanding write amplification is the reason LSM-trees exist — the next module shows the design that sacrifices read performance to achieve dramatically lower write amplification.

B-tree maintenance operations — VACUUM, REINDEX, fillfactor tuning — are direct consequences of B-tree structure: dead entries from MVCC accumulate in leaf pages (requiring VACUUM), page splits produce partially-filled pages (requiring REINDEX for fragmentation), and the optimal fillfactor depends on the insert pattern (sequential vs random).

---

## 16. Connections to Other Modules

- **M48 — LSM-Tree Storage Engines:** LSM-trees are the alternative to B-trees for write-heavy workloads. Understanding B-tree write amplification (this module) is the direct motivation for understanding why LSM-trees use a different architecture.
- **M49 — Write-Ahead Log:** The WAL is what makes B-tree modifications crash-safe. Every page modification in a B-tree is first recorded in the WAL. Understanding the B-tree (which pages are modified, when splits occur) explains why WAL can be large and why checkpoint frequency matters.
- **M50 — Buffer Pool Management:** B-tree pages live in the buffer pool. Understanding the B-tree's page structure explains why buffer pool hit rate is critical for query performance (root and upper-level internal pages should be permanently cached) and why random workloads have poor buffer pool locality.
- **CSF-ALG-101 M03:** That module described B-trees at the algorithmic level (O(log N) operations). This module implements the physical reality: page layout, split mechanics, write amplification, and production-specific optimizations (HOT, fillfactor, VACUUM).

---

## 17. Anti-Patterns

**Anti-pattern: Using UUID primary keys without considering index performance.** UUID (36 characters or 16 bytes binary) primary keys generate random values, causing B-tree inserts to be distributed uniformly across all leaf pages. This produces maximum page splits and the worst possible write amplification compared to sequential (BIGSERIAL) keys. For high-insert-rate tables, prefer `BIGSERIAL` primary keys. If UUIDs are required for application reasons (distributed ID generation), use UUID v7 (time-ordered UUIDs) which approximate sequential order and dramatically reduce page splits.

**Anti-pattern: Running `REINDEX` instead of `VACUUM ANALYZE` as routine maintenance.** `REINDEX` rewrites the entire index from scratch — it requires a full table scan, significant I/O, and in the non-concurrent form, takes an exclusive lock. `VACUUM ANALYZE` is far less disruptive: it reclaims dead entries in-place, updates statistics, and is designed to run frequently without impact. `REINDEX` should be reserved for severe fragmentation or corruption, not routine maintenance.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pageinspect` (Postgres extension) | Inspect B-tree page internals | `bt_metap()`, `bt_page_items()` |
| `pg_stat_user_indexes` | Index usage and size statistics | `idx_scan`, `idx_tup_read` |
| `pg_stat_user_tables` | Table dead tuple count | `n_dead_tup` → VACUUM trigger |
| `REINDEX INDEX CONCURRENTLY` | Non-locking index rebuild | For bloated indexes |
| `EXPLAIN (ANALYZE, BUFFERS)` | Query plan with I/O statistics | `Buffers: shared hit=X read=Y` |
| `btree_engine.py` | B+-tree simulation | Insert, lookup, range scan, split tracking |
| `pgBadger` / `auto_explain` | Slow query logging | Identifies index-vs-seqscan choices |

---

## 19. Glossary

**B+-tree:** A variant of the B-tree where all data records are stored in leaf nodes; internal nodes contain only routing keys and child pointers. Leaf nodes are linked as a doubly-linked list for range scans. The dominant index structure in relational databases.

**Branching factor (b):** The number of children per internal node. Determines tree height: h = ceil(log_b(N)). A higher branching factor means a shorter (and faster) tree. Determined by page size and key/pointer sizes.

**Dead tuple:** In Postgres's MVCC model, an old version of a row that is no longer visible to any active transaction. Dead tuples in the heap and dead index entries in B-tree pages waste space and slow queries. Removed by VACUUM.

**fillfactor:** The percentage of a B-tree page's capacity used before a new page is allocated. Lower fillfactor reserves space for updates without splits (HOT updates). Default: 90 for indexes, 100 for heap pages.

**HOT update (Heap Only Tuple):** A Postgres optimization for UPDATEs on non-indexed columns. The new tuple is placed on the same heap page as the old tuple, and the old index entries are chained forward to the new tuple — no B-tree modification required.

**Page:** The fundamental unit of I/O in a storage engine. Fixed size (typically 4KB, 8KB, or 16KB). Every B-tree node is exactly one page. Every I/O reads or writes one page.

**Page split:** The operation that divides a full B-tree node into two nodes and promotes the middle key to the parent. Causes write amplification (2-3 page writes per split) and fragmentation (both new pages start ~50% full).

**REINDEX:** Rebuilds a Postgres index from scratch, fixing fragmentation and reclaiming all dead entry space. `REINDEX INDEX CONCURRENTLY` (Postgres 12+) does this without taking an exclusive lock.

**VACUUM:** Reclaims storage occupied by dead tuples in Postgres heap and index pages. Autovacuum runs automatically but may need tuning for high-write tables.

**Write amplification:** The ratio of bytes written to storage per byte of application data. B-trees: typically 20-200× due to page-granular writes and WAL overhead. LSM-trees have different (lower) write amplification for random inserts but higher for compaction.

---

## 20. Self-Assessment

1. A B+-tree has a page size of 8192 bytes, key size of 16 bytes (UUID), and pointer size of 8 bytes. What is the branching factor for internal nodes? How many levels does the tree need for 1 billion rows with 100 rows per leaf?
2. You insert 1000 random keys into a B+-tree with LEAF_ORDER=10 (10 keys per leaf). Approximately how many leaf splits do you expect? How many internal splits?
3. Why does a range scan in a B+-tree only require one tree traversal (from root to first qualifying leaf), but a range scan in a BST might require multiple root-to-leaf traversals?
4. Run `btree_engine.py` and explain the difference in split counts between sequential and random inserts. What does this imply for primary key design in write-heavy tables?
5. Explain HOT updates. Under what conditions does Postgres use them? What must the developer do to maximize HOT update eligibility?
6. A Postgres table with 100M rows has a B-tree index on a `created_at TIMESTAMPTZ` column. You want to delete all rows older than 1 year. After the deletion, what happens to the index? What should you run next, and why?
7. What is the difference between `VACUUM` and `VACUUM FULL`? When would you use each?
8. In the `btree_engine.py` implementation, `_split_and_propagate` is recursive. What is the base case that stops the recursion? What is the worst-case recursion depth?

---

## 21. Module Summary

The B+-tree is the foundational data structure of relational database storage engines — Postgres, MySQL, SQLite, and virtually every OLTP database uses it. Its design is not arbitrary: the high branching factor (128-512), page-aligned nodes, and leaf-level linked list are all direct responses to the page-granular I/O model of disk and SSD storage.

The implementation — `BTreeIndex` with `LeafNode`, `InternalNode`, page split and propagation, range scan, and write amplification tracking — makes every design property concrete: tree height grows as log_b(N) (logarithmically slow), splits produce partially-filled pages (fragmentation), and inserting one row writes multiple 8KB pages (amplification). The write amplification analysis demonstrates why sequential inserts are dramatically more efficient than random inserts — a direct implication for primary key design.

Production B-tree operations — VACUUM for dead entry reclaim, REINDEX for fragmentation, fillfactor tuning for update-heavy patterns, HOT updates for non-indexed column updates — are all direct consequences of B-tree structure that every data engineer who runs Postgres must understand.

The next module — M48: LSM-Tree Storage Engines — introduces the alternative architecture: Log-Structured Merge-Trees, which accept slower reads and more complex structure in exchange for dramatically lower write amplification and fully sequential I/O. The contrast between B-trees and LSM-trees is the central choice in storage engine design.
