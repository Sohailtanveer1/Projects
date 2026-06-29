# M54: Indexes

**Course:** DBE-INT-102 — Postgres Architecture  
**Module:** 03 of 05  
**Global Module ID:** M54  
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

A data engineer who knows only B-tree indexes is like a carpenter who knows only a hammer. For most nails, that is enough — B-tree covers equality, range, sort, and prefix queries. But data engineering tables contain arrays of event tags, JSON documents, full-text descriptions, geospatial coordinates, and time-series data that grows in monotonically increasing page order. B-tree handles none of these efficiently.

Postgres ships with five major index types: B-tree, Hash, GiST, GIN, and BRIN. Each was built for a different access pattern. Choosing the wrong index type means either the query ignores the index entirely (wrong type for the predicate operator) or the index exists but performs worse than a sequential scan. Choosing the right type means queries that would scan 100GB of data instead scan 10MB.

Beyond index types, there are index features that determine when indexes help and when they hurt: partial indexes (index only the rows you actually query), covering indexes (avoid heap fetches entirely), expression indexes (index computed column values), and index bloat (writes accumulate dead entries that slow scans and waste disk). Each of these features shifts the break-even point for whether an index helps a specific workload.

This module covers all five index types — when to use each, what operators each supports, and how each works internally — plus index features and the operational questions every data engineer must answer: how to create indexes without blocking writes, how to detect bloat, and how to identify unused indexes that waste write performance without helping any query.

---

## 2. Mental Model

### An Index Is a Trade: Read Speed for Write Overhead

Every index speeds up reads and slows writes. When you insert a row, Postgres must update every index on the table — writing one or more index pages per index per insert. The question is never "should I add an index?" but "does the read speedup justify the write overhead for this specific table's read/write ratio?"

For a read-heavy analytics table updated once daily, 20 indexes is defensible. For a high-throughput event ingestion table receiving 100,000 inserts/second, even 3 indexes may be too many.

### An Index Is a Sorted Secondary Structure

Every index is a secondary structure that maps key values to heap locations (TIDs: page number + offset within page). The index structure determines which queries it can accelerate:

- **B-tree:** sorted by key value → supports `=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `LIKE 'prefix%'`, `ORDER BY`, `GROUP BY`
- **Hash:** maps key to bucket by hash function → supports only `=`
- **GiST:** generalized search tree; pluggable — any operator that defines a "bounding box" abstraction → supports geometric overlap, nearest-neighbor, range types, text search prefix
- **GIN:** inverted index; maps element values to the set of rows containing them → supports `@>` (contains), `&&` (overlap), `@@` (full-text match), `= ANY(array)`
- **BRIN:** block range index; stores min/max per range of heap pages → supports `=`, `<`, `>` on naturally ordered columns (typically timestamps on append-only tables)

### Index-Only Scans: The Fastest Possible Index Access

When all columns referenced in a query (WHERE + SELECT) are in the index, Postgres can return results without reading the heap at all — an **index-only scan**. The index leaf pages contain all the data needed. This eliminates every random I/O that makes index scans expensive. Index-only scans are the primary motivation for covering indexes.

---

## 3. Core Concepts

### 3.1 B-Tree Index

A B-tree index (Postgres uses B+-tree: all data at leaf level, leaves linked for range scans) is the default index type and handles the overwhelming majority of use cases.

**Internal structure:**
- Root → internal pages (keys + child pointers) → leaf pages (keys + TIDs)
- Leaf pages are a doubly-linked list enabling range scans without traversing the tree
- Height: typically 3-4 levels for tables up to hundreds of millions of rows
- `branching_factor ≈ (8192 - 24) / (key_size + 8)`: approximately 128-512 for typical keys

**Supported operators:** `=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, `LIKE 'prefix%'`, `~ '^prefix'` (if using text_pattern_ops), array element equality with `= ANY()`

**Index-only scan eligibility:** Yes — leaf pages contain key + TID. Add non-indexed columns with `INCLUDE` to create covering indexes without affecting sort order.

**Multi-column indexes:** B-tree supports composite keys. The leading column(s) must be present in the query for the index to be usable. `CREATE INDEX ON orders(customer_id, status)` supports queries filtering on `customer_id` alone or `customer_id AND status`, but NOT `status` alone (the planner would need to scan the entire index in random order to find all status values).

```sql
-- Standard B-tree
CREATE INDEX idx_orders_customer ON orders(customer_id);

-- Composite (query must include customer_id to use this)
CREATE INDEX idx_orders_customer_status ON orders(customer_id, status);

-- Covering index: store amount without sorting by it
-- Enables index-only scan for: SELECT customer_id, status, amount WHERE customer_id = X
CREATE INDEX idx_orders_customer_covering ON orders(customer_id, status) INCLUDE (amount);

-- Descending order (for ORDER BY customer_id DESC queries)
CREATE INDEX idx_orders_customer_desc ON orders(customer_id DESC);

-- Partial index: only index rows we actually query
-- Reduces index size; queries MUST include the partial condition
CREATE INDEX idx_orders_pending ON orders(customer_id) WHERE status = 'pending';
-- Usable by: WHERE customer_id = 42 AND status = 'pending'
-- Not usable by: WHERE customer_id = 42 (no status filter)
```

### 3.2 Hash Index

A hash index stores `hash(key) → TID` in a hash table structure. Before Postgres 10, hash indexes were not WAL-logged and had to be rebuilt after a crash. Since Postgres 10, they are crash-safe.

**Only use case:** Equality predicates (`=`) where the key is large (e.g., very long strings) and B-tree overhead from full key comparison is measurable. In practice, B-tree is nearly always preferred because it also supports range queries and sorts. Hash indexes exist but are rarely the right choice.

```sql
CREATE INDEX idx_events_uuid ON events USING HASH (event_uuid);
-- Supports: WHERE event_uuid = 'abc123'
-- Does NOT support: WHERE event_uuid > 'abc123' (no ordering)
```

### 3.3 GiST Index (Generalized Search Tree)

GiST is an extensible index framework: it doesn't implement a specific data structure but provides a template that any data type can implement by defining bounding box and overlap operators. The result is an index that supports the overlap/containment/nearest-neighbor queries that B-tree cannot express.

**Built-in GiST support:**
- **Geometric types:** `point`, `box`, `circle`, `polygon` — operators `&&` (overlap), `@>` (contains), `<->` (distance/nearest-neighbor)
- **Range types:** `int4range`, `tsrange`, `daterange` — operators `&&`, `@>`, `<@`
- **Full-text search:** `tsvector` — with GiST for prefix matching and similarity
- **IP network types:** `inet`, `cidr` — operators `<<` (subnet containment)

**Not ideal for full-text equality/containment** — that is GIN's domain. GiST is better for geometric and range types where bounding boxes make sense.

```sql
-- Geospatial example: find all events within a bounding box
CREATE TABLE locations (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT,
    pos  POINT
);
CREATE INDEX idx_locations_pos ON locations USING GIST (pos);

-- Uses GiST index:
SELECT name FROM locations WHERE pos <@ box '((0,0),(100,100))';  -- within box
SELECT name, pos <-> point '(50,50)' AS dist
FROM locations ORDER BY pos <-> point '(50,50)' LIMIT 10;  -- nearest-neighbor

-- Range type example: find all subscriptions active during a given date range
CREATE TABLE subscriptions (
    id        BIGSERIAL PRIMARY KEY,
    user_id   INT,
    active_range daterange
);
CREATE INDEX idx_subscriptions_range ON subscriptions USING GIST (active_range);

-- Uses GiST index (range overlap):
SELECT * FROM subscriptions
WHERE active_range && daterange('2024-01-01', '2024-12-31');

-- Full-text prefix search (GiST for pg_trgm similarity)
CREATE EXTENSION pg_trgm;
CREATE INDEX idx_products_name_trgm ON products USING GIST (name gist_trgm_ops);
-- Supports LIKE '%partial%' and similarity() queries
SELECT * FROM products WHERE name % 'postgres';   -- similarity >= pg_trgm.similarity_threshold
SELECT * FROM products WHERE name LIKE '%partial%'; -- GiST with trigram ops
```

### 3.4 GIN Index (Generalized Inverted Index)

GIN is an inverted index: for each element value, it stores a sorted list of TIDs (rows containing that element). It is optimal for data types that are containers of elements: arrays, JSON documents, and `tsvector` (full-text search tokens).

**Core pattern:** "find all rows where the container includes element X" — the classic inverted index lookup.

**Built-in GIN support:**
- **Arrays:** `@>` (array contains), `<@` (array contained by), `&&` (array overlap), `= ANY(array)`
- **JSONB:** `@>` (JSON document contains), `?` (key exists), `?|` (any key exists), `?&` (all keys exist)
- **Full-text search:** `@@` (tsvector matches tsquery)
- **pg_trgm trigrams:** `LIKE '%substring%'` and `ILIKE` (when using gin_trgm_ops)

**GIN vs GiST for full-text search:**
- GIN: faster for lookups (`@@` queries); slower to build and update (larger, sorted posting lists)
- GiST: faster to build and update; slower for lookups; supports fuzzy prefix matching (GiST + pg_trgm)
- Rule: use GIN for full-text search on large tables where lookups dominate; use GiST when writes are frequent or the column is updated often

```sql
-- Array column: find posts with any of the given tags
CREATE TABLE posts (
    id   BIGSERIAL PRIMARY KEY,
    tags TEXT[]
);
CREATE INDEX idx_posts_tags ON posts USING GIN (tags);

-- Uses GIN index:
SELECT * FROM posts WHERE tags @> ARRAY['postgres', 'performance'];  -- contains all
SELECT * FROM posts WHERE tags && ARRAY['postgres', 'dbt'];           -- overlap

-- JSONB column: find documents containing a specific key/value
CREATE TABLE events (
    id      BIGSERIAL PRIMARY KEY,
    payload JSONB
);
CREATE INDEX idx_events_payload ON events USING GIN (payload);

-- Uses GIN index:
SELECT * FROM events WHERE payload @> '{"type": "click", "user_id": 42}';
SELECT * FROM events WHERE payload ? 'error_code';           -- key exists
SELECT * FROM events WHERE payload ?| ARRAY['error', 'warn']; -- any key exists

-- Full-text search:
CREATE TABLE articles (
    id      BIGSERIAL PRIMARY KEY,
    body    TEXT,
    body_ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
);
CREATE INDEX idx_articles_fts ON articles USING GIN (body_ts);

-- Uses GIN index:
SELECT id, ts_headline('english', body, q) AS snippet
FROM articles, plainto_tsquery('english', 'database performance') q
WHERE body_ts @@ q;

-- Substring matching with pg_trgm GIN:
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_products_name_gin ON products USING GIN (name gin_trgm_ops);

-- Uses GIN trigram index (not possible with standard B-tree):
SELECT * FROM products WHERE name ILIKE '%partial string%';
```

### 3.5 BRIN Index (Block Range INdex)

BRIN stores summary statistics (min, max, null count) for contiguous ranges of heap pages, called block ranges. A block range is `pages_per_range` pages (default 128 = 1MB per range for 8KB pages).

**Key insight:** BRIN is not a precise index. It stores min/max per block range. When the query filters `WHERE created_at > '2024-01-01'`, Postgres checks each block range's max — if the block range's max is before `'2024-01-01'`, skip the entire range (128 pages = 1MB). If the block range overlaps the predicate, scan all pages in that range.

**Prerequisite: physical correlation.** BRIN works only when the column's values are correlated with physical page order — i.e., the data was inserted in roughly sorted order on that column. A timestamp column on an append-only event table (events are always inserted with `NOW()`) has near-perfect correlation (correlation ≈ 0.99 in pg_stats). A `user_id` column with random UUIDs has no correlation and BRIN is useless.

**When to use BRIN:**
- Timestamp/date columns on append-only or time-ordered tables
- Auto-incrementing integer IDs (perfect correlation = 1.0)
- Any column where `pg_stats.correlation` is close to 1.0

**Size advantage:** A BRIN index on a 1-billion-row table with 128 pages per range = ~7.8M ranges × (8 bytes min + 8 bytes max) ≈ 125MB. A B-tree index on the same table might be 20-50GB. BRIN is orders of magnitude smaller.

```sql
-- BRIN on timestamp (most common use case)
CREATE INDEX idx_events_created_brin ON events USING BRIN (created_at);
-- Default pages_per_range = 128; for very large tables, try 16-32 for better precision
CREATE INDEX idx_events_created_brin ON events USING BRIN (created_at pages_per_range = 32);

-- Check correlation before creating BRIN:
SELECT correlation FROM pg_stats
WHERE tablename = 'events' AND attname = 'created_at';
-- If correlation < 0.8, BRIN will not help — use B-tree instead

-- Compare index sizes:
SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size
FROM pg_indexes
WHERE tablename = 'events';

-- BRIN for time-range partitioned data:
-- A 3-year events table (1B rows, 500GB) with a BRIN index:
-- B-tree on created_at: ~30GB index
-- BRIN on created_at (pages_per_range=128): ~5MB index
-- BRIN on created_at (pages_per_range=32): ~20MB index
-- Query WHERE created_at > NOW() - INTERVAL '7 days':
--   BRIN skips all block ranges entirely before 7 days ago
--   Only the last 7 days' worth of block ranges are scanned
```

### 3.6 Index Features

**Partial Indexes:** Index only the rows satisfying a WHERE condition. Smaller, faster to update, and can make the index-only viable for selective subsets.

```sql
-- Index only active users (status = 'active' is 5% of users)
CREATE INDEX idx_users_active ON users(last_login_at) WHERE status = 'active';
-- Size: 5% of a full index on last_login_at
-- Queries using this index MUST include: WHERE status = 'active'
```

**Expression Indexes:** Index the result of a function or expression. Required when queries involve functions on the column.

```sql
-- Index case-insensitive email
CREATE INDEX idx_users_email_lower ON users(LOWER(email));
-- Usable by: WHERE LOWER(email) = 'user@example.com'
-- NOT usable by: WHERE email = 'User@Example.com'

-- Index date part of a timestamp
CREATE INDEX idx_events_date ON events(DATE(created_at));
-- Usable by: WHERE DATE(created_at) = '2024-01-15'
```

**Covering Indexes (INCLUDE):** Add non-key columns to the index leaf pages so that index-only scans can return their values without fetching the heap. Added columns are not part of the sort key and can be any data type.

```sql
-- Query: SELECT id, amount FROM orders WHERE customer_id = 42 AND status = 'pending'
-- Without INCLUDE: index scan returns customer_id + status → fetch heap page for amount
-- With INCLUDE: index scan returns customer_id + status + amount → no heap fetch
CREATE INDEX idx_orders_cov ON orders(customer_id, status) INCLUDE (id, amount);
```

**CONCURRENTLY:** Create an index without locking writes. Takes longer (scans the table twice) but critical for production tables that cannot tolerate write downtime.

```sql
-- Blocks all writes during creation (DO NOT use on production tables):
CREATE INDEX idx_name ON table(column);

-- Allows writes during creation (recommended for production):
CREATE INDEX CONCURRENTLY idx_name ON table(column);

-- Drop index without blocking reads:
DROP INDEX CONCURRENTLY idx_name;

-- Rebuild bloated index without blocking reads (Postgres 12+):
REINDEX INDEX CONCURRENTLY idx_name;
```

---

## 4. Hands-On Walkthrough

### 4.1 Inspecting Indexes and Usage

```sql
-- List all indexes on a table with size and type
SELECT
    i.indexname,
    i.indexdef,
    pg_size_pretty(pg_relation_size(i.indexname::regclass)) AS index_size,
    ix.indisunique,
    ix.indisprimary
FROM pg_indexes i
JOIN pg_index ix ON ix.indexrelid = (i.schemaname || '.' || i.indexname)::regclass
WHERE i.tablename = 'orders'
ORDER BY pg_relation_size(i.indexname::regclass) DESC;

-- Find unused indexes (not scanned since last stats reset)
SELECT
    s.relname AS table_name,
    s.indexrelname AS index_name,
    s.idx_scan AS scans,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.idx_scan = 0
  AND NOT i.indisprimary
  AND NOT i.indisunique
ORDER BY pg_relation_size(s.indexrelid) DESC;

-- Find bloated indexes (estimated dead entry ratio)
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
    idx_blks_hit,
    idx_blks_read
FROM pg_statio_user_indexes
ORDER BY pg_relation_size(indexname::regclass) DESC;

-- Check column correlation for BRIN suitability
SELECT attname, correlation
FROM pg_stats
WHERE tablename = 'events'
ORDER BY abs(correlation) DESC;
-- correlation close to 1.0 or -1.0 → BRIN is effective
-- correlation close to 0 → BRIN is useless, use B-tree

-- Visibility map status (for index-only scan eligibility)
-- High all_visible fraction means index-only scans work efficiently
SELECT
    relname,
    n_live_tup,
    pg_relation_size(relid) AS heap_size,
    all_visible         -- pages where all tuples are visible (from pg_visibility extension)
FROM pg_stat_user_tables
WHERE relname = 'orders';
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
index_advisor.py

Simulate index selection decisions for the five Postgres index types.

Components:
  - ColumnProfile: characterizes a column's data type, cardinality, and access patterns
  - IndexType: enum of B-tree, Hash, GiST, GIN, BRIN
  - IndexAdvisor: recommends index type and features based on column profile + query pattern
  - PartialIndexCalculator: estimates size reduction from partial indexes
  - CoveringIndexAnalyzer: determines if a covering index would eliminate heap fetches
  - BRINEffectivenessCalc: estimates BRIN effectiveness from physical correlation
  - IndexSizeEstimator: estimates index sizes for B-tree, GIN, BRIN
  - UsageTracker: simulates pg_stat_user_indexes (tracks index scans)

No external dependencies.
"""

from __future__ import annotations

import math
import enum
from dataclasses import dataclass, field
from typing import Optional


# ─── Data Types and Patterns ─────────────────────────────────────────────────

class DataType(enum.Enum):
    INTEGER    = 'integer'
    BIGINT     = 'bigint'
    TEXT       = 'text'
    UUID       = 'uuid'
    TIMESTAMP  = 'timestamp'
    DATE       = 'date'
    FLOAT      = 'float'
    ARRAY      = 'array'
    JSONB      = 'jsonb'
    TSVECTOR   = 'tsvector'
    POINT      = 'point'
    RANGE_TYPE = 'range_type'


class QueryPattern(enum.Enum):
    EQUALITY         = 'equality'              # WHERE col = val
    RANGE            = 'range'                 # WHERE col BETWEEN a AND b
    SORT             = 'sort'                  # ORDER BY col
    PREFIX           = 'prefix'                # WHERE col LIKE 'prefix%'
    SUBSTRING        = 'substring'             # WHERE col LIKE '%substring%'
    ARRAY_CONTAINS   = 'array_contains'        # WHERE arr @> ARRAY[val]
    ARRAY_OVERLAP    = 'array_overlap'         # WHERE arr && ARRAY[val]
    JSON_CONTAINS    = 'json_contains'         # WHERE jsonb @> '{"key": val}'
    JSON_KEY_EXISTS  = 'json_key_exists'       # WHERE jsonb ? 'key'
    FULLTEXT         = 'fulltext'              # WHERE tsvec @@ tsquery
    GEOMETRIC_OVERLAP = 'geometric_overlap'   # WHERE geom && bbox
    NEAREST_NEIGHBOR = 'nearest_neighbor'     # ORDER BY geom <-> point
    RANGE_OVERLAP    = 'range_overlap'        # WHERE tsrange && '[a,b)'


class IndexType(enum.Enum):
    BTREE = 'btree'
    HASH  = 'hash'
    GIST  = 'gist'
    GIN   = 'gin'
    BRIN  = 'brin'
    NONE  = 'none'   # No suitable index


@dataclass
class ColumnProfile:
    """Characterizes a column for index recommendation."""
    name:             str
    data_type:        DataType
    n_distinct:       int              # Number of distinct values (approximate)
    n_rows:           int              # Total rows in table
    null_fraction:    float            # Fraction of NULLs (0.0 → 1.0)
    correlation:      float            # Physical order correlation (-1 → 1)
    avg_value_size:   int              # Average size of one value in bytes
    write_rate:       float            # Inserts+updates per second (for overhead estimate)


@dataclass
class IndexRecommendation:
    """Result of the advisor's analysis."""
    index_type:       IndexType
    reason:           str
    ddl:              str              # SQL to create the index
    estimated_size_mb: float
    write_overhead_pct: float         # Approximate % write throughput overhead
    warnings:         list[str] = field(default_factory=list)


# ─── Index Advisor ────────────────────────────────────────────────────────────

class IndexAdvisor:
    """
    Recommends the most appropriate Postgres index type for a column + query pattern.
    Mirrors the decision logic a senior engineer applies manually.
    """

    def recommend(self, col: ColumnProfile, patterns: list[QueryPattern],
                  table_name: str = 'table') -> IndexRecommendation:
        """
        Analyze column profile and query patterns; return best index recommendation.
        """
        # ── Special data types ───────────────────────────────────────────────
        if col.data_type in (DataType.ARRAY,):
            if any(p in patterns for p in [QueryPattern.ARRAY_CONTAINS,
                                            QueryPattern.ARRAY_OVERLAP]):
                return self._gin_recommendation(col, table_name, 'array operators')

        if col.data_type == DataType.JSONB:
            if any(p in patterns for p in [QueryPattern.JSON_CONTAINS,
                                            QueryPattern.JSON_KEY_EXISTS]):
                return self._gin_recommendation(col, table_name, 'JSONB containment/key operators')

        if col.data_type == DataType.TSVECTOR:
            if QueryPattern.FULLTEXT in patterns:
                return self._gin_recommendation(col, table_name, 'full-text search (@@)')

        if col.data_type in (DataType.POINT, DataType.RANGE_TYPE):
            if any(p in patterns for p in [QueryPattern.GEOMETRIC_OVERLAP,
                                            QueryPattern.NEAREST_NEIGHBOR,
                                            QueryPattern.RANGE_OVERLAP]):
                return self._gist_recommendation(col, table_name,
                                                  'geometric/range overlap operators')

        if col.data_type == DataType.TEXT and QueryPattern.SUBSTRING in patterns:
            return self._gin_trgm_recommendation(col, table_name)

        # ── Timestamp / auto-increment with high correlation ─────────────────
        if (col.data_type in (DataType.TIMESTAMP, DataType.DATE, DataType.INTEGER,
                               DataType.BIGINT)
                and abs(col.correlation) > 0.8
                and QueryPattern.RANGE in patterns
                and QueryPattern.EQUALITY not in patterns):
            return self._brin_recommendation(col, table_name)

        # ── Equality-only → Hash (rarely preferred; B-tree usually wins) ─────
        if (patterns == [QueryPattern.EQUALITY]
                and col.data_type in (DataType.UUID, DataType.TEXT)
                and col.avg_value_size > 32):
            return self._hash_recommendation(col, table_name)

        # ── Default: B-tree ──────────────────────────────────────────────────
        return self._btree_recommendation(col, patterns, table_name)

    # ── Index type recommendations ────────────────────────────────────────────

    def _btree_recommendation(self, col: ColumnProfile, patterns: list[QueryPattern],
                               table: str) -> IndexRecommendation:
        size_mb = IndexSizeEstimator.btree_size_mb(col)
        overhead = self._write_overhead(col, size_mb)
        warnings = []
        if col.n_distinct < 100 and col.n_rows > 100_000:
            warnings.append(
                f"Low cardinality ({col.n_distinct} distinct values on {col.n_rows:,} rows). "
                "Selectivity may be too low for index scans to be chosen by the planner. "
                "Consider a partial index to reduce cardinality impact.")
        ddl = f"CREATE INDEX CONCURRENTLY idx_{table}_{col.name} ON {table}({col.name});"
        if QueryPattern.EQUALITY in patterns and QueryPattern.RANGE not in patterns:
            ddl = (f"-- B-tree for equality is fine, but Hash may be slightly smaller "
                   f"for large keys:\n"
                   f"CREATE INDEX CONCURRENTLY idx_{table}_{col.name} ON {table}({col.name});")
        return IndexRecommendation(
            index_type=IndexType.BTREE,
            reason=f"B-tree: supports {[p.value for p in patterns]}. "
                   f"Default choice for scalar types.",
            ddl=ddl, estimated_size_mb=size_mb,
            write_overhead_pct=overhead, warnings=warnings)

    def _gin_recommendation(self, col: ColumnProfile, table: str,
                             use_case: str) -> IndexRecommendation:
        size_mb = IndexSizeEstimator.gin_size_mb(col)
        overhead = self._write_overhead(col, size_mb) * 2.5  # GIN is expensive to update
        warnings = []
        if col.write_rate > 100:
            warnings.append(
                f"High write rate ({col.write_rate}/s). GIN indexes are expensive to update "
                "(each row update may touch many posting lists). Consider 'fastupdate=on' "
                "(default) which batches GIN writes; flush with vacuum.")
        return IndexRecommendation(
            index_type=IndexType.GIN,
            reason=f"GIN: inverted index for {use_case}. Maps element values to row sets.",
            ddl=f"CREATE INDEX CONCURRENTLY idx_{table}_{col.name} "
                f"ON {table} USING GIN ({col.name});",
            estimated_size_mb=size_mb,
            write_overhead_pct=overhead, warnings=warnings)

    def _gin_trgm_recommendation(self, col: ColumnProfile, table: str) -> IndexRecommendation:
        size_mb = IndexSizeEstimator.gin_size_mb(col) * 1.5  # Trigrams expand size
        overhead = self._write_overhead(col, size_mb) * 3.0
        return IndexRecommendation(
            index_type=IndexType.GIN,
            reason="GIN with pg_trgm ops: enables LIKE '%substring%' and ILIKE searches.",
            ddl=(f"CREATE EXTENSION IF NOT EXISTS pg_trgm;\n"
                 f"CREATE INDEX CONCURRENTLY idx_{table}_{col.name}_trgm "
                 f"ON {table} USING GIN ({col.name} gin_trgm_ops);"),
            estimated_size_mb=size_mb,
            write_overhead_pct=overhead,
            warnings=["Trigram indexes are large (~3× raw column size). "
                      "Only create if LIKE '%substring%' is a common query pattern."])

    def _gist_recommendation(self, col: ColumnProfile, table: str,
                              use_case: str) -> IndexRecommendation:
        size_mb = IndexSizeEstimator.btree_size_mb(col) * 1.5  # GiST is larger than B-tree
        overhead = self._write_overhead(col, size_mb) * 1.5
        return IndexRecommendation(
            index_type=IndexType.GIST,
            reason=f"GiST: generalized search tree for {use_case}.",
            ddl=f"CREATE INDEX CONCURRENTLY idx_{table}_{col.name} "
                f"ON {table} USING GIST ({col.name});",
            estimated_size_mb=size_mb,
            write_overhead_pct=overhead,
            warnings=["GiST indexes support approximate searches; verify operator class "
                      "is correct for your data type."])

    def _brin_recommendation(self, col: ColumnProfile, table: str) -> IndexRecommendation:
        size_mb = IndexSizeEstimator.brin_size_mb(col)
        overhead = self._write_overhead(col, size_mb) * 0.01  # Extremely cheap
        warnings = []
        if abs(col.correlation) < 0.9:
            warnings.append(
                f"Column correlation is {col.correlation:.2f} (< 0.9). "
                "BRIN effectiveness is reduced. Consider B-tree if range scans "
                "need high precision.")
        return IndexRecommendation(
            index_type=IndexType.BRIN,
            reason=f"BRIN: stores min/max per block range (128 pages). "
                   f"Optimal for naturally ordered columns (correlation={col.correlation:.2f}). "
                   f"Orders-of-magnitude smaller than B-tree.",
            ddl=f"CREATE INDEX CONCURRENTLY idx_{table}_{col.name}_brin "
                f"ON {table} USING BRIN ({col.name} pages_per_range = 128);",
            estimated_size_mb=size_mb,
            write_overhead_pct=overhead, warnings=warnings)

    def _hash_recommendation(self, col: ColumnProfile, table: str) -> IndexRecommendation:
        size_mb = IndexSizeEstimator.btree_size_mb(col) * 0.7  # Hash smaller for large keys
        overhead = self._write_overhead(col, size_mb)
        return IndexRecommendation(
            index_type=IndexType.HASH,
            reason="Hash: equality-only lookups on large keys (UUID, long text). "
                   "Slightly smaller than B-tree; no range/sort support.",
            ddl=f"CREATE INDEX CONCURRENTLY idx_{table}_{col.name}_hash "
                f"ON {table} USING HASH ({col.name});",
            estimated_size_mb=size_mb,
            write_overhead_pct=overhead,
            warnings=["Hash index only supports '=' operator. "
                      "If range queries are ever added, B-tree is safer."])

    def _write_overhead(self, col: ColumnProfile, index_size_mb: float) -> float:
        """Rough estimate: overhead as % of write throughput."""
        if col.write_rate <= 0:
            return 0.0
        # Assume each write touches proportional amount of index
        return min(50.0, index_size_mb / (col.n_rows * col.avg_value_size / 1024 / 1024) * 20)


# ─── Index Size Estimator ─────────────────────────────────────────────────────

class IndexSizeEstimator:
    """Estimates index sizes for planning and capacity purposes."""

    PAGE_SIZE = 8192   # Bytes

    @classmethod
    def btree_size_mb(cls, col: ColumnProfile) -> float:
        """
        B-tree index size estimate.
        Leaf page: ~(key_size + 8 bytes TID + 4 bytes overhead) per entry
        Branching factor: (page_size - 24 header) / (key_size + 8)
        Leaf pages: n_distinct * leaf_entries_per_page... actually simpler:
        Total leaf entries ≈ n_rows (non-unique) or n_distinct (unique)
        """
        entry_size  = col.avg_value_size + 8 + 4    # key + TID + item overhead
        entries_per_page = max(1, (cls.PAGE_SIZE - 24) // entry_size)
        leaf_pages  = math.ceil(col.n_rows / entries_per_page)
        # Internal pages add ~10-30% overhead
        total_pages = leaf_pages * 1.20
        return total_pages * cls.PAGE_SIZE / 1024 / 1024

    @classmethod
    def gin_size_mb(cls, col: ColumnProfile) -> float:
        """
        GIN index size estimate.
        GIN inverted index: one posting list per distinct element value.
        Assumes average 10 elements per row, average 5 bytes per element.
        """
        avg_elements_per_row = 10
        total_elements = col.n_rows * avg_elements_per_row
        # Each posting list entry: 6 bytes (TID compressed)
        posting_bytes = total_elements * 6
        # Key tree: distinct_elements × 20 bytes
        distinct_elements = min(col.n_distinct * avg_elements_per_row,
                                 col.n_rows * avg_elements_per_row // 2)
        key_tree_bytes = distinct_elements * 20
        return (posting_bytes + key_tree_bytes) / 1024 / 1024

    @classmethod
    def brin_size_mb(cls, col: ColumnProfile) -> float:
        """
        BRIN index size estimate.
        One entry per block range (default 128 pages = 1MB of heap).
        Each entry: 2 × avg_value_size (min + max) + 8 bytes overhead.
        """
        heap_pages   = math.ceil(col.n_rows * (col.avg_value_size + 24) / cls.PAGE_SIZE)
        n_ranges     = math.ceil(heap_pages / 128)
        entry_size   = 2 * col.avg_value_size + 8
        total_bytes  = n_ranges * entry_size
        return max(0.001, total_bytes / 1024 / 1024)


# ─── Partial Index Calculator ─────────────────────────────────────────────────

class PartialIndexCalculator:
    """
    Estimates size and selectivity for partial indexes.
    A partial index indexes only the rows satisfying a WHERE condition.
    """

    def analyze(self, table_rows: int, partial_fraction: float,
                col: ColumnProfile, table: str) -> dict:
        """
        Compare full vs partial index.
        partial_fraction: fraction of rows included in the partial index.
        """
        full_col    = ColumnProfile(
            name=col.name, data_type=col.data_type,
            n_distinct=col.n_distinct, n_rows=table_rows,
            null_fraction=col.null_fraction, correlation=col.correlation,
            avg_value_size=col.avg_value_size, write_rate=col.write_rate)
        partial_col = ColumnProfile(
            name=col.name, data_type=col.data_type,
            n_distinct=max(1, int(col.n_distinct * partial_fraction)),
            n_rows=int(table_rows * partial_fraction),
            null_fraction=col.null_fraction, correlation=col.correlation,
            avg_value_size=col.avg_value_size, write_rate=col.write_rate * partial_fraction)

        full_size_mb    = IndexSizeEstimator.btree_size_mb(full_col)
        partial_size_mb = IndexSizeEstimator.btree_size_mb(partial_col)

        return {
            'table_rows':          table_rows,
            'indexed_rows':        partial_col.n_rows,
            'partial_fraction':    partial_fraction,
            'full_index_mb':       full_size_mb,
            'partial_index_mb':    partial_size_mb,
            'size_reduction_pct':  (1 - partial_size_mb / full_size_mb) * 100,
            'ddl': (f"CREATE INDEX CONCURRENTLY idx_{table}_{col.name}_partial\n"
                    f"  ON {table}({col.name}) WHERE status = 'target_value';")
        }


# ─── BRIN Effectiveness Calculator ────────────────────────────────────────────

class BRINEffectivenessCalc:
    """
    Estimates the fraction of the table BRIN can skip for a given range query.
    Works only when correlation is high (values inserted in physical order).
    """

    def estimate_skip_fraction(self, correlation: float,
                                 query_selectivity: float,
                                 pages_per_range: int = 128) -> dict:
        """
        Estimate what fraction of heap pages BRIN skips.
        correlation: physical order correlation of the column
        query_selectivity: fraction of rows satisfying the predicate
        """
        # Perfect correlation: skip = (1 - selectivity) of the table
        # (all matching rows are in a contiguous block range)
        # Imperfect correlation: some matching rows are scattered
        ideal_skip    = 1.0 - query_selectivity
        actual_skip   = ideal_skip * abs(correlation)
        pages_scanned = 1.0 - actual_skip

        return {
            'correlation':        correlation,
            'query_selectivity':  query_selectivity,
            'pages_per_range':    pages_per_range,
            'ideal_skip_pct':     ideal_skip * 100,
            'actual_skip_pct':    actual_skip * 100,
            'pages_scanned_pct':  pages_scanned * 100,
            'effective':          actual_skip > 0.5,
            'recommendation':     (
                'BRIN effective: skips most of the table' if actual_skip > 0.7
                else 'BRIN marginally effective: consider B-tree' if actual_skip > 0.3
                else 'BRIN ineffective: use B-tree (correlation too low)')
        }


# ─── Usage Tracker (pg_stat_user_indexes simulation) ─────────────────────────

class IndexUsageTracker:
    """
    Tracks index scan counts to identify unused indexes.
    Simulates pg_stat_user_indexes.
    """

    def __init__(self):
        self._scans: dict[str, int]     = {}
        self._sizes: dict[str, float]   = {}

    def register(self, index_name: str, size_mb: float):
        self._scans[index_name] = 0
        self._sizes[index_name] = size_mb

    def record_scan(self, index_name: str, count: int = 1):
        if index_name in self._scans:
            self._scans[index_name] += count

    def unused_indexes(self) -> list[dict]:
        return [
            {'index': k, 'scans': v, 'size_mb': self._sizes[k]}
            for k, v in self._scans.items() if v == 0
        ]

    def print_report(self):
        print(f"\n  {'Index':<40} {'Scans':>8} {'Size MB':>10}")
        print(f"  {'-'*40} {'-'*8} {'-'*10}")
        for name, scans in sorted(self._scans.items(), key=lambda x: x[1]):
            flag = ' ← UNUSED' if scans == 0 else ''
            print(f"  {name:<40} {scans:>8,} {self._sizes[name]:>10.2f}{flag}")


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Postgres Index Advisor ===\n")
    advisor    = IndexAdvisor()
    tracker    = IndexUsageTracker()

    table_name = "events"
    n_rows     = 10_000_000

    # ── Phase 1: Index Type Recommendations ───────────────────────────────────
    print("─── Phase 1: Index Type Recommendations ───\n")

    test_cases = [
        (ColumnProfile("created_at", DataType.TIMESTAMP, n_distinct=10_000_000,
                       n_rows=n_rows, null_fraction=0.0, correlation=0.98,
                       avg_value_size=8, write_rate=1000.0),
         [QueryPattern.RANGE],
         "Timestamp on append-only table (high correlation) — time-range queries"),

        (ColumnProfile("user_id", DataType.INTEGER, n_distinct=500_000,
                       n_rows=n_rows, null_fraction=0.0, correlation=0.05,
                       avg_value_size=4, write_rate=1000.0),
         [QueryPattern.EQUALITY, QueryPattern.RANGE],
         "User ID (random distribution) — equality and range"),

        (ColumnProfile("tags", DataType.ARRAY, n_distinct=200,
                       n_rows=n_rows, null_fraction=0.1, correlation=0.0,
                       avg_value_size=40, write_rate=500.0),
         [QueryPattern.ARRAY_CONTAINS, QueryPattern.ARRAY_OVERLAP],
         "Tags array column — contains/overlap operators"),

        (ColumnProfile("payload", DataType.JSONB, n_distinct=n_rows,
                       n_rows=n_rows, null_fraction=0.0, correlation=0.0,
                       avg_value_size=500, write_rate=1000.0),
         [QueryPattern.JSON_CONTAINS, QueryPattern.JSON_KEY_EXISTS],
         "JSONB payload — containment and key existence"),

        (ColumnProfile("body_ts", DataType.TSVECTOR, n_distinct=n_rows,
                       n_rows=n_rows, null_fraction=0.0, correlation=0.0,
                       avg_value_size=200, write_rate=200.0),
         [QueryPattern.FULLTEXT],
         "tsvector column — full-text search"),

        (ColumnProfile("name", DataType.TEXT, n_distinct=n_rows,
                       n_rows=n_rows, null_fraction=0.0, correlation=0.0,
                       avg_value_size=30, write_rate=200.0),
         [QueryPattern.SUBSTRING],
         "Text name column — LIKE '%substring%' queries"),
    ]

    for col, patterns, description in test_cases:
        rec = advisor.recommend(col, patterns, table_name)
        print(f"  Column: {col.name} ({description})")
        print(f"  → Index type: {rec.index_type.value.upper()}")
        print(f"    Reason:     {rec.reason}")
        print(f"    Est. size:  {rec.estimated_size_mb:.1f} MB")
        print(f"    Write overhead: {rec.write_overhead_pct:.1f}%")
        print(f"    DDL: {rec.ddl}")
        for w in rec.warnings:
            print(f"    ⚠ {w}")
        # Register in usage tracker
        tracker.register(f"idx_{table_name}_{col.name}", rec.estimated_size_mb)
        print()

    # ── Phase 2: BRIN Effectiveness ────────────────────────────────────────────
    print("─── Phase 2: BRIN Effectiveness by Correlation ───\n")
    brin_calc = BRINEffectivenessCalc()
    print(f"  {'Correlation':>12} {'Selectivity':>12} {'Skip %':>8} {'Effective':>10}")
    print(f"  {'-'*12} {'-'*12} {'-'*8} {'-'*10}")
    for corr in [0.99, 0.90, 0.70, 0.50, 0.20, 0.05]:
        for sel in [0.01, 0.10]:
            result = brin_calc.estimate_skip_fraction(corr, sel)
            print(f"  {corr:>12.2f} {sel:>12.2%} "
                  f"{result['actual_skip_pct']:>7.1f}% "
                  f"{result['effective']:>10}")
    print()

    # ── Phase 3: Partial Index Analysis ───────────────────────────────────────
    print("─── Phase 3: Partial Index Size Reduction ───\n")
    partial_calc = PartialIndexCalculator()
    orders_col   = ColumnProfile("customer_id", DataType.INTEGER, n_distinct=50_000,
                                  n_rows=1_000_000, null_fraction=0.0, correlation=0.1,
                                  avg_value_size=4, write_rate=500.0)
    for fraction in [0.5, 0.2, 0.05, 0.01]:
        result = partial_calc.analyze(1_000_000, fraction, orders_col, "orders")
        print(f"  Partial fraction={fraction:.0%}: "
              f"indexed_rows={result['indexed_rows']:,}, "
              f"full={result['full_index_mb']:.1f}MB → "
              f"partial={result['partial_index_mb']:.1f}MB "
              f"({result['size_reduction_pct']:.0f}% smaller)")

    # ── Phase 4: Index Size Comparison ────────────────────────────────────────
    print("\n─── Phase 4: Index Size Comparison (10M rows, 8-byte timestamp) ───\n")
    ts_col = ColumnProfile("created_at", DataType.TIMESTAMP, n_distinct=10_000_000,
                            n_rows=10_000_000, null_fraction=0.0, correlation=0.99,
                            avg_value_size=8, write_rate=1000.0)
    btree_mb = IndexSizeEstimator.btree_size_mb(ts_col)
    brin_mb  = IndexSizeEstimator.brin_size_mb(ts_col)
    print(f"  B-tree on created_at: {btree_mb:.1f} MB")
    print(f"  BRIN  on created_at:  {brin_mb:.3f} MB")
    print(f"  Size ratio: B-tree is {btree_mb / max(0.001, brin_mb):.0f}× larger than BRIN")
    print(f"  (But BRIN only works because correlation = 0.99)")

    # ── Phase 5: Unused Index Detection ───────────────────────────────────────
    print("\n─── Phase 5: Unused Index Detection ───")
    # Simulate some queries running
    tracker.record_scan(f"idx_{table_name}_created_at", count=50_000)
    tracker.record_scan(f"idx_{table_name}_user_id",    count=25_000)
    tracker.record_scan(f"idx_{table_name}_tags",       count=8_000)
    # payload, body_ts, name indexes never scanned
    tracker.print_report()
    unused = tracker.unused_indexes()
    if unused:
        print(f"\n  {len(unused)} unused indexes found.")
        total_waste = sum(u['size_mb'] for u in unused)
        print(f"  Total wasted space: {total_waste:.1f} MB")
        print(f"  To drop safely: DROP INDEX CONCURRENTLY <index_name>;")
```

---

## 6. Visual Reference

### Index Type Decision Tree

```
What is the column's data type and query pattern?
         │
         ├── Array column?  ──────────────────────────────────→  GIN
         │   (@>, &&, = ANY)
         │
         ├── JSONB column? ────────────────────────────────────→  GIN
         │   (@>, ?, ?|, ?&)
         │
         ├── tsvector / full-text? ────────────────────────────→  GIN
         │   (@@)
         │
         ├── Text LIKE '%substring%'? ────────────────────────→  GIN + pg_trgm
         │
         ├── Geometric / Point / Range type? ────────────────→  GiST
         │   (&&, <->, @>, <@)
         │
         ├── Timestamp/Integer AND high correlation (>0.8)? ──→  BRIN
         │   Range queries only, append-only table
         │
         ├── Large text/UUID equality ONLY? ──────────────────→  Hash (rarely)
         │   (= only, no range/sort needed)
         │
         └── Everything else? ─────────────────────────────────→  B-tree
             (=, <, >, BETWEEN, LIKE 'prefix%', ORDER BY)
```

### Scan Type vs Index Type Mapping

```
Query Predicate                   B-tree   Hash   GiST   GIN    BRIN
─────────────────────────────────────────────────────────────────────
WHERE col = val                     ✓        ✓      ~       ~      ~
WHERE col > val                     ✓        ✗      ~       ✗      ✓
WHERE col BETWEEN a AND b           ✓        ✗      ~       ✗      ✓
WHERE col LIKE 'prefix%'            ✓*       ✗      ✗       ✗      ✗
WHERE col LIKE '%substring%'        ✗        ✗      ✓**     ✓**    ✗
WHERE arr @> ARRAY[x]               ✗        ✗      ✗       ✓      ✗
WHERE jsonb @> '{}'                 ✗        ✗      ✗       ✓      ✗
WHERE tsvec @@ tsquery              ✗        ✗      ✓       ✓      ✗
WHERE geom && bbox                  ✗        ✗      ✓       ✗      ✗
ORDER BY geom <-> point             ✗        ✗      ✓       ✗      ✗
WHERE range && '[a,b)'              ✗        ✗      ✓       ✗      ✗
ORDER BY col                        ✓        ✗      ✗       ✗      ✗

✓ = native support   ✗ = not supported   ~ = possible but rarely chosen
* requires text_pattern_ops operator class for LIKE
** requires pg_trgm extension
```

---

## 7. Common Mistakes

**Mistake 1: Creating an index and not checking if the planner uses it.** After `CREATE INDEX`, always run `EXPLAIN ANALYZE` on the target query to verify the planner actually uses the new index. Common reasons it doesn't: the planner's statistics are stale (run `ANALYZE`); the selectivity is too low (query returns too many rows); `random_page_cost` is too high for SSDs; or the expression in the query doesn't match the indexed expression (e.g., index on `email` but query has `LOWER(email)`).

**Mistake 2: Not using `CONCURRENTLY` on production tables.** `CREATE INDEX` without `CONCURRENTLY` takes an `AccessShareLock` that blocks all writes to the table for the entire index build duration — which can be hours for a large table. This causes a production outage. Always use `CREATE INDEX CONCURRENTLY` on any table that receives writes. It takes longer (two passes over the table) but does not block writes.

**Mistake 3: Indexing low-cardinality columns without partial filtering.** An index on `status` with 3 distinct values ('active', 'inactive', 'deleted') on a 10M-row table has essentially no selectivity — each value returns millions of rows. The planner will choose a sequential scan. Worse, the index still incurs write overhead for every status update. The fix: if you frequently query only `WHERE status = 'active'` (which is 2% of rows), create a partial index `WHERE status = 'active'` — small, selective, and the planner will use it.

---

## 8. Production Failure Scenarios

### Scenario 1: GIN Index Causing Write Stalls (fastupdate)

**Symptoms:** After deploying a feature that adds tags to posts (each post update writes to the `tags` array column, which has a GIN index), write throughput drops by 60%. Postgres logs show frequent `autovacuum: processing index "idx_posts_tags"`.

**Root cause:** GIN indexes use a "pending list" optimization (`fastupdate=on` by default): new entries are written to a pending list rather than inserted directly into the main GIN structure (avoiding expensive GIN tree insertions on every write). The pending list is flushed to the main GIN structure during VACUUM or when the pending list exceeds `gin_pending_list_limit` (4MB default). During flush, the GIN tree is locked, stalling all writes to the index. If writes are faster than autovacuum can flush, pending lists grow and flush stalls accumulate.

**Fix:**
```sql
-- Option 1: Increase gin_pending_list_limit to reduce flush frequency
ALTER INDEX idx_posts_tags SET (fastupdate = on, gin_pending_list_limit = 64000); -- 64MB

-- Option 2: Disable fastupdate if write throughput is more important than latency
-- (each write goes directly to GIN tree — higher per-write cost but no stall spikes)
ALTER INDEX idx_posts_tags SET (fastupdate = off);

-- Option 3: Manual vacuum to flush pending list during off-peak hours
VACUUM posts;  -- flushes GIN pending list

-- Monitor pending list size:
SELECT pg_size_pretty(pg_relation_size('idx_posts_tags')) AS index_size;
-- Watch for growth between vacuums
```

### Scenario 2: BRIN Index on a Non-Ordered Column

**Symptoms:** A data engineer creates a BRIN index on `user_id` to save space (the table is 500GB). Range queries like `WHERE user_id BETWEEN 1000 AND 2000` are slower than before with no index. `EXPLAIN ANALYZE` shows `Bitmap Heap Scan` with 95% of table pages scanned.

**Root cause:** `user_id` was assigned randomly (UUID-based internal ID) when users registered. Users with IDs 1000-2000 are physically scattered across all heap pages (correlation ≈ 0.02 in pg_stats). BRIN stores min/max per 128-page block range, but since each block range contains user IDs from across the entire range (1 to 10M), no block ranges can be skipped. The BRIN index is nearly useless — it returns almost all block ranges as candidates, and the actual heap scan reads 95% of the table.

**Fix:**
```sql
-- Step 1: Check correlation before creating BRIN
SELECT attname, correlation FROM pg_stats
WHERE tablename = 'users' AND attname = 'user_id';
-- Result: correlation = 0.02 → BRIN is wrong for this column

-- Step 2: Drop the useless BRIN index
DROP INDEX CONCURRENTLY idx_users_userid_brin;

-- Step 3: Create appropriate index based on actual query patterns
-- If equality queries: B-tree
CREATE INDEX CONCURRENTLY idx_users_userid ON users(user_id);

-- Step 4: For the table size concern, address bloat instead of wrong index type
-- Check table bloat:
SELECT pg_size_pretty(pg_relation_size('users')) AS table_size,
       pg_size_pretty(pg_total_relation_size('users')) AS total_size;
-- If table is bloated (dead tuples), VACUUM FULL (with downtime)
-- or pg_repack (without downtime) reduces table size more effectively
```

---

## 9. Performance and Tuning

### Index Maintenance and Monitoring

```sql
-- ── Detect bloated indexes ────────────────────────────────────────────────────
-- pgstattuple extension gives bloat statistics
CREATE EXTENSION IF NOT EXISTS pgstattuple;

SELECT
    index_size,
    round(leaf_fragmentation::numeric, 2)  AS fragmentation_pct,
    round(dead_leaf_percent::numeric, 2)   AS dead_leaf_pct
FROM pgstatindex('idx_orders_customer');
-- dead_leaf_percent > 30% → REINDEX CONCURRENTLY to reclaim space and improve scan speed

-- ── Rebuild bloated index (Postgres 12+, no write lock) ───────────────────────
REINDEX INDEX CONCURRENTLY idx_orders_customer;

-- ── Identify all candidate indexes for rebuild ─────────────────────────────────
SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS size
FROM pg_indexes
WHERE tablename = 'orders'
ORDER BY pg_relation_size(indexname::regclass) DESC;

-- ── Index hit rate (complement of buffer pool miss rate for indexes) ────────────
SELECT
    relname AS table_name,
    indexrelname AS index_name,
    idx_blks_hit,
    idx_blks_read,
    round(idx_blks_hit::numeric / nullif(idx_blks_hit + idx_blks_read, 0) * 100, 2)
        AS index_hit_pct
FROM pg_statio_user_indexes
ORDER BY idx_blks_read DESC;

-- ── Total index overhead on writes (proxy: sum of index size vs table size) ────
SELECT
    tablename,
    pg_size_pretty(pg_relation_size(tablename::regclass)) AS table_size,
    pg_size_pretty(sum(pg_relation_size(indexname::regclass))) AS total_index_size,
    count(*) AS n_indexes
FROM pg_indexes
GROUP BY tablename
ORDER BY sum(pg_relation_size(indexname::regclass)) DESC;

-- ── Index scan rate by table (identify hot indexes) ────────────────────────────
SELECT
    relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC
LIMIT 20;
```

---

## 10. Interview Q&A

**Q1: When would you choose a BRIN index over a B-tree index? What is the prerequisite for BRIN to be effective?**

BRIN is the right choice when three conditions hold simultaneously: the column's values are strongly correlated with physical heap page order, the query access pattern is range-based rather than point-lookup, and the table is large enough that the size difference between BRIN and B-tree matters operationally.

The critical prerequisite is physical correlation. BRIN stores only the minimum and maximum value for each block range — a contiguous group of heap pages (128 pages = 1MB of heap by default). When a range query arrives, BRIN skips entire block ranges whose max is below the query's lower bound. This skip is only effective when values increase as you move through the heap — i.e., when rows were inserted in sorted order on that column. A timestamp column on an event table (events are always inserted at the current time, so page 1 contains the oldest events and the last page contains the newest) has near-perfect correlation (≈ 0.99). A random UUID column has zero correlation.

The size advantage is dramatic. A BRIN index on a 1-billion-row table with 128 pages per range has about 7.8 million ranges × ~20 bytes each = 156MB. A B-tree on the same column might be 30-50GB. For cloud storage costs and backup sizes, this difference is meaningful. The operational trade-off: B-tree provides precise point lookups (O(log N)); BRIN provides approximate range filtering and must scan all pages in the matching block ranges.

**Q2: A query uses `WHERE tags @> ARRAY['postgres', 'performance']` on an array column. What index do you create and why?**

The `@>` operator (array contains) is an inverted index operation — given a set of element values, find all rows where the array column contains all of them. B-tree indexes impose a total ordering on arrays (lexicographic by array elements) and cannot efficiently answer "which rows contain these elements?" because that requires scanning the entire index in the worst case.

GIN (Generalized Inverted Index) is exactly the right type here. GIN builds an inverted index: for each distinct element value that appears in any array in the column, it maintains a sorted posting list of all row IDs (TIDs) containing that element. For the query `WHERE tags @> ARRAY['postgres', 'performance']`, the GIN index directly fetches the posting list for 'postgres' and the posting list for 'performance', intersects them, and returns the matching TIDs — an O(log(n_distinct_elements) + result_size) operation regardless of how many rows exist.

The DDL: `CREATE INDEX CONCURRENTLY idx_posts_tags ON posts USING GIN (tags)`. The planner will use this for `@>` (contains), `&&` (overlap), `= ANY(ARRAY[...])`, and `<@` (contained by). One operational note: GIN indexes are more expensive to update than B-tree — each insert must update the posting lists for every element in the new array. For high-write workloads, `fastupdate=on` (default) batches GIN writes into a pending list and flushes periodically. Monitor the pending list size and ensure autovacuum keeps up.

---

## 11. Cross-Question Chain

**Interviewer:** You have a 500GB events table, and the primary query pattern is `WHERE created_at BETWEEN '2024-01-01' AND '2024-01-31'`. The table has 1 billion rows. What index do you create?

**Candidate:** I'd check physical correlation first: `SELECT correlation FROM pg_stats WHERE tablename = 'events' AND attname = 'created_at'`. If correlation is above 0.9 — which it should be for an events table where rows are inserted chronologically — BRIN is the right choice. `CREATE INDEX CONCURRENTLY idx_events_created_brin ON events USING BRIN (created_at pages_per_range = 32)`. For a range query on a naturally ordered column, BRIN skips entire block ranges before the query window's start date, scanning only the pages that contain January 2024 data. The index will be a few megabytes instead of the 20-30GB a B-tree would require.

**Interviewer:** The query also needs `SELECT event_id, user_id, event_type FROM events WHERE created_at BETWEEN ...`. Will BRIN help with `SELECT *`?

**Candidate:** BRIN helps by restricting which heap pages Postgres reads — it filters at the block range level, not the row level. So even for `SELECT *`, BRIN reduces I/O by skipping the heap pages that don't contain any matching rows. If January 2024 data occupies, say, pages 8000-8500 out of 80,000 total pages, BRIN allows Postgres to read only those 500 pages (6.25%) instead of all 80,000. The benefit is the same whether we're selecting 3 columns or all columns — the reduction is in heap pages read, not in columns fetched per page. BRIN doesn't help with column pruning (that's columnar storage, not row-based Postgres without column encoding).

**Interviewer:** Six months later, you run `EXPLAIN ANALYZE` and the query is much slower. BRIN is still used. What happened?

**Candidate:** The most likely cause: the BRIN index is stale. BRIN indexes are not automatically updated page-by-page like B-tree indexes. They require manual or autovacuum-triggered `BRIN_SUMMARIZE_RANGE` calls to update the min/max summaries for pages that have been modified. If rows were deleted or updated in the January 2024 range and new rows were inserted into pages the January BRIN ranges cover, the BRIN summaries could be out of date — causing the index to either include extra pages (broader scan) or miss the right pages.

But more likely: the query is now much larger. Six months ago, January 2024 was recent data in a few hundred pages. Now the table has grown by 500M rows. The absolute block range count is the same for January 2024, but the surrounding table is much larger, and if there's been any out-of-order data (reloads, corrections, backdated events), those pages may have contaminated BRIN summaries. Run `SELECT brin_summarize_new_values('idx_events_created_brin')` to update summaries for unsummarized page ranges. Then check if the query speed is restored.

**Interviewer:** The events table also has a `payload JSONB` column. Queries like `WHERE payload @> '{"event_type": "purchase"}'` are slow. What do you do?

**Candidate:** Create a GIN index on the JSONB column: `CREATE INDEX CONCURRENTLY idx_events_payload ON events USING GIN (payload)`. This creates an inverted index over all key-value pairs and nested objects in the JSONB data, making `@>` (contains), `?` (key exists), and `?|` / `?&` (any/all keys exist) queries use the index instead of scanning the full 500GB table.

For high-write tables, I'd also consider a more targeted approach: if queries always filter on `event_type` specifically, create a computed column: `payload->>'event_type'` (a text column), index that with B-tree, and use `WHERE payload->>'event_type' = 'purchase'`. This creates a much smaller B-tree index than the full JSONB GIN index, with lower write overhead. The trade-off: the B-tree on the extracted field only works for that specific path, while the GIN on the full JSONB works for any key/value combination.

**Interviewer:** Both the BRIN and GIN indexes are created. The table is now 500GB + BRIN (10MB) + GIN (30GB). What's the total write overhead of having these two indexes on a 1,000-inserts/second event ingestion pipeline?

**Candidate:** BRIN has negligible write overhead — it updates only when a new heap page is created and summarized, which happens once per 128 pages × 8KB = approximately once per 1MB of new data. At 1,000 inserts/second with each event ~500 bytes, that's about 0.5MB/s of new data, so BRIN updates perhaps once every 2 seconds. Essentially zero overhead.

GIN is the expensive one. Every insert updates the GIN index for every distinct key-value pair in the JSONB payload. If each event has 10 key-value pairs, each insert touches 10 GIN posting lists. With `fastupdate=on` (default), these writes go to a pending list and are batched — the amortized write cost is lower. But when the pending list flushes (during vacuum or when the list fills), there's a burst of GIN tree insertions. At 1,000 inserts/second × 10 keys per event = 10,000 GIN list entries per second. With GIN fastupdate, the pending list grows at roughly 10,000 × 20 bytes = 200KB/s. It flushes every 4MB (default `gin_pending_list_limit`), about every 20 seconds. During a flush, GIN takes an exclusive lock on the index for milliseconds to seconds depending on the pending list size. I'd monitor `pg_relation_size('idx_events_payload')` growth and autovacuum timing to ensure flushes aren't accumulating.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What operators does a B-tree index support? | `=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `IS NULL`, `LIKE 'prefix%'`, `ORDER BY`, `GROUP BY`. Does NOT support `LIKE '%substring%'`, array containment, or JSONB containment. |
| 2 | What is an index-only scan and when is it possible? | Returns column values from the index leaf pages without reading the heap. Requires: all queried columns (SELECT + WHERE) are in the index (or INCLUDE list), AND the visibility map bit is set for the heap pages (indicating all tuples are visible). |
| 3 | What is the `INCLUDE` clause in a B-tree index? | Adds non-key columns to index leaf pages without including them in the sort key. Enables index-only scans for queries that need those columns. Example: `CREATE INDEX ON orders(customer_id) INCLUDE (amount)` — allows `SELECT amount WHERE customer_id = X` to skip the heap. |
| 4 | When should you use a partial index? | When queries always include a specific filter condition (e.g., `WHERE status = 'active'`) and the filtered subset is much smaller than the full table. Partial indexes are smaller, faster to maintain, and more selective. The WHERE condition must appear in the query for the index to be used. |
| 5 | What is a GIN index and what is it for? | Generalized Inverted Index: maps element values to sorted posting lists of TIDs (rows containing that element). Designed for multi-valued types: arrays (`@>`, `&&`), JSONB (`@>`, `?`), and tsvector (`@@`). |
| 6 | What is the difference between GIN and GiST for full-text search? | GIN is faster for lookup (`@@`) but slower to build/update and larger. GiST is faster to build and update, supports fuzzy prefix matching (with pg_trgm), but slower for exact full-text lookups. Use GIN for read-heavy full-text; GiST for write-heavy or when prefix/similarity queries dominate. |
| 7 | What is BRIN and what is its prerequisite? | Block Range INdex: stores min/max per group of 128 heap pages. Prerequisite: physical correlation — the column's values must increase (or decrease) with physical page order. Check: `SELECT correlation FROM pg_stats`. Effective when `abs(correlation) > 0.8`. |
| 8 | Why is BRIN orders of magnitude smaller than B-tree? | B-tree indexes one entry per row. BRIN indexes one entry per 128 pages (≈1MB of heap), storing only min/max. For a 1B-row table: B-tree ≈ 30GB; BRIN ≈ 150MB. |
| 9 | When does the planner ignore an existing index? | When: selectivity is too high (sequential scan cheaper); statistics are stale (wrong cardinality estimate); `random_page_cost` is set too high (overestimates index I/O); query expression doesn't match the indexed expression (e.g., `LOWER(email)` vs index on `email`); or partial index condition doesn't match the query's WHERE clause. |
| 10 | What is `CREATE INDEX CONCURRENTLY` and why is it essential? | Creates an index in two passes without taking a write-blocking lock. The standard `CREATE INDEX` holds an exclusive lock (blocking all writes) for the entire build — can be hours for large tables. `CONCURRENTLY` allows writes to proceed during the build. Always use on production tables. |
| 11 | What does GIN `fastupdate=on` do and when is it a problem? | Batches GIN write operations into a pending list, deferring the expensive GIN tree insertion until VACUUM or the pending list fills. Reduces per-write latency. Problem: when the pending list flushes, GIN locks the index — causing write latency spikes on high-write tables. Fix: increase `gin_pending_list_limit`, run manual `VACUUM`, or disable fastupdate. |
| 12 | How do you detect unused indexes in Postgres? | Query `pg_stat_user_indexes WHERE idx_scan = 0`. These indexes have not been used since the last statistics reset. Unused indexes consume disk space and slow all writes without benefiting any reads. Drop with `DROP INDEX CONCURRENTLY`. |
| 13 | When should you use a trigram GIN index (`gin_trgm_ops`)? | When queries use `LIKE '%substring%'` or `ILIKE` patterns — which B-tree cannot support and GiST handles only approximately. Trigram GIN breaks text into 3-character substrings and indexes each trigram. Larger than a B-tree (each value generates many trigrams), but enables arbitrary substring lookups. |
| 14 | What is index bloat and how do you fix it? | Index bloat: dead index entries from deleted/updated rows. Unlike heap bloat (which VACUUM reclaims in-place), index dead entries accumulate and are reclaimed during VACUUM but can fragment the index over time. Fix: `REINDEX INDEX CONCURRENTLY idx_name` rebuilds the index from scratch (Postgres 12+, non-blocking). |
| 15 | What is a composite index and what is the "leading column" rule? | A composite index on (A, B) is sorted by A first, then B within each A value. Queries can use the index if they filter on A alone, or A AND B. They CANNOT use the index for B alone (that would require scanning the entire index in random A order). |

---

## 13. Further Reading

- **Postgres documentation — "Indexes" chapter:** Complete reference for all index types, operator classes, partial indexes, and expression indexes.
- **"The Internals of PostgreSQL" by Hironobu Suzuki (interdb.jp) — Chapter 1 (Database Cluster) and Chapter 7 (Vacuum Processing):** Explains the visibility map, HOT updates, and how they interact with index-only scans and index bloat.
- **Postgres source: `src/backend/access/`:** `nbtree/` for B-tree, `gin/` for GIN, `gist/` for GiST, `brin/` for BRIN — each has a `README` explaining the algorithm.
- **"Use the Index, Luke" (use-the-index-luke.com):** The best practical guide to B-tree index behavior, composite indexes, and when the planner uses indexes.
- **pg_trgm documentation:** Explains trigram indexing for substring and similarity search.

---

## 14. Lab Exercises

**Exercise 1: Scan Type Verification**
Create a `posts` table with an array `tags TEXT[]` column. Insert 100,000 rows with random tags from a vocabulary of 50 values. Create a GIN index. Run `EXPLAIN ANALYZE` for: `WHERE tags @> ARRAY['tag1']`, `WHERE tags && ARRAY['tag1', 'tag2']`. Then try the same queries without the GIN index (`DROP INDEX`). Compare execution times.

**Exercise 2: BRIN vs B-tree**
Create an `events` table with `created_at TIMESTAMPTZ DEFAULT NOW()`. Insert 1M rows (they will be in chronological order, giving high correlation). Create both a BRIN and a B-tree index on `created_at`. Compare index sizes. Run `EXPLAIN ANALYZE` for `WHERE created_at > NOW() - INTERVAL '1 day'` with each index. Compare rows_scanned and execution time.

**Exercise 3: Unused Index Audit**
On any development Postgres database, run the unused index query from Section 4.1. For each unused index found, ask: when was it last scanned (`last_idx_scan` from `pg_stat_user_indexes`)? Is it a primary key or unique constraint (cannot be dropped)? Calculate the write overhead of each unused index as a fraction of total index size on the table.

**Exercise 4: `index_advisor.py` Extension**
In `index_advisor.py`, add a new `QueryPattern`: `NEAREST_NEIGHBOR` (for `ORDER BY geom <-> point`). Extend the `IndexAdvisor.recommend()` method to return a GiST recommendation when `NEAREST_NEIGHBOR` appears in the patterns. Test it with a `ColumnProfile` using `DataType.POINT`.

---

## 15. Key Takeaways

Every Postgres index type is a specialized data structure for a specific operator family. B-tree handles the equality/range/sort cases that comprise 80% of queries. GIN handles the inverted-index cases for arrays, JSONB, and full-text search — any query of the form "find rows containing element X." GiST handles geometric and range overlap, where bounding-box abstractions make sense. BRIN handles range queries on naturally ordered columns, trading precision for extreme size efficiency. Hash exists but is rarely the right answer.

The decision between index types is not a preference — it is determined by the operators in the query's WHERE clause. `@>` with arrays requires GIN. `&&` with a range type requires GiST. `@@` for full-text requires GIN (or GiST with pg_trgm for fuzzy). `LIKE '%substring%'` requires GIN with gin_trgm_ops. Using B-tree for any of these results in a sequential scan, not a slow index scan — B-tree simply cannot express those operations.

The operational concerns — index bloat, unused indexes, write overhead, CONCURRENTLY creation — are as important as the type selection. An index that exists but is never scanned adds write overhead with zero read benefit. An index built without CONCURRENTLY on a production table causes a write outage.

---

## 16. Connections to Other Modules

- **M47 — B-Tree Storage Engines:** The B-tree index in Postgres (nbtree) is a direct implementation of the B+-tree from M47. The page layout, split algorithm, and range scan via leaf linked list described in M47 are the mechanics of every B-tree index in this module.
- **M53 — Query Planning:** The scan type decision (index scan vs bitmap scan vs seq scan) from M53 depends on which indexes exist (this module) and their selectivity. `random_page_cost` is the cost model parameter that controls when the planner chooses index vs sequential scans.
- **M55 — Vacuuming and Bloat:** Index bloat (dead index entries) is addressed by VACUUM and REINDEX CONCURRENTLY. The visibility map (maintained by VACUUM) is the prerequisite for index-only scans — if VACUUM hasn't marked pages as all-visible, index-only scans must fetch the heap.
- **M52 — Postgres Architecture:** `CREATE INDEX CONCURRENTLY` is safe because Postgres's multi-version concurrency model allows the index builder to co-exist with live write transactions. The index build scans the table twice: once to build the initial structure, and once to catch rows inserted during the build — coordinated via the snapshot mechanism in the procarray (M52).

---

## 17. Anti-Patterns

**Anti-pattern: Creating an index on every column a query touches.** Indexes on columns with very low cardinality (< 100 distinct values on millions of rows) are almost never used by the planner — the selectivity is too low. Worse, they impose write overhead on every INSERT/UPDATE/DELETE. Before creating an index, check: what is the cardinality of this column? What is the typical query selectivity? Could a partial index on a specific value make it selective enough to be useful?

**Anti-pattern: Using a JSONB GIN index as a substitute for schema design.** A GIN index on a `payload JSONB` column enables ad-hoc queries against arbitrary JSON fields — but it is large (typically 5-20% of the table size), expensive to update, and the planner's statistics for JSONB are less precise than for typed columns. If specific JSONB fields are queried frequently, extract them as typed computed columns (`payload->>'event_type'` as a `TEXT` stored generated column) and index the extracted column with a B-tree. The result is a smaller, more selective, and planner-friendly index.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pg_stats` | Planner statistics per column | Check `correlation` for BRIN suitability |
| `pg_stat_user_indexes` | Index scan counts | Find unused indexes (`idx_scan = 0`) |
| `pg_statio_user_indexes` | Index I/O statistics | `idx_blks_read` for index cache hit rate |
| `pg_indexes` | Index definitions | List all indexes and their DDL |
| `pgstattuple` (extension) | Index bloat analysis | `dead_leaf_percent` indicates need for REINDEX |
| `pg_trgm` (extension) | Trigram similarity | Enables GIN/GiST for `LIKE '%substring%'` |
| `REINDEX INDEX CONCURRENTLY` | Rebuild bloated index | Non-blocking rebuild (Postgres 12+) |
| `index_advisor.py` | Index type recommendation | Simulates decision logic for all 5 types |

---

## 19. Glossary

**BRIN (Block Range INdex):** Index type storing min/max values per 128-page block range. Effective only when column values are physically ordered on disk (high correlation). Orders of magnitude smaller than B-tree.

**Correlation (pg_stats):** Statistical measure of alignment between column value order and physical row order. Values near 1.0 or -1.0 indicate physical ordering; values near 0.0 indicate random distribution. Critical for BRIN effectiveness.

**Expression index:** An index on the result of a function or expression rather than a raw column value. Example: `CREATE INDEX ON users(LOWER(email))`. Required when queries apply functions to the indexed column.

**fastupdate:** GIN index option (default on) that batches write operations into a pending list, deferring expensive GIN tree insertions to flush time (during VACUUM). Reduces write latency but can cause lock spikes during flush.

**GIN (Generalized Inverted Index):** Index type mapping each element value to a posting list of TIDs containing that element. Ideal for multi-valued types: arrays, JSONB, tsvector.

**GiST (Generalized Search Tree):** Extensible index framework supporting custom bounding-box operators. Used for geometric types, range types, and full-text prefix search.

**Index bloat:** Accumulation of dead index entries from deleted/updated rows. Reclaimed by VACUUM but can fragment the index. Fixed by `REINDEX INDEX CONCURRENTLY`.

**Index-only scan:** Satisfies a query entirely from index leaf pages, without reading the heap. Requires all columns in the query to be in the index (or INCLUDE list) and visibility map bits set.

**Partial index:** An index with a WHERE clause that indexes only the rows satisfying the condition. Smaller, faster to update, and more selective than a full index.

**Posting list (GIN):** Sorted list of TIDs for all rows containing a given element value. The core data structure of GIN.

**Trigram:** A sequence of 3 consecutive characters in a text string. The pg_trgm extension uses trigrams to build GIN/GiST indexes supporting `LIKE '%substring%'` queries.

---

## 20. Self-Assessment

1. A query has `WHERE payload @> '{"type": "error"}'` on a JSONB column. What index type do you create? Write the DDL.
2. You have a 10-billion-row time-series table where `event_time` is always set to `NOW()` at insert. What index type do you recommend for `WHERE event_time > NOW() - INTERVAL '7 days'`? What must you verify before creating it?
3. An engineer creates a B-tree index on a `status` column with 3 distinct values on a 50M-row table. The index is never used by the planner. Why, and what is the correct fix?
4. In `index_advisor.py`, trace the recommendation for a `TIMESTAMP` column with `correlation = 0.98` and `QueryPattern.RANGE`. Why is BRIN chosen? What would happen if `correlation = 0.3`?
5. A `CREATE INDEX` command has been running for 2 hours and the table is actively receiving inserts. What is happening to writes? How do you avoid this next time?
6. What is the "leading column rule" for composite indexes? Give an example of a composite index on `(a, b, c)` and describe which query predicates can and cannot use it.
7. What does `REINDEX INDEX CONCURRENTLY` do and when do you use it? What Postgres version introduced it?
8. Describe the trade-off between GIN with `fastupdate=on` vs `fastupdate=off` for a high-write table. How does the pending list mechanism work?

---

## 21. Module Summary

Postgres offers five index types because access patterns are fundamentally different: B-tree for ordered scalar queries, GIN for element-level inverted lookups in multi-valued types, GiST for geometric and range overlap, BRIN for naturally ordered time-series, and Hash for equality on large keys (rarely needed). Matching the index type to the query operator family is not optional — a B-tree cannot express `@>` on an array, just as GIN cannot support `ORDER BY`. Using the wrong type results in a sequential scan, not a slower index scan.

The simulation — `IndexAdvisor`, `BRINEffectivenessCalc`, `PartialIndexCalculator`, `IndexSizeEstimator`, `IndexUsageTracker` — makes the decision logic explicit. The advisor maps column data type + query pattern to index type, estimates index size across types, and quantifies write overhead. The BRIN effectiveness calculator shows concretely how correlation determines whether BRIN skips 90% of the table or 5%.

The operational discipline matters as much as the type selection: always use `CONCURRENTLY` to avoid write outages during index creation; monitor `pg_stat_user_indexes` for unused indexes; use `REINDEX INDEX CONCURRENTLY` to address bloat; and watch GIN pending list sizes on high-write tables. An index that is never scanned is pure overhead.

The next module — M55: Vacuuming and Bloat — explains the MVCC transaction ID mechanism that makes dead tuples accumulate, how autovacuum decides when to reclaim them, and what happens when it falls behind — which directly affects index-only scan eligibility via the visibility map and causes the transaction ID wraparound emergency.
