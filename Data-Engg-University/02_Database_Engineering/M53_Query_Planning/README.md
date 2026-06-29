# M53: Query Planning

**Course:** DBE-INT-102 — Postgres Architecture  
**Module:** 02 of 05  
**Global Module ID:** M53  
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

Every SQL query you write goes through a transformation pipeline before a single byte is read from disk. Postgres does not execute SQL directly — it compiles it into a physical execution plan, then runs the plan. The planner's job is to find the cheapest plan out of potentially millions of alternatives. It does this using a cost model, row count estimates, and statistics collected about your data. When it gets the plan wrong, queries that should take 10ms take 10 seconds.

Data engineers encounter bad query plans constantly: a query that runs fast on a 1,000-row development table grinds to a halt on the production 100-million-row table. A join that should use a nested-loop index scan uses a sequential scan instead because the statistics are stale. A query that worked fine for 18 months suddenly slows down after a data distribution shift. `EXPLAIN ANALYZE` is the tool that reveals what went wrong — but reading it requires understanding how the planner works.

This module maps the complete path from SQL text to query result: parse → analyze → rewrite → plan → execute. It explains how the planner estimates row counts, how it assigns costs to operations, how it decides between index scan and sequential scan, and how it chooses between nested-loop, hash, and merge join algorithms. It explains `pg_stat_statements` — the view that tells you which queries are slow in production without you having to be watching. And it explains the planner's failure modes: stale statistics, poor histogram resolution, correlation misconceptions, and the N+1 problem.

---

## 2. Mental Model

### SQL Is a Specification, Not a Program

When you write `SELECT * FROM orders WHERE customer_id = 42 AND status = 'pending'`, you are specifying *what* you want, not *how* to get it. Postgres must decide:
- Should it scan the entire `orders` table sequentially, or use an index on `customer_id`?
- If there are indexes on both `customer_id` and `status`, should it use one, both, or neither?
- Should it filter for `customer_id = 42` first, then `status = 'pending'`, or vice versa?
- If this query joins to another table, should it fetch the matching orders first and then join, or scan the other table and probe into orders?

The planner explores this decision tree and assigns a **cost** (a unit-less number in page I/Os) to each alternative. It picks the plan with the lowest cost.

### The Planner Is a Compiler, Not an Interpreter

An interpreter executes SQL one statement at a time, making local decisions. A compiler — which is what the Postgres planner is — translates the entire statement into an optimized physical execution plan before executing anything. This is why `PREPARE` can speed up repeated queries: the parse + plan phase runs once, and the execution phase runs many times.

### Statistics Drive the Cost Model

The planner does not know your data. It estimates row counts by reading pre-computed statistics from `pg_statistic` (accessible via `pg_stats`). These statistics are computed by `ANALYZE` and include: the number of rows in the table (`n_live_tup`), the most common values in each column, the column's histogram (distribution of values), and the null fraction. If these statistics are stale or inaccurate, the planner makes wrong row count estimates, which cause it to choose wrong plans.

---

## 3. Core Concepts

### 3.1 The Four-Stage Pipeline

**Stage 1: Parse**
The SQL text is tokenized and parsed into a parse tree. The parser checks SQL syntax — missing keywords, unbalanced parentheses, malformed expressions. It does not check whether tables or columns exist. Output: a raw parse tree (abstract syntax tree, or AST).

```
Input: "SELECT name FROM users WHERE id = $1"
Output: ParseTree {
    SelectStmt {
        targetList: [ColumnRef("name")],
        fromClause: [RangeVar("users")],
        whereClause: OpExpr("=", ColumnRef("id"), ParamRef(1))
    }
}
```

**Stage 2: Analyze (Semantic Analysis)**
The query analyzer resolves names against the system catalogs: it checks whether `users` exists, whether `name` and `id` are columns in `users`, whether the data types are compatible for comparison, and whether the current user has SELECT privilege on `users`. Output: a query tree with all names resolved to OIDs (object identifiers).

**Stage 3: Rewrite**
The rule system applies view definitions and row-level security policies. If `users` is actually a view, the rewriter substitutes the view's definition into the query. This is why querying a view is equivalent to querying the underlying tables — the view is expanded before planning, not at execution time.

**Stage 4: Plan**
The planner takes the query tree and produces a physical execution plan. This is the most complex stage. The planner:
1. Generates all possible plans (or a subset for complex queries with many joins)
2. Estimates the cost of each plan using statistics from `pg_statistic`
3. Returns the plan with the lowest estimated total cost

**Stage 5: Execute**
The executor runs the physical plan, fetching pages from the buffer pool, evaluating predicates, performing joins, aggregations, and sorts. Results are sent back to the backend process, which forwards them to the client.

### 3.2 Cost Model

The planner assigns costs in units of "sequential page reads." Tunable via GUC parameters:

| GUC Parameter | Default | Meaning |
|---|---|---|
| `seq_page_cost` | 1.0 | Cost of reading one page sequentially |
| `random_page_cost` | 4.0 | Cost of reading one page randomly (disk seek + read) |
| `cpu_tuple_cost` | 0.01 | Cost of processing one row |
| `cpu_index_tuple_cost` | 0.005 | Cost of processing one index entry |
| `cpu_operator_cost` | 0.0025 | Cost of evaluating one WHERE clause operator |
| `parallel_tuple_cost` | 0.1 | Cost of passing one row to a parallel worker |
| `parallel_setup_cost` | 1000.0 | Overhead of starting a parallel worker |

**Key insight:** `random_page_cost = 4.0` assumes a spinning disk where random I/O is ~4× slower than sequential I/O. For SSDs, `random_page_cost = 1.1` to 1.5 is more accurate. Getting this wrong causes the planner to over-prefer sequential scans over index scans.

**Plan cost formula for a sequential scan:**

```
cost = seq_page_cost × n_pages + cpu_tuple_cost × n_rows
     = 1.0 × 10000 + 0.01 × 1000000
     = 10000 + 10000
     = 20000 cost units
```

**Plan cost formula for an index scan:**

```
cost = index_cost + fetch_cost
index_cost = cpu_index_tuple_cost × n_index_entries_scanned
fetch_cost = random_page_cost × n_pages_fetched

For selectivity 0.001 (0.1% of rows):
  index_entries_scanned = 0.001 × 1,000,000 = 1,000
  pages_fetched = 0.001 × 10,000 = 10
  index_cost = 0.005 × 1,000 = 5
  fetch_cost = 4.0 × 10 = 40
  total = 45 cost units  → index scan wins
```

### 3.3 Row Count Estimation

The planner estimates how many rows will pass each filter using selectivity estimates:

**Equality on a column:** `WHERE status = 'pending'`
- If `status` appears in the most-common-values (MCV) list in `pg_stats`, use the stored frequency: `selectivity = mcv_frequency['pending']`
- If not in MCV, use the histogram's "other values" probability

**Range predicates:** `WHERE created_at > '2024-01-01'`
- The planner uses the histogram stored in `pg_stats.histogram_bounds`
- Estimates what fraction of the histogram buckets fall above the bound

**Multiple predicates (AND):** Assumes statistical independence — `selectivity = sel_A × sel_B`
- This is the **most common source of bad estimates**: if `status = 'pending'` is correlated with `created_at > '2024-01-01'` (pending orders are always recent), the true selectivity is much higher than the product of individual selectivities. Extended statistics (`CREATE STATISTICS`) can capture correlations.

### 3.4 Scan Types

**Sequential Scan (Seq Scan):**
Reads every page in the table linearly from page 0 to page N. Efficient when a large fraction of the table is needed (> ~5-10% of rows). Not the "stupid" option — for full table reads, it is faster than random I/O.

**Index Scan:**
Traverses the B-tree index to find matching TIDs (tuple identifiers: page + offset), then fetches each heap page by TID. Efficient when few rows match (< ~5-10%). Each heap page fetch is a random I/O. `random_page_cost` controls how expensive this looks to the planner.

**Index-Only Scan:**
Like an index scan, but returns column values directly from the index leaf pages without fetching the heap. Only possible when all queried columns are in the index. Must check the visibility map (per-page flag indicating all tuples are visible to all transactions) — if the visibility map bit is not set for a page, the executor must fetch the heap page anyway to check MVCC visibility. Stale visibility maps cause index-only scans to degrade into effective index scans.

**Bitmap Index Scan + Bitmap Heap Scan:**
Two-phase approach: (1) traverse the index and build a bitmap of which heap pages contain matching tuples; (2) sort the page IDs and fetch them in sequential order. Converts random I/O from an index scan into near-sequential I/O. Used when selectivity is moderate (too many rows for a pure index scan, but too few for a seq scan to be optimal). Also used when combining multiple indexes (bitmap AND/OR of two bitmaps = intersection/union without fetching pages twice).

### 3.5 Join Algorithms

**Nested-Loop Join:**
For each row in the outer table, scan the inner table for matching rows.

```
for outer_row in outer_table:
    for inner_row in inner_table:
        if join_condition(outer_row, inner_row):
            emit(outer_row, inner_row)
```

Cost: O(N_outer × N_inner). Optimal when the inner table is small, or when the inner join can be satisfied by an index lookup (making the inner loop O(log N) instead of O(N_inner)). The planner prefers nested-loop for small tables or when one side has a highly selective index.

**Hash Join:**
Build a hash table from the smaller ("inner") table, then probe it with each row from the larger ("outer") table.

```
hash_table = {}
for inner_row in inner_table:
    hash_table[hash(inner_row.join_key)].append(inner_row)

for outer_row in outer_table:
    matching = hash_table.get(hash(outer_row.join_key), [])
    for inner_row in matching:
        if join_condition(outer_row, inner_row):
            emit(outer_row, inner_row)
```

Cost: O(N_inner + N_outer) to build and probe. Optimal for larger tables where nested-loop would be O(N²). Requires enough `work_mem` to hold the hash table. If the hash table spills to disk ("hash batches > 1"), performance degrades significantly. The planner prefers hash joins when both input sides are large and unsorted.

**Merge Join:**
Both inputs must be sorted on the join key. Merge them in one pass like merge sort.

```
i, j = 0, 0
while i < len(left) and j < len(right):
    if left[i].key == right[j].key:
        emit all matches
    elif left[i].key < right[j].key:
        i += 1
    else:
        j += 1
```

Cost: O(N log N) for sorting + O(N_outer + N_inner) for the merge pass. Optimal when inputs are already sorted (from an index or a prior sort step) or when the join output needs to be sorted. The planner prefers merge joins when both inputs have useful indexes on the join key, or when the plan already requires a sort.

### 3.6 EXPLAIN and EXPLAIN ANALYZE

`EXPLAIN` shows the plan the planner chose, with estimated costs and row counts. `EXPLAIN ANALYZE` actually runs the query and shows actual rows and actual time alongside the estimates.

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT o.id, c.name
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.status = 'pending'
  AND o.created_at > '2024-01-01';
```

Reading the output (rightmost/innermost nodes execute first):

```
Hash Join  (cost=1234.56..5678.90 rows=1000 width=24) (actual time=45.2..312.8 rows=847 loops=1)
  Hash Cond: (o.customer_id = c.id)
  Buffers: shared hit=2345 read=456
  ->  Seq Scan on orders o  (cost=0.00..3456.78 rows=1000 width=16) (actual time=0.1..201.3 rows=847 loops=1)
        Filter: ((status = 'pending') AND (created_at > '2024-01-01'))
        Rows Removed by Filter: 99153
        Buffers: shared hit=1234 read=456
  ->  Hash  (cost=890.12..890.12 rows=12345 width=12) (actual time=44.1..44.1 rows=12345 loops=1)
        Buckets: 16384  Batches: 1  Memory Usage: 768kB
        Buffers: shared hit=1111
        ->  Seq Scan on customers c  (cost=0.00..890.12 rows=12345 width=12) (actual time=0.1..22.3 rows=12345 loops=1)
              Buffers: shared hit=1111

Planning Time: 1.234 ms
Execution Time: 313.1 ms
```

Key diagnostic signals:
- `rows=1000` (estimate) vs `rows=847` (actual) → good estimate, plan is valid
- `Rows Removed by Filter: 99153` → sequential scan read 100,000 rows to return 847 → could benefit from an index on `status` or `created_at`
- `Buffers: shared read=456` → 456 pages read from disk (not in buffer pool) → cold cache or large table
- `Batches: 1` → hash table fit in memory → good
- `actual time=0.1..201.3` → this node took 201ms → the bottleneck is here

### 3.7 pg_stat_statements

`pg_stat_statements` is a Postgres extension that tracks all queries executed, normalized (with literal values replaced by `$1`, `$2`, etc.), and aggregates statistics per query shape:

```sql
-- Install once
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Top 10 slowest queries by total time
SELECT
    left(query, 80)                    AS query,
    calls,
    round(total_exec_time::numeric, 2) AS total_ms,
    round(mean_exec_time::numeric, 2)  AS mean_ms,
    round(stddev_exec_time::numeric, 2) AS stddev_ms,
    rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;

-- Queries with highest per-call time (likely full table scans or missing indexes)
SELECT
    left(query, 80) AS query,
    calls,
    round(mean_exec_time::numeric, 2) AS mean_ms,
    round(max_exec_time::numeric, 2)  AS max_ms
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;

-- Queries with highest total I/O (buffer reads)
SELECT
    left(query, 80) AS query,
    calls,
    shared_blks_read,
    shared_blks_hit,
    round(shared_blks_read::numeric / nullif(calls, 0), 0) AS avg_reads_per_call
FROM pg_stat_statements
ORDER BY shared_blks_read DESC
LIMIT 10;

-- Reset statistics (do this after tuning to measure improvement)
SELECT pg_stat_statements_reset();
```

---

## 4. Hands-On Walkthrough

### 4.1 Reading an Explain Plan

```sql
-- Create a sample scenario
CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    status      TEXT NOT NULL,
    amount      NUMERIC(10,2),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_orders_customer ON orders(customer_id);
-- Note: no index on status or created_at initially

-- Check planner estimates vs actuals
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM orders WHERE customer_id = 42;

-- ─ With small table (< 1000 rows): planner may choose Seq Scan despite index
--   because random_page_cost × 1 > seq scan cost of 1-2 pages

-- ─ With large table (> 10000 rows) and selective predicate:
--   Index Scan on idx_orders_customer

-- Check for correlation issue
EXPLAIN (ANALYZE)
SELECT * FROM orders
WHERE status = 'pending' AND created_at > '2024-01-01';
-- May show: estimated rows=1000, actual rows=50000
-- Cause: planner assumed independence; in reality, all pending orders are recent

-- Fix: create statistics capturing correlation
CREATE STATISTICS orders_status_date (dependencies) ON status, created_at FROM orders;
ANALYZE orders;
-- Re-run EXPLAIN: estimate should improve
```

### 4.2 Forcing Plan Choices (for Testing)

```sql
-- Disable sequential scans (force index use) — NEVER do this in production
SET enable_seqscan = off;
EXPLAIN SELECT * FROM orders WHERE customer_id = 42;
-- Will now use Index Scan even if planner thinks seq scan is cheaper
SET enable_seqscan = on;

-- Disable hash joins
SET enable_hashjoin = off;
EXPLAIN SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id;
-- Will use Merge Join or Nested Loop instead
SET enable_hashjoin = on;

-- Force parallel query (for testing)
SET max_parallel_workers_per_gather = 4;
SET parallel_tuple_cost = 0;
SET parallel_setup_cost = 0;
EXPLAIN SELECT count(*), status FROM orders GROUP BY status;
SET max_parallel_workers_per_gather = 2;

-- Reset all settings
RESET ALL;
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
query_planner.py

Simulate key aspects of the Postgres query planner in Python.

Components:
  - Statistics: pg_stats-like row count estimates and histogram
  - CostModel: translates row/page counts to planner cost units
  - ScanPlan / IndexScanPlan / BitmapScanPlan: physical scan operators
  - NestedLoopJoin / HashJoin / MergeJoin: physical join operators
  - Planner: selects the lowest-cost plan from alternatives
  - ExplainPrinter: renders EXPLAIN-style output
  - PgStatStatements: tracks query execution statistics

This is a cost-model simulation, not a query executor.
The "execution" phase returns canned row counts to demonstrate
how actual vs. estimated rows diverges.

No external dependencies.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Optional


# ─── Statistics (pg_stats equivalent) ────────────────────────────────────────

@dataclass
class ColumnStats:
    """
    Simulates a row in pg_stats: per-column statistics used by the planner.
    In production: populated by ANALYZE, stored in pg_statistic system catalog.
    """
    column_name:    str
    null_frac:      float = 0.0           # Fraction of NULLs (0.0 → 1.0)
    n_distinct:     float = -1.0          # Negative = fraction of total rows
    most_common_vals:  list = field(default_factory=list)
    most_common_freqs: list = field(default_factory=list)
    histogram_bounds:  list = field(default_factory=list)  # Boundary values
    correlation:       float = 0.0        # Physical order correlation (-1 → 1)


@dataclass
class TableStats:
    """
    Simulates pg_class + pg_stat_user_tables: per-table statistics.
    """
    table_name:  str
    n_rows:      int       # n_live_tup (estimated live row count)
    n_pages:     int       # relpages (number of 8KB pages)
    columns:     dict[str, ColumnStats] = field(default_factory=dict)


class Statistics:
    """
    Repository of table and column statistics.
    Simulates the pg_statistic catalog and the planner's stats access layer.
    """

    def __init__(self):
        self._tables: dict[str, TableStats] = {}

    def add_table(self, stats: TableStats):
        self._tables[stats.table_name] = stats

    def table(self, name: str) -> TableStats:
        return self._tables[name]

    def selectivity(self, table: str, column: str, op: str, value) -> float:
        """
        Estimate the fraction of rows satisfying col op value.
        Mirrors the planner's restriction_selectivity() family.
        """
        ts = self._tables.get(table)
        if ts is None:
            return 0.01  # Unknown table: assume 1%

        col = ts.columns.get(column)
        if col is None:
            return 0.01  # Unknown column: assume 1%

        if op == '=':
            # Check most-common-values list first
            for val, freq in zip(col.most_common_vals, col.most_common_freqs):
                if val == value:
                    return freq
            # Not in MCV: use (1 - sum(mcv_freqs) - null_frac) / (n_distinct - len(mcv))
            mcv_freq_total = sum(col.most_common_freqs)
            other_frac     = 1.0 - mcv_freq_total - col.null_frac
            n_other        = max(1, (abs(col.n_distinct) * ts.n_rows) - len(col.most_common_vals))
            return other_frac / max(1, n_other / ts.n_rows)

        elif op == '>':
            if not col.histogram_bounds:
                return 0.3   # No histogram: guess 30%
            bounds = col.histogram_bounds
            # Find what fraction of buckets are above `value`
            for i, bound in enumerate(bounds):
                if value <= bound:
                    return 1.0 - (i / len(bounds))
            return 0.0

        elif op == '<':
            return 1.0 - self.selectivity(table, column, '>', value)

        elif op == 'BETWEEN':
            lo, hi = value
            sel_lo = self.selectivity(table, column, '>', lo)
            sel_hi = self.selectivity(table, column, '<', hi)
            return max(0.0, sel_lo + sel_hi - 1.0)

        return 0.1  # Unknown operator: guess 10%

    def estimate_rows(self, table: str, predicates: list[tuple]) -> int:
        """
        Estimate output rows after applying all predicates.
        Assumes independence between predicates (standard planner behavior).
        Combined selectivity = product of individual selectivities.
        This is the key source of estimation error when columns are correlated.
        """
        ts = self._tables.get(table)
        if ts is None:
            return 1

        combined_sel = 1.0
        for (col, op, val) in predicates:
            combined_sel *= self.selectivity(table, col, op, val)

        return max(1, int(ts.n_rows * combined_sel))


# ─── Cost Model ───────────────────────────────────────────────────────────────

@dataclass
class CostConfig:
    """GUC parameters controlling the cost model."""
    seq_page_cost:         float = 1.0
    random_page_cost:      float = 4.0
    cpu_tuple_cost:        float = 0.01
    cpu_index_tuple_cost:  float = 0.005
    cpu_operator_cost:     float = 0.0025
    parallel_tuple_cost:   float = 0.1
    parallel_setup_cost:   float = 1000.0


class CostModel:
    """
    Translates physical operations into planner cost units.
    Mirrors PostgreSQL's costsize.c functions.
    """

    def __init__(self, config: CostConfig = None):
        self.cfg = config or CostConfig()

    def seqscan_cost(self, n_pages: int, n_rows: int, n_output_rows: int) -> tuple[float, float]:
        """(startup_cost, total_cost) for a sequential scan."""
        startup = 0.0
        run = (self.cfg.seq_page_cost * n_pages +
               self.cfg.cpu_tuple_cost * n_rows +
               self.cfg.cpu_operator_cost * n_rows)
        return startup, run

    def indexscan_cost(self, n_index_tuples: int, n_heap_pages: int,
                       n_output_rows: int) -> tuple[float, float]:
        """(startup_cost, total_cost) for an index scan."""
        startup = 0.1  # Root + first few index pages
        index_cpu = self.cfg.cpu_index_tuple_cost * n_index_tuples
        heap_io   = self.cfg.random_page_cost * n_heap_pages
        tuple_cpu = self.cfg.cpu_tuple_cost * n_output_rows
        return startup, startup + index_cpu + heap_io + tuple_cpu

    def bitmap_scan_cost(self, n_index_tuples: int, n_heap_pages: int,
                         n_output_rows: int) -> tuple[float, float]:
        """
        Bitmap index scan + bitmap heap scan.
        Heap pages are accessed in sorted order (near-sequential).
        """
        startup = self.cfg.cpu_index_tuple_cost * n_index_tuples  # Build bitmap
        heap_io = self.cfg.seq_page_cost * n_heap_pages * 1.1     # ~sequential
        tuple_cpu = self.cfg.cpu_tuple_cost * n_output_rows
        return startup, startup + heap_io + tuple_cpu

    def nestloop_cost(self, outer_rows: int, inner_startup: float,
                      inner_total: float) -> tuple[float, float]:
        """Nested-loop join: outer_rows iterations of the inner plan."""
        startup = inner_startup
        run     = outer_rows * inner_total
        return startup, startup + run

    def hashjoin_cost(self, outer_rows: int, inner_rows: int,
                      inner_cost: float, outer_cost: float,
                      work_mem_rows: int) -> tuple[float, float]:
        """Hash join: build phase + probe phase."""
        build_startup = inner_cost
        probe_run     = outer_cost + self.cfg.cpu_tuple_cost * outer_rows
        # Spill penalty if inner doesn't fit in work_mem
        batches = math.ceil(inner_rows / max(1, work_mem_rows))
        spill_penalty = 0.0 if batches == 1 else math.log2(batches) * inner_rows * 0.01
        return build_startup, build_startup + probe_run + spill_penalty

    def mergejoin_cost(self, outer_rows: int, outer_sorted: bool,
                       inner_rows: int, inner_sorted: bool,
                       outer_cost: float, inner_cost: float) -> tuple[float, float]:
        """Merge join: sort costs + merge pass."""
        sort_outer = 0.0 if outer_sorted else outer_rows * math.log2(max(2, outer_rows)) * 0.01
        sort_inner = 0.0 if inner_sorted else inner_rows * math.log2(max(2, inner_rows)) * 0.01
        merge_run  = self.cfg.cpu_tuple_cost * (outer_rows + inner_rows)
        startup    = outer_cost + inner_cost + sort_outer + sort_inner
        return startup, startup + merge_run


# ─── Physical Plan Nodes ──────────────────────────────────────────────────────

@dataclass
class PlanNode:
    """Base class for all plan nodes."""
    node_type:      str
    startup_cost:   float
    total_cost:     float
    est_rows:       int
    width:          int = 8        # Average row width in bytes
    children:       list['PlanNode'] = field(default_factory=list)
    # Populated by EXPLAIN ANALYZE simulation
    actual_rows:    Optional[int] = None
    actual_time_ms: Optional[float] = None


@dataclass
class SeqScanNode(PlanNode):
    table:      str = ''
    predicates: list = field(default_factory=list)
    rows_removed: int = 0


@dataclass
class IndexScanNode(PlanNode):
    table:      str = ''
    index:      str = ''
    predicates: list = field(default_factory=list)


@dataclass
class BitmapScanNode(PlanNode):
    table:      str = ''
    index:      str = ''
    predicates: list = field(default_factory=list)


@dataclass
class HashJoinNode(PlanNode):
    join_cond:   str = ''
    hash_batches: int = 1
    hash_mem_kb:  int = 0


@dataclass
class NestedLoopNode(PlanNode):
    join_cond:   str = ''


@dataclass
class MergeJoinNode(PlanNode):
    join_cond:   str = ''


# ─── Planner ──────────────────────────────────────────────────────────────────

class Planner:
    """
    Simplified Postgres query planner.
    Given a query description, generates candidate plans and picks the cheapest.
    """

    def __init__(self, stats: Statistics, cost_config: CostConfig = None):
        self.stats      = stats
        self.cost_model = CostModel(cost_config or CostConfig())

    def plan_scan(self, table: str, predicates: list[tuple],
                  available_indexes: dict[str, str]) -> PlanNode:
        """
        Choose between sequential scan, index scan, and bitmap scan.
        available_indexes: {column_name: index_name}
        """
        ts        = self.stats.table(table)
        est_rows  = self.stats.estimate_rows(table, predicates)
        selectivity = est_rows / max(1, ts.n_rows)

        # Sequential scan cost
        ss_start, ss_total = self.cost_model.seqscan_cost(
            ts.n_pages, ts.n_rows, est_rows)
        best_node = SeqScanNode(
            node_type='Seq Scan',
            startup_cost=ss_start, total_cost=ss_total,
            est_rows=est_rows, table=table, predicates=predicates,
            rows_removed=ts.n_rows - est_rows)

        # Index scan candidates
        for (col, op, val) in predicates:
            if col in available_indexes and op == '=':
                index_name   = available_indexes[col]
                n_idx_tuples = est_rows
                n_heap_pages = max(1, int(ts.n_pages * selectivity))
                is_start, is_total = self.cost_model.indexscan_cost(
                    n_idx_tuples, n_heap_pages, est_rows)
                if is_total < best_node.total_cost:
                    best_node = IndexScanNode(
                        node_type='Index Scan',
                        startup_cost=is_start, total_cost=is_total,
                        est_rows=est_rows, table=table,
                        index=index_name, predicates=predicates)

                # Bitmap scan (better for moderate selectivity)
                bm_start, bm_total = self.cost_model.bitmap_scan_cost(
                    n_idx_tuples, n_heap_pages, est_rows)
                if bm_total < best_node.total_cost:
                    best_node = BitmapScanNode(
                        node_type='Bitmap Index/Heap Scan',
                        startup_cost=bm_start, total_cost=bm_total,
                        est_rows=est_rows, table=table,
                        index=index_name, predicates=predicates)

        return best_node

    def plan_join(self, left: PlanNode, right: PlanNode,
                  join_cond: str, work_mem_rows: int = 100_000) -> PlanNode:
        """
        Choose between nested loop, hash join, and merge join.
        """
        outer_rows = left.est_rows
        inner_rows = right.est_rows
        output_rows = max(1, int(math.sqrt(outer_rows * inner_rows) * 0.1))

        # Nested loop
        nl_start, nl_total = self.cost_model.nestloop_cost(
            outer_rows, right.startup_cost, right.total_cost)
        best_node = NestedLoopNode(
            node_type='Nested Loop',
            startup_cost=nl_start, total_cost=nl_total,
            est_rows=output_rows, join_cond=join_cond,
            children=[left, right])

        # Hash join
        hj_start, hj_total = self.cost_model.hashjoin_cost(
            outer_rows, inner_rows,
            right.total_cost, left.total_cost,
            work_mem_rows)
        # Choose inner = smaller table
        inner_is_right = inner_rows <= outer_rows
        if hj_total < best_node.total_cost:
            best_node = HashJoinNode(
                node_type='Hash Join',
                startup_cost=hj_start, total_cost=hj_total,
                est_rows=output_rows, join_cond=join_cond,
                hash_batches=max(1, math.ceil(inner_rows / work_mem_rows)),
                hash_mem_kb=int(inner_rows * 20 / 1024),
                children=[left if inner_is_right else right,
                          right if inner_is_right else left])

        # Merge join (assume unsorted inputs)
        mj_start, mj_total = self.cost_model.mergejoin_cost(
            outer_rows, False, inner_rows, False,
            left.total_cost, right.total_cost)
        if mj_total < best_node.total_cost:
            best_node = MergeJoinNode(
                node_type='Merge Join',
                startup_cost=mj_start, total_cost=mj_total,
                est_rows=output_rows, join_cond=join_cond,
                children=[left, right])

        return best_node


# ─── EXPLAIN Printer ──────────────────────────────────────────────────────────

class ExplainPrinter:
    """
    Renders a plan tree in EXPLAIN-style text output.
    Optionally includes actual rows/time for EXPLAIN ANALYZE simulation.
    """

    def print_plan(self, node: PlanNode, indent: int = 0, analyze: bool = True):
        prefix = '  ' * indent + '-> ' if indent > 0 else ''
        cost_str  = f"(cost={node.startup_cost:.2f}..{node.total_cost:.2f} rows={node.est_rows})"
        actual_str = ''
        if analyze and node.actual_rows is not None:
            actual_str = f" (actual rows={node.actual_rows} time={node.actual_time_ms:.1f}ms)"
        print(f"{prefix}{node.node_type}  {cost_str}{actual_str}")

        # Node-specific details
        if isinstance(node, (SeqScanNode, IndexScanNode, BitmapScanNode)):
            table_str = f"{'  ' * indent}   on {node.table}"
            if hasattr(node, 'index') and node.index:
                table_str += f" using {node.index}"
            print(table_str)
            if isinstance(node, SeqScanNode) and node.rows_removed > 0:
                print(f"{'  ' * indent}   Rows Removed by Filter: {node.rows_removed:,}")
        elif isinstance(node, HashJoinNode):
            print(f"{'  ' * indent}   Hash Cond: {node.join_cond}")
            if node.hash_batches > 1:
                print(f"{'  ' * indent}   Batches: {node.hash_batches}  "
                      f"Memory Usage: {node.hash_mem_kb}kB")

        for child in node.children:
            self.print_plan(child, indent + 1, analyze)


# ─── pg_stat_statements simulation ───────────────────────────────────────────

class PgStatStatements:
    """
    Tracks normalized query execution statistics.
    Simulates the pg_stat_statements extension.
    """

    def __init__(self):
        self._stats: dict[str, dict] = {}

    def record(self, query_template: str, exec_time_ms: float, rows: int,
               blks_read: int, blks_hit: int):
        """Record one execution of a query."""
        if query_template not in self._stats:
            self._stats[query_template] = {
                'calls': 0, 'total_exec_time': 0.0, 'max_exec_time': 0.0,
                'rows': 0, 'shared_blks_read': 0, 'shared_blks_hit': 0
            }
        s = self._stats[query_template]
        s['calls']           += 1
        s['total_exec_time'] += exec_time_ms
        s['max_exec_time']    = max(s['max_exec_time'], exec_time_ms)
        s['rows']            += rows
        s['shared_blks_read'] += blks_read
        s['shared_blks_hit']  += blks_hit

    def top_by_total_time(self, n: int = 5) -> list[dict]:
        return sorted(
            [{'query': q, **s} for q, s in self._stats.items()],
            key=lambda x: x['total_exec_time'], reverse=True
        )[:n]

    def print_report(self):
        print(f"\n  {'Query':<50} {'Calls':>6} {'Total ms':>10} {'Mean ms':>9} {'Reads':>8}")
        print(f"  {'-'*50} {'-'*6} {'-'*10} {'-'*9} {'-'*8}")
        for row in self.top_by_total_time(10):
            mean = row['total_exec_time'] / max(1, row['calls'])
            print(f"  {row['query'][:50]:<50} {row['calls']:>6,} "
                  f"{row['total_exec_time']:>10.1f} {mean:>9.2f} "
                  f"{row['shared_blks_read']:>8,}")


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Query Planner Simulation ===\n")

    # ── Phase 1: Build Statistics ─────────────────────────────────────────────
    print("─── Phase 1: Table Statistics ───")
    stats = Statistics()

    orders_stats = TableStats(
        table_name='orders',
        n_rows=1_000_000,
        n_pages=10_000,
        columns={
            'customer_id': ColumnStats(
                column_name='customer_id',
                n_distinct=50_000,
                histogram_bounds=list(range(0, 50_001, 1000)),
            ),
            'status': ColumnStats(
                column_name='status',
                n_distinct=5,
                most_common_vals=['completed', 'pending', 'cancelled', 'shipped', 'refunded'],
                most_common_freqs=[0.60, 0.15, 0.10, 0.10, 0.05],
            ),
            'created_at': ColumnStats(
                column_name='created_at',
                n_distinct=-0.999,  # Almost all distinct
                histogram_bounds=['2020-01-01', '2021-01-01', '2022-01-01',
                                  '2023-01-01', '2024-01-01', '2025-01-01'],
                correlation=0.95,   # High physical order correlation → index-only scan friendly
            ),
        }
    )

    customers_stats = TableStats(
        table_name='customers',
        n_rows=50_000,
        n_pages=500,
        columns={
            'id': ColumnStats(column_name='id', n_distinct=-1.0),
            'name': ColumnStats(column_name='name', n_distinct=-0.999),
        }
    )

    stats.add_table(orders_stats)
    stats.add_table(customers_stats)
    print(f"  orders: {orders_stats.n_rows:,} rows, {orders_stats.n_pages:,} pages")
    print(f"  customers: {customers_stats.n_rows:,} rows, {customers_stats.n_pages:,} pages")

    # ── Phase 2: Selectivity Estimates ────────────────────────────────────────
    print("\n─── Phase 2: Selectivity Estimates ───")
    for (col, op, val) in [
        ('status', '=', 'pending'),
        ('status', '=', 'completed'),
        ('created_at', '>', '2024-01-01'),
        ('customer_id', '=', 42),
    ]:
        sel = stats.selectivity('orders', col, op, val)
        est = stats.estimate_rows('orders', [(col, op, val)])
        print(f"  orders.{col} {op} {repr(val)!s:<20}: "
              f"selectivity={sel:.4f}, est_rows={est:,}")

    # Correlated predicates (independence assumption error)
    est_combined = stats.estimate_rows('orders', [
        ('status', '=', 'pending'),
        ('created_at', '>', '2024-01-01')
    ])
    print(f"\n  Combined (status=pending AND created_at > 2024): est_rows={est_combined:,}")
    print(f"  (Independence assumption: sel_status × sel_created_at = product)")
    print(f"  True rows may be very different if columns are correlated!")

    # ── Phase 3: Scan Plan Selection ──────────────────────────────────────────
    print("\n─── Phase 3: Scan Plan Selection ───")
    planner = Planner(stats)
    available_indexes = {'customer_id': 'idx_orders_customer'}

    print("\n  Query: SELECT * FROM orders WHERE customer_id = 42")
    scan_plan = planner.plan_scan('orders', [('customer_id', '=', 42)], available_indexes)
    ExplainPrinter().print_plan(scan_plan, analyze=False)

    print("\n  Query: SELECT * FROM orders WHERE status = 'pending'")
    scan_plan2 = planner.plan_scan('orders', [('status', '=', 'pending')], available_indexes)
    ExplainPrinter().print_plan(scan_plan2, analyze=False)
    print(f"  (No index on status → Seq Scan; est {scan_plan2.est_rows:,} rows)")

    print("\n  Query: SELECT * FROM orders WHERE status = 'pending' AND created_at > '2024-01-01'")
    scan_plan3 = planner.plan_scan('orders', [
        ('status', '=', 'pending'), ('created_at', '>', '2024-01-01')
    ], available_indexes)
    ExplainPrinter().print_plan(scan_plan3, analyze=False)

    # ── Phase 4: Join Plan Selection ──────────────────────────────────────────
    print("\n─── Phase 4: Join Plan Selection ───")
    print("\n  Query: SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id")

    orders_scan    = planner.plan_scan('orders', [], available_indexes)
    customers_scan = planner.plan_scan('customers', [], {})
    join_plan      = planner.plan_join(
        orders_scan, customers_scan,
        join_cond='o.customer_id = c.id',
        work_mem_rows=100_000
    )
    ExplainPrinter().print_plan(join_plan, analyze=False)

    # Force tiny inner table → nested loop preferred
    print("\n  Join where inner side is tiny (1,000 customers):")
    tiny_customers = TableStats(table_name='customers_tiny', n_rows=1_000, n_pages=10)
    stats.add_table(tiny_customers)
    tiny_scan  = planner.plan_scan('customers_tiny', [], {})
    join_plan2 = planner.plan_join(orders_scan, tiny_scan,
                                    join_cond='o.customer_id = c.id', work_mem_rows=100_000)
    ExplainPrinter().print_plan(join_plan2, analyze=False)

    # ── Phase 5: pg_stat_statements Simulation ────────────────────────────────
    print("\n─── Phase 5: pg_stat_statements ───")
    pg_stat = PgStatStatements()

    import random
    rng = random.Random(42)

    query_patterns = [
        ("SELECT * FROM orders WHERE customer_id = $1", 0.5, 10, 5, 45),
        ("SELECT count(*) FROM orders WHERE status = $1", 150.0, 1, 9500, 500),
        ("SELECT id, name FROM customers WHERE id = $1", 0.1, 1, 0, 2),
        ("INSERT INTO orders VALUES ($1, $2, $3)", 1.2, 1, 0, 0),
        ("UPDATE orders SET status=$1 WHERE id=$2", 2.1, 1, 1, 3),
    ]

    for query, base_ms, rows, blks_read, blks_hit in query_patterns:
        n_calls = rng.randint(100, 5000)
        for _ in range(n_calls):
            jitter = rng.uniform(0.5, 2.0)
            pg_stat.record(query, base_ms * jitter, rows,
                          blks_read, blks_hit)

    print("\n  Top queries by total execution time (pg_stat_statements):")
    pg_stat.print_report()

    # ── Phase 6: Cost Model with SSD Tuning ──────────────────────────────────
    print("\n─── Phase 6: Cost Sensitivity — SSD vs HDD ───")
    for rpc, label in [(4.0, "HDD (random_page_cost=4.0)"),
                       (1.1, "SSD (random_page_cost=1.1)")]:
        cfg = CostConfig(random_page_cost=rpc)
        ssd_planner = Planner(stats, cfg)
        plan = ssd_planner.plan_scan('orders', [('customer_id', '=', 42)], available_indexes)
        print(f"\n  {label}:")
        ExplainPrinter().print_plan(plan, analyze=False)
```

---

## 6. Visual Reference

### Query Lifecycle (SQL → Result)

```
SQL Text
  "SELECT o.id, c.name FROM orders o JOIN customers c ON ..."
         │
         ▼
  ┌──────────────┐
  │   PARSER     │  Tokenize + build parse tree (syntax only)
  └──────┬───────┘
         │ ParseTree
         ▼
  ┌──────────────┐
  │   ANALYZER   │  Resolve names → OIDs, type-check, permission-check
  └──────┬───────┘
         │ Query Tree
         ▼
  ┌──────────────┐
  │   REWRITER   │  Expand views, apply RLS policies
  └──────┬───────┘
         │ Query Tree (views substituted)
         ▼
  ┌──────────────────────────────────────────────────────────┐
  │                      PLANNER                              │
  │                                                           │
  │  Read pg_statistic → estimate row counts                  │
  │  Generate plan alternatives:                              │
  │    Scan: SeqScan vs IndexScan vs BitmapScan               │
  │    Join: NestedLoop vs HashJoin vs MergeJoin              │
  │    Join order: (A ⋈ B) ⋈ C vs A ⋈ (B ⋈ C)              │
  │  Assign costs (cost units = sequential page reads)        │
  │  Pick lowest-cost plan                                    │
  └──────┬───────────────────────────────────────────────────┘
         │ Physical Plan (tree of operators)
         ▼
  ┌──────────────┐
  │   EXECUTOR   │  Drive plan tree; fetch pages from buffer pool
  └──────┬───────┘
         │ Result rows
         ▼
  Client (via backend → TCP)
```

### Scan Decision: When Does the Planner Choose Each Scan Type?

```
Selectivity (fraction of rows returned)
0.001%         0.1%          1%          5%          100%
  │              │             │           │            │
  ●──────────────●─────────────┤           │            │
  Index Scan                   │           │            │
                 ●─────────────●───────────┤            │
                 Bitmap Scan               │            │
                                           ●────────────●
                                           Sequential Scan

Rule of thumb (varies with random_page_cost and data clustering):
  < 1%   → Index Scan preferred (very few heap page fetches)
  1-10%  → Bitmap Scan (sort page IDs, fetch near-sequentially)
  > 10%  → Sequential Scan (reading most pages anyway; seq I/O is cheap)
```

### Join Algorithm Selection

```
                        Inner table size
                   Small (<1K)      Large (>100K)
                 ┌─────────────────────────────────┐
    Outer  Small │  Nested Loop    │  Nested Loop   │
    table  Large │  Nested Loop +  │  Hash Join     │
    size         │  Index on inner │  (or MergeJoin │
                 │                 │   if sorted)   │
                 └─────────────────────────────────┘

Nested Loop  → best when inner is tiny or has a highly selective index
Hash Join    → best for large, unsorted inputs; requires work_mem for hash table
Merge Join   → best when both inputs already sorted on join key (from index)
```

---

## 7. Common Mistakes

**Mistake 1: Running `EXPLAIN` without `ANALYZE` and trusting the estimated rows.** `EXPLAIN` without `ANALYZE` shows what the planner *thinks* it will do, based on statistics. The estimated rows can be orders of magnitude off if statistics are stale. `EXPLAIN ANALYZE` actually executes the query and shows both estimated and actual rows. The gap between them reveals the root cause of most bad plan choices. Always use `EXPLAIN (ANALYZE, BUFFERS)` for diagnosis.

**Mistake 2: Setting `random_page_cost = 4.0` on an SSD server.** This default value was calibrated for spinning disks where random I/O costs ~4× more than sequential I/O. On an NVMe SSD, random I/O costs 1.1 to 1.5× sequential I/O. With `random_page_cost = 4.0` on SSDs, the planner overestimates the cost of index scans and under-uses them, preferring sequential scans even when an index would be faster. Set `random_page_cost = 1.1` for NVMe, `1.5` for SATA SSD.

**Mistake 3: Over-relying on `enable_seqscan = off` or `enable_hashjoin = off` to fix a slow query in production.** These settings disable plan types globally (or per-session). If you disable sequential scans, the planner will use an index scan even when a sequential scan would be faster — trading a bad plan for a different bad plan. The right fix is: update statistics (`ANALYZE`), add the right index, or use extended statistics for correlated columns. Use `enable_*` settings only in local sessions for diagnostic testing.

---

## 8. Production Failure Scenarios

### Scenario 1: Stale Statistics Cause a 100× Slowdown

**Symptoms:** A cron job that runs `DELETE FROM events WHERE created_at < NOW() - INTERVAL '90 days'` runs in 2 seconds every night. After a large data backfill (200M rows inserted over a weekend), the same query takes 3 minutes. `EXPLAIN ANALYZE` shows: `estimated rows=500, actual rows=18000000`. The planner chose a Nested Loop join based on the 500-row estimate; the actual 18M rows make it catastrophic.

**Root cause:** The backfill script did not run `ANALYZE` after inserting data. The planner's statistics still reflect the pre-backfill data distribution. The row count estimate of 500 is based on stale statistics that show a tiny table.

**Fix:**
```sql
-- Immediate: update statistics manually
ANALYZE events;

-- After ANALYZE, the planner re-estimates correctly:
-- estimated rows=18,000,000, actual rows=18,000,000
-- Plan changes from Nested Loop to Hash Join → back to 2 seconds

-- Prevention: add ANALYZE to the backfill pipeline
-- After any bulk INSERT/DELETE/UPDATE, run:
ANALYZE table_name;
-- Or for the entire database:
ANALYZE;

-- Long term: autovacuum should handle this automatically unless
-- the backfill happened too fast for autovacuum to keep up.
-- Check: SELECT n_live_tup, last_autoanalyze FROM pg_stat_user_tables WHERE relname = 'events';
```

### Scenario 2: Hash Join Spills to Disk

**Symptoms:** A complex report query runs in 30 seconds on staging (8GB `work_mem`) but takes 25 minutes in production (4MB `work_mem` default). `EXPLAIN ANALYZE` shows: `Hash (Batches: 128)`.

**Root cause:** The hash table for the join's inner side requires 2GB of memory. With 4MB `work_mem`, the hash join splits into 128 batches — each batch requires reading and writing temp files to disk. 128× more I/O than the single-batch case.

**Fix:**
```sql
-- For this session only: increase work_mem before running the query
SET work_mem = '2GB';
SELECT /* expensive report */ ...;
RESET work_mem;

-- Verify: EXPLAIN ANALYZE should now show Batches: 1
-- and Memory Usage: ~2000MB (the hash table fits)

-- For a reporting role: set work_mem permanently
ALTER ROLE report_user SET work_mem = '1GB';

-- Do NOT set work_mem = 2GB globally:
-- 50 backends × 3 sort/hash nodes × 2GB = 300GB RAM → OOM
```

---

## 9. Performance and Tuning

### Planner Tuning for Data Engineering Workloads

```sql
-- For SSD servers: lower random_page_cost to encourage index usage
ALTER SYSTEM SET random_page_cost = 1.1;   -- NVMe SSD
-- ALTER SYSTEM SET random_page_cost = 1.5; -- SATA SSD

-- Increase statistics target for columns with poor estimates
-- Default: 100 histogram buckets. Increase for skewed distributions.
ALTER TABLE orders ALTER COLUMN status SET STATISTICS 500;
ALTER TABLE events ALTER COLUMN event_type SET STATISTICS 500;
ANALYZE orders, events;

-- Create extended statistics for correlated columns
-- Helps when multiple-column predicates give bad combined estimates
CREATE STATISTICS orders_status_date (dependencies)
    ON status, created_at FROM orders;
CREATE STATISTICS orders_status_customer (ndistinct, dependencies)
    ON status, customer_id FROM orders;
ANALYZE orders;

-- Enable parallel query for analytics workloads
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
ALTER SYSTEM SET max_parallel_workers = 8;
ALTER SYSTEM SET parallel_leader_participation = on;
SELECT pg_reload_conf();

-- Per-session work_mem for analytics queries
SET work_mem = '256MB';
-- Or per-role:
ALTER ROLE analyst SET work_mem = '256MB';

-- Monitor plan regressions with pg_stat_statements
-- Reset after tuning to measure improvement cleanly
SELECT pg_stat_statements_reset();
```

### EXPLAIN Reading Guide

```sql
-- Always use these EXPLAIN options for diagnosis:
EXPLAIN (
    ANALYZE,   -- Actually run the query (shows actual rows and time)
    BUFFERS,   -- Shows shared_blks_hit/read (cache hits vs disk reads)
    FORMAT TEXT -- Human-readable (JSON available for tooling)
)
SELECT ...;

-- Warning signs in EXPLAIN ANALYZE output:
-- 1. est_rows << actual_rows (10x off or more) → stale statistics
-- 2. Seq Scan with Rows Removed by Filter: N where N >> output rows → missing index
-- 3. Hash (Batches: N > 1) → hash table spilling to disk → increase work_mem
-- 4. Sort Method: external merge → sort spilling to disk → increase work_mem
-- 5. actual time >> estimated time → underlying assumption is wrong
-- 6. Nested Loop with large actual_rows → bad join order or bad cardinality estimate
```

---

## 10. Interview Q&A

**Q1: Walk me through what happens when Postgres receives a SQL query, from the text string to the first byte of results.**

SQL arrives as a text string at the backend process. The first step is parsing: the parser tokenizes the SQL and builds a raw parse tree — an abstract syntax tree verifying only syntax, not semantics. At this stage, the parser doesn't know if `users` is a valid table; it just confirms the SQL is grammatically correct.

The analyzer follows, performing semantic analysis against the system catalogs. It resolves every name to an OID: `users` becomes the OID of the `users` table, `id` becomes the OID of that column, and `SELECT` privilege is verified. Type compatibility is checked — you can't compare an integer column to a boolean literal without an explicit cast. Output is a query tree with all names resolved to catalog OIDs.

The rewrite system then applies rules: view definitions are expanded inline (querying a view becomes querying its underlying tables), row-level security policies are injected as additional WHERE clauses, and any rules created with `CREATE RULE` are applied.

The planner receives the rewritten query tree and generates a physical execution plan. It reads statistics from `pg_statistic` to estimate row counts for each operation, assigns costs in units of page reads, generates alternatives for each scan and join, and selects the plan with the lowest total cost. This is where index scan vs sequential scan, and hash join vs nested loop, are decided.

The executor then drives the plan tree bottom-up: leaf nodes fetch pages from the buffer pool, filter predicates are applied, join algorithms execute, aggregations and sorts run, and result rows bubble up to the top of the plan tree. The executor streams results back to the backend process, which forwards them to the client over the PostgreSQL wire protocol.

**Q2: Why might the Postgres planner choose a sequential scan even though an index exists on the WHERE clause column?**

The planner's decision is always based on estimated cost, not on the existence of an index. A sequential scan is cheaper than an index scan when the selectivity — the fraction of rows the predicate returns — is above a threshold, typically around 5-10% of the table.

The cost model compares two things: the sequential scan reads all N_pages pages sequentially at `seq_page_cost` per page; the index scan traverses the B-tree and then fetches each matching row's heap page at `random_page_cost` per page. When selectivity is high — say 20% of a million-row table means 200,000 rows — the index scan needs to fetch 200,000 heap pages randomly, at `random_page_cost = 4.0` each, which far exceeds the sequential scan cost of reading all 10,000 pages at 1.0 each.

There are also cases where the planner should choose an index scan but doesn't. First: stale statistics. If the statistics show 500 rows but the table actually has 5,000,000, the planner thinks a sequential scan is cheap (500 pages) when it's actually expensive. Fix: `ANALYZE`. Second: wrong cost parameters. `random_page_cost = 4.0` on an SSD server makes index scans look 4× more expensive than they are; setting it to 1.1 makes the planner prefer index scans at lower selectivities. Third: the table is small enough that a sequential scan is only 1-2 page reads, cheaper than any index traversal.

---

## 11. Cross-Question Chain

**Interviewer:** You have a query that takes 500ms. `EXPLAIN ANALYZE` shows a sequential scan on a 10M-row table with 9,950,000 rows removed by filter. What do you do?

**Candidate:** The filter is returning 50,000 rows (0.5% selectivity) after reading all 10M rows — classic missing index signal. First, I verify: does an index on the filter column already exist? `\d table_name` in psql, or `SELECT indexname FROM pg_indexes WHERE tablename = 'table_name'`. If no index: `CREATE INDEX CONCURRENTLY idx_table_filter_col ON table_name(filter_col)`. Run `EXPLAIN ANALYZE` again — the planner should switch to an Index Scan or Bitmap Index Scan, reducing the execution from reading 10M rows to reading 50,000 rows.

**Interviewer:** You create the index, run `EXPLAIN ANALYZE` again, but the planner still uses a sequential scan. Why might this happen?

**Candidate:** Several causes. First: stale statistics. If `ANALYZE` hasn't run since the index was created, the planner may not know the updated row distribution. Run `ANALYZE table_name` then re-explain. Second: `random_page_cost` is too high. On an SSD server with the default `random_page_cost = 4.0`, the planner overestimates index scan cost — set `random_page_cost = 1.1` and test. Third: the planner's row count estimate is wrong. If `EXPLAIN ANALYZE` shows estimated rows=5,000,000 (50% selectivity) but actual rows=50,000, the statistics are stale or the column distribution is skewed beyond what the histogram captures — increase `STATISTICS 500` on the column and re-analyze. Fourth: the table is in cache. If `shared_blks_read=0` and all pages are in the buffer pool, a sequential scan through cached pages might genuinely be cheaper than random page lookups through the index.

**Interviewer:** After adding the index, the query drops to 50ms. Three months later, it's back to 500ms. What happened?

**Candidate:** Data distribution shift or bloat. Three scenarios. One: the predicate selectivity changed. If filter_col = 'active' was 0.5% of rows three months ago and is now 25% (many more active records), the planner is now correct to choose a sequential scan. The "slow" query is actually the right plan. Solution: either accept it, or create a partial index (`CREATE INDEX idx_active ON table WHERE filter_col = 'active'`) if queries always filter this way. Two: the index has bloat. Three months of heavy writes can fragment the index — deleted entries create gaps, and range scans traverse more pages than they need to. `REINDEX CONCURRENTLY idx_table_filter_col` rebuilds the index. Three: statistics are stale again. A data pipeline inserted 5M new rows without triggering autovacuum quickly enough. Run `ANALYZE table_name`. Check `pg_stat_user_tables.last_autoanalyze` to verify autovacuum ran recently.

**Interviewer:** How does `pg_stat_statements` help you find this problem proactively before users complain?

**Candidate:** `pg_stat_statements` normalizes queries — it replaces literal values with `$1`, `$2`, etc. — and aggregates execution statistics by query shape. So all executions of `SELECT ... WHERE filter_col = 'active'` and `SELECT ... WHERE filter_col = 'inactive'` roll up into one row: `SELECT ... WHERE filter_col = $1`. I can query it for `mean_exec_time` trending over time — if that number increases from 50ms to 500ms over three months, I know the query regressed before users file a ticket. I'd compare `shared_blks_read / calls` (disk reads per call) — if it jumps from 10 pages to 10,000 pages per call, the planner switched from index scan to sequential scan. Integrating `pg_stat_statements` into a time-series metrics pipeline (scrape every 15 minutes, compute deltas, alert on `mean_exec_time` crossing a threshold) gives the early warning system.

**Interviewer:** A query has estimated rows=100 but actual rows=2,000,000. You run ANALYZE and re-plan — estimates are still wrong. What causes this and how do you fix it?

**Candidate:** After `ANALYZE`, wrong estimates usually mean one of two things: column correlation or very skewed distribution. First, check whether the query has multiple predicates whose columns are correlated. The planner assumes independence and multiplies selectivities: `sel_A × sel_B`. If `status = 'pending'` has selectivity 0.15 and `region = 'US'` has selectivity 0.40, the planner estimates 6% combined. But if US orders are disproportionately pending, the true combined selectivity might be 30%. Fix: `CREATE STATISTICS tbl_status_region (dependencies) ON status, region FROM tbl; ANALYZE tbl;` — the `dependencies` flag tells the planner about the functional dependency between these columns, improving its combined estimate. Second, if it's a single column with a very skewed distribution (e.g., one value accounts for 80% of rows but the histogram has only 100 buckets), increase the statistics target: `ALTER TABLE tbl ALTER COLUMN status SET STATISTICS 500; ANALYZE tbl;`. More histogram buckets means finer-grained distribution resolution.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What are the four stages of the Postgres query pipeline before execution? | Parse (syntax → parse tree) → Analyze (names → OIDs, permission check) → Rewrite (expand views, apply RLS) → Plan (generate alternatives, estimate costs, pick cheapest). |
| 2 | What does `seq_page_cost = 1.0` mean in the cost model? | All planner costs are expressed in units of "sequential page reads." An index scan costing 45 is 45× more expensive than reading one sequential page. `random_page_cost = 4.0` means a random I/O costs 4× a sequential I/O. |
| 3 | Why should you set `random_page_cost = 1.1` on SSD servers? | The default 4.0 was calibrated for spinning disks. On SSDs, random I/O is close to sequential I/O in cost. With 4.0 on an SSD, the planner overestimates index scan cost and prefers sequential scans even when index scans would be faster. |
| 4 | How does the planner estimate selectivity for `WHERE status = 'pending'`? | It checks `pg_stats.most_common_vals` for 'pending'. If found, uses `most_common_freqs[i]` as the selectivity. If not found, uses (1 − sum of all MCV freqs − null_frac) divided by estimated number of other distinct values. |
| 5 | What is the "independence assumption" in the planner, and when does it fail? | When multiple predicates are ANDed, the planner multiplies individual selectivities: `sel_A × sel_B`. This assumes independence. It fails when columns are correlated (e.g., status=pending and created_at > last_week) — the true combined selectivity is different from the product. Fix: `CREATE STATISTICS ... (dependencies)`. |
| 6 | When does the planner prefer an Index Scan over a Sequential Scan? | When selectivity is low (typically < 5-10% of rows), making the number of random heap page fetches cheaper than reading all pages sequentially. Also depends on `random_page_cost` — lower values (SSDs) shift the threshold toward preferring index scans at higher selectivities. |
| 7 | What is a Bitmap Index Scan and why is it better than an Index Scan at moderate selectivity? | A bitmap scan first traverses the index and builds a bitmap of matching heap page IDs, then sorts them and reads them in page order. This converts random I/O into near-sequential I/O. Also enables combining multiple indexes with bitmap AND/OR. Better than Index Scan when many rows match (too many random fetches). |
| 8 | When does the planner choose a Hash Join? | When both sides of the join are large and unsorted. Hash join builds a hash table from the smaller (inner) side, then probes with each row from the outer side. O(N_inner + N_outer). Requires `work_mem` to hold the hash table; spills to disk if not enough memory. |
| 9 | When does the planner choose a Nested Loop Join? | When the inner side is small (the loop executes few times), or when the inner join can be satisfied by an index lookup (making each inner loop O(log N) instead of O(N)). Nested loop is always correct but O(N_outer × N_inner) — catastrophic for large unsorted inputs. |
| 10 | What does `Rows Removed by Filter: 9950000` in EXPLAIN ANALYZE mean? | The node processed 9,950,000 rows to produce the output. Combined with output row count, this quantifies filter waste. `Rows Removed by Filter: 9,950,000` on a 10M-row table producing 50,000 rows = 99.5% waste → missing index. |
| 11 | What does `Hash (Batches: 128)` mean in an EXPLAIN ANALYZE output? | The hash table for the join's inner side didn't fit in `work_mem` and spilled to disk in 128 batches. Each batch requires a disk write + read cycle. Fix: increase `work_mem` for this session/role until batches = 1. |
| 12 | What is `pg_stat_statements` and what does it track? | A Postgres extension that tracks all query shapes (with literals normalized to $1, $2), and aggregates: call count, total/mean/max execution time, rows returned, buffer hits/reads. Essential for identifying slow queries in production without monitoring every execution. |
| 13 | How does `EXPLAIN ANALYZE` differ from `EXPLAIN`? | `EXPLAIN` shows the plan and estimated costs/rows without running the query. `EXPLAIN ANALYZE` actually executes the query and shows both estimated and actual rows/time. The gap between estimated and actual rows reveals bad statistics; the gap between actual time and expected time reveals execution problems. |
| 14 | What causes "estimated rows=500, actual rows=5000000" in EXPLAIN ANALYZE? | Stale statistics: `ANALYZE` was not run after a large data load. The planner used pg_statistic data from before the data was inserted. Fix: `ANALYZE table_name`. Also: highly skewed distributions that the histogram can't capture — increase `STATISTICS` target for the column. |
| 15 | What does `CREATE STATISTICS orders_status_date (dependencies) ON status, created_at FROM orders` do? | Creates extended statistics capturing the functional dependency between `status` and `created_at`. The planner uses this to improve its combined selectivity estimate for queries with predicates on both columns, avoiding the independence assumption multiplication error. |

---

## 13. Further Reading

- **Postgres documentation — "Using EXPLAIN":** The canonical guide to reading EXPLAIN output, covering all node types and cost formulas.
- **"The Internals of PostgreSQL" by Hironobu Suzuki (interdb.jp) — Chapter 3 (Query Processing):** Detailed walkthrough of the planner's cost calculations with worked examples.
- **Postgres source: `src/backend/optimizer/`:** `costsize.c` contains all cost formula implementations; `planmain.c` and `planner.c` control plan generation.
- **"Use the Index, Luke" (use-the-index-luke.com):** Database-agnostic guide to query optimization with SQL examples; excellent for building intuition about when indexes help.
- **`pg_stat_statements` documentation:** Explains all columns and how to interpret buffer statistics for I/O analysis.

---

## 14. Lab Exercises

**Exercise 1: Statistics Inspection**
On any Postgres database, run `SELECT * FROM pg_stats WHERE tablename = 'your_table' LIMIT 5`. Identify: the `most_common_vals` and `most_common_freqs` for a low-cardinality column. Calculate what selectivity the planner would assign to `WHERE column = 'some_value'`.

**Exercise 2: Scan Type Comparison**
Create a table with 1M rows and one indexed column. Run `EXPLAIN (ANALYZE, BUFFERS)` for: `WHERE indexed_col = 1` (1 row), `WHERE indexed_col < 100` (100 rows), `WHERE indexed_col < 100000` (100,000 rows), and `WHERE indexed_col < 1000000` (all rows). Note at what row count the planner switches from Index Scan → Bitmap Scan → Seq Scan.

**Exercise 3: Join Algorithm Selection**
In `query_planner.py`, modify the inner table's row count in Phase 4 from 50,000 to 1,000 and re-run. Observe how the join algorithm changes. Try values: 100, 1000, 10000, 100000, 1000000. At what inner table size does the planner switch from Nested Loop to Hash Join?

**Exercise 4: Correlated Predicates**
In `query_planner.py`, add a `correlation` value to `ColumnStats`. Extend `Statistics.estimate_rows()` to detect high inter-column correlation (above 0.9) and correct the combined selectivity estimate. Compare the corrected vs uncorrected estimates for the `status AND created_at` predicate.

---

## 15. Key Takeaways

The Postgres planner is a cost-based optimizer that translates SQL into a physical execution plan by estimating costs using pre-computed statistics. Getting the plan right depends on three things: accurate statistics (updated by `ANALYZE`), correct cost parameters (particularly `random_page_cost` for your storage type), and the right indexes existing for the queries your workload runs.

`EXPLAIN ANALYZE` is the single most important diagnostic tool for query performance. The gap between estimated and actual rows is the primary diagnostic: if they match, the planner made the right choice and the query is just slow because of data volume; if they diverge by 10× or more, there's a statistics or correlation problem causing a wrong plan choice.

The scan type hierarchy — Index Scan for high selectivity, Bitmap Scan for moderate, Seq Scan for low — and the join algorithm hierarchy — Nested Loop for small inner, Hash Join for large unsorted, Merge Join for pre-sorted — are mechanical cost calculations. Understanding the cost model makes "why is the planner doing this?" answerable without guessing.

---

## 16. Connections to Other Modules

- **M52 — Postgres Architecture:** The planner runs inside the backend process (M52). The parse → plan phase is why `PREPARE` is useful: it runs once, and the execute phase runs many times inside the same backend process.
- **M54 — Indexes:** The scan type decision (seq scan vs index scan vs bitmap scan) depends entirely on which indexes exist and their selectivity. This module explains the cost model that drives index usage; M54 explains which index types (B-tree, GiST, GIN, BRIN) serve which query patterns.
- **M50 — Buffer Pool Management:** `EXPLAIN ANALYZE BUFFERS` output (`shared_blks_read/hit`) directly reflects the buffer pool hit rate from M50. High `blks_read` means buffer pool misses; every miss is a disk read through the buffer pool's clock sweep eviction.
- **M55 — Vacuuming and Bloat:** `ANALYZE` is the vacuum command that updates `pg_statistic`. Autovacuum runs `ANALYZE` automatically, but when it falls behind (M55), statistics become stale and the planner makes wrong choices — the exact failure mode shown in Scenario 1.
- **M47 — B-Tree Storage Engines:** The planner's index scan cost formula (`cpu_index_tuple_cost × n_index_entries + random_page_cost × n_heap_pages`) maps directly to the B-tree traversal cost from M47: height × page reads for the tree traversal, then one random I/O per result row.

---

## 17. Anti-Patterns

**Anti-pattern: Using `SELECT *` in joins.** When you write `SELECT * FROM orders JOIN customers ...`, the planner must include every column from both tables in the output. This prevents index-only scans (because not all columns are in the index) and increases row width (more data to process in sorts and hash joins). Always project only the columns you need: `SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ...`.

**Anti-pattern: Wrapping indexed columns in functions in WHERE clauses.** `WHERE LOWER(email) = 'user@example.com'` cannot use an index on `email` — the index is ordered by the raw column value, not the function result. The planner falls back to a sequential scan and evaluates `LOWER(email)` on every row. Fix: create a functional index (`CREATE INDEX idx_email_lower ON users(LOWER(email))`) or use a case-insensitive collation (Postgres 12+: `CREATE INDEX ... ON users(email COLLATE "und-x-icu")`).

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `EXPLAIN (ANALYZE, BUFFERS)` | Diagnose query plan + actual performance | Check estimated vs actual rows, buffer reads |
| `pg_stats` | View per-column planner statistics | Inspect histogram, MCV, correlation |
| `pg_stat_statements` | Track query performance over time | Find slow queries by total/mean time |
| `ANALYZE table` | Update planner statistics | After bulk loads, before diagnosing bad plans |
| `CREATE STATISTICS` | Capture inter-column correlations | Fix bad multi-column selectivity estimates |
| `query_planner.py` | Cost model simulation | Visualize planner decisions |
| `RESET ALL` | Clear session GUC overrides | After testing with `enable_seqscan = off` |
| `pg_stat_statements_reset()` | Clear stats accumulation | Before/after performance tuning |

---

## 19. Glossary

**Bitmap Index Scan:** Two-phase scan: build a bitmap of matching heap page IDs from the index, sort them, then read heap pages near-sequentially. Efficient at moderate selectivity; also enables combining multiple indexes with AND/OR.

**Cost (planner):** A unit-less number proportional to sequential page reads. All plan alternatives are compared by cost; the lowest-cost plan is chosen. `seq_page_cost = 1.0` is the baseline.

**Hash Join:** Join algorithm that builds a hash table from the inner table, then probes it with each outer row. O(N_inner + N_outer). Requires `work_mem` for the hash table; spills to disk if insufficient.

**Index Scan:** Traverses the B-tree index to find matching TIDs, then fetches each heap page by TID (random I/O). Efficient at low selectivity (< ~5-10% of rows).

**Independence assumption:** The planner's assumption that multiple AND predicates are statistically independent, leading to combined selectivity = product of individual selectivities. Fails for correlated columns.

**Merge Join:** Join algorithm requiring both inputs sorted on the join key. One merge pass after sorting. Best when inputs are already sorted (from indexes or prior sort steps).

**Nested Loop Join:** For each outer row, iterate the inner relation. O(N_outer × N_inner). Best for small inner tables or when inner can use an index lookup.

**Parse tree:** The abstract syntax tree produced by the parser from SQL text. Contains the structural representation of the query with no semantic resolution.

**pg_stat_statements:** Extension tracking normalized query shapes and their aggregate execution statistics (time, rows, buffer reads). Essential production observability tool.

**Query tree:** The output of semantic analysis. Like a parse tree but with all names resolved to catalog OIDs and types checked.

**Random page cost:** Cost of fetching one page via random I/O (e.g., an index heap fetch). Default `random_page_cost = 4.0` (spinning disk). Should be `1.1` for NVMe SSD.

**Selectivity:** The fraction of rows in a table satisfying a predicate. Selectivity = 0.01 means 1% of rows match.

**Sequential Scan:** Reads every page in the table linearly. Efficient when selectivity is high (> 5-10% of rows) or the table is small.

---

## 20. Self-Assessment

1. Trace the complete pipeline for the query `SELECT * FROM orders WHERE customer_id = 42`. What happens in each stage: parse, analyze, rewrite, plan, execute?
2. The planner estimates 50,000 rows for a query but actually returns 5,000,000. You run `ANALYZE` and re-plan — estimates are still wrong. What are three possible causes and their fixes?
3. Why is `random_page_cost = 4.0` wrong for an SSD server? What does it cause, and what value should you use?
4. In `query_planner.py`, trace what happens in Phase 3 when `status = 'pending'` has selectivity 0.15 and there is no index on `status`. What plan is chosen and why?
5. A hash join shows `Batches: 64` in EXPLAIN ANALYZE. What does this mean? What are two ways to fix it?
6. What does `pg_stat_statements` normalize, and why does normalization make it more useful than logging individual queries?
7. A query uses `WHERE LOWER(email) = 'x@y.com'` and ignores an existing index on `email`. Why? What is the fix?
8. Explain the difference between `EXPLAIN` and `EXPLAIN ANALYZE`. In which production scenario should you be careful about using `EXPLAIN ANALYZE`?

---

## 21. Module Summary

The Postgres query planner is a cost-based compiler: it receives a query tree, reads pre-computed statistics from `pg_statistic`, generates physical execution plans (combinations of scan types and join algorithms), assigns costs to each alternative, and returns the cheapest. Every planner decision is a cost calculation, not a rule: the planner uses an index when the estimated cost of an index scan (index traversal + random heap page fetches) is lower than a sequential scan (reading all pages); it chooses hash join when the cost of building and probing a hash table is lower than a nested loop.

The simulation — `Statistics`, `CostModel`, `Planner`, `ExplainPrinter`, `PgStatStatements` — makes the cost calculations concrete. You can see exactly why the planner switches from Index Scan to Bitmap Scan to Sequential Scan as estimated row count rises, and why Hash Join wins over Nested Loop when both tables are large.

The two most important practical lessons: First, `EXPLAIN (ANALYZE, BUFFERS)` is the diagnostic tool — the gap between estimated and actual rows is the root cause of almost every bad plan, and fixing it means running `ANALYZE`, fixing cost parameters, or creating extended statistics for correlated columns. Second, `pg_stat_statements` is the production monitoring tool — it aggregates query performance over time without you having to watch every query, making performance regressions visible before users notice them.

The next module — M54: Indexes — goes deeper into the index types Postgres offers beyond B-tree: GiST for geometric and text search, GIN for full-text and array containment, and BRIN for naturally ordered time-series data.
