# M51: Column-Oriented Storage

**Course:** DBE-INT-101 — Storage Engine Internals  
**Module:** 05 of 05  
**Global Module ID:** M51  
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

B-trees (M47), LSM-trees (M48), WAL (M49), and buffer pools (M50) were all built around one assumption: the common operation is accessing individual rows by their key. `SELECT * FROM users WHERE id = 42` reads one row, touches 3-4 pages, and the result is served in microseconds. This is the OLTP model: row-oriented, key-based, tiny result sets.

Analytics is fundamentally different. `SELECT region, avg(revenue) FROM orders GROUP BY region` does not want one row. It wants one column (revenue) from potentially 500 million rows. In a row-oriented store (Postgres heap file), each row is stored as a contiguous block of all its columns: (id, customer_id, region, revenue, status, created_at, ...). To read the `revenue` column, you must read EVERY row — including all the columns you don't need. For a row with 30 columns where you need only 1: you're reading 30× more data than necessary.

Column-oriented storage inverts this: instead of storing rows together, each column is stored as a contiguous array. To compute `avg(revenue)`, read only the `revenue` column — one contiguous byte stream. Skip 29 columns entirely. The I/O reduction is 30×. For a 100-column table: 100×.

And that's before compression. Column values tend to be homogeneous (all integers, all strings from a small vocabulary, all timestamps). Homogeneous data compresses dramatically better than row-mixed data. Dictionary encoding, run-length encoding, and delta encoding can reduce a column to 1-5% of its raw size — turning a 100GB column into 1-5GB. The combined effect of columnar layout + aggressive compression makes analytics workloads on Parquet files 10-100× faster than the same data in Postgres heap files.

This is the architecture of Parquet, ORC, Apache Arrow, BigQuery's Capacitor, Snowflake, and Redshift. Understanding it makes Spark, Hive, Presto, and DuckDB query optimization predictable and tunable.

---

## 2. Mental Model

### Row-Oriented vs Column-Oriented Layout

```
Same 5-row table in both layouts:

ROW-ORIENTED (Postgres heap page):
  Row 0: [id=1][region="West"][revenue=1500][status="paid"][ts=2024-01-01]
  Row 1: [id=2][region="East"][revenue=2300][status="paid"][ts=2024-01-02]
  Row 2: [id=3][region="West"][revenue=  800][status="pend"][ts=2024-01-03]
  Row 3: [id=4][region="East"][revenue=4200][status="paid"][ts=2024-01-04]
  Row 4: [id=5][region="West"][revenue=1100][status="pend"][ts=2024-01-05]

  SELECT avg(revenue): must read ALL 5 rows (including id, region, status, ts)
  Bytes read: 5 rows × 50 bytes/row = 250 bytes to get 5 × 8-byte revenue values

COLUMN-ORIENTED (Parquet column chunk):
  id:      [1, 2, 3, 4, 5]
  region:  ["West", "East", "West", "East", "West"]
  revenue: [1500, 2300, 800, 4200, 1100]  ← only this column is read for avg(revenue)
  status:  ["paid", "paid", "pend", "paid", "pend"]
  ts:      [...]

  SELECT avg(revenue): reads ONLY revenue column
  Bytes read: 5 × 8-byte values = 40 bytes (6× less)
  (In practice, 100-column table: 100× I/O reduction)
```

### Why Columns Compress Better

Mixing types in a row (integer, string, float, timestamp...) prevents effective compression — the data has high entropy at a local level. Storing a single column (all integers, or all strings from a small set) exposes statistical regularity:

- `region` column: only 4 distinct values (West, East, North, South) across 500M rows → dictionary encoding: replace 6-char strings with 2-bit integers. Compression: 24:1.
- `status` column: only 3 values → 2 bits each.
- `revenue` column: values monotonically increasing within each day → delta encoding. Instead of storing [1500, 2300, 800, 4200, ...], store [1500, +800, -1500, +3400, ...]. Delta values have much smaller range → more compressible with integer packing.
- `ts` column: timestamps sorted within a row group → delta encoding. Store first timestamp + deltas. 8-byte timestamps become 2-4 byte deltas.

Parquet files in practice achieve 5-15× compression on typical analytics datasets. With Snappy or Zstd on top of encoding: 10-30×.

---

## 3. Core Concepts

### 3.1 Parquet File Layout

Parquet is a columnar file format designed for analytics workloads. Its layout is a hierarchy: File → Row Groups → Column Chunks → Pages.

```
Parquet File Structure:
┌──────────────────────────────────────────────────────────────────┐
│  Magic Number: "PAR1" (4 bytes)                                  │
├──────────────────────────────────────────────────────────────────┤
│  Row Group 0 (target: 128MB)                                     │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Column Chunk: "id" (all values of column "id" for RG0)    │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │  Dictionary Page (optional): unique values           │  │  │
│  │  ├──────────────────────────────────────────────────────┤  │  │
│  │  │  Data Page 0: encoded row values 0..N-1              │  │  │
│  │  │  Data Page 1: encoded row values N..2N-1             │  │  │
│  │  │  ...                                                 │  │  │
│  │  └──────────────────────────────────────────────────────┘  │  │
│  ├────────────────────────────────────────────────────────────┤  │
│  │  Column Chunk: "region"                                    │  │
│  │  ...                                                       │  │
│  ├────────────────────────────────────────────────────────────┤  │
│  │  Column Chunk: "revenue"                                   │  │
│  │  ...                                                       │  │
│  └────────────────────────────────────────────────────────────┘  │
├──────────────────────────────────────────────────────────────────┤
│  Row Group 1 ...                                                  │
│  Row Group 2 ...                                                  │
├──────────────────────────────────────────────────────────────────┤
│  Footer (Thrift-encoded metadata):                               │
│    Schema (column names, types, encoding)                        │
│    For each Row Group:                                           │
│      For each Column Chunk:                                      │
│        file_offset, total_compressed_size, total_uncompressed_size│
│        min/max statistics, null count, distinct_count            │
│        encoding list, compression codec                          │
│  Footer Length (4 bytes)                                         │
│  Magic Number: "PAR1" (4 bytes)                                  │
└──────────────────────────────────────────────────────────────────┘
```

**Row Group:** A horizontal partition of the file — a contiguous range of rows. Typical size: 128MB. The row group is the unit of parallelism: Spark/Presto/Trino processes one row group per task. Larger row groups → fewer metadata lookups, better compression (more data = better statistics). Smaller row groups → finer parallelism, faster filtering (less data to skip).

**Column Chunk:** All values for one column within one row group. A column chunk is the unit of columnar I/O — reading one column reads one column chunk, skipping all others.

**Data Page:** The smallest unit within a column chunk. Typically 1MB. Contains encoded values for a range of rows within the row group. The unit of decompression and encoding.

**Footer:** The metadata index at the end of the file. Contains per-row-group, per-column-chunk statistics (min, max, null count, distinct count). Read first to determine which row groups and column chunks to read — the foundation of predicate pushdown.

### 3.2 Encoding Schemes

**Dictionary Encoding:**
Replaces repeated string (or other) values with integer codes. A "dictionary" maps codes to values. Efficient for low-cardinality columns (columns with few distinct values relative to row count).

```
Raw: ["West", "East", "West", "West", "North", "East", "West"]
Dict: {0: "West", 1: "East", 2: "North"}
Encoded: [0, 1, 0, 0, 2, 1, 0]  (3-bit integers instead of 4-6 byte strings)
Space: 7 × 3 bits = 21 bits + 3 dictionary entries ≈ 30 bytes
Raw:   7 × 4-5 bytes ≈ 30-35 bytes (no savings for 7 rows)
       (At 1M rows: 1M × 3 bits = 375KB vs 1M × 5 bytes = 5MB → 13× savings)

Threshold: Parquet uses dictionary encoding when distinct values < 1000
           (or configurable). Falls back to plain encoding if too many distinct values.
```

**Run-Length Encoding (RLE):**
Encodes sequences of repeated values as (value, count) pairs. Highly effective for sorted or clustered data.

```
Raw:     [0, 0, 0, 0, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1]  (14 values)
RLE:     [(0, 4), (1, 3), (0, 2), (1, 5)]              (4 pairs)
Savings: 14 values → 4 pairs → 3.5× compression (better for longer runs)

Common use: boolean columns (null/not-null bitfields, bool flags)
            status codes with long runs after sorting
```

**Delta Encoding:**
Stores first value then differences (deltas) from previous value. Efficient when values change slowly.

```
Raw:    [1698800000, 1698800060, 1698800120, 1698800180, ...]   (Unix timestamps, 8 bytes each)
Delta:  [1698800000, +60, +60, +60, ...]                       (first value + 1-byte deltas)
Savings: 8 bytes → 1 byte per value after the first → 8× savings for 60-second granularity

Delta-of-delta: used for sensor data where deltas are nearly constant
  Raw deltas: [60, 60, 61, 60, 60] → delta-of-delta: [0, 0, +1, -1, 0]
  Even smaller values → 1-2 bits per value with further integer packing
```

**Bit Packing (Integer Packing):**
When values fit in fewer than 64 bits, pack multiple values into each 64-bit word.

```
8-bit values [0..255] → pack 8 per 64-bit word (8× compression)
3-bit values [0..7]   → pack 21 per 64-bit word (21× compression)
2-bit values [0..3]   → pack 32 per 64-bit word (32× compression)

After dictionary encoding (values replaced with codes), many columns have small codes:
  1000 distinct values → 10-bit codes → 6 per 64-bit word
  100 distinct values  →  7-bit codes → 9 per 64-bit word
  4 distinct values    →  2-bit codes → 32 per 64-bit word
```

### 3.3 Predicate Pushdown and Column Pruning

These are the two mechanisms that make columnar storage dramatically faster than reading raw files.

**Column Pruning:** The query engine reads only the columns referenced in the query (SELECT list, WHERE clause, GROUP BY). For a 100-column table where a query touches 3 columns: 97 column chunks are never read — 97% I/O reduction.

**Predicate Pushdown (Row Group Filtering):** The footer contains min/max statistics for each column per row group. A WHERE clause can eliminate entire row groups without reading any data pages.

```
Query: SELECT sum(revenue) FROM orders WHERE ts BETWEEN '2024-01-01' AND '2024-03-31'

Footer statistics per row group:
  Row Group 0: ts_min=2023-07-01, ts_max=2023-12-31 → SKIP (no overlap with filter)
  Row Group 1: ts_min=2024-01-01, ts_max=2024-06-30 → READ (overlaps)
  Row Group 2: ts_min=2024-07-01, ts_max=2024-12-31 → SKIP

Reading 1/3 of the data instead of all of it: 3× I/O reduction.
For highly selective predicates: 10-100× I/O reduction.
```

For even finer filtering: **page-level statistics** (Parquet's "offset index" and "column index" features, added in Parquet 2.x) allow skipping individual pages within a column chunk based on min/max per page.

**Bloom Filters in Parquet (Apache Parquet 2.0+):** Parquet supports per-column-chunk Bloom filters. A point filter `WHERE user_id = 'abc123'` can check the Bloom filter before reading any column chunk data — if the Bloom filter says "definitely not present," the column chunk is entirely skipped.

### 3.4 Vectorized Execution

Modern columnar query engines process data in vectors — batches of column values — rather than one row at a time. A query engine reading 1,000 `revenue` values as a contiguous array of 1,000 int64s can:
- Use SIMD instructions (AVX2/AVX-512) to sum all 1,000 values in ~15 instructions (vs 1,000 instructions one-at-a-time)
- Process values in CPU cache-friendly patterns (sequential memory access)
- Defer null handling to a separate pass (no per-value null check branching)

This is why DuckDB (vectorized in-process columnar engine) can process 1 billion rows/second on a single core for simple aggregations — SIMD on contiguous column arrays.

### 3.5 Definition and Repetition Levels (Dremel)

Parquet handles nested data (arrays, maps, nested structs) using the **Dremel encoding** — invented by Google for BigQuery and published in the Dremel paper (2010).

Every column value has three components:
- **Value:** The actual data value (or null)
- **Definition Level (def_level):** How many nested nullable fields in the path to this value are actually defined (non-null). Determines whether the value is null, and at which nesting level.
- **Repetition Level (rep_level):** For repeated fields (arrays), which level of the path is repeated for this value. Determines where a new list begins.

```
Schema: message User { required int64 id; repeated group addresses { required string city; } }

Row 0: id=1, addresses=["New York", "Boston"]
Row 1: id=2, addresses=[]
Row 2: id=3, addresses=["Chicago"]

Column "addresses.city":
  values:    ["New York", "Boston",   null, "Chicago"]
  rep_level: [    0,          1,        0,      0    ]
  def_level: [    1,          1,        0,      1    ]

rep=0 → new top-level record starts here
rep=1 → continuation of the addresses list in the same record
def=0 → addresses list is empty (no city defined)
def=1 → city is defined
```

For data engineers, the practical implication is: nested Parquet columns (ARRAY<STRING>, STRUCT<...>) are more expensive to read than flat columns because each value requires decoding three components. Denormalizing nested data into flat columns improves read performance for commonly accessed nested fields.

---

## 4. Hands-On Walkthrough

### 4.1 Parquet File Inspection with PyArrow

```python
import pyarrow as pa
import pyarrow.parquet as pq

# ── Write a Parquet file ──────────────────────────────────────────────────────
data = {
    'id':      list(range(100000)),
    'region':  ['West' if i % 4 < 2 else 'East' for i in range(100000)],
    'revenue': [i * 10 + (i % 100) for i in range(100000)],
    'status':  ['paid' if i % 3 != 0 else 'pending' for i in range(100000)],
}
table = pa.table(data)
pq.write_table(table, 'orders.parquet',
               row_group_size=50000,      # 2 row groups
               compression='snappy',
               write_statistics=True,     # Min/max stats for predicate pushdown
               use_dictionary=['region', 'status'])  # Dictionary-encode low-cardinality columns

# ── Inspect the footer ────────────────────────────────────────────────────────
pf = pq.ParquetFile('orders.parquet')
meta = pf.metadata
print(f"File metadata:")
print(f"  Row groups:      {meta.num_row_groups}")
print(f"  Total rows:      {meta.num_rows}")
print(f"  Serialized size: {meta.serialized_size} bytes")

for rg in range(meta.num_row_groups):
    rgm = meta.row_group(rg)
    print(f"\nRow Group {rg}: {rgm.num_rows} rows, {rgm.total_byte_size/1024:.1f}KB compressed")
    for col in range(rgm.num_columns):
        cm = rgm.column(col)
        stats = cm.statistics
        print(f"  Column '{cm.path_in_schema}':")
        print(f"    Compression:    {cm.compression}")
        print(f"    Encoding:       {cm.encodings}")
        print(f"    Compressed:     {cm.total_compressed_size/1024:.1f}KB")
        print(f"    Uncompressed:   {cm.total_uncompressed_size/1024:.1f}KB")
        if stats:
            print(f"    Min: {stats.min}, Max: {stats.max}, "
                  f"Null count: {stats.null_count}, Distinct: {stats.distinct_count}")

# ── Read only selected columns (column pruning) ───────────────────────────────
# Reads ONLY revenue and region column chunks — skips id, status
result = pq.read_table('orders.parquet', columns=['region', 'revenue'])
print(f"\nRead 2/4 columns: {result.shape}")

# ── Predicate pushdown (row group filtering) ──────────────────────────────────
from pyarrow import dataset as ds
dataset = ds.dataset('orders.parquet', format='parquet')
# This filter is pushed down to the footer statistics check:
filtered = dataset.to_table(
    columns=['revenue'],
    filter=ds.field('region') == 'West'
)
print(f"West-only revenue rows: {len(filtered)}")

# ── Measure compression ratio per column ─────────────────────────────────────
for rg in range(meta.num_row_groups):
    rgm = meta.row_group(rg)
    for col in range(rgm.num_columns):
        cm = rgm.column(col)
        ratio = cm.total_uncompressed_size / cm.total_compressed_size
        print(f"  {cm.path_in_schema}: {ratio:.1f}× compression")
# region: ~8-12× (dictionary + snappy on low-cardinality strings)
# status: ~6-10× (dictionary + snappy)
# revenue: ~2-3× (integer, semi-random — less compressible)
# id: ~1.5-2× (monotonically increasing — delta encoding helps)
```

### 4.2 Parquet with PySpark: Predicate Pushdown Verification

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.statistics.fallBackToHdfs", "true") \
    .getOrCreate()

df = spark.read.parquet("s3://bucket/orders/")

# Observe the query plan to verify predicate pushdown
df.filter(F.col("region") == "West") \
  .select("revenue") \
  .explain(extended=True)
# IMPORTANT: Look for "PushedFilters: [IsNotNull(region), EqualTo(region,West)]"
# in the PhysicalPlan. This means the filter was pushed into the Parquet reader
# and row groups with max(region) < "West" or min(region) > "West" are skipped.

# Verify with EXPLAIN:
df.filter(F.col("ts").between("2024-01-01", "2024-03-31")) \
  .agg(F.sum("revenue")) \
  .explain()
# PushedFilters: [GreaterThanOrEqual(ts,2024-01-01), LessThanOrEqual(ts,2024-03-31)]
# Parquet source reads only row groups where ts_max >= 2024-01-01 AND ts_min <= 2024-03-31

# Check how many row groups were actually read (via Spark metrics)
# spark.sparkContext.uiWebUrl -> Stages -> Task Metrics -> "bytes read"
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
columnar_storage.py

A complete columnar storage engine implementation in Python.

Components:
  - ColumnChunk: Stores one column's values with encoding and statistics
  - DictionaryEncoder: Encode low-cardinality string columns
  - RLEEncoder: Run-length encode repeated values
  - DeltaEncoder: Delta-encode monotonic integer/timestamp columns
  - RowGroup: A set of ColumnChunks for a range of rows (Parquet row group)
  - ParquetLikeFile: Full file with multiple row groups, footer, and predicate pushdown
  - ColumnPruner: Demonstrates I/O savings from column pruning
  - PredicatePushdownSimulator: Demonstrates row group skipping

Design mirrors Parquet:
  - File = multiple row groups, each with column chunks
  - Footer contains per-column-chunk statistics (min, max, null count)
  - Predicate pushdown skips row groups using footer statistics
  - Column pruning reads only requested columns
  - Three encodings: dictionary, RLE, delta

No external dependencies.
"""

from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional


# ─── Encoders ────────────────────────────────────────────────────────────────

@dataclass
class EncodingResult:
    """Result of encoding a column."""
    encoding:       str
    encoded_data:   Any         # The encoded representation
    dictionary:     Optional[list] = None   # For dictionary encoding
    original_bytes: int = 0
    encoded_bytes:  int = 0

    @property
    def compression_ratio(self) -> float:
        if self.encoded_bytes == 0:
            return 1.0
        return self.original_bytes / self.encoded_bytes


class DictionaryEncoder:
    """
    Replace repeated values with integer codes.
    Efficient for low-cardinality columns (few distinct values).
    """

    MAX_DICT_SIZE = 1000   # Fall back to plain if more distinct values

    def encode(self, values: list) -> EncodingResult:
        distinct = list(dict.fromkeys(values))   # Preserve order, deduplicate
        if len(distinct) > self.MAX_DICT_SIZE:
            # Too many distinct values: use plain encoding
            return self.plain_encode(values)

        code_map = {v: i for i, v in enumerate(distinct)}
        encoded = [code_map[v] for v in values]

        # Bits needed per code
        bits_per_code = max(1, math.ceil(math.log2(len(distinct) + 1)))
        encoded_bytes = math.ceil(len(encoded) * bits_per_code / 8)
        dict_bytes = sum(len(str(v)) + 2 for v in distinct)

        # Original: assume average 8 bytes per value (string)
        original_bytes = len(values) * 8

        return EncodingResult(
            encoding='DICTIONARY',
            encoded_data=encoded,
            dictionary=distinct,
            original_bytes=original_bytes,
            encoded_bytes=encoded_bytes + dict_bytes
        )

    def decode(self, result: EncodingResult, indices: Optional[list] = None) -> list:
        """Decode dictionary-encoded values. indices=None means all values."""
        if result.dictionary is None:
            return result.encoded_data
        codes = result.encoded_data
        if indices is not None:
            codes = [codes[i] for i in indices]
        return [result.dictionary[c] for c in codes]

    def plain_encode(self, values: list) -> EncodingResult:
        original_bytes = sum(len(str(v)) for v in values)
        return EncodingResult(
            encoding='PLAIN',
            encoded_data=values,
            original_bytes=original_bytes,
            encoded_bytes=original_bytes
        )


class RLEEncoder:
    """
    Run-Length Encoding: (value, count) pairs.
    Efficient for sorted/clustered data with long runs.
    """

    def encode(self, values: list) -> EncodingResult:
        if not values:
            return EncodingResult('RLE', [], original_bytes=0, encoded_bytes=0)

        runs = []
        current = values[0]
        count = 1
        for v in values[1:]:
            if v == current:
                count += 1
            else:
                runs.append((current, count))
                current = v
                count = 1
        runs.append((current, count))

        # Bytes: each run = value (8 bytes) + count (4 bytes) = 12 bytes
        original_bytes = len(values) * 8
        encoded_bytes  = len(runs) * 12

        return EncodingResult(
            encoding='RLE',
            encoded_data=runs,
            original_bytes=original_bytes,
            encoded_bytes=encoded_bytes
        )

    def decode(self, result: EncodingResult) -> list:
        values = []
        for val, count in result.encoded_data:
            values.extend([val] * count)
        return values

    def decode_range(self, result: EncodingResult, row_start: int, row_end: int) -> list:
        """Decode only rows in [row_start, row_end) — skip prefix runs."""
        values = []
        pos = 0
        for val, count in result.encoded_data:
            run_end = pos + count
            if run_end <= row_start:
                pos = run_end
                continue
            if pos >= row_end:
                break
            start_in_run = max(0, row_start - pos)
            end_in_run   = min(count, row_end - pos)
            values.extend([val] * (end_in_run - start_in_run))
            pos = run_end
        return values


class DeltaEncoder:
    """
    Delta encoding for monotonic integer/timestamp columns.
    Stores first value then differences.
    """

    def encode(self, values: list[int]) -> EncodingResult:
        if not values:
            return EncodingResult('DELTA', [], original_bytes=0, encoded_bytes=0)

        first = values[0]
        deltas = [values[i] - values[i-1] for i in range(1, len(values))]

        # Bytes for deltas: assume each delta fits in fewer bits
        max_delta = max((abs(d) for d in deltas), default=0)
        if max_delta == 0:
            bits_per_delta = 1
        else:
            bits_per_delta = max(1, math.ceil(math.log2(max_delta + 1)) + 1)  # +1 for sign
        encoded_bytes = 8 + math.ceil(len(deltas) * bits_per_delta / 8)   # first_value + deltas

        return EncodingResult(
            encoding='DELTA',
            encoded_data={'first': first, 'deltas': deltas},
            original_bytes=len(values) * 8,
            encoded_bytes=encoded_bytes
        )

    def decode(self, result: EncodingResult) -> list[int]:
        first  = result.encoded_data['first']
        deltas = result.encoded_data['deltas']
        values = [first]
        for d in deltas:
            values.append(values[-1] + d)
        return values


# ─── Column Statistics ────────────────────────────────────────────────────────

@dataclass
class ColumnStats:
    """Per-column statistics stored in the Parquet footer."""
    col_name:       str
    min_value:      Any = None
    max_value:      Any = None
    null_count:     int = 0
    distinct_count: int = 0
    total_rows:     int = 0

    def may_contain(self, value: Any) -> bool:
        """True if value COULD be in this column chunk (min/max check)."""
        if self.min_value is None or self.max_value is None:
            return True
        try:
            return self.min_value <= value <= self.max_value
        except TypeError:
            return True   # Cannot compare (mixed types) → don't skip

    def satisfies_range(self, low: Any, high: Any) -> bool:
        """True if this column chunk's range overlaps [low, high]."""
        if self.min_value is None or self.max_value is None:
            return True
        try:
            return not (self.max_value < low or self.min_value > high)
        except TypeError:
            return True


# ─── Column Chunk ─────────────────────────────────────────────────────────────

class ColumnChunk:
    """
    One column's data for one row group.
    Chooses encoding automatically based on data characteristics.
    """

    def __init__(self, col_name: str, values: list):
        self.col_name    = col_name
        self._raw_values = values
        self.stats       = self._compute_stats(values)
        self.encoding    = self._encode(values)

    def _compute_stats(self, values: list) -> ColumnStats:
        non_null = [v for v in values if v is not None]
        return ColumnStats(
            col_name=self.col_name,
            min_value=min(non_null) if non_null else None,
            max_value=max(non_null) if non_null else None,
            null_count=len(values) - len(non_null),
            distinct_count=len(set(non_null)),
            total_rows=len(values)
        )

    def _encode(self, values: list) -> EncodingResult:
        """Choose the best encoding for these values."""
        if not values:
            return EncodingResult('PLAIN', [], 0, 0)

        non_null = [v for v in values if v is not None]
        if not non_null:
            return EncodingResult('PLAIN', [], 0, 0)

        # Strategy selection:
        # 1. Try dictionary encoding for strings or low-cardinality values
        # 2. Try delta encoding for monotonic integers
        # 3. Try RLE for sorted/clustered data
        # 4. Fall back to plain

        distinct_ratio = self.stats.distinct_count / max(1, len(non_null))

        # String columns or low-cardinality columns → try dictionary
        if isinstance(non_null[0], str) or distinct_ratio < 0.1:
            enc = DictionaryEncoder().encode(values)
            if enc.compression_ratio > 1.5:
                return enc

        # Integer/timestamp columns → try delta encoding if monotonically increasing
        if isinstance(non_null[0], (int, float)):
            is_monotonic = all(non_null[i] <= non_null[i+1] for i in range(len(non_null)-1))
            if is_monotonic and len(non_null) > 10:
                enc = DeltaEncoder().encode([int(v) for v in non_null])
                if enc.compression_ratio > 1.5:
                    return enc

        # Try RLE if there are long runs
        rle_enc = RLEEncoder().encode(values)
        if rle_enc.compression_ratio > 2.0:
            return rle_enc

        # Plain encoding
        raw_bytes = sum(
            len(str(v)) if isinstance(v, str) else 8
            for v in values if v is not None
        )
        return EncodingResult('PLAIN', values, original_bytes=raw_bytes, encoded_bytes=raw_bytes)

    def read_all(self) -> list:
        """Decode all values."""
        enc = self.encoding
        if enc.encoding == 'DICTIONARY':
            return DictionaryEncoder().decode(enc)
        elif enc.encoding == 'RLE':
            return RLEEncoder().decode(enc)
        elif enc.encoding == 'DELTA':
            return DeltaEncoder().decode(enc)
        else:
            return enc.encoded_data

    def read_range(self, row_start: int, row_end: int) -> list:
        """Read rows [row_start, row_end) within this column chunk."""
        enc = self.encoding
        if enc.encoding == 'RLE':
            return RLEEncoder().decode_range(enc, row_start, row_end)
        all_vals = self.read_all()
        return all_vals[row_start:row_end]

    @property
    def compressed_size_bytes(self) -> int:
        return self.encoding.encoded_bytes

    @property
    def uncompressed_size_bytes(self) -> int:
        return self.encoding.original_bytes

    @property
    def compression_ratio(self) -> float:
        return self.encoding.compression_ratio


# ─── Row Group ────────────────────────────────────────────────────────────────

class RowGroup:
    """
    A horizontal slice of the table (contiguous rows).
    Contains one ColumnChunk per column.
    """

    def __init__(self, row_offset: int, columns: dict[str, list]):
        self.row_offset  = row_offset
        self.num_rows    = max((len(v) for v in columns.values()), default=0)
        self.chunks: dict[str, ColumnChunk] = {}
        for col_name, values in columns.items():
            self.chunks[col_name] = ColumnChunk(col_name, values)
        self._bytes_read = 0

    def satisfies_predicate(self, col: str, op: str, value: Any) -> bool:
        """
        Check if this row group MIGHT contain rows satisfying the predicate.
        Returns False → definitely skip this row group.
        Returns True  → might contain matching rows (must read to be sure).
        """
        if col not in self.chunks:
            return True
        stats = self.chunks[col].stats

        if op == '=':
            return stats.may_contain(value)
        elif op == '>=':
            return stats.max_value is None or stats.max_value >= value
        elif op == '<=':
            return stats.min_value is None or stats.min_value <= value
        elif op == 'BETWEEN':
            low, high = value
            return stats.satisfies_range(low, high)
        return True

    def scan(self, columns: list[str], predicates: Optional[list] = None) -> dict[str, list]:
        """
        Read specified columns. predicates is applied AFTER reading (page-level filter).
        In production, predicate pushdown happens at row-group level (before reading)
        and page level (within column chunks using page statistics).
        Returns {col_name: [values...]}
        """
        result = {}
        for col in columns:
            if col in self.chunks:
                self._bytes_read += self.chunks[col].compressed_size_bytes
                result[col] = self.chunks[col].read_all()
        return result

    @property
    def bytes_read(self) -> int:
        return self._bytes_read

    @property
    def total_compressed_bytes(self) -> int:
        return sum(c.compressed_size_bytes for c in self.chunks.values())

    @property
    def compression_summary(self) -> str:
        parts = []
        for name, chunk in self.chunks.items():
            parts.append(f"{name}({chunk.encoding.encoding} {chunk.compression_ratio:.1f}×)")
        return ", ".join(parts)


# ─── Parquet-Like File ────────────────────────────────────────────────────────

class ParquetLikeFile:
    """
    A file with multiple row groups, footer metadata, and predicate pushdown.
    """

    ROW_GROUP_SIZE = 1000   # Rows per row group (production: ~100K-1M rows)

    def __init__(self, schema: list[str]):
        self.schema     = schema
        self.row_groups: list[RowGroup] = []
        self._total_rows = 0

    def write(self, data: dict[str, list]):
        """Write a dict of column → values as one or more row groups."""
        n_rows = max(len(v) for v in data.values())
        row_offset = self._total_rows

        for start in range(0, n_rows, self.ROW_GROUP_SIZE):
            end = min(start + self.ROW_GROUP_SIZE, n_rows)
            rg_data = {col: vals[start:end] for col, vals in data.items()}
            rg = RowGroup(row_offset + start, rg_data)
            self.row_groups.append(rg)

        self._total_rows += n_rows

    def query(self, select_cols: list[str],
              predicates: Optional[list[tuple]] = None) -> dict[str, list]:
        """
        Execute a query with column pruning and predicate pushdown.
        predicates: list of (col, op, value) tuples e.g. [('region', '=', 'West')]
        Returns {col: [values...]} for rows satisfying all predicates.
        """
        result = {col: [] for col in select_cols}
        rgs_read = 0
        rgs_skipped = 0

        for rg in self.row_groups:
            # Row group filter (predicate pushdown via footer statistics)
            if predicates:
                skip = False
                for col, op, val in predicates:
                    if not rg.satisfies_predicate(col, op, val):
                        skip = True
                        break
                if skip:
                    rgs_skipped += 1
                    continue

            rgs_read += 1
            # Column pruning: only read requested columns
            # Also read predicate columns needed for row-level filtering
            cols_needed = set(select_cols)
            if predicates:
                for col, _, _ in predicates:
                    cols_needed.add(col)
            rg_data = rg.scan(list(cols_needed))

            # Row-level predicate filter (within the row group)
            n_rows = len(next(iter(rg_data.values()), []))
            mask = [True] * n_rows
            if predicates:
                for col, op, val in predicates:
                    col_vals = rg_data.get(col, [])
                    for i, v in enumerate(col_vals):
                        if not mask[i]:
                            continue
                        if op == '=':
                            mask[i] = (v == val)
                        elif op == '>=':
                            mask[i] = (v >= val)
                        elif op == '<=':
                            mask[i] = (v <= val)

            for col in select_cols:
                if col in rg_data:
                    result[col].extend(v for v, m in zip(rg_data[col], mask) if m)

        return result, {'row_groups_read': rgs_read, 'row_groups_skipped': rgs_skipped}

    def print_file_stats(self):
        print(f"\n  Parquet-like File Statistics:")
        print(f"    Total rows:    {self._total_rows:,}")
        print(f"    Row groups:    {len(self.row_groups)}")
        for i, rg in enumerate(self.row_groups):
            total_uncompressed = sum(c.uncompressed_size_bytes for c in rg.chunks.values())
            print(f"    RG {i}: {rg.num_rows:,} rows, "
                  f"compressed={rg.total_compressed_bytes/1024:.1f}KB, "
                  f"uncompressed={total_uncompressed/1024:.1f}KB")
            print(f"      Encodings: {rg.compression_summary}")

    def print_footer_stats(self):
        """Print statistics that would be in the Parquet footer."""
        print(f"\n  Footer Statistics (predicate pushdown metadata):")
        for i, rg in enumerate(self.row_groups):
            print(f"  Row Group {i} (rows {rg.row_offset}-{rg.row_offset+rg.num_rows-1}):")
            for col_name, chunk in rg.chunks.items():
                s = chunk.stats
                print(f"    {col_name}: min={s.min_value}, max={s.max_value}, "
                      f"nulls={s.null_count}, distinct={s.distinct_count}")


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import random
    rng = random.Random(42)

    print("=== Columnar Storage Demo ===\n")

    N = 5000
    regions  = ['West', 'East', 'North', 'South']
    statuses = ['paid', 'pending', 'refunded']

    data = {
        'id':      list(range(N)),
        'region':  [regions[rng.randint(0, 3)] for _ in range(N)],
        'revenue': sorted([rng.randint(100, 10000) for _ in range(N)]),  # monotonic
        'status':  [statuses[rng.randint(0, 2)] for _ in range(N)],
        'ts':      list(range(1698800000, 1698800000 + N * 60, 60)),   # 1-min intervals
    }

    # ── Phase 1: Encoding Demo ────────────────────────────────────────────────
    print("─── Phase 1: Encoding Effectiveness ───")
    for col_name in ['region', 'status', 'revenue', 'ts', 'id']:
        chunk = ColumnChunk(col_name, data[col_name])
        print(f"  {col_name:10s}: encoding={chunk.encoding.encoding:12s} "
              f"ratio={chunk.compression_ratio:.1f}× "
              f"({chunk.uncompressed_size_bytes//1024}KB → {chunk.compressed_size_bytes//1024}KB)")

    # ── Phase 2: Write Parquet-like File ──────────────────────────────────────
    print("\n─── Phase 2: File Layout ───")
    pfile = ParquetLikeFile(schema=list(data.keys()))
    pfile.write(data)
    pfile.print_file_stats()
    pfile.print_footer_stats()

    # ── Phase 3: Column Pruning ───────────────────────────────────────────────
    print("\n─── Phase 3: Column Pruning ───")
    # Query: SELECT sum(revenue) — read only 1 of 5 columns
    results, scan_info = pfile.query(select_cols=['revenue'])
    total_revenue = sum(results['revenue'])
    bytes_if_all_cols = sum(
        sum(c.compressed_size_bytes for c in rg.chunks.values())
        for rg in pfile.row_groups
    )
    bytes_revenue_only = sum(
        rg.chunks['revenue'].compressed_size_bytes
        for rg in pfile.row_groups
    )
    print(f"  SELECT sum(revenue): sum={total_revenue:,}")
    print(f"  Bytes if reading ALL columns: {bytes_if_all_cols//1024}KB")
    print(f"  Bytes reading ONLY revenue:   {bytes_revenue_only//1024}KB")
    print(f"  I/O reduction: {bytes_if_all_cols/bytes_revenue_only:.1f}×")

    # ── Phase 4: Predicate Pushdown ───────────────────────────────────────────
    print("\n─── Phase 4: Predicate Pushdown (Row Group Filtering) ───")
    # Query: WHERE region = 'West' — some row groups may be skipped
    results, scan_info2 = pfile.query(
        select_cols=['revenue'],
        predicates=[('region', '=', 'West')]
    )
    print(f"  WHERE region='West': {len(results['revenue']):,} matching rows")
    print(f"  Row groups examined: {scan_info2['row_groups_read']}")
    print(f"  Row groups skipped:  {scan_info2['row_groups_skipped']} "
          f"(via footer statistics)")

    # Query with a range predicate (revenue range)
    # Sort data to make range predicates effective
    high_rev_count = sum(1 for r in data['revenue'] if r >= 5000)
    results3, scan_info3 = pfile.query(
        select_cols=['id', 'revenue'],
        predicates=[('revenue', '>=', 8000)]
    )
    print(f"\n  WHERE revenue >= 8000: {len(results3['id']):,} matching rows")
    print(f"  Row groups examined: {scan_info3['row_groups_read']}")
    print(f"  Row groups skipped:  {scan_info3['row_groups_skipped']}")

    # ── Phase 5: Compression by Encoding ──────────────────────────────────────
    print("\n─── Phase 5: Compression Summary ───")
    print("  Column       Encoding       Ratio   Uncompressed   Compressed")
    print("  " + "-" * 65)
    for col_name in data:
        chunk = ColumnChunk(col_name, data[col_name])
        print(f"  {col_name:<12s} {chunk.encoding.encoding:<14s} "
              f"{chunk.compression_ratio:>5.1f}×  "
              f"{chunk.uncompressed_size_bytes//1024:>10}KB  "
              f"{chunk.compressed_size_bytes//1024:>8}KB")
```

---

## 6. Visual Reference

### Row-Oriented vs Column-Oriented I/O

```
Query: SELECT avg(revenue) FROM orders
Table: 5 columns (id, region, revenue, status, ts), 10M rows

ROW-ORIENTED READ (Postgres heap):
  To read "revenue", must read EVERY row:
  [id=1][region="West"][revenue=1500][status="paid"][ts=1698800000]
  [id=2][region="East"][revenue=2300][status="paid"][ts=1698800060]
  ...
  Bytes read: 10M rows × 50 bytes/row = 500MB
  Useful bytes (revenue only): 10M × 8 bytes = 80MB
  Wasted I/O: 420MB (84% overhead)

COLUMN-ORIENTED READ (Parquet):
  Read ONLY the "revenue" column chunk:
  [1500][2300][800][4200][1100][3200][...]
  Bytes read (compressed): 80MB / 3 (delta compression) ≈ 27MB
  Wasted I/O: 0MB
  Speed advantage: 500MB / 27MB ≈ 18× less I/O
```

### Predicate Pushdown with Footer Statistics

```
File with 4 row groups (500K rows each, 2M total rows):

Footer (read first, ~few KB):
  RG0: ts_min=Jan, ts_max=Mar, revenue_min=100, revenue_max=9800
  RG1: ts_min=Apr, ts_max=Jun, revenue_min=150, revenue_max=9900
  RG2: ts_min=Jul, ts_max=Sep, revenue_min=200, revenue_max=9700
  RG3: ts_min=Oct, ts_max=Dec, revenue_min=100, revenue_max=9800

Query: WHERE ts BETWEEN 'Apr' AND 'Jun'

Predicate evaluation against footer:
  RG0: ts_max=Mar < 'Apr' → SKIP (max < filter_low)
  RG1: [Apr, Jun] overlaps ['Apr','Jun'] → READ
  RG2: ts_min=Jul > 'Jun' → SKIP (min > filter_high)
  RG3: ts_min=Oct > 'Jun' → SKIP

Result: Read only RG1 (25% of data) → 4× I/O reduction
        For month-level query on year of data: ~1/12 reads → 12× reduction
```

---

## 7. Common Mistakes

**Mistake 1: Writing small Parquet files.** The Parquet footer must be read before any data can be accessed. For a 1KB file, the footer overhead may be larger than the data. For 1,000 files each of 1MB, the per-file overhead (open, read footer, seek to row group) consumes most of the query time. Parquet achieves good performance with files of 100MB-1GB each. Small files are a common consequence of streaming writes (one file per micro-batch). Fix with a periodic compaction job that merges small files into larger ones. Target: files large enough that each Spark task reads one file and the file contains thousands of row groups.

**Mistake 2: Not sorting data before writing Parquet for range predicate workloads.** Predicate pushdown with footer statistics is effective only when the column being filtered is "clustered" — most rows in a given row group share similar values, so min/max statistics narrow the search. If `region` values are randomly distributed across row groups (every row group has all 4 regions), the footer statistics can never skip a row group. Sorting by the most frequently filtered column before writing Parquet allows row group statistics to skip entire row groups. In practice: `ORDER BY ts` before writing Parquet for time-series analytics; `ORDER BY region, ts` for multi-column filter patterns.

**Mistake 3: Selecting all columns (`SELECT *`) from Parquet in Spark/Presto.** The fundamental I/O advantage of Parquet is column pruning. `SELECT *` reads all column chunks, eliminating that advantage. In analytical pipelines, always specify only the columns you need. A `SELECT *` on a 100-column Parquet table reads 100× more I/O than a `SELECT col1, col2, col3` query. Many data engineers habitually use `SELECT *` from OLTP experience — it's a significant antipattern in columnar analytics.

---

## 8. Production Failure Scenarios

### Scenario 1: Parquet Footer Read Bottleneck on S3

**Symptoms:** A Spark job reading 50,000 Parquet files from S3 (each ~2MB) takes 45 minutes for what should be a 2-minute query. Executor CPU utilization is near 0% for the first 30 minutes; S3 request counts show 100,000 GET requests before any actual data is read.

**Root cause:** For each Parquet file, the Spark reader makes 2-3 S3 GET requests: (1) read the last 8 bytes (footer length), (2) read the footer (last N KB), (3) read the data pages. With 50,000 files × 3 requests = 150,000 serial S3 GET requests. S3 GET latency is ~5-20ms per request. At 50ms average latency: 150,000 × 50ms = 7,500 seconds = 125 minutes. The footer overhead is dominating.

**Fix:**
```python
# Option 1: Compaction — merge 50,000 small files into 100 files of ~1GB each
spark.read.parquet("s3://bucket/raw/") \
     .repartition(100) \
     .write.mode("overwrite") \
     .parquet("s3://bucket/compacted/")

# Option 2: Enable S3 vectorized prefetch (AWS Glue / EMR)
spark.conf.set("fs.s3a.experimental.input.fadvise", "random")
spark.conf.set("fs.s3a.readahead.range", "64K")

# Option 3: Hudi/Delta/Iceberg table formats — maintain file manifest
# Table formats keep a transaction log with metadata about files,
# avoiding the need to open each file to read its footer.
# Apache Iceberg: footer stats are in the Iceberg manifest, not per-file.
```

### Scenario 2: Predicate Pushdown Not Working in Spark

**Symptoms:** A Spark query with a WHERE clause on a timestamp column reads all 500 Parquet files (50GB) instead of the expected 10 files (1GB). Spark UI shows `Files read: 500` despite the filter.

**Root cause:** Predicate pushdown was disabled. Check: `spark.conf.get("spark.sql.parquet.filterPushdown")` returns `"false"`. Someone had disabled it as a workaround for an unrelated bug. The Spark UI's physical plan shows no `PushedFilters` in the `FileScan parquet` node.

**Fix:**
```python
spark.conf.set("spark.sql.parquet.filterPushdown", "true")

# Verify pushdown is working:
df.filter(F.col("ts") >= "2024-01-01").explain()
# Should show: PushedFilters: [GreaterThanOrEqual(ts,2024-01-01 00:00:00)]

# Also verify that statistics were written when the Parquet was created:
import pyarrow.parquet as pq
meta = pq.read_metadata("file.parquet")
for rg in range(meta.num_row_groups):
    for col in range(meta.row_group(rg).num_columns):
        cm = meta.row_group(rg).column(col)
        if cm.statistics is None:
            print(f"RG {rg} col {cm.path_in_schema}: NO STATISTICS — pushdown won't work!")
# If statistics are None: re-write with write_statistics=True
```

---

## 9. Performance and Tuning

### Parquet Write Optimization

```python
import pyarrow as pa
import pyarrow.parquet as pq

# ── Row Group Size ────────────────────────────────────────────────────────────
# Larger = better compression, fewer footer reads, coarser parallelism
# Smaller = finer parallelism, faster individual row group reads
# Target: ~128MB compressed per row group (Parquet default: 128MB uncompressed)
pq.write_table(table, 'out.parquet',
               row_group_size=500_000)   # ~500K rows per row group

# ── Compression Codec ─────────────────────────────────────────────────────────
# Snappy (default): fast decompress, moderate compression (3-5×)
# Zstd:            slow decompress, excellent compression (8-15×), tunable level
# Gzip:            slow decompress, good compression (4-8×)
# For Spark jobs (CPU-bound): Snappy (faster decompress = faster scan)
# For storage cost: Zstd level 3-5 (2× better than Snappy, ~20% slower decompress)
pq.write_table(table, 'out.parquet', compression='zstd')

# ── Dictionary Encoding Threshold ─────────────────────────────────────────────
# Disable auto-dictionary for high-cardinality columns (UUID, hash, free text)
# Dictionary on a UUID column: 1 dict entry per row → wastes more space than it saves
pq.write_table(table, 'out.parquet',
               use_dictionary=['region', 'status', 'country'],   # Low-cardinality only
               write_statistics=True)   # Always write stats for pushdown

# ── Sort Before Write (critical for predicate pushdown) ───────────────────────
# Sort by the primary filter column so row group min/max are tight
table_sorted = table.sort_by([('ts', 'ascending'), ('region', 'ascending')])
pq.write_table(table_sorted, 'out_sorted.parquet')

# ── Optimal Page Size ─────────────────────────────────────────────────────────
# Larger data pages = fewer dictionary lookups per page = better for sequential scans
# Smaller data pages = better skipping within a column chunk (page-level statistics)
pq.write_table(table, 'out.parquet',
               data_page_size=1_048_576)   # 1MB pages (default: 1MB)
```

### Spark Parquet Tuning

```python
# ── Critical Spark Parquet settings ──────────────────────────────────────────
spark.conf.set("spark.sql.parquet.filterPushdown", "true")         # Enable predicate pushdown
spark.conf.set("spark.sql.parquet.mergeSchema", "false")           # NEVER merge schemas
spark.conf.set("spark.hadoop.parquet.enable.summary.metadata", "false")  # Skip summary files
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")   # 128MB per Spark task

# ── Vectorized reading (critical for performance) ────────────────────────────
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")  # Default true

# ── Column statistics (for predicate pushdown effectiveness) ─────────────────
spark.conf.set("spark.sql.parquet.recordLevelFilter.enabled", "true")

# ── Monitor what's happening during reads ────────────────────────────────────
# In Spark UI: Stages → Tasks → expand Task metrics
# Look for:
#   "records read": total rows in Parquet files (before filter)
#   "bytes read": actual bytes read from storage
#   Compare to file size: low ratio = good predicate pushdown
```

---

## 10. Interview Q&A

**Q1: What is column-oriented storage, and why does it outperform row-oriented storage for analytics?**

Column-oriented storage organizes data so that all values for a given column are stored contiguously, rather than storing all columns of a row together. For a table with 100 columns, a query reading 3 columns in a row-oriented store must read the entire row (all 100 columns) for every row it touches — reading 97 columns it doesn't need. In a column-oriented store, only the 3 needed column chunks are read — a 33× I/O reduction.

The I/O advantage compounds with compression. A column contains values of a single type from many rows. Low-cardinality string columns (like `region` or `status`) have only a few distinct values repeated millions of times — dictionary encoding reduces them to 2-3 bits per value. Monotonically increasing columns (timestamps, sequential IDs) have small delta values — delta encoding reduces 8-byte values to 1-2 bytes. Compression ratios of 5-20× are typical. Combined with I/O reduction from column pruning, the total speedup for analytical queries on columnar storage vs row-oriented storage is typically 10-100×.

However, column-oriented storage is not universally better. For OLTP point queries (`SELECT * FROM users WHERE id = 42`), row-oriented storage reads one row in 3-4 I/Os and reconstructs the full record. Column-oriented storage must read one page from each column — potentially 100 I/Os for a 100-column table. The crossover is roughly: if a query reads > 30% of columns per row, row-oriented is faster; if < 10%, columnar is dramatically faster.

**Q2: What is predicate pushdown in Parquet and how does it work?**

Predicate pushdown moves filter evaluation from the query engine to the storage layer, allowing entire data blocks to be skipped without reading them. In Parquet, it works through the footer statistics.

When a Parquet file is written, each column chunk stores statistics in the footer: min value, max value, null count, and distinct count for each column in each row group. When a query includes a WHERE clause, the Parquet reader checks the footer statistics for each row group before reading any data. If the filter is `WHERE ts BETWEEN '2024-Q1' AND '2024-Q2'` and a row group's ts statistics show `ts_min='2023-Q3', ts_max='2023-Q4'`, that row group cannot contain any matching rows — it is skipped entirely with no data reads.

For queries on well-sorted data (data sorted by the filter column before writing), predicate pushdown can skip 90-99% of the data. For example, 1 year of time-series data queried by quarter: only 25% of row groups are read. For month-level queries: 8% of row groups. The effectiveness depends entirely on whether the statistics are tight (data is sorted or clustered on the filter column). Randomly distributed data gives the same min/max in every row group, and no row groups can be skipped.

---

## 11. Cross-Question Chain

**Interviewer:** You're migrating a Postgres analytics table (500GB, 100 columns) to Parquet files on S3 for Spark queries. What decisions do you make when writing the Parquet files?

**Candidate:** I'd make decisions across four dimensions. First, sort order: I'd sort the data by the most frequently filtered columns — if 80% of queries filter by `ts`, sort by `ts`. This clusters similar values within row groups, making the min/max statistics tight enough to skip most row groups. Second, row group size: I'd target 128-256MB per row group to balance footer overhead (fewer row groups = faster metadata reads) against parallelism (each Spark task processes one row group). At 500GB with 128MB row groups: ~4,000 row groups, ~40 files of ~100 row groups each. Third, compression: Zstd for storage cost (2-3× better than Snappy for typical analytical data) or Snappy for faster decompression if the job is CPU-bound. Fourth, encoding: dictionary encoding on all low-cardinality columns (region, status, category — any column with fewer than 1,000 distinct values), delta encoding for timestamps, plain for high-cardinality strings like free text.

**Interviewer:** After writing the Parquet files, a Spark job running `SELECT region, sum(revenue) FROM orders WHERE ts BETWEEN 'Q1' AND 'Q2' GROUP BY region` is slower than expected. How do you diagnose it?

**Candidate:** I'd check the Spark UI physical plan first: does it show `PushedFilters: [GreaterThanOrEqual(ts,...), LessThanOrEqual(ts,...)]`? If not, predicate pushdown is disabled — enable `spark.sql.parquet.filterPushdown`. If the plan shows the filters are pushed, I'd check the Stage metrics: how many bytes were actually read vs total file size? If bytes_read ≈ total_file_size, the statistics are not tight enough to skip row groups — the ts data is not sorted, so every row group has ts_min='Q1' and ts_max='Q4' (spanning the whole year). I'd rewrite the files with the data sorted by ts.

If the bytes read look right, I'd check whether the job is CPU-bound: high CPU with fast I/O means decompression is the bottleneck — switch from Zstd to Snappy. If there are too many small files (each task reads a tiny file), repartition to fewer larger files. The job should spend most of its time in the aggregation phase, not the scan phase.

**Interviewer:** The job reads from 500,000 files each of 1MB. What's the specific problem and fix?

**Candidate:** The classic small-files problem. For each of 500,000 files: Spark submits a task, the task opens the file on S3 (GET request), reads the footer (another GET), reads the data pages. At ~50ms per S3 request pair × 500,000 files = 25,000 seconds = 7 hours of S3 round-trip time alone. The I/O latency completely dominates.

Fix: compact the 500,000 1MB files into ~500 files of 1GB each. The compaction job itself reads all 500GB, which takes time, but subsequent jobs run orders of magnitude faster. This should be done as a scheduled maintenance job whenever small-file accumulation exceeds a threshold. Apache Hudi, Delta Lake, and Apache Iceberg all have built-in small-file compaction (Hudi's `HoodieCompactor`, Delta's `OPTIMIZE`, Iceberg's `rewrite_data_files`).

**Interviewer:** After compaction, you notice Parquet's compression is not as good as expected — only 1.5× instead of the expected 8-10×. What columns should you investigate and why?

**Candidate:** I'd check each column's distinct count vs row count. Poor compression typically comes from: (1) high-cardinality string columns with many distinct values — user IDs, URLs, order IDs. Dictionary encoding falls back to plain when distinct values exceed the threshold (1,000 by default). A `user_id` column with 10 million distinct values gets no dictionary compression. (2) Numerical columns that are truly random (no delta structure) — random revenue values, random session identifiers. (3) Pre-compressed data embedded in a column — binary blobs, base64-encoded data, compressed JSON. Parquet compression on pre-compressed data produces minimal gain.

The fix: examine the compression ratio per column with `pq.read_metadata()`. For high-cardinality identifier columns: use integer IDs instead of strings (UUID → bigint via a lookup table). For random numeric columns: accept they won't compress well, or consider whether they belong in Parquet at all (very wide tables with many random columns may be better served by a hybrid storage model).

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the fundamental I/O advantage of column-oriented storage for analytics? | Column pruning: reads only the columns referenced in the query. For a 100-column table, a query touching 3 columns reads 3% of the data compared to row-oriented (100%). I/O reduction: up to 97×. |
| 2 | What are the four levels of the Parquet file hierarchy? | File → Row Groups → Column Chunks → Pages. File contains one footer with all metadata. Row groups are horizontal partitions (~128MB). Column chunks store one column per row group. Pages are the decompression unit (~1MB). |
| 3 | What information is in the Parquet footer? | Per-row-group, per-column statistics: min, max, null count, distinct count. Column chunk offsets and sizes. Schema (column names, types, encodings). Read first to enable predicate pushdown and column pruning without reading data. |
| 4 | What is dictionary encoding? | Replace repeated values with integer codes. A "dictionary" maps codes to values. Efficient for low-cardinality columns (< 1,000 distinct values). Region column with 4 values: replace 5-byte strings with 2-bit codes. Compression: up to 32× for 2-value columns. |
| 5 | What is run-length encoding (RLE)? | Store (value, count) pairs instead of repeated values. Efficient when values repeat in long runs. `[A, A, A, B, B] → [(A,3), (B,2)]`. Excellent for sorted boolean or low-cardinality columns after sorting. |
| 6 | What is delta encoding? | Store first value then differences. Efficient for monotonically increasing sequences. Timestamps at 60-second intervals: `[1698800000, 1698800060, ...]` → `[1698800000, +60, +60, ...]`. Transforms 8-byte values into 1-2 byte deltas. |
| 7 | What is predicate pushdown in Parquet? | Using footer statistics (min/max per column per row group) to skip entire row groups that cannot contain rows matching the WHERE clause. Avoids reading data entirely. Effective for sorted/clustered data; ineffective for randomly distributed data. |
| 8 | Why does sorting data before writing Parquet improve query performance? | Sorted data clusters similar values within row groups, making min/max statistics tight. Tight statistics allow predicate pushdown to skip more row groups. For time-series sorted by ts: a quarter-level query skips 75% of row groups. Unsorted: no row groups can be skipped. |
| 9 | What is the small-files problem in Parquet? | Many small files (e.g., 500,000 × 1MB) cause massive overhead: one S3 GET request per file for the footer, plus another for data. 500,000 × 50ms = 7 hours of S3 latency. Fix: compact to fewer large files (500 × 1GB). Target: 128MB-1GB per file. |
| 10 | What is the Dremel encoding in Parquet? | A method for storing nested/repeated data (arrays, structs) as flat column arrays using definition levels (how many nullable fields are defined) and repetition levels (which level repeated). Allows columnar storage of arbitrarily nested schemas. |
| 11 | What is vectorized execution and why does it benefit columnar storage? | Processing data in batches (vectors of column values) rather than one row at a time. Enables SIMD CPU instructions (process 8-16 values simultaneously), cache-friendly sequential memory access. DuckDB/Velox: ~1B rows/second aggregation on columnar data using vectorized execution. |
| 12 | When does predicate pushdown fail to reduce I/O in Parquet? | When data is not sorted/clustered on the filter column — every row group has the same min/max values, so no row groups can be skipped. Also when `spark.sql.parquet.filterPushdown = false` (disabled), or when statistics are missing from the file (written without `write_statistics=True`). |
| 13 | What is column pruning in Parquet? | The query engine reads only the column chunks for columns referenced in the query (SELECT, WHERE, GROUP BY). Skips all other column chunks. `SELECT * FROM parquet_table` disables column pruning — always specify needed columns. |
| 14 | What compression codec should you use for Parquet in Spark? | Snappy (default): fast decompress, moderate compression (3-5×) — best for CPU-bound jobs. Zstd: better compression (8-15×), slower decompress — best for storage cost. Gzip: best compression of the three, slowest — rarely used in Spark. Never use uncompressed for production workloads. |
| 15 | Why is `SELECT *` an anti-pattern for Parquet? | It disables column pruning — all column chunks are read. For a 100-column Parquet table, `SELECT *` reads 100× more I/O than `SELECT col1, col2, col3`. The entire benefit of columnar storage (column pruning + compression) requires specifying only the columns you need. |

---

## 13. Further Reading

- **"Dremel: Interactive Analysis of Web-Scale Datasets" — Melnik et al., VLDB 2010:** The original paper that introduced the Dremel encoding for nested data and inspired Parquet. BigQuery's storage layer is based on this paper.
- **Parquet format specification (github.com/apache/parquet-format):** The authoritative technical spec for the Parquet binary format, encoding algorithms, and nested type encoding.
- **"DuckDB: an Embeddable Analytical Database" (Müehleisen & Raasveldt, SIGMOD 2019):** Explains vectorized execution on columnar data, including how SIMD enables 1B rows/second aggregation.
- **"The Design and Implementation of Modern Column-Oriented Database Systems" — Abadi et al., 2013:** Academic survey comparing C-Store (Vertica), MonetDB, and other column stores — covers late materialization, vectorization, and compression in depth.
- **Apache Arrow documentation:** Explains the in-memory columnar format that Spark, Pandas, and DuckDB use to pass columnar data between components — the "in-flight" equivalent of Parquet's "at-rest" columnar format.

---

## 14. Lab Exercises

**Exercise 1: Compression Ratio by Column Type**
Run `columnar_storage.py`. Create 5 test columns: (1) all-identical values, (2) low-cardinality (4 values), (3) monotonically increasing integers, (4) random integers, (5) random strings. Measure compression ratio for each with each encoder (dictionary, RLE, delta). Which encoder is best for each type?

**Exercise 2: Predicate Pushdown Effectiveness vs Sort Order**
Create a 10,000-row dataset with a `region` column (4 values) and `revenue`. Write it to `ParquetLikeFile` twice: (1) unsorted, (2) sorted by region. Query `WHERE region = 'West'` on both. How many row groups are skipped in each case? Explain why.

**Exercise 3: Column Pruning I/O Savings**
Write a 10-column dataset to `ParquetLikeFile`. Query all 10 columns and measure total bytes read. Then query 1 column, 3 columns, 5 columns. Plot bytes_read vs columns_selected. At what point does selecting more columns start to approach the cost of reading all columns?

**Exercise 4: Nested Data Dremel Encoding**
Implement a simplified Dremel encoder for a schema with one repeated field: `User(id, repeated addresses[city])`. Given 5 users with varying numbers of addresses (including one user with 0 addresses), encode the `addresses.city` column with definition and repetition levels. Verify you can reconstruct the original nested structure from the encoded representation.

---

## 15. Key Takeaways

Columnar storage is the architecture that makes analytics on billions of rows practical. The two fundamental mechanisms — column pruning (read only needed columns) and predicate pushdown (skip row groups that can't match the filter) — combine to reduce I/O by 10-100× compared to row-oriented stores. Compression on homogeneous column data adds another 5-20× reduction. The combined effect is what makes BigQuery scan 1TB for $5 and Spark jobs on Parquet finish in minutes rather than hours.

The encoding choices — dictionary for low-cardinality strings, delta for monotonic integers and timestamps, RLE for sorted repeated values — are deterministic given data characteristics. Understanding which encoding applies when makes it possible to predict compression ratios before writing and diagnose unexpectedly poor compression after.

Predicate pushdown is effective only when data is sorted on the filter column. This is the most important write-time decision: sorting by the primary query filter column determines whether row group statistics are tight enough to skip rows. The small-files problem is the most common production performance failure — each file has per-request overhead, and 500,000 small files produce 500,000 × latency.

These mechanics — Parquet structure, encoding, predicate pushdown, vectorized execution — underlie Spark, Presto/Trino, BigQuery, Snowflake, and DuckDB. They are the foundations of the Data Engineering school (DE-*) courses that follow.

---

## 16. Connections to Other Modules

- **M47 — B-Tree Storage Engines:** B-trees are optimal for OLTP point lookups; columnar storage is optimal for OLAP range aggregations. The key distinction is the ratio of columns accessed per row: B-trees assume all columns needed (one row, all columns); Parquet assumes few columns across many rows.
- **DE-PDA-101 — Pandas and PyArrow:** PyArrow is the Python implementation of Apache Arrow's in-memory columnar format and the primary interface for reading/writing Parquet files. The `pa.Table` and `pq.write_table()` APIs used in this module are the foundation of the DE-PDA-101 data processing workflows.
- **DE-ORC-101 — ORC Format:** ORC (Optimized Row Columnar) is Apache Hive's columnar format — similar to Parquet but with different encoding, indexing, and Bloom filter placement. Understanding Parquet internals makes ORC's design choices immediately interpretable by contrast.
- **DCS-SPK-101 — Apache Spark:** Spark's Parquet reader is one of its most performance-critical components. The predicate pushdown, vectorized reading, and column pruning covered here are exactly what Spark's `ParquetFileFormat` implements under the hood.

---

## 17. Anti-Patterns

**Anti-pattern: Using ORC or Parquet without enabling statistics at write time.** Statistics (min/max, null count) are what enable predicate pushdown. If files were written without statistics (e.g., `write_statistics=False` in PyArrow, or the tool that created them didn't support statistics), every WHERE clause requires reading all data pages — no row groups can be skipped. Always verify `pq.read_metadata(file).row_group(0).column(0).statistics` is not None. If files lack statistics, a rewrite with statistics enabled is the fix.

**Anti-pattern: Using Parquet for OLTP-style workloads with many small reads.** Parquet is designed for scanning large ranges of rows. For lookup-by-key patterns (`WHERE user_id = 'abc123'`), Parquet requires reading an entire row group (~128MB) to find one matching row — even with a Bloom filter. Parquet on object storage is entirely wrong for OLTP lookups. Use Postgres, DynamoDB, or another OLTP store. Parquet's Bloom filters help some, but the fundamental unit of I/O (one row group) is orders of magnitude too large for point lookup workloads.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pyarrow.parquet` | Python Parquet read/write | `write_table()`, `read_metadata()`, `read_table(columns=[...])` |
| `pq.read_metadata()` | Inspect Parquet footer stats | Row group count, column stats, encodings |
| `parquet-tools` (CLI) | Command-line Parquet inspection | `parquet-tools inspect`, `parquet-tools schema` |
| DuckDB `DESCRIBE PARQUET` | Parquet metadata from SQL | `SELECT * FROM parquet_metadata('file.parquet')` |
| Spark UI (Stages tab) | Monitor actual bytes read | Compare to file size; reveals pushdown effectiveness |
| `columnar_storage.py` | Columnar engine simulation | Dictionary, RLE, delta encoding; predicate pushdown |
| `pandas.read_parquet` | Read Parquet to DataFrame | `columns=` for pruning, `filters=` for pushdown |

---

## 19. Glossary

**Column chunk:** All values for one column within one Parquet row group. The unit of columnar I/O — reading one column reads one column chunk per row group.

**Column pruning:** Reading only the column chunks for columns referenced in the query. Skips all other columns entirely. The primary I/O optimization of columnar storage.

**Delta encoding:** Storing the first value and then differences (deltas) from the previous value. Efficient for monotonically increasing integers and timestamps.

**Dictionary encoding:** Replacing column values with integer codes mapped through a dictionary. Efficient for low-cardinality columns (< ~1,000 distinct values). Compression: 10-30× for string columns with few distinct values.

**Dremel encoding:** Google's method for storing nested/repeated data (arrays, maps, structs) as flat column arrays using definition and repetition levels. The basis for Parquet's nested type encoding.

**Footer:** The metadata section at the end of a Parquet file. Contains schema, per-row-group per-column statistics (min, max, null count), column chunk offsets. Read before any data to enable predicate pushdown and column pruning.

**Predicate pushdown:** Using row group statistics (from the footer) to skip entire row groups that cannot contain rows matching a WHERE clause. Requires data sorted on the filter column for maximum effectiveness.

**Row group:** A horizontal partition of a Parquet file containing all columns for a contiguous range of rows. Typical size: 128MB. The unit of parallelism (Spark processes one row group per task) and predicate filtering.

**Run-length encoding (RLE):** Storing (value, count) pairs instead of repeated individual values. Efficient for sorted or clustered data with long runs of identical values.

**Vectorized execution:** Processing column data in batches (vectors) rather than row-at-a-time. Enables SIMD CPU instructions and cache-efficient sequential memory access. Used by DuckDB, Velox (Meta), and Apache Arrow compute kernels.

---

## 20. Self-Assessment

1. A 50-column table has 1 billion rows. A query reads 3 columns. What is the theoretical I/O reduction of columnar storage vs row-oriented storage, before compression?
2. In `columnar_storage.py`, the `region` column (4 distinct values) uses dictionary encoding. Calculate the compression ratio: raw string average 5 bytes, 2-bit code, 1M rows.
3. A query has `WHERE ts BETWEEN '2024-Q1' AND '2024-Q2'`. The Parquet file has 8 row groups covering equal quarters of a 2-year period. How many row groups can predicate pushdown skip, assuming data is sorted by ts?
4. Why does `SELECT *` from a Parquet file defeat the purpose of columnar storage?
5. In `columnar_storage.py`, modify the `RowGroup.satisfies_predicate` method to handle the `!=` operator. What limitation does a min/max check have for inequality predicates?
6. Explain when RLE compression achieves better compression than dictionary encoding. Give a concrete example.
7. You write 10 million rows to Parquet sorted by `user_id`. Queries always filter by `ts`. What problem does this create, and how would you fix it without rewriting the file?
8. What is vectorized execution and why is it specifically suited to columnar storage? Why can't row-oriented storage use the same technique as effectively?

---

## 21. Module Summary

Column-oriented storage is the final and most impactful piece of the storage engine puzzle. While B-trees, LSM-trees, WAL, and buffer pools are all built around the row-as-unit-of-access assumption, columnar storage inverts this: columns are the unit of access, and rows are assembled only at query time (late materialization).

The Parquet file — row groups as horizontal partitions, column chunks as vertical partitions, footer as the metadata index, and encoding (dictionary, RLE, delta) as the compression layer — is the universal format of the modern analytics stack. Its design decisions are each traceable to a performance goal: row groups for parallelism and predicate pushdown, column chunks for I/O reduction, footer statistics for skip-without-reading, and encoding schemes for compression without full decompression.

The implementation — `ColumnChunk` with automatic encoding selection, `RowGroup` with statistics-based predicate evaluation, `ParquetLikeFile` with query planning, column pruning, and row group skipping — demonstrates the full end-to-end flow from data ingestion to analytical query execution.

Production mastery — sorting data for predicate pushdown effectiveness, compacting small files, choosing compression codecs, verifying pushdown with EXPLAIN, monitoring bytes read in Spark UI — translates directly to optimizing Spark and Presto jobs on S3 Parquet data, the dominant pattern in cloud data warehousing.

This completes DBE-INT-101: Storage Engine Internals. The five modules — B-Trees (M47), LSM-Trees (M48), WAL (M49), Buffer Pool (M50), and Column-Oriented Storage (M51) — together cover the full spectrum of database storage: the core data structures (B-tree, LSM), the durability mechanism (WAL), the performance layer (buffer pool), and the analytics format (Parquet/columnar). The next course — DBE-INT-102: Query Execution Internals — builds on this foundation, covering how the query engine transforms SQL into physical plans that efficiently access the storage structures built in this course.
