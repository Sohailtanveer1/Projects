# Course Catalog

All 35 courses in the university. Each entry covers: what the course is, why it exists, prerequisites, modules, learning outcomes, and relevance to interviews and production.

---

## How to Read This Catalog

**Why it exists** — the specific gap this course fills. What a student cannot do without it, and can do after completing it.

**Interview relevance** — the frequency and type of interview questions this course enables. Ratings: High (appears in ≥70% of senior DE interviews), Medium (30–70%), Low (<30%).

**Production relevance** — how directly this course maps to day-to-day work at a data engineering company. Ratings: Critical (you will use this daily), Important (weekly), Background (useful context).

---

## School 1: CS Foundations (CSF)

### CSF-ARC-101 — How Computers Execute Programs

**Semester:** 0  
**Why it exists:** Every performance problem in data engineering — Spark OOM, Python slowness, Kafka throughput limits — traces back to hardware. A data engineer who doesn't understand how CPUs and memory work will cargo-cult solutions without understanding why they work.  
**Prerequisites:** None  
**Modules:**
- M01: The Von Neumann Machine (CPU, registers, fetch-decode-execute cycle)
- M02: CPU Microarchitecture (pipelining, branch prediction, out-of-order execution, SIMD/AVX2)
- M03: The ISA Contract (x86-64, ARM AArch64, RISC-V — what the software-hardware contract is)
- M04: CPU Performance Analysis (perf, flame graphs, performance counters)
- M05: From Python to Electrons (tracing one for loop from bytecode to machine code to hardware)

**Learning outcomes:** Explain CPU execution from first principles. Profile CPU-bound code with `perf`. Understand why SIMD exists and when Spark/NumPy uses it. Explain branch prediction and its failure modes.  
**Interview relevance:** High — "Explain why this code is slow" questions assume CPU knowledge  
**Production relevance:** Critical — underpins every performance optimization you will ever do

---

### CSF-ARC-102 — Memory Architecture

**Semester:** 0  
**Why it exists:** The single biggest performance gap in data engineering is treating memory as uniform. A data engineer who understands cache lines, NUMA, and page faults can explain why sequential reads are 100× faster than random reads — which directly explains columnar storage, Parquet page layout, and Spark memory management.  
**Prerequisites:** CSF-ARC-101  
**Modules:**
- M01: The Memory Hierarchy (registers, L1/L2/L3, DRAM, SSD, NVMe — latency at each tier)
- M02: Cache Lines and Coherence (64-byte cache lines, MESI protocol, false sharing)
- M03: NUMA Architecture (how NUMA topology affects Spark executor placement)
- M04: Virtual Memory and Page Tables (page faults, TLB, huge pages)
- M05: Memory Performance Analysis (tracemalloc, Valgrind, identifying memory bottlenecks)

**Learning outcomes:** Explain cache line effects with a benchmark. Identify false sharing in concurrent code. Explain why columnar storage is faster for scans at the hardware level. Diagnose a page fault storm.  
**Interview relevance:** High — "Why is sequential access faster?" is a near-universal question  
**Production relevance:** Critical — directly explains Spark memory tuning and Parquet layout

---

### CSF-ALG-101 — Algorithms and Data Structures for Data Engineers

**Semester:** 0  
**Why it exists:** Data engineers who lack DSA fundamentals cannot pass coding interviews. More importantly, they cannot make informed choices between hash joins and sort-merge joins, or between B-trees and LSM-trees, without understanding the underlying algorithmic complexity.  
**Prerequisites:** None  
**Modules:**
- M01: Complexity Analysis (Big-O, amortized analysis, space vs time)
- M02: Hash Tables (collision resolution, load factors, why Spark uses hash joins)
- M03: Trees (BST, AVL, B-tree — and why databases use B-trees, not BSTs)
- M04: Sorting Algorithms (merge sort, external merge sort — the algorithm behind Spark shuffle)
- M05: Graphs and DAGs (BFS/DFS, topological sort — the algorithm behind Airflow and Spark DAGs)

**Learning outcomes:** Analyze algorithm complexity. Explain why Spark uses hash joins vs sort-merge joins for given data sizes. Implement topological sort (as in Airflow's task scheduler). Explain external merge sort.  
**Interview relevance:** High — coding screen prerequisite  
**Production relevance:** Background — rarely coded directly; foundational for reasoning about tooling

---

### CSF-OS-101 — Operating System Internals

**Semester:** 0  
**Why it exists:** Every Spark job, every Kafka broker, every Airflow task runs on an operating system. Understanding process scheduling, virtual memory, file I/O, and system calls explains why OS-level configuration (ulimits, file descriptors, page cache) matters for data pipelines at scale.  
**Prerequisites:** CSF-ARC-101, CSF-ARC-102  
**Modules:**
- M01: Processes and Threads (fork, exec, thread creation, context switching)
- M02: Process Scheduling (CFS, nice values, CPU affinity — why JVM GC pauses matter)
- M03: Virtual Memory and Page Cache (page cache, dirty pages, msync — why Kafka is fast)
- M04: File I/O and System Calls (VFS, read/write path, O_DIRECT, mmap)
- M05: Signals and IPC (signals, pipes, Unix sockets — how Spark Driver talks to Executors)

**Learning outcomes:** Explain how the OS page cache makes Kafka fast. Trace a file read from application to disk. Explain context switching overhead and why it matters for Spark executors. Use `strace` to diagnose a stuck process.  
**Interview relevance:** Medium — "How does the OS affect Kafka performance?" is Staff-level  
**Production relevance:** Critical — OS configuration is daily for Kafka and Spark on-prem

---

## School 2: Systems (SYS)

### SYS-LNX-101 — Linux for Data Engineers

**Semester:** 1  
**Why it exists:** Data engineering runs on Linux. A data engineer who cannot navigate a Linux server, debug a running process, or interpret `iostat` output cannot debug production issues.  
**Prerequisites:** CSF-OS-101  
**Modules:**
- M01: Linux File System (inodes, hard links, permissions, ACLs)
- M02: Process Management (ps, top, htop, kill, nice, systemd)
- M03: Storage and I/O (iostat, iotop, lsblk, mount, XFS vs ext4)
- M04: Memory Diagnostics (free, vmstat, /proc/meminfo, OOM killer)
- M05: CPU Diagnostics (perf, sar, mpstat, CPU throttling)

**Learning outcomes:** Diagnose a server that is "slow" using only CLI tools. Explain what the OOM killer does and how to configure it for a Kafka broker. Interpret `iostat` output for a Spark worker.  
**Interview relevance:** High — "Your Spark job is slow. What do you check first?" expects Linux CLI  
**Production relevance:** Critical — first-line production debugging

---

### SYS-LNX-102 — Linux Performance Engineering

**Semester:** 1  
**Why it exists:** `SYS-LNX-101` covers diagnosis. This course covers optimization — understanding why a system is slow and what to do about it.  
**Prerequisites:** SYS-LNX-101  
**Modules:**
- M01: Brendan Gregg's USE Method (Utilization, Saturation, Errors — applied framework)
- M02: Flame Graphs (generating and reading CPU/memory flame graphs)
- M03: eBPF and BCC Tools (bpftrace, execsnoop, opensnoop for production tracing)
- M04: Network Performance (ss, netstat, tcpdump, nmap, iperf3)
- M05: Linux Tuning for Data (Kafka, Spark, Flink-specific OS configuration)

**Learning outcomes:** Generate a flame graph for a Python process. Apply the USE method to a Kafka broker. Configure Linux kernel parameters for a Kafka broker (tcp_backlog, ulimits, vm.swappiness).  
**Interview relevance:** High — production tuning questions at Senior+ level  
**Production relevance:** Critical — on-call engineers need this for every production incident

---

### SYS-NET-101 — Networking for Data Engineers

**Semester:** 1  
**Why it exists:** Distributed data systems are distributed because they communicate over networks. A data engineer who doesn't understand TCP, DNS, and network latency cannot explain why Kafka replication adds latency, why Spark shuffle is expensive, or why multi-region replication has a hard lower bound.  
**Prerequisites:** CSF-OS-101  
**Modules:**
- M01: TCP/IP Fundamentals (IP addressing, TCP handshake, flow control, congestion control)
- M02: DNS and Service Discovery (DNS resolution, TTL, service mesh basics)
- M03: TLS and mTLS (TLS handshake, certificate chains, mTLS for Kafka)
- M04: Network Latency (RTT, bandwidth vs latency, cross-region latency — and its effect on Raft)
- M05: Load Balancing and Proxies (L4 vs L7, connection pooling, reverse proxies)

**Learning outcomes:** Explain why `acks=all` in Kafka costs latency (cross-broker TCP RTT). Calculate the minimum replication lag in a multi-region Kafka setup. Configure TLS for a Kafka cluster. Explain what a TCP SYN flood is and how it affects a Kafka broker.  
**Interview relevance:** Medium — network depth is Staff-level  
**Production relevance:** Critical — required for production Kafka and distributed Spark

---

### SYS-NET-102 — Cloud Networking and Security

**Semester:** 1  
**Why it exists:** Most data engineering runs in cloud VPCs. Understanding VPC design, IAM, security groups, and private endpoints is required for production cloud data platforms.  
**Prerequisites:** SYS-NET-101  
**Modules:**
- M01: VPC Architecture (subnets, routing tables, NAT gateways, peering)
- M02: IAM and RBAC (IAM roles, service accounts, Workload Identity — Google Cloud)
- M03: Security Groups and Network Policies (inbound/outbound rules, least-privilege networking)
- M04: Private Endpoints and VPC Service Controls (preventing data exfiltration)
- M05: Cloud DNS and Service Mesh basics

**Learning outcomes:** Design a VPC for a cloud data platform with correct subnet segmentation. Configure Workload Identity for BigQuery access from a Spark cluster. Explain VPC Service Controls and when to use them.  
**Interview relevance:** Medium — cloud architecture interviews assume VPC knowledge  
**Production relevance:** Critical — every cloud data platform requires this

---

### SYS-DST-101 — Distributed Systems Theory

**Semester:** 2  
**Why it exists:** Kafka, Spark, BigQuery, and every distributed database make trade-offs grounded in distributed systems theory. A data engineer who doesn't understand CAP, consensus, and failure models will make wrong architectural decisions and give wrong interview answers.  
**Prerequisites:** SYS-NET-101, CSF-OS-101  
**Modules:**
- M01: The CAP Theorem and PACELC (CP vs AP, partitions, latency vs consistency)
- M02: Consensus Algorithms (Paxos intuition, Raft leader election and log replication)
- M03: Replication (synchronous vs asynchronous, leader-follower, multi-leader, leaderless)
- M04: Failure Modes (split-brain, network partition, partial failure, cascading failure)
- M05: Distributed Transactions (2PC, Saga, compensating transactions)

**Learning outcomes:** Classify Kafka, Postgres, Cassandra, and BigQuery as CP or AP with justification. Whiteboard Raft leader election in a 5-minute interview. Explain why 2PC is a blocking protocol and why Saga is preferred for long-lived transactions. Explain split-brain and how ISR prevents it in Kafka.  
**Interview relevance:** High — Staff DE interviews always include distributed systems theory  
**Production relevance:** Important — needed for architecture decisions, less for daily work

---

### SYS-DST-102 — Distributed Systems in Practice

**Semester:** 2  
**Why it exists:** Theory without implementation is not engineering. This course bridges the gap by implementing toy versions of consensus and replication, and by reading the failure analysis reports from real distributed systems.  
**Prerequisites:** SYS-DST-101  
**Modules:**
- M01: Implementing a Leader-Follower Replication Log (in Python)
- M02: Implementing a Raft Follower (simplified state machine in Python)
- M03: Reading Real Failure Reports (Kafka ISR, ZooKeeper failures, network partition postmortems)
- M04: Clock Synchronization (NTP, logical clocks, vector clocks, hybrid logical clocks)
- M05: Observability in Distributed Systems (distributed tracing, OpenTelemetry)

**Learning outcomes:** Implement a simple Raft follower state machine. Explain why HLC (Hybrid Logical Clocks) are used in CockroachDB. Read a distributed system failure report and identify the root cause in terms of distributed systems theory.  
**Interview relevance:** High — implementation depth differentiates candidates  
**Production relevance:** Important — reasoning ability for production incidents

---

## School 3: Database Engineering (DBE)

### DBE-INT-101 — Storage Engine Internals

**Semester:** 2  
**Why it exists:** Databases are not magic boxes. A data engineer who understands B-trees, LSM-trees, WAL, and buffer pool management can explain why Postgres is fast for reads, why RocksDB is fast for writes, and why Kafka's log structure is a deliberate design choice.  
**Prerequisites:** CSF-ARC-102, SYS-LNX-101  
**Modules:**
- M01: B-Tree Storage Engines (page layout, split/merge, write amplification, buffer pool)
- M02: LSM-Tree Storage Engines (MemTable, SSTable, compaction, RocksDB internals)
- M03: Write-Ahead Log (WAL structure, crash recovery, checkpointing)
- M04: Buffer Pool Management (LRU eviction, dirty page flushing, double-write buffer)
- M05: Column-Oriented Storage (Parquet internals, page layout, dictionary encoding, RLE)

**Learning outcomes:** Explain B-tree vs LSM-tree write amplification with numbers. Describe WAL crash recovery step by step. Explain why Parquet is faster than CSV for aggregation queries (at the byte level). Explain what a buffer pool is and why it exists.  
**Interview relevance:** High — storage engine questions are common at Senior+ level  
**Production relevance:** Important — directly relevant for BigQuery, Iceberg, Delta Lake tuning

---

### DBE-INT-102 — Postgres Architecture

**Semester:** 2  
**Why it exists:** Postgres is the most widely used database in data engineering stacks (metadata stores, dbt targets, lineage databases). Understanding Postgres internals enables production-grade configuration and debugging.  
**Prerequisites:** DBE-INT-101  
**Modules:**
- M01: Postgres Architecture (process model, shared buffer, WAL writer, checkpointer)
- M02: Query Planning (parse → analyze → plan → execute, pg_stat_statements)
- M03: Indexes (B-tree, GiST, GIN, BRIN — when to use each)
- M04: Vacuuming and Bloat (MVCC transaction IDs, autovacuum, table bloat)
- M05: Postgres for Data Engineering (connection pooling with PgBouncer, logical replication for CDC)

**Learning outcomes:** Configure autovacuum for a high-write table. Read `pg_stat_statements` to find slow queries. Explain why Postgres uses MVCC. Set up logical replication for CDC to Kafka.  
**Interview relevance:** Medium — Postgres is common but often implicit  
**Production relevance:** Critical — Airflow metadata, dbt target, most ETL tools use Postgres

---

### DBE-INT-103 — Transaction Isolation and Concurrency

**Semester:** 2  
**Why it exists:** ACID is promised by every database, but the guarantees differ by isolation level. A data engineer who doesn't understand MVCC, snapshot isolation, and serializable isolation will introduce data races and phantom reads into pipelines.  
**Prerequisites:** DBE-INT-102, SYS-DST-101  
**Modules:**
- M01: ACID Properties (atomicity, consistency, isolation, durability — with examples of each failing)
- M02: Isolation Levels (read uncommitted → read committed → repeatable read → serializable)
- M03: MVCC Internals (version chains, read timestamps, garbage collection)
- M04: Locking (shared/exclusive locks, deadlock detection, lock-free algorithms)
- M05: Snapshot Isolation in Practice (Delta Lake ACID, Iceberg snapshot isolation)

**Learning outcomes:** Demonstrate a read skew anomaly with SQL. Explain MVCC version chain with a diagram. Explain how Delta Lake achieves ACID for batch writes. Identify the correct isolation level for a given pipeline scenario.  
**Interview relevance:** High — "What is ACID?" followed by "What is MVCC?" is a classic pair  
**Production relevance:** Critical — data correctness depends on isolation level choice

---

### DBE-SQL-101 — SQL Internals and Query Optimization

**Semester:** 3  
**Why it exists:** Writing SQL is table stakes. Understanding how a SQL engine executes a query — the parse tree, the logical plan, the physical plan, the execution — is what separates engineers who write slow SQL from engineers who debug it.  
**Prerequisites:** DBE-INT-102  
**Modules:**
- M01: SQL Execution Pipeline (parse → bind → optimize → execute, with Postgres and BigQuery examples)
- M02: Join Algorithms (nested loop, hash join, sort-merge join — when each is chosen)
- M03: Reading EXPLAIN Plans (Postgres EXPLAIN ANALYZE, BigQuery query plan, Spark explain())
- M04: Index Design for Queries (selectivity, covering indexes, partial indexes)
- M05: Aggregation Internals (GROUP BY hash aggregate vs sort aggregate, pre-aggregation)

**Learning outcomes:** Read a Postgres EXPLAIN ANALYZE output and identify the bottleneck. Explain why BigQuery doesn't use indexes. Rewrite a slow query using index coverage. Explain when a hash join is preferred over sort-merge.  
**Interview relevance:** High — SQL query optimization is the most common DE interview topic  
**Production relevance:** Critical — daily for debugging slow analytical queries

---

### DBE-SQL-102 — Advanced Analytical SQL

**Semester:** 3  
**Why it exists:** Modern data engineering involves complex analytical SQL: window functions, recursive CTEs, sessionization, funnel analysis. These patterns appear in dbt models, data warehouse queries, and data science pipelines.  
**Prerequisites:** DBE-SQL-101  
**Modules:**
- M01: Window Functions (PARTITION BY, ORDER BY, ROWS vs RANGE, running totals, lag/lead)
- M02: Advanced Aggregation (GROUPING SETS, ROLLUP, CUBE, conditional aggregation)
- M03: Recursive CTEs (hierarchical data, graph traversal, bill-of-materials)
- M04: Analytical Patterns (sessionization, funnel analysis, cohort analysis, retention)
- M05: SQL for Data Quality (deduplication, gap detection, data contract validation)

**Learning outcomes:** Write a funnel query using window functions and conditional aggregation. Write a recursive CTE that traverses a DAG. Implement sessionization logic using window functions. Use SQL to detect schema drift in an output table.  
**Interview relevance:** High — advanced SQL is the most-tested skill in DE take-home exams  
**Production relevance:** Critical — these patterns are in every mature dbt codebase

---

## School 4: Programming (PRG)

### PRG-PY-101 — Python Internals

**Semester:** 1  
**Why it exists:** Python is the lingua franca of data engineering. Engineers who understand CPython internals can explain the GIL, profile memory leaks, and optimize Python UDFs without cargo-culting.  
**Prerequisites:** None  
**Modules:**
- M01: CPython Architecture (interpreter, bytecode, eval loop, reference counting)
- M02: The GIL (what it is, when it's released, when it does and doesn't matter)
- M03: Memory Management (reference counting, cyclic GC, `tracemalloc`, memory leaks)
- M04: Python Profiling (cProfile, py-spy, line_profiler, flame graphs for Python)
- M05: Python Bytecode (dis.dis(), peephole optimizer, why some Python patterns are faster)

**Learning outcomes:** Explain the GIL with a multithreaded benchmark. Profile a Python script using `cProfile` and identify the hot path. Find a memory leak using `tracemalloc`. Explain why list comprehensions are faster than `for` loops in CPython.  
**Interview relevance:** High — Python internals questions at Senior+ level  
**Production relevance:** Critical — Python UDF performance in Spark depends on this knowledge

---

### PRG-PY-102 — Python Concurrency

**Semester:** 1  
**Why it exists:** Data pipelines use all three concurrency models: threading (for I/O-bound parallelism), multiprocessing (for CPU-bound parallelism), and asyncio (for high-concurrency network I/O). Understanding which to use and when is a production engineering skill.  
**Prerequisites:** PRG-PY-101, CSF-OS-101  
**Modules:**
- M01: Threading Model (thread creation, GIL implications, when threads help)
- M02: Multiprocessing (fork vs spawn, shared memory, `concurrent.futures.ProcessPoolExecutor`)
- M03: Asyncio Deep Dive (event loop, coroutines, `await`, `asyncio.gather`, backpressure)
- M04: Concurrency Patterns (producer-consumer, work queues, rate limiting)
- M05: Concurrency in Data Pipelines (async database calls, parallel file uploads, rate-limited APIs)

**Learning outcomes:** Choose between threading/multiprocessing/asyncio for a given task. Implement a rate-limited async API client. Explain why asyncio is not parallelism. Demonstrate the GIL preventing CPU parallelism in a benchmark.  
**Interview relevance:** High — concurrency appears in Python coding interviews  
**Production relevance:** Critical — data pipeline performance depends on concurrency model

---

### PRG-PY-103 — Python Data Libraries

**Semester:** 2  
**Why it exists:** Pandas, NumPy, and PyArrow are the three libraries every data engineer uses daily. Understanding their internals — NumPy's vectorized operations, PyArrow's columnar format, Pandas' memory model — explains performance and enables optimization.  
**Prerequisites:** PRG-PY-101, CSF-ARC-102  
**Modules:**
- M01: NumPy Internals (strided arrays, vectorized operations, broadcasting, BLAS)
- M02: Pandas Architecture (DataFrame internals, dtypes, copy vs view, memory usage)
- M03: PyArrow and the Apache Arrow Format (columnar format, zero-copy reads, IPC)
- M04: Polars (Lazy evaluation, query optimizer, comparison to Pandas)
- M05: Python Performance Patterns (vectorization, Cython, Numba for hot paths)

**Learning outcomes:** Profile a Pandas pipeline and identify the copy overhead. Replace a Python UDF with a vectorized NumPy operation. Explain why PyArrow enables PySpark Pandas UDFs to be fast. Implement a Polars lazy pipeline.  
**Interview relevance:** Medium — Pandas optimization is a common take-home topic  
**Production relevance:** Critical — daily for data engineers who write Python pipelines

---

### PRG-SE-101 — Software Engineering for Data

**Semester:** 2  
**Why it exists:** Data pipelines are software. Engineers who don't test, version, document, or review their code produce pipelines that fail silently. This course covers the software engineering practices specific to data work.  
**Prerequisites:** PRG-PY-101  
**Modules:**
- M01: Testing Data Pipelines (unit testing transforms, integration testing I/O, `pytest`, `great_expectations`)
- M02: Type Systems (Python typing, mypy, Pydantic for data validation)
- M03: Design Patterns for Data (strategy, factory, decorator — as applied to pipeline components)
- M04: Code Review for Data Engineers (what to look for, how to give useful feedback)
- M05: Structured Logging (structlog, log levels, correlation IDs, log aggregation)

**Learning outcomes:** Write a pytest suite that tests a data transformation without hitting a database. Use mypy to catch a type error in a pipeline before it runs. Add structured logging with correlation IDs to a multi-step pipeline.  
**Interview relevance:** Medium — software craft questions at Senior+ level  
**Production relevance:** Critical — pipeline reliability depends on testing

---

## School 5: Data Engineering (DE)

### DE-PDA-101 — Pipeline Design and Architecture

**Semester:** 3  
**Why it exists:** Knowing individual tools is not enough. This course teaches how to design a pipeline from requirements: choosing batch vs streaming, designing idempotent pipelines, handling schema evolution, and reasoning about SLAs.  
**Prerequisites:** PRG-PY-103, DBE-SQL-101  
**Modules:**
- M01: ELT vs ETL (trade-offs, when each applies, how the modern data stack changed the decision)
- M02: Idempotency Design (how to make any pipeline safe to re-run)
- M03: Batch Pipeline Patterns (full load vs incremental, watermarking, bookmarks)
- M04: Schema Evolution Strategies (additive vs breaking changes, migration strategies)
- M05: SLAs and SLOs for Pipelines (defining freshness, completeness, correctness SLOs)

**Learning outcomes:** Design an idempotent incremental pipeline for a given source. Define freshness and completeness SLOs for a pipeline. Explain forward- and backward-compatible schema evolution strategies.  
**Interview relevance:** High — pipeline design is a standard DE interview topic  
**Production relevance:** Critical — every pipeline needs an idempotency design

---

### DE-PDA-102 — Data Lake Architecture

**Semester:** 3  
**Why it exists:** Data lakes are the foundation of the modern data stack. Understanding how to organize them (medallion architecture), what storage format to use, and how to partition data for query performance is foundational.  
**Prerequisites:** DE-PDA-101, DBE-INT-101  
**Modules:**
- M01: Bronze/Silver/Gold Architecture (contracts between layers, schema enforcement, quality gates)
- M02: Partitioning Strategies (by date, by key, by hash — and query implications of each)
- M03: File Format Selection (Parquet vs ORC vs Avro — when to use each)
- M04: Compaction and Small File Problem (why small files kill performance and how to fix it)
- M05: Data Lake Governance (cataloging, lineage, access control)

**Learning outcomes:** Design a medallion architecture for a given domain. Choose a partitioning strategy for a given query access pattern. Implement a compaction job for a small-file-infested partition.  
**Interview relevance:** High — medallion architecture appears in every DE interview  
**Production relevance:** Critical — every data lake needs an explicit architecture

---

### DE-MOD-101 — Dimensional Modeling

**Semester:** 3  
**Why it exists:** The data warehouse is still the most widely deployed analytical store. Dimensional modeling is the dominant design methodology. An engineer who cannot design a star schema cannot design a data warehouse.  
**Prerequisites:** DBE-SQL-102  
**Modules:**
- M01: Kimball's Dimensional Modeling (fact tables, dimension tables, star vs snowflake)
- M02: Slowly Changing Dimensions (SCD Types 0–7, when to use each)
- M03: Fact Table Design (granularity, additive vs semi-additive vs non-additive measures)
- M04: Bridge Tables and Many-to-Many Relationships
- M05: Dimensional Modeling in Practice (designing the schema for a given business domain)

**Learning outcomes:** Design a star schema for a retail domain. Implement SCD Type 2 with correct effective/expiry dates. Explain the difference between a transactional fact table and an accumulating snapshot fact table.  
**Interview relevance:** High — the most commonly tested data modeling topic  
**Production relevance:** Critical — used daily in data warehouse and dbt projects

---

### DE-MOD-102 — Modern Data Modeling

**Semester:** 3  
**Why it exists:** The modern data stack challenges Kimball's assumptions. Wide tables (OBT), Data Vault for auditability, data contracts for team interfaces — these are the patterns in production at fast-moving companies.  
**Prerequisites:** DE-MOD-101  
**Modules:**
- M01: Data Vault Modeling (hubs, links, satellites — when it replaces Kimball and why)
- M02: One Big Table (OBT) Anti-Pattern and When It Is Actually Correct
- M03: Data Contracts (defining schemas as team interfaces, versioning contracts)
- M04: Schema Registry and Compatibility (Avro/Protobuf schemas, backward/forward compatibility)
- M05: Activity Schema (an emerging pattern for behavioral analytics)

**Learning outcomes:** Design a Data Vault model for a given source system. Write a data contract for a Silver layer output. Configure Avro schema backward compatibility in Confluent Schema Registry.  
**Interview relevance:** Medium — Data Vault is tested at Senior+ level  
**Production relevance:** Important — relevant in enterprise data engineering contexts

---

### DE-ORC-101 — Orchestration with Apache Airflow

**Semester:** 4  
**Why it exists:** Airflow is the dominant pipeline orchestrator. Understanding its architecture — scheduler, executor, metadata database, task states — is required to run it in production without reliability problems.  
**Prerequisites:** DE-PDA-101, PRG-PY-102  
**Modules:**
- M01: Airflow Architecture (scheduler loop, executor model, metadata DB, DAG serialization)
- M02: DAG Design Patterns (dynamic DAGs, task groups, dependencies, branching)
- M03: Executor Types (LocalExecutor, CeleryExecutor, KubernetesExecutor — trade-offs)
- M04: Airflow in Production (HA scheduler, connection pools, alerting, task retries)
- M05: Testing Airflow DAGs (unit tests for tasks, integration tests, CI/CD for DAGs)

**Learning outcomes:** Explain what the Airflow scheduler does in one loop iteration. Design a DAG with correct dependencies and retry logic. Choose between CeleryExecutor and KubernetesExecutor for a given scale. Write a unit test for an Airflow task.  
**Interview relevance:** High — Airflow architecture is in most DE interviews  
**Production relevance:** Critical — on-call for Airflow is a common data engineer responsibility

---

### DE-TRF-101 — Data Transformation with dbt

**Semester:** 4  
**Why it exists:** dbt is the standard tool for SQL-based data transformation. Understanding dbt's materialization strategies, test framework, and CI/CD integration is required for a modern data engineering role.  
**Prerequisites:** DBE-SQL-102, DE-MOD-101  
**Modules:**
- M01: dbt Architecture (models, materializations: table/view/incremental/snapshot)
- M02: dbt Testing (schema tests, data tests, custom generic tests, `dbt-expectations`)
- M03: dbt Macros and Jinja (macros, packages, hooks, cross-database compatibility)
- M04: dbt Incremental Models (unique key strategies, merge behavior, late-arriving data)
- M05: dbt in CI/CD (slim CI, deferral, GitHub Actions integration, environment management)

**Learning outcomes:** Design an incremental dbt model with correct unique key logic. Write a custom generic test. Set up dbt slim CI in GitHub Actions. Explain the difference between `unique_key` and `merge_update_columns`.  
**Interview relevance:** High — dbt is now a baseline expectation at most companies  
**Production relevance:** Critical — dbt is daily work for most data engineers

---

## School 6: Distributed Computing Systems (DCS)

### DCS-SPK-101 — Apache Spark Architecture

**Semester:** 4  
**Why it exists:** Spark is the dominant distributed batch processing engine. Understanding its architecture — Driver, Executors, DAG, stages, shuffle — is required to debug, tune, and design Spark jobs correctly.  
**Prerequisites:** SYS-DST-101, PRG-PY-102  
**Modules:**
- M01: Spark Architecture (Driver, Cluster Manager, Executors, Task Scheduler)
- M02: RDD and DataFrame Internals (transformations vs actions, lazy evaluation, DAG)
- M03: Spark's Execution Model (stages, tasks, shuffle boundaries, pipelining)
- M04: Catalyst Optimizer (logical plan → analyzed plan → optimized plan → physical plan)
- M05: Tungsten Execution Engine (Whole-Stage Code Generation, vectorized readers)

**Learning outcomes:** Draw the Spark execution model from memory. Explain what creates a stage boundary. Explain what Catalyst's physical planning does. Explain Whole-Stage Code Generation and why it matters.  
**Interview relevance:** High — Spark architecture is the most common DE interview topic after SQL  
**Production relevance:** Critical — understanding the architecture is prerequisite to every Spark job

---

### DCS-SPK-102 — Spark Performance Engineering

**Semester:** 4  
**Why it exists:** Most Spark jobs in production are slower than necessary. This course teaches systematic performance engineering: measuring, diagnosing, and fixing Spark performance problems.  
**Prerequisites:** DCS-SPK-101  
**Modules:**
- M01: Spark UI Deep Dive (reading the Spark UI to identify bottlenecks, stage details, task metrics)
- M02: Data Skew (detecting skew, salting, adaptive query execution, skew join optimization)
- M03: Memory Management (executor memory layout, off-heap, serialization)
- M04: Shuffle Optimization (partition count tuning, AQE, broadcast joins, sort-merge optimization)
- M05: Reading Physical Plans (`explain(mode="extended")` — cost-based optimizer, join selection)

**Learning outcomes:** Identify a data skew problem from the Spark UI. Apply salting to a skewed join. Tune executor memory configuration for a given job. Explain what AQE does and what problems it solves.  
**Interview relevance:** High — "How do you tune a slow Spark job?" is a canonical question  
**Production relevance:** Critical — Spark tuning is regular work for data engineers

---

### DCS-PYS-101 — PySpark and Structured Streaming

**Semester:** 5  
**Why it exists:** PySpark is how most data engineers write Spark. Understanding the performance implications of Python UDFs, the Arrow-based Pandas UDF, and Structured Streaming is essential.  
**Prerequisites:** DCS-SPK-102, PRG-PY-103  
**Modules:**
- M01: Python UDF vs Pandas UDF vs SQL (performance, serialization overhead, when to use each)
- M02: Structured Streaming (trigger modes, checkpointing, watermarks, late data handling)
- M03: Delta Lake from PySpark (ACID transactions, MERGE, time travel, schema enforcement)
- M04: PySpark Testing (unit testing transformations, mock data, `chispa`)
- M05: PySpark in Production (packaging, dependency management, job submission, monitoring)

**Learning outcomes:** Implement the same transformation as a Python UDF, Pandas UDF, and SQL expression — and benchmark all three. Write a Structured Streaming job with a watermark. Implement a MERGE with Delta Lake. Package and submit a PySpark job to a cluster.  
**Interview relevance:** High — PySpark is the working language of most Spark data engineers  
**Production relevance:** Critical — daily tool

---

### DCS-STR-101 — Stream Processing Theory

**Semester:** 4  
**Why it exists:** Streaming is not just fast batch. Event time, watermarks, exactly-once, and out-of-order data are concepts that do not exist in batch. Engineers who don't understand them will produce incorrect streaming pipelines.  
**Prerequisites:** SYS-DST-101, SYS-NET-101  
**Modules:**
- M01: The Dataflow Model (event time vs processing time, the fundamental paper)
- M02: Windowing (tumbling, sliding, session — semantics and trade-offs)
- M03: Watermarks (how they work, late data policy, idling sources)
- M04: Exactly-Once Semantics (idempotent producers, transactional consumers, 2PC in streaming)
- M05: Out-of-Order Data Handling (late events, retractions, accumulation modes)

**Learning outcomes:** Explain the difference between event time and processing time with a concrete example. Explain how a watermark is computed. Implement a streaming aggregation with a watermark in PySpark or Flink. Explain exactly-once end-to-end in a Kafka → Flink → Kafka topology.  
**Interview relevance:** High — streaming theory is tested at Senior+ level  
**Production relevance:** Critical — correctness of streaming pipelines depends on this

---

### DCS-STR-102 — Stream Processing in Production

**Semester:** 5  
**Why it exists:** Running streaming in production requires understanding Flink, Spark Structured Streaming, and the operational concerns that differ from batch.  
**Prerequisites:** DCS-STR-101, DCS-PYS-101  
**Modules:**
- M01: Apache Flink Architecture (JobManager, TaskManager, checkpointing, savepoints)
- M02: Flink vs Spark Structured Streaming (when to choose each)
- M03: Streaming Pipeline Operational Patterns (backpressure, consumer lag, dead-letter topics)
- M04: Streaming Observability (consumer lag dashboard, checkpoint duration alerting)
- M05: Stateful Streaming (state backends, state TTL, keyed state patterns)

**Learning outcomes:** Explain Flink checkpointing. Choose between Flink and Structured Streaming for a given latency requirement. Implement a dead-letter topic pattern. Monitor consumer lag in Prometheus.  
**Interview relevance:** High — streaming production operations are tested at Senior+  
**Production relevance:** Critical — on-call for streaming systems requires this

---

### DCS-KFK-101 — Apache Kafka Internals

**Semester:** 4  
**Why it exists:** Kafka is the most widely deployed event streaming platform. Understanding its architecture — partitions, replicas, ISR, consumer groups, KRaft — is required to configure it correctly and debug it when things go wrong.  
**Prerequisites:** SYS-DST-101, SYS-NET-101  
**Modules:**
- M01: Kafka Architecture (brokers, topics, partitions, replicas, ZooKeeper and KRaft)
- M02: Producer Internals (batching, linger.ms, acks, idempotent producer, transactions)
- M03: Consumer Groups (group coordinator, partition assignment, rebalancing, committed offsets)
- M04: ISR and Replication (ISR, min.insync.replicas, leader election, replica lag)
- M05: Log Compaction and Retention (log segments, compaction policy, time and size retention)

**Learning outcomes:** Explain what ISR is and what happens when it shrinks. Configure a producer for exactly-once delivery. Explain consumer group rebalancing and why it causes consumer lag. Explain log compaction and when to use it.  
**Interview relevance:** High — Kafka internals are tested at Senior+ DE interviews  
**Production relevance:** Critical — production Kafka requires internals knowledge for on-call

---

### DCS-KFK-102 — Kafka Ecosystem and Operations

**Semester:** 5  
**Why it exists:** Kafka does not work in isolation. Schema Registry, Kafka Connect, Kafka Streams, and security are the ecosystem tools that make Kafka production-grade.  
**Prerequisites:** DCS-KFK-101  
**Modules:**
- M01: Schema Registry (Avro/Protobuf schemas, compatibility modes, subject naming strategies)
- M02: Kafka Connect (connector framework, source/sink connectors, SMTs, error handling)
- M03: Kafka Streams (KStream, KTable, GlobalKTable, DSL vs Processor API)
- M04: Kafka Security (SASL/SCRAM, mTLS, ACLs, encryption at rest)
- M05: Kafka Operations (cluster expansion, partition reassignment, throttling, monitoring)

**Learning outcomes:** Configure Schema Registry with backward compatibility. Deploy a Debezium CDC connector. Implement a simple Kafka Streams topology. Add SASL authentication to a Kafka cluster. Perform a partition reassignment without downtime.  
**Interview relevance:** High — Kafka Connect and Schema Registry are daily tools  
**Production relevance:** Critical — production Kafka deployments require the full ecosystem

---

## School 7: Cloud Platform & Leadership (CPL)

### CPL-CLD-101 — BigQuery Architecture and Optimization

**Semester:** 5  
**Why it exists:** BigQuery is the dominant cloud data warehouse. Understanding Dremel, Colossus, and the Capacitor file format explains why BigQuery behaves differently from Postgres and how to optimize for it.  
**Prerequisites:** DBE-SQL-102, DCS-SPK-101  
**Modules:**
- M01: BigQuery Architecture (Dremel query execution, Colossus storage, Jupiter network)
- M02: BigQuery Storage Optimization (partitioning, clustering, materialized views)
- M03: BigQuery IAM and Security (column-level security, row-level security, VPC Service Controls)
- M04: BigQuery Cost Management (bytes billed, slot-based vs on-demand, reservations, INFORMATION_SCHEMA)
- M05: BigQuery Performance Debugging (query plan, slot contention, shuffle spill)

**Learning outcomes:** Explain why BigQuery doesn't have indexes (Dremel-based execution). Choose between partitioning and clustering for a given query access pattern. Calculate the cost of a given query and reduce it by ≥50%. Debug a slow BigQuery query using the query plan.  
**Interview relevance:** High — BigQuery appears in most GCP DE interviews  
**Production relevance:** Critical — BigQuery cost management is a daily concern

---

### CPL-CLD-102 — Open Table Formats

**Semester:** 5  
**Why it exists:** Apache Iceberg, Delta Lake, and Apache Hudi have replaced Hive-style Parquet-on-S3 as the modern data lake storage layer. Understanding their file formats, ACID guarantees, and trade-offs is required for modern data lakehouse design.  
**Prerequisites:** DBE-INT-101, DCS-PYS-101  
**Modules:**
- M01: Apache Iceberg (table format, metadata tree, snapshot isolation, time travel)
- M02: Delta Lake (transaction log, checkpoints, MERGE, schema enforcement)
- M03: Apache Hudi (Copy-on-Write vs Merge-on-Read, clustering, indexing)
- M04: Choosing a Table Format (comparison matrix, migration strategies)
- M05: Table Format Operations (compaction, vacuum, snapshot expiry, schema evolution)

**Learning outcomes:** Explain the Iceberg metadata tree (manifest list → manifest files → data files). Compare Iceberg, Delta Lake, and Hudi on ACID guarantees, read performance, and write amplification. Design a migration from Hive-style to Iceberg. Implement table maintenance (compaction, snapshot expiry).  
**Interview relevance:** High — table format questions are increasingly common  
**Production relevance:** Critical — table formats are now the default for new data lakes

---

### CPL-CLD-103 — Multi-Cloud Data Architecture

**Semester:** 5  
**Why it exists:** Data engineering teams increasingly run on multiple clouds or need to reason about cloud vendor lock-in. Understanding the data services on AWS, GCP, and Azure — and the trade-offs between them — is a Staff-level skill.  
**Prerequisites:** CPL-CLD-101, DCS-KFK-102  
**Modules:**
- M01: AWS Data Stack (S3, Glue, EMR, Kinesis, MSK, Redshift, Athena)
- M02: GCP Data Stack (GCS, Dataflow, Dataproc, Pub/Sub, BigQuery, Looker)
- M03: Azure Data Stack (ADLS, Databricks, Event Hubs, Synapse)
- M04: Cloud Cost Architecture (RI vs Spot, egress costs, cross-cloud data gravity)
- M05: Avoiding Lock-In (open formats, managed vs self-managed trade-offs)

**Learning outcomes:** Map each GCP service to its AWS equivalent. Estimate cross-cloud data transfer costs for a given data volume. Explain data gravity and why it matters for cloud architecture. Design a cloud-agnostic data lake using open formats.  
**Interview relevance:** Medium — multi-cloud is Staff-level  
**Production relevance:** Important — relevant for organizations with multiple cloud commitments

---

### CPL-PRD-101 — Production Data Engineering

**Semester:** 5  
**Why it exists:** A data pipeline that works in development but fails silently in production is worthless. This course teaches the operational engineering required to own data pipelines in production: SLOs, observability, incident management, and data quality monitoring.  
**Prerequisites:** All prior schools  
**Modules:**
- M01: SLI/SLO/SLA Framework (defining and measuring freshness, completeness, correctness)
- M02: Observability Stack (Prometheus metrics, Grafana dashboards, structured logging, alerting)
- M03: Distributed Tracing (OpenTelemetry for data pipelines, trace context propagation)
- M04: Data Quality Monitoring (anomaly detection, data contracts, Great Expectations in production)
- M05: Incident Management (on-call, runbooks, postmortems, blameless culture)

**Learning outcomes:** Define SLOs for a given pipeline. Set up a Prometheus alert for pipeline freshness. Write a runbook for a given failure scenario. Lead a blameless post-mortem. Implement Great Expectations in a production dbt pipeline.  
**Interview relevance:** High — production ownership is tested at Senior+ level  
**Production relevance:** Critical — daily work for engineers who are on-call

---

### CPL-PRD-102 — CI/CD and Data Platform Engineering

**Semester:** 6  
**Why it exists:** The gap between "writes pipelines" and "owns a data platform" is CI/CD, infrastructure-as-code, and deployment automation. This course closes that gap.  
**Prerequisites:** CPL-PRD-101, PRG-SE-101  
**Modules:**
- M01: CI/CD for Data (GitHub Actions, dbt CI, Spark job CI, integration test patterns)
- M02: Infrastructure as Code (Terraform for data infrastructure — BigQuery datasets, Kafka topics)
- M03: Secrets Management (Vault, cloud secrets managers, rotation, pipeline integration)
- M04: Blue-Green and Canary Deployments for Data Pipelines
- M05: Platform Engineering for Data (self-service data platform design, developer experience)

**Learning outcomes:** Build a GitHub Actions CI pipeline for a dbt project. Write Terraform to provision a BigQuery dataset with correct IAM. Implement a blue-green deployment for a batch pipeline. Design a self-service data platform for a team of 20 engineers.  
**Interview relevance:** High — platform engineering is tested at Staff level  
**Production relevance:** Critical — platform ownership requires this

---

### CPL-SYD-101 — Data System Design

**Semester:** 6  
**Why it exists:** System design interviews are the final barrier to Senior and Staff roles. This course teaches the frameworks, vocabulary, and practice required to lead a 45-minute system design session.  
**Prerequisites:** All prior schools  
**Modules:**
- M01: System Design Framework (requirements → capacity → architecture → trade-offs → failure modes)
- M02: Design: Real-Time Analytics Platform (Kafka → Flink → BigQuery → Grafana)
- M03: Design: Batch Data Warehouse (Airflow → Spark → dbt → BigQuery)
- M04: Design: Stream-Batch Unified Platform (Lakehouse, Delta Lake, unified APIs)
- M05: Design: Enterprise Data Platform (governance, access control, multi-team, cost attribution)

**Learning outcomes:** Lead a 45-minute system design session without notes. Produce a written architecture document for a greenfield data platform. Evaluate a proposed architecture for failure modes and trade-offs.  
**Interview relevance:** High — the most important interview skill at Senior+ level  
**Production relevance:** Critical — Staff engineers design systems, not just pipelines

---

### CPL-SYD-102 — Data Architecture Patterns

**Semester:** 6  
**Why it exists:** Lambda architecture, Kappa architecture, data mesh, and lakehouse are the major paradigms. Understanding when each applies — and when it doesn't — is what separates Staff engineers from Senior engineers.  
**Prerequisites:** CPL-SYD-101  
**Modules:**
- M01: Lambda Architecture (batch layer + speed layer + serving layer — and why it was abandoned)
- M02: Kappa Architecture (streaming-first, replayability, and its limits)
- M03: Lakehouse Architecture (open table formats, unified compute, and its limits)
- M04: Data Mesh (domain ownership, data as a product, federated governance)
- M05: Choosing an Architecture (decision framework for a given organizational context)

**Learning outcomes:** Explain why Lambda architecture was replaced by Kappa for most use cases. Explain what problem data mesh solves (organizational, not technical). Recommend an architecture for a given company size and data maturity level.  
**Interview relevance:** High — architecture pattern questions appear at Staff+ level  
**Production relevance:** Important — required for architectural decisions, not daily work

---

### CPL-LDR-101 — Interview Preparation

**Semester:** 6  
**Why it exists:** Technical knowledge and interview performance are different skills. This course teaches interview-specific skills: SQL under time pressure, system design under ambiguity, behavioral storytelling.  
**Prerequisites:** All prior schools  
**Modules:**
- M01: SQL Interview Workbook (50 problems, timed, with model solutions)
- M02: Python + DSA Interview Workbook (30 coding problems with data engineering context)
- M03: Spark Interview Scenarios (20 debugging and tuning scenarios)
- M04: System Design Practice (10 problems with worked solutions and common follow-ups)
- M05: Behavioral Interviews (STAR framework, 30 DE-specific behavioral questions)

**Learning outcomes:** Solve a medium SQL problem in under 10 minutes. Lead a system design session with minimal prompting. Tell 5 STAR stories covering: technical ambiguity, cross-functional conflict, architectural disagreement, production incident, mentorship.  
**Interview relevance:** High — this is the interview course  
**Production relevance:** Background — interview skills have limited production relevance

---

### CPL-LDR-102 — Technical Leadership

**Semester:** 6  
**Why it exists:** Staff and Principal data engineers spend more time on architecture, mentorship, documentation, and influence than on coding. This course teaches those skills explicitly.  
**Prerequisites:** CPL-SYD-102  
**Modules:**
- M01: Architecture Decision Records (writing ADRs, driving consensus, managing dissent)
- M02: RFC Process (writing and reviewing Requests for Comments, async technical decision-making)
- M03: Technical Mentorship (frameworks, common traps, giving useful feedback)
- M04: Influence Without Authority (driving technical change across teams you don't own)
- M05: Technical Strategy (writing strategy documents, roadmaps, technology radars)

**Learning outcomes:** Write an ADR for a real or hypothetical technology decision. Facilitate a technical design review for a system you did not design. Write a 1-page technical strategy document for a data platform.  
**Interview relevance:** High — leadership signals are the primary differentiator at Staff+ interviews  
**Production relevance:** Critical — Staff+ engineers operate in this mode daily

---

## Course Summary

| Code | Course Name | School | Semester | Modules |
|---|---|---|---|---|
| CSF-ARC-101 | How Computers Execute Programs | CSF | 0 | 5 |
| CSF-ARC-102 | Memory Architecture | CSF | 0 | 5 |
| CSF-ALG-101 | Algorithms and Data Structures | CSF | 0 | 5 |
| CSF-OS-101 | OS Internals | CSF | 0 | 5 |
| SYS-LNX-101 | Linux for Data Engineers | SYS | 1 | 5 |
| SYS-LNX-102 | Linux Performance Engineering | SYS | 1 | 5 |
| SYS-NET-101 | Networking for Data Engineers | SYS | 1 | 5 |
| SYS-NET-102 | Cloud Networking and Security | SYS | 1 | 5 |
| SYS-DST-101 | Distributed Systems Theory | SYS | 2 | 5 |
| SYS-DST-102 | Distributed Systems in Practice | SYS | 2 | 5 |
| DBE-INT-101 | Storage Engine Internals | DBE | 2 | 5 |
| DBE-INT-102 | Postgres Architecture | DBE | 2 | 5 |
| DBE-INT-103 | Transaction Isolation and Concurrency | DBE | 2 | 5 |
| DBE-SQL-101 | SQL Internals and Query Optimization | DBE | 3 | 5 |
| DBE-SQL-102 | Advanced Analytical SQL | DBE | 3 | 5 |
| PRG-PY-101 | Python Internals | PRG | 1 | 5 |
| PRG-PY-102 | Python Concurrency | PRG | 1 | 5 |
| PRG-PY-103 | Python Data Libraries | PRG | 2 | 5 |
| PRG-SE-101 | Software Engineering for Data | PRG | 2 | 5 |
| DE-PDA-101 | Pipeline Design and Architecture | DE | 3 | 5 |
| DE-PDA-102 | Data Lake Architecture | DE | 3 | 5 |
| DE-MOD-101 | Dimensional Modeling | DE | 3 | 5 |
| DE-MOD-102 | Modern Data Modeling | DE | 3 | 5 |
| DE-ORC-101 | Orchestration with Apache Airflow | DE | 4 | 5 |
| DE-TRF-101 | Data Transformation with dbt | DE | 4 | 5 |
| DCS-SPK-101 | Apache Spark Architecture | DCS | 4 | 5 |
| DCS-SPK-102 | Spark Performance Engineering | DCS | 4 | 5 |
| DCS-PYS-101 | PySpark and Structured Streaming | DCS | 5 | 5 |
| DCS-STR-101 | Stream Processing Theory | DCS | 4 | 5 |
| DCS-STR-102 | Stream Processing in Production | DCS | 5 | 5 |
| DCS-KFK-101 | Apache Kafka Internals | DCS | 4 | 5 |
| DCS-KFK-102 | Kafka Ecosystem and Operations | DCS | 5 | 5 |
| CPL-CLD-101 | BigQuery Architecture and Optimization | CPL | 5 | 5 |
| CPL-CLD-102 | Open Table Formats | CPL | 5 | 5 |
| CPL-CLD-103 | Multi-Cloud Data Architecture | CPL | 5 | 5 |
| CPL-PRD-101 | Production Data Engineering | CPL | 5 | 5 |
| CPL-PRD-102 | CI/CD and Data Platform Engineering | CPL | 6 | 5 |
| CPL-SYD-101 | Data System Design | CPL | 6 | 5 |
| CPL-SYD-102 | Data Architecture Patterns | CPL | 6 | 5 |
| CPL-LDR-101 | Interview Preparation | CPL | 6 | 5 |
| CPL-LDR-102 | Technical Leadership | CPL | 6 | 5 |

**Total: 41 courses × 5 modules = 205 modules (revised from earlier estimate of 35 courses due to course count reconciliation with `01_UNIVERSITY_HIERARCHY.md`)**
