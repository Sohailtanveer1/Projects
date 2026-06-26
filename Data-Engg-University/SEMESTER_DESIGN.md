# Data Engineering University — Semester Design

The university is structured as a six-semester program. Each semester has explicit goals, required prior knowledge, skills gained, a capstone project, and graduation criteria. Semesters are not time-locked — progress at your own pace — but they are *dependency-locked*: you cannot start a semester until you can demonstrate the graduation criteria of the previous one.

---

## Semester 0 — Foundation Sciences

**Tagline:** *Before you can build systems, you must understand machines.*

### Goals
Understand what a computer actually does when code runs. Build the mental model that makes every future performance conversation concrete rather than abstract.

### Required Prior Knowledge
- Any programming language at "hello world + loops + functions" level.
- No data engineering experience required.

### Modules

| School | Module | Why It's Here |
|---|---|---|
| 00 CS Foundations | M01 How Computers Execute Programs | The root of everything. All performance, all debugging traces back here. |
| 00 CS Foundations | M02 Memory and Storage Hierarchy | Cache lines, RAM, NVMe — the latency ladder every DE needs memorized. |
| 00 CS Foundations | M03 Concurrency and Parallelism | Threads, GIL, race conditions — you will hit all of these in Spark and Python. |
| 00 CS Foundations | M04 Networking Fundamentals | Sockets, TCP/IP — every pipeline is a distributed system. |
| 00 CS Foundations | M05 OS Internals | Kernel, syscalls, virtual memory — what Linux is actually doing when Spark runs. |

### Skills Gained
- Read and interpret CPU flame graphs.
- Explain cache miss rates and their effect on query performance.
- Distinguish CPU-bound vs I/O-bound vs network-bound workloads.
- Predict whether adding more cores will help or not (Amdahl's Law).
- Explain the Python GIL and its practical consequences.

### Dependencies
None. This is the root semester.

### Recommended Study Order
00-M01 → 00-M02 → 00-M03 → 00-M04 → 00-M05

### Capstone Project
**"Profile Before You Optimize"**

Given a Python script that processes 10 million rows (provided), the student must:
1. Run it unmodified and record baseline runtime.
2. Use `perf stat` and `cProfile` to identify the bottleneck.
3. Classify the bottleneck as CPU-bound, memory-bound, or I/O-bound with evidence.
4. Apply one targeted optimization from the module (vectorization, sequential memory access, or reduced syscall overhead).
5. Re-benchmark and quantify the improvement.
6. Write a one-page technical explanation of *why* the optimization worked, citing specific concepts from the modules (cache lines, pipeline stalls, interpreter dispatch overhead, etc.).

### Graduation Criteria
A student graduates Semester 0 when they can:
- [ ] Explain the fetch-decode-execute cycle from first principles, with no notes, in under 2 minutes.
- [ ] Draw the memory latency ladder (registers → L1 → L2 → L3 → RAM → SSD → HDD → network) with correct order-of-magnitude numbers.
- [ ] Explain why `numpy.sum()` is ~150× faster than an equivalent Python loop.
- [ ] Explain what a cache line is and why columnar storage formats are cache-friendly.
- [ ] Describe the Python GIL and two strategies for working around it.
- [ ] Complete the capstone project with a measured, explained speedup of ≥2×.

---

## Semester 1 — Operating Systems and Networking

**Tagline:** *The operating system is the data engineer's runtime. Know your runtime.*

### Goals
Master Linux as a working environment and diagnostic tool. Understand network protocols well enough to debug latency, configure firewalls, and reason about distributed system communication.

### Required Prior Knowledge
- Semester 0 graduation criteria met.
- Comfortable with a terminal (can navigate directories, edit files with vim/nano, run scripts).

### Modules

| School | Module | Why It's Here |
|---|---|---|
| 01 Linux | M01 Linux Architecture | Kernel space vs user space — what happens when Spark makes a syscall. |
| 01 Linux | M02 File System and IO | inodes, page cache, O_DIRECT, fsync — critical for understanding Kafka and databases. |
| 01 Linux | M03 Processes and Signals | fork/exec, signals, zombie processes — Spark worker lifecycle. |
| 01 Linux | M04 Shell Scripting | Automation, cron, pipes — every DE writes shell scripts in production. |
| 01 Linux | M05 Performance Tools | perf, strace, iostat, vmstat — diagnostic arsenal for on-call incidents. |
| 02 Networking | M01 OSI and TCP/IP | Protocol stack — why network latency is what it is. |
| 02 Networking | M02 DNS and HTTP | DNS TTL failures, HTTP timeouts — real production issues. |
| 02 Networking | M03 Load Balancers and Proxies | L4 vs L7, connection pooling — how data services scale. |
| 02 Networking | M04 Network Security | TLS, mTLS, VPC, firewall rules — production security baseline. |

### Skills Gained
- Diagnose a slow Spark job using `top`, `iostat`, `strace`, and `perf`.
- Write a shell script that automates pipeline deployment with proper error handling.
- Explain what happens during a TCP connection from a Kafka producer to a broker.
- Configure basic firewall rules and understand VPC network isolation.
- Explain the Linux page cache and how it affects Kafka and database I/O.

### Dependencies
- Requires: Semester 0 (all modules).

### Recommended Study Order
01-M01 → 01-M02 → 01-M03 → 01-M04 → 02-M01 → 02-M02 → 01-M05 → 02-M03 → 02-M04

(Performance tools after processes; networking security after networking basics.)

### Capstone Project
**"Debug a Broken Pipeline on Linux"**

A provided Docker container runs a pipeline that is failing intermittently. The student must:
1. Use `strace`, `lsof`, `netstat`, and `perf` to diagnose the failure.
2. Identify whether the failure is: a file descriptor leak, a network timeout, a signal handling bug, or a disk I/O bottleneck.
3. Fix the root cause (it is one of these four).
4. Write a post-mortem document explaining: what broke, why, how you found it, and how you fixed it.

### Graduation Criteria
A student graduates Semester 1 when they can:
- [ ] Explain what happens between `fork()` and the first user instruction in a new process.
- [ ] Describe how the Linux page cache interacts with `write()` and `fsync()`.
- [ ] Use `strace` to trace a process's system calls and identify a file descriptor leak.
- [ ] Explain the TCP three-way handshake and TIME_WAIT state.
- [ ] Write a shell script with proper error handling (`set -euo pipefail`, traps, logging).
- [ ] Explain the difference between L4 and L7 load balancing and when you'd use each.
- [ ] Complete the capstone project and write a complete post-mortem.

---

## Semester 2 — Databases and Distributed Systems Theory

**Tagline:** *All data systems are distributed systems. All distributed systems fail. Know why.*

### Goals
Build the theoretical foundation for understanding every database and distributed data system. CAP theorem, consensus, replication, and ACID are not abstract — they determine what happens to your data when a node crashes at 3 AM.

### Required Prior Knowledge
- Semester 1 graduation criteria met.
- Comfortable with SQL at "SELECT with JOINs and GROUP BY" level.

### Modules

| School | Module | Why It's Here |
|---|---|---|
| 04 Databases | M01 Storage Engines | B-tree vs LSM-tree — why Postgres and Cassandra make different trade-offs. |
| 04 Databases | M02 Indexing | B-tree, hash, bitmap indexes — why some queries are fast and others aren't. |
| 04 Databases | M03 Query Execution | Parser → planner → executor — what happens when you press Enter on a SQL query. |
| 04 Databases | M04 ACID and Isolation Levels | Atomicity, MVCC, serializable isolation — what "safe" actually means. |
| 04 Databases | M05 Replication and Sharding | WAL shipping, logical replication, sharding strategies — how databases scale. |
| 03 Distributed Systems | M01 CAP Theorem | Consistency vs Availability — the fundamental trade-off of every distributed system. |
| 03 Distributed Systems | M02 Consensus and Raft | Leader election, log replication — how Kafka, ZooKeeper, and etcd work internally. |
| 03 Distributed Systems | M03 Replication and Partitioning | Leader-follower, hash vs range partitioning — Kafka and Spark shuffle. |
| 03 Distributed Systems | M04 Distributed Transactions | 2PC, Saga pattern — why cross-service transactions are hard. |
| 03 Distributed Systems | M05 Observability | Structured logging, metrics, tracing — production visibility. |

### Skills Gained
- Choose the right database storage engine for a given workload (OLTP vs OLAP, read-heavy vs write-heavy).
- Read and interpret an SQL `EXPLAIN` plan.
- Explain MVCC and how it enables non-blocking reads.
- Describe the Raft consensus algorithm at a whiteboard interview level.
- Classify any distributed system by its CAP trade-off.
- Design a sharding strategy for a given access pattern.

### Dependencies
- Requires: Semester 0 (for storage engine internals — cache lines, I/O).
- Requires: Semester 1 (for replication internals — fsync, page cache, network).

### Recommended Study Order
04-M01 → 04-M02 → 03-M01 → 04-M03 → 04-M04 → 03-M02 → 03-M03 → 04-M05 → 03-M04 → 03-M05

### Capstone Project
**"Design a Multi-Region Postgres Cluster"**

On paper (architecture document + diagrams):
1. Design a primary-replica Postgres cluster across 2 AWS regions.
2. Specify: replication mode (synchronous vs asynchronous), failover strategy, RPO/RTO targets, and the trade-offs of your choices.
3. Choose a sharding key for a `user_events` table with 10 billion rows and justify it.
4. Identify 3 failure scenarios and describe how your design handles each.
5. Produce a Mermaid architecture diagram of the full system.

### Graduation Criteria
- [ ] Explain the difference between synchronous and asynchronous replication and the RPO/RTO implications of each.
- [ ] Draw a B-tree and an LSM-tree and explain the write amplification trade-off.
- [ ] Describe MVCC in 2 minutes from first principles.
- [ ] Explain Raft leader election on a whiteboard.
- [ ] Given a CAP scenario, correctly classify the system as CP or AP.
- [ ] Complete the capstone project and successfully defend your replication and sharding choices.

---

## Semester 3 — Core Data Engineering Stack

**Tagline:** *Now you build pipelines. Real ones, that handle real failures.*

### Goals
Master the primary tools of the data engineering profession. By the end of this semester you can design, build, test, and deploy a production-grade data pipeline.

### Required Prior Knowledge
- Semester 2 graduation criteria met.
- Python at intermediate level (functions, classes, file I/O, error handling).
- SQL at intermediate level (window functions, CTEs).

### Modules

| School | Module | Why It's Here |
|---|---|---|
| 06 SQL | M01 SQL Execution Internals | Know what the DB is doing with your query. |
| 06 SQL | M02 Window Functions | Essential for analytics engineering and interview questions. |
| 06 SQL | M03 Query Optimization | Statistics, predicate pushdown, join algorithms. |
| 06 SQL | M04 Advanced Joins | Hash join, merge join, anti-join, lateral. |
| 06 SQL | M05 Recursive CTEs | Tree traversal, graph queries — uncommon but impressive in interviews. |
| 07 Python | M01 Python Internals | CPython, bytecode, GIL — foundational for all Python performance work. |
| 07 Python | M02 Concurrency | asyncio, threading, multiprocessing — how to do parallel work in Python. |
| 07 Python | M03 Memory Management | gc, tracemalloc, generators — prevent leaks in long-running pipeline workers. |
| 07 Python | M04 Data Structures and Algorithms | Big-O, sorting, hashing — required for the coding interview. |
| 07 Python | M05 Python for Data Engineering | pandas, polars, pyarrow, boto3 — the practical toolkit. |
| 05 Data Engineering | M01 Batch vs Streaming | The first architectural decision in any pipeline design. |
| 05 Data Engineering | M02 Data Lake Architecture | Zones, formats, metadata — structure your lake before you fill it. |
| 05 Data Engineering | M03 ELT vs ETL | When to transform in the warehouse vs in flight. |
| 05 Data Engineering | M04 Medallion Architecture | Bronze/Silver/Gold — the standard layering pattern. |
| 05 Data Engineering | M05 Data Contracts | Schemas, SLAs, ownership — how pipelines stay consistent at scale. |

### Skills Gained
- Write and optimize complex SQL queries including window functions and recursive CTEs.
- Diagnose a Python memory leak in a long-running pipeline.
- Build a batch ELT pipeline with proper error handling, logging, and retry logic.
- Design a Bronze/Silver/Gold data lake on S3 or GCS.
- Write a data contract and explain what happens when a producer violates it.

### Dependencies
- Requires: Semester 2 (SQL execution requires query execution understanding from Databases).
- Requires: Semester 0 (Python internals require CPU/memory understanding).

### Recommended Study Order
07-M01 → 07-M04 → 06-M01 → 06-M02 → 07-M02 → 07-M03 → 06-M03 → 06-M04 → 06-M05 → 07-M05 → 05-M01 → 05-M02 → 05-M03 → 05-M04 → 05-M05

### Capstone Project
**"End-to-End Batch Pipeline: NYC Taxi Data"**

Using the NYC Taxi & Limousine Commission public dataset (available at nyc.gov):
1. Ingest raw CSVs into a Bronze layer on local storage or cloud (S3/GCS).
2. Clean and validate to Silver (remove nulls, enforce schema, add audit columns).
3. Aggregate to Gold (daily revenue by zone, average trip duration by hour, top 10 pickup locations).
4. Write all transformations in Python (pandas or polars) with unit tests.
5. Schedule the pipeline using a cron job or Airflow (basic DAG).
6. Write a data contract for the Silver layer's schema.
7. Introduce an intentional schema-breaking change in the input data and demonstrate how your contract catches it.

### Graduation Criteria
- [ ] Write a SQL query using RANK(), LAG(), and a CTE — correctly — in an interview setting (no IDE).
- [ ] Explain what `EXPLAIN ANALYZE` output means for a given query.
- [ ] Fix a Python memory leak using `tracemalloc` (demonstrated on a provided script).
- [ ] Explain the difference between ELT and ETL and give a concrete example of when each is appropriate.
- [ ] Complete the capstone project with all layers populated, tested, and documented.

---

## Semester 4 — Distributed Compute and Streaming

**Tagline:** *At scale, the single machine disappears. Your code runs on hundreds of them simultaneously.*

### Goals
Master Apache Spark and Apache Kafka — the two most dominant distributed data technologies. Understand them at the architecture level so you can tune them, debug them, and make principled decisions about when to use each.

### Required Prior Knowledge
- Semester 3 graduation criteria met.
- Comfortable writing Python at an intermediate level.
- Understand TCP/IP networking (Semester 1).
- Understand distributed systems theory (Semester 2).

### Modules

| School | Module | Why It's Here |
|---|---|---|
| 08 Apache Spark | M01 Spark Architecture | Driver, executor, cluster manager — you must know where your code runs. |
| 08 Apache Spark | M02 RDD vs DataFrame vs Dataset | Catalyst optimizer — why DataFrames are fast, RDDs are not. |
| 08 Apache Spark | M03 Spark Execution Engine | DAG, stages, tasks, wide vs narrow — the unit of work in Spark. |
| 08 Apache Spark | M04 Shuffle and Partitioning | The most common source of Spark performance problems. |
| 08 Apache Spark | M05 Spark Optimization | Broadcast join, AQE, caching, bucketing — the solutions. |
| 09 PySpark | M01 PySpark API | SparkSession, DataFrame API — the Python surface. |
| 09 PySpark | M02 PySpark UDFs | Row UDF vs Pandas UDF — performance consequences of your choice. |
| 09 PySpark | M03 Structured Streaming | Micro-batch vs continuous, watermarks, state store. |
| 09 PySpark | M04 Delta Lake with PySpark | ACID on object storage, time travel, DML. |
| 10 Streaming | M01 Stream Processing Fundamentals | Event time vs processing time — the core conceptual problem. |
| 10 Streaming | M02 Windowing and Watermarks | How to aggregate over time when events arrive late. |
| 10 Streaming | M03 Exactly-Once Semantics | What "exactly once" actually means and how to achieve it. |
| 10 Streaming | M04 Flink vs Spark Streaming | True streaming vs micro-batch — when the difference matters. |
| 11 Kafka | M01 Kafka Architecture | Broker, partition, replica — the internals of the world's dominant message bus. |
| 11 Kafka | M02 Producers and Consumers | acks, offsets, consumer group rebalancing — production configuration. |
| 11 Kafka | M03 Kafka Streams | KStream, KTable, joins, state stores — stateful stream processing. |
| 11 Kafka | M04 Kafka Connect | Source/sink connectors — integrating Kafka with the rest of the stack. |
| 11 Kafka | M05 Schema Registry | Avro, Protobuf, schema evolution, compatibility modes. |

### Skills Gained
- Debug a Spark data skew problem using the Spark UI.
- Explain why a Spark shuffle causes stage boundaries and how to minimize them.
- Write a Structured Streaming job with watermarks and stateful aggregation.
- Configure a Kafka producer for exactly-once delivery.
- Design a Kafka topic partitioning strategy for a given throughput requirement.
- Explain schema evolution compatibility modes and choose the right one for a use case.

### Dependencies
- Requires: Semester 2 (distributed systems theory: replication, partitioning, CAP).
- Requires: Semester 3 (Python internals for UDF performance understanding).

### Recommended Study Order
08-M01 → 08-M02 → 08-M03 → 08-M04 → 08-M05 → 09-M01 → 09-M02 → 11-M01 → 11-M02 → 10-M01 → 10-M02 → 09-M03 → 10-M03 → 11-M03 → 11-M04 → 11-M05 → 09-M04 → 10-M04

### Capstone Project
**"Real-Time E-Commerce Analytics Platform"**

Build a streaming pipeline that processes simulated e-commerce click events:
1. **Kafka:** Create a topic with 8 partitions. Write a Python producer that generates synthetic click events (user_id, product_id, event_type, timestamp) at 10,000 events/second.
2. **PySpark Structured Streaming:** Consume from Kafka. Apply a 5-minute tumbling window to compute per-product click counts. Handle late events up to 10 minutes. Write results to Delta Lake.
3. **Delta Lake:** Store aggregated results. Demonstrate time travel (query the state 30 minutes ago).
4. **Schema Registry:** Define an Avro schema for the click event. Demonstrate a backward-compatible schema change (adding a nullable field).
5. **Monitoring:** Add basic metrics (events processed/sec, lag, batch duration) to stdout.
6. **Documentation:** Write a 1-page architecture diagram (Mermaid) and runbook explaining how to restart the pipeline after a failure.

### Graduation Criteria
- [ ] Whiteboard the Spark execution model: DAG → stages → tasks, with correct identification of shuffle boundaries.
- [ ] Diagnose a Spark OOM error from a provided stack trace and propose the fix.
- [ ] Explain the difference between event time and processing time and why it matters.
- [ ] Describe Kafka's exactly-once producer configuration (`acks=all`, `enable.idempotence=true`, `transactional.id`).
- [ ] Complete the capstone project with a working streaming pipeline and written runbook.

---

## Semester 5 — Modern Data Stack and System Design

**Tagline:** *Production data engineering is not just pipelines. It is a system of systems.*

### Goals
Master the tools that define the modern data stack (BigQuery, Airflow, dbt, open table formats, data modeling). Learn to design complete data systems at the architecture level, as required for staff engineer roles and consulting.

### Required Prior Knowledge
- Semester 4 graduation criteria met.
- Understand the Medallion Architecture (Semester 3).
- Understand Spark and Kafka (Semester 4).

### Modules

| School | Module | Why It's Here |
|---|---|---|
| 12 BigQuery | M01–M05 (all) | The dominant cloud data warehouse. Architecture is unlike any traditional DB. |
| 13 Airflow | M01–M05 (all) | The standard orchestrator for batch pipelines. |
| 14 dbt | M01–M04 (all) | The standard transformation layer of the modern data stack. |
| 15 Iceberg/Delta/Hudi | M01–M04 (all) | Open table formats — ACID on data lakes. Essential for lakehouse. |
| 16 Data Modeling | M01–M04 (all) | Dimensional modeling, Data Vault, schema evolution. |
| 17 System Design | M01–M04 (all) | Design at scale: pipelines, streaming platforms, warehouses, lakehouses. |
| 18 Production Eng | M01–M04 (all) | Monitoring, CI/CD, data quality, incident response. |

### Skills Gained
- Build a fully tested dbt project with CI/CD.
- Optimize a BigQuery query by 10× using correct clustering, partitioning, and materialized views.
- Design a data warehouse from scratch (schema, partitioning, access control, cost management).
- Write a post-mortem for a data pipeline incident.
- Explain the internal file layout of an Iceberg table and how snapshot isolation works.
- Lead a system design conversation for a data lake, streaming platform, or warehouse.

### Dependencies
- Requires: All prior semesters.

### Recommended Study Order
13-M01 → 13-M02 → 13-M03 → 13-M04 → 13-M05 → 14-M01 → 14-M02 → 14-M03 → 14-M04 → 12-M01 → 12-M02 → 12-M03 → 12-M04 → 12-M05 → 15-M01 → 15-M02 → 15-M03 → 15-M04 → 16-M01 → 16-M02 → 16-M03 → 16-M04 → 17-M01 → 17-M02 → 17-M03 → 17-M04 → 18-M01 → 18-M02 → 18-M03 → 18-M04

### Capstone Project
**"Staff Engineer Take-Home: Design and Build a Lakehouse"**

This is a full system design + partial implementation exercise:

**Part 1 — Architecture (document + diagrams):**
Design a lakehouse for a fintech company processing 50 billion events/day:
- Data ingestion from Kafka (streaming) and S3 (batch).
- Bronze/Silver/Gold medallion architecture on Apache Iceberg.
- Transformation layer: dbt for Gold layer, PySpark for Silver.
- Orchestration: Airflow DAG with proper dependency management and alerting.
- Serving: BigQuery external tables over Iceberg for BI tools.
- Data quality: Great Expectations checks at each layer boundary.
- Monitoring: latency SLOs, data freshness alerts, cost budgets.

**Part 2 — Implementation (scaled-down version):**
Implement the Silver layer transformation in PySpark + Delta Lake using a provided sample dataset (1 GB of synthetic fintech events). Include unit tests, CI pipeline (GitHub Actions), and data quality checks.

**Part 3 — Incident Response:**
Given a provided "incident report" (a Gold layer aggregation was wrong for 6 hours), write a post-mortem: timeline, root cause, impact assessment, remediation steps, and preventive measures.

### Graduation Criteria
- [ ] Lead a 45-minute system design session for "Design a streaming data warehouse" without notes.
- [ ] Write a dbt model with an incremental strategy, a schema test, and a custom data test.
- [ ] Explain the internal structure of an Iceberg table (manifest list → manifest files → data files) from memory.
- [ ] Optimize a provided BigQuery query (using EXPLAIN) and reduce byte scanning by ≥50%.
- [ ] Complete all three parts of the capstone project.

---

## Graduation from the University

A student who completes all six semesters and all capstone projects has demonstrated:

1. **Computer science fundamentals** (CPU, memory, OS, networking) at a depth that allows them to reason about performance from first principles.
2. **Distributed systems theory** (CAP, Raft, replication, 2PC) that explains why every distributed data tool behaves the way it does.
3. **Core engineering craft** (SQL, Python, shell) at a senior level.
4. **Primary data engineering tools** (Spark, Kafka, Airflow, dbt, BigQuery, Iceberg/Delta).
5. **Production engineering** (monitoring, CI/CD, data quality, incident response).
6. **System design** at the staff/principal engineer level.

This is the profile of a **Staff Data Engineer**. The university does not hand out titles. It hands out capability.
