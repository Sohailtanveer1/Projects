# Data Engineering University — Full Curriculum

> **Legend:** ✅ Complete · 🔄 In Progress · 📋 Planned

---

## School 00 — CS Foundations

The bedrock. Every senior data engineer must understand what the computer is actually doing when code runs. Without this, debugging becomes guesswork.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | How Computers Execute Programs | 🔄 In Progress | CPU fetch-decode-execute, registers, machine code, ISA |
| M02 | Memory and Storage Hierarchy | 📋 Planned | Registers → L1/L2/L3 cache → RAM → SSD → HDD, cache lines, NUMA |
| M03 | Concurrency and Parallelism | 📋 Planned | Threads, processes, GIL, locks, race conditions, atomic ops |
| M04 | Networking Fundamentals | 📋 Planned | Sockets, TCP/IP, latency, bandwidth, MTU |
| M05 | OS Internals | 📋 Planned | Kernel, syscalls, scheduling, virtual memory, page faults |

---

## School 01 — Linux

The operating system of data engineering. Every Spark cluster, Kafka broker, and database server runs on Linux. Knowing it deeply separates engineers who guess from engineers who diagnose.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Linux Architecture | 📋 Planned | Kernel space vs user space, system calls, device drivers |
| M02 | File System and I/O | 📋 Planned | VFS, inodes, ext4/XFS, page cache, O_DIRECT, fsync |
| M03 | Processes and Signals | 📋 Planned | fork/exec, process lifecycle, signal handling, zombie processes |
| M04 | Shell Scripting | 📋 Planned | bash, pipes, redirects, cron, trap, process substitution |
| M05 | Performance Tools | 📋 Planned | top, htop, perf, strace, ltrace, iostat, vmstat, netstat |

---

## School 02 — Networking

Data pipelines are distributed systems. Every message sent between services traverses a network. Understanding latency, packet loss, and protocol overhead is essential for production debugging.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | OSI Model and TCP/IP | 📋 Planned | 7 layers, IP routing, TCP handshake, UDP, ICMP |
| M02 | DNS and HTTP | 📋 Planned | DNS resolution, HTTP/1.1 vs HTTP/2 vs HTTP/3, TLS |
| M03 | Load Balancers and Proxies | 📋 Planned | L4 vs L7 LB, reverse proxy, connection pooling |
| M04 | Network Security | 📋 Planned | TLS certificates, mTLS, VPC, firewall rules, VPN |

---

## School 03 — Distributed Systems

The theory behind every large-scale data system. CAP theorem, consistency models, and consensus protocols determine the behavior of Kafka, Spark, and every distributed database.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | CAP Theorem | 📋 Planned | Consistency, Availability, Partition Tolerance, PACELC |
| M02 | Consensus and Raft | 📋 Planned | Leader election, log replication, Raft vs Paxos |
| M03 | Replication and Partitioning | 📋 Planned | Leader-follower, multi-leader, leaderless, hash/range partitioning |
| M04 | Distributed Transactions | 📋 Planned | 2PC, Saga pattern, compensating transactions |
| M05 | Observability | 📋 Planned | Structured logging, metrics, distributed tracing, OpenTelemetry |

---

## School 04 — Databases

The engine beneath every data warehouse and operational store. Understanding storage engines, indexing, and query execution lets you tune queries and choose the right database.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Storage Engines | 📋 Planned | B-tree vs LSM-tree, InnoDB, RocksDB, write amplification |
| M02 | Indexing | 📋 Planned | B-tree, hash, bitmap, partial, covering indexes |
| M03 | Query Execution | 📋 Planned | Parser → planner → executor, cost-based optimizer, join algorithms |
| M04 | ACID and Isolation Levels | 📋 Planned | Atomicity, durability, MVCC, read phenomena, snapshot isolation |
| M05 | Replication and Sharding | 📋 Planned | WAL shipping, logical replication, horizontal sharding |

---

## School 05 — Data Engineering

The craft. Core patterns and principles that apply regardless of which tool you use.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Batch vs Streaming | 📋 Planned | Micro-batch, true streaming, latency trade-offs, when to use each |
| M02 | Data Lake Architecture | 📋 Planned | Raw/curated/enriched zones, storage formats, metadata |
| M03 | ELT vs ETL | 📋 Planned | Transform-in-warehouse vs transform-in-flight, pushdown |
| M04 | Medallion Architecture | 📋 Planned | Bronze/Silver/Gold, idempotency, incremental processing |
| M05 | Data Contracts | 📋 Planned | Schema, SLAs, ownership, breaking changes, enforcement |

---

## School 06 — SQL

SQL is the universal language of data. Knowing internals turns you from someone who writes SQL into someone who understands why it's slow and how to fix it.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | SQL Execution Internals | 📋 Planned | Parse → bind → optimize → execute, explain plans |
| M02 | Window Functions | 📋 Planned | OVER, PARTITION BY, ROWS vs RANGE, LAG/LEAD, RANK |
| M03 | Query Optimization | 📋 Planned | Statistics, predicate pushdown, join reordering, materialized views |
| M04 | Advanced Joins | 📋 Planned | Hash join, merge join, nested-loop join, anti-join, lateral join |
| M05 | Recursive CTEs | 📋 Planned | WITH RECURSIVE, tree traversal, graph queries |

---

## School 07 — Python

The glue language of data engineering. Knowing the CPython internals prevents surprises in production (GIL, memory leaks, slow UDFs).

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Python Internals | 📋 Planned | CPython bytecode, GIL, reference counting, object model |
| M02 | Concurrency: asyncio and threading | 📋 Planned | Event loop, coroutines, thread safety, multiprocessing |
| M03 | Memory Management | 📋 Planned | gc module, memory leaks, tracemalloc, generators vs lists |
| M04 | Data Structures and Algorithms | 📋 Planned | Big-O, heaps, trees, sorting, hashing |
| M05 | Python for Data Engineering | 📋 Planned | pandas, polars, pyarrow, boto3, connection pooling |

---

## School 08 — Apache Spark

The dominant batch processing engine. Understanding the execution engine prevents 90% of Spark production failures (OOM, skew, slow shuffles).

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Spark Architecture | 📋 Planned | Driver, executor, cluster manager, JVM heap |
| M02 | RDD vs DataFrame vs Dataset | 📋 Planned | Tungsten, Catalyst optimizer, encoder |
| M03 | Spark Execution Engine | 📋 Planned | DAG, stages, tasks, wide vs narrow transformations |
| M04 | Shuffle and Partitioning | 📋 Planned | Hash shuffle, sort-shuffle, AQE, skew join |
| M05 | Spark Optimization | 📋 Planned | Broadcast join, caching, predicate pushdown, bucketing |

---

## School 09 — PySpark

The Python API for Spark. Includes UDF internals, Structured Streaming, and Delta Lake integration.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | PySpark API | 📋 Planned | SparkSession, DataFrame API, Column, Row |
| M02 | PySpark UDFs | 📋 Planned | Python UDF overhead, Pandas UDF (Arrow), Scala UDF via JVM |
| M03 | Structured Streaming | 📋 Planned | Micro-batch vs continuous, triggers, state store, watermark |
| M04 | Delta Lake with PySpark | 📋 Planned | ACID on object storage, DML, time travel |

---

## School 10 — Streaming

The theory and practice of processing infinite data streams. Windowing, watermarks, and exactly-once guarantees are the hard problems.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Stream Processing Fundamentals | 📋 Planned | Event time vs processing time, out-of-order events |
| M02 | Windowing and Watermarks | 📋 Planned | Tumbling, sliding, session windows, late data handling |
| M03 | Exactly-Once Semantics | 📋 Planned | At-least-once vs at-most-once vs exactly-once, idempotency |
| M04 | Flink vs Spark Streaming | 📋 Planned | True streaming vs micro-batch, state backends, latency |

---

## School 11 — Kafka

The backbone of modern streaming architectures. Understanding partitions, replication, and consumer groups is table stakes for any senior data engineer.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Kafka Architecture | 📋 Planned | Broker, ZooKeeper/KRaft, topic, partition, replica |
| M02 | Producers and Consumers | 📋 Planned | acks, linger.ms, batch.size, consumer group rebalancing |
| M03 | Kafka Streams | 📋 Planned | KStream, KTable, joins, state stores, exactly-once |
| M04 | Kafka Connect | 📋 Planned | Source/sink connectors, worker config, SMTs |
| M05 | Schema Registry | 📋 Planned | Avro/Protobuf, compatibility modes, schema evolution |

---

## School 12 — BigQuery

Google's serverless data warehouse. The architecture (Dremel + Colossus + Borg) is unlike any traditional database — understanding it determines cost and performance.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | BigQuery Architecture | 📋 Planned | Dremel, Colossus, Borg, slot-based execution |
| M02 | Storage and Columnar Format | 📋 Planned | Capacitor format, nested/repeated fields, clustering, partitioning |
| M03 | Query Optimization | 📋 Planned | Query plan, slot usage, BI Engine, materialized views |
| M04 | Cost Management | 📋 Planned | On-demand vs flat-rate, reservations, byte scanning |
| M05 | BigQuery ML | 📋 Planned | In-database ML, model types, BQML pipeline |

---

## School 13 — Airflow

The standard orchestrator for batch pipelines. Understanding the scheduler internals prevents silent failures and runaway queues.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Airflow Architecture | 📋 Planned | Scheduler, Webserver, Executor, Metadata DB, Workers |
| M02 | DAGs and Operators | 📋 Planned | DAG definition, BashOperator, PythonOperator, sensors |
| M03 | Task Dependencies | 📋 Planned | set_upstream/downstream, XCom, task groups, dynamic tasks |
| M04 | Custom Operators | 📋 Planned | BaseOperator, hooks, connections, testing |
| M05 | Airflow in Production | 📋 Planned | Celery vs Kubernetes executor, scaling, monitoring, DAG factory |

---

## School 14 — dbt

The transformation layer of the modern data stack. dbt's model-based approach, materialization strategies, and testing framework are now industry standard.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | dbt Architecture | 📋 Planned | Profiles, projects, adapters, compiled SQL |
| M02 | Models and Materializations | 📋 Planned | Table, view, incremental, ephemeral, snapshots |
| M03 | Testing and Documentation | 📋 Planned | Schema tests, custom tests, dbt docs, lineage |
| M04 | Advanced dbt Patterns | 📋 Planned | Macros, packages, meta, exposures, CI/CD |

---

## School 15 — Open Table Formats (Iceberg / Delta / Hudi)

The layer that brings ACID transactions, schema evolution, and time travel to data lakes. Understanding the internal file layout determines partition pruning and merge performance.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Table Format Comparison | 📋 Planned | Iceberg vs Delta vs Hudi, when to use each |
| M02 | Apache Iceberg | 📋 Planned | Manifest lists, manifest files, snapshot isolation, hidden partitioning |
| M03 | Delta Lake | 📋 Planned | Delta log, checkpoint, OPTIMIZE, Z-order, VACUUM |
| M04 | Apache Hudi | 📋 Planned | CoW vs MoR, timeline, upserts, Hudi metadata table |

---

## School 16 — Data Modeling

How you structure data determines query performance, maintainability, and business agility. Covers classical and modern approaches.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Dimensional Modeling | 📋 Planned | Star schema, snowflake, fact tables, dimensions, SCD types |
| M02 | Data Vault | 📋 Planned | Hubs, links, satellites, business keys, auditability |
| M03 | One Big Table | 📋 Planned | Denormalization trade-offs, when OBT makes sense |
| M04 | Schema Evolution | 📋 Planned | Backward/forward compatibility, Avro, Protobuf, Parquet schema |

---

## School 17 — System Design

Translating all the above into systems that can handle real-world scale. Essential for staff-level interviews and production architecture decisions.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Design a Data Pipeline | 📋 Planned | Ingestion, transformation, serving, SLAs |
| M02 | Design a Streaming Platform | 📋 Planned | Kafka + Flink/Spark, state management, scaling |
| M03 | Design a Data Warehouse | 📋 Planned | Storage, compute separation, query routing, caching |
| M04 | Design a Lakehouse | 📋 Planned | Open table formats + compute engines + governance |

---

## School 18 — Production Engineering

The difference between a pipeline that works in dev and one that works at 3 AM when you're oncall.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Monitoring and Alerting | 📋 Planned | Prometheus, Grafana, SLOs, PagerDuty, runbooks |
| M02 | CI/CD for Data | 📋 Planned | dbt CI, Spark unit tests, GitHub Actions, Blue-Green deploys |
| M03 | Data Quality Frameworks | 📋 Planned | Great Expectations, Soda, dbt tests, quarantine patterns |
| M04 | Incident Response | 📋 Planned | On-call, post-mortems, runbooks, rollback strategies |

---

## School 19 — Interview Preparation

Structured preparation for every stage of the data engineering interview process, from screening to staff-level system design.

| # | Module | Status | Key Concepts |
|---|--------|--------|--------------|
| M01 | Coding Interview | 📋 Planned | SQL challenges, Python DSA, Spark transformation questions |
| M02 | System Design Interview | 📋 Planned | Framework, common questions, how to communicate trade-offs |
| M03 | Behavioral Interview | 📋 Planned | STAR method, data engineering-specific scenarios |
