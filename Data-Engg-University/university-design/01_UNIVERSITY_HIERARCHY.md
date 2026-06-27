# Data Engineering University — Complete Hierarchy
## University → School → Department → Course → Module → Lesson

> This document is the master structural map of the university. It defines every unit of learning by title and position in the hierarchy. It does NOT contain educational content — that lives in the module `README.md` files. Every entry here is a commitment: the topic will be taught, at this depth, in this order.

---

## Hierarchy Key

```
🏛️  School
  📂  Department
    📘  Course  (code: SCHOOL-DEPT-NUM)
      📄  Module
        ▸   Lesson
```

---

# 🏛️ School 1 — Computer Science Foundations (CSF)

**Purpose:** Build the hardware-to-software mental model that underlies every data engineering decision. Without this school, engineers cargo-cult configuration. With it, they reason from first principles.

---

## 📂 Department 1.1 — Computer Architecture (CSF-ARC)

### 📘 Course CSF-ARC-101: How Computers Execute Programs

**Prerequisite:** None  
**Semester:** 0

| # | Module | Lessons |
|---|--------|---------|
| M01 | The Von Neumann Machine | Stored-program concept · Von Neumann architecture components · Von Neumann bottleneck · Historical evolution to multi-core |
| M02 | The CPU in Detail | Fetch-decode-execute cycle · Control unit · ALU operations · Registers and their roles |
| M03 | Instruction Set Architecture | What an ISA defines · x86-64 vs ARM vs RISC-V · Machine code and encoding · The ISA-software contract |
| M04 | CPU Microarchitecture | Pipelines and stages · Superscalar execution · Out-of-order execution · Branch prediction |
| M05 | From Source Code to Machine Code | Compilation stages · CPython bytecode · JVM bytecode and JIT · Native compilation (C/Rust) |

### 📘 Course CSF-ARC-102: Memory Architecture

**Prerequisite:** CSF-ARC-101  
**Semester:** 0

| # | Module | Lessons |
|---|--------|---------|
| M01 | The Memory Hierarchy | Why a hierarchy exists · Registers · L1/L2/L3 cache · DRAM · Persistent storage · Latency numbers |
| M02 | Cache Internals | Cache lines · Direct-mapped vs set-associative · Cache coherence (MESI protocol) · False sharing |
| M03 | Virtual Memory | Page tables · TLB · Page faults · Memory-mapped files · mmap() and its uses |
| M04 | NUMA Architecture | Non-uniform memory access · Socket topology · NUMA-aware programming · Spark NUMA configuration |
| M05 | Memory Access Patterns | Sequential vs random access · Prefetching · Columnar access vs row access · Benchmark: Parquet vs CSV |

---

## 📂 Department 1.2 — Operating Systems (CSF-OS)

### 📘 Course CSF-OS-101: OS Internals

**Prerequisite:** CSF-ARC-102  
**Semester:** 0

| # | Module | Lessons |
|---|--------|---------|
| M01 | Kernel and User Space | Privilege rings · Kernel space vs user space · System calls · Syscall overhead |
| M02 | Process Management | Process vs thread · Process lifecycle · PCB (Process Control Block) · Context switching cost |
| M03 | CPU Scheduling | Scheduling algorithms (CFS, Round Robin) · Priority and nice values · CPU affinity · Implications for Spark executors |
| M04 | Virtual Memory Management | Page allocation · Swap · OOM killer · JVM heap vs off-heap |
| M05 | I/O Subsystem | Blocking vs non-blocking I/O · epoll · io_uring · Kernel page cache · Direct I/O |

### 📘 Course CSF-OS-102: Concurrency Fundamentals

**Prerequisite:** CSF-OS-101  
**Semester:** 0

| # | Module | Lessons |
|---|--------|---------|
| M01 | Threads and Processes | POSIX threads · Thread vs process trade-offs · Thread safety · Race conditions |
| M02 | Synchronization Primitives | Mutex · Semaphore · Spinlock · Read-write lock · When each is appropriate |
| M03 | Lock-Free Programming | CAS (Compare-And-Swap) · Atomic operations · ABA problem · Lock-free data structures |
| M04 | Python Concurrency | CPython GIL internals · threading module · multiprocessing module · concurrent.futures |
| M05 | Concurrency Failure Modes | Deadlock · Livelock · Starvation · Priority inversion · Debugging with thread dumps |

---

## 📂 Department 1.3 — Algorithms & Complexity (CSF-ALG)

### 📘 Course CSF-ALG-101: Data Structures & Algorithms for Data Engineers

**Prerequisite:** CSF-ARC-101  
**Semester:** 1

| # | Module | Lessons |
|---|--------|---------|
| M01 | Complexity Analysis | Big-O, Big-Θ, Big-Ω · Time vs space trade-offs · Amortized analysis · Practical benchmarking |
| M02 | Core Data Structures | Arrays vs linked lists · Hash tables (open addressing vs chaining) · Trees · Heaps |
| M03 | Sorting Algorithms | Comparison sorts (merge, quick, heap) · Non-comparison sorts (radix, counting) · Spark sort internals |
| M04 | Hashing | Hash functions · Consistent hashing (Kafka, Cassandra) · Bloom filters · HyperLogLog |
| M05 | Graph Algorithms | BFS/DFS · DAG topological sort (Airflow, Spark DAG) · Shortest path · Spark GraphX overview |

---

# 🏛️ School 2 — Systems & Infrastructure (SYS)

**Purpose:** Master Linux as a diagnostic tool, networking as a constraint, and distributed systems theory as the explanation for every trade-off in every data technology.

---

## 📂 Department 2.1 — Linux & Unix Systems (SYS-LNX)

### 📘 Course SYS-LNX-101: Linux for Data Engineers

**Prerequisite:** CSF-OS-101  
**Semester:** 1

| # | Module | Lessons |
|---|--------|---------|
| M01 | Linux Architecture | Kernel subsystems · VFS abstraction · Device driver model · Init system (systemd) |
| M02 | File System Internals | inodes · ext4/XFS structure · journaling · Hard vs soft links · File descriptor lifecycle |
| M03 | I/O and the Page Cache | Kernel page cache · read() vs mmap() · O_DIRECT · fsync() · Write-back vs write-through |
| M04 | Processes and Signals | fork() and exec() · Signal handling · Zombie processes · ulimits · cgroups |
| M05 | Storage and Disks | Block devices · Partitioning · RAID concepts · LVM · NFS and distributed file systems |

### 📘 Course SYS-LNX-102: Linux Performance & Operations

**Prerequisite:** SYS-LNX-101  
**Semester:** 1

| # | Module | Lessons |
|---|--------|---------|
| M01 | Observability Tools | top, htop, ps · vmstat · iostat · lsof · ss/netstat |
| M02 | CPU Profiling | perf stat · perf top · flame graphs · strace · ltrace |
| M03 | Memory Profiling | /proc/meminfo · smaps · valgrind · massif · Java heap dumps |
| M04 | Shell Scripting | bash scripting · set -euo pipefail · Error handling · Pipes and redirects · Here-docs |
| M05 | Automation & Cron | cron syntax · systemd timers · Idempotent scripts · Log rotation · Alert hooks |

---

## 📂 Department 2.2 — Computer Networking (SYS-NET)

### 📘 Course SYS-NET-101: Networking Fundamentals

**Prerequisite:** CSF-ARC-101  
**Semester:** 1

| # | Module | Lessons |
|---|--------|---------|
| M01 | OSI and TCP/IP Model | 7 layers · Encapsulation · Protocol stack · Where data engineering tools operate |
| M02 | IP Routing and Subnetting | IPv4/IPv6 · CIDR notation · Routing tables · VPC subnetting for cloud DE |
| M03 | TCP in Depth | Three-way handshake · Congestion control · Slow start · TIME_WAIT · Nagle algorithm |
| M04 | UDP and Application Protocols | UDP use cases · DNS (resolution chain · TTL · failure modes) · HTTP/1.1 vs 2 vs 3 |
| M05 | Network Diagnostics | ping · traceroute · tcpdump · Wireshark · curl · Network latency profiling |

### 📘 Course SYS-NET-102: Network Engineering for Data Systems

**Prerequisite:** SYS-NET-101  
**Semester:** 2

| # | Module | Lessons |
|---|--------|---------|
| M01 | Load Balancers | L4 vs L7 load balancing · Round-robin vs least-connections · Health checks · Connection draining |
| M02 | Proxies and Service Mesh | Forward vs reverse proxy · Nginx · Envoy · Service mesh (Istio overview) |
| M03 | Network Security | TLS 1.2 vs 1.3 · Certificate lifecycle · mTLS · VPN · Firewall rules |
| M04 | Cloud Networking | VPC design · Security groups · Private endpoints · VPC peering · Transit Gateway |
| M05 | Bandwidth and Latency Optimization | Compression · TCP tuning · CDN · Colocating compute and storage |

---

## 📂 Department 2.3 — Distributed Systems Theory (SYS-DST)

### 📘 Course SYS-DST-101: Foundations of Distributed Systems

**Prerequisite:** SYS-NET-101  
**Semester:** 2

| # | Module | Lessons |
|---|--------|---------|
| M01 | The Nature of Distributed Systems | Fallacies of distributed computing · Partial failure · Network partitions · Two Generals Problem |
| M02 | Consistency Models | Linearizability · Sequential consistency · Eventual consistency · Causal consistency |
| M03 | CAP Theorem | Consistency vs Availability vs Partition Tolerance · PACELC · Practical CP and AP examples |
| M04 | Time and Ordering | Logical clocks · Lamport timestamps · Vector clocks · Event ordering in Kafka |
| M05 | Failure Detection | Heartbeats · Phi accrual failure detector · Gossip protocol · Split-brain |

### 📘 Course SYS-DST-102: Consensus and Replication

**Prerequisite:** SYS-DST-101  
**Semester:** 2

| # | Module | Lessons |
|---|--------|---------|
| M01 | Leader Election | Bully algorithm · Ring algorithm · ZooKeeper-based election · KRaft (Kafka) |
| M02 | Paxos | Basic Paxos · Multi-Paxos · Paxos failure modes · Why Paxos is hard to implement |
| M03 | Raft | Leader election · Log replication · Safety guarantees · Raft vs Paxos practical comparison |
| M04 | Replication Strategies | Single-leader · Multi-leader · Leaderless (Dynamo-style) · Replication lag |
| M05 | Distributed Transactions | Two-phase commit (2PC) · Three-phase commit · Saga pattern · Compensating transactions |

---

# 🏛️ School 3 — Database Engineering (DBE)

**Purpose:** Understand how data is physically stored, indexed, and queried. The storage engine is the most fundamental component of any data system — including Spark, Kafka, and BigQuery.

---

## 📂 Department 3.1 — Database Internals (DBE-INT)

### 📘 Course DBE-INT-101: Storage Engines

**Prerequisite:** CSF-ARC-102, SYS-LNX-101  
**Semester:** 2

| # | Module | Lessons |
|---|--------|---------|
| M01 | B-Tree Storage Engines | B-tree structure · Page layout · Write-ahead log (WAL) · InnoDB internals |
| M02 | LSM-Tree Storage Engines | MemTable · SSTables · Compaction strategies · RocksDB · Write amplification |
| M03 | Columnar Storage Engines | Column families · Compression per column · Predicate pushdown · Parquet internals |
| M04 | Transaction Processing | ARIES recovery algorithm · Buffer pool management · Lock manager · Deadlock detection |
| M05 | Storage Engine Comparison | OLTP vs OLAP workloads · Read/write amplification factors · Choosing the right engine |

### 📘 Course DBE-INT-102: Indexing and Query Processing

**Prerequisite:** DBE-INT-101  
**Semester:** 2

| # | Module | Lessons |
|---|--------|---------|
| M01 | Index Types | B-tree index · Hash index · Bitmap index · Partial index · Covering index |
| M02 | Index Internals | B-tree page splits · Index fragmentation · Fill factor · Maintenance strategies |
| M03 | Query Processing Pipeline | Parser · Binder · Logical planner · Physical planner · Cost-based optimizer · Executor |
| M04 | Join Algorithms | Nested-loop join · Hash join · Sort-merge join · Index join · Anti-join |
| M05 | Query Optimization | Statistics and histograms · Cardinality estimation · Plan hints · Materialized views |

### 📘 Course DBE-INT-103: Transactions and Isolation

**Prerequisite:** DBE-INT-101  
**Semester:** 2

| # | Module | Lessons |
|---|--------|---------|
| M01 | ACID Properties | Atomicity and rollback · Consistency constraints · Isolation · Durability and fsync |
| M02 | Read Phenomena | Dirty reads · Non-repeatable reads · Phantom reads · Write skew |
| M03 | Isolation Levels | Read uncommitted · Read committed · Repeatable read · Serializable |
| M04 | MVCC | Snapshot isolation mechanics · Postgres MVCC · Version chain · Vacuum/autovacuum |
| M05 | Distributed Transactions in Databases | XA transactions · Distributed deadlock · Practical transaction patterns |

---

## 📂 Department 3.2 — SQL Engineering (DBE-SQL)

### 📘 Course DBE-SQL-101: SQL Internals and Optimization

**Prerequisite:** DBE-INT-102  
**Semester:** 3

| # | Module | Lessons |
|---|--------|---------|
| M01 | SQL Execution Internals | Parsing · Logical vs physical plan · Plan cache · Parameterized queries |
| M02 | EXPLAIN and Query Plans | Reading an EXPLAIN output · Seq scan vs index scan · Cost estimates vs actual · Plan regression |
| M03 | Join Optimization | Hash join vs nested loop vs merge · Join reordering · Cross joins (accidental) · Lateral joins |
| M04 | Index Strategy | Which queries benefit from an index · Multi-column index column order · When NOT to index |
| M05 | Query Performance Profiling | pg_stat_statements · Slow query log · Execution plan visualization · Identifying bottlenecks |

### 📘 Course DBE-SQL-102: Advanced SQL for Data Engineers

**Prerequisite:** DBE-SQL-101  
**Semester:** 3

| # | Module | Lessons |
|---|--------|---------|
| M01 | Window Functions | OVER clause · PARTITION BY · ORDER BY · Frame clauses (ROWS vs RANGE) |
| M02 | Window Function Patterns | Running totals · Moving averages · Ranking · LAG/LEAD · FIRST_VALUE/LAST_VALUE |
| M03 | CTEs and Subqueries | WITH clause · Recursive CTEs · Tree and graph traversal · CTE materialization |
| M04 | Advanced Aggregations | GROUPING SETS · ROLLUP · CUBE · FILTER clause · Distinct aggregations |
| M05 | Analytical SQL Patterns | Sessionization · Funnel analysis · Cohort analysis · Gap and island problems |

---

## 📂 Department 3.3 — Scalable Data Stores (DBE-SDS)

### 📘 Course DBE-SDS-101: Database Replication and Sharding

**Prerequisite:** DBE-INT-103, SYS-DST-102  
**Semester:** 3

| # | Module | Lessons |
|---|--------|---------|
| M01 | Replication Mechanics | WAL streaming · Logical replication · Replication slots · Monitoring replica lag |
| M02 | Replication Topologies | Primary-replica · Cascaded replication · Multi-primary · Active-active trade-offs |
| M03 | Sharding Strategies | Hash sharding · Range sharding · Directory sharding · Shard key selection |
| M04 | Consistent Hashing | Ring topology · Virtual nodes · Rebalancing on node join/leave · Hotspots |
| M05 | Global Databases | Google Spanner · CockroachDB · TrueTime · External consistency |

---

# 🏛️ School 4 — Programming Engineering (PRG)

**Purpose:** Python mastery at the depth required for data engineering work — not just writing scripts, but understanding the runtime, managing memory, achieving performance, and building maintainable systems.

---

## 📂 Department 4.1 — Python Engineering (PRG-PY)

### 📘 Course PRG-PY-101: Python Internals

**Prerequisite:** CSF-ARC-101, CSF-OS-101  
**Semester:** 1

| # | Module | Lessons |
|---|--------|---------|
| M01 | CPython Architecture | Interpreter architecture · Object model · Reference counting · Garbage collection |
| M02 | Bytecode and the Eval Loop | dis module · Bytecode instruction set · ceval.c eval loop · Frame objects |
| M03 | The GIL | GIL internals · When the GIL is released · GIL impact on threading · PEP 703 (no-GIL) |
| M04 | Python Memory Model | PyObject header · Small integer cache · String interning · Memory allocator (pymalloc) |
| M05 | Python Performance Profiling | cProfile · line_profiler · memory_profiler · tracemalloc · Benchmarking with timeit |

### 📘 Course PRG-PY-102: Python Concurrency and Async

**Prerequisite:** PRG-PY-101, CSF-OS-102  
**Semester:** 2

| # | Module | Lessons |
|---|--------|---------|
| M01 | Threading in Python | threading.Thread · Thread pools · ThreadPoolExecutor · Thread safety patterns |
| M02 | Multiprocessing | Process vs thread · multiprocessing.Pool · Shared memory · Inter-process communication |
| M03 | asyncio Internals | Event loop architecture · Coroutines · async/await · Tasks and Futures · uvloop |
| M04 | Async Patterns for Data Engineering | Async HTTP (aiohttp) · Async Kafka · Async database clients · Backpressure handling |
| M05 | Concurrency Anti-Patterns | GIL traps · Deadlock in async code · Thread-safety violations · Debugging concurrent bugs |

### 📘 Course PRG-PY-103: Python for Data Engineering

**Prerequisite:** PRG-PY-102  
**Semester:** 3

| # | Module | Lessons |
|---|--------|---------|
| M01 | pandas Internals | DataFrame architecture · Block manager · Copy-on-write (pandas 2.0) · Memory usage |
| M02 | Apache Arrow and PyArrow | Arrow columnar format · Zero-copy reads · RecordBatch · Arrow → Pandas interop |
| M03 | Polars | Polars vs pandas architecture · Lazy evaluation · Streaming mode · When to use polars |
| M04 | Data Serialization | JSON vs CSV vs Parquet vs Avro vs Protobuf · Compression codecs · Schema evolution |
| M05 | Cloud SDK Patterns | boto3 architecture · GCS client · S3 multipart upload · Retry and backoff patterns |

---

## 📂 Department 4.2 — Software Engineering Practices (PRG-SE)

### 📘 Course PRG-SE-101: Engineering Practices for Data Engineers

**Prerequisite:** PRG-PY-101  
**Semester:** 2

| # | Module | Lessons |
|---|--------|---------|
| M01 | Testing Strategies | Unit vs integration vs end-to-end · Test doubles (mock, stub, fake) · Pytest patterns |
| M02 | Testing Data Pipelines | Schema validation tests · Data quality tests · Pipeline idempotency tests · Snapshot tests |
| M03 | Code Quality | Type hints · mypy · ruff/flake8 · Pre-commit hooks · Code review discipline |
| M04 | Version Control for Data | Git branching strategies · Feature flags · Trunk-based development · dbt + Git |
| M05 | Documentation Standards | Docstrings · README conventions · ADR (Architecture Decision Records) · Runbooks |

---

# 🏛️ School 5 — Data Engineering (DE)

**Purpose:** The craft. Core patterns, tools, and principles of professional data engineering — pipeline design, lake architecture, orchestration, transformation, and data contracts.

---

## 📂 Department 5.1 — Pipeline Design & Architecture (DE-PDA)

### 📘 Course DE-PDA-101: Data Pipeline Fundamentals

**Prerequisite:** PRG-PY-103  
**Semester:** 3

| # | Module | Lessons |
|---|--------|---------|
| M01 | Batch vs Streaming | Latency-throughput spectrum · Use-case mapping · Micro-batch vs true streaming |
| M02 | ETL vs ELT Architecture | Transform-in-flight vs transform-in-warehouse · Pushdown compute · When each applies |
| M03 | Pipeline Design Patterns | Fan-in, fan-out · Idempotency · Exactly-once delivery · Backfill and reprocessing |
| M04 | Resilience Patterns | Retry with backoff · Dead-letter queues · Circuit breaker · Checkpointing |
| M05 | Pipeline Observability | Data lineage · Row count checks · Freshness SLAs · Alerting on pipeline anomalies |

### 📘 Course DE-PDA-102: Data Lake Architecture

**Prerequisite:** DE-PDA-101  
**Semester:** 3

| # | Module | Lessons |
|---|--------|---------|
| M01 | Data Lake Fundamentals | Why data lakes exist · Object storage (S3/GCS/ADLS) · File formats in the lake |
| M02 | Medallion Architecture | Bronze/Silver/Gold zones · Zone contracts · Idempotent zone transitions |
| M03 | Lake Partitioning Strategy | Partition key selection · Partition pruning · Over-partitioning · Hive-style partitions |
| M04 | Data Lake Governance | Metadata management · Data catalog (Glue, Dataplex) · Access control · Data classification |
| M05 | Data Lake Anti-Patterns | The data swamp · Schema chaos · Partition explosion · Uncontrolled compaction |

---

## 📂 Department 5.2 — Data Modeling (DE-MOD)

### 📘 Course DE-MOD-101: Dimensional Modeling

**Prerequisite:** DBE-SQL-102  
**Semester:** 4

| # | Module | Lessons |
|---|--------|---------|
| M01 | Dimensional Modeling Concepts | Facts and dimensions · Grain definition · Star schema · Snowflake schema |
| M02 | Slowly Changing Dimensions | SCD Type 0, 1, 2, 3, 4, 6 · Implementation in SQL and dbt · When each applies |
| M03 | Fact Table Design | Transactional vs snapshot vs accumulating snapshot · Factless fact tables · Degenerate dimensions |
| M04 | Dimension Table Design | Conforming dimensions · Role-playing dimensions · Junk dimensions · Date dimension |
| M05 | Modeling for Analytics | Query patterns driving model choices · Pre-aggregation · Bridge tables |

### 📘 Course DE-MOD-102: Modern Data Modeling

**Prerequisite:** DE-MOD-101  
**Semester:** 4

| # | Module | Lessons |
|---|--------|---------|
| M01 | Data Vault 2.0 | Hubs · Links · Satellites · Business keys · Auditability · Load patterns |
| M02 | One Big Table | When denormalization wins · Cost of joins at scale · OBT anti-patterns |
| M03 | Activity Schema | Activity stream pattern · Enrichment · Use cases in product analytics |
| M04 | Schema Evolution | Backward/forward compatibility · Avro · Protobuf · Parquet schema evolution |
| M05 | Data Contracts | Producer-consumer contract · Schema registry · SLA contracts · Contract testing |

---

## 📂 Department 5.3 — Orchestration (DE-ORC)

### 📘 Course DE-ORC-101: Apache Airflow

**Prerequisite:** DE-PDA-101, PRG-PY-103  
**Semester:** 4

| # | Module | Lessons |
|---|--------|---------|
| M01 | Airflow Architecture | Scheduler internals · Metadata DB · Executor types · Webserver · Workers |
| M02 | DAGs and Operators | DAG definition · BashOperator · PythonOperator · Sensors · Hooks |
| M03 | Task Dependencies | set_upstream/downstream · Trigger rules · XCom · Task groups · Dynamic task mapping |
| M04 | Custom Operators | Extending BaseOperator · Hooks and connections · Testing operators · Operator reuse |
| M05 | Airflow in Production | CeleryExecutor vs KubernetesExecutor · Scaling · DAG factory pattern · CI/CD for DAGs |

---

## 📂 Department 5.4 — Transformation Engineering (DE-TRF)

### 📘 Course DE-TRF-101: dbt (data build tool)

**Prerequisite:** DBE-SQL-102, DE-PDA-101  
**Semester:** 4

| # | Module | Lessons |
|---|--------|---------|
| M01 | dbt Architecture | Profiles · Projects · Adapters · Compiled SQL · dbt run pipeline |
| M02 | Models and Materializations | Table · View · Incremental · Ephemeral · Snapshot · When each applies |
| M03 | dbt Testing | Schema tests · Custom data tests · dbt-expectations · Freshness checks |
| M04 | dbt Documentation | YAML descriptions · dbt docs generate · Lineage graph · Meta fields · Exposures |
| M05 | Advanced dbt | Macros · Packages · Hooks · Analyses · dbt CI/CD · DAG optimization |

---

# 🏛️ School 6 — Distributed Compute & Streaming (DCS)

**Purpose:** Apache Spark, Apache Kafka, and stream processing theory. The dominant execution engines for large-scale batch and streaming data.

---

## 📂 Department 6.1 — Apache Spark (DCS-SPK)

### 📘 Course DCS-SPK-101: Spark Architecture and Internals

**Prerequisite:** SYS-DST-101, PRG-PY-101  
**Semester:** 4

| # | Module | Lessons |
|---|--------|---------|
| M01 | Spark Cluster Architecture | Driver · Executors · Cluster managers (YARN, Kubernetes, Standalone) · JVM heap layout |
| M02 | RDD Internals | Resilient Distributed Dataset · Lineage graph · Narrow vs wide dependencies · Persistence levels |
| M03 | Catalyst Optimizer | DataFrame vs RDD · Logical plan · Physical plan · Rule-based vs cost-based optimization |
| M04 | Tungsten Execution Engine | Code generation · Off-heap memory · Whole-stage codegen · Binary processing |
| M05 | DAG, Stages, and Tasks | How the DAG is built · Stage boundaries (shuffle) · Task scheduling · Speculative execution |

### 📘 Course DCS-SPK-102: Spark Performance Engineering

**Prerequisite:** DCS-SPK-101  
**Semester:** 4

| # | Module | Lessons |
|---|--------|---------|
| M01 | Shuffle Internals | Hash shuffle · Sort-merge shuffle · Shuffle write/read · Shuffle service |
| M02 | Partitioning Strategy | Default parallelism · repartition vs coalesce · Partition sizing · Partition skew |
| M03 | Adaptive Query Execution (AQE) | Runtime statistics · Dynamic partition coalescing · Skew join optimization · Dynamic pruning |
| M04 | Caching and Persistence | StorageLevel options · Memory fractions · Cache eviction · When NOT to cache |
| M05 | Spark Optimization Toolkit | Broadcast join · Bucketing · Predicate pushdown · Kryo serialization · Tuning memory |

---

## 📂 Department 6.2 — PySpark & Delta Lake (DCS-PYS)

### 📘 Course DCS-PYS-101: PySpark Engineering

**Prerequisite:** DCS-SPK-102, PRG-PY-103  
**Semester:** 5

| # | Module | Lessons |
|---|--------|---------|
| M01 | PySpark API Deep Dive | SparkSession · DataFrame API · Column expressions · Row object · Catalyst integration |
| M02 | PySpark UDF Performance | Python UDF serialization overhead · Pandas UDF (Arrow batches) · Scalar vs grouped vs map |
| M03 | Structured Streaming | DStream legacy vs Structured Streaming · Trigger modes · Output modes · State store |
| M04 | Streaming Stateful Operations | updateStateByKey → mapGroupsWithState · Watermarks · Late data · Arbitrary state |
| M05 | Delta Lake with PySpark | Delta ACID operations · DML (MERGE, UPDATE, DELETE) · Time travel · Vacuum · Optimize |

---

## 📂 Department 6.3 — Stream Processing Theory (DCS-STR)

### 📘 Course DCS-STR-101: Fundamentals of Stream Processing

**Prerequisite:** SYS-DST-101, DBE-INT-103  
**Semester:** 4

| # | Module | Lessons |
|---|--------|---------|
| M01 | The Streaming Model | Event time vs processing time vs ingestion time · Why the distinction matters |
| M02 | Windowing | Tumbling windows · Sliding windows · Session windows · Global windows |
| M03 | Watermarks and Late Data | Watermark definition · Allowed lateness · Dropped late events · Trigger behavior |
| M04 | Exactly-Once Semantics | At-most-once · At-least-once · Exactly-once · Idempotent writes · Transactional sources |
| M05 | State Management | Stateful operators · State backends · RocksDB state · State checkpointing · TTL |

### 📘 Course DCS-STR-102: Streaming Engines in Production

**Prerequisite:** DCS-STR-101, DCS-PYS-101  
**Semester:** 5

| # | Module | Lessons |
|---|--------|---------|
| M01 | Apache Flink Architecture | JobManager · TaskManager · Slots · Task chaining · Savepoints vs checkpoints |
| M02 | Flink vs Spark Streaming | True streaming vs micro-batch · Latency comparison · State management comparison |
| M03 | Streaming Anti-Patterns | State explosion · Missing watermarks · Unbounded windows · Backpressure |
| M04 | Streaming Observability | Lag metrics · Throughput · Processing latency · Checkpoint duration · Backpressure graphs |
| M05 | Streaming Production Patterns | Blue-green streaming deploys · Schema evolution in streams · Reprocessing from source |

---

## 📂 Department 6.4 — Apache Kafka (DCS-KFK)

### 📘 Course DCS-KFK-101: Kafka Architecture and Internals

**Prerequisite:** SYS-DST-102, SYS-NET-102  
**Semester:** 4

| # | Module | Lessons |
|---|--------|---------|
| M01 | Kafka Architecture | Broker · Topic · Partition · Replica · Controller · ZooKeeper vs KRaft |
| M02 | Kafka Storage Internals | Log segment files · Index files · Log compaction · Retention policies |
| M03 | Producer Internals | RecordAccumulator · Sender thread · Batching · Compression · acks=all semantics |
| M04 | Consumer Internals | Fetch request · Consumer group coordinator · Rebalancing protocols · Offset management |
| M05 | Kafka Replication | ISR (In-Sync Replicas) · Leader election · Unclean leader election · Replication factor |

### 📘 Course DCS-KFK-102: Kafka Ecosystem

**Prerequisite:** DCS-KFK-101  
**Semester:** 5

| # | Module | Lessons |
|---|--------|---------|
| M01 | Kafka Streams | KStream vs KTable · Topology · State stores · Interactive queries · Exactly-once |
| M02 | Kafka Connect | Source vs sink connectors · Worker configuration · Single Message Transforms (SMTs) |
| M03 | Schema Registry | Avro/Protobuf/JSON Schema · Compatibility modes · Schema evolution workflow |
| M04 | Kafka Security | SASL authentication · ACLs · TLS · Audit logging · Multi-tenant isolation |
| M05 | Kafka Operations | Partition reassignment · Throttling replication · Consumer lag monitoring · Upgrade procedures |

---

# 🏛️ School 7 — Cloud, Production & Leadership (CPL)

**Purpose:** Cloud data platforms, production engineering discipline, system design, and the leadership skills that distinguish staff engineers from senior engineers.

---

## 📂 Department 7.1 — Cloud Data Platforms (CPL-CLD)

### 📘 Course CPL-CLD-101: Google BigQuery

**Prerequisite:** DBE-SQL-102, DCS-SPK-101  
**Semester:** 5

| # | Module | Lessons |
|---|--------|---------|
| M01 | BigQuery Architecture | Dremel engine · Colossus storage · Borg compute · Slot-based execution model |
| M02 | BigQuery Storage | Capacitor columnar format · Managed partitioned tables · Clustered tables · Table snapshots |
| M03 | Query Optimization | INFORMATION_SCHEMA · Query plan exploration · Slot usage · BI Engine |
| M04 | Cost Management | On-demand vs flat-rate pricing · Reservations · Byte scanning optimization · Cost attribution |
| M05 | Advanced BigQuery | External tables (Biglake) · Materialized views · BQML · Omni (multi-cloud) |

### 📘 Course CPL-CLD-102: Open Table Formats (Iceberg / Delta / Hudi)

**Prerequisite:** DCS-PYS-101, DBE-INT-101  
**Semester:** 5

| # | Module | Lessons |
|---|--------|---------|
| M01 | Table Format Fundamentals | What problem open formats solve · ACID on object storage · Format comparison |
| M02 | Apache Iceberg | Catalog · Snapshot isolation · Manifest list → manifest files → data files · Hidden partitioning |
| M03 | Iceberg Operations | Time travel · Schema evolution · Partition evolution · Expiring snapshots |
| M04 | Delta Lake | Delta log · Checkpoint files · OPTIMIZE · Z-order clustering · VACUUM · DML |
| M05 | Apache Hudi | Copy-on-Write vs Merge-on-Read · Timeline · Upserts · Metadata table · Hudi vs Iceberg |

### 📘 Course CPL-CLD-103: Cloud Data Engineering (Multi-Cloud)

**Prerequisite:** CPL-CLD-101  
**Semester:** 6

| # | Module | Lessons |
|---|--------|---------|
| M01 | AWS Data Stack | S3 · Glue · EMR · Kinesis · Redshift · Lake Formation · AWS Glue Data Catalog |
| M02 | GCP Data Stack | GCS · Dataflow · Dataproc · Pub/Sub · BigQuery · Dataplex · Cloud Composer |
| M03 | Azure Data Stack | ADLS Gen2 · Azure Data Factory · Azure Databricks · Synapse · Event Hubs |
| M04 | Cloud Cost Engineering | RI vs Spot vs On-demand · Storage tiering · Compute right-sizing · Cost allocation tags |
| M05 | Multi-Cloud Architecture | Portability trade-offs · Open standards strategy · Data mesh in cloud · Egress cost management |

---

## 📂 Department 7.2 — Production Engineering (CPL-PRD)

### 📘 Course CPL-PRD-101: Data Systems Observability

**Prerequisite:** SYS-LNX-102, DE-PDA-101  
**Semester:** 5

| # | Module | Lessons |
|---|--------|---------|
| M01 | Observability Fundamentals | Metrics, logs, traces — the three pillars · SLIs, SLOs, SLAs · Error budgets |
| M02 | Metrics and Alerting | Prometheus data model · PromQL · Grafana dashboards · Alert routing (PagerDuty) |
| M03 | Distributed Tracing | OpenTelemetry · Trace context propagation · Span attributes · Trace sampling |
| M04 | Data Quality Monitoring | Row count anomalies · Schema drift detection · Freshness monitoring · Great Expectations |
| M05 | Incident Management | On-call rotation design · Runbook standards · Severity classification · Blameless post-mortems |

### 📘 Course CPL-PRD-102: CI/CD for Data Engineering

**Prerequisite:** PRG-SE-101, DE-TRF-101  
**Semester:** 5

| # | Module | Lessons |
|---|--------|---------|
| M01 | CI/CD Principles for Data | Infrastructure as Code · Immutable deployments · Rollback strategy · Blue-green for pipelines |
| M02 | Testing in CI | dbt CI checks · Spark unit tests · Data contract validation · Schema regression tests |
| M03 | GitHub Actions for Data | Workflow syntax · Matrix builds · Secrets management · Self-hosted runners |
| M04 | Container and Infrastructure | Docker for data pipelines · Helm for Spark · Terraform for cloud resources |
| M05 | Deployment Patterns | Canary deploys · Feature flags · Shadow mode · Progressive rollout |

---

## 📂 Department 7.3 — System Design (CPL-SYD)

### 📘 Course CPL-SYD-101: Data System Design

**Prerequisite:** All prior schools  
**Semester:** 6

| # | Module | Lessons |
|---|--------|---------|
| M01 | Design Framework | Requirements gathering · Capacity estimation · API design · Data model first |
| M02 | Design a Batch Pipeline | Ingestion patterns · Transformation architecture · Serving layer · SLAs |
| M03 | Design a Streaming Platform | Source connectors · Processing topology · Sink connectors · Backpressure |
| M04 | Design a Data Warehouse | Storage layer · Compute separation · Query routing · Caching strategy · Cost model |
| M05 | Design a Lakehouse | Storage format · Catalog · Compute engines · Governance · Cost optimization |

### 📘 Course CPL-SYD-102: Advanced Architecture Patterns

**Prerequisite:** CPL-SYD-101  
**Semester:** 6

| # | Module | Lessons |
|---|--------|---------|
| M01 | Data Mesh | Domain ownership · Data as a product · Self-serve platform · Federated governance |
| M02 | Lambda and Kappa Architecture | Batch layer vs speed layer · Why Kappa superseded Lambda · When Lambda still applies |
| M03 | Event-Driven Architecture | Event sourcing · CQRS · Outbox pattern · Event store design |
| M04 | Enterprise Data Platform Design | Platform vs pipeline distinction · Self-service analytics · Metadata-driven architectures |
| M05 | Architecture Trade-off Analysis | How to evaluate a design · Failure mode analysis · Operational complexity scoring |

---

## 📂 Department 7.4 — Technical Leadership (CPL-LDR)

### 📘 Course CPL-LDR-101: Interview Preparation

**Prerequisite:** All prior schools  
**Semester:** 6

| # | Module | Lessons |
|---|--------|---------|
| M01 | SQL Interview Mastery | Common patterns · Time-series queries · Sessionization · Window function challenges |
| M02 | Python & DSA Interview | LeetCode patterns for DE · Heap problems · Graph problems · Custom data structure questions |
| M03 | Spark Interview Deep Dive | Architecture questions · Optimization scenarios · Debugging walkthroughs |
| M04 | System Design Interview | Framework · Scoping · Trade-off presentation · Common DE system design questions |
| M05 | Behavioral Interview | STAR method for DE · Leadership examples · Conflict resolution · Failure stories |

### 📘 Course CPL-LDR-102: Staff and Principal Engineering

**Prerequisite:** CPL-LDR-101  
**Semester:** 6

| # | Module | Lessons |
|---|--------|---------|
| M01 | Technical Strategy | Technology radar · Build vs buy decisions · Deprecation strategy · Tech debt management |
| M02 | Architecture Decision Records | ADR format · When to write one · Decision lifecycle · Communicating decisions |
| M03 | Engineering Influence | Technical writing · Design review process · RFC process · Persuading without authority |
| M04 | Mentorship and Leveling | 1:1 structure · Giving feedback · Growth plans · Sponsorship vs mentorship |
| M05 | Principal Engineering Habits | Research practice · Teaching back · Red team reviews · Staying current without drowning |

---

## Summary Counts

| Level | Count |
|---|---|
| Schools | 7 |
| Departments | 18 |
| Courses | 35 |
| Modules | 175 |
| Estimated Lessons | ~700 |

---

## Semester Assignment Summary

| Semester | Schools / Departments |
|---|---|
| 0 | CSF-ARC (full) · CSF-OS (full) |
| 1 | CSF-ALG · SYS-LNX · PRG-PY-101 |
| 2 | SYS-NET · SYS-DST · DBE-INT · PRG-PY-102 · PRG-SE |
| 3 | DBE-SQL · DBE-SDS · PRG-PY-103 · DE-PDA |
| 4 | DE-MOD · DE-ORC · DE-TRF · DCS-SPK · DCS-STR-101 · DCS-KFK-101 |
| 5 | DCS-PYS · DCS-STR-102 · DCS-KFK-102 · CPL-CLD-101/102 · CPL-PRD |
| 6 | CPL-CLD-103 · CPL-SYD · CPL-LDR |
