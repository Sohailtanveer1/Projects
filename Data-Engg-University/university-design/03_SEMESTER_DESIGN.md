# Semester Design

Seven semesters, numbered 0–6. Semester 0 is the foundation; Semester 6 is staff-level synthesis. Each semester has explicit goals, required prior knowledge, courses, skills gained, estimated hours, capstone project, and graduation criteria.

Semesters are dependency-locked, not time-locked: you may not start a semester until you can demonstrate the graduation criteria of the previous one.

---

## Semester 0 — The Machine

**Tagline:** *Before you build systems, understand what runs them.*

### Goals
Build the foundational mental model of computer hardware and operating systems. After this semester, every performance conversation has a hardware anchor.

### Courses
| Code | Course | Hours |
|---|---|---|
| CSF-ARC-101 | How Computers Execute Programs | 30 |
| CSF-ARC-102 | Memory Architecture | 30 |
| CSF-OS-101 | OS Internals | 30 |
| CSF-OS-102 | Concurrency Fundamentals | 25 |

**Total: ~115 hours**

### Required Prior Knowledge
- Basic programming (loops, functions, variables) in any language.
- Comfortable using a terminal to run commands.

### Skills Gained
- Explain the fetch-decode-execute cycle, CPU registers, and pipelining.
- Describe the memory hierarchy with correct latency numbers.
- Explain virtual memory, page faults, and the OS scheduler.
- Describe the GIL and Python concurrency model.
- Explain cache lines, false sharing, and why columnar formats are faster.

### Difficulty Progression
Week 1–2: CPU architecture (concrete, physical) → Week 3–4: memory hierarchy (more abstract) → Week 5–6: OS internals (requires CPU + memory) → Week 7–8: concurrency (requires OS).

### Capstone Project — "Profile Before You Optimize"

A provided Python script processes 10 million rows using a naive loop. The student must:

1. Establish a baseline runtime using `time` and `cProfile`.
2. Identify the bottleneck as CPU-bound, memory-bound, or I/O-bound using `cProfile`, `tracemalloc`, and `perf stat`.
3. Apply one targeted fix: vectorization (NumPy), memory access restructuring, or reduced syscall overhead.
4. Re-benchmark and quantify the improvement.
5. Write a 500-word technical explanation of *why* the optimization worked, citing specific hardware concepts (cache lines, pipeline stalls, reference counting overhead, etc.).

**Pass criteria:** Measured ≥2× speedup with a correct hardware-level explanation.

### Graduation Criteria
A student may proceed to Semester 1 when they can, without notes:
- [ ] Explain the fetch-decode-execute cycle in under 2 minutes.
- [ ] Draw the memory latency ladder (registers → L1 → RAM → SSD) with correct order-of-magnitude numbers.
- [ ] Explain why NumPy sum is ~150× faster than a Python loop (at the hardware level).
- [ ] Explain what a cache line is and how it relates to Parquet vs CSV performance.
- [ ] Describe the Python GIL and give two strategies for working around it.
- [ ] Complete the capstone with a measured, explained speedup of ≥2×.

---

## Semester 1 — The Runtime Environment

**Tagline:** *Linux is the operating system of data engineering. Know it cold.*

### Goals
Master Linux as a diagnostic and operational environment. Understand Python deeply enough to avoid its performance traps. Build algorithmic intuition for interview scenarios.

### Courses
| Code | Course | Hours |
|---|---|---|
| SYS-LNX-101 | Linux for Data Engineers | 35 |
| SYS-LNX-102 | Linux Performance & Operations | 30 |
| PRG-PY-101 | Python Internals | 35 |
| CSF-ALG-101 | Data Structures & Algorithms | 30 |

**Total: ~130 hours**

### Required Prior Knowledge
- Semester 0 graduation criteria met.
- Comfortable using a Linux terminal (ls, cd, cat, grep, pipes).

### Skills Gained
- Navigate, diagnose, and profile a Linux system.
- Write robust shell scripts with error handling, logging, and retry.
- Read CPython bytecode and explain interpreter overhead.
- Diagnose Python memory leaks with `tracemalloc`.
- Select the right data structure for a given access pattern.
- Analyze algorithm time/space complexity.

### Difficulty Progression
Weeks 1–4: Linux (hands-on, immediately useful) → Weeks 5–6: Python internals (conceptually layered on OS) → Weeks 7–8: Algorithms (abstract but interview-critical).

### Capstone Project — "Linux Monitoring Tool"

Build a Python CLI tool that:
1. Reads system metrics from `/proc`: CPU usage per core, memory usage, disk I/O per device, network I/O per interface.
2. Displays a live dashboard (updated every 2 seconds) using only the standard library (no psutil).
3. Detects and alerts when CPU > 80%, memory > 90%, or disk utilization > 95%.
4. Logs alerts to a rotating log file using `logging`.
5. Ships with a shell script that installs it as a systemd service.

**Pass criteria:** Tool runs correctly; shell script installs and starts cleanly; code passes `mypy` and `ruff` checks; student can explain any line of code under cross-examination.

### Graduation Criteria
- [ ] Use `strace` to trace a Python script's system calls and identify a specific syscall overhead.
- [ ] Explain what happens between `fork()` and the first instruction of the child process.
- [ ] Describe how the Linux page cache interacts with `write()` and `fsync()`.
- [ ] Write a shell script with `set -euo pipefail`, trap handlers, and log rotation.
- [ ] Use `dis.dis()` on a Python function and explain the bytecode.
- [ ] Implement a hash table, heap, and sorted set from scratch in Python.
- [ ] Complete and defend the capstone project.

---

## Semester 2 — Networks and Distributed Systems

**Tagline:** *Every data pipeline is a distributed system. Every distributed system can fail.*

### Goals
Build the theoretical and practical foundation for understanding all distributed data systems: networking, consensus, replication, and distributed transactions.

### Courses
| Code | Course | Hours |
|---|---|---|
| SYS-NET-101 | Networking Fundamentals | 35 |
| SYS-NET-102 | Network Engineering for Data Systems | 30 |
| SYS-DST-101 | Foundations of Distributed Systems | 40 |
| SYS-DST-102 | Consensus and Replication | 40 |
| PRG-PY-102 | Python Concurrency and Async | 30 |
| PRG-SE-101 | Engineering Practices | 25 |

**Total: ~200 hours**

### Required Prior Knowledge
- Semester 1 graduation criteria met.

### Skills Gained
- Explain TCP/IP, TLS, DNS failure modes, and HTTP/2.
- Design a VPC with appropriate subnets, security groups, and private endpoints.
- Classify any distributed system as CP or AP with justification.
- Whiteboard Raft leader election and log replication.
- Explain 2PC and the Saga pattern and choose between them for a use case.
- Write async Python code using asyncio for concurrent I/O operations.
- Apply pytest, mypy, and pre-commit hooks to a Python project.

### Difficulty Progression
Weeks 1–4: Networking (concrete, protocol-level) → Weeks 5–6: Python async (builds on networking) → Weeks 7–10: Distributed systems theory (most abstract) → Weeks 11–12: Engineering practices (practical).

### Capstone Project — "Distributed Key-Value Store"

Implement a simplified distributed key-value store in Python:
1. Three nodes that can be started as separate processes.
2. Leader election using a simple token ring or basic Raft-inspired protocol.
3. Replicated writes (leader broadcasts to followers, majority acknowledgment required).
4. Read from any node (eventual consistency).
5. Handles one node failure gracefully (remaining two nodes continue to serve).
6. CLI client: `./client get key`, `./client set key value`.
7. Network communication using Python sockets (no frameworks).

**Pass criteria:** Three-node cluster survives a single node kill; writes are durable after leader election; student can explain every design decision.

### Graduation Criteria
- [ ] Explain the TCP three-way handshake, congestion control, and TIME_WAIT.
- [ ] Draw a VPC architecture for a Kafka cluster with private subnets and security groups.
- [ ] Classify Kafka (AP or CP) with a nuanced justification referencing acks, ISR, and min.insync.replicas.
- [ ] Whiteboard Raft leader election in under 5 minutes.
- [ ] Explain the difference between 2PC and the Saga pattern and give a scenario where each is correct.
- [ ] Write an asyncio coroutine that fetches data from 100 URLs concurrently with a semaphore limit.
- [ ] Complete and defend the capstone project.

---

## Semester 3 — Databases and Core Data Engineering

**Tagline:** *SQL is the language. The storage engine is the grammar.*

### Goals
Master database internals, advanced SQL, and the foundational patterns of data engineering. Build the first complete batch pipeline.

### Courses
| Code | Course | Hours |
|---|---|---|
| DBE-INT-101 | Storage Engines | 35 |
| DBE-INT-102 | Indexing and Query Processing | 35 |
| DBE-INT-103 | Transactions and Isolation | 30 |
| DBE-SQL-101 | SQL Internals and Optimization | 35 |
| DBE-SQL-102 | Advanced SQL for Data Engineers | 35 |
| PRG-PY-103 | Python for Data Engineering | 35 |
| DE-PDA-101 | Data Pipeline Fundamentals | 30 |
| DE-PDA-102 | Data Lake Architecture | 30 |

**Total: ~265 hours**

### Required Prior Knowledge
- Semester 2 graduation criteria met.
- Comfortable with basic SQL (SELECT, JOIN, GROUP BY, subqueries).

### Skills Gained
- Explain B-tree vs LSM-tree storage engines with production examples.
- Read and interpret any EXPLAIN/EXPLAIN ANALYZE output.
- Write window functions, recursive CTEs, and advanced aggregations.
- Explain MVCC and snapshot isolation.
- Build a batch ELT pipeline with schema validation, retry, and alerting.
- Design a Bronze/Silver/Gold data lake.
- Use pandas, polars, and pyarrow fluently.

### Difficulty Progression
Weeks 1–6: Database internals (heavy theory) → Weeks 7–10: Advanced SQL (practical theory) → Weeks 11–14: Python data tools + pipeline patterns (applied).

### Capstone Project — "SQL Analytics Platform"

Using the NYC Taxi & Limousine Commission public dataset (publicly available):
1. Load 3 years of trip data (>100M rows) into Postgres.
2. Design and build a star schema (fact_trips, dim_vendor, dim_zone, dim_date).
3. Create indexes and measure query performance improvement (before/after EXPLAIN ANALYZE).
4. Write 10 analytical SQL queries using window functions, CTEs, and advanced aggregations.
5. Build a Python pipeline (pandas + pyarrow) that loads incremental data into the star schema.
6. Write a data quality check that validates row counts, null rates, and value ranges.

**Pass criteria:** All 10 queries produce correct results; student can explain any EXPLAIN plan; Python pipeline is idempotent.

### Graduation Criteria
- [ ] Explain the difference between a B-tree and an LSM-tree and give a production example of each.
- [ ] Read an EXPLAIN ANALYZE plan and identify the most expensive node.
- [ ] Write a SQL query computing 7-day rolling average revenue per driver using a window function.
- [ ] Explain MVCC and how it prevents dirty reads without blocking.
- [ ] Design a Bronze/Silver/Gold lake for a given data source.
- [ ] Complete and defend the capstone project.

---

## Semester 4 — Distributed Compute and the Modern Data Stack

**Tagline:** *Build the stack that runs at millions of rows per second.*

### Goals
Master Apache Spark, Kafka, Airflow, dbt, and data modeling. Integrate them into a coherent modern data stack. Build a full ELT platform.

### Courses
| Code | Course | Hours |
|---|---|---|
| DE-MOD-101 | Dimensional Modeling | 35 |
| DE-MOD-102 | Modern Data Modeling | 30 |
| DE-ORC-101 | Apache Airflow | 40 |
| DE-TRF-101 | dbt | 40 |
| DCS-SPK-101 | Spark Architecture and Internals | 45 |
| DCS-SPK-102 | Spark Performance Engineering | 40 |
| DCS-STR-101 | Fundamentals of Stream Processing | 35 |
| DCS-KFK-101 | Kafka Architecture and Internals | 40 |
| DBE-SDS-101 | Database Replication and Sharding | 30 |

**Total: ~335 hours**

### Required Prior Knowledge
- Semester 3 graduation criteria met.

### Skills Gained
- Design a star schema and implement SCD Type 2.
- Write a dbt project with incremental models, tests, and documentation.
- Build an Airflow DAG with correct dependency management, XCom, and alerting.
- Explain Spark's DAG, stages, tasks, and shuffle.
- Tune a Spark job: broadcast join, partitioning, AQE.
- Explain watermarks, windows, and exactly-once semantics in stream processing.
- Configure Kafka producers and consumers for production throughput and durability.

### Difficulty Progression
Weeks 1–4: Data modeling (theory-first) → Weeks 5–8: Airflow + dbt (hands-on stack) → Weeks 9–14: Spark architecture and tuning → Weeks 15–18: Kafka + streaming fundamentals.

### Capstone Project — "Mini Data Warehouse + Orchestrated Pipeline"

Build a complete modern data stack using Docker Compose:
1. **Source:** PostgreSQL database with synthetic e-commerce data (orders, customers, products).
2. **Ingestion:** Python + Airbyte-style connector (custom) that extracts to S3-compatible storage (MinIO).
3. **Transform:** dbt project on DuckDB with star schema (fact_orders, dim_customer, dim_product, dim_date), SCD Type 2 on dim_customer, 5 data tests.
4. **Orchestration:** Airflow DAG that runs ingestion → dbt daily, with failure alerts.
5. **Serving:** 3 SQL reports answering business questions (e.g., revenue by product category, customer cohort retention).
6. **CI:** GitHub Actions runs dbt tests on every PR.

**Pass criteria:** Full pipeline runs end-to-end; dbt tests pass; Airflow DAG handles a simulated upstream failure gracefully; student defends every design decision.

### Graduation Criteria
- [ ] Whiteboard the Spark execution model (DAG → stages → tasks) with correct shuffle boundary identification.
- [ ] Diagnose a Spark OOM error from a provided stack trace.
- [ ] Write a dbt incremental model with a correct unique key strategy.
- [ ] Explain Kafka's ISR and how `acks=all` + `min.insync.replicas=2` provides durability.
- [ ] Explain the difference between event time and processing time and why watermarks exist.
- [ ] Complete and defend the capstone project.

---

## Semester 5 — Streaming, PySpark, and Cloud

**Tagline:** *Real-time. Cloud-native. Production-ready.*

### Goals
Master streaming (PySpark Structured Streaming, Flink, Kafka Streams), cloud data warehousing (BigQuery), open table formats, and production engineering (observability, CI/CD).

### Courses
| Code | Course | Hours |
|---|---|---|
| DCS-PYS-101 | PySpark Engineering | 45 |
| DCS-STR-102 | Streaming Engines in Production | 40 |
| DCS-KFK-102 | Kafka Ecosystem | 40 |
| CPL-CLD-101 | Google BigQuery | 40 |
| CPL-CLD-102 | Open Table Formats (Iceberg/Delta/Hudi) | 40 |
| CPL-PRD-101 | Data Systems Observability | 35 |
| CPL-PRD-102 | CI/CD for Data Engineering | 30 |

**Total: ~270 hours**

### Required Prior Knowledge
- Semester 4 graduation criteria met.

### Skills Gained
- Write PySpark Structured Streaming with watermarks, stateful operations, and Delta Lake sinks.
- Explain the internal file layout of an Iceberg table.
- Optimize a BigQuery query by 10× using clustering, partitioning, and materialized views.
- Configure Kafka Connect with a schema registry and backward-compatible schema evolution.
- Build a CI/CD pipeline for a data project using GitHub Actions.
- Define SLOs and write Prometheus/Grafana dashboards for pipeline monitoring.

### Difficulty Progression
Weeks 1–6: PySpark + streaming production (builds directly on Semester 4) → Weeks 7–10: Cloud platform + table formats (applied distributed storage) → Weeks 11–14: Production engineering (operational hardening).

### Capstone Project — "Real-Time E-Commerce Streaming Platform"

Build a streaming analytics platform:
1. **Producer:** Python app publishing synthetic click events (user_id, product_id, action, timestamp) to Kafka at 10K events/second. Avro schema in Schema Registry.
2. **Stream Processing:** PySpark Structured Streaming with 5-minute tumbling windows. Output: per-product click counts with watermark (allow 10-minute late data). Sink to Delta Lake.
3. **Batch Layer:** Airflow DAG that runs daily dbt models over Delta Lake Gold layer.
4. **BigQuery:** External table over Delta Lake for BI tool access. Query optimization: clustering by product_id.
5. **Observability:** Prometheus metrics (events/sec, consumer lag, batch duration). Grafana dashboard. Alert when lag > 60 seconds.
6. **CI/CD:** GitHub Actions runs dbt tests and PySpark unit tests on every PR.

**Pass criteria:** End-to-end pipeline runs; metrics visible in Grafana; a simulated consumer failure recovers without data loss; student defends all architectural choices.

### Graduation Criteria
- [ ] Explain the internal structure of an Iceberg table (manifest list → manifest files → data files).
- [ ] Optimize a provided BigQuery query (reduce bytes scanned by ≥50%, measured via INFORMATION_SCHEMA).
- [ ] Configure a Kafka producer for exactly-once delivery (all three required settings).
- [ ] Write a Structured Streaming job that handles late events correctly (demonstrate with a test).
- [ ] Define an SLO and write the PromQL alert rule for it.
- [ ] Complete and defend the capstone project.

---

## Semester 6 — System Design, Cloud Architecture, and Leadership

**Tagline:** *Design the platform. Lead the team. Graduate ready for staff.*

### Goals
Synthesize all prior learning into staff-level system design capability and technical leadership competence. Build an enterprise-grade data platform.

### Courses
| Code | Course | Hours |
|---|---|---|
| CPL-CLD-103 | Cloud Data Engineering (Multi-Cloud) | 45 |
| CPL-SYD-101 | Data System Design | 50 |
| CPL-SYD-102 | Advanced Architecture Patterns | 40 |
| CPL-LDR-101 | Interview Preparation | 40 |
| CPL-LDR-102 | Staff and Principal Engineering | 35 |

**Total: ~210 hours**

### Required Prior Knowledge
- Semester 5 graduation criteria met.

### Skills Gained
- Lead a 45-minute system design session for any DE system.
- Explain data mesh, event-driven architecture, and Lambda/Kappa trade-offs.
- Design a multi-cloud data platform with cost controls and governance.
- Write ADRs for architectural decisions.
- Communicate trade-offs to non-technical stakeholders.
- Mentor junior and mid-level engineers.

### Difficulty Progression
Weeks 1–4: Multi-cloud and architecture patterns (breadth) → Weeks 5–10: System design practice (depth + synthesis) → Weeks 11–14: Leadership skills and interview preparation.

### Capstone Project — "Enterprise Data Platform"

Design and partially implement a lakehouse for a fintech company processing 50B events/day:

**Part 1 — Architecture Document (25 pages minimum):**
- Full architecture diagram (Mermaid + written explanation).
- Data ingestion design (Kafka + batch S3).
- Bronze/Silver/Gold on Apache Iceberg.
- Orchestration: Airflow DAG dependency map.
- Serving: BigQuery external tables + dbt Gold models.
- Governance: data catalog, lineage, access control.
- Observability: SLO definition, dashboards, alert runbook.
- Cost model: estimated monthly GCP cost with optimization plan.
- Three ADRs: one for table format choice, one for orchestrator choice, one for streaming vs micro-batch.

**Part 2 — Scaled-Down Implementation:**
- PySpark Silver layer transformation on a provided 1GB sample dataset.
- dbt Gold layer with 5 models, 10 tests, and full documentation.
- GitHub Actions CI pipeline.
- 3 unit tests for the PySpark transformation.

**Part 3 — Incident Response:**
Given a provided incident report (Gold layer SLA breach), write a complete post-mortem: timeline, root cause, impact, remediation, five preventive measures.

**Pass criteria:** Architecture document reviewed and approved; implementation runs end-to-end; post-mortem identifies real root cause; student defends all choices under 30-minute cross-examination.

### Graduation Criteria
- [ ] Lead a 45-minute system design session for "Design a streaming data warehouse" without notes.
- [ ] Write a data mesh architecture document explaining domain decomposition for a given company.
- [ ] Explain three consistency models (eventual, sequential, linearizable) with a concrete example of each.
- [ ] Write an ADR for a technology choice, including at least 3 alternatives considered.
- [ ] Complete all three parts of the capstone project.

---

## Semester Summary

| Semester | Theme | Hours | Difficulty | Capstone |
|---|---|---|---|---|
| 0 | The Machine | ~115 | ★★★☆☆ | Linux Profiling Tool |
| 1 | The Runtime | ~130 | ★★★☆☆ | Linux Monitoring Tool |
| 2 | Networks & Distributed Theory | ~200 | ★★★★☆ | Distributed Key-Value Store |
| 3 | Databases & Core DE | ~265 | ★★★★☆ | SQL Analytics Platform |
| 4 | Distributed Compute & Modern Stack | ~335 | ★★★★★ | Mini Data Warehouse |
| 5 | Streaming, Cloud & Production | ~270 | ★★★★★ | Real-Time Streaming Platform |
| 6 | System Design & Leadership | ~210 | ★★★★★ | Enterprise Data Platform |
| **Total** | | **~1,525 hrs** | | |
