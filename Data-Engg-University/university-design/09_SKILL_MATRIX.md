# Skill Matrix

Expected proficiency levels by semester and by engineering level. Used to set expectations, self-assess, and plan study.

**Proficiency Scale:**
- **0 — None:** No exposure.
- **1 — Awareness:** Know what it is; cannot use it.
- **2 — Basic:** Can use it for simple tasks with documentation.
- **3 — Proficient:** Can use it independently for common tasks.
- **4 — Advanced:** Can tune, debug, and teach it.
- **5 — Expert:** Deep internals knowledge; can design systems around it; can interview others on it.

---

## Skill Progression by Semester

### Programming

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Python fundamentals | 2 | 3 | 3 | 4 | 4 | 4 | 5 |
| Python internals (CPython, GIL, bytecode) | 0 | 3 | 4 | 4 | 4 | 5 | 5 |
| Python concurrency (asyncio, threading) | 0 | 1 | 3 | 4 | 4 | 4 | 5 |
| Python memory management | 0 | 2 | 3 | 4 | 4 | 4 | 5 |
| Python testing (pytest, mypy) | 1 | 2 | 3 | 4 | 4 | 5 | 5 |
| Shell scripting (bash) | 1 | 3 | 3 | 3 | 4 | 4 | 4 |
| Data structures & algorithms | 1 | 3 | 3 | 4 | 4 | 4 | 5 |

### SQL

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Basic SQL (SELECT, JOIN, GROUP BY) | 1 | 2 | 2 | 4 | 4 | 4 | 5 |
| Window functions | 0 | 0 | 1 | 4 | 5 | 5 | 5 |
| CTEs and recursive CTEs | 0 | 0 | 1 | 4 | 4 | 5 | 5 |
| Query optimization (EXPLAIN plans) | 0 | 0 | 1 | 4 | 4 | 5 | 5 |
| Advanced aggregations (ROLLUP, CUBE) | 0 | 0 | 0 | 3 | 4 | 4 | 5 |
| SQL for analytics (sessionization, funnels) | 0 | 0 | 0 | 3 | 4 | 4 | 5 |
| BigQuery SQL (partitioning, clustering) | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| dbt SQL patterns | 0 | 0 | 0 | 1 | 4 | 5 | 5 |

### Data Engineering Patterns

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Batch pipeline design | 0 | 0 | 0 | 3 | 4 | 4 | 5 |
| ELT/ETL architecture | 0 | 0 | 0 | 3 | 4 | 5 | 5 |
| Medallion architecture | 0 | 0 | 0 | 3 | 4 | 5 | 5 |
| Data lake design | 0 | 0 | 0 | 3 | 4 | 5 | 5 |
| Idempotency patterns | 0 | 0 | 1 | 3 | 4 | 5 | 5 |
| Data contracts | 0 | 0 | 0 | 2 | 3 | 4 | 5 |
| Data quality frameworks | 0 | 0 | 0 | 2 | 3 | 4 | 5 |
| Dimensional modeling | 0 | 0 | 0 | 1 | 4 | 4 | 5 |
| Data Vault | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| Schema evolution | 0 | 0 | 1 | 2 | 3 | 4 | 5 |

### Apache Spark

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Spark architecture (driver/executor/DAG) | 0 | 0 | 0 | 0 | 4 | 4 | 5 |
| DataFrame API | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| Shuffle and partitioning | 0 | 0 | 0 | 0 | 4 | 4 | 5 |
| Spark performance tuning | 0 | 0 | 0 | 0 | 3 | 5 | 5 |
| PySpark UDFs (Python + Pandas) | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| Spark UI (reading and diagnosing) | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| Adaptive Query Execution (AQE) | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| Delta Lake (ACID, MERGE, time travel) | 0 | 0 | 0 | 0 | 2 | 4 | 5 |

### Streaming

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Event time vs processing time | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| Windowing (tumbling, sliding, session) | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| Watermarks | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| Exactly-once semantics | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| Structured Streaming | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| Apache Flink architecture | 0 | 0 | 0 | 0 | 1 | 3 | 4 |
| Flink vs Spark trade-offs | 0 | 0 | 0 | 0 | 1 | 3 | 5 |

### Apache Kafka

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Kafka architecture (broker/topic/partition) | 0 | 0 | 0 | 0 | 4 | 4 | 5 |
| Producer configuration (acks, batching) | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| Consumer groups and rebalancing | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| Kafka Connect | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| Schema Registry (Avro/Protobuf) | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| Kafka Streams | 0 | 0 | 0 | 0 | 1 | 3 | 4 |
| Kafka operations (rebalancing, throttling) | 0 | 0 | 0 | 0 | 2 | 3 | 5 |

### Cloud

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Cloud fundamentals (VPC, IAM, object storage) | 0 | 0 | 2 | 2 | 3 | 3 | 4 |
| BigQuery architecture + optimization | 0 | 0 | 0 | 0 | 1 | 4 | 5 |
| BigQuery cost management | 0 | 0 | 0 | 0 | 1 | 3 | 5 |
| Open table formats (Iceberg/Delta/Hudi) | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| Multi-cloud architecture | 0 | 0 | 0 | 0 | 1 | 2 | 4 |
| Terraform / IaC | 0 | 0 | 0 | 0 | 1 | 3 | 4 |
| Container orchestration (Docker/K8s basics) | 0 | 1 | 2 | 2 | 3 | 3 | 4 |

### Orchestration & Transformation

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Apache Airflow (architecture, DAGs) | 0 | 0 | 0 | 1 | 4 | 4 | 5 |
| Airflow in production (KubernetesExecutor, scaling) | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| dbt (models, materializations, tests) | 0 | 0 | 0 | 1 | 4 | 5 | 5 |
| dbt advanced (macros, packages, CI) | 0 | 0 | 0 | 0 | 3 | 4 | 5 |

### Architecture & System Design

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Distributed systems theory (CAP, Raft, 2PC) | 0 | 0 | 4 | 4 | 4 | 5 | 5 |
| Data warehouse design | 0 | 0 | 0 | 2 | 4 | 4 | 5 |
| Data lake architecture | 0 | 0 | 0 | 3 | 4 | 5 | 5 |
| Lakehouse architecture | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| Data mesh principles | 0 | 0 | 0 | 0 | 1 | 2 | 4 |
| System design interview skills | 0 | 0 | 0 | 1 | 2 | 3 | 5 |
| Architecture Decision Records (ADRs) | 0 | 0 | 1 | 1 | 2 | 3 | 5 |

### Debugging & Observability

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Linux performance tools (perf, strace, iostat) | 0 | 4 | 4 | 4 | 4 | 4 | 5 |
| Python profiling (cProfile, tracemalloc) | 1 | 3 | 4 | 4 | 4 | 4 | 5 |
| Spark UI debugging | 0 | 0 | 0 | 0 | 3 | 4 | 5 |
| Kafka consumer lag monitoring | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| Distributed tracing (OpenTelemetry) | 0 | 0 | 0 | 0 | 1 | 3 | 4 |
| Prometheus + Grafana | 0 | 0 | 0 | 0 | 1 | 3 | 5 |
| Structured logging | 0 | 1 | 2 | 3 | 3 | 4 | 5 |
| Post-mortem writing | 0 | 0 | 1 | 2 | 3 | 4 | 5 |

### Security

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| TLS / mTLS concepts | 0 | 0 | 3 | 3 | 3 | 4 | 4 |
| IAM and RBAC for cloud | 0 | 0 | 1 | 2 | 3 | 4 | 4 |
| Data encryption (at rest and in transit) | 0 | 0 | 2 | 2 | 3 | 4 | 4 |
| GDPR / data privacy principles | 0 | 0 | 1 | 1 | 2 | 3 | 4 |
| Kafka security (SASL, ACLs) | 0 | 0 | 0 | 0 | 2 | 3 | 4 |
| Secrets management (Vault, cloud secrets) | 0 | 0 | 1 | 2 | 3 | 4 | 4 |

### Cost Optimization

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Cloud cost modeling | 0 | 0 | 0 | 0 | 1 | 3 | 5 |
| Spark cost optimization (right-sizing) | 0 | 0 | 0 | 0 | 2 | 4 | 5 |
| BigQuery cost optimization | 0 | 0 | 0 | 0 | 1 | 4 | 5 |
| Storage tiering (hot/warm/cold) | 0 | 0 | 0 | 1 | 2 | 3 | 5 |
| Kafka retention cost management | 0 | 0 | 0 | 0 | 2 | 3 | 4 |

### Leadership & Communication

| Skill | Sem 0 | Sem 1 | Sem 2 | Sem 3 | Sem 4 | Sem 5 | Sem 6 |
|---|---|---|---|---|---|---|---|
| Technical writing | 1 | 2 | 2 | 3 | 3 | 4 | 5 |
| Technical communication to non-engineers | 0 | 0 | 1 | 1 | 2 | 3 | 5 |
| Architecture review and critique | 0 | 0 | 0 | 1 | 2 | 3 | 5 |
| Mentorship and code review | 0 | 0 | 1 | 2 | 2 | 3 | 4 |
| Interview skills (giving and receiving) | 0 | 0 | 0 | 1 | 2 | 3 | 5 |

---

## Target Proficiency by Engineering Level

| Skill Category | Junior (L3) | Mid (L4) | Senior (L5) | Staff (L6) | Principal (L7) |
|---|---|---|---|---|---|
| Python (internals) | 2 | 3 | 4 | 5 | 5 |
| SQL (advanced) | 2 | 3 | 4 | 5 | 5 |
| Spark (architecture + tuning) | 1 | 3 | 4 | 5 | 5 |
| Kafka (internals) | 1 | 2 | 4 | 5 | 5 |
| Streaming (theory + production) | 1 | 2 | 3 | 5 | 5 |
| Distributed systems theory | 1 | 2 | 4 | 5 | 5 |
| Storage engines | 1 | 2 | 3 | 5 | 5 |
| Data modeling | 2 | 3 | 4 | 5 | 5 |
| Cloud platform | 2 | 3 | 4 | 5 | 5 |
| System design | 1 | 2 | 3 | 5 | 5 |
| Observability + debugging | 2 | 3 | 4 | 5 | 5 |
| Security | 1 | 2 | 3 | 4 | 5 |
| Cost optimization | 0 | 2 | 3 | 5 | 5 |
| Leadership + communication | 1 | 2 | 3 | 4 | 5 |

---

## Self-Assessment Test (one question per skill category)

Use these to gauge your current level before starting a learning path.

| Category | Self-Assessment Question |
|---|---|
| Python internals | Can you explain the GIL, when it's released, and two production consequences of it? |
| SQL | Can you write a query that computes a 7-day rolling average without a subquery? |
| Spark | Can you explain what happens during a shuffle in terms of the DAG, stage boundaries, and network I/O? |
| Kafka | Can you explain ISR, what happens when min.insync.replicas is violated, and how a consumer group rebalances? |
| Streaming | Can you explain the difference between event time and processing time and why watermarks exist? |
| Distributed systems | Can you whiteboard Raft leader election and log replication in 5 minutes? |
| Storage engines | Can you explain the write amplification trade-off between B-tree and LSM-tree? |
| Data modeling | Can you design a star schema for a retail e-commerce domain with SCD Type 2 on the customer dimension? |
| Cloud | Can you explain how BigQuery separates storage (Colossus) from compute (Dremel) and what that means for cost? |
| System design | Can you lead a 45-minute "Design a streaming analytics platform" session with requirements clarification, capacity estimation, and architecture diagram? |
| Debugging | Can you identify a data skew issue in a Spark job from the Spark UI and propose two mitigations? |
| Security | Can you explain mTLS, when you'd require it, and how to configure it in Kafka? |
| Cost | Can you estimate the monthly BigQuery cost for scanning 1 TB/day and propose three ways to reduce it? |
| Leadership | Can you write an ADR for a technology choice, including three alternatives and a consequence analysis? |
