# School Profiles

Detailed profile for each of the seven schools. Each profile answers: what does this school teach, why does it exist, who needs it, and what does a graduate know that they didn't know before.

---

## School 1 — Computer Science Foundations (CSF)

**Tagline:** *Understand the machine before you build on it.*

### Purpose
Data engineers work with systems that execute on real hardware — CPUs, memory buses, disks, and networks with finite latency and throughput. Without understanding the hardware, engineers treat the computer as a magic black box and can only tune systems by trial and error. This school eliminates that.

### Departments
- 1.1 Computer Architecture (CSF-ARC)
- 1.2 Operating Systems (CSF-OS)
- 1.3 Algorithms & Complexity (CSF-ALG)

### Learning Objectives
- Trace the path of a Python expression from source code to CPU instruction and back.
- Explain why columnar storage is faster than row storage for aggregation queries.
- Explain what a cache line is, why cache misses are expensive, and how to write cache-friendly code.
- Describe the fetch-decode-execute cycle, pipelining, and branch prediction.
- Explain the GIL, virtual memory, and the Linux process scheduler.
- Analyze algorithm complexity and select appropriate data structures for DE workloads.

### Prerequisites
- Basic programming literacy in any language (loops, functions, conditionals).
- No CS background required.

### Career Relevance
A Staff Engineer or Principal Engineer cannot do their job without this knowledge. Questions about "why is this Spark job slow?" ultimately trace back to CPU pipelining, cache misses, or memory bandwidth — all from this school.

Senior engineers who lack this background are visible in interviews: they know the knobs but not the reasons.

### Estimated Duration
- Computer Architecture: 40–50 hours
- Operating Systems: 40–50 hours
- Algorithms & Complexity: 30–40 hours
- **Total: 110–140 hours**

### Difficulty
★★★☆☆ — Conceptually dense. Requires mental model building. No prior CS required, but demands patience.

### Expected Outcomes
Graduates of this school can:
- Read a CPU flame graph and identify hot paths.
- Explain the hardware reason why a Pandas UDF is faster than a Python UDF.
- Profile Python code with cProfile and identify memory leaks with tracemalloc.
- Select the right data structure for a given access pattern.
- Explain the GIL and its impact on Python concurrency.

---

## School 2 — Systems & Infrastructure (SYS)

**Tagline:** *The runtime you live in. Know it cold.*

### Purpose
Every data pipeline runs on Linux. Every distributed system is a network of Linux machines talking over TCP/IP. Every Kafka broker, Spark executor, and Airflow worker is a Linux process managed by the OS kernel. This school makes Linux and networking transparent rather than a mystery.

### Departments
- 2.1 Linux & Unix Systems (SYS-LNX)
- 2.2 Computer Networking (SYS-NET)
- 2.3 Distributed Systems Theory (SYS-DST)

### Learning Objectives
- Diagnose a production Linux system using `perf`, `strace`, `iostat`, and `vmstat`.
- Explain how the kernel page cache affects Kafka and database I/O.
- Write robust shell scripts with proper error handling for pipeline automation.
- Explain TCP's three-way handshake, congestion control, and TIME_WAIT.
- Classify distributed systems by CAP/PACELC trade-offs.
- Explain Raft leader election and log replication from memory.
- Understand why distributed transactions are hard and when Sagas are appropriate.

### Prerequisites
- CSF — Computer Science Foundations (full)

### Career Relevance
- **Production engineering:** On-call incidents involve diagnosing Linux processes, network timeouts, and distributed system split-brains. This school is survival gear for oncall.
- **Interviews:** Staff-level system design interviews require CAP theorem, consensus protocols, and distributed transaction knowledge.
- **Architecture:** Any senior data engineer designing multi-node systems must understand replication, partitioning, and failure modes.

### Estimated Duration
- Linux & Unix Systems: 50–60 hours
- Computer Networking: 40–50 hours
- Distributed Systems Theory: 60–70 hours
- **Total: 150–180 hours**

### Difficulty
★★★★☆ — Highly practical but conceptually demanding, especially distributed systems theory.

### Expected Outcomes
Graduates can:
- Use `strace` to diagnose a hanging process.
- Configure Linux `ulimits` for a Kafka broker.
- Explain the MESI cache coherence protocol and false sharing.
- Whiteboard Raft leader election in an interview.
- Classify any distributed system by its consistency model.

---

## School 3 — Database Engineering (DBE)

**Tagline:** *All data systems are databases in disguise.*

### Purpose
Spark is a distributed database engine. Kafka is a distributed log. Iceberg is a transactional storage layer. BigQuery is a columnar database. Airflow's metadata store is a relational database. Understanding database internals — storage engines, indexing, query processing, and transactions — gives the analytical framework to understand all of them.

### Departments
- 3.1 Database Internals (DBE-INT)
- 3.2 SQL Engineering (DBE-SQL)
- 3.3 Scalable Data Stores (DBE-SDS)

### Learning Objectives
- Explain the B-tree and LSM-tree trade-offs.
- Read and interpret an EXPLAIN/EXPLAIN ANALYZE query plan.
- Write complex SQL including window functions, recursive CTEs, and advanced aggregations.
- Explain MVCC, snapshot isolation, and how they enable non-blocking reads.
- Design a sharding strategy for a 10-billion-row table.
- Explain WAL-based replication and its implications for replication lag.

### Prerequisites
- SYS-LNX (file system and I/O)
- SYS-DST (distributed systems theory for replication)
- CSF-ARC-102 (memory hierarchy for storage engine internals)

### Career Relevance
- SQL is the most demanded data engineering skill in job listings.
- Storage engine knowledge is directly applicable to choosing between Postgres, Cassandra, BigQuery, and Iceberg.
- MVCC and isolation level knowledge is required for reasoning about Delta Lake and Iceberg ACID semantics.

### Estimated Duration
- Database Internals: 60–70 hours
- SQL Engineering: 50–60 hours
- Scalable Data Stores: 40–50 hours
- **Total: 150–180 hours**

### Difficulty
★★★★☆ — SQL is approachable; storage engine internals are deeply technical.

### Expected Outcomes
Graduates can:
- Write and optimize complex analytical SQL queries under interview pressure.
- Explain the difference between B-tree and LSM-tree and give production examples of each.
- Configure Postgres replication and monitor replica lag.
- Design a sharding strategy with justification for the key choice.
- Explain what MVCC is and how it enables snapshot isolation.

---

## School 4 — Programming Engineering (PRG)

**Tagline:** *Python deep enough to understand what it's actually doing.*

### Purpose
Python is the language of data engineering. But most data engineers use Python without understanding CPython internals, the GIL, memory management, or async I/O. This creates a ceiling: engineers can write pipelines but cannot diagnose slow UDFs, memory leaks, or concurrency bugs. This school removes that ceiling.

### Departments
- 4.1 Python Engineering (PRG-PY)
- 4.2 Software Engineering Practices (PRG-SE)

### Learning Objectives
- Explain CPython's reference counting, garbage collector, and small integer cache.
- Use `dis.dis()` to inspect bytecode and explain why certain patterns are slow.
- Diagnose a Python memory leak using `tracemalloc` and `objgraph`.
- Write thread-safe code that avoids GIL-related performance problems.
- Use asyncio correctly for I/O-bound data engineering tasks.
- Apply testing, type hints, and code quality tools to data pipeline code.

### Prerequisites
- CSF (complete — Python internals require CPU/memory/OS understanding)
- Basic Python programming (functions, classes, file I/O)

### Career Relevance
- Python UDF performance is a common Spark interview topic.
- Memory leaks in long-running pipeline processes are a common production incident.
- Code quality practices (testing, type hints, reviews) are required for staff-level roles.

### Estimated Duration
- Python Engineering: 60–70 hours
- Software Engineering Practices: 30–40 hours
- **Total: 90–110 hours**

### Difficulty
★★★☆☆ — Python is familiar; the internals layer is new and requires attention.

### Expected Outcomes
Graduates can:
- Write a Pandas UDF and explain why it is faster than a Python UDF.
- Fix a Python memory leak in a provided long-running script.
- Write async pipeline code for concurrent API calls.
- Apply pytest, mypy, and ruff to a data pipeline project.
- Diagnose a GIL-related performance problem with `threading` vs `multiprocessing`.

---

## School 5 — Data Engineering (DE)

**Tagline:** *The craft. Build pipelines that don't page you at 3 AM.*

### Purpose
The core profession. This school teaches data engineering patterns, lake architecture, orchestration, transformation, and data modeling. It is where theory becomes craft. A graduate of this school can design, build, test, and operate a production-grade data pipeline independently.

### Departments
- 5.1 Pipeline Design & Architecture (DE-PDA)
- 5.2 Data Modeling (DE-MOD)
- 5.3 Orchestration (DE-ORC)
- 5.4 Transformation Engineering (DE-TRF)

### Learning Objectives
- Design a batch pipeline with idempotency, retry logic, and data quality checks.
- Build a Bronze/Silver/Gold data lake and explain the contract between each zone.
- Design a star schema and implement SCD Type 2 using dbt.
- Write a dbt project with models, tests, and documentation.
- Build and operate an Airflow DAG with correct dependency management and alerting.
- Write and enforce a data contract.

### Prerequisites
- PRG (complete — Python for pipelines)
- DBE-SQL (SQL for dbt)
- SYS-LNX (shell scripting for Airflow and orchestration)

### Career Relevance
This is the core of the job description at 80% of data engineering roles. Every module in this school directly maps to a common interview question or production scenario.

### Estimated Duration
- Pipeline Design: 40–50 hours
- Data Modeling: 50–60 hours
- Orchestration: 40–50 hours
- Transformation Engineering: 40–50 hours
- **Total: 170–210 hours**

### Difficulty
★★★★☆ — Practical and immediately applicable, but depth requires prior schools.

### Expected Outcomes
Graduates can independently build and operate an end-to-end data pipeline from raw ingestion to serving layer, using dbt + Airflow + SQL + Python, with monitoring, data quality, and CI/CD.

---

## School 6 — Distributed Compute & Streaming (DCS)

**Tagline:** *When one machine isn't enough.*

### Purpose
At scale, data processing must be distributed. This school teaches Apache Spark (the dominant batch compute engine), Apache Kafka (the dominant message bus), stream processing theory, and PySpark. A graduate understands these systems deeply enough to configure them, tune them, and debug them in production.

### Departments
- 6.1 Apache Spark (DCS-SPK)
- 6.2 PySpark & Delta Lake (DCS-PYS)
- 6.3 Stream Processing Theory (DCS-STR)
- 6.4 Apache Kafka (DCS-KFK)

### Learning Objectives
- Explain the Spark execution model: driver → DAG → stages → tasks → shuffle.
- Diagnose a Spark data skew problem using the Spark UI.
- Write and benchmark Pandas UDFs vs Python UDFs.
- Configure a Kafka producer for exactly-once delivery.
- Design a Kafka topic partitioning strategy for a given throughput.
- Explain watermarks and late data in Structured Streaming.
- Compare Flink vs Spark Streaming for true streaming latency requirements.

### Prerequisites
- SYS-DST (complete — Spark and Kafka are distributed systems)
- PRG (complete — Python/JVM internals for UDFs)
- DBE-INT (storage engines explain Delta Lake and Kafka storage)

### Career Relevance
Spark and Kafka are the two most in-demand distributed data technologies. Together, they appear in ~70% of senior data engineering job descriptions. The ability to tune them in production (not just use their APIs) is what separates mid-level from senior.

### Estimated Duration
- Apache Spark: 70–80 hours
- PySpark & Delta Lake: 50–60 hours
- Stream Processing Theory: 50–60 hours
- Apache Kafka: 60–70 hours
- **Total: 230–270 hours**

### Difficulty
★★★★★ — The most technically demanding school. Requires mastery of all prior schools.

### Expected Outcomes
Graduates can tune a Spark job for performance, debug a Kafka consumer lag issue, build a Structured Streaming pipeline with watermarks, and explain the trade-offs between Flink and Spark for a given latency requirement.

---

## School 7 — Cloud, Production & Leadership (CPL)

**Tagline:** *Build for production. Lead from strength.*

### Purpose
The final school combines cloud data platforms (BigQuery, open table formats, multi-cloud), production engineering discipline (observability, CI/CD, incident response), system design at scale, and the leadership skills required for staff and principal roles.

### Departments
- 7.1 Cloud Data Platforms (CPL-CLD)
- 7.2 Production Engineering (CPL-PRD)
- 7.3 System Design (CPL-SYD)
- 7.4 Technical Leadership (CPL-LDR)

### Learning Objectives
- Design a complete cloud data platform on GCP, AWS, or Azure.
- Optimize a BigQuery query by 10× using clustering, partitioning, and materialized views.
- Explain Apache Iceberg's file layout and how snapshot isolation works.
- Lead a 45-minute system design session for a data warehouse or streaming platform.
- Write a post-mortem for a data quality incident.
- Explain ADRs, data mesh, and event-driven architecture trade-offs.
- Communicate technical trade-offs to non-technical stakeholders.

### Prerequisites
- All prior schools — this is the capstone school.

### Career Relevance
- Cloud: 90% of modern data engineering runs on cloud platforms.
- Production: Staff engineers are expected to own reliability.
- System Design: The primary evaluation signal at senior/staff interviews.
- Leadership: The differentiator between a senior engineer and a staff engineer.

### Estimated Duration
- Cloud Data Platforms: 80–100 hours
- Production Engineering: 50–60 hours
- System Design: 60–70 hours
- Technical Leadership: 40–50 hours
- **Total: 230–280 hours**

### Difficulty
★★★★★ — Requires synthesis of all prior learning. The integration layer of the entire university.

### Expected Outcomes
Graduates can design and own a complete cloud data platform, lead architectural decisions, communicate trade-offs to stakeholders, and perform at a Staff Data Engineer level in interviews and on the job.

---

## School Summary

| School | Code | Departments | Courses | Difficulty | Hours |
|---|---|---|---|---|---|
| CS Foundations | CSF | 3 | 5 | ★★★☆☆ | 110–140 |
| Systems & Infrastructure | SYS | 3 | 6 | ★★★★☆ | 150–180 |
| Database Engineering | DBE | 3 | 6 | ★★★★☆ | 150–180 |
| Programming Engineering | PRG | 2 | 4 | ★★★☆☆ | 90–110 |
| Data Engineering | DE | 4 | 5 | ★★★★☆ | 170–210 |
| Distributed Compute & Streaming | DCS | 4 | 7 | ★★★★★ | 230–270 |
| Cloud, Production & Leadership | CPL | 4 | 7 | ★★★★★ | 230–280 |
| **Total** | | **23** | **40** | | **1,130–1,370 hrs** |

**Full-time equivalent:** ~1,250 hours ÷ 40 hrs/week = ~31 weeks (~8 months full-time)  
**Working professional:** ~1,250 hours ÷ 10 hrs/week = ~125 weeks (~2.5 years part-time)  
**Intensive part-time:** ~1,250 hours ÷ 20 hrs/week = ~63 weeks (~15 months)
