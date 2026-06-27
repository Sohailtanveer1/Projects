# Learning Paths

Nine paths through the university. Each path is opinionated about what to study and in what order, based on a specific career goal. Paths diverge where the career goal requires specialisation and converge on the foundational layer every data engineer needs.

---

## Common Foundation (All Paths)

Every path begins here, regardless of experience or goal:

```
CSF-ARC-101 → CSF-ARC-102 → CSF-OS-101 → CSF-OS-102 → SYS-LNX-101
```

This is non-negotiable. A data engineer who does not understand the CPU, memory hierarchy, OS, and Linux cannot reason about performance, diagnose production failures, or progress to staff level.

---

## Path 1 — Interview Track

**For:** Job seekers targeting data engineering roles at tech companies within 3–4 months.  
**Assumes:** Some programming experience. No prior data engineering.  
**Focus:** SQL, Python, Spark basics, and the system design vocabulary needed to pass screening.

```
Semester 0 (partial):
  CSF-ARC-101  How Computers Execute Programs
  CSF-ARC-102  Memory Architecture
  CSF-OS-101   OS Internals

Semester 1 (partial):
  PRG-PY-101   Python Internals
  CSF-ALG-101  Algorithms & Data Structures

Semester 3:
  DBE-SQL-101  SQL Internals and Optimization
  DBE-SQL-102  Advanced SQL for Data Engineers
  PRG-PY-103   Python for Data Engineering

Semester 4 (partial):
  DE-MOD-101   Dimensional Modeling
  DCS-SPK-101  Spark Architecture
  DCS-KFK-101  Kafka Architecture

Semester 6:
  CPL-LDR-101  Interview Preparation (full course)
```

**Approximate hours:** 320–380  
**Typical duration:** 10–16 weeks full-time, 6–9 months part-time

**Diverges from other paths at:** Semester 4 (goes directly to interview prep instead of completing full stack)  
**Converges with Production Track at:** Semester 3 (SQL and Python are shared)

**What this path intentionally skips:** Linux deep-dive, distributed systems theory, Kafka ecosystem, observability, CI/CD, advanced architecture. These are for after getting the job.

---

## Path 2 — Production Engineer Track

**For:** Engineers who have landed a data engineering role and want to operate pipelines at production scale without being paged at 3 AM.  
**Assumes:** Comfortable with Python and SQL. Some DE experience.  
**Focus:** Breadth of tooling + operational depth.

```
Phase 1 — Foundations (Semesters 0–1):
  All of CSF
  All of SYS-LNX
  PRG-PY-101, PRG-PY-102, PRG-SE-101

Phase 2 — Infrastructure (Semester 2):
  SYS-NET-101, SYS-NET-102
  SYS-DST-101, SYS-DST-102
  PRG-PY-102

Phase 3 — Data Stack (Semesters 3–4):
  All of DBE
  All of PRG-PY-103
  All of DE
  DCS-SPK-101, DCS-SPK-102
  DCS-KFK-101, DCS-KFK-102

Phase 4 — Production Hardening (Semester 5):
  DCS-PYS-101
  DCS-STR-101, DCS-STR-102
  CPL-PRD-101 (Observability)
  CPL-PRD-102 (CI/CD)
  CPL-CLD-102 (Table Formats)

Phase 5 — Cloud (Semester 5–6):
  CPL-CLD-101 (BigQuery)
  CPL-CLD-103 (Multi-Cloud)
```

**Approximate hours:** 900–1,100  
**Typical duration:** 18–24 months part-time

**Focus modules:** SYS-LNX-102 (performance tools), CPL-PRD-101 (observability), CPL-PRD-102 (CI/CD) — these are the most production-relevant.

---

## Path 3 — Freelance Consultant Track

**For:** Engineers who want to deliver end-to-end data solutions for clients on cloud platforms.  
**Assumes:** Some analytics or DE background. Client-facing experience helpful.  
**Focus:** Fast deliverables, modern cloud stack, data modeling, documentation.

```
Sprint 1 — Client Communication Layer:
  DE-MOD-101  Dimensional Modeling  ← what clients ask for
  DE-MOD-102  Modern Data Modeling  ← data contracts, schema evolution
  DE-TRF-101  dbt                   ← industry standard transformation tool

Sprint 2 — Cloud Stack:
  CPL-CLD-101  BigQuery             ← dominant analytics warehouse
  DE-ORC-101   Airflow              ← standard orchestrator
  PRG-PY-103   Python for DE        ← ingestion scripts

Sprint 3 — Modern Table Formats:
  CPL-CLD-102  Open Table Formats   ← lakehouse is now the default

Sprint 4 — Production Quality:
  CPL-PRD-102  CI/CD for Data       ← clients need tested, deployed pipelines
  PRG-SE-101   Engineering Practices ← code quality clients can maintain

Sprint 5 — Expanding Scope:
  DCS-KFK-101  Kafka Architecture
  DCS-STR-101  Stream Processing Theory
  CPL-SYD-101  Data System Design   ← architecture conversations with clients
```

**Approximate hours:** 450–550  
**What to build as portfolio:** A public GitHub repo containing a dbt project on BigQuery with a real dataset, a Mermaid architecture diagram, and a README explaining trade-offs.

---

## Path 4 — Cloud Specialist Track

**For:** Engineers who want deep expertise in cloud data platforms (GCP, AWS, or Azure).  
**Assumes:** Some data engineering or analytics background.  
**Focus:** Cloud-native architectures, cost optimization, managed services.

```
Foundation (Semesters 0–1, partial):
  CSF-ARC-101, CSF-ARC-102  ← understand cloud VM performance
  SYS-NET-102               ← VPC, private endpoints, security groups
  SYS-DST-101               ← CAP theorem for cloud service selection

Core Data Stack (Semesters 3–4, partial):
  DBE-SQL-102   Advanced SQL
  DE-PDA-102    Data Lake Architecture ← S3/GCS/ADLS patterns
  DE-TRF-101    dbt
  DCS-SPK-101   Spark Architecture ← EMR, Dataproc, Azure Databricks
  DCS-KFK-101   Kafka ← MSK, Confluent Cloud, Event Hubs

Cloud Specialization (Semester 5–6):
  CPL-CLD-101   BigQuery (full)
  CPL-CLD-102   Open Table Formats (full)
  CPL-CLD-103   Multi-Cloud (full)
  CPL-PRD-101   Observability ← Cloud Monitoring, CloudWatch
  CPL-PRD-102   CI/CD ← Terraform, Cloud Build, GitHub Actions
  CPL-SYD-101   Data System Design ← cloud architecture interviews
```

**Approximate hours:** 500–600  
**Certification alignment:** GCP Professional Data Engineer, AWS Data Analytics Specialty, Azure Data Engineer Associate.

---

## Path 5 — Streaming Specialist Track

**For:** Engineers targeting streaming-first architectures: real-time analytics, event-driven systems, or IoT data platforms.  
**Assumes:** Comfortable with Python and some DE experience.  
**Focus:** Deep streaming theory + Kafka + Flink/Spark Streaming.

```
Foundation (mandatory):
  CSF-ARC-101, CSF-ARC-102
  SYS-NET-101
  SYS-DST-101, SYS-DST-102  ← consensus is essential for Kafka internals

Kafka Deep Dive:
  DCS-KFK-101  Kafka Architecture (full, deep)
  DCS-KFK-102  Kafka Ecosystem (full, deep)

Streaming Theory:
  DCS-STR-101  Stream Processing Theory (full, deep)
  DCS-STR-102  Streaming Engines in Production

PySpark Streaming:
  DCS-SPK-101  Spark Architecture
  DCS-PYS-101  PySpark Engineering (focus: Structured Streaming module)

Schema Management:
  CPL-CLD-102  Open Table Formats (focus: Iceberg for streaming sink)
  DE-MOD-102   Modern Data Modeling (focus: schema evolution)

Production:
  CPL-PRD-101  Observability (focus: consumer lag, throughput monitoring)
```

**Approximate hours:** 500–600  
**Key additional reading:** "Kafka: The Definitive Guide" (2nd ed.); "The Dataflow Model" paper (Akidau et al., 2015); Apache Flink documentation on stateful stream processing.

---

## Path 6 — Analytics Engineer Track

**For:** Analysts transitioning to analytics engineering; dbt practitioners deepening their DE foundation.  
**Assumes:** Strong SQL. Some Python. No infrastructure background required.  
**Focus:** SQL mastery, dbt, data modeling, and the data engineering patterns that feed analytics.

```
Foundation (minimal but required):
  CSF-ARC-101   How Computers Execute Programs ← explains why SQL is slow/fast
  CSF-ARC-102   Memory Architecture ← explains columnar storage
  DBE-INT-101   Storage Engines ← explains BigQuery and Snowflake architecture

SQL Mastery:
  DBE-SQL-101   SQL Internals and Optimization (full)
  DBE-SQL-102   Advanced SQL for Data Engineers (full)

Data Modeling:
  DE-MOD-101    Dimensional Modeling (full)
  DE-MOD-102    Modern Data Modeling (full)

dbt:
  DE-TRF-101    dbt (full, deep — this is the core tool)

Orchestration (minimal):
  DE-ORC-101    Apache Airflow (Modules 1–3 only)

Cloud Serving:
  CPL-CLD-101   BigQuery (Modules 1–3: architecture, storage, optimization)

Data Contracts and Quality:
  CPL-PRD-101   Data Systems Observability (focus: data quality monitoring)
  PRG-SE-101    Engineering Practices (focus: testing and documentation)
```

**Approximate hours:** 350–420  
**This path explicitly skips:** Spark internals, Kafka, Linux performance tools, distributed systems theory. These are out of scope for the analytics engineer role. Return to Path 2 or Path 7 after a year of experience.

---

## Path 7 — Senior Engineer Track

**For:** Mid-level data engineers (2–4 years experience) targeting senior-level roles.  
**Assumes:** Comfortable with Spark, SQL, Python, and Airflow. Has shipped pipelines to production.  
**Focus:** Depth (not breadth) — understanding the systems you already use at the architectural level.

```
Depth Gaps to Close (most seniors miss these):
  CSF-ARC-101, CSF-ARC-102   ← most never studied hardware formally
  SYS-DST-101, SYS-DST-102   ← CAP + Raft are senior interview table stakes
  DBE-INT-101, DBE-INT-102   ← storage engine knowledge separates senior from mid

SQL and Python Depth:
  DBE-SQL-101   SQL Internals (EXPLAIN plans, join algorithms)
  PRG-PY-101    Python Internals (GIL, bytecode, memory — most seniors lack this)

Spark Performance (the hard part):
  DCS-SPK-102   Spark Performance Engineering (full, deep)
  DCS-PYS-101   PySpark Engineering (focus: UDF performance, Structured Streaming)

Data Modeling Depth:
  DE-MOD-101, DE-MOD-102  (if not already known)
  DE-TRF-101    dbt (if not already deeply known)

Production:
  CPL-PRD-101   Observability (full)
  CPL-PRD-102   CI/CD (full)

Architecture Introduction:
  CPL-SYD-101   Data System Design (Modules 1–3)

Interview Preparation:
  CPL-LDR-101   Interview Preparation (all modules)
```

**Approximate hours:** 400–500  
**Most valuable modules for this path:** DCS-SPK-102 (Spark Performance), SYS-DST-101 (CAP theorem), DBE-INT-101 (Storage Engines), CPL-PRD-101 (Observability). These are the gaps that most mid-level-to-senior transitions require.

---

## Path 8 — Staff Engineer Track

**For:** Senior engineers (4+ years) targeting staff-level roles. The "I need to know *why*" path.  
**Assumes:** Strong production experience. Has owned a significant data system.  
**Focus:** Theory depth + system design + production mastery + architecture vocabulary.

```
Phase 1 — Theory Audit (Month 1):
  All of CSF (if not already done — most seniors haven't)
  All of SYS-DST (CAP, Raft, 2PC, observability)
  DBE-INT-101 through DBE-INT-103 (storage engines → transactions)
  Read the founding papers: Dynamo (2007), Raft (2014), Dremel (2010), Dataflow Model (2015)

Phase 2 — System Depth (Months 2–4):
  DCS-SPK-101, DCS-SPK-102 (full depth + founding paper: "Resilient Distributed Datasets")
  DCS-KFK-101, DCS-KFK-102 (full depth + founding paper: "Kafka: a Distributed Messaging System")
  DCS-STR-101, DCS-STR-102 (full depth + "The Dataflow Model" paper)
  CPL-CLD-102 (Iceberg spec at iceberg.apache.org, Delta paper)

Phase 3 — Architecture (Months 4–6):
  CPL-SYD-101 (full)
  CPL-SYD-102 (full: data mesh, event sourcing, CQRS)
  CPL-PRD-101, CPL-PRD-102 (full ownership of production reliability)

Phase 4 — Leadership (Month 6):
  CPL-LDR-101 (interview preparation — you are now the interviewer)
  CPL-LDR-102 (ADRs, RFC process, mentorship, red team reviews)
```

**Approximate hours:** 600–800  
**Distinguishing behavior:** At staff level, you read the *papers*, not just the documentation. Every major module in this path has a foundational paper. Reading those papers is the path.

---

## Path 9 — Principal Engineer Track

**For:** Staff engineers targeting principal or distinguished engineer roles, or those seeking technical leadership at organization scope.  
**Assumes:** Completed Path 8 or equivalent. Has made architecture decisions affecting multiple teams.  
**Focus:** Systems thinking at organizational scale, research practice, and leadership.

```
This path is not a list of courses — it is a set of practices layered on top of Path 8.

Practice 1 — Research:
  For every major technology in your stack, read the founding paper.
  Go beyond the paper to the "why did this fail?" papers (Chord, CRDT failures, Paxos pitfalls).
  Write a 1-page summary of each paper as if briefing your engineering manager.

Practice 2 — Teaching:
  For every module in the university, write a one-page technical briefing
  suitable for onboarding a new junior engineer to your team.
  If you cannot write it clearly, you do not understand it deeply enough.

Practice 3 — Architecture Decision Records:
  For every significant technical decision in your work, write an ADR.
  Include: context, 3 options considered, decision, and consequences (positive AND negative).
  Publish every ADR to your team's documentation.

Practice 4 — Red Team Reviews:
  Before any architectural decision goes to implementation, assign a red team (yourself if needed).
  Write the strongest case against your proposed design.
  If you cannot answer the red team, revise the design.

Practice 5 — Cross-Functional Communication:
  Practice explaining every concept at three levels: business (no jargon), engineering manager
  (trade-offs + risks), staff engineer (full technical depth).
  Request feedback on which level was most effective.

Course Additions (if not in Path 8):
  CPL-SYD-102   Advanced Architecture Patterns (Data Mesh, CQRS, Event Sourcing)
  CPL-LDR-102   Staff and Principal Engineering (full)
```

**This path has no "hours" estimate.** Principal engineering is a continuous practice, not a curriculum milestone. The measure is: can you make a technical decision that affects 10+ engineers, communicate it clearly to non-technical stakeholders, and defend it against strong objections from senior colleagues?

---

## Where Paths Overlap

```
                    Interview  Production  Freelance  Cloud   Streaming  Analytics  Senior  Staff   Principal
CSF (full)              ●●○       ●●●●●     ○○○○○    ●●○○○    ●●●○○     ●○○○○      ●●●●○   ●●●●●    ●●●●●
SYS-LNX                 ●○○       ●●●●●     ○○○○○    ●○○○○    ●●○○○     ○○○○○      ●●●○○   ●●●●●    ●●●●●
SYS-NET                 ○○○       ●●●●○     ○○○○○    ●●●●○    ●●●○○     ○○○○○      ●●○○○   ●●●●●    ●●●●●
SYS-DST                 ○○○       ●●●●●     ○○○○○    ●●○○○    ●●●●●     ○○○○○      ●●●●○   ●●●●●    ●●●●●
DBE-INT                 ●○○       ●●●●○     ●○○○○    ●●○○○    ●●○○○     ●●●○○      ●●●●○   ●●●●●    ●●●●●
DBE-SQL                 ●●●●●     ●●●●●     ●●●●○    ●●●○○    ●●○○○     ●●●●●      ●●●●●   ●●●●●    ●●●●●
PRG-PY                  ●●●○○     ●●●●●     ●●●○○    ●●●○○    ●●●●○     ●●○○○      ●●●●○   ●●●●●    ●●●●●
DE (all)                ●●●○○     ●●●●●     ●●●●●    ●●●●○    ●●●○○     ●●●●●      ●●●●●   ●●●●●    ●●●●●
DCS-SPK                 ●●○○○     ●●●●●     ○○○○○    ●●●○○    ●●○○○     ○○○○○      ●●●●●   ●●●●●    ●●●●●
DCS-KFK                 ●●○○○     ●●●●●     ○○○○○    ●●○○○    ●●●●●     ○○○○○      ●●●○○   ●●●●●    ●●●●●
DCS-STR                 ○○○○○     ●●●●○     ○○○○○    ●●○○○    ●●●●●     ○○○○○      ●●●○○   ●●●●●    ●●●●●
CPL-CLD                 ○○○○○     ●●●●●     ●●●●○    ●●●●●    ●●●○○     ●●●●○      ●●●○○   ●●●●●    ●●●●●
CPL-PRD                 ○○○○○     ●●●●●     ●●○○○    ●●●○○    ●●●●○     ●○○○○      ●●●●○   ●●●●●    ●●●●●
CPL-SYD                 ●●○○○     ●●●●○     ●●○○○    ●●●○○    ●●●○○     ○○○○○      ●●●○○   ●●●●●    ●●●●●
CPL-LDR                 ●●●●●     ○○○○○     ○○○○○    ●○○○○    ○○○○○     ○○○○○      ●●●●●   ●●●●●    ●●●●●

● = covered   ○ = skipped or optional   (density of dots = depth of coverage)
```
