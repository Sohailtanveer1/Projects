# Data Engineering University — Learning Paths

Six paths through the curriculum, from complete beginner to principal engineer. Each path is sequenced so that every module's prerequisites are met before you need them.

---

## Path 1 — Interview Fast-Track

**Goal:** Land a data engineering role at a mid-to-large company within 3–4 months.  
**Assumes:** Some coding experience (Python or SQL). No prior DE experience required.  
**Time:** ~120 hours total · ~8–10 hours/week.

```
Week 1–2   │ 00 CS Foundations M01  How Computers Execute Programs
           │ 00 CS Foundations M02  Memory and Storage Hierarchy
Week 3–4   │ 06 SQL M01             SQL Execution Internals
           │ 06 SQL M02             Window Functions
           │ 06 SQL M03             Query Optimization
Week 5–6   │ 07 Python M01          Python Internals
           │ 07 Python M04          Data Structures and Algorithms
Week 7–8   │ 04 Databases M01       Storage Engines
           │ 04 Databases M02       Indexing
           │ 04 Databases M04       ACID and Isolation Levels
Week 9–10  │ 05 Data Engineering M01 Batch vs Streaming
           │ 05 Data Engineering M03 ELT vs ETL
           │ 05 Data Engineering M04 Medallion Architecture
Week 11–12 │ 08 Spark M01           Spark Architecture
           │ 08 Spark M03           Spark Execution Engine
           │ 08 Spark M04           Shuffle and Partitioning
Week 13–14 │ 11 Kafka M01           Kafka Architecture
           │ 11 Kafka M02           Producers and Consumers
Week 15–16 │ 19 Interview Prep M01  Coding Interview
           │ 19 Interview Prep M02  System Design Interview
           │ 19 Interview Prep M03  Behavioral Interview
```

**Milestone check:** After Week 8, you should be able to explain an EXPLAIN plan and design a star schema. After Week 12, you should be able to whiteboard a Spark shuffle and a Kafka consumer group.

---

## Path 2 — Production Engineer

**Goal:** Build and operate production-grade data pipelines that don't page you at 3 AM.  
**Assumes:** Comfortable with Python and SQL. Some DE experience.  
**Time:** ~200 hours total · ongoing.

```
Phase 1 — Foundations (Months 1–2)
  00 CS Foundations   All modules
  01 Linux            All modules
  02 Networking       M01–M02

Phase 2 — Core Stack (Months 2–4)
  04 Databases        All modules
  06 SQL              All modules
  07 Python           All modules
  08 Apache Spark     All modules
  09 PySpark          All modules

Phase 3 — Platform (Months 4–6)
  11 Kafka            All modules
  10 Streaming        All modules
  13 Airflow          All modules
  14 dbt              All modules
  15 Iceberg/Delta    All modules

Phase 4 — Production Hardening (Months 6–8)
  05 Data Engineering All modules
  16 Data Modeling    All modules
  18 Production Eng   All modules
  12 BigQuery         All modules
```

**Key mindset for this path:** Every module has a "Failure Scenarios" and "Debugging" section. Study these as carefully as the happy path.

---

## Path 3 — Freelance / Consultant

**Goal:** Deliver end-to-end data solutions for clients on modern cloud stacks.  
**Assumes:** Some DE or analytics experience. Comfortable selling your work.  
**Time:** ~150 hours total.

```
Sprint 1 — Client Communication (2 weeks)
  16 Data Modeling M01   Dimensional Modeling (what clients ask for)
  05 Data Engineering M05 Data Contracts (what clients need to agree to)
  14 dbt M01–M03         dbt Architecture + Models + Testing
    → Deliverable: You can set up dbt from scratch for a client

Sprint 2 — Cloud Stack (4 weeks)
  12 BigQuery All modules
  13 Airflow M01–M04
  09 PySpark M01–M02
    → Deliverable: Full ELT pipeline on GCP

Sprint 3 — Modern Table Formats (2 weeks)
  15 Iceberg/Delta/Hudi M01–M03
    → Deliverable: Lakehouse proof-of-concept

Sprint 4 — Productionization (2 weeks)
  18 Production Eng M02   CI/CD for Data
  18 Production Eng M03   Data Quality Frameworks
    → Deliverable: Tested, deployed, documented pipeline

Sprint 5 — Expanding Scope (ongoing)
  11 Kafka M01–M02
  10 Streaming M01–M02
  17 System Design M01
```

**Client-facing framing:** Lead with outcomes (latency, cost, correctness), not tools. Use System Design modules to structure your architecture conversations.

---

## Path 4 — Staff Engineer

**Goal:** Technical leadership, architectural decision-making, and cross-team influence.  
**Assumes:** 3+ years of data engineering experience. Comfortable reading research papers.  
**Time:** 250+ hours · designed to go deep, not fast.

```
Foundation Audit (Month 1)
  Do you actually know these?
  00 CS Foundations   All modules (especially M03 Concurrency, M05 OS Internals)
  03 Distributed Systems All modules — this is the theory behind everything
  04 Databases        All modules (especially M01 Storage Engines)

Core Mastery (Months 2–5)
  08 Spark            All modules — read the Spark papers (Resilient Distributed Datasets, 2012)
  10 Streaming        All modules — read the Dataflow Model paper (2015)
  11 Kafka            All modules — read the Kafka paper (2011)
  15 Table Formats    All modules — read Iceberg spec on iceberg.apache.org

Systems Thinking (Months 5–7)
  17 System Design    All modules
  16 Data Modeling    All modules (especially M04 Schema Evolution)
  18 Production Eng   All modules (especially M04 Incident Response)
  05 Data Engineering All modules

Leadership Skills (Month 7–8)
  19 Interview Prep   M02 System Design (now you're the interviewer, not the candidate)
  Write your own ADRs (Architecture Decision Records) for each major module
  Teach back: write a module summary as if onboarding a junior engineer
```

**Staff-level mindset shifts:**
- Read the *why*, not just the *how*. Every major technology has a founding paper.  
- Learn the failure modes before the happy path.  
- Ask "what does this NOT do well?" for every tool you evaluate.  
- System Design modules: aim to spot the missing constraints in the problem statement.

---

## Prerequisites Map (Quick Reference)

```
No prerequisites:
  00-M01  How Computers Execute Programs   ← Start here
  06-M01  SQL Execution Internals          ← Can start anytime

Requires 00-M01:
  00-M02  Memory and Storage Hierarchy
  00-M03  Concurrency and Parallelism
  00-M05  OS Internals

Requires 00-M01 + 00-M02:
  04-M01  Storage Engines
  01-M02  File System and IO

Requires 00-M01 + 00-M03:
  07-M01  Python Internals
  07-M02  Concurrency asyncio and threading

Requires 04-M01 + 04-M02:
  04-M03  Query Execution
  04-M04  ACID and Isolation Levels

Requires 07 Python (all):
  08-M01  Spark Architecture
  09-M01  PySpark API

Requires 08 Spark (all):
  09-M03  Structured Streaming
  10-M03  Exactly-Once Semantics

Requires 10 + 11 Kafka + Streaming (all):
  17-M02  Design a Streaming Platform

Requires everything:
  19      Interview Preparation
```

---

## Path 5 — Absolute Beginner

**Goal:** Build a complete foundation from zero. No assumed knowledge of programming, math, or computers beyond everyday use.  
**Assumes:** Can use a computer. No programming, no command line, no CS background required.  
**Time:** ~300 hours total · 5–10 hours/week · 6–12 months.

This path does not skip foundations to get to "the fun stuff" faster. The foundations *are* the fun stuff, because without them every subsequent concept is memorization instead of understanding.

```
Phase 1 — Learn to Think Like a Computer (Months 1–2)
  Before any code: understand what computers are.
  00-M01  How Computers Execute Programs
  00-M02  Memory and Storage Hierarchy
  00-M03  Concurrency and Parallelism (just the first-principles section)

  First code: Python basics (not in this curriculum — use Python.org tutorial first)
  Target: write functions, loops, file I/O, and basic error handling.

Phase 2 — The Unix Environment (Month 2–3)
  01-M01  Linux Architecture
  01-M02  File System and IO
  01-M03  Processes and Signals
  01-M04  Shell Scripting
  Goal: feel at home in a terminal.

Phase 3 — Data and SQL (Months 3–5)
  06-M01  SQL Execution Internals
  06-M02  Window Functions
  06-M03  Query Optimization
  Practice: solve 50 SQL problems on LeetCode or StrataScratch.

Phase 4 — Python for Data (Months 5–7)
  07-M01  Python Internals  (now you understand WHY)
  07-M04  Data Structures and Algorithms
  07-M05  Python for Data Engineering
  Build: a Python script that reads a CSV, cleans it, and writes a summary.

Phase 5 — Data Engineering Concepts (Months 7–9)
  05-M01  Batch vs Streaming
  05-M02  Data Lake Architecture
  05-M03  ELT vs ETL
  05-M04  Medallion Architecture
  Build: a simple batch pipeline (CSV → cleaned Parquet → aggregated CSV).

Phase 6 — Your First Real Tool (Months 9–11)
  08-M01  Spark Architecture
  08-M02  RDD vs DataFrame vs Dataset
  08-M03  Spark Execution Engine
  09-M01  PySpark API
  Build: rewrite your batch pipeline in PySpark.

Phase 7 — Job Ready (Month 11–12)
  13-M01  Airflow Architecture
  13-M02  DAGs and Operators
  14-M01  dbt Architecture
  14-M02  Models and Materializations
  19-M01  Coding Interview
  19-M03  Behavioral Interview
```

**Milestones:**
- Month 2: Can navigate Linux confidently; understand what the CPU is doing when code runs.
- Month 5: Can write SQL window functions and optimize a slow query with EXPLAIN.
- Month 9: Can design and build a batch pipeline from scratch.
- Month 12: Can answer entry-level data engineering interview questions with depth.

**What this path does NOT cover:** Kafka, streaming, distributed systems theory, or staff-level system design. Those require real-world experience to make sense. Finish this path, get a job, then return for Path 2 or Path 4.

---

## Path 6 — Principal Engineer

**Goal:** Technical leadership, architectural vision, and the ability to make and communicate high-stakes engineering decisions across organizational boundaries.  
**Assumes:** Staff Engineer level competence (Path 4 complete, or equivalent experience).  
**Time:** This is not a time-bounded path. It is a continuous practice.

The difference between a Staff Engineer and a Principal Engineer is not more modules. It is the habit of reasoning at system-of-systems scale, across organizational boundaries, with explicit trade-off documentation.

```
Foundation Audit (Month 1 — non-negotiable)
  Revisit these with fresh eyes as a senior engineer:
  03-M01  CAP Theorem         — can you identify the CP/AP choice in every system you own?
  03-M02  Consensus and Raft  — can you explain this to a VP without jargon?
  03-M04  Distributed Transactions — do you know when to use Saga vs 2PC in your org?
  17-M01 through 17-M04     — redesign each from scratch; compare to your previous designs.

Breadth Expansion (Months 2–4)
  Everything in Path 4 not yet covered:
  00-M05  OS Internals         — kernel scheduling, virtual memory
  02-M03  Load Balancers       — L4 vs L7, connection draining
  03-M05  Observability        — distributed tracing, SLOs
  15 Table Formats (all)       — read the Iceberg spec, the Delta Lake paper, the Hudi RFC
  18 Production Engineering    — all modules; own the production reliability story

Research Practice (ongoing)
  For each major system in your stack, read the founding paper:
  - Dremel (2010)              — BigQuery's query engine
  - Dynamo (2007)              — eventual consistency, consistent hashing
  - Spanner (2012)             — globally distributed ACID transactions
  - Raft (2014)                — consensus (re-read; teach it to someone)
  - The Dataflow Model (2015)  — Flink and Beam's theoretical foundation
  - Delta Lake paper (2020)    — ACID on object storage

Leadership Practice (ongoing)
  These are not modules. They are habits.

  Architecture Decision Records (ADRs):
    For every significant technical decision in your work, write an ADR:
    - Context: what is the situation?
    - Options considered: at least 3.
    - Decision: what was chosen and why?
    - Consequences: what does this make easier? What does this make harder?

  Teaching Back:
    For every module in this university, write a one-page summary
    as if onboarding a new junior engineer to your team. If you cannot
    do this, you do not understand the module deeply enough.

  Red Team Reviews:
    Before any architectural decision goes to implementation,
    appoint a "red team" (yourself, if necessary) to argue the opposite
    of your proposed design. Document the strongest counterargument.
    If you cannot answer it, revise the design.

  Cross-functional Communication:
    Practice explaining every technical concept at three levels:
    - Level 1: For a business stakeholder (no jargon, focus on outcomes).
    - Level 2: For an engineering manager (high-level, trade-offs, risks).
    - Level 3: For a staff engineer (full technical depth).

Staff → Principal Transition Criteria
  You are operating at Principal Engineer level when:
  [ ] You routinely make architectural decisions that affect multiple teams.
  [ ] You can write an ADR that anticipates and answers the objections of the
      3 most senior engineers who will disagree with you.
  [ ] You can explain any module in this university, at any depth,
      to any audience, in real time.
  [ ] You have delivered a system that runs in production at a scale that
      proves out the trade-offs you made at design time.
  [ ] You have mentored at least one engineer from mid-level to senior.
  [ ] You have killed a bad idea you originally proposed — publicly, with evidence.
```

**On the relationship between this path and the modules:**

Principal engineering is not about knowing more modules. It is about knowing *why* the modules exist, knowing *when* the principles break down, and knowing *what to do next* when the standard solutions do not apply. Return to any module in the curriculum and read it as a curriculum designer, not as a student. Ask: what is missing? What would a learner misunderstand? What would they need to know that this module does not teach?

That critical reading *is* the principal engineer practice.

---

## Where the Paths Overlap

```
                    Beginner  Interview  Production  Freelance  Staff  Principal
                    (Path 5)  (Path 1)   (Path 2)    (Path 3)   (Path4) (Path 6)
00 CS Foundations      ●●○○○     ●●○○○      ●●●●●       ○○○○○     ●●●●●    ●●●●●
01 Linux               ●●●●○     ○○○○○      ●●●●●       ○○○○○     ●●●●●    ●●●●●
02 Networking          ○○○○○     ○○○○○      ●●○○○       ○○○○○     ●●●●●    ●●●●●
03 Distributed Systems ○○○○○     ○○○○○      ●●●●●       ○○○○○     ●●●●●    ●●●●●
04 Databases           ○○○○○     ●●●○○      ●●●●●       ○○○○○     ●●●●●    ●●●●●
05 Data Engineering    ●●●●●     ●●●○○      ●●●●●       ●●●○○     ●●●●●    ●●●●●
06 SQL                 ●●●○○     ●●●●●      ●●●●●       ●●●○○     ●●●●●    ●●●●●
07 Python              ●●●●●     ●●●●○      ●●●●●       ●●●○○     ●●●●●    ●●●●●
08 Apache Spark        ●●●○○     ●●●○○      ●●●●●       ●●○○○     ●●●●●    ●●●●●
09 PySpark             ●○○○○     ○○○○○      ●●●●●       ●●○○○     ●●●●●    ●●●●●
10 Streaming           ○○○○○     ○○○○○      ●●●●●       ●●○○○     ●●●●●    ●●●●●
11 Kafka               ○○○○○     ●●○○○      ●●●●●       ●●○○○     ●●●●●    ●●●●●
12 BigQuery            ○○○○○     ○○○○○      ●●●●●       ●●●●●     ●●●●●    ●●●●●
13 Airflow             ●●○○○     ○○○○○      ●●●●●       ●●●●○     ●●●●●    ●●●●●
14 dbt                 ●●○○○     ○○○○○      ●●●●●       ●●●●●     ●●●●●    ●●●●●
15 Table Formats       ○○○○○     ○○○○○      ●●●●●       ●●●○○     ●●●●●    ●●●●●
16 Data Modeling       ○○○○○     ●●○○○      ●●●●●       ●●●●●     ●●●●●    ●●●●●
17 System Design       ○○○○○     ●●○○○      ●●●●●       ●●●○○     ●●●●●    ●●●●●
18 Production Eng      ○○○○○     ○○○○○      ●●●●●       ●●○○○     ●●●●●    ●●●●●
19 Interview Prep      ●○○○○     ●●●○○      ○○○○○       ○○○○○     ●●●○○    ○○○○○

● = modules covered in this path   ○ = modules skipped or optional
(filled dots indicate depth, not just mention)
```
