# Graduation Requirements

What a graduate of this university should know at each engineering level. These are not course completions — they are demonstrated competencies. A student who has read all the modules but cannot answer these questions has not graduated.

---

## How to Use This Document

For each level, the graduation requirements are organized into:
1. **Theory** — concepts that must be explainable from first principles, without notes.
2. **Practice** — tasks that must be executable in a professional setting.
3. **Production** — failure scenarios and debugging capabilities.
4. **Architecture** — design decisions that must be justifiable.
5. **Communication** — the ability to convey technical knowledge to others.

A "graduation" at each level means: a senior engineer could interview you at that level and come away saying "this person is clearly operating at L[N]."

---

## Level 1 — Junior Data Engineer (L3)

**Semesters required:** 0–3 (complete)  
**Approximate experience equivalent:** 0–1 years on the job  
**Defining characteristic:** Can be given a well-scoped task and complete it with mentorship.

### Theory
- [ ] Explain what the CPU does during a Python `for` loop at the bytecode and hardware level.
- [ ] Explain the difference between a B-tree and an LSM-tree, and give a production example of each.
- [ ] Explain what ACID means and what "isolation" specifically guarantees.
- [ ] Explain why columnar storage is faster than row storage for aggregation queries.
- [ ] Explain what the GIL is and name one situation where it limits performance.

### Practice
- [ ] Write a SQL query using window functions to compute a 7-day rolling average.
- [ ] Read and interpret an EXPLAIN ANALYZE output for a simple query.
- [ ] Build a batch ELT pipeline with Python that is idempotent (running twice = same result).
- [ ] Write a dbt model with at least one schema test and one data test.
- [ ] Use `strace` to identify what system calls a Python script makes.

### Production
- [ ] Given a pipeline that has been silent (no output) for 6 hours, describe the first five things you check.
- [ ] Explain what a rolling deployment is and why you'd use it instead of a direct cutover.
- [ ] Describe two ways a batch pipeline can fail silently (without throwing an exception).

### Architecture
- [ ] Design a Bronze/Silver/Gold data lake for a given data source.
- [ ] Explain the ELT vs ETL trade-off for a given scenario.

### Communication
- [ ] Write a README for a pipeline that a new engineer can follow to run it without asking questions.
- [ ] Explain a technical concept (your choice) to a non-technical stakeholder in 2 minutes.

---

## Level 2 — Mid-Level Data Engineer (L4)

**Semesters required:** 0–4 (complete)  
**Approximate experience equivalent:** 2–4 years on the job  
**Defining characteristic:** Can scope and complete a moderately complex task independently. Begins to have opinions about tooling choices.

### Theory
- [ ] Explain Spark's execution model: driver, DAG, stages, tasks, shuffle boundaries.
- [ ] Explain Kafka's ISR and what `acks=all` + `min.insync.replicas=2` guarantees.
- [ ] Explain MVCC and how it enables non-blocking reads.
- [ ] Classify a given distributed system as CP or AP (with justification referencing CAP theorem).
- [ ] Explain event time vs processing time and why watermarks exist.
- [ ] Explain what a partition in Kafka represents physically (log segments, offsets, retention).

### Practice
- [ ] Debug a Spark data skew problem using the Spark UI (identify which task is slow and why).
- [ ] Write a PySpark Structured Streaming job that handles late events with a watermark.
- [ ] Configure a Kafka producer for exactly-once delivery (name the three required settings).
- [ ] Write a dbt incremental model with a correct unique key strategy for upserts.
- [ ] Design and implement a star schema with SCD Type 2 for a given business domain.

### Production
- [ ] Given a Spark OOM stack trace, identify the likely cause and propose a fix.
- [ ] Explain consumer lag, how to measure it, and what the common causes are.
- [ ] Describe what a dead-letter queue is and how to implement one for a batch pipeline.
- [ ] Given a dbt CI failure, describe how you would triage it.

### Architecture
- [ ] Explain the Bronze/Silver/Gold medallion architecture and the contract between each layer.
- [ ] Compare dimensional modeling vs Data Vault for a given use case.
- [ ] Choose between batch and streaming for a given latency requirement.

### Communication
- [ ] Explain a Spark performance problem and its solution to a non-Spark engineer.
- [ ] Write a data contract (schema + SLA) for a new data pipeline's Silver layer output.
- [ ] Give a code review with actionable, specific feedback (not just "fix this").

---

## Level 3 — Senior Data Engineer (L5)

**Semesters required:** 0–5 (complete)  
**Approximate experience equivalent:** 4–7 years on the job  
**Defining characteristic:** Can design and own a complex component of a data platform. Mentors junior engineers. Has a point of view on architectural choices.

### Theory
- [ ] Explain Raft leader election and log replication from memory, in a whiteboard setting.
- [ ] Explain the internal file layout of an Apache Iceberg table (manifest list → manifest files → data files → metadata files).
- [ ] Explain Spark's Catalyst optimizer and Tungsten execution engine.
- [ ] Explain the MESI cache coherence protocol and why false sharing is a performance problem.
- [ ] Explain Python's asyncio event loop, the relationship between coroutines and I/O, and when NOT to use asyncio.

### Practice
- [ ] Tune a Spark job: measure a provided job's baseline performance, apply 3 targeted optimizations, and quantify the improvement.
- [ ] Optimize a BigQuery query by ≥50% bytes scanned (using clustering, partitioning, or materialized views).
- [ ] Write a PySpark Pandas UDF and demonstrate it is faster than a Python UDF for the same transformation.
- [ ] Build a CI/CD pipeline for a dbt project using GitHub Actions.
- [ ] Define an SLO, write the Prometheus alert rule, and write a runbook for the alert.

### Production
- [ ] Write a complete post-mortem for a provided incident scenario.
- [ ] Explain Kafka consumer group rebalancing and describe three ways it causes consumer lag.
- [ ] Debug a provided Spark job that takes 10× longer on certain partition keys (skew).
- [ ] Diagnose a provided Python pipeline that has a memory leak using `tracemalloc`.

### Architecture
- [ ] Compare Apache Iceberg, Delta Lake, and Apache Hudi and recommend one for a given use case with justification.
- [ ] Design a data quality framework for a data platform with 50+ pipelines.
- [ ] Explain data mesh and identify which aspects of it are relevant to your current organization.

### Communication
- [ ] Mentor a junior engineer through a technical problem using the Socratic method (questions, not answers).
- [ ] Write an ADR for a technology choice that accurately represents the alternatives considered and their trade-offs.
- [ ] Lead a technical design review for a system you did not design.

---

## Level 4 — Staff Data Engineer (L6)

**Semesters required:** 0–6 (complete)  
**Approximate experience equivalent:** 7+ years on the job  
**Defining characteristic:** Sets technical direction for a team or domain. Makes architectural decisions with organization-wide consequences. Can own production reliability for a complete data platform.

### Theory
- [ ] Explain the CAP theorem, PACELC, and classify BigQuery, Kafka, and Postgres each with justification.
- [ ] Explain the Two Generals Problem and why it is the fundamental limit of distributed systems.
- [ ] Explain the difference between linearizability and sequential consistency with a concrete example of each.
- [ ] Explain Google's Dremel paper: what problem it solved, how shuffle servers enable massive parallelism, and how it influenced BigQuery.
- [ ] Explain the Dataflow Model paper: what problem it solved, the relationship between event time and watermarks, and how it influenced Flink and Beam.

### Practice
- [ ] Lead a 45-minute system design session for "Design a streaming data warehouse" without notes, covering requirements, capacity estimation, architecture, trade-offs, and failure modes.
- [ ] Design a multi-region data platform on GCP or AWS with RPO and RTO targets for each component.
- [ ] Write a complete architecture document (minimum 15 pages) for a greenfield data platform.
- [ ] Design and enforce a data contract standard across multiple teams.

### Production
- [ ] Design an observability strategy for a data platform: which SLOs to define, what to alert on, and how to route alerts.
- [ ] Lead an incident post-mortem and produce a post-mortem document that prevents recurrence.
- [ ] Evaluate a proposed architecture for failure modes (present the three most likely failures and their mitigations).

### Architecture
- [ ] Explain the trade-offs between Lambda architecture, Kappa architecture, and Lakehouse architecture.
- [ ] Design a data governance strategy for a company with 10+ data-producing teams.
- [ ] Choose an orchestration strategy for a portfolio of 100+ pipelines with mixed batch and streaming requirements.

### Communication
- [ ] Explain any technical concept in this curriculum to a business stakeholder, without jargon, in 5 minutes.
- [ ] Present an architecture proposal to a panel of senior engineers and defend it against strong objections.
- [ ] Write an RFC (Request for Comments) for a proposed technology change affecting multiple teams.

---

## Level 5 — Principal Data Engineer (L7)

**Semesters required:** 0–6 (complete) + ongoing research practice  
**Approximate experience equivalent:** 10+ years on the job, 3+ years at staff level  
**Defining characteristic:** Technical vision and strategy at organization scope. Makes decisions that affect 10+ engineers across multiple teams. Defines what "good" looks like for the entire engineering organization.

### Theory
- [ ] For any significant technology in your stack, be able to cite and summarize the founding paper.
- [ ] Explain the academic basis for "exactly-once" semantics and why it is impossible without specific infrastructure constraints.
- [ ] Explain CRDT (Conflict-free Replicated Data Types), why they exist, and whether they apply to your organization's data systems.

### Practice
- [ ] Design a company-wide data architecture strategy (not just a single platform) that scales for 5 years.
- [ ] Build and maintain a technology radar for data engineering.
- [ ] Define the engineering career ladder for the data engineering track.

### Production
- [ ] Design a post-mortem process that is adopted and followed by the entire engineering organization.
- [ ] Build a reliability engineering program for a data platform: SLOs, error budgets, toil reduction.

### Architecture
- [ ] Lead an organization-wide migration from one architectural paradigm to another (e.g., ETL → ELT, warehouse → lakehouse).
- [ ] Make a build-vs-buy decision for a core infrastructure component with a full written analysis.

### Communication
- [ ] Write a technical strategy document that is understandable and actionable by both engineers and executives.
- [ ] Influence architecture decisions you are not directly responsible for, without authority.
- [ ] Develop and publish reusable engineering principles that are adopted across the organization.

---

## Graduation Summary

| Level | Title | Semesters | Key Differentiator |
|---|---|---|---|
| L3 | Junior Data Engineer | 0–3 | Can execute scoped tasks with mentorship |
| L4 | Mid-Level Data Engineer | 0–4 | Can scope and execute independently |
| L5 | Senior Data Engineer | 0–5 | Can design and own complex components; mentors others |
| L6 | Staff Data Engineer | 0–6 | Sets technical direction; makes architectural decisions |
| L7 | Principal Data Engineer | 0–6 + research practice | Technical vision at organization scope |
