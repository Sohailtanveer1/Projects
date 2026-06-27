# Capstone Projects

Seven progressively harder projects — one per semester. Each project is self-contained, resembles real-world engineering work, and demonstrates mastery of the semester's skills. Projects are graded by the student through self-review against the acceptance criteria, not by automated systems.

---

## Design Principles for Capstones

1. **Every project produces a real artifact.** Not a quiz answer or a summary. Working code, running infrastructure, or a publishable architecture document.
2. **Every project has measurable acceptance criteria.** "Implement X" is not an acceptance criterion. "X runs and produces output Y, measurable as Z" is.
3. **Projects escalate in scope.** A Semester 6 project cannot be completed with Semester 3 knowledge. A Semester 0 project cannot require Semester 3 knowledge.
4. **Projects are defensible.** Every project includes a "defense" component: the student must explain every design decision under cross-examination. If they can't explain it, they don't pass.
5. **Projects are portfolio-worthy.** Every completed project should be publishable on GitHub as evidence of engineering ability.

---

## Semester 0 Capstone — "Profile Before You Optimize"

**Theme:** Understanding performance through measurement, not guessing.

### Setup
A provided Python script (`slow_pipeline.py`) processes 10 million integer records using a naive Python loop, computes a running total, filters records above a threshold, and writes results to a CSV. It takes approximately 3–5 minutes to run.

### Deliverables

**Part 1 — Baseline Measurement**
Run the script with `time`, `cProfile`, and `tracemalloc`. Document:
- Wall clock time
- Top 5 functions by cumulative time (from cProfile)
- Peak memory usage (from tracemalloc)
- CPU utilization during the run (from `top` or `htop`)

**Part 2 — Bottleneck Classification**
Classify the bottleneck as CPU-bound, memory-bound, or I/O-bound. Provide evidence for your classification (not a guess — show the data).

**Part 3 — One Targeted Fix**
Apply one of the following optimizations (student's choice, justified):
- Replace the Python loop with NumPy vectorized operations
- Restructure memory access from random to sequential
- Reduce the number of Python object allocations in the hot path

**Part 4 — Remeasurement**
Re-run all measurements from Part 1. Document the improvement. Calculate the speedup ratio.

**Part 5 — Technical Explanation**
Write 400–600 words explaining WHY the optimization worked. Must reference: cache lines, CPU pipeline stalls, Python interpreter dispatch overhead, or reference counting — whichever applies to the chosen fix. No vague language ("it's faster because NumPy is fast"). Specific mechanisms only.

### Acceptance Criteria
- [ ] Baseline measurement documented with actual numbers (not estimated).
- [ ] Bottleneck classification supported by cProfile or perf data.
- [ ] Speedup ≥ 2× (measured, not claimed).
- [ ] Technical explanation cites a specific hardware mechanism.
- [ ] Student can explain any line of the optimized code under cross-examination.

### Portfolio Packaging
Publish as a GitHub repository with: `slow_pipeline.py`, `optimized_pipeline.py`, `ANALYSIS.md` (containing the full Part 1–5 report), and a `README.md` with a one-paragraph summary.

---

## Semester 1 Capstone — "Linux System Monitor"

**Theme:** Building operational tooling from scratch, understanding the OS from the inside.

### What You Build
A Python CLI tool that monitors system resources using Linux's `/proc` filesystem — no `psutil`, no `subprocess`, no external dependencies beyond the Python standard library.

### Deliverables

**Core Tool (`sysmon.py`)**
- Reads CPU usage per core from `/proc/stat` (requires computing delta between two reads).
- Reads memory breakdown (total, free, available, cached, buffers) from `/proc/meminfo`.
- Reads disk I/O per device (read_bytes, write_bytes, read_ops, write_ops) from `/proc/diskstats`.
- Reads network I/O per interface (bytes_in, bytes_out, packets_in, packets_out) from `/proc/net/dev`.
- Displays a live dashboard that refreshes every 2 seconds using `curses` or ANSI escape codes.

**Alerting**
- Configurable thresholds via a YAML file (not hardcoded).
- Alerts to stdout when CPU > threshold, memory available < threshold, or disk write rate > threshold.
- Log alerts to a rotating log file using Python's `logging` module with `RotatingFileHandler`.

**Systemd Service**
A shell script (`install.sh`) that:
- Copies the tool to `/usr/local/bin/sysmon`.
- Creates a systemd unit file at `/etc/systemd/system/sysmon.service`.
- Enables and starts the service.
- Verifies the service is running (`systemctl status sysmon`).

**Unit Tests**
At least 5 unit tests using `pytest`:
- Test that `/proc/stat` parsing produces correct values for a known input.
- Test that alert thresholds trigger correctly.
- Test that log rotation works.
- Test that the config YAML parser handles malformed input gracefully.
- Test that disk I/O delta calculation is correct between two consecutive reads.

### Acceptance Criteria
- [ ] Tool reads all four resource types correctly (verified by comparing to `top` and `iostat`).
- [ ] Live dashboard refreshes without screen flicker.
- [ ] Alerts fire correctly when thresholds are crossed (simulated by using `stress` or `dd`).
- [ ] All 5 tests pass with `pytest`.
- [ ] `install.sh` installs and starts the service cleanly on a fresh Ubuntu 22.04 VM.
- [ ] Code passes `mypy` (strict) and `ruff`.
- [ ] Student can explain: what `/proc/stat`'s idle vs total counters mean, how rotating logs work, and why systemd manages the process lifecycle rather than a while-loop.

---

## Semester 2 Capstone — "Distributed Key-Value Store"

**Theme:** Implementing distributed systems theory rather than just studying it.

### What You Build
A simplified distributed key-value store with leader election, replication, and failure recovery. Python sockets only — no gRPC, no Redis, no ZooKeeper.

### Architecture
- 3 server nodes running as separate OS processes.
- 1 CLI client.
- Communication over TCP sockets.
- Persistent storage: each node writes its state to a local JSON file.

### Required Behavior

**Leader Election**
- On startup, nodes discover each other via a static config file (`nodes.yaml`).
- The node with the lowest ID that is reachable by a majority becomes leader.
- If the leader fails (no heartbeat for 5 seconds), the remaining nodes elect a new leader.

**Writes**
- Client sends `SET key value` to any node.
- Non-leader nodes forward to the leader.
- Leader writes to its local store and broadcasts to followers.
- Leader acknowledges to the client only after a majority (≥ 2 of 3) confirms the write.

**Reads**
- `GET key` returns the value from any node (eventual consistency — the node serves its local state).
- `GET_CONSISTENT key` is routed to the leader for linearizable reads.

**Failure Handling**
- Kill one node with `kill -9`. The remaining two nodes elect a new leader within 10 seconds.
- All writes during the single-node-failure period are committed to the majority.
- When the killed node restarts, it catches up from the leader.

### Acceptance Criteria
- [ ] Three-node cluster starts cleanly from `./start_cluster.sh`.
- [ ] `SET` and `GET` work correctly in the healthy state.
- [ ] Killing one node does not interrupt the ability to SET and GET (after a new leader is elected).
- [ ] The restarted node syncs its state from the leader.
- [ ] Student can explain: why majority quorum is required, what happens if you kill 2 of 3 nodes, and why reads from followers can be stale.

---

## Semester 3 Capstone — "SQL Analytics Platform"

**Theme:** Building a queryable analytical platform from raw data, end-to-end.

### Dataset
NYC Taxi & Limousine Commission (TLC) Trip Record Data. Publicly available at nyc.gov/site/tlc/about/tlc-trip-record-data.page. Use 2022 Yellow Taxi data (~12M rows across 12 monthly files).

### Deliverables

**1. Star Schema Design**
Design and implement in Postgres:
- `fact_trips` (trip_id, vendor_key, pickup_zone_key, dropoff_zone_key, date_key, passenger_count, trip_distance, fare_amount, total_amount, duration_seconds)
- `dim_vendor` (vendor_key, vendor_name)
- `dim_zone` (zone_key, zone_name, borough, service_zone)
- `dim_date` (date_key, full_date, year, month, day, day_of_week, is_weekend, quarter)

**2. Indexes**
Create indexes and document with BEFORE/AFTER EXPLAIN ANALYZE output:
- Index on `fact_trips.pickup_zone_key` (for zone-based queries).
- Index on `fact_trips.date_key` (for date-range queries).
- Composite index on `(date_key, vendor_key)` (for the most common analytical pattern).

**3. Ten Analytical SQL Queries**
Each query must use at least one of: window function, CTE, advanced aggregation (ROLLUP/CUBE/GROUPING SETS). Queries must answer a real business question (provided as a spec).

Example questions:
- Rolling 7-day average revenue per borough.
- Top 10 pickup zones by total revenue, for each month (using RANK()).
- Revenue by vendor as a percentage of total revenue.
- Trips that crossed both 10 PM and midnight (gap/island problem).
- Customers cohort analysis (first trip month × return month matrix).

**4. Python Incremental Load Pipeline**
A Python script (`load_incremental.py`) that:
- Reads a new monthly CSV file from a configurable source path.
- Validates: row count > 0, no nulls in mandatory columns, fare_amount > 0.
- Transforms to match the star schema.
- Upserts into Postgres using `ON CONFLICT DO UPDATE`.
- Logs success or failure to a structured JSON log.
- Is idempotent: running it twice with the same file produces the same result.

**5. Data Quality Checks**
A second script (`check_quality.py`) that runs after each load:
- Row count vs previous load (flag if > 20% different).
- Null rate per column (flag if any mandatory column has > 0% nulls).
- Value range checks (fare_amount > 0, trip_distance > 0, passenger_count ≤ 8).
- Prints a pass/fail report per check.

### Acceptance Criteria
- [ ] All 10 queries return correct results (verified against the dataset's known statistics).
- [ ] EXPLAIN ANALYZE shows index usage for all three indexed queries.
- [ ] Incremental load is idempotent (running twice = same result).
- [ ] Data quality checks catch intentionally injected bad rows (provided test CSV with 5 violations).
- [ ] Student can explain any query plan and any design choice in the star schema.

---

## Semester 4 Capstone — "Spark Batch Platform"

**Theme:** Building and tuning a distributed batch processing platform.

### What You Build
A local Spark environment (Docker Compose) that processes the NYC Taxi data at scale using PySpark, dbt, and Airflow, with Delta Lake as the storage layer.

### Architecture
```
Raw CSVs (local MinIO)
    → Bronze Layer (PySpark, Delta Lake)
    → Silver Layer (PySpark transformation, Delta Lake)
    → Gold Layer (dbt models on DuckDB)
    → Airflow DAG orchestrating all steps
    → Data quality checks at each layer boundary
```

### Deliverables

**Bronze Layer (PySpark)**
- Read raw CSVs from MinIO using `spark.read.csv()`.
- Add audit columns: `_ingested_at` (timestamp), `_source_file` (filename), `_batch_id` (UUID).
- Write as Delta Lake table, partitioned by `pickup_year` and `pickup_month`.
- Schema enforcement: reject rows that don't match the expected schema.

**Silver Layer (PySpark)**
- Join with zone lookup table.
- Filter invalid rows (fare_amount ≤ 0, trip_distance ≤ 0, passenger_count > 8).
- Cast and standardize column types.
- Add computed columns: `trip_duration_minutes`, `revenue_per_mile`.
- Write as Delta Lake table. Use `MERGE INTO` for upserts (idempotent runs based on `trip_id`).

**Gold Layer (dbt on DuckDB)**
- `gold_daily_revenue_by_zone`: daily revenue and trip count per pickup zone.
- `gold_vendor_comparison`: revenue, average trip distance, and average fare by vendor, per month.
- `gold_peak_hours`: average trips per hour of day by borough.
- All 3 models: schema tests (not_null, unique, accepted_values) + 1 custom data test each.

**Airflow DAG**
- Tasks: (1) ingest_bronze, (2) transform_silver, (3) run_dbt_gold, (4) check_data_quality.
- Task 4 reads dbt test results and fails the DAG if any test failed.
- Email alert (configured to local SMTP mock) on DAG failure.
- Retry logic: 2 retries with 5-minute delay on tasks 1–3.

**Performance Tuning**
Document the Spark optimization applied at each stage:
- Bronze: appropriate partition count for the data volume.
- Silver: broadcast join for the zone lookup table (it's small).
- Silver: AQE enabled — show before/after job duration.

### Acceptance Criteria
- [ ] End-to-end pipeline runs from raw CSV to dbt Gold in a single `docker-compose up`.
- [ ] Silver MERGE INTO is idempotent (run twice = same result, verified by row count).
- [ ] All 3 dbt models and all schema tests pass.
- [ ] Airflow DAG shows correct dependency graph; failure in task 2 skips tasks 3 and 4.
- [ ] Student demonstrates AQE improvement in the Spark UI.
- [ ] Student can explain every Spark configuration parameter in the job (no magic numbers).

---

## Semester 5 Capstone — "Real-Time Streaming Platform"

**Theme:** Building a production-grade streaming pipeline with monitoring and schema governance.

### Architecture
```
Python Event Producer
    → Kafka (8 partitions, Avro schema in Confluent Schema Registry)
    → PySpark Structured Streaming (5-min tumbling windows, watermark)
    → Delta Lake (Silver layer, streaming writes)
    → Airflow DAG (daily dbt Gold runs over Delta Lake)
    → BigQuery External Table (serving layer)
    → Prometheus + Grafana (monitoring)
```

### Deliverables

**Event Producer**
- Python app generating synthetic e-commerce events: `(user_id, product_id, action, amount, event_timestamp)`.
- Publishes to Kafka at 10,000 events/second sustained.
- Uses Avro schema registered in Schema Registry.
- Implements backward-compatible schema change: adds an optional `session_id` field without breaking existing consumers.

**PySpark Structured Streaming**
- Subscribes to Kafka topic.
- Computes per-product `SUM(amount)` in a 5-minute tumbling window (event time).
- Watermark: allow up to 10 minutes of late data.
- Sink: Delta Lake Silver table using `foreachBatch`.
- Metrics: log batch processing time and records processed per batch to stdout.

**dbt Gold Models**
- `gold_product_revenue_daily`: daily revenue per product, built incrementally.
- `gold_top_products_weekly`: top 10 products by weekly revenue (window function).
- CI: GitHub Actions runs dbt tests on every PR.

**BigQuery Serving**
- BigQuery external table pointing at Delta Lake's Parquet files on GCS (or simulated with local files).
- Clustering by `product_id` and partitioning by date.
- Query optimization: run INFORMATION_SCHEMA.JOBS to show bytes scanned before vs after clustering.

**Monitoring**
- Prometheus metrics exposed by the producer: `events_produced_total`, `producer_lag_seconds`.
- Prometheus metrics for the consumer: `consumer_lag` (computed as producer latest offset − consumer committed offset).
- Grafana dashboard with: events/sec graph, consumer lag graph, 5-min window output count.
- Alert rule: fire if consumer lag exceeds 60 seconds for more than 2 minutes.

### Acceptance Criteria
- [ ] Producer sustains 10K events/second (verified by Prometheus metric).
- [ ] Structured Streaming produces correct 5-minute window aggregations (verified by comparing expected vs actual for a known subset of events).
- [ ] Backward-compatible schema change deployed without restarting the consumer.
- [ ] Grafana dashboard shows all three graphs with live data.
- [ ] Alert fires correctly when consumer lag is artificially induced (by pausing the consumer for 3 minutes).
- [ ] Student explains watermark behavior when a late event arrives 12 minutes after its event time.

---

## Semester 6 Capstone — "Enterprise Data Platform"

**Theme:** Staff-level system design, implementation, and incident response.

This is the graduation project. It is evaluated at the same standard as a staff engineer take-home or a principal engineer architecture review.

### Part 1 — Architecture Document (minimum 25 pages)

Design a lakehouse for a fintech company with the following requirements:
- 50 billion events per day ingested from two sources: Kafka (real-time transaction events) and S3 (daily batch partner files).
- Data retained for 7 years with full audit trail.
- P95 query latency ≤ 5 seconds on Gold layer aggregations.
- Sub-minute latency for fraud detection streaming alerts.
- Multi-region: active-active in US-EAST and EU-WEST.
- Compliance: GDPR right-to-be-forgotten, SOC2 audit log.

**Required sections:**
1. Executive Summary (for a VP of Engineering, no jargon).
2. Architecture Diagram (Mermaid, fully labelled).
3. Data Flow Design: ingestion → bronze → silver → gold → serving.
4. Table Format Choice: compare Iceberg, Delta, Hudi; select one with justification.
5. Orchestration Design: Airflow DAG dependency map.
6. Serving Layer: BigQuery external tables + dbt Gold models.
7. Streaming Architecture: Kafka → PySpark Streaming for fraud detection.
8. Governance: data catalog, lineage, GDPR deletion strategy.
9. Observability: SLO definitions, alert rules, runbooks (3 runbooks minimum).
10. Cost Model: estimated monthly cloud cost with 3 optimization levers.
11. Failure Mode Analysis: top 5 failure scenarios with mitigation.
12. Three Architecture Decision Records:
    - Table format selection.
    - Orchestrator selection.
    - Streaming vs micro-batch for fraud detection.

**Evaluation standard:** Would a Staff Engineer at Google, Meta, or Databricks approve this architecture? If not, it doesn't pass.

### Part 2 — Scaled-Down Implementation

Implement the Silver layer transformation using a 1GB sample dataset (synthetic fintech events, provided):
- PySpark job with correct partitioning, schema validation, and Delta Lake MERGE INTO.
- dbt Gold layer: 5 models, 10 tests (mix of schema tests and custom data tests), full documentation.
- GitHub Actions CI: runs dbt tests on PR, runs PySpark unit tests on PR.
- 3 PySpark unit tests using `pytest` and `pyspark.sql.testing`.

### Part 3 — Incident Post-Mortem

Given the following incident report:

> "At 14:32 UTC on Thursday, our Gold layer `daily_revenue` dbt model produced revenue figures 23% lower than expected for the previous day. The discrepancy was not detected until 09:15 UTC Friday, when a business analyst flagged the numbers in a weekly report. By the time the incident was resolved at 11:40 UTC Friday, the incorrect figures had been included in a board-level financial report."

Write a complete post-mortem (minimum 800 words):
- Incident timeline (reconstruct from the given information + any plausible technical events).
- Root cause analysis (propose a plausible technical root cause — must be consistent with the architecture).
- Impact assessment (data, business, and compliance impact).
- Immediate remediation steps.
- Five preventive measures (specific, implementable — not "be more careful").

### Acceptance Criteria
- [ ] Architecture document passes a 30-minute cross-examination by a senior/staff reviewer.
- [ ] All three ADRs include at least 3 alternatives considered, not just 1.
- [ ] Implementation runs end-to-end; all dbt tests pass; CI pipeline is green.
- [ ] Post-mortem root cause is technically plausible and consistent with the architecture.
- [ ] Post-mortem's five preventive measures are specific and implementable (not generic).
- [ ] Student defends every design decision in Part 1 under 30-minute cross-examination.
