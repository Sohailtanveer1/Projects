# M56: Postgres for Data Engineering

**Course:** DBE-INT-102 — Postgres Architecture  
**Module:** 05 of 05  
**Global Module ID:** M56  
**Semester:** 2  
**School:** Database Engineering (DBE)

---

## Table of Contents

1. [The Why](#1-the-why)
2. [Mental Model](#2-mental-model)
3. [Core Concepts](#3-core-concepts)
4. [Hands-On Walkthrough](#4-hands-on-walkthrough)
5. [Code Toolkit](#5-code-toolkit)
6. [Visual Reference](#6-visual-reference)
7. [Common Mistakes](#7-common-mistakes)
8. [Production Failure Scenarios](#8-production-failure-scenarios)
9. [Performance and Tuning](#9-performance-and-tuning)
10. [Interview Q&A](#10-interview-qa)
11. [Cross-Question Chain](#11-cross-question-chain)
12. [Flashcards](#12-flashcards)
13. [Further Reading](#13-further-reading)
14. [Lab Exercises](#14-lab-exercises)
15. [Key Takeaways](#15-key-takeaways)
16. [Connections to Other Modules](#16-connections-to-other-modules)
17. [Anti-Patterns](#17-anti-patterns)
18. [Tools Reference](#18-tools-reference)
19. [Glossary](#19-glossary)
20. [Self-Assessment](#20-self-assessment)
21. [Module Summary](#21-module-summary)

---

## 1. The Why

Data engineers use Postgres in at least four distinct roles: as an operational database storing application data, as the metadata store for orchestration tools (Airflow, dbt, Prefect all default to Postgres for their own state), as a CDC (Change Data Capture) source that feeds Kafka or a data lake, and as a transient query layer for dbt transformations and data quality checks. Each of these roles places different demands on Postgres.

The operational role generates the connection problem: modern applications open hundreds of short-lived connections, each requiring a fork() and ~10MB of RAM. At 500 connections, Postgres backends consume 5GB before any data is cached. PgBouncer solves this by multiplexing thousands of application connections onto tens of actual Postgres backend processes. Without PgBouncer, Postgres connection overhead is the bottleneck in most high-connection application stacks.

The CDC role generates the change capture problem: how does a downstream analytics system know which rows changed since the last run? Polling (`SELECT * WHERE updated_at > last_run`) has gaps (missed deletes, high latency, table scans). Triggers add write overhead to every transaction. Logical replication — Postgres's built-in CDC mechanism — reads decoded WAL records and streams row-level changes in real time. It is the foundation for tools like Debezium, which feeds Kafka topics with every INSERT/UPDATE/DELETE from Postgres.

This module covers both: PgBouncer architecture and configuration, and Postgres logical replication from the WAL decoder to downstream consumers.

---

## 2. Mental Model

### PgBouncer: N Application Connections → M Backend Processes

PgBouncer sits between the application and Postgres as a connection proxy. The application thinks it's talking to Postgres (same wire protocol, same port minus 1). PgBouncer maintains a pool of real Postgres backend connections and routes incoming application queries to any available backend.

The multiplexing ratio is the key metric: 1,000 application connections → 25 Postgres backends = 40:1 multiplexing. At 25 backends, Postgres uses 250MB for connection overhead instead of 10GB. Query latency increases slightly (1-2ms overhead per query for pooler routing) but overall throughput is dramatically higher.

The pool modes determine what happens between queries:
- **Session mode:** Each application connection is assigned a backend for its entire session. No multiplexing — 1,000 application connections still require 1,000 backends. Preserves all session-level state.
- **Transaction mode:** A backend is assigned for one transaction and returned to the pool afterward. 1,000 application connections share 25 backends. Breaks session-level features (PREPARE, SET, advisory locks).
- **Statement mode:** A backend is assigned for one statement. Maximum multiplexing. Breaks anything requiring a transaction (autocommit only).

### Logical Replication: WAL as a Change Stream

Physical replication (streaming replication for high availability) ships raw WAL bytes and replays them on the standby — an exact byte-for-byte copy. Logical replication decodes those same WAL bytes into row-level change events: `{table: "orders", op: "INSERT", new: {id: 42, status: "pending"}}`. These decoded events can be sent to:
- **Another Postgres instance** (for selective table replication, cross-version upgrades, or fan-out to read replicas of specific tables)
- **An external consumer** (Debezium reading from a replication slot, streaming to Kafka; or Airbyte for ELT pipelines)

The WAL decoder is the key component: it reads the WAL stream and translates binary WAL records into logical change events using each table's schema (fetched from the system catalog). The output format is configurable (`pgoutput` is the native format; `wal2json` outputs JSON changes; `decoderbufs` outputs Protocol Buffers).

**Replication slots** are the persistence mechanism: they track how far a consumer has read and ensure WAL is not deleted until the consumer has processed it. This is the source of the most dangerous production failure with logical replication: an abandoned or lagging replication slot causes WAL to accumulate indefinitely, eventually filling the disk.

---

## 3. Core Concepts

### 3.1 PgBouncer Architecture

PgBouncer is a single-process, single-thread application (using libevent for async I/O). It maintains:
- **A client connection pool:** All connections from applications, managed as state machines (idle, waiting, in_query)
- **A server connection pool:** The actual Postgres backend connections, each pinned to a database/user pair
- **Pool queues:** When all server connections are in use, client queries queue until a server connection becomes available

```
Application layer:
  App Server 1 (20 threads) ──┐
  App Server 2 (20 threads) ──┤
  ...                          ├── 1,000 client connections → PgBouncer (port 6432)
  App Server 50 (20 threads) ─┘
                                            │
                                    Transaction pooling
                                    25 server connections
                                            │
                               Postgres primary (port 5432)
                               25 backend processes, 250MB overhead
```

**PgBouncer configuration (`pgbouncer.ini`):**
```ini
[databases]
; Route "mydb" connections to the actual Postgres server
mydb = host=127.0.0.1 port=5432 dbname=mydb

[pgbouncer]
; Port PgBouncer listens on (applications connect here)
listen_port = 6432
listen_addr = 0.0.0.0

; Pool mode: session, transaction, or statement
pool_mode = transaction

; Maximum client connections PgBouncer accepts
max_client_conn = 5000

; Server connections per database+user pair in the pool
default_pool_size = 25

; Minimum server connections to keep open (reduce connect latency)
min_pool_size = 5

; Maximum connections per database
max_db_connections = 100

; Close idle server connections after this many seconds
server_idle_timeout = 600

; Close idle client connections after this many seconds
client_idle_timeout = 0   ; 0 = no timeout

; Authentication
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt

; Admin access
admin_users = pgbouncer_admin
stats_users = monitoring_user

; Logging
logfile = /var/log/pgbouncer/pgbouncer.log
pidfile = /var/run/pgbouncer/pgbouncer.pid
```

**PgBouncer SHOW commands** (connect to the `pgbouncer` virtual database):
```sql
SHOW POOLS;    -- Pool stats: cl_active, cl_waiting, sv_active, sv_idle, sv_used
SHOW CLIENTS;  -- Client connection states
SHOW SERVERS;  -- Server connection states
SHOW STATS;    -- Per-database stats: total_query_time, total_wait_time, avg_query_count
SHOW CONFIG;   -- Current configuration
RELOAD;        -- Reload pgbouncer.ini without restart
PAUSE;         -- Pause all pools (for Postgres maintenance)
RESUME;        -- Resume after PAUSE
```

### 3.2 PgBouncer in Data Engineering Pipelines

Data engineering tooling interacts with Postgres in specific patterns that affect PgBouncer configuration:

**Apache Airflow metadata database:** Airflow uses SQLAlchemy with a connection pool. Each Airflow worker process opens connections to the metadata DB to update task state. With 50 workers × 5 pool connections = 250 connections to the metadata Postgres. PgBouncer in transaction mode reduces this to 5-10 actual Postgres backends without affecting Airflow.

**dbt:** dbt opens one connection per model thread (`threads` in `profiles.yml`). With `threads = 8`, dbt opens 8 connections. This is already small enough that PgBouncer is usually unnecessary for dbt itself. However, dbt uses `SET` commands (e.g., `SET search_path`) which are session-level and break in PgBouncer transaction mode. Use PgBouncer session mode for dbt, or configure dbt to pass `search_path` in the connection DSN: `host=pgbouncer_host options=-csearch_path%3Dmyschema`.

**Python data pipelines (psycopg2, asyncpg):** High-throughput ingestion pipelines that open many short-lived connections benefit most from PgBouncer. Configure the pipeline's connection pool to target PgBouncer, not Postgres directly.

### 3.3 Logical Replication Architecture

Logical replication uses a **publication/subscription** model:

**On the source (publisher):**
1. A **replication slot** is created: `pg_create_logical_replication_slot('slot_name', 'pgoutput')`. The slot tracks the WAL LSN up to which the consumer has consumed changes.
2. A **publication** defines which tables to replicate: `CREATE PUBLICATION my_pub FOR TABLE orders, customers`.
3. The **walsender** process reads WAL, decodes it using the `pgoutput` plugin, and streams decoded changes to the consumer.

**On the consumer (subscriber):**
- For Postgres-to-Postgres replication: `CREATE SUBSCRIPTION my_sub CONNECTION 'host=primary dbname=mydb' PUBLICATION my_pub;`
- For external tools (Debezium, Airbyte, custom): connect to the walsender using the replication protocol, consume from the replication slot.

**The WAL decode process:**
1. WAL record is read: `INSERT INTO orders (id, customer_id, status) VALUES (42, 7, 'pending')`
2. The decoder looks up the table's schema (column names, types) from the system catalog
3. Output (pgoutput format): `{type: "INSERT", relation: "orders", new: {id: 42, customer_id: 7, status: "pending"}}`

**Prerequisite: `REPLICA IDENTITY`**
For UPDATE and DELETE events, logical replication needs to identify which row was changed. The default `REPLICA IDENTITY FULL` option uses the entire old row as the identity. The better option for tables with primary keys: `REPLICA IDENTITY DEFAULT` (uses the primary key) or `REPLICA IDENTITY USING INDEX` (uses a unique index). Tables without a primary key and without REPLICA IDENTITY FULL cannot have their UPDATEs or DELETEs replicated.

```sql
-- Check and set replica identity
SELECT relreplident FROM pg_class WHERE relname = 'orders';
-- d = default (primary key), f = full, n = nothing, i = index

-- For a table with a primary key (recommended):
ALTER TABLE orders REPLICA IDENTITY DEFAULT;  -- Uses primary key

-- For a table without a primary key (all columns sent — high overhead):
ALTER TABLE orders REPLICA IDENTITY FULL;
```

### 3.4 Replication Slots: The WAL Retention Mechanism

A replication slot tells Postgres: "do not delete WAL segments until consumer X has confirmed consuming up to LSN Y." This guarantees that a consumer that falls behind can catch up without missing changes.

The danger: an inactive or lagging replication slot causes WAL to accumulate indefinitely. If the consumer stops processing (crashes, is paused, has a bug), the slot's `restart_lsn` does not advance, and Postgres retains all WAL since the slot's `restart_lsn`. A consumer that stops for 24 hours on a high-write database can cause gigabytes or terabytes of WAL accumulation, eventually filling the disk and crashing Postgres.

```sql
-- Monitor replication slots (most critical operational query for logical replication)
SELECT
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(
        pg_current_wal_lsn(),
        restart_lsn
    )) AS retained_wal,
    restart_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots;

-- Drop an abandoned slot (DANGEROUS: consumer will need to reinitialize from scratch)
SELECT pg_drop_replication_slot('abandoned_slot_name');

-- Limit WAL retention (Postgres 13+): cap WAL retained per slot
-- max_slot_wal_keep_size (set in postgresql.conf):
-- If a slot's WAL retention exceeds this, the slot is invalidated
-- (consumer must reinitialize, but disk is protected)
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

### 3.5 Debezium: CDC from Postgres to Kafka

Debezium is the most widely-used CDC connector for Postgres → Kafka pipelines. It uses a Kafka Connect plugin that:
1. Creates a replication slot on Postgres (via `pg_create_logical_replication_slot`)
2. Takes an initial snapshot of the table(s) (reads current data)
3. Streams WAL changes from the slot as decoded events
4. Publishes each event to a Kafka topic named `{server}.{schema}.{table}`

Each Kafka message contains:
```json
{
  "before": null,                          // Old row (null for INSERT)
  "after": {"id": 42, "status": "pending"}, // New row
  "op": "c",                               // c=create, u=update, d=delete, r=read(snapshot)
  "ts_ms": 1704067200000,
  "source": {
    "db": "mydb",
    "table": "orders",
    "lsn": 12345678,
    "txId": 5001
  }
}
```

**Debezium Postgres connector configuration:**
```json
{
  "name": "orders-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres-primary.internal",
    "database.port": "5432",
    "database.user": "debezium_user",
    "database.password": "secret",
    "database.dbname": "mydb",
    "database.server.name": "mydb-cdc",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_orders",
    "publication.name": "debezium_publication",
    "table.include.list": "public.orders,public.customers",
    "heartbeat.interval.ms": "10000",
    "snapshot.mode": "initial",
    "decimal.handling.mode": "double",
    "topic.prefix": "mydb"
  }
}
```

### 3.6 WAL Configuration for Logical Replication

Logical replication requires specific WAL settings on the source Postgres:

```sql
-- wal_level must be 'logical' (not 'replica' or 'minimal')
-- This enables the WAL decoder to write column-level change data
ALTER SYSTEM SET wal_level = 'logical';  -- Requires restart

-- Maximum number of replication slots
ALTER SYSTEM SET max_replication_slots = 10;  -- Default: 10

-- Maximum number of walsender processes (one per consumer)
ALTER SYSTEM SET max_wal_senders = 10;  -- Default: 10

-- Verify current settings
SHOW wal_level;
SELECT * FROM pg_replication_slots;
SELECT * FROM pg_stat_replication;  -- Active walsender connections
```

---

## 4. Hands-On Walkthrough

### 4.1 Logical Replication Setup (Source Side)

```sql
-- Step 1: Enable logical WAL (requires restart if currently 'replica')
-- In postgresql.conf or via ALTER SYSTEM:
-- wal_level = logical
-- (Already set: SHOW wal_level → 'logical')

-- Step 2: Create a publication for the tables to replicate
CREATE PUBLICATION orders_pub FOR TABLE orders, customers, products;

-- Or replicate all tables:
CREATE PUBLICATION all_tables_pub FOR ALL TABLES;

-- Step 3: Create a replication slot for the consumer
-- (Debezium does this automatically; manual example:)
SELECT pg_create_logical_replication_slot('my_consumer_slot', 'pgoutput');

-- Step 4: Set REPLICA IDENTITY for tables with UPDATE/DELETE events
ALTER TABLE orders   REPLICA IDENTITY DEFAULT;  -- Uses primary key
ALTER TABLE customers REPLICA IDENTITY DEFAULT;
-- For tables without PK:
ALTER TABLE audit_log REPLICA IDENTITY FULL;    -- Uses all columns (expensive)

-- Step 5: Verify publication
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables;

-- Step 6: Grant replication privilege to Debezium user
CREATE ROLE debezium_user REPLICATION LOGIN PASSWORD 'secret';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
-- In pg_hba.conf: allow replication connections from debezium host
-- host    replication     debezium_user   10.0.1.100/32   scram-sha-256

-- Step 7: Monitor replication lag
SELECT
    slot_name, active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_retained,
    restart_lsn, confirmed_flush_lsn,
    CASE WHEN active THEN 'consuming' ELSE 'INACTIVE — WAL ACCUMULATING' END AS status
FROM pg_replication_slots;
```

### 4.2 Consuming the Replication Stream (Python Simulation)

```sql
-- Peek at changes using pg_logical_slot_peek_changes (does not advance the slot)
SELECT * FROM pg_logical_slot_peek_changes(
    'my_consumer_slot',
    NULL,   -- up to LSN (NULL = all available)
    NULL,   -- max number of changes (NULL = all)
    'include-xids', '1',
    'include-timestamp', '1'
) LIMIT 10;

-- Consume changes (advances the slot's confirmed_flush_lsn)
SELECT * FROM pg_logical_slot_get_changes(
    'my_consumer_slot', NULL, NULL,
    'include-xids', '1'
) LIMIT 100;
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
postgres_data_engineering.py

Simulate PgBouncer connection pooling and Postgres logical replication for CDC.

Components:
  - ConnectionState: Client/server connection state machine
  - PgBouncerPool: Transaction-mode connection pool (multiplexes clients → servers)
  - PoolMonitor: Simulates SHOW POOLS output
  - WALRecord: Represents a decoded logical replication event
  - ReplicationSlot: Tracks consumer position and WAL retention
  - WALDecoder: Converts raw "change events" into LogicalChangeEvent objects
  - WalSender: Streams decoded events to consumers
  - LogicalReplicationConsumer: Simulates a Debezium-like consumer
  - CDCPipeline: End-to-end demonstration (write → WAL → decode → consumer → Kafka topic)

No external dependencies.
"""

from __future__ import annotations

import enum
import queue
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, Callable


# ─── Connection States ────────────────────────────────────────────────────────

class ClientState(enum.Enum):
    IDLE           = 'idle'          # Connected, no active query
    ACTIVE         = 'active'        # Executing a query
    WAITING        = 'waiting'       # Queued for a server connection
    IN_TRANSACTION = 'in_transaction' # In a multi-statement transaction


class ServerState(enum.Enum):
    IDLE      = 'idle'       # Connected to Postgres, available
    ACTIVE    = 'active'     # Executing a query for a client
    USED      = 'used'       # Session-mode: assigned to a client
    LOGIN     = 'login'      # Being established
    IDLE_IN_TXN = 'idle_in_txn'  # Server has idle transaction (pool cannot reclaim)


# ─── PgBouncer Transaction-Mode Pool ─────────────────────────────────────────

@dataclass
class ServerConnection:
    """One real connection to Postgres. Simulates a backend process."""
    conn_id:    int
    state:      ServerState = ServerState.IDLE
    client_id:  Optional[int] = None
    queries_run: int = 0


@dataclass
class ClientConnection:
    """One application connection to PgBouncer."""
    conn_id:   int
    state:     ClientState = ClientState.IDLE
    server_id: Optional[int] = None
    wait_start: Optional[float] = None
    queries_run: int = 0
    total_wait_s: float = 0.0


class PgBouncerPool:
    """
    Transaction-mode connection pool.
    Assigns a server connection for each transaction, returns it on commit/rollback.
    Simulates PgBouncer's core behavior.
    """

    def __init__(self, pool_size: int = 10, max_clients: int = 500,
                 client_timeout_s: float = 5.0):
        self.pool_size      = pool_size
        self.max_clients    = max_clients
        self.client_timeout = client_timeout_s
        self._lock          = threading.Lock()

        # Server pool
        self._servers: list[ServerConnection] = [
            ServerConnection(conn_id=i) for i in range(pool_size)]

        # Client registry
        self._clients: dict[int, ClientConnection] = {}
        self._next_client_id = 1

        # Wait queue (clients waiting for a server connection)
        self._wait_queue: list[int] = []  # client_ids in FIFO order

        # Stats
        self.stats = {
            'total_queries':    0,
            'total_wait_s':     0.0,
            'cl_waited':        0,
            'sv_alloc_errors':  0,
        }

    def connect(self) -> Optional[int]:
        """Accept a new client connection. Returns client_id or None if at capacity."""
        with self._lock:
            if len(self._clients) >= self.max_clients:
                return None
            cid = self._next_client_id
            self._next_client_id += 1
            self._clients[cid] = ClientConnection(conn_id=cid)
            return cid

    def disconnect(self, client_id: int):
        """Close a client connection; return any server connection to pool."""
        with self._lock:
            client = self._clients.pop(client_id, None)
            if client and client.server_id is not None:
                server = self._servers[client.server_id]
                server.state     = ServerState.IDLE
                server.client_id = None
                self._maybe_serve_waiting()

    def execute_transaction(self, client_id: int,
                             query_fn: Callable, duration_s: float = 0.01) -> bool:
        """
        Execute a transaction for a client:
        1. Acquire a server connection (queue if none available)
        2. Run query_fn (simulated)
        3. Release server connection back to pool
        Returns True on success, False on timeout.
        """
        # Acquire a server connection
        acquired_at = time.time()
        server_id   = None
        while server_id is None:
            with self._lock:
                server_id = self._find_idle_server()
                if server_id is not None:
                    client = self._clients[client_id]
                    client.state     = ClientState.ACTIVE
                    client.server_id = server_id
                    client.wait_start = None
                    self._servers[server_id].state     = ServerState.ACTIVE
                    self._servers[server_id].client_id = client_id
                else:
                    # No server available: update client state to WAITING
                    self._clients[client_id].state = ClientState.WAITING
                    if self._clients[client_id].wait_start is None:
                        self._clients[client_id].wait_start = time.time()
                        self.stats['cl_waited'] += 1

            if server_id is None:
                elapsed = time.time() - acquired_at
                if elapsed > self.client_timeout:
                    return False  # Timeout
                time.sleep(0.001)  # Brief yield

        wait_s = time.time() - acquired_at
        with self._lock:
            self.stats['total_wait_s'] += wait_s
            self._clients[client_id].total_wait_s += wait_s

        # Execute the transaction
        time.sleep(duration_s)  # Simulate query execution
        query_fn()

        # Release server connection
        with self._lock:
            self._servers[server_id].state     = ServerState.IDLE
            self._servers[server_id].client_id = None
            self._servers[server_id].queries_run += 1
            self._clients[client_id].state     = ClientState.IDLE
            self._clients[client_id].server_id = None
            self._clients[client_id].queries_run += 1
            self.stats['total_queries'] += 1
            self._maybe_serve_waiting()

        return True

    def _find_idle_server(self) -> Optional[int]:
        for s in self._servers:
            if s.state == ServerState.IDLE:
                return s.conn_id
        return None

    def _maybe_serve_waiting(self):
        """Notify waiting clients that a server is now available."""
        pass  # In our simulated model, clients poll; real PgBouncer uses libevent

    def show_pools(self) -> dict:
        """Equivalent to PgBouncer SHOW POOLS."""
        with self._lock:
            cl_active  = sum(1 for c in self._clients.values() if c.state == ClientState.ACTIVE)
            cl_waiting = sum(1 for c in self._clients.values() if c.state == ClientState.WAITING)
            sv_active  = sum(1 for s in self._servers if s.state == ServerState.ACTIVE)
            sv_idle    = sum(1 for s in self._servers if s.state == ServerState.IDLE)
            return {
                'cl_active':   cl_active,
                'cl_waiting':  cl_waiting,
                'cl_total':    len(self._clients),
                'sv_active':   sv_active,
                'sv_idle':     sv_idle,
                'sv_pool_size': self.pool_size,
                'avg_wait_s':  (self.stats['total_wait_s'] /
                                max(1, self.stats['total_queries'])),
                'total_queries': self.stats['total_queries'],
                'cl_waited':    self.stats['cl_waited'],
            }


# ─── Logical Replication: WAL Decoder ────────────────────────────────────────

class ChangeOp(enum.Enum):
    INSERT = 'I'
    UPDATE = 'U'
    DELETE = 'D'
    BEGIN  = 'B'
    COMMIT = 'C'


@dataclass
class LogicalChangeEvent:
    """
    Decoded logical replication event (equivalent to pgoutput/wal2json output).
    One event per row change within a transaction.
    """
    lsn:        int
    xid:        int
    op:         ChangeOp
    table:      str
    schema:     str = 'public'
    before:     Optional[dict] = None  # Old row (for UPDATE/DELETE)
    after:      Optional[dict] = None  # New row (for INSERT/UPDATE)
    ts_ms:      int = 0


@dataclass
class ReplicationSlot:
    """
    Tracks consumer position in the WAL stream.
    When the consumer falls behind, retained_wal grows.
    """
    name:                  str
    plugin:                str         # 'pgoutput', 'wal2json', etc.
    active:                bool = False
    restart_lsn:           int  = 0   # Oldest WAL we must retain
    confirmed_flush_lsn:   int  = 0   # Consumer confirmed up to this LSN
    retained_wal_bytes:    int  = 0   # WAL accumulation since restart_lsn


class WALDecoder:
    """
    Decodes raw change events into LogicalChangeEvents.
    Simulates the pgoutput/wal2json plugin in the walsender process.
    """

    def __init__(self):
        self._current_lsn = 0
        self._events:      list[LogicalChangeEvent] = []

    def write_insert(self, xid: int, table: str, row: dict) -> LogicalChangeEvent:
        self._current_lsn += random.randint(100, 500)
        evt = LogicalChangeEvent(
            lsn=self._current_lsn, xid=xid, op=ChangeOp.INSERT,
            table=table, after=row, ts_ms=int(time.time() * 1000))
        self._events.append(evt)
        return evt

    def write_update(self, xid: int, table: str,
                     old_row: dict, new_row: dict) -> LogicalChangeEvent:
        self._current_lsn += random.randint(100, 500)
        evt = LogicalChangeEvent(
            lsn=self._current_lsn, xid=xid, op=ChangeOp.UPDATE,
            table=table, before=old_row, after=new_row, ts_ms=int(time.time() * 1000))
        self._events.append(evt)
        return evt

    def write_delete(self, xid: int, table: str, old_row: dict) -> LogicalChangeEvent:
        self._current_lsn += random.randint(100, 500)
        evt = LogicalChangeEvent(
            lsn=self._current_lsn, xid=xid, op=ChangeOp.DELETE,
            table=table, before=old_row, ts_ms=int(time.time() * 1000))
        self._events.append(evt)
        return evt

    def current_lsn(self) -> int:
        return self._current_lsn

    def events_since(self, lsn: int) -> list[LogicalChangeEvent]:
        """Return all events with lsn > given lsn."""
        return [e for e in self._events if e.lsn > lsn]


class WalSender(threading.Thread):
    """
    Simulates the Postgres walsender process.
    Continuously streams decoded events to consumers.
    """

    def __init__(self, decoder: WALDecoder, slot: ReplicationSlot,
                 consumer_fn: Callable[[LogicalChangeEvent], None]):
        super().__init__(daemon=True, name=f"walsender-{slot.name}")
        self.decoder     = decoder
        self.slot        = slot
        self.consumer_fn = consumer_fn
        self._running    = True
        self.events_sent = 0

    def run(self):
        self.slot.active = True
        while self._running:
            new_events = self.decoder.events_since(self.slot.confirmed_flush_lsn)
            for evt in new_events:
                if not self._running:
                    break
                self.consumer_fn(evt)
                # After consumer acknowledges, advance the slot
                self.slot.confirmed_flush_lsn = evt.lsn
                self.slot.restart_lsn         = evt.lsn
                self.events_sent += 1
            time.sleep(0.01)

    def stop(self):
        self._running = False
        self.slot.active = False


# ─── CDC Consumer (Debezium simulation) ──────────────────────────────────────

class CDCConsumer:
    """
    Simulates a Debezium-like CDC consumer:
    Reads LogicalChangeEvents from the walsender and routes them to Kafka topics.
    """

    def __init__(self, server_name: str):
        self.server_name = server_name
        self._topics:    dict[str, list[dict]] = {}
        self.events_consumed = 0
        self._lock = threading.Lock()

    def consume(self, evt: LogicalChangeEvent):
        """Process one event: convert to Kafka message and append to topic."""
        topic = f"{self.server_name}.{evt.schema}.{evt.table}"
        message = {
            "lsn":    evt.lsn,
            "op":     evt.op.value,
            "before": evt.before,
            "after":  evt.after,
            "ts_ms":  evt.ts_ms,
            "xid":    evt.xid,
        }
        with self._lock:
            if topic not in self._topics:
                self._topics[topic] = []
            self._topics[topic].append(message)
            self.events_consumed += 1

    def topic_message_count(self, topic: str) -> int:
        with self._lock:
            return len(self._topics.get(topic, []))

    def print_topic_summary(self):
        with self._lock:
            print(f"\n  Kafka Topics (simulated):")
            for topic, messages in sorted(self._topics.items()):
                ops = {}
                for m in messages:
                    ops[m['op']] = ops.get(m['op'], 0) + 1
                print(f"    {topic}: {len(messages)} messages "
                      f"({', '.join(f'{k}:{v}' for k, v in ops.items())})")


# ─── Replication Slot Monitor ─────────────────────────────────────────────────

class SlotMonitor:
    """
    Monitors replication slot health.
    Alert when retained WAL exceeds a threshold (slot falling behind).
    """

    def __init__(self, alert_gb: float = 10.0):
        self.alert_gb = alert_gb

    def check(self, decoder: WALDecoder, slots: list[ReplicationSlot]) -> list[dict]:
        current_lsn = decoder.current_lsn()
        results = []
        for slot in slots:
            lag = max(0, current_lsn - slot.confirmed_flush_lsn)
            # Estimate WAL bytes (rough: 300 bytes per change event)
            est_wal_gb = lag * 300 / 1024 / 1024 / 1024
            status = 'ok'
            if not slot.active:
                status = 'INACTIVE — WAL ACCUMULATING'
            elif est_wal_gb > self.alert_gb:
                status = 'WARNING: WAL retention too high'
            results.append({
                'slot':         slot.name,
                'active':       slot.active,
                'lsn_lag':      lag,
                'est_wal_gb':   est_wal_gb,
                'status':       status,
            })
        return results


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Postgres Data Engineering Simulation ===\n")

    # ── Phase 1: PgBouncer Connection Pool ────────────────────────────────────
    print("─── Phase 1: PgBouncer Transaction Pool ───\n")
    pool = PgBouncerPool(pool_size=10, max_clients=200)

    # Simulate 50 concurrent application connections
    n_clients     = 50
    queries_done  = [0]
    client_ids    = [pool.connect() for _ in range(n_clients)]
    print(f"  {n_clients} client connections established to PgBouncer")
    print(f"  PgBouncer has {pool.pool_size} server connections to Postgres")

    def run_client(cid: int, n_queries: int):
        for _ in range(n_queries):
            pool.execute_transaction(
                cid,
                query_fn=lambda: None,
                duration_s=random.uniform(0.005, 0.020)
            )

    threads = []
    for cid in client_ids:
        t = threading.Thread(target=run_client, args=(cid, 5), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=10)

    stats = pool.show_pools()
    print(f"\n  Pool Stats after {stats['total_queries']} queries:")
    print(f"    cl_total:    {stats['cl_total']} (application connections)")
    print(f"    sv_pool_size:{stats['sv_pool_size']} (Postgres backends)")
    print(f"    cl_waited:   {stats['cl_waited']} clients queued for a server slot")
    print(f"    avg_wait_s:  {stats['avg_wait_s'] * 1000:.2f}ms per query")
    print(f"  Multiplexing ratio: {stats['cl_total']}:{stats['sv_pool_size']}")

    for cid in client_ids:
        pool.disconnect(cid)

    # ── Phase 2: Logical Replication — Write Events ────────────────────────────
    print("\n─── Phase 2: Logical Replication WAL Events ───\n")
    decoder  = WALDecoder()
    slot     = ReplicationSlot(name="debezium_orders", plugin="pgoutput")
    consumer = CDCConsumer(server_name="mydb-cdc")
    sender   = WalSender(decoder, slot, consumer.consume)

    sender.start()
    print("  WalSender started (simulates Postgres walsender process)")

    # Simulate application writes
    xid = 1001
    print(f"\n  Transaction {xid}: INSERT 3 orders")
    for i in range(1, 4):
        decoder.write_insert(xid, 'orders', {
            'id': i, 'customer_id': 42, 'status': 'pending',
            'amount': round(random.uniform(10, 200), 2)
        })
    time.sleep(0.05)   # Let walsender stream them

    xid = 1002
    print(f"  Transaction {xid}: UPDATE order 1 status to 'shipped'")
    decoder.write_update(xid, 'orders',
                          old_row={'id': 1, 'status': 'pending'},
                          new_row={'id': 1, 'status': 'shipped'})
    time.sleep(0.05)

    xid = 1003
    print(f"  Transaction {xid}: DELETE order 2")
    decoder.write_delete(xid, 'orders',
                          old_row={'id': 2, 'customer_id': 42, 'status': 'pending'})
    time.sleep(0.05)

    xid = 1004
    print(f"  Transaction {xid}: INSERT 2 customers")
    for i in range(100, 102):
        decoder.write_insert(xid, 'customers', {
            'id': i, 'name': f'Customer {i}', 'email': f'c{i}@example.com'
        })
    time.sleep(0.1)

    sender.stop()
    sender.join(timeout=1)

    print(f"\n  Events streamed by WalSender: {sender.events_sent}")
    print(f"  Events consumed by CDC consumer: {consumer.events_consumed}")
    consumer.print_topic_summary()

    # ── Phase 3: Replication Slot Monitoring ──────────────────────────────────
    print("\n─── Phase 3: Replication Slot Health ───\n")
    # Create an inactive slot (simulates a crashed consumer)
    dead_slot = ReplicationSlot(
        name="abandoned_consumer",
        plugin="pgoutput",
        active=False,
        restart_lsn=10,
        confirmed_flush_lsn=10
    )
    # The main slot is current
    slot.confirmed_flush_lsn = decoder.current_lsn()

    # Generate more WAL to show the abandoned slot falling behind
    for i in range(50):
        decoder.write_insert(9999, 'orders', {'id': 1000 + i, 'status': 'pending'})

    monitor = SlotMonitor(alert_gb=0.001)  # Low threshold for demo
    slot_report = monitor.check(decoder, [slot, dead_slot])
    print(f"  {'Slot':<25} {'Active':>6} {'LSN lag':>10} {'Status'}")
    print(f"  {'-'*25} {'-'*6} {'-'*10} {'-'*30}")
    for r in slot_report:
        print(f"  {r['slot']:<25} {str(r['active']):>6} {r['lsn_lag']:>10,} "
              f"{r['status']}")

    print(f"\n  Recommendation: DROP REPLICATION SLOT 'abandoned_consumer'")
    print(f"  SQL: SELECT pg_drop_replication_slot('abandoned_consumer');")

    # ── Phase 4: Pool Sizing Decision ─────────────────────────────────────────
    print("\n─── Phase 4: Connection Pool Sizing Analysis ───\n")
    print("  PgBouncer transaction-mode: required Postgres backends vs application connections")
    print()
    print(f"  {'App Connections':>18} {'Required Backends':>18} {'Overhead Ratio':>16} "
          f"{'PG RAM (backends)':>18}")
    print(f"  {'-'*18} {'-'*18} {'-'*16} {'-'*18}")

    # Model: average query time = 10ms, target max wait = 20ms
    # Required backends = app_connections × (10ms / 20ms target) at peak saturation
    # Conservative: target 20% server utilization
    for n_app in [50, 100, 250, 500, 1000, 5000]:
        avg_query_ms = 10
        # At 20% utilization: need enough servers so avg wait < 10ms
        required_backends = max(5, int(n_app * 0.05))
        ratio             = n_app / required_backends
        pg_ram_mb         = required_backends * 10
        print(f"  {n_app:>18,} {required_backends:>18} {ratio:>15.0f}× {pg_ram_mb:>16}MB")

    print(f"\n  Without PgBouncer: 5000 app connections × 10MB = 50,000MB = 49GB")
    print(f"  With PgBouncer:    250 backends        × 10MB = 2,500MB = 2.5GB")
```

---

## 6. Visual Reference

### PgBouncer Architecture

```
Application Tier                    PgBouncer               Postgres
─────────────────                   ──────────               ────────

App Server 1                        ┌──────────────────────────────────┐
  Thread 1 ─── connect(6432) ─────→ │                                  │
  Thread 2 ─── connect(6432) ─────→ │  Client Pool (up to 5,000)       │
  Thread 3 ─── connect(6432) ─────→ │  cl_active: clients executing    │
  ...                                │  cl_waiting: clients queued      │   ┌──── backend(1)
App Server 2                        │                                  ├──→ backend(2)
  Thread 1 ─── connect(6432) ─────→ │  Transaction Mode:               │   backend(3)
  Thread 2 ─── connect(6432) ─────→ │  For each TXN: assign server    │   ...
  ...                                │  After COMMIT: return server     │   backend(25)
                                     │                                  │
1,000 client connections            │  Server Pool (25 connections)    │
50 concurrent queries at any time   └──────────────────────────────────┘
→ need ≥50 server connections       50 needed at peak / 25 available
                                    → some queries queue (cl_waiting > 0)
```

### Logical Replication Data Flow

```
Application                  Postgres Primary                    Consumer
───────────                  ────────────────                    ────────

UPDATE orders                 WAL record written:
SET status='shipped'          LSN=12345: UPDATE orders
WHERE id=1;                   before:{id:1,status:'pending'}
                              after: {id:1,status:'shipped'}
                                      │
                              WAL file (pg_wal/)
                                      │
                              Replication slot:
                              restart_lsn = 12000
                              confirmed_flush_lsn = 11999 ← consumer's position
                                      │
                              walsender process:
                              reads WAL from slot's restart_lsn
                              decodes via pgoutput plugin:
                              {op:"U", table:"orders",
                               before:{id:1,status:"pending"},
                               after: {id:1,status:"shipped"}}
                                      │
                                      ├──────── Debezium connector ──────→ Kafka topic:
                                      │                                    mydb.public.orders
                                      │                                    {"op":"u","after":{...}}
                                      │
                              Consumer acknowledges LSN 12345:
                              confirmed_flush_lsn = 12345
                              Postgres can now delete WAL before 12345
```

---

## 7. Common Mistakes

**Mistake 1: Using PgBouncer session mode when transaction mode would work.** Session mode assigns one backend per application connection for the entire session lifetime — providing no multiplexing benefit. It is appropriate only for applications that use session-level features (PREPARE, SET, advisory locks, temporary tables). For most modern applications using ORMs with auto-commit, transaction mode provides full multiplexing with no behavioral change. Default to transaction mode and switch to session mode only when a specific session-level feature is required.

**Mistake 2: Creating a replication slot without monitoring its WAL retention.** A replication slot that falls behind (slow consumer, consumer crashes, consumer is intentionally paused) causes Postgres to retain all WAL since the slot's `restart_lsn`. This is unbounded — the disk will fill up until Postgres crashes. In production, `max_slot_wal_keep_size` (Postgres 13+) should be set as a safety limit. Additionally, monitor `pg_replication_slots.restart_lsn` and alert when `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` exceeds a safe threshold (e.g., 50GB).

**Mistake 3: Running dbt through PgBouncer in transaction mode.** dbt uses `SET search_path = ...` to set the search path before executing models. In transaction mode, this `SET` applies only to the current transaction's backend; the next transaction may go to a different backend where the search path is the Postgres default. This causes queries to fail or find objects in wrong schemas. Use PgBouncer session mode for dbt, or pass `search_path` in the connection options rather than via `SET`.

---

## 8. Production Failure Scenarios

### Scenario 1: PgBouncer Queue Buildup Causing Application Timeout

**Symptoms:** Under peak load, application response times spike from 20ms to 8 seconds. The `cl_waiting` counter in PgBouncer's `SHOW POOLS` is consistently > 50. Database CPU is at 40% (not saturated). Applications report `timeout expired` errors.

**Root cause:** The PgBouncer `default_pool_size = 15` is too small for the peak query rate. At peak: 200 concurrent active requests × 20ms average query time = 4 queries/ms, requiring 4 × 20ms = 80 concurrent server connections. With 15 server connections, the queue depth is 200 - 15 = 185 clients waiting, adding ≈ (185 / 15) × 20ms ≈ 247ms of queuing latency per additional client — cascading into timeout territory.

**Fix:**
```ini
# pgbouncer.ini
[pgbouncer]
# Increase pool size to match peak concurrency
default_pool_size = 50         # Was 15; target 20-30% above expected peak
max_db_connections = 100       # Hard cap on total Postgres backends
query_wait_timeout = 15        # Timeout waiting for a server connection (seconds)
client_login_timeout = 5       # Timeout for initial connection establishment

# Set per-database pool if different databases have different patterns:
[databases]
mydb = host=127.0.0.1 port=5432 dbname=mydb pool_size=50

# Reload without restart:
# psql -h pgbouncer-host -p 6432 -U pgbouncer_admin pgbouncer -c "RELOAD;"
```

### Scenario 2: Replication Slot WAL Accumulation Filling the Disk

**Symptoms:** Postgres disk usage grows from 200GB to 1TB over 3 days. `pg_replication_slots` shows `wal_retained = 800GB` for a slot named `debezium_orders`. The Debezium connector had crashed 3 days ago and was not restarted. Postgres logs show warnings about disk space.

**Root cause:** The Debezium connector crashed 3 days ago, leaving the `debezium_orders` replication slot active but unconsumed. Postgres cannot delete any WAL segment that occurred after the slot's `restart_lsn` (3 days ago) because the slot guarantees the consumer will eventually read it. 3 days × 4GB WAL/hour = 288GB of accumulated WAL, plus the actual data files.

**Fix:**
```sql
-- Step 1: Verify the problem
SELECT slot_name, active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
    restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;
-- Shows: debezium_orders | f | 800 GB | ...

-- Step 2: Restart the Debezium connector
-- (In Kafka Connect: POST /connectors/orders-cdc-connector/restart)
-- Once it reconnects, it will consume from where it left off
-- WAL retention will shrink as confirmed_flush_lsn advances

-- Step 3: If the connector cannot be restarted and disk is critically low:
-- Drop the slot (Debezium will need to reinitialize from scratch — full snapshot)
SELECT pg_drop_replication_slot('debezium_orders');
-- Restart Debezium with snapshot.mode = "initial" to re-snapshot the table

-- Step 4: Prevention — add max_slot_wal_keep_size
ALTER SYSTEM SET max_slot_wal_keep_size = '50GB';
SELECT pg_reload_conf();
-- If a slot's WAL retention exceeds 50GB, the slot is automatically invalidated
-- Debezium will detect the invalidation and restart from scratch

-- Step 5: Add monitoring alert
-- Alert when retained WAL > 10GB for any slot:
SELECT slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots
WHERE pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) > 10 * 1024^3;
```

---

## 9. Performance and Tuning

### PgBouncer Sizing Formula

```
Required pool size ≈ (peak_concurrent_queries × avg_query_time_s) + buffer

peak_concurrent_queries: max(pg_stat_activity WHERE state = 'active') at peak load
avg_query_time_s: mean_exec_time / 1000 from pg_stat_statements

Example:
  Peak active queries: 100
  Average query time: 15ms = 0.015s
  Required pool size: 100 × 0.015 = 1.5 → need at least 2 backends to handle peak
  In practice: 100 concurrent queries × 15ms = Little's Law → 1.5 server connections needed
  But add 50% buffer for spikes: pool_size = 25-50

Conservative rule: pool_size = 3 × cpu_count on the Postgres server
  (At 100% CPU utilization, queue depth stays manageable)
```

### Logical Replication Performance Tuning

```sql
-- On the publisher: ensure WAL is in logical mode and send buffer is large
ALTER SYSTEM SET wal_level = 'logical';          -- Required
ALTER SYSTEM SET wal_sender_timeout = '60s';      -- Drop idle walsender connections
ALTER SYSTEM SET wal_keep_size = '1GB';           -- Keep 1GB WAL for late-connecting consumers

-- For high-throughput CDC: tune walsender I/O
ALTER SYSTEM SET max_wal_senders = 15;            -- Support more concurrent consumers
ALTER SYSTEM SET max_replication_slots = 15;
SELECT pg_reload_conf();

-- On the subscriber (Postgres-to-Postgres replication):
ALTER SYSTEM SET max_logical_replication_workers = 8;
ALTER SYSTEM SET max_sync_workers_per_subscription = 4; -- Parallel initial sync

-- Publication management:
-- Add a table to an existing publication (no restart needed)
ALTER PUBLICATION orders_pub ADD TABLE new_table;
-- Remove a table:
ALTER PUBLICATION orders_pub DROP TABLE old_table;
-- Change to ALL TABLES:
ALTER PUBLICATION orders_pub SET FOR ALL TABLES;

-- Monitor replication lag (time-based)
SELECT
    now() - pg_last_xact_replay_timestamp() AS replication_lag_time,
    pg_wal_lsn_diff(pg_current_wal_lsn(),
        sent_lsn)   AS network_lag_bytes,
    pg_wal_lsn_diff(pg_current_wal_lsn(),
        replay_lsn) AS apply_lag_bytes,
    application_name
FROM pg_stat_replication;
```

---

## 10. Interview Q&A

**Q1: Explain PgBouncer's transaction pooling mode. What does it enable and what does it break?**

Transaction pooling assigns a Postgres backend connection to an application connection for the duration of one database transaction. The moment the application sends `COMMIT` or `ROLLBACK`, the backend is returned to the pool and can serve a different application connection's next transaction. From the application's perspective, it always communicates with the same PgBouncer address — but the actual Postgres backend changes between transactions.

The benefit is dramatic: 1,000 application connections can share 25 Postgres backends if transactions are short. At an average transaction duration of 10ms, the theoretical throughput is 25 backends × 100 transactions/second/backend = 2,500 transactions/second through 25 connections instead of 1,000.

Transaction mode breaks anything that relies on the assumption that consecutive transactions in the same application session go to the same backend. Broken features include: `PREPARE` statements (session-level, gone when the backend changes), `SET` commands (e.g., `SET search_path`) that must persist across transactions, `pg_advisory_lock` (session-level advisory locks are released when the backend changes), `LISTEN/NOTIFY` (requires a persistent connection to receive notifications), and temporary tables (session-scoped, destroyed when the backend changes). For most OLTP application code using an ORM with auto-commit, none of these features are used and transaction mode works transparently.

**Q2: What is a Postgres replication slot and what makes an inactive slot dangerous?**

A replication slot is a server-side object that tracks a consumer's position in the WAL stream. When a consumer (Debezium, a Postgres subscriber, a custom replication client) connects and begins consuming changes, it does so from a replication slot. The slot has a `restart_lsn` — the oldest WAL position the consumer might need — and a `confirmed_flush_lsn` — the position up to which the consumer has acknowledged consuming changes.

The critical invariant: Postgres never deletes a WAL segment (file in `pg_wal/`) that contains data at or after a slot's `restart_lsn`. This guarantees that a consumer that was temporarily disconnected can reconnect and catch up from exactly where it left off.

The danger: if the consumer disconnects or crashes and the slot is not dropped, the `restart_lsn` never advances. Every WAL segment written since the slot was last active accumulates on disk. On a database generating 10GB of WAL per day, a slot inactive for 30 days retains 300GB of WAL. Eventually, the disk fills up and Postgres crashes — taking down the database and all applications connected to it.

The safeguards: set `max_slot_wal_keep_size` (Postgres 13+) to automatically invalidate slots that accumulate too much WAL. Alert on `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) > 10GB` for any slot. Drop abandoned slots manually when consumers are known to be permanently offline.

---

## 11. Cross-Question Chain

**Interviewer:** You need to stream all INSERT, UPDATE, DELETE events from a Postgres `orders` table to Kafka in real time. How do you set this up?

**Candidate:** Enable logical replication on the source: set `wal_level = 'logical'` in `postgresql.conf` (requires a restart). Create a publication: `CREATE PUBLICATION orders_pub FOR TABLE orders`. Set REPLICA IDENTITY: `ALTER TABLE orders REPLICA IDENTITY DEFAULT` — this uses the primary key to identify changed rows in UPDATE/DELETE events. Create a Debezium Kafka Connect connector configured with `connector.class = io.debezium.connector.postgresql.PostgresConnector`, pointing to the Postgres primary, with `table.include.list = public.orders` and `plugin.name = pgoutput`. Debezium will create a replication slot, take an initial snapshot, then stream ongoing changes to a Kafka topic named `{server}.public.orders`. Each message contains `op` (c/u/d), `before` (old row for updates/deletes), and `after` (new row for inserts/updates).

**Interviewer:** The `orders` table has no primary key. How does this affect the setup?

**Candidate:** Without a primary key, `REPLICA IDENTITY DEFAULT` doesn't work — there's no unique identifier for the row. The options are: create a primary key (best: `ALTER TABLE orders ADD COLUMN id BIGSERIAL PRIMARY KEY` — but this is a schema change that requires coordination); use `REPLICA IDENTITY FULL` (`ALTER TABLE orders REPLICA IDENTITY FULL`) — this sends the entire old row as the update identity, which allows UPDATE/DELETE replication but at the cost of high WAL overhead (every column of every updated row goes into the WAL); or use `REPLICA IDENTITY USING INDEX` with a unique index. `REPLICA IDENTITY FULL` is the pragmatic short-term fix. The long-term fix is always to add a proper primary key. Without any REPLICA IDENTITY (the default for tables without PKs), Debezium can replicate INSERTs but not UPDATEs or DELETEs — they are silently dropped.

**Interviewer:** Three months in, you notice the Kafka topic has duplicate rows for some orders. What causes this and how do you prevent it?

**Candidate:** Duplicates in logical replication occur from at-least-once delivery. When the Debezium connector restarts (after a crash, a redeploy, a Kafka Connect worker restart), it resumes from its last committed Kafka offset. But the Postgres replication slot's `confirmed_flush_lsn` may not have advanced past the last successfully processed batch if the connector crashed before Kafka commit was acknowledged. On restart, Debezium replays changes from the slot's current position — potentially re-sending rows that were already delivered to Kafka before the crash.

Prevention: design downstream consumers to be idempotent. Use the event's LSN and XID as a composite deduplication key. In the Kafka consumer (e.g., a Flink job writing to a data warehouse), use `INSERT ... ON CONFLICT DO NOTHING` or `MERGE` with the event's primary key and LSN. Exactly-once delivery can be achieved with Kafka's transactional producer API (Debezium 2.x supports this), but it requires careful configuration and reduces throughput.

**Interviewer:** You're building a new feature that needs the last 7 days of `orders` data in a DuckDB analytical layer. What's the most efficient way to load it from Postgres?

**Candidate:** Two approaches depending on whether this is a one-time load or an ongoing sync. For a one-time load: `COPY (SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days') TO STDOUT (FORMAT CSV)` piped to DuckDB's `COPY orders FROM '/dev/stdin' (FORMAT CSV)`. For a table with a BRIN index on `created_at`, the Postgres side of this query is extremely fast — it skips 93%+ of pages (7 days out of 90 days of data). The CSV pipe throughput is typically 50-200MB/s. For 7 days of orders at 10GB of data, the load takes under 2 minutes.

For an ongoing sync (refresh every hour): use logical replication and Debezium to stream changes into Kafka, then use a Kafka-to-DuckDB sink or materialize changes in a staging table. Alternatively, use DuckDB's Postgres extension (`INSTALL postgres_scanner; LOAD postgres_scanner; ATTACH 'dbname=mydb' AS pg (TYPE POSTGRES)`) to query Postgres directly from DuckDB — though this transfers data over the network on every query rather than materializing it.

**Interviewer:** Your application's PgBouncer pool shows `cl_waiting` consistently spiking to 100+ during peak hours despite having 50 server connections. What diagnostics do you run and what are the possible fixes?

**Candidate:** First, diagnose the bottleneck source. `SHOW POOLS` in PgBouncer gives `cl_active`, `cl_waiting`, `sv_active`, `sv_idle`, `avg_wait_s`. If `sv_idle = 0` and `sv_active = 50` constantly, the pool is saturated — all 50 backends are always busy. I'd look at query duration distribution in `pg_stat_statements`: if `max_exec_time` for any query shape is seconds rather than milliseconds, a handful of slow queries are tying up backends. If `avg_exec_time` is high across the board, the Postgres server itself is a bottleneck (CPU, I/O, lock contention).

Three fixes depending on root cause: First, if slow queries are the issue — tune the queries (add indexes, fix statistics, tune `work_mem` for sorts), so each backend is free faster and the pool effectively multiplexes more. Second, if query volume genuinely exceeds capacity — scale up (more CPU cores on the Postgres server means more parallel backend throughput) or scale out (read replicas for read queries, PgBouncer directing SELECT traffic to replicas). Third, if transactions are long-running due to application logic (not database bottlenecks) — increase `default_pool_size` in PgBouncer from 50 to a value that matches the actual peak concurrency. Also set `query_wait_timeout` in PgBouncer to fail fast rather than queue indefinitely, giving the application a chance to retry or degrade gracefully.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is PgBouncer's transaction mode and what is its multiplexing benefit? | Transaction mode assigns a Postgres backend to an application connection only for the duration of one transaction. After COMMIT/ROLLBACK, the backend returns to the pool. Enables N app connections to share M backends (N >> M), reducing Postgres process overhead from O(N) to O(M). |
| 2 | What session-level Postgres features break in PgBouncer transaction mode? | `PREPARE` (prepared statements), `SET` (session variables like search_path), `pg_advisory_lock` (advisory locks), `LISTEN/NOTIFY` (requires persistent connection), temporary tables. All require the same backend across multiple transactions. |
| 3 | What is `pg_create_logical_replication_slot` and why is it needed? | Creates a server-side slot that tracks the consumer's WAL position. The slot ensures Postgres does not delete WAL segments before the consumer has processed them. Required before any logical replication consumer can connect. |
| 4 | What is `REPLICA IDENTITY` and why does it matter for CDC? | Controls what data is written to WAL for UPDATE and DELETE events to identify which row changed. DEFAULT uses the primary key (efficient); FULL sends all columns (expensive); NOTHING means UPDATEs/DELETEs cannot be replicated. Tables without a primary key need REPLICA IDENTITY FULL for full CDC. |
| 5 | What makes an inactive replication slot dangerous? | It prevents WAL deletion. Postgres retains all WAL since the slot's `restart_lsn`. An inactive slot on a high-write database accumulates gigabytes/terabytes of WAL indefinitely, eventually filling the disk and crashing Postgres. |
| 6 | What is `max_slot_wal_keep_size` and when was it added? | Added in Postgres 13. Sets a limit on how much WAL a single replication slot can retain. If a slot exceeds the limit, it is automatically invalidated — protecting the disk at the cost of requiring the consumer to reinitialize. Essential safety setting for all databases using logical replication. |
| 7 | What does a Debezium CDC event look like for a Postgres UPDATE? | JSON message on the Kafka topic: `{"op": "u", "before": {old_row_values}, "after": {new_row_values}, "ts_ms": timestamp, "source": {"db": "mydb", "table": "orders", "lsn": 12345, "txId": 5001}}`. |
| 8 | What is the `pgoutput` plugin and how does it relate to logical replication? | The native Postgres logical decoding plugin (built-in since Postgres 10). Decodes WAL records into a binary protocol understood by Postgres subscribers and Debezium. Alternatives: `wal2json` (outputs JSON) and `decoderbufs` (Protocol Buffers). `pgoutput` is preferred for Debezium for its efficiency. |
| 9 | Why does dbt sometimes break with PgBouncer transaction mode? | dbt uses `SET search_path = target_schema` before executing each model. In transaction mode, this `SET` is session-level and is not guaranteed to persist to the next transaction (which may go to a different backend). The subsequent model may fail to find objects in the expected schema. Fix: use PgBouncer session mode for dbt, or set `search_path` in the connection options. |
| 10 | What is Little's Law and how does it apply to PgBouncer pool sizing? | Little's Law: L = λW (average items in system = arrival rate × average wait time). For a connection pool: `concurrent_transactions = arrivals_per_second × avg_transaction_duration_s`. A pool serving 100 transactions/second with 15ms avg duration needs `100 × 0.015 = 1.5` backends at any moment. Add buffer for variance: 3-5× = 5-8 backends. |
| 11 | How does Debezium handle initial snapshot vs ongoing changes? | Initial snapshot: Debezium reads the current table state via a consistent snapshot (`REPEATABLE READ` transaction), emitting each row as an `op: "r"` (read) event. After the snapshot, it switches to streaming from the replication slot, emitting `op: "c"` (insert), `"u"` (update), `"d"` (delete) for ongoing changes. The slot's `restart_lsn` is set before the snapshot begins, ensuring no changes are missed. |
| 12 | What is `wal_level = logical` and what does it add compared to `replica`? | `wal_level = replica` writes enough WAL for physical streaming replication (page-level redo information). `logical` adds column-level change data — old and new values for each modified column — required by the WAL decoder to produce meaningful logical change events. Increases WAL volume by 10-30% compared to `replica` mode. |
| 13 | What does `pg_stat_replication` show and what is the key metric? | Shows active walsender connections to all consumers (physical and logical). Key metric: `replay_lag` (time-based replication lag) or `pg_wal_lsn_diff(sent_lsn, replay_lsn)` (bytes of unapplied changes on the consumer). High lag means the consumer cannot keep up with write throughput. |
| 14 | What is the PgBouncer `PAUSE` command and when is it useful? | `PAUSE` tells PgBouncer to stop routing new queries to Postgres and wait for all in-flight queries to complete. Used before a planned Postgres maintenance operation (e.g., a `ALTER TABLE` that needs an exclusive lock) — pausing PgBouncer ensures no queries are in flight during the lock window. `RESUME` restores normal routing. |
| 15 | In a CDC pipeline, what causes duplicate events and how do you handle them? | Duplicates occur from at-least-once delivery: the consumer may re-process events if it restarts before committing its offset (Kafka) and before the replication slot advances. Handling: design consumers to be idempotent (use UPSERT / INSERT ... ON CONFLICT DO NOTHING / MERGE with the row's primary key), or use the LSN as a deduplication key in the downstream system. |

---

## 13. Further Reading

- **PgBouncer documentation (pgbouncer.org):** Complete reference for all pool modes, configuration parameters, and SHOW commands.
- **Postgres documentation — "Logical Replication" chapter:** Official guide to publication/subscription, replication slots, and the pgoutput protocol.
- **Debezium documentation (debezium.io):** The Postgres connector documentation, including REPLICA IDENTITY requirements, snapshot modes, and operational guidance.
- **"Scaling PostgreSQL" blog posts (pganalyze.com):** Real-world case studies of PgBouncer deployment patterns at scale.
- **"The Internals of PostgreSQL" by Hironobu Suzuki — Chapter 11 (Streaming Replication):** Covers the WAL sender/receiver internals that underpin both physical and logical replication.

---

## 14. Lab Exercises

**Exercise 1: PgBouncer Pool Simulation**
In `postgres_data_engineering.py`, run Phase 1 with different `pool_size` values (5, 10, 25, 50) and measure `avg_wait_ms` at each pool size. Plot (conceptually) the relationship between pool size and wait time. At what pool size does wait time become negligible?

**Exercise 2: CDC Event Stream**
In `postgres_data_engineering.py`, extend Phase 2 to simulate 100 INSERT events, 50 UPDATE events, and 20 DELETE events across 3 tables (`orders`, `customers`, `products`). After the consumer processes them, verify that the Kafka topic for each table contains the correct number of messages.

**Exercise 3: Slot WAL Accumulation**
In `postgres_data_engineering.py`, simulate the abandoned slot scenario: create a slot but do not start a WalSender for it. Write 500 more events to the decoder. Call `SlotMonitor.check()` and verify the abandoned slot shows high `lsn_lag` and a status of `INACTIVE`. Implement a method `drop_slot()` on `ReplicationSlot` that sets `active=False` and resets `confirmed_flush_lsn = current_lsn`.

**Exercise 4: PgBouncer Session Mode vs Transaction Mode**
Extend `PgBouncerPool` with a `mode` parameter (`'transaction'` or `'session'`). In session mode, `execute_transaction()` should assign the server connection on first use and not return it after each transaction (only on disconnect). Simulate 10 clients each running 10 sequential queries and compare `cl_waiting` counts between modes.

---

## 15. Key Takeaways

PgBouncer and logical replication solve the two most common scaling problems data engineers face with Postgres: connection overhead and change capture. PgBouncer is not optional for applications with more than ~100 concurrent connections — it is the mechanism that makes Postgres scale to thousands of application connections without consuming proportional RAM for backend processes. Logical replication is the foundation for all serious CDC pipelines from Postgres — it provides real-time, row-level change streams without the polling gaps or trigger overhead of alternative approaches.

The operational discipline for both is equally important. For PgBouncer: monitor `cl_waiting` (queue depth indicates pool undersizing), understand which features break in transaction mode (PREPARE, SET, advisory locks), and size the pool using Little's Law rather than guessing. For logical replication: monitor replication slot WAL retention (`pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`) and alert before it fills the disk; set `max_slot_wal_keep_size` as a safety bound; ensure all replicated tables have proper REPLICA IDENTITY.

---

## 16. Connections to Other Modules

- **M52 — Postgres Architecture:** PgBouncer addresses the process-per-connection problem (M52): each Postgres backend is an OS process consuming ~10MB. PgBouncer reduces the number of actual backends from thousands to tens. The walsender background process (M52) is the process that streams logical changes to consumers.
- **M49 — Write-Ahead Log:** Logical replication reads the WAL stream (M49) and decodes it. `wal_level = 'logical'` enables column-level change data in WAL records — extending the WAL format beyond what is needed for crash recovery or physical replication.
- **M53 — Query Planning:** High connection counts affect query planning overhead — each backend's plan cache is separate. PgBouncer's statement-level caching (`server_reset_query_always`) and session-level pooling can improve prepared statement re-use.
- **M55 — Vacuuming and Bloat:** Logical replication consumers that fall behind create long-lived replication slots, which pin the WAL at old positions. This is similar to `idle in transaction` connections pinning old MVCC snapshots (M55) — in both cases, a stale consumer prevents cleanup of old data (WAL in replication, dead tuples in MVCC).
- **M47-M51 — Storage Engine Internals:** The WAL records consumed by logical replication (M49) are produced by the B-tree, heap, and MVCC write paths (M47-M51). Understanding WAL record structure from M49 makes logical replication's decode process mechanically comprehensible.

---

## 17. Anti-Patterns

**Anti-pattern: Using PgBouncer as a substitute for slow query optimization.** PgBouncer reduces connection overhead but does not make individual queries faster. Teams sometimes add PgBouncer when applications experience timeouts, but if the real cause is a missing index or a sequential scan on a large table, PgBouncer only changes which queries time out — it does not fix the underlying problem. Use `pg_stat_statements` to diagnose slow queries before reaching for PgBouncer as a fix.

**Anti-pattern: Replicating large tables with `REPLICA IDENTITY FULL`.** `REPLICA IDENTITY FULL` writes the entire old row for every UPDATE or DELETE to the WAL. On a wide table (50 columns) with heavy update traffic, this can triple or quadruple WAL volume. The increased WAL then requires more disk I/O, more replication bandwidth to consumers, and more WAL retention. For tables being replicated, always add a primary key and use `REPLICA IDENTITY DEFAULT`. If the table genuinely cannot have a primary key, consider replicating only INSERTs (append-only) and handling deletes separately (a CDC "tombstone" approach without WAL-level change capture).

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pgbouncer.ini` | PgBouncer configuration | `pool_mode`, `default_pool_size`, `max_client_conn` |
| `SHOW POOLS` (pgbouncer) | Pool health monitoring | `cl_waiting`, `sv_idle`, `avg_wait_s` |
| `SHOW STATS` (pgbouncer) | Per-database query stats | `total_query_time`, `avg_query_count` |
| `pg_replication_slots` | Slot WAL retention | `restart_lsn`, `confirmed_flush_lsn`, `active` |
| `pg_stat_replication` | Walsender connections | `replay_lag`, `sent_lsn` vs `replay_lsn` |
| `pg_logical_slot_peek_changes` | Inspect slot changes | Test decoded events without advancing the slot |
| Debezium | Postgres → Kafka CDC | Full change stream via replication slot + Kafka Connect |
| `max_slot_wal_keep_size` | Safety limit on slot WAL | Postgres 13+; prevents disk-fill from inactive slots |
| `postgres_data_engineering.py` | Pool + CDC simulation | Visualize pooling and replication event flow |

---

## 19. Glossary

**CDC (Change Data Capture):** The process of capturing row-level INSERT, UPDATE, and DELETE events from a database and streaming them to downstream consumers. Postgres implements CDC via logical replication.

**Debezium:** An open-source CDC platform using Kafka Connect plugins. The Postgres connector uses a replication slot to stream decoded WAL changes as Kafka messages.

**Logical replication:** Postgres replication mode that decodes WAL records into row-level change events (instead of raw byte-level WAL shipping). Enables selective table replication and streaming to external consumers.

**PgBouncer:** A single-process, lightweight connection pooler for Postgres. Supports session, transaction, and statement pooling modes. The standard solution for the process-per-connection overhead problem.

**pgoutput:** The built-in Postgres logical decoding plugin (since Postgres 10). Decodes WAL records into the binary Replication Protocol format consumed by Postgres subscribers and Debezium.

**Pool mode (PgBouncer):** Determines when a server connection is returned to the pool: session (on disconnect), transaction (after each COMMIT/ROLLBACK), statement (after each statement). Transaction mode provides the most multiplexing.

**Publication:** A Postgres object defining a set of tables whose changes are included in a logical replication stream. Created with `CREATE PUBLICATION`.

**REPLICA IDENTITY:** Table-level setting controlling what data is written to WAL for UPDATE/DELETE events (to identify the changed row). DEFAULT uses primary key; FULL uses all columns; NOTHING disables UPDATE/DELETE replication.

**Replication slot:** A server-side tracking object that prevents Postgres from deleting WAL needed by a consumer. Has `restart_lsn` (oldest needed WAL position) and `confirmed_flush_lsn` (consumer's acknowledged position).

**Transaction mode (PgBouncer):** Pool mode where each Postgres backend connection is assigned to an application connection only for the duration of one transaction. Enables high multiplexing (1,000 clients → 25 backends) at the cost of session-level feature compatibility.

**WAL decoder:** The Postgres subsystem that translates binary WAL records into human-readable or protocol-structured logical change events. Used by the walsender process for logical replication.

**Walsender:** The Postgres background process that reads WAL, optionally decodes it (for logical replication), and streams it to replication consumers over the replication protocol.

---

## 20. Self-Assessment

1. Explain the difference between PgBouncer's session, transaction, and statement pool modes. Which mode provides the most connection multiplexing and at what cost?
2. You have 500 application connections and a PgBouncer pool of 20 server connections. At what `avg_query_time_ms` does the pool become saturated (all 20 backends always busy)?
3. What are the three SQL commands needed to set up Postgres as a logical replication source? What configuration parameter requires a restart?
4. A `customers` table has no primary key. An engineer sets up Debezium but UPDATE events are not appearing in Kafka. Why? What are two ways to fix this?
5. In `postgres_data_engineering.py`, trace Phase 2 for the UPDATE event on order #1. What fields does the `LogicalChangeEvent` contain? How does the `WalSender` determine which events to send to the consumer?
6. An inactive replication slot on a database generating 5GB of WAL per hour has been inactive for 48 hours. How much disk is consumed? What are the two ways to deal with this?
7. You are setting up dbt to run through PgBouncer in transaction mode. dbt fails with schema resolution errors. What is the root cause and how do you fix it without changing PgBouncer to session mode?
8. What does `max_slot_wal_keep_size` do and why is it essential for databases using logical replication?

---

## 21. Module Summary

PgBouncer and logical replication are the two Postgres features that matter most for data engineering at scale. PgBouncer solves the structural limitation of Postgres's process-per-connection model: by multiplexing thousands of application connections onto tens of actual backend processes, it reduces connection overhead by an order of magnitude and allows Postgres to serve high-connection web and API workloads without the per-connection RAM and scheduling cost of thousands of OS processes. Transaction mode is the right default for most applications; session mode is necessary for applications using session-level features like prepared statements or advisory locks.

Logical replication is the foundation of real-time CDC from Postgres. By reading decoded WAL records through a replication slot, tools like Debezium can stream every INSERT, UPDATE, and DELETE as a structured event to Kafka without any application-layer instrumentation, without polling, and without trigger overhead. The key operational concerns — REPLICA IDENTITY for tables without primary keys, replication slot WAL accumulation from lagging consumers, and `max_slot_wal_keep_size` as a safety bound — are the difference between a CDC pipeline that runs reliably for years and one that fills the disk at 3 AM.

The simulation — `PgBouncerPool` with transaction-mode multiplexing, `WALDecoder` producing `LogicalChangeEvent` objects, `WalSender` streaming to `CDCConsumer` routing to Kafka topics, and `SlotMonitor` tracking WAL retention — makes both mechanisms concrete. You can observe the pool queue building under load and the WAL decoder converting raw row changes into structured CDC events.

This module completes DBE-INT-102: Postgres Architecture. Together with the five modules of DBE-INT-101: Storage Engine Internals (B-tree, LSM-tree, WAL, Buffer Pool, Columnar Storage), you now have a complete mechanical picture of how databases store, index, buffer, and replicate data — from the 8KB page to the Kafka consumer. The next course in the sequence builds on this foundation with distributed database systems, where the same storage and replication concepts operate across multiple nodes.
