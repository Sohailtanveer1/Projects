# M41: Distributed Transactions

**Course:** SYS-DST-101 — Distributed Systems Theory  
**Module:** 05 of 05  
**Global Module ID:** M41  
**Semester:** 2  
**School:** Systems (SYS)

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

Every non-trivial data engineering system eventually faces the same problem: a business operation that must atomically update multiple independent data stores. Process a payment and record it in the ledger database and publish an event to the billing topic and update the inventory count in the warehouse — all of these, or none of them. In a monolith with a single relational database, this is trivial: wrap everything in a transaction, commit or roll back. In a distributed system with separate databases, message queues, and microservices, it becomes one of the hardest problems in computer science.

Distributed transactions are the engineering discipline for solving this problem: how do you achieve atomicity (all-or-nothing) across multiple independent systems that each have their own local consistency guarantees?

The two dominant answers are Two-Phase Commit (2PC) and the Saga pattern. 2PC uses a central coordinator to achieve strong atomicity: all participants either commit or abort together. The Saga pattern abandons global atomicity and instead breaks the operation into a sequence of local transactions, with compensating transactions to undo completed steps if a later step fails. 2PC provides the strongest guarantee but is vulnerable to coordinator failure and holds locks across system boundaries. Sagas trade strong atomicity for availability and performance but require careful design of compensation logic.

Understanding both patterns, and knowing which to use when, is non-negotiable for staff-level data engineering. The systems data engineers interact with daily implement these patterns everywhere: Kafka transactions, Flink's exactly-once semantics, database-to-Kafka CDC pipelines, multi-step ETL workflows, microservice order processing pipelines. When these systems behave unexpectedly — a Kafka transaction leaves data in an abort state, a Flink job processes records twice after recovery, an order pipeline gets stuck halfway through — distributed transaction theory explains why and how to fix it.

---

## 2. Mental Model

### The Atomicity Problem

Local atomicity (within a single database) is provided by the database's WAL and lock manager: write to WAL, acquire locks, commit, release locks — all within one process, one disk, one transaction log. Either the WAL entry is durable and the transaction committed, or it isn't. There is no external coordination required.

Distributed atomicity requires coordinating multiple independent systems that each have their own atomicity primitives but no shared state. You cannot write a single WAL entry that spans two separate databases. The fundamental challenge: how do you make two systems agree to commit or abort together, when each only knows about its own local state?

### The Two Approaches

**2PC (Two-Phase Commit):** Introduces a coordinator that first asks all participants "are you ready to commit?" (Phase 1: Prepare), waits for unanimous YES, then tells all participants "commit" (Phase 2: Commit). If any participant says NO, the coordinator tells everyone to abort. This achieves global atomicity. The cost: the coordinator is a single point of failure and all participants hold locks between Phase 1 and Phase 2.

**Saga:** Breaks the global transaction into a sequence of local transactions T₁, T₂, ..., Tₙ. Each Tᵢ commits locally. If Tₖ fails, execute compensating transactions C₁, C₂, ..., Cₖ₋₁ to undo the already-committed T₁...Tₖ₋₁. There is no global atomicity: between the commit of Tᵢ and the execution of Cᵢ on failure, other readers can see the partially-applied state. The Saga achieves eventual consistency, not strong atomicity.

```
                    2PC                          Saga
Atomicity:      Strong (all-or-nothing)     Eventual (visible intermediate states)
Blocking:       Yes (locks held in Phase 1-2) No (each step commits locally)
Failure mode:   Coordinator crash → blocked  Step failure → compensation
Complexity:     Coordination protocol        Compensation logic design
Use case:       Same-type systems (DBs)      Cross-system operations
```

---

## 3. Core Concepts

### 3.1 Two-Phase Commit (2PC)

2PC achieves distributed atomicity by introducing a coordinator that orchestrates a two-phase commit protocol with all participants (the databases, services, or message brokers involved in the transaction).

**Phase 1 — Prepare (Voting Phase):**

1. The application writes to all participants (e.g., two databases).
2. The coordinator sends `PREPARE` to all participants.
3. Each participant:
   - Checks whether it can commit (validates constraints, acquires locks)
   - Writes a "prepared" log entry to its durable WAL (so it can survive a crash and still honor its vote)
   - Responds `YES` (ready to commit) or `NO` (must abort)
4. If all respond YES, the coordinator proceeds to Phase 2. If any respond NO, the coordinator sends `ABORT` to all.

**Phase 2 — Commit (Completion Phase):**

1. The coordinator writes the commit decision to its own durable log (critical: this write must be durable before sending COMMIT).
2. The coordinator sends `COMMIT` to all participants.
3. Each participant commits its local transaction and releases its locks.
4. Each participant sends acknowledgment to the coordinator.

**The crucial invariant:** Once the coordinator's commit decision is written to its durable log (end of Phase 1), the transaction will eventually commit on all participants — even if the coordinator crashes, even if participants crash. The commit decision is permanent. Any coordinator or participant that recovers will re-read its log and complete the commit.

**The blocking problem (2PC's fatal weakness):**

Between Phase 1 (each participant votes YES and holds locks) and Phase 2 (COMMIT sent and acknowledged), all participants are blocked. They hold their locks but cannot commit or abort without a coordinator decision. If the coordinator crashes during this window:

- All participants are blocked indefinitely.
- They cannot release their locks (they already voted YES — unilaterally releasing would violate the atomicity guarantee).
- They cannot commit (the coordinator might have sent ABORT to other participants).
- They cannot abort (the coordinator might have sent COMMIT to other participants).

This is the "in-doubt period" — the participants are stuck waiting for the coordinator to recover. In a system with long-running transactions or a coordinator that crashes during Phase 2, this blocking can persist for the coordinator's recovery time (seconds to minutes). All rows locked by the prepared transaction are unavailable during this period.

**3PC (Three-Phase Commit)** was designed to solve this but is rarely used in practice. It adds a PreCommit phase that allows participants to determine the commit outcome even if the coordinator crashes, but requires additional message rounds and is very difficult to implement correctly in the presence of network partitions.

**2PC implementations in data engineering:**

**XA transactions:** The standard protocol for 2PC across heterogeneous databases. Supported by most relational databases (Postgres, MySQL, Oracle, SQL Server). An XA-capable transaction manager (Atomikos, Narayana, or the JTA runtime in a Java application server) acts as the coordinator. Each database's XA resource manager is a participant.

```sql
-- Postgres XA-style 2PC:
-- Phase 1 (prepare):
BEGIN;
-- execute writes
UPDATE accounts SET balance = balance - 100 WHERE id = 'A';
PREPARE TRANSACTION 'txn-payment-42';   -- XID

-- Phase 2 (commit or rollback):
COMMIT PREPARED 'txn-payment-42';
-- OR
ROLLBACK PREPARED 'txn-payment-42';

-- Check for in-doubt prepared transactions:
SELECT gid, prepared, owner, database FROM pg_prepared_xacts;
-- Any transaction here is blocking locks — investigate and resolve
```

**Kafka transactions:** Kafka's transactional producer implements a form of 2PC across Kafka partitions. The transaction coordinator (a special Kafka broker partition) is the coordinator. Kafka partition leaders are participants. `beginTransaction()` → `send()` → `commitTransaction()` is the client API over this 2PC-like protocol (described in detail in Section 3.3).

### 3.2 Saga Pattern

The Saga pattern was proposed by Hector Garcia-Molina and Kenneth Salem in 1987 as an alternative to long-lived database transactions. A Saga is a sequence of local transactions, each of which commits immediately. If any step fails, the Saga executes compensating transactions to undo the effect of the completed steps.

**Formal definition:**
A Saga is a sequence T₁, T₂, ..., Tₙ where:
- Each Tᵢ is a local transaction that commits immediately upon completion
- For each Tᵢ, there exists a compensating transaction Cᵢ that semantically undoes the effect of Tᵢ
- If Tₖ fails, execute Cₖ₋₁, Cₖ₋₂, ..., C₁ in reverse order

**Compensating transactions vs rollback:**
A rollback is not a compensating transaction. A rollback literally undoes a transaction within the database's transactional machinery — it is as if the transaction never happened. A compensating transaction is a new transaction that semantically reverses the effect of a previously committed transaction. If T₁ added $100 to an account, C₁ subtracts $100. But between T₁ committing and C₁ executing, other reads may have seen the $100. C₁ cannot pretend T₁ never happened — it creates a new committed change.

This distinction is critical: compensating transactions mean that intermediate states are externally visible. This is the fundamental difference between a Saga and a 2PC transaction. A 2PC transaction provides isolation — no other reader sees the partial state. A Saga does not provide isolation — the committed state after each Tᵢ is immediately visible to readers.

**Saga implementation styles:**

**Choreography:** Each service reacts to events produced by the previous step and produces events for the next step. No central coordinator. Services communicate via an event bus (Kafka, EventBridge). Each service knows its role in the Saga and what to do on success or failure.

```
Saga: Create Order
  OrderService:    Receives CreateOrder command
                   Creates order (status=PENDING) → publishes OrderCreated
  InventoryService: Receives OrderCreated
                   Reserves inventory → publishes InventoryReserved
                   On failure: publishes InventoryReservationFailed
  PaymentService:  Receives InventoryReserved
                   Charges payment → publishes PaymentCompleted
                   On failure: publishes PaymentFailed
  OrderService:    Receives PaymentCompleted
                   Updates order (status=CONFIRMED) → publishes OrderConfirmed
                   Receives PaymentFailed
                   Publishes OrderCompensate → triggers compensation
```

**Orchestration:** A central saga orchestrator (a stateful service) explicitly commands each participant, tracks state, and decides when to compensate. More complex to build but easier to reason about (one place to understand the Saga's full state).

```
SagaOrchestrator:
  State: STARTED
  Step 1: Send ReserveInventory to InventoryService
          Await InventoryReserved or InventoryReservationFailed
  
  On InventoryReserved:
    State: INVENTORY_RESERVED
    Step 2: Send ChargePayment to PaymentService
            Await PaymentCompleted or PaymentFailed
  
  On PaymentFailed:
    State: COMPENSATING
    Compensation Step 1: Send ReleaseInventory to InventoryService
                         Await InventoryReleased
    State: COMPENSATED
  
  On PaymentCompleted:
    State: COMPLETED
    Step 3: Send ConfirmOrder to OrderService
```

**Properties of well-designed compensating transactions:**

**Idempotent:** Running C₁ twice must have the same effect as running it once. If the compensation fails and is retried, the result must be the same.

**Commutative** (ideally): The order in which compensation steps execute should not matter for the final state. This simplifies recovery.

**Guaranteed to succeed:** Compensation must not fail. If C₁ can fail, you have a Saga that is partially compensated — an inconsistent state with no automatic recovery path. Compensations should retry indefinitely until they succeed, or be designed as infallible operations (adding a negative entry to a ledger rather than reversing a debit, for example).

### 3.3 Kafka Transactions (Exactly-Once Semantics)

Kafka's transactional producer implements exactly-once semantics (EOS) for produce operations across multiple partitions. It solves a specific class of distributed transaction: ensuring that a set of Kafka messages is either all visible to consumers or none are visible.

**The problem without transactions:**

A stream processing application reads from input topic, processes, and writes to output topic. With at-least-once semantics: if the application crashes after writing to the output but before committing the input offset, on recovery it re-reads the input and writes duplicates to the output. With at-most-once semantics: if it commits the input offset first and then crashes before writing the output, the record is lost.

**Kafka transaction architecture:**

- **Transaction coordinator:** A special Kafka partition that manages transaction state. Each transactional producer is assigned to a coordinator based on its `transactional.id`.
- **Transaction log:** The `__transaction_state` internal topic. Records transaction lifecycle events (BEGIN, PREPARE_COMMIT, COMMIT, ABORT).
- **Producer ID (PID) + epoch:** Identifies the producer instance. The epoch prevents old zombie producers from interfering.

**Transaction lifecycle:**

```
producer.initTransactions()      → registers transactional.id with coordinator
                                   (fences any previous producer with same transactional.id)
producer.beginTransaction()      → marks start of transaction
producer.send(record1)           → sends to partition leader (not yet visible to consumers)
producer.send(record2)           → sends to another partition
producer.sendOffsetsToTransaction(offsets, consumerGroup)
                                 → atomically commits consumer group offsets
                                 → this is the 2PC-style phase: coordinator votes with
                                   both the output partition leaders and offset partitions
producer.commitTransaction()     → coordinator writes COMMIT to transaction log
                                 → all partition leaders receive EndTransactionMarker
                                 → messages become visible to read_committed consumers
```

**read_committed vs read_uncommitted:**

- `isolation.level=read_uncommitted` (default): consumers see all messages, including those from in-progress or aborted transactions. Gives lower latency but may deliver duplicates or aborted records.
- `isolation.level=read_committed`: consumers see only committed transactional messages. The consumer holds back messages from open transactions (LSO — Last Stable Offset — is the high-water mark up to which all transactions are committed or aborted).

**LSO and consumer lag:** A long-running open transaction causes the LSO to stall. `read_committed` consumers cannot advance past the LSO. If the transactional producer is slow or dead (open transaction for a long time), consumers appear to have growing lag even though messages are in the topic. This is the "stuck LSO" problem. Monitor `LastStableOffset - LogEndOffset` for topics with transactional producers.

**Exactly-once stream processing (Flink with Kafka):**

```
Kafka (input) → Flink job → Kafka (output)

Flink's EOS uses Kafka transactions:
1. Flink checkpoint begins (every checkpoint.interval ms)
2. Flink pauses consuming input
3. Flink opens a Kafka transaction for output
4. Flink flushes buffered output records
5. Flink pre-commits the transaction (Kafka PREPARE_COMMIT analog)
6. Flink writes checkpoint state to state backend (RocksDB/HDFS)
7. Checkpoint completes
8. Flink commits the Kafka transaction (output records become visible)
9. Flink commits the input Kafka offset (via sendOffsetsToTransaction)

If Flink crashes before step 8:
  On recovery: Flink aborts the in-progress Kafka transaction (output invisible)
  Flink replays from last checkpoint → re-processes → exactly-once output
```

### 3.4 Outbox Pattern

The outbox pattern solves a common distributed transaction problem: how do you atomically update a database and publish a message to a message broker, when you can't do a 2PC between them?

The naive approach — write to database, then publish to Kafka — has a gap: if the process crashes after the database write but before the Kafka produce, the event is lost. If it publishes to Kafka first and then crashes before the database write, the event is published for an operation that never completed.

**The outbox solution:**

1. Within the database transaction that makes the business change, also insert a row into an "outbox" table describing the event to be published.
2. Commit the database transaction. Now the business change and the outbox entry are atomically committed together in the database.
3. A separate process (the outbox publisher, often implemented with Debezium CDC) reads new rows from the outbox table and publishes them to Kafka.
4. After successful Kafka publication, mark the outbox row as processed (or delete it).

```sql
-- Step 1: Business logic + outbox in one transaction
BEGIN;
  UPDATE accounts SET balance = balance - 100 WHERE id = 'acct-A';
  UPDATE accounts SET balance = balance + 100 WHERE id = 'acct-B';
  INSERT INTO outbox (id, aggregate_id, event_type, payload, created_at)
  VALUES (
    gen_random_uuid(),
    'acct-A',
    'MoneyTransferred',
    '{"from":"acct-A","to":"acct-B","amount":100}',
    NOW()
  );
COMMIT;
-- The business change and the event are now atomically in the DB.
-- The outbox publisher picks up the row and publishes to Kafka.
```

**Properties:**
- At-least-once delivery (the publisher may retry if it fails after publishing but before marking processed)
- The Kafka consumer must be idempotent (handle duplicate delivery)
- Debezium CDC on the outbox table provides reliable, low-latency CDC-based outbox publishing

**Why not use XA/2PC here?** Most message brokers do not support XA. Kafka's transactional producer is an alternative but requires the application to be a Kafka consumer that reads-process-writes within a transaction boundary. The outbox pattern works for any database + any message broker combination without requiring the broker to support 2PC.

### 3.5 Saga vs 2PC: Decision Framework

**Use 2PC when:**
- All participants support XA or a compatible 2PC protocol (homogeneous database tier)
- Transactions are short-lived (milliseconds to seconds)
- Strong isolation is required — intermediate states must never be visible
- The number of participants is small (2-3 databases maximum)
- The coordinator is itself highly available

**Use Saga when:**
- Participants are heterogeneous (databases, message queues, HTTP services)
- Transactions are long-lived (seconds to minutes)
- Participants do not support 2PC/XA
- High availability is required (no blocking on coordinator failure)
- Intermediate state visibility is acceptable (or can be managed via status flags)

**Use the Outbox Pattern when:**
- You need to atomically update a database and publish a message to a broker
- The broker does not support XA
- You want at-least-once delivery with a separate idempotent consumer

**The microservices rule:** Never use 2PC across microservice boundaries. Each microservice owns its database; 2PC between their databases would create coupling at the data layer, defeating the purpose of microservice isolation. Use Saga or Outbox for cross-service coordination.

### 3.6 Saga Failure Modes and Recovery

**Countermeasures for Saga isolation issues:**

Since Saga steps commit locally and are visible to other readers before the Saga completes, applications must handle seeing partial Saga states:

**Semantic lock:** The first Saga step sets a flag on the resource (e.g., `order.status = PENDING`). Other readers that see `PENDING` know the order is mid-Saga and must not make conflicting changes. The flag is cleared when the Saga completes or compensates.

**Commutative updates:** Design Saga steps so their effects commute — the order of execution doesn't matter. If deducting $100 and adding $100 can happen in any order and produce the same final result, the Saga can execute them in any order, simplifying recovery.

**Pessimistic view:** At each step, assume the Saga will need to compensate. Report the order as PENDING (not CONFIRMED) until the entire Saga completes. This is the standard UX pattern: "Your order is being processed" rather than "Your order is confirmed" during Saga execution.

**Re-read value:** Before a Saga step that reads and then conditionally updates, re-read the value with a version number or CAS (compare-and-swap). This prevents a lost-update anomaly where two concurrent Sagas both read the same value and both apply their updates, double-counting.

**Types of Saga failures and recovery:**

**Forward recovery (continue):** If a step fails transiently (timeout, temporary unavailability), retry it. Use exponential backoff. Most Saga failures should be retried before triggering compensation — compensation is the last resort.

**Backward recovery (compensate):** If a step fails permanently (business rule violation, resource not found), execute compensation in reverse order. Compensation must be idempotent — if the compensation step itself fails and is retried, the final state must be the same.

**Saga state machine:** Always persist the Saga's current state durably. If the Saga orchestrator crashes, on recovery it must be able to determine which steps completed and resume from the right point. A Saga state machine that is stored only in memory will lose its position on crash, potentially re-executing completed steps (duplicates) or skipping compensation.

---

## 4. Hands-On Walkthrough

### 4.1 Postgres Prepared Transactions (2PC)

```sql
-- ── On connection 1 (coordinator role) ──────────────────────────────────

-- Begin a distributed transaction that spans two operations
-- In a real 2PC, these would be two separate databases/connections
-- Here we demonstrate the Postgres PREPARE TRANSACTION syntax

BEGIN;

-- Simulate participant A's work
UPDATE accounts 
SET balance = balance - 100, updated_at = NOW()
WHERE account_id = 'acct-001'
  AND balance >= 100;

-- Verify row was affected (part of Phase 1 validation)
-- If no rows affected, we'd ROLLBACK here instead
DO $$
BEGIN
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Insufficient balance';
  END IF;
END $$;

-- Phase 1: Prepare — this persists the transaction state
-- The transaction is now "in doubt" — it holds locks but is not committed
PREPARE TRANSACTION 'transfer-txn-001';
-- Connection is now free (transaction is detached from connection)

-- ── Coordinator decides: all participants prepared? Then commit ──────────

-- Phase 2: Commit (or rollback)
COMMIT PREPARED 'transfer-txn-001';
-- All locks released, changes visible

-- To abort instead:
-- ROLLBACK PREPARED 'transfer-txn-001';

-- ── Checking for stuck prepared transactions ────────────────────────────
-- ANY row here means a transaction is holding locks and waiting
-- This is a critical operational concern — these block VACUUM and DDL

SELECT 
    gid,                        -- Transaction ID (our string key)
    prepared,                   -- When it was prepared
    owner,                      -- DB user who prepared it
    database,                   -- Database name
    age(NOW(), prepared) AS age -- How long it's been in-doubt
FROM pg_prepared_xacts
ORDER BY prepared ASC;          -- Oldest first = most dangerous

-- Clean up an orphaned prepared transaction:
-- ROLLBACK PREPARED 'transfer-txn-001';
-- (Do this only after confirming the coordinator has decided to abort)
```

### 4.2 Kafka Transactions: Producer to Multiple Partitions

```python
from confluent_kafka import Producer, KafkaException

conf = {
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'payment-processor-1',   # Unique per producer instance
    'enable.idempotence': True,                   # Required for transactions
}
producer = Producer(conf)

# Initialize transactions (registers with transaction coordinator)
# Fences any previous producer with same transactional.id
producer.init_transactions()

try:
    producer.begin_transaction()

    # Write to multiple partitions atomically
    producer.produce('payments', key='pay-001',
                     value='{"amount": 100, "from": "A", "to": "B"}')
    producer.produce('audit-log', key='pay-001',
                     value='{"event": "payment_initiated", "id": "pay-001"}')

    # If this is a read-process-write Kafka stream:
    # Commit consumer offsets atomically with the output records
    # (prevents re-processing on recovery → exactly-once)
    consumer_offsets = {
        TopicPartition('payments-input', 0): OffsetAndMetadata(12345 + 1)
    }
    producer.send_offsets_to_transaction(consumer_offsets, 'my-consumer-group')

    # Commit: all records become visible to read_committed consumers atomically
    producer.commit_transaction()
    print("Transaction committed")

except KafkaException as e:
    print(f"Transaction failed: {e}")
    producer.abort_transaction()
    # All produced records are invisible to read_committed consumers
```

```bash
# Verify transactions are being committed/aborted correctly
# Check for stuck/aborted transactions in a Kafka topic

# Consumer with read_committed isolation:
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic payments \
  --from-beginning \
  --property isolation.level=read_committed

# vs read_uncommitted (sees in-flight transactions too):
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic payments \
  --from-beginning \
  --property isolation.level=read_uncommitted

# Check Last Stable Offset (LSO) vs Log End Offset (LEO)
# Large gap = stuck open transaction blocking read_committed consumers
kafka-log-dirs.sh --bootstrap-server localhost:9092 \
  --topic-list payments --describe \
  | python3 -c "
import json, sys
for line in sys.stdin:
    if line.startswith('{'):
        data = json.loads(line)
        for broker in data.get('brokers', []):
            for log in broker.get('logDirs', []):
                for partition in log.get('partitions', []):
                    lso = partition.get('offsetLag', 0)
                    if lso > 0:
                        print(f\"  STUCK LSO: {partition['partition']} lag={lso}\")
"
```

### 4.3 Implementing Outbox with Debezium

```sql
-- Schema: outbox table for transactional outbox pattern
CREATE TABLE outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(255) NOT NULL,  -- 'Order', 'Payment', etc.
    aggregate_id    VARCHAR(255) NOT NULL,  -- Business entity ID
    event_type      VARCHAR(255) NOT NULL,  -- 'OrderCreated', 'PaymentProcessed'
    payload         JSONB NOT NULL,         -- Event payload
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Optional: track delivery status for manual debugging
    delivered_at    TIMESTAMPTZ,
    status          VARCHAR(50) DEFAULT 'PENDING'  -- PENDING, DELIVERED
);

-- Index for Debezium CDC polling efficiency
CREATE INDEX outbox_status_created_idx ON outbox(status, created_at)
WHERE status = 'PENDING';

-- Usage: atomic business operation + outbox insert
BEGIN;
  -- Business operation
  INSERT INTO orders (id, user_id, total, status)
  VALUES ('ord-123', 'usr-456', 49.99, 'PENDING');

  -- Outbox entry (same transaction)
  INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
  VALUES (
    'Order',
    'ord-123',
    'OrderCreated',
    '{"orderId": "ord-123", "userId": "usr-456", "total": 49.99}'::jsonb
  );
COMMIT;
-- Both committed atomically. Debezium picks up the outbox row via CDC
-- and publishes to Kafka topic: outbox.Order.OrderCreated
```

```yaml
# Debezium connector config for outbox pattern
# (PostgreSQL source connector with outbox event router SMT)
{
  "name": "outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.dbname": "orders_db",
    "database.user": "debezium",
    "database.password": "...",
    "table.include.list": "public.outbox",
    "plugin.name": "pgoutput",
    "publication.name": "outbox_publication",
    
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.by.field": "aggregate_type",
    "transforms.outbox.route.topic.replacement": "outbox.${routedByValue}",
    "transforms.outbox.table.field.event.key": "aggregate_id",
    "transforms.outbox.table.field.event.type": "event_type",
    "transforms.outbox.table.field.event.payload": "payload"
  }
}
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
distributed_transactions.py

Implementations and simulations of:
  - Two-Phase Commit (2PC) coordinator and participants
  - Saga orchestrator with compensation
  - Outbox pattern publisher
  - Exactly-once delivery tracker

No external dependencies.
"""

import time
import uuid
import threading
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable


# ─── Two-Phase Commit ────────────────────────────────────────────────────────────

class PrepareResult(Enum):
    YES = "YES"
    NO  = "NO"


class TwoPCOutcome(Enum):
    COMMITTED = "COMMITTED"
    ABORTED   = "ABORTED"
    UNKNOWN   = "UNKNOWN"   # Coordinator crashed — in-doubt


@dataclass
class TwoPCParticipant:
    """
    Simulates a 2PC participant (e.g., a database involved in the transaction).
    """
    participant_id: str
    fail_on_prepare: bool = False    # Simulate prepare failure
    fail_on_commit: bool = False     # Simulate commit failure (partial)
    _prepared: bool = False
    _committed: bool = False
    _data: dict = field(default_factory=dict)
    _pending_data: dict = field(default_factory=dict)

    def write(self, key: str, value):
        """Buffer a write (not yet committed)."""
        self._pending_data[key] = value

    def prepare(self, transaction_id: str) -> PrepareResult:
        """
        Phase 1: Check if we can commit. If yes, durably log the prepare state
        and hold all locks. We are now committed to committing or aborting
        on coordinator's direction.
        """
        if self.fail_on_prepare:
            print(f"  [{self.participant_id}] PREPARE FAILED (simulated failure)")
            return PrepareResult.NO

        # In reality: write to WAL, acquire locks
        self._prepared = True
        print(f"  [{self.participant_id}] PREPARED ✓ (holding locks for txn {transaction_id[:8]})")
        return PrepareResult.YES

    def commit(self, transaction_id: str) -> bool:
        """Phase 2: Commit the prepared transaction. Release locks."""
        if not self._prepared:
            raise RuntimeError(f"{self.participant_id}: commit called without prepare")
        if self.fail_on_commit:
            print(f"  [{self.participant_id}] COMMIT FAILED (simulated failure — "
                  f"coordinator will retry)")
            return False

        # Apply buffered writes, release locks
        self._data.update(self._pending_data)
        self._pending_data.clear()
        self._prepared = False
        self._committed = True
        print(f"  [{self.participant_id}] COMMITTED ✓")
        return True

    def rollback(self, transaction_id: str):
        """Phase 1 or 2: Rollback — discard buffered writes, release locks."""
        self._pending_data.clear()
        self._prepared = False
        print(f"  [{self.participant_id}] ROLLED BACK")

    def read(self, key: str):
        """Read committed data."""
        return self._data.get(key)


class TwoPCCoordinator:
    """
    2PC coordinator. Orchestrates prepare → commit/abort across all participants.
    
    Critical: the coordinator's decision must be written to durable storage
    before sending COMMIT to participants. Here we use a simple in-memory
    decision log (in reality: WAL or distributed KV store).
    """

    def __init__(self, coordinator_id: str):
        self.coordinator_id = coordinator_id
        self._decision_log: dict[str, TwoPCOutcome] = {}  # txn_id → outcome
        self._lock = threading.Lock()

    def execute(self, participants: list[TwoPCParticipant],
                writes: dict[str, dict]) -> TwoPCOutcome:
        """
        Execute a distributed transaction across participants.
        writes: {participant_id: {key: value}} mapping writes to each participant.
        """
        transaction_id = str(uuid.uuid4())
        print(f"\n[Coordinator] Starting transaction {transaction_id[:8]}")

        # Apply pending writes to each participant
        for participant in participants:
            for key, value in writes.get(participant.participant_id, {}).items():
                participant.write(key, value)

        # ── Phase 1: Prepare ──────────────────────────────────────────────────
        print(f"[Coordinator] Phase 1: Sending PREPARE to all participants")
        votes: dict[str, PrepareResult] = {}
        for p in participants:
            votes[p.participant_id] = p.prepare(transaction_id)

        all_yes = all(v == PrepareResult.YES for v in votes.values())

        # ── Coordinator decision ──────────────────────────────────────────────
        # CRITICAL: Write the decision to durable log BEFORE sending Phase 2.
        # If we crash after this write, on recovery we re-read the decision
        # and complete Phase 2. Without this write, a crash leaves participants
        # in-doubt indefinitely.
        if all_yes:
            with self._lock:
                self._decision_log[transaction_id] = TwoPCOutcome.COMMITTED
            print(f"[Coordinator] Decision: COMMIT (logged durably)")
        else:
            with self._lock:
                self._decision_log[transaction_id] = TwoPCOutcome.ABORTED
            print(f"[Coordinator] Decision: ABORT (one or more participants voted NO)")

        # ── Phase 2: Commit or Abort ──────────────────────────────────────────
        print(f"[Coordinator] Phase 2: Sending "
              f"{'COMMIT' if all_yes else 'ABORT'} to all participants")

        if all_yes:
            # Retry commit until all participants acknowledge
            # (In reality: retry loop with backoff for failed commits)
            for p in participants:
                success = False
                for attempt in range(3):
                    if p.commit(transaction_id):
                        success = True
                        break
                    print(f"  [Coordinator] Retrying commit on {p.participant_id} "
                          f"(attempt {attempt + 2})")
                    time.sleep(0.1)
                if not success:
                    print(f"  [Coordinator] WARNING: {p.participant_id} failed to commit "
                          f"after retries — manual intervention required")
        else:
            for p in participants:
                p.rollback(transaction_id)

        decision = self._decision_log[transaction_id]
        print(f"[Coordinator] Transaction {transaction_id[:8]} outcome: {decision.value}")
        return decision

    def recover(self, transaction_id: str,
                participants: list[TwoPCParticipant]) -> TwoPCOutcome:
        """
        Recover from a coordinator crash: re-read the decision log and
        complete any in-progress transactions.
        """
        decision = self._decision_log.get(transaction_id, TwoPCOutcome.UNKNOWN)
        if decision == TwoPCOutcome.COMMITTED:
            print(f"[Coordinator recovery] Replaying COMMIT for {transaction_id[:8]}")
            for p in participants:
                p.commit(transaction_id)
        elif decision == TwoPCOutcome.ABORTED:
            print(f"[Coordinator recovery] Replaying ABORT for {transaction_id[:8]}")
            for p in participants:
                p.rollback(transaction_id)
        else:
            print(f"[Coordinator recovery] No decision found for {transaction_id[:8]} "
                  f"— transaction is in-doubt (coordinator crashed before deciding)")
        return decision


# ─── Saga Orchestrator ────────────────────────────────────────────────────────────

class SagaStepStatus(Enum):
    PENDING     = "PENDING"
    COMPLETED   = "COMPLETED"
    FAILED      = "FAILED"
    COMPENSATED = "COMPENSATED"


@dataclass
class SagaStep:
    """A single step in a Saga, with its compensating transaction."""
    step_id: str
    name: str
    action: Callable[[], bool]            # Returns True on success
    compensate: Callable[[], bool]        # Returns True on success
    status: SagaStepStatus = SagaStepStatus.PENDING
    result: Optional[dict] = None


class SagaOrchestrator:
    """
    Orchestrated Saga: central coordinator that executes steps and manages
    compensation on failure.
    
    Key property: Saga state is durably persisted at each step
    (simulated here with an in-memory log).
    """

    def __init__(self, saga_id: str):
        self.saga_id = saga_id
        self.steps: list[SagaStep] = []
        self._log: list[dict] = []   # Durable state log (in-memory simulation)
        self.completed = False
        self.compensated = False

    def add_step(self, name: str, action: Callable, compensate: Callable,
                 step_id: Optional[str] = None) -> 'SagaOrchestrator':
        """Add a step to the saga. Returns self for chaining."""
        step = SagaStep(
            step_id=step_id or str(uuid.uuid4())[:8],
            name=name,
            action=action,
            compensate=compensate,
        )
        self.steps.append(step)
        return self

    def _persist_state(self):
        """Persist current step states (in production: write to DB or event store)."""
        snapshot = {
            'saga_id': self.saga_id,
            'timestamp': time.time(),
            'steps': [
                {'id': s.step_id, 'name': s.name, 'status': s.status.value}
                for s in self.steps
            ],
            'completed': self.completed,
            'compensated': self.compensated,
        }
        self._log.append(snapshot)

    def execute(self) -> bool:
        """
        Execute the saga. Returns True if all steps succeed, False if compensated.
        """
        print(f"\n[Saga {self.saga_id[:8]}] Starting execution")
        completed_steps: list[SagaStep] = []

        for step in self.steps:
            print(f"  [Saga] Executing step: {step.name}")

            # Retry transient failures before compensating
            success = False
            for attempt in range(3):
                try:
                    success = step.action()
                    if success:
                        break
                except Exception as e:
                    print(f"  [Saga] Step {step.name} exception (attempt {attempt+1}): {e}")
                if attempt < 2:
                    time.sleep(0.1 * (2 ** attempt))

            if success:
                step.status = SagaStepStatus.COMPLETED
                completed_steps.append(step)
                self._persist_state()
                print(f"  [Saga] ✓ {step.name} completed")
            else:
                step.status = SagaStepStatus.FAILED
                self._persist_state()
                print(f"  [Saga] ✗ {step.name} failed — starting compensation")
                self._compensate(completed_steps)
                return False

        self.completed = True
        self._persist_state()
        print(f"[Saga {self.saga_id[:8]}] ✓ All steps completed")
        return True

    def _compensate(self, completed_steps: list[SagaStep]):
        """Execute compensating transactions in reverse order."""
        print(f"  [Saga] Compensating {len(completed_steps)} completed step(s)...")
        for step in reversed(completed_steps):
            print(f"  [Saga] Compensating: {step.name}")

            # Compensation must eventually succeed — retry indefinitely
            for attempt in range(5):
                try:
                    if step.compensate():
                        step.status = SagaStepStatus.COMPENSATED
                        self._persist_state()
                        print(f"  [Saga] ✓ Compensated: {step.name}")
                        break
                except Exception as e:
                    print(f"  [Saga] Compensation attempt {attempt+1} failed: {e}")
                time.sleep(0.1 * (2 ** attempt))
            else:
                print(f"  [Saga] CRITICAL: Compensation for {step.name} failed after "
                      f"5 attempts — requires manual intervention")

        self.compensated = True
        self._persist_state()

    def get_state_summary(self) -> dict:
        return {
            'saga_id': self.saga_id,
            'completed': self.completed,
            'compensated': self.compensated,
            'steps': [
                {'name': s.name, 'status': s.status.value}
                for s in self.steps
            ]
        }


# ─── Outbox Pattern ──────────────────────────────────────────────────────────────

@dataclass
class OutboxEntry:
    id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: dict
    created_at: float
    delivered: bool = False


class OutboxPublisher:
    """
    Simulates the outbox pattern:
    1. Business logic writes to DB + outbox atomically
    2. Publisher polls outbox and sends to message broker
    3. Marks delivered (or deletes)

    Key property: at-least-once delivery (publisher may retry on failure)
    Consumer must be idempotent.
    """

    def __init__(self):
        self._db: dict = {}                    # Simulated business data store
        self._outbox: list[OutboxEntry] = []   # Outbox table
        self._published: list[OutboxEntry] = []  # Simulated Kafka topic
        self._lock = threading.Lock()

    def write_with_outbox(self, business_key: str, business_value: dict,
                           aggregate_type: str, aggregate_id: str,
                           event_type: str, event_payload: dict) -> str:
        """
        Atomically writes business data and an outbox entry.
        In production: both in a single DB transaction.
        """
        with self._lock:
            # Simulate database transaction (both writes are atomic)
            self._db[business_key] = business_value
            entry = OutboxEntry(
                id=str(uuid.uuid4()),
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                event_type=event_type,
                payload=event_payload,
                created_at=time.time(),
            )
            self._outbox.append(entry)
            print(f"  [Outbox] Wrote business data + outbox entry "
                  f"(event_type={event_type})")
            return entry.id

    def poll_and_publish(self, fail_rate: float = 0.0):
        """
        Poll outbox for undelivered entries and publish to message broker.
        fail_rate: simulate publish failure for testing at-least-once behavior.
        """
        with self._lock:
            pending = [e for e in self._outbox if not e.delivered]

        for entry in pending:
            # Simulate broker publish (may fail)
            if random.random() < fail_rate:
                print(f"  [Outbox] Publish failed for {entry.id[:8]} "
                      f"— will retry (at-least-once)")
                continue

            with self._lock:
                # Publish to broker
                self._published.append(entry)
                # Mark as delivered in outbox (idempotent: safe to re-mark)
                entry.delivered = True
            print(f"  [Outbox] Published: {entry.event_type} "
                  f"(aggregate={entry.aggregate_id})")

    def published_events(self) -> list[OutboxEntry]:
        return list(self._published)

    def pending_entries(self) -> list[OutboxEntry]:
        return [e for e in self._outbox if not e.delivered]


# ─── Exactly-Once Delivery Tracker ──────────────────────────────────────────────

class ExactlyOnceTracker:
    """
    Idempotency key-based deduplication for at-least-once consumers.
    Consumers receiving duplicate messages check this tracker before processing.
    """

    def __init__(self, dedup_window_seconds: float = 3600.0):
        self._processed: dict[str, float] = {}    # idempotency_key → timestamp
        self._dedup_window = dedup_window_seconds
        self._lock = threading.Lock()

    def is_duplicate(self, idempotency_key: str) -> bool:
        """Return True if this key has been processed within the dedup window."""
        with self._lock:
            self._clean_expired()
            return idempotency_key in self._processed

    def mark_processed(self, idempotency_key: str):
        """Mark a key as processed."""
        with self._lock:
            self._processed[idempotency_key] = time.time()

    def _clean_expired(self):
        """Remove entries older than the dedup window."""
        cutoff = time.time() - self._dedup_window
        expired = [k for k, t in self._processed.items() if t < cutoff]
        for k in expired:
            del self._processed[k]

    def process_once(self, idempotency_key: str,
                     fn: Callable, *args, **kwargs):
        """Execute fn exactly once for a given key. Skip duplicates."""
        if self.is_duplicate(idempotency_key):
            print(f"  [EOS] Duplicate detected for key {idempotency_key[:8]} — skipping")
            return None
        result = fn(*args, **kwargs)
        self.mark_processed(idempotency_key)
        return result


# ─── Demo ─────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Distributed Transactions Demo ===\n")

    # ── 1. 2PC: Success path ────────────────────────────────────────────────────
    print("─── 1. Two-Phase Commit: Success Path ───")
    coordinator = TwoPCCoordinator("coord-1")
    db_a = TwoPCParticipant("database-A")
    db_b = TwoPCParticipant("database-B")

    outcome = coordinator.execute(
        participants=[db_a, db_b],
        writes={
            "database-A": {"account_balance": 900},   # Debit 100
            "database-B": {"account_balance": 1100},  # Credit 100
        }
    )
    print(f"  Result: {outcome.value}")
    print(f"  DB-A balance: {db_a.read('account_balance')}")
    print(f"  DB-B balance: {db_b.read('account_balance')}")

    # ── 2. 2PC: Participant votes NO (insufficient funds) ─────────────────────
    print("\n─── 2. Two-Phase Commit: Participant Votes NO ───")
    db_c = TwoPCParticipant("database-C", fail_on_prepare=True)
    db_d = TwoPCParticipant("database-D")

    outcome = coordinator.execute(
        participants=[db_c, db_d],
        writes={"database-C": {"account_balance": -100},
                "database-D": {"account_balance": 200}}
    )
    print(f"  Result: {outcome.value}")

    # ── 3. Saga: Success path ────────────────────────────────────────────────────
    print("\n─── 3. Saga: Successful Order Placement ───")

    # Simulate state that steps read/write
    state = {'inventory': 10, 'payment_charged': False, 'order_confirmed': False}

    saga = SagaOrchestrator("order-saga-001")
    saga.add_step(
        name="Reserve Inventory",
        action=lambda: (state.update({'inventory': state['inventory'] - 1}) or True),
        compensate=lambda: (state.update({'inventory': state['inventory'] + 1}) or True),
    ).add_step(
        name="Charge Payment",
        action=lambda: (state.update({'payment_charged': True}) or True),
        compensate=lambda: (state.update({'payment_charged': False}) or True),
    ).add_step(
        name="Confirm Order",
        action=lambda: (state.update({'order_confirmed': True}) or True),
        compensate=lambda: (state.update({'order_confirmed': False}) or True),
    )

    success = saga.execute()
    print(f"  Final state: {state}")
    print(f"  Saga completed: {saga.completed}")

    # ── 4. Saga: Compensation on failure ──────────────────────────────────────
    print("\n─── 4. Saga: Payment Fails → Compensation ───")

    state2 = {'inventory': 10, 'payment_charged': False, 'order_confirmed': False}
    call_count = 0

    def failing_payment():
        global call_count
        call_count += 1
        return False   # Payment always fails

    saga2 = SagaOrchestrator("order-saga-002")
    saga2.add_step(
        name="Reserve Inventory",
        action=lambda: (state2.update({'inventory': state2['inventory'] - 1}) or True),
        compensate=lambda: (state2.update({'inventory': state2['inventory'] + 1}) or True),
    ).add_step(
        name="Charge Payment",
        action=failing_payment,
        compensate=lambda: (state2.update({'payment_charged': False}) or True),
    )

    success2 = saga2.execute()
    print(f"  Final state after compensation: {state2}")
    print(f"  Saga compensated: {saga2.compensated}")
    print(f"  Inventory restored: {state2['inventory'] == 10}")

    # ── 5. Outbox pattern ────────────────────────────────────────────────────────
    print("\n─── 5. Outbox Pattern ───")
    outbox = OutboxPublisher()

    # Write order + outbox entry atomically
    outbox.write_with_outbox(
        business_key="order:ord-001",
        business_value={"status": "PENDING", "total": 49.99},
        aggregate_type="Order",
        aggregate_id="ord-001",
        event_type="OrderCreated",
        event_payload={"orderId": "ord-001", "total": 49.99},
    )

    print(f"  Pending outbox entries: {len(outbox.pending_entries())}")

    # Publisher polls and delivers
    outbox.poll_and_publish()
    print(f"  Published events: {len(outbox.published_events())}")
    print(f"  Pending after publish: {len(outbox.pending_entries())}")

    # ── 6. Exactly-once deduplication ──────────────────────────────────────────
    print("\n─── 6. Exactly-Once Delivery (Idempotency Key) ───")
    tracker = ExactlyOnceTracker()
    processed_count = 0

    def process_payment(payment_id: str, amount: float):
        global processed_count
        processed_count += 1
        print(f"  [Consumer] Processed payment {payment_id}: ${amount}")

    # First delivery
    tracker.process_once("pay-001", process_payment, "pay-001", 99.99)
    # Duplicate (simulating at-least-once re-delivery)
    tracker.process_once("pay-001", process_payment, "pay-001", 99.99)
    # Different payment
    tracker.process_once("pay-002", process_payment, "pay-002", 49.99)

    print(f"  Total processing calls: {processed_count} (expected 2, not 3)")
```

---

## 6. Visual Reference

### 2PC Protocol Flow

```
                    COORDINATOR                 PARTICIPANT-A    PARTICIPANT-B
                        │                            │                │
Phase 1 (Prepare):      │                            │                │
  Write A, Write B      │── write ──────────────────►│                │
                        │── write ─────────────────────────────────►  │
  Send PREPARE          │── PREPARE ─────────────────►│                │
                        │── PREPARE ───────────────────────────────► │
  Participants log      │                 [log: prepared]  [log: prepared]
  prepare state, hold   │                 [hold locks]     [hold locks]
  locks, respond        │◄── YES ─────────────────────│                │
                        │◄── YES ───────────────────────────────────── │
                        │                            │                │
Phase 2 (Commit):       │                            │                │
  Log COMMIT decision   │ [log: COMMIT]              │                │
  ← critical write →    │                            │                │
  Send COMMIT           │── COMMIT ──────────────────►│                │
                        │── COMMIT ─────────────────────────────────► │
  Participants commit,  │                 [commit]   [release locks]
  release locks, ACK    │◄── ACK ──────────────────── │                │
                        │◄── ACK ─────────────────────────────────── │
  Transaction complete  │                            │                │

BLOCKING SCENARIO (coordinator crashes between Phase 1 and Phase 2):
  Participants are STUCK: voted YES (can't unilaterally abort)
                          haven't received COMMIT/ABORT
                          holding locks indefinitely
  Duration: until coordinator recovers and re-reads decision log
```

### Saga Choreography Flow (Order Processing)

```
OrderService    InventoryService    PaymentService    NotificationService
    │                  │                  │                   │
    ├─OrderCreated────►│                  │                   │
    │                  ├─InventoryReserved►│                  │
    │                  │                  ├─PaymentCompleted──►│
    │◄─────────────────────────────────────────────────────────│
    │  OrderConfirmed                                          │
    
FAILURE PATH (payment fails):
OrderService    InventoryService    PaymentService
    │                  │                  │
    ├─OrderCreated────►│                  │
    │                  ├─InventoryReserved►│
    │                  │                  ✗ PaymentFailed
    │◄─────────────────────────────────── │  (publishes event)
    │  PaymentFailed                       │
    ├─ReleaseInventory►│                  │
    │                  ├─InventoryReleased►│
    │  OrderCancelled                      │

Each arrow is an event on a Kafka topic. Each service reacts to events
and publishes results. No central coordinator.
```

### Outbox Pattern

```
APPLICATION TIER:
┌─────────────────────────────────────────────────────────┐
│  BEGIN TRANSACTION                                       │
│    UPDATE orders SET status='CONFIRMED' WHERE id='ord1' │
│    INSERT INTO outbox (aggregate_id, event_type, ...)   │
│  COMMIT                  ← atomic: both or neither      │
└─────────────────────────────────────────────────────────┘
              │
              │ (both committed atomically)
              ▼
┌─────────────┐          ┌────────────────┐
│   orders    │          │    outbox       │
│  table      │          │  table          │
│  id:ord1    │          │  id:evt-001    │
│  status:OK  │          │  event:Created │
│             │          │  delivered:N   │
└─────────────┘          └────────────────┘
                                 │
              ┌──────────────────┘
              │  Debezium CDC reads new outbox rows
              ▼
     ┌──────────────────┐          ┌─────────────────┐
     │ Outbox Publisher │──produce─►│   Kafka Topic   │
     │ (Debezium)       │          │  orders.created │
     └──────────────────┘          └─────────────────┘
              │
              │ After successful publish:
              └──UPDATE outbox SET delivered=true WHERE id='evt-001'
```

---

## 7. Common Mistakes

**Mistake 1: Leaving prepared transactions (pg_prepared_xacts) unresolved.** Postgres prepared transactions hold row-level locks, prevent VACUUM from reclaiming dead tuples, and can cause table bloat and query performance degradation. A 2PC coordinator crash that leaves a prepared transaction unresolved blocks any row the transaction locked — permanently, until a DBA manually resolves it. Always monitor `pg_prepared_xacts` and alert on any entry older than 5 minutes. If the coordinator has decided to commit or abort, apply that decision manually (`COMMIT PREPARED` or `ROLLBACK PREPARED`). If the decision is unknown, contact the coordinator's recovery log before taking action.

**Mistake 2: Forgetting that Saga compensations see updated state.** A compensating transaction C₁ runs after T₁ committed. In the time between T₁ and C₁, another transaction may have read and modified the row T₁ changed. C₁ may be reversing not just T₁'s change but also a subsequent change made by another transaction. This is the "lost update" anomaly in compensation. Compensations should check the current state before modifying it — if the current state is not what T₁ left (someone else changed it), the compensation logic must handle that case explicitly.

**Mistake 3: Using `isolation.level=read_uncommitted` when consuming from transactional Kafka producers.** The default Kafka isolation level is `read_uncommitted`. If a transactional producer fails and aborts a transaction, consumers with `read_uncommitted` will have already seen those messages — they were delivered before the abort was received. Those messages need to be deduplication at the consumer. For truly transactional consumers, set `isolation.level=read_committed` and accept the LSO lag. For consumers that can handle duplicates, use `read_uncommitted` and idempotency keys.

**Mistake 4: Not persisting Saga state durably.** A Saga orchestrator that stores its current step state only in memory loses its position on crash. On recovery, it doesn't know which steps completed. If it restarts the Saga from the beginning, steps that already committed will execute a second time (duplicates). If it doesn't restart, completed work is lost. Always persist the Saga state to a durable store (a database table, Redis with AOF, or the event log itself) before executing each step. Recovery must be able to resume a Saga from its last durable state.

**Mistake 5: Making compensating transactions that can fail without retry.** A compensation that returns an error without retrying leaves the system in a partially-compensated state. If T₁ and T₂ completed and T₃ failed, compensation must execute C₂ and C₁. If C₂ fails and is not retried, T₁ and T₂'s effects remain — a permanently inconsistent state. Compensating transactions must be designed to retry until they succeed (with appropriate idempotency). If a compensation genuinely cannot be executed (the compensated system is gone), the system requires manual intervention — alert immediately and record the state for human resolution.

---

## 8. Production Failure Scenarios

### Scenario 1: 2PC Coordinator Crash Causes Postgres Table Deadlock

**Symptoms:** A payment processing service uses XA transactions across two Postgres databases (payment_db and ledger_db). The transaction manager (the payment service) crashes at 14:37. DBA investigation finds two rows in `pg_prepared_xacts` on each database, both from 14:37. All queries that touch those rows are blocked. The customer service team reports that 47 payments are "stuck" — neither completed nor failed. Four hours later, VACUUM runs and takes 8× longer than normal due to the locked rows preventing dead tuple cleanup.

**What happened:** The payment service prepared both databases (Phase 1 complete), wrote the commit decision to its in-memory state, and then crashed before completing Phase 2 (sending COMMIT). The in-memory decision was lost. Both databases are now in the "prepared" state — they voted YES and are waiting for a COMMIT or ROLLBACK that never comes.

**Resolution:**
1. Check the payment service's persistent store (if it exists) for the transaction decision
2. If found: replay the commit or rollback using `COMMIT PREPARED 'txn-id'` / `ROLLBACK PREPARED 'txn-id'` on both databases
3. If not found: examine the business logic — for each in-doubt transaction, determine from the business database whether the payment was intended to succeed. If yes: commit both databases. If no: rollback both.
4. After resolving: add monitoring on `pg_prepared_xacts` age > 5 minutes → PagerDuty
5. Long-term: ensure the coordinator writes its commit decision to a durable store (the database, not memory) before beginning Phase 2

**Key lesson:** 2PC requires that the coordinator's decision be durably written before Phase 2. If the coordinator uses an in-memory decision log, a crash produces in-doubt transactions that require manual resolution. Always write the coordinator's decision to a database table, external KV store, or distributed log before sending COMMIT to participants.

### Scenario 2: Kafka Stuck LSO Causes Consumer Lag Alert Storm

**Symptoms:** Monitoring alerts fire for growing consumer lag on 12 Kafka consumer groups, all consuming from the `payments` topic. The `LogEndOffset` shows new messages are being produced, but all `read_committed` consumers' offsets are stuck. Producers are not seeing errors. New `read_uncommitted` consumers can see all messages.

**Root cause investigation:** Check `LastStableOffset - LogEndOffset` for the `payments` topic:
```bash
kafka-log-dirs.sh --bootstrap-server localhost:9092 \
  --topic-list payments --describe | grep offsetLag
# offsetLag: 182455  ← 182k messages behind LSO
```

The gap between LSO and LEO is growing. This means there is an open, uncommitted transaction from a transactional producer. The `read_committed` consumer cannot advance its offset past the open transaction's starting offset. Check the transaction coordinator:

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group __transaction_state
```

A transactional producer (`payment-processor-3`) started a transaction 14 minutes ago and has not committed or aborted. The producer process is still alive but stuck in a long-running database query — it opened the Kafka transaction but has not produced any messages or committed.

**Resolution:** The transactional producer's `transaction.timeout.ms` (default 60 seconds) should have expired the transaction, but it was set to 300 seconds for this service. After 300 seconds, the transaction coordinator auto-aborts the transaction, LSO advances, and consumers resume.

**Long-term fix:** Set `transaction.timeout.ms` to 30 seconds for most use cases. If processing legitimately takes longer than 30 seconds, restructure: open the transaction only after all database work is complete (minimize the transaction window). Add monitoring: `records-lag-max` growing across all consumer groups simultaneously + `LastStableOffset` not advancing = stuck LSO from open transaction.

### Scenario 3: Saga Missing Compensation Causes Phantom Inventory Hold

**Symptoms:** The warehouse team reports that 2,000 inventory units across 150 SKUs are "reserved" (unavailable for new orders) but have no corresponding confirmed or cancelled order. The reservations have been in place for 3-5 days. These phantom holds are causing orders to fail with "out of stock" errors even though physical inventory is available.

**Root cause:** The order processing Saga has three steps: (1) Reserve inventory, (2) Charge payment, (3) Confirm order. The Saga is orchestrated via a stateful AWS Step Functions workflow. A dependency service used in Step 3 (the notification service) started returning 504 errors 5 days ago. Step 3 is configured to abort the Saga (not retry) on 504 errors.

However, the Saga's compensation logic only compensates Steps 1 and 2 on failure of Step 2 (payment failure). It does not compensate Step 1 (release inventory) on failure of Step 3. The Saga designer assumed "if payment succeeded, the order is confirmed — Step 3 can't fail." It did.

The notification service failure causes Step 3 to fail, which aborts the Saga — but inventory is never released and payment may or may not be reversed (depending on Step 2's compensation status).

**Fix:** Every Saga step must have a compensation, and the compensation chain must run for ANY step failure, not just specific step failures. Step 3 ("Confirm Order") compensation: update order status to CANCELLED. Step 1 compensation must also be triggered when Step 3 fails: release the inventory reservation. The Saga state machine must be: failed at any step → compensate ALL completed steps in reverse order.

Additionally: the notification service's 504s should not fail the Saga — notification delivery failure is not a business failure. Step 3 should be split: (3a) confirm order in database (non-compensable — but can be retried), (3b) send notification (best-effort, failure is acceptable).

### Scenario 4: Outbox Duplicate Events Cause Double-Charges

**Symptoms:** A financial services company using the Outbox pattern for payment events experiences 23 cases of double-charging in a week. Customers are billed twice for the same transaction. The payment service shows only one payment record per customer, but the downstream billing service processed two events.

**Root cause:** The Outbox publisher (a Debezium connector replicating the `outbox` table) had a configuration issue: it was delivering events with `at-least-once` guarantees (correct) but the billing service consumer was not idempotent. The billing service processed each Kafka message exactly once per message offset — but the Debezium connector restarted after a brief connectivity issue and re-delivered 23 outbox events from before the last committed consumer offset.

The billing service had no idempotency check: it would charge the customer on every `PaymentCreated` event it received, regardless of whether that event had been processed before.

**Fix:** Every consumer of the outbox topic must be idempotent. The billing service must deduplicate by the outbox entry's `id` (a UUID that is stable for a given payment event). Before processing a `PaymentCreated` event, check: has this `outbox_event_id` already been processed? If yes, skip. If no, process and record. Store processed event IDs in a deduplication table or Redis with a TTL equal to the maximum possible redelivery window (typically 24 hours).

The general rule: outbox/CDC events are always at-least-once. Consumer idempotency is mandatory, not optional.

---

## 9. Performance and Tuning

### 2PC Latency Budget

Each round-trip in 2PC adds latency:
```
2PC total latency = prepare_leader_fsync + RTT(coord→A) + A_prepare_fsync
                  + RTT(A→coord) + coord_decision_fsync
                  + RTT(coord→A) + A_commit_fsync + RTT(A→coord)
                  + RTT(coord→B) + B_prepare_fsync ... (same for B)

Cross-AZ (same region): RTT ≈ 2-5ms
Cross-region:           RTT ≈ 50-150ms

For a 2-participant 2PC in the same region:
  ≈ 3-4 × RTT + 4 × fsync ≈ 3×3ms + 4×5ms ≈ 29ms minimum

For cross-region 2PC:
  ≈ 3×100ms + 4×5ms ≈ 320ms minimum
```

This is why 2PC is impractical across regions. For in-region use, 30-50ms per transaction is acceptable for many use cases but limits transaction throughput.

### Kafka Transaction Tuning

```
# Kafka transaction configuration:

# Producer:
transactional.id=<unique-id-per-producer-instance>
transaction.timeout.ms=30000      # Max time for an open transaction (default: 60s)
                                  # Shorter = faster LSO recovery on producer crash
enable.idempotence=true           # Required for transactions

# Consumer:
isolation.level=read_committed    # Sees only committed transactional messages
fetch.min.bytes=1                 # Don't wait for large batches with EOS
max.poll.interval.ms=300000       # Allow enough time for processing before heartbeat

# Broker (topic-level):
min.insync.replicas=2             # Required for durability with acks=all
# Transactional producers always use acks=all

# LSO monitoring:
# kafka_log_log_startoffset vs kafka_log_log_logendoffset
# Alert: LastStableOffset stagnant while LogEndOffset growing = stuck LSO
```

### Saga Timeout and Retry Configuration

```python
# Saga step retry configuration:
# For transient failures (network, timeout): retry with backoff
# For permanent failures (business rule violation): go straight to compensation

STEP_RETRY_CONFIG = {
    'max_retries': 3,
    'initial_backoff_ms': 100,
    'max_backoff_ms': 5000,
    'retryable_exceptions': [TimeoutError, ConnectionError],
    'non_retryable_exceptions': [ValueError, BusinessRuleViolation],
}

# Compensation retry configuration:
# Compensations must eventually succeed — retry indefinitely
COMPENSATION_RETRY_CONFIG = {
    'max_retries': float('inf'),   # Retry forever
    'initial_backoff_ms': 100,
    'max_backoff_ms': 60000,       # Cap at 1 minute between retries
    'alert_after_retries': 5,      # Alert on-call after 5 failed compensation attempts
}

# Saga timeout:
# Each Saga should have an overall timeout beyond which it's declared
# failed and compensation begins. This prevents Sagas from blocking
# resources indefinitely due to a hanging downstream service.
SAGA_TIMEOUT_SECONDS = 300   # 5 minutes maximum for order placement Saga
```

---

## 10. Interview Q&A

**Q1: Explain Two-Phase Commit. What is the blocking problem and when does it occur?**

Two-Phase Commit is a protocol for achieving atomicity across multiple independent participants (databases, services). A coordinator orchestrates two phases. In Phase 1 (Prepare), the coordinator asks all participants whether they are ready to commit. Each participant checks its constraints, acquires necessary locks, writes a "prepared" record to its durable WAL (so it can honor its vote even after a crash), and responds YES or NO. If all respond YES, the coordinator proceeds to Phase 2. If any respond NO, the coordinator sends ABORT to all.

In Phase 2 (Commit), the coordinator first writes its commit decision to its own durable log — this is the critical point of no return. Then it sends COMMIT to all participants, who commit their local transactions, release their locks, and acknowledge.

The blocking problem occurs when the coordinator crashes between the end of Phase 1 (all participants have voted YES and are holding locks) and the completion of Phase 2 (coordinator has not yet sent COMMIT or ABORT). At this point, all participants are in a state called "in-doubt" — they cannot unilaterally commit (the coordinator may have decided to abort and notified other participants) and cannot unilaterally abort (the coordinator may have decided to commit). They are stuck holding their locks until the coordinator recovers and resends the Phase 2 decision.

In practice, this means that if the coordinator crashes during a 2PC transaction, all rows locked by that transaction are unavailable — potentially indefinitely. For short transactions (milliseconds) with a quickly-recovering coordinator, this is acceptable. For long-lived transactions or in systems where coordinator crashes are common, this blocking is unacceptable, which is why 2PC is often replaced by Saga patterns in modern microservice architectures.

**Q2: What is the Saga pattern and how does it differ from 2PC?**

The Saga pattern breaks a distributed transaction into a sequence of local transactions, each of which commits immediately to its local database. If any step fails, the Saga executes compensating transactions in reverse order to undo the effects of the completed steps. Unlike 2PC, a Saga does not achieve global atomicity — between the commit of each step and its eventual compensation (if needed), other readers may see the partial state.

The key differences: 2PC provides strong atomicity (no intermediate states are visible) but is blocking (participants hold locks until the coordinator completes Phase 2). The Saga provides eventual consistency (intermediate states are visible) but is non-blocking (each step commits independently, releasing its resources immediately).

This makes Sagas the right choice for cross-service distributed transactions in microservice architectures. Each service owns its database; 2PC would require cross-database coordination that creates tight coupling. With a Saga, each service makes a local commit and either proceeds or compensates. The Saga also works across heterogeneous systems (databases, message queues, HTTP APIs) that don't all support 2PC/XA.

The design challenge in Sagas is compensation: each step must have a compensating transaction that is idempotent, retries indefinitely until it succeeds, and accounts for the fact that the compensated state may have been further modified by other operations between the original step and the compensation.

**Q3: What is the outbox pattern and what problem does it solve?**

The outbox pattern solves the problem of atomically updating a database and publishing a message to a message broker, when the broker does not support distributed transactions with the database. The naive approach — write to database, then publish to Kafka — has a gap: if the process crashes between the two operations, either the database write succeeded without a message being published (missed event) or a message was published for a write that never completed (phantom event).

The outbox pattern uses the database itself as an intermediate store. Within the same database transaction as the business logic, the application inserts a row into an "outbox" table describing the event to be published. When this transaction commits, both the business change and the outbox entry are atomically persisted — there is no gap. A separate publisher process (often Debezium CDC) reads new outbox rows and publishes them to Kafka, then marks them as delivered.

The outbox achieves at-least-once delivery: if the publisher crashes after publishing but before marking the row as delivered, it will re-read and republish the row. Consumers of the outbox topic must therefore be idempotent — they must handle duplicate delivery without producing incorrect results.

---

## 11. Cross-Question Chain

**Interviewer:** You're designing an e-commerce order processing service. When a customer places an order, you need to: (1) create the order in the orders database, (2) reserve inventory in the inventory service, (3) charge the customer's payment method, and (4) send a confirmation email. How do you implement this as a Saga?

**Candidate:** This is a textbook Saga. The steps are heterogeneous — a database write, an HTTP call to an inventory microservice, a payment API call, and an email service. 2PC won't work (none of these speak XA together), so a Saga is the right choice.

I'd use an orchestrated Saga (rather than choreography) because order processing has complex failure paths and I want one place to understand and debug the Saga's state. The orchestrator is a stateful service — I'd use a database table to persist the Saga's state machine so it survives crashes.

The steps and their compensations: Step 1 is create order with status=PENDING. Compensation: update order status to CANCELLED. Step 2 is reserve inventory. Compensation: release inventory reservation. Step 3 is charge payment. Compensation: issue refund. Step 4 is send confirmation email. Compensation: ideally send a cancellation email, but email delivery is not a business-critical operation — if the email fails, I'd retry it independently rather than compensating the Saga.

The semantic lock I'd use: the order stays in status=PENDING until the full Saga completes. Any business logic that checks order status must treat PENDING as "unknown outcome" — not confirmed, not cancelled. The customer sees "Your order is being processed" until status becomes CONFIRMED or CANCELLED.

**Interviewer:** Step 3 (payment) fails. Walk me through exactly what happens, including what states are visible to the customer at each point.

**Candidate:** Here's the exact sequence. Before Step 1: order doesn't exist. After Step 1: order is in the database with status=PENDING and order_id=ord-123. If the customer queries their order history right now, they see a PENDING order. Inventory is not yet reserved — status is ambiguous.

After Step 2: inventory is reserved in the inventory service. The inventory item shows as "unavailable" for new orders. The order is still PENDING.

Step 3 fails: payment API returns `CARD_DECLINED`. The Saga transitions to COMPENSATING state. I persist this state change before executing compensation — if the orchestrator crashes during compensation, it will resume from COMPENSATING state and know which steps to undo.

Compensation Step 2: release inventory reservation. The inventory service's reservation is removed. The item is available for new orders again.

Compensation Step 1: update order status to CANCELLED. The customer now sees their order in CANCELLED status.

At no point between Step 1 and the final CANCELLED status should the customer's account be debited — Step 3 failed before the charge. The customer sees a PENDING order briefly (milliseconds to seconds) and then a CANCELLED order with a reason of "payment declined." This intermediate visibility (PENDING → CANCELLED) is the Saga's "eventual consistency" tradeoff compared to 2PC's invisible intermediate state.

**Interviewer:** The inventory service's compensation (releasing the reservation) fails because the inventory service is temporarily down. What happens?

**Candidate:** The compensation must retry. Compensation for Step 2 goes into a retry loop with exponential backoff: 100ms, 200ms, 400ms, 800ms... capped at 60 seconds between retries. The Saga remains in COMPENSATING state. The inventory is still reserved — it appears "unavailable" to new orders.

If the inventory service is down for 5 minutes, the compensation retries during those 5 minutes. When the inventory service comes back up, the next retry succeeds, the reservation is released, and the Saga transitions to COMPENSATED state.

I'd alert the on-call engineer if a compensation fails more than 5 times in a row. This is not an emergency — the Saga will eventually self-heal when the service recovers. But it signals that the inventory service may be having issues, and monitoring compensation retry counts is a leading indicator of service health problems.

The key principle: compensations never give up. They retry indefinitely because a partially-compensated Saga is a permanently inconsistent state. The only exception is if the compensation is literally impossible (the compensated system has been decommissioned, the data has been deleted, etc.) — in that case, a human must manually resolve it and mark the Saga as manually-compensated.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is Two-Phase Commit (2PC) and what problem does it solve? | A protocol for achieving distributed atomicity across multiple participants. Phase 1: coordinator asks all participants "can you commit?" (PREPARE). Phase 2: coordinator sends COMMIT or ABORT based on unanimous YES/NO votes. Ensures all participants commit or all abort together. |
| 2 | What is the "in-doubt" state in 2PC? | A participant state entered after voting YES in Phase 1 but before receiving Phase 2 decision. The participant holds its locks but cannot commit or abort unilaterally. Occurs when the coordinator crashes between Phase 1 and Phase 2. |
| 3 | Why is the coordinator's decision write the most critical operation in 2PC? | Once all participants vote YES, the coordinator must write its commit/abort decision to durable storage before sending Phase 2. If the coordinator crashes after this write, recovery reads the decision and completes Phase 2. Without this write, a crash leaves all participants in-doubt indefinitely. |
| 4 | What is an XA transaction? | The standard protocol for 2PC across heterogeneous databases. Supported by most relational databases. An XA transaction manager (Atomikos, Narayana) acts as the 2PC coordinator. Each database's XA resource manager is a participant. |
| 5 | What is the Saga pattern? | A sequence of local transactions T₁...Tₙ each committing immediately. For each Tᵢ, a compensating transaction Cᵢ semantically reverses it. If Tₖ fails, execute Cₖ₋₁, Cₖ₋₂, ..., C₁ in reverse order. Achieves eventual consistency without global locking. |
| 6 | What is a compensating transaction and how does it differ from a rollback? | A rollback is a database operation that literally undoes uncommitted changes as if they never happened. A compensating transaction is a new committed transaction that semantically reverses a previously-committed transaction. Other readers may see the effect of T₁ before C₁ executes — compensation is visible, rollback is not. |
| 7 | What are the two Saga implementation styles? | Choreography: each service reacts to events from the previous step, no central coordinator. Orchestration: a central Saga orchestrator sends commands and receives replies, maintains state, decides when to compensate. Choreography is simpler but harder to debug; orchestration is more complex but easier to reason about. |
| 8 | Why must compensating transactions be idempotent? | If a compensation fails and is retried, running it multiple times must produce the same result as running it once. Without idempotency, a double-retry of C₁ could over-compensate (e.g., refunding twice, releasing inventory twice). |
| 9 | What is the outbox pattern? | An approach to atomically update a database and publish a message to a broker without 2PC. The business change and an "outbox" table insert are committed in the same DB transaction. A separate publisher (Debezium CDC) reads outbox rows and publishes to Kafka. Guarantees at-least-once delivery. |
| 10 | Why must outbox consumers be idempotent? | The outbox publisher may re-deliver a message if it crashes after publishing but before marking the outbox row as delivered. Consumers must handle duplicate delivery without producing incorrect results (e.g., charging twice). Use idempotency keys (the outbox row's UUID) to deduplicate. |
| 11 | What is Kafka's transactional producer and what does it guarantee? | A producer configured with `transactional.id` that uses Kafka's internal 2PC protocol (transaction coordinator + partition leaders) to ensure a set of messages is either all visible to `read_committed` consumers or none are visible. Used for exactly-once stream processing. |
| 12 | What is the LSO (Last Stable Offset) in Kafka and why does it matter? | The highest offset at which all transactions are settled (committed or aborted). `read_committed` consumers cannot advance past the LSO. An open transaction causes the LSO to stall, making consumers appear to have growing lag even though messages exist above the LSO. |
| 13 | What is the "stuck LSO" problem in Kafka? | An open transactional produce (transaction started but not committed or aborted) causes the LSO to stall. All `read_committed` consumers stop advancing. Alert signal: LSO not advancing while LEO grows. Fix: set `transaction.timeout.ms` to a short value so the coordinator auto-aborts hung transactions. |
| 14 | When should you use 2PC vs Saga? | Use 2PC when: participants support XA, transactions are short-lived, strong isolation is required, and participants are homogeneous databases. Use Saga when: participants are heterogeneous (APIs, queues, DBs), transactions are long-lived, strong isolation is not required, and high availability is needed. |
| 15 | What is the "semantic lock" countermeasure in Sagas? | Setting a status flag on a resource at the start of a Saga (e.g., `order.status=PENDING`). Other readers that see PENDING know the resource is mid-Saga and must not make conflicting changes. The lock is cleared when the Saga completes or compensates. |
| 16 | What is the difference between Saga choreography and orchestration? | Choreography: services communicate via events (Kafka/EventBridge), each service reacts independently, no central coordinator — decentralized but hard to trace. Orchestration: a central coordinator commands each step, tracks state, triggers compensation — more complex to build but failure paths are explicit and debuggable. |
| 17 | How does Flink achieve exactly-once semantics with Kafka? | Flink uses Kafka transactions: at checkpoint time, Flink pre-commits the Kafka output transaction, writes its state to the checkpoint backend, then commits the Kafka transaction. If Flink crashes before the commit, on recovery it aborts the in-progress transaction and replays from the last checkpoint — exactly-once output. |
| 18 | What happens to a `read_committed` Kafka consumer when a transactional producer aborts? | Messages from the aborted transaction were never visible to `read_committed` consumers (they were below the LSO). The consumer never sees them. The abort marker causes the LSO to advance past the aborted messages, and the consumer continues. |
| 19 | Why is 2PC between microservices considered an anti-pattern? | Each microservice owns its database; 2PC between their databases couples them at the data layer — if one database's 2PC participant blocks, the other's locks are held. This defeats microservice isolation, creates distributed deadlock risk, and requires coordinated rollback across service boundaries. Use Saga or Outbox instead. |
| 20 | What is the "re-read value" countermeasure in Sagas? | Before a Saga step that reads and then conditionally updates, re-read the value with a version number or CAS check. Prevents lost updates when two concurrent Sagas both read the same initial value and apply their respective changes — one's change overwrites the other's. |

---

## 13. Further Reading

- **"Designing Data-Intensive Applications" by Martin Kleppmann, Chapter 9 (Consistency and Consensus) and Chapter 12 (The Future of Data Systems):** Chapter 9 covers 2PC, atomic commit, and the blocking problem in depth. Chapter 12 covers the end-to-end argument and exactly-once processing patterns. Both are essential.
- **"Sagas" by Garcia-Molina and Salem (1987):** The original paper proposing the Saga pattern. Surprisingly readable and short (14 pages). The original framing was long-lived database transactions, not microservices, but the core mechanism is identical.
- **"Pattern: Saga" on microservices.io (Chris Richardson):** The definitive web resource on the Saga pattern for microservices. Covers choreography vs orchestration, countermeasures, and concrete examples. The diagrams are excellent.
- **Debezium documentation — "Outbox Event Router" SMT:** The definitive reference for implementing the transactional outbox pattern with Debezium. Explains the `EventRouter` SMT configuration and the routing logic.
- **Kafka documentation — "Transactions":** The official Kafka documentation on transactional producers and consumers. Covers the transaction protocol, LSO semantics, and exactly-once stream processing with Kafka Streams and consumer groups.
- **"Database Internals" by Alex Petrov, Chapter 13 (Distributed Transactions):** A thorough treatment of 2PC, 3PC, and Paxos Commit, with analysis of the blocking problem and alternatives. More rigorous than DDIA and complementary to it.

---

## 14. Lab Exercises

**Exercise 1: Postgres Prepared Transactions — Simulate Coordinator Crash**

Set up two connections to Postgres. Connection 1: BEGIN, execute writes, PREPARE TRANSACTION 'test-txn-001'. Now disconnect Connection 1 (simulate coordinator crash). On a third connection: query `pg_prepared_xacts` — see the prepared transaction. Observe that the locked rows are inaccessible from other connections. Manually resolve by running COMMIT PREPARED or ROLLBACK PREPARED based on a "recovered" decision. Document: what blocked, for how long, and the resolution steps.

**Exercise 2: Kafka Transaction LSO Observation**

Set up a Kafka topic with a transactional producer. Begin a transaction and produce several messages, but do NOT commit. In a second terminal, run a `read_committed` consumer — observe it receives nothing (LSO not advancing). In a third terminal, run a `read_uncommitted` consumer — observe it receives the uncommitted messages. Commit the transaction in the first terminal — observe the `read_committed` consumer now receives all messages. Then repeat with an aborted transaction: the `read_committed` consumer skips the aborted messages; the `read_uncommitted` consumer may have seen them.

**Exercise 3: Saga Compensation with State Verification**

Using `distributed_transactions.py`'s `SagaOrchestrator`: build a 3-step Saga (reserve inventory, charge payment, confirm order). In Step 3, simulate a failure. Verify: (1) Step 1 and 2's effects are visible before the failure, (2) after compensation, Step 1 and 2's effects are reversed, (3) compensation runs in reverse order (Step 2 compensated before Step 1). Run the Saga again with Step 2 failing — verify only Step 1 is compensated.

**Exercise 4: Outbox Pattern with Debezium**

Set up a Postgres + Debezium + Kafka environment. Create the outbox table from the walkthrough. Write a script that inserts rows into a business table and the outbox table in the same transaction. Observe: Debezium picks up the outbox rows and publishes them to a Kafka topic. Simulate a "crash" (disconnect Debezium after publishing but before the outbox row is marked delivered) and observe re-delivery on reconnect. Verify that consumers with idempotency keys handle the duplicate correctly.

**Exercise 5: Exactly-Once Consumer Deduplication**

Using `ExactlyOnceTracker` from the code toolkit: simulate a consumer receiving 100 payment events, where 15 are duplicates (same idempotency key, different Kafka offsets — simulating at-least-once re-delivery). Verify that the tracker processes each unique payment exactly once (85 processing calls, not 100). Simulate the tracker restarting and receiving the same 100 events again — verify the dedup window (1 hour) prevents re-processing. Adjust `dedup_window_seconds` and observe the trade-off between memory use and duplicate protection.

---

## 15. Key Takeaways

Distributed transactions solve the hardest problem in distributed systems: achieving atomicity across multiple independent systems that each have their own local consistency guarantees. There is no free solution — every approach trades something.

Two-Phase Commit achieves strong atomicity (no intermediate states visible, all-or-nothing) but at the cost of locking and blocking. Between Phase 1 (participants vote YES) and Phase 2 (coordinator sends COMMIT), all participants hold their locks. If the coordinator crashes in this window, participants are in-doubt indefinitely — blocked until the coordinator recovers and replays its decision. This makes 2PC unsuitable for long-lived operations, cross-region scenarios, or any system where coordinator availability cannot be guaranteed. The coordinator's decision log must be durably written before Phase 2 begins — this is the non-negotiable invariant that makes recovery possible.

Sagas trade strong atomicity for availability. Each step commits locally; compensation reverses completed steps if a later step fails. Intermediate states are visible. The critical requirements for production Sagas: persist Saga state durably at each step (so recovery can resume from the right point), make all compensations idempotent (safe to retry), design compensations to retry indefinitely (a failed compensation is a permanently inconsistent state), and handle the fact that compensation runs on already-committed state (other readers may have seen the intermediate states).

The Outbox pattern solves the database + message broker atomicity problem without 2PC. Write the business change and an outbox entry in the same DB transaction. A CDC publisher delivers the outbox entry to Kafka. At-least-once delivery means consumers must be idempotent — always.

Kafka's transactional producer implements a form of 2PC within Kafka itself to provide exactly-once semantics for stream processing. The LSO is the mechanism that gates `read_committed` consumers — an open transaction holds the LSO back, making consumers appear to lag. Monitor LSO vs LEO as a proxy for stuck open transactions.

---

## 16. Connections to Other Modules

- **M38 — Consensus Algorithms:** 2PC uses a coordinator as a central authority. Raft provides the consensus mechanism for making the coordinator itself fault-tolerant — a coordinator backed by a Raft quorum (like the Kafka transaction coordinator, backed by a Kafka partition with its own leader election) removes the coordinator as a single point of failure.
- **M39 — Replication:** The Outbox pattern leverages Postgres logical replication (Debezium CDC) as the delivery mechanism. Understanding WAL, logical decoding, and replication slots is essential for production Outbox implementations.
- **M40 — Failure Modes:** Partial failure is the core challenge that both 2PC and Saga address. 2PC eliminates partial failure via the prepare phase (participants agree to commit before any does). Saga handles partial failure via compensation (undo completed steps when a later step fails). The blocking problem in 2PC is itself a partial failure mode.
- **DE-ORC-101 — Airflow:** Airflow DAG tasks with `retry` configurations implement a simplified Saga-like pattern — if a downstream task fails, upstream tasks' effects may need compensation. Understanding when Airflow's retry logic is sufficient vs when true Saga compensation is needed is a staff-level skill.
- **DCS-STR-101 — Flink Fundamentals:** Flink's exactly-once semantics and checkpoint-based recovery are the production implementation of Kafka transactions for stream processing. Understanding the underlying distributed transaction mechanism explains when Flink's EOS guarantees hold and when they don't (e.g., non-idempotent sinks).

---

## 17. Anti-Patterns

**Anti-pattern: 2PC across microservice HTTP boundaries.** Some systems use HTTP calls within a 2PC protocol — the coordinator sends Prepare to a microservice's HTTP endpoint, waits for a YES/NO response, then sends Commit or Abort. This is 2PC over HTTP. It has all of 2PC's problems (blocking, coordinator crash = in-doubt participants) plus HTTP's lack of transactional guarantees (idempotency of the HTTP call itself is not provided). Use Saga for cross-microservice coordination.

**Anti-pattern: Long-running Kafka transactions.** Keeping a Kafka transaction open while performing database queries, waiting for user input, or doing expensive computation. The LSO is stalled for the entire duration. All `read_committed` consumers are blocked. Transaction timeout fires. `transaction.timeout.ms` should be set to the minimum practical value (30-60 seconds). Open the Kafka transaction only for the narrow window of actually producing messages.

**Anti-pattern: Choreography Saga with no central state tracking.** In a choreography Saga, each service reacts to events from the previous step. If the Saga is complex (5+ steps, multiple failure paths), debugging a stuck or partially-compensated Saga requires tracing events across multiple systems' logs. Without a central Saga log (even just a status table recording which Sagas are in-progress and their current state), diagnosing why 100 orders are stuck at "Step 3 of 6" requires reading logs from 6 separate services. Always maintain a Saga status table — even in choreography-style Sagas.

**Anti-pattern: Saga compensation that can permanently fail.** A compensation that calls an external API that may return 404 (resource not found, perhaps deleted by another process). If the API returns 404 and the compensation marks itself as "failed," the Saga is permanently in a partially-compensated state with no automatic resolution. Compensation for external API calls must handle "already gone" responses as success (the effect is already reversed — no further action needed). A 404 on a compensation is usually idempotent success.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `pg_prepared_xacts` | Monitor in-doubt Postgres 2PC transactions | Alert on entries older than 5 minutes |
| `PREPARE TRANSACTION` | Postgres 2PC Phase 1 | Durably prepares a transaction for coordinator decision |
| `COMMIT PREPARED` / `ROLLBACK PREPARED` | Postgres 2PC Phase 2 | Complete or abort a prepared transaction |
| Kafka `transactional.id` producer | Kafka EOS | Enables transactional produce across partitions |
| `isolation.level=read_committed` | Kafka consumer | Sees only committed transactional messages |
| `transaction.timeout.ms` | Kafka transaction timeout | Auto-abort long-running open transactions |
| Debezium `EventRouter` SMT | Outbox pattern CDC | Routes outbox table CDC events to per-aggregate topics |
| `distributed_transactions.py` | 2PC and Saga simulation | TwoPCCoordinator, SagaOrchestrator, OutboxPublisher |
| Axon Framework / Eventuate Tram | Production Saga frameworks | Production-grade Saga orchestration with persistence |
| AWS Step Functions | Managed Saga orchestration | Stateful Saga orchestration as a managed service |
| `kafka-log-dirs.sh` | Kafka LSO monitoring | Check `offsetLag` for stuck LSO detection |

---

## 19. Glossary

**2PC (Two-Phase Commit):** A distributed transaction protocol that achieves atomicity across multiple participants via a prepare phase (all participants vote) and a commit phase (coordinator sends the unanimous decision). Provides strong atomicity but is vulnerable to blocking when the coordinator crashes between phases.

**Atomicity:** The property that a set of operations either all succeed or all fail — there is no partial completion. Local atomicity is provided by a database transaction. Distributed atomicity requires coordination between multiple systems.

**Compensating transaction:** A transaction that semantically reverses the effect of a previously-committed transaction. Unlike a rollback, a compensation is a new committed change — its execution is visible to other readers. Must be idempotent and designed to eventually succeed.

**Exactly-once semantics (EOS):** A delivery guarantee where a message is processed exactly once — not zero times (lost) and not more than once (duplicate). In Kafka, achieved via transactional producers + `read_committed` consumers + state backend checkpointing.

**In-doubt:** The state of a 2PC participant after voting YES in Phase 1 but before receiving a Phase 2 decision. The participant holds its locks and cannot commit or abort unilaterally. Duration: until the coordinator recovers and replays its decision.

**LSO (Last Stable Offset):** In Kafka, the highest offset at which all transactions up to that point are settled (committed or aborted). `read_committed` consumers cannot read messages above the LSO. An open transaction stalls the LSO.

**Outbox pattern:** A pattern for achieving at-least-once delivery of database events to a message broker without 2PC. The business change and an outbox table entry are committed atomically in the same database transaction; a CDC publisher delivers the outbox entry to the broker.

**Saga:** A sequence of local transactions that each commit immediately. If a step fails, compensating transactions reverse the completed steps in reverse order. Achieves eventual consistency without global locking.

**Saga orchestration:** A Saga implementation style where a central coordinator commands each step, tracks state, and triggers compensation. More complex but easier to trace and debug than choreography.

**Saga choreography:** A Saga implementation style where each service reacts to events from the previous step and publishes results for the next. No central coordinator — decentralized but harder to trace across multiple services.

**Semantic lock:** A status flag (e.g., `status=PENDING`) set at the start of a Saga to prevent conflicting operations by other transactions until the Saga completes or compensates.

**Transactional producer (Kafka):** A Kafka producer configured with `transactional.id` that uses the transaction coordinator protocol to ensure messages are atomically visible (committed) or invisible (aborted) to `read_committed` consumers.

**XA transaction:** The industry-standard protocol for Two-Phase Commit across heterogeneous databases. Supported by most relational databases. An XA transaction manager coordinates the 2PC protocol across XA-capable participants.

---

## 20. Self-Assessment

1. Describe the two phases of 2PC. At what point is the transaction's outcome determined? What happens if the coordinator crashes before that point vs. after it?
2. A Postgres database has a prepared transaction from 3 hours ago. What does this indicate? How do you determine whether to commit or rollback it?
3. What is the difference between a compensating transaction and a rollback? Give a concrete example where a compensation would produce different results than a rollback.
4. A Saga for order placement completes Step 1 (create order) and Step 2 (reserve inventory) but fails at Step 3 (charge payment). The compensation for Step 2 (release inventory) also fails because the inventory service is temporarily down. What should happen? What should NOT happen?
5. Explain why `isolation.level=read_committed` in a Kafka consumer can cause consumer lag that is not due to slow processing.
6. What is the outbox pattern and when would you use it instead of a Kafka transactional producer?
7. A Saga orchestrator crashes during compensation (after successfully compensating Step 3 but before compensating Step 2). When it recovers, how does it know which compensation steps have already been executed? What design decision enables this?
8. Why is 2PC between microservices an anti-pattern? What would you use instead?
9. A Flink job with exactly-once semantics crashes mid-checkpoint. When it recovers, it replays from the last successful checkpoint. Why won't the output topic have duplicate records?
10. Compare the failure modes of a choreography Saga vs an orchestration Saga. When would you choose each despite its failure modes?

---

## 21. Module Summary

Distributed transactions represent the convergence of everything covered in SYS-DST-101. CAP theorem (M37) explains why strong distributed atomicity is expensive — it requires coordination that limits availability. Consensus algorithms (M38) provide the foundation for coordinators (like Kafka's transaction coordinator) that can themselves tolerate failures. Replication (M39) is the mechanism that makes each participant's committed state durable. Failure modes (M40) — particularly partial failure — are the exact failure scenarios that both 2PC and Saga must handle correctly.

Two-Phase Commit achieves strong atomicity through a prepare-then-commit protocol. The blocking problem — participants holding locks while waiting for a crashed coordinator — is 2PC's fundamental limitation. It makes 2PC appropriate for short-lived transactions across a small number of XA-capable database participants in the same region, and unsuitable for cross-microservice operations, long-lived transactions, or cross-region coordination. Prepared transaction monitoring (`pg_prepared_xacts`) is non-negotiable in any Postgres deployment using 2PC.

The Saga pattern sacrifices strong atomicity for availability and heterogeneity. Each step commits locally; compensating transactions undo completed steps if a later step fails. The design challenges are compensation (idempotent, guaranteed to eventually succeed, handles already-modified state) and Saga state persistence (durably record progress so crashes resume from the right point). Orchestrated Sagas centralize state and failure logic; choreographed Sagas distribute it via events. Both require a central Saga status log for operational visibility.

The Outbox pattern bridges the database-broker atomicity gap that neither 2PC (broker doesn't speak XA) nor Saga (this is a within-transaction constraint) fully addresses. Write business change + outbox entry atomically in the database; deliver asynchronously via CDC. At-least-once delivery requires idempotent consumers — non-negotiable.

Kafka's transactional producer implements exactly-once semantics for stream processing by applying a 2PC-like protocol across Kafka partitions. The LSO mechanism gates `read_committed` consumers and is the primary operational signal for stuck open transactions. Flink's exactly-once checkpoint mechanism is the production implementation that connects Kafka transactions to stateful stream processing.

The staff-level expectation: design a complete distributed transaction solution for a given use case — specifying 2PC vs Saga, compensation logic, idempotency mechanisms, state persistence, and failure recovery paths. This is the synthesis of all five SYS-DST-101 modules: theory (CAP, consensus, replication, failures) applied to the practical problem of making distributed systems behave correctly in the face of failures.
