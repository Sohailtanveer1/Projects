# M46: Observability in Distributed Systems

**Course:** SYS-DST-102 — Distributed Systems in Practice  
**Module:** 05 of 05  
**Global Module ID:** M46  
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

Every module in SYS-DST-101 and SYS-DST-102 has described distributed system failures and the mechanisms behind them. The consensus algorithm failed because a split vote wasn't resolved. The replication log fell behind. The ISR collapsed. The clock skewed. These are intellectual reconstructions — the kind of analysis you can do after the fact if you have enough data.

The problem is getting enough data. A distributed system spans dozens of services, hundreds of processes, and thousands of log lines per second. When something goes wrong — a user's order takes 12 seconds instead of 50ms — the root cause might be a slow database query, a retry storm on a downstream service, a garbage collection pause in a JVM, a network partition between two microservices, or a cascading replication lag. Without observability infrastructure, you are blind: you know something is wrong (the 12-second response time) but not where, why, or which component failed first.

Observability is the property of a system that allows you to answer these questions from the outside, without needing to push new code. The three pillars of observability are: **metrics** (aggregated numerical measurements, e.g., P99 latency), **logs** (timestamped records of discrete events), and **traces** (causally-connected spans of work across services). All three are necessary — none is sufficient alone. Metrics tell you something is wrong. Logs explain what happened in one service. Traces show the chain of causation across all services.

This module covers all three pillars with emphasis on distributed tracing — the pillar most specific to distributed systems — and implements them using OpenTelemetry, the industry-standard framework for instrumentation.

---

## 2. Mental Model

### The Three Pillars

```
Metrics:  "What is wrong?"
  - Aggregated counters, gauges, histograms
  - Query P99 latency, error rate, throughput
  - Tool: Prometheus (scrape) + Grafana (visualize)
  - Limitation: No per-request detail; no causation

Logs:     "What happened in this service?"
  - Timestamped, structured event records
  - "2026-06-28 12:00:01.234 ERROR db_query failed key=order-123"
  - Tool: Structured logging (structlog/JSON) + ELK/Loki
  - Limitation: Per-service, not cross-service; correlation by request_id required

Traces:   "How did this request flow through all services?"
  - A trace is a tree of spans, each representing a unit of work in one service
  - Spans carry: trace_id (connects all spans in a request), span_id, parent_span_id,
    timestamps, operation name, status, attributes
  - Tool: OpenTelemetry SDK + Jaeger/Tempo/Zipkin
  - Limitation: Sampling — not every request traced
```

### The Trace Data Model

A **trace** is a directed acyclic graph of **spans**. One trace = one end-to-end request (e.g., "user places an order").

```
Trace ID: abc123
│
├── Span 1: POST /order [API Gateway] 0ms → 1200ms
│   │
│   ├── Span 2: validate_order [OrderService] 5ms → 50ms
│   │   └── Span 4: SELECT * FROM inventory [Postgres] 10ms → 45ms   ← SLOW
│   │
│   ├── Span 3: charge_payment [PaymentService] 55ms → 400ms
│   │
│   └── Span 5: publish_event [Kafka] 405ms → 420ms
```

From this trace you can see: the 1200ms total response time was mostly dominated by Span 4 (a slow Postgres query taking 35ms) and Span 3 (payment taking 345ms). Without the trace, you'd see the 1200ms in a metric but not know which service or operation was responsible.

The key innovation of distributed tracing: the trace_id and parent_span_id are **propagated in HTTP headers / message headers** across service boundaries. Every service that receives a request also receives the trace context, creates its own span as a child, and propagates the context to downstream calls. The result is a tree that spans the entire request's journey.

---

## 3. Core Concepts

### 3.1 Trace Context Propagation

When service A calls service B, it includes the current trace context in the request headers:

```
HTTP headers (W3C Trace Context standard):
  traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
               ^^ ^^ trace_id (32 hex chars)          ^^ span_id     ^^ flags

Kafka message headers (OpenTelemetry Kafka propagation):
  traceparent: same format
  tracestate: vendor-specific extensions (optional)
```

Service B receives these headers, extracts the trace_id and parent_span_id, and creates its own span with the extracted parent_span_id as its parent. This creates the parent-child relationship that makes the trace a tree.

Without propagation, traces would be per-service islands — no connection between service A's span and service B's span. Context propagation is the mechanism that makes distributed tracing "distributed."

### 3.2 Span Attributes and Status

A span records:
- **Name:** the operation (e.g., `db.query`, `kafka.publish`, `http.request`)
- **Kind:** SERVER (received request), CLIENT (sending request), PRODUCER/CONSUMER (Kafka), INTERNAL
- **Start/end time:** wall clock, often with nanosecond precision
- **Attributes:** key-value pairs (e.g., `db.system=postgresql`, `db.statement=SELECT...`, `http.status_code=200`)
- **Status:** OK, ERROR (with error description)
- **Events:** timestamped log entries attached to the span
- **Links:** cross-trace references (e.g., linking a consumer span to the producer span that created the message)

The OpenTelemetry Semantic Conventions define standard attribute names — `db.system`, `http.method`, `messaging.system` — ensuring that tools like Jaeger can display database queries and HTTP calls with rich context without custom instrumentation.

### 3.3 Sampling

Tracing every request in production is expensive: a single request might generate 50+ spans. At 10,000 requests per second: 500,000 spans/second to store. Sampling reduces this to a manageable rate.

**Head-based sampling:** The decision to sample is made at the start of the trace (at the entry service). All spans in a sampled trace are recorded. Simple to implement but the decision is made before you know if the trace is interesting (e.g., you can't decide to always trace errors if the error isn't known yet at trace start).

**Tail-based sampling:** All spans are sent to a collector buffer. At trace completion, the collector decides whether to keep the trace (e.g., keep all error traces, keep 1% of successful traces). More complex but more useful — error traces are never discarded. OpenTelemetry Collector supports tail-based sampling via the `tail_sampling` processor.

**Adaptive sampling:** The sampling rate adjusts automatically based on throughput — 100% sampling at low traffic, 1% at peak. Maintains a fixed number of samples per unit time regardless of load.

### 3.4 OpenTelemetry

OpenTelemetry (OTel) is the CNCF standard for distributed observability. It provides:

- **SDK:** Language-specific libraries for creating and exporting spans, metrics, and logs (Python, Java, Go, etc.)
- **Semantic conventions:** Standard attribute names for common operations (HTTP, database, messaging)
- **Exporters:** Connectors to backend systems (Jaeger, Tempo, Zipkin, Prometheus, OTLP)
- **Collector:** A standalone process that receives, processes (sample, filter, enrich), and exports telemetry

The key benefit of OTel over vendor-specific SDKs (Datadog APM, New Relic, etc.): instrumentation is backend-agnostic. You write OTel instrumentation once and can send to Jaeger, Tempo, or a commercial APM without changing application code — just change the exporter.

### 3.5 Structured Logging

Structured logging emits logs as machine-parseable records (typically JSON) rather than free-form strings. Each record is a dictionary with typed fields:

```json
{
  "timestamp": "2026-06-28T12:00:01.234Z",
  "level": "ERROR",
  "service": "order-service",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "event": "db_query_failed",
  "table": "orders",
  "query_duration_ms": 4500,
  "error": "connection timeout"
}
```

The `trace_id` and `span_id` fields are the key connection: they allow you to jump from a log line to the corresponding trace in Jaeger with a single click. Without them, correlating logs across services requires manually matching timestamps — unreliable and slow.

---

## 4. Hands-On Walkthrough

### 4.1 Instrumenting a Python Service with OpenTelemetry

```bash
# Install OpenTelemetry packages
pip install opentelemetry-sdk opentelemetry-api \
    opentelemetry-exporter-otlp-proto-grpc \
    opentelemetry-instrumentation-requests \
    opentelemetry-instrumentation-psycopg2 \
    --break-system-packages
```

```python
# service_instrumented.py — minimal OTel instrumentation
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.trace import SpanKind, Status, StatusCode

# 1. Set up the tracer provider
exporter = InMemorySpanExporter()   # In production: use OTLP exporter
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("order-service", "1.0.0")

# 2. Create a root span for an incoming request
def handle_order_request(order_id: str, user_id: str):
    with tracer.start_as_current_span(
        "POST /orders",
        kind=SpanKind.SERVER,
        attributes={
            "http.method": "POST",
            "http.route": "/orders",
            "order.id": order_id,
            "user.id": user_id,
        }
    ) as root_span:
        try:
            result = process_order(order_id)
            root_span.set_status(Status(StatusCode.OK))
            return result
        except Exception as e:
            root_span.set_status(Status(StatusCode.ERROR, str(e)))
            root_span.record_exception(e)
            raise

def process_order(order_id: str):
    # 3. Create a child span for the DB query
    with tracer.start_as_current_span(
        "db.query inventory",
        kind=SpanKind.CLIENT,
        attributes={
            "db.system": "postgresql",
            "db.operation": "SELECT",
            "db.statement": f"SELECT * FROM inventory WHERE order_id='{order_id}'",
        }
    ) as db_span:
        # Simulate DB query
        import time
        time.sleep(0.035)   # 35ms query
        db_span.set_attribute("db.rows_returned", 1)

    # 4. Create a child span for Kafka publish
    with tracer.start_as_current_span(
        "kafka.produce order-events",
        kind=SpanKind.PRODUCER,
        attributes={
            "messaging.system": "kafka",
            "messaging.destination": "order-events",
            "messaging.destination_kind": "topic",
            "messaging.kafka.partition": 3,
        }
    ):
        import time
        time.sleep(0.005)   # 5ms Kafka produce

    return {"status": "processed", "order_id": order_id}

# 5. Context propagation — extract from incoming headers
def handle_with_propagation(headers: dict, order_id: str):
    propagator = TraceContextTextMapPropagator()
    ctx = propagator.extract(carrier=headers)

    with tracer.start_as_current_span(
        "POST /orders",
        context=ctx,       # Parent context from upstream service
        kind=SpanKind.SERVER,
    ):
        return process_order(order_id)
```

### 4.2 Querying Distributed Traces in Jaeger

```bash
# Start Jaeger all-in-one for local development
docker run -d --name jaeger \
  -p 16686:16686 \    # Jaeger UI
  -p 4317:4317 \      # OTLP gRPC
  -p 4318:4318 \      # OTLP HTTP
  jaegertracing/all-in-one:latest

# Configure Python exporter to send to Jaeger
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

# Query Jaeger UI at http://localhost:16686
# - Service: order-service
# - Operation: POST /orders
# - Min Duration: 1s (to find slow requests)
# - Tags: error=true (to find error traces)

# Jaeger HTTP API queries:
curl "http://localhost:16686/api/traces?service=order-service&operation=POST+/orders&minDuration=500ms&limit=20"
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
distributed_observability.py

A complete observability implementation for distributed data pipelines.

Components:
  - TraceContext: W3C trace context (trace_id, span_id, parent_span_id)
  - Span: A unit of traced work
  - Tracer: Creates and manages spans
  - TracePropagator: Injects/extracts trace context into/from carrier dicts
  - StructuredLogger: JSON-format logs with trace correlation
  - MetricsCollector: Simple Prometheus-style counters, gauges, histograms
  - PipelineInstrumentation: Wraps a data pipeline with full observability

No external dependencies (no opentelemetry SDK required).
Produces output compatible with Jaeger's expected data model.
"""

import hashlib
import json
import os
import random
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


# ─── Trace Context (W3C Trace Context Standard) ──────────────────────────────────

def _random_hex(n_bytes: int) -> str:
    return os.urandom(n_bytes).hex()


@dataclass
class TraceContext:
    """
    W3C Trace Context: the minimal information needed to link spans across services.
    https://www.w3.org/TR/trace-context/
    """
    trace_id:       str              # 16 bytes / 32 hex chars — unique per trace
    span_id:        str              # 8 bytes / 16 hex chars — unique per span
    parent_span_id: Optional[str]    # span_id of the parent span (None for root)
    sampled:        bool = True      # Whether this trace is being collected

    @classmethod
    def new_root(cls, sampled: bool = True) -> 'TraceContext':
        """Create a new root context (for the first span in a trace)."""
        return cls(
            trace_id=_random_hex(16),
            span_id=_random_hex(8),
            parent_span_id=None,
            sampled=sampled,
        )

    @classmethod
    def child_of(cls, parent: 'TraceContext') -> 'TraceContext':
        """Create a new context as a child of the given parent context."""
        return cls(
            trace_id=parent.trace_id,
            span_id=_random_hex(8),
            parent_span_id=parent.span_id,
            sampled=parent.sampled,
        )

    def to_traceparent(self) -> str:
        """Serialize to W3C traceparent header value."""
        flags = "01" if self.sampled else "00"
        return f"00-{self.trace_id}-{self.span_id}-{flags}"

    @classmethod
    def from_traceparent(cls, header: str) -> Optional['TraceContext']:
        """Parse W3C traceparent header. Returns None if invalid."""
        try:
            version, trace_id, span_id, flags = header.split("-")
            return cls(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=span_id,   # The sender's span_id is our parent
                sampled=(flags == "01"),
            )
        except (ValueError, AttributeError):
            return None


# ─── Span ────────────────────────────────────────────────────────────────────────

class SpanStatus(Enum):
    UNSET = "UNSET"
    OK    = "OK"
    ERROR = "ERROR"


class SpanKind(Enum):
    INTERNAL = "INTERNAL"   # Internal to a service
    SERVER   = "SERVER"     # Receiving an inbound RPC
    CLIENT   = "CLIENT"     # Sending an outbound RPC
    PRODUCER = "PRODUCER"   # Sending to a message queue
    CONSUMER = "CONSUMER"   # Consuming from a message queue


@dataclass
class SpanEvent:
    """A timestamped event attached to a span (structured log entry)."""
    timestamp_ms: float
    name:         str
    attributes:   dict = field(default_factory=dict)


@dataclass
class Span:
    """
    A single unit of traced work. Represents one operation in one service.
    """
    name:           str
    context:        TraceContext
    kind:           SpanKind = SpanKind.INTERNAL
    start_ms:       float = field(default_factory=lambda: time.monotonic() * 1000)
    end_ms:         Optional[float] = None
    attributes:     dict = field(default_factory=dict)
    events:         list[SpanEvent] = field(default_factory=list)
    status:         SpanStatus = SpanStatus.UNSET
    status_message: str = ""
    service_name:   str = "unknown-service"

    def set_attribute(self, key: str, value: Any):
        self.attributes[key] = value

    def set_status(self, status: SpanStatus, message: str = ""):
        self.status = status
        self.status_message = message

    def add_event(self, name: str, attributes: dict = None):
        self.events.append(SpanEvent(
            timestamp_ms=time.monotonic() * 1000,
            name=name,
            attributes=attributes or {},
        ))

    def record_exception(self, exception: Exception):
        self.set_status(SpanStatus.ERROR, str(exception))
        self.add_event("exception", {
            "exception.type": type(exception).__name__,
            "exception.message": str(exception),
        })

    def finish(self):
        self.end_ms = time.monotonic() * 1000

    @property
    def duration_ms(self) -> float:
        if self.end_ms is None:
            return 0.0
        return self.end_ms - self.start_ms

    def to_dict(self) -> dict:
        """Serialize span to a Jaeger-compatible dict."""
        return {
            "traceID": self.context.trace_id,
            "spanID": self.context.span_id,
            "parentSpanID": self.context.parent_span_id,
            "operationName": self.name,
            "serviceName": self.service_name,
            "startTime": self.start_ms * 1000,   # microseconds for Jaeger
            "duration": self.duration_ms * 1000,  # microseconds
            "kind": self.kind.value,
            "status": self.status.value,
            "statusMessage": self.status_message,
            "attributes": self.attributes,
            "events": [
                {
                    "timestamp": e.timestamp_ms * 1000,
                    "name": e.name,
                    "attributes": e.attributes,
                }
                for e in self.events
            ],
        }


# ─── Tracer ───────────────────────────────────────────────────────────────────────

class SpanExporter:
    """Base class for span exporters. Override export() to send to a backend."""
    def export(self, span: Span):
        pass


class InMemorySpanExporter(SpanExporter):
    """Collects spans in memory for testing and inspection."""
    def __init__(self):
        self._spans: list[Span] = []
        self._lock = threading.Lock()

    def export(self, span: Span):
        with self._lock:
            self._spans.append(span)

    def get_spans(self) -> list[Span]:
        with self._lock:
            return list(self._spans)

    def get_trace(self, trace_id: str) -> list[Span]:
        with self._lock:
            return [s for s in self._spans if s.context.trace_id == trace_id]

    def clear(self):
        with self._lock:
            self._spans.clear()


class ConsoleSpanExporter(SpanExporter):
    """Prints finished spans to stdout (for development)."""
    def export(self, span: Span):
        status_str = f" [{span.status.value}]" if span.status != SpanStatus.UNSET else ""
        print(f"  [TRACE] {span.service_name} | {span.name}{status_str} "
              f"| trace={span.context.trace_id[:8]}... "
              f"| span={span.context.span_id[:8]}... "
              f"| parent={span.context.parent_span_id[:8] if span.context.parent_span_id else 'none'} "
              f"| {span.duration_ms:.2f}ms")


_local_context = threading.local()


class Tracer:
    """
    Creates and manages spans. Maintains a per-thread span stack for context propagation.
    """

    def __init__(self, service_name: str, exporters: list[SpanExporter] = None,
                 sample_rate: float = 1.0):
        self.service_name = service_name
        self._exporters = exporters or [ConsoleSpanExporter()]
        self._sample_rate = sample_rate

    def _should_sample(self, parent_context: Optional[TraceContext] = None) -> bool:
        if parent_context is not None:
            return parent_context.sampled   # Respect parent's sampling decision
        return random.random() < self._sample_rate

    def _current_span(self) -> Optional[Span]:
        stack = getattr(_local_context, 'span_stack', [])
        return stack[-1] if stack else None

    def _push_span(self, span: Span):
        if not hasattr(_local_context, 'span_stack'):
            _local_context.span_stack = []
        _local_context.span_stack.append(span)

    def _pop_span(self) -> Optional[Span]:
        stack = getattr(_local_context, 'span_stack', [])
        return stack.pop() if stack else None

    @contextmanager
    def start_span(self, name: str, kind: SpanKind = SpanKind.INTERNAL,
                   parent_context: Optional[TraceContext] = None,
                   attributes: dict = None):
        """
        Context manager that creates a span, makes it current, and finishes it on exit.
        If a span is already active (in the thread-local stack), the new span is a child.
        """
        # Determine parent
        current = self._current_span()
        if parent_context is not None:
            # Explicit parent (from incoming request headers)
            ctx = TraceContext.child_of(parent_context)
            ctx.sampled = self._should_sample(parent_context)
        elif current is not None:
            # Implicit parent from thread-local stack
            ctx = TraceContext.child_of(current.context)
        else:
            # New root span
            sampled = self._should_sample()
            if sampled:
                ctx = TraceContext.new_root(sampled=True)
            else:
                # Not sampled — create a no-op span
                ctx = TraceContext.new_root(sampled=False)

        span = Span(
            name=name,
            context=ctx,
            kind=kind,
            attributes=attributes or {},
            service_name=self.service_name,
        )

        self._push_span(span)
        try:
            yield span
        except Exception as e:
            span.record_exception(e)
            raise
        finally:
            span.finish()
            self._pop_span()
            if ctx.sampled:
                for exporter in self._exporters:
                    exporter.export(span)

    def inject(self, carrier: dict, span: Optional[Span] = None) -> dict:
        """
        Inject trace context into a carrier dict (for HTTP headers / Kafka message headers).
        If span is not provided, uses the current active span.
        """
        s = span or self._current_span()
        if s and s.context.sampled:
            carrier["traceparent"] = s.context.to_traceparent()
        return carrier

    def extract(self, carrier: dict) -> Optional[TraceContext]:
        """
        Extract trace context from carrier dict (incoming request headers).
        Returns None if no valid context found.
        """
        header = carrier.get("traceparent")
        if not header:
            return None
        return TraceContext.from_traceparent(header)


# ─── Structured Logger ────────────────────────────────────────────────────────────

import sys

class StructuredLogger:
    """
    Emits structured (JSON) log lines with automatic trace correlation.
    Integrates with the Tracer to include trace_id and span_id in every log line.
    """

    LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}

    def __init__(self, service_name: str, tracer: Optional[Tracer] = None,
                 min_level: str = "INFO", output=sys.stdout):
        self.service_name = service_name
        self.tracer = tracer
        self.min_level_value = self.LEVELS.get(min_level, 20)
        self._output = output

    def _emit(self, level: str, event: str, **kwargs):
        if self.LEVELS.get(level, 0) < self.min_level_value:
            return

        record = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.") +
                         f"{int(time.time() * 1000) % 1000:03d}Z",
            "level": level,
            "service": self.service_name,
            "event": event,
        }

        # Add trace correlation if a span is active
        if self.tracer:
            current_span = self.tracer._current_span()
            if current_span and current_span.context.sampled:
                record["trace_id"] = current_span.context.trace_id
                record["span_id"] = current_span.context.span_id

        record.update(kwargs)
        print(json.dumps(record), file=self._output)

    def debug(self, event: str, **kwargs):    self._emit("DEBUG", event, **kwargs)
    def info(self, event: str, **kwargs):     self._emit("INFO", event, **kwargs)
    def warning(self, event: str, **kwargs):  self._emit("WARNING", event, **kwargs)
    def error(self, event: str, **kwargs):    self._emit("ERROR", event, **kwargs)
    def critical(self, event: str, **kwargs): self._emit("CRITICAL", event, **kwargs)


# ─── Metrics Collector ────────────────────────────────────────────────────────────

class MetricsCollector:
    """
    Simple Prometheus-compatible metrics collection.
    Implements counters, gauges, and histograms.
    """

    def __init__(self, service_name: str):
        self.service_name = service_name
        self._counters:   dict[str, float] = {}
        self._gauges:     dict[str, float] = {}
        self._histograms: dict[str, list[float]] = {}
        self._lock = threading.Lock()

        # Standard Prometheus histogram buckets (in ms)
        self.LATENCY_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]

    def increment(self, name: str, value: float = 1.0, labels: dict = None):
        """Increment a counter."""
        key = self._make_key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def set_gauge(self, name: str, value: float, labels: dict = None):
        """Set a gauge to an absolute value."""
        key = self._make_key(name, labels)
        with self._lock:
            self._gauges[key] = value

    def observe(self, name: str, value_ms: float, labels: dict = None):
        """Record a latency observation in milliseconds."""
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._histograms:
                self._histograms[key] = []
            self._histograms[key].append(value_ms)

    def _make_key(self, name: str, labels: dict = None) -> str:
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def get_percentile(self, name: str, percentile: float, labels: dict = None) -> Optional[float]:
        """Compute a percentile from histogram data."""
        key = self._make_key(name, labels)
        with self._lock:
            values = self._histograms.get(key, [])
        if not values:
            return None
        sorted_values = sorted(values)
        idx = int(len(sorted_values) * percentile / 100)
        return sorted_values[min(idx, len(sorted_values) - 1)]

    def print_summary(self):
        print(f"\n  === Metrics: {self.service_name} ===")
        with self._lock:
            for name, value in sorted(self._counters.items()):
                print(f"  counter {name}: {value:.0f}")
            for name, value in sorted(self._gauges.items()):
                print(f"  gauge   {name}: {value:.2f}")
            for name, values in sorted(self._histograms.items()):
                if values:
                    sorted_v = sorted(values)
                    p50 = sorted_v[len(sorted_v) // 2]
                    p99 = sorted_v[min(int(len(sorted_v) * 0.99), len(sorted_v)-1)]
                    print(f"  histo   {name}: "
                          f"count={len(values)} p50={p50:.1f}ms p99={p99:.1f}ms")


# ─── Pipeline Instrumentation ─────────────────────────────────────────────────────

class InstrumentedPipeline:
    """
    Wraps a data pipeline stage with full observability:
    traces, structured logs, and metrics — all correlated.

    This demonstrates the three-pillar pattern in practice.
    """

    def __init__(self, service_name: str):
        self.exporter = InMemorySpanExporter()
        self.tracer = Tracer(
            service_name=service_name,
            exporters=[self.exporter, ConsoleSpanExporter()],
        )
        self.logger = StructuredLogger(service_name, tracer=self.tracer)
        self.metrics = MetricsCollector(service_name)

    def process_kafka_message(self, message: dict,
                               headers: dict = None) -> Optional[dict]:
        """
        Process a Kafka message with full observability.
        Extracts trace context from message headers (if present).
        Creates a CONSUMER span as a child of the producer's span.
        """
        headers = headers or {}
        parent_ctx = self.tracer.extract(headers)
        start = time.monotonic()

        with self.tracer.start_span(
            "kafka.consume",
            kind=SpanKind.CONSUMER,
            parent_context=parent_ctx,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination": message.get("topic", "unknown"),
                "messaging.kafka.partition": message.get("partition", 0),
                "messaging.kafka.offset": message.get("offset", 0),
            }
        ) as span:
            try:
                self.logger.info("processing_message",
                                  topic=message.get("topic"),
                                  key=message.get("key"))

                # Simulate processing
                result = self._transform(message.get("value", {}), span)

                duration_ms = (time.monotonic() - start) * 1000
                self.metrics.increment("messages_processed_total",
                                        labels={"topic": message.get("topic")})
                self.metrics.observe("message_processing_duration_ms", duration_ms,
                                      labels={"topic": message.get("topic")})

                span.set_status(SpanStatus.OK)
                self.logger.info("message_processed",
                                  duration_ms=round(duration_ms, 2))
                return result

            except Exception as e:
                duration_ms = (time.monotonic() - start) * 1000
                self.metrics.increment("messages_failed_total",
                                        labels={"topic": message.get("topic"),
                                                "error": type(e).__name__})
                self.logger.error("message_processing_failed",
                                   error=str(e),
                                   duration_ms=round(duration_ms, 2))
                return None

    def _transform(self, data: dict, parent_span: Span) -> dict:
        """Inner transformation with its own span."""
        with self.tracer.start_span(
            "transform.enrich",
            kind=SpanKind.INTERNAL,
            attributes={"input.keys": list(data.keys())},
        ) as span:
            time.sleep(0.002)   # 2ms processing
            result = {k: v.upper() if isinstance(v, str) else v
                      for k, v in data.items()}
            span.set_attribute("output.keys", list(result.keys()))
            span.set_status(SpanStatus.OK)
            return result

    def produce_kafka_message(self, topic: str, key: str,
                               value: dict) -> dict:
        """
        Produce a Kafka message with trace context injection.
        The trace context is included in message headers,
        allowing downstream consumers to continue the trace.
        """
        with self.tracer.start_span(
            "kafka.produce",
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination": topic,
                "messaging.kafka.key": key,
            }
        ):
            headers = {}
            self.tracer.inject(headers)   # Inject current span's context
            time.sleep(0.003)   # 3ms Kafka produce
            self.metrics.increment("messages_produced_total",
                                    labels={"topic": topic})

            return {
                "topic": topic,
                "key": key,
                "value": value,
                "headers": headers,   # Contains traceparent header
            }


# ─── Demo ─────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Distributed Observability Demo ===\n")

    # ── 1. Basic Tracing ──────────────────────────────────────────────────────────
    print("─── Phase 1: Basic Span Creation ───")
    exporter = InMemorySpanExporter()
    tracer = Tracer("order-service", exporters=[exporter, ConsoleSpanExporter()])

    with tracer.start_span("POST /orders", kind=SpanKind.SERVER,
                           attributes={"http.method": "POST",
                                       "order.id": "order-123"}) as root:
        time.sleep(0.010)

        with tracer.start_span("db.query inventory", kind=SpanKind.CLIENT,
                               attributes={"db.system": "postgresql"}):
            time.sleep(0.035)   # Slow query

        with tracer.start_span("kafka.produce", kind=SpanKind.PRODUCER,
                               attributes={"messaging.system": "kafka"}):
            time.sleep(0.005)

    spans = exporter.get_spans()
    print(f"\n  Spans in trace: {len(spans)}")
    trace_id = spans[0].context.trace_id
    for span in exporter.get_trace(trace_id):
        indent = "    " if span.context.parent_span_id else "  "
        print(f"{indent}[{span.name}] {span.duration_ms:.2f}ms "
              f"status={span.status.value}")

    # ── 2. Context Propagation ────────────────────────────────────────────────────
    print("\n─── Phase 2: Cross-Service Context Propagation ───")
    service_a_exporter = InMemorySpanExporter()
    service_a = Tracer("service-a", [service_a_exporter, ConsoleSpanExporter()])
    service_b_exporter = InMemorySpanExporter()
    service_b = Tracer("service-b", [service_b_exporter, ConsoleSpanExporter()])

    with service_a.start_span("GET /upstream", kind=SpanKind.SERVER) as span_a:
        # Service A calls Service B — inject trace context
        headers = {}
        service_a.inject(headers, span_a)
        print(f"\n  Propagated header: {headers.get('traceparent', 'NONE')}")

        # Service B receives request — extract and continue trace
        parent_ctx = service_b.extract(headers)
        with service_b.start_span("GET /downstream", kind=SpanKind.SERVER,
                                  parent_context=parent_ctx) as span_b:
            time.sleep(0.010)

    # Verify same trace_id across both services
    spans_a = service_a_exporter.get_spans()
    spans_b = service_b_exporter.get_spans()
    print(f"\n  service-a trace_id: {spans_a[0].context.trace_id[:16]}...")
    print(f"  service-b trace_id: {spans_b[0].context.trace_id[:16]}...")
    print(f"  Same trace? {spans_a[0].context.trace_id == spans_b[0].context.trace_id}")
    print(f"  service-b parent = service-a span? "
          f"{spans_b[0].context.parent_span_id == spans_a[0].context.span_id}")

    # ── 3. Pipeline Observability ─────────────────────────────────────────────────
    print("\n─── Phase 3: Instrumented Pipeline ───")
    pipeline = InstrumentedPipeline("transform-service")

    # Produce messages with trace context
    messages = []
    for i in range(5):
        msg = pipeline.produce_kafka_message(
            topic="orders-raw",
            key=f"order-{i}",
            value={"order_id": f"order-{i}", "status": "pending"},
        )
        messages.append(msg)

    print(f"\n  Produced {len(messages)} messages. Processing...")

    # Consume with trace context extraction (continues the trace)
    for i, msg in enumerate(messages):
        pipeline.process_kafka_message(
            message={
                "topic": "orders-raw",
                "key": msg["key"],
                "value": msg["value"],
                "partition": i % 3,
                "offset": i,
            },
            headers=msg["headers"],   # Contains traceparent — continues trace
        )

    # ── 4. Metrics Summary ────────────────────────────────────────────────────────
    print("\n─── Phase 4: Metrics Summary ───")
    pipeline.metrics.print_summary()

    # ── 5. Structured Log Sample ──────────────────────────────────────────────────
    print("\n─── Phase 5: Structured Logging ───")
    logger = StructuredLogger("order-service", tracer=service_a)
    with service_a.start_span("process_batch") as span:
        logger.info("batch_started", batch_size=100)
        time.sleep(0.001)
        logger.warning("slow_query_detected", query_ms=450, threshold_ms=200)
        logger.info("batch_completed", processed=100, failed=0)

    # ── 6. Trace Visualization ────────────────────────────────────────────────────
    print("\n─── Phase 6: Trace Structure ───")
    all_spans = pipeline.exporter.get_spans()
    # Group by trace_id
    traces: dict[str, list[Span]] = {}
    for span in all_spans:
        tid = span.context.trace_id
        traces.setdefault(tid, []).append(span)

    print(f"  Total traces captured: {len(traces)}")
    for i, (tid, trace_spans) in enumerate(list(traces.items())[:2]):
        print(f"\n  Trace {i+1}: {tid[:16]}...")
        for span in sorted(trace_spans, key=lambda s: s.start_ms):
            indent = "    " if span.context.parent_span_id else "  "
            print(f"  {indent}[{span.name}] {span.duration_ms:.2f}ms "
                  f"({span.service_name})")

    print("\n✓ Distributed observability demo complete")
```

---

## 6. Visual Reference

### Distributed Trace Flow

```
[User] → POST /api/orders
           │
           ▼ Trace: abc123def456...
    ┌─────────────────────────────────────────────────────┐
    │ Span 1: POST /api/orders [api-gateway] 0-1200ms     │
    │  traceparent: 00-abc123...-span001...-01             │
    └───────────────────┬─────────────────────────────────┘
                        │ HTTP header: traceparent=00-abc123...-span001...-01
                        ▼
    ┌─────────────────────────────────────────────────────┐
    │ Span 2: /orders [order-service] 10-900ms            │
    │  parent: span001, new span_id: span002               │
    └────────┬────────────────┬───────────────────────────┘
             │                │
             ▼                ▼
    ┌──────────────┐  ┌─────────────────────────────────┐
    │ Span 3: DB   │  │ Span 4: gRPC to payment-service │
    │ [order-svc]  │  │ [order-service] 50-400ms         │
    │ 10-45ms      │  │  traceparent injected in header  │
    │ SLOW: 35ms   │  └─────────────┬───────────────────┘
    └──────────────┘                │
                                    ▼
                        ┌─────────────────────────────────┐
                        │ Span 5: charge [payment-service]│
                        │ parent: span004, 55-395ms        │
                        └─────────────────────────────────┘

Jaeger UI shows waterfall chart:
  Span 1:  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1200ms
  Span 2:     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 890ms
  Span 3:       ━━━ 35ms  ← shows as "slow DB query"
  Span 4:          ━━━━━━━━━━━━━━━━━━━━━━━━━ 350ms
  Span 5:            ━━━━━━━━━━━━━━━━━━━━━━━ 340ms
```

### The Three Pillars — When to Use Each

```
Question: "Why is the P99 latency up?"
  Metrics → "P99 for /api/orders is 1200ms (was 50ms)"

Question: "Which service is slow?"
  Traces → Filter by operation=POST /api/orders, min_duration=500ms
            → Find that Span 5 [payment-service] is 340ms (was 10ms)

Question: "What happened in payment-service?"
  Logs → filter by trace_id=abc123... or service=payment-service, time=12:00±5min
          → Find: "2026-06-28T12:00:01 ERROR external_api_timeout api=stripe latency=350ms"

Each pillar answers a different question. Together: full incident diagnosis.
```

### OpenTelemetry Architecture

```
Your Application
  │
  │ opentelemetry-sdk
  ▼
[TracerProvider]
  │ BatchSpanProcessor
  ▼
[OTel Collector] ←── receives OTLP (gRPC/HTTP)
  │
  ├── filter: drop health checks
  ├── sample: keep 100% errors, 1% success
  ├── enrich: add k8s_pod, k8s_namespace
  │
  ├──── Jaeger/Tempo (traces)
  ├──── Prometheus (metrics)
  └──── Loki/Elasticsearch (logs)
```

---

## 7. Common Mistakes

**Mistake 1: Not propagating trace context through message queues.** When a Kafka producer creates a message, it should inject the current trace context into the message headers. When the consumer reads the message, it should extract the trace context and use it as the parent for its span. Without this, the producer's trace and the consumer's trace are completely disconnected — you see the producer's span end and the consumer's span start independently, with no visible connection. You can't tell which producer caused which consumer processing. This is the most common observability gap in data pipeline systems.

**Mistake 2: Sampling only at the edge, not at the collector.** Many teams set a 1% sample rate at the producer (application) level. This means 99% of traces — including the occasional 1-second outlier — are never recorded. Head-based sampling at the application makes this irreversible: once a trace is dropped at the producer, it's gone. The correct pattern: record all spans, send to an OTel Collector, and use tail-based sampling at the collector (keep all error traces + 1% of success traces). This ensures high-latency and error traces are always captured.

**Mistake 3: Using span names with high cardinality.** Span names are used for aggregation in trace backends (Jaeger, Tempo). A span named `db.query SELECT * FROM orders WHERE id='order-12345'` has unbounded cardinality — every unique order ID creates a new operation name in the backend's index. Correctly: `db.query orders` (operation + table), with the full query in a span attribute (`db.statement`). Attribute values can have high cardinality; span names should not.

---

## 8. Production Failure Scenarios

### Scenario 1: Trace Context Lost at Kafka Boundary

**Symptoms:** Engineers can trace requests from the API gateway to the order service perfectly. But when the order service produces a Kafka message and the inventory service consumes it, the inventory service spans appear as separate, disconnected traces in Jaeger. Engineers can't determine which API request triggered which inventory update.

**Root cause:** The order service was using a custom Kafka producer wrapper that did not include the message headers. The trace context was injected into a `headers` dict in the application code, but the wrapper's `produce()` method dropped the headers before calling the underlying Kafka client.

**Fix:** Ensure the Kafka producer wrapper passes headers through to the underlying client:
```python
# Inject trace context into Kafka message headers
headers = {}
tracer.inject(headers)
kafka_producer.produce(
    topic="order-events",
    key=order_id,
    value=event_payload,
    headers=list(headers.items()),   # Pass headers to Kafka
)
```

**Metric signature:** 100% of inventory service spans show `parentSpanID=null` (root spans) in Jaeger — all consumer spans are disconnected roots. Should be near 0% if trace context is propagated correctly.

### Scenario 2: Cardinality Explosion in Prometheus from Trace ID Labels

**Symptoms:** The Prometheus database grew from 2GB to 200GB over 3 days. Grafana queries became slow, then timed out. The Prometheus server's memory usage exceeded its limit and the process was OOM-killed.

**Root cause:** A developer added a `trace_id` label to a Prometheus counter metric, with the intention of being able to correlate metrics to traces:
```python
# BAD: every request has a unique trace_id → unbounded cardinality
counter.labels(service="order-service", trace_id=current_trace_id).inc()
```

Prometheus stores one time series per unique label combination. With a new `trace_id` (32 hex chars, unique per request) for every request, at 10,000 requests/second, 864 million new time series were created per day. Prometheus was not designed for this cardinality.

**Fix:** Remove `trace_id` from metric labels. Trace IDs belong in span attributes (traces), not in metric labels. Metrics use low-cardinality labels (service, operation, status_code, region). Use the trace ID in log lines and span attributes to correlate between metrics and traces — but not in Prometheus labels:
```python
# CORRECT: low-cardinality labels only
counter.labels(service="order-service", operation="process_order", status="ok").inc()
# Log the trace_id separately:
logger.info("order_processed", trace_id=current_trace_id)
```

---

## 9. Performance and Tuning

### OTel Sampling Configuration

```python
# OpenTelemetry Collector tail-based sampling config (collector.yaml)
# This is the recommended pattern for production:
processors:
  tail_sampling:
    decision_wait: 10s      # Wait 10s for all spans to arrive before deciding
    num_traces: 100000      # Max number of traces to buffer
    expected_new_traces_per_sec: 1000
    policies:
      # Always keep error traces
      - name: errors-policy
        type: status_code
        status_code: {status_codes: [ERROR]}

      # Always keep slow traces (>500ms)
      - name: slow-policy
        type: latency
        latency: {threshold_ms: 500}

      # Sample 1% of everything else
      - name: default-policy
        type: probabilistic
        probabilistic: {sampling_percentage: 1}
```

### Span Export Batching

```python
# BatchSpanProcessor configuration — avoid per-span I/O
from opentelemetry.sdk.trace.export import BatchSpanProcessor

processor = BatchSpanProcessor(
    exporter,
    max_queue_size=2048,         # Max spans in queue before dropping
    max_export_batch_size=512,   # Spans per export request
    export_timeout_millis=30000, # 30s export timeout
    schedule_delay_millis=5000,  # Export every 5s (or when queue is full)
)
# Default values are reasonable for most applications
# Tune max_queue_size if high-throughput spans are being dropped
```

---

## 10. Interview Q&A

**Q1: What are the three pillars of observability and what question does each answer?**

The three pillars are metrics, logs, and traces. Each answers a different question at a different level of granularity.

Metrics answer: "Is something wrong, and how wrong?" They are aggregated numerical measurements — request rate, error rate, P99 latency, queue depth. Metrics are cheap to store (one time series per label combination) and fast to query, but they are lossy: you know the P99 is 1200ms but not which specific request took 1200ms or what happened inside it. Metrics are the alerting layer.

Logs answer: "What happened inside this service?" They are timestamped records of discrete events — a database query returned an error, a retry was attempted, a circuit breaker opened. Logs have full detail about a specific event but are per-service: a log line in the order service doesn't automatically connect to a log line in the payment service for the same request. Without a common identifier (trace_id), correlating logs across services is done by matching timestamps — imprecise and slow.

Traces answer: "How did this specific request flow through all services?" A trace is a tree of spans, one per service (or per operation within a service), all sharing a common trace_id. Traces show the causal chain — which service called which other service, in what order, with what latency at each step. When the P99 alert fires (metrics), you find a slow trace (traces), then look at the logs for the slow span's service (logs). All three together provide complete incident diagnosis that none provides alone.

**Q2: Explain W3C Trace Context propagation. What exactly is in the header, and what does each field mean?**

W3C Trace Context defines a standard HTTP header called `traceparent`. Its format is:
`00-<trace-id>-<parent-id>-<flags>`

- `00`: the spec version (always 00 currently)
- `trace-id`: 16-byte (32 hex char) globally unique identifier for the entire distributed trace — all spans in a single end-to-end request share this ID
- `parent-id`: 8-byte (16 hex char) identifier of the calling span — the span in the upstream service that initiated this call
- `flags`: a single byte, with the low bit being the "sampled" flag (01 = this trace is sampled and should be recorded, 00 = not sampled)

When service A calls service B, A injects the `traceparent` header with its current span's trace_id and span_id (as parent-id). Service B extracts this header, creates its own span with the same trace_id and sets its parent_span_id to the extracted parent-id. This creates the parent-child relationship in the trace tree. Without the header, service B would create a new root trace — disconnected from A's trace. The trace_id is what connects all the spans in a single request across all services.

---

## 11. Cross-Question Chain

**Interviewer:** Your Kafka pipeline has a producer, a transformer, and a consumer. How would you implement distributed tracing across all three?

**Candidate:** Three steps. First, instrument the producer: when it writes to Kafka, inject the current trace context into the Kafka message headers. OpenTelemetry's Kafka instrumentation does this with `traceparent` in the message headers. The span created for the produce operation is kind=PRODUCER.

Second, instrument the transformer: when it reads the message, extract the `traceparent` header from the message and use it as the parent context for its span. The span created for the consume + transform is kind=CONSUMER with the producer's span as parent. The transformer then produces a new message to another topic — inject the (now updated) trace context again.

Third, instrument the consumer: same pattern — extract from the message headers, create a CONSUMER span as a child of the transformer's PRODUCER span.

The result: a single trace shows the entire pipeline path — from producer to transformer to consumer — as one connected tree of spans, all sharing the same trace_id.

**Interviewer:** What if there's a long delay between the transformer producing and the consumer reading — say, 30 seconds?

**Candidate:** The trace is still valid — there's no timeout on a trace's open span tree. The consumer's span will show a "parent" that started 30 seconds ago. In Jaeger or Tempo, you'll see the trace timeline shows a 30-second gap between the transformer's PRODUCER span and the consumer's CONSUMER span. This is actually useful: it shows the message was queued for 30 seconds, which you can correlate with Kafka consumer lag metrics.

The practical issue is tail-based sampling with a decision_wait of 10 seconds — if the trace takes 30 seconds end-to-end, the collector might have already made a sampling decision before the consumer span arrives. For long-running pipelines, increase `decision_wait` to accommodate the maximum expected end-to-end latency, or use a separate trace for each pipeline stage (linked via a Span Link rather than a parent-child relationship).

**Interviewer:** How would you correlate a slow P99 metric alert with the specific spans responsible?

**Candidate:** The metric alert fires: "P99 latency for kafka_consumer_duration_ms > 5000ms." The metric tells me something is slow but not which messages or why. Three steps to diagnose.

First, in Jaeger (or Tempo), query for traces with operation=kafka.consume, min_duration=5s, time=alert window. This surfaces the actual slow traces. Examine the waterfall — which child span dominates the duration? That identifies the bottleneck operation.

Second, find the trace_id of a slow trace. Look up the logs for that trace_id in the logging system (Loki or Elasticsearch): `trace_id:<slow_trace_id>`. The log lines show exactly what happened: was it a retry? A slow DB query? An external API call?

Third, if the root cause is in a child service, follow the parent_span_id chain across services. The trace shows the whole path; you pick the slow span's service and look at its logs.

The key enabler is trace_id in every log line — without it, you'd need to match timestamps across services manually, which is imprecise and slow. With trace_id in logs, you go from "the metric alert fired" to "the root cause is in the external payments API response time" in under 5 minutes.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What are the three pillars of observability and what does each answer? | Metrics: "Is something wrong?" (aggregates). Logs: "What happened in this service?" (event records). Traces: "How did this request flow through all services?" (causal spans). |
| 2 | What is a trace? | A directed acyclic graph (tree) of spans, all sharing the same trace_id. Represents the end-to-end journey of one request through a distributed system. |
| 3 | What is a span? | A single unit of traced work: one operation in one service. Contains: name, trace_id, span_id, parent_span_id, start/end time, attributes, status, events. |
| 4 | What is the W3C traceparent header format? | `00-<trace_id (32 hex)>-<parent_span_id (16 hex)>-<flags (01=sampled)>`. Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`. |
| 5 | What is trace context propagation and why is it necessary? | The act of injecting the current span's trace_id and span_id into outgoing requests (HTTP headers, Kafka message headers), and extracting them from incoming requests to create child spans. Without it, spans are disconnected islands — no cross-service trace. |
| 6 | What is head-based vs tail-based sampling? | Head-based: sampling decision at trace start (before knowing if trace is interesting). Tail-based: all spans buffered, decision made at trace completion (keep errors/slow traces, drop fast successes). Tail-based is better for capturing rare slow/error traces. |
| 7 | What is the cardinality problem in Prometheus metrics? | If metric labels contain high-cardinality values (trace_id, user_id, order_id), Prometheus creates one time series per unique label combination → millions of time series → memory exhaustion. Metric labels must be low-cardinality (status, service, operation). |
| 8 | What does SpanKind.PRODUCER vs CONSUMER indicate? | PRODUCER: span for the act of publishing a message to a queue/topic. CONSUMER: span for the act of consuming and processing a message. Together they model message queue boundaries in a trace. |
| 9 | What is OpenTelemetry Collector used for? | Receives telemetry from applications (OTLP protocol), processes it (filter, sample, enrich), and exports to multiple backends (Jaeger, Prometheus, Loki). Decouples application instrumentation from backend choice. |
| 10 | How do you correlate a log line with its trace? | Include `trace_id` and `span_id` as fields in every structured log record. When debugging, filter logs by `trace_id=<id-from-Jaeger>`. This jumps from trace to logs in one step. |
| 11 | What is the OTel Semantic Convention for a database span? | db.system (e.g., "postgresql"), db.operation ("SELECT"), db.name (database name), db.statement (query text), db.rows_affected. Allows Jaeger to display DB spans with full query context. |
| 12 | What is a Span Link? | A reference from one span to another span, possibly in a different trace. Used for async work like Kafka consumers (the consumer span links to the producer span) or batch jobs (links to the triggering event). Different from parent-child (which is synchronous causality). |
| 13 | What should you do differently for a long-running pipeline (30-second E2E) vs a short request (50ms)? | Increase tail_sampling `decision_wait` to accommodate maximum E2E latency. Consider using Span Links (not parent-child) between producer and consumer spans to avoid holding trace context for 30 seconds. Use separate trace IDs per pipeline stage if the end-to-end duration is too long for tail sampling to handle. |
| 14 | What is the BatchSpanProcessor and why use it over SimpleSpanProcessor? | BatchSpanProcessor queues finished spans and exports them in batches (by count or time). SimpleSpanProcessor exports each span immediately. BatchSpanProcessor adds minimal latency overhead per request (queue push is O(1)) vs SimpleSpanProcessor which blocks the request thread during export. Always use BatchSpanProcessor in production. |
| 15 | What four fields must every structured log record include for full observability? | timestamp (ISO 8601), level (ERROR/INFO/etc.), service (service name), event (what happened). For trace correlation add: trace_id, span_id. For debugging add: any relevant business identifiers (order_id, user_id, etc.). |

---

## 13. Further Reading

- **OpenTelemetry documentation (opentelemetry.io/docs):** The official specification and SDK documentation. The "Getting Started" guide for Python is the fastest path to working instrumentation. The Semantic Conventions reference defines all standard attribute names.
- **"Distributed Systems Observability" by Cindy Sridharan (O'Reilly, 2018):** The book that popularized the "three pillars" framing (though Sridharan herself later revised this view). Chapters 1-3 cover the foundational concepts.
- **"Distributed Tracing in Practice" by Austin Parker et al. (O'Reilly, 2020):** Focused entirely on distributed tracing. Chapter 4 (instrumentation patterns) and Chapter 5 (sampling) are the most relevant.
- **Jaeger documentation (jaegertracing.io):** Architecture, deployment, and query language. The "Deep Dive" section on sampling is particularly relevant for production deployments.
- **Google "Dapper: a Large-Scale Distributed Systems Tracing Infrastructure" (2010):** The paper that introduced modern distributed tracing. The W3C traceparent header and OTel's data model are direct descendants of Dapper's design. Essential background reading.
- **Brendan Gregg's "Systems Performance" — USE Method:** While focused on Linux performance, Gregg's Utilization-Saturation-Errors method is the metrics counterpart to distributed tracing. Required reading for anyone doing production observability.

---

## 14. Lab Exercises

**Exercise 1: Run the Demo and Inspect Trace Structure**
Run `distributed_observability.py`. Observe Phase 2 (context propagation): verify that `service-a trace_id == service-b trace_id` (same trace) and that `service-b parent_span_id == service-a span_id` (parent-child relationship). This is the fundamental mechanic of distributed tracing.

**Exercise 2: Break Context Propagation and Observe the Result**
In the `produce_kafka_message` method, comment out `self.tracer.inject(headers)`. Rerun the demo. Observe Phase 3: now the consumer spans have `parent_span_id=None` — they appear as root spans disconnected from the producer trace. This simulates the most common real-world tracing gap in Kafka pipelines.

**Exercise 3: Implement a Slow Span Alert**
Add a method to `InMemorySpanExporter` that returns all spans with `duration_ms > threshold`. Call it after the demo with `threshold=30` and observe that the `db.query inventory` span (35ms) is flagged. This simulates the "query for slow traces" pattern in Jaeger.

**Exercise 4: Add Error Handling to a Span**
Modify `_transform()` to raise an exception when `data.get('order_id') == 'order-2'`. Observe that `record_exception()` is called, the span status becomes ERROR, and the exception event is recorded in the span. Check that the containing span (kafka.consume) also becomes ERROR (because `start_span` catches and re-raises exceptions while recording them).

---

## 15. Key Takeaways

Observability in distributed systems requires all three pillars — metrics, logs, and traces — because each answers a different question at a different level of granularity. Metrics detect problems; traces identify where in the call graph the problem is; logs explain what happened in the specific component that failed.

Distributed tracing is the pillar most specific to distributed systems. Its key mechanism is trace context propagation: injecting trace_id and parent_span_id into every outgoing call (HTTP headers, Kafka message headers, gRPC metadata) so that spans across all services can be connected into a single tree. Without propagation across Kafka boundaries, the producer's trace and the consumer's trace are invisible to each other.

Structured logging with trace_id in every record is the connection from traces to logs. A trace identifies which span is slow; the trace_id lets you pull the logs for exactly that span's processing window, from exactly that service.

OpenTelemetry provides the industry-standard framework for all three pillars. Its SDK, semantic conventions, and Collector architecture allow instrumentation to be written once and exported to any backend — Jaeger, Prometheus, Loki, or any commercial APM — without changing application code.

---

## 16. Connections to Other Modules

- **M40 — Failure Modes:** Observability is the mechanism that makes failure modes visible. Gray failures (a node alive but degraded) are detected by per-node P99 latency monitoring — the same span-level latency data that distributed tracing captures. Without traces, gray failures are invisible.
- **M44 — Reading Real Failure Reports:** The five-step failure analysis framework in M44 requires metrics, logs, and traces at each step. "Map to metrics" (step 4) means knowing which Prometheus metrics to query. "Trace the causation chain" (step 3) is literally distributed tracing applied to failure analysis.
- **M45 — Clock Synchronization:** Span timestamps are wall clock times. If node clocks are skewed, span start/end times across services will be misaligned in the trace timeline — the child span will appear to start before the parent. HLC (M45) produces timestamps that are approximately correct even with clock skew.
- **M38 — Consensus Algorithms:** etcd (Raft-based) exposes metrics (`etcd_server_leader_changes_seen_total`, `etcd_server_proposals_pending`) that are the observability signals for consensus failure. These are Prometheus metrics — the metrics pillar applied to distributed consensus.

---

## 17. Anti-Patterns

**Anti-pattern: Using print statements instead of structured logging in a distributed pipeline.** A print statement with no trace_id, no service name, no level, and no machine-parseable format is useless at scale. You cannot query "show me all errors for order-123 across all services" without structured logging. The migration cost is low (Python's `structlog` or `python-json-logger` adds structured logging in under 10 minutes) but the operational benefit is enormous. Every new service should start with structured logging as a baseline requirement, not an afterthought.

**Anti-pattern: Tracing only at service boundaries, not within a service.** If an API request takes 500ms but you only have spans at service boundaries, you might see "order-service took 500ms" but not know if it was the Postgres query, the Redis cache lookup, or the external API call. Create child spans for every significant operation within a service — database queries, cache operations, external API calls, large loops. The right granularity: any operation that takes >5ms and whose performance you'd want to diagnose independently deserves its own span.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| OpenTelemetry SDK (Python) | Application instrumentation | `tracer.start_span()`, inject/extract context |
| OTel Collector | Telemetry pipeline | Receive, sample, enrich, export |
| Jaeger | Distributed trace backend | Query by service, operation, min_duration, tags |
| Prometheus | Metrics storage and alerting | `rate()`, `histogram_quantile()` |
| Grafana | Metrics visualization | Dashboards, cross-pillar navigation |
| Loki | Log aggregation | `{service="order-service"} | json | trace_id="abc123"` |
| structlog (Python) | Structured logging | Automatic JSON output with trace correlation |
| `distributed_observability.py` | Full observability demo | TraceContext, Span, Tracer, StructuredLogger, MetricsCollector |
| W3C traceparent header | Cross-service trace propagation | `00-<trace_id>-<parent_span_id>-<flags>` |

---

## 19. Glossary

**BatchSpanProcessor:** An OTel SDK component that queues finished spans and exports them in batches. Preferred over SimpleSpanProcessor (which exports one at a time) because it adds minimal per-request overhead.

**Cardinality:** The number of unique values of a label. Low-cardinality: status (ok/error), method (GET/POST). High-cardinality: user_id, order_id, trace_id. Prometheus stores one time series per unique label combination — high-cardinality labels cause memory exhaustion.

**Head-based sampling:** Sampling decision made at the start of a trace (the root span). All spans in a sampled trace are recorded; all in a non-sampled trace are discarded. Cannot selectively keep interesting (error/slow) traces.

**Observability:** The ability to understand a system's internal state from its external outputs (metrics, logs, traces). A system is "observable" if you can diagnose any failure without pushing new code.

**OpenTelemetry (OTel):** CNCF standard for distributed observability. Provides language SDKs, semantic conventions, exporters, and the OTel Collector. Backend-agnostic: write once, export anywhere.

**Sampling:** Reducing the volume of trace data by recording only a fraction of requests. Necessary for high-throughput services where recording every request is too expensive.

**Semantic Conventions:** OTel's standard attribute names for common operations (db.system, http.method, messaging.system). Ensures consistent naming across services and tool compatibility.

**Span:** A single unit of traced work. One span = one operation in one service. Contains: name, trace_id, span_id, parent_span_id, start/end time, attributes, events, status.

**Span Link:** A reference from one span to another span in a different (or same) trace. Used for async causality (Kafka consumer links to its producer) vs parent-child (synchronous causality).

**Structured logging:** Emitting log records as machine-parseable JSON dicts rather than free-form strings. Enables querying by field (trace_id, level, service) rather than full-text search.

**Tail-based sampling:** Sampling decision made after the trace is complete (all spans received). Can selectively keep error/slow traces and discard successful/fast traces. Requires buffering all spans until trace completion.

**Trace:** A directed acyclic graph of spans, all sharing the same trace_id. Represents the end-to-end journey of one request through a distributed system.

**Trace Context Propagation:** The mechanism of injecting trace_id and parent_span_id into outgoing requests and extracting them from incoming requests. W3C `traceparent` header is the standard format.

**traceparent:** The W3C standard HTTP (and Kafka/gRPC) header for trace context propagation. Format: `00-<trace_id>-<parent_span_id>-<flags>`.

---

## 20. Self-Assessment

1. A user reports that their order took 12 seconds to process. Describe the exact steps you would take using metrics, then traces, then logs to identify the root cause.
2. What is the difference between a span attribute and a span event? Give an example of something you would put in each.
3. You have a 5-service pipeline: API → order-service → Kafka → inventory-service → database. Draw the trace tree (spans and their parent-child relationships) for a successful request.
4. You are implementing a Python Kafka consumer. Write the code (using the classes from `distributed_observability.py`) that extracts trace context from the message headers and creates a CONSUMER span as a child of the producer's span.
5. Run `distributed_observability.py`. In Phase 2, what would happen if you called `service_b.start_span()` without the `parent_context=parent_ctx` argument? What would the resulting trace structure look like?
6. Why should metric labels be low-cardinality? What would happen to Prometheus if you added `user_id` as a label to a request counter for a service with 10 million users?
7. Explain tail-based sampling's advantage over head-based sampling for detecting rare errors.
8. In the OTel Collector config for tail sampling, there are three policies: errors, slow, and probabilistic. In what order does the collector evaluate them? What happens to a trace that matches both "errors" and "probabilistic"?

---

## 21. Module Summary

This module covered the complete observability stack for distributed data systems: metrics (what is wrong), logs (what happened in this service), and traces (how did this request flow through all services). All three pillars are necessary for production incident diagnosis.

Distributed tracing is the uniquely distributed system contribution to observability. Its core mechanism — trace context propagation via W3C `traceparent` headers — extends the happens-before relation from M45 (clocks and causality) into the observability domain: a trace is the practical, recorded representation of the causal chain of events that produced an outcome. The trace_id propagates through HTTP calls, Kafka message headers, and gRPC metadata, linking spans from every service into one connected tree.

The implementation — TraceContext, Span, Tracer, TracePropagator, StructuredLogger, MetricsCollector, and InstrumentedPipeline — is a simplified but complete working model of the OpenTelemetry SDK's data structures and propagation mechanisms. Understanding the implementation makes the OTel SDK's design choices obvious: the BatchSpanProcessor exists for the same reason WAL batching exists (fsync amortization); tail-based sampling exists for the same reason ISR lag thresholds exist (don't make binary decisions too early); trace_id in logs exists for the same reason LSN tracking exists in replication (a shared identifier that connects distributed state).

This is the final module of SYS-DST-102: Distributed Systems in Practice. The course has moved from theory (SYS-DST-101) to implementation: building a replication log, implementing a Raft follower, reading real failure reports, understanding clock synchronization, and building production observability. The next school — Database Engineering (DBE) — begins with DBE-INT-101: Storage Engine Internals, applying the same first-principles approach to the internals of the databases that distributed systems are built on top of.
