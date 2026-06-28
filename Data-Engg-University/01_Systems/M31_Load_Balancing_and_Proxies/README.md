# M31: Load Balancing and Proxies

**Course:** SYS-NET-101 — Networking for Data Engineers  
**Module:** 05 of 05  
**Global Module ID:** M31  
**Semester:** 1  
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

Every production data system eventually runs on more than one server. The moment you have multiple Kafka brokers, multiple Spark workers, multiple schema registry instances, or multiple JDBC connection pools, you need a mechanism to distribute traffic across them. Load balancing is that mechanism. But load balancing is not just about distributing traffic — it is about doing so in a way that preserves correctness, maintains connection state, handles failures, and meets latency SLOs.

Data engineers encounter load balancing in specific, recurring patterns that are distinct from web application load balancing:

**Pattern 1 — Kafka bootstrap and advertised listeners.** Kafka does NOT use a traditional load balancer for data-plane traffic. Clients connect to any broker via the bootstrap address, receive the full broker topology (partition leaders and their addresses), and then connect directly to the appropriate partition leader. A load balancer in front of Kafka brokers that round-robins connections would route a producer for partition 5 to the wrong broker — the leader for partition 5 is a specific broker, not any broker. Understanding this explains why Kafka advertised listeners must point directly to brokers, not to a load balancer.

**Pattern 2 — Database connection pooling.** A data pipeline that opens a new JDBC connection per query creates a new TCP connection each time (TCP handshake + TLS handshake + database authentication = 5–50 ms overhead per query). A connection pool maintains a fixed number of pre-authenticated connections and lends them to queries as needed. The pool IS a load balancer: it distributes queries across its pooled connections, and if backed by PgBouncer, distributes them across multiple Postgres instances.

**Pattern 3 — Cloud load balancers for REST endpoints.** Schema Registry, Kafka REST Proxy, Kafka Connect, and Airflow's UI are HTTP services. Cloud Network Load Balancers (NLBs) and Application Load Balancers (ALBs) distribute HTTPS traffic to multiple instances of these services. The choice between NLB (L4) and ALB (L7) affects how TLS is terminated, how health checks work, how sticky sessions behave, and what HTTP features (header rewriting, path routing) are available.

**Pattern 4 — Service mesh in Kubernetes.** When Kafka clients, Spark executors, and streaming pipeline pods all run in Kubernetes, a service mesh (Istio, Linkerd) adds sidecar proxies that intercept all TCP connections and provide automatic mTLS, circuit breaking, retry logic, and observability. The sidecar proxy is a reverse proxy that runs inside every pod, invisible to the application code.

Understanding load balancing means understanding which pattern applies to each component, what the correct topology is, and what breaks when topology is misconfigured.

---

## 2. Mental Model

### The Proxy as an Intermediary

Every load balancer is a proxy: an intermediary that receives connections on one side, makes decisions about where to forward them, and opens connections on the other side. The taxonomy of proxies is based on how much of the protocol they understand:

**L4 (Transport Layer) proxy:** Operates at the TCP/UDP level. Sees IP addresses and ports. Does not read the application payload. Forwards packets between client and server after establishing two TCP connections (client → proxy, proxy → server). Fast, low overhead, protocol-agnostic. Cannot make routing decisions based on URL paths, HTTP headers, or gRPC method names.

**L7 (Application Layer) proxy:** Operates at the HTTP/gRPC/etc. level. Reads the full application request. Can route based on URL path, HTTP headers, host names, gRPC service names, or request body contents. Can add headers, rewrite URLs, perform authentication, cache responses. More CPU-intensive than L4 — must parse every request.

**Forward proxy:** Sits in front of clients. Client connects to the proxy, which forwards to the server. Used for egress control, caching, and content filtering. The server sees the proxy's IP, not the client's.

**Reverse proxy:** Sits in front of servers. Client connects to the proxy (thinking it's the server), which forwards to one of many backend servers. The client sees only the proxy's address. Used for load balancing, TLS termination, and health checking.

### Connection Topology: The Critical Distinction

Load balancers create different connection topologies with different performance and correctness properties:

**TCP passthrough (L4, no termination):** Client → [L4 LB passes TCP stream] → Server. The L4 load balancer forwards the TCP stream without terminating the connection. TLS, if present, is established end-to-end between client and server. The load balancer cannot inspect TLS content. Lowest overhead; correct for Kafka data-plane traffic.

**TCP termination (L4 with termination):** Client → [L4 LB terminates TCP] → [L4 LB initiates new TCP] → Server. The load balancer terminates the client's TCP connection and opens a new one to the server. Enables connection multiplexing (many client connections → fewer server connections). Requires server to trust the load balancer for source IP (via proxy protocol header).

**TLS termination (L7):** Client → [L7 LB terminates TLS] → [L7 LB initiates new TLS or plain TCP] → Server. The load balancer decrypts TLS, inspects the HTTP request, makes routing decisions, and re-encrypts (or forwards plain HTTP) to the server. The server handles a fraction of TLS load. Enables URL-based routing, header manipulation, and centralized certificate management.

**TLS passthrough (SNI-based L4):** The L4 load balancer reads the TLS ClientHello's SNI (Server Name Indication) field without decrypting the traffic, and routes to the appropriate backend based on hostname. TLS is still end-to-end between client and server. Used to multiplex multiple TLS-protected services on a single IP.

---

## 3. Core Concepts

### 3.1 Load Balancing Algorithms

The choice of algorithm determines how traffic is distributed and whether "hot spots" develop:

**Round Robin:** Requests are distributed sequentially across backends: backend 1, backend 2, backend 3, backend 1, ... Equal distribution by count, not by work. If requests have varying cost (some queries are fast, some are slow), round robin creates imbalanced load. Suitable for stateless, equally-weighted backends.

**Weighted Round Robin:** Each backend has a weight proportional to its capacity. A backend with weight 2 receives twice as many requests as one with weight 1. Used when backends have different hardware capacities.

**Least Connections:** Each new connection is routed to the backend with the fewest active connections. Better than round robin for workloads with variable request duration (e.g., JDBC queries: some fast, some slow). Requires the load balancer to track active connection counts.

**Least Response Time:** Routes to the backend with the lowest combination of active connections and response time. More sophisticated than least connections; requires the LB to measure response times.

**IP Hash / Sticky Sessions:** Routes all requests from the same client IP (or session cookie) to the same backend. Necessary when backend state is not shared (e.g., in-memory caches, file uploads). Creates imbalanced load if some clients generate more traffic than others. Should be avoided when horizontal consistency is maintained by shared storage.

**Random with Two Choices (Power of Two):** Pick two backends at random, route to the one with fewer connections. Mathematically proven to reduce maximum load compared to round robin, with minimal overhead. Used by Envoy proxy and many modern load balancers.

**Consistent Hashing:** Routes requests with the same key to the same backend, using a hash ring that distributes keys evenly and minimizes redistribution when backends are added or removed. Used by distributed caches (Redis Cluster, Cassandra) where a given key must always reach the same node.

### 3.2 Health Checks

A load balancer that routes to unhealthy backends defeats the purpose of load balancing. Health checks are the mechanism by which the load balancer detects and removes unhealthy backends:

**TCP health check:** Opens a TCP connection to the backend. If the connection succeeds, the backend is healthy. Verifies the server is up and the port is listening, but does not verify that the application is functioning correctly.

**HTTP health check:** Sends an HTTP GET to a specific endpoint (e.g., `/health`, `/ready`). The backend returns 200 for healthy, 5xx for unhealthy. Can also check response content. More meaningful than TCP — verifies the application is serving requests.

**gRPC health check:** Uses the gRPC Health Checking Protocol. The LB sends a `grpc.health.v1.Health/Check` RPC. Appropriate for gRPC services.

**Health check parameters:**
```
interval: how often to check (typically 5–30 seconds)
timeout:  how long to wait for a response (typically 2–5 seconds)
threshold: how many consecutive failures before marking unhealthy
           (typically 2–3 failures to avoid flapping)
```

**Kafka broker health check:** Kafka does not have an HTTP health endpoint by default. Cloud load balancers that front Kafka use TCP health checks on port 9092. A better approach is to use Kafka's own `kafka-broker-api-versions.sh` or a custom health check that verifies the broker can respond to metadata requests.

### 3.3 Connection Pooling

Connection pooling is one of the highest-leverage performance optimizations in data engineering. Every database connection, every Kafka producer, every gRPC channel is a resource that has setup cost (TCP handshake, TLS handshake, authentication). Pooling amortizes this cost.

**How a connection pool works:**
1. At startup, open N connections to the backend (the "pool")
2. When a query arrives, borrow a connection from the pool
3. Execute the query on the borrowed connection
4. Return the connection to the pool (do not close it)
5. The next query borrows the same connection — no setup cost

**Pool sizing — the central decision:**
Too few connections: query queue builds up; high wait time before a connection is available (latency spike). Too many connections: each backend connection consumes server resources (memory, file descriptors). A Postgres server can typically handle 100–500 connections before performance degrades; beyond that, `pgbouncer` is needed to multiplex client connections.

**The correct size, via Little's Law:**
```
pool_size = throughput_rps × average_query_latency_s
```
For 1000 queries/second at 10 ms average latency: `pool_size = 1000 × 0.010 = 10 connections`. A pool of 10 connections saturates at 1000 QPS. To handle burst or add headroom, multiply by 1.5–2×.

**Connection pool implementations for data engineering:**
- **HikariCP** (Java): The gold standard for JDBC connection pooling. Sub-millisecond connection acquisition; used by default in many frameworks (Spring Boot, dbt, Airflow's SQLAlchemy).
- **PgBouncer** (Postgres): A dedicated Postgres connection multiplexer. Runs as a sidecar or separate process. Reduces Postgres backend connections from hundreds (one per app thread) to tens (the number of actual concurrent queries). Supports transaction-mode pooling (most efficient).
- **sqlalchemy.pool** (Python): SQLAlchemy's built-in connection pool used by Airflow, dbt, and most Python database clients.

**PgBouncer modes:**
- **Session mode:** A backend connection is assigned to a client for the entire session duration. Least efficient; equivalent to direct connections. Used when session-level features (temp tables, `SET` variables) are required.
- **Transaction mode:** A backend connection is assigned only for the duration of a transaction. Most efficient — a backend connection can serve many clients. Does not support session-level features. The recommended mode for most data engineering workloads.
- **Statement mode:** A backend connection is assigned only for a single SQL statement. Most aggressive pooling. Cannot use multi-statement transactions.

### 3.4 L4 Load Balancer: AWS Network Load Balancer

An AWS Network Load Balancer (NLB) operates at Layer 4. It receives TCP connections and forwards them to target instances. Key properties:

**Connection-level routing:** Each TCP connection (not each request) is routed to a backend when the connection is established. Once a connection is assigned, all traffic for that connection flows to the same backend. This means long-lived connections (Kafka producer-to-broker) stay on their initial backend for the connection's lifetime.

**No HTTP parsing:** The NLB does not read HTTP headers, cookies, or request bodies. It cannot route based on URL paths or gRPC service names.

**TLS passthrough or TLS termination:** NLBs can pass TLS through to backends (the backend terminates TLS, so Kafka's mTLS works end-to-end), or terminate TLS themselves (the NLB decrypts and forwards plain TCP to backends). For Kafka with mTLS, TLS passthrough is required — the NLB cannot participate in the client certificate exchange.

**Health check:** TCP or HTTP (requires a specific health endpoint on each backend).

**Source IP preservation:** NLBs can preserve the client source IP (important for IP-based access control in Kafka `ssl.principal.mapping.rules`). ALBs replace the source IP with the LB IP; the original IP is only accessible via the `X-Forwarded-For` header.

**Limitation for Kafka:** An NLB in front of Kafka brokers creates a fundamental problem: Kafka clients connect to the NLB, receive broker metadata (which contains the brokers' advertised listener addresses — the actual broker IPs or hostnames), and then connect directly to those addresses. If the brokers advertise their internal IPs, the clients must be able to reach those IPs directly. If they cannot (e.g., Kafka is in a private VPC and clients are outside), the advertised listeners must be updated to point to the NLB or to individual per-broker NLBs.

### 3.5 L7 Load Balancer: AWS Application Load Balancer

An AWS Application Load Balancer operates at Layer 7 (HTTP/HTTPS/HTTP2/WebSocket). It parses the full HTTP request and can route based on path, host, headers, and query parameters.

**Key properties:**

**Request-level routing:** Each HTTP request can be routed independently. A single client with one long-lived HTTP/2 connection can have its requests distributed across multiple backends.

**Path-based routing:**
```
/topics/* → Schema Registry instances
/connectors/* → Kafka Connect instances
/subjects/* → Schema Registry instances
```

**Host-based routing:** Multiple virtual hosts on a single ALB, routing to different backend services based on the `Host` header.

**TLS termination:** The ALB decrypts TLS, routes based on HTTP content, then re-encrypts (or plain-forwards) to backends. Centralized certificate management — one certificate on the ALB for all backends. For mTLS (client certificates), ALBs support client certificate forwarding (passing the cert to backends via the `X-Amzn-Mtls-Clientcert-*` headers).

**Sticky sessions:** ALBs can issue a cookie (`AWSALB`) that pins a client to a specific backend instance. Used for stateful applications. Not needed for stateless REST services.

**Limitations:** ALBs add 1–3 ms of additional latency compared to NLBs due to HTTP parsing overhead. They do not support raw TCP protocols (Kafka, Postgres, Redis) — only HTTP, HTTPS, HTTP/2, and WebSocket.

### 3.6 Kubernetes Load Balancing: kube-proxy and Services

In Kubernetes, Services provide load balancing at the cluster level, implemented by `kube-proxy`:

**ClusterIP Service:** Creates a virtual IP (the ClusterIP) in the cluster's service CIDR. `kube-proxy` installs iptables (or IPVS) rules on every node that intercept traffic to the ClusterIP and redirect it to one of the healthy pod endpoints. The selection is random by default (equivalent to round-robin for established connections).

```
Client → ClusterIP:9092 → [iptables/IPVS selects pod] → Pod IP:9092
```

**NodePort Service:** Exposes a port on every node's external IP. Traffic arriving at any node's external IP on that port is forwarded to pod endpoints. Used to expose services to traffic from outside the cluster when a cloud load balancer is unavailable.

**LoadBalancer Service:** Provisions a cloud load balancer (NLB in AWS, Network LB in GCP) that distributes external traffic to node ports, which then forward to pods via NodePort rules. The standard way to expose Kafka, Schema Registry, and Kafka Connect to external clients.

**IPVS mode vs iptables mode:** For large clusters (> 1000 services), IPVS is dramatically more efficient than iptables. IPtables uses a linear list of rules; IPVS uses hash tables. At 10,000 services, iptables rule traversal takes O(N) time per connection, while IPVS is O(1). Enable IPVS for large Kubernetes clusters: `kubectl edit configmap kube-proxy -n kube-system` and set `mode: ipvs`.

### 3.7 Service Mesh: Envoy Sidecar Proxy

A service mesh like Istio adds an Envoy sidecar proxy to every pod. Traffic interception happens at the iptables level (the init container installs rules that redirect all inbound and outbound traffic through the sidecar):

```
[Application port 8080]
    ↑ iptables redirect
[Envoy inbound port 15006]
    ↑ forwarded after policy check
[Envoy outbound port 15001]
    ↑ iptables redirect
[Application socket]
```

**What Envoy provides:**
- **Automatic mTLS:** Certificates issued by Istio's CA (SPIFFE/SVID). Every pod-to-pod connection is mutually authenticated and encrypted without any application code change.
- **Circuit breaking:** Envoy tracks error rates and response times per backend. If a backend exceeds configurable thresholds (error rate > 50%, response time > threshold), Envoy "trips" the circuit and immediately returns errors instead of waiting for slow/failing backends.
- **Load balancing (L7):** Within a service, Envoy uses round robin or least-connection across pod replicas.
- **Retries:** Configurable retry policies per service, so application code doesn't need retry logic.
- **Observability:** Every request generates metrics (success rate, P99 latency) and traces (distributed trace spans) automatically.

**Kafka and service mesh:** Kafka clients have their own connection management (connecting directly to partition leaders). A service mesh sidecar intercepts these connections, which can interfere with Kafka's topology-aware routing. The standard recommendation is to exclude Kafka ports from Envoy's iptables interception rules, or to configure Envoy `DestinationRule` resources with `trafficPolicy.connectionPool` settings appropriate for Kafka's long-lived connections.

### 3.8 Proxy Protocol

When an L4 load balancer performs TCP termination (connecting to the backend on behalf of the client), the backend loses visibility into the original client IP. The client's IP is the LB's IP. Proxy Protocol is a simple header prepended to the TCP connection that carries the original source IP and port:

```
PROXY TCP4 192.168.1.5 10.0.0.1 56324 9092\r\n
          ^client IP  ^LB IP  ^client port ^LB port
```

Kafka supports Proxy Protocol v2 for this purpose. When configured, the broker reads the Proxy Protocol header and uses the original client IP for authentication, ACLs, and logging. Without Proxy Protocol, all connections appear to come from the load balancer's IP, breaking IP-based ACLs.

**Kafka Proxy Protocol configuration:**
```properties
# server.properties
listeners=EXTERNAL://0.0.0.0:9094
advertised.listeners=EXTERNAL://kafka-nlb.prod.internal:9094
listener.security.protocol.map=EXTERNAL:SSL
# Enable Proxy Protocol for the EXTERNAL listener
listener.name.external.ssl.principal.mapping.rules=DEFAULT
kafka.security.protocol=SSL
# Proxy Protocol v2 support:
# Requires Kafka 3.3+ or specific network configurations
```

---

## 4. Hands-On Walkthrough

### 4.1 Inspecting Active Connections with ss

```bash
# All TCP connections with process names
ss -tnp

# Connections to a Kafka broker (grouped by destination)
ss -tnp 'dst :9092' | sort

# Count connections per destination IP (identifies load distribution)
ss -tn 'dst :9092' | awk 'NR>1{print $5}' | \
  cut -d: -f1 | sort | uniq -c | sort -rn
# Example output:
#   142  10.0.1.47    ← broker 1 (more connections)
#    89  10.0.1.48    ← broker 2
#    91  10.0.1.49    ← broker 3
# → imbalanced load; check partition leadership distribution

# Connections in specific states
ss -tn state established dst :9092 | wc -l    # ESTABLISHED count
ss -tn state time-wait   dst :9092 | wc -l    # TIME_WAIT count
ss -tn state close-wait  dst :9092 | wc -l    # CLOSE_WAIT count (potential leak)
```

### 4.2 Testing Load Balancer Behavior

```bash
# Test that an NLB/ALB is distributing to multiple backends
# (Connect multiple times and check which backend IP responds)
for i in $(seq 1 10); do
  # For an HTTP service behind a LB:
  curl -s -o /dev/null -w "%{remote_ip}\n" https://schema-registry.prod.internal:8081/subjects
done
# If properly load-balanced, you should see multiple different IPs

# Test NLB TCP health check
nc -z kafka-nlb.prod.internal 9092 && echo "TCP health check: OK" || echo "FAILED"

# Test Kafka metadata endpoint through LB (validates Kafka-layer health)
kafka-topics.sh --bootstrap-server kafka-nlb.prod.internal:9092 --list

# Observe NLB connection distribution in Kafka broker logs
# Each broker should show similar numbers of new client connections per minute
grep "New connection from" /var/log/kafka/server.log | \
  awk '{print $NF}' | sort | uniq -c | sort -rn
```

### 4.3 Testing PgBouncer Connection Pooling

```bash
# Check PgBouncer pool statistics
psql -h pgbouncer.prod.internal -p 6432 -U pgbouncer pgbouncer -c "SHOW POOLS;"
# Output: database | user | cl_active | cl_waiting | sv_active | sv_idle | sv_used
# cl_active: clients actively using a connection
# cl_waiting: clients waiting for a free connection (queue depth!)
# sv_active: server connections in use
# sv_idle: server connections available

# Show all PgBouncer clients
psql -h pgbouncer.prod.internal -p 6432 -U pgbouncer pgbouncer -c "SHOW CLIENTS;"

# Show server connections
psql -h pgbouncer.prod.internal -p 6432 -U pgbouncer pgbouncer -c "SHOW SERVERS;"

# Live stats (1-second refresh)
watch -n1 'psql -h pgbouncer -p 6432 -U pgbouncer pgbouncer -c "SHOW STATS_TOTALS;" 2>/dev/null'
```

### 4.4 Inspecting Kubernetes Service Load Balancing

```bash
# View Service endpoints (the backends that kube-proxy distributes to)
kubectl get endpoints kafka -n kafka-prod
# NAME    ENDPOINTS                                          AGE
# kafka   10.0.1.5:9092,10.0.1.6:9092,10.0.1.7:9092       2d

# Check if endpoints are healthy (no NotReady pods)
kubectl get pods -n kafka-prod -o wide

# View iptables rules for a specific Service (kube-proxy iptables mode)
# Get the ClusterIP of the kafka service:
CLUSTER_IP=$(kubectl get svc kafka -n kafka-prod -o jsonpath='{.spec.clusterIP}')
# View the DNAT rules that distribute traffic:
sudo iptables -t nat -L KUBE-SVC-KAFKA -n | head -30
# (KUBE-SVC-<hash> rules show probability-weighted random backend selection)

# Check Envoy proxy stats for a pod (if using Istio)
kubectl exec <pod> -c istio-proxy -- pilot-agent request GET stats | \
  grep 'upstream_cx_active\|upstream_rq_active'
```

### 4.5 Configuring Kafka Advertised Listeners for an NLB

```bash
# Scenario: Kafka brokers in a private VPC, exposed via 3 NLBs (one per broker)
# Each NLB has a DNS name. Advertised listeners must use NLB DNS, not broker IPs.

# server.properties for kafka-0:
cat << EOF >> server.properties
# Internal listener (cluster replication, uses broker's actual IP)
listeners=INTERNAL://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094

# Internal listener: brokers talk to each other via internal IPs
# External listener: clients connect via their per-broker NLB
advertised.listeners=\
  INTERNAL://kafka-0.kafka.prod.svc.cluster.local:9093,\
  EXTERNAL://kafka-0-nlb.us-east-1.elb.amazonaws.com:9094

listener.security.protocol.map=INTERNAL:SSL,EXTERNAL:SSL

# Clients will discover the EXTERNAL addresses from broker metadata
# and connect to kafka-0-nlb, kafka-1-nlb, kafka-2-nlb individually
EOF
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
lb_diagnostics.py

Load balancer and connection pool diagnostic toolkit for data engineers.
Checks backend distribution, connection pool health, and detects imbalances.

Dependencies: stdlib only (socket, subprocess, time, collections)
"""

import os
import re
import time
import socket
import subprocess
import collections
from dataclasses import dataclass, field
from typing import Optional


# ─── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class ConnectionDistribution:
    """Distribution of TCP connections across backends."""
    total_connections: int = 0
    # backend_ip → connection_count
    per_backend: dict = field(default_factory=dict)
    # Imbalance metrics
    max_connections: int = 0
    min_connections: int = 0
    imbalance_pct: float = 0.0   # (max - min) / max × 100
    cv: float = 0.0              # coefficient of variation (stdev / mean)


@dataclass
class HealthCheckResult:
    """Result of a health check against one backend."""
    host: str
    port: int
    healthy: bool
    latency_ms: float
    error: Optional[str] = None
    http_status: Optional[int] = None
    http_body_snippet: Optional[str] = None


@dataclass
class PoolStats:
    """Connection pool statistics."""
    pool_name: str
    active_connections: int = 0      # connections currently in use
    idle_connections: int = 0        # connections available
    waiting_clients: int = 0         # clients waiting for a connection
    total_connections: int = 0
    max_pool_size: int = 0
    utilization_pct: float = 0.0


# ─── Connection Distribution Analysis ──────────────────────────────────────────

def analyze_connection_distribution(
    port: int,
    filter_state: str = 'established',
) -> ConnectionDistribution:
    """
    Analyze how TCP connections to a specific port are distributed
    across backend IPs, using /proc/net/tcp and /proc/net/tcp6.
    """
    # Convert port to hex for /proc/net/tcp format
    port_hex = f"{port:04X}"

    backend_counts: dict[str, int] = collections.defaultdict(int)
    total = 0

    for path in ('/proc/net/tcp', '/proc/net/tcp6'):
        try:
            with open(path) as f:
                for line in f.readlines()[1:]:  # skip header
                    parts = line.split()
                    if len(parts) < 4:
                        continue
                    state_hex = parts[3]
                    # State 01 = ESTABLISHED
                    if filter_state == 'established' and state_hex != '01':
                        continue
                    # Remote address format: XXXXXXXX:PPPP (hex IP:port)
                    remote = parts[2]
                    remote_port_hex = remote.split(':')[1]
                    if remote_port_hex.upper() != port_hex:
                        continue
                    # Parse IP from hex (IPv4 in /proc/net/tcp is little-endian)
                    remote_ip_hex = remote.split(':')[0]
                    if len(remote_ip_hex) == 8:  # IPv4
                        ip_bytes = bytes.fromhex(remote_ip_hex)
                        # Linux stores in little-endian order
                        ip = '.'.join(str(b) for b in reversed(ip_bytes))
                        backend_counts[ip] += 1
                        total += 1
        except IOError:
            pass

    result = ConnectionDistribution(
        total_connections=total,
        per_backend=dict(backend_counts),
    )

    if backend_counts:
        counts = list(backend_counts.values())
        result.max_connections = max(counts)
        result.min_connections = min(counts)
        if result.max_connections > 0:
            result.imbalance_pct = (
                (result.max_connections - result.min_connections)
                / result.max_connections * 100
            )
        if len(counts) > 1:
            mean = sum(counts) / len(counts)
            variance = sum((c - mean) ** 2 for c in counts) / len(counts)
            stdev = variance ** 0.5
            result.cv = stdev / mean if mean > 0 else 0.0

    return result


def get_connections_by_state(port: int) -> dict[str, int]:
    """
    Count TCP connections to a given port by TCP state.
    Returns {state_name: count}.
    """
    state_names = {
        '01': 'ESTABLISHED', '02': 'SYN_SENT', '03': 'SYN_RECV',
        '04': 'FIN_WAIT1', '05': 'FIN_WAIT2', '06': 'TIME_WAIT',
        '07': 'CLOSE', '08': 'CLOSE_WAIT', '09': 'LAST_ACK',
        '0A': 'LISTEN', '0B': 'CLOSING',
    }
    port_hex = f"{port:04X}"
    counts: dict[str, int] = collections.defaultdict(int)

    for path in ('/proc/net/tcp', '/proc/net/tcp6'):
        try:
            with open(path) as f:
                for line in f.readlines()[1:]:
                    parts = line.split()
                    if len(parts) < 4:
                        continue
                    remote = parts[2]
                    remote_port_hex = remote.split(':')[1]
                    if remote_port_hex.upper() != port_hex:
                        continue
                    state_hex = parts[3].upper()
                    state_name = state_names.get(state_hex, f'UNKNOWN({state_hex})')
                    counts[state_name] += 1
        except IOError:
            pass

    return dict(counts)


# ─── Health Checking ─────────────────────────────────────────────────────────────

def tcp_health_check(
    host: str,
    port: int,
    timeout_s: float = 5.0,
) -> HealthCheckResult:
    """Perform a TCP connect health check against a backend."""
    start = time.perf_counter()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout_s)
        s.connect((host, port))
        s.close()
        latency_ms = (time.perf_counter() - start) * 1000
        return HealthCheckResult(
            host=host, port=port, healthy=True, latency_ms=latency_ms
        )
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        latency_ms = (time.perf_counter() - start) * 1000
        return HealthCheckResult(
            host=host, port=port, healthy=False,
            latency_ms=latency_ms, error=str(e)
        )
    finally:
        try:
            s.close()
        except Exception:
            pass


def http_health_check(
    host: str,
    port: int,
    path: str = '/health',
    expected_status: int = 200,
    timeout_s: float = 5.0,
    use_https: bool = False,
) -> HealthCheckResult:
    """
    Perform an HTTP health check against a backend.
    Uses urllib (stdlib) for simplicity; no external dependencies.
    """
    import urllib.request
    import urllib.error
    import ssl

    scheme = 'https' if use_https else 'http'
    url = f"{scheme}://{host}:{port}{path}"
    start = time.perf_counter()

    try:
        ctx = ssl.create_default_context() if use_https else None
        if ctx:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE  # diagnostic tool; don't verify

        req = urllib.request.Request(url, method='GET')
        with urllib.request.urlopen(req, timeout=timeout_s,
                                     context=ctx) as resp:
            latency_ms = (time.perf_counter() - start) * 1000
            body = resp.read(256).decode('utf-8', errors='replace')
            status = resp.status
            return HealthCheckResult(
                host=host, port=port,
                healthy=(status == expected_status),
                latency_ms=latency_ms,
                http_status=status,
                http_body_snippet=body[:100],
            )
    except urllib.error.HTTPError as e:
        latency_ms = (time.perf_counter() - start) * 1000
        return HealthCheckResult(
            host=host, port=port, healthy=False,
            latency_ms=latency_ms,
            http_status=e.code,
            error=str(e),
        )
    except Exception as e:
        latency_ms = (time.perf_counter() - start) * 1000
        return HealthCheckResult(
            host=host, port=port, healthy=False,
            latency_ms=latency_ms, error=str(e)
        )


def health_check_backends(
    backends: list[tuple[str, int]],
    check_type: str = 'tcp',  # 'tcp' or 'http'
    http_path: str = '/health',
) -> list[HealthCheckResult]:
    """
    Health check all backends in parallel using threads.
    Returns results sorted by host.
    """
    import concurrent.futures

    def check_one(backend):
        host, port = backend
        if check_type == 'http':
            return http_health_check(host, port, path=http_path)
        return tcp_health_check(host, port)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(backends)) as ex:
        results = list(ex.map(check_one, backends))

    return sorted(results, key=lambda r: r.host)


# ─── Connection Pool Sizing ───────────────────────────────────────────────────────

def compute_pool_size(
    throughput_rps: float,
    avg_latency_ms: float,
    headroom_factor: float = 1.5,
) -> dict:
    """
    Compute recommended connection pool size using Little's Law.
    L = λ × W → pool_size = throughput_rps × avg_latency_s
    """
    avg_latency_s = avg_latency_ms / 1000
    base_size = throughput_rps * avg_latency_s
    recommended_size = int(base_size * headroom_factor) + 1

    return {
        'throughput_rps': throughput_rps,
        'avg_latency_ms': avg_latency_ms,
        'base_pool_size': round(base_size, 1),
        'recommended_pool_size': recommended_size,
        'max_throughput_at_recommended': round(
            recommended_size / avg_latency_s, 1
        ),
    }


def analyze_pool_utilization(
    active: int,
    idle: int,
    waiting: int,
    max_size: int,
) -> dict:
    """
    Analyze connection pool health and provide recommendations.
    """
    total = active + idle
    utilization = active / total if total > 0 else 0
    result = {
        'active': active,
        'idle': idle,
        'waiting': waiting,
        'total': total,
        'max': max_size,
        'utilization_pct': round(utilization * 100, 1),
        'status': 'OK',
        'recommendations': [],
    }

    if waiting > 0:
        result['status'] = 'DEGRADED'
        result['recommendations'].append(
            f"Clients waiting for connections ({waiting} waiting). "
            f"Increase pool size. Current max: {max_size}. "
            f"Try: max_pool_size = {int(max_size * 1.5)}"
        )
    if utilization > 0.9:
        result['status'] = 'WARNING' if result['status'] == 'OK' else result['status']
        result['recommendations'].append(
            f"Pool at {utilization*100:.0f}% utilization. "
            f"Approaching saturation. Consider increasing pool size."
        )
    if idle > max_size * 0.5 and total < max_size * 0.3:
        result['recommendations'].append(
            f"Many idle connections ({idle} idle out of {total}). "
            f"Consider reducing min_pool_size to free database connections."
        )

    return result


# ─── LB Topology Validator ────────────────────────────────────────────────────────

def validate_kafka_lb_topology(
    bootstrap_servers: str,
    expected_broker_count: int,
) -> dict:
    """
    Validate that a Kafka bootstrap resolves to multiple broker IPs
    and that each broker is reachable.
    Detects the common misconfiguration of a single-IP NLB without
    per-broker addressing.
    """
    result = {
        'bootstrap_servers': bootstrap_servers,
        'resolution_results': {},
        'reachable_brokers': [],
        'unreachable_brokers': [],
        'warnings': [],
    }

    unique_ips = set()
    for server in bootstrap_servers.split(','):
        server = server.strip()
        host, port_str = server.rsplit(':', 1)
        port = int(port_str)
        try:
            infos = socket.getaddrinfo(host, port, socket.AF_UNSPEC,
                                       socket.SOCK_STREAM)
            ips = list({info[4][0] for info in infos})
            result['resolution_results'][server] = ips
            unique_ips.update(ips)
        except socket.gaierror as e:
            result['resolution_results'][server] = f"DNS error: {e}"

    # Check reachability
    for server in bootstrap_servers.split(','):
        server = server.strip()
        host, port_str = server.rsplit(':', 1)
        hc = tcp_health_check(host, int(port_str), timeout_s=3.0)
        if hc.healthy:
            result['reachable_brokers'].append(
                f"{server} ({hc.latency_ms:.1f}ms)"
            )
        else:
            result['unreachable_brokers'].append(
                f"{server}: {hc.error}"
            )

    # Detect topology issues
    bootstrap_entries = [s.strip() for s in bootstrap_servers.split(',')]
    if len(bootstrap_entries) < expected_broker_count:
        result['warnings'].append(
            f"bootstrap_servers has {len(bootstrap_entries)} entries but "
            f"expected {expected_broker_count} brokers. "
            f"Clients may not discover all brokers if some fail."
        )
    if len(unique_ips) == 1 and len(bootstrap_entries) > 1:
        result['warnings'].append(
            "All bootstrap hostnames resolve to the same IP — this looks like "
            "a single NLB IP. Kafka metadata will still work, but if the NLB "
            "fails, all bootstrap connections fail simultaneously. "
            "Consider per-broker NLBs or direct broker DNS names."
        )

    return result


# ─── Report ─────────────────────────────────────────────────────────────────────

def lb_health_report(
    kafka_port: int = 9092,
    http_backends: Optional[list[tuple[str, int]]] = None,
    kafka_bootstrap: Optional[str] = None,
) -> None:
    """Print a load balancer and connection distribution health report."""
    print("=" * 70)
    print("LOAD BALANCER / CONNECTION POOL HEALTH REPORT")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # --- Connection distribution to Kafka port
    print(f"\n[1] TCP Connection Distribution (port {kafka_port})")
    dist = analyze_connection_distribution(kafka_port)
    if dist.total_connections == 0:
        print(f"  No ESTABLISHED connections to port {kafka_port}")
    else:
        print(f"  Total connections: {dist.total_connections}")
        for ip, count in sorted(dist.per_backend.items(),
                                 key=lambda x: x[1], reverse=True):
            pct = count / dist.total_connections * 100
            bar = '█' * (count * 20 // max(dist.per_backend.values()))
            print(f"  {ip:<18}  {count:>4} ({pct:>5.1f}%)  {bar}")
        if dist.imbalance_pct > 20:
            print(f"\n  ⚠️  Imbalance: {dist.imbalance_pct:.0f}% "
                  f"(max={dist.max_connections}, min={dist.min_connections}). "
                  f"Check partition leadership distribution.")
        else:
            print(f"\n  ✓ Distribution balanced (imbalance: "
                  f"{dist.imbalance_pct:.1f}%)")

    # --- Connection states
    print(f"\n[2] Connection States (port {kafka_port})")
    states = get_connections_by_state(kafka_port)
    for state, count in sorted(states.items(), key=lambda x: x[1], reverse=True):
        warn = ""
        if state == 'CLOSE_WAIT' and count > 100:
            warn = "  ⚠️  high CLOSE_WAIT — possible connection leak"
        elif state == 'TIME_WAIT' and count > 50000:
            warn = "  ⚠️  high TIME_WAIT — check ephemeral port range"
        elif state == 'SYN_RECV' and count > 500:
            warn = "  ⚠️  high SYN_RECV — possible SYN backlog issue"
        print(f"  {state:<20}  {count:>6}{warn}")

    # --- HTTP backend health checks
    if http_backends:
        print(f"\n[3] HTTP Backend Health Checks")
        results = health_check_backends(
            http_backends, check_type='http', http_path='/health'
        )
        for r in results:
            icon = "✓" if r.healthy else "✗"
            status = f"HTTP {r.http_status}" if r.http_status else (r.error or "")
            print(f"  {icon} {r.host}:{r.port}  {status}  "
                  f"({r.latency_ms:.1f}ms)")

    # --- Pool sizing reference
    print(f"\n[4] Connection Pool Sizing Reference (Little's Law)")
    print(f"  {'Throughput':>12}  {'Latency':>10}  {'Min Pool':>10}  "
          f"{'Recommended':>12}")
    print(f"  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*12}")
    for tput, lat in [(100, 10), (1000, 10), (1000, 50), (10000, 5), (10000, 50)]:
        r = compute_pool_size(tput, lat)
        print(f"  {tput:>11} rps  {lat:>8}ms  "
              f"{int(r['base_pool_size']):>9}  "
              f"{r['recommended_pool_size']:>11}")

    print()
    print("=" * 70)


# ─── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load balancer diagnostic toolkit")
    parser.add_argument('--port', type=int, default=9092,
                        help='Port to analyze connection distribution for')
    parser.add_argument('--backends', nargs='+', metavar='HOST:PORT',
                        help='HTTP backends to health-check')
    parser.add_argument('--pool-size', nargs=2, type=float,
                        metavar=('THROUGHPUT_RPS', 'LATENCY_MS'),
                        help='Compute recommended pool size for given load')
    parser.add_argument('--kafka-bootstrap', metavar='BOOTSTRAP_SERVERS',
                        help='Validate Kafka bootstrap topology')
    args = parser.parse_args()

    if args.pool_size:
        tput, lat = args.pool_size
        r = compute_pool_size(tput, lat)
        print(f"Pool sizing for {tput} rps at {lat}ms latency:")
        print(f"  Base pool size:        {r['base_pool_size']}")
        print(f"  Recommended (1.5×):    {r['recommended_pool_size']}")
        print(f"  Max throughput:        {r['max_throughput_at_recommended']} rps")
    elif args.kafka_bootstrap:
        r = validate_kafka_lb_topology(args.kafka_bootstrap, expected_broker_count=3)
        print(f"Kafka bootstrap topology: {r['bootstrap_servers']}")
        for server, ips in r['resolution_results'].items():
            print(f"  {server}: {ips}")
        if r['reachable_brokers']:
            print(f"Reachable: {r['reachable_brokers']}")
        if r['unreachable_brokers']:
            print(f"Unreachable: {r['unreachable_brokers']}")
        for w in r['warnings']:
            print(f"⚠️  {w}")
    else:
        backends = []
        if args.backends:
            for ep in args.backends:
                h, p = ep.rsplit(':', 1)
                backends.append((h, int(p)))
        lb_health_report(
            kafka_port=args.port,
            http_backends=backends or None,
        )
```

---

## 6. Visual Reference

### L4 vs L7 Load Balancer Comparison

```
L4 (Network Load Balancer)        L7 (Application Load Balancer)
────────────────────────────       ──────────────────────────────
Client TCP conn ──────► LB         Client TLS conn ──────► LB
LB TCP conn ──────► Backend         LB parses HTTP request
                                    LB TLS conn ──────► Backend

Sees: IP, port, TCP flags          Sees: URL, headers, cookies, body
Cannot: route by URL               Can: route by URL, add headers,
                                       authenticate, cache

Latency overhead: ~0.1ms           Latency overhead: ~1-3ms
TLS: pass-through or terminate     TLS: always terminates
Protocols: any TCP/UDP             Protocols: HTTP, HTTPS, HTTP/2, WS
For Kafka: ✓ (data plane)          For Kafka: ✗ (cannot parse protocol)
For Schema Registry: ✓             For Schema Registry: ✓ (better)
```

### Kafka Advertised Listeners with Per-Broker NLBs

```
External Clients
       │
       ├──bootstrap──► NLB-for-any-broker (round robin)
       │                       │
       │              kafka-0-nlb ──► kafka-0 (get metadata)
       │
       │  Metadata response: "partition 0 leader is kafka-0-nlb:9094"
       │                      "partition 1 leader is kafka-1-nlb:9094"
       │                      "partition 2 leader is kafka-2-nlb:9094"
       │
       ├──partition 0──► kafka-0-nlb ──► kafka-0 (direct to leader)
       ├──partition 1──► kafka-1-nlb ──► kafka-1
       └──partition 2──► kafka-2-nlb ──► kafka-2

Each broker has its OWN NLB. Clients route directly to partition leaders.
A single shared NLB in front of all brokers would break this topology.
```

### PgBouncer Connection Multiplexing

```
Application Threads (200 connections)
  thread-1 ─────────┐
  thread-2 ──────── ├──► PgBouncer (transaction-mode pool)
  thread-3 ──────── │         │
  ...               │    10 actual Postgres backend connections
  thread-200 ───────┘
       │
       transaction: borrow connection → execute → return
       idle threads: no backend connection held

Without PgBouncer: 200 Postgres backend processes, each using ~5MB RAM = 1 GB
With PgBouncer:    10 Postgres backend processes, each using ~5MB RAM = 50 MB
```

### Service Mesh Sidecar Interception

```
Pod
┌────────────────────────────────────────────┐
│ Application (port 8080)                    │
│       │ iptables redirect (all traffic)    │
│       ▼                                    │
│ Envoy sidecar (port 15001/15006)           │
│   ├─ TLS termination (if inbound mTLS)     │
│   ├─ Load balancing (if outbound)          │
│   ├─ Circuit breaking                      │
│   ├─ Metrics emission                      │
│   └─ Trace propagation                     │
│       │ forward to actual destination      │
└────────────────────────────────────────────┘
```

---

## 7. Common Mistakes

**Mistake 1: Putting a shared round-robin load balancer in front of Kafka for data-plane traffic.** Kafka clients resolve partition leaders from broker metadata and must connect directly to the partition leader. A round-robin LB that routes Kafka produce requests to random brokers will cause every request to be forwarded internally by the receiving broker to the actual leader — doubling network hops and latency. The correct architecture is per-broker load balancers (one NLB per broker) or direct broker DNS names.

**Mistake 2: Not configuring `advertised.listeners` correctly after adding an NLB.** If `advertised.listeners` still points to the broker's internal IP after an NLB is added, external clients will connect to the bootstrap address (NLB), receive metadata with internal IPs, then fail to connect to those internal IPs from outside the VPC. `advertised.listeners` must match the address clients use to reach each broker — either a per-broker NLB DNS name or the internal IP (for intra-VPC clients).

**Mistake 3: Using connection pool session mode instead of transaction mode for stateless workloads.** PgBouncer session mode holds a backend connection for the entire client session — no better than a direct connection for most data engineering workloads. Transaction mode releases the backend connection after each transaction, allowing 10 backend connections to serve 200 application threads. Use transaction mode unless you have session-level features (temp tables, advisory locks, `SET LOCAL`).

**Mistake 4: Setting connection pool size too large.** More connections is not always better. Postgres performs best with 1 connection per CPU core for CPU-bound queries. With 8 cores, 8 connections saturate the CPU. Adding more connections creates queuing at the CPU level, increasing latency. The sweet spot is `pool_size = cores × (1 + wait_time / cpu_time)` — HikariCP documentation covers this as the formula for pool sizing.

**Mistake 5: Not accounting for health check false positives.** A TCP health check that succeeds (port is listening) does not mean the application is healthy. A Kafka broker that has run out of disk space, or a schema registry instance that has lost its ZooKeeper connection, may accept TCP connections but fail all application requests. Use HTTP health checks with meaningful health endpoints (`/health/ready` that checks actual service state) for L7 services.

**Mistake 6: Using L7 ALB for Kafka TLS with mTLS.** An ALB terminates TLS, which breaks mTLS. If the Kafka broker requires a client certificate for authentication, the ALB cannot forward the client certificate to the broker in the same way it was presented to the ALB. Use an NLB with TLS passthrough for mTLS-authenticated Kafka connections, so the TLS handshake (including client certificate exchange) happens end-to-end between the client and broker.

---

## 8. Production Failure Scenarios

### Scenario 1: Kafka Producers Routing to Wrong Broker Through Shared NLB

**Symptoms:** Kafka producers show elevated latency (P50 jumps from 1 ms to 4 ms) after a new NLB was placed in front of all three Kafka brokers. `kafka-consumer-groups.sh --describe` shows all partitions with the same broker host in the Leader column, but that host is the NLB DNS name, not individual broker hostnames.

**Root cause:** The `advertised.listeners` was changed to point to the shared NLB (`kafka-nlb.prod.internal:9092`) for all three brokers. Every broker now advertises the same address. Kafka clients connect to the NLB, receive metadata saying all partitions are on `kafka-nlb.prod.internal`, and send all produce requests to the NLB. The NLB distributes these across all three brokers randomly. A produce request for partition 5 (led by broker 2) that lands on broker 1 is internally redirected by broker 1 to broker 2 — one extra network hop.

**Fix:** Each broker must advertise its own NLB (one NLB per broker) or its own direct DNS name. Update `advertised.listeners` on each broker:
```properties
# Broker 0:
advertised.listeners=PLAINTEXT://kafka-0.prod.internal:9092
# Broker 1:
advertised.listeners=PLAINTEXT://kafka-1.prod.internal:9092
# Broker 2:
advertised.listeners=PLAINTEXT://kafka-2.prod.internal:9092
```
After a rolling restart, clients receive metadata with per-broker addresses and route directly.

### Scenario 2: PgBouncer cl_waiting Spike During Airflow Peak

**Symptoms:** Airflow DAGs that run at 8 AM (many tasks starting simultaneously) fail with `OperationalError: could not connect to server`. The error is `connection timeout`. The Postgres primary is healthy, CPU at 30%. The `SHOW POOLS` PgBouncer output shows `cl_waiting=150` during the peak.

**Root cause:** The Airflow metadata database connection pool (PgBouncer `pool_size=10`) is exhausted. At 8 AM, 200 Airflow worker tasks start simultaneously. Each needs a Postgres connection to check task state. PgBouncer has only 10 backend connections (`pool_size=10`). The 150 waiting clients pile up in the PgBouncer queue. PgBouncer's `query_wait_timeout` (default 120 seconds) eventually expires, causing the OperationalError.

**Fix:**
```ini
# pgbouncer.ini — increase pool size
[databases]
airflow_metadata = host=postgres.prod.internal port=5432 dbname=airflow pool_size=30

[pgbouncer]
default_pool_size = 30
max_client_conn = 500
pool_mode = transaction
query_wait_timeout = 30   # fail fast rather than holding connections for 120s
```

Also verify that Postgres can handle 30 connections: `SHOW max_connections;` — default is 100, which handles 30+ PgBouncer connections easily.

### Scenario 3: Envoy Circuit Breaker Trips on Kafka-Adjacent Service

**Symptoms:** Schema Registry requests fail with `503 Service Unavailable` even though all Schema Registry pods are healthy and responding normally. The failure correlates with a brief spike in Schema Registry response time 10 minutes ago. Envoy proxy logs show `upstream_cx_overflow`.

**Root cause:** Istio's Envoy proxy has circuit breaker settings (via `DestinationRule`) that limit the number of pending requests and connections per backend. During the response time spike, the connection pool to Schema Registry was briefly exhausted. Envoy tripped the circuit breaker (`upstream_cx_overflow`). After the circuit trips, Envoy rejects new requests immediately (503) without trying the backend — even after the Schema Registry has recovered. Envoy's circuit breaker uses an exponential backoff before retrying tripped backends.

**Fix:**
```yaml
# Tune the Envoy circuit breaker via Istio DestinationRule
apiVersion: networking.istio.io/v1alpha3
kind: DestinationRule
metadata:
  name: schema-registry
spec:
  host: schema-registry.kafka-prod.svc.cluster.local
  trafficPolicy:
    connectionPool:
      tcp:
        maxConnections: 100
        connectTimeout: 5s
      http:
        h2UpgradePolicy: DO_NOT_UPGRADE
        http1MaxPendingRequests: 100
        maxRequestsPerConnection: 0
    outlierDetection:
      consecutive5xxErrors: 10      # trip after 10 errors (not 5)
      interval: 30s                 # evaluation window
      baseEjectionTime: 10s         # eject for 10s, not the default 30s
      maxEjectionPercent: 50        # eject at most 50% of backends at once
```

### Scenario 4: kube-proxy iptables O(N) Causing Network Latency at Scale

**Symptoms:** In a large Kubernetes cluster with 5,000 services, network-intensive Spark jobs show gradually increasing inter-executor shuffle latency over time. The latency correlates with the number of Kubernetes Services. Per-packet processing CPU on nodes rises. `conntrack -L | wc -l` shows millions of connection tracking entries.

**Root cause:** kube-proxy in iptables mode installs one iptables rule per endpoint per service. With 5,000 services and an average of 3 pods each, that's 15,000+ iptables rules. Each new TCP connection from a Spark executor must traverse all rules sequentially (O(N)) before being NATted to the correct backend. This linear traversal adds microseconds per connection but with 40,000+ shuffle connections in a large job, it accumulates to seconds.

**Fix:** Switch kube-proxy to IPVS mode:
```bash
# Edit kube-proxy ConfigMap
kubectl edit configmap kube-proxy -n kube-system
# Change: mode: "iptables"  →  mode: "ipvs"

# Restart kube-proxy DaemonSet
kubectl rollout restart daemonset kube-proxy -n kube-system

# Verify IPVS is active
ipvsadm -L --stats | head -20
```
IPVS uses kernel hash tables for connection routing — O(1) regardless of the number of services. In a 5,000-service cluster, switching to IPVS reduces per-connection routing overhead from ~50 µs to < 1 µs.

---

## 9. Performance and Tuning

### HikariCP Pool Configuration for Data Engineering

```java
// HikariCP config for Airflow, dbt, or custom pipeline JDBC connections
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:postgresql://postgres.prod.internal:5432/pipeline_db");
config.setUsername("pipeline_user");
config.setPassword(secrets.get("db_password"));

// Pool sizing: start with Little's Law estimate
config.setMaximumPoolSize(20);        // max connections to Postgres
config.setMinimumIdle(5);             // keep 5 connections warm at all times
config.setIdleTimeout(300_000);       // close idle connections after 5 minutes
config.setMaxLifetime(1_800_000);     // recycle connections after 30 minutes
                                       // (prevents stale connections after DB restart)
config.setConnectionTimeout(5_000);   // wait max 5s for a pool connection
config.setValidationTimeout(3_000);   // connection validation test timeout

// Connection validation: verify connections aren't stale before using
config.setConnectionTestQuery("SELECT 1");  // for JDBC4+, this is automatic

// For Postgres specifically: faster startup validation
config.addDataSourceProperty("loginTimeout", "3");
config.addDataSourceProperty("connectTimeout", "3");
config.addDataSourceProperty("socketTimeout", "30");
```

### PgBouncer Transaction Mode Configuration

```ini
; pgbouncer.ini — production configuration for Airflow metadata DB

[databases]
; Define pools per database/user pair
airflow = host=postgres-primary.prod.internal port=5432 dbname=airflow
          pool_size=20 max_db_connections=25

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432

; CRITICAL: transaction mode is the efficient choice for most workflows
pool_mode = transaction

; Maximum clients across all pools
max_client_conn = 500

; Default pool size if not specified per database
default_pool_size = 20

; Queue management
query_wait_timeout = 15          ; fail after 15s waiting, not 120s
query_timeout = 0                ; no per-query timeout (application manages)
client_idle_timeout = 60         ; disconnect idle clients after 60s

; Connection health
server_check_delay = 30          ; verify server connections every 30s
server_check_query = SELECT 1    ; simple liveness check
server_lifetime = 3600           ; recycle backend connections hourly

; Logging
log_connections = 1
log_disconnections = 1
log_pooler_errors = 1
```

### Kafka Advertised Listeners for Multi-Topology Deployments

```properties
# server.properties for a broker serving both in-cluster and external clients
#
# Two listeners:
# INTERNAL: for Kafka-to-Kafka communication (replication, inter-broker)
# EXTERNAL: for producer/consumer clients (via per-broker NLB)

listeners=INTERNAL://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094

# advertised.listeners uses different addresses for each listener type
# INTERNAL uses Kubernetes DNS (headless service, StatefulSet pod name)
# EXTERNAL uses the per-broker NLB DNS (accessible from outside the cluster)
advertised.listeners=\
  INTERNAL://kafka-0.kafka.kafka-prod.svc.cluster.local:9093,\
  EXTERNAL://kafka-0-ext.us-east-1.elb.amazonaws.com:9094

listener.security.protocol.map=INTERNAL:SSL,EXTERNAL:SSL

# Kafka uses the INTERNAL listener for replication (low latency, same-AZ)
inter.broker.listener.name=INTERNAL
```

---

## 10. Interview Q&A

**Q1: Why can't you put a standard round-robin load balancer in front of Kafka brokers for producer traffic? What topology does Kafka actually require?**

Kafka's data plane is fundamentally incompatible with round-robin load balancing because Kafka is not a stateless request-response service — it is a partitioned, replicated log where each partition has a designated leader broker. A producer sending records to topic `payments`, partition 3 must send them to whichever broker is currently the leader for that partition. If the records land on a non-leader broker (via round-robin routing), the broker must either reject the request with `NotLeaderOrFollowerException` (causing the client to retry with metadata refresh) or internally forward the records — both are expensive.

The Kafka client's discovery mechanism is designed around this constraint. The client connects to the bootstrap address to get partition metadata, which tells it: "Partition 3 of topic payments is led by broker kafka-1 at 10.0.1.48:9092." The client then opens a direct TCP connection to 10.0.1.48:9092 and sends partition 3's records there. This is why the `advertised.listeners` configuration is so critical — it specifies the addresses that clients will use to reach specific brokers after they've resolved the partition metadata.

The correct load balancing topology for Kafka has three levels: at the bootstrap level, a shared NLB or DNS round-robin can be used (the bootstrap is only used once per client session to fetch metadata, not for ongoing traffic). At the data-plane level, each broker must be addressable directly — either via its own NLB, a static IP, or a DNS name that resolves to exactly that broker's IP. At the internal replication level, brokers communicate with each other via their inter-broker listener, which should use internal DNS names directly (no NLB needed between brokers in the same Kubernetes cluster or VPC).

**Q2: Explain the difference between PgBouncer's transaction mode and session mode. Why does transaction mode allow significantly better connection multiplexing?**

PgBouncer is a connection multiplexer that maintains a pool of actual Postgres backend connections and lends them to client applications. The pooling mode determines how long a backend connection remains allocated to a client once assigned.

In session mode, a backend connection is allocated to a client application for the entire duration of the client's session — from when the client connects to PgBouncer until the client disconnects. This is equivalent to a traditional connection pool in the application itself and provides no advantage in connection count reduction. If 200 application threads connect to PgBouncer in session mode, PgBouncer still opens 200 backend connections to Postgres.

In transaction mode, a backend connection is allocated only for the duration of a single transaction and released back to the pool when the transaction commits or rolls back. Between transactions, the client is not holding any backend connection. This is the transformative insight: most application threads spend the vast majority of their time not inside a transaction — they're processing business logic, waiting for I/O, or handling other requests. Transaction mode exploits this idle time. If 200 application threads each execute 5 queries per second, each taking 2 ms, each thread holds a backend connection for 10 ms and is idle for 990 ms per second. Transaction mode means 200 threads need a backend connection only 1% of the time simultaneously — so 2–10 backend connections can serve 200 threads, reducing Postgres backend count by 20×. This reduces Postgres memory consumption, process overhead, and connection setup cost significantly.

The trade-off is that transaction mode does not support session-level state: temporary tables, advisory locks, `SET` statements (like `SET search_path`), and prepared statements with named handles are all scoped to the session and do not survive transaction-mode release. Applications that rely on these features must use session mode or rewrite them (e.g., using `SET LOCAL` for transaction-scoped settings, which works correctly in transaction mode).

**Q3: What is a circuit breaker in the context of service mesh proxies, and how does it protect data infrastructure from cascading failures?**

A circuit breaker is a stability pattern — named after the electrical circuit breaker — that prevents a slow or failing downstream service from causing the entire caller to degrade. In a service mesh like Istio, Envoy sidecar proxies implement circuit breakers as part of their load balancing logic.

The circuit breaker has three states. In the "closed" state (normal operation), requests flow through to the backend. The proxy monitors the backend's response time and error rate. When the backend's error rate or connection failure rate exceeds a configurable threshold — say, 5 consecutive 5xx errors or a timeout rate > 20% — the circuit "trips" to the "open" state. In the open state, the proxy immediately returns an error response (503 Service Unavailable) to callers without attempting to reach the failing backend. This is critical for preventing cascading failures: if Schema Registry is overwhelmed and taking 30 seconds to respond, without a circuit breaker, every Kafka producer waiting for schema validation will block for 30 seconds before timing out. With a circuit breaker, once the circuit trips, producers fail immediately (fast failure) and can take recovery action (retry with backoff, use a fallback schema) instead of blocking and consuming thread resources. After a configurable "half-open" period, the proxy allows a small number of test requests through. If they succeed, the circuit closes; if they fail, the circuit remains open.

For data infrastructure specifically, circuit breakers protect against the "thundering herd after recovery" problem: when a Kafka broker restarts, thousands of producers and consumers try to reconnect simultaneously. Without circuit breaking, this burst can overwhelm the broker before it has fully initialized, causing repeated failures. With a circuit breaker, the traffic is held back during the trip period and gradually allowed through as the broker stabilizes.

---

## 11. Cross-Question Chain

**Interviewer:** You're asked to expose a Kafka cluster (3 brokers) running in a Kubernetes cluster to producers running in an on-premises data center. The connection goes through a VPN. Design the exposure architecture.

**Candidate:** The critical constraint is that Kafka's partition-leader routing requires clients to connect directly to specific brokers, not via a single shared address. My architecture: one Kubernetes LoadBalancer Service (which provisions an AWS NLB) per broker. Each NLB gets a static IP or a fixed DNS name. The advertised listeners on each broker are set to the respective NLB's DNS name on the external port. The bootstrap address — what the on-prem producers configure in `bootstrap.servers` — can be any of the three NLB addresses, since bootstrap is only used once to fetch metadata. After fetching metadata, the producer learns the per-broker NLB addresses from the advertised listeners and connects directly. The VPN must allow TCP traffic from the on-prem network to all three NLB IPs on port 9092 (or whatever the external listener port is).

**Interviewer:** This works, but your security team says that having three separate NLBs is too expensive. They want one shared NLB in front of all three brokers. Is there any way to make this work correctly?

**Candidate:** Yes, but it requires a more sophisticated setup. The key insight is that the shared NLB can be used for bootstrap only, while the producers still need to reach brokers individually. There are two approaches. First, use a single NLB that routes based on the destination port: assign each broker a unique port on the shared NLB (e.g., broker 0 → port 9092, broker 1 → port 9093, broker 2 → port 9094). The NLB routes each port to the corresponding broker. The advertised listeners use the shared NLB DNS but with per-broker ports. Producers will connect to broker 0 at `kafka-nlb.internal:9092`, broker 1 at `kafka-nlb.internal:9093`, broker 2 at `kafka-nlb.internal:9094`. This is the most common solution for cost-conscious deployments.

Second, use a single NLB with SNI-based routing. Each broker gets a unique DNS name (broker-0.prod.internal, broker-1.prod.internal, broker-2.prod.internal), all pointing to the NLB IP. The NLB uses SNI routing (reading the TLS ClientHello's `server_name` extension) to route to the correct backend. This requires Kafka to use TLS (for SNI to be available) and an NLB that supports SNI-based target groups. Less common but maintains standard port 9092 for all brokers.

**Interviewer:** The security team approves the per-port solution. During implementation, you notice that after a Kafka partition leader election (broker 1 takes over some partitions that were on broker 0), producers start failing with `NotLeaderOrFollowerException` for those partitions. After 5 minutes, they recover. What's happening during those 5 minutes?

**Candidate:** When a leader election occurs, the broker metadata changes: some partitions now have a new leader (broker 1 instead of broker 0). Producers that were successfully sending to broker 0 for those partitions continue doing so until they receive `NotLeaderOrFollowerException`. The producer's Kafka client then triggers a metadata refresh — it fetches the current partition metadata from the bootstrap address and learns the new leader. The producer then reconnects to broker 1 and resumes. This should take seconds, not minutes. Five minutes suggests the metadata refresh is not happening promptly.

The likely cause: `metadata.max.age.ms` (default 300,000 ms = 5 minutes) controls how long the producer caches metadata before proactively refreshing it. If the producer receives `NotLeaderOrFollowerException` and retries, it should trigger an immediate metadata refresh. If it's taking 5 minutes, the producer might not be receiving the exception (perhaps the old connection is silently failing rather than returning the error) or the exception is being caught and retried without triggering a metadata refresh. I'd check: is `metadata.max.age.ms` set to 300000? Is there a retry logic that's swallowing the exception? Is the old connection still open (TCP TIME_WAIT or established) without returning errors? Use `kafka-consumer-groups.sh` to check partition leader assignments during the failure window and correlate with producer logs.

**Interviewer:** The 5-minute delay is confirmed to be `metadata.max.age.ms=300000`. The team wants to reduce it to 30 seconds. What are the trade-offs?

**Candidate:** Reducing `metadata.max.age.ms` from 300 seconds to 30 seconds has two effects. The benefit: producers will detect broker topology changes 10× faster. After a leader election, producers will discover the new leader within 30 seconds even without receiving `NotLeaderOrFollowerException` (proactive refresh). This reduces the recovery window significantly.

The cost: more metadata fetch requests. With 300 producers each refreshing metadata every 30 seconds, that's 10 metadata requests per second to the brokers. Metadata requests are lightweight, but at scale (thousands of producers), this increases broker load. The typical guidance is: for stable clusters, 5–10 minutes is fine; for clusters with frequent leadership changes or in disaster recovery testing, 30–60 seconds is better.

A complementary setting is `reconnect.backoff.max.ms` (default 1000 ms) — the maximum backoff before attempting to reconnect to a failed broker. Combined with `metadata.max.age.ms=30000`, the recovery sequence becomes: `NotLeaderOrFollowerException` → immediate metadata refresh → reconnect to new leader within 1 second. This is the configuration for low-latency recovery.

**Interviewer:** Final scenario: you deploy the Kafka cluster with this NLB setup. Six months later, ops reports that the NLB cost has grown to $2,000/month due to LCU (Load Capacity Unit) charges driven by "active connections." Your cluster has 500 producers and 200 consumers. What Kafka and application-level configuration changes can reduce the connection count?

**Candidate:** The total Kafka connection count is: producers × brokers + consumers × brokers (since Kafka clients maintain a connection to every broker, not just partition leaders). With 500 producers × 3 brokers + 200 consumers × 3 brokers = 1,500 + 600 = 2,100 active connections. Each connection is a TCP connection through the NLB, contributing to LCU charges.

Reduction strategies: first, check whether all 500 producers are necessary. Many pipelines create a `KafkaProducer` per Spark executor, per Airflow task, or per microservice instance rather than sharing a singleton. Reducing to a shared producer per service instance (thread-safe in Kafka) reduces connection count proportionally.

Second, configure `connections.max.idle.ms` (default 9 minutes) to close idle broker connections. A producer that is only connected to 3 topics should not maintain connections to all 3 brokers — only the leaders for its topics. Setting `connections.max.idle.ms=60000` (1 minute) closes unused broker connections faster, but may cause reconnect overhead if a producer resumes after a pause. The right value depends on the pipeline's burst pattern.

Third, consolidate consumer groups. If 200 consumers are actually 200 independent short-lived consumers (each checking one message then disconnecting), they should be replaced with long-lived consumer group members with `subscribe()`. Each consumer group member maintains one connection per broker; short-lived consumers open and close connections rapidly, generating more LCU events.

Fourth, for the NLB specifically, consider whether the `keepalive` settings are causing the NLB to bill for connections that are logically idle but TCP-alive. Setting `socket.keepalive.enable=true` on Kafka clients prevents NAT expiry but also keeps the NLB billing for those connections. If the cluster is within the VPC (no NAT), `socket.keepalive.enable=false` can be set and `connections.max.idle.ms` controls connection lifetime instead.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the difference between L4 and L7 load balancing? | L4: routes by IP/port/TCP state, protocol-agnostic, low overhead. L7: reads HTTP headers/URL/body, supports path routing and header manipulation, 1–3ms overhead. |
| 2 | Why can't you use a round-robin L4 load balancer for Kafka data-plane traffic? | Kafka producers must connect to the partition leader specifically. Round-robin routes to random brokers, causing internal forwarding or NotLeaderOrFollowerException. |
| 3 | What is `advertised.listeners` in Kafka? | The address each broker advertises to clients in metadata responses. Clients use these addresses to connect to specific brokers. Must be reachable by clients, not just internal IPs. |
| 4 | What is PgBouncer transaction mode? | Backend connection is released back to pool after each transaction. Allows 10 connections to serve 200 threads. Does not support session-level features (temp tables, SET variables). |
| 5 | How do you size a connection pool using Little's Law? | pool_size = throughput_rps × avg_latency_s. For 1000 rps at 10ms: pool_size = 10. Add 1.5× headroom = 15 connections. |
| 6 | What is a circuit breaker in a service mesh? | Stops routing to a failing backend after it exceeds an error threshold. Immediately returns 503 to protect callers. Tries again after a backoff period. Prevents cascading failures. |
| 7 | What is Proxy Protocol? | A header prepended to TCP connections by L4 LBs, containing the original client IP/port. Required when the server needs the real client IP for authentication or ACLs. |
| 8 | What is kube-proxy's limitation in iptables mode at scale? | O(N) rule traversal for each new connection, where N = number of services × endpoints. At 10,000+ services, this adds significant per-connection CPU overhead. Fix: switch to IPVS mode (O(1)). |
| 9 | What is TLS passthrough at an L4 LB? | The LB reads the SNI from the TLS ClientHello to route, but does not decrypt the TLS stream. TLS is terminated end-to-end at the backend. Required for mTLS to work through an LB. |
| 10 | What Kafka configuration controls how quickly producers detect a new partition leader? | `metadata.max.age.ms` (default 300s). Producers also detect on `NotLeaderOrFollowerException`. Reduce to 30s for faster topology change detection. |
| 11 | What is a Kubernetes headless service and how does it differ from a ClusterIP service? | Headless: `clusterIP: None`, DNS returns all pod IPs. ClusterIP: single virtual IP, kube-proxy load-balances. Kafka uses headless + StatefulSet for direct per-broker addressing. |
| 12 | What is the "two choices" load balancing algorithm? | Pick two backends randomly; route to the one with fewer connections. Provides near-optimal load distribution with minimal overhead. Used by Envoy proxy. |
| 13 | What causes CLOSE_WAIT to accumulate on a Kafka broker? | Application clients that have received FIN (connection close) but haven't called close() on the socket. A bug in the client code — resource leak. |
| 14 | What is consistent hashing used for in distributed systems? | Routes requests with the same key to the same backend, using a hash ring. Minimizes redistribution when backends are added/removed. Used in Redis Cluster, Cassandra, distributed caches. |
| 15 | What does an AWS NLB preserve that an ALB does not? | Source IP address. NLBs can forward the client's real IP to the backend. ALBs replace the source IP with the LB's IP; real IP only available via X-Forwarded-For header. |
| 16 | Why should `connections.max.idle.ms` be tuned for Kafka producers? | Producers maintain connections to all brokers, even those with no relevant partitions. Idle connections waste LB LCU charges and file descriptors. Setting to 60s closes unused broker connections. |
| 17 | What is the Envoy sidecar proxy interception mechanism in Istio? | An init container installs iptables rules that redirect all inbound (port 15006) and outbound (port 15001) traffic through the Envoy sidecar. The application sees no difference. |
| 18 | What health check is more meaningful for a Kafka broker — TCP or HTTP? | Neither natively. TCP verifies the port is listening; HTTP requires a health endpoint. For Kafka, a TCP check to port 9092 is common, but a better check is a Kafka metadata API request via a custom health check script. |
| 19 | What is IPVS mode in kube-proxy? | Uses kernel-level hash tables for service routing (O(1) lookup). Replaces iptables mode (O(N)). Required for large Kubernetes clusters (> 1000 services) to avoid connection routing overhead. |
| 20 | What must match between Kafka `advertised.listeners` and client `bootstrap.servers`? | The addresses in `advertised.listeners` must be reachable from the client's network. If clients are external to the cluster, `advertised.listeners` must use externally routable addresses (NLB DNS, public IPs), not internal cluster IPs. |

---

## 13. Further Reading

- **"Envoy Proxy Architecture" — envoyproxy.io/docs:** The authoritative documentation for Envoy's load balancing algorithms, circuit breaker configuration, and connection pool settings. Chapters on upstream clusters and circuit breaking are directly applicable to Istio-based deployments.
- **"PgBouncer Usage and FAQ" — pgbouncer.org:** The official PgBouncer documentation, including the transaction mode constraints (what doesn't work in transaction mode) and the pool sizing guide. Essential reading before deploying PgBouncer for Airflow or dbt.
- **Kafka documentation: "Multi-Region Deployment" and "Listener Configuration":** Confluent's guide to configuring `advertised.listeners` for multi-environment deployments, including NLB, NodePort, and LoadBalancer Service configurations in Kubernetes.
- **"HikariCP: About Pool Sizing":** The HikariCP GitHub repository's wiki entry on pool sizing. Explains the formula `pool_size = Tn × (Cm - 1) + 1` for avoiding deadlocks, and the trade-off between pool size and database connection count.
- **"The Site Reliability Workbook" — Google, Chapter 23 (Managing Load):** Practical treatment of load balancing strategies, health checking, and circuit breaking in large-scale production systems.
- **"Load Balancing in the Cloud" — AWS, GCP, Azure documentation:** Each cloud provider's documentation for their NLB and ALB equivalents. AWS: NLB vs ALB decision guide. GCP: Internal TCP Load Balancing guide. Azure: Application Gateway vs Load Balancer.
- **"Istio Service Mesh" — istio.io/docs:** The authoritative reference for Istio's DestinationRule and VirtualService resources, including circuit breaker configuration, retry policies, and traffic management. The "Traffic Management" section is essential for data platform engineers using Kubernetes.

---

## 14. Lab Exercises

**Exercise 1: Observe Kafka Connection Distribution**

On a machine with active Kafka producers running, execute `ss -tnp dst :9092 | awk 'NR>1{print $5}' | cut -d: -f1 | sort | uniq -c`. Observe whether connections are evenly distributed across all broker IPs or skewed toward one. If running a test producer, use `kafka-producer-perf-test.sh` with 3 producer instances and verify each connects to all brokers.

**Exercise 2: PgBouncer Transaction Mode Benchmark**

Set up PgBouncer in both session mode and transaction mode in front of a local Postgres. Run `pgbench -c 100 -j 4 -T 30` (100 clients, 4 threads, 30 seconds) against both. Compare: transactions/second, average latency, max Postgres backend connections (visible via `SHOW SERVERS` in PgBouncer). Observe the connection count difference.

**Exercise 3: Simulate Circuit Breaker Behavior**

Using Istio in a local Kubernetes cluster (minikube or kind), deploy a simple HTTP service and a DestinationRule with `consecutiveGatewayErrors: 3`. Generate errors by killing the backend pod. Observe Envoy's circuit breaker tripping (503 responses) and recovery. Check `kubectl exec <client-pod> -- curl -s http://backend/health` every second to see the circuit state change.

**Exercise 4: kube-proxy iptables vs IPVS Latency**

In a test Kubernetes cluster, create 1,000 ClusterIP services. Benchmark TCP connection establishment time to a specific service. Then switch kube-proxy to IPVS mode. Repeat the benchmark. Compare connection setup latency — the difference is measurable at 1,000 services and dramatic at 10,000.

**Exercise 5: Kafka Advertised Listeners Topology Test**

Set up a 3-broker Kafka cluster with `advertised.listeners` configured correctly (per-broker DNS names) and incorrectly (all pointing to a single NLB IP). Run a producer for 10 minutes in each configuration. Use `kafka-log-dirs.sh --describe` to check partition distribution and observe whether the single-NLB configuration causes internal forwarding (measurable as increased inter-broker traffic).

---

## 15. Key Takeaways

Load balancing and proxies are the infrastructure that makes distributed data systems scalable and fault-tolerant, but they are not invisible — they have topology requirements, protocol constraints, and performance characteristics that directly affect Kafka, Spark, and database connectivity.

The three most important load balancing facts for data engineers: Kafka's partition-leader model makes it fundamentally incompatible with shared round-robin load balancing for data-plane traffic — each broker must be reachable at a distinct address, and `advertised.listeners` must contain those addresses. PgBouncer in transaction mode reduces Postgres backend connections from one-per-application-thread to one-per-concurrent-query, typically a 10–20× reduction — the single highest-leverage database performance optimization for most data platforms. Connection pooling and load balancer topology are prerequisites to understanding why a system's performance is what it is; changing the topology often matters more than tuning individual components.

At the Kubernetes layer, kube-proxy Services provide ClusterIP-based load balancing for stateless services, but Kafka and other stateful workloads require headless services + StatefulSets for direct pod addressing. At scale (thousands of services), IPVS mode replaces iptables to avoid O(N) routing overhead. In service mesh environments, Envoy sidecar proxies add circuit breaking and automatic mTLS, but Kafka's protocol requires careful configuration to prevent the sidecar from interfering with partition-leader-aware connections.

---

## 16. Connections to Other Modules

- **M27 — TCP/IP Fundamentals:** Load balancers create TCP connections on both sides. Understanding connection states (ESTABLISHED, TIME_WAIT, CLOSE_WAIT) is prerequisite to diagnosing connection distribution problems. The BDP calculation informs NLB buffer sizing for cross-region replication.
- **M28 — DNS and Service Discovery:** `advertised.listeners` is a DNS-based service discovery mechanism. Kubernetes ClusterIP and headless service DNS (M28) are the DNS layer that kube-proxy's load balancing acts on.
- **M29 — TLS and mTLS:** L4 NLBs support TLS passthrough (required for mTLS), while L7 ALBs terminate TLS (incompatible with mTLS). Choosing the right load balancer type requires understanding TLS termination.
- **M30 — Network Latency:** Load balancers add latency (L4 ≈ 0.1 ms, L7 ≈ 1–3 ms, service mesh sidecar ≈ 0.2–0.5 ms). This overhead must be included in latency budgets for data pipelines.
- **DCS-KFK-101 — Apache Kafka Internals:** The partition leader model, advertised listeners, and bootstrap mechanism are covered in Kafka internals. This module provides the network-layer foundation for understanding why Kafka's listener configuration works the way it does.
- **SYS-NET-102 — Cloud Networking and Security:** Cloud-specific load balancers (AWS NLB, ALB, GCP Network LB, Azure Application Gateway) are covered in depth in the cloud networking course, building on the L4/L7 concepts introduced here.

---

## 17. Anti-Patterns

**Anti-pattern: Using `advertised.listeners` with internal Kubernetes pod IPs.** Kafka broker pod IPs change every time a pod restarts. If `advertised.listeners` uses pod IPs, clients that cached the old IP will fail after the pod restarts. Use Kubernetes StatefulSet DNS names (`kafka-0.kafka.kafka-prod.svc.cluster.local`) which are stable across pod restarts.

**Anti-pattern: Using a single HikariCP pool for mixed OLTP and OLAP queries.** Long-running analytical queries (OLAP) hold connections for seconds to minutes, starving fast operational queries (OLTP) of connections. Use separate pools: a small pool (2–5 connections) with `connectionTimeout=1s` for OLTP and a larger pool with longer timeouts for OLAP. This ensures OLTP queries always have a connection available.

**Anti-pattern: Setting PgBouncer `max_client_conn` lower than the application's total thread count.** If `max_client_conn=200` but 300 application threads connect to PgBouncer simultaneously, 100 threads get `Connection refused`. Always set `max_client_conn` higher than the maximum number of application threads that could connect simultaneously — 2–4× the application thread count is a safe margin.

**Anti-pattern: Configuring Istio to intercept Kafka ports.** The Envoy sidecar proxy intercepts all TCP traffic by default. For Kafka connections, where the client discovers broker addresses from metadata and connects directly to specific brokers, having Envoy intercept and potentially reroute these connections can break partition-leader-aware routing. Exclude Kafka ports from Envoy's iptables rules: in the pod's annotation: `traffic.sidecar.istio.io/excludeOutboundPorts: "9092,9093,9094"`.

**Anti-pattern: Using ALB for Kafka Connect or Schema Registry with long-lived connections.** ALBs have an idle connection timeout (default 60 seconds). Long-lived HTTP connections (like those used by Kafka Connect workers polling the REST API) will be silently dropped at the ALB after 60 seconds of inactivity. Either increase the ALB idle timeout to match the application's connection lifetime, or implement connection keep-alive on the client.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `ss -tnp 'dst :<port>'` | Active connections to a port | Count, state, and process info |
| `ss -tn state established dst :<port> \| wc -l` | Count established connections | Quick connection count |
| `psql -h pgbouncer -p 6432 -c "SHOW POOLS;"` | PgBouncer pool statistics | cl_waiting = queue depth |
| `psql -h pgbouncer -p 6432 -c "SHOW STATS_TOTALS;"` | PgBouncer throughput stats | Queries/sec, avg latency |
| `kubectl get endpoints <svc>` | Kubernetes service backends | Shows pod IPs and ports |
| `kubectl edit configmap kube-proxy -n kube-system` | Switch kube-proxy to IPVS | Set `mode: ipvs` |
| `ipvsadm -L --stats` | IPVS routing table and stats | After switching to IPVS mode |
| `iptables -t nat -L KUBE-SVC-<hash> -n` | kube-proxy iptables rules | Shows probability-weighted backend selection |
| `kafka-metadata-quorum.sh --describe` | KRaft/broker topology | Shows leader, ISR, and lag |
| `kafka-topics.sh --bootstrap-server <host> --describe --topic <t>` | Topic partition/leader info | Verify partition leader distribution |
| `curl -s http://envoy-admin:15000/stats \| grep upstream` | Envoy proxy stats | Circuit breaker status, connection counts |
| `pilot-agent request GET stats` | Istio sidecar stats | From inside a pod |
| `ethtool -l eth0` | NIC queue count and channels | Tune with `-L` for RSS |

---

## 19. Glossary

**Advertised Listeners (Kafka):** The addresses each Kafka broker publishes to clients in metadata responses. Clients use these addresses to connect directly to specific brokers after metadata discovery.

**Circuit Breaker:** A stability pattern that stops routing to a failing backend after it exceeds an error threshold. Immediately returns errors to callers. Prevents cascading failures.

**ClusterIP:** A stable virtual IP assigned to a Kubernetes Service. kube-proxy routes traffic to the ClusterIP to healthy pod endpoints.

**Connection Pool:** A set of pre-established connections to a backend, shared by multiple application threads. Amortizes connection setup cost.

**Consistent Hashing:** A load balancing algorithm that routes requests with the same key to the same backend using a hash ring, minimizing redistribution on backend changes.

**IPVS (IP Virtual Server):** Linux kernel-level load balancing using hash tables. Used by kube-proxy in IPVS mode for O(1) service routing, replacing iptables (O(N)).

**L4 Load Balancer:** A load balancer that operates at the TCP/UDP layer. Routes by IP, port, and TCP state. Cannot read application payload. Example: AWS Network Load Balancer.

**L7 Load Balancer:** A load balancer that operates at the HTTP/application layer. Routes by URL, headers, and request content. Example: AWS Application Load Balancer.

**Least Connections:** A load balancing algorithm that routes new connections to the backend with the fewest active connections. Better than round-robin for variable-duration requests.

**NLB (Network Load Balancer):** AWS's Layer 4 load balancer. Supports TCP passthrough, TLS termination, and source IP preservation. Suitable for Kafka and other non-HTTP protocols.

**PgBouncer:** A Postgres-specific connection multiplexer. Maintains a pool of real Postgres backend connections and lends them to client applications. Dramatically reduces Postgres connection count.

**Proxy Protocol:** A header prepended to TCP connections by L4 load balancers, carrying the original client IP and port. Allows backends to see the real client IP even after NAT.

**Reverse Proxy:** A proxy that sits in front of servers, intercepting client connections and distributing them to backend servers. The client sees only the proxy's address.

**Round Robin:** A load balancing algorithm that distributes requests sequentially across backends. Simple but does not account for variable request cost.

**Service Mesh:** An infrastructure layer that adds sidecar proxies (typically Envoy) to every pod, providing automatic mTLS, load balancing, circuit breaking, and observability.

**Session Mode (PgBouncer):** Backend connection held for the entire client session. No connection sharing benefit. Required for session-level features (temp tables, advisory locks).

**SNI (Server Name Indication):** A TLS extension that includes the target hostname in the ClientHello. Allows L4 load balancers to route TLS connections based on hostname without decrypting.

**Sticky Session:** A load balancing feature that routes all requests from the same client to the same backend. Required for stateful backends; causes imbalanced load.

**Transaction Mode (PgBouncer):** Backend connection released after each transaction. Allows 10 connections to serve hundreds of threads. Cannot use session-level features.

---

## 20. Self-Assessment

1. You have a 5-broker Kafka cluster and want to expose it to clients outside the Kubernetes cluster. Design the load balancer topology. How many NLBs do you need, and what do you set in `advertised.listeners`?
2. A data pipeline application uses 100 threads, each making 50 JDBC queries per second to Postgres. Each query takes 2 ms on average. Using Little's Law, what is the minimum connection pool size needed? What would you actually configure, and why?
3. Explain why PgBouncer transaction mode is incompatible with `SET search_path = myschema`. What PgBouncer mode or workaround would you use for applications that depend on `SET` commands?
4. A Kubernetes cluster has 8,000 Services and is experiencing slow connection setup times for new pods. Describe the root cause and the fix.
5. Your Envoy sidecar proxy is causing Kafka producers to fail with topology errors. What Istio annotation would you add to the producer pod to fix this, and what does it do?
6. Describe the three states of an Envoy circuit breaker and explain what happens in the "half-open" state.
7. An ALB in front of your Schema Registry has an idle timeout of 60 seconds. Schema Registry clients hold long-lived connections. What symptom appears, and what is the fix?
8. What is Proxy Protocol, and in what scenario is it required for a Kafka deployment using an NLB?
9. Explain the difference between `connections.max.idle.ms` in Kafka and `pool_size` in HikariCP. What does each control, and what happens when each is set too high vs too low?
10. A PgBouncer `SHOW POOLS` output shows `cl_waiting=45`. What does this mean, and what is the immediate action and root cause investigation?

---

## 21. Module Summary

Load balancing and proxies are the final layer of network infrastructure that data engineers must understand. Every production data system distributes traffic across multiple servers, and the mechanism for that distribution — the load balancer — has topology requirements that are specific to each protocol. Kafka, Postgres, HTTP services, and gRPC services each have different requirements, and applying the wrong load balancing model breaks correctness, not just performance.

The central lesson of this module is protocol awareness. Kafka's partition-leader model requires each broker to be directly addressable — a shared round-robin load balancer in front of Kafka brokers violates this requirement and causes internal forwarding or `NotLeaderOrFollowerException`. `advertised.listeners` is the configuration that makes each broker's unique address visible to clients; getting it wrong is the most common Kafka networking misconfiguration. By contrast, HTTP services like Schema Registry and Kafka Connect work correctly behind a shared L7 ALB, which adds URL-based routing, TLS termination, and health checking capabilities that L4 NLBs cannot provide.

Connection pooling — implemented via HikariCP for JDBC and PgBouncer for Postgres — is one of the highest-leverage performance optimizations in data engineering. Little's Law provides the analytical foundation for pool sizing: `pool_size = throughput × latency`. PgBouncer's transaction mode extends this to Postgres specifically, reducing backend connection count from one-per-thread to one-per-concurrent-transaction — typically a 10–20× reduction.

At the Kubernetes layer, kube-proxy's ClusterIP services provide DNS-addressed L4 load balancing for stateless services, while headless services with StatefulSets provide the direct pod addressing that Kafka requires. Service meshes add Envoy sidecar proxies that implement circuit breaking, automatic mTLS, and observability — but Kafka's topology-aware connection model requires careful sidecar configuration to avoid interference.

This module completes SYS-NET-101 — Networking for Data Engineers. Together, the five modules have built a complete networking foundation: TCP/IP fundamentals (M27) → DNS and service discovery (M28) → TLS and mTLS (M29) → network latency and its distributed systems implications (M30) → load balancing and proxies (M31). The next course in sequence, SYS-NET-102 — Cloud Networking and Security, extends this foundation to cloud-specific networking: VPC architecture, IAM, security groups, and private endpoints.
