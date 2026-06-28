# M36: Cloud DNS and Service Mesh Basics

**Course:** SYS-NET-102 — Cloud Networking and Security  
**Module:** 05 of 05  
**Global Module ID:** M36  
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

Every distributed data system relies on a foundational assumption: services can find each other by name rather than by IP address. This assumption is so fundamental that it is almost invisible — until it breaks. When a Spark job loses connection to the schema registry, when Kafka Connect cannot resolve broker hostnames, when Airflow's metadata database becomes unreachable after an RDS failover, the root cause is almost always in the DNS and service discovery layer rather than in any application configuration.

DNS is the phonebook of every distributed system. IP addresses are ephemeral — an EC2 instance is terminated and replaced, an RDS primary fails over to a standby, a Kubernetes pod is rescheduled to a different node. The service's IP address changes but its DNS name stays constant. Everything that depends on that service must not have the IP address hardcoded; it must resolve the DNS name at connection time. This is not a Kubernetes concept or a cloud concept — it is the foundational operational principle of all distributed systems since the 1980s.

Data engineers encounter DNS in several recurring situations. First, Kafka bootstrap servers: producers and consumers connect to bootstrap broker hostnames, which return partition metadata with broker addresses. If those broker addresses are not DNS-resolvable by clients (M31's `advertised.listeners` problem), the client connects to bootstrap but cannot reach the actual partition leader. Second, database failover: AWS RDS Multi-AZ uses a single DNS CNAME that always points to the current primary. When a failover occurs (taking 30–60 seconds), the CNAME is updated to point to the new primary. Any application with a hardcoded IP address is permanently broken after a failover; any application that re-resolves the DNS name reconnects automatically. Third, Kubernetes-internal communication: every Kubernetes Service gets a DNS name in the cluster's internal DNS zone (`service.namespace.svc.cluster.local`). Pods that reference services by IP instead of DNS name break every time a service is deleted and recreated.

Service meshes — specifically Istio and Envoy — extend DNS-based service discovery with traffic management, observability, and security capabilities that are transparent to application code. For data engineering, the two most relevant service mesh capabilities are mTLS (mutual TLS: automatic encryption and authentication for all inter-service traffic) and traffic management (circuit breaking, retries, and timeout policies applied at the mesh level rather than hardcoded in each application). Understanding how a service mesh intercepts traffic explains many behaviors that otherwise appear mysterious — why does adding a timeout in application code seem to have no effect? (Because the mesh's timeout policy fires first.) Why can two pods that should communicate according to security groups still fail to connect? (Because Istio's AuthorizationPolicy is denying the request after TLS handshake.)

---

## 2. Mental Model

### DNS as the Service Registry

In any distributed system, service discovery requires three things: an identifier (the service name), a registry (maps names to current endpoints), and a client (resolves names before connecting). DNS provides all three:

- **Identifier:** A fully-qualified domain name (FQDN) like `kafka.prod.internal` or `schema-registry.kafka-prod.svc.cluster.local`
- **Registry:** DNS servers that store A records (name → IP), CNAME records (name → another name), and SRV records (name → host + port)
- **Client:** The DNS resolver, embedded in every OS and container, which is called before every new TCP connection when the hostname is not yet in the local cache

Every TCP connection follows this sequence: resolve hostname → connect to IP. The DNS resolution step is often invisible (it takes 1–2ms and the OS caches results) but it is mandatory. When DNS is broken, no connection can be established — the application never gets to the TCP handshake. This is why DNS debugging should always come before network debugging: if `dig kafka.prod.internal` returns nothing, there is no point testing TCP connectivity to port 9092.

### Kubernetes DNS: The Predictable Naming Scheme

Kubernetes runs its own DNS server (CoreDNS) for in-cluster service discovery. Every Kubernetes Service object gets a DNS entry automatically created by CoreDNS:

```
<service-name>.<namespace>.svc.<cluster-domain>
```

For `cluster.local` domain (the default):
- `kafka` service in namespace `kafka-prod` → `kafka.kafka-prod.svc.cluster.local`
- Same namespace: can use short name `kafka` or `kafka.kafka-prod`
- Cross namespace: must use `kafka.kafka-prod.svc.cluster.local` or `kafka.kafka-prod`

This naming scheme is completely predictable and does not require any registration — it is automatic when a Service object exists. The moment a Service is created, its DNS entry is live. The moment it is deleted, the DNS entry is gone. Pods that connect to services by DNS name are automatically load-balanced across the service's endpoints (the healthy pods matching the selector).

### Service Mesh: Transparent Traffic Management

A service mesh like Istio operates through sidecar injection: a second container (Envoy proxy) is injected alongside every application pod. Network traffic is intercepted by iptables rules at the network namespace level — the application believes it is connecting directly to the destination, but in reality, all outbound and inbound traffic passes through the local Envoy sidecar first. The application code requires zero changes.

The mesh enables capabilities at the proxy layer that would otherwise require changes in every application:
- **mTLS:** Each sidecar holds a certificate. All connections between sidecars are mutually authenticated and encrypted — even if the application uses plain HTTP internally. The application sends HTTP; the sidecar encrypts it to mTLS.
- **Circuit breaking:** The sidecar tracks error rates per upstream. If a destination is failing, the sidecar opens the circuit and fails fast rather than queuing up slow failing requests.
- **Retries:** Automatic retry logic configured via `VirtualService` resources — retry on 503, up to 3 times, with exponential backoff.
- **Observability:** Every request is traced (distributed tracing via Jaeger or Zipkin), metered (request counts, error rates, P99 latency), and logged — without application instrumentation.

---

## 3. Core Concepts

### 3.1 DNS Record Types Relevant to Data Engineering

**A record:** Maps a hostname to an IPv4 address. The most common record type.
```
kafka-broker-1.prod.internal  →  10.0.1.100
```

**CNAME record:** Maps a hostname to another hostname (an alias). AWS RDS, ElastiCache, and MSK use CNAMEs for failover.
```
postgres.prod.internal  →  db-instance-id.abc123.us-east-1.rds.amazonaws.com
```
Critical property: CNAME resolution requires an extra DNS lookup. After RDS failover, the CNAME is updated to point to the new primary's hostname. Applications that re-resolve the CNAME pick up the new primary automatically. Applications that cache the resolved IP past the TTL remain pointed at the old (now standby) primary.

**SRV record:** Maps a service name to a hostname AND port. Used by some service discovery systems (Consul, Kubernetes headless services) to advertise service locations including port information.
```
_kafka._tcp.prod.internal  →  kafka-1.prod.internal:9092
```

**PTR record:** Reverse DNS — maps an IP to a hostname. Used for logging and security tools that want to show hostnames instead of IPs.

**TTL (Time To Live):** How long a resolver may cache the DNS answer. For stable services, TTLs of 300–3600 seconds are normal. For failover scenarios, AWS RDS uses a TTL of 5 seconds on its failover CNAME so that applications reconnect within seconds of a failover event.

### 3.2 Route 53: AWS DNS for Data Platforms

Route 53 provides two types of DNS relevant to data engineering:

**Public hosted zones:** DNS for publicly-routable domain names. Not relevant for internal data platform communication.

**Private hosted zones:** DNS zones that are only resolvable from within specific VPCs. This is the mechanism for giving internal services human-readable names without exposing them to the public internet.

```bash
# Create a private hosted zone for internal data platform DNS
aws route53 create-hosted-zone \
  --name "data.prod.internal" \
  --caller-reference "$(date +%s)" \
  --hosted-zone-config "PrivateZone=true,Comment=Data platform internal DNS" \
  --vpc "VPCRegion=us-east-1,VPCId=vpc-0abc123"

# Add an A record for Kafka
aws route53 change-resource-record-sets \
  --hosted-zone-id Z1234567890 \
  --change-batch '{
    "Changes": [{
      "Action": "CREATE",
      "ResourceRecordSet": {
        "Name": "kafka.data.prod.internal",
        "Type": "A",
        "TTL": 60,
        "ResourceRecords": [
          {"Value": "10.0.1.100"},
          {"Value": "10.0.1.101"},
          {"Value": "10.0.1.102"}
        ]
      }
    }]
  }'
```

**Route 53 Resolver:** The DNS resolver in every VPC. By default, it answers queries for the VPC's private hosted zones and forwards everything else to Route 53's public resolvers. For hybrid architectures (on-premises to cloud DNS resolution), Resolver inbound and outbound endpoints enable bidirectional DNS resolution across Direct Connect or VPN.

**Route 53 Health Checks + Failover:** Route 53 can monitor endpoint health and automatically update DNS to point to a secondary endpoint when the primary becomes unhealthy. Useful for active-passive database failover at the DNS level.

### 3.3 AWS Route 53 vs CoreDNS in Kubernetes

When a pod in Kubernetes makes a DNS query, it reaches CoreDNS first (the cluster's internal resolver). CoreDNS answers queries for cluster-internal names (`*.cluster.local`) from its own cache and from the Kubernetes API (watching Services and Endpoints objects). For external names (anything not `*.cluster.local`), CoreDNS forwards to the VPC's Route 53 Resolver (169.254.169.254).

This creates a two-tier DNS resolution path:

```
Pod queries "kafka.kafka-prod.svc.cluster.local"
  → CoreDNS (kube-dns service, typically 10.96.0.10)
  → Answer from CoreDNS cache (from K8s API Service watcher)
  → Returns ClusterIP of the kafka service

Pod queries "secretsmanager.us-east-1.amazonaws.com"
  → CoreDNS
  → Not in cluster.local zone → forward to 169.254.169.254 (VPC resolver)
  → Route 53 Resolver checks private hosted zones
  → If interface endpoint private DNS enabled: returns 10.x.x.x (endpoint ENI IP)
  → Returns IP to pod
```

This chain is why M35's private endpoint DNS depends on CoreDNS correctly forwarding to the VPC resolver — and why disabling `enableDnsSupport` on the VPC breaks both interface endpoint DNS and pod external DNS resolution simultaneously.

### 3.4 Kubernetes Service Types and DNS

**ClusterIP (default):** Creates a virtual IP (the ClusterIP) that is only routable within the cluster. DNS resolves the service name to this virtual IP; kube-proxy maintains iptables rules that DNAT the virtual IP to one of the healthy pod IPs.

```
kafka.kafka-prod.svc.cluster.local → 10.96.45.100 (ClusterIP)
  → iptables DNAT → 10.0.1.155 (pod IP, chosen randomly/IPVS)
```

**Headless service (`clusterIP: None`):** No virtual IP is assigned. DNS returns the individual pod IPs directly (A records for each pod). Used when clients need to connect to specific pods (stateful services like Kafka, ZooKeeper, Cassandra).

```yaml
# Headless service for Kafka (clients need per-broker connections)
apiVersion: v1
kind: Service
metadata:
  name: kafka-headless
  namespace: kafka-prod
spec:
  clusterIP: None     # ← Headless
  selector:
    app: kafka
  ports:
  - port: 9092
    name: kafka
```

```
# DNS returns all pod IPs:
kafka-headless.kafka-prod.svc.cluster.local → 10.0.1.155, 10.0.1.156, 10.0.1.157

# StatefulSet pods get individual DNS names:
kafka-0.kafka-headless.kafka-prod.svc.cluster.local → 10.0.1.155
kafka-1.kafka-headless.kafka-prod.svc.cluster.local → 10.0.1.156
kafka-2.kafka-headless.kafka-prod.svc.cluster.local → 10.0.1.157
```

This is critical for Kafka in Kubernetes: each broker needs a stable, individually-addressable hostname so that clients receiving partition metadata can construct the correct `advertised.listeners` address for broker-specific connections (M31). Kafka deployed as a StatefulSet with a headless service provides exactly this: `kafka-0.kafka-headless.kafka-prod.svc.cluster.local` is stable even when `kafka-0`'s pod IP changes after a restart.

**LoadBalancer:** Creates an external load balancer (NLB in AWS) with a stable DNS name. The load balancer's DNS name is registered in Route 53 or your DNS system. Used for external access to Kafka (from producers/consumers outside the cluster).

**ExternalName:** Maps a Kubernetes service name to an external DNS name. Useful for giving a Kubernetes-style name to an external database:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: data-pipelines
spec:
  type: ExternalName
  externalName: db-prod.abc123.us-east-1.rds.amazonaws.com
```

Pods in `data-pipelines` can connect to `postgres.data-pipelines.svc.cluster.local` and CoreDNS returns a CNAME to the RDS hostname. The RDS endpoint is resolved through normal DNS. When RDS fails over and the RDS hostname CNAME changes, pods using the ExternalName service automatically pick up the new endpoint without any Kubernetes configuration change.

### 3.5 CoreDNS Configuration

CoreDNS is configured via a ConfigMap (`coredns` in the `kube-system` namespace) called the Corefile:

```
# Default Corefile
.:53 {
    errors
    health {
       lameduck 5s
    }
    ready
    kubernetes cluster.local in-addr.arpa ip6.arpa {
       pods insecure
       fallthrough in-addr.arpa ip6.arpa
       ttl 30
    }
    prometheus :9153
    forward . /etc/resolv.conf {
       max_concurrent 1000
    }
    cache 30
    loop
    reload
    loadbalance
}
```

Key directives:
- `kubernetes cluster.local`: handles all `*.cluster.local` queries from the K8s API
- `forward . /etc/resolv.conf`: forwards non-cluster queries to the upstream resolver (VPC resolver at 169.254.169.254 on EKS)
- `cache 30`: caches DNS responses for 30 seconds (can be tuned up or down)

**Customizing CoreDNS for a private hosted zone:** If your data platform uses a private Route 53 zone (`data.prod.internal`), and you want pods to resolve those names, add a stub zone to the CoreDNS ConfigMap:

```
data.prod.internal:53 {
    errors
    cache 30
    forward . 169.254.169.254   # Forward to VPC resolver for this zone
}
```

Without this stub zone, a pod querying `kafka.data.prod.internal` would first check the `cluster.local` zone (not found), then fall through to the forward directive which queries the VPC resolver — which would actually work for most cases. But an explicit stub zone is cleaner and avoids edge cases with search domain resolution.

### 3.6 DNS Search Domains and NDOTS

Kubernetes pods have a search domain list in `/etc/resolv.conf`:

```
search kafka-prod.svc.cluster.local svc.cluster.local cluster.local us-east-1.compute.internal
nameserver 10.96.0.10
options ndots:5
```

`ndots:5` means: if the hostname has fewer than 5 dots, try appending each search domain before attempting the hostname as an absolute FQDN. For `kafka`, which has 0 dots, the resolver tries:
1. `kafka.kafka-prod.svc.cluster.local` (current namespace)
2. `kafka.svc.cluster.local`
3. `kafka.cluster.local`
4. `kafka.us-east-1.compute.internal`
5. `kafka` (as absolute)

This is why short names like `kafka` work within the same namespace — step 1 succeeds. But for external hostnames with many dots (like `secretsmanager.us-east-1.amazonaws.com` — 4 dots, still < 5), the resolver exhausts all search domains before trying the FQDN. This generates 4 unnecessary DNS queries before the correct answer, adding latency to every first connection.

**Mitigation:** For external fully-qualified hostnames with 4+ dots, append a trailing dot to force absolute resolution and skip search domain appending: `secretsmanager.us-east-1.amazonaws.com.` (note the trailing dot). Alternatively, lower `ndots` to 2 for pods that primarily communicate with external services, at the cost of requiring fully-qualified names for cross-namespace services.

### 3.7 Istio Service Mesh Basics

Istio is the most widely deployed service mesh. Its architecture has two planes:

**Control plane (Istiod):** Manages the mesh configuration. Watches Kubernetes resources (Services, Endpoints, VirtualServices, DestinationRules, AuthorizationPolicies). Distributes configuration to all Envoy sidecars via the xDS protocol (Envoy's configuration API). Manages certificate issuance and rotation for mTLS.

**Data plane (Envoy sidecars):** One Envoy proxy container injected into every pod (via Istio's MutatingWebhookConfiguration). Iptables rules in each pod's network namespace redirect all inbound and outbound traffic through the local Envoy. The application code is unmodified — it never knows it's talking through a proxy.

**Key Istio resources for data engineering:**

```yaml
# VirtualService: traffic management rules for a service
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: kafka-vs
  namespace: kafka-prod
spec:
  hosts:
  - kafka.kafka-prod.svc.cluster.local
  http:
  - retries:
      attempts: 3
      perTryTimeout: 5s
      retryOn: 5xx,connect-failure,reset
    timeout: 30s
  tcp:
  - route:
    - destination:
        host: kafka.kafka-prod.svc.cluster.local
```

```yaml
# DestinationRule: connection pool and circuit breaker settings
apiVersion: networking.istio.io/v1alpha3
kind: DestinationRule
metadata:
  name: kafka-dr
  namespace: kafka-prod
spec:
  host: kafka.kafka-prod.svc.cluster.local
  trafficPolicy:
    connectionPool:
      tcp:
        maxConnections: 100
      http:
        http2MaxRequests: 1000
        maxRequestsPerConnection: 10
    outlierDetection:
      consecutive5xxErrors: 5
      interval: 30s
      baseEjectionTime: 30s
      maxEjectionPercent: 50
```

```yaml
# PeerAuthentication: enforce mTLS for all traffic in a namespace
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: default
  namespace: kafka-prod
spec:
  mtls:
    mode: STRICT   # All traffic must use mTLS; plain HTTP is rejected
```

```yaml
# AuthorizationPolicy: allow only schema-registry to call the kafka REST proxy
apiVersion: security.istio.io/v1beta1
kind: AuthorizationPolicy
metadata:
  name: kafka-rest-allow-schema-registry
  namespace: kafka-prod
spec:
  selector:
    matchLabels:
      app: kafka-rest-proxy
  rules:
  - from:
    - source:
        principals:
        - "cluster.local/ns/kafka-prod/sa/schema-registry"
```

### 3.8 mTLS in Data Platforms

Mutual TLS (mTLS) is TLS where both sides present certificates — the server proves its identity to the client AND the client proves its identity to the server. In a service mesh, Istiod acts as the Certificate Authority: it issues SPIFFE-format X.509 certificates to each pod based on its Kubernetes ServiceAccount identity.

For data platforms, mTLS provides two important properties: **encryption** (all inter-service traffic is encrypted in transit, even between pods on the same node) and **authentication** (a Kafka producer pod can only be from a pod with a certificate issued by the cluster's CA for a permitted ServiceAccount — no impersonation possible).

This is particularly relevant for Kafka, which has its own TLS and SASL authentication mechanism (M27, M31). In an Istio mesh, you have two layers: Istio mTLS (handled by Envoy sidecars, transparent to Kafka clients) and Kafka's own authentication (SASL/SCRAM or mTLS certificates for Kafka protocol). The two layers can coexist. In practice, many teams use Istio mTLS for in-cluster traffic and Kafka SASL/PLAIN for application-level authentication, getting both network encryption and application-level identity.

---

## 4. Hands-On Walkthrough

### 4.1 Diagnosing DNS Resolution in Kubernetes

```bash
# From inside a pod, check what DNS server is configured
cat /etc/resolv.conf
# nameserver 10.96.0.10    ← CoreDNS ClusterIP
# search kafka-prod.svc.cluster.local svc.cluster.local cluster.local ...
# options ndots:5

# Test cluster-internal DNS resolution
kubectl exec -n data-pipelines spark-pod-xyz -- \
  nslookup kafka.kafka-prod.svc.cluster.local
# Server:  10.96.0.10      ← CoreDNS
# Name:    kafka.kafka-prod.svc.cluster.local
# Address: 10.96.45.100    ← ClusterIP

# Test headless service resolution (returns pod IPs)
kubectl exec -n data-pipelines spark-pod-xyz -- \
  nslookup kafka-headless.kafka-prod.svc.cluster.local
# Name: kafka-headless.kafka-prod.svc.cluster.local
# Address: 10.0.1.155
# Address: 10.0.1.156
# Address: 10.0.1.157

# Test individual pod DNS via StatefulSet headless service
kubectl exec -n data-pipelines spark-pod-xyz -- \
  nslookup kafka-0.kafka-headless.kafka-prod.svc.cluster.local
# Address: 10.0.1.155

# Debug search domain expansion: use dig with +search to trace
kubectl exec -n data-pipelines spark-pod-xyz -- \
  dig +search +trace kafka
# Shows all search domain attempts

# Use a dedicated debug pod with full dns tools
kubectl run dnsutils --image=gcr.io/kubernetes-e2e-test-images/dnsutils:1.3 \
  -n data-pipelines --restart=Never -it --rm -- /bin/sh
  # Inside: dig, nslookup, host all available
```

### 4.2 Creating a Private Route 53 Zone for Internal Services

```bash
# Create the private hosted zone
ZONE_ID=$(aws route53 create-hosted-zone \
  --name "data.prod.internal" \
  --caller-reference "$(date +%s)" \
  --hosted-zone-config "PrivateZone=true" \
  --vpc "VPCRegion=us-east-1,VPCId=vpc-0abc123" \
  --query 'HostedZone.Id' --output text | cut -d/ -f3)

echo "Zone ID: $ZONE_ID"

# Add Kafka bootstrap record (multi-value for broker IPs)
aws route53 change-resource-record-sets \
  --hosted-zone-id $ZONE_ID \
  --change-batch '{
    "Changes": [{
      "Action": "CREATE",
      "ResourceRecordSet": {
        "Name": "kafka-bootstrap.data.prod.internal",
        "Type": "A",
        "TTL": 60,
        "MultiValueAnswer": true,
        "SetIdentifier": "broker-1",
        "ResourceRecords": [{"Value": "10.0.1.100"}],
        "HealthCheckId": "HC_KAFKA_1"
      }
    }]
  }'

# Verify the record resolves from inside the VPC
# (Run from an EC2 instance in the same VPC)
dig kafka-bootstrap.data.prod.internal
# Should return broker IPs
```

### 4.3 Inspecting CoreDNS Health and Cache

```bash
# Check CoreDNS pods are running
kubectl get pods -n kube-system -l k8s-app=kube-dns
# NAME                       READY   STATUS    RESTARTS   AGE
# coredns-66bff467f8-2kpqd   1/1     Running   0          10d
# coredns-66bff467f8-9w5rp   1/1     Running   0          10d

# Check CoreDNS logs for query errors
kubectl logs -n kube-system -l k8s-app=kube-dns --tail=50

# View current CoreDNS ConfigMap
kubectl get configmap coredns -n kube-system -o yaml

# Check CoreDNS metrics (Prometheus endpoint)
kubectl port-forward -n kube-system svc/kube-dns 9153:9153 &
curl -s http://localhost:9153/metrics | grep -E 'coredns_dns_requests|coredns_dns_responses|coredns_cache'
# coredns_dns_requests_total{...} 1234
# coredns_cache_hits_total{...}   987
# coredns_cache_misses_total{...} 247

# Cache hit ratio = 987/(987+247) = ~80% (healthy)
# Low cache hit ratio → check TTL settings or high pod churn (many unique names)
```

### 4.4 Debugging Istio mTLS and Traffic Policies

```bash
# Check if Istio sidecar is injected in a pod
kubectl get pod kafka-0 -n kafka-prod -o jsonpath='{.spec.containers[*].name}'
# kafka istio-proxy    ← two containers: app + sidecar

# Check Istio proxy status for a pod
istioctl proxy-status
# NAME                      CDS  LDS  EDS  RDS  ECDS  ISTIOD       VERSION
# kafka-0.kafka-prod        SYNCED SYNCED SYNCED SYNCED ...

# Analyze traffic to understand what policies apply
istioctl analyze -n kafka-prod
# ✓ No validation messages found when namespace is configured correctly
# ✗ VirtualService references a host "kafka" not defined in registry

# Check mTLS status between two pods
istioctl x check-inject -n kafka-prod
# Lists which pods have injection enabled

# Get Envoy proxy config for a pod (to see routes and clusters)
istioctl proxy-config listeners kafka-0.kafka-prod
istioctl proxy-config clusters kafka-0.kafka-prod
istioctl proxy-config routes kafka-0.kafka-prod

# Test L7 connectivity with Istio aware tool
kubectl exec -n data-pipelines spark-pod-xyz -c istio-proxy -- \
  curl -v http://schema-registry.kafka-prod.svc.cluster.local:8081/subjects

# View AuthorizationPolicy denials in Envoy access log
kubectl logs kafka-0 -n kafka-prod -c istio-proxy | \
  grep -E '"response_code":403|RBAC|denied'
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
dns_mesh_diagnostics.py

DNS and service mesh diagnostic toolkit for data engineers.
Diagnoses DNS resolution, validates service discovery, audits CoreDNS
configuration, and checks Istio mTLS and traffic policy health.

Dependencies: boto3 (optional, for Route53 audit), kubernetes (optional)
             Standard library sufficient for DNS and connectivity checks
"""

import os
import re
import json
import time
import socket
import subprocess
from dataclasses import dataclass, field
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


# ─── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class DNSResolutionResult:
    hostname: str
    resolved_ips: list[str]
    resolution_ms: float
    nameserver: Optional[str]
    ttl: Optional[int]
    is_cname: bool
    cname_target: Optional[str]
    error: Optional[str]


@dataclass
class ServiceDiscoveryCheck:
    service_name: str
    namespace: str
    fqdn: str
    cluster_ip: Optional[str]
    pod_ips: list[str]       # For headless services
    is_headless: bool
    reachable: bool
    error: Optional[str]


@dataclass
class CoreDNSHealth:
    pods_running: int
    pods_ready: int
    cache_hit_ratio: Optional[float]
    errors_per_minute: Optional[float]
    warnings: list[str]


@dataclass
class SearchDomainAnalysis:
    hostname: str
    ndots: int
    dot_count: int
    will_try_search_domains: bool
    search_domains_tried: list[str]
    extra_queries_before_success: int
    recommendation: str


# ─── DNS Diagnostics ─────────────────────────────────────────────────────────────

def resolve_hostname(
    hostname: str,
    timeout_s: float = 3.0,
) -> DNSResolutionResult:
    """
    Resolve a hostname and measure resolution time.
    Returns resolution time, IPs, and whether a CNAME chain is involved.
    """
    start = time.perf_counter()
    error = None
    resolved_ips = []
    is_cname = False
    cname_target = None

    try:
        # getaddrinfo does full resolution including CNAME chains
        results = socket.getaddrinfo(hostname, None, socket.AF_INET)
        resolved_ips = list({r[4][0] for r in results})
    except socket.gaierror as e:
        error = str(e)

    resolution_ms = (time.perf_counter() - start) * 1000

    # Try to detect CNAME via subprocess dig (if available)
    try:
        dig_result = subprocess.run(
            ['dig', '+short', 'CNAME', hostname],
            capture_output=True, text=True, timeout=3
        )
        if dig_result.returncode == 0 and dig_result.stdout.strip():
            is_cname = True
            cname_target = dig_result.stdout.strip().rstrip('.')
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Get TTL via dig if available
    ttl = None
    try:
        dig_result = subprocess.run(
            ['dig', '+noall', '+answer', hostname],
            capture_output=True, text=True, timeout=3
        )
        if dig_result.returncode == 0:
            for line in dig_result.stdout.splitlines():
                parts = line.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    ttl = int(parts[1])
                    break
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    return DNSResolutionResult(
        hostname=hostname,
        resolved_ips=resolved_ips,
        resolution_ms=resolution_ms,
        nameserver=None,
        ttl=ttl,
        is_cname=is_cname,
        cname_target=cname_target,
        error=error,
    )


def check_rds_failover_readiness(rds_endpoint: str) -> dict:
    """
    Validate that an RDS endpoint is properly configured for failover.
    Checks: CNAME structure, TTL, resolution time.

    RDS Multi-AZ endpoints use a low-TTL CNAME that updates on failover.
    Applications that cache IPs past the TTL are broken after failover.
    """
    result = resolve_hostname(rds_endpoint)
    warnings = []

    if result.error:
        return {
            'endpoint': rds_endpoint,
            'status': 'ERROR',
            'error': result.error,
            'warnings': ['DNS resolution failed — endpoint may be incorrect or unreachable'],
        }

    # RDS failover endpoints should be CNAMEs with low TTLs
    if not result.is_cname:
        warnings.append(
            "Endpoint resolved to A record (not CNAME). "
            "RDS Multi-AZ endpoints should use CNAME so failover is transparent. "
            "If this is not an RDS Multi-AZ endpoint, ensure your connection string "
            "uses the RDS-provided endpoint, not a hardcoded IP or static A record."
        )

    if result.ttl is not None:
        if result.ttl > 60:
            warnings.append(
                f"DNS TTL is {result.ttl}s. After an RDS failover, the CNAME will "
                f"update to point to the new primary, but clients will not pick it up "
                f"until the TTL expires. If TTL is 300s, clients may be stuck pointing "
                f"at the old primary for up to 5 minutes. AWS RDS sets TTL=5s on its "
                f"failover DNS entries. A high TTL here may indicate a custom DNS record "
                f"in Route 53 that should be lowered."
            )
        elif result.ttl <= 5:
            pass  # RDS standard low TTL — good

    if result.resolution_ms > 100:
        warnings.append(
            f"DNS resolution took {result.resolution_ms:.1f}ms. "
            f"This could indicate DNS latency under load. "
            f"Check CoreDNS cache hit ratio and pod count."
        )

    return {
        'endpoint': rds_endpoint,
        'status': 'OK' if not warnings else 'WARNING',
        'resolved_ips': result.resolved_ips,
        'is_cname': result.is_cname,
        'cname_target': result.cname_target,
        'ttl_seconds': result.ttl,
        'resolution_ms': round(result.resolution_ms, 2),
        'warnings': warnings,
        'failover_guidance': (
            "Applications must re-resolve the RDS hostname after each connection "
            "failure, not cache the resolved IP. SQLAlchemy with connection pool "
            "health checks, or PgBouncer with connection_lifetime, handles this "
            "correctly. Hardcoded IPs or very high DNS TTL caches will break failover."
        ),
    }


def analyze_search_domain_impact(
    hostname: str,
    ndots: int = 5,
    search_domains: Optional[list[str]] = None,
) -> SearchDomainAnalysis:
    """
    Analyze how many extra DNS queries will be generated for a hostname
    due to the ndots search domain expansion behavior.
    
    This matters for high-frequency service calls: if every first connection
    to an external service generates 4 wasted DNS queries, it adds latency
    and load on CoreDNS.
    """
    dot_count = hostname.rstrip('.').count('.')
    will_search = dot_count < ndots

    if search_domains is None:
        search_domains = [
            'svc.cluster.local',
            'cluster.local',
            'us-east-1.compute.internal',
        ]

    tried = []
    extra_queries = 0
    if will_search:
        for domain in search_domains:
            attempted = f"{hostname}.{domain}"
            tried.append(attempted)
            extra_queries += 1

    if will_search and not hostname.endswith('.'):
        recommendation = (
            f"'{hostname}' has {dot_count} dots (< ndots={ndots}). "
            f"The resolver will try {extra_queries} search domain(s) before "
            f"attempting the hostname directly. "
            f"Add a trailing dot ('{hostname}.') for absolute resolution, "
            f"or ensure ndots is set appropriately."
        )
    else:
        recommendation = (
            f"'{hostname}' will be resolved directly "
            f"(has {dot_count} dots >= ndots={ndots}, or ends with '.')."
        )

    return SearchDomainAnalysis(
        hostname=hostname,
        ndots=ndots,
        dot_count=dot_count,
        will_try_search_domains=will_search,
        search_domains_tried=tried,
        extra_queries_before_success=extra_queries,
        recommendation=recommendation,
    )


# ─── Kubernetes Service Discovery ────────────────────────────────────────────────

def check_kubernetes_service_dns(
    service: str,
    namespace: str,
    cluster_domain: str = 'cluster.local',
) -> ServiceDiscoveryCheck:
    """
    Check DNS resolution for a Kubernetes service.
    Tests both the standard FQDN and individual pod entries for headless services.
    Run this from inside a pod in the cluster.
    """
    fqdn = f"{service}.{namespace}.svc.{cluster_domain}"
    result = resolve_hostname(fqdn)

    is_headless = False
    pod_ips = []
    cluster_ip = None

    if result.error:
        return ServiceDiscoveryCheck(
            service_name=service,
            namespace=namespace,
            fqdn=fqdn,
            cluster_ip=None,
            pod_ips=[],
            is_headless=False,
            reachable=False,
            error=result.error,
        )

    # Headless services return multiple pod IPs directly (not a single ClusterIP)
    # ClusterIP services return a single virtual IP in the 10.96.0.0/12 range (typically)
    if len(result.resolved_ips) > 1:
        is_headless = True
        pod_ips = result.resolved_ips
    else:
        cluster_ip = result.resolved_ips[0] if result.resolved_ips else None

    return ServiceDiscoveryCheck(
        service_name=service,
        namespace=namespace,
        fqdn=fqdn,
        cluster_ip=cluster_ip,
        pod_ips=pod_ips,
        is_headless=is_headless,
        reachable=bool(result.resolved_ips),
        error=None,
    )


def check_data_platform_service_discovery(
    namespace_map: dict = None,
    cluster_domain: str = 'cluster.local',
) -> list[ServiceDiscoveryCheck]:
    """
    Check DNS resolution for a standard data platform service topology.
    
    namespace_map: {service_name: namespace} mapping.
    Defaults to typical data platform layout if not specified.
    """
    if namespace_map is None:
        namespace_map = {
            'kafka': 'kafka-prod',
            'kafka-headless': 'kafka-prod',
            'schema-registry': 'kafka-prod',
            'kafka-connect': 'kafka-prod',
            'postgres': 'data-pipelines',
            'airflow-webserver': 'airflow',
            'spark-history-server': 'data-pipelines',
        }

    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {
            ex.submit(check_kubernetes_service_dns, svc, ns, cluster_domain): (svc, ns)
            for svc, ns in namespace_map.items()
        }
        for future in as_completed(futures):
            results.append(future.result())

    return sorted(results, key=lambda r: (r.namespace, r.service_name))


# ─── Route 53 Private Zone Auditor ───────────────────────────────────────────────

class Route53Auditor:
    """Audit Route 53 private hosted zones for data platform DNS records."""

    def __init__(self, region: str = None):
        try:
            import boto3
            self.client = boto3.client(
                'route53',
                region_name=region or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
            )
        except ImportError:
            raise RuntimeError("boto3 not installed.")

    def list_private_zones(self, vpc_id: str) -> list[dict]:
        """List all private hosted zones associated with a VPC."""
        zones = []
        paginator = self.client.get_paginator('list_hosted_zones')
        for page in paginator.paginate():
            for zone in page['HostedZones']:
                if zone['Config']['PrivateZone']:
                    # Check VPC association
                    try:
                        assoc = self.client.list_vpc_association_authorizations(
                            HostedZoneId=zone['Id'].split('/')[-1]
                        )
                        vpcs = [v['VPCId'] for v in assoc.get('VPCs', [])]
                        if not vpc_id or vpc_id in vpcs:
                            zones.append({
                                'zone_id': zone['Id'].split('/')[-1],
                                'name': zone['Name'],
                                'record_count': zone['ResourceRecordSetCount'],
                                'vpcs': vpcs,
                            })
                    except Exception:
                        # Fall back to listing without VPC filter
                        zones.append({
                            'zone_id': zone['Id'].split('/')[-1],
                            'name': zone['Name'],
                            'record_count': zone['ResourceRecordSetCount'],
                        })
        return zones

    def get_zone_records(self, zone_id: str) -> list[dict]:
        """List all records in a hosted zone."""
        records = []
        paginator = self.client.get_paginator('list_resource_record_sets')
        for page in paginator.paginate(HostedZoneId=zone_id):
            for record in page['ResourceRecordSets']:
                values = []
                if 'ResourceRecords' in record:
                    values = [r['Value'] for r in record['ResourceRecords']]
                elif 'AliasTarget' in record:
                    values = [f"ALIAS→{record['AliasTarget']['DNSName']}"]

                records.append({
                    'name': record['Name'],
                    'type': record['Type'],
                    'ttl': record.get('TTL'),
                    'values': values,
                })
        return records

    def find_high_ttl_records(
        self,
        zone_id: str,
        threshold_s: int = 300,
    ) -> list[dict]:
        """Find records with TTL above threshold — risk for failover scenarios."""
        issues = []
        for record in self.get_zone_records(zone_id):
            if record['ttl'] and record['ttl'] > threshold_s:
                if record['type'] in ('A', 'CNAME'):
                    issues.append({
                        'name': record['name'],
                        'type': record['type'],
                        'ttl': record['ttl'],
                        'warning': (
                            f"TTL={record['ttl']}s. If this record points to an "
                            f"instance that can fail over, clients may cache the "
                            f"old IP for up to {record['ttl']}s after the failover."
                        )
                    })
        return issues


# ─── DNS Query Performance Benchmarker ───────────────────────────────────────────

def benchmark_dns_resolution(
    hostnames: list[str],
    iterations: int = 10,
) -> dict:
    """
    Benchmark DNS resolution time for a list of hostnames.
    Useful for comparing resolution latency with and without caching,
    or before/after CoreDNS tuning.
    """
    results = {}
    for hostname in hostnames:
        times = []
        errors = 0
        for _ in range(iterations):
            # Clear the OS resolver cache between iterations is not possible via Python,
            # but we can measure realistic cached-hit performance by running sequential calls
            r = resolve_hostname(hostname)
            if r.error:
                errors += 1
            else:
                times.append(r.resolution_ms)
            time.sleep(0.01)  # small gap between iterations

        if times:
            avg = sum(times) / len(times)
            p95 = sorted(times)[int(0.95 * len(times))]
            results[hostname] = {
                'avg_ms': round(avg, 2),
                'min_ms': round(min(times), 2),
                'max_ms': round(max(times), 2),
                'p95_ms': round(p95, 2),
                'errors': errors,
                'diagnosis': (
                    'FAST (cached)' if avg < 1 else
                    'NORMAL' if avg < 10 else
                    'SLOW — check CoreDNS health and cache hit ratio' if avg < 50 else
                    'VERY SLOW — DNS may be overloaded or misconfigured'
                ),
            }
        else:
            results[hostname] = {'error': 'all resolutions failed', 'errors': errors}

    return results


# ─── Istio Diagnostic Helpers ─────────────────────────────────────────────────────

def run_istioctl_command(args: list[str]) -> Optional[str]:
    """Run an istioctl command and return stdout, or None if not available."""
    try:
        result = subprocess.run(
            ['istioctl'] + args,
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def check_istio_proxy_sync(namespace: str = None) -> list[dict]:
    """
    Check Istio proxy sync status for pods in a namespace.
    Unsynced proxies may route traffic based on stale configuration.
    """
    args = ['proxy-status']
    if namespace:
        args += ['-n', namespace]

    output = run_istioctl_command(args)
    if not output:
        return [{'status': 'istioctl not available or Istio not installed'}]

    proxies = []
    for line in output.splitlines()[1:]:  # Skip header
        parts = line.split()
        if len(parts) >= 5:
            proxies.append({
                'pod': parts[0],
                'cds': parts[1],
                'lds': parts[2],
                'eds': parts[3],
                'rds': parts[4],
                'synced': all(p == 'SYNCED' for p in parts[1:5]),
                'warning': (
                    None if all(p == 'SYNCED' for p in parts[1:5])
                    else f"Proxy not fully synced: {dict(zip(['CDS','LDS','EDS','RDS'], parts[1:5]))}"
                ),
            })

    return proxies


def check_mtls_mode(namespace: str) -> dict:
    """
    Check what mTLS mode is configured for a namespace.
    Returns the effective PeerAuthentication mode.
    """
    result = subprocess.run(
        ['kubectl', 'get', 'peerauthentication', '-n', namespace, '-o', 'json'],
        capture_output=True, text=True, timeout=10,
    )

    if result.returncode != 0:
        return {
            'namespace': namespace,
            'mtls_mode': 'PERMISSIVE (default — no PeerAuthentication found)',
            'warning': (
                "No PeerAuthentication resource found. mTLS is PERMISSIVE by default "
                "— both plain HTTP and mTLS are accepted. "
                "For production, set mode: STRICT to enforce mTLS."
            ),
        }

    try:
        data = json.loads(result.stdout)
        for item in data.get('items', []):
            mode = (item.get('spec', {})
                       .get('mtls', {})
                       .get('mode', 'PERMISSIVE'))
            return {
                'namespace': namespace,
                'mtls_mode': mode,
                'resource_name': item['metadata']['name'],
                'warning': (
                    None if mode == 'STRICT'
                    else f"mTLS mode is {mode}. Consider STRICT for production namespaces."
                ),
            }
    except (json.JSONDecodeError, KeyError):
        pass

    return {'namespace': namespace, 'mtls_mode': 'unknown', 'error': 'parse failed'}


# ─── Full Report ─────────────────────────────────────────────────────────────────

def dns_mesh_report(
    rds_endpoint: Optional[str] = None,
    kafka_hostnames: Optional[list[str]] = None,
    k8s_services: Optional[dict] = None,
    check_istio: bool = False,
    istio_namespace: Optional[str] = None,
    benchmark: bool = False,
) -> None:
    """Print a comprehensive DNS and service mesh diagnostic report."""
    print("=" * 70)
    print("DNS / SERVICE MESH DIAGNOSTIC REPORT")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # RDS failover readiness
    if rds_endpoint:
        print(f"\n[1] RDS FAILOVER READINESS: {rds_endpoint}")
        check = check_rds_failover_readiness(rds_endpoint)
        print(f"  Status: {check['status']}")
        print(f"  Resolved: {check.get('resolved_ips', [])}")
        print(f"  Is CNAME: {check.get('is_cname', 'unknown')}")
        print(f"  TTL: {check.get('ttl_seconds', 'unknown')}s")
        for w in check.get('warnings', []):
            print(f"  ⚠️  {w[:100]}...")

    # Kafka DNS resolution
    if kafka_hostnames:
        print(f"\n[2] KAFKA HOST RESOLUTION")
        for hostname in kafka_hostnames:
            r = resolve_hostname(hostname)
            if r.error:
                print(f"  ✗ {hostname}: {r.error}")
            else:
                print(f"  ✓ {hostname} → {r.resolved_ips} (TTL={r.ttl}s, "
                      f"{r.resolution_ms:.1f}ms)")
                if r.is_cname:
                    print(f"    CNAME → {r.cname_target}")

            # Analyze search domain impact
            analysis = analyze_search_domain_impact(hostname)
            if analysis.will_try_search_domains and analysis.extra_queries_before_success > 0:
                print(f"  ℹ️  {analysis.recommendation[:100]}...")

    # Kubernetes service discovery
    if k8s_services:
        print(f"\n[3] KUBERNETES SERVICE DISCOVERY")
        checks = check_data_platform_service_discovery(k8s_services)
        for c in checks:
            if c.error:
                print(f"  ✗ {c.fqdn}: {c.error}")
            elif c.is_headless:
                print(f"  ✓ {c.fqdn} (headless) → {len(c.pod_ips)} pod IPs: {c.pod_ips}")
            else:
                print(f"  ✓ {c.fqdn} → ClusterIP {c.cluster_ip}")

    # Istio mesh check
    if check_istio and istio_namespace:
        print(f"\n[4] ISTIO MESH STATUS (namespace: {istio_namespace})")
        proxies = check_istio_proxy_sync(istio_namespace)
        unsynced = [p for p in proxies if isinstance(p, dict) and not p.get('synced', True)]
        print(f"  Proxy sync: {len(proxies) - len(unsynced)}/{len(proxies)} synced")
        for p in unsynced:
            print(f"  ⚠️  {p.get('pod', 'unknown')}: {p.get('warning', '')[:80]}")

        mtls = check_mtls_mode(istio_namespace)
        icon = "✓" if mtls.get('mtls_mode') == 'STRICT' else "⚠️ "
        print(f"  {icon} mTLS mode: {mtls['mtls_mode']}")
        if mtls.get('warning'):
            print(f"  ⚠️  {mtls['warning'][:100]}")

    # DNS benchmark
    if benchmark and kafka_hostnames:
        print(f"\n[5] DNS RESOLUTION BENCHMARK")
        results = benchmark_dns_resolution(kafka_hostnames, iterations=5)
        for hostname, stats in results.items():
            if 'avg_ms' in stats:
                print(f"  {hostname}: avg={stats['avg_ms']}ms, "
                      f"p95={stats['p95_ms']}ms — {stats['diagnosis']}")
            else:
                print(f"  {hostname}: {stats.get('error', 'failed')}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DNS and service mesh diagnostic toolkit")
    parser.add_argument('--rds', metavar='ENDPOINT', help='RDS endpoint to check')
    parser.add_argument('--kafka', nargs='+', metavar='HOST', help='Kafka hosts to resolve')
    parser.add_argument('--resolve', metavar='HOSTNAME', help='Resolve a single hostname')
    parser.add_argument('--search-analysis', metavar='HOSTNAME',
                        help='Analyze search domain impact for a hostname')
    parser.add_argument('--istio', action='store_true', help='Check Istio proxy status')
    parser.add_argument('--namespace', metavar='NS', help='Kubernetes namespace')
    parser.add_argument('--benchmark', action='store_true',
                        help='Benchmark DNS resolution times')
    args = parser.parse_args()

    if args.resolve:
        r = resolve_hostname(args.resolve)
        if r.error:
            print(f"Resolution failed: {r.error}")
        else:
            print(f"{args.resolve} → {r.resolved_ips}")
            print(f"  TTL: {r.ttl}s, time: {r.resolution_ms:.1f}ms, CNAME: {r.is_cname}")
            if r.cname_target:
                print(f"  CNAME target: {r.cname_target}")

    elif args.search_analysis:
        a = analyze_search_domain_impact(args.search_analysis)
        print(a.recommendation)
        if a.will_try_search_domains:
            print(f"  Search domains tried ({a.extra_queries_before_success}):")
            for d in a.search_domains_tried:
                print(f"    {d}")
    else:
        dns_mesh_report(
            rds_endpoint=args.rds,
            kafka_hostnames=args.kafka,
            check_istio=args.istio,
            istio_namespace=args.namespace,
            benchmark=args.benchmark,
        )
```

---

## 6. Visual Reference

### DNS Resolution Path in Kubernetes

```
Pod (10.0.1.50)
  │
  │ DNS query: "kafka.kafka-prod.svc.cluster.local"
  ▼
CoreDNS (10.96.0.10) ← Pod's /etc/resolv.conf nameserver
  │
  ├── Is *.cluster.local? YES
  │     → Kubernetes API watch (Services/Endpoints)
  │     → Returns ClusterIP: 10.96.45.100
  │
  └── Is external hostname? (e.g., secretsmanager.us-east-1.amazonaws.com)
        → Forward to 169.254.169.254 (VPC Resolver)
        → Private hosted zone? YES → returns 10.0.x.x (interface endpoint ENI)
        → No private zone? → Public Route 53 → returns public IP

                                          ┌───────────────────────────────┐
Short name resolution (ndots=5):          │  Search domain expansion:     │
  "kafka" (0 dots < 5)                   │  kafka.kafka-prod.svc.cluster │
  → tries search domains first:          │  .local → FOUND (step 1)      │
     kafka.kafka-prod.svc.cluster.local  │                               │
     kafka.svc.cluster.local             │  secretsmanager.us-east-1     │
     kafka.cluster.local                 │  .amazonaws.com (4 dots < 5)  │
     kafka.us-east-1.compute.internal    │  → tries 4 search domains     │
     kafka (absolute)                    │  → then tries absolute FQDN   │
                                         └───────────────────────────────┘
```

### Kubernetes Service Types and DNS

```
ClusterIP Service (default):            Headless Service (clusterIP: None):
  kafka.kafka-prod.svc.cluster.local      kafka-headless.kafka-prod.svc.cluster.local
  → 10.96.45.100 (virtual ClusterIP)      → 10.0.1.155 (pod 1 direct IP)
  → kube-proxy DNAT to one pod            → 10.0.1.156 (pod 2 direct IP)
                                          → 10.0.1.157 (pod 3 direct IP)
                                          
  Used for: HTTP services,                Used for: Kafka, ZooKeeper, 
  Schema Registry, Airflow UI             Cassandra (stateful, per-pod connections)
  
StatefulSet pods (headless required):
  kafka-0.kafka-headless.kafka-prod.svc.cluster.local → 10.0.1.155 (stable!)
  kafka-1.kafka-headless.kafka-prod.svc.cluster.local → 10.0.1.156
  kafka-2.kafka-headless.kafka-prod.svc.cluster.local → 10.0.1.157
  
  Even after pod restart: kafka-0 always maps to the same stable ordinal hostname.
  The IP may change, but kafka-0's DNS name is stable → Kafka advertised.listeners safe.
```

### Istio Sidecar Traffic Interception

```
Pod network namespace:
  Application container          Envoy sidecar (istio-proxy)
  [Kafka Producer]               [Envoy]
        │                             │
        │ writes to :9092             │
        │   ──────────────────────────►│ iptables redirects
        │                              │ outbound to Envoy (port 15001)
        │                              │
        │                              │ mTLS handshake with
        │                              │ destination Envoy sidecar
        │                              │
        │                              ▼
        │                     [Envoy at destination pod]
        │                              │
        │                              │ decrypts mTLS, checks
        │                              │ AuthorizationPolicy
        │                              │
        │                              ▼
        │                     [Kafka Broker]
        │
        ◄─────────────────────────────── response (mTLS encrypted)
        
Application code: plain TCP to kafka:9092
Network wire: mTLS-encrypted, authenticated
Zero code changes required.
```

---

## 7. Common Mistakes

**Mistake 1: Using a headless service for a stateless service, or a ClusterIP service for a stateful service.** ClusterIP services load-balance across all healthy pods — correct for Schema Registry (any instance can answer) but wrong for Kafka brokers (each broker is a distinct partition leader). Kafka in Kubernetes must use a headless service with a StatefulSet so clients can address `kafka-0`, `kafka-1`, `kafka-2` individually, as required by Kafka's partition metadata protocol. Using a ClusterIP service for Kafka causes clients to randomly land on any broker regardless of partition leadership, which either fails or requires the broker to proxy the request (causing latency).

**Mistake 2: Hardcoding IP addresses in Kafka `bootstrap.servers`.** If a Kafka broker's pod IP changes (pod restart, node replacement), the hardcoded IP is stale and connections fail. Always use DNS names: `kafka-0.kafka-headless.kafka-prod.svc.cluster.local:9092,kafka-1.kafka-headless...`. The pod IP changes; the ordinal hostname is stable.

**Mistake 3: Not configuring `advertised.listeners` with the Kubernetes DNS name for Kafka.** Covered in M31, but it recurs here in the DNS context: `advertised.listeners` is what Kafka returns to clients after the initial bootstrap connection. If it contains the pod IP rather than the stable DNS name, clients that receive partition metadata will use the IP, which becomes stale after the next pod restart. Set: `KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka-0.kafka-headless.kafka-prod.svc.cluster.local:9092`.

**Mistake 4: Long DNS TTLs on Route 53 records pointing to EC2 instances that can be replaced.** If a Route 53 A record for `kafka-bootstrap.prod.internal` has TTL=3600 (1 hour) and the Kafka EC2 instance is replaced (different IP), clients using the old IP continue connecting to a non-existent host for up to an hour. For any record pointing to replaceable infrastructure, use TTLs of 30–60 seconds.

**Mistake 5: Enabling Istio's STRICT mTLS without first updating all clients to support mTLS.** Setting `PeerAuthentication mode: STRICT` means all traffic to pods in that namespace must use mTLS. Any client outside the mesh (an EC2-based service, an external monitoring tool, or a legacy pod without a sidecar) that tries to connect with plain HTTP gets `Connection reset by peer`. The correct migration path: start with PERMISSIVE (both allowed), ensure all clients are mesh-enrolled or have explicit exceptions, then switch to STRICT.

---

## 8. Production Failure Scenarios

### Scenario 1: Kafka Consumers Lose Connection After Rolling Broker Restart

**Symptoms:** After a rolling restart of Kafka brokers (for a config change), 30% of Kafka consumers report `Connection to node 1 (kafka-1.kafka-headless.kafka-prod.svc.cluster.local/10.0.1.156) could not be established`. The remaining 70% are working. Consumers for partitions assigned to broker 1 are unable to reconnect.

**Root cause:** The Kafka consumer's Java client cached the broker's IP address from before the pod restart. After `kafka-1` pod restarted, it got a new pod IP (`10.0.1.158`), but the DNS entry (`kafka-1.kafka-headless.kafka-prod.svc.cluster.local`) TTL had not yet expired (CoreDNS caches for 30s by default). The consumer attempted to reconnect to `10.0.1.156` (the old IP, now assigned to a different pod or unassigned). After the TTL expired, new DNS queries returned `10.0.1.158` and connections recovered.

The deeper root cause: the consumer re-resolved the hostname on connection failure (correct behavior), but there was a 30-second window during the TTL expiry. During high-throughput bursts, even 30 seconds of partition connectivity loss can cause lag accumulation.

**Fix:** Lower CoreDNS cache TTL for the `kafka-prod` namespace to 5–10 seconds, or configure the Kafka client to re-resolve more aggressively:
```bash
# Lower CoreDNS cache TTL in CoreDNS ConfigMap
kubectl edit configmap coredns -n kube-system
# Change: cache 30
# To:     cache 5
```

### Scenario 2: Airflow Cannot Reconnect to RDS After Failover

**Symptoms:** At 3:47 AM, RDS Multi-AZ performs an automatic failover (detected by CloudWatch alarm on `DatabaseConnections` dropping to zero). The failover completes in 45 seconds (standard for Multi-AZ). However, Airflow workers continue failing to connect to Postgres with `FATAL: connection timeout` for the next 8 minutes.

**Root cause:** Airflow's SQLAlchemy connection pool holds pre-established connections to the old Postgres primary IP. The `pool_recycle` parameter is set to 3600 (1 hour), meaning connections are recycled every hour — not after each failure. When the RDS primary fails over, the old connections are broken. SQLAlchemy's `pool_pre_ping` is not enabled, so there is no health check on connection checkout. The workers attempt to use broken connections from the pool, get errors, and the pool only replaces them on the next `pool_recycle` cycle.

Additionally, the DNS CNAME (`postgres.data.prod.internal → db-instance-id.abc123.us-east-1.rds.amazonaws.com`) had a custom TTL of 300 seconds set by the infrastructure team — five times the AWS RDS default of 5 seconds. Even after the RDS failover updated the underlying hostname, clients using this Route 53 record cached the old name for up to 5 minutes.

**Fix:**
1. Enable `pool_pre_ping=True` in SQLAlchemy — sends `SELECT 1` before each connection checkout to detect broken connections
2. Lower the Route 53 record TTL to 30 seconds (matches RDS failover speed)
3. Configure `pool_recycle` to 300 seconds (shorter than typical failover + reconnect window)

### Scenario 3: Istio STRICT mTLS Blocks Prometheus Scraping

**Symptoms:** After migrating the `kafka-prod` namespace to Istio STRICT mTLS, Prometheus metrics collection for Kafka brokers stops. Prometheus shows all `kafka-prod` targets as `Connection refused`. Kafka is otherwise healthy; consumers and producers are unaffected.

**Root cause:** Prometheus runs in the `monitoring` namespace and is not enrolled in the Istio mesh (no sidecar injected). When STRICT mTLS is enabled for `kafka-prod`, all inbound connections to pods in `kafka-prod` must use mTLS. Prometheus' plain HTTP scraping (connecting to the Kafka exporter on port 9308) is rejected by the Envoy sidecar because it is not mTLS.

**Fix option 1 (recommended):** Add a `PeerAuthentication` exception for the metrics port:
```yaml
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: kafka-metrics-permissive
  namespace: kafka-prod
spec:
  selector:
    matchLabels:
      app: kafka
  mtls:
    mode: STRICT
  portLevelMtls:
    9308:              # JMX exporter metrics port
      mode: PERMISSIVE # Allow plain HTTP on this port for Prometheus
```

**Fix option 2:** Enroll Prometheus in the Istio mesh by enabling sidecar injection in the monitoring namespace. Prometheus becomes a mesh member and uses mTLS automatically.

### Scenario 4: Cross-Namespace Spark Calls Fail After CoreDNS ConfigMap Change

**Symptoms:** Spark jobs in the `data-transformations` namespace suddenly fail to connect to the Schema Registry in `kafka-prod`. Error: `UnknownHostException: schema-registry.kafka-prod`. The Schema Registry is running and accessible from within `kafka-prod`. The failure started at 14:32 — the time a platform engineer applied a CoreDNS ConfigMap update.

**Root cause:** The CoreDNS ConfigMap update inadvertently introduced a syntax error in the `Corefile`. CoreDNS's hot-reload feature detected the change and attempted to reload, but due to the syntax error, CoreDNS fell back to an empty configuration. The empty configuration has no `kubernetes` plugin directive — so queries for `*.cluster.local` names are forwarded to the VPC resolver (which has no knowledge of Kubernetes services) and return NXDOMAIN.

**Diagnosis:**
```bash
kubectl logs -n kube-system -l k8s-app=kube-dns --tail=20
# plugin/reload: error reloading config: ...Corefile:12 - Error during parsing: Unknown directive 'kbernetes'
# (typo: 'kbernetes' instead of 'kubernetes')
```

**Fix:** Correct the typo in the ConfigMap and reapply:
```bash
kubectl edit configmap coredns -n kube-system
# Fix: kbernetes → kubernetes
# CoreDNS automatically reloads on ConfigMap change (within 60s)
```

---

## 9. Performance and Tuning

### CoreDNS Scaling

Default EKS clusters deploy 2 CoreDNS replicas. For large clusters (100+ nodes, 500+ pods), CoreDNS can become a bottleneck if:
- Cache hit rate drops below ~80% (pods churn rapidly, generating high unique query volume)
- Each pod makes many unique external DNS queries (e.g., `ndots:5` causing 4 search-domain queries per external hostname)

**Scaling CoreDNS:**
```bash
# Scale CoreDNS horizontally
kubectl scale deployment coredns -n kube-system --replicas=4

# Or configure HPA for CoreDNS (EKS supports this via CoreDNS HPA)
# aws eks update-addon --cluster-name data-platform --addon-name coredns \
#   --configuration-values '{"replicaCount": 4}'

# Monitor CoreDNS cache hit rate
kubectl port-forward -n kube-system svc/kube-dns 9153:9153
curl -s http://localhost:9153/metrics | \
  awk '/coredns_cache_hits_total/{hits+=$2} /coredns_cache_misses_total/{misses+=$2}
       END{print "Cache hit ratio:", hits/(hits+misses)*100 "%"}'
```

### DNS Latency Optimization

CoreDNS `cache 30` caches responses for 30 seconds. Increasing to 300 seconds reduces upstream queries significantly but means stale records persist longer after service updates.

For data engineering workloads with stable service topology (services don't change frequently), `cache 120` is a reasonable balance.

The `ndots` problem: lowering `ndots` from 5 to 2 in the kubelet configuration reduces unnecessary search domain queries:

```yaml
# In pod spec or in kubelet configuration
dnsConfig:
  options:
  - name: ndots
    value: "2"
```

With `ndots:2`, only hostnames with fewer than 2 dots trigger search domain expansion. `kafka` (0 dots) → still tries search domains. `kafka.kafka-prod` (1 dot) → still tries. `kafka.kafka-prod.svc` (2 dots) → resolved directly. For fully-qualified names, this eliminates the search-domain overhead entirely.

### Service Mesh Performance

Istio sidecar proxies add ~1–3ms of latency per hop (one sidecar adds 0.5–1ms; a full hop through two sidecars — source and destination — adds 1–3ms). For long-running batch jobs (Spark reading terabytes from S3), this overhead is negligible. For high-frequency microservice calls with P99 latency requirements under 10ms, profile with and without the mesh.

Istio's `DestinationRule` `connectionPool.tcp.maxConnections` limits how many parallel TCP connections the Envoy proxy will make to a destination. The default is 1024 — usually sufficient, but for Spark executors that make hundreds of simultaneous connections to Kafka, verify the connection pool is not the bottleneck.

---

## 10. Interview Q&A

**Q1: Explain how Kubernetes DNS resolution works for a pod trying to connect to a service in a different namespace. What are the search domain implications?**

When a pod makes a DNS query, the query goes to CoreDNS — the cluster's internal DNS server, whose IP is configured as the nameserver in every pod's `/etc/resolv.conf`. CoreDNS handles all `*.cluster.local` queries by watching the Kubernetes API for Service and Endpoint objects. When a Service is created, CoreDNS automatically has its record; when it is deleted, the record is gone.

For cross-namespace resolution, the full FQDN is `<service>.<namespace>.svc.cluster.local`. From within the same namespace, the short name `<service>` works because the search domain `<current-namespace>.svc.cluster.local` is the first entry in the pod's search domain list — it gets appended to the short name automatically.

The `ndots:5` setting creates a subtle efficiency problem for external hostnames. If a hostname has fewer than 5 dots, the resolver tries appending each search domain before attempting the hostname as an absolute FQDN. `secretsmanager.us-east-1.amazonaws.com` has 4 dots — one fewer than the threshold. The resolver first tries `secretsmanager.us-east-1.amazonaws.com.kafka-prod.svc.cluster.local` (NXDOMAIN), then two more search domain variants (NXDOMAIN each), before finally trying the hostname as an absolute FQDN and getting the correct answer. This generates four DNS queries where one would suffice, adding latency to every first connection and increasing CoreDNS load.

The mitigation is straightforward: for external hostnames used frequently, append a trailing dot to force absolute resolution, or lower `ndots` to 2 in the pod's DNS configuration. Lowering `ndots` requires verifying that all internal services are referenced with at least two-component names (`kafka.kafka-prod` rather than just `kafka`).

**Q2: What is a Kubernetes headless service and why is it required for Kafka running as a StatefulSet?**

A headless service is a Service resource with `clusterIP: None`. Unlike a standard ClusterIP service (which assigns a virtual IP and load-balances across all pods), a headless service has no virtual IP. DNS queries for a headless service return the individual IP addresses of all pods matching the selector — A records, one per pod. Clients receive all pod IPs and connect directly.

Kafka requires headless services because Kafka's distributed protocol is inherently stateful and per-broker. Each Kafka partition has exactly one leader broker. When a producer or consumer connects to Kafka, the initial bootstrap connection returns partition metadata that includes the address of each partition's leader broker. The client then connects directly to that specific broker to produce or consume from that partition. A load balancer (ClusterIP service) that distributes connections randomly across brokers would break this model — the client would connect to broker 2 expecting broker 1 (the partition leader) and either get an error or cause the broker to proxy the request.

The StatefulSet + headless service combination gives each pod a stable, predictable DNS name: `kafka-0.kafka-headless.kafka-prod.svc.cluster.local`, `kafka-1.kafka-headless...`, and so on. The pod's IP address changes when the pod is rescheduled; the ordinal DNS name is stable and permanent for the lifetime of the StatefulSet. This is critical for Kafka's `advertised.listeners` configuration: the address advertised to clients in partition metadata must be a stable DNS name, not a pod IP. Using the ordinal hostname in `advertised.listeners` guarantees that clients' cached partition metadata remains valid even after pod restarts and pod IP changes.

**Q3: Explain what happens at the network level when Istio STRICT mTLS is enabled in a namespace, and describe the most common operational breakage it causes.**

When Istio `PeerAuthentication mode: STRICT` is applied to a namespace, every inbound connection to pods in that namespace must complete a mutual TLS handshake before any application data is exchanged. The Envoy sidecar injected into each pod intercepts all inbound traffic at the iptables level. When a connection arrives, Envoy checks: is this a TLS ClientHello (starting a TLS handshake)? If yes, proceed with mTLS. If no (plain TCP or plain HTTP), Envoy rejects the connection with a TCP RST — the application never sees the traffic.

The most common operational breakage is external services or tools that are not Istio mesh members trying to connect to pods in the STRICT namespace. Prometheus is the canonical example: Prometheus runs in a monitoring namespace that may not have Istio sidecar injection enabled. When Prometheus scrapes a metrics endpoint (plain HTTP on port 9308 for Kafka exporter, or port 9090 for application metrics) inside a STRICT mTLS namespace, the connection is rejected by the destination's Envoy sidecar. Prometheus shows all those targets as `Connection refused`.

The fix is a `portLevelMtls` exception in the `PeerAuthentication` resource: specific ports (like 9308 for metrics) can be configured as PERMISSIVE while the rest of the namespace remains STRICT. This allows Prometheus's plain HTTP scraping to succeed on the metrics port while maintaining mTLS enforcement for all other application traffic. The cleaner long-term solution is enrolling Prometheus in the mesh, but the port-level exception is the practical short-term fix most teams use.

---

## 11. Cross-Question Chain

**Interviewer:** You're deploying a three-broker Kafka cluster on Kubernetes. Walk me through the DNS configuration needed for both internal cluster consumers and external consumers (producers running on EC2 outside the cluster).

**Candidate:** Two separate path requirements. For internal consumers (pods within the cluster), I deploy Kafka as a StatefulSet with a headless Service. The headless service gives each broker a stable ordinal DNS name: `kafka-0.kafka-headless.kafka-prod.svc.cluster.local`, `kafka-1...`, `kafka-2...`. The Kafka `advertised.listeners` for each broker is set to its ordinal hostname on the internal listener port 9092. The `bootstrap.servers` in consumer configs uses all three: `kafka-0.kafka-headless.kafka-prod.svc.cluster.local:9092,...`. After bootstrap, consumers receive partition metadata and connect to specific brokers by their ordinal names — which resolve to current pod IPs via CoreDNS.

For external consumers (EC2 outside the cluster), those ordinal names (`*.svc.cluster.local`) are not resolvable outside the cluster — they're cluster-internal DNS. I have two options. First, I create a LoadBalancer service per broker (or use a NodePort service with NLB for the whole StatefulSet) and configure the external `advertised.listeners` to use the NLB's DNS name or an Elastic IP. The external listener is separate from the internal listener. Second, if using AWS MSK instead of self-hosted Kafka, MSK provides broker-specific endpoints already.

**Interviewer:** A new Spark pod starts in the `data-transformations` namespace and tries to connect to `kafka-0.kafka-headless.kafka-prod.svc.cluster.local`. Walk me through every DNS hop until the IP is resolved.

**Candidate:** The Spark pod's `/etc/resolv.conf` has `nameserver 10.96.0.10` (CoreDNS's ClusterIP) and `options ndots:5`. The hostname `kafka-0.kafka-headless.kafka-prod.svc.cluster.local` has 6 dots — more than 5, so `ndots:5` applies (more dots than the threshold means it's tried as an absolute FQDN first, no search domain expansion).

The query goes to CoreDNS at `10.96.0.10`. CoreDNS sees the query is for `*.cluster.local` — matches the `kubernetes cluster.local` plugin block. CoreDNS queries the Kubernetes API (which it watches via a shared informer). It looks up the Service `kafka-headless` in namespace `kafka-prod` — this is a headless service. For headless services, CoreDNS returns A records for each pod matching the selector. But for a StatefulSet pod-specific query (`kafka-0.kafka-headless...`), CoreDNS returns the specific pod's IP — say `10.0.1.155`.

The Spark pod receives `10.0.1.155` and initiates a TCP connection to port 9092. If Istio is present and STRICT mTLS is enabled in `kafka-prod`, the Envoy sidecar in the Spark pod intercepts the outbound connection, performs the mTLS handshake with the Envoy sidecar in `kafka-0`, and the application-level Kafka protocol flows over the mTLS tunnel transparently.

**Interviewer:** That Spark pod now cannot resolve `kafka-0.kafka-headless.kafka-prod.svc.cluster.local`. What's your debugging sequence?

**Candidate:** Three layers to check in order. First: is CoreDNS running and healthy? `kubectl get pods -n kube-system -l k8s-app=kube-dns` — confirm two replicas Running. Check logs: `kubectl logs -n kube-system -l k8s-app=kube-dns --tail=20` for errors. If CoreDNS is crashing or shows config parse errors, that's the root cause.

Second: does the headless Service and StatefulSet exist and are the pods ready? `kubectl get svc kafka-headless -n kafka-prod` — confirm `ClusterIP: None`. `kubectl get pods -n kafka-prod -l app=kafka` — confirm `kafka-0` is Running and Ready. If `kafka-0` is not Ready, CoreDNS has no endpoint to return for it.

Third: test DNS resolution directly from the failing pod: `kubectl exec -n data-transformations spark-pod -- nslookup kafka-0.kafka-headless.kafka-prod.svc.cluster.local`. If this returns NXDOMAIN, the issue is CoreDNS or the Service/Pod status. If it times out, CoreDNS is unreachable (check the CoreDNS Service and if port 53 is open). If it returns an IP but the Spark connection still fails, the issue is network/security — security group (if EC2-backed nodes), NetworkPolicy, or Istio AuthorizationPolicy.

**Interviewer:** CoreDNS is healthy, the service and pods exist, and nslookup resolves correctly. Spark still gets connection refused. What's next?

**Candidate:** With DNS working, the problem is at the TCP layer or the application layer. `Connection refused` means the packet reached the pod but nothing accepted it — so it's not a security group or NetworkPolicy issue (those produce timeouts, not refusals). The Kafka broker process might not be listening on 9092, or it's listening on the wrong interface.

I'd check: `kubectl exec -n kafka-prod kafka-0 -- ss -tnlp | grep 9092` — is Kafka listening? If not, Kafka didn't start or crashed. Check `kubectl logs kafka-0 -n kafka-prod` for startup errors.

If Kafka is listening, verify the Istio sidecar isn't the refusal point: `kubectl exec -n kafka-prod kafka-0 -c istio-proxy -- curl -v http://localhost:9092` — if Envoy is running, check `istioctl proxy-config listeners kafka-0.kafka-prod | grep 9092`. Then check Istio `AuthorizationPolicy` in `kafka-prod` — if there's a policy that selects Kafka pods and doesn't list `data-transformations/spark` as an allowed source, Envoy returns a 403 or TCP RST that looks like connection refused to the client.

**Interviewer:** You find no Istio policies blocking it, and Kafka is listening. But the connection is still refused. What else?

**Candidate:** At this point I've ruled out DNS, security groups (not applicable — would be timeout), NetworkPolicy (would be timeout), Kafka process not running, and Istio policy. The remaining candidates are: wrong port (Kafka might be configured with `listeners=INTERNAL://0.0.0.0:9093` rather than `9092`), a host-level firewall (iptables rules on the node that predate Istio injection), or the container's readiness probe is passing but the Kafka process is in an unhealthy state.

Check which port Kafka is actually listening on: `kubectl exec -n kafka-prod kafka-0 -- ss -tnlp` — shows all listening sockets. If it's `9093` and the service is configured for `9092`, the service port mapping is wrong. Fix: `kubectl edit svc kafka-headless -n kafka-prod` and update `targetPort` to match the actual container port.

If the port is correct and the process is listening, I'd try connecting from inside the kafka-prod namespace rather than data-transformations — `kubectl run -it --rm test -n kafka-prod --image=busybox -- nc -z kafka-0.kafka-headless.kafka-prod.svc.cluster.local 9092`. If that works but cross-namespace doesn't, it's a NetworkPolicy issue that I missed (the policy selects kafka pods but I didn't check the egress side in `data-transformations` — there might be an egress deny all in `data-transformations` without a rule for kafka-prod).

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the Kubernetes DNS name format for a service? | `<service>.<namespace>.svc.<cluster-domain>`. For default cluster.local domain: `kafka.kafka-prod.svc.cluster.local`. Short names (`kafka`) work within the same namespace due to search domain appending. |
| 2 | What is a Kubernetes headless service and when do data engineers need it? | `clusterIP: None` — no virtual IP assigned. DNS returns individual pod IPs directly. Required for stateful services like Kafka where clients must connect to specific instances (partition leaders). |
| 3 | Why does Kafka in Kubernetes require a headless service rather than a ClusterIP service? | Kafka clients need to connect to specific brokers (partition leaders). A ClusterIP load-balances randomly across all pods. Kafka's partition metadata protocol requires addressable individual brokers. |
| 4 | What is the DNS name for `kafka-0` pod in a StatefulSet with headless service `kafka-headless` in namespace `kafka-prod`? | `kafka-0.kafka-headless.kafka-prod.svc.cluster.local`. StatefulSet pods get ordinal-based stable DNS names via their headless service. This name is stable even when the pod IP changes. |
| 5 | What is `ndots:5` and what problem does it cause for external hostname resolution? | `ndots:5` means hostnames with < 5 dots trigger search domain appending before absolute lookup. `secretsmanager.us-east-1.amazonaws.com` (4 dots) generates 4 extra NXDOMAIN queries before the correct answer, adding latency and CoreDNS load. |
| 6 | How do you force absolute DNS resolution to avoid search domain overhead? | Append a trailing dot: `secretsmanager.us-east-1.amazonaws.com.`. Or lower `ndots` to 2 in pod dnsConfig. The trailing dot signals to the resolver that the name is already fully qualified — no search domain expansion. |
| 7 | What is the difference between Route 53 private hosted zones and public hosted zones? | Private hosted zones: only resolvable from within associated VPCs. Used for internal service names. Public hosted zones: resolvable from the internet. Data platforms use private zones for all internal service DNS. |
| 8 | What is the CoreDNS `cache` directive and what trade-off does tuning it involve? | `cache N` caches DNS responses for N seconds, reducing upstream queries. Higher value = less CoreDNS load but stale records persist longer after service updates. Lower value = fresher records but more upstream DNS queries. Default is 30s. |
| 9 | What DNS record type does AWS RDS Multi-AZ use for its failover endpoint, and what TTL does AWS use? | CNAME, with TTL=5 seconds. After failover, the CNAME is updated to point to the new primary. Low TTL ensures clients pick up the new primary within seconds. |
| 10 | An application uses a hardcoded IP for the RDS database. Why does this break after a Multi-AZ failover? | RDS Multi-AZ failover promotes the standby to primary with a different IP. Hardcoded IPs always point to the old primary (now a standby). Applications must use the RDS-provided DNS CNAME endpoint, which is updated automatically on failover. |
| 11 | What is CoreDNS and where does it run in a Kubernetes cluster? | CoreDNS is the cluster's internal DNS server, running as a Deployment in `kube-system` namespace. Its ClusterIP is configured as the nameserver in every pod's `/etc/resolv.conf`. It resolves `*.cluster.local` names and forwards external names to the VPC resolver. |
| 12 | What Istio resource applies mTLS policy to a namespace? | `PeerAuthentication`. Mode `STRICT` requires all inbound traffic to use mTLS. Mode `PERMISSIVE` accepts both mTLS and plain HTTP. Mode `DISABLE` turns off mTLS. |
| 13 | Why does enabling Istio STRICT mTLS break Prometheus scraping? | Prometheus is typically not a mesh member (no Envoy sidecar). It scrapes metrics over plain HTTP. STRICT mTLS requires all connections to present client certificates via mTLS handshake. Plain HTTP connections are rejected by the destination's Envoy sidecar. |
| 14 | What is an Istio `VirtualService` used for? | Defines traffic routing rules for a service: timeouts, retries, traffic splits, fault injection. Applied at the Envoy sidecar level transparently to applications. |
| 15 | What is Istio `DestinationRule` used for? | Configures connection pool sizes, circuit breaker (outlier detection) settings, and load balancing policy for a destination service. |
| 16 | What does `istioctl proxy-status` show? | Sync status of each Envoy proxy in the mesh — whether CDS/LDS/EDS/RDS (the four xDS APIs) are SYNCED or have diverged from Istiod. Unsynced proxies use stale configuration. |
| 17 | What is the xDS protocol in the context of Istio? | The API used by Istiod (control plane) to distribute configuration to Envoy proxies (data plane). CDS=Cluster Discovery, LDS=Listener Discovery, EDS=Endpoint Discovery, RDS=Route Discovery. Envoy proxies poll Istiod for config updates. |
| 18 | What is mTLS (mutual TLS) and how does Istio implement it transparently? | Both client and server present X.509 certificates. Istio injects Envoy sidecars into pods; Istiod issues SPIFFE certificates to each sidecar. iptables redirect all pod traffic through the local Envoy, which performs mTLS handshakes. Applications use plain TCP/HTTP — encryption is handled by the mesh layer. |
| 19 | What Kubernetes resource makes `ExternalName` service resolution work for an RDS endpoint? | A Service with `type: ExternalName` and `externalName: db-prod.abc123.us-east-1.rds.amazonaws.com`. CoreDNS returns a CNAME pointing to the RDS hostname. Pods connect to the Kubernetes service name; DNS resolution follows the CNAME chain to the RDS IP. |
| 20 | Name the four conditions required for a pod in namespace A to resolve a Kubernetes service in namespace B. | 1) The Service exists in namespace B, 2) A healthy pod matching the Service selector exists, 3) The pod uses the full FQDN `svc.namespace-b.svc.cluster.local` or CoreDNS adds the correct search domain, 4) CoreDNS is running and healthy (`kubectl get pods -n kube-system -l k8s-app=kube-dns`). |

---

## 13. Further Reading

- **CoreDNS documentation — "Kubernetes plugin":** The authoritative reference for how CoreDNS handles Kubernetes Service and Endpoint discovery, including headless services, ExternalName, and stub zones. Essential for understanding CoreDNS Corefile customization.
- **Kubernetes documentation — "DNS for Services and Pods":** The official Kubernetes guide covering the FQDN naming convention, search domain behavior, ndots configuration, and headless service DNS semantics. The "How Pods and Services are discovered" section is essential reading.
- **Istio documentation — "Traffic Management" and "Security":** The canonical references for VirtualService, DestinationRule, PeerAuthentication, and AuthorizationPolicy. The "Understanding Istio Configuration" guide explains the relationship between resources and Envoy xDS configuration.
- **"The Service Mesh" by William Morgan (Buoyant):** A practitioner's introduction to service mesh concepts, mTLS, and observability. Explains the sidecar model and traffic interception without assuming prior Envoy knowledge.
- **"Kafka on Kubernetes: The Practical Guide" — Confluent blog:** Covers headless services, StatefulSet configuration, advertised.listeners setup, and external access patterns for Kafka in Kubernetes. Addresses the specific DNS requirements that differ from stateless services.
- **AWS documentation — "Resolving DNS queries for VPCs using Route 53 Resolver":** Covers Route 53 Resolver endpoints for hybrid DNS (on-premises ↔ VPC), private hosted zone association, and the DNS resolution order that determines how pod DNS queries traverse from CoreDNS to the VPC resolver to Route 53.

---

## 14. Lab Exercises

**Exercise 1: Kubernetes DNS Exploration**

In a running Kubernetes cluster (minikube or EKS), create a ClusterIP service and a headless service for the same set of pods. From a pod in the same namespace, run `nslookup` on both service names and observe the difference in DNS responses (single IP vs multiple IPs). Then cross-namespace: create a pod in a different namespace and test both short names and FQDNs. Document which names work from each namespace and why.

**Exercise 2: StatefulSet Stable DNS Verification**

Deploy a simple StatefulSet (nginx or any image) with a headless service. Delete one of the pods and wait for it to be recreated. Observe that the pod comes back with a different IP but the same ordinal DNS name (e.g., `web-0.nginx-headless...`). Verify with `nslookup` that the DNS name resolves to the new pod IP. This demonstrates why Kafka `advertised.listeners` should use ordinal hostnames, not IPs.

**Exercise 3: CoreDNS Search Domain Debugging**

From inside a pod, run `cat /etc/resolv.conf` to see the search domain list and ndots setting. Pick an external hostname with 4 dots (like `secretsmanager.us-east-1.amazonaws.com`). Use `dig +search` to trace all DNS queries generated by the resolver when searching for this hostname. Count the NXDOMAIN responses before the correct answer. Then add a trailing dot and compare: `dig +search secretsmanager.us-east-1.amazonaws.com.` — should produce only one query.

**Exercise 4: RDS DNS Failover Simulation**

If you have an RDS Multi-AZ instance, use `dig` to check the current CNAME target and TTL. Initiate a manual failover via the RDS console. Continuously run `dig` every 2 seconds and observe: when does the CNAME update? How long before clients using the DNS name pick up the new primary? Compare this with what would happen if the connection string used a hardcoded IP (never recovers automatically).

**Exercise 5: Istio mTLS Migration**

In a test namespace with Istio sidecar injection enabled, deploy two pods (a producer and a consumer service). Verify they communicate. Set `PeerAuthentication mode: PERMISSIVE` and verify communication works. Then switch to `STRICT` and verify inter-mesh communication still works. Then try connecting from a pod outside the mesh (no sidecar) — observe the connection failure. Add a `portLevelMtls: PERMISSIVE` exception for the metrics port only and verify that the external pod can reach the metrics endpoint while the rest of the service requires mTLS.

---

## 15. Key Takeaways

DNS is the service registry of distributed systems. Every distributed data platform component — Kafka, Postgres, Schema Registry, Airflow's metadata DB — must be referenced by DNS name rather than IP. IPs are ephemeral; DNS names are stable. When connections break after a pod restart, an RDS failover, or an instance replacement, the root cause is almost always an application that bypassed DNS by caching a resolved IP past its TTL, or by hardcoding an IP address in configuration.

The two DNS concepts that matter most for data engineering are: headless services (required for Kafka in Kubernetes so partition metadata contains individually-addressable broker DNS names) and low-TTL CNAME records (required for RDS failover so reconnecting clients immediately get the new primary, not the old one). A well-designed data platform has no hardcoded IP addresses anywhere in its configuration.

CoreDNS is the DNS server for all Kubernetes-internal service discovery. Its health directly affects the health of the entire platform — if CoreDNS is overloaded or mis-configured, every service-to-service call in the cluster degrades simultaneously. Monitor CoreDNS cache hit rate (should be above 80%), pod count (scale with cluster size), and ConfigMap changes (syntax errors trigger silent fallback to broken configuration).

Service meshes add traffic management and security at the proxy layer without application code changes. For data engineering, the two most relevant capabilities are mTLS (automatic encryption and mutual authentication for all inter-service traffic) and traffic policies (circuit breaking, retries, and timeouts configured via VirtualService and DestinationRule). The most common service mesh operational incident for data engineers is Istio STRICT mTLS breaking external tools (Prometheus, monitoring agents, legacy services) that are not mesh members — solved with port-level mTLS exceptions or by enrolling the external tools in the mesh.

---

## 16. Connections to Other Modules

- **M27 — TCP/IP Fundamentals:** DNS is the step before TCP. Every TCP connection is preceded by a DNS lookup (unless the IP is already in cache). Understanding that DNS returns IPs and that TTL controls cache lifetime is prerequisite to understanding RDS failover and pod IP change scenarios.
- **M31 — Load Balancing and Proxies:** `advertised.listeners` in Kafka is the intersection of DNS and load balancing. The address Kafka returns to clients in partition metadata must be a DNS name (not an IP), and it must be reachable by clients. Headless services with ordinal names solve this. The NLB pattern for external Kafka access also relies on DNS (the NLB's stable DNS name as the external listener address).
- **M32 — VPC Architecture:** Route 53 private hosted zones live within VPCs. The VPC's `enableDnsSupport` and `enableDnsHostnames` attributes control whether DNS resolution works inside the VPC and whether EC2 hostnames are registered. The DNS resolution path for interface endpoint private DNS (covered in M35) runs through the VPC resolver.
- **M34 — Security Groups and Network Policies:** DNS queries go to CoreDNS on UDP/TCP port 53. Kubernetes NetworkPolicies with default-deny egress must explicitly allow pod-to-kube-system traffic on port 53 or DNS breaks. Security groups on EC2 nodes must allow UDP 53 from pods to the VPC resolver at 169.254.169.254 for external DNS queries.
- **M35 — Private Endpoints and VPC Service Controls:** Interface endpoint private DNS works by overriding Route 53's public zone with a private hosted zone that maps service hostnames to endpoint ENI IPs. CoreDNS forwards unrecognized names to the VPC resolver, which serves the private hosted zone — completing the chain from pod → CoreDNS → VPC resolver → private zone → endpoint IP.

---

## 17. Anti-Patterns

**Anti-pattern: Using environment variables with hardcoded IPs for Kafka `bootstrap.servers`.** A common pattern in containerized applications is injecting service addresses via environment variables at pod startup. If those variables contain IPs (`10.0.1.155:9092`) rather than DNS names (`kafka-0.kafka-headless.kafka-prod.svc.cluster.local:9092`), they become stale the moment a broker pod restarts. Always use DNS names in configuration.

**Anti-pattern: Creating a Route 53 CNAME for RDS with TTL=300 as a "best practice."** Teams sometimes add a custom Route 53 CNAME for their RDS instance to provide a "friendly" hostname. They set a conservative TTL of 300 seconds without realizing this undermines RDS Multi-AZ failover — the original RDS endpoint has TTL=5 for precisely this reason. If you add a custom DNS alias for RDS, match the TTL to the RDS endpoint's own TTL (5–30 seconds).

**Anti-pattern: Modifying the CoreDNS ConfigMap without testing syntax first.** CoreDNS hot-reloads configuration from its ConfigMap. A syntax error in the Corefile causes CoreDNS to fall back to an empty configuration, breaking all in-cluster DNS resolution simultaneously. Before applying any CoreDNS ConfigMap change, test the Corefile syntax with `coredns -conf COREFILE -dryrun`.

**Anti-pattern: Enrolling all namespaces in Istio sidecar injection without a migration plan.** Enabling Istio sidecar injection in a namespace causes the sidecar to be injected into all new pods. Existing pods are unaffected until they restart. The result is a mixed namespace: some pods have sidecars, some don't. With PERMISSIVE mTLS, this works fine. With STRICT mTLS, injected pods reject connections from non-injected pods. Migrate namespaces one at a time, verify all pods in the namespace are injected before switching to STRICT.

**Anti-pattern: Using Istio VirtualService timeouts as a substitute for application-level connection pool configuration.** A VirtualService timeout of 30 seconds applies to individual requests. It does not recycle connections in the pool, retry on connection errors at the pool level, or handle the case where a connection was established before the request that times out. Application-level connection pool health checks (SQLAlchemy `pool_pre_ping`, Kafka client's reconnect logic) are still necessary alongside service mesh timeout policies.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `kubectl exec POD -- nslookup HOSTNAME` | DNS resolution from inside pod | Tests CoreDNS resolution for cluster-internal and external names |
| `kubectl exec POD -- cat /etc/resolv.conf` | View pod DNS config | Shows nameserver, search domains, ndots setting |
| `dig +search HOSTNAME` | Trace search domain expansion | Shows all DNS queries generated for a hostname with search domains |
| `dig +short CNAME HOSTNAME` | Check for CNAME records | Useful to verify RDS endpoints are CNAMEs, not A records |
| `kubectl get pods -n kube-system -l k8s-app=kube-dns` | Check CoreDNS health | First check when DNS is broken cluster-wide |
| `kubectl logs -n kube-system -l k8s-app=kube-dns` | CoreDNS error logs | Shows config parse errors and query failures |
| `kubectl edit configmap coredns -n kube-system` | Edit CoreDNS Corefile | Modify cache TTL, add stub zones, tune ndots behavior |
| `istioctl proxy-status` | Check Envoy sync state | Lists which proxies are SYNCED vs stale |
| `istioctl proxy-config listeners POD.NS` | Inspect Envoy listener config | Shows what ports Envoy is intercepting and routing |
| `istioctl analyze -n NAMESPACE` | Validate Istio resources | Detects misconfigured VirtualServices, missing DestinationRules |
| `aws route53 list-resource-record-sets` | List Route 53 records | `--hosted-zone-id ZONE_ID --query 'ResourceRecordSets'` |
| `aws route53 create-hosted-zone` | Create private hosted zone | `--hosted-zone-config PrivateZone=true` |
| `dns_mesh_diagnostics.py --resolve HOSTNAME` | Resolve + diagnose single hostname | Checks resolution time, CNAME, TTL |
| `dns_mesh_diagnostics.py --rds ENDPOINT` | RDS failover readiness check | Validates CNAME structure and TTL for RDS endpoints |

---

## 19. Glossary

**A Record:** A DNS record type that maps a hostname directly to an IPv4 address. Changes require updating the record and waiting for TTL expiry.

**CNAME Record:** A DNS record type that maps a hostname to another hostname (an alias). AWS RDS and ElastiCache use CNAMEs for failover DNS — the CNAME target is updated on failover; clients re-resolving the CNAME get the new target.

**ClusterIP:** The default Kubernetes Service type. Assigns a virtual IP within the cluster that is only routable inside the cluster. kube-proxy translates the virtual IP to a healthy pod IP via iptables rules.

**CoreDNS:** The DNS server deployed in every Kubernetes cluster. Runs as a Deployment in `kube-system`. Handles `*.cluster.local` queries from the Kubernetes API; forwards external queries to the VPC resolver.

**ExternalName Service:** A Kubernetes Service type that maps a cluster-internal DNS name to an external DNS hostname via CNAME. Allows pods to use a Kubernetes service name to reach an external resource (e.g., RDS).

**Headless Service:** A Kubernetes Service with `clusterIP: None`. DNS returns individual pod IPs directly instead of a virtual load-balancing IP. Required for stateful applications like Kafka.

**Istiod:** The Istio control plane. Manages certificate issuance, watches Kubernetes resources, and distributes configuration to Envoy proxies via the xDS API.

**mTLS (Mutual TLS):** TLS where both the client and server present X.509 certificates, providing mutual authentication. Istio implements mTLS transparently via Envoy sidecars; applications use plain HTTP.

**ndots:** A DNS resolver option specifying the threshold number of dots in a hostname below which search domains are tried before the absolute hostname. Default in Kubernetes is 5 — hostnames with fewer than 5 dots trigger search domain expansion.

**PeerAuthentication:** An Istio resource that configures the mTLS policy for a namespace or specific pods. Mode STRICT enforces mTLS for all inbound traffic; PERMISSIVE accepts both; DISABLE turns off mTLS.

**Route 53 Resolver:** The DNS resolver service in every AWS VPC, available at `169.254.169.254` (link-local). Answers queries for Route 53 private hosted zones and forwards public queries to Route 53 public resolvers.

**Search Domains:** A list of DNS suffixes appended to short hostnames before attempting absolute resolution. Configured in `/etc/resolv.conf`. Kubernetes pods have the current namespace's domain as the first search domain.

**Service Mesh:** A dedicated infrastructure layer for managing service-to-service communication, typically implemented as sidecar proxies (Envoy) injected into each pod. Provides mTLS, traffic management, and observability transparently.

**SPIFFE (Secure Production Identity Framework For Everyone):** The identity standard used by Istio for pod certificates. Each pod's certificate encodes its identity as a SPIFFE URI: `spiffe://cluster.local/ns/<namespace>/sa/<serviceaccount>`.

**StatefulSet:** A Kubernetes workload resource that provides stable, ordered, persistent pod identities. Each pod gets a stable ordinal name (pod-0, pod-1) and, when combined with a headless service, a stable DNS name. Required for Kafka, ZooKeeper, and other stateful workloads.

**TTL (Time To Live):** A field in DNS records specifying how long resolvers may cache the answer. After TTL seconds, resolvers must re-query. Low TTLs (5–30s) enable fast failover. High TTLs (300–3600s) reduce DNS load but slow failover.

**VirtualService:** An Istio resource that configures routing rules for traffic to a destination — timeouts, retries, fault injection, traffic splits. Applies at the Envoy sidecar level, transparent to application code.

**xDS:** The family of APIs Envoy uses to receive configuration from its control plane (Istiod). CDS=Cluster Discovery Service, LDS=Listener Discovery Service, EDS=Endpoint Discovery Service, RDS=Route Discovery Service.

---

## 20. Self-Assessment

1. Write the Kubernetes DNS FQDN for the `schema-registry` service in namespace `kafka-prod`. What would you write in `bootstrap.servers` for Kafka if you had three brokers in a StatefulSet with headless service `kafka-headless` in `kafka-prod`?
2. Why does a ClusterIP Kubernetes service break Kafka partition replication, and what service configuration does Kafka require instead?
3. A pod in namespace `data-pipelines` cannot resolve `kafka.kafka-prod`. It can resolve `kafka.kafka-prod.svc.cluster.local`. Explain the difference and what search domain configuration causes the short name to fail.
4. What is `ndots:5` and why does `secretsmanager.us-east-1.amazonaws.com` (4 dots) generate extra DNS queries in a Kubernetes pod? How do you prevent this without lowering `ndots` globally?
5. Describe the Route 53 CNAME + low TTL approach for RDS Multi-AZ failover. What happens to a connection using a hardcoded IP after failover vs one using the DNS CNAME?
6. What does `kubectl logs -n kube-system -l k8s-app=kube-dns` showing `Unknown directive 'kbernetes'` indicate, and what is the immediate impact on the cluster?
7. A Prometheus pod (no Istio sidecar) cannot scrape metrics from a pod in a namespace with `PeerAuthentication mode: STRICT`. What Istio resource and configuration change resolves this without disabling mTLS for the entire namespace?
8. What is the difference between `istioctl proxy-status` showing `STALE` vs `SYNCED`, and why does a stale proxy status indicate a potential traffic routing problem?
9. Explain the complete DNS resolution path when a pod in EKS namespace `data-pipelines` queries `secretsmanager.us-east-1.amazonaws.com` with a Secrets Manager interface endpoint and private DNS enabled.
10. `kafka-0` pod in a StatefulSet restarts and gets a new pod IP. Which Kafka client configuration approach continues working correctly — one using the pod IP directly, or one using `kafka-0.kafka-headless.kafka-prod.svc.cluster.local:9092`? Explain why.

---

## 21. Module Summary

DNS and service mesh are the connective tissue of distributed data systems. DNS provides the naming layer that decouples service consumers from the ephemeral IP addresses of service providers. Service meshes provide the traffic management and security layer that operates transparently between services. Together they enable the fundamental property of cloud-native data platforms: services can be restarted, rescheduled, failed over, and scaled without requiring coordinated changes in all their consumers' configuration.

The operational principles that matter most for data engineering: use DNS names, never IP addresses, for all service references. For Kafka in Kubernetes, this requires headless services with StatefulSets that give each broker a stable ordinal hostname — the foundation that makes `advertised.listeners` work correctly across pod restarts. For databases (RDS, Cloud SQL), use the provider-managed DNS CNAME endpoint, not a custom Route 53 record with a long TTL that would undermine automatic failover. For all Kubernetes services, understand the `ndots:5` search domain behavior and its latency implications for external service connections.

CoreDNS is a critical shared dependency: when it fails or is misconfigured, all inter-service communication in the cluster degrades simultaneously. Monitor its cache hit rate, scale it with cluster size, and treat its ConfigMap with the same care as production application configuration — syntax errors take effect immediately.

Istio and service meshes bring mTLS, circuit breaking, and traffic policy management to data platform microservices. The most important operational knowledge for data engineers is understanding that STRICT mTLS breaks non-mesh clients (Prometheus, legacy tools) with connection refusal errors, and that the fix is port-level mTLS exceptions rather than downgrading the entire namespace to PERMISSIVE mode.

This concludes SYS-NET-102: Cloud Networking and Security. The five modules together (M32–M36) cover the complete cloud networking stack from VPC architecture through IAM, security groups, private endpoints, and DNS/service mesh — the end-to-end network security and connectivity model underlying every production data platform.
