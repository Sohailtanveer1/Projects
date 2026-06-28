# M28: DNS and Service Discovery

**Course:** SYS-NET-101 — Networking for Data Engineers  
**Module:** 02 of 05  
**Global Module ID:** M28  
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

Every TCP connection from the previous module starts with a hostname, not an IP address. Before the SYN packet can be sent, the application must resolve `kafka-broker-1.internal` or `postgres.prod.svc.cluster.local` into an IP address. That resolution — DNS — is the invisible prerequisite for every network call in your data infrastructure, and it fails in ways that are uniquely difficult to diagnose.

DNS failures are particularly insidious for three reasons. First, they are often intermittent: a DNS response that is cached works fine, but when the cache expires and the resolver is briefly unavailable, the lookup fails. The failure is transient enough that it doesn't appear in monitoring, but frequent enough to cause data pipeline retries and consumer lag. Second, DNS failures look like network failures to the application: a `UnknownHostException` or `getaddrinfo ENOTFOUND` in a stack trace is often misdiagnosed as a Kafka or database problem when the real issue is a stale or missing DNS record. Third, DNS TTL mismatches are a class of failure unique to distributed systems: when a Kafka broker is replaced and its IP changes, clients that have cached the old IP will continue sending to the dead address until their DNS cache expires — which, if the TTL was set to 24 hours, means 24 hours of failed connections.

There are also three positive reasons DNS matters deeply for data engineers specifically.

**Reason 1 — Service discovery at scale.** A production Kafka cluster has many brokers. A production Spark cluster has many executors. A data engineer should not need to know the IP address of any individual node. Instead, the cluster exposes a DNS name, and the DNS layer handles mapping that name to the right IP. When nodes scale up or down, DNS is updated, and clients using the DNS name automatically route to current nodes. This is the foundation of Kubernetes service discovery and cloud load balancer DNS.

**Reason 2 — Graceful failover.** When a Kafka broker fails, its replacement gets a new IP. If clients discovered the broker via a DNS name (rather than a hardcoded IP), the cluster can update the DNS record and clients that honor TTL will reconnect to the new broker without a configuration change. Engineers who hardcode IPs in `bootstrap.servers` lose this property — they must manually update every producer and consumer configuration when a broker IP changes.

**Reason 3 — Cloud environments are DNS-dependent.** In AWS, GCP, and Azure, every service — RDS, MSK, GKE services, Cloud SQL — is accessed via a DNS name. The DNS name resolves to an internal load balancer IP that itself routes to the actual instance(s). When the cloud platform replaces an instance (auto-healing, scaling, maintenance), it updates the DNS record. Engineers who cache the IP they observed at deployment time will have broken connections after the first maintenance window.

---

## 2. Mental Model

### DNS as a Distributed Phone Book

The phone book analogy is old but structurally correct: DNS maps names to addresses, just as a phone book maps names to phone numbers. But the critical property that makes DNS interesting is that it is a *distributed* phone book — no single server holds all entries. The phone book is split into sections (zones), each section managed by a different authority, and any resolver can navigate the hierarchy to find the right section and get the answer.

The hierarchy is tree-structured, read right to left:

```
kafka-broker-1.prod.data.example.com.
                                    ^─── root (.)
                              ^──────── TLD (.com)
                       ^────────────── registered domain (example.com)
                  ^──────────────────── subdomain (data.example.com)
            ^─────────────────────────── subdomain (prod.data.example.com)
       ^───────────────────────────────── hostname record
```

The resolution process navigates this tree from right to left, asking each level of authority for the next level down, until a definitive answer is reached.

### Caching and TTL: The Freshness Knob

DNS is designed to be fast through caching. Every DNS response includes a TTL (Time To Live) value in seconds. The resolver caches the answer for that many seconds before querying again. This knob controls the trade-off between freshness and load:

- **High TTL (3600–86400 seconds):** Low DNS query rate; stale records persist for hours or days after changes.
- **Low TTL (30–60 seconds):** Near-real-time propagation of changes; high query rate; more resolver load.
- **Zero TTL:** Every lookup forces a fresh query; maximum freshness; no caching allowed.

The right TTL for data infrastructure depends on how frequently the underlying IP changes. A stable database server might use TTL 3600. A Kubernetes pod that gets replaced on every deployment should use TTL 5–30. The wrong TTL is one of the most common silent causes of production outages after infrastructure changes.

### Negative Caching: Caching Failure

DNS also caches *negative* responses — answers that say "this name does not exist" (NXDOMAIN). The duration of negative caching is controlled by the SOA record's minimum TTL (the MINIMUM field). If a service is temporarily unavailable and returns NXDOMAIN, resolvers will cache that negative response for the SOA minimum TTL. New pods that start after the outage may continue to get NXDOMAIN for minutes or hours even after the record is restored, because resolvers are still serving the cached negative response.

---

## 3. Core Concepts

### 3.1 DNS Resolution: The Full Walk

A full DNS resolution for `kafka-broker-1.prod.data.example.com` from a fresh cache:

```
1. Application calls getaddrinfo("kafka-broker-1.prod.data.example.com")
   │
2. OS libc resolver checks /etc/hosts — not found
   │
3. libc resolver checks /etc/nsswitch.conf for resolution order
   │  (typically: files dns — check files first, then DNS)
   │
4. libc resolver sends query to the resolver specified in /etc/resolv.conf
   │  (e.g., 10.0.0.2 — the VPC DNS resolver in AWS)
   │
5. VPC resolver checks its cache — not found
   │
6. VPC resolver queries root nameserver: "Who handles .com?"
   │  Root responds: "Ask the .com TLD servers (e.g., a.gtld-servers.net)"
   │
7. VPC resolver queries .com TLD: "Who handles example.com?"
   │  TLD responds: "Ask ns1.example.com (authoritative for example.com)"
   │
8. VPC resolver queries ns1.example.com: "kafka-broker-1.prod.data.example.com?"
   │  Authoritative server responds with: A record → 10.0.1.47, TTL 60
   │
9. VPC resolver caches the answer (for 60 seconds) and returns 10.0.1.47 to libc
   │
10. libc returns 10.0.1.47 to the application
    │
11. Application opens TCP connection to 10.0.1.47:9092
```

In practice, steps 6–8 are almost never needed — the VPC resolver caches TLD and authoritative server referrals too. The steady-state query path is step 4 → cached response. Steps 6–8 represent the "cold cache" path, and the full walk might add 10–50 ms on the first query.

### 3.2 DNS Record Types

**A record:** Maps a hostname to an IPv4 address.
```
kafka-broker-1.prod.internal.  60  IN  A  10.0.1.47
```

**AAAA record:** Maps a hostname to an IPv6 address.
```
kafka-broker-1.prod.internal.  60  IN  AAAA  fd00::47
```

**CNAME record:** Maps a hostname to another hostname (canonical name). Resolution continues by resolving the target hostname.
```
kafka.prod.internal.  300  IN  CNAME  kafka-nlb-abcd.us-east-1.elb.amazonaws.com.
```
CNAMEs add one extra DNS lookup per hop. A CNAME chain (A → B → C) adds two extra lookups. Kubernetes services use CNAMEs less than direct A/AAAA records; AWS NLBs and ALBs are accessed via CNAME to the load balancer's DNS name.

**SRV record:** Maps a service name to a hostname and port. Used for service discovery where the port is not fixed.
```
_kafka._tcp.prod.internal.  60  IN  SRV  10 50 9092 kafka-broker-1.prod.internal.
                                          ^weight ^port  ^target hostname
```
Kafka clients do not use SRV records — they use a bootstrap mechanism. But other service discovery systems (Consul, Kubernetes headless services) use SRV records to expose port information alongside the address.

**TXT record:** Arbitrary text data. Used for verification (domain ownership), SPF, DKIM, and service configuration metadata.

**PTR record:** Reverse DNS — maps an IP address to a hostname. Used in logs and security tooling.
```
47.1.0.10.in-addr.arpa.  3600  IN  PTR  kafka-broker-1.prod.internal.
```

**SOA record (Start of Authority):** Describes the zone itself — authoritative nameserver, admin email, serial number, refresh/retry intervals, and the minimum TTL for negative caching.
```
prod.internal.  3600  IN  SOA  ns1.prod.internal. admin.example.com. (
    2024010101  ; serial
    3600        ; refresh
    900         ; retry
    604800      ; expire
    60          ; minimum (negative cache TTL)
)
```

**NS record:** Lists the authoritative nameservers for a zone.

### 3.3 The /etc/resolv.conf File

The libc resolver reads `/etc/resolv.conf` to find its upstream DNS servers and search domains:

```
# /etc/resolv.conf (typical Linux server)
nameserver 10.0.0.2      # Primary DNS resolver (VPC DNS in AWS)
nameserver 10.0.0.3      # Secondary/fallback
search prod.internal internal us-east-1.compute.internal
options ndots:5 timeout:2 attempts:2
```

**`nameserver`:** The IP of the recursive resolver to query. In AWS VPCs, this is `169.254.169.253` (the VPC DNS server at the reserved address). In Kubernetes, it's the ClusterIP of the `kube-dns` or `CoreDNS` service.

**`search`:** A list of domain suffixes appended to short names. If you query for `kafka` and the search list is `prod.internal internal`, the resolver tries `kafka.prod.internal`, then `kafka.internal`, then `kafka.` (the literal name). This is why a pod can connect to `kafka` without specifying the full name — the search domain expands it.

**`ndots`:** The minimum number of dots in a name before it's tried as an absolute name first. `ndots:5` means a name with fewer than 5 dots will first be tried with each search domain appended before trying it bare. In Kubernetes, `ndots:5` causes every short service name (like `kafka`) to generate up to 5 DNS queries before the right one is found — a significant source of DNS query volume in large clusters.

**`timeout`:** Seconds to wait for a response from each nameserver before trying the next.

**`attempts`:** Number of times to query each nameserver before giving up.

### 3.4 Java DNS Caching and the JVM TTL Problem

The JVM has its own DNS cache, independent of the OS resolver. This matters enormously for Kafka and Spark because both run on the JVM.

By default in many JVM versions and distributions:
- `networkaddress.cache.ttl = -1` in `$JAVA_HOME/lib/security/java.security` means **cache forever** (positive DNS results cached indefinitely until JVM restart).
- `networkaddress.cache.negative.ttl = 10` caches negative results for 10 seconds.

**Implication:** A Kafka broker that had IP `10.0.1.47` is replaced and gets IP `10.0.1.88`. The DNS A record is updated with TTL 60. The OS resolver updates after 60 seconds. But the Kafka client JVM has `networkaddress.cache.ttl=-1` — it never re-queries DNS. It continues sending to `10.0.1.47`, which is now dead. The connection will fail, the client will try to reconnect, and when reconnecting it will re-resolve the DNS name — at which point it gets the new IP. So the failure is self-healing on reconnect, but only because the reconnect forces a new DNS lookup.

The production-safe configuration for JVMs connecting to infrastructure that may change IPs:
```
# In $JAVA_HOME/lib/security/java.security or via JVM flag:
networkaddress.cache.ttl=60        # Re-resolve DNS every 60 seconds
networkaddress.cache.negative.ttl=10  # Don't cache failures for long
```

Or via JVM startup flag: `-Dsun.net.inetaddr.ttl=60`

AWS documentation explicitly recommends setting this for applications connecting to AWS services (RDS, ElastiCache, MSK) because AWS may change the underlying IP of a managed service without changing its DNS name.

### 3.5 Kubernetes DNS: CoreDNS and the Service Abstraction

In Kubernetes, DNS is the primary service discovery mechanism. CoreDNS runs as a Deployment in the `kube-system` namespace, exposed as a ClusterIP service. Every Pod's `/etc/resolv.conf` points to the CoreDNS ClusterIP.

**Kubernetes DNS name structure:**
```
<service-name>.<namespace>.svc.<cluster-domain>
```
For example: `kafka.kafka-prod.svc.cluster.local`

**ClusterIP services:** A single stable virtual IP (the ClusterIP) that kube-proxy routes to the healthy pod endpoints. The DNS name resolves to the ClusterIP, which is stable even as pods come and go.

```
kafka.kafka-prod.svc.cluster.local  5  IN  A  172.20.0.150   (ClusterIP)
```

**Headless services** (`clusterIP: None`): Instead of resolving to a ClusterIP, headless service DNS returns *all* pod IPs directly:
```
kafka.kafka-prod.svc.cluster.local  5  IN  A  10.0.1.5
kafka.kafka-prod.svc.cluster.local  5  IN  A  10.0.1.6
kafka.kafka-prod.svc.cluster.local  5  IN  A  10.0.1.7
```
This enables client-side load balancing — the client gets all IPs and can choose which to connect to. Kafka clients use this pattern: the bootstrap address resolves to all broker IPs, and the client connects to each to get the full broker list.

**StatefulSet DNS:** Each pod in a StatefulSet gets a stable DNS name based on its ordinal:
```
kafka-0.kafka.kafka-prod.svc.cluster.local  → 10.0.1.5
kafka-1.kafka.kafka-prod.svc.cluster.local  → 10.0.1.6
kafka-2.kafka.kafka-prod.svc.cluster.local  → 10.0.1.7
```
This is how Kafka brokers are addressed in Kubernetes — each broker has a stable, predictable DNS name. When `kafka-1` pod is replaced, the new pod comes up with the same ordinal and the same DNS name, allowing other brokers to reconnect without configuration changes.

### 3.6 DNS TTL Propagation and the Update Window

When a DNS record is changed, the change propagates as old cache entries expire. The update window is determined by the TTL of the old record — not the new one.

**Example:** You change a DNS record from `10.0.1.47` to `10.0.1.88`. The old record had TTL 3600. Resolvers that cached the old record within the last 3600 seconds will continue returning `10.0.1.47` for up to 3600 seconds after the change. Only after their cached entry expires will they query the authoritative server and get the new value.

**Pre-change TTL reduction:** The professional practice for planned IP migrations is to reduce the TTL well before the change:
1. T-48h: Reduce TTL from 3600 to 60 seconds
2. Wait 3601 seconds (old TTL) for all caches to expire and pick up the new TTL=60 record
3. T-0: Make the IP change
4. After 60 seconds, all resolvers have the new IP
5. T+1h: Optionally restore TTL to 3600 for stability

### 3.7 Split-Horizon DNS

Split-horizon (split-brain) DNS presents different answers for the same name depending on the source of the query. The most common pattern in data engineering:

- **Internal clients** (within the VPC) get the private IP: `kafka.prod.internal → 10.0.1.47`
- **External clients** (or clients in a different VPC) get the public IP or a different endpoint: `kafka.prod.internal → 203.0.113.47` (or NXDOMAIN)

This is implemented in AWS using Route 53 private hosted zones (visible only within designated VPCs) alongside public zones with the same or different names.

**Problem:** A developer running a Kafka consumer on their laptop gets NXDOMAIN for `kafka.prod.internal` because the private zone is only visible inside the VPC. The fix is either VPN (which routes DNS queries through the corporate resolver, which can resolve private zones) or a bastion host.

### 3.8 Service Mesh Basics

A service mesh adds a network proxy sidecar (typically Envoy) to each pod in a Kubernetes cluster. The sidecar intercepts all inbound and outbound network traffic and provides:
- **mTLS between all services** (automatic certificate management)
- **Load balancing and circuit breaking** (beyond what kube-proxy provides)
- **Observability** (automatic metrics, traces, and logs for every service call)
- **Traffic management** (canary deployments, traffic splitting)

**DNS interaction:** In a service mesh (Istio, Linkerd, Consul Connect), DNS resolution works the same as without the mesh — the application queries `kafka.kafka-prod.svc.cluster.local` and gets a ClusterIP. The sidecar proxy then intercepts the connection to that ClusterIP and applies mesh policies (TLS, load balancing, retries).

The key change is that the sidecar proxy, not the kernel, manages the TCP connection to the backend. DNS-reported IPs may be virtual (ClusterIP) that the proxy translates to actual pod IPs using its own service endpoint registry (from the control plane, not from DNS).

**Relevance for Kafka:** Running Kafka inside a service mesh is non-trivial. Kafka uses its own advertised listeners mechanism for service discovery — clients connect to the bootstrap address, discover the full broker list (as IP:port pairs from the broker metadata response), and then connect directly to each broker. A service mesh sidecar intercepts these direct broker connections, but the broker advertised listeners must be configured correctly to expose the mesh-accessible addresses, not the raw pod IPs.

---

## 4. Hands-On Walkthrough

### 4.1 Tracing a DNS Resolution with `dig`

```bash
# Full iterative resolution (trace every step)
dig +trace kafka-broker-1.prod.internal A

# Simple lookup with TTL visible
dig kafka-broker-1.prod.internal A
# Answer section shows:
# kafka-broker-1.prod.internal. 57 IN A 10.0.1.47
#                                ^^ TTL (seconds remaining in cache)

# Query a specific nameserver directly (bypass local cache)
dig @10.0.0.2 kafka-broker-1.prod.internal A

# Check all record types
dig kafka-broker-1.prod.internal ANY

# Reverse lookup (PTR)
dig -x 10.0.1.47

# Check SOA (minimum TTL for negative caching)
dig prod.internal SOA
```

### 4.2 Checking /etc/resolv.conf and Search Domain Behavior

```bash
# View current resolver config
cat /etc/resolv.conf

# See what the OS would actually resolve (includes search domain expansion)
getent hosts kafka    # short name — will apply search domains

# Trace the full resolution including search domain attempts
strace -e trace=network -f getent hosts kafka 2>&1 | grep sendmsg
```

### 4.3 Diagnosing Kubernetes DNS

```bash
# Check CoreDNS pods
kubectl -n kube-system get pods -l k8s-app=kube-dns

# View a pod's resolv.conf
kubectl exec -it <pod-name> -- cat /etc/resolv.conf
# Expected:
# nameserver 10.96.0.10        (CoreDNS ClusterIP)
# search kafka-prod.svc.cluster.local svc.cluster.local cluster.local
# options ndots:5

# Test DNS from inside a pod
kubectl exec -it <pod-name> -- nslookup kafka.kafka-prod.svc.cluster.local

# Check CoreDNS logs for errors
kubectl -n kube-system logs -l k8s-app=kube-dns --tail=100

# Check CoreDNS config (Corefile)
kubectl -n kube-system get configmap coredns -o yaml

# Count DNS queries per second (via CoreDNS metrics if Prometheus is configured)
# kubectl exec -it <coredns-pod> -- curl -s localhost:9153/metrics | grep coredns_dns_requests_total
```

### 4.4 Diagnosing DNS Latency

```bash
# Measure DNS resolution time
time dig kafka-broker-1.prod.internal A

# More precise measurement
dig +stats kafka-broker-1.prod.internal A
# Query time: 1 msec   ← from cache; should be < 5ms
# Query time: 43 msec  ← cache miss, full resolution

# Bulk measurement with parallel queries (dnsperftest tool)
dnsperf -s 10.0.0.2 -d /tmp/queries.txt -t 5

# Monitor DNS resolution time from an application perspective
strace -Tve trace=network python3 -c "import socket; socket.getaddrinfo('kafka.prod.internal', 9092)"
# Each network syscall shows its duration after each call
```

### 4.5 Testing DNS Changes Before They Go Live

```bash
# Query the authoritative nameserver directly (not cached VPC resolver)
# First find the authoritative NS for your zone:
dig prod.internal NS

# Then query it directly:
dig @ns1.prod.internal kafka-broker-1.prod.internal A

# This bypasses all caches and shows what the authoritative server will return
# Useful to verify a record change before it propagates

# Test from a specific namespace (simulates remote client perspective)
unshare --net dig @8.8.8.8 kafka.public-endpoint.example.com A
```

### 4.6 Diagnosing JVM DNS Cache

```bash
# Check the JVM security policy file for TTL settings
find $JAVA_HOME -name "java.security" | xargs grep -E "cache.ttl|cache.negative"

# For a running JVM, check via JMX or arthas (Java diagnostic tool)
# If using arthas (attach to running JVM):
# ognl "@java.net.InetAddress@addressCache"
# ognl "@java.net.InetAddress@negativeCache"

# Force DNS re-resolution in a Kafka producer (Java):
# Set in application config or JVM flags:
# -Dsun.net.inetaddr.ttl=60
# -Dsun.net.inetaddr.negative.ttl=5
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
dns_diagnostics.py

DNS and service discovery diagnostic toolkit for data engineers.
Diagnoses DNS resolution, TTL, Kubernetes service discovery,
and Java DNS cache configuration.

Dependencies: dnspython (pip install dnspython)
              subprocess (stdlib)
"""

import os
import re
import time
import socket
import struct
import subprocess
import ipaddress
from dataclasses import dataclass, field
from typing import Optional
import sys


# ─── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class DNSRecord:
    """A single DNS record returned from a query."""
    name: str
    record_type: str      # A, AAAA, CNAME, SRV, TXT, PTR, SOA
    ttl: int              # seconds
    value: str            # IP address, target hostname, or text


@dataclass
class ResolutionResult:
    """Result of resolving a hostname, including timing and cache state."""
    hostname: str
    resolved_addresses: list[str] = field(default_factory=list)
    cname_chain: list[str] = field(default_factory=list)
    ttl: int = -1
    resolution_ms: float = 0.0
    from_cache: bool = False
    error: Optional[str] = None


@dataclass
class ResolverConfig:
    """Parsed /etc/resolv.conf configuration."""
    nameservers: list[str] = field(default_factory=list)
    search_domains: list[str] = field(default_factory=list)
    ndots: int = 1
    timeout_s: int = 5
    attempts: int = 2


@dataclass
class K8sServiceInfo:
    """Information about a Kubernetes service from DNS."""
    service_name: str
    namespace: str
    cluster_domain: str = "cluster.local"
    cluster_ip: Optional[str] = None    # None if headless
    pod_ips: list[str] = field(default_factory=list)  # populated for headless
    is_headless: bool = False
    resolution_ms: float = 0.0


# ─── /etc/resolv.conf Parser ────────────────────────────────────────────────────

def parse_resolv_conf(path: str = '/etc/resolv.conf') -> ResolverConfig:
    """Parse /etc/resolv.conf into a structured config."""
    config = ResolverConfig()
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith('#') or not line:
                    continue
                parts = line.split()
                if not parts:
                    continue
                key = parts[0]
                if key == 'nameserver' and len(parts) > 1:
                    config.nameservers.append(parts[1])
                elif key == 'search':
                    config.search_domains.extend(parts[1:])
                elif key == 'options':
                    for opt in parts[1:]:
                        if opt.startswith('ndots:'):
                            config.ndots = int(opt.split(':')[1])
                        elif opt.startswith('timeout:'):
                            config.timeout_s = int(opt.split(':')[1])
                        elif opt.startswith('attempts:'):
                            config.attempts = int(opt.split(':')[1])
    except IOError as e:
        config.nameservers = ['(could not read resolv.conf: ' + str(e) + ')']
    return config


# ─── DNS Resolution with Timing ─────────────────────────────────────────────────

def resolve_hostname(
    hostname: str, timeout_s: float = 5.0
) -> ResolutionResult:
    """
    Resolve a hostname to IP addresses using the OS resolver (including
    /etc/hosts and search domain expansion).
    Measures resolution latency.
    """
    result = ResolutionResult(hostname=hostname)
    start = time.perf_counter()
    try:
        # getaddrinfo returns list of (family, type, proto, canonname, sockaddr)
        infos = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC,
                                   socket.SOCK_STREAM)
        result.resolution_ms = (time.perf_counter() - start) * 1000
        seen = set()
        for info in infos:
            addr = info[4][0]
            if addr not in seen:
                seen.add(addr)
                result.resolved_addresses.append(addr)
        if not result.resolved_addresses:
            result.error = "No addresses returned"
    except socket.gaierror as e:
        result.resolution_ms = (time.perf_counter() - start) * 1000
        result.error = f"DNS resolution failed: {e}"
    return result


def resolve_with_search_domains(
    short_name: str, resolv_conf: ResolverConfig
) -> list[ResolutionResult]:
    """
    Simulate search domain expansion: try the name with each search domain
    and the bare name, in the order the OS resolver would attempt them.
    Returns results for each attempt.
    """
    candidates = []
    dot_count = short_name.count('.')
    bare_first = dot_count >= resolv_conf.ndots

    if bare_first:
        candidates.append(short_name)

    for domain in resolv_conf.search_domains:
        candidates.append(f"{short_name}.{domain}")

    if not bare_first:
        candidates.append(short_name)

    results = []
    for candidate in candidates:
        r = resolve_hostname(candidate)
        results.append(r)
        if not r.error:
            break  # OS stops at first success
    return results


def bulk_resolve(
    hostnames: list[str], timeout_s: float = 5.0
) -> dict[str, ResolutionResult]:
    """Resolve multiple hostnames and return a dict of hostname → result."""
    return {h: resolve_hostname(h, timeout_s) for h in hostnames}


# ─── dig/nslookup wrappers ───────────────────────────────────────────────────────

def dig_query(
    hostname: str,
    record_type: str = 'A',
    nameserver: Optional[str] = None,
) -> dict:
    """
    Run a dig query and return parsed results.
    Returns dict with: answers, ttl, query_time_ms, authority, flags.
    """
    cmd = ['dig', '+noall', '+answer', '+stats', '+authority']
    if nameserver:
        cmd.append(f'@{nameserver}')
    cmd.extend([hostname, record_type])

    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT,
                                      timeout=10).decode()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError) as e:
        return {'error': str(e), 'answers': [], 'ttl': -1}

    records = []
    min_ttl = -1
    query_time_ms = -1

    for line in out.splitlines():
        # Parse answer/authority records
        # Format: <name> <ttl> IN <type> <value>
        m = re.match(
            r'^(\S+)\s+(\d+)\s+IN\s+(\w+)\s+(.*)', line.strip()
        )
        if m:
            name, ttl, rtype, value = m.groups()
            ttl = int(ttl)
            records.append(DNSRecord(
                name=name, record_type=rtype, ttl=ttl, value=value.strip()
            ))
            if min_ttl == -1 or ttl < min_ttl:
                min_ttl = ttl

        # Parse query time from stats
        if 'Query time:' in line:
            m2 = re.search(r'Query time: (\d+) msec', line)
            if m2:
                query_time_ms = int(m2.group(1))

    return {
        'answers': [r for r in records
                    if r.record_type == record_type.upper()],
        'all_records': records,
        'ttl': min_ttl,
        'query_time_ms': query_time_ms,
    }


def get_authoritative_nameservers(zone: str) -> list[str]:
    """Return the authoritative nameservers for a DNS zone."""
    result = dig_query(zone, 'NS')
    return [r.value.rstrip('.') for r in result.get('all_records', [])
            if r.record_type == 'NS']


def get_soa_ttl(zone: str) -> int:
    """
    Return the SOA minimum TTL (negative cache TTL) for a zone.
    This is the last field in the SOA RDATA.
    """
    result = dig_query(zone, 'SOA')
    for r in result.get('all_records', []):
        if r.record_type == 'SOA':
            # SOA RDATA: mname rname serial refresh retry expire minimum
            parts = r.value.split()
            if len(parts) >= 7:
                return int(parts[-1])
    return -1


# ─── Kubernetes DNS ──────────────────────────────────────────────────────────────

def resolve_k8s_service(
    service: str,
    namespace: str,
    cluster_domain: str = 'cluster.local',
) -> K8sServiceInfo:
    """
    Resolve a Kubernetes service DNS name and detect whether it is
    a ClusterIP service (single IP) or headless service (multiple IPs).
    """
    fqdn = f"{service}.{namespace}.svc.{cluster_domain}"
    info = K8sServiceInfo(
        service_name=service,
        namespace=namespace,
        cluster_domain=cluster_domain,
    )

    start = time.perf_counter()
    result = resolve_hostname(fqdn)
    info.resolution_ms = (time.perf_counter() - start) * 1000

    if result.error:
        return info

    addresses = result.resolved_addresses
    if len(addresses) == 1:
        # Single IP → ClusterIP service
        info.cluster_ip = addresses[0]
        info.is_headless = False
    elif len(addresses) > 1:
        # Multiple IPs → headless service (returns pod IPs directly)
        info.pod_ips = addresses
        info.is_headless = True

    return info


def check_k8s_coredns_health() -> dict:
    """
    Check CoreDNS health by querying the CoreDNS ClusterIP
    (typically first entry in /etc/resolv.conf in-cluster).
    Returns health status and response times.
    """
    config = parse_resolv_conf()
    if not config.nameservers:
        return {'status': 'error', 'reason': 'No nameservers in resolv.conf'}

    coredns_ip = config.nameservers[0]
    results = []

    # Query a known-good internal name
    for _ in range(3):
        r = dig_query('kubernetes.default.svc.cluster.local', 'A',
                      nameserver=coredns_ip)
        results.append(r.get('query_time_ms', -1))

    valid = [t for t in results if t >= 0]
    if not valid:
        return {'status': 'error', 'reason': 'No responses from CoreDNS',
                'coredns_ip': coredns_ip}

    return {
        'status': 'ok',
        'coredns_ip': coredns_ip,
        'response_times_ms': valid,
        'avg_ms': sum(valid) / len(valid),
        'max_ms': max(valid),
    }


# ─── JVM DNS Cache Checker ───────────────────────────────────────────────────────

def check_jvm_dns_settings() -> dict:
    """
    Attempt to find and read JVM DNS cache TTL settings from
    the java.security file in the active Java installation.
    """
    java_home = os.environ.get('JAVA_HOME', '')
    if not java_home:
        # Try to find java in PATH
        try:
            java_path = subprocess.check_output(
                ['which', 'java'], stderr=subprocess.DEVNULL
            ).decode().strip()
            # Resolve symlinks
            real_java = os.path.realpath(java_path)
            java_home = os.path.dirname(os.path.dirname(real_java))
        except (subprocess.CalledProcessError, FileNotFoundError):
            java_home = ''

    result = {
        'java_home': java_home,
        'cache_ttl': None,
        'negative_cache_ttl': None,
        'security_file': None,
        'warnings': [],
    }

    if not java_home:
        result['warnings'].append("JAVA_HOME not set and java not found in PATH")
        return result

    security_file = os.path.join(java_home, 'lib', 'security', 'java.security')
    if not os.path.exists(security_file):
        # Older Java layout
        security_file = os.path.join(
            java_home, 'jre', 'lib', 'security', 'java.security'
        )

    if not os.path.exists(security_file):
        result['warnings'].append(
            f"java.security not found under {java_home}"
        )
        return result

    result['security_file'] = security_file

    try:
        with open(security_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith('#'):
                    continue
                if 'networkaddress.cache.ttl' in line and 'negative' not in line:
                    parts = line.split('=', 1)
                    if len(parts) == 2:
                        result['cache_ttl'] = parts[1].strip()
                elif 'networkaddress.cache.negative.ttl' in line:
                    parts = line.split('=', 1)
                    if len(parts) == 2:
                        result['negative_cache_ttl'] = parts[1].strip()
    except IOError as e:
        result['warnings'].append(f"Could not read {security_file}: {e}")

    # Assess the settings
    ttl = result['cache_ttl']
    if ttl == '-1' or ttl is None:
        result['warnings'].append(
            "networkaddress.cache.ttl=-1 (or unset=default -1): JVM caches DNS "
            "FOREVER. If Kafka broker IPs change, JVM will not re-resolve until "
            "connection fails and is re-established. "
            "Recommend setting to 60."
        )
    elif ttl == '0':
        result['warnings'].append(
            "networkaddress.cache.ttl=0: JVM never caches DNS. Every hostname "
            "resolution goes to the OS resolver. May cause high DNS query volume "
            "under heavy connection creation. Recommend 30–60."
        )

    return result


# ─── DNS Propagation Checker ─────────────────────────────────────────────────────

def check_dns_propagation(
    hostname: str,
    record_type: str = 'A',
    nameservers: Optional[list[str]] = None,
) -> dict:
    """
    Query multiple nameservers for the same record and check for consistency.
    Useful to verify that a DNS change has propagated.
    """
    if nameservers is None:
        # Use the configured resolver plus Google's and Cloudflare's
        config = parse_resolv_conf()
        nameservers = config.nameservers[:1] + ['8.8.8.8', '1.1.1.1']

    results = {}
    for ns in nameservers:
        r = dig_query(hostname, record_type, nameserver=ns)
        answers = [rec.value for rec in r.get('answers', [])]
        results[ns] = {
            'addresses': answers,
            'ttl': r.get('ttl', -1),
            'query_time_ms': r.get('query_time_ms', -1),
        }

    # Check consistency
    address_sets = [frozenset(v['addresses']) for v in results.values()
                    if v['addresses']]
    consistent = len(set(address_sets)) <= 1 if address_sets else False

    return {
        'hostname': hostname,
        'record_type': record_type,
        'consistent': consistent,
        'per_nameserver': results,
    }


# ─── Report ──────────────────────────────────────────────────────────────────────

def dns_health_report(test_hostnames: Optional[list[str]] = None) -> None:
    """Print a DNS diagnostic report."""
    print("=" * 70)
    print("DNS / SERVICE DISCOVERY HEALTH REPORT")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # --- Resolver config
    print("\n[1] Resolver Configuration (/etc/resolv.conf)")
    config = parse_resolv_conf()
    for ns in config.nameservers:
        print(f"  nameserver: {ns}")
    for sd in config.search_domains:
        print(f"  search: {sd}")
    print(f"  ndots: {config.ndots}", end="")
    if config.ndots >= 5:
        print(
            "  (Kubernetes default — short names generate up to "
            f"{len(config.search_domains) + 1} queries each)", end=""
        )
    print()

    # --- Hostname resolution tests
    if test_hostnames:
        print(f"\n[2] Hostname Resolution Tests")
        for h in test_hostnames:
            r = resolve_hostname(h)
            if r.error:
                print(f"  {h:<50} ✗ {r.error} ({r.resolution_ms:.1f}ms)")
            else:
                addrs = ', '.join(r.resolved_addresses)
                flag = "⚠️ SLOW" if r.resolution_ms > 50 else ""
                print(f"  {h:<50} {addrs}  ({r.resolution_ms:.1f}ms) {flag}")

    # --- JVM DNS cache
    print("\n[3] JVM DNS Cache Configuration")
    jvm = check_jvm_dns_settings()
    if jvm['java_home']:
        print(f"  JAVA_HOME: {jvm['java_home']}")
        print(f"  Security file: {jvm['security_file']}")
        print(f"  networkaddress.cache.ttl: {jvm['cache_ttl'] or '(not set — defaults to -1)'}")
        print(f"  networkaddress.cache.negative.ttl: {jvm['negative_cache_ttl'] or '(not set)'}")
    else:
        print("  Java not found in PATH or JAVA_HOME")
    for w in jvm['warnings']:
        print(f"  ⚠️  {w}")

    # --- Kubernetes DNS (if in-cluster)
    if os.path.exists('/var/run/secrets/kubernetes.io'):
        print("\n[4] Kubernetes CoreDNS Health")
        health = check_k8s_coredns_health()
        print(f"  CoreDNS IP: {health.get('coredns_ip', 'unknown')}")
        if health['status'] == 'ok':
            print(f"  Status: ✓ responding")
            print(f"  Avg response: {health['avg_ms']:.1f}ms  "
                  f"Max: {health['max_ms']:.1f}ms")
            if health['max_ms'] > 10:
                print("  ⚠️  DNS latency > 10ms may indicate CoreDNS under load")
        else:
            print(f"  Status: ✗ {health.get('reason', 'unknown error')}")
    else:
        print("\n[4] Kubernetes DNS: not running in-cluster (skipped)")

    print()
    print("=" * 70)


# ─── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DNS diagnostic toolkit")
    parser.add_argument('--hosts', nargs='+',
                        help='Hostnames to resolve and test')
    parser.add_argument('--propagation', metavar='HOSTNAME',
                        help='Check DNS propagation consistency for a hostname')
    parser.add_argument('--jvm', action='store_true',
                        help='Check JVM DNS cache settings only')
    args = parser.parse_args()

    if args.jvm:
        jvm = check_jvm_dns_settings()
        print(f"JAVA_HOME: {jvm['java_home']}")
        print(f"cache.ttl: {jvm['cache_ttl']}")
        print(f"negative.ttl: {jvm['negative_cache_ttl']}")
        for w in jvm['warnings']:
            print(f"WARNING: {w}")
    elif args.propagation:
        result = check_dns_propagation(args.propagation)
        print(f"DNS propagation check: {result['hostname']}")
        print(f"Consistent: {result['consistent']}")
        for ns, data in result['per_nameserver'].items():
            print(f"  {ns}: {data['addresses']} (TTL={data['ttl']}s, "
                  f"{data['query_time_ms']}ms)")
    else:
        dns_health_report(test_hostnames=args.hosts or [
            'localhost',
            socket.gethostname(),
        ])
```

---

## 6. Visual Reference

### DNS Resolution Hierarchy

```
Application
    │  "resolve kafka.prod.internal"
    ▼
/etc/nsswitch.conf
    │  order: files dns
    ▼
/etc/hosts ──────────────── check first
    │  not found
    ▼
/etc/resolv.conf ─────────── read nameserver IP
    │  nameserver 10.0.0.2
    ▼
VPC Resolver (10.0.0.2) ─── check cache
    │  cache miss
    ▼
Root Nameservers (.)  ────── who handles .internal?
    │  → authoritative NS for .internal
    ▼
Authoritative NS ─────────── definitive answer
    │  kafka.prod.internal A 10.0.1.47 TTL 60
    ▼
VPC Resolver caches for 60s and returns 10.0.1.47
    ▼
Application opens TCP to 10.0.1.47:9092
```

### Kubernetes DNS Name Structure

```
<service>.<namespace>.svc.<cluster-domain>
   │          │              │
   │          │              └── cluster.local (default)
   │          └──────────────── kafka-prod
   └─────────────────────────── kafka

Full FQDN: kafka.kafka-prod.svc.cluster.local

ClusterIP service (single IP):
  kafka.kafka-prod.svc.cluster.local  →  172.20.0.150 (virtual IP, stable)

Headless service (direct pod IPs):
  kafka.kafka-prod.svc.cluster.local  →  10.0.1.5
                                      →  10.0.1.6
                                       →  10.0.1.7

StatefulSet per-pod:
  kafka-0.kafka.kafka-prod.svc.cluster.local  →  10.0.1.5  (stable)
  kafka-1.kafka.kafka-prod.svc.cluster.local  →  10.0.1.6  (stable)
```

### TTL Propagation Window

```
Time
  │
  ├── T-48h: Reduce TTL 3600 → 60 seconds
  │          Old TTL: caches expire old value in up to 3600s
  │
  ├── T-45h: All caches have picked up TTL=60 record
  │          (3600 seconds after T-48h)
  │
  ├── T-0:   Change IP from 10.0.1.47 → 10.0.1.88
  │
  ├── T+60s: All resolvers have the new IP (TTL=60 expired)
  │
  ├── T+1h:  Optionally restore TTL to 3600 for DNS load reduction
  │
  └── Done
```

---

## 7. Common Mistakes

**Mistake 1: Hardcoding IP addresses in `bootstrap.servers`.** `bootstrap.servers=10.0.1.47:9092,10.0.1.48:9092,10.0.1.49:9092` breaks when broker IPs change — after a broker replacement, a blue-green deployment, or an auto-healing event. Use DNS names: `bootstrap.servers=kafka-0.kafka.prod.internal:9092,kafka-1.kafka.prod.internal:9092,kafka-2.kafka.prod.internal:9092`. With DNS names, a broker replacement only requires a DNS record update; no client configuration changes.

**Mistake 2: Not pre-reducing TTL before a migration.** Changing a DNS record from IP A to IP B with a 24-hour TTL means up to 24 hours of dual-routing — some clients still resolving to A, some to B. Always pre-reduce TTL to 60 seconds at least one TTL period before the migration.

**Mistake 3: Ignoring JVM `networkaddress.cache.ttl=-1`.** The JVM default (in many distributions) of caching DNS results forever means Kafka and Spark applications will not adapt to IP changes until the connection fails and reconnects. Always set `sun.net.inetaddr.ttl=60` for JVMs connecting to infrastructure.

**Mistake 4: `ndots:5` in Kubernetes without understanding the query multiplication.** With `ndots:5` and a search list of five domains, a query for a short name like `kafka` generates up to 6 DNS queries (one per search domain + bare name). In a large Kubernetes cluster with many pods, this is a significant source of CoreDNS load. Use FQDNs with a trailing dot (`kafka.kafka-prod.svc.cluster.local.`) in configuration when possible — the trailing dot tells the resolver it's already absolute, skipping all search domain expansions.

**Mistake 5: Assuming DNS failure means the service is down.** DNS failures (`NXDOMAIN`, `SERVFAIL`) can happen when the DNS infrastructure is overloaded, when the resolv.conf nameserver is unreachable, or when negative caching is serving a stale failure. Always verify that the IP is actually unreachable before concluding the service is down.

**Mistake 6: Not testing split-horizon DNS before deploying cross-team.** A service registered in a private hosted zone is invisible from outside the VPC. An engineer testing from their laptop gets NXDOMAIN and concludes the DNS record is wrong, when the record is correct but private. Document split-horizon configurations and provide alternative resolution methods (VPN, bastion) for developers.

---

## 8. Production Failure Scenarios

### Scenario 1: Kafka Producers Stall After Broker Replacement

**Symptoms:** After replacing a Kafka broker node (new instance, new IP), producers to that broker's partitions begin failing with `TimeoutException: Failed to update metadata after 60000 ms`. Other brokers are unaffected. The new broker is healthy and accepting connections manually. The failure lasts 2–4 minutes, then self-heals.

**Root cause:** JVM DNS cache. The producer's JVM resolved the broker's hostname 3 hours ago and cached the result with `networkaddress.cache.ttl=-1`. The new broker has a new IP that the DNS record now points to, but the JVM still sends to the old (dead) IP. The `TimeoutException` appears after `request.timeout.ms`. When the producer attempts to re-bootstrap (reconnect to the bootstrap servers), it re-calls `getaddrinfo`, which *does* hit the OS resolver (not the JVM cache, which uses `InetAddress.getByName` internally). The reconnect resolves the new IP and the producer recovers.

The reason it only lasts 2–4 minutes: `request.timeout.ms` (60s by default) plus metadata refresh retry interval. The JVM cache is bypassed on reconnect because Kafka uses `InetAddress.getAllByName` which does respect the JVM cache — but after a connection failure, the cache entry for that host is invalidated (in Java 6+). So the self-heal happens when the connection is explicitly closed and re-opened.

**Fix:** Set `sun.net.inetaddr.ttl=60` JVM property for all Kafka client JVMs. Add to the Kafka producer/consumer startup: `-Dsun.net.inetaddr.ttl=60 -Dsun.net.inetaddr.negative.ttl=5`.

### Scenario 2: CoreDNS CPU Spike Under Load

**Symptoms:** Kubernetes data pipeline pods (Spark executors, Airflow workers) begin experiencing intermittent DNS resolution timeouts. `kubectl -n kube-system top pod -l k8s-app=kube-dns` shows CoreDNS pods at 90%+ CPU. The problem correlates with job start time when many pods are launched simultaneously.

**Root cause:** DNS query amplification from `ndots:5`. Each pod's resolv.conf has `ndots:5` and five search domains. Every short service name generates 6 queries instead of 1. During job startup, hundreds of Spark executors simultaneously resolve dozens of hostnames, generating thousands of DNS queries per second — well above CoreDNS's default capacity.

**Diagnosis:**
```bash
# Count DNS queries per second from CoreDNS metrics
kubectl exec -n kube-system <coredns-pod> -- \
  wget -qO- localhost:9153/metrics | grep coredns_dns_requests_total

# Check for SERVFAIL responses (CoreDNS dropping overloaded queries)
kubectl -n kube-system logs <coredns-pod> | grep SERVFAIL
```

**Fix:** Three complementary approaches:
1. Use FQDNs with trailing dots in service configuration files to bypass search domain expansion.
2. Scale CoreDNS horizontally: `kubectl -n kube-system scale deployment coredns --replicas=4`.
3. Enable NodeLocal DNSCache — a DaemonSet that runs a DNS cache on each node, reducing queries to the central CoreDNS. This reduces CoreDNS load by ~80% in large clusters.

### Scenario 3: Silent Consumer Partition Assignment Failure from DNS Split-Brain

**Symptoms:** A Kafka consumer group in Kubernetes fails to rebalance. Some consumers in the group can reach the group coordinator; others cannot. The coordinator is healthy. The consumers that fail show `NetworkException: Disconnected from node <broker-id>` during the rebalance handshake.

**Root cause:** DNS split-brain. The Kafka broker advertised listener is configured with its hostname (`kafka-0.kafka.prod.svc.cluster.local`). Consumers in Namespace A (same cluster) resolve this correctly. Consumers in Namespace B (different Kubernetes cluster joined via cluster federation) resolve the name through a different DNS authority that returns a stale or missing record.

The split-brain arose because the broker was migrated between StatefulSet configurations, and the old DNS record (pointing to a decommissioned ClusterIP) was not cleaned up in the federated cluster's DNS.

**Fix:** Clean up stale DNS records. Verify that all consumer pods resolve the broker advertised listeners consistently:
```bash
# From each consumer pod namespace:
kubectl exec -it <consumer-pod> -n namespace-a -- \
  nslookup kafka-0.kafka.prod.svc.cluster.local

kubectl exec -it <consumer-pod> -n namespace-b -- \
  nslookup kafka-0.kafka.prod.svc.cluster.local
```
Both must return the same IP. Resolve the discrepancy before restarting the consumer group.

### Scenario 4: Negative DNS Cache Causes Extended Downtime After Recovery

**Symptoms:** A Kafka Schema Registry goes down for maintenance. After it is restored (5 minutes downtime), Kafka producers that use schema validation continue to fail for an additional 10 minutes. The Schema Registry is verified healthy via its REST API. DNS records are correct.

**Root cause:** Negative DNS caching. During the downtime, DNS queries for the Schema Registry hostname returned `SERVFAIL` (because the Schema Registry host was down and its health check caused it to be removed from the load balancer DNS). These `SERVFAIL` responses were cached by the VPC resolver for the SOA minimum TTL (600 seconds in this case). After restoration, the VPC resolver is still serving the cached negative response for up to 600 more seconds.

**Fix:** Reduce the SOA minimum TTL for the internal zone to 30–60 seconds. For the immediate incident, flush the VPC resolver cache (in AWS Route 53, this is done via the console or CLI: `aws route53 test-dns-answer` does not flush caches, but restarting the resolver on affected nodes does). Set `networkaddress.cache.negative.ttl=5` in JVMs to limit negative caching at the application level.

---

## 9. Performance and Tuning

### DNS Query Latency Targets

DNS should be fast. These are reasonable production targets:

| Scenario | Expected Latency | Alert Threshold |
|----------|-----------------|-----------------|
| In-cluster (CoreDNS, same node cache) | < 0.5 ms | > 5 ms |
| In-cluster (CoreDNS, cross-node) | 1–3 ms | > 10 ms |
| VPC DNS resolver (cache hit) | < 1 ms | > 5 ms |
| VPC DNS resolver (cache miss) | 5–20 ms | > 50 ms |
| External (public DNS, cache hit) | 1–5 ms | > 20 ms |
| External (public DNS, full recursion) | 20–100 ms | > 200 ms |

A DNS query that takes > 100 ms is in trouble. If `getaddrinfo` blocks for multiple seconds, it will cause connection timeouts that look like service outages.

### Kubernetes NodeLocal DNSCache

NodeLocal DNSCache runs a lightweight `node-local-dns` DaemonSet that caches DNS at the node level, dramatically reducing CoreDNS load in large clusters:

```yaml
# node-local-dns runs on every node; pods query 169.254.20.10 (link-local)
# instead of the CoreDNS ClusterIP
# Reduces CoreDNS query volume by 80-90% in large clusters
```

The NodeLocal cache serves cache hits locally (sub-millisecond) and only forwards cache misses to CoreDNS, which removes CoreDNS from the critical path for the vast majority of queries.

### Tuning CoreDNS for Data Engineering Workloads

```
# Corefile additions for data-intensive clusters

.:53 {
    errors
    health
    ready
    kubernetes cluster.local in-addr.arpa ip6.arpa {
        pods insecure
        fallthrough in-addr.arpa ip6.arpa
        ttl 30        # cache TTL for Kubernetes service records
    }
    prometheus :9153    # metrics endpoint
    forward . /etc/resolv.conf {
        max_concurrent 1000  # limit upstream forwarding concurrency
    }
    cache 30 {           # cache external DNS lookups for 30 seconds
        success 9984
        denial 9984
    }
    loop
    reload
    loadbalance round_robin  # randomize response order for basic load balancing
}
```

### Recommended TTL Values for Data Engineering

| DNS Record | Recommended TTL | Reasoning |
|------------|----------------|-----------|
| Stable DB host (RDS primary) | 300s | Changes rarely; some cache warming acceptable |
| Kafka broker hostname | 60s | May change on broker replacement |
| Kubernetes ClusterIP service | 5–30s | Changes when service is recreated |
| StatefulSet pod DNS | 5–10s | Changes when pod restarts |
| Load balancer DNS | 60s | Changes on LB replacement; lower = faster failover |
| Any endpoint behind auto-healing | 30–60s | Must update faster than auto-healing cycles |

---

## 10. Interview Q&A

**Q1: A Kafka producer is failing with `UnknownHostException: kafka-broker-1.prod.internal`. The broker is running and healthy. What are the possible causes and how do you diagnose them?**

`UnknownHostException` means the OS resolver returned an error for that hostname — either `NXDOMAIN` (no such record) or `SERVFAIL` (resolver itself failed). The hostname not resolving while the service runs is a DNS-layer problem, not a Kafka problem. There are five plausible causes.

First, the DNS record genuinely does not exist — either it was never created or was deleted. Verify with `dig kafka-broker-1.prod.internal A` and `dig @<authoritative-ns> kafka-broker-1.prod.internal A`. If the authoritative nameserver returns NXDOMAIN but the VPC resolver doesn't, the record exists but hasn't propagated. If both return NXDOMAIN, the record needs to be created.

Second, the DNS record exists but is in the wrong zone or the wrong split-horizon scope. The producer may be querying from a VPC or subnet where the private zone is not visible. Verify that `/etc/resolv.conf` on the producer host points to a resolver that has access to the private zone.

Third, the resolver specified in `/etc/resolv.conf` is unreachable or overloaded. Query it directly: `dig @<nameserver-ip> kafka-broker-1.prod.internal A`. If this times out, the resolver is the problem, not the DNS record.

Fourth, negative DNS caching. If the record was recently absent (during a deployment or outage), the NXDOMAIN response may be cached. Check the SOA minimum TTL: `dig prod.internal SOA`. If it's 600 seconds, the producer will continue getting NXDOMAIN for up to 10 minutes after the record is restored. In the JVM, `networkaddress.cache.negative.ttl` may extend this further.

Fifth, on Kubernetes, the CoreDNS pod may be overloaded or down. Check `kubectl -n kube-system get pods -l k8s-app=kube-dns` and the CoreDNS logs.

**Q2: Explain how DNS-based service discovery works in Kubernetes for a Kafka StatefulSet. What DNS records exist, what are their TTLs, and what happens to a Kafka consumer when a Kafka pod restarts?**

A Kafka StatefulSet in Kubernetes creates a predictable set of DNS records through the interplay of the StatefulSet, a headless Service, and CoreDNS.

The headless Service (with `clusterIP: None`) named `kafka` in namespace `kafka-prod` creates a top-level A record: `kafka.kafka-prod.svc.cluster.local` that returns all pod IPs when queried. Each pod in the StatefulSet — `kafka-0`, `kafka-1`, `kafka-2` — also gets an individual stable DNS record: `kafka-0.kafka.kafka-prod.svc.cluster.local` pointing to that specific pod's IP. These per-pod records are what makes StatefulSets different from regular Deployments: the DNS name is stable across pod restarts because the ordinal (`0`, `1`, `2`) is stable even when the pod is replaced.

The TTL for Kubernetes service records is typically 5–30 seconds depending on CoreDNS configuration (default is the `ttl` setting in the Kubernetes plugin of the Corefile).

When a `kafka-1` pod restarts — say due to an OOM kill — the sequence is: the old pod is terminated and its IP (`10.0.1.6`) is released. A new `kafka-1` pod starts and gets a new IP (`10.0.1.91`). CoreDNS updates the A record for `kafka-1.kafka.kafka-prod.svc.cluster.local` to `10.0.1.91`. Within one TTL period (say 10 seconds), resolvers will get the new IP when they query.

For a Kafka consumer, the impact depends on the consumer's connection state. If the consumer had an active connection to the old `10.0.1.6`, that TCP connection will be broken (the pod is gone). The consumer's Kafka client detects the broken connection and attempts to reconnect. The reconnect calls `getaddrinfo("kafka-1.kafka.kafka-prod.svc.cluster.local")`, which (after the TTL expiry) returns `10.0.1.91`. The consumer reconnects successfully. The key risk is the JVM DNS cache: if `networkaddress.cache.ttl=-1`, the JVM will not re-resolve and will continue connecting to the dead IP on retry, potentially looping in reconnect failures until the JVM cache is explicitly cleared (which happens on connection exception in Java 8+ with the `InetAddress` invalidation behavior, but this is version-dependent).

**Q3: What is the difference between a ClusterIP service and a headless service in Kubernetes, and when would you use each for a Kafka deployment?**

A ClusterIP service creates a stable virtual IP (the ClusterIP) in Kubernetes' virtual service network. `kube-proxy` on each node installs iptables or IPVS rules that distribute TCP connections to the ClusterIP across all healthy backend pods. DNS for a ClusterIP service returns a single IP — the ClusterIP. The load balancing is done at the Linux kernel level as each connection is initiated.

A headless service sets `clusterIP: None`. No ClusterIP is allocated; no iptables rules are installed. DNS for a headless service returns all pod IPs directly, in round-robin order. The client receives multiple A records and is responsible for connection management — choosing which pod to connect to, handling pod failures, and distributing load.

For a Kafka deployment, headless services are the correct choice, and the reason is specific to how Kafka's protocol works. Kafka clients do not use load balancing in the traditional sense: a producer sends partition 3's data to partition 3's leader, period. The client discovers which broker hosts which partition's leader through the metadata API, and then connects directly to that broker. If a ClusterIP service were used, the client's connection to the ClusterIP might be routed to any broker — it cannot control which broker it reaches. Kafka's internal discovery mechanism — where it fetches partition metadata and then connects directly to the leader broker — requires direct, stable connections to individual brokers.

Headless services with StatefulSet DNS give each broker (`kafka-0`, `kafka-1`, `kafka-2`) a stable, directly routable DNS name. The `bootstrap.servers` config points to these specific DNS names, the Kafka client connects to each, fetches metadata, and then maintains direct connections to partition leaders. This is why Kafka's Kubernetes documentation always uses headless services and StatefulSets together — they are designed for this direct-connection pattern.

---

## 11. Cross-Question Chain

**Interviewer:** Your Kubernetes Spark job is failing with intermittent `java.net.UnknownHostException` for the Postgres metadata database. The job fails 1 in 10 runs. How do you approach this?

**Candidate:** Intermittent `UnknownHostException` 1-in-10 is a soft DNS failure — the record exists most of the time but occasionally doesn't resolve. The most common causes in Kubernetes are CoreDNS overload (queries timing out during job startup bursts), negative cache serving a stale NXDOMAIN, or the resolver being momentarily unreachable. I'd start by adding DNS resolution timing to the job startup: before connecting to Postgres, explicitly time `InetAddress.getAllByName("postgres.prod.svc.cluster.local")` and log the result and duration. This converts the intermittent exception into a measurable metric. Then I'd check CoreDNS CPU and query rate during failed job runs using Prometheus metrics.

**Interviewer:** You add logging and discover the resolution succeeds but takes 350–400 ms on the failed runs — the connection timeout is 500 ms. So it's not NXDOMAIN, it's slow resolution. What's causing 350 ms DNS lookups in Kubernetes?

**Candidate:** 350 ms is a very specific number — it's suspiciously close to the default `timeout:5 attempts:2` resolver setting but with a single timeout. The most likely cause is search domain expansion with `ndots:5`. The resolver is trying the hostname with each search domain in sequence before finding the correct one. If `postgres.prod.svc.cluster.local` is the right answer but the resolver first tries `postgres.spark-jobs.svc.cluster.local` (NXDOMAIN, waits for response), then `postgres.svc.cluster.local` (NXDOMAIN), then `postgres.cluster.local` (NXDOMAIN), and finally `postgres.prod.svc.cluster.local` (success) — each round trip to CoreDNS at 2–5ms plus the CoreDNS response time adds up. But 350ms is too slow for that scenario. More likely: one of the intermediate NXDOMAIN queries is timing out (CoreDNS under load, not responding within the `timeout:2` window), causing a 2-second wait, which somehow manifests as 350ms. I'd check `options timeout:2` in the pod's resolv.conf and whether the first attempted name is hitting a slow external resolver.

**Interviewer:** The resolv.conf shows `options ndots:5 timeout:2 attempts:2`. CoreDNS is healthy, CPU at 15%. But you notice something: `postgres` has exactly 1 dot (`postgres.prod`). How many DNS queries does this trigger, and can you trace them?

**Candidate:** `postgres.prod` has 1 dot, which is less than `ndots:5`. So the resolver will *not* try it as an absolute name first — it will prepend each search domain before trying the bare name. With a typical Kubernetes search list of four domains — `spark-jobs.svc.cluster.local`, `svc.cluster.local`, `cluster.local`, and possibly the node's DNS domain — the query sequence is: `postgres.prod.spark-jobs.svc.cluster.local` (NXDOMAIN), `postgres.prod.svc.cluster.local` (NXDOMAIN), `postgres.prod.cluster.local` (NXDOMAIN), `postgres.prod.<node-domain>` (NXDOMAIN), and finally `postgres.prod` bare (NXDOMAIN or success if it's the right name). Each NXDOMAIN is a round trip to CoreDNS. The correct FQDN is actually `postgres.prod.svc.cluster.local` — but this has 4 dots, still below `ndots:5`, so it also gets search domains prepended before being tried bare. The fix is to use the full FQDN with a trailing dot in the JDBC URL: `postgres.prod.svc.cluster.local.` — the trailing dot signals an absolute name, skipping all search domain expansion entirely.

**Interviewer:** You add the trailing dot to the JDBC URL. The intermittent failures stop. But now you want to prevent this class of problem across 50 Spark jobs. What's your systematic fix?

**Candidate:** Three complementary mitigations for the whole cluster. First, deploy NodeLocal DNSCache — a DaemonSet that runs a caching resolver on each node at `169.254.20.10`. This caches positive and negative responses locally, so repeated search domain queries (which will always return NXDOMAIN) are served from cache after the first miss, with sub-millisecond latency instead of round-tripping to CoreDNS. This alone reduces CoreDNS query volume by 80–90% in large clusters and eliminates timeout-induced slow resolution.

Second, set `ndots:2` instead of `ndots:5` in a custom CoreDNS config for the Spark namespace. `ndots:2` means any name with 2 or more dots (like `postgres.prod.svc.cluster.local`) is tried as absolute first, bypassing search domain expansion. Most service FQDNs have 4+ dots so they resolve in one query. Short names with fewer dots still expand, but those are rarely used in job configs.

Third, enforce FQDNs in the Spark job infrastructure layer: the Helm chart or job template that renders the JDBC URL should always emit the full `service.namespace.svc.cluster.local.` form with trailing dot. This makes the behavior deterministic regardless of `ndots` setting.

**Interviewer:** Now a different angle. You're migrating the Postgres metadata database from an EC2 instance (IP 10.0.1.50) to RDS (IP 10.0.2.20). Both are in the same VPC. The DNS record `postgres.prod.internal` currently points to 10.0.1.50, TTL 3600. Describe exactly how you would execute this migration with zero downtime.

**Candidate:** The migration has three phases separated by mandatory wait periods. Phase one starts 48 hours before cutover: I lower the TTL from 3600 to 60 seconds on `postgres.prod.internal`. This creates a 3600-second window where resolvers will pick up the new TTL=60 record as their old caches expire. I must wait the full 3600 seconds (the old TTL) before proceeding, to guarantee that all resolvers — including any with fresh caches — are now operating with TTL=60 and will see changes within 60 seconds.

Phase two is the actual cutover. I update the A record from `10.0.1.50` to the RDS endpoint's IP (or better, use a CNAME to the RDS endpoint's DNS name, since RDS IPs can change under AWS maintenance). Within 60 seconds, all resolvers that query after the change will get the RDS IP. I monitor the EC2 Postgres for new connections dropping to zero (indicating all clients have transitioned) while simultaneously watching the RDS Postgres for incoming connections rising.

A critical additional step for JVM-based clients — Airflow metadata DB connections, dbt, any JDBC clients — is checking `sun.net.inetaddr.ttl`. If it's `-1` (cache forever), those JVMs will continue connecting to `10.0.1.50` until their connection fails and forces a re-resolution. I'd either pre-set `sun.net.inetaddr.ttl=60` during the migration window (requiring restarts) or accept that those clients will reconnect naturally once the old connection drops. The RDS has a separate endpoint DNS name anyway, so I can test the RDS before flipping the shared `postgres.prod.internal` record.

Phase three: after confirming traffic has fully migrated, restore the TTL to 300 seconds to reduce DNS query load.

**Interviewer:** The migration succeeds. But 6 hours later, you get a page: Airflow's metadata database connections are intermittently failing with `Connection refused`. The RDS is healthy. DNS resolves correctly to the RDS IP. What do you investigate?

**Candidate:** `Connection refused` after a successful migration with correct DNS points to a connection pool or networking issue, not DNS or application logic. `Connection refused` specifically (not timeout) means the destination is actively rejecting the TCP SYN with a RST — the host is reachable but the port is not listening or has rejected the connection. Three possibilities in this post-migration scenario: the Airflow connection pool has stale connections that were established to the EC2 Postgres (now decommissioned) and are returning RST when the pool tries to reuse them — but this would manifest as errors, not `Connection refused` to the RDS IP. More likely: the RDS security group or VPC security group has not been updated to allow inbound on port 5432 from all Airflow worker subnets. The old EC2 had broader security group rules; the new RDS has the default restrictive rules. I'd verify with `telnet <rds-ip> 5432` from an Airflow worker to confirm RST vs timeout. `Connection refused` = RST = security group drop. Fix: add an inbound rule to the RDS security group allowing TCP 5432 from the Airflow worker subnet CIDR.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is a DNS A record? | Maps a hostname to an IPv4 address. The most common record type. |
| 2 | What is a DNS CNAME record? | Maps a hostname to another hostname. Resolution continues by resolving the CNAME target. Adds one extra DNS lookup per hop. |
| 3 | What is DNS TTL? | Time To Live — how long a resolver may cache the record before re-querying. Controls propagation speed vs DNS load trade-off. |
| 4 | What is negative DNS caching? | Caching of NXDOMAIN (not found) responses. Duration set by SOA minimum TTL. Can extend downtime after a record is restored. |
| 5 | Where does a Linux process look first when resolving a hostname? | `/etc/hosts`, then the DNS resolver specified in `/etc/resolv.conf`, as ordered by `/etc/nsswitch.conf`. |
| 6 | What does `ndots:5` mean in `/etc/resolv.conf`? | Names with fewer than 5 dots are tried with each search domain appended before being tried bare. Kubernetes default. Causes query multiplication. |
| 7 | What is the JVM default for `networkaddress.cache.ttl`? | -1 in many distributions — caches DNS forever until JVM restart. Dangerous for infrastructure that changes IPs. Set to 60 for Kafka/Spark. |
| 8 | What is a headless Kubernetes service? | A service with `clusterIP: None`. DNS returns all pod IPs directly (no ClusterIP). Used for stateful workloads like Kafka that need direct pod connections. |
| 9 | Why does Kafka use headless services + StatefulSets in Kubernetes? | Kafka clients must connect directly to specific brokers (partition leaders). A ClusterIP load balancer would route to the wrong broker. Direct pod DNS names are required. |
| 10 | What DNS record does a Kubernetes StatefulSet pod get? | `<pod-name>.<service-name>.<namespace>.svc.<cluster-domain>`. E.g. `kafka-0.kafka.kafka-prod.svc.cluster.local`. Stable across pod restarts. |
| 11 | What is split-horizon DNS? | Different DNS answers for the same name depending on who's asking. Internal clients get private IPs; external clients get public IPs or NXDOMAIN. |
| 12 | Why pre-reduce TTL before a DNS migration? | New TTL takes effect after old TTL expires. Reducing to 60s guarantees changes propagate within 60s after cutover. Without pre-reduction, old IPs persist for hours. |
| 13 | What is a DNS SRV record? | Maps a service name to a hostname and port. Used by service discovery systems (Consul, K8s headless services) to advertise both address and port. |
| 14 | What does a trailing dot in a DNS name do? | Marks the name as fully qualified (absolute). The resolver does not append search domains, sending exactly one query. Eliminates ndots-related query multiplication. |
| 15 | What is NodeLocal DNSCache in Kubernetes? | A DaemonSet that runs a caching DNS resolver on each node. Reduces CoreDNS load by 80–90% by serving repeated queries from local cache. |
| 16 | What command shows the TTL remaining in a cached DNS response? | `dig <hostname> A` — the number in the answer section before `IN A` is the remaining TTL in seconds. |
| 17 | How does `dig +trace` differ from plain `dig`? | `+trace` performs a full iterative resolution from the root, showing each delegation step. Plain `dig` uses the configured recursive resolver (uses its cache). |
| 18 | What is the SOA minimum TTL used for? | Controls negative cache TTL — how long resolvers cache NXDOMAIN responses. High SOA minimum = extended outage after a record is restored. |
| 19 | Why is `dig @<authoritative-ns> <name>` useful during a migration? | It queries the authoritative server directly, bypassing all caches. Shows the current authoritative record before it has propagated to recursive resolvers. |
| 20 | What is the difference between SERVFAIL and NXDOMAIN? | NXDOMAIN: the name definitively does not exist (authoritative answer). SERVFAIL: the resolver encountered an error (could not reach authoritative server, internal error). SERVFAIL may be transient; NXDOMAIN is definitive. |

---

## 13. Further Reading

- **"DNS and BIND" — Cricket Liu & Paul Albitz (O'Reilly):** The canonical reference for DNS administration. Chapters 2–4 cover the protocol, resolution, and record types comprehensively. Worth reading chapters 2 and 10 (troubleshooting) before your next DNS incident.
- **RFC 1034 and RFC 1035:** The original DNS specifications. RFC 1034 defines the conceptual model; RFC 1035 defines the wire format and record types. Dry but precise — worth skimming section 3 of RFC 1034 to understand the zone/delegation model from first principles.
- **RFC 2308 — Negative Caching of DNS Queries:** Defines how NXDOMAIN responses should be cached and how the SOA minimum TTL governs negative cache lifetime. Essential reading if you've ever been confused about why DNS failures persist after the record is fixed.
- **Kubernetes CoreDNS documentation:** `https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/` — definitive reference for Kubernetes DNS record format and behavior.
- **NodeLocal DNSCache documentation:** `https://kubernetes.io/docs/tasks/administer-cluster/nodelocal-dnscache/` — setup guide and performance analysis for the node-level DNS cache.
- **Amazon Web Services: DNS Resolution for VPCs:** AWS documentation on Route 53 Resolver, private hosted zones, and the `.2` resolver address. Critical for engineers building on AWS.
- **Google's resolv.conf and DNS timeout analysis** (internal Google SRE blog posts, various): Google has written extensively about DNS timeouts in Kubernetes at scale and the evolution toward NodeLocal DNSCache. The public-facing version appears in the Kubernetes Enhancement Proposals (KEPs) for NodeLocal DNSCache.

---

## 14. Lab Exercises

**Exercise 1: Trace Search Domain Expansion**

On a machine with a multi-domain search list (or in a Kubernetes pod with `ndots:5`), run `strace -e trace=network getent hosts kafka 2>&1 | grep sendto`. Count how many DNS queries are sent for the short name `kafka`. Then run `getent hosts kafka.kafka-prod.svc.cluster.local.` (with trailing dot). Count again. Observe the query reduction.

**Exercise 2: Measure DNS Resolution Latency Distribution**

Write a Python script that resolves a hostname 100 times in a loop and records each resolution time. Plot the distribution. Identify the warm-cache (sub-millisecond) vs cold-cache (5–50 ms) bimodal distribution. Now add `time.sleep(TTL + 1)` between calls to force cache expiry and observe the cold-cache latency.

**Exercise 3: Simulate TTL Propagation**

Set up a local DNS server (dnsmasq or CoreDNS in Docker) with a record pointing to IP A with TTL 10. Start a client polling resolution every 2 seconds. Change the record to IP B. Observe how quickly the client sees the new IP — it should be within 10 seconds. Now set TTL 60 and repeat. Observe the 60-second delay in propagation.

**Exercise 4: Diagnose JVM DNS Caching**

Write a Java or Kotlin program that resolves a hostname in a loop. After 30 seconds, change the IP in your local DNS. Observe whether the Java program picks up the new IP (it should not, with default `cache.ttl=-1`). Add `-Dsun.net.inetaddr.ttl=5` and repeat. Verify that the Java program now picks up the new IP within 5 seconds.

**Exercise 5: Kubernetes DNS Debugging**

Deploy a Kafka StatefulSet with a headless service in a Kubernetes cluster. From a test pod in the same namespace, query: the headless service DNS (all pod IPs), each individual pod DNS, and a non-existent service (observe NXDOMAIN). Check CoreDNS logs during the queries. Identify the TTL of the pod DNS records.

---

## 15. Key Takeaways

DNS is the first step of every network connection. Before TCP can start, before TLS can negotiate, before Kafka can send a message — a hostname must resolve to an IP. DNS failures are invisible unless you look for them: they manifest as `UnknownHostException` (misread as service outage), as slow connection startup (misread as service slowness), and as post-migration stalls (misread as deployment failure).

The three most important DNS behaviors for data engineers to internalize: **TTL governs propagation time** — a high-TTL record change will not be seen by clients for hours; always pre-reduce TTL before planned IP changes. **JVM DNS cache is separate from the OS cache** — with `networkaddress.cache.ttl=-1`, a Kafka client JVM will never re-resolve a hostname even if the underlying IP changes; set this to 60 on all JVM-based data services. **Kubernetes `ndots:5` multiplies DNS queries** — a single short hostname generates 5–6 DNS queries, which at scale overwhelms CoreDNS; use FQDNs with trailing dots or deploy NodeLocal DNSCache.

The diagnostic tools are simple: `dig +trace` for full resolution visibility, `dig @<ns>` for authoritative source queries, `cat /etc/resolv.conf` for resolver config, and `kubectl -n kube-system logs -l k8s-app=kube-dns` for Kubernetes DNS errors.

---

## 16. Connections to Other Modules

- **M27 — TCP/IP Fundamentals:** DNS resolution is the prerequisite for TCP connection establishment. The TCP handshake cannot begin until `getaddrinfo` returns an IP. DNS latency adds directly to connection startup time.
- **M29 — TLS and mTLS:** TLS certificates are issued to hostnames (Subject Alternative Names). DNS-based service discovery and TLS certificate validation are tightly coupled — a DNS name change can invalidate an existing certificate.
- **M30 — Network Latency:** DNS propagation delay is a form of latency that affects failover speed. High TTLs cause long DNS propagation delays that extend the recovery time from infrastructure failures.
- **M31 — Load Balancing and Proxies:** DNS-based load balancing (returning multiple A records) is the simplest form of load distribution. Kubernetes ClusterIP services combine DNS with kube-proxy for connection-level balancing.
- **DCS-KFK-101 — Apache Kafka Internals:** Kafka's `bootstrap.servers` configuration uses DNS names. Kafka clients resolve these names on startup and on reconnect. StatefulSet DNS names are the correct approach for Kubernetes Kafka deployments.
- **SYS-NET-102 — Cloud Networking and Security:** Cloud DNS (Route 53, Cloud DNS, Azure DNS) uses the same protocol but has cloud-specific semantics: private hosted zones, health-check-based routing (Route 53), DNSSEC, and resolver endpoints.

---

## 17. Anti-Patterns

**Anti-pattern: Using `hostAliases` in Kubernetes pods instead of DNS records.** `hostAliases` adds entries to the pod's `/etc/hosts`. This bypasses DNS entirely — updates require pod restart, it doesn't support TTL, and it creates configuration drift (different pods have different hostfiles). Always use DNS; only use `hostAliases` for emergency workarounds.

**Anti-pattern: Setting DNS TTL to 0 for all records.** TTL=0 disables all caching. Every DNS-dependent operation requires a fresh resolver query. Under load, this creates a thundering-herd problem: all pods in a Spark job simultaneously query DNS on startup, overwhelming the resolver. Use low but non-zero TTLs (30–60 seconds) instead.

**Anti-pattern: Using `dnsPolicy: None` in Kubernetes pods without understanding the implications.** `dnsPolicy: None` disables all Kubernetes DNS injection. The pod must specify its own `dnsConfig`. Missing this configuration means service DNS names (`kafka.kafka-prod.svc.cluster.local`) won't resolve. Only use `dnsPolicy: None` when you have a specific need for custom DNS configuration and understand exactly what you're replacing.

**Anti-pattern: Ignoring DNS in load testing.** Load tests that repeatedly connect to the same services often get the benefit of DNS caching and JVM DNS cache. Production traffic that creates many new connections (new consumers, executor startups) may experience DNS resolution time that load tests never measured. Include DNS resolution time in load test metrics.

**Anti-pattern: Using AWS-resolved private hostnames as permanent service addresses.** AWS gives each EC2 instance a private hostname like `ip-10-0-1-47.us-east-1.compute.internal`. Using this as a service address means the address changes every time the instance is replaced. Register services under meaningful DNS names (Route 53 private hosted zone) and use those, never the AWS-assigned instance hostname.

---

## 18. Tools Reference

| Tool | Purpose | Key Flags |
|------|---------|-----------|
| `dig <host> A` | DNS lookup with TTL visible | `+trace` full recursion, `@<ns>` specific nameserver |
| `dig +trace` | Full iterative resolution from root | Shows all delegation steps |
| `dig +stats` | Include query time in output | Combined with `+answer` |
| `nslookup <host>` | Simple hostname lookup | `-type=SRV` for service records |
| `host <host>` | Simple DNS lookup | `-a` all record types |
| `getent hosts <host>` | OS resolver lookup (incl. /etc/hosts) | Respects nsswitch.conf order |
| `strace -e trace=network getent hosts <h>` | Trace DNS system calls | Shows socket calls and timing |
| `cat /etc/resolv.conf` | View resolver config | Check nameservers and search domains |
| `cat /etc/hosts` | View static hostname mappings | — |
| `cat /etc/nsswitch.conf` | View name resolution order | Look for `hosts: files dns` |
| `resolvectl status` | systemd-resolved status | Shows per-interface DNS settings |
| `systemd-resolve --statistics` | DNS cache statistics | Cache hit rate, query count |
| `kubectl exec <pod> -- cat /etc/resolv.conf` | Pod resolver config | Verify CoreDNS IP and search domains |
| `kubectl -n kube-system logs -l k8s-app=kube-dns` | CoreDNS logs | SERVFAIL, NXDOMAIN errors |
| `dig -x <ip>` | Reverse DNS lookup (PTR) | — |

---

## 19. Glossary

**A Record:** DNS record mapping a hostname to an IPv4 address. The most common record type.

**AAAA Record:** DNS record mapping a hostname to an IPv6 address.

**Authoritative Nameserver:** The DNS server that holds the definitive records for a zone. Its answers are not cached from elsewhere; it is the source of truth.

**CNAME (Canonical Name):** DNS record that maps a hostname to another hostname. The resolution process follows the chain until an A/AAAA record is found.

**ClusterIP:** A stable virtual IP assigned to a Kubernetes Service. Traffic to the ClusterIP is load-balanced to healthy pod endpoints by kube-proxy.

**CoreDNS:** The default DNS server in Kubernetes, running as a Deployment in kube-system. Handles service DNS resolution, external forwarding, and custom zone configuration.

**FQDN (Fully Qualified Domain Name):** A hostname that specifies its complete position in the DNS hierarchy, typically ending with a dot (e.g., `kafka.prod.internal.`).

**Headless Service:** A Kubernetes Service with `clusterIP: None`. DNS returns all pod IPs directly, enabling client-side load balancing and direct pod addressing.

**`ndots`:** A resolver option controlling search domain expansion. Names with fewer than `ndots` dots are tried with search domains appended first.

**Negative Caching:** Caching of NXDOMAIN (name not found) responses. Controlled by the SOA minimum TTL. Causes post-outage DNS delays.

**NodeLocal DNSCache:** A Kubernetes DaemonSet that runs a caching DNS resolver on each node, reducing CoreDNS load by serving repeated queries locally.

**NS Record:** DNS record listing the authoritative nameservers for a zone.

**NXDOMAIN:** DNS response code indicating the queried name does not exist. Authoritative answer.

**PTR Record:** DNS record for reverse lookup — maps an IP address to a hostname.

**Recursive Resolver:** A DNS server that performs the full resolution process on behalf of clients, querying root servers, TLD servers, and authoritative servers as needed, then caching results.

**SERVFAIL:** DNS response code indicating the server encountered an error while processing the query. May be transient; does not mean the record does not exist.

**SOA (Start of Authority):** DNS record describing a zone's authoritative server, admin contact, and timing parameters including negative cache TTL.

**Split-Horizon DNS:** Returning different DNS answers for the same name based on the query source. Private IPs for internal clients; public IPs for external clients.

**SRV Record:** DNS record mapping a service name to a hostname and port, enabling service discovery with port information.

**StatefulSet:** A Kubernetes workload type that gives each pod a stable name and DNS entry, persisting across pod restarts. Used for stateful services like Kafka.

**TTL (Time To Live):** The number of seconds a DNS record may be cached by a resolver before being re-queried.

**Zone:** A portion of the DNS namespace managed by a specific organization and served by specific authoritative nameservers.

---

## 20. Self-Assessment

1. Trace the full DNS resolution path for `kafka.prod.svc.cluster.local` from a Kubernetes pod, starting from the application's `getaddrinfo` call and ending with the TCP connection being established.
2. A Kafka broker is replaced and gets a new IP. The DNS TTL is 60 seconds. JVM `networkaddress.cache.ttl` is `-1`. Describe exactly what happens to: (a) a consumer that has an active connection, (b) a consumer that is restarting, (c) a new consumer starting for the first time.
3. What is `ndots:5` and why does it matter in a cluster where Spark executors start by resolving 10 hostnames? How many total DNS queries are generated on startup with `ndots:5` and a 5-domain search list?
4. Describe the correct procedure to migrate a Postgres database from IP A to IP B with zero downtime, assuming clients include both JVM applications and Python applications.
5. What is the difference between a CNAME and an A record? When would you use a CNAME for a Kafka broker address? What are the drawbacks?
6. You see `SERVFAIL` in CoreDNS logs for `postgres.prod.svc.cluster.local`. The Postgres pod is running and healthy. What are three possible causes?
7. Why does a Kafka deployment in Kubernetes need a headless service rather than a regular ClusterIP service?
8. What is split-horizon DNS and how would it cause a developer on their laptop to fail to connect to a Kafka cluster that is accessible from within the Kubernetes cluster?
9. A DNS record change from IP A to IP B was made, but some clients are still connecting to IP A two hours later. What are two distinct root causes (one OS-level, one JVM-level) and how would you diagnose each?
10. What is negative DNS caching, which DNS record controls its duration, and why does it matter for production incident response?

---

## 21. Module Summary

DNS is the invisible infrastructure beneath every distributed data system. Before TCP can connect, before Kafka can produce, before Spark can shuffle — a name must be resolved to an IP address. This module covered the full DNS resolution path (from `/etc/hosts` through the recursive resolver to the authoritative nameserver), the five main DNS record types (A, AAAA, CNAME, SRV, PTR, SOA), the mechanics of TTL-based caching and negative caching, and the JVM's separate DNS cache that operates independently of the OS resolver.

For Kubernetes — the dominant runtime for cloud-native data engineering — DNS is the primary service discovery mechanism. CoreDNS serves cluster-internal names; ClusterIP services provide stable virtual IPs for stateless services; headless services expose pod IPs directly for stateful workloads like Kafka. The `ndots:5` default causes significant query amplification in large clusters, and NodeLocal DNSCache is the production solution for high-density Kubernetes environments.

The four most dangerous DNS misconfigurations in production data engineering: hardcoded IPs instead of DNS names (breaks on every infrastructure change), JVM `networkaddress.cache.ttl=-1` (stale IPs after broker replacement), high TTL on infrastructure that changes IPs (long propagation window during migrations), and `ndots:5` without FQDN discipline (DNS query storms on job startup).

The diagnostic toolkit is simple but powerful: `dig +trace` for full resolution visibility, `dig @<nameserver>` to query specific resolvers, `cat /etc/resolv.conf` to understand the resolver chain, and `kubectl -n kube-system logs -l k8s-app=kube-dns` for Kubernetes DNS errors.

The next module — M29: TLS and mTLS — builds directly on this foundation. TLS certificates are bound to DNS names; a TLS handshake cannot succeed if the DNS name does not match the certificate's Subject Alternative Names. Understanding DNS is a prerequisite for understanding how certificate validation works in a Kafka cluster with mutual TLS authentication.
