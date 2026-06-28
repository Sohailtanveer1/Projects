# M34: Security Groups and Network Policies

**Course:** SYS-NET-102 — Cloud Networking and Security  
**Module:** 03 of 05  
**Global Module ID:** M34  
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

IAM (M33) controls what an authenticated identity is authorized to do via API calls. Security groups and network policies control what network traffic can even reach a service — before any authentication occurs. They are complementary, not redundant: IAM is the authorization layer; security groups are the network firewall layer. A misconfigured security group can block legitimate traffic completely (causing `Connection refused` or `Connection timed out`), allow unauthorized network access to services that rely on network-level trust, or create debugging nightmares where everything looks correct at the IAM layer but traffic simply never arrives.

Data engineers encounter security groups in three critical recurring scenarios:

**Scenario 1: New Kafka consumer can't connect.** A Spark job runs in a new subnet and cannot reach the Kafka brokers on port 9092. The Kafka security group allows inbound 9092 from the old Spark subnet (`10.0.10.0/24`) but not from the new one (`10.0.20.0/24`). The connection times out — not a Kafka configuration problem, not an IAM problem. Pure security group misconfiguration.

**Scenario 2: Airflow metadata database connection pool exhausted.** A new Airflow deployment was configured to use 200 parallel worker connections to Postgres. The Postgres security group allows inbound 5432 from the Airflow security group — so far correct — but Postgres's `max_connections=100` is exceeded before security groups become relevant. However, when the same team also fails to add the new Airflow worker subnet to the Postgres security group, connections from new workers are silently dropped. The two problems (security group and max_connections) present identically at the surface: `could not connect to server`. Understanding which layer is blocking requires methodical diagnosis.

**Scenario 3: Kubernetes network policies blocking internal service communication.** A data pipeline running in Kubernetes uses network policies to isolate namespaces. After a refactor that moved the schema registry client into a new namespace, schema validation calls start failing. The new namespace is not in the schema registry service's network policy `ingress.from` rule, so packets are dropped at the CNI level — before they reach the schema registry pod.

Security groups and network policies are the network-level controls that complete the defense-in-depth model: VPC subnets provide coarse segmentation (public vs private), security groups provide host-level inbound/outbound filtering within the VPC, and Kubernetes network policies provide pod-level isolation within a cluster. Understanding how each layer works — and how to debug each layer independently — is a production engineering requirement.

---

## 2. Mental Model

### Security Groups: Stateful Packet Filters

An AWS security group is a stateful firewall associated with an EC2 instance (or ENI — Elastic Network Interface). It evaluates every incoming and outgoing TCP/UDP/ICMP packet and permits or denies it based on rules.

**Stateful** means: if an outbound connection is permitted by an outbound rule, the return traffic (the response) is automatically allowed, even if there is no matching inbound rule for it. Conversely, if an inbound connection is permitted by an inbound rule, the response traffic is automatically allowed outbound. You never need to add bidirectional rules for a single connection — just the direction of connection initiation.

**Default behavior:**
- All inbound traffic is denied by default (no inbound rules = no inbound traffic)
- All outbound traffic is allowed by default (the default outbound rule allows `0.0.0.0/0`)

**Security group = allow-only.** Security groups have no explicit Deny rules. Every rule is an Allow. If no rule matches, traffic is denied. This is different from AWS Network ACLs (NACLs), which support explicit Deny rules and are evaluated in numerical order.

### The Target: Source or Security Group Reference

Security group rules specify what is allowed to connect. The source can be:
- An IP CIDR block: `10.0.10.0/24` — all IPs in this subnet
- Another security group ID: `sg-0123456789abcdef0` — any EC2 instance that has this security group attached

**Security group references are more powerful than CIDR references** for dynamic cloud environments. When you reference another security group (rather than a CIDR), the rule automatically applies to all instances that have that security group — even as instances are added, replaced, or scaled. You do not need to update the CIDR when a new instance launches in a different IP.

Example: Instead of allowing `10.0.10.0/24` to connect to Kafka, allow `sg-spark-workers` to connect. When a new Spark worker launches with `sg-spark-workers` attached, it automatically can reach Kafka — no security group update needed.

### Network Policies: Kubernetes-Native L3/L4 Firewall

A Kubernetes NetworkPolicy is a resource that specifies which pods can send traffic to which other pods (or to external endpoints), at the IP/port level. Network policies are implemented by the CNI (Container Network Interface) plugin — Calico, Cilium, Weave, or Amazon VPC CNI with Calico.

**Without any NetworkPolicy, all pods can reach all other pods by default** (within the cluster, pods share a flat network). NetworkPolicy is a "deny unless explicitly allowed" firewall once any policy exists for a pod — but only if the CNI plugin supports and enforces policies.

Key distinction from security groups: NetworkPolicy is a Kubernetes API resource, namespaced, and applied based on label selectors. It operates at the pod level, not the instance level. Security groups in AWS apply to EC2 instances; Kubernetes NetworkPolicies apply to pods, identified by their labels.

---

## 3. Core Concepts

### 3.1 Security Group Rule Structure

An inbound security group rule has five fields:

| Field | Example | Meaning |
|---|---|---|
| Type | Custom TCP | Protocol category (predefined or custom) |
| Protocol | TCP | TCP, UDP, ICMP, or All |
| Port Range | 9092 | Single port, range (9092-9094), or All |
| Source | sg-0abc123 or 10.0.0.0/8 | Which traffic to allow |
| Description | Kafka producers | Human-readable label (required best practice) |

An outbound rule is identical in structure, with `Destination` instead of `Source`.

**Common Kafka security group rules:**
```
Inbound:
  TCP 9092  from sg-kafka-clients      ← Kafka producer/consumer port
  TCP 9093  from sg-kafka-brokers      ← Inter-broker replication port
  TCP 9094  from sg-external-clients   ← External listener (if used)
  TCP 2181  from sg-kafka-brokers      ← ZooKeeper client (legacy)
  TCP 2888  from sg-kafka-zookeeper    ← ZooKeeper peer (legacy)
  TCP 3888  from sg-kafka-zookeeper    ← ZooKeeper leader election (legacy)
  TCP 9999  from sg-kafka-monitoring   ← JMX for metrics (Prometheus)

Outbound (usually allow all):
  All traffic  0.0.0.0/0              ← Brokers need to reach S3, updates, etc.
```

**Common Postgres/PgBouncer security group rules:**
```
Inbound:
  TCP 5432  from sg-airflow-workers    ← Airflow metadata DB
  TCP 5432  from sg-dbt-runners        ← dbt target DB
  TCP 5432  from sg-spark-workers      ← Spark JDBC connections
  TCP 6432  from sg-application-layer  ← PgBouncer (if using connection pooler)

Outbound:
  All traffic  0.0.0.0/0
```

### 3.2 Security Group vs Network ACL

AWS has two network-level controls that are often confused:

**Security Group (SG):**
- Operates at the instance/ENI level
- Stateful: return traffic is automatically allowed
- Allow-only rules (no explicit Deny)
- Multiple security groups can be attached to one instance
- Rules evaluated as a set (all rules checked; if any allows, traffic passes)

**Network Access Control List (NACL):**
- Operates at the subnet level
- Stateless: outbound rules must explicitly allow return traffic
- Supports both Allow and Deny rules
- Rules evaluated in numerical order (lowest number first); first match wins
- One NACL per subnet

**For data engineering:** Security groups are the primary tool. NACLs are used for coarse subnet-level blocking — e.g., blocking all traffic from known malicious IP ranges, or blocking all traffic between the production and development subnets at the subnet level. Most data platform security is implemented via security groups, not NACLs.

### 3.3 Security Group Best Practices for Data Platforms

**One security group per service role.** Rather than one large "data-platform" security group, create:
- `sg-kafka-brokers`: attached to Kafka broker instances
- `sg-kafka-clients`: attached to Kafka producer/consumer instances (Spark, Airflow, Connect)
- `sg-zookeeper`: attached to ZooKeeper instances (legacy)
- `sg-spark-workers`: attached to Spark worker nodes
- `sg-airflow-workers`: attached to Airflow worker instances
- `sg-postgres`: attached to Postgres instances
- `sg-schema-registry`: attached to Schema Registry instances
- `sg-bastion`: attached to the bastion/jump host

Then, rules reference security groups rather than CIDRs:
- Kafka brokers allow TCP 9092 from `sg-kafka-clients`
- Kafka brokers allow TCP 9093 from `sg-kafka-brokers` (inter-broker)
- Postgres allows TCP 5432 from `sg-airflow-workers` and `sg-dbt-runners`

**Why this matters:** When an Airflow deployment scales horizontally (new worker nodes), the new instances get `sg-airflow-workers` attached and automatically can reach Postgres — no security group rule update needed. With CIDR-based rules, every new subnet requires a rule update.

### 3.4 Kubernetes NetworkPolicy

A NetworkPolicy specifies:
- **podSelector:** Which pods this policy applies to (the "targets")
- **policyTypes:** `Ingress` (inbound), `Egress` (outbound), or both
- **ingress.from:** Which sources can send traffic to the target pods
- **egress.to:** Which destinations the target pods can send traffic to

```yaml
# Allow only Kafka clients to reach Kafka brokers on port 9092
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kafka-broker-ingress
  namespace: kafka-prod
spec:
  podSelector:
    matchLabels:
      app: kafka-broker          # Applies to pods with this label
  policyTypes:
  - Ingress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: data-pipelines
      podSelector:
        matchLabels:
          role: kafka-client     # AND: pod must have this label in that namespace
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: kafka-prod
      podSelector:
        matchLabels:
          app: kafka-broker      # Allow broker-to-broker replication
    ports:
    - port: 9092
      protocol: TCP
    - port: 9093
      protocol: TCP
```

**The AND vs OR semantics in NetworkPolicy:** Within a single `from` element, `namespaceSelector` AND `podSelector` are evaluated as AND — the source must match both. Separate elements in the `from` list are OR — traffic is allowed if it matches any element.

```yaml
# This is AND (traffic must be from namespace AND pod matching labels):
- from:
  - namespaceSelector:
      matchLabels:
        env: production
    podSelector:
      matchLabels:
        role: kafka-client

# This is OR (traffic from namespace OR from any pod with the label):
- from:
  - namespaceSelector:
      matchLabels:
        env: production
  - podSelector:
      matchLabels:
        role: kafka-client
```

This AND vs OR distinction is the most common NetworkPolicy mistake and the most common source of "why is my traffic blocked?" confusion.

### 3.5 Default Deny NetworkPolicy

The recommended Kubernetes security posture is "default deny" — a NetworkPolicy that blocks all traffic to/from pods in a namespace unless explicitly allowed:

```yaml
# Default deny all ingress in a namespace
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-ingress
  namespace: kafka-prod
spec:
  podSelector: {}          # Empty selector = applies to ALL pods in namespace
  policyTypes:
  - Ingress
  # No ingress rules = deny all ingress
---
# Default deny all egress
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-egress
  namespace: kafka-prod
spec:
  podSelector: {}
  policyTypes:
  - Egress
  # No egress rules = deny all egress
```

After applying default deny, individual NetworkPolicies are added to allow specific traffic. This is defense-in-depth: if a developer forgets to add a NetworkPolicy for a new pod, the pod cannot send or receive traffic — the failure is safe (traffic blocked, not accidentally permitted).

**Egress allowances required for DNS:** After applying default deny egress, pods cannot resolve DNS names (DNS uses UDP/TCP 53 to CoreDNS). Always add a DNS egress rule alongside default deny egress:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-dns-egress
  namespace: kafka-prod
spec:
  podSelector: {}
  policyTypes:
  - Egress
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: kube-system
    ports:
    - port: 53
      protocol: UDP
    - port: 53
      protocol: TCP
```

### 3.6 GCP Firewall Rules

GCP firewall rules function similarly to AWS security groups but have some architectural differences:

**VPC-level vs instance-level:** GCP firewall rules apply at the VPC level but are targeted at instances by network tags or service accounts. Unlike AWS SGs (which are attached to ENIs), GCP rules use metadata to identify targets.

**Network tags:** A simple string label applied to a GCP instance. Firewall rules can target instances by tag: "allow TCP 9092 from instances with tag `kafka-client` to instances with tag `kafka-broker`."

**Service account targeting:** More precise than tags — a firewall rule that only applies to instances running as a specific service account. Harder to accidentally misapply than tags.

```bash
# Create a Kafka broker ingress rule allowing only kafka-client-tagged instances
gcloud compute firewall-rules create allow-kafka-broker-ingress \
  --network=data-platform-vpc \
  --direction=INGRESS \
  --action=ALLOW \
  --rules=tcp:9092,tcp:9093 \
  --source-tags=kafka-client \
  --target-tags=kafka-broker \
  --priority=1000 \
  --description="Allow Kafka clients to reach brokers on 9092/9093"

# Allow Postgres access from Airflow workers
gcloud compute firewall-rules create allow-postgres-from-airflow \
  --network=data-platform-vpc \
  --direction=INGRESS \
  --action=ALLOW \
  --rules=tcp:5432 \
  --source-tags=airflow-worker \
  --target-tags=postgres-primary \
  --priority=1000 \
  --description="Allow Airflow workers to reach Postgres"
```

**Implied rules:** GCP VPCs have two implied rules that cannot be deleted:
- Implied deny all ingress (lowest priority: 65535)
- Implied allow all egress (lowest priority: 65535)

Higher-priority rules (lower number) override lower-priority ones. The default-deny ingress is already built in to GCP VPCs — every service must have an explicit allow rule to receive traffic.

### 3.7 Diagnosing Security Group Problems

Security group issues manifest as two different error types, which indicate different problems:

**`Connection refused` (TCP RST):** The TCP packet reached the target host, which actively rejected it. This means the security group allowed the packet through (the packet arrived at the instance), but nothing is listening on that port, OR the OS firewall (iptables, nftables) rejected it. Not a security group problem — check if the service is running and listening on the expected port.

**`Connection timed out` (no response):** The TCP SYN packet was sent but no SYN-ACK or RST came back. The packet was silently dropped. This is the signature of security group (or NACL) blocking — the packet never reached the instance (or the response never made it back). In AWS, VPC flow logs will show REJECT for packets blocked by security groups.

```bash
# Test from inside a container/instance:
# "Connection timed out" → security group blocking
# "Connection refused"   → security group OK but service not running

# Test TCP connectivity (not just ICMP ping)
nc -z -v kafka.prod.internal 9092
# Timed out → SG blocking
# Connected → SG OK (but doesn't confirm Kafka is healthy)
# Connection refused → SG OK, but port 9092 not listening

# Test with timeout to distinguish quickly
timeout 3 bash -c "echo > /dev/tcp/kafka.prod.internal/9092" && \
  echo "SG OK" || echo "SG BLOCKING or service down"

# More detailed test with curl (for HTTP services)
curl -v --max-time 5 http://schema-registry.prod.internal:8081/subjects
# * Connection timed out → SG blocking
# * Connection refused   → SG OK, service not listening
# * 200 OK               → working
```

### 3.8 Security Group Auditing with VPC Flow Logs

VPC flow logs (covered in M32) record REJECT entries for traffic blocked by security groups:

```
# Flow log entry showing REJECT:
version account-id interface-id srcaddr  dstaddr  srcport dstport proto pkts bytes start end action
2       123456789  eni-abc123   10.0.20.5 10.0.10.8 54321  9092   6     1    60    ...       REJECT

# srcaddr: 10.0.20.5 = new Spark worker in new subnet
# dstaddr: 10.0.10.8 = Kafka broker
# dstport: 9092 = Kafka port
# action: REJECT = security group blocked this

# Athena query to find recent REJECT events on port 9092:
SELECT srcaddr, dstaddr, dstport, packets, bytes, action
FROM vpc_flow_logs
WHERE action = 'REJECT'
  AND dstport = 9092
  AND start > extract(epoch from now() - interval '1 hour')
ORDER BY start DESC
LIMIT 50;
```

---

## 4. Hands-On Walkthrough

### 4.1 Auditing Kafka Security Groups

```bash
# List all security groups attached to Kafka broker instances
KAFKA_INSTANCE_IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:Role,Values=kafka-broker" \
            "Name=instance-state-name,Values=running" \
  --query 'Reservations[*].Instances[*].InstanceId' \
  --output text)

for INSTANCE_ID in $KAFKA_INSTANCE_IDS; do
  echo "=== $INSTANCE_ID ==="
  aws ec2 describe-instances --instance-ids $INSTANCE_ID \
    --query 'Reservations[0].Instances[0].SecurityGroups' \
    --output table
done

# Show all inbound rules for the Kafka security group
KAFKA_SG_ID="sg-0123456789abcdef0"
aws ec2 describe-security-groups --group-ids $KAFKA_SG_ID \
  --query 'SecurityGroups[0].IpPermissions[*].{
    Protocol:IpProtocol,
    FromPort:FromPort,
    ToPort:ToPort,
    CIDR:IpRanges[0].CidrIp,
    SGRef:UserIdGroupPairs[0].GroupId,
    Desc:UserIdGroupPairs[0].Description
  }' \
  --output table

# Check if a specific source SG can reach Kafka on port 9092
SOURCE_SG="sg-spark-workers-abc123"
aws ec2 describe-security-groups --group-ids $KAFKA_SG_ID \
  --query "SecurityGroups[0].IpPermissions[?
    FromPort<=\`9092\` && ToPort>=\`9092\`
  ].UserIdGroupPairs[?GroupId==\`$SOURCE_SG\`].GroupId" \
  --output text
# If empty → no rule allows this SG to reach Kafka on 9092
```

### 4.2 Adding a Missing Security Group Rule

```bash
# Scenario: New Spark subnet (10.0.20.0/24) needs access to Kafka on 9092
# Option A: CIDR-based (less preferred — needs updating when subnets change)
aws ec2 authorize-security-group-ingress \
  --group-id sg-kafka-brokers-abc123 \
  --protocol tcp \
  --port 9092 \
  --cidr 10.0.20.0/24 \
  --description "Spark workers in new subnet"

# Option B: Security group reference (preferred — auto-applies to new instances)
aws ec2 authorize-security-group-ingress \
  --group-id sg-kafka-brokers-abc123 \
  --source-group sg-spark-workers-def456 \
  --protocol tcp \
  --port 9092 \
  --description "Spark workers SG → Kafka brokers"

# Verify the rule was added
aws ec2 describe-security-groups --group-ids sg-kafka-brokers-abc123 \
  --query 'SecurityGroups[0].IpPermissions[?FromPort==`9092`]' \
  --output json
```

### 4.3 Debugging Kubernetes NetworkPolicy

```bash
# Check which NetworkPolicies exist in a namespace
kubectl get networkpolicies -n kafka-prod -o wide

# Describe a specific NetworkPolicy
kubectl describe networkpolicy kafka-broker-ingress -n kafka-prod

# Check if a pod is selected by a NetworkPolicy
# (using label selector matching)
kubectl get pods -n kafka-prod -l app=kafka-broker
kubectl get networkpolicies -n kafka-prod -o json | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
for np in data['items']:
    sel = np['spec'].get('podSelector', {}).get('matchLabels', {})
    print(np['metadata']['name'], '→ selects:', sel)
"

# Use Cilium (if installed) to verify network connectivity
kubectl exec -n data-pipelines spark-pod-xyz -- \
  curl -v --max-time 5 http://kafka.kafka-prod.svc.cluster.local:9092

# Use netcat to test port reachability from a pod
kubectl run -it --rm test-pod \
  --image=busybox \
  --namespace=data-pipelines \
  -- nc -z kafka.kafka-prod.svc.cluster.local 9092
# Exit code 0 = connected; non-zero = blocked

# Check Calico network policy logs (if using Calico)
kubectl logs -n kube-system -l k8s-app=calico-node | \
  grep -i "denied\|drop" | tail -20

# Cilium network policy verdict (if using Cilium)
kubectl exec -n kube-system cilium-pod-xyz -- \
  cilium monitor --type drop | head -50
```

### 4.4 Testing with a Network Policy Dry Run

```bash
# Apply the default-deny policy to a test namespace and verify behavior
kubectl create namespace test-isolation
kubectl apply -f - << 'EOF'
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: test-isolation
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress
EOF

# Deploy a test pod and verify it can't reach anything
kubectl run test-pod -n test-isolation --image=busybox -- sleep 3600
kubectl exec -n test-isolation test-pod -- wget -q --timeout=5 \
  http://kafka.kafka-prod.svc.cluster.local:8081 -O /dev/null \
  && echo "REACHABLE" || echo "BLOCKED (expected)"

# Add an allow rule and verify it works
kubectl apply -f - << 'EOF'
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-schema-registry
  namespace: test-isolation
spec:
  podSelector: {}
  policyTypes:
  - Egress
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: kafka-prod
    ports:
    - port: 8081
      protocol: TCP
EOF

kubectl exec -n test-isolation test-pod -- wget -q --timeout=5 \
  http://schema-registry.kafka-prod.svc.cluster.local:8081/subjects \
  -O /dev/null && echo "REACHABLE" || echo "BLOCKED"

# Clean up
kubectl delete namespace test-isolation
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
sg_diagnostics.py

Security group and network policy diagnostic toolkit for data engineers.
Audits AWS security group rules, checks connectivity, validates Kubernetes
NetworkPolicy configurations, and identifies common misconfigurations.

Dependencies: boto3 (AWS), kubernetes (optional, for K8s NetworkPolicy audit)
"""

import os
import re
import json
import time
import socket
import concurrent.futures
from dataclasses import dataclass, field
from typing import Optional


# ─── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class SGRule:
    protocol: str         # tcp, udp, icmp, -1 (all)
    from_port: int
    to_port: int
    source: str           # CIDR or SG ID
    description: str
    is_sg_reference: bool # True if source is an SG, not a CIDR


@dataclass
class SecurityGroupInfo:
    sg_id: str
    name: str
    description: str
    vpc_id: str
    inbound_rules: list[SGRule]
    outbound_rules: list[SGRule]
    warnings: list[str]


@dataclass
class ConnectivityResult:
    source_label: str
    target_host: str
    target_port: int
    reachable: bool
    latency_ms: Optional[float]
    error: Optional[str]
    # Security group inference: timed out → SG blocking; refused → SG OK
    sg_blocking: Optional[bool]   # True if timeout implies SG block


@dataclass
class NetworkPolicyInfo:
    name: str
    namespace: str
    pod_selector: dict
    policy_types: list[str]
    ingress_rules: list[dict]
    egress_rules: list[dict]
    warnings: list[str]


# ─── AWS Security Group Auditor ─────────────────────────────────────────────────

class SecurityGroupAuditor:
    """
    Audits AWS Security Groups for common data platform misconfigurations.
    """

    # Sensitive ports that should never be open to 0.0.0.0/0
    SENSITIVE_PORTS = {
        22: 'SSH',
        3389: 'RDP',
        9092: 'Kafka',
        9093: 'Kafka-SSL',
        5432: 'Postgres',
        3306: 'MySQL',
        6379: 'Redis',
        27017: 'MongoDB',
        2181: 'ZooKeeper',
        8080: 'HTTP-alt',
        9200: 'Elasticsearch',
    }

    def __init__(self, region: str = None):
        try:
            import boto3
            self.ec2 = boto3.client(
                'ec2',
                region_name=region or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
            )
        except ImportError:
            raise RuntimeError("boto3 not installed. Run: pip install boto3")

    def get_security_group(self, sg_id: str) -> SecurityGroupInfo:
        """Retrieve and analyze a security group."""
        resp = self.ec2.describe_security_groups(GroupIds=[sg_id])
        sg = resp['SecurityGroups'][0]
        warnings = []

        inbound = self._parse_rules(sg.get('IpPermissions', []))
        outbound = self._parse_rules(sg.get('IpPermissionsEgress', []))

        # Audit inbound rules
        for rule in inbound:
            for cidr in [rule.source]:
                if cidr in ('0.0.0.0/0', '::/0'):
                    if rule.from_port in self.SENSITIVE_PORTS:
                        svc = self.SENSITIVE_PORTS[rule.from_port]
                        warnings.append(
                            f"CRITICAL: {svc} port {rule.from_port} is open to "
                            f"{cidr} (the public internet). "
                            f"This should be restricted to a specific SG or CIDR."
                        )
                    elif rule.protocol == '-1':
                        warnings.append(
                            f"WARNING: All traffic (all ports, all protocols) "
                            f"allowed inbound from {cidr}."
                        )

        # Audit outbound rules
        all_outbound = any(
            r.protocol == '-1' and r.source in ('0.0.0.0/0', '::/0')
            for r in outbound
        )
        if not all_outbound and not outbound:
            warnings.append(
                "INFO: No outbound rules configured. "
                "This instance cannot initiate any outbound connections. "
                "This may be intentional (isolated DB) or a misconfiguration."
            )

        return SecurityGroupInfo(
            sg_id=sg['GroupId'],
            name=sg.get('GroupName', ''),
            description=sg.get('Description', ''),
            vpc_id=sg.get('VpcId', ''),
            inbound_rules=inbound,
            outbound_rules=outbound,
            warnings=warnings,
        )

    def _parse_rules(self, permissions: list) -> list[SGRule]:
        """Parse AWS IP permissions into SGRule objects."""
        rules = []
        for perm in permissions:
            protocol = perm.get('IpProtocol', '-1')
            from_port = perm.get('FromPort', 0)
            to_port = perm.get('ToPort', 65535)

            # CIDR-based rules
            for ip_range in perm.get('IpRanges', []):
                rules.append(SGRule(
                    protocol=protocol,
                    from_port=from_port,
                    to_port=to_port,
                    source=ip_range.get('CidrIp', ''),
                    description=ip_range.get('Description', ''),
                    is_sg_reference=False,
                ))

            # IPv6 CIDR rules
            for ip_range in perm.get('Ipv6Ranges', []):
                rules.append(SGRule(
                    protocol=protocol,
                    from_port=from_port,
                    to_port=to_port,
                    source=ip_range.get('CidrIpv6', ''),
                    description=ip_range.get('Description', ''),
                    is_sg_reference=False,
                ))

            # Security group reference rules
            for pair in perm.get('UserIdGroupPairs', []):
                rules.append(SGRule(
                    protocol=protocol,
                    from_port=from_port,
                    to_port=to_port,
                    source=pair.get('GroupId', ''),
                    description=pair.get('Description', ''),
                    is_sg_reference=True,
                ))

        return rules

    def find_sgs_allowing_port(
        self,
        vpc_id: str,
        port: int,
    ) -> list[dict]:
        """
        Find all security groups in a VPC that have inbound rules
        allowing the specified port from any source.
        """
        paginator = self.ec2.get_paginator('describe_security_groups')
        results = []

        for page in paginator.paginate(
            Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
        ):
            for sg in page['SecurityGroups']:
                for perm in sg.get('IpPermissions', []):
                    from_port = perm.get('FromPort', 0)
                    to_port = perm.get('ToPort', 65535)
                    if perm.get('IpProtocol') == '-1' or \
                       (from_port <= port <= to_port):
                        sources = (
                            [r.get('CidrIp') for r in perm.get('IpRanges', [])] +
                            [p.get('GroupId') for p in perm.get('UserIdGroupPairs', [])]
                        )
                        results.append({
                            'sg_id': sg['GroupId'],
                            'sg_name': sg.get('GroupName', ''),
                            'port': port,
                            'sources': [s for s in sources if s],
                        })
        return results

    def get_instances_with_sg(self, sg_id: str) -> list[dict]:
        """Find all EC2 instances that have a specific security group attached."""
        paginator = self.ec2.get_paginator('describe_instances')
        instances = []

        for page in paginator.paginate(
            Filters=[
                {'Name': 'instance.group-id', 'Values': [sg_id]},
                {'Name': 'instance-state-name', 'Values': ['running']},
            ]
        ):
            for reservation in page['Reservations']:
                for instance in reservation['Instances']:
                    name = next(
                        (t['Value'] for t in instance.get('Tags', [])
                         if t['Key'] == 'Name'), ''
                    )
                    instances.append({
                        'instance_id': instance['InstanceId'],
                        'name': name,
                        'private_ip': instance.get('PrivateIpAddress', ''),
                        'az': instance.get('Placement', {}).get('AvailabilityZone', ''),
                    })
        return instances


# ─── Connectivity Checker ─────────────────────────────────────────────────────────

def check_tcp_connectivity(
    host: str,
    port: int,
    timeout_s: float = 5.0,
    source_label: str = 'local',
) -> ConnectivityResult:
    """
    Test TCP connectivity to a host:port.
    Distinguishes between timeout (SG blocking) and refused (SG OK, service down).
    """
    start = time.perf_counter()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout_s)
        s.connect((host, port))
        s.close()
        latency_ms = (time.perf_counter() - start) * 1000
        return ConnectivityResult(
            source_label=source_label,
            target_host=host,
            target_port=port,
            reachable=True,
            latency_ms=latency_ms,
            error=None,
            sg_blocking=False,
        )
    except socket.timeout:
        latency_ms = (time.perf_counter() - start) * 1000
        return ConnectivityResult(
            source_label=source_label,
            target_host=host,
            target_port=port,
            reachable=False,
            latency_ms=latency_ms,
            error='Connection timed out',
            sg_blocking=True,  # Timeout = SG likely blocking
        )
    except ConnectionRefusedError:
        latency_ms = (time.perf_counter() - start) * 1000
        return ConnectivityResult(
            source_label=source_label,
            target_host=host,
            target_port=port,
            reachable=False,
            latency_ms=latency_ms,
            error='Connection refused',
            sg_blocking=False,  # Refused = SG OK, service not listening
        )
    except OSError as e:
        latency_ms = (time.perf_counter() - start) * 1000
        return ConnectivityResult(
            source_label=source_label,
            target_host=host,
            target_port=port,
            reachable=False,
            latency_ms=latency_ms,
            error=str(e),
            sg_blocking=None,
        )
    finally:
        try:
            s.close()
        except Exception:
            pass


def check_data_platform_connectivity(
    kafka_hosts: list[tuple[str, int]] = None,
    postgres_hosts: list[tuple[str, int]] = None,
    schema_registry_hosts: list[tuple[str, int]] = None,
    timeout_s: float = 5.0,
) -> list[ConnectivityResult]:
    """
    Run connectivity checks against all data platform components in parallel.
    Returns results with diagnosis of whether SG or service is the problem.
    """
    endpoints = []
    if kafka_hosts:
        for host, port in kafka_hosts:
            endpoints.append((host, port, 'kafka'))
    if postgres_hosts:
        for host, port in postgres_hosts:
            endpoints.append((host, port, 'postgres'))
    if schema_registry_hosts:
        for host, port in schema_registry_hosts:
            endpoints.append((host, port, 'schema-registry'))

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(endpoints)) as ex:
        futures = {
            ex.submit(check_tcp_connectivity, h, p, timeout_s, label): (h, p, label)
            for h, p, label in endpoints
        }
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    return sorted(results, key=lambda r: (r.target_host, r.target_port))


# ─── Kubernetes NetworkPolicy Auditor ─────────────────────────────────────────────

def audit_kubernetes_network_policies(namespace: str = None) -> list[NetworkPolicyInfo]:
    """
    Audit Kubernetes NetworkPolicies for common misconfigurations.
    Requires kubectl to be configured and available.
    """
    import subprocess

    cmd = ['kubectl', 'get', 'networkpolicies', '-o', 'json']
    if namespace:
        cmd += ['-n', namespace]
    else:
        cmd += ['--all-namespaces']

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return []

    policies = []
    for item in data.get('items', []):
        spec = item.get('spec', {})
        meta = item.get('metadata', {})
        warnings = []

        policy_types = spec.get('policyTypes', [])
        pod_sel = spec.get('podSelector', {})
        ingress_rules = spec.get('ingress', [])
        egress_rules = spec.get('egress', [])

        # Warn about AND vs OR confusion in from/to rules
        for rule in ingress_rules:
            from_list = rule.get('from', [])
            for entry in from_list:
                has_ns_sel = 'namespaceSelector' in entry
                has_pod_sel = 'podSelector' in entry
                if has_ns_sel and has_pod_sel:
                    # This is AND — only traffic from pods matching BOTH selectors
                    # This is intentional but worth flagging for clarity
                    pass  # Correct AND usage
                elif not has_ns_sel and has_pod_sel:
                    # podSelector alone in an entry = matches pods in SAME namespace
                    # Common intent: cross-namespace; common mistake: same-ns only
                    ns = meta.get('namespace', '')
                    warnings.append(
                        f"Ingress rule has podSelector without namespaceSelector. "
                        f"This only allows traffic from pods IN THE SAME NAMESPACE "
                        f"({ns}). Add namespaceSelector to allow cross-namespace "
                        f"traffic."
                    )

        # Warn about missing DNS egress in egress-restricting policies
        if 'Egress' in policy_types and not egress_rules:
            warnings.append(
                "Empty egress rules with Egress policyType = deny all outbound. "
                "Pods cannot resolve DNS names. Add a DNS egress rule for "
                "UDP/TCP 53 to kube-system namespace."
            )

        # Warn about wildcard namespace selectors
        for rule in ingress_rules:
            for entry in rule.get('from', []):
                ns_sel = entry.get('namespaceSelector', {})
                if ns_sel == {}:  # Empty selector = all namespaces
                    warnings.append(
                        "WARNING: Empty namespaceSelector ({}) matches ALL namespaces. "
                        "Any pod in any namespace can send traffic. "
                        "Restrict to specific namespaces using matchLabels."
                    )

        policies.append(NetworkPolicyInfo(
            name=meta.get('name', ''),
            namespace=meta.get('namespace', ''),
            pod_selector=pod_sel,
            policy_types=policy_types,
            ingress_rules=ingress_rules,
            egress_rules=egress_rules,
            warnings=warnings,
        ))

    return policies


# ─── Report ─────────────────────────────────────────────────────────────────────

def sg_audit_report(
    sg_ids: Optional[list[str]] = None,
    kafka_bootstrap: Optional[str] = None,
    check_k8s: bool = False,
    namespace: Optional[str] = None,
) -> None:
    """Print a comprehensive security group and connectivity audit report."""
    print("=" * 70)
    print("SECURITY GROUP / NETWORK POLICY AUDIT REPORT")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Connectivity checks
    if kafka_bootstrap:
        print(f"\n[1] KAFKA CONNECTIVITY CHECK ({kafka_bootstrap})")
        hosts = []
        for server in kafka_bootstrap.split(','):
            server = server.strip()
            host, port_str = server.rsplit(':', 1)
            hosts.append((host, int(port_str)))

        results = check_data_platform_connectivity(kafka_hosts=hosts, timeout_s=5.0)
        for r in results:
            if r.reachable:
                icon = "✓"
                diagnosis = f"reachable ({r.latency_ms:.1f}ms)"
            elif r.sg_blocking:
                icon = "✗"
                diagnosis = "TIMED OUT → likely security group blocking"
            else:
                icon = "✗"
                diagnosis = f"refused → SG OK but service not listening (error: {r.error})"
            print(f"  {icon} {r.target_host}:{r.target_port} — {diagnosis}")

    # Security group audit
    if sg_ids:
        try:
            auditor = SecurityGroupAuditor()
            print(f"\n[2] SECURITY GROUP AUDIT")
            for sg_id in sg_ids:
                try:
                    sg_info = auditor.get_security_group(sg_id)
                    print(f"\n  SG: {sg_info.sg_id} ({sg_info.name})")
                    print(f"  Inbound rules ({len(sg_info.inbound_rules)}):")
                    for rule in sg_info.inbound_rules[:10]:
                        proto = rule.protocol if rule.protocol != '-1' else 'ALL'
                        port_range = (
                            f"{rule.from_port}" if rule.from_port == rule.to_port
                            else f"{rule.from_port}-{rule.to_port}"
                        )
                        src_type = "SG" if rule.is_sg_reference else "CIDR"
                        print(f"    {proto}/{port_range:<10} {src_type}: {rule.source}  "
                              f"({rule.description or 'no description'})")
                    for warning in sg_info.warnings:
                        icon = "🚨" if "CRITICAL" in warning else "⚠️ "
                        print(f"  {icon} {warning}")
                except Exception as e:
                    print(f"  Error auditing {sg_id}: {e}")
        except RuntimeError as e:
            print(f"  {e} — running demo mode")
            _demo_sg_analysis()

    # Kubernetes NetworkPolicy audit
    if check_k8s:
        print(f"\n[3] KUBERNETES NETWORKPOLICY AUDIT (namespace: {namespace or 'all'})")
        policies = audit_kubernetes_network_policies(namespace=namespace)
        if not policies:
            print("  No NetworkPolicies found (or kubectl not configured).")
            print("  Without NetworkPolicies, all pods can reach all other pods.")
        else:
            for np in policies:
                print(f"\n  Policy: {np.namespace}/{np.name}")
                print(f"  Types: {np.policy_types}")
                print(f"  Selector: {np.pod_selector}")
                for w in np.warnings:
                    print(f"  ⚠️  {w}")

    print("\n" + "=" * 70)


def _demo_sg_analysis():
    """Demo security group analysis without AWS credentials."""
    print("\n  [DEMO] Analyzing sample security group rules:\n")

    # Simulate an overly permissive SG
    demo_rules = [
        SGRule('tcp', 9092, 9092, '0.0.0.0/0', 'kafka public', False),
        SGRule('tcp', 5432, 5432, '10.0.0.0/8', 'postgres internal', False),
        SGRule('tcp', 22, 22, 'sg-bastion-abc', 'SSH via bastion', True),
        SGRule('-1', 0, 65535, '0.0.0.0/0', 'all traffic open', False),
    ]

    sensitive_ports = {9092: 'Kafka', 5432: 'Postgres', 22: 'SSH'}
    for rule in demo_rules:
        proto = rule.protocol if rule.protocol != '-1' else 'ALL'
        port = str(rule.from_port) if rule.from_port == rule.to_port else 'ALL'
        print(f"  Rule: {proto}/{port} from {rule.source}")
        if rule.source == '0.0.0.0/0':
            svc = sensitive_ports.get(rule.from_port, 'all services')
            if rule.protocol == '-1':
                print(f"    🚨 CRITICAL: ALL traffic open to internet — "
                      f"remove or restrict immediately")
            elif rule.from_port in sensitive_ports:
                print(f"    🚨 CRITICAL: {svc} open to internet (0.0.0.0/0) — "
                      f"restrict to known source SG or CIDR")
        elif not rule.is_sg_reference and '/' in rule.source:
            print(f"    ⚠️  Using CIDR source — consider switching to SG reference "
                  f"for dynamic auto-scaling environments")
        else:
            print(f"    ✓ Source is a security group reference (preferred)")


# ─── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Security group diagnostic toolkit")
    parser.add_argument('--sg-ids', nargs='+', metavar='SG_ID',
                        help='Security group IDs to audit')
    parser.add_argument('--kafka', metavar='BOOTSTRAP_SERVERS',
                        help='Kafka bootstrap servers to check connectivity')
    parser.add_argument('--k8s', action='store_true',
                        help='Audit Kubernetes NetworkPolicies')
    parser.add_argument('--namespace', metavar='NAMESPACE',
                        help='Kubernetes namespace to audit')
    parser.add_argument('--test', nargs=2, metavar=('HOST', 'PORT'),
                        help='Test connectivity to HOST:PORT')
    args = parser.parse_args()

    if args.test:
        host, port = args.test[0], int(args.test[1])
        result = check_tcp_connectivity(host, port)
        if result.reachable:
            print(f"✓ {host}:{port} reachable ({result.latency_ms:.1f}ms)")
        elif result.sg_blocking:
            print(f"✗ {host}:{port} TIMED OUT → security group likely blocking")
        else:
            print(f"✗ {host}:{port} {result.error} → SG OK, service issue")
    else:
        sg_audit_report(
            sg_ids=args.sg_ids,
            kafka_bootstrap=args.kafka,
            check_k8s=args.k8s,
            namespace=args.namespace,
        )
```

---

## 6. Visual Reference

### Security Group Architecture for a Data Platform

```
Internet
    │
    ▼ [Inbound: HTTPS 443]
┌─────────────────────────────────────────────────────────────┐
│  sg-alb (Application Load Balancer)                         │
│  Inbound:  443 from 0.0.0.0/0                               │
│  Outbound: 8081 to sg-schema-registry                       │
│            9092 to sg-kafka-brokers (NLB)                   │
└───────────────────────────┬─────────────────────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
┌──────────────────┐ ┌──────────────┐ ┌──────────────────────┐
│ sg-kafka-brokers │ │ sg-schema-   │ │ sg-spark-workers     │
│ Inbound:         │ │ registry     │ │ Inbound: none        │
│  9092 from       │ │ Inbound:     │ │ Outbound:            │
│    sg-kafka-     │ │  8081 from   │ │  9092 to sg-kafka    │
│    clients       │ │    sg-alb    │ │  5432 to sg-postgres │
│  9092 from       │ │  8081 from   │ │  8081 to sg-schema-  │
│    sg-spark-     │ │    sg-spark  │ │    registry          │
│    workers       │ │  8081 from   │ │  443 to 0.0.0.0/0   │
│  9093 from       │ │    sg-kafka- │ │    (S3, updates)     │
│    sg-kafka-     │ │    connect   │ └──────────────────────┘
│    brokers       │ └──────────────┘
│ Outbound: all    │          
└──────────────────┘          ┌──────────────────────┐
                              │ sg-postgres           │
                              │ Inbound:              │
                              │  5432 from sg-airflow │
                              │  5432 from sg-spark   │
                              │  5432 from sg-dbt     │
                              │ Outbound: none        │
                              └──────────────────────┘
```

### Connection Refused vs Timed Out

```
Scenario A: Security Group blocking
  Spark → [SYN] → NIC → [SG evaluates] → REJECT → packet dropped
  Spark waits for SYN-ACK...
  After timeout_s: "Connection timed out"
  VPC Flow Log: action=REJECT

Scenario B: Security Group allows, service not running
  Spark → [SYN] → NIC → [SG evaluates] → ACCEPT → Kafka process
  Kafka: nothing listening on 9092
  Kernel sends RST immediately
  Spark: "Connection refused" (instant)
  VPC Flow Log: action=ACCEPT

Scenario C: Security Group allows, service running
  Spark → [SYN] → NIC → SG → Kafka
  Kafka: SYN-ACK → Spark → ACK → ESTABLISHED
  "Connected in 0.8ms"
```

### Kubernetes NetworkPolicy AND vs OR

```
# Case 1: AND semantics (both conditions in same element)
from:
- namespaceSelector:                 ← Must be from namespace env=production
    matchLabels:                      AND
      env: production
  podSelector:                       ← Must be a kafka-client pod
    matchLabels:
      role: kafka-client
# Result: only kafka-client pods in production namespace

# Case 2: OR semantics (separate list elements)
from:
- namespaceSelector:                 ← Traffic from any pod in production namespace
    matchLabels:
      env: production
- podSelector:                       ← OR: any kafka-client pod in same namespace
    matchLabels:
      role: kafka-client
# Result: all pods in production ns, OR kafka-client pods in same ns
```

---

## 7. Common Mistakes

**Mistake 1: Using CIDR-based security group rules for dynamic auto-scaling workloads.** When a new EMR cluster launches in a new subnet (`10.0.30.0/24`), it cannot connect to Kafka unless the Kafka security group has a rule for that CIDR. Security group references (`sg-kafka-clients`) automatically apply to any instance with that security group, regardless of IP — the rule doesn't need to be updated when instances launch in new subnets or get new IPs.

**Mistake 2: Opening port 0-65535 to an internal CIDR instead of a specific security group.** Allowing "all ports" from `10.0.0.0/8` to a Kafka broker means that any instance in the entire `10.0.0.0/8` network can connect on any port — including SSH (22), JMX (9999), and the broker's internal ports. Restrict to specific ports and specific security groups.

**Mistake 3: Confusing `Connection refused` with `Connection timed out` when debugging.** These two errors have fundamentally different implications. `Timed out` means the packet was dropped before reaching the destination (security group or NACL blocking). `Refused` means the packet reached the host but nothing accepted it (process not running, wrong port). Always check which error type you're seeing before changing security groups.

**Mistake 4: Applying default-deny egress in Kubernetes without adding DNS egress.** After applying `policyType: Egress` with no egress rules, pods cannot resolve any DNS names — which means they cannot reach any service by hostname. This breaks all microservice communication that uses DNS (which is everything in Kubernetes). Always add an explicit egress rule for UDP/TCP port 53 to `kube-system` alongside any egress-restricting NetworkPolicy.

**Mistake 5: Using an empty `namespaceSelector: {}` in a NetworkPolicy.** An empty namespace selector (`{}`) matches ALL namespaces. A NetworkPolicy that says "allow ingress from `namespaceSelector: {}`" allows traffic from every pod in every namespace in the cluster. This is almost never the intended behavior. Always specify `matchLabels` on namespace selectors.

**Mistake 6: Not applying NetworkPolicies to new namespaces.** A default-deny policy in `kafka-prod` namespace does not automatically apply to a new `kafka-staging` namespace. Each namespace requires its own NetworkPolicy setup. When a new namespace is created, it starts with no NetworkPolicies (all traffic allowed) unless an admission controller or automation applies baseline policies automatically.

---

## 8. Production Failure Scenarios

### Scenario 1: Kafka Connect Debezium Cannot Reach Postgres After Security Group Rotation

**Symptoms:** Debezium CDC connector reports `Connection to postgres.prod.internal:5432 refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.` A manual `nc -z postgres.prod.internal 5432` from the Kafka Connect host times out (not refused — a discrepancy).

**Root cause:** The error message from Debezium is misleading — it shows "refused" but the actual error is a timeout. The security group for Postgres was recently rotated (a new SG replaced the old SG on the Postgres instance for compliance reasons). The old SG (`sg-postgres-old`) had rules allowing `sg-kafka-connect`. The new SG (`sg-postgres-new`) was a copy of the production database SG that only allows connections from `sg-application-tier` — the Kafka Connect security group was never added.

**Diagnosis:**
```bash
# Test from the Kafka Connect host:
timeout 3 nc -z postgres.prod.internal 5432 || echo "TIMEOUT (SG blocking)"
# Output: TIMEOUT (SG blocking) → confirms SG issue, not Debezium config

# Check which SG is on the Postgres instance:
aws ec2 describe-instances \
  --filters "Name=private-dns-name,Values=postgres.prod.internal" \
  --query 'Reservations[0].Instances[0].SecurityGroups'
# Shows sg-postgres-new, not sg-postgres-old

# Check sg-postgres-new for port 5432 rules:
aws ec2 describe-security-groups --group-ids sg-postgres-new \
  --query 'SecurityGroups[0].IpPermissions[?FromPort==`5432`]'
# Shows: only sg-application-tier is allowed; sg-kafka-connect is missing
```

**Fix:** Add an inbound rule to `sg-postgres-new`:
```bash
aws ec2 authorize-security-group-ingress \
  --group-id sg-postgres-new \
  --source-group sg-kafka-connect \
  --protocol tcp \
  --port 5432 \
  --description "Debezium CDC connector"
```

### Scenario 2: Kubernetes Schema Registry Client Breaks After Namespace Migration

**Symptoms:** After a team refactors a Spark pipeline into a dedicated namespace `data-transformations`, schema validation calls to the schema registry fail. The error is `Connection timed out` after 30 seconds, impacting pipeline latency SLOs.

**Root cause:** The schema registry deployment in `kafka-prod` namespace has a NetworkPolicy:
```yaml
spec:
  podSelector:
    matchLabels: {app: schema-registry}
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: data-pipelines   # old namespace
      podSelector:
        matchLabels:
          role: kafka-client
```
The Spark pods are now in `data-transformations` (new namespace) and are not covered by the `data-pipelines` namespace selector. The NetworkPolicy blocks all other traffic — the pods in `data-transformations` silently have their packets dropped.

**Fix:** Update the NetworkPolicy to include the new namespace:
```yaml
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: data-pipelines
      podSelector:
        matchLabels:
          role: kafka-client
    - namespaceSelector:                               # Add this
        matchLabels:
          kubernetes.io/metadata.name: data-transformations
      podSelector:
        matchLabels:
          role: kafka-client
```

### Scenario 3: Intermittent Kafka Producer Failures After MSK Security Group Change

**Symptoms:** Kafka producers intermittently fail with `Connection to broker kafka-2.msk.internal:9092 was lost` on existing long-lived connections. New connections succeed. The failures correlate with a security group rule update that replaced CIDR-based rules with SG-based rules.

**Root cause:** Security group rule changes affect new connections — the stateful connection tracking continues to allow existing connections. However, if the old rule that was deleted included a rule that was being used for the existing connection's NAT or reverse-path lookup, some cloud implementations can drop existing connections. More specifically: the security group update removed the source CIDR rule and added an SG reference rule, but the MSK brokers' internal cross-broker replication connections used a different ENI than expected. The replication traffic was using the management ENI's CIDR (not the broker data ENI's SG), and that CIDR rule was removed.

**Diagnosis:**
```bash
# Check VPC flow logs for REJECT on the inter-broker port (9093)
# Replication traffic lost = brokers can't replicate = ISR shrinks

# Check MSK broker metrics in CloudWatch
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kafka \
  --metric-name UnderReplicatedPartitions \
  --dimensions Name=Cluster Name,Value=data-platform-kafka \
               Name=Broker ID,Value=2 \
  --start-time $(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 \
  --statistics Maximum
# Spike in UnderReplicatedPartitions = replication broken = SG blocking 9093
```

**Fix:** Re-add the CIDR rule for inter-broker replication on port 9093, or identify the correct ENIs used by MSK internal traffic and reference their security groups.

### Scenario 4: GCP Firewall Rule Priority Conflict Blocks Airflow to Postgres

**Symptoms:** Airflow metadata database connection suddenly fails after a network security review. GCP Cloud Logging shows `Connection refused` from Airflow workers to the Postgres primary.

**Root cause:** The security review added a "block all non-approved traffic" firewall rule with priority 500:
```
Rule: deny-all-internal
Direction: INGRESS
Action: DENY
Source: 10.0.0.0/8
Target: all instances
Priority: 500
```

This rule has higher priority (lower number) than the existing allow rule for Postgres:
```
Rule: allow-postgres-from-airflow
Direction: INGRESS
Action: ALLOW
Source tags: airflow-worker
Target tags: postgres-primary
Port: 5432
Priority: 1000
```

In GCP, lower priority number = higher precedence. The `deny-all-internal` rule at priority 500 evaluates before `allow-postgres-from-airflow` at priority 1000. The deny rule matches first and blocks all internal traffic.

**Fix:** Either set `deny-all-internal` to a lower priority (e.g., 900, below the specific allow rules), or restructure: use a high-priority deny for specific risky ports, not a blanket deny-all.

---

## 9. Performance and Tuning

### Security Group Rule Evaluation Performance

AWS security group rule evaluation happens in the AWS network fabric before packets reach the instance — it adds negligible latency (< 0.01 ms). The number of rules in a security group (AWS allows up to 60 inbound + 60 outbound per SG, and up to 5 SGs per ENI = up to 300 inbound rules) does not significantly affect throughput. Security group rule count is an operational complexity concern, not a performance concern.

**EKS Network Policy Performance:** Kubernetes NetworkPolicies implemented by Calico use iptables, which has the same O(N) rule evaluation issue discussed in M31 (kube-proxy iptables). Cilium implements NetworkPolicy using eBPF — O(1) lookup — and is significantly more performant at scale. For clusters with hundreds of NetworkPolicies or millions of packets per second, Cilium provides better performance than Calico/iptables.

### Minimizing MSK Security Group Complexity

For MSK (Managed Kafka on AWS), create a dedicated SG per client type rather than one SG for all clients. This allows monitoring which clients are connecting without complex CloudWatch metrics:

```
sg-msk-brokers         → assigned to MSK broker ENIs
sg-msk-producers       → assigned to producer EC2/EKS pods
sg-msk-consumers       → assigned to consumer EC2/EKS pods  
sg-msk-connect         → assigned to Kafka Connect workers
sg-msk-schema-registry → assigned to Schema Registry instances
sg-msk-monitoring      → assigned to Prometheus/monitoring instances

MSK broker SG inbound rules:
  9092 from sg-msk-producers
  9092 from sg-msk-consumers
  9092 from sg-msk-connect
  9092 from sg-msk-schema-registry
  9092 from sg-msk-monitoring
  9093 from sg-msk-brokers (inter-broker replication)
```

---

## 10. Interview Q&A

**Q1: What is the difference between a security group and a Network ACL in AWS? When would you use each?**

Security groups and Network ACLs (NACLs) are both network-level controls in AWS, but they operate at different layers with different semantics.

Security groups operate at the instance level (attached to an ENI). They are stateful: if an inbound connection is permitted, the response traffic is automatically allowed outbound — you only need rules in the direction of connection initiation. They support only Allow rules — there are no explicit Deny rules. Multiple security groups can be combined on a single instance, and their rules are evaluated as a union (if any rule across all attached SGs allows the traffic, it passes). Security groups are the primary tool for most data platform security requirements.

NACLs operate at the subnet level — every packet entering or leaving a subnet is evaluated against the NACL. They are stateless: if you allow inbound TCP traffic, you must also explicitly allow the return traffic in the outbound rules (or allow the ephemeral port range 1024-65535 outbound). They support both Allow and Deny rules, evaluated in numerical order where the first matching rule wins. Each subnet has exactly one NACL.

For data engineering, security groups cover almost all security requirements. NACLs are useful for coarse-grained controls: blocking all traffic between the production VPC and development VPC at the subnet level (where even compromised instances in production cannot reach development), or blocking known malicious IP ranges before they can even attempt to connect to any instance. Think of NACLs as the building-level security perimeter and security groups as the office-level access control.

**Q2: Explain how Kubernetes NetworkPolicy works, and describe the most common mistake teams make when implementing cross-namespace traffic rules.**

A Kubernetes NetworkPolicy is a namespaced resource that specifies which pods can send traffic to which other pods, and on which ports. When any NetworkPolicy exists that selects a pod (via `podSelector`), that pod's traffic becomes subject to the policy's rules — traffic is denied unless explicitly allowed by a matching rule. Pods with no selecting NetworkPolicy receive and send unrestricted traffic.

NetworkPolicies are implemented by the cluster's CNI plugin (Calico, Cilium, Amazon VPC CNI + Calico). Without a CNI plugin that supports NetworkPolicy, the policy resource exists in Kubernetes but is not enforced — a common and dangerous gap.

The most common mistake in cross-namespace traffic rules is the AND vs OR semantics of the `from` list. When a NetworkPolicy has a `from` element containing both a `namespaceSelector` and a `podSelector` within the same list element, both conditions must be true simultaneously — this is AND. Traffic must be from a pod that is both in a namespace matching `namespaceSelector` AND has labels matching `podSelector`. But when a team writes a policy intending "allow traffic from namespace A, or from pods with label X," they often put both conditions in the same element (AND semantics) instead of separate elements (OR semantics). The result: traffic from namespace A without the required label is blocked, and traffic from pods with the required label in a different namespace is also blocked, even though both should be permitted.

The debugging signature of this mistake: the NetworkPolicy appears to allow the traffic you want (you can see the namespace and pod selector you intended), but traffic is still being dropped. Checking with `cilium monitor --type drop` or VPC flow logs reveals the packets are being dropped. The fix is to split the single `from` element into two separate list elements.

**Q3: When a Kafka producer reports `Connection timed out` to a broker, what is your debugging sequence?**

The first and most important observation: `Connection timed out` means the TCP SYN packet was sent but no response came back. The packet was silently dropped somewhere in the network path. `Connection refused` would mean the packet arrived but the host rejected it — that's a different problem entirely. `Timed out` narrows the diagnosis to: security group blocking, NACL blocking, routing issue, or the destination host is completely down.

My debugging sequence starts closest to the destination and works outward. First, verify the Kafka broker is actually running: SSH to the broker and run `ss -tnlp | grep 9092` — is the process listening? If not, it's a Kafka process problem, not a network problem. Second, test from a host that is known to work (an existing producer that is successfully connected): `nc -z kafka-broker.internal 9092`. If this succeeds from the working host but fails from the new host, the network or security group difference between the hosts is the problem. Third, compare security groups: list the SGs attached to the new (failing) host and compare against the SGs attached to the working host. If the new host is missing `sg-kafka-clients`, that's the cause. Fourth, check VPC flow logs: query for REJECT entries from the new host's IP to the broker's IP on port 9092. A REJECT entry confirms security group blocking.

If the security groups look identical and flow logs show no REJECT, the problem is at a different layer: NACL (subnet level), routing table (route missing), or DNS (the hostname resolves to an unexpected IP). Check NACL rules for the subnets involved and verify the resolved IP is correct with `dig kafka-broker.internal`.

---

## 11. Cross-Question Chain

**Interviewer:** You're setting up a new Kafka cluster on EKS for a data platform. Walk me through the network security design — both security groups (since the EKS nodes use EC2) and Kubernetes NetworkPolicies.

**Candidate:** I'd design two layers working together. At the EC2 level, the EKS worker nodes need security groups that allow inter-node traffic (pods can be on any node, so pod-to-pod traffic crosses nodes). EKS requires a node security group that allows all TCP between nodes in the cluster's security group (the VPC CNI assigns pod IPs from the node's subnet, and pods on different nodes communicate through the node security group). For external traffic to Kafka brokers specifically, the Kafka broker pods' nodes would have a security group allowing port 9092 from the load balancer security group, and the load balancer SG allows inbound 9092 from permitted producer/consumer source CIDRs or SGs.

At the Kubernetes level, I'd apply NetworkPolicies per namespace. The `kafka-prod` namespace would have a default-deny-all-ingress policy (empty podSelector, Ingress policyType, no rules). Then specific allow policies: `allow-kafka-broker-ingress` that permits TCP 9092 and 9093 from pods with `role: kafka-client` in the `data-pipelines` namespace AND from pods with `app: kafka-broker` in `kafka-prod` (for inter-broker replication). I'd also add a DNS egress policy so Kafka pods can resolve service DNS names.

**Interviewer:** The Spark jobs run in a `data-pipelines` namespace and need to reach both Kafka (in `kafka-prod`) and Schema Registry (also in `kafka-prod`). What NetworkPolicies do you add?

**Candidate:** I need both ingress policies on the destination side and egress policies on the source side. On the `kafka-prod` side, I already have `allow-kafka-broker-ingress` — I add a separate `allow-schema-registry-ingress` NetworkPolicy selecting `app: schema-registry` pods, allowing port 8081 from pods with `role: kafka-client` in the `data-pipelines` namespace.

On the `data-pipelines` side, if I have default-deny-egress applied, I need to explicitly allow outbound traffic to `kafka-prod` on ports 9092 and 8081. That's an egress NetworkPolicy on `data-pipelines` pods: `to namespaceSelector matching kafka-prod, ports 9092 and 8081`. Plus the DNS egress rule (UDP/TCP 53 to kube-system). The default-deny on egress is important — without it, a compromised Spark pod could exfiltrate data to arbitrary external addresses. With it, the Spark pods can only reach the explicitly permitted destinations.

**Interviewer:** A Spark pod in `data-pipelines` is getting `Connection timed out` to Schema Registry. The NetworkPolicy looks correct. What's your next step?

**Candidate:** I'd verify that the CNI plugin is actually enforcing NetworkPolicies — if the cluster uses a CNI that doesn't support NetworkPolicy (like Flannel without Calico), the policies exist but are not enforced. Run `kubectl get pods -n kube-system | grep -E 'calico|cilium'` to verify a policy-capable CNI is running.

Assuming CNI enforcement is active, the issue is likely the AND vs OR semantics. Let me look at the Schema Registry ingress NetworkPolicy carefully. If the `from` element has both `namespaceSelector` and `podSelector` in the same element, that's AND semantics — the Spark pod must match BOTH the namespace selector AND the pod selector simultaneously. If the Spark pods don't have the label `role: kafka-client`, they won't match the AND condition. The fix is to verify the Spark pods actually have the `role: kafka-client` label: `kubectl get pods -n data-pipelines -l role=kafka-client`. If that returns no pods, the label is missing from the Spark pod spec.

**Interviewer:** You add the label to the Spark pods and it still times out. What now?

**Candidate:** After eliminating the label issue, I'd run a packet-level diagnosis. If using Cilium, `kubectl exec -n kube-system cilium-pod -- cilium monitor --type drop` shows real-time packet drops with the reason (Policy denied) and the policy name. If using Calico, `kubectl logs -n kube-system -l k8s-app=calico-node | grep -i deny` surfaces denied packets.

Alternatively, temporarily bypass the NetworkPolicy by running a test pod in the `kafka-prod` namespace itself: `kubectl run test -n kafka-prod --image=busybox -- nc -z schema-registry.kafka-prod.svc.cluster.local 8081`. If this succeeds but the `data-pipelines` pod fails, the NetworkPolicy is definitely the blocking layer. If even this fails, the Schema Registry pod itself has an issue (not a NetworkPolicy problem).

One more check: verify the Schema Registry service DNS resolves correctly from the Spark pod: `kubectl exec -n data-pipelines spark-pod -- nslookup schema-registry.kafka-prod.svc.cluster.local`. If this hangs, the DNS egress policy might be the issue — pods can't resolve hostnames, so all connections fail as "timed out" on DNS resolution rather than TCP connection.

**Interviewer:** What's the difference in how GCP Firewall Rules handle priority vs AWS Security Groups?

**Candidate:** This is a fundamental architectural difference. AWS security groups have no priority — all rules are evaluated as a set, and if any rule allows the traffic, it passes. There are no Deny rules in security groups; the absence of an Allow is an implicit Deny. So in AWS, you never have rule conflicts — you just add more Allow rules, and they all apply simultaneously.

GCP firewall rules use explicit priority ordering (1–65535, lower = higher precedence) and support both ALLOW and DENY rules. When multiple rules could match a packet, the highest-priority rule (lowest number) wins. This creates the potential for conflict: a high-priority DENY rule can block traffic that a lower-priority ALLOW rule would permit. This is what happened in the production scenario where a `deny-all-internal` at priority 500 overrode `allow-postgres-from-airflow` at priority 1000.

The GCP model is more flexible — you can implement "block everything except specific services" by using a high-priority deny-all and lower-priority allows for specific ports. In AWS, you'd need NACLs (which support Deny) to achieve the same effect, since security groups only support Allow. The trade-off is that GCP priority conflicts are a real operational hazard: adding a new broad-deny rule without carefully reviewing priority ordering can break existing allowed connections.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the difference between `Connection timed out` and `Connection refused` in the context of security groups? | Timed out: packet was dropped (SG blocking, NACL blocking, routing issue). Refused: packet arrived but nothing listening (SG OK, service not running). Check VPC flow logs for REJECT entries to confirm SG blocking. |
| 2 | Are security groups stateful or stateless? What does this mean? | Stateful. If an inbound TCP connection is permitted, response traffic is automatically allowed without a matching outbound rule. Eliminates the need for bidirectional rules for a single connection. |
| 3 | What is the default inbound and outbound behavior of a new AWS security group? | Inbound: all denied (no rules). Outbound: all allowed (one default rule: all traffic to 0.0.0.0/0). Must explicitly add inbound rules for any service to receive connections. |
| 4 | Why are security group references preferred over CIDR sources for dynamic workloads? | SG references automatically apply to all instances with that SG, regardless of IP. With CIDR sources, every new subnet or instance requires a rule update. SG references work with auto-scaling. |
| 5 | What does an empty `podSelector: {}` mean in a Kubernetes NetworkPolicy? | Applies to ALL pods in the namespace. Used in default-deny policies to restrict traffic to/from all pods before specific allows are added. |
| 6 | In a Kubernetes NetworkPolicy `from` list, what is the difference between one element with both selectors vs two separate elements? | One element with both selectors = AND (must match both). Two separate elements = OR (must match either). This AND/OR distinction is the most common NetworkPolicy mistake. |
| 7 | What happens to DNS resolution when you apply default-deny egress in Kubernetes? | DNS breaks — pods can't reach CoreDNS on UDP/TCP 53 in kube-system. All hostnames become unresolvable. Always add an explicit DNS egress allow rule alongside default-deny egress. |
| 8 | What is the GCP equivalent of an AWS security group? | GCP Firewall Rules, but with priority ordering (lower number = higher priority) and support for DENY rules. AWS SGs are allow-only; GCP rules support both ALLOW and DENY. |
| 9 | What ports must be open between Kafka brokers for a healthy cluster? | TCP 9093 (inter-broker replication, if using INTERNAL listener). TCP 9092 for clients. TCP 2181 for ZooKeeper (legacy). If using KRaft: TCP 9093 for controller quorum. |
| 10 | What is a Network ACL and how does it differ from a security group? | NACL: subnet-level stateless firewall supporting Allow and Deny rules evaluated in numeric order. SG: instance-level stateful allow-only firewall with rules as a set. NACLs require explicit return traffic rules; SGs do not. |
| 11 | What does `sg_blocking=True` infer from a `Connection timed out` error? | That a security group (or NACL, or routing) silently dropped the packet. The SYN never got a response. VPC flow logs with REJECT confirm security group blocking. |
| 12 | What is the maximum number of security groups that can be attached to a single ENI in AWS? | 5 security groups per ENI (Elastic Network Interface). Each SG can have up to 60 inbound + 60 outbound rules, giving up to 300 inbound rules total per ENI. |
| 13 | Why is default-deny NetworkPolicy the recommended security posture in Kubernetes? | Without any policy, all pods communicate with all others. Default-deny means new pods start with no network access until explicitly permitted — safe failure mode. An forgotten NetworkPolicy means denied traffic, not accidentally open access. |
| 14 | What CNI plugins support Kubernetes NetworkPolicy enforcement? | Calico, Cilium, Weave, and Amazon VPC CNI with Calico. Flannel and Amazon VPC CNI alone do NOT enforce NetworkPolicies. Applying policies without an enforcement-capable CNI creates a false sense of security. |
| 15 | How do you find which SG is blocking Kafka traffic using VPC Flow Logs? | Query flow logs for `action = 'REJECT'` and `dstport = 9092`. The `srcaddr` shows which client is being blocked. Cross-reference with the SG attached to the Kafka broker's ENI to confirm which rule is absent. |
| 16 | What security group configuration allows Kafka brokers to replicate to each other? | An inbound rule on the Kafka broker SG allowing TCP 9093 (inter-broker listener port) from the Kafka broker SG itself (self-referencing). This allows any instance in the SG to reach other instances in the same SG on 9093. |
| 17 | What is the risk of opening all outbound traffic (0.0.0.0/0) on Kafka broker security groups? | Kafka brokers can initiate outbound connections to any destination — enabling a compromised broker to exfiltrate data. Restrict egress to: S3 VPC endpoint, Secrets Manager endpoint, and broker-to-broker traffic. |
| 18 | In GCP, what is the implied rule for ingress that exists in every VPC? | Implied deny all ingress at priority 65535 (lowest priority). Every VPC has this built-in rule, meaning all inbound traffic is denied unless explicit ALLOW rules exist at higher priority (lower number). |
| 19 | What is the Cilium CLI command to monitor real-time packet drops caused by NetworkPolicy? | `cilium monitor --type drop` — shows dropped packets with policy name, source/destination pod, and port. Essential for debugging NetworkPolicy issues without relying on application-level errors. |
| 20 | An EKS pod can connect to services in its own namespace but not to services in other namespaces. What is the likely NetworkPolicy issue? | A `podSelector` in the `from` rule without a `namespaceSelector` only allows traffic from the same namespace. Add a `namespaceSelector` with the target namespace's label to the same `from` element to create AND semantics allowing cross-namespace traffic. |

---

## 13. Further Reading

- **Kubernetes documentation — "Network Policies":** The authoritative reference for NetworkPolicy syntax, including the AND vs OR semantics of `from`/`to` lists with examples. The "Behavior of to and from selectors" section is essential reading.
- **AWS documentation — "Security groups for your VPC":** Covers the complete security group model including rule evaluation, statefulness, and the difference from NACLs. The "Comparison of security groups and network ACLs" table is particularly useful.
- **"Kubernetes Network Policies Best Practices" — Cilium documentation:** Practical guidance on designing NetworkPolicies for real applications, including the default-deny pattern, namespace isolation, and debugging with Cilium's eBPF-based tools.
- **"Deep Dive into AWS VPC Security" — AWS re:Invent sessions:** Multi-year sessions on VPC security groups, NACLs, and VPC flow logs. The 2022 and 2023 sessions cover modern MSK and EKS security group patterns.
- **Calico documentation — "Network Policy Tutorial":** Step-by-step exercises implementing NetworkPolicies with Calico, including how to verify policy enforcement and debug drops.
- **"A Practical Guide to Kubernetes Network Policy":** Multiple blog posts by Ahmet Alp Balkan and others on the common NetworkPolicy pitfalls (AND vs OR, missing DNS egress, empty selectors). Search for "kubernetes networkpolicy and or semantics" for the most-cited explanations.

---

## 14. Lab Exercises

**Exercise 1: Security Group Connectivity Matrix**

Create a spreadsheet (or Python dict) representing the security group configuration for a 5-component data platform: Kafka brokers, Spark workers, Airflow workers, Schema Registry, Postgres. For each pair (source, destination), specify: which port, which SG reference, and whether the rule currently exists. Use the code toolkit's `check_tcp_connectivity` to verify each connection in a test environment. Identify which rules are missing.

**Exercise 2: Reproduce "Timed Out" vs "Refused"**

In an AWS VPC with a running service (or in Docker locally): (1) Stop the service but keep the port open in the security group — observe `Connection refused`. (2) Block the port in the security group (or iptables) while the service is running — observe `Connection timed out`. Document the exact error messages from `nc`, `curl`, and Python's `socket.connect` for each case.

**Exercise 3: NetworkPolicy AND vs OR**

In a test Kubernetes cluster (minikube works), apply the two NetworkPolicy variants from Section 3.4 (AND vs OR semantics). Deploy test pods with and without the matching labels. Verify that AND semantics blocks pods that match only one of the two conditions, while OR semantics allows them. Use `kubectl exec -- nc -z` to test connectivity.

**Exercise 4: Default Deny in Kubernetes**

Apply the default-deny-all policy to a test namespace. Verify that: (1) existing pods can no longer connect to anything, (2) pods can't resolve DNS names (which breaks all service connectivity). Then add the DNS egress rule and verify DNS works. Then add a specific service egress rule and verify that service is reachable while others remain blocked.

**Exercise 5: VPC Flow Log Analysis**

Enable VPC Flow Logs on a test VPC (or use an existing production VPC flow log). Intentionally connect to a port that is blocked by a security group. Query the flow logs (via Athena or CloudWatch Insights) to find the REJECT entry. Extract: source IP, destination IP, destination port, and timestamp. Cross-reference with the instance's security group to identify which rule is missing.

---

## 15. Key Takeaways

Security groups and NetworkPolicies are the network firewall layer that complements IAM's identity-based access control. Together they implement defense in depth: IAM controls what an authenticated identity can do; security groups and NetworkPolicies control whether network traffic can even reach a service for authentication.

The three most important operational facts: `Connection timed out` means the packet was dropped (diagnose with VPC flow logs looking for REJECT); `Connection refused` means the packet arrived but nothing was listening (diagnose at the service level, not the security group level). Second, security group references are superior to CIDR rules for dynamic environments — they automatically apply to new instances without rule updates. Third, Kubernetes NetworkPolicy AND vs OR semantics is the single most common source of "why is my traffic still blocked?" confusion — within a `from` element, selectors are AND; separate elements are OR.

The Kubernetes default-deny pattern is the correct security posture for production namespaces, but it requires remembering that DNS egress (UDP/TCP 53 to kube-system) must be explicitly allowed alongside any egress-restricting NetworkPolicy, or all service DNS resolution breaks silently.

---

## 16. Connections to Other Modules

- **M27 — TCP/IP Fundamentals:** Security groups operate at the TCP layer — they evaluate the same fields as TCP/IP headers (source IP, destination IP, protocol, port). The SYN packet is what security groups evaluate on connection initiation. Understanding TCP state machines explains why stateful SGs can track connections.
- **M32 — VPC Architecture:** Security groups live within VPCs. Public subnets require security groups that restrict inbound access (only ALB/NLB should accept internet traffic). Private subnets restrict inbound to VPC-internal sources only.
- **M33 — IAM and RBAC:** IAM and security groups are complementary. Security groups block or allow network access; IAM allows or denies API actions. A Spark job needs both: security group that allows TCP 9092 to Kafka, AND an IAM role with Kafka API permissions (for MSK IAM auth) or mTLS client cert (for SSL auth).
- **M35 — Private Endpoints and VPC Service Controls:** VPC Service Controls work above security groups to restrict which GCP service accounts can access which services from which networks. SecurityGroups handle TCP-level access; VPC SC handles API-level exfiltration prevention.
- **M31 — Load Balancing and Proxies:** Load balancers are the entry point for external traffic through security groups. The ALB/NLB security group is the only SG with inbound rules allowing external traffic; all backend security groups only allow traffic from the LB SG.

---

## 17. Anti-Patterns

**Anti-pattern: One mega security group for all data platform components.** A single "data-platform" SG on all instances that allows all ports between all instances in the group defeats the purpose of security groups entirely. If a Spark executor is compromised, it can reach Kafka's internal admin port (9999), Postgres, and ZooKeeper with no network restriction. Use one SG per service role.

**Anti-pattern: Copying security group rules without understanding them.** When a new service fails to connect, the quickest "fix" is often `--cidr 0.0.0.0/0` or adding a `0-65535` port range. This opens the service to the entire internet or internal network indiscriminately. Every security group rule addition should be reviewed: which specific source, which specific port, what service and why.

**Anti-pattern: Forgetting to update security groups when subnets or services change.** Security groups with CIDR rules for `10.0.10.0/24` (old Spark subnet) must be updated when Spark moves to `10.0.30.0/24` (new subnet). Using SG references (`sg-spark-workers`) avoids this problem entirely, but CIDR rules are common in legacy configurations.

**Anti-pattern: Not having a dedicated "monitoring" security group.** Prometheus scrapes metrics from JMX exporter (port 9999 for Kafka), node exporter (port 9100), and application metrics endpoints. These ports should only be accessible from the monitoring SG, not from the general Kafka client SG. Without a monitoring SG, either monitoring breaks (if ports are not open) or internal ports are unnecessarily exposed to all clients.

**Anti-pattern: Applying Kubernetes NetworkPolicies without verifying CNI enforcement.** Applying `kubectl apply -f networkpolicy.yaml` when the CNI doesn't support NetworkPolicy (e.g., Flannel) gives the appearance of security with none of the enforcement. Always verify: `kubectl get pods -n kube-system | grep calico` or `cilium` before relying on NetworkPolicies for security.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `aws ec2 describe-security-groups` | List SG rules | `--group-ids sg-xxx --query 'SecurityGroups[0].IpPermissions'` |
| `aws ec2 authorize-security-group-ingress` | Add inbound rule | `--source-group sg-xxx --protocol tcp --port 9092` |
| `aws ec2 revoke-security-group-ingress` | Remove inbound rule | Same syntax as authorize |
| `nc -z -v HOST PORT` | Test TCP connectivity | `-z` = scan, `-v` = verbose. Timeout=SG block; refused=service down |
| `timeout 3 bash -c "echo > /dev/tcp/HOST/PORT"` | Quick TCP test in bash | Exit 0=connected; non-zero=failed |
| `kubectl get networkpolicies -n NAMESPACE` | List NetworkPolicies | `-o wide` for selector details |
| `kubectl describe networkpolicy POLICY -n NS` | Inspect NetworkPolicy | Shows full ingress/egress rules |
| `cilium monitor --type drop` | Real-time packet drops | In a pod: `kubectl exec -n kube-system cilium-pod -- cilium monitor --type drop` |
| `kubectl exec POD -- nc -z HOST PORT` | Test from inside pod | Essential for diagnosing pod-level connectivity |
| `gcloud compute firewall-rules list` | List GCP firewall rules | `--filter="network=..."` |
| `gcloud compute firewall-rules create` | Create GCP firewall rule | `--rules=tcp:9092 --source-tags=kafka-client --target-tags=kafka-broker` |
| Athena on VPC Flow Logs | Analyze REJECT entries | `WHERE action='REJECT' AND dstport=9092` |

---

## 19. Glossary

**ACCEPT (Flow Log):** A VPC flow log action indicating the packet was permitted by the security group or NACL. Does not mean the packet arrived or the connection succeeded — only that it wasn't dropped by the firewall.

**Calico:** A Kubernetes CNI plugin that supports NetworkPolicy enforcement using iptables. More common in older clusters. Performance degrades with many policies due to O(N) iptables rule evaluation.

**Cilium:** A Kubernetes CNI plugin that implements NetworkPolicy using eBPF. O(1) rule evaluation. Provides richer visibility via `cilium monitor`. Preferred for high-throughput clusters.

**CNI (Container Network Interface):** The plugin layer in Kubernetes responsible for pod networking and NetworkPolicy enforcement. Different CNI plugins have different capabilities — not all enforce NetworkPolicies.

**Connection Refused:** TCP RST received. The packet reached the destination but no process was listening on the port. Security group is NOT the issue. Check if the service is running.

**Connection Timed Out:** No response to TCP SYN. The packet was dropped before reaching the destination. Security group, NACL, routing issue, or host completely down. Check VPC flow logs for REJECT.

**Default Deny:** A security posture where all traffic is denied unless explicitly permitted. In Kubernetes, implemented via a NetworkPolicy with empty podSelector and no rules. In AWS, the default behavior of security groups (no rules = all inbound denied).

**Elastic Network Interface (ENI):** A virtual network card in AWS. Security groups are attached to ENIs (not directly to instances, though the console shows them on instances). An EC2 instance can have multiple ENIs with different security groups.

**GCP Firewall Rule:** GCP's network-level access control. Unlike AWS SGs, supports both ALLOW and DENY actions with priority ordering. Applied to instances by network tags or service accounts.

**Network ACL (NACL):** AWS subnet-level stateless firewall. Supports both Allow and Deny rules evaluated in numerical order. Return traffic must be explicitly allowed (ephemeral ports).

**NetworkPolicy:** A Kubernetes resource specifying allowed ingress/egress traffic for pods. Enforced by the CNI plugin. Default-allow (no policies = all traffic allowed); becomes default-deny per-pod when any selecting policy exists.

**REJECT (Flow Log):** A VPC flow log action indicating the packet was dropped by a security group or NACL. The source never receives a TCP RST — the connection appears to time out.

**Security Group:** AWS instance-level stateful firewall. Allow-only rules (no Deny). Stateful: return traffic automatically allowed. Rules evaluated as a set (any matching Allow = permitted). Up to 5 per ENI.

**Security Group Reference:** A security group rule where the source is another security group ID rather than a CIDR. All instances with the referenced SG attached are covered by the rule, regardless of IP address. Preferred over CIDR for dynamic environments.

---

## 20. Self-Assessment

1. A Spark job on an EMR cluster fails to connect to Kafka on port 9092 with `Connection timed out`. List the first three things you check and the exact commands you run.
2. Explain why security group references are preferred over CIDR-based rules for a Kafka cluster that auto-scales Spark workers.
3. Write the AWS CLI command to add an inbound rule allowing TCP port 5432 from security group `sg-airflow-workers-abc123` to security group `sg-postgres-prod-def456`, with description "Airflow metadata DB."
4. What happens to pod DNS resolution when you apply a NetworkPolicy with `policyTypes: [Egress]` and no egress rules? How do you fix it?
5. Explain the AND vs OR semantics of Kubernetes NetworkPolicy `from` lists. Give an example where confusing them would block traffic you intended to allow.
6. A GCP firewall rule with priority 500 denies all internal traffic. A separate rule with priority 1000 allows TCP 5432 from Airflow workers to Postgres. What happens, and how do you fix it?
7. What two conditions does a VPC flow log REJECT entry reveal about a security group problem?
8. How would you configure a Kafka broker security group to allow inter-broker replication on port 9093 without exposing 9093 to any external clients?
9. Write the Kubernetes NetworkPolicy that allows only pods with label `role: spark-executor` in namespace `data-transformations` to reach pods with label `app: schema-registry` in namespace `kafka-prod` on port 8081.
10. What is the CNI plugin risk when deploying Kubernetes NetworkPolicies, and how do you verify enforcement is active?

---

## 21. Module Summary

Security groups and Kubernetes NetworkPolicies are the network firewall controls that enforce perimeter isolation within a cloud VPC and within a Kubernetes cluster. They complement IAM (identity-based access control) by adding network-based access control: IAM determines what authenticated identities can do; security groups and NetworkPolicies determine what network traffic is even allowed to reach a service.

The foundational operational skill is correctly diagnosing `Connection timed out` versus `Connection refused`. Timed out means the packet was dropped — diagnose with VPC flow logs looking for REJECT entries, then check which security group rule or NACL is missing. Refused means the packet arrived but nothing was listening — the security group is fine, and the problem is at the service layer (process not running, wrong port).

AWS security groups are allow-only, stateful, and instance-level. Security group references (specifying another SG as the source) are preferred over CIDR references for dynamic cloud workloads — auto-scaling groups get new IPs but retain their security group, so SG-reference rules work without updates. One security group per service role (sg-kafka-brokers, sg-spark-workers, sg-postgres) provides both security isolation and operational clarity.

Kubernetes NetworkPolicies add pod-level isolation within a cluster. The critical design pattern is default-deny-all in each namespace, followed by explicit allow rules for required communication paths. The AND vs OR semantics of `from`/`to` lists is the most common source of NetworkPolicy debugging pain: selectors in the same list element are AND; selectors in separate list elements are OR. And DNS egress must always be explicitly permitted when default-deny egress is applied, or all hostname resolution breaks.

GCP firewall rules differ from AWS security groups in supporting both ALLOW and DENY actions with priority ordering — a powerful feature that also introduces the risk of priority conflicts, where a high-priority deny rule overrides intended allow rules.

The next module — M35: Private Endpoints and VPC Service Controls — completes the cloud networking security picture by addressing data exfiltration prevention: how to ensure that data cannot leave an organization's cloud environment even through technically valid API calls.
