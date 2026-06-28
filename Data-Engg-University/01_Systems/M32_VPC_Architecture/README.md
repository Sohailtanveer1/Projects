# M32: VPC Architecture

**Course:** SYS-NET-102 — Cloud Networking and Security  
**Module:** 01 of 05  
**Global Module ID:** M32  
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

Every cloud data platform — whether it is a Kafka cluster on EC2, a Spark job on EMR, a BigQuery pipeline, or a Kubernetes cluster on EKS — runs inside a Virtual Private Cloud. The VPC is the network boundary within which these components live, communicate, and are isolated from the rest of the internet. A data engineer who does not understand VPC architecture cannot understand why Spark executors can talk to each other but not to S3 (without a VPC endpoint), why a Kafka broker in a private subnet cannot receive external traffic (without a NAT gateway or load balancer), or why cross-region replication costs money.

The VPC is also the security perimeter. In cloud environments, the network is not a given — it is configured. Data exfiltration, where data leaves an organization's cloud account without authorization, typically exploits VPC misconfigurations: a public subnet where a private one should be, a missing VPC Service Control, a NAT gateway that routes data to an unexpected external destination. Understanding VPC architecture is therefore both a performance and a security concern.

For data engineers specifically, VPC design decisions have concrete implications:

**S3 and GCS egress costs.** Without a VPC endpoint for S3, traffic from Spark or Kafka to S3 routes through the public internet via a NAT gateway, incurring NAT gateway data processing charges ($0.045/GB in us-east-1 as of this writing) in addition to S3 transfer costs. A Spark job reading 10 TB from S3 without a VPC endpoint can cost hundreds of dollars in NAT fees alone. A Gateway VPC endpoint for S3 routes this traffic privately at no additional cost.

**Cross-AZ bandwidth costs.** AWS charges $0.01/GB for cross-AZ data transfer. A Kafka cluster with brokers in three AZs replicates every write to all replicas, crossing AZ boundaries. With 100 MB/s throughput and replication factor 3, cross-AZ bandwidth is 200 MB/s (two additional replicas) = 720 GB/hour = $7.20/hour = $174/day in cross-AZ transfer fees alone. AZ placement of data components is therefore a significant cost lever.

**VPC peering for multi-account architectures.** Large data engineering organizations separate their data platform into multiple AWS accounts (for security isolation, blast radius reduction, and cost attribution). A data pipeline that reads from a "source" account's database and writes to a "platform" account's data lake must traverse a VPC peering connection between the two accounts. Peering has capacity and routing constraints (non-transitive — you cannot route through an intermediate VPC to reach a third VPC) that affect architecture decisions.

Understanding VPC architecture is prerequisite to understanding everything else in SYS-NET-102. The subsequent modules — IAM, security groups, private endpoints, and DNS — all depend on knowing what a VPC is, how its subnets are organized, and how traffic flows within and between them.

---

## 2. Mental Model

### The VPC as a Software-Defined Data Center

A VPC is a logically isolated section of a cloud provider's network, dedicated to your account. It behaves like a private data center network that you control entirely. Within the VPC, you define:

- The IP address space (CIDR block): e.g., `10.0.0.0/16` (65,536 IP addresses)
- How that space is subdivided (subnets): e.g., `10.0.1.0/24` for AZ-a, `10.0.2.0/24` for AZ-b
- How traffic flows (route tables): rules that say "traffic destined for X goes to Y"
- What enters and exits (internet gateways, NAT gateways, VPC endpoints)

The fundamental mental model: a VPC is a **network namespace**. Everything inside it can communicate by default (subject to security groups). Nothing from outside can reach in unless you explicitly provision an entry point. Nothing from inside can reach the internet unless you explicitly provision an exit point.

### Public Subnets vs Private Subnets

The most important VPC concept for data engineers is the public/private subnet distinction:

**Public subnet:** A subnet whose route table has a route to an Internet Gateway (IGW). Resources in a public subnet can have public IPs and receive inbound traffic from the internet. Typically used for: NAT gateways, load balancers, bastion hosts. **NOT** for Kafka brokers, Spark nodes, databases, or any data component.

**Private subnet:** A subnet whose route table has no direct route to an IGW. Resources cannot be reached from the internet directly. Outbound internet access (for software updates, external API calls) goes via a NAT gateway in the public subnet. All data components belong here: Kafka brokers, Spark workers, Postgres databases, Redis, schema registries.

The rule is simple: if it holds data or processes data, it goes in a private subnet. If it needs to accept inbound internet traffic, it goes in a public subnet. Load balancers in the public subnet receive internet traffic and forward to data components in private subnets.

### Routing Table: The Traffic Director

Every subnet in a VPC is associated with a route table. The route table is a list of rules: "for traffic destined for CIDR X, send it to Y." Route tables are evaluated in order from most specific (longest prefix match) to least specific.

A typical private subnet route table:
```
Destination        Target
10.0.0.0/16        local            ← all VPC-internal traffic stays local
0.0.0.0/0          nat-xxxxxxxx     ← all other traffic goes to NAT gateway
```

A typical public subnet route table:
```
Destination        Target
10.0.0.0/16        local
0.0.0.0/0          igw-xxxxxxxx     ← internet-bound traffic goes to Internet Gateway
```

The `local` route is implicit and cannot be removed — it ensures all VPC-internal traffic stays within the VPC without leaving.

---

## 3. Core Concepts

### 3.1 CIDR Blocks and IP Address Planning

Every VPC is defined by a CIDR block (Classless Inter-Domain Routing). The CIDR block determines the range of IP addresses available within the VPC.

**CIDR notation:** `10.0.0.0/16` means: start at IP `10.0.0.0`, and the first 16 bits are fixed (the "network" bits). The remaining 16 bits are free for assignment — giving `2^16 = 65,536` IP addresses in the range `10.0.0.0` to `10.0.255.255`.

**Private RFC 1918 ranges** (only these should be used for VPCs):
```
10.0.0.0/8       →  16,777,216 addresses  (10.0.0.0 – 10.255.255.255)
172.16.0.0/12    →   1,048,576 addresses  (172.16.0.0 – 172.31.255.255)
192.168.0.0/16   →      65,536 addresses  (192.168.0.0 – 192.168.255.255)
```

**Planning for scale:** Choose VPC CIDR sizes that accommodate future growth. A `/16` VPC (65,536 IPs) is a reasonable default for a data platform. Avoid `/24` VPCs — they provide only 256 IPs, which is insufficient for a Kubernetes cluster with 100+ pods (each pod gets its own IP in EKS/GKE).

**Subnet sizing:** Subnets carve up the VPC CIDR. A `/24` subnet within a `/16` VPC gives 256 IPs (251 usable — cloud providers reserve 5). For a Kafka cluster with 3 brokers per AZ × 3 AZs, a `/27` subnet (32 IPs, 27 usable) is sufficient. For an EKS node group that might scale to 100 nodes, each with 30 pods, you need `100 × 30 = 3,000` IPs minimum — a `/20` subnet (4,096 IPs).

**Non-overlapping CIDR planning (multi-VPC):** When multiple VPCs need to communicate via VPC peering or AWS Transit Gateway, their CIDR blocks must not overlap. A common pattern:
```
Production VPC:      10.0.0.0/16    (10.0.0.0 – 10.0.255.255)
Development VPC:     10.1.0.0/16    (10.1.0.0 – 10.1.255.255)
Data Platform VPC:   10.2.0.0/16    (10.2.0.0 – 10.2.255.255)
```

### 3.2 Subnets and Availability Zone Placement

A subnet exists within a single Availability Zone (AZ). Resources in the subnet are physically located (within a building/cluster of data centers) in that AZ. For high availability, data components must span multiple AZs:

**The standard 3-AZ layout for a data platform:**
```
VPC: 10.0.0.0/16

Public subnets:
  us-east-1a:  10.0.0.0/24   (NAT gateway, ALB)
  us-east-1b:  10.0.1.0/24   (NAT gateway, ALB)
  us-east-1c:  10.0.2.0/24   (NAT gateway, ALB)

Private subnets (data):
  us-east-1a:  10.0.10.0/23  (Kafka brokers, Spark, Postgres)
  us-east-1b:  10.0.12.0/23
  us-east-1c:  10.0.14.0/23

Private subnets (Kubernetes):
  us-east-1a:  10.0.20.0/22  (EKS nodes + pods)
  us-east-1b:  10.0.24.0/22
  us-east-1c:  10.0.28.0/22
```

**Why separate Kubernetes subnets:** EKS with the VPC CNI plugin assigns each pod an IP from the VPC subnet. A cluster of 50 nodes × 30 pods each = 1,500 pod IPs needed. This exhausts a `/24` (254 usable IPs) quickly. Kubernetes subnets need to be large (`/22` = 1,024 IPs, or `/21` = 2,048 IPs).

**AZ failure isolation:** If AZ us-east-1a fails, resources in us-east-1b and us-east-1c must continue operating. This requires:
- Kafka: at least one ISR replica in each of the surviving AZs
- Spark: executors can be rescheduled on nodes in surviving AZs
- EKS: node groups distributed across all three AZs
- Postgres: primary in one AZ, standby in another (synchronous replication)

### 3.3 Internet Gateway and NAT Gateway

**Internet Gateway (IGW):** A horizontally-scaled, redundant, highly available VPC component that allows communication between resources in public subnets and the internet. One IGW per VPC. Performs NAT for instances with public IP addresses (maps private IP ↔ public IP for traffic entering and leaving via the IGW). No bandwidth limit or per-GB charge beyond standard data transfer rates.

**NAT Gateway:** A managed network address translation device in a public subnet that allows resources in private subnets to initiate outbound connections to the internet, while preventing inbound connections from the internet. Resources in private subnets use the NAT gateway as their default gateway.

NAT gateway pricing (AWS, us-east-1):
- Hourly charge: $0.045/hour (~$32/month per NAT gateway)
- Data processing: $0.045/GB of data processed

**One NAT gateway per AZ:** A common cost-cutting mistake is using a single NAT gateway in one AZ for resources across all AZs. Traffic from resources in AZ-b to the NAT gateway in AZ-a crosses AZ boundaries, incurring cross-AZ bandwidth charges ($0.01/GB) on top of NAT gateway processing charges. For a data-intensive workload, per-AZ NAT gateways are more cost-effective despite the additional hourly cost.

**Why data engineers care about NAT gateways:**
```
Spark executor in private subnet → S3
Without VPC endpoint: executor → NAT gateway ($0.045/GB) → internet → S3
With VPC endpoint:    executor → VPC endpoint (free) → S3 directly

At 10 TB/day: $0.045 × 10,000 GB = $450/day in NAT charges avoided by a VPC endpoint
```

### 3.4 VPC Peering

VPC peering creates a direct network connection between two VPCs, allowing resources in each VPC to communicate using private IPs as if they were in the same network. Traffic stays within the AWS backbone (does not traverse the public internet).

**Properties of VPC peering:**
- **Non-transitive:** If VPC A peers with VPC B and VPC B peers with VPC C, VPC A cannot communicate with VPC C through VPC B. You need a direct peering between A and C.
- **Cross-account:** VPC peering works between VPCs in different AWS accounts (common in multi-account architectures).
- **Cross-region:** VPC peering works between VPCs in different regions (inter-region peering), but incurs inter-region data transfer charges ($0.02–$0.09/GB depending on regions).
- **No overlapping CIDRs:** The two VPCs must have non-overlapping CIDR blocks.

**When to use VPC peering:** Connecting a production data pipeline to a data warehouse, where they are in separate accounts for security isolation. The pipeline VPC (source account) peers with the data platform VPC (platform account) to allow Spark to read from the source and write to the platform.

**Non-transitivity example:**
```
Source Account VPC (10.1.0.0/16)  ←peer→  Platform VPC (10.2.0.0/16)
                                   ←peer→  Analytics VPC (10.3.0.0/16)

Source Account VPC CANNOT reach Analytics VPC (10.3.0.0/16)
without a direct peering between them.
```

### 3.5 AWS Transit Gateway

Transit Gateway (TGW) is a network transit hub that connects multiple VPCs and on-premises networks. It solves the non-transitivity problem of VPC peering at the cost of additional per-GB fees.

**When to use Transit Gateway vs VPC peering:**
- Fewer than 5–10 VPCs with simple connectivity → VPC peering (lower cost, simpler)
- Many VPCs with complex connectivity requirements → Transit Gateway (hub-and-spoke model, transitive routing)
- On-premises connectivity → Transit Gateway (attaches to VPN or Direct Connect)

**TGW pricing (us-east-1):** $0.05/GB processed (more expensive than peering for high-volume connections). For data-intensive connections (e.g., replicating terabytes between VPCs), calculate whether TGW vs peering is more cost-effective.

### 3.6 VPC Endpoints

A VPC endpoint allows private connectivity between a VPC and AWS services without routing traffic through the public internet, NAT gateway, or VPN. Critical for cost and security.

**Gateway endpoints (free):** Available for S3 and DynamoDB only. The endpoint appears in the route table as a route for the S3/DynamoDB CIDR range, directing traffic through the private AWS network. No charge for the endpoint or the data processed through it. Every production VPC that accesses S3 should have a Gateway endpoint.

**Interface endpoints (paid):** Create an Elastic Network Interface (ENI) in your subnet with a private IP. DNS for the service (e.g., `sqs.us-east-1.amazonaws.com`) resolves to this private IP within the VPC. Supports most AWS services: SQS, SNS, Secrets Manager, SSM, ECR, CloudWatch, Kafka MSK, Glue, etc.

Interface endpoint pricing: $0.01/hour per AZ ($7.20/month per AZ) + $0.01/GB. For a service that processes 100 GB/day, a single-AZ interface endpoint costs: $7.20 + $30 = $37.20/month. Compare to routing through NAT gateway: $32 (hourly) + $135 (data processing) = $167/month — the endpoint is 4× cheaper.

**PrivateLink:** The technology underpinning interface endpoints. Also used by SaaS vendors to expose their services inside a customer's VPC without peering. Confluent Cloud, Snowflake, and Databricks all support PrivateLink, allowing a data platform to access these services via private IPs within the customer's VPC.

### 3.7 VPC Flow Logs

VPC Flow Logs capture metadata about IP traffic going to and from network interfaces in the VPC. They do NOT capture packet contents — only header information (source IP, destination IP, port, protocol, bytes, action ACCEPT/REJECT).

**What flow logs capture:**
```
version account-id interface-id srcaddr dstaddr srcport dstport protocol packets bytes start end action log-status
2 123456789012 eni-abc123 10.0.1.5 10.0.2.47 54321 9092 6 10 5000 1640000000 1640000060 ACCEPT OK
```

**Uses for data engineers:**
- Detecting traffic to unexpected destinations (data exfiltration investigation)
- Diagnosing security group blocks (REJECT entries)
- Identifying high-volume connections (bytes field) for cost attribution
- Auditing which services access which databases

Flow logs incur storage costs (to S3 or CloudWatch Logs) and can generate enormous volumes in busy VPCs. For large clusters, sampling or using Traffic Mirroring on specific ENIs is more cost-effective than full VPC flow logging.

### 3.8 GCP VPC Architecture

Google Cloud Platform's VPC architecture differs from AWS in important ways that matter for data engineers using BigQuery, Dataproc, or GKE:

**Global VPC:** In GCP, a VPC is a global resource — a single VPC spans all regions. Subnets are regional (associated with a region, not a zone). This contrasts with AWS where a VPC is regional and subnets are per-AZ.

**GCP subnet:** A subnet in GCP is regional (covers all zones in a region). Resources in `us-central1-a` and `us-central1-b` can share the same subnet. Traffic within the same region is free regardless of zone (no cross-AZ charges like AWS).

**Shared VPC:** GCP's equivalent of multi-account VPC sharing. A "host project" owns the VPC, and "service projects" attach to it. Resources in service projects (e.g., Dataproc clusters, GKE nodes) use the host project's VPC subnets. This allows centralized network management while distributing workloads across projects.

**Private Google Access:** GCP's equivalent of AWS's VPC endpoints for Google services. When enabled on a subnet, VMs without external IPs can access Google APIs and services (BigQuery, GCS, Pub/Sub) using internal routing without traversing the public internet. Enabled per subnet via a subnet flag: `--enable-private-ip-google-access`.

---

## 4. Hands-On Walkthrough

### 4.1 Inspecting a VPC Configuration with AWS CLI

```bash
# List all VPCs in the region
aws ec2 describe-vpcs --output table \
  --query 'Vpcs[*].[VpcId,CidrBlock,Tags[?Key==`Name`].Value|[0],IsDefault]'

# List subnets and their AZ placement
aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=vpc-0123456789abcdef0" \
  --query 'Subnets[*].[SubnetId,CidrBlock,AvailabilityZone,
            Tags[?Key==`Name`].Value|[0],MapPublicIpOnLaunch]' \
  --output table

# Show route tables for a VPC
aws ec2 describe-route-tables \
  --filters "Name=vpc-id,Values=vpc-0123456789abcdef0" \
  --query 'RouteTables[*].{ID:RouteTableId,Routes:Routes[*].{Dest:DestinationCidrBlock,
            Target:GatewayId||NatGatewayId||TransitGatewayId||VpcPeeringConnectionId}}' \
  --output json

# Check if a subnet has a route to the internet (is it public?)
aws ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=subnet-0123456789abcdef0" \
  --query 'RouteTables[0].Routes[?DestinationCidrBlock==`0.0.0.0/0`].GatewayId' \
  --output text
# igw-* = public subnet; nat-* = private subnet; nothing = isolated subnet

# List NAT gateways and their public IPs
aws ec2 describe-nat-gateways \
  --filter "Name=state,Values=available" \
  --query 'NatGateways[*].[NatGatewayId,SubnetId,
            NatGatewayAddresses[0].PublicIp,NatGatewayAddresses[0].PrivateIp]' \
  --output table

# List VPC endpoints
aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=vpc-0123456789abcdef0" \
  --query 'VpcEndpoints[*].[VpcEndpointId,ServiceName,VpcEndpointType,State]' \
  --output table

# List VPC peering connections
aws ec2 describe-vpc-peering-connections \
  --query 'VpcPeeringConnections[*].[VpcPeeringConnectionId,
            AccepterVpcInfo.VpcId,RequesterVpcInfo.VpcId,Status.Code]' \
  --output table
```

### 4.2 Checking if S3 Traffic Goes Through NAT (Cost Audit)

```bash
# If there is NO VPC endpoint for S3, traffic goes through NAT
# Check VPC endpoints for S3
aws ec2 describe-vpc-endpoints \
  --filters "Name=service-name,Values=com.amazonaws.us-east-1.s3" \
            "Name=vpc-id,Values=vpc-0123456789abcdef0" \
  --query 'VpcEndpoints[*].[ServiceName,State]' \
  --output text

# If no output → no S3 VPC endpoint → S3 traffic goes through NAT gateway
# Estimate daily cost from CloudWatch NAT metrics:
aws cloudwatch get-metric-statistics \
  --namespace AWS/NATGateway \
  --metric-name BytesOutToDestination \
  --dimensions Name=NatGatewayId,Value=nat-0123456789abcdef0 \
  --start-time $(date -u -d '24 hours ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 86400 \
  --statistics Sum \
  --query 'Datapoints[0].Sum'
# Divide by 1_073_741_824 to get GB; multiply by $0.045 for NAT cost
```

### 4.3 Verifying Cross-AZ Traffic for Kafka

```bash
# From a Kafka broker, check which AZ it's in
aws ec2 describe-instances \
  --instance-ids i-0123456789abcdef0 \
  --query 'Reservations[0].Instances[0].Placement.AvailabilityZone'

# Fetch Kafka broker list and their AZ placement
# (assumes Kafka topic describes the brokers)
kafka-broker-api-versions.sh \
  --bootstrap-server kafka.prod.internal:9092 2>/dev/null | \
  grep 'hostname=' | awk -F'[=,]' '{print $2}'

# For each broker hostname, get the EC2 instance and its AZ
# This script maps broker hostnames to AZs:
for broker in kafka-0 kafka-1 kafka-2; do
  INSTANCE_ID=$(aws ec2 describe-instances \
    --filters "Name=private-dns-name,Values=${broker}.internal" \
    --query 'Reservations[0].Instances[0].InstanceId' \
    --output text)
  AZ=$(aws ec2 describe-instances \
    --instance-ids $INSTANCE_ID \
    --query 'Reservations[0].Instances[0].Placement.AvailabilityZone' \
    --output text)
  echo "$broker → $AZ"
done
```

### 4.4 Enabling VPC Flow Logs for Security Analysis

```bash
# Create an S3 bucket for flow logs
aws s3 mb s3://my-vpc-flow-logs-123456789012

# Enable flow logs to S3
aws ec2 create-flow-logs \
  --resource-type VPC \
  --resource-ids vpc-0123456789abcdef0 \
  --traffic-type ALL \
  --log-destination-type s3 \
  --log-destination arn:aws:s3:::my-vpc-flow-logs-123456789012 \
  --log-format '${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} ${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status}'

# After a few minutes, query flow logs with Athena:
# (Create the Athena table first, then:)
# Find all REJECT entries to a Kafka broker:
# SELECT srcaddr, dstaddr, dstport, action, SUM(bytes) as total_bytes
# FROM vpc_flow_logs
# WHERE dstaddr = '10.0.10.5'  -- Kafka broker IP
#   AND action = 'REJECT'
# GROUP BY srcaddr, dstaddr, dstport, action
# ORDER BY total_bytes DESC
# LIMIT 20;
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
vpc_diagnostics.py

VPC architecture diagnostic and audit toolkit for data engineers.
Analyzes subnet placement, routing, VPC endpoints, and estimates costs.

Dependencies: boto3 (pip install boto3)
              google-cloud-compute (optional, for GCP support)
"""

import os
import json
import time
from dataclasses import dataclass, field
from typing import Optional


# ─── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class SubnetInfo:
    subnet_id: str
    cidr_block: str
    availability_zone: str
    name: str
    is_public: bool      # has route to IGW
    available_ips: int
    total_ips: int
    map_public_ip: bool


@dataclass
class RouteTableInfo:
    route_table_id: str
    associated_subnets: list[str]
    routes: list[dict]   # [{destination, target}]
    has_igw_route: bool
    has_nat_route: bool
    has_s3_endpoint_route: bool


@dataclass
class VPCEndpointInfo:
    endpoint_id: str
    service_name: str
    endpoint_type: str   # Gateway or Interface
    state: str
    route_table_ids: list[str]   # for Gateway endpoints


@dataclass
class VPCCostEstimate:
    vpc_id: str
    nat_gateway_count: int
    nat_monthly_fixed_cost: float        # hourly × 24 × 30
    missing_s3_endpoint: bool
    estimated_s3_nat_cost_per_tb: float  # cost if S3 traffic goes through NAT
    cross_az_warnings: list[str]


# ─── AWS VPC Inspector ──────────────────────────────────────────────────────────

class VPCInspector:
    """
    Inspects VPC configuration using boto3.
    Requires AWS credentials (IAM role, instance profile, or env vars).
    """

    def __init__(self, region: str = None, profile: str = None):
        try:
            import boto3
            session = boto3.Session(
                region_name=region or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
                profile_name=profile,
            )
            self.ec2 = session.client('ec2')
            self.cloudwatch = session.client('cloudwatch')
        except ImportError:
            raise RuntimeError(
                "boto3 not installed. Run: pip install boto3"
            )

    def list_vpcs(self) -> list[dict]:
        """List all VPCs with their CIDR and Name tag."""
        paginator = self.ec2.get_paginator('describe_vpcs')
        vpcs = []
        for page in paginator.paginate():
            for vpc in page['Vpcs']:
                name = next(
                    (t['Value'] for t in vpc.get('Tags', [])
                     if t['Key'] == 'Name'), ''
                )
                vpcs.append({
                    'vpc_id': vpc['VpcId'],
                    'cidr': vpc['CidrBlock'],
                    'name': name,
                    'is_default': vpc['IsDefault'],
                    'state': vpc['State'],
                })
        return vpcs

    def get_subnets(self, vpc_id: str) -> list[SubnetInfo]:
        """Get all subnets in a VPC with public/private classification."""
        paginator = self.ec2.get_paginator('describe_subnets')
        subnets = []

        # First, build a map of subnet → is_public from route tables
        public_subnet_ids = self._find_public_subnets(vpc_id)

        for page in paginator.paginate(
            Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
        ):
            for s in page['Subnets']:
                name = next(
                    (t['Value'] for t in s.get('Tags', [])
                     if t['Key'] == 'Name'), ''
                )
                # Total IPs = 2^(32-prefix) - 5 (AWS reserves 5)
                prefix = int(s['CidrBlock'].split('/')[1])
                total = 2 ** (32 - prefix)
                subnets.append(SubnetInfo(
                    subnet_id=s['SubnetId'],
                    cidr_block=s['CidrBlock'],
                    availability_zone=s['AvailabilityZone'],
                    name=name,
                    is_public=s['SubnetId'] in public_subnet_ids,
                    available_ips=s['AvailableIpAddressCount'],
                    total_ips=total - 5,
                    map_public_ip=s.get('MapPublicIpOnLaunch', False),
                ))

        return sorted(subnets, key=lambda x: (x.availability_zone, x.cidr_block))

    def _find_public_subnets(self, vpc_id: str) -> set[str]:
        """Find subnets that have a route to an Internet Gateway."""
        public_ids = set()
        paginator = self.ec2.get_paginator('describe_route_tables')
        for page in paginator.paginate(
            Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
        ):
            for rt in page['RouteTables']:
                has_igw = any(
                    r.get('GatewayId', '').startswith('igw-')
                    for r in rt.get('Routes', [])
                    if r.get('DestinationCidrBlock') == '0.0.0.0/0'
                )
                if has_igw:
                    for assoc in rt.get('Associations', []):
                        if 'SubnetId' in assoc:
                            public_ids.add(assoc['SubnetId'])
        return public_ids

    def get_route_tables(self, vpc_id: str) -> list[RouteTableInfo]:
        """Get all route tables in a VPC with analysis."""
        paginator = self.ec2.get_paginator('describe_route_tables')
        tables = []

        for page in paginator.paginate(
            Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
        ):
            for rt in page['RouteTables']:
                routes = []
                has_igw = False
                has_nat = False
                has_s3_ep = False

                for r in rt.get('Routes', []):
                    dest = r.get('DestinationCidrBlock') or r.get(
                        'DestinationPrefixListId', '')
                    target = (r.get('GatewayId') or r.get('NatGatewayId') or
                              r.get('TransitGatewayId') or
                              r.get('VpcPeeringConnectionId') or
                              r.get('VpcEndpointId') or 'local')
                    routes.append({'destination': dest, 'target': target,
                                   'state': r.get('State', '')})
                    if r.get('GatewayId', '').startswith('igw-'):
                        has_igw = True
                    if r.get('NatGatewayId', ''):
                        has_nat = True
                    if r.get('VpcEndpointId', '') or \
                       r.get('DestinationPrefixListId', ''):
                        has_s3_ep = True  # prefix list = S3 gateway endpoint

                subnet_ids = [
                    a['SubnetId'] for a in rt.get('Associations', [])
                    if 'SubnetId' in a
                ]
                tables.append(RouteTableInfo(
                    route_table_id=rt['RouteTableId'],
                    associated_subnets=subnet_ids,
                    routes=routes,
                    has_igw_route=has_igw,
                    has_nat_route=has_nat,
                    has_s3_endpoint_route=has_s3_ep,
                ))

        return tables

    def get_vpc_endpoints(self, vpc_id: str) -> list[VPCEndpointInfo]:
        """Get all VPC endpoints."""
        paginator = self.ec2.get_paginator('describe_vpc_endpoints')
        endpoints = []
        for page in paginator.paginate(
            Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
        ):
            for ep in page['VpcEndpoints']:
                endpoints.append(VPCEndpointInfo(
                    endpoint_id=ep['VpcEndpointId'],
                    service_name=ep['ServiceName'],
                    endpoint_type=ep['VpcEndpointType'],
                    state=ep['State'],
                    route_table_ids=ep.get('RouteTableIds', []),
                ))
        return endpoints

    def get_nat_gateways(self, vpc_id: str) -> list[dict]:
        """Get all NAT gateways and their subnet/AZ placement."""
        paginator = self.ec2.get_paginator('describe_nat_gateways')
        nats = []
        for page in paginator.paginate(
            Filters=[
                {'Name': 'vpc-id', 'Values': [vpc_id]},
                {'Name': 'state', 'Values': ['available']},
            ]
        ):
            for ng in page['NatGateways']:
                subnet_resp = self.ec2.describe_subnets(
                    SubnetIds=[ng['SubnetId']]
                )
                az = subnet_resp['Subnets'][0]['AvailabilityZone'] \
                    if subnet_resp['Subnets'] else 'unknown'
                public_ip = next(
                    (a['PublicIp'] for a in ng.get('NatGatewayAddresses', [])
                     if 'PublicIp' in a), None
                )
                nats.append({
                    'nat_id': ng['NatGatewayId'],
                    'subnet_id': ng['SubnetId'],
                    'availability_zone': az,
                    'public_ip': public_ip,
                    'state': ng['State'],
                })
        return nats

    def estimate_costs(self, vpc_id: str) -> VPCCostEstimate:
        """
        Estimate monthly costs and identify cost optimization opportunities.
        """
        nat_gateways = self.get_nat_gateways(vpc_id)
        endpoints = self.get_vpc_endpoints(vpc_id)

        # Check for S3 Gateway endpoint
        has_s3_endpoint = any(
            'amazonaws' in ep.service_name and '.s3' in ep.service_name
            and ep.endpoint_type == 'Gateway'
            for ep in endpoints
        )

        # NAT gateway fixed cost: $0.045/hr × 24 × 30 per NAT
        nat_monthly_fixed = len(nat_gateways) * 0.045 * 24 * 30

        # Cost of routing 1 TB through NAT instead of S3 endpoint
        # $0.045/GB × 1024 GB = $46.08 per TB
        s3_nat_cost_per_tb = 0.045 * 1024 if not has_s3_endpoint else 0.0

        # Check AZ distribution of NAT gateways
        nat_azs = {ng['availability_zone'] for ng in nat_gateways}
        cross_az_warnings = []
        subnets = self.get_subnets(vpc_id)
        private_azs = {s.availability_zone for s in subnets if not s.is_public}
        missing_nat_azs = private_azs - nat_azs
        if missing_nat_azs and len(nat_gateways) > 0:
            cross_az_warnings.append(
                f"Private subnets in {missing_nat_azs} route through NAT in "
                f"{nat_azs}. This incurs cross-AZ traffic charges ($0.01/GB). "
                f"Consider adding a NAT gateway in each AZ."
            )

        return VPCCostEstimate(
            vpc_id=vpc_id,
            nat_gateway_count=len(nat_gateways),
            nat_monthly_fixed_cost=nat_monthly_fixed,
            missing_s3_endpoint=not has_s3_endpoint,
            estimated_s3_nat_cost_per_tb=s3_nat_cost_per_tb,
            cross_az_warnings=cross_az_warnings,
        )


# ─── CIDR Utility Functions ──────────────────────────────────────────────────────

def parse_cidr(cidr: str) -> tuple[str, int]:
    """Parse CIDR into (network_address, prefix_length)."""
    ip, prefix = cidr.split('/')
    return ip, int(prefix)


def cidr_to_range(cidr: str) -> tuple[int, int]:
    """Convert CIDR to (first_ip_int, last_ip_int)."""
    ip, prefix = parse_cidr(cidr)
    parts = [int(p) for p in ip.split('.')]
    ip_int = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]
    mask = (0xFFFFFFFF << (32 - prefix)) & 0xFFFFFFFF
    network = ip_int & mask
    broadcast = network | (~mask & 0xFFFFFFFF)
    return network, broadcast


def cidrs_overlap(cidr1: str, cidr2: str) -> bool:
    """Return True if two CIDR blocks overlap (would prevent VPC peering)."""
    s1, e1 = cidr_to_range(cidr1)
    s2, e2 = cidr_to_range(cidr2)
    return not (e1 < s2 or e2 < s1)


def usable_ips(cidr: str) -> int:
    """Return the number of usable IPs in a CIDR (subtract 5 for AWS reservations)."""
    _, prefix = parse_cidr(cidr)
    return max(0, 2 ** (32 - prefix) - 5)


def check_peering_compatibility(vpcs: list[dict]) -> list[str]:
    """
    Check if a list of VPCs (each with a 'cidr' field) can be fully peered.
    Returns a list of conflict descriptions.
    """
    conflicts = []
    for i in range(len(vpcs)):
        for j in range(i + 1, len(vpcs)):
            a, b = vpcs[i], vpcs[j]
            if cidrs_overlap(a['cidr'], b['cidr']):
                conflicts.append(
                    f"OVERLAP: {a.get('name', a['cidr'])} ({a['cidr']}) "
                    f"and {b.get('name', b['cidr'])} ({b['cidr']}) "
                    f"cannot be peered — CIDRs overlap."
                )
    return conflicts


# ─── Subnet Planner ─────────────────────────────────────────────────────────────

def plan_vpc_subnets(
    vpc_cidr: str,
    azs: list[str],
    components: dict,
) -> list[dict]:
    """
    Generate a subnet allocation plan for a data platform VPC.

    components is a dict of {role: {'min_ips': N, 'type': 'public'|'private'}}.
    Example:
      components = {
        'public':     {'min_ips': 20,   'type': 'public'},
        'kafka':      {'min_ips': 50,   'type': 'private'},
        'kubernetes': {'min_ips': 3000, 'type': 'private'},
      }
    """
    import ipaddress

    network = ipaddress.IPv4Network(vpc_cidr, strict=False)
    prefix = network.prefixlen

    plan = []
    # For each component type, allocate the smallest prefix that fits min_ips
    # across all AZs
    for role, spec in components.items():
        min_ips = spec['min_ips'] + 5  # AWS reserves 5
        required_prefix = 32
        while (2 ** (32 - required_prefix)) < min_ips:
            required_prefix -= 1

        for az in azs:
            plan.append({
                'role': role,
                'az': az,
                'type': spec['type'],
                'required_prefix': required_prefix,
                'usable_ips': 2 ** (32 - required_prefix) - 5,
                'recommended_cidr': f'/{required_prefix}',
            })

    return plan


# ─── Report ─────────────────────────────────────────────────────────────────────

def vpc_audit_report(vpc_id: str, region: str = 'us-east-1') -> None:
    """
    Print a comprehensive VPC audit report.
    Requires AWS credentials to be configured.
    """
    print("=" * 70)
    print(f"VPC AUDIT REPORT: {vpc_id} ({region})")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    try:
        inspector = VPCInspector(region=region)
    except RuntimeError as e:
        print(f"Cannot connect to AWS: {e}")
        _demo_report()
        return

    # 1. Subnets
    print("\n[1] SUBNET INVENTORY")
    subnets = inspector.get_subnets(vpc_id)
    print(f"  {'Name':<30} {'CIDR':<18} {'AZ':<15} {'Type':<10} "
          f"{'Available':>10}")
    print(f"  {'-'*30} {'-'*18} {'-'*15} {'-'*10} {'-'*10}")
    for s in subnets:
        t = 'PUBLIC' if s.is_public else 'private'
        util_pct = (1 - s.available_ips / max(s.total_ips, 1)) * 100
        warn = "  ⚠️ " if s.available_ips < 20 else ""
        print(f"  {s.name:<30} {s.cidr_block:<18} {s.availability_zone:<15} "
              f"{t:<10} {s.available_ips:>7}/{s.total_ips:<5}{warn}")

    # 2. NAT Gateways
    print("\n[2] NAT GATEWAYS")
    nat_gateways = inspector.get_nat_gateways(vpc_id)
    if not nat_gateways:
        print("  No NAT gateways found.")
    for ng in nat_gateways:
        print(f"  {ng['nat_id']}  AZ: {ng['availability_zone']:<15}  "
              f"Public IP: {ng.get('public_ip', 'N/A')}")

    # 3. VPC Endpoints
    print("\n[3] VPC ENDPOINTS")
    endpoints = inspector.get_vpc_endpoints(vpc_id)
    if not endpoints:
        print("  ⚠️  No VPC endpoints found. S3 traffic routes through NAT gateway.")
    for ep in endpoints:
        svc = ep.service_name.split('.')[-1]
        print(f"  {ep.endpoint_id}  {svc:<20}  {ep.endpoint_type:<10}  {ep.state}")

    # 4. Cost Estimates
    print("\n[4] COST ANALYSIS")
    costs = inspector.estimate_costs(vpc_id)
    print(f"  NAT gateways: {costs.nat_gateway_count} × $32.40/month = "
          f"${costs.nat_monthly_fixed_cost:.2f}/month (fixed)")
    if costs.missing_s3_endpoint:
        print(f"  ⚠️  No S3 VPC endpoint. Each TB through NAT costs "
              f"${costs.estimated_s3_nat_cost_per_tb:.2f}")
        print(f"     Fix: aws ec2 create-vpc-endpoint "
              f"--vpc-id {vpc_id} --service-name "
              f"com.amazonaws.{region}.s3 --vpc-endpoint-type Gateway "
              f"--route-table-ids <rt-ids>")
    else:
        print("  ✓ S3 VPC endpoint configured (no NAT charges for S3 traffic)")
    for warn in costs.cross_az_warnings:
        print(f"  ⚠️  {warn}")

    print()
    print("=" * 70)


def _demo_report():
    """Print a demo report without AWS credentials."""
    print("\n  [DEMO MODE — no AWS credentials detected]\n")
    print("  Example VPC design for a data platform:\n")
    vpcs = [
        {'name': 'Production VPC',    'cidr': '10.0.0.0/16'},
        {'name': 'Development VPC',   'cidr': '10.1.0.0/16'},
        {'name': 'Data Platform VPC', 'cidr': '10.2.0.0/16'},
        {'name': 'Shared Services',   'cidr': '10.3.0.0/16'},
    ]
    print("  VPC CIDR Conflict Check:")
    conflicts = check_peering_compatibility(vpcs)
    if conflicts:
        for c in conflicts:
            print(f"  ⚠️  {c}")
    else:
        print("  ✓ No CIDR conflicts — all VPCs can be peered")

    print("\n  Subnet Plan (10.0.0.0/16, 3 AZs):")
    components = {
        'public':     {'min_ips': 20,    'type': 'public'},
        'kafka':      {'min_ips': 50,    'type': 'private'},
        'spark':      {'min_ips': 200,   'type': 'private'},
        'kubernetes': {'min_ips': 3000,  'type': 'private'},
    }
    plan = plan_vpc_subnets(
        '10.0.0.0/16',
        ['us-east-1a', 'us-east-1b', 'us-east-1c'],
        components,
    )
    print(f"  {'Role':<15} {'AZ':<15} {'Type':<10} {'CIDR':<10} {'Usable IPs':>12}")
    print(f"  {'-'*15} {'-'*15} {'-'*10} {'-'*10} {'-'*12}")
    for p in plan:
        print(f"  {p['role']:<15} {p['az']:<15} {p['type']:<10} "
              f"{p['recommended_cidr']:<10} {p['usable_ips']:>12}")

    print("\n  CIDR Utilities:")
    print(f"  /24 usable IPs: {usable_ips('10.0.1.0/24')}")
    print(f"  /22 usable IPs: {usable_ips('10.0.4.0/22')}")
    print(f"  /20 usable IPs: {usable_ips('10.0.0.0/20')}")
    print(f"  Overlap check 10.0.0.0/16 vs 10.1.0.0/16: "
          f"{cidrs_overlap('10.0.0.0/16', '10.1.0.0/16')}")
    print(f"  Overlap check 10.0.0.0/16 vs 10.0.0.0/24: "
          f"{cidrs_overlap('10.0.0.0/16', '10.0.0.0/24')}")

    print("=" * 70)


# ─── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="VPC diagnostic toolkit")
    parser.add_argument('--vpc-id', help='VPC ID to audit (e.g., vpc-0123...)')
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--check-cidrs', nargs='+', metavar='CIDR',
                        help='Check if a list of CIDRs overlap')
    parser.add_argument('--plan-subnets', action='store_true',
                        help='Show example subnet plan for a 3-AZ data platform')
    args = parser.parse_args()

    if args.check_cidrs:
        vpcs = [{'name': c, 'cidr': c} for c in args.check_cidrs]
        conflicts = check_peering_compatibility(vpcs)
        if conflicts:
            for c in conflicts: print(f"⚠️  {c}")
        else:
            print("✓ No CIDR overlaps detected")
    elif args.plan_subnets:
        plan = plan_vpc_subnets(
            '10.0.0.0/16',
            ['us-east-1a', 'us-east-1b', 'us-east-1c'],
            {
                'public': {'min_ips': 20, 'type': 'public'},
                'kafka': {'min_ips': 50, 'type': 'private'},
                'kubernetes': {'min_ips': 3000, 'type': 'private'},
            }
        )
        for p in plan:
            print(f"{p['role']}/{p['az']}: {p['recommended_cidr']} "
                  f"({p['usable_ips']} IPs, {p['type']})")
    else:
        vpc_id = args.vpc_id or 'vpc-demo'
        vpc_audit_report(vpc_id, args.region)
```

---

## 6. Visual Reference

### Standard 3-AZ VPC Layout for a Data Platform

```
VPC: 10.0.0.0/16
┌─────────────────────────────────────────────────────────────────┐
│                                                                   │
│  PUBLIC SUBNETS (Internet-facing)                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │  10.0.0.0/24 │  │  10.0.1.0/24 │  │  10.0.2.0/24 │           │
│  │  us-east-1a  │  │  us-east-1b  │  │  us-east-1c  │           │
│  │  NAT GW  ALB │  │  NAT GW  ALB │  │  NAT GW  ALB │           │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
│         │                 │                 │                     │
│  PRIVATE SUBNETS (Data components)                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ 10.0.10.0/23 │  │ 10.0.12.0/23 │  │ 10.0.14.0/23 │           │
│  │ Kafka broker │  │ Kafka broker │  │ Kafka broker │           │
│  │ Spark worker │  │ Spark worker │  │ Spark worker │           │
│  │ Postgres RW  │  │ Postgres RO  │  │              │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ 10.0.20.0/22 │  │ 10.0.24.0/22 │  │ 10.0.28.0/22 │           │
│  │  EKS nodes   │  │  EKS nodes   │  │  EKS nodes   │           │
│  │  + pods      │  │  + pods      │  │  + pods      │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│                                                                   │
│  VPC Endpoints: S3 (Gateway, free), ECR, Secrets Manager          │
└─────────────────────────────────────────────────────────────────┘
         ↕ Internet Gateway (for public subnets only)
         ↕ VPC Endpoint → S3, DynamoDB (bypasses NAT)
```

### Traffic Flow: Private Subnet to S3

```
WITHOUT VPC endpoint:
  Spark Executor (10.0.10.5)
      → route table: 0.0.0.0/0 → nat-abc123
      → NAT Gateway (public subnet, 10.0.0.10, EIP: 54.x.x.x)
      → Internet → S3 bucket
      Cost: $0.045/GB NAT processing + cross-AZ if NAT is in different AZ

WITH Gateway VPC Endpoint:
  Spark Executor (10.0.10.5)
      → route table: pl-63a5400a (S3 prefix list) → vpce-xxxxx
      → Private AWS network → S3 bucket
      Cost: $0 (Gateway endpoints are free)
```

### VPC Peering: Non-Transitivity

```
Source Account                   Platform Account         Analytics Account
VPC: 10.1.0.0/16                 VPC: 10.2.0.0/16         VPC: 10.3.0.0/16
┌──────────────┐                 ┌──────────────┐         ┌──────────────┐
│              │◄── pcx-ab ──────►              │         │              │
│  Application │                 │  Data Lake   │         │  Analytics   │
│  Postgres    │                 │  Kafka       │         │  BigQuery    │
└──────────────┘                 └──────────────┘         └──────────────┘
                                       ▲
                                  pcx-bc │
                                       ▼
                            ┌──────────────┐
                            │   Analytics  │← pcx-bc exists
                            └──────────────┘
                            
Source Account CANNOT reach Analytics VPC through Platform VPC.
Non-transitivity: must add pcx-ac (direct peering) for Source→Analytics.
```

---

## 7. Common Mistakes

**Mistake 1: Placing Kafka brokers in a public subnet.** A Kafka broker in a public subnet has a public IP and is reachable from the internet. Even if security groups restrict access, this violates defense-in-depth principles. Kafka brokers belong in private subnets. External access is provided by a load balancer in the public subnet forwarding to the private brokers.

**Mistake 2: Using a single shared NAT gateway for all AZs.** Traffic from resources in AZ-b and AZ-c to a NAT gateway in AZ-a crosses AZ boundaries, incurring $0.01/GB cross-AZ bandwidth charges. For data-intensive workloads (Spark reading from S3 via NAT), this is significant. Use one NAT gateway per AZ and configure the private route table in each AZ to use the local NAT gateway.

**Mistake 3: Forgetting to add a VPC Gateway endpoint for S3.** Every production VPC that uses S3 (which is every data platform VPC) should have a Gateway endpoint for S3. It costs nothing and saves $0.045/GB of NAT processing charges. The mistake is typically forgetting to add the endpoint's route to all private route tables — if any private route table is missing the S3 prefix list route, that subnet's traffic still goes through NAT.

**Mistake 4: Using overlapping CIDRs across VPCs.** Creating a VPC with `10.0.0.0/16` and later trying to peer it with another VPC using `10.0.0.0/24` (which overlaps) results in a peering failure. Plan all VPC CIDRs before creation. A common organizational convention: each team or account gets a `/16` from a pre-allocated pool (e.g., production: `10.0.0.0/16`, staging: `10.1.0.0/16`, etc.).

**Mistake 5: Not accounting for Kubernetes pod IP consumption.** EKS with VPC CNI assigns each pod a real VPC IP. A node group with 50 nodes × 30 pods each = 1,500 IPs. A `/24` subnet (254 usable IPs) cannot fit even 9 nodes × 30 pods. Kubernetes subnets must be large — `/21` (2,048 IPs) or larger.

**Mistake 6: Over-relying on VPC peering for many-VPC connectivity.** VPC peering is point-to-point and non-transitive. With 10 VPCs, fully connecting them requires `10 × 9 / 2 = 45` peering connections. Managing 45 route table entries across all VPCs is operationally complex. At scale, Transit Gateway provides a hub-and-spoke model where each VPC needs only one attachment.

---

## 8. Production Failure Scenarios

### Scenario 1: Spark Job Costs $1,200/Day in Unexpected NAT Charges

**Symptoms:** AWS bill shows NAT gateway processing charges of $1,200/day. A data team recently migrated a Spark job to a new VPC. The job reads 25 TB/day from S3.

**Root cause:** The new VPC does not have a Gateway VPC endpoint for S3. All 25 TB of S3 reads from Spark executors routes through the NAT gateway. Cost: `25,000 GB × $0.045 = $1,125/day`. The previous VPC had an S3 endpoint but the team forgot to configure it in the new VPC.

**Investigation:**
```bash
# Verify: is there an S3 VPC endpoint?
aws ec2 describe-vpc-endpoints \
  --filters "Name=service-name,Values=com.amazonaws.us-east-1.s3" \
            "Name=state,Values=available" \
  --query 'VpcEndpoints[*].[VpcEndpointId,State,RouteTableIds]'
# Empty output → no S3 endpoint

# Verify: are the route tables missing the S3 prefix list?
aws ec2 describe-route-tables \
  --filters "Name=vpc-id,Values=vpc-newvpc123" \
  --query 'RouteTables[*].Routes[?contains(DestinationPrefixListId, `pl-`)]'
# Empty output → no prefix list routes (no VPC endpoints in route tables)
```

**Fix:**
```bash
# Get route table IDs for private subnets
RT_IDS=$(aws ec2 describe-route-tables \
  --filters "Name=vpc-id,Values=vpc-newvpc123" \
  --query 'RouteTables[?not_null(Associations[0].SubnetId)].RouteTableId' \
  --output text | tr '\t' ' ')

# Create the S3 Gateway endpoint
aws ec2 create-vpc-endpoint \
  --vpc-id vpc-newvpc123 \
  --service-name com.amazonaws.us-east-1.s3 \
  --vpc-endpoint-type Gateway \
  --route-table-ids $RT_IDS
```

### Scenario 2: Kafka Cluster Partitioned After AZ Failure, Cannot Recover

**Symptoms:** AZ us-east-1b becomes temporarily unavailable (AWS AZ issue, 10 minutes). Kafka cluster has 3 brokers, one in each AZ. During the outage, the ISR shrinks and `min.insync.replicas=2` blocks produces. After AZ recovery, the broker in us-east-1b cannot rejoin the cluster because its Kubernetes pod cannot reach the other brokers.

**Root cause:** The Kubernetes worker nodes in us-east-1b are in a different subnet than the other AZs, and the route table for that subnet lost its local VPC route during an AWS network incident. The pod can reach the internet (via NAT) but not private VPC IPs. This is unusual but has occurred due to AWS route table corruption during AZ incidents.

**Diagnosis:**
```bash
# From the affected pod, test connectivity to another broker
kubectl exec -n kafka kafka-1 -- ping -c 3 10.0.10.5  # broker-0 IP
# Expected: connected; if no output → route table issue

# Check the route table for the affected subnet from outside the pod
aws ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=subnet-affected-1b" \
  --query 'RouteTables[0].Routes'
# Look for: {"DestinationCidrBlock": "10.0.0.0/16", "GatewayId": "local"}
# If this route is MISSING, VPC-internal traffic is broken
```

**Fix:** AWS support or route table repair. In the interim, migrate the affected pod to a different AZ by cordoning the affected nodes and draining pods.

### Scenario 3: VPC Peering Asymmetric Routing — One-Way Connection

**Symptoms:** Services in VPC A (10.1.0.0/16) can connect to VPC B (10.2.0.0/16), but connections from VPC B to VPC A time out.

**Root cause:** VPC peering requires route table entries on BOTH sides. The team added a route in VPC A's route table (`10.2.0.0/16 → pcx-abc123`) but forgot to add the return route in VPC B's route table (`10.1.0.0/16 → pcx-abc123`). TCP connections from VPC A to VPC B work because VPC B can route responses back (it has the VPC A CIDR in its route table — wait, that contradicts). Actually the reverse: connections from VPC A initiate and go through, but VPC B's response has no route back to VPC A (VPC A's CIDR is missing from VPC B's route table), so responses are dropped.

**Fix:**
```bash
# Add the return route to VPC B's route table
aws ec2 create-route \
  --route-table-id rtb-vpce-b-private \
  --destination-cidr-block 10.1.0.0/16 \
  --vpc-peering-connection-id pcx-abc123
```

### Scenario 4: Cross-AZ Bandwidth Costs Exceed Compute Costs

**Symptoms:** AWS bill review shows $4,000/month in "DataTransfer-Regional-Bytes" charges attributed to a Kafka cluster. The Kafka cluster has replication factor 3 with each replica in a different AZ.

**Root cause:** With RF=3 and 3 AZs, each byte written to Kafka is replicated to 2 additional brokers in other AZs. At 100 MB/s write throughput, inter-AZ replication traffic is 200 MB/s (to two replicas). Monthly: `200 MB/s × 86,400 s × 30 days = 518 TB`. At $0.01/GB: `518,000 GB × $0.01 = $5,180/month`.

**This is not a misconfiguration — it is the inherent cost of multi-AZ replication.** The options are:
1. Accept the cost (RF=3 multi-AZ is the correct setting for production)
2. Use RF=2 with `min.insync.replicas=1` for non-critical topics (halves replication traffic)
3. Use same-AZ follower preference: configure `replica.selector.class=RackAwareReplicaSelector` so consumers read from local-AZ replicas (for consumer traffic, not replication)
4. Negotiate AWS enterprise pricing for inter-AZ data transfer

---

## 9. Performance and Tuning

### Subnet IP Space Sizing for Kubernetes

```
For EKS with VPC CNI (each pod gets a VPC IP):

Target cluster: 50 nodes, max 30 pods per node = 1,500 pod IPs
Plus: 50 node IPs = 1,550 total IPs needed

Subnet sizing options:
  /22 = 4,091 usable IPs  ← minimum recommendation
  /21 = 8,187 usable IPs  ← comfortable headroom
  /20 = 16,379 usable IPs ← large clusters

Always plan for 3× growth:
  1,550 current × 3 = 4,650 → use /21 (8,187 IPs)
```

### VPC Endpoint Cost-Benefit for Common Services

```
Service         Endpoint Type    Hourly Cost    When Beneficial
──────────────  ─────────────    ───────────    ───────────────
S3              Gateway          Free           Always — add immediately
DynamoDB        Gateway          Free           Always if using DynamoDB
ECR (Docker)    Interface        $0.01/hr/AZ    High pull frequency (EKS)
Secrets Manager Interface        $0.01/hr/AZ    Any use (security + cost)
SSM             Interface        $0.01/hr/AZ    EC2 instance management
SQS             Interface        $0.01/hr/AZ    High message volume
CloudWatch      Interface        $0.01/hr/AZ    High metric/log volume
MSK (Kafka)     Interface        $0.01/hr/AZ    Cross-account Kafka access
Glue            Interface        $0.01/hr/AZ    Spark/Glue job isolation
```

### NAT Gateway vs VPC Endpoint Cost Comparison

```
Traffic: 10 TB/day through NAT vs through VPC Endpoint

NAT Gateway (per AZ):
  Fixed: $0.045/hr × 24 × 30 = $32.40/month
  Data:  10,000 GB/day × 30 × $0.045 = $13,500/month
  Total: ~$13,532/month per AZ

VPC Endpoint (Interface, per AZ):
  Fixed: $0.01/hr × 24 × 30 = $7.20/month
  Data:  10,000 GB/day × 30 × $0.01 = $3,000/month
  Total: ~$3,007/month per AZ

Savings: $10,525/month per AZ from switching to Interface endpoint
         (for S3: Gateway endpoint = $0/month, savings = full $13,532)
```

---

## 10. Interview Q&A

**Q1: Explain the difference between a public subnet and a private subnet in a VPC. Where do you place Kafka brokers and why?**

A public subnet is defined by its route table: it has a default route (`0.0.0.0/0`) that points to an Internet Gateway. Resources in a public subnet can optionally receive a public IP address and are directly reachable from the internet. A private subnet's route table has no direct route to an Internet Gateway. Instead, its default route points to a NAT gateway in a public subnet, allowing outbound internet access (for software updates, external API calls) while preventing inbound connections from the internet.

Kafka brokers always belong in private subnets. The reasons are both security and architectural. From a security standpoint, Kafka brokers contain the organization's data and authentication credentials. Exposing them to inbound internet connections violates the principle of defense in depth — a security group misconfiguration would then directly expose the brokers to the internet. From an architectural standpoint, external clients do not connect directly to Kafka brokers; they connect to a load balancer (in a public subnet) which forwards to brokers. The brokers' advertised listeners use their private VPC IPs, and the load balancer routes to them. The only inbound traffic that should reach Kafka brokers originates from within the VPC (other services) or from authorized external sources via the load balancer.

The typical architecture: ALB or NLB in public subnets accepting connections from external producers and consumers (or from a VPN), forwarding to Kafka brokers in private subnets. Internal services (Spark jobs, schema registry, Kafka Connect) connect to the brokers directly via private IPs within the VPC, bypassing the load balancer entirely.

**Q2: A Spark job that reads 10 TB/day from S3 is running on EMR in a VPC. What do you check first when the team reports unexpectedly high AWS bills, and what is the fix?**

The first thing to check is whether the VPC has a Gateway VPC endpoint for S3. Without one, all traffic from the EMR cluster to S3 routes through the NAT gateway, incurring NAT gateway data processing charges of $0.045/GB — on top of any other data transfer fees. For 10 TB/day: `10,000 GB × $0.045 = $450/day = $13,500/month` in NAT charges alone. This is a common and easily avoidable cost.

The diagnosis is straightforward: `aws ec2 describe-vpc-endpoints --filters "Name=service-name,Values=com.amazonaws.us-east-1.s3" "Name=state,Values=available"`. If the output is empty, there is no S3 endpoint. The fix is a one-line AWS CLI command to create a Gateway endpoint and add it to all private route tables: `aws ec2 create-vpc-endpoint --vpc-id <vpc-id> --service-name com.amazonaws.us-east-1.s3 --vpc-endpoint-type Gateway --route-table-ids <all-private-rt-ids>`. The Gateway endpoint routes S3 traffic through the private AWS backbone at no charge.

I would also check cross-AZ costs: if the EMR cluster spans multiple AZs and the NAT gateway is only in one AZ, all EMR nodes in other AZs are paying cross-AZ transfer fees to reach the NAT gateway ($0.01/GB), which is additional cost. Each AZ should have its own NAT gateway with a route in the local subnet's route table pointing to it.

**Q3: Explain VPC peering and its non-transitivity. How does this affect a multi-account data platform architecture?**

VPC peering creates a direct private network connection between two VPCs, allowing resources in each to communicate using private IPs. The traffic stays within the AWS backbone and does not traverse the public internet. A key property — non-transitivity — means that if VPC A peers with VPC B and VPC B peers with VPC C, resources in VPC A cannot communicate with resources in VPC C through VPC B. Each pair of VPCs that needs to communicate requires its own peering connection.

In a multi-account data platform architecture, non-transitivity directly constrains the design. A typical setup has three accounts: a "source" account containing operational databases, a "platform" account containing the data lake and Kafka cluster, and an "analytics" account containing data warehouses and BI tools. If the pipeline needs to read from the source account (for CDC via Debezium), process in the platform account (Spark), and make results available in the analytics account, that requires three peering connections: source↔platform, platform↔analytics, and source↔analytics. Without the third connection, a query in the analytics account that needs raw data from the source account cannot traverse through the platform account.

At scale, when an organization has dozens of accounts (one per team, environment, or service), fully connecting them requires O(N²) peering connections and O(N²) route table entries. This becomes unmanageable around 10–15 VPCs. The standard solution at that point is AWS Transit Gateway, which provides a hub-and-spoke topology: each VPC attaches to the Transit Gateway, and routing is centrally managed. The Transit Gateway provides transitive routing (A can reach C through the TGW), at the cost of $0.05/GB processed plus per-attachment fees.

---

## 11. Cross-Question Chain

**Interviewer:** Your company is building a new cloud data platform. The platform needs to access an operational Postgres database in the "source" AWS account, run Spark jobs in a dedicated "platform" account, and make results available to an "analytics" account. Walk me through the VPC design.

**Candidate:** I'd design with three VPCs — one per account — with non-overlapping CIDRs. A reasonable allocation: source account `10.0.0.0/16`, platform account `10.1.0.0/16`, analytics account `10.2.0.0/16`. Each VPC gets a standard 3-AZ layout with public and private subnets. The Postgres database in the source account lives in a private subnet. Spark on EMR in the platform account lives in private subnets with a large CIDR (`/20`) to accommodate task node scaling. Since all three accounts need to communicate, I'd set up three VPC peering connections: source↔platform (for the CDC pipeline to read from Postgres), platform↔analytics (for Spark to write results to Redshift or S3 in the analytics account), and a Gateway S3 endpoint in each VPC (free, avoids NAT charges for S3 reads).

**Interviewer:** Why not use Transit Gateway instead of peering?

**Candidate:** For three VPCs with straightforward connectivity, peering is cheaper and simpler. Three peering connections are manageable — three route table entries per VPC (one for each peering partner). Transit Gateway adds per-attachment fees ($0.05/hour each) and per-GB processing charges ($0.05/GB), which matters for data-intensive pipelines. The cross-account S3 access doesn't go through TGW anyway (that uses S3 VPC endpoints or cross-account IAM policies). I'd switch to TGW if the organization grows to 8+ VPCs where managing 28+ peering connections becomes operationally complex, or if they need on-premises connectivity via Direct Connect (TGW integrates natively with Direct Connect, peering does not).

**Interviewer:** The Spark jobs need to pull from the Postgres in the source account. What else besides VPC peering do you need to configure?

**Candidate:** VPC peering establishes network reachability, but three more things are needed. First, route tables on both sides: a route for `10.0.0.0/16` (source VPC CIDR) in the platform VPC's route table pointing to the peering connection, and a route for `10.1.0.0/16` (platform CIDR) in the source VPC's route table. Peering is bi-directional in routing — you must configure both sides. Second, security groups: the Postgres security group in the source account must allow inbound TCP/5432 from the platform VPC's CIDR (`10.1.0.0/16`). Security groups are stateful — responses are automatically allowed. Third, IAM credentials: the Spark jobs need credentials to authenticate to Postgres (database-level credentials, not IAM — Postgres uses username/password or client certs, not IAM roles). If using Secrets Manager to store the credentials, the platform VPC also needs an Interface endpoint for Secrets Manager so the Spark executor can fetch the secret without leaving the VPC.

**Interviewer:** One of your Spark jobs suddenly starts failing with "connection refused" to the Postgres database. What's your debugging process?

**Candidate:** I work from the outside in. First, verify the peering connection is still active: `aws ec2 describe-vpc-peering-connections --query 'VpcPeeringConnections[?Status.Code==`active`]'`. If the status is anything other than "active" — maybe "deleted" or "expired" — that's the cause.

Second, check route tables. Even if peering is active, a route table change can break connectivity. From the platform VPC's private route table, verify there's still a route for `10.0.0.0/16` via the peering connection. Same for the source VPC.

Third, check security groups on the Postgres instance: inbound rules for TCP/5432 from `10.1.0.0/16` (or the specific Spark subnet CIDR). Security group changes are a common cause of sudden connectivity breaks.

Fourth, if peering, routes, and security groups all look correct, test with a direct TCP connection from a pod in the platform account: `nc -z -v postgres.source.internal 5432`. If this works, the network is fine and the issue is at the database level (authentication, wrong credentials, Postgres max_connections exceeded). If it hangs, use VPC flow logs to see if the connection attempt is ACCEPT or REJECT.

**Interviewer:** The flow logs show REJECT for packets from `10.1.5.23` (Spark executor) to `10.0.2.44:5432` (Postgres). What does this tell you?

**Candidate:** A REJECT in VPC flow logs means a security group or network ACL explicitly denied the packet. Since security groups are stateful and the REJECT is on the ingress to the Postgres host, the Postgres host's security group is refusing the connection. The Spark executor IP `10.1.5.23` is in the `10.1.0.0/16` platform VPC. The Postgres security group likely has an inbound rule that permits `10.1.0.0/16` on port 5432, but if `10.1.5.23` is in a subnet that was recently added (or a new Spark task node added in a subnet not covered by the security group rule), the rule might not cover it.

I'd check the Postgres security group's inbound rules: `aws ec2 describe-security-groups --group-ids sg-postgres-prod --query 'SecurityGroups[0].IpPermissions'`. If the rule is for `10.1.10.0/24` (a specific Spark subnet) rather than `10.1.0.0/16` (the entire platform VPC), adding a new Spark subnet breaks connectivity. Fix: broaden the security group rule to `10.1.0.0/16` so all platform VPC traffic is allowed.

**Interviewer:** After fixing the security group, you realize the data engineer's Spark job is reading 5 TB/day from the source account's S3 bucket and the bill is much higher than expected. What's the cause?

**Candidate:** Cross-account S3 reads don't traverse VPC peering — S3 is a public service, not a VPC resource. The traffic path is: Spark executor in platform VPC → NAT gateway in platform VPC → internet → S3 in source account. This incurs NAT gateway processing charges ($0.045/GB) and potentially cross-region data transfer charges if the accounts are in different regions.

The fix is a cross-account S3 bucket policy combined with a VPC endpoint. A Gateway VPC endpoint for S3 in the platform VPC redirects S3 traffic to the private AWS backbone regardless of which account owns the bucket. Additionally, update the source account's S3 bucket policy to allow reads from the platform account's IAM role. With this configuration: Spark executor → VPC endpoint → AWS private network → S3 source account bucket. No NAT gateway, no internet transit, no NAT processing charges. The remaining cost is S3 egress to another account within the same region, which AWS currently charges at $0 for same-region transfers (as of this writing, with some nuances around requester-pays buckets).

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What makes a subnet "public" in a VPC? | Its route table has a default route (`0.0.0.0/0`) pointing to an Internet Gateway. Without this route, the subnet is private regardless of other settings. |
| 2 | What is the cost of an AWS Gateway VPC endpoint for S3? | Zero. Gateway endpoints for S3 and DynamoDB are free — no hourly fee and no data processing charge. Every production VPC should have one. |
| 3 | Why is VPC peering non-transitive? | AWS does not route packets through an intermediate VPC. Each pair of VPCs needing connectivity requires a direct peering connection. A→B and B→C does not allow A→C. |
| 4 | What is the cost of routing 1 TB through a NAT gateway? | $0.045/GB × 1,024 GB = $46.08. Plus the hourly NAT gateway charge (~$32/month). Compare to $0 for an S3 Gateway endpoint. |
| 5 | How does Kubernetes pod IP assignment affect VPC subnet sizing? | EKS VPC CNI assigns each pod a real VPC IP. A 50-node cluster × 30 pods/node = 1,500 IPs. A /24 subnet (254 usable IPs) is insufficient; use /21 or larger. |
| 6 | What is the difference between a Gateway endpoint and an Interface endpoint? | Gateway: free, only S3/DynamoDB, implemented via route table prefix list. Interface: paid ($0.01/hr/AZ + $0.01/GB), covers most AWS services, creates an ENI with private IP. |
| 7 | What are the two things you must configure for VPC peering to work bidirectionally? | Route table entries on BOTH sides (each VPC needs a route to the other's CIDR via the peering connection) and security groups allowing traffic from the peer VPC's CIDR. |
| 8 | What is the cross-AZ data transfer cost in AWS? | $0.01/GB. Applies to Kafka inter-broker replication, NAT gateway traffic from a different AZ, and ALB forwarding to backends in different AZs. |
| 9 | When should you use Transit Gateway instead of VPC peering? | When you have many VPCs (8+) requiring complex connectivity, or need on-premises connectivity via Direct Connect/VPN. VPC peering is simpler and cheaper for few VPCs. |
| 10 | What does a VPC Flow Log REJECT entry indicate? | Traffic was blocked by a security group or Network ACL. Security groups are the first thing to check. VPC flow logs do NOT capture packet contents. |
| 11 | What is GCP's equivalent of AWS Private Subnet + VPC Endpoint? | Private Google Access, enabled per subnet. Allows VMs without external IPs to reach Google APIs (BigQuery, GCS, Pub/Sub) via internal routing without NAT or internet. |
| 12 | How do you prevent cross-AZ NAT gateway charges? | Put one NAT gateway in each AZ, and configure each AZ's private route table to route `0.0.0.0/0` to its local NAT gateway. |
| 13 | What is PrivateLink? | The AWS technology behind Interface endpoints. Also used by SaaS vendors (Confluent, Snowflake, Databricks) to expose their services inside a customer's VPC via private IP. |
| 14 | What RFC 1918 ranges are valid for VPC CIDRs? | 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16. Only these private ranges should be used for VPC CIDRs to avoid conflicts with public internet routing. |
| 15 | What is GCP's Shared VPC? | A GCP host project owns the VPC; service projects use its subnets. Equivalent to AWS Resource Access Manager (RAM) for sharing subnets across accounts. |
| 16 | How many usable IP addresses does a /24 subnet provide in AWS? | 251: 256 total minus 5 AWS-reserved addresses (network, router, DNS, future use, broadcast). |
| 17 | What does VPC Flow Logs capture? | IP header metadata: source/dest IP, ports, protocol, byte count, packet count, action (ACCEPT/REJECT), timestamp. NOT packet contents (that requires Traffic Mirroring). |
| 18 | Why does a Kafka cluster with RF=3 across 3 AZs incur significant cross-AZ charges? | Every write is replicated to 2 additional brokers in other AZs. At 100 MB/s write throughput, 200 MB/s crosses AZ boundaries: ~518 TB/month × $0.01/GB = $5,180/month. |
| 19 | What is the maximum number of peering connections before Transit Gateway becomes preferable? | Generally 8–10 VPCs. At 10 VPCs fully connected, you need 45 peering connections. TGW provides hub-and-spoke with O(N) attachments instead of O(N²) connections. |
| 20 | What is the key difference between AWS VPC (regional) and GCP VPC (global)? | AWS VPC is regional — subnets are per-AZ, cross-region requires peering. GCP VPC is global — one VPC spans all regions, subnets are regional (cover all zones in a region). |

---

## 13. Further Reading

- **AWS VPC Documentation — "VPCs and Subnets":** The authoritative reference for VPC CIDR allocation, subnet design, and route table configuration. Includes the list of AWS-reserved IP addresses in each subnet and the rules for secondary CIDR blocks.
- **AWS Architecture Blog — "One to Many: Evolving VPC Design":** A detailed treatment of how VPC architecture evolves from a simple single-VPC to a multi-account, Transit Gateway-connected architecture. Covers the decision points for peering vs TGW.
- **"The Practical Guide to AWS Networking Costs" — Corey Quinn, The Duckbill Group:** A practitioner-focused analysis of AWS networking costs (NAT gateways, cross-AZ transfer, VPC endpoints). Frequently updated. Essential reading for any engineer responsible for cloud costs.
- **AWS re:Invent 2023 — "Advanced VPC Design and New Capabilities":** A deep-dive session on VPC advanced topics: IPv6, prefix lists, security group references, and VPC endpoints at scale. Available on YouTube.
- **Google Cloud VPC Documentation — "VPC Network Overview":** Explains GCP's global VPC model, Shared VPC, and Private Google Access. Essential for data engineers working with BigQuery or GKE.
- **AWS "EKS Best Practices Guide" — Networking section:** The definitive guide to sizing Kubernetes subnets in AWS, VPC CNI configuration, and custom networking for pod IP management when the primary subnet is exhausted.

---

## 14. Lab Exercises

**Exercise 1: VPC Audit with the Code Toolkit**

Run `python vpc_diagnostics.py` without AWS credentials to see the demo report. Then create a free-tier AWS account, create a VPC with one public and one private subnet, and run `python vpc_diagnostics.py --vpc-id vpc-yourId`. Verify that the tool correctly identifies the public vs private subnet and reports that no S3 VPC endpoint exists. Then create the endpoint and run again.

**Exercise 2: Calculate the Cost Impact of a Missing S3 VPC Endpoint**

Given a Spark cluster that reads 20 TB/day from S3 via NAT, calculate: (a) monthly NAT gateway data processing cost, (b) monthly savings from adding an S3 Gateway endpoint, (c) the monthly cost of one NAT gateway with and without the endpoint. Use current AWS pricing from aws.amazon.com/vpc/pricing.

**Exercise 3: CIDR Overlap Checker**

Using the `check_peering_compatibility` function, create 6 VPC CIDRs for a hypothetical organization: production, staging, development, data platform, analytics, and shared services. Choose CIDRs that do not overlap, then introduce one overlap and verify the tool detects it. Plan a proper CIDR allocation scheme.

**Exercise 4: Subnet Sizing for Kubernetes**

A Kubernetes cluster needs to handle peak load of 200 nodes with 30 pods per node. Using `plan_vpc_subnets`, compute the required subnet prefix length for a 3-AZ cluster. Verify the calculation: total IPs needed = 200 nodes × (30 pods + 1 node IP) + 50% headroom.

**Exercise 5: VPC Peering Connectivity Matrix**

Draw the connectivity diagram for a 5-account organization: (1) source-account-A, (2) source-account-B, (3) platform-account, (4) analytics-account, (5) shared-services-account. Define which accounts need direct connectivity, and determine how many VPC peering connections are required vs a Transit Gateway solution. Calculate the cost difference at 10 TB/month of inter-VPC traffic.

---

## 15. Key Takeaways

VPC architecture is the network foundation on which all cloud data infrastructure runs. The three most important VPC facts for data engineers: every data component (Kafka, Spark, databases) belongs in a private subnet, never a public subnet; every VPC that touches S3 needs a Gateway endpoint (it's free and saves significant NAT costs); and VPC peering is non-transitive, so multi-account connectivity requires either direct peering between all accounts or a Transit Gateway.

Subnet design has lasting consequences. Pod IPs in EKS come from VPC subnets, making under-sized Kubernetes subnets a common and painful operational problem. CIDR allocation must be planned before VPCs are created because changing a VPC's CIDR is complex and changing subnet CIDRs is impossible without recreation.

Cost optimization in VPCs centers on three levers: S3 and other Gateway/Interface VPC endpoints (eliminate NAT costs), per-AZ NAT gateways (eliminate cross-AZ charges to reach the NAT), and awareness of cross-AZ replication costs for stateful systems like Kafka.

---

## 16. Connections to Other Modules

- **M27 — TCP/IP Fundamentals:** VPC route tables implement the IP routing concepts from M27. The "local" route, default routes, and longest-prefix matching are all TCP/IP routing concepts applied at the VPC level.
- **M28 — DNS and Service Discovery:** VPC DNS is configured per-VPC (`enableDnsHostnames`, `enableDnsSupport`). The VPC's internal DNS resolver (at VPC_CIDR+2, e.g., `10.0.0.2`) is the `nameserver` in each instance's `/etc/resolv.conf`.
- **M31 — Load Balancing and Proxies:** NLBs and ALBs live in public subnets. Kafka brokers they front live in private subnets. The VPC's routing enables the LB to forward to private subnet backends.
- **M33 — IAM and RBAC:** IAM policies control who can access VPC resources (EC2 instances, S3 via VPC endpoint). IAM and VPC are complementary security layers — IAM controls identity-based access, VPC controls network-based access.
- **M34 — Security Groups and Network Policies:** Security groups are the primary ingress/egress control mechanism within a VPC. They complement VPC subnet design — private subnets provide network-level isolation, security groups provide host-level isolation.
- **M35 — Private Endpoints and VPC Service Controls:** Builds directly on VPC endpoints (this module) to cover the full story of data exfiltration prevention.

---

## 17. Anti-Patterns

**Anti-pattern: Using the default VPC for production workloads.** The AWS default VPC has all subnets public by default (`MapPublicIpOnLaunch=true`). Resources launched in the default VPC automatically get public IPs. Never run Kafka, Spark, or databases in the default VPC. Create a dedicated VPC with explicit public/private subnet separation.

**Anti-pattern: Creating VPC CIDRs without an organizational plan.** Creating VPCs with whatever CIDR is convenient (`10.0.0.0/16` for everything) and then discovering overlaps when peering is needed months later is extremely common. Define a CIDR allocation policy before creating any VPCs. Large organizations should assign CIDR ranges centrally.

**Anti-pattern: Treating VPC CIDR as abundant because the range is "large."** A `/16` VPC has 65,536 addresses, but EKS can consume thousands of pod IPs from a single cluster. Organizations that deploy multiple Kubernetes clusters in the same VPC can exhaust the IP space. Monitor `AvailableIpAddressCount` per subnet and alert when it drops below 20% of capacity.

**Anti-pattern: Using VPC peering across regions for data-intensive pipelines.** Inter-region VPC peering (supported, but charged at inter-region data transfer rates: $0.02–$0.09/GB) is significantly more expensive than same-region VPC peering. A pipeline that replicates 10 TB/day between us-east-1 and eu-west-1 via inter-region VPC peering costs $0.02 × 10,000 = $200/day. Consider whether AWS Transit Gateway inter-region peering, S3 Cross-Region Replication, or Kafka MirrorMaker (which uses the public internet with TLS) is more cost-effective.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `aws ec2 describe-vpcs` | List VPCs and their CIDRs | `--query 'Vpcs[*].[VpcId,CidrBlock]'` |
| `aws ec2 describe-subnets` | List subnets and AZ placement | `--filters "Name=vpc-id,Values=vpc-..."` |
| `aws ec2 describe-route-tables` | Inspect routing rules | Check for igw-*/nat-*/vpce-* targets |
| `aws ec2 describe-vpc-endpoints` | List VPC endpoints | Verify S3 Gateway endpoint exists |
| `aws ec2 describe-nat-gateways` | List NAT gateways | Check per-AZ coverage |
| `aws ec2 describe-vpc-peering-connections` | List peering connections | Verify status = active |
| `aws ec2 create-vpc-endpoint` | Create S3 Gateway endpoint | `--vpc-endpoint-type Gateway` |
| `aws ec2 create-flow-logs` | Enable VPC flow logs | `--log-destination-type s3` |
| `aws cloudwatch get-metric-statistics` | NAT gateway byte metrics | `--namespace AWS/NATGateway` |
| `gcloud compute networks list` | List GCP VPCs | Shows global VPC names |
| `gcloud compute networks subnets list` | List GCP subnets with Private Google Access | `--filter="network=..."` |
| `gcloud compute networks peerings list` | List GCP VPC peerings | `--network=<vpc-name>` |

---

## 19. Glossary

**Availability Zone (AZ):** An isolated data center cluster within a cloud region. AZs are physically separated to avoid correlated failures. Subnets belong to a single AZ.

**CIDR (Classless Inter-Domain Routing):** Notation for specifying IP address ranges. `10.0.0.0/16` means the first 16 bits are fixed, giving 65,536 addresses.

**Gateway VPC Endpoint:** A free VPC endpoint that routes traffic to S3 or DynamoDB through the AWS private backbone via a route table entry. No data processing fees.

**Interface VPC Endpoint:** A paid VPC endpoint that creates an ENI with a private IP in a subnet. Supports most AWS services. Priced per hour per AZ plus per GB.

**Internet Gateway (IGW):** A VPC component that allows public subnets to communicate with the internet. One per VPC; horizontally scaled and highly available.

**NAT Gateway:** A managed NAT device in a public subnet that allows private subnet resources to initiate outbound internet connections. Charges $0.045/hour + $0.045/GB.

**Network ACL (NACL):** A stateless firewall at the subnet level. Rules are evaluated in order; both inbound and outbound rules must be configured. Less commonly used than security groups for data engineering.

**Private Google Access:** GCP feature that allows VMs without external IPs to access Google APIs (BigQuery, GCS) internally. Enabled per subnet.

**Private Subnet:** A subnet whose route table has no direct route to an Internet Gateway. Outbound internet access via NAT gateway only. All data components belong here.

**PrivateLink:** AWS technology that provides private connectivity to AWS services and SaaS providers via Interface endpoints, using private IPs within the customer's VPC.

**Public Subnet:** A subnet whose route table has a default route to an Internet Gateway. Resources can receive public IPs. Used for load balancers, NAT gateways, bastion hosts.

**Route Table:** A set of routing rules associated with a subnet that determines where traffic is sent based on destination CIDR prefix match.

**Shared VPC (GCP):** A GCP model where a host project owns the VPC and service projects attach to it, sharing the subnets. Equivalent to AWS RAM for subnet sharing.

**Transit Gateway (TGW):** An AWS hub-and-spoke network transit service that provides transitive routing between attached VPCs and on-premises networks. Priced per attachment and per GB.

**VPC (Virtual Private Cloud):** A logically isolated network within a cloud provider's infrastructure. The private data center equivalent — you control IP ranges, routing, and access.

**VPC Flow Logs:** Metadata capture of IP traffic through VPC network interfaces. Records source/destination IPs, ports, protocol, bytes, and action (ACCEPT/REJECT). Not packet contents.

**VPC Peering:** A direct private network connection between two VPCs. Non-transitive. Free for same-region; per-GB charges for inter-region.

---

## 20. Self-Assessment

1. A data engineer creates a new VPC with CIDR `10.0.0.0/16` and puts Kafka brokers in a subnet with `MapPublicIpOnLaunch=true`. What is wrong, and what should they do instead?
2. Calculate the usable IP addresses in a `/22` subnet (AWS). Would this be sufficient for a Kubernetes node group with 80 nodes × 30 pods per node?
3. You have three VPCs: `10.0.0.0/16` (production), `10.0.0.0/24` (development), `10.1.0.0/16` (platform). Which pairs can be peered? Why?
4. A Spark job reads 50 TB/month from S3 through a NAT gateway. Calculate the monthly cost. What single change eliminates this cost?
5. Describe the routing path for a Spark executor in a private subnet (`10.0.10.5`) making an HTTPS request to `s3.amazonaws.com`, both without and with a Gateway VPC endpoint.
6. Your organization has 12 VPCs that all need to communicate. How many VPC peering connections would fully connect them? Why might you choose Transit Gateway instead?
7. A Kafka cluster with RF=3, one broker per AZ, writes 200 MB/s. Estimate the monthly cross-AZ data transfer cost.
8. Explain the GCP Shared VPC model and how it differs from AWS multi-account VPC architecture.
9. What does a REJECT entry in VPC flow logs mean, and what is the first thing to check?
10. Why does EKS require larger subnets than traditional EC2 deployments, and what is the minimum subnet prefix for a 100-node cluster with 30 pods per node?

---

## 21. Module Summary

VPCs are the network fabric of cloud data platforms. Every component — Kafka brokers, Spark workers, metadata databases, orchestrators — runs within a VPC, and the VPC's design determines what can communicate with what, at what cost, and with what security guarantees.

The foundational principle is subnet segregation: public subnets for internet-facing entry points (load balancers, NAT gateways), private subnets for data components. This is both a security boundary (data components are not directly internet-reachable) and a routing design (private subnets use NAT gateways as the internet exit point, not direct Internet Gateway routes).

CIDR planning is a one-time decision with long-lasting consequences. VPC CIDRs must be non-overlapping for peering to work, and subnet sizes must accommodate growth — especially Kubernetes pod IPs, which consume VPC addresses at scale. The correct approach is to plan a CIDR allocation scheme for the entire organization before creating any VPCs.

Cost optimization centers on VPC endpoints (S3 Gateway endpoints are free and eliminate NAT charges for the most common data engineering traffic pattern) and per-AZ NAT gateways (eliminating cross-AZ transfer fees for NAT-bound traffic). These two changes alone can reduce VPC networking costs by 50–90% for typical data platform workloads.

VPC peering provides direct connectivity between VPCs but is non-transitive: A↔B and B↔C does not allow A↔C. Multi-account architectures require either direct peering between all communicating account pairs or Transit Gateway for hub-and-spoke connectivity. The choice between them is a cost and complexity trade-off: peering is simpler and cheaper for few VPCs; TGW is more manageable for many.

The next module — M33: IAM and RBAC — builds on this network foundation by addressing the identity layer: who is authorized to access what within and across VPCs.
