# M33: IAM and RBAC

**Course:** SYS-NET-102 — Cloud Networking and Security  
**Module:** 02 of 05  
**Global Module ID:** M33  
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

IAM (Identity and Access Management) is the mechanism by which cloud infrastructure decides who can do what to which resources. In a data engineering context, IAM is not an abstract security concept — it is the daily operational reality that determines whether a Spark job can read from S3, whether Airflow can create BigQuery tables, whether a Kafka connector can write to Secrets Manager, and whether a data analyst can accidentally delete a production dataset.

Data engineers encounter IAM in specific recurring failure modes that are entirely avoidable with proper understanding:

**Failure mode 1 — Overly broad permissions that create security risk.** A Spark job given `s3:*` on `arn:aws:s3:::*` (all S3 buckets, all operations) can accidentally read sensitive HR data, write to the wrong bucket, or be exploited to exfiltrate data if the job itself has a vulnerability. The principle of least privilege requires giving the job only the exact permissions it needs: `s3:GetObject` and `s3:ListBucket` on the specific input bucket, and `s3:PutObject` on the specific output bucket.

**Failure mode 2 — Static credentials in code.** AWS access keys (`AKIAIOSFODNN7EXAMPLE` + secret key) hardcoded in a pipeline script or committed to version control are the most common cause of cloud data breaches. Attackers scan public GitHub repositories for these strings continuously. The correct approach in all modern cloud environments is role-based access: the compute resource (EC2 instance, EKS pod, Lambda function) assumes an IAM role, and the role grants the necessary permissions via temporary, automatically-rotated credentials.

**Failure mode 3 — Permission denied at runtime with no diagnostic information.** A Spark job fails with `AccessDeniedException` but the error message does not say which specific permission is missing. Understanding IAM policy structure — how actions, resources, and conditions compose into a permission decision — enables rapid diagnosis. The error `s3:GetObject was denied on arn:aws:s3:::data-lake/input/2024-01-15/part-00000.parquet` immediately tells you: add `s3:GetObject` to the IAM policy for the specific bucket prefix.

**Failure mode 4 — Cross-account permission chains.** A Spark job in account A tries to read an S3 bucket in account B. This requires both: an IAM role in account A that has permission to assume a role in account B, AND an IAM role in account B that trusts account A's role and grants S3 access. Many data engineers understand single-account IAM but are confused by cross-account trust relationships. Understanding the trust policy vs permissions policy distinction resolves this immediately.

For Google Cloud (BigQuery, Dataproc, GKE), the equivalent mechanism is IAM Roles and Service Accounts with Workload Identity — the GCP version of the same principle: compute resources authenticate via a service account identity rather than static credentials.

---

## 2. Mental Model

### IAM as a Three-Question Decision

When any API call is made against a cloud service (S3 GetObject, BigQuery RunQuery, EC2 DescribeInstances), the IAM system answers three questions in sequence:

1. **Who is making this request?** (Identity / Authentication)
   The caller proves their identity: an IAM user with access keys, an IAM role with temporary credentials, a service account, a federated identity.

2. **What are they allowed to do?** (Authorization)
   The IAM system evaluates all policies attached to the identity and determines if the requested action on the requested resource is explicitly allowed or denied.

3. **Is there anything that overrides the answer?** (Explicit denies, SCPs, permission boundaries)
   An explicit Deny in any policy always overrides an Allow. Organization-level Service Control Policies (SCPs) restrict what any identity in the organization can do, regardless of IAM policies.

The decision process in AWS:
```
Is the caller root? → Yes → Allow (except when SCPs block it)
                   → No
                       ↓
Is there an explicit Deny? → Yes → DENY
                           → No
                               ↓
Is there an explicit Allow? → Yes → ALLOW
                            → No  → DENY (implicit deny is the default)
```

### The Principal-Action-Resource-Condition Model

Every IAM policy statement has four components:

- **Principal:** Who this applies to (in trust policies) — an IAM role, user, account, or service
- **Action:** What operation is being permitted or denied — `s3:GetObject`, `bigquery.tables.getData`
- **Resource:** Which resource the action applies to — a specific S3 bucket ARN, a BigQuery dataset
- **Condition:** Optional constraints — `aws:RequestedRegion == us-east-1`, IP ranges, time of day

```json
{
  "Effect": "Allow",
  "Principal": {"AWS": "arn:aws:iam::123456789012:role/SparkJobRole"},
  "Action": ["s3:GetObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::data-lake-prod",
    "arn:aws:s3:::data-lake-prod/input/*"
  ],
  "Condition": {
    "StringEquals": {"aws:RequestedRegion": "us-east-1"}
  }
}
```

### Roles vs Users: The Fundamental Shift

An IAM user is a permanent identity with long-lived credentials (access keys). It represents a human or application with static credentials. Appropriate for: human engineers who need console access, legacy applications that cannot use roles.

An IAM role is a temporary identity that is assumed by a principal (a service, an EC2 instance, another account's role). When a role is assumed, the caller receives temporary security credentials (valid for 1–12 hours). No permanent credentials exist for roles. Appropriate for: all compute workloads (EC2 instances, Lambda, EKS pods, ECS tasks, Spark jobs).

The architectural principle is: **compute workloads should never use IAM user access keys.** They should assume IAM roles. This eliminates the credential leakage risk, enables automatic rotation, and provides better auditability.

---

## 3. Core Concepts

### 3.1 AWS IAM Policy Types

**Identity-based policies:** Attached to an IAM identity (user, group, role). Specifies what actions the identity can perform. The most common policy type.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::data-lake-prod",
        "arn:aws:s3:::data-lake-prod/pipeline-output/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:GetDatabase",
        "glue:GetPartitions"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:123456789012:catalog",
        "arn:aws:glue:us-east-1:123456789012:database/analytics",
        "arn:aws:glue:us-east-1:123456789012:table/analytics/*"
      ]
    }
  ]
}
```

**Resource-based policies:** Attached to a resource (S3 bucket policy, SQS queue policy, Secrets Manager secret policy). Specifies who can access the resource and what they can do. Resource-based policies can grant cross-account access without requiring the caller to assume a role.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::987654321098:role/DataPipelineRole"
      },
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::source-data-bucket",
        "arn:aws:s3:::source-data-bucket/*"
      ]
    }
  ]
}
```

**Trust policies:** A special resource-based policy attached to an IAM role that defines which principals can assume the role. Without a trust policy allowing a principal, the role cannot be assumed even if the role's permissions policy is correct.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

This trust policy allows EC2 instances to assume the role. The `Principal.Service` is the AWS service that is allowed to assume the role. Common services: `ec2.amazonaws.com`, `lambda.amazonaws.com`, `ecs-tasks.amazonaws.com`, `eks.amazonaws.com`.

**Service Control Policies (SCPs):** Organization-level policies that define the maximum permissions any identity in an organizational unit (OU) can have. SCPs do not grant permissions — they restrict the maximum that identity-based policies can grant. Example: an SCP that prevents any IAM action in regions outside `us-east-1` and `us-west-2`.

**Permission Boundaries:** An IAM feature that sets the maximum permissions an identity-based policy can grant to a principal. Used to delegate IAM permission management to developers without allowing them to escalate their own privileges. Example: a developer can create IAM roles for their service, but the permission boundary ensures those roles can only access specific S3 buckets and cannot access billing or IAM itself.

### 3.2 The EC2 Instance Profile

An EC2 instance profile is the mechanism by which an EC2 instance assumes an IAM role. When you launch an EC2 instance with an instance profile, the instance metadata service (IMDS) provides temporary credentials for the associated IAM role. Applications running on the instance can retrieve these credentials from `http://169.254.169.254/latest/meta-data/iam/security-credentials/<role-name>`.

All AWS SDKs (boto3, AWS SDK for Java, etc.) use the **credential provider chain**, which checks for credentials in this order:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. AWS config file (`~/.aws/config`)
4. EC2 instance metadata (IMDS) — for instance profile credentials
5. ECS task metadata — for ECS task role credentials
6. EKS service account token — for IRSA (IAM Roles for Service Accounts)

The correct configuration for a Spark job on EC2 is to use NO explicit credentials — the SDK automatically picks up the instance profile credentials from IMDS. No access keys in code, no `~/.aws/credentials` file needed.

**IMDSv2 (Instance Metadata Service v2):** A more secure version of IMDS that requires a session-oriented request (a PUT to get a token, then GET with the token). Prevents SSRF (Server-Side Request Forgery) attacks that used IMDSv1 to steal credentials. All new EC2 instances should require IMDSv2 only by setting `HttpTokens=required` on the instance metadata options.

### 3.3 IAM Roles for Service Accounts (IRSA) in EKS

When Kafka Connect, Spark, Airflow, or other data workloads run as Kubernetes pods on EKS, they need AWS credentials to access S3, Secrets Manager, or MSK. The naive solution — mounting an IAM user's access keys as a Kubernetes Secret — is insecure (static credentials, no rotation, cluster-wide blast radius if the secret is compromised).

IRSA is the correct mechanism for EKS pods to assume IAM roles:

**How IRSA works:**
1. EKS creates an OIDC (OpenID Connect) provider that can issue tokens for Kubernetes service accounts
2. An IAM role is created with a trust policy that allows the OIDC provider to assume it for a specific Kubernetes service account (identified by namespace and name)
3. The pod's service account is annotated with the IAM role ARN
4. When the pod starts, EKS automatically mounts a projected service account token (a short-lived OIDC token, renewed every 24 hours by default)
5. The pod's AWS SDK uses this token to call `sts:AssumeRoleWithWebIdentity` and receive temporary IAM credentials

```yaml
# Kubernetes ServiceAccount with IRSA annotation
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-job-sa
  namespace: data-pipelines
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/SparkS3Role
```

```json
// IAM Role trust policy for IRSA
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE:sub":
          "system:serviceaccount:data-pipelines:spark-job-sa",
        "oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE:aud":
          "sts.amazonaws.com"
      }
    }
  }]
}
```

The `Condition` is critical: it constrains the trust to a specific Kubernetes service account in a specific namespace. Without this condition, any pod in the cluster with any service account could assume the role.

### 3.4 Cross-Account IAM Roles

The pattern for a data pipeline in account A (platform) to access resources in account B (source):

**Step 1:** Create an IAM role in account B (source) with a trust policy that allows account A to assume it.

```json
// Trust policy on the role in Account B (source account: 987654321098)
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "AWS": "arn:aws:iam::123456789012:role/SparkJobRole"
    },
    "Action": "sts:AssumeRole"
  }]
}
```

**Step 2:** Grant the role in account B the necessary permissions (e.g., S3 read from the source bucket).

```json
// Permissions policy on the role in Account B
{
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:ListBucket"],
    "Resource": ["arn:aws:s3:::source-data-bucket", "arn:aws:s3:::source-data-bucket/*"]
  }]
}
```

**Step 3:** Grant the SparkJobRole in account A permission to assume the role in account B.

```json
// Identity-based policy addition on SparkJobRole in Account A (123456789012)
{
  "Statement": [{
    "Effect": "Allow",
    "Action": "sts:AssumeRole",
    "Resource": "arn:aws:iam::987654321098:role/CrossAccountS3ReadRole"
  }]
}
```

**Step 4:** In the Spark job code, explicitly assume the cross-account role:

```python
import boto3

def get_cross_account_session(role_arn: str, session_name: str):
    """Assume a cross-account IAM role and return a boto3 session."""
    sts = boto3.client('sts')
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
        DurationSeconds=3600,  # 1 hour; max 12 hours for chained role assumptions
    )
    creds = response['Credentials']
    return boto3.Session(
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'],
    )

# Usage:
cross_account_session = get_cross_account_session(
    role_arn='arn:aws:iam::987654321098:role/CrossAccountS3ReadRole',
    session_name='spark-job-daily-run',
)
s3 = cross_account_session.client('s3')
s3.get_object(Bucket='source-data-bucket', Key='input/data.parquet')
```

### 3.5 GCP IAM: Service Accounts and Roles

Google Cloud uses a parallel but distinct IAM model:

**Service Accounts:** The GCP equivalent of IAM roles for compute. A service account is an identity associated with an application or compute resource. Unlike AWS IAM roles (assumed by EC2/ECS/EKS), GCP service accounts are directly bound to resources.

**Predefined Roles:** GCP provides pre-built roles for each service:
- `roles/bigquery.dataViewer`: Read access to BigQuery datasets and tables
- `roles/bigquery.dataEditor`: Read and write access to BigQuery data
- `roles/bigquery.jobUser`: Permission to run BigQuery jobs (required to query, even if dataViewer is set)
- `roles/bigquery.admin`: Full BigQuery admin
- `roles/storage.objectViewer`: Read access to GCS objects
- `roles/storage.objectCreator`: Write access to GCS objects (cannot read)
- `roles/storage.objectAdmin`: Full GCS object access

**Binding:** A GCP IAM binding connects a principal (member) to a role on a specific resource:
```
Principal: serviceAccount:spark-job@project-id.iam.gserviceaccount.com
Role:      roles/bigquery.dataEditor
Resource:  Dataset: my-project.analytics
```

**GCP IAM policy evaluation:** Unlike AWS (where an explicit deny overrides everything), GCP IAM uses a simpler allow-only model at the resource level. However, GCP Organization Policy Service adds deny constraints at the organization level.

### 3.6 Workload Identity (GKE)

Workload Identity is GCP's equivalent of AWS IRSA — the mechanism for GKE pods to authenticate as a GCP service account without needing to mount service account key files.

**How Workload Identity works:**
1. A GCP service account (GSA) is created for the workload
2. A Kubernetes service account (KSA) in GKE is linked to the GSA via an IAM policy binding and a KSA annotation
3. Pods using the KSA automatically receive credentials for the GSA from the GKE metadata server
4. GCP APIs see the request as authenticated by the GSA

```bash
# Step 1: Create the GCP service account
gcloud iam service-accounts create spark-job-sa \
  --display-name="Spark Job Service Account"

# Step 2: Grant the GSA permissions on BigQuery
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:spark-job-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# Step 3: Allow the Kubernetes service account to impersonate the GSA
gcloud iam service-accounts add-iam-policy-binding spark-job-sa@my-project.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:my-project.svc.id.goog[data-pipelines/spark-job-ksa]"

# Step 4: Annotate the Kubernetes service account
kubectl annotate serviceaccount spark-job-ksa \
  --namespace data-pipelines \
  iam.gke.io/gcp-service-account=spark-job-sa@my-project.iam.gserviceaccount.com
```

### 3.7 Auditing: CloudTrail and Cloud Audit Logs

IAM decisions are only half the story — the other half is knowing what actually happened.

**AWS CloudTrail:** Records every API call made to AWS services. Each event includes: who made the call (the principal ARN), what they called (the action), on which resource, from what IP, and the timestamp. For IAM debugging, CloudTrail answers "was this permission ever used?" and "who made this API call?"

```bash
# Find all S3 API calls by a specific role in the last 24 hours
aws cloudtrail lookup-events \
  --lookup-attributes AttributeKey=Username,AttributeValue=SparkJobRole \
  --start-time $(date -u -d '24 hours ago' +%Y-%m-%dT%H:%M:%SZ) \
  --query 'Events[*].[EventName,EventTime,Resources[0].ResourceName]' \
  --output table

# Find all AccessDenied events (failed permission checks)
aws cloudtrail lookup-events \
  --lookup-attributes AttributeKey=EventName,AttributeValue=GetObject \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ) \
  --query 'Events[?ErrorCode==`AccessDenied`].[EventTime,Username,Resources]' \
  --output json
```

**IAM Access Analyzer:** An AWS tool that analyzes IAM policies and resource policies to find over-permissive configurations. Specifically, it identifies resources that are accessible from outside the AWS account or organization — S3 buckets with public access, KMS keys shared with external accounts, IAM roles with cross-account trust relationships.

**AWS Policy Simulator:** A console and CLI tool that simulates what actions a given IAM identity can perform, without making actual API calls. Invaluable for testing policies before deployment.

```bash
# Simulate whether the SparkJobRole can GetObject from the data lake
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::123456789012:role/SparkJobRole \
  --action-names s3:GetObject \
  --resource-arns arn:aws:s3:::data-lake-prod/input/data.parquet \
  --query 'EvaluationResults[0].EvalDecision'
# Output: "allowed" or "implicitDeny" or "explicitDeny"
```

### 3.8 The Principle of Least Privilege in Practice

The principle of least privilege (PoLP) states that any identity should have exactly the permissions necessary to perform its function — no more, no less. In practice, applying PoLP requires:

**Action-level specificity:** Instead of `s3:*`, use the exact actions needed: `["s3:GetObject", "s3:ListBucket"]`. Instead of `glue:*`, use `["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions"]`.

**Resource-level specificity:** Instead of `"Resource": "*"`, specify the exact ARN: `"arn:aws:s3:::data-lake-prod/input/*"`. For Glue, include the exact catalog, database, and table ARNs.

**Condition keys:** Add conditions that further restrict when permissions apply:
```json
{
  "Effect": "Allow",
  "Action": "s3:GetObject",
  "Resource": "arn:aws:s3:::data-lake-prod/*",
  "Condition": {
    "StringEquals": {"aws:RequestedRegion": "us-east-1"},
    "Bool": {"aws:SecureTransport": "true"}
  }
}
```
The `aws:SecureTransport` condition requires HTTPS — denying HTTP requests even if the action would otherwise be allowed.

**Time-based access:** Temporary elevated permissions using IAM roles with short `DurationSeconds` in `AssumeRole` calls, rather than persistent user access.

**IAM Access Advisor:** Shows the last time each IAM permission was used by a principal. Permissions that have never been used after 90 days are candidates for removal.

---

## 4. Hands-On Walkthrough

### 4.1 Inspecting the Current IAM Context

```bash
# Who am I? (Shows the current identity being used)
aws sts get-caller-identity
# Output: {Account: "123456789012", UserId: "AROAEXAMPLE:i-1234567890abcdef0", 
#          Arn: "arn:aws:sts::123456789012:assumed-role/SparkJobRole/i-1234..."}
# The "assumed-role" prefix confirms you're using a role, not a user

# What policies are attached to the current role?
ROLE_NAME=$(aws sts get-caller-identity --query 'Arn' --output text | \
  cut -d'/' -f2)
aws iam list-attached-role-policies \
  --role-name "$ROLE_NAME" \
  --query 'AttachedPolicies[*].[PolicyName,PolicyArn]' \
  --output table

# View the effective permissions of a role (inline + attached)
aws iam get-role --role-name SparkJobRole \
  --query 'Role.{RoleName:RoleName,TrustPolicy:AssumeRolePolicyDocument}' \
  --output json

# Get the policy document for a specific attached policy
aws iam get-policy-version \
  --policy-arn arn:aws:iam::123456789012:policy/SparkS3ReadPolicy \
  --version-id v1 \
  --query 'PolicyVersion.Document' \
  --output json
```

### 4.2 Debugging an AccessDenied Error

```bash
# When you get AccessDenied, first get the full error:
aws s3 cp s3://data-lake-prod/input/test.parquet /tmp/test.parquet
# An error occurred (AccessDenied) when calling the GetObject operation:
# User: arn:aws:sts::123456789012:assumed-role/SparkJobRole/i-0abc123 
# is not authorized to perform: s3:GetObject on resource: 
# "arn:aws:s3:::data-lake-prod/input/test.parquet" 
# because no identity-based policy allows the s3:GetObject action

# Step 1: Simulate what is allowed
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::123456789012:role/SparkJobRole \
  --action-names s3:GetObject \
  --resource-arns arn:aws:s3:::data-lake-prod/input/test.parquet

# Step 2: Check if it's the bucket policy blocking access
aws s3api get-bucket-policy --bucket data-lake-prod | python3 -m json.tool

# Step 3: Check if there's a bucket ACL issue
aws s3api get-bucket-acl --bucket data-lake-prod

# Step 4: Check CloudTrail for the specific denied call
aws cloudtrail lookup-events \
  --lookup-attributes AttributeKey=EventName,AttributeValue=GetObject \
  --start-time $(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --query 'Events[?ErrorCode!=null].[EventTime,ErrorCode,Username]' \
  --output table
```

### 4.3 Setting Up IRSA for a Spark Job on EKS

```bash
# Step 1: Get the OIDC provider URL for the EKS cluster
OIDC_URL=$(aws eks describe-cluster \
  --name data-platform-cluster \
  --query "cluster.identity.oidc.issuer" \
  --output text | sed 's|https://||')

# Step 2: Create the IAM role with trust policy for IRSA
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
cat > /tmp/trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${OIDC_URL}"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "${OIDC_URL}:sub": "system:serviceaccount:data-pipelines:spark-job-sa",
        "${OIDC_URL}:aud": "sts.amazonaws.com"
      }
    }
  }]
}
EOF

aws iam create-role \
  --role-name SparkJobS3Role \
  --assume-role-policy-document file:///tmp/trust-policy.json

# Step 3: Create and attach the permissions policy
cat > /tmp/permissions.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::data-lake-prod",
        "arn:aws:s3:::data-lake-prod/input/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::data-lake-prod/output/*"
    }
  ]
}
EOF

aws iam create-policy \
  --policy-name SparkJobS3Policy \
  --policy-document file:///tmp/permissions.json

aws iam attach-role-policy \
  --role-name SparkJobS3Role \
  --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/SparkJobS3Policy

# Step 4: Annotate the Kubernetes service account
kubectl annotate serviceaccount spark-job-sa \
  --namespace data-pipelines \
  eks.amazonaws.com/role-arn=arn:aws:iam::${ACCOUNT_ID}:role/SparkJobS3Role

# Step 5: Verify from inside a pod
kubectl run -it --rm debug-pod \
  --image=amazon/aws-cli \
  --serviceaccount=spark-job-sa \
  --namespace=data-pipelines \
  -- aws sts get-caller-identity
# Should show the role ARN: arn:aws:sts::123456789012:assumed-role/SparkJobS3Role/...
```

### 4.4 GCP Workload Identity Setup

```bash
# Enable Workload Identity on a GKE cluster
gcloud container clusters update data-platform-cluster \
  --workload-pool=my-project.svc.id.goog \
  --region=us-central1

# Create GCP service account for BigQuery access
gcloud iam service-accounts create bq-pipeline-sa \
  --display-name="BigQuery Pipeline Service Account" \
  --project=my-project

# Grant BigQuery permissions to the service account
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:bq-pipeline-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:bq-pipeline-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Create the Workload Identity binding
gcloud iam service-accounts add-iam-policy-binding \
  bq-pipeline-sa@my-project.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:my-project.svc.id.goog[data-pipelines/bq-pipeline-ksa]"

# Create and annotate the Kubernetes service account
kubectl create serviceaccount bq-pipeline-ksa --namespace data-pipelines
kubectl annotate serviceaccount bq-pipeline-ksa \
  --namespace=data-pipelines \
  iam.gke.io/gcp-service-account=bq-pipeline-sa@my-project.iam.gserviceaccount.com

# Verify Workload Identity is working
kubectl run -it --rm wi-test \
  --image=google/cloud-sdk:slim \
  --serviceaccount=bq-pipeline-ksa \
  --namespace=data-pipelines \
  -- gcloud auth print-access-token
# Should print a valid access token for bq-pipeline-sa
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
iam_diagnostics.py

IAM audit and diagnostic toolkit for data engineers.
Checks effective permissions, detects overly broad policies,
audits service account usage, and validates IRSA configuration.

Dependencies: boto3 (AWS), google-cloud-iam (GCP, optional)
"""

import os
import json
import re
import time
from dataclasses import dataclass, field
from typing import Optional


# ─── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class PolicyStatement:
    effect: str          # Allow or Deny
    actions: list[str]
    resources: list[str]
    conditions: dict
    is_wildcard_action: bool    # True if any action is "*" or "service:*"
    is_wildcard_resource: bool  # True if any resource is "*"


@dataclass
class PolicyAnalysis:
    policy_name: str
    policy_arn: str
    statements: list[PolicyStatement]
    wildcard_action_count: int
    wildcard_resource_count: int
    overpermissive_score: int    # 0=fine, 1=warn, 2=critical
    warnings: list[str]


@dataclass
class RoleAudit:
    role_name: str
    role_arn: str
    trust_principals: list[str]   # who can assume this role
    attached_policies: list[str]
    inline_policy_names: list[str]
    last_used: Optional[str]      # ISO8601 timestamp
    policy_analyses: list[PolicyAnalysis]
    cross_account_trust: bool     # trusts principals from other accounts
    service_trust: list[str]      # services (ec2.amazonaws.com, etc.)


@dataclass
class IRSAConfig:
    cluster_name: str
    oidc_provider_url: str
    role_arn: str
    namespace: str
    service_account_name: str
    trust_is_valid: bool
    warnings: list[str]


# ─── Policy Analyzer ─────────────────────────────────────────────────────────────

def analyze_policy_document(
    policy_name: str,
    policy_arn: str,
    document: dict,
) -> PolicyAnalysis:
    """
    Analyze an IAM policy document for overly broad permissions.
    Returns warnings for wildcard actions and resources.
    """
    statements = []
    wildcard_actions = 0
    wildcard_resources = 0
    warnings = []

    overpermissive_actions = {
        '*', 's3:*', 'ec2:*', 'iam:*', 'sts:*', 'glue:*',
        'secretsmanager:*', 'kms:*', 'cloudwatch:*',
    }
    critical_actions = {'iam:*', 'sts:*', 'kms:*', 'secretsmanager:*'}

    for stmt in document.get('Statement', []):
        actions = stmt.get('Action', [])
        if isinstance(actions, str):
            actions = [actions]
        resources = stmt.get('Resource', [])
        if isinstance(resources, str):
            resources = [resources]

        is_wildcard_action = any(
            a == '*' or a.endswith(':*') for a in actions
        )
        is_wildcard_resource = '*' in resources

        statements.append(PolicyStatement(
            effect=stmt.get('Effect', 'Allow'),
            actions=actions,
            resources=resources,
            conditions=stmt.get('Condition', {}),
            is_wildcard_action=is_wildcard_action,
            is_wildcard_resource=is_wildcard_resource,
        ))

        if stmt.get('Effect') == 'Allow':
            for action in actions:
                if action in overpermissive_actions:
                    wildcard_actions += 1
                    if action in critical_actions:
                        warnings.append(
                            f"CRITICAL: Broad action '{action}' in statement. "
                            f"This is overly permissive for a data pipeline role."
                        )
                    else:
                        warnings.append(
                            f"WARNING: Broad action '{action}'. "
                            f"Scope down to specific actions."
                        )

            if is_wildcard_resource:
                wildcard_resources += 1
                if is_wildcard_action:
                    warnings.append(
                        f"CRITICAL: '*' action on '*' resource — "
                        f"this is equivalent to full admin access."
                    )
                else:
                    warnings.append(
                        f"WARNING: Action(s) {actions[:3]} on '*' resource. "
                        f"Scope to specific ARNs."
                    )

    score = 0
    if wildcard_actions > 0 and wildcard_resources > 0:
        score = 2
    elif wildcard_actions > 0 or wildcard_resources > 0:
        score = 1

    return PolicyAnalysis(
        policy_name=policy_name,
        policy_arn=policy_arn,
        statements=statements,
        wildcard_action_count=wildcard_actions,
        wildcard_resource_count=wildcard_resources,
        overpermissive_score=score,
        warnings=warnings,
    )


# ─── AWS IAM Auditor ─────────────────────────────────────────────────────────────

class AWSIAMAuditor:
    """
    Audits AWS IAM roles for over-permission and validates IRSA configuration.
    Requires: boto3 and appropriate IAM read permissions.
    """

    def __init__(self, region: str = None):
        try:
            import boto3
            self.iam = boto3.client('iam')
            self.sts = boto3.client('sts')
            self.eks = boto3.client(
                'eks',
                region_name=region or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
            )
            self.region = region or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        except ImportError:
            raise RuntimeError("boto3 not installed. Run: pip install boto3")

    def get_caller_identity(self) -> dict:
        """Return the current caller's ARN, account, and user ID."""
        return self.sts.get_caller_identity()

    def audit_role(self, role_name: str) -> RoleAudit:
        """
        Perform a comprehensive audit of an IAM role:
        - Trust policy analysis
        - Attached policy analysis
        - Inline policy analysis
        - Last-used timestamp
        """
        role_resp = self.iam.get_role(RoleName=role_name)
        role = role_resp['Role']

        # Parse trust policy
        trust_doc = role['AssumeRolePolicyDocument']
        trust_principals = []
        service_trust = []
        cross_account = False
        my_account = self.get_caller_identity().get('Account', '')

        for stmt in trust_doc.get('Statement', []):
            principal = stmt.get('Principal', {})
            if isinstance(principal, dict):
                for ptype, pval in principal.items():
                    vals = [pval] if isinstance(pval, str) else pval
                    for v in vals:
                        if ptype == 'Service':
                            service_trust.append(v)
                        elif ptype in ('AWS', 'Federated'):
                            trust_principals.append(v)
                            # Check for cross-account: principal ARN has different account
                            if my_account and my_account not in v and 'amazonaws.com' not in v:
                                cross_account = True
            elif isinstance(principal, str) and principal == '*':
                trust_principals.append('*')
                cross_account = True  # wildcard = anyone

        # Attached policies
        attached = self.iam.list_attached_role_policies(RoleName=role_name)
        attached_arns = [p['PolicyArn'] for p in attached['AttachedPolicies']]
        attached_names = [p['PolicyName'] for p in attached['AttachedPolicies']]

        # Inline policies
        inline = self.iam.list_role_policies(RoleName=role_name)
        inline_names = inline['PolicyNames']

        # Analyze attached policies
        policy_analyses = []
        for policy_arn in attached_arns:
            policy_resp = self.iam.get_policy(PolicyArn=policy_arn)
            version_id = policy_resp['Policy']['DefaultVersionId']
            version_resp = self.iam.get_policy_version(
                PolicyArn=policy_arn, VersionId=version_id
            )
            doc = version_resp['PolicyVersion']['Document']
            analysis = analyze_policy_document(
                policy_resp['Policy']['PolicyName'],
                policy_arn,
                doc,
            )
            policy_analyses.append(analysis)

        # Analyze inline policies
        for policy_name in inline_names:
            inline_resp = self.iam.get_role_policy(
                RoleName=role_name, PolicyName=policy_name
            )
            doc = inline_resp['PolicyDocument']
            analysis = analyze_policy_document(policy_name, f'inline:{role_name}', doc)
            policy_analyses.append(analysis)

        # Last used
        last_used = role.get('RoleLastUsed', {}).get('LastUsedDate')
        last_used_str = last_used.isoformat() if last_used else None

        return RoleAudit(
            role_name=role_name,
            role_arn=role['Arn'],
            trust_principals=trust_principals,
            attached_policies=attached_names,
            inline_policy_names=inline_names,
            last_used=last_used_str,
            policy_analyses=policy_analyses,
            cross_account_trust=cross_account,
            service_trust=service_trust,
        )

    def simulate_permission(
        self,
        role_arn: str,
        action: str,
        resource: str,
    ) -> str:
        """
        Simulate whether a role can perform an action on a resource.
        Returns: 'allowed', 'implicitDeny', or 'explicitDeny'
        """
        result = self.iam.simulate_principal_policy(
            PolicySourceArn=role_arn,
            ActionNames=[action],
            ResourceArns=[resource],
        )
        return result['EvaluationResults'][0]['EvalDecision']

    def check_irsa_config(
        self,
        cluster_name: str,
        role_name: str,
        namespace: str,
        sa_name: str,
    ) -> IRSAConfig:
        """
        Validate that an IRSA configuration is correctly set up:
        - Cluster has OIDC provider
        - Role trust policy references the correct OIDC provider
        - Trust policy conditions match the namespace and service account
        """
        warnings = []

        # Get cluster OIDC URL
        cluster = self.eks.describe_cluster(name=cluster_name)
        oidc_url = cluster['cluster']['identity']['oidc']['issuer']
        oidc_provider = oidc_url.replace('https://', '')

        # Get role trust policy
        role = self.iam.get_role(RoleName=role_name)
        trust_doc = role['Role']['AssumeRolePolicyDocument']
        role_arn = role['Role']['Arn']

        account_id = self.get_caller_identity()['Account']
        expected_principal = (
            f"arn:aws:iam::{account_id}:oidc-provider/{oidc_provider}"
        )
        expected_sub = f"system:serviceaccount:{namespace}:{sa_name}"

        trust_is_valid = False
        for stmt in trust_doc.get('Statement', []):
            if stmt.get('Action') != 'sts:AssumeRoleWithWebIdentity':
                continue
            principal = stmt.get('Principal', {}).get('Federated', '')
            conditions = stmt.get('Condition', {})
            sub_condition = conditions.get('StringEquals', {}).get(
                f'{oidc_provider}:sub', ''
            )
            aud_condition = conditions.get('StringEquals', {}).get(
                f'{oidc_provider}:aud', ''
            )

            if principal == expected_principal:
                if sub_condition != expected_sub:
                    warnings.append(
                        f"Trust policy :sub condition is '{sub_condition}', "
                        f"expected '{expected_sub}'. "
                        f"Any pod in any namespace can assume this role."
                    )
                if aud_condition != 'sts.amazonaws.com':
                    warnings.append(
                        f"Trust policy :aud condition should be 'sts.amazonaws.com', "
                        f"got '{aud_condition}'."
                    )
                trust_is_valid = True
            else:
                warnings.append(
                    f"Principal '{principal}' does not match "
                    f"expected OIDC provider '{expected_principal}'."
                )

        if not trust_is_valid:
            warnings.append(
                "No valid AssumeRoleWithWebIdentity statement found in trust policy."
            )

        return IRSAConfig(
            cluster_name=cluster_name,
            oidc_provider_url=oidc_url,
            role_arn=role_arn,
            namespace=namespace,
            service_account_name=sa_name,
            trust_is_valid=trust_is_valid,
            warnings=warnings,
        )

    def find_unused_permissions(
        self,
        role_name: str,
        days_threshold: int = 90,
    ) -> list[str]:
        """
        Use IAM Access Advisor to find permissions that have not been used
        in the past `days_threshold` days. These are candidates for removal.
        """
        role_arn = self.iam.get_role(RoleName=role_name)['Role']['Arn']

        # Generate service last accessed report
        job = self.iam.generate_service_last_accessed_details(Arn=role_arn)
        job_id = job['JobId']

        # Poll until complete
        for _ in range(20):
            time.sleep(2)
            result = self.iam.get_service_last_accessed_details(JobId=job_id)
            if result['JobStatus'] == 'COMPLETED':
                break
        else:
            return ['Timeout waiting for access advisor report']

        cutoff = time.time() - (days_threshold * 86400)
        unused = []
        for svc in result['ServicesLastAccessed']:
            last_auth = svc.get('LastAuthenticatedDate')
            if last_auth is None:
                unused.append(
                    f"{svc['ServiceName']} (namespace: {svc['ServiceNamespace']}) "
                    f"— NEVER USED"
                )
            elif last_auth.timestamp() < cutoff:
                unused.append(
                    f"{svc['ServiceName']} (namespace: {svc['ServiceNamespace']}) "
                    f"— last used {last_auth.date()}"
                )

        return unused


# ─── Credential Chain Inspector ──────────────────────────────────────────────────

def inspect_credential_chain() -> dict:
    """
    Inspect which credential source the current boto3 session is using.
    Helps diagnose credential-related issues.
    """
    result = {
        'source': 'unknown',
        'identity': None,
        'warnings': [],
    }

    # Check environment variables
    if os.environ.get('AWS_ACCESS_KEY_ID'):
        key = os.environ['AWS_ACCESS_KEY_ID']
        result['source'] = 'environment_variables'
        if key.startswith('AKIA'):
            result['warnings'].append(
                "AKIA prefix indicates a long-term IAM user key. "
                "Use instance profile or IRSA for compute workloads."
            )
        elif key.startswith('ASIA'):
            result['source'] = 'assumed_role_via_env'
            result['warnings'].append(
                "ASIA prefix indicates temporary credentials (assumed role). "
                "This is acceptable but verify they are being auto-rotated."
            )

    # Check ~/.aws/credentials
    creds_file = os.path.expanduser('~/.aws/credentials')
    if os.path.exists(creds_file) and not os.environ.get('AWS_ACCESS_KEY_ID'):
        result['source'] = 'credentials_file'
        result['warnings'].append(
            "Using ~/.aws/credentials file. On compute instances, "
            "prefer instance profile/IRSA over credential files."
        )

    # Try to get caller identity to confirm credentials work
    try:
        import boto3
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        result['identity'] = {
            'arn': identity['Arn'],
            'account': identity['Account'],
            'user_id': identity['UserId'],
        }
        # Determine source from ARN
        arn = identity['Arn']
        if ':assumed-role/' in arn:
            result['source'] = 'assumed_role'
        elif ':user/' in arn:
            result['source'] = 'iam_user'
            result['warnings'].append(
                "Running as an IAM user. On EC2/EKS, use an instance profile "
                "or IRSA instead. IAM user keys are long-lived and higher risk."
            )
        elif ':root' in arn:
            result['source'] = 'root'
            result['warnings'].append(
                "CRITICAL: Running as the AWS root account. "
                "Never use root credentials for operational workloads."
            )
    except Exception as e:
        result['warnings'].append(f"Could not validate credentials: {e}")

    return result


# ─── Report ─────────────────────────────────────────────────────────────────────

def iam_audit_report(
    role_names: Optional[list[str]] = None,
    check_credential_chain: bool = True,
) -> None:
    """Print a comprehensive IAM audit report."""
    print("=" * 70)
    print("IAM AUDIT REPORT")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Credential chain
    if check_credential_chain:
        print("\n[1] CREDENTIAL CHAIN")
        chain = inspect_credential_chain()
        print(f"  Source: {chain['source']}")
        if chain.get('identity'):
            arn = chain['identity']['arn']
            print(f"  ARN:    {arn}")
            print(f"  Acct:   {chain['identity']['account']}")
        for w in chain.get('warnings', []):
            icon = "🚨" if "CRITICAL" in w else "⚠️ "
            print(f"  {icon}  {w}")

    # Role audits
    if role_names:
        try:
            auditor = AWSIAMAuditor()
            print("\n[2] ROLE AUDITS")
            for role_name in role_names:
                print(f"\n  Role: {role_name}")
                try:
                    audit = auditor.audit_role(role_name)
                    print(f"  ARN:       {audit.role_arn}")
                    print(f"  Last used: {audit.last_used or 'Never'}")
                    print(f"  Trusts:    {audit.service_trust or audit.trust_principals}")
                    if audit.cross_account_trust:
                        print(f"  ⚠️  Cross-account trust detected!")
                    print(f"  Policies: {audit.attached_policies}")
                    for pa in audit.policy_analyses:
                        score_icon = ['✓', '⚠️ ', '🚨'][pa.overpermissive_score]
                        print(f"\n    {score_icon} Policy: {pa.policy_name}")
                        for w in pa.warnings:
                            print(f"       {w}")
                except Exception as e:
                    print(f"  Error auditing {role_name}: {e}")
        except RuntimeError as e:
            print(f"  {e}")
    else:
        _demo_report()

    print("\n" + "=" * 70)


def _demo_report():
    """Demo without AWS credentials."""
    print("\n  [DEMO MODE]\n")

    # Analyze sample policies
    sample_policies = [
        {
            'name': 'OverlyBroadS3Policy',
            'arn': 'arn:aws:iam::123456789012:policy/OverlyBroadS3Policy',
            'document': {
                'Statement': [{
                    'Effect': 'Allow',
                    'Action': 's3:*',
                    'Resource': '*',
                }]
            }
        },
        {
            'name': 'LeastPrivilegeSparkPolicy',
            'arn': 'arn:aws:iam::123456789012:policy/LeastPrivilegeSparkPolicy',
            'document': {
                'Statement': [
                    {
                        'Effect': 'Allow',
                        'Action': ['s3:GetObject', 's3:ListBucket'],
                        'Resource': [
                            'arn:aws:s3:::data-lake-prod',
                            'arn:aws:s3:::data-lake-prod/input/*',
                        ],
                    },
                    {
                        'Effect': 'Allow',
                        'Action': ['s3:PutObject'],
                        'Resource': 'arn:aws:s3:::data-lake-prod/output/*',
                    },
                ]
            }
        },
        {
            'name': 'DangerousIAMPolicy',
            'arn': 'arn:aws:iam::123456789012:policy/DangerousIAMPolicy',
            'document': {
                'Statement': [{
                    'Effect': 'Allow',
                    'Action': ['iam:*', 'sts:*'],
                    'Resource': '*',
                }]
            }
        },
    ]

    print("  Policy Analysis Examples:")
    for p in sample_policies:
        analysis = analyze_policy_document(p['name'], p['arn'], p['document'])
        score_icon = ['✓', '⚠️ ', '🚨'][analysis.overpermissive_score]
        print(f"\n  {score_icon} {analysis.policy_name}")
        for w in analysis.warnings:
            print(f"     {w}")

    # Credential chain check
    print("\n  Credential Chain:")
    chain = inspect_credential_chain()
    print(f"  Source: {chain['source']}")
    for w in chain.get('warnings', []):
        print(f"  ⚠️   {w}")


# ─── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="IAM diagnostic toolkit")
    parser.add_argument('--roles', nargs='+', metavar='ROLE_NAME',
                        help='IAM role names to audit')
    parser.add_argument('--simulate', nargs=3,
                        metavar=('ROLE_ARN', 'ACTION', 'RESOURCE'),
                        help='Simulate permission: ROLE_ARN ACTION RESOURCE')
    parser.add_argument('--who-am-i', action='store_true',
                        help='Show current IAM identity and credential source')
    args = parser.parse_args()

    if args.who_am_i:
        chain = inspect_credential_chain()
        print(f"Credential source: {chain['source']}")
        if chain.get('identity'):
            print(f"ARN:    {chain['identity']['arn']}")
            print(f"Acct:   {chain['identity']['account']}")
        for w in chain.get('warnings', []):
            print(f"⚠️  {w}")
    elif args.simulate:
        role_arn, action, resource = args.simulate
        try:
            auditor = AWSIAMAuditor()
            decision = auditor.simulate_permission(role_arn, action, resource)
            icon = '✓' if decision == 'allowed' else '✗'
            print(f"{icon} {action} on {resource}: {decision}")
        except RuntimeError as e:
            print(f"Error: {e}")
    else:
        iam_audit_report(role_names=args.roles)
```

---

## 6. Visual Reference

### AWS IAM Decision Flow

```
API Call: s3:GetObject on arn:aws:s3:::data-lake/input/data.parquet
                                 ↓
                    Is caller the root account?
                    Yes → Allow (unless SCP blocks)
                    No  ↓
                    ─────────────────────────────
                    Collect all applicable policies:
                      - Identity-based policies (role's attached + inline)
                      - Resource-based policies (S3 bucket policy)
                      - SCPs (if in AWS Organization)
                      - Permission boundaries (if set)
                    ─────────────────────────────
                    Is there an explicit Deny in ANY policy?
                    Yes → DENY ← (deny always wins)
                    No  ↓
                    Is there an Allow in identity-based AND/OR resource-based policy?
                    No  → DENY (implicit deny — default closed)
                    Yes → ALLOW
```

### Cross-Account Role Assumption Chain

```
Account A: 123456789012 (Platform)          Account B: 987654321098 (Source)
┌────────────────────────────────┐          ┌────────────────────────────────┐
│                                │          │                                │
│  SparkJobRole                  │          │  CrossAccountS3ReadRole        │
│  ┌──────────────────────────┐  │          │  ┌──────────────────────────┐  │
│  │ Permissions Policy       │  │  sts:    │  │ Trust Policy             │  │
│  │ ─────────────────────── │  │ AssumeRole│  │ ─────────────────────── │  │
│  │ sts:AssumeRole on       ├──┼──────────►│  │ Principal: SparkJobRole  │  │
│  │ CrossAccountS3ReadRole  │  │          │  │ in Account A             │  │
│  └──────────────────────────┘  │          │  └──────────────────────────┘  │
│                                │          │  ┌──────────────────────────┐  │
│                                │          │  │ Permissions Policy       │  │
│                                │          │  │ ─────────────────────── │  │
│                                │          │  │ s3:GetObject on          │  │
│                                │          │  │ source-bucket/*          │  │
│                                │          │  └──────────────────────────┘  │
└────────────────────────────────┘          └────────────────────────────────┘

Spark code calls: sts.assume_role(CrossAccountS3ReadRole ARN)
Receives: temporary credentials (valid 1–12 hours)
Uses credentials to: s3.get_object(Bucket='source-bucket', ...)
```

### IRSA vs Static Credentials

```
❌ Wrong: Static credentials in code
   kubectl create secret generic aws-creds \
     --from-literal=AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
     --from-literal=AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI...
   
   Risk: Key leaks → any code in cluster can use it → no rotation
   Audit: CloudTrail shows role session name, not which pod

✓ Correct: IRSA (IAM Roles for Service Accounts)
   Kubernetes SA annotation → OIDC token → sts:AssumeRoleWithWebIdentity
   → Temporary creds (24hr) → automatic rotation
   
   Scope: Only pods using spark-job-sa in data-pipelines namespace
   Audit: CloudTrail shows "system:serviceaccount:data-pipelines:spark-job-sa"
```

### GCP IAM Hierarchy

```
Organization: my-org.com
  └── Folder: Data Platform
       ├── Project: data-platform-prod
       │    ├── IAM: data-engineer@my-org.com → roles/bigquery.dataEditor
       │    ├── IAM: spark-job-sa@... → roles/bigquery.dataEditor
       │    │                         → roles/bigquery.jobUser
       │    └── IAM: airflow-sa@...   → roles/bigquery.dataViewer
       │
       └── Project: data-platform-dev
            └── IAM: data-engineer@my-org.com → roles/bigquery.admin
```

---

## 7. Common Mistakes

**Mistake 1: Using IAM user access keys for compute workloads.** A Kafka Connect worker that uses `AWS_ACCESS_KEY_ID` from an environment variable is using a permanent credential that never rotates, has a fixed blast radius (if stolen, anyone can use it indefinitely), and is impossible to audit properly (CloudTrail shows the user, not the specific pod or job). Use EC2 instance profiles or EKS IRSA instead. Access keys are appropriate only for human developers accessing the console/CLI from their laptops.

**Mistake 2: Not constraining IRSA trust policy conditions to a specific service account.** An IRSA trust policy without the `StringEquals` condition on `:sub` allows any pod in the cluster (regardless of namespace or service account) to assume the role. The condition `"oidc.../oidc-provider:sub": "system:serviceaccount:data-pipelines:spark-job-sa"` is mandatory. Without it, a compromised pod in a different namespace can steal S3 access.

**Mistake 3: Granting `s3:*` instead of specific S3 actions.** A data pipeline that reads input and writes output needs: `s3:GetObject` (read), `s3:ListBucket` (list), `s3:PutObject` (write). Granting `s3:*` additionally allows `s3:DeleteObject`, `s3:DeleteBucket`, `s3:PutBucketPolicy`, and hundreds of other actions the pipeline will never use. Each extra action is a privilege escalation vector if the pipeline is compromised.

**Mistake 4: Forgetting the `s3:ListBucket` permission when reading S3 with Spark.** Spark's S3A connector calls `ListObjectsV2` (mapped to `s3:ListBucket` on the bucket resource, not the object resource) before reading files. A policy that only grants `s3:GetObject` on `arn:aws:s3:::bucket/*` but not `s3:ListBucket` on `arn:aws:s3:::bucket` (note: no trailing `/*`) causes `AccessDenied` on list operations. Both the bucket ARN (for ListBucket) and the object ARN (for GetObject) must be in the resource list.

**Mistake 5: Over-relying on the root account for initial setup.** Using the AWS root account for anything beyond account setup (billing, creating the first IAM admin user) is a security anti-pattern. The root account cannot be restricted by SCPs, has unlimited permissions, and should have no access keys. Use IAM admin users or AWS SSO for all operational access.

**Mistake 6: Not using conditions to enforce encryption in transit.** IAM policies can enforce HTTPS by adding `"Condition": {"Bool": {"aws:SecureTransport": "true"}}`. Without this, S3 objects can be read over plain HTTP. For Kafka on MSK with TLS, IAM policies can require TLS by adding the condition — a belt-and-suspenders approach alongside network-level controls.

---

## 8. Production Failure Scenarios

### Scenario 1: Airflow DAG Failing with AccessDenied After IAM Policy Update

**Symptoms:** Airflow DAGs that use `S3Hook` to write results to a staging bucket start failing with `botocore.exceptions.ClientError: An error occurred (AccessDenied)` 30 minutes after a routine IAM policy update.

**Root cause:** A security engineer updated the IAM policy attached to the Airflow task role to remove `s3:*` (correctly applying least privilege), but the replacement specific-action policy (`s3:GetObject`, `s3:PutObject`, `s3:ListBucket`) was attached to a different policy version. The old policy version is still the default version. Additionally, the new policy grants `s3:PutObject` on `arn:aws:s3:::staging-bucket/results/*` but Airflow's S3Hook also calls `s3:ListBucketMultipartUploads` when aborting failed multipart uploads — an action not included in the new policy.

**Diagnosis:**
```bash
# Get the exact error details from CloudTrail
aws cloudtrail lookup-events \
  --lookup-attributes AttributeKey=Username,AttributeValue=AirflowTaskRole \
  --query 'Events[?ErrorCode!=null].{Time:EventTime,Action:EventName,Error:ErrorCode,Msg:ErrorMessage}' \
  --output table

# Simulate the failing action
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::123456789012:role/AirflowTaskRole \
  --action-names s3:ListBucketMultipartUploads \
  --resource-arns arn:aws:s3:::staging-bucket \
  --query 'EvaluationResults[0].EvalDecision'
# Output: implicitDeny
```

**Fix:** Add `s3:ListBucketMultipartUploads` to the IAM policy, and also verify the default policy version is the one with the correct actions.

### Scenario 2: IRSA Not Working — Pod Still Getting 403

**Symptoms:** A Spark pod with IRSA configured is getting `AccessDenied` when calling S3. The pod logs show `Unable to load AWS credentials from any provider in the chain`.

**Root cause — three possible causes (check in order):**

1. The Kubernetes service account annotation is missing the role ARN
2. The IAM trust policy's `:sub` condition references the wrong namespace/SA name
3. The EKS cluster doesn't have the OIDC provider registered in IAM

```bash
# Check 1: Service account annotation
kubectl get serviceaccount spark-job-sa -n data-pipelines \
  -o jsonpath='{.metadata.annotations}'
# Expected: {"eks.amazonaws.com/role-arn": "arn:aws:iam::123456789012:role/SparkJobS3Role"}

# Check 2: Trust policy conditions
aws iam get-role --role-name SparkJobS3Role \
  --query 'Role.AssumeRolePolicyDocument.Statement[0].Condition' \
  --output json
# Verify: sub = "system:serviceaccount:data-pipelines:spark-job-sa"

# Check 3: OIDC provider is registered
OIDC_URL=$(aws eks describe-cluster --name data-platform-cluster \
  --query "cluster.identity.oidc.issuer" --output text | sed 's|https://||')
aws iam list-open-id-connect-providers | grep $OIDC_URL
# Empty output → OIDC provider not registered → IRSA cannot work

# Fix Check 3:
eksctl utils associate-iam-oidc-provider \
  --cluster data-platform-cluster --approve
```

### Scenario 3: Cross-Account S3 Access Denied Despite Correct Role

**Symptoms:** A Spark job in account A assumes the cross-account role in account B and then gets `AccessDenied` on S3 GetObject, even though the cross-account role has `s3:GetObject` in its permissions policy.

**Root cause:** S3 bucket policies take precedence in cross-account scenarios differently than same-account. In cross-account S3 access, BOTH the IAM role policy (in the accessing account) AND the S3 bucket policy (in the bucket's account) must explicitly allow the access. The bucket in account B has a bucket policy that only allows access from `arn:aws:iam::987654321098:root` (same account), which implicitly denies the cross-account role in account A.

**The cross-account S3 access rule:** For principal in account A to access a bucket in account B: (1) the IAM role in account A must allow the S3 action, AND (2) the S3 bucket policy in account B must explicitly allow the principal from account A. If the bucket policy is absent or doesn't include account A, the request is denied.

**Fix:** Add a statement to the S3 bucket policy in account B:
```json
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::123456789012:role/SparkJobRole"
  },
  "Action": ["s3:GetObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::source-data-bucket",
    "arn:aws:s3:::source-data-bucket/*"
  ]
}
```

### Scenario 4: Privilege Escalation via Overly Broad IAM Role Creation Permission

**Symptoms:** A security audit reveals that a data engineer's IAM role has `iam:CreateRole` and `iam:AttachRolePolicy` permissions (granted so they could create roles for their Spark jobs). The audit finds that the engineer created a role with `AdministratorAccess` policy attached — effectively giving themselves admin access.

**Root cause:** `iam:CreateRole` + `iam:AttachRolePolicy` without a permission boundary is a privilege escalation vector. A user who can create roles and attach any policy can create a role with administrator access and then assume that role.

**Fix — Permission Boundaries:**
```json
// Boundary policy: caps maximum permissions for any role the engineer creates
{
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:*", "glue:*", "emr:*"],
    "Resource": "*"
  },
  {
    "Effect": "Deny",
    "Action": ["iam:*", "sts:AssumeRole", "organizations:*"],
    "Resource": "*"
  }]
}
```

```json
// Add to the engineer's policy: can only create roles WITH the boundary
{
  "Effect": "Allow",
  "Action": ["iam:CreateRole", "iam:AttachRolePolicy"],
  "Resource": "*",
  "Condition": {
    "StringEquals": {
      "iam:PermissionsBoundary": "arn:aws:iam::123456789012:policy/DataEngineerBoundary"
    }
  }
}
```

---

## 9. Performance and Tuning

### Credential Refresh Optimization

**AWS SDK credential caching:** The AWS SDK caches temporary credentials and automatically refreshes them before they expire. For IRSA, the default token expiry is 24 hours. For assumed roles, the default is 1 hour. Applications do not need to manage token refresh manually — the SDK handles it.

**STS token caching for cross-account assumptions:** If a Spark job assumes a cross-account role every task, the overhead of the `sts:AssumeRole` API call adds ~50–100 ms per assumption. Cache the session at the application level (refresh when `Credentials.Expiration` is within 5 minutes) rather than assuming the role per task.

```python
import boto3
from datetime import datetime, timezone

_cached_session = None
_session_expiry = None

def get_cross_account_session(role_arn: str) -> boto3.Session:
    """Get a cached cross-account session, refreshing when near expiry."""
    global _cached_session, _session_expiry

    now = datetime.now(timezone.utc)
    if _cached_session is None or (
        _session_expiry and (_session_expiry - now).total_seconds() < 300
    ):
        sts = boto3.client('sts')
        resp = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName='spark-job',
            DurationSeconds=3600,
        )
        creds = resp['Credentials']
        _cached_session = boto3.Session(
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken'],
        )
        _session_expiry = creds['Expiration']

    return _cached_session
```

### GCP Service Account Key Rotation (Workload Identity eliminates this)

When using Workload Identity (the correct approach), service account keys are never created or rotated — credentials are short-lived tokens issued by the metadata server. If for legacy reasons static service account keys must be used, rotate them every 90 days and alert on keys older than 60 days.

---

## 10. Interview Q&A

**Q1: Explain the difference between an IAM trust policy and an IAM permissions policy. Why does a role need both?**

A permissions policy and a trust policy serve completely different functions for an IAM role. The permissions policy defines what the role is allowed to do — it specifies actions (like `s3:GetObject`), resources (like a specific S3 bucket ARN), and conditions under which those actions are permitted. The trust policy defines who is allowed to assume the role — it specifies which principals (AWS services, other IAM roles, external identity providers) can call `sts:AssumeRole` to obtain temporary credentials for the role.

A role needs both because they answer two distinct security questions. The trust policy answers "should this caller be allowed to become this role?" The permissions policy answers "given that the caller has become this role, what can they do?" Without a trust policy, nobody can assume the role even if the permissions policy is perfect. Without a permissions policy, anyone who assumes the role has no effective permissions. For an EC2 instance running a Spark job: the trust policy has `Principal: {Service: "ec2.amazonaws.com"}` (the EC2 service is trusted to assume this role), and the permissions policy has the specific S3 and Glue actions the Spark job needs.

The practical implication for debugging: when a Spark job gets `AccessDenied` when trying to assume a role (error: `is not authorized to perform: sts:AssumeRole`), the trust policy is wrong or missing. When it successfully assumes the role but then gets `AccessDenied` on `s3:GetObject`, the permissions policy is wrong or incomplete. These are two separate configuration problems with separate fixes.

**Q2: What is IAM Roles for Service Accounts (IRSA), how does it work, and what are the security properties that make it better than mounting access keys as Kubernetes secrets?**

IRSA is the mechanism by which EKS pods authenticate to AWS services using IAM roles, without any long-lived credentials. It works through an OIDC (OpenID Connect) chain: EKS acts as an OIDC identity provider, issuing signed tokens that identify specific Kubernetes service accounts. When a pod starts with a service account that has an IRSA annotation, Kubernetes automatically mounts a projected volume with a short-lived OIDC token identifying the service account. The AWS SDK detects this token (via the `AWS_WEB_IDENTITY_TOKEN_FILE` environment variable injected by EKS) and calls `sts:AssumeRoleWithWebIdentity`, exchanging the OIDC token for temporary IAM credentials (valid up to 24 hours, auto-renewed).

The security properties that make this superior to static credentials are scoping, rotation, and auditability. Scoping: the IRSA trust policy's `StringEquals` condition on the `:sub` claim pins the trust to a specific Kubernetes service account in a specific namespace — `system:serviceaccount:data-pipelines:spark-job-sa`. No other pod, even in the same cluster, can assume this role by default. Static access keys mounted as Kubernetes secrets can be read by any pod with access to the secret, or stolen if the secret is misconfigured. Rotation: IRSA credentials are temporary (hours), automatically renewed, and never stored on disk. Static access keys are permanent until explicitly rotated. Auditability: CloudTrail records IRSA assumptions with the exact service account name (`system:serviceaccount:...`), enabling precise attribution of which pod made which API call. Static key CloudTrail records show only the access key ID, which is less specific.

**Q3: Describe the principle of least privilege and explain how you apply it when writing IAM policies for a data pipeline. Give concrete examples.**

The principle of least privilege states that any identity should have exactly the permissions necessary for its function — no more. In practice for data engineering IAM policies, this means three things: action-level specificity, resource-level specificity, and condition constraints.

Action-level specificity means replacing wildcard actions with the exact operations needed. A Spark job that reads from S3, queries Glue, and writes back to S3 needs `["s3:GetObject", "s3:ListBucket"]` for reads, `["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions"]` for Glue, and `["s3:PutObject"]` for writes. Not `s3:*` or `glue:*`. This matters because `s3:*` includes `s3:DeleteBucket`, `s3:PutBucketPolicy`, and `s3:PutBucketAcl` — all of which could allow the pipeline to destroy data or misconfigure security if the job's code is exploited.

Resource-level specificity means specifying exact ARNs instead of `"Resource": "*"`. The S3 read policy should have `"Resource": ["arn:aws:s3:::data-lake-prod", "arn:aws:s3:::data-lake-prod/input/*"]` — the bucket ARN for `ListBucket` and the prefix ARN for `GetObject`. Not the entire `data-lake-prod` bucket (which would allow reading output data, other teams' data, etc.) and certainly not all S3 buckets.

Condition constraints add a final layer: `"Condition": {"Bool": {"aws:SecureTransport": "true"}}` enforces HTTPS, preventing S3 reads over plain HTTP. `"StringEquals": {"aws:RequestedRegion": "us-east-1"}` prevents the credentials from being used to access resources in unexpected regions — a common data exfiltration technique that exploits the fact that IAM is global while data buckets are regional.

---

## 11. Cross-Question Chain

**Interviewer:** Your Spark job is running on EMR in a VPC and needs to read from an S3 bucket in the same account, query the Glue catalog, and write results back to S3. Walk me through designing the IAM role for this job.

**Candidate:** The first step is defining what the job actually needs to do. Reading from S3 requires `s3:GetObject` on the input prefix and `s3:ListBucket` on the bucket (a common mistake is forgetting `ListBucket`). The Glue catalog queries require `glue:GetDatabase`, `glue:GetTable`, and `glue:GetPartitions` on the specific catalog, database, and tables — not `glue:*`. Writing results requires `s3:PutObject` on the output prefix. I'd also need `s3:GetObject` on the output prefix if Spark needs to read back what it wrote for deduplication checks.

The IAM role would have a trust policy allowing `ec2.amazonaws.com` to assume it (EMR runs on EC2). The instance profile attached to the EMR cluster references this role. The permissions policy would have three statements: one for S3 reads, one for Glue reads, one for S3 writes — each scoped to the exact resource ARNs. I'd add a condition `"aws:SecureTransport": "true"` on the S3 statements to enforce HTTPS, and scope the Glue resources to the specific catalog ARN, database ARN, and table ARN pattern.

**Interviewer:** Now the architecture changes: the S3 bucket is in a different AWS account. What changes?

**Candidate:** Cross-account S3 access requires changes on both sides. In the platform account (where EMR runs), the Spark job role needs permission to call `sts:AssumeRole` on the cross-account role ARN in the source account. In the source account, I create a new IAM role with two parts: a trust policy that lists the Spark job role ARN from the platform account as the principal allowed to assume it, and a permissions policy granting `s3:GetObject` and `s3:ListBucket` on the source bucket.

Critically, I also need to add a statement to the source bucket's S3 bucket policy explicitly allowing the cross-account role's ARN — because cross-account S3 access requires both the IAM role policy AND the bucket policy to allow it. Same-account access only needs the IAM role policy. In the Spark job code, I explicitly call `sts.assume_role` to get temporary credentials for the cross-account role, then create an S3 client with those credentials.

**Interviewer:** How does this change if the Spark job moves to EKS instead of EMR?

**Candidate:** On EKS, the correct mechanism is IRSA instead of an EC2 instance profile. The fundamental difference is scope: an EMR instance profile gives all code running on all EMR nodes the same IAM role. IRSA scopes credentials to a specific Kubernetes service account in a specific namespace — only pods using `spark-job-sa` in `data-pipelines` can assume the role. This is significantly more fine-grained.

The implementation: I create a Kubernetes ServiceAccount `spark-job-sa` in the `data-pipelines` namespace. The IAM role's trust policy changes from `Principal: {Service: "ec2.amazonaws.com"}` to `Principal: {Federated: "arn:aws:iam::<account>:oidc-provider/<oidc-url>"}` with a `StringEquals` condition on `<oidc-url>:sub = "system:serviceaccount:data-pipelines:spark-job-sa"`. The Kubernetes ServiceAccount gets an annotation `eks.amazonaws.com/role-arn: arn:aws:iam::<account>:role/SparkJobRole`. Pods that reference this service account automatically get the IRSA environment variables injected, and the AWS SDK picks them up automatically — no code changes needed in the Spark job.

**Interviewer:** A junior engineer on your team is working on a new pipeline and asks for `s3:*` and `iam:CreateRole` permissions on `"Resource": "*"` for their development role. What do you do?

**Candidate:** I'd approve neither as-is and explain why, then offer the correct scoped alternatives. `s3:*` on `"Resource": "*"` is dangerous in two ways: it grants write and delete access to every S3 bucket in the account (including production data lakes), and it grants bucket management permissions like `s3:PutBucketPolicy` (which could misconfigure other buckets' security). The correct scope is specific actions (`s3:GetObject`, `s3:PutObject`, `s3:ListBucket`) on their development bucket prefix only.

`iam:CreateRole` on `"Resource": "*"` without a permission boundary is a privilege escalation vulnerability. The engineer could create a role with `AdministratorAccess` attached and assume it, giving themselves full admin access. The correct approach is to grant `iam:CreateRole` and `iam:AttachRolePolicy` only with a condition requiring a permission boundary: `"Condition": {"StringEquals": {"iam:PermissionsBoundary": "arn:aws:iam::<account>:policy/DevRoleBoundary"}}`. The boundary policy limits what any role the engineer creates can do — no IAM, no account management, only data-engineering-relevant services.

I'd also point the engineer to the IAM Access Advisor after they've been running for a few weeks — if they discover they needed more specific actions, we can add them. Starting locked down and expanding is much safer than starting broad and trying to restrict.

**Interviewer:** How do you audit which IAM permissions are actually being used by your data platform?

**Candidate:** Three mechanisms working together. First, IAM Access Advisor at the role level: for each IAM role, Access Advisor shows the last time each AWS service was accessed and which specific actions were used. Permissions that haven't been used in 90+ days are candidates for removal. I'd run `iam.generate_service_last_accessed_details` for each pipeline role quarterly and review the results.

Second, CloudTrail analysis: I'd set up an Athena table on top of the CloudTrail S3 bucket and run queries to find the distinct set of API actions each role ARN has actually called in the past 30 days. Comparing this set to the set of permitted actions in the IAM policy reveals the permission gap — actions permitted but never called.

Third, IAM Access Analyzer: this continuously analyzes resource policies (S3 bucket policies, KMS key policies, SQS policies) and alerts on resources that are accessible from outside the AWS account or organization. It's the automated version of "is anything public that shouldn't be?" Combined, these three give a complete picture of both what is being used and what is over-permitted.

**Interviewer:** Final question: explain how GCP's Workload Identity differs from AWS's IRSA, and what they have in common.

**Candidate:** Both solve the same problem — compute workloads in Kubernetes need cloud credentials without static keys. The core mechanism in both is OIDC federation: Kubernetes acts as an OIDC identity provider, issues tokens for service accounts, and the cloud IAM system exchanges those tokens for cloud-native credentials via the `AssumeRoleWithWebIdentity` (AWS) or token exchange (GCP) API.

The differences are in how credentials are attached. In AWS IRSA, the IAM role has a trust policy that directly references the EKS OIDC provider URL. The Kubernetes ServiceAccount is annotated with the role ARN. The AWS SDK environment variable `AWS_WEB_IDENTITY_TOKEN_FILE` is injected into pods automatically by EKS. In GCP Workload Identity, the binding is a separate IAM policy binding on the GCP Service Account that grants the Kubernetes service account the `roles/iam.workloadIdentityUser` role on the GSA. The annotation maps the KSA to the GSA. The GKE metadata server handles token exchange transparently — pods just call GCP APIs and the metadata server provides credentials.

The key architectural difference: in AWS, the trust lives on the IAM role (pull model — the role decides who can assume it). In GCP, the trust lives as a binding on the GSA (push model — the GSA is granted the ability to be impersonated by a specific KSA). Both effectively scope credentials to a specific Kubernetes service account, but the configuration lives in different places. Engineers who know IRSA will find Workload Identity familiar in concept but different in the configuration steps.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What is the IAM evaluation order for an explicit Deny vs Allow? | Explicit Deny always wins over any Allow, regardless of which policy it's in. The default is implicit Deny (if no Allow, access is denied). |
| 2 | What is an IAM trust policy? | A resource-based policy attached to an IAM role that specifies which principals (services, roles, accounts) are allowed to call `sts:AssumeRole` to assume the role. |
| 3 | What is IRSA? | IAM Roles for Service Accounts — the EKS mechanism that maps Kubernetes service accounts to IAM roles via OIDC federation, enabling pod-level AWS credential scoping without static keys. |
| 4 | What prefix does an IAM user access key start with? | `AKIA` (permanent). Temporary credentials (assumed roles) start with `ASIA`. Seeing `AKIA` in a compute environment is a warning sign. |
| 5 | Why must you include both `s3:ListBucket` on the bucket AND `s3:GetObject` on `bucket/*` for S3 read access? | `ListBucket` requires the bucket ARN (no trailing `/*`). `GetObject` requires the object ARN (with `/*`). Granting only `GetObject` causes AccessDenied on Spark's `ListObjectsV2` calls. |
| 6 | What is a permission boundary in IAM? | An IAM policy set as a maximum-permissions cap on an identity. An identity cannot exceed the boundary, even if its identity-based policy would otherwise grant more. Used to delegate IAM management safely. |
| 7 | What is the difference between GCP Workload Identity and AWS IRSA? | Same goal (pod-level cloud credentials via OIDC). Difference: AWS trust lives on the IAM role's trust policy (role decides who can assume it). GCP trust lives as an IAM binding on the GSA (role grants impersonation to KSA). |
| 8 | What is the credential provider chain in AWS SDKs? | The ordered list of sources the SDK checks for credentials: env vars → credentials file → config file → EC2 IMDS → ECS task metadata → IRSA/WebIdentity. First found source wins. |
| 9 | How does cross-account S3 access differ from same-account access in IAM? | Same-account: only IAM role policy needed. Cross-account: BOTH the IAM role policy (in the accessing account) AND the S3 bucket policy (in the bucket's account) must allow the access. |
| 10 | What is IMDSv2 and why was it introduced? | Instance Metadata Service v2 — requires a PUT request to get a session token, then a GET with the token. Prevents SSRF attacks that used IMDSv1 to steal instance credentials by making a direct GET to 169.254.169.254. |
| 11 | What is AWS CloudTrail used for in IAM context? | Records every API call with the caller's ARN, action, resource, timestamp, and error code. Used to audit permission usage, investigate AccessDenied events, and detect unusual access patterns. |
| 12 | What GCP roles are required for a service account to run BigQuery queries? | `roles/bigquery.dataEditor` (or dataViewer) for data access AND `roles/bigquery.jobUser` for running jobs. DataViewer alone is not sufficient to actually execute a query. |
| 13 | What is an IAM Service Control Policy (SCP)? | An organization-level policy that sets the maximum permissions available to all identities in an AWS organizational unit. SCPs do not grant permissions — they restrict them. A Deny in an SCP cannot be overridden by any IAM policy. |
| 14 | What is the AWS Policy Simulator used for? | Testing IAM policies without making real API calls. Input a role ARN, action, and resource ARN; receive `allowed`, `implicitDeny`, or `explicitDeny`. Essential for validating policies before deployment. |
| 15 | What is the IRSA trust policy condition that prevents other pods from assuming the role? | `"StringEquals": {"<oidc-url>:sub": "system:serviceaccount:<namespace>:<service-account-name>"}` — constrains the trust to a specific namespace and service account name. |
| 16 | What is GCP Shared VPC and how does IAM interact with it? | The host project owns the VPC; service projects use its subnets. IAM controls who in service projects can use which subnets via the `compute.networkUser` role on specific subnets. |
| 17 | How long can temporary credentials from `sts:AssumeRole` last? | 15 minutes to 12 hours (default 1 hour). Cannot exceed the role's `MaxSessionDuration` setting. Chained role assumptions (assuming a role that then assumes another role) are limited to 1 hour regardless. |
| 18 | What is IAM Access Analyzer? | An AWS service that analyzes IAM and resource policies to find resources accessible from outside the account/organization. Identifies S3 buckets, KMS keys, and IAM roles with overly broad trust policies. |
| 19 | What is the `aws:SecureTransport` IAM condition used for? | Requires HTTPS for S3 (and other services). `"Condition": {"Bool": {"aws:SecureTransport": "true"}}` causes requests over HTTP to be denied even if the action would otherwise be allowed. |
| 20 | Why should `iam:CreateRole` always be paired with a permission boundary condition? | Without a boundary, a user with `iam:CreateRole` + `iam:AttachRolePolicy` can create a role with AdministratorAccess and assume it — full privilege escalation. The boundary caps what the created roles can do. |

---

## 13. Further Reading

- **AWS IAM User Guide — "IAM JSON Policy Elements Reference":** The authoritative documentation for IAM policy syntax: every `Effect`, `Action`, `Resource`, `Condition` key, and operator. Required reading for writing precise IAM policies.
- **AWS Documentation — "IAM roles for service accounts" (EKS):** The step-by-step guide for setting up IRSA, including creating OIDC providers, trust policies, and testing from within a pod.
- **"IAM Access Analyzer findings for external access" — AWS Security Blog:** Explains how Access Analyzer detects over-permissive resource policies and how to use it in a CI/CD pipeline to prevent insecure resource configurations from being deployed.
- **"A Practical Guide to AWS Security for Data Engineers" — Multiple sources:** Several data engineering newsletters (The Pragmatic Engineer, Towards Data Science) have written practical treatments of IAM for data pipelines. Search for "IAM least privilege Spark" or "EKS IRSA BigQuery equivalent."
- **GCP Documentation — "Workload Identity":** The official guide to setting up Workload Identity for GKE, including troubleshooting steps for common failures (missing binding, wrong project ID in the member string).
- **AWS re:Invent: "Mastering IAM Policy Writing" (available on YouTube):** A 60-minute deep-dive on IAM policy evaluation logic, condition keys, and permission boundaries. Covers the advanced topics (SCPs, permission boundaries, resource-based policy evaluation) that trip up even experienced engineers.
- **"Hacking the Cloud" — hackingthe.cloud:** A documentation of attack techniques that exploit cloud IAM misconfigurations (privilege escalation via `iam:CreateRole`, SSRF to steal IMDS credentials, cross-account trust exploitation). Understanding attack techniques is the best way to understand why IAM controls matter.

---

## 14. Lab Exercises

**Exercise 1: Policy Analysis with the Code Toolkit**

Run `python iam_diagnostics.py` (demo mode, no AWS credentials required). Review the analysis output for the three sample policies: `OverlyBroadS3Policy`, `LeastPrivilegeSparkPolicy`, and `DangerousIAMPolicy`. Write a corrected version of `OverlyBroadS3Policy` that grants only read access to a specific S3 prefix using the exact actions needed for Spark (include `ListBucket`).

**Exercise 2: IAM Role Audit**

In an AWS account, create two IAM roles: one with overly broad permissions (`s3:*` on `*`) and one with least-privilege permissions (specific actions on specific ARNs). Run `python iam_diagnostics.py --roles OverbroadRole LeastPrivRole`. Compare the output. Use the IAM Policy Simulator to confirm the least-privilege role can perform the required operations.

**Exercise 3: IRSA Setup**

In a test EKS cluster (minikube + LocalStack or a real EKS cluster), follow the IRSA walkthrough in Section 4.3. Create a service account, IAM role, and trust policy. Verify with `kubectl run` that the pod gets the expected identity from `aws sts get-caller-identity`. Deliberately break one of the trust policy conditions and observe the failure mode.

**Exercise 4: Cross-Account IAM**

Set up a cross-account role assumption scenario using two IAM users (simulating two accounts) or two AWS accounts if available. Implement the pattern from Section 3.4. Test the assumption with `python vpc_diagnostics.py` (adapting to call the cross-account session). Deliberately omit the bucket policy step and observe the `AccessDenied` error in CloudTrail.

**Exercise 5: IAM Least Privilege Audit**

For an existing data pipeline IAM role in your environment, use CloudTrail and IAM Access Advisor to answer: (1) Which S3 buckets did this role access in the past 30 days? (2) Which IAM actions from its permissions policy were never used in 90 days? (3) Write a tighter permissions policy based on actual usage. Use `aws iam simulate-principal-policy` to verify the tighter policy still allows all required operations.

---

## 15. Key Takeaways

IAM is the identity layer of cloud security. Understanding IAM is not optional for data engineers — every pipeline, every data access, every cross-account operation depends on correct IAM configuration. The three foundational principles: no static credentials in compute workloads (use instance profiles and IRSA), least privilege (specific actions and resources, not wildcards), and trust policy correctness (the trust policy determines who can assume a role; the permissions policy determines what they can do).

IRSA and Workload Identity are the correct mechanisms for cloud-native Kubernetes workloads. They scope credentials to a specific service account in a specific namespace, provide automatic rotation, and produce auditable CloudTrail records. The IRSA trust policy condition constraint (`system:serviceaccount:<ns>:<sa>`) is not optional — without it, any pod can assume the role.

Cross-account IAM has a subtle but critical difference from same-account IAM: for S3 cross-account access, both the IAM role policy AND the S3 bucket policy must allow the access. Understanding this prevents a specific class of production failures where the role policy looks correct but the bucket policy is missing.

---

## 16. Connections to Other Modules

- **M32 — VPC Architecture:** VPC endpoints (M32) and IAM work together for S3 access: the VPC endpoint routes traffic privately, and the IAM policy (with `aws:sourceVpce` condition) can restrict S3 access to only requests coming from the VPC endpoint — preventing access from outside the VPC even with valid credentials.
- **M34 — Security Groups and Network Policies:** Security groups control network-level access; IAM controls identity-level access. Both are needed: security groups prevent network connections from reaching a service; IAM prevents unauthorized API calls from authenticated identities.
- **M35 — Private Endpoints and VPC Service Controls:** GCP VPC Service Controls restrict which service accounts can access which Google APIs from which VPC networks. This module (IAM) and M35 (VPC Service Controls) are complementary controls.
- **M29 — TLS and mTLS:** Kafka IAM-based authentication (on MSK) and TLS-based authentication (via client certificates) are alternative approaches to the same problem. Understanding IAM helps evaluate when IAM authentication vs mTLS is more appropriate.
- **CPL-PRD-101 — Production Data Engineering:** IAM auditing (CloudTrail, Access Analyzer) is a component of the observability stack covered in production data engineering.

---

## 17. Anti-Patterns

**Anti-pattern: Using `"Resource": "*"` for S3 actions.** Even if the actions are specific (`s3:GetObject`), allowing them on `"*"` means the Spark job can read from any S3 bucket in the account, including sensitive HR data, financial records, or production data that should be isolated. Always specify the exact bucket and prefix ARN.

**Anti-pattern: Creating one shared IAM role for all data pipelines.** A single "DataPipelineRole" shared by Airflow, Spark, Kafka Connect, dbt, and all other tools creates an overly broad, impossible-to-audit role. If one pipeline is compromised, all services' data is at risk. Create separate roles per service (AirflowRole, SparkRole, KafkaConnectRole) with the minimum permissions each service needs.

**Anti-pattern: Storing GCP service account key files in the repository.** Service account JSON key files (`service-account-key.json`) committed to version control are permanent credential leaks. Once the key is in git history, it must be treated as compromised even after deletion. Use Workload Identity for GKE workloads and Application Default Credentials for local development.

**Anti-pattern: Not setting `MaxSessionDuration` appropriately on IAM roles.** The default `MaxSessionDuration` for an IAM role is 1 hour. For long-running Spark jobs (4–8 hours), the temporary credentials expire mid-job, causing failures. Set `MaxSessionDuration` to match the maximum expected job duration. For IRSA, the projected token is renewed automatically by Kubernetes but the role session must be long enough.

**Anti-pattern: Trusting `AWS: "*"` in a cross-account role.** A trust policy with `"Principal": {"AWS": "*"}` allows any AWS account (including malicious ones) to assume the role, provided they can satisfy any conditions. If a role also has broad permissions, this is a catastrophic misconfiguration. Always specify the exact account ARN or role ARN in cross-account trust policies.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `aws sts get-caller-identity` | Show current IAM identity | Verify role vs user vs root |
| `aws iam simulate-principal-policy` | Test permission without real call | `--action-names s3:GetObject --resource-arns arn:...` |
| `aws iam get-role` | Inspect role trust + metadata | `--query 'Role.AssumeRolePolicyDocument'` |
| `aws iam list-attached-role-policies` | List policies on a role | Quick permission inventory |
| `aws iam generate-service-last-accessed-details` | IAM Access Advisor | Find unused permissions |
| `aws cloudtrail lookup-events` | Search API call history | `--lookup-attributes AttributeKey=Username,AttributeValue=<role>` |
| `aws iam list-open-id-connect-providers` | Check OIDC providers | Required for IRSA |
| `eksctl utils associate-iam-oidc-provider` | Register EKS OIDC with IAM | Prerequisite for IRSA |
| `gcloud iam service-accounts list` | List GCP service accounts | With `--project=<project-id>` |
| `gcloud iam service-accounts get-iam-policy` | View GSA IAM bindings | Check Workload Identity binding |
| `gcloud projects get-iam-policy` | View project-level IAM | Find all bindings |
| `gcloud auth print-access-token` | Test GCP credentials | Inside GKE pods to verify Workload Identity |

---

## 19. Glossary

**Access Key:** A long-lived IAM credential pair (access key ID + secret access key) for IAM users. Should NOT be used in compute workloads. Key IDs starting with `AKIA` are permanent; `ASIA` are temporary (assumed role).

**CloudTrail:** AWS service that logs every API call with caller identity, action, resource, and timestamp. The audit log for all IAM actions.

**Credential Provider Chain:** The ordered list of credential sources the AWS SDK checks when making API calls. Instance profiles and IRSA are at the end of the chain; environment variables are first.

**Cross-Account Role:** An IAM role in account B whose trust policy allows a principal in account A to assume it. Requires `sts:AssumeRole` permission in account A and an explicit bucket/resource policy for S3.

**IAM (Identity and Access Management):** The AWS/GCP service that manages authentication (who you are) and authorization (what you can do) for cloud resources.

**IAM Access Analyzer:** AWS service that identifies resources accessible from outside the account/organization via analysis of resource-based policies.

**IAM Access Advisor:** AWS feature showing when each IAM service permission was last used. Used to identify and remove unused permissions.

**Identity-Based Policy:** IAM policy attached to a user, group, or role. Specifies what that identity is allowed (or denied) to do.

**IRSA (IAM Roles for Service Accounts):** EKS mechanism mapping Kubernetes service accounts to IAM roles via OIDC. Provides pod-level, automatically-rotating AWS credentials.

**Least Privilege:** Security principle: grant only the exact permissions required. No wildcards where specific actions/resources suffice.

**OIDC (OpenID Connect):** An identity layer on top of OAuth 2.0. Used by EKS (IRSA) and GKE (Workload Identity) to federate Kubernetes service account tokens with cloud IAM.

**Permission Boundary:** An IAM feature that caps the maximum permissions an identity can have, regardless of attached policies. Used to safely delegate IAM role creation.

**Principal:** An IAM entity that can make requests: IAM user, IAM role, AWS service, or federated identity.

**Resource-Based Policy:** IAM policy attached to a resource (S3 bucket, SQS queue). Specifies who can access the resource. Can grant cross-account access without role assumption.

**Service Account (GCP):** A GCP identity for non-human principals (applications, VMs, containers). The GCP equivalent of an AWS IAM role for compute workloads.

**SCP (Service Control Policy):** Organization-level IAM policy that restricts maximum permissions for all identities in an AWS organizational unit. Explicit Deny in an SCP overrides all identity-based Allow policies.

**Trust Policy:** A resource-based policy attached to an IAM role that specifies which principals can assume the role (`sts:AssumeRole` or `sts:AssumeRoleWithWebIdentity`).

**Workload Identity (GCP):** GKE mechanism mapping Kubernetes service accounts to GCP service accounts via OIDC. The GCP equivalent of AWS IRSA.

---

## 20. Self-Assessment

1. A Spark job on EC2 fails with `Unable to load credentials from any provider`. What are the four most likely causes and how do you check each?
2. Write the IAM trust policy for an IAM role that can only be assumed by IRSA from Kubernetes pods using the service account `pipeline-sa` in namespace `data-eng` on a specific EKS cluster OIDC provider.
3. Explain why `"Principal": {"AWS": "*"}` in a cross-account trust policy is dangerous, even if the IAM role has a Deny statement for most actions.
4. A data engineer has `iam:CreateRole` and `iam:AttachRolePolicy` permissions. Walk through the steps they would take to escalate their privileges to admin. What IAM control prevents this?
5. What is the difference between `roles/bigquery.dataViewer` and `roles/bigquery.jobUser` in GCP? Can you run a BigQuery query with only `dataViewer`?
6. Why does cross-account S3 access require both an IAM role policy AND an S3 bucket policy, while same-account access only requires the IAM role policy?
7. An IRSA-enabled pod is getting credentials but they're for the wrong role. The annotation on the ServiceAccount has the correct role ARN. What else might be wrong?
8. Explain IAM Access Advisor and how you use it to implement least privilege in practice.
9. Write the IAM policy that grants a Spark job: read access to `s3://data-lake-prod/input/`, write access to `s3://data-lake-prod/output/`, and Glue read access to the `analytics` database. Include all required resource ARNs.
10. Describe the `aws:SecureTransport` condition key. When would you add it to an S3 IAM policy, and what does it prevent?

---

## 21. Module Summary

IAM is the mechanism by which cloud infrastructure enforces identity-based access control. In data engineering, every API call — every S3 read, every BigQuery query, every Secrets Manager fetch — goes through IAM evaluation. Understanding IAM is prerequisite to both building correct pipelines and debugging the inevitable access failures.

The central shift in modern cloud IAM is from static credentials (IAM user access keys) to dynamic role assumption (EC2 instance profiles, IRSA for EKS, Workload Identity for GKE). Static credentials are permanent, do not rotate, and create blast radius when leaked. Role-based credentials are temporary, automatically rotated, and scoped to specific compute resources. Every data pipeline in production should use role-based authentication — never access keys embedded in code or environment variables.

IAM policies have two distinct components that serve different functions: trust policies define who can become a role; permissions policies define what the role can do. For compute workloads, the trust policy is `ec2.amazonaws.com` (EC2), the OIDC provider (IRSA/Workload Identity), or another IAM role (cross-account). Confusing these is a common source of `AccessDenied` errors.

The principle of least privilege is not an abstract goal — it is operationally achievable through action-level specificity (exact S3 actions, not `s3:*`), resource-level specificity (exact ARNs, not `*`), and condition constraints (enforce HTTPS, restrict to specific regions). IAM Access Advisor provides the data needed to continuously tighten permissions based on actual usage.

Cross-account IAM introduces a rule that trips up even experienced engineers: for cross-account S3 access, both the IAM role in the accessing account AND the S3 bucket policy in the bucket's account must allow the access. Same-account access only requires the IAM role. This asymmetry is the most common cause of mysterious `AccessDenied` errors in multi-account data platform architectures.

The next module — M34: Security Groups and Network Policies — addresses the network-level complement to IAM: where IAM controls which identities can make API calls, security groups control which network endpoints can receive TCP connections.
