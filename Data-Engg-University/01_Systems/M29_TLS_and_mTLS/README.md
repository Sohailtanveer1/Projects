# M29: TLS and mTLS

**Course:** SYS-NET-101 — Networking for Data Engineers  
**Module:** 03 of 05  
**Global Module ID:** M29  
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

A Kafka cluster without TLS transmits every message in plaintext over the network. Any machine that can observe network traffic between producers, brokers, and consumers can read your data, replay your messages, or impersonate a broker. In a shared network environment — a multi-tenant Kubernetes cluster, a data center where multiple teams share the same VLAN, a cloud VPC where lateral movement is possible after an initial compromise — unauthenticated, unencrypted Kafka is a significant security boundary violation.

TLS solves three distinct security problems simultaneously: **confidentiality** (encryption prevents eavesdroppers from reading traffic), **integrity** (MACs prevent tampering in transit), and **server authentication** (the client verifies it is talking to the real Kafka broker, not an impersonator). Mutual TLS (mTLS) extends server authentication to **client authentication** — the broker verifies the identity of each producer and consumer using a client certificate, providing cryptographic proof of identity that is stronger than username/password.

Why data engineers specifically need to understand TLS — not just that it exists, but how it works:

**Reason 1 — Certificate expiry is a Tier-1 incident.** When a TLS certificate expires, every connection that validates it immediately fails. There is no graceful degradation: a Kafka broker whose certificate expired at 3:17 AM will refuse all client connections at 3:17 AM. Certificate expiry has taken down production Kafka clusters at companies where certificate management was treated as a sysadmin detail rather than an engineering concern. Understanding certificate chains and expiry dates is part of operating any data infrastructure in production.

**Reason 2 — mTLS misconfiguration is the most common Kafka security incident.** The most frequent Kafka security incidents in production are not sophisticated attacks — they are configuration errors: wrong truststore path, certificate with wrong Subject Alternative Name, client certificate signed by a CA that the broker's truststore does not contain. Diagnosing these requires knowing what the TLS handshake is trying to validate.

**Reason 3 — TLS adds measurable overhead.** A TLS 1.3 handshake adds 1–2 RTTs on top of the TCP handshake. For a Kafka producer connecting to a broker in the same AZ, that adds 2–4 ms of connection setup time. For a system that creates many short-lived connections (a data pipeline that creates a new producer per batch), this overhead accumulates. Understanding TLS overhead informs connection pooling decisions and whether to enable session resumption.

---

## 2. Mental Model

### TLS as a Secure Channel Negotiation

Think of TLS as a protocol that runs on top of TCP, whose sole purpose is to negotiate a shared secret key before application data flows. Once both sides agree on a key and have verified each other's identity (or at least the server's identity), all subsequent data is encrypted with that key. The key is never transmitted — it is derived independently by both sides from exchanged parameters, so an eavesdropper who sees the handshake cannot derive the key.

The challenge TLS solves is: how do two strangers, communicating over a public network, agree on a secret key without a pre-shared secret? The answer is asymmetric cryptography. Each party has a key pair: a **public key** (can be shared with anyone) and a **private key** (never leaves the owner's machine). Messages encrypted with the public key can only be decrypted with the private key. This asymmetric property bootstraps the symmetric key exchange.

### The Certificate as a Signed Identity Claim

A TLS certificate is a document that says: "The entity at this hostname has this public key." The critical property is that the document is *signed* by a Certificate Authority (CA) — a trusted third party that has verified the claim. When a Kafka client connects to `kafka-broker-1.prod.internal` and receives a certificate, it is asking: "Is this certificate signed by a CA that I trust, and does the hostname in the certificate match the hostname I connected to?" If both are true, the client has cryptographic assurance that it is talking to the genuine broker — not an impersonator.

In mTLS, the server asks the same question back: "Is this client's certificate signed by a CA that I trust?" The client must present a certificate that the server's truststore can validate. If it cannot, the TLS handshake fails with a certificate verification error.

### Keystores and Truststores: The Two-Container Model

The Java/JVM TLS model (which governs Kafka, because Kafka is written in Java) uses two containers:

**Keystore:** Contains the entity's own certificate and private key. Used when presenting your identity to the other party. A broker's keystore holds the broker certificate. A client's keystore holds the client certificate (only needed for mTLS).

**Truststore:** Contains the certificates of CAs that the entity trusts. Used when validating the other party's certificate. A client's truststore holds the CA certificate that signed the broker certificates. A broker's truststore (for mTLS) holds the CA certificate that signed client certificates.

If the signer of the other party's certificate is not in your truststore, the handshake fails with `PKIX path building failed` or `unable to find valid certification path`.

---

## 3. Core Concepts

### 3.1 Asymmetric Cryptography Fundamentals

TLS uses asymmetric cryptography for key exchange and certificate signing, and symmetric cryptography for the bulk data encryption.

**Asymmetric (public-key) cryptography:**
- Every party generates a key pair: public key + private key
- The public key is distributed freely; the private key is never shared
- Data encrypted with the public key can only be decrypted with the private key
- Data signed with the private key can be verified by anyone with the public key
- Common algorithms: RSA (2048 or 4096-bit), ECDSA (P-256, P-384), Ed25519
- Asymmetric operations are computationally expensive — 100–1000× slower than symmetric

**Symmetric (shared-key) cryptography:**
- Both parties share the same key for encryption and decryption
- Extremely fast — suitable for bulk data encryption
- The challenge is key distribution: how do you share the key securely?
- Common algorithms in TLS 1.3: AES-128-GCM, AES-256-GCM, ChaCha20-Poly1305

TLS's design insight: use asymmetric cryptography *only* for the handshake (key exchange and authentication), then switch to symmetric cryptography for all application data. This gives security without performance cost.

### 3.2 Certificate Anatomy

An X.509 certificate is a structured document with the following important fields:

```
Certificate:
  Version: 3 (v3)
  Serial Number: 01:ab:cd:...
  Signature Algorithm: sha256WithRSAEncryption
  Issuer: CN=Internal-CA, O=ExampleCorp, C=US   ← who signed this certificate
  Validity:
    Not Before: Jan  1 00:00:00 2024 GMT
    Not After:  Jan  1 00:00:00 2025 GMT          ← EXPIRY DATE
  Subject: CN=kafka-broker-1.prod.internal, O=ExampleCorp
  Subject Public Key Info:
    Public Key Algorithm: id-ecPublicKey
    Public-Key: (256 bit)                         ← broker's public key
  X509v3 extensions:
    X509v3 Subject Alternative Name:              ← CRITICAL: hostnames validated
      DNS:kafka-broker-1.prod.internal
      DNS:kafka.prod.internal
      IP:10.0.1.47
    X509v3 Key Usage: critical
      Digital Signature, Key Encipherment
    X509v3 Extended Key Usage:
      TLS Web Server Authentication
    X509v3 Basic Constraints: critical
      CA:FALSE                                    ← not a CA; cannot sign other certs
```

**Subject Alternative Names (SANs):** The most important field for connection validation. When a client connects to a hostname, it validates that the hostname (or IP) appears in the SAN list. The old `CN` (Common Name) field is deprecated for hostname validation — modern clients use SANs only. A certificate without the correct SAN will fail validation even if the CN matches.

**Expiry date:** The `Not After` date. When a certificate expires, all connections that validate it immediately fail. There is no warning to connecting clients — the handshake simply fails with `certificate expired`.

**Key Usage and Extended Key Usage:** Controls what the certificate can be used for. A server certificate needs `TLS Web Server Authentication`. A client certificate needs `TLS Web Client Authentication`. Using a certificate for the wrong purpose causes validation failure.

**Basic Constraints CA:FALSE:** Marks the certificate as an end-entity certificate, not a CA certificate. A CA certificate has `CA:TRUE` and can sign other certificates. End-entity certificates cannot issue subordinate certificates.

### 3.3 Certificate Chains

In production, certificates are rarely signed directly by a well-known root CA (like DigiCert or Let's Encrypt). Instead, they form a chain:

```
Root CA (self-signed, trusted by OS/JVM)
    │ signs
    ▼
Intermediate CA (issued by Root CA)
    │ signs
    ▼
End-entity certificate (issued by Intermediate CA)
  (the broker's or client's certificate)
```

When a client validates a server certificate, it builds the chain upward: "This certificate was signed by Intermediate CA. Is Intermediate CA's certificate signed by something in my truststore?" The chain must be complete and each link must be valid.

**Chain completion:** The server is responsible for sending its full certificate chain (end-entity + all intermediate CAs). If it sends only the end-entity certificate and the client doesn't have the intermediate CA certificate, validation fails — even if the root CA is trusted. Java's `keytool` and OpenSSL both have ways to bundle the chain.

**For internal PKI (private CA):** Data engineering infrastructure typically uses an internal CA rather than a public CA. The internal CA root certificate must be installed in the JVM's truststore on every machine that validates internal certificates. This is the primary operational task when setting up TLS for Kafka.

### 3.4 TLS 1.2 Handshake

TLS 1.2 requires 2 round trips before application data can flow (on top of the TCP handshake's 1 RTT):

```
Client                              Server
  │                                    │
  │── ClientHello ───────────────────► │  t=0 (after TCP handshake)
  │    - TLS version: 1.2              │  Offers: cipher suites, extensions,
  │    - Random nonce (client_random)  │  supported curves, session ID
  │    - Supported cipher suites       │
  │                                    │
  │◄─ ServerHello ──────────────────── │  t=RTT/2
  │◄─ Certificate ──────────────────── │  Server's certificate chain
  │◄─ ServerKeyExchange ──────────────-│  (for ECDHE: server's ephemeral key)
  │◄─ ServerHelloDone ────────────────-│
  │                                    │
  │  [Client validates certificate]    │
  │                                    │
  │── ClientKeyExchange ─────────────► │  t=RTT   Client sends its part of key exchange
  │── ChangeCipherSpec ───────────────►│  "Switching to encrypted comms"
  │── Finished ────────────────────── ►│  MAC of all handshake messages
  │                                    │
  │◄─ ChangeCipherSpec ─────────────── │  t=2×RTT
  │◄─ Finished ─────────────────────── │
  │                                    │
  │◄══ Encrypted application data ════►│  t=2×RTT + server processing
```

**Total overhead:** 2 RTTs after TCP (which itself cost 1 RTT), so a TLS 1.2 connection to a new server costs 3 RTTs before application data flows.

**Session resumption (TLS 1.2):** After the initial handshake, the server can issue a session ticket. On reconnection, the client sends the session ticket and both sides can resume the session with 1 RTT instead of 2. This is important for Kafka producers that frequently reconnect.

### 3.5 TLS 1.3 Handshake

TLS 1.3 (RFC 8446, 2018) reduces the handshake to 1 RTT by merging the key exchange into the ClientHello:

```
Client                              Server
  │                                    │
  │── ClientHello ───────────────────► │  t=0
  │    - TLS version: 1.3              │  Includes key share for supported groups
  │    - Supported cipher suites       │  (client guesses which group server prefers)
  │    - Key shares (ECDH public keys) │
  │                                    │
  │◄─ ServerHello ──────────────────── │  t=RTT/2
  │◄─ {Certificate} ──────────────────-│  (encrypted, because key is already derived)
  │◄─ {CertificateVerify} ────────────-│
  │◄─ {Finished} ─────────────────────-│
  │                                    │
  │── {Finished} ────────────────────► │  t=RTT
  │◄══ Encrypted application data ════►│  t=RTT + server processing
```

**0-RTT (early data):** TLS 1.3 supports 0-RTT resumption, where the client sends application data *with* the ClientHello on reconnection. This eliminates handshake latency entirely for reconnections. However, 0-RTT data is susceptible to replay attacks and should be used carefully (Kafka clients do not currently use 0-RTT).

**TLS 1.3 cipher suites:** Reduced to three: TLS_AES_128_GCM_SHA256, TLS_AES_256_GCM_SHA384, TLS_CHACHA20_POLY1305_SHA256. All use AEAD (Authenticated Encryption with Associated Data) and perfect forward secrecy.

**Perfect forward secrecy (PFS):** TLS 1.3 requires ephemeral key exchange (ECDHE). Even if the server's private key is compromised in the future, past sessions cannot be decrypted because each session used a different ephemeral key. TLS 1.2 with RSA key exchange (non-ephemeral) did not have PFS.

### 3.6 Mutual TLS (mTLS)

Standard TLS authenticates only the server. mTLS adds client authentication: the server requests a certificate from the client, and the client must present a valid certificate signed by a CA that the server trusts.

**mTLS handshake additions (TLS 1.3):**

```
Client                              Server
  │                                    │
  │── ClientHello + key share ────────►│
  │◄─ ServerHello + key share ─────────│
  │◄─ {CertificateRequest} ────────────│  ← NEW: server requests client cert
  │◄─ {Certificate (server)} ──────────│
  │◄─ {CertificateVerify} ─────────────│
  │◄─ {Finished} ──────────────────────│
  │                                    │
  │── {Certificate (client)} ─────────►│  ← NEW: client presents its certificate
  │── {CertificateVerify} ────────────►│  ← NEW: client proves possession of private key
  │── {Finished} ──────────────────────►│
  │◄══ Encrypted application data ════►│
```

**What mTLS provides over username/password:**
- Cryptographic identity: the client proves it possesses the private key corresponding to the certificate
- No password to steal: there is no credential that can be phished or leaked in a config file
- Fine-grained identity: each client (each Kafka producer or consumer) can have its own certificate with a unique Subject (CN or O), enabling per-client ACLs
- Revocation: a compromised client can have its certificate revoked via CRL or OCSP

**Kafka mTLS configuration:**

On the broker (`server.properties`):
```properties
listeners=SSL://0.0.0.0:9093
advertised.listeners=SSL://kafka-broker-1.prod.internal:9093
ssl.keystore.location=/etc/kafka/ssl/kafka.broker.keystore.jks
ssl.keystore.password=keystorepassword
ssl.key.password=keypassword
ssl.truststore.location=/etc/kafka/ssl/kafka.broker.truststore.jks
ssl.truststore.password=truststorepassword
ssl.client.auth=required    # 'required' = mTLS; 'requested' = optional; 'none' = no client cert
ssl.endpoint.identification.algorithm=HTTPS  # validates broker hostname in cert SAN
```

On the client (`consumer.properties` or producer config):
```properties
security.protocol=SSL
ssl.keystore.location=/etc/kafka/ssl/kafka.client.keystore.jks
ssl.keystore.password=keystorepassword
ssl.key.password=keypassword
ssl.truststore.location=/etc/kafka/ssl/kafka.client.truststore.jks
ssl.truststore.password=truststorepassword
ssl.endpoint.identification.algorithm=HTTPS  # validates broker hostname — must match SAN
```

### 3.7 Certificate Lifecycle: Generation, Distribution, Rotation

**Step 1: Generate a CA (internal PKI)**
```bash
# Generate CA private key
openssl genrsa -out ca.key 4096

# Self-sign the CA certificate (10 years; CAs have long validity)
openssl req -new -x509 -days 3650 -key ca.key \
  -subj "/CN=Internal-Data-CA/O=ExampleCorp/C=US" \
  -out ca.crt
```

**Step 2: Generate a broker certificate**
```bash
# Generate broker private key and CSR (Certificate Signing Request)
openssl genrsa -out broker.key 2048
openssl req -new -key broker.key \
  -subj "/CN=kafka-broker-1.prod.internal/O=ExampleCorp" \
  -out broker.csr

# Create SAN extension file (CRITICAL: hostname must match exactly)
cat > broker_ext.cnf << EOF
[req]
req_extensions = v3_req
[v3_req]
subjectAltName = DNS:kafka-broker-1.prod.internal,DNS:kafka.prod.internal,IP:10.0.1.47
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
EOF

# Sign the CSR with the CA (1 year validity; rotate annually or more frequently)
openssl x509 -req -days 365 -in broker.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -extfile broker_ext.cnf -extensions v3_req \
  -out broker.crt
```

**Step 3: Create JKS keystores for Kafka**
```bash
# Combine broker cert + key into a PKCS12 bundle
openssl pkcs12 -export -in broker.crt -inkey broker.key \
  -chain -CAfile ca.crt \
  -name "kafka-broker-1" -passout pass:keystorepassword \
  -out broker.p12

# Convert PKCS12 to JKS keystore
keytool -importkeystore \
  -srckeystore broker.p12 -srcstoretype pkcs12 \
  -srcstorepass keystorepassword \
  -destkeystore kafka.broker.keystore.jks \
  -deststorepass keystorepassword \
  -deststoretype JKS

# Create truststore containing the CA certificate
keytool -import -trustcacerts -alias internal-ca \
  -file ca.crt -keystore kafka.broker.truststore.jks \
  -storepass truststorepassword -noprompt
```

**Certificate rotation:** Rotating certificates without downtime requires careful sequencing:
1. Generate new certificate (while old is still valid)
2. Add new certificate to the keystore alongside the old one (dual-cert keystore)
3. Deploy new keystore to brokers (they now offer both certs; clients still validate old cert)
4. Wait for all clients to reconnect and pick up new cert (or restart clients)
5. Remove old certificate from keystore
6. Optionally revoke old certificate in CRL

### 3.8 OCSP and Certificate Revocation

Certificate revocation handles the case where a certificate must be invalidated before its expiry — for example, if the private key is compromised.

**CRL (Certificate Revocation List):** The CA publishes a list of revoked certificate serial numbers. Clients download the CRL (URL embedded in the certificate's `CRL Distribution Points` extension) and check if the certificate's serial number is on the list. CRLs can be large and are updated infrequently (typically daily), creating a window where a revoked certificate is still trusted.

**OCSP (Online Certificate Status Protocol):** Instead of downloading a full CRL, the client queries an OCSP responder in real time with the certificate's serial number. The responder returns: `good`, `revoked`, or `unknown`. Faster than CRL and always current. But it adds an OCSP query to every TLS handshake — 50–200 ms round trip to the OCSP server.

**OCSP Stapling:** The server queries the OCSP responder itself, caches the response (OCSP staple), and sends it during the TLS handshake. The client gets the revocation status without an extra round trip. This is the recommended approach for production Kafka TLS.

In Kafka, Java's OCSP stapling is controlled by: `-Dcom.sun.security.enableAIAcaIssuers=true -Dcom.sun.net.ssl.checkRevocation=true`.

---

## 4. Hands-On Walkthrough

### 4.1 Inspecting a Certificate

```bash
# Inspect a certificate file
openssl x509 -in broker.crt -text -noout

# Key sections to check:
# - Validity dates (Not Before / Not After)
# - Subject Alternative Names (must include your hostname)
# - Issuer (which CA signed it)
# - Key Usage / Extended Key Usage

# Check certificate expiry only
openssl x509 -in broker.crt -noout -enddate
# notAfter=Jan  1 00:00:00 2025 GMT

# Days until expiry
python3 -c "
import ssl, datetime
cert = ssl.PEM_cert_to_DER_cert(open('broker.crt').read())
# Or use openssl:
"
openssl x509 -in broker.crt -noout -enddate | \
  awk -F= '{print $2}' | \
  xargs -I{} date -d "{}" +%s | \
  xargs -I{} python3 -c "
import time; exp={}; now=int(time.time()); print(f'Expires in {(exp-now)//86400} days')
"
```

### 4.2 Testing a TLS Connection

```bash
# Test TLS connection and print the server certificate
openssl s_client -connect kafka-broker-1.prod.internal:9093 \
  -servername kafka-broker-1.prod.internal

# With client certificate (mTLS)
openssl s_client -connect kafka-broker-1.prod.internal:9093 \
  -servername kafka-broker-1.prod.internal \
  -cert client.crt -key client.key \
  -CAfile ca.crt

# Check specific TLS version support
openssl s_client -connect kafka-broker-1.prod.internal:9093 \
  -tls1_2    # test TLS 1.2
openssl s_client -connect kafka-broker-1.prod.internal:9093 \
  -tls1_3    # test TLS 1.3

# Show the full certificate chain
openssl s_client -connect kafka-broker-1.prod.internal:9093 \
  -showcerts -CAfile ca.crt

# Quick check: verify cert matches key (modulus must match)
openssl x509 -noout -modulus -in broker.crt | openssl md5
openssl rsa  -noout -modulus -in broker.key | openssl md5
# These two hashes must be identical
```

### 4.3 Validating Certificate Chain

```bash
# Verify that a certificate is signed by a CA
openssl verify -CAfile ca.crt broker.crt
# Output: broker.crt: OK

# Verify with an intermediate CA
openssl verify -CAfile root-ca.crt -untrusted intermediate-ca.crt broker.crt

# Check the full chain that a TLS handshake would present
openssl s_client -connect kafka-broker-1.prod.internal:9093 \
  -showcerts 2>/dev/null | \
  awk '/BEGIN CERT/,/END CERT/' | \
  csplit -z -f chain-cert - '/-----BEGIN CERTIFICATE-----/' '{*}' 2>/dev/null
# Now examine each cert in chain-cert* files
```

### 4.4 Checking a JKS Keystore

```bash
# List all entries in a keystore
keytool -list -keystore kafka.broker.keystore.jks \
  -storepass keystorepassword

# Detailed view (shows cert content)
keytool -list -v -keystore kafka.broker.keystore.jks \
  -storepass keystorepassword

# Check the truststore (which CAs are trusted)
keytool -list -v -keystore kafka.broker.truststore.jks \
  -storepass truststorepassword

# Verify certificate expiry from keystore
keytool -list -v -keystore kafka.broker.keystore.jks \
  -storepass keystorepassword | grep -A2 "Valid from"
```

### 4.5 Measuring TLS Handshake Overhead

```bash
# Measure total connection time (TCP + TLS)
time openssl s_client -connect kafka-broker-1.prod.internal:9093 \
  -servername kafka-broker-1.prod.internal </dev/null 2>&1

# Compare TLS 1.2 vs 1.3 handshake time
time openssl s_client -tls1_2 -connect kafka-broker-1.prod.internal:9093 </dev/null 2>&1
time openssl s_client -tls1_3 -connect kafka-broker-1.prod.internal:9093 </dev/null 2>&1

# Using curl for HTTP over TLS with timing breakdown
curl -w "@curl-format.txt" -o /dev/null -s https://kafka-broker-1.prod.internal:8083/connectors
# curl-format.txt:
# time_namelookup:  %{time_namelookup}\n
# time_connect:     %{time_connect}\n
# time_appconnect:  %{time_appconnect}\n  ← this is TCP+TLS time
# time_total:       %{time_total}\n
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
tls_diagnostics.py

TLS/mTLS diagnostic toolkit for data engineers.
Validates certificates, checks expiry, tests connections,
and assesses TLS configuration for Kafka clusters.

Dependencies: cryptography (pip install cryptography)
              stdlib: ssl, socket, subprocess
"""

import os
import ssl
import socket
import subprocess
import datetime
import json
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path


# ─── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class CertInfo:
    """Parsed information from an X.509 certificate."""
    subject_cn: str = ""
    subject_org: str = ""
    issuer_cn: str = ""
    issuer_org: str = ""
    not_before: Optional[datetime.datetime] = None
    not_after: Optional[datetime.datetime] = None
    days_until_expiry: int = -1
    san_dns: list[str] = field(default_factory=list)
    san_ips: list[str] = field(default_factory=list)
    key_algorithm: str = ""
    key_bits: int = 0
    is_ca: bool = False
    serial_number: str = ""
    thumbprint_sha256: str = ""
    # Validity flags
    is_expired: bool = False
    is_expiring_soon: bool = False   # within 30 days
    cert_key_usage: list[str] = field(default_factory=list)
    extended_key_usage: list[str] = field(default_factory=list)


@dataclass
class TLSConnectionResult:
    """Result of a TLS connection test."""
    hostname: str
    port: int
    success: bool = False
    tls_version: str = ""
    cipher_suite: str = ""
    server_cert: Optional[CertInfo] = None
    cert_chain_depth: int = 0
    handshake_ms: float = 0.0
    error: Optional[str] = None
    hostname_verified: bool = False


@dataclass
class KeystoreInfo:
    """Contents of a JKS or PKCS12 keystore."""
    path: str
    store_type: str = ""
    entries: list[CertInfo] = field(default_factory=list)
    error: Optional[str] = None


# ─── Certificate Parsing ────────────────────────────────────────────────────────

def parse_cert_from_file(cert_path: str) -> CertInfo:
    """
    Parse an X.509 certificate from a PEM file.
    Requires the `cryptography` library.
    Falls back to openssl subprocess if cryptography is not installed.
    """
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.backends import default_backend
        import hashlib

        with open(cert_path, 'rb') as f:
            cert_data = f.read()

        # Try PEM first, then DER
        try:
            cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        except Exception:
            cert = x509.load_der_x509_certificate(cert_data, default_backend())

        info = CertInfo()

        # Subject
        try:
            info.subject_cn = cert.subject.get_attributes_for_oid(
                x509.NameOID.COMMON_NAME)[0].value
        except (IndexError, Exception):
            info.subject_cn = ""
        try:
            info.subject_org = cert.subject.get_attributes_for_oid(
                x509.NameOID.ORGANIZATION_NAME)[0].value
        except (IndexError, Exception):
            info.subject_org = ""

        # Issuer
        try:
            info.issuer_cn = cert.issuer.get_attributes_for_oid(
                x509.NameOID.COMMON_NAME)[0].value
        except (IndexError, Exception):
            info.issuer_cn = ""

        # Validity
        info.not_before = cert.not_valid_before_utc if hasattr(
            cert, 'not_valid_before_utc') else cert.not_valid_before.replace(
            tzinfo=datetime.timezone.utc)
        info.not_after = cert.not_valid_after_utc if hasattr(
            cert, 'not_valid_after_utc') else cert.not_valid_after.replace(
            tzinfo=datetime.timezone.utc)

        now = datetime.datetime.now(datetime.timezone.utc)
        delta = info.not_after - now
        info.days_until_expiry = delta.days
        info.is_expired = delta.days < 0
        info.is_expiring_soon = 0 <= delta.days <= 30

        # Serial number
        info.serial_number = format(cert.serial_number, 'x')

        # Thumbprint
        der_bytes = cert.public_bytes(serialization.Encoding.DER)
        import hashlib
        info.thumbprint_sha256 = hashlib.sha256(der_bytes).hexdigest()

        # Subject Alternative Names
        try:
            san_ext = cert.extensions.get_extension_for_class(
                x509.SubjectAlternativeName)
            info.san_dns = [
                name.value for name in
                san_ext.value.get_values_for_type(x509.DNSName)
            ]
            info.san_ips = [
                str(name.value) for name in
                san_ext.value.get_values_for_type(x509.IPAddress)
            ]
        except x509.ExtensionNotFound:
            pass

        # Basic Constraints (is CA?)
        try:
            bc = cert.extensions.get_extension_for_class(
                x509.BasicConstraints)
            info.is_ca = bc.value.ca
        except x509.ExtensionNotFound:
            info.is_ca = False

        # Key info
        pub_key = cert.public_key()
        from cryptography.hazmat.primitives.asymmetric import rsa, ec, ed25519
        if isinstance(pub_key, rsa.RSAPublicKey):
            info.key_algorithm = "RSA"
            info.key_bits = pub_key.key_size
        elif isinstance(pub_key, ec.EllipticCurvePublicKey):
            info.key_algorithm = f"EC ({pub_key.curve.name})"
            info.key_bits = pub_key.key_size
        elif isinstance(pub_key, ed25519.Ed25519PublicKey):
            info.key_algorithm = "Ed25519"
            info.key_bits = 256

        return info

    except ImportError:
        # Fall back to openssl
        return _parse_cert_via_openssl(cert_path)
    except Exception as e:
        info = CertInfo()
        info.subject_cn = f"(parse error: {e})"
        return info


def _parse_cert_via_openssl(cert_path: str) -> CertInfo:
    """Parse certificate using openssl subprocess (fallback)."""
    info = CertInfo()
    try:
        out = subprocess.check_output(
            ['openssl', 'x509', '-in', cert_path, '-text', '-noout'],
            stderr=subprocess.DEVNULL
        ).decode()

        for line in out.splitlines():
            line = line.strip()
            if 'Subject:' in line and 'CN=' in line:
                m = __import__('re').search(r'CN\s*=\s*([^,/\n]+)', line)
                if m:
                    info.subject_cn = m.group(1).strip()
            elif 'Not After' in line:
                # "Not After : Jan  1 00:00:00 2025 GMT"
                date_str = line.split(':', 1)[1].strip()
                try:
                    info.not_after = datetime.datetime.strptime(
                        date_str, '%b %d %H:%M:%S %Y %Z'
                    ).replace(tzinfo=datetime.timezone.utc)
                    delta = info.not_after - datetime.datetime.now(
                        datetime.timezone.utc)
                    info.days_until_expiry = delta.days
                    info.is_expired = delta.days < 0
                    info.is_expiring_soon = 0 <= delta.days <= 30
                except ValueError:
                    pass
            elif 'DNS:' in line or 'IP Address:' in line:
                for san in line.split(','):
                    san = san.strip()
                    if san.startswith('DNS:'):
                        info.san_dns.append(san[4:])
                    elif san.startswith('IP Address:') or san.startswith('IP:'):
                        info.san_ips.append(san.split(':')[1].strip())
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        info.subject_cn = f"(openssl error: {e})"
    return info


def hostname_matches_cert(hostname: str, cert: CertInfo) -> bool:
    """
    Check if a hostname matches the certificate's SANs.
    Implements basic wildcard matching (* prefix).
    """
    def match_one(pattern: str, host: str) -> bool:
        if pattern == host:
            return True
        if pattern.startswith('*.'):
            suffix = pattern[2:]
            # Wildcard matches exactly one label
            parts = host.split('.', 1)
            if len(parts) == 2 and parts[1] == suffix:
                return True
        return False

    for dns_san in cert.san_dns:
        if match_one(dns_san.lower(), hostname.lower()):
            return True
    if hostname in cert.san_ips:
        return True
    return False


# ─── TLS Connection Testing ─────────────────────────────────────────────────────

def test_tls_connection(
    hostname: str,
    port: int,
    ca_cert_path: Optional[str] = None,
    client_cert_path: Optional[str] = None,
    client_key_path: Optional[str] = None,
    timeout_s: float = 10.0,
) -> TLSConnectionResult:
    """
    Attempt a TLS connection and return connection details.
    Supports optional mTLS (client cert + key).
    """
    import time
    result = TLSConnectionResult(hostname=hostname, port=port)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2

    if ca_cert_path:
        ctx.load_verify_locations(ca_cert_path)
    else:
        ctx.load_default_certs()

    if client_cert_path and client_key_path:
        ctx.load_cert_chain(certfile=client_cert_path, keyfile=client_key_path)

    start = time.perf_counter()
    try:
        with socket.create_connection((hostname, port), timeout=timeout_s) as sock:
            with ctx.wrap_socket(sock, server_hostname=hostname) as ssock:
                result.handshake_ms = (time.perf_counter() - start) * 1000
                result.success = True
                result.tls_version = ssock.version() or ""
                result.cipher_suite = ssock.cipher()[0] if ssock.cipher() else ""

                # Get peer certificate
                der_cert = ssock.getpeercert(binary_form=True)
                if der_cert:
                    # Write to temp file and parse
                    import tempfile
                    with tempfile.NamedTemporaryFile(
                        suffix='.der', delete=False
                    ) as tf:
                        tf.write(der_cert)
                        tmp_path = tf.name
                    try:
                        result.server_cert = parse_cert_from_file(tmp_path)
                    finally:
                        os.unlink(tmp_path)

                # Check cert chain depth
                chain = ssock.getpeercert()
                if chain:
                    result.cert_chain_depth = 1  # basic

                if result.server_cert:
                    result.hostname_verified = hostname_matches_cert(
                        hostname, result.server_cert
                    )

    except ssl.SSLCertVerificationError as e:
        result.handshake_ms = (time.perf_counter() - start) * 1000
        result.error = f"Certificate verification failed: {e}"
    except ssl.SSLError as e:
        result.handshake_ms = (time.perf_counter() - start) * 1000
        result.error = f"SSL error: {e}"
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        result.handshake_ms = (time.perf_counter() - start) * 1000
        result.error = f"Connection error: {e}"

    return result


# ─── Certificate Expiry Monitoring ──────────────────────────────────────────────

def scan_cert_directory(
    directory: str,
    warn_days: int = 30,
    critical_days: int = 7,
) -> list[dict]:
    """
    Scan a directory for certificate files (.crt, .pem, .cer)
    and return expiry status for each.
    """
    issues = []
    cert_extensions = {'.crt', '.pem', '.cer', '.cert'}

    for path in Path(directory).rglob('*'):
        if path.suffix.lower() not in cert_extensions:
            continue
        if not path.is_file():
            continue

        cert = parse_cert_from_file(str(path))
        if cert.is_expired:
            issues.append({
                'path': str(path),
                'cn': cert.subject_cn,
                'days': cert.days_until_expiry,
                'severity': 'CRITICAL',
                'message': f'EXPIRED {abs(cert.days_until_expiry)} days ago',
            })
        elif cert.days_until_expiry <= critical_days:
            issues.append({
                'path': str(path),
                'cn': cert.subject_cn,
                'days': cert.days_until_expiry,
                'severity': 'CRITICAL',
                'message': f'Expires in {cert.days_until_expiry} days',
            })
        elif cert.days_until_expiry <= warn_days:
            issues.append({
                'path': str(path),
                'cn': cert.subject_cn,
                'days': cert.days_until_expiry,
                'severity': 'WARNING',
                'message': f'Expires in {cert.days_until_expiry} days',
            })

    return sorted(issues, key=lambda x: x['days'])


def check_remote_cert_expiry(
    endpoints: list[tuple[str, int]],
    ca_cert_path: Optional[str] = None,
    warn_days: int = 30,
) -> list[dict]:
    """
    Check TLS certificate expiry on remote endpoints.
    Each endpoint is (hostname, port).
    """
    results = []
    for hostname, port in endpoints:
        r = test_tls_connection(hostname, port, ca_cert_path=ca_cert_path)
        if r.success and r.server_cert:
            cert = r.server_cert
            severity = 'OK'
            if cert.is_expired:
                severity = 'CRITICAL'
            elif cert.days_until_expiry <= 7:
                severity = 'CRITICAL'
            elif cert.days_until_expiry <= warn_days:
                severity = 'WARNING'

            results.append({
                'endpoint': f'{hostname}:{port}',
                'cn': cert.subject_cn,
                'days_until_expiry': cert.days_until_expiry,
                'not_after': cert.not_after.isoformat() if cert.not_after else '',
                'severity': severity,
                'tls_version': r.tls_version,
                'cipher': r.cipher_suite,
                'handshake_ms': round(r.handshake_ms, 1),
            })
        else:
            results.append({
                'endpoint': f'{hostname}:{port}',
                'cn': '',
                'days_until_expiry': -1,
                'severity': 'ERROR',
                'error': r.error,
            })

    return sorted(results, key=lambda x: x.get('days_until_expiry', 9999))


# ─── Kafka TLS Config Validator ──────────────────────────────────────────────────

def validate_kafka_tls_config(
    broker_hostname: str,
    broker_port: int,
    ca_cert_path: str,
    client_cert_path: Optional[str] = None,
    client_key_path: Optional[str] = None,
) -> dict:
    """
    Validate all aspects of a Kafka TLS (or mTLS) configuration.
    Returns a dict of checks and their pass/fail status.
    """
    checks = {}

    # Check 1: CA cert file exists and is readable
    if os.path.exists(ca_cert_path):
        ca_cert = parse_cert_from_file(ca_cert_path)
        checks['ca_cert_readable'] = True
        checks['ca_cert_is_ca'] = ca_cert.is_ca
        checks['ca_cert_days_remaining'] = ca_cert.days_until_expiry
        if not ca_cert.is_ca:
            checks['ca_cert_warning'] = (
                "CA cert does not have CA:TRUE basic constraint — "
                "may not validate broker certificates"
            )
    else:
        checks['ca_cert_readable'] = False
        checks['ca_cert_error'] = f"File not found: {ca_cert_path}"

    # Check 2: Client cert (if mTLS)
    if client_cert_path:
        if os.path.exists(client_cert_path):
            client_cert = parse_cert_from_file(client_cert_path)
            checks['client_cert_readable'] = True
            checks['client_cert_cn'] = client_cert.subject_cn
            checks['client_cert_days_remaining'] = client_cert.days_until_expiry
            checks['client_cert_expired'] = client_cert.is_expired
            checks['client_cert_expiring_soon'] = client_cert.is_expiring_soon
            if 'clientAuth' not in str(client_cert.extended_key_usage):
                checks['client_cert_warning'] = (
                    "Client cert may not have TLS Web Client Authentication EKU"
                )
        else:
            checks['client_cert_readable'] = False
            checks['client_cert_error'] = f"File not found: {client_cert_path}"

        # Check that key matches cert
        if (os.path.exists(client_cert_path) and
                client_key_path and os.path.exists(client_key_path)):
            try:
                mod_cert = subprocess.check_output(
                    ['openssl', 'x509', '-noout', '-modulus', '-in',
                     client_cert_path],
                    stderr=subprocess.DEVNULL
                ).decode().strip()
                mod_key = subprocess.check_output(
                    ['openssl', 'rsa', '-noout', '-modulus', '-in',
                     client_key_path],
                    stderr=subprocess.DEVNULL
                ).decode().strip()
                checks['client_cert_key_match'] = (mod_cert == mod_key)
            except (subprocess.CalledProcessError, FileNotFoundError):
                checks['client_cert_key_match'] = None  # could not verify

    # Check 3: TLS connection
    conn = test_tls_connection(
        broker_hostname, broker_port, ca_cert_path,
        client_cert_path, client_key_path
    )
    checks['connection_success'] = conn.success
    checks['tls_version'] = conn.tls_version
    checks['cipher_suite'] = conn.cipher_suite
    checks['handshake_ms'] = conn.handshake_ms
    checks['hostname_verified'] = conn.hostname_verified

    if not conn.success:
        checks['connection_error'] = conn.error
    elif conn.server_cert:
        checks['server_cert_cn'] = conn.server_cert.subject_cn
        checks['server_cert_days_remaining'] = conn.server_cert.days_until_expiry
        checks['server_cert_expired'] = conn.server_cert.is_expired
        checks['server_cert_expiring_soon'] = conn.server_cert.is_expiring_soon
        checks['server_cert_san_dns'] = conn.server_cert.san_dns

        if not conn.hostname_verified:
            checks['hostname_warning'] = (
                f"Hostname '{broker_hostname}' not found in server cert SANs: "
                f"{conn.server_cert.san_dns + conn.server_cert.san_ips}"
            )

    return checks


# ─── Report ──────────────────────────────────────────────────────────────────────

def tls_health_report(
    cert_dirs: Optional[list[str]] = None,
    endpoints: Optional[list[tuple[str, int]]] = None,
    ca_cert_path: Optional[str] = None,
) -> None:
    """Print a TLS health report for certificate directories and endpoints."""
    print("=" * 70)
    print("TLS / mTLS HEALTH REPORT")
    print(f"Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    if cert_dirs:
        for d in cert_dirs:
            print(f"\n[Certificate Directory: {d}]")
            if not os.path.isdir(d):
                print(f"  (directory not found)")
                continue
            issues = scan_cert_directory(d)
            if not issues:
                print("  ✓ All certificates appear healthy")
            else:
                for issue in issues:
                    icon = "🔴" if issue['severity'] == 'CRITICAL' else "🟡"
                    print(f"  {icon} [{issue['severity']}] {issue['path']}")
                    print(f"      CN: {issue['cn']}")
                    print(f"      {issue['message']}")

    if endpoints:
        print(f"\n[Remote Endpoint Certificate Check]")
        results = check_remote_cert_expiry(endpoints, ca_cert_path=ca_cert_path)
        for r in results:
            if r.get('error'):
                print(f"  🔴 {r['endpoint']}: ERROR — {r['error']}")
            else:
                icon = {"OK": "✓", "WARNING": "🟡", "CRITICAL": "🔴"}.get(
                    r['severity'], "?")
                print(f"  {icon} {r['endpoint']}: {r['cn']}")
                print(f"      Expires: {r['not_after'][:10]} "
                      f"({r['days_until_expiry']} days remaining)")
                print(f"      TLS: {r['tls_version']}, "
                      f"Cipher: {r['cipher']}, "
                      f"Handshake: {r['handshake_ms']}ms")

    print()
    print("=" * 70)


# ─── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TLS diagnostic toolkit")
    parser.add_argument('--connect', metavar='HOST:PORT',
                        help='Test TLS connection to HOST:PORT')
    parser.add_argument('--ca', metavar='CA_CERT',
                        help='CA certificate for validation')
    parser.add_argument('--cert', metavar='CLIENT_CERT',
                        help='Client certificate for mTLS')
    parser.add_argument('--key', metavar='CLIENT_KEY',
                        help='Client private key for mTLS')
    parser.add_argument('--inspect', metavar='CERT_FILE',
                        help='Inspect a certificate file')
    parser.add_argument('--scan-dir', metavar='DIR',
                        help='Scan directory for expiring certificates')
    args = parser.parse_args()

    if args.inspect:
        cert = parse_cert_from_file(args.inspect)
        print(f"Subject CN     : {cert.subject_cn}")
        print(f"Issuer CN      : {cert.issuer_cn}")
        print(f"Not Before     : {cert.not_before}")
        print(f"Not After      : {cert.not_after}")
        print(f"Days remaining : {cert.days_until_expiry}"
              + (" ⚠️ EXPIRING SOON" if cert.is_expiring_soon else "")
              + (" 🔴 EXPIRED" if cert.is_expired else ""))
        print(f"SAN DNS        : {cert.san_dns}")
        print(f"SAN IPs        : {cert.san_ips}")
        print(f"Is CA          : {cert.is_ca}")
        print(f"Key            : {cert.key_algorithm} {cert.key_bits} bit")

    elif args.connect:
        host, port = args.connect.rsplit(':', 1)
        result = test_tls_connection(
            host, int(port),
            ca_cert_path=args.ca,
            client_cert_path=args.cert,
            client_key_path=args.key,
        )
        if result.success:
            print(f"✓ Connected to {host}:{port}")
            print(f"  TLS version    : {result.tls_version}")
            print(f"  Cipher suite   : {result.cipher_suite}")
            print(f"  Handshake time : {result.handshake_ms:.1f}ms")
            if result.server_cert:
                cert = result.server_cert
                print(f"  Server cert CN : {cert.subject_cn}")
                print(f"  Days remaining : {cert.days_until_expiry}")
                print(f"  Hostname match : {result.hostname_verified}")
        else:
            print(f"✗ Connection failed: {result.error}")

    elif args.scan_dir:
        issues = scan_cert_directory(args.scan_dir)
        if not issues:
            print(f"✓ No expiry issues in {args.scan_dir}")
        else:
            for issue in issues:
                print(f"[{issue['severity']}] {issue['path']}: {issue['message']}")

    else:
        tls_health_report(
            cert_dirs=['/etc/kafka/ssl'] if os.path.isdir('/etc/kafka/ssl') else [],
            endpoints=[],
        )
```

---

## 6. Visual Reference

### TLS 1.3 Handshake Timeline

```
        Client                          Server
           │                               │
   TCP SYN │──────────────────────────────►│
           │◄─────────────────────── SYN-ACK│
   TCP ACK │──────────────────────────────►│
           │          [TCP: 1 RTT]         │
           │                               │
ClientHello│──────────────────────────────►│  Includes: key share, supported ciphers
(+key share)                               │
           │◄──────────────────────────────│  ServerHello + key share
           │◄──────────────────────────────│  {Certificate} (encrypted)
           │◄──────────────────────────────│  {CertificateVerify}
           │◄──────────────────────────────│  {Finished}
           │          [TLS: 1 RTT]         │
           │                               │
  {Finished}──────────────────────────────►│
           │                               │
  DATA ════╪═══════════════════════════════╪════
           │     Total: 2 RTTs (TCP+TLS)   │
```

### Certificate Chain Validation

```
Client Truststore                 Certificate Chain from Server
─────────────────                 ─────────────────────────────
                                  [3] kafka-broker-1.prod.internal
                                       Signed by: Internal-Intermediate-CA
                                             │
                                  [2] Internal-Intermediate-CA
                                       Signed by: Internal-Root-CA
                                             │
[1] Internal-Root-CA ◄──── MATCH ───── [1] Internal-Root-CA
    (in truststore)                         Self-signed
    TRUSTED                                 (sent by server for chain completion)

Validation: Walk chain upward until reaching a certificate in the truststore.
If no match found → PKIX path building failed.
```

### mTLS: Bidirectional Authentication

```
Standard TLS                    mTLS
────────────                    ─────
Client → Server                Client ←→ Server
  Server presents cert           Server presents cert
  Client validates server         Client validates server cert
                                  Server requests client cert   ← added
                                  Client presents client cert   ← added
                                  Server validates client cert  ← added

  Who is authenticated?           Who is authenticated?
  Server only.                    Both sides.
  Client is anonymous.            Client has cryptographic identity.
```

### Keystore vs Truststore

```
BROKER                          CLIENT

Keystore:                       Truststore:
 ├─ broker.crt (its own cert)    ├─ CA.crt (trusts whoever signed broker cert)
 └─ broker.key (private key)
                                 Keystore (mTLS only):
Truststore (mTLS only):          ├─ client.crt (its own cert)
 └─ CA.crt (trusts whoever        └─ client.key (private key)
    signed client cert)
```

---

## 7. Common Mistakes

**Mistake 1: Missing SAN in the certificate.** A certificate where the `Subject Alternative Name` extension does not list the hostname the client connects to. Modern TLS clients (Java 7+, Python ssl module) validate the SAN, not the CN. A certificate with `CN=kafka-broker-1.prod.internal` but no SAN extension will fail validation even though the CN matches. Always specify SANs explicitly in the OpenSSL config when generating certificates.

**Mistake 2: Setting `ssl.endpoint.identification.algorithm=` (empty) to silence errors.** When TLS hostname verification fails, a common "fix" is to disable it: `ssl.endpoint.identification.algorithm=`. This disables hostname verification entirely, eliminating the protection that TLS provides against man-in-the-middle attacks. The correct fix is to regenerate the certificate with the correct SAN.

**Mistake 3: Not including the full chain in the server's keystore.** If the broker's keystore contains only the end-entity certificate (not the intermediate CA certificate), clients whose truststore only contains the root CA will fail to build the certificate chain. The broker must send the complete chain: end-entity + all intermediate CAs up to (but not including) the root. Use `openssl pkcs12 -chain -CAfile intermediate-ca.crt` to bundle the chain.

**Mistake 4: Mismatched CAs between keystore and truststore.** If the client certificate was signed by CA-A but the broker's truststore only contains CA-B, mTLS fails with `certificate_unknown` during the handshake. The CA in the truststore must be the exact CA (or a CA in the chain) that signed the presented certificate.

**Mistake 5: No certificate rotation process.** Setting up TLS once and leaving it until certificates expire. Production systems must have automated certificate rotation: monitoring for expiry (alert at 30 days, page at 7 days), automated rotation procedures tested in non-production, and runbooks for emergency certificate replacement.

**Mistake 6: Storing private keys in plain text in application config files.** `ssl.key.password=plaintext_password` in a Kafka client config committed to git is a private key exposure risk. Use secrets management (Vault, Kubernetes Secrets, AWS Secrets Manager) to inject keystores and passwords at runtime.

---

## 8. Production Failure Scenarios

### Scenario 1: Kafka Cluster Goes Dark at 3 AM — Certificate Expiry

**Symptoms:** All Kafka producers and consumers fail simultaneously with `SSL handshake failed`. Kafka brokers are running. Error in client logs: `javax.net.ssl.SSLHandshakeException: PKIX path validation failed: java.security.cert.CertPathValidatorException: validity check failed`. The time is exactly when the certificate's `Not After` date passed.

**Root cause:** The broker certificate expired. Every client that attempts a new TLS handshake (or that reconnects after the expiry) fails certificate validation because the certificate is past its validity period.

**Immediate mitigation:**
```bash
# 1. Generate a new certificate immediately (same CA, same SANs, new validity)
openssl x509 -req -days 365 -in broker.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -extfile broker_ext.cnf -extensions v3_req \
  -out broker_new.crt

# 2. Update the keystore
keytool -delete -alias kafka-broker-1 \
  -keystore kafka.broker.keystore.jks -storepass keystorepassword
# (then re-import with the new cert)

# 3. Reload Kafka config (rolling restart to pick up new keystore)
# On a Kafka cluster, restart one broker at a time:
systemctl restart kafka

# 4. Verify new cert is being served
openssl s_client -connect kafka-broker-1:9093 2>/dev/null | \
  openssl x509 -noout -enddate
```

**Prevention:** Certificate expiry monitoring is non-negotiable.
```bash
# Prometheus + alertmanager alert rule:
# alert: KafkaCertificateExpiringSoon
# expr: (ssl_certificate_expiry_seconds - time()) / 86400 < 30
# for: 1h
# labels: severity: warning
# annotations: summary: "Kafka cert expires in {{ $value | humanizeDuration }}"
```

### Scenario 2: New Kafka Consumer Fails mTLS — Wrong SAN

**Symptoms:** A newly deployed Kafka consumer pod fails all connections with `javax.net.ssl.SSLHandshakeException: No subject alternative names present`. Existing consumers (deployed last month) work fine. The broker certificate is the same.

**Root cause:** `ssl.endpoint.identification.algorithm=HTTPS` is enabled on the consumer. The broker certificate was generated without a SAN extension — it has `CN=kafka-broker-1.prod.internal` in the Subject but no `X509v3 Subject Alternative Name` extension. Older Java versions and older consumer versions validated the CN; newer versions (Java 7+ in strict mode) require SANs. The new consumer was built with a newer JVM or a newer Kafka client library that enforces SAN validation.

**Diagnosis:**
```bash
openssl s_client -connect kafka-broker-1.prod.internal:9093 2>/dev/null | \
  openssl x509 -text -noout | grep -A5 "Subject Alternative"
# If nothing appears → no SAN extension → this is the problem
```

**Fix:** Regenerate the broker certificate with explicit SANs:
```bash
cat > broker_ext.cnf << EOF
[v3_req]
subjectAltName = DNS:kafka-broker-1.prod.internal, \
                 DNS:kafka.prod.internal, \
                 IP:10.0.1.47
extendedKeyUsage = serverAuth
EOF

openssl x509 -req -days 365 -in broker.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -extfile broker_ext.cnf -extensions v3_req \
  -out broker_new.crt
```

**Short-term workaround (while cert is being regenerated):** Set `ssl.endpoint.identification.algorithm=` (empty) on the consumer to disable hostname verification. This is a security regression — document it, set a ticket to regenerate the cert within 24 hours, and never leave it empty in production permanently.

### Scenario 3: mTLS Failure After CA Rotation

**Symptoms:** After rotating the internal CA (the old CA expired), Kafka producers begin failing with `javax.net.ssl.SSLHandshakeException: Received fatal alert: certificate_unknown`. The broker certificate was regenerated with the new CA. But the brokers' truststore (for mTLS client validation) still only contains the old CA.

**Root cause:** The sequencing was wrong. The broker keystore was updated with a new CA-signed certificate, but the broker truststore was not updated with the new CA's root certificate. Clients whose certificates were signed by the new CA cannot be validated by the broker because the broker's truststore only trusts the old CA.

**Correct CA rotation sequence:**
1. Generate new CA.
2. **Update all truststores to include BOTH old CA and new CA.** (Dual-CA trust period.)
3. Rotate broker certificates to be signed by the new CA. Deploy to brokers.
4. Rotate all client certificates to be signed by the new CA. Deploy to clients.
5. Wait until no clients have certificates signed by the old CA.
6. Remove old CA from all truststores.

The critical step is #2: both CAs must be trusted simultaneously during the transition to allow old-CA-signed clients and new-CA-signed clients to coexist. Any truststore update that removes the old CA before all clients have rotated will cause the failure described above.

### Scenario 4: TLS Handshake Latency Spike Under Load

**Symptoms:** Kafka producer P99 produce latency increases from 5 ms to 80 ms during periods of high producer connection churn (e.g., when a Spark job starts hundreds of executors simultaneously). The latency returns to normal after the connection burst subsides. TLS is enabled; mTLS is enabled.

**Root cause:** Certificate verification overhead. During mTLS, the broker must validate each client certificate. Certificate validation involves: parsing the certificate, building the chain, verifying signatures (RSA or ECDSA operations), and optionally checking OCSP. With RSA-2048 client certificates, each verification requires an RSA public key operation (~0.5 ms on modern hardware). If 500 Spark executors connect simultaneously and each requires mTLS verification, the broker's certificate verification thread pool is saturated, queuing handshakes and adding latency.

**Fixes:**
1. Use ECDSA certificates (P-256) instead of RSA-2048. ECDSA verification is 5–10× faster.
2. Enable TLS session resumption on Kafka brokers (via `ssl.session.timeout.ms` and `ssl.session.cache.size`) so reconnecting clients skip the full handshake.
3. Configure `reconnect.backoff.ms` and `reconnect.backoff.max.ms` on Spark-side Kafka clients to stagger the connection burst.
4. Increase the broker's `num.network.threads` to handle more concurrent handshakes.

---

## 9. Performance and Tuning

### TLS Performance Overview

TLS adds three types of overhead:

**1. Handshake latency (one-time per connection):**
- TCP handshake: 1 RTT
- TLS 1.3: +1 RTT
- TLS 1.2: +2 RTTs
- Certificate verification: 1–10 ms CPU (RSA) or 0.1–1 ms (ECDSA/P-256)
- Total for new connection, same AZ (1 ms RTT): TLS 1.3 = 2 ms; TLS 1.2 = 3 ms

**2. Encryption/decryption throughput overhead:**
- AES-GCM with hardware AES-NI: < 0.5% CPU overhead at 10 Gbps
- Without AES-NI (older hardware): 5–15% CPU overhead
- ChaCha20-Poly1305: slightly more CPU but no hardware dependency

**3. Header overhead per record:**
- TLS record header: 5 bytes
- MAC/authentication tag: 16 bytes (AEAD ciphers)
- Total: 21 bytes per TLS record
- With typical 16 KB TLS records: < 0.15% overhead

**Practical conclusion:** On modern hardware with AES-NI, encryption overhead is negligible. The significant overhead is the handshake — which is why connection pooling and session resumption matter.

### Kafka TLS Tuning

```properties
# server.properties — broker TLS tuning

# Use TLS 1.3 only (1.2 as fallback)
ssl.protocol=TLSv1.3
ssl.enabled.protocols=TLSv1.3,TLSv1.2

# Prefer ECDSA certificates (faster verification than RSA)
# (configure by using ECDSA keys when generating certificates)

# Session resumption cache (reduces handshake overhead for reconnecting clients)
ssl.session.timeout.ms=86400000   # 24 hours — cache sessions for a day
ssl.session.cache.size=20480       # cache 20k sessions

# Network threads — increase for high connection churn environments
num.network.threads=8   # default 3; increase for TLS handshake saturation

# Cipher suites — explicit allowlist (in preference order)
ssl.cipher.suites=TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,\
TLS_CHACHA20_POLY1305_SHA256
```

### ECDSA vs RSA for Kafka Certificates

| Property | RSA-2048 | RSA-4096 | ECDSA P-256 |
|----------|----------|----------|-------------|
| Key size | 2048 bit | 4096 bit | 256 bit (equivalent to RSA-3072) |
| Sign speed | ~1 ms | ~5 ms | ~0.1 ms |
| Verify speed | ~0.05 ms | ~0.1 ms | ~0.2 ms |
| Handshake CPU | Moderate | High | Low |
| Certificate size | ~1.5 KB | ~2 KB | ~0.7 KB |
| Security | Good (2030) | Excellent | Excellent |

For Kafka broker certificates: ECDSA P-256 is strongly preferred. Smaller certificates, faster verification, excellent security. The main reason RSA is still common is legacy tooling and institutional familiarity — not technical superiority.

### Certificate Management Best Practices

```bash
# Prometheus alert: cert expiring within 30 days
# Using ssl_exporter (https://github.com/ribbybibby/ssl_exporter):
# ssl_cert_not_after{...} < time() + 30*86400

# Automated cert rotation with cert-manager (Kubernetes):
# cert-manager automatically renews certificates 30 days before expiry
# and stores them in Kubernetes Secrets

# Vault PKI secrets engine for programmatic cert issuance:
# vault write pki/issue/kafka \
#   common_name="kafka-broker-1.prod.internal" \
#   alt_names="kafka.prod.internal" \
#   ip_sans="10.0.1.47" \
#   ttl="720h"    # 30-day certificates rotated frequently
```

---

## 10. Interview Q&A

**Q1: Explain the TLS 1.3 handshake. How many round trips does it require, and why is that an improvement over TLS 1.2?**

TLS 1.3 requires one round trip for the handshake itself, on top of the TCP three-way handshake's one round trip — two RTTs total from the moment a client initiates a connection to the moment it can send application data.

The improvement over TLS 1.2 comes from a fundamental redesign of how key exchange works. In TLS 1.2, the client first sends a ClientHello advertising its capabilities, waits for the server to respond and select a cipher suite, and only then begins the key exchange in the second flight. That produces two round trips for the handshake. TLS 1.3 eliminates the wait by having the client include its key share — the client's side of the ECDHE key exchange — directly in the ClientHello. The client guesses which key exchange group the server will prefer (it offers shares for the most common groups like X25519 and P-256). The server receives the ClientHello, selects a cipher suite, performs the key exchange with the client's key share, derives the session key, and immediately sends its own key share plus the encrypted certificate, CertificateVerify, and Finished — all in one flight. The client receives everything, verifies the server certificate, derives the same session key independently, sends Finished, and begins sending application data. The entire handshake is one RTT.

This matters for Kafka in two specific scenarios. First, any system with high connection churn — Spark jobs that create executor connections, or producers that reconnect frequently — pays the handshake cost many times. Reducing from 2 to 1 RTT halves the per-connection setup overhead. At a cross-AZ RTT of 2 ms, this saves 2 ms per connection — which accumulates when hundreds of connections are being established simultaneously during a job startup. Second, TLS 1.3 also removes the ability to use weak cipher suites that TLS 1.2 still supported (RC4, MD5, SHA-1), improving security without configuration effort.

**Q2: What is mTLS and why would you use it for a Kafka cluster rather than just standard TLS with SASL/PLAIN authentication?**

Standard TLS authenticates only the server: the client verifies the broker's certificate and establishes an encrypted channel, but the broker has no cryptographic proof of who the client is. Authentication of the client is then handled by the application layer — in Kafka's case, mechanisms like SASL/PLAIN (username/password), SASL/SCRAM (hashed password), or SASL/GSSAPI (Kerberos). These mechanisms transmit a credential over the already-encrypted TLS channel.

Mutual TLS extends the authentication to the client at the TLS layer. During the TLS handshake, the server sends a CertificateRequest, and the client must present a valid X.509 certificate signed by a CA that the server's truststore trusts. The client also sends a CertificateVerify message — a digital signature over the handshake transcript using the client's private key — proving that the client actually possesses the private key corresponding to the certificate. This provides cryptographic identity: you cannot impersonate a client without possessing its private key.

The advantages of mTLS over SASL/PLAIN for Kafka: there is no shared secret to steal — SASL/PLAIN passwords can be leaked in config files, logs, or credential stores, while a private key that never leaves the client's keystore has no such exposure. Identity is per-client and revocable — each producer or consumer application can have its own certificate with a unique Subject (e.g., `CN=payments-producer`), enabling Kafka ACLs to be scoped per certificate subject. A compromised client can have its certificate revoked via CRL without requiring password resets across the system. And the authentication is hardware-bound if the private key is stored in an HSM or TPM.

The disadvantage: operational complexity. Certificates must be issued, distributed, monitored for expiry, and rotated. This requires either a robust internal PKI or a secrets management system. SASL/SCRAM is simpler to operate if the security requirements don't demand hardware-bound authentication.

**Q3: A Kafka consumer fails with `PKIX path building failed: unable to find valid certification path to requested target`. Describe exactly what is happening and how you fix it.**

This error means the consumer's TLS certificate chain validator could not establish a trust path from the broker's certificate to any certificate in the consumer's truststore. The validator walks the certificate chain: it starts with the broker's end-entity certificate, identifies its issuer (from the `Issuer` field), looks for that issuer's certificate in either the chain presented by the broker or the truststore, and repeats upward until it either finds a trusted root or exhausts all options. "Unable to find valid certification path" means it exhausted all options without hitting a trusted root.

There are four distinct root causes. First, the broker's certificate was signed by an internal CA whose root certificate is not in the consumer's truststore. This is the most common cause. The fix is to add the internal CA's root certificate to the consumer's truststore: `keytool -import -trustcacerts -alias internal-ca -file ca.crt -keystore client.truststore.jks`. Second, the broker is not sending the full certificate chain — it's only sending its end-entity certificate without the intermediate CA. The consumer cannot build the chain because it doesn't have the intermediate CA certificate. The fix is to include the intermediate CA in the broker's keystore (bundle the full chain in the PKCS12 before importing to JKS). Third, the truststore is being loaded from the wrong path — a misconfigured `ssl.truststore.location` pointing to a file that doesn't contain the right CA. Fourth, the truststore contains the right CA certificate but it was imported without the `-trustcacerts` flag, so it's stored as a regular certificate rather than a trusted anchor.

Diagnosis: `openssl s_client -connect kafka-broker-1:9093 -showcerts -CAfile ca.crt` shows the full chain the broker presents and whether it validates. `keytool -list -v -keystore client.truststore.jks` shows what's in the truststore. Cross-referencing the `Issuer` in the broker certificate with the `Subject` of certificates in the truststore will identify the gap.

---

## 11. Cross-Question Chain

**Interviewer:** You're asked to enable mTLS on a production Kafka cluster that currently has no TLS at all. Three brokers, hundreds of producers and consumers. Walk me through your approach.

**Candidate:** This is a live system, so zero-downtime is the constraint. I'd do it in four phases. Phase one: build the PKI. Generate an internal CA and sign broker certificates (one per broker, with correct SANs for the broker's DNS name and IP). Distribute the CA root certificate to every machine in the cluster — this is the foundation. Phase two: enable TLS listeners on brokers *alongside* the existing PLAINTEXT listener, not replacing it. Kafka supports multiple listeners simultaneously: `listeners=PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9093`. This lets me add TLS without touching existing clients. Phase three: migrate clients. Update each client application's configuration to use `security.protocol=SSL` with the keystore and truststore pointing to the new certificates. Roll clients in batches, verifying connectivity after each batch. Phase four: after all clients are on SSL, remove the PLAINTEXT listener from brokers and set `ssl.client.auth=required` for mTLS. Then rolling restart the brokers again to pick up the final config. Total disruption: two rolling restarts of the broker fleet, each taking 20–30 minutes for a 3-broker cluster.

**Interviewer:** During phase two, you add the SSL listener. The first broker restart goes fine. On the second broker, producers start throwing `NotLeaderOrFollowerException` during the restart. Is this a TLS problem?

**Candidate:** No — this is normal behavior during a rolling restart. When a broker restarts, it is temporarily unavailable. Its partitions undergo leader election, and producers sending to partitions that were led by the restarting broker get `NotLeaderOrFollowerException` until the new leader is elected and the producer's metadata is refreshed. This is a Kafka partition leadership issue, not TLS. The signal that would indicate a TLS problem is `SSLHandshakeException`, `SSLPeerUnverifiedException`, or `PKIX path building failed`. The `NotLeaderOrFollowerException` during a restart is expected and self-heals as soon as the producer refreshes its metadata (controlled by `metadata.max.age.ms`). To minimize it: ensure `min.insync.replicas=1` during the migration to allow leadership to switch quickly, and stagger restarts by at least 30 seconds to allow leader election to complete.

**Interviewer:** After enabling mTLS (phase four), a new producer team tries to connect and gets `Received fatal alert: certificate_unknown`. What are the possible causes?

**Candidate:** `certificate_unknown` is a TLS alert sent by the server — the broker — indicating it received a client certificate but could not validate it. The four most likely causes: the client certificate was signed by a CA that is not in the broker's truststore; the client certificate was signed by the correct CA but the CA certificate in the truststore is expired; the client certificate has been explicitly revoked and the broker is performing CRL or OCSP checking; or the client sent no certificate at all (their keystore path or password is wrong, causing no certificate to be presented, which the broker treats as `certificate_unknown`).

To diagnose: `openssl s_client -connect kafka-broker-1:9093 -cert client.crt -key client.key -CAfile ca.crt` — if this fails with the same alert, the issue is the certificate itself. Then check: `openssl verify -CAfile broker-truststore-ca.crt client.crt` — if this fails, the CA mismatch is confirmed. Check the broker truststore with `keytool -list -v -keystore kafka.broker.truststore.jks` to see which CAs are trusted, and compare the issuer in the client certificate against the subjects of trusted CAs.

**Interviewer:** It turns out the client cert was signed by a different department's CA — CA-B — and the broker only trusts CA-A. How do you fix this without disrupting existing clients?

**Candidate:** The solution is to add CA-B to the broker's truststore without removing CA-A. A truststore can contain multiple trusted CAs; any client certificate signed by any of them will be accepted. I'd add CA-B: `keytool -import -trustcacerts -alias dept-ca-b -file ca-b.crt -keystore kafka.broker.truststore.jks -storepass truststorepassword`. Then instruct Kafka to reload the truststore without a full broker restart — Kafka supports dynamic certificate reloading via `kafka-configs.sh --alter` for some configs, but the truststore reload typically requires a broker restart. I'd do a rolling restart (broker by broker) after updating the truststore on each node. The disruption is a normal rolling restart, and existing clients (using CA-A) are unaffected because CA-A is still in the truststore.

**Interviewer:** Six months later, certificates start expiring. How do you rotate a broker certificate without downtime?

**Candidate:** Certificate rotation without downtime requires the dual-certificate approach. First, generate a new certificate for the broker with a new expiry date — same CA, same SANs. Second, import it into the keystore with a *different alias* than the current certificate: `keytool -importkeystore -srckeystore new_broker.p12 -destkeystore kafka.broker.keystore.jks`. Now the keystore has two certificates: the old one (alias `kafka-broker-1-old`) and the new one (alias `kafka-broker-1-new`). Third, update the `ssl.keystore.location` and perform a rolling broker restart. When the broker restarts, it will present the new certificate. Clients that were connected to the old certificate will disconnect during the rolling restart and reconnect to the new certificate. Since both certificates are signed by the same CA, clients' truststores remain valid.

After verifying that all brokers are serving the new certificate (`openssl s_client -connect kafka-broker-1:9093 | openssl x509 -noout -enddate`), remove the old certificate from the keystore. For client certificates in mTLS, the same process applies: generate new client certs, update client keystores, roll client restarts, then clean up old entries.

**Interviewer:** What would you do differently if you had to rotate the CA itself — the root CA is expiring?

**Candidate:** CA rotation is the highest-risk certificate operation because it requires updating every truststore in the system. The correct sequence is: first, generate the new CA. Second — and this is the critical step — update every truststore to contain *both* the old CA and the new CA simultaneously. This must be done before any certificate is reissued. Every broker truststore and every client truststore must trust both CAs. Roll this out with a broker and client restart. Third, reissue all broker certificates signed by the new CA. Roll out to brokers. Fourth, reissue all client certificates signed by the new CA. Roll out to clients. Fifth — only after verifying that no certificates signed by the old CA are still in use — remove the old CA from all truststores. The intermediate state where both CAs are trusted in parallel is the safety net: it means that any client or broker that hasn't yet rotated its certificate can still connect, because both CAs are trusted.

The danger is removing the old CA too early. If any client certificate is still signed by the old CA and you've removed the old CA from the broker's truststore, that client gets `certificate_unknown`. Always inventory all client certificates and their issuing CAs before removing any CA from a truststore.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What three security properties does TLS provide? | Confidentiality (encryption), Integrity (MAC), Server authentication (certificate). mTLS adds: Client authentication. |
| 2 | How many RTTs does a TLS 1.3 handshake add on top of the TCP handshake? | 1 RTT. TLS 1.3 sends the key share in the ClientHello, completing key exchange in one round trip. Total: 2 RTTs (1 TCP + 1 TLS). |
| 3 | How many RTTs does TLS 1.2 add? | 2 RTTs. Total: 3 RTTs (1 TCP + 2 TLS). TLS 1.3 is 1 RTT faster per connection. |
| 4 | What is a Certificate Authority (CA)? | A trusted entity that signs certificates, vouching that a public key belongs to a specific identity. The signer of your certificate must be in the verifier's truststore. |
| 5 | What is a Subject Alternative Name (SAN)? | An X.509v3 extension listing the hostnames and IP addresses the certificate is valid for. Modern TLS validates the SAN, not the CN. Missing SAN → hostname mismatch error. |
| 6 | What is the difference between a keystore and a truststore? | Keystore: your own certificate and private key. Truststore: CA certificates you trust to sign others' certificates. |
| 7 | What does `ssl.client.auth=required` do in Kafka? | Enables mTLS — the broker requires clients to present a valid certificate. `requested` = optional. `none` = no client cert required. |
| 8 | What error means the broker's certificate chain can't be validated? | `PKIX path building failed: unable to find valid certification path`. The CA that signed the broker cert is not in the client's truststore. |
| 9 | What does `certificate_unknown` alert mean in TLS? | The server received a client certificate (in mTLS) it couldn't validate — the signing CA is not in the server's truststore, the cert is revoked, or no cert was presented. |
| 10 | What is perfect forward secrecy (PFS)? | Each TLS session uses a fresh ephemeral key (ECDHE). Compromising the server's private key later cannot decrypt past sessions. Required in TLS 1.3. |
| 11 | Why prefer ECDSA over RSA for Kafka certificates? | ECDSA signing is 10× faster, verification is comparable, keys are 10× smaller, and certificates are more compact — less overhead in high-connection-churn environments. |
| 12 | What is TLS session resumption? | After an initial handshake, the server issues a session ticket. On reconnect, the client presents the ticket to resume without a full handshake (1 RTT → 0 RTT for TLS). |
| 13 | What must you do before changing a DNS record for a Kafka broker with TLS? | Ensure the new DNS name is in the broker's certificate SAN. If not, regenerate the certificate first — connecting via a name not in the SAN will fail hostname verification. |
| 14 | What does `ssl.endpoint.identification.algorithm=HTTPS` do in Kafka clients? | Enables hostname verification: the client checks that the connected hostname appears in the broker certificate's SAN. Setting it empty disables verification — a security regression. |
| 15 | How do you rotate a CA without downtime? | 1. Add new CA to all truststores (alongside old CA). 2. Reissue all certs with new CA. 3. Roll out new certs. 4. After all old-CA certs are replaced, remove old CA from truststores. |
| 16 | What command verifies a certificate was signed by a specific CA? | `openssl verify -CAfile ca.crt broker.crt` — returns `OK` if valid, error if not. |
| 17 | What command checks if a private key matches a certificate? | Compare modulus: `openssl x509 -noout -modulus -in cert.crt \| md5sum` must equal `openssl rsa -noout -modulus -in key.key \| md5sum`. |
| 18 | What is OCSP stapling? | The server pre-fetches and caches its OCSP revocation status, then sends it during the TLS handshake. Clients get revocation status without an extra round trip. |
| 19 | What JVM property controls DNS caching, and what should it be set to for Kafka? | `sun.net.inetaddr.ttl`. Default is -1 (cache forever). Set to 60 for Kafka so the JVM re-resolves broker hostnames after IP changes. |
| 20 | Why does a certificate without a SAN fail on modern Java even if the CN matches? | Java 7+ and all modern TLS clients require SANs for hostname validation (RFC 2818). The CN field is deprecated for this purpose and ignored. A missing SAN extension causes `No subject alternative names present`. |

---

## 13. Further Reading

- **"Bulletproof TLS and PKI" — Ivan Ristić:** The most thorough practitioner's guide to TLS. Covers certificate anatomy, PKI design, TLS configuration, and common vulnerabilities. Chapters 4–7 on certificates and deployment are directly applicable to Kafka TLS setup.
- **RFC 8446 (TLS 1.3):** The specification. Section 4 (Handshake Protocol) is readable and precise. Understanding the handshake from the spec eliminates ambiguity about what is happening during connection setup.
- **Confluent Kafka TLS/SSL documentation:** `https://docs.confluent.io/platform/current/kafka/authentication_ssl.html` — the definitive reference for Kafka TLS configuration. Covers keystore/truststore setup, mTLS, and troubleshooting step by step.
- **Let's Encrypt Certificate Compatibility:** Even for internal infrastructure, understanding Let's Encrypt's approach to certificate automation (ACME protocol, 90-day cert lifetimes, automated renewal) informs how to build robust certificate lifecycle management.
- **cert-manager (Kubernetes):** `https://cert-manager.io/docs/` — the standard Kubernetes certificate management controller. Integrates with Vault, Let's Encrypt, and internal CAs. Automates certificate issuance and renewal for Kubernetes-native Kafka deployments.
- **Hashicorp Vault PKI Secrets Engine:** `https://developer.hashicorp.com/vault/docs/secrets/pki` — Vault's programmatic CA. Enables short-lived certificates (hours instead of years) and automated renewal, eliminating the certificate expiry class of outage entirely.
- **"The TLS Handshake in Detail" — Cloudflare Blog:** A well-illustrated explanation of both TLS 1.2 and 1.3 handshakes with timing diagrams. Accessible without deep cryptographic background.

---

## 14. Lab Exercises

**Exercise 1: Build a Complete Internal PKI**

Using OpenSSL, generate: a root CA, an intermediate CA signed by the root, a broker certificate signed by the intermediate CA with correct SANs. Bundle the full chain. Verify with `openssl verify -CAfile root-ca.crt -untrusted intermediate-ca.crt broker.crt`. Then deliberately test failure: connect with a truststore that only contains the intermediate CA (missing root) — observe the error. Add the root to the truststore and retry.

**Exercise 2: Time TLS 1.2 vs TLS 1.3 Handshake**

Set up a test TLS server with `openssl s_server`. Time 100 TLS 1.2 handshakes and 100 TLS 1.3 handshakes with `openssl s_client` in a loop. Compare the per-handshake latency distribution. Add artificial latency with `tc qdisc add dev lo root netem delay 5ms` (5 ms loopback latency) and repeat — observe how the RTT difference between TLS 1.2 and 1.3 grows with latency.

**Exercise 3: Diagnose a Certificate SAN Mismatch**

Generate a certificate with `CN=kafka.prod.internal` but no SAN extension. Connect with Python's `ssl` module using `ssl.CERT_REQUIRED`. Observe the `CertificateError: hostname ... doesn't match`. Regenerate with a SAN. Verify the error disappears.

**Exercise 4: Set Up Kafka with mTLS (local Docker)**

Use the Confluent Platform Docker images. Configure: a broker keystore and truststore, a producer with a client certificate signed by the same internal CA, and a consumer with a different client certificate. Verify that each client can produce/consume. Then revoke one client certificate and add its serial to a CRL. Configure the broker to check the CRL. Verify that the revoked client is rejected while the valid client continues to work.

**Exercise 5: Simulate Certificate Expiry**

Generate a certificate with `-days 0` (expires immediately) or use `faketime` to advance the system clock past the certificate's expiry. Observe the SSL handshake failure. Practice the emergency certificate rotation procedure from scratch, timing how long it takes to restore service.

---

## 15. Key Takeaways

TLS is not a checkbox — it is a system. The components are: a certificate authority that everyone in the system trusts, certificates with correct SANs bound to each service's DNS name, keystores that hold private keys securely, truststores that hold only the CAs you intend to trust, and a certificate lifecycle process that rotates certificates before they expire.

The three most operationally important TLS facts for data engineers: certificate expiry is a Tier-1 incident — monitor expiry and alert at 30 days, page at 7 days, automate rotation; `ssl.endpoint.identification.algorithm` must never be disabled — a blank endpoint algorithm is a silently broken security posture, not a fix for hostname mismatch; and CA rotation requires a dual-trust period — adding the new CA to all truststores before reissuing any certificates is the invariant that prevents outages during CA rotation.

On performance: TLS 1.3 on modern hardware with AES-NI has negligible encryption overhead. The meaningful overhead is the handshake — one additional RTT over TCP. Minimizing this means connection pooling (fewer handshakes), TLS session resumption (cheaper reconnects), and ECDSA certificates (faster verification). For a same-AZ Kafka cluster, TLS 1.3 adds approximately 1–2 ms per new connection, which is insignificant if connections are long-lived.

---

## 16. Connections to Other Modules

- **M27 — TCP/IP Fundamentals:** TLS runs on top of TCP. The TLS handshake cost is measured in RTTs, and RTT is the fundamental TCP latency unit. TLS 1.3's 1-RTT handshake improvement matters more at high-latency paths (cross-region) than at same-rack paths.
- **M28 — DNS and Service Discovery:** TLS certificates are bound to DNS names via SANs. A DNS name change (broker hostname update, Kubernetes pod rescheduling) requires a certificate update if the new name is not already in the certificate's SAN. Certificate validation begins with DNS name matching.
- **M30 — Network Latency:** TLS handshake RTT adds to connection setup latency. The cross-region TLS handshake latency calculation is a direct application of M30's RTT analysis.
- **DCS-KFK-101 — Apache Kafka Internals:** Kafka's security model (SSL listener configuration, mTLS via `ssl.client.auth`, ACL integration with certificate subject) builds directly on TLS fundamentals.
- **DCS-KFK-102 — Kafka Ecosystem and Operations:** Module 4 (Kafka Security) covers mTLS setup, SASL integration, and ACLs in full operational detail — this module is the prerequisite.
- **SYS-NET-102 — Cloud Networking and Security:** Cloud-managed TLS (AWS ACM, GCP Certificate Manager) handles certificate issuance and rotation automatically for managed services. Understanding what the cloud does on your behalf requires knowing what TLS does at all.

---

## 17. Anti-Patterns

**Anti-pattern: Self-signed certificates in production.** A self-signed certificate is its own CA — it cannot be verified by a truststore unless it is explicitly added to every truststore. This works for development but creates a management nightmare in production: every new client must manually add the broker's self-signed certificate. Use an internal CA instead: sign all certificates with the CA, and add only the CA to truststores.

**Anti-pattern: Sharing a single certificate across all brokers.** Using one certificate with `CN=kafka.prod.internal` and the same private key on all three brokers means: if the private key is compromised, all broker communications are compromised simultaneously. Each broker should have its own certificate with its own private key and a unique SAN listing that broker's specific hostname and IP.

**Anti-pattern: Storing private keys unencrypted on disk.** A Kafka broker's `ssl.key.password` protects the keystore's private key entries. Using a blank password (`ssl.key.password=`) leaves the private key readable by any process with file system access. At minimum, use a password stored in a secrets manager (Vault, AWS Secrets Manager) injected at runtime.

**Anti-pattern: Not testing certificate rotation in staging.** Certificate rotation is a rare and high-risk operation. Without documented, tested procedures and a staging environment where rotation can be practiced, the first time you perform a rotation under pressure (because the cert expires in 2 hours) is the most likely time to make a mistake. Rotation procedures must be tested quarterly.

**Anti-pattern: Using TLS 1.0 or 1.1.** Both are deprecated and have known vulnerabilities (POODLE, BEAST, RC4 weaknesses). Configure `ssl.enabled.protocols=TLSv1.3,TLSv1.2` on Kafka brokers and clients. TLS 1.0 and 1.1 support in Java is disabled by default since Java 11.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `openssl x509 -text -noout -in cert.crt` | Inspect certificate | Check SAN, expiry, issuer |
| `openssl s_client -connect host:port` | Test TLS connection | `-showcerts` for chain, `-cert/-key` for mTLS |
| `openssl verify -CAfile ca.crt cert.crt` | Verify certificate against CA | Returns `OK` or error |
| `openssl genrsa -out key.pem 2048` | Generate RSA private key | `-ecparam -genkey -name prime256v1` for ECDSA |
| `openssl req -new -key key.pem -out csr.pem` | Generate CSR | `-subj "/CN=..."` for non-interactive |
| `openssl x509 -req -days 365 -in csr.pem -CA ca.crt -CAkey ca.key` | Sign a CSR | `-extfile ext.cnf` for SAN extensions |
| `openssl pkcs12 -export -in cert.crt -inkey key.pem` | Create PKCS12 bundle | `-chain -CAfile intermediate.crt` to include chain |
| `keytool -list -v -keystore ks.jks` | List keystore contents | `-storepass <pass>` required |
| `keytool -importkeystore -srckeystore ks.p12 -srcstoretype pkcs12` | Import PKCS12 into JKS | `-destkeystore ks.jks` |
| `keytool -import -alias ca -file ca.crt -keystore ts.jks` | Import CA into truststore | `-trustcacerts -noprompt` for scripting |
| `keytool -delete -alias old-cert -keystore ks.jks` | Remove cert from keystore | For cleanup after rotation |
| `openssl x509 -noout -enddate -in cert.crt` | Check expiry date only | Pipe to `date` for days remaining |
| `openssl x509 -noout -modulus -in cert.crt \| md5sum` | Cert/key match check | Must match output of same cmd with `-rsa` |
| `curl -w "%{time_appconnect}" -o /dev/null -s https://host/` | TLS handshake time | `time_appconnect` minus `time_connect` = TLS time |

---

## 19. Glossary

**AES-GCM:** Advanced Encryption Standard in Galois/Counter Mode. The dominant symmetric cipher in TLS 1.3. Hardware-accelerated on modern CPUs (AES-NI). Provides both encryption and authentication.

**CA (Certificate Authority):** A trusted entity that issues and signs digital certificates. The trust anchor for certificate validation. May be a public CA (DigiCert, Let's Encrypt) or an internal CA.

**Certificate Chain:** A sequence of certificates from the end-entity certificate up to a trusted root CA, where each certificate was signed by the next one in the chain.

**CRL (Certificate Revocation List):** A list published by a CA of certificates it has revoked before their expiry. Clients download and check the CRL to validate revocation status.

**CSR (Certificate Signing Request):** A message sent to a CA containing the public key and subject information for a certificate to be issued. The CA signs it and returns a certificate.

**ECDHE (Elliptic Curve Diffie-Hellman Ephemeral):** The key exchange algorithm used in TLS 1.3. Provides perfect forward secrecy through ephemeral key pairs.

**ECDSA (Elliptic Curve Digital Signature Algorithm):** An asymmetric signature algorithm using elliptic curve mathematics. Faster and smaller than RSA for equivalent security.

**End-Entity Certificate:** A certificate for a specific host or person, as opposed to a CA certificate. Has `Basic Constraints CA:FALSE`. Cannot sign other certificates.

**JKS (Java KeyStore):** Java's legacy keystore format. Still widely used for Kafka TLS configuration. PKCS12 is preferred but JKS is common in existing deployments.

**Keystore:** A file containing an entity's own certificate and private key. Used to present identity to others.

**mTLS (Mutual TLS):** TLS with both server and client authentication via certificates. The server validates the client's certificate just as the client validates the server's.

**OCSP (Online Certificate Status Protocol):** Real-time certificate revocation checking via an HTTP query to the CA's OCSP responder.

**OCSP Stapling:** The server pre-fetches its OCSP status and includes it in the TLS handshake, eliminating the client's need for a separate OCSP query.

**PKCS12:** A standard file format for storing certificate chains and private keys. The recommended format for Java keystores (as opposed to legacy JKS).

**PFS (Perfect Forward Secrecy):** Property that past TLS sessions cannot be decrypted if the server's private key is compromised in the future. Requires ephemeral key exchange (ECDHE). Mandatory in TLS 1.3.

**SAN (Subject Alternative Name):** X.509v3 extension listing hostnames and IPs a certificate is valid for. Modern TLS validates the SAN, not the CN. Required for hostname verification to succeed.

**Session Ticket:** A TLS resumption mechanism where the server encrypts session state and sends it to the client. The client presents it on reconnection to resume without a full handshake.

**TLS (Transport Layer Security):** The cryptographic protocol providing confidentiality, integrity, and authentication over TCP. Current version: TLS 1.3 (RFC 8446, 2018).

**Truststore:** A file containing CA certificates that the entity trusts. Used to validate others' certificates.

**X.509:** The standard defining the format of public-key certificates. Used in TLS, HTTPS, email signing, and code signing.

---

## 20. Self-Assessment

1. What are the four security properties that mTLS provides that standard TLS does not? In what production scenario is each property valuable?
2. Draw the TLS 1.3 handshake message sequence. Label each message and identify which messages are encrypted. How many RTTs total (including TCP) before application data flows?
3. A Kafka broker certificate has `CN=kafka.prod.internal` but no SAN extension. A Java client connecting with `ssl.endpoint.identification.algorithm=HTTPS` fails. Explain exactly what validation step fails and why.
4. What is the `Issuer` field in a certificate, and how does it relate to the truststore on the connecting client?
5. Describe the correct sequence for rotating the root CA in a Kafka cluster with mTLS, emphasizing the invariant that prevents outages.
6. A broker certificate uses RSA-2048, and you're seeing high CPU on the broker during Spark job startup (hundreds of executors connecting simultaneously). What is causing the CPU spike and what is the fix?
7. What is perfect forward secrecy, and why is it important for a Kafka cluster that handles regulated financial data?
8. You need to generate a broker certificate for `kafka-1.kafka.prod.internal` that is also accessible as `kafka.prod.internal` and via its IP `10.0.1.6`. What OpenSSL configuration do you need to ensure this works?
9. What is OCSP stapling, what problem does it solve, and why is it preferable to the non-stapled OCSP approach?
10. A Kafka consumer runs in Docker. The `ssl.truststore.location` is set to a path that exists on the host but the container only mounts `/etc/kafka/ssl/`. What error will the consumer produce, and how do you diagnose it?

---

## 21. Module Summary

TLS is the cryptographic foundation on which secure data infrastructure operates. It provides confidentiality, integrity, and server authentication through a handshake protocol that negotiates a shared key using asymmetric cryptography, then encrypts all application data with fast symmetric ciphers. mTLS extends this to bidirectional authentication — both the server and client present certificates, enabling cryptographic identity for producers, consumers, and brokers without passwords.

The TLS 1.3 handshake requires one RTT after the TCP handshake — two RTTs total before application data flows. The handshake is the dominant TLS overhead for connection-heavy data systems; bulk encryption overhead is negligible on modern hardware with AES-NI. For Kafka specifically, this means every new TCP connection between a producer and a broker pays the handshake cost: use connection pooling, enable session resumption, prefer TLS 1.3, and use ECDSA certificates to minimize per-handshake CPU.

The three pillars of operational TLS competence are: certificate lifecycle management (monitor expiry, automate rotation, test rotation procedures in staging), correct PKI structure (internal CA signing all certificates, correct SANs on every certificate, complete chain in keystores), and truststore hygiene (each truststore contains exactly the CAs you intend to trust — no more, no less).

Certificate expiry has taken down production Kafka clusters. Hostname mismatch has silently broken connections. CA rotation done in the wrong order has caused cascading authentication failures. These are not exotic failure modes — they are the routine failures that happen when TLS is treated as infrastructure detail rather than engineering concern.

The next module — M30: Network Latency — builds directly on this module's RTT analysis, extending from the latency of a single connection to the latency implications of distributed consensus protocols (Raft) and cross-region replication in a world where every network hop has a physics-based lower bound.
