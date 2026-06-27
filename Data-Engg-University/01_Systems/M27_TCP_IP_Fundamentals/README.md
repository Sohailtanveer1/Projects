# M27: TCP/IP Fundamentals

**Course:** SYS-NET-101 — Networking for Data Engineers  
**Module:** 01 of 05  
**Global Module ID:** M27  
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

Every distributed data system — Kafka brokers replicating partitions, Spark executors exchanging shuffle data, Flink checkpointing state to S3 — moves bytes over a network. The network is not a transparent wire. It drops packets. It reorders them. It introduces latency that varies by time of day, by route, by congestion. It has limits: a single TCP connection cannot exceed the bandwidth-delay product of its path, a principle most data engineers have never heard of but encounter every day as "why is this data transfer slow?"

The reason TCP/IP fundamentals matter for data engineers — specifically — is that ignorance of them leads to wrong mental models about performance and wrong configurations in production.

**The wrong mental model:** "The network is fast enough; if my pipeline is slow it must be my code or my cluster."

**The right mental model:** "Every remote call pays an RTT tax. Every large data transfer pays a bandwidth-delay product tax. Every misconfigured TCP socket pays a retransmission tax."

Three concrete examples of why this matters:

**Example 1 — Kafka `acks=all` latency.** When a Kafka producer sends a record with `acks=all`, the broker must replicate to all ISR replicas before acknowledging. Each replication round trip is a full TCP RTT between brokers. On the same rack that's 100–200 µs. Across availability zones it's 1–3 ms. Across regions it's 30–150 ms. A data engineer who doesn't understand TCP RTT will cargo-cult the `acks=all` setting without understanding that they are guaranteeing at minimum one cross-broker RTT per batch, and they will not understand why their producer latency P99 is exactly the AZ-crossing RTT.

**Example 2 — Spark shuffle performance.** The Spark shuffle phase sends intermediate data between Executors over TCP. If the Executor JVM has too few TCP connections open simultaneously (because `spark.shuffle.io.numConnectionsPerPeer` is too low) or if TCP slow start keeps each connection at low throughput for the first few seconds, the shuffle will be slower than the network capacity allows. The root cause is TCP slow start, not Spark configuration.

**Example 3 — TCP SYN queue exhaustion on Kafka brokers.** A Kafka broker under high client churn (many short-lived producers reconnecting) can exhaust its SYN backlog queue, causing new connections to fail with "Connection refused" even though the broker process is healthy. This is a Linux networking parameter (`net.core.somaxconn`, `net.ipv4.tcp_max_syn_backlog`) problem, not a Kafka problem. An engineer who doesn't know what a SYN queue is cannot find this root cause.

The goal of this module is to give you the mental model that makes these diagnoses possible: what IP does, what TCP adds on top, and how the combination creates observable, measurable behavior you can tune.

---

## 2. Mental Model

### The Network Stack as a Contract Tower

Think of the TCP/IP stack as a tower of contracts, where each layer makes promises to the layer above it while only caring about the layer below it.

```
Application Layer    (HTTP, gRPC, Kafka protocol, Spark shuffle protocol)
        │  "I want to send a stream of bytes to this address:port"
        ▼
Transport Layer      (TCP or UDP)
        │  TCP: "I will deliver every byte, in order, exactly once"
        │  UDP: "I will try; no promises"
        ▼
Internet Layer       (IP)
        │  "I will route your packet toward the destination; it might get lost"
        ▼
Link Layer           (Ethernet, WiFi)
        │  "I will transmit frames between directly connected devices"
        ▼
Physical Layer       (copper, fiber, radio)
```

The key insight is the **impedance mismatch between IP and TCP.** IP is a *best-effort* packet delivery service — it can drop, duplicate, or reorder packets with no notification. TCP sits on top and converts that unreliable service into a *reliable byte stream* by adding sequence numbers, acknowledgments, retransmissions, and flow control. That conversion is not free: TCP adds latency (the handshake), overhead (headers, ACKs), and behavioral constraints (congestion control slows you down when the network is stressed).

### IP Addressing: The Postal Address Analogy

An IP address is a hierarchical identifier, like a postal address. The network prefix (determined by the subnet mask) is the "city and street" — it tells routers how to route a packet toward the right network. The host part is the "building number" — it identifies the specific machine within that network.

**IPv4:** 32-bit address space, written as four decimal octets (e.g., `10.0.1.47`). With a `/24` mask, the first 24 bits (`10.0.1`) are the network prefix, the last 8 bits (`47`) are the host. A `/24` subnet contains 254 usable hosts.

**CIDR notation:** `10.0.0.0/16` means the first 16 bits are the network prefix, leaving 16 bits for hosts (65,534 usable). Cloud VPCs typically use `/16` for the VPC with `/24` subnets carved out of it.

**Private address ranges (RFC 1918):** `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`. These are never routed on the public internet. Cloud VPCs always use private addresses internally.

### TCP: A Reliable Stream Built on Unreliable Packets

TCP achieves reliability through three mechanisms working together:

**Sequence numbers and acknowledgments:** Every byte of data is numbered. The receiver tells the sender "I've received everything up to byte N, send me from N+1." If the sender doesn't receive an ACK within a timeout period, it retransmits.

**Flow control (receive window):** The receiver advertises how much buffer space it has available. The sender cannot have more unacknowledged data in flight than this window size. This prevents a fast sender from overwhelming a slow receiver.

**Congestion control:** The sender maintains a separate "congestion window" (cwnd) that starts small and grows. When the network shows signs of congestion (packet loss), the congestion window shrinks. This is what makes new TCP connections start slowly — the slow start phase.

The amount of data a TCP connection can have "in flight" at once is:

```
max_throughput = min(receive_window, congestion_window) / RTT
```

This formula is the single most important TCP performance concept. It explains: why high-latency connections are slow even on fast networks (small cwnd/RTT ratio), why you need large buffers on high-bandwidth links (need large receive window), and why TCP connection reuse matters (avoids slow start).

---

## 3. Core Concepts

### 3.1 The TCP Three-Way Handshake

Before any application data can flow, TCP requires a handshake to establish connection state at both ends:

```
Client                          Server
  │                               │
  │──── SYN (seq=x) ─────────────►│   Client sends SYN with random initial seq number
  │                               │
  │◄─── SYN-ACK (seq=y, ack=x+1)─│   Server responds with its own seq number + ACK
  │                               │
  │──── ACK (ack=y+1) ───────────►│   Client ACKs the server's SYN
  │                               │
  │◄══════ DATA FLOWS ════════════►│   Connection established; data can now flow
```

**Cost:** One full round trip before any application data can be sent. If the client is in us-east-1 and the server is in us-west-2 (RTT ~65 ms), the handshake alone costs 65 ms before a single byte of application data moves.

**TCP Fast Open (TFO):** A Linux extension that allows data to be sent with the SYN packet on repeat connections (after the first), eliminating the handshake latency for subsequent connections to the same server. Rarely used in practice in data systems.

**Implication for Kafka producers:** Every time a Kafka producer reconnects to a broker (after a restart, after a broker failover), it pays a full handshake RTT before it can send any data. Connection pools and long-lived connections avoid this cost.

### 3.2 TCP Connection Teardown

Closing a connection also requires a handshake. The four-way close:

```
Active Closer                  Passive Closer
      │                               │
      │──── FIN ─────────────────────►│   "I'm done sending"
      │◄─── ACK ─────────────────────│   "Got it"
      │                               │   (passive closer may still send data)
      │◄─── FIN ─────────────────────│   "I'm also done sending"
      │──── ACK ─────────────────────►│   "Got it"
      │                               │
      │  [TIME_WAIT: 2×MSL]           │   Active closer waits before releasing port
```

**TIME_WAIT:** After sending the final ACK, the active closer enters TIME_WAIT state for 2×MSL (Maximum Segment Lifetime, typically 60 seconds on Linux, so TIME_WAIT = 120 seconds). During this time, the port is not fully released. This prevents a delayed duplicate packet from a previous connection being misinterpreted by a new connection on the same 4-tuple (src_ip:src_port:dst_ip:dst_port).

**Implication for high-throughput systems:** A Kafka broker with thousands of short-lived producer connections accumulates thousands of TIME_WAIT sockets. This is visible in `ss -s` as a large TIME-WAIT count. It is mostly harmless but can exhaust the ephemeral port range (`net.ipv4.ip_local_port_range`) if connections are created and destroyed faster than TIME_WAIT expires. The fix is connection pooling, not tuning TIME_WAIT away.

### 3.3 TCP Sequence Numbers and Retransmission

TCP assigns a sequence number to each byte. The initial sequence number (ISN) is randomly chosen to prevent collision with stale packets from previous connections. As data is sent, sequence numbers increment by the number of bytes sent.

**ACK mechanics:** The receiver sends a cumulative ACK: "I've received all bytes up to sequence N." If the receiver gets packets out of order, it buffers them and waits for the gap to be filled before advancing the ACK.

**Retransmission timeout (RTO):** The sender starts a retransmission timer when it sends data. If the timer expires before receiving an ACK, it retransmits. RTO is computed using a smoothed RTT estimate plus a variance term (RFC 6298):

```
SRTT = 0.875 × SRTT + 0.125 × RTT_sample
RTTVAR = 0.75 × RTTVAR + 0.25 × |SRTT - RTT_sample|
RTO = SRTT + 4 × RTTVAR
```

The minimum RTO in Linux is 200 ms (`tcp_rto_min`). The first retransmission happens at 200 ms–1 s; each subsequent retransmission doubles (exponential backoff).

**Fast retransmit:** Rather than waiting for the RTO, TCP uses three duplicate ACKs as a signal of packet loss. If the sender receives three ACKs for the same sequence number (because the receiver is acknowledging the last in-order byte while newer bytes arrive out of order), it retransmits without waiting for timeout. This is much faster than RTO-based retransmission.

**Implication for data engineers:** A single packet loss on a large Spark shuffle transfer forces a retransmission. If the loss triggers RTO (not fast retransmit), the transfer stalls for 200 ms minimum. On a high-volume shuffle, this can add seconds to the stage duration. Monitoring for TCP retransmissions (`netstat -s | grep retransmit`) is part of network diagnosis for slow Spark jobs.

### 3.4 TCP Flow Control and the Receive Window

Each side advertises a receive window in TCP headers. This is the amount of data the receiver can buffer before it needs to be consumed by the application. If the application is slow to read from the socket, the buffer fills up, the advertised window shrinks to zero, and the sender stops transmitting ("zero window" condition).

**Zero window:** When the window drops to zero, the sender enters a state where it sends only periodic "window probe" packets to check if the window has reopened. This causes a visible pause in the transfer.

**Implication:** A Python consumer reading from Kafka with a slow processing loop can cause the broker's TCP window to fill, triggering zero-window probes. The symptom is low consumer throughput with no CPU pressure on the broker — the consumer is the bottleneck, not the broker.

**Window Scaling (RFC 1323):** The original TCP specification allows a maximum window of 65,535 bytes. On a 1 Gbps link with 1 ms RTT, the bandwidth-delay product is 125 KB — the original window is already too small. Window scaling extends the window to 1 GB using a scale factor negotiated during the handshake. Linux enables this by default (`net.ipv4.tcp_window_scaling = 1`).

### 3.5 TCP Congestion Control

Congestion control is the mechanism that prevents TCP senders from overwhelming the network. It operates via the congestion window (cwnd), which limits how much data can be in flight.

**Slow Start:** When a TCP connection is first established (or after a timeout), cwnd starts at approximately 10 MSS (Maximum Segment Size, typically 1460 bytes on Ethernet). For each ACK received, cwnd increases by 1 MSS. This doubles cwnd per RTT — exponential growth.

```
RTT 1: cwnd = 10 segments
RTT 2: cwnd = 20 segments
RTT 3: cwnd = 40 segments
...continues until ssthresh (slow start threshold) is reached
```

Once cwnd reaches ssthresh (initially a large value, set to half of cwnd when loss is detected), TCP switches to **Congestion Avoidance**: cwnd increases by 1 MSS per RTT (linear growth), not per ACK.

**Loss detection and response:**
- Three duplicate ACKs (fast retransmit): ssthresh = cwnd/2; cwnd = ssthresh (halved)
- Timeout (RTO): ssthresh = cwnd/2; cwnd = 1 MSS (restart slow start)

**Modern congestion control algorithms:**
- **CUBIC (Linux default):** Uses a cubic function for cwnd growth, behaving more aggressively at low utilization and backing off more gently. Better for high-bandwidth-delay paths.
- **BBR (Bottleneck Bandwidth and RTT):** Google's algorithm that models the actual bottleneck bandwidth rather than using packet loss as the primary signal. More efficient on paths with buffer bloat.

**Implication for data engineers:** A new TCP connection between two Kafka brokers starts in slow start. For a cross-AZ replication link with 1 ms RTT, reaching 1 MB/s throughput takes several RTTs of slow start ramp-up. This is why replication initially lags after a broker restart, then catches up rapidly. It is also why long-lived TCP connections between Kafka brokers are important — they have already ramped up their cwnd.

### 3.6 Bandwidth-Delay Product

The bandwidth-delay product (BDP) is the amount of data that can be "in flight" on a network path at full utilization:

```
BDP = Bandwidth × RTT
```

Examples:
- 1 Gbps link, 0.1 ms RTT (same rack): BDP = 1e9 × 0.0001 = 12.5 KB
- 1 Gbps link, 1 ms RTT (cross-AZ): BDP = 1e9 × 0.001 = 125 KB
- 1 Gbps link, 50 ms RTT (cross-region): BDP = 1e9 × 0.05 = 6.25 MB
- 10 Gbps link, 50 ms RTT (cross-region): BDP = 1e10 × 0.05 = 62.5 MB

For a TCP connection to fully utilize the available bandwidth, the receive window and congestion window must be at least as large as the BDP. On a cross-region 10 Gbps link, you need at least 62.5 MB of TCP buffer to saturate the link. The Linux default socket buffer (`net.core.rmem_default`) is typically 212 KB — far below what's needed for high-speed cross-region transfers.

**Implication for cross-region Kafka replication:** MirrorMaker 2 replicating across regions at 10 Gbps will be buffer-limited unless the OS socket buffers are tuned. The fix is increasing `net.core.rmem_max`, `net.core.wmem_max`, and the TCP buffer sizes (`net.ipv4.tcp_rmem`, `net.ipv4.tcp_wmem`).

### 3.7 NAT and Port Translation

Network Address Translation (NAT) rewrites the source IP (and port) of outgoing packets so that many internal hosts can share a single public IP. The NAT device maintains a mapping table:

```
Internal: 10.0.1.5:52341  → External: 203.0.113.1:12000 → Destination: 52.94.1.1:9092
```

**Stateful NAT:** The NAT device must maintain this mapping for the duration of the connection. NAT devices have a timeout — if no packets flow for N minutes, the mapping is deleted.

**Problem:** A long-lived Kafka connection that is idle (no data for several minutes) can have its NAT mapping silently deleted. The next packet from the producer will be dropped (no mapping for the destination to route the reply), and the producer will not discover the broken connection until it tries to send data and receives a connection reset or timeout.

**Fix:** TCP keepalives. If the OS sends TCP keepalive probes on idle connections (`tcp_keepalive_time`, `tcp_keepalive_intvl`, `tcp_keepalive_probes`), the NAT mapping is refreshed. Kafka clients configure this via `connections.max.idle.ms` and the OS keepalive settings.

### 3.8 IP Fragmentation and MTU

The Maximum Transmission Unit (MTU) is the largest IP packet that can traverse a network link without fragmentation. Standard Ethernet MTU is 1500 bytes. Jumbo frames extend this to 9000 bytes on networks configured to support them.

If an IP packet is larger than the MTU of a link along the path, either:
1. The router fragments the packet into smaller pieces (if the `DF` bit is not set)
2. The packet is dropped and an ICMP "fragmentation needed" message is sent back (if `DF` is set)

**Path MTU Discovery (PMTUD):** TCP sets the `DF` (Don't Fragment) bit on IP headers and relies on ICMP "fragmentation needed" messages to discover the smallest MTU on the path. If firewalls block ICMP (common), PMTUD fails and connections appear to hang after the handshake when data transmission begins — a symptom called "black hole detection failure."

**MSS (Maximum Segment Size):** During the TCP handshake, each side advertises its MSS — the largest TCP payload it will accept. MSS is typically MTU - 40 bytes (20 IP header + 20 TCP header) = 1460 bytes for standard Ethernet.

**Jumbo frames in data centers:** Data center networks between Kafka brokers and Spark clusters often use jumbo frames (MTU 9000). This reduces packet header overhead for large data transfers. But jumbo frames only work if *every* link on the path supports them — a single standard-MTU link causes either fragmentation or PMTUD failures.

---

## 4. Hands-On Walkthrough

### 4.1 Observing the TCP Handshake with `tcpdump`

```bash
# Terminal 1: capture traffic on lo (loopback) for port 9092
sudo tcpdump -i lo -n 'tcp port 9092' -S

# Terminal 2: connect to a Kafka broker
nc -z localhost 9092

# Expected output in Terminal 1:
# 14:23:01.123456 IP 127.0.0.1.52341 > 127.0.0.1.9092: Flags [S], seq 1234567890
# 14:23:01.123501 IP 127.0.0.1.9092 > 127.0.0.1.52341: Flags [S.], seq 987654321, ack 1234567891
# 14:23:01.123512 IP 127.0.0.1.52341 > 127.0.0.1.9092: Flags [.], ack 987654322
```

**Flag meanings in `tcpdump` output:**
- `[S]` = SYN
- `[S.]` = SYN-ACK (the `.` means ACK flag is also set)
- `[.]` = ACK only
- `[P.]` = PSH + ACK (data push)
- `[F.]` = FIN + ACK
- `[R.]` = RST + ACK (connection reset)

### 4.2 Checking Socket State with `ss`

```bash
# Show all TCP connections with process info
ss -tnp

# Show connection states summary
ss -s

# Show connections to a specific port (Kafka broker)
ss -tnp dst :9092

# Show listening sockets with backlog info
ss -tlnp

# Example output of ss -s:
# Total: 892
# TCP:   634 (estab 421, closed 187, orphaned 0, timewait 156)
#
# Transport Total     IP        IPv6
# RAW       0         0         0
# UDP       8         6         2
# TCP       634       612       22
# INET      642       618       24
# FRAG      0         0         0
```

**Key state counts to watch:**
- `estab` (ESTABLISHED): active connections
- `timewait` (TIME-WAIT): connections waiting to fully close — high count means many short-lived connections
- `closed` (includes SYN_SENT, SYN_RECV, CLOSE_WAIT, etc.): connections in transition states

### 4.3 Observing TCP Congestion Window with `ss`

```bash
# Show TCP socket internals including cwnd and RTT
ss -tipm dst :9092

# Example output:
#  State    Recv-Q Send-Q Local Address:Port  Peer Address:Port
#  ESTAB    0      0      10.0.1.5:52341      10.0.1.10:9092
#    cubic wscale:7,7 rto:204 rtt:0.148/0.04 ato:40 mss:1448 pmtu:1500
#    rcvmss:1448 advmss:1448 cwnd:10 ssthresh:2147483647
#    bytes_sent:2048 bytes_acked:2048 bytes_received:512
#    segs_out:4 segs_in:3 data_segs_out:2 data_segs_in:1
#    send 782Mbps lastsnd:204 lastrcv:204 lastack:204
#    pacing_rate 941Mbps delivery_rate 782Mbps
#    delivered:3 busy:0ms
```

**Important fields:**
- `rtt:0.148/0.04` — smoothed RTT / variance in ms
- `cwnd:10` — congestion window in MSS units (10 × 1448 = 14.5 KB currently in flight)
- `ssthresh:2147483647` — slow start threshold (max = not yet triggered congestion)
- `send 782Mbps` — current estimated send rate

A connection that has been idle then resumes will often show `cwnd` has been reset to a small value — TCP Idle Restart, controlled by `net.ipv4.tcp_slow_start_after_idle`.

### 4.4 Diagnosing Retransmissions

```bash
# Global retransmission statistics
netstat -s | grep -i retran

# Example output:
#     1234 segments retransmitted
#     0 fast retransmits
#     5 retransmits in slow start

# Per-connection retransmit count via ss
ss -tipm | grep retrans

# Watch retransmissions in real time (1-second delta)
watch -n1 'netstat -s | grep retran'

# More detail with nstat (per-second deltas)
nstat -az | grep -i retran
```

A rising `TcpRetransSegs` counter is the primary signal of packet loss between your data nodes. On well-functioning internal networks, this should be near zero.

### 4.5 Checking TCP Buffer Tuning

```bash
# Current buffer settings
sysctl net.core.rmem_default net.core.rmem_max
sysctl net.core.wmem_default net.core.wmem_max
sysctl net.ipv4.tcp_rmem net.ipv4.tcp_wmem

# Example output:
# net.core.rmem_max = 16777216        (16 MB max receive buffer)
# net.ipv4.tcp_rmem = 4096 87380 16777216
#   ^min    ^default  ^max

# Calculate required buffer for 1 Gbps cross-region (50ms RTT):
# BDP = 1e9 bps × 0.050 s / 8 bits = 6.25 MB
# Need tcp_rmem max >= 6.25 MB -- 16 MB is sufficient

# Apply tuning (temporary):
sysctl -w net.core.rmem_max=134217728   # 128 MB
sysctl -w net.core.wmem_max=134217728
sysctl -w net.ipv4.tcp_rmem='4096 87380 134217728'
sysctl -w net.ipv4.tcp_wmem='4096 65536 134217728'

# Persist in /etc/sysctl.d/99-network-tuning.conf
```

### 4.6 Checking MTU and PMTUD

```bash
# Check interface MTU
ip link show eth0
# Example: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 9000

# Test PMTU manually (DF bit set, vary packet size)
ping -M do -s 8972 10.0.1.10
# 8972 bytes data + 28 bytes IP/ICMP header = 9000 byte packet
# If MTU is 9000 everywhere: response received
# If a link has MTU 1500: "Frag needed" or timeout (if ICMP blocked)

# Check actual path MTU (uses PMTUD algorithm)
tracepath 10.0.1.10
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
tcp_diagnostics.py

Production TCP/IP diagnostic toolkit for data engineers.
Reads from /proc and /sys — no external dependencies.

Usage:
    python tcp_diagnostics.py
    python tcp_diagnostics.py --watch 5   # refresh every 5 seconds
"""

import os
import re
import time
import socket
import struct
import argparse
from dataclasses import dataclass, field
from typing import Optional


# ─── Data Structures ───────────────────────────────────────────────────────────

@dataclass
class TCPStats:
    """Global TCP statistics from /proc/net/snmp and /proc/net/netstat."""
    # Connection stats
    active_opens: int = 0        # Total TCP connections initiated by us
    passive_opens: int = 0       # Total TCP connections accepted
    curr_estab: int = 0          # Currently ESTABLISHED connections
    # Error stats
    retrans_segs: int = 0        # Total retransmitted segments (cumulative)
    in_errs: int = 0             # Bad TCP packets received
    out_rsts: int = 0            # RST segments sent
    # Performance stats
    in_segs: int = 0             # Total segments received
    out_segs: int = 0            # Total segments sent
    # Snapshot time for delta computation
    snapshot_ts: float = field(default_factory=time.time)


@dataclass
class TCPStateDelta:
    """Change in TCP statistics between two snapshots."""
    retrans_per_sec: float = 0.0
    new_connections_per_sec: float = 0.0
    segments_in_per_sec: float = 0.0
    segments_out_per_sec: float = 0.0
    elapsed_s: float = 0.0


@dataclass
class SocketStateCount:
    """Count of TCP sockets in each state."""
    established: int = 0
    syn_sent: int = 0
    syn_recv: int = 0
    fin_wait1: int = 0
    fin_wait2: int = 0
    time_wait: int = 0
    close: int = 0
    close_wait: int = 0
    last_ack: int = 0
    listen: int = 0
    closing: int = 0


@dataclass
class NetworkInterface:
    """Statistics for a single network interface from /proc/net/dev."""
    name: str
    rx_bytes: int = 0
    rx_packets: int = 0
    rx_errors: int = 0
    rx_dropped: int = 0
    tx_bytes: int = 0
    tx_packets: int = 0
    tx_errors: int = 0
    tx_dropped: int = 0
    snapshot_ts: float = field(default_factory=time.time)


@dataclass
class InterfaceDelta:
    """Per-second rates for a network interface."""
    name: str
    rx_mbps: float = 0.0
    tx_mbps: float = 0.0
    rx_kpps: float = 0.0       # kilo-packets per second
    tx_kpps: float = 0.0
    rx_errors_per_sec: float = 0.0
    tx_errors_per_sec: float = 0.0
    elapsed_s: float = 0.0


@dataclass
class TCPBufferConfig:
    """Current TCP buffer configuration from sysctl."""
    rmem_min: int = 0
    rmem_default: int = 0
    rmem_max: int = 0
    wmem_min: int = 0
    wmem_default: int = 0
    wmem_max: int = 0
    core_rmem_default: int = 0
    core_rmem_max: int = 0
    core_wmem_default: int = 0
    core_wmem_max: int = 0
    window_scaling: int = 0
    slow_start_after_idle: int = 0


# ─── /proc Readers ─────────────────────────────────────────────────────────────

def parse_tcp_stats() -> TCPStats:
    """
    Read cumulative TCP statistics from /proc/net/snmp.
    Returns a TCPStats snapshot for delta computation.
    """
    stats = TCPStats()
    try:
        with open('/proc/net/snmp', 'r') as f:
            lines = f.readlines()

        # Format: header line followed by value line, repeated for each protocol
        for i, line in enumerate(lines):
            if line.startswith('Tcp:') and i + 1 < len(lines):
                headers = line.split()
                values = lines[i + 1].split()
                if values[0] == 'Tcp:':
                    tcp_map = dict(zip(headers[1:], values[1:]))
                    stats.active_opens = int(tcp_map.get('ActiveOpens', 0))
                    stats.passive_opens = int(tcp_map.get('PassiveOpens', 0))
                    stats.curr_estab = int(tcp_map.get('CurrEstab', 0))
                    stats.retrans_segs = int(tcp_map.get('RetransSegs', 0))
                    stats.in_errs = int(tcp_map.get('InErrs', 0))
                    stats.out_rsts = int(tcp_map.get('OutRsts', 0))
                    stats.in_segs = int(tcp_map.get('InSegs', 0))
                    stats.out_segs = int(tcp_map.get('OutSegs', 0))
                    break
    except (IOError, KeyError, ValueError):
        pass
    stats.snapshot_ts = time.time()
    return stats


def compute_tcp_delta(before: TCPStats, after: TCPStats) -> TCPStateDelta:
    """Compute per-second rates from two TCPStats snapshots."""
    elapsed = after.snapshot_ts - before.snapshot_ts
    if elapsed <= 0:
        return TCPStateDelta()
    return TCPStateDelta(
        retrans_per_sec=(after.retrans_segs - before.retrans_segs) / elapsed,
        new_connections_per_sec=(after.active_opens - before.active_opens) / elapsed,
        segments_in_per_sec=(after.in_segs - before.in_segs) / elapsed,
        segments_out_per_sec=(after.out_segs - before.out_segs) / elapsed,
        elapsed_s=elapsed,
    )


def parse_socket_states() -> SocketStateCount:
    """
    Count TCP sockets in each state from /proc/net/tcp and /proc/net/tcp6.

    State codes in /proc/net/tcp (hex):
      01=ESTABLISHED 02=SYN_SENT 03=SYN_RECV 04=FIN_WAIT1 05=FIN_WAIT2
      06=TIME_WAIT   07=CLOSE    08=CLOSE_WAIT 09=LAST_ACK 0A=LISTEN 0B=CLOSING
    """
    state_map = {
        '01': 'established', '02': 'syn_sent', '03': 'syn_recv',
        '04': 'fin_wait1', '05': 'fin_wait2', '06': 'time_wait',
        '07': 'close', '08': 'close_wait', '09': 'last_ack',
        '0A': 'listen', '0B': 'closing',
    }
    counts = SocketStateCount()

    for path in ('/proc/net/tcp', '/proc/net/tcp6'):
        try:
            with open(path, 'r') as f:
                for line in f.readlines()[1:]:  # skip header
                    parts = line.split()
                    if len(parts) < 4:
                        continue
                    state_hex = parts[3].upper()
                    attr = state_map.get(state_hex)
                    if attr:
                        setattr(counts, attr, getattr(counts, attr) + 1)
        except IOError:
            pass

    return counts


def parse_network_interfaces() -> dict[str, NetworkInterface]:
    """
    Read interface statistics from /proc/net/dev.
    Returns dict of interface name → NetworkInterface.
    """
    interfaces = {}
    try:
        with open('/proc/net/dev', 'r') as f:
            lines = f.readlines()[2:]  # skip two header lines

        for line in lines:
            parts = line.split()
            if len(parts) < 17:
                continue
            name = parts[0].rstrip(':')
            iface = NetworkInterface(
                name=name,
                rx_bytes=int(parts[1]),
                rx_packets=int(parts[2]),
                rx_errors=int(parts[3]),
                rx_dropped=int(parts[4]),
                tx_bytes=int(parts[9]),
                tx_packets=int(parts[10]),
                tx_errors=int(parts[11]),
                tx_dropped=int(parts[12]),
                snapshot_ts=time.time(),
            )
            interfaces[name] = iface
    except (IOError, ValueError):
        pass
    return interfaces


def compute_interface_delta(
    before: NetworkInterface, after: NetworkInterface
) -> InterfaceDelta:
    """Compute per-second rates for a network interface."""
    elapsed = after.snapshot_ts - before.snapshot_ts
    if elapsed <= 0:
        return InterfaceDelta(name=before.name)

    rx_bytes_per_s = (after.rx_bytes - before.rx_bytes) / elapsed
    tx_bytes_per_s = (after.tx_bytes - before.tx_bytes) / elapsed
    rx_pkts_per_s = (after.rx_packets - before.rx_packets) / elapsed
    tx_pkts_per_s = (after.tx_packets - before.tx_packets) / elapsed

    return InterfaceDelta(
        name=before.name,
        rx_mbps=rx_bytes_per_s * 8 / 1e6,
        tx_mbps=tx_bytes_per_s * 8 / 1e6,
        rx_kpps=rx_pkts_per_s / 1000,
        tx_kpps=tx_pkts_per_s / 1000,
        rx_errors_per_sec=(after.rx_errors - before.rx_errors) / elapsed,
        tx_errors_per_sec=(after.tx_errors - before.tx_errors) / elapsed,
        elapsed_s=elapsed,
    )


def read_tcp_buffer_config() -> TCPBufferConfig:
    """Read current TCP buffer configuration from /proc/sys/net."""
    def read_sysctl(path: str) -> str:
        try:
            with open(path) as f:
                return f.read().strip()
        except IOError:
            return '0'

    def parse_triple(s: str) -> tuple[int, int, int]:
        parts = s.split()
        if len(parts) == 3:
            return int(parts[0]), int(parts[1]), int(parts[2])
        return 0, 0, 0

    rmem = parse_triple(read_sysctl('/proc/sys/net/ipv4/tcp_rmem'))
    wmem = parse_triple(read_sysctl('/proc/sys/net/ipv4/tcp_wmem'))

    return TCPBufferConfig(
        rmem_min=rmem[0], rmem_default=rmem[1], rmem_max=rmem[2],
        wmem_min=wmem[0], wmem_default=wmem[1], wmem_max=wmem[2],
        core_rmem_default=int(read_sysctl('/proc/sys/net/core/rmem_default')),
        core_rmem_max=int(read_sysctl('/proc/sys/net/core/rmem_max')),
        core_wmem_default=int(read_sysctl('/proc/sys/net/core/wmem_default')),
        core_wmem_max=int(read_sysctl('/proc/sys/net/core/wmem_max')),
        window_scaling=int(read_sysctl('/proc/sys/net/ipv4/tcp_window_scaling')),
        slow_start_after_idle=int(
            read_sysctl('/proc/sys/net/ipv4/tcp_slow_start_after_idle')
        ),
    )


# ─── Analysis Functions ────────────────────────────────────────────────────────

def compute_bdp_bytes(bandwidth_gbps: float, rtt_ms: float) -> int:
    """
    Compute the bandwidth-delay product for a given link.
    Returns minimum TCP buffer size needed to saturate the link.
    """
    bandwidth_bps = bandwidth_gbps * 1e9
    rtt_s = rtt_ms / 1000
    return int(bandwidth_bps * rtt_s / 8)  # convert bits to bytes


def assess_buffer_config(config: TCPBufferConfig, bandwidth_gbps: float = 1.0,
                          rtt_ms: float = 1.0) -> list[str]:
    """
    Assess current buffer configuration against the bandwidth-delay product
    and return a list of warnings.
    """
    warnings = []
    bdp = compute_bdp_bytes(bandwidth_gbps, rtt_ms)

    if config.rmem_max < bdp:
        warnings.append(
            f"tcp_rmem max ({config.rmem_max // 1024}KB) < BDP "
            f"({bdp // 1024}KB) for {bandwidth_gbps}Gbps / {rtt_ms}ms RTT. "
            f"Cross-region transfers will be buffer-limited."
        )
    if config.core_rmem_max < bdp:
        warnings.append(
            f"net.core.rmem_max ({config.core_rmem_max // 1024}KB) < BDP "
            f"({bdp // 1024}KB). Increase to at least {bdp * 2 // 1024}KB."
        )
    if config.window_scaling == 0:
        warnings.append(
            "tcp_window_scaling is disabled. Max window capped at 65535 bytes. "
            "STRONGLY recommended to enable."
        )
    if config.slow_start_after_idle == 1:
        warnings.append(
            "tcp_slow_start_after_idle=1: idle connections restart slow start. "
            "For Kafka long-lived connections, set to 0 to maintain cwnd."
        )
    return warnings


def sample_tcp_stats(interval_s: float = 1.0) -> TCPStateDelta:
    """Take two snapshots separated by interval_s and return per-second rates."""
    before = parse_tcp_stats()
    time.sleep(interval_s)
    after = parse_tcp_stats()
    return compute_tcp_delta(before, after)


def sample_interface_rates(
    interfaces: Optional[list[str]] = None, interval_s: float = 1.0
) -> list[InterfaceDelta]:
    """
    Sample all (or specified) network interfaces and return per-second rates.
    Skips loopback by default if interfaces is None.
    """
    before = parse_network_interfaces()
    time.sleep(interval_s)
    after = parse_network_interfaces()

    results = []
    for name, before_iface in before.items():
        if interfaces and name not in interfaces:
            continue
        if interfaces is None and name == 'lo':
            continue
        if name in after:
            delta = compute_interface_delta(before_iface, after[name])
            results.append(delta)
    return sorted(results, key=lambda d: d.rx_mbps + d.tx_mbps, reverse=True)


# ─── Report ────────────────────────────────────────────────────────────────────

def tcp_health_report(sample_s: float = 2.0) -> None:
    """Print a full TCP/IP health report."""
    print("=" * 70)
    print("TCP/IP HEALTH REPORT")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # --- Socket state counts
    print("\n[1] TCP Socket States")
    states = parse_socket_states()
    print(f"  ESTABLISHED : {states.established:>6}")
    print(f"  TIME_WAIT   : {states.time_wait:>6}", end="")
    if states.time_wait > 10000:
        print("  ⚠️  HIGH — check for connection churn", end="")
    print()
    print(f"  CLOSE_WAIT  : {states.close_wait:>6}", end="")
    if states.close_wait > 100:
        print("  ⚠️  HIGH — possible connection leak (app not closing)", end="")
    print()
    print(f"  SYN_RECV    : {states.syn_recv:>6}", end="")
    if states.syn_recv > 500:
        print("  ⚠️  HIGH — possible SYN flood or SYN backlog exhaustion", end="")
    print()
    print(f"  LISTEN      : {states.listen:>6}")
    print(f"  FIN_WAIT2   : {states.fin_wait2:>6}")

    # --- Per-second TCP stats
    print(f"\n[2] TCP Traffic (sampling {sample_s}s)...")
    delta = sample_tcp_stats(sample_s)
    print(f"  Retransmissions/s : {delta.retrans_per_sec:>8.2f}", end="")
    if delta.retrans_per_sec > 10:
        print("  ⚠️  ELEVATED — check for packet loss", end="")
    elif delta.retrans_per_sec > 0:
        print("  (normal background)", end="")
    else:
        print("  ✓ clean", end="")
    print()
    print(f"  New connections/s : {delta.new_connections_per_sec:>8.2f}")
    print(f"  Segments in/s     : {delta.segments_in_per_sec:>8.0f}")
    print(f"  Segments out/s    : {delta.segments_out_per_sec:>8.0f}")

    # --- Interface rates
    print(f"\n[3] Interface Throughput ({sample_s}s sample)")
    iface_deltas = sample_interface_rates(interval_s=sample_s)
    if not iface_deltas:
        print("  (no non-loopback interfaces found)")
    for d in iface_deltas:
        print(f"  {d.name:<12}  RX: {d.rx_mbps:>8.1f} Mbps  TX: {d.tx_mbps:>8.1f} Mbps", end="")
        if d.rx_errors_per_sec > 0 or d.tx_errors_per_sec > 0:
            print(f"  ⚠️  errors: rx={d.rx_errors_per_sec:.1f}/s tx={d.tx_errors_per_sec:.1f}/s", end="")
        print()

    # --- Buffer config
    print("\n[4] TCP Buffer Configuration")
    cfg = read_tcp_buffer_config()
    print(f"  tcp_rmem (min/default/max): "
          f"{cfg.rmem_min//1024}KB / {cfg.rmem_default//1024}KB / {cfg.rmem_max//1024}KB")
    print(f"  tcp_wmem (min/default/max): "
          f"{cfg.wmem_min//1024}KB / {cfg.wmem_default//1024}KB / {cfg.wmem_max//1024}KB")
    print(f"  core rmem_max: {cfg.core_rmem_max//1024//1024}MB")
    print(f"  core wmem_max: {cfg.core_wmem_max//1024//1024}MB")
    print(f"  window_scaling: {cfg.window_scaling}  (1=enabled, required)")
    print(f"  slow_start_after_idle: {cfg.slow_start_after_idle}  (0=keep cwnd on idle, 1=reset)")

    print("\n[5] Buffer Adequacy Check (1Gbps / 1ms RTT — same-AZ)")
    warnings_1ms = assess_buffer_config(cfg, bandwidth_gbps=1.0, rtt_ms=1.0)
    print("\n[5b] Buffer Adequacy Check (10Gbps / 50ms RTT — cross-region)")
    warnings_50ms = assess_buffer_config(cfg, bandwidth_gbps=10.0, rtt_ms=50.0)
    all_warnings = warnings_1ms + warnings_50ms
    if all_warnings:
        for w in all_warnings:
            print(f"  ⚠️  {w}")
    else:
        print("  ✓ Buffers sufficient for checked scenarios")

    print("\n[6] BDP Reference Table")
    print(f"  {'Scenario':<35}  {'BDP':>10}  {'Min Buffer':>12}")
    print(f"  {'-'*35}  {'-'*10}  {'-'*12}")
    scenarios = [
        ("Same rack (0.1ms RTT, 10Gbps)", 10.0, 0.1),
        ("Same DC (0.5ms RTT, 10Gbps)", 10.0, 0.5),
        ("Cross-AZ (1ms RTT, 1Gbps)", 1.0, 1.0),
        ("Cross-AZ (1ms RTT, 10Gbps)", 10.0, 1.0),
        ("Cross-region (50ms RTT, 1Gbps)", 1.0, 50.0),
        ("Cross-region (50ms RTT, 10Gbps)", 10.0, 50.0),
    ]
    for label, bw, rtt in scenarios:
        bdp = compute_bdp_bytes(bw, rtt)
        print(f"  {label:<35}  {bdp//1024:>8}KB  {bdp*2//1024:>10}KB")

    print()
    print("=" * 70)


# ─── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TCP/IP diagnostic toolkit")
    parser.add_argument('--watch', type=float, default=0,
                        help='Repeat every N seconds (0 = run once)')
    parser.add_argument('--sample', type=float, default=2.0,
                        help='Sampling interval in seconds (default: 2.0)')
    args = parser.parse_args()

    if args.watch > 0:
        while True:
            os.system('clear')
            tcp_health_report(sample_s=args.sample)
            time.sleep(args.watch)
    else:
        tcp_health_report(sample_s=args.sample)
```

---

## 6. Visual Reference

### TCP Three-Way Handshake Timeline

```
Client               Network              Server
  │                                          │
  │──── SYN ────────────────────────────────►│  t=0ms
  │                                          │
  │                                          │  [Server allocates TCB,
  │                                          │   generates ISN, sends SYN-ACK]
  │
  │◄─── SYN-ACK ────────────────────────────│  t=RTT/2 (arrives at client)
  │
  │──── ACK + data ─────────────────────────►│  t=RTT  (client sends ACK)
  │                                          │
  │◄═══ Response ════════════════════════════│  t=RTT + server_processing
  │
  ● One full RTT before first data byte received
```

### TCP Congestion Window Growth

```
cwnd
(segments)
    │
 64 │                                    ████
 48 │                               ████
 32 │                          ████
 16 │                   ████████
 10 │         ██████████                      ← Slow start to ssthresh
  5 │    ████                                 ← ssthresh reached; congestion avoidance
  1 │████                                     ← packet loss; timeout restart
    └─────────────────────────────────────────── RTT
      1  2  3  4  5  6  7  8  9  10  11  12
```

### Bandwidth-Delay Product

```
Pipe model: BDP = bandwidth × RTT
┌─────────────────────────────────────────────┐
│ Cross-region pipe (1Gbps, 50ms RTT)         │
│ BDP = 1e9 bps × 0.05s / 8 = 6.25 MB        │
│                                             │
│  ┌──6.25 MB of data "in flight"──────────┐  │
│  │  ████████████████████████████████████ │  │
│  └────────────────────────────────────── ┘  │
│                                             │
│ If TCP window < 6.25 MB, link is UNDERUSED  │
└─────────────────────────────────────────────┘
```

### IP Addressing: CIDR Notation

```
Address: 10.0.1.47/24

Binary:  00001010.00000000.00000001.00101111
Mask:    11111111.11111111.11111111.00000000
         ────────────────────────── ────────
         Network prefix (24 bits)   Host (8 bits)

Network: 10.0.1.0
Hosts:   10.0.1.1 – 10.0.1.254  (254 addresses)
Broadcast: 10.0.1.255
```

---

## 7. Common Mistakes

**Mistake 1: Treating the network as a transparent pipe.** Engineers write "the transfer should take X seconds at Y Gbps" without accounting for TCP handshake overhead, slow start, or the bandwidth-delay product. A single large file transfer on a new TCP connection will spend the first several RTTs in slow start, achieving a fraction of the link capacity.

**Mistake 2: Ignoring TIME_WAIT.** Seeing thousands of TIME_WAIT connections in `ss -s` and treating it as a bug. TIME_WAIT is normal and intentional — it prevents stale packets from being misinterpreted. The real problem it indicates is connection churn: too many short-lived connections where connection pooling would be better.

**Mistake 3: Disabling TIME_WAIT with `tcp_tw_reuse` or `tcp_tw_recycle`.** `tcp_tw_recycle` is removed in Linux 4.12 because it breaks connections from NATted clients (multiple clients behind one NAT share one IP, and the timestamp-based optimization incorrectly rejects packets). `tcp_tw_reuse` is safer but still requires careful understanding. The right fix for connection exhaustion is connection pooling.

**Mistake 4: Confusing CLOSE_WAIT with TIME_WAIT.** TIME_WAIT is the active closer's wait state — it's normal. CLOSE_WAIT means the application has received FIN but has not called close() on the socket — it's a bug, specifically a resource leak in the application. A rising CLOSE_WAIT count means your application is not properly closing connections.

**Mistake 5: Assuming packet loss is zero in a data center.** Internal data center networks do drop packets, especially under congestion. A 1 Gbps Kafka topic running at 900 Mbps sustained on a 1 Gbps NIC will occasionally drop packets in the NIC TX queue. Monitoring `netstat -s | grep retran` should be standard practice on Kafka and Spark nodes.

**Mistake 6: Not accounting for TCP slow start after idle in Kafka.** If `net.ipv4.tcp_slow_start_after_idle=1` (Linux default), a Kafka broker connection that is idle for a few seconds will restart slow start when the next batch is sent. This causes periodic throughput dips. Set it to `0` for Kafka brokers.

---

## 8. Production Failure Scenarios

### Scenario 1: Kafka Producer "Connection Refused" Under Load

**Symptoms:** Kafka producers intermittently fail with `Connection refused` to a healthy broker. The broker process is running and accepting connections when tested manually. The failure rate correlates with producer reconnect frequency.

**Root cause:** SYN backlog exhaustion. When a client sends a SYN, the kernel places it in the SYN queue while waiting for the ACK. The queue has a fixed size (`net.ipv4.tcp_max_syn_backlog`, default 1024). Under high client churn (many producers restarting, autoscaling Spark cluster connecting and disconnecting), the SYN queue fills and new connections are dropped before the application even sees them. The kernel logs: `TCP: request_sock_TCP: Possible SYN flooding on port 9092. Sending cookies.`

**Fix:**
```bash
# Increase SYN backlog
sysctl -w net.ipv4.tcp_max_syn_backlog=8192

# Increase the listen() backlog (application accept queue)
sysctl -w net.core.somaxconn=8192

# Also increase the listen backlog in Kafka server.properties:
# socket.listen.backlog.size=8192

# Persist:
echo "net.ipv4.tcp_max_syn_backlog=8192" >> /etc/sysctl.d/99-kafka.conf
echo "net.core.somaxconn=8192" >> /etc/sysctl.d/99-kafka.conf
```

### Scenario 2: MirrorMaker 2 Cross-Region Replication Plateau

**Symptoms:** MirrorMaker 2 replicating from us-east-1 to eu-west-1 (RTT ~80 ms) tops out at ~120 Mbps on a 10 Gbps link. Expected throughput is much higher.

**Diagnosis:**
```bash
# Check current buffer limits
sysctl net.ipv4.tcp_rmem net.core.rmem_max

# Compute what's needed:
# BDP = 10 Gbps × 80ms RTT = 10e9 × 0.08 / 8 = 100 MB
# Current rmem_max = 16 MB → buffer-limited
# 120 Mbps = 16 MB / 0.08s / 8 × 8 = 16 MB / RTT — confirms buffer limit
```

**Fix:**
```bash
sysctl -w net.core.rmem_max=268435456       # 256 MB
sysctl -w net.core.wmem_max=268435456
sysctl -w net.ipv4.tcp_rmem='4096 87380 268435456'
sysctl -w net.ipv4.tcp_wmem='4096 65536 268435456'
sysctl -w net.ipv4.tcp_slow_start_after_idle=0
```

After tuning, throughput will increase until it hits the actual bottleneck (CPU, disk, or network capacity).

### Scenario 3: Spark Shuffle Slowness After Executor Restart

**Symptoms:** After a Spark Executor restarts (due to OOM or node failure), the shuffle read phase for the stage that reads from that executor is significantly slower than normal — often 3–5× longer for the first minute after restart.

**Root cause:** TCP slow start. When the restarted Executor opens new TCP connections to fetch shuffle blocks from other executors, each new connection starts in slow start. A 10 Gbps link with 0.5 ms RTT needs cwnd to reach ~80 MSS to saturate the link. With slow start doubling per RTT, this takes ~7 RTTs = ~3.5 ms. But if `tcp_slow_start_after_idle=1`, any connection that is briefly idle between block requests will restart slow start. The cumulative effect across hundreds of shuffle connections is significant throughput reduction.

**Fix:** `sysctl -w net.ipv4.tcp_slow_start_after_idle=0` on all Spark nodes. Also ensure `spark.shuffle.io.numConnectionsPerPeer` is set high enough (default 1; set to 4–8 for large clusters) so there are pre-warmed connections available.

### Scenario 4: Silent Connection Drop Behind NAT (Cloud Kafka)

**Symptoms:** Kafka consumers running in Kubernetes (behind a NAT/load balancer) periodically stop receiving messages for 2–5 minutes before resuming. No error is logged by the consumer. The broker shows the consumer as active. The consumer's `records-lag` metric rises during the dead period.

**Root cause:** The cloud NAT gateway has a TCP idle timeout (typically 5 minutes for established connections). A Kafka consumer that is caught up and has no records to consume sits idle. After 5 minutes of silence, the NAT mapping is deleted. The next fetch request from the consumer is dropped; the broker never receives it and never sends a response. The consumer waits for `request.timeout.ms`, then reconnects. The total outage is `request.timeout.ms` (default 30 seconds) plus reconnect time.

**Fix:**
```bash
# Enable TCP keepalives at the OS level on consumer nodes
sysctl -w net.ipv4.tcp_keepalive_time=60        # start keepalives after 60s idle
sysctl -w net.ipv4.tcp_keepalive_intvl=10       # probe every 10s
sysctl -w net.ipv4.tcp_keepalive_probes=9       # give up after 9 failed probes

# In Kafka consumer config:
# socket.keepalive.enable=true   (enables SO_KEEPALIVE on the socket)
```

With keepalives every 60 seconds, the NAT mapping is refreshed before the 5-minute NAT timeout, preventing silent connection drops.

---

## 9. Performance and Tuning

### Kernel Parameters for Data Engineering Workloads

**For Kafka brokers (same-DC replication, high connection count):**
```bash
# /etc/sysctl.d/99-kafka-network.conf

# Increase socket backlogs for high connection count
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535

# Increase ephemeral port range for many outgoing connections
net.ipv4.ip_local_port_range = 1024 65535

# TCP keepalive: refresh NAT mappings, detect dead clients
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6

# Do not restart slow start on idle connections (maintains cwnd)
net.ipv4.tcp_slow_start_after_idle = 0

# Socket memory: default buffers adequate for same-DC
# (BDP for 1Gbps / 1ms = 125KB; default 212KB is sufficient)
net.core.rmem_default = 212992
net.core.rmem_max = 16777216      # 16 MB sufficient for same-DC 10Gbps / 1ms

# TCP TIME_WAIT: allow socket reuse for outgoing connections only
net.ipv4.tcp_tw_reuse = 1         # safe for outgoing connections; avoid for servers
```

**For cross-region replication (MirrorMaker 2, cross-DC Kafka):**
```bash
# /etc/sysctl.d/99-crossregion-network.conf

# Large buffers for high-BDP paths
# BDP for 10Gbps / 50ms = 62.5 MB; set to 256 MB for headroom
net.core.rmem_max = 268435456
net.core.wmem_max = 268435456
net.ipv4.tcp_rmem = 4096 87380 268435456
net.ipv4.tcp_wmem = 4096 65536 268435456

# Ensure window scaling is on
net.ipv4.tcp_window_scaling = 1

# BBR congestion control for high-BDP, buffer-bloat environments
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr

net.ipv4.tcp_slow_start_after_idle = 0
```

**For Spark shuffle (many parallel connections, short-lived):**
```bash
# Connection recycling: avoid ephemeral port exhaustion
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1

# Moderate buffer sizes (shuffle connections are short-lived and local)
net.core.rmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216

net.ipv4.tcp_slow_start_after_idle = 0
```

### Connection Pooling

The most impactful "tuning" for TCP performance in data engineering is avoiding the creation of new TCP connections in the first place. Each new connection pays: handshake latency (1 RTT), slow start warmup (several RTTs to reach full bandwidth), and a new port (contributes to TIME_WAIT accumulation).

Connection pooling in data engineering contexts:
- **Kafka producers/consumers:** Kafka clients maintain a single long-lived connection per broker. Do not create a new `KafkaProducer` per event.
- **Spark:** Executor-to-executor shuffle connections are reused within a shuffle stage. The `spark.shuffle.io.numConnectionsPerPeer` setting controls how many parallel connections are used.
- **JDBC:** Always use a connection pool (HikariCP, c3p0). Each JDBC connection is a TCP connection. Creating one per query is catastrophic at scale.

---

## 10. Interview Q&A

**Q1: A Kafka producer with `acks=all` has a P99 latency of 8 ms. You know the same producer with `acks=1` has a P99 of 1 ms. There are three brokers, all in the same availability zone. What explains the 8 ms, and how would you investigate?**

With `acks=all`, the broker leader must receive acknowledgments from all in-sync replicas (ISR) before it can acknowledge the producer. Even within the same AZ, replication requires a full TCP round trip between the leader and each follower. The 8 ms P99 suggests that under certain conditions, one of the replication round trips is taking much longer than the typical sub-millisecond AZ network RTT.

The most likely culprit is one of three things. First, the replication connection itself may be experiencing TCP slow start after a period of inactivity — if `net.ipv4.tcp_slow_start_after_idle=1`, a brief pause in replication traffic will cause cwnd to reset, and the first large batch will take multiple RTTs to transmit through slow start. This produces periodic spikes, which matches the P99/P50 gap described. Second, GC pauses on a follower broker can delay the replication acknowledgment. A follower that is in a GC pause cannot process and acknowledge the fetch; the leader's replica.lag.time.max.ms timer advances. Third, `linger.ms` and batch accumulation on the follower's replica fetch can add latency if the follower's `replica.fetch.response.max.bytes` is tuned too low, causing multiple round trips per batch.

To investigate: run `ss -tipm` between the leader and follower brokers to check cwnd and RTT values during a high-latency window. Inspect the Kafka broker metric `kafka.server:type=ReplicaManager,name=IsrExpandsPerSec` and `IsrShrinksPerSec` to see if replicas are flapping in and out of ISR. Check GC logs on follower brokers for pause durations. Use `tc qdisc show` to check if any traffic shaping is applied between broker nodes.

**Q2: Explain the bandwidth-delay product and why it matters for cross-region Kafka replication.**

The bandwidth-delay product is the amount of data that can be "in flight" on a network path simultaneously at full utilization, calculated as bandwidth multiplied by the round-trip time. It represents the physical capacity of the "pipe" — if you think of a network link as a pipe, the BDP is the volume of water that fits in the pipe while it's flowing at full rate.

The relevance to TCP is direct: TCP can only have `min(receive_window, congestion_window)` bytes unacknowledged at any moment. The sender must wait for ACKs before it can send more data. If the pipe can hold 50 MB of data in flight (a 10 Gbps link with 40 ms RTT), but the TCP window is only 16 MB, the sender must stop and wait for ACKs before the pipe is even half-full. The link is 68% idle by physics, not by choice.

For cross-region Kafka replication — say, us-east-1 to eu-west-1 with an RTT of approximately 75 ms on a 10 Gbps link — the BDP is about 93 MB. The Linux default `net.core.rmem_max` is 212 KB, and the TCP `rmem` max is typically 8–16 MB. This means the replication stream is operating at roughly 16 MB / 75 ms / 8 bits = 1.7 Gbps — even on a 10 Gbps link. The fix is increasing `net.core.rmem_max` and `net.ipv4.tcp_rmem` to at least twice the BDP, which on this path means 200 MB. After the buffer is large enough, the congestion window can grow to fill the pipe and the link will approach its actual capacity limit.

**Q3: You see 150,000 TIME_WAIT sockets in `ss -s` on a Kafka broker. Is this a problem? What does it tell you?**

TIME_WAIT sockets are not inherently a problem — they are a deliberate TCP mechanism to prevent stale packets from prior connections from polluting new connections on the same four-tuple (source IP, source port, destination IP, destination port). The 2 × MSL wait period (typically 60–120 seconds on Linux) ensures that any delayed duplicate packet from the old connection has expired from the network before a new connection can reuse that port combination. Without TIME_WAIT, a TCP segment from a previous connection could arrive at the server and be misinterpreted as belonging to a new connection, corrupting the data stream.

What 150,000 TIME_WAIT sockets tells you is that this broker has a very high rate of short-lived connection creation and teardown. On a Kafka broker, this almost always means one of three things: short-lived producer connections (producers creating a new `KafkaProducer` instance per request rather than reusing one), a client library that does not implement connection pooling correctly, or a monitoring or health-check system that opens and closes a TCP connection on every check. The correct fix is not to disable TIME_WAIT (which introduces correctness risks), but to identify the client creating the churn and implement connection reuse.

The only operational concern with 150,000 TIME_WAIT entries is ephemeral port exhaustion: if the broker itself is making many outgoing connections (e.g., replication to followers, or ZooKeeper/KRaft connections) and the ephemeral port range is exhausted, new outgoing connections will fail with `EADDRNOTAVAIL`. Check `net.ipv4.ip_local_port_range`; the default is 32768–60999 (28,231 ports). At 150,000 TIME_WAIT you may need to expand the range to 1024–65535. But again, the root fix is connection pooling on the clients.

---

## 11. Cross-Question Chain

**Interviewer:** Let's say you have a Kafka cluster and you're seeing producer send latency spike from 2 ms to 150 ms every 30–60 seconds, in a sawtooth pattern. Where do you start?

**Candidate:** A sawtooth pattern — regular periodic spikes followed by recovery — points away from random network congestion and toward something cyclical. The three most common causes are GC pauses on the broker, TCP slow start after an idle period, and OS-level scheduler jitter. I'd start by correlating the spike timing with broker JVM GC logs. If the GC pauses align with the latency spikes, that's my root cause. If they don't correlate, I'd check whether `net.ipv4.tcp_slow_start_after_idle=1` on the broker — a producer that pauses briefly between batches will restart slow start, causing the first post-idle batch to take multiple RTTs. That would appear as a sawtooth if batches are sent in bursts.

**Interviewer:** The GC logs show no pauses at those intervals. You check `tcp_slow_start_after_idle` and it's already set to 0. What next?

**Candidate:** With GC and slow start ruled out, I'd look at two things: first, whether the sawtooth correlates with Kafka's internal log flush interval or index checkpoint interval (`log.flush.interval.ms`). A broker that flushes all dirty log segments every 30 seconds will have elevated `%iowait` during the flush, which delays processing of incoming produce requests. The fix is relying on OS page cache flush rather than Kafka's explicit flush. Second, I'd check whether it correlates with the broker's heartbeat to the ZooKeeper or KRaft controller — a session keep-alive that blocks the request thread would appear at regular intervals.

**Interviewer:** You check the broker logs and see that every 45 seconds there's a log entry: `[QuotaManager] Applying throttle: producer throttled at 0 Mbps for 200 ms`. Explain what that means and what causes it.

**Candidate:** Kafka's quota manager enforces per-client or per-user byte rate limits. When a producer exceeds its quota — measured in bytes per second averaged over a window — the broker throttles it by delaying acknowledgments. A throttle of `0 Mbps for 200 ms` means the producer was asked to pause for 200 ms, which explains the 150 ms observed latency spikes (the 200 ms throttle reduces somewhat due to pipelining). The quota is configured in Kafka with `kafka-configs.sh --entity-type clients --entity-name <client-id> --add-config producer_byte_rate=<bytes>`. The 45-second periodicity suggests the producer has a bursty traffic pattern — quiescent for 45 seconds, then a large burst that triggers the quota. The fix is either increasing the quota if the throughput is legitimate, or smoothing the producer batching to spread traffic more evenly using `linger.ms` and `batch.size`.

**Interviewer:** Now let's zoom out. The Kafka cluster is cross-AZ with three brokers — one per AZ. A partition's leader is in AZ-A, followers are in AZ-B and AZ-C. You're told the AZ-A to AZ-B replication lag is consistently 3 ms, but AZ-A to AZ-C is consistently 18 ms. Both AZ-B and AZ-C are the same cloud region. How do you explain the difference and how do you diagnose it?

**Candidate:** Within the same cloud region, AZ-to-AZ RTT should be nearly symmetric — typically 1–3 ms. A 18 ms consistent lag to AZ-C (vs 3 ms to AZ-B) is a 6× difference that is not explained by pure network latency. The most likely causes are: first, AZ-C's broker has a resource bottleneck — high CPU, high disk I/O, or memory pressure — causing it to process the replica fetch slowly. A follower's replication throughput is limited by how fast it can write fetched data to disk. If AZ-C's broker has slow disks or is sharing I/O with other workloads, replication lag grows.

To diagnose: first, use `ss -tipm` from AZ-A's broker to the AZ-C follower and measure the actual TCP RTT — if it's 1 ms as expected, the network is not the issue. Then SSH to AZ-C's broker and run `iostat -xm 1` to check disk write latency and `%util`. If `w_await` is elevated (> 10 ms), the follower is I/O-bound. Also check `kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica` to confirm which follower is lagging. If disk is clean, check CPU and whether AZ-C's broker has more partitions than AZ-A or AZ-B (unbalanced partition leadership).

**Interviewer:** It turns out AZ-C's broker disk `w_await` is 22 ms. You add a faster disk. But the replication lag to AZ-C is still 15 ms. What are you missing?

**Candidate:** If disk is now fast but replication lag persists at 15 ms, the bottleneck has shifted elsewhere. Three candidates: TCP buffer sizing on the replication connection, the follower's replica fetch size configuration, or CPU overhead from data processing. For TCP: the replication connection between AZ-A and AZ-C crosses AZs. Run `ss -tipm` from AZ-A to AZ-C and look at the `cwnd` value and `rtt`. If `cwnd` is small (e.g., 10 MSS = 14 KB) relative to the BDP (which at 3 ms RTT and 1 Gbps is 375 KB), the connection is delivering far below link capacity. Check `net.ipv4.tcp_slow_start_after_idle` and `net.core.rmem_max` on AZ-C's broker. For replica fetch size: `replica.fetch.max.bytes` (default 1 MB) limits how much data the follower fetches per request. If the leader is producing at > 1 MB per replication interval, the follower makes multiple fetch requests per batch, each costing one RTT. Increasing `replica.fetch.max.bytes` to 10 MB would reduce the number of round trips needed.

**Interviewer:** What is the theoretical minimum replication lag you could achieve in this AZ-A to AZ-C cross-AZ setup, and what would you need to change to approach it?

**Candidate:** The theoretical minimum replication lag is one full round trip between the leader and follower, because the follower must receive the data, write it to its log, and send an acknowledgment back. If the AZ-A to AZ-C RTT is 2 ms, the minimum replication lag is 2 ms (one RTT for the fetch-response cycle). The practical minimum is slightly higher because of: the follower's disk write latency (even an NVMe drive adds 0.05–0.1 ms), the serialization/deserialization of the fetch response, and the follower's replica fetch thread scheduling.

To approach the theoretical minimum, you'd need: NVMe drives with sub-millisecond write latency on the follower, `replica.fetch.max.bytes` and `replica.fetch.response.max.bytes` large enough to never require multiple round trips per batch, TCP buffer sizes at least as large as the BDP (2 ms × 10 Gbps = 2.5 MB — easily satisfied by defaults), `tcp_slow_start_after_idle=0` so the replication connection maintains its cwnd, and the broker request handler threads not saturated so fetch requests are processed immediately. Under those conditions, you would expect to see `ReplicaManager/MaxLag` at or below 2–3 ms.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What are the three steps of the TCP three-way handshake? | SYN → SYN-ACK → ACK. One full RTT cost before application data can flow. |
| 2 | What is the bandwidth-delay product (BDP)? | BDP = Bandwidth × RTT. The amount of data in flight at full utilization. TCP window must be ≥ BDP to saturate the link. |
| 3 | What does TCP slow start do? | Starts with cwnd ~10 MSS; doubles per RTT until ssthresh is reached, then linear growth. A new connection starts slowly and ramps up. |
| 4 | What is TIME_WAIT and why does it exist? | 2×MSL wait after active close. Prevents stale packets from old connections corrupting new ones on the same 4-tuple. |
| 5 | What does a large TIME_WAIT count indicate? | High connection churn — too many short-lived connections. Fix: connection pooling, not disabling TIME_WAIT. |
| 6 | What does CLOSE_WAIT indicate? | Application has received FIN but not called close(). It's a bug — resource leak in the application. |
| 7 | What does `tcp_slow_start_after_idle=1` do to Kafka? | Resets cwnd on idle connections, causing throughput dips when the next batch arrives. Set to 0 for Kafka. |
| 8 | What is the formula for maximum TCP throughput on a single connection? | throughput = min(receive_window, cwnd) / RTT |
| 9 | What is a TCP fast retransmit? | After 3 duplicate ACKs, retransmit without waiting for RTO. Faster than RTO-based retransmit (< 1 RTT vs 200ms+). |
| 10 | What Linux sysctl limits how many SYN requests can be queued? | `net.ipv4.tcp_max_syn_backlog`. Default 1024. Kafka brokers should set to 8192+. |
| 11 | Why does `acks=all` Kafka latency scale with the number of AZs? | Each follower replica acknowledgment requires one TCP RTT. Cross-AZ RTT (1–3 ms) adds directly to produce latency. |
| 12 | What is TCP window scaling and why is it needed? | RFC 1323 extension that allows windows > 65 KB using a scale factor negotiated in handshake. Required for high-BDP links. |
| 13 | What is the MTU of standard Ethernet? And jumbo frames? | Standard: 1500 bytes. Jumbo frames: 9000 bytes. Must be consistent on all path links. |
| 14 | What is the TCP MSS and how is it related to MTU? | MSS = MTU - 40 bytes (20 IP + 20 TCP headers). Standard Ethernet MSS = 1460 bytes. |
| 15 | What is Path MTU Discovery (PMTUD) and how can it fail? | TCP uses DF bit + ICMP "fragmentation needed" to discover path MTU. Fails if firewalls block ICMP, causing hangs after handshake. |
| 16 | What happens when a NAT mapping expires on an idle Kafka connection? | The next packet is dropped silently. Consumer stalls until request.timeout.ms, then reconnects. Fix: TCP keepalives. |
| 17 | What sysctl enables BBR congestion control? | `net.ipv4.tcp_congestion_control=bbr` (requires `net.core.default_qdisc=fq`). Better for high-BDP, high-latency paths. |
| 18 | What does `ss -tipm` show that `ss -tnp` does not? | Internal TCP state: cwnd, ssthresh, RTT, retransmits, delivery rate. Essential for TCP performance diagnosis. |
| 19 | What is the ephemeral port range and why does it matter for Kafka? | `net.ipv4.ip_local_port_range` (default 32768–60999). High TIME_WAIT can exhaust it. Expand to 1024–65535 for high-churn nodes. |
| 20 | What is the difference between TCP receive window (flow control) and congestion window? | Receive window: set by receiver to avoid overwhelming its buffer. Congestion window: set by sender to avoid overwhelming the network. Throughput limited by min of both. |

---

## 13. Further Reading

- **"TCP/IP Illustrated, Volume 1" — W. Richard Stevens:** The definitive reference. Chapters 17–22 cover TCP data flow, interactive data, bulk data, timeout, and retransmission in exhaustive detail. Worth reading chapters 17–19 before your next production network incident.
- **"High Performance Browser Networking" — Ilya Grigorik (free online):** Chapter 2 covers TCP in the context of web performance but the TCP fundamentals are universally applicable. The explanation of slow start and bandwidth-delay product is exceptionally clear.
- **Brendan Gregg's "Systems Performance" — Chapter 10 (Network):** Performance analysis perspective. Covers the metrics, tools (`ss`, `netstat`, `tcpdump`), and methodology for network performance diagnosis.
- **RFC 793 (TCP):** The original TCP specification. Still readable. Section 3.4 (establishing a connection) and 3.5 (closing a connection) are worth reading once in your career to understand TIME_WAIT from first principles.
- **RFC 5681 (TCP Congestion Control):** Defines slow start, congestion avoidance, fast retransmit, and fast recovery. The algorithm descriptions are precise.
- **Google's BBR paper (2016) — "BBR: Congestion-Based Congestion Control":** Explains the problems with loss-based congestion control (CUBIC) and how BBR models the bottleneck bandwidth. Relevant for cross-region data engineering workloads.
- **Linux `ip-sysctl` man page:** `man 7 tcp` and `sysctl net.ipv4` — full documentation of every tunable. The canonical reference before touching TCP sysctls.

---

## 14. Lab Exercises

**Exercise 1: Observe TCP Slow Start**

Set up two machines (or use two network namespaces on one machine). Use `iperf3 -c <server> -t 30` to run a 30-second transfer. Use `ss -tipm` to watch the `cwnd` value grow from 10 to hundreds of MSS over the first several seconds. Note the time it takes to reach full utilization. Then run `iperf3 -c <server> -t 5 --idle-restart=5` to observe slow start restart after idle.

**Exercise 2: Measure the BDP for Your AZ Path**

Use `ping` to measure the RTT to another host in a different AZ. Use `iperf3 -c <cross-az-host> -n 1G -P 4` to measure achievable bandwidth. Calculate the BDP. Compare against `sysctl net.core.rmem_max`. Is your current buffer sufficient? Tune it and re-test.

**Exercise 3: Simulate SYN Backlog Exhaustion**

Using `hping3 --flood --syn -p 9092 <broker>` (requires `--rand-source` to simulate a flood), observe the SYN queue filling with `ss -tnp state syn-recv`. Watch the Kafka broker logs for `Possible SYN flooding` warnings. Tune `net.ipv4.tcp_max_syn_backlog` and observe the change in behavior.

**Exercise 4: Diagnose a Time_Wait Storm**

Write a Python script that creates and closes 10,000 short-lived TCP connections to a local socket. Observe `ss -s` TIME-WAIT count. Rewrite the script to use a connection pool (keep N connections alive, reuse them). Compare TIME-WAIT counts.

**Exercise 5: Capture and Annotate a TCP Handshake**

Use `tcpdump -i <interface> -w handshake.pcap tcp port 9092` to capture Kafka connection traffic. Open the pcap in Wireshark. Identify: the SYN packet and its sequence number, the SYN-ACK with the server's sequence number, the first data packet, the first ACK, and the window size advertisement. Calculate the RTT from the packet timestamps.

---

## 15. Key Takeaways

TCP/IP is not magic — it is a specific set of algorithms with specific, measurable performance characteristics. Understanding those characteristics is what separates engineers who can diagnose network-related data engineering problems from those who cannot.

The three most important concepts for a data engineer to internalize are: the **bandwidth-delay product** (which tells you when you need to tune TCP buffers), **TCP slow start** (which tells you why new or idle connections start slowly), and **the cost of a TCP handshake** (which is why connection pooling matters).

The three most important diagnostic habits are: using `ss -tipm` to inspect active connection state (cwnd, RTT, retransmits), monitoring `netstat -s | grep retran` for packet loss signals, and calculating BDP before assuming buffer defaults are sufficient for cross-region workloads.

Every Kafka `acks=all` latency, every Spark shuffle slowness, and every cross-region replication plateau has a network layer explanation. The tools and concepts in this module give you the vocabulary to find it.

---

## 16. Connections to Other Modules

- **M28 — DNS and Service Discovery:** DNS failures and TTL expiry interact with TCP connection lifecycle. A stale DNS record causes a TCP SYN to be sent to the wrong IP, which fails with timeout or reset.
- **M29 — TLS and mTLS:** TLS is layered on top of TCP. The TLS handshake adds 1–2 additional RTTs on top of the TCP handshake. Understanding the TCP handshake cost is prerequisite to understanding TLS overhead.
- **M30 — Network Latency:** The physics of RTT, bandwidth-delay product, and cross-region latency is the foundation of M30's analysis of how latency affects distributed consensus (Raft) and replication.
- **M31 — Load Balancing and Proxies:** L4 load balancers operate at the TCP level — they balance connections. Understanding TCP connection state (TIME_WAIT, established count) is necessary to understand how TCP-level balancers work.
- **M26 — CPU Diagnostics (SYS-LNX-101):** TCP SYN processing, packet receive via NAPI, and interrupt handling all consume CPU. The `%soft` (softirq) CPU time seen in `top` and `mpstat` is largely network interrupt processing.
- **SYS-DST-101 — Distributed Systems Theory:** Raft leader election timeouts and heartbeat intervals are bounded below by the TCP RTT between nodes. The CAP theorem's "network partition" is a TCP-level event.
- **DCS-KFK-101 — Apache Kafka Internals:** Nearly every Kafka performance topic — ISR, replication lag, producer latency, consumer group rebalance — has a TCP explanation at some level.

---

## 17. Anti-Patterns

**Anti-pattern: Creating a new `KafkaProducer` per message.** Each `KafkaProducer` instance opens TCP connections to every broker. Creating one per message means thousands of connections per second, each paying handshake latency and slow start. The correct pattern is a singleton `KafkaProducer` per application (or per thread in multi-threaded applications), reused across all messages.

**Anti-pattern: Using `tcp_tw_recycle`.** This sysctl was removed in Linux 4.12 because it breaks NATted deployments (multiple clients behind a NAT share an IP; the per-IP timestamp check used by `tw_recycle` incorrectly rejects legitimate connections). Never use it. The correct response to TIME_WAIT pressure is connection pooling.

**Anti-pattern: Increasing `tcp_fin_timeout` to reduce CLOSE_WAIT.** `tcp_fin_timeout` controls how long FIN_WAIT_2 persists (half-closed connections), not CLOSE_WAIT. CLOSE_WAIT is controlled by the application, not the kernel. If you have CLOSE_WAIT accumulation, the fix is in the application code — finding and fixing the code path that fails to call `close()`.

**Anti-pattern: Using UDP for "faster" Kafka or Spark communication.** UDP is faster per-packet (no handshake, no retransmit) but unreliable. Kafka's correctness guarantees — at-least-once, exactly-once delivery — are built on TCP's reliable delivery. Building exactly-once semantics on UDP would require reimplementing most of TCP. It is not done in data engineering.

**Anti-pattern: Ignoring jumbo frame consistency.** Enabling jumbo frames (MTU 9000) on some nodes but not all creates PMTUD dependency. If ICMP is blocked (common in cloud environments), connections will hang after the handshake when the first large packet exceeds the standard MTU on a link. Either configure jumbo frames everywhere consistently, or leave standard MTU everywhere.

---

## 18. Tools Reference

| Tool | Purpose | Key Flags |
|------|---------|-----------|
| `ss -tnp` | List TCP connections with processes | `-t` TCP only, `-n` no DNS, `-p` show process |
| `ss -tipm` | Detailed TCP socket internals (cwnd, RTT) | `-i` internal info, `-m` memory info |
| `ss -s` | Socket state summary | — |
| `ss -tlnp` | Listening sockets with backlog | `-l` listening only |
| `tcpdump -i eth0 -n tcp port 9092` | Capture TCP traffic | `-S` absolute seq numbers, `-w file.pcap` save |
| `netstat -s` | Cumulative TCP/IP statistics | `\| grep -i retran` for retransmits |
| `nstat -az` | Per-second delta TCP counters | Better than `netstat -s` for rates |
| `ip link show` | Interface MTU and state | — |
| `ping -M do -s <size>` | Test PMTU (DF bit set) | `-c 5` count, `-i 0.2` fast pings |
| `tracepath` | Path MTU discovery trace | — |
| `iperf3 -c <host>` | TCP throughput test | `-P 4` parallel streams, `-n 1G` bytes |
| `sysctl net.ipv4.tcp_rmem` | Read TCP buffer config | `sysctl -w key=val` to set |
| `cat /proc/net/tcp` | Raw TCP socket table (hex) | Post-process with `ss` instead |
| `cat /proc/net/snmp` | Cumulative TCP statistics | Includes retransmits, active opens |
| `cat /proc/net/dev` | Interface byte/packet counters | `watch -n1` for live rates |
| `hping3 --syn -p 9092` | TCP SYN testing / flood simulation | `--flood` for backlog stress test |

---

## 19. Glossary

**ACK (Acknowledgment):** A TCP segment flagging that data up to sequence number N has been received. Cumulative: one ACK covers all data up to that sequence number.

**BDP (Bandwidth-Delay Product):** The amount of data that fits "in flight" on a network path at full utilization. BDP = Bandwidth × RTT.

**CIDR (Classless Inter-Domain Routing):** IP address notation that specifies the network prefix length, e.g., `10.0.0.0/24`.

**Congestion Window (cwnd):** A sender-side limit on unacknowledged data in flight, managed by the congestion control algorithm. Starts small and grows with successful delivery.

**FIN:** TCP segment flag signaling "I'm done sending data." Part of the four-way TCP close sequence.

**Flow Control:** The mechanism by which a TCP receiver limits how much data the sender can send, via the advertised receive window. Prevents buffer overflow at the receiver.

**ISN (Initial Sequence Number):** A randomly chosen starting sequence number in the TCP SYN. Prevents confusion with stale packets from previous connections.

**MTU (Maximum Transmission Unit):** The largest IP packet a network link can carry without fragmentation. Standard Ethernet: 1500 bytes. Jumbo frames: 9000 bytes.

**MSS (Maximum Segment Size):** The largest TCP payload (not counting headers) that a host will accept. MSS = MTU − 40 bytes. Advertised during the handshake.

**NAT (Network Address Translation):** Rewrites source IP/port of outgoing packets to enable many internal hosts to share one public IP.

**PMTUD (Path MTU Discovery):** TCP mechanism to discover the smallest MTU along a network path, using the DF bit and ICMP "fragmentation needed" responses.

**Receive Window:** The amount of buffer space the receiver has available, advertised in TCP headers. Limits the sender's unacknowledged data in flight.

**RFC 1918:** The IETF specification defining private IP address ranges: `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`.

**RTT (Round-Trip Time):** The time for a packet to travel from source to destination and back. The fundamental latency unit for TCP analysis.

**RTO (Retransmission Timeout):** The time after which TCP retransmits an unacknowledged segment. Computed from smoothed RTT; minimum 200 ms in Linux.

**Slow Start:** TCP's initial phase of connection ramp-up where cwnd doubles per RTT until ssthresh is reached.

**SYN:** TCP segment flag initiating a connection. SYN flood: an attack sending many SYNs without completing the handshake, exhausting the SYN backlog.

**TIME_WAIT:** TCP state after active close. Lasts 2×MSL (up to 120 seconds). Prevents stale packets from corrupting new connections.

**CLOSE_WAIT:** TCP state when a connection's FIN has been received but the application has not yet called close(). Indicates an application bug.

**TCP Four-Tuple:** The unique identifier of a TCP connection: (source IP, source port, destination IP, destination port). Time_wait holds this 4-tuple reserved.

**Window Scaling:** RFC 1323 extension allowing TCP receive windows larger than 65 KB, necessary for high-BDP links.

---

## 20. Self-Assessment

1. Without looking it up, describe what happens in the TCP three-way handshake. What state does each side enter at each step?
2. A Kafka producer in us-west-2 sends to a broker in us-east-1 (RTT = 65 ms). With `acks=1`, what is the absolute minimum produce latency? With `acks=all` and two follower replicas in the same AZ as the leader (RTT = 1 ms), what is the minimum produce latency?
3. Calculate the BDP for a 10 Gbps link with 40 ms RTT. What `net.core.rmem_max` would you set?
4. You see 5,000 CLOSE_WAIT sockets on a Spark executor. What does this tell you? What would you look for in the application code?
5. Explain in one sentence why TIME_WAIT exists and why you should not disable it.
6. A new TCP connection to a Kafka broker achieves only 50 Mbps for the first second on a 1 Gbps link. What TCP mechanism explains this, and what is the equation that governs the throughput during this phase?
7. What is `tcp_slow_start_after_idle`, what does it do, and what value should you set it to on a Kafka broker?
8. A cross-region replication job plateaus at 200 Mbps on a 10 Gbps link with 50 ms RTT. Without changing the hardware, what single tuning change is most likely to fix it?
9. What does `ss -tipm` show that `ss -tnp` does not? Give three specific fields it reveals.
10. Why does a Kafka consumer behind a cloud NAT sometimes stop receiving messages for several minutes without any error?

---

## 21. Module Summary

TCP/IP is the foundation on which every distributed data system is built. IP provides best-effort packet routing using hierarchical 32-bit addresses; TCP adds reliability, ordering, and flow control on top of that unreliable service. The cost of TCP's reliability guarantees is concrete and measurable: one RTT for the handshake before data flows, several RTTs of slow start before full bandwidth is reached, and buffer limits that prevent high-BDP links from being fully utilized unless tuned.

For data engineers, four TCP concepts deserve permanent residence in your mental model. The **handshake cost** (1 RTT) is why connection pooling is not optional — a Kafka producer that creates a new connection per message pays 65 ms overhead per message on a cross-region path. **Slow start** is why new and idle connections start slowly — a Kafka replication connection that sits idle briefly will restart slow start, causing periodic throughput dips unless `tcp_slow_start_after_idle=0`. The **bandwidth-delay product** is why default TCP buffers are inadequate for cross-region workloads — a 10 Gbps link with 50 ms RTT needs 62.5 MB of buffer to saturate; the Linux default is 212 KB. And **TIME_WAIT** is why high connection churn is expensive — each short-lived connection leaves a TIME_WAIT socket for 60–120 seconds, consuming port space and kernel memory.

The diagnostic tools that operationalize these concepts are `ss -tipm` (TCP socket internals: cwnd, RTT, retransmits), `nstat -az` (per-second TCP statistics), `tcpdump` (packet-level visibility), and the `/proc/net/` virtual filesystem (programmatic access to all TCP state). The tuning levers are in `/etc/sysctl.d/`: `tcp_rmem`/`tcp_wmem` for buffer sizing, `tcp_max_syn_backlog`/`somaxconn` for connection capacity, `tcp_keepalive_*` for NAT compatibility, and `tcp_slow_start_after_idle` for steady-state throughput on long-lived connections.

The module that follows — M28: DNS and Service Discovery — builds directly on this foundation, examining how hostnames are resolved to IP addresses before a TCP connection can even begin, and how DNS failures and TTL misconfiguration interact with TCP connection management in distributed data systems.
