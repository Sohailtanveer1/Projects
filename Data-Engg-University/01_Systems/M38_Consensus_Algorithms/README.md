# M38: Consensus Algorithms

**Course:** SYS-DST-101 — Distributed Systems Theory  
**Module:** 02 of 05  
**Global Module ID:** M38  
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

M37 established that CP systems sacrifice availability to maintain consistency. But "maintaining consistency" requires a mechanism — some way for distributed nodes to agree on what the correct value is, even when some nodes are down and messages can be lost. That mechanism is consensus.

Consensus is the fundamental problem underlying every strongly-consistent distributed system. When a Kafka cluster needs to elect a new controller after the old one fails, it needs consensus. When ZooKeeper processes a configuration write, it uses consensus to ensure all nodes agree on the new value. When a CockroachDB node commits a transaction, it uses consensus to ensure the transaction is durably recorded on a quorum of nodes before returning success. When Kubernetes' etcd stores cluster state, it uses Raft to ensure all etcd replicas agree. The systems data engineers use daily are built on consensus algorithms, and when those systems behave mysteriously — a ZooKeeper quorum loss causing Kafka controller election to stall, an etcd split-brain during a Kubernetes node partition, a Kafka KRaft metadata log falling behind — understanding consensus explains what happened and why.

The two dominant consensus algorithms are Paxos (Leslie Lamport, 1989) and Raft (Diego Ongaro and John Ousterhout, 2014). Paxos is theoretically elegant but notoriously difficult to implement correctly in a practical system. Raft was designed explicitly for understandability — it decomposes consensus into leader election, log replication, and safety constraints, making each component independently comprehensible. Raft is the consensus algorithm behind etcd (Kubernetes), CockroachDB, TiKV (TiDB), and Kafka KRaft. Understanding Raft at the whiteboard level is a hard requirement for senior and staff data engineering interviews.

---

## 2. Mental Model

### What Consensus Means

Consensus is the problem of getting a set of distributed nodes to agree on a single value, given that some nodes may crash and messages may be delayed or lost. Formally, a consensus algorithm must satisfy three properties:

**Agreement:** All non-failing nodes that decide, decide the same value.  
**Validity:** The decided value must have been proposed by some node — you can't decide a value that was invented out of thin air.  
**Termination:** Eventually, all non-failing nodes decide some value — the algorithm makes progress.

These properties seem obvious, but satisfying all three simultaneously in the presence of failures is not. The FLP impossibility result (Fischer, Lynch, Paterson, 1985) proved that no deterministic consensus algorithm can guarantee termination in an asynchronous system with even a single crash failure. Paxos and Raft work around this by using randomization and timeouts — they are not deterministically guaranteed to terminate, but they terminate with overwhelming probability in practice.

### Why a Single Leader is the Key Insight

Both Paxos and Raft use a leader to establish consensus. The intuition: if you have a single node that all others defer to, agreement is trivially maintained — the leader decides, everyone follows. The hard problem is ensuring that there is always at most one leader at any time (preventing split-brain) and that a new leader can be safely elected when the old one fails.

**The quorum principle:** To ensure safety (at most one leader and no data loss), both Paxos and Raft use a majority quorum. In a cluster of N nodes, a quorum is ⌊N/2⌋ + 1. For 3 nodes: quorum = 2. For 5 nodes: quorum = 3. The key property: any two quorums in a cluster must overlap by at least one node. This overlapping node acts as a "witness" that ensures the new leader has seen everything the old leader committed.

Why majority (not all)? All-or-nothing (requiring all N nodes) provides no fault tolerance — one failure stops the system. Majority provides fault tolerance: N=3 tolerates 1 failure, N=5 tolerates 2 failures. The overlap property ensures safety even with failures.

---

## 3. Core Concepts

### 3.1 Paxos: The Intuition

Paxos solves the problem of reaching agreement on a single value (Single-Decree Paxos). It has two phases:

**Phase 1 — Prepare/Promise:**
A node that wants to be leader (the "proposer") picks a proposal number `n` (higher than any it has seen) and sends `Prepare(n)` to all nodes. Each node (the "acceptor") that receives this message either:
- Ignores it if it has already seen a higher proposal number (someone else is also trying to lead)
- Responds with `Promise(n, accepted_value)` — promising not to accept any proposal with number < n, and returning any value it has already accepted (this is critical: it prevents the new proposer from overwriting something that might already be committed)

If the proposer receives `Promise` responses from a quorum of acceptors, it proceeds to Phase 2.

**Phase 2 — Accept/Accepted:**
The proposer chooses a value `v`:
- If any `Promise` responses contained an already-accepted value, the proposer MUST use the highest-numbered one (cannot invent its own value — this preserves any value that may already be committed)
- Otherwise, the proposer can propose its own value

It sends `Accept(n, v)` to all acceptors. Each acceptor that hasn't promised to ignore n responds with `Accepted(n, v)`. When the proposer receives `Accepted` from a quorum, the value is committed.

**The key safety insight:** If a value was committed in an earlier round, any new leader attempting Phase 2 will discover that value (because its Phase 1 quorum must include at least one node from the previous commit quorum), and will be forced to propagate it rather than overwrite it.

**Why Paxos is hard to use in practice:** Single-Decree Paxos decides one value. Building a replicated log (deciding a sequence of values) requires Multi-Paxos, which requires electing a stable leader, distinguishing log position preparation from instance preparation, handling leader changes, and many other nuances. Lamport's original paper intentionally left these details unspecified. Every real Paxos implementation (Google Chubby, Apache ZooKeeper's ZAB, Google Spanner) invented its own solutions to these gaps — making each implementation effectively its own algorithm.

### 3.2 Raft: Leader Election

Raft decomposes consensus into three relatively independent sub-problems: leader election, log replication, and safety. Every node in a Raft cluster is in one of three states: **Follower**, **Candidate**, or **Leader**.

**Normal operation:** One leader, all others followers. The leader receives all client writes. It sends `AppendEntries` RPCs to all followers (also used as heartbeats when there are no new entries). Followers reset their election timeout on each heartbeat. If a follower receives no heartbeat before its timeout expires, it suspects the leader has crashed and starts an election.

**Election timeout:** Each follower has a randomized election timeout (e.g., 150–300ms in the original paper). The randomization prevents multiple followers from starting elections simultaneously, making split votes rare.

**Election process:**
1. A follower's election timeout fires. It increments its `currentTerm` (a monotonically increasing counter that serves as the logical clock for the cluster) and transitions to **Candidate**.
2. The candidate votes for itself and sends `RequestVote(term, lastLogIndex, lastLogTerm)` to all other nodes.
3. Each node grants its vote if:
   - It hasn't voted in this term yet, AND
   - The candidate's log is at least as up-to-date as the voter's log (`lastLogIndex` and `lastLogTerm` comparison — the "election restriction")
4. If the candidate receives votes from a quorum (majority), it becomes the new **Leader**.
5. If no candidate gets a majority (split vote), all candidates time out and start a new election with a higher term.

**The election restriction (critical for safety):** A candidate can only win if its log is at least as up-to-date as a majority of nodes. "At least as up-to-date" means: higher term number in the last log entry, or same term number but longer log. This guarantees that the new leader has all committed entries — because any committed entry was on a majority of nodes, and the new leader's quorum overlaps with that majority.

### 3.3 Raft: Log Replication

Once a leader is elected, it handles all client requests:

1. Client sends a command to the leader (or gets redirected to the leader by any follower).
2. Leader appends the command to its local log as a new entry, with the current term number.
3. Leader sends `AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)` to all followers in parallel.
4. Each follower verifies the `prevLogIndex` and `prevLogTerm` — if its log doesn't match at that position, it rejects and sends back its current log state so the leader can find the divergence point.
5. If the follower accepts, it appends the entries to its log and responds success.
6. When the leader receives success from a quorum of followers (including itself), the entry is **committed**.
7. The leader notifies followers of the new commit index in the next `AppendEntries`.
8. The leader responds to the client that the command was applied.

**Log matching property:** Raft maintains a strong invariant: if two log entries in different nodes' logs have the same index and term, they are identical — and all entries before them are also identical. This is enforced by the `prevLogIndex/prevLogTerm` check in `AppendEntries`. If the follower's log diverges, the leader will work backward until it finds the common prefix, then overwrite the follower's divergent entries.

**Commit rule:** A leader can only commit an entry from the **current term** by quorum acknowledgment. It cannot directly commit entries from previous terms by counting acknowledgments — only by committing a new entry in the current term that happens to advance the commit index past the old entries. This subtle rule prevents an edge case where a leader commits an entry from a previous term that the new leader might have overwritten.

### 3.4 Raft: Safety Properties

**Leader Completeness:** If a log entry is committed in term T, it will be present in the logs of all leaders for term > T. Guaranteed by the election restriction.

**State Machine Safety:** If a server has applied log entry X to its state machine, no other server will ever apply a different command at log index X. Guaranteed by the Log Matching Property.

**No two leaders in the same term:** Each node votes at most once per term. A quorum grants votes to at most one candidate per term. Therefore, at most one candidate can win a given term.

### 3.5 Raft in Production: Kafka KRaft

Prior to KRaft (Kafka Raft Metadata), Kafka used ZooKeeper (ZAB consensus) for metadata: topic configuration, partition assignments, broker health, and controller election. ZooKeeper was a separate cluster requiring separate operational expertise, and all Kafka metadata operations had to go through ZooKeeper — a bottleneck.

Kafka KRaft (introduced in 2.8, production-ready in 3.3+) replaces ZooKeeper with a Raft-based metadata log built into Kafka itself. A subset of Kafka brokers (the "KRaft controllers") form a Raft quorum:

```
KRaft cluster (3 controllers):
  - Controller-1: leader (elected via Raft)
  - Controller-2: follower
  - Controller-3: follower
  
  All metadata changes (create topic, assign partitions, 
  update ISR, broker registration) go through the Raft log.
  The log is replicated to all controllers before acknowledging.
```

**KRaft metadata log entries include:**
- `RegisterBrokerRecord`: a broker comes online
- `TopicRecord`: a new topic is created
- `PartitionRecord`: partition assignment
- `PartitionChangeRecord`: ISR change, leader change
- `ProducerIdsRecord`: producer ID allocation (for idempotent producers)

**Leader election in KRaft:**
When the active KRaft controller fails (stops sending heartbeats), the other controllers start a Raft election. The winner becomes the new active controller and resumes processing metadata changes. This process takes seconds — much faster than the ZooKeeper-based controller election, which required lease expiry, ZooKeeper notifications, and then a controller candidate claiming the `/controller` ZNode.

### 3.6 Raft in Production: etcd and Kubernetes

Kubernetes stores all cluster state (pod definitions, service definitions, config maps, secrets, node status) in etcd, a distributed key-value store that uses Raft. The etcd Raft cluster typically has 3 or 5 members.

**Write path:**
1. `kubectl apply -f deployment.yaml` → kube-apiserver writes to etcd leader
2. etcd leader appends to its Raft log
3. etcd leader sends `AppendEntries` to followers (controller-1, controller-2)
4. Followers acknowledge (quorum reached)
5. etcd commits the log entry, applies to state machine
6. etcd notifies kube-apiserver: write committed
7. kube-controller-manager and kube-scheduler watch etcd and react to the change

**etcd elections and Kubernetes availability:** If the etcd leader crashes and an election occurs, Kubernetes writes are blocked for the duration of the election (seconds). Reads from etcd may still work (depending on `--consistency` flag). Applications running in the cluster are unaffected (their workloads keep running). Only new deployments, scaling operations, and cluster state changes fail during the election.

This is the CP behavior of Raft in practice: Kubernetes cluster state is unavailable for writes during etcd leadership changes, prioritizing consistency over availability.

### 3.7 ZooKeeper Atomic Broadcast (ZAB)

ZAB is the consensus protocol in ZooKeeper. It is not Raft but shares similar structure: a leader-based protocol with a quorum requirement. The key differences from Raft:

- ZAB has an explicit recovery phase (Synchronization phase) where the new leader synchronizes all followers to its state before processing new writes. Raft handles this via the log replication mechanism.
- ZAB uses "epochs" instead of "terms" for leader identity.
- ZAB guarantees total order of updates (like Raft), but also provides a stronger guarantee: the primary order guarantee means transactions are delivered in the order the leader sent them, which matters for ZooKeeper's sequential consistency guarantee.

For data engineers: ZooKeeper's ZAB behavior explains why Kafka (before KRaft) would stall during ZooKeeper leader elections — all Kafka controller operations blocked until ZAB completed the leader election and synchronization phase.

---

## 4. Hands-On Walkthrough

### 4.1 Observing Kafka KRaft Leader Election

```bash
# Identify the current KRaft controller (active controller)
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status
# Output example:
# ClusterId:              abc123xyz
# LeaderId:               1
# LeaderEpoch:            7
# HighWatermark:          1234
# MaxFollowerLag:         0
# MaxFollowerLagTimeMs:   -1
# CurrentVoters:          [1, 2, 3]
# CurrentObservers:       []

# Watch leader epoch — increments on each election
watch -n 2 kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status | grep -E "LeaderId|LeaderEpoch"

# List the metadata log (Raft log entries)
kafka-dump-log.sh \
  --files /var/kafka/data/__cluster_metadata-0/00000000000000000000.log \
  --print-data-log | head -100
# Shows individual Raft log entries:
# offset: 0 CreateTime: ... magic: 2 compresscodec: none payload:
# {type:REGISTER_BROKER_RECORD, brokerId:1, ...}
# {type:TOPIC_RECORD, name:events, topicId:...}

# Simulate a KRaft controller failure
# Kill the process with ID matching the current leader
kill -9 $(ps aux | grep "kafka.server.KafkaRaftServer" | grep "node.id=1" | awk '{print $2}')

# Watch election occur (leader changes to 2 or 3)
watch -n 1 kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9093 describe --status | grep "LeaderId\|LeaderEpoch"
# LeaderEpoch will increment: 7 → 8
# LeaderId will change: 1 → 2 (or 3)
```

### 4.2 Observing etcd Raft State

```bash
# Check etcd cluster health and leader
ETCDCTL_API=3 etcdctl \
  --endpoints=https://etcd1:2379,https://etcd2:2379,https://etcd3:2379 \
  --cacert=/etc/etcd/ca.crt \
  --cert=/etc/etcd/client.crt \
  --key=/etc/etcd/client.key \
  endpoint status --write-out=table

# Output:
# ENDPOINT        ID              STATUS  LEADER      RAFT TERM  RAFT INDEX
# etcd1:2379  abc123  healthy  abc123  47         12345
# etcd2:2379  def456  healthy  abc123  47         12345
# etcd3:2379  ghi789  healthy  abc123  47         12345
# LEADER column shows which node is the Raft leader
# RAFT TERM shows current election term (increments on each election)
# RAFT INDEX shows log index (commit position)

# Watch for leader changes (increments in RAFT TERM indicate an election)
watch -n 5 'ETCDCTL_API=3 etcdctl \
  --endpoints=https://etcd1:2379,https://etcd2:2379,https://etcd3:2379 \
  endpoint status --write-out=table'

# Check etcd member list
ETCDCTL_API=3 etcdctl member list --write-out=table

# Monitor etcd Raft metrics via Prometheus
curl http://etcd1:2379/metrics | grep -E "etcd_server_leader_changes|etcd_server_proposals"
# etcd_server_leader_changes_seen_total 2     ← 2 elections have occurred
# etcd_server_proposals_committed_total 12345 ← Raft log entries committed
# etcd_server_proposals_applied_total 12345   ← Applied to state machine
# etcd_server_proposals_pending 0             ← 0 in-flight proposals
```

### 4.3 Diagnosing Raft Health via Metrics

```bash
# Key etcd metrics for Raft health (via Prometheus/Grafana)

# 1. Leader changes: should be near-zero in a healthy cluster
# Sudden spike → elections occurring → check network latency and node health
etcd_server_leader_changes_seen_total

# 2. Proposal apply lag: committed - applied (should be 0)
# If positive, the state machine is falling behind applying committed entries
etcd_server_proposals_committed_total - etcd_server_proposals_applied_total

# 3. Peer round-trip time: time for AppendEntries to complete
# > 100ms within a single AZ → disk or CPU issue on follower
etcd_network_peer_round_trip_time_seconds_bucket

# 4. WAL fsync duration: time to flush log to disk
# > 10ms → disk is saturated (use faster SSD for etcd)
etcd_disk_wal_fsync_duration_seconds_bucket{quantile="0.99"}

# 5. Kafka KRaft equivalent: metadata follower lag
kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication
# Shows per-follower lag in log entries and milliseconds
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
raft_simulator.py

A simplified Raft consensus simulator for learning and visualization.
Implements: leader election, log replication, and safety properties.
Does NOT implement: log compaction, cluster membership changes, or 
client interaction — focuses on the core consensus mechanism.

No external dependencies.
"""

import random
import time
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ─── Types ──────────────────────────────────────────────────────────────────────

class NodeState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass
class LogEntry:
    term: int       # Term when this entry was created
    index: int      # Position in the log (1-indexed)
    command: str    # The command to apply


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int   # Highest log index that matches (for leader's tracking)


@dataclass
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool


# ─── Raft Node ──────────────────────────────────────────────────────────────────

class RaftNode:
    """
    Simplified Raft node. Communicates via direct method calls (no real network).
    For a real system, replace method calls with RPCs.

    Key Raft state:
    - currentTerm: latest term this node has seen (persisted)
    - votedFor: candidateId voted for in current term (persisted)
    - log: list of log entries (persisted)
    - commitIndex: index of highest log entry known to be committed
    - lastApplied: index of highest log entry applied to state machine
    """

    def __init__(self, node_id: str, election_timeout_range: tuple = (150, 300)):
        self.node_id = node_id
        self.state = NodeState.FOLLOWER

        # Persistent state (would be written to disk in real implementation)
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: list[LogEntry] = []   # 0-indexed, but log is 1-indexed conceptually

        # Volatile state
        self.commit_index = 0           # Highest index known to be committed
        self.last_applied = 0           # Highest index applied to state machine

        # Leader state (reset on election)
        self.next_index: dict[str, int] = {}     # Next log index to send to each follower
        self.match_index: dict[str, int] = {}    # Highest replicated index for each follower

        # Cluster membership (set after initialization)
        self.peers: dict[str, 'RaftNode'] = {}

        # Election timeout (randomized to prevent split votes)
        self.election_timeout_ms = random.randint(*election_timeout_range)
        self._last_heartbeat = time.monotonic()
        self._lock = threading.Lock()

        # State machine (simplified: just a dict)
        self.state_machine: dict[str, str] = {}
        self._votes_received: set[str] = set()

        # Debug log
        self._events: list[str] = []

    def _log(self, msg: str):
        ts = time.monotonic()
        event = f"[T{self.current_term}] [{self.node_id}|{self.state.value}] {msg}"
        self._events.append(event)
        print(event)

    @property
    def last_log_index(self) -> int:
        return len(self.log)  # 1-indexed

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _is_log_up_to_date(self, other_last_log_index: int, other_last_log_term: int) -> bool:
        """
        Raft election restriction: a candidate's log is "at least as up-to-date"
        if its last entry has a higher term, or same term and >= length.
        """
        if other_last_log_term != self.last_log_term:
            return other_last_log_term > self.last_log_term
        return other_last_log_index >= self.last_log_index

    def _step_down(self, new_term: int):
        """
        If we see a higher term, revert to follower.
        This is the crucial rule that prevents stale leaders.
        """
        if new_term > self.current_term:
            self._log(f"Stepping down: saw term {new_term} > current {self.current_term}")
            self.current_term = new_term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            self._votes_received.clear()

    # ─── Leader Election ──────────────────────────────────────────────────────────

    def start_election(self):
        """
        Called when election timeout fires. Transitions to CANDIDATE
        and sends RequestVote to all peers.
        """
        with self._lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id   # Vote for self
            self._votes_received = {self.node_id}
            self._log(f"Starting election for term {self.current_term}")

        # Send RequestVote to all peers
        for peer_id, peer in self.peers.items():
            request = RequestVoteRequest(
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=self.last_log_index,
                last_log_term=self.last_log_term,
            )
            response = peer.handle_request_vote(request)
            self._handle_vote_response(response)

    def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        """
        Process a vote request from a candidate.
        Grant vote if: (1) request term >= our term,
                       (2) haven't voted or voted for this candidate,
                       (3) candidate's log is at least as up-to-date as ours.
        """
        with self._lock:
            if request.term > self.current_term:
                self._step_down(request.term)

            vote_granted = False
            if (request.term >= self.current_term and
                (self.voted_for is None or self.voted_for == request.candidate_id) and
                self._is_log_up_to_date(request.last_log_index, request.last_log_term)):
                self.voted_for = request.candidate_id
                vote_granted = True
                self._log(f"Granted vote to {request.candidate_id} for term {request.term}")
            else:
                self._log(f"Denied vote to {request.candidate_id}: "
                         f"term={request.term} voted_for={self.voted_for} "
                         f"log_ok={self._is_log_up_to_date(request.last_log_index, request.last_log_term)}")

            return RequestVoteResponse(term=self.current_term, vote_granted=vote_granted)

    def _handle_vote_response(self, response: RequestVoteResponse):
        """Process a vote response. Become leader if quorum achieved."""
        with self._lock:
            if response.term > self.current_term:
                self._step_down(response.term)
                return

            if self.state != NodeState.CANDIDATE:
                return  # Already became leader or stepped down

            if response.vote_granted:
                self._votes_received.add(response.term)  # simplified: track count
                total_nodes = len(self.peers) + 1
                quorum = total_nodes // 2 + 1

                if len(self._votes_received) >= quorum:
                    self._become_leader()

    def _become_leader(self):
        """Transition to leader. Initialize nextIndex and matchIndex for all peers."""
        self.state = NodeState.LEADER
        self._log(f"Became LEADER for term {self.current_term}")

        # Initialize leader state
        for peer_id in self.peers:
            self.next_index[peer_id] = self.last_log_index + 1
            self.match_index[peer_id] = 0

        # Send initial empty AppendEntries (heartbeat) to establish authority
        self.send_heartbeat()

    # ─── Log Replication ──────────────────────────────────────────────────────────

    def client_write(self, command: str) -> bool:
        """
        Accept a client write. Only the leader can accept writes.
        Appends to log, replicates to followers, commits when quorum acknowledges.
        """
        if self.state != NodeState.LEADER:
            self._log(f"Rejecting write (not leader): {command}")
            return False

        with self._lock:
            entry = LogEntry(
                term=self.current_term,
                index=self.last_log_index + 1,
                command=command,
            )
            self.log.append(entry)
            self._log(f"Appended log entry index={entry.index}: {command}")

        # Replicate to all followers
        success_count = 1  # Leader counts itself
        for peer_id, peer in self.peers.items():
            if self._replicate_to_peer(peer_id, peer):
                success_count += 1

        # Check if we have quorum
        total_nodes = len(self.peers) + 1
        quorum = total_nodes // 2 + 1

        if success_count >= quorum:
            # Commit the entry
            new_commit_index = entry.index
            # Raft rule: only commit entries from current term
            if self.log[new_commit_index - 1].term == self.current_term:
                self.commit_index = new_commit_index
                self._apply_committed_entries()
                self._log(f"Committed up to index {self.commit_index}")
                return True

        self._log(f"Write failed: only {success_count}/{total_nodes} acknowledged")
        return False

    def _replicate_to_peer(self, peer_id: str, peer: 'RaftNode') -> bool:
        """Send AppendEntries to a peer and process the response."""
        next_idx = self.next_index.get(peer_id, 1)
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 else 0
        entries_to_send = self.log[next_idx - 1:]

        request = AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries_to_send,
            leader_commit=self.commit_index,
        )

        response = peer.handle_append_entries(request)

        if response.term > self.current_term:
            self._step_down(response.term)
            return False

        if response.success:
            self.match_index[peer_id] = response.match_index
            self.next_index[peer_id] = response.match_index + 1
            return True
        else:
            # Log inconsistency: decrement nextIndex and retry
            if peer_id in self.next_index:
                self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)
            return False

    def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """
        Process AppendEntries from the leader.
        Returns success if log is consistent at prev_log_index/prev_log_term.
        """
        with self._lock:
            if request.term > self.current_term:
                self._step_down(request.term)

            if request.term < self.current_term:
                return AppendEntriesResponse(
                    term=self.current_term,
                    success=False,
                    match_index=0,
                )

            # Valid heartbeat from current leader — reset election timer
            self._last_heartbeat = time.monotonic()
            if self.state == NodeState.CANDIDATE:
                self._step_down(request.term)

            # Consistency check: does our log match at prevLogIndex?
            if request.prev_log_index > 0:
                if (request.prev_log_index > len(self.log) or
                    self.log[request.prev_log_index - 1].term != request.prev_log_term):
                    self._log(f"Log inconsistency at index {request.prev_log_index}")
                    return AppendEntriesResponse(
                        term=self.current_term,
                        success=False,
                        match_index=min(request.prev_log_index - 1, len(self.log)),
                    )

            # Append new entries (overwrite any conflicting entries)
            for i, entry in enumerate(request.entries):
                log_index = request.prev_log_index + i + 1
                if log_index <= len(self.log):
                    if self.log[log_index - 1].term != entry.term:
                        # Conflict: truncate from here and replace
                        self.log = self.log[:log_index - 1]
                        self.log.append(entry)
                        self._log(f"Overwrote conflicting entry at index {log_index}")
                    # else: same term, no-op (already have this entry)
                else:
                    self.log.append(entry)

            # Update commit index
            if request.leader_commit > self.commit_index:
                self.commit_index = min(request.leader_commit, len(self.log))
                self._apply_committed_entries()

            match_index = request.prev_log_index + len(request.entries)
            return AppendEntriesResponse(
                term=self.current_term,
                success=True,
                match_index=match_index,
            )

    def _apply_committed_entries(self):
        """Apply all committed but not-yet-applied entries to the state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            # Apply to state machine (simplified: parse "key=value")
            if '=' in entry.command:
                key, value = entry.command.split('=', 1)
                self.state_machine[key.strip()] = value.strip()
            self._log(f"Applied to state machine: {entry.command}")

    def send_heartbeat(self):
        """Send empty AppendEntries to all followers (heartbeat)."""
        if self.state != NodeState.LEADER:
            return
        for peer_id, peer in self.peers.items():
            self._replicate_to_peer(peer_id, peer)

    def get_status(self) -> dict:
        return {
            'node_id': self.node_id,
            'state': self.state.value,
            'current_term': self.current_term,
            'log_length': len(self.log),
            'commit_index': self.commit_index,
            'last_applied': self.last_applied,
            'state_machine': dict(self.state_machine),
        }


# ─── Cluster Simulation ──────────────────────────────────────────────────────────

class RaftCluster:
    """
    Simulate a Raft cluster with controllable node failures.
    """

    def __init__(self, node_ids: list[str]):
        self.nodes = {nid: RaftNode(nid) for nid in node_ids}
        # Connect all nodes to each other
        for nid, node in self.nodes.items():
            node.peers = {pid: peer for pid, peer in self.nodes.items() if pid != nid}

    def elect_initial_leader(self) -> Optional[str]:
        """Run election from node-0 (simplified: directly start election)."""
        first_node = list(self.nodes.values())[0]
        first_node.start_election()
        leader = self.get_leader()
        if leader:
            print(f"\n✓ Leader elected: {leader.node_id} (term {leader.current_term})")
        return leader.node_id if leader else None

    def get_leader(self) -> Optional[RaftNode]:
        """Return the current leader, or None if no leader."""
        for node in self.nodes.values():
            if node.state == NodeState.LEADER:
                return node
        return None

    def write(self, command: str) -> bool:
        """Write a command through the leader."""
        leader = self.get_leader()
        if not leader:
            print("✗ No leader available")
            return False
        return leader.client_write(command)

    def partition_node(self, node_id: str):
        """
        Simulate a partition by removing a node from all peers' peer lists
        and removing all peers from its peer list.
        """
        node = self.nodes[node_id]
        # Remove partitioned node from others' peer lists
        for nid, n in self.nodes.items():
            if nid != node_id and node_id in n.peers:
                del n.peers[node_id]
        # Clear the partitioned node's peers
        node.peers = {}
        print(f"\n⚡ Node {node_id} partitioned from cluster")

    def heal_partition(self, node_id: str):
        """Reconnect a partitioned node."""
        node = self.nodes[node_id]
        node.peers = {pid: peer for pid, peer in self.nodes.items() if pid != node_id}
        for nid, n in self.nodes.items():
            if nid != node_id:
                n.peers[node_id] = node
        print(f"\n✓ Node {node_id} reconnected to cluster")

        # Sync the reconnected node from the leader
        leader = self.get_leader()
        if leader:
            leader._replicate_to_peer(node_id, node)

    def show_state(self):
        """Print current state of all nodes."""
        print("\n" + "─" * 60)
        for nid, node in self.nodes.items():
            status = node.get_status()
            star = "★ " if status['state'] == 'LEADER' else "  "
            print(f"{star}{nid} [{status['state']}] "
                  f"term={status['current_term']} "
                  f"log={status['log_length']} "
                  f"commit={status['commit_index']} "
                  f"sm={status['state_machine']}")
        print("─" * 60)


# ─── Safety Verifier ─────────────────────────────────────────────────────────────

def verify_raft_safety(cluster: RaftCluster) -> list[str]:
    """
    Check Raft safety properties on a cluster:
    1. At most one leader per term
    2. Log matching: nodes agree on entries at the same index
    3. Committed entries are present in all nodes' logs
    """
    violations = []

    # Property 1: At most one leader per term
    leaders_by_term = {}
    for node in cluster.nodes.values():
        if node.state == NodeState.LEADER:
            term = node.current_term
            if term in leaders_by_term:
                violations.append(
                    f"SAFETY VIOLATION: Two leaders in term {term}: "
                    f"{leaders_by_term[term]} and {node.node_id}"
                )
            leaders_by_term[term] = node.node_id

    # Property 2: Log matching at commit index
    nodes = list(cluster.nodes.values())
    min_commit = min(n.commit_index for n in nodes)
    for i in range(min_commit):
        entries_at_index = {}
        for node in nodes:
            if i < len(node.log):
                entry = node.log[i]
                key = (entry.term, entry.command)
                entries_at_index[node.node_id] = key

        unique_entries = set(entries_at_index.values())
        if len(unique_entries) > 1:
            violations.append(
                f"LOG MISMATCH at index {i+1}: {entries_at_index}"
            )

    # Property 3: Committed entries in all logs
    leader = cluster.get_leader()
    if leader:
        for i in range(leader.commit_index):
            if i >= len(leader.log):
                break
            committed_entry = leader.log[i]
            for node in cluster.nodes.values():
                if i < len(node.log):
                    if node.log[i].term != committed_entry.term or \
                       node.log[i].command != committed_entry.command:
                        violations.append(
                            f"COMMITTED ENTRY MISSING on {node.node_id} at index {i+1}"
                        )

    return violations


# ─── Demo ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Raft Consensus Simulator ===\n")

    # Create 3-node cluster
    cluster = RaftCluster(["node-A", "node-B", "node-C"])

    print("Step 1: Elect initial leader")
    leader_id = cluster.elect_initial_leader()
    cluster.show_state()

    print("\nStep 2: Write some entries through the leader")
    cluster.write("topic=events")
    cluster.write("partitions=12")
    cluster.write("replication_factor=3")
    cluster.show_state()

    print("\nStep 3: Verify safety properties")
    violations = verify_raft_safety(cluster)
    if violations:
        for v in violations:
            print(f"  ✗ {v}")
    else:
        print("  ✓ All Raft safety properties hold")

    print("\nStep 4: Partition the leader")
    cluster.partition_node(leader_id)
    cluster.show_state()

    print("\nStep 5: New election (manually trigger on node-B)")
    non_partitioned = [n for nid, n in cluster.nodes.items() if nid != leader_id]
    non_partitioned[0].start_election()
    new_leader = cluster.get_leader()
    if new_leader:
        print(f"  ✓ New leader: {new_leader.node_id}")
        cluster.write("min_insync_replicas=2")
    else:
        print("  ✗ No leader (only 2 nodes visible — need quorum of 2, may still work)")
    cluster.show_state()

    print("\nStep 6: Heal partition and verify convergence")
    cluster.heal_partition(leader_id)
    cluster.show_state()

    violations = verify_raft_safety(cluster)
    if violations:
        for v in violations:
            print(f"  ✗ {v}")
    else:
        print("  ✓ All Raft safety properties hold after healing")
```

---

## 6. Visual Reference

### Raft Leader Election State Machine

```
        ┌────────────────────────────────────────────────┐
        │                    FOLLOWER                    │
        │  • Reset election timer on each heartbeat      │
        │  • Grant vote if candidate's log is current    │
        └────────────────────────────────────────────────┘
               │                              ▲
               │ Election timeout fires       │ Discover leader
               │ (no heartbeat from leader)   │ or higher term
               ▼                              │
        ┌────────────────────────────────────────────────┐
        │                   CANDIDATE                    │
        │  • Increment currentTerm                       │
        │  • Vote for self                               │
        │  • Send RequestVote to all peers               │
        │  • Wait for quorum of votes                    │
        └────────────────────────────────────────────────┘
               │                              
               │ Received votes from majority 
               ▼                              
        ┌────────────────────────────────────────────────┐
        │                    LEADER                      │
        │  • Send heartbeats to all followers            │
        │  • Accept client writes                        │
        │  • Replicate log entries (AppendEntries)       │
        │  • Commit when quorum acknowledges             │
        └────────────────────────────────────────────────┘

Terms (logical clock):
  Term 1: Node A is leader
  [Node A crashes]
  Term 2: Node B wins election (new term)
  [Network recovers, Node A reconnects]
  Node A: sees term 2 > term 1 → steps down → FOLLOWER
```

### Raft Log Replication

```
LEADER (node-A):
  Log: [T1: "topic=events"] [T1: "partitions=12"] [T2: "min.isr=2"]
       index:1                index:2                index:3
       ← committed up to 3 →

FOLLOWER (node-B):
  Log: [T1: "topic=events"] [T1: "partitions=12"] [T2: "min.isr=2"]
                                                    ← committed: 3

FOLLOWER (node-C) [was partitioned, just rejoined]:
  Old log: [T1: "topic=events"] [T1: "partitions=12"]
  
  Leader sends AppendEntries:
    prevLogIndex=2, prevLogTerm=1, entries=[{T2:"min.isr=2"}]
  
  Node-C: checks log[2] = T1:"partitions=12" ✓ matches prevLogTerm=1
  Appends entry. Log:
  [T1: "topic=events"] [T1: "partitions=12"] [T2: "min.isr=2"]
  
  Safety maintained: all three nodes agree on all committed entries.
```

### Quorum Sizing

```
Cluster size  Quorum needed  Failures tolerated
1             1              0
2             2              0   (not useful: any failure stops writes)
3             2              1   ← Most common for production
5             3              2   ← Used for higher fault tolerance
7             4              3   ← Large etcd clusters

Rule: quorum = ⌊N/2⌋ + 1
Rule: max failures = ⌊(N-1)/2⌋

Why odd sizes? N=4 tolerates 1 failure (quorum=3), same as N=3.
Extra node adds cost but no additional fault tolerance.
N=4: 4 nodes, need 3, tolerate 1.
N=5: 5 nodes, need 3, tolerate 2.
→ Always use odd cluster sizes.
```

---

## 7. Common Mistakes

**Mistake 1: Using an even-numbered Raft cluster.** A 4-node cluster tolerates 1 failure (needs quorum of 3, just like a 3-node cluster). But during a 2-node partition, neither island has quorum — both halves have exactly 2 nodes. A 3-node cluster in the same 2-node partition has one island with 2 (quorum) and one with 1. The 4-node cluster provides the same fault tolerance as a 3-node cluster at higher cost. Always use odd cluster sizes: 3, 5, or 7.

**Mistake 2: Confusing "leader elected" with "data committed."** A newly elected leader does not immediately have all committed data accessible. The leader's log is complete (election restriction guarantees this), but the commit index may be lower than the actual committed index until the new leader commits a new log entry in the current term. In Raft, the new leader's first act after winning the election is to send heartbeats that advance the commit index on all followers. Until that first heartbeat round completes, followers may not know the latest committed value.

**Mistake 3: Assuming Raft provides real-time consistency.** Raft provides linearizability — but only when reads go through the leader and the leader's lease is valid. A follower's read may return stale data because it hasn't received the latest committed log entry yet. In etcd, `--consistency` flag controls this: `--consistency=s` (serializable) reads from the local node (stale but fast); `--consistency=l` (linearizable) reads from the leader (always fresh but slower). In Kafka, consumer reads from follower replicas can be stale; `isolation.level=read_committed` reads only committed offsets from the leader.

**Mistake 4: Setting etcd election timeout too low in a shared environment.** etcd's default election timeout is 1000ms. In a Kubernetes cluster on VMs with noisy neighbors, GC pauses, or disk I/O spikes, a VM can be paused for > 200ms, causing heartbeat timeouts and spurious elections. Every spurious election is a write pause. For production etcd, run on dedicated nodes with fast NVMe storage and set `--heartbeat-interval=100ms` with `--election-timeout=1000ms` (10× ratio). Check `etcd_disk_wal_fsync_duration_seconds_bucket` — if P99 > 10ms, etcd is on too slow a disk.

**Mistake 5: Not accounting for Raft performance when sizing Kafka KRaft controllers.** KRaft metadata writes are Raft log replication. Every partition creation, ISR change, and broker registration is a Raft log entry. In large clusters with thousands of topic partitions and frequent ISR changes, the KRaft metadata log can receive thousands of writes per second. The KRaft controllers' disk must handle this write volume at sub-10ms fsync latency. Co-locating KRaft controllers with high-throughput Kafka brokers can starve the controller of disk I/O, causing election timeouts and metadata write stalls.

---

## 8. Production Failure Scenarios

### Scenario 1: etcd Split Brain Causes Kubernetes Chaos

**Symptoms:** A Kubernetes cluster undergoes a network partition that separates two etcd nodes from the third. The minority island (2 nodes) attempts to elect a leader. The majority island (1 node with quorum requirement of 2) cannot form quorum. Both halves stop accepting writes. Kubernetes kube-apiserver loses etcd connectivity and begins refusing all write operations. Running pods are unaffected (kubelet continues operating independently), but no new deployments, scaling operations, or ConfigMap updates are possible for 8 minutes until the network partition heals.

**Root cause:** The etcd cluster was 3 nodes spread across 3 availability zones. The multi-zone network experienced an asymmetric partition: AZ-1 ↔ AZ-2 communication failed, but both could still reach AZ-3. This meant no single island had quorum (AZ-1 alone = 1 node, AZ-2 alone = 1 node, AZ-3 alone = 1 node). Raft correctly refused writes on all three nodes since none had quorum.

**This is Raft CP behavior:** The cluster prioritized consistency (no split-brain, no divergent state) over availability (writes refused during partition).

**Lessons:** For critical Kubernetes clusters, use 5-node etcd to tolerate 2 simultaneous failures. Monitor `etcd_server_leader_changes_seen_total` — any increase during a production incident indicates a Raft election occurred.

### Scenario 2: Kafka KRaft Controller Election Causes Metadata Stall

**Symptoms:** During a routine broker restart, the Kafka KRaft active controller fails to send heartbeats for 4 seconds (the controller's JVM GC paused). The other KRaft controllers trigger an election. The election completes in 800ms. However, for 15 seconds after the election, all topic creation and partition assignment operations fail with `NOT_CONTROLLER` errors. Producers and consumers for existing topics are unaffected.

**Root cause:** After the new KRaft controller won the election, it needed to reload the metadata log into memory — all committed Raft log entries from the beginning of time. The cluster had been running for 18 months with 2,300 topics and 45,000 partitions. The metadata log contained 890,000 log entries. Loading these into memory took 14 seconds. During this initialization phase, the new controller rejected metadata write requests with `NOT_CONTROLLER` until it was ready.

**Fix:** Enable log compaction for the `__cluster_metadata` internal topic (KRaft automatically creates periodic snapshots). With snapshots, the new controller loads the most recent snapshot + delta entries, reducing initialization time from 14 seconds to under 2 seconds:

```bash
# Check KRaft snapshot configuration
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-name 1 \
  --describe | grep snapshot

# KRaft takes snapshots automatically, but ensure:
# metadata.max.retention.bytes is set to limit log growth
# metadata.max.retention.ms limits retention by time
```

### Scenario 3: ZooKeeper Quorum Loss Causes Kafka Controller Election Failure

**Symptoms:** (Legacy Kafka pre-KRaft scenario.) One of three ZooKeeper nodes suffers a disk failure and goes offline. ZooKeeper quorum is maintained with 2 of 3 nodes. However, 20 minutes later, a routine restart of the second ZooKeeper node for OS patching causes ZooKeeper quorum loss: only 1 of 3 nodes remains, below the quorum threshold of 2.

With ZooKeeper in quorum loss, Kafka's controller cannot renew its ZooKeeper lease. The controller resigns. Other brokers attempt to elect a new controller via ZooKeeper — but ZooKeeper is not accepting writes (quorum lost). No Kafka controller election can complete. All Kafka metadata operations stall: no new consumer groups can join, no ISR changes are processed, no new topics can be created.

**This is ZAB CP behavior:** ZooKeeper refused writes to prevent a split-brain scenario, sacrificing availability.

**Root cause:** Maintenance was performed on the second ZooKeeper node before the first failed node was replaced. Standard operational rule: never perform maintenance on a consensus cluster member while the cluster is already degraded (operating at N-1 nodes).

**Resolution:** Replace the disk-failed ZooKeeper node first, allow it to rejoin and sync, then proceed with the restart. Always ensure the consensus cluster is at full health before performing any maintenance that removes another member.

### Scenario 4: Raft Election Loop Due to Symmetric Partition

**Symptoms:** An etcd 3-node cluster enters a leadership election loop. Monitoring shows `etcd_server_leader_changes_seen_total` incrementing every 2-3 seconds. All three nodes are in CANDIDATE state simultaneously. No writes are succeeding. The cluster is completely unavailable for writes for 45 seconds.

**Root cause:** A network switch misconfiguration caused an asymmetric partition: each node can send to the others, but none can receive responses. Every node's election timeout fires. Every node starts a new election. Every node sends `RequestVote` to peers. No node receives vote responses (responses are dropped). Every node times out and starts another election with a higher term. This is a "symmetric vote split" exacerbated by low randomization in election timeouts — all nodes were using the same JVM build which produced the same random seed.

**Fix:** Ensure election timeout randomization uses a true random source (not a deterministic seed). etcd uses crypto/rand by default. Investigate the switch misconfiguration causing the asymmetric packet drop.

---

## 9. Performance and Tuning

### Raft Write Latency Components

A Raft write has three latency components:
1. **Leader disk fsync:** The leader flushes the log entry to disk before sending AppendEntries. Typically 1–10ms on NVMe SSD.
2. **Network RTT to followers:** AppendEntries sent to followers, response received. Within AZ: 0.5–2ms. Cross-AZ: 2–5ms.
3. **Follower disk fsync:** Followers must fsync before responding. Typically 1–10ms.

Total write latency = fsync_leader + RTT + fsync_follower. Cross-AZ: typically 5–20ms. Cross-region: 100ms+.

**For etcd:** The `--heartbeat-interval` and `--election-timeout` must be calibrated to disk and network latency:
- Heartbeat interval should be 10× the average fsync duration (so heartbeats complete before the next one is sent)
- Election timeout should be 10× the heartbeat interval (allows for transient delays before triggering election)
- Standard production values: `--heartbeat-interval=100ms`, `--election-timeout=1000ms`

**For Kafka KRaft:** KRaft controller performance is dominated by metadata write throughput. Clusters with frequent ISR changes (many brokers, high replication) generate more metadata writes. Monitor `kafka.controller:type=KafkaController,name=MetadataApplyLatencyMs` — values above 50ms indicate metadata processing backlog.

### Quorum Write Throughput

Raft processes log entries sequentially through the leader — writes cannot be parallelized across the Raft log. The throughput ceiling is: `max_throughput = 1 / leader_write_latency`. For a leader with 5ms round-trip to followers: at most 200 writes/second through a single Raft log.

Practical mitigation: **pipelining** (sending multiple AppendEntries without waiting for each to be acknowledged). Both etcd and Kafka KRaft pipeline AppendEntries for throughput. With pipelining depth D, throughput becomes `D / write_latency`.

For ZooKeeper: a well-tuned 3-node ZooKeeper cluster handles ~50,000 writes/second with pipelining. For most Kafka use cases, ZooKeeper was never the write bottleneck — Kafka partitions are the bottleneck long before ZooKeeper/KRaft metadata.

---

## 10. Interview Q&A

**Q1: Explain Raft leader election. What prevents two leaders from being elected simultaneously?**

Raft's election process begins when a follower's election timeout expires — it hasn't received a heartbeat from the leader and suspects the leader has failed. The follower increments its `currentTerm` (the monotonically increasing logical clock for the cluster), votes for itself, and sends `RequestVote` RPCs to all other nodes. Each node grants at most one vote per term, based on two conditions: it hasn't already voted in this term, and the candidate's log is at least as up-to-date as the voter's (the "election restriction").

Two leaders cannot be elected in the same term because a candidate needs votes from a majority (quorum) of nodes to win. In any cluster of N nodes, two different candidates would each need ⌊N/2⌋ + 1 votes — but there are only N votes total. By the pigeonhole principle, two candidates cannot both achieve quorum from the same set of N votes. Since each node votes at most once per term, only one candidate can win any given term.

The term counter prevents stale leaders: if an old leader reconnects after being partitioned, it receives AppendEntries responses with `term > currentTerm`. Upon seeing a higher term, it must step down to follower. The old leader's term is lower than the current term — it was incremented during the election that occurred while it was partitioned. The old leader recognizes it is no longer authoritative and yields to the new leader.

**Q2: What is the "election restriction" in Raft and why is it necessary for safety?**

The election restriction is the rule that a node only grants a vote if the candidate's log is at least as up-to-date as the voter's own log. "At least as up-to-date" is determined by comparing the last log entry: if the last entries have different terms, the one with the higher term is more up-to-date; if they have the same term, the one with the longer log is more up-to-date.

Without this restriction, a candidate with an empty or stale log could be elected leader, and would then potentially overwrite committed entries. Consider a 5-node cluster where the leader committed entry X to nodes 1, 2, and 3 (a quorum of 3). Then the leader crashes. Node 4 starts an election — it has an empty log but needs only 3 votes to win. It could win votes from nodes 1, 4, and 5. As leader, it would overwrite nodes 1, 2, and 3's logs with its empty log, erasing the committed entry X.

With the election restriction, node 4 cannot win votes from nodes 1, 2, or 3 — they all have entry X in their logs, making their logs more up-to-date than node 4's empty log. They deny the vote. Only candidates that have entry X can win the election, guaranteeing that the committed entry is preserved in any future leader's log. This is how Raft ensures Leader Completeness: all committed entries propagate to all future leaders.

**Q3: How does Kafka KRaft differ from the original ZooKeeper-based Kafka metadata management? What does this change operationally for data engineers?**

In the original Kafka architecture (pre-KRaft), all Kafka metadata — broker registration, topic configuration, partition assignments, ISR membership, and controller election — was stored in Apache ZooKeeper. Kafka ran a ZAB consensus cluster (ZooKeeper) alongside the Kafka broker cluster. Every metadata operation required a ZooKeeper write. The Kafka controller was elected via a ZooKeeper ephemeral node (`/controller`): the first broker to write that ZNode became the controller; if the controller crashed, its ZNode expired and another broker raced to claim it.

This created operational complexity: two separate systems to operate, monitor, and tune. ZooKeeper had its own quorum requirements, its own JVM heap tuning, and its own failure modes. The separation also created scaling limits: ZooKeeper stored all topic metadata in memory, limiting the number of topics and partitions a Kafka cluster could support to tens of thousands before ZooKeeper became a bottleneck.

KRaft replaces ZooKeeper with a Raft consensus log built into a subset of Kafka brokers (the KRaft controllers). Metadata is now a Raft-replicated append-only log within Kafka itself — using the same storage and replication machinery as Kafka topics. This eliminates the ZooKeeper dependency entirely.

Operationally for data engineers: there is no longer a separate ZooKeeper cluster to manage, monitor, or debug. The KRaft metadata log supports millions of partitions (versus ZooKeeper's practical limit of tens of thousands). Controller elections are faster (seconds rather than tens of seconds, since there is no ZooKeeper lease expiry wait). Monitoring is unified: KRaft metrics appear in the same Kafka JMX metrics stream, not in a separate ZooKeeper metrics endpoint.

---

## 11. Cross-Question Chain

**Interviewer:** You're designing a metadata store for a data catalog that needs to store dataset schemas and be strongly consistent. How does Raft factor into your design?

**Candidate:** A data catalog metadata store needs strong consistency — if one team writes a schema change, other teams reading immediately afterward must see it. Raft provides exactly this guarantee through linearizability: all writes go through the Raft leader, are committed to a quorum of replicas before ACKing, and all reads from the leader reflect committed state.

For the implementation, I'd use etcd (Kubernetes-native, well-operated) or a Raft-based key-value store as the metadata backend. Schema entries would be stored as key-value pairs: `dataset/{namespace}/{name}/schema` → Avro/JSON schema blob. The Raft quorum (3 or 5 nodes) ensures that even if one node fails, reads and writes continue with no data loss.

**Interviewer:** Your catalog team says they need < 10ms write latency globally. Is Raft compatible with that?

**Candidate:** Not with a globally distributed Raft cluster. A Raft write requires a round-trip to quorum — if the quorum members are in data centers with 80ms cross-region RTT, writes will take at least 80ms. Raft cannot overcome the speed of light.

There are two approaches. First, geo-partition the metadata: each region runs its own Raft cluster for its own datasets. Writes to us-east datasets go to the us-east Raft cluster (< 5ms within a region). Cross-region catalog browsing goes through asynchronous replication — slightly stale but fast. For the data catalog case this is often acceptable: you don't need globally synchronized schema views in real time.

Second, use a database like CockroachDB or Google Spanner, which uses Raft per-partition but routes writes to the partition's leader, which may be placed close to the primary write region. The write goes to the local leader and is synchronously replicated to quorum (within the same region), then asynchronously shipped to other regions. This gives < 10ms for writes to local data while providing eventual global consistency.

**Interviewer:** A Raft election occurs in your etcd cluster. Describe exactly what happens during the election and what impact it has on your data catalog.

**Candidate:** Here's the exact sequence. The etcd leader's heartbeat timer on follower nodes expires — typically after 1000ms of no heartbeat. One follower (whichever times out first due to randomized timeouts) transitions to CANDIDATE, increments its term, votes for itself, and sends `RequestVote` to the other two followers.

Each follower receives `RequestVote`. It checks: has it already voted in this term? Is the candidate's log at least as up-to-date? If both conditions are met, it grants the vote. The first candidate to receive 2 votes (including its own self-vote) wins — this typically takes one full network RTT, about 1–2ms within a single AZ.

The new leader immediately sends heartbeats to all followers, advancing their commit index and re-establishing leadership. From the client's perspective, writes to etcd time out or return `leader not found` for the duration of the election — typically 1–3 seconds total (election timeout + election RTT + leader initialization).

For the data catalog, this means: any catalog metadata write in flight during the election gets a timeout error. The client (catalog service) should retry with exponential backoff. Read operations that don't require linearizability can continue from followers. Schema lookup operations that tolerate up to a few seconds of stale data can continue without interruption. New schema creation requests fail and must be retried.

**Interviewer:** How would you monitor Raft health in this system to catch problems before they cause user-visible impact?

**Candidate:** Four key metrics, in priority order.

First: `etcd_server_leader_changes_seen_total`. This counter increments on every election. In a healthy cluster it changes only during planned maintenance. An unexpected increment means an unplanned election occurred — investigate the cause immediately: check node health, disk latency, and network quality.

Second: `etcd_disk_wal_fsync_duration_seconds_bucket`. The P99 latency for WAL disk fsyncs. This is the primary driver of Raft write latency. If P99 > 10ms, etcd is on too slow a disk or the disk is saturated. Alert at P99 > 25ms.

Third: `etcd_network_peer_round_trip_time_seconds_bucket`. P99 RTT between etcd peers. Within a single AZ this should be < 2ms. Rising RTT indicates network congestion or a slow node — early warning for an upcoming election.

Fourth: `etcd_server_proposals_pending`. The number of writes waiting to be committed. In steady state this should be near zero. A sustained non-zero value means the Raft log is saturated — the write throughput exceeds what the cluster can process. Alert if sustained > 10 for more than 30 seconds.

Combine these into a Grafana dashboard with a "Raft health" composite metric: red if any of these are in alarm state. The goal is to get paged by a rising `etcd_disk_wal_fsync_duration` before the cluster reaches the point of election timeouts and write failures.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What are the three properties a consensus algorithm must satisfy? | Agreement (all deciding nodes decide the same value), Validity (the decided value was proposed by some node), Termination (all non-failing nodes eventually decide). FLP result shows deterministic termination is impossible in asynchronous systems with failures. |
| 2 | What is a quorum in Raft, and why is majority used instead of all nodes? | Quorum = ⌊N/2⌋ + 1 (majority). All-or-nothing (all N) provides no fault tolerance. Majority allows ⌊(N-1)/2⌋ failures while ensuring any two quorums overlap by at least one node — the overlap ensures safety (no conflicting commits). |
| 3 | What are the three Raft node states? | FOLLOWER (default: receives heartbeats, grants votes), CANDIDATE (election in progress: requests votes), LEADER (won election: accepts writes, sends AppendEntries). |
| 4 | What triggers a Raft election? | A follower's randomized election timeout fires without receiving a heartbeat from the leader. Timeout is randomized (e.g., 150–300ms) to prevent split votes. |
| 5 | What is the Raft "election restriction" and what does it prevent? | A voter only grants a vote if the candidate's log is at least as up-to-date (higher last-entry term, or same term and longer log). Prevents electing a leader with a stale log that might overwrite committed entries. |
| 6 | What is the Raft "log matching property"? | If two log entries on different nodes have the same index and term, they are identical, and all preceding entries are also identical. Enforced by the prevLogIndex/prevLogTerm consistency check in AppendEntries. |
| 7 | How does Raft prevent a stale leader from causing split-brain after a partition heals? | When the old leader receives any message with term > currentTerm, it immediately steps down to FOLLOWER. The new leader increments the term during its election; the old leader's term is lower and it yields upon reconnection. |
| 8 | What is "Leader Completeness" in Raft? | If a log entry is committed in term T, it will be present in all leaders elected in terms > T. Guaranteed by the election restriction: any candidate winning a majority vote must have received a vote from a node that has the committed entry. |
| 9 | What is the difference between "committed" and "applied" in Raft? | Committed: the entry is on a quorum of nodes' logs (durable). Applied: the entry has been executed against the state machine. Applied always trails committed. An entry is safe to expose to clients only after it is committed. |
| 10 | Why does Raft use odd cluster sizes? | Even-sized clusters have the same fault tolerance as the next smaller odd cluster (N=4 tolerates 1 failure, same as N=3), but cost more. N=5 tolerates 2 failures; N=4 also tolerates only 1. Use 3, 5, or 7. |
| 11 | What is ZAB (ZooKeeper Atomic Broadcast) and how does it relate to Raft? | ZAB is the consensus protocol used by Apache ZooKeeper. Like Raft, it is leader-based with quorum replication. Unlike Raft, it has an explicit Synchronization phase for leader recovery and uses "epochs" instead of "terms." ZAB guarantees total order and primary order. |
| 12 | What replaces ZooKeeper in modern Kafka (KRaft)? | KRaft (Kafka Raft Metadata) — a Raft-based metadata log built into Kafka itself. KRaft controllers form a Raft quorum and store all cluster metadata (topic configs, partition assignments, ISR) as log entries. Eliminates the separate ZooKeeper cluster. |
| 13 | What is the FLP impossibility result? | Fischer, Lynch, Paterson (1985) proved that no deterministic consensus algorithm can guarantee termination (progress) in a fully asynchronous system with even a single crash failure. Paxos and Raft work around this using randomization and timeouts. |
| 14 | In Paxos Phase 1 (Prepare/Promise), why must a new proposer use the highest-numbered previously-accepted value? | If any acceptor in the Phase 1 quorum has already accepted a value in a previous round, that value may already be committed (if other acceptors also accepted it). The new proposer must propagate it rather than overriding it, preventing two different values from being committed. |
| 15 | What does etcd's `--consistency` flag control? | Read consistency: `--consistency=l` (linearizable) routes reads to the leader (always fresh, higher latency). `--consistency=s` (serializable) reads from the local node (may be stale but faster). |
| 16 | What etcd metric indicates Raft election instability? | `etcd_server_leader_changes_seen_total`. In a healthy cluster, this counter rarely increments. A sudden increase means elections are occurring — investigate disk latency, network RTT, and node health. |
| 17 | What is pipelining in Raft and why does it matter for throughput? | Pipelining sends multiple AppendEntries RPCs without waiting for each to be acknowledged. Without pipelining, throughput = 1 / write_latency. With pipelining depth D, throughput = D / write_latency. Both etcd and Kafka KRaft use pipelining. |
| 18 | What is the key constraint on etcd WAL fsync latency for stable Raft operation? | WAL fsync P99 should be < 10ms. At 10ms fsync with 100ms heartbeat interval, there is comfortable headroom (10 heartbeats per fsync). If fsync approaches the heartbeat interval, followers may not respond before the election timeout — causing spurious elections. |
| 19 | Why can't Raft commits be parallelized to increase throughput? | The Raft log is a total ordered sequence. Each entry must be committed in order — log index N cannot be committed until log index N-1 is committed. This sequential constraint means throughput is bounded by single-node write latency, not aggregate cluster throughput. |
| 20 | In Kafka KRaft, what types of operations generate metadata log entries? | Broker registration (`RegisterBrokerRecord`), topic creation (`TopicRecord`), partition assignment (`PartitionRecord`), ISR changes (`PartitionChangeRecord`), producer ID allocation (`ProducerIdsRecord`), and broker deregistration. Each is a Raft log entry replicated to all controllers. |

---

## 13. Further Reading

- **"In Search of an Understandable Consensus Algorithm" by Ongaro and Ousterhout (2014):** The Raft paper itself. Intentionally written for clarity — the contrast with Paxos papers is striking. Sections 5 (Raft basics) and 6 (leader election and log replication) are the core. Read this before any other Raft resource.
- **The Raft Visualization (https://raft.github.io):** Interactive browser-based animation of leader election and log replication. Watching the animation while reading the paper is the single most effective way to internalize Raft.
- **"Paxos Made Simple" by Leslie Lamport (2001):** Lamport's own accessible explanation of Paxos. Far more readable than the original 1989 paper. Understand why Paxos leaves Multi-Paxos details "for the reader" — this is what Raft was designed to resolve.
- **"Designing Data-Intensive Applications" by Martin Kleppmann, Chapter 9:** The best practitioners' introduction to consensus, linearizability, and distributed transactions. Explains the relationship between consensus, atomic broadcast, and total order broadcast.
- **etcd documentation — "FAQ" and "Tuning":** The etcd FAQ section on election timeouts, disk requirements, and `--heartbeat-interval` tuning reflects real-world lessons from running Raft in production. The disk performance section is particularly important.
- **Kafka KIP-595 (KRaft):** The Kafka Improvement Proposal that defined KRaft. Reading the motivation section explains exactly what problems ZooKeeper created for Kafka at scale, and how Raft-native metadata solves them.

---

## 14. Lab Exercises

**Exercise 1: Whiteboard Raft Election**

Without looking at notes, whiteboard a 5-node Raft cluster going through a leader election: the current leader (node-A) crashes, node-C times out first, draws the RequestVote messages, the vote responses, and the transition to LEADER. Then replay the scenario where node-B and node-C time out simultaneously — show the split vote, the term increment, and the second election that resolves it. Verify your whiteboard against the Raft visualization at raft.github.io.

**Exercise 2: Raft Simulator Exploration**

Using `raft_simulator.py` from the code toolkit: (1) Create a 3-node cluster, elect a leader, write 5 entries, and verify all nodes agree. (2) Partition the leader and trigger a new election. Attempt a write — observe it fails on the isolated old leader (no quorum). Observe the new leader accepts writes. (3) Heal the partition and verify that the old leader steps down upon seeing the higher term. Verify safety properties pass after healing.

**Exercise 3: etcd Raft Metric Observation**

In a Kubernetes cluster with Prometheus monitoring: (1) Record baseline values for `etcd_server_leader_changes_seen_total`, `etcd_disk_wal_fsync_duration_seconds_bucket`, and `etcd_network_peer_round_trip_time_seconds_bucket`. (2) Deliberately cause an etcd leader change by draining one node with `kubectl drain`. (3) Observe the `leader_changes` counter increment. (4) Time how long Kubernetes API writes are blocked during the election using a tight loop of `kubectl get pods --watch`. Document the total election duration.

**Exercise 4: Kafka KRaft Metadata Log Inspection**

On a KRaft-mode Kafka cluster: (1) Run `kafka-metadata-quorum.sh describe --status` and record the leader and epoch. (2) Create a new topic: `kafka-topics.sh --create --topic test-raft --partitions 3 --replication-factor 3`. (3) Run `kafka-metadata-quorum.sh describe --replication` and observe the new entries in the metadata log. (4) Use `kafka-dump-log.sh` to inspect the `__cluster_metadata` log and identify the `TopicRecord` and `PartitionRecord` entries created. (5) Stop and restart the leader controller; observe the epoch increment.

**Exercise 5: Raft Fault Tolerance Calculation**

Calculate fault tolerance for the following scenarios and determine the minimum cluster size: (1) You need to tolerate 2 simultaneous node failures; (2) You are running in 3 availability zones and need to tolerate a complete AZ failure; (3) You have a 4-node cluster and wonder if it's better than a 3-node cluster; (4) You want to add a node to a 3-node cluster to improve throughput — is going to 4 nodes beneficial? Explain why odd cluster sizes are always preferred.

---

## 15. Key Takeaways

Consensus algorithms solve the distributed agreement problem: how a set of nodes can agree on a single value (or ordered sequence of values) despite node failures and message loss. The quorum principle — any two quorums must overlap — is the mathematical foundation that makes safe consensus possible with failures.

Raft decomposes consensus into three independent sub-problems: leader election (a randomized timeout mechanism that elects one stable leader per term), log replication (a strong consistency protocol where entries are committed only after quorum acknowledgment), and safety (the election restriction that ensures new leaders always have all committed entries).

The election restriction is the most subtle and most critical part of Raft. It is why a candidate's log must be at least as up-to-date as a majority of nodes before it can win an election. Without it, a new leader could be elected with a stale log and would overwrite committed entries — violating the fundamental safety guarantee.

In production data engineering: etcd uses Raft for Kubernetes cluster state, Kafka KRaft uses Raft for metadata management, and CockroachDB uses Raft per-range for distributed SQL. When these systems experience election events — triggered by slow disks (high fsync latency), network partitions, or GC pauses — understanding the Raft state machine explains both the failure behavior and the recovery path.

The interview test: can you whiteboard Raft leader election from scratch in 5 minutes, including the election restriction and the term-based step-down rule? This is the concrete demonstration of distributed systems theory knowledge that Staff-level interviews require.

---

## 16. Connections to Other Modules

- **M37 — CAP Theorem:** Raft provides the mechanism behind CP systems. The quorum requirement is how CP systems enforce "at most one leader" — preventing split-brain — and how they guarantee that committed writes are preserved across leader changes.
- **M39 — Replication:** Raft log replication is a specific instance of leader-follower replication. M39 covers the replication landscape broadly (synchronous/asynchronous, multi-leader, leaderless); Raft is the synchronous leader-follower approach where the leader drives replication.
- **M40 — Failure Modes:** Split-brain is the failure Raft is designed to prevent. The quorum mechanism ensures that two nodes cannot simultaneously believe they are the leader for the same term. M40 covers the broader class of failure modes; Raft specifically addresses the split-brain failure.
- **DCS-KFK-101 — Kafka Internals:** Kafka KRaft is Raft applied to Kafka metadata management. The KRaft controller election process described in this module maps directly to the leader election mechanism in this module.
- **SYS-DST-102 M02 — Implementing a Raft Follower:** The next course implements a simplified Raft follower state machine in Python, building directly on the theory in this module.

---

## 17. Anti-Patterns

**Anti-pattern: Running etcd on shared storage with application workloads.** etcd's Raft performance is dominated by WAL fsync latency. Sharing a disk with high-throughput application I/O (Spark shuffle files, Kafka log segments) causes disk latency spikes that trigger heartbeat timeouts and spurious elections. Run etcd on dedicated NVMe SSDs (or dedicated cloud block storage with guaranteed IOPS).

**Anti-pattern: Setting `--election-timeout` without measuring actual `fsync` latency.** Election timeout must be well above the P99 disk fsync latency. If P99 fsync is 50ms and election timeout is 100ms, a single slow fsync can trigger a spurious election. Measure `etcd_disk_wal_fsync_duration_seconds_bucket{quantile="0.99"}` and set election timeout to at least 10× that value.

**Anti-pattern: Deploying a 2-node Raft cluster as "a primary and a backup."** A 2-node Raft cluster cannot tolerate any failures: quorum = 2, and with either node unavailable, writes are refused. A 2-node etcd is strictly worse than a single-node etcd in terms of availability — the extra node adds the risk of quorum loss without adding fault tolerance. Use 3-node or 5-node clusters.

**Anti-pattern: Not monitoring Raft elections as a production signal.** A Raft election is always a sign that something went wrong — the leader became unavailable, even briefly. In a healthy cluster, leader changes should be near-zero except during planned maintenance. Each unexpected election means there was a write pause, and the cluster barely avoided a longer outage. Treat each unexpected election as an incident worth investigating.

---

## 18. Tools Reference

| Tool | Purpose | Key Usage |
|------|---------|-----------|
| `kafka-metadata-quorum.sh describe --status` | Kafka KRaft leader and epoch | Check active controller, term, and follower lag |
| `kafka-metadata-quorum.sh describe --replication` | Raft follower lag | Per-follower log lag in entries and ms |
| `kafka-dump-log.sh --files __cluster_metadata` | Inspect KRaft metadata log | View Raft log entries for topic/partition/broker records |
| `etcdctl endpoint status --write-out=table` | etcd Raft status | Shows leader, term, and commit index per node |
| `etcdctl member list` | etcd cluster membership | Lists all Raft members |
| `curl etcd:2379/metrics \| grep etcd_server_leader` | etcd election count | `etcd_server_leader_changes_seen_total` |
| `curl etcd:2379/metrics \| grep etcd_disk_wal` | etcd disk fsync latency | P99 WAL fsync duration — key Raft health metric |
| `raft_simulator.py` | Simulate Raft elections | Observe election, log replication, partition behavior |
| `verify_raft_safety()` | Check Raft invariants | Automated safety property verification |
| raft.github.io | Interactive Raft visualization | Browser animation of leader election and log replication |

---

## 19. Glossary

**AppendEntries:** The Raft RPC used by the leader to replicate log entries to followers. Also used as a heartbeat (empty entries) to prevent election timeouts. Contains prevLogIndex/prevLogTerm for consistency checking.

**Candidate:** A Raft node state in which the node is requesting votes to become leader. Transitions from FOLLOWER when election timeout fires.

**Commit Index:** The highest log index known to be committed (safely replicated to a quorum). Entries at or below commitIndex are safe to apply to the state machine.

**currentTerm:** The current term number, a monotonically increasing integer used as Raft's logical clock. Incremented on each election. A node that sees a higher term immediately updates its term and steps down to FOLLOWER.

**Election Restriction:** The rule that a voter only grants a vote if the candidate's log is at least as up-to-date as the voter's. Prevents electing a leader with a stale log that might overwrite committed entries.

**Election Timeout:** The randomized timer that triggers a Raft election when no heartbeat is received from the leader. Randomization prevents split votes.

**etcd:** A distributed key-value store using Raft consensus. Used by Kubernetes to store all cluster state. Typical production deployment: 3 or 5 nodes.

**FLP Impossibility:** Fischer, Lynch, Paterson (1985) — no deterministic consensus algorithm can guarantee termination in an asynchronous system with even one crash failure. Practical algorithms use randomization and timeouts.

**Follower:** The default Raft node state. Receives heartbeats and log entries from the leader, grants votes to candidates.

**KRaft (Kafka Raft):** Kafka's built-in Raft metadata system replacing ZooKeeper. KRaft controllers form a Raft quorum and store all cluster metadata as Raft log entries.

**Leader:** The Raft node state where the node is the current leader for its term. Accepts all writes, replicates to followers, commits when quorum acknowledges.

**Log Matching Property:** In Raft: if two log entries on different nodes share the same index and term, they are identical, and so are all preceding entries.

**Paxos:** A consensus algorithm by Leslie Lamport (1989). Theoretically elegant but leaves implementation details unspecified, making practical use difficult. Two-phase: Prepare/Promise then Accept/Accepted.

**Quorum:** Majority of nodes in a Raft cluster: ⌊N/2⌋ + 1. Any two quorums must overlap by at least one node, ensuring any committed entry is seen by any new leader.

**Raft:** A consensus algorithm by Ongaro and Ousterhout (2014), designed for understandability. Three sub-problems: leader election, log replication, safety. Used in etcd, CockroachDB, TiKV, Kafka KRaft.

**RequestVote:** The Raft RPC used by candidates to request votes during an election. Includes the candidate's term and log state for the election restriction check.

**Term:** A monotonically increasing integer in Raft, equivalent to an epoch or logical clock for the cluster. Each election starts a new term. A node steps down when it sees a higher term.

**WAL (Write-Ahead Log):** The durable log in Raft nodes (and databases generally) where log entries are written before being acknowledged. WAL fsync latency is the dominant factor in Raft write latency.

**ZAB (ZooKeeper Atomic Broadcast):** The consensus protocol used by Apache ZooKeeper. Leader-based with quorum replication, similar to Raft but with an explicit Synchronization phase and "epochs" instead of terms.

---

## 20. Self-Assessment

1. A 3-node Raft cluster has nodes A (leader, term=5), B (follower), and C (follower). Node A crashes. Describe step by step what happens: which node starts the election first, what messages are exchanged, and what conditions must be met for the new leader to be elected.
2. What is the election restriction? Give a concrete example of what could go wrong if it didn't exist.
3. Why does Raft use randomized election timeouts? What failure mode would result from deterministic timeouts?
4. In a 5-node Raft cluster, the leader commits entry X to nodes 1, 2, and 3 (a quorum). The leader then crashes. Can node 5 (which doesn't have entry X) become the new leader? Why or why not?
5. What does `etcd_server_leader_changes_seen_total` measure, and what does an unexpected increment tell you about the cluster?
6. Describe the Raft log consistency check in `AppendEntries`. What happens when a follower's log doesn't match at `prevLogIndex`/`prevLogTerm`? How does the leader recover?
7. Why is an even-numbered Raft cluster strictly worse than the next smaller odd-numbered cluster?
8. A Kafka KRaft cluster has 3 controllers. The active controller (broker 1) experiences a 30-second GC pause. Describe what happens from the moment the pause starts until the cluster returns to normal operation.
9. Explain the difference between Paxos and Raft. Why did the Raft authors create a new algorithm rather than implement Multi-Paxos?
10. What etcd operational requirement follows from the WAL fsync latency requirement? If `etcd_disk_wal_fsync_duration_seconds{quantile="0.99"}` = 50ms, what should the `--election-timeout` be set to?

---

## 21. Module Summary

Consensus algorithms solve the most fundamental problem in distributed systems: how do nodes agree on a shared value when messages can be lost and nodes can crash? The answer, in both Paxos and Raft, is a leader-based quorum protocol. A single leader receives all writes. It replicates them to a majority (quorum) of nodes before committing. The quorum overlap property — any two quorums share at least one node — ensures that any new leader must have seen all committed entries, making the system safe.

Raft's contribution was decomposing this into three understandable sub-problems. Leader election uses randomized timeouts and a majority vote: the first candidate to collect quorum votes in a given term wins. The election restriction — only nodes with up-to-date logs can win — is the safety guarantee that prevents new leaders from overwriting committed data. Log replication is a push-based protocol where the leader sends `AppendEntries` to all followers, with a consistency check that ensures the follower's log matches at the point being extended.

For data engineers, the critical systems built on Raft or similar consensus are: etcd (Kubernetes cluster state — understanding Raft explains why etcd elections cause Kubernetes write pauses), Kafka KRaft (metadata management — understanding Raft explains KRaft controller election behavior), and ZooKeeper/ZAB (legacy Kafka metadata, HBase, — understanding ZAB explains ZooKeeper quorum loss behavior). When these systems experience problems, the diagnostic path runs through Raft: check the leader, check the term, check the disk fsync latency, check for spurious elections.

The interview expectation: whiteboard Raft leader election from memory in 5 minutes. Draw the three states, the election timeout trigger, the RequestVote exchange, the quorum vote count, and the term-based step-down rule. This is the concrete technical depth that distinguishes a Senior from a Staff data engineer in distributed systems theory interviews.

The next module — M39: Replication — covers the broader landscape of replication strategies: synchronous vs asynchronous, leader-follower (which Raft is a specific instance of), multi-leader, and leaderless approaches. Each has distinct consistency guarantees, latency characteristics, and failure modes that map directly to data systems in production.
