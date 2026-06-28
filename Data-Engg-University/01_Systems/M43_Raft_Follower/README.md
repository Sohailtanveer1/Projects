# M43: Implementing a Raft Follower

**Course:** SYS-DST-102 — Distributed Systems in Practice  
**Module:** 02 of 05  
**Global Module ID:** M43  
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

M38 described Raft as a consensus algorithm with three node states, an election restriction, and a Log Matching Property. That description is necessary but not sufficient. Reading a description of Raft and implementing a Raft follower are different cognitive activities — the implementation forces precision at every decision point that a description leaves ambiguous.

What election timeout should a follower use? The paper says "a random duration in the range [150ms, 300ms]" — but what is the consequence of choosing the wrong range in practice? What exactly should happen when a follower receives an AppendEntries with a prevLogTerm that doesn't match its local log? The paper gives a precise rule, but implementing it requires understanding why the rule exists. What is the difference between a follower committing an entry and applying it? These questions have exact answers; finding those answers by implementing the code is the only way to truly know them.

This module implements the full Raft follower state machine in Python. The implementation covers: election timeout and the transition from FOLLOWER to CANDIDATE, vote granting with the election restriction, AppendEntries processing with consistency checks, log truncation on conflict detection, and commit index advancement. We do not implement the leader (M38's `RaftCluster` covered that with a full cluster) — this module focuses on the follower's perspective: what does a node do when it is following a leader, when the leader goes silent, and when it needs to seek votes?

The implementation is the educational tool. By the time you finish it, Raft's safety guarantees will feel inevitable rather than arbitrary — because you will have seen exactly which design choices produce which properties.

---

## 2. Mental Model

### The Raft Follower as a State Machine

A Raft follower is a state machine with three states (FOLLOWER, CANDIDATE, LEADER) and a small set of inputs:
- AppendEntries RPC (from the leader): heartbeat or log entries
- RequestVote RPC (from a candidate): vote request
- Election timeout fires: no heartbeat received in time

The follower spends most of its time in FOLLOWER state, processing AppendEntries. When it doesn't hear from the leader for `election_timeout` milliseconds, it transitions to CANDIDATE, increments its term, votes for itself, and sends RequestVote RPCs to all other nodes.

### The Log as the Source of Truth

A Raft node's log is the authoritative record of everything it knows. The log has two important indices:
- **commitIndex:** the highest log entry the node knows is committed (safe to apply to the state machine)
- **lastApplied:** the highest log entry actually applied to the state machine

The invariant: `lastApplied <= commitIndex <= len(log)`. The follower applies entries between `lastApplied` and `commitIndex` to its state machine.

The leader advances followers' `commitIndex` by including its `leaderCommit` in each AppendEntries call. The follower sets its `commitIndex = min(leaderCommit, index_of_last_new_entry)`.

### Why the Election Restriction Exists

The election restriction (a candidate can only win if its log is at least as up-to-date as the majority's logs) ensures that a newly elected leader always has all committed entries. If voters could elect a candidate with a stale log, that candidate would become leader and overwrite committed entries — violating safety. The restriction prevents this: any candidate that gets a majority of votes has a log at least as up-to-date as any node in that majority, which by definition includes the committed entries (committed on a majority).

---

## 3. Core Concepts

### 3.1 Persistent vs Volatile State

Raft requires certain state to survive crashes (written to stable storage before responding to any RPC):

**Persistent (must survive crash):**
- `currentTerm`: latest term seen. Ensures a node never reverts to an older term after restart.
- `votedFor`: candidateId voted for in current term (or null). Ensures a node never grants two votes in the same term after restart.
- `log[]`: the complete log. A node must not lose committed entries.

**Volatile (safe to lose on crash):**
- `commitIndex`: can be re-derived from AppendEntries (leader re-sends it)
- `lastApplied`: always starts at 0 on restart; entries are re-applied from the log
- Leader-only state (`nextIndex[]`, `matchIndex[]`): re-derived after election

The critical insight: if `votedFor` were volatile, a node could crash after voting and then vote again for a different candidate in the same term after restart — violating the "at most one vote per term" invariant.

### 3.2 AppendEntries Consistency Check

When a follower receives AppendEntries, it performs a two-step check:

**Step 1 — Term check:** `if request.term < currentTerm → reject`. A stale leader is rejected.

**Step 2 — Log consistency check:**
- `prevLogIndex`: the index of the log entry immediately before the new entries
- `prevLogTerm`: the term of that entry
- The follower checks: does my log contain an entry at `prevLogIndex` with term `prevLogTerm`?
- If NO: reject with `success=false`. The leader will decrement `nextIndex[follower]` and retry with an earlier prevLogIndex (or use fast backtracking).
- If YES: the follower can safely append the new entries. If there are conflicting entries (same index, different term), truncate the log from the conflict point and replace with the new entries.

This check implements the Log Matching Property: if the leader's log matches the follower's log at `prevLogIndex/prevLogTerm`, the Log Matching Property guarantees that all entries before `prevLogIndex` are also identical.

### 3.3 Log Truncation on Conflict

When a follower's log has conflicting entries at a given index (different term from the leader's entry), the follower must truncate its log from the conflict point:

```
Follower's log:    [T1][T1][T1][T2][T2]    (index 1-5)
                                    ^ conflict at index 4: follower has T2, leader has T3

Leader sends:      entries at index 4-6 (term T3)
                   prevLogIndex=3, prevLogTerm=T1 (matches)

Follower action:
  1. Check index 3: term T1 matches → consistency check passes
  2. Check existing entry at index 4: my term is T2, leader says T3 → CONFLICT
  3. Truncate log from index 4 onwards: log = [T1][T1][T1]
  4. Append new entries: log = [T1][T1][T1][T3][T3][T3]
```

Truncation discards entries the follower had but the leader doesn't. These entries were never committed (if they were committed, the Log Matching Property guarantees the leader would also have them). Truncation is always safe.

### 3.4 Election Timeout Design

The election timeout must satisfy: `election_timeout > max_RTT + leader_processing_time`. If the timeout is too short, followers will time out even when the leader is healthy, causing unnecessary elections. If too long, recovery after a real leader failure is slow.

The randomization is critical: if all followers used the same election timeout, they would all start elections simultaneously, split the vote repeatedly, and never elect a leader (livelock). Randomized timeouts ensure that — with high probability — one follower's timer fires before others, giving it a head start in collecting votes.

Typical production values: `election_timeout ∈ [150ms, 300ms]`, heartbeat interval 50ms (must be < election_timeout). etcd uses 100ms heartbeat, 500ms election timeout. KRaft uses 2000ms election timeout.

---

## 4. Hands-On Walkthrough

### 4.1 Inspecting etcd Raft State

```bash
# etcd exposes Raft state via its gRPC API and endpoint status
# Install etcdctl if not present
ETCD_VER=v3.5.12
curl -L https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz | tar xz

# Check cluster members and their Raft state
etcdctl --endpoints=http://localhost:2379 endpoint status --write-out=table
# Output:
# +──────────────────────+──────────────+─────────+──────────+────────────────┬──────────────────+
# | ENDPOINT             | ID           | VERSION | DB SIZE | IS LEADER       | RAFT TERM | RAFT INDEX |
# +──────────────────────+──────────────+─────────+──────────+────────────────┬──────────────────+
# | http://localhost:2379| 8e9e05c52164 | 3.5.12  | 20 kB   | true            | 2         | 10         |
# | http://localhost:2380| 9b54ec40c4b3 | 3.5.12  | 20 kB   | false           | 2         | 10         |
# | http://localhost:2381| daf3fd52e3bb | 3.5.12  | 20 kB   | false           | 2         | 10         |
# IS LEADER: which node is leader
# RAFT TERM: current term number
# RAFT INDEX: last log index (commitIndex for leader, or last_applied for followers)

# Watch for leader changes (term increments)
watch -n 0.5 'etcdctl endpoint status --write-out=table'

# Check Prometheus metrics for Raft health
curl http://localhost:2381/metrics | grep etcd_server_leader_changes_seen_total
# etcd_server_leader_changes_seen_total 0  (non-zero = elections occurred)

curl http://localhost:2381/metrics | grep etcd_server_proposals
# etcd_server_proposals_committed_total 10
# etcd_server_proposals_applied_total 10    (should match committed)
# etcd_server_proposals_pending 0           (non-zero = commit stalled)
# etcd_server_proposals_failed_total 0
```

### 4.2 Simulating a Raft Election

```python
# Using M38's RaftCluster to observe elections from the outside
from raft_simulator import RaftCluster  # from M38

cluster = RaftCluster(n_nodes=5)
cluster.elect_initial_leader()
cluster.show_state()

# Partition the leader
leader = cluster.find_leader()
cluster.partition_node(leader)

# After election_timeout, a new leader should emerge
import time
time.sleep(0.5)   # Wait for election
cluster.show_state()
# Should show a new leader with term = old_term + 1
```

---

## 5. Code Toolkit

```python
#!/usr/bin/env python3
"""
raft_follower.py

A complete implementation of the Raft follower state machine in Python.

This implements:
  - All three node states: FOLLOWER, CANDIDATE, LEADER
  - Full AppendEntries RPC handling (heartbeat + log replication)
  - Full RequestVote RPC handling with election restriction
  - Election timeout with randomization
  - Log consistency check (prevLogIndex/prevLogTerm)
  - Log truncation on conflict
  - CommitIndex and lastApplied tracking
  - Persistent state simulation (term, votedFor, log)
  - Applied state machine (key-value store)

Intentionally NOT implemented (kept minimal to focus on the follower logic):
  - Network transport (RPCs are direct method calls on node objects)
  - Leader-side nextIndex/matchIndex management
  - Log compaction / snapshots

No external dependencies.
"""

import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ─── Raft State Enums ────────────────────────────────────────────────────────────

class NodeState(Enum):
    FOLLOWER  = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER    = "LEADER"


# ─── Log Entry ───────────────────────────────────────────────────────────────────

@dataclass
class LogEntry:
    """A single Raft log entry."""
    term:    int     # The term when this entry was received by the leader
    index:   int     # 1-based position in the log
    command: str     # The state machine command (e.g., "SET x=5")


# ─── RPC Request/Response Types ──────────────────────────────────────────────────

@dataclass
class AppendEntriesRequest:
    term:           int             # Leader's current term
    leader_id:      str             # Leader's node ID (so followers can redirect clients)
    prev_log_index: int             # Index of log entry immediately preceding new ones
    prev_log_term:  int             # Term of prevLogIndex entry
    entries:        list[LogEntry]  # Log entries to store (empty for heartbeat)
    leader_commit:  int             # Leader's commitIndex


@dataclass
class AppendEntriesResponse:
    term:        int   # Follower's current term (for leader to update if stale)
    success:     bool  # True if follower matched prevLogIndex/prevLogTerm
    match_index: int   # Highest log index follower has appended (for leader's matchIndex)
    # Fast backtracking optimization:
    conflict_term:  Optional[int] = None  # Term of conflicting entry (if success=False)
    conflict_index: Optional[int] = None  # First index of conflict_term (if success=False)


@dataclass
class RequestVoteRequest:
    term:           int  # Candidate's term
    candidate_id:   str  # Candidate requesting vote
    last_log_index: int  # Index of candidate's last log entry
    last_log_term:  int  # Term of candidate's last log entry


@dataclass
class RequestVoteResponse:
    term:        int   # Responder's current term (for candidate to update if stale)
    vote_granted: bool  # True means candidate received vote


# ─── Raft Node (Follower State Machine) ─────────────────────────────────────────

class RaftNode:
    """
    A single Raft node. Implements the complete Raft consensus algorithm
    from the follower's perspective, including CANDIDATE and LEADER states.

    Raft paper: https://raft.github.io/raft.pdf
    """

    ELECTION_TIMEOUT_MIN_MS = 150
    ELECTION_TIMEOUT_MAX_MS = 300
    HEARTBEAT_INTERVAL_MS   = 50

    def __init__(self, node_id: str, peers: list['RaftNode'] = None):
        self.node_id = node_id
        self.peers: list['RaftNode'] = peers or []

        # ── Persistent state (must survive crashes) ──────────────────────────────
        # In production: written to disk before responding to any RPC
        self._current_term: int = 0
        self._voted_for:    Optional[str] = None
        self._log:          list[LogEntry] = []   # 0-indexed internally, 1-indexed in Raft paper

        # ── Volatile state ───────────────────────────────────────────────────────
        self._commit_index: int = 0   # Highest known committed entry (1-indexed, 0=none)
        self._last_applied: int = 0   # Highest applied entry (1-indexed, 0=none)

        # ── State machine (applied log entries) ──────────────────────────────────
        self._state_machine: dict[str, str] = {}

        # ── Node state ───────────────────────────────────────────────────────────
        self._state: NodeState = NodeState.FOLLOWER
        self._leader_id: Optional[str] = None
        self._lock = threading.Lock()

        # ── Election timeout ─────────────────────────────────────────────────────
        self._last_heartbeat = time.monotonic()
        self._election_timeout_ms = self._random_election_timeout()
        self._running = False
        self._timer_thread: Optional[threading.Thread] = None

        # ── Leader-only state ────────────────────────────────────────────────────
        # nextIndex[peer]: next log index to send to peer
        # matchIndex[peer]: highest log index confirmed replicated to peer
        self._next_index:  dict[str, int] = {}
        self._match_index: dict[str, int] = {}

        # ── Diagnostics ──────────────────────────────────────────────────────────
        self._events: list[str] = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────────

    def start(self):
        """Start the node's background timer thread."""
        self._running = True
        self._timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
        self._timer_thread.start()

    def stop(self):
        self._running = False

    def set_peers(self, peers: list['RaftNode']):
        """Set or update the peer list."""
        with self._lock:
            self.peers = [p for p in peers if p.node_id != self.node_id]

    # ── Timer Loop ────────────────────────────────────────────────────────────────

    def _timer_loop(self):
        """
        Background thread: fires the election timeout if no heartbeat received.
        Also drives leader heartbeats when this node is the leader.
        """
        while self._running:
            time.sleep(0.01)  # 10ms tick
            with self._lock:
                state = self._state

            if state == NodeState.FOLLOWER or state == NodeState.CANDIDATE:
                elapsed_ms = (time.monotonic() - self._last_heartbeat) * 1000
                if elapsed_ms >= self._election_timeout_ms:
                    self._start_election()
            elif state == NodeState.LEADER:
                self._send_heartbeats()
                time.sleep(self.HEARTBEAT_INTERVAL_MS / 1000)

    def _random_election_timeout(self) -> float:
        """
        Random election timeout in [ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS].
        Randomization is critical: prevents simultaneous elections (split votes).
        """
        return random.uniform(
            self.ELECTION_TIMEOUT_MIN_MS,
            self.ELECTION_TIMEOUT_MAX_MS,
        )

    # ── Election ──────────────────────────────────────────────────────────────────

    def _start_election(self):
        """
        Transition to CANDIDATE state and start a new election.
        Called when election timeout fires.
        """
        with self._lock:
            # Increment current term — persistent state change
            self._current_term += 1
            term = self._current_term

            # Vote for self
            self._voted_for = self.node_id
            self._state = NodeState.CANDIDATE
            self._leader_id = None
            self._last_heartbeat = time.monotonic()
            self._election_timeout_ms = self._random_election_timeout()

            # Build vote request
            last_log_index = len(self._log)
            last_log_term = self._log[-1].term if self._log else 0

            self._log_event(f"Starting election for term {term} "
                           f"(last_log_index={last_log_index}, last_log_term={last_log_term})")

            peers = list(self.peers)

        # Collect votes (outside the lock to avoid deadlock with peer locks)
        votes_granted = 1   # Vote for self
        request = RequestVoteRequest(
            term=term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

        for peer in peers:
            try:
                response = peer.handle_request_vote(request)
                with self._lock:
                    # If we see a higher term, immediately step down
                    if response.term > self._current_term:
                        self._step_down(response.term)
                        return
                    if response.vote_granted:
                        votes_granted += 1
            except Exception:
                pass   # Peer unreachable — just skip

        # Check if we won the election
        with self._lock:
            if self._state != NodeState.CANDIDATE or self._current_term != term:
                return   # State changed while we were collecting votes

            quorum = len(self.peers) // 2 + 1
            if votes_granted >= quorum:
                self._become_leader()
            else:
                self._log_event(f"Lost election in term {term}: "
                               f"{votes_granted}/{len(self.peers)+1} votes")

    # ── RequestVote RPC Handler ───────────────────────────────────────────────────

    def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        """
        Handle a RequestVote RPC from a candidate.

        Vote is granted if ALL of the following:
          1. Candidate's term >= our current term
          2. We haven't voted for anyone else in this term (or already voted for this candidate)
          3. Candidate's log is at least as up-to-date as ours (election restriction)

        The election restriction (§5.4.1 of the Raft paper):
          Candidate's log is "at least as up-to-date" if:
          - Its last entry's term > our last entry's term, OR
          - Its last entry's term == our last entry's term AND its log is >= our log length
        """
        with self._lock:
            # If request has a newer term, update our term and revert to follower
            if request.term > self._current_term:
                self._step_down(request.term)

            # Reject if candidate's term is stale
            if request.term < self._current_term:
                return RequestVoteResponse(term=self._current_term, vote_granted=False)

            # Check if we can vote for this candidate
            already_voted_for_someone_else = (
                self._voted_for is not None and
                self._voted_for != request.candidate_id
            )
            if already_voted_for_someone_else:
                self._log_event(f"Rejected vote for {request.candidate_id} in term {request.term}: "
                               f"already voted for {self._voted_for}")
                return RequestVoteResponse(term=self._current_term, vote_granted=False)

            # Election restriction: is candidate's log at least as up-to-date as ours?
            my_last_log_index = len(self._log)
            my_last_log_term = self._log[-1].term if self._log else 0

            candidate_log_up_to_date = (
                request.last_log_term > my_last_log_term or
                (request.last_log_term == my_last_log_term and
                 request.last_log_index >= my_last_log_index)
            )

            if not candidate_log_up_to_date:
                self._log_event(f"Rejected vote for {request.candidate_id}: "
                               f"log not up-to-date "
                               f"(my: term={my_last_log_term} idx={my_last_log_index}, "
                               f"candidate: term={request.last_log_term} idx={request.last_log_index})")
                return RequestVoteResponse(term=self._current_term, vote_granted=False)

            # Grant vote — persistent state change
            self._voted_for = request.candidate_id
            self._last_heartbeat = time.monotonic()   # Reset election timeout on vote grant

            self._log_event(f"Granted vote to {request.candidate_id} in term {request.term}")
            return RequestVoteResponse(term=self._current_term, vote_granted=True)

    # ── AppendEntries RPC Handler ─────────────────────────────────────────────────

    def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """
        Handle an AppendEntries RPC from the leader.

        This implements the 5 AppendEntries rules from the Raft paper (§5.3):
        1. Reply false if term < currentTerm (§5.1)
        2. Reply false if log doesn't contain entry at prevLogIndex with term prevLogTerm (§5.3)
        3. If existing entry conflicts with new entry (same index, different terms),
           delete existing entry and all that follow it (§5.3)
        4. Append any new entries not already in the log
        5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, lastNewEntryIndex)
        """
        with self._lock:
            # Rule 1: Reject if leader's term is stale
            if request.term < self._current_term:
                return AppendEntriesResponse(
                    term=self._current_term,
                    success=False,
                    match_index=0,
                )

            # Seeing a valid AppendEntries means there's an active leader
            # Update term if necessary and revert to follower
            if request.term > self._current_term:
                self._step_down(request.term)
            elif self._state == NodeState.CANDIDATE:
                # Another node won the election for this term
                self._state = NodeState.FOLLOWER

            self._leader_id = request.leader_id
            self._last_heartbeat = time.monotonic()   # Reset election timeout

            # Rule 2: Log consistency check
            if request.prev_log_index > 0:
                # We need an entry at prev_log_index
                if len(self._log) < request.prev_log_index:
                    # We don't have the entry at prev_log_index at all
                    self._log_event(f"AE reject: log too short "
                                   f"(len={len(self._log)}, prev_log_index={request.prev_log_index})")
                    return AppendEntriesResponse(
                        term=self._current_term,
                        success=False,
                        match_index=len(self._log),
                        # Fast backtracking: tell leader our log length so it skips ahead
                        conflict_term=None,
                        conflict_index=len(self._log) + 1,
                    )

                # We have the entry — check its term
                # Log is 0-indexed internally, but Raft uses 1-indexed
                our_entry = self._log[request.prev_log_index - 1]
                if our_entry.term != request.prev_log_term:
                    # Term mismatch: log divergence
                    conflict_term = our_entry.term
                    # Find the first index in our log with conflict_term
                    # (helps leader skip the entire conflicting term at once)
                    conflict_index = next(
                        (e.index for e in self._log if e.term == conflict_term),
                        request.prev_log_index,
                    )
                    self._log_event(f"AE reject: term mismatch at index {request.prev_log_index} "
                                   f"(expected={request.prev_log_term}, have={our_entry.term})")
                    return AppendEntriesResponse(
                        term=self._current_term,
                        success=False,
                        match_index=request.prev_log_index - 1,
                        conflict_term=conflict_term,
                        conflict_index=conflict_index,
                    )

            # Rule 3: Find conflicts and truncate
            # Iterate through new entries and check against our log
            for i, new_entry in enumerate(request.entries):
                log_index = request.prev_log_index + i + 1   # 1-indexed
                if log_index <= len(self._log):
                    existing = self._log[log_index - 1]
                    if existing.term != new_entry.term:
                        # Conflict: truncate our log from here onwards
                        self._log_event(f"Conflict at index {log_index}: "
                                       f"truncating from index {log_index} "
                                       f"(had term={existing.term}, new term={new_entry.term})")
                        self._log = self._log[:log_index - 1]
                        break
                else:
                    # No conflict (new entry extends our log) — stop checking
                    break

            # Rule 4: Append any new entries not already in the log
            for new_entry in request.entries:
                if new_entry.index > len(self._log):
                    self._log.append(new_entry)
                    self._log_event(f"Appended entry at index {new_entry.index} "
                                   f"term={new_entry.term}: {new_entry.command!r}")

            # Rule 5: Update commitIndex
            if request.leader_commit > self._commit_index:
                last_new_entry_index = (
                    request.entries[-1].index if request.entries else request.prev_log_index
                )
                old_commit = self._commit_index
                self._commit_index = min(request.leader_commit, last_new_entry_index)
                if self._commit_index > old_commit:
                    self._log_event(f"CommitIndex advanced from {old_commit} "
                                   f"to {self._commit_index}")

            # Apply newly committed entries to state machine
            self._apply_committed_entries()

            return AppendEntriesResponse(
                term=self._current_term,
                success=True,
                match_index=len(self._log),
            )

    # ── State Machine Application ─────────────────────────────────────────────────

    def _apply_committed_entries(self):
        """
        Apply all committed-but-not-yet-applied entries to the state machine.
        Must be called while holding the lock.
        """
        while self._last_applied < self._commit_index:
            self._last_applied += 1
            entry = self._log[self._last_applied - 1]
            self._execute_command(entry.command)

    def _execute_command(self, command: str):
        """
        Execute a state machine command. Parses simple "SET key=value" and "DEL key" format.
        In production: this is where the database write would happen.
        """
        try:
            if command.startswith("SET "):
                _, rest = command.split(" ", 1)
                key, value = rest.split("=", 1)
                self._state_machine[key.strip()] = value.strip()
            elif command.startswith("DEL "):
                _, key = command.split(" ", 1)
                self._state_machine.pop(key.strip(), None)
            elif command.startswith("NOOP"):
                pass   # Leader's initial no-op entry
        except Exception:
            pass   # Malformed command — in production: this would be an error

    # ── Leader Logic ──────────────────────────────────────────────────────────────

    def _become_leader(self):
        """
        Transition to LEADER state. Must be called while holding the lock.
        Initializes nextIndex and matchIndex for all peers.
        """
        self._state = NodeState.LEADER
        self._leader_id = self.node_id

        # Leader initialization: nextIndex = last log index + 1 for all peers
        # matchIndex = 0 for all peers (nothing confirmed replicated yet)
        for peer in self.peers:
            self._next_index[peer.node_id] = len(self._log) + 1
            self._match_index[peer.node_id] = 0

        self._log_event(f"Became LEADER in term {self._current_term} "
                       f"(log length={len(self._log)})")

        # Leaders must append a NOOP entry to commit all previous entries
        # (Raft's "leader completeness" requires this)
        noop = LogEntry(
            term=self._current_term,
            index=len(self._log) + 1,
            command="NOOP",
        )
        self._log.append(noop)

    def _send_heartbeats(self):
        """
        Send AppendEntries heartbeats to all peers (leader only).
        Also replicates any uncommitted log entries.
        """
        with self._lock:
            if self._state != NodeState.LEADER:
                return
            term = self._current_term
            commit_index = self._commit_index
            peers = list(self.peers)

        for peer in peers:
            try:
                with self._lock:
                    if self._state != NodeState.LEADER:
                        return
                    next_idx = self._next_index.get(peer.node_id, 1)
                    prev_log_index = next_idx - 1
                    prev_log_term = (
                        self._log[prev_log_index - 1].term
                        if prev_log_index > 0 and prev_log_index <= len(self._log)
                        else 0
                    )
                    # Send all entries from next_idx onwards
                    entries_to_send = [
                        e for e in self._log
                        if e.index >= next_idx
                    ]

                    request = AppendEntriesRequest(
                        term=term,
                        leader_id=self.node_id,
                        prev_log_index=prev_log_index,
                        prev_log_term=prev_log_term,
                        entries=entries_to_send,
                        leader_commit=commit_index,
                    )

                response = peer.handle_append_entries(request)

                with self._lock:
                    if response.term > self._current_term:
                        self._step_down(response.term)
                        return

                    if response.success:
                        self._match_index[peer.node_id] = response.match_index
                        self._next_index[peer.node_id] = response.match_index + 1
                        # Advance commit index if a majority has matched
                        self._advance_commit_index()
                    else:
                        # Fast backtracking: use conflict_index if provided
                        if response.conflict_index:
                            self._next_index[peer.node_id] = response.conflict_index
                        else:
                            self._next_index[peer.node_id] = max(
                                1, self._next_index.get(peer.node_id, 1) - 1
                            )
            except Exception:
                pass   # Peer unreachable

    def _advance_commit_index(self):
        """
        Leader advances commitIndex when a log entry is replicated on a majority.
        Only entries from the current term can be committed this way (§5.4.2).
        Must be called while holding the lock.
        """
        # Find the highest index replicated on a majority
        n = len(self._log)
        while n > self._commit_index:
            if self._log[n - 1].term == self._current_term:
                # Count replicas (leader itself + peers with matchIndex >= n)
                replicas = 1 + sum(
                    1 for mid in self._match_index.values() if mid >= n
                )
                quorum = (len(self.peers) + 1) // 2 + 1
                if replicas >= quorum:
                    self._commit_index = n
                    self._log_event(f"Committed up to index {n} "
                                   f"(replicated on {replicas}/{len(self.peers)+1} nodes)")
                    self._apply_committed_entries()
                    break
            n -= 1

    # ── Step Down ────────────────────────────────────────────────────────────────

    def _step_down(self, new_term: int):
        """
        Step down to FOLLOWER for a higher term. Persistent state change.
        Must be called while holding the lock.
        """
        if new_term > self._current_term:
            self._log_event(f"Stepping down from {self._state.value}: "
                           f"term {self._current_term} → {new_term}")
            self._current_term = new_term
            self._voted_for = None   # Reset vote for new term
            self._state = NodeState.FOLLOWER
            self._leader_id = None

    # ── Read Interface ────────────────────────────────────────────────────────────

    def read(self, key: str) -> Optional[str]:
        """Read from state machine (may return stale data if not the leader)."""
        with self._lock:
            return self._state_machine.get(key)

    def client_write(self, command: str) -> bool:
        """
        Accept a client write (leader only).
        Returns True if appended to log (not yet committed).
        """
        with self._lock:
            if self._state != NodeState.LEADER:
                return False
            entry = LogEntry(
                term=self._current_term,
                index=len(self._log) + 1,
                command=command,
            )
            self._log.append(entry)
            self._log_event(f"Leader appended client write at index {entry.index}: {command!r}")
            return True

    # ── Status and Diagnostics ───────────────────────────────────────────────────

    def get_status(self) -> dict:
        with self._lock:
            return {
                'node_id':      self.node_id,
                'state':        self._state.value,
                'term':         self._current_term,
                'voted_for':    self._voted_for,
                'leader_id':    self._leader_id,
                'log_length':   len(self._log),
                'commit_index': self._commit_index,
                'last_applied': self._last_applied,
                'state_machine': dict(self._state_machine),
            }

    def get_log(self) -> list[dict]:
        with self._lock:
            return [
                {'index': e.index, 'term': e.term, 'command': e.command}
                for e in self._log
            ]

    def _log_event(self, msg: str):
        event = f"[{self.node_id}] {msg}"
        self._events.append(event)
        print(f"  {event}")


# ─── Raft Cluster ─────────────────────────────────────────────────────────────────

class RaftCluster:
    """
    A cluster of RaftNodes with direct method-call "network" simulation.
    Used for demonstration and testing.
    """

    def __init__(self, n_nodes: int = 3):
        self.nodes = {
            f"node-{i}": RaftNode(f"node-{i}")
            for i in range(n_nodes)
        }
        # Wire up peer references
        all_nodes = list(self.nodes.values())
        for node in all_nodes:
            node.set_peers(all_nodes)

    def start(self):
        for node in self.nodes.values():
            node.start()

    def stop(self):
        for node in self.nodes.values():
            node.stop()

    def find_leader(self) -> Optional[str]:
        """Return the node_id of the current leader, or None."""
        for nid, node in self.nodes.items():
            if node.get_status()['state'] == 'LEADER':
                return nid
        return None

    def write(self, command: str, wait_s: float = 2.0) -> bool:
        """Write a command to the leader, wait for commit."""
        leader_id = self.find_leader()
        if not leader_id:
            return False
        leader = self.nodes[leader_id]
        result = leader.client_write(command)
        if not result:
            return False
        # Wait for commit (simplified: wait for state machine to have the value)
        time.sleep(wait_s * 0.1)
        return True

    def show_state(self):
        print("\n  ─── Cluster State ───")
        for nid, node in self.nodes.items():
            s = node.get_status()
            print(f"  {nid}: state={s['state']:<10} term={s['term']} "
                  f"log_len={s['log_length']} commit={s['commit_index']} "
                  f"applied={s['last_applied']} leader={s['leader_id']}")

    def show_logs(self):
        print("\n  ─── Node Logs ───")
        for nid, node in self.nodes.items():
            log = node.get_log()
            entries_str = ", ".join(
                f"[{e['index']}:T{e['term']}:{e['command']!r}]"
                for e in log
            )
            print(f"  {nid}: [{entries_str}]")

    def verify_safety(self) -> bool:
        """
        Verify core Raft safety properties:
        1. At most one leader per term
        2. Log Matching Property: committed entries match across all nodes
        3. State Machine Safety: all nodes have same state at same last_applied
        """
        print("\n  ─── Safety Verification ───")
        all_ok = True

        # Property 1: At most one leader per term
        leaders_by_term: dict[int, list[str]] = {}
        for nid, node in self.nodes.items():
            s = node.get_status()
            if s['state'] == 'LEADER':
                t = s['term']
                leaders_by_term.setdefault(t, []).append(nid)

        for term, leaders in leaders_by_term.items():
            if len(leaders) > 1:
                print(f"  ✗ SAFETY VIOLATION: Multiple leaders in term {term}: {leaders}")
                all_ok = False
            else:
                print(f"  ✓ At most one leader per term (term {term}: {leaders[0]})")

        # Property 2: Log Matching — committed entries should be identical
        min_commit = min(
            node.get_status()['commit_index'] for node in self.nodes.values()
        )
        logs_match = True
        reference = list(self.nodes.values())[0].get_log()
        for nid, node in self.nodes.items():
            log = node.get_log()
            for i in range(min_commit):
                if i >= len(reference) or i >= len(log):
                    continue
                if reference[i] != log[i]:
                    print(f"  ✗ LOG MISMATCH at index {i+1}: "
                          f"reference={reference[i]} vs {nid}={log[i]}")
                    logs_match = False
                    all_ok = False

        if logs_match:
            print(f"  ✓ Log Matching Property holds for committed entries (up to index {min_commit})")

        # Property 3: State Machine Safety — all applied entries produce same state
        applied_states = {}
        for nid, node in self.nodes.items():
            s = node.get_status()
            if s['last_applied'] > 0:
                applied_states[nid] = s['state_machine']

        if len(set(str(s) for s in applied_states.values())) <= 1:
            print(f"  ✓ State Machine Safety: all applied states identical")
        else:
            print(f"  ✗ STATE MACHINE DIVERGENCE: {applied_states}")
            all_ok = False

        return all_ok


# ─── Demo ─────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Raft Follower State Machine Demo ===\n")

    cluster = RaftCluster(n_nodes=3)
    cluster.start()

    # ── 1. Wait for leader election ──────────────────────────────────────────────
    print("─── Phase 1: Initial Leader Election ───")
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        if cluster.find_leader():
            break
        time.sleep(0.1)

    cluster.show_state()
    leader_id = cluster.find_leader()
    if leader_id:
        print(f"\n  Leader elected: {leader_id}")
    else:
        print("  ERROR: No leader elected within 3 seconds")

    # ── 2. Write some entries ────────────────────────────────────────────────────
    print("\n─── Phase 2: Client Writes ───")
    for i in range(5):
        cluster.write(f"SET key-{i}=value-{i}", wait_s=0.5)

    time.sleep(0.5)   # Allow replication
    cluster.show_state()
    cluster.show_logs()

    # ── 3. Verify safety properties ─────────────────────────────────────────────
    print("\n─── Phase 3: Safety Verification ───")
    cluster.verify_safety()

    # ── 4. Inspect AppendEntries details ────────────────────────────────────────
    print("\n─── Phase 4: Manual AppendEntries ───")
    # Find a follower and send a valid AppendEntries directly
    follower_id = next(
        nid for nid, n in cluster.nodes.items()
        if n.get_status()['state'] == 'FOLLOWER'
    )
    follower = cluster.nodes[follower_id]
    print(f"\n  Sending AppendEntries to {follower_id}...")

    # Valid heartbeat
    with follower._lock:
        current_term = follower._current_term
        log_len = len(follower._log)
        commit = follower._commit_index

    request = AppendEntriesRequest(
        term=current_term,
        leader_id=leader_id,
        prev_log_index=log_len,
        prev_log_term=follower.get_log()[-1]['term'] if follower.get_log() else 0,
        entries=[],   # Heartbeat
        leader_commit=commit,
    )
    response = follower.handle_append_entries(request)
    print(f"  Heartbeat response: success={response.success} match_index={response.match_index}")

    # Invalid AppendEntries (wrong prevLogTerm — simulates stale leader)
    bad_request = AppendEntriesRequest(
        term=current_term,
        leader_id="stale-leader",
        prev_log_index=1,
        prev_log_term=99,   # Wrong term
        entries=[],
        leader_commit=0,
    )
    bad_response = follower.handle_append_entries(bad_request)
    print(f"  Bad request response: success={bad_response.success} "
          f"conflict_term={bad_response.conflict_term} "
          f"conflict_index={bad_response.conflict_index}")

    cluster.stop()
    print("\n✓ Raft follower state machine demo complete")
```

---

## 6. Visual Reference

### Raft State Machine Transitions

```
                  discovers current leader
                  or grants vote to candidate
         ┌──────────────────────────────────────┐
         │                                       ↓
    [CANDIDATE] ──── wins election ────→ [LEADER]
         ↑                                    │
         │                              discovers node
    election                              with higher
    timeout                                  term
         │                                    │
         └──── [FOLLOWER] ────────────────────┘
                    ↑
                    │
              discovers node
              with higher term
              OR receives valid
              AppendEntries/RequestVote

Starting state: FOLLOWER
```

### AppendEntries Consistency Check (§5.3)

```
Leader log:    [1:T1][2:T1][3:T2][4:T2][5:T3]
                                   ↑ leader_commit=4

AppendEntries to follower:
  prev_log_index=4, prev_log_term=T2
  entries=[LogEntry(index=5, term=T3, command="SET x=5")]
  leader_commit=4

Follower log (case A — in sync):
  [1:T1][2:T1][3:T2][4:T2]  ← has entry at index 4 with term T2 ✓
  Consistency check: PASS
  Action: append entry 5
  Update commitIndex = min(4, 5) = 4
  Apply entries 1-4 to state machine

Follower log (case B — missing entries):
  [1:T1][2:T1]  ← log length 2, prev_log_index=4 doesn't exist
  Consistency check: FAIL (log too short)
  Response: success=False, conflict_index=3
  Action: leader decrements nextIndex to 3 and retries

Follower log (case C — conflicting term):
  [1:T1][2:T1][3:T1][4:T1]  ← has entry at index 4 but with T1, not T2
  Consistency check: FAIL (term mismatch at index 4)
  Response: success=False, conflict_term=T1, conflict_index=3 (first T1 index)
  Action: leader skips to index 3 and retries
  (Fast backtracking: skips entire T1 range instead of decrementing one by one)
```

### Election Restriction — Why It Ensures Safety

```
Scenario: 5 nodes, leader crashes after partial replication

Before crash:
  Node-0 (leader): log=[1,2,3,4,5]  committed up to 3
  Node-1:          log=[1,2,3,4]    (got 4 but not 5)
  Node-2:          log=[1,2,3]      (only in sync through 3)
  Node-3:          log=[1,2,3]
  Node-4:          log=[1,2]        (lagging)

Node-0 crashes. Which node can be elected leader?

Node-1 requests votes:
  last_log_index=4, last_log_term=T1
  Node-2 sees: my last=(3,T1), candidate's last=(4,T1) → T1==T1, 4>3 → GRANT
  Node-3: same → GRANT
  Node-4: my last=(2,T1), candidate's last=(4,T1) → GRANT
  Node-1 gets 4 votes (including self) → WINS → safe (has entries 1-4)

Node-4 requests votes first:
  last_log_index=2, last_log_term=T1
  Node-1 sees: my last=(4,T1), candidate's last=(2,T1) → T1==T1, 2<4 → REJECT
  Node-2: my last=(3,T1), candidate's last=(2,T1) → 2<3 → REJECT
  Node-3: my last=(3,T1) → REJECT
  Node-4 gets only 1 vote (self) → LOSES → correct (would discard committed entry 3)

The election restriction ensures: any node that wins has a log at least as
complete as any node in its majority → it has all committed entries.
```

---

## 7. Common Mistakes

**Mistake 1: Not updating currentTerm when receiving a message with a higher term.** The Raft paper is explicit (§5.1): "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower." A node that doesn't update its term when it sees a higher term can make decisions based on stale term information. For example: a candidate that ignores a RequestVoteResponse with a higher term might continue its election campaign even though a new leader has been elected with a newer term. Every RPC handler must check the term first.

**Mistake 2: Allowing a vote for a candidate with a stale log because "it's in the same term."** The election restriction is separate from the term check. A candidate can have the same (or higher) term as the voter and still have a stale log — for example, if it was isolated during a burst of writes and missed many entries. Both conditions must be checked independently: (1) candidate's term >= our term, AND (2) candidate's log is at least as up-to-date as ours.

**Mistake 3: Setting commitIndex = leaderCommit without checking the last new entry index.** Rule 5 of AppendEntries says: `commitIndex = min(leaderCommit, indexOfLastNewEntry)`. The follower may not have received all of the leader's entries yet. If the follower sets `commitIndex = leaderCommit` unconditionally, it would advance its commit index to entries it hasn't received — then when it tries to apply those entries, they don't exist in its log.

**Mistake 4: Committing entries from previous terms based solely on replication count.** Raft §5.4.2: a leader can only commit entries from its current term by counting replicas. Entries from previous terms are committed indirectly — when a current-term entry is committed, all preceding entries are committed too (by the Log Matching Property). A leader that commits a previous-term entry by quorum count can lead to a scenario where a future leader overwrites that entry. The `_advance_commit_index` implementation checks `self._log[n-1].term == self._current_term` for exactly this reason.

---

## 8. Production Failure Scenarios

### Scenario 1: Split Vote Livelock

**Symptoms:** A Kafka KRaft cluster (3 controllers) loses its leader. All three controllers enter candidate state simultaneously. Controller-0 votes for itself, Controller-1 votes for itself, Controller-2 votes for itself. Each gets only 1/3 votes. No candidate wins. After their election timeouts expire, they all start new elections simultaneously again. The cluster remains leaderless for 30 seconds.

**Root cause:** All three controller election timeouts were configured with the same value (or very narrow range). When the leader failed, all three timed out at the same millisecond, started elections simultaneously, and split the vote perfectly (each voted for itself before seeing another candidate's RequestVote). The fixed timeout range meant they timed out in unison on subsequent rounds too.

**Why randomization prevents this:** With `election_timeout ∈ [150ms, 300ms]`, the probability that all three nodes time out within a 1ms window is `(1/150)^2 ≈ 0.004%` — negligible. In practice, one node consistently times out first, wins the election before the others even start candidacy.

**Fix:** Ensure `election_timeout_min` and `election_timeout_max` differ by at least a factor of 2. KRaft default: 1000-2000ms. etcd default: 500-2500ms. If experiencing repeated split votes, check that randomization is truly random (not seeded with a fixed seed, which can produce identical sequences across nodes).

### Scenario 2: Stale Leader Confused by Network Partition

**Symptoms:** A 5-node Raft cluster experiences a network partition: [node-0 (leader), node-1] are isolated from [node-2, node-3, node-4]. The majority side (node-2/3/4) elects a new leader (node-2, term=5). Meanwhile, node-0 continues accepting writes and returning success to clients — even though it cannot replicate to a majority. 45 seconds later, the partition heals. Node-0's writes are discarded when it sees node-2's higher term.

**Root cause:** node-0 accepted client writes without waiting for quorum replication. It continued believing it was the leader (correctly, by Raft's rules — there's no mechanism for a partitioned leader to "know" it's cut off) and returned success to clients whose writes were then lost.

**Prevention:** Two mechanisms. First, clients should use linearizable reads (read from leader only, with a quorum check). Second, leaders should implement a "leader lease" check: if the leader has not received acknowledgment from a majority within `election_timeout`, it should stop accepting writes. This is a common extension to basic Raft (used in etcd's ReadIndex and LeaseRead mechanisms). Basic Raft doesn't prevent this — application-level solutions are required.

---

## 9. Performance and Tuning

### Election Timeout Tuning

```
Election timeout range selection:
  Min constraint: election_timeout_min > max(network_RTT + leader_processing_time)
    If network RTT = 10ms, leader processing = 5ms:
    election_timeout_min > 15ms → use 150ms (10× safety margin)

  Max constraint: election_timeout_max < desired_failover_SLA
    If SLA requires leader failover within 500ms:
    election_timeout_max < 500ms → use 300ms

  Randomization ratio: max/min >= 2 (to prevent consistent split votes)
    150ms/300ms: ratio = 2 ✓
    etcd 500ms/2500ms: ratio = 5 ✓

Heartbeat interval:
  Must be << election_timeout_min (to prevent spurious elections)
  heartbeat_interval = election_timeout_min / 5 is a common heuristic
  150ms timeout → 30ms heartbeat interval
  etcd: 100ms heartbeat, 500ms election timeout (ratio = 5)
```

### Log Append Throughput

```
# Raft throughput is bounded by leader's disk fsync latency
# (same constraint as WAL in M42 — Raft entries are the WAL)

# With single-entry writes and NVMe (2ms P99 fsync):
max_writes_per_second = 1000ms / 2ms = 500 writes/second

# With batching (group commit):
# Leader batches 50 entries, fsyncs once every 10ms
max_throughput = 50 entries / 10ms = 5,000 entries/second

# etcd implementation:
# Uses batch fsync with wal.fdatasync() every 10ms by default
# Achieves ~5,000 writes/second on standard NVMe SSD

# KRaft implementation:
# Configurable: log.flush.interval.messages and log.flush.interval.ms
# Default: flush every 1 message (each write) → throughput limited by disk
# Tuning: flush every 1000ms → higher throughput, brief durability window
```

---

## 10. Interview Q&A

**Q1: Explain the Raft election restriction. Why is it necessary, and what exactly does "at least as up-to-date" mean?**

The election restriction prevents a candidate with a stale log from becoming leader, which would cause the new leader to overwrite committed entries — the fundamental safety violation Raft must prevent. If any node could become leader regardless of log state, the new leader might be missing entries that were committed (replicated to a majority) during the previous leader's tenure. When the new leader started replicating its log to followers, those followers would truncate their logs to match the new leader, discarding the committed entries.

"At least as up-to-date" has a precise, two-part definition from the Raft paper. Candidate log C is at least as up-to-date as voter log V if: (1) C's last entry has a higher term than V's last entry — a higher term indicates more recent leadership, or (2) C's last entry has the same term as V's last entry but C's log is at least as long as V's log — same term but longer means more entries, all from the same leader.

The reason a higher term takes precedence over length: a log with even one entry from a more recent term is definitely more complete than a longer log from an older term, because the higher-term entry was written by a leader that had already seen and superseded the older term's entries.

**Q2: Why can a Raft leader only commit entries from its current term? What goes wrong if it commits previous-term entries directly?**

This is §5.4.2 of the Raft paper and one of the subtler points. A leader can only mark an entry as committed once it has been replicated to a majority AND that entry's term matches the current term. Entries from previous terms can only be committed indirectly: when a current-term entry is committed, the Log Matching Property guarantees all preceding entries in the log are also committed.

The problem with committing previous-term entries directly by quorum count: imagine a 5-node cluster. Leader-A (term 1) replicates entry X to nodes 1 and 2, then crashes. Leader-B (term 2) is elected — it doesn't have entry X in its log. Now imagine Leader-A restarts and claims X is "committed" because it was on a quorum (nodes A, 1, 2). But Leader-B is the current leader with a higher term, and nodes 3, 4, and possibly 1, 2 will follow Leader-B. Leader-B's log doesn't include X. If Leader-B replicates its own entries and commits them, it will overwrite X on nodes 1 and 2 when it brings their logs into sync with its own.

The Raft solution: Leader-A cannot claim X is committed unless A is still the current leader AND X's term matches A's current term. Once a higher-term leader exists, Leader-A cannot commit anything — it will step down the moment it sees Leader-B's term in any RPC.

---

## 11. Cross-Question Chain

**Interviewer:** What is the minimum number of nodes needed in a Raft cluster to tolerate one node failure?

**Candidate:** Three nodes. A Raft cluster of N nodes can tolerate ⌊N/2⌋ failures while still forming a majority. With N=3: majority is 2, tolerates 1 failure. With N=2: majority is 2, tolerates 0 failures — losing one node makes a majority impossible. So three is the minimum for fault tolerance.

**Interviewer:** Why not use two nodes then, with a tie-breaker rule?

**Candidate:** A tie-breaker rule for two-node clusters can't solve the fundamental problem: the two nodes cannot distinguish between "the other node crashed" and "we are partitioned from each other but both still running." If node-A becomes the tie-breaker and accepts writes when node-B is unreachable, both A and B might be simultaneously active (split-brain) — both believing they are the authoritative leader. Any "tie-breaker" rule that allows a single node to form a majority of one violates consensus safety. The quorum requirement (strictly more than half) exists specifically to prevent two nodes from simultaneously believing they have authority.

**Interviewer:** Your company is considering a 5-node Raft cluster for etcd. What are the failure tolerance and cost implications compared to a 3-node cluster?

**Candidate:** A 5-node cluster tolerates 2 simultaneous failures vs 1 for a 3-node cluster. The quorum for 5-node is 3, meaning the cluster stays available as long as 3 nodes are alive. This is useful for multi-AZ deployments: with 5 nodes across 3 AZs (2+2+1), losing any single AZ still leaves 3 or 4 nodes available — the cluster survives. A 3-node cluster across 3 AZs (1+1+1) loses availability if any one AZ goes down (loses majority).

The cost is write latency and throughput. Each write must be replicated to a quorum before committing: for 5 nodes, the leader waits for 2 followers to confirm (the leader itself + 2 = quorum of 3). For 3 nodes, it waits for 1 follower. The commit latency is bounded by the second-slowest node in the quorum (the slower of the 2 followers for a 5-node cluster). If followers have variable latency due to cross-AZ RTT or load, the tail latency for commits is higher with 5 nodes. For etcd specifically, KRaft write latency with 5 nodes across AZs is typically 15-30ms P99 vs 8-15ms for 3 nodes in the same AZ.

**Interviewer:** How does the election restriction protect the cluster during the failover when 2 nodes have failed?

**Candidate:** With 5 nodes and 2 failures, the remaining 3 nodes form a majority and must elect a new leader from among themselves. The 3 surviving nodes will have varying log states — the one with the most up-to-date log (highest last-log-term, then longest log within that term) will win the election because the other two will reject a vote request from a candidate with a less complete log.

This ensures the new leader has all committed entries: any entry that was committed required acknowledgment from a quorum of 3 nodes. Of the 5 nodes, 3 are still alive — the quorum that committed any given entry overlaps with the 3 surviving nodes by at least 1 node. That 1 node has the committed entry and will reject vote requests from candidates that don't. So the eventual winner always has all committed entries.

**Interviewer:** What if the 2 failed nodes were the ones that received the latest committed entries?

**Candidate:** If the 2 failed nodes were in the commit quorum for entry X, then at least one of the 3 surviving nodes was also in that quorum (since quorum = 3 and total survivors = 3, we know all committed entries were replicated to all 3 survivors — wait, let me be precise: a quorum of 3 from 5 means entry X was on at least 3 nodes. The 2 that failed might be 2 of those 3 — but then at least 1 of the 3 surviving nodes had the entry. Actually: 5 nodes, quorum=3 for commit, 2 failed. Of the 3 that had entry X, at least 1 must be among the 3 survivors: 3 had X + 2 didn't = 5 total. If both failed nodes were in the "had X" group, that's 2 out of 3 that had X — 1 survivor still has it. The election restriction ensures that survivor doesn't vote for a candidate missing X (the surviving node's log is at least as up-to-date as any candidate). So the winner must have X.

---

## 12. Flashcards

| # | Front | Back |
|---|-------|-------|
| 1 | What are the three Raft node states and when does each transition occur? | FOLLOWER → CANDIDATE: election timeout fires (no heartbeat). CANDIDATE → LEADER: wins majority vote. CANDIDATE/LEADER → FOLLOWER: sees message with higher term. LEADER → FOLLOWER: sees message with higher term. |
| 2 | What persistent state must survive a crash in Raft? Why? | `currentTerm` (must not revert to old term), `votedFor` (must not vote twice in same term), `log` (must not lose committed entries). All three must be fsynced before responding to RPCs. |
| 3 | What is the election restriction? State it precisely. | A voter grants a vote to candidate C only if C's log is "at least as up-to-date" as the voter's: C's last entry has a higher term than voter's, OR same term and C's log is at least as long. Prevents leaders with stale logs. |
| 4 | What does the AppendEntries consistency check verify? | That the follower's log contains an entry at `prevLogIndex` with term `prevLogTerm`. This inductively ensures all preceding entries match (Log Matching Property). |
| 5 | What happens when a follower detects a conflicting log entry? | Truncates its log from the conflict point onwards, then appends the leader's entries. Truncation is safe because conflicting entries were never committed (committed entries always match by Log Matching Property). |
| 6 | Why can a leader only commit entries from its current term? | Previous-term entries committed by quorum count can be overwritten by a future leader if the original leader crashes before a higher-term entry confirms the commit. Raft §5.4.2 prevents this by only using current-term entries as commit triggers. |
| 7 | What is commitIndex vs lastApplied? | `commitIndex`: highest entry known to be committed (safe to apply). `lastApplied`: highest entry actually applied to state machine. Invariant: `lastApplied ≤ commitIndex`. Applied entries are never un-applied. |
| 8 | Why must election timeouts be randomized? | Without randomization, all followers timeout simultaneously, start elections at the same time, and split votes repeatedly (livelock). Randomization ensures one follower typically starts its election before others, preventing split votes. |
| 9 | What is the quorum formula for a Raft cluster of N nodes? | Quorum = ⌊N/2⌋ + 1. Fault tolerance = ⌊N/2⌋ (nodes that can fail). N=3: quorum=2, tolerates 1. N=5: quorum=3, tolerates 2. Always use odd N to maximize fault tolerance per node count. |
| 10 | What does a Raft leader do immediately after winning election? | Appends a no-op (NOOP) entry to its log and commits it. This is required to commit all entries from previous terms without waiting for new writes — the no-op entry's commit propagates commitment of all preceding entries via Log Matching. |
| 11 | What is fast backtracking in AppendEntries? | An optimization where the follower includes `conflict_term` and `conflict_index` in a rejection response. The leader uses this to jump back to the first index of the conflict term rather than decrementing nextIndex one at a time. Reduces the number of roundtrips to catch up a lagging follower. |
| 12 | What property guarantees that a newly elected Raft leader has all committed entries? | The election restriction (combined with majority vote). Committed entries are on a majority of nodes. The majority that voted for the new leader overlaps with the majority that committed any given entry by at least one node — that node rejects any candidate missing the committed entry. |
| 13 | What is a split-vote and how does Raft handle it? | When multiple candidates each get fewer votes than a quorum (vote split among candidates). Raft handles it by having each candidate wait for a new randomized election timeout before starting a new election. Randomization makes it unlikely that candidates timeout simultaneously again. |
| 14 | In etcd, what metrics indicate Raft problems? | `etcd_server_leader_changes_seen_total` > 0: frequent elections. `etcd_server_proposals_pending` > 0: commit stalled. `etcd_disk_wal_fsync_duration_seconds{quantile="0.99"}` > 10ms: WAL performance issue. |
| 15 | What does a Raft follower do when it receives an AppendEntries from a lower-term leader? | Rejects it immediately (returns `success=false` with its current term). The sending leader will see the higher term in the response and step down to follower. |

---

## 13. Further Reading

- **Raft paper: "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout, 2014):** The primary source. §5.3 (Log Replication), §5.4 (Safety), and Figure 2 (State Machine Summary) are the essential sections. This module implements exactly what Figure 2 specifies.
- **Raft visualization (https://raft.github.io/):** An interactive animated visualization of leader election and log replication. Invaluable for building intuition before implementation.
- **etcd source: `etcd-io/etcd` → `raft/raft.go`:** The production Go implementation of Raft that runs Kubernetes cluster state. The `Step()` function is the state machine dispatcher — compare it to `handle_request_vote` and `handle_append_entries` in this module.
- **"Designing Data-Intensive Applications" Chapter 9:** The section on consensus covers Raft's safety properties in less mathematical terms. Useful for interview-style explanations.
- **KRaft design doc (KIP-500):** Apache Kafka's migration from ZooKeeper to Raft-based metadata management. Explains the practical adaptations Kafka made to the Raft paper for their specific use case.

---

## 14. Lab Exercises

**Exercise 1: Trigger a Split Vote**
Set `ELECTION_TIMEOUT_MIN_MS = ELECTION_TIMEOUT_MAX_MS = 150` (no randomization) and run a 3-node cluster. Observe that leader election takes much longer or fails entirely. Then restore randomization and compare time to first leader.

**Exercise 2: Force Log Truncation**
Manually construct a scenario: start a 3-node cluster, write 3 entries, then partition node-1. Write 3 more entries (only node-0 and node-2 get them). Heal the partition, isolate node-0 (original leader). Now partition node-2. Start a new election — node-1 becomes leader (it has the shorter log). Write new entries. Heal node-2 and observe it truncate its conflicting entries to match node-1's log.

**Exercise 3: Verify Election Restriction**
Create two nodes: node-A with log=[T1,T1,T2] and node-B with log=[T1,T1]. Have node-B send a RequestVote to node-A. Verify node-A rejects (node-B's log is shorter and same term). Have node-A send a RequestVote to node-B. Verify node-B grants the vote (node-A's log is longer).

**Exercise 4: Measure Commit Latency**
Add timing to `client_write()` and measure time from write to commit (entry applied). Run with 1, 3, and 5 nodes. Observe how latency increases with cluster size (must wait for quorum of followers to confirm).

---

## 15. Key Takeaways

The Raft follower state machine has three states and five fundamental rules (AppendEntries rules from the paper). Every safety property of Raft traces back to these five rules plus the election restriction. The election restriction ensures that a leader always has all committed entries. The AppendEntries consistency check (prevLogIndex/prevLogTerm) ensures that followers never apply entries from a divergent history. Term-based step-down ensures that stale leaders immediately yield to newer leaders.

The randomized election timeout is not a detail — it is the mechanism that makes liveness possible. Without it, Raft could livelock in split votes forever. The correctness (safety) doesn't depend on randomization, but progress (liveness) does.

The implementation reveals which decisions are critical (term update on every RPC, vote persistence, log truncation before append) and which are optimizations (fast backtracking, batched heartbeats, NOOP entry). The critical decisions cannot be skipped; the optimizations trade simplicity for performance.

---

## 16. Connections to Other Modules

- **M38 — Consensus Algorithms:** M38 described the Raft algorithm theoretically and implemented a full cluster (`RaftCluster`). This module implements the follower state machine at method level — every `handle_request_vote` and `handle_append_entries` method implements exactly one of the rules from M38's theoretical description.
- **M42 — Leader-Follower Replication Log:** M43's `_log` is the Raft log; the WAL in M42 is the physical storage for the Raft log. The `flush_lsn` concept in M42 maps to `commitIndex` in M43 — both are the durable, committed position.
- **M40 — Failure Modes:** The split-vote failure scenario (section 8.1) is an instance of the split-brain failure type from M40. The election restriction is Raft's mechanism for preventing split-brain at the leader level.
- **M44 — Reading Real Failure Reports:** M44 analyzes real Raft/consensus failure reports (ZooKeeper failures, etcd split-brain scenarios). The code in this module gives concrete meaning to every technical term in those reports.

---

## 17. Anti-Patterns

**Anti-pattern: Responding to RPCs before persisting state changes.** The follower must persist `currentTerm`, `votedFor`, and `log` to stable storage BEFORE responding to RPCs. If a follower updates `votedFor` in memory and responds, then crashes before the fsync, it will forget it voted — and could grant a second vote in the same term after recovery. This violates the "at most one vote per term" invariant. The implementation simulates this with the ordering in `handle_request_vote` (update in lock, which represents the atomic persist-then-respond contract).

**Anti-pattern: Applying state machine changes before logging them.** Some naive implementations apply a command to the state machine when the leader executes it, before the log entry is replicated. If the leader crashes before the entry is committed, the state change was applied but the entry won't survive — the new leader has no knowledge of it, and the state machine diverges. Always apply to the state machine in `_apply_committed_entries`, only after entries are committed.

---

## 18. Tools Reference

| Tool / Resource | Purpose | Key Usage |
|----------------|---------|-----------|
| `raft_follower.py` | Full Raft follower state machine | RaftNode, RaftCluster, handle_append_entries/request_vote |
| `etcdctl endpoint status` | Check Raft term and leader across etcd cluster | `etcdctl endpoint status --write-out=table` |
| `etcd_server_leader_changes_seen_total` | Count of leader elections (Prometheus metric) | Alert if > 2/hour |
| `etcd_server_proposals_pending` | Uncommitted proposals in Raft (Prometheus metric) | Alert if > 0 sustained |
| `raft.github.io` | Animated Raft visualization | Visual debugging of election and log replication |
| `etcd-io/raft/raft.go` | Production Go Raft implementation | Source for `Step()`, `becomeLeader()`, `appendEntry()` |

---

## 19. Glossary

**AppendEntries:** The Raft RPC sent by the leader to followers. Contains: term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit. Used for both log replication and heartbeats (empty entries).

**Candidate:** A Raft node state. Entered when the election timeout fires and no heartbeat was received. Candidate increments its term, votes for itself, and sends RequestVote RPCs to all peers.

**CommitIndex:** The highest log index that the leader knows is replicated on a majority (committed). Sent to followers in AppendEntries. Followers advance their commitIndex and apply entries accordingly.

**Election restriction:** The rule that a voter grants a vote only if the candidate's log is at least as up-to-date as the voter's. Implemented in `handle_request_vote` via the `candidate_log_up_to_date` check.

**Election timeout:** The duration a follower waits without a heartbeat before starting an election. Must be randomized to prevent simultaneous elections (split votes).

**Fast backtracking:** An optimization to AppendEntries rejection: the follower includes the conflicting term and its first index. Allows the leader to jump back by an entire term instead of decrementing nextIndex by 1. Reduces catch-up time for lagging followers.

**Log Matching Property:** If two logs contain an entry with the same index and term, then the logs are identical in all entries up to that index. Guaranteed by the AppendEntries consistency check.

**matchIndex:** Leader-side array tracking the highest log index confirmed replicated to each follower. Used by the leader to determine when an entry has reached a quorum.

**nextIndex:** Leader-side array tracking the next log index to send to each follower. Initially set to leader's last log index + 1 after election. Decremented on rejection.

**No-op entry:** A log entry appended by a new leader immediately after election. Forces the leader to commit all preceding entries from previous terms (since the no-op's commit propagates commitment to all preceding entries via Log Matching Property).

**Quorum:** The majority of a cluster: ⌊N/2⌋ + 1 nodes. Any two quorums overlap by at least one node — this overlap is what makes consensus possible.

**RequestVote:** The Raft RPC sent by a candidate to request a vote. Contains: term, candidateId, lastLogIndex, lastLogTerm.

**Step down:** The act of reverting from LEADER or CANDIDATE to FOLLOWER upon seeing a higher term. Clears voted_for for the new term.

**Term:** A monotonically increasing integer representing an "epoch" of leadership. Each election starts a new term. Used to detect stale messages (any message with a lower term is rejected).

---

## 20. Self-Assessment

1. A 5-node Raft cluster has a leader in term 3. Two followers crash. What happens? Can the remaining 3 nodes still commit new entries?
2. A follower receives an AppendEntries with `prevLogIndex=5, prevLogTerm=2`, but its log has only 4 entries. What does it return? What should the leader do with this response?
3. A follower receives an AppendEntries with `prevLogIndex=5, prevLogTerm=2`, and its log has 6 entries, but entry at index 5 has term 1 (not 2). What does it return? What does `conflict_term` and `conflict_index` contain?
4. Why does the leader append a NOOP entry immediately after winning an election, rather than waiting for the first client write?
5. Run `raft_follower.py` and add `time.sleep(2.0)` inside `handle_request_vote` for one node. Observe that other nodes can still elect a leader — the slow node is not needed for quorum. Remove the sleep and observe that the slow node catches up.
6. Why does `_step_down` reset `voted_for = None` when it sets the new term? What would break if it didn't?
7. Describe the exact sequence of events (including disk writes) when a client writes a value to a 3-node Raft cluster and receives a success response.
8. What property guarantees that if two nodes both have a committed entry at index 7, those entries are identical?

---

## 21. Module Summary

This module implemented the complete Raft follower state machine in Python: state transitions (FOLLOWER/CANDIDATE/LEADER), RequestVote handling with the election restriction, AppendEntries handling with the five rules from the paper, log truncation on conflict, commitIndex advancement, and state machine application. The implementation makes concrete every rule that M38 described abstractly.

The critical design decisions — persistent state (term, votedFor, log) before responding, election restriction during vote granting, log truncation before appending new entries, and only committing current-term entries directly — are not arbitrary. Each prevents a specific failure mode. Violating any one of them is sufficient to compromise either safety (split-brain committed entries) or liveness (permanent split votes).

The production failure scenarios ground the implementation in reality: split-vote livelock from non-randomized timeouts and stale-leader split-brain from network partitions are the two most common Raft failure modes in production clusters. Both are preventable with correct implementation and configuration.

The next module — M44: Reading Real Failure Reports — applies everything from SYS-DST-101 and SYS-DST-102 to actual distributed system failure postmortems. The code you've written in M42 and M43 provides the precise vocabulary to read those reports at the level of "exactly which Raft invariant was violated" rather than "the cluster had a problem."
