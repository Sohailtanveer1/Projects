# CSF-ALG-101 M05: Graphs and DAGs

**Course:** CSF-ALG-101 — Algorithms and Data Structures for Data Engineers  
**Module:** 05 of 05  
**Filesystem position:** 00_CS_Foundations/M16_Graphs_and_DAGs  
**Prerequisites:** CSF-ALG-101 M01 (Complexity Analysis), M04 (Sorting Algorithms)

---

## Table of Contents

1. [The Problem Graphs Solve](#1-the-problem-graphs-solve)
2. [How to Use This Module](#2-how-to-use-this-module)
3. [Prerequisites Check](#3-prerequisites-check)
4. [Core Theory: Graphs](#4-core-theory-graphs)
5. [Visual: Graph Representations and Traversals](#5-visual-graph-representations-and-traversals)
6. [Deep Dive: BFS, DFS, and Their Properties](#6-deep-dive-bfs-dfs-and-their-properties)
7. [Directed Acyclic Graphs](#7-directed-acyclic-graphs)
8. [Topological Sort](#8-topological-sort)
9. [Critical Path Analysis](#9-critical-path-analysis)
10. [Mental Models](#10-mental-models)
11. [Failure Scenarios](#11-failure-scenarios)
12. [Data Engineering Connections](#12-data-engineering-connections)
13. [Code Toolkit](#13-code-toolkit)
14. [Hands-On Labs](#14-hands-on-labs)
15. [Interview Q&A](#15-interview-qa)
16. [Cross-Question Chain](#16-cross-question-chain)
17. [Common Misconceptions](#17-common-misconceptions)
18. [Performance Reference Card](#18-performance-reference-card)
19. [Connections to Other Modules](#19-connections-to-other-modules)
20. [Flashcards](#20-flashcards)
21. [Module Summary](#21-module-summary)

---

## 1. The Problem Graphs Solve

An Airflow DAG has 47 tasks. Some can run in parallel; some must wait for others to finish. Task E cannot start until tasks B and C complete. Task B cannot start until task A completes. The scheduler must determine which tasks can run right now, and in what order to schedule them to finish the entire pipeline as fast as possible. This is a graph problem.

A Spark job executes a chain of transformations: read Parquet → filter → join → groupBy → write. Spark does not execute these naively in sequence — it builds a DAG of operations, optimizes it through the Catalyst optimizer (collapsing filters, reordering joins), then translates it into physical stages separated by shuffle boundaries. Understanding this DAG is how you read `explain()` output.

A dbt project has 200 models. Some reference others. Running `dbt run` requires computing the dependency order so no model runs before its upstream dependencies are complete. A circular reference (model A depends on model B, model B depends on model A) is a fatal error that must be caught before any SQL executes.

All three problems — Airflow task scheduling, Spark execution planning, dbt dependency resolution — reduce to the same fundamental algorithms: topological sort for ordering, cycle detection for validation, and BFS/DFS for traversal. This module explains how they work and why.

---

## 2. How to Use This Module

**For interview prep:** Sections 4, 6, 7, 8, 15, 16. Topological sort — both Kahn's BFS-based algorithm and the DFS-based algorithm — is a canonical interview question at data engineering companies. Section 15 has six staff-level Q&As. Section 16 escalates from "what is a graph" to "design the Airflow scheduler."

**For production engineering:** Sections 11, 12. Section 11 covers real production failures (circular dependencies, Airflow deadlocks, Spark skipped stages from broken lineage). Section 12 maps every algorithm to the specific system that uses it.

**For fundamental understanding:** Read all sections in order. The progression is: graph definitions → BFS/DFS → DAGs → topological sort → critical path → production systems.

---

## 3. Prerequisites Check

- **Big-O notation** (M01): Graph algorithms are analyzed in terms of V (vertices) and E (edges). O(V + E) is the standard complexity for traversal algorithms — you need to understand why.
- **Queues and stacks** (implicit from M01/M02): BFS uses a queue (FIFO); DFS uses a stack (LIFO, either explicit or via recursion). The difference between queue and stack produces BFS vs DFS.
- **Sorting** (M04): Topological sort is named "sort" because it produces a linear ordering — a sequence, like a sorted array — but the algorithm is graph traversal, not comparison sort. The name is historical.

---

## 4. Core Theory: Graphs

### 4.1 Definitions

A **graph** G = (V, E) consists of:
- **V**: a set of **vertices** (also called nodes)
- **E**: a set of **edges**, each connecting two vertices

**Directed graph (digraph):** Each edge has a direction — edge (u, v) goes from u to v, but not necessarily from v to u.

**Undirected graph:** Each edge is bidirectional — edge {u, v} connects u and v symmetrically.

**Weighted graph:** Each edge has an associated weight (cost, distance, latency).

**Degree:** The number of edges incident to a vertex.
- In a directed graph: **in-degree** (edges arriving) and **out-degree** (edges leaving).
- A vertex with in-degree 0 is a **source** (no dependencies).
- A vertex with out-degree 0 is a **sink** (no successors).

**Path:** A sequence of vertices v₁, v₂, ..., vₖ where each (vᵢ, vᵢ₊₁) is an edge.

**Cycle:** A path that starts and ends at the same vertex.

**Connected:** An undirected graph is connected if there is a path between every pair of vertices.

**Strongly connected:** A directed graph is strongly connected if there is a directed path from every vertex to every other vertex.

**Sparse vs dense:**
- Sparse: E ≈ O(V) — few edges relative to vertices. Most real-world graphs (Airflow DAGs, dependency graphs, social networks).
- Dense: E ≈ O(V²) — nearly every pair of vertices has an edge. Rare in data engineering.

### 4.2 Graph Representations

**Adjacency list:** For each vertex, store the list of its neighbors.

```python
# Directed graph: A → B, A → C, B → D, C → D
graph = {
    'A': ['B', 'C'],
    'B': ['D'],
    'C': ['D'],
    'D': [],
}
```

- Space: O(V + E)
- Check edge (u, v): O(degree(u)) — scan u's neighbor list
- Iterate all neighbors of u: O(degree(u))
- **Preferred for sparse graphs** — most graph algorithms on sparse graphs

**Adjacency matrix:** V × V boolean matrix where `matrix[u][v] = True` if edge (u, v) exists.

```python
# Same graph as adjacency matrix (A=0, B=1, C=2, D=3)
#     A  B  C  D
# A [ 0, 1, 1, 0 ]
# B [ 0, 0, 0, 1 ]
# C [ 0, 0, 0, 1 ]
# D [ 0, 0, 0, 0 ]
```

- Space: O(V²) — impractical for large sparse graphs
- Check edge (u, v): O(1) — direct matrix lookup
- Iterate all neighbors of u: O(V) — scan entire row
- **Preferred for dense graphs or when O(1) edge lookup is needed**

For Airflow DAGs (typically 10–1000 tasks, sparse dependencies), adjacency list is always used. For Spark's logical plan DAG (operators as vertices, data flow as edges), adjacency list is used internally.

### 4.3 Complexity Analysis in Terms of V and E

Most graph algorithms are expressed in terms of |V| (number of vertices) and |E| (number of edges):

| Scenario | V | E | Notes |
|---|---|---|---|
| Airflow DAG | 10–1000 tasks | 10–2000 dependencies | Sparse: E ≈ 2V |
| Spark execution DAG | 5–500 operators | 5–500 data flows | Sparse: E ≈ V |
| dbt project | 10–500 models | 10–1000 model refs | Sparse: E ≈ 2V |
| Social network | 10⁹ users | 10¹⁰ connections | Sparse: E ≈ 10V |
| Neural network layers | 100 layers | 100 edges | Linear: E = V-1 |

For sparse graphs, E = O(V), so O(V + E) = O(V). For dense graphs, E = O(V²), so O(V + E) = O(V²). Always express graph complexity with both V and E explicitly — the ratio E/V determines which dominates.

---

## 5. Visual: Graph Representations and Traversals

### 5.1 Example DAG (Airflow-like Task Graph)

```
Task dependency DAG:

        ┌─────────────────────────────────┐
        │           extract_data           │  ← source (in-degree = 0)
        └──────────┬──────────────────────┘
                   │
          ┌────────┴────────┐
          ↓                 ↓
  ┌──────────────┐  ┌──────────────┐
  │ transform_A  │  │ transform_B  │
  └──────┬───────┘  └──────┬───────┘
         │                  │
         └────────┬─────────┘
                  ↓
         ┌─────────────────┐
         │  validate_data  │
         └────────┬────────┘
                  │
        ┌─────────┴──────────┐
        ↓                    ↓
┌──────────────┐    ┌──────────────┐
│  load_to_dw  │    │  send_report │
└──────────────┘    └──────────────┘
     (sink)              (sink)

Adjacency list:
  extract_data  → [transform_A, transform_B]
  transform_A   → [validate_data]
  transform_B   → [validate_data]
  validate_data → [load_to_dw, send_report]
  load_to_dw    → []
  send_report   → []
  
In-degrees:
  extract_data: 0  ← only source; starts immediately
  transform_A:  1
  transform_B:  1
  validate_data: 2  ← must wait for BOTH transforms
  load_to_dw:   1
  send_report:  1
```

### 5.2 BFS Traversal (Level-Order)

```
BFS from extract_data:

Queue: [extract_data]          Visited: {}
─────────────────────────────────────────────────────
Dequeue: extract_data          Visited: {extract_data}
  Enqueue neighbors: transform_A, transform_B
Queue: [transform_A, transform_B]

Dequeue: transform_A           Visited: {extract_data, transform_A}
  Enqueue: validate_data
Queue: [transform_B, validate_data]

Dequeue: transform_B           Visited: {..., transform_B}
  validate_data already queued → skip (already seen)
Queue: [validate_data]

Dequeue: validate_data         Visited: {..., validate_data}
  Enqueue: load_to_dw, send_report
Queue: [load_to_dw, send_report]

Dequeue: load_to_dw            Visited: {..., load_to_dw}
Queue: [send_report]

Dequeue: send_report           Visited: {..., send_report}
Queue: []  ← done

BFS order: extract_data, transform_A, transform_B, validate_data, load_to_dw, send_report
Level 0: [extract_data]
Level 1: [transform_A, transform_B]
Level 2: [validate_data]
Level 3: [load_to_dw, send_report]
```

### 5.3 DFS Traversal (Depth-First)

```
DFS from extract_data (recursive, follow neighbors in list order):

Call stack (→ means recurse into):
  dfs(extract_data)
    dfs(transform_A)
      dfs(validate_data)
        dfs(load_to_dw) → base case (no neighbors)
        dfs(send_report) → base case (no neighbors)
      return from validate_data
    return from transform_A
    dfs(transform_B)
      validate_data: already visited → skip
    return from transform_B
  return from extract_data

DFS order: extract_data, transform_A, validate_data, load_to_dw, send_report, transform_B
```

### 5.4 Cycle Detection in a Directed Graph

```
Graph with a cycle:
  A → B → C → D → B  ← cycle! (B → C → D → B)

DFS with three-color marking:
  WHITE (0): unvisited
  GRAY  (1): currently on the DFS call stack (being processed)
  BLACK (2): fully processed (all descendants explored)

dfs(A): A = GRAY
  dfs(B): B = GRAY
    dfs(C): C = GRAY
      dfs(D): D = GRAY
        neighbor B is GRAY → CYCLE DETECTED
        (we're currently exploring B's descendants and found B again)
```

The key insight: a GRAY node found during DFS means we found a back edge — an edge pointing to an ancestor currently on the call stack. This is precisely a cycle.

---

## 6. Deep Dive: BFS, DFS, and Their Properties

### 6.1 Breadth-First Search (BFS)

BFS explores vertices level by level, using a queue (FIFO). It visits all vertices at distance 1 from the source before visiting any at distance 2, and so on.

```python
from collections import deque

def bfs(graph: dict, start: str) -> list[str]:
    """
    BFS traversal. Returns vertices in the order visited.
    graph: adjacency list {vertex: [neighbor, ...]}
    """
    visited = {start}
    queue = deque([start])
    order = []
    
    while queue:
        vertex = queue.popleft()   # FIFO: oldest-discovered first
        order.append(vertex)
        for neighbor in graph[vertex]:
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
    
    return order
```

**Complexity:** O(V + E) — each vertex is enqueued once (O(V)), and each edge is examined once from each endpoint (O(E)).

**Properties:**
- Finds the **shortest path** (fewest edges) from source to all reachable vertices
- Visits vertices in non-decreasing order of their distance from the source
- Uses O(V) space for the queue (in the worst case, the entire frontier of a tree is enqueued)

**BFS in data engineering:**
- Airflow: computing task levels (which tasks are at the same "distance" from start and can potentially run in parallel)
- Spark lineage: traversing the RDD/DataFrame lineage to identify which partitions to recompute on failure
- dbt: `dbt ls --select +model_name` traverses upstream dependencies using BFS/DFS
- Data quality: propagating data quality issues from upstream to downstream tables

### 6.2 Depth-First Search (DFS)

DFS explores as far as possible along each branch before backtracking, using a stack (either explicit or via recursion).

```python
def dfs_recursive(graph: dict, start: str, visited: set = None) -> list[str]:
    """Recursive DFS. Returns vertices in discovery order."""
    if visited is None:
        visited = set()
    visited.add(start)
    order = [start]
    for neighbor in graph[start]:
        if neighbor not in visited:
            order.extend(dfs_recursive(graph, neighbor, visited))
    return order


def dfs_iterative(graph: dict, start: str) -> list[str]:
    """Iterative DFS using an explicit stack."""
    visited = set()
    stack = [start]
    order = []
    
    while stack:
        vertex = stack.pop()    # LIFO: most-recently-discovered first
        if vertex in visited:
            continue
        visited.add(vertex)
        order.append(vertex)
        # Push neighbors in reverse order to maintain left-to-right traversal
        for neighbor in reversed(graph[vertex]):
            if neighbor not in visited:
                stack.append(neighbor)
    
    return order
```

**Complexity:** O(V + E) — same as BFS.

**Properties:**
- Does NOT find shortest paths (DFS may take a long winding path to a nearby vertex)
- Naturally reveals the **tree structure** of the graph (DFS tree)
- DFS finishing times produce a reverse topological order (for DAGs)
- Uses O(V) stack space (proportional to the maximum recursion depth = longest path in the graph)

**DFS in data engineering:**
- Topological sort (Section 8): DFS-based algorithm produces correct topological order via finishing times
- Cycle detection: GRAY/BLACK coloring during DFS
- Spark DAG: Catalyst optimizer traverses the logical plan DAG with DFS to apply transformation rules
- dbt: `ref()` dependency resolution uses DFS to build the full dependency graph

### 6.3 BFS vs DFS: When to Use Which

| Property | BFS | DFS |
|---|---|---|
| Shortest path (unweighted) | **Yes** — guaranteed | No |
| Cycle detection | Yes (via parent tracking) | **Yes** — cleaner (GRAY/BLACK) |
| Topological sort | **Yes** — Kahn's algorithm | **Yes** — via finishing times |
| Memory for wide, shallow graph | **Bad** (wide frontier) | Good (one path at a time) |
| Memory for deep, narrow graph | Good | **Bad** (recursion depth limit) |
| Natural for levels/layers | **Yes** | No |

For Airflow scheduling: BFS is more natural — it processes tasks level by level, which corresponds to parallel execution waves.

For cycle detection in dbt: DFS is cleaner — the GRAY/BLACK coloring directly identifies back edges (cycles).

---

## 7. Directed Acyclic Graphs

### 7.1 Definition and Properties

A **Directed Acyclic Graph (DAG)** is a directed graph that contains no directed cycles. Every path terminates at a sink — you can never follow edges and return to where you started.

Properties that follow from acyclicity:

**1. At least one source (in-degree 0):** Every DAG has at least one vertex with no incoming edges. Proof: if every vertex had at least one incoming edge, you could follow incoming edges backward forever — creating an infinite path in a finite graph, which means a cycle. Contradiction.

**2. At least one sink (out-degree 0):** By symmetric argument.

**3. Topological ordering exists:** The vertices can be linearly ordered such that for every edge (u, v), u appears before v in the ordering. This is equivalent to the DAG property — a graph has a topological ordering if and only if it is a DAG.

**4. Finite longest path:** Since there are no cycles, the longest directed path is bounded by V-1 (visiting each vertex at most once).

### 7.2 Why DAGs Are Everywhere in Data Engineering

DAGs model **dependency relationships** where cycles are prohibited:

- **Task execution:** Task A must complete before Task B starts. "B depends on A" is modeled as edge A → B. A cycle would mean "A must complete before A starts" — impossible, hence the acyclicity constraint.
- **Data lineage:** Table X is derived from Table Y. Reading Y must happen before writing X. A cycle in data lineage would mean "Table X is derived from Table X" — logically impossible.
- **Model materialization:** A dbt model that SELECTs from another model creates a dependency edge. A circular dependency would mean a model SELECTs from itself indirectly — SQL cannot execute this.

### 7.3 DAG Validation

Before executing any DAG, you must validate that it is actually a DAG — that no cycles exist. This validation is cycle detection.

Failing to validate leads to:
- Airflow DAG hanging forever (task A waiting for task B which is waiting for task A)
- dbt `dbt run` entering an infinite compilation loop
- Spark query planner attempting to execute a circular data flow

The standard validation algorithm is DFS with GRAY/BLACK coloring — O(V + E).

---

## 8. Topological Sort

### 8.1 Definition

A **topological sort** of a DAG is a linear ordering of its vertices such that for every directed edge (u, v), vertex u appears before vertex v in the ordering.

A topological sort is not unique — a DAG may have many valid topological orderings. For the example DAG in Section 5:

One valid ordering: `extract_data → transform_A → transform_B → validate_data → load_to_dw → send_report`

Another valid ordering: `extract_data → transform_B → transform_A → validate_data → send_report → load_to_dw`

Both are correct — `transform_A` and `transform_B` have no ordering constraint between them (neither depends on the other), so either can come first.

### 8.2 Kahn's Algorithm (BFS-Based)

Kahn's algorithm (1962) is the most intuitive topological sort for practitioners because it directly models task scheduling: always run a task that has no remaining dependencies.

```
Algorithm:
1. Compute in-degree for every vertex.
2. Initialize queue with all vertices of in-degree 0 (sources).
3. While queue is not empty:
   a. Dequeue vertex u.
   b. Add u to the result (it's safe to process u now).
   c. For each neighbor v of u:
      - Decrement in-degree of v by 1 (u's dependency is now satisfied).
      - If in-degree of v becomes 0: enqueue v (all its dependencies are done).
4. If result length < V: a cycle exists (some vertices were never enqueued).
```

```python
from collections import deque

def topological_sort_kahn(graph: dict) -> list[str] | None:
    """
    Kahn's BFS-based topological sort.
    Returns topological order, or None if a cycle is detected.
    graph: adjacency list {vertex: [successor, ...]}
    """
    # Step 1: compute in-degrees
    in_degree = {v: 0 for v in graph}
    for v in graph:
        for neighbor in graph[v]:
            in_degree[neighbor] = in_degree.get(neighbor, 0) + 1

    # Step 2: initialize queue with sources
    queue = deque(v for v in graph if in_degree[v] == 0)
    result = []

    # Step 3: process
    while queue:
        u = queue.popleft()
        result.append(u)
        for v in graph[u]:
            in_degree[v] -= 1
            if in_degree[v] == 0:
                queue.append(v)

    # Step 4: cycle detection
    if len(result) < len(graph):
        return None  # cycle detected — not all vertices were processed
    return result
```

**Complexity:** O(V + E) — each vertex is enqueued once, each edge is processed once.

**Why Kahn's is intuitive for scheduling:** The queue at any point contains all tasks that are currently "runnable" — all their dependencies have completed. An Airflow scheduler can directly implement this: the queue is the set of tasks it can submit to workers right now.

**Cycle detection:** If the result is shorter than V, some vertices were never added to the queue — their in-degree never reached 0 because they are part of a cycle where each vertex is waiting for another vertex in the same cycle.

### 8.3 DFS-Based Topological Sort

The DFS-based algorithm uses finishing times: a vertex is added to the result only after all its descendants have been fully explored (i.e., when DFS "finishes" the vertex). The result is built in reverse — prepend to the front.

```python
def topological_sort_dfs(graph: dict) -> list[str] | None:
    """
    DFS-based topological sort.
    Returns topological order, or None if a cycle is detected.
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {v: WHITE for v in graph}
    result = []
    has_cycle = [False]

    def dfs(v):
        if has_cycle[0]:
            return
        color[v] = GRAY  # currently on the call stack
        for neighbor in graph[v]:
            if color[neighbor] == GRAY:
                has_cycle[0] = True  # back edge → cycle
                return
            if color[neighbor] == WHITE:
                dfs(neighbor)
        color[v] = BLACK  # fully explored
        result.append(v)  # add to result AFTER all descendants are done

    for v in graph:
        if color[v] == WHITE:
            dfs(v)

    if has_cycle[0]:
        return None
    return result[::-1]  # reverse: last finished = first in topological order
```

**Why the reverse of DFS finishing order is topological:**

For any edge (u → v), DFS finishes v before it finishes u (because DFS fully explores v's subtree while processing u's neighbor). Therefore v has a smaller finishing time than u, meaning v appears before u in the `result` list (which is built from first-finished to last-finished). Reversing gives u before v — which is the correct topological order for an edge u → v.

### 8.4 Comparison: Kahn's vs DFS

| Property | Kahn's (BFS-based) | DFS-based |
|---|---|---|
| Intuition | Task scheduling: run tasks with no pending deps | Finishing time: a task is "done" after all its dependents finish |
| Cycle detection | Naturally: result shorter than V | Naturally: GRAY node found during DFS |
| Implementation | Iterative (queue) | Recursive or iterative (stack) |
| Stack overflow risk | No | Yes (deep recursion on long chains) |
| Parallelism | Explicit: queue = all currently runnable tasks | Not explicit |
| Use in practice | Airflow scheduler, dbt build order | Spark DAG traversal, compiler dependency analysis |

Airflow uses Kahn's: the queue of in-degree-0 tasks corresponds directly to the worker pool. When a task completes, Airflow decrements the in-degree of its downstream tasks and enqueues any that reach in-degree 0. This is Kahn's algorithm running live, with task completion as the "dequeue" event.

---

## 9. Critical Path Analysis

### 9.1 The Critical Path

For a DAG where each vertex has a duration (time to execute), the **critical path** is the longest directed path from source to sink. It determines the minimum possible pipeline completion time, regardless of how much parallelism is available.

```
Task durations (minutes):
  extract_data:  10
  transform_A:   30
  transform_B:   15
  validate_data:  5
  load_to_dw:    20
  send_report:    2

Paths from source to sinks:
  Path 1: extract_data → transform_A → validate_data → load_to_dw
           = 10 + 30 + 5 + 20 = 65 min

  Path 2: extract_data → transform_A → validate_data → send_report
           = 10 + 30 + 5 + 2 = 47 min

  Path 3: extract_data → transform_B → validate_data → load_to_dw
           = 10 + 15 + 5 + 20 = 50 min

  Path 4: extract_data → transform_B → validate_data → send_report
           = 10 + 15 + 5 + 2 = 32 min

Critical path: Path 1 (65 minutes) — the LONGEST path
Minimum pipeline duration: 65 minutes (even with infinite workers)
```

The critical path cannot be shortened by adding more workers. The only way to reduce the minimum pipeline duration is to optimize tasks ON the critical path — transform_A (30 min) or load_to_dw (20 min). Optimizing transform_B (15 min) has zero effect on total pipeline time because transform_B is not on the critical path.

### 9.2 Computing the Critical Path via Dynamic Programming on the DAG

```
Algorithm (longest path in DAG):
1. Topological sort the DAG.
2. Process vertices in topological order.
3. For each vertex v: 
   earliest_start[v] = max(earliest_start[u] + duration[u]) for all u → v
   (i.e., v can start only after all its predecessors finish)
4. Critical path length = max(earliest_start[sink] + duration[sink]) for all sinks

This is a form of dynamic programming on the DAG structure.
```

```
Process in topological order:
[extract_data, transform_A, transform_B, validate_data, load_to_dw, send_report]

earliest_start[extract_data]  = 0           (source, no predecessors)
earliest_start[transform_A]   = 0 + 10 = 10 (after extract_data)
earliest_start[transform_B]   = 0 + 10 = 10 (after extract_data)
earliest_start[validate_data] = max(10+30, 10+15) = max(40, 25) = 40  ← bottleneck
earliest_start[load_to_dw]    = 40 + 5 = 45
earliest_start[send_report]   = 40 + 5 = 45

Completion times:
  load_to_dw:  45 + 20 = 65  ← critical path end
  send_report: 45 + 2  = 47

Critical path length: 65 minutes
Critical path nodes: tasks where slack = 0
  (slack = latest allowable start - earliest possible start)
```

### 9.3 Critical Path in Production

**Airflow:** The critical path concept explains why adding more workers doesn't always speed up a pipeline. If your pipeline's critical path is a single 4-hour transformation task that cannot be parallelized, it takes 4 hours minimum regardless of worker count. Profiling should focus on the critical path tasks.

**Spark:** Each Spark stage has a duration. The critical path through stages (accounting for data dependencies) is the minimum job time. A stage that is not on the critical path (its data is not needed by the next stage on the critical path) can be parallelized aggressively without affecting total time.

**dbt:** `dbt build --select tag:daily` runs all models with the `daily` tag. The critical path through dbt's model dependency graph determines the minimum build time. Identifying and optimizing the longest model chain (in terms of query execution time) gives the most improvement.

---

## 10. Mental Models

### 10.1 The Factory Floor Model

Imagine an automobile assembly line where each workstation is a vertex and the dependency arrows say "this part must be installed before this other part." You cannot install the windshield before the frame is assembled.

**BFS** is like a factory manager walking the floor level by level: first all parts that can be assembled with no prerequisites, then all parts that needed those first parts, and so on.

**Topological sort** is the master assembly sequence: a linear list of every task in an order where no task appears before its prerequisites.

**Critical path** is the answer to "if we added unlimited workers, what's the fastest we could possibly finish?" — it's the single longest dependency chain that cannot be parallelized.

### 10.2 The Spreadsheet Cell Dependency Model

Think of an Excel spreadsheet where cells contain formulas referencing other cells. Cell B2 = A1 + A2. Cell C3 = B2 * 2. If you change A1, Excel must recalculate B2 before it can recalculate C3.

The dependency graph of cells is a DAG (circular references are errors that Excel explicitly catches). Excel's recalculation engine performs a topological sort to determine evaluation order. This is exactly what dbt does for SQL models.

### 10.3 The Drain Model for Cycle Detection

DFS with GRAY/BLACK coloring: imagine water flowing through a pipe network. GRAY vertices are "currently in the pipe" (water is flowing through them right now). If water flowing through a pipe arrives at a vertex already in the pipe — it's a cycle. The water would loop forever. That's a cycle in the graph.

---

## 11. Failure Scenarios

### 11.1 Airflow DAG with a Cycle — Silent Deadlock

```python
# Dangerous Airflow DAG — circular dependency

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("broken_dag", start_date=datetime(2024, 1, 1)) as dag:
    task_a = BashOperator(task_id="task_a", bash_command="echo A")
    task_b = BashOperator(task_id="task_b", bash_command="echo B")
    task_c = BashOperator(task_id="task_c", bash_command="echo C")
    
    task_a >> task_b  # A must complete before B
    task_b >> task_c  # B must complete before C
    task_c >> task_a  # C must complete before A  ← CYCLE!

# Airflow raises: AirflowDagCycleException: Cycle detected in DAG: broken_dag
# This is caught at DAG parse time — Airflow runs cycle detection (DFS)
# on every DAG file during the scheduler's DAG discovery loop.
```

Airflow validates DAGs during the scheduler heartbeat loop (every `scheduler_heartbeat_sec` seconds). A cycle prevents the DAG from being loaded into the metadata database entirely — it appears in `airflow dags list` as an import error.

**What happens without detection:** If cycle detection were skipped, task_a would wait for task_c, task_c would wait for task_b, task_b would wait for task_a — all three tasks stuck in `queued` state forever. The DAG run never completes. This consumes a DAG run slot for eternity.

### 11.2 dbt Circular Reference — Compilation Error

```sql
-- models/model_a.sql
SELECT user_id, SUM(amount) as total
FROM {{ ref('model_b') }}  -- model_a depends on model_b
GROUP BY user_id

-- models/model_b.sql
SELECT user_id, COUNT(*) as cnt
FROM {{ ref('model_a') }}  -- model_b depends on model_a  ← CYCLE!
GROUP BY user_id
```

```bash
$ dbt compile
Running with dbt=1.7.0
Found 2 models

Encountered an error:
  Compilation Error in model model_a (models/model_a.sql)
  Cycle detected! model_a → model_b → model_a
```

dbt builds the entire project's dependency graph at compile time and runs Kahn's algorithm to produce the build order. If `len(topological_order) < num_models`, a cycle is reported. The compilation fails before any SQL is executed.

### 11.3 Spark Skipped Stage Due to Broken Lineage

```python
# Spark silently skips stages when cached data is lost

rdd = sc.textFile("s3://bucket/large_file.gz")  # 500 GB
rdd_filtered = rdd.filter(lambda x: "ERROR" in x)
rdd_cached = rdd_filtered.cache()

# ... long computation using rdd_cached ...
rdd_result = rdd_cached.map(expensive_transform).reduce(aggregate)
# Executor dies, rdd_cached partition is lost

# Spark recomputes from lineage:
# rdd → filter → cache (recompute) → map → reduce
# This involves re-reading the 500 GB file and re-filtering
# The lineage DAG tells Spark exactly what to recompute

# If lineage is broken (e.g., input file deleted):
# Spark: SparkException: Job aborted due to stage failure:
#   Task 0 in stage 5.0 failed 4 times; last failure:
#   org.apache.hadoop.mapred.InvalidInputException: Input path does not exist: s3://bucket/large_file.gz
```

Spark traverses the DAG lineage in reverse (from output toward input) to identify what must be recomputed. A broken lineage node (deleted input, unreachable source) causes stage failure after all retry attempts are exhausted.

### 11.4 Memory Exhaustion from Unbounded BFS Queue

```python
# BFS on a massive graph with high branching factor

# Graph: 10 million users, each user follows 200 others
# BFS from one user: Level 0 = 1 node, Level 1 = 200 nodes,
# Level 2 = 200 × 200 = 40,000 nodes, Level 3 = 8 million nodes
# BFS queue at Level 3: 8 million node IDs in memory

# For social network graphs, BFS is often limited in depth:
def bfs_limited(graph, start, max_depth=2):
    from collections import deque
    visited = {start}
    queue = deque([(start, 0)])  # (node, depth)
    result = []
    while queue:
        node, depth = queue.popleft()
        result.append(node)
        if depth < max_depth:
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, depth + 1))
    return result
```

For data lineage traversal (Airflow, dbt), graphs are small and this is not a concern. For social graph analytics at scale (follower networks, recommendation systems), BFS must be bounded in depth or distributed across a graph processing framework like GraphX or Apache Giraph.

### 11.5 Topological Sort on Disconnected Graphs

```python
# Bug: topological sort that only processes one connected component

def broken_topo_sort(graph):
    visited = set()
    result = []
    
    # WRONG: only starts DFS from 'start_node' — misses disconnected components
    start_node = next(iter(graph))
    def dfs(v):
        visited.add(v)
        for neighbor in graph[v]:
            if neighbor not in visited:
                dfs(neighbor)
        result.append(v)
    
    dfs(start_node)  # only one component traversed!
    return result[::-1]

# FIX: iterate all vertices, start DFS from any unvisited vertex
def correct_topo_sort(graph):
    color = {v: 0 for v in graph}  # 0=WHITE, 1=GRAY, 2=BLACK
    result = []
    
    def dfs(v):
        color[v] = 1  # GRAY
        for neighbor in graph[v]:
            if color[neighbor] == 0:
                dfs(neighbor)
        color[v] = 2  # BLACK
        result.append(v)
    
    for v in graph:  # iterate ALL vertices — handles disconnected graphs
        if color[v] == 0:
            dfs(v)
    
    return result[::-1]
```

An Airflow DAG with multiple independent subgraphs (e.g., task group A and task group B that never interact) is still one DAG object but contains multiple connected components. The topological sort must handle all components, not just the one reachable from one starting vertex.

---

## 12. Data Engineering Connections

### 12.1 Airflow Scheduler: Kahn's Algorithm in Production

The Airflow scheduler runs a continuous loop:

```
Airflow Scheduler Loop (simplified):

1. DAG discovery: parse DAG files → build DAG graph → validate (DFS cycle detection)
2. For each active DAG run:
   a. Compute in-degrees of all tasks for this run
      (based on dependencies + current task states)
   b. Find all tasks with in-degree 0 AND state = "none" → runnable tasks
   c. Submit runnable tasks to the executor (LocalExecutor, CeleryExecutor, etc.)
3. Task completion callback:
   a. Mark task as "success" or "failed"
   b. Decrement in-degree of downstream tasks
   c. If a task failed and propagate_skips=True: mark downstream as "skipped"
   d. Re-run step 2b for newly runnable tasks
4. Repeat

This is exactly Kahn's algorithm:
  - "in-degree 0" = all dependencies completed
  - "decrement in-degree" = a dependency just completed
  - "enqueue" = submit to the executor worker pool
```

Airflow's metadata database stores the current state of every task instance. The scheduler's in-memory representation is a DAG of TI (TaskInstance) objects with their states. Kahn's queue is maintained in memory and rebuilt from the database on scheduler restart.

**Concurrency controls:** Airflow adds constraints beyond Kahn's:
- `dag_concurrency`: max simultaneous tasks per DAG run
- `pool`: named resource pools with slot limits (e.g., only 5 Spark tasks running at once)
- `max_active_tasks_per_dag`: overall DAG-level limit

These constraints throttle the queue, converting the basic Kahn's algorithm into a priority-queue-based scheduler.

### 12.2 Spark: The Logical and Physical DAG

Spark's execution model has two DAGs:

**Logical DAG (DataFrame/RDD lineage):**
```
df = spark.read.parquet("events/")    # Node: FileScan
   .filter(col("status") == "active") # Node: Filter
   .join(users, on="user_id")         # Node: Join
   .groupBy("user_id")                # Node: Aggregate
   .agg(count("*"))                   # Node: Count

Logical DAG:
  FileScan → Filter → Join → Aggregate

Catalyst optimization (tree rewriting on the logical DAG):
  - Push Filter below Join (predicate pushdown)
  - Reorder joins if multiple (cost-based optimization)
  After optimization: Filter → FileScan → Join → Aggregate
```

**Physical DAG (stages and tasks):**
```
Physical DAG (after Catalyst produces physical plan):

Stage 0: FileScan + Filter (no shuffle boundary)
  Tasks: one per Parquet partition

Shuffle boundary (Join requires repartitioning by user_id)

Stage 1: Shuffle Read + Join + Aggregate (after shuffle)
  Tasks: one per shuffle partition (spark.sql.shuffle.partitions)

Spark's DAGScheduler:
  - Builds stage dependency graph (Stage 0 must complete before Stage 1)
  - Schedules tasks within each stage (parallel)
  - Handles stage retries on task failure (recompute from lineage)
  - Tracks task completion and submits next-stage tasks
```

The DAGScheduler uses topological sort to determine stage execution order. A stage is submitted when all its parent stages are complete — Kahn's algorithm at the stage level.

### 12.3 dbt: Ref Graph and Build Order

```python
# dbt builds a DAG from {{ ref() }} calls

# models/stg_events.sql
SELECT * FROM raw.events WHERE status = 'active'

# models/dim_users.sql  
SELECT * FROM raw.users

# models/fct_engagement.sql
SELECT 
    e.user_id,
    u.user_name,
    COUNT(*) as event_count
FROM {{ ref('stg_events') }} e          -- edge: fct_engagement → stg_events
JOIN {{ ref('dim_users') }} u            -- edge: fct_engagement → dim_users
ON e.user_id = u.user_id
GROUP BY e.user_id, u.user_name

# DAG:
#   stg_events ──→ fct_engagement
#   dim_users  ──→ fct_engagement

# Topological order: [stg_events, dim_users, fct_engagement]
# (stg_events and dim_users can run in parallel)
# fct_engagement runs after both

# dbt run --select fct_engagement+   ← + means downstream too
# Uses BFS/DFS from fct_engagement to find all downstream models to run
```

dbt's `--select` syntax supports graph operators:
- `+model`: model and all its upstream dependencies (BFS backward from model)
- `model+`: model and all downstream dependents (BFS forward)
- `1+model+1`: model, its immediate parents, and immediate children (1-hop BFS)

These are BFS/DFS operations on the dbt dependency DAG.

### 12.4 Data Lineage: Column-Level DAG

Modern data lineage tools (OpenLineage, Marquez, DataHub, Atlan) track lineage at the column level — not just "table A is derived from table B" but "column X in table A is derived from column Y in table B via this transformation."

The column-level lineage graph is a DAG where:
- Vertices: (table_name, column_name) pairs
- Edges: transformation relationships

```
Example column lineage (DAG):

raw.events.amount ──→ stg_events.amount ──→ fct_revenue.total_amount
raw.events.user_id ─┐
                    ├──→ stg_events.user_id ──→ fct_revenue.user_id
raw.users.user_id ──┘

Operations on this column-level DAG:
  Impact analysis: "if we change the type of raw.events.amount, which downstream 
                    columns are affected?"
  → BFS forward from raw.events.amount
  
  Root cause: "fct_revenue.total_amount has nulls — where did the nulls come from?"
  → BFS backward from fct_revenue.total_amount
```

---

## 13. Code Toolkit

### 13.1 `dag.py` — DAG with Topological Sort, Cycle Detection, Critical Path

```python
"""
dag.py — A production-quality DAG implementation.

Provides:
  - Directed graph with adjacency list
  - Kahn's topological sort (BFS-based)
  - DFS-based topological sort with cycle detection
  - Critical path analysis (longest path with task durations)
  - BFS and DFS traversal
  - In-degree computation
  - Level assignment (for parallel execution waves)

Run directly for demo on an Airflow-like task dependency graph.
"""
from __future__ import annotations
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DAG:
    """
    Directed Acyclic Graph with topological sort and critical path analysis.
    
    Vertices are strings (task IDs, model names, etc.).
    Edges represent "must complete before" (dependency) relationships.
    Optional duration per vertex for critical path computation.
    """
    _adjacency: dict[str, list[str]] = field(default_factory=dict)
    _durations: dict[str, float] = field(default_factory=dict)  # vertex → duration

    # ─── Graph construction ──────────────────────────────────────────────

    def add_vertex(self, v: str, duration: float = 1.0) -> None:
        """Add a vertex with optional duration (for critical path)."""
        if v not in self._adjacency:
            self._adjacency[v] = []
        self._durations[v] = duration

    def add_edge(self, u: str, v: str) -> None:
        """Add a directed edge u → v (u must complete before v)."""
        self.add_vertex(u)
        self.add_vertex(v)
        self._adjacency[u].append(v)

    @property
    def vertices(self) -> list[str]:
        return list(self._adjacency.keys())

    @property
    def edges(self) -> list[tuple[str, str]]:
        return [(u, v) for u in self._adjacency for v in self._adjacency[u]]

    def in_degrees(self) -> dict[str, int]:
        """Return in-degree for every vertex."""
        deg = {v: 0 for v in self._adjacency}
        for u in self._adjacency:
            for v in self._adjacency[u]:
                deg[v] = deg.get(v, 0) + 1
        return deg

    # ─── BFS and DFS ─────────────────────────────────────────────────────

    def bfs(self, start: str) -> list[str]:
        """BFS from start. Returns vertices in visit order."""
        visited = {start}
        queue = deque([start])
        order = []
        while queue:
            u = queue.popleft()
            order.append(u)
            for v in self._adjacency.get(u, []):
                if v not in visited:
                    visited.add(v)
                    queue.append(v)
        return order

    def dfs(self, start: str) -> list[str]:
        """DFS from start. Returns vertices in discovery order."""
        visited: set[str] = set()
        order: list[str] = []

        def _dfs(v: str) -> None:
            visited.add(v)
            order.append(v)
            for neighbor in self._adjacency.get(v, []):
                if neighbor not in visited:
                    _dfs(neighbor)

        _dfs(start)
        return order

    # ─── Topological sort ────────────────────────────────────────────────

    def topological_sort_kahn(self) -> Optional[list[str]]:
        """
        Kahn's BFS-based topological sort.
        Returns sorted vertex list, or None if a cycle exists.
        O(V + E).
        """
        in_deg = self.in_degrees()
        queue = deque(v for v in self._adjacency if in_deg.get(v, 0) == 0)
        result: list[str] = []

        while queue:
            u = queue.popleft()
            result.append(u)
            for v in self._adjacency[u]:
                in_deg[v] -= 1
                if in_deg[v] == 0:
                    queue.append(v)

        if len(result) < len(self._adjacency):
            return None  # cycle detected
        return result

    def topological_sort_dfs(self) -> Optional[list[str]]:
        """
        DFS-based topological sort using finishing times.
        Returns sorted vertex list, or None if a cycle exists.
        O(V + E).
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {v: WHITE for v in self._adjacency}
        result: list[str] = []
        has_cycle = [False]

        def _dfs(v: str) -> None:
            if has_cycle[0]:
                return
            color[v] = GRAY
            for neighbor in self._adjacency.get(v, []):
                if color.get(neighbor, WHITE) == GRAY:
                    has_cycle[0] = True
                    return
                if color.get(neighbor, WHITE) == WHITE:
                    _dfs(neighbor)
            color[v] = BLACK
            result.append(v)

        for v in self._adjacency:
            if color[v] == WHITE:
                _dfs(v)

        if has_cycle[0]:
            return None
        return result[::-1]

    def has_cycle(self) -> bool:
        """Return True if this graph contains a directed cycle."""
        return self.topological_sort_kahn() is None

    # ─── Level assignment (for parallel execution) ────────────────────────

    def levels(self) -> dict[str, int]:
        """
        Assign each vertex the level (0-indexed) at which it can run.
        Level 0 = no dependencies (sources).
        Level k = all dependencies are at levels < k.

        Tasks at the same level can run in parallel.
        Computed via BFS from all sources simultaneously.
        O(V + E).
        """
        in_deg = self.in_degrees()
        level = {}
        queue = deque()
        for v in self._adjacency:
            if in_deg.get(v, 0) == 0:
                queue.append(v)
                level[v] = 0

        while queue:
            u = queue.popleft()
            for v in self._adjacency[u]:
                in_deg[v] -= 1
                new_level = level[u] + 1
                level[v] = max(level.get(v, 0), new_level)
                if in_deg[v] == 0:
                    queue.append(v)

        return level

    # ─── Critical path ────────────────────────────────────────────────────

    def critical_path(self) -> dict:
        """
        Compute the critical path (longest path) in the DAG.
        Requires vertex durations (set via add_vertex(v, duration=...)).
        
        Returns:
          {
            'length': float,           # total duration of critical path
            'path': list[str],         # vertices on the critical path
            'earliest_start': dict,    # earliest start time per vertex
            'slack': dict,             # float slack per vertex (0 = on critical path)
          }
        
        O(V + E).
        """
        topo = self.topological_sort_kahn()
        if topo is None:
            raise ValueError("Cannot compute critical path: graph contains a cycle")

        # Forward pass: compute earliest start times
        earliest_start: dict[str, float] = {v: 0.0 for v in topo}
        for u in topo:
            for v in self._adjacency[u]:
                new_start = earliest_start[u] + self._durations.get(u, 0)
                if new_start > earliest_start[v]:
                    earliest_start[v] = new_start

        # Completion time per vertex
        completion = {v: earliest_start[v] + self._durations.get(v, 0) for v in topo}
        pipeline_duration = max(completion.values())

        # Backward pass: latest allowable start times
        latest_start: dict[str, float] = {}
        for v in topo:
            if not self._adjacency[v]:  # sink
                latest_start[v] = pipeline_duration - self._durations.get(v, 0)

        for u in reversed(topo):
            if u in latest_start:
                continue
            latest_u = min(
                latest_start[v] for v in self._adjacency[u]
            ) - self._durations.get(u, 0)
            latest_start[u] = latest_u

        # Slack = latest_start - earliest_start (0 = on critical path)
        slack = {v: latest_start.get(v, 0) - earliest_start[v] for v in topo}

        # Reconstruct critical path (vertices with slack ≈ 0)
        critical = [v for v in topo if abs(slack.get(v, 0)) < 1e-9]

        return {
            'length': pipeline_duration,
            'path': critical,
            'earliest_start': earliest_start,
            'slack': slack,
        }


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Airflow-like Task DAG ===\n")

    dag = DAG()
    dag.add_vertex("extract_data",  duration=10)
    dag.add_vertex("transform_A",   duration=30)
    dag.add_vertex("transform_B",   duration=15)
    dag.add_vertex("validate_data", duration=5)
    dag.add_vertex("load_to_dw",    duration=20)
    dag.add_vertex("send_report",   duration=2)

    dag.add_edge("extract_data", "transform_A")
    dag.add_edge("extract_data", "transform_B")
    dag.add_edge("transform_A",  "validate_data")
    dag.add_edge("transform_B",  "validate_data")
    dag.add_edge("validate_data","load_to_dw")
    dag.add_edge("validate_data","send_report")

    print(f"Has cycle: {dag.has_cycle()}")
    print(f"\nKahn's topological sort:")
    print(f"  {dag.topological_sort_kahn()}")
    print(f"\nDFS-based topological sort:")
    print(f"  {dag.topological_sort_dfs()}")

    levels = dag.levels()
    print(f"\nExecution levels (parallel waves):")
    from collections import defaultdict
    level_groups = defaultdict(list)
    for v, lv in sorted(levels.items(), key=lambda x: x[1]):
        level_groups[lv].append(v)
    for lv in sorted(level_groups):
        print(f"  Level {lv}: {level_groups[lv]} ← can run in parallel")

    cp = dag.critical_path()
    print(f"\nCritical path analysis:")
    print(f"  Pipeline duration: {cp['length']} min")
    print(f"  Critical path:     {cp['path']}")
    print(f"\n  Task slack (0 = on critical path):")
    for v, s in sorted(cp['slack'].items(), key=lambda x: x[1]):
        flag = " ← CRITICAL" if abs(s) < 1e-9 else f"  (slack={s:.0f}min)"
        print(f"    {v:<20} {flag}")

    print(f"\n=== Cycle Detection Demo ===\n")
    cyclic_dag = DAG()
    cyclic_dag.add_edge("A", "B")
    cyclic_dag.add_edge("B", "C")
    cyclic_dag.add_edge("C", "A")  # cycle!
    result = cyclic_dag.topological_sort_kahn()
    print(f"Cyclic graph topological sort: {result}  (None = cycle detected)")
```

### 13.2 `airflow_scheduler_sim.py` — Airflow Scheduler Simulation

```python
"""
airflow_scheduler_sim.py — Simulates the Airflow scheduler loop.

Models:
  - Task state machine (none → queued → running → success/failed)
  - Kahn's algorithm for deciding which tasks are runnable
  - Concurrency limits (max_active_tasks)
  - Downstream skip propagation on failure

Run directly for a step-by-step simulation of the example task graph.
"""
from __future__ import annotations
from collections import deque, defaultdict
from dataclasses import dataclass, field
from enum import Enum
import random


class TaskState(Enum):
    NONE     = "none"      # not yet started
    QUEUED   = "queued"    # dependencies met, waiting for worker
    RUNNING  = "running"   # currently executing
    SUCCESS  = "success"   # completed successfully
    FAILED   = "failed"    # completed with failure
    SKIPPED  = "skipped"   # upstream failed, propagated skip


TERMINAL = {TaskState.SUCCESS, TaskState.FAILED, TaskState.SKIPPED}
DONE     = {TaskState.SUCCESS, TaskState.SKIPPED}  # downstream can proceed


@dataclass
class TaskSpec:
    task_id: str
    duration_ticks: int       # simulated time steps to complete
    fail_probability: float = 0.0


@dataclass
class TaskInstance:
    spec: TaskSpec
    state: TaskState = TaskState.NONE
    remaining_ticks: int = 0

    @property
    def task_id(self) -> str:
        return self.spec.task_id


class AirflowSimulator:
    """
    Simulates one DAG run using Kahn's algorithm for scheduling.
    
    Each "tick" represents one scheduler heartbeat cycle.
    Tasks run for spec.duration_ticks ticks then complete.
    """

    def __init__(
        self,
        tasks: list[TaskSpec],
        dependencies: list[tuple[str, str]],   # (upstream, downstream)
        max_active_tasks: int = 4,
        random_seed: int = 42,
    ) -> None:
        random.seed(random_seed)
        self.max_active_tasks = max_active_tasks
        self.instances: dict[str, TaskInstance] = {
            t.task_id: TaskInstance(t) for t in tasks
        }
        # Build adjacency: upstream → list of downstreams
        self.downstream: dict[str, list[str]] = defaultdict(list)
        self.upstream: dict[str, list[str]] = defaultdict(list)
        for up, down in dependencies:
            self.downstream[up].append(down)
            self.upstream[down].append(up)

        # Kahn's in-degree tracking
        self.in_degree: dict[str, int] = {t.task_id: 0 for t in tasks}
        for up, down in dependencies:
            self.in_degree[down] += 1

        self.tick = 0
        self.log: list[str] = []

    def runnable(self) -> list[str]:
        """Tasks with in_degree 0 and state NONE — ready to queue."""
        return [
            tid for tid, deg in self.in_degree.items()
            if deg == 0 and self.instances[tid].state == TaskState.NONE
        ]

    def active_count(self) -> int:
        return sum(
            1 for ti in self.instances.values()
            if ti.state == TaskState.RUNNING
        )

    def run(self) -> dict:
        """Run the simulation until all tasks are in terminal states."""

        # Initialize: queue all sources (in_degree 0)
        for tid in self.runnable():
            self.instances[tid].state = TaskState.QUEUED
            self.log.append(f"Tick 0: {tid} → QUEUED (source task)")

        while any(ti.state not in TERMINAL for ti in self.instances.values()):
            self.tick += 1
            self._scheduler_tick()

        succeeded = sum(1 for ti in self.instances.values() if ti.state == TaskState.SUCCESS)
        failed    = sum(1 for ti in self.instances.values() if ti.state == TaskState.FAILED)
        skipped   = sum(1 for ti in self.instances.values() if ti.state == TaskState.SKIPPED)

        return {
            "total_ticks": self.tick,
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
            "task_states": {tid: ti.state.value for tid, ti in self.instances.items()},
        }

    def _scheduler_tick(self) -> None:
        """One scheduler heartbeat: advance running tasks, queue runnable tasks."""

        # 1. Advance running tasks
        for ti in self.instances.values():
            if ti.state == TaskState.RUNNING:
                ti.remaining_ticks -= 1
                if ti.remaining_ticks <= 0:
                    # Task completes
                    if random.random() < ti.spec.fail_probability:
                        ti.state = TaskState.FAILED
                        self.log.append(f"Tick {self.tick}: {ti.task_id} → FAILED")
                        self._propagate_skip(ti.task_id)
                    else:
                        ti.state = TaskState.SUCCESS
                        self.log.append(f"Tick {self.tick}: {ti.task_id} → SUCCESS")
                        self._unlock_downstream(ti.task_id)

        # 2. Submit queued tasks up to concurrency limit
        queued = [
            tid for tid, ti in self.instances.items()
            if ti.state == TaskState.QUEUED
        ]
        for tid in queued:
            if self.active_count() >= self.max_active_tasks:
                break
            ti = self.instances[tid]
            ti.state = TaskState.RUNNING
            ti.remaining_ticks = ti.spec.duration_ticks
            self.log.append(f"Tick {self.tick}: {tid} → RUNNING (will take {ti.remaining_ticks} ticks)")

    def _unlock_downstream(self, completed_task: str) -> None:
        """Decrement in_degree for downstream tasks; queue those that reach 0."""
        for down in self.downstream.get(completed_task, []):
            self.in_degree[down] -= 1
            if self.in_degree[down] == 0:
                down_ti = self.instances[down]
                if down_ti.state == TaskState.NONE:
                    down_ti.state = TaskState.QUEUED
                    self.log.append(f"Tick {self.tick}: {down} → QUEUED (dependencies met)")

    def _propagate_skip(self, failed_task: str) -> None:
        """Mark all downstream tasks as SKIPPED (propagate failure)."""
        queue = deque(self.downstream.get(failed_task, []))
        while queue:
            tid = queue.popleft()
            ti = self.instances[tid]
            if ti.state not in TERMINAL:
                ti.state = TaskState.SKIPPED
                self.in_degree[tid] = -1  # prevent future queueing
                self.log.append(f"Tick {self.tick}: {tid} → SKIPPED (upstream failed)")
                queue.extend(self.downstream.get(tid, []))


if __name__ == "__main__":
    tasks = [
        TaskSpec("extract_data",  duration_ticks=2),
        TaskSpec("transform_A",   duration_ticks=6),
        TaskSpec("transform_B",   duration_ticks=3),
        TaskSpec("validate_data", duration_ticks=1, fail_probability=0.0),
        TaskSpec("load_to_dw",    duration_ticks=4),
        TaskSpec("send_report",   duration_ticks=1),
    ]
    deps = [
        ("extract_data", "transform_A"),
        ("extract_data", "transform_B"),
        ("transform_A",  "validate_data"),
        ("transform_B",  "validate_data"),
        ("validate_data","load_to_dw"),
        ("validate_data","send_report"),
    ]

    print("=== Airflow Scheduler Simulation (No Failures) ===\n")
    sim = AirflowSimulator(tasks, deps, max_active_tasks=4)
    result = sim.run()
    for entry in sim.log:
        print(f"  {entry}")
    print(f"\nResult: {result['total_ticks']} ticks, "
          f"{result['succeeded']} succeeded, {result['failed']} failed, "
          f"{result['skipped']} skipped")

    print("\n=== Airflow Scheduler Simulation (validate_data Fails) ===\n")
    tasks_with_fail = [
        TaskSpec("extract_data",  duration_ticks=2),
        TaskSpec("transform_A",   duration_ticks=6),
        TaskSpec("transform_B",   duration_ticks=3),
        TaskSpec("validate_data", duration_ticks=1, fail_probability=1.0),  # always fails
        TaskSpec("load_to_dw",    duration_ticks=4),
        TaskSpec("send_report",   duration_ticks=1),
    ]
    sim2 = AirflowSimulator(tasks_with_fail, deps, max_active_tasks=4)
    result2 = sim2.run()
    for entry in sim2.log:
        print(f"  {entry}")
    print(f"\nResult: {result2['total_ticks']} ticks, "
          f"{result2['succeeded']} succeeded, {result2['failed']} failed, "
          f"{result2['skipped']} skipped")
```

### 13.3 `lineage_tracer.py` — Data Lineage Graph Traversal

```python
"""
lineage_tracer.py — Data lineage graph traversal.

Models column-level data lineage as a DAG.
Provides:
  - Impact analysis: which downstream columns are affected if a source changes?
  - Root cause: which upstream sources could cause a given column to be null/wrong?
  - Lineage path: full chain from source to target

Run directly for a dbt-like lineage example.
"""
from __future__ import annotations
from collections import deque
from dataclasses import dataclass, field


@dataclass
class LineageGraph:
    """
    Column-level lineage DAG.
    
    Nodes: (table, column) pairs — represented as "table.column" strings.
    Edges: transformation relationships (source → derived).
    """
    _forward: dict[str, list[str]] = field(default_factory=dict)   # source → [derived]
    _backward: dict[str, list[str]] = field(default_factory=dict)  # derived → [sources]
    _transformations: dict[tuple[str, str], str] = field(default_factory=dict)  # (src, dst) → description

    def add_lineage(self, source: str, derived: str, transformation: str = "") -> None:
        """Add a lineage edge: source column → derived column via transformation."""
        if source not in self._forward:
            self._forward[source] = []
        if derived not in self._backward:
            self._backward[derived] = []
        self._forward[source].append(derived)
        self._backward[derived].append(source)
        self._transformations[(source, derived)] = transformation

    def impact_analysis(self, changed_column: str) -> dict:
        """
        Forward BFS: which downstream columns are affected if changed_column changes?
        Returns affected columns grouped by distance (number of hops).
        """
        visited = {changed_column}
        queue = deque([(changed_column, 0)])
        affected_by_level: dict[int, list[str]] = {}

        while queue:
            col, depth = queue.popleft()
            for downstream in self._forward.get(col, []):
                if downstream not in visited:
                    visited.add(downstream)
                    if depth + 1 not in affected_by_level:
                        affected_by_level[depth + 1] = []
                    affected_by_level[depth + 1].append(downstream)
                    queue.append((downstream, depth + 1))

        return {
            "changed": changed_column,
            "total_affected": sum(len(v) for v in affected_by_level.values()),
            "by_level": affected_by_level,
        }

    def root_cause(self, problematic_column: str) -> dict:
        """
        Backward BFS: which upstream sources could cause issues in problematic_column?
        """
        visited = {problematic_column}
        queue = deque([(problematic_column, 0)])
        sources_by_level: dict[int, list[str]] = {}

        while queue:
            col, depth = queue.popleft()
            for upstream in self._backward.get(col, []):
                if upstream not in visited:
                    visited.add(upstream)
                    if depth + 1 not in sources_by_level:
                        sources_by_level[depth + 1] = []
                    sources_by_level[depth + 1].append(upstream)
                    queue.append((upstream, depth + 1))

        # Identify true root sources (no upstream sources)
        root_sources = [
            col for col in visited
            if col != problematic_column and not self._backward.get(col)
        ]
        return {
            "problematic": problematic_column,
            "by_level": sources_by_level,
            "root_sources": root_sources,
        }

    def lineage_path(self, source: str, target: str) -> list[str] | None:
        """BFS to find shortest path from source to target (if it exists)."""
        if source == target:
            return [source]
        visited = {source}
        queue = deque([(source, [source])])
        while queue:
            col, path = queue.popleft()
            for downstream in self._forward.get(col, []):
                if downstream not in visited:
                    new_path = path + [downstream]
                    if downstream == target:
                        return new_path
                    visited.add(downstream)
                    queue.append((downstream, new_path))
        return None  # no path


if __name__ == "__main__":
    lg = LineageGraph()

    # Build a dbt-like lineage graph
    # raw → staging → intermediate → mart

    lg.add_lineage("raw.events.amount",    "stg_events.amount",    "cast to decimal")
    lg.add_lineage("raw.events.user_id",   "stg_events.user_id",   "passthrough")
    lg.add_lineage("raw.events.ts",        "stg_events.event_date","DATE(ts)")
    lg.add_lineage("raw.users.user_id",    "dim_users.user_id",    "passthrough")
    lg.add_lineage("raw.users.name",       "dim_users.user_name",  "passthrough")

    lg.add_lineage("stg_events.amount",    "int_daily_revenue.revenue", "SUM(amount) GROUP BY date")
    lg.add_lineage("stg_events.event_date","int_daily_revenue.date",    "passthrough")
    lg.add_lineage("stg_events.user_id",   "fct_engagement.user_id",   "passthrough")
    lg.add_lineage("dim_users.user_id",    "fct_engagement.user_id",   "JOIN key")
    lg.add_lineage("dim_users.user_name",  "fct_engagement.user_name", "passthrough")

    lg.add_lineage("int_daily_revenue.revenue", "mart_finance.total_revenue", "SUM")
    lg.add_lineage("int_daily_revenue.date",    "mart_finance.report_date",   "passthrough")

    print("=== Impact Analysis ===")
    print("What if raw.events.amount changes type?\n")
    impact = lg.impact_analysis("raw.events.amount")
    print(f"Total affected columns: {impact['total_affected']}")
    for level, cols in sorted(impact['by_level'].items()):
        print(f"  Hop {level}: {cols}")

    print("\n=== Root Cause Analysis ===")
    print("mart_finance.total_revenue has wrong values — what are the root sources?\n")
    rc = lg.root_cause("mart_finance.total_revenue")
    for level, cols in sorted(rc['by_level'].items()):
        print(f"  Hop {level} upstream: {cols}")
    print(f"  Root sources (no upstream): {rc['root_sources']}")

    print("\n=== Lineage Path ===")
    path = lg.lineage_path("raw.events.amount", "mart_finance.total_revenue")
    print(f"raw.events.amount → mart_finance.total_revenue:")
    if path:
        print("  " + " →\n  ".join(path))
```

---

## 14. Hands-On Labs

### Lab 1: Implement and Verify Both Topological Sort Algorithms

**Goal:** Implement Kahn's and DFS-based topological sort, verify they both produce valid orderings, and observe cycle detection behavior.

```python
# lab1_topological_sort.py
"""
Implements both topological sort algorithms, verifies correctness,
and measures performance on large DAGs.
"""
import random
import time
from dag import DAG

def verify_topological_order(dag: DAG, order: list[str]) -> bool:
    """Check that for every edge (u, v), u appears before v in order."""
    position = {v: i for i, v in enumerate(order)}
    for u, v in dag.edges:
        if position.get(u, -1) >= position.get(v, float('inf')):
            print(f"  VIOLATION: {u} (pos {position.get(u)}) should be before {v} (pos {position.get(v)})")
            return False
    return True

def random_dag(n_vertices: int, n_edges: int, seed: int = 42) -> DAG:
    """Generate a random DAG with n_vertices vertices and approximately n_edges edges."""
    random.seed(seed)
    dag = DAG()
    vertices = [f"v{i}" for i in range(n_vertices)]
    for v in vertices:
        dag.add_vertex(v)
    added = 0
    attempts = 0
    while added < n_edges and attempts < n_edges * 10:
        attempts += 1
        u_idx = random.randint(0, n_vertices - 2)
        v_idx = random.randint(u_idx + 1, n_vertices - 1)  # v_idx > u_idx ensures no cycle
        u, v = vertices[u_idx], vertices[v_idx]
        if v not in dag._adjacency.get(u, []):
            dag.add_edge(u, v)
            added += 1
    return dag

if __name__ == "__main__":
    print("=== Topological Sort: Correctness Verification ===\n")

    dag = DAG()
    for task in ["extract", "transform_A", "transform_B", "validate", "load", "report"]:
        dag.add_vertex(task)
    dag.add_edge("extract", "transform_A")
    dag.add_edge("extract", "transform_B")
    dag.add_edge("transform_A", "validate")
    dag.add_edge("transform_B", "validate")
    dag.add_edge("validate", "load")
    dag.add_edge("validate", "report")

    kahn_order = dag.topological_sort_kahn()
    dfs_order  = dag.topological_sort_dfs()
    print(f"Kahn's order: {kahn_order}")
    print(f"  Valid: {verify_topological_order(dag, kahn_order)}")
    print(f"DFS order:   {dfs_order}")
    print(f"  Valid: {verify_topological_order(dag, dfs_order)}")

    print("\n=== Cycle Detection ===\n")
    cyclic = DAG()
    cyclic.add_edge("A", "B")
    cyclic.add_edge("B", "C")
    cyclic.add_edge("C", "A")
    print(f"Cyclic graph - Kahn's: {cyclic.topological_sort_kahn()} (None = cycle)")
    print(f"Cyclic graph - DFS:   {cyclic.topological_sort_dfs()} (None = cycle)")

    print("\n=== Performance on Large DAGs ===\n")
    for n_v, n_e in [(100, 200), (1000, 3000), (10000, 25000)]:
        large_dag = random_dag(n_v, n_e)
        t0 = time.perf_counter()
        order = large_dag.topological_sort_kahn()
        t_kahn = (time.perf_counter() - t0) * 1000
        t0 = time.perf_counter()
        order2 = large_dag.topological_sort_dfs()
        t_dfs = (time.perf_counter() - t0) * 1000
        valid = verify_topological_order(large_dag, order) if order else False
        print(f"V={n_v:>6}, E={n_e:>6}: Kahn={t_kahn:.2f}ms  DFS={t_dfs:.2f}ms  valid={valid}")
```

---

### Lab 2: Critical Path — Finding the Bottleneck

**Goal:** Given a pipeline DAG with task durations, compute the critical path and identify which task optimizations actually reduce total pipeline time.

```python
# lab2_critical_path.py
"""
Demonstrates critical path analysis on an Airflow-like DAG.
Shows which task optimizations reduce total pipeline time.
"""
from dag import DAG

def analyze_optimization(base_dag: DAG, task_to_optimize: str, new_duration: float) -> dict:
    """Compute critical path before and after optimizing a task."""
    cp_before = base_dag.critical_path()
    
    # Build new DAG with optimized task duration
    optimized = DAG()
    optimized._adjacency = {k: v[:] for k, v in base_dag._adjacency.items()}
    optimized._durations = dict(base_dag._durations)
    optimized._durations[task_to_optimize] = new_duration
    
    cp_after = optimized.critical_path()
    saved = cp_before['length'] - cp_after['length']
    return {
        'task': task_to_optimize,
        'original_duration': base_dag._durations[task_to_optimize],
        'new_duration': new_duration,
        'pipeline_before': cp_before['length'],
        'pipeline_after': cp_after['length'],
        'time_saved': saved,
        'on_critical_path': task_to_optimize in cp_before['path'],
    }

if __name__ == "__main__":
    dag = DAG()
    dag.add_vertex("extract_data",  duration=10)
    dag.add_vertex("transform_A",   duration=30)  # expensive!
    dag.add_vertex("transform_B",   duration=15)
    dag.add_vertex("validate_data", duration=5)
    dag.add_vertex("load_to_dw",    duration=20)
    dag.add_vertex("send_report",   duration=2)

    dag.add_edge("extract_data", "transform_A")
    dag.add_edge("extract_data", "transform_B")
    dag.add_edge("transform_A",  "validate_data")
    dag.add_edge("transform_B",  "validate_data")
    dag.add_edge("validate_data","load_to_dw")
    dag.add_edge("validate_data","send_report")

    cp = dag.critical_path()
    print(f"Base pipeline duration: {cp['length']} minutes")
    print(f"Critical path: {' → '.join(cp['path'])}\n")

    print(f"{'Task':<20} {'On CP?':>7} {'Old':>5} {'New':>5} {'Saved':>7}  Note")
    print("-" * 65)

    optimizations = [
        ("transform_A",  15),   # halve the expensive critical-path task
        ("transform_B",   5),   # halve a non-critical-path task
        ("load_to_dw",   10),   # halve a critical-path task
        ("validate_data", 1),   # small task, marginal improvement
        ("send_report",   0),   # eliminate non-critical task
    ]
    for task, new_dur in optimizations:
        r = analyze_optimization(dag, task, new_dur)
        note = "← reduces pipeline!" if r['time_saved'] > 0 else "no pipeline impact"
        print(f"{task:<20} {str(r['on_critical_path']):>7} {r['original_duration']:>5.0f} "
              f"{r['new_duration']:>5.0f} {r['time_saved']:>7.0f}m  {note}")
```

---

### Lab 3: dbt Dependency Graph — Simulating dbt ls and dbt run

**Goal:** Build a dbt-like model dependency graph and implement `dbt ls --select` filtering using BFS/DFS.

```python
# lab3_dbt_dependency_graph.py
"""
Simulates dbt's model dependency resolution.
Implements dbt ls --select operators:
  +model    = model + all upstream dependencies
  model+    = model + all downstream dependents
  model     = just the model

Shows build order for a subset of models.
"""
from collections import deque
from dag import DAG

def select_upstream(dag: DAG, model: str) -> set[str]:
    """Return model and all its upstream dependencies (BFS backward)."""
    selected = {model}
    queue = deque([model])
    # Build reverse adjacency
    reverse = {}
    for u, v in dag.edges:
        if v not in reverse:
            reverse[v] = []
        reverse[v].append(u)
    
    while queue:
        m = queue.popleft()
        for upstream in reverse.get(m, []):
            if upstream not in selected:
                selected.add(upstream)
                queue.append(upstream)
    return selected

def select_downstream(dag: DAG, model: str) -> set[str]:
    """Return model and all downstream dependents (BFS forward)."""
    selected = {model}
    queue = deque([model])
    while queue:
        m = queue.popleft()
        for downstream in dag._adjacency.get(m, []):
            if downstream not in selected:
                selected.add(downstream)
                queue.append(downstream)
    return selected

def build_order(dag: DAG, selected: set[str]) -> list[str]:
    """Return topological build order for only the selected models."""
    sub_dag = DAG()
    for v in selected:
        sub_dag.add_vertex(v)
    for u, v in dag.edges:
        if u in selected and v in selected:
            sub_dag.add_edge(u, v)
    return sub_dag.topological_sort_kahn() or []

if __name__ == "__main__":
    dag = DAG()

    # Staging layer
    for m in ["stg_events", "stg_users", "stg_orders"]:
        dag.add_vertex(m)

    # Intermediate layer
    dag.add_edge("stg_events", "int_event_counts")
    dag.add_edge("stg_users",  "int_event_counts")
    dag.add_edge("stg_orders", "int_order_totals")
    dag.add_edge("stg_users",  "int_order_totals")

    # Mart layer
    dag.add_edge("int_event_counts", "mart_engagement")
    dag.add_edge("int_order_totals", "mart_finance")
    dag.add_edge("int_event_counts", "mart_finance")

    print("=== dbt Model DAG ===\n")
    print(f"Full build order: {dag.topological_sort_kahn()}\n")

    # Simulate dbt ls --select
    selects = [
        ("+mart_finance",    select_upstream(dag, "mart_finance")),
        ("mart_finance+",    select_downstream(dag, "mart_finance")),
        ("+int_event_counts+", select_upstream(dag, "int_event_counts") |
                               select_downstream(dag, "int_event_counts")),
    ]

    for selector, selected in selects:
        order = build_order(dag, selected)
        print(f"dbt ls --select '{selector}':")
        print(f"  Models selected: {sorted(selected)}")
        print(f"  Build order:     {order}")
        print()
```

---

## 15. Interview Q&A

**Q1: What is a DAG and why is it used to model data pipeline dependencies?**

A Directed Acyclic Graph (DAG) is a directed graph with no cycles. Every edge points "forward" — following edges eventually leads to a terminal node (sink), never looping back. The three defining properties that make DAGs natural for pipeline modeling are: directionality (dependency relationships are asymmetric — A must complete before B, not the other way around), acyclicity (a task cannot depend on itself directly or transitively — this would be a logical impossibility), and the existence of at least one source (some task has no dependencies and can start immediately) and at least one sink (the pipeline must terminate).

Data pipeline dependencies are inherently DAG-shaped: a dbt model SELECT statement cannot reference a table that is derived from itself. An Airflow task cannot be triggered by its own output. A Spark transformation cannot consume its own result without an intermediate action. These are not just design constraints — they are logical necessities. Circular dependencies are not just errors; they are meaningless. Airflow, dbt, Spark, and every other pipeline tool validate the DAG property at load time because executing a pipeline with a cycle is undefined behavior: which task runs first when A waits for B and B waits for A?

The DAG structure enables two critical operations: topological sort (finding a valid execution order) and critical path analysis (finding the minimum possible pipeline duration). Both are impossible on cyclic graphs — topological sort has no valid answer and the longest path is infinite.

**Q2: Walk me through Kahn's algorithm for topological sort and explain how it detects cycles.**

Kahn's algorithm computes in-degrees for every vertex — the number of incoming edges, representing the number of dependencies. It initializes a queue with all vertices of in-degree zero (sources — tasks with no dependencies). These are the tasks that can start immediately.

The main loop dequeues one vertex at a time, adds it to the result, and processes its outgoing edges: for each neighbor (downstream task), decrement its in-degree by one, representing the completion of one dependency. If a neighbor's in-degree reaches zero, all its dependencies have now been processed, so it is enqueued. This repeats until the queue is empty.

Cycle detection is automatic: any vertex that is part of a cycle will never have its in-degree reduced to zero, because the cycle creates a situation where each vertex is waiting for another vertex in the same cycle. When the queue empties, if the result contains fewer than V vertices, those missing vertices were never enqueued — they are part of a cycle. This makes Kahn's algorithm simultaneously a topological sort and a cycle validator in a single O(V + E) pass.

The intuition for scheduling is direct: at any moment, the queue contains exactly the set of tasks that are currently runnable — all their dependencies have completed. This is how Airflow's scheduler actually works: the queue of in-degree-zero tasks is the pool of tasks submitted to the executor at each heartbeat.

**Q3: Airflow's scheduler seems complex. Explain it in terms of graph algorithms.**

At its core, the Airflow scheduler is Kahn's algorithm running incrementally over time, with task completions as the events that drive progress. At DAG load time, Airflow validates the DAG by running DFS cycle detection — a cycle raises `AirflowDagCycleException` and prevents the DAG from being registered. When a DAG run starts, the scheduler computes in-degrees for all task instances and initializes the "runnable" set with all in-degree-zero tasks (sources).

At each scheduler heartbeat, the scheduler queries the metadata database for completed task instances, decrements the in-degrees of their downstream tasks, and adds newly runnable tasks (those whose in-degree just reached zero) to the submission queue. Tasks from the queue are submitted to the executor pool up to the `max_active_tasks` concurrency limit. This limit converts the pure Kahn's queue into a rate-limited priority queue.

On task failure, the scheduler propagates the failure forward using BFS from the failed task: all downstream tasks are marked as "upstream_failed" and removed from contention. This is a forward BFS starting from the failure node, marking all reachable vertices as skipped.

The metadata database acts as persistent state for the in-degree counters and task states — allowing the scheduler to crash and restart without losing progress. On restart, it reconstructs the in-degree values from the stored task states and resumes Kahn's algorithm from wherever it was.

**Q4: How does Spark build and use its execution DAG?**

Spark builds two layers of DAG. The first is the logical plan DAG — a tree of relational operators (Scan, Filter, Join, Aggregate, Project) representing what the user asked for. When you chain DataFrame operations in Python, Spark lazily builds this DAG in the driver JVM without executing anything. Catalyst — Spark's query optimizer — analyzes and rewrites this logical DAG by applying a set of rules implemented as tree rewriting passes: predicate pushdown (move filters as close to the data source as possible), constant folding, join reordering (cost-based).

The second is the physical plan DAG, produced after Catalyst selects physical implementations for each logical operator (e.g., choosing BroadcastHashJoin vs SortMergeJoin). The DAGScheduler then analyzes the physical plan for **shuffle boundaries** — operations that require data to be redistributed across executors (wide transformations: joins, groupBys, repartitions). Each segment between shuffle boundaries becomes one stage. Stages form a DAG themselves, where a stage cannot start until all its parent stages (those that produce its input shuffles) are complete.

The DAGScheduler submits stages in topological order — a stage is submitted when all its parent stages have finished and their shuffle data is available. Within each stage, tasks run in parallel — one per partition. On task failure, the DAGScheduler replays the lineage DAG backward from the failed partition to recompute only what's needed, without re-running the entire pipeline.

**Q5: A dbt project has 300 models. `dbt run` is slow. How would you use graph analysis to diagnose and fix the problem?**

The first step is to understand the critical path through dbt's DAG — the longest chain of sequential dependencies that determines the minimum possible build time. dbt provides this indirectly: `dbt ls --select +model_name` shows upstream dependencies for a model, and `dbt build --select tag:daily` with verbose logging shows each model's start and end time. I would parse the logs to build an execution timeline and identify the critical path.

Once I know the critical path, I can focus optimization there. If the critical path is dominated by a single complex SQL model (say, a 45-minute historical aggregation), I would: (1) materialize intermediate models as tables rather than views so they're computed once, not re-derived on every downstream reference; (2) add incremental materialization so only new data is processed; (3) refactor the slow model to use approximate aggregation where exactness is not required.

The second analysis is parallelism. dbt's `--threads` flag controls how many models can run simultaneously. If the DAG has many independent subgraphs (models with no cross-dependencies), increasing threads to match the number of independent chains can dramatically reduce wall-clock time. I would use `dbt ls --select '*'` to dump all models, then build the DAG in Python and compute the maximum number of models at any level (the width of the topological sort levels) — this is the maximum parallelism achievable. If the current `--threads` is below the maximum width, increasing it helps. If it's above, adding threads provides no benefit.

The third analysis is selector-based partial builds. If certain mart models are needed urgently, I can use `dbt build --select +mart_critical_report` to run only that model and its upstream dependencies, bypassing the rest of the 300-model graph entirely.

**Q6: You're designing a new workflow orchestration system. How would you implement topological sort-based scheduling with support for task retries and dynamic task spawning?**

The core scheduler uses Kahn's algorithm with a persistent in-degree counter per task. In-degrees are stored in a database, not in memory, so the scheduler can restart without losing progress. When a task completes successfully, the scheduler runs a database transaction: decrement in-degrees of all downstream tasks, and for each that reaches zero, insert a "runnable" record into the work queue. The work queue is a database table (or Redis sorted set) that workers poll for available work.

Task retries add a state machine overlay. A task that fails transitions to "failed" state and the retry counter is decremented. If retries remain, the task is re-enqueued into the work queue with a backoff delay (exponential: 1 min, 4 min, 16 min). The task's downstream in-degrees are NOT decremented on failure — downstream tasks only unlock when the task eventually succeeds (or when the workflow is configured to propagate failure downstream). This "non-decrement on failure" is the mechanism that holds back downstream tasks until the upstream task either recovers or the operator marks it as acceptable.

Dynamic task spawning — where task A at runtime decides to create tasks B, C, D that didn't exist in the original DAG — requires a mutable in-memory graph. When A runs and discovers it needs to spawn B, C, D: insert B, C, D into the task table, insert edges (A → B, A → C, A → D) into the dependency table, set in-degrees of B, C, D to 0, and immediately enqueue them. The scheduler's Kahn's loop treats these exactly like any other tasks with in-degree 0.

The trickiest part is the schema for dynamic tasks — the DAG definition must allow "placeholder" nodes that can be populated at runtime (Airflow calls these DynamicTaskMapping, introduced in 2.3). The key invariant to maintain is that at any moment, the in-degree stored in the database accurately reflects how many incomplete upstream tasks each task is waiting for. Any implementation that violates this invariant will either deadlock (task waits for an in-degree that never reaches zero) or run tasks in the wrong order (in-degree reaches zero before an upstream task is actually complete).

---

## 16. Cross-Question Chain

**Q1 [Interviewer]: What is a topological sort?**

A topological sort of a directed acyclic graph is a linear ordering of all vertices such that for every directed edge (u, v), vertex u appears before vertex v in the ordering. Intuitively, if u must complete before v can start (u → v), then u precedes v in the sorted sequence. A topological sort exists if and only if the graph is a DAG — cyclic graphs have no valid topological ordering. The sort is not unique: vertices with no ordering constraint between them (neither depends on the other) can appear in either relative order. The canonical algorithms are Kahn's (BFS-based, O(V+E)) and DFS-based (using finishing times, also O(V+E)).

**Q2 [Interviewer]: Why does topological sort only work on DAGs and not on graphs with cycles?**

A topological sort requires that for every edge (u → v), u comes before v. Now suppose there's a cycle: u → v → w → u. The topological sort requires u before v (from edge u → v), v before w (from edge v → w), and w before u (from edge w → u). But u before v and w before u means u comes before itself — a contradiction. No linear ordering can satisfy all three constraints simultaneously. This is not just a limitation of specific algorithms — it's a fundamental impossibility. Kahn's algorithm reveals this by construction: in a cycle, every vertex has at least one incoming edge from within the cycle, so no vertex's in-degree ever reaches zero, and the queue stays empty before all vertices are processed.

**Q3 [Interviewer]: Airflow validates DAGs at load time. How does it detect cycles, and what happens if a cycle is somehow introduced at runtime?**

Airflow uses DFS with three-color marking (WHITE/GRAY/BLACK) during the DAG parsing phase, which runs every time the scheduler's DAG processor re-parses the DAG file (controlled by `scheduler_dag_dir_list_interval`). When DFS encounters a GRAY vertex — one currently on the call stack — it has found a back edge, which is definitionally a cycle. Airflow raises `AirflowDagCycleException`, the DAG is not registered in the metadata database, and the DAG appears as an import error in `airflow dags list`.

At runtime, Airflow DAGs are typically static — the task structure is fixed when the DAG is parsed. However, Airflow supports dynamic task generation (via `expand()` in Airflow 2.3+), where tasks can be generated at runtime based on upstream data. In theory, a bug in the dynamic task generator could create circular task dependencies that weren't present in the original DAG definition. Airflow's runtime scheduler doesn't re-validate the full DAG structure on every heartbeat — it trusts the parse-time validation. A dynamically introduced cycle would manifest as a deadlock: tasks waiting for each other forever, with the DAG run never completing. The practical protection is that `expand()` is constrained to fan-out patterns (one task generating many parallel tasks), not arbitrary graph modifications, which structurally prevents cycles.

**Q4 [Interviewer]: A data pipeline DAG has a 4-hour critical path. The team wants to reduce it to 2 hours. Where do you start?**

I start by computing the critical path — the longest directed path through the DAG, weighted by task duration. Only tasks on the critical path affect the total pipeline time. Optimizing any task not on the critical path has zero effect on pipeline duration.

For the critical path, I then compute slack for each task: the difference between the latest allowable start time and the earliest possible start time. Tasks with zero slack are on the critical path. The critical path node with the highest duration is the highest-leverage optimization target.

For a 4-hour pipeline, I would identify whether the critical path is a single long-running task (a 4-hour SQL aggregation, for example) or a chain of medium-duration tasks (eight 30-minute tasks that must run sequentially). These require different interventions. A single long task might be parallelizable: partition the data and run it as multiple shorter tasks in parallel (a fan-out/fan-in pattern that does not add to the critical path length). A chain of sequential tasks might indicate unnecessary serialization — if task B only needs columns A1 and A2 from task A, and task A writes 100 columns, perhaps A can be split so that the A1/A2 portion completes faster and B starts earlier.

I would also verify that the critical path tasks are actually being scheduled promptly — sometimes a task on the critical path is waiting in the queue because workers are busy with non-critical tasks. The scheduler should prioritize critical path tasks, which requires either Airflow's `priority_weight` setting or a queue configured to deprioritize non-critical work.

**Q5 [Interviewer]: How does Spark decide which operations to execute in the same stage vs different stages?**

The stage boundary rule is simple: every operation that requires a shuffle creates a stage boundary. A shuffle is any operation that requires rows to move between executors — data that was on executor A for partition 3 might need to be on executor B for partition 7 after the shuffle.

In Spark's physical plan, operations are classified as narrow (each output partition depends on at most one input partition) or wide (each output partition may depend on multiple input partitions). Narrow operations — map, filter, flatMap, union of co-partitioned data — can be pipelined within a stage: each record is processed through a chain of narrow operations without any data movement between executors. Wide operations — groupByKey, reduceByKey, join (non-broadcast), repartition — require shuffling and create a stage boundary.

Concretely: a physical plan of `FileScan → Filter → GroupBy → Sort → Write` creates three stages. Stage 0: FileScan + Filter (pipelined, both narrow). Shuffle boundary (GroupBy requires repartitioning by group key). Stage 1: ShuffleRead + GroupBy + Sort (GroupBy and Sort can be pipelined after the shuffle). Shuffle/write barrier. Stage 2: ShuffleRead + Write (final output). The DAGScheduler submits these stages in topological order: Stage 0 first, then Stage 1 after Stage 0's shuffle data is available, then Stage 2.

Understanding stage boundaries explains why `explain()` output and the Spark UI show stage information. A job with many small stages (many shuffles) has high coordination overhead. Consolidating operations — using `reduceByKey` instead of `groupByKey.mapValues(sum)`, or eliminating intermediate repartitions — reduces stage count and coordination overhead.

**Q6 [Interviewer]: Design a data lineage system that can answer "which downstream tables are affected if this source column changes type?" at scale.**

The core data structure is a column-level DAG. Vertices are `(catalog, database, table, column)` tuples — the fully qualified column identity. Edges represent derivation: column X in model Y is derived from column Z in model W via some transformation. This graph is built by parsing SQL — dbt models, Spark jobs, BigQuery queries — and extracting column-level lineage from the parse trees.

For scale (thousands of tables, millions of columns), the graph must be stored in a graph database (Neo4j, Amazon Neptune) or a purpose-built lineage store, not in memory. The "affected columns" query is a forward BFS from the changed column: traverse all outgoing edges, collecting affected columns level by level. With a graph database, this is a single graph traversal query (Cypher: `MATCH (source)-[:DERIVED_FROM*]->(affected)`) and runs in O(V + E) time on the relevant subgraph.

At scale, the BFS result can be cached per column: when a column changes, the impact set is pre-computed and stored. Updates to the cache are triggered whenever a new transformation is deployed (new dbt model, new Spark job). The cache invalidation policy follows the graph: when column X's transformation changes, any column that depends on X (transitively) may have a different impact set, so those caches must be invalidated — itself a graph traversal. This recursive invalidation is bounded by the DAG depth, which is typically small (5–20 hops in practice).

The output format matters for usability. Rather than a flat list of affected columns, I would group by severity: directly affected (1 hop), indirectly affected (2–5 hops), distantly affected (5+ hops). This lets a data engineer quickly understand whether a type change in a raw source column affects a single staging model or propagates all the way to a production dashboard.

---

## 17. Common Misconceptions

**"BFS gives the topological sort order."**
BFS from a single source does not produce a topological sort — it produces a level-order traversal. Two separate tasks at the same BFS level might have a dependency between them that BFS doesn't respect. Kahn's algorithm uses BFS mechanics (a queue, processing vertices level by level) but explicitly tracks in-degrees and only dequeues vertices whose in-degree has reached zero. The in-degree tracking is what makes it topological.

**"A DAG is a tree."**
A tree is a special case of a DAG (a DAG where each vertex has in-degree at most 1). A general DAG allows diamond shapes — multiple paths from one vertex to another, meaning a vertex can have multiple predecessors. Airflow DAGs commonly have diamond patterns: two parallel tasks both feeding into one downstream task. Trees cannot express this.

**"Adding more Airflow workers always speeds up the pipeline."**
Workers increase parallelism only for tasks at the same level (the same topological position). The critical path — the longest sequential dependency chain — determines the minimum pipeline duration regardless of worker count. If the critical path is a single 4-hour SQL query, 100 workers finish in 4 hours. Only optimizing the critical path tasks reduces total time.

**"Spark's `rdd.cache()` prevents recomputation."**
Caching persists a partition in executor memory. If the executor dies, the cached partition is lost and Spark recomputes it from the lineage DAG. The lineage DAG is not eliminated by caching — it is always maintained. This means if the input data (at the root of the lineage) is deleted or moved, Spark cannot recompute lost cached partitions and the job fails. Checkpointing (writing to HDFS/S3) breaks the lineage and creates a true recovery point.

**"Topological sort runs in O(V log V) like a regular sort."**
Topological sort uses graph traversal (BFS or DFS), not comparison-based sorting. Both Kahn's and DFS-based topological sort run in O(V + E). The word "sort" in "topological sort" refers to the output — a linear ordering — not to the comparison-based sort algorithm class. There's no N log N lower bound here; the O(N log N) bound applies to comparison sorts, not topological ordering.

---

## 18. Performance Reference Card

| Algorithm | Time | Space | Notes |
|---|---|---|---|
| BFS | O(V + E) | O(V) | Queue width = max frontier size |
| DFS | O(V + E) | O(V) | Stack depth = longest path |
| Kahn's topological sort | O(V + E) | O(V) | Also detects cycles |
| DFS topological sort | O(V + E) | O(V) | Three-color cycle detection |
| In-degree computation | O(V + E) | O(V) | Scan all edges |
| Critical path (DP on DAG) | O(V + E) | O(V) | Two passes: forward + backward |
| Level assignment | O(V + E) | O(V) | BFS from all sources simultaneously |

**Practical scale for common systems:**

| System | V | E | Algorithm used |
|---|---|---|---|
| Airflow DAG | 10–1000 | 10–5000 | Kahn's (scheduler loop) + DFS (cycle detection) |
| Spark physical plan | 5–500 | 5–500 | DFS (Catalyst) + Kahn's (DAGScheduler) |
| dbt project | 10–500 | 10–2000 | Kahn's (build order) + BFS (--select) |
| Column lineage | 10K–10M | 50K–50M | BFS (impact/root cause), graph DB |

At 10 million columns and 50 million lineage edges, O(V + E) = 60 million operations — runs in seconds in a graph database, not feasible in-memory on a single machine without graph partitioning.

---

## 19. Connections to Other Modules

**CSF-ALG-101 M01 (Complexity Analysis):** Graph algorithm complexity uses V (vertices) and E (edges) instead of N. O(V + E) is the canonical "linear in the graph size" complexity — same logic as O(N) for arrays. The critical path DP on a DAG is O(V + E), the same complexity as sorting-based DP in M04.

**CSF-ALG-101 M04 (Sorting Algorithms):** Topological sort is named "sort" but uses graph traversal (O(V+E)), not comparison sort (O(V log V)). The critical path computation on the DAG uses dynamic programming with topological order as the processing sequence — the DP naturally processes vertices in topological order, just as external sort processes runs in sorted order.

**DE-ORC-101 (Airflow):** Everything in this module is directly instantiated in Airflow: DAG construction (Python objects), cycle detection (parse-time DFS), Kahn's algorithm (scheduler heartbeat), critical path (pipeline SLA analysis), BFS for downstream task selection. Airflow is Kahn's algorithm as a service.

**DCS-SPK-101 (Spark Architecture):** The logical plan DAG, Catalyst optimizer (tree rewriting on the DAG), physical plan stages, and DAGScheduler are all direct applications of DAG algorithms. The RDD lineage graph is a DAG used for fault tolerance — tracing backward to recompute lost partitions.

**CPL-SYD-101 (Data System Design):** System design interviews often ask "how does Airflow/Spark execute tasks?" The answer requires explaining DAGs and topological sort. The "design a workflow orchestrator" problem is fundamentally a DAG scheduling problem.

---

## 20. Flashcards

| # | Front | Back |
|---|---|---|
| 1 | What is a DAG? | Directed Acyclic Graph: directed edges + no cycles. Every path terminates; following edges never loops back to a visited node. |
| 2 | Why must pipeline dependency graphs be DAGs and not general directed graphs? | A cycle means "task A must complete before task A starts" — logically impossible. Cycles make topological ordering undefined. |
| 3 | What is a topological sort? | A linear ordering of DAG vertices such that for every edge (u → v), u appears before v. Exists if and only if the graph is a DAG. |
| 4 | What is Kahn's algorithm? | BFS-based topological sort. Track in-degrees; enqueue all in-degree-0 vertices; dequeue, emit, decrement downstream in-degrees, enqueue newly-0 vertices. O(V+E). |
| 5 | How does Kahn's algorithm detect cycles? | If result length < V at the end, some vertices were never enqueued — their in-degrees never reached 0 because they are in a cycle waiting on each other. |
| 6 | What is the DFS-based topological sort? | Apply DFS; add each vertex to result AFTER all its descendants are fully explored (BLACK). Reverse the result. GRAY vertex encountered during DFS = cycle. |
| 7 | What is the three-color DFS marking? | WHITE=unvisited, GRAY=on current DFS path (in stack), BLACK=fully explored. Cycle detected when a GRAY node is encountered as a neighbor. |
| 8 | BFS uses a ___; DFS uses a ___ | BFS: queue (FIFO — visit oldest-discovered neighbors first). DFS: stack (LIFO — visit most-recently-discovered path first, via recursion or explicit stack). |
| 9 | What is the critical path in a pipeline DAG? | The longest directed path from source to sink, weighted by task durations. Determines the minimum possible pipeline completion time regardless of parallelism. |
| 10 | Does adding more Airflow workers reduce the critical path? | No. The critical path is the longest sequential dependency chain. More workers increase parallelism only for tasks NOT on the critical path. |
| 11 | What is the complexity of BFS and DFS? | Both O(V + E). Each vertex processed once, each edge examined once. |
| 12 | What is in-degree in a directed graph? | The number of edges pointing INTO a vertex. In-degree 0 = source (no dependencies). Kahn's starts with all in-degree-0 vertices. |
| 13 | How does Airflow's scheduler implement Kahn's algorithm? | Queries completed tasks from DB → decrements downstream in-degrees → enqueues newly-0 tasks → submits to executor pool up to max_active_tasks concurrency. |
| 14 | What creates a stage boundary in Spark? | Any wide transformation requiring a shuffle: groupBy, join (non-broadcast), repartition, distinct. Narrow transformations (filter, map) pipeline within a stage. |
| 15 | How does Spark's DAGScheduler use topological sort? | Submits stages in topological order: a stage is submitted only when all its parent stages (whose shuffle output it reads) have completed. |
| 16 | What is `dbt run --select +model_name`? | Run model_name and all its upstream dependencies. The `+` triggers backward BFS from model_name through the dependency graph. |
| 17 | What is impact analysis on a lineage DAG? | Forward BFS from a changed column, collecting all downstream columns that derive from it (directly or transitively). |
| 18 | Why is Kahn's algorithm more natural than DFS for Airflow scheduling? | Kahn's queue at any moment contains all currently runnable tasks — a direct mapping to the worker submission queue. DFS finishing times have no direct scheduling interpretation. |
| 19 | What is a source vertex in a DAG? | A vertex with in-degree 0 — no incoming edges, no dependencies. Can always run immediately. In Airflow: a task with no upstream dependencies. |
| 20 | What is the connection between the critical path and task optimization priority? | Only tasks on the critical path (slack = 0) affect total pipeline duration. Optimizing any off-critical-path task has zero impact on wall-clock completion time. |

---

## 20. Further Reading

**Foundational algorithms:**
- Cormen, T. et al. (2022). *Introduction to Algorithms*, 4th ed., Chapter 22 (Elementary Graph Algorithms) and Chapter 23 (Minimum Spanning Trees). The standard reference for BFS, DFS, and topological sort proofs.
- Kahn, A.B. (1962). *Topological Sorting of Large Networks*. Communications of the ACM. The original Kahn's algorithm paper — short and readable.

**Data engineering systems:**
- Airflow documentation: https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html — DAG structure, cycle detection, scheduling concepts
- Spark documentation: https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations — RDD lineage and DAG execution model
- dbt documentation: https://docs.getdbt.com/reference/node-selection/graph-operators — graph operators for --select syntax

**Data lineage:**
- OpenLineage specification: https://openlineage.io/ — the open standard for column-level lineage capture
- "The Data Engineering Podcast" episodes on data lineage and DataHub — practical perspectives on lineage graph construction and use

---

## 21. Module Summary

Graphs model relationships between objects. Directed Acyclic Graphs add two critical properties: directionality (dependencies have a direction) and acyclicity (no circular dependencies). Every data pipeline dependency graph — Airflow tasks, dbt models, Spark operations, data lineage columns — is a DAG, because circular dependencies are logically impossible in a pipeline.

BFS (breadth-first, uses a queue) explores level by level and finds shortest paths. DFS (depth-first, uses a stack or recursion) explores as deep as possible before backtracking and naturally produces finishing times. Both run in O(V + E).

Topological sort produces a linear ordering of DAG vertices respecting all edges — every predecessor before every successor. Kahn's algorithm (BFS-based) iteratively removes in-degree-0 vertices; the queue at each step is exactly the set of currently runnable tasks, making it the natural basis for schedulers. DFS-based topological sort uses finishing times (vertices are added to the result after all their descendants are explored); reversing gives topological order. Both detect cycles as a byproduct: Kahn's if result is shorter than V, DFS if a GRAY vertex is encountered.

Critical path analysis finds the longest weighted path through the DAG, determining the minimum pipeline completion time. Only tasks on the critical path (slack = 0) benefit from optimization — improving any non-critical task has zero effect on total time.

In data engineering: Airflow's scheduler is Kahn's algorithm running live (task completions drive in-degree decrements). Spark's DAGScheduler submits physical plan stages in topological order, separated by shuffle boundaries. dbt's `--select` operators are BFS/DFS on the model dependency graph. Data lineage systems answer impact analysis (forward BFS) and root cause analysis (backward BFS) on column-level lineage DAGs.

---

**CSF-ALG-101: 5 of 5 modules complete. Course COMPLETE.**  
**Next course: CSF-OS-101 — Operating System Internals**  
**(M01: Processes and Threads, M02: Process Scheduling, M03: Virtual Memory and Page Cache, M04: File I/O and System Calls, M05: Signals and IPC)**
