# Module Generation Strategy

This document defines the exact template and requirements for every module in Data Engineering University. Every module must follow this structure, in this order, without omitting any section.

---

## The 21-Section Module Template

### Section 1 — Module Header Block

Every module begins with a metadata block in this exact format:

```markdown
# Module NN — Title

> **School:** NN School Name  
> **Prerequisites:** [Link to prerequisite module(s)] or "None"  
> **Status:** ✅ Complete | 🔄 In Progress | 📋 Planned  
> **Estimated Study Time:** N–M hours
```

---

### Section 2 — Learning Objectives (## 1. Learning Objectives)

Numbered list of 5–8 specific, measurable objectives. Each objective must begin with an action verb (Explain, Describe, Trace, Design, Diagnose, Compare, Implement, Predict).

**Quality bar:** A reader should be able to use these objectives as a self-assessment checklist. Objectives like "understand X" are banned. Objectives like "Explain X from first principles without notes" are required.

---

### Section 3 — Prerequisites (## 2. Prerequisites)

Explicit cross-links to prerequisite modules with a one-sentence explanation of *why* each prerequisite is required. If there are no prerequisites, state "None — this is the entry point."

---

### Section 4 — Why This Topic Exists (## 3. Why This Topic Exists)

2–4 paragraphs. Answers: what problem existed before this technology/concept? What did engineers have to do without it? What pain does it eliminate? What would a data engineer not be able to do or diagnose without understanding this?

This section must *not* describe the technology itself — it describes the problem the technology solves. The technology is described in Section 7 (First Principles).

---

### Section 5 — History (## 4. History)

A timeline of key events. Each entry: **Year — Event.** At least 5 entries. Must include: the original paper or invention, key milestones, and the current dominant form.

The history section is not trivia. It must explain *why* the technology evolved the way it did — what constraints or failures drove each transition.

---

### Section 6 — First Principles (## 5. First Principles)

The deepest section of the module. Explains the concept from first principles — starting from what is already known (from prerequisite modules) and building up to the concept being taught.

Subsections (###) are required for: core definitions, the fundamental model, mathematical or logical foundations where applicable, and the connection between the concept and the prerequisite concepts.

**Quality bar:** A reader with no prior exposure to this topic should be able to read this section and have a complete mental model of *how* it works, not just *what* it does.

This section typically includes: ASCII whiteboard diagrams, precise terminology definitions, and at least one worked example tracing through the concept step by step.

---

### Section 7 — Internal Architecture (## 6. Internal Architecture)

A Mermaid diagram showing the components of the system and how they connect. Every node must be labelled. Every edge must represent a meaningful relationship (data flow, control flow, dependency).

The diagram must be followed by a paragraph explaining each component and its role.

---

### Section 8 — Execution Flow (## 7. Execution Flow)

A step-by-step trace of a specific operation through the system. Start from a user action (a query, a producer call, a job submission) and trace every step until the operation completes. Name the specific functions, processes, and data structures involved at each step.

This section often includes a Mermaid sequence diagram.

---

### Section 9 — System Behavior (## 8. Memory / CPU / Network / Storage Behavior)

Quantitative analysis of the system's resource consumption. Must include:

- **CPU:** What does the CPU spend most of its time doing? What are the hot paths?
- **Memory:** What data lives in memory? What is the memory footprint per unit of work?
- **Network:** What crosses the network? How much data? What protocols?
- **Storage:** What is written to disk? In what format? With what durability guarantees?

Include order-of-magnitude numbers (e.g., "a Kafka producer with `batch.size=16384` writes up to 16 KB per network request").

---

### Section 10 — Failure Scenarios (## 9. Failure Scenarios)

At least 3 failure scenarios. For each:
- **Name:** A short descriptive name for the failure mode.
- **Symptom:** What the engineer observes (error message, slow query, missing data, alert).
- **Root cause:** What is actually wrong at the system level.
- **Diagnosis:** How to find the root cause (specific commands, logs, metrics).

This section is written from the perspective of an on-call engineer. "It was slow" is not a symptom. "Spark task duration p99 exceeded 30 minutes; Spark UI showed 98% of shuffle data concentrated in 2 of 200 partitions" is a symptom.

---

### Section 11 — Recovery (## 10. Recovery)

For each failure scenario in Section 10, the recovery procedure. Must specify: whether the recovery is automatic or manual, what data loss (if any) may occur, and how to verify the system is healthy after recovery.

---

### Section 12 — Trade-offs (## 11. Trade-offs)

A table with columns: **Choice | Advantage | Disadvantage**. At least 5 rows. Covers the primary design decisions in the technology being studied.

No trade-off table may include a row where one option is strictly superior to another — if that were true, the inferior option would not exist. Every row must represent a genuine engineering judgment call.

---

### Section 13 — Optimization (## 12. Optimization)

At least 4 specific, actionable optimization techniques. For each:
- The technique (specific configuration, code pattern, or architectural change).
- Why it works (explain the mechanism, not just the result).
- When to apply it (the specific condition that makes it beneficial).
- What it costs (no optimization is free — always state the trade-off).

---

### Section 14 — Production Examples (## 13. Production Examples)

At least 2 concrete production scenarios. Each scenario includes:
- The business context (what was being built).
- The specific technical problem that arose.
- The root cause (traced to the concepts in this module).
- The fix (specific configuration change, code change, or architectural change).
- The measured result (quantified improvement).

Production examples are not toy examples. They describe real patterns observed in production data engineering at scale.

---

### Section 15 — Code (## 14. Code)

At least 3 code blocks covering:
1. Observability: how to inspect the system's state (dis.dis, explain(), kafka-consumer-groups.sh, etc.).
2. The main operation: how to use the technology from Python/PySpark/SQL.
3. A performance comparison: demonstrating a fast vs slow approach, with measurable output.

All code must be runnable. All code must include expected output.

---

### Section 16 — Hands-On Lab (## 15. Hands-On Lab)

3 progressively harder lab exercises. Each lab:
- States a specific goal.
- Provides setup instructions (pip install, SparkSession config, etc.).
- Provides the starting code.
- States the expected output.
- Does not give away the solution (the student fills in the implementation).

Lab 1: Verify a core concept with minimal code.
Lab 2: Apply the concept to a data engineering scenario.
Lab 3: Diagnose and fix a broken implementation.

---

### Section 17 — Exercises (## 16. Exercises)

5 exercises mixing conceptual, practical, and research tasks. Answer guidance (not full answers) is provided for conceptual questions. Research questions cite where to find the answer.

---

### Section 18 — Interview Q&A (## 17. Interview Q&A)

6–8 interview questions with full, staff-level answers. Each answer:
- Is written as a spoken response (not a bullet list).
- Cites specific mechanisms, numbers, and trade-offs.
- Is the answer a Staff Engineer would give, not a junior engineer.
- Runs 150–300 words.

---

### Section 19 — Cross-Question Chain (## 18. Cross-Question Chain)

A simulated escalating interview conversation. Starts with a surface question, then digs deeper with each follow-up. Minimum 6 exchanges. Each exchange:
- Builds on the previous answer.
- Requires deeper knowledge.
- Ends with the deepest possible technical question on the topic.

Format: alternating `Interviewer:` and `→ You explain:` lines.

---

### Section 20 — Cheat Sheet (## 19. Cheat Sheet)

A single, self-contained ASCII reference card that fits within 72 characters width. Structured as labelled sections within a box. Must be memorizable in 10 minutes. Contains: key commands, key numbers, key configuration parameters, key trade-offs.

---

### Section 21 — Flashcards (## 20. Flashcards)

Exactly 20 flashcards. Format: a Markdown table with columns `# | Front | Back`.

Rules:
- Front: a complete question or "what is X?", not a fragment.
- Back: a complete, precise answer. No "see the module" — the answer must stand alone.
- Cards 1–5: vocabulary/definitions.
- Cards 6–10: how-it-works (mechanism).
- Cards 11–15: why-it-matters (data engineering application).
- Cards 16–20: failure modes and debugging.

---

### Section 22 — Summary + References (## 21. Summary / ## References)

**Summary:** 2–3 paragraphs recapping the module's core insight. The summary should be read last, not first. Must reference the "next module" and explain why the next module builds on this one.

**References:** Formatted per `REPOSITORY_STANDARDS.md §9`. Minimum 5 references. Must include at least one primary source (paper, official documentation, or source code).

**Footer:** Last two lines are always:
```markdown
*Next module → [MNN: Title](../MNN_Title/README.md)*  
*Back to [School NN index](../../CURRICULUM.md#school-nn--school-name)*
```

---

## Module Generation Checklist

Before marking a module as ✅ Complete, verify every item:

- [ ] All 21 sections present and in order
- [ ] Learning objectives use action verbs and are measurable
- [ ] Prerequisites include cross-links with explanations
- [ ] History has ≥5 entries explaining *why* the technology evolved
- [ ] First Principles section includes ASCII whiteboard
- [ ] Internal Architecture has a Mermaid diagram with a paragraph explanation
- [ ] Execution Flow has a traced step-by-step example
- [ ] System Behavior has CPU, Memory, Network, and Storage subsections with numbers
- [ ] Failure Scenarios has ≥3 scenarios with symptom, root cause, and diagnosis
- [ ] Recovery section covers each failure scenario
- [ ] Trade-offs table has ≥5 genuine engineering trade-offs
- [ ] Optimization has ≥4 techniques with mechanism and cost explained
- [ ] Production Examples has ≥2 scenarios with quantified results
- [ ] Code has ≥3 runnable blocks with expected output
- [ ] Lab has 3 progressive exercises with setup instructions
- [ ] Exercises has 5 mixed questions with answer guidance
- [ ] Interview Q&A has 6–8 staff-level answers (150–300 words each)
- [ ] Cross-Question Chain has ≥6 escalating exchanges
- [ ] Cheat Sheet fits within 72 characters width
- [ ] Flashcards: exactly 20, all 4 categories covered
- [ ] Summary references the next module
- [ ] References has ≥5 sources including ≥1 primary source
- [ ] Footer links to next module and school index
- [ ] All terminology matches `REPOSITORY_STANDARDS.md §11`
- [ ] All code blocks have language tags
- [ ] All diagrams have explanation paragraphs
- [ ] Cross-links use relative paths
- [ ] Quality Gates checklist passed (see `QUALITY_GATES.md`)
