# Quality Gates

Every module must pass all quality gates before its status is changed to ✅ Complete. Quality gates are organized into nine review domains. A module with any ❌ in any domain remains 🔄 In Progress.

---

## Gate 1 — Technical Accuracy

| # | Check | Pass Criteria |
|---|---|---|
| 1.1 | Every technical claim is correct | No factual errors in mechanisms, numbers, or system behavior |
| 1.2 | Numbers are sourced and realistic | Latency numbers, throughput numbers, and configuration values match primary documentation or reputable benchmarks |
| 1.3 | No deprecated APIs in primary code examples | All code targets the current stable version of the technology |
| 1.4 | No fabricated behavior | All described behavior can be verified from source code, official docs, or a repeatable experiment |
| 1.5 | Opinions are labelled as opinions | Any statement that is a trade-off judgment or personal recommendation is prefaced with "In practice…", "One common approach…", or equivalent |

**Verdict:** All 5 checks must pass. A single factual error requires a rewrite of the affected section.

---

## Gate 2 — Logical Consistency

| # | Check | Pass Criteria |
|---|---|---|
| 2.1 | Concepts build on prerequisites | The module assumes only what has been taught in listed prerequisite modules |
| 2.2 | No circular dependencies | The module does not reference concepts from a later module without a forward-reference label ("covered in detail in X") |
| 2.3 | Terminology is consistent | All terms match the definitions in `REPOSITORY_STANDARDS.md §11` |
| 2.4 | Section flow is logical | Each section builds on the previous; the reader is never asked to accept something unexplained |
| 2.5 | The cross-question chain is coherent | Each interviewer follow-up logically follows from the previous answer; no question appears to come from a different topic |

**Verdict:** All 5 checks must pass.

---

## Gate 3 — Production Engineering Depth

| # | Check | Pass Criteria |
|---|---|---|
| 3.1 | Failure scenarios are production-realistic | Scenarios describe actual failure modes observed in real systems, not toy examples |
| 3.2 | Symptoms are specific and observable | Symptoms reference actual error messages, metrics, or tool output — not "it was slow" |
| 3.3 | Diagnosis uses real tools | Diagnosis steps use actual commands (`strace`, `perf`, `kafka-consumer-groups.sh`, Spark UI, etc.) |
| 3.4 | Recovery procedures are complete | Recovery steps include: how to verify the fix, what data loss may have occurred, how to prevent recurrence |
| 3.5 | Production examples are specific | At least one example includes a company/team context, scale (data volume, throughput), and measured improvement |
| 3.6 | Optimization techniques include costs | Every optimization states what it trades away (memory for speed, latency for throughput, etc.) |

**Verdict:** All 6 checks must pass.

---

## Gate 4 — Diagram Review

| # | Check | Pass Criteria |
|---|---|---|
| 4.1 | All diagrams have explanation paragraphs | No diagram appears without an immediately following explanation |
| 4.2 | All nodes are labelled | No anonymous boxes or nodes |
| 4.3 | Mermaid diagrams compile | Paste each Mermaid block into mermaid.live — it must render without errors |
| 4.4 | ASCII diagrams use correct character set | Box-drawing characters from `DIAGRAM_STANDARDS.md`, not ASCII dashes and pipes |
| 4.5 | At least one ASCII whiteboard diagram | Required in First Principles or Internal Architecture |
| 4.6 | Diagrams reveal, not decorate | Each diagram shows structure or flow that the text alone cannot convey |

**Verdict:** All 6 checks must pass.

---

## Gate 5 — Interview Depth

| # | Check | Pass Criteria |
|---|---|---|
| 5.1 | Interview answers are staff-level | Answers reference mechanisms, numbers, and trade-offs — not surface definitions |
| 5.2 | Interview answers are spoken-form | Answers are written as prose a person would say aloud, not as bullet lists |
| 5.3 | Interview answers are 150–300 words | Not too short (shallow) and not too long (loses the interviewer) |
| 5.4 | Cross-question chain escalates correctly | Each follow-up requires deeper knowledge than the previous; the chain reaches the theoretical or system-level root of the topic |
| 5.5 | Questions cover failure modes | At least 2 of the interview questions are failure/debugging scenarios ("You see X. What do you do?") |
| 5.6 | Questions are realistic | Questions match what companies like Google, Meta, Databricks, Confluent, or Amazon ask in staff-level interviews |

**Verdict:** All 6 checks must pass.

---

## Gate 6 — Exercise Review

| # | Check | Pass Criteria |
|---|---|---|
| 6.1 | Lab exercises are runnable | Every lab can be run with standard tools (`pip install`, standard Python, a local Spark/Kafka setup) |
| 6.2 | Lab exercises have expected output | For every lab, the expected output is stated — not "results may vary" |
| 6.3 | Labs are progressive | Lab 1 is easier than Lab 2, which is easier than Lab 3 |
| 6.4 | Exercises are varied | The 5 exercises include a mix of conceptual, practical, and research tasks |
| 6.5 | Conceptual exercises have answer guidance | No conceptual exercise is left with "figure it out" — at minimum, the approach is hinted |
| 6.6 | Labs are self-contained | A lab does not depend on completing a previous lab unless explicitly stated |

**Verdict:** All 6 checks must pass.

---

## Gate 7 — Code Review

| # | Check | Pass Criteria |
|---|---|---|
| 7.1 | All code blocks have language tags | No untagged code blocks |
| 7.2 | All code is syntactically correct | Every code block can be parsed without errors |
| 7.3 | All code is semantically correct | Every code block, if run, produces the stated output (or is labelled `# PSEUDOCODE`) |
| 7.4 | Imports are explicit | No implicit imports; every library used is imported at the top of its code block |
| 7.5 | No magic numbers | Every constant has a comment explaining its value |
| 7.6 | Performance comparisons are fair | When comparing fast vs slow code, both versions must do the same thing |

**Verdict:** All 6 checks must pass.

---

## Gate 8 — Markdown Formatting

| # | Check | Pass Criteria |
|---|---|---|
| 8.1 | All 21 sections are present and in order | No missing or reordered sections |
| 8.2 | Heading levels are not skipped | No H2 → H4 jumps |
| 8.3 | Tables have aligned columns | All GFM tables render correctly |
| 8.4 | Bold is not decorative | Bold is used only for key terms being defined and critical warnings |
| 8.5 | Module footer is present | Last two lines link to next module and school index |
| 8.6 | File is `README.md` in correct folder | `MNN_Title/README.md` |

**Verdict:** All 6 checks must pass.

---

## Gate 9 — Cross-Reference Review

| # | Check | Pass Criteria |
|---|---|---|
| 9.1 | All cross-links are valid | Every `[text](../path)` link points to a file that exists |
| 9.2 | Prerequisites are cross-linked | Section 2 links to every prerequisite module |
| 9.3 | "Next module" link is correct | The next module in sequence is the correct dependency order per `MODULE_DEPENDENCY_GRAPH.md` |
| 9.4 | No orphan concepts | Every concept introduced in this module that is used in a later module is cross-referenced there |
| 9.5 | Flashcards reference terminology | Flashcard terms are consistent with terms used in the module body |

**Verdict:** All 5 checks must pass.

---

## Summary Scorecard

| Gate | Domain | Checks | Required |
|---|---|---|---|
| 1 | Technical Accuracy | 5 | All 5 |
| 2 | Logical Consistency | 5 | All 5 |
| 3 | Production Engineering Depth | 6 | All 6 |
| 4 | Diagram Review | 6 | All 6 |
| 5 | Interview Depth | 6 | All 6 |
| 6 | Exercise Review | 6 | All 6 |
| 7 | Code Review | 6 | All 6 |
| 8 | Markdown Formatting | 6 | All 6 |
| 9 | Cross-Reference Review | 5 | All 5 |
| **Total** | | **51** | **All 51** |

A module is ✅ Complete when all 51 checks pass. There are no partial passes.

---

## Quality Gate Enforcement Policy

**Self-review:** Before marking a module complete, the author runs through all 51 checks and marks each one explicitly.

**Revision triggers:** The following automatically revert a module to 🔄 In Progress:
- A factual error is discovered in Section 5 (First Principles) or Section 6 (Internal Architecture).
- A code block is found to produce incorrect output.
- A cross-link points to a non-existent file.
- A diagram fails to compile.
- A newer version of a technology changes a described behavior (e.g., a Spark configuration is deprecated).

**Version policy:** When a technology releases a major version that changes a described behavior, the affected module is flagged for revision in `PROGRESS.md` and reverted to 🔄 In Progress until updated.
