# Repository Structure

The complete GitHub repository layout for Data Engineering University. Every folder serves a defined purpose. Nothing is created speculatively.

---

## Root Structure

```
Data-Engg-University/
│
├── README.md                          University master index
├── CURRICULUM.md                      All schools, courses, modules — status
├── PROGRESS.md                        Per-learner completion tracker
│
├── university-design/                 Phase 2: University architecture documents
│   ├── 01_UNIVERSITY_HIERARCHY.md     Full tree (School → Department → Course → Module → Lesson)
│   ├── 02_SCHOOL_PROFILES.md          School metadata, objectives, prerequisites
│   ├── 03_SEMESTER_DESIGN.md          7 semesters with capstone + graduation criteria
│   ├── 04_COURSE_CATALOG.md           All 35 courses: purpose, prereqs, outcomes
│   ├── 05_MODULE_DEPENDENCY_GRAPH.md  Dependency chains with WHY explanations
│   ├── 06_LEARNING_PATHS.md           9 paths with overlap matrix
│   ├── 07_CAPSTONE_PROJECTS.md        7 projects with acceptance criteria
│   ├── 08_REPOSITORY_STRUCTURE.md     This document
│   ├── 09_SKILL_MATRIX.md             Skill progression per semester
│   ├── 10_GRADUATION_REQUIREMENTS.md  Level-by-level competency definitions
│   └── 11_CURRICULUM_REVIEW.md        Final review checklist
│
├── standards/                         Repository-wide conventions (law, not suggestions)
│   ├── REPOSITORY_STANDARDS.md        Markdown, naming, code, citation conventions
│   ├── MODULE_GENERATION_STRATEGY.md  21-section module template
│   ├── DIAGRAM_STANDARDS.md           Mermaid, PlantUML, ASCII rules
│   └── QUALITY_GATES.md               51-check gate every module must pass
│
├── assets/                            Shared non-module resources
│   ├── diagrams/                      Exported PNG/SVG diagrams
│   │   ├── architecture/              System architecture diagrams
│   │   ├── dependency-graphs/         Module and course dependency graphs
│   │   └── cheat-sheets-visual/       Visual cheat sheet exports
│   ├── flashcards/                    Anki-compatible TSV decks
│   │   ├── 00_cs_foundations.tsv
│   │   ├── 01_linux.tsv
│   │   ├── 02_networking.tsv
│   │   └── ...
│   └── cheat-sheets/                  Master cheat sheets per school (Markdown)
│       ├── 00_cs_foundations.md
│       ├── 06_sql.md
│       └── ...
│
├── templates/                         Reusable templates
│   ├── MODULE_TEMPLATE.md             Blank 21-section module template
│   ├── ADR_TEMPLATE.md                Architecture Decision Record template
│   ├── POSTMORTEM_TEMPLATE.md         Incident post-mortem template
│   ├── RUNBOOK_TEMPLATE.md            Operations runbook template
│   └── DATA_CONTRACT_TEMPLATE.md      Data contract template (schema + SLAs)
│
├── references/                        Curated reading list
│   ├── PAPERS.md                      Foundational papers with summaries
│   ├── BOOKS.md                       Canonical books with chapter-level DE relevance notes
│   └── TALKS.md                       Conference talks (Spark Summit, Kafka Summit, VLDB)
│
├── interview-workbooks/               Interview preparation material
│   ├── SQL_WORKBOOK.md                50 SQL problems with solutions and explanations
│   ├── PYTHON_DSA_WORKBOOK.md         30 Python/DSA problems
│   ├── SPARK_SCENARIOS.md             20 Spark debugging and tuning scenarios
│   ├── SYSTEM_DESIGN_WORKBOOK.md      10 system design problems with model answers
│   └── BEHAVIORAL_WORKBOOK.md         STAR framework + 30 DE-specific behavioral questions
│
├── case-studies/                      Real-world architecture case studies
│   ├── NETFLIX_DATA_MESH.md
│   ├── UBER_REAL_TIME_PLATFORM.md
│   ├── AIRBNB_MINERVA.md
│   ├── LINKEDIN_VENICE.md
│   └── PINTEREST_VISUAL_SIGNALS.md
│
├── production-incidents/              Documented failure scenarios (fictional but realistic)
│   ├── KAFKA_CONSUMER_LAG_STORM.md
│   ├── SPARK_SKEW_PRODUCTION.md
│   ├── AIRFLOW_THUNDERING_HERD.md
│   ├── DBT_SCHEMA_REGRESSION.md
│   ├── BIGQUERY_COST_EXPLOSION.md
│   └── ICEBERG_SNAPSHOT_ACCUMULATION.md
│
├── architecture-guides/               Architecture decision guides
│   ├── CHOOSING_A_TABLE_FORMAT.md
│   ├── BATCH_VS_STREAMING.md
│   ├── ORCHESTRATOR_COMPARISON.md
│   ├── CLOUD_STORAGE_SELECTION.md
│   └── DATA_MODELING_APPROACH.md
│
├── scripts/                           Utility scripts for the repository
│   ├── check_module_completeness.py   Validates a module against the 21-section template
│   ├── build_flashcard_deck.py        Extracts flashcard tables from modules into TSV
│   ├── generate_progress_report.py    Reads PROGRESS.md and generates a summary
│   └── validate_links.sh              Checks all internal cross-links for 404s
│
└── schools/                           All educational content
    ├── 00_CS_Foundations/
    ├── 01_Linux/
    ├── 02_Networking/
    ├── 03_Distributed_Systems/
    ├── 04_Databases/
    ├── 05_Data_Engineering/
    ├── 06_SQL/
    ├── 07_Python/
    ├── 08_Apache_Spark/
    ├── 09_PySpark/
    ├── 10_Streaming/
    ├── 11_Kafka/
    ├── 12_BigQuery/
    ├── 13_Airflow/
    ├── 14_dbt/
    ├── 15_Table_Formats/
    ├── 16_Data_Modeling/
    ├── 17_System_Design/
    ├── 18_Production_Engineering/
    └── 19_Interview_Preparation/
```

---

## School Folder Structure

Each school folder follows this pattern:

```
00_CS_Foundations/
│
├── README.md                          School overview: purpose, departments, courses
│
├── CSF-ARC-101_How_Computers_Execute_Programs/
│   ├── README.md                      Course overview: purpose, modules, prerequisites
│   ├── M01_Von_Neumann_Machine/
│   │   ├── README.md                  Full module (21 sections)
│   │   └── lab/
│   │       ├── lab1_fetch_decode_execute.py
│   │       ├── lab2_cache_line_effect.py
│   │       └── lab3_python_vs_numpy.py
│   ├── M02_CPU_Microarchitecture/
│   │   ├── README.md
│   │   └── lab/
│   └── ...
│
├── CSF-ARC-102_Memory_Architecture/
│   └── ...
│
└── CSF-OS-101_OS_Internals/
    └── ...
```

### Rationale for This Structure

The school folder maps to the university hierarchy: `school/course/module/`. The lab subfolder is adjacent to the module README so a student can run labs without leaving the module directory. Course READMEs give navigation context without duplicating content.

---

## Special Folders

### `interview-workbooks/`

Self-contained study guides for each interview type. These are NOT module content — they are curated problem sets with model answers.

- `SQL_WORKBOOK.md` — 50 problems organized by difficulty: basic (10) → intermediate (20) → advanced (10) → expert (10). Each problem: question, expected output, solution SQL, and explanation of the approach.
- `SPARK_SCENARIOS.md` — 20 scenarios a data engineer would encounter in a Spark interview ("Your job is OOM. What do you do?"). Model answers walk through the diagnosis, not just the fix.
- `SYSTEM_DESIGN_WORKBOOK.md` — 10 system design problems. Each: requirements, a worked solution with diagrams, and a list of follow-up questions the interviewer might ask.

### `production-incidents/`

Fictional but technically accurate incident reports. Each incident:
- Has a timeline.
- Has a technical root cause (always traceable to a concept in the curriculum).
- Has a post-mortem with lessons learned.
- Is cross-linked to the module that explains the underlying concept.

Purpose: give students practice reading post-mortems before they experience real incidents.

### `case-studies/`

Analysis of real architectural decisions made by data engineering teams at major companies. Each case study:
- Describes the problem the team faced.
- Describes the solution they chose.
- Explains the trade-offs (what they gained, what they gave up).
- Is cross-linked to the relevant modules.

Sources: engineering blog posts, conference talks, and published papers from each company.

### `architecture-guides/`

Decision frameworks for common architectural choices. These are not module content — they are synthesis documents that reference multiple modules and help a student choose between options in a real project. Each guide:
- Defines the decision being made.
- Lists the options.
- Provides a decision matrix with weighted criteria.
- Gives a recommendation for each common scenario.

---

## File Count Estimates (at full completion)

| Folder | Files (est.) |
|---|---|
| university-design/ | 11 |
| standards/ | 4 |
| assets/flashcards/ | 20 (one per school) |
| assets/cheat-sheets/ | 20 |
| assets/diagrams/ | 50–100 |
| templates/ | 5 |
| references/ | 3 |
| interview-workbooks/ | 5 |
| case-studies/ | 5–10 |
| production-incidents/ | 6–12 |
| architecture-guides/ | 5 |
| scripts/ | 4 |
| schools/ (all modules) | ~175 README.md + ~350 lab files |
| **Total** | **~680 files** |
