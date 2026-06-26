# Data Engineering University — Repository Structure

```
Data-Engg-University/
│
├── README.md                          ← Master index, quick-start, how to navigate
├── CURRICULUM.md                      ← All schools and modules with status
├── LEARNING_PATHS.md                  ← 4 guided paths (Interview / Production / Freelance / Staff)
├── MODULE_DEPENDENCY_GRAPH.md         ← Mermaid graph of prerequisites
├── PROGRESS.md                        ← Completion tracker (updated after every module)
│
├── assets/
│   ├── diagrams/                      ← Shared SVG / PNG / Mermaid sources
│   ├── flashcards/                    ← Master flashcard decks per school (Anki-compatible)
│   └── cheat_sheets/                  ← Master cheat sheets per school
│
├── 00_CS_Foundations/
│   ├── M01_How_Computers_Execute_Programs/
│   │   └── README.md                  ← Full module (objectives → flashcards)
│   ├── M02_Memory_and_Storage_Hierarchy/
│   ├── M03_Concurrency_and_Parallelism/
│   ├── M04_Networking_Fundamentals/
│   └── M05_OS_Internals/
│
├── 01_Linux/
│   ├── M01_Linux_Architecture/
│   ├── M02_File_System_and_IO/
│   ├── M03_Processes_and_Signals/
│   ├── M04_Shell_Scripting/
│   └── M05_Performance_Tools/
│
├── 02_Networking/
│   ├── M01_OSI_and_TCP_IP/
│   ├── M02_DNS_and_HTTP/
│   ├── M03_Load_Balancers_and_Proxies/
│   └── M04_Network_Security/
│
├── 03_Distributed_Systems/
│   ├── M01_CAP_Theorem/
│   ├── M02_Consensus_and_Raft/
│   ├── M03_Replication_and_Partitioning/
│   ├── M04_Distributed_Transactions/
│   └── M05_Observability/
│
├── 04_Databases/
│   ├── M01_Storage_Engines/
│   ├── M02_Indexing/
│   ├── M03_Query_Execution/
│   ├── M04_ACID_and_Isolation_Levels/
│   └── M05_Replication_and_Sharding/
│
├── 05_Data_Engineering/
│   ├── M01_Batch_vs_Streaming/
│   ├── M02_Data_Lake_Architecture/
│   ├── M03_ELT_vs_ETL/
│   ├── M04_Medallion_Architecture/
│   └── M05_Data_Contracts/
│
├── 06_SQL/
│   ├── M01_SQL_Execution_Internals/
│   ├── M02_Window_Functions/
│   ├── M03_Query_Optimization/
│   ├── M04_Advanced_Joins/
│   └── M05_Recursive_CTEs/
│
├── 07_Python/
│   ├── M01_Python_Internals/
│   ├── M02_Concurrency_asyncio_threading/
│   ├── M03_Memory_Management/
│   ├── M04_Data_Structures_and_Algorithms/
│   └── M05_Python_for_Data_Engineering/
│
├── 08_Apache_Spark/
│   ├── M01_Spark_Architecture/
│   ├── M02_RDD_vs_DataFrame_vs_Dataset/
│   ├── M03_Spark_Execution_Engine/
│   ├── M04_Shuffle_and_Partitioning/
│   └── M05_Spark_Optimization/
│
├── 09_PySpark/
│   ├── M01_PySpark_API/
│   ├── M02_PySpark_UDFs/
│   ├── M03_Structured_Streaming/
│   └── M04_Delta_Lake_with_PySpark/
│
├── 10_Streaming/
│   ├── M01_Stream_Processing_Fundamentals/
│   ├── M02_Windowing_and_Watermarks/
│   ├── M03_Exactly_Once_Semantics/
│   └── M04_Flink_vs_Spark_Streaming/
│
├── 11_Kafka/
│   ├── M01_Kafka_Architecture/
│   ├── M02_Producers_and_Consumers/
│   ├── M03_Kafka_Streams/
│   ├── M04_Kafka_Connect/
│   └── M05_Schema_Registry/
│
├── 12_BigQuery/
│   ├── M01_BigQuery_Architecture/
│   ├── M02_Storage_and_Columnar_Format/
│   ├── M03_Query_Optimization_BQ/
│   ├── M04_Cost_Management/
│   └── M05_BigQuery_ML/
│
├── 13_Airflow/
│   ├── M01_Airflow_Architecture/
│   ├── M02_DAGs_and_Operators/
│   ├── M03_Task_Dependencies/
│   ├── M04_Custom_Operators/
│   └── M05_Airflow_Production/
│
├── 14_dbt/
│   ├── M01_dbt_Architecture/
│   ├── M02_Models_and_Materializations/
│   ├── M03_Testing_and_Documentation/
│   └── M04_dbt_Advanced_Patterns/
│
├── 15_Iceberg_Delta_Hudi/
│   ├── M01_Table_Format_Comparison/
│   ├── M02_Apache_Iceberg/
│   ├── M03_Delta_Lake/
│   └── M04_Apache_Hudi/
│
├── 16_Data_Modeling/
│   ├── M01_Dimensional_Modeling/
│   ├── M02_Data_Vault/
│   ├── M03_One_Big_Table/
│   └── M04_Schema_Evolution/
│
├── 17_System_Design/
│   ├── M01_Design_a_Data_Pipeline/
│   ├── M02_Design_a_Streaming_Platform/
│   ├── M03_Design_a_Data_Warehouse/
│   └── M04_Design_a_Lakehouse/
│
├── 18_Production_Engineering/
│   ├── M01_Monitoring_and_Alerting/
│   ├── M02_CI_CD_for_Data/
│   ├── M03_Data_Quality_Frameworks/
│   └── M04_Incident_Response/
│
└── 19_Interview_Preparation/
    ├── M01_Coding_Interview/
    ├── M02_System_Design_Interview/
    └── M03_Behavioral_Interview/
```

## Conventions

| Item | Convention |
|---|---|
| School prefix | `NN_School_Name/` (zero-padded) |
| Module prefix | `MNN_Topic_Name/` inside each school |
| Module entry point | `README.md` inside each module folder |
| Diagrams | Mermaid inside `README.md`; PNG exports in `assets/diagrams/` |
| Flashcards | Anki-compatible TSV in `assets/flashcards/<school>.tsv` |
| Cheat sheets | Markdown in `assets/cheat_sheets/<school>.md` |
| Progress | ✅ Complete · 🔄 In Progress · 📋 Planned |
