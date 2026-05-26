# Sources & Sinks: Kafka, File System, & Delta Lake Stream Integrations

## 1. Executive Overview

### Why This Topic Exists
A streaming application is only as stable as its connections to external systems. In Structured Streaming, data inputs and outputs are managed by **Sources** and **Sinks**. 

This module covers the internal mechanics of streaming connectors (particularly **Kafka**, **File System**, and **Delta Lake**), how to configure rates to prevent memory exhaustion, and how to write custom sinks using the **`foreachBatch`** API.

### Production Problem Solved
1. **Source Flooding:** Prevents memory crashes when a stream starts up after downtime by limiting batch sizes.
2. **Transactional Writes:** Guarantees data is written to sinks exactly-once using transactional commit logs.
3. **Multi-System Writing:** Allows saving streaming outputs to multiple target systems (e.g., a database and a search index) simultaneously.

### Why Senior Engineers Care
Data engineers must connect streaming pipelines to enterprise message brokers and storage layers. Improper connector settings (such as omitting partition limits on Kafka sources) can overload executors. Knowing how Spark manages connection brokers, commits transaction metadata, and executes micro-batch iterations is essential.

### Common Misconceptions
* *“Using `foreachBatch` is identical to standard batch processing.”*
  **Reality:** While `foreachBatch` allows applying batch APIs to streaming micro-batches, it executes inside the streaming query context. You must configure checkpointing and offsets to maintain exactly-once guarantees.
* *“Delta Lake streaming sinks require manual table lock management.”*
  **Reality:** Delta Lake natively supports concurrent ACID transactions. Multiple streaming writes can target the same Delta table simultaneously; Delta Lake resolves lock conflicts automatically.

---

## 2. Internal Architecture Deep Dive

Spark structures connections using Source and Sink interfaces:

```mermaid
graph TD
    subgraph sg_1 ["Ingestion (Sources)"]
        Kafka[Kafka Source: tracks partition offsets]
        File[File Source: scans directory file paths]
        DeltaSrc[Delta Lake Source: reads table transaction log]
    end

    subgraph sg_2 ["Streaming Engine"]
        Kafka & File & DeltaSrc --> Engine[Spark Structured Streaming Engine]
    end

    subgraph sg_3 ["Output (Sinks)"]
        Engine --> DeltaSink[Delta Lake Sink: ACID Appends]
        Engine --> KafkaSink[Kafka Sink: publish message streams]
        Engine --> FEB[foreachBatch Sink: custom multi-system targets]
    end
```

### 1. Kafka Source Mechanics
* **Offset Tracking:** The driver coordinates Kafka consumer client connections. Instead of relying on Kafka's internal consumer groups, Spark tracks partition offsets directly in its checkpoint directory.
* **Rate Limiting:** Protects executor memory from flooding by restricting batch sizes:
  `spark.sql.streaming.kafka.maxOffsetsPerTrigger` (limits the number of messages processed per partition in a single trigger batch).

### 2. Delta Lake Source & Sink
* **Delta Source:** Scans the table's transaction log (`_delta_log/`) to detect new commit files, reading only the added parquet files.
* **Delta Sink:** Writes data as parquet files and commits a new transaction log entry. It supports three output modes:
  * **Append:** Appends new rows to the table (default).
  * **Complete:** Overwrites the entire table with each batch (useful for aggregates).
  * **Update:** Updates changed rows in-place (requires supported formats).

---

## 3. Physical Execution Walkthrough

Let's analyze the physical plan of a query that reads from Kafka and writes to Delta:

```python
# Spark SQL Query
df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_events") \
    .load()

query = df_stream.writeStream.format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints") \
    .start("/data/silver_events")
```

### Physical Plan Analysis
The physical plan reveals the V2 streaming scan and write operators:

```
== Physical Plan ==
WriteToDataSourceV2 delta
+- * Project [codegen id : 1]
   +- MicroBatchScan[key#0, value#1, topic#2, partition#3, offset#4] KafkaSource[Subscribe[raw_events]]
```

### Execution Steps
1. **MicroBatchScan:** The driver connects to Kafka, fetches the target partition offsets, and logs the range in the checkpoint.
2. **Project:** Serializes keys and values into Tungsten UnsafeRows.
3. **WriteToDataSourceV2:** Executors fetch the records from Kafka, write them to parquet files under `/data/silver_events`, and commit the transaction to Delta Lake's `_delta_log/` to complete the batch.

---

## 4. Distributed Systems Perspective

### Kafka Partition-Task Alignment
When reading from Kafka, Spark aligns task counts to Kafka partitions. If a Kafka topic has 32 partitions, Spark launches exactly 32 parallel tasks during the `MicroBatchScan` phase.
* **Tuning:** If you have 64 executor cores, 32 partitions will leave half of your cores idle. Increase the Kafka topic partition count or apply a repartition step after ingestion to maximize parallelism.

---

## 5. Performance Engineering Section

### Dynamic Ingestion Tuning
To prevent memory overload when a stream starts up after downtime, configure the following safety settings:
```properties
# Limit max offsets processed per trigger interval (prevents OOM)
spark.sql.streaming.kafka.maxOffsetsPerTrigger        100000
# Starting offset configuration for new streams
spark.sql.streaming.kafka.startingOffsets             latest
# Min partition count for file system directory scans
spark.sql.files.minPartitionNum                       20
```

---

## 6. Spark UI & Debugging Analysis

Open the **Structured Streaming Tab** in the Spark UI to debug source throughput:

* **Input Rate vs. Process Rate:** Monitor the rate trends. If Process Rate drops while Input Rate remains high, check for memory spills or network timeouts in the Stages tab.
* **Offsets Checked:** Click on the query description. Verify the processed offsets map to your Kafka partitions, confirming all topic streams are active.

---

## 7. Real Production Scenarios

### Case Study: Optimizing a Kafka Ingestion Stream after a 4-Hour Outage
An enterprise ingestion pipeline consumed clickstream logs from a Kafka topic (64 partitions).
* **The Problem:** After a 4-hour Kafka broker outage, the pipeline was restarted. It ran for 2 minutes and crashed with executor JVM out-of-memory errors.
* **The Root Cause:** The pipeline had no rate limits configured. Upon restart, Spark attempted to ingest the entire 4-hour backlog (50 million records) in a single micro-batch, overloading the executor memory.
* **The Solution:**
  1. Configured rate limits:
     `spark.sql.streaming.kafka.maxOffsetsPerTrigger=50000` (limits ingestion to 50k messages per partition per batch).
* **Result:** The pipeline processed the backlog stably over several small, fast batches and recovered without memory issues.

---

## 8. Failure & Incident Scenarios

### Incident: Duplicate records in Delta Lake due to unclean sink retries
* **Symptom:** Downstream reports show duplicate records for specific transaction IDs.
* **Logs:**
```
26/05/25 14:06:12 WARN StreamExecution: Task 0.0 in stage 1.0 failed, retrying.
26/05/25 14:06:12 INFO DeltaLog: Transaction committed successfully for version 12.
```
* **Root-Cause Analysis:** The pipeline used a custom database sink inside `foreachBatch`. During a task run, the executor wrote records to the database but crashed before completing the batch. The driver retried the task, writing the same records again, creating duplicates because the database lacked unique upsert keys.
* **Remediation:** 
  Ensure your target database tables have unique constraints (primary keys) and use upsert/merge statements inside `foreachBatch` to ensure writes are idempotent.

---

## 9. Hands-On Labs

### Lab Setup
Ensure you run this lab within the PySpark Jupyter notebook environment.

### 1. Beginner Lab: Writing a ForeachBatch Sink
Write a streaming query that reads from a local directory source and uses `foreachBatch` to print the count of each micro-batch.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SinkLab").master("local[*]").getOrCreate()

# Create input schema
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([StructField("message", StringType(), True)])

# Stream Source
df = spark.readStream.schema(schema).text("c:/Users/a/Desktop/pyspark/data/stream_input/")

# Custom foreachBatch function
def process_batch(batch_df, batch_id):
    print(f"Batch ID: {batch_id}, Row Count: {batch_df.count()}")
    batch_df.show(3)

# Write stream
query = df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.stop()
```

### 2. Intermediate Lab: Kafka Stream Mocking
If Kafka is available, configure a Kafka stream reader, deserialize the `value` column from binary to string, and write the output to a local Delta table.

```python
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
```

### 3. Advanced Lab: Idempotent Merge in ForeachBatch
Write a script that uses `foreachBatch` to merge streaming records into an existing Delta table using MERGE INTO statements, preventing duplicate records.

---

## 10. Benchmarking & Profiling

We benchmark latency and throughput under different streaming sinks (10 million events):

| Sink Type | Transactional Safety | End-to-End Latency | Max Throughput |
| :--- | :--- | :--- | :--- |
| **Delta Lake Sink** | Yes (ACID Commit Logs) | 120 ms | High |
| **Kafka Sink** | Yes (At-Least-Once) | 80 ms | Very High |
| **Console Sink** | No | 450 ms | Low |

---

## 11. Advanced Optimization Patterns

### Schema Enforcement in Delta Sinks
To allow streaming writes to adapt to changing schemas automatically, enable schema merges in your write streams:
```python
query = df.writeStream.format("delta") \
    .option("checkpointLocation", "/checkpoints") \
    .option("mergeSchema", "true") \
    .start("/data/delta_table")
```

---

## 12. Senior-Level Interview Section

### Q1: Why is configuring `maxOffsetsPerTrigger` considered a critical best practice when streaming from Kafka?
* **Answer:** If a streaming pipeline experiences downtime, records accumulate in Kafka. Upon restart, Spark will attempt to ingest the entire backlog in a single micro-batch. Without `maxOffsetsPerTrigger` limits, this massive ingestion can overload the executor memory, causing OOM crashes.

### Q2: How does the `foreachBatch` API enable integration with non-streaming storage systems? What are the lifecycle implications?
* **Answer:** `foreachBatch` allows developers to process each streaming micro-batch as a standard batch DataFrame. This enables the use of non-streaming APIs (like caching, partition re-ordering, and saving to multiple databases). The lifecycle implication is that you must handle idempotent writes manually, as the target storage system may not support Spark's checkpoint commit protocols.

---

## 13. Production Design Patterns

### The Medallion Ingestion Pattern
In enterprise data platforms, raw events are ingested into a Bronze Delta table (Append mode). A downstream streaming job reads from Bronze, cleans the records, and writes them to a Silver Delta table, optimizing data layouts.

---

## 14. Comparison Section

| Feature | Delta Lake Sink | Kafka Sink | File System Sink |
| :--- | :--- | :--- | :--- |
| **Transaction Logs** | Yes (ACID) | No | Yes (Metadata) |
| **Output Modes** | Append, Update, Complete | Append | Append |
| **Latency Profile** | Low | Very Low | Low |

---

## 15. Expert-Level Mental Models

### The Ingestion Flow Gate Model
An elite engineer visualizes the ingestion process as a water gate. They configure rate limits to ensure incoming data flows match executor processing capacities, preventing memory overflows.

---

## 16. Final Mastery Checklist

* [ ] Can write structured streaming queries from Kafka and File sources.
* [ ] Understands the role of Delta Lake transactional commit logs.
* [ ] Knows how to use `foreachBatch` to write custom sinks.
* [ ] Can configure rate limits to prevent executor memory exhaustion.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ Structured Streaming Engine: Micro-Batching vs. Continuous Processing](41_structured_streaming_engine.md) | [▶️ Stream State Management: flatMapGroupsWithState & Stateful Operations](43_stateful_operators.md) |
<!-- END_NAVIGATION_LINKS -->
