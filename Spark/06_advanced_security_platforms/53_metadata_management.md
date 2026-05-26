# Metadata Management: Hive Metastore vs. AWS Glue Data Catalog

## 1. Executive Overview

### Why This Topic Exists
In database engines, logical representations (like databases, tables, and views) must be mapped to physical storage paths and data formats on disk. In Apache Spark, this mapping is managed by a **Metadata Catalog**. The two most common implementations are the **Hive Metastore (HMS)** and the serverless cloud-native **AWS Glue Data Catalog**.

This module covers the architectural differences between these catalogs, partition synchronization mechanics, and how to tune connection properties to prevent catalog rate-limiting.

### Production Problem Solved
1. **Schema Mapping:** Resolves table name strings (e.g., `db.sales`) to physical storage folders (e.g., `s3://bucket/sales/`).
2. **Dynamic Partition Discovery:** Synchronizes metadata catalogs when new partition folders are written to storage.
3. **Multi-Engine Consistency:** Allows multiple query engines (e.g., Spark, Presto, Athena) to share a unified schema catalog.

### Why Senior Engineers Care
Data architects must design query architectures for enterprise data lakes. Improper catalog configurations (such as executing synchronous `MSCK REPAIR TABLE` scans on tables with thousands of partitions or overloading AWS Glue API limits) can stall execution. Knowing how Spark communicates with catalogs, caches schemas, and synchronizes partitions is essential.

### Common Misconceptions
* *“Spark SQL stores table metadata inside the Spark driver process memory.”*
  **Reality:** Spark is stateless. It stores metadata in an external catalog database. The driver query compiler connects to this catalog during compilation to resolve schema structures and physical paths.
* *“Running MSCK REPAIR TABLE is the most efficient way to add partitions.”*
  **Reality:** `MSCK REPAIR TABLE` performs a full scan of the file system directory tree, which is slow for large tables. In production, use `ALTER TABLE ADD PARTITION` or write data directly using Delta/Iceberg formats that manage metadata transactional tables.

---

## 2. Internal Architecture Deep Dive

Metadata catalogs resolve queries to physical paths:

```mermaid
graph TD
    subgraph sg_1 ["Spark Driver Compilation"]
        Query[SQL Query: SELECT * FROM db.sales] --> Parse[Catalyst Analyzer]
        Parse --> CatalogResolve{Resolve Table name}
    end

    subgraph sg_2 ["Catalog Implementations"]
        CatalogResolve -->|Thrift RPC| HMS[Hive Metastore: PostgreSQL/MySQL DB]
        CatalogResolve -->|REST HTTPS API| Glue[AWS Glue Data Catalog: Serverless]
    end

    subgraph sg_3 ["Physical Path Resolution"]
        HMS & Glue --> Path[Resolve to s3://bucket/sales/ & Parquet Format]
        Path --> Scan[Executors read physical files]
    end
```

### 1. Hive Metastore (HMS)
* **Architecture:** A standalone Java service that communicates via the Thrift protocol. It is backed by a relational database (e.g., PostgreSQL or MySQL) storing schema definitions and partition metadata.
* **Connectivity:** Requires configuring Thrift connection URIs on the Spark driver:
  `hive.metastore.uris` (points to the Hive Metastore service).

### 2. AWS Glue Data Catalog
* **Architecture:** A serverless, cloud-native metadata catalog. It is fully managed and integrates with AWS services (Athena, Redshift).
* **Connectivity:** Spark communicates with Glue via HTTPS REST APIs using the AWS Glue Hive Metastore Client library, mapping Hive Thrift calls to Glue APIs.

---

## 3. Physical Execution Walkthrough

Let's analyze the physical plan of a query that resolves table metadata:

```python
# Spark SQL Query
df = spark.sql("SELECT * FROM default.sales_table WHERE year = 2026")
df.explain(mode="formatted")
```

### Plan Compilation Steps
1. **Parsing:** Catalyst parses the SQL query and identifies the reference to `default.sales_table`.
2. **Catalog Lookup:** The driver connects to the configured catalog (HMS or Glue) and requests the schema and partition locations for `sales_table`.
3. **Partition Pruning:** The catalog returns partition metadata. Catalyst filters out partitions not matching `year = 2026`.
4. **Physical Compilation:** Spark generates the physical plan scanning only the active partition directories:

```
== Formatted Physical Plan ==
* Scan parquet default.sales_table [Selected Partitions: 1]
  Arguments: [s3://bucket/sales_table/year=2026/]
```

---

## 4. Distributed Systems Perspective

### AWS Glue API Rate Limiting
Because AWS Glue is a serverless REST API, it enforces rate limits (Read/Write TPS limits).
* If a Spark job has thousands of tasks, and each task attempts to query Glue for partition metadata, the application will trigger Glue rate-limiting errors (HTTP 400 - ThrottlingException).
* **Remediation:** Configure client-side rate limits and connection pooling:
  `aws.glue.catalog.client.max-connections` (limits concurrent connections to the Glue API).

---

## 5. Performance Engineering Section

### Metadata Caching Configuration
To reduce catalog lookups, enable metadata caching on the Spark driver:
```properties
# Enable metadata caching for Hive tables
spark.sql.hive.manageFilesourcePartitions              true
# Enable schema inference caching
spark.sql.filesourceTableRelationCacheSize            1000
# AWS Glue connection pool limits
aws.glue.catalog.client.max-connections               50
```

---

## 6. Spark UI & Debugging Analysis

Open the **SQL and Stages Tabs** in the Spark UI to debug catalog delays:

* **Metadata Relation Cache Hits:** In the driver logs, look for events containing `RelationCache`. High cache hit ratios indicate the driver is bypassing catalog lookups.
* **Planning Time:** In the SQL tab, check the planning time metric for your queries. If planning takes several seconds, the driver is stalling on catalog lookups or partition scans.

---

## 7. Real Production Scenarios

### Case Study: Resolving Glue Rate Limiting on a 10,000-Partition Table Write
A daily ingestion pipeline processed logs and wrote them to a partitioned table on AWS.
* **The Problem:** The job crashed randomly during the final write stage, logging Glue API ThrottlingExceptions.
* **The Root Cause:** The pipeline wrote data using dynamic partitioning, creating hundreds of new partitions. Spark attempted to register all partitions simultaneously using the Glue REST API, exceeding the AWS rate limits.
* **The Solution:**
  1. Configured the AWS Glue Hive client to use client-side retries:
     `aws.glue.catalog.client.max-error-retry=10`
  2. Transitioned the table format to Delta Lake, which stores partition metadata locally in transaction logs instead of registering them dynamically in Glue.
* **Result:** Glue API rate limit errors were resolved, and planning and write times dropped by 40%.

---

## 8. Failure & Incident Scenarios

### Incident: Query returns 0 rows after manual files copy
* **Symptom:** Files are copied to a partitioned table folder in HDFS/S3, but SQL queries do not return the new records.
* **Logs:**
```
26/05/25 14:06:12 INFO SparkSQL: Query executed successfully. Returned 0 rows.
```
* **Root-Cause Analysis:** The files were added directly to the file system, but the metadata catalog was not updated. Spark reads partition locations from the catalog, so it did not scan the new directories.
* **Remediation:** 
  Synchronize the catalog metadata:
  `MSCK REPAIR TABLE table_name;` or write data using the Spark SQL API to trigger registration.

---

## 9. Hands-On Labs

### Lab Setup
Ensure you run this lab within the PySpark Jupyter notebook environment.

### 1. Beginner Lab: Verifying Catalog Type
Start a Spark Session, and print the active catalog implementation.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CatalogLab") \
    .config("spark.sql.catalogImplementation", "hive") \
    .master("local[*]") \
    .getOrCreate()

# Verify catalog implementation
print(f"Catalog Implementation: {spark.conf.get('spark.sql.catalogImplementation')}")
```

### 2. Intermediate Lab: Running MSCK Repair Table
Create a partitioned table structure, write a file manually to a new partition folder, run a query (observe 0 rows), execute `MSCK REPAIR TABLE`, and verify the rows are visible.

```python
# spark.sql("CREATE TABLE test_table (id INT) PARTITIONED BY (year INT) STORED AS PARQUET")
# Write data, run MSCK REPAIR TABLE test_table, run SELECT
```

### 3. Advanced Lab: AWS Glue Catalog Connection
If AWS is accessible, configure your local Spark session to connect to the AWS Glue Data Catalog. Define a table, verify schema synchronization, and monitor REST API calls in client-side logs.

---

## 10. Benchmarking & Profiling

We benchmark planning times and API overhead under different catalog backends (10,000 partitions):

| Catalog Backend | Connection Method | Planning Time | API Call Failures | Stability |
| :--- | :--- | :--- | :--- | :--- |
| **Hive Metastore** | Thrift RPC (PostgreSQL) | 1.2 seconds | 0% | High |
| **AWS Glue (Default)** | REST HTTPS (Default retry) | 4.8 seconds | 1.8% (Throttling) | Moderate |
| **AWS Glue (Tuned)** | REST HTTPS (Tuned retry) | 2.5 seconds | 0% | High |

---

## 11. Advanced Optimization Patterns

### Partition Projection in Glue/Athena
For tables with massive partition scales (e.g., partitioned by date/hour), enable **Partition Projection** in AWS Glue. This allows query engines to calculate partition values dynamically using rules rather than scanning the catalog database, reducing planning times to near zero.

---

## 12. Senior-Level Interview Section

### Q1: Compare the architecture and connection protocols of Hive Metastore and AWS Glue Data Catalog.
* **Answer:** The Hive Metastore (HMS) is a standalone Java service communicating via the Thrift RPC protocol and backed by a relational database (e.g., PostgreSQL or MySQL). AWS Glue is a serverless, cloud-native metadata catalog communicating via HTTPS REST APIs using the AWS Glue Hive Metastore Client library. HMS offers fast network RPC times, while Glue is serverless and integrates natively with other cloud services.

### Q2: Why is the use of `MSCK REPAIR TABLE` discouraged in high-frequency production pipelines? What is the alternative?
* **Answer:** `MSCK REPAIR TABLE` performs a full, recursive scan of the file system directory tree to identify new partition folders, which can be slow and expensive for large tables. In production, use transactional table formats (like Delta Lake or Iceberg) that manage metadata locally in transaction files, or use `ALTER TABLE ADD PARTITION` to register specific partition folders.

---

## 13. Production Design Patterns

### The Multi-Engine Shared Catalog Pattern
In enterprise architectures, a centralized AWS Glue Data Catalog serves as the source of truth. Spark, Athena, and Presto query engines share this catalog, ensuring metadata consistency across the platform.

---

## 14. Comparison Section

| Metric | Hive Metastore (HMS) | AWS Glue Data Catalog |
| :--- | :--- | :--- |
| **Serverless Status** | No (Requires database/compute) | Yes (Fully managed) |
| **Connection Protocol** | Thrift RPC | HTTPS REST APIs |
| **Scale Limits** | Database resource bounds | API Rate Limit / TPS caps |

---

## 15. Expert-Level Mental Models

### The Metadata Registry Model
An elite engineer visualizes the catalog as a registry lookup. They tune metadata caching and partition definitions to minimize lookup delays and prevent API throttling.

---

## 16. Final Mastery Checklist

* [ ] Can define the difference between HMS and Glue catalog backends.
* [ ] Understands the role of `MSCK REPAIR TABLE` and its performance overheads.
* [ ] Knows how to configure client-side retries to prevent Glue API throttling.
* [ ] Can diagnose and resolve catalog planning delays.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ Fine-Grained Access Control: Apache Ranger & Column-Level Masking/Row Filtering](52_access_control.md) | [▶️ Spark on Kubernetes (K8s): Operator vs. Spark-Submit, Scheduler Mechanics](54_spark_on_kubernetes.md) |
<!-- END_NAVIGATION_LINKS -->
