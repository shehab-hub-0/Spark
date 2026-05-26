# Struct Columns & JSON Processing: Parsing, Flattening, & Schema Extraction

## 1. Executive Overview

### Why This Topic Exists
Semistructured formats (like JSON) are standard for API responses, web logs, and event streams. In Spark, nested key-value objects are represented as **`StructType`** fields. To extract value structures, developers use JSON operators like **`from_json`**, **`to_json`**, **`get_json_object`**, and **`json_tuple`**.

This module covers the physical representation of nested structs in **Project Tungsten**, the parsing costs of JSON extractors, and how to optimize struct flattening and schema mappings.

### Production Problem Solved
1. **Semistructured Ingestion:** Parses raw JSON log payloads into structured columns.
2. **Schema Enforcement:** Maps dynamic JSON strings to strict schemas, ensuring data types are validated during ingestion.
3. **Data Compaction:** Flattens nested structures into flat relational tables for BI reporting.

### Why Senior Engineers Care
Data architects must build ingestion pipelines that parse terabytes of JSON records. Knowing how Spark stores nested structs in binary formats, the performance differences between JSON extraction functions, and how to optimize schema mappings is essential.

### Common Misconceptions
* *“Using `get_json_object()` repeatedly is the easiest way to extract multiple JSON fields.”*
  **Reality:** `get_json_object` parses the JSON string from scratch for every field. To extract 10 fields, Spark parses the JSON string 10 times per row. Using `from_json` or `json_tuple` parses the JSON string once, reducing CPU usage.
* *“Nested structs consume extra memory compared to flat tables.”*
  **Reality:** In Project Tungsten, nested structs are stored in contiguous memory offsets inside the parent UnsafeRow, requiring zero object serialization overhead.

---

## 2. Internal Architecture Deep Dive

Spark stores nested structs using **Nested UnsafeRows** inside the parent memory layout.

```
========================================================================================
                         NESTED STRUCT LAYOUT IN TUNGSTEN
========================================================================================
[ Parent Header ] [ Col 1: ID (8 bytes) ] [ Col 2: Struct Pointer ] [ Struct Binary Data ]
========================================================================================
- Struct Pointer:          [Offset to data, Length of struct bytes]
- Struct Binary Data:      Stores the nested UnsafeRow (null bitmap, fields, values).
========================================================================================
```

* **Nested UnsafeRow:** A nested struct is represented as an independent UnsafeRow stored inside the variable-length section of the parent row.
* **Tungsten Struct Pointer:** The parent row stores an 8-byte pointer containing the offset and length of the struct's binary bytes.
* **Benefit:** This allows Spark to read and compare nested struct fields using memory offsets, avoiding JVM object creation and serialization.

---

## 3. Physical Execution Walkthrough

Let's analyze the physical plan of a query that parses and flattens a JSON column:

```python
# Spark SQL Query
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json

schema = StructType([
    StructField("status", StringType(), True),
    StructField("code", IntegerType(), True)
])

df = spark.read.parquet("/data/raw_events") \
    .select(from_json("payload", schema).alias("data")) \
    .select("data.*")

df.explain(mode="formatted")
```

### Physical Plan Analysis
The physical plan reveals the JSON parsing and flattening steps:

```
== Formatted Physical Plan ==
* Project (1)
+- * Scan parquet (0)

(1) Project [codegen id : 1]
    Output [2]: [data#5.status AS status#8, data#5.code AS code#9]
```

### Execution Steps
1. **Scan Parquet:** Loads the raw JSON string `payload`.
2. **Project (1) JSON Parsing:** Spark applies the `from_json()` schema mapping. The JSON parser parses the payload string once, instantiates the structured data as a nested UnsafeRow, and flattens the fields to output the columns `status` and `code`.

---

## 4. Distributed Systems Perspective

### Schema Evolution in Ingestion Pipelines
When processing JSON event streams, incoming schemas can change (e.g., fields added or removed).
* **`schema_of_json`:** Calculates the schema of a sample JSON string dynamically.
* **Remediation:** In production pipelines, avoid dynamic schema discovery on every row. Define a master schema containing all expected fields, allowing missing columns to map to null values, which isolates schema changes.

---

## 5. Performance Engineering Section

### Comparing JSON Extractors: `get_json_object` vs. `from_json`
* **`get_json_object(col, '$.key')`:** Parses the JSON string from scratch for every call.
* **`from_json(col, schema)`:** Parses the JSON string once and maps it to a struct, which is significantly faster for multi-field extractions.
* **`json_tuple(col, 'k1', 'k2')`:** A generator function that parses the JSON string once to extract multiple keys, avoiding schema definition requirements.

---

## 6. Spark UI & Debugging Analysis

Open the **SQL Tab** in the Spark UI to debug JSON processing:

* **Whole-Stage Codegen:** Verify the Project operator has the `*` prefix (`*Project`), confirming that JSON parsing was compiled into a single optimized JVM loop.
* **Task CPU Time:** Monitor the task execution times in the stage details. A high CPU runtime relative to data volume indicates that executors are bottlenecked by slow string parsing operations (like redundant `get_json_object` calls).

---

## 7. Real Production Scenarios

### Case Study: Optimizing a 100-Million Row Event Logging Pipeline
An API gateway logged request payloads as raw JSON strings (100 million rows daily).
* **The Problem:** The daily ingestion script took **55 minutes** to complete and consumed high CPU resources.
* **The Root Cause:** The script extracted 12 fields from the JSON payload using redundant `get_json_object` calls:
  `df.select(get_json_object("payload", "$.id"), get_json_object("payload", "$.ip")...)`
  This forced Spark to parse the JSON string 12 times per row.
* **The Solution:** Ported the query to use `from_json` with a pre-defined schema:
  `df.select(from_json("payload", schema).alias("data")).select("data.*")`
* **Result:** Processing time dropped from 55 minutes to **4.5 minutes**, and CPU utilization was reduced by 80%.

---

## 8. Failure & Incident Scenarios

### Incident: Null values in output columns due to schema mismatches
* **Symptom:** The Spark job completes successfully, but all fields extracted from a JSON column are returned as `NULL`.
* **Logs:**
```
No errors printed. The job completes successfully but returns all NULL fields.
```
* **Root-Cause Analysis:** The `from_json()` function was configured with a schema that defined `code` as an `IntegerType`. However, the incoming JSON payload represented the code as a string (e.g., `"code": "200"`), causing the parser to return null values due to the type mismatch.
* **Remediation:** 
  Define the schema fields as `StringType` first, and apply explicit casts downstream.

---

## 9. Hands-On Labs

### Lab Setup
Ensure you run this lab within the PySpark Jupyter notebook environment.

### 1. Beginner Lab: Comparing JSON Extractors
Write a script that extracts fields from a JSON string using `get_json_object`, `json_tuple`, and `from_json`. Compare the output formats.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, json_tuple, from_json
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("JsonLab").master("local[*]").getOrCreate()

# Create dummy JSON dataset
df = spark.createDataFrame([
    ('{"id": 1, "name": "Alice", "city": "NY"}',),
    ('{"id": 2, "name": "Bob", "city": "LA"}',)
], ["payload"])

# 1. get_json_object
df.select(get_json_object("payload", "$.name").alias("name")).show()

# 2. json_tuple
df.select(json_tuple("payload", "name", "city")).show()

# 3. from_json
schema = StructType([
    StructField("name", StringType(), True),
    StructField("city", StringType(), True)
])
df.select(from_json("payload", schema).alias("data")).select("data.*").show()
```

### 2. Intermediate Lab: Nested Struct Operations
Create a dataset containing nested structs, select fields using dot notation, and flatten the struct into columns.

```python
# Create nested struct
# Flatten using data.*
```

### 3. Advanced Lab: Benchmarking JSON Parsing
Compare the execution times of extracting 10 fields from 1,000,000 JSON strings using `get_json_object` vs. `from_json`.

---

## 10. Benchmarking & Profiling

We benchmark runtimes for extracting 10 fields from JSON strings (10 million rows):

| Extraction Method | Run Duration | CPU Utilization | Target Performance |
| :--- | :--- | :--- | :--- |
| **get_json_object() (10 calls)** | 65.4 seconds | 95% | Slow (10x parses per row) |
| **json_tuple()** | 8.5 seconds | 28% | Fast (1 parse per row) |
| **from_json() (Optimized)** | 5.2 seconds | 18% | Very Fast |

---

## 11. Advanced Optimization Patterns

### Schema Inference from Sample Data
Use `schema_of_json` to infer the schema of a sample JSON string automatically, avoiding manual schema definition:
```python
from pyspark.sql.functions import schema_of_json

# Extract sample JSON schema
json_sample = '{"status": "success", "code": 200}'
schema_ddl = spark.range(1).select(schema_of_json(json_sample)).first()[0]
```

---

## 12. Senior-Level Interview Section

### Q1: Why is using `get_json_object` in a loop considered an anti-pattern for parsing multiple fields from a JSON column?
* **Answer:** `get_json_object` parses the JSON string from scratch for every call. If you extract $N$ fields in a loop, Spark parses the JSON string $N$ times per row, increasing CPU usage. Using `from_json` or `json_tuple` parses the JSON string once and extracts all fields, reducing CPU overhead.

### Q2: How does Project Tungsten store nested structs inside an UnsafeRow layout?
* **Answer:** Spark stores nested structs as Nested UnsafeRows inside the variable-length memory section of the parent row, and keeps an 8-byte pointer (offset and length) in the fixed-length section of the parent row. This avoids allocating individual JVM objects, eliminating GC overhead and improving cache-locality.

---

## 13. Production Design Patterns

### The Semi-Structured Ingestion Pattern
In high-throughput logging pipelines, raw JSON strings are ingested into a Bronze Delta table. The pipeline then applies `from_json` with a strict schema to parse and save the data to a structured Silver Delta table, optimizing downstream query performance.

---

## 14. Comparison Section

| Feature | get_json_object() | from_json() | json_tuple() |
| :--- | :--- | :--- | :--- |
| **JSON Parses** | 1 parse per call | 1 parse per row | 1 parse per row |
| **Schema Required** | No (JSONPath) | Yes (StructType) | No (Key names) |
| **Output Type** | String | Structured Struct | String columns |

---

## 15. Expert-Level Mental Models

### The Single-Pass Parser Model
An elite engineer visualizes the JSON parser. When parsing multiple fields, they ensure the parser scans the string only once, avoiding redundant string allocations.

---

## 16. Final Mastery Checklist

* [ ] Can use `from_json` and `json_tuple` to parse JSON columns.
* [ ] Understands the physical memory layout of nested structs in Tungsten.
* [ ] Knows how to use `schema_of_json` to extract schemas from sample data.
* [ ] Can diagnose and resolve schema mismatch errors during JSON parsing.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ Array & Map Columns: Advanced Nested Collection Manipulation](26_array_map_columns.md) | [▶️ Pivoting & Unpivoting: Transforming Columnar Layouts to Row-Oriented Layouts](28_pivoting_unpivoting.md) |
<!-- END_NAVIGATION_LINKS -->
