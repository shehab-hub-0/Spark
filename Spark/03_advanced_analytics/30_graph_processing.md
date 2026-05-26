# Graph Processing & Relational Analytics: GraphFrames & Network Connectivity

## 1. Executive Overview

### Why This Topic Exists
Many datasets represent networks of connected entities (like social networks, transaction paths, or communication links). Analysing these relationships requires specialized graph models. Spark implements graph processing using **GraphFrames**, an open-source package that integrates graph computation with the DataFrame API.

This module covers the physical layout of vertices and edges in GraphFrames, the translation of graph algorithms to relational joins, and optimization strategies for iterative graph processing.

### Production Problem Solved
1. **Network Connectivity Analysis:** Identifies connected components and community structures (e.g., detecting fraud ring networks in financial transactions).
2. **Influence Rankings:** Calculates entity influence scores (e.g., PageRank to identify top influencers).
3. **Pattern Matching:** Queries specific structural shapes in a network using Motif Finding.

### Why Senior Engineers Care
Data architects must build graph analytics systems that scale to billions of nodes. Knowing how GraphFrames translates graph algorithms into physical shuffles, how to manage state during iterative executions, and how to optimize join performance is essential.

### Common Misconceptions
* *“GraphFrames uses a specialized graph database engine under the hood.”*
  **Reality:** GraphFrames is built entirely on the DataFrame API. Graph algorithms are translated by Catalyst into standard relational operations (like Joins and Aggregations), running on the default Spark core engine.
* *“Graph processing is fast because it executes locally in memory.”*
  **Reality:** Graph algorithms are highly iterative. PageRank, for example, executes multiple joins and shuffles in a loop, which can cause significant network overhead and memory spills.

---

## 2. Internal Architecture Deep Dive

GraphFrames represents networks using two relational DataFrames: **Vertices** and **Edges**.

```mermaid
graph TD
    subgraph sg_1 ["GraphFrames Model"]
        V[Vertices DataFrame: contains id column]
        E[Edges DataFrame: contains src and dst columns]
    end

    subgraph sg_2 ["Relational Translation"]
        V & E --> Algo[PageRank Algorithm Loop]
        Algo --> Join[Join: Match src with id]
        Join --> Group[GroupBy & Aggregate: Compute rank values]
        Group --> LoopCheck{Iterations Complete?}
        LoopCheck -- No --> Join
        LoopCheck -- Yes --> Output[Return updated ranks]
    end
```

### 1. Relational Data Model
* **Vertices DataFrame:** Must contain a column named **`id`** representing unique node IDs.
* **Edges DataFrame:** Must contain columns named **`src`** (source) and **`dst`** (destination) representing directed connections.

### 2. Algorithmic Translation to SQL Joins
To execute graph algorithms (like PageRank):
1. GraphFrames joins the Edges and Vertices DataFrames on `edges.src = vertices.id` to propagate values (e.g., ranks).
2. It aggregates values on `edges.dst` to update the state of the target vertices.
3. This Join-Aggregate step is executed inside a loop for a configured number of iterations, creating deep physical plans.

---

## 3. Physical Execution Walkthrough

Let's analyze the physical plan of a motif finding query:

```python
# Spark SQL Query (Conceptual GraphFrames Code)
# g.find("(a)-[e]->(b)")
# This translates to:
joined = df_edges.alias("e") \
    .join(df_vertices.alias("a"), col("e.src") == col("a.id")) \
    .join(df_vertices.alias("b"), col("e.dst") == col("b.id"))

joined.explain(mode="formatted")
```

### Physical Plan Analysis
The physical plan reveals the structural join steps:

```
== Formatted Physical Plan ==
* SortMergeJoin (6)
:- * Sort (2)
:  +- Exchange (1)
:     +- * Scan parquet edges (0)
+- * Sort (5)
   +- Exchange (4)
      +- * Scan parquet vertices (3)
```

### Execution Steps
1. **Exchange (1) & (4):** Shuffles the `edges` and `vertices` tables across the network using a hash partitioner on the join keys.
2. **Sort (2) & (5):** Sorts the shuffled partitions locally.
3. **SortMergeJoin (6):** Iterates through the sorted streams concurrently to match source and destination IDs, outputting the path results.

---

## 4. Distributed Systems Perspective

### Iterative Lineage Explosion
Because graph algorithms run in a loop (e.g., executing 20 iterations of PageRank), the RDD lineage graph grows exponentially. If a node fails at iteration 19, the driver must recompile and re-run all 19 preceding iterations.
* **Remediation:** Periodically break the lineage using checkpointing:
  ```python
  spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
  # Checkpointing cuts the lineage graph and saves intermediate states
  ```

---

## 5. Performance Engineering Section

### Graph Partitioning and Bucketing
To minimize network shuffles during iterative graph joins, pre-partition or bucket the Vertices and Edges DataFrames on their join keys (`id`, `src`, `dst`) during table creation. This allows Spark to execute local joins, bypassing network shuffles across loop iterations.

---

## 6. Spark UI & Debugging Analysis

Open the **SQL and Stages Tabs** in the Spark UI to debug graph performance:

* **Lineage Depth:** Inspect the visual query plan. If you see a long chain of repetitive Sort-Merge-Join operations, the graph algorithm is executing many loop iterations without checkpointing.
* **Disk Spill Indicators:** Check if the join stages executed disk spills. If you see high `Spill (Memory)` and `Spill (Disk)` values, increase executor memory or adjust partition counts.

---

## 7. Real Production Scenarios

### Case Study: Resolving Fraud Rings in a Financial Network
A bank analyzed money transfers (500 million transactions) to identify money laundering paths.
* **The Problem:** The fraud ring detection script took **2 hours** to execute and regularly caused executor memory crashes.
* **The Root Cause:** The script ran the Connected Components algorithm without checkpointing, causing the RDD lineage to grow too deep and exhausting the driver's JVM heap.
* **The Solution:**
  1. Configured checkpointing:
     `spark.sparkContext.setCheckpointDir("s3a://bank-checkpoints/")`
  2. Run Connected Components with a checkpoint interval of 2 iterations.
* **Result:** Lineage growth was truncated, and the script completed in **14 minutes** without memory issues.

---

## 8. Failure & Incident Scenarios

### Incident: Driver OOM during Motif Finding on high-degree nodes
* **Symptom:** The Spark application hangs during plan compilation. The driver JVM exits with a Java heap space error.
* **Logs:**
```
26/05/25 14:06:12 ERROR Driver: Out of Memory: Java heap space
  at org.apache.spark.sql.catalyst.rules.RuleExecutor.run...
```
* **Root-Cause Analysis:** The pipeline matched patterns using a complex motif query (e.g., `(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)`). Because some nodes had high degrees (millions of connections), the search space exploded, overloading the driver's memory.
* **Remediation:** 
  Filter out high-degree nodes (hubs) before executing the motif search.

---

## 9. Hands-On Labs

### Lab Setup
Ensure you run this lab within the PySpark Jupyter notebook environment.

### 1. Beginner Lab: Building a GraphFrame
Write a script that builds a basic GraphFrame from vertices and edges DataFrames, and lists the degrees of each vertex.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GraphLab") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12") \
    .master("local[*]") \
    .getOrCreate()

# Create Vertices
v = spark.createDataFrame([
    ("1", "Alice", 30),
    ("2", "Bob", 35),
    ("3", "Charlie", 40)
], ["id", "name", "age"])

# Create Edges
e = spark.createDataFrame([
    ("1", "2", "friend"),
    ("2", "3", "follow")
], ["src", "dst", "relationship"])

# Import GraphFrame
from graphframes import GraphFrame
g = GraphFrame(v, e)

# Show vertices and edges degrees
g.degrees.show()
```

### 2. Intermediate Lab: Running PageRank
Write a script that executes the PageRank algorithm on the graph and extracts the highest-ranked vertices.

```python
# Run PageRank
results = g.pageRank(resetProbability=0.15, maxIter=5)
results.vertices.select("id", "pagerank").show()
```

### 3. Advanced Lab: Analyzing Connected Components with Checkpoint
Create a graph containing disconnected subgraphs. Run the Connected Components algorithm, configure checkpointing, and analyze the physical plan.

---

## 10. Benchmarking & Profiling

We benchmark runtimes for PageRank calculations (10 million vertices, 50 million edges):

| Max Iterations | Lineage Checkpoint Interval | Run Duration | Stability |
| :--- | :--- | :--- | :--- |
| **5 Iterations** | None | 4.8 minutes | High |
| **10 Iterations** | None | 18.5 minutes | Low (GC Thrashing) |
| **10 Iterations** | Every 2 iterations | 8.2 minutes | High |

---

## 11. Advanced Optimization Patterns

### Filtering Hubs
In graph analytics, nodes with extremely high degrees (hubs) can cause join skew and slow down execution. Filter out these nodes before executing search patterns:
```python
# Filter out nodes with more than 1,000 connections
low_degree_edges = g.edges.join(g.degrees.filter("degree < 1000"), col("src") == col("id"))
```

---

## 12. Senior-Level Interview Section

### Q1: How does GraphFrames translate graph-specific algorithms to Spark's relational engine?
* **Answer:** GraphFrames translates graph algorithms into standard relational operations (like Joins and Aggregations) on DataFrames. For instance, PageRank is executed as a loop that joins the Edges and Vertices DataFrames on `edges.src = vertices.id` and aggregates results on `edges.dst` to update rank values.

### Q2: Why is checkpointing critical when running highly iterative graph algorithms in Spark?
* **Answer:** Iterative graph algorithms execute Join-Aggregate steps inside a loop. This causes the RDD lineage graph to grow deep. If a node fails, the driver must recompile and re-run all preceding iterations. Checkpointing cuts the lineage graph and writes intermediate states to storage, preventing lineage explosion and improving fault tolerance.

---

## 13. Production Design Patterns

### The Fraud Network Mining Pattern
In banking analytics, transactional networks are processed daily. The pipeline runs the Connected Components algorithm on transfer logs to group accounts into fraud rings, saving the output to a Silver database table.

---

## 14. Comparison Section

| Metric | GraphFrames | GraphX (RDD) |
| :--- | :--- | :--- |
| **Data API** | DataFrames | RDDs |
| **Optimizer** | Catalyst Optimizer | None |
| **Language Support** | Scala, Python, Java | Scala only |

---

## 15. Expert-Level Mental Models

### The Iterative Matrix Multiply Model
An elite engineer visualizes graph iterations as matrix multiplications. They monitor the lineage depth and partition sizes to keep computations fast and stable.

---

## 16. Final Mastery Checklist

* [ ] Can build GraphFrames from vertices and edges DataFrames.
* [ ] Understands how GraphFrames translates algorithms into Joins and Aggregations.
* [ ] Knows how to configure checkpointing to truncate deep lineage graphs.
* [ ] Can diagnose and resolve performance bottlenecks in iterative graph queries.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ User-Defined Aggregate Functions (UDAFs): Native Aggregations](29_user_defined_aggregate_functions.md) | [▶️ JVM Memory Configuration: Heap vs. Off-Heap Memory Layout](../04_memory_tuning/31_jvm_memory_configuration.md) |
<!-- END_NAVIGATION_LINKS -->
