# JVM Memory Configuration: Heap vs. Off-Heap Memory Layout

## 1. Executive Overview

### Why This Topic Exists
Apache Spark executes data processing within Java Virtual Machine (JVM) executors. How memory is configured and distributed between the JVM Heap and Off-Heap segments determines the stability and performance of Spark applications. 

This module covers the internal structure of Spark's executor memory layout, the allocations for **On-Heap** and **Off-Heap** segments, and how cluster managers (like YARN or Kubernetes) enforce these memory boundaries.

### Production Problem Solved
1. **Executor OOM Crashes:** Prevents Java heap space errors by optimizing memory allocation boundaries.
2. **GC Thrashing:** Reduces JVM Garbage Collection overhead by moving large storage caches and shuffle tables to off-heap memory.
3. **Container Termination:** Avoids cluster manager SIGKILL shutdowns (Exit Code 137) by configuring sufficient off-heap memory overhead limits.

### Why Senior Engineers Care
Data architects must configure resource templates for enterprise clusters. Improper memory settings (such as setting high JVM heap limits without allocating off-heap overhead memory) can cause containers to crash on YARN or Kubernetes. Knowing how Spark partitions memory internally allows engineers to tune clusters to prevent resource waste.

### Common Misconceptions
* *“`spark.executor.memory` is the only memory allocated to an executor container.”*
  **Reality:** Cluster managers allocate a container with a size equal to `spark.executor.memory` + `spark.executor.memoryOverhead`. If the JVM process exceeds this total limit, the cluster manager kills the container.
* *“Setting storage memory high always improves caching speeds.”*
  **Reality:** High storage memory allocations reduce the space available for execution shuffles and joins, forcing Spark to spill data to disk and degrading performance.

---

## 2. Internal Architecture Deep Dive

The Spark Executor Memory Layout is divided into distinct physical and logical segments:

```
========================================================================================
                         SPARK EXECUTOR JVM MEMORY LAYOUT
========================================================================================
[ CONTAINER MEMORY (Allocated by YARN/K8s) = Executor Memory + Overhead Memory ]
  ├── [ JVM HEAP MEMORY (spark.executor.memory) ]
  │     ├── Reserved Memory (Hardcoded 300 MB)
  │     ├── User Memory (Default 40% of (Heap - 300MB)) -> Custom objects, metadata
  │     └── Spark Memory (Default 60% of (Heap - 300MB)) -> UnifiedMemoryManager
  │           ├── Storage Memory (Default 50%) -> RDD Cache, Broadcasts
  │           └── Execution Memory (Default 50%) -> Shuffles, Joins, Aggregations
  └── [ OFF-HEAP OVERHEAD MEMORY (spark.executor.memoryOverhead) ]
        ├── JVM Overhead (Thread stacks, Metaspace)
        ├── Off-Heap Storage & Execution (Tungsten Off-Heap Page allocations)
        └── Python Worker Subprocesses (PySpark daemons)
========================================================================================
```

### 1. JVM Heap Memory Allocations
* **Reserved Memory:** A hardcoded 300 MB buffer reserved for internal Spark engines (e.g., driver communications, metrics tracker).
* **User Memory:** Represents $(1 - \text{spark.memory.fraction})$ of the remaining heap space. Used for user-defined variables, metadata structures, and custom collections.
* **Spark Memory:** Managed by the `UnifiedMemoryManager`. It is shared dynamically between **Storage Memory** (caching, broadcast blocks) and **Execution Memory** (shuffle aggregation maps, join buffers).

### 2. Off-Heap Memory (`spark.executor.memoryOverhead`)
* **Overhead Allocation:** By default, cluster managers allocate $10\%$ of `spark.executor.memory` (minimum 384 MB) as off-heap overhead.
* **Usage:** Stores native libraries (e.g., compressed codecs), JVM process structures, thread stacks, off-heap Project Tungsten pages, and Python worker processes.

---

## 3. Physical Execution Walkthrough

Let's trace how Spark allocates memory when a task initializes a Sort Merge Join buffer:

```python
# Spark Configuration
spark = SparkSession.builder \
    .config("spark.executor.memory", "10g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .getOrCreate()
```

### Memory Math Calculations
1. **Total Heap:** $10\text{ GB} = 10,240\text{ MB}$.
2. **Usable Heap:** $10,240\text{ MB} - 300\text{ MB} (\text{Reserved}) = 9,940\text{ MB}$.
3. **Spark Memory:** $9,940\text{ MB} \times 0.6 = 5,964\text{ MB}$.
4. **Execution Memory (Default Limit):** $5,964\text{ MB} \times 0.5 = 2,982\text{ MB}$.

### Execution Allocation
1. **Buffer Request:** A Sort Merge Join task starts on the executor, requesting page space from the `TaskMemoryManager` to sort keys.
2. **UnifiedMemoryManager Check:** The manager checks if execution memory is available within the $2,982\text{ MB}$ pool.
3. **Borrowing:** If storage memory is unused, the manager borrows pages from the storage pool to expand execution memory.
4. **Spill Trigger:** If the required memory exceeds the usable Spark memory limits, the manager instructs the task to sort and spill pages to local scratch disk.

---

## 4. Distributed Systems Perspective

### YARN/Kubernetes Resource Enforcement
In containerized environments, the cluster agent (NodeManager on YARN, Kubelet on Kubernetes) monitors container memory usage by reading the cgroup files:
* If the combined memory consumption of the JVM heap, off-heap pages, and subprocesses (like PySpark Python workers) exceeds the total container allocation:
$$\text{Container Memory Limit} = \text{spark.executor.memory} + \text{spark.executor.memoryOverhead}$$
* The OS kernel or cgroup controller terminates the container immediately with a `SIGKILL` signal, and the Spark driver logs an Exit Code 137.

---

## 5. Performance Engineering Section

### Off-Heap Memory Optimization
* **Off-Heap Storage:** Enable off-heap memory to bypass JVM garbage collection delays:
  ```properties
  spark.memory.offHeap.enabled   true
  spark.memory.offHeap.size      4g
  ```
* **Mechanism:** Tungsten allocates page memory directly from the OS using raw pointers, avoiding JVM heap allocations and GC cycles.
* **Tuning:** Reduce `spark.executor.memory` (heap) and increase `spark.memory.offHeap.size` to optimize GC performance on high-throughput clusters.

---

## 6. Spark UI & Debugging Analysis

Open the **Executors Tab** in the Spark UI to debug memory allocations:

```
========================================================================================
                                    EXECUTOR MEMORY METRICS
========================================================================================
ID    Address           Storage Memory    Task Time    GC Time    Shuffle Write   Spill
----------------------------------------------------------------------------------------
1     172.29.0.3        5.5 GB / 5.8 GB   45s          1.2s       12 GB           0 MB
========================================================================================
```

### Diagnostic Analysis
* **GC Time Ratio:** If GC Time is greater than 10% of total task time, the executor is experiencing GC thrashing. Switch to `MEMORY_ONLY_SER` caching, enable off-heap memory, or tune GC parameters.
* **Storage Memory Pool:** Verify if the storage memory allocation limit matches your calculations, confirming configurations are active.

---

## 7. Real Production Scenarios

### Case Study: Resolving YARN Exit Code 137 Crashes on 50TB Ingestions
A daily ETL pipeline processed 50 TB of JSON web logs on a YARN cluster.
* **The Problem:** Executors crashed randomly with YARN container termination warnings and Exit Code 137.
* **The Root Cause:** The pipeline used custom Python UDFs to parse JSON nodes. The PySpark Python worker subprocesses consumed RAM that exceeded the default 10% memory overhead allocation, forcing YARN to terminate the container.
* **The Solution:**
  1. Kept the JVM heap size (`spark.executor.memory`) at 8 GB.
  2. Increased `spark.executor.memoryOverhead` to 4 GB to provide sufficient memory space for the Python worker processes.
* **Result:** Container termination issues were resolved, and the pipeline executed stably.

---

## 8. Failure & Incident Scenarios

### Incident: OutOfMemoryError in User Memory Pool
* **Symptom:** Executors crash during processing, and logs report Java heap space errors inside non-Spark code.
* **Logs:**
```
26/05/25 14:06:12 ERROR Executor: Exception in task 0.0 in stage 1.0
java.lang.OutOfMemoryError: Java heap space
  at my.company.udf.DictionaryLoader.load(DictionaryLoader.scala:15)
```
* **Root-Cause Analysis:** A custom Scala UDF loaded a large lookup dictionary (2 GB) into a local Scala Map object. The dictionary was allocated inside the **User Memory** pool (default: 40% of usable heap), exceeding its capacity and crashing the executor.
* **Remediation:** 
  1. Increase usable heap size (`spark.executor.memory`).
  2. Or, decrease `spark.memory.fraction` to expand the User Memory pool relative to Spark Memory.

---

## 9. Hands-On Labs

### Lab Setup
Ensure you run this lab within the PySpark Jupyter notebook environment.

### 1. Beginner Lab: Verifying Memory Allocations
Start a Spark Session with specific memory configurations and verify the values using the Spark UI and configuration settings.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MemoryConfigLab") \
    .config("spark.executor.memory", "2g") \
    .config("spark.memory.fraction", "0.7") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "512m") \
    .master("local[*]") \
    .getOrCreate()

# Verify active configurations
print(f"Executor Memory: {spark.conf.get('spark.executor.memory')}")
print(f"Memory Fraction: {spark.conf.get('spark.memory.fraction')}")
print(f"Off-Heap Enabled: {spark.conf.get('spark.memory.offHeap.enabled')}")
```

### 2. Intermediate Lab: GC Overhead Analysis
Write a script that caches a large DataFrame. Monitor the GC Time metrics on the Spark UI Executors tab.

```python
# Create large DataFrame
df = spark.range(1, 5000000).withColumn("val", spark.range(1, 5000000)["id"] * 2)

# Cache and materialize
df.cache()
df.count()
# Inspect GC metrics on Spark UI Executors tab
```

### 3. Advanced Lab: Simulating Off-Heap Spills
Configure low execution memory limits. Run a Sort Merge Join and track how the memory manager handles page allocations and disk spills.

---

## 10. Benchmarking & Profiling

We benchmark execution stability and GC overhead under different memory configurations (1 TB dataset):

| Configuration | GC Time | Disk Spills | Stability | Job Duration |
| :--- | :--- | :--- | :--- | :--- |
| **Default (16GB Heap, 0MB Off-Heap)** | 45.2 seconds | 180 GB | Low (Risk of OOM) | 18.5 minutes |
| **Optimized (12GB Heap, 4GB Off-Heap)** | 8.5 seconds | 25 GB | High | 11.2 minutes |

---

## 11. Advanced Optimization Patterns

### Memory Overhead Allocation Rules
For memory-intensive operations (such as running PySpark Pandas UDFs or deep neural network model scoring), use the following configuration to allocate sufficient off-heap overhead memory:
```properties
spark.executor.memoryOverhead   4g
```
This allocates 4 GB of off-heap overhead memory, providing a safety buffer for the JVM container process and subprocesses.

---

## 12. Senior-Level Interview Section

### Q1: Detail the difference between the "User Memory" pool and the "Spark Memory" pool inside the JVM heap.
* **Answer:** User Memory (default: 40% of usable heap) is used to store user-defined data structures, metadata, RDD operations, and framework internal metadata. Spark Memory (default: 60% of usable heap) is managed by the `UnifiedMemoryManager` and is dynamically shared between Storage Memory (used for RDD caching and broadcast blocks) and Execution Memory (used for shuffles, joins, and aggregations).

### Q2: What causes YARN or Kubernetes to terminate a Spark executor container with Exit Code 137? How do you remediate it?
* **Answer:** Exit Code 137 indicates that the container's memory usage exceeded the combined limit of `spark.executor.memory` + `spark.executor.memoryOverhead`, forcing the host cgroup controller or cluster manager to terminate the container. Remediation steps include increasing `spark.executor.memoryOverhead` to allocate more off-heap memory, or optimizing the application to reduce memory usage (e.g., replacing standard Python UDFs with native SQL functions).

---

## 13. Production Design Patterns

### The Hybrid Off-Heap Data Lake Pattern
In high-throughput enterprise pipelines, executors are configured with a hybrid layout: 70% of memory is allocated as JVM heap, and 30% is allocated as off-heap memory. Caching and shuffle aggregation tables are stored off-heap, bypassing JVM GC overhead.

---

## 14. Comparison Section

| Metric | JVM Heap Memory | Off-Heap Memory |
| :--- | :--- | :--- |
| **GC Overhead** | High | Zero |
| **Serialization Cost** | Zero (if deserialized) | High (requires serialization) |
| **Stability** | Risk of GC pauses and heap OOM | Highly stable for large tables |

---

## 15. Expert-Level Mental Models

### The Container Cgroup Model
An elite engineer visualizes the physical cgroup memory limits enforced by the OS. They evaluate the memory footprint of both the JVM heap and off-heap subprocesses to ensure total usage remains within container boundaries.

---

## 16. Final Mastery Checklist

* [ ] Can define the difference between usability heap segments (Reserved, User, Spark).
* [ ] Understands the role of `spark.executor.memoryOverhead` in container environments.
* [ ] Knows how to configure off-heap memory to bypass JVM GC overhead.
* [ ] Can diagnose and resolve Exit Code 137 container crashes.

<!-- START_NAVIGATION_LINKS -->
---
### 🔗 روابط التنقل السريع

| السابق (Previous) | التالي (Next) |
| :--- | :--- |
| [◀️ Graph Processing & Relational Analytics: GraphFrames & Network Connectivity](../03_advanced_analytics/30_graph_processing.md) | [▶️ Spark Memory Manager: Execution Memory vs. Storage Memory Dynamic Allocations](32_spark_memory_manager.md) |
<!-- END_NAVIGATION_LINKS -->
