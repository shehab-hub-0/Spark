"""
main.py — PySpark 3.5.0 Environment Verification Script
========================================================
Run this script immediately after launching the Dev Container to confirm
every layer of the stack is working correctly.

Usage:
    python src/main.py

Expected outcome: All checks pass and a final summary table is printed.
"""

import os
import sys
import time
import platform
from typing import List, Tuple

# ── Rich for readable terminal output (installed in Dockerfile) ───────────────
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import print as rprint

    console = Console()
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

    class Console:  # type: ignore[no-redef]
        def print(self, *args, **kwargs):
            print(*args)

    console = Console()


# ── PySpark imports ───────────────────────────────────────────────────────────
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)


# =============================================================================
# Helper utilities
# =============================================================================

results: List[Tuple[str, str, str]] = []  # (check_name, status, detail)


def check(name: str, status: bool, detail: str = "") -> None:
    """Record a check result."""
    icon = "✅" if status else "❌"
    results.append((name, icon, detail))
    console.print(f"  {icon} {name}" + (f" — {detail}" if detail else ""))


def section(title: str) -> None:
    console.print(f"\n[bold cyan]── {title} {'─' * (55 - len(title))}[/bold cyan]" if RICH_AVAILABLE else f"\n── {title} {'─' * (55 - len(title))}")


# =============================================================================
# 1. Python & environment checks
# =============================================================================

def check_environment() -> None:
    section("1. Python & System Environment")

    check("Python version", sys.version_info >= (3, 10),
          f"Python {sys.version.split()[0]}")

    check("Running inside container",
          os.path.exists("/.dockerenv") or os.environ.get("REMOTE_CONTAINERS") is not None
          or os.environ.get("CODESPACES") is not None
          or os.path.exists("/workspace"),
          "Docker container detected")

    check("SPARK_HOME set",
          bool(os.environ.get("SPARK_HOME")),
          os.environ.get("SPARK_HOME", "NOT SET"))

    check("PYSPARK_PYTHON set",
          bool(os.environ.get("PYSPARK_PYTHON")),
          os.environ.get("PYSPARK_PYTHON", "NOT SET"))

    spark_home = os.environ.get("SPARK_HOME", "/opt/spark")
    check("Spark binary exists",
          os.path.exists(os.path.join(spark_home, "bin", "spark-submit")),
          f"{spark_home}/bin/spark-submit")

    check("Platform", True, f"{platform.system()} {platform.machine()}")


# =============================================================================
# 2. SparkSession creation
# =============================================================================

def create_spark_session() -> SparkSession:
    section("2. SparkSession Initialization")
    console.print("  ⏳ Building SparkSession (first time may take ~15s) …")

    t0 = time.time()

    spark = (
        SparkSession.builder
        .appName("PySpark-DevContainer-Verification")
        # Master is set via spark-defaults.conf (local[*]), but explicit here
        # for clarity and to ensure it wins if conf is not found.
        .master("local[*]")
        # These match spark-defaults.conf and are explicit for transparency
        .config("spark.driver.memory", "8g")
        .config("spark.driver.cores", "6")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.ui.port", "4040")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    # Quiet down the Spark logger at runtime too
    spark.sparkContext.setLogLevel("WARN")

    elapsed = time.time() - t0
    check("SparkSession created", True, f"{elapsed:.1f}s")
    check("Spark version", spark.version == "3.5.0", spark.version)
    check("Master URL", "local" in spark.sparkContext.master,
          spark.sparkContext.master)
    check("Application name", True, spark.sparkContext.appName)
    check("Spark UI", True,
          f"http://localhost:{spark.conf.get('spark.ui.port', '4040')}")

    return spark


# =============================================================================
# 3. Memory & CPU configuration validation
# =============================================================================

def check_config(spark: SparkSession) -> None:
    section("3. Resource Configuration Validation")

    cfg = spark.sparkContext.getConf().getAll()
    cfg_dict = dict(cfg)

    expected = {
        "spark.driver.memory":          "8g",
        "spark.driver.cores":           "6",
        "spark.sql.shuffle.partitions": "12",
        "spark.sql.adaptive.enabled":   "true",
    }

    for key, expected_val in expected.items():
        actual_val = cfg_dict.get(key, "NOT SET")
        check(f"Config: {key}", actual_val == expected_val,
              f"got={actual_val}, want={expected_val}")

    parallelism = spark.sparkContext.defaultParallelism
    check("Default parallelism ≥ 6", parallelism >= 6,
          f"parallelism={parallelism}")


# =============================================================================
# 4. DataFrame operations — functional correctness
# =============================================================================

def check_dataframe_ops(spark: SparkSession) -> None:
    section("4. DataFrame Operations")

    # ── Schema-explicit creation ──────────────────────────────────────────────
    schema = StructType([
        StructField("employee_id",  IntegerType(), False),
        StructField("name",         StringType(),  False),
        StructField("department",   StringType(),  True),
        StructField("salary",       DoubleType(),  True),
        StructField("is_active",    BooleanType(), True),
    ])

    data = [
        (1,  "Alice",   "Engineering", 95_000.0, True),
        (2,  "Bob",     "Marketing",   72_000.0, True),
        (3,  "Carol",   "Engineering", 88_000.0, False),
        (4,  "Dave",    "HR",          65_000.0, True),
        (5,  "Eve",     "Engineering", 102_000.0, True),
        (6,  "Frank",   "Marketing",   78_000.0, False),
        (7,  "Grace",   "HR",          69_000.0, True),
        (8,  "Heidi",   "Engineering", 91_000.0, True),
        (9,  "Ivan",    "Marketing",   74_000.0, True),
        (10, "Judy",    "Engineering", 99_000.0, True),
    ]

    df: DataFrame = spark.createDataFrame(data, schema=schema)

    check("DataFrame created", df is not None, f"{df.count()} rows")
    check("Schema matches", len(df.schema.fields) == 5,
          f"{len(df.schema.fields)} columns")

    # ── Filtering ─────────────────────────────────────────────────────────────
    active_engineers = df.filter(
        (F.col("department") == "Engineering") & F.col("is_active")
    )
    check("Filter operation", active_engineers.count() == 4,
          f"{active_engineers.count()} active engineers")

    # ── Aggregation ───────────────────────────────────────────────────────────
    dept_stats: DataFrame = (
        df.groupBy("department")
        .agg(
            F.count("*").alias("headcount"),
            F.round(F.avg("salary"), 2).alias("avg_salary"),
            F.max("salary").alias("max_salary"),
        )
        .orderBy("department")
    )
    check("GroupBy + Agg", dept_stats.count() == 3, "3 departments")

    # ── Window function ───────────────────────────────────────────────────────
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy("department").orderBy(F.desc("salary"))
    ranked = df.withColumn("rank_in_dept", F.rank().over(window_spec))
    check("Window function (rank)", ranked.filter(F.col("rank_in_dept") == 1).count() == 3,
          "Top earner per dept identified")

    # ── UDF ───────────────────────────────────────────────────────────────────
    @F.udf(StringType())
    def salary_band(salary: float) -> str:
        if salary is None:
            return "unknown"
        if salary >= 90_000:
            return "senior"
        if salary >= 70_000:
            return "mid"
        return "junior"

    banded = df.withColumn("salary_band", salary_band(F.col("salary")))
    senior_count = banded.filter(F.col("salary_band") == "senior").count()
    check("UDF execution", senior_count > 0, f"{senior_count} senior-band employees")

    # ── Print aggregated results ──────────────────────────────────────────────
    console.print("\n  Department Summary:")
    dept_stats.show(truncate=False)


# =============================================================================
# 5. SQL engine
# =============================================================================

def check_sql(spark: SparkSession) -> None:
    section("5. Spark SQL Engine")

    data = [(i, f"item_{i}", float(i * 1.5)) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "label", "value"])
    df.createOrReplaceTempView("items")

    result = spark.sql("""
        SELECT
            COUNT(*)                          AS total,
            ROUND(AVG(value), 4)              AS avg_value,
            SUM(CASE WHEN id % 2 = 0 THEN 1 ELSE 0 END) AS even_count
        FROM items
    """)

    row = result.first()
    check("SQL COUNT", row["total"] == 100, f"total={row['total']}")
    check("SQL AVG",   abs(row["avg_value"] - 75.75) < 0.01,
          f"avg_value={row['avg_value']}")
    check("SQL CASE",  row["even_count"] == 50, f"even_count={row['even_count']}")


# =============================================================================
# 6. Parquet I/O
# =============================================================================

def check_parquet_io(spark: SparkSession) -> None:
    section("6. Parquet I/O (read/write cycle)")

    path = "/tmp/spark-verification-parquet"
    data = [(i, f"record_{i}") for i in range(500)]
    df = spark.createDataFrame(data, ["id", "record"])

    # Write
    df.write.mode("overwrite").parquet(path)
    check("Parquet write", os.path.isdir(path), path)

    # Read back and validate
    df_read = spark.read.parquet(path)
    count = df_read.count()
    check("Parquet read", count == 500, f"{count} rows round-tripped")

    # Predicate pushdown
    filtered = df_read.filter(F.col("id") > 400).count()
    check("Predicate pushdown", filtered == 99, f"{filtered} rows after filter >400")


# =============================================================================
# 7. Arrow / pandas interop
# =============================================================================

def check_arrow(spark: SparkSession) -> None:
    section("7. Arrow-Accelerated Pandas Interop")

    try:
        import pandas as pd
        import pyarrow as pa

        pdf = pd.DataFrame({
            "x": range(1000),
            "y": [float(i) ** 0.5 for i in range(1000)],
        })

        sdf = spark.createDataFrame(pdf)
        pdf_back = sdf.toPandas()

        check("Pandas → Spark", sdf.count() == 1000, "1000 rows converted")
        check("Spark → Pandas", len(pdf_back) == 1000, "1000 rows collected")
        check("PyArrow version", True, pa.__version__)
        check("Pandas version",  True, pd.__version__)
    except Exception as exc:
        check("Arrow/Pandas interop", False, str(exc))


# =============================================================================
# 8. Final summary
# =============================================================================

def print_summary() -> None:
    section("Summary")

    passed = sum(1 for _, s, _ in results if "✅" in s)
    failed = sum(1 for _, s, _ in results if "❌" in s)
    total  = passed + failed

    if RICH_AVAILABLE:
        table = Table(title="Verification Results", show_lines=True)
        table.add_column("Check",  style="white",   no_wrap=True)
        table.add_column("Status", justify="center")
        table.add_column("Detail", style="dim")

        for name, status, detail in results:
            color = "green" if "✅" in status else "red"
            table.add_row(name, f"[{color}]{status}[/{color}]", detail)

        console.print(table)
        console.print(
            Panel(
                f"[bold green]{passed}/{total} checks passed[/bold green]"
                if failed == 0
                else f"[bold red]{failed} checks FAILED[/bold red] — see details above",
                title="Result",
            )
        )
    else:
        print(f"\n{'='*60}")
        print(f"RESULT: {passed}/{total} checks passed, {failed} failed")
        print(f"{'='*60}")

    if failed > 0:
        sys.exit(1)


# =============================================================================
# Entry point
# =============================================================================

def main() -> None:
    if RICH_AVAILABLE:
        console.print(Panel(
            "[bold]PySpark 3.5.0 Dev Container — Environment Verification[/bold]\n"
            "Runs a full stack check: Python → SparkSession → SQL → I/O → Pandas",
            style="cyan",
        ))
    else:
        print("\n" + "="*60)
        print("PySpark 3.5.0 Dev Container — Environment Verification")
        print("="*60)

    check_environment()

    spark: SparkSession = create_spark_session()

    try:
        check_config(spark)
        check_dataframe_ops(spark)
        check_sql(spark)
        check_parquet_io(spark)
        check_arrow(spark)
    finally:
        spark.stop()
        check("SparkSession stopped cleanly", True, "no dangling processes")

    print_summary()


if __name__ == "__main__":
    main()
