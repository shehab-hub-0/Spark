"""
tests/test_spark_basics.py — Example PySpark unit tests.
Run with: pytest tests/ -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for tests — created once, reused across all tests."""
    session = (
        SparkSession.builder
        .appName("PySpark-Tests")
        .master("local[2]")          # Only 2 cores needed for unit tests
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")  # Disable UI for test speed
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_spark_session_is_running(spark):
    assert spark is not None
    assert spark.version == "3.5.0"


def test_create_dataframe(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "label"])
    assert df.count() == 2
    assert len(df.columns) == 2


def test_filter_and_count(spark):
    df = spark.range(10)  # 0..9
    evens = df.filter(F.col("id") % 2 == 0)
    assert evens.count() == 5


def test_group_by_agg(spark):
    data = [("eng", 100), ("eng", 200), ("hr", 150)]
    df = spark.createDataFrame(data, ["dept", "salary"])
    result = df.groupBy("dept").agg(F.sum("salary").alias("total"))
    totals = {row["dept"]: row["total"] for row in result.collect()}
    assert totals["eng"] == 300
    assert totals["hr"] == 150
