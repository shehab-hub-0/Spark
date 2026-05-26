import sys
from pyspark.sql import SparkSession

def print_banner(title):
    print("\n" + "=" * 60)
    print(f" {title.upper()} ".center(60, "#"))
    print("=" * 60)

try:
    print_banner("1. Pre-initializing S3 Buckets in MinIO")
    import io
    from minio import Minio
    minio_client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    for b in ["spark-logs", "warehouse"]:
        if not minio_client.bucket_exists(b):
            minio_client.make_bucket(b)
            print(f"✅ Created missing MinIO bucket: '{b}'")
        else:
            print(f"ℹ️ MinIO bucket '{b}' already exists.")

    # Spark requires the event-logs directory structure to be present inside spark-logs
    try:
        minio_client.put_object("spark-logs", "event-logs/", io.BytesIO(b""), 0)
        print("✅ Created event-logs directory prefix in spark-logs bucket.")
    except Exception as e:
        print("ℹ️ Note on event-logs folder creation:", e)

    print_banner("2. Initializing Spark Session")
    spark = SparkSession.builder \
        .appName("ComprehensiveSparkTest") \
        .getOrCreate()
    
    print("Spark Session created successfully.")
    print("Spark Version:", spark.version)
    print("Master URL:", spark.sparkContext.master)
    print("Loaded Configurations:")
    for k, v in sorted(spark.sparkContext.getConf().getAll()):
        if "secret" not in k and "password" not in k:
            print(f"  {k}: {v}")
            
except Exception as e:
    print("❌ Failed to initialize Spark Session:", e)
    sys.exit(1)

# ── PHASE 2: Core RDD Map-Reduce ──────────────────────────────────
try:
    print_banner("2. Testing Core RDD Operations")
    data = list(range(1, 10001))
    rdd = spark.sparkContext.parallelize(data, numSlices=4)
    rdd_sum = rdd.map(lambda x: x * 2).reduce(lambda a, b: a + b)
    expected = sum(x * 2 for x in data)
    if rdd_sum == expected:
        print("✅ RDD Map-Reduce Test Passed. Result Sum =", rdd_sum)
    else:
        print(f"❌ RDD Map-Reduce Test Failed. Expected {expected}, got {rdd_sum}")
except Exception as e:
    print("❌ RDD Test Failed with exception:", e)

# ── PHASE 3: DataFrame Basics ─────────────────────────────────────
try:
    print_banner("3. Testing DataFrame Transformations")
    df_data = [("Alice", 25, "HR"), ("Bob", 30, "IT"), ("Charlie", 35, "IT"), ("David", 40, "HR")]
    columns = ["Name", "Age", "Department"]
    df = spark.createDataFrame(df_data, schema=columns)
    
    print("Original DataFrame:")
    df.show()
    
    print("Filtered & Aggregated Department Count:")
    agg_df = df.filter(df.Age > 28).groupBy("Department").count()
    agg_df.show()
    print("✅ DataFrame Basics Test Passed.")
except Exception as e:
    print("❌ DataFrame Basics Test Failed with exception:", e)

# ── PHASE 4: MinIO S3A Direct Read/Write ──────────────────────────
try:
    print_banner("4. Testing S3A (MinIO) Read/Write")
    s3_path = "s3a://warehouse/direct_test_parquet"
    print(f"Writing test DataFrame to {s3_path}...")
    
    test_data = [(i, f"value_{i}") for i in range(1, 1001)]
    test_df = spark.createDataFrame(test_data, ["id", "val"])
    
    test_df.write.mode("overwrite").parquet(s3_path)
    print("Parquet data written successfully. Now reading back...")
    
    read_df = spark.read.parquet(s3_path)
    count = read_df.count()
    
    if count == 1000:
        print(f"✅ S3A Read/Write Test Passed. Successfully read 1000 rows back.")
    else:
        print(f"❌ S3A Read/Write Test Failed. Count is {count} (expected 1000).")
except Exception as e:
    print("❌ S3A (MinIO) Test Failed with exception:", e)

# ── PHASE 5: Iceberg + Nessie Integration ─────────────────────────
try:
    print_banner("5. Testing Iceberg & Nessie Catalog Integration")
    
    # Create an Iceberg database and table via Nessie catalog
    print("Creating table nessie.db.test_iceberg...")
    spark.sql("CREATE DATABASE IF NOT EXISTS nessie.db")
    spark.sql("""
        CREATE OR REPLACE TABLE nessie.db.test_iceberg (
            id bigint,
            data string,
            category string
        ) USING iceberg
        PARTITIONED BY (category)
    """)
    
    # Insert some data
    print("Inserting data into Iceberg table...")
    spark.sql("""
        INSERT INTO nessie.db.test_iceberg VALUES 
        (1, 'sensor_data_1', 'iot'),
        (2, 'sensor_data_2', 'iot'),
        (3, 'user_action_1', 'web')
    """)
    
    # Query data
    print("Querying Iceberg table...")
    res_df = spark.sql("SELECT * FROM nessie.db.test_iceberg WHERE category = 'iot'")
    res_df.show()
    
    cnt = res_df.count()
    if cnt == 2:
        print("✅ Iceberg + Nessie Test Passed. Successfully read 2 iot rows.")
    else:
        print(f"❌ Iceberg + Nessie Test Failed. Got {cnt} rows, expected 2.")
        
except Exception as e:
    print("❌ Iceberg & Nessie Test Failed with exception:", e)

# ── PHASE 6: JDBC PostgreSQL Connection ───────────────────────────
try:
    print_banner("6. Testing PostgreSQL JDBC Connection")
    
    # Connection details matching local postgres container environment
    jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
    jdbc_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    
    test_db_df = spark.createDataFrame([(1, "Spark JDBC Test"), (2, "Success Connection")], ["id", "msg"])
    
    print(f"Writing data to postgres table 'spark_jdbc_test'...")
    test_db_df.write.jdbc(url=jdbc_url, table="spark_jdbc_test", mode="overwrite", properties=jdbc_properties)
    
    print("Reading data back via JDBC...")
    read_db_df = spark.read.jdbc(url=jdbc_url, table="spark_jdbc_test", properties=jdbc_properties)
    read_db_df.show()
    
    if read_db_df.count() == 2:
        print("✅ PostgreSQL JDBC Test Passed.")
    else:
        print("❌ PostgreSQL JDBC Test Failed. Row count mismatch.")
        
except Exception as e:
    print("❌ PostgreSQL JDBC Test Failed with exception:", e)

# ── PHASE 7: Spark Stop ───────────────────────────────────────────
print_banner("7. Stopping Spark Session")
spark.stop()
print("Test suite completed successfully!")
