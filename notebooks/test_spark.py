from pyspark.sql import SparkSession

# Connect to the Spark master inside the Docker network
spark = SparkSession.builder \
    .appName("TestCluster") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

data = list(range(1, 1000000))
rdd = spark.sparkContext.parallelize(data)

result = rdd.map(lambda x: x * 2).reduce(lambda a, b: a + b)
print("---------------------------------")
print("Spark Execution Result:", result)
print("---------------------------------")

spark.stop()
