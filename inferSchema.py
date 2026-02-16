from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Schema Display").getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/default/test/emp.csv")

# Print schema
df.printSchema()