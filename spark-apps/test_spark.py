from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test Spark") \
    .getOrCreate()

# Sprawdź dane w pamięci Sparka
df = spark.sql("SELECT * FROM neo_data")
df.show()