from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, ArrayType, LongType

# Konfiguracja Spark
spark = SparkSession.builder \
    .appName("NASA NEO Real-Time Processor") \
    .getOrCreate()

# Definicja schematu danych
schema = StructType([
    StructField("links", StructType([
        StructField("self", StringType(), True)
    ]), True),
    StructField("id", StringType(), True),
    StructField("neo_reference_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nasa_jpl_url", StringType(), True),
    StructField("absolute_magnitude_h", DoubleType(), True),
    StructField("estimated_diameter", StructType([
        StructField("kilometers", StructType([
            StructField("estimated_diameter_min", DoubleType(), True),
            StructField("estimated_diameter_max", DoubleType(), True)
        ]), True),
        StructField("meters", StructType([
            StructField("estimated_diameter_min", DoubleType(), True),
            StructField("estimated_diameter_max", DoubleType(), True)
        ]), True),
        StructField("miles", StructType([
            StructField("estimated_diameter_min", DoubleType(), True),
            StructField("estimated_diameter_max", DoubleType(), True)
        ]), True),
        StructField("feet", StructType([
            StructField("estimated_diameter_min", DoubleType(), True),
            StructField("estimated_diameter_max", DoubleType(), True)
        ]), True)
    ]), True),
    StructField("is_potentially_hazardous_asteroid", BooleanType(), True),
    StructField("close_approach_data", ArrayType(StructType([
        StructField("close_approach_date", StringType(), True),
        StructField("close_approach_date_full", StringType(), True),
        StructField("epoch_date_close_approach", LongType(), True),
        StructField("relative_velocity", StructType([
            StructField("kilometers_per_second", StringType(), True),
            StructField("kilometers_per_hour", StringType(), True),
            StructField("miles_per_hour", StringType(), True)
        ]), True),
        StructField("miss_distance", StructType([
            StructField("astronomical", StringType(), True),
            StructField("lunar", StringType(), True),
            StructField("kilometers", StringType(), True),
            StructField("miles", StringType(), True)
        ]), True),
        StructField("orbiting_body", StringType(), True)
    ])), True),
    StructField("is_sentry_object", BooleanType(), True)
])

# Odbieranie danych z Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "neo-topic") \
    .load()

# Przetwarzanie danych
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/tmp/spark_output").option("checkpointLocation", "/tmp/spark_checkpoints") \
    .start()

query.awaitTermination()