from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, ArrayType, LongType

# Konfiguracja Spark
spark = SparkSession.builder \
    .appName("NASA NEO Real-Time Processor") \
    .getOrCreate()

# Poprawiony schemat danych (upewnij się, że pasuje do rzeczywistych danych)
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
        ]), True)
    ]), True),
    StructField("is_potentially_hazardous_asteroid", BooleanType(), True),
    StructField("close_approach_data", ArrayType(StructType([
        StructField("close_approach_date", StringType(), True)
    ])), True)
])

# Odbieranie danych z Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "neo-topic") \
    .option("failOnDataLoss", "false") \
    .load()

# Przetwarzanie danych
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Sprawdź schemat danych (debugowanie)
parsed_df.printSchema()

# Przykład: Filtrowanie potencjalnie niebezpiecznych asteroid
hazardous_asteroids = parsed_df.filter(col("is_potentially_hazardous_asteroid") == True)

# Zapisz wyniki do plików Parquet
hazardous_asteroids.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/tmp/hazardous_asteroids") \
    .option("checkpointLocation", "/tmp/hazardous_asteroids_checkpoints") \
    .start()

# Wyświetl wyniki w konsoli
console_query = hazardous_asteroids.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

spark.streams.awaitAnyTermination()