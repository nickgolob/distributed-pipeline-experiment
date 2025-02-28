from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import psycopg2
import json
import os

# Environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydatabase")

# Spark session
spark = SparkSession.builder \
    .appName("KafkaPostgresConsumer") \
    .getOrCreate()

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "test-topic") \
    .load()

# Define the schema for the Kafka value
schema = StructType([
    StructField("id", IntegerType()),
    StructField("value", StringType())
])

# Parse the Kafka value into a DataFrame
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(col("value").cast("string")) \
    .selectExpr("CAST(value AS STRING) AS json_value") \
    .selectExpr("json_value") \
    .withColumn("json", spark.sql.functions.from_json("json_value", schema)) \
    .select("json.*")

# Function to write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    # Convert the DataFrame to Pandas to insert into PostgreSQL
    data = batch_df.toPandas()

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST
    )
    cursor = conn.cursor()

    # Insert into PostgreSQL
    for _, row in data.iterrows():
        cursor.execute("INSERT INTO messages (data) VALUES (%s)", [json.dumps(row.to_dict())])

    conn.commit()
    cursor.close()
    conn.close()

# Write the stream to PostgreSQL
query = kafka_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .start()

# Await termination of the stream
query.awaitTermination()
