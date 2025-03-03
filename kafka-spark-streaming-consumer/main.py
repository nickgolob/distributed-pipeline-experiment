from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkPostgres") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "kafka:9092"  # Adjust if necessary
kafka_topic = "your_kafka_topic"

# Read from Kafka
raw_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Assume Kafka message is a string; adjust as needed based on your data format
messages_df = raw_stream_df.selectExpr("CAST(value AS STRING)").alias("message")

# Transform the data (example: split the message into words or apply your logic)
transformed_df = messages_df.select(
    expr("split(message, ' ')").alias("words")  # Example transformation
)

# Define PostgreSQL JDBC options
postgres_url = "jdbc:postgresql://postgres:5432/messages_db"
postgres_properties = {
    "user": "admin", 
    "password": "admin", 
    "driver": "org.postgresql.Driver"
}

# Write the streaming data to PostgreSQL
query = transformed_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: 
        batch_df.write.jdbc(url=postgres_url, table="messages", mode="append", properties=postgres_properties)
    ) \
    .outputMode("append") \
    .start()

query.awaitTermination()