
from pyspark.sql import functions as F


TOPIC = "restaurant_orders"

TARGET_TABLE = (
    "workspace.doordash_streaming.bronze_orders"
)

CHECKPOINT_PATH = (
    "/Volumes/workspace/doordash_streaming/"
    "kafka_checkpoints/bronze_orders"
)


def run_bronze(spark, dbutils, bootstrap_servers):

    # 1. Retrieve secrets
    api_key = dbutils.secrets.get(
        "kafka-secrets", "kafka-api-key"
    )

    api_secret = dbutils.secrets.get(
        "kafka-secrets", "kafka-api-secret"
    )

    # 2. Configure Kafka authentication
    jaas_config = (
        "kafkashaded.org.apache.kafka.common."
        "security.plain.PlainLoginModule required "
        f'username="{api_key}" '
        f'password="{api_secret}";'
    )

    # 3. Read Kafka events
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            bootstrap_servers
        )
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .load()
    )

    # 4. Preserve raw Kafka data
    bronze_df = kafka_df.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("message_value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingestion_time")
    )

    # 5. Write to Bronze Delta
    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )

    query.awaitTermination()

    print("Bronze ingestion completed.")
