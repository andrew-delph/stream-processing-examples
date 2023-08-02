# consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "my-topic"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaStreamConsumer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    kafkaStreamDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    messagesDF = kafkaStreamDF \
        .selectExpr("CAST(value AS STRING)") \
        .withColumn("value", col("value").cast(StringType()))

    query = messagesDF \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()
