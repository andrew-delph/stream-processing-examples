from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.sql.types import StringType
from pymongo import MongoClient

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "my-topic"
MONGO_USER = "root"
MONGO_PASSWORD = "example"
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@mongo:27017/"
MONGO_DATABASE = "mydatabase"
MONGO_COLLECTION = "word_count"

# def save_to_mongo(batchDF, epoch_id):
#     # Transform and write batchDF to MongoDB here
#     (batchDF.write
#      .format("mongo")
#      .mode("append")
#      .option("uri", MONGO_URI)
#      .option("database", MONGO_DATABASE)
#      .option("collection", MONGO_COLLECTION)
#      .save())
import socket

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def save_to_mongo(batchDF, epoch_id):

    host_name = socket.gethostname()
    logger.info(f"Processing on host: {host_name}")

    # Create a connection to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    # Iterate through the DataFrame rows and update MongoDB
    for row in batchDF.collect():
        word = row['word']
        # word = str(host_name)
        count = row['count']

        # Increment the count for the word, or insert a new document if the word doesn't exist
        collection.update_one({'word': word}, {'$inc': {'count': count}}, upsert=True)

    client.close()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaStreamConsumer") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    kafkaStreamDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    messagesDF = kafkaStreamDF.selectExpr("CAST(value AS STRING)")

    wordsDF = messagesDF.select(explode(split(messagesDF.value, " ")).alias("word"))

    wordCountDF = wordsDF.groupBy("word").count()

    query = wordCountDF \
        .writeStream \
        .foreachBatch(save_to_mongo) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

