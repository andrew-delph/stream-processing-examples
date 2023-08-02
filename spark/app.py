from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.sql.types import StringType

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "my-topic"
MONGO_USER = "root"
MONGO_PASSWORD = "example"
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@mongo:27017/"
MONGO_DATABASE = "mydatabase"
MONGO_COLLECTION = "word_count"


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

def save_to_mongo(batchDF, epoch_id):
    # Transform and write batchDF to MongoDB here
    (batchDF.write
     .format("mongo")
     .mode("append")
     .option("uri", MONGO_URI)
     .option("database", MONGO_DATABASE)
     .option("collection", MONGO_COLLECTION)
     .save())

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaStreamConsumer") \
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

