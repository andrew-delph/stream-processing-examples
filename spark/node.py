from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array  # Ensure 'array' is imported here
from pyspark.ml.feature import Word2Vec
import random

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Graph Embeddings") \
    .getOrCreate()

# Generate random edges
edges = []
def even():
    return int(random.random()*100*2)

for i in range (1000):
    edges.append((even(),even()))
    edges.append((even()-1,even()-1))

# Create a DataFrame from the list of edges
edges_df = spark.createDataFrame(edges, ["src", "dst"])

# Convert integer nodes to strings and create an array column
edges_df = edges_df.withColumn("text", array(col("src").cast("string"), col("dst").cast("string")))

# Use Word2Vec to generate embeddings
word2Vec = Word2Vec(vectorSize=5, minCount=0, inputCol="text", outputCol="result")
model = word2Vec.fit(edges_df)


# Transform model to get embeddings
result = model.transform(edges_df)

result.show()

from pyspark.sql.functions import desc  # Import the desc function

result_sorted = result.orderBy(desc("result"))

# Show the sorted results
result_sorted.show()

# Stop the Spark session
spark.stop()