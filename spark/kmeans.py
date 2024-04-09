from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

    # Initialize a SparkSession
spark = SparkSession.builder \
        .appName("Link Prediction Example") \
        .master("local[*]") \
        .getOrCreate()


data = [(Vectors.dense([0.0, 0.0]), 2.0), (Vectors.dense([1.0, 1.0]), 2.0),
(Vectors.dense([3.0, 0.0]), 2.0), (Vectors.dense([4.0, 1.0]), 2.0),
(Vectors.dense([3.0, 0.0]), 2.0), (Vectors.dense([2.0, 1.0]), 2.0),
        (Vectors.dense([9.0, 8.0]), 2.0), (Vectors.dense([8.0, 9.0]), 2.0)]

import random

for i in range (1000):
        data.append((Vectors.dense([random.random()*10, random.random()*10]), 2.0))

df = spark.createDataFrame(data, ["features", "weighCol"])
kmeans = KMeans(k=2)
kmeans.setSeed(1)

kmeans.setWeightCol("weighCol")

kmeans.setMaxIter(10)

kmeans.getMaxIter()

kmeans.clear(kmeans.maxIter)
kmeans.getSolver()

model = kmeans.fit(df)
model.getMaxBlockSizeInMB()

model.getDistanceMeasure()

model.setPredictionCol("newPrediction")

model.predict(df.head().features)

centers = model.clusterCenters()
len(centers)

transformed = model.transform(df).select("features", "newPrediction")
rows = transformed.collect()
rows[0].newPrediction == rows[1].newPrediction

rows[2].newPrediction == rows[3].newPrediction

model.hasSummary

summary = model.summary
summary.k

summary.clusterSizes

summary.trainingCost

import tempfile
temp_path = tempfile.mkdtemp()
kmeans_path = temp_path + "/kmeans"
kmeans.save(kmeans_path)
kmeans2 = KMeans.load(kmeans_path)
kmeans2.getK()

model_path = temp_path + "/kmeans_model"
model.save(model_path)
model2 = KMeansModel.load(model_path)
model2.hasSummary

model.clusterCenters()[0] == model2.clusterCenters()[0]

model.clusterCenters()[1] == model2.clusterCenters()[1]

model.transform(df).take(1) == model2.transform(df).take(1)

centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
