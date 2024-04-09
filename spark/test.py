from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import random

def main():
    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName("Link Prediction Example") \
        .master("local[*]") \
        .getOrCreate()

    # Set log level to only print errors to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")

    # Generate random user data
    num_users = 100
    users = [(i, "User{}".format(i)) for i in range(num_users)]
    users = [(1,"u1"),(2,"u2"),(3,"u3")]
    users_df = spark.createDataFrame(users, ["id", "name"])

    # Generate random friendships, represented as pairs of user IDs
    num_friendships = 2000
    friendships = [(random.randint(0, num_users - 1), random.randint(0, num_users - 1)) for _ in range(num_friendships)]
    friendships =[(1,2),(2,1),(2,3),(3,2)]
    friendships_df = spark.createDataFrame(friendships, ["src", "dst"])

    # Create a DataFrame representing whether a pair of users are friends (label 1) or not (label 0)
    friendships_df = friendships_df.withColumn("label", col("src") < col("dst")).distinct()

    # Convert boolean labels to numeric (1 for true, 0 for false)
    friendships_df = friendships_df.withColumn("label", when(col("label") == True, 1).otherwise(0))

    # Index the user IDs to convert them to a format suitable for ML algorithms
    # Set handleInvalid to "keep" to handle unseen labels
    indexer = StringIndexer(inputCols=["src", "dst"], outputCols=["src_indexed", "dst_indexed"], handleInvalid="keep")

    # Assemble the features into a single vector
    assembler = VectorAssembler(inputCols=["src_indexed", "dst_indexed"], outputCol="features")

    # Use a simple Logistic Regression model for prediction
    lr = LogisticRegression(featuresCol="features", labelCol="label")

    # Set up the pipeline
    pipeline = Pipeline(stages=[indexer, assembler, lr])

    # Split the data into training and test sets
    train, test = friendships_df.randomSplit([0.8, 0.2], seed=12345)

    # Train the model
    model = pipeline.fit(train)

    # Make predictions on the test data
    predictions = model.transform(test)

    # Show some predictions
    predictions.select("src", "dst", "label", "prediction").show()

    # Evaluate the model using the area under the ROC curve metric
    evaluator = BinaryClassificationEvaluator()
    auc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})

    print(f"Area under ROC curve: {auc}")

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()