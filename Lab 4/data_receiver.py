from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

import threading
import time

# Biến toàn cục
batch_counter = 0
MAX_BATCHES = 10
query = None
collected_data = None


def update_model(batch_df, batch_id):
    global batch_counter, collected_data

    if batch_df.count() == 0:
        return

    if collected_data is None:
        collected_data = batch_df
    else:
        collected_data = collected_data.union(batch_df)

    assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
    assembled_data = assembler.transform(collected_data).select("features", "y")

    lr = LinearRegression(featuresCol="features", labelCol="y")
    model = lr.fit(assembled_data)

    predictions = model.transform(assembled_data)
    evaluator = RegressionEvaluator(labelCol="y", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    batch_counter += 1
    print(f"[Batch {batch_id}] Hệ số: {model.coefficients} | Intercept: {model.intercept} | RMSE: {rmse} | Batches: {batch_counter}")
    print("---------------------------------------------------")


def monitor_batches():
    global query
    while batch_counter < MAX_BATCHES:
        time.sleep(1)
    print("Huấn luyện đủ 10 batch. Dừng lại.")
    query.stop()


def start_streaming():
    global query

    spark = SparkSession.builder.appName("SparkML Linear Regression Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    raw_stream = spark.readStream.format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    data_stream = raw_stream.select(
        split(col("value"), ",").getItem(0).cast("float").alias("feature1"),
        split(col("value"), ",").getItem(1).cast("float").alias("feature2"),
        split(col("value"), ",").getItem(2).cast("float").alias("y")
    )

    query = data_stream.writeStream \
        .foreachBatch(update_model) \
        .outputMode("update") \
        .start()

    threading.Thread(target=monitor_batches).start()
    query.awaitTermination()

