from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import numpy as np
import threading
import time


coefficients = np.array([0.0, 0.0])
learning_rate = 0.01
batch_counter = 0
MAX_BATCHES = 10
query = None 


def update_model(batch_df, batch_id):
    global coefficients, batch_counter

    if batch_df.count() == 0:
        return

    pd_df = batch_df.toPandas()

    X = pd_df[['feature1', 'feature2']].values.astype(float)
    y = pd_df['y'].values.astype(float).reshape(-1, 1)

    predictions = np.dot(X, coefficients)
    error = predictions.reshape(-1, 1) - y
    gradient = np.mean(X * error, axis=0)
    coefficients -= learning_rate * gradient

    batch_counter += 1
    print(f"Batch {batch_id} - Hệ số cập nhật: {coefficients} - Batch count: {batch_counter}")


def monitor_batches():
    global query
    while batch_counter < MAX_BATCHES:
        time.sleep(1)
    print("Đã đủ 10 batch, dừng query.")
    query.stop()


def start_streaming():
    global query

    spark = SparkSession.builder \
        .appName("Streaming Model Training") \
        .getOrCreate()

    raw_stream = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    processed_stream = raw_stream.select(
        split(col("value"), ",").getItem(0).cast("float").alias("feature1"),
        split(col("value"), ",").getItem(1).cast("float").alias("feature2"),
        split(col("value"), ",").getItem(2).cast("float").alias("y")
    )

    query = processed_stream.writeStream \
        .foreachBatch(update_model) \
        .outputMode("update") \
        .start()

    monitor_thread = threading.Thread(target=monitor_batches)
    monitor_thread.start()

    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()
