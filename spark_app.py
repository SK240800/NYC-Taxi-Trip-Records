from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TaxiDataStreaming") \
    .config("spark.local.dir", "/path/to/local-storage/") \
    .getOrCreate()

# Read from Kafka
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-bootstrap:9092") \
    .option("subscribe", "taxi_data") \
    .load()

# Parse and transform the data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .selectExpr("json_tuple(value, 'tpep_pickup_datetime', 'total_amount') AS pickup_time", 
                "CAST(json_tuple(value, 'total_amount') AS DOUBLE) AS total_amount")

# Aggregate total fares per 5-minute window
aggregated_stream = parsed_stream.groupBy(window(col("pickup_time"), "5 minutes")) \
    .agg(sum("total_amount").alias("total_fare"))

# Write results to local Parquet files
query = aggregated_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/path/to/output-folder/streaming-output/") \
    .option("checkpointLocation", "/path/to/output-folder/checkpoints/") \
    .start()

query.awaitTermination()


