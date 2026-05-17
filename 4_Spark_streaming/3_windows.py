from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, approx_count_distinct, sum, count

spark = SparkSession.builder.appName("ClusterMonitoring").getOrCreate()

# Чтение потока
raw_data = spark.readStream.format("rate").option("rowsPerSecond", 500).load()

enriched_df = raw_data.select(
    "timestamp",
    (col("value") % 1000).alias("user_id"),
    (col("value") % 100 * 1024).alias("bytes_sent")
)

# Решение задачи:
monitoring_stats = (enriched_df
    .withWatermark("timestamp", "5 minutes")
    .groupBy(window(col("timestamp"), "1 minute"))
    .agg(
        count("*").alias("total_requests"),
        sum("bytes_sent").alias("total_traffic"),
        approx_count_distinct("user_id").alias("unique_users")
    )
)

query = (monitoring_stats.writeStream
    .format("console")
    .outputMode("update")
    .start())

query.awaitTermination()
