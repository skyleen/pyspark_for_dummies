from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import session_window
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType

spark = (SparkSession.builder
                    .appName("spark_streaming_4")
                    .config("spark.sql.session.timeZone", "UTC")
                    .getOrCreate()
        )

df_stream = (spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load())

df = (df_stream
      .withColumns({"ts": F.element_at(F.split(F.col("value"),','), 1).cast("timestamp"),
                    "user_id": F.element_at(F.split(F.col("value"),','), 2).cast("string"),
                    "action": F.element_at(F.split(F.col("value"),','), 3).cast("string")}
                    )
      .withWatermark("ts", "30 minutes")
      )

df_metrics = (df.groupBy(F.session_window("ts", "30 minutes"), "user_id")
            .agg(F.count(F.col("action")).alias("actions_count"),
                 F.collect_list("action").alias("actions_list"))
            .select("user_id", "session_window.start", "session_window.end", "actions_list", "actions_count"))

query1 = (df_metrics
          .writeStream.format("console")
          .outputMode("append")
          .trigger(processingTime="10 seconds", )
          .option("checkpointLocation", "./checkpoint_task3")
          .start()
          )

spark.streams.awaitAnyTermination()