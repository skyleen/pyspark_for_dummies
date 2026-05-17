from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType

spark = (SparkSession.builder
                    .appName("spark_streaming_3")
                    .config("spark.sql.session.timeZone", "UTC")
                    .getOrCreate()
        )

# 2.1. Парсинг и очистка
# Разделите входящую строку на поля: ts (Timestamp), host (String), cpu (Integer), mem (Integer). Для этого можно воспользоваться функцией F.element_at.
# Watermark: Установите водяную метку на 1 минуту по колонке ts.
# Deduplication: Удалите повторяющиеся записи для одного и того же сервера в один и тот же момент времени (ts, host).

df_stream = (spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load())

df = (df_stream
      .withColumns({"ts": F.element_at(F.split(F.col("value"),','), 1).cast("timestamp"),
                    "host": F.element_at(F.split(F.col("value"),','), 2).cast("string"),
                    "cpu": F.element_at(F.split(F.col("value"),','), 3).cast("string"),
                    "mem": F.element_at(F.split(F.col("value"),','), 4).cast("string")}
                    )
      .withWatermark("ts", "1 minute")
      .dropDuplicatesWithinWatermark(["ts", "host"])
      )

query1 = (df.groupBy(F.window(F.col("ts"), "1 minute"))
         .agg(F.avg("cpu").alias("avg_cpu"),
              F.approx_count_distinct("host").alias("total_hosts"))
          .select("window", "avg_cpu", "total_hosts")
          .writeStream
          .format("console")
          .outputMode("append")
          .option("truncate", "false")
          .start())

spark.streams.awaitAnyTermination()