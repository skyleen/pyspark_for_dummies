from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, MapType, TimestampType

spark = (SparkSession.builder
                    .appName("spark_streaming_final")
                    .config("spark.sql.session.timeZone", "UTC")
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.default.parallelism", "3")
                    # Ограничение количества потоков в Streaming
                    .config("spark.streaming.concurrentJobs", "1")
                    .config("spark.streaming.blockInterval", "5s")
                    .master("local[2]")
                    .getOrCreate()
        )

df_schema = StructType([
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", LongType(), True),
    StructField("title", StringType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("minor", BooleanType(), True),
    StructField("timestamp", LongType(), True),
    StructField("length", StructType([
                                            StructField("old", LongType()),
                                            StructField("new", LongType())
                                            ]), True),
    StructField("comment", StringType(), True),
    StructField("server_url", StringType(), True),
    StructField("wiki", StringType(), True)
])

df_stream = (spark.readStream
    .format("json")
    .schema(df_schema)
    .load("./source_wiki_stream/"))

df_enriched = (df_stream
                  .withColumn("correction_length", F.abs(F.ifnull(F.col("length.new") - F.col("length.old"), F.lit(0))))
                  .withColumn("event_timestamp", F.from_unixtime(F.col("timestamp")).cast("timestamp"))
                  )

df_watermark = df_enriched.withWatermark("event_timestamp", "10 seconds")

df_total = (df_watermark
                .groupBy(F.window("event_timestamp", "1 minute", "30 seconds"))
                .agg(F.count(F.col("*")).alias("total_corrections"),
                     F.round(F.avg(F.col("correction_length")), 2).alias("avg_correction_length"))
                .withColumns({"window_start": F.col("window.start"),
                               "window_end":  F.col("window.end")})
            )
# 1. Пользователи с количеством правок более 5
# Рассчитайте пользователей, которые совершили более 5 правок за скользящее окно длительностью 1 минута с шагом 30 секунд.
# Установите водяную метку на 2 минуты.
# Для каждого окна дополнительно вычислите:
# общее количество правок за окно
# среднее изменение размера текста (length.new - length.old)
# Выводите агрегаты в удобном формате (консоль, DataFrame, лог).
df_users = (df_watermark
             .groupBy(F.window("event_timestamp", "1 minute", "30 seconds"), "user")
             .agg(F.count("*").alias("user_corrections"),
                  F.round(F.avg(F.col("correction_length")), 2).alias("avg_correction_length_by_user"))
             .withColumns({"window_start": F.col("window.start"),
                           "window_end":  F.col("window.end")})
             .filter(F.col("user_corrections") > 5)
          )
df_users_final = (df_users.join(df_total, ["window_start", "window_end"])
                          .select("window_start",
                                  "window_end",
                                  "user",
                                  "user_corrections",
                                  "avg_correction_length_by_user",
                                  "total_corrections",
                                  "avg_correction_length"))
# 2. Страницы с количеством правок выше порога
# Рассчитайте страницы (title), которые получили более 3 правок за то же скользящее окно.
# Используйте ту же водяную метку 2 минуты.
# Выводите также средний размер изменения статьи (length.new - length.old) за окно.
df_titles = (df_watermark
             .groupBy(F.window("event_timestamp", "1 minute", "30 seconds"), "title")
             .agg(F.count("*").alias("title_corrections"),
                 F.avg(F.col("correction_length")).alias("avg_correction_length_by_title"))
             .withColumns({"window_start": F.col("window.start"),
                           "window_end":  F.col("window.end")})
             .filter(F.col("title_corrections") > 3)
          )
df_titles_final = (df_titles.join(df_total, ["window_start", "window_end"])
                            .select(
                                   "window_start",
                                   "window_end",
                                    "title",
                                    "title_corrections",
                                    "avg_correction_length_by_title",
                                    "total_corrections",
                                    "avg_correction_length"))
# Требования к реализации
# Режимы вывода (outputMode) и способ отображения агрегатов можно выбирать на усмотрение, главное - корректность вычислений и скользящие окна.
# Использовать checkpoint для обеспечения устойчивости стрима.
def streaming_to_console(df, query_name, checkpoint_path):
    return (df
                .writeStream
                .outputMode("append")
                .format("console")
                # .trigger(processingTime="100 seconds")
                .queryName(f"{query_name}_console")
                .option("checkpointLocation", checkpoint_path)
                .option("truncate", False)
                .start()
            )

user_queries = streaming_to_console(df_users_final, "Active users >5 corrections", "./checkpoint_final_users")
title_queries = streaming_to_console(df_titles_final, "Popular titles >3 corrections", "./checkpoint_final_titles")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nKeyboardInterrupt received. Stopping...")
    for query in spark.streams.active:
        query.stop()
    spark.stop()