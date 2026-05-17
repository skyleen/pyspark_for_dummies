from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType

spark = (SparkSession.builder
                    .appName("spark_streaming_5")
                    .config("spark.sql.session.timeZone", "UTC")
                    .getOrCreate()
        )

# Архивация: Сохраните весь DataFrame батча в формате Parquet в папку output_bank_archive. Режим записи: append.
#
# Алерты: Отфильтруйте транзакции, где сумма (amount) превышает 5000. Сохраните результат в формате JSON в папку output_bank_alerts.
#
# Дашборд: Сгруппируйте данные по категории (category), просуммируйте amount. Отсортируйте по убыванию суммы и выведите в консоль (.show()) топ-3 категории текущего батча. Вывод в консоль должен быть реализован с помощью .show() внутри функции process_batch

def process_batch(df, batch_id):
    # Архивация
    print(f"\nProcessed batch: {batch_id}")
    df.write.format("parquet").mode("append").save("output_bank_archive")
    # Алерты
    df_alerts = df.filter(F.col("amount") > 5000)
    df_alerts.write.format("json").mode("append").save("output_bank_alerts")
    print(f"Saved alerts: {df_alerts.count()}")
    # Дашборд
    print(f"Top 3 categories:")
    (df.groupBy("category")
     .agg(F.sum(F.col("amount")).alias("total_amount"))
     .orderBy("total_amount", ascending=False)
     .show(3))

df_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("card_type", StringType(), True)
])

df_stream = (spark.readStream
    .format("json")
    .schema(df_schema)
    .load("./source_bank_transactions"))

query1 = (df_stream
          .writeStream
          .foreachBatch(process_batch)
          .trigger(processingTime="5 seconds")
          .option("maxFilesPerTrigger", 1)
          .option("checkPointLocation", "./checkpoint_bank")
          .start())

spark.streams.awaitAnyTermination()