from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType

spark = (SparkSession.builder
                    .appName("spark_streaming_1")
                    .config("spark.sql.session.timeZone", "UTC")
                    .getOrCreate()
        )

df_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("exchange", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("qty", DoubleType(), True)
])

df_trades = (spark.readStream
    .format("json")
    .schema(df_schema)
    .load("./source_crypto_stream"))

# 2. Агрегация в скользящих окнах
# 2.1. Расчет объема и VWAP: Для каждой биржи рассчитайте показатели в скользящих окнах длительностью 1 минута с шагом 30 секунд:
# total_volume_btc - суммарный объем (сумма qty).
# vwap_usd - средневзвешенная цена VWAP=∑Qty∑(Price×Qty)
# 2.2. Установите водяную метку на 2 минуты по колонке ts

df = (df_trades.withWatermark("ts", "2 minutes")
               .groupBy(F.window(F.col("ts"), "1 minute", "30 seconds"), F.col("exchange"))
               .agg(F.sum(F.col("qty")).alias("total_volume_btc"),
                    (F.sum(F.col("price") * F.col("qty"))/F.sum(F.col("qty"))).alias("vwap_usd"))
               )


# 3.1. Оперативный мониторинг:
# Выведите агрегаты в консоль в режиме update.
query1 = (df.writeStream
            .format("console")
            .outputMode("update")
            .start())

# 3.2. Долгосрочное хранение:
# Запишите результаты в формате Parquet в каталог output_volumes.
query2 = (df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", "./output_volumes")
            .option("checkpointLocation", './checkpoint_parquet')
            .start())


# Используйте режим вывода append.
# Укажите checkpoint_parquet для checkpoint.
# Чтобы запустить два потока одновременно, вызовите .writeStream дважды, сохранив их в переменные (например, query1 и query2),
# и используйте spark.streams.awaitAnyTermination().

spark.streams.awaitAnyTermination()