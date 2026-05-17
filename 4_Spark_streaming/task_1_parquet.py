from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# Прочитайте данные из каталога output_volumes как batch DataFrame,
# Для корректной работы с временными метками, при создании SparkSession укажите: .config("spark.sql.session.timeZone", "UTC")
# Используя Parquet, найдите оконный интервал, начинающийся в 2026-01-19 14:03:30 (UTC), для биржи Cryptojab:

spark = (SparkSession
         .builder
         .config("spark.sql.session.timeZone", "UTC")
         .appName("task_1_parquet")
         .getOrCreate())

df = (spark.read
           .format("parquet")
           .option("path", "./output_volumes")
           .load())
df.printSchema()
(df.filter((F.col("window.start") == '2026-01-19 14:03:30') & (F.col("exchange") == 'Cryptojab'))
    .select("exchange", "window.start", "window.end", "total_volume_btc", "vwap_usd")
    .show())

spark.stop()