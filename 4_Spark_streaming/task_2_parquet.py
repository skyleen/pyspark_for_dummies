from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# Прочитайте данные из каталога output_volumes как batch DataFrame,
# Для корректной работы с временными метками, при создании SparkSession укажите: .config("spark.sql.session.timeZone", "UTC")
# Используя Parquet, найдите оконный интервал, начинающийся в 2026-01-19 14:03:30 (UTC), для биржи Cryptojab:

spark = (SparkSession
         .builder
         .config("spark.sql.session.timeZone", "UTC")
         .appName("task_2_parquet")
         .getOrCreate())

df = (spark.read
           .format("parquet")
           .option("path", "./output_bikeshare")
           .load())
df.printSchema()

(df.filter((F.col("window.start") >= '2026-01-19 08:04:00') & (F.col("window.start") <= '2026-01-19 08:06:00'))
    .select("window.start", "window.end", "zone", "alert_level", "low_stock_stations")
    .orderBy("window.start", "low_stock_stations")
    .show())

spark.stop()
