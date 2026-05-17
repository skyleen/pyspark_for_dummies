from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


spark = SparkSession.builder \
    .appName("PartitionExample") \
    .master("local[8]") \
    .getOrCreate()

# Создание тестового DataFrame
num_records = 10_000_000

df_data = spark.range(0, num_records)  \
    .withColumn("year", (lit(2024) + (col("id") % 2))) \
    .withColumn("month", ((col("id") % 12) + 1)) \
    .withColumn("day", ((col("id") % 28) + 1))

# Пути для сохранения
output_plain = "/Users/n.shikhaleva/Downloads/data/plain"
output_partitioned = "/Users/n.shikhaleva/Downloads/data/partitioned"

# Сохранение без партиционирования
df_data.write.mode("overwrite").parquet(output_plain)

# Сохранение с партиционированием по year и month
df_data.write.mode("overwrite").partitionBy("year", "month").parquet(output_partitioned)

# Чтение из непартиционированной директории с фильтром по дате
df_plain = spark.read.parquet(output_plain) \
    .filter((col("year") == 2024) & (col("month") == 1))
print(f"Plain DF rows count: {df_plain.count()}")

# Чтение из партиционированной директории с тем же фильтром
df_partitioned = spark.read.parquet(output_partitioned) \
    .filter((col("year") == 2024) & (col("month") == 1))
print(f"Partitioned DF rows count: {df_partitioned.count()}")

spark.stop()
