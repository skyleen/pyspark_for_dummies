from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("PartitionPruningAnalysis").getOrCreate()

# Генерируем данные для партиционирования по year и month
data = []
for year in range(2022, 2025):
    for month in range(1, 13):
        for day in range(1, 5):
            data.append((year, month, day, f"event_{year}-{month}-{day}", 100 + day))

df_events = spark.createDataFrame(data, ["year", "month", "day", "event_id", "value"])

output_path = "partitioned_events_explain"
df_events.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)

# Читаем данные с фильтром по партиционированным колонкам
df_filtered = spark.read.parquet(output_path).filter((col("year") == 2023) & (col("month") == 6))
df_filtered.explain()

spark.stop()
