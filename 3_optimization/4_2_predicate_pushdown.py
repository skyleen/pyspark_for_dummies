from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("PredicatePushdownAnalysis").getOrCreate()

# Создаем DataFrame и записываем его в Parquet
df_large_data = spark.range(0, 10_000_000) \
                     .withColumn("value", col("id") * 10) \
                     .withColumn("category", (col("id") % 10).cast("string"))
parquet_path = "data_for_pushdown"
df_large_data.write.mode("overwrite").parquet(parquet_path)

# Читаем данные с фильтром по колонке value
df_filtered_read = spark.read.parquet(parquet_path).filter(col("value") > 50_000_000)
df_filtered_read.explain()

spark.stop()
