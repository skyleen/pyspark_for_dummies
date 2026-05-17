from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ProjectionPushdown").getOrCreate()

# Создаем DataFrame с несколькими столбцами и записываем в Parquet
df_many_cols = spark.range(0, 10_000) \
                    .withColumn("col_a", col("id") * 10) \
                    .withColumn("col_b", col("id") * 0.5) \
                    .withColumn("col_c", (col("id") % 5)) \
                    .withColumn("col_d", col("id") / 3)

parquet_path_proj = "projection_pushdown"
df_many_cols.write.mode("overwrite").parquet(parquet_path_proj)

# Читаем данные, выбирая только несколько столбцов
df_selected_cols = spark.read.parquet(parquet_path_proj).select("col_a", "col_c")
df_selected_cols.explain()

spark.stop()
