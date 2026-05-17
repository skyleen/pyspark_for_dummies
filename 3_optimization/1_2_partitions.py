from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .master("local[*]")\
        .appName('Example')\
        .getOrCreate()

# Создаем DataFrame
df = spark.range(0, 50_000_000)

# Сохраняем с дефолтным числом партиций
df.write.mode("overwrite").parquet("/Users/n.shikhaleva/Downloads/output_default")

print(f"Number of partitions:{df.rdd.getNumPartitions()}")

# Repartition — увеличиваем до 64
df_repart = df.repartition(64)
df_repart.write.mode("overwrite").parquet("/Users/n.shikhaleva/Downloads/output_repartition_64")

print(f"Number of partitions after repartition:{df_repart.rdd.getNumPartitions()}")

# Coalesce — уменьшаем до 8
df_coalesce = df.coalesce(8)
df_coalesce.write.mode("overwrite").parquet("/Users/n.shikhaleva/Downloads/output_coalesce_8")

print(f"Number of partitions after coalesce:{df_coalesce.rdd.getNumPartitions()}")

# На практике, оптимальное число партиций часто соответствует тому, чтобы каждая партиция содержала 128-256 МБ данных,
# что позволяет эффективно использовать распределенную файловую систему и минимизировать накладные расходы.

spark.stop()
