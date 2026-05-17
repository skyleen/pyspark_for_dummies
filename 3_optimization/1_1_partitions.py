from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .master("local[*]")\
        .appName('Example')\
        .getOrCreate()

df = spark.range(0, 1000)
# df.write.mode('overwrite').parquet("/Users/n.shikhaleva/Downloads/example_parts")

print(f"Number of partitions:{df.rdd.getNumPartitions()}")

# repartition()
# Нужно увеличить количество партиций для повышения параллелизма.
# Данные неравномерно распределены и требуется сбалансировать нагрузку.
# Перед тяжёлыми операциями: join, groupBy, sort для увеличения параллелизма.
# Оптимальным количеством партиций обычно считается 2-4 задачи на каждое ядро, чтобы добиться хорошей балансировки нагрузки.

df = df.repartition(24)
print(f"Number of partitions after repartition:{df.rdd.getNumPartitions()}")

# coalesce()
# используется для уменьшения количества партиций в DataFrame или RDD.
# Он выполняется как узкое преобразование, не перемешивая данные между узлами, и просто объединяет существующие партиции.
# Это делает его эффективным, если данные уже равномерно распределены.
# Нужно снизить число партиций, например, перед сохранением в файл.
# Данные уже равномерно распределены, и важна производительность.
# Хотим избежать shuffle.

df = df.coalesce(1)
df.write.mode('overwrite').csv("/Users/n.shikhaleva/Downloads/df_final")
print(f"Number of partitions after coalesce:{df.rdd.getNumPartitions()}")

spark.stop()