# Nested Loop Join (Объединение вложенными циклами):
# Это обобщенный алгоритм объединения, при котором каждая строка из одной таблицы сравнивается с каждой строкой из другой
# по заданному условию, которое может быть не равенством (например, >, <, BETWEEN).
# Если одна из таблиц достаточно мала, Spark может использовать Broadcast Nested Loop Join,
# где меньший DataFrame транслируется на все узлы, а затем происходит локальный вложенный цикл.
# Это гораздо эффективнее, чем полный Nested Loop Join без broadcast, который потребует перемещения данных.
# Используется для неэквивалентных условий Join.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat

spark = (SparkSession.builder
    .appName("BroadcastNestedLoopJoin")
    .getOrCreate())

df_large = spark.range(0, 1_000_000).withColumnRenamed("id", "large_id")
df_small = spark.range(0, 100).withColumnRenamed("id", "small_id")

print("BroadcastNestedLoopJoin с неэквивалентным условием:")
df_nested_loop_join = df_large.join(df_small, col("large_id") > col("small_id"))
df_nested_loop_join.explain()

spark.stop()