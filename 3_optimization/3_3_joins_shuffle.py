# Shuffle Hash Join:
# Эта стратегия применяется, когда одна из таблиц слишком велика для Broadcast Join,
# но достаточно мала, чтобы поместиться в память исполнителя после перераспределения данных по ключу объединения.
# Обе таблицы перераспределяются по ключу JOIN, а затем меньшая сторона используется для построения хеш-таблицы,
# а большая — для поиска совпадений.
# Сортировки не применяется (в отличие от Sort Merge Join), но всё ещё требует Shuffle.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat

spark = (SparkSession
         .builder
         .appName("ShuffleHashJoin")
         .config("spark.sql.join.preferSortMergeJoin", False) # Отключаем Sort-Merge Join
         .getOrCreate())

# Большой DataFrame
df_transactions = (spark.range(0, 10_000_000)
    .withColumn("user_id", col("id") % 1_000_000)
    .withColumn("amount", col("id") * 0.1)
    .drop("id"))

# Средний DF
df_users_medium = (spark.range(0, 600_000)
    .withColumnRenamed("id", "user_id")
    .withColumn("username", concat(lit("user_"), col("user_id"))))

# Маленький DataFrame
df_users_small = (spark.range(0, 300_000)
    .withColumnRenamed("id", "user_id")
    .withColumn("username", concat(lit("user_"), col("user_id"))))

# Принудительный Shuffle Hash Join --> spark.sql.join.preferSortMergeJoin = false
df_joined = df_transactions.join(df_users_medium, df_transactions.user_id == df_users_medium.user_id, "inner")
df_joined.explain()

spark.stop()