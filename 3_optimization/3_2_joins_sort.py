# Sort Merge Join:
# Эти универсальная стратегия для объединения больших таблиц, когда Broadcast невозможен.
# Обе таблицы подвергаются shuffle, а затем сортируются внутри каждой партиции.
# После этого отсортированные данные соединяются. Этот метод эффективен для очень больших наборов данных,
# но требует дорогостоящих операций Shuffle и sort.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat

spark = (SparkSession
         .builder
         .appName("SortMergeJoin")
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

# Автоматический Sort Merge Join
df_joined = df_transactions.join(df_users_medium, df_transactions.user_id == df_users_medium.user_id, "inner")
df_joined.explain()

spark.stop()