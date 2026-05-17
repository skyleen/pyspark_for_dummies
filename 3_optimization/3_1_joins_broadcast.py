# Broadcast Hash Join:
# Это наиболее эффективный тип объединения, когда один из DataFrame достаточно мал,
# чтобы целиком поместиться в оперативную память всех исполнителей.
# Меньший DataFrame транслируется (копируется) на все исполнители,
# а затем большой DataFrame объединяется с транслированным локально.
# Это исключает Shuffle, что значительно ускоряет процесс.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, broadcast

spark = (SparkSession
         .builder
         .appName("BroadcastHashJoin")
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

# Автоматический Broadcast Hash Join
df_joined = df_transactions.join(df_users_small, df_transactions.user_id == df_users_small.user_id, "inner")
df_joined.explain()

# Принудительный broadcast()
df_joined = df_transactions.join(broadcast(df_users_medium), df_transactions.user_id == df_users_medium.user_id, "inner")
df_joined.explain()

spark.stop()