from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, col, date_sub, current_date
import time

spark = SparkSession.builder.appName("CheckpointCacheExample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir("./checkpoints")

def measure_time(df, label):
    import time
    start = time.time()
    df.count()
    end = time.time()
    print(f"{label} — время выполнения: {end - start:.2f} сек.")

# Создание набора транзакций
df = (
    spark.range(0, 10_000_000)
    .withColumn("user_id", (rand() * 10000).cast("int"))
    .withColumn("product_id", (rand() * 1000).cast("int"))
    .withColumn("amount", (rand() * 100).cast("decimal(10,2)"))
    .withColumn("date", date_sub(current_date(), (rand() * 365).cast("int")))
)
# Фильтрация
filtered = df.filter(col("amount") > 80)

# Без кэширования:
print("Без кэширования:")
measure_time(filtered.groupBy("user_id").count(), "Группировка по user_id")
filtered.groupBy("user_id").count().explain()
measure_time(filtered.groupBy("product_id").count(), "Группировка по product_id")
filtered.groupBy("product_id").count().explain()

print("*"*50)

# С cache
df_cached = filtered.cache()
df_cached.count()  # триггер кэширования

print("С использованием cache:")
measure_time(df_cached.groupBy("user_id").count(), "Группировка по user_id")
df_cached.groupBy("user_id").count().explain()
measure_time(df_cached.groupBy("product_id").count(), "Группировка по product_id")
df_cached.groupBy("product_id").count().explain()

df_cached.unpersist()
print("*"*50)

# С checkpoint
df_cp = filtered.checkpoint()

print("С использованием checkpoint:")
measure_time(df_cp.groupBy("user_id").count(), "Группировка по user_id")
df_cp.groupBy("user_id").count().explain()
measure_time(df_cp.groupBy("product_id").count(), "Группировка по product_id")
df_cp.groupBy("product_id").count().explain()

spark.stop()