from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import rand, current_date, date_sub
import time

spark = SparkSession.builder.appName("CacheExample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def measure_time(df):
    start = time.time()
    df.count()
    end = time.time()
    print(f"Затраченное время: {end - start:.2f} сек.")

# Создание датафрейма
df = (
    spark.range(0, 10_000_000)
    .withColumn("date", date_sub(current_date(), (rand() * 365).cast("int")))
    .withColumn("ProductId", (rand() * 100).cast("int"))
)

# Группировка по ProductId
grouped = df.groupBy("ProductId").count()

# print("План выполнения без кэша:")
# grouped.explain()
measure_time(grouped)
print("*"*40)

# Кэширование по умолчанию (MEMORY_AND_DISK)
df.cache()
df.count()  # активация кэша
print(f"StorageLevel по умолчанию: {df.storageLevel}")

print("План выполнения после кэширования:")
grouped = df.groupBy("ProductId").count()
# grouped.explain()
measure_time(grouped)

# Очистка кэша
df.unpersist()

# Установка нового уровня хранения
df.persist(StorageLevel.MEMORY_ONLY)
df.count()  # активация

print(f"StorageLevel после переустановки: {df.storageLevel}")

# Очистка кэша
df.unpersist()

spark.stop()
