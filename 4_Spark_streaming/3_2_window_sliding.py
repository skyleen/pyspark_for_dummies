from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, desc

# Запуск SparkSession
spark = (SparkSession.builder
    .appName("SlidingWindowExample")
    .master("local[*]")
    .getOrCreate())

# Создаем DataFrame генерируя 1 строку в секунду
df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

# Группируем по окну: считаем события за 30 секунд, обновляя каждые 10 секунд
sliding_window_df = (df
    .groupBy(window(col("timestamp"), "30 seconds", "10 seconds"))
    .count()
    .orderBy(desc("window.start")) # Сортируем по убыванию времени начала окна
)

# Запускаем поток
query = (sliding_window_df.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .start())

query.awaitTermination()