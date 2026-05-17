from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col

# Запуск SparkSession
spark = (SparkSession.builder
    .appName("TumblingWindowExample")
    .master("local[*]")
    .getOrCreate())

# Создаем DataFrame генерируя 1 строку в секунду
df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

# Группируем по окну размером 1 минута и считаем количество
tumbling_window_df = (df
    .groupBy(window(col("timestamp"), "1 minutes"))
    .count()
)

# Запускаем поток и выводим в консоль
query = (tumbling_window_df.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .start())

query.awaitTermination()