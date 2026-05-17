import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, when

spark = (SparkSession.builder
         .appName("Mongo_Streaming")
         .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.0.1")
         .getOrCreate())

# rate генерирует 2 строки в секунду с колонками: timestamp и value
rate_stream = (spark.readStream
               .format("rate")
               .option("rowsPerSecond", 2)
               .load())

# Добавляем случайный ID пользователя (от 0 до 100) и тип действия (клик или покупка)
live_events_stream = rate_stream \
    .withColumn("user_id", (rand() * 100).cast("int")) \
    .withColumn("action", when(rand() > 0.8, "purchase").otherwise("click"))

# Запускаем запись потока в Mongo
print("Запускаем поток. Данные начали поступать в MongoDB...")
streaming_query = (live_events_stream.writeStream
                   .format("mongodb")
                   .option("connection.uri", "mongodb://spark_user:spark_password@localhost:27017/?authSource=admin")
                   .option("database", "shop_db")
                   .option("collection", "live_stream")
                   .option("checkpointLocation", "/tmp/mongo_checkpoints")

                   .outputMode("append")
                   .start())

# ЗАпускаем поток на 15 секунд
time.sleep(15)

# Останавливаем поток
streaming_query.stop()
print("Поток остановлен.")

# Проверяем результат: читаем накопленные данные в обычном пакетном режиме
print("Результат в MongoDB (последние события):")
(spark.read
 .format("mongodb")
 .option("connection.uri", "mongodb://spark_user:spark_password@localhost:27017/?authSource=admin")
 .option("database", "shop_db")
 .option("collection", "live_stream")
 .load()
 .orderBy("timestamp", ascending=False)
 .show(truncate=False))

spark.stop()
