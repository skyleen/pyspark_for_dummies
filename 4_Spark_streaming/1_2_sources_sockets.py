from pyspark.sql import SparkSession
from pyspark.sql.functions import length

# Создаем SparkSession
spark = SparkSession.builder.appName("SocketSourceExample").getOrCreate()

# Определяем потоковый источник
# Spark будет слушать данные с localhost на порту 9999
lines = (spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load())

# Применяем преобразование
# У нас одна колонка value, в которой будет строка.
# Мы посчитаем длину каждой строки и добавим ее в новую колонку word_count
word_count = lines.withColumn("char_count", length("value"))


# Запускаем потоковую задачу и выводим результат
query = (word_count.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "/Users/n.shikhaleva/Documents/PythonProject/files/datastream/socket_checkpoint")
    .start())

# Ожидаем завершения задачи по сигналу (Ctrl+C)
query.awaitTermination()