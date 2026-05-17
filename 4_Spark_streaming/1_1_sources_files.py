from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType

# Создаем SparkSession
spark = SparkSession.builder.appName("FileSourceExample").getOrCreate()

# Определяем схему данных для логов
log_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("url", StringType(), True)
])

# Определяем потоковый источник
# Spark будет отслеживать новые файлы в папке logs
df_stream = (spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(log_schema)
    .load("/Users/n.shikhaleva/Documents/PythonProject/files/datastream/logs"))

# Применяем преобразование: отфильтруем только главную страницу
filtered_stream = df_stream.filter("url = '/index.html'")

# Запускаем потоковую задачу и выводим результат
query = (filtered_stream.writeStream
    .format("console")
    .option("checkpointLocation", "/Users/n.shikhaleva/Documents/PythonProject/files/datastream/checkpoint_dir")
    .outputMode("append")
    .start())

# Ожидаем завершения задачи по сигналу (Ctrl+C)
query.awaitTermination()