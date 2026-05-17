from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Запуск SparkSession и подключение пакета Kafka
spark = (SparkSession.builder
    .appName("KafkaStructuredStreaming")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

#Схема для парсинга данных из JSON
json_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True)
])

# Чтение данных из Kafka
df_kafka_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test_data")
    .option("maxOffsetsPerTrigger", 10000) # Лимит сообщений на батч
    .load())

# Парсинг бинарной колонки 'value' в JSON
parsed_stream = (df_kafka_stream.selectExpr("CAST(value AS STRING) AS json_string")
    .select(from_json(col("json_string"), json_schema).alias("data"))
    .select("data.*"))

# Запуск потока и вывод в консоль
query = (parsed_stream.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "/Users/n.shikhaleva/Documents/PythonProject/files/datastream/kafka_checkpoint")
    .start())

query.awaitTermination()