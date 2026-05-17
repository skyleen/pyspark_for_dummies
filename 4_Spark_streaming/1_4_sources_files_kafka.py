import requests # Добавляем импорт requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Запуск SparkSession и подключение пакета Kafka
spark = (SparkSession.builder
    .appName("KafkaStructuredStreamingWithJoin")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# Схема для парсинга данных из JSON, получаемых из Kafka (посты)
kafka_json_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True)
])

# Схема для парсинга данных пользователей из статического API
# {"id": 1, "name": "...", "username": "...", "email": "..."}
user_static_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("username", StringType(), True),
    StructField("email", StringType(), True)
])

# Определение статического источника (профили пользователей)
# Теперь читаем данные о пользователях напрямую из API
try:
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    users_data_list = response.json()
    static_user_df = spark.createDataFrame(users_data_list, schema=user_static_schema)
except Exception as e:
    print(f"Ошибка при загрузке данных пользователей из API: {e}")
    # Создаем пустой DataFrame, чтобы избежать ошибки, если API недоступен
    static_user_df = spark.createDataFrame([], user_static_schema)

# Чтение данных из Kafka
df_kafka_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test_data")
    .load())

# Парсинг бинарной колонки 'value' в JSON
parsed_stream = (df_kafka_stream.selectExpr("CAST(value AS STRING) AS json_string")
    .select(from_json(col("json_string"), kafka_json_schema).alias("data"))
    .select("data.*"))

# Выполнение stream-static join
# Соединяем потоковые данные из Kafka по userId с статическими данными пользователей по id
enriched_stream = parsed_stream.join(static_user_df, parsed_stream["userId"] == static_user_df["id"], "left_outer")


# Запуск потока и вывод в консоль
query = (enriched_stream.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "./kafka_join_checkpoint")
    .start())

query.awaitTermination()