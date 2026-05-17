from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Инициализация SparkSession
spark = (SparkSession.builder
    .appName("KafkaSparkProcessing")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# Схема для входящих данных
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("value", DoubleType(), True)
])

# Чтение данных из Kafka
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "user_events")
    .option("startingOffsets", "latest")
    .load())

# Парсинг JSON данных
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 1. Подсчет общего количества событий и суммы значений для каждого пользователя
user_totals = parsed_df.groupBy("user_id").agg(
    count("*").alias("total_events"),
    round(sum("value"), 2).alias("total_value")
)

# Подготовка для записи в Kafka
user_totals_for_kafka = (user_totals
    .select(col("user_id").cast(StringType()).alias("key"),
            to_json(struct(col("user_id"), col("total_events"), col("total_value"))).alias("value")))

# 2. Подсчет событий по типам
event_type_counts = parsed_df.groupBy("event_type").agg(
    count("*").alias("count")
)

event_type_counts_for_kafka = (event_type_counts
    .select(col("event_type").cast(StringType()).alias("key"),
            to_json(struct(col("*"))).alias("value")))

# 3. Популярные категории
popular_categories = parsed_df.groupBy("category").agg(
    count("*").alias("event_count"),
    round(avg("value"), 2).alias("avg_value")
).filter(col("event_count") > 5)

popular_categories_for_kafka = (popular_categories
    .select(col("category").cast(StringType()).alias("key"),
            to_json(struct(col("*"))).alias("value")))

# Функция для запуска запросов с триггером и режимом update
def start_query(query, name, checkpoint_location):
    return (query.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", name)
        .option("checkpointLocation", checkpoint_location)
        .outputMode("update")  # Режим обновления
        .trigger(processingTime="10 seconds")  # Фиксированный интервал триггера
        .start())

# Запуск всех потоков обработки
queries = []

# Запрос 1: Общие данные по пользователям
query1 = start_query(
    user_totals_for_kafka,
    "user_totals",
    "./checkpoint_user_totals"
)
queries.append(query1)

# Запрос 2: Количество событий по типам
query2 = start_query(
    event_type_counts_for_kafka,
    "event_type_counts",
    "./checkpoint_event_type_counts"
)
queries.append(query2)

# Запрос 3: Популярные категории
query3 = start_query(
    popular_categories_for_kafka,
    "popular_categories",
    "./checkpoint_popular_categories"
)
queries.append(query3)

# Ожидание завершения всех запросов
for query in queries:
    query.awaitTermination()


# 1_docker exec broker kafka-topics --bootstrap-server broker:29092 --create --topic user_events --partitions 1 --replication-factor 1
# 1_docker exec broker kafka-topics --bootstrap-server broker:29092 --create --topic user_totals --partitions 1 --replication-factor 1
# 1_docker exec broker kafka-topics --bootstrap-server broker:29092 --create --topic event_type_counts --partitions 1 --replication-factor 1
# 1_docker exec broker kafka-topics --bootstrap-server broker:29092 --create --topic popular_categories --partitions 1 --replication-factor 1


# 1_docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092 --topic user_totals --from-beginning
#
# # Для просмотра распределения событий по типам
# 1_docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092 --topic event_type_counts --from-beginning
#
# # Для просмотра популярных категорий
# 1_docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092 --topic popular_categories --from-beginning