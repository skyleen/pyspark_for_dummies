from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Инициализация SparkSession
spark = (SparkSession.builder
    .appName("KafkaSessionWindows")
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

# Парсинг JSON-данных и преобразование временной метки в тип Timestamp
parsed_df = (df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")
 .withColumn("event_time", to_timestamp(col("timestamp"))))

# Сессионная агрегация: считаем события для каждого пользователя в рамках сессии
session_window_agg = (parsed_df
    .groupBy(
        session_window(
            col("event_time"),
            # Определяем интервал динамически
            when(col("event_type") == "purchase", "5 seconds").otherwise("10 seconds")
        ),
        col("user_id")
    )
    .agg(
        count("*").alias("events_count"),
        sum("value").alias("total_value")
    )
    .orderBy(desc("session_window.start"))
)

# Вывод в консоль
query = (session_window_agg.writeStream
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint_session_window")
    .outputMode("complete")
    .start())

query.awaitTermination()