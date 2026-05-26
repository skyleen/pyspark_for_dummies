from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

# Подключаем коннектор MongoDB
spark = (SparkSession.builder
    .appName("Mongo")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.0.1")
    .getOrCreate())

# Создаем иерархическую схему данных
mongo_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("user_profile", StructType([            # Вложенный документ
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ]), True),
    StructField("action_tags", ArrayType(StringType()), True) # Массив строк
])

# Тестовые данные
mongo_data = [
    (1, ("Alice", 28), ["login", "view_item", "add_to_cart"]),
    (2, ("Bob", 34), ["login", "checkout"])
]

df_mongo = spark.createDataFrame(mongo_data, mongo_schema)

(df_mongo.write
    .format("mongodb")
    .option("connection.uri", "mongodb://spark_user:spark_password@localhost:27017/?authSource=admin")
    .option("database", "shop_db")
    .option("collection", "raw_events")
    .mode("append")
    .save())

mongo_exploration_df = (spark.read
    .format("mongodb")
    .option("connection.uri", "mongodb://spark_user:spark_password@localhost:27017/?authSource=admin")
    .option("database", "shop_db")
    .option("collection", "raw_events")
    .option("sampleSize", "5000") # Количество документов для сканирования
    .load())

mongo_exploration_df.printSchema()

# Прописываем ожидаемую структуру
expected_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("user_profile", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ]), True),
    StructField("action_tags", ArrayType(StringType()), True)
])

mongo_prod_df = (spark.read
    .format("mongodb")
    .option("connection.uri", "mongodb://spark_user:spark_password@localhost:27017/?authSource=admin")
    .option("database", "shop_db")
    .option("collection", "raw_events")
    .schema(expected_schema) # 1. Передаем схему (sampleSize больше не нужен)
    .option("mode", "DROPMALFORMED") # 2. Стратегия парсинга
    .option("aggregation.pipeline", """[{"$match": {"user_profile.age": {"$gt": 30}}}]""") # 3. Pushdown фильтр
    .load())

mongo_prod_df.show(truncate=False)

spark.stop()
