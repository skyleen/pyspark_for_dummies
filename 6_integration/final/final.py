import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, MapType

load_dotenv() # rename .env.example to .env first

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_USER = os.getenv("S3_USER")
S3_PASS = os.getenv("S3_PASS")
PG_URL = os.getenv("PG_URL")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
MNG_URL = os.getenv("MNG_URL")
MNG_DB = os.getenv("MNG_DB")
MNG_USER = os.getenv("MNG_USER")
MNG_PASS = os.getenv("MNG_PASS")
CH_HOST = os.getenv("CH_HOST")
CH_PORT = os.getenv("CH_PORT")
CH_DB = os.getenv("CH_DB")
CH_USER = os.getenv("CH_USER")
CH_PASS = os.getenv("CH_PASS")

spark = (SparkSession.builder
    .appName("final_integration")
    .master('local[*]')
    .config("spark.driver.memory", "15g")
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.7.3,"
            "com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.10.0,"
            "org.mongodb.spark:mongo-spark-connector_2.13:11.0.1,"
            "org.apache.hadoop:hadoop-aws:3.5.0,"
            "software.amazon.awssdk:bundle:2.44.7")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_USER)
    .config("spark.hadoop.fs.s3a.secret.key", S3_PASS)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.committer.name", "magic")
    .getOrCreate())

# connections
pg_jdbc_url = f"{PG_URL}/{PG_DB}"
pg_table_name = 'users'
mng_url = f"mongodb://{MNG_USER}:{MNG_PASS}@{MNG_URL}/?authSource=admin"
minio_url = "s3a://events/raw_logs/"

# schemas
pg_users_table_schema =StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("level", StringType(), True)
])
mng_products_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("specs", MapType(StringType(), StringType(), True)),
    StructField("refresh_rate", StringType(), True),
    StructField("os", StringType(), True),
    StructField("details", MapType(StringType(), StringType(), True))
])
minio_logs_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("timestamp", StringType(), False)
])
logs_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("timestamp", TimestampType(), False)
])
# prepare data
# pg_data = [
#     (1, 'Anastasia', 'Limassol', 'Premium'), (2, 'Petr', 'Moscow', 'Standard'),
#     (3, 'Maria', 'Astana', 'Premium'), (4, 'Igor', 'Yerevan', 'Standard'),
#     (5, 'Elena', 'Tbilisi', 'Premium')
# ]
# pg_init_df = spark.createDataFrame(pg_data, pg_users_table_schema)
# (pg_init_df.write
#     .format("jdbc")
#     .option("url", pg_jdbc_url)
#     .option("driver", "org.postgresql.Driver")
#     .option("user", PG_USER)
#     .option("password", PG_PASS)
#     .option("dbtable", f"public.{pg_table_name}")
#     .mode("overwrite")
#     .save())
#
# mng_data = [
#   { "id": 101, "title": "Mechanical Keyboard", "category": "IT", "price": 12000.00, "details": {"switch": "Red"} },
#   { "id": 102, "title": "Coffee Machine", "category": "Home", "price": 45000.00, "specs": {"pressure": "15 bar"} },
#   { "id": 103, "title": "Data Engineering Course", "category": "Education", "price": 15000.00 },
#   { "id": 104, "title": "Monitor 4K", "category": "IT", "price": 35000.00, "refresh_rate": "144Hz" },
#   { "id": 105, "title": "Smartphone", "category": "IT", "price": 60000.00, "os": "Android" }
# ]
# mng_init_df = spark.createDataFrame(mng_data, mng_products_schema)
# (mng_init_df.write
#     .format("mongodb")
#     .option("connection.uri", mng_url)
#     .option("database", MNG_DB)
#     .option("collection", "products")
#     .mode("overwrite")
#     .save())
#
# raw_logs = [
#     (1, 101, "2026-03-01 09:00:00"), (1, 104, "2026-03-02 10:15:00"), (2, 102, "2026-03-03 11:30:00"),
#     (3, 101, "2026-03-04 12:45:00"), (3, 103, "2026-03-05 13:00:00"), (3, 105, "2026-03-06 14:15:00"),
#     (4, 102, "2026-03-07 15:30:00"), (5, 101, "2026-03-08 16:45:00"), (5, 105, "2026-03-09 17:00:00"),
#     (1, 102, "2026-03-10 18:15:00"), (2, 105, "2026-03-11 19:30:00"), (3, 102, "2026-03-12 20:45:00"),
#     (4, 104, "2026-03-13 21:00:00"), (5, 103, "2026-03-14 22:15:00"), (1, 105, "2026-03-15 23:30:00"),
#     (2, 101, "2026-03-16 08:45:00"), (3, 104, "2026-03-17 09:00:00"), (4, 101, "2026-03-18 10:15:00"),
#     (5, 102, "2026-03-19 11:30:00"), (1, 103, "2026-03-20 12:45:00")
# ]
# minio_init_df = spark.createDataFrame(raw_logs, minio_logs_schema)
# minio_init_df = minio_init_df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))
# minio_init_df.write.mode("overwrite").parquet(minio_url)

# extract
users_df = (spark.read
            .format("jdbc")
            .option("url", pg_jdbc_url)
            .option("driver", "org.postgresql.Driver")
            .option("user", PG_USER)
            .option("password", PG_PASS)
            .option("dbtable", "public.users")
            .option("partitionColumn", "id")
            .option("lowerBound", "1")
            .option("upperBound", "10")
            .option("numPartitions", "3")
            .option("fetchsize", "10")
            .load())
products_df = (spark.read
                .format("mongodb")
                .option("connection.uri", mng_url)
                .option("database", MNG_DB)
                .option("collection", "products")
                .option("sampleSize", "5000")
                .schema(mng_products_schema)
                .load())
logs_df = (spark.read
           .format("parquet")
           .schema(logs_schema)
           .option("mode", "DROPMALFORMED")
           .load(minio_url))

# transform
# Соедините логи просмотров с пользователями и товарами.
# Отфильтруйте пользователей уровня Premium.
# Рассчитайте витрину: город, количество просмотров, средняя цена товаров и самая популярная категория в городе.
join_df = (logs_df
           .join(products_df, logs_df.product_id == products_df.id, "left")
           .join(users_df, logs_df.user_id == users_df.id, "left")
           .filter(users_df.level == 'Premium')
           )
mart_df = (join_df
           .groupby("city","category")
           .agg(
                F.sum("price").alias("price"),
                F.count("*").alias("views_count"))
           .orderBy("city", F.desc("views_count"))
           .groupBy("city")
           .agg(
                F.sum("views_count").alias("view_count"),
                (F.sum("price")/F.sum("views_count")).alias("avg_price"),
                F.first("category").alias("most_popular_category")))
# load
# Запишите результат в ClickHouse в таблицу premium_analytics_report.
# Используйте .coalesce(1) для оптимизации записи.
ch_table = "premium_analytics_report"
(mart_df.coalesce(1)
    .write
    .format("clickhouse")
    .option("host", CH_HOST)
    .option("http_port", CH_PORT)
    .option("database", CH_DB)
    .option("table", ch_table)
    .option("user", CH_USER)
    .option("password", CH_PASS)
    .option("engine", "MergeTree()")
    .option("order_by", "city")
    .option("batchsize", "100000")
    .mode("overwrite")
    .save())

# result
ch_df = (spark.read
         .format("clickhouse")
         .option("host", CH_HOST)
         .option("http_port", CH_PORT)
         .option("database", CH_DB)
         .option("table", ch_table)
         .option("user", CH_USER)
         .option("password", CH_PASS)
         .load())

ch_df.show()

spark.stop()