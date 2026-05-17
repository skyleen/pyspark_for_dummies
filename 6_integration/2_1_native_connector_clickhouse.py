from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = (SparkSession.builder
    .appName("ClickHouse")
    .master('local[*]')
    .config("spark.driver.memory", "15g")
    .config("spark.jars.packages", "com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.10.0")
    .getOrCreate())

# Тестовый DataFrame
data = [(101, "iPhone 15", 95000.0, "Moscow"), (102, "MacBook Air", 120000.5, "Kazan")]
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("city", StringType(), True)
])
df = spark.createDataFrame(data, schema)

(df.write
    .format("clickhouse")
    .option("host", "localhost")
    .option("http_port", "8123")
    .option("database", "analytics_db")
    .option("table", "sales_history")
    .option("user", "spark_user")
    .option("password", "spark_password")
    .option("engine", "MergeTree()")
    .option("order_by", "id")
    .option("batchsize", "100000") # Переопределение размера пакета
    .mode("append")
    .save())

ch_df = (spark.read
    .format("clickhouse")
    .option("host", "localhost")
    .option("http_port", "8123")
    .option("database", "analytics_db")
    .option("table", "sales_history")
    .option("user", "spark_user")
    .option("password", "spark_password")
    .load())

# Pushdown фильтрации
filtered_df = ch_df.filter("price > 1000 AND city = 'Moscow'")
filtered_df.show()

spark.stop()