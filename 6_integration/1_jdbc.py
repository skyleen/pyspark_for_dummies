# spark = (SparkSession.builder
#     .appName("PostgresIntegration")
#     .config("spark.jars.packages",
#             "org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
#     .getOrCreate())
#
# spark = (SparkSession.builder
#     .appName("LocalDriverApp")
#     .config("spark.jars", "/absolute/path/to/postgresql-42.7.3.jar")
#     .getOrCreate())

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from pyspark.sql.functions import col

# Обязательно подгружаем нужный драйвер при старте сессии
spark = (SparkSession.builder
    .appName("JDBC Integration")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate())

jdbc_url = "jdbc:postgresql://localhost:5432/spark_db"

# Создаем тестовые данные
data = [
    (1, "Alice", "Sales", 150000, True),
    (2, "Bob", "Marketing", 120000, False),
    (3, "Carol", "IT", 250000, True),
    (4, "Dave", "IT", 80000, True),
    (5, "Eve", "HR", 100000, True)
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("is_active", BooleanType(), True)
])

initial_df = spark.createDataFrame(data, schema)

# Записываем исходные данные в Postgres
(initial_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("driver", "org.postgresql.Driver")
    .option("user", "spark_user")
    .option("password", "spark_password")
    .option("dbtable", "public.employees")
    .mode("overwrite")
    .save())

# Cчитываем данные из таблицы (В 3 параллельных потока)
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("driver", "org.postgresql.Driver")
    .option("user", "spark_user")
    .option("password", "spark_password")
    # Используем подзапрос, чтобы забрать только нужные строки
    .option("dbtable", "(SELECT id, name, department, salary FROM employees WHERE is_active = true) AS subq")
    # Параметры параллельного чтения
    .option("partitionColumn", "id")
    .option("lowerBound", "1")
    .option("upperBound", "100")
    .option("numPartitions", "3")
    # Защита от OutOfMemory
    .option("fetchsize", "10")
    .load())

# Минимальная обработка (вычисляем премию 10%)
processed_df = df.withColumn("bonus", col("salary") * 0.1)

processed_df.show()

(processed_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("driver", "org.postgresql.Driver")
    .option("user", "spark_user")
    .option("password", "spark_password")
    .option("dbtable", "public.employees_bonuses")
    # Оптимизация записи и безопасная перезапись
    .option("batchsize", "5000")
    .option("truncate", "true")
    .mode("overwrite")
    .save())

spark.stop()
