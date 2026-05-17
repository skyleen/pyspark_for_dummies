from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("ConditionalExpressionsExample") \
    .getOrCreate()

# Исходные данные
data = [
    (1, "Пользователь А", 75, "Москва", 1200.50, datetime.date(2024, 5, 10)),
    (2, "Пользователь Б", 92, "Санкт-Петербург", 1500.00, datetime.date(2024, 6, 1)),
    (3, "Пользователь В", 45, "Екатеринбург", 450.75, datetime.date(2024, 5, 20)),
    (4, "Пользователь Г", None, "Новосибирск", 900.00, datetime.date(2024, 4, 15)), # Возраст = null
    (5, "Пользователь Д", 88, None, 2000.00, datetime.date(2024, 6, 5)), # Город = null
    (6, "Пользователь Е", 30, "Казань", None, datetime.date(2024, 3, 25)), # Доход = null
    (7, "Пользователь Ж", 60, "Москва", float('nan'), datetime.date(2024, 5, 12)), # Доход = NaN
    (8, "Пользователь З", None, "Самара", 700.00, None) # Возраст = null, Дата = null
]

# Определение схемы DataFrame
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("income", DoubleType(), True),
    StructField("registration_date", DateType(), True)
])

df = spark.createDataFrame(data, schema=schema)
df.show(truncate=False)

df_categorized = df.withColumn(
    "age_group", # Новый столбец для возрастной группы
    F.when(F.col("age").isNull(), "Возраст не указан")
     .when(F.col("age") < 18, "Младше 18") # Если возраст < 18
     .when((F.col("age") >= 18) & (F.col("age") <= 60), "Рабочий возраст")
     .otherwise("Пенсионный возраст") # Все остальные случаи (возраст > 60)
).withColumn(
    "income_status", # Новый столбец для статуса дохода
    F.when(F.col("income").isNull(), "Доход не указан")
     .when(F.isnan(F.col("income")), "Доход NaN (ошибка)")
     .when(F.col("income") < 1000, "Низкий доход")
     .when(F.col("income") >= 1000, "Высокий доход")
     .otherwise("Неизвестно")
).withColumn(
    "is_active_user", # Пример сложной бизнес-логики
    F.when((F.col("age") >= 18) & (F.col("income") >= 1000) & (F.col("registration_date") > F.lit(datetime.date(2024, 1, 1))), True)
     .otherwise(False)
)
df_categorized.select("id", "age", "age_group", "income", "income_status", "registration_date", "is_active_user").show(truncate=False)

# Исключаем NaN из расчёта, так как NaN не является null и если его оставить вернеться nan
average_income_val = df.filter(F.col("income").isNotNull() & ~F.isnan(F.col("income"))).agg(F.avg("income")).collect()[0][0]

df_filled_nulls = df.withColumn(
    "city_filled",
    F.coalesce(F.col("city"), F.lit("Неизвестен")) # Если city null, используем "Неизвестен"
).withColumn(
    "income_filled",
    F.coalesce(F.col("income"), F.lit(average_income_val), F.lit(0.0)) # Если income null, берем average_income_val, если и он null, то 0.0
)
df_filled_nulls.select("id", "city", "city_filled", "income", "income_filled").show(truncate=False)

df_null_nan_check = df.withColumn(
    "is_age_null", F.isnull(F.col("age")) # Проверяем, является ли age null
).withColumn(
    "is_city_null", F.isnull(F.col("city")) # Проверяем, является ли city null
).withColumn(
    "is_income_null", F.isnull(F.col("income")) # Проверяем, является ли income null
).withColumn(
    "is_income_nan", F.isnan(F.col("income")) # Проверяем, является ли income NaN
)
df_null_nan_check.select("id", "age", "is_age_null", "city", "is_city_null", "income", "is_income_null", "is_income_nan").show(truncate=False)

# Останавливаем SparkSession
spark.stop()