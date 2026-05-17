from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Инициализация SparkSession
spark = (SparkSession.builder
    .appName("DateTimeExample")
    .getOrCreate())

# Исходные данные
data = [
    (1, "Иван Петров", "15-01-2025", "15-04-2025",  "06-05-2025 10:00:00"),
    (2, "Анна Сидорова ", "20-05-2023", None, "01-06-2023 18:30:00"), # Есть данные о старте, но нет конца
    (3, "Мария Козлова", "01-02-2024", "21-08-2024", "23-06-2025 09:30:00"),
    (4, "Дмитрий Смирнов ", "25-11-2023", "14-01-2024", "04-11-2024 14:15:00"),
    (5, "Елена Волкова", "14-01-2024", "14-05-2025", "04-06-2025 17:00:00"),
    (6, "Виктор Белов", "17-04-2023", "17-10-2024", "17-10-2024 02:03:00")
]

# Определение схемы
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("course_start_date", StringType(), True),
    StructField("course_end_date", StringType(), True),
    StructField("last_login_timestamp", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)
df.show(truncate=False)

df_transform_time = df.withColumn(
                    "course_start_date_parsed", F.to_date(F.col("course_start_date"), "dd-MM-yyyy")
                     ).withColumn(
                    "course_end_date_parsed", F.to_date(F.col("course_end_date"), "dd-MM-yyyy")
                     ).withColumn(
                    "last_login_ts_parsed", F.to_timestamp(F.col("last_login_timestamp"), "dd-MM-yyyy HH:mm:ss")
                                )
df_transform_time.printSchema()

df_transform_time = df_transform_time.withColumn(
    # Извлекаем год из даты начала курса
    "start_year", F.year(F.col("course_start_date_parsed"))
).withColumn(
    # Извлекаем месяц из даты начала курса
    "start_month", F.month(F.col("course_start_date_parsed"))
).withColumn(
    # Извлекаем час из времени последнего входа
    "login_hour", F.hour(F.col("last_login_ts_parsed"))
).withColumn(
    # Извлекаем минуты из времени последнего входа
    "login_minute", F.minute(F.col("last_login_ts_parsed"))
)

df_transform_time.select("id", "course_start_date_parsed", "start_year", "start_month",
                         "last_login_ts_parsed", "login_hour", "login_minute").show(truncate=False)

df_transform_time = (df_transform_time
                    .withColumn(
    # Вычисляем, сколько дней прошло с даты начала курса до сегодняшнего дня
    "days_since_course_start", F.datediff(F.current_date(), F.col("course_start_date_parsed")))
                    .withColumn(
    # Вычисляем, сколько месяцев прошло с даты окончания
    "months_until_course_end", F.months_between(F.current_date(), F.col("course_end_date_parsed"))
))

df_transform_time.select("id", "course_start_date_parsed", "days_since_course_start",
                         "course_end_date_parsed", "months_until_course_end").show(truncate=False)

df_transform_time = df_transform_time.withColumn(
    # Вычитаем 30 дней от даты начала курса
    "course_start_add_30_days", F.date_add(F.col("course_start_date_parsed"), -30)
).withColumn(
    # Вычитаем 30 дней от даты начала курса
    "course_start_sub_30_days", F.date_sub(F.col("course_start_date_parsed"), 30)
).withColumn(
    # Вычитаем 2 месяца из даты окончания курса
    "course_start_sub_2_months", F.add_months(F.col("course_start_date_parsed"), -2)
)
df_transform_time.select("id", "course_start_date_parsed"
                         , "course_start_add_30_days", "course_start_sub_30_days", "course_start_sub_2_months").show(truncate=False)

# Останавливаем SparkSession
spark.stop()