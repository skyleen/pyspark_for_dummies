from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("StringFunctionsExample") \
    .getOrCreate()

# Исходные данные
data = [
    (1, "Иван Петров", "Python;Spark;SQL"),
    (2, "Анна Сидорова ", "Java,Spring"), # Есть пробел в конце
    (3, "  Мария Козлова", "SQL"),       # Есть пробелы в начале
    (4, "Дмитрий Смирнов ", "Analytics"),
    (5, "Елена Волкова", "Cloud"),
    (6, "Виктор Белов", None)           # Отсутствуют данные в tags_str
]

# Определение схемы
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("course", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)
df.show(truncate=False)

df_string_clean = df.withColumn(
    "name_clean", F.initcap(F.trim(F.col("name"))) # Удаляем пробелы и приводим к единому регистру
)
df_string_clean.select("id", "name", "name_clean").show(truncate=False)

df_string_extract = df_string_clean.withColumn(
    "first_5_chars", F.substring(F.col("name_clean"), 1, 5) # Первые 5 символов имени
).withColumn(
    "last_name_pos", F.instr(F.col("name_clean"), " ") + 1 # Позиция начала фамилии (после первого пробела)
).withColumn(
    "full_name_length", F.length(F.col("name_clean")) # Длина имени
).withColumn(
    "is_ivan_like", F.col("name_clean").like("Иван%") # Начинается ли имя с "иван"
).withColumn(
    "contains_spark", F.col("course").rlike("Spark|spark") # Содержит ли тег "spark"
)

df_string_extract.show()

df_string_combine_split = df_string_extract.withColumn(
    # Объединение, F.lit() добавляет постоянные строки
    "greeting", F.concat(F.lit("Привет, "), F.col("name_clean"), F.lit("!"))
).withColumn(
    # Объединение с разделителем, cast() меняет тип строки
    "user_info", F.concat_ws(" - ", F.col("id").cast(StringType()), F.col("name_clean"))
).withColumn(
    # Разбиваем строку на массив по разделителям ";" или ","
    "tags_array", F.split(F.col("course"), "[;,]")
).withColumn(
    # Проверяем, есть ли "SQL" в массиве
    "has_sql_tag", F.array_contains(F.col("tags_array"), "SQL")
)
df_string_combine_split.select("id", "name_clean", "course", "greeting", "user_info", "tags_array", "has_sql_tag").show(truncate=False)
df_string_combine_split.printSchema()

df_string_other = df_string_combine_split.withColumn(
    # Заменяем ";" и "," на " | "
    "course_formatted", F.regexp_replace(F.col("course"), "[;,]", " | ")
).withColumn(
    # Заменяем первые 3 символа имени на "XXX"
    "name_overlay", F.overlay(F.col("name_clean"), F.lit("XXX"), 1, 3)
).withColumn(
    # Проверяем, является ли 'course' NULL
    "course_is_null", F.col("course").isNull()
)

df_string_other.select("id", "name_clean", "course", "course_formatted", "name_overlay", "course_is_null").show(truncate=False)

# Останавливаем SparkSession
spark.stop()