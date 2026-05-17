from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("SchemaDefinition") \
    .getOrCreate()

# Определение схемы
simple_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("employee_name", StringType(), True),
    StructField("age", IntegerType(), True),         # age может быть null
    StructField("salary", DoubleType(), False)
])

# Порядок элементов в кортежах должен соответствовать порядку столбцов в схеме
data = [
    (1, "Даша", 29, 50000.0),
    (2, "Вася", 30, 45000.5),
    (3, "Петя", None, 60000.0), # Возраст неизвестен, что допустимо
    (4, "Маша", 35, 70000.0)
]

# Передаем список данных и объект схемы
df = spark.createDataFrame(data, schema=simple_schema)

print("Содержимое DataFrame:")
df.show()
print("Схема:")
df.printSchema()

# Определение схемы с помощью DDL-строки
ddl_schema = """employee_id INT NOT NULL, 
                employee_name STRING NOT NULL, 
                age INT, 
                salary DOUBLE NOT NULL"""

# Порядок элементов в кортежах должен соответствовать порядку столбцов в схеме
data = [
    (1, "Даша", 29, 50000.0),
    (2, "Вася", 30, 45000.5),
    (3, "Петя", None, 60000.0), # Возраст неизвестен, что допустимо
    (4, "Маша", 35, 70000.0)
]

# Передаем список данных и объект схемы
df = spark.createDataFrame(data, schema=ddl_schema)

print("Содержимое DataFrame:")
df.show()
print("Схема:")
df.printSchema()

# Определение схемы
user_profile_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("username", StringType(), False),
    StructField("skills", ArrayType(StringType(), True), True), # массив строк, может быть null
    StructField("contact_info", MapType(StringType(), StringType(), True), True) # словарь, может быть null
])

# Данные для DataFrame
profile_data = [
    (1, "Андрей", ["Python", "Spark", "SQL"], {"email": "andrey@example.com", "phone": "111-222"}),
    (2, "Светлана", ["R", "Statistics"], {"phone": "333-444", "telegram": "@svetlana_tg"}),
    (3, "Дмитрий", ["Java"], None), # Дмитрий не указал контактную информацию
    (4, "Ольга", None, {"email": "olga@example.com"}) # Ольга не указала навыки
]

# Создание DataFrame
df_profiles = spark.createDataFrame(profile_data, schema=user_profile_schema)

print("DataFrame:")
# параметр truncate=False выводит все строки полностью
df_profiles.show(truncate=False)

print("Схема:")
df_profiles.printSchema()

df_processed_map = df_profiles.select(
    col("user_id"),
    col("username"),
    # Извлекаем значение по ключу phone из столбца contact_info
    col("contact_info")["phone"].alias("phone_number"),
    # Также можно извлечь email
    col("contact_info")["email"].alias("email_address")
)
df_processed_map.show(truncate=False)

# Разворачивание навыков с explode_outer
df_exploded_skills = df_profiles.select(
    col("user_id"),
    col("username"),
    explode_outer(col("skills")).alias("skill") # Сохраняет строки, где skills = null
)
print("Разворачивание ArrayType с explode_outer (навыки):")
df_exploded_skills.show()

# Разворачивание контактной информации с explode
df_exploded_contact = df_profiles.select(
    col("user_id"),
    col("username"),
    explode(col("contact_info")).alias("contact_key", "contact_value") # Удаляет строки, где contact_info = null
)
print("Разворачивание MapType с explode (контактная информация):")
df_exploded_contact.show()

# Не забываем остановить SparkSession
spark.stop()

# ArrayType:
# size(col): Возвращает количество элементов в массиве или карте.
# array_contains(col, value): Проверяет, содержит ли массив указанное значение.
# array_distinct(col): Возвращает массив с уникальными элементами.
# array_join(col, delimiter, null_replacement=None): Объединяет элементы массива в строку.
# sort_array(col, asc=True): Сортирует элементы массива.
# element_at(col, index): Возвращает элемент массива по индексу (начиная с 1).
# concat / array_concat(col1, col2, ...): Объединяет несколько массивов в один.
# arrays_overlap(col1, col2): Проверяет, есть ли общие элементы у двух массивов. Она возвращает true, если у массивов есть хотя бы один общий элемент, false — если нет.

# MapType:
# map_keys(col): Возвращает ArrayType из всех ключей карты.
# map_values(col): Возвращает ArrayType из всех значений карты.
# create_map(col1, col2, ...): Создает новую карту из пар колонок (ключ, значение).
# map_from_arrays(keys_col, values_col): Создает карту из двух массивов: один для ключей, другой для значений.