from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Создание SparkSession
spark = SparkSession.builder \
    .appName("SparkReadDataExamples") \
    .getOrCreate()

# mode: Определяет поведение Spark при обнаружении поврежденных или некорректных записей:
# PERMISSIVE (по умолчанию): Вставляет null для поврежденных полей.
# DROPMALFORMED: Отбрасывает строки с поврежденными записями.
# FAILFAST: Вызывает исключение при обнаружении первой поврежденной записи.

# Предполагаем, что у нас есть файл 'sample_data.csv' в корне проекта или доступном пути
# Создадим временный CSV-файл для демонстрации
csv_data = """Name,Age,City,IsStudent
Алиса,25,Москва,true
Вася,30,Санкт-Петерубург,false
Артур,22,Ростов,true
Маша,35,Уфа,false
"""
with open("/Users/n.shikhaleva/Downloads/sample_data.csv", "w") as f:
    f.write(csv_data)

# header: Указывает, содержит ли первый ряд файла заголовки столбцов. По умолчанию False.
# inferSchema: По умолчанию False. Если True, Spark автоматически определяет типы данных. Может быть медленным для больших файлов.
# schema:  Позволяет указать схему данных. Это предпочтительный и более производительный метод для больших файлов.
# sep (или delimiter): Строковое значение (по умолчанию ,). Указывает символ-разделитель для значений (например, ,, ;, \t)

df_csv_auto = spark.read.csv(
    "/Users/n.shikhaleva/Downloads/sample_data.csv",      # Путь к файлу
    header=True,            # Указывает, что первая строка содержит заголовки столбцов
    inferSchema=True        # Автоматически определяет типы данных для каждого столбца
)

print("Схема DataFrame:")
df_csv_auto.printSchema()
print("Первые 5 строк DataFrame:")
df_csv_auto.show(5)

# Создадим временный CSV-файл с разделителем ';'
csv_data = """Name;Age;City;IsStudent
Алиса;25;Москва;true
Вася;30;Санкт-Петерубург;false
Артур;22;Ростов;true
Маша;35;Уфа;false
"""
with open("/Users/n.shikhaleva/Downloads/sample_data2.csv", "w") as f:
    f.write(csv_data)

# Создание схемы для данных
custom_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True),
    StructField("IsStudent", BooleanType(), True)
])

df_csv_custom_schema = spark.read.csv(
    "/Users/n.shikhaleva/Downloads/sample_data2.csv",
    header=True,
    schema=custom_schema,   # Применяем явно заданную схему
    sep=";"           # Указываем, что разделитель значений - ;
)

print("Схема DataFrame:")
df_csv_custom_schema.printSchema()
print("Первые 5 строк DataFrame:")
df_csv_custom_schema.show(5)

# Создадим временный JSON файл для демонстрации
json_data = """{"name": "Алиса", "age": 25, "city": "Москва"}
{"name": "Вася", "age": 30, "city": "Санкт-Петерубург", "hobby": "reading"}
{"name": "Артур", "city": "Ростов", "occupation": "engineer"}
"""

with open("/Users/n.shikhaleva/Downloads/sample_data.json", "w") as f:
    f.write(json_data)

# Spark автоматически определяет схему для JSON
# multiLine: По умолчанию False.
# Если True, Spark рассматривает каждую JSON-запись как потенциально многострочную.
# Если False, каждая строка файла — отдельная JSON-запись.
# schema: Может использоваться для явного указания схемы, хотя Spark часто хорошо выводит ее для JSON.

df_json = spark.read.json("/Users/n.shikhaleva/Downloads/sample_data.json")

print("Схема DataFrame (JSON):")
df_json.printSchema()
print("Первые 5 строк DataFrame (JSON):")
df_json.show(5)

spark.stop()