from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
spark = SparkSession.builder \
    .appName("VectorAssemblerExample") \
    .getOrCreate()

# Для примера датафрейм с телефонами с полями: screen_size, battery_mah и ram_gb
data = [
    (6.1, 4000, 8),   # Смартфон A
    (6.7, 4500, 12),  # Смартфон B
    (5.4, 3000, 4)    # Смартфон C
]
columns = ["screen_size", "battery_mah", "ram_gb"]

df = spark.createDataFrame(data, columns)
print("Исходные данные:")
df.show()

# Настройка VectorAssembler
# Задача: Объединить screen_size, battery_mah и ram_gb в один вектор features
assembler = VectorAssembler(
    inputCols=["screen_size", "battery_mah", "ram_gb"],
    outputCol="features"
)

# Трансформация (Transform)
output = assembler.transform(df)

print("Результат (колонка features):")
output.show()
spark.stop()