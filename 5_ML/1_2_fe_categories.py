from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer

spark = SparkSession.builder.appName("StringIndexerExample").getOrCreate()

# Данные: Laptop (3), Smartphone (2), Tablet (1)
data = [
    ("Laptop",), ("Smartphone",), ("Laptop",),
    ("Tablet",), ("Smartphone",), ("Laptop",)
]
df = spark.createDataFrame(data, ["category"])

print("Исходные данные:")
df.show()

# Настройка StringIndexer
# inputCol: откуда берем (строки), outputCol: куда кладем (индексы)
indexer = StringIndexer(inputCol="category", outputCol="category_index")

# fit(): Пробегает по данным, считает частоту слов (строит словарь)
indexer_model = indexer.fit(df)

# transform(): Заменяет слова на индексы
indexed_df = indexer_model.transform(df)

print("Результат (индексы):")
indexed_df.show()
spark.stop()