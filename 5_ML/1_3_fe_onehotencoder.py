from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoder

spark = SparkSession.builder.appName("OHEExample").getOrCreate()

# Создадим DataFrame, имитирующий результат работы StringIndexer (уже есть индексы)
# 0.0=Laptop, 1.0=Smartphone, 2.0=Tablet
data = [(0.0,), (1.0,), (2.0,), (0.0,)]
df = spark.createDataFrame(data, ["category_index"])

print("Исходные индексы:")
df.show()

# Настройка OneHotEncoder
encoder = OneHotEncoder(inputCols=["category_index"], outputCols=["category_vec"])

# fit(): Здесь он нужен для считывания меты
encoder_model = encoder.fit(df)

# transform(): Превращает индексы в SparseVector
encoded_df = encoder_model.transform(df)

print("Результат (векторы):")
encoded_df.show(truncate=False)
spark.stop()