from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler

spark = (SparkSession
         .builder
         .appName("Task1_FeatureEngineering")
         .getOrCreate())

data = [
    (1, "Toyota", 2015, 12000, 1.6),
    (2, "Ford", 2018, 45000, 2.0),
    (3, "Toyota", 2020, 80000, 2.5),
    (4, "Nissan", 2012, 8000, 1.5),
    (5, "Ford", 2019, 50000, 2.0),
    (6, "BMW", 2021, 150000, 3.0),
    (7, "Nissan", 2016, 20000, 1.6)
]
columns = ["id", "brand", "year", "price", "engine_vol"]
df_cars = spark.createDataFrame(data, columns)

# Кодирование категорий: Примените StringIndexer, чтобы преобразовать текстовую колонку brand в числовую колонку brand_idx.
indexer = StringIndexer(inputCol="brand", outputCol="brand_idx")
# fit(): Пробегает по данным, считает частоту слов (строит словарь)
indexer_model = indexer.fit(df_cars)
# transform(): Заменяет слова на индексы
indexed_df = indexer_model.transform(df_cars)

# Сборка признаков: Примените VectorAssembler, чтобы собрать колонки brand_idx, year и engine_vol в единый вектор raw_features.
vector = VectorAssembler(inputCols=["brand_idx","year","engine_vol"], outputCol="raw_features")
vector_df = vector.transform(indexed_df)

# Масштабирование (Стандартизация): Используйте StandardScaler (с параметрами withStd=True, withMean=True) на колонке raw_features,
# чтобы получить отмасштабированный вектор признаков scaled_features.
scaler = StandardScaler(inputCol="raw_features", outputCol="scaled_features", withStd=True, withMean=True)
scaler_model = scaler.fit(vector_df)

# Выведите исходный id, price и итоговую матрицу scaled_features.
(scaler_model
    .transform(vector_df)
    .select("id", "price", "scaled_features")
    .show(truncate=False))

spark.stop()