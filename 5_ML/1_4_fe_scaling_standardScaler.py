from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors

spark = SparkSession.builder.appName("ScalerExample").getOrCreate()

# В Spark StandardScaler работает ТОЛЬКО с векторами!
# Поэтому данные должны быть уже собраны через VectorAssembler
data = [
    (Vectors.dense([100000.0, 30.0]),), # Зарплата, Возраст
    (Vectors.dense([50000.0, 45.0]),),
    (Vectors.dense([200000.0, 25.0]),)
]
df = spark.createDataFrame(data, ["features"])

print("Исходные векторы:")
df.show(truncate=False)

# Настройка: withStd=True (делим на отклонение), withMean=False (не вычитаем среднее для разреженных данных)
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                        withStd=True, withMean=False)

# fit(): Вычисляет среднее и отклонение по всему датасету
scalerModel = scaler.fit(df)

# transform(): Масштабирует данные
scaledData = scalerModel.transform(df)

print("Отмасштабированные признаки:")
scaledData.show(truncate=False)
spark.stop()