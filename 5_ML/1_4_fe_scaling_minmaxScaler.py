from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors

spark = SparkSession.builder.appName("MinMaxExample").getOrCreate()

data = [
    (Vectors.dense([10.0]),),  # Минимум
    (Vectors.dense([50.0]),),  # Середина
    (Vectors.dense([90.0]),)   # Максимум
]
df = spark.createDataFrame(data, ["features"])

print("Исходные данные:")
df.show()

scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

scalerModel = scaler.fit(df)
scaledData = scalerModel.transform(df)
print("Результат (scaledFeatures):")
scaledData.show()
spark.stop()