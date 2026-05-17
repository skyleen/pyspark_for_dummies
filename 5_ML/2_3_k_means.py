from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder.appName("KMeans_Example").getOrCreate()

# Данные: возраст, сумма покупок (тыс. руб)
data = [
    (20, 5.0),  (22, 6.5),  (21, 5.5),   # Студенты
    (40, 55.0), (45, 60.0), (42, 58.0),  # VIP-клиенты
    (65, 15.0), (70, 18.0), (68, 16.0)   # Пенсионеры
]
df = spark.createDataFrame(data, ["age", "spending"])

# Собираем фичи в вектор
assembler = VectorAssembler(inputCols=["age", "spending"], outputCol="features")
df_features = assembler.transform(df)

# Инициализируем алгоритм, задав k=3 (количество кластеров)
kmeans = KMeans(k=3, featuresCol="features", predictionCol="cluster")

# Обучаем модель
kmeans_model = kmeans.fit(df_features)


# Получаем предсказания
predictions = kmeans_model.transform(df_features)
predictions.show()

# Оценка качества кластеризации
evaluator = ClusteringEvaluator(predictionCol="cluster", featuresCol="features", metricName="silhouette", distanceMeasure="squaredEuclidean")
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette Score: {silhouette}")

spark.stop()