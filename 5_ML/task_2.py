from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = (SparkSession
         .builder
         .appName("Task2")
         .getOrCreate())
# 1. Подготовка датасета: Создайте датафрейм и самостоятельно разбейте данные на train_df и test_df в пропорции 70% на 30%. Установите seed=42.
data = [
    (1, 2010, 150000, 1.6, 5000),
    (2, 2015, 80000, 2.0, 12000),
    (3, 2018, 40000, 1.5, 18000),
    (4, 2020, 15000, 2.5, 30000),
    (5, 2012, 120000, 1.6, 7500),
    (6, 2022, 5000, 3.0, 50000),
    (7, 2016, 60000, 2.0, 15000),
    (8, 2019, 30000, 2.5, 25000)
]
columns = ["id", "year", "mileage", "engine_vol", "price"]
df_reg = spark.createDataFrame(data, columns)

train_df, test_df = df_reg.randomSplit([0.7, 0.3], seed=42)

# 2. Векторизация: Соберите колонки year, mileage и engine_vol в единый столбец features при помощи VectorAssembler.
# Сделайте трансформацию для train и test датасетов.
vector = VectorAssembler(inputCols=["year","mileage","engine_vol"], outputCol="raw_features")
vector_traind_df = vector.transform(train_df)
vector_test_df = vector.transform(test_df)

# 3. Обучение модели: Инициализируйте LinearRegression. Обучите модель на train_df.
lir = LinearRegression(labelCol="price", featuresCol="raw_features")
lir_model = lir.fit(vector_traind_df)

# 4. Интерпретация: Выведите из модели вычисленные coefficients (веса для каждого признака) и intercept (смещение).
print(f"Веса признаков (коэффициенты): {lir_model.coefficients}")
print(f"Стартовое смещение (пересечение): {lir_model.intercept:.2f}")

# 5. Оценка качества: Сделайте предсказания на test_df и используйте RegressionEvaluator (метрика r2), чтобы оценить качество модели.
prediction = lir_model.transform(vector_test_df)
evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")
score = evaluator.evaluate(prediction)

print(f"Ошибка r2: {score:.3f} тыс. руб.")

spark.stop()