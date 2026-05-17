from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("LinReg_Example").getOrCreate()

# Создаем набор данных (Опыт_лет, Пройдено_курсов, Зарплата_тыс)
data = [
    (1.0, 2.0, 50.0), (3.0, 1.0, 70.0), (5.0, 4.0, 120.0), (10.0, 3.0, 180.0),
    (2.0, 1.0, 56.0), (4.0, 2.0, 91.0), (6.0, 2.0, 118.0), (7.0, 3.0, 139.0),
    (8.0, 1.0, 138.0), (9.0, 2.0, 159.0)
]
df = spark.createDataFrame(data, ["experience", "courses", "salary"])

# Сборка вектора признаков
assembler = VectorAssembler(inputCols=["experience", "courses"], outputCol="features")
df_prepared = assembler.transform(df)

# Разбиваем данные (80% на обучение, 20% на тест)
# seed=42 фиксирует случайность
train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)

# Инициализация и обучение ТОЛЬКО на train_df
lir = LinearRegression(featuresCol="features", labelCol="salary")
lir_model = lir.fit(train_df)

print(f"Веса признаков (коэффициенты): {lir_model.coefficients}")
print(f"Стартовое смещение (пересечение): {lir_model.intercept:.2f}")

# Предсказания на test_df
predictions = lir_model.transform(test_df)
predictions.select("experience", "courses", "salary", "prediction").show()

# Оцениваем качество с помощью RMSE
evaluator = RegressionEvaluator(
    labelCol="salary",
    predictionCol="prediction",
    metricName="rmse"
)

rmse_score = evaluator.evaluate(predictions)
print(f"Ошибка RMSE: {rmse_score:.3f} тыс. руб.")

spark.stop()