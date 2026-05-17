from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = (SparkSession
         .builder
         .appName("Task5")
         .getOrCreate())

# 1. Создание датасета: Сформируйте датафрейм и самостоятельно разбейте данные на train_df и test_df (пропорция 70/30, seed=42).
# Исходные данные для датафрейма:
data = [
    (1, 45, "Month-to-month", 75.5, 1),
    (2, 60, "One year", 105.2, 0),
    (3, 22, "Month-to-month", 55.0, 1),
    (4, 30, "Two year", 20.5, 0),
    (5, 55, "One year", 90.0, 0),
    (6, 40, "Month-to-month", 85.5, 1),
    (7, 28, "Two year", 25.0, 0),
    (8, 65, "Month-to-month", 100.5, 1),
    (9, 50, "One year", 60.5, 0),
    (10, 35, "Two year", 30.0, 0),
    (11, 42, "Month-to-month", 80.0, 1),
    (12, 58, "One year", 95.0, 0),
    (13, 25, "Month-to-month", 50.0, 1),
    (14, 32, "Two year", 22.5, 0),
    (15, 52, "One year", 88.0, 0),
    (16, 38, "Month-to-month", 82.0, 1),
    (17, 29, "Two year", 26.0, 0),
    (18, 62, "Month-to-month", 98.0, 1),
    (19, 48, "One year", 65.0, 0),
    (20, 36, "Two year", 32.0, 0)
]
columns = ["client_id", "age", "contract_type", "monthly_charges", "churn"]

df_churn = spark.createDataFrame(data, columns)
train_df, test_df = df_churn.randomSplit([0.7, 0.3], seed=42)

# 2. Подготовка признаков: Текстовая колонка contract_type содержит категорильные данные, преобразуйте ее в числовой формат.
# Затем соберите все независимые переменные в единый вектор features для подачи в алгоритм.
indexer = StringIndexer(inputCol="contract_type", outputCol="contract_type_idx")
assembler = VectorAssembler(inputCols=[ "age", "contract_type_idx", "monthly_charges"], outputCol="features")
gbt = GBTClassifier(labelCol="churn", featuresCol="features", maxIter=10, seed=42)

# 3. Обучение модели: Инициализируйте GBTClassifier (Градиентный Бустинг). Установите maxIter=10 и seed=42. Соберите трансформеры и алгоритм в единый объект Pipeline.
# Обучите Pipeline на train_df.
pipeline = Pipeline(stages=[indexer, assembler, gbt])
trained_pipeline = pipeline.fit(train_df)

# 4. Предсказания и оценка: Сделайте предсказания на test_df. Выведите колонки client_id, probability и prediction.
# При помощи BinaryClassificationEvaluator измерьте метрику ROC AUC.
prediction = trained_pipeline.transform(test_df)
prediction.select("client_id", "churn", "probability", "prediction").show(truncate=False)

evaluator = BinaryClassificationEvaluator(labelCol="churn", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
score = evaluator.evaluate(prediction)

print(f"ROC UAC: {score}")

spark.stop()