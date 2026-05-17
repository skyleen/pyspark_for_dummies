from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = (SparkSession
         .builder
         .appName("Task3")
         .getOrCreate())
# 1. Подготовка датасета: Создайте датафрейм и самостоятельно разбейте данные на тренировочную и тестовую выборку (в пропорции 70% на 30%, seed=42):
data = [
    (1, 50000, 700, "Employed", 1),
    (2, 20000, 600, "Unemployed", 0),
    (3, 80000, 750, "Employed", 1),
    (4, 30000, 620, "Employed", 0),
    (5, 120000, 800, "Employed", 1),
    (6, 15000, 550, "Unemployed", 0),
    (7, 45000, 680, "Employed", 1),
    (8, 25000, 590, "Unemployed", 0),
    (9, 60000, 720, "Employed", 1),
    (10, 35000, 640, "Employed", 0)
]
columns = ["client_id", "income", "credit_score", "status", "approved"]
df_credit = spark.createDataFrame(data, columns)

df_train, df_test = df_credit.randomSplit([0.7, 0.3], seed=42)

# 2. Создание ступеней конвейера: Настройте StringIndexer для статуса и VectorAssembler для колонок income, credit_score и status_idx.
# Задайте алгоритм LogisticRegression для обучения на целевую колонку approved (maxIter=10).
indexer = StringIndexer(inputCol="status", outputCol="status_idx")
assembler = VectorAssembler(inputCols=["income", "credit_score", "status_idx"], outputCol="raw_features")
logreg = LogisticRegression(featuresCol="raw_features", labelCol="approved", maxIter=10)

# 3. Конвейер: Соберите эти 3 трансформера/алгоритма в единый класс Pipeline.
# Вызовите метод fit от пайплайна к тренировочным данным.
pipeline = Pipeline(stages=[indexer, assembler, logreg])
trained_logreg = pipeline.fit(df_train)

# 4. Оценка: Примените обученный конвейер к test_df и выведите ROC AUC.
prediction = trained_logreg.transform(df_test)
evaluator = BinaryClassificationEvaluator(labelCol="approved", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
roc_auc = evaluator.evaluate(prediction)

# Выведите на экран колонки предсказаний.
prediction.select("client_id", "approved", "probability", "prediction").show(truncate=False)
print(f"Метрика ROC AUC: {roc_auc:.3f}")

spark.stop()