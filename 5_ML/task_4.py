from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = (SparkSession
         .builder
         .appName("Task4")
         .getOrCreate())

# 1. Подготовка датасета: Загрузите данные в датафрейм и разбейте их на train_df и test_df (в пропорции 70% на 30%, установите seed=42):
data = [
    (1, 7.4, 0.70, 0.0, 9.4, 0),
    (2, 7.8, 0.88, 0.0, 9.8, 0),
    (3, 7.8, 0.76, 0.04, 9.8, 1),
    (4, 11.2, 0.28, 0.56, 9.8, 1),
    (5, 7.4, 0.70, 0.0, 9.4, 0),
    (6, 7.4, 0.66, 0.0, 9.4, 0),
    (7, 7.9, 0.60, 0.06, 9.4, 1),
    (8, 7.3, 0.65, 0.0, 10.0, 1),
    (9, 7.8, 0.58, 0.02, 9.5, 1),
    (10, 7.5, 0.50, 0.36, 10.5, 1),
    (11, 6.7, 0.58, 0.08, 9.2, 0),
    (12, 7.5, 0.50, 0.36, 10.5, 1)
]
columns = ["id", "acidity", "volatile_acidity", "citric_acid", "alcohol", "quality"]
df_wine = spark.createDataFrame(data, columns)

df_train, df_test = df_wine.randomSplit([0.7, 0.3], seed=42)

# 2. Сбор признаков: Примените VectorAssembler ко всем числовым колонкам (кроме id и quality), собрав их в колонку features.
# Примените трансформации к датасетам.
assembler = VectorAssembler(inputCols=["acidity", "volatile_acidity", "citric_acid", "alcohol"], outputCol="features")
assembled_train_df = assembler.transform(df_train)
assembled_test_df = assembler.transform(df_test)

# 3. CrossValidator: Настройте экземпляр RandomForestClassifier (ограничьте seed=42).
# Используя ParamGridBuilder, задайте сетку поиска для двух гиперпараметров дерева:
# maxDepth - варианты [2, 4]
# numTrees - варианты [5, 10]
forest = RandomForestClassifier(labelCol="quality", featuresCol="features", seed=42)
evaluator = BinaryClassificationEvaluator(labelCol="quality", metricName="areaUnderROC")
grid = (ParamGridBuilder()
    .addGrid(forest.maxDepth, [2, 4])         # Глубина отдельного дерева
    .addGrid(forest.numTrees, [5, 10])  # Количество деревьев в ансамбле
    .build())

# 4. Запуск: Укажите метрику BinaryClassificationEvaluator(metricName="areaUnderROC").
# Запустите обучение через CrossValidator, указав параметры: numFolds=2 (используем 2 из-за маленького объема данных) и seed=42.
# Обучите объект валидатора.
cv = CrossValidator(estimator=forest,
                    estimatorParamMaps=grid,
                    evaluator=evaluator,
                    numFolds=2,
                    seed=42)
cv_model = cv.fit(assembled_train_df)

# 5. Анализ: Вытащите обученную лучшую модель из валидатора (cv_model.bestModel)
# и выведите на экран её лучшие параметры количества деревьев (getNumTrees) и глубины (getOrDefault('maxDepth')).
trained_model = cv_model.bestModel
predictions = trained_model.transform(assembled_test_df)
score = evaluator.evaluate(predictions)

print(f"Num trees: {trained_model.getNumTrees}")
print(f"Depth: {trained_model.getOrDefault('maxDepth')}")
print(f"ROC AUC: {score}")

spark.stop()