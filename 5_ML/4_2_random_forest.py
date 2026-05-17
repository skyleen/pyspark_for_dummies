from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkFiles

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# 1. Загрузка данных (Титаник)
spark = SparkSession.builder.appName("RandomForestCV").getOrCreate()
url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
spark.sparkContext.addFile(url)

df = spark.read.csv(SparkFiles.get("titanic.csv"), header=True, inferSchema=True)
df = df.select(
    col("Survived").alias("label"),
    col("Pclass").cast("double"),
    col("Fare").cast("double"),
    col("Age").cast("double"),
    col("Sex")
).dropna()

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# 2. Создаем заново базовые трансформеры
indexer = StringIndexer(inputCol="Sex", outputCol="sex_index")
assembler = VectorAssembler(inputCols=["Pclass", "Fare", "Age", "sex_index"], outputCol="features")

# 3. Инициализируем базовый алгоритм Случайного Леса
rf = RandomForestClassifier(featuresCol="features", labelCol="label", seed=42)

# 4. Собираем новый Pipeline, объединяя старые стадии (indexer, assembler) и новый алгоритм (rf)
rf_pipeline = Pipeline(stages=[indexer, assembler, rf])

# 5. Строим сетку для перебора гиперпараметров ансамбля
rf_grid = (ParamGridBuilder()
    .addGrid(rf.maxDepth, [3, 5])         # Глубина отдельного дерева
    .addGrid(rf.numTrees, [10, 50, 100])  # Количество деревьев в ансамбле
    .build())

# 6. Задаем метрику оценки (это задача бинарной классификации, используем ROC AUC)
evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")

# 7. Заворачиваем Pipeline в CrossValidator
rf_cv = CrossValidator(
    estimator=rf_pipeline,
    estimatorParamMaps=rf_grid,
    evaluator=evaluator,
    numFolds=3,
    seed=42
)

# 8. Запускаем перебор (2 * 3 * 3 = 18 моделей)
rf_cv_model = rf_cv.fit(train_df)

# 9. Лучшая модель
rf_predictions = rf_cv_model.bestModel.transform(test_df)
print(f"ROC AUC лучшего Случайного Леса: {evaluator.evaluate(rf_predictions):.3f}")

spark.stop()