from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkFiles
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 1. Инициализация Spark
spark = SparkSession.builder.appName("TitanicPipelineCV").getOrCreate()

# 2. Загрузка сырых данных
url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
spark.sparkContext.addFile(url)

df = spark.read.csv(SparkFiles.get("titanic.csv"), header=True, inferSchema=True)

df = df.select(
    col("Survived").alias("label"),
    col("Pclass").cast("double"),
    col("Fare").cast("double"),
    col("SibSp").cast("double"),
    col("Sex")
).dropna()

# 3. Разбиваем на train / test
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# 4. Создаем этапы (Stages) Пайплайна
indexer = StringIndexer(inputCol="Sex", outputCol="sex_index")
assembler = VectorAssembler(inputCols=["Pclass", "Fare", "SibSp", "sex_index"], outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label")

# 5. Собираем Пайплайн
pipeline = Pipeline(stages=[indexer, assembler, lr])

# 6. Настраиваем CrossValidator
grid = (ParamGridBuilder()
    .addGrid(lr.maxIter, [10, 50])
    .addGrid(lr.regParam, [0.1, 0.01])
    .build())

evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=grid,
    evaluator=evaluator,
    numFolds=3,
    seed=42
)

# 7. Обучаем и находим лучший Pipeline
print("Начинаем подбор гиперпараметров... Это может занять время!")
cv_model = cv.fit(train_df)

# 8. Проверяем качество и сохраняем на диск
final_roc_auc = evaluator.evaluate(cv_model.bestModel.transform(test_df))
print(f"ROC AUC лучшей модели: {final_roc_auc:.3f}")

cv_model.bestModel.write().overwrite().save("best_titanic_pipeline_model")
print("Пайплайн успешно сохранен на диск!")


# from pyspark.ml import PipelineModel
#
# # Загружаем конвейер с диска
# loaded_pipeline = PipelineModel.load("path/to/saved_pipeline")
#
# # Получаем предсказания
# new_predictions = loaded_pipeline.transform(test_df)