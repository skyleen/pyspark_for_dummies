from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkFiles

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# 1. Загрузка данных (Титаник)
spark = SparkSession.builder.appName("GBT_Example").getOrCreate()
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

# 2. Базовые трансформеры
indexer = StringIndexer(inputCol="Sex", outputCol="sex_index")
assembler = VectorAssembler(inputCols=["Pclass", "Fare", "Age", "sex_index"], outputCol="features")

# 3. Инициализация (maxIter в Бустинге — это аналог numTrees в Лесе)
gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=20, maxDepth=4, seed=42)

# 4. Интеграция в конвейер
gbt_pipeline = Pipeline(stages=[indexer, assembler, gbt])

# 5. Обучение
gbt_model = gbt_pipeline.fit(train_df)

# 6. Оценка качества
evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")
gbt_preds = gbt_model.transform(test_df)
print(f"ROC AUC Градиентного Бустинга: {evaluator.evaluate(gbt_preds):.3f}")

spark.stop()