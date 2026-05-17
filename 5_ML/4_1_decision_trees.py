from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkFiles

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier # Новый импорт!
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 1. Загрузка данных (Титаник) - Точно так же, как в Уроке 5
spark = SparkSession.builder.appName("Trees").getOrCreate()
url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
spark.sparkContext.addFile(url)

df = spark.read.csv(SparkFiles.get("titanic.csv"), header=True, inferSchema=True)
df = df.select(
    col("Survived").alias("label"),
    col("Pclass").cast("double"),
    col("Fare").cast("double"),
    col("Age").cast("double"),     # Добавим возраст в признаки
    col("Sex")
).dropna()

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# 2. Этапы конвейера
indexer = StringIndexer(inputCol="Sex", outputCol="sex_index")
assembler = VectorAssembler(inputCols=["Pclass", "Fare", "Age", "sex_index"], outputCol="features")

# 3. Базовый алгоритм: Дерево Решений (Ограничим глубину дерева 4 уровнями)
dt = DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=4)

# 4. Собираем и учим Pipeline
pipeline = Pipeline(stages=[indexer, assembler, dt])
dt_model = pipeline.fit(train_df)

# 5. Оценка качества
evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")
predictions = dt_model.transform(test_df)
print(f"ROC AUC Дерева решений: {evaluator.evaluate(predictions):.3f}")

# 6. Извлекаем обученное дерево из пайплайна (оно лежит под индексом 2)
# и выводим на экран его логику
tree_model = dt_model.stages[2]
print(tree_model.toDebugString)

spark.stop()