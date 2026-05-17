from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("LogReg_Example").getOrCreate()

# Формат: (пол, возраст, время_на_сайте, кликнул_ли)
data = [
    ("M", 25, 12.5, 0.0), ("F", 35, 19.3, 1.0),
    ("M", 22, 9.1,  0.0), ("F", 42, 25.5, 1.0),
    ("M", 50, 20.0, 1.0), ("F", 28, 10.5, 0.0),
    ("M", 30, 22.0, 1.0), ("F", 21, 5.5,  0.0),
    ("F", 38, 18.0, 1.0), ("M", 45, 15.0, 0.0)
]

columns = ["gender", "age", "time_on_site", "label"]
df = spark.createDataFrame(data, columns)

# Индексируем пол (превращаем строки M/F в числа 0.0/1.0)
indexer = StringIndexer(inputCol="gender", outputCol="gender_indexed")
indexer_model = indexer.fit(df)
df_indexed = indexer_model.transform(df)

# Собираем признаки в единый вектор
assembler = VectorAssembler(inputCols=["gender_indexed", "age", "time_on_site"], outputCol="features")
df_features = assembler.transform(df_indexed)

# Разбиваем данные (70%/30%)
train_df, test_df = df_features.randomSplit([0.7, 0.3], seed=42)

# Обучаем логистическую регрессию только на train_df
lr = LogisticRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(train_df)

# Делаем предсказания на тестовых данных
predictions = lr_model.transform(test_df)

# Посмотрим, что модель добавила в наш датафрейм:
predictions.select("label", "probability", "prediction").show(truncate=False)

evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)
roc_auc = evaluator.evaluate(predictions)
print(f"Метрика ROC AUC: {roc_auc:.3f}")

spark.stop()