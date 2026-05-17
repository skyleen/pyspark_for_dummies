from pyspark.sql import SparkSession
from pyspark import SparkFiles
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

spark = SparkSession.builder.appName("CV_Titanic").getOrCreate()

# Скачиваем внешний датасет
url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/titanic.csv"
spark.sparkContext.addFile(url)
df_raw = spark.read.csv(SparkFiles.get("titanic.csv"), header=True, inferSchema=True)

# Выбираем важнейшие признаки для предсказания.
# Колонка survived (1 - выжил, 0 - не выжил) правильные ответ (label).
df_clean = df_raw.select(
    F.col("survived").alias("label"),  # переименовываем целевую переменную
    "pclass",  # класс билета (1, 2, 3)
    "fare",  # стоимость билета
    "sibsp",  # количество родственников на борту
    "sex"  # пол (строка: "male", "female")
).dropna()

# Переводим строковый столбец "sex" в числовой
indexer = StringIndexer(inputCol="sex", outputCol="sex_index")
df_indexed = indexer.fit(df_clean).transform(df_clean)

# Собираем все признаки в вектор
assembler = VectorAssembler(
    inputCols=["pclass", "fare", "sibsp", "sex_index"],
    outputCol="features"
)
df_features = assembler.transform(df_indexed)

# Откладываем 20% данных в тестовую выборку
train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

# Создаем базовый алгоритм
# Настройки будут подобраны автоматически с помощью сетки
lr = LogisticRegression(featuresCol="features", labelCol="label")

# Пробуем другие параметры: maxIter и regParam с другими значениями
grid = (ParamGridBuilder()
        .addGrid(lr.maxIter, [10, 50])
        .addGrid(lr.regParam, [0.1, 0.05, 0.01])
        .build())

evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")

cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=grid,
    evaluator=evaluator,
    numFolds=3,
    seed=42
)

cv_model = cv.fit(train_df)
best_lr_model = cv_model.bestModel

print(f"Лучший maxIter: {best_lr_model.getOrDefault('maxIter')}")
print(f"Лучший regParam: {best_lr_model.getOrDefault('regParam')}")

# Финальная проверка модели на тестовых данных
final_predictions = best_lr_model.transform(test_df)
final_roc_auc = evaluator.evaluate(final_predictions)

print(f"Итоговый ROC AUC на тестовых данных: {final_roc_auc:.3f}")

spark.stop()


# params list
# lr = LogisticRegression()
# print(lr.explainParams())