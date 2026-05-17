from pyspark import SparkFiles
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = (SparkSession
         .builder
         .appName("ml_final_task")
         .getOrCreate())

url = "https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv"
spark.sparkContext.addFile(url)

# 1. Загрузка и базовая очистка
# Считайте датасет из SparkFiles.get("Telco-Customer-Churn.csv") с указанием наличия заголовков (header=True).
# Вопрос автоматического вывода типов (inferSchema=True) оставьте на свое усмотрение или укажите схему вручную.
# Колонка TotalCharges по умолчанию загружается как строка (StringType). Приведите ее к типу DoubleType.
# Удалите строки с пропущенными значениями (возникают после приведения типов).
# Преобразуйте текстовую целевую переменную Churn в числовой признак label (1.0 для "Yes", 0.0 для "No"). Исходную колонку убрать.
# Разбейте полностью очищенный датафрейм на тренировочную и тестовую выборки. Оптимальную пропорцию разбиения выберите самостоятельно. Установите seed=42.
df_raw = spark.read.csv(SparkFiles.get("Telco-Customer-Churn.csv"), header=True, inferSchema=True)
df_clean = (df_raw
            .withColumns({"TotalCharges": F.col("TotalCharges").try_cast(DoubleType()),
                          "label": F.when(F.col("Churn") == "Yes", F.lit(1)).otherwise(F.lit(0)).cast(IntegerType())})
            .drop("Churn")
            .dropna(subset=["TotalCharges"])
            )

# df_clean.show(10)
# df_clean.printSchema()
train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)

# 2. Обработка признаков (Feature Engineering)
# Закодируйте все текстовые категориальные колонки (кроме customerID) с помощью StringIndexer с параметром handleInvalid="keep".
# Затем соберите их вместе с числовыми колонками в единый вектор features с использованием VectorAssembler.
# Инициализируйте алгоритм Градиентного Бустинга или Случайного Леса на ваш выбор.
cols_to_index = ','.join(c for c,t in df_clean.dtypes if (c != "customerID") & (t == "string")).split(",")

indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") for col in cols_to_index]
assembler = VectorAssembler(inputCols=[f"{col}_idx" for col in cols_to_index], outputCol="features")
forest = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)

# 3. Обучение классификатора внутри Pipeline через Cross-Validation
# Соберите все этапы преобразования из пункта 2 и ваш алгоритм в единый объект Pipeline.
# Вместо простого обучения Pipeline, используйте ParamGridBuilder для создания сетки гиперпараметров выбранного алгоритма (например, сетка параметров maxDepth или numTrees).
# Передайте подготовленный Pipeline и сетку параметров в CrossValidator. Обучите созданный валидатор на тренировочных данных.
stages = indexers + [assembler, forest]
pipeline = Pipeline(stages=stages)

grid = (ParamGridBuilder()
        .addGrid(forest.maxDepth, [3, 5, 7])
        .addGrid(forest.numTrees, [5, 10, 15])
        .build())
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

cv = CrossValidator(estimator=pipeline,
                    estimatorParamMaps=grid,
                    evaluator=evaluator,
                    numFolds=5,
                    seed=42)

print("Starting models (3*3*5=45) training... It might take time.")
model = cv.fit(train_df)

# 4. Итоговая оценка (Evaluation)
# Назначьте лучшую модель, найденную кросс-валидатором (атрибут bestModel), и выполните ей предсказание (метод transform) на тестовом датафрейме.
# При помощи BinaryClassificationEvaluator измерьте метрику ROC AUC на тесте.
# Выведите метрику в консоль. Обязательно выведите на экран лучшие гиперпараметры, которые нашел валидатор.
# А также покажите 10 строк финального предсказания (колонки: customerID, probability, prediction, label).
best_model = model.bestModel
predictions = best_model.transform(test_df)
score = evaluator.evaluate(predictions)

print(f"Best model ROC AUC score: {score:.3f}")
print(f"Best model predictions: ")
(predictions
    .select("customerID", "probability", "prediction", "label")
    .show(10, truncate=False))

spark.stop()
