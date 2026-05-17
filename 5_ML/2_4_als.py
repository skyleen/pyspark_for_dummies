# Alternating Least Squares - Метод чередующихся наименьших квадратов

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("ALS_Example").getOrCreate()

# Данные: ID_пользователя, ID_фильма, Оценка
# Для примера: 101 - Фантастика, 102 - Комедия, 103 - Мелодрама, 104 - Детектив
ratings_data = [
    (1, 101, 5.0), (1, 102, 4.0), (1, 103, 1.0),   # Пользователь 1 любит фантастику и комедию, не любит мелодрамы
    (2, 101, 1.0), (2, 103, 5.0), (2, 104, 4.0),   # Пользователь 2 не любит фантастику, любит мелодрамы и детективы
    (3, 101, 5.0), (3, 102, 5.0), (3, 104, 1.0),   # Пользователь 3 любит фантастику и комедии, не любит детективы
    (4, 101, 5.0), (4, 102, 5.0)                   # Пользователь 4 любит фантастику и комедию
]
df_ratings = spark.createDataFrame(ratings_data, ["userId", "movieId", "rating"])
# Разбиваем данные для честной оценки алгоритма
train_df, test_df = df_ratings.randomSplit([0.8, 0.2], seed=42)

# Инициализируем ALS
als = ALS(
    maxIter=10,              # Количество итераций обучения
    userCol="userId",        # Колонка пользователей
    itemCol="movieId",       # Колонка фильмов
    ratingCol="rating",      # Колонка с оценками
    coldStartStrategy="drop"
)

# Обучаем модель
als_model = als.fit(train_df)

# Генерируем топ-5 рекомендаций
all_recommendations = als_model.recommendForAllUsers(5)

# Фильтрация фильмов, которые пользователь УЖЕ смотрел
# Разворачиваем массив с рекомендациями в обычные строки
exploded_recs = all_recommendations.select(
    "userId",
    F.explode("recommendations").alias("rec")
).select(
    "userId",
    F.col("rec.movieId").alias("movieId"),
    F.col("rec.rating").alias("prediction")
)

# Вычитаем те фильмы, которые пользователь УЖЕ смотрел через left_anti join
clean_recs = exploded_recs.join(df_ratings, on=["userId", "movieId"], how="left_anti")

# Итоговые рекомендации
clean_recs.orderBy("userId", F.col("prediction").desc()).show()


# Делаем предсказания на тестовой выборке
predictions = als_model.transform(test_df)

# Оцениваем RMSE (насколько сильно предсказанный рейтинг отклоняется от реального)
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)
rmse = evaluator.evaluate(predictions)
print(f"Ошибка логики рекомендаций (RMSE): {rmse:.3f}")

spark.stop()