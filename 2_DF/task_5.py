from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("df_task_5")
         .getOrCreate())

# Предварительная подготовка данных: Приведите все числовые колонки к соответствующим типам.
# Удалите строки с отсутствующим IMDb рейтингом и жанром. Удалите дубликаты по колонкам: title, releaseYear и imdbId.

df_netflix = (spark.read
              .option("header", True)
              .option("unescapedQuoteHandling", 'STOP_AT_CLOSING_QUOTE')
              .csv("/Users/n.shikhaleva/Downloads/netflix_data.csv"))

df_netflix = (df_netflix.withColumn("releaseYear", F.col("releaseYear").cast("integer"))
                        .withColumn("imdbAverageRating", F.col("imdbAverageRating").cast("double"))
                        .withColumn("imdbNumVotes", F.col("imdbNumVotes").cast("integer"))
                        .withColumn("genres", F.split(F.col("genres"), ', '))
                        .withColumn("availableCountries", F.split(F.col("availableCountries"), ', '))
              )

df_netflix = df_netflix.dropDuplicates(subset=["title", "releaseYear","imdbId"])
df_netflix = df_netflix.dropna(subset=["imdbAverageRating", "genres"])

# Задачи:
# 1. Анализ соотношения фильмов и сериалов по жанрам и годам: Определите, как соотношение фильмов и сериалов меняется с течением времени,
# и какие жанры преобладают в каждом типе контента.
# Для каждого года выпуска посчитайте общее количество фильмов и сериалов.
# А для каждого жанра посчитайте количество фильмов и сериалов.

# Вывод 1: год выпуска (releaseYear), количество фильмов (total_movies), количество сериалов (total_series).  Выведите информацию за последние 10 лет.

df_pandas = (df_netflix.groupBy(["releaseYear", "type"])
                        .agg(F.count("*").alias("total_count"))
                        .pandas_api())
df_pandas = df_pandas.pivot(index="releaseYear", columns="type", values="total_count")
df_pandas = df_pandas.rename(columns={"tv": "total_series", "movie": "total_movies"})
df_netflix_pivot = df_pandas.to_spark("releaseYear")

df_netflix_pivot.sort("releaseYear", ascending=False).show(10)

# Вывод 2: жанр (genre), количество фильмов (movie_count), количество сериалов (tv_series_count).
# Отсортируйте по общему количеству (сумма movie_count и tv_series_count) по убыванию. Выведите топ-15 жанров.

df_netflix_genres = df_netflix.withColumn("genres", F.explode_outer(F.col("genres")))

df_pandas = (df_netflix_genres.groupBy(["genres", "type"])
                              .agg(F.count("*").alias("total_count"))
                              .pandas_api())
df_pandas = df_pandas.pivot(index="genres", columns="type", values="total_count")
df_pandas = df_pandas.rename(columns={"tv": "tv_series_count", "movie": "movie_count"})
df_netflix_pivot_genres = df_pandas.to_spark("genres")

df_netflix_pivot_genres.sort(F.col("tv_series_count") + F.col("movie_count"), ascending=False).show(15)

# 2. Популярность жанров на IMDb: Для каждого жанра посчитайте общее количество фильмов, средний рейтинг IMDb и общее количество голосов IMDb по всем фильмам в этом жанре.
# Вывод: жанр (genre), количество фильмов (title_count), средний рейтинг (average_rating), общее количество голосов (total_votes).
# Отсортируйте по среднему рейтингу по убыванию. Выведите топ-10 жанров.

(df_netflix_genres.groupBy("genres")
                  .agg(
                        F.count("title").alias("title_count"),
                        F.round(F.avg(F.col("imdbAverageRating")), 2).alias("average_rating"),
                        F.sum(F.col("imdbNumVotes")).alias("total_votes")
                 )
                 .sort("average_rating", ascending=False)
                 .show(10))

# 3. Динамика выпуска контента по годам и рейтинг: Проанализируйте, как менялось количество выпускаемого контента и его средний рейтинг IMDb по годам.
# Исключите года, для которых releaseYear некорректен или отсутствует.
# Вывод: год выпуска (releaseYear), общее количество фильмов (total_titles), средний IMDb рейтинг (average_rating).
# Выведите информацию за последние 10 лет.

(df_netflix.groupBy("releaseYear")
           .agg(F.count(F.col("title")).alias("total_titles"),
                F.round(F.avg(F.col("imdbAverageRating")),2).alias("average_rating"))
           .sort("releaseYear", ascending=False)
           .show(10))

# 4. Анализ динамики прироста фильмов по годам: Для каждого года выпуска посчитайте количество выпущенных фильмов.
# Затем рассчитайте прирост количества фильмов по сравнению с предыдущим годом.
# Определите года с наибольшим абсолютным приростом.
# Вывод: год выпуска (releaseYear), количество фильмов в текущем году (current_year_title_count),
# количество фильмов в предыдущем году (previous_year_title_count), прирост количества фильмов (title_count_growth).
# Отсортируйте по приросту количества фильмов, по убыванию. Выведите 10 первых строк.
w = Window.orderBy("releaseYear")
(df_netflix.groupBy("releaseYear")
           .agg(F.count(F.col("title")).alias("current_year_title_count"),)
           .withColumn("previous_year_title_count", F.lag(F.col("current_year_title_count"), 1).over(w).alias("previous_year_title_count"))
           .withColumn("title_count_growth", F.col("current_year_title_count") - F.col("previous_year_title_count"))
           .sort("title_count_growth", ascending=False)
           .show(10))

# 5. Поиск "Скрытых Жемчужин" (High Rating, Low Votes): Найдите фильмы с высоким средним рейтингом IMDb, но относительно небольшим количеством голосов.
# Это могут быть "скрытые жемчужины", которые известны немногим, но очень ценятся.
# Определите топ-10 фильмов с imdbAverageRating выше 8.0, у которых imdbNumVotes находится в диапазоне от 300 до 10000
# Вывод: название (title), тип (type), жанры (genres), год выпуска (releaseYear), средний рейтинг IMDb (imdbAverageRating), количество голосов IMDb (imdbNumVotes).
# Выведите 10 с наибольшим рейтингом.

(df_netflix.where((F.col("imdbAverageRating") > 8.0) & F.col("imdbNumVotes").between(300, 10000))
           .select("title", "type", "genres", "releaseYear", "imdbAverageRating", "imdbNumVotes")
           .sort("imdbAverageRating", ascending=False)
           .show(10, truncate=False))

spark.stop()