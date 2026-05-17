import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


df_books_schema = StructType([
    StructField("ISBN", StringType(), False),
    StructField("Book-Title", StringType(), True),
    StructField("Book-Author", StringType(), True),
    StructField("Year-of-Publication", IntegerType(), True),
    StructField("Publisher", StringType(), True),
    StructField("Image-URL-S", StringType(), True),
    StructField("Image-URL-M", StringType(), True),
    StructField("Image-URL-L", StringType(), True)
])

df_ratings_schema = StructType([
    StructField("User-Id", IntegerType(), False),
    StructField("ISBN", StringType(), False),
    StructField("Book-Rating", IntegerType(), True)
])

df_users_schema = StructType([
    StructField("User-Id", IntegerType(), False),
    StructField("Location", StringType(), True),
    StructField("Age", IntegerType(), True)
])



def get_new_columns(df):
    return [c.replace("-", "_").lower() for c in df.columns]


# Приложение 1: Неоптимизированное
spark = (SparkSession
         .builder
         .appName("optimization_final_1")
         .getOrCreate())

# 1: Подготовка данных
df_books_raw = spark.read.csv("/Users/n.shikhaleva/Downloads/оптимизация-итоговое/books.csv",
                              header=True, sep=";", schema=df_books_schema)
df_books = df_books_raw.toDF(*get_new_columns(df_books_raw))

df_ratings_raw = spark.read.csv("/Users/n.shikhaleva/Downloads/оптимизация-итоговое/ratings.csv",
                              header=True, sep=";", schema=df_ratings_schema)
df_ratings = df_ratings_raw.toDF(*get_new_columns(df_ratings_raw))

df_users_raw = spark.read.csv("/Users/n.shikhaleva/Downloads/оптимизация-итоговое/users.csv",
                              header=True, sep=";", schema=df_users_schema)
df_users = df_users_raw.toDF(*get_new_columns(df_users_raw))

print("*"*20, "APPLICATION 1. NOT OPTIMIZED","*"*20)
start_time = time.perf_counter()

## 2.1: Топ-10 книг по среднему рейтингу
df_avg_book_rate = ((df_ratings
                       .where(~(F.col("book_rating") == 0))
                       .groupBy("isbn")
                       .agg(F.round(F.avg("book_rating"),2).alias("avg_rating"),
                            F.count("book_rating").alias("rating_count"))
                       .where(F.col("rating_count") > 300))
               .join(df_books, "isbn")
               .select("book_title", "book_author", "year_of_publication","avg_rating")
               .sort("avg_rating", ascending=False)
               .limit(10))
print("Top-10 books by avg rating:")
df_avg_book_rate.show(truncate=False)

## 2.2: Извлечение страны и объединение таблиц
df_users_rates = ((df_users.withColumn("country",
                                       F.trim(F.regexp_extract(F.col("location"), r'^([^,]+),\s*([^,]+),\s*([^,]+)$', 3))))
                   .join(df_ratings, "user_id"))

# 2.3: Топ-5 стран по среднему рейтингу
df_avg_ctr_rate = (df_users_rates
                       .where(~(F.col("country") == '') & ~(F.col("book_rating") == 0))
                       .groupBy("country")
                       .agg(F.round(F.avg("book_rating"),2).alias("avg_country_rating"),
                            F.count("book_rating").alias("rating_count"))
                       .sort("rating_count", ascending=False)
                       .limit(5))
print("Top-10 countries by avg rating:")
df_avg_ctr_rate.show()

end_time = time.perf_counter()
print(f"Total time: {(end_time - start_time):.2f} seconds.\n")

spark.stop()

# Приложение 2: Оптимизированное
spark = (SparkSession
         .builder
         .appName("optimization_final_2")
         .getOrCreate())


# 1: Подготовка данных
df_books_raw = spark.read.csv("/Users/n.shikhaleva/Downloads/оптимизация-итоговое/books.csv",
                              header=True, sep=";", schema=df_books_schema)
df_books = df_books_raw.toDF(*get_new_columns(df_books_raw))
df_books.cache()
df_books.count()

df_ratings_raw = spark.read.csv("/Users/n.shikhaleva/Downloads/оптимизация-итоговое/ratings.csv",
                              header=True, sep=";", schema=df_ratings_schema).where(~(F.col("Book-Rating") == 0))
df_ratings = df_ratings_raw.toDF(*get_new_columns(df_ratings_raw))
df_ratings.cache()
df_ratings.count()

df_users_raw = spark.read.csv("/Users/n.shikhaleva/Downloads/оптимизация-итоговое/users.csv",
                              header=True, sep=";", schema=df_users_schema)
df_users = df_users_raw.toDF(*get_new_columns(df_users_raw))
df_users.cache()
df_users.count()

print("*"*20, "APPLICATION 2. OPTIMIZED","*"*20)
start_time = time.perf_counter()
## 2.1: Топ-10 книг по среднему рейтингу
df_avg_book_rate = (df_ratings
                       .groupBy("isbn")
                       .agg(F.round(F.avg("book_rating"),2).alias("avg_rating"),
                            F.count("book_rating").alias("rating_count"))
                       .where(F.col("rating_count") > 300))

print("Top-10 books by avg rating:")
(df_books.join(df_avg_book_rate, "isbn")
         .select("book_title", "book_author", "year_of_publication","avg_rating")
         .sort("avg_rating", ascending=False)
         .limit(10)
         .show(truncate=False))

## 2.2: Извлечение страны и объединение таблиц
df_users_w_countries = (df_users.withColumn("country",F.trim(F.regexp_extract(F.col("location"),
                                                      r'^([^,]+),\s*([^,]+),\s*([^,]+)$', 3)))
                                .where(~(F.col("country") == '')))

df_users_rates = (df_ratings
                  .join(df_users_w_countries, "user_id")
                  .select("country", "book_rating"))

# 2.3: Топ-5 стран по среднему рейтингу
df_avg_ctr_rate = (df_users_rates
                       .groupBy("country")
                       .agg(F.round(F.avg("book_rating"),2).alias("avg_country_rating"),
                            F.count("book_rating").alias("rating_count"))
                       .sort("rating_count", ascending=False)
                       .limit(5))
print("Top-10 countries by avg rating:")
df_avg_ctr_rate.show()

df_users.unpersist()
df_books.unpersist()
df_ratings.unpersist()

end_time = time.perf_counter()
print(f"Total time: {(end_time - start_time):.2f} seconds.")

spark.stop()
