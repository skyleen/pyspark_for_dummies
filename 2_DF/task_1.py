from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimeType, MapType, ArrayType, \
    DoubleType

spark = (SparkSession.builder
         .appName("df_task_1")
         .getOrCreate())

data = [
    (101, date(2025, 1, 1), {"mobile": 3, "desktop": 1}, ["/home", "/products", "/cart"], 4.5),
    (102, date(2025, 1, 1), {"desktop": 2}, ["/home", "/about"], 3.0),
    (101, date(2025, 1, 2), {"mobile": 2}, ["/products", "/checkout"], None),
    (103, date(2025, 1, 2), {"tablet": 1, "mobile": 1}, ["/blog", "/contact"], 5.0),
    (104, date(2025, 1, 3), {"desktop": 4}, ["/dashboard"], 3.5),
    (101, date(2025, 1, 3), {"mobile": 1, "desktop": 1}, ["/home", "/products"], 4.0),
    (105, date(2025, 1, 4), {"mobile": 5}, ["/faq"], None),
    (102, date(2025, 1, 4), {"desktop": 1, "mobile": 1}, ["/settings"], 3.8),
    (103, date(2025, 1, 5), {"tablet": 2}, ["/products"], 4.2),
    (106, date(2025, 1, 5), {"desktop": 3, "mobile": 2}, ["/login", "/profile", "/home"], 4.7),
    (101, date(2025, 1, 6), {"mobile": 1}, ["/cart", "/checkout"], 4.0),
    (104, date(2025, 1, 6), {"desktop": 2, "tablet": 1}, ["/contact"], None),
    (105, date(2025, 1, 7), {"mobile": 3, "desktop": 1}, ["/pricing"], 4.1),
    (106, date(2025, 1, 7), {"desktop": 1}, ["/home", "/about"], 3.9),
    (107, date(2025, 1, 8), {"mobile": 4, "tablet": 2}, ["/products", "/blog"], 4.9)
]

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("activity_date", DateType(), True),
    StructField("session_by_device", MapType(StringType(), IntegerType()), True),
    StructField("visited_pages", ArrayType(StringType()), True),
    StructField("usability_rating", DoubleType(), True)
])

df = spark.createDataFrame(data, schema=schema)

# 2: Расчет построчных показателей
# 2.1. Рассчитайте total_sessions_count для каждой записи (строки): Создайте новую колонку total_sessions_count.
# Значение этой колонки должно быть суммой количества сессий по всем устройствам (mobile, desktop, tablet) для каждой записи.

df_device = (df.select(F.col("user_id"),
                      F.col('activity_date'),
                      F.explode(F.col("session_by_device")).alias("device", "session_count"))
             .groupBy("user_id", 'activity_date')
             .agg(F.sum(F.col("session_count")).alias("total_session_count")))

df = df.join(df_device, ['user_id', 'activity_date'], 'inner')



# 2.2. Извлеките количество мобильных сессий: Создайте новую колонку mobile_sessions,
# которая будет содержать количество сессий, проведенных с "mobile" устройства из sessions_by_device.
# Если данных о "mobile" устройстве нет в данных для данной записи, значение должно быть 0.

df = df.withColumn("mobile_sessions", F.coalesce(F.col("session_by_device")['mobile'],F.lit(0)))

# 3: Агрегация данных по пользователям
# 3.1. Для каждого пользователя рассчитайте total_sessions_all_time: Это общее количество всех сессий пользователя за весь период наблюдений.
# Отсортируйте результат по total_sessions_all_time в порядке убывания.

df_users = (df.groupby('user_id')
            .agg(F.sum(F.col("total_session_count")).alias("total_session_all_time"))
            .sort("total_session_all_time", ascending=False))

# 3.2. Для каждого пользователя получите unique_visited_pages_all_time: Это список всех уникальных посещенных страниц пользователя за весь период.

df_users_pages = (df.select(F.col('user_id'), F.explode_outer(F.col('visited_pages')).alias("page"))
                    .groupBy('user_id')
                    .agg(F.collect_set('page').alias('unique_visited_pages_all_time'))
                    .sort(F.col('user_id')))

# 4: Фильтрация данных
# 4.1. Отфильтруйте DataFrame df_user_activity, чтобы показать только те записи, где usability_rating выше 3.5.
# Отсортируйте результат по usability_rating в порядке убывания.

df_filter = df.filter(F.col("usability_rating") > 3.5).sort("usability_rating", ascending = False)

# Results
df.printSchema()
df.show(truncate=False)
df_users.show(truncate=False)
df_users_pages.show(truncate=False)
df_filter.show(truncate=False)

spark.stop()