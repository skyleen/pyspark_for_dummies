from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("df_task_4")
         .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.1.1") # <-- подключаем avro
         .getOrCreate())

# Предварительная подготовка данных: Преобразуйте строковую колонку timestamp в формат TimestampType и назовите её event_time.
df_avro = spark.read.format("avro").load("/Users/n.shikhaleva/Downloads/activity_log.avro")
df_avro = (df_avro.withColumn("event_time", F.to_timestamp(F.col("timestamp"), "dd-LLL-yyyy HH:mm"))
                  .drop("timestamp"))
# Задачи:
# 1. Ежедневная Активность: Посчитайте общее количество событий (event_id) для каждого дня.
# Вывод: дата (date), общее количество событий (total_events). Отсортируйте по дате по возрастанию.
(df_avro.withColumn("date", F.to_date(F.col('event_time')))
        .groupBy("date")
        .agg(F.count(F.col("event_id")).alias("total_count"))
        .sort("date")
        .show())

# 2. Анализ Пользователей и Сессий: Найдите всех уникальных пользователей.
# Посчитайте общее количество уникальных сессий во всём наборе данных.
# И посчитайте количество уникальных сессий для каждого отдельного пользователя.
# Вывод 1: список идентификаторов пользователя (user_id). Отсортируйте по user_id по возрастанию.

(df_avro.select("user_id").distinct()
        .sort("user_id")
        .show())

# Вывод 2: одно число - общее количество уникальных сессий.

print("Unique session total count: ", df_avro.select("session_id").distinct().count())

# Вывод 3: идентификатор пользователя (user_id), количество уникальных сессий (unique_sessions_count).
# Отсортируйте по user_id по возрастанию.

(df_avro.select("user_id", "session_id").distinct()
        .groupBy("user_id")
        .agg(F.count(F.col("session_id")).alias("unique_sessions_count"))
        .sort("user_id")
        .show())

# 3. Детализация Сессий: Определите количество событий, произошедших в каждой уникальной пользовательской сессии.
# Вывод: идентификатор пользователя (user_id), идентификатор сессии (session_id), количество событий в сессии (count_events_in_session).
# Отсортируйте по count_events_in_session по возрастанию.

(df_avro.groupBy("user_id", "session_id")
        .agg(F.count(F.col("event_id")).alias("count_events_in_session"))
        .sort("count_events_in_session")
        .show())

# 4. Анализ Покупок: Посчитайте общее количество событий типа "purchase" (покупок).
# Найдите общую сумму всех покупок. И посчитайте среднюю сумму одной покупки, округлите результат до двух знаков после запятой.
# Вывод 1: Одно число - общее количество покупок (count_purchase_amount).

(df_avro.filter(F.col("event_type") == 'purchase')
        .agg(F.count(F.col("event_type")).alias("count_purchase_amount"))
        .show())

# Вывод 2: Одно число - общая сумма покупок (sum_purchase_amount).

(df_avro.filter(F.col("event_type") == 'purchase')
        .agg(F.sum(F.col("amount")).alias("sum_purchase_amount"))
        .show())

# Вывод 3: Одно число - средняя сумма покупки (average_purchase_amount).

(df_avro.filter(F.col("event_type") == 'purchase')
        .agg(F.round(F.avg(F.col("amount")), 2).alias("average_purchase_amount"))
        .show())

# 5. Анализ Продолжительности: Для каждой уникальной сессии (user_id, session_id) найдите самое раннее (min_time) и самое позднее (max_time) время события.
# Вычислите продолжительность сессии в секундах (session_duration_seconds).
# Также вычислите среднюю продолжительность сессии по всем сессиям и округлите результат до двух знаков после запятой.
# Вывод 1: Идентификатор пользователя (user_id), идентификатор сессии (session_id), время начала сессии (min_time),
# время окончания сессии (max_time), продолжительность сессии в секундах (session_duration_seconds).
# Отсортируйте по user_id и session_id.


(df_avro.groupBy("user_id", "session_id")
        .agg(F.min(F.col("event_time")).alias("min_time"),
             F.max(F.col("event_time")).alias("max_time"))
        .withColumn("session_duration_seconds", F.timestamp_diff('SECOND', "min_time", "max_time"))
        .sort("user_id", "session_id")
        .show())

# Вывод 2: Одно число - средняя продолжительность сессии в секундах (average_session_duration_seconds).

(df_avro.groupBy("session_id").agg(F.min(F.col("event_time")).alias("min_time"),
                                   F.max(F.col("event_time")).alias("max_time"))
        .withColumn("session_duration_seconds", F.timestamp_diff('SECOND', "min_time", "max_time"))
        .agg(F.avg(F.col("session_duration_seconds")).alias("average_session_duration_seconds"))
        .show())

spark.stop()