from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, when, broadcast, lit, col, count, sum, array, explode
import time

# Инициализация сеанса Spark
spark = SparkSession.builder.appName("Skew").master("local[*]").getOrCreate()
# aqe
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Функция для подсчета затарченного времени на действие
def measure_time(query):
    start = time.time()
    query.count()
    end = time.time()
    print(f"Затраченное время: {end - start:.2f} sec.")


# large_df: данные распределены равномерно
large_df = spark.range(0, 8_000_000).withColumn("value", rand())

# skewed_df: 50% из записей имеют id=0
skewed_df = (spark.range(0, 3_000_000)
             .withColumn("value", rand())
             .withColumn("id", when(col("id") % 2 == 0, lit(0))
                         .when(col("id") % 3 == 1, lit(1))
                         .otherwise(col("id")))
             )

# Перераспределение large_df
# Так как large_df равномерно распределен, после repartition партиции будут равномерными
large_df_repartitioned = large_df.repartition(5, "id")
# num_partitions_large = large_df_repartitioned.rdd.getNumPartitions()
# print(f"Количество партиций (large_df_repartitioned): {num_partitions_large}")
# partition_sizes_large = large_df_repartitioned.rdd.glom().map(len).collect()
# print(f"Размер партиций (large_df_repartitioned): {partition_sizes_large}")

# Перераспределение skewed_df
# Здесь мы увидим перекос, так как id=0 будет концентрироваться в одной партиции
skewed_df_repartitioned = skewed_df.repartition(5, "id")
# num_partitions_skewed = skewed_df_repartitioned.rdd.getNumPartitions()
# print(f"Количество партиций (skewed_df_repartitioned): {num_partitions_skewed}")
# partition_sizes_skewed = skewed_df_repartitioned.rdd.glom().map(len).collect()
# print(f"Размер партиций (skewed_df_repartitioned): {partition_sizes_skewed}")
#
# skewed_df.groupBy(col("id")).agg(count("*").alias("count")).orderBy("id").show(10)

print("join по-умолчанию:")
df_joined = skewed_df.join(large_df, "id", 'inner')
measure_time(df_joined)
#
# Метод с Broadcast Join
print("Соединение с broadcast JOIN:")
broadcast_join_df = large_df.join(broadcast(skewed_df), "id")
measure_time(broadcast_join_df)
#
# Метод изоляции перекошенных значений

# Значение перекоса
skewed_value = [0, 1]
# Отберем значения с перекошенным ключом
large_skewed_df = large_df.filter(col("id").isin(skewed_value))
skewed_only_skew_df = skewed_df.filter(col("id").isin(skewed_value))
# Отфильруем остальные значения
large_non_skew_df = large_df.filter(~col("id").isin(skewed_value))
skewed_non_skew_df = skewed_df.filter(~col("id").isin(skewed_value))

# Соединяем не перекошенные данные
non_skewed_join_df = large_non_skew_df.join(skewed_non_skew_df, "id")

# Соединяем перекошенные данные с broadcast
skewed_join_df = large_skewed_df.join(broadcast(skewed_only_skew_df), "id")

# Объединяем результаты
final_join_df = non_skewed_join_df.union(skewed_join_df)
print("Соединение с изоляцией перекошенных ключей:")
measure_time(final_join_df)
#
# Метод соления ключей

# Значение перекоса
num_salts = 5
skewed_values = [0, 1]

# Добавление соли только к перекошенным ключам
salted_skewed_df = skewed_df.withColumn(
    "salt",
    when(col("id").isin(skewed_values), (rand() * num_salts).cast("int"))
    .otherwise(lit(0))
)

# В большом DataFrame создаём все возможные значения соли и дублируем записи
df_uniform = (
    large_df.withColumn(
        "salt_values",
        when(col("id").isin(skewed_values), array([lit(i) for i in range(num_salts)]))
        .otherwise(array(lit(0)))
    )
    .withColumn("salt", explode(col("salt_values")))
    .drop("salt_values")
)

# Выполнение Join по основному ключу и соли
df_joined_salt = salted_skewed_df.join(df_uniform, ["id", "salt"], "inner")
df_joined_salt.orderBy("id").show()

# Группировка с солью и последующая агрегация
print("Группировка с соленым ключом")
group_salt = (
    skewed_df.withColumn(
        "salt",
        when(col("id").isin(skewed_values), (rand() * num_salts).cast("int"))
        .otherwise(lit(0))  # Неперекошенные ключи получают соль 0
    )
    .groupBy("id", "salt")  # Первая группировка по id и соли
    .agg(count("value").alias("count"))
    .groupBy("id")  # Удаляем соль, агрегируя по id
    .agg(sum(col("count")).alias("count"))  # Суммируем частичные подсчеты
    .orderBy("id")
)

group_salt.show(10)

# Завершение работы Spark
spark.stop()
