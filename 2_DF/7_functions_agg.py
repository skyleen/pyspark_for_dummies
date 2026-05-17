from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("AggregationFunctionsExample") \
    .getOrCreate()

# Исходные данные
sales_data = [
    ("Север", "Обувь", 100.0, 5, datetime.date(2025, 3, 10)),
    ("Север", "Одежда", 150.0, 7, datetime.date(2025, 3, 15)),
    ("Север", "Обувь", 120.0, 6, datetime.date(2025, 4, 10)),
    ("Север", "Обувь", None, 3, datetime.date(2025, 5, 1)),
    ("Север", "Одежда", 200.0, 8, datetime.date(2025, 5, 15)),
    ("Юг", "Электроника", 800.0, 1, datetime.date(2025, 3, 20)),
    ("Юг", "Одежда", 110.0, 5, datetime.date(2025, 4, 1)),
    ("Юг", "Электроника", 900.0, None, datetime.date(2025, 5, 5)),
    ("Восток", "Обувь", 250.0, 10, datetime.date(2025, 3, 5)),
    ("Восток", "Электроника", 1800.0, 2, datetime.date(2025, 4, 12)),
    ("Восток", "Одежда", 300.0, 3, datetime.date(2025, 4, 20)),
    ("Восток", "Обувь", 150.0, 4, datetime.date(2025, 5, 10)),

]

# Определение схемы
sales_schema = StructType([
    StructField("region", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("units_sold", IntegerType(), True),
    StructField("sale_date", DateType(), True)
])
df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_sales.show(truncate=False)

df_sum_count = df_sales.groupBy("region", "category").agg(
    F.sum("amount").alias("total_amount"),
    F.sum("units_sold").alias("total_units_sold"),
    F.count("amount").alias("count_transactions_with_amount"), # Количество транзакций с указанной суммой
    F.count("units_sold").alias("count_transactions_with_units"), # Количество транзакций с указанным количеством единиц
    F.countDistinct("sale_date").alias("distinct_sale_days")
)
df_sum_count.orderBy("region", "category").show(truncate=False)

df_avg_min_max = df_sales.groupBy("region").agg(
    F.avg("amount").alias("average_amount"),
    F.min("amount").alias("min_amount"),
    F.max("amount").alias("max_amount"),
    F.avg("units_sold").alias("average_units_sold")
)
df_avg_min_max.orderBy("region").show(truncate=False)

df_stats = df_sales.groupBy("category").agg(
    F.round(F.variance("amount"),2).alias("amount_variance"), # Используем дополнительно округление
    F.round(F.stddev("amount"),2).alias("amount_stddev")
)
df_stats.orderBy("category").show(truncate=False)

df_collections = df_sales.groupBy("region").agg(
    F.collect_list("category").alias("all_categories_list"),
    F.collect_set("sale_date").alias("unique_sale_dates_set")
)
df_collections.orderBy("region").show(truncate=False)
df_collections.printSchema()

df_global_aggregates = df_sales.agg(
    F.sum("amount").alias("total_amount"),
    F.round(F.avg("amount"),2).alias("average_amount"), # Дополнительно округляем
    F.count("*").alias("total_records")
)
df_global_aggregates.show(truncate=False)

spark.stop()