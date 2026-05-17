from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Функция для замены невалидных значений
def replace_invalid(column):
    return F.when(F.col(column).isin(["UNKNOWN", "ERROR", "NULL"]), None).otherwise(F.col(column))

spark = SparkSession.builder \
.appName("df_project") \
.getOrCreate()

df = spark.read.option("header", True).csv("/Users/n.shikhaleva/Downloads/dirty_cafe_sales.csv")

# df.printSchema()
# df.show()
# df.summary().show()

# Создаем список новых имен
new_columns = [c.replace(" ", "_").lower() for c in df.columns]

# Применяем их к DataFrame
df = df.toDF(*new_columns)

# print("Новые названия колонок:")
# print(df.columns)
#
# total_rows = df.count()
# print(f"Общее количество строк: {total_rows}")
#
# unique_rows = df.distinct().count()
# print(f"Количество уникальных строк: {unique_rows}")
#
# print("Проверка на дубликаты по 'transaction_id':")
# duplicate_counts_df = df.groupBy("transaction_id").count().filter(F.col("count") > 1).show()

# Применение преобразований к существующим колонкам и приводим их к корректным типам данных
df = df.withColumn('item', replace_invalid('item'))
df = df.withColumn('quantity', replace_invalid('quantity').cast("integer"))
df = df.withColumn('price_per_unit', replace_invalid('price_per_unit').cast("double"))
df = df.withColumn('total_spent', replace_invalid('total_spent').cast("double"))
df = df.withColumn('payment_method', replace_invalid('payment_method'))
df = df.withColumn('location', replace_invalid('location'))
df = df.withColumn('transaction_date', replace_invalid('transaction_date').cast("date"))

# print("Схема DataFrame после очистки и приведения типов:")
# df.printSchema()

item_price=(df.dropna(subset=["item", "price_per_unit"])
            .select("item", F.col("price_per_unit").alias("known_price_for_item"))
            .distinct())

df_joined = df.join(item_price, on="item", how="left_outer")

df = (df_joined.withColumn("price_per_unit",F.coalesce(F.col("price_per_unit"), F.col("known_price_for_item")))
               .drop("known_price_for_item")) # Удаляем вспомогательную колонку

df = df.withColumn("total_spent", F.when(F.col("total_spent").isNull(), F.col("quantity") * F.col("price_per_unit")).otherwise(F.col("total_spent")))

df = df.withColumn("quantity", F.when(F.col("quantity").isNull(), F.col("total_spent") / F.col("price_per_unit")).otherwise(F.col("quantity")))

# print("Статистика после частичного восстановления данных")
# df.summary().show()

df = df.dropna(subset=["item", "quantity", "total_spent"])

# print("Статистика после удаления строк с NULL:")
# df.summary().show()

df = df.withColumn("month", F.month(F.col("transaction_date")).cast("int"))

month_stat = df.groupby("month").agg(F.count("transaction_id").alias("count_transaction_id")).sort("count_transaction_id", ascending=False)
print("Statistics by month:")
month_stat.show()

total_spent_by_item = df.groupby("item").agg(F.sum("total_spent").alias("total_spent_by_item")).sort("total_spent_by_item", ascending=False)
print("Total spent by item:")
total_spent_by_item.show()

month_stat_by_item = df.groupby("month","item").agg(F.sum("quantity").alias("total_count_by_item")).sort(F.col("month").asc(), F.col("total_count_by_item").desc())
print("Most popular item by month:")
month_stat_by_item.show()

avg_tran = df.agg(F.round(F.avg("total_spent"), 2).alias("avg_spent"))
print("Avg transaction:")
avg_tran.show()

filter_df = df.where((df["location"] == "In-store") & (df["payment_method"] == "Credit Card")).count()
print(f"Payed by Credit Card in store: {filter_df}")


spark.stop()
