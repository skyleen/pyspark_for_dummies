from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .master("local[*]")
         .appName('Example')
         .getOrCreate()
        )

data = [("Алиса", 30, "Москва"), ("Вася", 25, "Казань"), ("Петя", 35, "Новосибирск"), ("Маша", 32, "Санкт-Петербург"), ("Артем", 23, "Москва"), ("Виталий", 35, "Махачкала"), ("Маша", 18, "Казань")]
df_data = spark.createDataFrame(data, ["name", "age", "city"])
df_data.show()

select_df = df_data.select("name", "age")
select_df.show()

select_expr = df_data.selectExpr("upper(name) as upper_name", "age * 365 as age_in_days_expr")
select_expr.show()

new_column_df = df_data.withColumn("age_in_days", df_data.age * 365)
new_column_df.show()

multiple_columns_df = df_data.withColumns({
    "age_in_days": F.col("age") * 365,
    "is_adult": F.col("age") >= 18,
    "status": F.lit("Active") # Добавляем строковый литерал
})
multiple_columns_df.show()

filter_df = df_data.where(df_data.city == "Москва")
filter_df.show()

sort_df = df_data.sort(df_data.age.desc(), df_data.name.asc())
sort_df.show()

# Только уникальные строки
distinct_df = df_data.distinct()
print("Результат df.distinct() (уникальные строки):")
distinct_df.show()

# Удаление дубликатов только по столбцу name
drop_duplicates_df = df_data.dropDuplicates(["name"])
print("Результат df.dropDuplicates(['name']) (уникальные по 'name'):")
drop_duplicates_df.show()

# Добавляем новый df
timezone = [("Москва", 3), ("Махачкала", 3), ("Казань", 3), ("Новосибирск", 7), ("Петербург", 3)]
df_timezone = spark.createDataFrame(timezone, ["city", "timezone"])

joined_df = df_timezone.join(df_data, df_data.city == df_timezone.city, "inner")
joined_df.show()

# DataFrame 2 (имеет общие строки с df_data и новые)
data2 = [
    ("Олег", 28, "Самара"),
    ("Вася", 25, "Казань"), # Дубликат
    ("Петя", 35, "Новосибирск"), # Дубликат
    ("Елена", 40, "Москва")
]
df2 = spark.createDataFrame(data2, ["name", "age", "city"])

union_result_df = df_data.union(df2)
union_result_df.show()

# Посчитаем средний возраст по городам
agg_df = df_data.groupBy("city").agg(F.avg("age").alias("age_avg"))
agg_df.show()

pivot_by_age_city = df_data.groupBy("city").pivot("age").agg(F.count("name"))

print("Результат pivot():")
pivot_by_age_city.show()

print("Результат pivot() с заполнением NULL значений нулями:")
pivot_by_age_city_filled = pivot_by_age_city.na.fill(0)
pivot_by_age_city_filled.show()

drop_df = df_data.drop("age")
drop_df.show()

spark.stop()
