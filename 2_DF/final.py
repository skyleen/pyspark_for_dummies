from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("df_final_task")
         .getOrCreate())

# 1. Загрузка и предварительная обработка данных
# 1.1. Загрузка и вывод схемы: Загрузите файл retail_store_sales.csv.
# Выведите первые 5 строк загруженного DataFrame и его схему (df.printSchema()).

df_raw = spark.read.csv("/Users/n.shikhaleva/Downloads/retail_store_sales.csv", header=True)
print("Original dataset:")
df_raw.show(5)
df_raw.printSchema()

# 1.2. Очистка названий столбцов: Преобразуйте названия всех столбцов к единому регистру - snake_case.
# Выведите обновленную схему DataFrame или названия столбцов, чтобы убедиться в изменении названий.

new_cols = [c.replace(" ", "_").lower() for c in df_raw.columns]
df_new_cols = df_raw.toDF(*new_cols)

# 1.3. Преобразование типов данных: Проанализируйте к каким типам данных относятся данные в столбцах
# и приведите столбец к соответствующему типу.

df_data = df_new_cols.withColumns({
                            "transaction_id": F.col("transaction_id").cast("string"),
                            "customer_id": F.col("customer_id").cast("string"),
                            "category": F.col("category").cast("string"),
                            "item": F.col("item").cast("string"),
                            "price_per_unit": F.col("price_per_unit").cast("double"),
                            "quantity": F.regexp_replace(F.col("quantity"), '[(.0)]', '').cast("integer"),
                            "total_spent": F.col("total_spent").cast("double"),
                            "payment_method": F.col("payment_method").cast("string"),
                            "location": F.col("location").cast("string"),
                            "transaction_date": F.col("transaction_date").cast("date"),
                            "discount_applied": F.col("discount_applied").cast("boolean")
                           })
print("Dataset with adjusted columns and data types:")
df_data.show(5)
df_data.printSchema()

# 2. Очистка и валидация данных
# Для наглядной оценки результата и отслеживания прогресса в процессе очистки данных
# рекомендуется после каждого основного этапа восстановления выводить количество пропусков в изменённых колонках.
# Это поможет оценить эффективность каждого шага. Также можете показывать пример из 20 строк, где видно,
# как данные были восстановлены. Для этого можно использовать методы .show() или .limit().

print("DF stats before cleaning:")
df_data.summary().show()

# 2.1 Заполнение отсутствующие Price Per Unit
# Если отсутствует цена за единицу товара, но общая сумма и количество имеются, вычислите цену за единицу
# и заполните пропущенные значения. Округлите до двух знаков после запятой.
df_data = df_data.withColumn("price_per_unit",
                                        F.when(F.col("price_per_unit").isNotNull(), F.col("price_per_unit"))
                                         .when(F.col("total_spent").isNotNull() & F.col("quantity").isNotNull(),
                                               F.round((F.col("total_spent") / F.col("quantity")),2))
                                         .otherwise(None))

# 2.2. Восстановление отсутствующих Item:
# Так как данные статические для каждого товара, то составьте справочник товаров в отдельный DataFrame
# с Category, Item и Price Per Unit.
# Для транзакций, где отсутствует название товара, но имеется категория и цена,
# попытайтесь определить название товара, путём объединения (join) с загруженным справочником товаров.
df_dict_products = (df_data.where(F.col("item").isNotNull())
                           .selectExpr("category", "item as item_dict", "price_per_unit").distinct()
                           .sort("category", "item_dict"))

df_data = (df_data.join(df_dict_products, on=["category", "price_per_unit"], how="left")
                  .withColumn("item", F.coalesce(F.col("item"), F.col("item_dict")))
                  .drop("item_dict"))

# 2.3. Заполнение отсутствующих Quantity и Total Spent:
# Проверьте есть ли транзакции, с пропусками в Total Spent, но с данными в Quantity и Price Per Unit,
# если такие данные есть, то восстановите их.
# Аналогично проверьте Quantity, если значения отсутствует, но имеются сумма транзакции и цена за товар,
# вычислите количество проданного товара и заполните пропущенные значения. Результат приведите к целому числу.

df_data = (df_data.withColumn("total_spent",
                                        F.when(F.col("total_spent").isNotNull(), F.col("total_spent"))
                                         .when(F.col("quantity").isNotNull() & F.col("price_per_unit").isNotNull(),
                                               F.round((F.col("quantity") * F.col("price_per_unit")),2))
                                         .otherwise(None))
                  .withColumn("quantity",
                                       F.when(F.col("quantity").isNotNull(), F.col("quantity"))
                                       .when(F.col("total_spent").isNotNull() & F.col("price_per_unit").isNotNull(),
                                             (F.col("total_spent") / F.col("price_per_unit")).cast("integer"))
                                       .otherwise(None))
           )

# 2.4. Удалите оставшийся строки с пропусками в Category, Quantity ,Total Spent и Price Per Unit

df_data = df_data.dropna(subset=["category", "quantity", "total_spent", "price_per_unit"])

print("DF stats after cleaning:")
df_data.summary().show()

# 3. Разведочный анализ данных
# 3.1. Самые популярные категории товаров: Рассчитайте общее количество проданных единиц товара для каждой категории.
# Определите Топ-5 категорий по общему количеству проданных единиц.

print("===================DATA ANALYSIS===================")

print("Top-5 product categories:")
(df_data.groupBy("category")
        .agg(F.sum(F.col("quantity")).alias("total_count"))
        .sort("total_count", ascending=False)
        .show(5, truncate=False))

# 3.2. Анализ среднего чека:
# Рассчитайте среднее значение Total Spent для каждого метода оплаты. Округлите до двух знаков после запятой.

print(f"Avg spent per payment method:")
(df_data.groupBy("payment_method")
        .agg(F.round(F.avg(F.col("total_spent")), 2).alias("avg_spent"))
        .sort("avg_spent", ascending=False)
        .show(truncate=False))

# Рассчитайте среднее значение Total Spent для каждой места, где прошла оплата. Округлите до двух знаков после запятой.

print(f"Avg spent per location:")
(df_data.groupBy("location")
        .agg(F.round(F.avg(F.col("total_spent")), 2).alias("avg_spent"))
        .sort("avg_spent", ascending=False)
        .show(truncate=False))

# 4. Генерация признаков
# 4.1. Временные признаки: Добавьте два новых столбца на основе Transaction Date:
# day_of_week: День недели
# transaction_month: Месяц транзакции

df_data = (df_data.withColumn("day_of_week", F.dayname(F.col("transaction_date")))
                  .withColumn("day_of_week_order", F.weekday(F.col("transaction_date")))
                  .withColumn("transaction_month", F.monthname(F.col("transaction_date")))
                  .withColumn("transaction_month_order", F.month(F.col("transaction_date"))))

# 4.2. Продажи по дням недели: Рассчитайте среднюю сумму продаж (Total Spent) для каждого дня недели.
# Выведите результаты, отсортированные по дням недели.
print(f"Avg spent per day of week:")
(df_data.groupBy("day_of_week", "day_of_week_order")
        .agg(F.round(F.avg(F.col("total_spent")),2).alias("avg_spent"))
        .sort(F.col("day_of_week_order"))
        .drop("day_of_week_order")
        .show())

# 4.3. Продажи по месяцам: Рассчитайте среднюю сумму продаж (Total Spent) для каждого месяца.
# Выведите результаты, отсортированные по месяцам.
print(f"Avg spent per month:")
(df_data.groupBy("transaction_month", "transaction_month_order")
        .agg(F.round(F.avg(F.col("total_spent")),2).alias("avg_spent"))
        .sort("transaction_month_order")
        .drop("transaction_month_order")
        .show())

# 4.4. Признаки клиента: Рассчитайте customer_lifetime_value (CLV) для каждого клиента как общую сумму (Total Spent),
# потраченную этим клиентом за все транзакции.
# Выведите Топ-10 клиентов по их CLV (customer_id и их CLV).

print(f"Top 10 clients by CLV:")
(df_data.groupBy("customer_id")
        .agg(F.round(F.sum(F.col("total_spent")),2).alias("customer_lifetime_value"))
        .sort("customer_lifetime_value", ascending=False)
        .show(10))

spark.stop()
