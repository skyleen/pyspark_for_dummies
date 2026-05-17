from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("WindowFunctionsExampleEnhanced") \
    .getOrCreate()

# Исходные данные о продажах - ИЗМЕНЕНЫ для демонстрации рангов
sales_data = [
    ("Север", datetime.date(2024, 9, 10), 100.0, 5),
    ("Север", datetime.date(2024, 10, 10), 150.0, 7),
    ("Север", datetime.date(2024, 11, 10), 150.0, 6),
    ("Север", datetime.date(2024, 12, 10), 200.0, 9),
    ("Юг", datetime.date(2024, 10, 10), 80.0, 4),
    ("Юг", datetime.date(2024, 11, 10), 110.0, 5),
    ("Юг", datetime.date(2024, 12, 10), 90.0, 3),
    ("Восток", datetime.date(2024, 10, 10), 250.0, 10),
    ("Восток", datetime.date(2024, 11, 10), 180.0, 8),
    ("Восток", datetime.date(2024, 11, 15), 180.0, 7),
    ("Восток", datetime.date(2024, 12, 10), 300.0, 12),
]

# Определение схемы
sales_schema = StructType([
    StructField("region", StringType(), True),
    StructField("sale_date", DateType(), True),
    StructField("amount", DoubleType(), True),
    StructField("units_sold", IntegerType(), True)
])
df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_sales.orderBy("region", "sale_date").show()

# Определяем окно: разбиваем данные по region и сортируем внутри каждого окна по amount (по возрастанию)
window_spec_rank = Window.partitionBy("region").orderBy("amount")

df_ranked_sales = df_sales.withColumn(
    "row_num_in_region", F.row_number().over(window_spec_rank) # Порядковый номер в регионе
).withColumn(
    "rank_by_amount", F.rank().over(window_spec_rank) # Ранг по сумме в регионе
).withColumn(
    "dense_rank_by_amount", F.dense_rank().over(window_spec_rank) # Плотный ранг по сумме
)

df_ranked_sales.orderBy("region", "amount", "sale_date").show()

# Спецификация окна: партиция по региону, сортировка по дате
window_spec_lag_lead = Window.partitionBy("region").orderBy("sale_date")

df_lag_lead_sales = df_sales.withColumn(
    "prev_sale_amount", F.lag("amount", 1).over(window_spec_lag_lead) # Сумма предыдущей продажи
).withColumn(
    "next_sale_amount", F.lead("amount", 1).over(window_spec_lag_lead) # Сумма следующей продажи
).withColumn(
    "diff_from_prev_sale", F.col("amount") - F.col("prev_sale_amount") # Разница с предыдущей
)

df_lag_lead_sales.orderBy("region", "sale_date").show()

# Окно для кумулятивной суммы: от начала партиции до текущей строки
window_spec_cumulative = Window.partitionBy("region").orderBy("sale_date").rowsBetween(Window.unboundedPreceding, 0)

# Окно для скользящего среднего (текущая + две предыдущие продажи)
window_spec_rolling_avg = Window.partitionBy("region").orderBy("sale_date").rowsBetween(-2, 0) # -2 = две предыдущие строки, 0 = текущая

df_agg_window_sales = df_sales.withColumn(
    "cumulative_amount", F.sum("amount").over(window_spec_cumulative)
).withColumn(
    "rolling_avg_amount", F.avg("amount").over(window_spec_rolling_avg)
)

df_agg_window_sales.orderBy("region", "sale_date").show()

# Останавливаем SparkSession
spark.stop()

spark.stop()