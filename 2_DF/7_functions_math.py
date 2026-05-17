from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("MathFunctionsExample") \
    .getOrCreate()

# Исходные данные
data = [
    (1, 10.5, 2, 3.0),
    (2, 5.0, -1, 4.0),
    (3, 8.2, None, 5.0),
    (4, -3.7, 2, 6.0),
    (5, 12.0, 0, 7.0),
    (6, None, 1, 8.0),
    (7, 9.0, 6, 2.0),
    (8, float('nan'), 3, 4.0),
    (9, 6.0, 2, float('nan'))
]

# Определение схемы. Столбец "value2" теперь IntegerType.
# При создании DataFrame из данных с float('nan') для IntegerType, NaN будет преобразован в null.
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("value1", DoubleType(), True),
    StructField("value2", IntegerType(), True),
    StructField("value3", DoubleType(), True)
])

df = spark.createDataFrame(data, schema=schema)
df.show(truncate=False)

df_math_basic = df.withColumn(
    "sum_values", F.col("value1") + F.col("value2")
).withColumn(
    "diff_values", F.col("value1") - F.col("value2")
).withColumn(
    "product_values", F.col("value1") * F.col("value2")
).withColumn(
    "division_values", F.col("value2") / F.col("value1")
).withColumn(
    "value1_round", F.round(F.col("value1"), 1)
).withColumn(
    "value1_floor", F.floor(F.col("value1"))
).withColumn(
    "value1_ceil", F.ceil(F.col("value1"))
).withColumn(
    "abs_value1", F.abs(F.col("value1"))
)
df_math_basic.select("id", "value1", "value2", "sum_values", "diff_values",
                     "product_values", "division_values", "value1_round",
                     "value1_floor", "value1_ceil", "abs_value1").show(truncate=False)


df_math_advanced = df.withColumn(
    "exp_value1", F.exp(F.col("value1")) # Экспонента
).withColumn(
    "log_value1", F.log(F.col("value1")) # Натуральный логарифм
).withColumn(
    "log10_value1", F.log10(F.col("value1")) # Десятичный логарифм
).withColumn(
    "pow_value1_to_3", F.pow(F.col("value1"), 3) # В степени 3
).withColumn(
    "sqrt_value1", F.sqrt(F.col("value1")) # Квадратный корень
)

df_math_advanced.select("id", "value1", "exp_value1", "log_value1",
                        "log10_value1", "pow_value1_to_3", "sqrt_value1").show(truncate=False)

# Останавливаем SparkSession
spark.stop()