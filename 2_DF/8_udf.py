from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("UDFExample") \
    .getOrCreate()

example_df = spark.range(5).toDF("num")

def power3(double_value):
    """Возводит число в третью степень."""
    return double_value ** 3

# Регистрация UDF для использования с DataFrame API
power3udf_df = F.udf(power3, IntegerType()) # Указываем возвращаемый тип IntegerType
print("Использование UDF с DataFrame API:")
example_df.select(power3udf_df(F.col("num")).alias("num_cubed")).show()

# Регистрация UDF как функции Spark SQL (для использования в SQL-запросах)
spark.udf.register("power3udf_sql", power3, IntegerType())
print("Использование UDF как функции Spark SQL:")
example_df.selectExpr("power3udf_sql(num) AS num_cubed").show()

spark.stop()