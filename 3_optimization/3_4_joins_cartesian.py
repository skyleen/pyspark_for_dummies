# Cartesian Product Join (Декартово Произведение):
# Создает все возможные комбинации строк из двух DataFrame.
# Каждая строка из первого DataFrame объединяется с каждой строкой из второго.
# Это крайне дорогая операция, требующая shuffle.
# Она используется, когда нет условия объединения (ON clause отсутствует) или если условие всегда истинно (ON 1=1).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat

spark = (SparkSession
         .builder
         .appName("CartesianProductJoin")
         .getOrCreate())

df1_cartesian = spark.range(0, 10_000_000).withColumnRenamed("id", "col1")
df2_cartesian = spark.range(0, 10_000_000).withColumnRenamed("id", "col2")

print("Cartesian Product Join без условия Join:")
df_cartesian_forced = df1_cartesian.join(df2_cartesian)
df_cartesian_forced.explain()

spark.stop()