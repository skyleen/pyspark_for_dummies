from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("ShuffleExplain").getOrCreate()

data = [(i % 1000, i * 1.0) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["key", "value"])

df_result = df.groupBy("key").agg(sum("value").alias("total_value"))
df_result.explain()

spark.stop()