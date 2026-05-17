from pyspark.sql import SparkSession
from pyspark.sql.functions import col,concat,lit,sin

spark = SparkSession.builder.appName("CodegenAnalysis").getOrCreate()

# Создаем DataFrame с несколькими последовательными преобразованиями
df = spark.range(0, 1_000_000) \
          .withColumn("age", (col("id") % 80) + 20) \
          .withColumn("name", concat(lit("user_"), col("id").cast("string"))) \
          .withColumn("salary", col("id") * 10.5)

df_result = df.filter(col("age") > 30) \
              .filter(col("salary") < 5_000_000) \
              .select("name", "age", (col("salary") * sin(col("age"))).alias("adjusted_salary"))

df_result.explain()

spark.stop()