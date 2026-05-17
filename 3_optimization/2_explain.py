from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("ShuffleExplain")
         .getOrCreate())

data = [("Алиса", 30, "Москва"),
        ("Вася", 25, "Казань"),
        ("Петя", 35, "Новосибирск"),
        ("Маша", 32, "Санкт-Петербург"),
        ("Артем", 23, "Москва"),
        ("Виталий", 35, "Махачкала"),
        ("Маша", 18, "Казань")]
df = spark.createDataFrame(data, ["name", "age", "city"])

# explain() - physical execution plan only
filtered_df = df.filter(df.age > 30)
filtered_df.explain()

# df.explain(True) - all optimization steps
filtered_df.explain(True)

spark.stop()