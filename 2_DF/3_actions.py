from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local[*]")
         .appName('Example')
         .getOrCreate()
        )

data = [("Алиса", 30, "Москва"), ("Вася", 25, "Казань"), ("Петя", 35, "Новосибирск"), ("Маша", 32, "Санкт-Петербург"), ("Артем", 23, "Москва"), ("Виталий", 35, "Махачкала"), ("Маша", 18, "Казань")]
df_data = spark.createDataFrame(data, ["name", "age", "city"])
df_data.show(3)

total_rows = df_data.count()
print(f"Общее количество строк: {total_rows}")
cnt_masha = df_data.filter(df_data["name"] == "Маша").count()
print(f"Количество с именем Маша: {cnt_masha}")

all_data_list = df_data.collect()
print(all_data_list)

pandas_df = df_data.toPandas()
print(pandas_df)

print(df_data.take(2))

# Указываем путь для сохранения
output_path = "spark_output/example_data_csv"
# .mode("overwrite") позволяет перезаписать данные, если директория уже существует.
# header=True добавляет заголовок в файл.
df_data.write.mode("overwrite").csv(output_path, header=True)

spark.stop()
