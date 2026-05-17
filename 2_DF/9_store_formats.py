from pyspark.sql import SparkSession

# Создание SparkSession
spark = (SparkSession.builder
    .appName("WriteAvroExample")
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.1.1") # <-- подключаем avro
    .getOrCreate())

#  Пример данных
data = [("Алиса", 30, "Москва"), ("Вася", 25, "Казань"), ("Петя", 35, "Новосибирск"), ("Маша", 32, "Санкт-Петербург"), ("Артем", 23, "Москва"), ("Виталий", 35, "Махачкала"), ("Маша", 18, "Казань")]
df = spark.createDataFrame(data, ["Name", "Age", "City"])

# Запись данных в Avro
df.write.format("avro").save("/Users/n.shikhaleva/Downloads/avro")

# Чтение данных из Avro
df_avro = spark.read.format("avro").load("/Users/n.shikhaleva/Downloads/avro")
df_avro.show()


# Запись данных в parquet
df.write.parquet("/Users/n.shikhaleva/Downloads/parquet")
# Чтение данных из Parquet
df_parquet = spark.read.parquet("/Users/n.shikhaleva/Downloads/parquet")
df_parquet.show()


# Запись данных в ORC
df.write.orc("/Users/n.shikhaleva/Downloads/orc")
# Чтение данных из ORC
df_orc = spark.read.orc("/Users/n.shikhaleva/Downloads/orc")
df_orc.show()

spark.stop()