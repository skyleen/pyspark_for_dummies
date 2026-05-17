from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("S3_MinIO_Integration")

         # 1. Подключение библиотек Hadoop-AWS
         .config("spark.jars.packages",
                 "org.apache.hadoop:hadoop-aws:3.5.0,"
                 "software.amazon.awssdk:bundle:2.44.7")

         # 2. Ключи доступа и адрес локального MinIO (порт 9002)
         .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9002")
         .config("spark.hadoop.fs.s3a.access.key", "minio_admin")
         .config("spark.hadoop.fs.s3a.secret.key", "minio_password")

         # 3. Системные настройки (обязательно для локального MinIO)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

         # 4. Оптимизация скорости записи в S3
         .config("spark.hadoop.fs.s3a.committer.name", "magic")
         .getOrCreate())

# Подготовка тестовых данных
data = [
    (1, "Laptop", "2024-01-01", 1200.0),
    (2, "Mouse", "2024-01-01", 25.5),
    (3, "Keyboard", "2024-01-02", 45.0)
]
columns = ["id", "category", "date", "price"]
df = spark.createDataFrame(data, columns)

# Запись в MinIO
(df.coalesce(1) # Схлопываем данные в 1 файл на каждую директорию даты
    .write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("date") # Создаст структуру в бакете: date=2024-01-01/
    .save("s3a://datalake/raw/sales_data/"))

print("Данные успешно записаны в MinIO.")

# Чтение данных из бакета
s3_df = spark.read.format("parquet").load("s3a://datalake/raw/sales_data/")

# Вывод схемы и данных
s3_df.printSchema()
s3_df.show()

spark.stop()
