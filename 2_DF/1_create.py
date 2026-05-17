from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .master("local[*]")\
        .appName('Example')\
        .getOrCreate()

# Создание RDD
data = [("Вася", 30), ("Петя", 24), ("Маша", 35), ("Даша", 29)]
rdd = spark.sparkContext.parallelize(data)

# Преобразование RDD в DataFrame
df_from_rdd = rdd.toDF(["Name", "Age"])
# Выводим схему
df_from_rdd.printSchema()

# Загрузка данных из CSV-файла
df_csv = spark.read.csv("data.csv", header=True, inferSchema=True)

# Загрузка данных из таблицы базы данных
df_jdbc = (spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql:dbserver")
        .option("dbtable", "schema.tablename")
        .option("user", "username")
        .option("password", "password")
        .load()
        )

# Создание DataFrame из списка кортежей
data = [("Ivan", 30), ("Maria", 25)]
columns = ["Name", "Age"]
df_from_list = spark.createDataFrame(data, schema=columns)
df_from_list.printSchema()

# Останавливаем SparkSession
spark.stop()