from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = (SparkSession.builder
    .appName("DeltaLab")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:4.1.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

data = [(1, "alice", 25), (2, "bob", 30)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.write.format("delta").save("./tmp/delta_lab")

# 1. DeltaTable.forPath читает лог и подключается к папке как к базе данных
my_table = DeltaTable.forPath(spark, "./tmp/delta_lab")

# 2. Метод update находит строку по условию (condition) и заменяет значения (set)
my_table.update(condition="id = 1", set={"age": "26"})

updates_df = spark.createDataFrame([
    (1, "alice_updated", 26),  # У Алисы изменилось имя и возраст
    (99, "david", 35)          # Новый пользователь
], ["id", "name", "age"])

my_table.alias("target").merge(updates_df.alias("source"),
                                "target.id = source.id" # Условие сопоставления (ключ)
                                ).whenMatchedUpdateAll(     # Если id совпал -> обновить все колонки
                                ).whenNotMatchedInsertAll(  # Если id не найден -> вставить всю строку
                                ).execute()


# whenMatchedUpdateAll(): обновляет все колонки в старой таблице значениями из новых данных.
# whenMatchedUpdate(set={...}): обновляет только указанные колонки (например, только возраст).
# whenNotMatchedInsertAll(): вставляет новую строку целиком, если совпадений не найдено.
# whenNotMatchedInsert(values={...}): вставляет новую строку, заполняя только указанные колонки.
# whenMatchedDelete(): удаляет строку из старой таблицы, если найдено совпадение.

# Смотрим историю всех операций
my_table.history().select("version", "timestamp", "operation").show(truncate=False)
# Вариант А: Читаем состояние по номеру версии
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("./tmp/delta_lab")
df_v0.show()
# Вариант Б: Читаем состояние по времени (например, до момента ошибки)
df_by_time = spark.read.format("delta").option("timestampAsOf", "2026-04-18 23:50:00").load("./tmp/delta_lab")
df_by_time.show()

# Пытаемся записать данные с новой колонкой city, которой нет в таблице
new_data = spark.createDataFrame([(4, "eve", 28, "New York")], ["id", "name", "age", "city"])

# Этот код упадет с ошибкой, предотвращая изменение структуры
# new_data.write.format("delta").mode("append").save("./tmp/delta_lab")
# Разрешаем добавление новой колонки в структуру таблицы:
new_data.write.format("delta").mode("append").option("mergeSchema", "true").save("./tmp/delta_lab")


# Отслеживание изменений
# 1. Создаем таблицу с включенным CDF
(spark.range(0, 3).write.format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .save("./tmp/delta_cdf_lab"))

cdfTable = DeltaTable.forPath(spark, "./tmp/delta_cdf_lab")

# 2. Вносим изменения (удаляем строку)
cdfTable.delete(condition="id = 1")

# 3. Читаем не таблицу, а лог действий
changes_df = (spark.read.format("delta")
    .option("readChangeData", "true")
    .option("startingVersion", 0)
    .load("./tmp/delta_cdf_lab"))

changes_df.show()

# Удаляем файлы, которые стали неактуальными более 24 часов назад
my_table.vacuum(24)

# Очистка СЕЙЧАС (0 часов)
# (например, когда данные пользователя нужно физически стереть).
# Для запуска придется отключить встроенную защиту от случайного удаления:
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
my_table.vacuum(0)

# Запускаем слияние файлов в крупные блоки + группировку строк по id
my_table.optimize().executeZOrderBy("id")