from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FileLogging").getOrCreate()
jvm = spark.sparkContext._jvm
log4j = jvm.org.apache.log4j

log4j.LogManager.getRootLogger().setLevel(log4j.Level.DEBUG)
log4j.LogManager.getLogger("org.apache.spark").setLevel(log4j.Level.DEBUG)

# Создание слушателя для записи в файл
file_appender = log4j.FileAppender(
    log4j.PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"),
    "spark_debug_log.txt", # Имя файла
    True                   # append: дописывать в конец
)

# Добавление слушателя к главному логгеру Spark
log4j.LogManager.getRootLogger().addAppender(file_appender)

# Тестовое действие для генерации логов
spark.range(0, 100).count()

spark.stop()