from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AdvancedLogging").getOrCreate()

# Настройка логгера через JVM-мост
log4j = spark.sparkContext._jvm.org.apache.log4j

# Устанавливаем DEBUG для всего приложения, но гасим системный шум (Hadoop, Akka, Spark)
log4j.LogManager.getRootLogger().setLevel(log4j.Level.DEBUG)           # Общий фон - DEBUG
log4j.LogManager.getLogger("org").setLevel(log4j.Level.ERROR)         # Весь системный шум - только ERROR
log4j.LogManager.getLogger("akka").setLevel(log4j.Level.ERROR)        # Сетевой шум (Akka) - только ERROR
log4j.LogManager.getLogger("org.apache.spark").setLevel(log4j.Level.WARN) # Ядро Spark – уровень WARN

# Тестовое действие
df = spark.range(0, 1000)
print(f"Результат вычислений: {df.count()}")

spark.stop()