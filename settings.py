from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAll([
    ("spark.app.name", "MySparkApp"),
    ("spark.executor.cores", "4"),           # 4-6 ядер оптимальный баланс для GC
    ("spark.executor.memory", "10g"),        # Начальное значение памяти
    ("spark.executor.memoryOverhead", "2g"), # ~15-20% для системных нужд и Python
    ("spark.driver.memory", "4g"),           # Память для управления и агрегации результатов
    ("spark.driver.maxResultSize", "4g"),    # ДОЛЖНО БЫТЬ <= driver.memory
    ("spark.sql.shuffle.partitions", "200")  # Настройка параллелизма джойнов
])

spark = SparkSession.builder.config(conf=conf).getOrCreate()
# динамическая аллокация для Jupyter
# "spark.dynamicAllocation.enabled": "true",
# "spark.dynamicAllocation.minExecutors": "1",
# "spark.dynamicAllocation.maxExecutors": "10",
# "spark.dynamicAllocation.executorIdleTimeout": "300s",
# "spark.dynamicAllocation.cachedExecutorIdleTimeout": "30m" # Убить экзекутор даже если есть кэш

# статическая аллокация для ETL
# "spark.dynamicAllocation.enabled": "false",
# "spark.executor.instances": "6",
# "spark.executor.cores": "5",
# "spark.executor.memory": "20g"