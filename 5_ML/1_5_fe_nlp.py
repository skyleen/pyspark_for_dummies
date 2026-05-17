from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover

spark = SparkSession.builder.appName("NLP_Basics").getOrCreate()

# Исходные данные
data = [
    (1, "Spark is amazing and fast"),
    (2, "Spark is built for big data"),
    (3, "Hadoop is also good")
]
df = spark.createDataFrame(data, ["id", "text"])

# Разбиваем на слова
tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordsData = tokenizer.transform(df)

# Удаляем стоп-слова
remover = StopWordsRemover(inputCol="words", outputCol="clean_words")
cleanData = remover.transform(wordsData)

cleanData.show(truncate=False)
spark.stop()