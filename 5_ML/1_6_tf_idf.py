from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

spark = SparkSession.builder.appName("TF-IDF_Hashing").getOrCreate()
data = [
    (0, "spark is fast"),
    (1, "spark is scaleable"),
    (2, "spark is unique")
]
df = spark.createDataFrame(data, ["id", "sentence"])

# Разбиваем на слова
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(df)

# HashingTF: Превращаем слова в хэши
# numFeatures=20 - это размер вектора (число корзин для хэшей)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)

print("TF (Частоты):")
featurizedData.select("words", "rawFeatures").show(truncate=False)

# Узнаем индексы интересующих нас слов
idx_unique = hashingTF.indexOf("unique")
idx_spark = hashingTF.indexOf("spark")
idx_is = hashingTF.indexOf("is")

print(f"Индекс 'unique': {idx_unique}")
print(f"Индекс 'spark' : {idx_spark}")
print(f"Индекс 'is'    : {idx_is}")

# IDF: Взвешиваем
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

print("\nРезультат TF-IDF (векторы):")
rescaledData.select("features").show(truncate=False)

print("Результат TF-IDF (расшифровка):")
rows = rescaledData.select("words", "features").collect()

# Словарик для обратного поиска
idx_to_word = {idx_unique: "unique", idx_spark: "spark", idx_is: "is"}

for row in rows:
    vector = row["features"]
    words = row["words"]
    print(f"Слова: {words}")
    # Пройдемся по ненулевым элементам вектора
    for idx, value in zip(vector.indices, vector.values):
        if idx in idx_to_word:
            print(f"  -> {idx_to_word[idx]} (idx {idx}): вес {value:.4f}")
    print("-" * 20)
spark.stop()
