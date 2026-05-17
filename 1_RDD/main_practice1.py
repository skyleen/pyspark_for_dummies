from pyspark import SparkConf, SparkContext

# Создаем объект SparkConf для настройки конфигурации Spark
conf = (SparkConf()
        .setAppName("Map_Example")
        .setMaster("local[*]")
        )
# Создаем объект SparkContext с использованием настроек SparkConf
sc = SparkContext(conf=conf)

# Чтение текстового файла в 1_RDD
lines_rdd = sc.textFile("/Users/n.shikhaleva/Downloads/idioms.txt")

# Разделяем каждую строку на слова, переводим в нижний регистр, убираем знаки и фильтруем по кол-ву символов
words_rdd = (lines_rdd.flatMap(lambda line: line.lower()
                               .split()).map(lambda word: word.rstrip('.?!'))
                               .filter(lambda word: len(word) > 3)
            )
#Преобразуем каждое слово в пару
word_pairs_rdd = words_rdd.map(lambda word: (word, 1))

# Суммируем счетчики для каждого слова
word_counts_rdd = word_pairs_rdd.reduceByKey(lambda a, b: a + b)

#Получаем топ-10 самых частых слов
top_10_words = word_counts_rdd.top(10, lambda x: x[1]) # Сортировка по убыванию количества

print("Топ-10 самых частых слов:")
for word, count in top_10_words:
    print(f"{word}: {count}")

count_unique_words = word_counts_rdd.count()
print(f"Количество уникальных слов: {count_unique_words}")

count_words = word_pairs_rdd.count()
print(f"Количество слов: {count_words}")

count_stone = word_counts_rdd.filter(lambda word: word[0] == 'камень').first()
print(f"Камень встречается {count_stone[1]} раз")

sc.stop()