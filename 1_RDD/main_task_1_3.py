import re

from pyspark import SparkConf, SparkContext

conf = (SparkConf()
        .setAppName("Map_Example")
        .setMaster("local[*]")
        )
sc = SparkContext(conf=conf)

# 1. Загрузите файл Rus_dict.txt в RDD.
dict_rdd = sc.textFile("/Users/n.shikhaleva/Downloads/Rus_dict.txt")
# 2. Статистика по словам:
# 2. 1. Посчитайте общее количество слов в словаре.
print(f"Total: {dict_rdd.count()}")

# 2.2. Найдите Топ-5 самых длинных слов.
top5_max_length = (dict_rdd
                   .top(5, key=lambda s: len(s)))
print(f"Top 5 longest words:")
for w in top5_max_length:
        print(f"{w}: {len(w)}")
print(f"---")
# 2.3. Найдите Топ-5 самых коротких слов.
top5_min_length = (dict_rdd
                   .sortBy(lambda w: len(w), ascending=True)
                   .take(5))
print(f"Top 5 smallest words:")
for w in top5_min_length:
        print(f"{w}: {len(w)}")
print(f"---")

# 3. Анализ по длине слов:
len_rdd = (dict_rdd
           .map(lambda x: (x, len(x))))
# 3.1. Посчитайте, сколько слов имеют каждую конкретную длину (например, слов длиной 3: X, слов длиной 5: Y). Отсортируйте результат по длине.
len_stat_rdd = (len_rdd
                .map(lambda v: (v[1], 1))
                .reduceByKey(lambda a, b: a + b)
                .sortBy(lambda v: v[0]))
print(f"Length stats:")
for k, v in len_stat_rdd.collect():
        print(f"{k}: {v}")
print(f"---")

# 3.2. Определите длину слова, которая встречается чаще всего.
most_pop_len = (len_stat_rdd
                   .sortBy(lambda v: v[1], ascending=False)
                   .first()[0])
print(f"Most popular word length: {most_pop_len}")

# 4. Символьный анализ:
# 4.1. Посчитайте, сколько слов содержат букву 'ё'.
e_words = (dict_rdd
           .filter(lambda w: "ё" in w.lower())
           .count())
print(f"Words with Ё letter: {e_words}")

# 4.2. Отфильтруйте и выведите все слова, которые являются палиндромами (читаются одинаково вперед и назад, например, "шалаш", "заказ").
palindromes = (dict_rdd
               .filter(lambda y: y == y[::-1]))
print(f"Palindromes:")
for w in palindromes.collect():
        print(f"{w}")

sc.stop()