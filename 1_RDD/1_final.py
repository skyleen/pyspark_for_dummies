from pyspark import SparkConf, SparkContext

conf = (SparkConf()
        .setAppName("Map_Example")
        .setMaster("local[*]")
        )
sc = SparkContext(conf=conf)

# 1. Загрузите файл List of Countries.txt в RDD.
countries_rdd = sc.textFile("/Users/n.shikhaleva/Downloads/List of Countries.txt")

# 2. Определите количество стран для каждой первой буквы их названия (сколько стран начинается на 'A', сколько на 'B' и так далее).
# Выведите результат в алфавитном порядке по букве.
first_letters_rdd = (countries_rdd
                             .map(lambda l: (l[:1], 1))
                             .reduceByKey(lambda a, b: a + b)
                             .sortByKey())
print(f"First letters stats.")
for k, v in first_letters_rdd.collect():
        print(f"{k}: {v}")
print(f"---")
# 3. Найдите 5 самых длинных названий стран в списке.
countries_len_rdd = (countries_rdd
                        .map(lambda l: (len(l), l)))

# Если есть несколько стран с одинаковой максимальной длиной, выведите их все.
top5_longest_names_rdd = (countries_len_rdd
                             .groupByKey()
                             .top(5)
                             )
print(f"Top5 countries with the longest names.")
for l,c in top5_longest_names_rdd:
     print(f"List of countries with length {l}: {sorted(list(c))}")
print(f"---")
# 4. Найдите 5 самых коротких названий стран в списке.
# Аналогично, если есть несколько с одинаковой минимальной длиной, выведите их все.
top5_shortest_names_rdd = (countries_len_rdd
                             .groupByKey()
                             .sortByKey()
                             .take(5)
                             )
print(f"Top5 countries with the shortest names.")
for l,c in top5_shortest_names_rdd:
        print(f"List of countries with length {l}: {sorted(list(c))}")
print(f"---")

# 5. Рассчитайте среднюю длину названия стран в списке. Округлите результат до одного знаков после запятой.
total_count = countries_len_rdd.count()
sum_lenght = (countries_len_rdd
              .map(lambda l: l[0])
              .reduce(lambda a, b: a + b))
avg_len = round(sum_lenght / total_count, 1)
print(f"Avg name length: {avg_len}")
print(f"---")

# 6. Найдите все страны, названия которых состоят из двух и более слов.
c_names_rdd = (countries_rdd
               .filter(lambda l: len(list(l.split())) > 1))

print(f"List of countries with multi-word name:\n"
      f"{c_names_rdd.collect()}")

sc.stop()
