import re

from pyspark import SparkConf, SparkContext

conf = (SparkConf()
        .setAppName("Map_Example")
        .setMaster("local[*]")
        )
sc = SparkContext(conf=conf)

# 1.1. Загрузите файл password.txt в 1_RDD.
raw_data = sc.textFile("/Users/n.shikhaleva/Downloads/password.txt")

# 1.2. Очистите каждую строку от лишних пробелов и удалите пустые строки, если они есть в файле.
clean_rdd = (raw_data
             .map(lambda x: x.rstrip())
             .filter(lambda z: len(z) > 0))

pass_len = clean_rdd.map(lambda z: (z, len(z)))
# 2. Длина паролей:
# 2.1. Рассчитайте среднюю длину пароля во всем списке. Округлите до целого числа.
p_len_sum = (pass_len
             .map(lambda z: z[1])
             .reduce(lambda a, b: a + b))
avg_len = int(p_len_sum / pass_len.count())
print(f"Avg password length: {avg_len}")
#
# 2.2. Найдите минимальную и максимальную длину паролей.
max_length = pass_len.sortBy(lambda a: a[1], ascending=False)
print(f"Max password length: {max_length.first()[1]}")
min_length = pass_len.sortBy(lambda a: a[1], ascending=True)
print(f"Min password length: {min_length.first()[1]}")
print("---")
# 2.3. Определите Топ-5 самых распространенных длин паролей (например, сколько паролей имеют длину 8 символов, сколько — 6 и т.д.).
top5_len_rdd = (pass_len
           .map(lambda z: (z[1],1))
           .reduceByKey(lambda a, b: a + b)
           .top(5, lambda x: x[1])
           )
print(f"Top 5 lengthes:")
for l in top5_len_rdd:
    print(f"{l[0]}: {l[1]}")
print("---")

# 3. Символьный состав:
# 3.1. Посчитайте количество паролей, содержащих только цифры.
numbers_only = (clean_rdd
                .filter(lambda z: z.isdigit())
                .count())
print(f"Passwords with numbers only: {numbers_only}")
# 3.2. Посчитайте количество паролей, содержащих только буквы (как строчные, так и заглавные).
chars_only = (clean_rdd
              .filter(lambda x: re.fullmatch(r"^[a-zA-Z]+$", x))
              .count())
print(f"Passwords with letters only: {chars_only}")
print("---")
# 4. Распространенные префиксы/суффиксы:
# 4.1 Определите Топ-5 самых распространенных префикса длиной 3 символа (например, 123, pas).
pref_rdd = (clean_rdd
            .map(lambda z: (z[0:3], 1))
            .reduceByKey(lambda a, b: a + b)
            .top(5, lambda x: x[1]))
print(f"Top 5 prefixes:")
for l in pref_rdd:
    print(f"{l}")
print("---")

# 4.2 Определите Топ-5 самых распространенных суффикса длиной 3 символа (например, 678, ord).
suf_rdd = (clean_rdd
            .map(lambda z: (z[len(z)-3:], 1))
            .reduceByKey(lambda a, b: a + b)
            .top(5, lambda x: x[1]))
print(f"Top 5 suffixes:")
for l in suf_rdd:
    print(f"{l}")
print("---")

sc.stop()