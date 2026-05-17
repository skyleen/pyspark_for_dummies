from pyspark import SparkConf, SparkContext

# Создаем объект SparkConf для настройки конфигурации Spark
conf = (SparkConf()
        .setAppName("Map_Example")
        .setMaster("local[*]")
        )
# Создаем объект SparkContext с использованием настроек SparkConf
sc = SparkContext(conf=conf)

# Теперь можно использовать объект SparkContext для выполнения операций с данными

# Возведение чисел в квадрат
# Создаем 1_RDD из списка чисел
data_rdd = sc.parallelize([1, 2, 3, 4, 5])

# Применяем функцию к каждому элементу 1_RDD (в данном случае, лямбда-функцию)
squared_rdd = data_rdd.map(lambda x: x * x)

#collect() запускает все предыдущие преобразования и вычисляет 1_RDD,
#а затем собирает все элементы полученного 1_RDD с распределенных узлов и возвращает их как обычный Python-список на драйвер-программу
print("map() - Возведение в квадрат:")
print(squared_rdd.collect())

# Создаем 1_RDD из списка строк
log_rdd = sc.parallelize(["INFO: app started", "ERROR: failed to connect", "WARNING: low disk space", "ERROR: disk full"])

# Фильтрация строк, содержащих "ERROR"
errors_rdd = log_rdd.filter(lambda line: "ERROR" in line)

# Выводим результат
print("filter() - Строки с 'ERROR':")
print(errors_rdd.collect())

# Создаем 1_RDD из списка строк
sentences_rdd = sc.parallelize(["hello world spark", "spark is great fun", "python rocks"])

# Разделение каждой строки на слова и создание плоского списка всех слов
words_from_sentences_rdd = sentences_rdd.flatMap(lambda line: line.split(" "))

# Выводим результат
print("flatMap() - Слова из предложений:")
print(words_from_sentences_rdd.collect())

# Исходный 1_RDD: список фруктов с их количествами
# Представим, что это данные из разных источников или партиций
fruit_data = [
        ("apple", 10), ("banana", 5), ("apple", 7),
        ("orange", 12), ("banana", 3), ("apple", 5)
    ]
fruits_rdd = sc.parallelize(fruit_data)

# Агрегируем количества фруктов по их названию
total_fruits_rdd = fruits_rdd.reduceByKey(lambda x, y: x + y)

print("reduceByKey() - Общее количество каждого фрукта:")
print(sorted(total_fruits_rdd.collect()))

# Исходный 1_RDD: список студентов и их курсов
student_courses = [
        ("Алиса", "Math"), ("Вася", "Physics"), ("Алиса", "Chemistry"),
        ("Петя", "History"), ("Вася", "Chemistry")
    ]
courses_rdd = sc.parallelize(student_courses)

# Группируем курсы по каждому студенту
# groupByKey возвращает (key, Iterable[value])
grouped_courses_rdd = courses_rdd.groupByKey()

print("groupByKey() - Курсы каждого студента:")
for student, courses_iterable in sorted(grouped_courses_rdd.collect()):
    print(f"  Студент '{student}': {sorted(list(courses_iterable))}")

# Создаем два исходных 1_RDD для демонстрации
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([3, 4, 5, 6])

# Объединяем rdd1 и rdd2
union_rdd = rdd1.union(rdd2)
print("union() - Объединение RDD1 и RDD2 (с дубликатами):")
print(sorted(union_rdd.collect()))

# Создаем два 1_RDD с парами (ключ, значение)
# RDD1: Пользователи и их возрасты
users_age_rdd = sc.parallelize([("user1", 30), ("user2", 25), ("user3", 35), ("user1", 31)])
# RDD2: Пользователи и их города
users_city_rdd = sc.parallelize([("user2", "New York"), ("user1", "London"), ("user4", "Paris")])

# Выполняем join по ключу (ID пользователя)
joined_rdd = users_age_rdd.join(users_city_rdd)
print("join() - Объединенные данные пользователей:")
print(sorted(joined_rdd.collect()))
# 'user1' имел два возраста в первом 1_RDD, поэтому он соединяется с 'London' дважды.

# Создаем 1_RDD с парами (слово, счетчик), например, из результатов word count
word_counts_rdd_unsorted = sc.parallelize([("apple", 2), ("banana", 1), ("orange", 1), ("grape", 1), ("zebra", 1)])

# Сортировка по ключу (слову) по убыванию
sorted_word_counts_rdd_desc = word_counts_rdd_unsorted.sortByKey(ascending=False)
print("sortByKey() - Сортировка по убыванию:")
print(sorted_word_counts_rdd_desc.collect())

words_with_duplicates_rdd = sc.parallelize(["apple", "banana", "apple", "orange", "grape", "banana", "apple"])

# Получаем 1_RDD с уникальными элементами
distinct_words_rdd = words_with_duplicates_rdd.distinct()
print("distinct() - Уникальные слова:")
print(sorted(distinct_words_rdd.collect()))

#Не забудьте остановить SparkContext, когда закончите его использовать.
#Если не остановить SparkContext, ваше приложение может продолжать работать в фоновом режиме и потреблять ресурсы
sc.stop()
