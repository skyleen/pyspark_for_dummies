from pyspark import SparkConf, SparkContext

# Создаем объект SparkConf для настройки конфигурации Spark
conf = (SparkConf()
        .setAppName("Map_Example")
        .setMaster("local[*]")
        )
# Создаем объект SparkContext с использованием настроек SparkConf
sc = SparkContext(conf=conf)

# Теперь можно использовать объект SparkContext для выполнения операций с данными

# Создаем 1_RDD из списка чисел
data_rdd = sc.parallelize([10, 1, 20, 2, 30, 3, 40, 4]) # Используем разнообразные данные

# Берем первые 3 элемента
first_three_elements = data_rdd.take(3)

print("take(3) - Первые 3 элемента:")
print(first_three_elements)
# Вывод может варьироваться в зависимости от распределения партиций

# Создаем 1_RDD из списка чисел
data_rdd = sc.parallelize([1, 10, 5, 20, 15, 3, 8])

# Возвращаем 2 наибольших элемента
top_two_elements = data_rdd.top(2)
print("top(2) - Два наибольших элемента:")
print(top_two_elements)

# Пример top() с пользовательским ключом (для строк по длине)
words_rdd = sc.parallelize(["apple", "banana", "kiwi", "grapefruit", "lemon"])
top_longest_words = words_rdd.top(2, key=lambda s: len(s))

print("top(2, key=len) - Два самых длинных слова:")
print(top_longest_words)

# Создаем 1_RDD из списка чисел
data_rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# Выполняем действие count()
element_count = data_rdd.count()

print("count() - Количество элементов в 1_RDD:")
print(element_count)

# Создаем 1_RDD со повторяющимися значениями
data_rdd = sc.parallelize([1, 2, 3, 3, 1, 4, 2, 5])

# Выполняем countByValue()
value_counts = data_rdd.countByValue()

print("countByValue() - Подсчет по значениям:")
print(value_counts)

# Создаем 1_RDD из списка чисел
data_rdd = sc.parallelize([1, 2, 3, 3])

# Используем reduce для суммирования всех элементов
sum_result = data_rdd.reduce(lambda x, y: x + y)
print("reduce() - Сумма элементов 1_RDD:")
print(sum_result)

# Пример reduce для нахождения максимального значения
max_result = data_rdd.reduce(lambda x, y: x if x > y else y)
print("reduce() - Максимальное значение:")
print(max_result)



# # Указываем путь для сохранения
# PATH = "spark_output_storage_examples"
#
# data_rdd = sc.parallelize(["строка один", "строка два", "строка три"])
# data_rdd.saveAsTextFile(PATH)
#
# # Указываем путь для сохранения
# PATH = "spark_output_storage_examples"
#
# # Создаем 1_RDD из пар
# data_rdd = sc.parallelize([("key1", 10), ("key2", 20)])
# data_rdd.saveAsSequenceFile(PATH)


# Создаем 1_RDD из списка чисел
data_rdd = sc.parallelize([10, 20, 30, 40, 50])

# Определяем функцию, которая будет выполняться на каждом элементе на исполнителях
def process_and_log_item(item):
    """
    Эта функция будет выполняться на каждом элементе 1_RDD на узлах-исполнителях.
    Ее вывод будет виден в логах Spark-воркеров/исполнителей (или на вашей консоли,
    если используете 'local[*]'), а не непосредственно в переменной драйвера.
    """
    print(f"Обработка элемента: {item}")

# Применяем foreach к 1_RDD
data_rdd.foreach(process_and_log_item)



#Не забудьте остановить SparkContext, когда закончите его использовать.
#Если не остановить SparkContext, ваше приложение может продолжать работать в фоновом режиме и потреблять ресурсы
sc.stop()
