import re

from pyspark import SparkConf, SparkContext

conf = (SparkConf()
        .setAppName("Map_Example")
        .setMaster("local[*]")
        )
sc = SparkContext(conf=conf)

# 1. Предварительная обработка данных
# 1.1. Загрузите файл logfiles.log в RDD.
logs_rdd = sc.textFile("/Users/n.shikhaleva/Downloads/logfiles.log")
# 1.2. Напишите функцию parse_log, которая будет парсить каждую строку лога. Функция должна извлекать и возвращать следующие поля в кортеже:
# IP-адрес
# Полную строку запроса (например, "GET /api/v1/users HTTP/1.0")
# Статус-код ответа
# Размер ответа в байтах
def parse_logs(logs):
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d{3}) (\d+)'
    m = re.match(pattern, logs)
    if m:
            ip = m.group(1)
            request = m.group(3)
            status = m.group(4)
            size = m.group(5)
            return (ip, request, status, size)
    return None

clean_logs = logs_rdd.map(parse_logs)
print(clean_logs.take(2))
# 2. Общая статистика по логам
# 2.1. Посчитайте общее количество запросов.
total_count = clean_logs.count()
print(f"Total requests count: {total_count}")

# 2.2.Расcчитайте средний размер ответа сервера (в байтах) по всем запросам.
total_size = (clean_logs
              .map(lambda x: int(x[3]))
              .reduce(lambda a, b: a+b))

print(f"Avg response size: {int(total_size/total_count)}")

# 2.3. Определите количество уникальных IP-адресов, которые обращались к серверу.
ip_count = clean_logs.map(lambda x: x[0]).distinct().count()
print(f"Unique IPs: {ip_count}")

# 3. Анализ HTTP-статусов
# 3.1. Посчитайте количество запросов для каждого HTTP-статус-кода (например, сколько 200, сколько 404, сколько 500).
status_code_stat = (clean_logs
                    .map(lambda x: (x[2] , 1))
                    .reduceByKey(lambda a, b: a + b))
print(f"Error code stats:")
for i, y in status_code_stat.collect():
        print(f"{i}: {y} requests")
print(f"---")

# 3.2. Определите долю успешных запросов (статус-код 200) от общего числа запросов в процентах
success_stat = (clean_logs
                .filter(lambda x: x[2] == "200")
                .count())
print(f"Success response rate: {(success_stat/total_count)*100}%")
print(f"---")
# 4. Анализ эндпоинтов и запросов
endpoints_rdd = (clean_logs
                 .map(lambda x: x[1].split()))
# 4.1. Найдите Топ-5 самых часто запрашиваемых эндпоинтов. Эндпоинт - это часть URL-адреса, которая указывает на конкретный ресурс, или функцию на сервере, к которой обращается клиент.
# Например, в запросе "GET /api/v1/users HTTP/1.0" эндпоинтом является /api/v1/users.
top5_endpoints = (endpoints_rdd
                  .map(lambda x: (x[1], 1))
                  .reduceByKey(lambda a, b: a+b)
                  .sortByKey(ascending=False)
                  .take(5))
print(f"Top 5 endpoints:")
for endpoint, count in top5_endpoints:
        print(f"{endpoint}: {count} requests")
print(f"---")
# 4.2. Посчитайте, сколько запросов каждого типа (GET, POST, PUT и т.д.) было сделано
types_stat = (endpoints_rdd
              .map(lambda x: (x[0], 1))
              .reduceByKey(lambda a, b: a+b)
              .sortBy(lambda x: x[1], ascending=False)
              .collect())
print(f"Endpoints' types stats:")
for endpoint, count in types_stat:
        print(f"{endpoint}: {count} requests")
print(f"---")

sc.stop()