from pyspark import SparkConf, SparkContext

# Создаем объект SparkConf для настройки конфигурации Spark
conf = (SparkConf()
        .setAppName("Map_Example")
        .setMaster("local[*]")
        )
# Создаем объект SparkContext с использованием настроек SparkConf
sc = SparkContext(conf=conf)

def data_type_convert(item):
    for key, value in item.items():
            if value !='NULL':
                if key in ('temperature', 'pressure'):
                    item[key] = float(value)
            else:
                item[key] = None
    return item

# 1.1. Создайте 1_RDD из предоставленных данных:
logs_rdd = sc.textFile("/Users/n.shikhaleva/Downloads/sensor_logs.txt")

# 1.2. Разделите текст на строки, используя разделитель '\n' и  разделите каждую строку 1_RDD на отдельные поля, используя символ |  как разделитель.
lines_rdd = (logs_rdd
             .flatMap(lambda r: r.split("\n"))
             .map(lambda c: c.split("|")))
header = lines_rdd.first()
data_rdd = (lines_rdd
            .filter(lambda x: x != header)
            .map(lambda r: {"timestamp": r[0], "sensor_id": r[1], "temperature": r[2], "pressure": r[3], "status": r[4], "error_code":r[5]}))

# 1.3. Преобразуйте поля (temperature, pressure) в соответствующий числовой тип (float). Преобразуйте NULL значения в None
clean_rdd = data_rdd.map(data_type_convert)

# 2. Подсчет общего количества записей по статусам: Посчитайте, сколько всего записей (активностей) было получено для каждого status (например, OK: X, WARNING: Y, ERROR: Z).
statuses = (clean_rdd
            .map(lambda x: (x['status'], 1))
            .reduceByKey(lambda a, b: a + b)
            .sortBy(lambda x: x[1], ascending=False)
            .collect())

print(f"Statuses stats:")
for s, c in statuses:
    print(f"{s}: {c}")
print(f"---")

# 3. Подсчет активных сенсоров с ошибками: Определите sensor_id, которые сообщают о статусе ERROR и количество полученных ошибок каждым из них
errors = (clean_rdd
          .filter(lambda x: x['status'].lower() == 'error')
          .map(lambda x: (x['sensor_id'], 1))
          .reduceByKey(lambda a, b: a + b)
          .collect())

print(f"Active sensors with errors:")
for s, c in sorted(errors):
    print(f"{s}: {c}")
print(f"---")

# 4. Расчет среднего значения температуры: Рассчитайте среднюю temperature. Исключите записи без температуры из расчета среднего. Округлите до двух знаков после запятой.
temperature = (clean_rdd
                  .filter(lambda x: x['temperature'] is not None)
                  .map(lambda x: x['temperature']))
sum_temp = temperature.reduce(lambda a, b: a + b)
avg_temperature = round(sum_temp/temperature.count(),2)
print(f"Avg temperature: {avg_temperature}")
print(f"---")

# 5. Общее количество ошибок по коду: Для каждого error_code (если он не NULL), посчитайте общее количество его появлений. Отсортируйте результат по номеру кода.
error_codes = (clean_rdd
                  .filter(lambda x: x['error_code'] is not None)
                  .map(lambda x: (x['error_code'], 1))
                  .reduceByKey(lambda a, b: a + b)
                  .collect())

print(f"Error codes stats:")
for s, c in sorted(error_codes):
    print(f"{s}: {c}")
print(f"---")

# 6. Высокое давление и температура: Отфильтруйте 1_RDD, чтобы получить только те записи, где pressure выше 10.0 и temperature выше 26.0.
# Для каждой отфильтрованной записи выведите кортеж (timestamp, sensor_id, temperature, pressure).

risks = (clean_rdd
         .filter(lambda x: x['temperature'] is not None and x['pressure'] is not None and x['pressure'] > 10.0 and x['temperature'] > 26.0)
         .map(lambda x: (x['timestamp'], x['sensor_id'], x['temperature'], x['pressure']))
         .collect())

print(f"High pressure and temperature:")
for t, s, tmp, p in sorted(risks):
    print(f"{t} {s} {tmp} {p}")
print(f"---")

sc.stop()