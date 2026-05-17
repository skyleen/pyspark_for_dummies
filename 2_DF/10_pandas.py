from pyspark import pandas as ps

data = [
    ("Маргарита", 12.50, ["Томаты", "моцарелла", "базилик"], True, 4.5, 1200),
    ("Пепперони", 15.00, ["Томаты", "моцарелла", "пепперони"], False, 4.8, 2500),
    ("Четыре сыра", 14.00, ["Моцарелла", "пармезан", "горгонзола", "чеддер"], True, 4.2, 850),
    ("Мясная", 16.50, ["Томаты", "моцарелла", "ветчина", "бекон", "салями"], False, 4.7, 1800),
    ("Гавайская", 13.00, ["Томаты", "моцарелла", "ветчина", "ананасы"], False, 3.9, 600),
    ("Вегетарианская", 13.50, ["Томаты", "моцарелла", "грибы", "перец", "лук"], True, 4.1, 950),
    ("Охотничья", 15.80, ["Томаты", "моцарелла", "охотничьи колбаски", "огурцы"], False, 4.4, 1100),
    ("Диабло", 14.90, ["Томаты", "моцарелла", "острый перец", "чили"], False, 4.9, 1500)
]
# Имена столбцов
columns = ["Name", "Price", "Ingredients", "Is_Vegetarian", "Rating", "Orders_Count"]

# Создаем pandas-on-Spark DataFrame
psdf = ps.DataFrame(data, columns=columns) # Указывайте columns при создании DataFrame из списка списков

# print(psdf)
# print("Инфо о DataFrame:")
# psdf.info()


# print("\nPandas API --> pandas:")
# pdf = psdf.to_pandas()
# print(type(pdf))
#
# print("pandas --> Pandas API:")
# psdf = ps.from_pandas(pdf)
# print(type(psdf))
#
# print("Pandas API --> Spark API:")
# sdf = psdf.to_spark()
# print(type(sdf))
#
# print("Spark API --> Pandas API:")
# psdf = sdf.pandas_api()
# print(type(psdf))

avg_price = ps.sql(
    """SELECT Is_Vegetarian, 
ROUND(AVG(Price),2) AS avg_price 
FROM {df} 
GROUP BY Is_Vegetarian 
ORDER BY avg_price DESC""", df=psdf)

avg_price.spark.explain()
print(avg_price)