from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (SparkSession.builder
         .master("local[*]")
         .appName('Pizza')
         .getOrCreate()
        )

data = [
    ("Маргарита", 12.50, ["Томаты", "моцарелла", "базилик"], True),
    ("Пепперони", 15.00, ["Томаты", "моцарелла", "пепперони"], False),
    ("Четыре сыра", 14.00, ["Моцарелла", "пармезан", "горгонзола", "чеддер"], True),
    ("Мясная", 16.50, ["Томаты", "моцарелла", "ветчина", "бекон", "салями"], False),
    ("Гавайская", 13.00, ["Томаты", "моцарелла", "ветчина", "ананасы"], False)
]

columns = ["Name", "Price", "Ingredients", "Is_Vegetarian"]

# Создаем DataFrame
df_pizzas = spark.createDataFrame(data, columns)

# Регистрируем DataFrame как временное представление
df_pizzas.createOrReplaceTempView("view_pizzas")

# # SQL-способ
# sqlWay = spark.sql("""
#     SELECT Is_Vegetarian, count(1) AS count
#     FROM view_pizzas
#     GROUP BY Is_Vegetarian
# """)
# print("SQL-запрос:")
# sqlWay.show()
#
# # DataFrame API способ
# dataFrameWay = df_pizzas \
#     .groupBy("Is_Vegetarian") \
#     .count()
# print("DataFrame API запрос:")
# dataFrameWay.show()
#
# print("План выполнения для SQL-запроса:")
# sqlWay.explain()
#
# print("План выполнения для DataFrame API:")
# dataFrameWay.explain()

# SQL-способ
print("SQL:")
tbl_sql = spark.sql("SELECT *, 'В наличии' as status FROM view_pizzas")
tbl_sql.show(3)

# DataFrame API способ
print("DataFrame API:")
tbl_df = df_pizzas.withColumn("status", F.lit('В наличии'))
tbl_df.show(3)

# Создаем дополнительный DataFrame с информацией о популярности пицц
popularity_data = [
    ("Маргарита", "Высокая"),
    ("Пепперони", "Средняя"),
    ("Четыре сыра", "Высокая"),
    ("Овощная", "Низкая") # Пицца, которой нет в основном списке
]
df_popularity = spark.createDataFrame(popularity_data, ["Name", "Popularity_Score"])
df_popularity.createOrReplaceTempView("view_popularity")


# SQL-способ
print("SQL FULL JOIN:")
sql_join = spark.sql('''
    SELECT
        p.Name,
        p.Price,
        pop.Popularity_Score
    FROM
        view_pizzas p
    FULL JOIN
        view_popularity pop
    ON
        p.Name = pop.Name
''')
sql_join.show()

# DataFrame API способ
print("DataFrame API FULL JOIN:")
join_df = (df_pizzas.join(df_popularity,
                         df_pizzas.Name == df_popularity.Name, # Условие соединения по столбцу 'Name'
                        'full')
                    .select(df_pizzas.Name, df_pizzas.Price, df_popularity.Popularity_Score)
            )
join_df.show()

# Регистрируем DataFrame как глобальное временное представление
df_pizzas.createOrReplaceGlobalTempView("global_view_pizzas")

# SQL-способ
spark.sql("SELECT * FROM global_temp.global_view_pizzas").show()

# DataFrame способ
spark.read.table("global_temp.global_view_pizzas").show()

# SQL-способ
spark.sql("DROP VIEW IF EXISTS global_temp.global_view_pizzas")

# DataFrame способ
spark.catalog.dropGlobalTempView("global_view_pizzas")

# Не забываем остановить SparkSession
spark.stop()