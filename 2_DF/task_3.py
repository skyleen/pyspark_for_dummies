from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("df_task_3")
         .getOrCreate())

df_species = spark.read.parquet("/Users/n.shikhaleva/Downloads/star_wars/species.parquet")
df_characters = spark.read.parquet("/Users/n.shikhaleva/Downloads/star_wars/characters.parquet")
df_organizations = spark.read.parquet("/Users/n.shikhaleva/Downloads/star_wars/organizations.parquet")

# dedup
df_species = df_species.dropDuplicates(subset=["name","classification", "designation", "average_height"])
df_characters = df_characters.dropDuplicates(subset=["name", "species", "homeworld", "year_born"])

# df_species.show()
# df_characters.show()
# df_organizations.show()

# 1 Распространенность видов и их классификаций:
# Посчитайте количество персонажей для каждого вида (characters.species),
# а также для каждой биологической классификации (species.classification).
# Для классификаций вам потребуется объединить таблицы characters и species.

# Вывод 1: название вида (species), количество персонажей (character_count). Отсортируйте по количеству персонажей по убыванию. Вывести 10 первых строк.
(df_characters.groupBy('species')
              .agg(F.count('*').alias('character_count'))
              .sort(F.col('character_count'), ascending=False)
              .show(10))
# Вывод 2: биологическая классификация (classification), общее количество персонажей (total_character_count). Отсортируйте по количеству персонажей по убыванию.
(df_species.join(df_characters, df_species.name == df_characters.species, 'left')
          .groupBy('classification')
          .agg(F.count('*').alias('total_character_count'))
          .sort(F.col('total_character_count'), ascending=False)
          .show()
 )

# 2 Средний рост по классификации видов: Рассчитайте средний рост (average_height) для каждой classification в таблице species.
# Округлите результат до одного знака после запятой.
# Вывод: классификация (classification), средний рост (average_height_class). Отсортируйте по среднему росту, по убыванию.
(df_species
     .groupBy(F.col('classification'))
     .agg(F.round(F.avg(F.col('average_height')), 1).alias('average_height_class'))
     .sort(F.col('average_height_class'), ascending=False)
     .show())

# 3 Члены Ордена Джедаев и Ситхов: Получите список всех лидеров и членов организаций "Jedi Order" и "Sith Order".
# Предварительно необходимо преобразовать столбцы leader и members в формат Array.
# Вывод: название организации (name), имя члена организации (name_member). Отсортируйте по названию организации.
(df_organizations.where(F.col('name').isin('Sith Order', 'Jedi Order'))
                 .select(F.col('name'), F.explode_outer(F.concat((F.split(F.col('leader'), ',')),
                                                                        F.split(F.col('members'), ','))).alias('name_member'))
                 .withColumn('name_member', F.trim(F.col('name_member')))
                 .orderBy('name')
                 .show(truncate=False))
# 4 Топ-5 старейших и топ-5 самых юных персонажей: Определите 5 персонажей имеющих максимальное и минимальное значение year_born.
# Исключите персонажей в отсутствующим возрастом.
# Вывод 1: 5 старейших персонажей. Имя персонажа (name), год рождения (year_born).
(df_characters.where(F.col('year_born').isNotNull())
              .select(F.col('name'), F.col('year_born'))
              .sort(F.col('year_born'), ascending=False)
              .show(5))

# Вывод 2: 5 самых юных персонажей. Имя персонажа (name), год рождения (year_born).
(df_characters.where(F.col('year_born').isNotNull())
              .select(F.col('name'), F.col('year_born'))
              .sort(F.col('year_born'))
              .show(5))

# 5 Персонажи с экстремальным индексом массы тела: Найдите 5 персонажей с максимальным и 5 персонажей с минимальным индексом массы тела (ИМТ).
# Раcсчитайте ИМТ: weight / height**2. Исключите NULL из вывода.
# Вывод 1: 5 персонажей с максимальным ИМТ
(df_characters.withColumn('bmi', F.round((F.col('weight') / (F.col('height')**2)) ,2))
              .filter(F.col('bmi').isNotNull())
              .sort(F.col('bmi'), ascending=False)
              .show(5))

# Вывод 2: 5 персонажей с минимальным ИМТ
(df_characters.withColumn('bmi', F.round((F.col('weight') / (F.col('height')**2)) ,2))
              .filter(F.col('bmi').isNotNull())
              .sort(F.col('bmi'))
              .show(5))

spark.stop()