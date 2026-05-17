from pyspark.sql import SparkSession
from pyspark.ml.feature import Binarizer, Imputer, PolynomialExpansion, VectorAssembler, SQLTransformer
spark = SparkSession.builder.appName("Binarizer").getOrCreate()

# Превращает непрерывные величины в 0 или 1 по порогу. Пример: Разделить возраст на "молодой" (<=30) и "зрелый" (>30).

# Создаем данные: (1, 20 лет), (2, 45 лет), (3, 30 лет)
data = [(1, 20.0), (2, 45.0), (3, 30.0)]
df = spark.createDataFrame(data, ["id", "age"])

# Если age > 30.0 -> 1.0, иначе -> 0.0
binarizer = Binarizer(inputCol="age", outputCol="is_senior", threshold=30.0)
df_bin = binarizer.transform(df)

df_bin.show()

spark.stop()


spark = SparkSession.builder.appName("Imputer").getOrCreate()

# Необходим для данных с пропусками. Модели ML падают, если во входных данных есть null значения.
# Imputer заменяет пропуски на среднее (mean) или медиану (median).

# Данные с пропуском
data = [(1, 20.0), (2, None), (3, 40.0)]
df = spark.createDataFrame(data, ["id", "age"])

# Заполняем пропуски в 'age' средним значением
imputer = Imputer(inputCols=["age"], outputCols=["age_imputed"])
imputer.setStrategy("mean")

model = imputer.fit(df)
df_filled = model.transform(df)

df_filled.show()

spark.stop()


spark = SparkSession.builder.appName("PolynomialExpansion").getOrCreate()
# Генерирует взаимодействия признаков (например, x, x^2, y, xy, y^2)
# Данные: (x=2.0, y=1.0)
data = [(1, 2.0, 1.0), (2, 3.0, 2.0)]
df = spark.createDataFrame(data, ["id", "x", "y"])

# Сначала собираем признаки в вектор
vec_assembler = VectorAssembler(inputCols=["x", "y"], outputCol="features")
df_vec = vec_assembler.transform(df)

# Генерируем полиномы степени 2
poly = PolynomialExpansion(inputCol="features", outputCol="polyFeatures", degree=2)
df_poly = poly.transform(df_vec)

df_poly.select("features", "polyFeatures").show(truncate=False)

spark.stop()


spark = SparkSession.builder.appName("SQLTransformer").getOrCreate()
# Позволяет писать любой SQL (SELECT ...) внутри пайплайна.
data = [(1, 20), (2, 45)]
df = spark.createDataFrame(data, ["id", "age"])

sql_trans = SQLTransformer(statement="SELECT *, (age * 12) AS age_months FROM __THIS__")
df_sql = sql_trans.transform(df)

df_sql.show()

spark.stop()