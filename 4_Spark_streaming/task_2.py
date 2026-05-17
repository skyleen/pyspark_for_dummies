from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType, BooleanType, IntegerType

spark = (SparkSession.builder
                    .appName("spark_streaming_2")
                    .config("spark.sql.session.timeZone", "UTC")
                    .getOrCreate()
        )

stations_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("has_charging_station", BooleanType(), True),
    StructField("charging_slots_available", IntegerType(), True)
])

df_stations = (spark.read
                    .format("csv")
                    .option("path", "./stations.csv")
                    .option("header", "true")
                    .schema(stations_schema)
                    .load())

stream_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("station_id", StringType(), True),
    StructField("bikes_available", IntegerType(), True),
    StructField("scooters_available", IntegerType(), True)
])

df_stream = (spark.readStream
                .format("json")
                .schema(stream_schema)
                .load("./source_bikeshare_stream"))


df_enriched = (df_stream.join(df_stations, "station_id", "left_outer")
                        .withColumn("is_low_stock", F.when((F.col("bikes_available") < 3) | (F.col("scooters_available") < 2),
                                                                    F.lit(True))
                                                              .otherwise(F.lit(False)))
               )

df = (df_enriched.withWatermark("ts", "2 minutes")
                  .groupBy(F.window("ts", "2 minutes", "1 minute"), F.col("zone"))
                  .agg(F.avg(F.col("bikes_available")).alias("avg_bikes"),
                       F.avg(F.col("scooters_available")).alias("avg_scooters"),
                       F.approx_count_distinct(F.when(F.col("is_low_stock"), F.col("station_id"))).alias("low_stock_stations"),
                       F.approx_count_distinct(F.when(F.col("has_charging_station"), F.col("station_id"))).alias("charging_stations"),
                       F.collect_set(F.when(F.col("is_low_stock"), F.col("station_name"))).alias("problem_stations")
                       )
                  .withColumn("alert_level", F.when(F.col("low_stock_stations") >= 5, F.lit("CRITICAL"))
                                                       .when(F.col("low_stock_stations").between(2,4), F.lit("WARNING"))
                                                       .otherwise(F.lit("OK")))
      )

query1 = (df.filter(F.col("alert_level").isin(["CRITICAL", "WARNING"]))
          .select("window.start", "window.end", "zone", "alert_level", "low_stock_stations", "problem_stations")
          .writeStream.format("console").outputMode("update")
          .start())

query2 = (df.withColumn("window_start", F.date_format(F.col("window.start"), "yyyy-MM-dd HH:mm"))
          .writeStream
          .format("parquet")
          .outputMode("append")
          .option("checkpointLocation", './checkpoint_parquet2')
          .option("path", "./output_bikeshare")
          .partitionBy("window_start", "zone")
          .start())

spark.streams.awaitAnyTermination()