#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, trim, col, avg, desc

spark = SparkSession.builder \
    .appName("Average Duration by Genre") \
    .getOrCreate()

input_path = "hdfs:///user/hadoop23133036/imdb"
output_path = "hdfs:///user/hadoop23133036/avg_duration_by_genre"

df = spark.read.csv(input_path, header=False, inferSchema=True)
df = df.toDF(
    "movie_name",
    "release_year",
    "metascore",
    "user_score",
    "average",
    "user_ratings",
    "duration_minutes",
    "genre"
)

df = df.filter(col("duration_minutes").isNotNull() & col("genre").isNotNull())
df = df.withColumn("genre", explode(split(col("genre"), r"\|")))
df = df.withColumn("genre", trim(col("genre")))
result = df.groupBy("genre").agg(
    avg("duration_minutes").alias("avg_duration")
).orderBy(desc("avg_duration"))
result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
print("Kết quả đã được lưu tại:", output_path)
spark.stop()

