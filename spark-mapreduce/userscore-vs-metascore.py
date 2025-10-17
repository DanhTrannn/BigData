#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit

spark = SparkSession.builder \
    .appName("Userscore vs Metascore Ratio") \
    .getOrCreate()

input_path = "hdfs:///user/hadoop23133036/imdb"
output_path = "hdfs:///user/hadoop23133036/user_vs_meta_ratio"

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

df = df.filter((col("metascore").isNotNull()) & (col("user_score").isNotNull()))
df = df.withColumn("user_bigger_meta", when(col("user_score") > col("metascore"), lit(1)).otherwise(lit(0)))
agg_df = df.agg(
    count("*").alias("total"),
    count(when(col("user_bigger_meta") == 1, True)).alias("user_bigger_meta_count")
)
agg_df = agg_df.withColumn("ratio", col("user_bigger_meta_count") / col("total"))
agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
print("Kết quả đã được lưu tại thư mục:", output_path)
spark.stop()