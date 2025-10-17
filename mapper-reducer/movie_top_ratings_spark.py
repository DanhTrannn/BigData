from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Top Movies by Average Rating") \
    .getOrCreate()

input_path = "hdfs:///user/hadoopthanhdanh/movies"
output_path = "hdfs:///user/hadopthanhdanh/output/top_movies"

df = spark.read.csv(input_path, header=False, inferSchema=True)

df = df.toDF("Movie_Name", "Release_Year", "Metascore", "User_Score",
             "Average", "User_Ratings", "Duration_Minutes", "Genre")

important_cols = ["Movie_Name", "Release_Year", "Average", "Genre"]
df_filtered = df.select(*important_cols).filter(col("Average").isNotNull())

top_movies = df_filtered.orderBy(col("Average").desc()).limit(10)

top_movies.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print("Kết quả top phim đã được lưu tại:", output_path)

spark.stop()
