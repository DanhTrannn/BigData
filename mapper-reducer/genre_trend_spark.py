from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim, lower, count

spark = SparkSession.builder \
    .appName("Genre Trend Analysis") \
    .getOrCreate()

input_path = "hdfs:///user/hadoopthanhdanh/movies"
output_path = "hdfs:///user/hadoopthanhdanh/output/genre_trend"

df = spark.read.csv(input_path, header=False, inferSchema=True)

df = df.toDF("Movie_Name", "Release_Year", "Metascore", "User_Score",
             "Average", "User_Ratings", "Duration_Minutes", "Genre")

genre_df = df.withColumn("Genre", explode(split(col("Genre"), r"\|")))

genre_df = genre_df.withColumn("Genre", trim(lower(col("Genre"))))

result = genre_df.groupBy("Release_Year", "Genre").agg(count("*").alias("Movie_Count")) \
                 .orderBy(col("Release_Year").asc(), col("Movie_Count").desc())

result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print("Kết quả thống kê thể loại theo năm đã được lưu tại:", output_path)
spark.stop()
