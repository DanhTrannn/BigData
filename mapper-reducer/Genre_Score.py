from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim, avg, count, round

# === Khởi tạo SparkSession ===
spark = SparkSession.builder \
    .appName("Average Score by Genre") \
    .getOrCreate()

# === Đọc dữ liệu ===
input_path = "hdfs:///user/hadoopcloud/movies"
df = spark.read.csv(input_path, header=False, inferSchema=True)

# Đổi tên cột nếu cần (giả sử dữ liệu có format JSON như trong ví dụ trước)
df = df.toDF("Movie_Name", "Release_Year", "Metascore", "Critic_Reviews", "User_Score", "User_Ratings","Duration_Minutes", "Genre", "Average")

df = df.filter((col("Genre").isNotNull()) & (col("Metascore").isNotNull()) & (col("User_Score").isNotNull()))
# === Map phase: tách genre thành từng dòng riêng ===
genre_df = df.withColumn("Genre", explode(split(col("Genre"), r"\||,")))  # hỗ trợ cả "|" hoặc ","
genre_df = genre_df.withColumn("Genre", trim(col("Genre"))).filter(col("Genre") != "")

# === Reduce phase: tính trung bình Metascore và đếm số phim cho mỗi thể loại ===
result = genre_df.groupBy("Genre").agg(
    round(avg("Metascore"), 2).alias("Avg_Metascore"),
    round(avg("User_Score"), 2).alias("Avg_User_Score"),
    count("*").alias("Movie_Count")   # 🧮 Đếm số lượng phim trong mỗi thể loại
).orderBy(col("Movie_Count").desc(), col("Avg_Metascore").desc(), col("Avg_User_Score").desc())

# === Ghi kết quả ra HDFS ===
output_path = "hdfs:///user/hadoopcloud/output/genre_score"
result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print("✅ Kết quả đã được lưu tại thư mục:", output_path)

spark.stop()