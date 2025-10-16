from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim, lower, count

spark = SparkSession.builder \
    .appName("Skills Count Job Analysis") \
    .getOrCreate()

input_path = "hdfs:///user/hadoop23133036/jobs"
df = spark.read.csv(input_path, header=False, inferSchema=True)
df = df.toDF("id","Job_Name", "Company", "Location", "Required_Skills", "min_salary", "max_salary")
df = df.filter((col("Required_Skills").isNotNull()))
skills_df = df.withColumn("Skill", explode(split(col("Required_Skills"), r"\|")))
skills_df = skills_df.withColumn("Skill", trim(col("Skill"))).filter(col("Skill") != "")
result = skills_df.groupBy("Skill").agg(count("*").alias("Count")).orderBy(col("Count").desc())
output_path = "hdfs:///user/hadoop23133036/output/skills_count"
result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
print("Kết quả đã được lưu tại thư mục:", output_path)
spark.stop()