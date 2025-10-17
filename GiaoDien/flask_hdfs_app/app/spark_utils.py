from pyspark.sql import SparkSession
import subprocess

HDFS_BASE = "hdfs:///user/hadoop23133036/imdb"

def read_df(spark: SparkSession):
    # try:
    #     df = spark.read.option('header', True).option('inferSchema', True).csv(HDFS_BASE)
    #     return df
    # except Exception:
    #     from pyspark.sql.types import StructType, StructField, StringType
    #     schema = StructType([
    #         StructField('Movie_Name', StringType(), True),
    #         StructField('Release_Year', StringType(), True),
    #         StructField('Metascore', StringType(), True),
    #         StructField('Critic_Reviews', StringType(), True),
    #         StructField('User_Score', StringType(), True),
    #         StructField('User_Ratings', StringType(), True),
    #         StructField('Duration_Minutes', StringType(), True),
    #         StructField('Genre', StringType(), True),
    #     ])
    #     return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField('Movie_Name', StringType(), True),
        StructField('Release_Year', StringType(), True),
        StructField('Metascore', StringType(), True),
        StructField('User_Score', StringType(), True),
        StructField('Average', StringType(), True),
        StructField('User_Ratings', StringType(), True),
        StructField('Duration_Minutes', StringType(), True),
        StructField('Genre', StringType(), True),
    ])

    df = spark.read.option('header', False).schema(schema).csv(HDFS_BASE)
    return df


def write_df(df, spark: SparkSession):
    tmp = HDFS_BASE + '.tmp'
    df.coalesce(1).write.mode('overwrite').option('header', True).csv(tmp)
    try:
        subprocess.run(['hdfs', 'dfs', '-rm', '-r', HDFS_BASE], check=False)
        subprocess.run(['hdfs', 'dfs', '-mv', tmp, HDFS_BASE], check=True)
    except Exception as e:
        print('Error moving temp CSV on HDFS:', e)
        raise
