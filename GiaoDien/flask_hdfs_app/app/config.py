import os

HDFS_PATH = os.environ.get(
    'HDFS_PATH',
    'D:\Hocky1-nam3\BigData\Project\data\metacritic_movies.csv'
)

SECRET_KEY = os.environ.get('FLASK_SECRET', 'change-me')