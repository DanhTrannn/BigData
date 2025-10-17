import os

HDFS_PATH = os.environ.get(
    'HDFS_PATH',
    'clean-data\movie_data_clean.csv'
)

SECRET_KEY = os.environ.get('FLASK_SECRET', 'change-me')