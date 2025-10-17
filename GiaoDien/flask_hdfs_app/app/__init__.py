from flask import Flask
from pyspark.sql import SparkSession
import os
def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get('FLASK_SECRET', 'change-me')

    # SparkSession
    spark = SparkSession.builder.appName('FlaskPySparkMovieCRUD').getOrCreate()
    app.spark = spark

    # import routes
    from .routes import main
    app.register_blueprint(main)

    return app
