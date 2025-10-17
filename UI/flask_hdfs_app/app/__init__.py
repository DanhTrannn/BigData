from flask import Flask
from pyspark.sql import SparkSession
import os
def create_app():
    TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), 'templates')
    app = Flask(__name__,template_folder=TEMPLATES_DIR)
    app.secret_key = os.environ.get('FLASK_SECRET', 'change-me')

    spark = SparkSession.builder.appName('FlaskPySparkMovieCRUD').getOrCreate()
    app.spark = spark

    from .routes import main
    app.register_blueprint(main)

    return app
