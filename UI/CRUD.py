"""
Flask + PySpark Movie CRUD Dashboard
File: flask_pyspark_dashboard.py

This single-file app will:
- Create minimal Jinja2 HTML templates under ./templates if they don't exist
- Start a Flask app that uses a SparkSession to read/write movie data from/to HDFS
- Support: list (read), create, update, delete via web forms

Requirements:
- Python 3.8+
- Flask
- pyspark
- pandas (for small conversions)

Run (example):
    python flask_pyspark_dashboard.py

Before running, set HDFS path in HDFS_PATH variable or pass environment variable HDFS_PATH.
If running locally without HDFS, you can use local filesystem path (file:///...) for testing.

Note: This implementation writes CSV via Spark overwrite. In production consider using Parquet + Hive.
"""

import os
import tempfile
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_extract
import pandas as pd

HDFS_PATH = os.environ.get('HDFS_PATH', 'hdfs:///user/hadoopthanhdanh/input/metacritic_movies.csv')

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), 'templates')
if not os.path.exists(TEMPLATES_DIR):
    os.makedirs(TEMPLATES_DIR)

INDEX_HTML = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Movies Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    <div class="container py-4">
      <h1 class="mb-4">Metacritic Movies Dashboard</h1>

      <div class="mb-3">
        <a class="btn btn-primary" href="{{ url_for('create') }}">Thêm</a>
        <a class="btn btn-secondary" href="{{ url_for('sync_hdfs') }}">Đẩy lên HDFS</a>
      </div>

      {% with messages = get_flashed_messages() %}
        {% if messages %}
          <div class="alert alert-info">{% for m in messages %}{{ m }}<br>{% endfor %}</div>
        {% endif %}
      {% endwith %}
      <form method="get" class="mb-3" class="d-flex">
        <input type="text" name="search" value="{{ search }}" class="form-control me-2" placeholder="Search movie by name">
        <button class="btn btn-primary" type="submit">Tìm kiếm</button>
      </form>
      <table class="table table-striped table-bordered bg-white">
        <thead>
          <tr>
            <th>#</th>
            <th>Movie_Name</th>
            <th>Release_Year</th>
            <th>Metascore</th>
            <th>Average</th>
            <th>User_Score</th>
            <th>User_Ratings</th>
            <th>Duration_Minutes</th>
            <th>Genre</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
        {% for r in rows %}
          <tr>
            <td>{{ (page - 1) * per_page + loop.index }}</td>
            <td>{{ r.Movie_Name }}</td>
            <td>{{ r.Release_Year }}</td>
            <td>{{ r.Metascore }}</td>
            <td>{{ r.Average }}</td>
            <td>{{ r.User_Score }}</td>
            <td>{{ r.User_Ratings }}</td>
            <td>{{ r.Duration_Minutes }}</td>
            <td>{{ r.Genre }}</td>
            <td>
              <a class="btn btn-sm btn-outline-primary" href="{{ url_for('edit', title=r.Movie_Name) }}">Cập nhật</a>
              <a class="btn btn-sm btn-outline-danger" href="{{ url_for('delete', title=r.Movie_Name) }}" onclick="return confirm('Delete this movie?')">Xóa</a>
            </td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
      <nav aria-label="Page navigation example">
        <ul class="pagination">
          <li class="page-item {% if page <= 1 %}disabled{% endif %}">
            <a class="page-link" href="?search={{ search }}&page={{ page-1 }}">Previous</a>
          </li>
          {% for p in range(1, total_pages+1) %}
          <li class="page-item {% if p == page %}active{% endif %}">
          <a class="page-link" href="?search={{ search }}&page={{ p }}">{{ p }}</a>
          </li>
          {% endfor %}
          <li class="page-item {% if page >= total_pages %}disabled{% endif %}">
            <a class="page-link" href="?search={{ search }}&page={{ page+1 }}">Next</a>
          </li>
        </ul>
      </nav>
    </div>
  </body>
</html>
"""

CREATE_HTML = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Thêm phim</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    <div class="container py-4">
      <h1>Thêm phim</h1>
      <form method="post">
        <div class="mb-3">
          <label class="form-label">Movie_Name</label>
          <input class="form-control" name="Movie_Name" required>
        </div>
        <div class="mb-3">
          <label class="form-label">Release_Year</label>
          <input class="form-control" name="Release_Year">
        </div>
        <div class="mb-3">
          <label class="form-label">Metascore</label>
          <input class="form-control" name="Metascore">
        </div>
        <div class="mb-3">
          <label class="form-label">User Score</label>
          <input class="form-control" name="User_Score">
        </div>
        <div class="mb-3">
          <label class="form-label">User_Ratings</label>
          <input class="form-control" name="User_Ratings">
        </div>
        <div class="mb-3">
          <label class="form-label">Duration_Minutes</label>
          <input class="form-control" name="Duration_Minutes">
        </div>
        <div class="mb-3">
          <label class="form-label">Genre</label>
          <input class="form-control" name="Genre">
        </div>
        <button class="btn btn-primary">Save</button>
        <a class="btn btn-secondary" href="{{ url_for('index') }}">Cancel</a>
      </form>
    </div>
  </body>
</html>
"""


EDIT_HTML = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Thêm phim</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    <div class="container py-4">
      <h1>Thêm phim</h1>
      <form method="post">
        <div class="mb-3">
          <label class="form-label">Movie_Name</label>
          <input class="form-control" name="Movie_Name" value="{{ movie.Movie_Name }}" required>
        </div>
        <div class="mb-3">
          <label class="form-label">Release_Year</label>
          <input class="form-control" name="Release_Year">
        </div>
        <div class="mb-3">
          <label class="form-label">Metascore</label>
          <input class="form-control" name="Metascore">
        </div>
        <div class="mb-3">
          <label class="form-label">User Score</label>
          <input class="form-control" name="User_Score">
        </div>
        <div class="mb-3">
          <label class="form-label">User_Ratings</label>
          <input class="form-control" name="User_Ratings">
        </div>
        <div class="mb-3">
          <label class="form-label">Duration_Minutes</label>
          <input class="form-control" name="Duration_Minutes">
        </div>
        <div class="mb-3">
          <label class="form-label">Genre</label>
          <input class="form-control" name="Genre">
        </div>
        <button class="btn btn-primary">Save</button>
        <a class="btn btn-secondary" href="{{ url_for('index') }}">Cancel</a>
      </form>
    </div>
  </body>
</html>
"""

templates = {
    'index.html': INDEX_HTML,
    'create.html': CREATE_HTML,
    'edit.html': EDIT_HTML,
}
for name, content in templates.items():
    path = os.path.join(TEMPLATES_DIR, name)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)


app = Flask(__name__, template_folder=TEMPLATES_DIR)
app.secret_key = os.environ.get('FLASK_SECRET', 'change-me')


spark = SparkSession.builder.appName('FlaskPySparkMovieCRUD').getOrCreate()


def read_df():
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

    df = spark.read.option('header', False).schema(schema).csv(HDFS_PATH)
    return df
    # try:
    #     df = spark.read.option('header', True).option('inferSchema', True).csv(HDFS_PATH)
    #     return df
    # except Exception as e:
    #     from pyspark.sql.types import StructType, StructField, StringType
    #     schema = StructType([
    #         StructField('Movie_Name', StringType(), True),
    #         StructField('Release_Year', StringType(), True),
    #         StructField('Metascore', StringType(), True),
    #         StructField('User_Score', StringType(), True),
    #         StructField('Average', StringType(), True),
    #         StructField('User_Ratings', StringType(), True),
    #         StructField('Duration_Minutes', StringType(), True),
    #         StructField('Genre', StringType(), True),
    #     ])
    #     return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

def write_df(df):
    tmp = HDFS_PATH + '.tmp'
    df.coalesce(1).write.mode('overwrite').option('header', True).csv(tmp)
    import subprocess
    try:
        subprocess.run(['hdfs', 'dfs', '-rm', '-r', HDFS_PATH], check=False)
        subprocess.run(['hdfs', 'dfs', '-mv', tmp, HDFS_PATH], check=True)
    except Exception as e:
        print('Error while moving temp CSV on HDFS:', e)
        raise

def df_to_list(df):
    try:
        pdf = df.toPandas()
        pdf = pdf.fillna('')
        return pdf.to_dict(orient='records')
    except Exception as e:
        print('Error converting df to pandas:', e)
        return []

@app.route('/')
def index():
    search = request.args.get('search','').strip()
    page = int(request.args.get('page',1))
    per_page = 50

    df = read_df()
    pdf = df.toPandas().fillna('')

    if search:
        pdf = pdf[pdf['Movie_Name'].str.contains(search, case=False, na=False)]
        page = 1  # reset page khi search mới

    total = len(pdf)
    total_pages = (total + per_page - 1) // per_page
    start, end = (page-1)*per_page, page*per_page
    rows = pdf.iloc[start:end].to_dict(orient='records')

    return render_template('index.html', rows=rows, search=search, page=page, total_pages=total_pages, per_page=per_page)

@app.route('/create', methods=['GET', 'POST'])
def create():
    if request.method == 'POST':
        new_title = request.form.get('Movie_Name')
        release = request.form.get('Release_Year')
        metaScore = request.form.get('Metascore')
        userScore = request.form.get('User_Score')
        userRating = request.form.get('User_Ratings')
        duration = request.form.get('Duration_Minutes')
        genres = request.form.get('Genre')

        df = read_df()
        new_row = [(new_title, release, metaScore, userScore, userRating, duration, genres)]
        cols = ['Movie_Name','Release_Year','Metascore','User_Score', 'User_Ratings', 'Duration_Minutes','Genre']
        new_df = spark.createDataFrame(new_row, cols)
        combined = df.unionByName(new_df, allowMissingColumns=True)
        try:
            write_df(combined)
            flash('Movie added and synced to HDFS')
            return redirect(url_for('index'))
        except Exception as e:
            flash(f'Error saving to HDFS: {e}')
    return render_template('create.html')

@app.route('/edit/<path:title>', methods=['GET', 'POST'])
def edit(title):
    df = read_df()
    pdf = df.toPandas().fillna('')
    record = pdf[pdf['Movie_Name'] == title]
    if record.empty:
        flash('Movie not found')
        return redirect(url_for('index'))
    rec = record.iloc[0].to_dict()

    if request.method == 'POST':
        new_title = request.form.get('Movie_Name') or rec.get('Movie_Name')
        release = request.form.get('Release_Year') or rec.get('Release_Year')
        metaScore = request.form.get('Metascore') or rec.get('Metascore')
        userScore = request.form.get('User_Score') or rec.get('User_Score')
        userRating = request.form.get('User_Ratings') or rec.get('User_Ratings')
        duration = request.form.get('Duration_Minutes') or rec.get('Duration_Minutes')
        genres = request.form.get('Genre') or rec.get('Genre')

        pdf.loc[pdf['Movie_Name'] == title, ['Movie_Name','Release_Year','Metascore','User_Score', 'User_Ratings', 'Duration_Minutes','Genre']] = [new_title, release,metaScore,userScore,userRating,duration,genres]
        new_df = spark.createDataFrame(pdf)
        try:
            write_df(new_df)
            flash('Movie updated')
            return redirect(url_for('index'))
        except Exception as e:
            flash(f'Error writing to HDFS: {e}')

    return render_template('edit.html', movie=rec)

@app.route('/delete/<path:title>')
def delete(title):
    df = read_df()
    pdf = df.toPandas().fillna('')
    pdf2 = pdf[pdf['Movie_Name'] != title]
    new_df = spark.createDataFrame(pdf2)
    try:
        write_df(new_df)
        flash('Movie deleted')
    except Exception as e:
        flash(f'Error deleting: {e}')
    return redirect(url_for('index'))

@app.route('/sync')
def sync_hdfs():
    df = read_df()
    try:
        write_df(df)
        flash('Synced to HDFS')
    except Exception as e:
        flash(f'Error syncing: {e}')
    return redirect(url_for('index'))

@app.route('/api/movies')
def api_movies():
    df = read_df()
    rows = df_to_list(df)
    return jsonify(rows)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
