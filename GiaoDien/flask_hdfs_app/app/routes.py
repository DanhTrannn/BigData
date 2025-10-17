from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from markupsafe import Markup
from app.spark_utils import read_df, write_df
from flask import current_app as app
import pandas as pd
import plotly.express as px

main = Blueprint('main', __name__)

@main.route('/')
def index():
    search = request.args.get('search','').strip()
    page = int(request.args.get('page',1))
    per_page = 50

    df = read_df(app.spark)
    pdf = df.toPandas().fillna('')

    if search:
        pdf = pdf[pdf['Movie_Name'].str.contains(search, case=False, na=False)]
        page = 1

    total = len(pdf)
    total_pages = (total + per_page - 1) // per_page
    start, end = (page-1)*per_page, page*per_page
    rows = pdf.iloc[start:end].to_dict(orient='records')

    return render_template('index.html', rows=rows, search=search, page=page, total_pages=total_pages, per_page=per_page)
@main.route('/sync')
def sync_hdfs():
    df = read_df()
    try:
        write_df(df)
        flash('Synced to HDFS')
    except Exception as e:
        flash(f'Error syncing: {e}')
    return redirect(url_for('index'))

@main.route('/create', methods=['GET', 'POST'])
def create():
    if request.method == 'POST':
        data = {col: request.form.get(col) for col in ['Movie_Name','Release_Year','Metascore','Critic_Reviews',
                                                       'User_Score','User_Ratings','Duration_Minutes','Genre']}
        df = read_df(app.spark)
        new_df = app.spark.createDataFrame([tuple(data.values())], data.keys())
        combined = df.unionByName(new_df, allowMissingColumns=True)
        try:
            write_df(combined, app.spark)
            flash('Movie added and synced to HDFS')
            return redirect(url_for('main.index'))
        except Exception as e:
            flash(f'Error saving to HDFS: {e}')
    return render_template('create.html')

@main.route('/edit/<path:title>', methods=['GET','POST'])
def edit(title):
    df = read_df(app.spark)
    pdf = df.toPandas().fillna('')
    record = pdf[pdf['Movie_Name'] == title]
    if record.empty:
        flash('Movie not found')
        return redirect(url_for('main.index'))
    rec = record.iloc[0].to_dict()

    if request.method == 'POST':
        for col in rec.keys():
            pdf.loc[pdf['Movie_Name'] == title, col] = request.form.get(col) or rec[col]
        new_df = app.spark.createDataFrame(pdf)
        try:
            write_df(new_df, app.spark)
            flash('Movie updated')
            return redirect(url_for('main.index'))
        except Exception as e:
            flash(f'Error writing to HDFS: {e}')

    return render_template('edit.html', movie=rec)

@main.route('/delete/<path:title>')
def delete(title):
    df = read_df(app.spark)
    pdf = df.toPandas().fillna('')
    pdf2 = pdf[pdf['Movie_Name'] != title]
    new_df = app.spark.createDataFrame(pdf2)
    try:
        write_df(new_df, app.spark)
        flash('Movie deleted')
    except Exception as e:
        flash(f'Error deleting: {e}')
    return redirect(url_for('main.index'))

# ---------- Charts ----------
# @main.route('/charts')
# def charts():
#     df = read_df(app.spark)
#     pdf = df.toPandas().fillna('')
#     genre_count = pdf['Genre'].value_counts().reset_index()
#     genre_count.columns = ['Genre', 'Count']

#     fig = px.bar(genre_count, x='Genre', y='Count', title='Số lượng phim theo Genre')
#     chart_html = Markup(fig.to_html(full_html=False))
#     return render_template('charts.html', chart_html=chart_html)

# ---------- API ----------
# @main.route('/api/movies')
# def api_movies():
#     df = read_df(app.spark)
#     return jsonify(df.fillna('').toPandas().to_dict(orient='records'))
