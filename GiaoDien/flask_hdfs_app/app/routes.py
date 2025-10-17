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
@main.route('/dashboard')
def charts():
    # Bieu do cua danhhh
    df1 = pd.read_csv("data-visualization/genre_trend.csv", header=None, names=["Year", "Genre", "Count"])
    df1["Count"] = pd.to_numeric(df1["Count"], errors="coerce")
    df1 = df1.dropna(subset=["Count"])
    top_genres = df1.groupby("Genre")["Count"].sum().nlargest(5).index
    df1_top = df1[df1["Genre"].isin(top_genres)]
    fig1 = px.line(df1_top, x="Year", y="Count", color="Genre", markers=True,
                   title="Xu hướng thể loại phim qua các năm")
    chart1 = fig1.to_html(full_html=False)
    df = pd.read_csv(
    "data-visualization/top10meta.csv",
    header=None,
    names=["Movie_Name", "Release_Year", "Average", "Genre"]
    )

    # Ép kiểu cột Average về số
    df["Average"] = pd.to_numeric(df["Average"], errors="coerce")

    # Bỏ các dòng không có điểm trung bình
    df = df.dropna(subset=["Average"])

    # Sắp xếp theo điểm trung bình giảm dần và lấy Top 10 phim
    df_top10 = df.sort_values(by="Average", ascending=False).head(10)

    # Vẽ biểu đồ Top 10 phim có điểm cao nhất
    fig2 = px.bar(
        df_top10,
        x="Movie_Name",
        y="Average",
        color="Genre",
        title="Top 10 phim có điểm trung bình cao nhất",
        text="Average"
    )
    fig2.update_traces(textposition="outside")

    chart2 = fig2.to_html(full_html=False)


    # Bieu do cua khoa: 4 5 
    # --- Biểu đồ 3 (ví dụ: heatmap giả lập) ---
    data = {
        "Year": [2018, 2019, 2020, 2021, 2022],
        "Drama": [120, 150, 180, 210, 230],
        "Action": [100, 130, 160, 180, 200],
        "Comedy": [90, 120, 140, 170, 190],
    }
    df3 = pd.DataFrame(data)
    fig3 = px.imshow(df3.set_index("Year").T, text_auto=True, aspect="auto", 
                     title="Tăng trưởng từng thể loại theo năm")
    chart3 = fig3.to_html(full_html=False)

    return render_template("dashboard.html", 
                           chart1=chart1, chart2=chart2, chart3=chart3)






