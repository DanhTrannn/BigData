from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from markupsafe import Markup
from app.spark_utils import read_df, write_df
from flask import current_app as app
import pandas as pd
import plotly.express as px
import numpy as np
from scipy.stats import gaussian_kde
import plotly.graph_objects as go

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


    # Biểu đồ 3: So sánh tỷ lệ UserScore > MetaScore giữa IMDb và Metacritic
    try:
        df_ratio = pd.read_csv("data-visualization/ratio-userscore-vs-metascore.csv")
        labels = ["IMDb", "Metacritic"]
        ratios = df_ratio['ratio'].astype(float).fillna(0).tolist()
        if len(ratios) != len(labels):
            if len(ratios) < len(labels):
                labels = labels[:len(ratios)]
            else:
                extra = [f"Row {i+1}" for i in range(len(labels), len(ratios))]
                labels = labels + extra

        fig3 = px.bar(x=labels, y=ratios, labels={'x':'Source','y':'Ratio (User > Meta)'},
                      title='Tỷ lệ số phim UserScore > MetaScore (IMDb vs Metacritic)')
        fig3.update_traces(text=[f"{r:.3f}" for r in ratios], textposition='outside')
        chart3 = fig3.to_html(full_html=False)
    except Exception as e:
        fig3 = px.bar(x=[], y=[], title=f"Error loading ratio CSV: {e}")
        chart3 = fig3.to_html(full_html=False)


    # Biểu đồ 4: Histogram phân bố thời lượng trung bình các thể loại
    try:
        df4 = pd.read_csv("data-visualization/avg_duration_by_genre.csv")
        fig4 = px.histogram(
            df4,
            x="avg_duration",
            nbins=40,
            labels={"avg_duration": "Thời lượng trung bình (phút)", "count": "Số lượng thể loại"},
            title="Phân bố chi tiết thời lượng trung bình của các thể loại phim",
            color_discrete_sequence=["#0074D9"],
            opacity=0.85
        )
        x_vals = df4["avg_duration"].dropna().values
        kde = gaussian_kde(x_vals)
        x_grid = np.linspace(x_vals.min(), x_vals.max(), 200)
        y_kde = kde(x_grid) * len(x_vals) * (x_grid.max()-x_grid.min())/40  # scale to histogram
        fig4.add_trace(go.Scatter(x=x_grid, y=y_kde, mode='lines', name='Mật độ', line=dict(color='red', width=2, dash='dash')))

        fig4.update_traces(texttemplate='%{y}', textposition='outside', marker_line_color='black', marker_line_width=1.2, selector=dict(type='histogram'))
        fig4.update_layout(
            bargap=0.06,
            xaxis_title="Thời lượng trung bình (phút)",
            yaxis_title="Số lượng thể loại",
            xaxis=dict(showgrid=True, gridcolor='lightgrey'),
            yaxis=dict(showgrid=True, gridcolor='lightgrey'),
            plot_bgcolor='white',
            margin=dict(l=60, r=20, t=60, b=40)
        )
        mean_val = np.mean(x_vals)
        max_val = np.max(x_vals)
        fig4.add_vline(x=mean_val, line_dash="dot", line_color="green", annotation_text=f"Trung bình: {mean_val:.1f}", annotation_position="top left")
        fig4.add_vline(x=max_val, line_dash="dot", line_color="orange", annotation_text=f"Lớn nhất: {max_val:.1f}", annotation_position="top right")
        chart4 = fig4.to_html(full_html=False)
    except Exception as e:
        fig4 = px.bar(x=[], y=[], title=f"Error loading avg_duration_by_genre.csv: {e}")
        chart4 = fig4.to_html(full_html=False)

    return render_template("dashboard.html", 
                           chart1=chart1, chart2=chart2, chart3=chart3, chart4=chart4)






