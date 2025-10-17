from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from markupsafe import Markup
from app.spark_utils import read_df, write_df
from flask import current_app as app
import pandas as pd
import plotly.express as px
import numpy as np
from scipy.stats import gaussian_kde
from plotly.subplots import make_subplots
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


    # Chart 5: So sánh điểm số Metacritic và IMDb
    df_meta = pd.read_csv("data-visualization/result1.csv")
    df_imdb = pd.read_csv("data-visualization/result2.csv")

    # Merge hai dataset
    df_merge = pd.merge(df_meta, df_imdb, on='Genre', suffixes=('_meta', '_imdb'))

    # Lấy top 10 thể loại phổ biến nhất dựa trên tổng số phim
    df_merge['Total_Movies'] = df_merge['Movie_Count_meta'] + df_merge['Movie_Count_imdb']
    df_top10 = df_merge.nlargest(10, 'Total_Movies')


    fig5 = make_subplots(rows=2, cols=1, 
                        subplot_titles=('Metacritic Scores & Movie Count', 'IMDb Scores & Movie Count'),
                        shared_xaxes=True,
                        vertical_spacing=0.15,
                        specs=[[{"secondary_y": True}],
                                [{"secondary_y": True}]])

    # Metacritic subplot
    fig5.add_trace(
        go.Bar(name='Meta Metascore', x=df_top10['Genre'], y=df_top10['Avg_Metascore_meta'],
                marker_color='#1f77b4', width=0.35, offset=-0.2),
        row=1, col=1
    )
    fig5.add_trace(
        go.Bar(name='Meta User Score', x=df_top10['Genre'], y=df_top10['Avg_User_Score_meta'],
                marker_color='#ff7f0e', width=0.35, offset=0.2),
        row=1, col=1
    )
    fig5.add_trace(
        go.Scatter(name='Meta Movie Count', x=df_top10['Genre'], y=df_top10['Movie_Count_meta'],
                    line=dict(color='#2ca02c', width=2), mode='lines+markers'),
        row=1, col=1, secondary_y=True
    )

    # IMDb subplot
    fig5.add_trace(
        go.Bar(name='IMDb Metascore', x=df_top10['Genre'], y=df_top10['Avg_Metascore_imdb'],
                marker_color='#1f77b4', width=0.35, offset=-0.2),
        row=2, col=1
    )
    fig5.add_trace(
        go.Bar(name='IMDb User Score', x=df_top10['Genre'], y=df_top10['Avg_User_Score_imdb'],
                marker_color='#ff7f0e', width=0.35, offset=0.2),
        row=2, col=1
    )
    fig5.add_trace(
        go.Scatter(name='IMDb Movie Count', x=df_top10['Genre'], y=df_top10['Movie_Count_imdb'],
                    line=dict(color='#2ca02c', width=2), mode='lines+markers'),
        row=2, col=1, secondary_y=True
    )

    # Update layout
    fig5.update_layout(
        title_text="So sánh điểm số và số lượng phim giữa Metacritic và IMDb",
        template='plotly_white',
        showlegend=True,
        height=800,
        xaxis_tickangle=-45,
        xaxis2_tickangle=-45
    )

    # Update y-axes titles
    fig5.update_yaxes(title_text="Điểm số", row=1, col=1)
    fig5.update_yaxes(title_text="Số lượng phim", row=1, col=1, secondary_y=True)
    fig5.update_yaxes(title_text="Điểm số", row=2, col=1)
    fig5.update_yaxes(title_text="Số lượng phim", row=2, col=1, secondary_y=True)
    chart5 = fig5.to_html(full_html=False)

    # Chart 6: So sánh tỉ lệ phim chất lượng cao theo năm (Metacritic vs IMDb)
    df_pct_meta = pd.read_csv("data-visualization/result3.csv", sep='\t', header=None, names=['Year','Value'])
    df_pct_imdb = pd.read_csv("data-visualization/result4.csv", sep='\t', header=None, names=['Year','Value'])

    # Chuyển kiểu dữ liệu
    df_pct_meta['Year'] = pd.to_numeric(df_pct_meta['Year'], errors='coerce').astype('Int64')
    df_pct_meta['Value'] = pd.to_numeric(df_pct_meta['Value'], errors='coerce')
    df_pct_imdb['Year'] = pd.to_numeric(df_pct_imdb['Year'], errors='coerce').astype('Int64')
    df_pct_imdb['Value'] = pd.to_numeric(df_pct_imdb['Value'], errors='coerce')

    # Merge theo Year để đảm bảo trục X thống nhất
    df_pct = pd.merge(df_pct_meta, df_pct_imdb, on='Year', how='outer', suffixes=('_meta', '_imdb'))
    df_pct = df_pct.sort_values('Year').dropna(subset=['Year'])


    fig6 = go.Figure()
    fig6.add_trace(go.Scatter(x=df_pct['Year'], y=df_pct['Value_meta'], mode='lines+markers', name='Metacritic', line=dict(color='#1f77b4')))
    fig6.add_trace(go.Scatter(x=df_pct['Year'], y=df_pct['Value_imdb'], mode='lines+markers', name='IMDb', line=dict(color='#ff7f0e')))

    fig6.update_layout(
        title='Comparison Movie Ratings of Metacritic and IMDB Over Years',
        xaxis_title='Year',
        yaxis_title='Percentage of High-Quality Movies (%)',
        template='plotly_white',
        height=450
    )

    chart6 = fig6.to_html(full_html=False)

    file1 = "data-visualization/top_movie_year_imdb.csv"    # Cluster 1
    file2 = "data-visualization/top_movie_year_movies.csv"  # Cluster 2

    df7 = pd.read_csv(file1)
    df8 = pd.read_csv(file2)

    # Thêm cột nguồn để phân biệt
    df7["Source"] = "IMDb"
    df8["Source"] = "Movies"

    # Gộp dữ liệu
    df_all = pd.concat([df7, df8])

    # Biểu đồ cột nhóm
    fig7 = px.bar(
        df_all,
        x="Release_Year",
        y="Metascore",
        color="Source",
        barmode="group",
        text="Movie_Name",
        title="So sánh Metascore & Tên phim giữa IMDb và Movies theo năm"
    )

    # Tùy chỉnh hiển thị hover và text
    fig7.update_traces(
        textposition="outside",
        hovertemplate=(
            "<b>Năm:</b> %{x}<br>"
            "<b>Phim:</b> %{text}<br>"
            "<b>Metascore:</b> %{y}<br>"
            "<b>Nguồn:</b> %{color}<extra></extra>"
        )
    )

    # Tùy chỉnh layout
    fig7.update_layout(
        xaxis_title="Năm phát hành",
        yaxis_title="Metascore",
        title_x=0.5,
        legend_title="Nguồn dữ liệu",
        bargap=0.3,
        height=600
    )

    chart7 = fig7.to_html(full_html=False)


    file3 = "data-visualization/duration_avg.csv"
    df_duration = pd.read_csv(file3)

    fig8 = px.pie(
        df_duration,
        names='duration',
        values='Average',
        title='Tỷ lệ điểm trung bình theo độ dài phim',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig8.update_traces(textinfo='percent+label', pull=[0.05]*len(df_duration))
    fig8.update_layout(title_x=0.5)
    chart8 = fig8.to_html(full_html=False)
    return render_template("dashboard.html", 
                           chart1=chart1, chart2=chart2, chart3=chart3, chart4=chart4, chart5=chart5, chart6=chart6, chart7 = chart7, chart8 = chart8)






