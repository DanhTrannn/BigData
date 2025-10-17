import pandas as pd
import re
import csv # để cấu hình ghi file CSV
import numpy as np

# Hàm chuyển đổi Duration từ chuỗi sang số phút
def duration_to_minutes(duration_str):
    hours = 0
    minutes = 0
    match_h = re.search(r"(\d+)\s*h", duration_str)
    match_m = re.search(r"(\d+)\s*m", duration_str)
    if match_h:
        hours = int(match_h.group(1))
    if match_m:
        minutes = int(match_m.group(1))
    return hours * 60 + minutes if hours + minutes > 0 else "N/A"


def normalize_required_genre(genre):
    if pd.isna(genre):
        return np.nan

    s = str(genre).strip()
    if s.lower() in ['không có', 'none', 'nan', '', 'no', 'null']:
        return np.nan
    

    s = s.replace('"', '').replace("'", '')
    s = re.sub(r'[\n\r\t]', ' ', s)
    s = re.sub(r'\s*,\s*', '|', s)
    s = re.sub(r'\s*\|\s*', '|', s)
    s = re.sub(r'\s{2,}', ' ', s)

    genre_list = [w.strip().title() for w in s.split('|') if w.strip()]
    return '|'.join(genre_list) if genre_list else np.nan

def fix_encoding(text):
    if text is None:
        return None
    try:
        return text.encode('latin1').decode('utf-8')
    except:
        return text

def normalize_value(x):
    if pd.isna(x):
        return None
    x = str(x).strip().upper()
    if x.endswith('M'):
        return float(x[:-1]) * 1_000_000
    elif x.endswith('K'):
        return float(x[:-1]) * 1_000
    else:
        return float(x)


def normalize_column(path_in, path_out):
# === Đọc dữ liệu gốc ===
    df = pd.read_csv(
        path_in,
        sep=",",
        header=0,
        encoding="latin1"
    )
    print(df.columns)

    # === Loại bỏ dòng thiếu dữ liệu quan trọng ===
    df = df.dropna(subset=[
        "Title", "Release_Year", "IMDb_Rating", "Votes", "Runtime", "Genre"])

    # === Sửa lỗi encoding tên phim ===
    df["Title"] = df["Title"].apply(fix_encoding)
    df["Title"] = df["Title"].str.replace('"', '').str.replace("'", '').str.replace(',', ';')

    df["IMDb_Rating"] = (pd.to_numeric(df["IMDb_Rating"], errors='coerce') / 10).round(2)

    # Loại bỏ dòng không thể chuyển sang số
    df = df.dropna(subset=["IMDb_Rating"])

    # === Chuẩn hóa Duration ===
    df["Runtime"] = df["Runtime"].apply(duration_to_minutes)
    df["Runtime"] = df["Runtime"].replace("N/A", np.nan)
    df = df.dropna(subset=["Runtime"])
    df = df.rename(columns={"Title": "Movie_Name", "Runtime": "Duration_Minutes"})

    # === Chuẩn hóa Genre ===
    df["Genre"] = df["Genre"].apply(normalize_required_genre)
    df["Genre"] = df["Genre"].replace("", np.nan)
    df = df.dropna(subset=["Genre"])


    df['Votes'] = df['Votes'].apply(normalize_value)
    # === Ghi ra file CSV sạch ===
    df.to_csv(
        path_out,
        index=False, encoding="utf-8-sig"
    )

    print(f"Đã chuẩn hóa dữ liệu và lưu thành công vào '{path_out}'")
    print(f"Tổng số dòng sau xử lý: {len(df)}")

if __name__ == "__main__":
    path_in = r"D:\BigData\Project\Python\BigData\data\imdb_movie_trends_50_movies_with_genre.csv"
    path_out = r"D:\BigData\Project\Python\BigData\clean-data\movie_data_clean.csv"
    normalize_column(path_in, path_out)
