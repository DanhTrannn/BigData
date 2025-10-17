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

# === Đọc dữ liệu gốc ===
df = pd.read_csv(
    r"D:\BigData\Project\Python\BigData\data\metacritic_movies.csv",
    sep=",",
    header=0,
    encoding="latin1"
)

# === Loại bỏ dòng thiếu dữ liệu quan trọng ===
df = df.dropna(subset=[
    "Movie_Name", "Release_Year", "Metascore", "User_Score", "Critic_Reviews", "User_Ratings"
])

# === Sửa lỗi encoding tên phim ===
df["Movie_Name"] = df["Movie_Name"].apply(fix_encoding)
df["Movie_Name"] = df["Movie_Name"].str.replace('"', '').str.replace("'", '').str.replace(',', ';')

# === Lọc các phim có quá ít đánh giá ===
df = df[(df["Critic_Reviews"] >= 4) & (df["User_Ratings"] >= 20)]

# === Chuẩn hóa dữ liệu số ===
df["Metascore"] = pd.to_numeric(df["Metascore"], errors="coerce") / 100
df["User_Score"] = pd.to_numeric(df["User_Score"], errors="coerce") / 10

# Loại bỏ dòng không thể chuyển sang số
df = df.dropna(subset=["Metascore", "User_Score"])

# Làm tròn 2 chữ số thập phân
df["Metascore"] = df["Metascore"].round(2)
df["User_Score"] = df["User_Score"].round(2)

# Trung bình 2 giá trị
df["Average"] = ((df["Metascore"] + df["User_Score"]) / 2).round(2)

# === Chuẩn hóa Duration ===
df["Duration_Minutes"] = df["Duration_Minutes"].apply(duration_to_minutes)

# === Chuẩn hóa Genre ===
df["Genre"] = df["Genre"].apply(normalize_required_genre)
df["Genre"] = df["Genre"].replace("", np.nan)
df = df.dropna(subset=["Genre"])

# === Ghi ra file CSV sạch ===
df.to_csv(
    r"D:\BigData\Project\Python\BigData\clean-data\movie_data_clean.csv",
    index=False, encoding="utf-8-sig"
)

print("Đã chuẩn hóa dữ liệu và lưu thành công vào 'clean-data/movie_data_clean2.csv'")
print(f"Tổng số dòng sau xử lý: {len(df)}")
