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
    
def rename_columns(df):
    rename_map = {
    'Title': 'Movie_Name',
    'Year': 'Release_Year',
    'IMDb_Rating': 'User_Score',      
    'Votes': 'User_Ratings',
    'Metascore': 'Metascore',
    'Runtime': 'Duration_Minutes',
    'Genre': 'Genre'
    }

    df = df.rename(columns=rename_map)
    
    return df

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
def clean_columns(path_input=None, path_output=None, latin = False):
# === Đọc dữ liệu gốc ===
    if latin:
        df = pd.read_csv(
            path_input,
            sep=",",
            header=0,
            encoding="latin1",
        )
    else:
        df = pd.read_csv(
            path_input,
            sep=",",
            header=0,
            encoding="utf-8",
        )
    # === Đổi tên cột ===
    df = rename_columns(df)

    # === Loại bỏ dòng thiếu dữ liệu quan trọng ===
    df = df.dropna(subset=[
        "Movie_Name", "Release_Year", "Metascore", "User_Score", "User_Ratings"
    ])

    # === Sửa lỗi encoding tên phim ===
    df["Movie_Name"] = df["Movie_Name"].apply(fix_encoding) 
    df["Movie_Name"] = df["Movie_Name"].str.replace('"', '').str.replace("'", '').str.replace(',', ';')


    # === Chuẩn hóa dữ liệu số ===
    df["Metascore"] = pd.to_numeric(df["Metascore"], errors="coerce") / 100
    df["User_Score"] = pd.to_numeric(df["User_Score"], errors="coerce") / 10
    
    # === Lọc các phim có quá ít đánh giá ===
    df["User_Ratings"] = df["User_Ratings"].apply(normalize_value)
    df["User_Ratings"] = df["User_Ratings"].astype(int)
    df = df[df["User_Ratings"] >= 20]

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

    df['Release_Year'] = (
        df['Release_Year']
        .astype(str)
        .str.extract(r'(\d{4})')  # lấy 4 chữ số đầu tiên nếu có text kèm
        .astype(float)
        .astype('Int64')  # giữ được NaN
    )
    df.dropna(subset=['Release_Year'], inplace=True)
    df['Release_Year'] = df['Release_Year'].astype(int)

    # === Loại bỏ cột không cần thiết ===
    if 'Critic_Reviews' in df.columns:
        df.drop(columns=['Critic_Reviews'], inplace=True)
        
    # Sắp xếp lại cột theo đúng thứ tự mong muốn
    df = df[[
        'Movie_Name', 'Release_Year', 'Metascore', 'User_Score',
        'Average', 'User_Ratings', 'Duration_Minutes', 'Genre'
    ]]

    # === Ghi ra file CSV sạch ===
    df.to_csv(
        path_output,
        index=False, encoding="utf-8-sig"
    )
    print(f"Đã chuẩn hóa dữ liệu và lưu thành công vào '{path_output}'")
    print(f"Tổng số dòng sau xử lý: {len(df)}")

if __name__ == "__main__":
    clean_columns("D:\\BigData\\Project\\Python\\BigData\\data\\metacritic_movies.csv", "D:\\BigData\\Project\\Python\\BigData\\clean-data\\movie_data_clean.csv", latin=True)
    clean_columns(r"D:\BigData\Project\Python\BigData\data\imdb_movie_trends_1550_movies_with_genre.csv", r"D:\BigData\Project\Python\BigData\clean-data\imdb_movie_trends_1550_clean.csv", latin=False)