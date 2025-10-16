import pandas as pd
import re
import csv  # để cấu hình ghi file CSV

df = pd.read_csv(
    r"data\metacritic_games_extended.csv",
    sep=",",
    header=0
)

def extract_number(value):
    if pd.isna(value):
        return None
    match = re.search(r"(\d+)", str(value))
    return int(match.group(1)) if match else None

df["Critic_reviews"] = df["Critic_reviews"].apply(extract_number)
df["User_ratings"] = df["User_ratings"].apply(extract_number)

df = df.dropna(subset=[
    "Name_game", "Release", "Metascore", "User_Score", "Critic_reviews", "User_ratings"
])

# Giữ lại game có >= 5 review từ chuyên gia và >= 50 đánh giá từ người dùng
df = df[
    (df["Critic_reviews"] >= 5) &
    (df["User_ratings"] >= 50)
]

df["Metascore_norm"] = pd.to_numeric(df["Metascore"], errors="coerce") / 100
df["User_Score_norm"] = pd.to_numeric(df["User_Score"], errors="coerce") / 10

# Loại bỏ dòng không thể chuyển sang số
df = df.dropna(subset=["Metascore_norm", "User_Score_norm"])

# Làm tròn 2 chữ số thập phân
df["Metascore_norm"] = df["Metascore_norm"].round(2)
df["User_Score_norm"] = df["User_Score_norm"].round(2)

# Trung bình 2 giá trị
df["Average_norm"] = ((df["Metascore_norm"] + df["User_Score_norm"]) / 2).round(2)


# Thay dấu "," bằng "|" để tránh lỗi tách cột khi import CSV vào Spark/HDFS
df["Platform"] = (
    df["Platform"]
    .astype(str)
    .str.replace(",", "|")
    .str.replace(" ", "", regex=False)
)

df.to_csv(
    r"clean-data\game_data_clean.csv",
    index=False,
    quoting=csv.QUOTE_NONE,  # không có dấu ngoặc kép ""
    escapechar='\\'           # tránh lỗi nếu có dấu phẩy trong dữ liệu
)

print(" Đã chuẩn hóa dữ liệu và lưu thành công vào 'clean-data/game_data_clean.csv'")
