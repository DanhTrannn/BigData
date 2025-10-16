import pandas as pd
import re
import csv  # để cấu hình ghi file CSV

# === 1Đọc dữ liệu gốc ===
df = pd.read_csv(
    r"C:\Users\Admin\Desktop\BigData\data\metacritic_games_extended.csv",
    sep=",",
    header=0
)

# === Hàm xử lý và trích xuất số từ chuỗi kiểu "Based on 75 Critics" ===
def extract_number(value):
    if pd.isna(value):
        return None
    match = re.search(r"(\d+)", str(value))
    return int(match.group(1)) if match else None

df["Critic_reviews"] = df["Critic_reviews"].apply(extract_number)
df["User_ratings"] = df["User_ratings"].apply(extract_number)

# === Loại bỏ dòng thiếu dữ liệu quan trọng ===
df = df.dropna(subset=[
    "Name_game", "Release", "Metascore", "User_Score", "Critic_reviews", "User_ratings"
])

# === Lọc các game có quá ít đánh giá ===
# Giữ lại game có >= 5 review từ chuyên gia và >= 50 đánh giá từ người dùng
df = df[
    (df["Critic_reviews"] >= 5) &
    (df["User_ratings"] >= 50)
]

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

df["Platform"] = (
    df["Platform"]
    .astype(str)
    .str.replace(",", "|")
    .str.replace(" ", "", regex=False)
)

# === Ghi ra file CSV sạch ===
df.to_csv(
    r"C:\Users\Admin\Desktop\BigData\clean-data\game_data_clean.csv",
    index=False,
    quoting=csv.QUOTE_NONE,  # không có dấu ngoặc kép ""
    escapechar='\\'           # tránh lỗi nếu có dấu phẩy trong dữ liệu
)

print("Đã chuẩn hóa dữ liệu và lưu thành công vào 'clean-data/game_data_clean.csv'")
print(f"Tổng số dòng sau xử lý: {len(df)}")