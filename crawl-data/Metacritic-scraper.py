import requests
from bs4 import BeautifulSoup
import time
import random
import csv
import re
import os

BASE_URL = "https://www.metacritic.com"
LIST_URL = "https://www.metacritic.com/browse/movie/?releaseYearMin=2018&releaseYearMax=2025&page={}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/123.0 Safari/537.36"
}

# --- Hàm tải HTML ---
def get_html(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Lỗi khi tải {url}: {e}")
        return None

# --- Chuyển Duration "1 h 25 m" sang phút ---
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

# --- Lấy chi tiết từng phim ---
def get_movie_details(movie_url):
    html = get_html(movie_url)
    if not html:
        return ["N/A"]*7

    soup = BeautifulSoup(html, "html.parser")

    
    metascore_div = soup.find("div", class_="c-siteReviewScore_background-critic_medium")
    metascore = metascore_div.get_text(strip=True) if metascore_div else "N/A"
    # --- Critic Reviews ---
    critic_reviews = "N/A"
    critic_span = soup.select_one("span.c-productScoreInfo_reviewsTotal a[href*='critic']")
    if critic_span:
        match = re.search(r'\d+', critic_span.get_text())
        if match:
            critic_reviews = match.group(0)

    # --- User Score ---
    user_score = "N/A"
    user_score_div = soup.select_one("div.c-siteReviewScore_background-user span")
    if user_score_div:
        user_score = user_score_div.get_text(strip=True)

    # --- User Ratings ---
    user_ratings = "N/A"
    user_span = soup.select_one("span.c-productScoreInfo_reviewsTotal a[href*='user']")
    if user_span:
        match = re.search(r'\d+', user_span.get_text())
        if match:
            user_ratings = match.group(0)

    # --- Release Year ---
    release_year = "N/A"
    release_div = soup.find("span", text="Release Date")
    if release_div:
        sibling = release_div.find_next_sibling("span")
        if sibling:
            match = re.search(r"\d{4}", sibling.get_text(strip=True))
            if match:
                release_year = match.group(0)

    # --- Duration ---
    duration = "N/A"
    duration_div = soup.find("span", text="Duration")
    if duration_div:
        sibling = duration_div.find_next_sibling("span")
        if sibling:
            duration = sibling.get_text(strip=True)

    # --- Genre ---
    genre = "N/A"
    genre_ul = soup.find("ul", class_="c-genreList")
    if genre_ul:
        genre_spans = genre_ul.find_all("span", class_="c-globalButton_label")
        if genre_spans:
            genre = ", ".join([g.get_text(strip=True) for g in genre_spans])

    return metascore,critic_reviews, user_score, user_ratings, duration, genre, release_year

# --- Crawl danh sách phim ---
def crawl_metacritic_movies(pages=1):
    movies_data = []

    for page in range(1,pages+1):
        print(f"\nCrawling page {page} ...")
        url = LIST_URL.format(page)
        html = get_html(url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        movie_cards = soup.find_all("div", {"data-testid": "filter-results"})

        for card in movie_cards:
            try:
                # Tên phim
                name_span = card.select_one("h3.c-finderProductCard_titleHeading span:nth-of-type(2)")
                name = name_span.get_text(strip=True) if name_span else "N/A"
                # User Score (tạm thời từ list page)
                user_score_div = card.select_one("div.c-siteReviewScore_user span")
                user_score_list = user_score_div.get_text(strip=True) if user_score_div else "N/A"

                # Link chi tiết phim
                link_tag = card.find("a", href=True)
                movie_url = BASE_URL + link_tag["href"] if link_tag else None

                # Lấy thêm tất cả chi tiết từ trang phim
                metascore, critic_reviews, user_score, user_ratings, duration, genre, release_year = ("N/A",)*7
                if movie_url:
                    metascore, critic_reviews, user_score, user_ratings, duration, genre, release_year = get_movie_details(movie_url)

                movies_data.append({
                    "Movie_Name": name,
                    "Release_Year": release_year,
                    "Metascore": metascore,
                    "Critic_Reviews": critic_reviews,
                    "User_Score": user_score,
                    "User_Ratings": user_ratings,
                    "Duration_Minutes": duration,
                    "Genre": genre
                })

                print(f"{name} | {release_year} | {user_score} | {metascore} | {duration} | {genre}")

                # Sleep ngẫu nhiên để tránh bị block
                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                print(f"Lỗi khi xử lý phim trên trang {page}: {e}")

    return movies_data

# --- Lưu CSV ---
def save_to_csv(data, filename="data/metacritic_movies.csv"):
    keys = ["Movie_Name", "Release_Year", "Metascore", "Critic_Reviews",
            "User_Score", "User_Ratings", "Duration_Minutes", "Genre"]

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if not file_exists:
            writer.writeheader()
        for row in data:
            writer.writerow(row)

    print(f"\nDữ liệu đã được lưu/ghi thêm vào {filename}")


if __name__ == "__main__":
    all_movies = crawl_metacritic_movies(pages=1)  # crawl 5 trang ví dụ
    save_to_csv(all_movies)
