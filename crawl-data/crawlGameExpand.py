import requests
from bs4 import BeautifulSoup
import time
import random
import csv
import re
import os

BASE_URL = "https://www.metacritic.com"
LIST_URL = "https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2025&page={}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/123.0 Safari/537.36"
}

def get_html(url):
    """Tải nội dung HTML từ URL"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Lỗi khi tải {url}: {e}")
        return None


def get_game_details(game_url):
    """Lấy thông tin chi tiết từng game"""
    html = get_html(game_url)
    if not html:
        return ("N/A",) * 8

    soup = BeautifulSoup(html, "html.parser")

    # --- Tên game ---
    title_div = soup.find("div", {"data-testid": "hero-title"})
    name = title_div.find("h1").get_text(strip=True) if title_div else "N/A"

    # --- Năm phát hành ---
    release_year = "N/A"
    for div in soup.find_all("div", class_="g-text-xsmall"):
        bold_span = div.find("span", class_="g-text-bold")
        if bold_span and "Released On" in bold_span.get_text():
            date_span = div.find("span", class_="u-text-uppercase")
            if date_span:
                match = re.search(r"\d{4}", date_span.get_text(strip=True))
                if match:
                    release_year = match.group(0)
            break

    # --- Metascore ---
    metascore_div = soup.find("div", class_="c-siteReviewScore_background-critic_medium")
    metascore = metascore_div.get_text(strip=True) if metascore_div else "N/A"

    # --- Số critic reviews ---
    critic_tag = soup.select_one("span.c-productScoreInfo_reviewsTotal span")
    critic_reviews = critic_tag.get_text(strip=True) if critic_tag else "N/A"

    # --- User Score ---
    user_score_div = soup.find("div", class_="c-siteReviewScore_user")
    user_score = user_score_div.get_text(strip=True) if user_score_div else "N/A"

    # --- Số user ratings ---
    user_tag = soup.select_one("div.c-productScoreInfo_text a[data-testid='user-path'] span")
    user_ratings = user_tag.get_text(strip=True) if user_tag else "N/A"

    # --- Thể loại (Genres) ---
    genre = "N/A"
    genre_list_container = soup.find("ul", class_=re.compile("c-genreList"))
    if genre_list_container:
        genre_spans = genre_list_container.find_all("span", class_="c-globalButton_label")
        if genre_spans:
            genre = ", ".join([g.get_text(strip=True) for g in genre_spans])

    # --- Nền tảng (Platforms) ---
    platform = "N/A"
    platform_container = soup.find("div", class_=re.compile("c-gameDetails_Platforms"))
    if platform_container:
        platform_items = platform_container.find_all("li", class_=re.compile("c-gameDetails_listItem"))
        if platform_items:
            platform = ", ".join([p.get_text(strip=True) for p in platform_items])

    return name, release_year, metascore, critic_reviews, user_score, user_ratings, genre, platform


def crawl_metacritic_games(pages=1):
    """Crawl danh sách game theo số trang"""
    games_data = []

    for page in range(2, pages + 1):
        print(f"\nCrawling page {page} ...")
        url = LIST_URL.format(page)
        html = get_html(url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        game_cards = soup.find_all("div", {"data-testid": "filter-results"})

        for card in game_cards:
            try:
                link_tag = card.find("a", href=True)
                game_url = BASE_URL + link_tag["href"] if link_tag else None

                if game_url:
                    (name, release_year, metascore, critic_reviews,
                     user_score, user_ratings, genre, platform) = get_game_details(game_url)
                else:
                    name = release_year = metascore = critic_reviews = user_score = user_ratings = genre = platform = "N/A"

                games_data.append({
                    "Name_game": name,
                    "Release": release_year,
                    "Metascore": metascore,
                    "Critic_reviews": critic_reviews,
                    "User_Score": user_score,
                    "User_ratings": user_ratings,
                    "Genre": genre,
                    "Platform": platform
                })

                print(f"{name} | {metascore} | {critic_reviews} | {user_score} | "
                      f"{user_ratings} | {genre} | {platform} | {release_year}")

                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                print(f"Lỗi khi xử lý game trên trang {page}: {e}")

    return games_data


def save_to_csv(data, filename="data/metacritic_games_extended.csv"):
    """Lưu dữ liệu vào file CSV"""
    keys = ["Name_game", "Release", "Metascore", "Critic_reviews",
            "User_Score", "User_ratings", "Genre", "Platform"]

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
    all_games = crawl_metacritic_games(pages=100)
    save_to_csv(all_games)