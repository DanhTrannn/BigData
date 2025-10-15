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
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Lỗi khi tải {url}: {e}")
        return None

def get_game_details(game_url):
    html = get_html(game_url)
    if not html:
        return "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"

    soup = BeautifulSoup(html, "html.parser")

    # Tên game
    title_div = soup.find("div", {"data-testid": "hero-title"})
    name = title_div.find("h1").get_text(strip=True) if title_div else "N/A"

    # Release year
    release_year = "N/A"
    for div in soup.find_all("div", class_="g-text-xsmall"):
        bold_span = div.find("span", class_="g-text-bold")
        if bold_span and "Released On" in bold_span.get_text():
            date_span = div.find("span", class_="u-text-uppercase")
            if date_span:
                import re
                match = re.search(r"\d{4}", date_span.get_text(strip=True))
                if match:
                    release_year = match.group(0)
            break

    # Metascore
    metascore_div = soup.find("div", class_="c-siteReviewScore_background-critic_medium")
    metascore = metascore_div.get_text(strip=True) if metascore_div else "N/A"

    # Số critic reviews
    critic_tag = soup.select_one("span.c-productScoreInfo_reviewsTotal span")
    critic_reviews = critic_tag.get_text(strip=True) if critic_tag else "N/A"

    # User Score
    user_score_div = soup.find("div", class_="c-siteReviewScore_user")
    user_score = user_score_div.get_text(strip=True) if user_score_div else "N/A"

    # Số user ratings
    user_tag = soup.select_one("div.c-productScoreInfo_text a[data-testid='user-path'] span")
    user_ratings = user_tag.get_text(strip=True) if user_tag else "N/A"

    return name, release_year, metascore, critic_reviews, user_score, user_ratings

def crawl_metacritic_games(pages=1):
    games_data = []

    for page in range(51, pages + 1):
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
                    name, release_year, metascore, critic_reviews, user_score, user_ratings = get_game_details(game_url)
                else:
                    name = release_year = metascore = critic_reviews = user_score = user_ratings = "N/A"

                games_data.append({
                    "Tên game": name,
                    "Release raw": release_year,
                    "Metascore raw": metascore,
                    "Critic reviews raw": critic_reviews,
                    "User Score raw": user_score,
                    "User ratings raw": user_ratings
                })

                print(f"{name} | {metascore} | {critic_reviews} | {user_score} | {user_ratings} | {release_year}")
                time.sleep(random.uniform(1, 3))

            except Exception as e:
                print(f"Lỗi khi xử lý game trên trang {page}: {e}")

    return games_data


def save_to_csv(data, filename="metacritic_games_raw.csv"):
    keys = ["Tên game","Release raw","Metascore raw","Critic reviews raw","User Score raw","User ratings raw"]
    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if not file_exists:  # Chỉ ghi header nếu file chưa tồn tại
            writer.writeheader()
        for row in data:
            writer.writerow(row)
    print(f"Dữ liệu thô đã được lưu/ghi thêm vào {filename}")


if __name__ == "__main__":
    all_games = crawl_metacritic_games(pages=100)
    save_to_csv(all_games)
