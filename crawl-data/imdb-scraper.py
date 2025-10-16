import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
import re

CHROME_DRIVER_PATH = "crawl-data\chromedriver.exe" 
CHROME_BINARY_LOCATION = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

# Hàm click nút "Load more" để tải thêm phim trên cùng một trang danh sách
def click_load_more(driver):
    """Liên tục click nút 'Load more' để tải thêm phim trên cùng một trang danh sách."""
    
    # Selector cho nút Load more
    LOAD_MORE_SELECTOR = '.ipc-see-more__button'
    MAX_CLICKS = 30
    
    for attempt in range(MAX_CLICKS):
        try:
            # Đợi cho nút xuất hiện và có thể click được
            load_more_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, LOAD_MORE_SELECTOR))
            )
            
            # Kiểm tra xem nút có hiển thị số lượng phim cần tải thêm không
            if load_more_button.text.lower() not in ("load more", "50 more", "100 more"):
                 break
                 
            load_more_button.click()
            print(f"   Đã nhấp 'Load more' lần {attempt + 1}. Đang chờ tải...")
            
            # Đợi một khoảng thời gian ngắn để nội dung mới được tải động
            time.sleep(random.uniform(5, 7))
            
        except (NoSuchElementException, TimeoutException):
            print("   Đã tải hết nội dung trên trang hoặc nút 'Load more' không còn.")
            break
        except Exception:
            break
        
    time.sleep(random.uniform(3, 5))


# Hàm cào THỂ LOẠI từ trang chi tiết phim 
def scrape_movie_genre(driver, link):
    """Truy cập trang chi tiết và cào thông tin Thể loại."""
    genre_list = []
    
    try:
        driver.get(link)
        time.sleep(random.uniform(5, 8)) 
        
        detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Selector cho khối thể loại
        genre_scroller = detail_soup.find('div', class_='ipc-chip-list__scroller')
        
        if genre_scroller:
            genre_tags = genre_scroller.find_all('span', class_='ipc-chip__text')
            for tag in genre_tags:
                genre_list.append(tag.text.strip())
        
        return ", ".join(genre_list)
        
    except Exception as e:
        # Lỗi có thể do bị chặn
        return "N/A (Cào lỗi)"


# Hàm trích xuất dữ liệu chi tiết từ một khối HTML của một bộ phim trên IMDb
def extract_movie_info_imdb(movie_html_block):
    """Trích xuất thông tin cơ bản từ khối HTML của một bộ phim."""
    soup_movie = BeautifulSoup(str(movie_html_block), 'html.parser')
    data = {
        'Title': "N/A", 'Year': "N/A", 'IMDb_Rating': "N/A", 'Votes': "N/A", 
        'Runtime': "N/A", 'Genre': "N/A", 'Link': "N/A",
    }
    
    try:
        # --- 1. Title, Link ---
        title_wrapper = soup_movie.find('a', class_='ipc-title-link-wrapper')
        if title_wrapper:
            title_text = title_wrapper.find('h3', class_='ipc-title__text').text.strip()
            data['Title'] = title_text.split('. ')[-1]
            data['Link'] = "https://www.imdb.com" + title_wrapper.get('href', 'N/A').split('?')[0]
            
        # --- 2. Metadata (Year, Runtime) ---
        metadata_div = soup_movie.find('div', class_='dli-title-metadata')
        if metadata_div:
            items = metadata_div.find_all('span', class_=lambda c: c and 'dli-title-metadata-item' in c)
            if len(items) > 0: data['Year'] = items[0].text.strip()
            if len(items) > 1: data['Runtime'] = items[1].text.strip()
            
        # --- 3. Rating & Votes ---
        rating_main_span = soup_movie.find('span', attrs={'data-testid': 'ratingGroup--imdb-rating'})
        
        if rating_main_span:
            # Rating (Số sao)
            rating_tag = rating_main_span.find('span', class_='ipc-rating-star--rating')
            if rating_tag: data['IMDb_Rating'] = rating_tag.text.strip() 
                 
            # Votes (Số người đánh giá)
            votes_tag = rating_main_span.find('span', class_='ipc-rating-star--voteCount')
            if votes_tag:
                votes_raw = votes_tag.text.strip()
                match = re.search(r'\((.*?)\)', votes_raw)
                data['Votes'] = match.group(1).strip() if match else re.sub(r'[()\s\n\t]+', '', votes_raw).replace('&nbsp;', '')
        
    except Exception:
        return None 
    return data


# --- HÀM CÀO CHÍNH
def scrape_imdb_movies():
    
    # 1. Cấu hình WebDriver cho môi trường Local
    chrome_options = Options()
    chrome_options.binary_location = CHROME_BINARY_LOCATION
    
    # TÙY CHỌN AN TOÀN VÀ HEADLESS
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    chrome_options.add_argument('--headless=new') 
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument('--no-sandbox') 
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument("--disable-features=RendererCodeIntegrity")
    chrome_options.add_argument('--disable-blink-features=AutomationControlled') 
    
    try:
        service = Service(CHROME_DRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=chrome_options) 
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
        })
    except Exception as e:
        print(f"LỖI KHỞI TẠO DRIVER. Lỗi: {e}")
        return

    movie_data = []
    BASE_SEARCH_URL = "https://www.imdb.com/search/title/?release_date=2018-01-01,2025-10-16&sort=num_votes,desc"
    
    # 2. BƯỚC 1: CÀO TRANG DANH SÁCH ĐẦU TIÊN
    all_movies_basic_info = [] 

    print(f"--- BƯỚC 1: Đang cào Trang Danh sách ĐẦU TIÊN | URL: {BASE_SEARCH_URL}")
    
    try:
        driver.get(BASE_SEARCH_URL)
        click_load_more(driver)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        movie_listings = soup.find_all('div', class_=lambda c: c and 'dli-parent' in c) 

        if not movie_listings:
            if "captcha" in driver.page_source.lower() or "verify" in driver.current_url.lower():
                print("\nPHÁT HIỆN CHẶN CAPTCHA. Dừng cào.")
            else:
                print(f"Không tìm thấy phim nào trên trang danh sách. Dừng cào.")
        
        print("\n[LOG BƯỚC 1 - DỮ LIỆU CƠ BẢN ĐÃ CÀO ĐƯỢC]")
        
        for movie_block in movie_listings:
            data = extract_movie_info_imdb(movie_block)
            if data and data['Title'] != 'N/A':
                print(f"  > Phim: {data['Title']} ({data['Year']}) | Rating: {data['IMDb_Rating']} ({data['Votes']})")
                all_movies_basic_info.append(data)
        
        print(f"Đã cào được {len(movie_listings)} khối tiềm năng. Tổng bản ghi sẽ xử lý: {len(all_movies_basic_info)}.")
        time.sleep(random.uniform(7, 10)) 
            
    except Exception as e:
        print(f"Lỗi không mong muốn trong quá trình cào trang danh sách: {e}")
    
    # 3. BƯỚC 2: CÀO THÔNG TIN CHI TIẾT (GENRE)
    movies_to_process = all_movies_basic_info 
    print(f"\n--- BƯỚC 2: CÀO {len(movies_to_process)} TRANG CHI TIẾT ĐỂ LẤY THỂ LOẠI ---")
    
    movie_data = movies_to_process 
    print("\n[LOG BƯỚC 2 - KẾT QUẢ CÀO GENRE]")
    
    for i, movie in enumerate(movie_data):
        link = movie['Link']
        
        genre = scrape_movie_genre(driver, link)
        movie['Genre'] = genre
        
        print(f"  > [{i+1}/{len(movie_data)}] Phim: {movie['Title']} | Thể loại: {movie['Genre']}")

        time.sleep(random.uniform(2, 5)) 

    driver.quit()
    
    if movie_data:
        df = pd.DataFrame(movie_data)

        if 'Link' in df.columns:
            df = df.drop(columns=['Link'])
        
        file_name = f'imdb_movie_trends_{len(df)}_movies_with_genre.csv'
        df.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"\nHoàn tất cào dữ liệu IMDb và lưu vào file '{file_name}'")
        print(f"Tổng số phim đã cào: {len(df)}")
    else:
        print("\nKhông có dữ liệu nào được cào thành công.")


if __name__ == "__main__":
    scrape_imdb_movies()
