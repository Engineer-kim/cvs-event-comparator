from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from datetime import datetime
import os

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def scrape_official_events():
    driver = get_driver()
    event_list = []
    today = datetime.now()

    print("🚀 4개 편의점 공식 홈페이지 '전체' 이벤트 수집을 시작합니다...")

    # 1. GS25
    print("  -> GS25 수집 중...")
    for page in range(1, 4): 
        try:
            driver.get(f"http://gs25.gsretail.com/gscvs/ko/customer-engagement/event/current-events?pageNum={page}")
            time.sleep(3)
            items = driver.find_elements(By.CSS_SELECTOR, "table.tbl_ltype1 tbody tr")
            if not items: break
            for item in items:
                try:
                    a_tag = item.find_element(By.CSS_SELECTOR, "p.tit a")
                    title = a_tag.text.strip()
                    if not title: continue
                    link = a_tag.get_attribute("href")
                    event_list.append({"brand": "GS25", "title": f"[공식] {title}", "link": link, "pub_date": today})
                except Exception: continue
        except Exception: break

    # 2. CU
    print("  -> CU 수집 중...")
    for page in range(1, 4):
        try:
            driver.get(f"https://cu.bgfretail.com/brand_info/news_list.do?category=brand_info&depth2=5&sf=N&pageIndex={page}")
            time.sleep(4) 
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            if not rows: break
            for row in rows:
                try:
                    a_tags = row.find_elements(By.TAG_NAME, "a")
                    for a in a_tags:
                        title = a.text.strip()
                        if title and len(title) > 2:
                            title = title.replace('\n', ' ')
                            link = "https://cu.bgfretail.com/brand_info/news_list.do?category=brand_info&depth2=5&sf=N"
                            event_list.append({"brand": "CU", "title": f"[공식] {title}", "link": link, "pub_date": today})
                            break 
                except Exception: continue
        except Exception: break

    # 3. 7-Eleven
    print("  -> 세븐일레븐 수집 중...")
    try:
        driver.get("https://www.7-eleven.co.kr/event/eventList.asp")
        time.sleep(3)
        for _ in range(4): 
            try:
                more_btn = driver.find_element(By.CSS_SELECTOR, "a.btn_more")
                if more_btn.is_displayed():
                    driver.execute_script("arguments[0].click();", more_btn)
                    time.sleep(2)
            except: pass
            
        items = driver.find_elements(By.CSS_SELECTOR, "ul#listUl li")
        for item in items:
            try:
                title = ""
                try: title = item.find_element(By.CSS_SELECTOR, "dt").get_attribute("innerText").strip()
                except: pass
                if not title:
                    try: title = item.find_element(By.CSS_SELECTOR, "img").get_attribute("alt").strip()
                    except: pass
                    
                link = "https://www.7-eleven.co.kr/event/eventList.asp"
                if title:
                    event_list.append({"brand": "세븐일레븐", "title": f"[공식] {title}", "link": link, "pub_date": today})
            except Exception: continue
    except Exception as e:
        print(f"세븐일레븐 오류: {e}")

    # 4. 이마트24 (조장님이 주신 HTML 구조 완벽 반영!)
    print("  -> 이마트24 수집 중...")
    try:
        # 진행중인 이벤트 정확한 주소 적용
        driver.get("https://emart24.co.kr/event/ing")
        time.sleep(5) # 데이터가 다 그려질 때까지 넉넉히 5초 대기
        
        items = driver.find_elements(By.CSS_SELECTOR, "a.eventWrap")
        for item in items:
            try:
                link = item.get_attribute("href")
                if not link: link = "https://emart24.co.kr/event/ing"
                
                # p 태그 안의 내용을 가져와서 줄바꿈 기준으로 쪼개기
                p_text = item.find_element(By.TAG_NAME, "p").get_attribute("innerText").strip()
                if not p_text: continue
                
                lines = [line.strip() for line in p_text.split('\n') if line.strip()]
                # 날짜 밑에 있는 마지막 줄이 항상 행사 제목!
                title = lines[-1] if lines else "이마트24 이벤트"
                
                event_list.append({"brand": "이마트24", "title": f"[공식] {title}", "link": link, "pub_date": today})
            except Exception: continue
    except Exception as e:
        print(f"이마트24 오류: {e}")

    driver.quit()

    df = pd.DataFrame(event_list)
    if not df.empty:
        df = df.drop_duplicates(subset=['brand', 'title'], keep='first')
        
        save_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, 'official_event_news.csv')
        df.to_csv(save_path, index=False, encoding='utf-8-sig')
        
        print("\n📊 [수집 완료 보고서]")
        print("------------------------")
        print(df['brand'].value_counts().to_string())
        print("------------------------")
        print(f"✅ 총 {len(df)}개의 이벤트가 성공적으로 수집 및 저장되었습니다!")
    else:
        print("❌ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    scrape_official_events()