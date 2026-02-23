import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime

class CUCrawler:
    """CU 편의점 행사 상품 수집 및 전처리를 담당하는 클래스"""
    
    def __init__(self):
        # CU 행사 상품 Ajax API 주소
        self.base_url = "https://cu.bgfretail.com/event/plusAjax.do"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://cu.bgfretail.com/event/plus.do"
        }
        self.product_list = []

    def fetch_page(self, page_index):
        """특정 페이지의 HTML 데이터를 가져옴 (전체 탭 기준)"""
        payload = {
            "pageIndex": page_index,
            "listType": "0",  # '전체' 탭 데이터 요청
            "searchCondition": "",
            "searchWord": ""
        }
        try:
            # 10초 타임아웃 설정으로 네트워크 지연 대응
            response = requests.post(self.base_url, data=payload, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"[Error] {page_index}페이지 호출 실패: {e}")
            return None

    def parse_data(self, html):
        """HTML 소스에서 상품 정보를 추출하고, 데이터가 없으면 False를 반환하여 루프 종료"""
        if not html:
            return False

        soup = BeautifulSoup(html, "html.parser")
        # 상품 리스트 단위 추출
        items = soup.select("li.prod_list")
        
        # [중요] 조기 종료 조건: 수집된 상품 리스트가 0개라면 수집 완료로 판단
        if len(items) == 0:
            return False

        for item in items:
            try:
                # 1. 브랜드명 (고정)
                brand = "CU"
                
                # 2. 상품명 (.name p)
                name = item.select_one(".name p").get_text(strip=True)
                
                # 3. 가격 (.price strong) - 숫자만 추출하여 정수형 변환
                price_raw = item.select_one(".price strong").get_text(strip=True)
                price = int(re.sub(r"[^\d]", "", price_raw))
                
                # 4. 행사 종류 (.badge span) - 사진의 plus1, plus2 등 클래스 대응
                event_element = item.select_one(".badge span")
                event = event_element.get_text(strip=True) if event_element else "행사정보없음"
                
                # 5. 이미지 URL (.prod_img img) - 절대 경로 보정
                img_url = item.select_one(".prod_img img")['src']
                if img_url.startswith("//"):
                    img_url = "https:" + img_url
                
                self.product_list.append({
                    "brand": brand,
                    "name": name,
                    "price": price,
                    "event": event,
                    "img_url": img_url
                })
            except (AttributeError, TypeError, KeyError):
                # 개별 아이템 파싱 에러 시 해당 상품만 건너뜀
                continue 
        
        return True

    def run(self, max_pages=150):
        """전체 페이지를 순회하며 크롤링 수행"""
        print(f"[Start] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} CU 전수 수집 시작...")
        
        for page in range(1, max_pages + 1):
            html = self.fetch_page(page)
            
            # parse_data가 False를 반환하면(데이터가 없으면) 즉시 중단
            if not self.parse_data(html):
                print(f"[Info] {page-1}페이지에서 더 이상 데이터가 없습니다. 수집을 종료합니다.")
                break
            
            print(f"[Progress] {page}페이지 수집 완료... (누적: {len(self.product_list)}건)")
            
            # 서버 부하 방지를 위한 매너 대기 (1초)
            time.sleep(1)

    def save_to_csv(self):
        """데이터를 '브랜드명_YYMMDD.csv' 형식으로 저장"""
        # 현재 날짜 기준 파일명 생성 (예: CU_260223.csv)
        date_str = datetime.now().strftime("%y%m%d")
        filename = f"CU_{date_str}.csv"
        
        df = pd.DataFrame(self.product_list)
        
        # 혹시 모를 중복 데이터 제거
        df = df.drop_duplicates(subset=['name', 'price', 'event'])
        
        # 엑셀 한글 깨짐 방지를 위한 utf-8-sig 인코딩 적용
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"[Success] '{filename}' 파일로 최종 {len(df)}건 저장 완료!")

# --- 메인 실행 로직 ---
if __name__ == "__main__":
    crawler = CUCrawler()
    crawler.run()
    crawler.save_to_csv()