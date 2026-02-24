시각화 파일들은 utils 폴더로 옮겼고 img는 assets으로 옮겼습니다!


# 🏪 편의점 행사 상품 데이터 대시보드 (conv-dashboard)

본 프로젝트는 국내 주요 편의점(CU, GS25, 7-Eleven, emart24)의 행사 상품 데이터를 자동으로 수집(Scraping)하고, 정제 및 분류하여 시각화하는 통합 대시보드 시스템입니다.

---

## 📂 프로젝트 구조 (Project Structure)

```text
conv-dashboard/
┃
┣━━ 📄 .gitignore                   # Git 관리 제외 목록 (데이터, 캐시 등)
┣━━ 📄 requirements.txt             # 프로젝트 실행을 위한 라이브러리 목록
┣━━ 📄 README.md                    # 프로젝트 개요 및 사용 가이드
┣━━ 📄 app.py                       # [Main] 통합 실행 및 대시보드 엔진
┃
┣━━ 📂 pages/                       # [페이지]
┃   ┣━━ 📄 01_Price_Analysis.py     # 가격 비교 분석 페이지
┃   ┣━━ 📄 02_Brand_Comparison.py   # 브랜드별 특징 분석
┃   ┗━━ 📄 03_Category_Trends.py    # 카테고리별 트렌드
┃   ┗━━ 📄 04_budget_combination
┃
┣━━ 📂 scraper/                     # [수집] 브랜드별 크롤러 패키지
┃   ┣━━ 📄 __init__.py              # 패키지 초기화 파일
┃   ┣━━ 📄 cu_scraper.py            # CU 데이터 수집 로직
┃   ┣━━ 📄 gs25_scraper.py          # GS25 데이터 수집 로직
┃   ┣━━ 📄 seven_eleven_scraper.py  # 세븐일레븐 데이터 수집 로직
┃   ┗━━ 📄 emart24_scraper.py       # 이마트24 데이터 수집 로직
┃
┣━━ 📂 utils/                       # [도구] 데이터 처리 공용 유틸리티
┃   ┣━━ 📄 __init__.py
┃   ┣━━ 📄 data_cleaner.py          # 데이터 정제 및 통합 (CSV 병합 등)
┃   ┣━━ 📄 data_categorize.py       # 키워드 기반 카테고리 자동 분류
┃   ┗━━ 📄 data_visualization.ipynb # 데이터 시각화 분석
┃   ┗━━ 📄 brandname_visual
┃   ┗━━ 📄 graph
┃
┣━━ 📂 assets/                 # [Asset] 브랜드 로고 및 이미지 자원
┃   ┣━━ 🖼️ logo_cu.png
┃   ┣━━ 🖼️ logo_gs25.png
┃   ┣━━ 🖼️ logo_7eleven.png
┃   ┗━━ 🖼️ logo_emart24.png
┃   ┗━━ 🖼️ brandname_visual.png
┃   ┗━━ 🖼️ graph.png
┃
┗━━ 📂 data/                   # [저장] 수집된 데이터 결과물 (CSV)
    ┣━━ 📄 CU_{YYMMDD}.csv
    ┣━━ 📄 GS25_{YYMMDD}.csv
    ┣━━ 📄 7Eleven_{YYMMDD}.csv
    ┣━━ 📄 emart24_{YYMMDD}.csv
    ┣━━ 📄 cleaned_data.csv    # 정제된 통합 데이터
    ┗━━ 📄 categorized_data.csv # 카테고리 분류 데이터
```

---

## 🛠️ 주요 구성 요소 설명

### 1. **`app.py` (Main Orchestrator)**
- 프로젝트의 진입점입니다.
- 모든 스크래퍼를 순차적으로 실행하거나, 수집된 데이터를 바탕으로 대시보드 서버를 구동하는 역할을 합니다.

### 2. **`scraper/` (Data Collection)**
- 각 편의점 사이트의 특성에 맞게 설계된 크롤링 모듈입니다.
- 모든 스크래퍼는 `scrape()` 함수를 통해 독립적으로 실행 가능하며, 결과는 `data/` 폴더에 저장됩니다.

### 3. **`utils/` (Data Processing)**
- **`data_cleaner.py`**: 수집된 여러 CSV 파일을 하나로 합치고, 가격 정보의 숫자화 및 노이즈 데이터를 제거합니다.
- **`data_categorize.py`**: 상품명을 분석하여 '식사류', '간식류', '음료', '생활용품' 등으로 자동 분류합니다.

### 4. **`data/` (Storage)**
- 수집 및 처리된 모든 CSV 데이터가 보관되는 장소입니다. 
- 파일명 형식: `{브랜드}_{날짜}.csv` (예: `CU_260224.csv`)

---

## 🚀 시작하기

1. **가상환경 설정 및 라이브러리 설치**
   ```bash
   pip install -r requirements.txt
   ```

2. **전체 데이터 수집 실행**
   ```bash
   python app.py
   ```

3. **데이터 정제 및 분류 (필요 시)**
   ```bash
   python utils/data_cleaner.py
   python utils/data_categorize.py
   ```

편의점 통합 행사 상품 수집 파트 (CU)
담당 역할
CU 공식 홈페이지 '전체' 행사 탭 데이터 전수 수집 및 전처리

데이터 현황
최종 수집 일자: 2026-02-23

총 데이터 건수: 1,706건

수집 항목: brand, name, price, event, img_url

사용 방법
가상환경 생성 및 라이브러리 설치: pip install -r requirements.txt

크롤러 실행: python script.py

결과 확인: 생성된 CU_260223.csv 파일을 분석 모듈에서 로드하여 사용

