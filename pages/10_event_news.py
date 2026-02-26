import streamlit as st
import pandas as pd
import math
from datetime import datetime, timedelta
import sys
import os

# 대시보드가 크롤러 도우미를 찾을 수 있게 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.news_scraper import fetch_realtime_cvs_news

st.set_page_config(page_title="행사 및 이벤트 소식", page_icon="🎉", layout="wide")
st.markdown("## 🎉 편의점 행사 및 이벤트 소식")
st.caption("편의점 4사의 최신 공식 이벤트를 한눈에 확인하세요!")

# CSV 파일에서 초스피드로 데이터 읽어오기
df = fetch_realtime_cvs_news()

brands = ["전체", "GS25", "CU", "세븐일레븐", "이마트24"]
selected_brand = st.selectbox("🏢 브랜드 필터", brands)

if selected_brand != "전체":
    df = df[df['brand'] == selected_brand].reset_index(drop=True)

# 20개씩 출력 로직 세팅
ITEMS_PER_PAGE = 20
total_items = len(df)
total_pages = math.ceil(total_items / ITEMS_PER_PAGE) if total_items > 0 else 1

if 'event_page' not in st.session_state:
    st.session_state['event_page'] = 1

# 페이지 오류 방지
if st.session_state['event_page'] > total_pages:
    st.session_state['event_page'] = total_pages
if st.session_state['event_page'] < 1:
    st.session_state['event_page'] = 1

col1, col2, col3 = st.columns([1, 2, 1])
with col1:
    if st.button("⬅️ 이전", disabled=(st.session_state['event_page'] <= 1)):
        st.session_state['event_page'] -= 1
        st.rerun()
        
with col2:
    st.markdown(f"<div style='text-align: center;'><b>{st.session_state['event_page']} / {total_pages} 페이지</b> (총 {total_items}건)</div>", unsafe_allow_html=True)
    
with col3:
    if st.button("다음 ➡️", disabled=(st.session_state['event_page'] >= total_pages)):
        st.session_state['event_page'] += 1
        st.rerun()

st.markdown("---")

start_idx = (st.session_state['event_page'] - 1) * ITEMS_PER_PAGE
end_idx = start_idx + ITEMS_PER_PAGE
display_df = df.iloc[start_idx:end_idx]

now = datetime.now()

if display_df.empty:
    st.warning("수집된 행사 소식이 없습니다.")
else:
    for _, row in display_df.iterrows():
        # 24시간 이내에 뜬 소식은 불꽃 뱃지 달아주기
        is_new = (now - row['pub_date']) < timedelta(hours=24)
        badge = "<span style='color:#ff4b4b; font-weight:bold;'>🔥 [NEW]</span>" if is_new else ""
        
        # 날짜를 보기 좋게 YYYY-MM-DD 형식으로 변환
        date_str = row['pub_date'].strftime("%Y-%m-%d")
        
        # 제목은 왼쪽, 날짜는 오른쪽에 배치 
        st.markdown(f"""
        <div style="padding: 15px; border-bottom: 1px solid #444; display: flex; flex-direction: column;">
            <div style="font-size: 13px; color: #bbb; margin-bottom: 5px;">
                <span style="background-color: #58a6ff; color: white; padding: 2px 8px; border-radius: 10px; margin-right: 5px;">{row['brand']}</span>
            </div>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <a href="{row['link']}" target="_blank" style="font-size: 17px; font-weight: bold; text-decoration: none; color: #ffffff;">
                    {badge} {row['title']}
                </a>
                <span style="font-size: 14px; color: #888;">{date_str}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)