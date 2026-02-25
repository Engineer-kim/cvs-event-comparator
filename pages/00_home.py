import streamlit as st
import os
import pandas as pd
import base64
from datetime import datetime

# 로컬 이미지를 HTML에서 사용하기 위한 base64 인코딩 함수
def get_base64_image(image_path):
    if not os.path.exists(image_path):
        return None
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

# --- 메인 대시보드 ---

# 1. 히어로 섹션
st.markdown(f"""
    <div class="hero-section">
        <div class="hero-title">🚀 편의점 득템 가이드</div>
        <div class="hero-subtitle">
            스마트한 소비를 위한 실시간 행사 압축 가이드!<br>
            CU, GS25, 7-Eleven, Emart24의 모든 혜택을 한눈에 비교하세요.
        </div>
    </div>
""", unsafe_allow_html=True)

# 2. 추천 상품 섹션 (가로 스크롤)
try:
    df_main = pd.read_csv('data/categorized_data.csv')
    display_df = pd.DataFrame()

    # 타이틀 
    if 'recent_keywords' in st.session_state and st.session_state['recent_keywords']:
        st.markdown("### 🎁 취향 저격 맞춤 추천")
        rec_list = []
        for kwd in st.session_state['recent_keywords']:
            matched = df_main[df_main['name'].astype(str).str.contains(kwd, case=False, na=False)]
            rec_list.append(matched)
        if rec_list:
            display_df = pd.concat(rec_list).drop_duplicates(subset=['name', 'brand', 'event'])
    else:
        st.markdown("### 🎲 오늘의 핫딜 추천")

    if len(display_df) < 10 and not df_main.empty:
        shortfall = 10 - len(display_df)
        remaining_df = df_main.drop(display_df.index, errors='ignore') if not display_df.empty else df_main
        if not remaining_df.empty:
            fill_df = remaining_df.sample(n=min(shortfall, len(remaining_df)))
            display_df = pd.concat([display_df, fill_df])

    display_df = display_df.head(10)

    if not display_df.empty:
        scroll_html = """<style>
.horizontal-scroll-wrapper {
    display: flex;
    overflow-x: auto;
    gap: 15px;
    padding: 10px 5px 20px 5px;
    scroll-behavior: smooth;
}
.horizontal-scroll-wrapper::-webkit-scrollbar {
    height: 8px;
}
.horizontal-scroll-wrapper::-webkit-scrollbar-thumb {
    background-color: #d1d5db;
    border-radius: 10px;
}
.scroll-item {
    flex: 0 0 220px;
    border: 1px solid #e0e0e0;
    border-radius: 12px;
    padding: 15px;
    background: white;
    box-shadow: 2px 2px 8px rgba(0,0,0,0.05);
    text-align: center;
}
</style>
<div class="horizontal-scroll-wrapper">"""

        for idx, row in display_df.iterrows():
            img_url = row['img_url'] if pd.notna(row['img_url']) else "https://via.placeholder.com/150?text=No+Image"
            price = int(str(row['price']).replace(',', '')) if pd.notna(row['price']) else 0
            unit_price = price // 2 if row['event'] == '1+1' else (price * 2 // 3 if row['event'] == '2+1' else price)
            
            scroll_html += f"""
    <div class="scroll-item">
        <img src="{img_url}" style="width:100%; height:120px; object-fit:contain; border-radius:8px; margin-bottom:10px;">
        <div style="font-size:12px; color:#888; text-align:left;">{row['brand']} | {row['event']}</div>
        <div style="font-size:15px; font-weight:bold; margin: 5px 0; text-align:left; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{row['name']}</div>
        <div style="font-size:18px; color:#ff4b4b; font-weight:900; text-align:left;">{price:,}원</div>
        <div style="font-size:12px; color:#555; text-align:left; margin-top:5px;">👉 개당 {unit_price:,}원</div>
    </div>"""
        scroll_html += "</div>"
        st.markdown(scroll_html, unsafe_allow_html=True)
except Exception as e:
    pass

# ------ 여기부터 시간대별로 상품 추천해주는 기능 (위치 이동됨) ------
st.markdown("<br>", unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def load_home_data():
    file_path = os.path.join('data', 'categorized_data.csv')
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return None

df_time = load_home_data()

if df_time is not None:
    now_hour = datetime.now().hour
    if 6 <= now_hour < 11:
        target_cat, title, icon = ["식사류"], "🌅 바쁜 아침, 든든한 한 끼!", "🥛"
    elif 11 <= now_hour < 14:
        target_cat, title, icon = ["식사류"], "🍱 오늘 점심 뭐 먹지?", "🥢"
    elif 14 <= now_hour < 18:
        target_cat, title, icon = ["간식류", "음료"], "☕ 나른한 오후, 당 충전 시간", "🍪"
    elif 18 <= now_hour < 21:
        target_cat, title, icon = ["식사류"], "🍺 하루를 마무리하는 저녁", "🍗"
    else:
        target_cat, title, icon = ["간식류", "식사류"], "🌙 출출한 밤, 야식의 유혹", "🍜"

    display_cats = " ".join([f"#{c}" for c in target_cat])
    st.markdown(f"### {icon} {title}")

    col_tag, col_btn = st.columns([4, 1])
    with col_tag:
        st.markdown(f"현재 시간대에 딱 맞는 **{display_cats}** 상품들입니다.")
    with col_btn:
        st.button("🔄 다른 상품 보기", use_container_width=True, key="refresh_time_items")

    recommend_df = df_time[df_time['category'].isin(target_cat)].copy()
    if not recommend_df.empty:
        exclude_keywords = ['쏘피', '좋은', '섬유유연제', '티셔츠', '순수한면', '면도날', '라엘', '순면', '비비안']
        filter_condition = recommend_df['name'].str.contains('|'.join(exclude_keywords), na=False)
        recommend_df = recommend_df[~filter_condition]

        display_items = recommend_df.sample(n=min(len(recommend_df), 5))
        cols = st.columns(5)
        for i, (_, row) in enumerate(display_items.iterrows()):
            with cols[i]:
                st.markdown(f"""
                    <div style="background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 15px; text-align: center; height: 100%;">
                        <div style="height: 100px; display: flex; align-items: center; justify-content: center; margin-bottom: 10px;">
                            <img src="{row['img_url']}" style="max-width: 100%; max-height: 100px; object-fit: contain;">
                        </div>
                        <div style="font-size: 0.85rem; font-weight: bold; color: white; margin-bottom: 5px; height: 40px; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; line-height: 1.2;">
                            {row['name']}
                        </div>
                        <div style="color: #58a6ff; font-weight: bold; font-size: 1.1rem;">{int(row['price']):,}원</div>
                        <div style="font-size: 0.8rem; color: #ff6b6b; font-weight: bold;">{row['event']}</div>
                        <div style="font-size: 0.75rem; color: #8b949e; margin-top: 5px;">📍 {row['brand']}</div>
                    </div>
                """, unsafe_allow_html=True)
    else:
        st.info(f"현재 {target_cat} 카테고리에 해당하는 행사 상품이 없습니다.")

st.markdown("<br><br>", unsafe_allow_html=True)

# 3. 퀵 메뉴 카드
st.markdown("### 🚀 빠른 메뉴")
r1_c1, r1_c2, r1_c3 = st.columns(3)

with r1_c1:
    st.markdown("""
        <a href="/overall_summary" target="_self" style="text-decoration:none; color:inherit;">
            <div class="dashboard-card" style="cursor:pointer;">
                <div class="card-icon">🔍</div>
                <div class="card-title">전체 요약</div>
                <div class="card-desc">이미지 기반의 카드 리스트로 모든 행사 상품을 검색하고 필터링하세요.</div>
                <div style="margin-top:20px; color:#58a6ff; font-weight:bold;">이동하기 →</div>
            </div>
        </a>
    """, unsafe_allow_html=True)

with r1_c2:
    st.markdown("""
        <a href="/brand_comparison" target="_self" style="text-decoration:none; color:inherit;">
            <div class="dashboard-card" style="cursor:pointer;">
                <div class="card-icon">📊</div>
                <div class="card-title">브랜드별 비교</div>
                <div class="card-desc">어느 편의점이 가장 혜택이 좋을까요? 차트와 통계로 브랜드별 전략을 비교합니다.</div>
                <div style="margin-top:20px; color:#58a6ff; font-weight:bold;">이동하기 →</div>
            </div>
        </a>
    """, unsafe_allow_html=True)

with r1_c3:
    st.markdown("""
        <a href="/best_value" target="_self" style="text-decoration:none; color:inherit;">
            <div class="dashboard-card" style="cursor:pointer;">
                <div class="card-icon">💎</div>
                <div class="card-title">가성비 비교</div>
                <div class="card-desc">할인율이 가장 높은 TOP 50 상품만 모았습니다. 지갑을 지키는 가장 쉬운 방법!</div>
                <div style="margin-top:20px; color:#58a6ff; font-weight:bold;">이동하기 →</div>
            </div>
        </a>
    """, unsafe_allow_html=True)

# 4. 하단 브랜드 로고 섹션
r2_c1, r2_c2, r2_c3 = st.columns(3)

with r2_c1:
    st.markdown("""
        <a href="/budget_combination" target="_self" style="text-decoration:none; color:inherit;">
            <div class="dashboard-card" style="cursor:pointer;">
                <div class="card-icon">🍱</div>
                <div class="card-title">예산 맞춤 꿀조합</div>
                <div class="card-desc">내 예산 안에서 가장 많이 절약할 수 있는 상품들의 조합을 추천해드려요.</div>
                <div style="margin-top:20px; color:#58a6ff; font-weight:bold;">이동하기 →</div>
            </div>
        </a>
    """, unsafe_allow_html=True)

with r2_c2:
    st.markdown("""
        <a href="/diet_guide" target="_self" style="text-decoration:none; color:inherit;">
            <div class="dashboard-card" style="cursor:pointer;">
                <div class="card-icon">🏋️</div>
                <div class="card-title">다이어트 가이드</div>
                <div class="card-desc">제로 슈거, 고단백 상품들만 쏙쏙 골라 건강한 편의점 식단을 제안합니다.</div>
                <div style="margin-top:20px; color:#58a6ff; font-weight:bold;">이동하기 →</div>
            </div>
        </a>
    """, unsafe_allow_html=True)

with r2_c3:
    st.markdown("""
        <a href="/night_snack_guide" target="_self" style="text-decoration:none; color:inherit;">
            <div class="dashboard-card" style="cursor:pointer;">
                <div class="card-icon">🌙</div>
                <div class="card-title">야식 & 안주 가이드</div>
                <div class="card-desc">오늘 밤 혼술 안주와 야식을 고민하시나요? 딱 맞는 행사 안주를 찾아보세요.</div>
                <div style="margin-top:20px; color:#58a6ff; font-weight:bold;">이동하기 →</div>
            </div>
        </a>
    """, unsafe_allow_html=True)

# 5. 세 번째 퀵 메뉴 행 (지도)
r3_c1, r3_c2, r3_c3 = st.columns(3)

with r3_c1:
    st.markdown("""
        <a href="/convenience_store_map" target="_self" style="text-decoration:none; color:inherit;">
            <div class="dashboard-card" style="cursor:pointer;">
                <div class="card-icon">📍</div>
                <div class="card-title">편의점 지도</div>
                <div class="card-desc">내 주변의 편의점은 어디에 있을까요? 브랜드별 위치를 지도에서 확인하세요.</div>
                <div style="margin-top:20px; color:#58a6ff; font-weight:bold;">이동하기 →</div>
            </div>
        </a>
    """, unsafe_allow_html=True)

# 하단 브랜드 로고 섹션
st.markdown("---")
st.markdown("### 🏢 함께하는 브랜드")
l1, l2, l3, l4 = st.columns(4)

logos = {
    "CU": "assets/logo_cu.png",
    "GS25": "assets/logo_gs25.png",
    "7Eleven": "assets/logo_7eleven.png",
    "emart24": "assets/logo_emart24.png"
}

for col, (name, path) in zip([l1, l2, l3, l4], logos.items()):
    with col:
        b64_img = get_base64_image(path)
        if b64_img:
            st.markdown(f"""
                <div class="brand-logo-card">
                    <img src="data:image/png;base64,{b64_img}">
                </div>
            """, unsafe_allow_html=True)
        else:
            st.button(name, use_container_width=True)

st.markdown("---")
st.caption("© 2026 Convenience Store Event Dashboard. Data updated daily.")
