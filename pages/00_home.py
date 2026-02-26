import streamlit as st
import os
import pandas as pd
import base64
from datetime import datetime
import pytz
import streamlit.components.v1 as components

# 한국 시간(KST) 설정
KST = pytz.timezone('Asia/Seoul')
now_hour = datetime.now(KST).hour

# 브랜드별 고유 컬러 반환 함수
def get_brand_color(brand):
    brand_colors = {
        "CU": "#652D90",
        "GS25": "#0054A6",
        "7-Eleven": "#008061",
        "7Eleven": "#008061",
        "세븐일레븐": "#008061",
        "emart24": "#FFB81C",
        "이마트24": "#FFB81C"
    }
    return brand_colors.get(brand, "#8b949e")

# 로컬 이미지를 HTML에서 사용하기 위한 base64 인코딩 함수
def get_base64_image(image_path):
    if not os.path.exists(image_path):
        return None
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()
    
@st.cache_data
def get_fixed_hot_deals(recent_keywords):
    try:
        df_main = pd.read_csv('data/categorized_data.csv')
        df_main['event'] = df_main['event'].astype(str).str.replace(' ', '', regex=False)
        df_main = df_main[df_main['event'].str.contains(r'\+', na=False, regex=True)]
        
        display_df = pd.DataFrame()

        if recent_keywords:
            rec_list = []
            for kwd in recent_keywords:
                matched = df_main[df_main['name'].astype(str).str.contains(kwd, case=False, na=False)]
                rec_list.append(matched)
            if rec_list:
                display_df = pd.concat(rec_list).drop_duplicates(subset=['name', 'brand', 'event'])
        
        if len(display_df) < 10 and not df_main.empty:
            shortfall = 10 - len(display_df)
            remaining_df = df_main.drop(display_df.index, errors='ignore')
            if not remaining_df.empty:
                fill_df = remaining_df.sample(n=min(shortfall, len(remaining_df)))
                display_df = pd.concat([display_df, fill_df])
        
        return display_df.head(10)
    except:
        return pd.DataFrame()

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
    current_keywords = st.session_state.get('recent_keywords', [])
    display_df = get_fixed_hot_deals(current_keywords)

    # 타이틀 
    if current_keywords:
        st.markdown("### 🎁 취향 저격 맞춤 추천")
    else:
        st.markdown("### 🎲 오늘의 핫딜 추천")

    if not display_df.empty:
        scroll_html = """<style>
    .horizontal-scroll-wrapper {
        display: flex;
        overflow: hidden;
        gap: 20px;
        padding: 15px 5px 25px 5px;
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
        flex: 0 0 calc(20% - 16px); /* 5개씩 보이도록 계산, gap 포함 */
        border: 1px solid #eef0f2;
        border-radius: 16px;
        padding: 15px;
        background: white;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        text-align: left; /* 전체 왼쪽 정렬로 변경하여 가독성 향상 */
        transition: transform 0.2s;
        box-sizing: border-box;
    }
    .scroll-item:hover {
        transform: translateY(-5px);
    }
    .item-name {
        font-size: 16px; /* 폰트 크기 확대 */
        font-weight: bold;
        color: #1a1a1a;
        margin: 8px 0;
        height: 42px; /* 두 줄 높이 확보 */
        line-height: 1.3;
        display: -webkit-box;
        -webkit-line-clamp: 2; /* 최대 2줄까지 표시 */
        -webkit-box-orient: vertical;
        overflow: hidden;
        text-overflow: ellipsis;
        word-break: break-all;
    }
    </style>
    <div class="horizontal-scroll-wrapper" id="hotdeal-scroll">
    """

        for idx, row in display_df.iterrows():
            img_url = row['img_url'] if pd.notna(row['img_url']) else "https://via.placeholder.com/150?text=No+Image"
            price = int(str(row['price']).replace(',', '')) if pd.notna(row['price']) else 0
            # 행사 종류에 따른 개당 가격 계산 로직 (기존 유지)
            unit_price = price // 2 if row['event'] == '1+1' else (
                price * 2 // 3 if row['event'] == '2+1' else (price * 3 // 4 if row['event'] == '3+1' else price))

            brand_color = get_brand_color(row['brand'])
            scroll_html += f"""
        <div class="scroll-item">
            <img src="{img_url}" style="width:100%; height:130px; object-fit:contain; border-radius:8px; margin-bottom:12px;">
            <div style="display: flex; align-items: center; gap: 5px; margin-bottom: 5px;">
                <span style="font-size:0.8rem; color:{brand_color}; background:{brand_color}15; padding:2px 6px; border-radius:4px; font-weight:bold;">{row['brand']}</span>
                <span style="font-size:11px; color:#ff4b4b; background:#fff0f0; padding:2px 6px; border-radius:4px; font-weight:bold;">{row['event']}</span>
            </div>
            <div class="item-name">{row['name']}</div>
            <div style="font-size:18px; color:#1a1a1a; font-weight:900;">{price:,}원</div>
            <div style="font-size:13px; color:#3182f6; font-weight:bold; margin-top:4px;">✨ 개당 {unit_price:,}원</div>
        </div>"""

        scroll_html += """
    </div>

    <script>
    const container = document.getElementById("hotdeal-scroll");
    const items = container.querySelectorAll(".scroll-item");

    items.forEach((el, i) => {
        if (i >= 10) el.style.display = "none";
    });

    let currentIndex = 0;
    const visibleCount = 5;

    function slide() {
        const itemWidth = items[0].offsetWidth + 20;
        currentIndex += visibleCount;

        if (currentIndex >= 10) currentIndex = 0;

        container.scrollTo({
            left: itemWidth * currentIndex,
            behavior: "smooth"
        });
    }

    setInterval(slide, 4000);
    </script>
    """

    components.html(scroll_html, height=350, scrolling=False)
except Exception as e:
    pass

# ------ 여기부터 시간대별로 상품 추천해주는 기능 (위치 이동됨) ------
st.markdown("<br>", unsafe_allow_html=True)

df_time = pd.read_csv('data/categorized_data.csv')

if df_time is not None:
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
        recommend_df = recommend_df[recommend_df['event'] != '세일']

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
                        <div style="margin-top: 5px;">
                            <span style="color:{get_brand_color(row['brand'])}; background:{get_brand_color(row['brand'])}15; padding:2px 6px; border-radius:4px; font-weight:bold; font-size:0.8rem;">📍 {row['brand']}</span>
                        </div>
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
                <div class="card-title">내 예산 맞춤 꿀조합 생성기</div>
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

# 5. 세 번째 퀵 메뉴 행 (럭키박스 & 지도)
r3_c1, r3_c2, r3_c3 = st.columns(3)

with r3_c1:
    st.markdown("""
        <a href="/random_picker" target="_self" style="text-decoration:none; color:inherit;">
            <div class="dashboard-card" style="cursor:pointer;">
                <div class="card-icon">🎁</div>
                <div class="card-title">럭키박스</div>
                <div class="card-desc">메뉴 결정이 힘드신가요? 랜덤 럭키박스로 오늘 행운의 상품을 뽑아보세요!</div>
                <div style="margin-top:20px; color:#58a6ff; font-weight:bold;">이동하기 →</div>
            </div>
        </a>
    """, unsafe_allow_html=True)

with r3_c2:
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

with r3_c3:
    st.markdown("""
        <a href="/jackpot_game" target="_self" style="text-decoration:none; color:inherit;">
            <div class="dashboard-card" style="cursor:pointer;">
                <div class="card-icon">🎰</div>
                <div class="card-title">잭팟 게임!</div>
                <div class="card-desc">똑같은 상품 3개를 맞추면 오늘 운세 대박! 행운의 메뉴를 잭팟으로 확인하세요.</div>
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
