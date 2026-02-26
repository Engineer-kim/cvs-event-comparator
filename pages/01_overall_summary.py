import streamlit as st
import pandas as pd
import os
from datetime import datetime
from utils.cart import init_cart, render_cart_button, render_floating_cart

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

st.set_page_config(page_title="전체 요약", page_icon="🏪", layout="wide")

# CSS 로드
if os.path.exists("style.css"):
    with open("style.css", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def get_data():
    file_path = os.path.join('data', 'categorized_data.csv')
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    df = pd.read_csv(file_path)
    df['event'] = df['event'].str.replace(' ', '', regex=False)
    df['price'] = df['price'].astype(str).str.replace(r'[^\d.]', '', regex=True)
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0).astype(int)

    def calc_info(row):
        e, p = row['event'], row['price']
        if e == '1+1': return p // 2, "50%"
        if e == '2+1': return (p * 2) // 3, "33%"
        if e == '3+1': return (p * 3) // 4, "25%"
        return p, "0%"

    df[['unit_price', 'discount_rate']] = df.apply(lambda x: pd.Series(calc_info(x)), axis=1)
    return df.drop_duplicates(subset=['name', 'event', 'brand'])

df = get_data()

init_cart()
render_floating_cart()

st.title(f"🏪 {datetime.now().strftime('%Y년 %m월')} 편의점 행사 정보 통합 보드")

if not df.empty:
    # 필터 영역 (Expander + 한 줄 통합)
    with st.expander("🔍 상세 필터 및 검색", expanded=True):
        # 첫 번째 줄: 검색 및 정렬
        r1_c1, r1_c2 = st.columns([3, 1])
        with r1_c1:
            search_query = st.text_input("📝 검색", "", placeholder="상품명 입력")

            if search_query:
                if 'recent_keywords' not in st.session_state:
                    st.session_state['recent_keywords'] = []
                if search_query in st.session_state['recent_keywords']:
                    st.session_state['recent_keywords'].remove(search_query)
                st.session_state['recent_keywords'].insert(0, search_query)
                st.session_state['recent_keywords'] = st.session_state['recent_keywords'][:5]

        with r1_c2:
            sort_option = st.selectbox("💰 정렬", ["기본", "가격 낮은 순", "가격 높은 순"])

        # 두 번째 줄: 브랜드, 행사, 분류
        r2_c1, r2_c2, r2_c3 = st.columns([1, 1, 1])
        with r2_c1:
            brand_list = sorted(df['brand'].unique().tolist())
            selected_brands = st.multiselect("🏪 브랜드", brand_list, default=brand_list)
        with r2_c2:
            # 행사 필터: SALE, 세일 제외
            event_list = sorted([e for e in df['event'].unique().tolist() if e not in ['SALE', '세일']])
            selected_events = st.multiselect("🎁 행사", event_list, default=event_list)
        with r2_c3:
            # 카테고리 필터
            cat_list = sorted(df['category'].unique().tolist())
            selected_cats = st.multiselect("📂 분류", cat_list, default=cat_list)

    # 데이터 필터링
    filtered_df = df[
        (df['brand'].isin(selected_brands)) & 
        (df['event'].isin(selected_events)) &
        (df['category'].isin(selected_cats)) &
        (df['name'].str.contains(search_query, case=False))
    ]

    if sort_option == "가격 낮은 순":
        filtered_df = filtered_df.sort_values(by='unit_price', ascending=True)
    elif sort_option == "가격 높은 순":
        filtered_df = filtered_df.sort_values(by='unit_price', ascending=False)
    else: 
        # "기본" 정렬일 때: 현재 검색창이 비어있고, 기억된 키워드가 있다면
        if not search_query and 'recent_keywords' in st.session_state and st.session_state['recent_keywords']:
            latest_kwd = st.session_state['recent_keywords'][0]
            # 최근 검색어가 포함된 상품들에 가산점을 줘서 최상단으로 정렬
            filtered_df['is_recommended'] = filtered_df['name'].str.contains(latest_kwd, case=False, na=False).astype(int)
            filtered_df = filtered_df.sort_values(by='is_recommended', ascending=False)
            filtered_df = filtered_df.drop(columns=['is_recommended'])

    # 상품 리스트 출력
    items_per_page = 30
    total_pages = max((len(filtered_df) // items_per_page) + (1 if len(filtered_df) % items_per_page > 0 else 0), 1)

    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1

    query_hash = search_query + str(selected_cats) + str(selected_events) + str(selected_brands) + sort_option
    if 'last_query' not in st.session_state or st.session_state.last_query != query_hash:
        st.session_state.current_page = 1
        st.session_state.last_query = query_hash

    start_idx = (st.session_state.current_page - 1) * items_per_page
    display_df = filtered_df.iloc[start_idx: start_idx + items_per_page]

    if not display_df.empty:
        cols = st.columns(5)
        for idx, (_, row) in enumerate(display_df.iterrows()):
            with cols[idx % 5]:
                img_url = row['img_url'] if pd.notna(row['img_url']) else ""
                st.markdown(f"""
                    <div class="product-card">
                        <div class="img-container"><img src="{img_url}"></div>
                        <div class="product-name">{row['name']}</div>
                        <div style="margin-top: 8px;">
                            <span style="font-size: 1.2rem; font-weight: 800; color: #ffffff;">{row['price']:,}원</span>
                            <span style="font-size: 0.85rem; color: #ff6b6b; font-weight: bold; margin-left: 5px;">({row['discount_rate']}↓)</span>
                        </div>
                        <div class="unit-price-text">개당 <b>{row['unit_price']:,}원</b></div>
                        <div style="margin-top: 5px;">
                            <span style="color:{get_brand_color(row['brand'])}; background:{get_brand_color(row['brand'])}15; padding:2px 6px; border-radius:4px; font-weight:bold; font-size:0.8rem;">📍 {row['brand']}</span>
                            <span class="event-tag" style="margin-left: 5px;">{row['event']}</span>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
                render_cart_button(row, f"cart_summary_{idx}")

        # 페이지네이션
        st.markdown("---")
        _, b1, p_box, b2, _ = st.columns([4, 0.3, 1, 0.3, 4])
        with b1:
            if st.button("❮", key="prev_btn") and st.session_state.current_page > 1:
                st.session_state.current_page -= 1
                st.rerun()
        with p_box:
            st.markdown(f"<div class='page-info-box'>{st.session_state.current_page} / {total_pages}</div>", unsafe_allow_html=True)
        with b2:
            if st.button("❯", key="next_btn") and st.session_state.current_page < total_pages:
                st.session_state.current_page += 1
                st.rerun()
    else:
        st.warning("결과가 없습니다.")
else:
    st.info("데이터를 로드하는 중입니다...")
