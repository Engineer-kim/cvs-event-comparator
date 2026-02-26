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

# 1. 페이지 설정
st.set_page_config(page_title="야식 & 안주 가이드", page_icon="🌙", layout="wide")

# 2. 공통 CSS 로드
if os.path.exists("style.css"):
    with open("style.css", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# 3. 스크롤 트리거 (05_diet_guide.py와 동일한 유틸리티)
def trigger_scroll():
    st.session_state.snack_do_scroll = True

def execute_scroll():
    st.components.v1.html(
        """
        <script>
        var scrollCount = 0;
        var maxTries = 20; 
        function resetScroll() {
            scrollCount++;
            var doc = window.parent.document;
            var allElements = doc.querySelectorAll('*');
            for (var i = 0; i < allElements.length; i++) {
                if (allElements[i].scrollTop > 0) {
                    allElements[i].scrollTop = 0;
                }
            }
            window.parent.scrollTo(0, 0);
            doc.documentElement.scrollTop = 0;
            doc.body.scrollTop = 0;
            if (scrollCount < maxTries) {
                setTimeout(resetScroll, 100);
            }
        }
        resetScroll();
        </script>
        """,
        height=0
    )

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

# 4. 타이틀 및 헤더
st.title(f"🌙 {datetime.now().strftime('%Y년 %m월')} 야식 & 안주 가이드")
st.markdown("##### 오늘 밤, 당신의 소중한 혼술과 야식을 책임질 최고의 행사 상품 큐레이션!")

if not df.empty:
    # 5. 상세 필터 및 테마 선택
    with st.expander("🔍 야식 테마 및 상세 필터", expanded=True):
        r1_c1, r1_c2, r1_c3 = st.columns([2, 1, 1])
        
        with r1_c1:
            search_query = st.text_input("📝 상품 검색", "", placeholder="예: 닭발, 감자칩, 소시지")
        with r1_c2:
            snack_themes = {
                "🍺 맥주와 찰떡궁합": ["치킨", "너겟", "소시지", "핫바", "만두", "피자", "감자", "나쵸", "과자", "팝콘", "땅콩", "아몬드", "어포"],
                "🔥 소주 & 매콤안주": ["닭발", "곱창", "막창", "족발", "편육", "육포", "오징어", "황태", "어묵탕", "부대찌개", "매콤", "불닭"],
                "🍜 든든한 야식": ["떡볶이", "라면", "컵라면", "짜장", "짬뽕", "우동", "도시락", "김밥", "삼각김밥", "햄버거"]
            }
            selected_theme = st.selectbox("🎯 야식 테마 선택", list(snack_themes.keys()))
            keywords = snack_themes[selected_theme]
        with r1_c3:
            sort_option = st.selectbox("💰 정렬 방식", ["할인율 순", "가격 낮은 순", "가격 높은 순"])

        r2_c1, r2_c2, r2_c3 = st.columns([1, 1, 1])
        with r2_c1:
            brand_list = sorted(df['brand'].unique().tolist())
            selected_brands = st.multiselect("🏪 편의점", brand_list, default=brand_list)
        with r2_c2:
            event_list = sorted([e for e in df['event'].unique().tolist() if e not in ['SALE', '세일']])
            selected_events = st.multiselect("🎁 행사 유형", event_list, default=event_list)
        with r2_c3:
            cat_list = sorted(df['category'].unique().tolist())
            selected_cats = st.multiselect("📂 상품 카테고리", cat_list, default=cat_list)

    # 6. 필터링 로직
    pattern = "|".join(keywords)
    # 야식과 거리가 먼 생활용품 등 제외 패턴
    exclude_pattern = "|".join(["피죤", "가그린", "칫솔", "치약", "샴푸", "린스", "면도기", "생리대", "마스크", "세제", "멀티비타민"])

    filtered_df = df[
        (df['name'].str.contains(pattern, case=False, na=False)) &
        (~df['name'].str.contains(exclude_pattern, case=False, na=False)) &
        (df['brand'].isin(selected_brands)) &
        (df['event'].isin(selected_events)) &
        (df['category'].isin(selected_cats)) &
        (df['name'].str.contains(search_query, case=False))
    ].copy()

    # 정렬
    if sort_option == "가격 낮은 순":
        filtered_df = filtered_df.sort_values(by='unit_price')
    elif sort_option == "가격 높은 순":
        filtered_df = filtered_df.sort_values(by='unit_price', ascending=False)
    else: # 할인율 순
        filtered_df = filtered_df.sort_values(by='discount_rate', ascending=False)

    # 7. 페이지네이션
    items_per_page = 30
    total_pages = max((len(filtered_df) // items_per_page) + (1 if len(filtered_df) % items_per_page > 0 else 0), 1)

    if 'snack_page' not in st.session_state:
        st.session_state.snack_page = 1

    query_hash = selected_theme + str(selected_brands) + str(selected_events) + str(selected_cats) + search_query + sort_option
    if 'snack_query_hash' not in st.session_state or st.session_state.snack_query_hash != query_hash:
        st.session_state.snack_page = 1
        st.session_state.snack_query_hash = query_hash

    start_idx = (st.session_state.snack_page - 1) * items_per_page
    display_df = filtered_df.iloc[start_idx: start_idx + items_per_page]

    # 8. 결과 출력
    if not display_df.empty:
        st.success(f"🍻 **{selected_theme}** 테마에 어울리는 상품 {len(filtered_df)}개를 찾았습니다!")

        if st.session_state.get("snack_do_scroll", False):
            execute_scroll()
            st.session_state.snack_do_scroll = False

        cols = st.columns(5)
        for idx, (_, row) in enumerate(display_df.iterrows()):
            with cols[idx % 5]:
                st.markdown(f"""
                    <div class="product-card">
                        <div class="img-container">
                            <img src="{row['img_url'] if pd.notna(row['img_url']) else ''}">
                        </div>
                        <div class="product-name" style="height: 45px; overflow: hidden;">{row['name']}</div>
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
                render_cart_button(row, f"cart_snack_{idx}")

        # 9. 하단 페이지네이션 컨트롤
        st.markdown("---")
        _, b1, p_box, b2, _ = st.columns([4, 0.3, 1, 0.3, 4])

        with b1:
            if st.button("❮", key="snack_prev") and st.session_state.snack_page > 1:
                st.session_state.snack_page -= 1
                trigger_scroll()
                st.rerun()

        with p_box:
            st.markdown(f"<div class='page-info-box'>{st.session_state.snack_page} / {total_pages}</div>", unsafe_allow_html=True)

        with b2:
            if st.button("❯", key="snack_next") and st.session_state.snack_page < total_pages:
                st.session_state.snack_page += 1
                trigger_scroll()
                st.rerun()
    else:
        st.warning("아쉽게도 해당 테마에 맞는 행사 상품이 현재 없습니다. 다른 테마나 필터를 선택해보세요!")
else:
    st.info("데이터를 불러오는 중입니다...")

st.markdown("---")
st.caption("※ 상품 정보는 각 편의점 공식 홈페이지의 행사 정보를 바탕으로 제공됩니다.")
