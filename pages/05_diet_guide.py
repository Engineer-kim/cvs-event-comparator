import streamlit as st
import pandas as pd
import os
from datetime import datetime
from utils.cart import (
    init_cart, render_cart_button, get_cart_count,
    calc_actual_total, calc_total_received, render_cart_warning,
    render_floating_cart
)

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
st.set_page_config(page_title="다이어트 & 식단 가이드", page_icon="🏋️", layout="wide")

# 2. 공통 CSS 로드
if os.path.exists("style.css"):
    with open("style.css", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# 3. 장바구니 초기화
init_cart()

# 4. 스크롤 트리거
def trigger_scroll():
    st.session_state.do_scroll = True

# 5. 스크롤 실행
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
                if (allElements[i].scrollTop > 0) allElements[i].scrollTop = 0;
            }
            window.parent.scrollTo(0, 0);
            doc.documentElement.scrollTop = 0;
            doc.body.scrollTop = 0;
            if (scrollCount < maxTries) setTimeout(resetScroll, 100);
        }
        resetScroll();
        </script>
        """,
        height=0
    )

# 6. (공통 cart 유틸에서 EVENT_UNITS, calc_actual_total, calc_total_received, render_cart_warning 사용)

# 9. 데이터 로드
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
        if e == '1+1': return p // 2,       "50%"
        if e == '2+1': return (p * 2) // 3, "33%"
        if e == '3+1': return (p * 3) // 4, "25%"
        return p, "0%"

    df[['unit_price', 'discount_rate']] = df.apply(lambda x: pd.Series(calc_info(x)), axis=1)
    return df.drop_duplicates(subset=['name', 'event', 'brand'])

df = get_data()

# 10. (render_cart_warning은 공통 cart 유틸 사용)

# 11. 우측 상단 고정 장바구니 + 타이틀
render_floating_cart()
st.title(f"🏋️ {datetime.now().strftime('%Y년 %m월')} 다이어트 & 식단 가이드")

# 12. 메인 콘텐츠
if not df.empty:
    with st.expander("🔍 상세 필터 및 테마 선택", expanded=True):
        r1_c1, r1_c2, r1_c3 = st.columns([2, 1, 1])
        with r1_c1:
            search_query = st.text_input("📝 검색", "", placeholder="상품명 입력")
        with r1_c2:
            tags = {
                "🥤 제로 & 저당": ["제로", "zero", "무가당", "슈가프리", "0칼로리"],
                "🍗 고단백 식단": ["단백질", "프로틴", "닭가슴살", "계란", "단백", "닭가슴"]
            }
            selected_tag = st.selectbox("🎯 식단 테마", list(tags.keys()))
            keywords = tags[selected_tag]
        with r1_c3:
            sort_option = st.selectbox("💰 정렬", ["기본", "가격 낮은 순", "가격 높은 순"])

        r2_c1, r2_c2, r2_c3 = st.columns([1, 1, 1])
        with r2_c1:
            brand_list = sorted(df['brand'].unique().tolist())
            selected_brands = st.multiselect("🏪 브랜드", brand_list, default=brand_list)
        with r2_c2:
            event_list = sorted([e for e in df['event'].unique().tolist() if e not in ['SALE', '세일']])
            selected_events = st.multiselect("🎁 행사", event_list, default=event_list)
        with r2_c3:
            cat_list = sorted(df['category'].unique().tolist())
            selected_cats = st.multiselect("📂 분류", cat_list, default=cat_list)

    # 13. 필터링
    pattern = "|".join(keywords)
    exclude_pattern = "|".join([
        "맥주", "라이트비어", "피죤", "필라이트", "카스라이트", "주류",
        "스팸", "베이컨", "부대찌개", "햄", "가그린", "구강", "리스테린",
        "순수한면", "대형", "무알콜", "제로백젤리"
    ])

    filtered_df = df[
        (df['name'].str.contains(pattern, case=False, na=False)) &
        (~df['name'].str.contains(exclude_pattern, case=False, na=False)) &
        (df['brand'].isin(selected_brands)) &
        (df['event'].isin(selected_events)) &
        (df['category'].isin(selected_cats)) &
        (df['name'].str.contains(search_query, case=False))
    ]

    if sort_option == "가격 낮은 순":
        filtered_df = filtered_df.sort_values(by='unit_price')
    elif sort_option == "가격 높은 순":
        filtered_df = filtered_df.sort_values(by='unit_price', ascending=False)
    else:
        filtered_df = filtered_df.sort_values(by='discount_rate', ascending=False)

    # 14. 페이지네이션
    items_per_page = 30
    total_pages = max(
        (len(filtered_df) // items_per_page) + (1 if len(filtered_df) % items_per_page > 0 else 0),
        1
    )

    if 'diet_page' not in st.session_state:
        st.session_state.diet_page = 1

    query_hash = selected_tag + str(selected_brands) + str(selected_events) + str(selected_cats) + search_query + sort_option
    if 'diet_query_hash' not in st.session_state or st.session_state.diet_query_hash != query_hash:
        st.session_state.diet_page = 1
        st.session_state.diet_query_hash = query_hash

    start_idx = (st.session_state.diet_page - 1) * items_per_page
    display_df = filtered_df.iloc[start_idx: start_idx + items_per_page]

    # 15. 상품 리스트
    if not display_df.empty:
        st.info(f"✨ **{selected_tag}** 테마 상품 {len(filtered_df)}건 검색")

        if st.session_state.get("do_scroll", False):
            execute_scroll()
            st.session_state.do_scroll = False

        cols = st.columns(5)
        for idx, (_, row) in enumerate(display_df.iterrows()):
            cart_key = (row['name'], row['brand'], row['event'])

            with cols[idx % 5]:
                st.markdown(f"""
                    <div class="product-card">
                        <div style="width:100%;height:180px;display:flex;align-items:center;
                                    justify-content:center;overflow:hidden;background-color:white;
                                    border-radius:10px;margin-bottom:10px;">
                            <img src="{row['img_url']}" style="max-width:100%;max-height:100%;object-fit:contain;">
                        </div>
                        <div class="product-name" style="height:45px;overflow:hidden;">{row['name']}</div>
                        <div style="margin-top:8px;">
                            <span style="font-size:1.2rem;font-weight:800;color:#ffffff;">{row['price']:,}원</span>
                            <span style="font-size:0.85rem;color:#ff6b6b;font-weight:bold;margin-left:5px;">({row['discount_rate']}↓)</span>
                        </div>
                        <div class="unit-price-text">개당 <b>{row['unit_price']:,}원</b></div>
                        <div style="margin-top: 5px;">
                            <span style="color:{get_brand_color(row['brand'])}; background:{get_brand_color(row['brand'])}15; padding:2px 6px; border-radius:4px; font-weight:bold; font-size:0.8rem;">📍 {row['brand']}</span>
                            <span class="event-tag" style="margin-left: 5px;">{row['event']}</span>
                        </div>
                    </div>
                """, unsafe_allow_html=True)

                render_cart_button(row, f"cart_diet_{idx}")

        # 16. 하단 페이지 내비게이션
        st.markdown("---")
        _, b1, p_box, b2, _ = st.columns([4, 0.3, 1, 0.3, 4])

        with b1:
            if st.button("❮", key="d_prev") and st.session_state.diet_page > 1:
                st.session_state.diet_page -= 1
                trigger_scroll()
                st.rerun()

        with p_box:
            st.markdown(
                f"<div class='page-info-box'>{st.session_state.diet_page} / {total_pages}</div>",
                unsafe_allow_html=True
            )

        with b2:
            if st.button("❯", key="d_next") and st.session_state.diet_page < total_pages:
                st.session_state.diet_page += 1
                trigger_scroll()
                st.rerun()

    else:
        st.warning("결과가 없습니다.")
else:
    st.info("데이터 로딩 중...")