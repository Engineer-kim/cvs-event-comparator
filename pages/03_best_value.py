import streamlit as st
import pandas as pd
import os
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

st.set_page_config(page_title="가성비 비교", page_icon="💎", layout="wide")

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
    df['price'] = pd.to_numeric(df['price'].astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(
        0).astype(int)

    def calc_info(row):
        e, p = row['event'], row['price']
        if e == '1+1': return p // 2, 50.0, "50%"
        if e == '2+1': return (p * 2) // 3, 33.3, "33%"
        if e == '3+1': return (p * 3) // 4, 25.0, "25%"
        return p, 0.0, "0%"

    df[['unit_price', 'discount_num', 'discount_rate']] = df.apply(lambda x: pd.Series(calc_info(x)), axis=1)
    return df


df = get_data()

init_cart()
render_floating_cart()

st.title("💎 최고의 가성비 아이템 (할인율 TOP 50)")

if not df.empty:
    # 필터 영역
    with st.expander("🔍 상세 필터 및 검색", expanded=True):
        r1_c1, r1_c2 = st.columns([3, 1])
        with r1_c1:
            search_query = st.text_input("📝 검색", "", placeholder="상품명 입력")
        with r1_c2:
            # 아래 코드의 정렬 옵션 반영
            sort_option = st.selectbox("💰 정렬", ["가성비 순 (할인율)", "가격 낮은 순", "가격 높은 순"])

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

    # 데이터 필터링
    filtered_df = df[
        (df['brand'].isin(selected_brands)) &
        (df['event'].isin(selected_events)) &
        (df['category'].isin(selected_cats)) &
        (df['name'].str.contains(search_query, case=False))
        ].copy()

    # 🔥 4️⃣ 정렬 로직 반영 (체감가 및 할인율 기준)
    if sort_option == "가격 낮은 순":
        best_value_df = filtered_df.sort_values(by='unit_price', ascending=True)
    elif sort_option == "가격 높은 순":
        best_value_df = filtered_df.sort_values(by='unit_price', ascending=False)
    else:  # 가성비 순 (할인율 높은 순 -> 가격 낮은 순)
        best_value_df = filtered_df.sort_values(by=['discount_num', 'unit_price'], ascending=[False, True])

    # TOP 50 제한
    best_value_df = best_value_df.head(50).reset_index(drop=True)

    if not best_value_df.empty:
        # -------------------------
        # 페이지네이션 (세션 상태 반영)
        # -------------------------
        items_per_page = 9
        total_pages = max((len(best_value_df) - 1) // items_per_page + 1, 1)

        if "best_value_page" not in st.session_state:
            st.session_state.best_value_page = 1

        # 필터 변경 등으로 페이지 수가 줄어들었을 경우 대비
        if st.session_state.best_value_page > total_pages:
            st.session_state.best_value_page = 1

        start_idx = (st.session_state.best_value_page - 1) * items_per_page
        page_items = best_value_df.iloc[start_idx: start_idx + items_per_page]

        st.divider()

        for _, row in page_items.iterrows():
            with st.container():
                c1, c2, c3 = st.columns([1.5, 4, 2])
                with c1:
                    img_url = row['img_url'] if pd.notna(row['img_url']) else ""
                    st.image(img_url, width=120)
                with c2:
                    st.markdown(f"### {row['name']}")
                    brand_color = get_brand_color(row['brand'])
                    st.markdown(
                        f"<span style='color:{brand_color}; background:{brand_color}15; padding:2px 6px; border-radius:4px; font-weight:bold; font-size:0.8rem;'>📍 {row['brand']}</span> | {row['category']} | <span class='event-tag'>{row['event']}</span>",
                        unsafe_allow_html=True)
                with c3:
                    st.markdown(f"<h2 style='color:#ff6b6b; margin-bottom:0;'>{row['discount_rate']} 할인</h2>",
                                unsafe_allow_html=True)
                    st.markdown(f"#### 개당 {int(row['unit_price']):,}원")
                    st.caption(f"정가 {int(row['price']):,}원")
                    render_cart_button(row, f"cart_best_{int(row.name)}")
                st.divider()

        # -------------------------
        # 페이지네이션 컨트롤 (버튼형 UI)
        # -------------------------
        _, b1, p_box, b2, _ = st.columns([4, 0.5, 1, 0.5, 4])

        with b1:
            if st.button("❮", key="best_prev_btn") and st.session_state.best_value_page > 1:
                st.session_state.best_value_page -= 1
                st.rerun()

        with p_box:
            st.markdown(
                f"<div class='page-info-box' style='text-align:center; padding-top:10px;'>{st.session_state.best_value_page} / {total_pages}</div>",
                unsafe_allow_html=True
            )

        with b2:
            if st.button("❯", key="best_next_btn") and st.session_state.best_value_page < total_pages:
                st.session_state.best_value_page += 1
                st.rerun()
    else:
        st.warning("결과가 없습니다.")
else:
    st.error("데이터가 없습니다.")