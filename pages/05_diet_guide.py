import streamlit as st
import pandas as pd
import os
from datetime import datetime

# 1. 페이지 설정
st.set_page_config(page_title="다이어트 & 식단 가이드", page_icon="🏋️", layout="wide")

# 2. 공통 CSS 로드
if os.path.exists("style.css"):
    with open("style.css", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# 3. 스크롤 트리거
def trigger_scroll():
    st.session_state.do_scroll = True

# 4. 스크롤 실행
#    - 100ms 간격으로 2초 동안 반복 실행
#    - Streamlit이 렌더링을 늦게 완료해도 확실히 잡힘
def execute_scroll():
    st.components.v1.html(
        \"\"\"
        <script>
        var scrollCount = 0;
        var maxTries = 20; // 100ms * 20 = 2초간 반복

        function resetScroll() {
            scrollCount++;
            var doc = window.parent.document;

            // 실제로 scrollTop > 0 인 요소를 찾아 리셋
            var allElements = doc.querySelectorAll('*');
            for (var i = 0; i < allElements.length; i++) {
                if (allElements[i].scrollTop > 0) {
                    allElements[i].scrollTop = 0;
                }
            }
            window.parent.scrollTo(0, 0);
            doc.documentElement.scrollTop = 0;
            doc.body.scrollTop = 0;

            // maxTries 횟수 안에서 계속 반복
            if (scrollCount < maxTries) {
                setTimeout(resetScroll, 100);
            }
        }

        // 즉시 시작
        resetScroll();
        </script>
        \"\"\",
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
        if e == '2+1': return p // 3, "33%"
        if e == '3+1': return p // 4, "25%"
        return p, "0%"

    df[['unit_price', 'discount_rate']] = df.apply(lambda x: pd.Series(calc_info(x)), axis=1)
    return df.drop_duplicates(subset=['name', 'event', 'brand'])

df = get_data()

# 5. 타이틀
st.title(f\"🏋️ {datetime.now().strftime('%Y년 %m월')} 다이어트 & 식단 가이드\")

if not df.empty:
    # 6. 상세 필터
    with st.expander(\"🔍 상세 필터 및 테마 선택\", expanded=True):
        # 첫 번째 줄: 검색, 식단 테마, 정렬
        r1_c1, r1_c2, r1_c3 = st.columns([2, 1, 1])
        
        with r1_c1:
            search_query = st.text_input(\"📝 검색\", \"\", placeholder=\"상품명 입력\")
        with r1_c2:
            tags = {
                \"🥤 제로 & 저당\": [\"제로\", \"zero\", \"무가당\", \"슈가프리\", \"0칼로리\"],
                \"🍗 고단백 식단\": [\"단백질\", \"프로틴\", \"닭가슴살\", \"계란\", \"단백\", \"닭가슴\"]
            }
            selected_tag = st.selectbox(\"🎯 식단 테마\", list(tags.keys()))
            keywords = tags[selected_tag]
        with r1_c3:
            sort_option = st.selectbox(\"💰 정렬\", [\"기본\", \"가격 낮은 순\", \"가격 높은 순\"])

        # 두 번째 줄: 브랜드, 행사, 분류
        r2_c1, r2_c2, r2_c3 = st.columns([1, 1, 1])
        with r2_c1:
            brand_list = sorted(df['brand'].unique().tolist())
            selected_brands = st.multiselect(\"🏪 브랜드\", brand_list, default=brand_list)
        with r2_c2:
            event_list = sorted([e for e in df['event'].unique().tolist() if e not in ['SALE', '세일']])
            selected_events = st.multiselect(\"🎁 행사\", event_list, default=event_list)
        with r2_c3:
            cat_list = sorted(df['category'].unique().tolist())
            selected_cats = st.multiselect(\"📂 분류\", cat_list, default=cat_list)

    # 7. 필터링
    pattern = \"|\".join(keywords)
    exclude_pattern = \"|\".join([\"맥주\", \"라이트비어\", \"피죤\", \"필라이트\", \"카스라이트\", \"주류\", \"스팸\", \"베이컨\", \"부대찌개\", \"햄\", \"가그린\", \"구강\", \"리스테린\", \"순수한면\", \"대형\", \"무알콜\", \"제로백젤리\"])

    filtered_df = df[
        (df['name'].str.contains(pattern, case=False, na=False)) &
        (~df['name'].str.contains(exclude_pattern, case=False, na=False)) &
        (df['brand'].isin(selected_brands)) &
        (df['event'].isin(selected_events)) &
        (df['category'].isin(selected_cats)) &
        (df['name'].str.contains(search_query, case=False))
    ]

    if sort_option == \"가격 낮은 순\":
        filtered_df = filtered_df.sort_values(by='unit_price')
    elif sort_option == \"가격 높은 순\":
        filtered_df = filtered_df.sort_values(by='unit_price', ascending=False)
    else:
        filtered_df = filtered_df.sort_values(by='discount_rate', ascending=False)

    # 8. 페이지네이션
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

    # 9. 상품 리스트
    if not display_df.empty:
        st.info(f\"✨ **{selected_tag}** 테마 상품 {len(filtered_df)}건 검색\")

        # ✅ 스크롤 실행 (100ms 간격 2초 반복)
        if st.session_state.get(\"do_scroll\", False):
            execute_scroll()
            st.session_state.do_scroll = False

        cols = st.columns(5)
        for idx, (_, row) in enumerate(display_df.iterrows()):
            with cols[idx % 5]:
                st.markdown(f\"\"\"
                    <div class=\"product-card\">
                        <div style=\"width: 100%; height: 180px; display: flex; align-items: center; justify-content: center; overflow: hidden; background-color: white; border-radius: 10px; margin-bottom: 10px;\">
                            <img src=\"{row['img_url']}\" style=\"max-width: 100%; max-height: 100%; object-fit: contain;\">
                        </div>
                        <div class=\"product-name\" style=\"height: 45px; overflow: hidden;\">{row['name']}</div>
                        <div style=\"margin-top: 8px;\">
                            <span style=\"font-size: 1.2rem; font-weight: 800; color: #ffffff;\">{row['price']:,}원</span>
                            <span style=\"font-size: 0.85rem; color: #ff6b6b; font-weight: bold; margin-left: 5px;\">({row['discount_rate']}↓)</span>
                        </div>
                        <div class=\"unit-price-text\">개당 <b>{row['unit_price']:,}원</b></div>
                        <div class=\"brand-text\">📍 {row['brand']} | <span class=\"event-tag\">{row['event']}</span></div>
                    </div>
                \"\"\", unsafe_allow_html=True)

        # 10. 하단 내비게이션
        st.markdown(\"---\")
        _, b1, p_box, b2, _ = st.columns([4, 0.3, 1, 0.3, 4])

        with b1:
            if st.button(\"❮\", key=\"d_prev\") and st.session_state.diet_page > 1:
                st.session_state.diet_page -= 1
                trigger_scroll()
                st.rerun()

        with p_box:
            st.markdown(
                f\"<div class='page-info-box'>{st.session_state.diet_page} / {total_pages}</div>\",
                unsafe_allow_html=True
            )

        with b2:
            if st.button(\"❯\", key=\"d_next\") and st.session_state.diet_page < total_pages:
                st.session_state.diet_page += 1
                trigger_scroll()
                st.rerun()

    else:
        st.warning(\"결과가 없습니다.\")
else:
    st.info(\"데이터 로딩 중...\")