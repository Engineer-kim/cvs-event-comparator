import streamlit as st
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="브랜드별 비교", page_icon="📊", layout="wide")

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
    # 이벤트 표기 정규화
    if 'event' in df.columns:
        df['event'] = df['event'].astype(str).str.replace(r'\s+', '', regex=True)
        df.loc[df['event'].str.contains(r'1\+1', regex=True, na=False), 'event'] = '1+1'
        df.loc[df['event'].str.contains(r'2\+1', regex=True, na=False), 'event'] = '2+1'
        df.loc[df['event'].str.contains(r'3\+1', regex=True, na=False), 'event'] = '3+1'
        df.loc[df['event'].str.contains(r'(?i)sale|세일', regex=True, na=False), 'event'] = 'SALE'

    # 브랜드명 정규화
    if 'brand' in df.columns:
        df['brand'] = df['brand'].astype(str).str.strip()

    df['price'] = pd.to_numeric(df['price'].astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(
        0).astype(int)

    def calc_unit_price(row):
        e, p = row['event'], row['price']
        if e == '1+1': return p // 2
        if e == '2+1': return (p * 2) // 3
        if e == '3+1': return (p * 3) // 4
        return p

    df['unit_price'] = df.apply(calc_unit_price, axis=1)
    return df


df = get_data()

st.title("📊 브랜드별 행사 비교")

if not df.empty:
    brand_colors = {
        "CU": "#9BC621",
        "7Eleven": "#008135",
        "emart24": "#FFB71B",
        "GS25": "#0095D3"
    }

    # 필터 영역 (01_overall_summary와 동일한 스타일)
    with st.expander("🔍 상세 필터 및 검색", expanded=True):
        # 첫 번째 줄: 검색 및 정렬
        r1_c1, r1_c2 = st.columns([3, 1])
        with r1_c1:
            search_query = st.text_input("📝 검색", "", placeholder="상품명 입력")
        with r1_c2:
            sort_option = st.selectbox("💰 정렬", ["기본", "가격 낮은 순", "가격 높은 순"])

        # 두 번째 줄: 브랜드, 행사, 분류
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
        (df['name'].str.contains(search_query, case=False, na=False))
        ]

    st.subheader("📊 브랜드별 행사 통계")

    if not filtered_df.empty:
        # 상세 통계 표 생성 (피벗) - 이벤트별 개수
        event_pivot = filtered_df.groupby(['brand', 'event']).size().unstack(fill_value=0)

        # 이벤트 컬럼 순서 정렬
        desired_order = ['1+1', '2+1', '3+1', '4+1', '5+1']
        existing_cols = [c for c in desired_order if c in event_pivot.columns]
        other_cols = sorted([c for c in event_pivot.columns if c not in desired_order])
        cols_order = existing_cols + other_cols
        event_pivot = event_pivot[cols_order]

        # 정렬: 첫 번째 컬럼(1+1) 기준으로 행의 순서 결정
        if sort_option == "가격 높은 순":
            if len(cols_order) > 0:
                sort_indices = sorted(range(len(event_pivot)),
                                      key=lambda i: event_pivot[cols_order[0]].iloc[i],
                                      reverse=True)
                event_pivot = event_pivot.iloc[sort_indices]
        elif sort_option == "가격 낮은 순":
            if len(cols_order) > 0:
                sort_indices = sorted(range(len(event_pivot)),
                                      key=lambda i: event_pivot[cols_order[0]].iloc[i],
                                      reverse=False)
                event_pivot = event_pivot.iloc[sort_indices]
        else:
            event_pivot = event_pivot.sort_index(ascending=True)

        brand_order = event_pivot.index.tolist()

        # 각 컬럼을 독립적으로 정렬하여 재구성
        sorted_event_data = {}
        for col in cols_order:
            if sort_option == "가격 높은 순":
                sorted_event_data[col] = sorted(event_pivot[col].values, reverse=True)
            elif sort_option == "가격 낮은 순":
                sorted_event_data[col] = sorted(event_pivot[col].values, reverse=False)
            else:
                sorted_event_data[col] = event_pivot[col].values

        # 브랜드명을 인덱스로 유지하면서 정렬된 값으로 DataFrame 재생성
        event_pivot_display = pd.DataFrame(sorted_event_data, index=brand_order)

        # 각 컬럼을 독립적으로 정렬한 후, 그 값들로 막대그래프 데이터 생성
        # 표와 동일한 정렬 방식 사용
        brand_counts_raw = filtered_df['brand'].value_counts()

        # 표의 첫 번째 컬럼(1+1)의 정렬된 값들
        first_col_sorted_values = sorted_event_data[cols_order[0]]

        # 막대그래프: 첫 번째 컬럼(1+1)의 정렬된 값 기준
        brand_counts = pd.DataFrame({
            '브랜드': brand_order,
            '상품 개수': first_col_sorted_values
        })
        brand_counts['브랜드'] = pd.Categorical(brand_counts['브랜드'], categories=brand_order, ordered=True)
        brand_counts = brand_counts.sort_values('브랜드')

        col1, col2 = st.columns(2)
        with col1:
            st.write("✨ 브랜드별 총 행사 상품 수")
            fig1 = px.bar(
                brand_counts,
                x='브랜드',
                y='상품 개수',
                text='상품 개수',
                color='브랜드',
                color_discrete_map=brand_colors,
                category_orders={"브랜드": brand_order}
            )
            fig1.update_layout(xaxis_tickangle=0, showlegend=False, height=400, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            st.write(f"📝 상세 통계 표 ({sort_option})")
            event_brand_counts = event_pivot_display.copy()
            event_brand_counts.index = event_brand_counts.index.astype(str)
            event_brand_counts.index.name = '브랜드'
            st.dataframe(event_brand_counts, use_container_width=True)

        st.subheader("💰 브랜드별 평균 개당 가격")
        avg_price_dict = dict(filtered_df.groupby('brand')['unit_price'].mean())
        avg_price = pd.DataFrame({
            '브랜드': brand_order,
            '평균가격': [avg_price_dict.get(b, 0) for b in brand_order]
        })
        # 브랜드를 범주형으로 설정 (brand_order 순서 유지)
        avg_price['브랜드'] = pd.Categorical(avg_price['브랜드'], categories=brand_order, ordered=True)
        # sort_values를 사용하지 않고 category 순서대로 정렬됨 (Plotly가 인식)
        avg_price = avg_price.sort_values('브랜드', key=lambda x: x.cat.codes)

        fig2 = px.line(avg_price, x='브랜드', y='평균가격', markers=True, category_orders={"브랜드": brand_order})
        fig2.update_traces(line=dict(color="#FF6B6B", width=3), marker=dict(size=10))
        fig2.update_layout(xaxis_tickangle=0, showlegend=False, height=400, hovermode="x unified",
                           margin=dict(l=20, r=20, t=20, b=20))
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("📉 브랜드별 평균 할인율")

        # 1. 할인율 계산 (0으로 나누기 방지)
        filtered_df = filtered_df.copy()
        filtered_df['discount_rate'] = 0.0  # 기본값 0 세팅

        # price가 0보다 큰 정상적인 데이터만 계산
        valid_mask = filtered_df['price'] > 0
        filtered_df.loc[valid_mask, 'discount_rate'] = (
                (filtered_df.loc[valid_mask, 'price'] - filtered_df.loc[valid_mask, 'unit_price'])
                / filtered_df.loc[valid_mask, 'price'] * 100
        )

        # "할인 행사 중인 상품(할인율 > 0)"의 평균
        discount_df = filtered_df[filtered_df['discount_rate'] > 0]
        avg_discount_dict = dict(discount_df.groupby('brand')['discount_rate'].mean())

        # 2. 브랜드별 평균 할인율 집계
        avg_discount = pd.DataFrame({
            '브랜드': brand_order,
            '평균할인율': [avg_discount_dict.get(b, 0) for b in brand_order]
        })

        # 3. Plotly 막대그래프 생성 (Toss 스타일 적용)
        fig3 = px.bar(
            avg_discount,
            x='브랜드',
            y='평균할인율',
            text=[f"{val:.1f}%" for val in avg_discount['평균할인율']],
            color='브랜드',
            color_discrete_map=brand_colors,
            category_orders={"브랜드": brand_order}
        )

        # 테두리 삭제(Flat), 얄쌍한 두께, 깔끔한 폰트
        fig3.update_traces(
            textposition='outside',
            textfont=dict(size=15, family="Pretendard, -apple-system, BlinkMacSystemFont, system-ui, sans-serif",
                          weight='bold'),
            marker_line_width=0,  # 테두리 두께 0 (테두리 없는 플랫한 느낌)
            opacity=1.0,  # 투명도 없이 색상을 쨍하고 선명하게
            width=0.45  # 막대 두께를 얇게 빼서 여백의 미 강조
        )

        # 미세한 차이를 시각적으로 극대화하기 위한 Y축 범위 동적 계산
        min_val = avg_discount['평균할인율'].min()
        max_val = avg_discount['평균할인율'].max()

        # 최소값에서 -2%, 최대값에서 +2% 정도 여유를 두어 돋보기 효과 주기
        y_min = max(0, min_val - 2)  # 최소값이 0 밑으로 뚫고 내려가지 않게 방어
        y_max = max_val + 2 if max_val > 0 else 10

        # 전체 레이아웃 (은은한 배경과 폰트)
        fig3.update_layout(
            xaxis_tickangle=0,
            showlegend=False,
            height=380,
            yaxis_title=None,

            font=dict(family="Pretendard, -apple-system, system-ui, sans-serif", size=13, color="#8B95A1"),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',

            xaxis=dict(
                showgrid=False,
                zeroline=False,
                tickfont=dict(size=14, color="#E5E8EB", weight='bold')
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='rgba(255, 255, 255, 0.05)',
                zeroline=False,
                showticklabels=False,
                range=[y_min, y_max]  # 0이 아닌 y_min부터 시작하도록 변경
            ),
            margin=dict(l=10, r=10, t=40, b=10)
        )
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("📈 브랜드별 핵심 요약")
        # 원본 필터링 데이터에서 브랜드별 통계 계산
        brand_stats = filtered_df.groupby('brand').agg({
            'name': 'count',
            'unit_price': 'mean'
        }).rename(columns={'name': '상품 수', 'unit_price': '평균 단가'})
        # 평균 단가 내림차순으로 정렬
        brand_stats = brand_stats.sort_values('평균 단가', ascending=False)

        if len(brand_stats) > 0:
            m_cols = st.columns(len(brand_stats))
            for i, (brand, row) in enumerate(brand_stats.iterrows()):
                with m_cols[i]:
                    st.metric(brand, f"{int(row['상품 수'])}개", f"평균 {int(row['평균 단가']):,}원")
    else:
        st.warning("필터링된 결과가 없습니다.")
else:
    st.error("데이터를 불러올 수 없습니다.")
