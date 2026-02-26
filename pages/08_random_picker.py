import streamlit as st
import pandas as pd
import random
import os
import time
from utils.cart import init_cart, add_to_cart, is_in_cart, remove_from_cart, render_floating_cart

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

st.set_page_config(page_title="럭키박스", page_icon="🎁", layout="wide")

# CSS 로드
if os.path.exists("style.css"):
    with open("style.css", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def get_data():
    file_path = os.path.join('data', 'categorized_data.csv')
    if not os.path.exists(file_path): return pd.DataFrame()
    return pd.read_csv(file_path)

df = get_data()

init_cart()
render_floating_cart()

st.title("🎁 럭키박스")
st.markdown("##### 오늘의 운명적 득템은? 럭키박스를 열어 당신을 기다리는 행운의 상품을 확인하세요!")

if not df.empty:
    # --- 상단 필터 설정 영역 (일렬 배치) ---
    with st.expander("🛠️ 럭키픽 필터 설정", expanded=True):
        col1, col2 = st.columns([1, 2])
        
        with col1:
            if 'category' in df.columns:
                categories = ["전체"] + sorted(df['category'].dropna().unique().tolist())
            else:
                categories = ["전체"]
            selected_cat = st.selectbox("📂 카테고리 선택", categories)
        
        with col2:
            if 'brand' in df.columns:
                brands = sorted(df['brand'].dropna().unique().tolist())
            else:
                brands = []
            selected_brand = st.multiselect("🏪 브랜드 선택 (미선택 시 전체)", brands, default=brands)

    st.markdown("<br>", unsafe_allow_html=True)
    
    # --- 중앙 실행 버튼 및 결과 출력 영역 ---
    col_l, col_c, col_r = st.columns([1, 2, 1])
    
    with col_c:
        pick_button = st.button("🎁 럭키박스 열기!", use_container_width=True, type="primary")

    st.markdown("---")

    if pick_button:
        # 필터링
        filtered_df = df[df['brand'].isin(selected_brand)] if selected_brand else df
        if selected_cat != "전체":
            filtered_df = filtered_df[filtered_df['category'] == selected_cat]

        if not filtered_df.empty:
            with st.spinner("🎲 행운의 상품을 고르는 중..."):
                time.sleep(1)
                picked = filtered_df.sample(n=1).iloc[0]
                # 결과를 session_state에 저장
                st.session_state.lucky_picked = picked.to_dict()
                st.balloons()
                
                # 결과 출력 (버튼과 동일한 너비의 중앙 컬럼 사용)
                with col_c:
                    st.success(f"🎉 오늘의 추천 상품은 **{picked_item['name']}** 입니다!")
                    
                    # 이미지 URL 처리
                    img_url = picked_item['img_url'] if pd.notna(picked_item['img_url']) else "https://via.placeholder.com/250?text=No+Image"
                    
                    st.markdown(f"""
                        <div style="background-color: #161b22; border: 2px solid #58a6ff; border-radius: 20px; padding: 30px; text-align: center;">
                            <div style="background: white; padding: 10px; border-radius: 15px; display: inline-block; margin-bottom: 20px;">
                                <img src="{img_url}" style="max-width: 250px; max-height: 250px; object-fit: contain;">
                            </div>
                            <h2 style="color: white; margin-bottom: 10px;">{picked_item['name']}</h2>
                            <div style="font-size: 1.5rem; color: #ff6b6b; font-weight: bold; margin-bottom: 10px;">
                                {picked_item['event']} | {int(picked_item['price']):,}원
                            </div>
                            <div style="margin-bottom: 10px;">
                                <span style="color:{get_brand_color(picked_item['brand'])}; background:{get_brand_color(picked_item['brand'])}15; padding:2px 6px; border-radius:4px; font-weight:bold; font-size:1.1rem;">📍 {picked_item['brand']}</span>
                                <span style="color: #8b949e; font-size: 1.1rem; margin-left: 5px;">({picked_item['category']})</span>
                            </div>
                            <hr style="border-color: #30363d; margin: 20px 0;">
                            <p style="color: #58a6ff; font-weight: bold; font-size: 1.1rem;">지금 바로 집 앞 {picked_item['brand']}(으)로 달려가세요! 🏃‍♂️</p>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            st.session_state.lucky_picked = None
            with col_c:
                st.warning("선택하신 조건에 맞는 상품이 없습니다. 필터를 조정해 보세요!")

    # 결과 렌더링은 session_state 기반 — rerun 후에도 유지됨
    picked_item = st.session_state.get('lucky_picked')

    if picked_item:
        with col_c:
            st.success(f"🎉 오늘의 추천 상품은 **{picked_item['name']}** 입니다!")

            img_url = picked_item['img_url'] if pd.notna(picked_item['img_url']) else "https://via.placeholder.com/250?text=No+Image"

            st.markdown(f"""
                <div style="background-color: #161b22; border: 2px solid #58a6ff; border-radius: 20px; padding: 30px; text-align: center;">
                    <div style="background: white; padding: 10px; border-radius: 15px; display: inline-block; margin-bottom: 20px;">
                        <img src="{img_url}" style="max-width: 250px; max-height: 250px; object-fit: contain;">
                    </div>
                    <h2 style="color: white; margin-bottom: 10px;">{picked_item['name']}</h2>
                    <div style="font-size: 1.5rem; color: #ff6b6b; font-weight: bold; margin-bottom: 10px;">
                        {picked_item['event']} | {int(picked_item['price']):,}원
                    </div>
                    <div style="color: #8b949e; font-size: 1.2rem;">
                        📍 {picked_item['brand']} ({picked_item['category']})
                    </div>
                    <hr style="border-color: #30363d; margin: 20px 0;">
                    <p style="color: #58a6ff; font-weight: bold; font-size: 1.1rem;">지금 바로 집 앞 {picked_item['brand']}(으)로 달려가세요! 🏃‍♂️</p>
                </div>
            """, unsafe_allow_html=True)

            cart_key = (picked_item['name'], picked_item['brand'], picked_item['event'])
            in_cart = is_in_cart(picked_item['name'], picked_item['brand'], picked_item['event'])
            unit_price = int(picked_item.get('unit_price', picked_item['price']))
            st.markdown("<br>", unsafe_allow_html=True)
            if in_cart:
                if st.button("✅ 장바구니에 담김 (취소)", use_container_width=True, key="lucky_cart_btn"):
                    remove_from_cart(cart_key)
                    st.rerun()
            else:
                if st.button("🛒 장바구니에 담기", use_container_width=True, key="lucky_cart_btn", type="primary"):
                    add_to_cart(
                        name=picked_item['name'],
                        brand=picked_item['brand'],
                        event=picked_item['event'],
                        price=int(picked_item['price']),
                        unit_price=unit_price,
                    )
                    st.rerun()
    elif 'lucky_picked' not in st.session_state:
        # 최초 대기 상태
        with col_c:
            st.markdown("""
                <div style="height: 300px; display: flex; flex-direction: column; align-items: center; justify-content: center; border: 2px dashed #30363d; border-radius: 20px; color: #8b949e;">
                    <div style="font-size: 4rem; margin-bottom: 10px;">🎁</div>
                    <h3>어떤 상품이 나올까요?</h3>
                    <p>위의 버튼을 눌러 럭키박스를 열어보세요!</p>
                </div>
            """, unsafe_allow_html=True)

else:
    st.error("데이터를 불러올 수 없습니다. data/categorized_data.csv 파일을 확인해주세요.")