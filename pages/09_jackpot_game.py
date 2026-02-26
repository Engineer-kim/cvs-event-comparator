import streamlit as st
import pandas as pd
import random
import time
import os
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

# 1. 데이터 로드
@st.cache_data(ttl=3600)
def load_game_data():
    file_path = os.path.join('data', 'categorized_data.csv')
    if not os.path.exists(file_path):
        return pd.DataFrame()
    return pd.read_csv(file_path)

df = load_game_data()
game_df = df[(df['img_url'].notna()) & (~df['img_url'].str.contains('7-eleven.co.kr', na=False)) & (df['name'].notna())].copy()

init_cart()
render_floating_cart()

# 2. 강제 중앙 정렬 CSS
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Pretendard:wght@400;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Pretendard', sans-serif;
    }

    .stApp {
        background-color: #111111;
    }

    /* 프리미엄 헤더 */
    .premium-header {
        background: linear-gradient(135deg, #1C1C1E 0%, #2C2C2E 100%);
        border-radius: 24px;
        padding: 40px 30px;
        color: #FFFFFF;
        margin-bottom: 30px;
        border: 1px solid #3A3A3C;
        text-align: center;
    }

    /* 슬롯 박스 */
    .slot-box {
        background-color: #1C1C1E;
        border-radius: 20px;
        padding: 20px;
        height: 270px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        border: 1px solid #3A3A3C;
    }

    .img-wrapper {
        background-color: #FFFFFF;
        border-radius: 16px;
        padding: 12px;
        display: flex;
        align-items: center;
        justify-content: center;
        margin-bottom: 12px;
    }

    .product-name {
        color: #E5E5EA;
        font-weight: 600;
        font-size: 0.9rem;
        text-align: center;
    }

    /* 버튼을 감싸는 div 자체를 flex 중앙 정렬 */
    div.stButton {
        text-align: center;
        display: flex;
        justify-content: center;
        width: 100%;
    }

    div.stButton > button {
        background: #3182F6 !important;
        color: white !important;
        border-radius: 18px !important;
        padding: 12px 0px !important;
        font-size: 1.15rem !important;
        font-weight: 700 !important;
        border: none !important;
        box-shadow: 0 10px 20px rgba(49, 130, 246, 0.35) !important;
        width: 100%;
        transition: all 0.2s ease-in-out !important;
    }

    div.stButton > button:hover {
        transform: scale(1.03);
        background-color: #1B64DA !important;
    }
    </style>
    """, unsafe_allow_html=True)

# 3. 세션 상태 초기화
if 'win_count' not in st.session_state:
    st.session_state.win_count = 0

# 4. 헤더 영역
st.markdown(f"""
    <div class="premium-header">
        <h1 style="margin:0; font-size: 2.2rem; font-weight: 700;">잭팟 게임! 🎰</h1>
        <p style="margin: 15px 0 0 0; color: #AEAEB2;">오늘 당신의 럭키 상품을 뽑아보세요!</p>
        <div style="margin-top: 20px; padding: 10px; background: rgba(49, 130, 246, 0.1); border-radius: 12px; display: inline-block; border: 1px solid rgba(49, 130, 246, 0.3);">
            <span style="color: #3182F6; font-weight: 700; font-size: 1.1rem;">🏆 오늘의 당첨 횟수: {st.session_state.win_count}회</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

if not game_df.empty:
    all_categories = sorted(game_df['category'].unique().tolist())
    selected_cat = st.selectbox("카테고리 선택", all_categories, label_visibility="collapsed")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    filtered_df = game_df[game_df['category'] == selected_cat]

    if 'slot_items' not in st.session_state:
        st.session_state.slot_items = []

    # 5. 슬롯 표시 영역
    col1, col2, col3 = st.columns(3)
    slots = st.session_state.slot_items
    cols = [col1, col2, col3]

    for i in range(3):
        with cols[i]:
            if slots:
                st.markdown(f"""
                    <div class='slot-box'>
                        <div class='img-wrapper'><img src="{slots[i]['img_url']}" style="width: 120px; height: 120px; object-fit: contain;"></div>
                        <div class='product-name'>{slots[i]['name']}</div>
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("<div class='slot-box'><div style='font-size: 3rem; color: #3A3A3C;'>?</div></div>", unsafe_allow_html=True)

    # 6. 중앙 정렬 버튼 (여백 추가)
    st.write("")
    st.write("")
    
    _, btn_col, _ = st.columns([1, 2, 1])
    with btn_col:
        if st.button(f"🎰 {selected_cat} 잭팟 시작하기", use_container_width=True):
            with st.spinner("돌아가는 중..."):
                time.sleep(1.0)
                if random.random() < 0.3:
                    winning_item = filtered_df.sample(1).iloc[0]
                    st.session_state.slot_items = [winning_item.to_dict() for _ in range(3)]
                else:
                    st.session_state.slot_items = [filtered_df.sample(1).iloc[0].to_dict() for _ in range(3)]
                
                # 당첨 여부 확인 및 횟수 증가
                if len(set([item['name'] for item in st.session_state.slot_items])) == 1:
                    st.session_state.win_count += 1
                
                st.rerun()

    # 7. 결과창 (당첨 시)
    if slots and len(set([item['name'] for item in slots])) == 1:
        st.balloons()
        st.markdown(f"""
            <div style="background: #1C1C1E; border-radius: 28px; padding: 40px; border: 2px solid #3182F6; text-align: center; margin-top: 40px;">
                <h1 style="margin: 0;">🎊</h1>
                <h2 style="color: white;">슈퍼 잭팟 달성!</h2>
                <p style="color: #3182F6; font-size: 1.5rem; font-weight: 700;">{slots[0]['name']}</p>
            </div>
            <script src="https://cdn.jsdelivr.net/npm/canvas-confetti@1.5.1/dist/confetti.browser.min.js"></script>
            <script>confetti({{ particleCount: 150, spread: 70, origin: {{ y: 0.6 }} }});</script>
        """, unsafe_allow_html=True)

        # 장바구니 담기 버튼
        won_item = slots[0]
        _, jackpot_cart_col, _ = st.columns([1, 2, 1])
        with jackpot_cart_col:
            cart_key = (won_item['name'], won_item['brand'], won_item['event'])
            in_cart = is_in_cart(won_item['name'], won_item['brand'], won_item['event'])
            unit_price = won_item.get('unit_price', won_item['price'])
            st.markdown("<br>", unsafe_allow_html=True)
            if in_cart:
                if st.button("✅ 장바구니에 담김 (취소)", use_container_width=True, key="jackpot_cart_btn"):
                    remove_from_cart(cart_key)
                    st.rerun()
            else:
                if st.button("🛒 당첨 상품 장바구니 담기!", use_container_width=True, key="jackpot_cart_btn", type="primary"):
                    add_to_cart(
                        name=won_item['name'],
                        brand=won_item['brand'],
                        event=won_item['event'],
                        price=int(won_item['price']),
                        unit_price=int(unit_price),
                    )
                    st.rerun()