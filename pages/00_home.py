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

# 2. 퀵 메뉴 카드
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

# 3. 하단 브랜드 로고 섹션
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
