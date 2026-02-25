import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster
from pyproj import Transformer
import os
import urllib.parse

# --- 페이지 설정 ---
st.set_page_config(page_title="편의점 지점 지도", page_icon="📍", layout="wide")

# CSS 로드
if os.path.exists("style.css"):
    with open("style.css", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# 데이터 로드
@st.cache_data(ttl=3600)
def get_processed_data():
    file_path = os.path.join('data', 'filtered_convenience_stores.csv')
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    df = pd.read_csv(file_path)
    df = df.dropna(subset=['x', 'y', 'adres'])
    
    # 주소 파싱
    addr_split = df['adres'].str.split(n=2, expand=True)
    df['city'] = addr_split[0].fillna("미분류")
    df['district'] = addr_split[1].fillna("미분류")
    
    # 좌표 변환
    transformer = Transformer.from_crs("epsg:3857", "epsg:4326", always_xy=True)
    lon, lat = transformer.transform(df['x'].values, df['y'].values)
    df['lat'] = lat
    df['lon'] = lon
    
    # 유효 범위 필터링
    df = df[(df['lat'] > 30) & (df['lat'] < 40) & (df['lon'] > 120) & (df['lon'] < 135)]
    return df

df_all = get_processed_data()

# 팝업 디자인
def get_pretty_popup(row):
    kakao_link = f"https://map.kakao.com/link/search/{urllib.parse.quote(row['fclty_nm'])}"
    naver_link = f"https://map.naver.com/v5/search/{urllib.parse.quote(row['fclty_nm'])}"
    return f"""
    <div style="font-family: sans-serif; width: 220px;">
        <h4 style="margin: 0 0 5px 0; color: #333;">{row['fclty_nm']}</h4>
        <p style="font-size: 13px; color: #444; margin: 0;">{row['adres']}</p>
        <hr style="margin: 10px 0;">
        <div style="display: flex; gap: 5px;">
            <a href="{kakao_link}" target="_blank" style="flex: 1; background: #FAE100; color: #3C1E1E; text-decoration: none; padding: 8px; border-radius: 6px; text-align: center; font-weight: bold; font-size: 12px;">카카오</a>
            <a href="{naver_link}" target="_blank" style="flex: 1; background: #03C75A; color: white; text-decoration: none; padding: 8px; border-radius: 6px; text-align: center; font-weight: bold; font-size: 12px;">네이버</a>
        </div>
    </div>
    """

st.title("📍 편의점 지도")
st.caption("지역을 선택하여 상세 위치를 확인하세요. (스크롤이 유지됩니다)")

@st.fragment
def map_section():
    # 1. 상단 필터
    with st.container():
        with st.expander("🔍 상세 지역 및 브랜드 필터", expanded=True):
            r1_c1, r1_c2, r1_c3 = st.columns([1, 1, 2])
            
            with r1_c1:
                all_cities = sorted(df_all['city'].unique().tolist())
                selected_city = st.selectbox("📍 시/도 선택", ["전국 (요약)"] + all_cities, key="f_city")
                
            with r1_c2:
                if selected_city == "전국 (요약)":
                    st.selectbox("🚩 시/군/구 선택", ["전체"], disabled=True, key="f_dist_dis")
                    selected_dist = "전체"
                else:
                    city_data = df_all[df_all['city'] == selected_city]
                    dist_options = ["전체 (시/도 요약)"] + sorted(city_data['district'].unique().tolist())
                    selected_dist = st.selectbox("🚩 시/군/구 선택", dist_options, key="f_dist")
                    
            with r1_c3:
                brand_list = sorted(df_all['brand'].unique().tolist())
                selected_brands = st.multiselect("🏪 브랜드 선택", brand_list, default=brand_list, key="f_brand")

    # 2. 필터링
    view_df = df_all[df_all['brand'].isin(selected_brands)]
    
    # 3. 지도 설정 계산
    map_center = [36.5, 127.5]
    zoom_level = 7
    mode = "national"

    if selected_city != "전국 (요약)":
        city_df = view_df[view_df['city'] == selected_city]
        if not city_df.empty:
            map_center = [city_df['lat'].mean(), city_df['lon'].mean()]
            zoom_level = 10
            mode = "city"
            
            if selected_dist != "전체 (시/도 요약)":
                dist_df = city_df[city_df['district'] == selected_dist]
                if not dist_df.empty:
                    map_center = [dist_df['lat'].mean(), dist_df['lon'].mean()]
                    zoom_level = 14
                    mode = "district"

    # 4. 지도 생성
    m = folium.Map(location=map_center, zoom_start=zoom_level, tiles="cartodbpositron")
    
    # CSS 주입 (Folium 내부용 - 핀이 안보이는 문제 해결)
    css = """
    <style>
    .map-sum-pin {
        border: 2.5px solid white;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white !important;
        font-weight: 900;
        box-shadow: 0 4px 10px rgba(0,0,0,0.25);
        font-family: 'Malgun Gothic', sans-serif;
        text-align: center;
    }
    .national-pin { 
        background: linear-gradient(135deg, #58a6ff 0%, #3a76d2 100%); 
    }
    .city-pin { 
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5253 100%); 
    }
    </style>
    """
    m.get_root().header.add_child(folium.Element(css))
    
    if mode == "national":
        summary = view_df.groupby('city').agg({'lat':'mean', 'lon':'mean', 'brand':'count'}).reset_index()
        for _, row in summary.iterrows():
            folium.Marker(
                location=[row['lat'], row['lon']],
                icon=folium.DivIcon(
                    html=f'<div class="map-sum-pin national-pin" style="width:48px; height:48px; font-size:13px;">{row["brand"]:,}</div>',
                    icon_size=(48, 48), icon_anchor=(24, 24)
                ),
                tooltip=f"{row['city']}: {row['brand']:,}개"
            ).add_to(m)

    elif mode == "city":
        summary = city_df.groupby('district').agg({'lat':'mean', 'lon':'mean', 'brand':'count'}).reset_index()
        for _, row in summary.iterrows():
            folium.Marker(
                location=[row['lat'], row['lon']],
                icon=folium.DivIcon(
                    html=f'<div class="map-sum-pin city-pin" style="width:42px; height:42px; font-size:11px;">{row["brand"]:,}</div>',
                    icon_size=(42, 42), icon_anchor=(21, 21)
                ),
                tooltip=f"{row['district']}: {row['brand']:,}개"
            ).add_to(m)

    elif mode == "district":
        marker_cluster = MarkerCluster().add_to(m)
        brand_config = {"CU": "purple", "GS25": "blue", "세븐일레븐": "green", "이마트24": "orange"}
        for idx, row in dist_df.iterrows():
            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=folium.Popup(get_pretty_popup(row), max_width=300),
                tooltip=row['fclty_nm'],
                icon=folium.Icon(color=brand_config.get(row['brand'], 'gray'), icon='shopping-cart', prefix='fa')
            ).add_to(marker_cluster)

    # 안내 메시지
    if mode == "national": st.info("💡 위 필터에서 **시/도**를 선택하면 구 단위 요약을 볼 수 있습니다.")
    elif mode == "city": st.info(f"💡 **{selected_city}** 내의 상세 **시/군/구**를 선택해 보세요.")
    elif mode == "district": st.success(f"✅ **{selected_city} {selected_dist}** 지역 매장 {len(dist_df):,}개 표시 중")

    # 5. 지도 출력
    st_folium(m, key="main_map_stable", width=None, height=850, use_container_width=True, returned_objects=[])

if not df_all.empty:
    map_section()
else:
    st.error("데이터 로드 실패")
