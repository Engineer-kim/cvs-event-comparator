import streamlit as st
import pandas as pd
from pathlib import Path
import os
import itertools
import random

# ----------------------------------
# 페이지 설정 및 CSS 로드
# ----------------------------------
st.set_page_config(page_title="내 예산 맞춤 꿀조합 생성기", page_icon="🍱", layout="wide")

def local_css(file_name):
    if os.path.exists(file_name):
        with open(file_name, encoding="utf-8") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
local_css("static/css/style.css")

# ----------------------------------
# 데이터 로딩 함수 (파일 내 직접 포함)
# ----------------------------------
@st.cache_data
def load_data():
    file_path = Path("data/categorized_data.csv")
    if not file_path.exists():
        st.error("데이터 파일을 찾을 수 없습니다.")
        return pd.DataFrame()
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        st.error(f"데이터 파일 읽기 실패: {e}")
        return pd.DataFrame()

    required_cols = ["brand", "name", "price", "event", "category", "img_url"]
    if not all(col in df.columns for col in required_cols):
        st.error("데이터 파일에 필수 컬럼이 부족합니다.")
        return pd.DataFrame()

    if df['price'].dtype == object:
        df['price'] = pd.to_numeric(df['price'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce').fillna(0)
    
    df['price'] = df['price'].astype(int)
    df['unit_price'] = df['price'].astype(float)
    df['discount_rate'] = 0.0

    masks = {
        '1+1': (df['price'] / 2, 50.0),
        '2+1': ((df['price'] * 2) / 3, 33.3),
        '3+1': ((df['price'] * 3) / 4, 25.0)
    }
    for event_type, (unit_price_calc, discount) in masks.items():
        mask = df['event'].astype(str).str.contains(event_type.replace('+', r'\+'), na=False)
        df.loc[mask, 'unit_price'] = unit_price_calc
        df.loc[mask, 'discount_rate'] = discount

    df['unit_price'] = df['unit_price'].astype(int)
    return df

# --- 데이터 로드 ---
df = load_data()

st.title("🍱 내 예산 맞춤 꿀조합 생성기")
st.markdown("""
    ##### 💰 당신의 예산과 취향을 완벽하게 저격할 편의점 꿀조합을 찾아드려요!
    ##### ✨ 텅장도 든든하게, 입맛도 만족스럽게! 최적의 할인 혜택과 알찬 구성으로 후회 없는 한 끼를 즐겨보세요!
""")
st.write("")

if df.empty:
    st.error("데이터 로딩에 실패했습니다. 관리자에게 문의해주세요.")
    st.stop()

# ----------------------------------
# 1. 사용자 입력 UI
# ----------------------------------
with st.container(border=True):
    st.subheader("🛒 나만의 꿀조합 레시피")
    col1, col2 = st.columns(2)
    with col1:
        budget = st.slider("💰 예산을 알려주세요", min_value=3000, max_value=30000, value=10000, step=1000)
    with col2:
        selected_brands = st.multiselect(
            "🏪 특정 편의점을 선호하시나요? (미선택 시 전체)", 
            options=list(df['brand'].unique()),
            default=[]
        )

    allowed_categories = ['식사류', '간식류', '음료', '생수']
    filtered_unique_categories = [cat for cat in df['category'].unique() if cat in allowed_categories]

    st.markdown("##### 어떤 종류의 상품을 담고 싶나요? (2개 이상 선택)")
    selected_categories = st.multiselect(
        "카테고리 선택", 
        options=filtered_unique_categories,
        label_visibility="collapsed"
    )

st.markdown("---")

# ----------------------------------
# 2. 조합 생성 및 결과 표시
# ----------------------------------
if st.button("✨ 최적의 꿀조합 찾기", use_container_width=True):
    if len(selected_categories) < 2:
        st.warning("⚠️ 최소 2개 이상의 카테고리를 선택해야 조합을 만들 수 있습니다!")
    else:
        with st.spinner("⏳ 최고의 꿀조합을 신중하게 선별하는 중입니다... 잠시만 기다려주세요."):
            # 1. 브랜드 필터링
            if selected_brands:
                filtered_df = df[df['brand'].isin(selected_brands)]
            else:
                filtered_df = df
            
            # 2. 선택한 카테고리별 후보 추출 및 키워드 정의
            candidate_items = []
            meal_keywords = ['도시락', '김밥', '샌드위치', '햄버거', '핫도그', '주먹밥', '샐러드', '면', '밥', '삼각김밥', '국', '찌개', '탕', '즉석밥', '덮밥', '볶음밥', '죽', '컵밥', '밥버거']
            soup_keywords = ['국', '찌개', '탕', '전골', '부대찌개', '순두부', '육개장', '곰탕', '설렁탕']
            
            meal_exclude_keywords = ['도시락김', '김밥김', '삼각김밥용', '볶음밥용', '찌개양념', '국물용', '소스', '양념', '세트', '재료', '용기', '즉석', '조리']

            rice_staple_keywords = [
                '즉석밥', '백미밥', '현미밥', '잡곡밥', '햇반', '오뚜기밥', '밥', 
                '볶음밥', '덮밥', '컵밥', '주먹밥', '김밥', '삼각김밥', 
                '김치볶음밥', '새우볶음밥', '소불고기덮밥' 
            ]
            not_rice_keywords = [
                '장조림', '양갱', '스낵', '과자', '초콜릿', '젤리', '사탕', '비스킷', '빵', '케이크', 
                '안주', '반찬', '요리', '소스', '양념', '볶음', '김치', '단무지', 
                '밥도둑', '밥이랑' 
            ]
            side_dish_keywords = [
                '장조림', '볶음', '김치', '고기', '햄', '소시지', '소세지', '참치', '김', '만두', '돈까스', '치킨', '너겟', 
                '젓갈', '절임', '무침', '조림', '구이', '튀김', 
                '계란', '어묵', '두부', '샐러드', '소스', '드레싱', '참기름', '고추장', '쌈장', 
                '닭가슴살', '육포', '스테이크' 
            ]
            integrated_keywords = ['컵밥', '찌개밥', '국밥', '덮밥']
            
            redundant_groups = [
                ['물', '생수', '에비앙', '삼다수', '아이시스', '평창수', '워터'],
                ['라면', '컵라면', '불닭', '너구리', '신라면', '짜파게티', '비빔면'],
                ['음료', '콜라', '사이다', '쥬스', '주스', '에이드', '탄산', '커피', '우유', '차', '아메리카노', '라떼']
            ]

            rice_mask = filtered_df['name'].str.contains('|'.join(rice_staple_keywords), case=False, na=False)
            not_rice_mask = filtered_df['name'].str.contains('|'.join(not_rice_keywords), case=False, na=False)
            rice_candidates = filtered_df[rice_mask & ~not_rice_mask].sort_values(by=['unit_price']).head(15).to_dict('records')
            
            side_mask = filtered_df['name'].str.contains('|'.join(side_dish_keywords), case=False, na=False)
            side_candidates = filtered_df[side_mask].sort_values(by=['unit_price']).head(20).to_dict('records')
            
            num_selected_categories = len(selected_categories)
            for cat in selected_categories:
                cat_df = filtered_df[filtered_df['category'] == cat]
                
                target_price = budget / num_selected_categories
                cat_df['price_diff'] = (cat_df['price'] - target_price).abs()

                cat_df = cat_df[cat_df['price'] <= budget * 0.6] 

                if cat == '식사류' and not cat_df.empty:
                    mask_include = cat_df['name'].str.contains('|'.join(meal_keywords), case=False, na=False)
                    mask_exclude = cat_df['name'].str.contains('|'.join(meal_exclude_keywords), case=False, na=False)
                    meal_items_mask = mask_include & ~mask_exclude
                    
                    meal_items = cat_df[meal_items_mask]
                    other_items = cat_df[~meal_items_mask]
                    
                    top_items_for_cat = pd.concat([
                        meal_items.sort_values(by=['discount_rate', 'price_diff'], ascending=[False, True]),
                        other_items.sort_values(by=['discount_rate', 'price_diff'], ascending=[False, True])
                    ]).drop_duplicates(subset=['name']).head(30)
                else:
                    top_items_for_cat = cat_df.sort_values(by=['discount_rate', 'price_diff'], ascending=[False, True]).head(30)
                
                if not top_items_for_cat.empty:
                    pool_list = top_items_for_cat.to_dict('records')
                    sample_size = min(len(pool_list), 10)
                    candidate_items.append(random.sample(pool_list, sample_size))
            
            # 3. 모든 가능한 조합 생성
            if len(candidate_items) == num_selected_categories: 
                all_combinations = list(itertools.product(*candidate_items))
                random.shuffle(all_combinations)
                
                valid_combinations = []
                seen_names = set()

                def has_redundancy(items):
                    for group in redundant_groups:
                        count = 0
                        for item in items:
                            if any(word in item['name'] for word in group):
                                count += 1
                        if count > 1: return True
                    return False

                for combo in all_combinations:
                    current_items = list(combo)
                    
                    if has_redundancy(current_items):
                        continue

                    # --- [식사류 짝꿍 맞추기 로직] ---
                    # '식사류' 카테고리 선택 시에만 동작
                    if '식사류' in selected_categories:
                        has_soup = any(any(skw in item['name'] for skw in soup_keywords) and 
                                       not any(ikw in item['name'] for ikw in integrated_keywords) 
                                       for item in current_items)
                        
                        has_staple_rice = any(any(rkw in item['name'] for rkw in rice_staple_keywords) and 
                                              not any(nrkw in item['name'] for nrkw in not_rice_keywords)
                                              for item in current_items)
                        
                        is_complete_meal = any(
                            any(ikw in item['name'] for ikw in ['도시락', '삼각김밥', '김밥', '컵밥', '덮밥', '샌드위치', '햄버거']) and 
                            not any(ekw in item['name'] for ekw in meal_exclude_keywords) 
                            for item in current_items
                        )
                        
                        if has_soup and not has_staple_rice and not is_complete_meal and rice_candidates:
                            rice_added_success = False
                            for rice_item in rice_candidates:
                                if sum(item['price'] for item in current_items) + rice_item['price'] <= budget:
                                    current_items.append(rice_item)
                                    has_staple_rice = True 
                                    rice_added_success = True
                                    break
                            if not rice_added_success:
                                continue

                        has_side = any(any(skw in item['name'] for skw in side_dish_keywords) for item in current_items)
                        if not has_soup and not is_complete_meal and not has_side and has_staple_rice and side_candidates:
                            side_added_success = False
                            for side_item in side_candidates:
                                if side_item['name'] not in [it['name'] for it in current_items]:
                                    if sum(item['price'] for item in current_items) + side_item['price'] <= budget:
                                        current_items.append(side_item)
                                        side_added_success = True
                                        break
                            if not side_added_success:
                                continue
                    
                    # --- 예산 기반 추가 담기 (중복 방지 및 고단가 위주) ---
                    current_total = sum(item['price'] for item in current_items)
                    
                    if budget - current_total >= 1500 and len(current_items) < 4:
                        all_selectable_candidates = []
                        for pool in candidate_items:
                            all_selectable_candidates.extend(pool)
                        all_selectable_candidates.sort(key=lambda x: x['price'], reverse=True)

                        for extra_item in all_selectable_candidates:
                            if extra_item['name'] not in [it['name'] for it in current_items]:
                                temp_items = current_items + [extra_item]
                                if not has_redundancy(temp_items) and sum(it['price'] for it in temp_items) <= budget:
                                    current_items = temp_items
                                    current_total += extra_item['price']
                                    if budget - current_total < 1000 or len(current_items) >= 4:
                                        break

                    total_price = sum(item['price'] for item in current_items)
                    
                    if total_price <= budget:
                        combo_names = tuple(sorted([item['name'] for item in current_items]))
                        
                        if combo_names not in seen_names:
                            saved_money = sum(item['price'] - item['unit_price'] for item in current_items)
                            valid_combinations.append({
                                'items': current_items,
                                'total_price': total_price,
                                'saved_money': saved_money
                            })
                            seen_names.add(combo_names)
                            
                            if len(valid_combinations) >= 30:
                                break

                # 4. '절약 금액'이 가장 큰 순서대로 정렬하여 상위 5개 추출
                valid_combinations.sort(key=lambda x: (x['total_price'], x['saved_money']), reverse=True)
                top_5 = valid_combinations[:5]
                
                # 5. 결과 출력
                if top_5:
                    st.subheader("🎉 짜잔! 당신을 위한 최고의 꿀조합이 도착했어요!")
                    st.markdown("##### 예산을 꽉 채워 풍성하고, 할인 혜택까지 놓치지 않은 알찬 구성!")
                    
                    cols = st.columns(len(top_5))
                    for idx, combo_data in enumerate(top_5):
                        with cols[idx]:
                            with st.container(border=True):
                                st.markdown(f"#### 🥇 추천 {idx + 1}위")
                                
                                for item in combo_data['items']:
                                    st.image(item['img_url'] if pd.notna(item['img_url']) else "https://via.placeholder.com/100", width=100)
                                    st.markdown(f"**{item['name']}** ({item['brand']})")
                                    st.markdown(f"_{item['event']}_ | {item['price']:,}원")
                                    st.divider()

                                st.markdown(f"**합계: {int(combo_data['total_price']):,}원**")
                                st.markdown(f"<span style='color:red; font-weight:bold;'>🔥 {int(combo_data['saved_money']):,}원 절약!</span>", unsafe_allow_html=True)
                else:
                    st.error("😥 아쉽게도 조건에 맞는 꿀조합을 찾지 못했어요. 예산을 조금 더 늘리거나, 다른 카테고리를 선택해보시는 건 어떠세요?")
            else:
                st.warning("⚠️ 선택하신 카테고리 중 일부에 해당하는 상품이 없습니다. 다른 카테고리를 선택해주세요.")
