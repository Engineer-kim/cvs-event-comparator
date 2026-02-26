import streamlit as st
import pandas as pd
from pathlib import Path
import os
import itertools
import random
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

# ==========================================
# 1. 상수 정의 (Constants)
# ==========================================
MEAL_KEYWORDS = ['도시락', '김밥', '샌드위치', '햄버거', '핫도그', '주먹밥', '샐러드', '면', '밥', '삼각김밥', '국', '찌개', '탕', '즉석밥', '덮밥', '볶음밥', '죽', '컵밥', '밥버거']
SOUP_KEYWORDS = ['국', '찌개', '탕', '전골', '부대찌개', '순두부', '육개장', '곰탕', '설렁탕']
MEAL_EXCLUDE_KEYWORDS = ['도시락김', '김밥김', '삼각김밥용', '볶음밥용', '찌개양념', '국물용', '세트', '재료', '용기', '즉석', '조리']
RICE_STAPLE_KEYWORDS = ['즉석밥', '백미밥', '현미밥', '잡곡밥', '햇반', '오뚜기밥', '밥', '볶음밥', '덮밥', '컵밥', '주먹밥', '김밥', '삼각김밥', '김치볶음밥', '새우볶음밥', '소불고기덮밥']
NOT_RICE_KEYWORDS = ['장조림', '양갱', '스낵', '과자', '초콜릿', '젤리', '사탕', '비스킷', '빵', '케이크', '안주', '반찬', '요리', '소스', '양념', '볶음', '김치', '단무지', '밥도둑', '밥이랑']
SIDE_DISH_KEYWORDS = ['장조림', '볶음', '김치', '고기', '햄', '소시지', '소세지', '참치', '김', '만두', '돈까스', '치킨', '너겟', '젓갈', '절임', '무침', '조림', '튀김', '닭가슴살', '육포', '스테이크']
INTEGRATED_KEYWORDS = ['컵밥', '찌개밥', '국밥', '덮밥']

REDUNDANT_GROUPS = [
    ['물', '생수', '에비앙', '삼다수', '아이시스', '평창수', '워터'],
    ['라면', '컵라면', '불닭', '너구리', '신라면', '짜파게티', '비빔면'],
    ['음료', '콜라', '사이다', '쥬스', '주스', '에이드', '탄산', '커피', '우유', '차', '아메리카노', '라떼']
]

# ==========================================
# 2. 데이터 로드 및 전처리
# ==========================================
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

# ==========================================
# 3. 비즈니스 로직 (조합 생성기)
# ==========================================
def has_redundancy(items):
    """중복된 종류의 상품이 있는지 검사합니다."""
    for group in REDUNDANT_GROUPS:
        count = sum(1 for item in items if any(word in item['name'] for word in group))
        if count > 1:
            return True
    return False

def get_candidate_pools(df, categories, budget):
    """선택된 카테고리별로 후보 상품 리스트를 생성합니다."""
    candidate_items = []
    target_price = budget / len(categories)
    
    for cat in categories:
        cat_df = df[(df['category'] == cat) & (df['price'] <= budget * 0.6)].copy()
        if cat_df.empty:
            continue
            
        cat_df['price_diff'] = (cat_df['price'] - target_price).abs()

        if cat == '식사류':
            mask_include = cat_df['name'].str.contains('|'.join(MEAL_KEYWORDS), case=False, na=False)
            mask_exclude = cat_df['name'].str.contains('|'.join(MEAL_EXCLUDE_KEYWORDS), case=False, na=False)
            meal_items_mask = mask_include & ~mask_exclude
            
            top_items = pd.concat([
                cat_df[meal_items_mask].sort_values(by=['discount_rate', 'price_diff'], ascending=[False, True]),
                cat_df[~meal_items_mask].sort_values(by=['discount_rate', 'price_diff'], ascending=[False, True])
            ]).drop_duplicates(subset=['name']).head(30)
        else:
            top_items = cat_df.sort_values(by=['discount_rate', 'price_diff'], ascending=[False, True]).head(30)
        
        if not top_items.empty:
            pool_list = top_items.to_dict('records')
            candidate_items.append(random.sample(pool_list, min(len(pool_list), 10)))
            
    return candidate_items

def find_best_combinations(df, selected_categories, budget):
    """최적의 꿀조합 상위 5개를 찾아 반환합니다."""
    rice_mask = df['name'].str.contains('|'.join(RICE_STAPLE_KEYWORDS), case=False, na=False)
    not_rice_mask = df['name'].str.contains('|'.join(NOT_RICE_KEYWORDS), case=False, na=False)
    rice_candidates = df[rice_mask & ~not_rice_mask].sort_values(by=['unit_price']).head(15).to_dict('records')
    
    side_mask = df['name'].str.contains('|'.join(SIDE_DISH_KEYWORDS), case=False, na=False)
    side_candidates = df[side_mask].sort_values(by=['unit_price']).head(20).to_dict('records')

    candidate_items = get_candidate_pools(df, selected_categories, budget)
    if len(candidate_items) < len(selected_categories):
        return []

    all_combinations = list(itertools.product(*candidate_items))
    random.shuffle(all_combinations)
    
    valid_combinations = []
    seen_names = set()

    for combo in all_combinations:
        current_items = list(combo)
        if has_redundancy(current_items):
            continue

        # 식사류 짝꿍 맞추기
        if '식사류' in selected_categories:
            has_soup = any(any(k in i['name'] for k in SOUP_KEYWORDS) and not any(k in i['name'] for k in INTEGRATED_KEYWORDS) for i in current_items)
            has_staple_rice = any(any(k in i['name'] for k in RICE_STAPLE_KEYWORDS) and not any(k in i['name'] for k in NOT_RICE_KEYWORDS) for i in current_items)
            is_complete_meal = any(any(k in i['name'] for k in ['도시락', '삼각김밥', '김밥', '컵밥', '덮밥', '샌드위치', '햄버거']) and not any(k in i['name'] for k in MEAL_EXCLUDE_KEYWORDS) for i in current_items)
            
            # 국물 단독일 경우 밥 추가
            if has_soup and not has_staple_rice and not is_complete_meal and rice_candidates:
                rice_added = next((r for r in rice_candidates if sum(i['price'] for i in current_items) + r['price'] <= budget), None)
                if rice_added:
                    current_items.append(rice_added)
                    has_staple_rice = True
                else: continue

            # 맨밥 단독일 경우 반찬 추가
            has_side = any(any(k in i['name'] for k in SIDE_DISH_KEYWORDS) for i in current_items)
            if not has_soup and not is_complete_meal and not has_side and has_staple_rice and side_candidates:
                side_added = next((s for s in side_candidates if s['name'] not in [i['name'] for i in current_items] and sum(i['price'] for i in current_items) + s['price'] <= budget), None)
                if side_added:
                    current_items.append(side_added)
                else: continue

        # 남은 예산 채우기
        # 남은 예산 채우기
        current_total = sum(i['price'] for i in current_items)
        if budget - current_total >= 1000 and len(current_items) < 5: # 아이템 개수도 여유롭게 5개로 늘림
            
            # 1. 먹을 것(식사/간식) 우선 타겟 설정
            target_fill_cats = []
            if '식사류' in selected_categories and '간식류' in selected_categories:
                target_fill_cats = ['식사류', '간식류']
            elif '식사류' in selected_categories:
                target_fill_cats = ['식사류']
            elif '간식류' in selected_categories:
                target_fill_cats = ['간식류']
            else:
                target_fill_cats = selected_categories # 음료, 생수만 선택했을 경우를 대비
                
            # 2. 타겟 카테고리의 상품만 모아서 섞기 (비싼 순 정렬 제거 -> 다양한 상품 추천 유도)
            all_selectable = [item for pool in candidate_items for item in pool if item['category'] in target_fill_cats]
            random.shuffle(all_selectable)

            # 3. 상품 추가
            for extra in all_selectable:
                if extra['name'] not in [i['name'] for i in current_items]:
                    temp_items = current_items + [extra]
                    if not has_redundancy(temp_items) and sum(i['price'] for i in temp_items) <= budget:
                        current_items = temp_items
                        current_total += extra['price']
                        if budget - current_total < 1000 or len(current_items) >= 5:
                            break

        # 최종 조합 저장
        total_price = sum(i['price'] for i in current_items)
        if total_price <= budget:
            combo_names = tuple(sorted([i['name'] for i in current_items]))
            if combo_names not in seen_names:
                saved_money = sum(i['price'] - i['unit_price'] for i in current_items)
                valid_combinations.append({'items': current_items, 'total_price': total_price, 'saved_money': saved_money})
                seen_names.add(combo_names)
                if len(valid_combinations) >= 30:
                    break

    valid_combinations.sort(key=lambda x: (x['total_price'], x['saved_money']), reverse=True)
    return valid_combinations[:5]


# ==========================================
# 4. 화면 UI 출력부 (Streamlit 페이지 로드 시 즉시 실행)
# ==========================================
if os.path.exists("static/css/style.css"):
    with open("static/css/style.css", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

df = load_data()

init_cart()
render_floating_cart()

st.title("🍱 내 예산 맞춤 꿀조합 생성기")
st.markdown("""
    ##### 💰 당신의 예산과 취향을 완벽하게 저격할 편의점 꿀조합을 찾아드려요!
    ##### ✨ 텅장도 든든하게, 입맛도 만족스럽게! 최적의 할인 혜택과 알찬 구성으로 후회 없는 한 끼를 즐겨보세요!
""")
st.write("")

if df.empty:
    st.error("데이터 로딩에 실패했습니다. 관리자에게 문의해주세요.")
    st.stop()

with st.container(border=True):
    st.subheader("🛒 나만의 꿀조합 레시피")
    col1, col2 = st.columns(2)
    with col1:
        budget = st.slider("💰 예산을 알려주세요", min_value=3000, max_value=30000, value=10000, step=1000)
    with col2:
        selected_brands = st.multiselect("🏪 특정 편의점을 선호하시나요? (미선택 시 전체)", options=list(df['brand'].unique()), default=[])

    allowed_categories = ['식사류', '간식류', '음료', '생수']
    filtered_unique_categories = [cat for cat in df['category'].unique() if cat in allowed_categories]

    st.markdown("##### 어떤 종류의 상품을 담고 싶나요? (2개 이상 선택)")
    selected_categories = st.multiselect("카테고리 선택", options=filtered_unique_categories, label_visibility="collapsed")

st.markdown("---")

# 결과를 session_state에 저장해서 rerun 후에도 유지
if 'budget_combinations' not in st.session_state:
    st.session_state.budget_combinations = []

if st.button("✨ 최적의 꿀조합 찾기", use_container_width=True):
    if len(selected_categories) < 2:
        st.warning("⚠️ 최소 2개 이상의 카테고리를 선택해야 조합을 만들 수 있습니다!")
    else:
        with st.spinner("⏳ 최고의 꿀조합을 신중하게 선별하는 중입니다... 잠시만 기다려주세요."):
            filtered_df = df[df['brand'].isin(selected_brands)] if selected_brands else df
            st.session_state.budget_combinations = find_best_combinations(filtered_df, selected_categories, budget)
            st.session_state.budget_searched = True

# 결과 렌더링은 버튼 블록 밖에서 — rerun 후에도 session_state로 유지됨
top_combinations = st.session_state.budget_combinations

if top_combinations:
    st.subheader("🎉 짜잔! 당신을 위한 최고의 꿀조합이 도착했어요!")
    st.markdown("##### 예산을 꽉 채워 풍성하고, 할인 혜택까지 놓치지 않은 알찬 구성!")

    cols = st.columns(len(top_combinations))
    for idx, combo in enumerate(top_combinations):
        with cols[idx]:
            with st.container(border=True):
                st.markdown(f"#### 🥇 추천 {idx + 1}위")
                for item_idx, item in enumerate(combo['items']):
                    img_url = item['img_url'] if pd.notna(item['img_url']) else "https://via.placeholder.com/100"
                    st.image(img_url, width=100)
                    st.markdown(f"**{item['name']}** ({item['brand']})")
                    st.markdown(f"_{item['event']}_ | {item['price']:,}원")
                    cart_key = (item['name'], item['brand'], item['event'])
                    in_cart = is_in_cart(item['name'], item['brand'], item['event'])
                    unit_price = int(item.get('unit_price', item['price']))
                    btn_key = f"budget_cart_{idx}_{item_idx}"
                    if in_cart:
                        if st.button("✅ 담김", key=btn_key, use_container_width=True):
                            remove_from_cart(cart_key)
                            st.rerun()
                    else:
                        if st.button("🛒 담기", key=btn_key, use_container_width=True):
                            add_to_cart(item['name'], item['brand'], item['event'], int(item['price']), unit_price)
                            st.rerun()
                    st.divider()
                st.markdown(f"**합계: {int(combo['total_price']):,}원**")
                st.markdown(f"<span style='color:red; font-weight:bold;'>🔥 {int(combo['saved_money']):,}원 절약!</span>", unsafe_allow_html=True)
elif st.session_state.get('budget_searched') and not top_combinations:
    st.error("😥 아쉽게도 조건에 맞는 꿀조합을 찾지 못했어요. 예산을 조금 더 늘리거나, 다른 카테고리를 선택해보시는 건 어떠세요?")