"""
공통 장바구니 유틸리티 모듈
모든 페이지에서 import하여 사용합니다.
"""
import streamlit as st

# 행사 유형별 최적 묶음 단위
EVENT_UNITS = {
    '1+1': 2,
    '2+1': 3,
    '3+1': 4,
}


def init_cart():
    """장바구니 세션 상태 초기화"""
    if 'cart' not in st.session_state:
        st.session_state.cart = {}


def add_to_cart(name: str, brand: str, event: str, price: int, unit_price: int):
    """장바구니에 상품 추가 (이미 있으면 수량 +1)"""
    init_cart()
    key = (name, brand, event)
    if key in st.session_state.cart:
        st.session_state.cart[key]['qty'] += 1
    else:
        st.session_state.cart[key] = {
            'name': name,
            'brand': brand,
            'event': event,
            'unit_price': unit_price,
            'price': price,
            'qty': 1,
        }


def remove_from_cart(key):
    """장바구니에서 상품 제거"""
    init_cart()
    if key in st.session_state.cart:
        del st.session_state.cart[key]


def is_in_cart(name: str, brand: str, event: str) -> bool:
    """상품이 장바구니에 있는지 확인"""
    init_cart()
    return (name, brand, event) in st.session_state.cart


def get_cart_count() -> int:
    """장바구니 전체 수량 합계"""
    init_cart()
    return sum(v['qty'] for v in st.session_state.cart.values())


def calc_actual_total(price: int, event: str, qty: int) -> int:
    """행사 적용 실제 결제금액 계산"""
    unit = EVENT_UNITS.get(event, 1)
    if unit == 1:
        return price * qty
    pay_needed = unit - 1
    sets = qty // pay_needed
    remainder = qty % pay_needed
    return price * (sets * pay_needed + remainder)


def calc_total_received(event: str, qty: int) -> int:
    """증정 포함 실제 받는 총 개수"""
    if event == '1+1':
        return qty * 2
    if event == '2+1':
        return qty + qty // 2
    if event == '3+1':
        return qty + qty // 3
    return qty


def render_cart_warning(item: dict):
    """행사 최적 수량 경고 배너 렌더링"""
    event = item['event']
    unit = EVENT_UNITS.get(event, 1)
    if unit == 1:
        return

    qty = item['qty']
    pay_needed = unit - 1
    if qty % pay_needed == 0:
        return

    need = pay_needed - (qty % pay_needed)
    optimal_qty = qty + need
    optimal_total = item['price'] * pay_needed
    optimal_unit = optimal_total // unit
    current_unit = item['price']

    st.markdown(
        f"""
        <div style="
            background: #2d1f00;
            border-left: 3px solid #ffaa00;
            border-radius: 6px;
            padding: 8px 10px;
            margin: 4px 0 8px 0;
            font-size: 0.8rem;
            color: #ffd280;
            line-height: 1.5;
        ">
            ⚠️ <b>{event} 상품</b>이에요!<br>
            지금 {qty}개 → 개당 <b>{current_unit:,}원</b><br>
            {need}개 더 추가하면 ({optimal_qty}개) → 개당 <b>{optimal_unit:,}원</b> 🎉
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_cart_button(row, button_key: str):
    """
    상품 카드 하단에 장바구니 담기/취소 버튼을 렌더링합니다.
    row: pandas Series (name, brand, event, price, unit_price 필드 필요)
    button_key: 고유한 버튼 키 문자열
    """
    import pandas as pd
    init_cart()
    cart_key = (row['name'], row['brand'], row['event'])
    in_cart = cart_key in st.session_state.cart

    if in_cart:
        if st.button("✅ 담김", key=button_key, use_container_width=True):
            remove_from_cart(cart_key)
            st.rerun()
    else:
        if st.button("🛒 담기", key=button_key, use_container_width=True):
            add_to_cart(
                name=row['name'],
                brand=row['brand'],
                event=row['event'],
                price=int(row['price']),
                unit_price=int(row['unit_price']),
            )
            st.rerun()


def render_floating_cart():
    """
    우측 상단 고정(fixed) 장바구니 팝오버 버튼.
    각 페이지 최상단(st.set_page_config 직후, 타이틀 전)에서 호출합니다.
    CSS로 해당 버튼 블록을 우측 상단에 고정 배치합니다.
    """
    init_cart()
    total_items = get_cart_count()

    # 고정 버튼을 위한 CSS: streamlit이 렌더링한 popover 버튼을 fixed 위치로 이동
    st.markdown(
        """
        <style>
        /* 장바구니 플로팅 버튼 고정 */
        div[data-testid="stPopover"] > div:first-child {
            position: fixed !important;
            top: 60px !important;
            right: 80px !important;
            z-index: 99999 !important;
        }
        div[data-testid="stPopover"] > div:first-child button {
            background: linear-gradient(135deg, #ff6b6b, #ee5253) !important;
            color: white !important;
            border: none !important;
            border-radius: 24px !important;
            padding: 8px 20px !important;
            font-size: 0.95rem !important;
            font-weight: 700 !important;
            box-shadow: 0 4px 15px rgba(255, 107, 107, 0.45) !important;
            cursor: pointer !important;
            white-space: nowrap !important;
            transition: all 0.2s ease !important;
        }
        div[data-testid="stPopover"] > div:first-child button:hover {
            transform: scale(1.06) !important;
            box-shadow: 0 6px 20px rgba(255, 107, 107, 0.6) !important;
        }
        /* 팝오버 내부 여유 공간 */
        div[data-testid="stPopoverBody"] {
            min-width: 360px !important;
            max-height: 72vh !important;
            overflow-y: auto !important;
            padding: 20px !important;
        }
        div[data-testid="stPopoverBody"] hr {
            margin: 10px 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    cart = st.session_state.cart
    badge = f"🛒 {total_items}개" if total_items > 0 else "🛒 0개"

    with st.popover(badge):
        st.markdown("### 🛒 장바구니")

        if not cart:
            st.info("담긴 상품이 없습니다.")
            return

        total_price = 0
        total_saved = 0

        for key, item in list(cart.items()):
            item_total = calc_actual_total(item['price'], item['event'], item['qty'])
            total_received = calc_total_received(item['event'], item['qty'])
            unit_price_actual = item_total // total_received if total_received > 0 else item_total
            total_price += item_total
            total_saved += item['price'] * item['qty'] - item_total

            st.markdown(f"**{item['name']}**")
            st.caption(f"📍 {item['brand']} | {item['event']}")

            qty_col, del_col = st.columns([3, 1])
            with qty_col:
                minus_col, num_col, plus_col = st.columns([1, 1, 1])
                with minus_col:
                    if st.button("－", key=f"fc_minus_{key}"):
                        if st.session_state.cart[key]['qty'] > 1:
                            st.session_state.cart[key]['qty'] -= 1
                        else:
                            del st.session_state.cart[key]
                        st.rerun()
                with num_col:
                    st.markdown(
                        f"<div style='text-align:center;padding-top:6px'>{item['qty']}</div>",
                        unsafe_allow_html=True,
                    )
                with plus_col:
                    if st.button("＋", key=f"fc_plus_{key}"):
                        st.session_state.cart[key]['qty'] += 1
                        st.rerun()
            with del_col:
                if st.button("🗑", key=f"fc_del_{key}"):
                    del st.session_state.cart[key]
                    st.rerun()

            render_cart_warning(item)
            st.markdown(f"결제 예상: **{item_total:,}원** (총 {total_received}개)")
            st.caption(f"개당 {unit_price_actual:,}원")
            st.markdown("---")

        st.markdown(
            f"""
            <div style="background:#1e3a5f;border-radius:10px;padding:14px;text-align:center;margin-bottom:8px;">
                <div style="color:#aaa;font-size:0.8rem;">총 결제 예상금액</div>
                <div style="color:#fff;font-size:1.5rem;font-weight:900;">{total_price:,}원</div>
                <div style="color:#4caf50;font-size:0.85rem;margin-top:4px;">💚 {total_saved:,}원 절약 중!</div>
                <div style="color:#ff6b6b;font-size:0.75rem;margin-top:2px;">※ 행사 적용 실제 결제가 기준</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🗑️ 전체 비우기", use_container_width=True, key="fc_clear_all"):
            st.session_state.cart = {}
            st.rerun()


    """
    사이드바 또는 메인 헤더에 표시할 장바구니 팝오버 전체 UI.
    app.py 또는 각 페이지 상단에서 호출합니다.
    """
    init_cart()
    total_items = get_cart_count()
    cart = st.session_state.cart

    with st.popover(f"🛒 {total_items}개", use_container_width=True):
        if not cart:
            st.info("담긴 상품이 없습니다.")
            return

        total_price = 0
        total_saved = 0

        for key, item in list(cart.items()):
            item_total = calc_actual_total(item['price'], item['event'], item['qty'])
            total_received = calc_total_received(item['event'], item['qty'])
            unit_price_actual = item_total // total_received if total_received > 0 else item_total
            total_price += item_total

            # 절약 금액
            original = item['price'] * item['qty']
            total_saved += original - item_total

            st.markdown(f"**{item['name']}**")
            st.caption(f"📍 {item['brand']} | {item['event']}")

            qty_col, del_col = st.columns([3, 1])
            with qty_col:
                minus_col, num_col, plus_col = st.columns([1, 1, 1])
                with minus_col:
                    if st.button("－", key=f"cart_minus_{key}"):
                        if st.session_state.cart[key]['qty'] > 1:
                            st.session_state.cart[key]['qty'] -= 1
                        else:
                            del st.session_state.cart[key]
                        st.rerun()
                with num_col:
                    st.markdown(
                        f"<div style='text-align:center;padding-top:6px'>{item['qty']}</div>",
                        unsafe_allow_html=True,
                    )
                with plus_col:
                    if st.button("＋", key=f"cart_plus_{key}"):
                        st.session_state.cart[key]['qty'] += 1
                        st.rerun()
            with del_col:
                if st.button("🗑", key=f"cart_del_{key}"):
                    del st.session_state.cart[key]
                    st.rerun()

            render_cart_warning(item)
            st.markdown(f"결제 예상: **{item_total:,}원** (총 {total_received}개)")
            st.caption(f"개당 {unit_price_actual:,}원")
            st.markdown("---")

        # 총합 요약
        st.markdown(
            f"""
            <div style="background:#1e3a5f;border-radius:10px;padding:14px;text-align:center;margin-bottom:8px;">
                <div style="color:#aaa;font-size:0.8rem;">총 결제 예상금액</div>
                <div style="color:#fff;font-size:1.5rem;font-weight:900;">{total_price:,}원</div>
                <div style="color:#ff6b6b;font-size:0.75rem;margin-top:2px;">※ 행사 적용 실제 결제가 기준</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🗑️ 전체 비우기", use_container_width=True, key="cart_clear_all"):
            st.session_state.cart = {}
            st.rerun()