import streamlit as st
import pandas as pd
import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

# Groq 클라이언트 초기화
api_key = os.getenv("GROQ_API_KEY")
client = None
if api_key and api_key != "your_groq_api_key_here":
    client = Groq(api_key=api_key)

# 데이터 로드
@st.cache_data
def load_chatbot_data():
    file_path = os.path.join('data', 'categorized_data.csv')
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return None

# ★ 핵심 개선: 맥락을 이해하고 '추천', '다른거'에 유연하게 대응하는 검색 로직
def get_chatbot_context(query, df, messages):
    if df is None or df.empty:
        return "데이터베이스를 찾을 수 없습니다."
    
    # 1. "다른거", "더 알려줘" 같은 이어지는 질문 처리 (이전 맥락 합치기)
    search_query = query
    if any(word in query for word in ['다른', '더', '또', '추천']):
        user_msgs = [m['content'] for m in messages if m['role'] == 'user']
        # 이전 질문이 있다면 현재 질문과 합쳐서 검색 힌트로 사용
        if len(user_msgs) > 1:
            search_query = user_msgs[-2] + " " + query
            
    # 2. 1+1, 2+1 등 특정 이벤트 필터링
    temp_df = df.copy()
    if '1+1' in search_query:
        temp_df = temp_df[temp_df['event'].astype(str).str.contains('1\+1', regex=True)]
    if '2+1' in search_query:
        temp_df = temp_df[temp_df['event'].astype(str).str.contains('2\+1', regex=True)]
        
    # 3. 불용어(검색에 방해되는 일상어) 제거
    stopwords = ['알려줘', '뭐있어', '뭐가', '있어', '주세요', '찾아줘', '어떤', '가장', '추천해줘', '추천', 
                 '행사', '중인거', '다른거', '다른', '더', '또', '거', '보여줘', '은', '는', '이', '가', 
                 '에서', '파는', '?', '중에', '중에서', '상품', '제품']
    
    query_cleaned = search_query
    for word in stopwords:
        query_cleaned = query_cleaned.replace(word, ' ')
        
    # 이벤트 키워드도 검색어에서는 제외
    keywords = [k for k in query_cleaned.split() if k.strip() and k not in ['1+1', '2+1']]
    
    # 4. 키워드가 남았다면 조건 검색 (예: "GS25 우유 추천" -> GS25, 우유 검색)
    if keywords:
        combined_text = temp_df['brand'].astype(str) + " " + temp_df['name'].astype(str) + " " + temp_df['category'].astype(str)
        mask = pd.Series(True, index=temp_df.index)
        for kw in keywords:
            mask = mask & combined_text.str.contains(kw, case=False, na=False)
        related = temp_df[mask]
        
        # 교집합이 없으면 합집합(OR)으로 재검색
        if related.empty:
            or_mask = pd.Series(False, index=temp_df.index)
            for kw in keywords:
                or_mask = or_mask | combined_text.str.contains(kw, case=False, na=False)
            related = temp_df[or_mask]
    else:
        # 5. 특정 키워드 없이 "추천해줘", "1+1 다른거" 라고만 했을 경우 전체(혹은 필터된) 데이터 사용
        related = temp_df
        
    # ★ 핵심 2: 매번 같은 대답을 하지 않도록 무작위(Random)로 15개를 섞어서 뽑아옵니다.
    if not related.empty:
        related = related.sample(n=min(15, len(related)))
    else:
        return "조건에 맞는 행사 상품이 현재 없습니다."
        
    # AI가 읽을 수 있도록 문자열로 변환
    context = ""
    for _, row in related.iterrows():
        context += f"[{row['brand']}] {row['name']} | {row['price']}원 | {row['event']} | {row['category']}\n"
    
    return context

def show_chatbot():
    """오른쪽 하단에 작고 예쁘게 고정되는 팝업 챗봇"""
    
    if 'messages' not in st.session_state:
        st.session_state.messages = []

    st.markdown("""
        <style>
        /* 1. 팝오버 전체 '컨테이너' (버튼 영역) */
        div[data-testid="stPopover"] {
            position: fixed !important;
            bottom: 30px !important;
            right: 30px !important;
            left: auto !important;
            width: 65px !important;  
            height: 65px !important; 
            z-index: 999999 !important;
        }
        
        /* 2. 둥근 챗봇 토글 버튼 */
        div[data-testid="stPopover"] > button {
            width: 65px !important;
            height: 65px !important;
            border-radius: 50% !important;
            background-color: #007bff !important;
            color: white !important;
            border: none !important;
            box-shadow: 0 6px 20px rgba(0,0,0,0.4) !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            font-size: 30px !important;
            padding: 0 !important;
            transition: transform 0.2s ease !important;
        }
        
        div[data-testid="stPopover"] > button:hover {
            transform: scale(1.1) rotate(5deg) !important;
        }
        
        /* 3. 챗봇 대화창 박스 속성 */
        div[data-testid="stPopoverBody"] {
            position: fixed !important;
            bottom: 110px !important;      
            right: 30px !important;        
            
            top: auto !important;          
            left: auto !important;
            transform: none !important;    
            margin: 0 !important;
            
            width: 380px !important; 
            min-width: 380px !important;
            max-width: 380px !important;
            height: 550px !important;      
            
            background-color: #1c2128 !important; 
            border: 1px solid #30363d !important;
            border-radius: 20px !important;
            box-shadow: 0 15px 50px rgba(0,0,0,0.7) !important;
            z-index: 999998 !important;
            padding: 0 !important; 
            overflow: hidden !important;
        }
        </style>
    """, unsafe_allow_html=True)

    with st.popover("💬"):
        st.markdown("""
            <div style='padding: 15px 20px; background-color: #21262d; border-bottom: 1px solid #30363d;'>
                <h4 style='margin: 0; color: #58a6ff; display: flex; align-items: center; gap: 10px;'>
                    🏪 편의점 꿀팁봇
                </h4>
            </div>
        """, unsafe_allow_html=True)
        
        chat_container = st.container(height=380)
        
        with chat_container:
            if not st.session_state.messages:
                st.info("무엇이든 물어보세요! 예: 'GS25 1+1 추천해줘'")
            
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

        if prompt := st.chat_input("질문을 입력하세요..."):
            st.session_state.messages.append({"role": "user", "content": prompt})
            with chat_container:
                with st.chat_message("user"):
                    st.markdown(prompt)

            if client:
                with chat_container:
                    with st.chat_message("assistant"):
                        df = load_chatbot_data()
                        # ★ 검색 시 이전 대화 기록(messages)을 함께 넘겨줍니다.
                        context = get_chatbot_context(prompt, df, st.session_state.messages)
                        
                        # ★ 핵심 3: 챗봇이 사람처럼 대화하도록 프롬프트 수정
                        system_prompt = f"""당신은 센스있고 친절한 편의점 행사 도우미입니다. 
                            아래 제공된 [검색된 데이터]를 바탕으로 자연스럽게 대화하듯 답변하세요.

                            - 사용자가 "추천해줘", "행사 중인 거", "다른 거"라고 물어보면, 데이터 중에서 2~3가지를 골라 왜 추천하는지(예: 간식으로 좋아요, 가성비가 좋아요 등) 가볍게 덧붙여서 매력적으로 추천해주세요.
                            - 만약 [검색된 데이터]에 '조건에 맞는 상품이 없다'고 나오면, "앗, 지금은 원하시는 상품이 없는 것 같아요. 대신 이런 건 어떠세요?" 라며 자연스럽게 대화를 이어가세요.
                            - 절대로 데이터를 지어내지 말고, 엑셀 데이터에 있는 정확한 상품명과 가격, 행사를 말하세요.
                            - 답변 시 한글만 사용하세요(한자 사용 금지).

                            [검색된 데이터]
                            {context}
                        """
                        
                        message_placeholder = st.empty()
                        full_response = ""
                        
                        try:
                            response = client.chat.completions.create(
                                model="llama-3.3-70b-versatile",
                                messages=[{"role": "system", "content": system_prompt}, *st.session_state.messages[-5:]],
                                stream=True,
                            )
                            for chunk in response:
                                if chunk.choices[0].delta.content:
                                    full_response += chunk.choices[0].delta.content
                                    message_placeholder.markdown(full_response + "▌")
                            message_placeholder.markdown(full_response)
                        except Exception as e:
                            full_response = f"현재 연결이 원활하지 않습니다. ({e})"
                            message_placeholder.markdown(full_response)
                
                st.session_state.messages.append({"role": "assistant", "content": full_response})
                st.rerun()