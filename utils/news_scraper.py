import pandas as pd
import os
import streamlit as st

@st.cache_data(ttl=60)
def fetch_realtime_cvs_news():
    file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'official_event_news.csv')
    try:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df['pub_date'] = pd.to_datetime(df['pub_date'])
            df = df.sort_values(by="pub_date", ascending=False).reset_index(drop=True)
            return df
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()