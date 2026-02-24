import pandas as pd
import glob
from loguru import logger
import os

def clean_and_merge():
    logger.info("데이터 정제를 시작합니다.")
    
    all_files = glob.glob("*.csv")
    
    exclude_files = ["cleaned_data.csv", "categorized_data.csv", "7Eleven_260223.csv"]
    all_files = [f for f in all_files if f not in exclude_files]
    
    if not all_files:
        logger.error("폴더 안에 처리할 CSV 파일이 없습니다.")
        return

    logger.info(f"정제 대상 파일: {all_files}")

    df_list = []
    
    for file in all_files:
        try:
            df = pd.read_csv(file, encoding='utf-8-sig')
            
            df['price'] = df['price'].astype(str).str.replace(r'[^0-9]', '', regex=True)
            df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0).astype(int)
            
            df_list.append(df)
            logger.info(f"파일 로드 완료: {file}")
            
        except Exception as e:
            logger.error(f"{file} 처리 중 오류 발생: {e}")

    combined_df = pd.concat(df_list, ignore_index=True)
    final_df = combined_df.dropna(subset=['brand', 'name', 'event']).drop_duplicates()
    
    final_df = final_df[~final_df['name'].str.contains('디폴트 이미지', na=False)]

    final_df.to_csv("cleaned_data.csv", index=False, encoding='utf-8-sig')
    logger.success(f"정제 및 통합 완료: 총 {len(final_df)}개의 데이터가 'cleaned_data.csv'에 저장되었습니다.")

if __name__ == "__main__":
    clean_and_merge()