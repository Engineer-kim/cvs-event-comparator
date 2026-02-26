import pandas as pd
import glob
from loguru import logger
import os
from datetime import datetime
import pytz


def clean_and_merge_batch():
    logger.info("데이터 정제를 시작합니다.")

    kst = pytz.timezone('Asia/Seoul')

    # ex) 26년 3월 로 바뀌면 "2603"이 된 파일들 읽어옴
    now_kst = datetime.now(kst)
    current_target = now_kst.strftime("%y%m")

    # data/ 폴더 안의 모든 csv 파일 찾기
    raw_files = glob.glob("data/*.csv")

    brands = ['7Eleven', 'CU', 'emart24', 'GS25']
    all_files = [
        f for f in raw_files
        if any(brand.lower() in f.lower() for brand in brands)
    ]

    if not all_files:
        logger.error(f"data/ 폴더 안에 {current_target}(현재 달)에 해당하는 CSV 파일이 없습니다.")
        return

    logger.info(f"정제 대상 파일 ({current_target} 데이터): {all_files}")

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

    if not df_list:
        logger.error("로드된 데이터가 없습니다.")
        return

        # 데이터 통합
    combined_df = pd.concat(df_list, ignore_index=True)

    # 필수 컬럼 결측치 제거 및 중복 제거
    final_df = combined_df.dropna(subset=['brand', 'name', 'event']).drop_duplicates()

    # 노이즈 데이터 제거
    final_df = final_df[~final_df['name'].str.contains('디폴트 이미지', na=False)]

    # 정제된 데이터 저장
    output_path = "data/cleaned_data.csv"
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.success(f"정제 및 통합 완료: 총 {len(final_df)}개의 데이터가 '{output_path}'에 저장되었습니다.")


if __name__ == "__main__":
    clean_and_merge_batch()