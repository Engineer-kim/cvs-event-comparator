import pandas as pd
from loguru import logger

def classify_product(name):
    name = str(name)
    
    household = [
        '샴푸', '린스', '컨디셔너', '엘라스틴', '세제', '제습제', '페브리즈', 
        '다우니', '면도기', '중형', '대형', '라이너', '폼', '탈취', '테크', 
        '샤프란', '바디피트', '좋은느낌', '디어스킨', '칫솔', '치약', '비누', 
        '티슈', '물티슈', '밴드', '가글', '핸드크림', '하기스', '아우라', 'LG'
    ]
    if any(word in name for word in household):
        return "생활/위생용품"

    snack = [
        '킷캣', '쿠키', '하리보', '초코', '베리', '캔디', '젤리', '스낵', 
        '과자', '허쉬', '아몬드', '칩', '봉지', '껌', '아이스크림', '하겐', 
        '마카롱', '파인트', '바', '콘', '초콜릿', '마쉬멜로우', '너츠', '견과',
        '짱셔요', '후룻컵', '크래커', '엠앤엠즈'
    ]
    if any(word in name for word in snack):
        return "간식류"


    meal = [
        '밥', '찌개', '육개장', '곰탕', '해장국', '사골', '비비고', '피자', 
        '만두', '오뚜기밥', '선지', '된장찌개', '참치', '김치찌개', '컵밥', 
        '도시락', '삼각김밥', '김밥', '샌드위치', '햄버거', '죽', '국밥', 
        '면', '라면', '파스타', '안주야'
    ]
    if any(word in name for word in meal):
        return "식사류"

    beverage = [
        'ml', 'L', '콜라', '사이다', '소다', '차', '보리', '타임', '에이드', 
        '커피', '쥬스', '음료', '스프라이트', '맥콜', '워터', '우유', '라떼', 
        '드링크', '탄산'
    ]
    if any(word in name for word in beverage):
        return "음료"

    return "기타"

def run_categorization():
    logger.info("새로운 키워드로 카테고리 분류를 시작합니다.")
    try:
        df = pd.read_csv('cleaned_data.csv', encoding='utf-8-sig')
        
    
        df['category'] = df['name'].apply(classify_product)
        

        df.to_csv('categorized_data.csv', index=False, encoding='utf-8-sig')
        
        logger.success("분류 완료")
        print("\n[ 카테고리별 데이터 분포 ]")
        print(df['category'].value_counts())
        
    except FileNotFoundError:
        logger.error("cleaned_data.csv 파일이 없습니다. 정제 코드를 먼저 실행하세요.")

if __name__ == "__main__":
    run_categorization()