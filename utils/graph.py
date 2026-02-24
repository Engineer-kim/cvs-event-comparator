import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. 한글 깨짐 방지 (Windows: Malgun Gothic, Mac: AppleGothic)
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# 2. 파일 리스트 및 편의점 이름 설정
file_names = [
    'CU_260223.csv',
    'GS25_260223.csv',
    '7Eleven_260224.csv',
    'cleaned_emart24_260223.csv'
]
labels = ['CU', 'GS25', '7-ELEVEN', 'E-MART24']

# 3. 데이터 로드 및 통합 작업
all_data = []

for file, label in zip(file_names, labels):
    try:
        df = pd.read_csv(file)
        
        # [전처리] 혹시 모를 가격 컬럼 숫자형 변환 (쉼표, '원' 제거)
        if df['price'].dtype == 'object':
            df['price'] = df['price'].str.replace(',', '').str.replace('원', '').astype(int)
        
        # [전처리] 이벤트명 공백 제거 (이마트24 '1 + 1' 대응)
        df['event'] = df['event'].astype(str).str.replace(' ', '', regex=False)
        
        # 편의점 구분 컬럼 추가
        df['store'] = label
        all_data.append(df)
    except Exception as e:
        print(f"{label} 파일 로드 중 오류 발생: {e}")

# 전체 데이터 하나로 통합
total_df = pd.concat(all_data, ignore_index=True)

# --- [분석 1] 통계적 요약 정보 출력 ---
print("=== 4대 편의점 가격 통계 비교 ===")
summary = total_df.groupby('store')['price'].describe()
print(summary)

# --- [분석 2] 시각화 (3개 영역) ---
# 전체 그래프 크기 설정
fig, axes = plt.subplots(1, 3, figsize=(24, 8))

# (1) Box Plot: 가격의 범위와 이상치(튀는 값) 확인
# 15,000원 이하 상품으로 제한하여 가독성 높임
sns.boxplot(x='store', y='price', data=total_df[total_df['price'] <= 15000], 
            ax=axes[0], palette='Set3')
axes[0].set_title('1) 편의점별 가격 분포 (1.5만원 이하)', fontsize=16, fontweight='bold')
axes[0].set_ylabel('가격(원)')

# (2) KDE Plot: 가격 밀도 그래프 (어느 가격대에 상품이 가장 많은가?)
# 5,000원 이하 가성비 구간 집중 분석
for label in labels:
    sns.kdeplot(total_df[(total_df['store'] == label) & (total_df['price'] <= 10000)]['price'], 
                label=label, ax=axes[1], fill=True, alpha=0.1)
axes[1].set_title('2) 주요 가격대 밀도 분석 (1만원 이하)', fontsize=16, fontweight='bold')
axes[1].set_xlabel('가격(원)')
axes[1].legend()

# (3) Violin Plot: 행사 종류별 가격 분포 차이
# 1+1 상품과 2+1 상품의 가격 전략 비교
sns.violinplot(x='event', y='price', hue='store', 
               data=total_df[total_df['price'] <= 10000], ax=axes[2], split=False)
axes[2].set_title('3) 행사 유형별 가격 분포 비교 (1만원 이하)', fontsize=16, fontweight='bold')
axes[2].set_ylabel('가격(원)')

# 레이아웃 조정 및 출력
plt.tight_layout()
plt.show()