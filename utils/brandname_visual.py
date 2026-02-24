import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. 한글 깨짐 방지 및 스타일 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False
sns.set_palette("pastel")

# 2. 파일 리스트 
file_names = [
    'CU_260223.csv',
    'GS25_260223.csv',
    '7Eleven_260224.csv',
    'cleaned_emart24_260223.csv'
]
labels = ['CU', 'GS25', '7-ELEVEN', 'E-MART24']

# 3. 4행 2열 차트 틀 만들기
fig, axes = plt.subplots(2, 4, figsize=(24, 12))

for i, file in enumerate(file_names):
    try:
        df = pd.read_csv(file)
        
        # [해결 핵심] 모든 공백 제거: '1 + 1' -> '1+1', '2 + 1' -> '2+1'
        df['event'] = df['event'].astype(str).str.replace(' ', '', regex=False)
        
        # 브랜드명 추출
        df['brand_name'] = df['name'].str.extract(r'(.+?)\)').fillna('기타')

        # 1+1 시각화 (이제 '1+1'로 통일됨)
        data_11 = df[df['event'] == '1+1']['brand_name'].value_counts().head(6)
        ax1 = axes[0, i]
        if not data_11.empty:
            ax1.pie(data_11, labels=data_11.index, autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
            ax1.set_title(f'{labels[i]} - 1+1', fontsize=16, fontweight='bold')
        else:
            ax1.text(0.5, 0.5, '1+1 데이터 없음', ha='center', fontsize=12)

        # 2+1 시각화
        data_21 = df[df['event'] == '2+1']['brand_name'].value_counts().head(6)
        ax2 = axes[1, i]
        if not data_21.empty:
            ax2.pie(data_21, labels=data_21.index, autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
            ax2.set_title(f'{labels[i]} - 2+1', fontsize=16, fontweight='bold')
        else:
            ax2.text(0.5, 0.5, '2+1 데이터 없음', ha='center', fontsize=12)

    except Exception as e:
        print(f"{labels[i]} 파일 로드 실패: {e}")

plt.tight_layout(pad=4.0)
plt.show()