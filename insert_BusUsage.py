import pandas as pd
from sqlalchemy import create_engine
import os



if __name__ == '__main__':
    # 2. SQLAlchemy 엔진 생성
    engine = create_engine(f'mysql+pymysql://root:1234@localhost:3306/Apartment-transaction', echo=False)

    # 3. CSV 파일 목록
    csv_files = [
        'C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/BusUsage_2022_preprocessed.csv',
        'C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/BusUsage_2023_preprocessed.csv'
    ]

    # 4. 컬럼 매핑 정의
    for csv_file in csv_files:
        print(f"📥 처리 중: {os.path.basename(csv_file)}")

        df = pd.read_csv(csv_file)

        # 컬럼 매핑
        df['sigungu'] = df['dep']
        df['year'] = df['year']
        df['month'] = df['month']
        df['day_type'] = df['day_type']
        df['transport_mode'] = df['transport_type']
        df['passengers'] = df['usage_cnt']

        # 필요한 컬럼만 정리
        df = df[['sigungu', 'year', 'month', 'day_type', 'transport_mode', 'passengers']]
        
        # 결측치 제거 (NOT NULL 대상)
        df.dropna(subset=['sigungu', 'year', 'month', 'day_type', 'transport_mode', 'passengers'], inplace=True)

        # DB에 삽입
        df.to_sql('BusUsage', con=engine, if_exists='append', index=False)
        print(f"✅ {len(df)}건 삽입 완료")

    print("🎉 전체 BusUsage 데이터 삽입 완료")