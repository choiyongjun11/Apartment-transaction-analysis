import pandas as pd
from sqlalchemy import create_engine
import os



if __name__ == '__main__':
    # 2. SQLAlchemy 엔진 생성
    engine = create_engine(f'mysql+pymysql://root:1234@localhost:3306/Apartment-transaction', echo=False)

    # 3. agency_code → name (sigungu) 매핑 테이블 로드
    agency_df = pd.read_csv('C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/Agency_preprocessed.csv')  # 컬럼: agency_code, name
    agency_df = agency_df[['agency_code', 'name']].drop_duplicates()
    agency_df = agency_df.rename(columns={'name': 'sigungu'})  # 이름을 sigungu로 변경

    # 4. 인구 파일 목록
    population_files = [
        'C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/PopulationMonthly_2022_preprocessed.csv',
        'C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/PopulationMonthly_2023_preprocessed.csv',
        'C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/PopulationMonthly_2024_preprocessed.csv'
    ]

    for pop_file in population_files:
        print(f"📥 처리 중: {os.path.basename(pop_file)}")

        # 인구 데이터 로드
        df = pd.read_csv(pop_file)

        # agency_code 기준으로 sigungu 이름 병합
        df = pd.merge(df, agency_df, on='agency_code', how='left')

        # 컬럼명 매핑 및 순서 정리
        df = df.rename(columns={
            'year': 'year',
            'month': 'month',
            'male': 'male',
            'female': 'female',
            'total_pop': 'total_population',
            'ratio': 'gender_ratio',
            'pop_per_household': 'household_population'
        })

        # 최종 컬럼 구성
        df = df[['sigungu', 'year', 'month', 'male', 'female', 'total_population', 'gender_ratio', 'household_population']]

        # 필수값 없는 행 제거
        df.dropna(subset=['sigungu', 'year', 'month', 'male', 'female', 'total_population'], inplace=True)

        # DB 삽입
        df.to_sql('PopulationStats', con=engine, if_exists='append', index=False)
        print(f"✅ {len(df)}건 삽입 완료")

    print("🎉 전체 PopulationStats 데이터 삽입 완료")