from database.handler import Handler
import pandas as pd
from sqlalchemy import create_engine
import os
import re



if __name__ == '__main__':
  # 2. SQLAlchemy 엔진 생성
  engine = create_engine(f'mysql+pymysql://root:1234@localhost:3306/Apartment-transaction', echo=False)

  # 3. 삽입할 CSV 파일 목록
  csv_files = [
      'C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/AptTransaction_2022_preprocessed.csv',
      'C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/AptTransaction_2023_preprocessed.csv',
      'C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/AptTransaction_2024_preprocessed.csv'
  ]

  # 4. 컬럼 매핑 (CSV 컬럼 순서와 DB 컬럼 이름이 동일하다고 가정)
  columns = [
      'year', 'sigungu', 'bunji', 'main_num', 'sub_num', 'apt_name', 'area',
      'contract_ym', 'contract_day', 'price', 'dong', 'floor', 'build_year',
      'road_name', 'deal_type', 'realtor_area', 'registry_date', 'apt_type'
  ]


  for csv_file in csv_files:
      print(f"Processing {csv_file}...")

      # 파일명에서 연도 추출
      match = re.search(r'AptTransaction_(\d{4})_', os.path.basename(csv_file))
      year = int(match.group(1))
      
      # CSV 로딩
      df = pd.read_csv(csv_file)

      # year 컬럼 삽입
      df['year'] = year

      # 컬럼 순서 정렬 (필요한 컬럼만)
      df = df[columns]

      # 날짜 형식 처리
      df['registry_date'] = pd.to_datetime(df['registry_date'], errors='coerce')

      # 삽입
      df.to_sql('AptTransaction', con=engine, if_exists='append', index=False)
      print(f"✅ Inserted {len(df)} rows from {os.path.basename(csv_file)}")

  print("🎉 All data inserted successfully.")






