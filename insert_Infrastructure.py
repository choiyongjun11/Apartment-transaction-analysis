import pandas as pd
from sqlalchemy import create_engine
import os
import re


# DB 연결 엔진 생성
engine = create_engine(f'mysql+pymysql://root:1234@localhost:3306/Apartment-transaction', echo=False)

# ====================
# Helper: 주소에서 시군구/동 추출 함수
# ====================

def extract_sigungu(full_address):
    if not isinstance(full_address, str):
        return None  # 문자열이 아닌 경우 (예: NaN, float 등) 예외 방지

    match = re.match(r'(.*?[시군구])', full_address)
    return match.group(1) if match else None

def extract_dong(address):
    if not isinstance(address, str):
        return None  # 문자열이 아닌 경우 (예: NaN, float 등) 예외 방지
    
    # 면/읍/동 추출
    patterns = ['([가-힣]+[면읍동])', '([가-힣]+리)']
    for p in patterns:
        match = re.search(p, address)
        if match:
            return match.group(1)
    return None

# ====================
# 1. 초중고등학교현황
# ====================
def insert_school_data(filepath):
    df = pd.read_csv(filepath, encoding='cp949')

    df['sigungu'] = df['소재지지번주소'].apply(extract_sigungu)
    df['dong'] = df['소재지지번주소'].apply(extract_dong)
    df['infra_type'] = df['학교구분명']
    df['facility_name'] = df['시설명']
    df['latitude'] = df['WGS84위도']
    df['longitude'] = df['WGS84경도']

    df = df[['sigungu', 'dong', 'infra_type', 'facility_name', 'latitude', 'longitude']]
    df.dropna(subset=['sigungu', 'dong'], inplace=True)

    df.to_sql('Infrastructure', con=engine, if_exists='append', index=False)
    print(f"✅ 학교 데이터 삽입 완료 ({len(df)} rows)")

# ====================
# 2. Hospital.csv
# ====================
def insert_hospital_data(filepath):
    df = pd.read_csv(filepath)

    df['sigungu'] = df['si_gun']
    df['dong'] = df['road_address'].apply(extract_dong)
    df['infra_type'] = '병원'
    df['facility_name'] = df['business_name']
    df['latitude'] = df['lat']
    df['longitude'] = df['lng']

    df = df[['sigungu', 'dong', 'infra_type', 'facility_name', 'latitude', 'longitude']]
    df.dropna(subset=['sigungu', 'dong'], inplace=True)

    df.to_sql('Infrastructure', con=engine, if_exists='append', index=False)
    print(f"✅ 병원 데이터 삽입 완료 ({len(df)} rows)")

# ====================
# 3. FacilityBase.csv
# ====================
def insert_facility_base_data(filepath):
    df = pd.read_csv(filepath)

    df['sigungu'] = df['sigungu']
    df['dong'] = df['dong']
    df['infra_type'] = df['facility_type']
    df['facility_name'] = df['facility_type']
    df['latitude'] = df['lat']
    df['longitude'] = df['lng']

    df = df[['sigungu', 'dong', 'infra_type', 'facility_name', 'latitude', 'longitude']]
    df.dropna(subset=['sigungu', 'dong'], inplace=True)

    df.to_sql('Infrastructure', con=engine, if_exists='append', index=False)
    print(f"✅ 기타 시설 데이터 삽입 완료 ({len(df)} rows)")



# ====================
# 실행
# ====================
if __name__ == '__main__':
    insert_school_data('C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/school.csv')
    insert_hospital_data('C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/Hospital.csv')
    insert_facility_base_data('C:/Users/김성진/OneDrive/Desktop/Apartment-transaction-analysis/file_list/FacilityBase.csv')

    print("🎉 모든 Infrastructure 데이터 삽입 완료")
