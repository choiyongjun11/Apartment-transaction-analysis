import pandas as pd
import os
import re
import numpy as np

def preprocess_apt_csv(input_csv: str, output_csv: str, source_id: int):
    # 1) CSV 읽기 (utf-8-sig)
    df = pd.read_csv(input_csv, encoding='utf-8-sig', dtype=str)

    # 2) 컬럼명 매핑 (CSV → DB 컬럼)
    column_map = {
        
        '시군구': 'sigungu',
        '번지': 'bunji',
        '본번': 'main_num',
        '부번': 'sub_num',
        '단지명': 'apt_name',
        '전용면적(㎡)': 'area',
        '계약년월': 'contract_ym',
        '계약일': 'contract_day',
        '거래금액(만원)': 'price',
        '동': 'dong',
        '층': 'floor',
        '매수자': 'buyer',
        '매도자': 'seller',
        '건축년도': 'build_year',
        '도로명': 'road_name',
        '해제사유발생일': 'cancel_date',
        '거래유형': 'deal_type',
        '중개사소재지': 'realtor_area',
        '등기일자': 'registry_date',
        '주택유형': 'apt_type'
    }
    df = df.rename(columns=column_map)

    # 3) 불필요 컬럼 제거
    drop_cols = [col for col in df.columns if col not in column_map.values()]
    df = df.drop(columns=drop_cols)

    # 4) '-' 또는 빈문자열을 NaN 처리
    df.replace(['-', ''], np.nan, inplace=True)

    # 5) price: 쉼표 제거 후 정수형
    df['price'] = df['price'].str.replace(',', '', regex=False)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    # 6) 기타 숫자형 컬럼 처리
    for col in ['area', 'contract_ym', 'contract_day', 'floor', 'build_year']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 7) 날짜 파싱 함수
    def parse_date(val):
        if pd.isna(val):
            return pd.NaT
        s = str(val).strip()
        if re.fullmatch(r'\d{8}', s):
            return pd.to_datetime(s, format='%Y%m%d', errors='coerce')
        return pd.to_datetime(s, format='%y.%m.%d', errors='coerce')

    df['cancel_date'] = df['cancel_date'].apply(parse_date).dt.date
    df['registry_date'] = df['registry_date'].apply(parse_date).dt.date

    # 8) dong 길이 제한
    df['dong'] = df['dong'].astype(str).str.slice(0, 20)

    # 9) source_id 추가
    df['source_id'] = source_id

    # 10) 컬럼 순서 재정렬 (DB 스키마 순서)
    final_cols = [
        'sigungu', 'bunji', 'main_num', 'sub_num', 'apt_name', 'area',
        'contract_ym', 'contract_day', 'price', 'dong', 'floor',
        'buyer', 'seller', 'build_year', 'road_name', 'cancel_date',
        'deal_type', 'realtor_area', 'registry_date', 'apt_type', 'source_id'
    ]
    df = df[final_cols]

    # 11) CSV 저장 (NULL은 \N)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig', na_rep='\\N')
    print(f"✅ {os.path.basename(output_csv)} 전처리 완료 ({len(df)}건)")

# 사용 예시
if __name__ == "__main__":
    base_path = r"C:\ajouDatabaseProject\Data"
    data_sources = [
        (2022, "아파트(매매)_실거래가_202201_202212.csv", 1),
        (2023, "아파트(매매)_실거래가_202301_202312.csv", 2),
        (2024, "아파트(매매)_실거래가_202401_202412.csv", 3),
    ]

    for year, fname, src_id in data_sources:
        inp = os.path.join(base_path, fname)
        out = os.path.join(base_path, f"AptTransaction_{year}_preprocessed.csv")
        preprocess_apt_csv(inp, out, src_id)
