import pandas as pd
import os
import re
import numpy as np

def preprocess_apt_csv(input_csv: str, output_csv: str, source_id: int):
    # 1) CSV 읽기 (utf-8-sig), 모든 컬럼을 문자열로 읽어둠
    df = pd.read_csv(input_csv, encoding='utf-8-sig', dtype=str)

    # 2) 컬럼명 매핑 (buyer, seller, cancel_date 제외)
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
        '건축년도': 'build_year',
        '도로명': 'road_name',
        '거래유형': 'deal_type',
        '중개사소재지': 'realtor_area',
        '등기일자': 'registry_date',
        '주택유형': 'apt_type'
    }
    df = df.rename(columns=column_map)

    # 3) buyer, seller, cancel_date 컬럼은 아예 무시하고(컬럼명이 없으므로 자동 제거됨)
    df = df[list(column_map.values())].copy()

    # 4) '-' 또는 빈문자열을 NaN 처리
    df.replace(['-', ''], np.nan, inplace=True)

    # 5) price: 쉼표 제거 후 숫자형으로 변환
    df['price'] = df['price'].str.replace(',', '', regex=False)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    # 6) 기타 숫자형 컬럼 처리 (area, contract_ym, contract_day, floor, build_year)
    for col in ['area', 'contract_ym', 'contract_day', 'floor', 'build_year']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 7) 날짜 파싱: registry_date 컬럼만 남김
    def parse_date(val):
        if pd.isna(val):
            return np.nan
        s = str(val).strip()
        if re.fullmatch(r'\d{8}', s):
            return pd.to_datetime(s, format='%Y%m%d', errors='coerce').date()
        # 혹시 'YY.MM.DD' 형태가 섞여있다면
        return pd.to_datetime(s, format='%y.%m.%d', errors='coerce').date()

    df['registry_date'] = df['registry_date'].apply(parse_date)

    # 8) dong 칼럼이 NaN이거나 길면 먼저 NaN, 문자열일 때 최대 20자 자르기
    df['dong'] = df['dong'].astype(str).str.strip()
    df.loc[df['dong'].isin(['nan', 'None']), 'dong'] = np.nan
    df['dong'] = df['dong'].str.slice(0, 20)

    # 9) source_id 추가
    df['source_id'] = source_id

    # 10) 컬럼 순서 재정렬 (buyer, seller, cancel_date 없음)
    final_cols = [
        'sigungu', 'bunji', 'main_num', 'sub_num', 'apt_name', 'area',
        'contract_ym', 'contract_day', 'price', 'dong', 'floor',
        'build_year', 'road_name', 'deal_type', 'realtor_area',
        'registry_date', 'apt_type', 'source_id'
    ]
    df = df[final_cols]

    # 11) 모든 NaN을 '\N'으로 바꾸기 + 문자열 "nan"도 '\N' 처리
    #     1) df.fillna(np.nan) 상태에서, 숫자/날짜 변환 실패로 생긴 NaN은 df.astype(str) 시 "nan"이 됩니다.
    #     2) df = df.astype(str).replace({'nan':'\\N', 'NaT':'\\N'}) 통해 모두 '\N'으로 변환
    df = df.astype(str).replace({'nan': '\\N', 'NaT': '\\N'})

    # 12) CSV 저장 (이제 모든 필드에서 결측은 '\N')
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
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
