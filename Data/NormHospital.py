import pandas as pd

# (1) CSV 읽기: 실제 경로를 본인 환경에 맞게 수정하세요.
file_path = r'C:\ajouDatabaseProject\Data\경기도병원현황.csv'
df = pd.read_csv(file_path, encoding='cp949')

# (2) 필요한 컬럼만 선택 및 영어 컬럼명으로 재명명
rename_map = {
    '시군명': 'si_gun',
    '사업장명': 'business_name',
    '소재지도로명주소': 'road_address',
    '소재지지번주소': 'jibun_address',
    '소재지우편번호': 'postal_code',
    '위도': 'lat',
    '경도': 'lng',
    '영업상태명': 'status',
    '소재지시설전화번호': 'phone',
    '업태구분명정보': 'business_type',
    '의료기관종별명': 'hospital_type',
    '진료과목내용정보': 'medical_departments'
}

# 존재하는 컬럼만 골라내기 위해, 우선 원본에 키가 있는지 체크한 후 선택합니다.
available_keys = [k for k in rename_map.keys() if k in df.columns]
df_selected = df[available_keys].rename(columns={k: rename_map[k] for k in available_keys})

# (3) 'closed_date'를 DATE 형식(YYYY-MM-DD)으로 변환
#     문자열이 YYYYMMDD 형태가 아니면 NaT가 됩니다.
if 'closed_date' in df_selected.columns:
    df_selected['closed_date'] = pd.to_datetime(
        df_selected['closed_date'],
        format='%Y%m%d',
        errors='coerce'
    )

# (4) 콘솔에 전처리된 데이터 상위 5개 행과 요약 정보 출력
print("=== 전처리된 데이터 상위 5개 ===")
print(df_selected.head(5), end="\n\n")

print("=== 컬럼별 데이터 타입 ===")
print(df_selected.dtypes, end="\n\n")

print(f"전체 행/열 크기: {df_selected.shape[0]} rows × {df_selected.shape[1]} columns", end="\n\n")

# (5) 전처리 완료된 결과를 새 CSV 파일로 저장 (UTF-8 BOM 포함)
output_path = r'C:\ajouDatabaseProject\Data\Hospital.csv'
df_selected.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"전처리된 파일이 아래 경로에 저장되었습니다:\n{output_path}")
