#2022~2024년 연간 인구증감 데이터를 PopulationChangeYearly 테이블 형식으로 Wide → Long 변환하는 과정
"""
    연간 인구 변화 CSV를 읽어 PopulationChangeYearly 테이블에 맞는
    Long 포맷으로 전처리한 뒤 CSV로 저장.
    
경로 설정
Windows 경로에서 백슬래시(\) 이스케이프 문제 방지를 위해 raw string(r"…"） 사용

CSV 읽기

encoding='utf-8-sig' (BOM 포함 UTF-8) 로 시도, 실패 시 euc-kr 로 재시도

thousands=',' 옵션으로 "6,827,298"처럼 쉼표가 포함된 숫자를 정수로 바로 파싱

컬럼 검증

2022, 2023, 2024년 각각에 대응하는 9개 열(전년남자인구수, 전년여자인구수, 전년인구수합계, 남자인구수, 여자인구수, 인구수합계, 인구증감남자인구수, 인구증감여자인구수, 인구증감합계)이 모두 존재하는지 확인

Wide → Long 변환

한 행(행정기관코드×1)의 데이터를 연도별로 분리

for year in [2022,2023,2024] 루프를 통해 3개의 레코드 생성

DataFrame 생성 및 저장

컬럼 순서를 명시적으로 고정하여 가독성 유지

to_csv(..., encoding='utf-8-sig', index=False)로 한글 깨짐 없이 저장

"""

import pandas as pd
import os

def preprocess_population_yearly(input_path: str, output_path: str):
 
    # --- CSV 읽기 (인코딩, 천 단위 쉼표 처리) ---
    try:
        df = pd.read_csv(input_path, encoding='utf-8-sig', thousands=',')
    except UnicodeDecodeError:
        df = pd.read_csv(input_path, encoding='euc-kr', thousands=',')

    # --- 컬럼 유효성 검사 (2022~2024년 기준) ---
    years = [2022, 2023, 2024]
    required_cols = []
    for y in years:
        required_cols += [
            f"{y}년전년남자인구수", f"{y}년전년여자인구수", f"{y}년전년인구수합계",
            f"{y}년남자인구수",   f"{y}년여자인구수",   f"{y}년인구수합계",
            f"{y}년인구증감남자인구수", f"{y}년인구증감여자인구수", f"{y}년인구증감합계"
        ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(f"누락된 컬럼이 있습니다: {missing}")

    # --- Wide → Long 변환: 연도별 레코드 생성 ---
    records = []
    for _, row in df.iterrows():
        agency_code = int(row['행정기관코드'])        # 정수형 변환
        agency_name = row['행정기관'].strip()         # 공백 제거
        for y in years:
            records.append({
                "agency_code":   agency_code,
                "agency_name":   agency_name,
                "year":          y,
                "prev_male":     int(row[f"{y}년전년남자인구수"]),
                "prev_female":   int(row[f"{y}년전년여자인구수"]),
                "prev_total":    int(row[f"{y}년전년인구수합계"]),
                "curr_male":     int(row[f"{y}년남자인구수"]),
                "curr_female":   int(row[f"{y}년여자인구수"]),
                "curr_total":    int(row[f"{y}년인구수합계"]),
                "male_delta":    int(row[f"{y}년인구증감남자인구수"]),
                "female_delta":  int(row[f"{y}년인구증감여자인구수"]),
                "total_delta":   int(row[f"{y}년인구증감합계"]),
            })

    # --- DataFrame 생성 & CSV 저장 ---
    preprocessed = pd.DataFrame(records, columns=[
        "agency_code", "agency_name", "year",
        "prev_male", "prev_female", "prev_total",
        "curr_male", "curr_female", "curr_total",
        "male_delta", "female_delta", "total_delta"
    ])
    preprocessed.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✅ 연간 전처리 완료: {output_path}")

if __name__ == "__main__":
    base_dir    = r"C:\ajouDatabaseProject\Data"
    input_file  = os.path.join(base_dir, "202212_202412_주민등록인구기타현황(인구증감)_연간.csv")
    output_file = os.path.join(base_dir, "PopulationChangeYearly_preprocessed.csv")
    os.makedirs(base_dir, exist_ok=True)
    preprocess_population_yearly(input_file, output_file)
