import pandas as pd
import os
import re

# ========================================
# NormPopulation.py
#  월간 및 연간 인구 데이터 전처리 스크립트
# ========================================

    # - pandas.read_csv: raw CSV를 DataFrame으로 로드
    #   * thousands=',': 천단위 구분자 콤마 제거 후 숫자 파싱
    #   * encoding: 한글 인코딩 호환
    # - clean_columns: 컬럼명 BOM(\ufeff)·공백 제거
    # - Wide→Long 변환:
    #   * df.iterrows(): 한 행씩 순회
    #   * for m in range(1,13): 월별 컬럼 추출하여 리스트(records)에 append
    #   * 결과: 행×월 개수 만큼 레코드 생성
    # - agency_records(set): 중복 기관명 제거용
    # - to_csv: index=False로 pandas 인덱스 제거, utf-8-sig로 BOM 포함
    # - print: 처리 건수로 데이터 적재 전 검증 가능
    
    
    
# 전역 세트: Agency 테이블용 (code, name) 정보 수집
agency_records = set()

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    # 헤더(컬럼명) 정리: BOM 제거, 앞뒤 공백(strip) 제거
    df.columns = df.columns.str.replace('\ufeff', '', regex=False).str.strip()
    return df

def preprocess_population_monthly(input_path: str, output_path: str, year: int):
    """
    월간 인구 통계 CSV를 Long 포맷으로 변환하고 Agency 정보 수집
    - Wide 형태를 Long 형태로 전개
    - 데이터 건수 출력
    """
    # 1) CSV 로드: UTF-8 BOM 포함 시도, 실패 시 EUC-KR
    try:
        df = pd.read_csv(input_path, encoding='utf-8-sig', thousands=',')
    except UnicodeDecodeError:
        df = pd.read_csv(input_path, encoding='euc-kr', thousands=',')
    # 2) 헤더 정리
    df = clean_columns(df)

    records = []
    # 각 행(Row) 처리: 한 행정기관의 월별 데이터
    for _, row in df.iterrows():
        agency_code = int(row['행정기관코드'])
        agency_name = str(row['행정기관']).strip()
        # Agency 정보 수집 (set으로 중복 제거)
        agency_records.add((agency_code, agency_name))

        # 각 월(1~12) 순회하여 레코드 생성
        for m in range(1, 13):
            records.append({
                "agency_code":       agency_code,
                "year":              year,
                "month":             m,
                "total_pop":         int(row[f"{m}월총인구수"]),
                "household":         int(row[f"{m}월세대수"]),
                "pop_per_household": float(row[f"{m}월세대당인구"]),
                "male":              int(row[f"{m}월남자인구수"]),
                "female":            int(row[f"{m}월여자인구수"]),
                "ratio":             float(row[f"{m}월남여비율"]),
            })

    # DataFrame 생성
    pdf = pd.DataFrame(records, columns=[
        "agency_code", "year", "month", "total_pop",
        "household", "pop_per_household", "male", "female", "ratio"
    ])
    # 처리 건수 로그
    print(f"🔄 월간({year}) 레코드 생성: {len(pdf)}건")
    # CSV 저장: 인덱스 제외, UTF-8 BOM 포함
    pdf.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✅ 월간 전처리 완료: {output_path}\n")

def preprocess_population_yearly(input_path: str, output_path: str, years=None):
    """
    연간 인구 변화 CSV를 Long 포맷으로 변환하고 Agency 정보 수집
    - Wide 형태를 Long 형태로 전개
    - 데이터 건수 출력
    """
    if years is None:
        years = [2022, 2023, 2024]

    try:
        df = pd.read_csv(input_path, encoding='utf-8-sig', thousands=',')
    except UnicodeDecodeError:
        df = pd.read_csv(input_path, encoding='euc-kr', thousands=',')
    df = clean_columns(df)

    records = []
    # 한 행(Row)에는 3년치 데이터가 함께 존재
    for _, row in df.iterrows():
        agency_code = int(row['행정기관코드'])
        agency_name = str(row['행정기관']).strip()
        agency_records.add((agency_code, agency_name))

        for y in years:
            records.append({
                "agency_code":  agency_code,
                "year":         y,
                "prev_male":    int(row[f"{y}년전년남자인구수"]),
                "prev_female":  int(row[f"{y}년전년여자인구수"]),
                "prev_total":   int(row[f"{y}년전년인구수합계"]),
                "curr_male":    int(row[f"{y}년남자인구수"]),
                "curr_female":  int(row[f"{y}년여자인구수"]),
                "curr_total":   int(row[f"{y}년인구수합계"]),
                "male_delta":   int(row[f"{y}년인구증감남자인구수"]),
                "female_delta": int(row[f"{y}년인구증감여자인구수"]),
                "total_delta":  int(row[f"{y}년인구증감합계"])
            })

    ydf = pd.DataFrame(records, columns=[
        "agency_code", "year",
        "prev_male", "prev_female", "prev_total",
        "curr_male", "curr_female", "curr_total",
        "male_delta", "female_delta", "total_delta"
    ])
    print(f"🔄 연간 레코드 생성: {len(ydf)}건")
    ydf.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✅ 연간 전처리 완료: {output_path}\n")

if __name__ == "__main__":
    # 데이터 디렉터리 설정
    base_dir = r"C:\ajouDatabaseProject\Data"
    os.makedirs(base_dir, exist_ok=True)

    # 1) 월간 데이터 전처리
    monthly_files = {
        2022: "202201_202212_주민등록인구및세대현황_월간.csv",
        2023: "202301_202312_주민등록인구및세대현황_월간.csv",
        2024: "202401_202412_주민등록인구및세대현황_월간.csv"
    }
    for year, fname in monthly_files.items():
        in_path = os.path.join(base_dir, fname)
        out_path = os.path.join(base_dir, f"PopulationMonthly_{year}_preprocessed.csv")
        if os.path.exists(in_path):
            preprocess_population_monthly(in_path, out_path, year)
        else:
            print(f"⚠️ 월간 파일 누락: {in_path}")

    # 2) 연간 데이터 전처리
    annual_fname = "202212_202412_주민등록인구기타현황(인구증감)_연간.csv"
    in_annual = os.path.join(base_dir, annual_fname)
    out_annual = os.path.join(base_dir, "PopulationYearly_preprocessed.csv")
    if os.path.exists(in_annual):
        preprocess_population_yearly(in_annual, out_annual)
    else:
        print(f"⚠️ 연간 파일 누락: {in_annual}")

    # 3) Agency 테이블용 CSV 생성
    agency_df = pd.DataFrame(sorted(agency_records), columns=["agency_code", "name"])
    agency_out = os.path.join(base_dir, "Agency_preprocessed.csv")
    agency_df.to_csv(agency_out, index=False, encoding='utf-8-sig')
    print(f"✅ Agency_preprocessed.csv 생성 완료 ({len(agency_df)}건)\n")



