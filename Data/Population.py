#2022~2024년 월간 CSV 한 번에 전처리하는 스크립트
#디렉토리·파일 경로부터 인코딩, 숫자 포맷, Wide→Long 변환 로직, 컬럼 검증, CSV 저장
"""
    전처리 함수: 월간 인구·세대 현황 CSV를 Long 형태로 변환하여 저장
    
    주요 포인트:
    - raw string (r"...") 사용으로 Windows 경로의 \ 이스케이프 문제 방지
    - encoding='utf-8-sig' / 'euc-kr' 로 다양한 한글 인코딩 대응
    - thousands=',' 옵션으로 "13,571,450" 형태 숫자의 쉼표 제거 후 int 파싱
    - Wide → Long 변환: 한 행에 1월~12월이 모두 나열된 형태를 for m in range(1,13)로 순회
    - 명시적 columns 지정으로 컬럼 순서 고정
    - index=False 로 pandas 기본 인덱스 컬럼 제외
"""
import pandas as pd
import os

def preprocess_population_monthly(input_path: str, output_path: str, year: int):

    # 1) CSV 읽기 시도: UTF-8 BOM 포함
    try:
        df = pd.read_csv(input_path, encoding='utf-8-sig', thousands=',')
    except UnicodeDecodeError:
        # UTF-8 읽기 실패 시 EUC-KR (CP949)로 재시도
        df = pd.read_csv(input_path, encoding='euc-kr', thousands=',')

    # 2) 컬럼 유효성 검사 (1월~12월 '총인구수' 확인)
    expected = [f"{m}월총인구수" for m in range(1, 13)]
    missing = [col for col in expected if col not in df.columns]
    if missing:
        raise KeyError(f"누락된 컬럼: {missing}")

    # 3) Wide → Long 변환
    records = []
    for _, row in df.iterrows():
        agency_code = int(row['행정기관코드'])       # 행정기관코드: 정수로 변환
        agency_name = row['행정기관'].strip()        # 행정기관명: 공백 제거
        for m in range(1, 13):
            records.append({
                "agency_code":       agency_code,                      # 행정기관코드
                "year":              year,                             # 연도 고정
                "month":             m,                                # 월
                "agency_name":       agency_name,                     # 행정기관명
                "total_pop":         int(row[f"{m}월총인구수"]),       # 총인구수
                "household":         int(row[f"{m}월세대수"]),         # 세대수
                "pop_per_household": float(row[f"{m}월세대당인구"]),    # 세대당 인구수
                "male":              int(row[f"{m}월남자인구수"]),     # 남자인구수
                "female":            int(row[f"{m}월여자인구수"]),     # 여자인구수
                "ratio":             float(row[f"{m}월남여비율"]),     # 남여비율
            })

    # 4) DataFrame 생성 및 저장
    preprocessed = pd.DataFrame(
        records,
        columns=[
            "agency_code", "year", "month", "agency_name",
            "total_pop", "household", "pop_per_household",
            "male", "female", "ratio"
        ]
    )
    preprocessed.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✅ {year}년 전처리 완료: {output_path}")


if __name__ == "__main__":
    # 기본 데이터 디렉터리
    base_dir = r"C:\ajouDatabaseProject\Data"
    os.makedirs(base_dir, exist_ok=True)

    # 처리할 연도별 파일 매핑
    files = {
        2022: "202201_202212_주민등록인구및세대현황_월간.csv",
        2023: "202301_202312_주민등록인구및세대현황_월간.csv",
        2024: "202401_202412_주민등록인구및세대현황_월간.csv",
    }

    # 연도별 반복 전처리
    for year, filename in files.items():
        input_path  = os.path.join(base_dir, filename)
        output_file = f"Population_{year}_preprocessed.csv"
        output_path = os.path.join(base_dir, output_file)
        preprocess_population_monthly(input_path, output_path, year)
