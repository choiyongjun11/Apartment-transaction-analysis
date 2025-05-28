import pandas as pd
import os

def preprocess_bus_usage(input_csv: str, output_csv: str):
    # 1) CSV 읽기
    df = pd.read_csv(input_csv, encoding='utf-8-sig')

    # 2) 컬럼명 변환
    df = df.rename(columns={
        '기점': 'dep',
        '종점': 'arr',
        '연도': 'year',
        '월': 'month',
        '일시': 'day_type',
        '수단': 'transport_type',
        '이용객수': 'usage_cnt'
    })

    # 3) 결측치/데이터 타입 처리
    df = df.fillna('')
    df['year'] = df['year'].astype(int)
    df['month'] = df['month'].astype(int)
    df['usage_cnt'] = pd.to_numeric(df['usage_cnt'], errors='coerce').fillna(0).astype(int)

    # 4) 저장 (MySQL에서 \N을 NULL로 인식하도록 설정)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig', na_rep='\\N')
    print(f"✅ {os.path.basename(output_csv)} 전처리 완료, {len(df)}건")


if __name__ == "__main__":
    base = r"C:\ajouDatabaseProject\Data"

    # 연도별 CSV 파일 매핑
    files = {
        2022: "정류소간 버스 이용객수_2022.csv",
        2023: "정류소간 버스 이용객수_2023.csv"
    }

    # 연도별 처리
    for year, filename in files.items():
        input_csv  = os.path.join(base, filename)
        output_csv = os.path.join(base, f"BusUsage_{year}_preprocessed.csv")
        preprocess_bus_usage(input_csv, output_csv)
