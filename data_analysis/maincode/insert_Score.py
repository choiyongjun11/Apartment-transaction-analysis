import pandas as pd
import re
from sqlalchemy import create_engine

# DB 연결 설정
engine = create_engine(f'mysql+pymysql://root:1234@localhost:3306/Apartment-transaction', echo=False)

# -----------------------------
# STEP 1. 거래 정보 로드 및 sigungu 정제
# -----------------------------
def get_transaction_base(engine):
    query = """
    SELECT id AS AptTransaction_id, year, sigungu, price, area, build_year
    FROM AptTransaction
    WHERE build_year IS NOT NULL
    """
    df = pd.read_sql(query, engine)

    def trim_sigungu(s):
        m = re.match(r'^([가-힣]+[도특별자치시]+ [가-힣]+[시군구])', s)
        return m.group(1) if m else s

    df['sigungu'] = df['sigungu'].apply(trim_sigungu)
    return df

# -----------------------------
# STEP 2. 거래 점수 (sigungu + year 기준 그룹화)
# -----------------------------
def calculate_transaction_score_grouped(txn_df):
    grouped = txn_df.groupby(['sigungu', 'year']).agg({
        'price': 'mean',
        'area': 'mean',
        'build_year': 'mean'
    }).reset_index()

    for col in ['price', 'area', 'build_year']:
        min_val = grouped[col].min()
        max_val = grouped[col].max()
        grouped[col + '_score'] = ((grouped[col] - min_val) / (max_val - min_val)) * 100

    grouped['transaction_score'] = (
        grouped['price_score'] * 0.4 +
        grouped['area_score'] * 0.3 +
        grouped['build_year_score'] * 0.3
    )

    return grouped[['sigungu', 'year', 'transaction_score']]

# -----------------------------
# STEP 3. 인구 점수
# -----------------------------
def get_population_score(engine):
    df = pd.read_sql("SELECT sigungu, year, total_population FROM PopulationStats", engine)
    grouped = df.groupby(['sigungu', 'year'])['total_population'].sum().reset_index()
    return normalize_score(grouped, 'total_population')

# -----------------------------
# STEP 4. 버스 점수
# -----------------------------
def get_bus_score(engine):
    df = pd.read_sql("SELECT sigungu, year, SUM(passengers) as total_passengers FROM BusUsage GROUP BY sigungu, year", engine)
    return normalize_score(df, 'total_passengers')

# -----------------------------
# STEP 5. 인프라 점수
# -----------------------------
def get_infra_score(engine):
    df = pd.read_sql("SELECT sigungu, infra_type FROM Infrastructure", engine)

    def classify(t):
        if '학교' in t: return 'school'
        if '병원' in t: return 'hospital'
        if '공원' in t: return 'park'
        return 'other'

    df['type'] = df['infra_type'].apply(classify)
    infra_count = df.groupby(['sigungu', 'type']).size().unstack(fill_value=0).reset_index()

    infra_count['infra_raw'] = (
        infra_count.get('school', 0) * 0.5 +
        infra_count.get('hospital', 0) * 0.3 +
        infra_count.get('park', 0) * 0.1 +
        infra_count.get('other', 0) * 0.1
    )
    return normalize_score(infra_count, 'infra_raw', group=['sigungu'])

# -----------------------------
# STEP 6. 학교 점수 (infra_type LIKE '%학교%')
# -----------------------------
def get_school_score(engine):
    df = pd.read_sql("SELECT sigungu FROM Infrastructure WHERE infra_type LIKE '%%학교%%'", engine)
    school_count = df.groupby('sigungu').size().reset_index(name='school_count')
    return normalize_score(school_count, 'school_count', group=['sigungu'])

# -----------------------------
# 공통: 정규화 함수
# -----------------------------
def normalize_score(df, col, group=['sigungu', 'year']):
    df = df.copy()
    min_val = df[col].min()
    max_val = df[col].max()
    df[col + '_score'] = ((df[col] - min_val) / (max_val - min_val)) * 100
    return df[[*group, col + '_score']]

# -----------------------------
# STEP 7. 모든 점수 병합 및 Score 계산
# -----------------------------
def join_scores_to_transactions(txn_df, transaction_score_df, pop, bus, infra, school):
    base = txn_df[['AptTransaction_id', 'sigungu', 'year']].drop_duplicates()

    base = base.merge(transaction_score_df, on=['sigungu', 'year'], how='left') \
               .merge(pop, on=['sigungu', 'year'], how='left') \
               .merge(bus, on=['sigungu', 'year'], how='left') \
               .merge(infra, on='sigungu', how='left') \
               .merge(school, on='sigungu', how='left')

    base = base.fillna(0)

    base['residence_score'] = round(
        base['transaction_score'] * 0.2 +
        base['total_population_score'] * 0.2 +
        base['total_passengers_score'] * 0.1 +
        base['infra_raw_score'] * 0.2 +
        base['school_count_score'] * 0.3, 2)

    base['investment_score'] = round(
        base['transaction_score'] * 0.5 +
        base['total_population_score'] * 0.3 +
        base['total_passengers_score'] * 0.05 +
        base['infra_raw_score'] * 0.05 +
        base['school_count_score'] * 0.1, 2)

    return base[['AptTransaction_id', 'year', 'sigungu', 'residence_score', 'investment_score']]

# -----------------------------
# STEP 8. Score 테이블에 삽입 (중복 방지)
# -----------------------------
def insert_scores(engine, df):
    existing_ids = pd.read_sql("SELECT AptTransaction_id FROM Score", engine)
    df = df[~df['AptTransaction_id'].isin(existing_ids['AptTransaction_id'])]
    df.to_sql('Score', engine, if_exists='append', index=False)
    print(f"✅ Score 테이블에 {len(df)}개 삽입 완료.")

# -----------------------------
# MAIN 실행
# -----------------------------
if __name__ == '__main__':
    txn_raw = get_transaction_base(engine)
    txn_score = calculate_transaction_score_grouped(txn_raw)
    pop_score = get_population_score(engine)
    bus_score = get_bus_score(engine)
    infra_score = get_infra_score(engine)
    school_score = get_school_score(engine)

    final_score_df = join_scores_to_transactions(txn_raw, txn_score, pop_score, bus_score, infra_score, school_score)
    insert_scores(engine, final_score_df)

