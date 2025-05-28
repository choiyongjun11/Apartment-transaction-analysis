import pandas as pd
import os

# ----------------------------------------
# 7종 SOC 파일용 전처리 스크립트
# 공통 → FacilityBase.csv
# 상세 → 7개 Detail CSV
# ----------------------------------------

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """BOM/공백 제거"""
    df.columns = (
        df.columns
        .str.replace('\ufeff', '', regex=False)
        .str.strip()
    )
    return df

# 1) 입력 파일별 설정
soc_files = {
    "CityPark": {
        "file": "도시공원.csv",
        "detail_cols": {
            "시설물 아이디": "facility_id",
            "공원구분": "park_type",
            "공원면적": "park_area",
            "관리기관명": "management_name",
            "전화번호": "phone"
        }
    },
    "ParcelBox": {
        "file": "무인택배함.csv",
        "detail_cols": {
            "시설물 아이디": "facility_id",
            "평일운영시작시각": "weekday_start",
            "평일운영종료시각": "weekday_end",
            "토요일운영시작시각": "saturday_start",
            "토요일운영종료시각": "saturday_end",
            "공휴일운영시작시각": "holiday_start",
            "공휴일운영종료시각": "holiday_end",
            "무료이용시간": "free_time",
            "고객센터전화번호": "phone"
        }
    },
    "FireStation": {
        "file": "소방서.csv",
        "detail_cols": {
            "시설물 아이디": "facility_id",
            "대표전화번호": "phone",
            "관할구역명": "jurisdiction"
        }
    },
    "BicycleParking": {
        "file": "자전거보관소.csv",
        "detail_cols": {
            "시설물 아이디": "facility_id",
            "보관대수": "parking_num",
            "관리기관명": "management_name",
            "관리기관 전화번호": "phone"
        }
    },
    "EvCharger": {
        "file": "전기충전소.csv",
        "detail_cols": {
            "시설물 아이디": "facility_id",
            "관리업체명": "company_name"
        }
    },
    "ParkingFacility": {
        "file": "주차장.csv",
        "detail_cols": {
            "시설물 아이디": "facility_id",
            "주차장구분": "parking_type",
            "주차장유형": "parking_kind",
            "주차구획수": "section_cnt",
            "주차기본시간": "basic_time_min",
            "주차기본요금": "basic_fee",
            "운영요일": "operation_days",
            "요금정보": "fee_info",
            "관리기관명": "management_org",
            "전화번호": "phone"
        }
    },
    "PoliceBox": {
        "file": "파출소.csv",
        "detail_cols": {
            "시설물 아이디": "facility_id",
            "지방청명": "region",
            "경찰서명": "station_name",
            "구분명": "type_name"
        }
    }
}

# 공통 컬럼
base_cols = {
    "시설물 아이디": "facility_id",
    "시도명": "sido",
    "시군구명": "sigungu",
    "법정읍면동명": "dong",
    "SOC유형": "soc_type",
    "구분명": "facility_type",
    "시설명": "facility_name",
    "도로명주소": "road_addr",
    "지번주소": "bunji_addr",
    "위도": "lat",
    "경도": "lng"
}

# 작업 디렉토리
base_dir = r"C:\ajouDatabaseProject\Data"
os.makedirs(base_dir, exist_ok=True)

# 저장용 리스트
base_records = []
detail_records = {name: [] for name in soc_files}

# 2) 파일별 반복
for name, cfg in soc_files.items():
    df = pd.read_csv(os.path.join(base_dir, cfg['file']), encoding='utf-8-sig')
    df = clean_columns(df)

    # 공통 정보
    tmp_base = df[list(base_cols.keys())].rename(columns=base_cols)
    base_records.append(tmp_base)

    # 상세 정보
    det = df[list(cfg['detail_cols'].keys())].rename(columns=cfg['detail_cols'])
    detail_records[name].append(det)

# 3) FacilityBase 저장
all_base = pd.concat(base_records, ignore_index=True).drop_duplicates(subset=['facility_id'])
all_base.to_csv(os.path.join(base_dir, 'FacilityBase.csv'), index=False, encoding='utf-8-sig')
print(f"✅ FacilityBase.csv ({len(all_base)}건) 저장 완료")

# 4) 7개 상세 저장
for name, dfs in detail_records.items():
    combined = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=[list(soc_files[name]['detail_cols'].values())[0]])
    combined.to_csv(os.path.join(base_dir, f"{name}Detail.csv"), index=False, encoding='utf-8-sig')
    print(f"✅ {name}Detail.csv ({len(combined)}건) 저장 완료")
