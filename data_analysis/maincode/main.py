import os
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from sqlalchemy import create_engine
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable

# DB 연결
engine = create_engine(f'mysql+pymysql://root:1234@localhost:3306/Apartment-transaction', echo=False)

def geocode_address(address):
    geolocator = Nominatim(user_agent="apt_map", timeout=5)
    try:
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
    except Exception:
        return None, None
    return None, None

def get_avg_bus_passengers(city, year, engine):
    query = """
        SELECT year, month, SUM(passengers) as total_passengers
        FROM BusUsage
        WHERE sigungu LIKE %s AND year = %s
        GROUP BY year, month
    """
    like_city = f"%{city}%"
    year = int(year)  # np.int64 → int
    df = pd.read_sql(query, engine, params=[(like_city, year)])
    if df.empty:
        return None
    df['total_passengers'] = df['total_passengers'].astype(float)
    avg = df['total_passengers'].mean()
    return round(avg, 2)

def get_top10_by_city(city, purpose):
    score_column = "residence_score" if purpose == "1" else "investment_score"

    # Score 테이블의 year 값도 함께 가져옴
    query = f"""
        SELECT s.sigungu, a.apt_name, a.area, a.price, a.build_year,
               a.road_name, s.{score_column}, s.year
        FROM Score s
        JOIN AptTransaction a ON s.AptTransaction_id = a.id
        WHERE s.sigungu = '{city}'
        ORDER BY s.{score_column} DESC
        LIMIT 10
    """
    df = pd.read_sql(query, engine)

    df['full_address'] = df['sigungu'] + ' ' + df['road_name']
    df[['latitude', 'longitude']] = df['full_address'].apply(lambda x: pd.Series(geocode_address(x)))

    # Score 테이블에서 추출한 year의 첫 값을 사용 (Top10이 동일한 year 기준이라 가정)
    year = df['year'].iloc[0] if 'year' in df.columns and not df.empty else 2024

    # 시 이름만 추출 (예: "경기도 수원시" → "수원시")
    short_city = city.split()[1]
    avg_passengers = get_avg_bus_passengers(short_city, year, engine)
    df['passengers'] = avg_passengers

    return df

def get_infrastructure_by_city(city):
    query = f"""
        SELECT facility_name, infra_type, latitude, longitude
        FROM Infrastructure
        WHERE sigungu = '{city}' AND latitude IS NOT NULL AND longitude IS NOT NULL
    """
    return pd.read_sql(query, engine)

def create_interactive_map(df, infra_df, city):
    center_lat, center_lng = df['latitude'].mean(), df['longitude'].mean()
    fmap = folium.Map(location=[center_lat, center_lng], zoom_start=13)

    apt_cluster = MarkerCluster().add_to(fmap)
    for _, row in df.iterrows():
        tooltip = f"{row['apt_name']} ({row['area']}\u33a1) - {row['price']}\u10d3"
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=tooltip,
            tooltip=tooltip,
            icon=folium.Icon(color='blue', icon='home')
        ).add_to(apt_cluster)

    infra_cluster = MarkerCluster().add_to(fmap)
    for _, row in infra_df.iterrows():
        tooltip = f"{row['infra_type']} - {row['facility_name']}"
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=tooltip,
            tooltip=tooltip,
            icon=folium.Icon(color='green', icon='info-sign')
        ).add_to(infra_cluster)

    map_path = rf"C:\Users\김성진\OneDrive\Desktop\Apartment-transaction-analysis\top10_map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    fmap.save(map_path)
    return map_path

def create_top10_table_html(df):
    score_label = '점수'
    score_col = [col for col in df.columns if col.endswith('_score')][0]
    display_df = df[['sigungu', 'apt_name', 'area', 'price', 'build_year', 'passengers', score_col]]
    display_df = display_df.rename(columns={
        'sigungu': '시군구', 'apt_name': '단지명', 'area': '전용면적',
        'price': '거래금액', 'build_year': '건축년도', 'passengers': '버스이용객수',
        score_col: score_label
    })
    html_path = f"C:/Users/\김성진/OneDrive/Desktop/Apartment-transaction-analysis/top10_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    display_df.to_html(html_path, index=False)
    return html_path

def main():
    purpose = input("선택하세요:\n1. 실거주 목적\n2. 투자 목적\n입력: ")
    choice = input("추천 방식을 선택하세요:\n1. 특정 시의 Top10\n2. 경기도 전체 추천\n입력: ")
    city = input("시 이름을 입력하세요 (예: 경기도 수원시): ") if choice == '1' else "경기도 수원시"

    print(f"'{city}' 지역의 Top10 추천을 분석 중입니다...")

    df_top10 = get_top10_by_city(city, purpose)
    infra_df = get_infrastructure_by_city(city)

    map_file = create_interactive_map(df_top10, infra_df, city)
    table_file = create_top10_table_html(df_top10)

    print(f"\n\U0001f4cd 지도 시각화 파일 경로: {map_file}")
    print(f"\U0001f4ca 표 시각화 파일 경로: {table_file}")

if __name__ == "__main__":
    main()
