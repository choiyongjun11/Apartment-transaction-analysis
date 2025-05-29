from date_analysis.example_mysql.handler import Handler


if __name__ == '__main__':
  #DB Table 추가
  Handler().create_AptTransaction_2024_preprocessed()

  #handler에 설정한 컬럼 select
  apt_name = Handler().get_apt_name('현대힐스테이트')

  #handler에 설정한 컬럼 insert
  Handler().insert_value('경기도 수원시 장안구 정자동',870-1,0870,0001,'백설마을현대코오롱아파트',59.7900,202412,31,"38,000",598,4,'개인','개인',1999,'만석로68번길 10',-,'중개거래','경기 수원시 장안구',25.03.20,'아파트')

  #handler에 설정한 컬럼 delete
  Handler().delete_year(2024)
