from db import Query

class Handler(Query):
    def create_AptTransaction_2024_preprocessed(self):
        options = [
      			"`id BIGINT PRIMARY KEY AUTO_INCREMENT",
      			"`year` BIGINT NOT NULL",                
      			"`sigungu` VARCHAR(60) NOT NULL",        
      			"`bunji` VARCHAR(30) NOT NULL",          
      			"`main_num` VARCHAR(10) NOT NULL",       
      			"`sub_num` VARCHAR(10) NOT NULL",        
      			"`apt_name` VARCHAR(100) NOT NULL",      
      			"`area` FLOAT NOT NULL",                 
      			"`contract_ym` BIGINT NOT NULL",         
      			"`contract_day` BIGINT NOT NULL",        
      			"`price` BIGINT NOT NULL",               
      			"`dong` VARCHAR(20) NOT NULL",           
      			"`floor` BIGINT NOT NULL",               
      			"`buyer` VARCHAR(30)",                   
      			"`seller` VARCHAR(30)",                  
      			"`build_year` BIGINT",                   
      			"`road_name` VARCHAR(100)",              
      			"`cancel_date` DATE",                   
      			"`deal_type` VARCHAR(20)",              
      			"`realtor_area` VARCHAR(100)",          
      			"`registry_date` DATE",                  
      			"`apt_type` VARCHAR(20)",               
      			"`source_id` BIGINT NOT NULL",
      			"FOREIGN KEY (source_id) REFERENCES DataSource(id)"
        ]
        self.create(table='AptTransaction_2024_preprocessed', options=options)

    def get_apt_name(self, apt: str):
        rows = self.select(table='AptTransaction_2024_preprocessed', columns=['apt_name'], where=[f"apt_name={apt}"])
        return [row[0] for row in rows]
      
    def insert_value(self, year: int, sigungu: str, bunji: str, main_num: str, sub_num: str, apt_name: str, area: float, contract_ym: int, contract_day: int, price: int, dong: str, floor: int,buyer: str, seller: str, build_year: int, road_name: str, cancel_date: date, deal_type: str, realtor_area: str,registry_date: date,apt_type: str,source_id: int):
        self.insert(table='AptTransaction_2024_preprocessed', columns=['year', 'sigungu, bunji', 'main_num', 'sub_num', 'apt_name', 'area', 'contract_ym', 'contract_day', 'price', 'dong', 'floor','buyer', 'seller', 'build_year', 'road_name', 'cancel_date', 'deal_type', 'realtor_area','registry_date','apt_type','source_id'],
                        values=[f"'{year}'",f"'{sigungu}'",f"'{bunji}'",f"'{main_num}'",f"'{sub_num}'",f"'{apt_name}'",f"'{area: float}'",f"'{contract_ym}'",f"'{contract_day}'",f"'{price}'",f"'{dong}'",f"'{floor}'",f"'{buyer}'",f"'{seller}'",f"'{build_year}'",f"'{road_name}'",f"'{cancel_date}'",f"'{deal_type}'",f"'{realtor_area}'",f"'{registry_date}'",f"'{apt_type}'",f"'{source_id}'"])
						
    def delete_scenario(self, year: int):
        self.delete(table='AptTransaction_2024_preprocessed', where=[f"year={year}"])
