from database.db import Query

class Handler(Query):
    def create_AptTransaction(self):
        options = [
      			"`id` INT AUTO_INCREMENT PRIMARY KEY",
      			"`year` INT NOT NULL",                
      			"`sigungu` VARCHAR(60) NOT NULL",        
      			"`bunji` VARCHAR(30) NOT NULL",          
      			"`main_num` VARCHAR(10) NOT NULL",       
      			"`sub_num` VARCHAR(10) NOT NULL",        
      			"`apt_name` VARCHAR(100) NOT NULL",      
      			"`area` FLOAT NOT NULL",                 
      			"`contract_ym` INT NOT NULL",         
      			"`contract_day` INT NOT NULL",        
      			"`price` INT NOT NULL",               
      			"`dong` VARCHAR(20) NOT NULL",           
      			"`floor` INT NOT NULL",                               
      			"`build_year` INT",                   
      			"`road_name` VARCHAR(100)",                               
      			"`deal_type` VARCHAR(20)",              
      			"`realtor_area` VARCHAR(100)",          
      			"`registry_date` DATE",                  
      			"`apt_type` VARCHAR(20)"
        ]
        self.create(table='AptTransaction', options=options)
        
    def create_Infrastructure(self):
        options = [
      			"`id` INT AUTO_INCREMENT PRIMARY KEY",             
      			"`sigungu` VARCHAR(60) NOT NULL",                   
      			"`dong` VARCHAR(20) NOT NULL",           
      			"`infra_type` VARCHAR(50) NOT NULL",                               
      			"`facility_name` VARCHAR(100) NOT NULL",                   
      			"`latitude` DECIMAL(10,7)",                               
      			"`longitude` DECIMAL(10,7)"              
        ]
        self.create(table='Infrastructure', options=options)
        
    def create_BusUsage(self):
        options = [
      			"`id` INT AUTO_INCREMENT PRIMARY KEY",             
      			"`sigungu` VARCHAR(60) NOT NULL",                   
                "`year` INT NOT NULL",  
                "`month` TINYINT NOT NULL",           
      			"`day_type` VARCHAR(20) NOT NULL",                               
      			"`transport_mode` VARCHAR(20) NOT NULL",                   
      			"`passengers` INT NOT NULL"                                        
        ]
        self.create(table='BusUsage', options=options)		
        
    def create_PopulationStats(self):
        options = [
      			"`id` INT AUTO_INCREMENT PRIMARY KEY",             
      			"`sigungu` VARCHAR(60) NOT NULL",                   
                "`year` INT NOT NULL",  
                "`month` TINYINT NOT NULL",    
                "`male` VARCHAR(10) NOT NULL", 
                "`female` VARCHAR(10) NOT NULL", 
                "`total_population` INT NOT NULL", 
                "`gender_ratio` DECIMAL(5,2)", 
                "`household_population` DECIMAL(5,2)"                                     
        ]
        self.create(table='PopulationStats', options=options)	
        
    def create_Score(self):
        options = [
      			"`id` INT AUTO_INCREMENT PRIMARY KEY",
      			"`AptTransaction_id` INT NOT NULL",
      			"`year` INT NOT NULL",                
      			"`sigungu` VARCHAR(60) NOT NULL",     
      			"`residence_score` FLOAT NOT NULL",
      			"`investment_score` FLOAT NOT NULL"                                      
        ]
        self.create(table='Score', options=options)	

