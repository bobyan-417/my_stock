'''
Created on Aug 31, 2016

@author: yanb
'''
from my_test.index.Index import Index
from my_test.utils.Assist import UnitHelper
from my_test.conf.Configuration import G_CONST_DATE_FIELD_NAME,\
     G_CONST_DAILY_RATIO_FIELD_NAME
import datetime

class IndexIncrease(Index):
    
    def __init__(self, collection_name, increase_rate = "0.09", monitor_period = "30d", duration = None):
        super(IndexIncrease, self).__init__(collection_name, monitor_period, duration)
        self.increase_rate = increase_rate        
        self.index_prefix = "INDEX_INCREASE_"   
    def get_index_data(self, raw_data):
        return raw_data    
           
    def get_index_db_name(self):
        db_instance_name = "STOCK_INDEX_INCREASE"    
        return db_instance_name

    def get_previous_max_days(self):      
        max_day = 1
        return max_day     
    def is_good_by(self):  
        rawData = self.get_refined_data()
        if(len(rawData) == 0):
            return False
        lastDate = rawData[0][G_CONST_DATE_FIELD_NAME]
        firstDate =  self.get_monitor_first_date(lastDate)
        is_good = False
        for dateInfo in rawData:
            curDate = dateInfo[G_CONST_DATE_FIELD_NAME]
            if(curDate < firstDate):
                break
            dailRate = dateInfo[G_CONST_DAILY_RATIO_FIELD_NAME] 
            if(dailRate >  self.increase_rate):
                is_good = True
                break
        return is_good
            
        pass
if __name__ == '__main__':
    pass