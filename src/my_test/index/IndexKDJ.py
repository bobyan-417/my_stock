'''
Created on Aug 24, 2016

@author: yanb
'''
from my_test.index.Index import Index
from my_test.conf.Configuration import G_CONST_DATE_FIELD_NAME, G_CONST_START_TIME, G_CONST_ID_FIELD_NAME, G_CONST_DB_DETAIL_TABLE,\
    G_CONST_DAILY_RATIO_FIELD_NAME, G_CONST_STUDY_GOOD_VALUE, G_CONST_STUDY_INFO,\
    G_CONST_STUDY_REAL_RATE, G_CONST_STUDY_CORRECT, G_CONST_STUDY_DEAL_DAYS,\
    G_CONST_ADJ_CLOSE_FIELD_NAME, G_CONST_ADJ_RATE_FIELD_NAME
from my_test.conf.Configuration import G_CONST_CALCULATION_K_NAME, G_CONST_CALCULATION_D_NAME, G_CONST_CALCULATION_J_NAME, G_CONST_CALCULATION_RSV_NAME
from my_test.conf.Configuration import G_CONST_HIGH_FIELD_NAME, G_CONST_LOW_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME
from my_test.utils.Assist import DBHelper, UnitHelper
from my_test.index import calclib
from datetime import timedelta
from my_test.index.IndexMACD import IndexMACD

class IndexKDJ(Index):
    '''
    classdocs
    '''


    def __init__(self, collection_name, period = 9,monitor_count = 30, duration = None, study_count = 200, verify_date = None, verify_count = 150, study_good_ratio = 0.05, study_good_count = 10):
        super(IndexKDJ, self).__init__(collection_name, monitor_count, duration, study_count, verify_date, verify_count, study_good_ratio, study_good_count)
        self.period = period
           
    
    def get_index_db_name(self):
        db_instance_name = "STOCK_INDEX_KDJ_" + str(self.period)    
        return db_instance_name
     
    def get_index_data(self, raw_data):
        #DBHelper().print_values(raw_data)
        data = self.calc_KDJ(self.period, raw_data)
        return data  

    def get_previous_max_days(self):        
        return self.period + 1
    def calc_rsv_value(self, calc_data):  
        pHigh = None
        pLow = None
        curDateInfo = calc_data[0]
        rsv = 0
        for dateinfo in calc_data:
            if(pHigh == None or pHigh < dateinfo[G_CONST_HIGH_FIELD_NAME]):
                pHigh = dateinfo[G_CONST_HIGH_FIELD_NAME]
            if(pLow == None or pLow > dateinfo[G_CONST_LOW_FIELD_NAME]):
                pLow = dateinfo[G_CONST_LOW_FIELD_NAME] 
        if(pLow != pHigh):
            rsv = (float(curDateInfo[G_CONST_ClOSE_FIELD_NAME]) - pLow)/(pHigh - pLow) * 100 
        return rsv           
    def calc_RSV(self, period, data):
        data_with_rsv = []
        #p_refs = []
        index = period
        p_high = None
        p_low = None
        total_count = len(data)
        if(total_count < period):
            return data_with_rsv
        data.reverse() 
        while(index < total_count):
            data_info = data[index]
            new_data_info = {G_CONST_DATE_FIELD_NAME : data_info[G_CONST_DATE_FIELD_NAME], 
                             G_CONST_ID_FIELD_NAME : data_info[G_CONST_ID_FIELD_NAME]}
            p_index = 0
            p_high = None
            p_low = None
            rsv = 0
            while(p_index < period):
                p_ref = data[index - p_index]
                if(p_high == None or p_high < p_ref[G_CONST_HIGH_FIELD_NAME]):
                    p_high = p_ref[G_CONST_HIGH_FIELD_NAME]
                if(p_low == None or p_low > p_ref[G_CONST_LOW_FIELD_NAME]):
                    p_low = p_ref[G_CONST_LOW_FIELD_NAME]   
                p_index += 1
            if(p_high != p_low):
                rsv = (float(data_info[G_CONST_ClOSE_FIELD_NAME]) - p_low)/(p_high - p_low) * 100
            new_data_info[G_CONST_CALCULATION_RSV_NAME] = rsv 
            data_with_rsv.append(new_data_info)
            index += 1                       
        data.reverse()         
        data_with_rsv.reverse()
        return data_with_rsv
    
    def calc_KDJ1(self, period, data):
        
        data_with_kdj = self.calc_RSV(period, data)
        #db_helper = DBHelper()
        #db_helper.print_values(data_with_kdj, 100)        
        previous_k = 50
        previous_d = 50
        index = 0       
        data_with_kdj.reverse()
        #last_index_value = self.get_last_index_value()
        for date_info in data_with_kdj:
            index = index + 1 
            '''   
            if(last_index_value != None and last_index_value[G_CONST_DATE_FIELD_NAME] > date_info[G_CONST_DATE_FIELD_NAME]):
                continue
            if(last_index_value != None and last_index_value[G_CONST_DATE_FIELD_NAME] == date_info[G_CONST_DATE_FIELD_NAME]):
                previous_k = last_index_value[G_CONST_CALCULATION_K_NAME]
                previous_d = last_index_value[G_CONST_CALCULATION_D_NAME]
                continue
            '''
            if(date_info.has_key(G_CONST_CALCULATION_RSV_NAME)):                 
                rsv_value = date_info[G_CONST_CALCULATION_RSV_NAME]
                k_value = float(2)/3 * previous_k + float(1)/3 * rsv_value
                d_value = float(2)/3 * previous_d + float(1)/3 * k_value
                j_value = float(3) * k_value - float(2) * d_value
                date_info[G_CONST_CALCULATION_K_NAME] = k_value
                date_info[G_CONST_CALCULATION_D_NAME] = d_value
                date_info[G_CONST_CALCULATION_J_NAME] = j_value
                previous_k = k_value
                previous_d = d_value
            #if(index % period == 0):
                #previous_k = 50
                #previous_d = 50 
        data_with_kdj.reverse()                 
        return data_with_kdj
    def calc_j_with_data(self,data1, data2):
        return float(3) * data1 - float(2) * data2
    
    def calc_KDJ(self, period, data):
        #data_with_rss = self.calc_RSV(period, data)
        calculator =  self.index_calculator
        data_with_rss = calculator.complexCalculateIndex(data, period, G_CONST_CALCULATION_RSV_NAME, method = self.calc_rsv_value)
        k_data = calculator.smaIndex(data_with_rss, G_CONST_CALCULATION_RSV_NAME, 3, 1, G_CONST_CALCULATION_K_NAME, 50)
        d_data = calculator.smaIndex(k_data, G_CONST_CALCULATION_K_NAME, 3, 1, G_CONST_CALCULATION_D_NAME, 50)
        jMethod = self.calc_j_with_data
        j_data = calculator.simpleCalculateIndex(k_data, G_CONST_CALCULATION_K_NAME, d_data, G_CONST_CALCULATION_D_NAME, G_CONST_CALCULATION_J_NAME, jMethod)
        #db_helper = DBHelper()
        #db_helper.print_values(data_with_kdj, 100)
        data_with_kdj = calculator.mergeIndex(k_data, d_data, j_data)                      
        return data_with_kdj    


    def is_good_one(self, index_data):
        indexData = index_data
        if(len(indexData) == 0):
            return False
        is_good = 0
        if(self.is_below_mid(index_data, 0)):
            is_good = 3
        return is_good
        monitorCount = self.get_monitor_count()
        index = 0
        
        preDea = None
        latestMonitorCount = 3
        for dateInfo in indexData:
            if(monitorCount <= index):
                break
                
            if(dateInfo.has_key(G_CONST_CALCULATION_K_NAME) and dateInfo.has_key(G_CONST_CALCULATION_D_NAME) and dateInfo.has_key(G_CONST_CALCULATION_J_NAME)):
                k_value = dateInfo[G_CONST_CALCULATION_K_NAME]
                d_value = dateInfo[G_CONST_CALCULATION_D_NAME]
                j_value = dateInfo[G_CONST_CALCULATION_J_NAME]
                dateValue = dateInfo[G_CONST_DATE_FIELD_NAME]
                if(abs(d_value - k_value) < 3 and d_value >= k_value and d_value < 50):
                    #self.logger.logInfo("[{}] k_value={}, d_value={}".format(dateValue, k_value, d_value))              
                    is_good = 3
                    for i in range(1, latestMonitorCount + 1):
                        if(index + i >= len(indexData)):
                            break
                        curDateInfo = indexData[index + i]
                        m_k_value = curDateInfo[G_CONST_CALCULATION_K_NAME]
                        m_d_value = curDateInfo[G_CONST_CALCULATION_D_NAME]
                        m_j_value = curDateInfo[G_CONST_CALCULATION_J_NAME] 
                        m_dateValue = curDateInfo[G_CONST_DATE_FIELD_NAME]                                      
                        if(m_k_value > m_d_value):
                            is_good = 0
                            break
                        #else:
                            #self.logger.logInfo("[{}] m_k_value={}, m_d_value={}".format(m_dateValue, m_k_value, m_d_value))
                    break                        
                preDea = d_value
                index += 1
                '''
                if(isUp):                    
                    if(d_value > k_value and d_value < 50):
                        is_good = 3
                        break
                    else:
                        isUp = False
                if(d_value < k_value):
                    isUp = True 
                ''' 
        #if(isValidated and not isDownTrend):
            #is_good = 3                              
        return is_good         
 
    def is_below_mid(self, index_data, index):
        dateInfo = index_data[index]
        k_value = dateInfo[G_CONST_CALCULATION_K_NAME]
        d_value = dateInfo[G_CONST_CALCULATION_D_NAME]
        if(d_value < 50 ):
            return True
        else:
            return False
                
                

          

if __name__ == '__main__': 
    dbHelper = DBHelper()
    stock_number = "000019"
    stock_tag = "sz"
    collection_name = G_CONST_DB_DETAIL_TABLE + "_" + stock_number + "_" + stock_tag
    period = 9
    duration = ("2015-03-05","2016-10-11")
    indx_handler = IndexKDJ(collection_name, period, duration=duration, study_count=2)
    #indx_handler.update_data()
    raw_data = indx_handler.get_raw_data()
    dbHelper.print_values(raw_data, 100, [G_CONST_DATE_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME, G_CONST_ADJ_RATE_FIELD_NAME], False)
    #data_kdj = indx_handler.get_index_data(raw_data)
    #print("count is " + str(len(raw_data)))
    #data_kdj = indx_handler.get_refined_data()
    #dbHelper.print_values(data_kdj, 100, [G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_K_NAME, G_CONST_CALCULATION_D_NAME,G_CONST_CALCULATION_J_NAME], False)
    #values = indx_handler.study_good()
    #macd_days = [5, 10, 2]
    #macd_handler = IndexMACD(collection_name, macd_days, duration=duration, study_count=500) 
    #values2 = macd_handler.study_good()
    #newValues = dbHelper.merge_index_data(values, values2)
    #dbHelper.print_values(values, 200,[G_CONST_DATE_FIELD_NAME, G_CONST_STUDY_DEAL_DAYS, G_CONST_STUDY_GOOD_VALUE, G_CONST_STUDY_REAL_RATE, G_CONST_STUDY_CORRECT],False)
    

    
          