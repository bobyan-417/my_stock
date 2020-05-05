'''
Created on Aug 26, 2016

@author: yanb
'''
from my_test.index.Index import Index
from my_test.conf.Configuration import  G_CONST_DATE_FIELD_NAME,\
    G_CONST_ID_FIELD_NAME, G_CONST_CALCULATION_RSI_12_NAME,\
    G_CONST_CALCULATION_RSI_24_NAME, G_CONST_CALCULATION_RSI_6_NAME,\
    G_CONST_ClOSE_FIELD_NAME, G_CONST_DB_DETAIL_TABLE,\
    G_CONST_CALCULATION_RSI_UP_NAME, G_CONST_CALCULATION_RSI_DOWN_NAME,\
    G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME,\
    G_CONST_CALCULATION_RSI_SIMPLE_DOWN_NAME
from my_test.utils.Assist import DBHelper
import copy

class IndexRSI(Index):
    '''
    classdocs
    '''
    def __init__(self,  collection_name, monitor_period = "5d",  monitor_count = 8, duration = None, study_count = 20, verify_date = None, verify_count = 150, study_good_ratio = 0.05, study_good_count = 10):
        super(IndexRSI, self).__init__(collection_name, monitor_count, duration, study_count, verify_date, verify_count, study_good_ratio, study_good_count)
        self.rsi_fields_info = {#6 :  G_CONST_CALCULATION_RSI_6_NAME,
                                12 : G_CONST_CALCULATION_RSI_12_NAME,
                                #24 : G_CONST_CALCULATION_RSI_24_NAME
                                }


    def get_index_db_name(self):
        db_instance_name = "STOCK_INDEX_RSI_MIX"    
        return db_instance_name
     
    def get_index_data(self, raw_data):
        #DBHelper().print_values(raw_data)        
        data = self.calc_RSI(self.rsi_fields_info, raw_data)
        return data  

    def get_previous_max_days(self):      
        fields_periods = self.rsi_fields_info.keys()
        fields_periods.sort()  
        max_day = fields_periods[-1]
        return max_day + 1
    
    def calc_raw_up_down(self, data):        
        rawData = copy.deepcopy(data)        
        rawData.reverse()
        preClose = None     
        totalUpValue = 0
        totalDownValue = 0
        upDiffList = []
        downDiffList = []
        for dateInfo in rawData:
            closeValue = dateInfo[G_CONST_ClOSE_FIELD_NAME]
            if(preClose == None):
                preClose = closeValue
                #upDiffList.append(0)
                #downDiffList.append(0)
            else:    
                diff = closeValue - preClose
                preClose = closeValue
                if(diff > 0):                    
                    dateInfo[G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME] = diff
                    dateInfo[G_CONST_CALCULATION_RSI_SIMPLE_DOWN_NAME] = 0
                    #totalUpValue += diff
                    #upDiffList.append(diff)
                    #downDiffList.append(0)
                else:
                    dateInfo[G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME] = 0
                    dateInfo[G_CONST_CALCULATION_RSI_SIMPLE_DOWN_NAME] = abs(diff)                    
                    #totalDownValue += abs(diff)
                    #downDiffList.append(abs(diff))
                    #upDiffList.append(0)
            '''
            if(len(upDiffList) == n):
                dateInfo[G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME] = float(totalUpValue)/n
                dateInfo[G_CONST_CALCULATION_RSI_SIMPLE_DOWN_NAME] = float(totalDownValue)/n
                totalUpValue -= upDiffList.pop(0)
                totalDownValue -= downDiffList.pop(0)
            '''
        rawData.reverse()                
        return rawData
    
    def calc_rsi_rate(self, up_data, down_data):
        if(down_data == 0):
            return 100
        rs = float(up_data)/(down_data) 
        rsi = rs/(1+rs) * 100
        return rsi
    def calc_rsi_up(self, data):
        curDateInfo = data[0]
        preDateInfo = data[1]
        diff = curDateInfo[G_CONST_ClOSE_FIELD_NAME] - preDateInfo[G_CONST_ClOSE_FIELD_NAME]
        if(diff < 0):
            diff = 0
        #print("diff is ".format(diff))
        return diff
    def calc_rsi_down(self, data):
        curDateInfo = data[0]
        preDateInfo = data[1]
        diff = curDateInfo[G_CONST_ClOSE_FIELD_NAME] - preDateInfo[G_CONST_ClOSE_FIELD_NAME]
        if(diff > 0):
            diff = 0
        return abs(diff)       
    def calc_RSI(self, field_info, data):
        
        #
        calculator = self.index_calculator        
        fields_periods = self.rsi_fields_info.keys()
        fields_periods.sort()
        rsiData = None
        #rawData = self.calc_raw_up_down(data)
        #DBHelper().print_values(rawData, 50, [G_CONST_DATE_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME, G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME, G_CONST_CALCULATION_RSI_SIMPLE_DOWN_NAME], False)
        #self.logger.logInfo("rsi_up")
        upRawData = calculator.complexCalculateIndex(data, 2, G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME, method = self.calc_rsi_up)
        #self.db_helper.print_values(upRawData, 20, [G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME])
        #self.logger.logInfo("rsi_down")
        downRawData = calculator.complexCalculateIndex(data, 2, G_CONST_CALCULATION_RSI_SIMPLE_DOWN_NAME, method = self.calc_rsi_down)
        #self.logger.logInfo("rsi_up_down_merge")
        rawData = calculator.mergeIndex(upRawData, downRawData, data)
        #self.db_helper.print_values(rawData, 40, [G_CONST_DATE_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME, G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME, G_CONST_CALCULATION_RSI_SIMPLE_DOWN_NAME], False)
        for period in fields_periods:
            
            #
            #break;
            #period = 12
            fieldName = self.rsi_fields_info[period]
            #self.logger.logInfo("rsi_ma_up")
            upSMAData = calculator.smaIndex(rawData, G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME, period, 1, G_CONST_CALCULATION_RSI_UP_NAME)
            
            #DBHelper().print_values(calculator.mergeIndex(rawData, upSMAData) , 50, [G_CONST_DATE_FIELD_NAME,G_CONST_CALCULATION_RSI_SIMPLE_UP_NAME,  G_CONST_CALCULATION_RSI_UP_NAME], False)
            #break
            #self.logger.logInfo("rsi_ma_down")
            downSMAData = calculator.smaIndex(rawData, G_CONST_CALCULATION_RSI_SIMPLE_DOWN_NAME, period, 1, G_CONST_CALCULATION_RSI_DOWN_NAME)
            divideMethod = self.calc_rsi_rate
            #self.logger.logInfo("rsi_divid_down_up")
            oneRsiData = calculator.simpleCalculateIndex(upSMAData, G_CONST_CALCULATION_RSI_UP_NAME, downSMAData, G_CONST_CALCULATION_RSI_DOWN_NAME, fieldName, divideMethod)
            if(rsiData == None):
                rsiData = oneRsiData
            else:
                rsiData = calculator.mergeIndex(rsiData, oneRsiData)
        return rsiData
    
    def is_good_one(self, index_data):
        indexData = index_data
        if(len(indexData) == 0):
            return False
        dateInfo = indexData[0]
        is_good = 3
        monitorCount = self.monitor_count
        firstRsiValue = dateInfo[G_CONST_CALCULATION_RSI_12_NAME]
        endDateInfo = indexData[monitorCount - 1]
        endRsiValue = endDateInfo[G_CONST_CALCULATION_RSI_12_NAME]
        if(firstRsiValue < 0 and firstRsiValue < endRsiValue):
            is_good = 0                  
        return is_good  
    
if __name__ == '__main__': 
    dbHelper = DBHelper()
    stock_number = "000004"
    stock_tag = "sz"
    collection_name = G_CONST_DB_DETAIL_TABLE + "_" + stock_number + "_" + stock_tag
    duration = ("2015-05-05","2016-09-08")
    indx_handler = IndexRSI(collection_name, duration=duration, study_count=500)
    #indx_handler.update_data()
    #raw_data = indx_handler.get_raw_data()
    #raw_data.reverse()
    #dbHelper.print_values(raw_data, 20)
    #raw_data.reverse()
    data_rsi = indx_handler.get_refined_data()
    #print("count is " + str(len(raw_data)))
    dbHelper.print_values(data_rsi, 40, [G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_RSI_6_NAME, G_CONST_CALCULATION_RSI_12_NAME, G_CONST_CALCULATION_RSI_24_NAME], False)  
        