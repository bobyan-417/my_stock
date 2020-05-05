'''
Created on Aug 28, 2016

@author: yanb
'''
from my_test.index.Index import Index
from my_test.conf.Configuration import G_CONST_DATE_FIELD_NAME,\
    G_CONST_ID_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME, G_CONST_DB_DETAIL_TABLE,\
    G_CONST_CALCULATION_MACD_DIFF_NAME, G_CONST_CALCULATION_MACD_DEA_NAME,\
    G_CONST_CALCULATION_MACD_BAR_NAME, G_CONST_DAILY_RATIO_FIELD_NAME,\
    G_CONST_STUDY_GOOD_VALUE, G_CONST_STUDY_INFO, G_CONST_STUDY_REAL_RATE,\
    G_CONST_CALCULATION_MACD_LONG_NAME, G_CONST_CALCULATION_MACD_SHORT_NAME,\
    G_CONST_STUDY_CORRECT
from my_test.utils.Assist import DBHelper






class IndexMACD(Index):
    '''
    classdocs
    '''

    def __init__(self,  collection_name , macd_days = [12, 26, 9], monitor_count = 8, duration = None, study_count = 200, verify_date = None, verify_count = 150, study_good_ratio = 0.05, study_good_count = 10):
        super(IndexMACD, self).__init__(collection_name, monitor_count, duration, study_count, verify_date, verify_count, study_good_ratio, study_good_count)
        self.macd_days = macd_days
        self.index_prefix = "INDEX_MACD_"

    def get_index_db_name(self):
        db_instance_name = "STOCK_INDEX_MACD_MIX"    
        return db_instance_name
     
    def get_index_data(self, raw_data):
        #DBHelper().print_values(raw_data)        
        data = self.calc_MACD(raw_data)
        return data  

    def get_previous_max_days(self):      
        max_day = 2
        return max_day + 1
    
    def get_divid_value(self, ema_number):
        
        return sum(range(1, ema_number + 1))
    
    def calculate_macd(self, data1, data2):
        return(data1 - data2) * 2
    def calc_MACD(self,  data):
        commenIndex = self.index_calculator
        
        shortPeriod = self.macd_days[0]
        longPeriod = self.macd_days[1]
        emaPeriod = self.macd_days[2]
        if(len(data) < longPeriod):
            return []
        newData1 = commenIndex.emaIndex(data, G_CONST_ClOSE_FIELD_NAME, shortPeriod, G_CONST_CALCULATION_MACD_SHORT_NAME)
    
        newData2 = commenIndex.emaIndex(data, G_CONST_ClOSE_FIELD_NAME, longPeriod, G_CONST_CALCULATION_MACD_LONG_NAME)
    
        diffData =  commenIndex.simpleCalculateIndex(newData1, G_CONST_CALCULATION_MACD_SHORT_NAME, 
                                        newData2, G_CONST_CALCULATION_MACD_LONG_NAME, G_CONST_CALCULATION_MACD_DIFF_NAME)
    
        deaData = commenIndex.emaIndex(diffData, G_CONST_CALCULATION_MACD_DIFF_NAME, emaPeriod, G_CONST_CALCULATION_MACD_DEA_NAME)
        sMethod = self.calculate_macd 
        macdData = commenIndex.simpleCalculateIndex(diffData, G_CONST_CALCULATION_MACD_DIFF_NAME, 
                                        deaData, G_CONST_CALCULATION_MACD_DEA_NAME, G_CONST_CALCULATION_MACD_BAR_NAME, sMethod)
        data_with_macd = commenIndex.mergeIndex(macdData, diffData, deaData)
        return data_with_macd
    
    def calc_MACD1(self,  data):
        data_with_macd = []
        index = 0
        total_count = len(data)
        #print("total count:{}".format(total_count))
        emaPeriods = []
        emaPeriods.append(self.macd_days[0])
        emaPeriods.append(self.macd_days[1])
        deaPeriod = self.macd_days[2]
        preEMAs = []
        preDEA = 0
        data.reverse()
        emaFeildsNames = [G_CONST_CALCULATION_MACD_SHORT_NAME, G_CONST_CALCULATION_MACD_LONG_NAME]
        for period in emaPeriods:
            totalValue = 0
            for i in range(0, period):
                totalValue += data[i][G_CONST_ClOSE_FIELD_NAME]
            avgValue = totalValue/period
            preEMAs.append(avgValue)
        maxPeriod = emaPeriods[-1]
        while(index < total_count):
            rawDateInfo = data[index]
            macdDateInfo = {G_CONST_DATE_FIELD_NAME : rawDateInfo[G_CONST_DATE_FIELD_NAME], 
                             G_CONST_ID_FIELD_NAME : rawDateInfo[G_CONST_ID_FIELD_NAME]}
            closeValue = rawDateInfo[G_CONST_ClOSE_FIELD_NAME] 
            i = 0
            for period in emaPeriods:
                #print("compare {} and {}".format(index, period))
                if(index < period):
                    continue
                fieldName =  emaFeildsNames[i]
                emaValue =  float(preEMAs[i] * (period - 1) + closeValue * 2)/(period + 1)
                macdDateInfo[fieldName] =  emaValue   
                preEMAs[i] = emaValue
                i += 1
            if(index >= maxPeriod):
                diff = preEMAs[0] - preEMAs[1] 
                diffFieldName = G_CONST_CALCULATION_MACD_DIFF_NAME
                macdDateInfo[diffFieldName] =  diff                  
                if(index < maxPeriod + deaPeriod - 1):
                    preDEA += diff
                elif(index == maxPeriod + deaPeriod - 1):
                    preDEA += diff
                    preDEA = preDEA/deaPeriod
                else:  
                    deaFieldName = G_CONST_CALCULATION_MACD_DEA_NAME          
                    deaValue = float(preDEA * (deaPeriod - 1) + diff * 2)/(deaPeriod + 1)
                    macdDateInfo[deaFieldName] =  deaValue              
                    macdBar = (diff - deaValue) * 2  
                    macdBarFieldName = G_CONST_CALCULATION_MACD_BAR_NAME
                    macdDateInfo[macdBarFieldName] =  macdBar     
                    preDEA = deaValue
            data_with_macd.append(macdDateInfo)  
            
            index += 1 
            
        data.reverse()
        data_with_macd.reverse()
        return data_with_macd


    def is_good_one(self, index_data):
        indexData = index_data
        if(len(indexData) == 0):
            return False
        dateInfo = indexData[0]
        dateValue = dateInfo[G_CONST_DATE_FIELD_NAME]
        dea = dateInfo[G_CONST_CALCULATION_MACD_DEA_NAME]
        is_good = 0
        previous_bar =  0  
        #DBHelper().print_values(indexData, 40)
        monitorCount = self.get_monitor_count()
        index = 0             
        is_good = 0
        if(self.is_down_trend(index_data, 0)):
            is_good = 3
        '''
        crossMonitorCount = 3
        if(dea > 0.2) :
            return is_good
        if(self.is_down_trend(indexData, index)):
            return is_good
        if(self.is_up_trend(indexData, index)):
            #self.logger.logInfo("[{}]macd is up trend".format(dateValue))
            is_good = 3
        else:
            for index in range(0, monitorCount):
                if(self.is_bottom_return(indexData, index)):
                    #self.logger.logInfo("[{}]macd is bottom return".format(dateValue))
                    is_good = 3
                    break
                if(self.is_cross_return(indexData, index)):
                    #self.logger.logInfo("[{}]macd is cross return".format(dateValue))
                    is_good = 3
                    break  
        '''                              
        return is_good         
 
    def is_bottom_return(self, index_data, index):
        if(len(index_data) <= index + 1):
            return False
        dateInfo = index_data[index]
        nextDateInfo = index_data[index + 1]
        curDiffValue = dateInfo[G_CONST_CALCULATION_MACD_DIFF_NAME]
        nextDiffValue = nextDateInfo[G_CONST_CALCULATION_MACD_DIFF_NAME]
        if(curDiffValue < -0.2 and curDiffValue > nextDiffValue):
            #self.logger.logInfo("curDiffValue = {}, nextDiffValue = {}".format(curDiffValue, nextDiffValue))
            return True
    def is_cross_return(self, index_data, index):
        crossMonitorCount = 6
        if(len(index_data) <= crossMonitorCount + 1):
            return False        
        isGood = False
        dateInfo = index_data[index]
        bar_value = dateInfo[G_CONST_CALCULATION_MACD_BAR_NAME]   
        macdDiff = dateInfo[G_CONST_CALCULATION_MACD_DIFF_NAME] 
        macdDEA = dateInfo[G_CONST_CALCULATION_MACD_DEA_NAME]   
        dateValue = dateInfo[G_CONST_DATE_FIELD_NAME]
        if(abs(macdDiff - macdDEA) < 0.03 and macdDiff <= macdDEA and macdDiff < 0.02):
            isGood = True
            nextDateInfo = index_data[index + 1]
            n_macdDea = nextDateInfo[G_CONST_CALCULATION_MACD_DEA_NAME]
            if(macdDEA < n_macdDea):
                isGood = False
            else:  
                for i in range(1, crossMonitorCount + 1):
                    curDateInfo = index_data[index + i]
                    m_bar_value = dateInfo[G_CONST_CALCULATION_MACD_BAR_NAME]   
                    m_macdDiff = dateInfo[G_CONST_CALCULATION_MACD_DIFF_NAME] 
                    m_macdDEA = dateInfo[G_CONST_CALCULATION_MACD_DEA_NAME]                                       
                    if(m_macdDiff > m_macdDEA):
                        isGood = False
                        break
        return isGood   
    def is_up_trend(self, index_data, index, monitor_count = 4):
        monitorCount = monitor_count
        if(len(index_data) <= index + monitorCount):
            return False
        dateInfo = index_data[index]
        preMacdDEA = dateInfo[G_CONST_CALCULATION_MACD_DEA_NAME]
        isGood = True
        for i in range(1, monitorCount):
            nextDateInfo = index_data[index + i]
            n_macdDea = nextDateInfo[G_CONST_CALCULATION_MACD_DEA_NAME]
            if(preMacdDEA <= n_macdDea ):
                isGood = False
                break
            preMacdDEA = n_macdDea
        return isGood  
    def is_down_trend(self, index_data, index):
        monitorCount = 20
        #self.db_helper.print_values(index_data, 4)
        if(len(index_data) <= index + monitorCount):
            return False
        dateInfo = index_data[index]
        preMacdDEA = dateInfo[G_CONST_CALCULATION_MACD_DEA_NAME]
        isDown = True
        hasBigChange = self.is_up_trend(index_data, index, 20)
        addDiff = 0
        if(hasBigChange):
            addDiff = 0.01
        for i in range(1, monitorCount):
            nextDateInfo = index_data[index + i]
            n_macdDea = nextDateInfo[G_CONST_CALCULATION_MACD_DEA_NAME]
            dataValue = nextDateInfo[G_CONST_DATE_FIELD_NAME]
            #self.logger.logInfo("[{}] pre={}, current={}".format(dataValue, preMacdDEA, n_macdDea))
            if(preMacdDEA > (n_macdDea + addDiff)):
                isDown = False
                break
            preMacdDEA = n_macdDea
        return isDown   
    def is_below_mid(self, index_data, index, monitor_count = 3):
        monitorCount = monitor_count
        if(len(index_data) <= index + monitorCount):
            return False
        isGood = True
        for i in range(0, monitorCount):
            nextDateInfo = index_data[index + i]
            n_macdDea = nextDateInfo[G_CONST_CALCULATION_MACD_DEA_NAME]
            if(0 < n_macdDea ):
                isGood = False
                break
        return isGood  
                                    
if __name__ == '__main__': 
    dbHelper = DBHelper()
    stock_number = "000966"
    stock_tag = "sz"
    collection_name = G_CONST_DB_DETAIL_TABLE + "_" + stock_number + "_" + stock_tag
    #macd_days = [5, 10, 2]
    #indx_handler = IndexMACD(collection_name, macd_days)
    #raw_data = indx_handler.get_raw_data()
    #raw_data.reverse()
    #dbHelper.print_values(raw_data, 40, [G_CONST_DATE_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME], False)  
    '''   
    data_macd = indx_handler.get_index_data(raw_data)
    EMA26FIELD = "INDEX_MACD_" + "EMA_26"
    EMA12FIELD = "INDEX_MACD_" + "EMA_12"
    DIFFFIELD = "INDEX_MACD_" + "MACDDIFF_9"
    DEAFIELD = "INDEX_MACD_" + "MACDDEA_9"
    BARFIELD = "INDEX_MACD_" + "MACDBAR_9"
    data_macd.reverse()
    dbHelper.print_values(data_macd, 80, [G_CONST_DATE_FIELD_NAME, EMA12FIELD, EMA26FIELD, DIFFFIELD, DEAFIELD, BARFIELD], False)   
    '''
    endValue = "2016-09-06"
    duration = ("2015-08-05", endValue)
    #indx_handler = IndexMACD(collection_name, duration=duration, study_count=3)  
    indx_handler = IndexMACD(collection_name, verify_date = "2016-09-06", monitor_count = 5, study_count = 1, verify_count=600)
    #values = indx_handler.get_refined_data()
    #dbHelper.print_values(values, 150,[G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_MACD_DIFF_NAME, G_CONST_CALCULATION_MACD_DEA_NAME, G_CONST_CALCULATION_MACD_BAR_NAME], False)  
    #values = indx_handler.study_good()
    data = indx_handler.study_good()
    dbHelper.print_values(data, 200,[G_CONST_DATE_FIELD_NAME, G_CONST_STUDY_GOOD_VALUE, G_CONST_STUDY_REAL_RATE, G_CONST_STUDY_CORRECT],False)  