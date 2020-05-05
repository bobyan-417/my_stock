'''
Created on Aug 21, 2016

@author: yanb
'''
import copy

from my_test.conf.Configuration import *
from my_test.utils.DBHandler import MogoDBHandler
from my_test.utils.Assist import DBHelper, Logger
import pymongo
import operator
import copy

class CommonIndex(object):
    '''
    classdocs
    '''


    def __init__(self):
        self.logger = Logger()
        '''
        Constructor
        '''
    def emaIndex1(self, data, c_field_name, period, ema_field_name):   
        data.reverse()
        newData = []  
        emaList = []
        emaPeriodRange = range(1, period + 1)
        for rawDateInfo in data:
            emaData = {G_CONST_DATE_FIELD_NAME : rawDateInfo[G_CONST_DATE_FIELD_NAME], 
                       G_CONST_ID_FIELD_NAME : rawDateInfo[G_CONST_ID_FIELD_NAME]}
            cValue = rawDateInfo[c_field_name]  
            if(len(emaList) < period - 1):
                emaList.append(cValue)
            else:
                emaList.append(cValue)
                shortEMAMultiply = map(lambda x, y : x * y, emaList, emaPeriodRange)
                emaValue = sum(shortEMAMultiply)/sum(emaPeriodRange)  
                emaData[ema_field_name] =  emaValue
                newData.append(emaData)
                emaList.pop(0)
        data.reverse()
        newData.reverse()                
        return newData
    def emaIndex(self, data, c_field_name, period, ema_field_name):   
        data.reverse()
        newData = [] 
        index = 0
        preEMA = 0
        if(len(data) < period):
            return []
        for index in range(0, period):
            preEMA += data[index][c_field_name]
            index += 1
        preEMA = round(float(preEMA)/period, 3)
       
        while(index < len(data)):
            rawDateInfo = data[index]
            
            if(not rawDateInfo.has_key(c_field_name)):
                index += 1
                continue             
            emaData = {G_CONST_DATE_FIELD_NAME : rawDateInfo[G_CONST_DATE_FIELD_NAME], 
                       G_CONST_ID_FIELD_NAME : rawDateInfo[G_CONST_ID_FIELD_NAME]}
            cValue = rawDateInfo[c_field_name]  
            emaValue = (preEMA * (period - 1)  + 2 * cValue)/(period + 1)
            emaData[ema_field_name] =  emaValue
            newData.append(emaData)
            preEMA = emaValue
            index += 1
        data.reverse()
        newData.reverse()                
        return newData
    def smaIndex(self, data, c_field_name, N, M, sma_field_name, first_value = 50):
        data.reverse()
        newData = [] 
        index = 0
        preSMA = float(first_value)
        while(index < len(data)):
            rawDateInfo = data[index]
            if(not rawDateInfo.has_key(c_field_name)):
                index += 1
                continue            
            emaData = {G_CONST_DATE_FIELD_NAME : rawDateInfo[G_CONST_DATE_FIELD_NAME], 
                       G_CONST_ID_FIELD_NAME : rawDateInfo[G_CONST_ID_FIELD_NAME]}
            cValue = rawDateInfo[c_field_name]
            emaValue = (preSMA * (N-M)  + M * cValue)/N
            emaData[sma_field_name] =  emaValue
            newData.append(emaData)
            preSMA = emaValue
            index += 1
        data.reverse()
        newData.reverse()                
        return newData  
    def maIndex(self, data, c_field_name, N, sma_field_name):
        data.reverse()
        newData = [] 
        index = 0
        totalValue = 0
        #self.logger.logInfo("calculate ma:count = {}".format(len(data)))
        while(index < len(data)):
            rawDateInfo = data[index]
            #self.logger.logInfo("index[{}]:{}".format(index, rawDateInfo[G_CONST_DATE_FIELD_NAME]))
            if(not rawDateInfo.has_key(c_field_name)):
                index += 1
                continue              
            if(index < (N - 1)):
                totalValue += rawDateInfo[c_field_name]
                index += 1
                continue
            else:
                totalValue += rawDateInfo[c_field_name]
                maData = {G_CONST_DATE_FIELD_NAME : rawDateInfo[G_CONST_DATE_FIELD_NAME], 
                       G_CONST_ID_FIELD_NAME : rawDateInfo[G_CONST_ID_FIELD_NAME]}
                maValue = float(totalValue)/N
                maData[sma_field_name] =  maValue
                newData.append(maData)
                totalValue -= data[index - N + 1][c_field_name]
            index += 1
        data.reverse()
        newData.reverse()                
        return newData  
    def simpleReduce(self, data1, data2):
        return data1 - data2
    def simpleDivide(self, data1, data2):
        return float(data1)/data2
    
    def complexCalculateIndex(self, data1, data1_num, new_field_name, data2 = None, data2_num = 1, method = None):
        i = 0
        j = 0
        newData = []
        if(method == None):
            return
        while(j < len(data1)):
            dateInfo1 = data1[j]
            date1 = dateInfo1[G_CONST_DATE_FIELD_NAME]
            if(j+ data1_num >= len(data1)):
                break
            if(data2 != None and i >= len(data2)):
                break
            newDateInfo = {G_CONST_DATE_FIELD_NAME : dateInfo1[G_CONST_DATE_FIELD_NAME], 
                           G_CONST_ID_FIELD_NAME : dateInfo1[G_CONST_ID_FIELD_NAME]} 
            value = None           
            if(data2 != None):
                dateInfo2 = data2[i]
                date2 = dateInfo2[G_CONST_DATE_FIELD_NAME]
                if(date1 > date2):
                    j = j + 1
                    continue
                elif(date1 < date2):
                    while(date1 < date2 and i < len(data2)):
                        dateInfo2 = data2[i]
                        date2 = dateInfo2[G_CONST_DATE_FIELD_NAME]  
                        i += 1 
                if(date1 == date2):
                    calcData1 = data1[j:(j+ data1_num )] 
                    calcData2 = data2[i:(j+ data1_num )] 
                    value = method(calcData1, calcData2)
                    i += 1
            else:
                calcData = data1[j:(j+ data1_num )]                 
                value = method(calcData)
                newDateInfo[new_field_name] = value
            if(value != None):
                newDateInfo[new_field_name] = value
                newData.append(newDateInfo)  
            j += 1              
                                                                           
        return newData 
    
    def simpleCalculateIndex(self, data1, data1_field_name, data2, data2_field_name, new_field_name, method = None):   
        i = 0
        newData = []
        if(method == None):
            method = self.simpleReduce
        for dateInfo1 in data1:
            date1 = dateInfo1[G_CONST_DATE_FIELD_NAME]
            if(i >= len(data2)):
                break
            dateInfo2 = data2[i]
            date2 = dateInfo2[G_CONST_DATE_FIELD_NAME]
            if(date1 > date2):
                continue
            elif(date1 < date2):
                while(date1 < date2 and i < len(data2)):
                    dateInfo2 = data2[i]
                    date2 = dateInfo2[G_CONST_DATE_FIELD_NAME]  
                    i += 1 
            if(date1 == date2):
                newDateInfo = {G_CONST_DATE_FIELD_NAME : dateInfo1[G_CONST_DATE_FIELD_NAME], 
                               G_CONST_ID_FIELD_NAME : dateInfo1[G_CONST_ID_FIELD_NAME]}
                value = method(dateInfo1[data1_field_name] , dateInfo2[data2_field_name])
                newDateInfo[new_field_name] = value
                newData.append(newDateInfo) 
                i += 1                                             
        return newData 
                           
    def mergeIndex(self, data1, *data):
        newData = copy.deepcopy(data1)
        if(type(data) == tuple):
            for mydata in data:
                for i,j in zip(newData, mydata):
                    i.update(j)
        else:
            for i,j in zip(newData, data):
                i.update(j)
        return newData
    
#from my_test.index.IndexMACD import IndexMACD   
def special_method(data1, data2):
    return (data1 - data2) * 2         
if __name__ == '__main__':
    dbHelper = DBHelper()
    stock_number = "000807"
    stock_tag = "sz"
    collection_name = G_CONST_DB_DETAIL_TABLE + "_" + stock_number + "_" + stock_tag
    macd_days = [12, 26, 9]    
    duration = ("2013-05-05","2016-08-08")
    '''
    indx_handler = IndexMACD(collection_name, macd_days, duration=duration, study_count=500) 
    
    commenIndex = CommonIndex()
    rawData = indx_handler.get_raw_data()
    newData1 = commenIndex.emaIndex(rawData, G_CONST_ClOSE_FIELD_NAME, 12, G_CONST_CALCULATION_MACD_SHORT_NAME)

    newData2 = commenIndex.emaIndex(rawData, G_CONST_ClOSE_FIELD_NAME, 26, G_CONST_CALCULATION_MACD_LONG_NAME)

    diffData =  commenIndex.simpleCalculateIndex(newData1, G_CONST_CALCULATION_MACD_SHORT_NAME, 
                                    newData2, G_CONST_CALCULATION_MACD_LONG_NAME, G_CONST_CALCULATION_MACD_DIFF_NAME)

    deaData = commenIndex.emaIndex(diffData, G_CONST_CALCULATION_MACD_DIFF_NAME, 9, G_CONST_CALCULATION_MACD_DEA_NAME)
    sMethod = special_method
    macdData = commenIndex.simpleCalculateIndex(diffData, G_CONST_CALCULATION_MACD_DIFF_NAME, 
                                    deaData, G_CONST_CALCULATION_MACD_DEA_NAME, G_CONST_CALCULATION_MACD_BAR_NAME, sMethod)
    expectedData = commenIndex.mergeIndex(macdData, diffData, deaData)
    dbHelper.print_values(expectedData, 20, [G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_MACD_DIFF_NAME, G_CONST_CALCULATION_MACD_DEA_NAME, G_CONST_CALCULATION_MACD_BAR_NAME])
    '''    
    #data_kdj = indx_handler.calc_KDJ(9, data)
    #db_helper = DBHelper()
    #db_helper.print_values(data_kdj, 100, [G_CONST_DATE_FIELD_NAME,G_CONST_CALCULATION_K_NAME, G_CONST_CALCULATION_D_NAME, G_CONST_CALCULATION_J_NAME, G_CONST_CALCULATION_RSV_NAME])   
        