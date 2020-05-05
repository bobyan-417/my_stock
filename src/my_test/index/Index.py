'''
Created on Aug 24, 2016

@author: yanb
'''
from my_test.conf.Configuration import *
from my_test.utils.DBHandler import MogoDBHandler, MogoDBSetHandler
from my_test.utils.Assist import DBHelper, Logger, UnitHelper
from my_test.exception.StockException import NotImplementException
import datetime
import pymongo
from my_test.index.calclib.Common import CommonIndex



class Index(object):
    '''
    classdocs
    '''


    def __init__(self, collection_name, monitor_count = 10, duration = None, study_count = 200, verify_date = None, verify_count = 300, study_good_ratio = 0.05, study_good_count = 10):
        self.logger = Logger()
        self.collection_name = collection_name
        self.latest_date = "now"
        self.start_time_field_name = "start_time"
        self.end_time_field_name = "end_time"
        self.duration = duration
        self.monitor_count = monitor_count
        self.study_count = study_count
        self.verify_date = verify_date
        self.verify_count = verify_count
        self.raw_data = None
        self.index_calculator = CommonIndex()
        self.db_helper = DBHelper()
        self.unit_helper = UnitHelper()
        self.latest_date = None
        self.data = None
        self.study_good_ratio = study_good_ratio
        self.study_good_count = study_good_count
        
    def get_latest_date(self):        
        return self.latest_date
    
    def get_monitor_count(self):
        '''
        unitHelper = UnitHelper()
        monitor_period = self.monitor_period
        seconds_range = unitHelper.toSecondNumber(monitor_period)
        firstDate = start_date - datetime.timedelta(seconds= seconds_range)   
        '''     
        return self.monitor_count
    def get_study_count(self):
        return self.study_count 
    def get_index_db_name(self):
        raise NotImplementException("Required child to implement this method")  
        
    def get_index_data(self, raw_data):
        raise NotImplementException("Required child to implement this method")   
     
    def get_previous_max_days(self):
        raise NotImplementException("Required child to implement this method")  
    
        
    def get_raw_data_date_range(self):     
        if(self.duration != None):
            return self.duration
        elif(self.verify_date != None):
            endDate = self.unit_helper.convertToDatetime(self.verify_date)
            startDate = self.unit_helper.reduceDays(endDate, self.verify_count)
            return (startDate, endDate)
        else:
            max_day = self.get_previous_max_days()   
            latest_index_values = self.get_latest_index_date_info(max_day)
            #print("count is " + str(len(latest_index_values)))
            #DBHelper().print_values(latest_index_values)
            date_range = ()
            if(len(latest_index_values) == 0):
                date_range = (G_CONST_START_TIME)
            else:
                last_index_value = latest_index_values[-1]
                start_time = last_index_value[G_CONST_DATE_FIELD_NAME]
                date_range = (start_time)     
    
            return date_range
        
    def get_refined_data(self):
        indexData = None
        if(self.data != None):
            latestDate = self.data[0][G_CONST_DATE_FIELD_NAME]
            dateRange = self.get_raw_data_date_range()
            if(len(dateRange) > 0 and dateRange[1] <= latestDate):
                indexData = filter(lambda x: x[G_CONST_DATE_FIELD_NAME] <= dateRange[1], self.data)
        if(indexData == None):
            rawData = self.get_raw_data()        
            indexData = self.get_index_data(rawData)
        if(len(indexData) > 0):
            self.latest_date = indexData[0][G_CONST_DATE_FIELD_NAME]
        return indexData
    
    def update_data(self):        
        index_data = self.get_refined_data()
        #DBHelper().print_values(index_data)
        self.save_data_to_index_db(index_data)
        return
    def get_index_db(self):
        dbInstance = self.get_index_db_name()
        dbConnection = MogoDBHandler(db_server = G_CONST_DB_SERVER, 
                                  db_port= G_CONST_DB_PORT, 
                                  db_instance= dbInstance, 
                                  collection_name = self.collection_name,
                                  ) 
        return dbConnection
    def get_raw_data(self):
        dbConnection = MogoDBHandler(db_server = G_CONST_DB_SERVER, 
                                  db_port= G_CONST_DB_PORT, 
                                  db_instance= G_CONST_DB_INSTANCE_NAME, 
                                  collection_name = self.collection_name,
                                  ) 
        date_range = self.get_raw_data_date_range()
        data = None
        if(type(date_range) is tuple):
            #self.logger.logInfo("raw date range[{}, {}]".format(*date_range))
            data = dbConnection.getValues(*date_range)
        else:
            data = dbConnection.getValues(date_range)
        #DBHelper().print_values(data, 3)
        data = self.adjust_close_data(data)
        self.raw_data = data
        return data 
    
    def adjust_close_data(self, raw_data):
        prevousClose = None
        previousRatio = None
        for dateInfo in raw_data:
            if(previousRatio == None and dateInfo.has_key(G_CONST_ADJ_RATE_FIELD_NAME)):
                prevousClose = dateInfo[G_CONST_ClOSE_FIELD_NAME]
                previousRatio = dateInfo[G_CONST_ADJ_RATE_FIELD_NAME]
            elif(previousRatio != None):
                adjCloseValue = prevousClose /(1 + previousRatio)
                curCloseValue = dateInfo[G_CONST_ClOSE_FIELD_NAME]
                if(curCloseValue != 0):                    
                    adjRatio = adjCloseValue/curCloseValue                
                    dateInfo[G_CONST_ClOSE_FIELD_NAME] *= adjRatio
                    dateInfo[G_CONST_LOW_FIELD_NAME] *= adjRatio
                    dateInfo[G_CONST_HIGH_FIELD_NAME] *= adjRatio                   
                    prevousClose = dateInfo[G_CONST_ClOSE_FIELD_NAME]
                    if(dateInfo.has_key(G_CONST_ADJ_RATE_FIELD_NAME)):
                        previousRatio = dateInfo[G_CONST_ADJ_RATE_FIELD_NAME] 
                    else:
                        previousRatio = None
                else:
                    previousRatio = None
                

        return raw_data
    
    def get_raw_data_by_index(self, index_data_info, relative_locs = [0]):
        if(not self.raw_data):
            return None
        index = 0
        rawData = self.raw_data
        expectedDate = index_data_info[G_CONST_DATE_FIELD_NAME]
        for dateInfo in rawData:
            if(dateInfo[G_CONST_DATE_FIELD_NAME] == expectedDate):
                break
            index += 1
        expecteDateInfo = []
        for relative_loc in relative_locs:               
            loc = index + relative_loc
            if(loc > 0 and loc < len(rawData)):
                expecteDateInfo.append(rawData[loc])
        return expecteDateInfo
       
    def get_latest_index_date_info(self, count):
        dbHandler = self.get_index_db()
        values = dbHandler.getLatestValue(count)
        return values
    def get_last_index_value(self):
        latest_index_date_value = None
        latest_index_date_values = self.get_latest_index_date_info(1)
        if(len(latest_index_date_values) > 0) :
            latest_index_date_value = latest_index_date_values[0]
        return latest_index_date_value    
    def save_data_to_index_db(self, index_data):
        dbConnection = self.get_index_db()
        try:
            for date_values in index_data:
                dbConnection.saveSingleValue(date_values)
        except pymongo.errors.DuplicateKeyError as e:
            print e         

    def getRequiredList(self):
        pass

    def study_good(self):  
        indexData = self.get_refined_data()  
        #self.db_helper.print_values(indexData, 10)      
        studyCount = self.get_study_count()
        index = 0
        goodTables = []

        #DBHelper().print_values(indexData, 40)
        while(index < studyCount and index < len(indexData)):
            isGood = self.is_good_one(indexData)
            dateInfo = indexData.pop(0)  
            newData = dateInfo     
            start = -1       
            study_good_count = self.study_good_count  
            waitTimeRange = range(-study_good_count, start)
            waitTimeRange.reverse()                    
            if(isGood >= 3):  
                nextDateInfos = self.get_raw_data_by_index(dateInfo, waitTimeRange)
                (days, nextRealRate, date, firstValue, lastValue) = self.get_increase_status(nextDateInfos)
                newData[G_CONST_STUDY_GOOD_VALUE] = isGood
                newData[G_CONST_STUDY_REAL_RATE] = str(nextRealRate) 
                newData[G_CONST_STUDY_DEAL_DAYS] = days 
                newData[G_CONST_STUDY_DEAL_DATE] = date
                newData[G_CONST_STUDY_DEAL_END_VALUE] = lastValue
                newData[G_CONST_STUDY_DEAL_START_VALUE] = firstValue
                #self.logger.logInfo("append date is {}".format(dateInfo[G_CONST_DATE_FIELD_NAME]))     
                if(nextRealRate > 0):
                    newData[G_CONST_STUDY_CORRECT] = 1 
                else:
                    newData[G_CONST_STUDY_CORRECT] = -1
                goodTables.append(newData)
               
            index += 1
        return goodTables 

    def is_good_by(self):  
        indexData = self.get_refined_data()        
        if(len(indexData) == 0):
            return False
        isGood = self.is_good_one(indexData) 
        if(isGood >= 3):
            return True
        else:
            return False
  
    def get_increase_status(self, date_info_in_days):
        rate = 1
        days = 0
        date = None
        firstValue = None
        if(len(date_info_in_days) > 0):
            firstValue = date_info_in_days[0][G_CONST_ClOSE_FIELD_NAME]
        lastValue = None
        while(days < len(date_info_in_days)):
            nextDateInfo = date_info_in_days[days]            
            nextRealRate = nextDateInfo[G_CONST_ADJ_RATE_FIELD_NAME]
            date = nextDateInfo[G_CONST_DATE_FIELD_NAME]
            #self.logger.logInfo("[{}] rate is {}".format(nextDate, nextRealRate))
            if(nextRealRate == None):
                days += 1
                continue
            rate = rate * (1 + nextRealRate)
            lastValue = nextDateInfo[G_CONST_ClOSE_FIELD_NAME]
            downRatio = 1 - self.study_good_ratio
            upRatio = 1 + self.study_good_ratio
            if(rate < downRatio or rate > upRatio):
                break
            days += 1
        rate = rate - 1
        rate = round(rate, 3)
        return(days, rate, date, firstValue, lastValue)                           
            