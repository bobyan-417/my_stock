'''
Created on Aug 18, 2016

@author: yanb
'''

from my_test.utils.DBHandler import MogoDBHandler, MogoDBSetHandler
from my_test.utils.ExcelHandler import ExcelHandler
from my_test.utils.Downloader import CsvDownloader
from my_test.utils.Downloader import JsonDownloader
from my_test.utils.Assist import DBHelper
from my_test.utils.Assist import Logger
from my_test.conf.Configuration import *
import math
import datetime
import pymongo
import re

class DataFecher(object):
    '''
    classdocs
    '''


    def __init__(self, download_type, logger):
        '''
        Constructor
        '''
        self.reader = ExcelHandler(G_CONST_DEFINE_FILE)
        self.downloader = self.get_downloader_instance(download_type)
        self.download_type = download_type
        self.stock_number_index = 1
        self.logger = logger
        
    def update_all_data(self, start_collection_name = None, is_force = False):
        #tags = ['sh','sz']
        dbSetHandler = MogoDBSetHandler(db_server = G_CONST_DB_SERVER, 
                                      db_port= G_CONST_DB_PORT, 
                                      db_instance= G_CONST_DB_INSTANCE_NAME)
        collectionNames = dbSetHandler.get_all_collections()
        collectionNames.sort()
        index = 0
        needSkip = (start_collection_name != None)
        for collectionName in collectionNames:
            #hList = self.reader.getAllValues(tag)
            if(needSkip and collectionName == start_collection_name):
                needSkip = False
            if(needSkip):
                continue
            index += 1
            self.logger.logInfo("[" + str(index) + "]collection_name " + collectionName)
            rlt = re.search(G_CONST_DB_DETAIL_TABLE + "_(\d+)_(\w{2})", collectionName)
            if(rlt != None):
                (stock_number, tag) = rlt.groups()
                #self.logger.logInfo("fetching " + stock_number + "." + tag)
                stag = "cn"
                if((stock_number == "000001" and tag == "sh") or 
                   (stock_number == "399001" and tag == "sz")):
                    stag = "zs"
                self.update_one_stock_data(stock_number, tag, stag, is_force)
                #try:

                #except Exception as e:    
                    #print e
    def update_one_stock_data(self, stock_number, tag, stag = "cn", is_force = False):        
        start_time = None
        last_raw_data = None
        if(not is_force):
            last_raw_data = self.get_latest_raw_data(stock_number, tag)
        if(last_raw_data != None):
            start_time = last_raw_data[G_CONST_DATE_FIELD_NAME]
        data = self.download_data(stock_number, tag, start_time, tails = stag)
        if(data == None):
            self.logger.logInfo("no data for " + stock_number + "." + tag)
            return
        #self.print_data(data)
        self.update_history_data_to_db(stock_number, tag, data, is_force)  
              
    def download_data(self, stock_number, tag, start_time = None, end_time = None, tails = None):
        url = None
        if(self.download_type == "sohu"):
            url = self._get_url_for_sohu(stock_number, tag, start_time, end_time, tails)
        elif(self.download_type == "yahoo"):
            url = self._get_url_for_yahoo(stock_number, tag)
        self.logger.logInfo("url:" + url)
        values = self.downloader.get_values(url)
        data = self.add_ratio_in_data(stock_number, tag, values)
        return data
    def print_data(self, data, line_count = 100, fields = []):
        db_helper = DBHelper()
        db_helper.print_values(data, line_count, fields)
    def _get_url_for_yahoo(self, stock_number, tag):
        prefix_url = G_CONST_YAHOO_HISTORY_CSV_URL
        url = prefix_url + stock_number + "." + tag
        return url
    
    def _get_url_for_sohu(self, stock_number, tag, start_time = None, end_time = None, tails=None):
        prefix_url = G_CONST_SOHU_HISTORY_CSV_URL
        timeFormat = "%Y%m%d"
        start_time_str = "19991110"
        today = datetime.date.today()
        end_time_str = today.strftime(timeFormat)        
        if(tails != None):
            prefix_url += tails + "_"
        else:
            prefix_url += "cn" + "_"
        if(start_time != None):
            if(type(start_time) == datetime.datetime):
                start_time_str = start_time.strftime(timeFormat)
            else:
                start_time_str = start_time
        if(end_time != None):
            if(type(end_time) == datetime.date):
                end_time_str = end_time.strftime(timeFormat)
            else:
                end_time_str = end_time
        time_range = "&start=" + start_time_str + "&end=" + end_time_str
        url = prefix_url + stock_number + time_range
        return url    
    def get_downloader_instance(self, download_type):
        downloader = None
        if(download_type == "sohu"):
            downloader = JsonDownloader()
        elif(download_type == "yahoo"):
            downloader = CsvDownloader
        return downloader
    
    def add_ratio_in_data(self, stock_number, tag, values):
        if(values == None):
            return values
        last_day_ref = None
        last_week_ref = self.get_latest_raw_data(stock_number, tag, {G_CONST_WEEK_RATIO_FIELD_NAME:{"$ne":None}})
        last_month_ref = self.get_latest_raw_data(stock_number, tag, {G_CONST_MONTH_RATIO_FIELD_NAME:{"$ne":None}})
        last_quater_ref = self.get_latest_raw_data(stock_number, tag, {G_CONST_QUATER_RATIO_FIELD_NAME:{"$ne":None}})
        #print(last_week_ref)
        values.reverse()
        count_index = 0
        preAdjClose = None
        for date_info in values:            
            date_info[G_CONST_DAILY_RATIO_FIELD_NAME] = None
            '''
            date_info[G_CONST_WEEK_RATIO_FIELD_NAME] = None
            date_info[G_CONST_MONTH_RATIO_FIELD_NAME] = None
            date_info[G_CONST_QUATER_RATIO_FIELD_NAME] = None
            '''
            date_info[G_CONST_ADJ_CLOSE_FIELD_NAME] = None
            
            count_index += 1                                  
            if(self.is_time_in_same_range(last_day_ref, date_info, "day")):
                if(last_day_ref != None):                    
                    self.set_ratio(date_info, last_day_ref, G_CONST_DAILY_RATIO_FIELD_NAME) 
            '''                
            if(self.is_time_in_same_range(last_day_ref, date_info, "week")):
                #print("current week is " + str(w_count) + "; previous week is " + str( current_week['number']))
                if(last_week_ref != None):
                    self.set_ratio(last_day_ref, last_week_ref, G_CONST_WEEK_RATIO_FIELD_NAME)  
                last_week_ref = last_day_ref                 
            if(self.is_time_in_same_range(last_day_ref, date_info, "month")):
                if(last_month_ref != None):
                    self.set_ratio(last_day_ref, last_month_ref, G_CONST_MONTH_RATIO_FIELD_NAME) 
                last_month_ref = last_day_ref  
            if(self.is_time_in_same_range(last_quater_ref, date_info, "quater")):
                if(last_quater_ref != None):
                    self.set_ratio(last_day_ref, last_quater_ref, G_CONST_QUATER_RATIO_FIELD_NAME)
                last_month_ref = last_day_ref 
            '''
            if(date_info.has_key(G_CONST_ADJ_RATE_FIELD_NAME)):                    
                if(preAdjClose == None or preAdjClose == 0):
                    date_info[G_CONST_ADJ_CLOSE_FIELD_NAME] = date_info[G_CONST_ClOSE_FIELD_NAME] 
                else:                     
                    changeRate = date_info[G_CONST_ADJ_RATE_FIELD_NAME]
                    date_info[G_CONST_ADJ_CLOSE_FIELD_NAME] = preAdjClose * (1 + changeRate)                    
                preAdjClose = date_info[G_CONST_ADJ_CLOSE_FIELD_NAME]                      
            last_day_ref = date_info   
        values.reverse() 
        return values 
    def set_avg(self, date_info, total_value, day_count, fieldname):
        avg_value = total_value/day_count
        date_info[fieldname] = avg_value
    def set_ratio(self, cur_date_info, last_date_info, field_name):
        last_closed_value = last_date_info[G_CONST_ClOSE_FIELD_NAME]
        current_close_value = cur_date_info[G_CONST_ClOSE_FIELD_NAME]
        if(last_closed_value == 0):
            self.logger.logInfo("value is " + str(last_closed_value) + ", skip it")
            cur_date_info[field_name] = None
        else:
            month_ratio = (current_close_value - last_closed_value)/last_closed_value
            cur_date_info[field_name] = month_ratio         
    def is_time_in_same_range(self, date_info_x, date_info_y, range_name): 
        if(date_info_x == None and date_info_y == None):
            return True
        elif(date_info_x == None or date_info_y == None):
            return False
        date_x = date_info_x[G_CONST_DATE_FIELD_NAME]
        date_y = date_info_y[G_CONST_DATE_FIELD_NAME]
        is_same = False
        if(range_name == "day"):
            x_day = date_x.timetuple().tm_yday
            y_day = date_y.timetuple().tm_yday
            is_same = y_day - x_day
        elif(range_name == "week"):
            x_week = date_x.isocalendar()[1]
            y_week = date_y.isocalendar()[1]
            is_same = y_week - x_week
        elif(range_name == "month"):
            x_month = date_x.month
            y_month = date_y.month 
            is_same = y_month - x_month
        elif(range_name == "quater"):
            x_quater = math.ceil(date_x.month/3)
            y_quater = math.ceil(date_y.month/3)
            is_same = y_quater - x_quater
        return is_same               
    def update_history_data_to_db(self, stock_number, tag, data, is_force = False):
        dbConnection =  MogoDBHandler.get_stock_db_handler(db_server = G_CONST_DB_SERVER, 
                                      db_port= G_CONST_DB_PORT, 
                                      db_instance= G_CONST_DB_INSTANCE_NAME, 
                                      stock_number = stock_number,
                                      stock_tag = tag
                                      
                                      )
        if(is_force):
            dbConnection.removeAllData()
        msg = ""
        try:
            for date_values in data:
                msg = date_values[G_CONST_DATE_FIELD_NAME].strftime(G_CONST_TIME_FORMATE)
                dbConnection.saveSingleValue(date_values)
        except pymongo.errors.DuplicateKeyError as e:
            self.logger.logInfo(str(e) + " for " + msg)
    def update_history_data_to_db_force(self, stock_number, tag, data):
        dbConnection =  MogoDBHandler.get_stock_db_handler(db_server = G_CONST_DB_SERVER, 
                                      db_port= G_CONST_DB_PORT, 
                                      db_instance= G_CONST_DB_INSTANCE_NAME, 
                                      stock_number = stock_number,
                                      stock_tag = tag,
                                      )
        #dbConnection.removeAllData()
        msg = ""
        try:
            for date_values in data:
                msg = date_values[G_CONST_DATE_FIELD_NAME].strftime(G_CONST_TIME_FORMATE)
                dbConnection.saveSingleValue(date_values)
        except pymongo.errors.DuplicateKeyError as e:
            self.logger.logInfo(str(e) + " for " + msg)         
                
    def get_latest_raw_data(self, stock_number, tag, conditions = {}):
        dbConnection =  MogoDBHandler.get_stock_db_handler(db_server = G_CONST_DB_SERVER, 
                                      db_port= G_CONST_DB_PORT, 
                                      db_instance= G_CONST_DB_INSTANCE_NAME, 
                                      stock_number = stock_number,
                                      stock_tag = tag,
                                      )
        lastValues = dbConnection.getLatestValue(conditions = conditions)  
        lastValue = None
        if(len(lastValues) > 0):
            lastValue = lastValues[0]
        return lastValue   
           
        
if __name__ == '__main__': 
    logger = Logger()
    logger.enableScreenOutput()
    fetcher = DataFecher("sohu", logger)
    stock_number = "600653"
    tag = "sh"
    #fetcher.update_one_stock_data(stock_number, tag)
    #data = fetcher.download_data(stock_number, tag, tails = "zs")
    #fetcher.update_history_data_to_db(stock_number, tag, data)
    #data.reverse()
    #fetcher.print_data(data, 50, [G_CONST_DATE_FIELD_NAME, G_CONST_DAILY_RATIO_FIELD_NAME])
    
    fetcher.update_all_data(start_collection_name=None)
    #stock_number = "000001"
    #tag = "sz"history_table_163821_sz
    #values = fetcher.download_data_from_remote(stock_number, tag)
    #time = "20160822"
    #fetcher.update_all_data(time, time)
    #fetcher.update_history_data_to_db(stock_number, tag, values)
    #fetcher.update_history_data_to_db(stock_number, tag, values)
    #fetcher.csv_downloader.print_values(values, 100, [G_CONST_DATE_FIELD_NAME, G_CONST_MONTH_RATIO_FIELD_NAME ])
    pass       