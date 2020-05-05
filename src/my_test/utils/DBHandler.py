# -*- coding: utf-8 -*-

################################################################################
# This is the confidential unpublished intellectual property of EMC Corporation,
# and includes without limitation exclusive copyright and trade secret rights
# of EMC throughout the world.
################################################################################

import pymongo
from pymongo import MongoClient
from Assist import Logger
from time import strptime, mktime
from datetime import datetime
from my_test.conf.Configuration import G_CONST_DB_DETAIL_TABLE, G_CONST_DATE_FIELD_NAME
from my_test.utils.Assist import DBHelper, UnitHelper

class MogoDBSetHandler():
    def __init__(self, db_server, db_instance, db_port = 27017, logger = None):
        self.logger = logger
        if(not self.logger):
            self.logger = Logger()        
        self.db_server = db_server
        self.db_port = int(db_port)
        self.db_instance = db_instance
        self.client = self.getClientConnection(db_server)
        if(self.client == None):
            self.logger.logInfo("Can't connect the db server, skip to create it.")
            return
        self.db = self.client[db_instance]
    def getClientConnection(self, dbServer):
        client = None
        try:
            client = MongoClient(dbServer, self.db_port)
        except Exception as e:
            self.logger.logInfo(str(e))
        return client        
    def get_all_collections(self):
        collections = []
        for collection in self.db.collection_names(False):
            collections.append(collection)
        return collections
    
    def get_db_handler(self, collection_name):
        db_handler =  MogoDBHandler(db_server = self.db_server, 
                                    db_port = self.db_port, 
                                    db_instance = self.db_instance, 
                                    collection_name = collection_name, 
                                    logger = self.logger)  
        return db_handler          

class MogoDBHandler():
    def __init__(self, db_server,  db_instance, collection_name, db_port = 27017, logger=None):
        self.logger = logger
        if(not self.logger):
            self.logger = Logger()        
        self.db_server = db_server
        self.db_port = int(db_port)
        self.client = self.getClientConnection(db_server)
        if(self.client == None):
            self.logger.logInfo("Can't connect the db server, skip to create it.")
            return
        self.db = self.client[db_instance]
        self.db_collection = self.db[collection_name]
        self.collection_name = collection_name
    @classmethod
    def get_stock_db_handler(cls, db_server,  db_instance, stock_number, stock_tag, db_port = 27017, logger=None):
        collection_name = G_CONST_DB_DETAIL_TABLE + "_" + stock_number + "_" + stock_tag
        return cls(db_server = db_server, db_port = db_port,  db_instance = db_instance, collection_name = collection_name, logger = logger)        
    def getClientConnection(self, dbServer):
        client = None
        try:
            client = MongoClient(dbServer, self.db_port)
        except Exception as e:
            self.logger.logInfo(str(e))
        return client
    def removeAllData(self):
        self.db_collection.drop()
    '''
      This subroutine is to read the csv file with timestamp and also may define
      the max points 
      The input value should be following:
      [
        {'datetime': date1}, {'prop1':"20"}, {"prop2":"30"},...
      ]
    '''    
    def saveSingleValue(self, h_value):
        #self.logger.logInfo("Saving value in to " + self.collection_name)           
        self.db_collection.insert_one(h_value)
        #self.logger.logInfo("Succeeded to save value in to " + self.collection_name)  
                                    
    def updateSingleValue(self, key, h_value):
        self.db_collection.update_one({"__id":key},{"$set":h_value})
    '''      
    def saveValues(self, values):

        if(self.client == None):
            self.logger.logInfo("Can't connect the db server, skip to create it.")
            return       
        
        
        self.db_collection.insert_many(values)
    '''    
    def getLatestValue(self, count = 1, conditions = {}):
        values = []
        if(self.db_collection.count() > 0):
                       
            dbCursor = self.db_collection.find(conditions).sort(G_CONST_DATE_FIELD_NAME, pymongo.DESCENDING).limit(count)
            #count = dbCursor.count()
            totalCount = dbCursor.count(True)
            index = 0
            #print("total count :{}, count: {}".format(totalCount, count))
            #DBHelper().print_values(dbCursor)
            try:
                while(index < count and index < totalCount):
                    #print("index is :{}".format(index))                
                    values.append(dbCursor[index])
                    index = index + 1 
            except IndexError as e:
                self.logger.logInfo("Index Error, skip it")           
        return values
    def getValues(self, start_time=None, end_time=None, sortType = pymongo.DESCENDING):
        fieldname = G_CONST_DATE_FIELD_NAME
        if(self.client == None):
            self.logger.logInfo("Can't connect the db server, skip to create it.")
            return None        
        condition = None
        utitHelper = UnitHelper()
        start = utitHelper.convertToDatetime(start_time)
        end = utitHelper.convertToDatetime(end_time)
        if(start and end):
            condition = {fieldname : {"$gte": start, "$lte":end}}
        elif(start):
            condition = {fieldname : {"$gte": start}}
        elif(end):
            condition = {fieldname : {"$lte": end}}
        values = None
        if(condition != None):
            #print(condition)
            values = self.db_collection.find(condition).sort(fieldname, sortType)
        elif(fieldname != None):
            values = self.db_collection.find().sort(fieldname, sortType)
        else:
            values = self.db_collection.find() 
        data = []
        count = values.count()
        index = 0
        while(index < count):
            data.append(values[index])
            index = index + 1
        return data 
    def convert_to_datetime(self, date_str):
        if(not (date_str)  or type(date_str) != str):
            return date_str
        formats = ["%Y-%m-%d", "%Y%m%d"]
        date_value = None
        for date_format in formats:
            date_value = self._try_convert_to_date_by_format(date_str, date_format)
            if(date_value):
                break
        if(date_value == None):
            date_value = date_str
        return date_value
    def _try_convert_to_date_by_format(self, date_value, date_format):
        rlt = None
        try:
            rlt = datetime.fromtimestamp(mktime(strptime(date_value, date_format)))
            return rlt
        except Exception as e:
            return None
        
        
                  

            
if __name__ == '__main__': 
    log = Logger()
    log.enableScreenOutput()
    '''
    timeformat = "%Y-%m-%d %H:%M:%S" 
    dbHandler = MogoDBHandler(db_server = "127.0.0.1", db_port=27017, db_instance="bob_test", collections="bob_table", logger = log)
    
    dbHandler.removeAllData()    
    for i in xrange(10):
        date = "2016-01-" + str(int(i) + 1) + " " + "12:11:01"
        log.logInfo(date)
        dt = datetime.fromtimestamp(mktime(strptime(date, timeformat)))
        value = [ {"timestamp":dt},{"name":"BobYan" + str(i)},{"location":"Chengdu"},{"Age":37}]    
        dbHandler.saveValue(value)
    
    starttime = datetime.fromtimestamp(mktime(strptime("2016-01-03 12:11:01", timeformat)))
    endtime = datetime.fromtimestamp(mktime(strptime("2016-01-07 00:00:00", timeformat)))
    values = dbHandler.getLineChartDataWithTimestamp("timestamp",starttime, endtime )
    
    #values = dbHandler.getValues()
    for value in values:
        log.logInfo(value)
    '''
    dbSet = MogoDBSetHandler(db_server = "127.0.0.1", db_instance = "STOCK", logger = log)
    collections = dbSet.get_all_collections()
    for collection in collections:
        log.logInfo("collection_name is " + collection) 