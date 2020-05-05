
from my_test.index.IndexBoll import IndexBoll
from my_test.index.IndexKDJ import IndexKDJ
from my_test.utils.DBHandler import MogoDBSetHandler
from my_test.conf.Configuration import G_CONST_DB_SERVER, G_CONST_DB_PORT,\
    G_CONST_DB_INSTANCE_NAME, G_CONST_STUDY_GOOD_VALUE, G_CONST_DATE_FIELD_NAME,\
    G_CONST_STUDY_CORRECT, G_CONST_STUDY_DEAL_DAYS, G_CONST_STUDY_REAL_RATE,\
    G_CONST_STUDY_DEAL_DATE, G_CONST_STUDY_DEAL_END_VALUE,\
    G_CONST_STUDY_DEAL_START_VALUE, G_CONST_ADJ_RATE_FIELD_NAME,\
    G_CONST_ADJ_CLOSE_FIELD_NAME
from my_test.utils.Assist import Logger, DBHelper
from my_test.index.IndexRSI import IndexRSI
from my_test.index.IndexMACD import IndexMACD

    
class IndexManager(object):
    def __init__(self, logger):
        self.logger = logger
        self.adjust_params = {
                                "normal" : {
                                              "adj" : {"monitor_count" : 2},
                                              "boll" : {"monitor_count" : 3},
                                               "macd" : {"monitor_count" : 5}
                                            },
                                "small" : {
                                              "adj" : {"monitor_count" : 2,"period":5,"study_good_ratio" : 0.02,"study_good_count" : 5},
                                              "boll" : {"monitor_count" : 3,"period" : 10,"study_good_ratio" : 0.02,"study_good_count" : 5},
                                              "macd" : {"monitor_count" : 5,"macd_days" : [5, 10, 5],"study_good_ratio" : 0.02,"study_good_count" : 5}
                                            }                              
                              }
        pass
    
    def update_all_index(self, start_collection_name = None):
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
            kdjIndex = IndexRSI(collectionName)                
            kdjIndex.update_data()
    def find_good_index(self, start_collection_name = None, seek_number = 10):
        #tags = ['sh','sz']
        dbSetHandler = MogoDBSetHandler(db_server = G_CONST_DB_SERVER, 
                                      db_port= G_CONST_DB_PORT, 
                                      db_instance= G_CONST_DB_INSTANCE_NAME)
        collectionNames = dbSetHandler.get_all_collections()
        collectionNames.sort()
        index = 0
        needSkip = (start_collection_name != None)
        goodIndexes = []
        #endValue = "2016-09-07"
        duration = ("2018-01-05", )
        #duration = None
        monitorCount = 5
        for collectionName in collectionNames:
            #hList = self.reader.getAllValues(tag)
            if(needSkip and collectionName == start_collection_name):
                needSkip = False
            if(needSkip):
                continue
            index += 1
            self.logger.logInfo("[" + str(index) + "]collection_name " + collectionName)
            kdjIndex = IndexKDJ(collectionName, duration = duration, monitor_count = monitorCount)
            macdIndex = IndexMACD(collectionName, duration = duration, monitor_count = monitorCount) 
            boolIndex = IndexBoll(collectionName, duration = duration, monitor_count = monitorCount)             
            if(boolIndex.is_good_by()):
                dateValue = boolIndex.get_latest_date()
                self.logger.logInfo("boll good is {}".format(dateValue))
                #if(kdjIndex.is_good_by()):
                    #self.logger.logInfo("---kdj good is {}".format(dateValue))
                    #if(macdIndex.is_good_by()):
                        #self.logger.logInfo("------macd good is {}".format(dateValue))
                goodIndexes.append(collectionName)  
                self.logger.logInfo("-------------[{}] good collection_name is {}".format(dateValue, collectionName) )
                if(len(goodIndexes) > seek_number):
                    break
            
        return  goodIndexes     
    def study_good_deep(self, start_collection_name = None):
        dbSetHandler = MogoDBSetHandler(db_server = G_CONST_DB_SERVER, 
                                      db_port= G_CONST_DB_PORT, 
                                      db_instance= G_CONST_DB_INSTANCE_NAME)
        collectionNames = dbSetHandler.get_all_collections()
        collectionNames.sort()
        index = 0
        needSkip = (start_collection_name != None)
        goodIndexes = []
        endValue = "2016-10-11"
        duration = ("2015-11-05", endValue)
        monitorCount = 5
        study_count = 20
        study_good_ratio = 0.03
        dbHelper = DBHelper()
        chooseParam = self.adjust_params["normal"]
        
        for collectionName in collectionNames:
            #hList = self.reader.getAllValues(tag)
            if(needSkip and collectionName == start_collection_name):
                needSkip = False
            if(needSkip):
                continue
            index += 1
            self.logger.logInfo("[" + str(index) + "]collection_name " + collectionName)  
                  
            #if(index > 100):
                #break
            realGoodTables = self.study_one_good_deep(collectionName)
            if(len(realGoodTables) > 0):
                goodIndexes.append([collectionName, realGoodTables])
        return  goodIndexes        
    
    def study_one_good_deep(self, start_collection_name = None):
        collectionName = start_collection_name
        endValue = "2016-11-17"
        duration = ("2016-06-05", endValue)
        monitorCount = 5
        study_count = 20
        dbHelper = DBHelper()
        study_good_ratio = 0.03
        chooseParam = self.adjust_params["normal"]
        
               
        bollIndex = IndexBoll(collectionName, duration = duration, monitor_count = 2 , study_count = study_count, study_good_ratio = study_good_ratio) 
        goodTables = bollIndex.study_good()
        realGoodTables = []
        #dbHelper.print_values(goodTables, 20,[G_CONST_DATE_FIELD_NAME, G_CONST_STUDY_DEAL_DAYS, G_CONST_STUDY_GOOD_VALUE, G_CONST_STUDY_REAL_RATE, G_CONST_STUDY_CORRECT],False)
        if(len(goodTables) > 0):                
            for goodPoint in goodTables:
                dateValue = goodPoint[G_CONST_DATE_FIELD_NAME]
                kdjIndex = IndexKDJ(collectionName, verify_date = dateValue, monitor_count = 3, study_count = study_count, study_good_ratio = study_good_ratio)
                #macdIndex = IndexMACD(collectionName, verify_date = dateValue, monitor_count = monitorCount, study_count = study_count)
                #rsiIndex = IndexRSI(collectionName, verify_date = dateValue, monitor_count = monitorCount, study_count = study_count)
                #self.logger.logInfo("kdj good is {}".format(dateValue)) 
                #if(macdIndex.is_good_by()):
                    #self.logger.logInfo("kdj good is {}".format(dateValue)) 
                if(True or kdjIndex.is_good_by()):
                    #self.logger.logInfo("---macd good is {}".format(dateValue))
                        #if(rsiIndex.is_good_by()):
                            #self.logger.logInfo("------macd good is {}".format(dateValue))
                        #self.logger.logInfo("-------------[{}] good collection_name is {}".format(dateValue, collectionName) )
                    realGoodTables.append(goodPoint)
            if(len(realGoodTables) > 0):
                self.logger.logInfo("-------------[] good collection_name is {}".format(collectionName) )
                
                dbHelper.print_values(realGoodTables, 20,[G_CONST_DATE_FIELD_NAME, G_CONST_STUDY_DEAL_DATE, G_CONST_STUDY_DEAL_DAYS,  G_CONST_STUDY_REAL_RATE, G_CONST_STUDY_CORRECT,G_CONST_STUDY_DEAL_START_VALUE,G_CONST_STUDY_DEAL_END_VALUE],False)
        return realGoodTables     

if __name__ == '__main__': 
    logger = Logger()   
    dbHelper = DBHelper()    
    indexManager = IndexManager(logger)
    
    #indexManager.update_all_index()
    indexes = indexManager.find_good_index(seek_number = 20)
    print(indexes)
    #goodIndex = indexManager.study_good_deep("history_table_002608_sz")
    '''
    for hIndex in goodIndex:
        indexName = hIndex[0]
        values = hIndex[1]
        logger.logInfo("good collection is ".format(indexName))
        dbHelper.print_values(values, 20,[G_CONST_DATE_FIELD_NAME, G_CONST_STUDY_DEAL_DATE, G_CONST_STUDY_DEAL_DAYS,  G_CONST_STUDY_REAL_RATE, G_CONST_STUDY_CORRECT,G_CONST_STUDY_DEAL_START_VALUE,G_CONST_STUDY_DEAL_END_VALUE],False)
    #590
    '''