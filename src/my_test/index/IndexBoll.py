'''
Created on Aug 24, 2016

@author: yanb
'''
from math import sqrt
from my_test.index.Index import Index
from my_test.conf.Configuration import G_CONST_DATE_FIELD_NAME,\
    G_CONST_CALCULATION_BOLL_MA, G_CONST_CALCULATION_BOLL_MD,\
    G_CONST_CALCULATION_BOLL_UP, G_CONST_CALCULATION_BOLL_DOWN,\
    G_CONST_DB_DETAIL_TABLE, G_CONST_CALCULATION_BOLL_MA_MD,\
    G_CONST_STUDY_CORRECT, G_CONST_STUDY_DEAL_DAYS, G_CONST_STUDY_GOOD_VALUE,\
    G_CONST_STUDY_REAL_RATE, G_CONST_DAILY_RATIO_FIELD_NAME,\
    G_CONST_HIGH_FIELD_NAME,G_CONST_OPEN_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME
from my_test.conf.Configuration import G_CONST_HIGH_FIELD_NAME, G_CONST_LOW_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME




class IndexBoll(Index):
    '''
    classdocs
    '''


    def __init__(self, collection_name, period = 20, p_ratio = 2,monitor_count = 30, duration = None, study_count = 200, verify_date = None, verify_count = 150, study_good_ratio = 0.05, study_good_count = 10):
        super(IndexBoll, self).__init__(collection_name, monitor_count, duration, study_count, verify_date, verify_count, study_good_ratio, study_good_count)
        self.period = period
        self.p_ratio = p_ratio    
    
    def get_index_db_name(self):
        db_instance_name = "STOCK_INDEX_BOLL_" + str(self.period)    
        return db_instance_name
     
    def get_index_data(self, raw_data):
        #DBHelper().print_values(raw_data)
        data = self.calc_boll(self.period, raw_data)
        return data  

    def get_previous_max_days(self):        
        return self.period + 1  
        


    def calc_boll_md(self, data1, data2):
        totalValue = 0
        ma = data2[0][G_CONST_CALCULATION_BOLL_MA]
        count = len(data1)
        for dateInfo in data1:
            closeValue = dateInfo[G_CONST_ClOSE_FIELD_NAME]
            totalValue += (closeValue - ma) * (closeValue - ma)
        value = float(totalValue)/count
        #self.logger.logInfo("value is " + str(value))
        mdValue = sqrt(value)
        return sqrt(mdValue)
    
    def calc_boll_up(self,ma_value, md_value):
        return ma_value + self.p_ratio * md_value
    def calc_boll_down(self,ma_value, md_value):
        return ma_value - self.p_ratio * md_value    
    def calc_boll(self, period, data):
        #self.logger.logInfo("calculate boll")
        data_with_ma = self.index_calculator.maIndex(data, G_CONST_ClOSE_FIELD_NAME, self.period, G_CONST_CALCULATION_BOLL_MA) 
        #self.db_helper.print_values(data_with_ma, 20, [G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_BOLL_MA], False)     
                      
        data_with_md = self.index_calculator.complexCalculateIndex(data, period, G_CONST_CALCULATION_BOLL_MD, data_with_ma, 1, self.calc_boll_md)  
        #data_wint_mamd = self.index_calculator.maIndex(data_with_md, G_CONST_CALCULATION_BOLL_MD, self.period, G_CONST_CALCULATION_BOLL_MA_MD)
        #self.logger.logInfo("----------------MD----------------")
        #self.db_helper.print_values(data_with_md, 70, [G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_BOLL_MD], False)
        data_with_up = self.index_calculator.simpleCalculateIndex(data_with_ma, G_CONST_CALCULATION_BOLL_MA, data_with_md
                                                                  , G_CONST_CALCULATION_BOLL_MD, G_CONST_CALCULATION_BOLL_UP, self.calc_boll_up)
        
       # self.logger.logInfo("----------------UP----------------")
        #self.db_helper.print_values(data_with_up, 20, [G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_BOLL_UP], False)        
        data_with_down = self.index_calculator.simpleCalculateIndex(data_with_ma, G_CONST_CALCULATION_BOLL_MA, data_with_md
                                                                  , G_CONST_CALCULATION_BOLL_MD, G_CONST_CALCULATION_BOLL_DOWN, self.calc_boll_down)
        #self.logger.logInfo("----------------DOWN----------------")
        #self.db_helper.print_values(data_with_down, 20, [G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_BOLL_DOWN], False)                   
        data_with_boll = self.index_calculator.mergeIndex(data_with_ma, data_with_up, data_with_down, data, data_with_md)
        #self.db_helper.print_values(data_with_boll, 200, [G_CONST_DATE_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME, G_CONST_CALCULATION_BOLL_UP, G_CONST_CALCULATION_BOLL_MA,G_CONST_CALCULATION_BOLL_DOWN, G_CONST_LOW_FIELD_NAME], False)
        return data_with_boll

    def is_good_one(self, index_data):
        indexData = index_data
        if(len(indexData) == 0):
            return False
        dateInfo = indexData[0]
        
        #is_good = 3
        #index = 0
        is_good = 0
        #if(self.is_quick_open_and_up(indexData, 0) ):
            #is_good = 3
        if(self.is_quick_open(indexData, 0) ):
            is_good = 3
        
        return is_good
        '''
        if(self.is_not_on_bottom(index_data, 0)):
            is_good = 0
        if(self.is_in_narrow_band(index_data, 0)):
            is_good = 0
        if(self.is_long_down_trend(index_data, 0)):
            is_good = 0
        return is_good   
        '''     
        '''
        is_good = 0
        index = 0
        
        monitorCount = self.monitor_count
        if(not dateInfo.has_key(G_CONST_CALCULATION_BOLL_MD)):
            #self.db_helper.print_values(index_data, 2, [G_CONST_DATE_FIELD_NAME, G_CONST_CALCULATION_BOLL_MA, G_CONST_CALCULATION_BOLL_MD, G_CONST_CALCULATION_BOLL_MA_MD])
            return is_good
        if(self.is_big_ball_before(indexData, 0)):
            return is_good
        if(self.is_in_narrow_band(indexData, 0)):
            return is_good
        if(self.is_long_down_trend(indexData, 0)):
            return is_good
        is_good = 3
        return is_good
        
        for dateInfo in indexData:
            if(monitorCount <= index):
                break                    
            if(not dateInfo.has_key(G_CONST_CALCULATION_BOLL_DOWN)):
                continue 
            is_good = self.is_good_open_increasing(indexData, index)
            if(is_good >= 3):
                break
            is_good = self.is_good_start_change(indexData, index)
            if(is_good >= 3):
                break
            is_good = self.is_good_in_progress(indexData, index)
            if(is_good >= 3):
                break            
                           
        return is_good    
        '''

    def is_good_open_increasing(self, index_data, index):
        monitorCount = 30
        skipCount = 3
        dateInfo = index_data[index]
        maValue = dateInfo[G_CONST_CALCULATION_BOLL_MA] 
        mdValue = dateInfo[G_CONST_CALCULATION_BOLL_MD]        
        isGood = 0
        if(4 * mdValue/maValue > 0.1):
            isGood = 3
            for j in range(1, skipCount):
                nextDateinfo = index_data[index + j]
                if(maValue < nextDateinfo[G_CONST_CALCULATION_BOLL_MA]):
                    isGood = 0
                    break                
            #if the trend is up
            if(isGood >= 3):
                for i in (skipCount, monitorCount):
                    curDateInfo = index_data[index + i]
                    curmdValue = curDateInfo[G_CONST_CALCULATION_BOLL_MD]
                    if(curmdValue * 4/maValue > 0.1):
                        isGood = 0
                        break
        return isGood
    def is_good_start_change(self, index_data, index):
        monitorCount = 30
        isChangeMointorCount = 2
        dateInfo = index_data[index]
        maValue = dateInfo[G_CONST_CALCULATION_BOLL_MA] 
        mdValue = dateInfo[G_CONST_CALCULATION_BOLL_MD] 
        closeValue = dateInfo[G_CONST_ClOSE_FIELD_NAME] 
        isGoodOne = False
        is_good = 0
        isChanged = False
        previousClose = closeValue
        previousMd = mdValue
        prevousMa = maValue
        isOpening = False
        isDowning = False     
        #self.logger.logInfo("date={},up={}, close = {},down = {}".format(dateValue,upValue, lowValue, downValue))
        for mIndex in range(1, monitorCount):
            if(mIndex + index >= len(index_data)):
                break
            mDateInfo = index_data[index + mIndex]
            cdateValue = mDateInfo[G_CONST_DATE_FIELD_NAME]
            cLowValue = mDateInfo[G_CONST_LOW_FIELD_NAME]
            cUpValue = mDateInfo[G_CONST_CALCULATION_BOLL_UP]
            cDownValue = mDateInfo[G_CONST_CALCULATION_BOLL_DOWN] 
            cMaValue = mDateInfo[G_CONST_CALCULATION_BOLL_MA] 
            cCloseValue = mDateInfo[G_CONST_ClOSE_FIELD_NAME] 
            cMdValue = mDateInfo[G_CONST_CALCULATION_BOLL_MD]            
            if((cUpValue - cDownValue)/cMaValue < 0.1):
                #self.logger.logInfo("date={},up={}, close = {},down = {}".format(dateValue,upValue, lowValue, downValue))
                break
            if(cLowValue > cMaValue):
                break
            if(mIndex > isChangeMointorCount and not isChanged):
                break
            if(cCloseValue < previousClose):
                isChanged = True
            if(cMaValue < prevousMa):
                isDowning = True
            else:
                isDowning = False
            if(cMdValue < previousMd):
                isOpening = True
            else:
                isOpening = False            
            if(isChanged and (cLowValue - cDownValue)/cMaValue < 0.04 ):
                #self.logger.logInfo("Bottom far: date={},up={}, ma = {},down = {}, lowValue={}, closeValue={}".format(dateValue,upValue, maValue, downValue, lowValue, closeValue))
                if((not isDowning or not isOpening)):
                    isGoodOne = True
                else:
                    isGoodOne = False
                
                break                    
            prevousMa = cMaValue
            previousMd = cMdValue
            previousClose = cCloseValue
            
        if(isGoodOne):
            isShortUp = self.is_short_up(index_data, index)
            isAwaylseDown = self.is_long_down_trend(index_data, index)  
            if(isShortUp or isAwaylseDown):
                is_good = 0                          
            else:
                is_good = 3

           
        return is_good

    def is_good_in_progress(self, index_data, index):        
        monitorCount = 30
        skipCount = 3
        dateInfo = index_data[index]
        maValue = dateInfo[G_CONST_CALCULATION_BOLL_MA] 
        mdValue = dateInfo[G_CONST_CALCULATION_BOLL_MD] 
        closeValue = dateInfo[G_CONST_ClOSE_FIELD_NAME] 
        isGoodOne = False
        is_good = 0
        isChanged = False
        previousClose = closeValue
        previousMd = mdValue
        prevousMa = maValue
        isOpening = False
        isDowning = False                   
        #self.logger.logInfo("date={},up={}, close = {},down = {}".format(dateValue,upValue, lowValue, downValue))
        for mIndex in range(1, monitorCount):
            if(mIndex + index >= len(index_data)):
                break
            mDateInfo = index_data[index + mIndex]
            cdateValue = mDateInfo[G_CONST_DATE_FIELD_NAME]
            cLowValue = mDateInfo[G_CONST_LOW_FIELD_NAME]
            cUpValue = mDateInfo[G_CONST_CALCULATION_BOLL_UP]
            cDownValue = mDateInfo[G_CONST_CALCULATION_BOLL_DOWN] 
            cMaValue = mDateInfo[G_CONST_CALCULATION_BOLL_MA] 
            cCloseValue = mDateInfo[G_CONST_ClOSE_FIELD_NAME] 
            cMdValue = mDateInfo[G_CONST_CALCULATION_BOLL_MD]            
            if((cUpValue - cDownValue)/cMaValue < 0.8):
                #self.logger.logInfo("date={},up={}, close = {},down = {}".format(dateValue,upValue, lowValue, downValue))
                break
            if(cLowValue < cMaValue):
                break
            if(cCloseValue < previousClose):
                isChanged = True
            if(cMaValue > prevousMa):
                isDowning = True
            else:
                isDowning = False           
            if((cCloseValue - cUpValue)/cMaValue < 0.04 ):
                #self.logger.logInfo("Bottom far: date={},up={}, ma = {},down = {}, lowValue={}, closeValue={}".format(dateValue,upValue, maValue, downValue, lowValue, closeValue))
                if(isChanged and not isDowning):
                    isGoodOne = True
                else:
                    isGoodOne = False
                
                break                    
            prevousMa = cMaValue
            previousMd = cMdValue
            previousClose = cCloseValue
            
        if(isGoodOne):
            is_good = 3
        return is_good   
    def is_in_narrow_band(self, index_data, index):
        monitorCount = 50
        narrowMonitorCount = 1
        dateInfo = index_data[index]
        mdValue = dateInfo[G_CONST_CALCULATION_BOLL_MD] 
        maValue = dateInfo[G_CONST_CALCULATION_BOLL_MA]
        dateValue = dateInfo[G_CONST_DATE_FIELD_NAME]
        previousMadValue = mdValue      
        totalMdValue = mdValue 
        isNarrow = True
        mIndex = 0 
        upCount = 0
        
        if((mdValue * 2 * self.p_ratio)/maValue < 0.2):
            #self.logger.logInfo("{} in less than maValue".format(index_data[index][G_CONST_DATE_FIELD_NAME]))
            return isNarrow
        #else:
            #self.logger.logInfo("[{}]md={},ma={}".format(dateValue, mdValue, maValue))
        #self.logger.logInfo("date={},up={}, close = {},down = {}".format(dateValue,upValue, lowValue, downValue))
        for mIndex in range(1, monitorCount):
            if(mIndex + index >= len(index_data)):
                break         
            mDateInfo = index_data[index + mIndex] 
            if(not mDateInfo.has_key(G_CONST_CALCULATION_BOLL_MD)):
                return True
            cMdValue = mDateInfo[G_CONST_CALCULATION_BOLL_MD] 
            if(mdValue > cMdValue):
                upCount += 1
        if(float(upCount)/mIndex > 0.2) : 
            isNarrow = False
        return isNarrow
    def is_big_ball_before(self,index_data, index):
        monitorCount = 30
        isBigBall = False
        for mIndex in range(0, monitorCount):
            if(mIndex + index >= len(index_data)):
                break
            mDateInfo = index_data[index + mIndex]
            cDailyRate = mDateInfo[G_CONST_DAILY_RATIO_FIELD_NAME]
            if(cDailyRate < -0.3):
                isBigBall = True
                break
        return isBigBall               
                
    def is_long_down_trend(self,index_data, index):
        monitorCount = 30
        isAlwayseDown = True   
        #prevousMa = None  
        for mIndex in range(1, monitorCount):
            if(mIndex + index >= len(index_data)):
                break
            mDateInfo = index_data[index + mIndex]
            cLowValue = mDateInfo[G_CONST_LOW_FIELD_NAME]
            cMaValue = mDateInfo[G_CONST_CALCULATION_BOLL_MA]
            #if(prevousMa != None and cMaValue < prevousMa):
                #isAlwayseDown = False                  
                #break
            if(cLowValue > cMaValue):
                isAlwayseDown = False
                break
            #prevousMa = cMaValue
        return isAlwayseDown
    def is_short_up(self,index_data, index):
        monitorCount = 3
        startDateInfo = index_data[index]
        endDateInfo = index_data[index + monitorCount - 1]
        closeDiff = startDateInfo[G_CONST_ClOSE_FIELD_NAME] - endDateInfo[G_CONST_ClOSE_FIELD_NAME]
        mdValue = startDateInfo[G_CONST_CALCULATION_BOLL_MD]    
        isShortUp = False
        if(closeDiff/(4 * mdValue) > 0.4):
            isShortUp = True 
        return isShortUp   
    def is_not_on_bottom(self, index_data, index):
        dateInfo = index_data[index]                             
        cLowValue = dateInfo[G_CONST_LOW_FIELD_NAME]
        cDownValue = dateInfo[G_CONST_CALCULATION_BOLL_DOWN]
        if(cLowValue < cDownValue):
            return True
        else:
            return False
    def is_long_narrow_then_open_up(self, index_data, index):
        monitorCount = 30
        isStarted = False
        isLongNarrow = True
        for i in range(0, monitorCount):
            if(isStarted):
                if(not self.is_in_narrow_band(index_data, index + i)):
                    isLongNarrow = False
                    break
            elif(self.is_in_narrow_band(index_data, index + i) and i < 3):
                isStarted = True
        is_good = False
        if(isStarted and isLongNarrow):
            self.logger.logInfo("{} in in long narrow ma(0) = {}, ma(1) = {}".format(index_data[index][G_CONST_DATE_FIELD_NAME], index_data[index][G_CONST_CALCULATION_BOLL_MA], index_data[index + 1][G_CONST_CALCULATION_BOLL_MA]))
            
        if(isStarted and isLongNarrow and index_data[index][G_CONST_CALCULATION_BOLL_MA] > index_data[index + 1][G_CONST_CALCULATION_BOLL_MA]):
            is_good = True
        return is_good

    def is_second_up(self, index_data, index):
        monitorCount = 50
        continueIncrease = 0
        isQuickOpen = False
        if (not index_data[index].has_key(G_CONST_CALCULATION_BOLL_MD)):
            return False
        mdValue = index_data[index][G_CONST_CALCULATION_BOLL_MD]
        openRate = 0.08
        if (monitorCount > len(index_data)):
            monitorCount = len(index_data)
        is_cadidate_one = False
        is_possible_good_one = False
        is_good_one = False
        cadidate_ma_value = None
        monitor_range = 2
        for i in range(0, monitorCount):
            if (not index_data[index + i].has_key(G_CONST_CALCULATION_BOLL_MD)):
                break
            #curMdValue = index_data[index + i][G_CONST_CALCULATION_BOLL_MD]
            curOpenValue = index_data[index + i][G_CONST_OPEN_FIELD_NAME]
            curCloseValue = index_data[index + i][G_CONST_ClOSE_FIELD_NAME]
            curMaValue = index_data[index + i][G_CONST_CALCULATION_BOLL_MA]
            curHighMdValue = index_data[index + i][G_CONST_CALCULATION_BOLL_UP]
            curMD = index_data[index + i][G_CONST_CALCULATION_BOLL_MD]
            #if (  not is_possible_good_one and cadidate_ma_value and cadidate_ma_value > cadidate_ma_value + 4 * curMD):
                #break
            if(i < monitor_range and curMaValue < curCloseValue and curMaValue > curOpenValue):
                if is_cadidate_one:
                    is_possible_good_one = True
                else:
                    is_cadidate_one = True
                    cadidate_ma_value = curMaValue
                    is_good_one = True
                    break
            if (i >= monitor_range and not is_cadidate_one):
                break
            #if(is_possible_good_one and curMaValue > cadidate_ma_value + 4 * curMD):
                #is_good_one = True
        return is_good_one

    def is_quick_open(self, index_data, index):
        monitorCount = 30
        continueIncrease = 0
        isQuickOpen = False
        if (not index_data[index].has_key(G_CONST_CALCULATION_BOLL_MD)):
            return False
        mdValue = index_data[index][G_CONST_CALCULATION_BOLL_MD]
        openRate = 0.08
        if (monitorCount > len(index_data)):
            monitorCount = len(index_data)
        for i in range(0, monitorCount):
            if (not index_data[index + i].has_key(G_CONST_CALCULATION_BOLL_MD)):
                break
            #curMdValue = index_data[index + i][G_CONST_CALCULATION_BOLL_MD]
            curHighValue = index_data[index + i][G_CONST_HIGH_FIELD_NAME]
            curHighMdValue = index_data[index + i][G_CONST_CALCULATION_BOLL_UP]
            curMD = index_data[index + i][G_CONST_CALCULATION_BOLL_MD]
            if (curHighValue >= curHighMdValue  or (curHighMdValue - curHighValue) * 2 < curMD):
                continueIncrease += 1
            else:
                break
            #mdValue = curMdValue
        if (continueIncrease >= 2 and continueIncrease < 10):
            # self.logger.logInfo("{} in in quick open: {}, narrowCount:{}".format(index_data[index][G_CONST_DATE_FIELD_NAME], continueIncrease, continueNarrow))
            isQuickOpen = True

        return isQuickOpen

    def is_quick_open_and_up(self, index_data, index):
        monitorCount = 30
        continueIncrease = 0
        continueException = 0
        continueNarrow = 0
        isLongNarrow = False
        isQuickOpen = False
        is_good = False
        if(not index_data[index].has_key(G_CONST_CALCULATION_BOLL_MD)):
            return False
        mdValue = index_data[index][G_CONST_CALCULATION_BOLL_MD]
        firtMdValue = mdValue
        isNarrowStarted = False
        openRate = 0.08
        diffValue = 0
        if(monitorCount > len(index_data)):
            monitorCount = len(index_data)
        for i in range(1, monitorCount):
            if(not index_data[index + i].has_key(G_CONST_CALCULATION_BOLL_MD)):
                break
            curMdValue = index_data[index + i][G_CONST_CALCULATION_BOLL_MD]
            if(isNarrowStarted):
                maValue = index_data[index + i][G_CONST_CALCULATION_BOLL_MA]
                #if((abs(mdValue - curMdValue)/curMdValue) < openRate and (curMdValue * 2 * self.p_ratio)/maValue <= 0.1):
                if((curMdValue * 2 * self.p_ratio)/maValue <= 0.1):
                    continueNarrow += 1

                if(firtMdValue < curMdValue):
                    break

            elif(((mdValue - curMdValue)/curMdValue) < openRate):
                isNarrowStarted = True
            if(not isNarrowStarted and ((mdValue - curMdValue)/curMdValue) >= openRate):
                continueIncrease += 1
            if(isNarrowStarted):
                diffValue += (mdValue - curMdValue)
            if(isNarrowStarted and abs(diffValue)/curMdValue >  0.4):
                #self.logger.logInfo("{} in break in diff: {}, curMdValue:{}".format(index_data[index][G_CONST_DATE_FIELD_NAME], diffValue, curMdValue))
                break
            mdValue = curMdValue
        if(continueIncrease >= 1 and continueIncrease < 10):
            #self.logger.logInfo("{} in in quick open: {}, narrowCount:{}".format(index_data[index][G_CONST_DATE_FIELD_NAME], continueIncrease, continueNarrow))
            isQuickOpen = True
        if(continueNarrow >= 5):
            #self.logger.logInfo("{} in in long narrow: {}".format(index_data[index][G_CONST_DATE_FIELD_NAME], continueNarrow))
            isLongNarrow = True
        if(isQuickOpen and isLongNarrow and index_data[index][G_CONST_CALCULATION_BOLL_MA] > index_data[index + 1][G_CONST_CALCULATION_BOLL_MA]):
            is_good = True
        return is_good
    
    def is_new_quick_open(self, index_data, index):
        monitorCount = 20
        if(monitorCount > len(index_data)):
            monitorCount = len(index_data)
        upToTopCount = 0
        for i in range(1, monitorCount):
            if(not index_data[index + i].has_key(G_CONST_CALCULATION_BOLL_MD)):
                break            
            curBollUp = index_data[index + i][G_CONST_CALCULATION_BOLL_UP]
            hightValue = index_data[index + i][G_CONST_HIGH_FIELD_NAME]
            if hightValue >= curBollUp:
                upToTopCount += 1
        if upToTopCount >= 4:
            return True
        else:
            return False

    def is_long_down(self, index_data, index):
        monitorCount = 60
        downCount = 0
        preMaValue = 0
        isShowHead = False
        isStartChecking = False
        isBroken = False
        is_good = False
        if(monitorCount > len(index_data)):
           monitorCount = len(index_data)  
        for i in range(0, monitorCount):
            if(not index_data[i].has_key(G_CONST_CALCULATION_BOLL_MA)):
                break             
            maValue = index_data[i][G_CONST_CALCULATION_BOLL_MA]
            closeValue = index_data[index][G_CONST_ClOSE_FIELD_NAME]
            if(i == 0 and closeValue < maValue):
                break
            if(preMaValue == 0):
                preMaValue = maValue
                continue
            else:  
                if(not isStartChecking and maValue > preMaValue):
                    isStartChecking = True    
                if(not isStartChecking):
                    if(i > 6):
                        break
                    continue       
                if(maValue > preMaValue):
                    downCount += 1
                    preMaValue = maValue
                elif((preMaValue - maValue)/maValue > 0.002):
                    isBroken = True
            if(isBroken):
                break
        if(downCount >= 30):
            is_good = True
        return is_good
                    
                
        
if __name__ == '__main__': 
    stock_number = "000040"
    stock_tag = "sz"
    monitorCount = 5
    collection_name = G_CONST_DB_DETAIL_TABLE + "_" + stock_number + "_" + stock_tag
    period = 20
    duration = ("2018-01-23")
    #duration = None
    study_count = 5
    indx_handler = IndexBoll(collection_name, period,  duration=duration, study_count=study_count, monitor_count = monitorCount)
    good = indx_handler.is_good_by()
    print "Good is {}".format(good)
    #indx_handler.update_data()
    #raw_data = indx_handler.get_raw_data()
    #data_kdj = indx_handler.get_index_data(raw_data)
    #print("count is " + str(len(raw_data)))
    #data_boll = indx_handler.get_refined_data()
    #goodValue = indx_handler.is_good_by()
    #print("good value is {}".format(goodValue))
    #indx_handler.db_helper.print_values(data_boll, 100, [G_CONST_DATE_FIELD_NAME, G_CONST_ClOSE_FIELD_NAME, G_CONST_CALCULATION_BOLL_UP, G_CONST_CALCULATION_BOLL_MA,G_CONST_CALCULATION_BOLL_DOWN,G_CONST_CALCULATION_BOLL_MA_MD], False)
    #values = indx_handler.study_good()
    #macd_days = [5, 10, 2]
    #macd_handler = IndexMACD(collection_name, macd_days, duration=duration, study_count=500) 
    #values2 = macd_handler.study_good()
    #newValues = dbHelper.merge_index_data(values, values2)
    #indx_handler.db_helper.print_values(values, 200,[G_CONST_DATE_FIELD_NAME, G_CONST_STUDY_DEAL_DAYS, G_CONST_STUDY_GOOD_VALUE, G_CONST_STUDY_REAL_RATE, G_CONST_STUDY_CORRECT],False)
    

    
          