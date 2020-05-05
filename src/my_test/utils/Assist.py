import os, re, sys
#import signal
import time
import platform
from time import strftime, strptime, mktime
import datetime
import smtplib 
import Queue
from threading import Thread
#from subprocess import PIPE, Popen

from my_test.conf.Configuration import G_CONST_DATE_FIELD_NAME,\
    G_CONST_STUDY_GOOD_VALUE, G_CONST_STUDY_INFO
import copy

class MailSender():
    SERVER = "emcmail.lss.emc.com"
    def __init__(self):
        self.log = Logger()
        pass
    def sendMail(self, subject, body, to_list, cc_list=[], mailfrom = "EnduranceDumpChecker@emc.com"):
        self.log.logInfo("entering " + sys._getframe().f_code.co_name)
        text = body
        if(os.path.isfile(body)):
            mailInfo = open(body, 'r')
            lines = mailInfo.readlines()
            text = ""
            for line in lines:
                text += line
            mailInfo.close()
        toListStr = ";".join(to_list)
        ccListStr = ";".join(cc_list)
        message = """\
From: %s
To: %s
Subject: %s

%s
""" % (mailfrom, toListStr, subject, text)
        
        if(cc_list):
            message = """\
From: %s
To: %s
CC: %s
Subject: %s

%s
""" % (mailfrom, toListStr,ccListStr, subject, text)
            
        try:
            server = smtplib.SMTP(self.SERVER)
            server.sendmail(mailfrom, to_list + cc_list, message)
            server.quit()
            self.log.logInfo("Succeed to send mail to " + toListStr + ": " + body)
            self.log.logInfo(message)
        except:
            exc_type = sys.exc_info()
            self.log.logInfo("Error: Can't send mail to " + toListStr + ": " + body)
            self.log.logInfo("Error Info: " + str(exc_type))
            cmd = "perl Email.pl --to='"+ toListStr +"' --subject='" + subject + "' --body='" + body + "' --from=" + mailfrom 
            if(cc_list):
                cmd = "perl Email.pl --to='"+ toListStr +"' --subject='" + subject + "' --body='" + body + "' --from=" + mailfrom +  " --cc=" + ccListStr
            self.log.logInfo("try:" + cmd)            
            f=os.popen(cmd)        
            lines = f.readlines();
            for line in lines:
                line.strip()
                self.log.logInfo(line)

class Logger(object):
    LOG_ERROR = 0
    LOG_INFO = 1
    LOG_DEBUG = 2
    LOG_TRACE = 5
    LOG_SCREEN = True
    LOG_ARCHIVE_SIZE = 2 * 1024 * 1024
    instance = None

    def __init__(self, log_path = None, log_name = None,  log_tag = ""):
        if(log_name):
            self.log_name = log_name
        else:
            cur_exe_filename = os.path.basename(sys.argv[0])        
            logName = cur_exe_filename + ".log"
            match = re.search("^(.*)\.py$", cur_exe_filename, re.IGNORECASE)
            if(match):
                logName = match.groups()[0] + ".log"
            self.log_name = logName
        if(log_path):
            self.log_path = log_path
        else:            
            log_path = os.path.abspath(sys.modules[self.__class__.__module__].__file__)
            log_path = os.path.dirname(log_path)
            log_path += "/../../logs"
            self.log_path = log_path
        self.log_full_path = self.log_path + "/" + self.log_name                  
        if(not os.path.isdir(self.log_path)):
            os.makedirs(self.log_path)
        self.log_level = self.LOG_INFO
        self.log_tag = ""
        if(log_tag):
            self.log_tag =  "[" + log_tag + "]"   
    def getLogFullPath(self):
        return self.log_full_path
    def getLogLevel(self):
        return self.log_level
    def setLogLevel(self, level):
        self.log_level = level                                      
        return
    
    def disableScreenOutput(self):
        self.LOG_SCREEN = False
        
    def enableScreenOutput(self):
        self.LOG_SCREEN = True
        
    def logInfo(self, info):
        info = self.getStringForInfo(info, "")
        logFile = self.getLogFullPath()
        logLevel = self.getLogLevel()
        curtime = strftime("%Y-%m-%d %H:%M:%S")
        if(logLevel >= self.LOG_INFO):
            if(self.LOG_SCREEN):
                print(curtime + self.log_tag + "\t" + info)   
            else:         
                log = open(logFile, 'a')
                log.write(curtime + self.log_tag + "\t" + info + "\n")
                log.close()
    def logDebug(self, info):
        info = self.getStringForInfo(info, "")
        logFile = self.getLogFullPath()
        logLevel = self.getLogLevel()
        curtime = strftime("%Y-%m-%d %H:%M:%S")
        if(logLevel >= self.LOG_DEBUG):
            if(self.LOG_SCREEN):
                print(curtime + self.log_tag + "\t" + info)   
            else:         
                log = open(logFile, 'a')
                log.write(curtime + self.log_tag + "\t" + info + "\n")
                log.close()
    def logError(self, info):
        info = self.getStringForInfo(info,"")
        logFile = self.getLogFullPath()
        logLevel = self.getLogLevel()
        curtime = strftime("%Y-%m-%d %H:%M:%S")
        if(logLevel >= self.LOG_ERROR):
            if(self.LOG_SCREEN):
                print(curtime + self.log_tag + "\tERROR:" + info) 
            else:           
                log = open(logFile, 'a')
                log.write(curtime + self.log_tag + "\tERROR:" + info + "\n")
                log.close() 
    def getStringForInfo(self, info, indent):
        myinfo = ""
        if(type(info) is list):
            myinfo = myinfo + "[\n" + indent + "  "
            for cellValue in info:
                myinfo = myinfo +  self.getStringForInfo(cellValue, indent + " ") + ","  
            myinfo = myinfo + "\n" + indent + "]"
        elif(type(info) is dict):
            myinfo = myinfo + "{\n" + indent + "  "
            for cellValue in info:
                if(cellValue == None):
                    cellValue = "None"
                substring = self.getStringForInfo(info[cellValue], indent + " ")
                if(substring == None):
                    substring = "None"
                if(indent == None):
                    indent = ""
                myinfo = myinfo + str(cellValue) + "=" + str(substring) + ",\n" + indent + "  "  
            myinfo = myinfo + "\n" + indent + "}"  
        elif((type(info) is int) or (type(info) is float) or (type(info) is datetime.datetime)):
            myinfo = str(info)  
        else:
            myinfo = info
        return myinfo             
    def needArchive(self):
        logSize = 0
        needArchive = False
        logPath = self.getLogFullPath()
        if(os.path.exists(logPath)):
            logSize = os.path.getsize(logPath)
        #if the log size is more than LOG_ARCHIVE_SIZE
        #Then it needs to be archivied
        if(logSize >= self.LOG_ARCHIVE_SIZE):
            needArchive = True
        return needArchive
    def archiveLog(self):
        curtime = strftime("%Y_%m_%d_%H_%M_%S")
        logPath = self.getLogFullPath()
        dirpath = os.path.dirname(logPath)
        basename = os.path.basename(logPath)
        backupPath = dirpath + os.sep + "backup"
        archiveBasename = curtime + basename 
        os.rename(logPath, backupPath + os.sep + archiveBasename)  
        
        
class StringHelper(object):
    def __init__(self):
        pass
    def parseRegex(self, regex):
        if(not regex):
            return regex
        length = len(regex)
        newRegex = ""
        i = 0
        while(i < length):                
            if(regex[i] == "*" or regex[i] == "?"):
                if(regex[i-1] <> "."):
                    newRegex += "." + regex[i]
                else:
                    newRegex = regex[i] 
            elif(regex[i] == "." and regex[i] <> "*" and regex[i] <> "?"):
                newRegex += "\\" + regex[i]
            else:
                newRegex += regex[i]
            i = i + 1
        return newRegex 
    def parseRegexPath(self, pathWithRegex):
        newpath = pathWithRegex.replace("/", os.path.sep)
        newpath = newpath.replace("\\", os.path.sep)        
        validatePath = self.parseRegex(newpath) 
        filename = os.path.basename(validatePath)
        filedir = os.path.dirname(validatePath)
        return(filedir, filename)
    def parsePath(self, pathWithRegex):
        if(not pathWithRegex):
            return (None, None)
        newpath = pathWithRegex.replace("/", os.path.sep)
        newpath = newpath.replace("\\", os.path.sep)        
        filename = os.path.basename(newpath)
        filedir = os.path.dirname(newpath)
        return(filedir, filename)    
    def isMatch(self, source, regex):
        return re.search(regex, source)
class UnitHelper(object):
    def __init__(self):
        pass
    def toSecondNumber(self, timeUnit=None):              
        match = re.search("(\d+)\s*(\w+)", timeUnit);
        if(match == None):
            print("Input invalid unit:" + timeUnit);
            return None;
        number = int(match.groups()[0]);
        unit = match.groups()[1];
        if(unit.lower()== 'd'):
            number = 24 * 60 * 60 * number;
        elif(unit.lower() == 'h'):
            number = 60 * 60 * number;
        elif(unit.lower() == 'm'):
            number = 60 * number;
        elif(unit.lower() == 's'):
            number = number;
        return number;    
    def divtd(self, td1, td2):
        divtdi = datetime.timedelta.__div__
        if isinstance(td2, (int, long)):
            return divtdi(td1, td2)
        us1 = td1.microseconds + 1000000 * (td1.seconds + 86400 * td1.days)
        us2 = td2.microseconds + 1000000 * (td2.seconds + 86400 * td2.days)
        return us1 / us2 # this does integer division, use float(us1) / us2 for fp division
    def toKBUnitNumber(self, value):
        match = re.search("(\d+)\s*(\w+)", value);
        if(match == None):
            print("Input invalid unit:" + value);
            return;
        number = int(match.groups()[0]);
        unit = match.groups()[1];
        if(unit.lower()== 'tb'):
            number = 1024 * 1024 * 1024 * number;
        elif(unit.lower() == 'gb'):
            number = 1024 * 1024 * number;
        elif(unit.lower() == 'mb'):
            number = 1024 * number;
        elif(unit.lower() == 'b'):
            number = int(number/1024);
        return number;          
            
    def compareDateTimeStr(self, d1, d2, tformat):
        #if d1 is 0 or empty, which means the start
        if(not d1 or d1 == "0"):
            return -1
        if(not d2 or d2 == "0"):
            return 1
        date1InSecond = mktime(strptime(d1, tformat))
        date2InSecond = mktime(strptime(d2, tformat))
        return date1InSecond - date2InSecond   
    def reduceDays(self, date, days):
        newDate = date - datetime.timedelta(days)
        return newDate
    def convertToDatetime(self, date_str):
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
            rlt = datetime.datetime.fromtimestamp(mktime(strptime(date_value, date_format)))
            return rlt
        except Exception as e:
            return None
    
class FolderHelper(object):
    def __init__(self):
        pass
    def delTree(self, folder):
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path): 
                    self.delTree(file_path)
            except Exception, e:
                print e
        os.rmdir(folder)
    def getPathWithRegex(self, rootPath, regex):
        pathList = []
        if(rootPath == "." or rootPath == ".."):
            return pathList
        if(re.search(regex, rootPath)):
            pathList.append(rootPath)        
        elif(os.path.isdir(rootPath)):
            for subdir in os.listdir(rootPath):
                pathList.extend(self.getPathWithRegex(os.path.join(rootPath,subdir), regex))
        return pathList    


class ThreadExecutor(object):
    def __init__(self):
        self.queue = Queue.Queue()
        self.logger = Logger()
        
    def put(self, action, msg, **args):
        actionInfo = dict()
        actionInfo['action'] = action
        actionInfo['args'] = args['args']
        actionInfo['msg'] = msg
        self.queue.put(actionInfo)  
          
    def _consumer(self,myId, args):
        logger = Logger(None, None, "Thread" + str(myId))
        while(True):
            try:                
                actionInfo = self.queue.get(False)
                action = actionInfo['action']
                kwargs = actionInfo['args']
                msg = actionInfo['msg']                 
                logger.logInfo("consuming " + msg)
                if(type(kwargs) is tuple):
                    action(logger = logger, *kwargs)
                else:
                    action(kwargs, logger = logger)          
                time.sleep(3)            
            except Queue.Empty:
                logger.logInfo("exits as there is action available ")
                break
    def work(self, max_thread_count):
        workers = []
        threadId = 1
        while(threadId <= max_thread_count):
            self.logger.logInfo("creating worker " + str(threadId))
            worker = Thread(target=self._consumer, args=(threadId, None))
            workers.append(worker)
            threadId = threadId + 1
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()        
        


class HealthCheck(object):
    def __init__(self,  logger = None, store_path = "/var/monitor/"):
        pid = os.getpid()
        if(store_path == None):
            if(platform.system() == "Windows"):
                store_path = "c:\\monitor"
            else:
                store_path = "/var/monitor"
        self.full_path = store_path + "/" + str(pid)
        self.pid = pid
        self.store_path = store_path
        if(logger == None):
            self.logger = Logger()
        else:
            self.logger = logger
        self.maxDeadTimes = 10
        self.processDeadTimes = dict()
        if(not os.path.isdir(store_path)):
            os.makedirs(store_path)            
    def product(self):
        product_file = self.full_path + ".running"
        if(not os.path.isfile(product_file)):
            path = os.getcwd()
            cur_exe_fullpath = sys.argv[0]
            cur_exe_name = os.path.basename(cur_exe_fullpath)
            conf = open(product_file, 'w')
            conf.write(path + "/" + cur_exe_name)
            conf.close()
    def pause(self):
        product_waiting =  self.full_path + ".waiting"
        if(not os.path.isfile(product_waiting)):
            path = os.getcwd()
            cur_exe_fullpath = sys.argv[0]
            cur_exe_name = os.path.basename(cur_exe_fullpath)
            conf = open(product_waiting, 'w')
            conf.write(path + "/" + cur_exe_name)
            conf.close()
    def dePause(self):
        product_waiting =  self.full_path + ".waiting"
        if(os.path.isfile(product_waiting)):
            os.unlink(product_waiting) 
 
    def consume(self):
        for confFile in os.listdir(self.store_path):
            confFilename = os.path.basename(confFile)
            match = re.search("^(.*)\.dead$", confFilename)
            if(match):
                processName = match.groups()[0]
                product_waiting =  self.store_path + "/" + processName + ".waiting" 
                if(os.path.isfile(product_waiting)):
                    continue
                if(self.processDeadTimes.has_key(processName)):
                    self.processDeadTimes[processName] = self.processDeadTimes[processName] + 1
                else:
                    self.processDeadTimes[processName] = 1
        for confFile in os.listdir(self.store_path): 
            confFilename = os.path.basename(confFile)
            match = re.search("^(.*)\.running$", confFilename)
            if(match):
                processName = match.groups()[0]
                product_waiting =  self.store_path + "/" + processName + ".waiting"
                if(os.path.isfile(product_waiting)):
                    continue
                self.processDeadTimes[processName] = 0
                deadFile = self.store_path + "/" + processName + ".dead"
                if(os.path.isfile(deadFile)):
                    os.unlink(deadFile)
                os.rename(self.store_path + "/" + confFile, deadFile)
        sendInfo = None
        for processName in self.processDeadTimes.keys():
            self.logger.logInfo(processName + " dead times is " + str(self.processDeadTimes[processName]))
            if(self.maxDeadTimes < self.processDeadTimes[processName]):
                confHandler = open(self.store_path + "/" + processName + ".dead", 'r')
                exe_fullpath = confHandler.readline()
                confHandler.close()
                self.logger.logInfo(exe_fullpath + " is dead. restart it")
                if(not sendInfo):
                    sendInfo = exe_fullpath + " is dead. restart it" + "\n"
                else:
                    sendInfo += exe_fullpath + " is dead. restart it" + "\n"
                #os.kill(self.pid, signal.SIGINT)
                cmd = "kill -9 " + str(processName)
                self.logger.logInfo("cmd: " + cmd)
                os.system(cmd)
                exe_fullpath.strip()               
                cmd = "nohup python " +  exe_fullpath + " > " + exe_fullpath +".nohup &"
                self.logger.logInfo("cmd: " + cmd)
                os.system(cmd)
                self.processDeadTimes[processName] = 0
                deadFile = self.store_path + "/" + processName + ".dead"
                os.unlink(deadFile)
                self.processDeadTimes.pop(processName)
        return sendInfo
    def toString(self):
        content = ""
        for processName in self.processDeadTimes.keys():
            deadTimes = "running"
            if(self.processDeadTimes[processName] > 0):
                deadTimes = "possible dead (checked " + str(self.processDeadTimes[processName]) + " times"
            content += processName + ":" + deadTimes + "\n"
        if(not content):
            content = "No any information recorded, please wait for next check or contact bob yan"
        return content 


class DBHelper(object):
    def __init__(self):
        pass
    def print_values(self, rows, line_count = 100, fields = [], use_titles = True):
        index = 0
        for datevalue in rows:
            output = ""
            only_datetime = False
            if(len(fields) == 0):
                for key in datevalue.keys():
                    if(use_titles):
                        #print("key is " + key)
                        output = output + key + "=" + str(datevalue[key]) + ";"
                    else:
                        output = output  + str(datevalue[key]) + ","
            else:       
                only_datetime = True         
                for key in fields:
                    if(datevalue.has_key(key) and datevalue[key] != None):
                        if(key != G_CONST_DATE_FIELD_NAME):
                            only_datetime = False
                        value = datevalue[key]
                        if(type(value) == float):
                            value = round(value, 3)
                        if(use_titles):
                            output = output + key + "=" + str(value) + ";"
                        else:
                            output = output  + str(value) + ","
            if(not only_datetime):
                print(output)
                if(index == line_count):
                    break
                index = index + 1     
                
    def merge_index_data(self, index_data_1, index_data_2):
        sameData = []
        for data1 in index_data_1:
            for data2 in index_data_2:
                if(data1[G_CONST_DATE_FIELD_NAME] == data2[G_CONST_DATE_FIELD_NAME]):
                    dataInfo = copy.deepcopy(data1)
                    dataInfo[G_CONST_STUDY_GOOD_VALUE] += data2[G_CONST_STUDY_GOOD_VALUE]
                    dataInfo[G_CONST_STUDY_INFO] += data2[G_CONST_STUDY_INFO]
                    sameData.append(dataInfo)  
        return sameData                   