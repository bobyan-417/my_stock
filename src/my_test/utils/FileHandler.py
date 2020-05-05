import os

from Assist import UnitHelper

class CSVFileHandler(object):
    def __init__(self):
        pass
    def _replaceTitle(self, source_file_path, newTitle):
        title = "";
        line = None;
        if(os.path.exists(source_file_path)):
            source_file = open(source_file_path, 'r');
            title = source_file.readline().strip("\n");
            line = title          
        if(title == newTitle):
            return True;
        else:
            target_file_path = source_file_path + ".tmp"
            if(os.path.exists(target_file_path)):
                os.remove(target_file_path)
            target_file = open(target_file_path, 'w')
            target_file.write(newTitle)            
            if(line != None):
                target_file.write("\n")
                line = source_file.readline();
            while line:
                if(line != None and len(line.strip()) > 2):
                    target_file.write(line)
                line = source_file.readline();
            target_file.close();
            if(os.path.exists(source_file_path)):
                source_file.close();
                os.remove(source_file_path)
            #("rename " + target_file_path + " to " + source_file_path)
            os.rename(target_file_path, source_file_path)
        return True;
    def getTitles(self, source_file_path):
        if(os.path.exists(source_file_path)):
            source_file = open(source_file_path, 'r');
            title = source_file.readline()            
            source_file.close()
            title = title.strip()
            return title.split(",")
        return None
    def _addLineToFile(self, source_file_path, line):
        if(line == None or len(line.strip()) == 0):
            return True
        source_file = open(source_file_path, 'a')
        source_file.write("\n")
        source_file.write(line)
        source_file.close()
        return True
    def addValuesToFile(self, source_file_path, values):
        title = None;
        line = None;
        if(values != None and len(values) > 0): 
            for hashValue in values:
                (key, value) = hashValue.items()[0]
                if(title == None):
                    title = str(key)
                    line = str(value)
                else:
                    title += "," + str(key)
                    line += "," + str(value)
        #print("expected title:" + title);
        if(os.path.exists(source_file_path)):
            self._replaceTitle(source_file_path, title)
            self._addLineToFile(source_file_path, line)
        else:
            filePath = os.path.dirname(source_file_path)
            if(not os.path.exists(filePath)):
                os.makedirs(filePath)
            source_file = open(source_file_path, 'a')
            source_file.write(title)
            source_file.write("\n")
            source_file.write(line)
            source_file.close()        
        return
    '''
    it reads the data and return following structure
    [
      {
        title1:data11,
        title2:data12,
        ...
      }
      {
        title1:data21,
        title2:data22,
        ...
      },
    ]
    '''
    def readData(self, confFile):   
        arrayNodes = []
        if(os.path.isfile(confFile)):
            readHandle = open (confFile)
            #remote the title line
            titleLine = readHandle.readline()
            titleLine = titleLine.strip()
            titles = titleLine.split(",")
            #arrayNodes.append({'titles':titles})
            for line in readHandle:
                line = line.strip()
                oneRow = line.split(",")
                if(len(titles) == len(oneRow)):
                    node = dict()
                    for i in range(0,len(titles)):
                        node[titles[i].lower()] = oneRow[i]
                    if(node):
                        arrayNodes.append(node)
            readHandle.close()
        return arrayNodes  
    '''
    it reads the data and return following structure
    [
      {
        title1:data11,
        title2:data12,
        ...
      }
      {
        title1:data21,
        title2:data22,
        ...
      },
    ]
    '''
    def readTableDataWithTimestamp(self, files, starttime, endtime, maxpoints = 1000):   
        tableValues = []
        origData = self.readDataWithTimestamp(files, starttime, endtime, maxpoints)
        titles = origData.pop(0)
        data = origData[::-1]
        print("my title is " + ",".join(titles))
        for rowValues in data:
            print("my value is " + ",".join(rowValues))
            hRowValue = dict()
            points = rowValues
            for i in range(0,len(titles)): 
                hRowValue[titles[i].lower()] = points[i]  
            if(hRowValue):             
                tableValues.append(hRowValue)
        return tableValues       
    '''
      This subroutine is to read the csv file with timestamp and also may define
      the max points 
      the return list is like:
      [
        {label: title1, data:[point1, point2, ..., pointMax]},
        {label: title2, data:[point1, point2, ..., pointMax]},
        ...
      ]
    '''       
    def readLineChartDataWithTimestamp(self, files, starttime, endtime, maxpoints = 100):   
        data = self.readDataWithTimestamp(files, starttime, endtime, maxpoints)
        hData = dict()
        titles = data.pop(0)
        for rowValues in data:
            points = rowValues
            index = 0
            for pointTitle in titles:                                   
                if(hData.has_key(pointTitle)):
                    value = "0"
                    if(len(points) > index):
                        value = points[index]
                    hData[pointTitle]['data'].append(value)
                else:
                    pointValues = []
                    value = "0"
                    if(len(points) > index):
                        value = points[index]                    
                    pointValues.append(value)
                    newPoint = dict()                    
                    newPoint['label'] = pointTitle
                    newPoint['data'] = pointValues
                    hData[pointTitle] = newPoint
                index = index + 1
        dataValues = []
        for pointTitle in titles:
            if(hData.has_key(pointTitle)):
                dataValues.append(hData[pointTitle])                   
        return dataValues 
    '''
    read the data in the files and return following structure:
    [
      [value11, value12, value13, ...],
      [value21, value22, value23, ...],
      ...
    ]
    '''  
    def readDataWithTimestamp(self, files, starttime, endtime, maxpoints = 100):   
        allLines = []
        titles = []  
        timeformat = "%Y-%m-%d %H:%M:%S"  
        unitHelper = UnitHelper()     
        for dataFile in files:
            readHandle = open (dataFile)
            fileTitle = readHandle.readline()
            fileTitle = fileTitle.strip();
            if(len(titles) == 0):
                titles = fileTitle.split(",")            
            for line in readHandle:
                line = line.strip()
                if(line):
                    datetimeStr = line.split(",")[0]  
                    isSelected = True
                    try:             
                        isSelected = not starttime or unitHelper.compareDateTimeStr(datetimeStr, starttime
                                                                       , timeformat) >= 0
                        if(not isSelected):
                            continue    
                        isSelected = isSelected and (not endtime or unitHelper.compareDateTimeStr(datetimeStr, endtime
                                                                       ,timeformat) <= 0)
                        if(not isSelected):
                            break;
                    except Exception as err:
                        self.log.logError(str(err))
                    if(isSelected):  
                        allLines.append(line) 
            readHandle.close(); 
        allPointsCount = len(allLines)
        step = int(allPointsCount/maxpoints);
        lines = []
        if(step > 1):
            for i in range(0, maxpoints):
                lines.append(allLines[step*i])
            startPoint = step*maxpoints + 1
            for restData in allLines[startPoint:]:
                lines.append(restData)
        else:
            lines = allLines
        data = []        
        data.append(titles)
        for line in lines:
            line = line.strip();
            if(not line):
                continue
            values = line.split(",")
            data.append(values)
        
        return data  
    def tail(self,  filename, lines=20 ):
        f = open(filename)
        if(not f):
            return None
        total_lines_wanted = lines
    
        BLOCK_SIZE = 1024
        f.seek(0, 2)
        block_end_byte = f.tell()
        lines_to_go = total_lines_wanted
        block_number = -1
        blocks = [] # blocks of size BLOCK_SIZE, in reverse order starting
                    # from the end of the file
        while lines_to_go > 0 and block_end_byte > 0:
            if (block_end_byte - BLOCK_SIZE > 0):
                # read the last block we haven't yet read
                f.seek(block_number*BLOCK_SIZE, 2)
                blocks.append(f.read(BLOCK_SIZE))
            else:
                # file too small, start from begining
                f.seek(0,0)
                # only read what was not read
                blocks.append(f.read(block_end_byte))
            lines_found = blocks[-1].count('\n')
            lines_to_go -= lines_found
            block_end_byte -= BLOCK_SIZE
            block_number -= 1
        f.close()
        all_read_text = ''.join(reversed(blocks))
        allLines = []
        for line in all_read_text.splitlines()[-total_lines_wanted:]:
            if(line):
                allLines.append(line)
        return allLines   
    def first(self, filename, lines=20):
        f = open(filename)
        if(not f):
            return None
        allLines = []
        curLine = 0
        for line in f:            
            if(line):
                line = line.rstrip()
                allLines.append(line) 
                curLine = curLine + 1
                if(curLine >= lines):
                    break; 
        f.close()
        return allLines           
if __name__ == "__main__":
    csvFilehandler = CSVFileHandler()
    csvFilehandler.readData("C:/bobyan/project/Endurance/Monitor/endurance_monitor/src/static/data/durability_thunderbird_uptime.csv")
    #values = csvFilehandler.readData(confFile)