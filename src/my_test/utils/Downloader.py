# -*- coding: utf-8 -*-
import urllib2
import csv
from time import strptime, mktime
from datetime import datetime
from my_test.conf.Configuration import G_CONST_TIME_FORMATE,\
    G_CONST_CHANCE_CLOSE_FIELD_NAME, G_CONST_ADJ_RATE_FIELD_NAME
from my_test.conf.Configuration import G_CONST_DATE_FIELD_NAME, G_CONST_ID_FIELD_NAME
from my_test.conf.Configuration import G_CONST_ClOSE_FIELD_NAME, G_CONST_OPEN_FIELD_NAME, G_CONST_HIGH_FIELD_NAME, G_CONST_LOW_FIELD_NAME
from my_test.conf.Configuration import G_CONST_VOLUMN_FIELD_NAME, G_CONST_EXCHANGE_RATE_FIELD_NAME, G_CONST_TURNOVER_FIELD_NAME
 
import json 
import re, time
from bs4 import BeautifulSoup

Time_vol = 0
Open_vol = 1
High_vol = 2
Low_vol = 3
Close_vol = 4
Volume_vol = 5
Adj_vol = 6

class CsvDownloader(object):
    def __init__(self):
        pass
    
    def get_values(self, url):        
        response = urllib2.urlopen(url)
        cr = csv.reader(response)
        values = self.parse_values(cr)
        return values
    
    def parse_values(self, lines):
        titles = None
        timeformat = G_CONST_TIME_FORMATE 
        values = []
        for line in lines:
            if(titles == None):
                titles = line 
                continue
            date_value = dict()
            for index in range(0,len(titles)):
                if(index ==  Time_vol):
                    timestamp = line[Time_vol]
                    dt = datetime.fromtimestamp(mktime(strptime(timestamp, timeformat))) 
                    date_value[G_CONST_ID_FIELD_NAME] = dt
                    date_value[G_CONST_DATE_FIELD_NAME] = dt
                else:                                    
                    date_value[titles[index]] = float(line[index])
            values.append(date_value)
        return values    

    def print_lines(self, rows, line_count = 100):
        index = 0
        for row in rows:
            print row
            if(index == line_count):
                break
            index = index + 1
            
class JsonDownloader(object):
    time_index = 0
    open_index = 1
    close_index = 2
    change_index = 3
    change_rate = 4
    low_index = 5
    high_index = 6
    volumn_index = 7
    turnover_index = 8
    exchange_rate = 9
    
    relation_table = {
                      G_CONST_DATE_FIELD_NAME : time_index,
                      G_CONST_ID_FIELD_NAME : time_index,
                      G_CONST_ClOSE_FIELD_NAME : close_index,
                      G_CONST_CHANCE_CLOSE_FIELD_NAME : change_index,
                      G_CONST_ADJ_RATE_FIELD_NAME : change_rate,                      
                      G_CONST_OPEN_FIELD_NAME : open_index,
                      G_CONST_HIGH_FIELD_NAME : high_index,
                      G_CONST_LOW_FIELD_NAME : low_index, 
                      G_CONST_VOLUMN_FIELD_NAME : volumn_index,
                      G_CONST_TURNOVER_FIELD_NAME : turnover_index,
                      G_CONST_EXCHANGE_RATE_FIELD_NAME : exchange_rate,                                            
                      }
    
    def __init__(self):
        pass
    def download_data(self, url):  
        try:  
            data = urllib2.urlopen(url).read()  
            return data  
        except Exception,e:  
            print e  
            
    def get_values(self, url):
        data = self.download_json_data(url)
        return data
    def download_json_data(self, url):
        retry = 5
        succeeded = False
        i = 0
        while(i < retry and not succeeded):
            try:
                jason_data = self.download_data(url)
                data = json.loads(jason_data, object_hook=_decode_dict)
                #print data[0]
                succeeded = True
                if(not data or not data[0].has_key('hq')):
                    return None
            except Exception as e:
                print(e)
                i = i + 1
                if i >= retry:
                    raise
                print("skip error retry {}".format(i))
                time.sleep(10)
        hist_data = data[0]['hq']
        #print hist_data
        all_data = []
        timeformat = G_CONST_TIME_FORMATE
        for data_array in hist_data:
            data_info = dict()
            for key in self.relation_table:
                index = self.relation_table[key]
                field_value = None
                if(key == G_CONST_DATE_FIELD_NAME or key == G_CONST_ID_FIELD_NAME):
                    timestamp = data_array[index]
                    field_value = datetime.fromtimestamp(mktime(strptime(timestamp, timeformat)))
                else:
                    if(data_array[index] == "-"):
                        field_value = "-"
                    elif(data_array[index].find("%") > 0 ):
                        field_value = float(data_array[index].strip('%'))/100
                    else:
                        field_value = float(data_array[index])                        
                data_info[key] = field_value 
            all_data.append(data_info)            
        return all_data

class HtmlDownloader(object):
    def __init__(self, url):
        self.url = url
        self.html = self.fetch_html(url)
        pass
    
    def fetch_html(self, url):
        html = self._get_html(url)
        soup = BeautifulSoup(html, "html.parser")
        print soup
        return str(soup)
    def _get_html(self, url):
        req = None
        try:  
            req = urllib2.urlopen(url)  
            html = req.read()  
            return html  
        finally:
            if req:
                req.close()
    def get_value(self, regex):
        result = None
        if self.html:
            pattern = re.compile(expression.encode('gb2312'))  
            result = pattern.findall(regex, self.html)
        return result
        

                
def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv


def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv
 
if __name__ == "__main__":
    #url = "http://table.finance.yahoo.com/table.csv?s=000001.sz" 
    #csvDowloader = CsvDownloader()
    
    #values = csvDowloader.get_values(url)
    #csvDowloader.print_values(values)
    
    #url = "http://q.my_test.sohu.com/hisHq?code=cn_600016&start=20160804&end=20160819"
    #jsonDownloader = JsonDownloader()
    #data = jsonDownloader.get_values(url)
    #jsonDownloader.print_values(data, 100)
    
    #url = 'http://q.stock.sohu.com/cn/603077/cwzb.shtml'
    #expression = ur'利润总额\D+?(-?\d+\.?\d*)\D*?(-?\d+.*?%)'
    expression = ur'[<]title[>](\w+)'
    #expression = ur'利润总额\D+?(-?\d+\.?\d*)\D*?(-?\d+.*?%)' 
    #html = HtmlDownloader()
    #html.fetch_html(url)
    #result = html.get_value(expression)
    url = "http://query.sse.com.cn/security/stock/getStockListData.do?&jsonCallBack=jsonpCallback43086&isPagination=true&stockCode=&csrcCode=C&areaName=1&stockType=1&pageHelp.cacheSize=1&pageHelp.beginPage=1&pageHelp.pageSize=125&pageHelp.pageNo=1&_=1526392923807"
    html = HtmlDownloader()
    result = html.fetch_html(url)
    print result
    