import urllib2
import csv

Open_vol = 1
High_vol = 2
Low_vol = 3
Close_vol = 4
Volume_vol = 5
Adj_vol = 6

def print_lines(rows, line_count = 100):
    index = 0
    for row in rows:
        print row
        if(index == line_count):
            break
        index = index + 1
def dowload(stock_id):
    url = "http://table.finance.yahoo.com/table.csv?s=" + stock_id 
    response = urllib2.urlopen(url)
    cr = csv.reader(response)
    return cr


if __name__ == "__main__":
    dowload("000001.sz")