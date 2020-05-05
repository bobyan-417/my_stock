# -*- coding: utf-8 -*-

import re
import logging

from my_test.utils.Downloader import HtmlDownloader

LOGGER = logging.getLogger(__name__)


class HTMLFetcher(object):
    FIELDS=[]
    URL_FORMAT=None

    def __init__(self, stock_id):
        self.stock_id = stock_id
        url = self.URL_FORMAT.format(stock_id)
        self.properties = self.get_properties(url)

    def get_properties(self, url):
        html_download = HtmlDownloader(url)
        properties = self.parse_properties(html_download.html)
        return properties


    def print_properties(self):
        LOGGER.info("The properties are below:")
        for prop in self.properties.keys():
            value = self.properties[prop]
            mark = "="
            if type(value) is dict:
                value = ["{}={}".format(key, value) for key, value in value.iterms()]
                mark = ":"
            LOGGER.info("{}{}{}".format(prop, mark, value))

class GSJJFetcher(HTMLFetcher):
    FIELDS = ["证券代码","证券简称","公司名称","公司曾有名称","证券简称更名历史","省份","城市","注册地址"
              "法人代表","总经理","职工总数","经营范围","公司网址","发行价格"]
    URL_FORMAT = "http://q.stock.sohu.com/cn/{}/gsjj.shtml"

    def parse_properties(self, html):
        titles = []
        pattern = "\<th.*?"
        properties = {}

        for field_name in self.FIELDS:
            pattern = "\<th\>{}.*?\<td\>([^<]*)\</td\>".format(field_name)
            values = re.findall(pattern, html, re.DOTALL)
            if not values:
                pattern = "\<h3\>{}.*?\<p\>\s*([^<]*)\s*\</p\>".format(field_name)
                values = re.findall(pattern, html, re.DOTALL)
            if values:
                properties[field_name] = values[0].strip()
        return properties

class MainFinnanceReport(HTMLFetcher):
    FIELDS = ["每股收益","每股净资产","每股经营现金流","净利润率","资产负债率","流动比率","速动比率","主营业务利润"
              "营业利润","利润总额","净利润","流动负债","长期负债","发行价格", "流动资产","固定资产", ""]
    URL_FORMAT = "http://q.stock.sohu.com/cn/{}/cwzb.shtml"

    def parse_properties(self, html):
        properties = {}
        for field_name in self.FIELDS:
            pattern = "\<th\>{}.*?\<td\>([^<]*)\</td\>".format(field_name)
            values = re.findall(pattern, html, re.DOTALL)
            if not values:
                pattern = "\<h3\>{}.*?\<p\>\s*([^<]*)\s*\</p\>".format(field_name)
                values = re.findall(pattern, html, re.DOTALL)
            if values:
                properties[field_name] = values[0].strip()
        return properties

if __name__ == "__main__":
    gsjj = MainFinnanceReport("688126")
    gsjj.print_properties()