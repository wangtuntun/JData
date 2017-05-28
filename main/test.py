#encoding=utf-8
import sys
import numpy as np
import csv
import gc
import datetime
import pandas as pd
from sklearn import cross_validation
import requests
import json
import datetime


def get_date_list(start, end, toFormat):
    date_list = []
    date = datetime.datetime.strptime(start, '%Y-%m-%d')
    print date.weekday()
    end = datetime.datetime.strptime(end, '%Y-%m-%d')
    while date <= end:
        date_list.append(date.strftime(toFormat))
        date = date + datetime.timedelta(1)
    return date_list

## api获取假期特征
def isHoliday(date):
    return requests.get('http://www.easybots.cn/api/holiday.php?d=%s'%date).content

dates=get_date_list("2017-03-31","2017-04-16",'%Y-%m-%d')
holidays=isHoliday(dates)
json_object=json.loads(holidays)
# for ele in json_object:
#     print ele,json_object[ele]


