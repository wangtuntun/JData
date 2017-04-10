#encoding=utf-8
import pandas as pd
import numpy as np
import datetime
import time
from datetime import timedelta
'''
func：对数据作出一些分析，得出一些结论型的东西，辅助得到解题思路
author：tuntunking
date：2017/3/28
ps:
'''
# data_dir="/public/home/scu1701/JData/Data/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData/"#server dealed data path
data_dir="/home/wangtuntun/JData/Data/" #local data path
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path

# 获取数据中包含的所有天
def get_all_date():
    return_list=[]
    return_list.append("2016-01-31")#二月份包括1/31
    year="2016-"
    month="02-"
    for day in range(1,30):
        if day < 10:
            day="0"+str(day)
        else:
            day=str(day)
        date=year+month+day
        return_list.append(date)
    month = "03-"
    for day in range(1, 32):
        if day < 10:
            day = "0" + str(day)
        else:
            day = str(day)
        date = year + month + day
        return_list.append(date)
    month = "04-"
    for day in range(1, 16):
        if day < 10:
            day = "0" + str(day)
        else:
            day = str(day)
        date = year + month + day
        return_list.append(date)
    return return_list


#获取最近几天的日期
def get_recent_days(last_day,return_days):
    # return_days=3
    return_list=[]
    deadline=datetime.datetime.strptime("2016-01-31", '%Y-%m-%d')
    last_date=datetime.datetime.strptime(last_day, '%Y-%m-%d')
    date_delta=last_date- deadline
    if  date_delta> timedelta(days=return_days):
        pass
    else:
        return_days=date_delta.days
    for i in range(0,return_days):
        next_day=(last_date-timedelta(days=i)).date()
        return_list.append(str(next_day))
    return return_list,return_days

#将234这三个月的交易情况进行合并，只要购买行为，最后有285 0029次成交量.两分钟跑完。
def get_all_buy():
    action2_data_path="/home/wangtuntun/JData/Data/JData_Action_201602.csv"
    action3_data_path="/home/wangtuntun/JData/Data/JData_Action_201603.csv"
    action3e_data_path="/home/wangtuntun/JData/Data/JData_Action_201603_extra.csv"
    action4_data_path="/home/wangtuntun/JData/Data/JData_Action_201604.csv"
    action2=pd.read_csv(action2_data_path,parse_dates=[2])
    action2=action2[action2["type"]==4]
    action3=pd.read_csv(action3_data_path,parse_dates=[2])
    action3=action3[action3["type"]==4]
    action3e=pd.read_csv(action3e_data_path,parse_dates=[2])
    action3e=action3e[action3e["type"]==4]
    action4=pd.read_csv(action4_data_path,parse_dates=[2])
    action4=action4[action4["type"]==4]

    sum_action1=action2.append(action3,ignore_index=True)
    sum_action2=sum_action1.append(action3,ignore_index=True)
    sum_action3=sum_action2.append(action3e,ignore_index=True)
    sum_action4=sum_action3.append(action4,ignore_index=True)

    # sum_filter=sum_action4[sum_action4["type"]==4]

    sum_type4_path="/home/wangtuntun/JData/Data/Dealed/234month_type4.csv"
    # sum_filter.to_csv(sum_type4_path,index=False)
    sum_action4.to_csv(sum_type4_path,index=False)

#将product按照skuid进行排序
def sort_product_by_skuid():
    product_path = "/home/wangtuntun/JData/Data/JData_Product.csv"
    result_path="/home/wangtuntun/JData/Data/Dealed/JData_Product_sorted.csv"
    product=pd.read_csv(product_path)
    product=product.sort_values(by=["sku_id"])
    product.to_csv(result_path,index=False)



#看下用户都买些什么东西。按照user_id,time,sku_id排序就可以了
#发现有些用户的记录完全是重复的，也有一个用户买多个商品的情况
#去掉重复的，发现三个月有一万多人买Product中的东西
#发现给出的数据，用户只购买一次。所以没有回头客一说。
def get_user_buy_count():
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    filter_path = dealed_data_dir+"234month_type4.csv"
    filter_sorted_path=dealed_data_dir+"234month_type4_sorted.csv"
    filter_data=pd.read_csv(filter_path)
    filter_data=filter_data.sort_values(by=["user_id","time","sku_id"])
    filter_data=filter_data.drop_duplicates()
    filter_data = filter_data.drop_duplicates(["user_id"])
    filter_data.to_csv(filter_sorted_path,index=False)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

#看下四月份，每个人的行为，购买还是收藏等。其实只是把四月份的数据排序而已.跑完需要一分钟。
#如果看所有用户四月份的所有type，发现大多数都没购买，看不出来什么。
def get_user_action():
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    action4_data_path = data_dir + "JData_Action_201604.csv"
    action4_sorted_path=dealed_data_dir + "JData_Action_201604_sorted.csv"
    action4_data=pd.read_csv(action4_data_path)
    action4_data=action4_data.sort_values(by=["user_id","time","sku_id"])
    action4_data.drop("model_id",axis=1,inplace=True)
    action4_data.to_csv(action4_sorted_path,index=False)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
#分析每天的购买量
'''
#发现3/1 3/7 3/14 3/15 3/16 4/14 销量有点不正常，比平常高出2-4倍，将这几个日期作为一个特征？
#感觉2/1----2/15号都在受春节的影响
#3/14 3/15 3/16 是受3/15影响，那么3/1 和 4/14 数据异常是什么原因导致的？
#感觉好像不受节假日以及周末的影响
'''
def get_day_sales():

    import csv
    csv_reader = csv.reader(open(dealed_data_dir + '234month_type4.csv'))
    raw_list = []
    for row in csv_reader:
        raw_list.append(row)
    raw_list = raw_list[1:len(raw_list)]
    day_flow_dict = {}
    date_list=get_all_date()
    for date in date_list:
        day_flow_dict[date] = 0
    for ele in raw_list:
        userid = int(float(ele[0]))
        sku_id = int(ele[1])
        date = (ele[2].split(" ")[0].strip())
        day_flow_dict[date] += 1
    day_flow_dict = sorted(day_flow_dict.items())
    for ele in day_flow_dict:
        print(ele)
    return
