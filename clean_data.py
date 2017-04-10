#encoding=utf-8
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import time
'''
func：对数据进行一些基本的清洗
author：tuntunking
date：2017/4/8
ps:
'''
# data_dir="/public/home/scu1701/JData/Data/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData/"#server dealed data path
data_dir="/home/wangtuntun/JData/Data/" #local data path
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path

#删除未交互数据
def remove_sku_without_interaction():
    return

#action表中，skuid没在product表中的记录删除
#在本地运行需要一分钟
#数据量太大，看不出来规律。
def filter_with_product_skuid():
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    action2_path = data_dir + "JData_Action_201602.csv"
    action3_path = data_dir + "JData_Action_201603.csv"
    action4_path = data_dir + "JData_Action_201604.csv"
    product_path = data_dir + "JData_Product.csv"
    filter_path = dealed_data_dir + "234month_skuid_filtered.csv"

    action2 = pd.read_csv(action2_path)
    action3 = pd.read_csv(action3_path)
    action4 = pd.read_csv(action4_path)
    product=pd.read_csv(product_path)
    sku_in_product=product["sku_id"].tolist()

    action2_filtered = action2[action2["sku_id"].isin(sku_in_product)]
    action3_filtered = action3[action3["sku_id"].isin(sku_in_product)]
    action4_filtered = action4[action4["sku_id"].isin(sku_in_product)]

    sum_action1 = action2_filtered.append(action3_filtered, ignore_index=True)
    sum_action2 = sum_action1.append(action4_filtered, ignore_index=True)

    sum_action2["user_id"] = sum_action2["user_id"].astype(int)
    sum_filterd = sum_action2.sort_values(by=["user_id", "time", "sku_id"])
    sum_filterd.to_csv(filter_path,index=False)

    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

#过滤掉重复的记录
def remove_duplicate_record():
    return


filter_with_product_skuid()