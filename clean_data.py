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

#将user表中的age字段进行处理，首先去除汉字“岁”，并把年龄段改为具体数字。
#缺省年龄-1,变为0; 16-25->1  26-35->2 36-45->3 46-55->4  56以上->5
def clean_user_info():
    import re
    user_path=data_dir + "JData_User.csv"
    result_path=data_dir + "JData_User2.csv"
    data=pd.read_csv(user_path)
    data.fillna(0)
    data_list=np.array(data)
    new_data_list=[]
    for row in data_list:
        new_age=0
        temp_list=[]
        temp_list.append(int(row[0]))
        age=row[1]
        temp_list
        age=re.sub("\D", "", str(age))
        if age == "1":
            new_age=0
        if age == "1625":
            new_age=1
        if age == "2635":
            new_age=2
        if age == "3645":
            new_age=3
        if age == "4655":
            new_age=4
        if age =="56":
            new_age=5
        sex=row[2]
        import math
        if math.isnan(sex):
            sex=0
        else:
            sex=int(sex)

        temp_list.append(new_age)
        temp_list.append(sex)
        temp_list.append(int(row[3]))
        temp_list.append(str(row[4]))
        new_data_list.append(temp_list)
    # for ele in new_data_list:
    #     print(ele)
    new_df=pd.DataFrame(new_data_list,columns=["user_id","age","sex","user_lv_cd","user_reg_dt"])
    new_df.to_csv(result_path,index=False)
#对produc表进行过滤，只保留出现在action中的数据
def filter_produc_by_action():
    product_info=pd.read_csv(data_dir+"JData_Product.csv")
    action_info=pd.read_csv(dealed_data_dir+"234month_skuid_filtered.csv")
    skuid_in_action=set(action_info["sku_id"].tolist())
    product_info=product_info[product_info["sku_id"].isin(skuid_in_action)]
    product_info.to_csv(data_dir+"JData_Product2.csv")
filter_produc_by_action()