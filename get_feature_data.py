#encoding=utf-8
import pandas as pd
import numpy as np
import datetime
import time
from datetime import timedelta
'''
得到所有特征
'''

# data_dir="/public/home/scu1701/JData/Data/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData/"#server dealed data path
data_dir="/home/wangtuntun/JData/Data/" #local data path
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path


#对234月数据进行特征的分析（这些特征是根据是根据理论分析出来的，没有进行验证），看有些特征是否有意义
#但是这个只是作为参老，最终还要看分数

#一 分析商家每天被动作次数
#冷门商家170413,基本没人买，但是每天都有点击和浏览，但是很少有添加到购物车行为
#较热门商家如5825,一般天数有人购买，几乎每天都有添加到购物车行为
#热门商家如52343,几乎每天都有人购买。
#所以有必要添加特征 is_hot
#二 分析商家滑动特征




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

def analize_feature():
    import time
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    action_data_path = dealed_data_dir + "234month_skuid_filtered.csv"
    action_data=pd.read_csv(action_data_path)
    action_data_array=np.array(action_data)
    product_path=data_dir + "JData_Product.csv"
    product_data=pd.read_csv(product_path)
    # product_data["sku_id"]=product_data["sku_id"].astype(int)
    sku_id_list=product_data["sku_id"].tolist()


    #------------每个sku_id被动作次数-------------------------#
    # 初始化 商品每天被动作次数
    sku_day_buy_dict={}
    sku_day_click_dict = {}
    sku_day_add2car_dict = {}
    sku_day_view_dict = {}
    # 初始化 商品每天被哪些用户动作次数
    sku_day_diff_user_buy_dict = {}
    sku_day_diff_user_click_dict = {}
    sku_day_diff_user_add2car_dict = {}
    sku_day_diff_user_view_dict = {}
    # 初始化 商品最近2天被动作次数
    sku_2days_buy_dict = {}
    sku_2days_click_dict = {}
    sku_2days_add2car_dict = {}
    sku_2days_view_dict = {}
    # 初始化 商品最近2天被不同的用户动作次数
    sku_2days_diff_user_buy_dict = {}
    sku_2days_diff_user_click_dict = {}
    sku_2days_diff_user_add2car_dict = {}
    sku_2days_diff_user_view_dict = {}
    # ------------每个用户动作多少次---------------------------#

    date_list=get_all_date()
    for date in date_list:
        for sku in sku_id_list:
            sku_day_buy_dict[(sku,date)] = 0.0#每个商品每天被买了多少次
            sku_day_add2car_dict[(sku,date)] = 0.0#每天被添加到购物车次数
            sku_day_click_dict[(sku, date)] = 0.0#每天点击次数
            sku_day_view_dict[(sku, date)] = 0.0  # 每天被浏览
            sku_2days_buy_dict[(sku, date)] = 0.0  # 每个商品最近2天平均被买了多少次
            sku_2days_click_dict[(sku, date)] = 0.0  # 每个商品最近2天平均被买了多少次
            sku_2days_add2car_dict[(sku, date)] = 0.0  # 每个商品最近2天平均被买了多少次
            sku_2days_view_dict[(sku, date)] = 0.0  # 每个商品最近2天平均被买了多少次

            sku_day_diff_user_buy_dict[(sku, date)] = set()#每个商品每天被哪些不同用户动作
            sku_day_diff_user_add2car_dict[(sku, date)] = set()
            sku_day_diff_user_click_dict[(sku, date)] = set()
            sku_day_diff_user_view_dict[(sku, date)] = set()
            sku_2days_diff_user_buy_dict[(sku,date)]=set()#每个商品最近2天被哪些不同用户动作
            sku_2days_diff_user_add2car_dict[(sku, date)] = set()
            sku_2days_diff_user_click_dict[(sku, date)] = set()
            sku_2days_diff_user_view_dict[(sku, date)] = set()
    #遍历action并得到每个商家每天被动作数量
    for row in action_data_array:
        user_id=row[0]
        sku_id=row[1]
        action_date=str(row[2]).split(" ")[0].strip()
        type=int(row[4])
        cate=row[5]
        brand=row[6]
        if type==4:#购买
            sku_day_buy_dict[(sku_id, action_date)] += 1
            sku_day_diff_user_buy_dict[(sku_id, date)].add(user_id)
        if type == 2:#添加到购物车
            sku_day_add2car_dict[(sku_id, action_date)] += 1
        if type == 6:#点击
            sku_day_click_dict[(sku_id, action_date)] += 1
        if type == 1:#浏览
            sku_day_view_dict[(sku_id, action_date)] += 1

    for sku,today in sku_day_diff_user_buy_dict:
        if len(sku_day_diff_user_buy_dict[(sku,today)]) != 0:
            print(sku,today,sku_day_diff_user_buy_dict[(sku,today)])

    for sku in sku_id_list:#每个商家
        for date in get_all_date():#每天对应的最近2天
            recent_days,days_number=get_recent_days(date,2)
            for today in recent_days:
                sku_2days_click_dict[(sku, date)] += sku_day_click_dict[(sku,today)]#每个商家每天对应的最近两天的动作数，就是该商家最近2天动作数的累加
                sku_2days_buy_dict[(sku, date)] += sku_day_buy_dict[(sku, today)]
                sku_2days_add2car_dict[(sku, date)] += sku_day_add2car_dict[(sku, today)]
                sku_2days_view_dict[(sku, date)] += sku_day_view_dict[(sku, today)]
            if days_number != 0:
                sku_2days_click_dict[(sku, date)] = sku_2days_click_dict[(sku, date)] / float(days_number)
                sku_2days_buy_dict[(sku, date)] = sku_2days_buy_dict[(sku, date)] / float(days_number)
                sku_2days_add2car_dict[(sku, date)] = sku_2days_add2car_dict[(sku, date)] / float(days_number)
                sku_2days_view_dict[(sku, date)] = sku_2days_view_dict[(sku, date)] / float(days_number)
            else:
                pass

    #查看特征
    # for ele in sku_2days_click_dict:
    #     if sku_2days_click_dict[ele] != 0.0:
    #         print(ele,sku_2days_click_dict[ele])


    # ------------每个sku_id每周被购买以及各个动作次数-------------#
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

analize_feature()