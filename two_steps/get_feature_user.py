#encoding=utf-8
import pandas as pd
import numpy as np
import datetime
import time
from datetime import timedelta
import csv
'''
构建特征，预测哪些用户会购买
'''
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
    for i in range(1,return_days+1):
        next_day=(last_date-timedelta(days=i)).date()
        return_list.append(str(next_day))
    return return_list,return_days

def get_feature(action_data_path,write_path):
    import time
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
#------------------------------------------------------------读取文件信息------------------------------------------------#

    user_path=data_dir + "JData_User2.csv" #用户信息;       将年龄字段有"16-25岁"改为1
    action_data=pd.read_csv(action_data_path)
    action_data_array=np.array(action_data)

    user_data = pd.read_csv(user_path)
    user_data_array=np.array(user_data)

    date_list = get_all_date()  # 训练集中出现的所有日期
    user_id_list=user_data["user_id"].tolist()#所有用户id


#-----------------------------------------------------------获取各种映射表-----------------------------------------------#
    #------------三张表的映射--------------------------#
    user_info_dict = {}
    for row in user_data_array:
        user_id = int(row[0])
        user_info_dict[user_id] = list(row[1:5])




    # ------------每个用户动作---------------------------#
    #下面的特征都是统计的sku_id
    # 初始化 用户每天动作次数
    user_day_click_skuid_dict = {}
    user_day_add2car_skuid_dict = {}
    user_day_view_skuid_dict = {}
    # 初始化 用户每天动作哪些商品
    user_day_diff_sku_click_skuid_dict = {}
    user_day_diff_sku_add2car_skuid_dict = {}
    user_day_diff_sku_view_skuid_dict = {}
    # 初始化 用户最近2天动作次数
    user_2days_click_skuid_dict = {}
    user_2days_add2car_skuid_dict = {}
    user_2days_view_skuid_dict = {}
    # 初始化 用户最近5天动作次数
    user_5days_click_skuid_dict = {}
    user_5days_add2car_skuid_dict = {}
    user_5days_view_skuid_dict = {}
    # 初始化 用户最近2天动作了哪些商品
    user_2days_diff_sku_click_skuid_dict = {}
    user_2days_diff_sku_add2car_skuid_dict = {}
    user_2days_diff_sku_view_skuid_dict = {}
    # 初始化 用户最近5天动作了哪些商品
    user_5days_diff_sku_click_skuid_dict = {}
    user_5days_diff_sku_add2car_skuid_dict = {}
    user_5days_diff_sku_view_skuid_dict = {}

    #------------每个brand被动作---------------------------#
    #下面的特征都是统计的brand 品牌
    # 初始化 用户每天动作哪些品牌
    user_day_diff_sku_click_brand_dict = {}
    user_day_diff_sku_add2car_brand_dict = {}
    user_day_diff_sku_view_brand_dict = {}
    # 初始化 用户最近2天动作了哪些品牌
    user_2days_diff_sku_click_brand_dict = {}
    user_2days_diff_sku_add2car_brand_dict = {}
    user_2days_diff_sku_view_brand_dict = {}
    # 初始化 用户最近5天动作了哪些品牌
    user_5days_diff_sku_click_brand_dict = {}
    user_5days_diff_sku_add2car_brand_dict = {}
    user_5days_diff_sku_view_brand_dict = {}


    # #初始化user
    for date in date_list:
        for user in user_id_list:
            #用户每天/前2天/前5天 动作多少/不同 sku
            user_day_add2car_skuid_dict[(user, date)] = 0.0
            user_day_click_skuid_dict[(user, date)] = 0.0
            user_day_view_skuid_dict[(user, date)] = 0.0

            user_2days_add2car_skuid_dict[(user,date)]=0.0
            user_2days_click_skuid_dict[(user, date)] = 0.0
            user_2days_view_skuid_dict[(user, date)] = 0.0

            user_5days_add2car_skuid_dict[(user, date)] = 0.0
            user_5days_click_skuid_dict[(user, date)] = 0.0
            user_5days_view_skuid_dict[(user, date)] = 0.0

            user_day_diff_sku_add2car_skuid_dict[(user,date)] = set()
            user_day_diff_sku_click_skuid_dict[(user, date)] = set()
            user_day_diff_sku_view_skuid_dict[(user, date)] = set()

            user_2days_diff_sku_add2car_skuid_dict[(user,date)] = set()
            user_2days_diff_sku_click_skuid_dict[(user, date)] = set()
            user_2days_diff_sku_view_skuid_dict[(user, date)] = set()

            user_5days_diff_sku_add2car_skuid_dict[(user, date)] = set()
            user_5days_diff_sku_click_skuid_dict[(user, date)] = set()
            user_5days_diff_sku_view_skuid_dict[(user, date)] = set()


            # 用户每天/最近2天 动作多少/不同 brand
            # 感觉统计用户对每个品牌分别动作了多少次没意义，就没统计
            user_day_diff_sku_add2car_brand_dict[(user, date)] = set()
            user_day_diff_sku_click_brand_dict[(user, date)] = set()
            user_day_diff_sku_view_brand_dict[(user, date)] = set()

            user_2days_diff_sku_add2car_brand_dict[(user, date)] = set()
            user_2days_diff_sku_click_brand_dict[(user, date)] = set()
            user_2days_diff_sku_view_brand_dict[(user, date)] = set()

            user_5days_diff_sku_add2car_brand_dict[(user, date)] = set()
            user_5days_diff_sku_click_brand_dict[(user, date)] = set()
            user_5days_diff_sku_view_brand_dict[(user, date)] = set()


    #遍历action并得到每个商品每天被动作数量以及用户每天动作数
    for row in action_data_array:
        user_id=row[0]
        sku_id=row[1]
        action_date=str(row[2]).split(" ")[0].strip()
        type=int(row[4])
        brand=row[6]

        # 添加到购物车。
        if type == 2:
            user_day_add2car_skuid_dict[(user_id, action_date)] += 1
            user_day_diff_sku_add2car_skuid_dict[(user_id, action_date)].add(sku_id)

            user_day_diff_sku_add2car_brand_dict[(user_id, action_date)].add(brand)
        # 点击。
        if type == 6:
            user_day_click_skuid_dict[(user_id, action_date)] += 1
            user_day_diff_sku_click_skuid_dict[(user_id, action_date)].add(sku_id)

            user_day_diff_sku_click_brand_dict[(user_id, action_date)].add(brand)
        # 浏览。
        if type == 1:
            user_day_view_skuid_dict[(user_id, action_date)] += 1
            user_day_diff_sku_view_skuid_dict[(user_id, action_date)].add(sku_id)

            user_day_diff_sku_view_brand_dict[(user_id, action_date)].add(brand)


    #遍历user每天特征获取user滑动特征
    for user in user_id_list:
        for date in get_all_date():
            #前2天
            recent_days,days_number = get_recent_days(date,2)
            for today in recent_days:
                user_2days_add2car_skuid_dict[(user,date)] += user_day_add2car_skuid_dict[(user,today)]
                user_2days_click_skuid_dict[(user, date)] += user_day_click_skuid_dict[(user, today)]
                user_2days_view_skuid_dict[(user, date)] += user_day_view_skuid_dict[(user, today)]

                for ele in user_day_diff_sku_add2car_skuid_dict[(user,today)]:
                    user_2days_diff_sku_add2car_skuid_dict[(user,date)].add(ele)
                for ele in user_day_diff_sku_click_skuid_dict[(user, today)]:
                    user_2days_diff_sku_click_skuid_dict[(user, date)].add(ele)
                for ele in user_day_diff_sku_view_skuid_dict[(user, today)]:
                    user_2days_diff_sku_view_skuid_dict[(user, date)].add(ele)

                for ele in user_day_diff_sku_add2car_brand_dict[(user,today)]:
                    user_2days_diff_sku_add2car_brand_dict[(user,date)].add(ele)
                for ele in user_day_diff_sku_click_brand_dict[(user, today)]:
                    user_2days_diff_sku_click_brand_dict[(user, date)].add(ele)
                for ele in user_day_diff_sku_view_brand_dict[(user, today)]:
                    user_2days_diff_sku_view_brand_dict[(user, date)].add(ele)


            if days_number != 0:
                user_2days_add2car_skuid_dict[(user, date)] /= days_number
                user_2days_click_skuid_dict[(user, date)] /= days_number
                user_2days_view_skuid_dict[(user, date)] /= days_number

            else:
                pass

            #前5天
            recent_days,days_number = get_recent_days(date,5)
            for today in recent_days:
                user_5days_add2car_skuid_dict[(user,date)] += user_day_add2car_skuid_dict[(user,today)]
                user_5days_click_skuid_dict[(user, date)] += user_day_click_skuid_dict[(user, today)]
                user_5days_view_skuid_dict[(user, date)] += user_day_view_skuid_dict[(user, today)]

                for ele in user_day_diff_sku_add2car_skuid_dict[(user,today)]:
                    user_5days_diff_sku_add2car_skuid_dict[(user,date)].add(ele)
                for ele in user_day_diff_sku_click_skuid_dict[(user, today)]:
                    user_5days_diff_sku_click_skuid_dict[(user, date)].add(ele)
                for ele in user_day_diff_sku_view_skuid_dict[(user, today)]:
                    user_5days_diff_sku_view_skuid_dict[(user, date)].add(ele)

                for ele in user_day_diff_sku_add2car_brand_dict[(user,today)]:
                    user_5days_diff_sku_add2car_brand_dict[(user,date)].add(ele)
                for ele in user_day_diff_sku_click_brand_dict[(user, today)]:
                    user_5days_diff_sku_click_brand_dict[(user, date)].add(ele)
                for ele in user_day_diff_sku_view_brand_dict[(user, today)]:
                    user_5days_diff_sku_view_brand_dict[(user, date)].add(ele)


            if days_number != 0:
                user_5days_add2car_skuid_dict[(user, date)] /= days_number
                user_5days_click_skuid_dict[(user, date)] /= days_number
                user_5days_view_skuid_dict[(user, date)] /= days_number

            else:
                pass

#---------------------------------------遍历action表，将所有特征都加上，然后写入文件-----------------------------------------#
    all_feature_list=[]
    for row in action_data_array:
        temp_list=[]
        #action 表特征
        user_id=row[0]
        temp_list.append(user_id)
        action_date=str(row[2]).split(" ")[0].strip()
        temp_list.append(action_date)
        type=int(row[4])
        # label=-1
        label=0#好像是有的模型需要-1
        if type==4:
            label=1

        #其他三个表特征
        user_info=user_info_dict[user_id]
        temp_list.extend(user_info)
        #统计特征

        user_2days_click_skuid=user_2days_click_skuid_dict[(user_id,action_date)]
        user_2days_add2car_skuid = user_2days_add2car_skuid_dict[(user_id, action_date)]
        user_2days_view_skuid = user_2days_view_skuid_dict[(user_id, action_date)]
        temp_list.append(user_2days_click_skuid)
        temp_list.append(user_2days_add2car_skuid)
        temp_list.append(user_2days_view_skuid)

        user_5days_click_skuid = user_5days_click_skuid_dict[(user_id, action_date)]
        user_5days_add2car_skuid = user_5days_add2car_skuid_dict[(user_id, action_date)]
        user_5days_view_skuid = user_5days_view_skuid_dict[(user_id, action_date)]
        temp_list.append(user_5days_click_skuid)
        temp_list.append(user_5days_add2car_skuid)
        temp_list.append(user_5days_view_skuid)


        user_2days_diff_sku_click_skuid=len(user_2days_diff_sku_click_skuid_dict[(user_id,action_date)])
        user_2days_diff_sku_add2car_skuid = len(user_2days_diff_sku_add2car_skuid_dict[(user_id, action_date)])
        user_2days_diff_sku_view_skuid = len(user_2days_diff_sku_view_skuid_dict[(user_id, action_date)])
        temp_list.append(user_2days_diff_sku_click_skuid)
        temp_list.append(user_2days_diff_sku_add2car_skuid)
        temp_list.append(user_2days_diff_sku_view_skuid)

        user_5days_diff_sku_click_skuid = len(user_5days_diff_sku_click_skuid_dict[(user_id, action_date)])
        user_5days_diff_sku_add2car_skuid = len(user_5days_diff_sku_add2car_skuid_dict[(user_id, action_date)])
        user_5days_diff_sku_view_skuid = len(user_5days_diff_sku_view_skuid_dict[(user_id, action_date)])
        temp_list.append(user_5days_diff_sku_click_skuid)
        temp_list.append(user_5days_diff_sku_add2car_skuid)
        temp_list.append(user_5days_diff_sku_view_skuid)

        user_2days_diff_sku_click_brand = len(user_2days_diff_sku_click_brand_dict[(user_id, action_date)])
        user_2days_diff_sku_add2car_brand = len(user_2days_diff_sku_add2car_brand_dict[(user_id, action_date)])
        user_2days_diff_sku_view_brand = len(user_2days_diff_sku_view_brand_dict[(user_id, action_date)])
        temp_list.append(user_2days_diff_sku_click_brand)
        temp_list.append(user_2days_diff_sku_add2car_brand)
        temp_list.append(user_2days_diff_sku_view_brand)

        user_5days_diff_sku_click_brand = len(user_5days_diff_sku_click_brand_dict[(user_id, action_date)])
        user_5days_diff_sku_add2car_brand = len(user_5days_diff_sku_add2car_brand_dict[(user_id, action_date)])
        user_5days_diff_sku_view_brand = len(user_5days_diff_sku_view_brand_dict[(user_id, action_date)])
        temp_list.append(user_5days_diff_sku_click_brand)
        temp_list.append(user_5days_diff_sku_add2car_brand)
        temp_list.append(user_5days_diff_sku_view_brand)


        #label
        temp_list.append(label)
        all_feature_list.append(temp_list)

    #将所有特征写入文件
    all_feature_df=pd.DataFrame(all_feature_list)
    all_feature_df.to_csv(write_path,index=False,header=False)


    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
data_dir="/public/home/scu1701/JData/Data3/"#server data path
# data_dir="/home/wangtuntun/JData/Data/" #local data path
# dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path
# action_data_path = dealed_data_dir + "234month_skuid_filtered_10.csv"  # 234三个月所有以交互数据
# write_path = dealed_data_dir + "all_feature_01_label_10_user.csv"
# get_feature(action_data_path,write_path)
