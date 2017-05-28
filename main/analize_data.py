#encoding=utf-8
import pandas as pd
import numpy as np
import datetime
import time
from datetime import timedelta
import  csv
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
'''
统计在action表中，有哪些skuid，userid。
本地运行时间一分钟
all userid 105321
userid in action 104740
all skuid 24186
skuid in action 3938
skuid相差太大，之前就过滤过一次，只保留skuid出现在product表中。当时没对product表进行过滤。
为了减少代码的运行时间，要对product表进行过滤，只保留出现在action表中的sku的信息
'''
def get_skuid_userid_in_action():
    user_info=pd.read_csv(data_dir+"JData_User.csv")
    userid_list=set(user_info["user_id"].tolist())
    sku_info=pd.read_csv(data_dir+"JData_Product.csv")
    skuid_list=set(sku_info["sku_id"].tolist())
    action_info=pd.read_csv(dealed_data_dir+"234month_skuid_filtered.csv")
    userid_in_action=set(action_info["user_id"].tolist())
    skuid_in_action=set(action_info["sku_id"].tolist())
    print("all userid"+str(len(userid_list)))
    print("userid in action"+str(len(userid_in_action)))
    print("all skuid" + str(len(skuid_list)))
    print("skuid in action" + str(len(skuid_in_action)))

'''
为了对三张表里的各个属性进行独热编码，需要了解每个属性的值的范围
age 0--->5
sex 0--->2
user_lv_cd 1--->5
attr1 -1--->3
attr2 -1--->2
attr3 -1--->2
brand 一共有40个不同的值
[30, 48, 83, 88, 90, 124, 174, 200, 209, 211, 214, 244, 306, 328, 355, 375, 403, 427, 484, 489, 515, 545, 556, 562, 596, 622, 623, 655, 658, 677, 693, 717, 766, 790, 800, 801, 812, 857, 885, 916]
comment_num 0--->4

'''
def get_tables_attr_value_range():
    user_info = pd.read_csv(data_dir + "JData_User2.csv")
    user_age=set(user_info["age"].tolist())
    user_age_sorted=sorted(user_age)
    print("age")
    print(user_age_sorted)
    sex = set(user_info["sex"].tolist())
    sex_sorted = sorted(sex)
    print("sex")
    print(sex_sorted)
    lv = set(user_info["user_lv_cd"].tolist())
    lv_sorted = sorted(lv)
    print("user_lv_cd")
    print(lv_sorted)

    sku_info = pd.read_csv(data_dir + "JData_Product2.csv")
    attr1 = set(sku_info["a1"].tolist())
    attr1_sorted = sorted(attr1)
    print("attr1")
    print(attr1_sorted)
    attr2 = set(sku_info["a2"].tolist())
    attr2_sorted = sorted(attr2)
    print("attr2")
    print(attr2_sorted)
    attr3 = set(sku_info["a3"].tolist())
    attr3_sorted = sorted(attr3)
    print("attr3")
    print(attr3_sorted)
    brand = set(sku_info["brand"].tolist())
    brand_sorted = sorted(brand)
    print("brand")
    print(brand_sorted)
    print(len(brand_sorted))

    comment_info=pd.read_csv(data_dir + "JData_Comment.csv")
    comment_num=set(comment_info["comment_num"].tolist())
    num_sorted=sorted(comment_num)
    print(num_sorted)

'''
最后发现：如果所有用户10w 所有商品（0.4w）都进行推荐（4亿），任务量比较大。需要进行筛选
用户筛选条件：用户总的动作次数以及注册时间（保留前1000） JData_User2_active_filtered.csv
商品筛选条件：被添加到购物车次数（保留前100） JData_Product2_hot_filtered.csv
        这样有个问题：有些商品刚刚上架。但是用户购买新商品的概率会比较低

#用户筛选条件暂时：次数大于2000或者注册时间天数小于30
#商品筛选条件：add2car排序前100

'''

def filter_user_sku():
    user_info = pd.read_csv(data_dir + "JData_User2.csv")
    user_id_list=user_info["user_id"].tolist()
    sku_info = pd.read_csv(data_dir + "JData_Product2.csv")
    sku_id_list=sku_info["sku_id"].tolist()
    # 初始化dict
    user_reg_dt_dict={}
    user_action_number_dict={}
    sku_add2car_number_dict={}
    for ele in user_id_list:
        user_action_number_dict[ele]=0
        user_reg_dt_dict[ele]=0
    for ele in sku_id_list:
        sku_add2car_number_dict[ele]=0
    #遍历user表获取注册天数
    end_date=datetime.datetime.strptime("2016-04-16", '%Y-%m-%d')
    user_info_list=np.array(user_info)
    for row in user_info_list:
        user_id = row[0]
        reg_date_str = str(row[4])
        try:
            reg_date=datetime.datetime.strptime(reg_date_str, '%Y-%m-%d')
            dealt_time=end_date - reg_date
            user_reg_dt_dict[user_id]=dealt_time.days
        except:
            # print(user_id)#有三个用户没有注册信息 234073 238906 267705
            pass
    # #遍历action获得两个词典的值
    action_raw_info=csv.reader(open(dealed_data_dir+"234month_skuid_filtered.csv"))
    action_info=[]
    for ele in action_raw_info:
        action_info.append(ele)
    action_info=action_info[1:len(action_info)]
    for row in action_info:
        user_id=int(row[0])
        sku_id=int(row[1])
        type=int(row[4])
        user_action_number_dict[user_id] += 1
        if type== 4:
            sku_add2car_number_dict[sku_id] += 1
    #筛选用户
    user_action_number_dict_sorted = sorted(user_action_number_dict.items(),key=lambda item:item[1],reverse=True)
    action_filtered_user=[]
    for ele in user_action_number_dict_sorted:
        action_filtered_user.append(ele)
    action_reg_filtered_userid=[]
    for user in user_id_list:
        # if user_reg_dt_dict[user] >100 and user_action_number_dict[user] <1000: #7771
        if user_action_number_dict[user] > 200:#2000 30 条件得到的用户有1189
                                                #200 30 ->29160
            action_reg_filtered_userid.append(user)
        elif user_reg_dt_dict[user] < 30:
            action_reg_filtered_userid.append(user)
    # print(len(action_reg_filtered_userid))
    # filtered_userid=action_reg_filtered_userid[0:5000]
    # filtered_userid = action_reg_filtered_userid[5000:10000]
    # filtered_userid = action_reg_filtered_userid[10000:15000]
    filtered_userid = action_reg_filtered_userid[15000:20000]
    # 筛选商品
    sku_add2car_number_dict_sorted=sorted(sku_add2car_number_dict.items(),key=lambda item:item[1],reverse=True)
    filtered_skuid=[]
    for ele in sku_add2car_number_dict_sorted:
        filtered_skuid.append(ele[0])
    filtered_skuid=filtered_skuid[0:3000]#总长度 3938
    # print(len(filtered_skuid))
    user_info_filter=user_info[user_info["user_id"].isin(filtered_userid)]
    sku_info_filter=sku_info[sku_info["sku_id"].isin(filtered_skuid)]
    user_info_filter.to_csv(data_dir+"JData_User2_active_filtered4.csv",index=False)
    sku_info_filter.to_csv(data_dir+"JData_Product2_hot_filtered.csv",index=False)

#看下提交结果中，有多少sku
def test_result():
    filtered_sku=pd.read_csv("/home/wangtuntun/PycharmProjects/JData/add_features/submit_xgb_4200.csv")
    filtered_skuid=set(filtered_sku["sku_id"].tolist())
    userid_set=set(filtered_sku["user_id"].tolist())
    print(len(userid_set),len(filtered_skuid))

#看下最近x天，有多少sku和user 和 pair
#最近5天 (2782, 40255,198352)
#最近15天 (3077, 64445, 439163)
def last_ndays_active_sku_user():
    raw_path = dealed_data_dir + "234month_skuid_filtered.csv"
    raw_data=pd.read_csv(raw_path,parse_dates=[2])
    # raw_data_5days=raw_data[raw_data["time"] > datetime.datetime.strptime("2016-04-10",'%Y-%m-%d')]
    raw_data_ndays = raw_data[raw_data["time"] >= datetime.datetime.strptime("2016-04-01", '%Y-%m-%d')]
    #get sku and user
    sku_id_set=set(raw_data_ndays["sku_id"].tolist())
    user_id_set=set(raw_data_ndays["user_id"].tolist())
    #get pair
    sku_id_list=raw_data_ndays["sku_id"].tolist()
    user_id_list=raw_data_ndays["user_id"].tolist()
    user_sku_list_zip=zip(user_id_list,sku_id_list)
    user_sku_list_zip_set=set(user_sku_list_zip)

    print(len(sku_id_set),len(user_id_set),len(user_sku_list_zip_set))

test_result()