#encoding=utf-8
import pandas as pd
import numpy as np
import datetime
import gc
from datetime import timedelta
import requests
import json

# 获取数据中包含的所有天
def get_all_date():
    return_list=[]
    year="2016-"
    return_list.append("2016-03-31")
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
    deadline=datetime.datetime.strptime("2016-04-01", '%Y-%m-%d')
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

#获取每天的假期信息。工作日为0,假期为1或者2.
# def if_holiday():
    # all_dates_list=get_all_date()
    # holiday_str=requests.get('http://www.easybots.cn/api/holiday.php?d=%s' % all_dates_list).content
    # holiday_json=json.loads(holiday_str)
    # return holiday_json

def if_holiday(info_path):
    f_read = open(info_path, "r+")
    info = f_read.readline()
    f_read.close()
    json_object = json.loads(info)
    return json_object

def analize_feature(predict_0415_result_path):
    import time
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
#------------------------------------------------------------读取文件信息------------------------------------------------#
    action_data_path = dealed_data_dir + "4month_skuid_filtered.csv"#234三个月所有以交互数据,并且样本经过均衡。
    product_path = data_dir + "JData_Product2.csv"#商品信息     只保留出现在action表中的商品
    user_path=data_dir + "JData_User2.csv" #用户信息;       将年龄字段有"16-25岁"改为1
    comment_path=data_dir + "JData_Comment.csv"#商品评价信息

    comment_dates= ['2016-04-15', '2016-04-11', '2016-04-04', '2016-03-28', '2016-03-21', '2016-03-14', '2016-03-07', '2016-02-29', '2016-02-22', '2016-02-15', '2016-02-08', '2016-02-01']
    holiday_info_path="/public/home/scu1701/JData/holiday_info.txt"
    is_holiday_json = if_holiday(holiday_info_path)

    action_data_4 = pd.read_csv(action_data_path,parse_dates=[2])
    action_data_4["user_id"] = action_data_4["user_id"].astype(int)
    action_data_4.drop("model_id",axis=1, inplace=True)
    action_data_4.drop("cate", axis=1, inplace=True)
    sku_id_list = action_data_4["sku_id"].tolist()  # 4月份出现的商品
    user_id_list = action_data_4["user_id"].tolist()  # 4月份出现的商品
    pair_list=zip(user_id_list,sku_id_list)
    pair_list_set_list=list(set(pair_list))
    user_id_set_list = list(set(user_id_list))
    sku_id_set_list = list(set(sku_id_list))

    action_data_array=np.array(action_data_4)
    del action_data_4
    gc.collect()

    product_data=pd.read_csv(product_path)
    product_data.drop("cate", axis=1, inplace=True)#cate字段不要，因为都是8
    product_data_array=np.array(product_data)
    del product_data
    gc.collect()
    comment_data=pd.read_csv(comment_path)
    comment_data_array=np.array(comment_data)
    del comment_data
    gc.collect()
    user_data = pd.read_csv(user_path)
    user_data_array=np.array(user_data)
    del user_data
    gc.collect()

    date_list = get_all_date()  # 训练集中出现的所有日期

    # 预测时使用的pair
    action_data = pd.read_csv(action_data_path, parse_dates=[2])
    filtered_data_5days = action_data[action_data["time"] >= datetime.datetime.strptime("2016-04-15", '%Y-%m-%d')]
    filtered_skuid = filtered_data_5days["sku_id"].tolist()
    filtered_userid = filtered_data_5days["user_id"].tolist()
    user_sku_list_zip = zip(filtered_userid, filtered_skuid)
    user_sku_list_zip_set = set(user_sku_list_zip)


#-----------------------------------------------------------获取各种映射表-----------------------------------------------#
    #------------三张表的映射--------------------------#
    user_info_dict = {}
    for row in user_data_array:
        user_id = int(row[0])
        user_info_dict[user_id] = list(row[1:5])

    product_info_dict = {}
    for row in product_data_array:
        sku_id = int(row[0])
        product_info_dict[sku_id] = list(row[1:5])#本来应该是1-6,但是去掉了cate字段，所以就少了一个属性


    comment_info_dict = {}
    for row in comment_data_array:
        sku_id = int(row[1])
        date = row[0]
        comment_info_dict[(sku_id, date)] = list(row[2:5])

    # ------------新添加特征---------------------------#
    #该user对该商品的交叉特征
    user_sku_action_dict = {}#该用户对该商品所有天总的动作次数

    #商品转化率
    sku_total_views = {}  #该商品总销量
    sku_total_clicks = {}  # 该商品总销量
    sku_total_add2car = {}  # 该商品总销量
    sku_total_sales = {}  # 该商品总销量
    #商品假期与工作日特征
    sku_total_workday_views = {}
    sku_total_workday_clicks = {}
    sku_total_workday_add2car = {}
    sku_total_workday_sales = {}

    sku_total_holiday_views = {}
    sku_total_holiday_clicks = {}
    sku_total_holiday_add2car = {}
    sku_total_holiday_sales = {}

    #用户总的动作次数
    user_total_actions = {}
    #用户假期与工作日动作次数
    user_total_workday_action = {}
    user_total_holiday_action = {}


    #------------每个sku被动作-------------------------#
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
    # 初始化 商品最近5天被动作次数
    sku_5days_buy_dict = {}
    sku_5days_click_dict = {}
    sku_5days_add2car_dict = {}
    sku_5days_view_dict = {}
    # 初始化 商品最近5天被不同的用户动作次数
    sku_5days_diff_user_buy_dict = {}
    sku_5days_diff_user_click_dict = {}
    sku_5days_diff_user_add2car_dict = {}
    sku_5days_diff_user_view_dict = {}

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


    # ------------新添加特征---------------------------#
    #初始化交叉特征
    for pair in pair_list_set_list:
        user_sku_action_dict[pair] = 1.0
    for skuid in sku_id_set_list:
        sku_total_sales[skuid] = 1.0
        sku_total_views[skuid] = 1.0
        sku_total_add2car[skuid] = 1.0
        sku_total_clicks[skuid] = 1.0
        sku_total_workday_sales[skuid] = 1.0
        sku_total_workday_views[skuid] = 1.0
        sku_total_workday_add2car[skuid] = 1.0
        sku_total_workday_clicks[skuid] = 1.0
        sku_total_holiday_sales[skuid] = 1.0
        sku_total_holiday_views[skuid] = 1.0
        sku_total_holiday_add2car[skuid] = 1.0
        sku_total_holiday_clicks[skuid] = 1.0
    for userid in user_id_set_list:
        user_total_actions[userid] = 1.0
        user_total_workday_action[userid] = 1.0
        user_total_holiday_action[userid] = 1.0

    #初始化sku
    for date in date_list:
        for sku in sku_id_set_list:
            sku=int(sku)
            sku_day_buy_dict[(sku,date)] = 0.0 # 每个商品每天被买了多少次
            sku_day_add2car_dict[(sku,date)] = 0.0 # 每天被添加到购物车次数
            sku_day_click_dict[(sku, date)] = 0.0 # 每天点击次数
            sku_day_view_dict[(sku, date)] = 0.0  # 每天被浏览
            sku_2days_buy_dict[(sku, date)] = 0.0  # 每个商品最近2天平均被动作了多少次
            sku_2days_click_dict[(sku, date)] = 0.0
            sku_2days_add2car_dict[(sku, date)] = 0.0
            sku_2days_view_dict[(sku, date)] = 0.0
            sku_5days_buy_dict[(sku, date)] = 0.0  # 每个商品最近5天平均被动作了多少次
            sku_5days_click_dict[(sku, date)] = 0.0
            sku_5days_add2car_dict[(sku, date)] = 0.0
            sku_5days_view_dict[(sku, date)] = 0.0

            sku_day_diff_user_buy_dict[(sku, date)] = set()#每个商品每天被哪些不同用户动作
            sku_day_diff_user_add2car_dict[(sku, date)] = set()
            sku_day_diff_user_click_dict[(sku, date)] = set()
            sku_day_diff_user_view_dict[(sku, date)] = set()
            sku_2days_diff_user_buy_dict[(sku,date)]=set()#每个商品最近2天被哪些不同用户动作
            sku_2days_diff_user_add2car_dict[(sku, date)] = set()
            sku_2days_diff_user_click_dict[(sku, date)] = set()
            sku_2days_diff_user_view_dict[(sku, date)] = set()
            sku_5days_diff_user_buy_dict[(sku, date)] = set()  # 每个商品最近5天被哪些不同用户动作
            sku_5days_diff_user_add2car_dict[(sku, date)] = set()
            sku_5days_diff_user_click_dict[(sku, date)] = set()
            sku_5days_diff_user_view_dict[(sku, date)] = set()

    # #初始化user
    for date in date_list:
        for user in user_id_set_list:
            user=int(user)
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
        user_id=int(row[0])
        sku_id=int(row[1])
        action_date=str(row[2]).split(" ")[0].strip()
        type=int(row[3])
        brand=row[4]
        action_date_holiday_list=action_date.split("-")
        action_date_holiday = "" + action_date_holiday_list[0] + action_date_holiday_list[1] + action_date_holiday_list[2]
        is_holiday=int(is_holiday_json[action_date_holiday])
        #用户总的动作数
        user_total_actions[user_id] += 1
        if is_holiday != 0:
            user_total_holiday_action[user_id] += 1
        else:
            user_total_workday_action[user_id] += 1

        # 购买。由于每个用户只买一次，所以没有用户购买次数的特征。只有商品被购买特征。
        if type== 4 :
            sku_day_buy_dict[(sku_id, action_date)] += 1
            sku_day_diff_user_buy_dict[(sku_id, action_date)].add(user_id)

            #商品总的被购买数
            sku_total_sales[sku_id] += 1 #该商品的总销量加一
            if is_holiday != 0:
                sku_total_holiday_sales[sku_id] += 1
            else:
                sku_total_workday_sales [sku_id] += 1

        # 添加到购物车。
        if type == 2:
            sku_day_add2car_dict[(sku_id, action_date)] += 1
            sku_day_diff_user_add2car_dict[(sku_id, action_date)].add(user_id)

            user_day_add2car_skuid_dict[(user_id, action_date)] += 1
            user_day_diff_sku_add2car_skuid_dict[(user_id, action_date)].add(sku_id)

            user_day_diff_sku_add2car_brand_dict[(user_id, action_date)].add(brand)

            sku_total_add2car[sku_id] += 1
            if is_holiday != 0:
                sku_total_holiday_add2car[sku_id] += 1
            else:
                sku_total_workday_add2car[sku_id] += 1

        # 点击。
        if type == 6:
            sku_day_click_dict[(sku_id, action_date)] += 1
            sku_day_diff_user_click_dict[(sku_id, action_date)].add(user_id)

            user_day_click_skuid_dict[(user_id, action_date)] += 1
            user_day_diff_sku_click_skuid_dict[(user_id, action_date)].add(sku_id)

            user_day_diff_sku_click_brand_dict[(user_id, action_date)].add(brand)

            sku_total_clicks[sku_id] += 1
            if is_holiday != 0:
                sku_total_holiday_clicks[sku_id] += 1
            else:
                sku_total_workday_clicks[sku_id] += 1
        # 浏览。
        if type == 1:
            sku_day_view_dict[(sku_id, action_date)] += 1
            sku_day_diff_user_view_dict[(sku_id, action_date)].add(user_id)

            user_day_view_skuid_dict[(user_id, action_date)] += 1
            user_day_diff_sku_view_skuid_dict[(user_id, action_date)].add(sku_id)

            user_day_diff_sku_view_brand_dict[(user_id, action_date)].add(brand)

            sku_total_views[sku_id] += 1
            if is_holiday != 0:
                sku_total_holiday_views[sku_id] += 1
            else:
                sku_total_workday_views[sku_id] += 1

    # #遍历sku每天特征获取sku滑动时间特征
    for sku in sku_id_set_list:#每个商家
        for date in get_all_date():#每天对应的最近x天
            sku=int(sku)
            #前2天
            recent_days,days_number=get_recent_days(date,2)
            for today in recent_days:
                # 每个商家每天对应的最近两天的动作数，就是该商家前2天每天动作数的累加
                sku_2days_click_dict[(sku, date)] += sku_day_click_dict[(sku,today)]
                sku_2days_buy_dict[(sku, date)] += sku_day_buy_dict[(sku, today)]
                sku_2days_add2car_dict[(sku, date)] += sku_day_add2car_dict[(sku, today)]
                sku_2days_view_dict[(sku, date)] += sku_day_view_dict[(sku, today)]
                # 每个商家前2天被哪些用户动作
                for ele in sku_day_diff_user_buy_dict[(sku, today)]:
                    sku_2days_diff_user_buy_dict[(sku, date)].add(ele)
                for ele in sku_day_diff_user_add2car_dict[(sku, today)]:
                    sku_2days_diff_user_add2car_dict[(sku, date)].add(ele)
                for ele in sku_day_diff_user_click_dict[(sku, today)]:
                    sku_2days_diff_user_click_dict[(sku, date)].add(ele)
                for ele in sku_day_diff_user_view_dict[(sku, today)]:
                    sku_2days_diff_user_view_dict[(sku, date)].add(ele)

            if days_number != 0:#对于2/1日当天的数据，就直接取当天的。其他的有几天就取几天的平均值。避免截断误差。
                sku_2days_click_dict[(sku, date)] = sku_2days_click_dict[(sku, date)] / float(days_number)
                sku_2days_buy_dict[(sku, date)] = sku_2days_buy_dict[(sku, date)] / float(days_number)
                sku_2days_add2car_dict[(sku, date)] = sku_2days_add2car_dict[(sku, date)] / float(days_number)
                sku_2days_view_dict[(sku, date)] = sku_2days_view_dict[(sku, date)] / float(days_number)
            else:
                pass

            # 最近5天
            recent_days, days_number = get_recent_days(date, 5)
            for today in recent_days:
                # 每个商家每天对应的最近两天的动作数，就是该商家最近5天动作数的累加
                sku_5days_click_dict[(sku, date)] += sku_day_click_dict[(sku,today)]
                sku_5days_buy_dict[(sku, date)] += sku_day_buy_dict[(sku, today)]
                sku_5days_add2car_dict[(sku, date)] += sku_day_add2car_dict[(sku, today)]
                sku_5days_view_dict[(sku, date)] += sku_day_view_dict[(sku, today)]
                # 每个商家最近5天被哪些用户动作
                for ele in sku_day_diff_user_buy_dict[(sku, today)]:
                    sku_5days_diff_user_buy_dict[(sku, date)].add(ele)
                for ele in sku_day_diff_user_add2car_dict[(sku, today)]:
                    sku_5days_diff_user_add2car_dict[(sku, date)].add(ele)
                for ele in sku_day_diff_user_click_dict[(sku, today)]:
                    sku_5days_diff_user_click_dict[(sku, date)].add(ele)#----------------在这儿提示内存错误-------------------------------------------------
                for ele in sku_day_diff_user_view_dict[(sku, today)]:
                    sku_5days_diff_user_view_dict[(sku, date)].add(ele)

            if days_number != 0:#对于2/1日当天的数据，就直接取当天的。其他的有几天就取几天的平均值
                sku_5days_click_dict[(sku, date)] = sku_5days_click_dict[(sku, date)] / float(days_number)
                sku_5days_buy_dict[(sku, date)] = sku_5days_buy_dict[(sku, date)] / float(days_number)
                sku_5days_add2car_dict[(sku, date)] = sku_5days_add2car_dict[(sku, date)] / float(days_number)
                sku_5days_view_dict[(sku, date)] = sku_5days_view_dict[(sku, date)] / float(days_number)
            else:
                pass

    #遍历user每天特征获取user滑动特征
    for user in user_id_set_list:
        for date in get_all_date():
            user=int(user)
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

# ---------------------------------------遍历action表，得到交叉特征统计数据-----------------------------------------#
    for row in action_data_array:
        user_id,sku_id=int(row[0]),int(row[1])
        user_sku_action_dict[(user_id,sku_id)] += 1


#---------------------------------------遍历所有userid和skuid，生成4/16当天的数据-----------------------------------------#
    # predict_0410_list=[]
    predict_0415_list = []
    for user_id,sku_id in user_sku_list_zip_set:
        action_date = "2016-04-15"
        predict_date = "2016-04-16"
        temp_list = []
        temp_list.append(user_id)
        temp_list.append(sku_id)
        temp_list.append(predict_date)  # 这里放入的不是0410,而是0411
        sku_info = product_info_dict[sku_id]  # 提示key error
        user_info = user_info_dict[user_id]
        comment_info = [0, 0, 0.0]  # 如果action表中，(sku_id,dt)对应的没有出现在comment表中，就用这个默认的进行填充
        user_comment_info = [0, 0, 0.0]
        comemnt_flag = 0
        for comemen_date in comment_dates:#comment 表的意思是，截止到某天的，这里的处理方式：只要是这个用户的信息就好，日期越靠后越好。
            try:
                comment_info = comment_info_dict[(sku_id, comemen_date)]  # 提示key error
                if int(comment_info[0]) != 0 and comemnt_flag != 1:
                    user_comment_info = comment_info
                    comemnt_flag = 1
            except:
                pass

        temp_list.extend(user_info)
        temp_list.extend(sku_info)
        temp_list.extend(user_comment_info)
        # 统计特征
        sku_2days_buy = sku_2days_buy_dict[(sku_id, action_date)]
        sku_2days_click = sku_2days_click_dict[(sku_id, action_date)]
        sku_2days_add2car = sku_2days_add2car_dict[(sku_id, action_date)]
        sku_2days_view = sku_2days_view_dict[(sku_id, action_date)]
        temp_list.append(sku_2days_buy)
        temp_list.append(sku_2days_click)
        temp_list.append(sku_2days_add2car)
        temp_list.append(sku_2days_view)

        sku_2days_diff_user_buy = len(sku_2days_diff_user_buy_dict[(sku_id, action_date)])  # 这个没有取平均值，会造成截断误差
        sku_2days_diff_user_click = len(sku_2days_diff_user_click_dict[(sku_id, action_date)])
        sku_2days_diff_user_add2car = len(sku_2days_diff_user_add2car_dict[(sku_id, action_date)])
        sku_2days_diff_user_view = len(sku_2days_diff_user_view_dict[(sku_id, action_date)])
        temp_list.append(sku_2days_diff_user_buy)
        temp_list.append(sku_2days_diff_user_click)
        temp_list.append(sku_2days_diff_user_add2car)
        temp_list.append(sku_2days_diff_user_view)

        sku_5days_buy = sku_5days_buy_dict[(sku_id, action_date)]
        sku_5days_click = sku_5days_click_dict[(sku_id, action_date)]
        sku_5days_add2car = sku_5days_add2car_dict[(sku_id, action_date)]
        sku_5days_view = sku_5days_view_dict[(sku_id, action_date)]
        temp_list.append(sku_5days_buy)
        temp_list.append(sku_5days_click)
        temp_list.append(sku_5days_add2car)
        temp_list.append(sku_5days_view)

        sku_5days_diff_user_buy = len(sku_5days_diff_user_buy_dict[(sku_id, action_date)])
        sku_5days_diff_user_click = len(sku_5days_diff_user_click_dict[(sku_id, action_date)])
        sku_5days_diff_user_add2car = len(sku_5days_diff_user_add2car_dict[(sku_id, action_date)])
        sku_5days_diff_user_view = len(sku_5days_diff_user_view_dict[(sku_id, action_date)])
        temp_list.append(sku_5days_diff_user_buy)
        temp_list.append(sku_5days_diff_user_click)
        temp_list.append(sku_5days_diff_user_add2car)
        temp_list.append(sku_5days_diff_user_view)

        user_2days_click_skuid = user_2days_click_skuid_dict[(user_id, action_date)]
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

        user_2days_diff_sku_click_skuid = len(user_2days_diff_sku_click_skuid_dict[(user_id, action_date)])
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

        # ---------------------新加入特征--------------------------------------------------------------------#
        # 交叉特征
        temp_list.append(user_sku_action_dict[(user_id, sku_id)])
        # 用户总动作次数
        user_actions = user_total_actions[user_id]
        user_holiday_action = user_total_holiday_action[user_id]
        user_workday_action = user_total_workday_action[user_id]
        temp_list.append(user_actions)
        temp_list.append(user_holiday_action)
        temp_list.append(user_workday_action)
        # 商品总销量
        sales = sku_total_sales[sku_id]
        clicks = sku_total_clicks[sku_id]
        add2cars = sku_total_add2car[sku_id]
        views = sku_total_views[sku_id]
        temp_list.append(views)
        temp_list.append(clicks)
        temp_list.append(add2cars)
        temp_list.append(sales)
        workday_views = sku_total_workday_views[sku_id]
        workday_clicks = sku_total_workday_clicks[sku_id]
        workday_add2car = sku_total_workday_add2car[sku_id]
        workday_sales = sku_total_workday_sales[sku_id]
        holiday_views = sku_total_holiday_views[sku_id]
        holiday_clicks = sku_total_holiday_clicks[sku_id]
        holiday_add2car = sku_total_holiday_add2car[sku_id]
        holiday_sales = sku_total_holiday_sales[sku_id]
        temp_list.append(workday_views)
        temp_list.append(workday_clicks)
        temp_list.append(workday_add2car)
        temp_list.append(workday_sales)
        temp_list.append(holiday_views)
        temp_list.append(holiday_clicks)
        temp_list.append(holiday_add2car)
        temp_list.append(holiday_sales)
        # 转化率
        sku_click_buy_rate = sales / clicks
        sku_view_buy_rate = sales / views
        sku_add2car_buy_rate = sales / add2cars
        temp_list.append(sku_click_buy_rate)
        temp_list.append(sku_view_buy_rate)
        temp_list.append(sku_add2car_buy_rate)

        # 曜日
        date_temp = datetime.datetime.strptime(action_date, '%Y-%m-%d')
        weekday = date_temp.weekday()
        temp_list.append(weekday)
        # 是否为假期
        action_date_holiday_list = action_date.split("-")
        action_date_holiday = "" + action_date_holiday_list[0] + action_date_holiday_list[1] + action_date_holiday_list[2]
        is_holiday = int(is_holiday_json[action_date_holiday])
        temp_list.append(is_holiday)

        print(user_actions, user_holiday_action, user_workday_action, views, clicks, add2cars, sales, workday_views,
              workday_clicks, workday_add2car, workday_sales, holiday_views, holiday_clicks, holiday_add2car,
              holiday_sales, weekday, is_holiday)

        # label
        temp_list.append(1)


        predict_0415_list.append(temp_list)

    predict_0415_df=pd.DataFrame(predict_0415_list)
    predict_0415_df.to_csv(predict_0415_result_path,index=False,header=False)


    #释放内存
    sku_day_buy_dict = {}
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
    # 初始化 商品最近5天被动作次数
    sku_5days_buy_dict = {}
    sku_5days_click_dict = {}
    sku_5days_add2car_dict = {}
    sku_5days_view_dict = {}
    # 初始化 商品最近5天被不同的用户动作次数
    sku_5days_diff_user_buy_dict = {}
    sku_5days_diff_user_click_dict = {}
    sku_5days_diff_user_add2car_dict = {}
    sku_5days_diff_user_view_dict = {}

    # ------------每个用户动作---------------------------#
    # 下面的特征都是统计的sku_id
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

    # ------------每个brand被动作---------------------------#
    # 下面的特征都是统计的brand 品牌
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


    # 该user对该商品的交叉特征
    user_sku_action_dict = {}  # 该用户对该商品所有天总的动作次数
    # 商品总销量
    sku_total_sales = {}  # 该商品总销量
    # 用户总的动作次数
    user_total_actions = {}
    #商品转化率
    sku_total_views = {}  #该商品总销量
    sku_total_clicks = {}  # 该商品总销量
    sku_total_add2car = {}  # 该商品总销量
    sku_total_sales = {}  # 该商品总销量
    #商品假期与工作日特征
    sku_total_workday_views = {}
    sku_total_workday_clicks = {}
    sku_total_workday_add2car = {}
    sku_total_workday_sales = {}

    sku_total_holiday_views = {}
    sku_total_holiday_clicks = {}
    sku_total_holiday_add2car = {}
    sku_total_holiday_sales = {}

    #用户总的动作次数
    user_total_actions = {}
    #用户假期与工作日动作次数
    user_total_workday_action = {}
    user_total_holiday_action = {}



    gc.collect()
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
data_dir="/public/home/scu1701/JData/Data5/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData5/"#server dealed data path

write_dir=dealed_data_dir+"change_feature/"
write_path = write_dir + "add_feature_predict.csv"
analize_feature(write_path)