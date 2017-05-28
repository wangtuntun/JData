#encoding=utf-8
'''
author:wangtuntun
date:2017/4/16
将原生的特征数据，转换为模型需要的数据格式，具体如下：
对于userid和skuid的处理:

进行独热编码操作时，每个特征的取值范围都不同，是否需要逐个对特征进型编码：

对于日期的处理：
    包括action中的time和user表中的user_reg_dt(comment表中的时间没有加入到特征列表中来)
    将月，日转为独热编码（好像星期没有影响）
对于三张表中的信息的处理:
    age,sex,user_lv_cd,attr1,attr2,attr3需要独热编码。
    brand怎么处理？
    comment_num独热编码
    has_bad_comment,bad_comemnt_rate 保持不变
统计特征的处理：ln(1+x)
三张表特征统计结果
age 0--->5
sex 0--->2
user_lv_cd 1--->5
attr1 -1--->3
attr2 -1--->2
attr3 -1--->2
brand 一共有40个不同的值
[30, 48, 83, 88, 90, 124, 174, 200, 209, 211, 214, 244, 306, 328, 355, 375, 403, 427, 484, 489, 515, 545, 556, 562, 596, 622, 623, 655, 658, 677, 693, 717, 766, 790, 800, 801, 812, 857, 885, 916]
comment_num 0--->4
ps:不能同时格式化两个文件，否则会因为超出时间限制而kill程序，d大概是两个小时（low queue）
'''
import numpy as np
import pandas as pd
import csv
import time
import datetime
import math

# data_dir="/public/home/scu1701/JData/Data/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData/"#server dealed data path
# data_dir="/home/wangtuntun/JData/Data/" #local data path
# dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path
# raw_path1=dealed_data_dir + "train_model_01_label.csv"
# result_path1=dealed_data_dir + "train_model_01_label_formatted"
# raw_path2=dealed_data_dir + "train_model_11_label.csv"
# result_path2=dealed_data_dir + "train_model_11_label_formatted"
# raw_path3=dealed_data_dir + "predict_0410_data"
# result_path3=dealed_data_dir + "predict_0410_data_formatted"
# raw_path4=dealed_data_dir + "all_feature_01_label_100.csv"
# result_path4=dealed_data_dir + "all_feature_01_label_100_formatted"
# raw_path5=dealed_data_dir + "all_feature_11_label_balanced.csv"
# result_path5=dealed_data_dir +"all_feature_11_label_balanced_formatted.csv"
# raw_path6=dealed_data_dir + "all_feature_01_label_balanced.csv"
# result_path6=dealed_data_dir +"all_feature_01_label_balanced_formatted.csv"
age_range=[0, 1, 2, 3, 4, 5]
sex_range=[0, 1, 2]
user_lv_cd_range=[1, 2, 3, 4, 5]
attr1_range=[-1, 1, 2, 3]
attr2_range=[-1, 1, 2]
attr3_range=[-1, 1, 2]
brand_range=[30, 48, 83, 88, 90, 124, 174, 200, 209, 211, 214, 244, 306, 328, 355, 375, 403, 427, 484, 489, 515, 545, 556, 562, 596, 622, 623, 655, 658, 677, 693, 717, 766, 790, 800, 801, 812, 857, 885, 916]
comment_num_range=[0, 1, 2, 3, 4]

action_month_range=[2,3,4]
all_month_range=[1,2,3,4,5,6,7,8,9,10,11,12]
day_range=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]

def one_hot_code(number,number_range):
    return_list=[]
    for ele in number_range:
        if ele == number:
            return_list.append(1)
        else:
            return_list.append(0)
    return return_list

def format_data(file_path):
    raw_data= csv.reader(open(file_path))
    return_list=[]
    for row in raw_data:
        temp_list=[]
        user_id,sku_id=row[0],row[1]#前2个特征
        action_date,age,sex,user_lv_cd,user_reg_dt,attr1,attr2,attr3,brand,comment_num,has_bad_comment,bad_comment_rate=row[2:14]#12个特征

        action_date = datetime.datetime.strptime(action_date, '%Y-%m-%d')
        action_date_month,action_date_day=action_date.month,action_date.day
        temp_list.extend(one_hot_code(action_date_month,action_month_range))#当前动作日期的月份
        temp_list.extend(one_hot_code(action_date_day, day_range))  # 当前动作日期的天数

        temp_list.extend(one_hot_code(age,age_range)) # age
        temp_list.extend(one_hot_code(sex, sex_range))  # sex
        temp_list.extend(one_hot_code(user_lv_cd, user_lv_cd_range))  # user_lv_cd

        try:
            user_reg_dt  = datetime.datetime.strptime(user_reg_dt, '%Y-%m-%d')#在这儿报错，说user_reg_dt为空
        except:
            user_reg_dt = datetime.datetime.strptime("2016-02-01", '%Y-%m-%d')  # 在这儿报错，说user_reg_dt为空
            print(user_id)#317次
        reg_month,reg_day=user_reg_dt.month,user_reg_dt.day
        temp_list.extend(one_hot_code(reg_month,all_month_range)) # user_reg_dt month
        temp_list.extend(one_hot_code(reg_day, day_range))  # user_reg_dt day

        temp_list.extend(one_hot_code(attr1, attr1_range))  # attr1
        temp_list.extend(one_hot_code(attr2, attr2_range))  # attr2
        temp_list.extend(one_hot_code(attr3, attr3_range))  # attr3
        temp_list.extend(one_hot_code(brand, brand_range))  # brand
        temp_list.extend(one_hot_code(comment_num, comment_num_range))  # comment_num

        temp_list.append(has_bad_comment) # has_bad_comment
        temp_list.append(bad_comment_rate) # bad comment rate

        for i in range(14,48):#接下来的34个统计特征
            row[i]=math.log(1.0 + float(row[i]))
            temp_list.append(row[i])

        label=row[48] # 标签
        temp_list.append(label)

        return_list.append(temp_list)
    return return_list

# print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
# # train_01_formatted=format_data(raw_path1)
# # train_01_formatted_df=pd.DataFrame(train_01_formatted)
# # train_01_formatted_df.to_csv(result_path1,index=False,header=False)
# #
# # train_11_formatted=format_data(raw_path2)
# # train_11_formatted_df=pd.DataFrame(train_11_formatted)
# # train_11_formatted_df.to_csv(result_path2,index=False,header=False)
#
# # predict_0410_data_formatted=format_data(raw_path3)
# # predict_0410_data_formatted_df=pd.DataFrame(predict_0410_data_formatted)
# # predict_0410_data_formatted_df.to_csv(result_path3,index=False,header=False)
#
# # predict_0415_data_formatted=format_data(raw_path4)
# # predict_0415_data_formatted_df=pd.DataFrame(predict_0415_data_formatted)
# # predict_0415_data_formatted_df.to_csv(result_path4,index=False,header=False)
#
# balanced_train_data_formatted=format_data(raw_path4)
# balanced_train_data_formatted_df=pd.DataFrame(balanced_train_data_formatted)
# balanced_train_data_formatted_df.to_csv(result_path4,index=False,header=False)
#
# print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))