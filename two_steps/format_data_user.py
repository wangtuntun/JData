#encoding=utf-8
import numpy as np
import pandas as pd
import csv
import time
import datetime
import math


age_range=[0, 1, 2, 3, 4, 5]
sex_range=[0, 1, 2]
user_lv_cd_range=[1, 2, 3, 4, 5]
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

def format_data(raw_file_path,write_path):
    # f_write=open(write_path,"w+")
    raw_data= csv.reader(open(raw_file_path))
    return_list=[]
    for row in raw_data:
        #len(row)=25
        temp_list=[]
        user_id=row[0]
        action_date,age,sex,user_lv_cd,user_reg_dt=row[1:6]

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

        for i in range(6,24):#接下来的34个统计特征
            row[i]=math.log(1.0 + float(row[i]))
            temp_list.append(row[i])

        label=row[24] # 标签
        temp_list.append(label)
        # write_str=",".join(temp_list)
        # f_write.write(write_str+"\n")
        return_list.append(temp_list)
    # return return_list
    pd.DataFrame(return_list).to_csv(write_path,index=False,header=False)

# data_dir="/public/home/scu1701/JData/Data3/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData3/"#server dealed data path
# # file_path=dealed_data_dir + "all_feature_01_label_10_user.csv"
# # file_path=dealed_data_dir + "predict_0415_data_user"
# file_path = dealed_data_dir + "predict_0415_data_2_steps.csv"
# return_list=format_data(file_path)
# # result_path=dealed_data_dir + "all_feature_01_label_10_user_formatted.csv"
# # result_path=dealed_data_dir + "predict_0415_data_user_formatted"
# result_path=dealed_data_dir + "predict_0415_data_2_steps_formatted.csv"
# pd.DataFrame(return_list).to_csv(result_path,index=False,header=False)