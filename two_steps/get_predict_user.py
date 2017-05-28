#encoding=utf-8

import numpy as np
import pandas as pd
import csv
import time
import pprint
'''
从预测结果中，找出最可能购买的用户
'''

# data_dir="/public/home/scu1701/JData/Data3/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData3/"#server dealed data path
data_dir="/home/wangtuntun/JData/Data/" #local data path
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path

predict_info_path_rf1= dealed_data_dir + "rf_pre_0415_info_user.csv"
predict_userid_info_path_rf1 =  dealed_data_dir + "predict_0415_data_user"



def run_demo(k):

    pair_pro_dict={}
    all_info=[]

    #读取文件内容
    pre_info_rf1=pd.read_csv(predict_info_path_rf1)
    prob1_list_rf1=pre_info_rf1["pro_1"].tolist()
    userid_list_rf1 = []
    userid_info=csv.reader(open(predict_userid_info_path_rf1))
    for row in userid_info:
        userid_list_rf1.append(row[0])


    info_list_rf1=list(zip(userid_list_rf1,prob1_list_rf1))
    all_info.extend(info_list_rf1)

    #初始化
    for row in all_info:
        user_id=row[0]
        pro=row[1]
        pair_pro_dict[user_id]=pro

    #得到前k个pair
    pair_pro_sorted = sorted(pair_pro_dict.items(), key=lambda item: item[1], reverse=True)
    top_k_userid_list = []
    for ele in pair_pro_sorted:
        top_k_userid_list.append(int(ele[0]))
    top_k_userid_list=top_k_userid_list[0:k]

    # #将结果写入文件
    pd.DataFrame(top_k_userid_list,columns=["user_id"]).to_csv("predict_userid.csv",index=False)

run_demo(1000)



