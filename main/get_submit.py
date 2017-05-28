#encoding=utf-8
'''
在这里获得分数最高的top-k pair
'''
import numpy as np
import pandas as pd
import csv
import time


# data_dir="/public/home/scu1701/JData/Data2/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData2/"#server dealed data path
data_dir="/home/wangtuntun/JData/Data/" #local data path
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path

predict_info_path_rf1= dealed_data_dir + "rf_pre_0415_info_2_steps.csv"
predict_pair_info_path_rf1 =  dealed_data_dir + "predict_0415_data"


def run_demo(k):

    predicted_pair = set()
    predicted_user = set()
    pair_pro_dict={}
    user_max_pro_dict={}
    user_flag_dict={}
    all_info=[]

    #读取文件内容
    pre_info_rf1=pd.read_csv(predict_info_path_rf1)
    prob1_list_rf1=pre_info_rf1["pro_1"].tolist()
    pair_info_rf1=pd.read_csv(predict_pair_info_path_rf1)
    userid_list_rf1=pair_info_rf1["user_id"].tolist()
    skuid_list_rf1=pair_info_rf1["sku_id"].tolist()
    info_list_rf1=list(zip(userid_list_rf1,skuid_list_rf1,prob1_list_rf1))
    all_info.extend(info_list_rf1)

    #初始化
    for row in all_info:
        user_id=row[0]
        user_max_pro_dict[user_id]=0
        user_flag_dict[user_id]=0

    #找到每个用户最大的pro
    for row in all_info:
        user_id = row[0]
        prob1 = float(row[2])
        if prob1 > user_max_pro_dict[user_id]:
            user_max_pro_dict[user_id]=prob1

    #设置pair dict的值
    for row in all_info:
        user_id = row[0]
        sku_id = row[1]
        prob1 = float(row[2])
        if prob1 >= user_max_pro_dict[user_id] and user_flag_dict[user_id] ==0 :
        # if user_flag_dict[user_id] == 0:
            pair_pro_dict[(user_id,sku_id)]=prob1
            user_flag_dict[user_id] = 1


    #得到前k个pair
    pair_pro_sorted = sorted(pair_pro_dict.items(), key=lambda item: item[1], reverse=True)
    # pd.DataFrame(list(pair_pro_sorted)).to_csv("sorted_pro.csv",index=False)
    count_k = k
    predicted_user_list=[]
    for ele in pair_pro_sorted:
        count_k -= 1
        if count_k >= 0 :
            predicted_user.add(int(ele[0][0]))
            predicted_pair.add( (int(ele[0][0]),int(ele[0][1])) )
            predicted_user_list.append(ele[0][0])


    #判断是否出现重复的用户
    len_user_set=len(predicted_user)
    len_user_list=len(predicted_user_list)
    print(len_user_set,len_user_list)

    #将结果写入文件
    pre_pair_list=list(predicted_pair)
    pd.DataFrame(pre_pair_list,columns=["user_id","sku_id"]).to_csv("submit_rf.csv",index=False)

run_demo(40000)



