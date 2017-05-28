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

# predict_info_path_rf1= dealed_data_dir + "rf_pre_0415_info_2_steps.csv"
# predict_pair_info_path_rf1 =  dealed_data_dir + "predict_0415_data"

write_dir= dealed_data_dir + "change_feature/"
predict_info_path= "/home/wangtuntun/JData/spark_dealed_data/LR_add_feature_0518_1v1_more/part-00000"
predict_pair_info_path =  write_dir + "add_feature_predict.csv"


def run_demo(k):

    predicted_pair = set()
    predicted_user = set()
    pair_pro_dict={}
    user_max_pro_dict={}
    user_flag_dict={}
    all_info=[]

    #读取概率内容
    prob1_list = []
    raw_pre_info=open(predict_info_path,"r+")
    pre_info=raw_pre_info.readlines()
    for ele in pre_info:
        v1=ele.split("]")
        v2=v1[0].split(",")
        v3=float(v2[1])
        prob1_list.append(v3)

    #读取pair信息
    pair_info=csv.reader(open(predict_pair_info_path))
    userid_list=[]
    skuid_list=[]
    for row in pair_info:
        userid=row[0]
        skuid=row[1]
        userid_list.append(userid)
        skuid_list.append(skuid)
    info_list=list(zip(userid_list,skuid_list,prob1_list))
    all_info.extend(info_list)

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
            user=int(float(ele[0][0]))
            sku=int(float(ele[0][1]))
            predicted_user.add(user)
            predicted_pair.add( (user,sku) )
            predicted_user_list.append(user)


    #判断是否出现重复的用户
    len_user_set=len(predicted_user)
    len_user_list=len(predicted_user_list)
    print(len_user_set,len_user_list)

    #将结果写入文件
    pre_pair_list=list(predicted_pair)
    pd.DataFrame(pre_pair_list,columns=["user_id","sku_id"]).to_csv("submit_lr_0518_2_more.csv",index=False)

run_demo(4000)



