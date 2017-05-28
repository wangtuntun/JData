#encoding=utf-8
'''
将多个不同的模型的预测结果进行合并，得到最终预测结果
'''
import numpy as np
import pandas as pd
import csv
import time


# data_dir="/public/home/scu1701/JData/Data2/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData2/"#server dealed data path
data_dir="/home/wangtuntun/JData/Data/" #local data path
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path
write_dir=dealed_data_dir + "change_train_data/"

pair_info_path = write_dir + "predict_0415_data"

model1_path= write_dir + "rf_pre_0415_info_change_train.csv"
model2_path = write_dir + "rf_1v1_more.csv"
model3_path = write_dir + "rf_1v1_less.csv"

model4_path = write_dir + "rf_1v10_gbdt.csv"
model5_path = write_dir + "rf_1v1_more_gbdt.csv"
model6_path = write_dir + "rf_1v1_less_gbdt.csv"

def run_demo(k):

    model_number = 6.0

    predicted_pair = set()
    predicted_user = set()
    pair_pro_dict={}
    user_max_pro_dict={}
    user_flag_dict={}


    #读取文件内容
    # pair_info=pd.read_csv(pair_info_path)
    # userid_list = pair_info["user_id"].tolist()
    # skuid_list= pair_info["sku_id"].tolist()
    pair_info=csv.reader(open(pair_info_path))
    userid_list=[]
    skuid_list=[]
    for row in pair_info:
        userid=int(row[0])
        skuid=int(row[1])
        userid_list.append(userid)
        skuid_list.append(skuid)


    model1_info = pd.read_csv(model1_path)
    model1_prob_list = model1_info["pro_1"]
    model2_info = pd.read_csv(model2_path)
    model2_prob_list = model2_info["pro_1"]
    model3_info = pd.read_csv(model3_path)
    model3_prob_list = model3_info["pro_1"]

    model4_info = pd.read_csv(model4_path)
    model4_prob_list = model4_info["pro_1"]
    model5_info = pd.read_csv(model5_path)
    model5_prob_list = model5_info["pro_1"]
    model6_info = pd.read_csv(model6_path)
    model6_prob_list = model6_info["pro_1"]




    #将所有信息组合在一起
    # user_sku_xgb_gbdt_rf_lr=list(zip(userid_list,skuid_list,xgb_prob_list,gbdt_prob_list,rf_prob_list,lr_prob_list))
    user_sku_models_prob1 = list(zip(userid_list, skuid_list,  model1_prob_list, model2_prob_list,model3_prob_list,  model4_prob_list, model5_prob_list,model6_prob_list))

    #初始化
    for row in user_sku_models_prob1:
        user_id=row[0]
        user_max_pro_dict[user_id]=0
        user_flag_dict[user_id]=0
    #找到每个用户最大的pro
    for row in user_sku_models_prob1:
        user_id = row[0]
        # prob1 = float(row[2])+float(row[3]) + float(row[4])+float(row[5])
        prob1 = float(row[2]) + float(row[3] + float(row[4])) + float(row[5]) + float(row[6] + float(row[7]))
        prob1 /= model_number
        if prob1 > user_max_pro_dict[user_id]:
            user_max_pro_dict[user_id]=prob1
    #设置pair dict的值
    for row in user_sku_models_prob1:
        user_id = row[0]
        sku_id = row[1]
        # prob1 = float(row[2]) + float(row[3]) + float(row[4]) + float(row[5])
        prob1 = float(row[2]) + float(row[3] + float(row[4])) + float(row[5]) + float(row[6] + float(row[7]))
        prob1 /= model_number
        if prob1 >= user_max_pro_dict[user_id] and user_flag_dict[user_id] ==0 :
        # if user_flag_dict[user_id] == 0:
            pair_pro_dict[(user_id,sku_id)]=prob1
            user_flag_dict[user_id] = 1


    #得到前k个pair
    pair_pro_sorted = sorted(pair_pro_dict.items(), key=lambda item: item[1], reverse=True)
    count_k = k
    predicted_user_list=[]
    for ele in pair_pro_sorted:
        count_k -= 1
        if count_k >= 0 :
            # print(ele)
            predicted_user.add(int(ele[0][0]))
            predicted_pair.add( (int(ele[0][0]),int(ele[0][1])) )
            predicted_user_list.append(ele[0][0])


    #判断是否出现重复的用户
    len_user_set=len(predicted_user)
    len_user_list=len(predicted_user_list)
    print(len_user_set,len_user_list)

    #将结果写入文件
    pre_pair_list=list(predicted_pair)
    pd.DataFrame(pre_pair_list,columns=["user_id","sku_id"]).to_csv("submit_models_6.csv",index=False)

# for i in range(200,800):
#     run_demo(i)
run_demo(4000)