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

pair_info_path = dealed_data_dir + "gbdt_pre_0415_info_pair.csv"

gbdt_predict_info_path= dealed_data_dir + "gbdt_pre_0415_info.csv"
rf_predict_info_path= dealed_data_dir + "rf_pre_0415_info.csv"
# xgb_predict_info_path= dealed_data_dir + "xgb_pre_0415_info.csv"
# lr_predict_info_path=  "/home/wangtuntun/JData/spark_dealed_data/01_label_balanced_lr_pre_info.csv/part-00000"

def run_demo(k):

    model_number = 2.0

    predicted_pair = set()
    predicted_user = set()
    pair_pro_dict={}
    user_max_pro_dict={}
    user_flag_dict={}


    #读取文件内容
    pair_info=pd.read_csv(pair_info_path)
    userid_list = pair_info["user_id"].tolist()
    skuid_list= pair_info["sku_id"].tolist()
    gbdt_info=pd.read_csv(gbdt_predict_info_path)
    gbdt_prob_list = gbdt_info["pro_1"].tolist()
    # xgb_info= pd.read_csv(xgb_predict_info_path)
    # xgb_prob_list = xgb_info["pro_1"].tolist()
    rf_info = pd.read_csv(rf_predict_info_path)
    rf_prob_list = rf_info["pro_1"]

    # lr_info = open(lr_predict_info_path, "r+")
    # lr_raw_list = lr_info.readlines()
    # lr_info.close()
    # lr_prob_list = []
    # for ele in lr_raw_list:
    #     v1 = ele.split(",")[1]
    #     v2 = v1.split("]")[0]
    #     p1 = float(v2)
    #     lr_prob_list.append(p1)

    #将所有信息组合在一起
    # user_sku_xgb_gbdt_rf_lr=list(zip(userid_list,skuid_list,xgb_prob_list,gbdt_prob_list,rf_prob_list,lr_prob_list))
    user_sku_xgb_gbdt_rf_lr = list(zip(userid_list, skuid_list,  gbdt_prob_list, rf_prob_list))

    #初始化
    for row in user_sku_xgb_gbdt_rf_lr:
        user_id=row[0]
        user_max_pro_dict[user_id]=0
        user_flag_dict[user_id]=0
    #找到每个用户最大的pro
    for row in user_sku_xgb_gbdt_rf_lr:
        user_id = row[0]
        # prob1 = float(row[2])+float(row[3]) + float(row[4])+float(row[5])
        prob1 = float(row[2]) + float(row[3])
        prob1 /= model_number
        if prob1 > user_max_pro_dict[user_id]:
            user_max_pro_dict[user_id]=prob1
    #设置pair dict的值
    for row in user_sku_xgb_gbdt_rf_lr:
        user_id = row[0]
        sku_id = row[1]
        # prob1 = float(row[2]) + float(row[3]) + float(row[4]) + float(row[5])
        prob1 = float(row[2]) + float(row[3])
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
    pd.DataFrame(pre_pair_list,columns=["user_id","sku_id"]).to_csv("submit_models.csv",index=False)

# for i in range(200,800):
#     run_demo(i)
run_demo(1000)
# 400 (47, 3, 0.022530400266133538, 0.05241635687732342, 0.0026064291920069507)
# 500 (33, 3, 0.022158248922575424, 0.05103092783505155, 0.0029097963142580016)
# 800 (47, 3, 0.022530400266133538, 0.05241635687732342, 0.0026064291920069507)  ok

# 900 (50, 3, 0.021919498277900584, 0.05102040816326531, 0.0025188916876574307)
# 1000 (50, 3, 0.02027100326212346, 0.047021943573667714, 0.0024370430544272946)
#banlanced_train_data
# n_estimators,max_depth,oob_score,random_state=500,500,True,10
# 317 (34, 2, 0.02877395042109218, 0.06880269814502529, 0.0020881186051367718) ok

# n_estimators,max_depth,oob_score,random_state=1000,1000,True,10
# 280 (33, 2, 0.029761743097568606, 0.07122302158273382, 0.0021208907741251323)
# 310 (33, 2, 0.028287261226167287, 0.06757679180887372, 0.002094240837696335)

# n_estimators,max_depth,oob_score,random_state=2000,2000,True,10
#333 (35, 2, 0.028830761968113638, 0.0689655172413793, 0.0020742584526031943)

#n_estimators,max_depth,oob_score,random_state=1000,1000,False,10
# 280 (33, 2, 0.029761743097568606, 0.07122302158273382, 0.0021208907741251323)
# 387 (38, 3, 0.029337240397352825, 0.06877828054298644, 0.0030432136335970784)

# n_estimators,max_depth,oob_score,random_state=1000,1000,False,0
# 218 (32, 3, 0.03305347462955598, 0.07773279352226721, 0.00326726203441516)


