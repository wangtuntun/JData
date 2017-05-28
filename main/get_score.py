#encoding=utf-8
'''
在这里获得分数最高的top-k pair
'''

import pandas as pd
import csv



# data_dir="/public/home/scu1701/JData/Data2/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData2/"#server dealed data path
data_dir="/home/wangtuntun/JData/Data/" #local data path
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path
# predict_info_path= dealed_data_dir + "balanced_rf_0410_predict_info.csv"
predict_info_path= dealed_data_dir + "balanced_gbdt_0410_predict_info.csv"
real_info_path= dealed_data_dir + "offline_predict_test_11_label.csv"
def run_demo(k):

    real_pair = set()
    real_user = set()
    predicted_pair = set()
    predicted_user = set()
    pair_pro_dict={}
    user_max_pro_dict={}
    user_flag_dict={}

    #读取文件内容
    pre_csv_reader=csv.reader(open(predict_info_path))
    info_list=[]
    for row in pre_csv_reader:
        info_list.append(row)
    #初始化
    for row in info_list:
        user_id=row[0]
        user_max_pro_dict[user_id]=0
        user_flag_dict[user_id]=0
    #找到每个用户最大的pro
    for row in info_list:
        user_id = row[0]
        prob1 = float(row[3])
        if prob1 > user_max_pro_dict[user_id]:
            user_max_pro_dict[user_id]=prob1
    #设置pair dict的值
    for row in info_list:
        user_id = row[0]
        sku_id = row[1]
        prob1 = float(row[3])
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
            predicted_user.add(int(ele[0][0]))
            predicted_pair.add( (int(ele[0][0]),int(ele[0][1])) )
            predicted_user_list.append(ele[0][0])

    #得到实际结果中，购买行为对
    real_csv_reader=csv.reader(open(real_info_path))
    for row in real_csv_reader:
        userid=int(row[0])
        skuid=int(row[1])
        length=len(row)
        label=int(row[length-1])
        if label == 1:
            real_user.add(userid)
            real_pair.add((userid,skuid))



    #计算结果
    pre_real_user_and = predicted_user & real_user
    pre_real_pair_and = predicted_pair & real_pair
    hit_user=len(pre_real_user_and)
    hit_pair=len(pre_real_pair_and)
    f11_p=float(hit_user) / float(k)
    f11_r=float(hit_user) / float(len(real_user))
    f12_p=float(hit_pair) / float(k)
    f12_r=float(hit_pair) / float(len(real_pair))
    f11=6*f11_p*f11_r / (5*f11_r + f11_p)
    f12=5*f12_p*f12_r / (2*f12_r + 3*f12_p)
    score=0.4*f11 + 0.6*f12
    print(hit_user,hit_pair,score,f11,f12)

    #判断是否出现重复的用户
    len_user_set=len(predicted_user)
    len_user_list=len(predicted_user_list)
    print(len_user_set,len_user_list)

    #将结果写入文件
    pre_pair_list=list(predicted_pair)
    pd.DataFrame(pre_pair_list,columns=["user_id","sku_id"]).to_csv("submit.csv",index=False)

for i in range(200,800):
    run_demo(i)
# run_demo(218)
#rf
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

#gbdt
# params_GBDT = {'n_estimators': 500, 'max_depth': 10, 'min_samples_split': 2, "learning_rate": 0.01,"subsample": 0.6,"loss": "deviance", "random_state": 10}
# 284 (35, 1, 0.0306351894982003, 0.07500000000000001, 0.0010586491636671608)