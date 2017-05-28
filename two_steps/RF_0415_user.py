#encoding=utf-8
import numpy as np
import pandas as pd
from sklearn import ensemble
import csv
import time
'''
利用RF模型，预测每个用户购买的概率.
'''
train_x=[]
train_y=[]
data_dir="/public/home/scu1701/JData/Data3/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData3/"#server dealed data path
# data_dir="/home/wangtuntun/JData/Data/" #local data path
# dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path

# train_data_path=dealed_data_dir + "all_feature_01_label_balanced_formatted.csv" # 1:1
train_data_path=dealed_data_dir + "all_feature_01_label_10_user_formatted.csv" #1:10

pre_data_path=dealed_data_dir + "predict_0415_data_user_formatted"
pair_path = dealed_data_dir + "predict_0415_data_user"

result_path= dealed_data_dir + "rf_pre_0415_info_user.csv"


# 获取训练集
train_data_csv=csv.reader(open(train_data_path))
for row in train_data_csv:
    train_x.append(row[0:len(row)-1])
    # print(row[0:len(row)-1])
    train_y.append(row[len(row)-1])
train_x=np.array(train_x)


# 将参数和数据代入模型训练
n_estimators,max_depth,oob_score,random_state=1000,1000,False,0
params_RF = {'n_estimators': n_estimators, 'max_depth': max_depth, "oob_score": oob_score, "random_state": random_state}
clf = ensemble.RandomForestClassifier(**params_RF)  # 这个是随机森林分类器
clf.fit(train_x, train_y)  # 训练

#获取预测集
pre_data_csv=csv.reader(open(pre_data_path))
pre_x=[]
for row in pre_data_csv:
    pre_x.append(row[0:len(row)-1])
pre_x=np.array(pre_x)

# 进行预测
pre_pro=clf.predict_proba(pre_x)
pd.DataFrame(pre_pro,columns=["pro_0","pro_1"]).to_csv(result_path,index=False)

# pre_label=xgb_model.predict(pre_x)

