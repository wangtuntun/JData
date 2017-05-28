#encoding=utf-8
import numpy as np
import pandas as pd
from sklearn import ensemble
import csv
import gc
import time

def predict(train_data_path,pre_data_path,result_path):

    train_x=[]
    train_y=[]

    # 获取训练集
    train_data_csv=csv.reader(open(train_data_path))
    for row in train_data_csv:
        train_x.append(row[0:len(row)-1])
        train_y.append(row[len(row)-1])
    train_x=np.array(train_x)

    # 将参数和数据代入模型训练
    n_estimators,max_depth,oob_score,random_state=100,100,False,490
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

    #释放内存
    del train_x
    del train_y
    del train_data_csv
    del pre_data_csv
    del pre_x
    del pre_pro
    gc.collect()


print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
train_data_path="/public/home/scu1701/JData/DealedData5/change_feature/train_balanced_1vs1_less_formatted_0518"
pre_data_path="/public/home/scu1701/JData/DealedData5/change_feature/add_feature_predict_formatted_0518.csv"
result_path="/public/home/scu1701/JData/DealedData5/change_feature/rf_1v1_less_0518.csv"
predict(train_data_path,pre_data_path,result_path)
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
