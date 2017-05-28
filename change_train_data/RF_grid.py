#encoding=utf-8
import numpy as np
import pandas as pd
from sklearn import ensemble
import csv
import gc
import time

data_dir="/public/home/scu1701/JData/Data3/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData3/"#server dealed data path
write_dir= dealed_data_dir + "change_train_data/"
train_data_path=write_dir + "all_feature_01_label_formatted" #1:10

pre_data_path=write_dir + "predict_0415_data_formatted"
result_path= write_dir + "rf_pre_0415_info_change_train.csv"

def predict():
    train_x = []
    train_y = []
    # 获取训练集
    train_data_csv=csv.reader(open(train_data_path))
    for row in train_data_csv:
        train_x.append(row[0:len(row)-1])
        train_y.append(row[len(row)-1])
    train_x=np.array(train_x)

    # 将参数和数据代入模型训练
    best_estimators=0
    max_score=0.0
    n_estimators,max_depth,oob_score,random_state=1000,1000,True,0
    print("n_estimators")
    for i in range(100,1000,10):
        print(i)
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        params_RF = {'n_estimators': i, 'max_depth': max_depth, "oob_score": oob_score, "random_state": random_state}
        clf = ensemble.RandomForestClassifier(**params_RF)  # 这个是随机森林分类器
        clf.fit(train_x, train_y)  # 训练
        score = clf.oob_score_
        print(i, score)

        if score>max_score:
            max_score=score
            best_estimators=i
    print("best estimators")
    print(best_estimators)

    best_depth=0
    max_score=0
    for i in range(100,1000,10):
        print(i)
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        params_RF = {'n_estimators': best_estimators, 'max_depth': i, "oob_score": oob_score, "random_state": random_state}
        clf = ensemble.RandomForestClassifier(**params_RF)  # 这个是随机森林分类器
        clf.fit(train_x, train_y)  # 训练
        score = clf.oob_score_

        print(i, score)
        if score > max_score:
            max_score=score
            best_depth = i
    print("best depth")
    print(best_depth)

    best_state = 0
    max_score = 0
    for i in range(100, 1000, 10):
        print(i)
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        params_RF = {'n_estimators': best_estimators, 'max_depth': best_depth, "oob_score": oob_score,
                     "random_state": i}
        clf = ensemble.RandomForestClassifier(**params_RF)  # 这个是随机森林分类器
        clf.fit(train_x, train_y)  # 训练
        score = clf.oob_score_

        print(i, score)
        if score > max_score:
            max_score = score
            best_state = i
    print("best state")
    print(best_state)


    #释放内存
    del train_x
    del train_y
    del train_data_csv

    gc.collect()

predict()