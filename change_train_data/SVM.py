#encoding=utf-8
import numpy as np
import pandas as pd
from sklearn import svm
import csv
import gc

data_dir="/public/home/scu1701/JData/Data/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData/"#server dealed data path

train_data_path= dealed_data_dir + "balanced_formatted" #1:10
pre_data_path= dealed_data_dir + "predict_0415_data_formatted"
result_path= dealed_data_dir + "svm_prob_info.csv"

def predict(train_data_path,pre_data_path,result_path):
    train_x = []
    train_y = []
    # 获取训练集
    train_data_csv=csv.reader(open(train_data_path))
    for row in train_data_csv:
        train_x.append(row[0:len(row)-1])
        train_y.append(row[len(row)-1])
    train_x=np.array(train_x)

    # 将参数和数据代入模型训练
    clf = svm.SVC(probability=True)  # class
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


# predict()
predict(train_data_path,pre_data_path,result_path)