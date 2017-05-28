#encoding=utf-8
'''
bagging
第一步是几个文件分开执行的，目的是得到predict_userid
这是第二步，利用前一步获取的predict_userid。生成预测集合
'''
import numpy as np
import pandas as pd
import csv
from sklearn import ensemble
import time
import gc
import get_predict_data_filtered_userid_sku as get_predict_data
import format_raw_data2model as format_pair

data_dir="/public/home/scu1701/JData/Data3/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData3/"#server dealed data path

two_step_dealed_data_dir=dealed_data_dir + "two_step/"


#获取训练模型
print("开始训练模型时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
train_x=[]
train_y=[]
train_data_path=dealed_data_dir + "all_feature_01_label_10_formatted" #1:10
train_data_csv=csv.reader(open(train_data_path))
for row in train_data_csv:
    train_x.append(row[0:len(row)-1])
    train_y.append(row[len(row)-1])
train_x=np.array(train_x)
# 将参数和数据代入模型训练
n_estimators,max_depth,oob_score,random_state=1000,1000,False,0
params_RF = {'n_estimators': n_estimators, 'max_depth': max_depth, "oob_score": oob_score, "random_state": random_state}
clf = ensemble.RandomForestClassifier(**params_RF)  # 这个是随机森林分类器
clf.fit(train_x, train_y)  # 训练
del train_x
del train_y
gc.collect()
print("完成训练模型时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


#获取预测集
print("开始获取预测集合时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
predict_path=two_step_dealed_data_dir + "predict_data"
get_predict_data.get_predict_data(predict_path)
print("完成获取预测集合时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

#对预测集进行格式化操作
print("开始格式化预测集合时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
predict_formatted_path=two_step_dealed_data_dir + "predict_data_formatted"
format_pair.format_data(predict_path,predict_formatted_path)
print("完成格式化预测集合时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

#生成预测结合数组
print("开始生成预测数组时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
pre_x=[]
pre_info=csv.reader(open(predict_formatted_path))
for row in pre_info:
    pre_x.append(row[0:len(row)-1])
pre_x=np.array(pre_x)
print("完成生成预测数组时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

#开始预测
print("开始预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
result_path= two_step_dealed_data_dir + "rf_pre_0415_info_2_steps"
pre_pro=clf.predict_proba(pre_x)
pd.DataFrame(pre_pro,columns=["pro_0","pro_1"]).to_csv(result_path,index=False)
print("完成预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))