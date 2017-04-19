#encoding=utf-8
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
'''
func：将数据划分为线下训练集和线下测试集。训练集用来训练和测试模型。线下测试集用来验证预测结果如何。
date:2017/04/15
author：tuntunking
ps:type=4大概占所有type的千分之一
'''


# train_types={"user_id":np.dtype(long),"sku_id":np.dtype(int),"model_id":np.dtype(str),"type":np.dtype(int),"cate":np.dtype(int),"brand":np.dtype(int)}
data_dir="/public/home/scu1701/JData/Data/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData/"#server dealed data path
# data_dir="/home/wangtuntun/JData/Data/" #local data path
# dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path
raw_data_path=dealed_data_dir + "all_feature_01_label"

train_path=dealed_data_dir + "train_model_01_label.csv"
test_path=dealed_data_dir + "offline_predict_test_01_label.csv"

raw_data=pd.read_csv(raw_data_path,parse_dates=[2])

#获取训练集和线下测试集并排序
raw_data_train=raw_data[raw_data["2"] <= datetime.datetime.strptime("2016-04-10",'%Y-%m-%d')]
train_sorted=raw_data_train.sort_values(by=["0","2","1"])
raw_date_test=raw_data[raw_data["2"] > datetime.datetime.strptime("2016-04-10",'%Y-%m-%d')]
test_sorted=raw_date_test.sort_values(by=["0","2","1"])

#存入文件
train_sorted.to_csv(train_path,index=False,header=False)
test_sorted.to_csv(test_path,index=False,header=False)