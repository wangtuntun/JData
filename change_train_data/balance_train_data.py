#encoding=utf-8
import sys
import numpy as np
import csv
import gc
import datetime
import pandas as pd
from sklearn import cross_validation

data_dir="/public/home/scu1701/JData/Data4/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData4/"#server dealed data path
action_data_path = dealed_data_dir + "all_feature_01_label"
write_dir=dealed_data_dir + "change_train_data/"


action_data = pd.read_csv(action_data_path, parse_dates=[2])
action_data_4 = action_data[action_data["2"] >= datetime.datetime.strptime("2016-04-01", '%Y-%m-%d')]
del action_data
gc.collect()
action_data_4_neg=action_data_4[action_data_4["48"] != 1]
action_data_4_pos=action_data_4[action_data_4["48"] == 1]

# write_path=write_dir+"balanced"

def vs_1_10(write_path):
    #负例下采样 十分之一
    neg_drop,neg_save=cross_validation.train_test_split(action_data_4_neg, test_size=0.1, random_state=10)
    del neg_drop
    gc.collect()

    #正例上采样
    pos_data=pd.DataFrame()
    for i in range(10):
        pos_data =pos_data.append(action_data_4_pos,ignore_index=True)

    #合并数据得到总的训练样本
    train_data=pos_data.append(neg_save,ignore_index=True)
    print(len(neg_save),len(pos_data),len(train_data))
    del pos_data
    del neg_save
    gc.collect()
    #将数据顺序搞乱
    train_data_array=np.array(train_data)
    # random.shuffle(train_data_array)
    np.random.shuffle(train_data_array)
    #存入本地
    pd.DataFrame(train_data_array).to_csv(write_path,index=False,header=False)
    del train_data
    del train_data_array
    gc.collect()


def vs_1_1_less(write_path):
    # 负例下采样 千分之一
    neg_drop, neg_save = cross_validation.train_test_split(action_data_4_neg, test_size=0.001, random_state=10)
    del neg_drop
    gc.collect()

    # 正例保持不变
    pos_data = action_data_4_pos

    # 合并数据得到总的训练样本
    train_data = pos_data.append(neg_save, ignore_index=True)
    print(len(neg_save), len(pos_data), len(train_data))
    del pos_data
    del neg_save
    gc.collect()
    # 将数据顺序搞乱
    train_data_array = np.array(train_data)
    # random.shuffle(train_data_array)
    np.random.shuffle(train_data_array)
    # 存入本地
    pd.DataFrame(train_data_array).to_csv(write_path, index=False, header=False)
    del train_data
    del train_data_array
    gc.collect()

def vs_1_1_more(write_path):
    # 负例下采样 百分之一
    neg_drop, neg_save = cross_validation.train_test_split(action_data_4_neg, test_size=0.01, random_state=10)
    del neg_drop
    gc.collect()

    # 正例上采样
    pos_data = pd.DataFrame()
    for i in range(10):
        pos_data = pos_data.append(action_data_4_pos, ignore_index=True)

    # 合并数据得到总的训练样本
    train_data = pos_data.append(neg_save, ignore_index=True)
    print(len(neg_save), len(pos_data), len(train_data))
    del pos_data
    del neg_save
    gc.collect()
    # 将数据顺序搞乱
    train_data_array = np.array(train_data)
    # random.shuffle(train_data_array)
    np.random.shuffle(train_data_array)
    # 存入本地
    pd.DataFrame(train_data_array).to_csv(write_path, index=False, header=False)
    del train_data
    del train_data_array
    gc.collect()