#encoding=utf-8
'''
bagging
将234的数据多次随机读取，将每次的读取结果用来训练不同的模型。最后将不同模型的预测结果合并
'''
import numpy as np
import pandas as pd
import csv
from sklearn import ensemble
import time
from sklearn import cross_validation
import gc
import get_feature_multi as get_feature_data
import format_data_multi as format_raw_data2model

data_dir="/public/home/scu1701/JData/Data/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData/"#server dealed data path
# data_dir="/home/wangtuntun/JData/Data/" #local data path
# dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path
multi_dealed_data_dir=dealed_data_dir + "multi_train/"
mode_number=100
def get_trained_model():#每次训练一个模型，之前的文件都会被覆盖。
    #随机读取数据
    print("开始读取数据时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    raw_data_path = dealed_data_dir + "234month_skuid_filtered.csv"
    raw_data = pd.read_csv(raw_data_path)
    neg_data = raw_data[raw_data["type"] != 4]
    pos_data = raw_data[raw_data["type"] == 4]
    drop, save = cross_validation.train_test_split(neg_data, test_size=0.01)
    train1=save.append(pos_data,ignore_index=True)
    train_path=multi_dealed_data_dir + "train1.csv"
    train1.to_csv(train_path,index=False,header=False)
    del drop
    del train1
    gc.collect()
    print("完成读取数据时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    #获取特征
    print("开始获取特征时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    feature_path=multi_dealed_data_dir + "feature1.csv"
    get_feature_data.get_feature(train_path,feature_path)
    print("完成获取特征时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    #格式化数据
    print("开始格式化数据时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    format_path= multi_dealed_data_dir + "format1.csv"
    format_list=format_raw_data2model.format_data(feature_path)
    format_df=pd.DataFrame(format_list)
    format_df.to_csv(format_path,index=False,header=False)
    print("完成格式化数据时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    #训练模型
    print("开始训练模型时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    train_x=[]
    train_y=[]
    train_data_csv=csv.reader(open(format_path))
    for row in train_data_csv:
        train_x.append(row[0:len(row)-1])
        # print(row[0:len(row)-1])
        train_y.append(row[len(row)-1])
    train_x=np.array(train_x)
    n_estimators,max_depth,oob_score,random_state=1000,1000,False,0
    params_RF = {'n_estimators': n_estimators, 'max_depth': max_depth, "oob_score": oob_score, "random_state": random_state}
    clf = ensemble.RandomForestClassifier(**params_RF)  # 这个是随机森林分类器
    clf.fit(train_x, train_y)  # 训练
    print("完成训练模型时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    del train_x
    del train_y
    gc.collect()
    return clf

#获取训练好的模型
import pickle
model_list=[]
output = open('train_models.pkl', 'wb')
print("开始训练模型")
for i in range(mode_number):
    print(i)
    clf=get_trained_model()
    model_list.append(clf)
    pickle.dump(clf,output)

#获取预测集
print("开始获取训练集时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
pre_data_path=dealed_data_dir + "predict_0415_data_formatted"
pair_path = dealed_data_dir + "predict_0415_data"
pre_data_csv=csv.reader(open(pre_data_path))
pre_x=[]
for row in pre_data_csv:
    pre_x.append(row[0:len(row)-1])
pre_x=np.array(pre_x)
print("完成获取训练集时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

#获取预测结果对应的pair
print("开始获取pair时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
pre_pair_path= multi_dealed_data_dir + "rf_pre_0415_info_pair.csv"
pair_info=[]
pair_data_csv= csv.reader(open(pair_path))
for row in pair_data_csv:
    userid=row[0]
    skuid=row[1]
    pair_info.append((userid,skuid))
pd.DataFrame(pair_info,columns=["user_id","sku_id"]).to_csv(pre_pair_path,index=False)
print("完成获取pair时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

#每个模型都进行预测
print("开始预测")
for i in range(mode_number):
    print(i)
    clf=model_list[i]
    print("开始预测时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    result_path= multi_dealed_data_dir + "rf_pre_0415_info" + str(i) + ".csv"
    pre_pro=clf.predict_proba(pre_x)
    pd.DataFrame(pre_pro,columns=["pro_0","pro_1"]).to_csv(result_path,index=False)
    print("完成预测时间")
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

