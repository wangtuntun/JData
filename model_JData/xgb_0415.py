#encoding=utf-8
import xgboost as xgb
import csv
import numpy as np
import pandas as pd
train_x=[]
train_y=[]
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path
#训练数据
train_data_path=dealed_data_dir + "all_feature_01_label_10_formatted"
#预测时使用数据
pair_path = dealed_data_dir + "predict_0415_data"
pre_data_path=dealed_data_dir + "predict_0415_data_formatted"
#预测结果存储
result_path= dealed_data_dir + "xgb_pre_0415_info.csv"
pre_pair_path= dealed_data_dir + "xgb_pre_0415_info_pair.csv"

# 获取训练集
train_data_csv=csv.reader(open(train_data_path))
for row in train_data_csv:
    train_x.append(row[0:len(row)-1])
    # print(row[0:len(row)-1])
    train_y.append(row[len(row)-1])
train_x=np.array(train_x)

# 训练模型
xgb_model = xgb.XGBClassifier(objective="binary:logistic").fit(train_x,train_y)

#获取预测集
pre_data_csv=csv.reader(open(pre_data_path))
pre_x=[]
for row in pre_data_csv:
    pre_x.append(row[0:len(row)-1])
pre_x=np.array(pre_x)

# 进行预测
pre_pro=xgb_model.predict_proba(pre_x)
pd.DataFrame(pre_pro,columns=["pro_0","pro_1"]).to_csv(result_path,index=False)


#获取pair
pair_info=[]
pair_data_csv= csv.reader(open(pair_path))
for row in pair_data_csv:
    userid=row[0]
    skuid=row[1]
    pair_info.append((userid,skuid))
pd.DataFrame(pair_info,columns=["user_id","sku_id"]).to_csv(pre_pair_path,index=False)

# pre_label=xgb_model.predict(pre_x)

