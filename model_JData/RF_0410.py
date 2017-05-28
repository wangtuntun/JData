#$encoding=utf-8
'''
环境 ubuntu+IDEA+python35
实现的功能：预测0410以后的结果

'''
import numpy as np
import pandas as pd
from sklearn import ensemble
import csv
import time

# data_dir="/public/home/scu1701/JData/Data3/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData3/"#server dealed data path
data_dir="/home/wangtuntun/JData/Data/" #local data path
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path

#该评价指标用来评价模型好坏
def rmspe(zip_list,count):
    sum_value=0.0
    for real,predict in zip_list:
        if real == predict:
            sum_value += 1.0
    ave_value=sum_value / count
    return ave_value

#提取特征和目标值
def get_features_target(data):
    data_array=pd.np.array(data)#传入dataframe，为了遍历，先转为array
    features_list=[]
    target_list=[]
    for line in data_array:
        feature_label_length=len(line)
        temp_list=[]
        for i in range(0,feature_label_length):
            if i == feature_label_length-1 :
                target_temp=int(line[i])
            else:
                temp_list.append(line[i])
        features_list.append(temp_list)
        target_list.append(target_temp)
    return features_list, target_list
    # return pd.DataFrame(features_list),pd.DataFrame(target_list)

#这个数据用来预测4/11-->4/15日的购买情况
#读取文件，返回label和（userid，skuid）
#参考 get_features_target()
def get_predict_pair(file_path):
    csv_reader=csv.reader(open(file_path))
    user_list=[]
    sku_list=[]
    for row in csv_reader:
        user_list.append(row[0])
        sku_list.append(row[1])
    return user_list,sku_list



def run_demo(n_estimators,max_depth,oob_score,random_state):

    feature_save_path = dealed_data_dir + "训练模型数据.csv"
    csv_reader = csv.reader(open(feature_save_path))

    predict_path = dealed_data_dir + "predict_0410_data_formatted"
    predict_reader = csv.reader(open(predict_path))

    # 获取预测数据的pair和label
    user_list, sku_list = get_predict_pair(dealed_data_dir + "predict_0410_data")

    #训练时使用的数据
    train_model_data=[]
    for ele in csv_reader:
        train_model_data.append(ele)#这个数据会在随后分割特征和label

    #预测时使用的数据
    predict_data=[]
    for ele in predict_reader:
        predict_feature_formated_length=len(ele)
        predict_data.append(ele[0:predict_feature_formated_length-1])#当时生成数据的时候，把label也放进去了。但是现在不需要label

    #划分训练时使用的数据
    # data_other,train_model_data=cross_validation.train_test_split(train_model_data,test_size=0.001,random_state=10)#为了减少代码运行时间，方便测试
    # train_and_valid, test = cross_validation.train_test_split(train_model_data, test_size=0.2, random_state=10)
    train= train_model_data #不需要测试集和验证集
    # train, valid = cross_validation.train_test_split(train_and_valid, test_size=0.01, random_state=10)
    train_feature, train_target = get_features_target(train)#将JData的数据代入发现，memory error


    #将参数和数据代入模型训练
    params_RF = {'n_estimators': n_estimators, 'max_depth': max_depth, "oob_score": oob_score ,"random_state": random_state}
    clf=ensemble.RandomForestClassifier(**params_RF)#这个是随机森林分类器
    clf.fit(train_feature, train_target) #训练


    #开始预测
    pre=clf.predict(predict_data)
    pre_prob=clf.predict_proba(predict_data)#dataframe
    pre_prob_1=pre_prob[:,1] # 返回label=1的概率   在这儿报错 index 1 is out ofbounds for axis 1 with size 1
    pre_list=list(pre)

    #看有哪些购买行为
    pre_1_list=[]
    for ele in pre_list:
        if float(ele) > 0.5:
            pre_1_list.append(ele)
    pd.DataFrame(pre_1_list).to_csv(dealed_data_dir + "balanced_rf_0410_predict_1.csv")

    #将结果写入文件
    zip_user_sku_reallabel_prelabel_pro1 = list(zip(user_list, sku_list, pre_list, pre_prob_1))
    zip_df=pd.DataFrame(zip_user_sku_reallabel_prelabel_pro1)
    result_path=dealed_data_dir + "balanced_rf_0410_predict_info.csv"
    zip_df.to_csv(result_path,header=False,index=False)


print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

n_estimators,max_depth,oob_score,random_state=1000,1000,False,0
run_demo(n_estimators,max_depth,oob_score,random_state)

print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))