#$encoding=utf-8
'''
环境 ubuntu+IDEA+python35
实现的功能：利用GBDT模型,RF模型实现分类

'''
import numpy as np
import pandas as pd
from sklearn import ensemble, cross_validation

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
        temp_list=[]
        for i in range(0,7):#15个特征加一个label
            if i == 6 :
                target_temp=int(line[i])
            else:
                temp_list.append(line[i])
        features_list.append(temp_list)
        target_list.append(target_temp)
    return features_list, target_list
    # return pd.DataFrame(features_list),pd.DataFrame(target_list)

def run_demo():

    feature_save_path = "credit_data.csv"  # 将最终生成的特征存入该文件
    raw_data=open(feature_save_path,"r+")
    data_list=raw_data.readlines()
    raw_data.close()
    data=[]
    for ele in data_list:
        feature_list=ele.split(",")
        temp_list=[]
        for feature in feature_list:
            feature=feature.strip()
            temp_list.append(feature)
        data.append(temp_list)
    # data_other,data=cross_validation.train_test_split(data,test_size=0.001,random_state=10)#为了减少代码运行时间，方便测试
    train_and_valid, test = cross_validation.train_test_split(data, test_size=0.2, random_state=10)
    train, valid = cross_validation.train_test_split(train_and_valid, test_size=0.01, random_state=10)
    train_feature, train_target = get_features_target(train)
    test_feature, test_target = get_features_target(test)
    valid_feature, valid_target = get_features_target(valid)

    params = {'n_estimators': 500, 'max_depth': 4, 'min_samples_split': 2}#这个参数不是万能的，应该针对特定模型
    params_RF = {'n_estimators': 500, 'max_depth': 4, 'min_samples_split': 2 ,"oob_score": True ,"random_state": 10}
    params_GBDT={'n_estimators': 500, 'max_depth': 10, 'min_samples_split': 2, "learning_rate": 0.01, "subsample": 0.6 ,"loss": "deviance" ,"random_state": 10}
    #由于GBDT使用了CART回归决策树，因此它的参数基本来源于决策树类，也就是说，和DecisionTreeClassifier和DecisionTreeRegressor的参数基本类似
    # clf=ensemble.GradientBoostingClassifier(**params_GBDT)#这个是GBDT分类器
    # clf = ensemble.GradientBoostingRegressor(**params)#这个是GBDT回归器
    # clf=ensemble.RandomForestRegressor(**params)#这个是随机森林回归器
    clf=ensemble.RandomForestClassifier(**params_RF)#这个是随机森林分类器
    #没有XGBoosting，没有LR。好像都是和树有关的。

    clf.fit(train_feature, train_target) #训练
    pre=clf.predict(test_feature)
    pre_prob=clf.predict_proba(test_feature)
    pre_prob_1=pre_prob[:,1]#返回label=1的概率
    pre_prob_0 = pre_prob[:, 0]  #返回label=-1的概率
    pre_list=list(pre)
    real_pre_zip=zip(test_target,pre_list)
    count=len(pre_list)
    precision=rmspe(real_pre_zip,count)
    print(precision)

run_demo()