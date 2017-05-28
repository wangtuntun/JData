#encoding=utf-8
'''
不同的训练集训练同一个模型，每个模型有不同的预测结果。这里将所有结果进行取均值操作，并将结果存入文件
'''
import numpy as np
import pandas as pd
import csv
import time


# data_dir="/public/home/scu1701/JData/Data2/"#server data path
# dealed_data_dir="/public/home/scu1701/JData/DealedData2/"#server dealed data path
data_dir="/home/wangtuntun/JData/Data/" #local data path
dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path
multi_dealed_data_dir=dealed_data_dir + "multi_train/"
#读取pair的位置
predict_pair_info_path_rf =  dealed_data_dir + "rf_pre_0415_info_pair.csv"
#合并后的结果存储位置
combine_result_path= dealed_data_dir + "rf_pre_0415_info_10_models.csv"


def run_demo(model_number):
    pair_info=pd.read_csv(predict_pair_info_path_rf)
    pair_number=len(pair_info)
    all_prob_array=np.zeros((pair_number,2))
    all_prob_df=pd.DataFrame(all_prob_array,columns=["pro_0","pro_1"])
    for i in range(model_number):
        model_pro_path=multi_dealed_data_dir + "rf_pre_0415_info" + str(i) + ".csv"
        model_prob_df=pd.read_csv(model_pro_path)
        all_prob_df += model_prob_df
    all_prob_df /= model_number
    all_prob_df.to_csv(combine_result_path,index=False)


model_number=10
run_demo(model_number)



