#encoding=utf-8
import gc
import time
import balance_train_data
import format_raw_data2model
import RF
import GBDT
data_dir="/public/home/scu1701/JData/Data4/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData4/"#server dealed data path
write_dir=dealed_data_dir+"change_train_data/"
# data_dir="/home/wangtuntun/JData/Data/" #local data path
# dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path

# 1v10
print("1v10")
print("开始获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_path=write_dir + "feature_1v10_gbdt"
balance_train_data.vs_1_10(feature_path)
gc.collect()
print("完成获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_formatted_path = write_dir + "feature_1v10_gbdt_formatted"
format_raw_data2model.format_data(feature_path,feature_formatted_path)
gc.collect()
print("完成格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
pre_data_path = write_dir + "predict_0415_data_formatted"
result_path = write_dir + "rf_1v10_gbdt.csv"
GBDT.predict(feature_formatted_path,pre_data_path,result_path)
print("完成预测数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

# 1v1  more
print("1v1  more")
print("开始获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_path=write_dir + "feature_1v1_more_gbdt"
balance_train_data.vs_1_1_more(feature_path)
gc.collect()
print("完成获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_formatted_path = write_dir + "feature_1v1_more_gbdt_formatted"
format_raw_data2model.format_data(feature_path,feature_formatted_path)
gc.collect()
print("完成格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
pre_data_path = write_dir + "predict_0415_data_formatted"
result_path = write_dir + "rf_1v1_more_gbdt.csv"
GBDT.predict(feature_formatted_path,pre_data_path,result_path)
print("完成预测数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


# 1v1 less
print("1v1 less")
print("开始获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_path=write_dir + "feature_1v1_less_gbdt"
balance_train_data.vs_1_1_less(feature_path)
gc.collect()
print("完成获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_formatted_path = write_dir + "feature_1v1_less_gbdt_formatted"
format_raw_data2model.format_data(feature_path,feature_formatted_path)
gc.collect()
print("完成格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
pre_data_path = write_dir + "predict_0415_data_formatted"
result_path = write_dir + "rf_1v1_less_gbdt.csv"
GBDT.predict(feature_formatted_path,pre_data_path,result_path)
print("完成预测数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))