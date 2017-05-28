#encoding=utf-8
import gc
import time
import balance_train_data
import format_raw_data2model
import RF
import GBDT
data_dir="/public/home/scu1701/JData/Data5/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData5/"#server dealed data path
write_dir=dealed_data_dir+"change_feature/"
# data_dir="/home/wangtuntun/JData/Data/" #local data path
# dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path

pre_data_path = write_dir + "add_feature_predict_formatted.csv"

'''
# 1v10
print("1v10")
print("开始获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_path=write_dir + "feature_1v10"
balance_train_data.vs_1_10(feature_path)
gc.collect()
print("完成获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_formatted_path = write_dir + "feature_1v10_formatted"
format_raw_data2model.format_data(feature_path,feature_formatted_path)
gc.collect()
print("完成格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始GBDT预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
result_path = write_dir + "gbdt_1v10.csv"
GBDT.predict(feature_formatted_path,pre_data_path,result_path)
print("完成GBDT预测数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始RF预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
result_path = write_dir + "rf_1v10.csv"
RF.predict(feature_formatted_path,pre_data_path,result_path)
print("完成RF预测数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
'''

# 1v1  more
print("1v1  more")
print("开始获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_path=write_dir + "feature_1v1_more"
balance_train_data.vs_1_1_more(feature_path)
gc.collect()
print("完成获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_formatted_path = write_dir + "feature_1v1_more_formatted"
format_raw_data2model.format_data(feature_path,feature_formatted_path)
gc.collect()
print("完成格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始GBDT预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
result_path = write_dir + "gbdt_1v1_more.csv"
GBDT.predict(feature_formatted_path,pre_data_path,result_path)
print("完成GBDT预测数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始RF预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
result_path = write_dir + "rf_1v1_more.csv"
RF.predict(feature_formatted_path,pre_data_path,result_path)
print("完成RF预测数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


# 1v1 less
print("1v1 less")
print("开始获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_path=write_dir + "feature_1v1_less"
balance_train_data.vs_1_1_less(feature_path)
gc.collect()
print("完成获取特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_formatted_path = write_dir + "feature_1v1_less_formatted"
format_raw_data2model.format_data(feature_path,feature_formatted_path)
gc.collect()
print("完成格式化特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始GBDT预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
result_path = write_dir + "gbdt_1v1_less.csv"
GBDT.predict(feature_formatted_path,pre_data_path,result_path)
print("完成GBDT预测数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

print("开始RF预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
result_path = write_dir + "rf_1v1_less.csv"
RF.predict(feature_formatted_path,pre_data_path,result_path)
print("完成RF预测数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))