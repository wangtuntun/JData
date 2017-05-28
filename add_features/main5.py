#encoding=utf-8
'''

训练集合数据 先统计再均衡，最近15天
预测集合4/15当天的pair

特征不要日期，不要brand

'''
import gc
import time
import  format_raw_data2model
import RF

data_dir="/public/home/scu1701/JData/Data5/"#server data path
dealed_data_dir="/public/home/scu1701/JData/DealedData5/"#server dealed data path
write_dir=dealed_data_dir+"change_feature/"
# data_dir="/home/wangtuntun/JData/Data/" #local data path
# dealed_data_dir="/home/wangtuntun/JData/DealedData/" #local dealed data path

feature_path = write_dir + "balanced"
predict_data_path = write_dir + "predict_0415_data"
result_path= write_dir + "rf_change_feature.csv"

print("开始格式化训练特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
feature_formatted_path = write_dir + "balanced_formatted"
format_raw_data2model.format_data(feature_path,feature_formatted_path)
gc.collect()
print("完成格式化训练特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))



print("开始格式化预测特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
predict_data_formatted_path = write_dir + "predict_0415_data_formatted"
format_raw_data2model.format_data(predict_data_path,predict_data_formatted_path)
gc.collect()
print("完成格式化预测特征数据时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


print("开始预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
RF.predict(feature_formatted_path,predict_data_formatted_path,result_path)
gc.collect()
print("完成预测时间")
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

