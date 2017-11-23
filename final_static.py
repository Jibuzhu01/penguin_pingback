#coding=gbk
__author__ = "yuanjun"
__writetime__ = "20171123"
import sys
import time

file_name = sys.argv[1]
total_num = 0
success_num = 0
time_spend = 0
with open(file_name) as fr:
    for line in fr:
        line = line.strip()
        feature_list = line.split('\t')
        for item in feature_list:
            item = item.split('###')
            if len(item)<2:
                continue
            label = item[0].strip()
            try:
                num = int(item[1].strip())
            except:
                num = 0
            if label == "WHNU":
                total_num += num
            if label == "SUNU":
                success_num += num
            if label == "TIME":
                time_spend = max(time_spend,num)
ftime = file_name[17:27]
out_str = ftime + '\t' + "total_num:" + str(total_num) + '\t' + "success_num:" + str(success_num) + '\t' + "time_spend:" + str(time_spend)
out_str += '\t' + str(float(success_num)/float(total_num))
print out_str