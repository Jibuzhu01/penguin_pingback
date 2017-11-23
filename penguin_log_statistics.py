#coding=gbk
__author__ = "yuanjun"
__writetime__ = "20171123"
import sys

file_name = sys.argv[1]
with open(file_name) as fr:
    for line in fr:
        line = line.strip()
        if "Final" not in line:
            continue
        print line
