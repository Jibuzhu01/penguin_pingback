#coding=gbk
__author__ = "yuanjun"
__writetime__ = "20171122"
import sys
import json
import traceback
import time
import requests
import urllib
import urllib2

def construct_match_feature_with_value(prefix, separator, value):
    return prefix + separator + str(value)

def get_accesstoken():
    params = {"grant_type":"clientcredentials","client_id":"2466ec015220c26c00daa61ba82850bf","client_secret":"111a579333ee9538aa734fa61bd1e9a5032c195e"}
    params_encode = urllib.urlencode(params)

    req_url = "https://auth.om.qq.com/omoauth2/accesstoken"
    req = requests.post(url=req_url, data=params)
    #print req
    try:
        req = urllib2.Request(url = req_url,data = params_encode)
        res_data = urllib2.urlopen(req)
    except:
        return ""
    res = res_data.read()
    print res

    res_dict = json.loads(res)
    res_data = res_dict.get("data","")

    if not res_data:
        print >> sys.stderr, "resp is empty"
        return ""
    else:
        accesstoken = res_data.get("access_token","")
        return accesstoken

def qier_pingback(accesstoken, msg_json, total_time):
#    print "access_token in qier_pingback ", accesstoken
    params = {"access_token":accesstoken, "msg":msg_json}
    params_encode = urllib.urlencode(params)
    req_url = "https://api.om.qq.com/data/receiveclient"
    url_start_time = int(time.time())
    req = urllib2.Request(url = req_url,data = params_encode)
    res_data = urllib2.urlopen(req)
    total_time += int(time.time()) - url_start_time
    res = res_data.read()
    res_dict = json.loads(res)
#   print res_dict
#   print res_dict["msg"].encode("utf-8","ignore")
    is_ok = res_dict["code"]
    if is_ok == 0:
        return True
    else:
        print res_dict["msg"].encode("utf-8", "ignore")
        return False

#accesstoken = get_accesstoken()
#print accesstoken 

def try_again():
    start_time = int(time.time())
    accesstoken = ""
    while accesstoken == "":
        try:
            accesstoken = get_accesstoken()
        except:
            continue
        if int(time.time())-start_time > 300:
            break
    print accesstoken
    return accesstoken
    
total_time = 0
start_time = int(time.time())
file_name = sys.argv[1]
success_num = 0
total_num = 0
accesstoken = ""
accesstoken = get_accesstoken()
fr = open(file_name)
reset_token_time = 3600
initialize_time = start_time
for line in fr:
#for line in sys.stdin:
    total_num += 1
    line = line.strip()
    if int(time.time()) - initialize_time > reset_token_time:
        initialize_time = int(time.time())
        accesstoken = ""
    try:
        if accesstoken == "":
            accesstoken = try_again()
        is_ok = qier_pingback(accesstoken, line, total_time)
    except:
        continue
    if is_ok == False:
        print line
    else:
        success_num += 1
    if  success_num % 200 == 0:
        ftime = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        print success_num," messages success! ", ftime
        print "url request time: %d seconds" % total_time

print "total num is %d" % total_num
print "total success num is %d" % success_num
end_time = int(time.time())
print end_time - start_time,"s consumed"
print "url request time: %d seconds" % total_time
feature_list = []
feature_list.append(construct_match_feature_with_value("WHNU","###",total_num))
feature_list.append(construct_match_feature_with_value("SUNU","###",success_num))
feature_list.append(construct_match_feature_with_value("TIME","###",end_time - start_time))
feature_str = "Final" + file_name + "\t" + "\t".join(feature_list)
print feature_str