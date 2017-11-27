#coding=gbk
__author__ = "yuanjun"
__writetime__ = "20171107"
import sys
import json
import traceback
import time
#from decode_req_log import decode_req_log
#from decode_resp_log import decode_resp_log

# ftime = time.strftime('%Y%m%d%H',time.localtime(time.time())) #1.ftime
ftime = int(time.time()) #1.ftime
article_action_dict = {3:"FAVOR",4:"NOTINTERESTED",5:"SHARE",6:"READ",8:"QUIT"}
netword_dict = {0:"未知",1:"wifi",3:"2G",4:"3G",5:"4G"}
subscribe_action_list = [12,13]
comment_action_list = [21,22,23]
terminal_dict = {"ios":"1","android":"2"}
action_list = [1,3,5,8]
action_dict = {0:0,1:7,3:10,5:9,8:2}
error_num = 0
req_num = 0
resp_num = 0

def decode_req_log(line):
    line = line.strip()
    #print line
    if not line:
        return False
    try:
        jo = json.loads(line,encoding="utf-8")
    except:
        print >> sys.stderr, "json loads failed..."
        #print line
    if "action" not in jo:
        return False
    if "target_item" not in jo:
        return False
    if "userinfo" not in jo:
        return False
    if "timestamp" not in jo:
        return False
    
    target_item = jo["target_item"]
    appendix = target_item.get("appendix","")
    if not appendix:
        #print "error"
        return False 
    appendix_list = appendix.split("|")
    if len(appendix_list) < 27:
        for i in range(27-len(appendix_list)):
            appendix_list.append("")
        #print "error"
        #return False
    (topic,keywords,mark,account,channel,art_source,account_openid,sub_topic,abtestid,position,image_type,aduser_flag,location,pagetime,rec_reason,adid,vulgar,sub_list,ip,recall_word,video_type,channel_id,doc_id,appmsgid,idx,new_sn,source_type) = appendix_list[0:27]
    if not account_openid.startswith("qie"): #首先判断是不是企鹅号的，不是就过了
        return False

    #1. √
    ts = int(jo["timestamp"]) #2.ts √
    #用户属性相关提取
    user_info_dict = jo["userinfo"]
    dev_info_dict = user_info_dict.get("dev_info",{})
    env_info_dict = user_info_dict.get("env_info",{})
    IP = env_info_dict.get("ip","") #3.IP
    if IP == "":
        IP = "null"
    imei = dev_info_dict.get("imei","") #4,imei
    imsi = dev_info_dict.get("imsi","") #5,imsi
    idfa = dev_info_dict.get("idfa","") #6.idfa
    if idfa == "":
        idfa = "null"
    if imei == "":
        imei = "null"
    if imsi == "":
        imsi = "null"
    idfv = "null" #7.idfv
    model = dev_info_dict.get("model","") #8.model
    if model == "":
        model = "null"
    mac = "null" #9.mac
    android_id = dev_info_dict.get("xid","") #10.adroid_id
    if android_id == "":
        android_id = "null"
    app_version = env_info_dict.get("app_ver","")#11.app_version
    os = env_info_dict.get("os","未知")#12.terminal
    os = os.strip().lower()
    if os == "":
        os = "未知"
#    terminal = terminal_dict.get(os,"0")
    terminal = os
    sub_terminal = terminal #13.sub_terminal
    network = env_info_dict.get("network","") #14.network
    network = netword_dict.get(network, "未知")
    wifi_ssid = "null" #15.wifi_ssid
    wifi_mac = "null" #16.wifi_mac
    
    #提取账号属性相关
    platform = env_info_dict.get("product","")#17.platform
    if platform == "":
        platform = "null"
    uin = "null" #18.uin

    #提取内容属性相关
    link = target_item.get("link","")
    id_length = 16 #doc_id为16位定长，如果不符合这个条件后续要修改这部分！！！
    try:
        doc_id = link.split("tencentdocid=")[1][0:id_length]
    except:
        return False
        
    video_time = 0 #20.video_time

    #提取操作行为
    sub_ch = "搜狗推荐流" #21.sub_ch
    source = 4 #22.source
    action = jo.get("action",0) #23.action

    try:
        action = int(action)
    except:
        action = 0
    if action not in action_list:
        action = 0
    action = action_dict[action]
    play_action = 0 #24.play_action，不存在
    time_long = jo.get("duration","0")#25.time_long
    try:
        time_long = int(time_long)
    except:
        time_long = 0
    final = jo.get("finished","") #26
    completed = 0
    if final == True:
        completed = 2
    if final == False:
        completed = 1
    data_source = "sogou"
    
    output_tuple = (ftime,ts,IP,imei,imsi,idfa,idfv,model,mac,android_id,app_version,terminal,sub_terminal,network,wifi_ssid,wifi_mac,platform,uin,doc_id,video_time,sub_ch,source,action,play_action,time_long,completed)
    output_dict = {"ts":ts,"IP":IP,"imei":imei,"imsi":imsi,"idfa":idfa,"idfv":idfv,"model":model,"mac":mac,"android_id":android_id,"app_version":app_version,"terminal":terminal,"sub_terminal":sub_terminal,"network":network,"wifi_ssid":wifi_ssid,"wifi_mac":wifi_mac,"platform":platform,"uin":uin,"doc_id":doc_id,"video_time":video_time,"sub_ch":"搜狗推荐流","source":source,"user_action":action,"play_action":play_action,"time_long":time_long,"complete":completed,"data_source":data_source}
    '''
    output_str = ""
    for item in output_tuple:
        try:
            output_str += str(item) + '\t'
        except:
            error_num += 1
            return False
    print output_str.encode('gbk','ignore')
    '''
    out_str = json.dumps(output_dict,encoding="gbk",ensure_ascii=False).encode("utf-8","ignore")
    print out_str

def decode_resp_log(line):
    line = line.strip()
    if not line:
        return False
    try:
        jo = json.loads(line,encoding="utf-8")
    except:
        print >> sys.stderr,"json loads failed..."
        return False
        #print line
    if "userinfo" not in jo:
        return False
    if "timestamp" not in jo:
        return False
    if "article_list" not in jo:
        return False

    article_list = jo["article_list"]

    if len(article_list) == 0:
        return False
    article_cnt = len(article_list)

    for i,article_info in enumerate(article_list):
        index_num = i
        appendix = article_info.get("appendix","")
        if not appendix:
            #print "error"
            continue
        appendix_list = appendix.split("|")
        if len(appendix_list) < 27:
            for i in range(27-len(appendix_list)):
                appendix_list.append("")
        (topic,keywords,mark,account,channel,art_source,account_openid,sub_topic,abtestid,position,image_type,aduser_flag,location,pagetime,rec_reason,adid,vulgar,sub_list,ip,recall_word,video_type,channel_id,doc_id,appmsgid,idx,new_sn,source_type) = appendix_list[0:27]
        if not account_openid.startswith("qie"): #首先判断是不是企鹅号的，不是就过了
            continue

        '''
        imsi = dev_info.get("imsi","")
        model = dev_info.get("model","unknown")
        OS = env_info.get("os","")
        app_ver = env_info.get("app_ver",-1)
        product = env_info.get("product","")
        distribution = env_info.get("distribution","-1")
        '''
        
        #1. √
        ts = int(jo["timestamp"])  # 2.ts √
        # 用户属性相关提取
        user_info_dict = jo["userinfo"]
        dev_info_dict = user_info_dict.get("dev_info", {})
        env_info_dict = user_info_dict.get("env_info", {})
        IP = env_info_dict.get("ip", "")  # 3.IP
        if IP == "":
            IP = "null"
        imei = dev_info_dict.get("imei", "")  # 4,imei
        imsi = dev_info_dict.get("imsi", "")  # 5,imsi
        idfa = dev_info_dict.get("idfa", "")  # 6.idfa
        if idfa == "":
            idfa = "null"
        if imei == "":
            imei = "null"
        if imsi == "":
            imsi = "null"
        idfv = "null"  # 7.idfv
        model = dev_info_dict.get("model", "")  # 8.model
        if model == "":
            model = "null"
        mac = "null"  # 9.mac
        android_id = dev_info_dict.get("xid", "")  # 10.adroid_id
        if android_id == "":
            android_id = "null"
        app_version = env_info_dict.get("app_ver", "")  # 11.app_version
        os = env_info_dict.get("os", "未知")  # 12.terminal
        os = os.strip().lower()
        if os == "":
            os = "未知"
            #    terminal = terminal_dict.get(os,"0")
        terminal = os
        sub_terminal = terminal  # 13.sub_terminal
        network = env_info_dict.get("network", "")  # 14.network
        network = netword_dict.get(network, "未知")
        wifi_ssid = "null"  # 15.wifi_ssid
        wifi_mac = "null"  # 16.wifi_mac

        # 提取账号属性相关
        platform = env_info_dict.get("product", "")  # 17.platform
        if platform == "":
            platform = "null"
        uin = "null"  # 18.uin

        #提取内容属性相关

        link = article_info.get("link","")
        id_length = 16 #doc_id为16位定长，如果不符合这个条件后续要修改这部分！！！

        try:
            doc_id = link.split("tencentdocid=")[1][0:id_length]
        except:
            continue

        video_time = 0 #20.video_time

        #提取操作行为
        sub_ch = "搜狗推荐流"  # 21.sub_ch
        source = 4  # 22.source
        action = 1 # 23 action
        play_action = 0  # 24.play_action，不存在
        time_long = jo.get("duration", "0")  # 25.time_long
        try:
            time_long = int(time_long)
        except:
            time_long = 0
        final = jo.get("finished", "")  # 26
        completed = 0
        if final == True:
            completed = 2
        if final == False:
            completed = 1
        data_source = "sogou"
        
        output_tuple = (ftime,ts,IP,imei,imsi,idfa,idfv,model,mac,android_id,app_version,terminal,sub_terminal,network,wifi_ssid,wifi_mac,platform,uin,doc_id,video_time,sub_ch,source,action,play_action,time_long,completed)
        output_dict = {"ts":ts,"IP":IP,"imei":imei,"imsi":imsi,"idfa":idfa,"idfv":idfv,"model":model,"mac":mac,"android_id":android_id,"app_version":app_version,"terminal":terminal,"sub_terminal":sub_terminal,"network":network,"wifi_ssid":wifi_ssid,"wifi_mac":wifi_mac,"platform":platform,"uin":uin,"doc_id":doc_id,"video_time":video_time,"sub_ch":sub_ch,"source":source,"user_action":action,"play_action":play_action,"time_long":time_long,"complete":completed,"data_source":data_source}
        
        '''
        output_str = ""
        for item in output_tuple:
            try:
                output_str += str(item) + '\t'
            except:
                error_num += 1
                return False
        print output_str.encode('gbk','ignore')
        '''

        out_str = json.dumps(output_dict, encoding="gbk", ensure_ascii=False).encode("utf-8", "ignore")
        print out_str

'''
test_file = "resp_demo"
num = 0
fr = open(test_file)
for line in fr:
    num += 1
    if num > 1:
        break
        '''
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        jo = json.loads(line,encoding='utf-8')
        if not jo:
            continue
        if "action" not in jo and "article_list" not in jo:
            continue
        if "action" in jo:
            action = int(jo.get("action",-1))
            if action in action_list:
                req_num += 1
                decode_req_log(line)
        elif "article_list" in jo:
            resp_num += 1
            decode_resp_log(line)
        else:
            continue
    except:
        traceback.print_exc()
        continue
print >> sys.stderr,error_num
print >> sys.stderr,req_num
print >> sys.stderr,resp_num
