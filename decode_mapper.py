#coding=gbk
import sys
import json
import traceback
#from decode_req_log import decode_req_log
#from decode_resp_log import decode_resp_log

article_action_dict = {3:"FAVOR",4:"NOTINTERESTED",5:"SHARE",6:"READ",8:"QUIT"}

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
    action = jo["action"]

    #if action not in article_action_dict:
    #    return False

    tm = jo["timestamp"]

    action_source = jo.get("source","")

    target_item = jo["target_item"]

    read_duration = jo.get("duration",-1)

    title = target_item.get("title","")
    '''
    if not title:
        return False
    '''
    url = target_item.get("link","")
    '''
    if not url:
        return False
    docid = hash_md5(url)
    if not docid:
        return False
    '''
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

    #print source_type
    dev_info = jo["userinfo"].get("dev_info",{})
    if not dev_info:
        return False

    env_info = jo["userinfo"].get("env_info",{})
    if not env_info:
        return False

    mid = dev_info.get("mid","")
    model = dev_info.get("model","unknown")
    imsi = dev_info.get("imsi","")
    OS = env_info.get("os","")
    app_ver = env_info.get("app_ver",-1)
    product = env_info.get("product","")
    distribution = env_info.get("distribution","-1")

    userinfo = ""
    try:
        userinfo = json.dump(dev_info,ensure_ascii=False)
    except:
        userinfo = ""

    if not mid:
        return False

    #output_str = mid + "\t" str(tm) + "\t" + str(action) + "\t" + docid + "\t" + title 
    #output_app = "\t".join(appendix_list)

    output_tuple = ("req",mid,str(tm),str(action),topic,mark,title,keywords,userinfo,imsi,url,account,channel,art_source,OS,account_openid,abtestid,sub_topic,image_type,str(read_duration),position,str(app_ver),aduser_flag,location,pagetime,rec_reason,adid,vulgar,sub_list,ip,action_source,recall_word,video_type,channel_id,doc_id,product,source_type,distribution,model)

    output_str = "\t".join(output_tuple)
    #print doc_id
    print output_str.encode('gbk','ignore')

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
    tm = jo["timestamp"]

    article_list = jo["article_list"]

    if len(article_list) == 0:
        return False
    article_cnt = len(article_list)

    dev_info = jo["userinfo"].get("dev_info",{})
    if not dev_info:
        return False
    env_info = jo["userinfo"].get("env_info",{})
    if not env_info:
        return False

    mid = dev_info.get("mid","")
    if not mid:
        return False

    imsi = dev_info.get("imsi","")
    model = dev_info.get("model","unknown")
    OS = env_info.get("os","")
    app_ver = env_info.get("app_ver",-1)
    product = env_info.get("product","")
    distribution = env_info.get("distribution","-1")

    userinfo = ""
    try:
        userinfo = json.dump(dev_info,ensure_ascii=False)
    except:
        userinfo = ""

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
            #print "error"
            #continue
        (topic,keywords,mark,account,channel,art_source,account_openid,sub_topic,abtestid,position,image_type,aduser_flag,location,pagetime,rec_reason,adid,vulgar,sub_list,ip,recall_word,video_type,channel_id,doc_id,appmsgid,idx,new_sn,source_type) = appendix_list[0:27]

        (topic,keywords,mark,account,channel,art_source,account_openid,sub_topic,abtestid,position,image_type,aduser_flag,location,pagetime,rec_reason,adid,vulgar,sub_list,ip,recall_word,video_type,channel_id,doc_id,appmsgid,idx,new_sn,source_type) = appendix_list[0:27]
        #print source_type
        #过滤相关文章、订阅页、网页信息流
        if channel_id == "50" or channel_id == "51" or channel_id == "1001":
            continue
        reason = "None"
        title = article_info.get("title","")
        read_num = article_info.get("read_num","")
        pub_time = article_info.get("pub_time","")
        img_list = "\a".join(article_info.get("img_list",[]))
        url = article_info.get("link","")

        output_tuple = ("resp",mid,str(tm),str(article_cnt),str(index_num),mark,title,reason,str(read_num),topic,keywords,str(pub_time),image_type,img_list,url,account,channel,art_source,account_openid,abtestid,sub_topic,userinfo,position,str(app_ver),aduser_flag,location,pagetime,rec_reason,adid,vulgar,sub_list,ip,recall_word,video_type,channel_id,doc_id,product,source_type,distribution,model)

        output_str = "\t".join(output_tuple)
        print output_str.encode('gbk','ignore')
        #return output_str.encode('gbk','ignore')

def decode_comment_log(line):
    line = line.strip()
    #print line
    if not line:
        return False
    try:
        jo = json.loads(line,encoding="utf-8")
    except:
        print >> sys.stderr, "json loads failed..."
        return False 
    if "action" not in jo:
        return False
    if "userinfo" not in jo:
        return False
    if "timestamp" not in jo:
        return False
    action = jo["action"]

    tm = jo["timestamp"]

    content = jo.get("content","")
    if not content:
        content = "empty"

    docid = jo.get("topicid","")
    if not docid:
        return False
    
    dev_info = jo["userinfo"].get("dev_info",{})
    if not dev_info:
        return False

    mid = dev_info.get("mid","")
    if not mid:
        return False

    output_tuple = ("comment",mid,str(tm),str(action),docid,content)
    output_str = "\t".join(output_tuple)
    print output_str.encode("gbk","ignore")
    #return output_str.encode("gbk","ignore")

def decode_subscribe_log(line):
    line = line.strip()
    if not line:
        return False
    try:
        jo = json.loads(line,encoding="utf-8")
    except:
        print >> sys.stderr, "json loads failed..."
        traceback.print_exc()
        return False
    if "action" not in jo:
        return False
    if "target_item" not in jo:
        return False
    if "userinfo" not in jo:
        return False
    if "timestamp" not in jo:
        return False
    action = jo["action"]
    tm = jo["timestamp"]
    action_source = jo.get("source","")
    target_item = jo["target_item"]
    
    id = target_item.get("id","")
    name = target_item.get("name","")
    sub_type = target_item.get("type",0)

    if not id:
        return False

    dev_info = jo["userinfo"].get("dev_info",{})
    if not dev_info:
        return False

    env_info = jo["userinfo"].get("env_info",{})
    if not env_info:
        return False

    mid = dev_info.get("mid","")
    product = env_info.get("product","")

    if not mid:
        return False

    output_tuple = ("subscribeact",mid,str(tm),str(action),id,name,str(sub_type),action_source,product)

    output_str = "\t".join(output_tuple)
    print output_str.encode('gbk','ignore')
    #return output_str.encode('gbk','ignore')

subscribe_action_list = [12,13]
comment_action_list = [21,22,23]

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
            if action in subscribe_action_list:
                decode_subscribe_log(line)
            elif action in comment_action_list:
                decode_comment_log(line)
            else:
                decode_req_log(line)
        elif "article_list" in jo:
            decode_resp_log(line)
        else:
            continue
    except:
        traceback.print_exc()
        continue

