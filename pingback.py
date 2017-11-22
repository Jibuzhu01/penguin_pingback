import requests
import urllib
import urllib2
import json
import sys

def get_accesstoken():
    params = {"grant_type":"clientcredentials","client_id":"2466ec015220c26c00daa61ba82850bf","client_secret":"111a579333ee9538aa734fa61bd1e9a5032c195e"}
    params_encode = urllib.urlencode(params)

    req_url = "https://auth.om.qq.com/omoauth2/accesstoken"
    req = requests.post(url=req_url, data=params)
    #print req
    req = urllib2.Request(url = req_url,data = params_encode)
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    #print res

    res_dict = json.loads(res)
    res_data = res_dict.get("data","")

    if not res_data:
        print >> sys.stderr, "resp is empty"
        return ""
    else:
        accesstoken = res_data.get("access_token","")
        return accesstoken

def qier_pingback(accesstoken, msg_json):
    params = {"access_token":accesstoken, "msg":msg_json}
    params_encode = urllib.urlencode(params)
    req_url = "https://api.om.qq.com/data/receiveclient"
    req = requests.post(url=req_url, data=params)
    req = urllib2.Request(url = req_url,data = params_encode)
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    res_dict = json.loads(res)
    is_ok = res_dict["code"]
    if is_ok == 0:
        return True
    else:
        return False
    #print res


if __name__ == "__main__":
    for line in sys.stdin:
        accesstoken = get_accesstoken()
        print accesstoken
        is_ok = qier_pingback(accesstoken, line)
        print is_ok