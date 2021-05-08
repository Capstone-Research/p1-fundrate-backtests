# coding=utf-8
import sys
import requests
import json
import concurrent.futures
from datetime import datetime
import time

base_url = "https://api.binance.com/api/v3/"
base_url_dapi = "https://dapi.binance.com/api/v1/"
kline_req_url = base_url+"klines"
fundrate_req_url = base_url+"fundingRate"

instruments=['BTCUSDT','ETHUSDT','EOSUSDT','LTCUSDT']
timeIntervals=['5m','15m','1h','12h']


def combine_urls():
    ret = []
    now = datetime.now()
    endTime = str(int(time.mktime(now.timetuple())*1e3))
    for ins in instruments:
        for itv in timeIntervals:
            _url = kline_req_url+'?symbol='+ins+'&interval='+itv+'&endTime='+endTime+'&limit=1000'
            ret.append({'path':_url,'instrument':ins,'interval':itv})
    return ret

def load_url(url, timeout):
    headers = {'content-type': 'application/json'}
    return requests.get(url, timeout = timeout)

def combine_records(url,kline):
    ins=url['instrument']
    interval=url['interval']
    
    mongoclient = MongoClient( mongoclient_uri + "/binance")
    db = mongoclient['binance']
    collection = db[ ins+'-'+interval ]
    updatecmd = "collection.update_one("
    updatecmd += "{'timestamp':'"+str(kline[0])+"'},"
    updatecmd += "{'$setOnInsert':{"
    updatecmd += "'open':'"+str(kline[1])+"',"
    updatecmd += "'high':'"+str(kline[2])+"',"
    updatecmd += "'low':'"+str(kline[3])+"',"
    updatecmd += "'close':'"+str(kline[4])+"',"
    updatecmd += "'vol':'"+str(kline[5])+"',"
        
    updatecmd += '}}, upsert = True)'
    print(updatecmd) 
    exec(updatecmd)    


def fetch_kline():
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        resp_err=0
        future_to_url = {executor.submit(load_url, url['path'], 10): url for url in combine_urls()}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                jsonObj = json.loads(future.result().text) 
                for kline in jsonObj:
                    combine_records(url,kline)
            except Exception as exc:
                resp_err = resp_err + 1

if __name__ == "__main__":
    fetch_kline()