# coding=utf-8
import asyncio
import aiohttp
import json
from datetime import datetime
import time
base_url = "https://api.binance.com/api/v3/"
base_url_dapi = "https://dapi.binance.com/api/v1/"
kline_req_url = base_url+"klines"
fundrate_req_url = base_url+"fundingRate"


instruments=['BTC',
    'ETH',
    'LINK',
    'BNB',
    'TRX', 
    'DOT',
    'ADA', 
    'EOS',
    'LTC',
    'BCH',
    'XRP',
    'ETC',
    'FIL',
    'EGLD',
    'DOGE',
    'UNI',
    'THETA', 
    'XLM']

async def request(session,url):
    headers = {'content-type': 'application/json'}
    async with session.get(url,headers=headers) as res:
        return await res.text()

async def collectdata():    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        now = datetime.now()
        itv='8h'
        startTime = 1483228800000 # 2017/1/1
        endTime = int(time.mktime(now.timetuple())*1e3)
        retdict = {}
        
        for ins in instruments:
            retdict[ins] = {}
        for ins in instruments:
            t = startTime
            while(t <= endTime):
                _url = kline_req_url+'?symbol='+ins+'USDT&interval='+itv+'&startTime='+str(t)+'&limit=1000'
                #print(_url)
                ret = await request(session,_url)
                arrobj = json.loads(ret)
                
                for kbar in arrobj:
                    ktime = kbar[0]
                    if(not ktime  in retdict[ins]):
                        retdict[ins][ktime] = [float(kbar[4])]
                    
                t = arrobj[len(arrobj)-1][6] # 拿最後一筆資料的收盘时间當作下一個的開頭
        return retdict
    
async def backtest():
    klines = await collectdata()
    print(klines)
    print('done')

if __name__ == "__main__":
    asyncio.run(backtest())
