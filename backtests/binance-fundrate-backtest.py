# coding=utf-8
import asyncio
import aiohttp
import json
from datetime import datetime
import time
base_url = "https://api.binance.com/api/v3/"
base_url_dapi = "https://dapi.binance.com/dapi/v1/"
kline_req_url = base_url+"klines"
fundrate_req_url = base_url_dapi+"fundingRate"

# 初始本金
initfund = 100000


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

async def collectdata_calc():    
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
            fundstart = 0 # 真實的 fund start time
            fundend = 0 # 真實的 fund end time
            compoundfund = initfund # 累計本金          
            positiveFundTimes = 0 # 資費為正的次數
            totalFundTimes = 0  # 資費總次數
            while(t <= endTime):
                
                fundrate_url = fundrate_req_url+'?symbol='+ins+'USD_PERP&startTime='+str(t)+'&limit=1000'
                #print(fundrate_url )
                retr = await request(session,fundrate_url)
                arrobj_r = json.loads(retr)
                
                
                for fds in arrobj_r:
                    ktime = fds['fundingTime']
                    if(fundstart<1):
                        fundstart = ktime
                    #start = ktime - 1000*60
                    #end = ktime + 1000*60
                    #kline_url = kline_req_url+'?symbol='+ins+'USDT&interval='+itv+'&startTime='+str(start)+'&endTime='+str(end)+'&limit=1'
                    #ret = await request(session,kline_url)
                    #arrobj = json.loads(ret)
                    
                    #if(len(arrobj)<1):continue
                    #if(len(arrobj[0])<1):continue
                    
                    if(not ktime  in retdict[ins]):
                        #closeprice = float(arrobj[0][4])
                        fundrate = float(fds['fundingRate'])
                        if(fundrate>0):
                            positiveFundTimes +=1
                        totalFundTimes += 1
                        compoundfund += compoundfund*fundrate
                        
                        retdict[ins][ktime] = [fundrate,compoundfund-initfund]
                        print(ins + ':' + str(ktime) + ' fundrate:'+str(fundrate)+' compoundfund:'+str(compoundfund))
                
                if(len(arrobj_r)<1):
                     t
                    break
                t = arrobj_r[len(arrobj_r)-1]['fundingTime']+1000 # 拿最後一筆資料的收盘时间當作下一個的開頭
                print('current time '+ str(t) )
            
            retdict[ins]['compoundfund'] = compoundfund
            retdict[ins]['positiveFundTimes'] = positiveFundTimes
            retdict[ins]['totalFundTimes'] = totalFundTimes
        return retdict
    
async def backtest():
    fundhist = await collectdata_calc()
    print(fundhist)
    for ins in instruments:
        starttime = fundhist[ins][0]
        print('回測時間總費率')
        
    print('done')

if __name__ == "__main__":
    asyncio.run(backtest())
