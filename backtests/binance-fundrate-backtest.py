# coding=utf-8
import asyncio
import aiohttp
import json
from datetime import datetime
import time
import csv
import codecs
base_url = "https://api.binance.com/api/v3/"
base_url_dapi = "https://dapi.binance.com/dapi/v1/"
kline_req_url = base_url+"klines"
fundrate_req_url = base_url_dapi+"fundingRate"
itv='8h'

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
                    if(fundstart<1):fundstart = ktime
                    
                    
                    if(not ktime  in retdict[ins]):
                        
                        fundrate = float(fds['fundingRate'])
                        if(fundrate>0):
                            positiveFundTimes +=1
                        totalFundTimes += 1
                        compoundfund += compoundfund*fundrate
                        
                        retdict[ins][ktime] = [fundrate,compoundfund-initfund] #,closeprice
                        print(ins + ':' + str(ktime) + ' fundrate:'+str(fundrate)+' compoundfund:'+str(compoundfund))
                
                if(len(arrobj_r)<1):
                    if(fundend<1):fundend = t
                    break
                t = arrobj_r[len(arrobj_r)-1]['fundingTime']+1000 # 拿最後一筆資料的收盘时间當作下一個的開頭
                print('current time '+ str(t) )
            
            retdict[ins]['fundstart'] = fundstart
            retdict[ins]['fundend'] = fundend
            retdict[ins]['compoundfund'] = compoundfund
            retdict[ins]['positiveFundTimes'] = positiveFundTimes
            retdict[ins]['totalFundTimes'] = totalFundTimes
            #time.sleep(15)
        return retdict
    
async def backtest():
    fundhist = await collectdata_calc()
    
    file_object = codecs.open('fundrate_report.txt', 'w', "utf-8")
    file_object.write('')
    file_object.close()
    
    for ins in instruments:
        starttime = fundhist[ins]['fundstart'] / 1000
        endtime = fundhist[ins]['fundend'] / 1000
        starttime_dt = datetime.fromtimestamp(starttime)
        endtime_dt = datetime.fromtimestamp(endtime)
        
        durationDays = (fundhist[ins]['fundend'] - fundhist[ins]['fundstart']) / 86400000
        onedayret = (fundhist[ins]['compoundfund'] - initfund)/durationDays/initfund
        yearret = onedayret * 365 * 100
        yearret_str = "{:.2f}".format(yearret)
        
        positiveRatio = (fundhist[ins]['positiveFundTimes'] / fundhist[ins]['totalFundTimes'])*100
        positiveRatio_str = "{:.2f}".format(positiveRatio)
        
        netReturn = fundhist[ins]['compoundfund'] - initfund
        netReturn_str = "{:.2f}".format(netReturn)
        grossRate = (netReturn / initfund)/durationDays*365*100
        grossRate_str = "{:.2f}".format(grossRate)
        
        msg = u''
        msg += ins + 'USDT \n'
        msg += u'回測時間:'+str(starttime_dt)+' to '+str(endtime_dt)+ ' 共 ' +str(int(durationDays)) + ' 天 \n'
        msg += u'初始資金:'+ str(initfund) +'USD\n'
        msg += u'費率為正次數:'+ str(fundhist[ins]['positiveFundTimes'])+'\n'
        msg += u'總領費率次數:'+ str(fundhist[ins]['totalFundTimes'])+'\n'
        msg += u'費率正比率:'+ positiveRatio_str +'%\n'
        msg += u'淨利:'+ netReturn_str +'USD\n'
        msg += u'毛利率:'+ grossRate_str +'%\n\n'
        
        file_object = codecs.open('fundrate_report.txt', 'a', "utf-8")
        file_object.write(msg)
        file_object.close()        
        
        # write csv
        with open( (ins+'.csv'), mode='w') as frate_file:
            frate_file = csv.writer(frate_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            frate_file.writerow(['time', 'fundrate', 'netprofit'])
            for key in fundhist[ins]:
                if(type(key) != type(1)):continue
                rate_ampl = fundhist[ins][key][0] * 3 * 365 * 100
                frate_file.writerow([ key , rate_ampl , fundhist[ins][key][1] ]) #fundhist[ins][key][2]]
        
        # write underlying price 
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            kline_url = kline_req_url+'?symbol='+ins+'USDT&interval='+itv+'&startTime='+str( fundhist[ins]['fundstart'] )+'&endTime='+str(fundhist[ins]['fundend'])+'&limit=1000'
            ret = await request(session,kline_url)
            arrobj = json.loads(ret)
            if(len(arrobj)<1):return
            with open( (ins+'_price.csv'), mode='w') as fprice_file:
                fprice_file = csv.writer(fprice_file , delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                fprice_file.writerow(['time', 'price'])
                for kline in arrobj:
                    closeprice = float(kline[4])
                    time = kline[0]
                    fprice_file.writerow([time,closeprice])
        
    print('done')

if __name__ == "__main__":
    asyncio.run(backtest())
