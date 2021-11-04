# coding=utf-8
import sys
import asyncio
import aiohttp
import json
from datetime import datetime
import pandas as pd
import time
import csv
import codecs
import math
import csv

base_url = "https://api.binance.com/api/v3/"
kline_req_url = base_url+"klines"
itv='15m'

# 初始本金
initfund = 100000


instruments=['BTC',
    'ETH',
    'DOGE',
]

async def request(session,url):
    headers = {'content-type': 'application/json'}
    async with session.get(url,headers=headers) as res:
        return await res.text()

async def collectdata_calc():    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        now = datetime.now()
        ONE_DAY = 86400000
        endTime = int(time.mktime(now.timetuple())*1e3)
        for ins in instruments:
            startTime = 1502942400000 # 2017年8月17日星期四 12:00:00
            # collect kline 
            t = startTime
            all_klines = []
            while(t <= endTime):
                kline_url = kline_req_url+'?symbol='+ins+'USDT&interval='+itv+'&startTime='+str(t)+'&limit=1000'
                retr = await request(session,kline_url)
                arrobj = json.loads(retr)
                if(len(arrobj)<1):return
                startTime = arrobj[0][0]
                if(len(arrobj)<1):
                    if(fundend<1):fundend = t
                    break
                t = arrobj[-1][6]+10 #
                all_klines += arrobj
            
            df = pd.DataFrame(all_klines)
            df.to_csv(ins+'_15m.csv')
        
def backTestFromCsv(ins):
    ONE_DAY = 86400000
    FIVE_DAYS = 86400000*5
    
    # #### 參數
    # 根據黃的算式應該是使用不超過總資金的 20%
    riskratio = 0.2
    leverage = 2
    
    all_klines = []
    with open(ins+'_15m.csv') as file_name:
        file_read = csv.reader(file_name)
        all_klines = list(file_read)
    all_klines.pop(0)
    compoundfund = initfund # 累計本金          
    positiveFundTimes = 0 # 勝率 資費為正的次數 
    totalTradeTimes = 0  # 總交易次數
    
    # mdd
    hh = -9999
    dd = 9999
    mdd = 9999
    
    # 日內最大回撤
    prvdaynetprofit = -9999
    dmdd = 9999
    
    # 最長未創高區間
    lastHHTimestamp = 0
    longestHHPeriod = -9999
    
    
    # 波動率
    avgfundrate = 0
    prvcompfund = 0
    avgvolatility = 0
    
    curposition = []
    timestart = float(all_klines[0][0]) 
    daystamp = float(all_klines[0][0])
    
    for kl in all_klines:
        curtime = float(kl[0][0])
        # 累積足夠數量的指標，需要放棄前5天
        if(curtime - timestart < FIVE_DAYS):continue
        
        if(curtime - daystamp >= ONE_DAY):
            print('one day has passed , current time = '+ str(t))
            daystamp = curtime
            curprice = float(kl[4])
            
            # 確認是否餘額風控後足夠可開倉
            avlbalance = 0
            notiontotal = 0
            for po in curposition:
                notiontotal += (curprice - po['entryprice'])*po['size']
                
            # 拿累加的總資產來開倉
            if((notiontotal/compoundfund)<riskratio):
               
                endprice = float(kl[0][4])
                
                # 計算低位
                # 計算高位
                size = compoundfund * riskratio * leverage / curprice
                pp = {''}
            
                totalTradeTimes += 1
                
            pnlToday = (endprice - entryprice) * size
            compoundfund += pnlToday
            
            
            # 計算 d_dd 日內波動
            d_dd = todaynetprofit-prvdaynetprofit
            if(d_dd < dmdd):dmdd = d_dd # d_dd
            prvdaynetprofit = todaynetprofit
            
            
            
            # 計算 mdd 創高區間
            if(compoundfund > hh):
                hh=compoundfund
                if(lastHHTimestamp<1):lastHHTimestamp=ktime
                period = (ktime - lastHHTimestamp)
                if(period > longestHHPeriod):
                    longestHHPeriod = period
                lastHHTimestamp = ktime
            elif(compoundfund < hh):
                dd = compoundfund - hh
                if(dd < mdd):mdd=dd
            
            # 波動累加 報酬率累加
            if(prvcompfund<1):prvcompfund = compoundfund
            avgvolatility += abs(compoundfund - prvcompfund) / prvcompfund
            avgfundrate += fundrate
            prvcompfund = compoundfund
                
        
        
        
        
    # 計算績效
    avgfundrate /= totalTradeTimes
    avgvolatility /= totalTradeTimes
    winrate = (positiveFundTimes / totalTradeTimes)*100
    
    # sharp
    def variance(data, ddof=0):
        n = len(data)
        mean = sum(data) / n
        return sum((x - mean) ** 2 for x in data) / (n - ddof)
    def stdev(data):
        var = variance(data)
        std_dev = math.sqrt(var)
        return std_dev            
    sharpe = avgfundrate / stdev(fundratecoll)
    
    
    retdict[ins]['fundstart'] = fundstart
    retdict[ins]['fundend'] = fundend
    
    retdict[ins]['positiveFundTimes'] = positiveFundTimes
    retdict[ins]['totalTradeTimes'] = totalTradeTimes # 盈利次數
    
    
    retdict[ins]['compoundfund'] = compoundfund  # 總報酬
    retdict[ins]['winrate'] = winrate # 勝率
    retdict[ins]['longestHHPeriod'] = longestHHPeriod/86400000 # 創高區間
    retdict[ins]['mdd'] = (mdd/initfund)*100 # mdd
    retdict[ins]['dmdd'] = (dmdd/initfund)*100 # dmdd
    retdict[ins]['sharpe'] = sharpe # sharpe
    retdict[ins]['avgvolatility'] = avgvolatility # avgvolatility
    
    
    return retdict
    
def backtest():
    for ins in instruments:
        fundhist = backTestFromCsv(ins)
    
    file_object = codecs.open('marting_report.txt', 'w', "utf-8")
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
        
        positiveRatio = (fundhist[ins]['positiveFundTimes'] / fundhist[ins]['totalTradeTimes'])*100
        positiveRatio_str = "{:.2f}".format(positiveRatio)
        
        netReturn = fundhist[ins]['compoundfund'] - initfund
        netReturn_str = "{:.2f}".format(netReturn)
        grossRate = (netReturn / initfund)/durationDays*365*100
        grossRate_str = "{:.2f}".format(grossRate)
        
        msg = u''
        msg += ins + 'USDT \n'
        msg += u'回測時間:'+str(starttime_dt)+' to '+str(endtime_dt)+ ' 共 ' +str(int(durationDays)) + ' 天 \n'
        msg += u'初始資金:'+ str(initfund) +'USD\n'
        msg += u'賺錢次數:'+ str(fundhist[ins]['positiveFundTimes'])+'\n'
        msg += u'總交易次數:'+ str(fundhist[ins]['totalTradeTimes'])+'\n'
        msg += u'勝率:'+ positiveRatio_str +'%\n'
        msg += u'總利潤:'+ netReturn_str +'USD\n'
        msg += u'最大創高區間:'+ ("{:.2f}".format(fundhist[ins]['longestHHPeriod'])) +'天\n'
        msg += u'最大拉回:'+ ("{:.2f}".format(fundhist[ins]['mdd'])) +'%\n'
        msg += u'每日最大拉回:'+ ("{:.2f}".format(fundhist[ins]['dmdd'])) +'%\n'
        msg += u'夏普比率:'+ ("{:.2f}".format(fundhist[ins]['sharpe'])) +'\n'
        msg += u'波動率:'+ ("{:.2f}".format(fundhist[ins]['avgvolatility'])) +'%\n'
        msg += u'每月報酬:'+ ("{:.2f}".format(grossRate/12.0)) +'%\n'
        msg += u'年化報酬:'+ grossRate_str +'%\n\n'
        
        
        file_object = codecs.open('fundrate_report.txt', 'a', "utf-8")
        file_object.write(msg)
        file_object.close()        
        
        
        

    # combine data
    timestampcol = []
    timeprice = {}
    for key in fundhist[ins]:
        if(type(key) != type(1)):continue
        timestampcol.append(key)
    for kline in arrobj:
        time = kline[0]
        if(not time in timeprice):
            timeprice[time] = kline[4]
        timestampcol.append(time)
    
    # 
    timestampcol.sort()
    
    
    with open( (ins+'_price.csv'), mode='w') as fprice_file:
        fprice_file = csv.writer(fprice_file , lineterminator='\n',  delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        fprice_file.writerow(['time','fundrate','netprofit','price'])
    
        prvrate = 0
        prvnetprofit = 0
        prvprice = 0
        for tt in timestampcol:
            if(tt in fundhist[ins]):
                prvrate = fundhist[ins][tt][0] * 3 * 365 * 100
                prvnetprofit = fundhist[ins][tt][1]
            elif(tt in timeprice):
                prvprice = timeprice[tt]
            _dt = datetime.fromtimestamp(tt/1000)
            fprice_file.writerow([_dt,prvrate,prvnetprofit,prvprice])
    
    
      
      
    
    
    
    
    
    
    # 綜合績效
    # 目前人工挑選，長遠來看具備上漲基本面及
    # 及歷史回測績效較好的幣種
    coinlist = ['ETH','EGLD','DOGE','DOT','LTC']
    coinweight = {'ETH':0.18,'EGLD':0.35,'DOGE':0.24,'DOT':0.1,'LTC':0.13}
    
    
    yearret = 0
    positiveRatio = 0
    mdd = 9999
    dmdd = 9999
    sharpe = 0
    avgvolatility = 0
    netReturn = 0
    grossRate = 0
    durationDays = 0
    starttime_dt = None
    endtime_dt = None
    timestart = 29207694050000
    timeend = -9999
    positiveFundTimes = 0
    totalTradeTimes = 0
    for ins in coinlist:
        ndays = (fundhist[ins]['fundend'] - fundhist[ins]['fundstart']) / 86400000
        starttime = fundhist[ins]['fundstart']/1000
        endtime = fundhist[ins]['fundend']/1000
        if(ndays>durationDays):durationDays=ndays
        if(starttime < timestart):
            starttime_dt = datetime.fromtimestamp(starttime)
            timestart = starttime
        if( endtime > timeend):
            endtime_dt = datetime.fromtimestamp(endtime)
            timeend = endtime
        
        onedayret = (fundhist[ins]['compoundfund'] - initfund)/ndays/initfund
        yearret += onedayret * 365 * 100 * coinweight[ins]
        positiveFundTimes += fundhist[ins]['positiveFundTimes']
        totalTradeTimes += fundhist[ins]['totalTradeTimes']        
        positiveRatio += (fundhist[ins]['positiveFundTimes'] / fundhist[ins]['totalTradeTimes']) * 100 * coinweight[ins]
        netReturn += (fundhist[ins]['compoundfund'] - initfund) * coinweight[ins]
        grossRate += ((netReturn / initfund)/ndays*365*100) * coinweight[ins]
        sharpe += fundhist[ins]['sharpe'] * coinweight[ins]
        avgvolatility += fundhist[ins]['avgvolatility'] * coinweight[ins]
        if(fundhist[ins]['mdd']<mdd ):mdd=fundhist[ins]['mdd']
        if(fundhist[ins]['dmdd']<dmdd ):dmdd=fundhist[ins]['dmdd']
    
    
    msg = u''
    msg += "'ETH','EGLD','DOGE','DOT','LTC' 綜合費率套利績效\n"
    msg += u'回測時間:'+str(starttime_dt)+' to '+str(endtime_dt)+ ' 共 ' +str(int(durationDays)) + ' 天 \n'
    msg += u'初始資金:'+ str(initfund) +'USD\n'
    msg += u'費率為正次數:'+ str(positiveFundTimes)+'\n'
    msg += u'總領費率次數:'+ str(totalTradeTimes)+'\n'
    msg += u'勝率:'+  ("{:.2f}".format(positiveRatio)) +'%\n'
    msg += u'總利潤:'+ ("{:.2f}".format(netReturn)) +'USD\n'
    msg += u'最大拉回:'+ ("{:.2f}".format(mdd)) +'%\n'
    msg += u'每日最大拉回:'+ ("{:.2f}".format(dmdd)) +'%\n'
    msg += u'夏普比率:'+ ("{:.2f}".format(sharpe)) +'\n'
    msg += u'波動率:'+ ("{:.2f}".format(avgvolatility)) +'%\n'
    msg += u'每月報酬:'+ ("{:.2f}".format(grossRate/12.0)) +'%\n'
    msg += u'年化報酬:'+ ("{:.2f}".format(grossRate)) +'%\n\n'
    
    file_object = codecs.open('fundrate_backtest_combine.txt', 'w', "utf-8")
    file_object.write(msg)
    file_object.close()        
    
    timestampcol = []
    for ins in coinlist:
        for key in fundhist[ins]:
            if(type(key) != type(1)):continue
            timestampcol.append(key)
    
    timestampcol.sort() 
    with open( ('combine_return.csv'), mode='w') as fprice_file:
        fprice_file = csv.writer(fprice_file , lineterminator='\n', delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        fprice_file.writerow(['time','fundrate','netprofit'])

        prvrate = {}
        prvnetprofit = {}
        for tt in timestampcol:
            for ins in coinlist:
                if(tt in fundhist[ins]):
                    prvrate[ins] = (fundhist[ins][tt][0] * 3 * 365 * 100) * coinweight[ins]
                    prvnetprofit[ins] = (fundhist[ins][tt][1]) * coinweight[ins]
            
            def _sum(arr):
                sum=0
                for i in arr:
                    sum = sum + i
                return(sum)               
            rate = _sum( list(prvrate.values()) )
            netprofit = _sum( list(prvnetprofit.values()) )
            _dt = datetime.fromtimestamp(tt/1000)
            dtformat = _dt.strftime('%Y-%m-%d %H:%M:%S')
            fprice_file.writerow([dtformat,rate,netprofit])

    
    print('done')

if __name__ == "__main__":
    mode = sys.argv[1]
    if(mode=='0'):
        asyncio.run(collectdata_calc())
    else:
        backtest()