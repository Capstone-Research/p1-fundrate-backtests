# coding=utf-8
import asyncio
import datetime
import gspread
import dateutil.parser
import time
import math
import numpy as np

# gsheet usage
# https://docs.gspread.org/en/latest/user-guide.html#opening-a-spreadsheet

# google gspread
gc = gspread.service_account()

# change this
sheetId = '1DPSTYQ0eGGk0R9oZLt-rvVwH_RccEV9luDbCttEvMB0'


# 一日更新
async def upd_one_row(curcapital):
    row_start = 2
    sh = gc.open_by_key(sheetId)
    # 日績效
    worksheet = sh.get_worksheet(1)
    # 初始本金
    initialCapital = float( worksheet.acell('B'+str(row_start)).value )
    
    shrows = worksheet.get_all_values()
    lastrow = shrows[-1]
    lastrowdt = datetime.datetime.strptime(lastrow[0], "%Y-%m-%d")
    
    now = datetime.datetime.now()
    today = datetime.datetime(year=now.year,month=now.month,day=now.day)
    tdiff = today - lastrowdt
    
    one_day_passed = False
    # 更新最後一排總金額
    if(tdiff.days>0):
        one_day_passed = True
        todaystr = u'' + str(now.year)+'-'+str(now.month)+'-'+str(now.day)
        newrow = [todaystr,curcapital]
        shrows.append(newrow)
    
    
    # 更新最後一排
    rows = []
    retdaily = []
    prvcap = initialCapital
    hh = -99999
    mdd = 99999999
    longestHHTimes = -999
    hhCounter = 0
    prvnet = 0
    positivetimes = 0
    positiveTotal = 0
    negativetimes = 0
    negativeTotal = 0
    
    for k in range(1,len(shrows)):
        currow = shrows[k]
        curcapital = float(currow[1])
        net = curcapital - initialCapital
        net_fix2 = float("{:.2f}".format(net))
        
        dayProfitPerc = ( curcapital - prvcap ) / prvcap * 100.0
        dayProfitPerc_fix2 = float("{:.2f}".format(dayProfitPerc)) 
        
        ret_perc = (net / initialCapital)*100.0
        retdaily.append(ret_perc)
        ret_perc_fix2 = float("{:.2f}".format(ret_perc))
        
        hhtext = u'創新高'
        hhrec = ''
        if(net>prvnet):
            if(net > hh):
                hh = net
            hhCounter+=1
            positivetimes+=1
            positiveTotal += abs(net)
            hhrec = ret_perc_fix2
            if(hhCounter > longestHHTimes):
                longestHHTimes = hhCounter
                
        elif(net<prvnet):
            hhCounter = 0
            negativetimes+=1
            negativeTotal += abs(net)
            
        dd = min(0,net-hh)
        dd_perc = (dd / prvcap)*100.0
        dd_perc_fix2 = float("{:.2f}".format(dd_perc))
        if(dd<mdd):
            mdd = dd
            
        
        if(net<prvnet):
            hhtext = dd_perc_fix2
        
        
        newrow = [net_fix2,dayProfitPerc_fix2,ret_perc_fix2,dd_perc_fix2,hhtext,hhrec,hhCounter]
        print(newrow)
        rows.append(newrow)
        prvcap = curcapital
        prvnet = net
    
    
    if(one_day_passed):
        idx = len(shrows)
        # 此處不知道為何使用批次無法更新，必須逐格更新
        worksheet.update('A'+ str(idx),shrows[-1][0])
        worksheet.update('B'+ str(idx),shrows[-1][1])
        worksheet.update('C'+ str(idx),rows[-1][0])
        worksheet.update('D'+ str(idx),rows[-1][1])
        worksheet.update('E'+ str(idx),rows[-1][2])
        worksheet.update('F'+ str(idx),rows[-1][3])
        worksheet.update('G'+ str(idx),rows[-1][4])
        worksheet.update('H'+ str(idx),rows[-1][5])
        worksheet.update('I'+ str(idx),rows[-1][6])
    
    
    totalReturn = float(rows[-1][0])
    _ret = totalReturn / initialCapital
    _mdd = (mdd/initialCapital) * 100.0
    datestart = datetime.datetime.strptime(shrows[1][0], "%Y-%m-%d")
    dateend = datetime.datetime.strptime(shrows[-1][0], "%Y-%m-%d")
    numdays =  (dateend - datestart).days
    onedayreturn = _ret / numdays
    yearreturn = onedayreturn * 365 * 100
    monthreturn = onedayreturn * 30 * 100
    winratio = (positivetimes / (len(rows)-1))*100.0
    prtloseratio = (positiveTotal/positivetimes) / (negativeTotal/negativetimes)
    riskretratio = _ret / abs(mdd)
    std = np.std(retdaily)
    sharpe = np.mean(retdaily) / std 
    
    
    worksheet = sh.get_worksheet(0)
    worksheet.update('B3',_ret)
    worksheet.update('C3',_mdd)
    worksheet.update('D3',riskretratio)
    worksheet.update('E3',riskretratio)
    worksheet.update('F3',winratio)
    worksheet.update('G3',prtloseratio)
    
    worksheet.update('B5',yearreturn)
    worksheet.update('C5',std)
    worksheet.update('D5',sharpe)
    worksheet.update('E5',monthreturn)
    worksheet.update('F5',std)
    worksheet.update('G5',longestHHTimes)    
    
    print('done')

if __name__ == "__main__":
    asyncio.run(upd_one_row(501866.5))
