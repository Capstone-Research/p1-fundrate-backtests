# coding=utf-8
import asyncio
import aiohttp
import json
from datetime import datetime
import gspread
import dateutil.parser
import time
import codecs
import math
# 這個 script 是專門給舊版換成新版用的，平常沒有用途
# gsheet usage
# https://docs.gspread.org/en/latest/user-guide.html#opening-a-spreadsheet

# google gspread
gc = gspread.service_account()

# change this
sheetId = '1DPSTYQ0eGGk0R9oZLt-rvVwH_RccEV9luDbCttEvMB0'
row_start = 2
row_end = 3000

async def recalc_fills():
    
    sh = gc.open_by_key(sheetId)
    sheetID = sh.id
    # 日績效
    worksheet = sh.get_worksheet(1)
    # 初始本金
    initialCapital = float( worksheet.acell('B'+str(row_start)).value )
    # 最後算完的儲存格
    rows = []
    
    prvcap = initialCapital
    hh = -99999
    longestHHTimes = -999
    hhCounter = 0
    prvnet = 0
    for k in range(row_start+1,row_end):
        rownum = 'B'+str(k)
        num = None
        try:num = worksheet.acell('B'+str(k)).value
        except:pass
        if(num == None):break
        curcapital = float(num)
        net = curcapital - initialCapital
        dayProfitPerc = ( curcapital - prvcap ) / prvcap * 100.0
        dayProfitPerc_fix2 = float("{:.2f}".format(dayProfitPerc)) 
        
        
        ret_perc = (net / initialCapital)*100.0
        ret_perc_fix2 = float("{:.2f}".format(ret_perc))
        
        
        hhtext = u'創新高'
        hhrec = ''
        if(net>prvnet):
            if(net > hh):
                hh = net
            hhCounter+=1
            hhrec = ret_perc_fix2
            if(hhCounter > longestHHTimes):
                longestHHTimes = hhCounter
                
        elif(net<prvnet):
            hhCounter = 0
            
        dd = min(0,net-hh)
        dd_perc = (dd / prvcap)*100.0
        dd_perc_fix2 = float("{:.2f}".format(dd_perc))
    
        if(net<prvnet):
            hhtext = dd_perc_fix2
        
        net_fix2 = float("{:.2f}".format(net))
        newrow = [dayProfitPerc_fix2,ret_perc_fix2,dd_perc_fix2,hhtext,hhrec,hhCounter]
        print(newrow)
        rows.append(newrow)
        prvcap = curcapital
        prvnet = net
    
    print('update' + str(row_start+len(rows)))
    worksheet.update('D'+ str(row_start+1)+':I'+str(row_start+len(rows)),rows)
    print('done')

if __name__ == "__main__":
    asyncio.run(recalc_fills())
