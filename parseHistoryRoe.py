import tushare as ts
import time
import json
ts.set_token('1b07e6a73ce5e67882078978389be86b55176383362a83d1458cc29e')
# set_token的参数为tushare中个人主要的token
pro = ts.pro_api()

def getAllShare():
     print("start:")
     print(time.localtime(time.time()))
     df1 = pro.stock_basic( list_status='L')
     sharelist = {}
     names = {}
     for idx in df1.index:
          parseSingleHistoryROE(df1.iloc[idx]['ts_code'], df1.iloc[idx]['name'])

     print("end:")
     print(time.localtime(time.time()))
     

def parseSingleHistoryROE(code, name):
    roelist = {}
    total_roe =0
    total_count = 0
        
    for year in [2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010]:
         month = "120"
         day = 1
         date = str(year) + month + str(day)
         roe = singleShareROE(code, date)
         if roe:
              if roe > 15:
                  roelist[date] = roe
              else:
                  return
         else:
              for d in range(2, 8):
                   date = str(year) + month + str(d)
                   roe = singleShareROE(code, date)
                   if roe:
                        if roe > 10:
                             roelist[date] = roe
                             break
                        else:
                             return
    for d in roelist:
        print(str(code)+'_'+ str(name) + '_' + str(d) + "==>" +str(roelist[d]))
        total_roe += roelist[d]
        total_count +=1
    if total_count > 0 and total_roe > 0:
        average_roe = total_roe / total_count
        print(str(code)+'_'+ str(name) + '_平均' + "==>" +str(average_roe))

def singleShareROE(code, date):
    roe = None
    time.sleep(0.3)
    df2 = pro.daily_basic(ts_code=code, start_date=date, end_date=date)
    for idx2 in df2.index:
        if df2.iloc[idx2]['pb'] and df2.iloc[idx2]['pe']:
              roe = 100* df2.iloc[idx2]['pb']/df2.iloc[idx2]['pe']
    return roe                    

getAllShare()

