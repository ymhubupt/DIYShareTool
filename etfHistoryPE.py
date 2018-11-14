import tushare as ts
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=['localhost:9200'])
ts.set_token('****')
# set_token为tushare中个人主页的token值
pro = ts.pro_api()

def etfHistoryPE(codes, start_date, end_date):
     data = {}
     for code in codes:
          df1 = pro.index_dailybasic(ts_code=code, start_date=start_date, end_date=end_date, fields='ts_code,trade_date,pe')
          df = df1.sort_values(by=['trade_date'])
          for idx in df.index:
               if df.iloc[idx]['trade_date'] not in data:
                    source = {}
                    source[code] = df.iloc[idx]['pe']
                    source['date'] = df.iloc[idx]['trade_date']
                    data[df.iloc[idx]['trade_date']] = source
               else:
                    data[df.iloc[idx]['trade_date']][code] = df.iloc[idx]['pe']

     indexDatas = []
     for k, v in data.items():
          action = {}
          action['index'] = {}
          action['index']['_index'] = 'etf'
          action['index']['_type'] = 'code'
          action['index']['_id'] = k
          indexDatas.append(action)
          indexDatas.append(v)

     rs = es.bulk(indexDatas)
     print(rs)
     
codes = ['000016.SH', '000905.SH', '000300.SH']
#codes 为指数代码列表， 需在tushare能够支持的股票列表范围内
etfHistoryPE(codes, start_date='20141008', end_date='20181024')
#start_date为开始日期 end_date为截止日期
