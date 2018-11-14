import tushare as ts
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=['localhost:9200'])
ts.set_token('****')
# set_token的参数为tushare个人主页中的token
pro = ts.pro_api()

def multiDailyIndex(codes, start_date, end_date):
     data = {}
     for code in codes:
          df1 = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
          df = df1.sort_values(by=['trade_date'])
          for idx in df.index:
               pct = code + "_" + "change"
               if df.iloc[idx]['trade_date'] not in data:
                    source = {}
                    source[code] = df.iloc[idx]['close']
                    source[pct] = df.iloc[idx]['pct_change']
                    source['Daily'] = df.iloc[idx]['trade_date']
                    data[df.iloc[idx]['trade_date']] = source
               else:
                    data[df.iloc[idx]['trade_date']][code] = df.iloc[idx]['close']
                    data[df.iloc[idx]['trade_date']][pct] = df.iloc[idx]['pct_change']

     indexDatas = []
     for k, v in data.items():
          action = {}
          action['index'] = {}
          action['index']['_index'] = 'daily'
          action['index']['_type'] = 'code'
          action['index']['_id'] = k
          indexDatas.append(action)
          indexDatas.append(v)

     rs = es.bulk(indexDatas)
     print(rs)

codes = ['002415.SZ', '002594.SZ', '300059.SZ','600309.SH', '600886.SH']
# codes 填待分析的股票列表
multiDailyIndex(codes, start_date='20171008', end_date='20181024')
# 第二个参数为开始日期
# 第三个参数为结束日期
