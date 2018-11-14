import tushare as ts
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=['localhost:9200'])
ts.set_token(*****)
#set_token的参数为 tushare中个人主要下的token值，每个账号都不同
pro = ts.pro_api()

def index(ts_code, start_date, end_date):
     df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
     data=[]
     for idx in df.index:
          action = {}
          action['index'] = {}
          action['index']['_index'] = 'single'
          action['index']['_type'] = 'code'
          action['index']['_id'] = df.iloc[idx]['trade_date']

          data.append(action)
          source = {}
          source[ts_code] = df.iloc[idx]['close']
          source['Daily'] = df.iloc[idx]['trade_date']
          data.append(source)
     rs = es.bulk(data)
     print(rs)

index('002415.SZ', start_date='20181008', end_date='20181108')
#这里第一个参数为 股票代码
#第二个参数为 k线开始日期
#第三个参数为 k线截止日期
