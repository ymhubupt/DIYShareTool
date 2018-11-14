import tushare as ts
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=['localhost:9200'])
ts.set_token('****')
# set_token的参数为tushare中个人主要的token
pro = ts.pro_api()

def multiDiffIndex(codes, start_date, end_date):
     data = {}
     for code in codes:
          df1 = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
          df = df1.sort_values(by=['trade_date'])
          end_price = 0
          start_price = 0
          for idx in df.index:
               if df.iloc[idx]['trade_date'] == end_date:
                    end_price = df.iloc[idx]['close']
               if df.iloc[idx]['trade_date'] == start_date:
                    start_price = df.iloc[idx]['close']
          data[code] = 100 * (end_price - start_price) / start_price

     diffData = {}
     for i in range(0, len(codes)):
          #print(codes[i])
          #print(data[codes[i]])
          for j in range(i+1, len(codes)):
               if data[codes[i]] - data[codes[j]] > 0:
                    diffIndex = codes[i] + "_" + codes[j]
                    diffData[diffIndex] = data[codes[i]] - data[codes[j]]
               else:
                    diffIndex = codes[j] + "_" + codes[i]
                    diffData[diffIndex] = data[codes[j]] - data[codes[i]]

     indexDatas = []
     for k, v in diffData.items():
          action = {}
          action['index'] = {}
          action['index']['_index'] = 'diff'
          action['index']['_type'] = 'code'
          action['index']['_id'] = k
          print(k)
          indexDatas.append(action)
          source = {}
          source['diff'] = v
          indexDatas.append(source)

     rs = es.bulk(indexDatas)
     print(rs)

codes = ['002415.SZ', '002594.SZ', '300059.SZ','600309.SH', '600886.SH']
# codes为待分析的股票列表
multiDiffIndex(codes, start_date='20181008', end_date='20181108')
# start_date值为开始日期 end_date为待分析截止日期
