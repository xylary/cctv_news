# -*- coding: utf-8 -*-
import pymongo
import json
import tushare as ts
import datetime
import time
import pandas as pd

ts.set_token('YOUR TOKEN HERE')
ts_pro = ts.pro_api()
db_conn = pymongo.MongoClient('127.0.0.1', port=27017)
news_collection = db_conn.cctv_news.data

try:
    tmp_df = pd.DataFrame.from_records(list(news_collection.find().sort('date', pymongo.DESCENDING).limit(1)))
    begin_date = datetime.datetime.strptime(tmp_df.date[0], "%Y%m%d") + datetime.timedelta(days=1)
except:    
    begin_date = datetime.datetime(2007,1,1)
    
curr_time = datetime.datetime.now()
if curr_time.hour < 22:
    end_date = curr_time - datetime.timedelta(days=1)
else:
    end_date = curr_time
    
for i in range((end_date - begin_date).days+1):
    this_date = begin_date + datetime.timedelta(days=i)
    this_date = this_date.strftime('%Y%m%d')
    this_df = ts_pro.cctv_news(date=this_date)
    try:
        news_collection.insert_many((json.loads(this_df.to_json(orient='records'))))
        print("Successfully retrieved and saved:", this_date)
    except:
        print("Failed to retrieve:", this_date)
    time.sleep(1)
    
    
# MONGODB EXAMPLE
    
#df = ts_pro.cctv_news(date='20181211')
#news_collection.insert_many((json.loads(df.to_json(orient='records'))))
#db = pd.DataFrame.from_records(list(news_collection.find()))
