# -*- coding: utf-8 -*-
import pymongo
import datetime
import pandas as pd

db_conn = pymongo.MongoClient('127.0.0.1', port=27017)
news_collection = db_conn.cctv_news.data
sector_rank_collection = db_conn.cctv_news.sector_rank
industry_prices = db_conn.industry_prices
sector_collection = db_conn.a_shares.industry_general
industry_code_name = industry_prices.code_name

news_df = pd.DataFrame.from_records(list(news_collection.find()))
industry_codename_df = pd.DataFrame.from_records(list(industry_code_name.find()))
industry_codename_df.set_index("sector_code",inplace=True)
industry_codename_df = industry_codename_df.T
sector_list = list(industry_prices.list_collection_names())
sector_list.remove("code_name")
sector_list.sort()

# Iterate the sector_rank to fill up the yet filled rank sequence
try:
    tmp_rank_df = pd.DataFrame.from_records(list(sector_rank_collection.find().sort('date', pymongo.DESCENDING).limit(1)))
    begin_date = datetime.datetime.strptime(tmp_rank_df.date[0], "%Y-%m-%d") + datetime.timedelta(days=1)
    begin_date = begin_date.strftime('%Y-%m-%d')
except:
    begin_date = "2007-01-04"       # The first date in the price DB is 2007-01-04
tmp_price_df = pd.DataFrame.from_records(list(industry_prices[sector_list[-1]].find({'date':{'$gt':begin_date}}).sort('date', pymongo.ASCENDING)))
try:
    begin_date = tmp_price_df.date.iloc[0]
    end_date = tmp_price_df.date.iloc[-2]
    for i in range(len(tmp_price_df.date)-1):
        curr_date = tmp_price_df.date[i]
        curr_df = pd.DataFrame([])
        for j in range(len(sector_list)):
            curr_sector_code = sector_list[j]
            curr_sector_prev_df = pd.DataFrame.from_records(list(industry_prices[sector_list[j]].find({'date':{'$lt':curr_date}}).sort('date', pymongo.DESCENDING).limit(1)))
            curr_sector_today_df = pd.DataFrame.from_records(list(industry_prices[sector_list[j]].find({'date':curr_date})))
            curr_sector_next_df = pd.DataFrame.from_records(list(industry_prices[sector_list[j]].find({'date':{'$gt':curr_date}}).sort('date', pymongo.ASCENDING).limit(1)))
            close2close = curr_sector_today_df.close[0]/curr_sector_prev_df.close[0]-1.
            close2open = curr_sector_today_df.close[0]/curr_sector_today_df.open[0]-1.
            open2open = curr_sector_next_df.open[0]/curr_sector_today_df.open[0]-1.
            if ~(close2close==0 and close2open==0 and open2open==0):
                single_df = pd.DataFrame([close2close,close2open,open2open],index=["close_to_close","close_to_open","open_to_open"],columns=[curr_sector_code])
                single_df = single_df.T
                curr_df = pd.concat([curr_df,single_df],axis=0)
        curr_df.sort_values("close_to_close",ascending=False,inplace=True)
        code_rank = curr_df.index.tolist()
        name_rank = industry_codename_df[code_rank]
        name_rank = name_rank.T
        name_rank = name_rank["sector_name"].tolist()
        this_document = {"date":curr_date, "code_rank":code_rank, "name_rank":name_rank, "close_to_close":curr_df["close_to_close"].tolist(),"close_to_open":curr_df["close_to_open"].tolist(),"open_to_open":curr_df["open_to_open"].tolist()}
        this_key = {"date":curr_date}
        if sector_rank_collection.count_documents(this_key) == 0:
            sector_rank_collection.insert_one(this_document)
            print("Inserted rank data to DB: "+ curr_date)
        else:
            print("Already found rank data in DB: "+ curr_date)
except:
    print("No new rank data needed to update")
    
    
    
# Run only once
'''
industry_code_name = industry_prices.code_name
for i in range(len(sector_list)):
    this_key = {"sector_code":sector_list[i]}
    if industry_code_name.count_documents(this_key) == 0:
        sector_single_df = pd.DataFrame.from_records(list(sector_collection.find({"sector_code":sector_list[i]}).limit(1)))
        this_document = {"sector_code":sector_list[i], "sector_name":sector_single_df.sector_name[0][:-2]}
        industry_code_name.insert_one(this_document)
'''  
