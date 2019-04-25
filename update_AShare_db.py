# -*- coding: utf-8 -*-
# Ideally running this script once a year should be enough
from WindPy import w
import pandas as pd
import pymongo
import datetime
import json

db_conn = pymongo.MongoClient('127.0.0.1', port=27017)
industry_collection = db_conn.a_shares.industry_general
w.start()

# Update sector constituents by year
current_date = datetime.datetime.today()
current_year = current_date.year
wind_sector_cons = w.wset("sectorconstituent","date="+str(current_year)+"-01-01;sectorid=a39901012d000000")
if wind_sector_cons.ErrorCode >= 0:
    sector_cons = wind_sector_cons.Data
    for i in range(len(sector_cons[1])):         ###
        this_sector_code = sector_cons[1][i]
        this_sector_name = sector_cons[2][i]
        try:
            sector_key = {"sector_code":this_sector_code}
            tmp_df = pd.DataFrame.from_records(list(industry_collection.find(sector_key).sort('date', pymongo.DESCENDING).limit(1)))
            init_year = datetime.datetime.strptime(tmp_df.date[0], "%Y-%m-%d").year+1
        except:    
            init_year = 2007
        for j in range(current_year-init_year+1):       ###
            this_date = str(j+init_year)+"-01-01"
            wind_sector_single = w.wset("sectorconstituent","date="+this_date+";windcode="+this_sector_code+";field=wind_code,sec_name")
            if wind_sector_single.ErrorCode >= 0:
                if len(wind_sector_single.Data) == 0:
                    this_document = {"date":this_date,"sector_code":this_sector_code,"sector_name":this_sector_name,"constituent_code":[],"constituent_name":[]}
                else:
                    this_document = {"date":this_date,"sector_code":this_sector_code,"sector_name":this_sector_name,"constituent_code":wind_sector_single.Data[0],"constituent_name":wind_sector_single.Data[1]}
                this_key = {"date":this_date,"sector_code":this_sector_code,"sector_name":this_sector_name}
                if industry_collection.count_documents(this_key) == 0:
                    industry_collection.insert_one(this_document)
                    print("Inserted to database: " + str(this_key))
else:
    print(wind_sector_cons.ErrorCode)

# Update stock general information (ONCE A YEAR SHOULD BE ENOUGH)
'''
stock_collection = db_conn.a_shares.stock_general
industry_document_all = list(db_conn.a_shares.industry_general.find())
for k in range(len(industry_document_all)):
    this_industry_document = industry_document_all[k]
    this_stock_list = this_industry_document["constituent_code"]
    for kk in range(len(this_stock_list)):
        this_stock = this_stock_list[kk]
        this_key = {"windcode":this_stock}
        if stock_collection.count_documents(this_key) > 0:
            print("Already in database; pass... " + str(this_key))
            continue
        wind_stock_info = w.wss(this_stock, "business,briefing,majorproducttype,majorproductname")
        if wind_stock_info.ErrorCode >= 0:
            this_document = {"windcode":this_stock,"business":wind_stock_info.Data[0],"briefing":wind_stock_info.Data[1],"product_type":wind_stock_info.Data[2],"product_name":wind_stock_info.Data[3]}
            stock_collection.insert_one(this_document)
            print("Inserted to database: " + str(this_key))
        else:
            print(wind_stock_info.ErrorCode)        
'''

# Update industry indice prices
industry_prices = db_conn.industry_prices
curr_time = datetime.datetime.now()
if curr_time.hour < 18:
    end_date = curr_time - datetime.timedelta(days=1)
else:
    end_date = curr_time
end_date = end_date.strftime('%Y-%m-%d')
if wind_sector_cons.ErrorCode >= 0:
    sector_cons = wind_sector_cons.Data
    for i in range(len(sector_cons[1])):         ###
        this_sector_code = sector_cons[1][i]
        this_sector_name = sector_cons[2][i]
        this_collection = industry_prices[this_sector_code]
        try:
            tmp_df = pd.DataFrame.from_records(list(this_collection.find().sort('date', pymongo.DESCENDING).limit(1)))
            begin_date = datetime.datetime.strptime(tmp_df.date[0], "%Y-%m-%d") + datetime.timedelta(days=1)
            begin_date = begin_date.strftime('%Y-%m-%d')
        except:    
            begin_date = '2007-01-01'
        if begin_date > end_date:
            continue
        
        wind_sector_prices = w.wsd(this_sector_code, "open,high,low,close", begin_date, end_date, "Fill=Previous")
        if wind_sector_prices.ErrorCode >= 0:
            this_df = pd.DataFrame(wind_sector_prices.Data,index=["open","high","low","close"],columns=wind_sector_prices.Times)
            this_df = this_df.T
            this_df['date'] = [x.strftime("%Y-%m-%d") for x in this_df.index]
            this_df = this_df[["date","open","high","low","close"]]
            this_collection.insert_many((json.loads(this_df.to_json(orient='records'))))
            print("Inserted price data to database: " + this_sector_code)
        else:
            print(wind_sector_prices.ErrorCode)   
else:
    print(wind_sector_cons.ErrorCode)




# WIND EXAMPLE
#w.wset("sectorconstituent","date=2019-04-01;sectorid=a39901012d000000")
#w.wset("sectorconstituent","date=2019-04-01;windcode=886001.WI;field=wind_code,sec_name")
#w.wss("002223.SZ", "majorproducttype,majorproductname,briefing")
