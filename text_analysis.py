# -*- coding: utf-8 -*-
import pymongo
#import tushare as ts
import datetime
import pandas as pd
import numpy as np
import json
#import pycharts
#import textrank4zh
#import jieba
import pkuseg
#import jiagu
import gensim

db_conn = pymongo.MongoClient('127.0.0.1', port=27017)
news_collection = db_conn.cctv_news.data
sector_rank_collection = db_conn.cctv_news.sector_rank
industry_code_name = db_conn.industry_prices.code_name
#sector_collection = db_conn.a_shares.industry_general
#news_df = pd.DataFrame.from_records(list(news_collection.find()))
#sector_rank_df = pd.DataFrame.from_records(list(sector_rank_collection.find()))

# Model 1
nlp_model = gensim.models.Word2Vec.load(u'../NLP/model_1/corpus.zhwiki.doc.model')
predict_result_collection = db_conn.cctv_news.predict_result_model_1
# Model 2
#nlp_model = gensim.models.KeyedVectors.load_word2vec_format(u'../NLP/model_2/news_12g_baidubaike_20g_novel_90g_embedding_64.bin',binary=True)
#predict_result_collection = db_conn.cctv_news.predict_result_model_2

seg = pkuseg.pkuseg()

industry_codename_df = pd.DataFrame.from_records(list(industry_code_name.find()))
sector_keywords = industry_codename_df.sector_name.tolist()
sector_keywords_seg = []
for ii in range(len(sector_keywords)):
    curr_sector_seg = seg.cut(sector_keywords[ii])
    sector_keywords_seg.append(curr_sector_seg)
    
sector_rank_df = pd.DataFrame.from_records(list(sector_rank_collection.find().sort("date",pymongo.ASCENDING)))
rank_date_list = sector_rank_df.date
seg = pkuseg.pkuseg(postag=True)

for i in range(len(rank_date_list)):
    curr_trade_date = rank_date_list[i]
    print("Processing:", curr_trade_date)
    date_list = np.tile(curr_trade_date,(len(sector_keywords)))
    
    if curr_trade_date == "2007-01-05":
        prev_trade_date = "2006-12-31"
    else:
        tmp_rank_df_2 = pd.DataFrame.from_records(list(sector_rank_collection.find({"date":{"$lt":curr_trade_date}}).sort("date",pymongo.DESCENDING).limit(1)))
        prev_trade_date = tmp_rank_df_2.date[0]
    curr_trade_date = datetime.datetime.strptime(curr_trade_date, "%Y-%m-%d")
    curr_trade_date = curr_trade_date.strftime("%Y%m%d")
    prev_trade_date = datetime.datetime.strptime(prev_trade_date, "%Y-%m-%d")
    prev_trade_date = prev_trade_date.strftime("%Y%m%d")
    
    # Notice: the trade day should be ONE DAY after the news date, and you should also
    #         watch out for the holidays when news is still on but market is not
    
    news_right_bf_curr_trade_date_df = pd.DataFrame.from_records(list(news_collection.find({'date':{'$lt':curr_trade_date}}).sort('date', pymongo.DESCENDING).limit(1)))
    news_right_bf_curr_trade_date = news_right_bf_curr_trade_date_df.date[0]
    news_day_aft_prev_trade_date_df = pd.DataFrame.from_records(list(news_collection.find({'date':{'$gte':prev_trade_date}}).sort('date', pymongo.ASCENDING).limit(1)))
    news_day_aft_prev_trade_date_date = news_day_aft_prev_trade_date_df.date[0]
    
    curr_news_df = pd.DataFrame.from_records(list(news_collection.find({'date':{'$in':[news_day_aft_prev_trade_date_date,news_right_bf_curr_trade_date]}})))
    sector_keywords_score = []
    
    for j in range(len(curr_news_df)):
        single_news_content = curr_news_df.content.iloc[j]
        news_seg_df = pd.DataFrame(seg.cut(single_news_content), columns=['word','tag'])
        news_seg_concise = news_seg_df.loc[(news_seg_df['tag']!="w")&(news_seg_df['tag']!="u")&(news_seg_df['tag']!="t")&(news_seg_df['tag']!="p")&(news_seg_df['tag']!="c")&(news_seg_df['tag']!="r")&(news_seg_df['tag']!="y")&(news_seg_df['tag']!="e")&(news_seg_df['tag']!="o")&(news_seg_df['tag']!="h")&(news_seg_df['tag']!="k")&(news_seg_df['tag']!="nr")]
        news_seg_concise.reset_index(drop=True, inplace=True)
        single_news_score = np.zeros(len(sector_keywords))
        
        for k in range(len(news_seg_concise)):
            for kk in range(len(sector_keywords)):
                this_score_sum = 0
                for kkk in range(len(sector_keywords_seg[kk])):
                    try:
                        this_score = nlp_model.similarity(news_seg_concise.word[k],sector_keywords_seg[kk][kkk])
                    except:
                        this_score = 0
                    this_score_sum += this_score
                single_news_score[kk] += this_score_sum/float(len(sector_keywords_seg[kk]))
            
        sector_keywords_score.append(single_news_score)
    sector_keywords_score = pd.DataFrame(sector_keywords_score,columns=sector_keywords)
    sector_keywords_score_sum = sector_keywords_score.sum(axis=0)
    predict_sector_rank = pd.DataFrame(sector_keywords_score_sum.sort_values(ascending=False),columns=['score'])
    predict_sector_rank.insert(1,'predict_rank',np.argsort(-predict_sector_rank['score']))
    actual_sector_rank = pd.DataFrame([sector_rank_df.close_to_close[i],sector_rank_df.close_to_open[i],sector_rank_df.open_to_open[i]],index=['close_to_close','close_to_open','open_to_open'],columns=sector_rank_df.name_rank[i])
    actual_sector_rank = actual_sector_rank.T
    actual_sector_rank.insert(0,'actual_rank',np.argsort(-actual_sector_rank['close_to_close']))
    curr_result_df = pd.merge(predict_sector_rank,actual_sector_rank,left_index=True,right_index=True,how='outer')
    curr_result_df.insert(0,'sector',curr_result_df.index)
    curr_result_df.insert(0,'date',date_list)
    
    # Write to database
    predict_result_collection.insert_many((json.loads(curr_result_df.to_json(orient='records'))))
    
    
    
# NLP EXAMPLE

#jiagu
# words = jiagu.seg(text)
# words = jiagu.seg(text, model='mmseg') # 使用mmseg算法进行分词
# jiagu.load_model('test/extra_data/model/cnc.model') # 使用国家语委分词标准 
# keywords = jiagu.keywords(text, 5)
# summary =  jiagu.summarize(text, 3)
# knowledge = jiagu.knowledge(text)
# jiagu.findword('input.txt', 'output.txt') # 根据文本，利用信息熵做新词发现。
# see: https://github.com/xylary/Jiagu#%E8%AF%84%E4%BB%B7%E6%A0%87%E5%87%86

#pkuseg
# seg = pkuseg.pkuseg(model_name='news')
# words_1 = seg.cut(text)
# tags: https://github.com/lancopku/pkuseg-python/blob/master/tags.txt
# see: https://github.com/lancopku/pkuseg-python/blob/master/readme/interface.md
