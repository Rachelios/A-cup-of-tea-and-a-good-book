# -*- coding: utf-8 -*-
"""
Created on Wed Jul 22 21:51:37 2020

@author: User
"""

#import modin.pandas as pd
import pandas as pd
import numpy as np
#dalake/athena for ga data
from pyathena import connect
from pyathena.util import as_pandas
from pyathena.pandas_cursor import PandasCursor
from multiprocessing.pool import ThreadPool
import io
import os
import time

def get_discovery_sessions(date, country, pyathena_connection, metrics_lookback):
    query = io.open("../queries v3/shop last n days sessions by country url split.sql", 
                    mode="r", 
                    encoding="utf-8").read()
    
    min_date = (pd.to_datetime(date) -  pd.to_timedelta(metrics_lookback-1, unit='d')).strftime('%Y-%m-%d')
    month_list = ', '.join(set([date.split('-')[1],min_date.split('-')[1]]))
    year_list = ', '.join(set([date.split('-')[0],min_date.split('-')[0]]))
    
    temp = query.format(cc=country, 
                        year_list = year_list, 
                        min_date=min_date, 
                        max_date=date, 
                        month_list=month_list)
    temp_tbl = pyathena_connection.execute(temp).as_pandas()
    
    temp_tbl.rename(columns = {'pageviews':'pageviews_{}'.format('l'+str(metrics_lookback)+'d'),
                               'sessions':'sessions_{}'.format('l'+str(metrics_lookback)+'d')}, inplace = True)
    
    return(temp_tbl)

def get_discovery_clicks(date, country, pyathena_connection, metrics_lookback):
    query = io.open("../queries v3/shop last n days clicks by country url split.sql", 
                    mode="r", 
                    encoding="utf-8").read()
    
    min_date = (pd.to_datetime(date) -  pd.to_timedelta(metrics_lookback-1, unit='d')).strftime('%Y-%m-%d')
    month_list = ', '.join(set([date.split('-')[1],min_date.split('-')[1]]))
    year_list = ', '.join(set([date.split('-')[0],min_date.split('-')[0]]))
    
    temp = query.format(cc=country, 
                        year_list = year_list, 
                        min_date=min_date, 
                        max_date=date, 
                        month_list=month_list)
    
    temp_tbl = pyathena_connection.execute(temp).as_pandas()
    
    temp_tbl.rename(columns = {'clicks':'clicks_{}'.format('l'+str(metrics_lookback)+'d'),
                               'bidding_clicks':'bidding_clicks_{}'.format('l'+str(metrics_lookback)+'d')}, inplace = True)
    
    return(temp_tbl)
    
def get_offer_data(date, country, pyathena_connection, subproduct, ams_sellers, shopee_id, lazada_id, lazmall_id, taobao_id):
    query_text = io.open("../queries v3/"+ subproduct + ".sql", 
                    mode="r", 
                    encoding="utf-8").read()
    
    query_text = query_text.format(cc=country, 
                                   catalog_date=date.replace('-',''), 
                                   shopee_id=shopee_id, 
                                   ams_sellers=str(ams_sellers)[1:-1].replace('"',"'"),
                                   lazada_id=lazada_id,
                                   lazmall_id=lazmall_id,
                                   taobao_id=taobao_id)
    #print(query_text)
    
    t0 = time.time()
    temp = pyathena_connection.execute(query_text).as_pandas()
    t1 = time.time()

    total = round(t1-t0,1)
    print(country,
          subproduct, 
          "data scanned:", round(pyathena_connection.data_scanned_in_bytes * 1e-6,1), "(mb) ", 
          "time: ", round(pyathena_connection.execution_time_in_millis * 1e-3,1), "(s) ", 
          "total time:", total, "(s)")
    
    temp['cc'] = country
    temp['catalog_date'] = date
    temp['ipg_subproduct'] = subproduct
    
    return(temp)
    
def get_metrics_csv(file_name, types_dict):
    col_names = pd.read_csv(file_name, nrows=0, compression='gzip').columns
    types_dict.update({col: str for col in col_names if col not in types_dict})
    return(pd.read_csv(file_name, dtype=types_dict, compression='gzip'))    
    
def get_offers_csv(subproduct):
    temp = pd.read_csv(subproduct+'_{}_{}.csv.gz'.format(country,date, compression='gzip'))
    temp['ipg_subproduct'] = subproduct
    return(temp)     

#parallel
def get_subproduct_data(args):
    #print(subproduct)
    date, country, subproduct, ams_sellers, shopee_id, lazada_id, lazmall_id, taobao_id = args
    
    if not ams_sellers:
        ams_sellers.append('none')
        
    conn = connect(aws_access_key_id='AKIAJRQRSAL7JEBFJUFQ',
               aws_secret_access_key='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',
               s3_staging_dir='s3://athena-user-queries/test-folder',
               region_name='ap-southeast-1',
               cursor_class=PandasCursor).cursor()
    #print('connected')
    
    get_offer_data(date, country, conn, subproduct, ams_sellers, shopee_id, lazada_id, lazmall_id, taobao_id).to_csv(subproduct+'_{}_{}.csv.gz'.format(country,date),
                                                           index=False, encoding='utf-8', compression='gzip')
    temp = get_offers_csv(subproduct)
    temp = temp.drop(['rn'], axis=1, errors='ignore')
    temp.fillna('', inplace=True)   
    
    temp['bidding_merchant'] = np.where(temp['bidding_offers']>0, temp['store_merchant_id'], np.nan)
    
    col_names = temp.columns

    non_grouping_columns = ['offers','copied_offers','comparable_offers','bidding_offers',
                            'asa_offers','ams_offers','lazada_offers','shopee_offers',
                            'store_merchant_id','bidding_merchant']

    groupby_cols = list(set(col_names) - set(non_grouping_columns))
    
    temp_agg = temp.groupby(groupby_cols).agg({'store_merchant_id':['count', ', '.join],
                                                'offers':['sum'],
                                                'copied_offers':['sum'],
                                                'comparable_offers':['sum'],
                                                'bidding_offers':['sum'],
                                                'bidding_merchant':[lambda x: ', '.join(x.dropna())],
                                                'asa_offers':['sum'],
                                                'ams_offers':['sum'],
                                                'lazada_offers':['sum'],
                                                'shopee_offers':['sum']}).reset_index()

    temp_agg.columns = groupby_cols + ['merchants','merchants_list','offers','copied_offers',
                                       'comparable_offers','bidding_offers','bidding_merchants_list',
                                       'asa_offers','ams_offers','lazada_offers','shopee_offers']
    
    groupby_cols.remove('catalog_date')
    
    temp_join = pd.merge(combined_ga.loc[combined_ga.ipg_subproduct == subproduct],
                         temp_agg,
                         how = 'left',
                         on = groupby_cols)

    temp_join.fillna({'catalog_date':date, 'merchants':0, 'offers':0,	'feeds':0,
                      'copied_offers':0, 'comparable_offers':0, 'bidding_offers':0,
                      'asa_offers':0,'ams_offers':0,'lazada_offers':0,'shopee_offers':0}, inplace=True)

    conn.close()

    return(temp_join)


dates = ['2021-01-10']
countries = ['my','id']
metrics_lookback = 30 #days

ams_data = pd.read_csv(r"C:\Users\User\Desktop\2020-11\Shopee AMS\shopee_ams_2021-01-11.csv")

ams_sellers = {cc : [s.replace("\'","\''") for s in ams_data.loc[ams_data.cc == cc].seller_name.dropna().unique()] for cc in countries}

#example
"""
ams_sellers = {'ph':['PEDIGREE® Official Store','Unilever Foods Official Store','Unilever Home Care',
                     'Unilever Beauty Official Store','Colgate-Palmolive Philippines','Mink PH Official',
                     'Lana Official Store','sustagenofficialstoreph','Lactum Official Store','Enfagrow Official Store']}
"""
shopee_ids = {'ph':'\'shopee.ph\'',
              'sg':'\'shopee.sg\'',
              'id':'\'shopee.co.id\'',
              'my':'\'shopee.com.my\'',
              'th':'\'shopee.co.th\'',
              'vn':'\'shopee.vn\''}

lazada_ids = {'ph':'\'lazada.com.ph\'',
              'sg':'\'lazada.sg\'',
              'id':'\'lazada.co.id\'',
              'my':'\'lazada.com.my\'',
              'th':'\'lazada.co.th\'',
              'vn':'\'lazada.vn\''}

lazmall_ids = {'ph':'\'lazmall.lazada.com.ph\'',
               'sg':'\'lazmall.lazada.sg\'',
               'id':'\'lazmall.lazada.co.id\'',
               'my':'\'lazmall.lazada.com.my\'',
               'th':'\'lazmall.lazada.co.th\'',
               'vn':'\'lazmall.lazada.vn\''}

taobao_ids = {'ph':'\'taobao.lazada.com.ph\'',
               'sg':'\'taobao.lazada.sg\'',
               'id':'\'taobao.lazada.co.id\'',
               'my':'\'taobao.lazada.com.my\'',
               'th':'\'taobao.lazada.co.th\'',
               'vn':'\'taobao.lazada.vn\''}                                        

query_list = ['brand',
              'category',
              'brand-category',
              'category-gender',
              'brand-category-gender',
              'brand-gender',
              'brand-series',
              'brand-series-gender',
              'brand-series-model',
              'brand-series-model-gender',
              'brand-series-category',
              'brand-series-category-gender',
              'brand-series-model-category',
              'brand-series-model-category-gender']


conn1 = connect(aws_access_key_id='AKIAJRQRSAL7JEBFJUFQ',
               aws_secret_access_key='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',
               s3_staging_dir='s3://athena-user-queries/test-folder',
               region_name='ap-southeast-1',
               cursor_class=PandasCursor).cursor()

#date = dates[0]
#country = countries[0]
for date in dates:
    os.chdir('C:\\Users\\User\\Desktop\\2020-07\\Athena Catalog')
    if not os.path.exists(date):
        os.makedirs(date)
    #os.makedirs(date)
    os.chdir(date)
    for country in countries:
        get_discovery_sessions(date,country,conn1,metrics_lookback).to_csv('shop last {} days sessions by country url split_{}_{}.csv.gz'.format(metrics_lookback,country,date),
                                                         index=False,encoding='utf-8', compression='gzip')
        
        get_discovery_clicks(date,country,conn1,metrics_lookback).to_csv('shop last {} days clicks by country url split_{}_{}.csv.gz'.format(metrics_lookback,country,date),
                                                         index=False,encoding='utf-8', compression='gzip')
        
        sessions = get_metrics_csv('shop last {} days sessions by country url split_{}_{}.csv.gz'.format(metrics_lookback,country,date),
                                   {'gender': float, 'sessions': int})
        
        clicks = get_metrics_csv('shop last {} days clicks by country url split_{}_{}.csv.gz'.format(metrics_lookback,country,date),
                                 {'gender': float, 'clicks': int})
        
        combined_ga = pd.merge(sessions,
                               clicks,
                               how = 'outer', 
                               on = ['cc', 'landingpagepath', 'ipg_product', 
                                     'ipg_subproduct', 'brand_url', 'series_url', 
                                     'model_url', 'gender', 'cat_l1', 'cat_l2', 'cat_l3','cat_l4'])
        
        combined_ga.fillna({'brand_url':'', 'cat_l1':'', 'cat_l2':'', 'cat_l3':'', 'cat_l4':'',
                            'gender':0, 'model_url':'', 'series_url':''}, inplace=True)

        #to aggregate offers by url
        #parallel - but doesn't handle errors so well
        #errors: socket.timeout: The read operation timed out
        
        pool = ThreadPool(processes=2)
        temp_list = [(date, country, subproduct, 
                      ams_sellers.get(country,['none']), shopee_ids.get(country,'\'none\''),
                      lazada_ids.get(country,'\'none\''),lazmall_ids.get(country,'\'none\''),taobao_ids.get(country,'\'none\'')) 
                    for subproduct in set(combined_ga.ipg_subproduct.unique()).intersection(query_list)]
        results = pool.map(get_subproduct_data, temp_list)
        pool.close()
        pool.join()
        
        """
        #not using multiprocessing: also slow...
        results = []
        temp_list = [(date, country, subproduct, ams_sellers.get(country,['none']), shopee_ids.get(country,'\'none\'')) for subproduct in set(combined_ga.ipg_subproduct.unique()).intersection(query_list)]
        for item in temp_list:
            results.append(get_subproduct_data(item))            
        """
        #sort by sessions
        out = pd.concat(results)
        
        #move date to the first colum
        cols = list(out)
        cols.insert(0,cols.pop(cols.index('catalog_date')))
        out = out.loc[:, cols]
        
        out.to_csv('shop post go-live report_{}_{}.csv'.format(country,date), index=False, encoding='utf-8')

conn1.close()
print('done, check {} for data'.format(os.getcwd()))