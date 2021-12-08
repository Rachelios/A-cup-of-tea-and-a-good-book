# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 09:08:48 2020

@author: User
"""

# cd .. : back to previous directory

# ls: list all files

# pwd: path working directory

# pip3 freeze > requirements.txt

# pip3 install -r requirements.txt

# pip3 freeze | grep bs4

import pandas as pd
import json
from hashlib import sha256
from datetime import datetime
from python_graphql_client import GraphqlClient


def __authorize(app_id, current_timestamp, payload, app_key):
    signature_format = '{}{}{}{}'.format(app_id, current_timestamp, payload, app_key)
    signature = sha256(signature_format.encode('utf-8')).hexdigest()
    return 'SHA256 Credential={}, Timestamp={}, Signature={}'.format(app_id, current_timestamp, signature)


shopee_ids = {'ph': (13228090000, "2KMCYGVKKS4RGO4QSFUNSHT7IQB2KTQD", "https://open-api.affiliate.shopee.ph/graphql"),
              'id': (
              11277530000, "7P4SH3XVAP6JM2XT6MYAYYKAZYWQBIAX", "https://open-api.affiliate.shopee.co.id/graphql"),
              'sg': (14248150000, "LNFZLJ5SWOLVVUY6NWCSDOG4SNTFXYYI", "https://open-api.affiliate.shopee.sg/graphql"),
              'my': (
              12180330000, "KBCHZ6X5U7VBA2VLNWQNBQTXDZB3ERMY", "https://open-api.affiliate.shopee.com.my/graphql"),
              'th': (
              15231530000, "MEHQJUMJFRCCZ6NBZMIPDGJ33XLQY6HR", "https://open-api.affiliate.shopee.co.th/graphql"),
              'vn': (17202000000, "4XMLHSXYY63QGCY5HFRBJB7JA4WPNK43", "https://open-api.affiliate.shopee.vn/graphql"), }

query = """{
 brandOffer(page:1, limit: 500){
    nodes {
      offerName
      offerLink
      commissionRate
      periodStartTime
      periodEndTime
      shopId
      offerDetails{
        catName
        commissionRate
      }      
    }
  pageInfo{
    page
    limit
    hasNextPage
  }
 }
}"""

payload = json.dumps({"query": "{}".format(query)})  # to convert the dictionary into a string

countries = ['ph', 'id', 'my', 'sg', 'th', 'vn']

dflist = []

for cc in countries:
    app_id, app_key, endpoint = shopee_ids.get(cc)
    timestamp = int(datetime.timestamp(datetime.now()))
    headers = {'Content-Type': 'application/json', 'Authorization': __authorize(app_id, timestamp, payload, app_key)}
    client = GraphqlClient(endpoint=endpoint, headers=headers)
    data = client.execute(query=query, headers=headers)
    df = pd.json_normalize(data=data['data']['brandOffer']['nodes'])
    if df.shape[0] > 0:
        df['periodStartDate'] = pd.to_datetime(df['periodStartTime'], unit='s', errors='coerce').dt.tz_localize(
            tz='utc').dt.tz_convert('Asia/Singapore')
        df['periodEndDate'] = pd.to_datetime(df['periodEndTime'], unit='s', errors='coerce').dt.tz_localize(
            tz='utc').dt.tz_convert('Asia/Singapore')
        df['cc'] = cc
        dflist.append(df)
    else:
        print("no data for :", cc)

offers = pd.concat(dflist)

today = pd.to_datetime(timestamp, unit='s').tz_localize(tz='utc').tz_convert('Asia/Singapore').strftime('%Y-%m-%d')

offers['query_date'] = today

import requests
from bs4 import BeautifulSoup

session_requests = requests.session()

headers = {"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}


def get_seller_name(session_requests, headers, offerLink):
    get_url = session_requests.get(offerLink, headers=headers)
    soup = BeautifulSoup(get_url.text, "html.parser")
    if soup.find('h1', class_='b2c-shop-name-action__name'):
        seller_name = str(soup.find('h1', class_='b2c-shop-name-action__name').contents[0])
    elif soup.find('h1', class_='section-seller-overview-horizontal__portrait-name'):
        seller_name = str(soup.find('h1', class_='section-seller-overview-horizontal__portrait-name').contents[0])
    else:
        seller_name = ''

    print(offerLink, seller_name.strip())
    return seller_name.strip()


offers['seller_name'] = offers.offerLink.apply(lambda x: get_seller_name(session_requests, headers, x))

## offers.to_csv('shopee_ams_{}.csv'.format(today),encoding='utf-8',index=False)
