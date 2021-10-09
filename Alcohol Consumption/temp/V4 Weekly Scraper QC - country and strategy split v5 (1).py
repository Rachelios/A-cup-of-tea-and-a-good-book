# -*- coding: utf-8 -*-
""" 
Created on Dec 2018


This script doesn't seperate by each feed of merchant like Lazada and Lazada asa due to current bug of the scraper log.

"""

import pandas as pd
from pandas import json_normalize
from elasticsearch import Elasticsearch

es = Elasticsearch('https://iprice:Iprice_21@search-iprice-production-elk-kzrsnpvgbuatlkgeitidaiqgti.ap-southeast-1.es.amazonaws.com:443')

date_query = {
  "query": {
    "bool": {
      "must": [
        {
          "match_all": {}
        },
        {
          "range": {
            "@timestamp": {
              "gte": "now-7d/d",
              "lte": "now/d"
            }
          }
        }
      ],
      "must_not": []
    }
  },
  "size": 0,
  "_source": {
    "excludes": []
  },
  "aggs": {
    "20": {
      "date_range": {
        "field": "@timestamp",
        "ranges": [
          {
            "from": "now-7d/d",
            "to": "now/d"
          }
        ]
      }
    }
  }
}

query = {
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "@timestamp": {
              "gte": "now-7d/d",
              "lte": "now/d"
            }
          }
        }
      ]
    }
  },
  "size": 0,
  "aggs": {
    "merchant_id": {
      "terms": {
        "field": "data.item.store.merchant_id.keyword",
        "size": 10000,
        "order": {
          "_count": "desc"
        }
      },
      "aggs": {
        "feed": {
          "terms": {
            "field": "data.item.store.feed.keyword",
            "size": 10,
          },
          "aggs": {
            "country": {
              "terms": {
                "field": "country.keyword",
                "size": 10,
                "order": {
                  "_count": "desc"
                }
              },
              "aggs": {
                "scraper_strategy": {
                    "terms": {
                      "field": "data.item.strategy.keyword",
                      "size": 5,
                      "order": {
                        "_count": "desc"
                      }
                    },
                    "aggs": {
                    "ScrapeResult": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "scrape result" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "404": {
                      "sum": {
                        "script": {
                          "inline": "!doc['data.item.is404'].empty && doc['data.item.is404'].value ? 1 : 0",
                          "lang": "painless"
                        }
                      }
                    },
                    "OutOfStock": {
                      "sum": {
                        "script": {
                          "inline": "!doc['data.item.outOfStock'].empty && doc['data.item.outOfStock'].value ? 1 : 0",
                          "lang": "painless"
                        }
                      }
                    },
                    "PriceChanged": {
                      "sum": {
                        "script": {
                          "inline": "!doc['data.item.updatedPrice'].empty && doc['data.item.updatedPrice'].value ? 1 : 0",
                          "lang": "painless"
                        }
                      }
                    },
                    "TotalMissingData": {
                      "sum": {
                        "script": {
                          "inline": """(doc['message.keyword'].value == "cannot detect availability" || doc['message.keyword'].value == "cannot detect price" || doc['message.keyword'].value == "cannot detect currency" || doc['message.keyword'].value == "no structured data found" || doc['message.keyword'].value == "cannot parse price" || doc['message.keyword'].value == "cannot extract structured data") ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "TotalScrapingError": {
                      "sum": {
                        "script": {
                          "inline": """(doc['message.keyword'].value == "spider parse error" || doc['message.keyword'].value == "downloader error" || doc['message.keyword'].value == "product url missing" || doc['message.keyword'].value == "requests exception" || doc['message.keyword'].value == "enrich item from json exception" || doc['message.keyword'].value == "enrich item from micro exception") ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "MissingAvailability": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "cannot detect availability" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "MissingPrice": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "cannot detect price" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "MissingCurrency": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "cannot detect currency" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "NoStructuredDataFound": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "no structured data found" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "ParseError": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "spider parse error" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "DownloaderError": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "downloader error" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "ProductURLMissing": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "product url missing" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "RequestsException": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "requests exception" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "CannotParsePrice": {
                      "sum": {
                        "script": {
                          "inline": """doc['message.keyword'].value == "cannot parse price" ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    },
                    "CannotExtractStructuredData": {
                      "sum": {
                        "script": {
                          "inline": """(doc['message.keyword'].value == "cannot extract structured data" || doc['message.keyword'].value == "enrich item from json exception" || doc['message.keyword'].value == "enrich item from micro exception") ? 1 : 0""",
                          "lang": "painless"
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

#query es for the scraper data
data = es.search(body=query, index='scraper_log_*', request_timeout=300)

#query es for the dates w/ proper formatting
dates = es.search(body=date_query, index='scraper_log_*', request_timeout=300)
start_date = json_normalize(dates['aggregations']['20']['buckets'])['from_as_string'][0][:10]
end_date = json_normalize(dates['aggregations']['20']['buckets'])['to_as_string'][0][:10]

#put all the scraper data in one df
listOfDf = []
for c1, merchant_id_bucket in enumerate(data['aggregations']['merchant_id']['buckets']):
	for c2, feed_bucket in enumerate(merchant_id_bucket['feed']['buckets']):
	    for c3, country_bucket in enumerate(feed_bucket['country']['buckets']):
	        temp = json_normalize(country_bucket['scraper_strategy']['buckets'])
	        temp['Country'] = country_bucket['key'] 
	        temp['Feed'] = feed_bucket['key']
	        temp['Merchant'] = merchant_id_bucket['key']
	        listOfDf.append(temp)
            
df = pd.concat(listOfDf)

#rename some columns and reset index
df.rename(columns=lambda x: x.replace('.value', ''), inplace=True)
df.rename(columns={'key':'ScraperStrategy','doc_count':'DocCount'}, inplace=True)
df.reset_index(inplace=True, drop=True)

#adding a row for totals
df.loc['Total'] = df.sum()
df['Country'].loc['Total'] = 'All'
df['Merchant'].loc['Total'] = 'All'
df['Feed'].loc['Total'] = 'All'
df['ScraperStrategy'].loc['Total'] = 'All'

#calculate some ratios
df['404%'] = df['404']/df['ScrapeResult']
df['Missing OOS Data%'] = df['MissingAvailability']/(df['ScrapeResult'] - df['404'])
df['OOS%'] = df['OutOfStock']/(df['ScrapeResult'] - df['404'] - df['MissingAvailability'])
df['Missing PC Data%'] = df['MissingPrice']/(df['ScrapeResult'] - df['404'])
df['PC%'] = df['PriceChanged']/(df['ScrapeResult'] - df['404'] - df['MissingPrice'])
df['Scrape Error%']=df['TotalScrapingError']/(df['TotalScrapingError']+df['ScrapeResult'])
df['TotalScrapes']=df['TotalScrapingError'] + df['ScrapeResult']
df['NoStructuredDataFound%']=df['NoStructuredDataFound']/df['ScrapeResult']

#reorder columns
columns1 = ['Merchant', 'Feed', 'Country', 'ScraperStrategy', 'DocCount', 'TotalScrapes', 'Scrape Error%', '404%', 'NoStructuredDataFound%', 'Missing OOS Data%', 'OOS%', 'Missing PC Data%', 'PC%',
            'ScrapeResult', '404', 'OutOfStock', 'PriceChanged', 'TotalMissingData', 
            'MissingAvailability', 'MissingCurrency', 'MissingPrice', 'NoStructuredDataFound',
            'TotalScrapingError', 'ProductURLMissing', 'RequestsException','DownloaderError', 
            'ParseError', 'CannotExtractStructuredData', 'CannotParsePrice']
df = df[columns1]

#set columns for the excel sheets
columns2 = ['Merchant', 'Feed', 'Country', 'ScraperStrategy', 'TotalScrapes', 'ScrapeResult', '404', 'Scrape Error%', '404%', 'Missing OOS Data%', 'OutOfStock', 'OOS%', 'Missing PC Data%', 'PriceChanged', 'PC%']
columns3 = ['Merchant', 'Feed', 'Country', 'ScraperStrategy', 'TotalScrapes', 'TotalScrapingError', 'Scrape Error%', 'ProductURLMissing','RequestsException','DownloaderError','ParseError']
columns4 = ['Merchant', 'Feed', 'Country', 'ScraperStrategy', 'TotalScrapes', 'ScrapeResult','Scrape Error%', '404', 'NoStructuredDataFound', 'NoStructuredDataFound%']

#filter thresholds 
limit_404 = 0.05
limit_OOS = 0.05
limit_PC = 0.05
limit_MissingOOS = 0.5
limit_MissingPC = 0.5
limit_ScrapeError = 0.5
limit_NSD = 0.5

#TODO: set up excel sheets for each group with scraper strategy
#filter the 404, OOS, and PC columns
high_scrape_errors_popular_products = df.loc[(df['ScraperStrategy'] == 'popular_product') & (df['Scrape Error%'] >= limit_ScrapeError)][columns3].sort_values(by='TotalScrapes', ascending=False)
high_scrape_errors_pdp_offer = df.loc[(df['ScraperStrategy'] == 'pdp_offer') & (df['Scrape Error%'] >= limit_ScrapeError)][columns3].sort_values(by='TotalScrapes', ascending=False)
high_scrape_errors_clicks = df.loc[(df['ScraperStrategy'] == 'clicks') & (df['Scrape Error%'] >= limit_ScrapeError)][columns3].sort_values(by='TotalScrapes', ascending=False)

high_404_popular_products = df.loc[(df['ScraperStrategy'] == 'popular_product') & (df['404%'] >= limit_404) & (df['Scrape Error%'] < limit_ScrapeError)][columns2].sort_values(by='TotalScrapes', ascending=False)
high_404_pdp_offer = df.loc[(df['ScraperStrategy'] == 'pdp_offer') & (df['404%'] >= limit_404) & (df['Scrape Error%'] < limit_ScrapeError)][columns2].sort_values(by='TotalScrapes', ascending=False)
high_404_clicks = df.loc[(df['ScraperStrategy'] == 'clicks') & (df['404%'] >= limit_404) & (df['Scrape Error%'] < limit_ScrapeError)][columns2].sort_values(by='TotalScrapes', ascending=False)

high_OOS_popular_products = df.loc[(df['ScraperStrategy'] == 'popular_product') & (df['Missing OOS Data%'] <= limit_MissingOOS) & (df['OOS%'] >= limit_PC) & (df['Scrape Error%'] < limit_ScrapeError)][columns2].sort_values(by='TotalScrapes', ascending=False)
high_OOS_pdp_offer = df.loc[(df['ScraperStrategy'] == 'pdp_offer') & (df['Missing OOS Data%'] <= limit_MissingOOS) & (df['OOS%'] >= limit_PC) & (df['Scrape Error%'] < limit_ScrapeError)][columns2].sort_values(by='TotalScrapes', ascending=False)
high_OOS_clicks = df.loc[(df['ScraperStrategy'] == 'clicks') & (df['Missing OOS Data%'] <= limit_MissingOOS) & (df['OOS%'] >= limit_PC) & (df['Scrape Error%'] < limit_ScrapeError)][columns2].sort_values(by='TotalScrapes', ascending=False)

high_PC_popular_products = df.loc[(df['ScraperStrategy'] == 'popular_product') & (df['Missing PC Data%'] <= limit_MissingPC) & (df['PC%'] >= limit_PC) & (df['Scrape Error%'] < limit_ScrapeError)][columns2].sort_values(by='TotalScrapes', ascending=False)
high_PC_pdp_offer = df.loc[(df['ScraperStrategy'] == 'pdp_offer') & (df['Missing PC Data%'] <= limit_MissingPC) & (df['PC%'] >= limit_PC) & (df['Scrape Error%'] < limit_ScrapeError)][columns2].sort_values(by='TotalScrapes', ascending=False)
high_PC_clicks = df.loc[(df['ScraperStrategy'] == 'clicks') & (df['Missing PC Data%'] <= limit_MissingPC) & (df['PC%'] >= limit_PC) & (df['Scrape Error%'] < limit_ScrapeError)][columns2].sort_values(by='TotalScrapes', ascending=False)

high_NSD_popular_products = df.loc[(df['ScraperStrategy'] == 'popular_product') & (df['NoStructuredDataFound%'] >= limit_NSD)][columns4].sort_values(by='TotalScrapes', ascending=False)
high_NSD_pdp_offer = df.loc[(df['ScraperStrategy'] == 'pdp_offer') & (df['NoStructuredDataFound%'] >= limit_NSD)][columns4].sort_values(by='TotalScrapes', ascending=False)
high_NSD_clicks = df.loc[(df['ScraperStrategy'] == 'clicks') & (df['NoStructuredDataFound%'] >= limit_NSD)][columns4].sort_values(by='TotalScrapes', ascending=False)

#set the sheet names for the excel
popular_products_scrape_error_sheet_name = ('popular_product SE > ' + str(round(limit_ScrapeError*100)) + '%')
pdp_offer_scrape_error_sheet_name = ('pdp_offer SE > ' + str(round(limit_ScrapeError*100)) + '%')
clicks_scrape_error_sheet_name = ('Clicks ScrapeError > ' + str(round(limit_ScrapeError*100)) + '%')

popular_products_sheet_name_404 = ('popular_product 404 > ' + str(round(limit_404*100)) + '%')
pdp_offer_sheet_name_404 = ('pdp_offer 404 > ' + str(round(limit_404*100)) + '%')
clicks_sheet_name_404 = ('Clicks 404 > ' + str(round(limit_404*100)) + '%')

popular_products_OOS_sheet_name = ('popular_product OOS > ' + str(round(limit_OOS*100)) + '%')
pdp_offer_OOS_sheet_name = ('pdp_offer OOS > ' + str(round(limit_OOS*100)) + '%')
clicks_OOS_sheet_name = ('Clicks OOS > ' + str(round(limit_OOS*100)) + '%')

popular_products_PC_sheet_name = ('popular_product PC > ' + str(round(limit_PC*100)) + '%')
pdp_offer_PC_sheet_name = ('pdp_offer PC > ' + str(round(limit_PC*100)) + '%')
clicks_PC_sheet_name = ('Clicks PC > ' + str(round(limit_PC*100)) + '%')

popular_products_NSD_sheet_name = ('popular_product NSD > ' + str(round(limit_NSD*100)) + '%')
pdp_offer_NSD_sheet_name = ('pdp_offer NSD > ' + str(round(limit_NSD*100)) + '%')
clicks_NSD_sheet_name = ('Clicks No Struct Data > ' + str(round(limit_NSD*100)) + '%')

popular_products_sheets = [popular_products_scrape_error_sheet_name, popular_products_sheet_name_404, popular_products_OOS_sheet_name, popular_products_PC_sheet_name, popular_products_NSD_sheet_name]
pdp_offer_sheets = [pdp_offer_scrape_error_sheet_name, pdp_offer_sheet_name_404, pdp_offer_OOS_sheet_name, pdp_offer_PC_sheet_name, pdp_offer_NSD_sheet_name]
clicks_sheets = [clicks_scrape_error_sheet_name, clicks_sheet_name_404, clicks_OOS_sheet_name, clicks_PC_sheet_name, clicks_NSD_sheet_name]

#write to excel
writer = pd.ExcelWriter("Scraper QC "+ start_date +" 8am to " + end_date + " 8am.xlsx", engine='xlsxwriter')
#clicks
high_scrape_errors_clicks.to_excel(writer, sheet_name=clicks_scrape_error_sheet_name, index=False)
high_404_clicks.to_excel(writer, sheet_name=clicks_sheet_name_404, index=False)
high_NSD_clicks.to_excel(writer, sheet_name=clicks_NSD_sheet_name, index=False)
high_OOS_clicks.to_excel(writer, sheet_name=clicks_OOS_sheet_name, index=False)
high_PC_clicks.to_excel(writer, sheet_name=clicks_PC_sheet_name, index=False)
#popular_products
high_scrape_errors_popular_products.to_excel(writer, sheet_name=popular_products_scrape_error_sheet_name, index=False)
high_404_popular_products.to_excel(writer, sheet_name=popular_products_sheet_name_404, index=False)
high_NSD_popular_products.to_excel(writer, sheet_name=popular_products_NSD_sheet_name, index=False)
high_OOS_popular_products.to_excel(writer, sheet_name=popular_products_OOS_sheet_name, index=False)
high_PC_popular_products.to_excel(writer, sheet_name=popular_products_PC_sheet_name, index=False)
#pdp_offer
high_scrape_errors_pdp_offer.to_excel(writer, sheet_name=pdp_offer_scrape_error_sheet_name, index=False)
high_404_pdp_offer.to_excel(writer, sheet_name=pdp_offer_sheet_name_404, index=False)
high_NSD_pdp_offer.to_excel(writer, sheet_name=pdp_offer_NSD_sheet_name, index=False)
high_OOS_pdp_offer.to_excel(writer, sheet_name=pdp_offer_OOS_sheet_name, index=False)
high_PC_pdp_offer.to_excel(writer, sheet_name=pdp_offer_PC_sheet_name, index=False)

df.sort_values(by='TotalScrapes', ascending=False).to_excel(writer, sheet_name='All Merchant Data', index=False)

for tab in popular_products_sheets:
    writer.sheets[tab].set_tab_color('blue')

for tab in clicks_sheets:
    writer.sheets[tab].set_tab_color('green')

# Add some cell formats.
pct_format = writer.book.add_format({'num_format': '0%'})
highlight_column_format = writer.book.add_format({'num_format': '0%', 'bg_color': '#f4aa42'})
comma_format = writer.book.add_format({'num_format': '#,##0'})
                                                  
# Set the formats on each tab
writer.sheets[popular_products_scrape_error_sheet_name].set_column('D:J',None, comma_format)
writer.sheets[pdp_offer_scrape_error_sheet_name].set_column('D:J',None, comma_format)
writer.sheets[clicks_scrape_error_sheet_name].set_column('D:J',None, comma_format)

#set the format for each column in each tab on these rows
writer.sheets[popular_products_sheet_name_404].set_column('E:O',None, comma_format)
writer.sheets[popular_products_sheet_name_404].set_column('H:J',None, pct_format)
writer.sheets[popular_products_sheet_name_404].set_column('L:M',None, pct_format)
writer.sheets[popular_products_sheet_name_404].set_column('O:O',None, pct_format)

writer.sheets[pdp_offer_sheet_name_404].set_column('H:J',None, pct_format)
writer.sheets[pdp_offer_sheet_name_404].set_column('E:O',None, comma_format)
writer.sheets[pdp_offer_sheet_name_404].set_column('L:M',None, pct_format)
writer.sheets[pdp_offer_sheet_name_404].set_column('O:O',None, pct_format)

writer.sheets[clicks_sheet_name_404].set_column('E:O',None, comma_format)
writer.sheets[clicks_sheet_name_404].set_column('H:J',None, pct_format)
writer.sheets[clicks_sheet_name_404].set_column('L:M',None, pct_format)
writer.sheets[clicks_sheet_name_404].set_column('O:O',None, pct_format)

writer.sheets[popular_products_NSD_sheet_name].set_column('E:F',None, comma_format)
writer.sheets[popular_products_NSD_sheet_name].set_column('G:G',None, pct_format)
writer.sheets[popular_products_NSD_sheet_name].set_column('H:I',None, comma_format)
writer.sheets[popular_products_NSD_sheet_name].set_column('J:J',None, pct_format)

writer.sheets[pdp_offer_NSD_sheet_name].set_column('E:F',None, comma_format)
writer.sheets[pdp_offer_NSD_sheet_name].set_column('G:G',None, pct_format)
writer.sheets[pdp_offer_NSD_sheet_name].set_column('H:I',None, comma_format)
writer.sheets[pdp_offer_NSD_sheet_name].set_column('J:J',None, pct_format)

writer.sheets[clicks_NSD_sheet_name].set_column('E:F',None, comma_format)
writer.sheets[clicks_NSD_sheet_name].set_column('G:G',None, pct_format)
writer.sheets[clicks_NSD_sheet_name].set_column('H:I',None, comma_format)
writer.sheets[clicks_NSD_sheet_name].set_column('J:J',None, pct_format)

writer.sheets[popular_products_OOS_sheet_name].set_column('E:O',None, comma_format)
writer.sheets[popular_products_OOS_sheet_name].set_column('H:J',None, pct_format)
writer.sheets[popular_products_OOS_sheet_name].set_column('L:M',None, pct_format)
writer.sheets[popular_products_OOS_sheet_name].set_column('O:O',None, pct_format)

writer.sheets[pdp_offer_OOS_sheet_name].set_column('E:O',None, comma_format)
writer.sheets[pdp_offer_OOS_sheet_name].set_column('H:J',None, pct_format)
writer.sheets[pdp_offer_OOS_sheet_name].set_column('L:M',None, pct_format)
writer.sheets[pdp_offer_OOS_sheet_name].set_column('O:O',None, pct_format)

writer.sheets[clicks_OOS_sheet_name].set_column('E:O',None, comma_format)
writer.sheets[clicks_OOS_sheet_name].set_column('H:J',None, pct_format)
writer.sheets[clicks_OOS_sheet_name].set_column('L:M',None, pct_format)
writer.sheets[clicks_OOS_sheet_name].set_column('O:O',None, pct_format)

writer.sheets[popular_products_PC_sheet_name].set_column('E:O',None, comma_format)
writer.sheets[popular_products_PC_sheet_name].set_column('H:J',None, pct_format)
writer.sheets[popular_products_PC_sheet_name].set_column('L:M',None, pct_format)
writer.sheets[popular_products_PC_sheet_name].set_column('O:O',None, pct_format)

writer.sheets[pdp_offer_PC_sheet_name].set_column('E:O',None, comma_format)
writer.sheets[pdp_offer_PC_sheet_name].set_column('H:J',None, pct_format)
writer.sheets[pdp_offer_PC_sheet_name].set_column('L:M',None, pct_format)
writer.sheets[pdp_offer_PC_sheet_name].set_column('O:O',None, pct_format)

writer.sheets[clicks_PC_sheet_name].set_column('E:O',None, comma_format)
writer.sheets[clicks_PC_sheet_name].set_column('H:J',None, pct_format)
writer.sheets[clicks_PC_sheet_name].set_column('L:M',None, pct_format)
writer.sheets[clicks_PC_sheet_name].set_column('O:O',None, pct_format)

writer.sheets['All Merchant Data'].set_column('E:AC',None, comma_format)
writer.sheets['All Merchant Data'].set_column('G:M',None, pct_format)

writer.sheets[popular_products_scrape_error_sheet_name].set_column('G:G',None, highlight_column_format)
writer.sheets[popular_products_sheet_name_404].set_column('I:I',None, highlight_column_format)
writer.sheets[popular_products_NSD_sheet_name].set_column('J:J',None, highlight_column_format)
writer.sheets[popular_products_OOS_sheet_name].set_column('L:L',None, highlight_column_format)
writer.sheets[popular_products_PC_sheet_name].set_column('O:O',None, highlight_column_format)

writer.sheets[pdp_offer_scrape_error_sheet_name].set_column('G:G',None, highlight_column_format)
writer.sheets[pdp_offer_sheet_name_404].set_column('I:I',None, highlight_column_format)
writer.sheets[pdp_offer_NSD_sheet_name].set_column('J:J',None, highlight_column_format)
writer.sheets[pdp_offer_OOS_sheet_name].set_column('L:L',None, highlight_column_format)
writer.sheets[pdp_offer_PC_sheet_name].set_column('O:O',None, highlight_column_format)

writer.sheets[clicks_scrape_error_sheet_name].set_column('G:G',None, highlight_column_format)
writer.sheets[clicks_sheet_name_404].set_column('I:I',None, highlight_column_format)
writer.sheets[clicks_NSD_sheet_name].set_column('J:J',None, highlight_column_format)
writer.sheets[clicks_OOS_sheet_name].set_column('L:L',None, highlight_column_format)
writer.sheets[clicks_PC_sheet_name].set_column('O:O',None, highlight_column_format)

# Close the Pandas Excel writer and output the Excel file.
writer.save()


