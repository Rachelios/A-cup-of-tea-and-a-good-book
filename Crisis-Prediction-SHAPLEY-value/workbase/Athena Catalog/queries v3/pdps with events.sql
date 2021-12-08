with sessions as (SELECT cc, landingpagepath, ipg_product, ipg_subproduct, try(split(landingpagepath,'/')[3]) as comparableurl,
                 sum(sessions) as sessions
                  FROM "dalake"."ga_bi_acquisition"
                  WHERE ipg_product = 'pc' and year in ({year_list})
                    and cc = '{cc}' and date between '{min_date}' and '{max_date}' 
                    and month in ({month_list})
                    and landingpagepath not in ('/search/','(not set)','/r/p/',
                                                '/translate_c','/translate','/search') and website = 'IPRICE'
                  group by 1,2,3,4,5),

clicks as (SELECT cc, landingpagepath, ipg_product, ipg_subproduct, try(split(landingpagepath,'/')[3]) as comparableurl,
                 sum(totalevents) as clicks
                  FROM "dalake"."ga_bi_conversions"
                  WHERE ipg_product = 'pc' and year in ({year_list})
                    and cc = '{cc}' and date between '{min_date}' and '{max_date}' 
                    and month in ({month_list})
                    and landingpagepath not in ('/search/','(not set)','/r/p/',
                                                '/translate_c','/translate','/search') and website = 'IPRICE'
                  group by 1,2,3,4,5),                 

metrics as (select sessions.*, clicks.clicks 
                    from sessions full outer join clicks 
                    on sessions.cc=clicks.cc 
                    and sessions.landingpagepath=clicks.landingpagepath),

metrics_rules as (select metrics.cc, metrics.landingpagepath, metrics.ipg_product, 
                          metrics.ipg_subproduct, metrics.comparableurl, metrics.sessions, metrics.clicks,
                          rules.rule_id, rules.brand_url, try(split(category_url,'/')[1]) as cat_l1, 
                          try(split(category_url,'/')[2]) as cat_l2, try(split(category_url,'/')[3]) as cat_l3, 
                          try(split(category_url,'/')[4]) as cat_l4, rules.model_url, rules.series_url, 
                          rules.characteristic_c1_value, rules.characteristic_c2_value, rules.characteristic_c3_value,
               rules.offercount, rules.store_count
                  from metrics left join "catalog-{cc}"."comparable_{cc}_{catalog_date}" as rules
                  on metrics.comparableurl = rules.comparableurl),
                  
offers as (SELECT id, brand_url, series_url, model_url, 
            characteristic_c1_value, characteristic_c2_value, characteristic_c3_value, 
            case when split(created,'T')[1] = (select max(split(created,'T')[1]) from "catalog-{cc}"."catalog_{cc}_{catalog_date}") then 0 else 1 end copied_offer,
            store_merchant_id
          FROM "catalog-{cc}"."catalog_{cc}_{catalog_date}"
          where comparable=true and is_404 = false and api_only = false and out_of_stock = false),                   

metrics_rules_offers_variant as (select metrics_rules.*, 
                                count(offers.id) as offers, 
                                coalesce(sum(offers.copied_offer),0) as copied_offers,
                                coalesce(count(distinct store_merchant_id),0) as merchants,
                                try(array_join(array_agg(distinct store_merchant_id), ',')) as merchant_list
                          from metrics_rules left join offers 
                          on (metrics_rules.brand_url = offers.brand_url and 
                              metrics_rules.series_url = offers.series_url and 
                              metrics_rules.model_url = offers.model_url and
                              metrics_rules.characteristic_c1_value = offers.characteristic_c1_value and
                              metrics_rules.characteristic_c2_value = offers.characteristic_c2_value and
                              metrics_rules.characteristic_c3_value = offers.characteristic_c3_value)
                          where metrics_rules.ipg_subproduct = 'variant'
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20),
                          
metrics_rules_offers_model as (select metrics_rules.*, 
                                count(offers.id) as offers, 
                                coalesce(sum(offers.copied_offer),0) as copied_offers,
                                coalesce(count(distinct store_merchant_id),0) as merchants,
                                try(array_join(array_agg(distinct store_merchant_id), ',')) as merchant_list
                          from metrics_rules left join offers 
                          on (metrics_rules.brand_url = offers.brand_url and 
                              metrics_rules.series_url = offers.series_url and 
                              metrics_rules.model_url = offers.model_url)
                          where metrics_rules.ipg_subproduct = 'model'
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)                          

select metrics_rules_offers_variant.* 
from metrics_rules_offers_variant

union all

select metrics_rules_offers_model.* 
from metrics_rules_offers_model
order by sessions desc
