with 
rules as (select comparableurl, comparabletype, rule_id, brand_url, try(split(category_url,'/')[1]) as cat_l1, 
                  try(split(category_url,'/')[2]) as cat_l2, try(split(category_url,'/')[3]) as cat_l3, 
                  try(split(category_url,'/')[4]) as cat_l4, model_url, series_url, 
                  characteristic_c1_value, characteristic_c2_value, characteristic_c3_value
                  from "catalog-{cc}"."comparable_{cc}_{catalog_date}"),
                  
offers as (SELECT id, brand_url, series_url, model_url, 
            characteristic_c1_value, characteristic_c2_value, characteristic_c3_value, 
            case when split(created,'T')[1] = (select max(split(created,'T')[1]) from "catalog-{cc}"."catalog_{cc}_{catalog_date}") then 0 else 1 end copied_offer,
            store_merchant_id
          FROM "catalog-{cc}"."catalog_{cc}_{catalog_date}"
          where comparable=true and is_404 = false and api_only = false and out_of_stock = false),                   

rules_offers_variant as (select rules.comparableurl, rules.comparabletype, rules.brand_url, rules.series_url, 
                                rules.model_url, rules.cat_l1, rules.cat_l2, rules.cat_l3, rules.cat_l4, 
                                rules.characteristic_c1_value, rules.characteristic_c2_value, 
                                rules.characteristic_c3_value,
                                count(offers.id) as offers, 
                                coalesce(sum(offers.copied_offer),0) as copied_offers,
                                coalesce(count(distinct store_merchant_id),0) as merchants,
                                try(array_join(array_agg(distinct store_merchant_id), ',')) as merchant_list
                          from rules left join offers 
                          on (rules.brand_url = offers.brand_url and 
                              rules.series_url = offers.series_url and 
                              rules.model_url = offers.model_url and
                              rules.characteristic_c1_value = offers.characteristic_c1_value and
                              rules.characteristic_c2_value = offers.characteristic_c2_value and
                              rules.characteristic_c3_value = offers.characteristic_c3_value)
                          where rules.comparabletype = 'variant'
                          group by 1,2,3,4,5,6,7,8,9,10,11,12),
                          
rules_offers_model as (select rules.comparableurl, rules.comparabletype, rules.brand_url, rules.series_url, 
                                rules.model_url, rules.cat_l1, rules.cat_l2, rules.cat_l3, rules.cat_l4, 
                                rules.characteristic_c1_value, rules.characteristic_c2_value, 
                                rules.characteristic_c3_value,
                                count(offers.id) as offers, 
                                coalesce(sum(offers.copied_offer),0) as copied_offers,
                                coalesce(count(distinct store_merchant_id),0) as merchants,
                                try(array_join(array_agg(distinct store_merchant_id), ',')) as merchant_list
                          from rules left join offers 
                          on (rules.brand_url = offers.brand_url and 
                              rules.series_url = offers.series_url and 
                              rules.model_url = offers.model_url)
                          where rules.comparabletype = 'model'
                          group by 1,2,3,4,5,6,7,8,9,10,11,12)                          

select rules_offers_variant.* 
from rules_offers_variant

union all

select rules_offers_model.* 
from rules_offers_model
order by offers desc
