select brand_url, series_url,
       case when store_feed <> '' then concat (store_merchant_id, ' - ' , store_feed) else store_merchant_id end as store_merchant_id,
       count(*) as offers,
       sum(case when split(created,'T')[1] = (select max(split(created,'T')[1]) from "catalog-{cc}"."catalog_{cc}_{catalog_date}") then 0 else 1 end) as copied_offers,
       sum(case when comparable = true then 1 when comparable = false then 0 end) as comparable_offers,
       sum(case when cpc > 0 then 1 else 0 end) as bidding_offers,
       sum(case when store_feed = 'asa' then 1 else 0 end) as asa_offers,
       sum(case when store_merchant_id = {shopee_id} and seller_name in ({ams_sellers}) then 1 else 0 end) as ams_offers,
       sum(case when store_merchant_id in ({lazada_id},{lazmall_id},{taobao_id}) then 1 else 0 end) as lazada_offers,
       sum(case when store_merchant_id = {shopee_id} then 1 else 0 end) as shopee_offers
from "catalog-{cc}"."catalog_{cc}_{catalog_date}"
where is_404 = false and out_of_stock = false 
  and api_only = false and brand_url <> '' and series_url <> ''
group by brand_url, series_url, 3