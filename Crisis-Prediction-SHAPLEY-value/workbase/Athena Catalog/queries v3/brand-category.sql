with                   
catalog as (select brand_url,
                  case when comparable = true then 1 when comparable = false then 0 end as comparable_offer, 
                  case split(category_url,'/')[1] when '' then null else split(category_url,'/')[1] end as cat_l1, 
                          try(split(category_url,'/')[2]) as cat_l2,
                          try(split(category_url,'/')[3]) as cat_l3, 
                          try(split(category_url,'/')[4]) as cat_l4,
                  case when store_feed <> '' then concat (store_merchant_id, ' - ' , store_feed) else store_merchant_id end as store_merchant_id,
                  case when split(created,'T')[1] = (select max(split(created,'T')[1]) from "catalog-{cc}"."catalog_{cc}_{catalog_date}") then 0 else 1 end copied_offer,
                  case when cpc > 0 then 1 else 0 end as bidding_offer,
                  case when store_feed = 'asa' then 1 else 0 end as asa_offer,
                  case when store_merchant_id = {shopee_id} and seller_name in ({ams_sellers}) then 1 else 0 end as ams_offer,
                  case when store_merchant_id in ({lazada_id},{lazmall_id},{taobao_id}) then 1 else 0 end as lazada_offer,
                  case when store_merchant_id = {shopee_id} then 1 else 0 end as shopee_offer                       
            from "catalog-{cc}"."catalog_{cc}_{catalog_date}"
            where is_404 = false and out_of_stock = false 
              and api_only = false and brand_url <> '' and category_url <> ''),

catalog_rollup as (select * from (
                    select brand_url, cat_l1, cat_l2, cat_l3, cat_l4, store_merchant_id,
                            count(*) as offers,
                            sum(copied_offer) as copied_offers, 
                            sum(comparable_offer) as comparable_offers,
                            sum(bidding_offer) as bidding_offers,
                            sum(asa_offer) as asa_offers,
                            sum(ams_offer) as ams_offers,
                            sum(lazada_offer) as lazada_offers,
                            sum(shopee_offer) as shopee_offers,
                            row_number() over (partition by brand_url, cat_l1, cat_l2, cat_l3, cat_l4, store_merchant_id order by count(*) desc) as rn
                    from catalog
                    group by GROUPING SETS (  (brand_url,cat_l1,store_merchant_id),
                                              (brand_url,cat_l1,cat_l2,store_merchant_id),
                                              (brand_url,cat_l1,cat_l2,cat_l3,store_merchant_id),
                                              (brand_url,cat_l1,cat_l2,cat_l3,cat_l4,store_merchant_id))))
                                                    
select * from catalog_rollup where rn = 1