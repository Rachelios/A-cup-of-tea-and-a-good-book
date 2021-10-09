SELECT sum(total_order) as total_orders, sum(Net_Revenue) as net_rev, ipg_affiliateNetwork, ipg_date

  FROM [dbo].[BI_View_Commissions]
where  ipg_site in ('iprice.vn', 'iprice.ph', 
'iprice.co.id', 'iprice.my', 'iprice.sg',
 'iprice.hk', 'ipricethailand.com')
 -- and ipg_section in ('sponsored-native', 'pc-sponsored')
and ipg_date >= '2021-06-01'
-- and ipg_merchantName = 'Tokopedia'
and ipg_affiliate_id not in (1080, 1048)
and ipg_affiliateNetwork = 'lazada'
group by  ipg_affiliateNetwork, ipg_date
order by ipg_date DESC



-- step 1: get list of brand with their respective market demands 
with base_data as (
SELECT brand_name, brand_url, category_name, category_url,  
sum(price_usd_sale) as brand_GMV

FROM "catalog-my".catalog_my_20211004
where  brand_name is not null
and brand_name not in ('', 'NO BRANDS')
group by 1,2,3,4
 
)
-- step 2:  calculate cum_sum and cum_perc 
select brand_name, brand_url, category_name, category_url, brand_GMV, sum(brand_GMV) OVER (PARTITION BY category_name, category_url 
                                                                                          ) AS market_GMV , brand_GMV/(sum(brand_GMV) OVER (PARTITION BY category_name, category_url )) as cum_perc
from base_data
where category_url like 'camera-photo/drones%'

group by 1,2,3,4,5

limit 10

-- step 3: calculate cum_sum and cum_perc
















order by ipg_year_week DESC


select sum(ga_totalEvents) as totalClicks, 
ipg_cc, ga_eventAction, ipg_year_month,
ipg_year_week, ipg_merchantName
from FactAnalyticsConversion
where ipg_product in ('pc', 'shop', 'trends')
and ipg_date >= '2021-07-01'
group by ipg_cc, ga_eventAction, ipg_year_month,
ipg_year_week, ipg_merchantName



   -- and ipg_affiliate_id = 1080
   group by 
  ipg_merchantName, 
    ipg_year_week
   order by ipg_year_week DESC


select count (*), ipg_year from ViewAllRevenue
group by ipg_year


SELECT sum(ipg_commission) as gross_rev, sum(ipg_order) as total_orders, 
ipg_year_week
  FROM [dbo].[ViewAllRevenue]
where ipg_date >= '2021-07-01'
   and ipg_affiliate_id = 1080
   group by 
 --  ipg_merchantName,
    ipg_year_week
   order by ipg_year_week DESC




-- THIS IS A LINE BREAK 
-- TODO: NEW, DIFFERENT QUERY BELOW! 

  SELECT B.store_merchant_id, sum(B.products) as total_pdp_offers, count( distinct B.parent_url) as num_PDPs
FROM (
  SELECT *
  FROM
  (with catalog_ph as
   (SELECT A.parent_url, 
    A.store_merchant_id, 
    A.category_url, 
    A.products,
    'ph' as cc
   --  NTILE(10) OVER(partition by A.parent_url order by A.price_sale ASC) as percentile
    FROM (
      SELECT category_url, 
      store_merchant_id,
    --  price_sale, 
      (case when series_url <>'' then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'/') when series_url ='' then CONCAT('/compare/',brand_url,'-',model_url,'/') end) as parent_url, 
      (case when characteristic_c1_value = '' then 'no-variant'
      when (series_url <>'' AND characteristic_c2_value ='') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url <>'' AND characteristic_c3_value ='') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           when (series_url <>'' AND characteristic_c3_value <>'') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c2_value ='') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c3_value ='') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           
      when (series_url ='' AND characteristic_c3_value <>'') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
end) as variant_url, count(id) as products
      FROM "catalog-ph"."catalog_ph_20210912"
      WHERE comparable =TRUE
      group by 1, 2, 3, 4
     ) as A),
   catalog_my as
   (SELECT A.parent_url, 
    A.store_merchant_id, 
    A.category_url, 
    A.products,
    'my' as cc
   --  NTILE(10) OVER(partition by A.parent_url order by A.price_sale ASC) as percentile
    FROM (
      SELECT category_url, 
      store_merchant_id,
    --  price_sale, 
      (case when series_url <>'' then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'/') when series_url ='' then CONCAT('/compare/',brand_url,'-',model_url,'/') end) as parent_url, 
      (case when characteristic_c1_value = '' then 'no-variant'
      when (series_url <>'' AND characteristic_c2_value ='') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url <>'' AND characteristic_c3_value ='') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           when (series_url <>'' AND characteristic_c3_value <>'') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c2_value ='') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c3_value ='') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           
      when (series_url ='' AND characteristic_c3_value <>'') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
end) as variant_url, count(id) as products
      FROM "catalog-my"."catalog_my_20210912"
      WHERE comparable =TRUE
      group by 1, 2, 3, 4
     ) as A),
    catalog_sg as
   (SELECT A.parent_url, 
    A.store_merchant_id, 
    A.category_url, 
    A.products,
    'sg' as cc
   --  NTILE(10) OVER(partition by A.parent_url order by A.price_sale ASC) as percentile
    FROM (
      SELECT category_url, 
      store_merchant_id,
    --  price_sale, 
      (case when series_url <>'' then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'/') when series_url ='' then CONCAT('/compare/',brand_url,'-',model_url,'/') end) as parent_url, 
      (case when characteristic_c1_value = '' then 'no-variant'
      when (series_url <>'' AND characteristic_c2_value ='') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url <>'' AND characteristic_c3_value ='') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           when (series_url <>'' AND characteristic_c3_value <>'') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c2_value ='') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c3_value ='') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           
      when (series_url ='' AND characteristic_c3_value <>'') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
end) as variant_url, count(id) as products
      FROM "catalog-sg"."catalog_sg_20210912"
      WHERE comparable =TRUE
      group by 1, 2, 3, 4
     ) as A),
     catalog_th as
   (SELECT A.parent_url, 
    A.store_merchant_id, 
    A.category_url, 
    A.products,
    'th' as cc
   --  NTILE(10) OVER(partition by A.parent_url order by A.price_sale ASC) as percentile
    FROM (
      SELECT category_url, 
      store_merchant_id,
    --  price_sale, 
      (case when series_url <>'' then CONCAT('/ราคา/',brand_url,'-',series_url,'-',model_url,'/') when series_url ='' then CONCAT('/ราคา/',brand_url,'-',model_url,'/') end) as parent_url, 
      (case when characteristic_c1_value = '' then 'no-variant'
      when (series_url <>'' AND characteristic_c2_value ='') then CONCAT('/ราคา/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url <>'' AND characteristic_c3_value ='') then CONCAT('/ราคา/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           when (series_url <>'' AND characteristic_c3_value <>'') then CONCAT('/ราคา/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c2_value ='') then CONCAT('/ราคา/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c3_value ='') then CONCAT('/ราคา/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           
      when (series_url ='' AND characteristic_c3_value <>'') then CONCAT('/ราคา/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
end) as variant_url, count(id) as products
      FROM "catalog-th"."catalog_th_20210912"
      WHERE comparable =TRUE
      group by 1, 2, 3, 4
     ) as A),
     catalog_vn as
   (SELECT A.parent_url, 
    A.store_merchant_id, 
    A.category_url, 
    A.products,
    'vn' as cc
   --  NTILE(10) OVER(partition by A.parent_url order by A.price_sale ASC) as percentile
    FROM (
      SELECT category_url, 
      store_merchant_id,
    --  price_sale, 
      (case when series_url <>'' then CONCAT('/gia-ban/',brand_url,'-',series_url,'-',model_url,'/') when series_url ='' then CONCAT('/gia-ban/',brand_url,'-',model_url,'/') end) as parent_url, 
      (case when characteristic_c1_value = '' then 'no-variant'
      when (series_url <>'' AND characteristic_c2_value ='') then CONCAT('/gia-ban/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url <>'' AND characteristic_c3_value ='') then CONCAT('/gia-ban/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           when (series_url <>'' AND characteristic_c3_value <>'') then CONCAT('/gia-ban/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c2_value ='') then CONCAT('/gia-ban/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c3_value ='') then CONCAT('/gia-ban/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           
      when (series_url ='' AND characteristic_c3_value <>'') then CONCAT('/gia-ban/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
end) as variant_url, count(id) as products
      FROM "catalog-vn"."catalog_vn_20210912"
      WHERE comparable =TRUE
      group by 1, 2, 3, 4
     ) as A),
     catalog_my as
   (SELECT A.parent_url, 
    A.store_merchant_id, 
    A.category_url, 
    A.products,
    'id' as cc
   --  NTILE(10) OVER(partition by A.parent_url order by A.price_sale ASC) as percentile
    FROM (
      SELECT category_url, 
      store_merchant_id,
    --  price_sale, 
      (case when series_url <>'' then CONCAT('/harga/',brand_url,'-',series_url,'-',model_url,'/') when series_url ='' then CONCAT('/harga/',brand_url,'-',model_url,'/') end) as parent_url, 
      (case when characteristic_c1_value = '' then 'no-variant'
      when (series_url <>'' AND characteristic_c2_value ='') then CONCAT('/harga/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url <>'' AND characteristic_c3_value ='') then CONCAT('/harga/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           when (series_url <>'' AND characteristic_c3_value <>'') then CONCAT('/harga/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c2_value ='') then CONCAT('/harga/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c3_value ='') then CONCAT('/harga/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           
      when (series_url ='' AND characteristic_c3_value <>'') then CONCAT('/harga/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
end) as variant_url, count(id) as products
      FROM "catalog-id"."catalog_id_20210912"
      WHERE comparable =TRUE
      group by 1, 2, 3, 4
     ) as A),
     catalog_hk as
   (SELECT A.parent_url, 
    A.store_merchant_id, 
    A.category_url, 
    A.products,
    'hk' as cc
   --  NTILE(10) OVER(partition by A.parent_url order by A.price_sale ASC) as percentile
    FROM (
      SELECT category_url, 
      store_merchant_id,
    --  price_sale, 
      (case when series_url <>'' then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'/') when series_url ='' then CONCAT('/compare/',brand_url,'-',model_url,'/') end) as parent_url, 
      (case when characteristic_c1_value = '' then 'no-variant'
      when (series_url <>'' AND characteristic_c2_value ='') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url <>'' AND characteristic_c3_value ='') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           when (series_url <>'' AND characteristic_c3_value <>'') then CONCAT('/compare/',brand_url,'-',series_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c2_value ='') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'/')
      when (series_url ='' AND characteristic_c3_value ='') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'/')           
      when (series_url ='' AND characteristic_c3_value <>'') then CONCAT('/compare/',brand_url,'-',model_url,'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c1_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c2_value,' ','-'),'+','-'),'.','')),'-',LOWER(REPLACE(REPLACE(REPLACE(characteristic_c3_value,' ','-'),'+','-'),'.','')),'/')
end) as variant_url, count(id) as products
      FROM "catalog-hk"."catalog_hk_20210912"
      WHERE comparable =TRUE
      group by 1, 2, 3, 4
     ) as A), 
catalog as (select * from catalog_ph
            union all 
            select * from catalog_my
            union all 
            select * from catalog_sg
            union all 
            select * from catalog_th
            union all
            select * from catalog_vn
            union all
            select * from catalog_id
            union all
            select * from catalog_hk),
   
   top80pv as (select landingpagepath, cc 
               from (
                 select a.cc, 
                 year, 
                 month, 
                 landingcontentgroup1, 
                 landingcontentgroup2,
                 landingpagepath, 
                 pageviews,
                 sum(pageviews) over (partition by a.cc order by pageviews desc) as rolling_sum,
                 tot_pageviews,
                 sum(pageviews) over (partition by a.cc order by pageviews desc) / cast(tot_pageviews as decimal(12,4)) as pct_total_by_cc
                 from (
                   select cc, 
                   year, 
                   month, 
                   landingcontentgroup1, 
                   landingcontentgroup2,
                   landingpagepath, 
                   sum(pageviews) as pageviews
                   from "dalake"."ga_bi_acquisition"
                   where (landingcontentgroup1 like '%pc-model%')
                   and month = 8 and year = 2021 
                   group by cc, year, month, landingcontentgroup1, landingcontentgroup2, landingpagepath)as a
                 left join (
                   select cc, sum(pageviews) as tot_pageviews
                   from "dalake"."ga_bi_acquisition"
                   where (landingcontentgroup1 like '%pc-model%')
                           and month = 8 and year = 2021 group by cc) as b
                       on a.cc = b.cc
                       order by pageviews desc) as a
                       where a.pct_total_by_cc <= 0.8
                       order by cc, pageviews desc)
                       
SELECT catalog.parent_url, catalog.store_merchant_id, catalog.category_url,  top80pv.landingpagepath, catalog.products, catalog.cc
FROM catalog
LEFT JOIN top80pv
ON catalog.parent_url = top80pv.landingpagepath
  and catalog.cc = top80pv.cc) as A
WHERE A.landingpagepath <> ''
) as B
GROUP BY 1
ORDER BY total_pdp_offers DESC





select category1, category2, category3, category_url from 
(SELECT category1, category2, category3, category_url, count(category_url) as occurance, 
 ROW_NUMBER() OVER (PARTITION BY category1, category2, category3 
 ORDER BY COUNT(category_url) DESC) as seqnum FROM "dasync"."category_focus_my" 

group by 1,2,3,4
order by occurance desc
)
where seqnum = 1




with master_brain AS 
    (SELECT CFLstat.category1,
         CFLstat.category2,
         CFLstat.category3,
         cat.category_url as true_category_url,
         max(Expensive) AS Expensive ,
     max(CFLstat.category_url) as category_url,
         cc,
         avg(TRY(CAST(monthly_usd_GMV AS double))) AS monthly_usd_GMV,
         sum(Brand_Count) AS brand_count,
         CAST(BoughtperYear AS double) AS BoughtperYear
    FROM "dasync"."category_focus_id" as CFLstat
    left join (select category1, category2, category3, category_url from 
(SELECT category1, category2, category3, category_url, count(category_url) as occurance, 
 ROW_NUMBER() OVER (PARTITION BY category1, category2, category3 
 ORDER BY COUNT(category_url) DESC) as seqnum FROM "dasync"."category_focus_id" 

group by 1,2,3,4
order by occurance desc
)
where seqnum = 1) as cat 
on CFLstat.category1 = cat.category1
and CFLstat.category2 = cat.category2
and CFLstat.category3 = cat.category3

    GROUP BY  1,2,3,4,7,10

    UNION ALL
    SELECT CFLstat.category1,
         CFLstat.category2,
         CFLstat.category3,
         cat.category_url as true_category_url,
         max(Expensive) AS Expensive ,
     max(CFLstat.category_url) as category_url,
         cc,
         avg(TRY(CAST(monthly_usd_GMV AS double))) AS monthly_usd_GMV,
         sum(Brand_Count) AS brand_count,
         CAST(BoughtperYear AS double) AS BoughtperYear
    FROM "dasync"."category_focus_my" as CFLstat
    left join (select category1, category2, category3, category_url from 
(SELECT category1, category2, category3, category_url, count(category_url) as occurance, 
 ROW_NUMBER() OVER (PARTITION BY category1, category2, category3 
 ORDER BY COUNT(category_url) DESC) as seqnum FROM "dasync"."category_focus_my" 

group by 1,2,3,4
order by occurance desc
)
where seqnum = 1) as cat 
on CFLstat.category1 = cat.category1
and CFLstat.category2 = cat.category2
and CFLstat.category3 = cat.category3

    GROUP BY  1,2,3,4,7,10

    UNION ALL
    SELECT CFLstat.category1,
         CFLstat.category2,
         CFLstat.category3,
         cat.category_url as true_category_url,
         max(Expensive) AS Expensive ,
     max(CFLstat.category_url) as category_url,
         cc,
         avg(TRY(CAST(monthly_usd_GMV AS double))) AS monthly_usd_GMV,
         sum(Brand_Count) AS brand_count,
         CAST(BoughtperYear AS double) AS BoughtperYear
    FROM "dasync"."category_focus_ph" as CFLstat
    left join (select category1, category2, category3, category_url from 
(SELECT category1, category2, category3, category_url, count(category_url) as occurance, 
 ROW_NUMBER() OVER (PARTITION BY category1, category2, category3 
 ORDER BY COUNT(category_url) DESC) as seqnum FROM "dasync"."category_focus_ph" 

group by 1,2,3,4
order by occurance desc
)
where seqnum = 1) as cat 
on CFLstat.category1 = cat.category1
and CFLstat.category2 = cat.category2
and CFLstat.category3 = cat.category3

    GROUP BY  1,2,3,4,7,10

    UNION ALL
    SELECT CFLstat.category1,
         CFLstat.category2,
         CFLstat.category3,
         cat.category_url as true_category_url,
         max(Expensive) AS Expensive ,
     max(CFLstat.category_url) as category_url,
         cc,
         avg(TRY(CAST(monthly_usd_GMV AS double))) AS monthly_usd_GMV,
         sum(Brand_Count) AS brand_count,
         CAST(BoughtperYear AS double) AS BoughtperYear
    FROM "dasync"."category_focus_th" as CFLstat
    left join (select category1, category2, category3, category_url from 
(SELECT category1, category2, category3, category_url, count(category_url) as occurance, 
 ROW_NUMBER() OVER (PARTITION BY category1, category2, category3 
 ORDER BY COUNT(category_url) DESC) as seqnum FROM "dasync"."category_focus_th" 

group by 1,2,3,4
order by occurance desc
)
where seqnum = 1) as cat 
on CFLstat.category1 = cat.category1
and CFLstat.category2 = cat.category2
and CFLstat.category3 = cat.category3

    GROUP BY  1,2,3,4,7,10

    UNION ALL
    SELECT CFLstat.category1,
         CFLstat.category2,
         CFLstat.category3,
         cat.category_url as true_category_url,
         max(Expensive) AS Expensive ,
     max(CFLstat.category_url) as category_url,
         cc,
         avg(TRY(CAST(monthly_usd_GMV AS double))) AS monthly_usd_GMV,
         sum(Brand_Count) AS brand_count,
         CAST(BoughtperYear AS double) AS BoughtperYear
    FROM "dasync"."category_focus_sg" as CFLstat
    left join (select category1, category2, category3, category_url from 
(SELECT category1, category2, category3, category_url, count(category_url) as occurance, 
 ROW_NUMBER() OVER (PARTITION BY category1, category2, category3 
 ORDER BY COUNT(category_url) DESC) as seqnum FROM "dasync"."category_focus_sg" 

group by 1,2,3,4
order by occurance desc
)
where seqnum = 1) as cat 
on CFLstat.category1 = cat.category1
and CFLstat.category2 = cat.category2
and CFLstat.category3 = cat.category3

    GROUP BY  1,2,3,4,7,10

    UNION ALL
    SELECT CFLstat.category1,
         CFLstat.category2,
         CFLstat.category3,
         cat.category_url as true_category_url,
         max(Expensive) AS Expensive ,
     max(CFLstat.category_url) as category_url,
         cc,
         avg(TRY(CAST(monthly_usd_GMV AS double))) AS monthly_usd_GMV,
         sum(Brand_Count) AS brand_count,
         CAST(BoughtperYear AS double) AS BoughtperYear
    FROM "dasync"."category_focus_vn" as CFLstat
    left join (select category1, category2, category3, category_url from 
(SELECT category1, category2, category3, category_url, count(category_url) as occurance, 
 ROW_NUMBER() OVER (PARTITION BY category1, category2, category3 
 ORDER BY COUNT(category_url) DESC) as seqnum FROM "dasync"."category_focus_vn" 

group by 1,2,3,4
order by occurance desc
)
where seqnum = 1) as cat 
on CFLstat.category1 = cat.category1
and CFLstat.category2 = cat.category2
and CFLstat.category3 = cat.category3

    GROUP BY  1,2,3,4,7,10
 ), checkExistence AS 
    (SELECT category3,
         array_join(array_agg(distinct cc),
         ',') AS ExistIn
    FROM master_brain
    WHERE category3 NOT LIKE 'Other%'
            AND category3 NOT IN ('')
    GROUP BY  1
    ORDER BY  category3), temp AS 
    (SELECT master_brain.*,
         checkExistence.ExistIn,
         (CASE
        WHEN (Expensive > 40
            AND CAST(BoughtperYear AS double) >=4) THEN
        'expensive & freqBought'
        WHEN ( Expensive < 40
            AND CAST(BoughtperYear AS double) >=4) THEN
        'freqBought'
        WHEN ( Expensive > 40
            AND CAST(BoughtperYear AS double) <4 ) THEN
        'expensive'
        ELSE 'others' END) type_normalised
    FROM master_brain
    LEFT JOIN checkExistence
        ON master_brain.category3 = checkExistence.category3
    WHERE master_brain.category3 NOT LIKE 'Other%'
            AND master_brain.category3 NOT IN ('') )
SELECT temp.*,
         sum(monthly_usd_GMV)
    OVER (PARTITION BY category1, category2, category3, ExistIn) AS crossCountry_marketDemands , avg(Expensive)
    OVER (PARTITION BY category1, category2,category3, ExistIn) AS crossCountry_avgPrice , avg(BoughtperYear)
    OVER (PARTITION BY category1, category2,category3, ExistIn) AS crossCountry_Freq, 
    avg(monthly_usd_GMV)
    OVER (PARTITION BY category1, category2, category3, ExistIn) AS crossCountry_avgMDemands, 

  no_PDPS.total_pdp_offers,
  no_PDPS.num_parent_pdps,
  no_PDPS.num_variant_pdps
FROM temp
left join "dasync"."count_no_pdps" as no_PDPS 
on temp.true_category_url = no_PDPs.category_url
and temp.cc = no_PDPs.cc
where monthly_usd_GMV > 0
ORDER BY  Expensive desc

-/////////////////////------------

-- step 1: get all constant pages in past 4 months

with constant_pages as 
(SELECT * FROM "dalake"."pdp_constant_pages"),

-- step 2:
-- step 2a: get table with traffic and clicks to calculate CTR 

CTR as 

(SELECT A.*, A.ipg_cc as Country, COALESCE(B.Clicks,0) as Clicks FROM (SELECT landingpagepath, sum(sessions) as traffic, ipg_cc, website, date FROM "dalake"."ga_bi_acquisition" 
where date > '2021-01-01'
and website = 'IPRICE'
group by 1,3,4,5) A 
left join 
(SELECT landingpagepath, ipg_cc, website, date, sum(totalevents) as Clicks FROM "dalake"."ga_bi_conversions" 
where date > '2021-01-01'
and website = 'IPRICE'
group by 1,2,3,4) B 
on A.landingpagepath = B.landingpagepath
and A.ipg_cc = B.ipg_cc
and A.date = B.date
and A.website = B.website),

-- step 2b: get table with clicks and orders to calculate CR 
CR as 
(SELECT C.*, C.ipg_cc as C_Country, COALESCE(D.Orderss,0) as Orderss FROM 
(SELECT landingpagepath, ipg_cc, website, date, sum(totalevents) as Clicks FROM "dalake"."ga_bi_conversions" 
where date > '2021-01-01'
and website = 'IPRICE'
group by 1,2,3,4) C 
left join 
(SELECT "ipg:exiturl" as exitURL, "ipg:cc" as cc, "ipg:site" as website, "ipg:date" as date, sum("ipg:order") as Orderss FROM "ho"."ho_transactions"  
where "ipg:date" > '2021-01-01'
and "ipg:site" in ('ipricethailand.com', 'iprice.co.id', 'iprice.my', 'iprice.ph', 'iprice.vn', 'iprice.hk', 'iprice.sg')
group by 1,2,3,4) D 
on C.landingpagepath = D.exitURL
and C.ipg_cc = D.cc
and C.date = D.date),



-- step 3: calculate weighted CTR for constant pages only by date, country  

weightedCTR as 

(SELECT CTR.date, CTR.ipg_cc, SUM(CTR.Clicks) as Clicks, SUM(CTR.traffic) as traffic, (CAST(((SUM(CTR.Clicks)*1.0*100)/SUM(CTR.traffic)*1.0) as decimal(6,2)))  as weightedavg_CTR from CTR inner join constant_pages on CTR.landingpagepath = constant_pages.landing_page
and CTR.ipg_cc = constant_pages.ipg_cc
group by 1,2
order by CTR.date DESC),

-- step 4: calculate unweighted CTR for constant pages only by date, country
-- by use an avg() each landingpage and aggregate by date/country
ctr_by_constant_page as (
select CTR.landingpagepath, CTR.website, CTR.date, CTR.Country, SUM(CTR.Traffic) as Traffic, sum(CTR.Clicks) as Clicks, (CAST(((SUM(CTR.Clicks)*1.0*100)/SUM(CTR.traffic)*1.0) as decimal(6,2))) as cal_CTR from CTR inner join constant_pages on CTR.landingpagepath = constant_pages.landing_page
and CTR.ipg_cc = constant_pages.ipg_cc
group by 1,2,3,4), 
cr_by_constant_page as (
select CR.landingpagepath, CR.website, CR.date, CR.C_Country, SUM(CR.Clicks) as Clicks, sum(CR.Orderss) as Orderss, (CAST(((SUM(CR.Orderss)*1.0*100)/SUM(CR.Clicks)*1.0) as decimal(6,2))) as cal_CR from CR inner join constant_pages on CR.landingpagepath = constant_pages.landing_page
and CR.ipg_cc = constant_pages.ipg_cc
group by 1,2,3,4),
temp1 as (SELECT ctr_by_constant_page.date, ctr_by_constant_page.Country, ctr_by_constant_page.website, avg(cal_CTR) as unweightedCTR
from ctr_by_constant_page
where country = 'ID'
group by 1,2,3
order by 1 DESC), 
temp2 as (SELECT cr_by_constant_page.date, cr_by_constant_page.c_country, cr_by_constant_page.website, avg(cal_CR) as unweightedCR
from cr_by_constant_page
where c_country = 'ID'w
group by 1,2,3
order by 1 DESC)
Select temp1.*, temp2.unweightedCR , ((temp1.unweightedCTR) * (temp2.unweightedcr))/100 as unweightedOPS from  temp1 left join temp2 
on temp1.date= temp2.date
and temp1.country = temp2.c_country
and temp1.website = temp2.website








OPS Basket Size Match


with clicklog AS 
    (SELECT count(_id) AS "clicks",
         concat(substr("dt",1,4),
         '-', substr("dt", 5, 2))AS "c_MONTH", _source.site as "c_site",_source.store.merchantId as "c_merchantid", _id AS "id", _source.priceUsd.sale AS "ClickedPrice"
    FROM "click_log"."click_log_all"
    WHERE "$PATH" LIKE '%.json-aa.gz'
            AND _source is NOT null
            AND dt >= '20201001'
            AND dt <= '20210710'
            AND _source.site IN ('iprice.vn', 'iprice.ph', 'iprice.co.id', 'iprice.my', 'iprice.sg', 'iprice.hk', 'ipricethailand.com')
            AND _type = 'product'
    GROUP BY  2,3,4,5,6 ),
    transactions AS 
    (SELECT sum("ipg:order") AS orderss,
         "ipg:logid" AS logid,
         "ipg:ordervalue" AS ordervalue,
         "ipg:site" AS site,"ipg:merchantid" AS merchantid,
         "bi:year_week" as week,
     "ipg:date" as date,
         "ipg:product" as Product,
      url_decode("ipg:exiturl") as ExitUrl,
         "bi:year_month" AS month
    FROM "ho"."ho_transactions"
    WHERE "ipg:catalogid" is NOT null
    GROUP BY  2,3,4,5,6,7,8,9,10),
    
 constant_pages as 
(SELECT * FROM "dalake"."pdp_constant_pages"),
   traffic_data as 
   (SELECT landingpagepath, sum(sessions) as traffic, ipg_cc as tipg_cc, website, date FROM "dalake"."ga_bi_acquisition" 
where date > '2021-01-01'
and website = 'IPRICE'
group by 1,3,4,5),

    
temp as (SELECT clicklog.*,
         transactions.*
FROM clicklog
JOIN transactions
    ON clicklog.id = transactions.logid
       AND clicklog.c_MONTH = transactions.month
       AND clicklog.c_site = transactions.site
        AND clicklog.c_merchantid = transactions.merchantid),


temp2 as (SELECT ExitUrl, date, month, site as website, merchantid,temp.orderss as orders,Product,
 logid,temp.ordervalue, temp.ClickedPrice,
case when (  ordervalue >= 0.75 *ClickedPrice) then 'yes' 
else 'no'
end as bs_match
from temp),

temp3 as (SELECT ExitUrl, sum(orders) as sum_orders, month, website,date, constant_pages.landing_Page, constant_pages.ipg_cc
from temp2
join constant_pages
on temp2.website = constant_pages.site
and temp2.ExitUrl = constant_pages.landing_Page
where bs_match = 'yes'
group by 1,3,4,5,6,7),

weightedOPS as (select temp3.*, traffic_data.traffic, traffic_data.date as tdate, 
                (CAST(((SUM(temp3.sum_orders)*1.000)/SUM(traffic_data.traffic)*1.000) as decimal(10,5)))  as weightedavg_OPS 
from temp3 
join traffic_data 
on temp3.landing_Page = traffic_data.landingpagepath
and temp3.ipg_cc = traffic_data.tipg_cc
and temp3.date = traffic_data.date
               group by 1,2,3,4,5,6,7,8,9)

-- step n: calculate unweighted OPS for constant pages only by date, country
-- by use an avg() each landingpage and aggregate by date/country

select * from weightedOPS

SELECT weightedOPS.date, weightedOPS.ipg_cc, weightedOPS.website, sum(weightedOPS.sum_orders) as Orders, sum(weightedOPS.traffic) as Sessions, avg(weightedavg_OPS) as unweightedOPS
from weightedOPS
group by 1,2,3
