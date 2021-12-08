SELECT cc, landingpagepath, ipg_product, ipg_subproduct,
                  
                 try(case when ipg_subproduct like '%brand%' 
                    then split(landingpagepath,'/')[2] end) as brand_url,
                  
                 try(case when ipg_subproduct like '%series%' 
                    then array_except(split(landingpagepath,'/'), 
                                      ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end])[2] end) as series_url,
                  
                 try(case when ipg_subproduct like '%model%' 
                       then array_except(split(landingpagepath,'/'), ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end, 
                                                                    case when ipg_subproduct like '%series%' 
                                                                         then array_except(split(landingpagepath,'/'), ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end])[2] 
                                                                    end])[2] end) as model_url,
                  
                 try(case when ipg_subproduct like '%gender%' then 
                       case when ((landingpagepath like '%/women/%') or (landingpagepath like '%/wanita/%') or (landingpagepath like '%/nu/%') or (landingpagepath like '%/ผหญง/%')) then 2 
                            when ((landingpagepath like '%/men/%') or (landingpagepath like '%/pria/%') or (landingpagepath like '%/nam/%') or (landingpagepath like '%/ผชาย/%')) then 1 end
                       else null end) as gender,  
                  
                 try(case when ipg_subproduct like '%category%' then
                      try(array_except(split(landingpagepath,'/'),
                            ARRAY [case when ipg_subproduct like 'brand%' then split(landingpagepath,'/')[2] end,
                                    case when ipg_subproduct like '%series%' then 
                                      array_except(split(landingpagepath,'/'), 
                                                    ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end])[2] end,
                                                          'pria','wanita','men','women','nam','nu','ผชาย','ผหญง',
                                                          case when ipg_subproduct like '%model%' then 
                                                            array_except(split(landingpagepath,'/'), 
                                                              ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end, 
                                                                      case when ipg_subproduct like '%series%' 
                                                                        then array_except(split(landingpagepath,'/'), 
                                                                                            ARRAY [case when ipg_subproduct like '%brand%' then 
                                                                                              split(landingpagepath,'/')[2] end])[2] 
                                                                    end])[2] 
                                                            end,''])[1]) 
                      end) as cat_l1,

                  try(case when ipg_subproduct like '%category%' then
                      try(array_except(split(landingpagepath,'/'),
                            ARRAY [case when ipg_subproduct like 'brand%' then split(landingpagepath,'/')[2] end,
                                    case when ipg_subproduct like '%series%' then 
                                      array_except(split(landingpagepath,'/'), 
                                                    ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end])[2] end,
                                                          'pria','wanita','men','women','nam','nu','ผชาย','ผหญง',
                                                          case when ipg_subproduct like '%model%' then 
                                                            array_except(split(landingpagepath,'/'), 
                                                              ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end, 
                                                                      case when ipg_subproduct like '%series%' 
                                                                        then array_except(split(landingpagepath,'/'), 
                                                                                            ARRAY [case when ipg_subproduct like '%brand%' then 
                                                                                              split(landingpagepath,'/')[2] end])[2] 
                                                                    end])[2] 
                                                            end,''])[2]) 
                      end) as cat_l2,

                 try(case when ipg_subproduct like '%category%' then
                      try(array_except(split(landingpagepath,'/'),
                            ARRAY [case when ipg_subproduct like 'brand%' then split(landingpagepath,'/')[2] end,
                                    case when ipg_subproduct like '%series%' then 
                                      array_except(split(landingpagepath,'/'), 
                                                    ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end])[2] end,
                                                          'pria','wanita','men','women','nam','nu','ผชาย','ผหญง',
                                                          case when ipg_subproduct like '%model%' then 
                                                            array_except(split(landingpagepath,'/'), 
                                                              ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end, 
                                                                      case when ipg_subproduct like '%series%' 
                                                                        then array_except(split(landingpagepath,'/'), 
                                                                                            ARRAY [case when ipg_subproduct like '%brand%' then 
                                                                                              split(landingpagepath,'/')[2] end])[2] 
                                                                    end])[2] 
                                                            end,''])[3]) 
                      end) as cat_l3,

                  try(case when ipg_subproduct like '%category%' then
                      try(array_except(split(landingpagepath,'/'),
                            ARRAY [case when ipg_subproduct like 'brand%' then split(landingpagepath,'/')[2] end,
                                    case when ipg_subproduct like '%series%' then 
                                      array_except(split(landingpagepath,'/'), 
                                                    ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end])[2] end,
                                                          'pria','wanita','men','women','nam','nu','ผชาย','ผหญง',
                                                          case when ipg_subproduct like '%model%' then 
                                                            array_except(split(landingpagepath,'/'), 
                                                              ARRAY [case when ipg_subproduct like '%brand%' then split(landingpagepath,'/')[2] end, 
                                                                      case when ipg_subproduct like '%series%' 
                                                                        then array_except(split(landingpagepath,'/'), 
                                                                                            ARRAY [case when ipg_subproduct like '%brand%' then 
                                                                                              split(landingpagepath,'/')[2] end])[2] 
                                                                    end])[2] 
                                                            end,''])[4]) 
                      end) as cat_l4,                                                                               

                 sum(pageviews) as pageviews,
                 sum(sessions) as sessions
                  FROM "dalake"."ga_bi_acquisition"
                  WHERE ipg_product = 'shop' and year in ({year_list}) and cc = '{cc}' and date between '{min_date}' and '{max_date}' and month in ({month_list})
                    and landingpagepath not in ('/search/','(not set)','/r/p/','translate_c') and website = 'IPRICE'
                  group by 1,2,3,4,5,6,7,8,9,10,11,12
                  order by 13 desc