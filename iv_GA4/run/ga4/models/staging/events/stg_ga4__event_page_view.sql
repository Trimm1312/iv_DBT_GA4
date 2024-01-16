

  create or replace view `set-ga-reporting`.`iv_GA4`.`stg_ga4__event_page_view`
  OPTIONS()
  as with page_view_with_params as (
   select * except(page_engagement_key),
      (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'entrances') as 
    
    entrances
    ,
      (select 
        
            value.float_value    
        
    from unnest(event_params) where key = 'value') as 
    
    value
    ,
      case when split(split(page_location,'/')[safe_ordinal(4)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(4)],'?')[safe_ordinal(1)]) end as pagepath_level_1,
      case when split(split(page_location,'/')[safe_ordinal(5)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(5)],'?')[safe_ordinal(1)]) end as pagepath_level_2,
      case when split(split(page_location,'/')[safe_ordinal(6)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(6)],'?')[safe_ordinal(1)]) end as pagepath_level_3,
      case when split(split(page_location,'/')[safe_ordinal(7)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(7)],'?')[safe_ordinal(1)]) end as pagepath_level_4,
      to_base64(md5(concat(session_key, page_location))) as page_engagement_key
      
      
 from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`    
 where event_name = 'page_view'
)
select *
from page_view_with_params;

