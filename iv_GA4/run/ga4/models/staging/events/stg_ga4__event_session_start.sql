

  create or replace view `set-ga-reporting`.`iv_GA4`.`stg_ga4__event_session_start`
  OPTIONS()
  as with session_start_with_params as (
   select *,
      (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'entrances') as 
    
    entrances
    ,
      (select 
        
            value.float_value    
        
    from unnest(event_params) where key = 'value') as 
    
    value
    
      
      
 from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`    
 where event_name = 'session_start'
)

select * from session_start_with_params;

