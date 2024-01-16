with scroll_with_params as (
   select *,
      (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'percent_scrolled') as 
    
    percent_scrolled
    
      
      
 from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`    
 where event_name = 'scroll'
)

select * from scroll_with_params