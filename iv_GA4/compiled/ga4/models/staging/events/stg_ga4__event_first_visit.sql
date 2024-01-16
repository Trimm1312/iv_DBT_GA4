-- TODO: Unclear why there are first_visit events firing when the ga_session_number is >1. This might cause confusion.

with first_visit_with_params as (
 select 
    *,
    (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'page_location') as 
    
    landing_page
     
    
      
 from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`    
 where event_name = 'first_visit'
)

select * from first_visit_with_params