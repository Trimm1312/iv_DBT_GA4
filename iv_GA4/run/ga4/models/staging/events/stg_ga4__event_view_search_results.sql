

  create or replace view `set-ga-reporting`.`iv_GA4`.`stg_ga4__event_view_search_results`
  OPTIONS()
  as -- reference here: https://support.google.com/analytics/answer/9216061?hl=en 
 
 with event_with_params as (
   select *,
      (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'entrances') as 
    
    entrances
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'search_term') as 
    
    search_term
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'unique_search_term') as 
    
    unique_search_term
    
      
      
 from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`
 where event_name = 'view_search_results'
)

select * from event_with_params;

