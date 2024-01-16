-- reference here: https://support.google.com/analytics/answer/9216061?hl=en&ref_topic=9756175
 
 with event_with_params as (
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
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'file_extension') as 
    
    file_extension
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'file_name') as 
    
    file_name
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'link_classes') as 
    
    link_classes
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'link_domain') as 
    
    link_domain
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'link_id') as 
    
    link_id
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'link_text') as 
    
    link_text
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'link_url') as 
    
    link_url
    
      
      
 from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`    
 where event_name = 'file_download'
)

select * from event_with_params