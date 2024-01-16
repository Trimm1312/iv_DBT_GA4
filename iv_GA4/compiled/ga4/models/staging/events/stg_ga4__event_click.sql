-- reference here: https://support.google.com/analytics/answer/9216061?hl=en 
 
 with click_with_params as (
   select *,
      (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'entrances') as 
    
    entrances
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'outbound') as 
    
    outbound
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
        
    from unnest(event_params) where key = 'link_url') as 
    
    link_url
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'click_element') as 
    
    click_element
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'link_id') as 
    
    link_id
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'click_region') as 
    
    click_region
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'click_tag_name') as 
    
    click_tag_name
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'click_url') as 
    
    click_url
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'file_name') as 
    
    file_name
    
      
      
 from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`
 where event_name = 'click'
)

select * from click_with_params