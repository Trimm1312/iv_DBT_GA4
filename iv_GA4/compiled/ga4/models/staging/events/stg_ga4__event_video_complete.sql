-- Defined as when the video ends. For embedded YouTube videos that have JS API support enabled. Collected by default via enhanced measurement.
-- More info: https://support.google.com/firebase/answer/9234069?hl=en
 
 with video_complete_with_params as (
   select *,
      (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'video_current_time') as 
    
    video_current_time
    ,
      (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'video_duration') as 
    
    video_duration
    ,
      (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'video_percent') as 
    
    video_percent
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'video_url') as 
    
    video_url
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'video_provider') as 
    
    video_provider
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'vide_title') as 
    
    vide_title
    ,
      (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'visible') as 
    
    visible
    
      
      
 from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`    
 where event_name = 'video_complete'
)

select * from video_complete_with_params