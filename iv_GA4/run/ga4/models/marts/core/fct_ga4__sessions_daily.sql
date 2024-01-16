
   
      

      
      -- 1. run the merge statement
      

    merge into `set-ga-reporting`.`iv_GA4`.`fct_ga4__sessions_daily` as DBT_INTERNAL_DEST
        using (
          

    

    

    




with session_metrics as (
    select 
        session_key,
        session_partition_key,
        client_key,
        stream_id,
        max(user_id) as user_id, -- user_id can be null at the start and end of a session and still be set in the middle
        min(event_date_dt) as session_partition_date, -- Date of the session partition, does not represent the true session start date which, in GA4, can span multiple days
        min(event_timestamp) as session_partition_min_timestamp,
        countif(event_name = 'page_view') as session_partition_count_page_views,
        sum(event_value_in_usd) as session_partition_sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_partition_max_session_engaged,
        sum(engagement_time_msec) as session_partition_sum_engagement_time_msec,
        min(session_number) as session_number
    from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`
    where session_key is not null
    
            and event_date_dt in (current_date,date_sub(current_date, interval 1 day),date_sub(current_date, interval 2 day),date_sub(current_date, interval 3 day))
    
    group by 1,2,3,4
)

    select * from session_metrics
) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date(DBT_INTERNAL_DEST.session_partition_date) in (
              current_date, date_sub(current_date, interval 1 day), date_sub(current_date, interval 2 day), date_sub(current_date, interval 3 day)
          ) 
        then delete

    when not matched then insert
        (`session_key`, `session_partition_key`, `client_key`, `stream_id`, `user_id`, `session_partition_date`, `session_partition_min_timestamp`, `session_partition_count_page_views`, `session_partition_sum_event_value_in_usd`, `session_partition_max_session_engaged`, `session_partition_sum_engagement_time_msec`, `session_number`)
    values
        (`session_key`, `session_partition_key`, `client_key`, `stream_id`, `user_id`, `session_partition_date`, `session_partition_min_timestamp`, `session_partition_count_page_views`, `session_partition_sum_event_value_in_usd`, `session_partition_max_session_engaged`, `session_partition_sum_engagement_time_msec`, `session_number`)

;

  

    