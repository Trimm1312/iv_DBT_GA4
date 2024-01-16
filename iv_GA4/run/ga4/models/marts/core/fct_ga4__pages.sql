
   
      

      
      -- 1. run the merge statement
      

    merge into `set-ga-reporting`.`iv_GA4`.`fct_ga4__pages` as DBT_INTERNAL_DEST
        using (
          

    

    

    



with page_view as (
    select
        event_date_dt,
        stream_id,
        page_location,  -- includes query string parameters not listed in query_parameter_exclusions variable
        page_key,
        page_path,
        page_title,  -- would like to move this to dim_ga4__pages but need to think how to handle page_title changing over time
        page_engagement_key,
        count(event_name) as page_views,
        count(distinct client_key ) as distinct_client_keys,
        sum( if(session_number = 1,1,0)) as new_client_keys,
        sum(entrances) as entrances,
from `set-ga-reporting`.`iv_GA4`.`stg_ga4__event_page_view`

        where event_date_dt in (current_date,date_sub(current_date, interval 1 day),date_sub(current_date, interval 2 day),date_sub(current_date, interval 3 day))

    group by 1,2,3,4,5,6,7
), page_engagement as (
    select
        page_view.* except(page_engagement_key),
        sum(page_engagement_time_msec) as total_engagement_time_msec,
        sum( page_engagement_denominator) as avg_engagement_time_denominator
    from `set-ga-reporting`.`iv_GA4`.`stg_ga4__page_engaged_time`
    right join page_view using (page_engagement_key)
    group by 1,2,3,4,5,6,7,8,9,10
), scroll as (
    select
        event_date_dt,
        page_location, 
        page_title,
        count(event_name) as scroll_events
    from `set-ga-reporting`.`iv_GA4`.`stg_ga4__event_scroll`
    
            where event_date_dt in (current_date,date_sub(current_date, interval 1 day),date_sub(current_date, interval 2 day),date_sub(current_date, interval 3 day))
    
    group by 1,2,3
)

select
    page_engagement.* except (page_key),
    ifnull(scroll.scroll_events, 0) as scroll_events
from page_engagement
left join scroll using (event_date_dt, page_location, page_title)
) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date(DBT_INTERNAL_DEST.event_date_dt) in (
              current_date, date_sub(current_date, interval 1 day), date_sub(current_date, interval 2 day), date_sub(current_date, interval 3 day)
          ) 
        then delete

    when not matched then insert
        (`event_date_dt`, `stream_id`, `page_location`, `page_path`, `page_title`, `page_views`, `distinct_client_keys`, `new_client_keys`, `entrances`, `total_engagement_time_msec`, `avg_engagement_time_denominator`, `scroll_events`)
    values
        (`event_date_dt`, `stream_id`, `page_location`, `page_path`, `page_title`, `page_views`, `distinct_client_keys`, `new_client_keys`, `entrances`, `total_engagement_time_msec`, `avg_engagement_time_denominator`, `scroll_events`)

;

  

    