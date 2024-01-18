-- Stay mindful of performance/cost when using this model. Making this model partitioned on date is not possible because there's no way to create a single record per session AND partition on date. 

select
    client_key,
    session_key,
    stream_id,
    max(user_id) as user_id,
    min(session_partition_min_timestamp) as session_start_timestamp,
    min(session_partition_date) as session_start_date,
    sum(session_partition_count_page_views) as count_pageviews,
    sum(session_partition_sum_event_value_in_usd) as sum_event_value_in_usd,
    max(session_partition_max_session_engaged) as is_session_engaged,
    sum(session_partition_sum_engagement_time_msec) as sum_engaged_time_msec,
    min(session_number) as session_number
    
        
            , sum(view_item_count) as count_view_item
        
            , sum(add_to_cart_count) as count_add_to_cart
        
            , sum(begin_checkout_count) as count_begin_checkout
        
            , sum(complete_registration_count) as count_complete_registration
        
            , sum(add_payment_info_count) as count_add_payment_info
        
            , sum(subscribe_count) as count_subscribe
        
            , sum(purchase_count) as count_purchase
        
    
from `set-ga-reporting`.`iv_GA4`.`fct_ga4__sessions_daily`
group by 1,2,3