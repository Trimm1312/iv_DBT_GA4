-- Mart for dimensions related to user devices (based on client_key)

with include_first_last_events as (
    select 
        *
    from `set-ga-reporting`.`iv_GA4`.`stg_ga4__client_key_first_last_events`
),
include_first_last_page_views as (
    select 
        include_first_last_events.*,
        first_last_page_views.first_page_location,
        first_last_page_views.first_page_hostname,
        first_last_page_views.first_page_referrer,
        first_last_page_views.last_page_location,
        first_last_page_views.last_page_hostname,
        first_last_page_views.last_page_referrer
    from include_first_last_events 
    left join `set-ga-reporting`.`iv_GA4`.`stg_ga4__client_key_first_last_pageviews` as first_last_page_views using (client_key)
),
include_user_properties as (
    

select * from include_first_last_page_views



)

select * from include_user_properties