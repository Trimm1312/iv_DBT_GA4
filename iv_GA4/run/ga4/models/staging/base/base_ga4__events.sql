
   
      

      
      -- 1. run the merge statement
      

    merge into `set-ga-reporting`.`iv_GA4`.`base_ga4__events` as DBT_INTERNAL_DEST
        using (
          

    

    

    




with source as (
    select 
        
    parse_date('%Y%m%d',event_date) as event_date_dt
    , event_timestamp
    , event_name
    , event_params
    , event_previous_timestamp
    , event_value_in_usd
    , event_bundle_sequence_id
    , event_server_timestamp_offset
    , user_id
    , user_pseudo_id
    , privacy_info
    , user_properties
    , user_first_touch_timestamp
    , user_ltv
    , device
    , geo
    , app_info
    , traffic_source
    , stream_id
    , platform
    , ecommerce.total_item_quantity
    , ecommerce.purchase_revenue_in_usd
    , ecommerce.purchase_revenue
    , ecommerce.refund_value_in_usd
    , ecommerce.refund_value
    , ecommerce.shipping_value_in_usd
    , ecommerce.shipping_value
    , ecommerce.tax_value_in_usd
    , ecommerce.tax_value
    , ecommerce.unique_items
    , ecommerce.transaction_id
    , items

        from `set-ga-reporting`.`analytics_270752382`.`events_*`
        where cast( replace(_table_suffix, 'intraday_', '') as int64) >= 20231201
    
        and parse_date('%Y%m%d', left( replace(_table_suffix, 'intraday_', ''), 8)) in (current_date,date_sub(current_date, interval 1 day),date_sub(current_date, interval 2 day),date_sub(current_date, interval 3 day))
    
),
renamed as (
    select 
        
    event_date_dt
    , event_timestamp
    , lower(replace(trim(event_name), " ", "_")) as event_name -- Clean up all event names to be snake cased
    , event_params
    , event_previous_timestamp
    , event_value_in_usd
    , event_bundle_sequence_id
    , event_server_timestamp_offset
    , user_id
    , user_pseudo_id
    , privacy_info.analytics_storage as privacy_info_analytics_storage
    , privacy_info.ads_storage as privacy_info_ads_storage
    , privacy_info.uses_transient_token as privacy_info_uses_transient_token
    , user_properties
    , user_first_touch_timestamp
    , user_ltv.revenue as user_ltv_revenue
    , user_ltv.currency as user_ltv_currency
    , device.category as device_category
    , device.mobile_brand_name as device_mobile_brand_name
    , device.mobile_model_name as device_mobile_model_name
    , device.mobile_marketing_name as device_mobile_marketing_name
    , device.mobile_os_hardware_model as device_mobile_os_hardware_model
    , device.operating_system as device_operating_system
    , device.operating_system_version as device_operating_system_version
    , device.vendor_id as device_vendor_id
    , device.advertising_id as device_advertising_id
    , device.language as device_language
    , device.is_limited_ad_tracking as device_is_limited_ad_tracking
    , device.time_zone_offset_seconds as device_time_zone_offset_seconds
    , device.browser as device_browser
    , device.browser_version as device_browser_version
    , device.web_info.browser as device_web_info_browser
    , device.web_info.browser_version as device_web_info_browser_version
    , device.web_info.hostname as device_web_info_hostname
    , geo.continent as geo_continent
    , geo.country as geo_country
    , geo.region as geo_region
    , geo.city as geo_city
    , geo.sub_continent as geo_sub_continent
    , geo.metro as geo_metro
    , app_info.id as app_info_id
    , app_info.version as app_info_version
    , app_info.install_store as app_info_install_store
    , app_info.firebase_app_id as app_info_firebase_app_id
    , app_info.install_source as app_info_install_source
    , traffic_source.name as user_campaign
    , traffic_source.medium as user_medium
    , traffic_source.source as user_source
    , stream_id
    , platform
    , struct(
        total_item_quantity
        , purchase_revenue_in_usd
        , purchase_revenue
        , refund_value_in_usd
        , refund_value
        , shipping_value_in_usd
        , shipping_value
        , tax_value_in_usd
        , tax_value
        , unique_items
        , transaction_id        
    ) as ecommerce
    , (select 
        array_agg(struct(
            unnested_items.item_id
            , unnested_items.item_name
            , unnested_items.item_brand
            , unnested_items.item_variant
            , unnested_items.item_category
            , unnested_items.item_category2
            , unnested_items.item_category3
            , unnested_items.item_category4
            , unnested_items.item_category5
            , unnested_items.price_in_usd
            , unnested_items.price
            , unnested_items.quantity
            , unnested_items.item_revenue_in_usd
            , unnested_items.item_revenue
            , unnested_items.item_refund_in_usd
            , unnested_items.item_refund
            , unnested_items.coupon
            , unnested_items.affiliation
            , unnested_items.location_id
            , unnested_items.item_list_id
            , unnested_items.item_list_name
            , unnested_items.item_list_index
            , unnested_items.promotion_id
            , unnested_items.promotion_name
            , unnested_items.creative_name
            , unnested_items.creative_slot
            , unnested_items.item_params
        )) from unnest(items) as unnested_items 
    ) items
    , (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'ga_session_id') as 
    
    session_id
    
    , (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'page_location') as 
    
    page_location
    
    , (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'ga_session_number') as 
    
    session_number
    
    , COALESCE(
        (SELECT value.int_value FROM unnest(event_params) WHERE key = "session_engaged"),
        (CASE WHEN (SELECT value.string_value FROM unnest(event_params) WHERE key = "session_engaged") = "1" THEN 1 END)
    ) as session_engaged
    , (select 
        
            value.int_value    
        
    from unnest(event_params) where key = 'engagement_time_msec') as 
    
    engagement_time_msec
    
    , (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'page_title') as 
    
    page_title
    
    , (select 
        
            value.string_value    
        
    from unnest(event_params) where key = 'page_referrer') as 
    
    page_referrer
    
    , (select 
        
            lower(value.string_value)   
        
    from unnest(event_params) where key = 'source') as 
    
    event_source
    
    , (select 
        
            lower(value.string_value)   
        
    from unnest(event_params) where key = 'medium') as 
    
    event_medium
    
    , (select 
        
            lower(value.string_value)   
        
    from unnest(event_params) where key = 'campaign') as 
    
    event_campaign
    
    , (select 
        
            lower(value.string_value)   
        
    from unnest(event_params) where key = 'content') as 
    
    event_content
    
    , (select 
        
            lower(value.string_value)   
        
    from unnest(event_params) where key = 'term') as 
    
    event_term
    
    , CASE 
        WHEN event_name = 'page_view' THEN 1
        ELSE 0
    END AS is_page_view
    , CASE 
        WHEN event_name = 'purchase' THEN 1
        ELSE 0
    END AS is_purchase

    from source
)

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id,user_pseudo_id, session_id, event_name, event_timestamp, to_json_string(ARRAY(SELECT params FROM UNNEST(event_params) AS params ORDER BY key))) = 1) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date(DBT_INTERNAL_DEST.event_date_dt) in (
              current_date, date_sub(current_date, interval 1 day), date_sub(current_date, interval 2 day), date_sub(current_date, interval 3 day)
          ) 
        then delete

    when not matched then insert
        (`event_date_dt`, `event_timestamp`, `event_name`, `event_params`, `event_previous_timestamp`, `event_value_in_usd`, `event_bundle_sequence_id`, `event_server_timestamp_offset`, `user_id`, `user_pseudo_id`, `privacy_info_analytics_storage`, `privacy_info_ads_storage`, `privacy_info_uses_transient_token`, `user_properties`, `user_first_touch_timestamp`, `user_ltv_revenue`, `user_ltv_currency`, `device_category`, `device_mobile_brand_name`, `device_mobile_model_name`, `device_mobile_marketing_name`, `device_mobile_os_hardware_model`, `device_operating_system`, `device_operating_system_version`, `device_vendor_id`, `device_advertising_id`, `device_language`, `device_is_limited_ad_tracking`, `device_time_zone_offset_seconds`, `device_browser`, `device_browser_version`, `device_web_info_browser`, `device_web_info_browser_version`, `device_web_info_hostname`, `geo_continent`, `geo_country`, `geo_region`, `geo_city`, `geo_sub_continent`, `geo_metro`, `app_info_id`, `app_info_version`, `app_info_install_store`, `app_info_firebase_app_id`, `app_info_install_source`, `user_campaign`, `user_medium`, `user_source`, `stream_id`, `platform`, `ecommerce`, `items`, `session_id`, `page_location`, `session_number`, `session_engaged`, `engagement_time_msec`, `page_title`, `page_referrer`, `event_source`, `event_medium`, `event_campaign`, `event_content`, `event_term`, `is_page_view`, `is_purchase`)
    values
        (`event_date_dt`, `event_timestamp`, `event_name`, `event_params`, `event_previous_timestamp`, `event_value_in_usd`, `event_bundle_sequence_id`, `event_server_timestamp_offset`, `user_id`, `user_pseudo_id`, `privacy_info_analytics_storage`, `privacy_info_ads_storage`, `privacy_info_uses_transient_token`, `user_properties`, `user_first_touch_timestamp`, `user_ltv_revenue`, `user_ltv_currency`, `device_category`, `device_mobile_brand_name`, `device_mobile_model_name`, `device_mobile_marketing_name`, `device_mobile_os_hardware_model`, `device_operating_system`, `device_operating_system_version`, `device_vendor_id`, `device_advertising_id`, `device_language`, `device_is_limited_ad_tracking`, `device_time_zone_offset_seconds`, `device_browser`, `device_browser_version`, `device_web_info_browser`, `device_web_info_browser_version`, `device_web_info_hostname`, `geo_continent`, `geo_country`, `geo_region`, `geo_city`, `geo_sub_continent`, `geo_metro`, `app_info_id`, `app_info_version`, `app_info_install_store`, `app_info_firebase_app_id`, `app_info_install_source`, `user_campaign`, `user_medium`, `user_source`, `stream_id`, `platform`, `ecommerce`, `items`, `session_id`, `page_location`, `session_number`, `session_engaged`, `engagement_time_msec`, `page_title`, `page_referrer`, `event_source`, `event_medium`, `event_campaign`, `event_content`, `event_term`, `is_page_view`, `is_purchase`)

;

  

    