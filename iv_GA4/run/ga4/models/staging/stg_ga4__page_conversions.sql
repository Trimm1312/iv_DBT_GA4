

  create or replace view `set-ga-reporting`.`iv_GA4`.`stg_ga4__page_conversions`
  OPTIONS()
  as 

select 
    page_key
    
    , countif(event_name = 'view_item') as view_item_count
    
    , countif(event_name = 'add_to_cart') as add_to_cart_count
    
    , countif(event_name = 'begin_checkout') as begin_checkout_count
    
    , countif(event_name = 'complete_registration') as complete_registration_count
    
    , countif(event_name = 'add_payment_info') as add_payment_info_count
    
    , countif(event_name = 'subscribe') as subscribe_count
    
    , countif(event_name = 'purchase') as purchase_count
    
from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`
group by 1;

