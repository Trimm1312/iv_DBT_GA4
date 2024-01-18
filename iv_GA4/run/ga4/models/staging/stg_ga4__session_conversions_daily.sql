
   
      

      
      -- 1. run the merge statement
      

    merge into `set-ga-reporting`.`iv_GA4`.`stg_ga4__session_conversions_daily` as DBT_INTERNAL_DEST
        using (
          

    

    

    





with event_counts as (
    select 
        session_key,
        session_partition_key,
        min(event_date_dt) as session_partition_date -- The date of this session partition
        
        , countif(event_name = 'view_item') as view_item_count
        
        , countif(event_name = 'add_to_cart') as add_to_cart_count
        
        , countif(event_name = 'begin_checkout') as begin_checkout_count
        
        , countif(event_name = 'complete_registration') as complete_registration_count
        
        , countif(event_name = 'add_payment_info') as add_payment_info_count
        
        , countif(event_name = 'subscribe') as subscribe_count
        
        , countif(event_name = 'purchase') as purchase_count
        
    from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`
    where 1=1
    
            and event_date_dt in (current_date,date_sub(current_date, interval 1 day),date_sub(current_date, interval 2 day),date_sub(current_date, interval 3 day))
    
    group by 1,2
)

select * from event_counts) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date(DBT_INTERNAL_DEST.session_partition_date) in (
              current_date, date_sub(current_date, interval 1 day), date_sub(current_date, interval 2 day), date_sub(current_date, interval 3 day)
          ) 
        then delete

    when not matched then insert
        (`session_key`, `session_partition_key`, `session_partition_date`, `purchase_count`)
    values
        (`session_key`, `session_partition_key`, `session_partition_date`, `purchase_count`)

;

  

    