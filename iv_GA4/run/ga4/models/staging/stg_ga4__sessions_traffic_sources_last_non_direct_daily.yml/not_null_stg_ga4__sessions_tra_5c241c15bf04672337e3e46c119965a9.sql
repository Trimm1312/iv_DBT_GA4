select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select last_non_direct_default_channel_grouping
from `set-ga-reporting`.`iv_GA4`.`stg_ga4__sessions_traffic_sources_last_non_direct_daily`
where last_non_direct_default_channel_grouping is null



      
    ) dbt_internal_test