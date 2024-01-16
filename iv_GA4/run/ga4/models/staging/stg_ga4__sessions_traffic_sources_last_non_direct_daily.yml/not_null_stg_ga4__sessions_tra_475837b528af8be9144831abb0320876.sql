select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select session_source
from `set-ga-reporting`.`iv_GA4`.`stg_ga4__sessions_traffic_sources_last_non_direct_daily`
where session_source is null



      
    ) dbt_internal_test