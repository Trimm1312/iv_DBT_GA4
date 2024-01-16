select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select page_location
from `set-ga-reporting`.`iv_GA4`.`stg_ga4__event_page_view`
where page_location is null



      
    ) dbt_internal_test