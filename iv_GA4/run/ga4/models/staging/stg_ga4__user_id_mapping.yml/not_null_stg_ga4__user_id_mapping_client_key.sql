select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select client_key
from `set-ga-reporting`.`iv_GA4`.`stg_ga4__user_id_mapping`
where client_key is null



      
    ) dbt_internal_test