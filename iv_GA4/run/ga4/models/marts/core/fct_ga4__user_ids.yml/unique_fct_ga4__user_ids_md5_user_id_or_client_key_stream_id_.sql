select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with dbt_test__target as (

  select md5(user_id_or_client_key || stream_id) as unique_field
  from `set-ga-reporting`.`iv_GA4`.`fct_ga4__user_ids`
  where md5(user_id_or_client_key || stream_id) is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



      
    ) dbt_internal_test