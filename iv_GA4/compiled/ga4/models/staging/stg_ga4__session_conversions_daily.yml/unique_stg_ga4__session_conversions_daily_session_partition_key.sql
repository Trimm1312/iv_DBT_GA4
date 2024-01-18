
    
    

with dbt_test__target as (

  select session_partition_key as unique_field
  from `set-ga-reporting`.`iv_GA4`.`stg_ga4__session_conversions_daily`
  where session_partition_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


