
    
    

with dbt_test__target as (

  select client_key as unique_field
  from `set-ga-reporting`.`iv_GA4`.`stg_ga4__client_key_first_last_events`
  where client_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


