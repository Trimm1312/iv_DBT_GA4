

  create or replace view `set-ga-reporting`.`iv_GA4`.`stg_ga4__event_user_engagement`
  OPTIONS()
  as -- Event defined as "when the app is in the foreground or webpage is in focus for at least one second."
 
 with user_engagement_with_params as (
   select *
      
      
 from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events`    
 where event_name = 'user_engagement'
)

select * from user_engagement_with_params;

