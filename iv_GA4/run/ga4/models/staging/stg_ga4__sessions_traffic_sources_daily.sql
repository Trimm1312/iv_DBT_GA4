
   
      

      
      -- 1. run the merge statement
      

    merge into `set-ga-reporting`.`iv_GA4`.`stg_ga4__sessions_traffic_sources_daily` as DBT_INTERNAL_DEST
        using (
          

    

    

    




with session_events as (
    select
        client_key
        ,session_partition_key
        ,event_date_dt as session_partition_date
        ,event_timestamp
        ,events.event_source
        ,event_medium
        ,event_campaign
        ,event_content
        ,event_term
        ,source_category
    from `set-ga-reporting`.`iv_GA4`.`stg_ga4__events` events
    left join `set-ga-reporting`.`iv_GA4`.`ga4_source_categories` source_categories on events.event_source = source_categories.source
    where session_partition_key is not null
    and event_name != 'session_start'
    and event_name != 'first_visit'
    
            and event_date_dt in (current_date,date_sub(current_date, interval 1 day),date_sub(current_date, interval 2 day),date_sub(current_date, interval 3 day))
    

   ),
set_default_channel_grouping as (
    select
        *
        ,
case
  -- Direct: Source exactly matches "(direct)" AND Medium is one of ("(not set)", "(none)")
  when (
      event_source is null
        and event_medium is null
    )
    or (
      event_source = '(direct)'
      and (event_medium = '(none)' or event_medium = '(not set)')
    )
    then 'Direct'

  -- Cross-network: Campaign Name contains "cross-network"
  when REGEXP_CONTAINS(event_campaign, r"cross-network")
    then 'Cross-network'

  -- Paid Shopping:
  --   (Source matches a list of shopping sites
  --   OR
  --   Campaign Name matches regex ^(.*(([^a-df-z]|^)shop|shopping).*)$)
  --   AND
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when (
      source_category = 'SOURCE_CATEGORY_SHOPPING'
      or REGEXP_CONTAINS(event_campaign, r"^(.*(([^a-df-z]|^)shop|shopping).*)$")
    )
    and REGEXP_CONTAINS(event_medium,r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Shopping'

  -- Paid Search:
  --   Source matches a list of search sites
  --   AND
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when source_category = 'SOURCE_CATEGORY_SEARCH'
    and REGEXP_CONTAINS(event_medium, r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Search'

  -- Paid Social:
  --   Source matches a regex list of social sites
  --   AND
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when source_category = 'SOURCE_CATEGORY_SOCIAL'
    and REGEXP_CONTAINS(event_medium, r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Social'

  -- Paid Video:
  --   Source matches a list of video sites
  --   AND
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when source_category = 'SOURCE_CATEGORY_VIDEO'
    and REGEXP_CONTAINS(event_medium,r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Video'

  -- Display:
  --   Medium is one of ("display", "banner", "expandable", "interstitial", "cpm")
  when event_medium in ('display', 'banner', 'expandable', 'interstitial', 'cpm')
    then 'Display'

  -- Paid Other:
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when REGEXP_CONTAINS(event_medium, r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Other'

  -- Organic Shopping:
  --   Source matches a list of shopping sites
  --   OR
  --   Campaign name matches regex ^(.*(([^a-df-z]|^)shop|shopping).*)$
  when source_category = 'SOURCE_CATEGORY_SHOPPING'
    or REGEXP_CONTAINS(event_campaign, r"^(.*(([^a-df-z]|^)shop|shopping).*)$")
    then 'Organic Shopping'

  -- Organic Social:
  --   Source matches a regex list of social sites
  --   OR
  --   Medium is one of ("social", "social-network", "social-media", "sm", "social network", "social media")
  when source_category = 'SOURCE_CATEGORY_SOCIAL'
    or event_medium in ("social","social-network","social-media","sm","social network","social media")
    then 'Organic Social'

  -- Organic Video:
  --   Source matches a list of video sites
  --   OR
  --   Medium matches regex ^(.*video.*)$
  when source_category = 'SOURCE_CATEGORY_VIDEO'
    or REGEXP_CONTAINS(event_medium, r"^(.*video.*)$")
    then 'Organic Video'

  -- Organic Search:
  --   Source matches a list of search sites
  --   OR
  --   Medium exactly matches organic
  when source_category = 'SOURCE_CATEGORY_SEARCH' or event_medium = 'organic'
    then 'Organic Search'

  -- Referral:
  --   Medium is one of ("referral", "app", or "link")
  when event_medium in ("referral", "app", "link")
    then 'Referral'

  -- Email:
  --   Source = email|e-mail|e_mail|e mail
  --   OR
  --   Medium = email|e-mail|e_mail|e mail
  when REGEXP_CONTAINS(event_source, r"email|e-mail|e_mail|e mail")
    or REGEXP_CONTAINS(event_medium, r"email|e-mail|e_mail|e mail")
    then 'Email'

  -- Affiliates:
  --   Medium = affiliate
  when event_medium = 'affiliate'
    then 'Affiliates'

  -- Audio:
  --   Medium exactly matches audio
  when event_medium = 'audio'
    then 'Audio'

  -- SMS:
  --   Source exactly matches sms
  --   OR
  --   Medium exactly matches sms
  when event_source = 'sms'
    or event_medium = 'sms'
    then 'SMS'

  -- Mobile Push Notifications:
  --   Medium ends with "push"
  --   OR
  --   Medium contains "mobile" or "notification"
  --   OR
  --   Source exactly matches "firebase"
  when REGEXP_CONTAINS(event_medium, r"push$")
    or REGEXP_CONTAINS(event_medium, r"mobile|notification")
    or event_source = 'firebase'
    then 'Mobile Push Notifications'

  -- Unassigned is the value Analytics uses when there are no other channel rules that match the event data.
  else 'Unassigned'
end

 as default_channel_grouping
    from session_events
),
first_session_source as (
    select
        client_key
        ,session_partition_key
        ,session_partition_date
        ,event_timestamp
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN event_source END) IGNORE NULLS) OVER (session_window), '(direct)') AS session_source
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_medium, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_medium
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(source_category, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_source_category
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_campaign, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_campaign
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_content, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_content
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_term, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_term
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(default_channel_grouping, 'Direct') END) IGNORE NULLS) OVER (session_window), 'Direct') AS session_default_channel_grouping
    from set_default_channel_grouping
    WINDOW session_window AS (PARTITION BY session_partition_key ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
find_non_direct_session_partition_key as (

    select
        *
        ,if(session_source <> '(direct)', session_partition_key, null) as non_direct_session_partition_key --provide the session_partition_key only if source is not direct. Useful for last non-direct attribution modeling
    from first_session_source
)

select
        client_key
        ,session_partition_key
        ,session_partition_date
        ,session_source
        ,session_medium
        ,session_source_category
        ,session_campaign
        ,session_content
        ,session_term
        ,session_default_channel_grouping
        ,non_direct_session_partition_key
        ,min(event_timestamp) as session_partition_timestamp
from find_non_direct_session_partition_key
group by 1,2,3,4,5,6,7,8,9,10,11) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date(DBT_INTERNAL_DEST.session_partition_date) in (
              current_date, date_sub(current_date, interval 1 day), date_sub(current_date, interval 2 day), date_sub(current_date, interval 3 day)
          ) 
        then delete

    when not matched then insert
        (`client_key`, `session_partition_key`, `session_partition_date`, `session_source`, `session_medium`, `session_source_category`, `session_campaign`, `session_content`, `session_term`, `session_default_channel_grouping`, `non_direct_session_partition_key`, `session_partition_timestamp`)
    values
        (`client_key`, `session_partition_key`, `session_partition_date`, `session_source`, `session_medium`, `session_source_category`, `session_campaign`, `session_content`, `session_term`, `session_default_channel_grouping`, `non_direct_session_partition_key`, `session_partition_timestamp`)

;

  

    