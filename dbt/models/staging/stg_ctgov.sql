with src as (
  select *
  from public.raw_ctgov_dedup
),

cleaned as (
  select
    nct_id,
    lower(coalesce("condition", '')) as condition,
    brief_title,
    overall_status,
    study_type,
    case
      when start_date ~ '^\d{4}-\d{2}-\d{2}$' then start_date::date
      else null
    end as start_date,
    location_country,
    case
      when last_update_post_date ~ '^\d{4}-\d{2}-\d{2}$' then last_update_post_date::date
      else null
    end as last_update_post_date
  from src
)

select *
from cleaned
