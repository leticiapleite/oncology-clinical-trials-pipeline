with base as (
  select
    case
      when start_date ~ '^\d{4}-\d{2}-\d{2}$' then start_date::date
      else null
    end as start_date,
    lower(coalesce(overall_status,'unknown')) as overall_status
  from public.raw_ctgov_dedup
),
filtered as (
  select *
  from base
  where start_date is not null
)
select
  date_trunc('month', start_date)::date as month,
  overall_status,
  count(*)::int as trials_started
from filtered
group by 1, 2
order by 1, 2
