with base as (
  select
    lower(coalesce(overall_status,'unknown')) as overall_status
  from public.raw_ctgov_dedup
)
select
  overall_status,
  count(*)::int as n
from base
group by 1
order by 2 desc
