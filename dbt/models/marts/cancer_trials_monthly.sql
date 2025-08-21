with base as (
  select * from {{ ref('stg_ctgov') }}
)
select
  date_trunc('month', start_date)::date as month,
  count(*) as trials_started
from base
where start_date is not null
  and condition ilike '%cancer%'
group by 1
order by 1
