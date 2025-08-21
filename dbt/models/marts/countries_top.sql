with exploded as (
  select
    trim(regexp_split_to_table(coalesce(location_country,''), '\s*\|\s*|\s*,\s*|;\s*')) as country
  from public.raw_ctgov_dedup
),
clean as (
  select country
  from exploded
  where country is not null and country <> '' and lower(country) <> 'none'
)
select
  country,
  count(*)::int as n
from clean
group by 1
order by 2 desc
