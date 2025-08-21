# Oncology ClinicalTrials Explorer
# --------------------------------------------------------------------
# Reads from:
# - public.raw_ctgov_dedup (base table)
# Recomputes aggregates on the fly using SQL (monthly trend, status, countries)
#
# Filters:
# - Month range
# - Overall status (multi)
# - Country (multi)    -> explodes location_country server-side
# - Study type (multi)
# - Text search (title/conditions)

from datetime import date, timedelta
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, text, bindparam
import streamlit as st

ENGINE_URL = "postgresql+psycopg2://analytics:analytics@warehouse:5432/healthcare"
engine = create_engine(ENGINE_URL, pool_pre_ping=True)

st.set_page_config(page_title="Oncology ClinicalTrials – Explorer", layout="wide")
st.title("Oncology ClinicalTrials — Explorer")

@st.cache_data(ttl=600)
def distinct_values(sql: str) -> list[str]:
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)
    return [x for x in df.iloc[:, 0].dropna().astype(str).tolist() if x.strip()]

@st.cache_data(ttl=600)
def load_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)

def as_array_param(values: Iterable[str]) -> list[str]:
    # normalize list for SQL ARRAY binding
    return [v for v in (values or []) if v and str(v).strip()]

# ----------------------------
# Sidebar — global filters
# ----------------------------
with st.sidebar:
    st.header("Filters")

    # Month window
    end_m = date.today().replace(day=1)
    start_m = (end_m - timedelta(days=365 * 3)).replace(day=1)  # last ~36 months
    start_m = st.date_input("Start (month)", start_m).replace(day=1)
    end_m = st.date_input("End (month)", end_m).replace(day=1)

    # Distincts (from raw table)
    statuses = distinct_values("""
        select distinct lower(coalesce(overall_status,'unknown')) as s
        from public.raw_ctgov_dedup
        where coalesce(overall_status,'') <> ''
        order by 1
    """)
    # Some registries don’t fill studyType consistently; handle nulls
    study_types = distinct_values("""
        select distinct coalesce(study_type,'Unknown') as t
        from public.raw_ctgov_dedup
        order by 1
    """)

    sel_status = st.multiselect("Overall status", statuses, default=statuses)
    sel_types = st.multiselect("Study type", study_types, default=study_types)
    # Country list from explode of location_country (top 50 by frequency)
    countries = distinct_values("""
        with exploded as (
          select trim(regexp_split_to_table(coalesce(location_country,''), '\\s*\\|\\s*|\\s*,\\s*|;\\s*')) as c
          from public.raw_ctgov_dedup
        )
        select c from exploded where c is not null and c <> '' group by 1 order by count(*) desc limit 50
    """)
    sel_countries = st.multiselect("Countries (top 50)", countries)

    search = st.text_input("Search (title or condition)", placeholder="e.g., breast, immunotherapy...")

    st.caption("Source: ClinicalTrials.gov (API v2). Transformations computed on the fly. "
               "Dates may be planned rather than actual recruitment start.")

# ----------------------------------------
# Build WHERE clause fragments + parameters
# ----------------------------------------
where_clauses = []
params: dict = {}

# Month filter — we keep start_date as text, but cast when possible
where_clauses.append("""
  case when start_date ~ '^\\d{4}-\\d{2}-\\d{2}$' then start_date::date end between :start_m and :end_m
""")
params["start_m"] = start_m
params["end_m"] = end_m

# Status filter
if sel_status:
    where_clauses.append("lower(coalesce(overall_status,'unknown')) = any(:statuses)")
    params["statuses"] = as_array_param(sel_status)

# Study type filter
if sel_types:
    where_clauses.append("coalesce(study_type,'Unknown') = any(:types)")
    params["types"] = as_array_param(sel_types)

# Search in title/condition
if search and search.strip():
    where_clauses.append("(lower(coalesce(brief_title,'')) like :q or lower(coalesce(condition,'')) like :q)")
    params["q"] = f"%{search.strip().lower()}%"

# Country filter — only if user selected countries
country_filter_sql = ""
if sel_countries:
    country_filter_sql = """
      and exists (
        select 1
        from regexp_split_to_table(coalesce(t.location_country,''), '\\s*\\|\\s*|\\s*,\\s*|;\\s*') as c(country)
        where trim(c.country) <> '' and trim(c.country) = any(:countries)
      )
    """
    params["countries"] = as_array_param(sel_countries)

# Final WHERE
WHERE = " and ".join([f"({c})" for c in where_clauses]) if where_clauses else "true"

# ----------------------
# Tabs
# ----------------------
tab1, tab2, tab3, tab4 = st.tabs(["📈 Monthly", "🧪 Status", "🌍 Countries", "🗃️ Table"])

# ----------------------
# Monthly trend (filtered)
# ----------------------
with tab1:
    st.subheader("Trials started per month (filtered)")
    sql_month = f"""
      with base as (
        select *
        from public.raw_ctgov_dedup t
        where {WHERE}
        {country_filter_sql}
      ),
      cleaned as (
        select case when start_date ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' then start_date::date end as start_date
        from base
      )
      select date_trunc('month', start_date)::date as month, count(*)::int as trials_started
      from cleaned
      where start_date is not null
      group by 1
      order by 1
    """
    dfm = load_df(sql_month, params)
    if dfm.empty:
        st.info("No data for the selected filters.")
    else:
        st.line_chart(dfm.set_index("month")["trials_started"])
        st.dataframe(dfm.tail(24), use_container_width=True)

# ----------------------
# Status distribution
# ----------------------
with tab2:
    st.subheader("Status distribution (filtered)")
    sql_status = f"""
      select lower(coalesce(overall_status,'unknown')) as overall_status,
             count(*)::int as n
      from public.raw_ctgov_dedup t
      where {WHERE}
      {country_filter_sql}
      group by 1
      order by 2 desc
    """
    dfs = load_df(sql_status, params)
    if dfs.empty:
        st.info("No data for the selected filters.")
    else:
        st.bar_chart(dfs.set_index("overall_status")["n"])
        st.dataframe(dfs, use_container_width=True)

    st.subheader("Starts by month × status (filtered)")
    sql_ms = f"""
      with base as (
        select
          case when start_date ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' then start_date::date end as start_date,
          lower(coalesce(overall_status,'unknown')) as overall_status
        from public.raw_ctgov_dedup t
        where {WHERE}
        {country_filter_sql}
      )
      select date_trunc('month', start_date)::date as month,
             overall_status,
             count(*)::int as trials_started
      from base
      where start_date is not null
      group by 1, 2
      order by 1, 2
    """
    dfms = load_df(sql_ms, params)
    if not dfms.empty:
        piv = (
            dfms.pivot_table(index="month", columns="overall_status",
                             values="trials_started", aggfunc="sum")
            .fillna(0).sort_index()
        )
        st.area_chart(piv)
        st.dataframe(dfms, use_container_width=True)
    else:
        st.info("No data for the selected filters.")

# ----------------------
# Countries (explode)
# ----------------------
with tab3:
    st.subheader("Top countries by presence (filtered)")
    sql_c = f"""
      with base as (
        select *
        from public.raw_ctgov_dedup t
        where {WHERE}
        {country_filter_sql}
      ),
      exploded as (
        select trim(regexp_split_to_table(coalesce(location_country,''), '\\s*\\|\\s*|\\s*,\\s*|;\\s*')) as country
        from base
      )
      select country, count(*)::int as n
      from exploded
      where country is not null and country <> ''
      group by 1
      order by 2 desc
      limit 20
    """
    dfc = load_df(sql_c, params)
    if dfc.empty:
        st.info("No country information for the selected filters.")
    else:
        st.bar_chart(dfc.set_index("country")["n"])
        st.dataframe(dfc, use_container_width=True)

# ----------------------
# Recent trials table + CSV
# ----------------------
with tab4:
    st.subheader("Filtered trials (sample)")
    sql_tbl = f"""
      select
        nct_id,
        brief_title,
        lower(coalesce(overall_status,'unknown')) as overall_status,
        coalesce(study_type,'Unknown') as study_type,
        start_date,
        location_country
      from public.raw_ctgov_dedup t
      where {WHERE}
      {country_filter_sql}
      order by coalesce(start_date, last_update_post_date) desc nulls last
      limit 500
    """
    dft = load_df(sql_tbl, params)
    st.dataframe(dft, use_container_width=True)

    if not dft.empty:
        st.download_button(
            "Download CSV",
            dft.to_csv(index=False).encode("utf-8"),
            file_name="clinicaltrials_filtered.csv",
            mime="text/csv",
        )
