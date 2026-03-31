-- transform/models/marts/fct_ev_adoption_trends.sql
-- Written to s3://your-bucket/gold/fct_ev_adoption_trends.parquet
-- EV sales and sales share over time by region.
-- Powers Dashboard Tile 2 (temporal line chart).

{{ config(
    materialized='external',
    location="s3://" ~ env_var('S3_BUCKET_NAME') ~ "/gold/fct_ev_adoption_trends/data.parquet",
    format='parquet'
) }}

with enriched as (
    select * from {{ ref('int_ev_enriched') }}
)

select
    year,
    region,
    powertrain,
    sum(ev_sales_units)        as total_ev_sales,
    avg(ev_sales_share_pct)    as avg_sales_share_pct,
    avg(yoy_growth_pct)        as avg_yoy_growth_pct
from enriched
where ev_sales_units > 0
group by year, region, powertrain
order by year, region