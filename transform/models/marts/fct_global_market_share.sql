-- transform/models/marts/fct_global_market_share.sql
-- Written to s3://your-bucket/gold/fct_global_market_share.parquet
-- Athena queries this file directly — no loading required.
-- EV sales share (%) by region for a given year.
-- Powers Dashboard Tile 1 (categorical bar chart).
{{ config(
    materialized='external',
    location="s3://" ~ env_var('S3_BUCKET_NAME') ~ "/gold/fct_global_market_share/data.parquet",
    format='parquet'
) }}

with enriched as (
    select * from {{ ref('int_ev_enriched') }}
)

select
    year,
    region,
    powertrain,
    avg(ev_sales_share_pct)    as avg_sales_share_pct,
    sum(ev_sales_units)        as total_ev_sales,
    avg(ev_stock_share_pct)    as avg_stock_share_pct,
    sum(charging_points)       as total_charging_points,

    -- Rank regions by sales share within each year
    row_number() over (
        partition by year
        order by avg(ev_sales_share_pct) desc
    ) as region_rank

from enriched
where ev_sales_units > 0
group by year, region, powertrain
order by year, avg_sales_share_pct desc