-- transform/models/intermediate/int_ev_enriched.sql
-- Materialised as a local DuckDB TABLE (used as input by mart models).
-- Pivots the long-format data into one row per region/year/powertrain.
-- This makes the mart models much simpler to write and query.

-- Handles the fact that the dataset mixes two granularities:
--   - EV sales / EV stock → broken down by powertrain (BEV, PHEV, FCEV)
--   - EV sales share / EV stock share → only at aggregate EV level

with source as (
    select * from {{ ref('stg_ev_sales') }}
),

cars_only as (
    select * from source
    where mode = 'Cars'
),

-- Part 1: sales and stock by powertrain (BEV, PHEV, FCEV)
sales_by_powertrain as (
    select
        region,
        year,
        powertrain,
        sum(case when parameter = 'EV sales'  then value else 0 end) as ev_sales_units,
        sum(case when parameter = 'EV stock'  then value else 0 end) as ev_stock_units,
        sum(case when parameter = 'EV charging points' then value else 0 end) as charging_points
    from cars_only
    where powertrain in ('BEV', 'PHEV', 'FCEV')
    group by region, year, powertrain
),

-- Part 2: share metrics at aggregate EV level only
share_metrics as (
    select
        region,
        year,
        avg(case when parameter = 'EV sales share' then value else null end) as ev_sales_share_pct,
        avg(case when parameter = 'EV stock share' then value else null end) as ev_stock_share_pct
    from cars_only
    where powertrain = 'EV'
    group by region, year
),

-- Join: attach share metrics to each powertrain row
joined as (
    select
        s.region,
        s.year,
        s.powertrain,
        s.ev_sales_units,
        s.ev_stock_units,
        s.charging_points,
        m.ev_sales_share_pct,
        m.ev_stock_share_pct
    from sales_by_powertrain s
    left join share_metrics m
        on s.region = m.region
        and s.year = m.year
),

-- Add YoY growth on ev_sales_units
with_yoy as (
    select
        *,
        lag(ev_sales_units) over (
            partition by region, powertrain
            order by year
        ) as prior_year_sales
    from joined
),

final as (
    select
        region,
        year,
        powertrain,
        ev_sales_units,
        ev_stock_units,
        charging_points,
        ev_sales_share_pct,
        ev_stock_share_pct,
        case
            when prior_year_sales > 0
            then round(
                (ev_sales_units - prior_year_sales) / prior_year_sales * 100, 2
            )
            else null
        end as yoy_growth_pct
    from with_yoy
)

select * from final