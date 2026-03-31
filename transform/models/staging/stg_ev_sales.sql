-- transform/models/staging/stg_ev_sales.sql
-- Materialised as a VIEW — no data stored, just a query definition.
-- Reads raw Parquet from S3 bronze layer.
-- Data is in long format: one row per metric (parameter) per region/year/powertrain.

with source as (
    select * from {{ source('bronze', 'ev_sales') }}
),

cleaned as (
    select
        trim(region)                             as region,
        trim(category)                           as category,
        trim(parameter)                          as parameter,  -- e.g. 'EV sales', 'EV stock share'
        trim(mode)                               as mode,       -- e.g. 'Cars', 'Buses'
        trim(powertrain)                         as powertrain, -- e.g. 'BEV', 'PHEV', 'EV'
        cast(year as integer)                    as year,
        trim(unit)                               as unit,       -- e.g. 'Vehicles', 'percent'
        coalesce(cast(value as double), 0)       as value

    from source

    where year is not null
      and region is not null
      and parameter is not null
)

select * from cleaned