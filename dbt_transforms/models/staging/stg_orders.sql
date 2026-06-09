-- models/staging/stg_orders.sql
-- Silver layer: typed, cleaned orders from BigQuery staging dataset.
-- Filters out cancelled orders and adds derived date dimensions.

with source as (
    select * from {{ source('retail_staging', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        cast(order_date as date)                       as order_date,
        cast(order_timestamp as timestamp)             as order_timestamp,
        status,
        payment_method,
        channel                                        as acquisition_channel,
        round(cast(subtotal as numeric), 2)            as subtotal,
        round(cast(shipping_cost as numeric), 2)       as shipping_cost,
        round(cast(tax as numeric), 2)                 as tax,
        round(cast(order_total as numeric), 2)         as order_total,
        state,
        city,

        -- derived date dimensions
        extract(year  from cast(order_date as date))   as order_year,
        extract(month from cast(order_date as date))   as order_month,
        extract(week  from cast(order_date as date))   as order_week,
        format_date('%Y-%m', cast(order_date as date)) as order_year_month,
        format_date('%A',    cast(order_date as date)) as order_day_of_week,

        -- flags
        case when status = 'delivered' then true else false end as is_completed,
        case when status = 'returned'  then true else false end as is_returned,
        case when shipping_cost = 0    then true else false end as is_free_shipping,

        _ingested_at

    from source
    where order_id is not null
      and customer_id is not null
      and cast(order_total as numeric) >= 0
)

select * from cleaned
