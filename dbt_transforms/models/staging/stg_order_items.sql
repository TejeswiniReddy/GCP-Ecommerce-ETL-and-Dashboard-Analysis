-- models/staging/stg_order_items.sql

with source as (
    select * from {{ source('retail_staging', 'order_items') }}
),
cleaned as (
    select
        order_item_id,
        order_id,
        product_id,
        cast(quantity   as int64)   as quantity,
        round(cast(unit_price   as numeric), 2) as unit_price,
        round(cast(discount_pct as numeric), 2) as discount_pct,
        round(cast(line_total   as numeric), 2) as line_total,
        round(
            cast(unit_price as numeric)
            * cast(discount_pct as numeric) / 100
            * cast(quantity as int64),
            2
        )                                        as discount_amount,
        _ingested_at
    from source
    where order_item_id is not null
      and order_id      is not null
      and product_id    is not null
      and cast(quantity as int64) > 0
)
select * from cleaned


-- models/staging/stg_returns.sql
-- (separate file in practice; combined here for brevity)
