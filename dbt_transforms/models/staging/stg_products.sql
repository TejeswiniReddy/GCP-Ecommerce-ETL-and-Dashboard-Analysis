-- models/staging/stg_products.sql

with source as (
    select * from {{ source('retail_staging', 'products') }}
),
cleaned as (
    select
        product_id,
        product_name,
        category,
        brand,
        sku,
        round(cast(unit_price as numeric), 2)  as unit_price,
        round(cast(unit_cost  as numeric), 2)  as unit_cost,
        round(
            (cast(unit_price as numeric) - cast(unit_cost as numeric))
            / nullif(cast(unit_price as numeric), 0) * 100,
            2
        )                                       as gross_margin_pct,
        cast(is_active as bool)                as is_active,
        cast(created_at as timestamp)           as created_at,
        _ingested_at
    from source
    where product_id is not null
      and cast(unit_price as numeric) > 0
)
select * from cleaned
