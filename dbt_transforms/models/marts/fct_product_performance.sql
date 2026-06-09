-- models/marts/fct_product_performance.sql
-- Gold layer: product-level sales, margin, return rate, and attach rate metrics.
-- Powers the Product Insights dashboard page.

with items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

orders as (
    select order_id, order_date, order_year_month, is_completed
    from {{ ref('stg_orders') }}
),

returns as (
    select
        product_id,
        count(distinct return_id) as total_returns,
        sum(refund_amount)        as total_refunded
    from {{ source('retail_staging', 'returns') }}
    where status = 'approved'
    group by product_id
),

item_sales as (
    select
        i.product_id,
        o.order_year_month,
        sum(i.quantity)                            as units_sold,
        sum(i.line_total)                          as gross_revenue,
        sum(i.discount_amount)                     as total_discount_given,
        count(distinct i.order_id)                 as orders_containing_product,
        round(avg(i.discount_pct), 2)              as avg_discount_pct,
        round(avg(i.line_total), 2)                as avg_line_total

    from items i
    join orders o using (order_id)
    where o.is_completed = true
    group by i.product_id, o.order_year_month
),

joined as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.sku,
        p.unit_price,
        p.unit_cost,
        p.gross_margin_pct,
        p.is_active,
        s.order_year_month,
        coalesce(s.units_sold, 0)                          as units_sold,
        coalesce(s.gross_revenue, 0)                       as gross_revenue,
        coalesce(s.total_discount_given, 0)                as total_discount_given,
        coalesce(s.orders_containing_product, 0)           as orders_containing_product,
        coalesce(s.avg_discount_pct, 0)                    as avg_discount_pct,
        coalesce(s.avg_line_total, 0)                      as avg_line_total,
        coalesce(r.total_returns, 0)                       as total_returns,
        coalesce(r.total_refunded, 0)                      as total_refunded,
        round(
            coalesce(r.total_returns, 0)
            / nullif(coalesce(s.units_sold, 0), 0) * 100,
            2
        )                                                   as return_rate_pct,
        round(
            coalesce(s.gross_revenue, 0) - coalesce(s.total_discount_given, 0)
            - (p.unit_cost * coalesce(s.units_sold, 0)),
            2
        )                                                   as estimated_gross_profit

    from products p
    left join item_sales s using (product_id)
    left join returns    r using (product_id)
)

select * from joined
