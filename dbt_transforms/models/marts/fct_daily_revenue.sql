-- models/marts/revenue/fct_daily_revenue.sql
-- Gold layer fact table: daily revenue aggregated by channel, state, and payment method.
-- Primary table powering the Looker Studio revenue trend charts.

with orders as (
    select * from {{ ref('stg_orders') }}
    where is_completed = true
),

daily_agg as (
    select
        order_date,
        order_year,
        order_month,
        order_year_month,
        order_day_of_week,
        state,
        acquisition_channel,
        payment_method,

        count(distinct order_id)                         as total_orders,
        count(distinct customer_id)                      as unique_customers,
        sum(order_total)                                 as gross_revenue,
        sum(subtotal)                                    as net_revenue,
        sum(shipping_cost)                               as shipping_revenue,
        sum(tax)                                         as tax_collected,
        round(avg(order_total), 2)                       as avg_order_value,
        round(avg(subtotal), 2)                          as avg_basket_size,
        sum(case when is_free_shipping then 1 else 0 end) as free_shipping_orders,
        count(distinct case
            when is_returned then order_id end)          as returned_orders

    from orders
    group by 1, 2, 3, 4, 5, 6, 7, 8
)

select
    *,
    round(returned_orders / nullif(total_orders, 0) * 100, 2) as return_rate_pct,
    round(free_shipping_orders / nullif(total_orders, 0) * 100, 2) as free_shipping_rate_pct
from daily_agg
