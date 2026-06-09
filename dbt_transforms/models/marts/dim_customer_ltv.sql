-- models/marts/dim_customer_ltv.sql
-- Gold layer: customer-level lifetime value, order frequency, and churn signals.
-- Joins staging orders and customers. Powers the Customer Intelligence dashboard page.

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
    where is_completed = true
),

customer_orders as (
    select
        customer_id,
        count(distinct order_id)                          as total_orders,
        sum(order_total)                                  as lifetime_value,
        round(avg(order_total), 2)                        as avg_order_value,
        min(order_date)                                   as first_order_date,
        max(order_date)                                   as last_order_date,
        date_diff(max(order_date), min(order_date), day)  as customer_age_days,
        countif(is_returned)                              as total_returns,
        sum(case when is_returned then order_total else 0 end) as returned_revenue

    from orders
    group by customer_id
),

joined as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.state,
        c.city,
        c.signup_channel,
        c.is_loyalty_member,
        c.signup_year,
        c.signup_year_month,

        coalesce(co.total_orders, 0)          as total_orders,
        coalesce(co.lifetime_value, 0)        as lifetime_value,
        coalesce(co.avg_order_value, 0)       as avg_order_value,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.customer_age_days, 0)     as customer_age_days,
        coalesce(co.total_returns, 0)         as total_returns,
        coalesce(co.returned_revenue, 0)      as returned_revenue,

        -- Segmentation
        case
            when coalesce(co.total_orders, 0) = 0             then 'no_orders'
            when coalesce(co.total_orders, 0) = 1             then 'one_time_buyer'
            when coalesce(co.total_orders, 0) between 2 and 4 then 'repeat_buyer'
            when coalesce(co.total_orders, 0) >= 5            then 'loyal_customer'
        end as customer_segment,

        -- LTV tier
        case
            when coalesce(co.lifetime_value, 0) >= 1000 then 'high_value'
            when coalesce(co.lifetime_value, 0) >= 300  then 'mid_value'
            when coalesce(co.lifetime_value, 0) > 0     then 'low_value'
            else 'inactive'
        end as ltv_tier,

        -- Churn signal: no order in last 90 days
        case
            when co.last_order_date < date_sub(current_date(), interval 90 day)
            then true else false
        end as is_churn_risk,

        round(
            coalesce(co.returned_revenue, 0)
            / nullif(coalesce(co.lifetime_value, 0), 0) * 100,
            2
        ) as return_revenue_pct

    from customers c
    left join customer_orders co using (customer_id)
)

select * from joined
