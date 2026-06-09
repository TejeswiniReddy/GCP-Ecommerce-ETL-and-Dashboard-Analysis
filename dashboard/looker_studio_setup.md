# Looker Studio Dashboard Setup Guide

This document explains how to connect the BigQuery Gold layer to Looker Studio
and provides the SQL for each dashboard chart.

---

## Connection Setup

1. Open [Looker Studio](https://lookerstudio.google.com)
2. Click **Create > Report**
3. Select **BigQuery** as data source
4. Connect to: `YOUR_PROJECT.retail_marts`
5. Add these tables as separate data sources:
   - `fct_daily_revenue`
   - `dim_customer_ltv`
   - `fct_product_performance`

---

## Dashboard Pages

### Page 1: Revenue Overview

**KPI Scorecards (top row)**

```sql
-- Total GMV (current month)
SELECT ROUND(SUM(gross_revenue), 2) AS gmv
FROM `retail_marts.fct_daily_revenue`
WHERE order_year_month = FORMAT_DATE('%Y-%m', CURRENT_DATE());

-- Average Order Value
SELECT ROUND(AVG(avg_order_value), 2) AS aov
FROM `retail_marts.fct_daily_revenue`
WHERE order_year_month = FORMAT_DATE('%Y-%m', CURRENT_DATE());

-- Total Orders
SELECT SUM(total_orders) AS total_orders
FROM `retail_marts.fct_daily_revenue`
WHERE order_year_month = FORMAT_DATE('%Y-%m', CURRENT_DATE());

-- Return Rate
SELECT ROUND(AVG(return_rate_pct), 2) AS avg_return_rate
FROM `retail_marts.fct_daily_revenue`
WHERE order_year_month = FORMAT_DATE('%Y-%m', CURRENT_DATE());
```

**Monthly Revenue Trend (Line Chart)**

```sql
SELECT
    order_year_month,
    SUM(gross_revenue)  AS gross_revenue,
    SUM(net_revenue)    AS net_revenue,
    SUM(total_orders)   AS total_orders
FROM `retail_marts.fct_daily_revenue`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
GROUP BY order_year_month
ORDER BY order_year_month;
```

**Revenue by Channel (Bar Chart)**

```sql
SELECT
    acquisition_channel,
    SUM(gross_revenue)           AS revenue,
    SUM(total_orders)            AS orders,
    ROUND(AVG(avg_order_value), 2) AS aov
FROM `retail_marts.fct_daily_revenue`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY acquisition_channel
ORDER BY revenue DESC;
```

**Revenue by State (Geo Map)**

```sql
SELECT
    state,
    SUM(gross_revenue) AS revenue,
    SUM(total_orders)  AS orders
FROM `retail_marts.fct_daily_revenue`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY state
ORDER BY revenue DESC;
```

**Day-of-Week Heatmap (Pivot Table)**

```sql
SELECT
    order_day_of_week,
    order_year_month,
    SUM(total_orders)   AS orders,
    SUM(gross_revenue)  AS revenue
FROM `retail_marts.fct_daily_revenue`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
GROUP BY order_day_of_week, order_year_month
ORDER BY order_year_month, order_day_of_week;
```

---

### Page 2: Customer Intelligence

**Customer Segment Mix (Pie / Donut)**

```sql
SELECT
    customer_segment,
    COUNT(*)                AS customers,
    ROUND(SUM(lifetime_value), 2) AS total_ltv,
    ROUND(AVG(lifetime_value), 2) AS avg_ltv
FROM `retail_marts.dim_customer_ltv`
WHERE customer_segment != 'no_orders'
GROUP BY customer_segment
ORDER BY total_ltv DESC;
```

**LTV Distribution by Tier (Bar)**

```sql
SELECT
    ltv_tier,
    COUNT(*)                       AS customers,
    ROUND(SUM(lifetime_value), 2)  AS total_ltv,
    ROUND(AVG(total_orders), 2)    AS avg_orders
FROM `retail_marts.dim_customer_ltv`
GROUP BY ltv_tier
ORDER BY total_ltv DESC;
```

**New Customer Acquisition by Month (Line)**

```sql
SELECT
    signup_year_month,
    COUNT(*) AS new_customers,
    COUNTIF(is_loyalty_member) AS loyalty_signups
FROM `retail_marts.dim_customer_ltv`
WHERE signup_year_month >= FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))
GROUP BY signup_year_month
ORDER BY signup_year_month;
```

**Churn Risk by State (Table)**

```sql
SELECT
    state,
    COUNT(*)                             AS total_customers,
    COUNTIF(is_churn_risk)               AS churn_risk_count,
    ROUND(COUNTIF(is_churn_risk) * 100.0 / COUNT(*), 1) AS churn_risk_pct
FROM `retail_marts.dim_customer_ltv`
WHERE total_orders > 0
GROUP BY state
ORDER BY churn_risk_pct DESC
LIMIT 20;
```

**Top Customers by LTV (Table)**

```sql
SELECT
    full_name,
    state,
    customer_segment,
    total_orders,
    ROUND(lifetime_value, 2)    AS lifetime_value,
    ROUND(avg_order_value, 2)   AS avg_order_value,
    last_order_date,
    is_churn_risk
FROM `retail_marts.dim_customer_ltv`
ORDER BY lifetime_value DESC
LIMIT 50;
```

---

### Page 3: Product Insights

**Top 10 Products by Revenue (Bar)**

```sql
SELECT
    product_name,
    category,
    SUM(gross_revenue)       AS revenue,
    SUM(units_sold)          AS units_sold,
    ROUND(AVG(return_rate_pct), 2) AS return_rate
FROM `retail_marts.fct_product_performance`
WHERE order_year_month >= FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH))
GROUP BY product_name, category
ORDER BY revenue DESC
LIMIT 10;
```

**Revenue and Margin by Category (Stacked Bar)**

```sql
SELECT
    category,
    SUM(gross_revenue)              AS gross_revenue,
    SUM(estimated_gross_profit)     AS gross_profit,
    ROUND(
        SUM(estimated_gross_profit)
        / NULLIF(SUM(gross_revenue), 0) * 100,
        1
    )                               AS blended_margin_pct,
    SUM(units_sold)                 AS units_sold,
    ROUND(AVG(return_rate_pct), 2)  AS avg_return_rate
FROM `retail_marts.fct_product_performance`
GROUP BY category
ORDER BY gross_revenue DESC;
```

**Return Rate by Category (Bar — sorted desc)**

```sql
SELECT
    category,
    SUM(total_returns)               AS total_returns,
    SUM(units_sold)                  AS units_sold,
    ROUND(
        SUM(total_returns) * 100.0
        / NULLIF(SUM(units_sold), 0),
        2
    )                                AS return_rate_pct
FROM `retail_marts.fct_product_performance`
WHERE units_sold > 0
GROUP BY category
ORDER BY return_rate_pct DESC;
```

**Monthly Product Trend (Line — multi-series)**

```sql
SELECT
    order_year_month,
    category,
    SUM(gross_revenue)  AS revenue,
    SUM(units_sold)     AS units
FROM `retail_marts.fct_product_performance`
WHERE order_year_month >= FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))
GROUP BY order_year_month, category
ORDER BY order_year_month, category;
```

---


