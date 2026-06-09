-- models/staging/stg_customers.sql

with source as (
    select * from {{ source('retail_staging', 'customers') }}
),

cleaned as (
    select
        customer_id,
        first_name,
        last_name,
        concat(first_name, ' ', last_name)           as full_name,
        email,
        state,
        city,
        signup_channel,
        cast(is_loyalty_member as bool)              as is_loyalty_member,
        cast(created_at as timestamp)                as created_at,
        extract(year from cast(created_at as timestamp))  as signup_year,
        format_date('%Y-%m', date(cast(created_at as timestamp))) as signup_year_month,
        _ingested_at
    from source
    where customer_id is not null
      and email like '%@%'
)

select * from cleaned
