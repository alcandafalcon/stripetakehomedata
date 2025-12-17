with payments as (

    select distinct
        merchant_id,
        payment_date
    from {{ ref('stg_payments') }}
),

lagged as (

    select
        merchant_id,
        payment_date,
        lag(payment_date)
            over (
                partition by merchant_id
                order by payment_date
            )
            as prev_payment_date
    from payments

),

intervals as (

    select
        merchant_id,
        datediff('day', prev_payment_date, payment_date)
            as days_since_last_payment
    from lagged
    where prev_payment_date is not null

),

stats as (

    select
        merchant_id,
        avg(days_since_last_payment) as avg_interval,
        stddev(days_since_last_payment) as stddev_interval,
        count(*) as interval_count
    from intervals
    group by 1

)

select
    merchant_id,
    avg_interval,
    stddev_interval,
    interval_count,
    case
        -- Check for ~30 day intervals (Net30) with low variance
        -- this would be much better, if we had the customer/billing_entity_id each payment was made from/to
        when
            interval_count >= 2
            and (stddev_interval is null or stddev_interval < 3)
            and (avg_interval between 28 and 32)
            then true

        -- Check for ~10 day intervals (Net10)
        when
            interval_count >= 2
            and (stddev_interval is null or stddev_interval < 2)
            and (avg_interval between 9 and 11)
            then true

        else false
    end as has_regular_payments
from stats
