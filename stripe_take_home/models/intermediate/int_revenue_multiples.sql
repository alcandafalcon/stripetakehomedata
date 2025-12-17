with monthly_payments as (

    select
        merchant_id,
        date_trunc('month', payment_date) as payment_month,
        sum(total_volume) as monthly_volume
    from {{ ref('stg_payments') }}
    group by 1, 2

),

merchant_stats as (
    select
        merchant_id,
        count(*) as active_months,

        -- Check for multiples of 1000 ($10.00)
        min(mod(monthly_volume, 1000)) as min_mod_1000,
        max(mod(monthly_volume, 1000)) as max_mod_1000,

        -- Check for multiples of 100 ($1.00)
        min(mod(monthly_volume, 100)) as min_mod_100,
        max(mod(monthly_volume, 100)) as max_mod_100,

        -- Check for multiples of 999 ($9.99)
        min(mod(monthly_volume, 999)) as min_mod_999,
        max(mod(monthly_volume, 999)) as max_mod_999,

        -- Check for multiples of 500 ($5.00)
        min(mod(monthly_volume, 500)) as min_mod_500,
        max(mod(monthly_volume, 500)) as max_mod_500

    from monthly_payments
    where monthly_volume > 0
    group by 1
)

select
    merchant_id,

    -- Boolean flags
    coalesce(max_mod_1000 = 0, false) as is_multiple_of_1000,
    coalesce(max_mod_100 = 0, false) as is_multiple_of_100,
    coalesce(max_mod_999 = 0, false) as is_multiple_of_999,
    coalesce(max_mod_500 = 0, false) as is_multiple_of_500,

    -- Consolidated flag: Is it a "clean" multiple of common pricing tiers?
    coalesce(max_mod_100 = 0 or max_mod_999 = 0 or max_mod_500 = 0, false)
        as has_consistent_pricing_units

from merchant_stats
