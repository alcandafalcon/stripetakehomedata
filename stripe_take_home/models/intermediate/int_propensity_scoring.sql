with merchants as (
    select * from {{ ref('stg_merchants') }}
),

performance as (
    select * from {{ ref('int_merchant_performance') }}
),

multiples as (
    select * from {{ ref('int_revenue_multiples') }}
),

intervals as (
    select * from {{ ref('int_payment_intervals') }}
),

joined as (
    select
        m.merchant_id,
        m.industry,
        m.first_charge_date,
        m.business_size,

        coalesce(p.total_volume_ltm, 0) as total_volume_ltm,
        coalesce(p.total_volume_prev_ltm, 0) as total_volume_prev_ltm,
        coalesce(p.subscription_volume_ltm, 0) as subscription_volume_ltm,
        coalesce(p.checkout_volume_ltm, 0) as checkout_volume_ltm,
        coalesce(p.payment_link_volume_ltm, 0) as payment_link_volume_ltm,
        coalesce(p.active_days_count, 0) as active_days_count,

        coalesce(mu.has_consistent_pricing_units, false)
            as has_consistent_pricing_units,
        
        coalesce(i.has_regular_payments, false) as has_regular_payments

    from merchants as m
    left join performance as p on m.merchant_id = p.merchant_id
    left join multiples as mu on m.merchant_id = mu.merchant_id
    left join intervals as i on m.merchant_id = i.merchant_id
),

scored as (
    select
        merchant_id,

        -- 1. Recurring/Behavioural: High active days
        case
            when active_days_count > 50 then 1 else 0
        end as score_frequency,

        -- 2. Pricing Consistency (New)
        case
            when has_consistent_pricing_units then 1 else 0
        end as score_pricing_consistency,

        -- 3. Use of Checkout or Payment Links
        case
            when
                checkout_volume_ltm > 0 or payment_link_volume_ltm > 0
                then 1
            else 0
        end as score_product_fit,

        -- 4. Industry
        case
            when
                industry in (
                    'Software',
                    'Education',
                    'Rentals',
                    'Leisure',
                    'Business services', 
                    'Religion, politics & other memberships',
                    'Ticketing & events'
                )
                then 1
            else 0
        end as score_industry,

        -- 5. Maturity
        case
            when
                first_charge_date
                < dateadd(
                    'month', -6, cast('{{ var("current_date") }}' as timestamp)
                )
                then 1
            else 0
        end as score_maturity,

        -- 6. Size
        case
            when lower(business_size) in ('medium', 'large') then 1 else 0
        end as score_size,

        -- NEW: Behavioural Fit
        -- mimicking Subscription features (regular payments), 
        -- but using Stripe Checkout and Stripe Payment Links
        case
            when 
                (checkout_volume_ltm > 0 or payment_link_volume_ltm > 0)
                and has_regular_payments
            then true
            else false
        end as is_behavioural_fit,

        -- NEW: Customer Profile Fit
        -- business types with propensity to use recurring invoicing features, growing volume
        case
            when
                industry in (
                    'Software',
                    'Education',
                    'Rentals',
                    'Leisure',
                    'Business services', 
                    'Religion, politics & other memberships',
                    'Ticketing & events'
                )
                and total_volume_ltm > total_volume_prev_ltm
            then true
            else false
        end as is_customer_profile_fit,

        -- NEW: Volume Fit
        -- merchants with growing volume and sufficient scale
        case
            when
                total_volume_ltm > total_volume_prev_ltm
                and total_volume_ltm >= 100000
            then true
            else false
        end as is_volume_fit

    from joined
)

select
    *,
    (
        score_frequency
        + score_pricing_consistency
        + score_product_fit
        + score_industry
        + score_maturity
        + score_size
    ) as total_propensity_score
from scored
