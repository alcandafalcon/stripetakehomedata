with merchants as (

    select *
    from {{ ref('stg_merchants') }}

),

suitability as (

    select *
    from {{ ref('int_merchant_suitability') }}

),

scoring as (

    select *
    from {{ ref('int_propensity_scoring') }}

),

performance as (

    select *
    from {{ ref('int_merchant_performance') }}

),

joined as (
    select
        m.merchant_id,
        m.industry,
        m.country,
        m.business_size,
        m.first_charge_date,

        -- Metrics from performance
        p.total_volume_ltm,
        p.subscription_volume_ltm,
        p.checkout_volume_ltm,
        p.payment_link_volume_ltm,
        p.daily_vol_stddev,
        p.active_days_count,

        -- Suitability Flags
        s.is_existing_subscriber,
        s.is_low_volume,
        s.is_churned,
        s.is_suitable_merchant,

        -- Scores
        sc.score_frequency,
        sc.score_pricing_consistency,
        sc.score_product_fit,
        sc.score_industry,
        sc.score_maturity,
        sc.score_size,
        sc.total_propensity_score,

        -- New Fit Flags
        sc.is_behavioural_fit,
        sc.is_customer_profile_fit,
        sc.is_volume_fit,

        -- Outreach Segment
        case 
            when sc.total_propensity_score >= 5 then 'Tier 1: High Propensity (Score 5-6)'
            when sc.total_propensity_score = 4 then 'Tier 2: Moderate Propensity (Score 4)'
            else 'Tier 2.5: Moderate Propensity (Score 3)' 
        end as outreach_segment

    from merchants as m
    left join suitability as s
        on m.merchant_id = s.merchant_id
    left join scoring as sc
        on m.merchant_id = sc.merchant_id
    left join performance as p
        on m.merchant_id = p.merchant_id
)

select *
from joined
