with performance as (
    select * from {{ ref('int_merchant_performance') }}
)

select
    merchant_id,

    -- Flagging logic
    coalesce(subscription_volume_ltm > 0, false) as is_existing_subscriber,

    coalesce(total_volume_ltm < 100000, false) as is_low_volume,

    coalesce(total_volume_l6m = 0, false) as is_churned,

    -- Combined suitability
    coalesce(
        (subscription_volume_ltm = 0) -- Not existing subscriber
        and (total_volume_ltm >= 100000) -- Not low volume
        and (total_volume_l6m > 0), false -- Not churned
    ) as is_suitable_merchant

from performance
