with source as (

    select * from {{ ref('merchants') }}

),

renamed as (

    select
        cast(merchant as string) as merchant_id,
        cast(industry as string) as industry,
        cast(first_charge_date as timestamp) as first_charge_date,
        cast(country as string) as country,
        cast(business_size as string) as business_size

    from source
    where cast(merchant as string) not in ('0.00E+00', '4.72E+10')
    {#  Since these have distinct industries & countries but share the same merchant_id, 
    I assume these were meant to be distinct merchants. But creating a surrogate key 
    from these fields impacts the join to payments (same set of payments to all of 
    these repeated merchants). Thus, I have decided to omit them from this analysis #}

)

select *
from renamed
