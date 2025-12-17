with source as (

    select * from {{ ref('payments') }}

),

renamed as (

    select
        cast(date as timestamp) as payment_date,
        cast(merchant as string) as merchant_id,
        cast(subscription_volume as numeric) as subscription_volume,
        cast(checkout_volume as numeric) as checkout_volume,
        cast(payment_link_volume as numeric) as payment_link_volume,
        cast(total_volume as numeric) as total_volume

    from source

)

select
    {{ dbt_utils.generate_surrogate_key(['payment_date', 'merchant_id', 'total_volume']) }} as payment_id,
    *
from renamed
where total_volume >= (subscription_volume + checkout_volume + payment_link_volume)
