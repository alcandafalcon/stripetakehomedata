with payments as (
    select * from {{ ref('stg_payments') }}
),

metrics as (
    select
        merchant_id,
        min(payment_date) as first_seen_date,
        max(payment_date) as last_seen_date,
        count(distinct payment_date) as active_days_count,

        -- Volume metrics using current_date var LTM 
        -- current date has been defined in dbt vars 
        sum(
            case
                when
                    payment_date
                    >= dateadd(
                        'year',
                        -1,
                        cast('{{ var("current_date") }}' as timestamp)
                    )
                    then total_volume
                else 0
            end
        ) as total_volume_ltm,
        sum(
            case
                when
                    payment_date
                    >= dateadd(
                        'year',
                        -1,
                        cast('{{ var("current_date") }}' as timestamp)
                    )
                    then subscription_volume
                else 0
            end
        ) as subscription_volume_ltm,
        sum(
            case
                when
                    payment_date
                    >= dateadd(
                        'year',
                        -1,
                        cast('{{ var("current_date") }}' as timestamp)
                    )
                    then checkout_volume
                else 0
            end
        ) as checkout_volume_ltm,
        sum(
            case
                when
                    payment_date
                    >= dateadd(
                        'year',
                        -1,
                        cast('{{ var("current_date") }}' as timestamp)
                    )
                    then payment_link_volume
                else 0
            end
        ) as payment_link_volume_ltm,

        -- Last 6 Months
        sum(
            case
                when
                    payment_date
                    >= dateadd(
                        'month',
                        -6,
                        cast('{{ var("current_date") }}' as timestamp)
                    )
                    then total_volume
                else 0
            end
        ) as total_volume_l6m,

        -- Previous LTM for Growth Calculation
        sum(
            case
                when
                    payment_date
                    >= dateadd(
                        'year',
                        -2,
                        cast('{{ var("current_date") }}' as timestamp)
                    )
                    and payment_date
                    < dateadd(
                        'year',
                        -1,
                        cast('{{ var("current_date") }}' as timestamp)
                    )
                    then total_volume
                else 0
            end
        ) as total_volume_prev_ltm,

        -- Stability SD of daily vol 
        stddev(total_volume) as daily_vol_stddev

    from payments
    group by 1
)

select * from metrics
