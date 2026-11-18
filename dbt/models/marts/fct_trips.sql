-- Trip-grain fact table: foreign keys to dim_zone and dim_date plus measures.
select
    trip_id,
    trip_date as date_day,
    pickup_zone_id,
    dropoff_zone_id,
    vendor_id,
    case payment_type
        when 1 then 'card'
        when 2 then 'cash'
        when 3 then 'no_charge'
        when 4 then 'dispute'
        else 'other'
    end as payment_method,
    passenger_count,
    trip_distance,
    duration_min,
    fare_amount,
    tip_amount,
    total_amount,
    case when payment_type = 1 then round(tip_amount / fare_amount * 100, 2) end as card_tip_pct
from {{ ref('stg_trips') }}
