-- Silver trips from the Delta Lake table written by the PySpark pipeline.
select
    md5(concat_ws('|', vendor_id, pickup_ts, dropoff_ts, pickup_zone_id, dropoff_zone_id, total_amount)) as trip_id,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    trip_date,
    pickup_zone_id,
    dropoff_zone_id,
    payment_type,
    passenger_count,
    trip_distance,
    duration_min,
    fare_amount,
    tip_amount,
    total_amount
from delta_scan('{{ var("lake") }}/silver/yellow_trips')
