-- One row per calendar day covered by the trips.
with bounds as (
    select min(trip_date) as first_day, max(trip_date) as last_day from {{ ref('stg_trips') }}
)
select
    cast(d as date) as date_day,
    year(d) as year,
    month(d) as month,
    day(d) as day_of_month,
    dayname(d) as day_name,
    isodow(d) in (6, 7) as is_weekend
from bounds, generate_series(first_day, last_day, interval 1 day) as t(d)
