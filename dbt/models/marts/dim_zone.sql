-- One row per NYC TLC taxi zone (from the official lookup, loaded as a seed).
select
    "LocationID" as zone_id,
    "Borough" as borough,
    "Zone" as zone_name,
    service_zone,
    "Zone" in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') as is_airport
from {{ ref('taxi_zone_lookup') }}
