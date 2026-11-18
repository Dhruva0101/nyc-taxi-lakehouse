-- Example report built on the star schema: revenue and tipping by borough and day.
select
    d.date_day,
    d.is_weekend,
    z.borough,
    count(*) as trips,
    round(sum(f.total_amount), 2) as revenue,
    round(avg(f.card_tip_pct), 2) as avg_card_tip_pct
from {{ ref('fct_trips') }} f
join {{ ref('dim_date') }} d using (date_day)
join {{ ref('dim_zone') }} z on f.pickup_zone_id = z.zone_id
group by all
