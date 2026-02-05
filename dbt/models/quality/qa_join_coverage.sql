with flights as (
    select *
    from {{ ref('int_flights_with_weather') }}
)

select
    date_trunc('day', event_hour_utc) as day_utc,
    count(*) as flights_total,
    count(*) filter (where temperature_2m is not null) as flights_with_weather,
    round(
        100.0 * count(*) filter (where temperature_2m is not null) / nullif(count(*), 0),
        2
    ) as pct_with_weather
from flights
group by 1
