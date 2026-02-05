with flights as (
    select
        airport_icao,
        date_trunc('day', event_hour_utc) as day_utc,
        count(*) as flights_count
    from {{ ref('int_flights_with_weather') }}
    group by 1, 2
),
weather as (
    select
        airport_icao,
        date_trunc('day', time_utc) as day_utc,
        count(*) as weather_hours_count
    from {{ ref('stg_weather_hourly') }}
    group by 1, 2
)

select
    coalesce(f.airport_icao, w.airport_icao) as airport_icao,
    coalesce(f.day_utc, w.day_utc) as day_utc,
    coalesce(f.flights_count, 0) as flights_count,
    coalesce(w.weather_hours_count, 0) as weather_hours_count,
    case
        when coalesce(w.weather_hours_count, 0) >= 24 then 0
        else 24 - coalesce(w.weather_hours_count, 0)
    end as missing_weather_hours
from flights f
full join weather w
    on f.airport_icao = w.airport_icao
   and f.day_utc = w.day_utc
