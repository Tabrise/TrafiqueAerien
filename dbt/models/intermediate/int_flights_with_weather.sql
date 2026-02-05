with flights as (
    select *
    from {{ ref('stg_opensky_flights') }}
),
weather as (
    select *
    from {{ ref('stg_weather_hourly') }}
)

select
    f.*,
    case
        when f.flight_type = 'departure' then f.first_seen_hour_utc
        else f.last_seen_hour_utc
    end as event_hour_utc,
    w.temperature_2m,
    w.precipitation,
    w.wind_speed_10m,
    w.cloud_cover
from flights f
left join weather w
    on w.airport_icao = f.airport_icao
   and w.time_utc = case
        when f.flight_type = 'departure' then f.first_seen_hour_utc
        else f.last_seen_hour_utc
    end
