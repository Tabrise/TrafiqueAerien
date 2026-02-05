with flights as (
    select *
    from {{ ref('int_flights_with_weather') }}
),
airports as (
    select *
    from {{ ref('stg_airports') }}
),
holidays as (
    select
        cast(date as date) as holiday_date,
        upper(country_code) as country_code,
        is_holiday
    from {{ ref('holidays_eu') }}
)

select
    f.icao24,
    f.callsign,
    f.flight_type,
    f.airport_icao,
    f.est_departure_airport,
    f.est_arrival_airport,
    f.first_seen_ts_utc,
    f.last_seen_ts_utc,
    f.event_hour_utc,
    date_diff('minute', f.first_seen_ts_utc, f.last_seen_ts_utc) as flight_duration_min,
    extract('hour' from f.event_hour_utc) as hour_of_day_utc,
    extract('dow' from f.event_hour_utc) as day_of_week_utc,
    extract('month' from f.event_hour_utc) as month_utc,
    extract('week' from f.event_hour_utc) as week_of_year_utc,
    extract('doy' from f.event_hour_utc) as day_of_year_utc,
    case when extract('dow' from f.event_hour_utc) in (0, 6) then true else false end as is_weekend_utc,
    f.temperature_2m,
    f.precipitation,
    f.wind_speed_10m,
    f.cloud_cover,
    a.country_code as airport_country_code,
    concat(f.est_departure_airport, '-', f.est_arrival_airport) as route_code,
    coalesce(h.is_holiday, false) as is_holiday
from flights f
left join airports a
    on a.icao = f.airport_icao
left join holidays h
    on h.country_code = a.country_code
   and h.holiday_date = cast(f.event_hour_utc as date)
