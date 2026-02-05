with raw as (
    select *
    from read_json_auto(
        '{{ var("raw_dir", "../data/raw") }}/weather/*/*.json',
        union_by_name=true
    )
),
expanded as (
    select
        airport_icao,
        timezone,
        hourly.time as time_list,
        hourly.temperature_2m as temperature_2m_list,
        hourly.precipitation as precipitation_list,
        hourly.wind_speed_10m as wind_speed_10m_list,
        hourly.cloud_cover as cloud_cover_list
    from raw
)

select
    airport_icao,
    coalesce(timezone, 'UTC') as timezone,
    list_extract(time_list, i) as time_local,
    strptime(list_extract(time_list, i), '%Y-%m-%dT%H:%M') as time_utc,
    list_extract(temperature_2m_list, i) as temperature_2m,
    list_extract(precipitation_list, i) as precipitation,
    list_extract(wind_speed_10m_list, i) as wind_speed_10m,
    list_extract(cloud_cover_list, i) as cloud_cover
from expanded, range(0, list_count(time_list)) as t(i)
