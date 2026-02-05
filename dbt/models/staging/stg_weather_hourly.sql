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
    struct_extract(z, 1) as time_local,
    strptime(struct_extract(z, 1), '%Y-%m-%dT%H:%M') as time_utc,
    struct_extract(z, 2) as temperature_2m,
    struct_extract(z, 3) as precipitation,
    struct_extract(z, 4) as wind_speed_10m,
    struct_extract(z, 5) as cloud_cover
from expanded,
    unnest(
        list_zip(
            time_list,
            temperature_2m_list,
            precipitation_list,
            wind_speed_10m_list,
            cloud_cover_list
        )
    ) as t(z)
