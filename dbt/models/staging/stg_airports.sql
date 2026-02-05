select
    upper(icao) as icao,
    latitude,
    longitude,
    coalesce(timezone, 'UTC') as timezone,
    upper(country_code) as country_code
from read_csv_auto('{{ var("reference_dir", "../data/reference") }}/airports_eu.csv')
