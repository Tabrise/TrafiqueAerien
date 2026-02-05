with raw as (
    select *
    from read_json_auto(
        '{{ var("raw_dir", "../data/raw") }}/opensky/*/*.jsonl',
        union_by_name=true
    )
)

select
    icao24,
    trim(callsign) as callsign,
    estDepartureAirport as est_departure_airport,
    estArrivalAirport as est_arrival_airport,
    firstSeen as first_seen,
    lastSeen as last_seen,
    estDepartureAirportHorizDistance as est_departure_airport_horiz_distance,
    estDepartureAirportVertDistance as est_departure_airport_vert_distance,
    estArrivalAirportHorizDistance as est_arrival_airport_horiz_distance,
    estArrivalAirportVertDistance as est_arrival_airport_vert_distance,
    departureAirportCandidatesCount as departure_airport_candidates_count,
    arrivalAirportCandidatesCount as arrival_airport_candidates_count,
    flight_type,
    airport_icao,
    to_timestamp(firstSeen) as first_seen_ts_utc,
    to_timestamp(lastSeen) as last_seen_ts_utc,
    date_trunc('hour', to_timestamp(firstSeen)) as first_seen_hour_utc,
    date_trunc('hour', to_timestamp(lastSeen)) as last_seen_hour_utc
from raw
