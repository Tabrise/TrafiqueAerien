API - Serving predictions

Prerequis
- Modele entraine: ml/artifacts/model.joblib + ml/artifacts/features.json
- Base DuckDB: data/analytics.duckdb (dbt run)

Lancer l'API
  uvicorn api.app:app --reload --port 8000

Endpoints
- GET /health
- GET /predictions?limit=100
- POST /predict
- GET /predictions.csv?limit=100
- GET /flights?limit=100&airport_icao=LFPG&flight_type=departure
- GET /flights/{icao24}?limit=100
- GET /airports?limit=200

Exemple /predict
{
  "records": [
    {
      "flight_type": "departure",
      "airport_icao": "LFPG",
      "est_departure_airport": "LFPG",
      "est_arrival_airport": "EDDF",
      "route_code": "LFPG-EDDF",
      "airport_country_code": "FR",
      "flight_duration_min": 90,
      "hour_of_day_utc": 12,
      "day_of_week_utc": 2,
      "month_utc": 2,
      "week_of_year_utc": 5,
      "day_of_year_utc": 32,
      "temperature_2m": 5.2,
      "precipitation": 0.0,
      "wind_speed_10m": 12.4,
      "cloud_cover": 65
    }
  ]
}