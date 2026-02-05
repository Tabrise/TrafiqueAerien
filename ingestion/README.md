Ingestion - Trafic aerien et meteo

Objectif
- Extraire les vols OpenSky (arrivees + departs) par aeroport
- Extraire la meteo historique (Open-Meteo) par aeroport

Prerequis
- config.yaml a la racine (copie de config.example.yaml)
- data/reference/airports_eu.csv avec au minimum:
  icao, latitude, longitude, timezone, country_code

Exemples
- Generer la liste des aeroports Europe (OurAirports)
  python ingestion\airports_fetch.py

- Vols (jour unique)
  python ingestion\opensky_fetch.py --date 2026-02-01

- Vols (plage de dates)
  python ingestion\opensky_fetch.py --start 2026-02-01 --end 2026-02-03

- Meteo (jour unique)
  python ingestion\weather_fetch.py --date 2026-02-01

Sorties
- OpenSky: data/raw/opensky/YYYY-MM-DD/*.jsonl
- Meteo:  data/raw/weather/YYYY-MM-DD/*.json

Notes
- La meteo est recuperee en UTC pour faciliter les jointures.
