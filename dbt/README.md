dbt - Preparation des donnees

Objectif
- Structurer les donnees brutes (OpenSky + Meteo) en tables analytiques
- Produire des marts pour le ML (features par vol)

Modele cible (a adapter)
1) Staging
- stg_opensky_flights
- stg_weather_hourly

2) Intermediate
- int_flights_with_weather (jointure vols + meteo)

3) Marts
- mart_flight_features (features par vol)

Notes
- Stockage par defaut propose: DuckDB (local)

Demarrage rapide (DuckDB)
1) Installer dbt-duckdb
   pip install dbt-duckdb

2) Configurer le profil
   Copier dbt/profiles.yml.example vers ~/.dbt/profiles.yml

3) Lancer dbt depuis le dossier dbt
   cd dbt
   dbt seed
   dbt debug
   dbt run

Variables utiles
- raw_dir: chemin des donnees brutes (defaut: data/raw)
  Exemple: dbt run --vars '{"raw_dir": "data/raw"}'
- reference_dir: chemin des donnees de reference (defaut: data/reference)
  Exemple: dbt run --vars '{"reference_dir": "data/reference"}'

Qualite et completude
- qa_daily_completeness: verifie la couverture meteo (24h) par aeroport/jour
- qa_join_coverage: % de vols avec meteo jointe