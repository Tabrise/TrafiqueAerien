Projet Open Data - Trafic aerien (Europe)

Objectif
- Recuperer et preparer des donnees de trafic aerien (par vol) et de meteo
- Construire un modele de prediction (retard, meteo, volume, etc.)

Decoupage
1) Recuperation + preparation des donnees (ingestion + dbt)
2) Modele ML (a ajouter dans un second temps)

Sources prevues
- OpenSky Network API (ADS-B ouvert, donnees de vols)
- Meteo: Open-Meteo (archive historique), ajustable si besoin

Frequence
- Batch quotidien

Arborescence
.
├─ ingestion/
│  ├─ opensky_fetch.py
│  ├─ weather_fetch.py
│  ├─ utils.py
│  └─ README.md
├─ dbt/
│  └─ README.md
├─ data/
│  ├─ raw/
│  └─ reference/
│     └─ airports_eu.csv
├─ config.example.yaml
└─ requirements.txt

Quickstart (Windows PowerShell)
1) Installer les dependances
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt

2) Config
   copy config.example.yaml config.yaml
   Renseigner vos identifiants OpenSky et la liste d'aeroports

3) Recuperation des donnees (exemples)
   python ingestion\airports_fetch.py
   python ingestion\opensky_fetch.py --date 2026-02-01
   python ingestion\weather_fetch.py --date 2026-02-01

4) Preparation dbt (DuckDB)
   Copier dbt/profiles.yml.example vers ~/.dbt/profiles.yml
   cd dbt
   dbt run

5) Entrainement ML (baseline)
   python ml\train.py --target delay_min --target-type regression

6) API (predictions)
   uvicorn api.app:app --reload --port 8000

Notes
- Le fichier airports_eu.csv doit contenir au minimum les colonnes:
  icao, latitude, longitude, timezone, country_code
- Les donnees brutes sont stockees dans data/raw/*
- La preparation sera faite dans dbt (schema a definir une fois la source stable)
