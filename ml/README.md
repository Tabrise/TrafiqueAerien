ML - Entrainement de base

Objectif
- Entrainement d'un modele baseline a partir de la table dbt `mart_flight_features`

Prerequis
- dbt run termine (table `mart_flight_features` dans DuckDB)
- Python deps: scikit-learn, duckdb, joblib

Exemples
- Regression (ex: delay_min si tu l'ajoutes plus tard)
  python ml\train.py --target delay_min --target-type regression

- Classification (ex: delay_gt_15)
  python ml\train.py --target delay_gt_15 --target-type classification

Sorties
- ml/artifacts/model.joblib
- ml/artifacts/metrics.json
