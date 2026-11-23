# Exercice 5 — Prédiction du prix d'une course

On entraîne un modèle de machine learning qui prédit le prix payé (`total_amount`) d'une course de taxi jaune à New York.

Les données viennent du parquet nettoyé par l'exercice 2 (branche 1), stocké dans RustFS :
`s3://nyc-raw/processed/yellow_2026_01/`

## Fichiers

```text
exo5_ml_prediction_service/
├── preparation.py      # chargement depuis RustFS, features et nettoyage
├── train.py            # entraînement et évaluation (RMSE)
├── predict.py          # prédiction avec le modèle sauvegardé
├── app.py              # front-end Streamlit
└── tests/
    └── test_donnees.py # tests sur les données d'entraînement et d'inférence
```

## Prérequis

- uv
- RustFS lancé : `docker compose up -d rustfs` depuis la racine du projet
- Les exercices 1 et 2 exécutés, pour avoir le parquet nettoyé dans RustFS

## Lancement

Depuis le dossier `exo5_ml_prediction_service` :

```powershell
uv sync
uv run python train.py      # entraîne le modèle et crée modele.joblib
uv run python predict.py    # prédit le prix de deux courses d'exemple
uv run streamlit run app.py # ouvre le front-end sur http://localhost:8501
```

Le fichier `modele.joblib` (environ 70 Mo) n'est pas versionné, il faut lancer `train.py` avant `predict.py` ou `app.py`.

## Tests et qualité du code

```powershell
uv run pytest
uv run flake8 preparation.py train.py predict.py app.py tests/
```

La documentation des fonctions a été générée avec `pyment` au format NumpyDoc.

## Modèle

Features utilisées : distance, durée de la course, zone de départ, zone d'arrivée, heure et jour de la semaine.
Les colonnes de montant (`fare_amount`, `tip_amount`, ...) ne sont pas utilisées car elles font partie du prix à prédire.

Nettoyage : on garde les courses avec un montant entre 0 et 300 $, une distance entre 0 et 100 miles et une durée entre 1 et 180 minutes (3 504 809 courses sur 3 724 888).
L'entraînement est fait sur un échantillon de 400 000 courses (80 % train, 20 % test).

| Modèle | RMSE |
|---|---|
| Régression linéaire | 9.98 |
| Forêt aléatoire | 7.68 |

On garde la forêt aléatoire.
