# Projet BIG DATA — Data Engineering pour l'intelligence artificielle

Projet de groupe (4 personnes) réalisé dans le cadre du cours **Data Engineering pour l'intelligence artificielle** à CY Tech.

L'objectif est de construire un pipeline exploitant les données **NYC TLC Yellow Taxi** : collecte, stockage dans un Data Lake, contrôle qualité, alimentation d'un Data Warehouse, visualisation et prédiction du prix d'une course.

## Architecture

```text
NYC TLC — fichiers Parquet mensuels
             |
             v
       RustFS / S3 (Data Lake)
             |   nyc-raw/yellow_tripdata_2026-01.parquet
             v
       Scala + Apache Spark
       lecture et validation
             |
        +----+-----------------------+
        |                            |
        v                            v
 Branche 1                     Branche 2 (à faire)
 Parquet nettoyé                Transformations OLAP
 et quarantaine                --> PostgreSQL
 dans RustFS                         |
        |                            v
        v                        Dashboard BI
 Modèle ML Python
 (à faire)
```

**Attention :** le traitement Spark implémenté pour l'instant s'exécute en mode local (`local[2]`). Le cluster Spark Docker est fourni par le projet, mais son exploitation effective reste à intégrer ou à valider.

## Avancement

| Exercice | Description | État |
|---|---|---|
| 1 | Collecte et dépôt des Parquet dans RustFS | Fonctionnel en local |
| 2 — Branche 1 | Lecture Spark, validation, export Parquet | Première version fonctionnelle, tests à enrichir |
| 3 | Schéma multidimensionnel SQL / Data Warehouse | À réaliser |
| 2 — Branche 2 | Ingestion des faits/dimensions dans PostgreSQL | À réaliser après l'exercice 3 |
| 4 | Dashboard connecté à PostgreSQL | À réaliser |
| 5 | Régression ML du montant payé | À réaliser |
| 6 (optionnel) | Orchestration Airflow ou Argo | Non commencé |

## Organisation du dépôt

```text
.
├── docker-compose.yml
├── exo1_data_retrieval/          # Collecte HTTP et upload S3
├── exo2_data_ingestion/          # Validation Scala / Spark
│   ├── build.sbt
│   └── src/
│       ├── main/scala/cytech/ingestion/
│       │   ├── Main.scala
│       │   └── QualityRules.scala
│       └── test/scala/cytech/ingestion/
│           └── QualityRulesSpec.scala
├── exo3_sql_olap/               # Schéma SQL (à faire)
├── exo4_dashboard/              # Visualisation (à faire)
└── exo5_ml_prediction_service/  # Modèle ML (à faire)
```

## Prérequis techniques

- Docker Desktop et Docker Compose ;
- Java 17 et SBT (le module exercice 2 utilise `sbtw.ps1` sous PowerShell) ;
- Scala 2.13.18 et Apache Spark 4.2.0, selon la configuration locale de l'exercice 2 ;
- PowerShell pour les commandes ci-dessous ;
- accès Internet pour télécharger les Parquet et les dépendances.

> Le dépôt d'origine mentionne Scala 2.13.17 : il a fallu passer à **2.13.18** pour éviter un conflit de versions avec les dépendances. Vérifier `exo2_data_ingestion/build.sbt` dans la branche utilisée.

## Démarrage local

Depuis la racine du dépôt :

```powershell
docker compose up -d rustfs
docker compose ps
```

Console de RustFS : <http://localhost:9001> ; API S3 : <http://localhost:9000>.

Créer un bucket `nyc-raw` si nécessaire, puis déposer `yellow_tripdata_2026-01.parquet` à sa racine, ou exécuter la collecte de l'exercice 1.

Configurer les accès dans **le terminal courant** (utiliser les identifiants de l'environnement local ; ne pas publier de vrais secrets dans Git) :

```powershell
$env:AWS_ACCESS_KEY_ID="<ACCESS_KEY_RUSTFS>"
$env:AWS_SECRET_ACCESS_KEY="<SECRET_KEY_RUSTFS>"
$env:S3_ENDPOINT="http://localhost:9000"
$env:S3_BUCKET="nyc-raw"
$env:S3_KEY="yellow_tripdata_2026-01.parquet"
```

Exécuter le module d'ingestion :

```powershell
cd exo2_data_ingestion
.\sbtw.ps1 test
.\sbtw.ps1 run
```

Pour les explications et les règles métier détaillées, voir [README de l'exercice 2](exo2_data_ingestion/README.md).

## Résultats observés — janvier 2026

Sur le fichier de janvier 2026, les essais réalisés avant les dernières modifications de libellés ont donné :

| Indicateur | Courses |
|---|---:|
| Total lu | 3 724 889 |
| Statut `VALIDE` | 2 552 515 |
| Statut `A_VERIFIER` | 1 172 373 |
| Statut `REJETE` | 1 |
| Total exporté dans le Parquet conservé | 3 724 888 |

Le nombre d'enregistrements relus depuis RustFS a été contrôlé et le message `Verification des exports reussie !` a été obtenu. **Ces chiffres devront être régénérés si les règles de qualité évoluent.**

Les fichiers créés se trouvent dans le bucket S3 :

- `nyc-raw/processed/yellow_2026_01/` — données conservées avec indicateurs qualité ;
- `nyc-raw/quarantine/yellow_2026_01/` — données rejetées avec motifs de rejet.

## Données et bonnes pratiques

- Source officielle : <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- Dictionnaire Yellow Taxi : <https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf>
- Ne pas committer les fichiers `.parquet`, dossiers `target/`, fichiers `.env` ou secrets réels.
- Le `docker-compose.yml` du template contient des identifiants **de développement seulement** ; les changer avant toute exposition publique du service.
- Les exports actuels utilisent `mode("overwrite")` : ils remplacent le préfixe de sortie du mois traité, **pas** le fichier brut. Adapter la stratégie pour traiter plusieurs mois.

## Travaux restants

1. Compléter le contrat qualité (référentiel des zones, colonnes complémentaires, cas limites) et les tests unitaires.
2. Concevoir les tables de faits et dimensions en SQL (exercice 3).
3. Revenir à la branche 2 : transformer les données et charger PostgreSQL par JDBC.
4. Construire le tableau de bord PostgreSQL puis le pipeline de Machine Learning.
5. Ajouter les captures d'écran, résultats et décisions au mini-rapport final.
