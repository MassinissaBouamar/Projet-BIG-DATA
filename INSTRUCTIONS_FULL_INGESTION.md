# Exercice 2 — Chargement complet de janvier 2026 dans PostgreSQL

**Prérequis :** le pilote de 100 courses a réussi ; `dw.fact_trip`, les 8 dimensions et les zones officielles sont créées ; les Parquet nettoyés sont lisibles sous `s3a://nyc-raw/processed/yellow_2026_01/`.

## 1. Ce que fait ce programme

Il lit le Parquet préparé par la branche 1, vérifie que **toutes les courses** respectent les contraintes du calendrier et des champs essentiels, transforme les 22 colonnes du schéma SQL et envoie le lot dans `staging.fact_trip_load` par JDBC. Une transaction PostgreSQL remplace ensuite uniquement les lignes de `dw.fact_trip` dont `source_file = 'yellow_tripdata_2026-01.parquet'`.

Si le transfert Spark échoue, la table finale reste inchangée. Si l'insertion SQL échoue (clé étrangère, contrainte, etc.), la transaction est annulée et l'ancien lot reste inchangé. Une réexécution réussie remplace le lot ; elle ne l'ajoute pas une deuxième fois. Le programme utilise un verrou PostgreSQL pour refuser une deuxième exécution simultanée de ce chargement. Ne pas utiliser la table de staging pour un autre import en parallèle.

**Attention :** ce programme ne fusionne pas encore la branche 1 et la branche 2 dans un unique job Spark. Ce sera l'étape d'intégration suivante demandée par l'énoncé.

## 2. Préparer les connexions (PowerShell)

Depuis `exo2_data_ingestion` :

```powershell
$env:AWS_ACCESS_KEY_ID="rustfsadmin"
$env:AWS_SECRET_ACCESS_KEY="rustfsadmin"
$env:S3_ENDPOINT="http://localhost:9000"
$env:S3_BUCKET="nyc-raw"
$env:PG_JDBC_URL="jdbc:postgresql://localhost:5432/nyc_dw"
$env:PG_USER="nyc"
$env:PG_PASSWORD=Read-Host "Mot de passe PostgreSQL"
```

Le mot de passe doit être celui de la base PostgreSQL locale. Ne jamais commiter ces identifiants. Le fichier `build.sbt` doit déjà contenir le pilote JDBC PostgreSQL et les options Java pour Spark/Java 17 testées lors du pilote 100 courses.

## 3. Exécuter

```powershell
.\sbtw.ps1 "runMain cytech.ingestion.PostgresFull"
```

Sur le premier lancement, le programme imprime :

- le nombre de courses conservées dans RustFS ;
- le nombre admissible pour le schéma SQL ;
- le nombre chargé en staging ;
- le nombre transféré de manière atomique dans `fact_trip`.

Si le nombre de courses admissibles diffère du nombre initial, le programme s'arrête **sans modifier la table finale**. Il faut alors analyser les dates ou montants incompatibles avec le contrat PostgreSQL avant de reprendre.

## 4. Vérifier les résultats dans PostgreSQL

Depuis la racine de votre dépôt :

```powershell
docker compose exec -T postgres psql -U nyc -d nyc_dw -c "SELECT source_file, COUNT(*) FROM dw.fact_trip GROUP BY source_file ORDER BY source_file;"
```

Si les 3 724 888 courses sont toutes admissibles, le résultat attendu **avant suppression du pilote** est :

- `yellow_tripdata_2026-01.parquet` : 3 724 888
- `yellow_tripdata_2026-01.parquet#pilot100` : 100

Soit **3 724 988 lignes** au total, le pilote représentant un doublon volontaire pour le test d'intégration. Le vrai résultat peut différer si les données ont été modifiées.

Après vérification, supprimer uniquement le lot pilote :

```powershell
docker compose exec -T postgres psql -U nyc -d nyc_dw -c "DELETE FROM dw.fact_trip WHERE source_file = 'yellow_tripdata_2026-01.parquet#pilot100';"
```

Ne pas faire un `TRUNCATE dw.fact_trip` ni un `mode("overwrite")` directement sur la table finale : on perdrait les contraintes, l'historique ou les données d'autres mois.

## 5. Limites et suite

- Mode Spark `local[2]` pour les premiers tests ; les Workers Docker ne sont pas encore mobilisés.
- La table de staging fait temporairement une copie du lot, ce qui consomme de l'espace disque.
- Le chargement remplace le mois entier, sans identifiant stable de course ; c'est un niveau d'idempotence **par lot mensuel**.
- Avant d'intégrer plusieurs mois, paramétrer le mois / nom de fichier / chemin de traitement et ajouter une vérification des autres valeurs métier (prix, zones et dates).
- Regrouper ensuite les deux branches dans un job Spark commun, sans relire deux fois inutilement les données brutes.
