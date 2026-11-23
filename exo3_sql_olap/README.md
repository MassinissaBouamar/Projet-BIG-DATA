# Exercice 3 — Data Warehouse NYC Yellow Taxi (PostgreSQL)

## Objectif

Creer un modele decisionnel **en flocon** pour les donnees de courses de taxi issues de la branche 1 de l'exercice 2. `dw.fact_trip` contient les mesures d'une course ; les dimensions fournissent les libelles pour les analyses. La normalisation `dim_location -> dim_borough / dim_service_zone` constitue le flocon.

**Granularite :** un enregistrement brut de course = une ligne de fait. Le fichier NYC ne fournit pas d'ID de course unique ; `trip_id` est une cle technique PostgreSQL.

**Valeurs manquantes :** les dimensions `dim_vendor`, `dim_ratecode`, `dim_payment`, `dim_location` utilisent respectivement les cles sentinelles `-1` (codes) et `0` (zones) pour permettre d'inserer les courses ayant des champs de reference manquants ou invalides. La transformation Spark devra faire cette correspondance explicitement. Les dates et heures doivent correspondre a `dim_date` et `dim_hour`.

**Attention aux doublons :** executer deux fois la future ingestion Spark sans supprimer/remplacer le lot mensuel duplique les faits. La branche 2 devra etre idempotente, par exemple en rechargeant un mois dans une transaction.

## Fichiers

- `creation.sql` : schemas `dw` et `staging`, dimensions, table de faits, cles et index.
- `insertion.sql` : donnees de reference (codes officiels, dates 2020-2035, heures, valeurs inconnues).
- `import_zones.sql` : chargement de `taxi_zone_lookup.csv` officiel dans les dimensions geographiques.
- `verification.sql` : controles SQL apres creation.
- `postgres-compose-snippet.yml` : bloc a inserer manuellement dans `docker-compose.yml` a la racine.
- `.env.example` : exemple de configuration locale. Ne pas versionner `.env`.

## Mise en place sur Windows / PowerShell

Depuis la **racine du repo Git clone** :

1. Copier le bloc `postgres` du snippet dans la section `services:` du `docker-compose.yml`. Ajouter `postgres-data:` dans `volumes:` (ne pas effacer `rustfs-data:`).
2. Copier `.env.example` vers la racine comme `.env` et choisir un mot de passe local (ne jamais le committer).
3. Lancer `docker compose up -d postgres` puis `docker compose ps`.
4. Executer les scripts :

```powershell
docker compose exec -T postgres psql -U nyc -d nyc_dw -v ON_ERROR_STOP=1 -f /sql/creation.sql
docker compose exec -T postgres psql -U nyc -d nyc_dw -v ON_ERROR_STOP=1 -f /sql/insertion.sql
```

5. Telecharger le referentiel depuis la page officielle [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), lien **Taxi Zone Lookup Table (CSV)**. Il faut que le fichier soit nomme `taxi_zone_lookup.csv` sous `exo3_sql_olap/` sur la machine Windows.

6. Importer les zones puis verifier :

```powershell
docker compose exec -T postgres psql -U nyc -d nyc_dw -v ON_ERROR_STOP=1 -f /sql/import_zones.sql
docker compose exec -T postgres psql -U nyc -d nyc_dw -v ON_ERROR_STOP=1 -f /sql/verification.sql
```

`dw.fact_trip` reste vide pour l'instant : la prochaine etape sera la branche 2 Scala/Spark qui transformera et chargera les courses vers PostgreSQL via JDBC.

## Exemples de requetes a effectuer apres ingestion

```sql
-- Courses et montant total par type de paiement
SELECT p.payment_label, COUNT(*) AS nb_courses, SUM(f.total_amount) AS total_usd
FROM dw.fact_trip AS f
JOIN dw.dim_payment AS p ON p.payment_type = f.payment_type
GROUP BY p.payment_label
ORDER BY nb_courses DESC;

-- Zones de depart et arrondissements les plus frequents
SELECT b.borough_name, l.zone_name, COUNT(*) AS nb_courses
FROM dw.fact_trip AS f
JOIN dw.dim_location AS l ON l.location_id = f.pickup_location_id
JOIN dw.dim_borough AS b ON b.borough_id = l.borough_id
GROUP BY b.borough_name, l.zone_name
ORDER BY nb_courses DESC LIMIT 10;
```

## Decisions de conception a justifier dans le rapport

- Schéma en flocon pour normaliser les zones et leurs regroupements.
- Mesures negatives conservees pour les litiges et ajustements.
- `quality_status` pour conserver les lignes `A_VERIFIER` sans les assimiler aux lignes parfaitement valides.
- Calendrier et heures en dimensions pour faciliter les analyses OLAP.
- Cles sentinelles pour les dimensions dont les donnees sources sont manquantes.
- Dates TLC interpretees comme des horaires locaux non convertis en UTC.

Sources de reference : [NYC TLC Yellow Taxi Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) et [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
