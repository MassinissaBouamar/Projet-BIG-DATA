BRANCHE 2 - PILOTE POSTGRESQL (100 COURSES)

1. Copier le fichier PostgresPilot.scala dans votre depot clone :
   exo2_data_ingestion/src/main/scala/cytech/ingestion/PostgresPilot.scala
2. Ajouter dans exo2_data_ingestion/build.sbt (ne pas remplacer les dependances existantes) :
   libraryDependencies += "org.postgresql" % "postgresql" % "42.7.14"
3. Depuis PowerShell, sous la racine du depot :
   docker compose ps
   $env:AWS_ACCESS_KEY_ID="rustfsadmin"
   $env:AWS_SECRET_ACCESS_KEY="rustfsadmin"
   $env:S3_ENDPOINT="http://localhost:9000"
   $env:S3_BUCKET="nyc-raw"
   $env:PG_JDBC_URL="jdbc:postgresql://localhost:5432/nyc_dw"
   $env:PG_USER="nyc"
   $env:PG_PASSWORD=Read-Host "Mot de passe PostgreSQL"
   cd exo2_data_ingestion
   .\sbtw.ps1 "runMain cytech.ingestion.PostgresPilot"
4. Verifier depuis la racine du depot :
   docker compose exec -T postgres psql -U nyc -d nyc_dw -v ON_ERROR_STOP=1 -f /sql/verifier_pilot.sql

Le programme refuse de relancer le meme lot sans nettoyage pour eviter les doublons.
Pour supprimer UNIQUEMENT les 100 lignes du pilote puis recommencer :
   docker compose exec postgres psql -U nyc -d nyc_dw -c "DELETE FROM dw.fact_trip WHERE source_file = 'yellow_tripdata_2026-01.parquet#pilot100';"

Ce pilote ecrit dans dw.fact_trip, et non dans une table temporaire. Ne pas
utiliser overwrite : cela risquerait de remplacer le schema et les contraintes.
La version finale doit rassembler les deux branches dans un meme job Spark,
traiter toutes les donnees, et assurer une ingestion mensuelle idempotente.
