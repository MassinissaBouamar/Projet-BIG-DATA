# Exercice 2 — Pipeline Spark unique à deux branches (janvier 2026)

Ce fichier complète le README de l'exercice 2. Le nouveau `UnifiedPipeline.scala` est **un seul programme Spark** qui lit le **Parquet brut** de janvier 2026 sur RustFS, applique les règles de `QualityRules.scala`, puis utilise les lignes contrôlées dans deux branches :

```
s3a://nyc-raw/yellow_tripdata_2026-01.parquet  (BRUT, jamais écrasé)
                   |
             QualityRules
                   |
                   +---- branche 1 ---> s3a://nyc-raw/processed/yellow_2026_01/
                   |                   s3a://nyc-raw/quarantine/yellow_2026_01/
                   |
                   +---- branche 2 ---> transformation en 22 colonnes SQL
                                      ---> staging.fact_trip_load (JDBC)
                                      ---> transaction SQL : remplacement
                                           du lot janvier dans dw.fact_trip
```

`QualityRules.scala`, `Main.scala`, `PostgresPilot.scala` et `PostgresFull.scala` ne sont **pas remplacés**. Pour la démonstration finale, utiliser `UnifiedPipeline`.

## Pré-requis

- Docker Desktop démarré ; services `rustfs` et `postgres` opérationnels.
- PostgreSQL contient les tables/dimensions de l'exercice 3, dont les valeurs de substitution et les 266 zones.
- Fichier brut présent dans le bucket `nyc-raw` à la racine.
- `build.sbt` contient Scala `2.13.18` (ou version compatible ultérieure), Spark `4.2.0`, ScalaTest et le pilote JDBC PostgreSQL.
- Les options Java 17 validées lors de `PostgresFull` sont présentes, en particulier :

```scala
Compile / run / fork := true
Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)
```

Ces options peuvent déjà figurer dans le `build.sbt` : **ne pas les dupliquer**.

## 1. Configuration PowerShell

Depuis le dossier `exo2_data_ingestion` :

```powershell
$env:AWS_ACCESS_KEY_ID="rustfsadmin"
$env:AWS_SECRET_ACCESS_KEY="rustfsadmin"
$env:S3_ENDPOINT="http://localhost:9000"
$env:S3_BUCKET="nyc-raw"
$env:PG_JDBC_URL="jdbc:postgresql://localhost:5432/nyc_dw"
$env:PG_USER="nyc"
$env:PG_PASSWORD=Read-Host "Mot de passe PostgreSQL"
```

Les identifiants `rustfsadmin` sont ceux du Docker Compose **de développement local** ; utiliser les vrais paramètres si le projet a été personnalisé. Ne pas enregistrer de mot de passe dans le code ou dans un fichier Git.

## 2. Première vérification non destructive

```powershell
.\sbtw.ps1 "runMain cytech.ingestion.UnifiedPipeline --dry-run"
```

Cette commande **lit et calcule**, mais **n'écrit ni Parquet ni lignes SQL**. Elle acquiert brièvement un verrou de contrôle PostgreSQL. Avec le dataset étudié, on devrait retrouver :

- 3 724 889 lignes brutes ;
- 3 724 888 lignes conservées ;
- 1 ligne rejetée ;
- 3 724 888 lignes compatibles avec le schéma SQL.

Si le nombre diffère, inspecter le diagnostic avant d'exécuter le chargement complet.

## 3. Exécuter les deux branches

Cette opération **réécrit** les préfixes `processed/yellow_2026_01` et `quarantine/yellow_2026_01` et **remplace** le lot de janvier 2026 dans `dw.fact_trip`. Le fichier brut n'est pas touché, et les autres mois ne sont pas supprimés.

```powershell
.\sbtw.ps1 "runMain cytech.ingestion.UnifiedPipeline"
```

Messages attendus (si les données n'ont pas changé) :

```text
=== SOURCE BRUTE : 3724889 COURSES ===
=== CONSERVEES : 3724888 ; REJETEES : 1 ===
=== BRANCHE 1 OK : 3724888 + 1 ===
Lignes en staging : 3724888
=== CHARGEMENT COMMIT : 3724888 COURSES ===
=== PIPELINE UNIFIE TERMINE ===
```

**Attention :** ce chargement complet prend du temps et écrit à nouveau 3,7 millions de lignes SQL. Ne le lancer qu'une fois après le dry-run. La table `staging.fact_trip_load` peut nécessiter de l'espace disque supplémentaire.

## 4. Vérifier en SQL

Depuis la racine du dépôt :

```powershell
docker compose exec -T postgres psql -U nyc -d nyc_dw -v ON_ERROR_STOP=1 -f /sql/verification_unifie.sql
```

Ou bien, depuis n'importe quel dossier du dépôt :

```powershell
docker compose exec -T postgres psql -U nyc -d nyc_dw -c "SELECT source_file, COUNT(*) FROM dw.fact_trip GROUP BY source_file ORDER BY source_file;"
```

Une fois le pilote supprimé, seule la ligne source `yellow_tripdata_2026-01.parquet` devrait apparaître (3 724 888 cours​es si le dataset est inchangé).

## 5. Idempotence et limites

- **Rejouable par lot** : la transaction SQL supprime et réinsère uniquement les lignes de ce fichier source ; un deuxième lancement réussi n'ajoute pas de doublons. En revanche, `trip_id` et `ingested_at` sont régénérés (pas de stabilité d'identifiant à la ligne).
- **Staging** : le tableau `staging.fact_trip_load` est une table persistante de travail, **pas** une table temporaire par session. Le verrou PostgreSQL empêche deux lancements simultanés qui utilisent cette convention.
- **Pas de transaction distribuée RustFS + PostgreSQL** : si la branche 2 échoue, PostgreSQL conserve l'ancien lot alors que la branche 1 peut déjà avoir remplacé ses Parquet. Une reprise complète permet de les réaligner.
- **Mode local** : Spark utilise `local[2]`, pas les Workers Docker. C'est fonctionnel pour le TP ; la mise en cluster réclame de revoir les endpoints et le déploiement.
- **Mois fixe** : pour l'instant, ce programme traite uniquement janvier 2026. Il faudra paramétrer `SourceFile`, `SourceMonth`, les chemins et le verrou pour traiter plusieurs mois.
- **Règles de qualité** : celles écrites dans `QualityRules.scala` sont une première version ; la validation exhaustive du dictionnaire et des zones reste à documenter.

## 6. Travail en équipe / Git

Depuis la racine du dépôt :

```powershell
git status
git add exo2_data_ingestion/src/main/scala/cytech/ingestion/UnifiedPipeline.scala exo2_data_ingestion/README_PIPELINE_UNIFIE.md exo3_sql_olap/verification_unifie.sql
git diff --cached --stat
git commit -m "feat(exo2): pipeline Spark unifie RustFS et PostgreSQL"
```

Créer une branche/PR si l'équipe travaille à plusieurs. Ne jamais versionner des `*.parquet`, des dossiers `target/`, ni des fichiers `.env`.
