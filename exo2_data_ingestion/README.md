# Exercice 2 — Nettoyage et ingestion multibranche

## Objectif

Lire les trajets des taxis jaunes de New York depuis un stockage objet **RustFS** au format **Parquet**, les valider avec **Scala / Apache Spark**, puis produire deux branches :

1. **Branche 1 — implémentée (première version)** : Parquet conservé et enrichi avec des indicateurs de qualité pour une exploitation ultérieure en Machine Learning ; enregistrements rejetés placés en quarantaine.
2. **Branche 2 — à développer** : transformation des données et chargement du Data Warehouse **PostgreSQL**, après définition du modèle multidimensionnel dans l'exercice 3.

## Structure

```text
exo2_data_ingestion/
├── build.sbt
├── sbtw.ps1
├── project/
└── src/
    ├── main/scala/cytech/ingestion/
    │   ├── Main.scala           # Orchestration : lecture, analyses, export, relecture
    │   └── QualityRules.scala   # Contrôles et classification des enregistrements
    └── test/scala/cytech/ingestion/
        └── QualityRulesSpec.scala # Test ScalaTest sur des courses synthétiques
```

## Prérequis et lancement (Windows / PowerShell)

Prérequis : Docker Desktop, JDK 17, RustFS lancé, fichier source présent, Scala/SBT configuré. La version du compilateur Scala a été mise à jour en **2.13.18** pour être compatible avec les dépendances Spark **4.2.0** du module.

Démarrer RustFS depuis la racine du dépôt :

```powershell
docker compose up -d rustfs
```

Un bucket `nyc-raw` doit contenir `yellow_tripdata_2026-01.parquet`.

Configurer les variables dans le terminal qui exécutera SBT :

```powershell
$env:AWS_ACCESS_KEY_ID="<ACCESS_KEY_RUSTFS>"
$env:AWS_SECRET_ACCESS_KEY="<SECRET_KEY_RUSTFS>"
$env:S3_ENDPOINT="http://localhost:9000"
$env:S3_BUCKET="nyc-raw"
$env:S3_KEY="yellow_tripdata_2026-01.parquet"
```

Depuis le dossier `exo2_data_ingestion` :

```powershell
.\sbtw.ps1 test
.\sbtw.ps1 run
```

`test` exécute les tests unitaires. `run` exécute le pipeline sur les données réelles de RustFS. **Ne jamais placer d'identifiants de production dans les sources ou dans Git.**

## Pipeline de la branche 1

1. Créer une `SparkSession` locale (`local[2]`).
2. Lire `s3a://nyc-raw/yellow_tripdata_2026-01.parquet` dans un DataFrame.
3. Contrôler le schéma et les champs indispensables.
4. Ajouter les colonnes `motifs_rejet`, `alertes_qualite`, `statut_qualite`.
5. Séparer les enregistrements rejetés des données conservées.
6. Exporter au format Parquet dans deux préfixes RustFS.
7. Relire les deux sorties et comparer les nombres de lignes.

### Classification de qualité

| Statut | Signification | Destination |
|---|---|---|
| `VALIDE` | Aucun problème signalé par les règles actuelles | `processed/` |
| `A_VERIFIER` | Valeur absente ou atypique, sans anomalie bloquante | `processed/` avec alertes |
| `REJETE` | Erreur bloquante selon la politique retenue | `quarantine/` avec motifs |

**Rejets bloquants** : date de départ ou d'arrivée manquante, arrivée avant le départ, distance manquante ou négative, montant total manquant.

**Alertes non bloquantes** : distance nulle, montant négatif, nombre de passagers nul/manquant/négatif, codes fournisseur, tarif ou paiement manquants/inconnus, indicateur `store_and_fwd_flag` manquant/invalide, identifiants de zones manquants/non positifs.

Les codes autorisés sont issus du dictionnaire NYC TLC et de la politique qualité de notre projet. Cette validation est **non exhaustive** : la présence dans le référentiel officiel de zones, les autres champs tarifaires et certains cas limites restent à tester.

### Pourquoi ne pas supprimer toutes les alertes ?

Certaines courses à distance nulle ou à montant négatif ont une signification métier possible (ajustements, litiges, etc.). Des champs manquants peuvent coexister avec des informations utiles pour l'analyse. Les enregistrements `A_VERIFIER` restent donc dans le Parquet conservé, mais sont identifiés explicitement.

Aucune valeur manquante n'est imputée arbitrairement.

## Résultats des essais — janvier 2026

| Mesure | Nombre |
|---|---:|
| Courses lues | 3 724 889 |
| `VALIDE` | 2 552 515 |
| `A_VERIFIER` | 1 172 373 |
| `REJETE` | 1 |
| Courses conservées dans `processed/` | 3 724 888 |

Principaux diagnostics observés :

| Diagnostic | Occurrences |
|---|---:|
| `passenger_count` manquant | 1 088 058 |
| `passenger_count` égal à 0 | 14 787 |
| `RatecodeID` manquant | 1 088 058 |
| `store_and_fwd_flag` manquant | 1 088 058 |
| Distance égale à 0 | 125 738 |
| Montant total négatif | 39 984 |

Les valeurs manquantes de `RatecodeID` et `store_and_fwd_flag` concernent les mêmes 1 088 058 lignes. Ces statistiques proviennent du diagnostic exécuté localement ; les relancer après tout changement de règles.

### Chemins de sortie

```text
s3a://nyc-raw/processed/yellow_2026_01/
s3a://nyc-raw/quarantine/yellow_2026_01/
```

Spark écrit des fichiers `part-*.parquet` sous ces préfixes. Le fichier brut n'est pas modifié. L'écriture actuelle emploie `overwrite` **sur les préfixes de sortie**, ce qui convient pour rejouer le même mois en développement.

## Tests et contrôle d'intégrité

`QualityRulesSpec.scala` construit trois courses fictives :

- une course valide → `VALIDE` ;
- une course à informations manquantes → `A_VERIFIER` ;
- une course à distance négative → `REJETE`.

Les assertions vérifient la classification. Les sorties réelles sont relues depuis RustFS afin de s'assurer que :

```text
nombre_initial = nombre_conserve + nombre_rejete
```

Un test réussi n'équivaut pas encore à une couverture exhaustive ; enrichir la suite avec les cas limites, les codes de référence et les comportements de nullité.

## Limites et travaux à poursuivre

- Développer un rapport de qualité agrégé et automatisé, et des tests pour chaque famille de règles.
- Vérifier les zones à partir du référentiel NYC TLC ; contrôler davantage de colonnes du contrat.
- Prévoir le traitement de plusieurs mois, avec paramètres date/chemins et une politique d'écriture adaptée.
- Définir le modèle OLAP (exercice 3), puis compléter la **branche 2 PostgreSQL**.
- Exécuter et valider le vrai déploiement Spark distribué ; le code actuel utilise une session locale.

## Références

- Jeux de données : <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- Dictionnaire NYC Yellow Taxi : <https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf>
