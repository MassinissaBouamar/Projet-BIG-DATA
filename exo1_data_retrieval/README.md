# Exercice 1 — Question 3 : HTTP vers RustFS

Les questions 1 et 2 sont déjà réalisées. Ce programme télécharge le Parquet de janvier
2026 et le dépose dans le bucket **existant** `nyc-raw`, via l'API S3 de RustFS
sur `http://localhost:9000`. Il ne crée pas de bucket et ne traite pas le contenu avec Spark.
Scala 2.13.17 et les dépendances Spark 4.2.0 du professeur sont conservés.
Ces dépendances Spark sont isolées dans la configuration SBT `sparkTemplate` :
Spark 4.2.0 apporte `scala-library` 2.13.18 et SBT refuse de la combiner au
compilateur 2.13.17. Elles ne font donc pas partie du classpath du programme HTTP/S3.
Pour écrire ultérieurement un programme Spark, il faudra d'abord aligner les
versions du compilateur et de la bibliothèque Scala avec celles de Spark.

## Exécuter sous Windows

Docker doit être démarré et RustFS accessible. Aucun redémarrage de conteneur ni
changement du compose n'est nécessaire. Utiliser le port **9000** (API), pas 9001 (console).

Depuis PowerShell, à la racine du dépôt :

```powershell
Set-Location .\exo1_data_retrieval

# Tests locaux : aucun identifiant ni accès au bucket n'est nécessaire.
powershell -NoProfile -ExecutionPolicy Bypass -File .\sbtw.ps1 test

# Saisir les identifiants configurés dans votre RustFS.
# Ils sont actuellement définis dans le docker-compose.yml du professeur.
$credential = Get-Credential -Message 'Identifiants S3 RustFS'
$env:AWS_ACCESS_KEY_ID = $credential.UserName
$env:AWS_SECRET_ACCESS_KEY = $credential.GetNetworkCredential().Password

$env:S3_ENDPOINT = 'http://localhost:9000'
$env:S3_BUCKET = 'nyc-raw'
# L'objet à la racine du bucket existe déjà sur ce PC : choisir une clé libre.
$env:S3_KEY = 'q3/yellow_tripdata_2026-01_mon_essai.parquet'

# Téléchargement, upload puis vérification complète.
powershell -NoProfile -ExecutionPolicy Bypass -File .\sbtw.ps1 run
```

Le lanceur `sbtw.ps1` utilise `JAVA_HOME`, puis Java dans le `PATH`, puis le Java
fourni avec IntelliJ dans `C:\Program Files\JetBrains`. Sur le PC examiné, ce
dernier est disponible. Un JDK 21 convient aussi. Pour le choisir explicitement :

```powershell
$env:JAVA_HOME = 'C:\chemin\vers\jdk-21'
```

Le lanceur télécharge SBT 1.10.11 depuis Maven Central et vérifie son SHA-256.
Il stocke les outils, dépendances et caches dans `.tools/`, ignoré par Git.
Le premier lancement nécessite Internet et peut prendre plusieurs minutes,
notamment pour les dépendances Spark du template. `-ExecutionPolicy Bypass`
concerne seulement le processus lancé, sans modifier la politique Windows globale.
Avec Java et SBT déjà installés, `sbt test` et `sbt run` conviennent également.

Résultat attendu :

```text
Transfert vers s3://nyc-raw/yellow_tripdata_2026-01.parquet ...
Upload vérifié : ... octets ; SHA-256 : ...
```

Dans la console RustFS sur `http://localhost:9001`, ouvrir `nyc-raw` pour voir l'objet.
Le succès du programme signifie aussi que la relecture S3 correspond au flux HTTP
en taille et SHA-256. Cette vérification relit tout le fichier depuis RustFS ; elle
ne le sauvegarde pas sur le disque et ne valide pas son schéma Parquet.

Après utilisation, retirer les secrets de la session :

```powershell
Remove-Item Env:AWS_ACCESS_KEY_ID, Env:AWS_SECRET_ACCESS_KEY -ErrorAction SilentlyContinue
Remove-Variable credential -ErrorAction SilentlyContinue
```

## Configuration

| Variable | Valeur par défaut / rôle |
| --- | --- |
| `AWS_ACCESS_KEY_ID` | Obligatoire : identifiant RustFS |
| `AWS_SECRET_ACCESS_KEY` | Obligatoire : clé secrète RustFS |
| `SOURCE_URL` | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet` |
| `S3_ENDPOINT` | `http://localhost:9000` |
| `S3_REGION` | `us-east-1`, utilisée pour signer les requêtes S3 |
| `S3_BUCKET` | `nyc-raw` (les underscores sont refusés) |
| `S3_KEY` | Nom de fichier extrait de l'URL : `yellow_tripdata_2026-01.parquet` |

Les variables sont lues dans l'environnement du processus. Aucun secret n'est
inscrit dans le code Scala. Un fichier `.env` **n'est pas chargé automatiquement** ;
les fichiers `.env*` sont ignorés par précaution. Les identifiants déjà présents
dans le compose du professeur n'ont pas été modifiés.

Si l'objet existe déjà, le programme s'arrête avant le téléchargement. Pour
conserver cet objet et faire un nouvel essai, choisir explicitement une clé libre :

```powershell
$env:S3_KEY = 'q3/yellow_tripdata_2026-01_essai.parquet'
powershell -NoProfile -ExecutionPolicy Bypass -File .\sbtw.ps1 run
```

## Comprendre le code pour le présenter

1. **`Config.scala`** lit et valide les paramètres. Les secrets sont lus séparément
   dans `Main`, sans être stockés dans l'objet de configuration.
2. **`Main.scala`** crée un client AWS SDK Java v2, compatible avec Scala, et le
   configure pour RustFS avec un endpoint local et des URL en *path style*
   (`http://localhost:9000/nyc-raw/...`). Il vérifie le bucket et l'absence de clé,
   ouvre le flux HTTP, appelle le transfert et affiche le résultat.
3. **`HttpSource.scala`** ouvre une connexion avec délais de connexion/lecture,
   exige HTTP 200, refuse un encodage modifiant les octets et vérifie la signature
   initiale `PAR1`. Les ressources HTTP sont fermées même en cas d'erreur.
4. **`Retriever.scala`** lit au maximum un bloc de 8 Mio à la fois, l'envoie avec
   `UploadPart` puis réutilise le tampon. L'empreinte SHA-256 est calculée pendant
   la lecture. La mémoire dédiée aux données reste bornée, indépendamment de la
   taille du fichier. Seuls les outils et les fichiers de compilation sont locaux.
5. Le flux peut avoir une taille HTTP connue ou inconnue. Si `Content-Length`
   est présent, il est comparé au nombre d'octets réellement lus **avant** la
   finalisation. Une source vide ou tronquée ne devient pas un objet final.
6. `CompleteMultipartUpload` assemble les blocs avec **`If-None-Match: *`** : un
   autre client ne doit pas pouvoir faire écraser sa clé entre le contrôle initial
   et la finalisation. Aucune variante qui écrase sans condition n'est tentée.
7. `HeadObject` vérifie la taille, puis `GetObject` relit l'objet et recalcule son
   SHA-256. L'ETag d'un multipart ne suffit pas pour cette comparaison. La relecture
   utilise `If-Match` sur l'ETag pour détecter un changement intervenu après le HEAD.

Le multipart est un peu plus explicite qu'un simple `PutObject`, mais permet des
reprises automatiques des requêtes S3 par le SDK sur le bloc courant, sans devoir
rejouer tout le flux HTTP et sans charger tout le fichier en mémoire.
La source HTTP n'est pas reprise : après une coupure, relancer le programme.

Références : [flux avec le SDK AWS](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/best-practices-s3-uploads.html)
et [écritures conditionnelles S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

## Erreurs et conservation des données

- En cas d'erreur avant finalisation, le programme abandonne uniquement le multipart
  qu'il vient de créer. Cela nettoie ses blocs temporaires, sans supprimer d'objet existant.
- Après finalisation, un échec de vérification conserve l'objet pour inspection.
- Un arrêt brutal du processus ou de Docker peut laisser des blocs temporaires.
  Aucun nettoyage global du bucket n'est lancé. Une finalisation dont la réponse
  réseau est perdue peut aussi avoir réussi : vérifier la console avant de relancer.
- HTTP 403 : vérifier les identifiants et les permissions. HTTP 404 S3 : vérifier
  le bucket. HTTP 409/412 : conflit d'écriture ou clé déjà présente. Un refus de
  permission n'est jamais interprété comme une preuve d'absence d'objet.
- Un échec produit un code de sortie non nul ; aucun message de succès ne précède
  la vérification. Le programme ne contient aucun appel `DeleteObject`.

La protection atomique repose sur le respect des écritures conditionnelles par
le serveur S3. Le programme conserve cette condition même si le serveur la refuse.
Le périmètre est le transfert du fichier indiqué, pas la validation du schéma ou
le traitement de ses données, qui relèvent des exercices suivants.

## Tests et fichiers ajoutés

`RetrieverSpec.scala` teste la configuration, un vrai serveur HTTP local (200,
404, faux Parquet, réponse sans taille), puis simule l'API S3 avec Mockito :
plusieurs blocs et ordre, taille et SHA-256, absence d'écrasement, refus d'accès,
source vide/tronquée, panne réseau, conflit à la finalisation, erreur de nettoyage
et échec de vérification. Ces tests ne contactent pas le vrai RustFS et ne créent
aucune donnée dans votre bucket. L'extension Mockito sélectionne les proxies
d'interfaces pour simuler `S3Client`.

`build.sbt` ajoute le SDK S3, son transport HTTP et les bibliothèques de test ;
`project/build.properties` fixe SBT ; `project/repositories` configure Maven Central
pour le lanceur Windows. Les exécutions et les tests utilisent un processus Java
séparé ; le code de sortie du programme est ainsi transmis à SBT puis à PowerShell.
L'encodage UTF-8 explicite conserve les accents dans les sorties Windows et le
binding SLF4J sans sortie évite les messages de logger non configuré du SDK.
Le `.gitignore` local exclut les caches, les sorties de
compilation, les secrets et les fichiers Parquet de cet exercice uniquement.
Les exercices 2 à 5, le compose et le `.gitignore` racine restent inchangés.

## Validation effectuée sur ce PC le 18 octobre 2026

- Compilation en Scala 2.13.17 avec SBT 1.10.11 et le Java 25.0.4 fourni par IntelliJ.
- **12 tests réussis**, dont les cas d'erreur décrits ci-dessus.
- Exécution réelle avec la clé par défaut : refus avant téléchargement, car
  `nyc-raw/yellow_tripdata_2026-01.parquet` existe déjà. Cet objet a été conservé.
- Exécution réelle avec une nouvelle clé :
  `s3://nyc-raw/q3/yellow_tripdata_2026-01_automatique.parquet`.
  Le fichier a été envoyé puis relu depuis RustFS : **64 165 080 octets**.
  SHA-256 vérifié : `8b3933fe6f0d7b6d8826613c0dd724edc680ff7c49e2bd4c7635c05102728637`.

Ce nouvel objet reste disponible dans RustFS. Aucun objet n'a été supprimé ni
écrasé, et aucun Parquet intermédiaire n'a été créé sur le disque.
Le travail reste non commité sur la branche `exo1-automatisation`.
