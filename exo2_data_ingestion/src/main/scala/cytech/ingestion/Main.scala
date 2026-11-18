package cytech.ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    // 1. Paramètres de connexion à RustFS
    val endpoint = sys.env.getOrElse(
      "S3_ENDPOINT",
      "http://localhost:9000"
    )
    val bucket = sys.env.getOrElse("S3_BUCKET", "nyc-raw")
    val key = sys.env.getOrElse(
      "S3_KEY",
      "yellow_tripdata_2026-01.parquet"
    )

    val accessKey = sys.env.getOrElse(
      "AWS_ACCESS_KEY_ID",
      sys.error("AWS_ACCESS_KEY_ID manquant")
    )
    val secretKey = sys.env.getOrElse(
      "AWS_SECRET_ACCESS_KEY",
      sys.error("AWS_SECRET_ACCESS_KEY manquant")
    )

    // 2. Création de la session Spark
    val spark = SparkSession.builder()
      .appName("NYC Taxi - Exercice 2")
      .master("local[2]")
      .config("spark.hadoop.fs.s3a.endpoint", endpoint)
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.access.key", accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", secretKey)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
      .getOrCreate()

    try {
      // 3. Lecture du Parquet depuis RustFS
      val source = s"s3a://$bucket/$key"
      val rawDF = spark.read.parquet(source)

      // 4. Affichage du schéma
      println("=== SCHEMA DES DONNEES ===")
      rawDF.printSchema()

      // 5. Affichage de quelques courses
      println("=== APERCU DES COURSES ===")
      rawDF.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "total_amount"
      ).show(5, truncate = false)

      // 6. Comptage des lignes
      println("=== NOMBRE DE COURSES ===")
      val total = rawDF.count()
      println(s"Nombre total de courses : $total")

      println("=== ANALYSE QUALITE DES DONNEES ===")
      val diagnostic = rawDF.agg(
        sum(
          when(col("tpep_pickup_datetime").isNull, 1)
            .otherwise(0)
        ).alias("dates_depart_manquantes"),
        sum(
          when(col("tpep_dropoff_datetime").isNull, 1)
            .otherwise(0)
        ).alias("dates_arrivee_manquantes"),
        sum(
          when(
            col("tpep_dropoff_datetime") < col("tpep_pickup_datetime"),
            1
          ).otherwise(0)
        ).alias("dates_incoherentes"),
        sum(
          when(col("trip_distance") <= 0, 1)
            .otherwise(0)
        ).alias("distances_nulles_ou_negatives"),
        sum(
          when(col("total_amount") < 0, 1)
            .otherwise(0)
        ).alias("montants_negatifs")
      )

      diagnostic.show(1, truncate = 0, vertical = true)

      println("=== ANALYSE DETAILLEE DISTANCES & MONTANTS ===")
      val analyse = rawDF.agg(
        sum(when(col("trip_distance") === 0, 1).otherwise(0))
          .alias("distance_egale_zero"),
        sum(when(col("trip_distance") < 0, 1).otherwise(0))
          .alias("distance_negative"),
        sum(when(col("trip_distance").isNull, 1).otherwise(0))
          .alias("distance_manquante"),
        sum(when(col("total_amount") < 0, 1).otherwise(0))
          .alias("montant_negatif"),
        sum(
          when(
            (col("trip_distance") <= 0) &&
            (col("total_amount") < 0),
            1
          ).otherwise(0)
        ).alias("distance_et_montant_anormaux")
      )

      analyse.show(1, truncate = 0, vertical = true)

      // Explorer les types de paiement des montants négatifs
      println("=== PAIEMENTS AVEC MONTANTS NEGATIFS ===")
      rawDF
        .filter(col("total_amount") < 0)
        .groupBy("payment_type")
        .count()
        .orderBy(col("count").desc)
        .show()

println("=== DISTANCES NULLES PAR TYPE DE PAIEMENT ===")

rawDF
  .filter(col("trip_distance") === 0)
  .groupBy("payment_type")
  .count()
  .orderBy(col("count").desc)
  .show(20, truncate = false)

println("=== EXEMPLES DE COURSES A DISTANCE NULLE ===")

rawDF
  .filter(col("trip_distance") === 0)
  .select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "payment_type"
  )
  .show(10, truncate = false)


println("=== VALIDATION METIER ===")

// Appliquer les regles de qualite au DataFrame brut
val qualityDF = QualityRules.validate(rawDF)

// Examiner la repartition des statuts
println("=== REPARTITION DES STATUTS ===")

qualityDF
  .groupBy("statut_qualite")
  .count()
  .orderBy("statut_qualite")
  .show(truncate = false)

println("=== ANALYSE DU NOMBRE DE PASSAGERS ===")

val diagnosticPassagers = qualityDF.agg(

  sum(
    when(col("passenger_count").isNull, 1)
      .otherwise(0)
  ).alias("passagers_manquants"),

  sum(
    when(col("passenger_count") === 0, 1)
      .otherwise(0)
  ).alias("passagers_zero"),

  sum(
    when(col("passenger_count") < 0, 1)
      .otherwise(0)
  ).alias("passagers_negatifs")

)

diagnosticPassagers.show(1, truncate = 0, vertical = true)

println("=== PASSAGERS INCONNUS PAR FOURNISSEUR ===")

qualityDF
  .filter(
    col("passenger_count").isNull ||
    col("passenger_count") <= 0
  )
  .groupBy("VendorID")
  .count()
  .orderBy(col("count").desc)
  .show(20, truncate = false)

println("=== REPARTITION DES ALERTES ===")

val alertesParType = qualityDF

  // Garder uniquement les lignes avec alertes
  .filter(col("alertes_qualite") =!= "")

  // Separer les alertes multiples
  .withColumn(
    "alerte",
    explode(
      split(col("alertes_qualite"), "\\s*\\|\\s*")
    )
  )

  // Compter les occurrences de chaque alerte
  .groupBy("alerte")
  .count()
  .orderBy(col("count").desc)

alertesParType.show(30, truncate = false)

println("=== DIAGNOSTIC DES VALEURS MANQUANTES ===")

val diagnosticmanquantes = qualityDF.agg(

  sum(when(col("passenger_count").isNull, 1)
    .otherwise(0)).alias("passagers_null"),

  sum(when(col("passenger_count") === 0, 1)
    .otherwise(0)).alias("passagers_zero"),

  sum(when(col("passenger_count") < 0, 1)
    .otherwise(0)).alias("passagers_negatifs"),

  sum(when(col("RatecodeID").isNull, 1)
    .otherwise(0)).alias("tarifs_null"),

  sum(when(
    col("RatecodeID").isNotNull &&
    !col("RatecodeID").isin(1, 2, 3, 4, 5, 6, 99),
    1
  ).otherwise(0)).alias("tarifs_invalides"),

  sum(when(col("store_and_fwd_flag").isNull, 1)
    .otherwise(0)).alias("flags_null"),

  sum(when(
    col("store_and_fwd_flag").isNotNull &&
    !col("store_and_fwd_flag").isin("Y", "N"),
    1
  ).otherwise(0)).alias("flags_invalides"),

  sum(when(
    col("RatecodeID").isNull &&
    col("store_and_fwd_flag").isNull,
    1
  ).otherwise(0)).alias("tarif_et_flag_null")
)

diagnosticmanquantes.show(1, truncate = 0, vertical = true)


// Afficher quelques exemples de courses suspectes
println("=== EXEMPLES D'ANOMALIES ===")

qualityDF
  .filter(col("alertes_qualite") =!= "")
  .select(
    "trip_distance",
    "total_amount",
    "payment_type",
    "alertes_qualite",
    "statut_qualite"
  )
  .show(10, truncate = false)

// Separer les lignes conservees et rejetees
val cleanedDF = qualityDF.filter(
  col("statut_qualite") =!= "REJETE"
)

val rejectedDF = qualityDF.filter(
  col("statut_qualite") === "REJETE"
)

// Controle de coherence des nombres
val total2= rawDF.count()
val conserved = cleanedDF.count()
val rejected = rejectedDF.count()

println(s"Total initial : $total")
println(s"Lignes conservees : $conserved")
println(s"Lignes rejetees : $rejected")

require(
  total == conserved + rejected,
  "Erreur : certaines lignes ont disparu !"
)

// Export des donnees
val cleanPath =
  s"s3a://$bucket/processed/yellow_2026_01"

val rejectedPath =
  s"s3a://$bucket/quarantine/yellow_2026_01"

cleanedDF.write
  .mode("overwrite")
  .parquet(cleanPath)

rejectedDF.write
  .mode("overwrite")
  .parquet(rejectedPath)

println("=== EXPORT RUSTFS TERMINE ===")

println("=== VERIFICATION DES EXPORTS ===")

// Relire les fichiers depuis RustFS
val verifiedCleanDF = spark.read.parquet(cleanPath)
val verifiedRejectedDF = spark.read.parquet(rejectedPath)

// Compter les lignes relues
val nbClean = verifiedCleanDF.count()
val nbRejected = verifiedRejectedDF.count()

println(s"Courses nettoyees dans RustFS : $nbClean")
println(s"Courses rejetees dans RustFS : $nbRejected")

// Verifier qu'aucune ligne n'a ete perdue
require(
  nbClean + nbRejected == rawDF.count(),
  "ERREUR : nombre de lignes incoherent"
)

// Verifier les statuts des courses sauvegardees
verifiedCleanDF
  .groupBy("statut_qualite")
  .count()
  .show()

println("Verification des exports reussie !")






    } finally {
      spark.stop()
    }
  }
}