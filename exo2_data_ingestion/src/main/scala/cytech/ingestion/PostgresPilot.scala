package cytech.ingestion

import java.sql.DriverManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Exercice 2, branche 2 : pilote d'insertion de 100 courses dans PostgreSQL.
 *
 * Lit le Parquet deja valide par la branche 1, adapte ses colonnes au
 * schema dw.fact_trip, verifie les zones connues et insere un lot de test.
 * Ne remplace pas encore le job multibranche definitif.
 */
object PostgresPilot {

  private val PilotSourceFile = "yellow_tripdata_2026-01.parquet#pilot100"

  def main(args: Array[String]): Unit = {
    val s3Endpoint = sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000")
    val s3Bucket = sys.env.getOrElse("S3_BUCKET", "nyc-raw")
    val s3AccessKey = sys.env.getOrElse(
      "AWS_ACCESS_KEY_ID", sys.error("AWS_ACCESS_KEY_ID manquant")
    )
    val s3SecretKey = sys.env.getOrElse(
      "AWS_SECRET_ACCESS_KEY", sys.error("AWS_SECRET_ACCESS_KEY manquant")
    )

    val jdbcUrl = sys.env.getOrElse(
      "PG_JDBC_URL", "jdbc:postgresql://localhost:5432/nyc_dw"
    )
    val pgUser = sys.env.getOrElse("PG_USER", "nyc")
    val pgPassword = sys.env.getOrElse(
      "PG_PASSWORD", sys.error("PG_PASSWORD manquant")
    )

    // Le serveur PostgreSQL est accessible via localhost car le Spark
    // de ce premier pilote s'execute sur Windows, en mode local.
    Class.forName("org.postgresql.Driver")
    val connection = DriverManager.getConnection(jdbcUrl, pgUser, pgPassword)
    try {
      val statement = connection.prepareStatement(
        "SELECT COUNT(*) FROM dw.fact_trip WHERE source_file = ?"
      )
      try {
        statement.setString(1, PilotSourceFile)
        val result = statement.executeQuery()
        try {
          result.next()
          require(
            result.getLong(1) == 0,
            "Le pilote a deja ete charge. Supprime le lot pilote avant de le relancer."
          )
        } finally result.close()
      } finally statement.close()
    } finally connection.close()

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Branche 2 - Pilote PostgreSQL")
      .master("local[2]")
      .config("spark.sql.session.timeZone", "America/New_York")
      .config("spark.hadoop.fs.s3a.endpoint", s3Endpoint)
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.access.key", s3AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", s3SecretKey)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("WARN")
      val parquetPath = s"s3a://$s3Bucket/processed/yellow_2026_01"
      val raw = spark.read.parquet(parquetPath)

      // Protections minimales du pilote : les cles de date doivent exister
      // dans dim_date (2020-2035), et la duree doit tenir dans INTEGER SQL.
      val eligible = raw
        .filter(col("statut_qualite").isin("VALIDE", "A_VERIFIER"))
        .filter(to_date(col("tpep_pickup_datetime"))
          .between("2020-01-01", "2035-12-31"))
        .filter(to_date(col("tpep_dropoff_datetime"))
          .between("2020-01-01", "2035-12-31"))
        .filter(col("trip_distance").isNotNull && col("trip_distance") >= 0)
        .filter(col("total_amount").isNotNull)
        .filter(expr(
          "timestampdiff(SECOND, tpep_pickup_datetime, " +
          "tpep_dropoff_datetime) BETWEEN 0 AND 2147483647"
        ))

      val sample = eligible.limit(100).cache()
      val sampleSize = sample.count()
      require(sampleSize == 100, s"Seulement $sampleSize courses eligibles")

      // Lecture des identifiants de zones reellement presents en PostgreSQL.
      // Une zone absente est remplacee par la zone sentinelle 0.
      val knownLocations = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "dw.dim_location")
        .option("user", pgUser)
        .option("password", pgPassword)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select(col("location_id"))

      val pickupLocations = broadcast(
        knownLocations.withColumnRenamed("location_id", "known_pickup_id")
      )
      val dropoffLocations = broadcast(
        knownLocations.withColumnRenamed("location_id", "known_dropoff_id")
      )

      val withLocations = sample
        .join(
          pickupLocations,
          col("PULocationID").cast("int") === col("known_pickup_id"),
          "left"
        )
        .join(
          dropoffLocations,
          col("DOLocationID").cast("int") === col("known_dropoff_id"),
          "left"
        )

      // Correspondance explicite entre le Parquet source et fact_trip.
      // trip_id et ingested_at sont generes par PostgreSQL : on les omet.
      val factDF = withLocations.select(
        to_date(lit("2026-01-01")).alias("source_month"),
        lit(PilotSourceFile).alias("source_file"),
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        date_format(col("tpep_pickup_datetime"), "yyyyMMdd")
          .cast("int").alias("pickup_date_key"),
        date_format(col("tpep_dropoff_datetime"), "yyyyMMdd")
          .cast("int").alias("dropoff_date_key"),
        hour(col("tpep_pickup_datetime"))
          .cast("smallint").alias("pickup_hour"),
        hour(col("tpep_dropoff_datetime"))
          .cast("smallint").alias("dropoff_hour"),
        coalesce(col("known_pickup_id"), lit(0))
          .cast("int").alias("pickup_location_id"),
        coalesce(col("known_dropoff_id"), lit(0))
          .cast("int").alias("dropoff_location_id"),
        when(col("VendorID").cast("int").isin(1, 2, 6, 7),
          col("VendorID").cast("smallint"))
          .otherwise(lit(-1).cast("smallint")).alias("vendor_id"),
        when(col("payment_type").cast("int").isin(0, 1, 2, 3, 4, 5, 6),
          col("payment_type").cast("smallint"))
          .otherwise(lit(-1).cast("smallint")).alias("payment_type"),
        when(col("RatecodeID").cast("int").isin(1, 2, 3, 4, 5, 6, 99),
          col("RatecodeID").cast("smallint"))
          .otherwise(lit(-1).cast("smallint")).alias("ratecode_id"),
        col("passenger_count").cast("int").alias("passenger_count"),
        when(col("store_and_fwd_flag").isin("Y", "N"),
          col("store_and_fwd_flag"))
          .otherwise(lit(null).cast("string")).alias("store_and_fwd_flag"),
        col("trip_distance").cast("double").alias("trip_distance"),
        expr(
          "timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime)"
        ).cast("int").alias("trip_duration_seconds"),
        col("fare_amount").cast("decimal(14,2)").alias("fare_amount"),
        col("tip_amount").cast("decimal(14,2)").alias("tip_amount"),
        col("total_amount").cast("decimal(14,2)").alias("total_amount"),
        col("statut_qualite").alias("quality_status"),
        coalesce(col("alertes_qualite"), lit("")).alias("quality_alerts")
      ).cache()

      println("=== APERCU DES DONNEES PRETES POUR POSTGRESQL ===")
      factDF.select(
        "pickup_datetime", "pickup_location_id", "vendor_id",
        "trip_distance", "total_amount", "quality_status"
      ).show(5, truncate = false)
      require(factDF.count() == 100, "La transformation a modifie le nombre de lignes")

      // Append conserve les tables SQL, leurs contraintes et la sequence trip_id.
      // Un seul writer JDBC suffit pour le lot pilote de 100 lignes.
      factDF.coalesce(1).write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "dw.fact_trip")
        .option("user", pgUser)
        .option("password", pgPassword)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "100")
        .mode("append")
        .save()

      println("=== PILOTE TERMINE : 100 lignes envoyees a dw.fact_trip ===")
    } finally {
      spark.stop()
    }
  }
}
