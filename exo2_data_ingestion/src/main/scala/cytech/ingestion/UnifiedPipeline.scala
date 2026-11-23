package cytech.ingestion

import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
 * Un seul job Spark : validation, sortie Parquet et ingestion PostgreSQL.
 *
 * Strategie :
 * 1. Lecture des donnees brutes dans RustFS et application de QualityRules.
 * 2. Branche 1 : Parquet conserve + quarantaine dans RustFS.
 * 3. Branche 2 : transformation depuis le meme DataFrame, staging JDBC.
 * 4. Remplacement du lot dans fact_trip dans UNE transaction SQL.
 *
 * En cas d'echec JDBC, fact_trip n'est pas modifiee.
 * Les deux destinations RustFS et PostgreSQL ne forment pas une
 * transaction globale : une erreur tardive peut laisser la sortie RustFS
 * mise a jour et la table PostgreSQL inchangee.
 *
 * Precondition : dw.fact_trip et les dimensions existent deja.
 * A executer seul (pas en concurrence avec une autre ingestion mensuelle).
 */
object UnifiedPipeline {

  private val SourceFile = "yellow_tripdata_2026-01.parquet"
  private val SourceMonth = "2026-01-01"
  private val StagingTable = "staging.fact_trip_load"
  private val FinalTable = "dw.fact_trip"

  // Colonnes uniquement : trip_id et ingested_at sont generes par Postgres.
  private val FactColumns = Seq(
    "source_month", "source_file", "pickup_datetime", "dropoff_datetime",
    "pickup_date_key", "dropoff_date_key", "pickup_hour", "dropoff_hour",
    "pickup_location_id", "dropoff_location_id", "vendor_id", "payment_type",
    "ratecode_id", "passenger_count", "store_and_fwd_flag", "trip_distance",
    "trip_duration_seconds", "fare_amount", "tip_amount", "total_amount",
    "quality_status", "quality_alerts"
  )

  private def scalarLong(c: Connection, sql: String): Long = {
    val stmt = c.createStatement()
    try {
      val rs = stmt.executeQuery(sql)
      try {
        require(rs.next(), s"Aucune valeur renvoyee pour : $sql")
        rs.getLong(1)
      } finally rs.close()
    } finally stmt.close()
  }

  private def execute(c: Connection, sql: String): Unit = {
    val stmt = c.createStatement()
    try { stmt.execute(sql); () }
    finally stmt.close()
  }

  def main(args: Array[String]): Unit = {
    // --dry-run : aucun fichier ecrit, aucune table modifiee.
    val dryRun = args.contains("--dry-run")
    require(
      args.forall(_ == "--dry-run"),
      "Option inconnue. Utiliser --dry-run ou aucun argument."
    )

    val endpoint = sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000")
    val bucket = sys.env.getOrElse("S3_BUCKET", "nyc-raw")
    val accessKey = sys.env.getOrElse(
      "AWS_ACCESS_KEY_ID", sys.error("AWS_ACCESS_KEY_ID manquant")
    )
    val secretKey = sys.env.getOrElse(
      "AWS_SECRET_ACCESS_KEY", sys.error("AWS_SECRET_ACCESS_KEY manquant")
    )
    val jdbcUrl = sys.env.getOrElse(
      "PG_JDBC_URL", "jdbc:postgresql://localhost:5432/nyc_dw"
    )
    val user = sys.env.getOrElse("PG_USER", "nyc")
    val password = sys.env.getOrElse(
      "PG_PASSWORD", sys.error("PG_PASSWORD manquant")
    )

    Class.forName("org.postgresql.Driver")
    val control = DriverManager.getConnection(jdbcUrl, user, password)
    var spark: SparkSession = null
    var locked = false

    try {
      // Un seul job a la fois utilise cette table de staging partagee.
      locked = scalarLong(
        control,
        "SELECT CASE WHEN pg_try_advisory_lock(202601) THEN 1 ELSE 0 END"
      ) == 1
      require(locked, "Une autre ingestion de janvier 2026 est en cours")

      spark = SparkSession.builder()
        .appName("NYC Taxi - Un seul job Spark, deux branches")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "America/New_York")
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

      spark.sparkContext.setLogLevel("WARN")

      // Lecture UNIQUE de la source brute et construction du graphe
      // commun aux deux branches. DISK_ONLY evite d'occuper toute la RAM.
      val rawPath = s"s3a://$bucket/$SourceFile"
      val processedPath = s"s3a://$bucket/processed/yellow_2026_01"
      val rejectedPath = s"s3a://$bucket/quarantine/yellow_2026_01"
      val raw = spark.read.parquet(rawPath)
      val quality = QualityRules.validate(raw).persist(StorageLevel.DISK_ONLY)
      val cleaned = quality.filter(col("statut_qualite") =!= "REJETE")
      val rejected = quality.filter(col("statut_qualite") === "REJETE")

      val originalCount = quality.count()
      val total = cleaned.count()
      val rejectedCount = rejected.count()
      require(
        originalCount == total + rejectedCount,
        "Perte de lignes entre donnees brutes et donnees classees"
      )
      println(s"=== SOURCE BRUTE : $originalCount COURSES ===")
      println(s"=== CONSERVEES : $total ; REJETEES : $rejectedCount ===")
      quality.groupBy("statut_qualite").count().show(truncate = false)

      // S'il existe des dates hors du calendrier SQL ou des valeurs qui ne
      // tiendraient pas en base, on bloque plutot que de perdre des lignes.
      val eligible = cleaned.filter(
        to_date(col("tpep_pickup_datetime"))
          .between("2020-01-01", "2035-12-31") &&
        to_date(col("tpep_dropoff_datetime"))
          .between("2020-01-01", "2035-12-31") &&
        col("trip_distance").isNotNull &&
        col("trip_distance") >= 0 &&
        col("total_amount").isNotNull &&
        expr("try_cast(total_amount AS DECIMAL(14,2)) IS NOT NULL") &&
        expr("fare_amount IS NULL OR try_cast(fare_amount AS DECIMAL(14,2)) IS NOT NULL") &&
        expr("tip_amount IS NULL OR try_cast(tip_amount AS DECIMAL(14,2)) IS NOT NULL") &&
        expr("timestampdiff(SECOND, tpep_pickup_datetime, " +
          "tpep_dropoff_datetime) BETWEEN 0 AND 2147483647")
      )

      val eligibleCount = eligible.count()
      println(s"Courses eligibles au schema SQL : $eligibleCount")
      require(
        eligibleCount == total,
        s"${total - eligibleCount} courses ne respectent pas les contraintes " +
        "du Data Warehouse. Aucun chargement effectue : analyser ces lignes."
      )

      if (dryRun) {
        println(
          s"=== DRY-RUN OK : $originalCount lignes, $total conservees, " +
          s"$rejectedCount rejetees, $eligibleCount compatibles SQL ==="
        )
        return
      }

      // Branche 1 : ecriture des deux sorties RustFS a partir du meme
      // DataFrame valide. Le fichier Parquet brut n'est jamais modifie.
      println("=== BRANCHE 1 : ECRITURE PARQUET VERS RUSTFS ===")
      cleaned.write.mode("overwrite").parquet(processedPath)
      rejected.write.mode("overwrite").parquet(rejectedPath)

      val savedClean = spark.read.parquet(processedPath).count()
      val savedRejected = spark.read.parquet(rejectedPath).count()
      require(
        savedClean == total && savedRejected == rejectedCount,
        s"Relecture RustFS incoherente : $savedClean / $savedRejected"
      )
      println(s"=== BRANCHE 1 OK : $savedClean + $savedRejected ===")

      // Branche 2 : meme DataFrame `eligible`, sans relire le Parquet
      // processed. Transformation en colonnes du modele OLAP.
      println("=== BRANCHE 2 : PREPARATION JDBC POSTGRESQL ===")
      val known = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "dw.dim_location")
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load().select("location_id")

      val pickup = broadcast(
        known.withColumnRenamed("location_id", "known_pickup_id")
      )
      val dropoff = broadcast(
        known.withColumnRenamed("location_id", "known_dropoff_id")
      )

      val enriched = eligible
        .join(
          pickup,
          col("PULocationID").cast("int") === col("known_pickup_id"),
          "left"
        )
        .join(
          dropoff,
          col("DOLocationID").cast("int") === col("known_dropoff_id"),
          "left"
        )

      val facts = enriched.select(
        to_date(lit(SourceMonth)).alias("source_month"),
        lit(SourceFile).alias("source_file"),
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
        expr("timestampdiff(SECOND, tpep_pickup_datetime, " +
          "tpep_dropoff_datetime)")
          .cast("int").alias("trip_duration_seconds"),
        col("fare_amount").cast("decimal(14,2)").alias("fare_amount"),
        col("tip_amount").cast("decimal(14,2)").alias("tip_amount"),
        col("total_amount").cast("decimal(14,2)").alias("total_amount"),
        col("statut_qualite").alias("quality_status"),
        coalesce(col("alertes_qualite"), lit(""))
          .alias("quality_alerts")
      )

      // On cree une copie de la structure cible sans ses contraintes ni
      // ses colonnes auto-generees. La table fact_trip reste intacte.
      val selectedColumns = FactColumns.mkString(", ")
      execute(control, "CREATE SCHEMA IF NOT EXISTS staging")
      execute(control,
        s"CREATE TABLE IF NOT EXISTS $StagingTable AS " +
        s"SELECT $selectedColumns FROM $FinalTable WHERE FALSE"
      )
      execute(control, s"TRUNCATE TABLE $StagingTable")

      println("=== TRANSFERT DU LOT VERS LA TABLE STAGING ===")
      facts.repartition(2).write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", StagingTable)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .option("numPartitions", "2")
        .mode("append")
        .save()

      val stagedCount = scalarLong(
        control, s"SELECT COUNT(*) FROM $StagingTable"
      )
      println(s"Lignes en staging : $stagedCount")
      require(
        stagedCount == eligibleCount,
        s"Nombre en staging incoherent : $stagedCount != $eligibleCount"
      )

      // La suppression eventuelle d'un lot anterieur ET l'insertion du
      // nouveau lot se font dans une seule transaction PostgreSQL.
      // Si une contrainte echoue, rollback conserve l'ancien lot.
      control.setAutoCommit(false)
      try {
        execute(control,
          s"DELETE FROM $FinalTable WHERE source_file = '$SourceFile'"
        )
        execute(control,
          s"INSERT INTO $FinalTable ($selectedColumns) " +
          s"SELECT $selectedColumns FROM $StagingTable"
        )
        val finalCount = scalarLong(control,
          s"SELECT COUNT(*) FROM $FinalTable WHERE source_file = '$SourceFile'"
        )
        require(
          finalCount == eligibleCount,
          s"Verification SQL echouee : $finalCount != $eligibleCount"
        )
        control.commit()
        println(s"=== CHARGEMENT COMMIT : $finalCount COURSES ===")
      } catch {
        case e: Throwable =>
          control.rollback()
          throw e
      } finally {
        control.setAutoCommit(true)
      }

      val pilotCount = scalarLong(
        control,
        s"SELECT COUNT(*) FROM $FinalTable " +
          "WHERE source_file = 'yellow_tripdata_2026-01.parquet#pilot100'"
      )
      if (pilotCount > 0) {
        println(s"ATTENTION : $pilotCount lignes #pilot100 restent en base.")
      }
      println("=== PIPELINE UNIFIE TERMINE ===")

    } finally {
      if (spark != null) spark.stop()
      if (locked) {
        try {
          execute(control, "SELECT pg_advisory_unlock(202601)")
        } catch {
          case _: Throwable => ()
        }
      }
      control.close()
    }
  }
}
