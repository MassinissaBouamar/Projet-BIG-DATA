
package cytech.ingestion

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object QualityRules {

  def validate(rawDF: DataFrame): DataFrame = {

    // 1. Verifier que les colonnes indispensables existent
    val requiredColumns = Seq(
      "VendorID",
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "trip_distance",
      "passenger_count",
      "RatecodeID",
      "store_and_fwd_flag",
      "PULocationID",
      "DOLocationID",
      "payment_type",
      "total_amount"
    )

    val missing = requiredColumns.filterNot(
      rawDF.columns.contains
    )

    require(
      missing.isEmpty,
      s"Colonnes manquantes : ${missing.mkString(", ")}"
    )

    // 2. Ajouter les motifs de rejet
    val withRejections = rawDF.withColumn(
      "motifs_rejet",
      concat_ws(" | ",

        when(
          col("tpep_pickup_datetime").isNull,
          "DATE_DEPART_MANQUANTE"
        ),

        when(
          col("tpep_dropoff_datetime").isNull,
          "DATE_ARRIVEE_MANQUANTE"
        ),

        when(
          col("tpep_dropoff_datetime") <
            col("tpep_pickup_datetime"),
          "DATES_INCOHERENTES"
        ),

        when(
          col("trip_distance").isNull,
          "DISTANCE_MANQUANTE"
        ),

        when(
          col("trip_distance") < 0,
          "DISTANCE_NEGATIVE"
        ),

        when(
          col("total_amount").isNull,
          "MONTANT_MANQUANT"
        )
      )
    )

    // 3. Ajouter les alertes non bloquantes
    val withAlerts = withRejections.withColumn(
      "alertes_qualite",
      concat_ws(" | ",

        when(
          col("trip_distance") === 0,
          "DISTANCE_NULLE"
        ),

        when(
          col("total_amount") < 0,
          "MONTANT_NEGATIF"
        ),

        when(
          col("VendorID").isNull ||
            !col("VendorID").isin(1, 2, 6, 7),
          "FOURNISSEUR_INCONNU"
        ),

        when(
          col("RatecodeID").isNull ||
            !col("RatecodeID").isin(1, 2, 3, 4, 5, 6, 99),
          "TARIF_INCONNU"
        ),

        when(
          col("payment_type").isNull ||
            !col("payment_type").isin(0, 1, 2, 3, 4, 5, 6),
          "PAIEMENT_INCONNU"
        ),

        when(
          col("store_and_fwd_flag").isNull ||
            !col("store_and_fwd_flag").isin("Y", "N"),
          "FLAG_INVALIDE"
        ),

        when(
          col("passenger_count").isNull ||
            col("passenger_count") <= 0,
          "PASSAGERS_A_VERIFIER"
        ),

        when(
          col("PULocationID").isNull ||
            col("PULocationID") <= 0,
          "ZONE_DEPART_INVALIDE"
        ),

        when(
          col("DOLocationID").isNull ||
            col("DOLocationID") <= 0,
          "ZONE_ARRIVEE_INVALIDE"
        )
      )
    )

    // 4. Attribuer un statut de qualite
    withAlerts.withColumn(
      "statut_qualite",

      when(
        length(col("motifs_rejet")) > 0,
        "REJETE"
      )
      .when(
        length(col("alertes_qualite")) > 0,
        "A_VERIFIER"
      )
      .otherwise("VALIDE")
    )
  }
}
