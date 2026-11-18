
package cytech.ingestion

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object QualityRules {

  // Cree une alerte si une condition est respectee
  private def flag(condition: Column, label: String): Column =
    when(condition, lit(label))

  def validate(rawDF: DataFrame): DataFrame = {

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

    // 1. Erreurs bloquantes
    val checkedDF = rawDF.withColumn(
      "motifs_rejet",
      concat_ws(" | ",
        flag(
          col("tpep_pickup_datetime").isNull,
          "DATE_DEPART_MANQUANTE"
        ),
        flag(
          col("tpep_dropoff_datetime").isNull,
          "DATE_ARRIVEE_MANQUANTE"
        ),
        flag(
          col("tpep_dropoff_datetime") <
            col("tpep_pickup_datetime"),
          "DATES_INCOHERENTES"
        ),
        flag(
          col("trip_distance").isNull,
          "DISTANCE_MANQUANTE"
        ),
        flag(
          col("trip_distance") < 0,
          "DISTANCE_NEGATIVE"
        ),
        flag(
          col("total_amount").isNull,
          "MONTANT_MANQUANT"
        )
      )
    )

    // 2. Anomalies non bloquantes
    val withAlerts = checkedDF.withColumn(
      "alertes_qualite",
      concat_ws(" | ",

        flag(
          col("trip_distance") === 0,
          "DISTANCE_NULLE"
        ),
        flag(
          col("total_amount") < 0,
          "MONTANT_NEGATIF"
        ),

        // Passagers
        flag(
          col("passenger_count").isNull,
          "PASSAGERS_MANQUANTS"
        ),
        flag(
          col("passenger_count") === 0,
          "PASSAGERS_ZERO"
        ),
        flag(
          col("passenger_count") < 0,
          "PASSAGERS_NEGATIFS"
        ),

        // Fournisseur
        flag(
          col("VendorID").isNull,
          "FOURNISSEUR_MANQUANT"
        ),
        flag(
          col("VendorID").isNotNull &&
            !col("VendorID").isin(1, 2, 6, 7),
          "FOURNISSEUR_INVALIDE"
        ),

        // Tarification
        flag(
          col("RatecodeID").isNull,
          "TARIF_MANQUANT"
        ),
        flag(
          col("RatecodeID").isNotNull &&
            !col("RatecodeID").isin(1, 2, 3, 4, 5, 6, 99),
          "TARIF_INVALIDE"
        ),
        flag(
          col("RatecodeID") === 99,
          "TARIF_NON_RENSEIGNE"
        ),

        // Flag de transmission
        flag(
          col("store_and_fwd_flag").isNull,
          "FLAG_MANQUANT"
        ),
        flag(
          col("store_and_fwd_flag").isNotNull &&
            !col("store_and_fwd_flag").isin("Y", "N"),
          "FLAG_INVALIDE"
        ),

        // Paiement
        flag(
          col("payment_type").isNull,
          "PAIEMENT_MANQUANT"
        ),
        flag(
          col("payment_type").isNotNull &&
            !col("payment_type").isin(0, 1, 2, 3, 4, 5, 6),
          "PAIEMENT_INVALIDE"
        ),

        // Zones
        flag(
          col("PULocationID").isNull ||
            col("PULocationID") <= 0,
          "ZONE_DEPART_INVALIDE"
        ),
        flag(
          col("DOLocationID").isNull ||
            col("DOLocationID") <= 0,
          "ZONE_ARRIVEE_INVALIDE"
        )
      )
    )

    // 3. Classification finale
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
