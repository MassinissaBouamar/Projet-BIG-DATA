import pandas as pd

CHEMIN_DONNEES = "../data/raw/yellow_tripdata_2026-01.parquet"

FEATURES = [
    "trip_distance",
    "duree_min",
    "PULocationID",
    "DOLocationID",
    "heure",
    "jour_semaine",
]
TARGET = "total_amount"


def charger_donnees(chemin):
    """Charge le fichier parquet des courses.

    Parameters
    ----------
    chemin : str
        Chemin vers le fichier parquet.

    Returns
    -------
    pandas.DataFrame
        Les courses brutes.

    """
    return pd.read_parquet(chemin)


def ajouter_features(df):
    """Calcule la duree de la course, l'heure et le jour de la semaine.

    Parameters
    ----------
    df : pandas.DataFrame
        Les courses avec les dates de depart et d'arrivee.

    Returns
    -------
    pandas.DataFrame
        Les courses avec les colonnes duree_min, heure et jour_semaine.

    """
    df = df.copy()
    duree = df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    df["duree_min"] = duree.dt.total_seconds() / 60
    df["heure"] = df["tpep_pickup_datetime"].dt.hour
    df["jour_semaine"] = df["tpep_pickup_datetime"].dt.day_of_week
    return df


def nettoyer(df):
    """Supprime les courses avec des valeurs aberrantes.

    Parameters
    ----------
    df : pandas.DataFrame
        Les courses apres ajout des features.

    Returns
    -------
    pandas.DataFrame
        Les courses gardees pour l'entrainement.

    """
    df = df[(df["total_amount"] > 0) & (df["total_amount"] < 300)]
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] < 100)]
    # une course de moins d'une minute ou de plus de 3h n'est pas realiste
    df = df[(df["duree_min"] > 1) & (df["duree_min"] < 180)]
    return df
