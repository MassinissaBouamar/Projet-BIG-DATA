import pandas as pd
import pytest

from predict import preparer_entree
from preparation import FEATURES, ajouter_features, nettoyer


def creer_courses():
    return pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime([
            "2026-01-05 08:00:00",  # course normale
            "2026-01-05 09:00:00",  # montant negatif
            "2026-01-05 10:00:00",  # distance nulle
            "2026-01-05 11:00:00",  # course de 5h
        ]),
        "tpep_dropoff_datetime": pd.to_datetime([
            "2026-01-05 08:20:00",
            "2026-01-05 09:15:00",
            "2026-01-05 10:10:00",
            "2026-01-05 16:00:00",
        ]),
        "trip_distance": [3.5, 2.0, 0.0, 10.0],
        "PULocationID": [161, 79, 48, 132],
        "DOLocationID": [236, 234, 48, 161],
        "total_amount": [25.0, -12.5, 10.0, 80.0],
    })


# donnees d'entrainement

def test_ajouter_features():
    df = ajouter_features(creer_courses())
    assert df["duree_min"].iloc[0] == 20
    assert df["heure"].iloc[0] == 8
    # le 5 janvier 2026 est un lundi
    assert df["jour_semaine"].iloc[0] == 0


def test_nettoyer_enleve_les_courses_aberrantes():
    df = nettoyer(ajouter_features(creer_courses()))
    assert len(df) == 1
    assert df["total_amount"].iloc[0] == 25.0


def test_donnees_entrainement_valides():
    df = nettoyer(ajouter_features(creer_courses()))
    assert df[FEATURES].isna().sum().sum() == 0
    assert (df["total_amount"] > 0).all()
    assert (df["trip_distance"] > 0).all()
    assert (df["duree_min"] > 0).all()


# donnees d'inference

def test_preparer_entree_colonnes():
    X = preparer_entree(creer_courses())
    assert list(X.columns) == FEATURES
    assert len(X) == 4


def test_preparer_entree_sans_prix():
    # a l'inference on ne connait pas encore le prix
    courses = creer_courses().drop(columns=["total_amount"])
    X = preparer_entree(courses)
    assert "total_amount" not in X.columns


def test_preparer_entree_colonne_manquante():
    courses = creer_courses().drop(columns=["trip_distance"])
    with pytest.raises(KeyError):
        preparer_entree(courses)
