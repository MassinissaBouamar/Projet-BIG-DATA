import joblib
import pandas as pd

from preparation import FEATURES, ajouter_features


def preparer_entree(courses):
    """Prepare les courses pour les donner au modele.

    Parameters
    ----------
    courses : pandas.DataFrame
        Les courses avec les dates, la distance et les zones.

    Returns
    -------
    pandas.DataFrame
        Les colonnes utilisees par le modele.

    """
    courses = ajouter_features(courses)
    return courses[FEATURES]


def predire(modele, courses):
    """Predit le prix des courses.

    Parameters
    ----------
    modele : sklearn.ensemble.RandomForestRegressor
        Le modele entraine par train.py.
    courses : pandas.DataFrame
        Les courses dont on veut estimer le prix.

    Returns
    -------
    numpy.ndarray
        Le prix estime de chaque course.

    """
    X = preparer_entree(courses)
    return modele.predict(X)


if __name__ == "__main__":
    modele = joblib.load("modele.joblib")

    # deux exemples : Midtown -> JFK et East Village -> Union Square
    courses = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(
            ["2026-01-15 08:30:00", "2026-01-17 23:10:00"]
        ),
        "tpep_dropoff_datetime": pd.to_datetime(
            ["2026-01-15 09:20:00", "2026-01-17 23:25:00"]
        ),
        "trip_distance": [17.5, 3.2],
        "PULocationID": [161, 79],
        "DOLocationID": [132, 234],
    })

    prix = predire(modele, courses)
    for p in prix:
        print("Prix estime :", round(p, 2), "$")
