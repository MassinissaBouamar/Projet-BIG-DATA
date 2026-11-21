import datetime

import joblib
import pandas as pd
import streamlit as st

from predict import predire


@st.cache_resource
def charger_modele():
    """Charge le modele entraine par train.py.

    Returns
    -------
    sklearn.ensemble.RandomForestRegressor
        Le modele sauvegarde dans modele.joblib.

    """
    return joblib.load("modele.joblib")


st.title("Estimation du prix d'une course de taxi à New York")

modele = charger_modele()

date = st.date_input("Date de la course", datetime.date(2026, 1, 15))
heure = st.time_input("Heure de départ", datetime.time(8, 30))
distance = st.number_input("Distance (miles)", min_value=0.1, value=3.0)
duree = st.number_input("Durée (minutes)", min_value=1, value=15)
depart = st.number_input("Zone de départ (PULocationID)", 1, 265, 161)
arrivee = st.number_input("Zone d'arrivée (DOLocationID)", 1, 265, 236)

if st.button("Estimer le prix"):
    debut = datetime.datetime.combine(date, heure)
    fin = debut + datetime.timedelta(minutes=duree)
    course = pd.DataFrame({
        "tpep_pickup_datetime": [debut],
        "tpep_dropoff_datetime": [fin],
        "trip_distance": [distance],
        "PULocationID": [depart],
        "DOLocationID": [arrivee],
    })
    prix = predire(modele, course)[0]
    st.success(f"Prix estimé : {prix:.2f} $")
