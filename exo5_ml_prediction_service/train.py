import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import root_mean_squared_error
from sklearn.model_selection import train_test_split

from preparation import (CHEMIN_DONNEES, FEATURES, TARGET, ajouter_features,
                         charger_donnees, nettoyer)


def entrainer(df):
    """Entraine une regression lineaire et une foret aleatoire.

    Parameters
    ----------
    df : pandas.DataFrame
        Les courses nettoyees avec les features.

    Returns
    -------
    sklearn.ensemble.RandomForestRegressor
        La foret aleatoire entrainee.

    """
    X = df[FEATURES]
    y = df[TARGET]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # modele de base pour avoir un point de comparaison
    lineaire = LinearRegression()
    lineaire.fit(X_train, y_train)
    rmse = root_mean_squared_error(y_test, lineaire.predict(X_test))
    print("RMSE regression lineaire :", round(rmse, 2))

    foret = RandomForestRegressor(
        n_estimators=50, max_depth=15, n_jobs=-1, random_state=42
    )
    foret.fit(X_train, y_train)
    rmse = root_mean_squared_error(y_test, foret.predict(X_test))
    print("RMSE foret aleatoire :", round(rmse, 2))

    return foret


if __name__ == "__main__":
    df = charger_donnees(CHEMIN_DONNEES)
    df = ajouter_features(df)
    df = nettoyer(df)
    print("Nombre de courses apres nettoyage :", len(df))

    # on prend un echantillon sinon l'entrainement est trop long
    df = df.sample(400000, random_state=42)

    modele = entrainer(df)
    joblib.dump(modele, "modele.joblib")
    print("Modele sauvegarde dans modele.joblib")
