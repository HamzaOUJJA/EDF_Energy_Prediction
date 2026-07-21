# =======================================================================
# ************** Projet : EDF Energy Prediction **************
# ************** Version : 1.0.0 **************
# =======================================================================
#
# spark/ml/constants.py — ML hyperparameters and safe operational features
# =======================================================================

from __future__ import annotations

import os

# =======================================================================
# Configuration générale ML
# =======================================================================

LABEL_COL: str = os.getenv("ML_LABEL_COL", "consumption_mw")

TRAIN_RATIO: float = float(os.getenv("ML_TRAIN_RATIO", "0.8"))

RANDOM_SEED: int = 42

ML_RF_NUM_TREES: int = int(os.getenv("ML_RF_NUM_TREES", "30"))
ML_RF_MAX_DEPTH: int = int(os.getenv("ML_RF_MAX_DEPTH", "8"))

ML_GBT_MAX_ITER: int = int(os.getenv("ML_GBT_MAX_ITER", "30"))
ML_GBT_MAX_DEPTH: int = int(os.getenv("ML_GBT_MAX_DEPTH", "6"))

ML_SPARK_PARTITIONS: int = int(os.getenv("ML_SPARK_PARTITIONS", "4"))


# =======================================================================
# Features autorisées pour le modèle opérationnel
# =======================================================================
#
# Règle importante :
# Une variable est acceptée seulement si elle est connue AVANT la prédiction.
#
# Ces variables sont sûres :
# - variables temporelles connues à l'avance ;
# - historiques de consommation déjà observés ;
# - prévision RTE J-1 normalement disponible avant le jour concerné.
#
# Les variables de production électrique réelle et de CO2 sont volontairement
# exclues du modèle pour éviter une fuite de données.
# =======================================================================

SAFE_OPERATIONAL_FEATURES: list[str] = [
    # Variables temporelles connues à l'avance
    "hour",
    "day_of_week",
    "day_of_year",
    "week_of_year",
    "month",
    "quarter",
    "season",
    "is_weekend",
    "is_peak_hour",

    # Encodage cyclique de l'heure
    "hour_sin",
    "hour_cos",

    # Historique de consommation connu avant la prédiction
    "lag_1h_mw",
    "lag_24h_mw",
    "lag_168h_mw",

    # Tendances historiques calculées uniquement avec le passé
    "rolling_24h_mean",
    "rolling_24h_std",

    # Prévision RTE J-1 disponible avant le jour J
    "forecast_j1_mw",
]


# =======================================================================
# Variables interdites dans le modèle opérationnel
# =======================================================================
#
# Ces variables peuvent rester dans Silver et Gold pour :
# - les dashboards ;
# - l'analyse métier ;
# - les graphiques Power BI / Grafana ;
# - l'étude du mix énergétique ;
# - l'analyse descriptive.
#
# Mais elles ne doivent pas entrer dans l'entraînement du modèle ML, car elles
# peuvent décrire l'état réel du système électrique à l'instant à prédire.
# =======================================================================

FORBIDDEN_LEAKAGE_FEATURES: set[str] = {
    "nuclear_mw",
    "wind_mw",
    "solar_mw",
    "hydro_mw",
    "gas_mw",
    "fuel_mw",
    "coal_mw",
    "bioenergy_mw",
    "co2_rate",
    "wind_onshore_mw",
    "wind_offshore_mw",
    "hydro_river_mw",
    "hydro_lake_mw",
    "hydro_step_mw",
    "renewable_share_pct",
    "nuclear_share_pct",
    "forecast_error_mw",
    "forecast_error_pct",
}


# =======================================================================
# Compatibilité avec ton code existant
# =======================================================================
#
# Le fichier gold_to_model.py utilise déjà CANDIDATE_FEATURES.
# Donc on garde le même nom, mais maintenant il contient seulement les
# variables sûres.
# =======================================================================

CANDIDATE_FEATURES: list[str] = SAFE_OPERATIONAL_FEATURES


def validate_no_data_leakage(feature_cols: list[str]) -> None:
    """
    Vérifie qu'aucune variable interdite n'est utilisée dans le modèle ML.

    Cette fonction protège le projet contre une fuite de données.
    Si une variable comme nuclear_mw, wind_mw, solar_mw ou co2_rate entre
    dans feature_cols, le job ML s'arrête immédiatement avec une erreur claire.

    Parameters
    ----------
    feature_cols:
        Liste finale des variables utilisées par le modèle.

    Raises
    ------
    ValueError:
        Si une variable interdite est détectée dans la liste des features.
    """
    leakage = sorted(set(feature_cols).intersection(FORBIDDEN_LEAKAGE_FEATURES))

    if leakage:
        raise ValueError(
            "Data leakage detected in ML features. "
            f"Forbidden features found: {leakage}. "
            "Ces variables sont conservées dans Silver/Gold pour l'analyse "
            "descriptive, mais elles ne doivent pas être utilisées dans le "
            "modèle opérationnel."
        )