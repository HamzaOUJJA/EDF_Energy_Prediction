
import os
import sys
import math
from datetime import datetime
from pathlib import Path

# ==========================================================
# Correction PySpark : même Python pour driver et worker
# ==========================================================
PYTHON_PATH = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel


# ==========================================================
# Colonnes utilisées par le modèle
# ==========================================================
FEATURE_COLUMNS = [
    "hour",
    "day_of_week",
    "day_of_year",
    "week_of_year",
    "month",
    "quarter",
    "season",
    "hour_sin",
    "hour_cos",
    "is_weekend",
    "is_peak_hour",
    "nuclear_mw",
    "wind_mw",
    "solar_mw",
    "hydro_mw",
    "gas_mw",
    "coal_mw",
    "bioenergy_mw",
    "wind_onshore_mw",
    "wind_offshore_mw",
    "co2_rate",
    "lag_1h_mw",
    "lag_24h_mw",
    "lag_168h_mw",
    "rolling_24h_mean",
    "rolling_24h_std",
    "forecast_j1_mw",
]


# ==========================================================
# Fonctions utilitaires
# ==========================================================
def format_fr(value: float) -> str:
    """Formater un nombre au format français."""
    return f"{value:,.2f}".replace(",", " ").replace(".", ",")


def get_season(month: int) -> int:
    """
    Codage simple des saisons :
    0 = hiver
    1 = printemps
    2 = été
    3 = automne
    """
    if month in [12, 1, 2]:
        return 0
    if month in [3, 4, 5]:
        return 1
    if month in [6, 7, 8]:
        return 2
    return 3


@st.cache_resource
def get_spark():
    """Créer une session Spark locale."""
    spark = (
        SparkSession.builder
        .appName("EDF_Streamlit_Prediction")
        .master("local[1]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    return spark


@st.cache_resource
def load_model(model_path: str):
    """Charger le modèle Spark ML."""
    return PipelineModel.load(model_path)


# ==========================================================
# Configuration Streamlit
# ==========================================================
st.set_page_config(
    page_title="EDF Energy Prediction",
    layout="wide"
)

st.title("EDF Energy Prediction — Test du modèle")
st.write(
    "Cette interface permet de tester le modèle Spark ML sauvegardé "
    "dans le dossier `data/models/rte/best`."
)

model_path = st.text_input(
    "Chemin du modèle Spark ML",
    value="data/models/rte/best"
)

# Vérification rapide du modèle
metadata_path = Path(model_path) / "metadata"
stages_path = Path(model_path) / "stages"

if not metadata_path.exists() or not stages_path.exists():
    st.warning(
        "Le modèle Spark ML n'est pas trouvé dans ce chemin. "
        "Le dossier doit contenir `metadata` et `stages`. "
        "Vérifie que le modèle a bien été copié depuis MinIO vers `data/models/rte/best`."
    )


# ==========================================================
# 1. Informations temporelles
# ==========================================================
st.subheader("1. Informations temporelles")

col1, col2, col3 = st.columns(3)

with col1:
    date_value = st.date_input("Date de prédiction")

with col2:
    hour = st.number_input(
        "Heure",
        min_value=0,
        max_value=23,
        value=18,
        step=1
    )

with col3:
    forecast_j1_mw = st.number_input(
        "Prévision RTE J-1 (MW)",
        value=59000.0,
        step=100.0
    )

dt = datetime.combine(date_value, datetime.min.time())

day_of_week = int(dt.isoweekday())      # lundi=1 ... dimanche=7
day_of_year = int(dt.timetuple().tm_yday)
week_of_year = int(dt.isocalendar().week)
month = int(dt.month)
quarter = int((month - 1) // 3 + 1)
season = get_season(month)

is_weekend = 1 if day_of_week in [6, 7] else 0
is_peak_hour = 1 if (8 <= hour <= 20 and is_weekend == 0) else 0

hour_sin = math.sin(2 * math.pi * hour / 24)
hour_cos = math.cos(2 * math.pi * hour / 24)


# ==========================================================
# 2. Historique de consommation
# ==========================================================
st.subheader("2. Historique de consommation")

col1, col2, col3, col4 = st.columns(4)

with col1:
    lag_1h_mw = st.number_input(
        "Consommation il y a 1h (MW)",
        value=57500.0,
        step=100.0
    )

with col2:
    lag_24h_mw = st.number_input(
        "Consommation même heure hier (MW)",
        value=58000.0,
        step=100.0
    )

with col3:
    lag_168h_mw = st.number_input(
        "Consommation même heure semaine dernière (MW)",
        value=57000.0,
        step=100.0
    )

with col4:
    rolling_24h_mean = st.number_input(
        "Moyenne dernières 24h (MW)",
        value=56000.0,
        step=100.0
    )

rolling_24h_std = st.number_input(
    "Écart-type des dernières 24h (MW)",
    value=2500.0,
    step=100.0
)


# ==========================================================
# 3. Mix énergétique et contexte
# ==========================================================
st.subheader("3. Mix énergétique et contexte")

col1, col2, col3, col4 = st.columns(4)

with col1:
    nuclear_mw = st.number_input(
        "Nucléaire (MW)",
        value=42000.0,
        step=100.0
    )
    wind_mw = st.number_input(
        "Éolien total (MW)",
        value=5000.0,
        step=100.0
    )

with col2:
    solar_mw = st.number_input(
        "Solaire (MW)",
        value=1500.0,
        step=100.0
    )
    hydro_mw = st.number_input(
        "Hydraulique (MW)",
        value=7000.0,
        step=100.0
    )

with col3:
    gas_mw = st.number_input(
        "Gaz (MW)",
        value=3500.0,
        step=100.0
    )
    coal_mw = st.number_input(
        "Charbon (MW)",
        value=300.0,
        step=100.0
    )

with col4:
    bioenergy_mw = st.number_input(
        "Bioénergie (MW)",
        value=800.0,
        step=100.0
    )
    co2_rate = st.number_input(
        "CO2 rate",
        value=45.0,
        step=1.0
    )

wind_onshore_mw = st.number_input(
    "Éolien terrestre (MW)",
    value=4000.0,
    step=100.0
)

wind_offshore_mw = st.number_input(
    "Éolien offshore (MW)",
    value=1000.0,
    step=100.0
)


# ==========================================================
# Création de la ligne envoyée au modèle
# ==========================================================
input_row = {
    "hour": float(hour),
    "day_of_week": float(day_of_week),
    "day_of_year": float(day_of_year),
    "week_of_year": float(week_of_year),
    "month": float(month),
    "quarter": float(quarter),
    "season": float(season),
    "hour_sin": float(hour_sin),
    "hour_cos": float(hour_cos),
    "is_weekend": float(is_weekend),
    "is_peak_hour": float(is_peak_hour),
    "nuclear_mw": float(nuclear_mw),
    "wind_mw": float(wind_mw),
    "solar_mw": float(solar_mw),
    "hydro_mw": float(hydro_mw),
    "gas_mw": float(gas_mw),
    "coal_mw": float(coal_mw),
    "bioenergy_mw": float(bioenergy_mw),
    "wind_onshore_mw": float(wind_onshore_mw),
    "wind_offshore_mw": float(wind_offshore_mw),
    "co2_rate": float(co2_rate),
    "lag_1h_mw": float(lag_1h_mw),
    "lag_24h_mw": float(lag_24h_mw),
    "lag_168h_mw": float(lag_168h_mw),
    "rolling_24h_mean": float(rolling_24h_mean),
    "rolling_24h_std": float(rolling_24h_std),
    "forecast_j1_mw": float(forecast_j1_mw),
}

input_df = pd.DataFrame([input_row], columns=FEATURE_COLUMNS)

st.subheader("4. Données envoyées au modèle")
st.dataframe(input_df, use_container_width=True)


# ==========================================================
# 5. Valeur réelle connue pour vérifier la prédiction
# ==========================================================
st.subheader("5. Vérification avec une valeur réelle")

real_consumption = st.number_input(
    "Consommation réelle connue (MW) - optionnel",
    value=0.0,
    step=100.0,
    help="Mets ici la vraie consommation si tu veux comparer la prédiction avec le réel."
)


# ==========================================================
# 6. Prédiction et comparaison
# ==========================================================
if st.button("Prédire la consommation"):
    try:
        spark = get_spark()
        model = load_model(model_path)

        spark_df = spark.createDataFrame(input_df)
        pred_df = model.transform(spark_df)

        prediction = pred_df.select("prediction").collect()[0]["prediction"]

        st.success(
            f"Consommation prédite par le modèle : {format_fr(prediction)} MW"
        )

        # --------------------------------------------------
        # Comparaison si la consommation réelle est fournie
        # --------------------------------------------------
        if real_consumption > 0:
            st.subheader("6. Comparaison avec la valeur réelle")

            # Erreur du modèle
            model_error_mw = abs(real_consumption - prediction)
            model_error_percent = (model_error_mw / real_consumption) * 100

            # Erreur de la prévision J-1
            forecast_error_mw = abs(real_consumption - forecast_j1_mw)
            forecast_error_percent = (forecast_error_mw / real_consumption) * 100

            # Erreur de la consommation même heure hier
            lag24_error_mw = abs(real_consumption - lag_24h_mw)
            lag24_error_percent = (lag24_error_mw / real_consumption) * 100

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric(
                    "Erreur modèle IA",
                    f"{format_fr(model_error_mw)} MW",
                    f"{model_error_percent:.2f} %"
                )

            with col2:
                st.metric(
                    "Erreur prévision J-1",
                    f"{format_fr(forecast_error_mw)} MW",
                    f"{forecast_error_percent:.2f} %"
                )

            with col3:
                st.metric(
                    "Erreur lag 24h",
                    f"{format_fr(lag24_error_mw)} MW",
                    f"{lag24_error_percent:.2f} %"
                )

            comparison_df = pd.DataFrame([
                {
                    "Méthode": "Modèle IA / RandomForest",
                    "Valeur prédite ou utilisée (MW)": prediction,
                    "Erreur MW": model_error_mw,
                    "Erreur %": model_error_percent
                },
                {
                    "Méthode": "Prévision RTE J-1",
                    "Valeur prédite ou utilisée (MW)": forecast_j1_mw,
                    "Erreur MW": forecast_error_mw,
                    "Erreur %": forecast_error_percent
                },
                {
                    "Méthode": "Consommation même heure hier",
                    "Valeur prédite ou utilisée (MW)": lag_24h_mw,
                    "Erreur MW": lag24_error_mw,
                    "Erreur %": lag24_error_percent
                }
            ])

            st.subheader("Tableau de comparaison")
            st.dataframe(comparison_df, use_container_width=True)

            best_method = comparison_df.sort_values("Erreur MW").iloc[0]["Méthode"]

            if best_method == "Modèle IA / RandomForest":
                st.success(
                    "Sur cet exemple, le modèle IA est meilleur que les deux baselines : "
                    "la prévision J-1 et la consommation de la veille."
                )
            else:
                st.warning(
                    f"Sur cet exemple, la meilleure méthode est : {best_method}. "
                    "Il faut tester plusieurs lignes historiques pour juger correctement "
                    "la performance globale du modèle."
                )

        else:
            st.info(
                "La prédiction fonctionne. Pour savoir si elle est bonne, "
                "ajoute une consommation réelle connue afin de calculer l'erreur."
            )

        st.info(
            "Attention : cette interface est un test manuel. Pour une vraie prédiction future, "
            "il faut vérifier que toutes les variables utilisées sont disponibles avant le moment prédit."
        )

    except Exception as e:
        st.error("Erreur pendant la prédiction.")
        st.exception(e)
