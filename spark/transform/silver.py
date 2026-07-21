# =======================================================================
# ************** Projet : EDF Energy Prediction **************
# ************** Version : 1.0.0 **************
# =======================================================================
#
# spark/transform/silver.py — Silver transformations
# =======================================================================

from __future__ import annotations

import logging
import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

CONSO_MIN_MW: float = 10_000.0
CONSO_MAX_MW: float = 120_000.0


# =======================================================================
# Colonnes Silver Parquet
# =======================================================================
# Ces colonnes incluent aussi des variables descriptives comme nuclear_mw,
# wind_mw, solar_mw ou co2_rate.
#
# Important :
# Ces variables restent dans Silver/Gold pour l'analyse et les dashboards,
# mais elles sont exclues du modèle ML dans spark/ml/constants.py.
# =======================================================================

SILVER_OUTPUT_COLUMNS: list[str] = [
    "datetime",
    "consumption_mw",
    "forecast_j1_mw",
    "forecast_error_mw",
    "forecast_error_pct",
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
    "hour",
    "day_of_week",
    "day_of_year",
    "week_of_year",
    "month",
    "year",
    "quarter",
    "season",
    "is_weekend",
    "is_peak_hour",
    "hour_sin",
    "hour_cos",
    "lag_1h_mw",
    "lag_24h_mw",
    "lag_168h_mw",
    "rolling_24h_mean",
    "rolling_24h_std",
    "renewable_share_pct",
    "nuclear_share_pct",
    "is_interpolated",
    "quality_score",
]


# =======================================================================
# Colonnes alignées avec infra/postgres/schema_dw.sql
# =======================================================================

SILVER_POSTGRES_COLUMNS: list[str] = [
    "datetime",
    "consumption_mw",
    "forecast_j1_mw",
    "forecast_error_mw",
    "forecast_error_pct",
    "nuclear_mw",
    "wind_mw",
    "solar_mw",
    "hydro_mw",
    "gas_mw",
    "bioenergy_mw",
    "fuel_mw",
    "coal_mw",
    "wind_onshore_mw",
    "wind_offshore_mw",
    "physical_exchanges_mw",
    "co2_rate",
    "hour",
    "day_of_week",
    "day_of_year",
    "week_of_year",
    "month",
    "year",
    "quarter",
    "season",
    "is_weekend",
    "is_peak_hour",
    "lag_1h_mw",
    "lag_24h_mw",
    "lag_168h_mw",
    "rolling_24h_mean",
    "rolling_24h_std",
    "renewable_share_pct",
    "nuclear_share_pct",
    "is_interpolated",
    "quality_score",
]


def clean(df: DataFrame) -> DataFrame:
    """
    Nettoyage métier de la couche Bronze vers Silver.

    Étapes :
    - création de datetime ;
    - suppression des doublons temporels ;
    - filtrage des consommations aberrantes ;
    - cast des colonnes numériques ;
    - imputation simple de certaines valeurs manquantes ;
    - création d'un indicateur is_interpolated.
    """
    logger.info("Cleaning Silver...")

    # -------------------------------------------------------------------
    # Création de la colonne datetime
    # -------------------------------------------------------------------
    if "date" in df.columns and "time" in df.columns:
        df = df.withColumn(
            "datetime",
            F.to_timestamp(
                F.concat_ws(" ", F.col("date"), F.col("time")),
                "yyyy-MM-dd HH:mm",
            ),
        )

    df = df.filter(F.col("datetime").isNotNull())

    # -------------------------------------------------------------------
    # Suppression des doublons sur la date/heure
    # -------------------------------------------------------------------
    before = df.count()
    df = df.dropDuplicates(["datetime"])
    after = df.count()

    logger.info(" -> %s duplicates removed", f"{before - after:,}")

    # -------------------------------------------------------------------
    # Filtrage des valeurs aberrantes de consommation
    # -------------------------------------------------------------------
    df = df.filter(
        F.col("consumption_mw").isNull()
        | F.col("consumption_mw").between(CONSO_MIN_MW, CONSO_MAX_MW)
    )

    # -------------------------------------------------------------------
    # Cast des colonnes numériques
    # -------------------------------------------------------------------
    numeric_cols = [
        "consumption_mw",
        "forecast_j1_mw",
        "forecast_j_mw",
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
    ]

    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # -------------------------------------------------------------------
    # Indicateur des lignes dont la consommation était manquante
    # -------------------------------------------------------------------
    df = df.withColumn("is_interpolated", F.col("consumption_mw").isNull())

    # -------------------------------------------------------------------
    # Imputation simple par propagation avant/arrière
    # -------------------------------------------------------------------
    window_fw = Window.orderBy("datetime").rowsBetween(
        Window.unboundedPreceding,
        0,
    )

    window_bw = Window.orderBy("datetime").rowsBetween(
        0,
        Window.unboundedFollowing,
    )

    for col in ["consumption_mw", "nuclear_mw", "wind_mw", "solar_mw"]:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.coalesce(
                    F.col(col),
                    F.last(F.col(col), ignorenulls=True).over(window_fw),
                    F.first(F.col(col), ignorenulls=True).over(window_bw),
                ),
            )

    logger.info(" -> %s lines after cleaning", f"{df.count():,}")

    return df


def engineer_features(df: DataFrame) -> DataFrame:
    """
    Feature engineering de la couche Silver.

    Les features utilisées pour le ML doivent être disponibles avant la
    prédiction. Les variables descriptives du mix énergétique restent
    disponibles dans Silver/Gold, mais ne sont pas utilisées dans le modèle ML.
    """
    logger.info("Feature engineering Silver...")

    # -------------------------------------------------------------------
    # Variables temporelles
    # -------------------------------------------------------------------
    df = (
        df.withColumn("hour", F.hour("datetime"))
        .withColumn("day_of_week", F.dayofweek("datetime"))
        .withColumn("day_of_year", F.dayofyear("datetime"))
        .withColumn("week_of_year", F.weekofyear("datetime"))
        .withColumn("month", F.month("datetime"))
        .withColumn("year", F.year("datetime"))
        .withColumn("quarter", F.quarter("datetime"))
        .withColumn("is_weekend", F.dayofweek("datetime").isin([1, 7]))
        .withColumn(
            "is_peak_hour",
            (F.hour("datetime").between(8, 20))
            & (~F.dayofweek("datetime").isin([1, 7])),
        )
    )

    # -------------------------------------------------------------------
    # Saison
    # 0 = hiver, 1 = printemps, 2 = été, 3 = automne
    # -------------------------------------------------------------------
    df = df.withColumn(
        "season",
        F.when(F.col("month").isin([12, 1, 2]), 0)
        .when(F.col("month").isin([3, 4, 5]), 1)
        .when(F.col("month").isin([6, 7, 8]), 2)
        .otherwise(3),
    )

    # -------------------------------------------------------------------
    # Encodage cyclique de l'heure
    # -------------------------------------------------------------------
    df = (
        df.withColumn(
            "hour_sin",
            F.sin(F.lit(2 * math.pi) * F.col("hour") / 24),
        )
        .withColumn(
            "hour_cos",
            F.cos(F.lit(2 * math.pi) * F.col("hour") / 24),
        )
    )

    # -------------------------------------------------------------------
    # Lags de consommation
    # -------------------------------------------------------------------
    # Les données RTE sont souvent en pas de 30 minutes.
    # 2 lignes = 1 heure
    # 48 lignes = 24 heures
    # 336 lignes = 168 heures = 7 jours
    #
    # Ces variables utilisent uniquement le passé, donc elles sont acceptables
    # pour le modèle opérationnel.
    # -------------------------------------------------------------------
    w = Window.orderBy("datetime")

    df = (
        df.withColumn("lag_1h_mw", F.lag("consumption_mw", 2).over(w))
        .withColumn("lag_24h_mw", F.lag("consumption_mw", 48).over(w))
        .withColumn("lag_168h_mw", F.lag("consumption_mw", 336).over(w))
    )

    # -------------------------------------------------------------------
    # Rolling features historiques
    # -------------------------------------------------------------------
    # Correction importante anti-fuite :
    # Avant : rowsBetween(-48, 0)
    # Problème : 0 inclut la ligne actuelle, donc la consommation à prédire.
    #
    # Maintenant : rowsBetween(-48, -1)
    # Cela utilise uniquement les 48 lignes précédentes, donc les 24 heures
    # précédentes, sans utiliser la ligne actuelle.
    # -------------------------------------------------------------------
    w_24h = Window.orderBy("datetime").rowsBetween(-48, -1)

    df = (
        df.withColumn("rolling_24h_mean", F.avg("consumption_mw").over(w_24h))
        .withColumn("rolling_24h_std", F.stddev("consumption_mw").over(w_24h))
    )

    # -------------------------------------------------------------------
    # Variables descriptives du mix énergétique
    # -------------------------------------------------------------------
    # Elles restent utiles pour les dashboards et l'analyse descriptive.
    # Elles ne doivent pas être utilisées dans le modèle ML opérationnel.
    # -------------------------------------------------------------------
    renewable_sum = (
        F.coalesce(F.col("wind_mw"), F.lit(0))
        + F.coalesce(F.col("solar_mw"), F.lit(0))
        + F.coalesce(F.col("hydro_mw"), F.lit(0))
    )

    df = (
        df.withColumn(
            "renewable_share_pct",
            F.when(
                F.col("consumption_mw") > 0,
                renewable_sum / F.col("consumption_mw") * 100,
            ).otherwise(None),
        )
        .withColumn(
            "nuclear_share_pct",
            F.when(
                F.col("consumption_mw") > 0,
                F.col("nuclear_mw") / F.col("consumption_mw") * 100,
            ).otherwise(None),
        )
    )

    # -------------------------------------------------------------------
    # Erreur de prévision J-1
    # -------------------------------------------------------------------
    # Ces colonnes sont gardées pour l'analyse comparative avec la prévision
    # RTE J-1, mais elles ne doivent pas être utilisées comme features du
    # modèle, car elles utilisent la vraie consommation.
    # -------------------------------------------------------------------
    df = (
        df.withColumn(
            "forecast_error_mw",
            F.when(
                F.col("forecast_j1_mw").isNotNull()
                & F.col("consumption_mw").isNotNull(),
                F.col("consumption_mw") - F.col("forecast_j1_mw"),
            ).otherwise(None),
        )
        .withColumn(
            "forecast_error_pct",
            F.when(
                F.col("consumption_mw") > 0,
                F.abs(F.col("forecast_error_mw")) / F.col("consumption_mw") * 100,
            ).otherwise(None),
        )
    )

    # -------------------------------------------------------------------
    # Score qualité simple
    # -------------------------------------------------------------------
    key_cols = [
        "consumption_mw",
        "nuclear_mw",
        "wind_mw",
        "solar_mw",
        "co2_rate",
    ]

    available_key = [c for c in key_cols if c in df.columns]

    if available_key:
        non_null_count = sum(
            F.when(F.col(c).isNotNull(), 1).otherwise(0)
            for c in available_key
        )

        df = df.withColumn(
            "quality_score",
            non_null_count / len(available_key),
        )

    logger.info(" -> %d columns", len(df.columns))

    return df