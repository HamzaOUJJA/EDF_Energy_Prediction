# =======================================================================
# Noms de fichiers PNG du rapport EDA (sans dépendance matplotlib).
# =======================================================================

from __future__ import annotations

DATA_CHART_FILENAMES: tuple[str, ...] = (
    "consommation_electrique_nationale.png",
    "volume_mensuel_de_consommation.png",
    "qualite_des_donnees_source.png",
)

ML_CHART_FILENAMES: tuple[str, ...] = (
    "courbe_apprentissage_foret_aleatoire.png",
    "predictions_dispersion_reel_vs_predit.png",
    "predictions_serie_temporelle_ecarts.png",
    "comparaison_des_modeles.png",
    "synthese_performance_ml.png",
    "repartition_des_donnees.png",
)


def all_report_filenames() -> list[str]:
    return list(DATA_CHART_FILENAMES) + list(ML_CHART_FILENAMES)
