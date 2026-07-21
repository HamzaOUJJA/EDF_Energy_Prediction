# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/common/eda_report.py — EDA report generation (shared: Airflow, Makefile CLI).
# =======================================================================

from __future__ import annotations

import logging
import os
import socket
from pathlib import Path
from typing import Any, Callable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd

from rte_pipeline.parsing.xls import iter_xls_file
from spark.common.eda_specs import ML_CHART_FILENAMES, DATA_CHART_FILENAMES, all_report_filenames
from spark.common.config import DATA_DIR, REPORT_EDA_BUCKET, REPORT_EDA_LOCAL, REPORT_EDA_S3_PREFIX, resolve_model_local_path
from spark.common.eda_style import (
    COLORS,
    HEADER_BOTTOM,
    add_accent_bar,
    add_chart_header,
    add_report_footer,
    apply_dashboard_style,
    apply_figure_layout,
    build_consumption_national_figure,
    build_ml_synthesis_figure,
    build_model_comparison_figure,
    build_monthly_comparison_figure,
    build_predictions_dispersion_figure,
    build_predictions_timeseries_figure,
    build_quality_diagnostic_figure,
    plot_data_split,
    plot_learning_curve_rf,
    prepare_time_series,
    save_report_figure,
    human_model_name,
    describe_learning_curve,
    synthesis_footnote,
    _style_ax_dashboard,
)
from spark.common.object_storage import (
    persist_report_file,
    remove_report_from_s3,
    sync_ml_report_artifacts,
    sync_model_artifacts,
    sync_report_pngs,
)

logger = logging.getLogger(__name__)

# (titre affiché, nom de fichier PNG)
DATA_CHART_SPECS: tuple[tuple[str, str], ...] = (
    ("Impact du nettoyage — Consommation nationale", DATA_CHART_FILENAMES[0]),
    ("Volume mensuel — brute vs nettoyée", DATA_CHART_FILENAMES[1]),
    ("Qualité des données — diagnostic avant / après ETL", DATA_CHART_FILENAMES[2]),
)

# =======================================================================
# Graphiques ML
# =======================================================================
# Correction :
# - Les graphiques de prédiction ne sont plus nommés "Forêt aléatoire".
# - Ils sont nommés "modèle retenu", car le meilleur modèle peut changer.
# - La courbe d'apprentissage reste liée à la forêt aléatoire, mais elle est
#   indiquée comme analyse complémentaire pour éviter la confusion.
# =======================================================================

ML_CHART_SPECS: tuple[tuple[str, str, str], ...] = (
    (
        "Courbe d'apprentissage – Forêt aléatoire, analyse complémentaire",
        ML_CHART_FILENAMES[0],
        "Entraînement (bleu) vs validation (orange) · axe X logarithmique · analyse complémentaire",
    ),
    (
        "Performance des prédictions – modèle retenu (jeu de test)",
        ML_CHART_FILENAMES[1],
        "Graphique A · nuage de points coloré par |erreur| · bande ±2×RMSE · courbe LOESS",
    ),
    (
        "Performance des prédictions – modèle retenu (jeu de test)",
        ML_CHART_FILENAMES[2],
        "Graphique B · 2 blocs côte à côte (données RTE incomplètes entre les périodes) · MM 6 h · MAE par bloc",
    ),
    (
        "Comparaison des modèles – Validation temporelle (80/20)",
        ML_CHART_FILENAMES[3],
        "RMSE (% du meilleur) + pénalité (1−R²) · palette vert → rouge = rang",
    ),
    (
        "Synthèse de performance ML",
        ML_CHART_FILENAMES[4],
        "Tableau comparatif · meilleure valeur en gras par colonne · Δ RMSE % vs meilleur modèle",
    ),
    (
        "Répartition des données – Split temporel",
        ML_CHART_FILENAMES[5],
        "Camembert 80/20 · effectifs et périodes · découpage temporel strict (test = période future)",
    ),
)

ML_PENDING = "ml_dashboard.pending"

KEY_NUMERIC = [
    "consumption_mw",
    "nuclear_mw",
    "wind_mw",
    "solar_mw",
    "hydro_mw",
    "gas_mw",
    "coal_mw",
    "fuel_mw",
    "bioenergy_mw",
    "co2_rate",
]


def postgres_dsn() -> str:
    """PostgreSQL DSN — container (postgres) or host CLI (localhost fallback)."""
    url = os.getenv("POSTGRES_CONN", "postgresql://edf:edf123@postgres:5432/edf_dw")
    override = os.getenv("EDA_PG_DSN", "").strip()
    if override:
        return override
    if "@postgres:" not in url:
        return url
    try:
        socket.gethostbyname("postgres")
    except OSError:
        return url.replace("@postgres:", "@localhost:")
    return url


def raw_data_dir() -> Path:
    """Directory of RTE XLS source files."""
    return Path(os.getenv("DATA_DIR", DATA_DIR))


def report_output_dir() -> Path:
    """Local directory for PNG artifacts (mirrored to MinIO)."""
    configured = os.getenv("REPORT_EDA_LOCAL", REPORT_EDA_LOCAL)
    path = Path(configured)
    if not path.is_absolute():
        root = Path(os.getenv("EDF_PROJECT_ROOT", Path.cwd()))
        path = root / configured
    path.mkdir(parents=True, exist_ok=True)
    return path


def report_destination_label(out: Path | None = None) -> str:
    out = out or report_output_dir()
    prefix = REPORT_EDA_S3_PREFIX.strip("/")
    s3_path = f"{prefix}/" if prefix else ""
    return f"{out}/ + s3a://{REPORT_EDA_BUCKET}/{s3_path}"


def sync_report_charts_to_minio(out: Path | None = None) -> list[str]:
    """Pousse vers MinIO tous les PNG EDA présents localement (filet de sécurité)."""
    out = out or report_output_dir()
    uploaded = sync_report_pngs(out, all_report_filenames())
    if uploaded:
        logger.info(
            "%d PNG synchronisé(s) → s3a://%s/%s",
            len(uploaded),
            REPORT_EDA_BUCKET,
            REPORT_EDA_S3_PREFIX.strip("/") or "",
        )
    return uploaded


def load_bronze_sample() -> pd.DataFrame:
    """Load RTE consumption rows from XLS (excludes Tempo calendar files)."""
    raw_dir = raw_data_dir()
    files = sorted(
        f
        for f in raw_dir.glob("eCO2mix*.xls")
        if "tempo" not in f.name.lower()
    )
    if not files:
        raise FileNotFoundError(f"No RTE XLS files in {raw_dir}")

    rows: list[dict[str, Any]] = []
    for path in files:
        for record in iter_xls_file(str(path)):
            rows.append(record)

    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    for col in KEY_NUMERIC:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["datetime"]).sort_values("datetime")


def load_silver_sample(raw_df: pd.DataFrame | None = None) -> pd.DataFrame | None:
    """Charge la couche Silver depuis PostgreSQL (dw.fact_consumption_silver)."""
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 unavailable — Silver comparison skipped")
        return None

    cols = [
        "datetime",
        "consumption_mw",
        "nuclear_mw",
        "wind_mw",
        "solar_mw",
        "hydro_mw",
        "gas_mw",
        "coal_mw",
        "fuel_mw",
        "bioenergy_mw",
        "co2_rate",
        "is_interpolated",
        "quality_score",
    ]
    sql = f"""
        SELECT {", ".join(cols)}
        FROM dw.fact_consumption_silver
        WHERE 1=1
    """
    params: list[Any] = []
    if raw_df is not None and not raw_df.empty:
        dt = pd.to_datetime(raw_df["datetime"], errors="coerce", utc=True).dropna()
        if not dt.empty:
            sql += " AND datetime >= %s AND datetime <= %s"
            params.extend([dt.min().to_pydatetime(), dt.max().to_pydatetime()])
    sql += " ORDER BY datetime"

    try:
        conn = psycopg2.connect(postgres_dsn())
        df = pd.read_sql(sql, conn, params=params or None)
        conn.close()
    except Exception as exc:
        logger.warning("Silver load failed (%s) — charts will show raw only", exc)
        return None

    if df.empty:
        logger.warning("Silver table empty — run make run-etl first")
        return None

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    for col in KEY_NUMERIC:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "quality_score" in df.columns:
        df["quality_score"] = pd.to_numeric(df["quality_score"], errors="coerce")
    return df.dropna(subset=["datetime"]).sort_values("datetime")


def load_ml_metrics() -> tuple[pd.DataFrame, str | None, str | None] | None:
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 unavailable — ML chart skipped")
        return None

    run_sql = """
        SELECT run_id, MAX(trained_at) AS trained_at
        FROM etl.model_metrics
        GROUP BY run_id
        ORDER BY trained_at DESC NULLS LAST
        LIMIT 1
    """
    metrics_sql = """
        SELECT model_name, rmse, mae, mape_pct, r2, train_time_s, trained_at, run_id
        FROM etl.model_metrics
        WHERE run_id = %s
        ORDER BY rmse ASC
    """
    fallback_sql = """
        WITH latest AS (
            SELECT MAX(trained_at) AS ts FROM etl.model_metrics
        )
        SELECT model_name, rmse, mae, mape_pct, r2, train_time_s, trained_at, run_id
        FROM etl.model_metrics
        WHERE trained_at >= (SELECT ts - INTERVAL '5 minutes' FROM latest)
        ORDER BY rmse ASC
    """
    try:
        conn = psycopg2.connect(postgres_dsn())
        cur = conn.cursor()
        cur.execute(run_sql)
        run_row = cur.fetchone()
        if not run_row:
            cur.close()
            conn.close()
            return None

        run_id, trained_at = run_row[0], run_row[1]
        if run_id:
            cur.execute(metrics_sql, (run_id,))
        else:
            cur.execute(fallback_sql)

        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        if not rows:
            return None

        df = pd.DataFrame(rows, columns=cols)
        ts_label = (
            pd.Timestamp(trained_at).strftime("%d/%m/%Y %H:%M UTC")
            if trained_at
            else None
        )
        return df, str(run_id) if run_id else None, ts_label

    except Exception as exc:
        logger.warning("Unable to load ML metrics: %s", exc)
        return None


def ml_artifact_dir() -> Path:
    """Répertoire local des artefacts ML (_report) pour les graphiques dashboard."""
    return resolve_model_local_path() / "_report"


def ensure_ml_artifacts_local() -> Path:
    """Télécharge les artefacts ML depuis MinIO si absents en local."""
    artifact_dir = ml_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    if (artifact_dir / "predictions.parquet").exists() or (artifact_dir / "learning_curve.parquet").exists():
        return artifact_dir

    try:
        n = sync_ml_report_artifacts(artifact_dir)
        logger.info("Sync artefacts ML MinIO → %s (%d objet(s))", artifact_dir, n)
    except Exception as exc:
        logger.warning("Sync artefacts ML échouée : %s", exc)

    if not (artifact_dir / "predictions.parquet").exists():
        try:
            sync_model_artifacts()
            sync_ml_report_artifacts(artifact_dir)
        except Exception as exc:
            logger.debug("sync_model_artifacts: %s", exc)

    return artifact_dir


def load_ml_artifacts() -> dict[str, Any]:
    """Charge prédictions, courbe RF et split depuis le disque local."""
    artifact_dir = ensure_ml_artifacts_local()
    out: dict[str, Any] = {"artifact_dir": str(artifact_dir)}

    pred_path = artifact_dir / "predictions.parquet"
    if pred_path.exists():
        try:
            out["predictions"] = pd.read_parquet(pred_path)
        except Exception as exc:
            logger.warning("Unable to read predictions: %s", exc)
            out["predictions"] = pd.DataFrame()

    curve_path = artifact_dir / "learning_curve.parquet"
    if curve_path.exists():
        try:
            out["learning_curve"] = pd.read_parquet(curve_path)
        except Exception as exc:
            logger.warning("Unable to read learning curve: %s", exc)
            out["learning_curve"] = pd.DataFrame()

    split_path = artifact_dir / "split_summary.json"
    if split_path.exists():
        try:
            import json

            out["split_info"] = json.loads(split_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Unable to read split summary: %s", exc)

    return out


def _persist_chart(local_path: str) -> str | None:
    uri = persist_report_file(local_path)
    if uri:
        logger.info("MinIO %s", uri)
    return uri


def _enrich_ml_subtitle(spec_index: int, metrics: pd.DataFrame) -> str:
    """Ajoute une phrase d'interprétation contextuelle au sous-titre du graphique."""
    _, _, base = ML_CHART_SPECS[spec_index]

    if spec_index != 3 or metrics.empty:
        return base

    ordered = metrics.sort_values("rmse").reset_index(drop=True)
    best = ordered.iloc[0]
    best_name = human_model_name(str(best["model_name"]))
    best_rmse = float(best["rmse"])

    linear = metrics[metrics["model_name"].str.contains("linear", case=False, na=False)]
    if not linear.empty and float(linear.iloc[0]["rmse"]) > best_rmse:
        lr_rmse = float(linear.iloc[0]["rmse"])
        reduction = (1.0 - best_rmse / lr_rmse) * 100.0
        return (
            f"{base} · {best_name} réduit l'erreur de {reduction:.0f} % "
            "par rapport à la régression linéaire"
        )

    if len(ordered) >= 2:
        worst_rmse = float(ordered.iloc[-1]["rmse"])
        reduction = (1.0 - best_rmse / worst_rmse) * 100.0
        worst_name = human_model_name(str(ordered.iloc[-1]["model_name"]))
        return f"{base} · {best_name} réduit l'erreur de {reduction:.0f} % vs {worst_name}"

    return base


def _save_dashboard_figure(
    fig: plt.Figure,
    spec_index: int,
    out: Path,
    *,
    subtitle: str | None = None,
) -> str:
    title, filename, default_subtitle = ML_CHART_SPECS[spec_index]

    add_accent_bar(fig)
    add_chart_header(fig, title, subtitle=subtitle or default_subtitle)
    add_report_footer(fig)

    if spec_index in (1, 2):
        bottom = 0.14 if spec_index == 2 else 0.12
        fig.subplots_adjust(left=0.08, right=0.98, top=HEADER_BOTTOM - 0.01, bottom=bottom)
        if spec_index == 2:
            fig.subplots_adjust(hspace=0.12, wspace=0.18)
    elif spec_index == 4:
        apply_figure_layout(fig, top=HEADER_BOTTOM - 0.02)
        fig.subplots_adjust(bottom=0.16)
    elif spec_index == 5:
        apply_figure_layout(fig, top=HEADER_BOTTOM - 0.02)
        fig.subplots_adjust(bottom=0.18)
    else:
        apply_figure_layout(fig)

    return save_report_figure(fig, str(out / filename))


def chart_consumption(df_raw: pd.DataFrame, out: Path, df_silver: pd.DataFrame | None = None) -> str:
    ts_raw = prepare_time_series(df_raw, "datetime", "consumption_mw")
    ts_silver = (
        prepare_time_series(df_silver, "datetime", "consumption_mw")
        if df_silver is not None and not df_silver.empty
        else None
    )

    fig, note = build_consumption_national_figure(ts_raw, ts_silver=ts_silver)
    subtitle = note or "Courbes superposées brute vs Silver · écarts colorés par type de correction"

    if df_silver is None or df_silver.empty:
        subtitle = f"{subtitle} · Silver indisponible — exécutez make run-etl".strip(" ·")

    add_accent_bar(fig)
    add_chart_header(fig, DATA_CHART_SPECS[0][0], subtitle=subtitle)
    add_report_footer(fig)

    return save_report_figure(fig, str(out / DATA_CHART_SPECS[0][1]))


def chart_monthly(df_raw: pd.DataFrame, out: Path, df_silver: pd.DataFrame | None = None) -> str:
    fig, note = build_monthly_comparison_figure(df_raw, df_silver)
    subtitle = note or "Agrégats mensuels TWh — barres groupées brute vs Silver"

    if df_silver is None or df_silver.empty:
        subtitle = f"{subtitle} · Silver indisponible".strip(" ·")

    add_accent_bar(fig)
    add_chart_header(fig, DATA_CHART_SPECS[1][0], subtitle=subtitle)
    add_report_footer(fig)

    return save_report_figure(fig, str(out / DATA_CHART_SPECS[1][1]))


def chart_quality(
    df_raw: pd.DataFrame,
    out: Path,
    df_silver: pd.DataFrame | None = None,
) -> str:
    cols = [c for c in KEY_NUMERIC if c in df_raw.columns]
    fig, note = build_quality_diagnostic_figure(df_raw, cols, df_silver)
    subtitle = note or "Dashboard 4 quadrants — complétude, anomalies, distribution, statistiques"

    add_accent_bar(fig)
    add_chart_header(fig, DATA_CHART_SPECS[2][0], subtitle=subtitle)
    add_report_footer(fig)

    return save_report_figure(fig, str(out / DATA_CHART_SPECS[2][1]))


def _remove_ml_pending(out: Path) -> None:
    pending = out / ML_PENDING
    if pending.exists():
        pending.unlink()


def chart_learning_curve(curve: pd.DataFrame, out: Path) -> str:
    fig, ax = plt.subplots(figsize=(11, 5.8))
    plot_learning_curve_rf(ax, curve)

    base = ML_CHART_SPECS[0][2]
    detail = describe_learning_curve(curve)

    return _save_dashboard_figure(fig, 0, out, subtitle=f"{base} · {detail}")


def chart_predictions_dispersion(preds: pd.DataFrame, out: Path) -> str:
    fig, _axes = build_predictions_dispersion_figure(preds)
    return _save_dashboard_figure(fig, 1, out)


def chart_predictions_timeseries(preds: pd.DataFrame, out: Path) -> str:
    fig, _axes = build_predictions_timeseries_figure(preds)
    return _save_dashboard_figure(fig, 2, out)


def chart_model_comparison(metrics: pd.DataFrame, out: Path) -> str:
    fig, _axes = build_model_comparison_figure(metrics)
    return _save_dashboard_figure(
        fig,
        3,
        out,
        subtitle=_enrich_ml_subtitle(3, metrics),
    )


def chart_ml_synthesis(metrics: pd.DataFrame, out: Path, *, best_model: str | None = None) -> str:
    fig, _axes = build_ml_synthesis_figure(metrics, best_model=best_model)
    foot = synthesis_footnote(metrics, best_model=best_model)
    base = ML_CHART_SPECS[4][2]

    return _save_dashboard_figure(fig, 4, out, subtitle=f"{base} · {foot}")


def chart_data_split(split_info: dict[str, Any], out: Path) -> str:
    fig, ax = plt.subplots(figsize=(9.5, 6.2))
    plot_data_split(ax, split_info)

    return _save_dashboard_figure(fig, 5, out)


def mark_ml_pending(out: Path | None = None) -> None:
    out = out or report_output_dir()

    for _title, name, _subtitle in ML_CHART_SPECS:
        path = out / name
        if path.exists():
            path.unlink()
        remove_report_from_s3(name)

    pending = out / ML_PENDING
    pending.write_text(
        "Dashboard ML en attente.\n"
        "Exécutez le job ML (DAG edf_ml_pipeline ou make run-gold-to-model), "
        "puis regénérez via make report-eda-ml.\n",
        encoding="utf-8",
    )


def _generate_and_persist_chart(label: str, builder: Callable[[], str]) -> str | None:
    """Génère un PNG puis l'envoie sur MinIO ; n'interrompt pas les autres graphiques."""
    try:
        path = builder()
        _persist_chart(path)
        return path
    except Exception as exc:
        logger.exception("Échec graphique %s : %s", label, exc)
        return None


def generate_data_charts(out: Path | None = None) -> list[str]:
    out = out or report_output_dir()
    apply_dashboard_style()

    logger.info("Loading RTE from %s", raw_data_dir())

    df_raw = load_bronze_sample()
    df_silver = load_silver_sample(df_raw)

    if df_silver is not None:
        logger.info("Silver loaded: %d rows for comparison", len(df_silver))
    else:
        logger.warning("Silver unavailable — data charts show raw source only (run make run-etl)")

    builders: list[tuple[str, Callable[..., str]]] = [
        ("consommation", lambda: chart_consumption(df_raw, out, df_silver)),
        ("mensuel", lambda: chart_monthly(df_raw, out, df_silver)),
        ("qualité", lambda: chart_quality(df_raw, out, df_silver)),
    ]

    paths: list[str] = []
    for label, builder in builders:
        path = _generate_and_persist_chart(label, builder)
        if path:
            paths.append(path)

    logger.info("%d data chart(s) -> %s", len(paths), report_destination_label(out))
    sync_report_charts_to_minio(out)

    return paths


def generate_ml_charts(
    out: Path | None = None,
    *,
    pending_if_missing: bool = True,
) -> list[str]:
    """Génère les tuiles ML (une métrique = un PNG, prêt pour Streamlit)."""
    out = out or report_output_dir()
    apply_dashboard_style()

    loaded = load_ml_metrics()
    if loaded is None:
        if pending_if_missing:
            mark_ml_pending(out)
            logger.info("ML dashboard pending — no metrics in etl.model_metrics")
        return []

    metrics, _run_id, _trained_at = loaded
    artifacts = load_ml_artifacts()

    preds = artifacts.get("predictions", pd.DataFrame())
    curve = artifacts.get("learning_curve", pd.DataFrame())
    split_info = artifacts.get("split_info") or {"n_train": 0, "n_test": 0}

    best_model = str(metrics.sort_values("rmse").iloc[0]["model_name"])

    _remove_ml_pending(out)

    ml_builders: list[tuple[str, Callable[[], str]]] = [
        ("comparaison_modèles", lambda: chart_model_comparison(metrics, out)),
        ("synthèse_ml", lambda: chart_ml_synthesis(metrics, out, best_model=best_model)),
    ]

    if not curve.empty:
        ml_builders.insert(0, ("courbe_apprentissage", lambda: chart_learning_curve(curve, out)))
    else:
        logger.warning(
            "Artefact courbe d'apprentissage absent (%s) — relancez make run-gold-to-model",
            ML_CHART_SPECS[0][1],
        )

    if not preds.empty:
        insert_at = 1 if not curve.empty else 0
        ml_builders.insert(insert_at, ("dispersion", lambda: chart_predictions_dispersion(preds, out)))
        ml_builders.insert(insert_at + 1, ("série_temporelle", lambda: chart_predictions_timeseries(preds, out)))
    else:
        logger.warning(
            "Artefact prédictions absent (%s, %s) — relancez make run-gold-to-model",
            ML_CHART_SPECS[1][1],
            ML_CHART_SPECS[2][1],
        )

    if split_info.get("n_train") or split_info.get("n_test"):
        ml_builders.append(("répartition_données", lambda: chart_data_split(split_info, out)))

    paths: list[str] = []
    for label, builder in ml_builders:
        path = _generate_and_persist_chart(label, builder)
        if path:
            paths.append(path)

    logger.info("ML dashboard — %d tile(s), best model: %s", len(paths), best_model)
    sync_report_charts_to_minio(out)

    return paths