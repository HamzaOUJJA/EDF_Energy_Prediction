# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/common/eda_style.py — Style EDA « rapport production » — chartes lisibles, export print-ready (300 DPI).
# =======================================================================

from __future__ import annotations

import textwrap
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from matplotlib import cm
from matplotlib.patches import Patch

# Palette corporate — énergie / data (contraste AA, fond clair)
COLORS = {
    "primary": "#0B3D91",
    "secondary": "#0D9488",
    "accent": "#EA580C",
    "series_light": "#93C5FD",
    "neutral": "#1E293B",
    "muted": "#64748B",
    "grid": "#E2E8F0",
    "missing": "#DC2626",
    "white": "#FFFFFF",
}

REPORT_DPI = 300
FOOTER_TEXT = "RTE éco2mix"
# Espace réservé en haut pour titre + sous-titre (fraction figure)
HEADER_BOTTOM = 0.80

# Dashboard ML — tuiles rapport / Streamlit
DASHBOARD = {
    "panel": "#F8FAFC",
    "panel_border": "#CBD5E1",
    "best_soft": "#ECFDF5",
    "bar": "#0B3D91",
    "bar_muted": "#CBD5E1",
    "line": "#0B3D91",
    "accent": "#EA580C",
    "accent_soft": "#FFF7ED",
    "text_muted": "#64748B",
    "scatter": "#3B82F6",
    "subtitle": "#475569",
}

# Performance rank — daltonien-friendly (ColorBrewer Blue / Orange + échelle vert → rouge)
PERFORMANCE_RANK_COLORS = ("#059669", "#65A30D", "#F59E0B", "#EA580C", "#DC2626")

# French labels for reports (avoid technical names in bars)
COLUMN_LABELS_FR: dict[str, str] = {
    "consumption_mw": "Consommation",
    "nuclear_mw": "Nucléaire",
    "wind_mw": "Éolien",
    "solar_mw": "Solaire",
    "hydro_mw": "Hydraulique",
    "gas_mw": "Gaz",
    "coal_mw": "Charbon",
    "fuel_mw": "Fioul",
    "bioenergy_mw": "Bioénergies",
    "co2_rate": "Taux CO₂",
    "forecast_j1_mw": "Prévision J-1",
    "forecast_j_mw": "Prévision J",
    "gas_cogen_mw": "Gaz — cogénération",
    "gas_tac_mw": "Gaz — TAC",
    "gas_ccg_mw": "Gaz — CCG",
    "wind_onshore_mw": "Éolien terrestre",
    "wind_offshore_mw": "Éolien offshore",
    "hydro_river_mw": "Hydraulique — fil de l'eau",
    "hydro_lake_mw": "Hydraulique — lacs",
    "hydro_step_mw": "Hydraulique — STEP",
    "battery_release_mw": "Déstockage batterie",
    "battery_storage_mw": "Stockage batterie",
    "quality_score": "Score qualité",
}


def human_label(column: str) -> str:
    """Returns a human-readable label for a column."""
    return COLUMN_LABELS_FR.get(column, column.replace("_mw", " (MW)").replace("_", " ").title())


def apply_production_style() -> None:
    """Matplotlib theme for deliverables PDF / slides."""
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": COLORS["grid"],
            "axes.labelcolor": COLORS["neutral"],
            "axes.titleweight": "bold",
            "axes.titlesize": 13,
            "axes.labelsize": 11,
            "xtick.color": COLORS["muted"],
            "ytick.color": COLORS["muted"],
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "grid.color": COLORS["grid"],
            "grid.linewidth": 0.6,
            "font.family": "sans-serif",
            "font.sans-serif": ["DejaVu Sans", "Arial", "Helvetica", "sans-serif"],
            "legend.fontsize": 9,
            "legend.frameon": False,
        }
    )


def apply_dashboard_style() -> None:
    """Thème graphiques ML — rendu moderne type dashboard analytique."""
    apply_production_style()
    plt.rcParams.update(
        {
            "axes.facecolor": DASHBOARD["panel"],
            "axes.titlesize": 12,
            "axes.titlepad": 10,
            "axes.labelsize": 10,
            "axes.titleweight": "600",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "figure.facecolor": COLORS["white"],
            "legend.fontsize": 9,
            "font.size": 10,
        }
    )


def _style_ax_dashboard(ax: plt.Axes, *, grid_axis: str = "y") -> None:
    ax.set_facecolor(DASHBOARD["panel"])
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color(DASHBOARD["panel_border"])
    ax.spines["bottom"].set_color(DASHBOARD["panel_border"])
    ax.spines["left"].set_linewidth(0.8)
    ax.spines["bottom"].set_linewidth(0.8)
    if grid_axis == "y":
        ax.grid(True, axis="y", alpha=0.55, linestyle="-", linewidth=0.5, color=COLORS["grid"])
    elif grid_axis == "both":
        ax.grid(True, alpha=0.4, linestyle="-", linewidth=0.45, color=COLORS["grid"])
    else:
        ax.grid(False)
    ax.set_axisbelow(True)
    ax.tick_params(colors=COLORS["muted"], length=0, pad=6)


def add_accent_bar(fig: plt.Figure, *, color: str | None = None) -> None:
    """Fine bande d'accent en haut de figure (identité visuelle)."""
    fig.patches.append(
        plt.Rectangle(
            (0, 1.0),
            1,
            0.012,
            transform=fig.transFigure,
            facecolor=color or COLORS["primary"],
            clip_on=False,
            linewidth=0,
            zorder=10,
        )
    )


def add_chart_header(
    fig: plt.Figure,
    title: str,
    *,
    subtitle: str | None = None,
    y_title: float = 0.96,
) -> None:
    """Titre + sous-titre explicatif (tuile dashboard)."""
    fig.suptitle(
        title,
        fontsize=15,
        fontweight="bold",
        color=COLORS["neutral"],
        y=y_title,
        ha="center",
    )
    if subtitle:
        wrapped = "\n".join(textwrap.wrap(subtitle, width=108))
        fig.text(
            0.5,
            y_title - 0.055,
            wrapped,
            ha="center",
            va="top",
            fontsize=9.5,
            color=DASHBOARD["subtitle"],
            linespacing=1.35,
        )


def apply_figure_layout(fig: plt.Figure, *, top: float = HEADER_BOTTOM, hspace: float | None = None) -> None:
    """Réserve l'espace header/footer sans rogner titre ni sous-titre."""
    if hspace is not None:
        fig.subplots_adjust(left=0.07, right=0.98, top=top, bottom=0.08, hspace=hspace, wspace=0.28)
    else:
        fig.tight_layout(rect=(0.02, 0.05, 0.98, top))


def add_report_footer(fig: plt.Figure, text: str = FOOTER_TEXT) -> None:
    """Pied de page source (bas droite)."""
    fig.text(
        0.99,
        0.008,
        text,
        ha="right",
        va="bottom",
        fontsize=8,
        color=COLORS["muted"],
        style="italic",
    )

def format_mw_axis(ax: plt.Axes, *, unit: str = "MW") -> None:
    """Formats the MW axis."""
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _pos: f"{x:,.0f}".replace(",", " "))
    )
    if ax.get_ylabel() in ("", "MW"):
        ax.set_ylabel(f"Puissance ({unit})")


def prepare_time_series(
    df: pd.DataFrame,
    datetime_col: str,
    value_col: str,
    *,
    gap_hours: int = 48,
) -> pd.DataFrame:
    """Sorts, converts dates and cuts lines on large data gaps."""
    out = df[[datetime_col, value_col]].copy()
    out[datetime_col] = pd.to_datetime(out[datetime_col], errors="coerce", utc=True)
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    out = out.dropna(subset=[datetime_col, value_col]).sort_values(datetime_col)
    if out.empty:
        return out
    gap = out[datetime_col].diff() > pd.Timedelta(hours=gap_hours)
    out.loc[gap, value_col] = float("nan")
    return out


def save_report_figure(fig: plt.Figure, path: str, *, dpi: int = REPORT_DPI) -> str:
    """Saves a report figure."""
    fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor="white", edgecolor="none")
    plt.close(fig)
    return path


MODEL_LABELS_FR: dict[str, str] = {
    "LinearRegression": "Régression linéaire",
    "DecisionTree": "Arbre de décision",
    "RandomForest": "Forêt aléatoire",
    "GradientBoosting": "Gradient Boosting",
}


def human_model_name(name: str) -> str:
    """Returns a human-readable model name."""
    return MODEL_LABELS_FR.get(name, name.replace("_", " "))


def _ordered_metrics(metrics: pd.DataFrame) -> pd.DataFrame:
    return metrics.sort_values("rmse", ascending=True).reset_index(drop=True)


def _rank_color(rank: int, total: int) -> str:
    idx = min(rank, len(PERFORMANCE_RANK_COLORS) - 1)
    if total <= 1:
        return PERFORMANCE_RANK_COLORS[0]
    scaled = int(rank * (len(PERFORMANCE_RANK_COLORS) - 1) / max(total - 1, 1))
    return PERFORMANCE_RANK_COLORS[scaled]


def _smooth_series(x: np.ndarray, y: np.ndarray, *, points: int = 200) -> tuple[np.ndarray, np.ndarray]:
    order = np.argsort(x)
    xs, ys = x[order], y[order]
    if len(xs) < 2:
        return xs, ys
    dense = np.linspace(xs.min(), xs.max(), points)
    smooth = np.interp(dense, xs, ys)
    return dense, smooth


def _learning_curve_columns(curve: pd.DataFrame) -> tuple[np.ndarray, np.ndarray | None, np.ndarray]:
    """Retourne (x, y_train, y_val) en évitant la colonne legacy rmse_mw."""
    df = curve.sort_values("train_rows")
    x = df["train_rows"].astype(float).to_numpy()
    if "rmse_validation_mw" in df.columns:
        y_val = df["rmse_validation_mw"].astype(float).to_numpy()
    elif "rmse_mw" in df.columns:
        y_val = df["rmse_mw"].astype(float).to_numpy()
    else:
        raise ValueError("learning_curve: colonne rmse_validation_mw absente")
    y_train = None
    if "rmse_train_mw" in df.columns:
        y_train = df["rmse_train_mw"].astype(float).to_numpy()
    return x, y_train, y_val


def describe_learning_curve(curve: pd.DataFrame) -> str:
    """Phrase d'interprétation pour la courbe d'apprentissage."""
    if curve.empty:
        return "Courbe indisponible — relancez le pipeline ML."
    x, y_train, y_val = _learning_curve_columns(curve)
    plateau_idx = int(np.argmin(y_val))
    plateau_rows = int(x[plateau_idx])
    plateau_rmse = float(y_val[plateau_idx])
    gap = float(y_val[plateau_idx] - y_train[plateau_idx]) if y_train is not None else float("nan")
    if y_train is not None and not np.isnan(gap):
        return (
            f"Validation minimale ~{plateau_rmse:,.0f} MW à {plateau_rows:,} lignes "
            f"(écart train/validation ~{gap:,.0f} MW)".replace(",", " ")
        )
    return f"Validation minimale ~{plateau_rmse:,.0f} MW à {plateau_rows:,} lignes".replace(",", " ")


def synthesis_footnote(metrics: pd.DataFrame, best_model: str | None = None) -> str:
    """Commentaire sous le tableau de synthèse."""
    ordered = _ordered_metrics(metrics)
    best_name = human_model_name(best_model or str(ordered.iloc[0]["model_name"]))
    gb = metrics[metrics["model_name"].str.contains("Gradient", case=False, na=False)]
    if gb.empty or "train_time_s" not in metrics.columns:
        return f"Modèle retenu : {best_name} (RMSE minimal). Validation sur période future."
    best_row = ordered.iloc[0]
    gb_row = gb.sort_values("rmse").iloc[0]
    delta = (float(gb_row["rmse"]) / float(best_row["rmse"]) - 1) * 100
    time_ratio = float(gb_row.get("train_time_s", 0) or 1) / max(float(best_row.get("train_time_s", 0) or 1), 0.1)
    return (
        f"Modèle retenu : {best_name} (RMSE minimal). "
        f"Gradient Boosting : précision proche (+{delta:.1f} %) mais ~{time_ratio:.0f}× plus lent."
    )


def _prediction_metrics(df: pd.DataFrame) -> dict[str, float]:
    actual = df["actual_mw"].astype(float)
    predicted = df["predicted_mw"].astype(float)
    err = actual - predicted
    rmse = float(np.sqrt((err**2).mean()))
    mae = float(np.abs(err).mean())
    mape = float((np.abs(err) / actual.replace(0, np.nan)).mean() * 100)
    ss_res = float((err**2).sum())
    ss_tot = float(((actual - actual.mean()) ** 2).sum())
    r2 = 1 - ss_res / ss_tot if ss_tot else float("nan")
    return {"rmse": rmse, "mae": mae, "mape": mape, "r2": r2}


def _metrics_table_rows(ordered: pd.DataFrame) -> tuple[list[str], list[list[str]], pd.DataFrame]:
    best_rmse = float(ordered["rmse"].min())
    table_cols = ["Algorithme", "RMSE (MW)", "MAE (MW)", "MAPE %", "R²", "Durée (s)", "Δ RMSE %"]
    table_rows: list[list[str]] = []
    raw = ordered.copy()
    raw["delta_rmse_pct"] = (raw["rmse"].astype(float) / best_rmse - 1.0) * 100.0

    for _, row in raw.iterrows():
        delta = float(row["delta_rmse_pct"])
        duration = float(row.get("train_time_s", 0) or 0)
        table_rows.append([
            human_model_name(str(row["model_name"])),
            f"{float(row['rmse']):,.0f}".replace(",", " "),
            f"{float(row['mae']):,.0f}".replace(",", " "),
            f"{float(row['mape_pct']):.1f}",
            f"{float(row['r2']):.3f}",
            f"{duration:.1f}",
            "0 %" if abs(delta) < 0.05 else f"+{delta:.1f} %",
        ])
    return table_cols, table_rows, raw


def _cell_heat_color(value: float, vmin: float, vmax: float, *, higher_is_better: bool) -> tuple[float, float, float, float]:
    if vmax <= vmin:
        t = 0.0
    else:
        t = (value - vmin) / (vmax - vmin)
    if not higher_is_better:
        t = 1.0 - t
    rgba = cm.RdYlGn(t)
    return (rgba[0], rgba[1], rgba[2], 0.35)


def plot_model_comparison_grouped(ax: plt.Axes, metrics: pd.DataFrame) -> str:
    """Barres groupées RMSE (% ref.) + pénalité (1−R²) — plus bas = meilleur."""
    _style_ax_dashboard(ax)
    ordered = _ordered_metrics(metrics)
    best_name = str(ordered.iloc[0]["model_name"])
    best_rmse = float(ordered["rmse"].min())
    best_r2 = float(ordered["r2"].max())
    best_gap = max(1.0 - best_r2, 1e-6)

    labels = [human_model_name(str(n)) for n in ordered["model_name"]]
    rmse_pct = (ordered["rmse"].astype(float) / best_rmse * 100.0).tolist()
    r2_penalty_pct = ((1.0 - ordered["r2"].astype(float)) / best_gap * 100.0).tolist()
    rmse_raw = ordered["rmse"].astype(float).tolist()
    r2_raw = ordered["r2"].astype(float).tolist()

    n = len(labels)
    x = np.arange(n)
    width = 0.36
    colors = [_rank_color(i, n) for i in range(n)]

    ax.bar(
        x - width / 2,
        rmse_pct,
        width,
        color=colors,
        alpha=0.92,
        edgecolor="white",
        linewidth=0.8,
        label="RMSE (% du meilleur = 100 %)",
        zorder=3,
    )
    ax.bar(
        x + width / 2,
        r2_penalty_pct,
        width,
        color=colors,
        alpha=0.55,
        edgecolor="white",
        linewidth=0.8,
        hatch="//",
        label="Écart R² (1−R², % du meilleur)",
        zorder=3,
    )
    ax.axhline(100, color=COLORS["neutral"], linestyle=(0, (4, 4)), linewidth=1.2, alpha=0.7, zorder=2)
    ax.text(
        n - 0.05,
        100,
        " Référence (meilleur RMSE)",
        va="bottom",
        ha="right",
        fontsize=8,
        color=COLORS["muted"],
    )

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=12, ha="right")
    ax.set_ylabel("Indice relatif (% — 100 % = meilleur modèle)")
    ymax = max(max(rmse_pct), max(r2_penalty_pct), 100) * 1.18
    ax.set_ylim(0, ymax)

    for i, (rp, rpp, rmse, r2) in enumerate(zip(rmse_pct, r2_penalty_pct, rmse_raw, r2_raw)):
        delta = (rmse / best_rmse - 1.0) * 100.0
        delta_txt = "ref." if abs(delta) < 0.05 else f"+{delta:.1f} %"
        ax.text(
            x[i] - width / 2,
            rp + ymax * 0.015,
            f"{rmse:,.0f}\n{delta_txt}".replace(",", " "),
            ha="center",
            va="bottom",
            fontsize=8,
            fontweight="bold",
            color=colors[i],
        )
        ax.text(
            x[i] + width / 2,
            rpp + ymax * 0.015,
            f"R²={r2:.3f}",
            ha="center",
            va="bottom",
            fontsize=8,
            color=COLORS["neutral"],
        )

    ax.legend(loc="upper left", frameon=True, facecolor="white", edgecolor=DASHBOARD["panel_border"], fontsize=8.5)
    return best_name


def plot_metrics_detail_table(
    ax: plt.Axes,
    metrics: pd.DataFrame,
    *,
    footnote: str | None = None,
) -> str:
    """Tableau métriques avec dégradé et meilleures valeurs par colonne."""
    ordered = _ordered_metrics(metrics)
    best_name = str(ordered.iloc[0]["model_name"])
    table_cols, table_rows, raw = _metrics_table_rows(ordered)
    ax.axis("off")

    best_row_pos = {
        "RMSE (MW)": int(raw["rmse"].astype(float).values.argmin()),
        "MAE (MW)": int(raw["mae"].astype(float).values.argmin()),
        "MAPE %": int(raw["mape_pct"].astype(float).values.argmin()),
        "R²": int(raw["r2"].astype(float).values.argmax()),
    }
    if "train_time_s" in raw.columns:
        best_row_pos["Durée (s)"] = int(raw["train_time_s"].astype(float).values.argmin())
    numeric_specs: dict[str, tuple[str, bool]] = {
        "RMSE (MW)": ("rmse", False),
        "MAE (MW)": ("mae", False),
        "MAPE %": ("mape_pct", False),
        "R²": ("r2", True),
        "Durée (s)": ("train_time_s", False),
    }

    tbl = ax.table(
        cellText=table_rows,
        colLabels=table_cols,
        loc="upper center",
        cellLoc="center",
        colWidths=[0.19, 0.11, 0.11, 0.1, 0.08, 0.1, 0.1],
        bbox=[0, 0.12 if footnote else 0, 1, 0.88 if footnote else 1],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1.0, 1.55)

    col_ranges: dict[str, tuple[float, float]] = {}
    for label, (field, _hb) in numeric_specs.items():
        vals = raw[field].astype(float)
        col_ranges[label] = (float(vals.min()), float(vals.max()))

    for (row, col), cell in tbl.get_celld().items():
        cell.set_edgecolor(DASHBOARD["panel_border"])
        cell.set_linewidth(0.6)
        if row == 0:
            cell.set_facecolor(COLORS["primary"])
            cell.set_text_props(color="white", fontweight="bold")
            continue
        col_name = table_cols[col]
        is_best_row = table_rows[row - 1][0] == human_model_name(best_name)
        if col_name in numeric_specs:
            field, higher_is_better = numeric_specs[col_name]
            val = float(raw.iloc[row - 1][field])
            vmin, vmax = col_ranges[col_name]
            cell.set_facecolor(_cell_heat_color(val, vmin, vmax, higher_is_better=higher_is_better))
            if (row - 1) == best_row_pos.get(col_name):
                cell.set_text_props(fontweight="bold")
        elif is_best_row:
            cell.set_facecolor(DASHBOARD["best_soft"])
            cell.set_text_props(fontweight="bold")
        else:
            cell.set_facecolor("white" if row % 2 else DASHBOARD["panel"])
    if footnote:
        ax.text(
            0.5,
            0.02,
            footnote,
            transform=ax.transAxes,
            ha="center",
            va="bottom",
            fontsize=8.5,
            color=DASHBOARD["text_muted"],
            wrap=True,
        )
    return best_name


def plot_learning_curve_rf(
    ax: plt.Axes,
    curve: pd.DataFrame,
) -> None:
    """Courbe d'apprentissage lissée avec écart train/validation (axe X log)."""
    _style_ax_dashboard(ax, grid_axis="both")
    if curve.empty:
        ax.text(0.5, 0.5, "Courbe indisponible", ha="center", va="center", transform=ax.transAxes)
        return

    x, y_train, y_val = _learning_curve_columns(curve)

    # Bande d'incertitude optionnelle (multi-runs)
    if "rmse_validation_std_mw" in curve.columns:
        std = curve.sort_values("train_rows")["rmse_validation_std_mw"].astype(float).to_numpy()
        ax.fill_between(x, y_val - std, y_val + std, color=DASHBOARD["accent"], alpha=0.15, zorder=1)

    xs, ys_v = _smooth_series(x, y_val)
    plateau_idx = int(np.argmin(y_val))
    x_plateau = float(x[plateau_idx])
    y_val_at = float(y_val[plateau_idx])

    if y_train is not None:
        _, ys_t = _smooth_series(x, y_train)
        y_train_at = float(y_train[plateau_idx])
        gap_at_best = y_val_at - y_train_at
        y_mid = (y_train_at + y_val_at) / 2

        ax.fill_between(xs, ys_t, ys_v, alpha=0.18, color=DASHBOARD["accent"], zorder=2)
        ax.plot(xs, ys_t, color=COLORS["primary"], linewidth=2.6, label="Entraînement", zorder=4)
        ax.plot(xs, ys_v, color=DASHBOARD["accent"], linewidth=2.6, label="Validation", zorder=5)
        ax.scatter(x, y_train, s=52, color=COLORS["primary"], edgecolors="white", linewidths=1.5, zorder=6)
        ax.scatter(x, y_val, s=52, color=DASHBOARD["accent"], marker="s", edgecolors="white", linewidths=1.5, zorder=6)

        # Repère d'écart au meilleur point (entre les deux courbes, pas en bas à gauche)
        bracket_x = x_plateau * 1.06
        ax.plot(
            [x_plateau, bracket_x, bracket_x],
            [y_train_at, y_train_at, y_val_at],
            color=COLORS["secondary"],
            linewidth=1.4,
            linestyle="-",
            zorder=7,
        )
        ax.plot([x_plateau, bracket_x], [y_val_at, y_val_at], color=COLORS["secondary"], linewidth=1.4, zorder=7)
        ax.annotate(
            f"Écart\n{gap_at_best:,.0f} MW".replace(",", " "),
            xy=(bracket_x, y_mid),
            xytext=(14, 0),
            textcoords="offset points",
            ha="left",
            va="center",
            fontsize=9,
            fontweight="bold",
            color=COLORS["neutral"],
            bbox=dict(
                boxstyle="round,pad=0.35",
                facecolor="white",
                edgecolor=COLORS["secondary"],
                linewidth=1.2,
                alpha=0.97,
            ),
            zorder=10,
        )
    else:
        ax.plot(xs, ys_v, color=COLORS["primary"], linewidth=2.6, label="Validation")
        ax.scatter(x, y_val, s=52, color=COLORS["primary"], edgecolors="white", linewidths=1.5, zorder=6)

    y_all = np.concatenate([y_train, y_val]) if y_train is not None else y_val
    y_lo = max(500, float(np.floor(y_all.min() / 50) * 50) - 50)
    y_hi = min(2000, float(np.ceil(y_all.max() / 50) * 50) + 50)
    ax.set_ylim(y_lo, y_hi)

    ax.axvline(x_plateau, color=COLORS["secondary"], linestyle=(0, (4, 3)), linewidth=1.4, alpha=0.9, zorder=3)
    ax.text(
        x_plateau,
        y_hi * 0.97,
        "  Meilleur compromis",
        va="top",
        ha="left",
        fontsize=8,
        color=COLORS["secondary"],
    )

    x_final = float(x.max())
    if abs(x_final - x_plateau) > x_final * 0.02:
        ax.axvline(x_final, color=COLORS["muted"], linestyle=(0, (2, 4)), linewidth=1.2, alpha=0.75, zorder=3)
        ax.text(
            x_final,
            y_hi * 0.82,
            "  Taille retenue",
            va="top",
            ha="left",
            fontsize=8,
            color=COLORS["muted"],
        )

    ax.set_xscale("log")
    ax.set_xlabel("Taille du jeu d'entraînement (lignes, échelle log)")
    ax.set_ylabel("RMSE (MW)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _pos: f"{int(v):,}".replace(",", " ")))
    ax.legend(
        loc="upper left",
        frameon=True,
        facecolor="white",
        edgecolor=DASHBOARD["panel_border"],
        fontsize=9,
    )


def _contiguous_segments(
    index: pd.DatetimeIndex,
    *,
    max_gap: pd.Timedelta = pd.Timedelta(hours=24),
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Découpe une série temporelle aux trous > max_gap."""
    if len(index) == 0:
        return []
    gaps = index.to_series().diff()
    break_at = gaps[gaps > max_gap].index
    segments: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    seg_start = index[0]
    for ts in break_at:
        prev = index[index.get_loc(ts) - 1]
        segments.append((seg_start, prev))
        seg_start = ts
    segments.append((seg_start, index[-1]))
    return segments


def _plot_time_segments(
    ax: plt.Axes,
    series: pd.Series,
    *,
    color: str,
    linewidth: float,
    alpha: float,
    label: str,
    zorder: int,
    linestyle: str = "-",
) -> None:
    """Trace une série sans relier les segments séparés par un trou."""
    labeled = False
    for start, end in _contiguous_segments(series.index):
        seg = series.loc[start:end]
        if seg.empty:
            continue
        plot_label = label if not labeled else "_nolegend_"
        ax.plot(
            seg.index,
            seg.values,
            color=color,
            linewidth=linewidth,
            alpha=alpha,
            label=plot_label,
            zorder=zorder,
            linestyle=linestyle,
        )
        labeled = True


def _segment_predictions_df(df: pd.DataFrame) -> list[pd.DataFrame]:
    """Découpe le jeu de test aux trous > 24 h."""
    if df.empty:
        return []
    ordered = df.sort_values("datetime")
    gaps = ordered["datetime"].diff() > pd.Timedelta(hours=24)
    seg_ids = gaps.cumsum()
    return [group.copy() for _, group in ordered.groupby(seg_ids)]


def _segment_caption(df: pd.DataFrame, *, index: int | None = None, total: int | None = None) -> str:
    start = df["datetime"].min().strftime("%d %b %Y")
    end = df["datetime"].max().strftime("%d %b %Y")
    n = len(df)
    if index is not None and total is not None and total > 1:
        return f"Période {index}/{total} · {n:,} pts · {start} → {end}".replace(",", " ")
    return f"{n:,} pts · {start} → {end}".replace(",", " ")


def _prepare_predictions_df(preds: pd.DataFrame) -> pd.DataFrame:
    df = preds.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    df["actual_mw"] = pd.to_numeric(df["actual_mw"], errors="coerce")
    df["predicted_mw"] = pd.to_numeric(df["predicted_mw"], errors="coerce")
    return df.dropna(subset=["actual_mw", "predicted_mw"])


def _lowess_curve(
    x: np.ndarray,
    y: np.ndarray,
    *,
    frac: float = 0.08,
    n_points: int = 300,
) -> tuple[np.ndarray, np.ndarray]:
    """Régression locale lissée (LOESS) — détection de biais systématique."""
    order = np.argsort(x)
    xs, ys = x[order], y[order]
    if len(xs) < 4:
        return xs, ys
    x_grid = np.linspace(float(xs.min()), float(xs.max()), n_points)
    y_smooth = np.empty(n_points)
    n = len(xs)
    k = max(int(frac * n), 3)
    for i, x0 in enumerate(x_grid):
        dist = np.abs(xs - x0)
        h = float(np.sort(dist)[min(k, n - 1)])
        h = max(h, 1e-9)
        w = np.clip(1.0 - (dist / h) ** 3, 0.0, 1.0) ** 3
        wsum = float(w.sum())
        if wsum < 1e-12:
            y_smooth[i] = float(ys[np.argmin(dist)])
            continue
        x_mean = float((w * xs).sum() / wsum)
        y_mean = float((w * ys).sum() / wsum)
        num = float((w * (xs - x_mean) * (ys - y_mean)).sum())
        den = float((w * (xs - x_mean) ** 2).sum())
        if den < 1e-12:
            y_smooth[i] = y_mean
        else:
            slope = num / den
            intercept = y_mean - slope * x_mean
            y_smooth[i] = intercept + slope * x0
    return x_grid, y_smooth


def _french_public_holidays(start: pd.Timestamp, end: pd.Timestamp) -> set[pd.Timestamp]:
    """Jours fériés fixes France (UTC, normalisés à minuit)."""
    fixed = {(1, 1), (5, 1), (5, 8), (7, 14), (8, 15), (11, 1), (11, 11), (12, 25)}
    days: set[pd.Timestamp] = set()
    for year in range(start.year, end.year + 1):
        for month, day in fixed:
            ts = pd.Timestamp(year=year, month=month, day=day, tz="UTC")
            if start.normalize() <= ts <= end.normalize():
                days.add(ts.normalize())
    return days


def _shade_weekends_and_holidays(
    ax: plt.Axes,
    t_min: pd.Timestamp,
    t_max: pd.Timestamp,
    *,
    y_lo: float,
    y_hi: float,
) -> None:
    """Bandes verticales grises pour week-ends et jours fériés."""
    _ = y_lo, y_hi
    holidays = _french_public_holidays(t_min, t_max)
    day = t_min.normalize()
    end_day = t_max.normalize()
    while day <= end_day:
        if day.dayofweek >= 5 or day in holidays:
            ax.axvspan(
                day,
                day + pd.Timedelta(days=1),
                ymin=0,
                ymax=1,
                facecolor="#E2E8F0",
                alpha=0.55,
                zorder=0,
            )
        day += pd.Timedelta(days=1)


def _style_predictions_ax(ax: plt.Axes, *, grid_axis: str = "both") -> None:
    """Grille discrète pour graphiques prédictions (spec rapport)."""
    ax.set_facecolor("white")
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color(COLORS["grid"])
    ax.spines["bottom"].set_color(COLORS["grid"])
    ax.tick_params(labelsize=11, colors=COLORS["neutral"])
    ax.xaxis.label.set_size(12)
    ax.yaxis.label.set_size(12)
    if grid_axis in ("both", "x"):
        ax.grid(True, axis="x", linestyle="--", alpha=0.5, color=COLORS["grid"], linewidth=0.6)
    if grid_axis in ("both", "y"):
        ax.grid(True, axis="y", linestyle="--", alpha=0.5, color=COLORS["grid"], linewidth=0.6)
    ax.set_axisbelow(True)


def _format_prediction_dates(ax: plt.Axes) -> None:
    """Dates lisibles avec année (période de test sur plusieurs années)."""
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b %Y"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=8))
    for label in ax.get_xticklabels():
        label.set_rotation(35)
        label.set_ha("right")


def plot_predictions_dispersion(ax: plt.Axes, preds: pd.DataFrame) -> None:
    """Graphique A — dispersion réel vs prédit (jeu de test)."""
    _style_predictions_ax(ax, grid_axis="both")
    if preds.empty:
        ax.text(0.5, 0.5, "Prédictions indisponibles", ha="center", va="center", transform=ax.transAxes)
        return

    df = _prepare_predictions_df(preds)
    actual = df["actual_mw"].to_numpy(dtype=float)
    predicted = df["predicted_mw"].to_numpy(dtype=float)
    stats = _prediction_metrics(df)
    band = 2.0 * stats["rmse"]

    lo = float(min(actual.min(), predicted.min()))
    hi = float(max(actual.max(), predicted.max()))
    pad = (hi - lo) * 0.03 or 100.0
    lo_p, hi_p = lo - pad, hi + pad

    diag = np.linspace(lo_p, hi_p, 200)
    ax.fill_between(
        diag,
        diag - band,
        diag + band,
        color="#94A3B8",
        alpha=0.22,
        zorder=1,
        label=f"Bande ±2×RMSE (±{band:,.0f} MW)".replace(",", " "),
    )
    ax.plot(diag, diag, color="#0F172A", linewidth=1.0, linestyle="--", zorder=4, label="Référence y = x")

    err_abs = np.abs(actual - predicted)
    in_band = err_abs <= band
    ax.scatter(
        actual[in_band],
        predicted[in_band],
        s=14,
        c="#2563EB",
        alpha=0.35,
        linewidths=0,
        zorder=2,
        label=f"|erreur| ≤ 2×RMSE ({int(in_band.sum()):,})".replace(",", " "),
    )
    ax.scatter(
        actual[~in_band],
        predicted[~in_band],
        s=22,
        c="#DC2626",
        alpha=0.75,
        linewidths=0.3,
        edgecolors="#991B1B",
        zorder=3,
        label=f"|erreur| > 2×RMSE ({int((~in_band).sum()):,})".replace(",", " "),
    )

    loess_x, loess_y = _lowess_curve(actual, predicted)
    ax.plot(
        loess_x,
        loess_y,
        color="#EA580C",
        linewidth=2.2,
        linestyle="-",
        zorder=5,
        label="LOESS (biais local)",
    )

    ax.set_xlim(lo_p, hi_p)
    ax.set_ylim(lo_p, hi_p)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xlabel("Consommation réelle (MW)")
    ax.set_ylabel("Prediction du modele retenu (MW)")
    format_mw_axis(ax, unit="MW")

    metrics_text = (
        f"R² = {stats['r2']:.3f}\n"
        f"RMSE = {stats['rmse']:,.0f} MW\n"
        f"MAE = {stats['mae']:,.0f} MW\n"
        f"MAPE = {stats['mape']:.1f} %"
    ).replace(",", " ")
    ax.text(
        0.02,
        0.98,
        metrics_text,
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=11,
        color=COLORS["neutral"],
        bbox=dict(boxstyle="round,pad=0.45", facecolor="white", edgecolor=COLORS["grid"], alpha=0.97),
        zorder=10,
    )
    ax.legend(
        loc="lower right",
        frameon=True,
        facecolor="white",
        edgecolor=COLORS["grid"],
        fontsize=9,
    )


def _segment_mae(df: pd.DataFrame) -> float:
    err = df["actual_mw"].astype(float) - df["predicted_mw"].astype(float)
    return float(np.abs(err).mean())


def _plot_predictions_timeseries_block(
    ax_top: plt.Axes,
    ax_bottom: plt.Axes,
    seg: pd.DataFrame,
    *,
    band: float,
    err_ylim: float,
    column_title: str,
    show_legend_top: bool,
    show_legend_bottom: bool,
    show_ylabel: bool,
    show_threshold_labels: bool,
) -> None:
    """Un bloc temporel : courbes (haut) + barres d'écart (bas)."""
    _style_predictions_ax(ax_top, grid_axis="y")
    _style_predictions_ax(ax_bottom, grid_axis="both")

    df = seg.sort_values("datetime")
    indexed = df.set_index("datetime")
    t_min, t_max = indexed.index.min(), indexed.index.max()

    roll = indexed[["actual_mw", "predicted_mw"]].rolling("6h", min_periods=1).mean()
    y_min = float(roll[["actual_mw", "predicted_mw"]].min().min())
    y_max = float(roll[["actual_mw", "predicted_mw"]].max().max())
    margin = max((y_max - y_min) * 0.05, 600.0)
    y_lo, y_hi = y_min - margin, y_max + margin

    ax_top.set_xlim(t_min, t_max)
    ax_top.set_ylim(y_lo, y_hi)
    _shade_weekends_and_holidays(ax_top, t_min, t_max, y_lo=y_lo, y_hi=y_hi)

    _plot_time_segments(
        ax_top,
        roll["predicted_mw"],
        color="#2563EB",
        linewidth=2.0,
        alpha=0.8,
        label="Prédit (MM 6 h)",
        zorder=3,
    )
    _plot_time_segments(
        ax_top,
        roll["actual_mw"],
        color="#0F172A",
        linewidth=2.4,
        alpha=1.0,
        label="Réel (MM 6 h)",
        zorder=4,
    )

    ax_top.set_title(column_title, fontsize=10, fontweight="bold", color=COLORS["neutral"], pad=8)
    if show_ylabel:
        ax_top.set_ylabel("Puissance (MW)")
        format_mw_axis(ax_top)
    else:
        ax_top.tick_params(labelleft=False)
    if show_legend_top:
        ax_top.legend(loc="upper right", frameon=True, facecolor="white", edgecolor=COLORS["grid"], fontsize=8)
    ax_top.tick_params(labelbottom=False)

    display = df.iloc[::6].copy()
    err = (display["actual_mw"] - display["predicted_mw"]).to_numpy(dtype=float)
    dates = display["datetime"]
    step = pd.Series(indexed.index).diff().median()
    bar_width = step.total_seconds() / 86400.0 * 5.0 if pd.notna(step) else 0.02
    bar_colors = np.where(err > 0, "#22C55E", "#DC2626")

    ax_bottom.bar(
        dates,
        err,
        width=bar_width,
        color=bar_colors,
        alpha=0.85,
        align="center",
        zorder=2,
    )

    ax_bottom.axhline(0, color="#0F172A", linewidth=1.0, zorder=3)
    ax_bottom.axhline(band, color="#64748B", linewidth=0.9, linestyle=(0, (4, 4)), zorder=1)
    ax_bottom.axhline(-band, color="#64748B", linewidth=0.9, linestyle=(0, (4, 4)), zorder=1)
    if show_threshold_labels:
        ax_bottom.text(
            t_max,
            band,
            f" +2×RMSE ({band:,.0f} MW)".replace(",", " "),
            ha="right",
            va="bottom",
            fontsize=8,
            color=COLORS["muted"],
        )
        ax_bottom.text(
            t_max,
            -band,
            f" −2×RMSE ({-band:,.0f} MW)".replace(",", " "),
            ha="right",
            va="top",
            fontsize=8,
            color=COLORS["muted"],
        )

    ax_bottom.set_ylim(-err_ylim, err_ylim)
    if show_ylabel:
        ax_bottom.set_ylabel("Écart réel − prédit (MW)")
    else:
        ax_bottom.tick_params(labelleft=False)
    ax_bottom.set_xlabel("Date (UTC)")
    _format_prediction_dates(ax_bottom)

    if show_legend_bottom:
        legend_patches = [
            Patch(facecolor="#22C55E", edgecolor="none", alpha=0.85, label="Sous-estimation (réel > prédit)"),
            Patch(facecolor="#DC2626", edgecolor="none", alpha=0.85, label="Sur-estimation (réel < prédit)"),
        ]
        ax_bottom.legend(
            handles=legend_patches,
            loc="upper left",
            frameon=True,
            facecolor="white",
            edgecolor=COLORS["grid"],
            fontsize=8,
        )


def build_predictions_dispersion_figure(preds: pd.DataFrame) -> tuple[plt.Figure, tuple[plt.Axes, ...]]:
    fig, ax = plt.subplots(figsize=(10.5, 10))
    plot_predictions_dispersion(ax, preds)
    return fig, (ax,)


def build_predictions_timeseries_figure(preds: pd.DataFrame) -> tuple[plt.Figure, tuple[plt.Axes, ...]]:
    """Graphique B — un panneau par bloc temporel contigu (côte à côte)."""
    full = _prepare_predictions_df(preds)
    segments = _segment_predictions_df(full)
    if not segments:
        segments = [full]

    n_seg = len(segments)
    fig_w = max(7.0 * n_seg, 14.0)
    fig, axes_grid = plt.subplots(
        2,
        n_seg,
        figsize=(fig_w, 8.8),
        sharey="row",
        gridspec_kw={"height_ratios": [2.2, 1.0], "hspace": 0.12, "wspace": 0.18},
    )
    if n_seg == 1:
        axes_grid = np.array([[axes_grid[0]], [axes_grid[1]]])

    stats = _prediction_metrics(full) if not full.empty else {"rmse": 0.0}
    band = 2.0 * stats["rmse"]
    if not full.empty:
        err_all = (full["actual_mw"] - full["predicted_mw"]).to_numpy(dtype=float)
        err_ylim = max(float(np.abs(err_all).max()), band) * 1.15
    else:
        err_ylim = band * 1.15

    axes_flat: list[plt.Axes] = []
    for col, seg in enumerate(segments):
        ax_top = axes_grid[0, col]
        ax_bottom = axes_grid[1, col]
        mae = _segment_mae(seg)
        caption = _segment_caption(seg, index=col + 1, total=n_seg)
        column_title = f"Bloc {col + 1}/{n_seg} · MAE {mae:,.0f} MW\n{caption}".replace(",", " ")
        _plot_predictions_timeseries_block(
            ax_top,
            ax_bottom,
            seg,
            band=band,
            err_ylim=err_ylim,
            column_title=column_title,
            show_legend_top=(col == n_seg - 1),
            show_legend_bottom=(col == 0),
            show_ylabel=(col == 0),
            show_threshold_labels=(col == n_seg - 1),
        )
        axes_flat.extend([ax_top, ax_bottom])

    if n_seg > 1:
        fig.text(
            0.5,
            0.01,
            "Jeu de test : blocs disjoints — absence de données RTE entre les périodes (non interpolée).",
            ha="center",
            va="bottom",
            fontsize=9,
            color=DASHBOARD["text_muted"],
            style="italic",
        )

    return fig, tuple(axes_flat)


def plot_data_split(
    ax: plt.Axes,
    split_info: dict[str, Any],
) -> None:
    """Camembert — effectifs train / test (split temporel 80/20)."""
    n_train = int(split_info.get("n_train", 0))
    n_test = int(split_info.get("n_test", 0))
    total = n_train + n_test
    if total <= 0:
        ax.text(0.5, 0.5, "Split indisponible", ha="center", va="center", transform=ax.transAxes)
        return

    ax.set_facecolor(DASHBOARD["panel"])
    pct_train = 100 * n_train / total
    pct_test = 100 * n_test / total
    sizes = [n_train, n_test]
    colors = [DASHBOARD["bar"], DASHBOARD["accent"]]
    legend_labels = [
        f"Entraînement · {n_train:,} obs. ({pct_train:.0f} %)".replace(",", " "),
        f"Test · {n_test:,} obs. ({pct_test:.0f} %)".replace(",", " "),
    ]

    def _autopct(pct: float) -> str:
        count = int(round(pct * total / 100.0))
        return f"{pct:.0f} %\n{count:,} obs.".replace(",", " ")

    wedges, _texts, autotexts = ax.pie(
        sizes,
        colors=colors,
        startangle=90,
        counterclock=False,
        autopct=_autopct,
        pctdistance=0.72,
        wedgeprops={"edgecolor": "white", "linewidth": 2.0, "alpha": 0.95},
    )
    for t in autotexts:
        t.set_fontsize(11)
        t.set_fontweight("bold")
        t.set_color(COLORS["white"])

    ax.legend(
        wedges,
        legend_labels,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=True,
        facecolor="white",
        edgecolor=DASHBOARD["panel_border"],
        fontsize=10,
    )
    ax.set_aspect("equal")

    def _fmt_period(start_key: str, end_key: str) -> str:
        try:
            s = pd.Timestamp(split_info.get(start_key)).strftime("%d/%m/%Y")
            e = pd.Timestamp(split_info.get(end_key)).strftime("%d/%m/%Y")
            return f"{s} → {e}"
        except Exception:
            return "—"

    ax.text(
        0.5,
        1.02,
        f"Split temporel 80/20 · {total:,} enregistrements · non aléatoire".replace(",", " "),
        transform=ax.transAxes,
        ha="center",
        va="bottom",
        fontsize=10,
        fontweight="bold",
        color=COLORS["neutral"],
    )
    caption = (
        f"Entraînement : {_fmt_period('train_start', 'train_end')}   ·   "
        f"Test : {_fmt_period('test_start', 'test_end')}\n"
        "Découpage temporel strict : les données de test sont postérieures à l'entraînement."
    )
    ax.text(0.5, -0.08, caption, transform=ax.transAxes, ha="center", fontsize=8.5, color=DASHBOARD["text_muted"])


def build_model_comparison_figure(metrics: pd.DataFrame) -> tuple[plt.Figure, tuple[plt.Axes]]:
    """Figure unique — barres groupées RMSE + pénalité R²."""
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    plot_model_comparison_grouped(ax, metrics)
    return fig, (ax,)


def build_ml_synthesis_figure(
    metrics: pd.DataFrame,
    *,
    best_model: str | None = None,
) -> tuple[plt.Figure, tuple[plt.Axes]]:
    """Tableau comparatif unique avec note de sélection."""
    fig, ax = plt.subplots(figsize=(12, 5.8))
    footnote = synthesis_footnote(metrics, best_model=best_model)
    plot_metrics_detail_table(ax, metrics, footnote=footnote)
    return fig, (ax,)


# --- Graphiques EDA données (rapport professionnel) ---------------------------------

# Bornes alignées sur spark/transform/silver.py (clean)
SILVER_CONSO_MIN_MW = 10_000.0
SILVER_CONSO_MAX_MW = 120_000.0

# Différenciation visuelle — bonnes pratiques EDA (brut vs traité)
DATA_LAYER = {
    "raw": "#94A3B8",
    "raw_label": "Données brutes (source XLS)",
    "silver": "#0B3D91",
    "silver_label": "Données nettoyées (Silver ETL)",
}

CORRECTION_COLORS = {
    "unchanged": "#059669",
    "minor": "#2563EB",
    "major": "#EA580C",
    "removed": "#DC2626",
}

CORRECTION_LABELS = {
    "unchanged": "Inchangé",
    "minor": "Correction mineure (imputation)",
    "major": "Correction majeure",
    "removed": "Anomalie supprimée",
}


def _is_consumption_outlier(values: pd.Series) -> pd.Series:
    cons = pd.to_numeric(values, errors="coerce")
    return cons.isna() | (cons < 0) | (cons < SILVER_CONSO_MIN_MW) | (cons > SILVER_CONSO_MAX_MW)


def _dedupe_raw(raw_df: pd.DataFrame) -> pd.DataFrame:
    work = raw_df.copy()
    work["datetime"] = _normalize_eda_datetime(work["datetime"])
    return work.sort_values("datetime").drop_duplicates("datetime", keep="last")


def _normalize_eda_datetime(series: pd.Series) -> pd.Series:
    """UTC + arrondi 30 min pour aligner XLS et PostgreSQL Silver."""
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    return dt.dt.floor("30min")


def _build_point_comparison(raw_df: pd.DataFrame, silver_df: pd.DataFrame) -> pd.DataFrame:
    raw = _dedupe_raw(raw_df)
    silver = silver_df.copy()
    silver["datetime"] = _normalize_eda_datetime(silver["datetime"])
    if "is_interpolated" not in silver.columns:
        silver["is_interpolated"] = False
    merged = raw[["datetime", "consumption_mw"]].merge(
        silver[["datetime", "consumption_mw", "is_interpolated"]],
        on="datetime",
        how="outer",
        suffixes=("_raw", "_silver"),
    ).sort_values("datetime")

    raw_v = pd.to_numeric(merged["consumption_mw_raw"], errors="coerce")
    silver_v = pd.to_numeric(merged["consumption_mw_silver"], errors="coerce")
    interp = merged["is_interpolated"].fillna(False).astype(bool)
    raw_out = _is_consumption_outlier(raw_v)
    silver_out = _is_consumption_outlier(silver_v)
    rel = (raw_v - silver_v).abs() / raw_v.abs().clip(lower=1.0)

    correction = pd.Series("unchanged", index=merged.index, dtype="object")
    correction.loc[raw_v.notna() & silver_v.isna()] = "removed"
    correction.loc[raw_v.isna() & silver_v.notna()] = "minor"
    both = raw_v.notna() & silver_v.notna()
    removed = both & raw_out & ~silver_out
    major = both & ~removed & (rel > 0.05)
    minor = both & ~removed & ~major & (interp | raw_out | (rel > 0.01))
    correction.loc[removed] = "removed"
    correction.loc[major] = "major"
    correction.loc[minor] = "minor"
    merged["correction"] = correction
    return merged


def _comparison_summary(merged: pd.DataFrame, raw_df: pd.DataFrame) -> dict[str, Any]:
    n_raw = len(raw_df)
    n_dedup = len(_dedupe_raw(raw_df))
    n_silver = int(merged["consumption_mw_silver"].notna().sum())
    modified = merged[merged["correction"] != "unchanged"]
    interp = int((merged["correction"] == "minor").sum())
    removed = int((merged["correction"] == "removed").sum())
    return {
        "n_raw": n_raw,
        "n_dedup": n_dedup,
        "n_silver": n_silver,
        "n_modified": len(modified),
        "pct_modified": 100.0 * len(modified) / max(len(merged), 1),
        "pct_imputed": 100.0 * interp / max(n_silver, 1),
        "n_removed": removed,
        "duplicates": n_raw - n_dedup,
    }


def _anomaly_heatmap_matrix(
    df: pd.DataFrame,
    numeric_cols: list[str],
    top_n: int = 6,
) -> tuple[np.ndarray, list[str], list[str]]:
    work = df.copy()
    work["datetime"] = pd.to_datetime(work["datetime"], errors="coerce", utc=True)
    work = work.dropna(subset=["datetime"])
    if work.empty:
        return np.empty((0, 0)), [], []
    work["month"] = work["datetime"].dt.tz_convert(None).dt.to_period("M").astype(str)
    rates = {}
    for c in numeric_cols:
        if c not in work.columns:
            continue
        cons = pd.to_numeric(work.get("consumption_mw"), errors="coerce") if c == "consumption_mw" else None
        if c == "consumption_mw":
            flag = work[c].isna() | _is_consumption_outlier(work[c])
        else:
            flag = work[c].isna()
        rates[c] = flag.groupby(work["month"]).mean().to_dict()
    cols = sorted(
        [c for c in numeric_cols if c in rates],
        key=lambda c: np.mean(list(rates[c].values())) if rates[c] else 0,
        reverse=True,
    )[:top_n]
    if not cols:
        return np.empty((0, 0)), [], []
    months = sorted({m for c in cols for m in rates[c]})
    matrix = np.array([[rates[c].get(m, 0.0) * 100.0 for m in months] for c in cols])
    return matrix, [human_label(c) for c in cols], [m[2:7] if len(m) >= 7 else m for m in months]


def _consumption_stats(cons: pd.Series) -> dict[str, float]:
    clean = pd.to_numeric(cons, errors="coerce").dropna()
    if clean.empty:
        return {"mean": np.nan, "std": np.nan, "min": np.nan, "max": np.nan, "outliers": 0}
    outlier = _is_consumption_outlier(clean).sum()
    return {
        "mean": float(clean.mean()),
        "std": float(clean.std(ddof=0)),
        "min": float(clean.min()),
        "max": float(clean.max()),
        "outliers": int(outlier),
    }


def _subsample_series(ts: pd.DataFrame, datetime_col: str, max_points: int = 8_000) -> pd.DataFrame:
    if len(ts) <= max_points:
        return ts
    step = max(len(ts) // max_points, 1)
    return ts.iloc[::step].copy()


def _rolling_mean_dataframe(
    ts: pd.DataFrame,
    datetime_col: str,
    value_col: str,
    *,
    window: str = "7D",
    min_periods: int = 72,
) -> pd.DataFrame:
    """Moyenne mobile pour lisser la comparaison brute vs Silver."""
    if ts.empty or len(ts) < min_periods:
        return pd.DataFrame(columns=[datetime_col, value_col])
    work = ts[[datetime_col, value_col]].copy().sort_values(datetime_col)
    ma = (
        work.set_index(datetime_col)[value_col]
        .rolling(window, min_periods=min_periods)
        .mean()
        .dropna()
    )
    return ma.reset_index()


def _shade_interp_blocks(ax: plt.Axes, merged: pd.DataFrame, *, alpha: float = 0.06) -> None:
    """Zones d'imputation Silver (is_interpolated), pas les trous bruts non appariés."""
    if "is_interpolated" not in merged.columns:
        return
    mask = merged["is_interpolated"].fillna(False).astype(bool)
    if not mask.any():
        return
    block_id = (mask != mask.shift()).cumsum()
    for bid in block_id[mask].unique():
        block = merged.loc[block_id == bid]
        if block.empty:
            continue
        ax.axvspan(
            block["datetime"].iloc[0],
            block["datetime"].iloc[-1],
            color=COLORS["missing"],
            alpha=alpha,
            zorder=1,
        )


def _daily_correction_stack(merged: pd.DataFrame) -> pd.DataFrame:
    """Nombre de points corrigés par jour et par type (pour barres empilées)."""
    work = merged.copy()
    work["date"] = work["datetime"].dt.floor("D")
    counts = (
        work.groupby(["date", "correction"], observed=True)
        .size()
        .unstack(fill_value=0)
    )
    for col in ("minor", "major", "removed"):
        if col not in counts.columns:
            counts[col] = 0
    return counts[["minor", "major", "removed"]].sort_index()


def plot_consumption_national_panels(
    ax_top: plt.Axes,
    ax_bottom: plt.Axes,
    ts_raw: pd.DataFrame,
    datetime_col: str,
    value_col: str,
    ts_silver: pd.DataFrame | None = None,
) -> str:
    """Série nationale — brute vs Silver superposées + écarts colorés par type de correction."""
    _style_ax_dashboard(ax_top, grid_axis="both")
    _style_ax_dashboard(ax_bottom, grid_axis="both")
    if ts_raw.empty:
        msg = "Données brutes indisponibles"
        ax_top.text(0.5, 0.5, msg, ha="center", va="center", transform=ax_top.transAxes)
        ax_bottom.set_visible(False)
        return msg

    plot_ts = _subsample_series(ts_raw, datetime_col)
    dt = plot_ts[datetime_col]
    t_min, t_max = dt.min(), dt.max()
    values = plot_ts[value_col].to_numpy(dtype=float)
    y_lo = float(np.nanmin(values))
    y_hi = float(np.nanmax(values))
    _shade_weekends_and_holidays(ax_top, t_min, t_max, y_lo=y_lo, y_hi=y_hi)

    note_parts: list[str] = []
    if ts_silver is not None and not ts_silver.empty:
        raw_ma = _rolling_mean_dataframe(ts_raw, datetime_col, value_col)
        silver_ma = _rolling_mean_dataframe(ts_silver, datetime_col, value_col)

        if not raw_ma.empty:
            ax_top.plot(
                raw_ma[datetime_col],
                raw_ma[value_col],
                color=DATA_LAYER["raw"],
                linewidth=2.4,
                alpha=0.95,
                linestyle=(0, (6, 3)),
                label=f"{DATA_LAYER['raw_label']} (MM 7 j)",
                zorder=3,
            )
        if not silver_ma.empty:
            ax_top.plot(
                silver_ma[datetime_col],
                silver_ma[value_col],
                color=DATA_LAYER["silver"],
                linewidth=2.8,
                alpha=1.0,
                linestyle="-",
                label=f"{DATA_LAYER['silver_label']} (MM 7 j)",
                zorder=4,
            )

        ax_top.plot(
            dt,
            values,
            color=DATA_LAYER["raw"],
            linewidth=0.45,
            alpha=0.18,
            linestyle="--",
            label="Mesures horaires brutes",
            zorder=2,
        )

        merged = _build_point_comparison(ts_raw, ts_silver)
        summary = _comparison_summary(merged, ts_raw)
        _shade_interp_blocks(ax_top, merged)

        outlier_mask = _is_consumption_outlier(plot_ts[value_col])
        if outlier_mask.any():
            ax_top.scatter(
                plot_ts.loc[outlier_mask, datetime_col],
                plot_ts.loc[outlier_mask, value_col],
                marker="x",
                s=22,
                color=COLORS["missing"],
                alpha=0.75,
                linewidths=1.0,
                label="Outliers bruts",
                zorder=6,
            )

        daily_stack = _daily_correction_stack(merged)
        if len(daily_stack) > 400:
            daily_stack = daily_stack.iloc[:: max(len(daily_stack) // 400, 1)]

        bottom = np.zeros(len(daily_stack))
        x_days = daily_stack.index
        for corr_type in ("minor", "major", "removed"):
            heights = daily_stack[corr_type].to_numpy(dtype=float)
            ax_bottom.bar(
                x_days,
                heights,
                bottom=bottom,
                width=1.2,
                color=CORRECTION_COLORS[corr_type],
                alpha=0.88,
                edgecolor="none",
                label=CORRECTION_LABELS[corr_type],
            )
            bottom = bottom + heights

        ax_bottom.set_ylabel("Points corrigés / jour")
        ax_bottom.set_title(
            "Corrections appliquées (volume journalier par type)",
            loc="left",
            fontsize=11,
            fontweight="600",
            pad=8,
        )
        ax_bottom.legend(loc="upper right", fontsize=7, ncol=3, frameon=True)

        pct_out = 100.0 * summary["n_removed"] / max(summary["n_dedup"], 1)
        note_parts.append(
            f"{summary['n_modified']:,} points modifiés ({summary['pct_modified']:.1f} %) · "
            f"{summary['pct_imputed']:.1f} % imputés · {pct_out:.1f} % anomalies supprimées".replace(",", " ")
        )
        ax_top.set_title(
            f"Impact du nettoyage — consommation nationale ({pct_out:.1f} % de points aberrants corrigés)",
            loc="left",
            fontsize=11,
            fontweight="600",
            pad=8,
        )
    else:
        ax_top.plot(
            dt,
            values,
            color=DATA_LAYER["raw"],
            linewidth=1.2,
            alpha=0.85,
            linestyle="--",
            label=DATA_LAYER["raw_label"],
            zorder=2,
        )
        outlier_mask = _is_consumption_outlier(plot_ts[value_col])
        if outlier_mask.any():
            ax_top.scatter(
                plot_ts.loc[outlier_mask, datetime_col],
                plot_ts.loc[outlier_mask, value_col],
                marker="x",
                s=18,
                color=COLORS["missing"],
                alpha=0.65,
                linewidths=0.8,
                label="Outliers bruts",
                zorder=5,
            )
        ax_top.set_title(
            "Consommation horaire — source brute (Silver indisponible)",
            loc="left",
            fontsize=11,
            fontweight="600",
            pad=8,
        )
        note_parts.append("Lancer make run-etl pour comparer avec la couche Silver")
        ax_bottom.text(
            0.5,
            0.5,
            "Couche Silver indisponible\n(comparaison avant/après)",
            ha="center",
            va="center",
            transform=ax_bottom.transAxes,
            fontsize=10,
            color=COLORS["muted"],
        )

    ax_top.set_xlim(t_min, t_max)
    ax_top.set_ylabel("Puissance (MW)")
    format_mw_axis(ax_top)
    ax_top.legend(loc="upper left", frameon=True, facecolor="white", edgecolor=DASHBOARD["panel_border"], fontsize=8)
    ax_top.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax_bottom.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    return " · ".join(p for p in note_parts if p)


def build_consumption_national_figure(
    ts_raw: pd.DataFrame,
    datetime_col: str = "datetime",
    value_col: str = "consumption_mw",
    ts_silver: pd.DataFrame | None = None,
) -> tuple[plt.Figure, str]:
    fig, (ax_top, ax_bottom) = plt.subplots(2, 1, figsize=(14, 8.8), sharex=False)
    note = plot_consumption_national_panels(ax_top, ax_bottom, ts_raw, datetime_col, value_col, ts_silver)
    fig.subplots_adjust(hspace=0.34, top=0.82, bottom=0.08, left=0.07, right=0.98)
    return fig, note


def _monthly_twh_chronology(df: pd.DataFrame) -> pd.DataFrame:
    work = df[["datetime", "consumption_mw"]].copy()
    work["datetime"] = pd.to_datetime(work["datetime"], errors="coerce", utc=True)
    work["consumption_mw"] = pd.to_numeric(work["consumption_mw"], errors="coerce")
    work = work.dropna()
    if work.empty:
        return pd.DataFrame(columns=["period", "label", "consumption_total_twh"])
    monthly = (
        work.set_index("datetime")
        .resample("MS")["consumption_mw"]
        .mean()
        .dropna()
        .reset_index()
    )
    hours = monthly["datetime"].dt.days_in_month * 24
    monthly["consumption_total_twh"] = monthly["consumption_mw"] * hours / 1_000_000
    monthly["period"] = monthly["datetime"]
    monthly["label"] = monthly["datetime"].dt.strftime("%Y-%m")
    return monthly[["period", "label", "consumption_total_twh"]]


def plot_monthly_comparison(
    ax_bars: plt.Axes,
    ax_table: plt.Axes,
    monthly_raw: pd.DataFrame,
    monthly_silver: pd.DataFrame | None = None,
) -> str:
    """Barres groupées avant/après par mois + tendance Silver + tableau récap."""
    _style_ax_dashboard(ax_bars, grid_axis="y")
    ax_table.axis("off")

    if monthly_raw.empty:
        msg = "Données mensuelles indisponibles"
        ax_bars.text(0.5, 0.5, msg, ha="center", va="center", transform=ax_bars.transAxes)
        return msg

    chron_raw = monthly_raw.sort_values("period").reset_index(drop=True)
    n = len(chron_raw)
    x = np.arange(n)
    width = 0.36

    ax_bars.bar(
        x - width / 2,
        chron_raw["consumption_total_twh"],
        width=width,
        color=DATA_LAYER["raw"],
        alpha=0.55,
        label=DATA_LAYER["raw_label"],
        edgecolor="white",
        linewidth=0.4,
    )

    note_parts: list[str] = []
    max_gap_row: dict[str, Any] | None = None
    max_gap_pct = 0.0

    if monthly_silver is not None and not monthly_silver.empty:
        chron_silver = monthly_silver.sort_values("period").reset_index(drop=True)
        merged = chron_raw.merge(chron_silver, on="label", suffixes=("_raw", "_silver"), how="inner")
        silver_aligned = merged["consumption_total_twh_silver"].to_numpy(dtype=float)
        raw_aligned = merged["consumption_total_twh_raw"].to_numpy(dtype=float)
        x_merged = np.arange(len(merged))

        ax_bars.bar(
            x_merged + width / 2,
            silver_aligned,
            width=width,
            color=DATA_LAYER["silver"],
            alpha=0.92,
            label=DATA_LAYER["silver_label"],
            edgecolor="white",
            linewidth=0.4,
        )

        if len(silver_aligned) >= 3:
            z = np.polyfit(x_merged, silver_aligned, 1)
            trend = np.poly1d(z)
            ax_bars.plot(
                x_merged,
                trend(x_merged),
                color=COLORS["accent"],
                linewidth=2.0,
                linestyle="-",
                label="Tendance (Silver)",
                zorder=5,
            )

        for i, row in merged.iterrows():
            raw_v = float(row["consumption_total_twh_raw"])
            sil_v = float(row["consumption_total_twh_silver"])
            if raw_v <= 0:
                continue
            pct = (sil_v - raw_v) / raw_v * 100.0
            if abs(pct) >= 1.0:
                ax_bars.annotate(
                    f"{pct:+.1f} %",
                    xy=(x_merged[i], max(raw_v, sil_v)),
                    xytext=(0, 4),
                    textcoords="offset points",
                    ha="center",
                    fontsize=6,
                    color=COLORS["muted"],
                )
            if abs(pct) > abs(max_gap_pct):
                max_gap_pct = pct
                max_gap_row = {"label": row["label"], "pct": pct, "raw": raw_v, "silver": sil_v}

        mean_pct = float(
            (
                (merged["consumption_total_twh_silver"] - merged["consumption_total_twh_raw"]).abs()
                / merged["consumption_total_twh_raw"].replace(0, np.nan)
                * 100.0
            ).mean()
        )
        note_parts.append(f"Écart mensuel moyen brute/Silver : {mean_pct:.2f} % TWh")
        ax_bars.set_title(
            "Volume mensuel — barres groupées brute vs nettoyée",
            loc="left",
            fontsize=11,
            fontweight="600",
            pad=8,
        )
    else:
        ax_bars.set_title("Volume mensuel — source brute (Silver indisponible)", loc="left", fontsize=11, fontweight="600", pad=8)
        note_parts.append("Lancer make run-etl pour la comparaison nettoyée")

    tick_step = max(n // 18, 1)
    ax_bars.set_xticks(x[::tick_step])
    ax_bars.set_xticklabels(chron_raw["label"].iloc[::tick_step], rotation=45, ha="right", fontsize=7)
    ax_bars.set_ylabel("Énergie (TWh)")
    ax_bars.legend(loc="upper left", fontsize=8, frameon=True)

    if max_gap_row is not None:
        cause = (
            f"Imputation / filtrage ETL ({abs(max_gap_row['pct']):.1f} % d'écart TWh)"
            if abs(max_gap_row["pct"]) >= 3
            else "Variations normales de qualité source"
        )
        table_data = [
            ["Mois écart max.", max_gap_row["label"]],
            ["Écart brute → Silver", f"{max_gap_row['pct']:+.1f} %"],
            ["TWh brute / Silver", f"{max_gap_row['raw']:.2f} / {max_gap_row['silver']:.2f}"],
            ["Cause probable", cause],
        ]
        tbl = ax_table.table(
            cellText=table_data,
            colLabels=["Indicateur", "Valeur"],
            loc="center",
            cellLoc="left",
            colWidths=[0.38, 0.62],
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(8)
        tbl.scale(1.0, 1.35)
        for (r, c), cell in tbl.get_celld().items():
            if r == 0:
                cell.set_facecolor(DASHBOARD["panel"])
                cell.set_text_props(fontweight="600")
            else:
                cell.set_facecolor("white")

    return " · ".join(note_parts)


def build_monthly_comparison_figure(
    df_raw: pd.DataFrame,
    df_silver: pd.DataFrame | None = None,
) -> tuple[plt.Figure, str]:
    monthly_raw = _monthly_twh_chronology(df_raw)
    monthly_silver = _monthly_twh_chronology(df_silver) if df_silver is not None and not df_silver.empty else None
    fig = plt.figure(figsize=(14, 9.0))
    gs = fig.add_gridspec(2, 1, height_ratios=[4.2, 1.0], hspace=0.28)
    ax_bars = fig.add_subplot(gs[0])
    ax_table = fig.add_subplot(gs[1])
    note = plot_monthly_comparison(ax_bars, ax_table, monthly_raw, monthly_silver)
    fig.subplots_adjust(top=0.82, bottom=0.10, left=0.07, right=0.98)
    return fig, note


def _quality_category_counts(
    df: pd.DataFrame,
    numeric_cols: list[str],
    *,
    use_silver_bounds: bool = False,
) -> dict[str, float]:
    if df.empty or not numeric_cols:
        return {"valid": 0.0, "missing": 0.0, "outlier": 0.0, "partial": 0.0}
    present = df[numeric_cols]
    score = present.notna().sum(axis=1) / len(numeric_cols)
    cons = pd.to_numeric(df.get("consumption_mw"), errors="coerce")
    missing = present.isna().any(axis=1) | cons.isna()
    if use_silver_bounds:
        outlier = (~missing) & (
            (cons < SILVER_CONSO_MIN_MW) | (cons > SILVER_CONSO_MAX_MW)
        )
    else:
        outlier = (~missing) & (
            (cons < 0) | (cons > SILVER_CONSO_MAX_MW)
        )
    partial = (~missing) & (~outlier) & (score < 1.0)
    valid = (~missing) & (~outlier) & (score >= 1.0)
    n = len(df)
    return {
        "valid": 100.0 * valid.sum() / n,
        "missing": 100.0 * missing.sum() / n,
        "outlier": 100.0 * outlier.sum() / n,
        "partial": 100.0 * partial.sum() / n,
    }


def _quality_volume_summary(raw_df: pd.DataFrame, silver_df: pd.DataFrame | None) -> dict[str, int]:
    n_raw = len(raw_df)
    if silver_df is None or silver_df.empty:
        return {"raw": n_raw, "silver": 0, "duplicates": 0, "removed": 0}
    dt = pd.to_datetime(raw_df["datetime"], errors="coerce", utc=True)
    n_dup = int(dt.duplicated().sum())
    n_silver = len(silver_df)
    return {
        "raw": n_raw,
        "silver": n_silver,
        "duplicates": n_dup,
        "removed": max(n_raw - n_dup - n_silver, 0),
    }


def plot_quality_raw_vs_silver_panels(
    axes: np.ndarray,
    raw_df: pd.DataFrame,
    silver_df: pd.DataFrame | None,
    numeric_cols: list[str],
) -> str:
    """Dashboard qualité 4 quadrants — avant/après nettoyage."""
    ax_complete, ax_heat, ax_dist, ax_stats = axes.flat
    for ax in (ax_complete, ax_heat, ax_dist):
        _style_ax_dashboard(ax, grid_axis="y")
    ax_stats.axis("off")

    if raw_df.empty or not numeric_cols:
        for ax in axes.flat:
            ax.text(0.5, 0.5, "Données indisponibles", ha="center", va="center", transform=ax.transAxes)
        return ""

    raw_counts = _quality_category_counts(raw_df, numeric_cols, use_silver_bounds=False)
    categories = ["Valides", "Manquantes", "Aberrantes", "Incomplètes"]
    keys = ["valid", "missing", "outlier", "partial"]
    y_pos = np.arange(len(categories))
    width = 0.34
    raw_vals = [raw_counts[k] for k in keys]
    ax_complete.barh(y_pos + width / 2, raw_vals, height=width, color=DATA_LAYER["raw"], label="Avant (brut)", alpha=0.85)
    if silver_df is not None and not silver_df.empty:
        silver_counts = _quality_category_counts(silver_df, numeric_cols, use_silver_bounds=True)
        silver_vals = [silver_counts[k] for k in keys]
        ax_complete.barh(y_pos - width / 2, silver_vals, height=width, color=DATA_LAYER["silver"], label="Après (Silver)", alpha=0.92)
        interp_pct = 100.0 * silver_df["is_interpolated"].fillna(False).astype(bool).mean() if "is_interpolated" in silver_df.columns else 0.0
    else:
        silver_vals = []
        interp_pct = 0.0
        ax_complete.text(0.98, 0.04, "Silver indisponible", transform=ax_complete.transAxes, ha="right", fontsize=8, color=COLORS["missing"])
    ax_complete.set_yticks(y_pos)
    ax_complete.set_yticklabels(categories)
    ax_complete.set_xlim(0, 100)
    ax_complete.set_xlabel("Part des enregistrements (%)")
    ax_complete.set_title("Q1 — Complétude avant / après", loc="left", fontweight="600", fontsize=10)
    ax_complete.legend(loc="lower right", fontsize=7.5)

    matrix_before, row_labels, col_labels = _anomaly_heatmap_matrix(raw_df, numeric_cols)
    ax_before = ax_heat.inset_axes([0.0, 0.0, 0.48, 1.0])
    ax_after = ax_heat.inset_axes([0.52, 0.0, 0.48, 1.0])
    ax_heat.set_axis_off()
    vmax = max(float(matrix_before.max()) if matrix_before.size else 1.0, 1.0)
    if matrix_before.size:
        im0 = ax_before.imshow(matrix_before, aspect="auto", cmap="Reds", vmin=0, vmax=vmax)
        ax_before.set_yticks(range(len(row_labels)))
        ax_before.set_yticklabels(row_labels, fontsize=7)
        ax_before.set_xticks(range(0, len(col_labels), max(len(col_labels) // 8, 1)))
        ax_before.set_xticklabels(
            [col_labels[i] for i in range(0, len(col_labels), max(len(col_labels) // 8, 1))],
            rotation=45,
            ha="right",
            fontsize=6,
        )
        ax_before.set_title("Avant", fontsize=8, fontweight="600")
    else:
        ax_before.text(0.5, 0.5, "N/A", ha="center", va="center", transform=ax_before.transAxes)

    if silver_df is not None and not silver_df.empty:
        matrix_after, _, _ = _anomaly_heatmap_matrix(silver_df, numeric_cols)
        if matrix_after.size:
            ax_after.imshow(matrix_after, aspect="auto", cmap="Reds", vmin=0, vmax=vmax)
            ax_after.set_yticks(range(len(row_labels)))
            ax_after.set_yticklabels(row_labels, fontsize=7)
            ax_after.set_xticks(range(0, len(col_labels), max(len(col_labels) // 8, 1)))
            ax_after.set_xticklabels(
                [col_labels[i] for i in range(0, len(col_labels), max(len(col_labels) // 8, 1))],
                rotation=45,
                ha="right",
                fontsize=6,
            )
        ax_after.set_title("Après", fontsize=8, fontweight="600")
    else:
        ax_after.text(0.5, 0.5, "Silver\nindisponible", ha="center", va="center", transform=ax_after.transAxes, fontsize=8)
    ax_heat.set_title("Q2 — Anomalies par mois et variable (%)", loc="left", fontweight="600", fontsize=10, y=1.02)

    cons_raw = pd.to_numeric(raw_df.get("consumption_mw"), errors="coerce").dropna()
    if len(cons_raw) > 20:
        ax_dist.hist(
            cons_raw,
            bins=45,
            density=True,
            alpha=0.45,
            color=COLORS["missing"],
            label="Avant (brut)",
            edgecolor="white",
            linewidth=0.2,
        )
        try:
            cons_raw.plot.kde(ax=ax_dist, color="#DC2626", linewidth=1.6, label="KDE brut")
        except Exception:
            pass
    if silver_df is not None and not silver_df.empty:
        cons_silver = pd.to_numeric(silver_df.get("consumption_mw"), errors="coerce").dropna()
        if len(cons_silver) > 20:
            ax_dist.hist(
                cons_silver,
                bins=45,
                density=True,
                alpha=0.40,
                color=DATA_LAYER["silver"],
                label="Après (Silver)",
                edgecolor="white",
                linewidth=0.2,
            )
            try:
                cons_silver.plot.kde(ax=ax_dist, color=DATA_LAYER["silver"], linewidth=1.8, label="KDE Silver")
            except Exception:
                pass
    ax_dist.set_xlabel("Consommation (MW)")
    ax_dist.set_ylabel("Densité")
    ax_dist.set_title("Q3 — Distribution consommation avant / après", loc="left", fontweight="600", fontsize=10)
    ax_dist.legend(fontsize=7.5)

    stats_before = _consumption_stats(raw_df["consumption_mw"])
    stats_after = _consumption_stats(silver_df["consumption_mw"]) if silver_df is not None and not silver_df.empty else stats_before

    def _pct_delta(before: float, after: float) -> str:
        if not np.isfinite(before) or before == 0:
            return "—"
        return f"{(after - before) / before * 100.0:+.1f} %"

    table_rows = [
        ["Moyenne (MW)", f"{stats_before['mean']:,.0f}".replace(",", " "), f"{stats_after['mean']:,.0f}".replace(",", " "), _pct_delta(stats_before["mean"], stats_after["mean"])],
        ["Écart-type", f"{stats_before['std']:,.0f}".replace(",", " "), f"{stats_after['std']:,.0f}".replace(",", " "), _pct_delta(stats_before["std"], stats_after["std"])],
        ["Min / Max", f"{stats_before['min']:,.0f} / {stats_before['max']:,.0f}".replace(",", " "), f"{stats_after['min']:,.0f} / {stats_after['max']:,.0f}".replace(",", " "), "cohérent" if stats_after["min"] >= SILVER_CONSO_MIN_MW * 0.5 else "filtré"],
        ["Nb outliers", f"{stats_before['outliers']:,}".replace(",", " "), f"{stats_after['outliers']:,}".replace(",", " "), "100 % corrigé" if stats_after["outliers"] == 0 and stats_before["outliers"] > 0 else "—"],
    ]
    tbl = ax_stats.table(
        cellText=table_rows,
        colLabels=["Métrique", "Avant", "Après", "Écart"],
        loc="center",
        cellLoc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.scale(1.0, 1.45)
    ax_stats.set_title("Q4 — Résumé statistique", loc="left", fontweight="600", fontsize=10, pad=12)

    vol = _quality_volume_summary(raw_df, silver_df)
    note_parts = [
        f"Rétention Silver : {100.0 * vol['silver'] / max(vol['raw'], 1):.1f} %",
        f"Doublons bruts : {vol['duplicates']:,}".replace(",", " "),
    ]
    if silver_df is not None and not silver_df.empty:
        note_parts.append(f"Imputation Silver : {interp_pct:.1f} %")
        note_parts.append(f"Outliers corrigés : {stats_before['outliers'] - stats_after['outliers']:,}".replace(",", " "))
    return " · ".join(note_parts)


def build_quality_diagnostic_figure(
    raw_df: pd.DataFrame,
    numeric_cols: list[str],
    silver_df: pd.DataFrame | None = None,
) -> tuple[plt.Figure, str]:
    fig, axes = plt.subplots(2, 2, figsize=(14, 9.2))
    note = plot_quality_raw_vs_silver_panels(axes, raw_df, silver_df, numeric_cols)
    fig.subplots_adjust(hspace=0.48, wspace=0.32, top=0.82, bottom=0.07, left=0.10, right=0.97)
    return fig, note
